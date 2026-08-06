//! HOT->WARM transition protocol for vector segments.
//!
//! Implements the staging-directory atomic transition: write .mpf files
//! to a staging directory, fsync each file, fsync the directory, update
//! manifest, rename staging to final, fsync parent.

use std::io::Write as _;
use std::path::Path;

use roaring::RoaringBitmap;

use crate::persistence::fsync::{fsync_directory, fsync_file};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::tiered::SegmentHandle;
use crate::vector::persistence::warm_segment::{
    write_codes_mpf, write_graph_mpf, write_mvcc_mpf, write_vectors_mpf,
};

/// Removes a staging directory unless the transition disarms it.
///
/// `transition_to_warm` has ~10 fallible steps between creating the staging
/// directory and renaming it away. Every `?` in that stretch used to leak the
/// whole directory: a live instance accumulated 14,499 orphans holding 20 GB
/// against 174 MB of real segments (issue #435). A guard is used rather than
/// cleanup at each `?` precisely because the leak came from the paths nobody
/// remembered to annotate.
struct StagingGuard<'a> {
    path: &'a Path,
    armed: bool,
}

impl<'a> StagingGuard<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path, armed: true }
    }

    /// Called once the directory has been renamed away and must NOT be removed.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            // Best-effort: a failure to clean up must not mask the error that
            // caused the unwind, and the startup sweep is the backstop.
            let _ = std::fs::remove_dir_all(self.path);
        }
    }
}

/// Remove `.segment-*.staging` leftovers from a shard's `vectors/` directory.
///
/// The backstop for orphans a guard could not handle: a `kill -9` between the
/// manifest commit and the rename, or anything written by a build that predates
/// the guard. Returns how many directories were removed.
///
/// Safe by construction: `.staging` paths are produced in exactly one place
/// (`transition_to_warm`) and consumed by nothing — every reader and recovery
/// path opens the final `segment-{id}` name — so a staging directory is
/// unreachable the moment it is not mid-write. Real `segment-*` directories do
/// not match the pattern and are never touched.
pub fn sweep_orphan_staging(vectors_dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(vectors_dir) else {
        return 0; // fresh shard, or no vector data — nothing to sweep
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with(".segment-")
            && name.ends_with(".staging")
            && entry.path().is_dir()
            && std::fs::remove_dir_all(entry.path()).is_ok()
        {
            removed += 1;
        }
    }
    if removed > 0 {
        tracing::info!(
            removed,
            dir = %vectors_dir.display(),
            "swept orphaned warm-tier staging directories"
        );
    }
    removed
}

/// Transition a HOT vector segment to WARM (mmap-backed on disk).
///
/// Protocol:
/// 1. Create staging directory: `{shard_dir}/vectors/.segment-{id}.staging`
/// 2. Write .mpf files to staging (codes, graph, vectors?, mvcc)
/// 3. Fsync each file and the staging directory
/// 4. Update manifest with FileEntry (tier=Warm, status=Active)
/// 5. Manifest commit (atomic durability point)
/// 6. Rename staging -> final: `{shard_dir}/vectors/segment-{id}`
/// 7. Fsync parent directory
/// 8. Return SegmentHandle for the new warm segment
///
/// If the process crashes between steps 4 and 6, recovery will see the
/// manifest entry but no final directory -- the staging dir can be cleaned up.
pub fn transition_to_warm(
    shard_dir: &Path,
    segment_id: u64,
    file_id: u64,
    codes_data: &[u8],
    graph_data: &[u8],
    vectors_data: Option<&[u8]>,
    mvcc_data: &[u8],
    manifest: &mut ShardManifest,
    wal: Option<&mut crate::persistence::wal_v3::segment::WalWriterV3>,
) -> std::io::Result<SegmentHandle> {
    let vectors_dir = shard_dir.join("vectors");
    std::fs::create_dir_all(&vectors_dir)?;

    let staging = vectors_dir.join(format!(".segment-{segment_id}.staging"));
    let final_dir = vectors_dir.join(format!("segment-{segment_id}"));

    // Step 1: Create staging directory. Remove a stale leftover for this same
    // id first — the sibling writer (`vector/persistence/segment_io.rs`) has
    // always done this; this path did not, so a retry could inherit a previous
    // attempt's partial files.
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    std::fs::create_dir_all(&staging)?;
    // Every early return from here to the rename removes the directory (#435).
    let mut staging_guard = StagingGuard::new(&staging);

    // Step 2: Write .mpf files to staging
    write_codes_mpf(&staging.join("codes.mpf"), file_id, codes_data)?;
    write_graph_mpf(&staging.join("graph.mpf"), file_id, graph_data)?;
    write_mvcc_mpf(&staging.join("mvcc.mpf"), file_id, mvcc_data)?;

    if let Some(vdata) = vectors_data {
        write_vectors_mpf(&staging.join("vectors.mpf"), file_id, vdata)?;
    }

    // Write empty deletion bitmap (no vectors deleted in fresh warm segment)
    {
        let bitmap = RoaringBitmap::new();
        let bitmap_path = staging.join("deletion.bitmap");
        let mut bitmap_file = std::fs::File::create(&bitmap_path)?;
        bitmap.serialize_into(&mut bitmap_file)?;
        bitmap_file.flush()?;
    }

    // Step 3: Fsync staging directory (file data already fsynced by writers)
    // Re-fsync each file to be absolutely certain
    for entry in std::fs::read_dir(&staging)? {
        let entry = entry?;
        fsync_file(&entry.path())?;
    }
    fsync_directory(&staging)?;

    // Step 4-5: Update manifest and commit (atomic durability point)
    let codes_pages = if codes_data.is_empty() {
        1
    } else {
        let payload_cap = 65536 - 64;
        (codes_data.len() + payload_cap - 1) / payload_cap
    };

    let entry = FileEntry {
        file_id,
        file_type: PageType::VecCodes as u8,
        status: FileStatus::Active,
        tier: StorageTier::Warm,
        page_size_log2: 16, // 64KB
        page_count: codes_pages as u32,
        byte_size: codes_data.len() as u64,
        created_lsn: 0,
        db_index: 0,
        max_key_hash: u64::MAX,
        last_modified_lsn: 0,
    };

    // Step 4a: Write FileCreate WAL record before manifest commit
    if let Some(wal) = wal {
        let mut entry_buf = [0u8; FileEntry::SIZE];
        entry.write_to(&mut entry_buf);
        wal.append(
            crate::persistence::wal_v3::record::WalRecordType::FileCreate,
            &entry_buf,
        );
        // Flush WAL so FileCreate is durable before manifest commit
        wal.flush_sync()?;
    }

    manifest.add_file(entry);
    manifest.commit()?;

    // Step 6: Rename staging -> final. The directory now lives under its final
    // name, so the guard must not remove it.
    std::fs::rename(&staging, &final_dir)?;
    staging_guard.disarm();

    // Step 7: Fsync parent directory
    fsync_directory(&vectors_dir)?;

    // Step 8: Return segment handle
    Ok(SegmentHandle::new(segment_id, final_dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::manifest::ShardManifest;

    /// A failed transition must not leave its staging directory behind.
    ///
    /// Found in production: 14,499 orphaned `.segment-*.staging` dirs holding
    /// 20 GB, against 174 MB of real segments — 99% of the vector store was
    /// abandoned scratch, all written in one 3-minute window and never
    /// reclaimed across a restart. Every early return between
    /// `create_dir_all(&staging)` and the rename used to leak the directory.
    /// See issue #435.
    ///
    /// The failure is forced at the RENAME, which is where production died
    /// (every orphan was fully written and fsynced). A non-empty `segment-{id}`
    /// makes `fs::rename` fail with ENOTEMPTY — guaranteed, unlike unlinking
    /// the manifest, whose already-open fd keeps accepting writes.
    ///
    /// Asserting `is_err()` is load-bearing: "no staging dir remains" is also
    /// true of a SUCCESSFUL transition (the rename moves it), so without
    /// pinning the failure path this test would pass with the guard deleted.
    #[test]
    fn failed_transition_leaves_no_staging_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let vectors = shard_dir.join("vectors");
        std::fs::create_dir_all(&vectors).unwrap();

        // Block the rename: a non-empty destination cannot be replaced.
        std::fs::create_dir_all(vectors.join("segment-7")).unwrap();
        std::fs::write(vectors.join("segment-7/occupied"), b"in the way").unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let result = transition_to_warm(
            &shard_dir,
            7,
            7,
            b"codes",
            b"graph",
            Some(b"vectors"),
            b"mvcc",
            &mut manifest,
            None,
        );

        assert!(
            result.is_err(),
            "test must exercise the FAILURE path — a success would remove the \
             staging dir by renaming it, making the assertion below vacuous"
        );
        let leaked: Vec<_> = std::fs::read_dir(&vectors)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".staging"))
            .collect();
        assert!(
            leaked.is_empty(),
            "a failed warm transition must clean up after itself; leaked {leaked:?}"
        );
        assert_eq!(
            std::fs::read(vectors.join("segment-7/occupied")).unwrap(),
            b"in the way",
            "cleanup must not touch the pre-existing segment directory"
        );
    }

    /// Orphans from older builds (or a kill -9 between commit and rename) must
    /// be reclaimable at startup — otherwise 20 GB sits there until someone
    /// notices `du` disagreeing with a glob.
    #[test]
    fn startup_sweep_removes_orphan_staging_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let vectors = tmp.path().join("vectors");
        std::fs::create_dir_all(vectors.join(".segment-1.staging")).unwrap();
        std::fs::create_dir_all(vectors.join(".segment-2.staging")).unwrap();
        std::fs::write(vectors.join(".segment-1.staging/codes.mpf"), b"x").unwrap();
        // A real segment must be left strictly alone.
        std::fs::create_dir_all(vectors.join("segment-1")).unwrap();
        std::fs::write(vectors.join("segment-1/codes.mpf"), b"keep").unwrap();

        let removed = sweep_orphan_staging(&vectors);

        assert_eq!(removed, 2, "both orphans must be swept");
        assert!(
            vectors.join("segment-1").is_dir(),
            "the sweep must never touch a real segment"
        );
        assert_eq!(
            std::fs::read(vectors.join("segment-1/codes.mpf")).unwrap(),
            b"keep",
            "real segment contents must be untouched"
        );
        assert!(!vectors.join(".segment-1.staging").exists());
        assert!(!vectors.join(".segment-2.staging").exists());
    }

    /// The sweep must be safe to call on a fresh or missing directory.
    #[test]
    fn startup_sweep_tolerates_missing_dir() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(sweep_orphan_staging(&tmp.path().join("nope")), 0);
    }

    #[test]
    fn test_transition_to_warm_creates_mpf_files() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0xAAu8; 2000];
        let graph = vec![0xBBu8; 500];
        let mvcc = vec![0u8; 24 * 10];

        let handle = transition_to_warm(
            &shard_dir,
            1,
            100,
            &codes,
            &graph,
            None,
            &mvcc,
            &mut manifest,
            None,
        )
        .unwrap();

        let seg_dir = handle.segment_dir();
        assert!(seg_dir.join("codes.mpf").exists());
        assert!(seg_dir.join("graph.mpf").exists());
        assert!(seg_dir.join("mvcc.mpf").exists());
        assert!(!seg_dir.join("vectors.mpf").exists()); // None passed
        // meta.mpf / undo.mpf were unread placeholders (VecMeta/VecUndo pages
        // with zero readers anywhere in the codebase) -- no longer written.
        assert!(!seg_dir.join("meta.mpf").exists());
        assert!(!seg_dir.join("undo.mpf").exists());
    }

    #[test]
    fn test_transition_staging_dir_cleaned() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0u8; 500];
        let graph = vec![0u8; 200];
        let mvcc = vec![0u8; 24 * 5];

        let _handle = transition_to_warm(
            &shard_dir,
            2,
            200,
            &codes,
            &graph,
            None,
            &mvcc,
            &mut manifest,
            None,
        )
        .unwrap();

        // Staging dir should not exist (renamed to final)
        let staging = shard_dir.join("vectors/.segment-2.staging");
        assert!(
            !staging.exists(),
            "staging directory should not remain after transition"
        );
    }

    #[test]
    fn test_transition_manifest_updated() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0u8; 500];
        let graph = vec![0u8; 200];
        let mvcc = vec![0u8; 24 * 5];

        let _handle = transition_to_warm(
            &shard_dir,
            3,
            300,
            &codes,
            &graph,
            None,
            &mvcc,
            &mut manifest,
            None,
        )
        .unwrap();

        // Manifest should have a new entry
        assert_eq!(manifest.files().len(), 1);
        let entry = &manifest.files()[0];
        assert_eq!(entry.file_id, 300);
        assert_eq!(entry.status, FileStatus::Active);
        assert_eq!(entry.tier, StorageTier::Warm);
        assert_eq!(entry.byte_size, 500);
    }

    #[test]
    fn test_transition_with_optional_vectors() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0u8; 500];
        let graph = vec![0u8; 200];
        let vectors = vec![0u8; 3000];
        let mvcc = vec![0u8; 24 * 5];

        let handle = transition_to_warm(
            &shard_dir,
            4,
            400,
            &codes,
            &graph,
            Some(&vectors),
            &mvcc,
            &mut manifest,
            None,
        )
        .unwrap();

        assert!(handle.segment_dir().join("vectors.mpf").exists());
    }

    #[test]
    fn test_transition_without_vectors() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0u8; 500];
        let graph = vec![0u8; 200];
        let mvcc = vec![0u8; 24 * 5];

        let handle = transition_to_warm(
            &shard_dir,
            5,
            500,
            &codes,
            &graph,
            None,
            &mvcc,
            &mut manifest,
            None,
        )
        .unwrap();

        assert!(!handle.segment_dir().join("vectors.mpf").exists());
    }

    #[test]
    fn test_warm_segment_open_after_transition() {
        use crate::vector::persistence::warm_segment::WarmSegmentFiles;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0xAAu8; 1000];
        let graph = vec![0xBBu8; 500];
        let mvcc = vec![0u8; 24 * 10];

        let handle = transition_to_warm(
            &shard_dir,
            6,
            600,
            &codes,
            &graph,
            None,
            &mvcc,
            &mut manifest,
            None,
        )
        .unwrap();

        let seg_dir = handle.segment_dir().to_path_buf();
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();

        // Verify we can read back the codes data (after sub-header)
        let cd = ws.codes_data(0);
        // The page contains: 64B MoonPageHeader + 32B VecCodes sub-header + payload (possibly LZ4)
        // codes_data(0) returns raw page data starting after MoonPageHeader (offset 64)
        // Sub-header is 32 bytes, so actual codes start at offset 32 within the returned slice
        let sub_hdr_size = crate::vector::persistence::warm_segment::VEC_CODES_SUB_HEADER_SIZE;
        // The payload_bytes in the header includes sub-header + data (possibly compressed)
        // Just verify the page is non-empty and has the right structure
        assert!(
            cd.len() >= sub_hdr_size,
            "codes page should have at least sub-header"
        );
        assert_eq!(ws.page_count_codes(), 1);
    }

    #[test]
    fn test_transition_creates_deletion_bitmap() {
        use roaring::RoaringBitmap;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0xAAu8; 2000];
        let graph = vec![0xBBu8; 500];
        let mvcc = vec![0u8; 24 * 10];

        let handle = transition_to_warm(
            &shard_dir,
            1,
            100,
            &codes,
            &graph,
            None,
            &mvcc,
            &mut manifest,
            None,
        )
        .unwrap();

        let seg_dir = handle.segment_dir();

        // deletion.bitmap must exist in segment directory
        let bitmap_path = seg_dir.join("deletion.bitmap");
        assert!(
            bitmap_path.exists(),
            "deletion.bitmap should be created during warm transition"
        );

        // Must deserialize to an empty RoaringBitmap
        let data = std::fs::read(&bitmap_path).unwrap();
        let bitmap = RoaringBitmap::deserialize_from(&data[..]).unwrap();
        assert!(
            bitmap.is_empty(),
            "fresh warm segment deletion bitmap should be empty"
        );
    }

    #[test]
    fn test_transition_writes_file_create_wal_record() {
        use crate::persistence::wal_v3::record::{WalRecordType, read_wal_v3_record};
        use crate::persistence::wal_v3::segment::{WAL_V3_HEADER_SIZE, WalSegment, WalWriterV3};

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let wal_dir = shard_dir.join("wal_v3");
        let mut wal = WalWriterV3::new(0, &wal_dir, 16 * 1024 * 1024).unwrap();

        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let codes = vec![0xAAu8; 500];
        let graph = vec![0xBBu8; 200];
        let mvcc = vec![0u8; 24 * 5];

        let _handle = transition_to_warm(
            &shard_dir,
            10,
            1000,
            &codes,
            &graph,
            None,
            &mvcc,
            &mut manifest,
            Some(&mut wal),
        )
        .unwrap();

        // Read back the WAL segment and verify FileCreate record exists
        let seg_path = WalSegment::segment_path(&wal_dir, wal.current_segment_sequence());
        let data = std::fs::read(&seg_path).unwrap();
        assert!(data.len() > WAL_V3_HEADER_SIZE, "WAL should have records");

        // Parse records after header to find FileCreate
        let mut offset = WAL_V3_HEADER_SIZE;
        let mut found_file_create = false;
        while offset < data.len() {
            if let Some(record) = read_wal_v3_record(&data[offset..]) {
                if record.record_type == WalRecordType::FileCreate {
                    found_file_create = true;
                    // Verify payload is a serialized FileEntry (48 bytes)
                    assert_eq!(record.payload.len(), FileEntry::SIZE);
                    let fe = FileEntry::read_from(&record.payload).unwrap();
                    assert_eq!(fe.file_id, 1000);
                    assert_eq!(fe.tier, StorageTier::Warm);
                    assert_eq!(fe.status, FileStatus::Active);
                    break;
                }
                let record_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += record_len;
            } else {
                break;
            }
        }
        assert!(found_file_create, "FileCreate WAL record should be present");
    }
}
