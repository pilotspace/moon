//! HOT->WARM transition protocol for vector segments.
//!
//! Implements the staging-directory atomic transition: write .mpf files
//! to a staging directory, fsync each file, fsync the directory, update
//! manifest, rename staging to final, fsync parent.

use std::path::Path;

use crate::persistence::fsync::{fsync_directory, fsync_file};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::tiered::SegmentHandle;
use crate::vector::persistence::warm_segment::{
    write_codes_mpf, write_graph_mpf, write_mvcc_mpf, write_vectors_mpf,
};

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
) -> std::io::Result<SegmentHandle> {
    let vectors_dir = shard_dir.join("vectors");
    std::fs::create_dir_all(&vectors_dir)?;

    let staging = vectors_dir.join(format!(".segment-{segment_id}.staging"));
    let final_dir = vectors_dir.join(format!("segment-{segment_id}"));

    // Step 1: Create staging directory
    std::fs::create_dir_all(&staging)?;

    // Step 2: Write .mpf files to staging
    write_codes_mpf(&staging.join("codes.mpf"), file_id, codes_data)?;
    write_graph_mpf(&staging.join("graph.mpf"), file_id, graph_data)?;
    write_mvcc_mpf(&staging.join("mvcc.mpf"), file_id, mvcc_data)?;

    if let Some(vdata) = vectors_data {
        write_vectors_mpf(&staging.join("vectors.mpf"), file_id, vdata)?;
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

    manifest.add_file(FileEntry {
        file_id,
        file_type: PageType::VecCodes as u8,
        status: FileStatus::Active,
        tier: StorageTier::Warm,
        page_size_log2: 16, // 64KB
        page_count: codes_pages as u32,
        byte_size: codes_data.len() as u64,
        created_lsn: 0,
        min_key_hash: 0,
        max_key_hash: u64::MAX,
    });
    manifest.commit()?;

    // Step 6: Rename staging -> final
    std::fs::rename(&staging, &final_dir)?;

    // Step 7: Fsync parent directory
    fsync_directory(&vectors_dir)?;

    // Step 8: Return segment handle
    Ok(SegmentHandle::new(segment_id, final_dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::manifest::ShardManifest;

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
            &shard_dir, 1, 100, &codes, &graph, None, &mvcc, &mut manifest,
        )
        .unwrap();

        let seg_dir = handle.segment_dir();
        assert!(seg_dir.join("codes.mpf").exists());
        assert!(seg_dir.join("graph.mpf").exists());
        assert!(seg_dir.join("mvcc.mpf").exists());
        assert!(!seg_dir.join("vectors.mpf").exists()); // None passed
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
            &shard_dir, 2, 200, &codes, &graph, None, &mvcc, &mut manifest,
        )
        .unwrap();

        // Staging dir should not exist (renamed to final)
        let staging = shard_dir.join("vectors/.segment-2.staging");
        assert!(!staging.exists(), "staging directory should not remain after transition");
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
            &shard_dir, 3, 300, &codes, &graph, None, &mvcc, &mut manifest,
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
            &shard_dir, 5, 500, &codes, &graph, None, &mvcc, &mut manifest,
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
            &shard_dir, 6, 600, &codes, &graph, None, &mvcc, &mut manifest,
        )
        .unwrap();

        let seg_dir = handle.segment_dir().to_path_buf();
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();

        // Verify we can read back the codes data
        let cd = ws.codes_data(0);
        assert_eq!(&cd[..1000], &[0xAAu8; 1000]);
        assert_eq!(ws.page_count_codes(), 1);
    }
}
