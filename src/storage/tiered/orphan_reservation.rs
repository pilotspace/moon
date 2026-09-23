//! Keep a crash-orphaned spill file's id reserved after the boot sweep
//! deletes the file (moon#1114).
//!
//! A spill publishes its batch, `apply_completion_vec` appends
//! `MOON.SPILLED <N> key...` to the AOF, and only then runs one deferred
//! manifest commit for the batch. A crash in between leaves `heap-<N>.mpf` on
//! disk, the marker in the AOF, and no manifest entry for `N`. The next boot
//! classifies the file as a crash orphan and deletes it in the background
//! (`classify_orphan_heap_files`, task #55).
//!
//! Until that deletion the disk scan in `next_file_id_seed` keeps the counter
//! above `N`. After it, nothing does: a boot that allocated no higher id
//! leaves `N` in neither the manifest nor the disk, and the boot after resumes
//! the counter at `N`. The AOF generation still holds the old marker, which on
//! replay authorises the re-issued `N` early — a write logged before its key
//! was spilled into the NEW file `N` is applied on top of the value it already
//! produced (`RPUSH X a` reads `a a`, the #1067 outcome).
//!
//! [`reserve_orphan_high_water`] closes it with the manifest's own
//! high-water record: before any orphan is deleted, the highest orphan id is
//! committed to the manifest as an id reservation (a page-less Tombstone,
//! [`FileEntry::is_id_reservation`]). The seed takes the maximum over every
//! manifest entry, and `gc_tombstones` never prunes the tombstone holding the
//! highest id (moon#1067), so the reservation outlives the file for as long
//! as it matters.
//!
//! - **One id, one commit.** Only the highest orphan id needs recording — the
//!   seed is a maximum, so every lower orphan id is covered by it — and only
//!   when it is above every id the manifest already lists. The manifest grows
//!   by at most one entry per boot whatever the orphan backlog (~59K files in
//!   the G2 production bench).
//! - **Commit, then unlink.** Recovery calls this synchronously, before the
//!   shard serves and before the background sweep is started; on a commit
//!   error the caller must not delete anything (the disk scan then keeps the
//!   seed safe, and the next boot retries).
//! - **Idempotent across a crash in between.** A reservation is not a
//!   registration: `classify_orphan_heap_files` still classifies the file as
//!   an orphan on the next boot, and this function finds its id already at
//!   the manifest maximum and commits nothing.
//! - **No shard-thread fsync.** Recovery runs before the event loop exists
//!   (task #59 moved the event loop's manifest fsyncs off-thread; this adds
//!   none there).
//!
//! [`FileEntry::is_id_reservation`]: crate::persistence::manifest::FileEntry::is_id_reservation

use std::path::{Path, PathBuf};

use crate::persistence::manifest::ShardManifest;

/// The id of a crash-orphan path as classified by
/// `classify_orphan_heap_files`: `heap-{id}.mpf` or `heap-{id}.tmp`. Both
/// count: `next_file_id_seed` counts both, so deleting either can lower it.
fn orphan_heap_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix("heap-")?;
    rest.strip_suffix(".mpf")
        .or_else(|| rest.strip_suffix(".tmp"))?
        .parse()
        .ok()
}

/// Before the crash-orphan sweep deletes `orphans`, durably record the
/// highest of their ids in `manifest` if it is above every id the manifest
/// lists. Returns the id reserved, or `None` when nothing needed recording.
///
/// Commits with [`ShardManifest::commit`] (fsync'd) when it records anything.
///
/// # Errors
///
/// The commit's I/O error. The caller must then delete NONE of `orphans`:
/// while the files stay on disk the seed's disk scan still clears their ids.
pub fn reserve_orphan_high_water(
    manifest: &mut ShardManifest,
    orphans: &[PathBuf],
) -> std::io::Result<Option<u64>> {
    let Some(orphan_max) = orphans.iter().filter_map(|p| orphan_heap_id(p)).max() else {
        return Ok(None);
    };
    let manifest_max = manifest.files().iter().map(|e| e.file_id).max();
    if manifest_max.is_some_and(|m| m >= orphan_max) {
        return Ok(None);
    }
    if !manifest.reserve_retired_id(orphan_max) {
        // Unreachable: an entry for this id would have been >= orphan_max.
        return Ok(None);
    }
    manifest.commit()?;
    Ok(Some(orphan_max))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
    use crate::persistence::page::PageType;
    use crate::storage::tiered::file_id_seed::next_file_id_seed;
    use crate::storage::tiered::kv_spill::classify_orphan_heap_files;
    use std::time::{Duration, Instant};

    fn active(file_id: u64) -> FileEntry {
        FileEntry {
            file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1,
            byte_size: 4096,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        }
    }

    /// A shard dir laid out as the seed reads it: `shard-0.manifest` listing
    /// `listed`, and `data/heap-{id}.mpf` for every id in `on_disk`.
    fn shard(listed: &[u64], on_disk: &[u64]) -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let mut m = ShardManifest::create(&dir.join("shard-0.manifest")).unwrap();
        for id in listed {
            m.add_file(active(*id)).unwrap();
        }
        m.commit().unwrap();
        for id in on_disk {
            std::fs::write(data.join(format!("heap-{id:06}.mpf")), [0u8; 16]).unwrap();
        }
        (tmp, dir)
    }

    fn open(dir: &Path) -> ShardManifest {
        ShardManifest::open(&dir.join("shard-0.manifest")).unwrap()
    }

    /// Stand-in for the background sweep.
    fn unlink_all(paths: &[PathBuf]) {
        for p in paths {
            std::fs::remove_file(p).unwrap();
        }
    }

    /// moon#1114: ids 1-2 listed; 5 is the unmanifested file a crash between
    /// the `MOON.SPILLED 5` marker and its manifest commit left behind.
    /// After the sweep deletes it, the next seed must still clear 5.
    #[test]
    fn swept_orphan_id_is_never_reissued() {
        let (_tmp, dir) = shard(&[1, 2], &[1, 2, 5]);
        assert_eq!(next_file_id_seed(&dir, 0).unwrap(), 6, "instrument");

        let mut m = open(&dir);
        let orphans = classify_orphan_heap_files(&dir, &m);
        assert_eq!(orphans, vec![dir.join("data/heap-000005.mpf")]);
        assert_eq!(
            reserve_orphan_high_water(&mut m, &orphans).unwrap(),
            Some(5)
        );
        unlink_all(&orphans);

        let reopened = open(&dir);
        let e = reopened
            .files()
            .iter()
            .find(|e| e.file_id == 5)
            .expect("the reservation must be durable before the unlink");
        assert!(e.is_id_reservation());
        assert_eq!(e.status, FileStatus::Tombstone);
        assert_eq!(
            next_file_id_seed(&dir, 0).unwrap(),
            6,
            "the seed moved back below the deleted orphan's id"
        );
    }

    /// A crash after the reservation commit and before the unlink: the next
    /// boot must still classify the file as an orphan (so it is reclaimed,
    /// not leaked) and must commit nothing new.
    #[test]
    fn crash_between_commit_and_unlink_is_idempotent() {
        let (_tmp, dir) = shard(&[1], &[1, 7]);
        let mut m = open(&dir);
        let orphans = classify_orphan_heap_files(&dir, &m);
        assert_eq!(
            reserve_orphan_high_water(&mut m, &orphans).unwrap(),
            Some(7)
        );
        drop(m); // crash: nothing unlinked

        let mut m = open(&dir);
        let epoch = m.epoch();
        let again = classify_orphan_heap_files(&dir, &m);
        assert_eq!(
            again, orphans,
            "a reservation must not register the file (it would leak forever)"
        );
        assert_eq!(reserve_orphan_high_water(&mut m, &again).unwrap(), None);
        assert_eq!(m.epoch(), epoch, "nothing to commit on the retry");
        assert_eq!(m.files().iter().filter(|e| e.file_id == 7).count(), 1);
        unlink_all(&again);
        assert_eq!(next_file_id_seed(&dir, 0).unwrap(), 8);
    }

    /// Only the highest orphan id is recorded, and only when it is above the
    /// manifest's maximum: a backlog of orphans costs at most one entry.
    #[test]
    fn records_one_entry_and_only_above_the_manifest_max() {
        let (_tmp, dir) = shard(&[10], &[3, 4, 10, 12, 13, 15]);
        std::fs::write(dir.join("data/heap-000020.tmp"), b"partial").unwrap();
        std::fs::write(dir.join("data/notes.txt"), b"keep").unwrap();
        let mut m = open(&dir);
        let orphans = classify_orphan_heap_files(&dir, &m);
        assert_eq!(orphans.len(), 6, "3,4,12,13,15 + the .tmp");
        let before = m.files().len();
        assert_eq!(
            reserve_orphan_high_water(&mut m, &orphans).unwrap(),
            Some(20),
            "the .tmp counts: the seed counts it too"
        );
        assert_eq!(m.files().len(), before + 1);

        // Orphans all below the manifest max: nothing to record.
        let (_tmp2, dir2) = shard(&[10], &[3, 10]);
        let mut m2 = open(&dir2);
        let orphans2 = classify_orphan_heap_files(&dir2, &m2);
        assert_eq!(reserve_orphan_high_water(&mut m2, &orphans2).unwrap(), None);
        assert_eq!(reserve_orphan_high_water(&mut m2, &[]).unwrap(), None);
    }

    /// The reservation is a tombstone: tombstone GC must keep it while it is
    /// the high-water mark (moon#1067), and may age it out once a higher id
    /// is listed.
    #[test]
    fn gc_keeps_the_reservation_while_it_is_the_high_water_mark() {
        let (_tmp, dir) = shard(&[1], &[1, 9]);
        let mut m = open(&dir);
        let orphans = classify_orphan_heap_files(&dir, &m);
        reserve_orphan_high_water(&mut m, &orphans).unwrap();
        let far = Instant::now() + Duration::from_secs(3600);
        m.commit().unwrap();
        m.commit().unwrap();
        assert_eq!(m.gc_tombstones(0, 0, far), 0);
        assert!(m.files().iter().any(|e| e.file_id == 9));

        m.add_file(active(11)).unwrap();
        assert_eq!(m.gc_tombstones(0, 0, far), 1);
        assert!(!m.files().iter().any(|e| e.file_id == 9));
    }
}
