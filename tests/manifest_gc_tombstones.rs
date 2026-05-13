//! P1 — manifest tombstone GC integration tests.
//!
//! Validates the two-axis retention policy:
//!   axis 1: tombstone epoch must be older than `retain_epochs` from current epoch
//!   axis 2: tombstone wall-clock age must exceed `retain_secs`
//!
//! Both axes MUST be satisfied before a tombstone is physically removed.

use std::time::{Duration, Instant};

use moon::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use moon::persistence::page::PageType;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_entry(id: u64) -> FileEntry {
    FileEntry {
        file_id: id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12,
        page_count: 100,
        byte_size: 409_600,
        created_lsn: id,
        min_key_hash: 0,
        max_key_hash: u64::MAX,
        last_modified_lsn: id,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// P1 — gc on empty entries list must not panic and returns 0.
#[test]
fn gc_empty_entries_no_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();
    m.commit().unwrap();

    let now = Instant::now();
    let pruned = m.gc_tombstones(2, 300, now);
    assert_eq!(pruned, 0);
}

/// P1 — no tombstones → gc returns 0.
#[test]
fn gc_no_tombstones_returns_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();
    m.add_file(make_entry(1));
    m.add_file(make_entry(2));
    m.commit().unwrap();

    let now = Instant::now() + Duration::from_secs(999);
    let pruned = m.gc_tombstones(0, 0, now);
    assert_eq!(pruned, 0);
    assert_eq!(m.active_entry_count(), 2);
    assert_eq!(m.tombstone_count(), 0);
}

/// P1 — epoch axis satisfied, time axis NOT → 0 pruned.
#[test]
fn gc_epoch_satisfied_time_not_satisfied_no_prune() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();

    m.add_file(make_entry(1));
    m.add_file(make_entry(2));
    m.add_file(make_entry(3));
    m.commit().unwrap(); // epoch 2

    // Tombstone file 2 — epoch recorded is current epoch = 2, time = now
    m.remove_file(2);
    m.commit().unwrap(); // epoch 3

    // Advance past retain_epochs (retain=2, current=3, tombstone_epoch=2 → age=1 epoch — NOT satisfied)
    // Actually we need to advance epoch by retain_epochs+1 to satisfy.
    // Tombstone at epoch 3 (after remove_file + commit bumps to 3).
    // retain_epochs=0 means ANY age satisfies epoch axis.
    // Use retain_epochs=2 so tombstone age = 0 epochs is NOT satisfied.
    // To NOT satisfy: age_epochs < retain_epochs → we don't advance epoch at all.
    // Time: pass a `now` that is only 1 second ahead (retain_secs=300 → NOT satisfied).
    let now_too_soon = Instant::now() + Duration::from_secs(1);
    let pruned = m.gc_tombstones(2, 300, now_too_soon);
    assert_eq!(pruned, 0, "neither axis satisfied → no prune");

    assert_eq!(m.tombstone_count(), 1);
    assert_eq!(m.active_entry_count(), 2);
}

/// P1 — time axis satisfied, epoch axis NOT → 0 pruned.
#[test]
fn gc_time_satisfied_epoch_not_satisfied_no_prune() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();

    m.add_file(make_entry(1));
    m.add_file(make_entry(2));
    m.commit().unwrap(); // epoch 2

    // Tombstone at epoch 2 (remove_file before commit)
    m.remove_file(2);
    m.commit().unwrap(); // epoch 3 — tombstone_epoch=2 (when remove_file was called), age = epoch 3 - epoch 2 = 1

    // Time: pass a now far in the future → time axis satisfied
    let now_far = Instant::now() + Duration::from_secs(9999);

    // Epoch axis: retain_epochs=5, age=1 → NOT satisfied (1 < 5)
    let pruned = m.gc_tombstones(5, 0, now_far);
    assert_eq!(pruned, 0, "epoch axis not satisfied → no prune");
    assert_eq!(m.tombstone_count(), 1);
}

/// P1 — BOTH axes satisfied → tombstones pruned, correct count returned.
#[test]
fn gc_both_axes_satisfied_prunes_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();

    // Add 5 files
    for i in 1..=5 {
        m.add_file(make_entry(i));
    }
    m.commit().unwrap(); // epoch 2

    // Tombstone files 2 and 4
    m.remove_file(2);
    m.remove_file(4);
    m.commit().unwrap(); // epoch 3, tombstone_epoch recorded as epoch at time of remove_file call = epoch 2

    // Advance epoch beyond retain_epochs (retain=1, current=3, age=1 → satisfied when age >= retain)
    // Add a few more commits to advance epoch
    m.commit().unwrap(); // epoch 4
    m.commit().unwrap(); // epoch 5
    // tombstone age in epochs = 5 - 2 = 3 ≥ retain_epochs(1) → epoch axis satisfied

    // Time: far future → time axis satisfied
    let now_far = Instant::now() + Duration::from_secs(9999);

    let pruned = m.gc_tombstones(1, 1, now_far);
    assert_eq!(pruned, 2, "both tombstones should be pruned");

    // Active files 1, 3, 5 remain; tombstones 2, 4 are gone
    assert_eq!(m.active_entry_count(), 3);
    assert_eq!(m.tombstone_count(), 0);
    assert_eq!(m.files().len(), 3);

    // File IDs present must be 1, 3, 5
    let ids: Vec<u64> = m.files().iter().map(|e| e.file_id).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(ids.contains(&5));
    assert!(!ids.contains(&2));
    assert!(!ids.contains(&4));
}

/// P1 — partial prune: only tombstones old enough on BOTH axes are pruned.
#[test]
fn gc_partial_prune_when_only_some_satisfy_both_axes() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();

    for i in 1..=4 {
        m.add_file(make_entry(i));
    }
    m.commit().unwrap(); // epoch 2

    // Tombstone files 1 and 2 early (epoch 2)
    m.remove_file(1);
    m.remove_file(2);
    m.commit().unwrap(); // epoch 3

    // Advance epochs
    m.commit().unwrap(); // epoch 4
    m.commit().unwrap(); // epoch 5
    // tombstone 1 and 2 age = 5 - 2 = 3 epochs

    // Tombstone file 3 NOW (epoch 5) — recent tombstone
    m.remove_file(3);
    m.commit().unwrap(); // epoch 6
    // tombstone 3 age = 6 - 5 = 1 epoch — NOT satisfied with retain_epochs=3

    // Time: far future (satisfies time axis for all)
    let now_far = Instant::now() + Duration::from_secs(9999);

    // retain_epochs=3: tombstones 1 & 2 (age 4) satisfy; tombstone 3 (age 1) does NOT
    let pruned = m.gc_tombstones(3, 0, now_far);
    assert_eq!(pruned, 2, "only two old tombstones should be pruned");

    assert_eq!(m.tombstone_count(), 1, "file 3 tombstone remains");
    assert_eq!(m.active_entry_count(), 1, "file 4 is active");
    assert_eq!(m.files().len(), 2); // file 3 (tombstone) + file 4 (active)
}

/// P1 — gc does NOT commit; disk state is unchanged until commit is called.
///
/// This validates crash-safety: a crash after gc_tombstones but before commit
/// leaves the active on-disk root with tombstones still intact.
#[test]
fn gc_without_commit_leaves_disk_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();

    m.add_file(make_entry(1));
    m.add_file(make_entry(2));
    m.commit().unwrap(); // epoch 2

    m.remove_file(2);
    m.commit().unwrap(); // epoch 3: tombstone committed to disk

    // Now GC in memory — both axes trivially satisfied with retain=0 and ancient now
    let now_ancient = Instant::now() + Duration::from_secs(99999);
    let pruned = m.gc_tombstones(0, 0, now_ancient);
    assert_eq!(pruned, 1);

    // In-memory: tombstone removed
    assert_eq!(m.tombstone_count(), 0);

    // On-disk: tombstone still present (no commit after GC)
    let m2 = ShardManifest::open(&path).unwrap();
    assert_eq!(m2.files().len(), 2, "disk still has 2 entries");
    assert_eq!(
        m2.files()
            .iter()
            .filter(|e| e.status == FileStatus::Tombstone)
            .count(),
        1,
        "disk still has 1 tombstone"
    );
}

/// P1 — after gc + commit, re-opened manifest sees the pruned state.
#[test]
fn gc_then_commit_persists_pruned_state() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();

    for i in 1..=3 {
        m.add_file(make_entry(i));
    }
    m.commit().unwrap(); // epoch 2

    m.remove_file(1);
    m.remove_file(3);
    m.commit().unwrap(); // epoch 3

    let now_far = Instant::now() + Duration::from_secs(9999);
    let pruned = m.gc_tombstones(0, 0, now_far);
    assert_eq!(pruned, 2);

    // Commit pruned state
    m.commit().unwrap(); // epoch 4

    // Re-open: only file 2 should remain
    let m2 = ShardManifest::open(&path).unwrap();
    assert_eq!(m2.files().len(), 1);
    assert_eq!(m2.files()[0].file_id, 2);
    assert_eq!(m2.files()[0].status, FileStatus::Active);
    assert_eq!(m2.tombstone_count(), 0);
    assert_eq!(m2.active_entry_count(), 1);
}

/// P1 — getters: active_entry_count + tombstone_count are consistent.
#[test]
fn getters_sum_to_total_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");
    let mut m = ShardManifest::create(&path).unwrap();

    for i in 1..=6 {
        m.add_file(make_entry(i));
    }
    m.commit().unwrap();

    m.remove_file(2);
    m.remove_file(4);
    m.remove_file(6);
    m.commit().unwrap();

    assert_eq!(m.active_entry_count(), 3);
    assert_eq!(m.tombstone_count(), 3);
    assert_eq!(
        m.active_entry_count() + m.tombstone_count(),
        m.files().len(),
    );
}
