//! RED tests for P9 — cold-tier orphan sweeper.
//!
//! These tests verify that `ColdIndex::orphan_sweep` correctly identifies and
//! removes cold-tier files whose keys have been overwritten by hot writes,
//! while preserving cold-only entries that have no corresponding hot copy.

use bytes::Bytes;
use moon::persistence::manifest::ShardManifest;
use moon::storage::db::Database;
use moon::storage::entry::Entry;
use moon::storage::tiered::cold_index::ColdIndex;
use moon::storage::tiered::kv_spill::spill_to_datafile;

fn make_string_entry(val: &'static str) -> Entry {
    Entry::new_string(Bytes::from_static(val.as_bytes()))
}

/// Scenario 1 (RED → GREEN): Key K1 is spilled to cold, then overwritten in hot.
/// After sweep, the cold-tier file should be removed and stats reflect it.
#[test]
fn test_orphan_sweep_removes_hot_shadowed_key() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let manifest_path = shard_dir.join("shard.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    // 1. Spill K1 to cold
    let mut cold_index = ColdIndex::new();
    spill_to_datafile(
        shard_dir,
        1,
        b"k1",
        &make_string_entry("cold_value"),
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();

    let cold_file = shard_dir.join("data/heap-000001.mpf");
    assert!(cold_file.exists(), "cold file should exist after spill");
    assert_eq!(cold_index.len(), 1);

    // 2. Hot DB contains K1 (simulates a SET that overwritten the cold entry)
    let mut db = Database::new();
    db.set(Bytes::from_static(b"k1"), make_string_entry("hot_value"));
    assert!(
        db.is_hot(b"k1"),
        "K1 should be hot after set"
    );

    // 3. Run orphan sweep — K1 is hot, so cold entry is orphan
    let stats = cold_index.orphan_sweep(&db, shard_dir, Some(&mut manifest)).unwrap();

    assert_eq!(stats.entries_reclaimed, 1, "one orphan entry should be reclaimed");
    assert!(stats.bytes_reclaimed > 0, "bytes reclaimed should be > 0");
    assert_eq!(cold_index.len(), 0, "cold index should be empty after sweep");
    assert!(
        !cold_file.exists(),
        "cold file should be deleted after orphan sweep"
    );
}

/// Scenario 2: Cold-only entry (no hot copy) MUST survive sweep.
/// This is the safety invariant — cold-only = live data, must not delete.
#[test]
fn test_orphan_sweep_preserves_cold_only_key() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let manifest_path = shard_dir.join("shard.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    // Spill K2 to cold (no hot write)
    let mut cold_index = ColdIndex::new();
    spill_to_datafile(
        shard_dir,
        2,
        b"k2",
        &make_string_entry("cold_only_value"),
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();

    let cold_file = shard_dir.join("data/heap-000002.mpf");
    assert!(cold_file.exists());

    // Hot DB does NOT contain K2
    let db = Database::new();
    assert!(!db.is_hot(b"k2"), "K2 should not be hot");

    // Sweep must preserve K2
    let stats = cold_index.orphan_sweep(&db, shard_dir, Some(&mut manifest)).unwrap();

    assert_eq!(stats.entries_reclaimed, 0, "cold-only entry must not be reclaimed");
    assert_eq!(stats.bytes_reclaimed, 0);
    assert_eq!(cold_index.len(), 1, "cold index must still contain K2");
    assert!(cold_file.exists(), "cold file for K2 must survive sweep");
}

/// Scenario 3: Mixed — K1 is hot (orphan), K2 is cold-only (live).
/// Only K1's cold entry should be removed.
#[test]
fn test_orphan_sweep_mixed_hot_and_cold_only() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let manifest_path = shard_dir.join("shard.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    let mut cold_index = ColdIndex::new();
    spill_to_datafile(
        shard_dir,
        10,
        b"k1",
        &make_string_entry("v1_cold"),
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();
    spill_to_datafile(
        shard_dir,
        11,
        b"k2",
        &make_string_entry("v2_cold"),
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();

    let file_k1 = shard_dir.join("data/heap-000010.mpf");
    let file_k2 = shard_dir.join("data/heap-000011.mpf");

    // K1 is hot (overwritten)
    let mut db = Database::new();
    db.set(Bytes::from_static(b"k1"), make_string_entry("v1_hot"));

    let stats = cold_index.orphan_sweep(&db, shard_dir, Some(&mut manifest)).unwrap();

    assert_eq!(stats.entries_reclaimed, 1);
    assert_eq!(cold_index.len(), 1, "only K2 should remain in cold index");
    assert!(!file_k1.exists(), "K1 cold file removed (orphan)");
    assert!(file_k2.exists(), "K2 cold file preserved (cold-only)");
}

/// Scenario 4: Empty cold index — sweep is a no-op.
#[test]
fn test_orphan_sweep_empty_cold_index() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let manifest_path = shard_dir.join("shard.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    let mut cold_index = ColdIndex::new();
    let db = Database::new();

    let stats = cold_index.orphan_sweep(&db, shard_dir, Some(&mut manifest)).unwrap();
    assert_eq!(stats.entries_reclaimed, 0);
    assert_eq!(stats.bytes_reclaimed, 0);
}

/// Scenario 5: Manifest tombstone is written for orphaned files.
#[test]
fn test_orphan_sweep_manifests_tombstone() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let manifest_path = shard_dir.join("shard.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    let mut cold_index = ColdIndex::new();
    spill_to_datafile(
        shard_dir,
        20,
        b"kx",
        &make_string_entry("val"),
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();

    // Confirm manifest has the active entry
    let before = manifest.files().iter().find(|f| f.file_id == 20).unwrap();
    assert_eq!(before.status, moon::persistence::manifest::FileStatus::Active);

    // Hot overwrite
    let mut db = Database::new();
    db.set(Bytes::from_static(b"kx"), make_string_entry("hot"));

    cold_index.orphan_sweep(&db, shard_dir, Some(&mut manifest)).unwrap();

    // Manifest entry should now be Tombstone
    let after = manifest.files().iter().find(|f| f.file_id == 20).unwrap();
    assert_eq!(
        after.status,
        moon::persistence::manifest::FileStatus::Tombstone,
        "manifest entry should be tombstoned after orphan sweep"
    );
}
