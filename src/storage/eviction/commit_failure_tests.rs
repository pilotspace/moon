//! moon#1291 F9: a durable batch spill (`evict_batch_durable`) whose manifest
//! commit fails keeps its victims hot — and must not leave the spill file
//! behind as a listed file whose slots are neither indexed nor graved.
//!
//! Before the fix the `add_file` entry stayed in the in-memory root, the next
//! successful commit listed the file Active, and a boot rebuilt its slots as
//! live cold keys: a key DELeted while hot (no cold entry, so no grave) came
//! back after a restart.

use bytes::Bytes;

use super::*;
use crate::persistence::manifest::{FileStatus, ShardManifest};
use crate::storage::tiered::cold_index::ColdIndex;
use crate::storage::tiered::slot_graves;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        maxmemory: 1,
        maxmemory_policy: "allkeys-lru".to_string(),
        maxmemory_samples: 5,
        num_shards: 1,
        ..Default::default()
    }
}

#[test]
fn a_failed_spill_commit_never_leaves_a_listed_file_of_unindexed_slots() {
    slot_graves::force_enabled(Some(true));
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let manifest_path = shard_dir.join("shard.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();
    let mut next_file_id = 1u64;

    let mut db = Database::new();
    db.cold_index = Some(ColdIndex::new());
    for i in 0..8 {
        db.set_string(format!("k:{i}").as_bytes(), Bytes::from(vec![b'v'; 64]));
    }

    // The batch is written and fsynced, then its manifest commit fails.
    manifest.set_inject_persist_error(true);
    let reclaimed = evict_batch_durable(
        &mut db,
        &config(),
        &EvictionPolicy::AllKeysLru,
        shard_dir,
        &mut next_file_id,
        &mut manifest,
        0,
        usize::MAX,
        &mut |_| {},
    );
    manifest.set_inject_persist_error(false);
    assert_eq!(reclaimed, 0, "a failed commit spills nothing");
    assert_eq!(db.len(), 8, "every victim stays hot");
    assert!(next_file_id > 1, "precondition: a spill file was written");

    // Every victim is later DELeted while hot: no cold entry, no grave.
    for i in 0..8 {
        db.remove(format!("k:{i}").as_bytes());
    }
    // The next successful commit (any later spill or sweep does one).
    manifest.commit().unwrap();

    let listed_active: Vec<u64> = manifest
        .files()
        .iter()
        .filter(|e| e.status == FileStatus::Active)
        .map(|e| e.file_id)
        .collect();
    let graves = db.cold_index.as_ref().unwrap().slot_graves().len();

    // The restart: a rebuild from the manifest as it is on disk.
    let reopened = ShardManifest::open(&manifest_path).unwrap();
    let rebuilt = ColdIndex::rebuild_from_manifest_per_db(shard_dir, &reopened);
    let back: Vec<String> = (0..8)
        .filter(|i| {
            let key = format!("k:{i}");
            rebuilt
                .per_db
                .iter()
                .any(|(_, ci)| ci.lookup(key.as_bytes()).is_some())
        })
        .map(|i| format!("k:{i}"))
        .collect();
    slot_graves::force_enabled(None);

    assert!(
        listed_active.is_empty(),
        "moon#1291 F9: the next commit listed the failed batch's file(s) {listed_active:?} Active"
    );
    assert!(
        back.is_empty(),
        "moon#1291 F9: keys DELeted while hot came back from the failed batch's file: {back:?}"
    );
    assert!(
        graves >= 8,
        "moon#1291 F9: the failed batch's slots must be graves for the next snapshot \
         (in case the failed commit reached the disk), got {graves}"
    );
}
