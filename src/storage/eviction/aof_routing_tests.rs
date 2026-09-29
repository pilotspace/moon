//! moon#1290 N7: the async-spill sink picks its path on whether an AOF
//! writer backs the process, not on the `appendonly` config string.
//!
//! `CONFIG SET appendonly yes` changes only `runtime_config.appendonly`
//! ("accepted but no live effect"). Routing on the string sent a no-AOF
//! process's victims to the `SpillThread`: out of RAM before any durable
//! copy, their manifest commit deferred, and no AOF to replay them — the
//! window the no-AOF durable path (and the moon#1281 graves trailer's
//! "no-AOF eviction is synchronous" premise) exists to close.

use bytes::Bytes;

use super::*;
use crate::persistence::manifest::{FileStatus, ShardManifest};
use crate::storage::tiered::cold_index::ColdIndex;

#[test]
fn config_set_appendonly_yes_without_a_writer_still_spills_durably() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut next_file_id = 1u64;
    let mut db = Database::new();
    db.cold_index = Some(ColdIndex::new());
    for i in 0..8 {
        db.set_string(format!("k:{i}").as_bytes(), Bytes::from(vec![b'v'; 64]));
    }
    // The operator ran `CONFIG SET appendonly yes`; no AOF writer exists.
    let config = RuntimeConfig {
        maxmemory: 1,
        maxmemory_policy: "allkeys-lru".to_string(),
        maxmemory_samples: 5,
        num_shards: 1,
        appendonly: "yes".to_string(),
        ..Default::default()
    };
    force_aof_backstop(Some(false));
    let (tx, rx) = flume::bounded::<SpillRequest>(64);
    let mut dropped = 0usize;
    let result = evict_to_budget(
        &mut db,
        &config,
        EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, Some(&mut manifest))
            .report(&mut |_| dropped += 1),
    );
    force_aof_backstop(None);

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        rx.len(),
        0,
        "moon#1290 N7: victims went to the async spill thread with no AOF writer behind it"
    );
    assert_eq!(dropped, 0, "nothing is plain-dropped");
    let listed = manifest
        .files()
        .iter()
        .filter(|e| e.status == FileStatus::Active)
        .count();
    assert!(
        listed > 0,
        "the victims' file is durably listed before they leave RAM"
    );
    let cold = db.cold_index.as_ref().unwrap();
    let spilled = (0..8)
        .filter(|i| cold.lookup(format!("k:{i}").as_bytes()).is_some())
        .count();
    assert!(
        spilled > 0 && spilled + db.len() == 8,
        "every victim is cold-readable"
    );
}

/// The floor only ever raises a small deficit, and never past 1/16 of the
/// target or [`DURABLE_BATCH_MIN_BYTES`].
#[test]
fn a_durable_batch_covers_many_writes_not_one() {
    assert_eq!(durable_batch_bytes(600, 2 << 20), 128 * 1024);
    assert_eq!(durable_batch_bytes(600, 1 << 30), DURABLE_BATCH_MIN_BYTES);
    assert_eq!(durable_batch_bytes(1 << 20, 2 << 20), 1 << 20);
    assert_eq!(durable_batch_bytes(10, 0), 10);
}
