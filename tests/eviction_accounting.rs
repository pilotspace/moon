//! Eviction accounting invariants (moon#585, moon#466).
//!
//! These live in their own integration-test binary on purpose: the counters
//! under test (`evicted_keys`, `spilled_keys`) are process-global atomics, so
//! a delta measured inside `cargo test`'s shared lib-test process would race
//! every other test that evicts. One binary, one file-local mutex, honest
//! deltas.
//!
//! # The invariant
//!
//! `evicted_keys` counts keys that **left the keyspace** to free memory —
//! exactly the keys `DBSIZE` stops counting. A key the tiering plane moved
//! from RAM to disk did NOT leave the keyspace (it is still readable, still
//! in `DBSIZE`, per moon#355) and is therefore NOT an eviction; it is a
//! spill.
//!
//! Before moon#585 the durable-batch spill path incremented `evicted_keys`
//! for every key it tiered, so an operator watching a moon with
//! `--disk-offload enable` saw `evicted_keys` climb by hundreds of thousands
//! while `DBSIZE` never moved — the exact contradiction reported in #585
//! (456,018 "evicted", `DBSIZE` unchanged).

use std::sync::Mutex;

use bytes::Bytes;
use moon::admin::metrics_setup::{evicted_keys, spilled_keys};
use moon::config::RuntimeConfig;
use moon::persistence::manifest::ShardManifest;
use moon::storage::db::Database;
use moon::storage::eviction::{EvictionRun, SpillContext, evict_to_budget};
use moon::storage::tiered::cold_index::ColdIndex;

/// Serialises every test that measures a global counter delta.
static COUNTER_LOCK: Mutex<()> = Mutex::new(());

const VALUE_LEN: usize = 4096;
const KEY_COUNT: usize = 64;

fn config(maxmemory: usize, policy: &str) -> RuntimeConfig {
    RuntimeConfig {
        maxmemory,
        num_shards: 1,
        maxmemory_policy: policy.to_string(),
        ..Default::default()
    }
}

fn filled_db(with_cold_index: bool) -> Database {
    let mut db = Database::new();
    if with_cold_index {
        db.cold_index = Some(ColdIndex::new());
    }
    for i in 0..KEY_COUNT {
        db.set_string(
            &Bytes::from(format!("key:{i:04}")),
            Bytes::from(vec![b'v'; VALUE_LEN]),
        );
    }
    db
}

/// moon#585 (RED before the fix): a key the durable batch spiller TIERS is
/// still in the keyspace, so it must not be counted as an eviction.
///
/// This is the reported bug's mechanism: `evicted_keys` grew by the number of
/// tiered keys while `DBSIZE` (`logical_len`) stayed flat, because the two
/// numbers were counting different things.
#[test]
fn tiered_victims_are_spills_not_evictions() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut next_file_id = 1u64;

    let mut db = filled_db(true);
    let keys_before = db.logical_len();
    assert_eq!(keys_before, KEY_COUNT);

    let evicted_before = evicted_keys();
    let spilled_before = spilled_keys();

    let cfg = config(1, "allkeys-lru");
    let mut ctx = SpillContext {
        shard_dir,
        manifest: &mut manifest,
        next_file_id: &mut next_file_id,
        db_index: 0,
    };
    let _ = evict_to_budget(&mut db, &cfg, EvictionRun::sync_spill(Some(&mut ctx)));

    let tiered = db.cold_index.as_ref().map(ColdIndex::len).unwrap_or(0);
    assert!(tiered > 0, "the batch spiller must have tiered something");
    assert!(db.len() < keys_before, "hot RAM must have been reclaimed");

    assert_eq!(
        db.logical_len(),
        keys_before,
        "a tiered key is still a key: DBSIZE must not move"
    );
    assert_eq!(
        evicted_keys() - evicted_before,
        0,
        "no key left the keyspace, so evicted_keys must not move — this is \
         moon#585: DBSIZE stayed flat while evicted_keys climbed by the \
         number of TIERED keys"
    );
    assert_eq!(
        spilled_keys() - spilled_before,
        tiered as u64,
        "tiered keys are counted as spills instead, so an operator can still \
         see the tiering activity"
    );
}

/// moon#585 (regression lock, GREEN before and after): a plain-dropped victim
/// really does leave the keyspace, so `DBSIZE` must fall by exactly the
/// `evicted_keys` delta. This is the half that always worked; it must keep
/// working.
#[test]
fn plain_dropped_victims_decrement_dbsize_by_exactly_evicted_keys() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let mut db = filled_db(false);
    let keys_before = db.logical_len();
    let evicted_before = evicted_keys();
    let spilled_before = spilled_keys();

    let cfg = config(VALUE_LEN * 8, "allkeys-lru");
    let _ = evict_to_budget(&mut db, &cfg, EvictionRun::plain());

    let evicted = evicted_keys() - evicted_before;
    assert!(evicted > 0, "eviction must have made progress");
    assert_eq!(
        db.logical_len(),
        keys_before - evicted as usize,
        "DBSIZE must fall by exactly the number of keys reported evicted"
    );
    assert_eq!(
        spilled_keys() - spilled_before,
        0,
        "a plain drop is not a spill"
    );
}

/// moon#466 (RED before the fix): a victim queued for async spill is still
/// resident RAM — the `SpillRequest` and the in-flight plane both pin a full
/// copy of the value — so `used_memory` must keep counting it until the
/// completion lands.
#[test]
fn pending_spill_bytes_stay_charged_to_used_memory() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut next_file_id = 1u64;
    // Never drained: every payload stays pinned for the whole test.
    let (tx, _rx) = flume::bounded(KEY_COUNT * 4);

    let mut db = filled_db(true);
    let mut cfg = config(1, "allkeys-lru");
    cfg.appendonly = "yes".to_string();

    let _ = evict_to_budget(
        &mut db,
        &cfg,
        EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None),
    );

    let in_flight = tx.len();
    assert!(in_flight > 0, "victims must have been queued for spill");
    assert_eq!(db.len(), 0, "hot entries were freed");

    assert!(
        db.pending_spill_bytes() >= in_flight * VALUE_LEN,
        "the queued payloads are resident RAM: pending_spill_bytes {} must \
         cover {} in-flight values of {VALUE_LEN} bytes",
        db.pending_spill_bytes(),
        in_flight
    );
    assert!(
        db.estimated_memory() >= in_flight * VALUE_LEN,
        "used_memory must count RAM that has not actually been released \
         (moon#466): estimated_memory {} vs {} in-flight bytes",
        db.estimated_memory(),
        in_flight * VALUE_LEN
    );
}

/// A FLUSH is a bulk DEL, so it must retire the in-flight spill records too:
/// otherwise a spill that lands afterwards resurrects a flushed key into the
/// cold index, and — with moon#466's charge in place — `used_memory` keeps
/// reporting bytes for a database the operator just emptied.
#[test]
fn flush_retires_in_flight_spills_and_their_byte_charge() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut next_file_id = 1u64;
    let (tx, _rx) = flume::bounded(KEY_COUNT * 4);

    let mut db = filled_db(true);
    let mut cfg = config(1, "allkeys-lru");
    cfg.appendonly = "yes".to_string();
    let _ = evict_to_budget(
        &mut db,
        &cfg,
        EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None),
    );
    assert!(
        db.pending_spill_bytes() > 0,
        "setup: something is in flight"
    );

    db.clear();

    assert_eq!(db.logical_len(), 0, "FLUSH must empty every plane");
    assert_eq!(
        db.pending_spill_bytes(),
        0,
        "the pending-byte charge must go with the records"
    );
    assert_eq!(
        db.estimated_memory(),
        0,
        "used_memory must read 0 after FLUSH"
    );
}

/// moon#466 pairing rule (must be GREEN before AND after): charging pending
/// bytes to `used_memory` must NOT turn `evict_to_budget` into a runaway loop
/// that drains the whole database chasing memory it has already scheduled for
/// release. Once the pending bytes cover the overshoot, the tick is done.
#[test]
fn eviction_stops_once_pending_bytes_cover_the_deficit() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut next_file_id = 1u64;
    let (tx, _rx) = flume::bounded(KEY_COUNT * 4);

    let mut db = filled_db(true);
    // Overshoot the budget by roughly two entries' worth.
    let budget = db.estimated_memory() - VALUE_LEN * 2;
    let mut cfg = config(budget, "allkeys-lru");
    cfg.appendonly = "yes".to_string();

    let res = evict_to_budget(
        &mut db,
        &cfg,
        EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None),
    );
    assert!(res.is_ok(), "a small deficit must be satisfiable");

    let taken = KEY_COUNT - db.len();
    assert!(
        taken <= 8,
        "a ~2-entry deficit must cost a handful of victims, not the whole \
         database — {taken} of {KEY_COUNT} keys were taken, which is the \
         runaway moon#466 warns about"
    );
    assert_eq!(
        db.logical_len(),
        KEY_COUNT,
        "in-flight keys are still keys: DBSIZE must not move"
    );
}
