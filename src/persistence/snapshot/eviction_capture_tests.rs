//! moon#1257 — eviction under an armed snapshot epoch.
//!
//! Eviction takes victims out of the hot table the snapshot walks, outside
//! `command::dispatch`, so no dispatch hook captures them. Before the fix a
//! victim in a range the walk had not written yet was simply missing from the
//! file (the unit proof in the issue: 1,438 of 1,501 evicted keys). Every
//! test here arms an epoch, lets the walk write part of the keyspace, evicts,
//! finishes the epoch and reads the file back as raw records: it must be
//! exactly the epoch-start keyspace, whatever eviction removed meanwhile.
//!
//! The databases carry their shard-slot `db_index`, as the shard's own do:
//! the eviction capture files each pre-image under it.

use bytes::Bytes;

use super::epoch_harness::{Divergence, Epoch, Record, diverge, string_keyspace, string_of};
use super::*;
use crate::config::RuntimeConfig;
use crate::persistence::snapshot_cow;
use crate::storage::eviction::{EvictionRun, evict_to_budget};

fn preload(db: &mut Database, prefix: &str, n: u32) {
    for i in 0..n {
        db.set_string(
            format!("{prefix}:{i:06}").as_bytes(),
            Bytes::from(format!("{prefix}-{i:06}-{}", "v".repeat(64))),
        );
    }
}

/// `n` databases, each stamped with its slot like `shard::Shard` does.
fn slots(n: usize) -> Vec<Database> {
    (0..n)
        .map(|i| {
            let mut db = Database::new();
            db.db_index = i;
            db
        })
        .collect()
}

/// A one-shard budget that is half of `db`'s memory now.
fn half_budget(db: &Database, policy: &str, appendonly: &str) -> RuntimeConfig {
    RuntimeConfig {
        maxmemory: db.estimated_memory() / 2,
        maxmemory_policy: policy.to_string(),
        appendonly: appendonly.to_string(),
        num_shards: 1,
        ..RuntimeConfig::default()
    }
}

/// Epoch-start keys of `expected` that are no longer in `dbs` (evicted).
fn evicted(
    expected: &std::collections::BTreeMap<(usize, Vec<u8>), Vec<u8>>,
    dbs: &[Database],
) -> usize {
    expected
        .keys()
        .filter(|(db, key)| dbs[*db].data().get(key).is_none())
        .count()
}

/// The issue's test: arm an epoch, write part of db 0, evict both databases
/// under `allkeys-random` down to half their memory. The file holds every
/// epoch-start key once, with its epoch-start value.
///
/// Red before the fix: `Divergence { missing: <the evicted keys whose range
/// was still pending>, .. }`.
#[test]
fn plain_eviction_mid_epoch_keeps_the_epoch_start_image() {
    let mut dbs = slots(2);
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 400);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    for _ in 0..3 {
        assert!(!epoch.tick_one(&dbs), "fixture: the walk finished early");
    }
    for db in 0..2 {
        let config = half_budget(&dbs[db], "allkeys-random", "no");
        evict_to_budget(&mut dbs[db], &config, EvictionRun::plain()).expect("evicts to budget");
    }
    let gone = evicted(&expected, &dbs);
    assert!(gone >= 900, "fixture: only {gone} keys were evicted");

    let records = epoch.finish(&dbs);
    assert_eq!(
        diverge(&expected, &records),
        Divergence::default(),
        "{gone} keys evicted mid-epoch"
    );
}

/// Every policy family and the drop-before-expiry path go through the same
/// removal: `volatile-ttl` picks keys with a TTL, `allkeys-lru` samples.
#[test]
fn every_policy_captures_its_victims() {
    for policy in [
        "allkeys-lru",
        "allkeys-lfu",
        "volatile-random",
        "volatile-ttl",
    ] {
        let mut dbs = slots(1);
        preload(&mut dbs[0], "p", 1200);
        // Far-future TTLs so the volatile policies have victims and nothing
        // expires during the test.
        let now = dbs[0].now_ms();
        for i in 0..1200u32 {
            let key = format!("p:{i:06}");
            dbs[0].set_expiry(key.as_bytes(), now + 3_600_000 + u64::from(i));
        }
        let expected = string_keyspace(&dbs);
        let mut epoch = Epoch::begin(&dbs);
        assert!(!epoch.tick_one(&dbs));
        let config = half_budget(&dbs[0], policy, "no");
        evict_to_budget(&mut dbs[0], &config, EvictionRun::plain()).expect("evicts to budget");
        let gone = evicted(&expected, &dbs);
        assert!(gone >= 300, "{policy}: only {gone} keys were evicted");
        let records = epoch.finish(&dbs);
        assert_eq!(
            diverge(&expected, &records),
            Divergence::default(),
            "{policy}: {gone} keys evicted mid-epoch"
        );
    }
}

/// A SWAPDB during the epoch moves the tables between slots. Eviction files
/// its pre-images by the SLOT the victim's database is in (`db_index`, kept
/// per slot by `swap_contents`), and the capture maps the slot to the epoch
/// database whose table is there now. The file still holds each database's
/// epoch-start keys in that database.
#[test]
fn eviction_after_a_swapdb_files_pre_images_under_the_right_database() {
    let mut dbs = slots(2);
    preload(&mut dbs[0], "a", 900);
    preload(&mut dbs[1], "b", 900);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));

    // `SWAPDB 0 1`, the way `ShardDbSet::swap` does it.
    snapshot_cow::note_swapdb(0, 1);
    let (left, right) = dbs.split_at_mut(1);
    crate::shard::db_plane::swap_contents(&mut left[0], &mut right[0]);
    assert_eq!(
        (dbs[0].db_index, dbs[1].db_index),
        (0, 1),
        "slots keep their index"
    );

    // Slot 0 now holds epoch database 1's table ("b:" keys), and slot 1 db 0's.
    for slot in 0..2 {
        let config = half_budget(&dbs[slot], "allkeys-random", "no");
        evict_to_budget(&mut dbs[slot], &config, EvictionRun::plain()).expect("evicts");
    }
    let records = epoch.finish(&dbs);
    assert_eq!(diverge(&expected, &records), Divergence::default());
}

/// The issue's loss trace, with an injected write failure: under
/// `--appendonly yes` an async spill takes the victim out of the hot table
/// at once and hands it to the spill thread. Here every spill write fails
/// (the shard directory is a regular file), and the failure path re-inserts
/// the value, exactly as `persistence_tick::apply_completion_vec` does, but
/// only after the walk has passed the victims' range.
///
/// Before the fix none of those victims reached the file: not at the walk
/// (they were in flight, which the walk does not serialize) and not after
/// (the re-insert lands behind the cursor). Now each victim's pre-image is
/// captured before it leaves the table.
#[test]
fn a_failed_async_spill_during_the_epoch_keeps_its_victims_in_the_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Injected failure: the shard "directory" is a regular file, so every
    // spill file create fails (root included).
    let shard_dir = tmp.path().join("shard-0");
    std::fs::write(&shard_dir, b"not a directory").expect("create the blocker");

    let mut dbs = slots(1);
    preload(&mut dbs[0], "s", 1500);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));

    let (tx, rx) = flume::unbounded();
    let mut next_file_id = 1u64;
    let config = half_budget(&dbs[0], "allkeys-random", "yes");
    evict_to_budget(
        &mut dbs[0],
        &config,
        EvictionRun::async_spill(&tx, &shard_dir, &mut next_file_id, 0, None),
    )
    .expect("the async spill takes victims");
    let mut requests: Vec<_> = rx.try_iter().collect();
    assert!(
        requests.len() >= 100,
        "fixture: only {} victims were spilled",
        requests.len()
    );
    let spilled = requests.len();

    // The walk passes every victim's range while their writes are in flight.
    while !epoch.tick(&dbs) {}

    // The writes fail; the failure path re-inserts each value hot.
    let completions = crate::storage::tiered::spill_thread::flush_buffer(&mut requests);
    let mut reinserted = 0;
    for c in completions {
        assert!(!c.success, "the injected write failure did not fail");
        let req = c
            .failed_request
            .expect("a failed completion carries its request");
        let db = &mut dbs[req.db_index];
        assert!(db.spill_inflight_is_newest(&req.key, req.file_id));
        db.spill_inflight_clear(&req.key, req.file_id);
        let entry = crate::storage::eviction::rehydrate_spill_payload(
            req.value_type,
            &req.value_bytes,
            req.ttl_ms,
        )
        .expect("rehydrates");
        db.set(&req.key, entry);
        reinserted += 1;
    }
    assert_eq!(reinserted, spilled);
    assert_eq!(
        string_keyspace(&dbs),
        expected,
        "the live keyspace is whole again"
    );

    let records = epoch.finish(&dbs);
    assert_eq!(
        diverge(&expected, &records),
        Divergence::default(),
        "{spilled} victims spilled mid-epoch, their writes failed"
    );
}

/// No save running: eviction captures nothing (the only cost is one
/// thread-local `bool` load per victim).
#[test]
fn eviction_with_no_save_running_captures_nothing() {
    snapshot_cow::disarm();
    let mut dbs = slots(1);
    preload(&mut dbs[0], "n", 600);
    let config = half_budget(&dbs[0], "allkeys-random", "no");
    evict_to_budget(&mut dbs[0], &config, EvictionRun::plain()).expect("evicts");
    assert!(snapshot_cow::pending_for_test().is_empty());
    assert!(snapshot_cow::pending_tombstones_for_test().is_empty());
}

/// A hash of `fields` fields (well past `LAZY_FREE_THRESHOLD`).
fn big_hash(fields: usize) -> Entry {
    let mut h = std::collections::HashMap::new();
    for i in 0..fields {
        h.insert(
            Bytes::from(format!("field-{i:06}")),
            Bytes::from_static(b"value-bytes-0123456789"),
        );
    }
    let mut e = Entry::new_string(Bytes::new());
    e.value = crate::storage::compact_value::CompactValue::from_redis_value(
        crate::storage::entry::RedisValue::Hash(Box::new(h)),
    );
    e
}

/// The file's `big` record(s) as field counts, and its other records.
fn split_big(records: Vec<Record>) -> (Vec<usize>, Vec<Record>) {
    let (big, rest): (Vec<_>, Vec<_>) = records.into_iter().partition(|(_, k, _)| k == "big");
    let lens = big
        .iter()
        .map(|(_, _, e)| match e.value.as_redis_value() {
            crate::storage::compact_value::RedisValueRef::Hash(m) => m.len(),
            _ => panic!("big loaded as a non-hashtable value"),
        })
        .collect();
    (lens, rest)
}

/// `db` with 300 small strings and a 5,000-field hash `big`, the only key
/// with a TTL, and a budget one byte under its memory: `volatile-lru`
/// evicts exactly `big`.
fn big_victim_fixture() -> (Vec<Database>, RuntimeConfig) {
    let mut dbs = slots(1);
    preload(&mut dbs[0], "s", 300);
    dbs[0].set(b"big", big_hash(5_000));
    let now = dbs[0].now_ms();
    dbs[0].set_expiry(b"big", now + 3_600_000);
    let config = RuntimeConfig {
        maxmemory: dbs[0].estimated_memory() - 1,
        maxmemory_policy: "volatile-lru".to_string(),
        appendonly: "no".to_string(),
        num_shards: 1,
        ..RuntimeConfig::default()
    };
    (dbs, config)
}

/// Review F1: evicting a large collection during a save MOVES the removed
/// entry into the pre-image. No deep clone on the shard thread, no second
/// copy on the lazy-free queue, the ledger still credited at once — and the
/// file still holds the victim, exactly once, as it was at epoch start.
///
/// Red before the fix: one clone (the victim was captured by copy, then
/// removed) and one lazy-free item (the original, freed separately).
#[test]
fn a_large_victim_is_moved_into_its_pre_image_not_cloned() {
    let (mut dbs, config) = big_victim_fixture();
    let mut expected = string_keyspace(&dbs[..0]);
    for (k, e) in dbs[0].data().iter() {
        if k.as_bytes() != b"big" {
            expected.insert((0, k.as_bytes().to_vec()), string_of(e));
        }
    }
    let epoch = Epoch::begin(&dbs);
    let clones = snapshot_cow::pre_image_clones_for_test();
    let before = dbs[0].estimated_memory();

    evict_to_budget(&mut dbs[0], &config, EvictionRun::plain()).expect("evicts to budget");

    assert!(dbs[0].data().get(b"big").is_none(), "fixture: big stayed");
    assert_eq!(dbs[0].data().len(), 300, "fixture: only big is volatile");
    assert_eq!(
        snapshot_cow::pre_image_clones_for_test() - clones,
        0,
        "the victim was deep-cloned into its pre-image"
    );
    assert_eq!(
        dbs[0].lazy_free_len(),
        0,
        "the victim is held by the snapshot AND queued for lazy free"
    );
    assert!(
        before - dbs[0].estimated_memory() > 5_000 * 32,
        "the ledger was not credited: {before} -> {}",
        dbs[0].estimated_memory()
    );
    let held: Vec<_> = snapshot_cow::pending_for_test()
        .into_iter()
        .map(|(db, k, _)| (db, k))
        .collect();
    assert_eq!(held, vec![(0, Bytes::from_static(b"big"))]);

    let (big, rest) = split_big(epoch.finish(&dbs));
    assert_eq!(big, vec![5_000], "the victim's epoch-start image, once");
    assert_eq!(diverge(&expected, &rest), Divergence::default());
}

/// First capture wins: a victim a write already captured this epoch is not
/// held again. The eviction frees it as it always did (lazily, it is large),
/// and the file holds the FIRST capture — the epoch-start value, not the
/// one the write made.
#[test]
fn a_victim_captured_earlier_is_freed_as_usual() {
    let (mut dbs, config) = big_victim_fixture();
    let epoch = Epoch::begin(&dbs);
    let clones = snapshot_cow::pre_image_clones_for_test();
    super::epoch_harness::run(&mut dbs, 0, &[b"HSET", b"big", b"new-field", b"x"]);
    assert_eq!(snapshot_cow::pre_image_clones_for_test() - clones, 1);

    evict_to_budget(&mut dbs[0], &config, EvictionRun::plain()).expect("evicts to budget");

    assert!(dbs[0].data().get(b"big").is_none(), "fixture: big stayed");
    assert_eq!(snapshot_cow::pre_image_clones_for_test() - clones, 1);
    assert_eq!(dbs[0].lazy_free_len(), 1, "the victim went to lazy free");
    let (big, _) = split_big(epoch.finish(&dbs));
    assert_eq!(big, vec![5_000], "the epoch-start image, not the HSET's");
    dbs[0].drain_lazy_free_elements(usize::MAX);
}
