//! moon#1221 review F1 (refs moon#1190): memory that UNLINK or active expiry
//! has already released must be visible to every maxmemory / OOM gate.
//!
//! A value handed to the lazy-free queue stays CHARGED in `used_memory`
//! until the drain frees it (see `db/lazy_free.rs`). Before this fix the
//! gates compared that ledger against the budget as if the bytes were live:
//! near `maxmemory` the first writes after an UNLINK of a large value evicted
//! live keys (`allkeys-*`), were refused with OOM (`noeviction`), or found no
//! victim at all (`volatile-*` with no TTL keys). On main (`32a7a02`) UNLINK
//! credited synchronously and every test here passes; on the PR head the
//! reviewer's three repros evicted 20 of 120 keys / answered OOM.
//!
//! Every gate is covered: the write-path `evict_to_budget`, the per-db
//! quota `check_db_maxmemory`, and the two whole-shard aggregate gates (the
//! 100 ms eviction tick and the memory-pressure cascade), which measure all
//! of a shard's databases as one total and evict db by db — so bytes queued
//! in db 1 must be reclaimed before db 0 loses a key.

use std::collections::HashMap;

use bytes::Bytes;

use crate::command::key;
use crate::config::RuntimeConfig;
use crate::protocol::Frame;
use crate::storage::compact_value::CompactValue;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, RedisValue, current_time_ms};
use crate::storage::eviction::{EvictionRun, evict_to_budget};

const LIVE: usize = 100;
const NEW: usize = 20;

fn bs(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

fn big_hash_entry(fields: usize, probe: Option<&Bytes>) -> Entry {
    let mut h = HashMap::new();
    for i in 0..fields {
        h.insert(
            Bytes::from(format!("field-{i:06}").into_bytes()),
            Bytes::from_static(b"value-bytes-0123456789"),
        );
    }
    if let Some(p) = probe {
        h.insert(Bytes::from_static(b"probe"), p.clone());
    }
    let mut e = Entry::new_string(Bytes::new());
    e.value = CompactValue::from_redis_value(RedisValue::Hash(Box::new(h)));
    e
}

fn small(v: u8) -> Entry {
    Entry::new_string(Bytes::from(vec![v; 256]))
}

fn live_key(i: usize) -> Vec<u8> {
    format!("live:{i:03}").into_bytes()
}

fn new_key(i: usize) -> Vec<u8> {
    format!("new:{i:03}").into_bytes()
}

/// A db sitting exactly at its budget: one large value (most of the bytes)
/// plus LIVE small keys.
fn at_budget(big: Entry) -> (Database, usize) {
    let mut db = Database::new();
    db.set(b"big", big);
    for i in 0..LIVE {
        db.set(&live_key(i), small(b'x'));
    }
    let budget = db.estimated_memory();
    (db, budget)
}

fn cfg(budget: usize, policy: &str) -> RuntimeConfig {
    RuntimeConfig {
        maxmemory: budget,
        maxmemory_policy: policy.to_string(),
        maxmemory_samples: 5,
        num_shards: 1,
        ..RuntimeConfig::default()
    }
}

/// Keys among the LIVE + NEW set still present.
fn survivors(db: &mut Database) -> usize {
    (0..LIVE)
        .filter(|&i| db.get(&live_key(i)).is_some())
        .count()
        + (0..NEW).filter(|&i| db.get(&new_key(i)).is_some()).count()
}

/// Write NEW small keys the way the write path does (write, then the gate).
fn write_and_count(db: &mut Database, cfg: &RuntimeConfig) -> usize {
    for i in 0..NEW {
        db.set(&new_key(i), small(b'y'));
        evict_to_budget(db, cfg, EvictionRun::plain()).expect("evictable");
    }
    survivors(db)
}

fn drain_all(db: &mut Database) {
    let mut steps = 0;
    while db.lazy_free_len() != 0 {
        db.drain_lazy_free_elements(1_000);
        steps += 1;
        assert!(steps < 100_000, "lazy-free drain made no progress");
    }
}

/// The reviewer's repro: UNLINK of the big hash released ~90% of the
/// budget; 20 small writes fit with room to spare. Nothing may be evicted.
#[test]
fn unlink_then_writes_do_not_evict_live_keys() {
    let (mut db, budget) = at_budget(big_hash_entry(5_000, None));
    let cfg = cfg(budget, "allkeys-random");
    assert_eq!(key::unlink(&mut db, &[bs(b"big")]), Frame::Integer(1));
    let right_after_unlink = db.estimated_memory();
    let survived = write_and_count(&mut db, &cfg);
    assert_eq!(
        survived,
        LIVE + NEW,
        "evicted {} live keys to make room for memory UNLINK already released \
         (budget {budget}, ledger right after UNLINK {right_after_unlink})",
        LIVE + NEW - survived
    );
}

/// Same under `noeviction`: the first write after the UNLINK must not be
/// refused with OOM.
#[test]
fn unlink_then_a_write_under_noeviction_is_not_oom() {
    let (mut db, budget) = at_budget(big_hash_entry(5_000, None));
    let cfg = cfg(budget, "noeviction");
    assert_eq!(key::unlink(&mut db, &[bs(b"big")]), Frame::Integer(1));
    db.set(b"new:0", small(b'y'));
    let r = evict_to_budget(&mut db, &cfg, EvictionRun::plain());
    assert!(
        r.is_ok(),
        "write after UNLINK refused ({r:?}): ledger {} vs budget {budget}",
        db.estimated_memory()
    );
}

/// `volatile-*` with no TTL keys has no victim at all: before the fix the
/// write after the UNLINK was refused with OOM for bytes already released.
#[test]
fn unlink_then_a_write_under_volatile_lru_without_ttl_keys_is_not_oom() {
    let (mut db, budget) = at_budget(big_hash_entry(5_000, None));
    let cfg = cfg(budget, "volatile-lru");
    assert!(db.unlink(b"big"));
    db.set(b"new:0", small(b'y'));
    let r = evict_to_budget(&mut db, &cfg, EvictionRun::plain());
    assert!(r.is_ok(), "write after UNLINK refused: {r:?}");
}

/// Active expiry of the big hash: same property.
#[test]
fn an_expired_big_value_then_writes_do_not_evict_live_keys() {
    let mut big = big_hash_entry(5_000, None);
    big.set_expires_at_ms(current_time_ms() - 1_000);
    let (mut db, budget) = at_budget(big);
    let cfg = cfg(budget, "allkeys-random");
    let mut removed = 0usize;
    crate::server::expiration::expire_cycle_direct(&mut db, &mut |_| removed += 1);
    assert_eq!(removed, 1, "the sweep must remove the expired hash");
    assert_eq!(db.lazy_free_len(), 1, "fixture: the value must be queued");
    let survived = write_and_count(&mut db, &cfg);
    assert_eq!(
        survived,
        LIVE + NEW,
        "evicted {} live keys after the big value expired",
        LIVE + NEW - survived
    );
}

/// The per-db quota gate (`--db-maxmemory`) is the same gate at a finer
/// grain: neither its evicting nor its rejecting arm may act on bytes the
/// queue already holds for release.
#[test]
fn the_db_quota_gate_reclaims_unlinked_bytes_before_evicting_or_refusing() {
    for policy in ["noeviction", "allkeys-random"] {
        let (mut db, budget) = at_budget(big_hash_entry(5_000, None));
        let rt = RuntimeConfig {
            db_maxmemory: vec![budget as u64],
            maxmemory_policy: policy.to_string(),
            num_shards: 1,
            ..RuntimeConfig::default()
        };
        assert!(db.unlink(b"big"));
        for i in 0..NEW {
            db.set(&new_key(i), small(b'y'));
            let r = crate::storage::db_quota::check_db_maxmemory(&mut db, 0, &rt);
            assert!(r.is_ok(), "{policy}: quota refused write {i}: {r:?}");
        }
        assert_eq!(
            survivors(&mut db),
            LIVE + NEW,
            "{policy}: the db quota evicted live keys for bytes UNLINK released"
        );
    }
}

/// A value at or above the shell-offload size (its emptied container goes to
/// the `moon-lazyfree` helper). Reclaimed under pressure it is credited
/// EXACTLY once: the gate frees only what it needs, the tick frees the rest,
/// and the ledger ends where a db that never held the value would be.
#[test]
fn a_huge_unlinked_value_is_credited_exactly_once_under_pressure() {
    let probe = Bytes::from(b"probe-payload-held-by-the-test".to_vec());
    let (mut db, budget) = at_budget(big_hash_entry(70_000, Some(&probe)));
    let cfg = cfg(budget, "allkeys-random");
    assert!(db.unlink(b"big"));
    let survived = write_and_count(&mut db, &cfg);
    assert_eq!(survived, LIVE + NEW, "live keys evicted");

    let mut control = Database::new();
    for i in 0..LIVE {
        control.set(&live_key(i), small(b'x'));
    }
    for i in 0..NEW {
        control.set(&new_key(i), small(b'y'));
    }
    assert!(
        db.lazy_free_len() == 1 && db.estimated_memory() > control.estimated_memory(),
        "the gate must reclaim only what it needs, not the whole value"
    );
    drain_all(&mut db);
    assert!(probe.is_unique(), "the drain must free the rest");
    assert_eq!(
        db.estimated_memory(),
        control.estimated_memory(),
        "the pressure drain and the tick drain together must credit the \
         value exactly once"
    );
}

/// An eviction victim is queued UNCHARGED (eviction credits synchronously).
/// When it sits ahead of a charged UNLINKed value, the pressure drain must
/// work through it to reach the bytes that are still charged.
#[test]
fn an_uncharged_victim_queued_ahead_does_not_hide_unlinked_bytes() {
    let mut db = Database::new();
    db.set(b"victim", big_hash_entry(5_000, None));
    db.set(b"big", big_hash_entry(5_000, None));
    for i in 0..LIVE {
        db.set(&live_key(i), small(b'x'));
    }
    // Exactly what `evict_one_with_spill`'s plain-drop arm does.
    let victim = db.remove(b"victim").expect("victim");
    db.lazy_free_or_drop(b"victim".len(), victim, false);
    let budget = db.estimated_memory();
    assert!(db.unlink(b"big"));
    assert_eq!(db.lazy_free_len(), 2, "fixture: both values queued");
    let cfg = cfg(budget, "noeviction");
    db.set(b"new:0", small(b'y'));
    let r = evict_to_budget(&mut db, &cfg, EvictionRun::plain());
    assert!(r.is_ok(), "write after UNLINK refused: {r:?}");
}

/// Pays only under pressure: a write that is under budget leaves the queue
/// to the tick (UNLINK stays O(1) and the gate stays O(1)).
#[test]
fn nothing_is_reclaimed_under_budget() {
    let (mut db, budget) = at_budget(big_hash_entry(5_000, None));
    let cfg = cfg(budget * 2, "allkeys-random");
    assert!(db.unlink(b"big"));
    let charged = db.estimated_memory();
    db.set(b"new:0", small(b'y'));
    evict_to_budget(&mut db, &cfg, EvictionRun::plain()).expect("under budget");
    let quota = RuntimeConfig {
        db_maxmemory: vec![(budget * 2) as u64],
        ..cfg.clone()
    };
    crate::storage::db_quota::check_db_maxmemory(&mut db, 0, &quota).expect("under quota");
    assert_eq!(
        db.lazy_free_len(),
        1,
        "an under-budget gate drained the queue"
    );
    assert!(
        db.estimated_memory() > charged,
        "nothing may have been credited"
    );
}

// ── The whole-shard aggregate gates ─────────────────────────────────────

/// Two databases on one shard: db 0 holds the live keys, db 1 the large
/// value that is UNLINKed. The shard is at its budget before the UNLINK;
/// after it, db 0 gains NEW keys, so the aggregate is over budget only by
/// bytes db 1's queue already holds for release.
fn two_db_shard(
    policy: &str,
) -> (
    std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
    std::sync::Arc<parking_lot::RwLock<RuntimeConfig>>,
) {
    use crate::shard::slice::{ShardSlice, init_shard, with_shard_db};
    let (shared, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
        Database::new(),
        Database::new(),
    ]]);
    init_shard(ShardSlice::new(inits.remove(0)));
    with_shard_db(0, |db| {
        for i in 0..LIVE {
            db.set(&live_key(i), small(b'x'));
        }
    });
    with_shard_db(1, |db| db.set(b"big", big_hash_entry(5_000, None)));
    let budget =
        with_shard_db(0, |db| db.estimated_memory()) + with_shard_db(1, |db| db.estimated_memory());
    assert!(with_shard_db(1, |db| db.unlink(b"big")));
    with_shard_db(0, |db| {
        for i in 0..NEW {
            db.set(&new_key(i), small(b'y'));
        }
    });
    let total =
        with_shard_db(0, |db| db.estimated_memory()) + with_shard_db(1, |db| db.estimated_memory());
    assert!(total > budget, "fixture: the shard must be over budget");
    shared
        .memory_publisher(0)
        .store(total, std::sync::atomic::Ordering::Relaxed);
    let rt = std::sync::Arc::new(parking_lot::RwLock::new(cfg(budget, policy)));
    (shared, rt)
}

fn db0_survivors() -> usize {
    crate::shard::slice::with_shard_db(0, survivors)
}

/// The 100 ms eviction tick (`timers::run_eviction`).
#[test]
fn the_eviction_tick_reclaims_every_db_before_evicting_any() {
    let survived = std::thread::spawn(|| {
        let (shared, rt) = two_db_shard("allkeys-random");
        crate::shard::timers::run_eviction(
            &shared,
            0,
            &rt,
            &mut None,
            &std::sync::Arc::new(parking_lot::Mutex::new(None)),
            &mut Vec::new(),
            &None,
            None,
            false,
            None,
        );
        db0_survivors()
    })
    .join()
    .expect("test thread");
    assert_eq!(
        survived,
        LIVE + NEW,
        "the eviction tick evicted {} live keys from db 0 for bytes db 1's \
         UNLINK already released",
        LIVE + NEW - survived
    );
}

/// The memory-pressure cascade (`persistence_tick::handle_memory_pressure`,
/// step 3), with no spill thread and no manifest — the plain-drop arm.
#[test]
fn the_memory_pressure_cascade_reclaims_every_db_before_evicting_any() {
    let survived = std::thread::spawn(|| {
        let (shared, rt) = two_db_shard("allkeys-random");
        crate::shard::persistence_tick::handle_memory_pressure(
            &None,
            &shared,
            0,
            &rt,
            &mut None,
            &mut 0,
            &mut None,
            None,
            None,
            &std::sync::Arc::new(parking_lot::Mutex::new(None)),
            &mut Vec::new(),
            &None,
            None,
            false,
        );
        db0_survivors()
    })
    .join()
    .expect("test thread");
    assert_eq!(
        survived,
        LIVE + NEW,
        "the pressure cascade evicted {} live keys from db 0 for bytes db 1's \
         UNLINK already released",
        LIVE + NEW - survived
    );
}
