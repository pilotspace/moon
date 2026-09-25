//! PR #1233 review: the cold tier's dead-slot ledger (moon#1215) is charged
//! at WRITE ADMISSION but is not something eviction chases.
//!
//! - (i) a write is admitted only when evictable memory plus the ledger fits
//!   the budget — on the slow path (`evict_to_budget`) and on the inline
//!   pre-gate alike;
//! - (ii) eviction pays for a small ledger by evicting live keys, but never
//!   pushes the hot set below half the budget for bytes it cannot free: the
//!   write is refused instead, and a refused write takes no further victim.

use bytes::Bytes;

use super::*;
use crate::storage::tiered::cold_index::ColdIndex;

fn config(maxmemory: usize, policy: &str) -> RuntimeConfig {
    RuntimeConfig {
        maxmemory,
        maxmemory_policy: policy.to_string(),
        maxmemory_samples: 5,
        num_shards: 1,
        ..Default::default()
    }
}

/// A database with `n` live string keys of `len`-byte values.
fn db_with_keys(n: usize, len: usize) -> Database {
    let mut db = Database::new();
    for i in 0..n {
        db.set_string(
            format!("key:{i:04}").as_bytes(),
            Bytes::from(vec![b'v'; len]),
        );
    }
    db
}

/// Give `db` a dead-slot ledger of roughly `bytes` and return its exact size.
fn add_ledger(db: &mut Database, bytes: usize) -> usize {
    let mut ci = db.cold_index.take().unwrap_or_else(ColdIndex::new);
    ci.note_dead_slot(1, Bytes::from(vec![b'd'; bytes.max(1)]), None);
    let ledger = ci.dead_slot_bytes();
    db.cold_index = Some(ci);
    ledger
}

#[test]
fn eviction_target_pays_for_a_small_ledger_and_stops_at_half_the_budget() {
    assert_eq!(eviction_target(1_000, 0), 1_000, "no ledger: unchanged");
    assert_eq!(eviction_target(1_000, 300), 700);
    assert_eq!(eviction_target(1_000, 500), 500);
    assert_eq!(eviction_target(1_000, 600), 500, "floor: half the budget");
    assert_eq!(eviction_target(1_000, 50_000), 500);
    assert_eq!(eviction_target(0, 10), 0);
}

/// (i) `noeviction`: the evictable part alone fits, the ledger tips it over
/// — the write is refused, no key is touched, and the inline pre-gate
/// refuses to wave it through too.
#[test]
fn noeviction_refuses_a_write_once_evictable_plus_ledger_is_over_budget() {
    let mut db = db_with_keys(8, 64);
    let evictable = db.estimated_memory();
    let cfg = config(evictable * 4, "noeviction");
    let budget = run_budget(&cfg, 0);
    assert!(evictable < budget, "fixture: the hot part alone fits");
    assert!(
        evict_to_budget(&mut db, &cfg, EvictionRun::plain()).is_ok(),
        "no ledger: admitted"
    );
    let ledger = add_ledger(&mut db, budget - evictable);
    assert!(evictable + ledger > budget && ledger <= budget);
    let r = evict_to_budget(&mut db, &cfg, EvictionRun::plain());
    assert!(
        matches!(&r, Err(Frame::Error(m)) if m.starts_with(b"OOM")),
        "the ledger counts at admission: {r:?}"
    );
    assert_eq!(db.len(), 8, "noeviction takes no victim");
    assert_eq!(admission_memory(&db), evictable + ledger);
    assert!(
        !can_skip_eviction(
            cfg.maxmemory,
            cfg.maxmemory_per_shard(),
            admission_memory(&db),
            0,
            crate::admin::metrics_setup::footprint_correction(),
        ),
        "the inline pre-gate must send this write to the slow path"
    );
    assert!(
        can_skip_eviction(
            cfg.maxmemory,
            cfg.maxmemory_per_shard(),
            db.estimated_memory(),
            0,
            crate::admin::metrics_setup::footprint_correction(),
        ),
        "fixture: the evictable part alone would have been waved through"
    );
}

/// (i) + (ii), small ledger: an evicting policy makes room for it by evicting
/// live keys, and the write is admitted with evictable + ledger in budget.
#[test]
fn a_small_ledger_is_paid_for_by_eviction_and_the_write_is_admitted() {
    let mut db = db_with_keys(64, 64);
    let cfg = config(db.estimated_memory(), "allkeys-lru");
    let budget = run_budget(&cfg, 0);
    let ledger = add_ledger(&mut db, budget / 4);
    assert!(ledger <= budget / 2, "fixture: under the floor");
    let before = db.len();
    let r = evict_to_budget(&mut db, &cfg, EvictionRun::plain());
    assert!(r.is_ok(), "admitted after eviction: {r:?}");
    assert!(db.len() < before, "live keys were evicted to make room");
    assert!(db.estimated_memory() + ledger <= budget);
}

/// (ii) large ledger: the hot set is evicted down to half the budget and no
/// further; the write is refused; a second write takes no victim at all.
#[test]
fn a_ledger_past_half_the_budget_refuses_writes_without_draining_the_hot_set() {
    let mut db = db_with_keys(64, 64);
    let cfg = config(db.estimated_memory(), "allkeys-lru");
    let budget = run_budget(&cfg, 0);
    let one_key = db.estimated_memory() / 64;
    add_ledger(&mut db, budget);
    let r = evict_to_budget(&mut db, &cfg, EvictionRun::plain());
    assert!(
        matches!(&r, Err(Frame::Error(m)) if m.starts_with(b"OOM")),
        "nothing eviction can free fits the write under the budget: {r:?}"
    );
    let left = db.estimated_memory();
    assert!(
        left <= budget / 2 && left + 2 * one_key > budget / 2,
        "evicted down to half the budget and no further (left {left}, budget {budget})"
    );
    let keys_left = db.len();
    assert!(keys_left > 0, "the hot set is not drained");
    let r2 = evict_to_budget(&mut db, &cfg, EvictionRun::plain());
    assert!(r2.is_err());
    assert_eq!(
        db.len(),
        keys_left,
        "a refused write takes no further victim"
    );
}

/// An aggregate run admits against the ledger it is given, not the evicted
/// database's own.
#[test]
fn an_aggregate_run_admits_against_the_ledger_it_is_given() {
    let mut db = db_with_keys(8, 64);
    let evictable = db.estimated_memory();
    let cfg = config(evictable * 4, "noeviction");
    let budget = run_budget(&cfg, 0);
    let total = evictable;
    assert!(
        evict_to_budget(
            &mut db,
            &cfg,
            EvictionRun::plain().total(total).ledger(budget - total + 1)
        )
        .is_err(),
        "a sibling database's ledger counts for an aggregate run"
    );
    assert!(evict_to_budget(&mut db, &cfg, EvictionRun::plain().total(total).ledger(0)).is_ok());
}

/// No cold index, or an empty ledger: the gate is exactly what it was.
#[test]
fn without_a_ledger_admission_memory_is_estimated_memory() {
    let mut db = db_with_keys(4, 16);
    assert_eq!(admission_memory(&db), db.estimated_memory());
    db.cold_index = Some(ColdIndex::new());
    assert_eq!(admission_memory(&db), db.estimated_memory());
    assert_eq!(cold_ledger_bytes(&db), 0);
}
