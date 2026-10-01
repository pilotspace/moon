//! moon#1298 (PROTOTYPE): the time-bucket wheel must be observably identical
//! to the sorted-set index. Every test runs the wheel ON explicitly
//! (`set_expiry_wheel(true)`); the rest of the lib suite is additionally run
//! with `MOON_EXPIRY_WHEEL=1` so every existing expiry / eviction test doubles
//! as a wheel test.

use bytes::Bytes;

use crate::server::expiration::expire_cycle_direct;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, current_time_ms};

fn volatile(ttl_ms: u64) -> Entry {
    Entry::new_string_with_expiry(Bytes::from_static(b"v"), ttl_ms)
}

fn wheel_db() -> Database {
    let mut db = Database::new();
    db.set_expiry_wheel(true);
    assert!(db.expiry_wheel_enabled());
    db
}

/// xorshift64: deterministic, no dev-dependency.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn key(i: u64) -> String {
    // Alternate inline (<= 23 B) and heap (> 23 B) keys.
    if i % 2 == 0 {
        format!("k:{i}")
    } else {
        format!("a-heap-sized-key-over-23-bytes:{i:06}")
    }
}

/// The wheel's `volatile-ttl` head is the exact smallest DEADLINE, including
/// deadlines 1 ms apart inside one 256 ms bucket and across a bucket edge.
#[test]
fn nearest_expiry_is_the_exact_smallest_deadline() {
    let mut db = wheel_db();
    let base = (current_time_ms() / 256) * 256 + 3_600_000;
    // 255 and 256 straddle a bucket edge; 258 is in the next bucket.
    for (i, off) in [258u64, 255, 256, 300, 1_000_000].iter().enumerate() {
        db.set(key(i as u64).as_bytes(), volatile(base + off));
    }
    let (ts, k) = db.peek_nearest_expiry().expect("a volatile key exists");
    assert_eq!(ts, base + 255);
    assert_eq!(k.as_bytes(), key(1).as_bytes());
    // Retarget the nearest one later: the next smallest takes over.
    assert!(db.set_expiry(key(1).as_bytes(), base + 2_000_000));
    let (ts, k) = db.peek_nearest_expiry().unwrap();
    assert_eq!((ts, k.as_bytes()), (base + 256, key(2).as_bytes()));
    assert!(db.debug_expiry_index_consistent());
}

/// One keyspace operation, generated once and applied to both databases.
#[derive(Clone, Copy, Debug)]
enum Op {
    SetTtl(u64, u64),
    Expire(u64, u64),
    Persist(u64),
    Overwrite(u64),
    Del(u64),
}

fn apply(db: &mut Database, op: Op) {
    match op {
        Op::SetTtl(k, ttl) => db.set(key(k).as_bytes(), volatile(ttl)),
        Op::Expire(k, ttl) => {
            db.set_expiry(key(k).as_bytes(), ttl);
        }
        Op::Persist(k) => {
            db.set_expiry(key(k).as_bytes(), 0);
        }
        Op::Overwrite(k) => db.set(
            key(k).as_bytes(),
            Entry::new_string(Bytes::from_static(b"p")),
        ),
        Op::Del(k) => {
            db.remove(key(k).as_bytes());
        }
    }
}

/// Random TTL writes, overwrites, retargets, PERSISTs and deletes keep the
/// wheel exactly equal to the scan-derived truth, and active expiry removes
/// exactly the keys the sorted-set database removes.
#[test]
fn wheel_matches_sorted_set_under_random_ops() {
    let mut tree = Database::new();
    tree.set_expiry_wheel(false);
    let mut wheel = wheel_db();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let now = current_time_ms();
    for step in 0..6_000u64 {
        let k = rng.below(300);
        // Deadlines: already due, tight same-bucket clusters, and far ones.
        let ttl = match rng.below(4) {
            0 => now - 1 - rng.below(5_000),
            1 => now + 3_600_000 + rng.below(8),
            2 => now + 3_600_000 + rng.below(100_000),
            _ => now + 86_400_000 * (1 + rng.below(30)),
        };
        let op = match rng.below(8) {
            0..=2 => Op::SetTtl(k, ttl),
            3 | 4 => Op::Expire(k, ttl),
            5 => Op::Persist(k),
            6 => Op::Overwrite(k),
            _ => Op::Del(k),
        };
        apply(&mut tree, op);
        apply(&mut wheel, op);
        assert_eq!(
            tree.expiry_index_len(),
            wheel.expiry_index_len(),
            "step {step} {op:?}"
        );
        if step % 61 == 0 {
            assert!(tree.debug_expiry_index_consistent(), "tree @ {step}");
            assert!(wheel.debug_expiry_index_consistent(), "wheel @ {step}");
            assert_eq!(
                tree.peek_nearest_expiry().map(|p| p.0),
                wheel.peek_nearest_expiry().map(|p| p.0),
                "nearest deadline @ {step}"
            );
        }
    }
    let (mut rt, mut rw) = (Vec::new(), Vec::new());
    for _ in 0..10_000 {
        expire_cycle_direct(&mut tree, &mut |k| rt.push(k.to_vec()));
        expire_cycle_direct(&mut wheel, &mut |k| rw.push(k.to_vec()));
        let t = current_time_ms();
        if !tree.has_due_expiry(t) && !wheel.has_due_expiry(t) {
            break;
        }
    }
    // Inside one millisecond the order is key bytes vs hash: compare as sets.
    rt.sort();
    rw.sort();
    assert!(!rt.is_empty(), "the workload must have expired something");
    assert_eq!(rt, rw, "wheel and sorted set expired different keys");
    assert_eq!(tree.len(), wheel.len());
    assert_eq!(tree.expiry_index_len(), wheel.expiry_index_len());
    assert!(wheel.debug_expiry_index_consistent());
}

/// Active expiry on the wheel reaps due keys in deadline order, leaves the
/// rest, and a due key that was DELeted or retargeted first is not touched.
#[test]
fn active_expiry_on_the_wheel_reaps_due_keys_in_deadline_order() {
    let mut db = wheel_db();
    let now = current_time_ms();
    for i in 0..500u64 {
        db.set(key(i).as_bytes(), volatile(now - 10_000 + i * 3));
    }
    for i in 500..600u64 {
        db.set(key(i).as_bytes(), volatile(now + 3_600_000 + i));
    }
    // Delete one due key before the sweep sees it, and retarget a future one.
    db.remove(key(11).as_bytes());
    assert!(db.set_expiry(key(510).as_bytes(), now + 7_200_000));
    let mut removed = Vec::new();
    let mut cycles = 0;
    while db.has_due_expiry(current_time_ms()) {
        expire_cycle_direct(&mut db, &mut |k| removed.push(k.to_vec()));
        cycles += 1;
        assert!(cycles < 10_000, "no progress");
    }
    let expect: Vec<Vec<u8>> = (0..500u64)
        .filter(|i| *i != 11)
        .map(|i| key(i).into_bytes())
        .collect();
    assert_eq!(removed, expect, "wheel reaped out of deadline order");
    assert_eq!(db.expiry_index_len(), 100);
    assert!(db.debug_expiry_index_consistent());
    assert!(db.get(key(510).as_bytes()).is_some());
}

/// FLUSHDB empties the wheel; a `clear` followed by new TTL writes works.
#[test]
fn clear_empties_the_wheel() {
    let mut db = wheel_db();
    let now = current_time_ms();
    for i in 0..100u64 {
        db.set(key(i).as_bytes(), volatile(now + 1_000_000 + i));
    }
    assert_eq!(db.expiry_index_len(), 100);
    db.clear();
    assert_eq!(db.expiry_index_len(), 0);
    assert!(db.expiry_index_is_empty());
    assert!(db.peek_nearest_expiry().is_none());
    db.set(b"again", volatile(now + 5_000_000));
    assert_eq!(db.expiry_index_len(), 1);
    assert!(db.debug_expiry_index_consistent());
}

/// `recalculate_memory` (the post-bulk-load healer) rebuilds a WHEEL index,
/// not a sorted set, and `keys_with_expiry` resolves every reference.
#[test]
fn rebuild_keeps_the_wheel_kind_and_resolves_keys() {
    let mut db = wheel_db();
    let now = current_time_ms();
    for i in 0..64u64 {
        db.set(key(i).as_bytes(), volatile(now + 1_000_000 + i * 7));
    }
    db.recalculate_memory();
    assert!(db.expiry_wheel_enabled());
    assert_eq!(db.expiry_index_len(), 64);
    let keys = db.keys_with_expiry();
    assert_eq!(keys.len(), 64);
    assert_eq!(keys[0].as_bytes(), key(0).as_bytes(), "deadline order");
    assert!(db.debug_expiry_index_consistent());
}

/// A reference nothing resolves (the entry is gone) is retired by the pop and
/// by the nearest-peek, never returned as a key.
#[test]
fn unresolvable_references_are_retired_not_returned() {
    let mut db = wheel_db();
    let now = current_time_ms();
    db.expiry_index_insert(now - 5_000, b"ghost-due");
    db.expiry_index_insert(1, b"ghost-nearest");
    db.set(b"live", volatile(now + 3_600_000));
    assert_eq!(db.expiry_index_len(), 3);
    let (_, k) = db.peek_nearest_expiry().expect("the live key");
    assert_eq!(k.as_bytes(), b"live");
    assert_eq!(
        db.expiry_index_len(),
        1,
        "both ghost heads (ahead of the live key) are retired by the peek"
    );
    assert!(db.pop_due_expiry(now).is_none(), "the live key is not due");
    db.expiry_index_insert(now - 5_000, b"ghost-due-2");
    assert!(
        db.pop_due_expiry(now).is_none(),
        "a due ghost resolves to nothing"
    );
    assert_eq!(db.expiry_index_len(), 1, "due ghost retired by the pop");
    assert!(db.debug_expiry_index_consistent());
}

/// moon#1299 on the wheel: a due key an open TXN holds is not reaped under
/// it, keeps its reference (the sweep puts it back), and is reaped once the
/// transaction ends. A free due key in the same bucket is reaped meanwhile.
#[test]
fn txn_held_due_key_is_skipped_then_reaped_after_the_txn() {
    use crate::transaction::isolation::{hold, txn_begin, txn_end};
    let mut db = wheel_db();
    let now = current_time_ms();
    db.set(b"held", volatile(now - 2_000));
    db.set(b"free", volatile(now - 1_999));
    txn_begin(4_242);
    assert!(hold(
        db.db_index,
        &Bytes::from_static(b"held"),
        4_242,
        || None
    ));
    let mut removed = Vec::new();
    expire_cycle_direct(&mut db, &mut |k| removed.push(k.to_vec()));
    assert_eq!(removed, vec![b"free".to_vec()], "held key must be skipped");
    assert!(
        db.data().get(&b"held"[..]).is_some(),
        "the held key survives the sweep"
    );
    assert_eq!(db.expiry_index_len(), 1, "its reference was put back");
    assert!(db.debug_expiry_index_consistent());
    txn_end(4_242);
    expire_cycle_direct(&mut db, &mut |k| removed.push(k.to_vec()));
    assert_eq!(removed.len(), 2, "reaped after the TXN released it");
    assert_eq!(db.expiry_index_len(), 0);
}

/// `volatile-ttl`'s exact nearest-deadline victim is the wheel's head: the
/// key with the smallest TTL among many in one bucket and across buckets.
#[test]
fn volatile_ttl_victim_comes_from_the_wheel_head() {
    let mut db = wheel_db();
    let now = current_time_ms();
    for i in 0..50u64 {
        db.set(key(i).as_bytes(), volatile(now + 3_600_000 + (49 - i) * 37));
    }
    let (_, victim) = db.peek_nearest_expiry().expect("a volatile key");
    assert_eq!(victim.as_bytes(), key(49).as_bytes(), "smallest TTL first");
}

/// The switch is wired: a fresh `Database` follows `MOON_EXPIRY_WHEEL`
/// (so `MOON_EXPIRY_WHEEL=1 cargo test --lib` runs every expiry test on the
/// wheel), and `with_capacity` agrees with `new`.
#[test]
fn the_env_switch_selects_the_index_kind() {
    let want = std::env::var("MOON_EXPIRY_WHEEL").is_ok_and(|v| !v.is_empty() && v != "0");
    assert_eq!(Database::new().expiry_wheel_enabled(), want);
    assert_eq!(Database::with_capacity(16).expiry_wheel_enabled(), want);
}
