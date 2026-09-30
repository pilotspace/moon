//! moon#1286: `INFO stats` `expired_keys` must count every expiry-driven
//! whole-key removal. `record_expired_key` had no production caller, so it
//! read 0 forever. Each assertion reads the calling thread's exact mirror
//! (`this_thread_expired_keys`), never the shared striped counter: every
//! expiry in any parallel test bumps that one.

use bytes::Bytes;

use super::{drain_lazy_expired, expire_cycle, expire_cycle_direct};
use crate::admin::metrics_setup::this_thread_expired_keys as counted;
use crate::storage::Database;
use crate::storage::db::HashTtlCond;
use crate::storage::entry::{Entry, current_time_ms};

fn expired_string(db: &mut Database, key: &[u8]) {
    db.set(
        key,
        Entry::new_string_with_expiry(Bytes::from_static(b"v"), current_time_ms() - 1),
    );
}

/// The active cycle counts each key it removes, once.
#[test]
fn the_active_cycle_counts_every_key_it_reaps() {
    let mut db = Database::new();
    for i in 0..50 {
        expired_string(&mut db, format!("act1286:{i}").as_bytes());
    }
    let before = counted();
    expire_cycle_direct(&mut db, &mut |_| {});
    assert_eq!(db.len(), 0, "precondition: all reaped");
    assert_eq!(counted() - before, 50);
    expire_cycle_direct(&mut db, &mut |_| {});
    assert_eq!(counted() - before, 50, "a second cycle counts nothing");
}

/// The lazy path: reads hide the keys (nothing counted yet — the key is still
/// resident), the drain removes and counts each exactly once, even when a key
/// was read repeatedly before the tick.
#[test]
fn the_lazy_drain_counts_every_key_once() {
    let mut db = Database::new();
    for i in 0..10 {
        expired_string(&mut db, format!("lazy1286:{i}").as_bytes());
    }
    let before = counted();
    for i in 0..10 {
        let key = format!("lazy1286:{i}");
        assert!(db.get(key.as_bytes()).is_none());
        assert!(db.get(key.as_bytes()).is_none());
    }
    assert_eq!(counted() - before, 0, "a read only hides; the drain counts");
    drain_lazy_expired(&mut db, &mut |_| {});
    assert_eq!(counted() - before, 10);
    drain_lazy_expired(&mut db, &mut |_| {});
    assert_eq!(
        counted() - before,
        10,
        "a drained queue counts nothing more"
    );
}

/// A key rewritten after a lazy read (GET then SET): the overwrite reaps the
/// hidden expired entry and counts it, as redis's `lookupKeyWrite` does; the
/// drain then finds a live key and counts nothing more (R1 finding 3).
#[test]
fn a_key_rewritten_before_the_drain_is_counted_once_by_the_write() {
    let mut db = Database::new();
    expired_string(&mut db, b"rew1286");
    let before = counted();
    assert!(db.get(b"rew1286").is_none());
    db.set(b"rew1286", Entry::new_string(Bytes::from_static(b"live")));
    assert_eq!(counted() - before, 1, "the overwrite reaped it");
    drain_lazy_expired(&mut db, &mut |_| {});
    assert_eq!(counted() - before, 1, "the drain skips the fresh value");
    assert!(db.exists(b"rew1286"));
}

/// An overwrite of an expired key nobody read (SET, and every write that
/// lands through `Database::set`) counts it; an overwrite of a live key, or
/// of one whose TTL is still ahead, does not (R1 finding 3).
#[test]
fn an_overwrite_counts_only_an_expired_entry() {
    let mut db = Database::new();
    expired_string(&mut db, b"ow1286:expired");
    db.set(
        b"ow1286:ttl",
        Entry::new_string_with_expiry(Bytes::from_static(b"v"), current_time_ms() + 60_000),
    );
    db.set(b"ow1286:plain", Entry::new_string(Bytes::from_static(b"v")));
    let before = counted();
    for key in [&b"ow1286:expired"[..], b"ow1286:ttl", b"ow1286:plain"] {
        db.set(key, Entry::new_string(Bytes::from_static(b"new")));
    }
    assert_eq!(counted() - before, 1);
    expire_cycle_direct(&mut db, &mut |_| {});
    assert_eq!(counted() - before, 1, "nothing left to reap");
    assert_eq!(db.len(), 3);
}

/// Redis counts a whole key only: a hash field reaped by its TTL, or a hash
/// that loses its last field to it, is not an expired key.
#[test]
fn hash_field_expiry_is_not_counted() {
    let mut db = Database::new();
    let ttl = db.now_ms() + 1_000;
    {
        let map = db.get_or_create_hash(b"h1286").expect("hash");
        map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
    }
    assert_eq!(
        db.hash_set_field_ttl(b"h1286", b"f", ttl, HashTtlCond::Always),
        Ok(1)
    );
    db.set_cached_now_ms_for_test(ttl + 1);
    let before = counted();
    expire_cycle(&mut db, &mut |_| {});
    assert!(
        !db.exists(b"h1286"),
        "precondition: the hash went with its field"
    );
    assert_eq!(counted() - before, 0);
}

/// A write that finds an expired key drops it first (`lookupKeyWrite`), which
/// redis counts.
#[test]
fn a_write_over_an_expired_key_counts_it() {
    let mut db = Database::new();
    expired_string(&mut db, b"w1286");
    let before = counted();
    db.get_or_create_hash(b"w1286").expect("hash");
    assert_eq!(counted() - before, 1);
}
