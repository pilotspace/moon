//! Probe budget for the storage accessors (moon#942).
//!
//! Test-only. Every assertion here counts **DashTable key lookups** — calls
//! that hash the key, route through the directory and walk a segment — with
//! [`crate::storage::dashtable::take_key_lookups`]. Nothing here touches a
//! clock.
//!
//! # Why a counter and not a timer
//!
//! moon#789 (`b3083c5a`) is the precedent: the PERF-08 wall-clock net turned
//! out to be measuring a page-fault artifact rather than the optimisation it
//! named, and the change it guarded measured **+11% on aarch64 and −17% on
//! x86_64** — the two architectures disagreed in *sign*. A probe-count
//! reduction is therefore **not** a throughput claim and this file makes none.
//! It pins a structural fact: how many times each accessor hashes its key.
//!
//! A number here going UP is the regression moon#942 is about. A number going
//! DOWN is an improvement in probes and says nothing whatever about wall
//! clock, which only a Linux benchmark host may answer.

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::dashtable::take_key_lookups;
use crate::storage::db_kind::{HashKind, ListKind, SetKind, SortedSetKind};
use crate::storage::entry::Entry;
use bytes::Bytes;

/// Reset the counter, run `f`, return `(result, lookups)`.
fn probes<T>(f: impl FnOnce() -> T) -> (T, u32) {
    let _ = take_key_lookups();
    let out = f();
    (out, take_key_lookups())
}

/// A database whose clock is pinned, so no test here can race a real TTL.
fn db_at(now_ms: u64) -> Database {
    let mut db = Database::new();
    db.set_cached_now_ms_for_test(now_ms);
    db
}

const NOW: u64 = 1_000_000;

fn bulk(s: &str) -> Frame {
    Frame::BulkString(Bytes::from(s.to_owned()))
}

// ── get_or_create and its two siblings ──────────────────────────────────────

#[test]
fn get_or_create_probe_budget() {
    let mut db = db_at(NOW);

    // MISS: fabricate the container.
    let (r, miss) = probes(|| db.get_or_create::<SetKind>(b"s").map(|_| ()));
    assert!(r.is_ok());

    // HIT: the key now exists and is live.
    let (r, hit) = probes(|| db.get_or_create::<SetKind>(b"s").map(|_| ()));
    assert!(r.is_ok());

    assert_eq!(
        (hit, miss),
        (2, 4),
        "get_or_create probe budget moved (hit={hit}, miss={miss}) — moon#942. \
         The hit path is `get` (liveness) then `get_mut` (hand out); the miss \
         path adds `promote_cold_if_present`'s own `contains_key` and the \
         `insert`. A RISE is the regression this test exists to catch."
    );
}

#[test]
fn get_mut_if_present_probe_budget() {
    let mut db = db_at(NOW);
    let _ = db.get_or_create::<SetKind>(b"s");

    let (r, hit) = probes(|| db.get_mut_if_present::<SetKind>(b"s").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    let (r, miss) = probes(|| {
        db.get_mut_if_present::<SetKind>(b"nope")
            .map(|o| o.is_some())
    });
    assert_eq!(r, Ok(false));

    assert_eq!(
        (hit, miss),
        (2, 3),
        "get_mut_if_present probe budget moved (hit={hit}, miss={miss}) — moon#942."
    );
}

#[test]
fn get_promoted_probe_budget() {
    let mut db = db_at(NOW);
    let _ = db.get_or_create::<HashKind>(b"h");

    let (r, hit) = probes(|| db.get_promoted::<HashKind>(b"h").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    let (r, miss) = probes(|| db.get_promoted::<HashKind>(b"nope").map(|o| o.is_some()));
    assert_eq!(r, Ok(false));

    assert_eq!(
        (hit, miss),
        (2, 3),
        "get_promoted probe budget moved (hit={hit}, miss={miss}) — moon#942."
    );
}

// ── the bespoke compact-encoding accessors ──────────────────────────────────

#[test]
fn get_or_create_intset_probe_budget() {
    let mut db = db_at(NOW);

    let (r, miss) = probes(|| db.get_or_create_intset(b"i").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    let (r, hit) = probes(|| db.get_or_create_intset(b"i").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    assert_eq!(
        (hit, miss),
        (2, 4),
        "get_or_create_intset probe budget moved (hit={hit}, miss={miss}) — moon#942."
    );
}

#[test]
fn get_or_create_hash_listpack_probe_budget() {
    let mut db = db_at(NOW);

    let (r, miss) = probes(|| db.get_or_create_hash_listpack(b"h").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    let (r, hit) = probes(|| db.get_or_create_hash_listpack(b"h").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    assert_eq!(
        (hit, miss),
        (2, 4),
        "get_or_create_hash_listpack probe budget moved (hit={hit}, miss={miss}) — moon#942."
    );
}

#[test]
fn get_or_create_list_listpack_probe_budget() {
    let mut db = db_at(NOW);

    let (r, miss) = probes(|| db.get_or_create_list_listpack(b"l").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    let (r, hit) = probes(|| db.get_or_create_list_listpack(b"l").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    assert_eq!(
        (hit, miss),
        (2, 4),
        "get_or_create_list_listpack probe budget moved (hit={hit}, miss={miss}) — moon#942."
    );
}

#[test]
fn get_or_create_zset_listpack_probe_budget() {
    let mut db = db_at(NOW);

    let (r, miss) = probes(|| db.get_or_create_zset_listpack(b"z").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    let (r, hit) = probes(|| db.get_or_create_zset_listpack(b"z").map(|o| o.is_some()));
    assert_eq!(r, Ok(true));

    assert_eq!(
        (hit, miss),
        (2, 4),
        "get_or_create_zset_listpack probe budget moved (hit={hit}, miss={miss}) — moon#942."
    );
}

#[test]
fn get_or_create_set_listpack_probe_budget() {
    let mut db = db_at(NOW);

    let (r, miss) = probes(|| {
        db.get_or_create_set_listpack(b"s", |_, _| false)
            .map(|o| o.is_some())
    });
    assert_eq!(r, Ok(true));

    let (r, hit) = probes(|| {
        db.get_or_create_set_listpack(b"s", |_, _| false)
            .map(|o| o.is_some())
    });
    assert_eq!(r, Ok(true));

    // The intset -> listpack edge (moon#899). `absorb_intset_into_listpack`
    // is the reason this accessor costs more than its four siblings.
    let mut db2 = db_at(NOW);
    {
        let is = db2
            .get_or_create_intset(b"t")
            .expect("set type")
            .expect("intset");
        is.insert(1);
        is.insert(2);
    }
    let (r, absorb) = probes(|| {
        db2.get_or_create_set_listpack(b"t", |_, _| true)
            .map(|o| o.is_some())
    });
    assert_eq!(
        r,
        Ok(true),
        "the intset must have been absorbed into a listpack"
    );

    assert_eq!(
        (hit, miss, absorb),
        (2, 4, 2),
        "get_or_create_set_listpack probe budget moved \
         (hit={hit}, miss={miss}, absorb={absorb}) — moon#942. The absorb arm \
         is the one `absorb_intset_into_listpack` used to charge its own extra \
         `get_mut` for."
    );
}

// ── end to end: the family moon#942 says this work exists for ───────────────

#[test]
fn sadd_end_to_end_probe_budget() {
    // The benchmark's own shape (`scripts/bench-ab-matrix.sh:78-85`):
    // `SADD set:<12-digit> <12-digit>` where the member is ZERO-PADDED, so
    // `numeric::canonical_i64` rejects it and the intset path is never taken.
    let key = || bulk("set:000000000042");

    // (a) Absent key -> a SetListpack is created.
    let mut db = db_at(NOW);
    let args = [key(), bulk("000000000007")];
    let (r, create) = probes(|| crate::command::set::sadd(&mut db, &args));
    assert_eq!(r, Frame::Integer(1));

    // (b) Steady state on the listpack encoding.
    let args2 = [key(), bulk("000000000008")];
    let (r, listpack_hit) = probes(|| crate::command::set::sadd(&mut db, &args2));
    assert_eq!(r, Frame::Integer(1));

    // (c) Steady state on the HASHTABLE encoding — 122 of 200 keys at the
    //     benchmark's own p=64 point (`pop-tally-5bf716a9-arm.txt`). Reached
    //     by overflowing the listpack policy first.
    let mut db2 = db_at(NOW);
    let mut big = vec![key()];
    big.extend((0..400).map(|i| bulk(&format!("member-{i}"))));
    assert_eq!(
        crate::command::set::sadd(&mut db2, &big),
        Frame::Integer(400)
    );
    let args3 = [key(), bulk("000000000009")];
    let (r, hashtable_hit) = probes(|| crate::command::set::sadd(&mut db2, &args3));
    assert_eq!(r, Frame::Integer(1));

    assert_eq!(
        (create, listpack_hit, hashtable_hit),
        (4, 2, 4),
        "SADD end-to-end probe budget moved \
         (create={create}, listpack_hit={listpack_hit}, hashtable_hit={hashtable_hit}) \
         — moon#942. The hashtable arm pays TWO accessors: \
         `get_or_create_set_listpack` answers `Ok(None)` and `get_or_create_set` \
         then repeats the whole skeleton. Redis pays one `dictFind` for all three."
    );
}

// ── invariants a probe collapse is most likely to break ─────────────────────

#[test]
fn expired_key_reads_as_absent_through_get_or_create() {
    let mut db = db_at(NOW);
    db.set(
        b"k",
        Entry::new_string_with_expiry(Bytes::from_static(b"v"), NOW - 1),
    );
    // A string is the WRONG type for a set — but the key is EXPIRED, so it
    // must read as absent and be fabricated fresh, not answer WRONGTYPE.
    assert!(
        db.get_or_create::<SetKind>(b"k").is_ok(),
        "an expired key must read as absent, not WRONGTYPE"
    );
    assert_eq!(
        db.expires_count(),
        0,
        "the expiry index must be unindexed with the entry (moon#541)"
    );
}

#[test]
fn expired_key_reads_as_absent_through_every_compact_accessor() {
    macro_rules! case {
        ($name:literal, $body:expr) => {{
            let mut db = db_at(NOW);
            db.set(
                b"k",
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), NOW - 1),
            );
            let f: fn(&mut Database) -> Result<bool, Frame> = $body;
            assert_eq!(
                f(&mut db),
                Ok(true),
                concat!($name, ": an expired key must fabricate, not WRONGTYPE")
            );
            assert_eq!(db.expires_count(), 0, concat!($name, ": expiry index leak"));
        }};
    }

    case!("intset", |db| db
        .get_or_create_intset(b"k")
        .map(|o| o.is_some()));
    case!("hash_listpack", |db| db
        .get_or_create_hash_listpack(b"k")
        .map(|o| o.is_some()));
    case!("list_listpack", |db| db
        .get_or_create_list_listpack(b"k")
        .map(|o| o.is_some()));
    case!("zset_listpack", |db| db
        .get_or_create_zset_listpack(b"k")
        .map(|o| o.is_some()));
    case!("set_listpack", |db| db
        .get_or_create_set_listpack(b"k", |_, _| false)
        .map(|o| o.is_some()));
}

#[test]
fn expired_key_reads_as_absent_through_read_accessors() {
    let mut db = db_at(NOW);
    db.set(
        b"k",
        Entry::new_string_with_expiry(Bytes::from_static(b"v"), NOW - 1),
    );
    assert_eq!(
        db.get_promoted::<ListKind>(b"k").map(|o| o.is_some()),
        Ok(false)
    );

    let mut db = db_at(NOW);
    db.set(
        b"k",
        Entry::new_string_with_expiry(Bytes::from_static(b"v"), NOW - 1),
    );
    assert_eq!(
        db.get_mut_if_present::<SortedSetKind>(b"k")
            .map(|o| o.is_some()),
        Ok(false)
    );
}

#[test]
fn wrongtype_still_errors_on_a_live_key() {
    let mut db = db_at(NOW);
    db.set(b"k", Entry::new_string(Bytes::from_static(b"v")));
    assert!(db.get_or_create::<SetKind>(b"k").is_err());
    assert!(db.get_or_create_intset(b"k").is_err());
    assert!(db.get_or_create_hash_listpack(b"k").is_err());
    assert!(db.get_or_create_list_listpack(b"k").is_err());
    assert!(db.get_or_create_zset_listpack(b"k").is_err());
    assert!(db.get_or_create_set_listpack(b"k", |_, _| false).is_err());
    assert!(db.get_promoted::<HashKind>(b"k").is_err());
    assert!(db.get_mut_if_present::<HashKind>(b"k").is_err());
}

#[test]
fn watch_version_bumps_once_per_mutable_handle() {
    let mut db = db_at(NOW);
    let _ = db.get_or_create::<HashKind>(b"h");
    let v0 = db.data().get(b"h").expect("present").version();
    let _ = db.get_or_create::<HashKind>(b"h");
    let v1 = db.data().get(b"h").expect("present").version();
    assert_eq!(
        v1,
        v0 + 1,
        "handing out a mutable handle is exactly ONE WATCH version bump (moon#926)"
    );

    // The compact accessors carry the same contract.
    let _ = db.get_or_create_hash_listpack(b"lp");
    let v0 = db.data().get(b"lp").expect("present").version();
    let _ = db.get_or_create_hash_listpack(b"lp");
    let v1 = db.data().get(b"lp").expect("present").version();
    assert_eq!(v1, v0 + 1);
}

#[test]
fn used_memory_is_identical_for_an_identical_sequence() {
    // Two databases, the same operations, byte-identical ledgers. This is the
    // invariant a probe collapse is most likely to break, by moving an
    // `entry_overhead` charge across a branch.
    let run = || {
        let mut db = db_at(NOW);
        for i in 0..64u32 {
            let k = format!("k{i}");
            let k = k.as_bytes();
            let _ = db.get_or_create_hash_listpack(k);
            let _ = db.get_or_create_list_listpack(k);
            let _ = db.get_or_create_intset(k);
            let _ = db.get_or_create_set_listpack(k, |_, _| true);
            let _ = db.get_or_create::<SetKind>(k);
            let _ = db.get_promoted::<SetKind>(k);
        }
        (db.used_memory, db.data().len())
    };
    assert_eq!(run(), run());
}
