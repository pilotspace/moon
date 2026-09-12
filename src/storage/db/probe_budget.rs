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
    match db2.get_or_create_intset(b"t") {
        Ok(Some(is)) => {
            is.insert(1);
            is.insert(2);
        }
        other => panic!(
            "expected a fresh intset, got {:?}",
            other.map(|o| o.is_some())
        ),
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
    /// The entry's WATCH version, or a failed assertion naming the key.
    fn version_of(db: &Database, key: &[u8]) -> u32 {
        match db.data().get(key) {
            Some(e) => e.version(),
            None => panic!("key {:?} should be present", String::from_utf8_lossy(key)),
        }
    }

    let mut db = db_at(NOW);
    let _ = db.get_or_create::<HashKind>(b"h");
    let v0 = version_of(&db, b"h");
    let _ = db.get_or_create::<HashKind>(b"h");
    let v1 = version_of(&db, b"h");
    assert_eq!(
        v1,
        v0 + 1,
        "handing out a mutable handle is exactly ONE WATCH version bump (moon#926)"
    );

    // The compact accessors carry the same contract.
    let _ = db.get_or_create_hash_listpack(b"lp");
    let v0 = version_of(&db, b"lp");
    let _ = db.get_or_create_hash_listpack(b"lp");
    let v1 = version_of(&db, b"lp");
    assert_eq!(v1, v0 + 1);
}

#[test]
fn used_memory_agrees_with_an_independent_recount() {
    // NOT `assert_eq!(run(), run())`. That form was written first and is a
    // tautology: `run` is a pure deterministic closure over a fresh
    // `Database`, so a build that moved an `entry_overhead` charge across a
    // branch would still agree with itself. The oracle has to be computed a
    // DIFFERENT WAY from the ledger, or it cannot fail.
    //
    // The ledger is maintained incrementally, one delta per accessor call.
    // The oracle walks the finished keyspace once and sums `entry_overhead`
    // from each entry's own `estimate_memory`. They agree only if every
    // incremental delta landed — which is exactly what a probe collapse could
    // break by moving a charge into or out of a branch.
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
    assert_eq!(db.data().len(), 64, "fixture: one key per iteration");

    let oracle: usize = db
        .data()
        .iter()
        .map(|(k, e)| crate::storage::db::entry_overhead(k.as_ref(), e))
        .sum();
    assert_eq!(
        db.used_memory, oracle,
        "the incrementally maintained ledger disagrees with a fresh walk of \
         the keyspace — an accessor's charge or credit went missing across a \
         branch (moon#788)"
    );
}

#[test]
fn the_intset_to_listpack_swing_lands_in_the_ledger() {
    // The ONE ledger write moon#942 moved: `absorb_intset_into_listpack` used
    // to apply it itself, before `stamp_mutation`; the accessor now applies it
    // after. Nothing else in the suite asserts `used_memory` across an absorb,
    // so without this the CHANGELOG's byte-identity claim rests on reading.
    // Built through SADD, not by poking the raw `&mut Intset`: the accessor
    // hands out a bare handle and leaves per-member accounting to the writer,
    // so a hand-built intset would leave the ledger legitimately behind the
    // keyspace and the oracle below would fail for the fixture's reasons
    // rather than the code's.
    let mut db = db_at(NOW);
    let mut args = vec![bulk("t")];
    args.extend((1..=8).map(|v| bulk(&v.to_string())));
    assert_eq!(crate::command::set::sadd(&mut db, &args), Frame::Integer(8));
    assert!(
        matches!(
            db.data().get(b"t").map(|e| e.value.as_redis_value()),
            Some(crate::storage::compact_value::RedisValueRef::SetIntset(_))
        ),
        "fixture: SADD of canonical integers must produce an intset"
    );

    // Snapshot the pre-conversion cost from the entry itself.
    let before = match db.data().get(b"t") {
        Some(e) => e.value.estimate_memory(),
        None => panic!("fixture: the intset vanished"),
    };
    let ledger_before = db.used_memory;

    match db.get_or_create_set_listpack(b"t", |_, _| true) {
        Ok(Some(_)) => {}
        other => panic!(
            "the intset should have been absorbed, got {:?}",
            other.map(|o| o.is_some())
        ),
    }

    let after = match db.data().get(b"t") {
        Some(e) => e.value.estimate_memory(),
        None => panic!("the key vanished across the absorb"),
    };
    assert_ne!(before, after, "fixture: the conversion changed nothing");
    assert_eq!(
        db.used_memory,
        ledger_before + after - before,
        "the intset -> listpack swing did not reach `used_memory` (moon#899)"
    );
    // And the whole-keyspace oracle agrees, which the swing alone cannot prove.
    let oracle: usize = db
        .data()
        .iter()
        .map(|(k, e)| crate::storage::db::entry_overhead(k.as_ref(), e))
        .sum();
    assert_eq!(db.used_memory, oracle);
}

// ── the invariant the whole preamble exists for (moon#459) ──────────────────

/// A set body in the spill-plane wire format.
///
/// The IN-FLIGHT SPILL PLANE is the RAM-resident half of the offload tier, and
/// `promote_cold_if_present` consults it FIRST
/// (`promote_inflight_if_present`). That makes it the cheapest reachable
/// stand-in for "this key lives on the cold side only": a fabrication that
/// skipped promotion would shadow it exactly as it would shadow an on-disk
/// cold entry — with no disk, no temp dir and no spill thread in the test.
fn spilled_set_body(members: &[&[u8]]) -> Bytes {
    let mut scratch = db_at(NOW);
    match scratch.get_or_create::<SetKind>(b"scratch") {
        Ok(set) => {
            for m in members {
                set.insert(Bytes::copy_from_slice(m));
            }
        }
        Err(e) => panic!("fixture: could not build the set: {e:?}"),
    }
    let Some(entry) = scratch.data().get(b"scratch") else {
        panic!("fixture: the set vanished before it could be serialised");
    };
    match crate::storage::tiered::kv_serde::serialize_collection(&entry.value.as_redis_value()) {
        Some(b) => Bytes::from(b),
        None => panic!("fixture: the set did not serialise"),
    }
}

/// Record `key` as mid-spill, carrying `body`. Does NOT touch the hot plane.
fn park_in_flight(db: &mut Database, key: &'static [u8], body: Bytes) {
    db.spill_inflight_mark(
        Bytes::from_static(key),
        crate::storage::db::PendingSpill {
            req_id: 1,
            value_type: crate::persistence::kv_page::ValueType::Set,
            value_bytes: body,
            ttl_ms: None,
        },
    );
}

#[test]
fn an_absent_key_with_a_spilled_value_is_promoted_not_fabricated_over() {
    let mut db = db_at(NOW);
    park_in_flight(
        &mut db,
        b"s",
        spilled_set_body(&[b"alpha", b"beta", b"gamma"]),
    );
    assert!(
        db.data().get(b"s").is_none(),
        "fixture: the key must be out of the hot plane"
    );

    let len = match db.get_or_create::<SetKind>(b"s") {
        Ok(set) => set.len(),
        Err(e) => panic!("expected the promoted set, got {e:?}"),
    };
    assert_eq!(
        len, 3,
        "get_or_create fabricated an EMPTY set over a spilled value — the \
         shadowed-write bug this accessor family exists to prevent (moon#459)"
    );
}

#[test]
fn an_expired_hot_key_with_a_spilled_value_is_also_promoted() {
    // The arm a probe collapse is most likely to lose. The old code dropped
    // the expired hot copy and THEN, because `contains_key` had become false,
    // still went through cold promotion. `settle_not_live` must keep doing
    // both, in that order — dropping an expired HOT copy is not a statement
    // about the cold plane.
    //
    // The expired copy is written FIRST: `Database::set` retires the in-flight
    // record for a key it overwrites, so parking after the write is what keeps
    // this test from passing vacuously against an empty spill plane.
    let mut db = db_at(NOW);
    db.set(
        b"s",
        Entry::new_string_with_expiry(Bytes::from_static(b"stale"), NOW - 1),
    );
    park_in_flight(&mut db, b"s", spilled_set_body(&[b"alpha", b"beta"]));
    assert!(
        db.spill_inflight_entry(b"s", NOW).is_some(),
        "fixture: there must be something to promote, or this test is vacuous"
    );

    let len = match db.get_or_create::<SetKind>(b"s") {
        Ok(set) => set.len(),
        Err(e) => panic!("expected the promoted set, got {e:?}"),
    };
    assert_eq!(
        len, 2,
        "an EXPIRED hot key must still promote its spilled value rather than \
         fabricate an empty container over it (moon#459 + moon#541)"
    );
    assert_eq!(
        db.expires_count(),
        0,
        "the expired incarnation must have left the expiry index (moon#541)"
    );
}
