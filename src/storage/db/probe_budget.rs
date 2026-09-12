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
use crate::storage::db::SetHandle;
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
        (2, 3),
        "get_or_create probe budget moved (hit={hit}, miss={miss}) — moon#942. \
         The hit path is `get` (liveness) then `get_mut` (hand out); the miss \
         path adds ONLY the `insert`. The fourth probe was \
         `promote_cold_if_present`'s own `contains_key`, re-answering a \
         question `hot_state` had just answered one probe earlier; \
         `settle_not_live` now calls `promote_cold_known_absent`, which is \
         that method without the re-ask. A RISE is the regression this test \
         exists to catch."
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
        (2, 2),
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
        (2, 2),
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
        (2, 3),
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
        (2, 3),
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
        (2, 3),
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
        (2, 3),
        "get_or_create_zset_listpack probe budget moved (hit={hit}, miss={miss}) — moon#942."
    );
}

#[test]
fn get_or_create_set_listpack_probe_budget() {
    let mut db = db_at(NOW);

    let (r, miss) = probes(|| {
        db.get_or_create_set_listpack(b"s", |_, _| false)
            .map(|h| matches!(h, SetHandle::Listpack(_)))
    });
    assert_eq!(r, Ok(true));

    let (r, hit) = probes(|| {
        db.get_or_create_set_listpack(b"s", |_, _| false)
            .map(|h| matches!(h, SetHandle::Listpack(_)))
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
            .map(|h| matches!(h, SetHandle::Listpack(_)))
    });
    assert_eq!(
        r,
        Ok(true),
        "the intset must have been absorbed into a listpack"
    );

    assert_eq!(
        (hit, miss, absorb),
        (2, 3, 2),
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
        (3, 2, 2),
        "SADD end-to-end probe budget moved \
         (create={create}, listpack_hit={listpack_hit}, hashtable_hit={hashtable_hit}) \
         — moon#942. All three arms now pay ONE accessor. The hashtable arm \
         used to pay two: `get_or_create_set_listpack` answered `Ok(None)` and \
         `get_or_create_set` then repeated the whole skeleton — a second \
         classification and probe pair for a key the first call already had in \
         hand. `SetHandle::Full` hands that entry back instead, running \
         `SetKind::upgrade` and its ledger delta on the handle the accessor \
         already holds. A RISE is the regression this test exists to catch."
    );
}

#[test]
fn sadd_onto_a_refused_intset_probe_budget() {
    // The other `SetHandle::Full` arm: a live `SetIntset` the moon#899 edge
    // REFUSES (the intset already holds more members than the listpack policy
    // allows), so the batch has to promote it to the full `IndexSet`. That
    // used to be `get_or_create_set_listpack` answering `Ok(None)` followed by
    // `get_or_create_set` re-classifying the very same entry.
    let mut db = db_at(NOW);
    let mut ints = vec![bulk("i")];
    ints.extend((0..200).map(|v| bulk(&v.to_string())));
    assert_eq!(
        crate::command::set::sadd(&mut db, &ints),
        Frame::Integer(200)
    );
    assert!(
        matches!(
            db.data().get(b"i").map(|e| e.value.as_redis_value()),
            Some(crate::storage::compact_value::RedisValueRef::SetIntset(_))
        ),
        "fixture: 200 canonical integers must still be an intset"
    );

    let args = [bulk("i"), bulk("not-an-integer")];
    let (r, refused) = probes(|| crate::command::set::sadd(&mut db, &args));
    assert_eq!(r, Frame::Integer(1));
    assert_eq!(
        refused, 2,
        "SADD onto a refused intset costs {refused} probes — moon#942. The \
         accessor already holds the entry; promoting it must not cost a \
         second full classification."
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
        .map(|h| matches!(h, SetHandle::Listpack(_))));
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
        Ok(SetHandle::Listpack(_)) => {}
        other => panic!(
            "the intset should have been absorbed, got {:?}",
            other.map(|h| matches!(h, SetHandle::Listpack(_)))
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

// ── the precondition `promote_cold_known_absent` is sold on (moon#942) ──────

#[test]
fn a_hot_key_with_an_inflight_record_is_not_clobbered_by_promotion() {
    // moon#942 removed `promote_cold_if_present`'s opening `contains_key`
    // from the ACCESSOR path only, by routing `settle_not_live` to
    // `promote_cold_known_absent`. This test pins WHY the split exists
    // rather than just deleting the probe outright.
    //
    // `promote_inflight_if_present` does not re-check hot residency: it
    // forgets the in-flight record and calls `Database::set` unconditionally.
    // So on a key that is hot AND still carries an in-flight spill record,
    // the `contains_key` is the only thing between a live value and the
    // older spilled body overwriting it. `promote_cold_outcome` (the on-disk
    // arm) carries its own guard; the in-flight arm does not.
    //
    // Mutation check: point `promote_cold_if_present` straight at
    // `promote_cold_known_absent` and this test reports 3 members — the
    // spilled body — instead of 2.
    let mut db = db_at(NOW);
    match db.get_or_create::<SetKind>(b"s") {
        Ok(set) => {
            set.insert(Bytes::from_static(b"live-one"));
            set.insert(Bytes::from_static(b"live-two"));
        }
        Err(e) => panic!("fixture: could not build the hot set: {e:?}"),
    }
    // Parked AFTER the write: `Database::set` retires the in-flight record
    // for a key it overwrites, so parking first would leave nothing to
    // clobber with and the test would pass vacuously.
    park_in_flight(
        &mut db,
        b"s",
        spilled_set_body(&[b"alpha", b"beta", b"gamma"]),
    );
    assert!(
        db.spill_inflight_entry(b"s", NOW).is_some(),
        "fixture: there must be a record that COULD clobber, or this is vacuous"
    );

    assert!(
        db.promote_cold_if_present(b"s", NOW),
        "a hot key is present in hot RAM after the call, by the contract"
    );

    let set = match db.get_ref_if_alive::<SetKind>(b"s", NOW) {
        Ok(Some(r)) => r,
        other => panic!("the hot set vanished: {:?}", other.map(|o| o.is_some())),
    };
    assert_eq!(
        set.len(),
        2,
        "`promote_cold_if_present` overwrote a LIVE value with an older          in-flight spill body — the hot guard it opens with is load-bearing,          and only a caller that has just looked (accessors::settle_not_live)          may skip it via `promote_cold_known_absent`"
    );
}

#[test]
fn a_key_deleted_mid_spill_does_not_resurrect_through_the_accessor() {
    // moon#459, through the accessor preamble the probe collapse rewrote.
    // `Database::remove` retires the in-flight record (`remove_cold_only`),
    // which withdraws the spill completion's authorization to publish. A
    // `get_or_create` afterwards must fabricate an EMPTY container, not
    // rehydrate the body the DEL was supposed to have destroyed.
    let mut db = db_at(NOW);
    match db.get_or_create::<SetKind>(b"s") {
        Ok(set) => {
            set.insert(Bytes::from_static(b"doomed"));
        }
        Err(e) => panic!("fixture: could not build the set: {e:?}"),
    }
    park_in_flight(
        &mut db,
        b"s",
        spilled_set_body(&[b"alpha", b"beta", b"gamma"]),
    );
    assert!(
        db.spill_inflight_entry(b"s", NOW).is_some(),
        "fixture: the key must be mid-spill for this to test anything"
    );

    // `remove_counting_cold` is the DEL/UNLINK path — the one that has to
    // count a mid-spill key as removed, and the one moon#459 was about.
    let (existed, _) = db.remove_counting_cold(b"s");
    assert!(existed, "fixture: the DEL must have found the key");

    let len = match db.get_or_create::<SetKind>(b"s") {
        Ok(set) => set.len(),
        Err(e) => panic!("expected a fresh empty set, got {e:?}"),
    };
    assert_eq!(
        len, 0,
        "a key DEL'd mid-spill came back to life through the accessor — the          DEL-acked-then-UNDONE bug (moon#459)"
    );
}

// ── the WATCH contract across the collapsed accessor pair (moon#926/#940) ───

/// The entry's WATCH version, or a failed assertion naming the key.
fn version_of(db: &Database, key: &[u8]) -> u32 {
    match db.data().get(key) {
        Some(e) => e.version(),
        None => panic!("key {:?} should be present", String::from_utf8_lossy(key)),
    }
}

#[test]
fn sadd_bumps_the_watch_version_exactly_once_on_every_encoding() {
    // moon#926's rule is "acquiring a mutable handle on a stored value IS the
    // bump". SADD on a HASHTABLE set used to acquire TWO — one from
    // `get_or_create_set_listpack` (discarded, `Ok(None)`) and one from
    // `get_or_create_set` — so one command moved the version by two. That is
    // not a lost-update bug (a watcher aborts either way), but it is the
    // observable shadow of the duplicate accessor this change removes, and it
    // is the assertion that fails if the collapse ever regresses.

    // (a) listpack steady state — one accessor before and after.
    let mut db = db_at(NOW);
    assert_eq!(
        crate::command::set::sadd(&mut db, &[bulk("lp"), bulk("alpha")]),
        Frame::Integer(1)
    );
    let v0 = version_of(&db, b"lp");
    assert_eq!(
        crate::command::set::sadd(&mut db, &[bulk("lp"), bulk("beta")]),
        Frame::Integer(1)
    );
    assert_eq!(
        version_of(&db, b"lp"),
        v0 + 1,
        "a listpack SADD is exactly one mutation (moon#926)"
    );

    // (b) intset steady state — one accessor, unchanged by this work.
    let mut db = db_at(NOW);
    assert_eq!(
        crate::command::set::sadd(&mut db, &[bulk("is"), bulk("1"), bulk("2")]),
        Frame::Integer(2)
    );
    let v0 = version_of(&db, b"is");
    assert_eq!(
        crate::command::set::sadd(&mut db, &[bulk("is"), bulk("3")]),
        Frame::Integer(1)
    );
    assert_eq!(
        version_of(&db, b"is"),
        v0 + 1,
        "an intset SADD is exactly one mutation (moon#926)"
    );

    // (c) hashtable steady state — the arm that used to bump TWICE.
    let mut db = db_at(NOW);
    let mut big = vec![bulk("ht")];
    big.extend((0..400).map(|i| bulk(&format!("member-{i}"))));
    assert_eq!(
        crate::command::set::sadd(&mut db, &big),
        Frame::Integer(400)
    );
    let v0 = version_of(&db, b"ht");
    assert_eq!(
        crate::command::set::sadd(&mut db, &[bulk("ht"), bulk("extra")]),
        Frame::Integer(1)
    );
    assert_eq!(
        version_of(&db, b"ht"),
        v0 + 1,
        "a hashtable SADD acquired TWO mutable handles for one command — the \
         duplicate accessor moon#942 removes (moon#926)"
    );
}

#[test]
fn sadd_on_a_wrongtype_key_still_bumps_the_version_moon940() {
    // moon#940 is OPEN and is NOT fixed here: `stamp_mutation` fires before
    // the arm that answers `Err(WRONGTYPE)`, so a rejected write still dirties
    // a watching transaction. This test pins the CURRENT behaviour so the
    // `SetHandle` change is provably neutral on it — neither fixing #940 (a
    // separate change, separately benchmarked) nor making it worse by adding a
    // second bump.
    let mut db = db_at(NOW);
    db.set(b"str", Entry::new_string(Bytes::from_static(b"v")));
    let v0 = version_of(&db, b"str");
    let r = crate::command::set::sadd(&mut db, &[bulk("str"), bulk("member")]);
    assert!(
        matches!(&r, Frame::Error(e) if e.starts_with(b"WRONGTYPE")),
        "expected WRONGTYPE, got {r:?}"
    );
    assert_eq!(
        version_of(&db, b"str"),
        v0 + 1,
        "moon#940 (open): a rejected SADD bumps the version exactly once. \
         Two would mean the collapse added a handle; zero would mean this \
         change fixed #940 as a side effect, which it must not do silently."
    );
}

// ── end to end: the LIST family (moon#942) ──────────────────────────────────
//
// The benchmark's own shape (`scripts/bench-ab-matrix.sh:81`) is
// `LPUSH list:<12-digit> xxxxxxxx` over `-r 100000`. That keyspace matters
// more than it looks: one leg pushes ~2.0M elements across 100,000 keys, so a
// `list:` key holds roughly 5 elements when the p=64 point starts and roughly
// 20 when it ends. `list-max-listpack-size` is 128
// (`encoding_limits.rs:69`), so **every timed LPUSH in the matrix lands on
// the LISTPACK arm**, never on the full `VecDeque`. Any claim about the
// benchmark row has to be a claim about `listpack_hit`.
const LKEY: &str = "list:000000000042";
const LELEM: &str = "xxxxxxxx";

/// A list of `len` elements under [`LKEY`], on whatever encoding `len`
/// implies, with a pinned clock.
fn list_of(len: usize) -> Database {
    let mut db = db_at(NOW);
    let args = [bulk(LKEY), bulk(LELEM)];
    for _ in 0..len {
        crate::command::list::lpush(&mut db, &args);
    }
    db
}

/// `OBJECT ENCODING key`, as a `String`, through the read-only path.
fn encoding_of(db: &mut Database, key: &str) -> String {
    match crate::command::key::object(db, &[bulk("ENCODING"), bulk(key)]) {
        Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("OBJECT ENCODING answered {other:?}"),
    }
}

#[test]
fn lpush_end_to_end_probe_budget() {
    let args = [bulk(LKEY), bulk(LELEM)];

    // (a) Absent key -> a ListListpack is fabricated.
    let mut db = db_at(NOW);
    let (r, create) = probes(|| crate::command::list::lpush(&mut db, &args));
    assert_eq!(r, Frame::Integer(1));

    // (b) Steady state on the LISTPACK encoding — the benchmark's own arm.
    let mut db = list_of(20);
    let (r, listpack_hit) = probes(|| crate::command::list::lpush(&mut db, &args));
    assert_eq!(r, Frame::Integer(21));
    assert_eq!(
        encoding_of(&mut db, LKEY),
        "listpack",
        "fixture: 21 elements must still be a listpack, or this row is \
         measuring the wrong arm"
    );

    // (c) Steady state on the FULL encoding, past `list-max-listpack-size`.
    let mut db = list_of(200);
    let (r, full_hit) = probes(|| crate::command::list::lpush(&mut db, &args));
    assert_eq!(r, Frame::Integer(201));
    assert_eq!(encoding_of(&mut db, LKEY), "linkedlist", "fixture");

    assert_eq!(
        (create, listpack_hit, full_hit),
        (3, 2, 4),
        "LPUSH end-to-end probe budget moved \
         (create={create}, listpack_hit={listpack_hit}, full_hit={full_hit}) \
         — moon#942. `create` and `listpack_hit` are at the floor the shared \
         accessor skeleton allows (`hot_state`, then `get_mut`, plus the \
         `insert_fresh` a miss adds) and a RISE in either is the regression \
         this row exists to catch.\n\
         \n\
         `full_hit` is 4 because the residue is STILL OPEN, and this is the \
         row that will fall when it closes. `get_or_create_list_listpack` \
         answers `Ok(None)` for a key that already holds the full \
         `VecDeque` (`accessors.rs:657`), and `lpush` answers that by \
         calling `get_or_create_list` (`list_write.rs:81`) — which re-runs \
         the whole skeleton (`hot_state`, `settle_not_live`, `get_mut`, \
         `stamp_mutation`, `ListKind::upgrade`) against the key the first \
         call had already classified and was still holding. It is the exact \
         shape moon#942 closed for `SADD` in a4ae9775 by returning a \
         `SetHandle` instead of `Ok(None)`, and closing it here needs the \
         same change to `accessors.rs`. Redis pays one `dictFind` for all \
         three arms.\n\
         \n\
         NOTE the benchmark does NOT exercise `full_hit`: at `-r 100000` a \
         `list:` key holds ~5-20 elements against a 128-element threshold, \
         so the matrix row is `listpack_hit` and closing the residue cannot \
         move it. It is worth closing for real lists, which are longer than \
         a benchmark's."
    );
}

#[test]
fn lpop_rpop_probe_budget() {
    let pop = [bulk(LKEY)];

    // (a) LISTPACK: the `&self` router (moon#897, one probe, cannot flatten)
    //     plus the listpack accessor's own pair.
    let mut db = list_of(20);
    let (r, listpack) = probes(|| crate::command::list::lpop(&mut db, &pop));
    assert_eq!(r, Frame::BulkString(Bytes::from_static(LELEM.as_bytes())));
    assert_eq!(encoding_of(&mut db, LKEY), "listpack", "fixture");

    let mut db = list_of(20);
    let (_, listpack_rpop) = probes(|| crate::command::list::rpop(&mut db, &pop));

    // (b) FULL: the router plus `get_or_create_list`.
    let mut db = list_of(200);
    let (r, full) = probes(|| crate::command::list::lpop(&mut db, &pop));
    assert_eq!(r, Frame::BulkString(Bytes::from_static(LELEM.as_bytes())));
    assert_eq!(encoding_of(&mut db, LKEY), "linkedlist", "fixture");

    let mut db = list_of(200);
    let (_, full_rpop) = probes(|| crate::command::list::rpop(&mut db, &pop));

    assert_eq!(
        (listpack, listpack_rpop, full, full_rpop),
        (3, 3, 3, 3),
        "LPOP/RPOP probe budget moved (listpack LPOP={listpack}, \
         listpack RPOP={listpack_rpop}, full LPOP={full}, \
         full RPOP={full_rpop}) — moon#942.\n\
         \n\
         The full arm used to be 4: `pop_eager` popped through \
         `get_or_create_list`, let that borrow end, and then asked \
         `get_list_ref_if_alive` a FOURTH time whether the list was now \
         empty — a question the `&mut VecDeque` it had just been holding \
         answers for free. `list.is_empty()` is read inside the existing \
         borrow instead.\n\
         \n\
         Three is the floor without a change to `accessors.rs`: the `&self` \
         router is what stops the pop flattening the compact encoding \
         (moon#897/#832), and it cannot be folded into the mutable accessor \
         from `src/command/`."
    );
}

#[test]
fn lpushx_rpushx_probe_budget() {
    let args = [bulk(LKEY), bulk(LELEM)];

    // (a) HIT on the listpack encoding.
    let mut db = list_of(20);
    let (r, listpack_hit) = probes(|| crate::command::list::lpushx(&mut db, &args));
    assert_eq!(r, Frame::Integer(21));

    // (b) HIT on the full encoding.
    let mut db = list_of(200);
    let (r, full_hit) = probes(|| crate::command::list::rpushx(&mut db, &args));
    assert_eq!(r, Frame::Integer(201));

    // (c) MISS: no such key. LPUSHX must NOT fabricate one.
    let mut db = db_at(NOW);
    let (r, miss) = probes(|| crate::command::list::lpushx(&mut db, &args));
    assert_eq!(r, Frame::Integer(0));
    assert!(
        db.data().get(LKEY.as_bytes()).is_none(),
        "LPUSHX on a missing key must not fabricate it (moon#830)"
    );

    // (d) WRONGTYPE.
    let mut db = db_at(NOW);
    db.set(b"str", Entry::new_string(Bytes::from_static(b"v")));
    let wrong = [bulk("str"), bulk(LELEM)];
    let (r, wrongtype) = probes(|| crate::command::list::rpushx(&mut db, &wrong));
    assert!(
        matches!(&r, Frame::Error(e) if e.starts_with(b"WRONGTYPE")),
        "expected WRONGTYPE, got {r:?}"
    );

    assert_eq!(
        (listpack_hit, full_hit, miss, wrongtype),
        (3, 3, 1, 1),
        "LPUSHX/RPUSHX probe budget moved (listpack_hit={listpack_hit}, \
         full_hit={full_hit}, miss={miss}, wrongtype={wrongtype}) — \
         moon#942. The existence-and-type gate used to be `db.get_list(key)` \
         = `get_promoted`, which costs two probes AND flattens the compact \
         encoding on the way past (moon#832) — the same pair moon#897 \
         removed from `LPOP`'s gate. It is now the `&self` router, which \
         costs one and cannot rewrite anything. The mutable accessor behind \
         it is unchanged, so the encoding OUTCOME is unchanged too; only the \
         redundant probe is gone."
    );
}

#[test]
fn list_writes_bump_the_watch_version_exactly_once() {
    // moon#926's rule is "acquiring a mutable handle on a stored value IS the
    // bump". This is the observable shadow of the accessor budget above, and
    // the guard that stops the `LPUSHX` gate swap from changing WATCH
    // semantics as a side effect.
    let args = [bulk(LKEY), bulk(LELEM)];
    let k = LKEY.as_bytes();

    // (a) LPUSH, listpack steady state — ONE handle.
    let mut db = list_of(20);
    let v0 = version_of(&db, k);
    assert_eq!(
        crate::command::list::lpush(&mut db, &args),
        Frame::Integer(21)
    );
    assert_eq!(
        version_of(&db, k),
        v0 + 1,
        "a listpack LPUSH is exactly one mutation (moon#926)"
    );

    // (b) LPUSH, full steady state — TWO, and that is the OPEN residue.
    let mut db = list_of(200);
    let v0 = version_of(&db, k);
    assert_eq!(
        crate::command::list::lpush(&mut db, &args),
        Frame::Integer(201)
    );
    assert_eq!(
        version_of(&db, k),
        v0 + 2,
        "moon#942 (open): one LPUSH onto a `linkedlist` moves a watched \
         key's version by TWO, because it acquires two mutable handles — \
         `get_or_create_list_listpack` (discarded, `Ok(None)`) and then \
         `get_or_create_set`'s list twin. Not a lost update (a watcher \
         aborts either way) but the observable tell that the duplicate \
         accessor is still there. This assertion is written to the CURRENT \
         value on purpose: when `accessors.rs` grows the `ListHandle` that \
         a4ae9775 gave sets, this line becomes `v0 + 1` in the same commit, \
         and until then a move in EITHER direction is a change nobody \
         intended"
    );

    // (c) LPOP on both encodings — one handle each.
    for (len, what) in [(20usize, "listpack"), (200, "linkedlist")] {
        let mut db = list_of(len);
        let v0 = version_of(&db, k);
        assert!(matches!(
            crate::command::list::lpop(&mut db, &[bulk(LKEY)]),
            Frame::BulkString(_)
        ));
        assert_eq!(
            version_of(&db, k),
            v0 + 1,
            "a {what} LPOP is exactly one mutation (moon#926)"
        );
    }

    // (d) LPUSHX: one bump on a hit, and — the half that matters for the
    //     gate swap — ZERO on a miss and ZERO on WRONGTYPE.
    let mut db = list_of(20);
    let v0 = version_of(&db, k);
    assert_eq!(
        crate::command::list::lpushx(&mut db, &args),
        Frame::Integer(21)
    );
    assert_eq!(
        version_of(&db, k),
        v0 + 1,
        "an LPUSHX that pushes is exactly one mutation (moon#926)"
    );

    let mut db = db_at(NOW);
    db.set(b"str", Entry::new_string(Bytes::from_static(b"v")));
    let v0 = version_of(&db, b"str");
    let r = crate::command::list::lpushx(&mut db, &[bulk("str"), bulk(LELEM)]);
    assert!(matches!(&r, Frame::Error(e) if e.starts_with(b"WRONGTYPE")));
    assert_eq!(
        version_of(&db, b"str"),
        v0,
        "an LPUSHX REFUSED for WRONGTYPE must not dirty the key. moon#940 \
         (open) is that `LPUSH` does — `stamp_mutation` fires inside \
         `get_or_create_list_listpack` before the `Err(WRONGTYPE)` arm — but \
         `LPUSHX` reaches its answer through a gate that never takes a \
         mutable handle, and must keep doing so. A gate swapped to \
         `get_mut_if_present` would cost one probe less and silently WIDEN \
         moon#940 to a second command; that is the trade this assertion \
         exists to refuse"
    );
}
