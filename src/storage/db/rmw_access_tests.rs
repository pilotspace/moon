//! moon#1221 review F3 (refs moon#1161): an INCR-family command records
//! exactly ONE access for the eviction policy, like redis's `lookupKeyWrite`
//! + `dbOverwrite`.
//!
//! Two defects, one property:
//! - the in-place INCR/DECR/INCRBY/DECRBY path (`Database::incr_string`,
//!   moon#942) reset the LFU counter to `LFU_INIT_VAL` on every call, so a
//!   write-hot counter never gained frequency and was the first thing
//!   `allkeys-lfu` evicted (300 INCRs: FREQ 5; 300 SETs: 11-13);
//! - INCRBYFLOAT (and the general INCR path a TTL-expired or cold key takes)
//!   read the key with `Database::get` (an access) and then wrote it with
//!   `Database::set`, whose overwrite records the access again — two Morris
//!   increments per command under LFU. (APPEND / SETRANGE / SETBIT /
//!   BITFIELD had the same shape; WS2's moon#1168 rewrites them in place.)
//!
//! `LFU { log_factor: 0, decay_time: 0 }` makes the Morris increment
//! deterministic (probability 1) and switches decay off, so the counter
//! counts recorded accesses exactly.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::storage::entry::{AccessTracking, ClockPin, Entry};
use crate::storage::eviction::force_access_tracking;

const T0: u32 = 1_800_000_000;
const ROUNDS: u8 = 10;
/// `LFU_INIT_VAL`: the counter every new key starts at.
const INIT: u8 = 5;

fn run(db: &mut Database, cmd: &[u8], args: &[&[u8]]) -> Frame {
    let frames: Vec<Frame> = args
        .iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
        .collect();
    let mut selected = 0usize;
    match crate::command::dispatch(db, cmd, &frames, &mut selected, 16) {
        crate::command::DispatchResult::Response(f) | crate::command::DispatchResult::Quit(f) => f,
    }
}

fn counter(db: &Database, key: &[u8]) -> u8 {
    db.data().get(key).expect("key").access_counter()
}

fn exact_lfu() -> crate::storage::eviction::ForceAccessTracking {
    force_access_tracking(AccessTracking::Lfu {
        log_factor: 0,
        decay_time: 0,
    })
}

/// The reviewer's repro, verbatim in spirit: default LFU parameters, 300
/// INCRs against 300 SET overwrites of a sibling key.
#[test]
fn incr_under_lfu_keeps_growing_frequency_like_set() {
    let _lfu = force_access_tracking(AccessTracking::Lfu {
        log_factor: 10,
        decay_time: 1,
    });
    let mut db = Database::new();
    let _p = ClockPin::set(T0, u64::from(T0) * 1000);
    db.refresh_now();
    db.set(b"ctr", Entry::new_string(Bytes::from_static(b"0")));
    db.set(b"str", Entry::new_string(Bytes::from_static(b"v")));
    for _ in 0..300 {
        let r = run(&mut db, b"INCR", &[b"ctr"]);
        assert!(matches!(r, Frame::Integer(_)), "INCR failed: {r:?}");
        db.set(b"str", Entry::new_string(Bytes::from_static(b"v")));
    }
    let via_set = counter(&db, b"str");
    let via_incr = counter(&db, b"ctr");
    assert!(
        via_set > 5,
        "control: 300 SET overwrites must grow FREQ (got {via_set})"
    );
    assert!(
        via_incr > 5,
        "300 INCRs left the LFU counter at {via_incr} while 300 SETs reached {via_set} \
         (redis INCR = lookupKeyWrite: the counter grows)"
    );
}

/// Each command of the INCR family, run ROUNDS times on an existing key,
/// moves the counter by exactly ROUNDS — one access per command, never zero,
/// never two. SET and GET are the controls.
#[test]
fn incr_family_commands_record_exactly_one_access() {
    type Case = (&'static str, &'static [u8], &'static [&'static [u8]]);
    let cases: [Case; 8] = [
        ("SET", b"SET", &[b"k", b"1"]),
        ("GET", b"GET", &[b"k"]),
        ("INCR", b"INCR", &[b"k"]),
        ("DECR", b"DECR", &[b"k"]),
        ("INCRBY", b"INCRBY", &[b"k", b"7"]),
        ("DECRBY", b"DECRBY", &[b"k", b"3"]),
        ("INCRBYFLOAT", b"INCRBYFLOAT", &[b"k", b"0.5"]),
        ("INCRBYFLOAT on an integer", b"INCRBYFLOAT", &[b"k", b"2"]),
    ];
    let _lfu = exact_lfu();
    let mut failures = Vec::new();
    for (name, cmd, args) in cases {
        let mut db = Database::new();
        let _p = ClockPin::set(T0, u64::from(T0) * 1000);
        db.refresh_now();
        db.set(b"k", Entry::new_string(Bytes::from_static(b"1")));
        assert_eq!(counter(&db, b"k"), INIT, "{name}: fixture");
        for _ in 0..ROUNDS {
            let r = run(&mut db, cmd, args);
            assert!(!matches!(r, Frame::Error(_)), "{name} failed: {r:?}");
        }
        let got = counter(&db, b"k");
        if got != INIT + ROUNDS {
            failures.push(format!("{name}: {got} (want {})", INIT + ROUNDS));
        }
    }
    assert!(
        failures.is_empty(),
        "LFU counter after {ROUNDS} commands: {failures:?}"
    );
}

/// redis records the lookup BEFORE it type- or value-checks the key, so a
/// refused read-modify-write still counts as one access.
#[test]
fn a_refused_read_modify_write_still_records_its_one_lookup() {
    type Case = (&'static str, &'static [u8], &'static [&'static [u8]]);
    let cases: [Case; 3] = [
        ("INCR not-an-integer", b"INCR", &[b"k"]),
        ("INCRBYFLOAT not-a-float", b"INCRBYFLOAT", &[b"k", b"1"]),
        ("DECR not-an-integer", b"DECR", &[b"k"]),
    ];
    let _lfu = exact_lfu();
    let mut failures = Vec::new();
    for (name, cmd, args) in cases {
        let mut db = Database::new();
        db.set(b"k", Entry::new_string(Bytes::from_static(b"abc")));
        for _ in 0..ROUNDS {
            let r = run(&mut db, cmd, args);
            assert!(
                matches!(r, Frame::Error(_)),
                "{name} must be refused: {r:?}"
            );
        }
        let got = counter(&db, b"k");
        if got != INIT + ROUNDS {
            failures.push(format!("{name}: {got} (want {})", INIT + ROUNDS));
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}

/// A key INCR creates starts at `LFU_INIT_VAL`, like any new key.
#[test]
fn a_counter_incr_creates_starts_at_the_initial_frequency() {
    let _lfu = exact_lfu();
    let mut db = Database::new();
    assert_eq!(run(&mut db, b"INCR", &[b"fresh"]), Frame::Integer(1));
    assert_eq!(counter(&db, b"fresh"), INIT);
}
