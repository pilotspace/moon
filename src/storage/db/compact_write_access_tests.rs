//! moon#1221 review INTEG-5 (refs moon#1161): a write to an EXISTING small
//! collection — listpack hash / list / set / zset, intset — is an access for
//! the eviction policy, recorded exactly once per command.
//!
//! moon#1161 made `get_or_create` (the full-encoding write accessor) record
//! redis's `lookupKeyWrite` access, but the five compact-encoding write
//! accessors (`get_or_create_{hash,list,set,zset}_listpack`,
//! `get_or_create_intset`) recorded nothing — and they are the path every
//! small collection takes (WS3 keeps lists listpack through LTRIM / LREM /
//! LINSERT / LMOVE). Under `allkeys-lru` a small hash written every second
//! aged like a key nobody touched. The write routers that probe a key's
//! encoding first (`list_route`, `set_route`) must not count as a second
//! access either.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::db::Database;
use crate::storage::entry::{AccessTracking, ClockPin};
use crate::storage::eviction::force_access_tracking;

const T0: u32 = 1_800_000_000;
const ROUNDS: u8 = 10;

fn at(db: &mut Database, secs: u32) -> ClockPin {
    let pin = ClockPin::set(secs, u64::from(secs) * 1000);
    db.refresh_now();
    pin
}

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

fn is_compact(db: &Database, key: &[u8]) -> bool {
    matches!(
        db.data().get(key).map(|e| e.value.as_redis_value()),
        Some(
            RedisValueRef::HashListpack(_)
                | RedisValueRef::ListListpack(_)
                | RedisValueRef::SetListpack(_)
                | RedisValueRef::SetIntset(_)
                | RedisValueRef::SortedSetListpack(_)
        )
    )
}

/// The reviewer's repro: under LRU, a write 100 s after creation must move
/// each compact key's access stamp.
#[test]
fn writes_to_compact_collections_record_lru_access() {
    let _lru = force_access_tracking(AccessTracking::Lru);
    let mut db = Database::new();
    let _p0 = at(&mut db, T0);
    run(&mut db, b"HSET", &[b"h", b"f", b"v"]);
    run(&mut db, b"RPUSH", &[b"l", b"a"]);
    run(&mut db, b"SADD", &[b"s", b"m"]);
    run(&mut db, b"SADD", &[b"i", b"1"]);
    run(&mut db, b"ZADD", &[b"z", b"1", b"m"]);
    let _p1 = at(&mut db, T0 + 100);
    run(&mut db, b"HSET", &[b"h", b"f", b"v2"]);
    run(&mut db, b"RPUSH", &[b"l", b"b"]);
    run(&mut db, b"SADD", &[b"s", b"n"]);
    run(&mut db, b"SADD", &[b"i", b"2"]);
    run(&mut db, b"ZADD", &[b"z", b"2", b"n"]);
    let mut stale = Vec::new();
    for k in [&b"h"[..], b"l", b"s", b"i", b"z"] {
        assert!(is_compact(&db, k), "fixture: {:?} must stay compact", k);
        if db.data().get(k).expect("key").last_access() != T0 + 100 {
            stale.push(String::from_utf8_lossy(k).into_owned());
        }
    }
    assert!(
        stale.is_empty(),
        "compact-encoding writes recorded no access: {stale:?}"
    );
}

/// One command line, command first.
fn run_line(db: &mut Database, line: &[&str]) -> Frame {
    let args: Vec<&[u8]> = line[1..].iter().map(|a| a.as_bytes()).collect();
    run(db, line[0].as_bytes(), &args)
}

struct Case {
    name: &'static str,
    setup: Vec<Vec<String>>,
    op: &'static [&'static str],
    compact: bool,
}

fn case(name: &'static str, setup: &[&[&str]], op: &'static [&'static str], compact: bool) -> Case {
    Case {
        name,
        setup: setup
            .iter()
            .map(|l| l.iter().map(|a| (*a).to_string()).collect())
            .collect(),
        op,
        compact,
    }
}

/// Each write command, run ROUNDS times against an existing key, moves the
/// LFU counter by exactly ROUNDS (`log_factor 0`, no decay: every recorded
/// access is +1). `compact` says which encoding the fixture pins.
#[test]
fn collection_writes_record_exactly_one_access_per_command() {
    let mut big_list = vec!["RPUSH".to_string(), "big".to_string()];
    let mut big_set = vec!["SADD".to_string(), "big".to_string()];
    for i in 0..200 {
        big_list.push(format!("m{i:03}"));
        big_set.push(format!("m{i:03}"));
    }
    let twelve: &[&str] = &[
        "RPUSH", "l", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
    ];
    let mut cases = vec![
        case(
            "HSET",
            &[&["HSET", "h", "f0", "v0"]],
            &["HSET", "h", "f", "v"],
            true,
        ),
        case(
            "HINCRBY",
            &[&["HSET", "h", "n", "0"]],
            &["HINCRBY", "h", "n", "1"],
            true,
        ),
        case("RPUSH", &[&["RPUSH", "l", "a"]], &["RPUSH", "l", "x"], true),
        case("LPUSH", &[&["RPUSH", "l", "a"]], &["LPUSH", "l", "x"], true),
        case("LPOP", &[twelve], &["LPOP", "l"], true),
        case(
            "LSET",
            &[&["RPUSH", "l", "a", "b"]],
            &["LSET", "l", "0", "z"],
            true,
        ),
        case(
            "LREM",
            &[&["RPUSH", "l", "a", "b"]],
            &["LREM", "l", "0", "zz"],
            true,
        ),
        case(
            "LTRIM",
            &[&["RPUSH", "l", "a", "b"]],
            &["LTRIM", "l", "0", "-1"],
            true,
        ),
        case(
            "LINSERT",
            &[&["RPUSH", "l", "a"]],
            &["LINSERT", "l", "BEFORE", "a", "x"],
            true,
        ),
        case(
            "SADD listpack",
            &[&["SADD", "s", "m0"]],
            &["SADD", "s", "m"],
            true,
        ),
        case(
            "SADD intset",
            &[&["SADD", "i", "1"]],
            &["SADD", "i", "2"],
            true,
        ),
        case(
            "SREM listpack",
            &[&["SADD", "s", "m0", "m1"]],
            &["SREM", "s", "nope"],
            true,
        ),
        case(
            "SREM intset",
            &[&["SADD", "i", "1", "2"]],
            &["SREM", "i", "99"],
            true,
        ),
        case(
            "ZADD",
            &[&["ZADD", "z", "1", "m0"]],
            &["ZADD", "z", "2", "m"],
            true,
        ),
        case(
            "ZINCRBY",
            &[&["ZADD", "z", "1", "m"]],
            &["ZINCRBY", "z", "1", "m"],
            true,
        ),
    ];
    // Full encodings: the routers' encoding probe must not be a second access.
    let mut full_list = case("LPOP full", &[], &["LPOP", "big"], false);
    full_list.setup = vec![big_list];
    let mut full_set = case("SREM full", &[], &["SREM", "big", "nope"], false);
    full_set.setup = vec![big_set];
    cases.push(full_list);
    cases.push(full_set);

    let _lfu = force_access_tracking(AccessTracking::Lfu {
        log_factor: 0,
        decay_time: 0,
    });
    let mut failures = Vec::new();
    for c in cases {
        let mut db = Database::new();
        let _p = at(&mut db, T0);
        for line in &c.setup {
            let line: Vec<&str> = line.iter().map(String::as_str).collect();
            let r = run_line(&mut db, &line);
            assert!(!matches!(r, Frame::Error(_)), "{} setup: {r:?}", c.name);
        }
        let key = c.op[1].as_bytes();
        let start = db.data().get(key).expect("fixture key").access_counter();
        for _ in 0..ROUNDS {
            let r = run_line(&mut db, c.op);
            assert!(!matches!(r, Frame::Error(_)), "{}: {r:?}", c.name);
        }
        assert_eq!(
            is_compact(&db, key),
            c.compact,
            "{}: fixture encoding changed",
            c.name
        );
        let end = db.data().get(key).expect("key").access_counter();
        if end.wrapping_sub(start) != ROUNDS {
            failures.push(format!("{}: +{}", c.name, end.wrapping_sub(start)));
        }
    }
    assert!(
        failures.is_empty(),
        "LFU counter delta after {ROUNDS} commands (want +{ROUNDS}): {failures:?}"
    );
}

/// A write that CREATES the key is not an access (redis creates it with a
/// fresh `LFU_INIT_VAL` counter) — on every compact kind.
#[test]
fn a_write_that_creates_a_compact_key_starts_at_the_initial_frequency() {
    let _lfu = force_access_tracking(AccessTracking::Lfu {
        log_factor: 0,
        decay_time: 0,
    });
    let mut db = Database::new();
    run(&mut db, b"HSET", &[b"h", b"f", b"v"]);
    run(&mut db, b"RPUSH", &[b"l", b"a"]);
    run(&mut db, b"SADD", &[b"s", b"m"]);
    run(&mut db, b"SADD", &[b"i", b"1"]);
    run(&mut db, b"ZADD", &[b"z", b"1", b"m"]);
    for k in [&b"h"[..], b"l", b"s", b"i", b"z"] {
        assert!(is_compact(&db, k));
        assert_eq!(
            db.data().get(k).expect("key").access_counter(),
            5,
            "{:?}: creation recorded an access",
            String::from_utf8_lossy(k)
        );
    }
}
