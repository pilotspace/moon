//! R1 findings 4 and 9 (moon#1286): an absolute deadline already in the past
//! deletes the key at once, as redis's `checkAlreadyExpired` does. It is a
//! deletion, not an expiry: `expired_keys` does not move (and `del` is
//! published — checked end to end in `tests/expired_keys_parity_1286.rs`).
//! Counts are read from the calling thread's exact mirror.

use bytes::Bytes;

use crate::admin::metrics_setup::this_thread_expired_keys as counted;
use crate::command::dump_restore::{dump, restore};
use crate::command::key::{expireat, pexpireat};
use crate::command::string::getex;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::{Entry, current_time_ms};

fn bulk(s: &str) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
}

fn with_key() -> Database {
    let mut db = Database::new();
    db.set(b"k", Entry::new_string(Bytes::from_static(b"v")));
    db
}

/// Nothing the command left behind is reaped (and counted) later either.
fn assert_gone_uncounted(db: &mut Database, before: u64, what: &str) {
    assert!(!db.exists(b"k"), "{what}: the key is deleted at once");
    crate::server::expiration::expire_cycle_direct(db, &mut |_| {});
    assert_eq!(counted(), before, "{what}: a deletion, not an expiry");
}

#[test]
fn expireat_and_pexpireat_in_the_past_delete_without_counting() {
    let past_s = (current_time_ms() / 1000 - 10).to_string();
    let past_ms = (current_time_ms() - 1000).to_string();
    for (what, args, pexpire) in [
        ("EXPIREAT k 1", vec![bulk("k"), bulk("1")], false),
        ("EXPIREAT k now-10", vec![bulk("k"), bulk(&past_s)], false),
        ("PEXPIREAT k 1", vec![bulk("k"), bulk("1")], true),
        (
            "PEXPIREAT k now-1000",
            vec![bulk("k"), bulk(&past_ms)],
            true,
        ),
        (
            "EXPIREAT k 1 LT",
            vec![bulk("k"), bulk("1"), bulk("LT")],
            false,
        ),
    ] {
        let mut db = with_key();
        let before = counted();
        let reply = if pexpire {
            pexpireat(&mut db, &args)
        } else {
            expireat(&mut db, &args)
        };
        assert_eq!(reply, Frame::Integer(1), "{what}");
        assert_gone_uncounted(&mut db, before, what);
    }
}

#[test]
fn a_blocked_condition_or_a_future_deadline_keeps_the_key() {
    let mut db = with_key();
    assert_eq!(
        expireat(&mut db, &[bulk("k"), bulk("1"), bulk("GT")]),
        Frame::Integer(0),
        "GT against no TTL (infinite) is blocked, as in redis"
    );
    assert!(db.exists(b"k"));
    let future = (current_time_ms() / 1000 + 100).to_string();
    assert_eq!(
        expireat(&mut db, &[bulk("k"), bulk(&future)]),
        Frame::Integer(1)
    );
    assert!(db.exists(b"k"));
    assert!(db.get(b"k").is_some_and(|e| e.has_expiry()));
    let mut empty = Database::new();
    assert_eq!(
        expireat(&mut empty, &[bulk("k"), bulk("1")]),
        Frame::Integer(0)
    );
}

#[test]
fn getex_exat_or_pxat_in_the_past_answers_the_value_and_deletes() {
    for opt in [["PXAT", "1"], ["EXAT", "1"]] {
        let mut db = with_key();
        let before = counted();
        assert_eq!(
            getex(&mut db, &[bulk("k"), bulk(opt[0]), bulk(opt[1])]),
            Frame::BulkString(Bytes::from_static(b"v"))
        );
        assert_gone_uncounted(&mut db, before, opt[0]);
    }
}

/// Byte-compared with redis 7.2.7 (`getExpireMillisecondsOrReply`).
#[test]
fn getex_expire_errors_match_redis() {
    let invalid = Frame::Error(Bytes::from_static(
        b"ERR invalid expire time in 'getex' command",
    ));
    let not_int = Frame::Error(Bytes::from_static(
        b"ERR value is not an integer or out of range",
    ));
    for (opt, n, want) in [
        ("EX", "-1", &invalid),
        ("EX", "0", &invalid),
        ("PX", "0", &invalid),
        ("PX", "-5", &invalid),
        ("EXAT", "0", &invalid),
        ("PXAT", "-5", &invalid),
        ("EX", "9223372036854775807", &invalid),
        ("EX", "abc", &not_int),
        ("EX", "1.5", &not_int),
    ] {
        let mut db = with_key();
        assert_eq!(
            &getex(&mut db, &[bulk("k"), bulk(opt), bulk(n)]),
            want,
            "GETEX k {opt} {n}"
        );
        assert!(db.exists(b"k"), "an error leaves the key");
    }
}

#[test]
fn restore_with_a_past_absttl_writes_nothing_and_replace_deletes() {
    let mut db = with_key();
    let Frame::BulkString(payload) = dump(&mut db, &[bulk("k")]) else {
        panic!("DUMP answered no payload");
    };
    let ok = Frame::SimpleString(Bytes::from_static(b"OK"));
    let before = counted();
    // REPLACE over the live key: deleted, not counted.
    assert_eq!(
        restore(
            &mut db,
            &[
                bulk("k"),
                bulk("1"),
                Frame::BulkString(payload.clone()),
                bulk("ABSTTL"),
                bulk("REPLACE"),
            ],
        ),
        ok
    );
    assert_gone_uncounted(&mut db, before, "RESTORE … ABSTTL REPLACE");
    // A fresh key: nothing is written, so nothing is reaped later.
    assert_eq!(
        restore(
            &mut db,
            &[
                bulk("k"),
                bulk("1"),
                Frame::BulkString(payload),
                bulk("ABSTTL")
            ],
        ),
        ok
    );
    assert_gone_uncounted(&mut db, before, "RESTORE … ABSTTL");
}

/// A key whose own TTL already passed does not exist for EXPIREAT / EXPIRE
/// with a past deadline: redis's `lookupKeyWrite` expires (and counts) it,
/// and the command answers 0. Here the lazy drain reaps and counts it once.
#[test]
fn a_past_deadline_on_an_already_expired_key_answers_0_and_counts_it_once() {
    for (what, args, pexpire) in [
        ("EXPIREAT k 1", vec![bulk("k"), bulk("1")], false),
        ("EXPIRE k -1", vec![bulk("k"), bulk("-1")], true),
    ] {
        let mut db = Database::new();
        db.set(
            b"k",
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), current_time_ms() - 5),
        );
        let before = counted();
        let reply = if pexpire {
            crate::command::key::expire(&mut db, &args)
        } else {
            expireat(&mut db, &args)
        };
        assert_eq!(reply, Frame::Integer(0), "{what}");
        crate::server::expiration::expire_cycle_direct(&mut db, &mut |_| {});
        assert!(!db.exists(b"k"), "{what}");
        assert_eq!(
            counted() - before,
            1,
            "{what}: reaped as an expired key, once"
        );
    }
}
