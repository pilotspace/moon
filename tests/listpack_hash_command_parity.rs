//! moon#799 commit 2: HSET / HGET / HDEL / HGETDEL moved onto one-scan pair
//! operations. This drives them through the real command functions and
//! compares against a `HashMap` oracle.
//!
//! **The encoding is asserted at every checkpoint, and that is the point.**
//! A hash promotes to a `HashMap` above the hash entry limit (128) fields,
//! and nothing ever downgrades. A "large hash" parity test therefore proves
//! nothing about the listpack path unless it also proves the key was STILL
//! listpack-encoded when the assertion ran -- otherwise the oracle is being
//! compared against the `HashMap` implementation, which is not what changed.
//! So this test works at exactly 128 fields, the widest listpack the command
//! layer can produce, and refuses to pass if the fixture has promoted.
//!
//! The bulk-size (`#866`-class) truncation guard for these operations lives
//! in `tests/listpack_entry_arms.rs`, at the `Listpack` level, at 2,048 and
//! 4,096 pairs -- because that is the ONLY place listpack code runs at that
//! size. Through the command layer it cannot: the encoding flips first.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use bytes::Bytes;

use moon::command::hash::{hdel, hget, hgetdel, hset};
use moon::protocol::Frame;
use moon::storage::compact_value::RedisValueRef;
use moon::storage::db::Database;
use moon::storage::encoding_limits::EncodingLimits;

fn f(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

/// Panics unless `key` is still a `HashListpack`. Guards against the whole
/// test silently migrating to the promoted `HashMap` path.
fn assert_still_listpack(db: &mut Database, key: &[u8], at: &str) {
    let entry = db.get(key).expect("key present");
    match entry.value.as_redis_value() {
        RedisValueRef::HashListpack(_) => {}
        other => panic!(
            "fixture is no longer listpack-encoded at {at} -- got {:?}. \
             The rest of this test would have been comparing the HashMap \
             implementation against itself.",
            std::mem::discriminant(&other)
        ),
    }
}

fn field(i: usize) -> Vec<u8> {
    format!("field:{i:04}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    format!("value-payload-{i:04}").into_bytes()
}

#[test]
fn listpack_hash_commands_match_a_hashmap_oracle_at_the_widest_listpack() {
    // Exactly the promotion threshold: `hset` upgrades when
    // the authority's hash entry limit, so 128 fields stay listpack.
    let n = EncodingLimits::moon_defaults().hash_entries;
    let mut db = Database::new();
    let mut oracle: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let key = b"h";

    // --- populate ---
    for i in 0..n {
        let r = hset(&mut db, &[f(key), f(&field(i)), f(&value(i))]);
        assert_eq!(r, Frame::Integer(1), "HSET {i} should report a new field");
        oracle.insert(field(i), value(i));
    }
    assert_still_listpack(&mut db, key, "after populate");

    // --- overwrite EVERY field: the moon#799 write path, one scan now ---
    for i in 0..n {
        let nv = format!("rewritten-{i:04}").into_bytes();
        let r = hset(&mut db, &[f(key), f(&field(i)), f(&nv)]);
        assert_eq!(
            r,
            Frame::Integer(0),
            "HSET {i} overwrote an existing field, so it must report 0 added"
        );
        oracle.insert(field(i), nv);
    }
    assert_still_listpack(&mut db, key, "after overwrite");

    // --- read every field back ---
    for i in 0..n {
        let got = hget(&mut db, &[f(key), f(&field(i))]);
        assert_eq!(
            got,
            Frame::BulkString(Bytes::from(oracle[&field(i)].clone())),
            "HGET disagreed at field {i}"
        );
    }
    assert_still_listpack(&mut db, key, "after reads");
    assert_eq!(
        hget(&mut db, &[f(key), f(b"absent")]),
        Frame::Null,
        "HGET of an absent field"
    );

    // --- HGETDEL every third field: returns the value AND removes the pair ---
    for i in (0..n).step_by(3) {
        let got = hgetdel(&mut db, &[f(key), f(b"FIELDS"), f(b"1"), f(&field(i))]);
        assert_eq!(
            got,
            Frame::Array(vec![Frame::BulkString(Bytes::from(oracle[&field(i)].clone()))].into()),
            "HGETDEL disagreed at field {i}"
        );
        oracle.remove(&field(i));
    }
    assert_still_listpack(&mut db, key, "after HGETDEL");

    // --- HDEL every remaining even field ---
    for i in (0..n).step_by(2) {
        let present = oracle.contains_key(&field(i));
        let got = hdel(&mut db, &[f(key), f(&field(i))]);
        assert_eq!(
            got,
            Frame::Integer(if present { 1 } else { 0 }),
            "HDEL disagreed at field {i}"
        );
        oracle.remove(&field(i));
    }
    assert_still_listpack(&mut db, key, "after HDEL");

    // --- the survivors must be exactly the oracle's, byte for byte ---
    for i in 0..n {
        let got = hget(&mut db, &[f(key), f(&field(i))]);
        match oracle.get(&field(i)) {
            Some(v) => assert_eq!(
                got,
                Frame::BulkString(Bytes::from(v.clone())),
                "field {i} should have survived"
            ),
            None => assert_eq!(got, Frame::Null, "field {i} should be gone"),
        }
    }

    // And nothing extra survived: the stored length must match the oracle's.
    let entry = db.get(key).expect("key present");
    match entry.value.as_redis_value() {
        RedisValueRef::HashListpack(lp) => {
            assert_eq!(
                lp.len() / 2,
                oracle.len(),
                "the listpack holds a different number of pairs than the oracle"
            );
        }
        _ => panic!("fixture promoted"),
    }
}
