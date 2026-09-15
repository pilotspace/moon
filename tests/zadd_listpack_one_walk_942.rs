//! ZADD and ZINCRBY update a listpack zset in ONE walk — moon#942.
//!
//! A zset listpack is `[member, score, member, score, …]`, the same
//! field/value layout a hash listpack has, so the update is
//! `Listpack::update_pair_value`: one borrowed scan that keeps the byte
//! offsets it walked past and rewrites the score where it stopped. It used to
//! be a local `listpack_zset_find` returning a pair ORDINAL followed by
//! `replace_at`, which walked back to that ordinal from the head — the second
//! scan moon#799 removed from HSET and left standing here.
//!
//! The walk COUNT is asserted in `src/storage/listpack.rs`'s
//! `one_walk_update_tests`, which can see the private seek counter. This
//! suite asserts the thing that actually has to survive the rewrite: the
//! ANSWER. Every flag combination, both ends and the middle of the listpack,
//! a single-member zset, and the two arms that are easy to get wrong — the
//! `CH` tally, which reads the score that WAS there, and ZINCRBY's NaN
//! fall-through, which must leave the listpack untouched.
//!
//! Everything here stays inside the listpack encoding (the default
//! `zset-max-listpack-entries` is 128), which is the regime the rewrite
//! touches; the B+tree arm is unchanged and is covered elsewhere.
//!
//! Run:
//!   cargo test --release --test zadd_listpack_one_walk_942

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use moon::command::sorted_set;
use moon::protocol::Frame;
use moon::storage::db::Database;

fn argv(args: &[&[u8]]) -> Vec<Frame> {
    args.iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
        .collect()
}

fn zadd(db: &mut Database, args: &[&[u8]]) -> Frame {
    sorted_set::zadd(db, &argv(args))
}

fn zincrby(db: &mut Database, args: &[&[u8]]) -> Frame {
    sorted_set::zincrby(db, &argv(args))
}

fn zscore(db: &mut Database, key: &[u8], member: &[u8]) -> Option<String> {
    match sorted_set::zscore(db, &argv(&[key, member])) {
        Frame::BulkString(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        Frame::Null => None,
        other => panic!("ZSCORE answered {other:?}"),
    }
}

fn zcard(db: &mut Database, key: &[u8]) -> i64 {
    match sorted_set::zcard(db, &argv(&[key])) {
        Frame::Integer(n) => n,
        other => panic!("ZCARD answered {other:?}"),
    }
}

fn bulk(f: &Frame) -> String {
    match f {
        Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected a bulk string, got {other:?}"),
    }
}

/// Ten members, `m0`..`m9`, scored 0, 10, .. 90. Well inside the listpack
/// encoding, and long enough that the first and last members sit at genuinely
/// different offsets.
fn fixture(db: &mut Database, key: &[u8]) {
    for i in 0..10i64 {
        let member = format!("m{i}");
        let score = (i * 10).to_string();
        assert_eq!(
            zadd(db, &[key, score.as_bytes(), member.as_bytes()]),
            Frame::Integer(1),
            "fixture member {member} was not added"
        );
    }
    assert_eq!(zcard(db, key), 10);
}

/// Re-scoring an existing member updates it in place: the reply is 0 (nothing
/// ADDED), the cardinality does not move, and the new score is what ZSCORE
/// reads back. The first member, the last member and a middle one, because
/// the rewrite changed where in the buffer the write lands.
#[test]
fn rescoring_an_existing_member_updates_in_place() {
    let mut db = Database::new();
    let key: &[u8] = b"z";
    fixture(&mut db, key);

    for (member, new_score) in [
        (&b"m0"[..], "111"),
        (b"m5", "-2.5"),
        (b"m9", "999999999999"),
        (b"m0", "0"),
    ] {
        assert_eq!(
            zadd(&mut db, &[key, new_score.as_bytes(), member]),
            Frame::Integer(0),
            "re-scoring {} must not report an addition",
            String::from_utf8_lossy(member)
        );
        assert_eq!(
            zscore(&mut db, key, member).as_deref(),
            Some(new_score),
            "score of {} did not take",
            String::from_utf8_lossy(member)
        );
        assert_eq!(zcard(&mut db, key), 10, "cardinality moved on a re-score");
    }

    // Every OTHER member must be untouched by all of that.
    for i in [1i64, 2, 3, 4, 6, 7, 8] {
        assert_eq!(
            zscore(&mut db, key, format!("m{i}").as_bytes()).as_deref(),
            Some((i * 10).to_string().as_str()),
            "neighbour m{i} was disturbed"
        );
    }
}

/// A member that is not there is appended, and the reply counts it.
#[test]
fn a_new_member_is_added() {
    let mut db = Database::new();
    let key: &[u8] = b"z";
    fixture(&mut db, key);

    assert_eq!(zadd(&mut db, &[key, b"7", b"fresh"]), Frame::Integer(1));
    assert_eq!(zcard(&mut db, key), 11);
    assert_eq!(zscore(&mut db, key, b"fresh").as_deref(), Some("7"));

    // Two pairs in one call, one existing and one new.
    assert_eq!(
        zadd(&mut db, &[key, b"1", b"m0", b"2", b"alsofresh"]),
        Frame::Integer(1),
        "only the new member counts as added"
    );
    assert_eq!(zcard(&mut db, key), 12);
    assert_eq!(zscore(&mut db, key, b"m0").as_deref(), Some("1"));
    assert_eq!(zscore(&mut db, key, b"alsofresh").as_deref(), Some("2"));
}

/// A one-member zset is where the pair arithmetic is tightest: the member is
/// simultaneously the first and the last, and there is no neighbour for an
/// off-by-one to land in.
#[test]
fn a_single_member_zset_updates_and_grows() {
    let mut db = Database::new();
    let key: &[u8] = b"solo";

    assert_eq!(zadd(&mut db, &[key, b"1", b"only"]), Frame::Integer(1));
    assert_eq!(zadd(&mut db, &[key, b"2", b"only"]), Frame::Integer(0));
    assert_eq!(zscore(&mut db, key, b"only").as_deref(), Some("2"));
    assert_eq!(zcard(&mut db, key), 1);

    // A member whose name equals the stored SCORE text must not be mistaken
    // for one — the score sits at an odd position and is never a member.
    assert_eq!(zscore(&mut db, key, b"2"), None);
    assert_eq!(zadd(&mut db, &[key, b"3", b"2"]), Frame::Integer(1));
    assert_eq!(zcard(&mut db, key), 2);
    assert_eq!(zscore(&mut db, key, b"only").as_deref(), Some("2"));
    assert_eq!(zscore(&mut db, key, b"2").as_deref(), Some("3"));
}

/// The flags all decide from the score already stored, which is what the
/// single scan now reads out. `CH` counts CHANGED, not written, so it has to
/// compare against the old value.
#[test]
fn flags_decide_from_the_stored_score() {
    let mut db = Database::new();
    let key: &[u8] = b"z";
    fixture(&mut db, key);

    // NX never touches an existing member, and still adds a new one.
    assert_eq!(
        zadd(&mut db, &[key, b"NX", b"555", b"m3"]),
        Frame::Integer(0)
    );
    assert_eq!(zscore(&mut db, key, b"m3").as_deref(), Some("30"));
    assert_eq!(
        zadd(&mut db, &[key, b"NX", b"555", b"nxnew"]),
        Frame::Integer(1)
    );
    assert_eq!(zscore(&mut db, key, b"nxnew").as_deref(), Some("555"));

    // XX never adds, and does update an existing member.
    assert_eq!(
        zadd(&mut db, &[key, b"XX", b"1", b"xxnew"]),
        Frame::Integer(0)
    );
    assert_eq!(zscore(&mut db, key, b"xxnew"), None);
    assert_eq!(
        zadd(&mut db, &[key, b"XX", b"31", b"m3"]),
        Frame::Integer(0)
    );
    assert_eq!(zscore(&mut db, key, b"m3").as_deref(), Some("31"));

    // GT raises only, LT lowers only.
    assert_eq!(
        zadd(&mut db, &[key, b"GT", b"20", b"m3"]),
        Frame::Integer(0)
    );
    assert_eq!(
        zscore(&mut db, key, b"m3").as_deref(),
        Some("31"),
        "GT lowered"
    );
    assert_eq!(
        zadd(&mut db, &[key, b"GT", b"40", b"m3"]),
        Frame::Integer(0)
    );
    assert_eq!(zscore(&mut db, key, b"m3").as_deref(), Some("40"));
    assert_eq!(
        zadd(&mut db, &[key, b"LT", b"50", b"m3"]),
        Frame::Integer(0)
    );
    assert_eq!(
        zscore(&mut db, key, b"m3").as_deref(),
        Some("40"),
        "LT raised"
    );
    assert_eq!(zadd(&mut db, &[key, b"LT", b"5", b"m3"]), Frame::Integer(0));
    assert_eq!(zscore(&mut db, key, b"m3").as_deref(), Some("5"));

    // CH counts members whose score actually moved.
    assert_eq!(
        zadd(&mut db, &[key, b"CH", b"5", b"m3"]),
        Frame::Integer(0),
        "re-writing the same score changes nothing"
    );
    assert_eq!(zadd(&mut db, &[key, b"CH", b"6", b"m3"]), Frame::Integer(1));
    assert_eq!(
        zadd(&mut db, &[key, b"CH", b"7", b"m3", b"1", b"chnew"]),
        Frame::Integer(2),
        "CH counts an update and an addition together"
    );
    // A GT that declines must not be counted as changed.
    assert_eq!(
        zadd(&mut db, &[key, b"GT", b"CH", b"1", b"m3"]),
        Frame::Integer(0)
    );
    assert_eq!(zscore(&mut db, key, b"m3").as_deref(), Some("7"));
}

/// ZINCRBY reads the stored score, adds to it and writes the sum back in the
/// same scan, and replies with the bytes it stored.
#[test]
fn zincrby_increments_in_place() {
    let mut db = Database::new();
    let key: &[u8] = b"z";
    fixture(&mut db, key);

    // First member, last member, and a middle one.
    assert_eq!(bulk(&zincrby(&mut db, &[key, b"5", b"m0"])), "5");
    assert_eq!(bulk(&zincrby(&mut db, &[key, b"-2.5", b"m0"])), "2.5");
    assert_eq!(bulk(&zincrby(&mut db, &[key, b"1", b"m9"])), "91");
    assert_eq!(bulk(&zincrby(&mut db, &[key, b"0", b"m5"])), "50");
    assert_eq!(zcard(&mut db, key), 10, "no member was duplicated");
    assert_eq!(zscore(&mut db, key, b"m0").as_deref(), Some("2.5"));
    assert_eq!(zscore(&mut db, key, b"m9").as_deref(), Some("91"));

    // The reply and what a later ZSCORE reads must be the same text.
    let reply = bulk(&zincrby(&mut db, &[key, b"0.25", b"m4"]));
    assert_eq!(zscore(&mut db, key, b"m4").as_deref(), Some(reply.as_str()));

    // An absent member starts at 0.
    assert_eq!(bulk(&zincrby(&mut db, &[key, b"3.5", b"brandnew"])), "3.5");
    assert_eq!(zcard(&mut db, key), 11);
    assert_eq!(zscore(&mut db, key, b"brandnew").as_deref(), Some("3.5"));
}

/// `inf + -inf` is NaN, which must never be written into a listpack: a
/// listpack stores the score as TEXT and `NaN` does not parse back, so the
/// member would silently read as 0. The listpack arm declines and the B+tree
/// arm answers, which is a PRE-EXISTING divergence from redis (it errors).
/// Pinned here because the rewrite moved that decision into a closure, and a
/// closure that declined for the wrong reason would corrupt the score
/// instead.
#[test]
fn zincrby_to_nan_leaves_the_stored_score_alone() {
    let mut db = Database::new();
    let key: &[u8] = b"z";
    assert_eq!(zadd(&mut db, &[key, b"inf", b"m"]), Frame::Integer(1));
    assert_eq!(zadd(&mut db, &[key, b"1", b"other"]), Frame::Integer(1));
    assert_eq!(zscore(&mut db, key, b"m").as_deref(), Some("inf"));

    // Whatever moon answers here, it must not be a listpack holding `NaN`.
    let reply = bulk(&zincrby(&mut db, &[key, b"-inf", b"m"]));
    assert!(
        reply.eq_ignore_ascii_case("nan"),
        "expected moon's pre-existing NaN reply, got {reply:?}"
    );
    assert_eq!(zcard(&mut db, key), 2, "the member was duplicated");
    // The neighbour must have survived the fall-through to the B+tree arm.
    assert_eq!(zscore(&mut db, key, b"other").as_deref(), Some("1"));

    // A finite increment onto an infinite score is NOT NaN and stays in the
    // listpack.
    let mut db = Database::new();
    assert_eq!(zadd(&mut db, &[key, b"inf", b"m"]), Frame::Integer(1));
    assert_eq!(bulk(&zincrby(&mut db, &[key, b"1", b"m"])), "inf");
    assert_eq!(zscore(&mut db, key, b"m").as_deref(), Some("inf"));
}
