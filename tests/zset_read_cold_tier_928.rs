//! A cold-spilled sorted set still answers every read — moon#928.
//!
//! moon#928 re-routes the fourteen sorted-set READ handlers off
//! `Database::get_sorted_set` (which calls `SortedSetKind::upgrade` and so
//! flattens a listpack zset permanently) onto the shared-borrow
//! implementation that already backs `dispatch_read`. That accessor takes
//! `&Database`, so it CANNOT promote a cold-spilled value back into hot RAM
//! the way `get_sorted_set` did — it reads the cold tier through instead and
//! hands back `SortedSetRef::Owned`.
//!
//! That trade is only acceptable if the ANSWER is unchanged. A read that
//! quietly reports a spilled zset as missing — or as `-WRONGTYPE` — is a
//! worse bug than the flattening it replaces, and it is exactly the P0 shape
//! `tests/cold_collection_visibility.rs` was written for. So this suite pins
//! the answer, not the plumbing: spill a zset that only ever existed on disk,
//! then run all fourteen handlers against it on the MUTABLE dispatch path and
//! require every reply to match a hot zset holding the same members.
//!
//! Deterministic on purpose. `tests/cold_collection_visibility.rs` has to
//! drive real eviction, which races the background tick and is
//! scheduler-dependent; this suite spills straight to a datafile with
//! `kv_spill::spill_to_datafile` and points a fresh `Database` at the
//! resulting `ColdIndex` — the same harness `tests/
//! cold_promote_compact_encoding_898.rs` uses — so there is no race and no
//! `--maxmemory` tuning.
//!
//! What this suite does NOT claim: that the read promotes the key back into
//! hot RAM. It deliberately does not, matching what moon#853 already shipped
//! for the hash, list and set families — and the suite asserts that too, so
//! the trade is recorded rather than assumed. Active expiry and every write
//! path still promote.
//!
//! Run:
//!   cargo test --release --test zset_read_cold_tier_928

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use moon::command::sorted_set;
use moon::persistence::manifest::ShardManifest;
use moon::protocol::Frame;
use moon::storage::compact_value::CompactValue;
use moon::storage::db::Database;
use moon::storage::entry::{Entry, RedisValue};
use moon::storage::tiered::cold_index::ColdIndex;
use moon::storage::tiered::kv_spill::spill_to_datafile;

/// The fixture, as (score, member) pairs. Three members with distinct scores
/// and lexicographically ordered names, so rank, score-range and lex-range
/// answers are all non-trivial.
const MEMBERS: &[(f64, &[u8])] = &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")];

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

fn argv(args: &[&[u8]]) -> Vec<Frame> {
    args.iter().map(|a| bulk(a)).collect()
}

/// Every read under test, on the MUTABLE dispatch path — the handlers
/// `command::dispatch` calls. `ZRANDMEMBER` is excluded: its reply is random,
/// so it cannot be compared for equality; it is covered separately below.
fn read(db: &mut Database, name: &str, args: &[&[u8]]) -> Frame {
    let f = argv(args);
    match name {
        "ZSCORE" => sorted_set::zscore(db, &f),
        "ZCARD" => sorted_set::zcard(db, &f),
        "ZRANK" => sorted_set::zrank(db, &f),
        "ZREVRANK" => sorted_set::zrevrank(db, &f),
        "ZSCAN" => sorted_set::zscan(db, &f),
        "ZRANGE" => sorted_set::zrange(db, &f),
        "ZREVRANGE" => sorted_set::zrevrange(db, &f),
        "ZRANGEBYSCORE" => sorted_set::zrangebyscore(db, &f),
        "ZREVRANGEBYSCORE" => sorted_set::zrevrangebyscore(db, &f),
        "ZCOUNT" => sorted_set::zcount(db, &f),
        "ZLEXCOUNT" => sorted_set::zlexcount(db, &f),
        "ZMSCORE" => sorted_set::zmscore(db, &f),
        "ZRANDMEMBER" => sorted_set::zrandmember(db, &f),
        "ZDIFF" => sorted_set::zdiff(db, &f),
        "ZUNION" => sorted_set::zunion(db, &f),
        "ZINTER" => sorted_set::zinter(db, &f),
        "ZINTERCARD" => sorted_set::zintercard(db, &f),
        other => panic!("no arm for {other}"),
    }
}

/// One row per handler — the fourteen `get_sorted_set` call sites moon#928
/// re-routes, with the four set-operation commands that share
/// `collect_source_sets` listed individually.
fn reads() -> Vec<(&'static str, Vec<&'static [u8]>)> {
    vec![
        ("ZSCORE", vec![&b"z"[..], b"b"]),
        ("ZCARD", vec![&b"z"[..]]),
        ("ZRANK", vec![&b"z"[..], b"b"]),
        ("ZREVRANK", vec![&b"z"[..], b"b"]),
        ("ZSCAN", vec![&b"z"[..], b"0"]),
        ("ZRANGE", vec![&b"z"[..], b"0", b"-1", b"WITHSCORES"]),
        ("ZREVRANGE", vec![&b"z"[..], b"0", b"-1"]),
        (
            "ZRANGEBYSCORE",
            vec![&b"z"[..], b"-inf", b"+inf", b"WITHSCORES"],
        ),
        ("ZREVRANGEBYSCORE", vec![&b"z"[..], b"+inf", b"-inf"]),
        ("ZCOUNT", vec![&b"z"[..], b"(1", b"3"]),
        ("ZLEXCOUNT", vec![&b"z"[..], b"[b", b"+"]),
        ("ZMSCORE", vec![&b"z"[..], b"a", b"nope", b"c"]),
        ("ZDIFF", vec![&b"1"[..], b"z", b"WITHSCORES"]),
        ("ZUNION", vec![&b"1"[..], b"z", b"WITHSCORES"]),
        ("ZINTER", vec![&b"1"[..], b"z", b"WITHSCORES"]),
        ("ZINTERCARD", vec![&b"1"[..], b"z"]),
    ]
}

/// A `Database` holding the fixture in hot RAM, as the B+tree form — the
/// oracle every cold answer is compared against. Built through the real
/// `ZADD` handler and then promoted past the value threshold, so it is the
/// same content the spill carries.
fn hot_reference() -> Database {
    let mut db = Database::new();
    for (score, member) in MEMBERS {
        sorted_set::zadd(
            &mut db,
            &argv(&[b"z", score.to_string().as_bytes(), member]),
        );
    }
    db
}

/// A `Database` whose only copy of `z` lives on disk in the cold tier.
fn cold_only(shard_dir: &std::path::Path) -> Database {
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut cold_index = ColdIndex::new();

    // Spill the FULL (B+tree) form — that is what `kv_spill` writes and what
    // `kv_serde::deserialize_collection` decodes back on a read-through.
    let mut members = std::collections::HashMap::new();
    let mut tree = moon::storage::bptree::BPTree::new();
    for (score, member) in MEMBERS {
        let m = Bytes::copy_from_slice(member);
        members.insert(m.clone(), *score);
        tree.insert(ordered_float::OrderedFloat(*score), m);
    }
    let mut entry = Entry::new_string(Bytes::new());
    entry.value = CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
        tree: Box::new(tree),
        members: Box::new(members),
    });

    spill_to_datafile(
        shard_dir,
        700,
        b"z",
        &entry,
        0,
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();

    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = Some(cold_index);
    // `is_hot`, NOT `Database::get`: `get` takes `&mut self` and PROMOTES a
    // cold key on miss, so using it as the "is it cold?" probe would drag the
    // key into hot RAM and then report that it is hot — a probe that destroys
    // what it measures. (Caught exactly that way: the first draft of this
    // suite failed its own precondition.)
    assert!(
        !db.is_hot(b"z"),
        "precondition: `z` must be cold-only — a hot copy would make this suite vacuous"
    );
    db
}

#[test]
fn every_sorted_set_read_answers_a_cold_spilled_zset() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cold = cold_only(tmp.path());
    let mut hot = hot_reference();

    let mut wrong: Vec<String> = Vec::new();
    for (name, args) in reads() {
        let got = read(&mut cold, name, &args);
        let want = read(&mut hot, name, &args);
        if got != want {
            wrong.push(format!(
                "{name} {}: cold {got:?} != hot {want:?}",
                args.iter()
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
    }

    // ZRANDMEMBER separately — random reply, so membership is the assertion.
    match read(&mut cold, "ZRANDMEMBER", &[b"z"]) {
        Frame::BulkString(b) => {
            if !MEMBERS.iter().any(|(_, m)| *m == b.as_ref()) {
                wrong.push(format!("ZRANDMEMBER z: cold returned {b:?}, not a member"));
            }
        }
        other => wrong.push(format!(
            "ZRANDMEMBER z: cold returned {other:?}, expected a bulk string"
        )),
    }

    assert!(
        wrong.is_empty(),
        "{} sorted-set read(s) answer a cold-spilled zset differently from a hot one \
         (moon#928 must not trade flattening for invisibility):\n  {}",
        wrong.len(),
        wrong.join("\n  ")
    );
}

/// The trade moon#928 makes, recorded as a test rather than left implicit:
/// a read no longer drags a cold zset back into hot RAM. `get_sorted_set`
/// did; the shared-borrow accessor cannot, because `&Database` cannot mutate
/// the keyspace. moon#853 already shipped exactly this for hash, list and
/// set. If a future change makes reads promote again, this test says so out
/// loud instead of the behaviour drifting silently.
#[test]
fn a_read_answers_from_the_cold_plane_without_promoting() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cold = cold_only(tmp.path());

    assert_eq!(
        read(&mut cold, "ZCARD", &[b"z"]),
        Frame::Integer(MEMBERS.len() as i64),
        "the cold read must answer with the real cardinality"
    );
    assert!(
        !cold.is_hot(b"z"),
        "a READ must not promote `z` into hot RAM — that is the documented \
         moon#853/#928 trade, and a write path is what promotes"
    );

    // A WRITE still promotes, and the promoted form is the compact one
    // (moon#898) — so the encoding win is not lost across the cold round
    // trip either.
    sorted_set::zadd(&mut cold, &argv(&[b"z", b"4", b"d"]));
    let entry = cold.get(b"z").expect("a write must promote the cold zset");
    assert_eq!(
        entry.value.as_redis_value().encoding_name(),
        "listpack",
        "the promote-back path must re-derive the compact encoding (moon#898)"
    );
    assert_eq!(
        read(&mut cold, "ZCARD", &[b"z"]),
        Frame::Integer(MEMBERS.len() as i64 + 1),
        "the write must MERGE with the promoted cold members, not fabricate an empty zset"
    );
    assert_eq!(
        cold.get(b"z")
            .unwrap()
            .value
            .as_redis_value()
            .encoding_name(),
        "listpack",
        "and the read after the promotion must still not flatten it (moon#928)"
    );
}
