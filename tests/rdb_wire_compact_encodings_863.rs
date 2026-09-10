//! moon#863 — the Redis-wire RDB codec must preserve compact encodings.
//!
//! `persistence::redis_rdb` is the codec a FULLRESYNC payload travels through
//! (`replication::apply::load_snapshot` feeds the snapshot straight to
//! `redis_rdb::load_rdb`). Until this suite went green it rebuilt every
//! container in its FULL form, because the Redis wire format has no type tag
//! for a compact encoding and the loader had no re-derivation step:
//!
//! | written as          | RDB tag             | loaded as (before) |
//! |---------------------|---------------------|--------------------|
//! | `SetIntset`         | `RDB_TYPE_SET` (2)  | `hashtable`        |
//! | `SetListpack`       | `RDB_TYPE_SET` (2)  | `hashtable`        |
//! | `HashListpack`      | `RDB_TYPE_HASH` (4) | `hashtable`        |
//! | `ListListpack`      | `RDB_TYPE_LIST` (1) | `linkedlist`       |
//! | `SortedSetListpack` | `RDB_TYPE_ZSET_2`   | `skiplist`         |
//!
//! moon's OWN RDB (`persistence::rdb`) has re-derived them since moon#840;
//! only this codec — the one replication uses — flattened.
//!
//! The above-threshold half of the suite is the moon#866 guard: a container
//! past the listpack limits must still load in its FULL form with EVERY
//! element intact. A compaction step that silently truncated would otherwise
//! look identical to a passing encoding test.

use bytes::Bytes;
use moon::persistence::redis_rdb::{load_rdb, write_rdb};
use moon::storage::compact_value::RedisValueRef;
use moon::storage::db::Database;
use moon::storage::encoding_limits::EncodingLimits;

/// moon#907 retired the free-standing threshold constants into the single
/// `EncodingLimits` authority. This test asserts boundaries, so it reads them
/// from the authority rather than restating numbers that can drift apart.
const LIMITS: EncodingLimits = EncodingLimits::moon_defaults();
use moon::storage::entry::{Entry, RedisValue, SetValue};
use moon::storage::listpack::Listpack;
use std::collections::{HashMap, VecDeque};

/// Serialize `db` with the redis-wire writer and read it back with the
/// redis-wire loader — exactly the hop a replica's FULLRESYNC payload makes.
fn wire_round_trip(db: Database) -> Database {
    let dbs = vec![db];
    let mut buf = Vec::new();
    write_rdb(&dbs, &mut buf);
    let mut loaded = vec![Database::new()];
    load_rdb(&mut loaded, &buf).expect("redis-wire RDB must load");
    loaded.into_iter().next().expect("one db")
}

fn encoding_of(db: &Database, key: &[u8]) -> String {
    let entry = db.data().get(key).unwrap_or_else(|| {
        panic!(
            "key {} missing after round trip",
            String::from_utf8_lossy(key)
        )
    });
    entry.as_redis_value().encoding_name().to_string()
}

fn listpack_of(items: &[&[u8]]) -> Listpack {
    let mut lp = Listpack::new();
    for i in items {
        lp.push_back(i);
    }
    lp
}

fn full_set(members: impl IntoIterator<Item = Bytes>) -> Entry {
    let mut set = SetValue::new();
    for m in members {
        set.insert(m);
    }
    let mut e = Entry::new_set();
    if let Some(rv) = e.redis_value_mut() {
        *rv = RedisValue::Set(Box::new(set));
    }
    e
}

// ---------------------------------------------------------------------------
// Below the thresholds: every compact encoding must survive the wire hop.
// ---------------------------------------------------------------------------

#[test]
fn compact_encodings_survive_the_redis_wire_round_trip() {
    let mut db = Database::new();

    let mut e = Entry::new_set_intset();
    if let Some(RedisValue::SetIntset(is)) = e.redis_value_mut() {
        for v in [1i64, 2, 3] {
            is.insert(v);
        }
    }
    db.set(b"si", e);

    let mut e = Entry::new_set_listpack();
    if let Some(RedisValue::SetListpack(lp)) = e.redis_value_mut() {
        *lp = listpack_of(&[b"x", b"y", b"z"]);
    }
    db.set(b"ss", e);

    let mut e = Entry::new_hash_listpack();
    if let Some(RedisValue::HashListpack(lp)) = e.redis_value_mut() {
        *lp = listpack_of(&[b"f1", b"v1", b"f2", b"v2"]);
    }
    db.set(b"h", e);

    let mut e = Entry::new_list_listpack();
    if let Some(RedisValue::ListListpack(lp)) = e.redis_value_mut() {
        *lp = listpack_of(&[b"a", b"b", b"c"]);
    }
    db.set(b"l", e);

    let mut e = Entry::new_sorted_set_listpack();
    if let Some(RedisValue::SortedSetListpack(lp)) = e.redis_value_mut() {
        let mut rendered = moon::storage::zset_score::ScoreBuf::new();
        for (member, score) in [(&b"alice"[..], 1.5f64), (&b"bob"[..], 2.5)] {
            lp.push_back(member);
            moon::storage::zset_score::render_score(score, &mut rendered);
            lp.push_back(&rendered);
        }
    }
    db.set(b"z", e);

    // What the master holds, before the hop.
    let keys: [&[u8]; 5] = [b"si", b"ss", b"h", b"l", b"z"];
    let before: Vec<String> = keys.iter().map(|k| encoding_of(&db, k)).collect();
    assert_eq!(
        before,
        vec!["intset", "listpack", "listpack", "listpack", "listpack"],
        "test fixture is wrong: the master side is not in the compact form"
    );

    let loaded = wire_round_trip(db);
    // Report the WHOLE table in one shot: failing on the first row would hide
    // which of the five types still reproduce and which a prior fix covered.
    let mismatches: Vec<String> = keys
        .iter()
        .zip(&before)
        .filter_map(|(key, want)| {
            let got = encoding_of(&loaded, key);
            (&got != want).then(|| {
                format!(
                    "{}: master={want} replica={got}",
                    String::from_utf8_lossy(key)
                )
            })
        })
        .collect();
    assert!(
        mismatches.is_empty(),
        "moon#863: compact encodings lost across the redis-wire RDB hop \
         (a replica would hold the master's data in the expensive form): {mismatches:?}"
    );

    // Encoding parity is worthless without content parity.
    match loaded
        .data()
        .get(b"si".as_ref())
        .expect("si")
        .as_redis_value()
    {
        RedisValueRef::SetIntset(is) => {
            assert_eq!(is.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
        }
        other => panic!("si: {}", other.encoding_name()),
    }
    match loaded
        .data()
        .get(b"z".as_ref())
        .expect("z")
        .as_redis_value()
    {
        RedisValueRef::SortedSetListpack(lp) => {
            let pairs: Vec<(String, String)> = lp
                .iter_pairs()
                .map(|(m, s)| {
                    (
                        String::from_utf8_lossy(&m.as_bytes()).into_owned(),
                        String::from_utf8_lossy(&s.as_bytes()).into_owned(),
                    )
                })
                .collect();
            assert_eq!(
                pairs,
                vec![
                    ("alice".to_string(), "1.5".to_string()),
                    ("bob".to_string(), "2.5".to_string())
                ],
                "zset listpack must round trip in score order with its scores intact"
            );
        }
        other => panic!("z: {}", other.encoding_name()),
    }
}

// ---------------------------------------------------------------------------
// Score fidelity: compacting a zset stores its scores as TEXT in the listpack
// (`zset_score::render_score`), so an exact f64 now makes an extra hop —
// f64 -> text -> f64 — that it did not make before. Scores that are hard to
// render are the ones that would break silently.
// ---------------------------------------------------------------------------

#[test]
fn zset_scores_survive_the_compacting_round_trip_exactly() {
    // In ASCENDING SCORE order — `MIN_POSITIVE` is a positive number, so it
    // sorts above `0.0`, not below `-1.0`.
    let cases: [(&[u8], f64); 8] = [
        (b"neg_inf", f64::NEG_INFINITY),
        (b"neg_one", -1.0),
        (b"zero", 0.0),
        (b"tiny", f64::MIN_POSITIVE),
        (b"epsilon", f64::EPSILON),
        (b"awkward", 0.1 + 0.2),
        (b"huge", 1e300),
        (b"pos_inf", f64::INFINITY),
    ];

    let mut db = Database::new();
    let mut e = Entry::new_sorted_set_listpack();
    if let Some(RedisValue::SortedSetListpack(lp)) = e.redis_value_mut() {
        let mut rendered = moon::storage::zset_score::ScoreBuf::new();
        // In score order, which is the layout the listpack encoding keeps.
        for (member, score) in cases {
            lp.push_back(member);
            moon::storage::zset_score::render_score(score, &mut rendered);
            lp.push_back(&rendered);
        }
    }
    db.set(b"z", e);

    let loaded = wire_round_trip(db);
    assert_eq!(encoding_of(&loaded, b"z"), "listpack");
    match loaded
        .data()
        .get(b"z".as_ref())
        .expect("z")
        .as_redis_value()
    {
        RedisValueRef::SortedSetListpack(lp) => {
            let got: Vec<(String, f64)> = lp
                .iter_pairs()
                .map(|(m, s)| {
                    (
                        String::from_utf8_lossy(&m.as_bytes()).into_owned(),
                        moon::storage::zset_score::parse_score(&s.as_bytes())
                            .expect("every stored score must parse"),
                    )
                })
                .collect();
            let want: Vec<(String, f64)> = cases
                .iter()
                .map(|(m, s)| (String::from_utf8_lossy(m).into_owned(), *s))
                .collect();
            assert_eq!(
                got, want,
                "a zset score changed value across the redis-wire RDB hop"
            );
        }
        other => panic!("z: {}", other.encoding_name()),
    }
}

// The NaN-score rejection lives with the codec, in
// `src/persistence/redis_rdb.rs`'s own test module: it needs `read_rdb_entry`
// directly, because a hand-built RDB file would be rejected by the CRC check
// before the score is ever read — an integration-level version of it passed
// with the guard REMOVED, which is a guard that proves nothing.

// ---------------------------------------------------------------------------
// Above the thresholds: full form, and — the moon#866 guard — nothing lost.
// ---------------------------------------------------------------------------

#[test]
fn above_threshold_containers_stay_full_and_keep_every_element() {
    // One element past the listpack entry limit. The hash, set and list
    // limits are the same number today; the assertion below makes that
    // explicit, so a later per-type configuration wave fails here loudly
    // instead of silently testing the wrong boundary for two of the three.
    assert_eq!(
        (LIMITS.set_entries, LIMITS.hash_entries, LIMITS.list_entries),
        (LIMITS.set_entries, LIMITS.set_entries, LIMITS.set_entries),
        "this test uses one N for set, hash and list; their entry limits have diverged",
    );
    const N: usize = LIMITS.set_entries + 1; // 129
    let mut db = Database::new();

    // Non-integer set past the listpack entry limit.
    db.set(
        b"bigset",
        full_set((0..N).map(|i| Bytes::from(format!("m{i}")))),
    );

    // All-integer set past the intset entry limit: too big for an intset AND too
    // big for a listpack, so it must stay a hashtable.
    db.set(
        b"bigints",
        full_set((0..=LIMITS.set_intset).map(|i| Bytes::from(i.to_string()))),
    );

    // A single OVER-SIZED element is the other way past the threshold.
    db.set(
        b"fatmember",
        full_set([
            Bytes::from_static(b"small"),
            Bytes::from(vec![b'x'; LIMITS.set_value + 1]),
            Bytes::from_static(b"other"),
        ]),
    );

    let mut map = HashMap::new();
    for i in 0..N {
        map.insert(Bytes::from(format!("f{i}")), Bytes::from(format!("v{i}")));
    }
    let mut e = Entry::new_hash();
    if let Some(rv) = e.redis_value_mut() {
        *rv = RedisValue::Hash(Box::new(map));
    }
    db.set(b"bighash", e);

    let mut deque = VecDeque::new();
    for i in 0..N {
        deque.push_back(Bytes::from(format!("e{i}")));
    }
    let mut e = Entry::new_list();
    if let Some(rv) = e.redis_value_mut() {
        *rv = RedisValue::List(deque);
    }
    db.set(b"biglist", e);

    // `bigset`/`bighash`/`biglist` sit ONE element past the threshold, so a
    // truncating loader would also flip their encoding and the encoding
    // assertions alone would catch it. `hugelist` sits far past it on purpose:
    // dropping elements there changes NOTHING about the encoding, so only the
    // element count below can fail — which is what makes that assertion a real
    // moon#866 guard rather than a restatement of the encoding one.
    const HUGE: usize = 5000;
    let mut deque = VecDeque::new();
    for i in 0..HUGE {
        deque.push_back(Bytes::from(format!("h{i}")));
    }
    let mut e = Entry::new_list();
    if let Some(rv) = e.redis_value_mut() {
        *rv = RedisValue::List(deque);
    }
    db.set(b"hugelist", e);

    db.set(
        b"hugeset",
        full_set((0..HUGE).map(|i| Bytes::from(format!("hm{i}")))),
    );

    let loaded = wire_round_trip(db);

    for key in [
        &b"bigset"[..],
        b"bigints",
        b"bighash",
        b"fatmember",
        b"hugeset",
    ] {
        assert_eq!(
            encoding_of(&loaded, key),
            "hashtable",
            "{} must stay in the full form past the listpack thresholds",
            String::from_utf8_lossy(key)
        );
    }
    for key in [&b"biglist"[..], b"hugelist"] {
        assert_eq!(
            encoding_of(&loaded, key),
            "linkedlist",
            "{} must stay in the full form past the listpack thresholds",
            String::from_utf8_lossy(key)
        );
    }

    // moon#866 guard: element counts, not just encodings.
    let counts: [(&[u8], usize); 7] = [
        (b"bigset", N),
        (b"bigints", LIMITS.set_intset + 1),
        (b"bighash", N),
        (b"biglist", N),
        (b"fatmember", 3),
        (b"hugelist", HUGE),
        (b"hugeset", HUGE),
    ];
    for (key, want) in counts {
        let entry = loaded.data().get(key).expect("key present");
        let got = match entry.as_redis_value() {
            RedisValueRef::Set(s) => s.len(),
            RedisValueRef::Hash(m) => m.len(),
            RedisValueRef::List(d) => d.len(),
            other => panic!(
                "{}: unexpected {}",
                String::from_utf8_lossy(key),
                other.encoding_name()
            ),
        };
        assert_eq!(
            got,
            want,
            "{} lost elements across the redis-wire RDB hop",
            String::from_utf8_lossy(key)
        );
    }
}
