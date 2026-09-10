//! moon#898: a value promoted back from the cold tier must return in the
//! encoding it left in.
//!
//! The spill wire format (`storage::value_codec`) is canonical per LOGICAL
//! type: `Set | SetListpack | SetIntset` all encode as `ValueType::Set`, and
//! the body holds elements, never an encoding tag. Decode therefore has
//! nothing to rebuild the compact form from and always produced the FULL one.
//! moon#840 hooked `compact_after_decode` into the restart path and moon#863
//! into the replication path; the cold tier — the path enabled BY DEFAULT
//! (`--disk-offload enable`) — was never swept. Because nothing demotes
//! (moon#832), a container that comes back full stays full for the key's
//! lifetime, and the keys most likely to travel this path are exactly the
//! long-lived, rarely-touched small collections the compact encodings exist
//! for.
//!
//! # Why the boundary is here and not in the codec
//!
//! `kv_serde::deserialize_collection` is shared by the promoting path AND the
//! NON-promoting read-through (`Database::cold_read_only` ->
//! `ValueKind::classify_cold`), and `classify_cold` accepts only the canonical
//! full forms — a cold-decoded `SetListpack` falls through its `_ =>
//! Err(WrongType)` arm and answers WRONGTYPE for a perfectly valid set. So
//! the re-derivation belongs at the two places a cold value actually ENTERS
//! the hot keyspace, both of which this suite drives:
//!
//!   * `Database::promote_cold_outcome` — the disk cold plane (reached here
//!     through `promote_cold_if_present`, which every mutable accessor and
//!     `Database::get` call).
//!   * `eviction::rehydrate_spill_payload` — the in-flight spill plane
//!     (`promote_inflight_if_present`, and the spill-pwrite-failure re-insert
//!     in `shard::persistence_tick`).
//!
//! # What the assertions are
//!
//! The encoding, not just the value. A value-only assertion passes on the
//! unfixed binary, which is exactly why this was invisible for so long — the
//! four pre-existing `kv_serde` round-trip tests all assert the FULL form and
//! were green throughout.
//!
//! Two above-threshold controls, per the moon#866 lesson (a bounded-batch bug
//! once LOST 65,536 elements):
//!
//!   * `hash_129` sits ONE field past `LISTPACK_MAX_ENTRIES`. Truncating it
//!     by one element flips its encoding to `listpack`, so the ENCODING
//!     assertion fires and the count assertion never gets the chance —
//!     which is precisely why it is not enough on its own.
//!   * `hash_5000` sits far past the limit, where dropping elements changes
//!     no encoding at all. Only its element-count assertion can fail, which
//!     is what makes it a real truncation guard.
//!
//! Run alone:
//!   cargo test --profile release-fast --test cold_promote_compact_encoding_898

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use std::collections::VecDeque;

use moon::persistence::manifest::ShardManifest;
use moon::storage::compact_value::CompactValue;
use moon::storage::db::Database;
use moon::storage::entry::{Entry, RedisValue};
use moon::storage::intset::Intset;
use moon::storage::listpack::Listpack;
use moon::storage::tiered::cold_index::ColdIndex;
use moon::storage::tiered::kv_spill::spill_to_datafile;

// ── fixtures ────────────────────────────────────────────────────────────

fn entry_of(value: RedisValue) -> Entry {
    let mut e = Entry::new_string(Bytes::new());
    e.value = CompactValue::from_redis_value(value);
    e
}

fn set_intset(members: &[i64]) -> RedisValue {
    let mut is = Intset::new();
    for m in members {
        is.insert(*m);
    }
    RedisValue::SetIntset(is)
}

fn listpack_of(items: &[&[u8]]) -> Listpack {
    let mut lp = Listpack::new();
    for it in items {
        lp.push_back(it);
    }
    lp
}

/// A hash in its FULL form with `n` fields — the above-threshold control.
fn full_hash(n: usize) -> RedisValue {
    let mut map = std::collections::HashMap::with_capacity(n);
    for i in 0..n {
        map.insert(
            Bytes::from(format!("f{i:05}")),
            Bytes::from(format!("v{i:05}")),
        );
    }
    RedisValue::Hash(Box::new(map))
}

/// Every case this suite pins: key, the value as it sits in HOT RAM before
/// the spill, and the encoding it must come back in.
fn cases() -> Vec<(&'static str, RedisValue, &'static str)> {
    vec![
        ("si", set_intset(&[1, 2, 3]), "intset"),
        (
            "ss",
            RedisValue::SetListpack(listpack_of(&[b"alpha", b"beta", b"gamma"])),
            "listpack",
        ),
        (
            "h",
            RedisValue::HashListpack(listpack_of(&[b"f1", b"v1", b"f2", b"v2"])),
            "listpack",
        ),
        (
            "l",
            RedisValue::ListListpack(listpack_of(&[b"a", b"b", b"c"])),
            "listpack",
        ),
        (
            "z",
            RedisValue::SortedSetListpack(listpack_of(&[b"a", b"1", b"b", b"2.5"])),
            "listpack",
        ),
        // Above-threshold controls — see the module doc for why there are two.
        ("hash_129", full_hash(129), "hashtable"),
        ("hash_5000", full_hash(5000), "hashtable"),
    ]
}

/// Elements a case must still hold after the round trip, so a truncation is
/// caught independently of the encoding.
fn element_count(v: &RedisValue) -> usize {
    match v {
        RedisValue::Hash(m) => m.len(),
        RedisValue::HashListpack(lp) => lp.len() / 2,
        RedisValue::List(l) => l.len(),
        RedisValue::ListListpack(lp) => lp.len(),
        RedisValue::Set(s) => s.len(),
        RedisValue::SetListpack(lp) => lp.len(),
        RedisValue::SetIntset(is) => is.len(),
        RedisValue::SortedSetBPTree { members, .. } => members.len(),
        RedisValue::SortedSetListpack(lp) => lp.len() / 2,
        other => panic!("unexpected value in a fixture: {}", other.type_name()),
    }
}

/// A member-set fingerprint that is identical across a type's encodings, so
/// the CONTENT assertion is independent of the encoding assertion.
fn fingerprint(v: &RedisValue) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = match v {
        RedisValue::Hash(m) => m
            .iter()
            .map(|(f, val)| [f.as_ref(), b"=", val.as_ref()].concat())
            .collect(),
        RedisValue::HashListpack(lp) => lp
            .to_hash_map()
            .iter()
            .map(|(f, val)| [f.as_ref(), b"=", val.as_ref()].concat())
            .collect(),
        RedisValue::List(l) => l.iter().map(|e| e.to_vec()).collect(),
        RedisValue::ListListpack(lp) => lp.to_vec_deque().iter().map(|e| e.to_vec()).collect(),
        RedisValue::Set(s) => s.iter().map(|m| m.to_vec()).collect(),
        RedisValue::SetListpack(lp) => lp.to_set_value().iter().map(|m| m.to_vec()).collect(),
        RedisValue::SetIntset(is) => is.iter().map(|i| i.to_string().into_bytes()).collect(),
        RedisValue::SortedSetBPTree { members, .. } => members
            .iter()
            .map(|(m, sc)| [m.as_ref(), b"=", sc.to_string().as_bytes()].concat())
            .collect(),
        RedisValue::SortedSetListpack(lp) => lp
            .iter_pairs()
            .map(|(m, sc)| {
                let score = moon::storage::zset_score::parse_score(&sc.as_bytes())
                    .expect("a stored score must parse");
                [m.as_bytes().as_slice(), b"=", score.to_string().as_bytes()].concat()
            })
            .collect(),
        other => panic!("unexpected value in a fixture: {}", other.type_name()),
    };
    out.sort();
    out
}

/// A list is ORDERED — the fingerprint above sorts, so order gets its own
/// check for the one type where it is a guarantee.
fn list_order(v: &RedisValue) -> Option<VecDeque<Bytes>> {
    match v {
        RedisValue::List(l) => Some(l.clone()),
        RedisValue::ListListpack(lp) => Some(lp.to_vec_deque()),
        _ => None,
    }
}

// ── the cold (on-disk) plane ────────────────────────────────────────────

#[test]
fn promote_from_the_cold_tier_preserves_every_compact_encoding() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut cold_index = ColdIndex::new();

    let cases = cases();

    // Spill every fixture, recording what it looked like in hot RAM first.
    let mut before = Vec::new();
    for (i, (key, value, want)) in cases.iter().enumerate() {
        let entry = entry_of(value.clone());
        let encoding = entry.value.as_redis_value().encoding_name();
        assert_eq!(
            encoding, *want,
            "precondition: {key} must be {want} in hot RAM before the spill, not {encoding}"
        );
        spill_to_datafile(
            shard_dir,
            100 + i as u64,
            key.as_bytes(),
            &entry,
            0,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();
        before.push((element_count(value), fingerprint(value), list_order(value)));
    }

    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = Some(cold_index);

    let now_ms = db.now_ms();
    let mut failures = Vec::new();
    for ((key, _, want), (count_before, fp_before, order_before)) in cases.iter().zip(before.iter())
    {
        assert!(
            db.promote_cold_if_present(key.as_bytes(), now_ms),
            "{key} must promote back out of the cold tier"
        );
        let entry = db.get(key.as_bytes()).expect("promoted key must be hot");
        let promoted = entry.value.to_redis_value();

        let got = entry.value.as_redis_value().encoding_name();
        if got != *want {
            failures.push(format!("{key}: promoted back as {got}, want {want}"));
        }
        assert_eq!(
            element_count(&promoted),
            *count_before,
            "{key} lost elements across the cold round trip (moon#866 class)"
        );
        assert_eq!(
            fingerprint(&promoted),
            *fp_before,
            "{key} changed content across the cold round trip"
        );
        assert_eq!(
            list_order(&promoted),
            *order_before,
            "{key}: a list must keep its order across the cold round trip"
        );
    }

    assert!(
        failures.is_empty(),
        "cold promote-back flattened these encodings (moon#898): {}",
        failures.join(", ")
    );
}

// ── the in-flight spill plane ───────────────────────────────────────────

#[test]
fn promote_from_the_inflight_spill_plane_preserves_every_compact_encoding() {
    use moon::storage::db::PendingSpill;
    use moon::storage::tiered::kv_serde::serialize_collection;
    use moon::storage::value_codec::value_type_of;

    let mut db = Database::new();
    let cases = cases();

    for (i, (key, value, _)) in cases.iter().enumerate() {
        let entry = entry_of(value.clone());
        let val_ref = entry.as_redis_value();
        let bytes = serialize_collection(&val_ref).expect("fixture must serialize");
        db.spill_inflight_mark(
            Bytes::from(key.as_bytes().to_vec()),
            PendingSpill {
                req_id: i as u64 + 1,
                value_type: value_type_of(&val_ref),
                value_bytes: Bytes::from(bytes),
                ttl_ms: None,
            },
        );
    }

    let now_ms = db.now_ms();
    let mut failures = Vec::new();
    for (key, value, want) in cases.iter() {
        assert!(
            db.promote_inflight_if_present(key.as_bytes(), now_ms),
            "{key} must promote back out of the in-flight plane"
        );
        let entry = db.get(key.as_bytes()).expect("promoted key must be hot");
        let promoted = entry.value.to_redis_value();

        let got = entry.value.as_redis_value().encoding_name();
        if got != *want {
            failures.push(format!("{key}: promoted back as {got}, want {want}"));
        }
        assert_eq!(
            element_count(&promoted),
            element_count(value),
            "{key} lost elements across the in-flight round trip (moon#866 class)"
        );
        assert_eq!(
            fingerprint(&promoted),
            fingerprint(value),
            "{key} changed content across the in-flight round trip"
        );
    }

    assert!(
        failures.is_empty(),
        "in-flight promote-back flattened these encodings (moon#898): {}",
        failures.join(", ")
    );
}

// ── the trap: the NON-promoting read-through must stay full ─────────────

/// `Database::cold_read_only` feeds `ValueKind::classify_cold`, which accepts
/// ONLY the canonical full forms — a `SetListpack` arriving there falls
/// through its `_ => Err(WrongType)` arm and answers WRONGTYPE for a valid
/// set. So the moon#898 re-derivation must NOT be pushed down into the codec
/// (`kv_serde::deserialize_collection`), which both paths share.
///
/// This test is the guard on that boundary: it fails if a future change
/// "simplifies" the fix by moving it one layer down.
#[test]
fn the_non_promoting_cold_read_through_still_answers_the_full_form() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut cold_index = ColdIndex::new();

    let entry = entry_of(RedisValue::SetListpack(listpack_of(&[b"alpha", b"beta"])));
    spill_to_datafile(
        shard_dir,
        7,
        b"ss",
        &entry,
        0,
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();

    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = Some(cold_index);
    let now_ms = db.now_ms();

    // The CONSEQUENCE first, so this test fails on the behaviour and not merely
    // on the mechanism: `get_set_ref_if_alive` routes the read-through value
    // through `ValueKind::classify_cold`, whose `_ => Err(WrongType)` arm a
    // compacted value falls into. Measured with the fix pushed down into
    // `deserialize_collection`: this is `Err(WRONGTYPE)` for a valid set.
    let set_ref = db.get_set_ref_if_alive(b"ss", now_ms);
    assert!(
        set_ref.is_ok(),
        "a cold-only SET answered WRONGTYPE through the &self accessor — the \
         moon#898 re-derivation has been pushed down into the decoder that the \
         NON-promoting read-through shares with the promoting paths"
    );
    assert!(
        set_ref.unwrap().is_some(),
        "a cold-only set must remain readable through the &self accessor"
    );

    // Then the mechanism: the read-through view is deliberately still full.
    let cold = db.get_cold_value(b"ss", now_ms).expect("cold read-through");
    assert_eq!(
        cold.encoding_name(),
        "hashtable",
        "the non-promoting read-through must hand `classify_cold` the canonical \
         FULL form"
    );

    // The key is still cold: the read-through promoted nothing.
    assert!(!db.is_hot(b"ss"), "a read-through must not promote");
}

// ── the NaN-score hazard the re-derivation could have created ───────────

/// A sorted-set score is 8 RAW bytes in the spill body, so a corrupted blob
/// can spell NaN. Compacting such a zset renders the score into the listpack
/// as the text `NaN`, which `zset_score::parse_score` refuses — every later
/// read of that member would silently answer `0.0`. moon#863 hit exactly this
/// when it wired the same helper into the redis-wire loader.
///
/// `compact_for_promotion` therefore hands a zset carrying any non-finite
/// score back in its FULL form, which is what the cold path did before this
/// fix, byte for byte.
///
/// The first two assertions are the hazard itself, stated as a fact about the
/// codec rather than as a fear — without them this test would pass for any
/// reason at all, including the fixture never reaching a score.
#[test]
fn a_cold_zset_with_a_nan_score_is_left_full_rather_than_read_as_zeros() {
    use moon::storage::bptree::BPTree;
    use moon::storage::zset_score::{ScoreBuf, parse_score, render_score};
    use ordered_float::OrderedFloat;

    // The hazard, measured: a NaN does render, and the rendering does not
    // parse back. A finite control proves the round trip works otherwise.
    let mut buf = ScoreBuf::new();
    render_score(f64::NAN, &mut buf);
    assert!(
        parse_score(&buf).is_none(),
        "precondition: a NaN rendered into a listpack must be unparseable — got {:?}",
        String::from_utf8_lossy(&buf)
    );
    let mut ok = ScoreBuf::new();
    render_score(1.5, &mut ok);
    assert_eq!(
        parse_score(&ok),
        Some(1.5),
        "control: a finite score must survive the render/parse round trip"
    );

    let zset = |score: f64| {
        let mut members = std::collections::HashMap::new();
        members.insert(Bytes::from_static(b"m"), score);
        let mut tree = BPTree::new();
        tree.insert(OrderedFloat(score), Bytes::from_static(b"m"));
        RedisValue::SortedSetBPTree {
            members: Box::new(members),
            tree: Box::new(tree),
        }
    };

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut cold_index = ColdIndex::new();
    for (i, (key, score)) in [("znan", f64::NAN), ("zok", 1.5)].iter().enumerate() {
        let entry = entry_of(zset(*score));
        spill_to_datafile(
            shard_dir,
            50 + i as u64,
            key.as_bytes(),
            &entry,
            0,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();
    }

    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = Some(cold_index);
    let now_ms = db.now_ms();

    assert!(db.promote_cold_if_present(b"zok", now_ms));
    assert_eq!(
        db.get(b"zok")
            .unwrap()
            .value
            .as_redis_value()
            .encoding_name(),
        "listpack",
        "control: a finite-score zset must still compact on promote-back"
    );

    assert!(db.promote_cold_if_present(b"znan", now_ms));
    assert_eq!(
        db.get(b"znan")
            .unwrap()
            .value
            .as_redis_value()
            .encoding_name(),
        "skiplist",
        "a zset carrying a non-finite score must come back in its FULL form — \
         compacting it renders the score as the text `NaN`, which parse_score \
         refuses, so every later read of that member answers 0.0"
    );
}

// ── byte transparency across the promote-back (moon#795 / moon#903) ─────

/// A compact encoding must not rewrite the bytes a client stored (moon#795):
/// `+5`, `000000012345` and `-0` are distinct set members, and an intset
/// stores integers, so putting them into one normalises `+5` to `5` and
/// collapses distinct members together.
///
/// `SADD` already keeps such a set out of intset form at write time
/// (moon#802). The re-derivation this fix routes the cold tier through did
/// NOT: `compact_after_decode`'s intset arm decided "all integers" with a bare
/// `parse::<i64>()` instead of `numeric::canonical_i64`, which is **moon#903**.
///
/// ## This test is RED on this branch alone, and that is deliberate
///
/// moon#903 lives in `src/storage/value_codec.rs`, which belongs to the
/// encoding-authority branch (`perf/encoding-authority-896-899` — the fix
/// rides in that branch's moon#899 commit), and fixing it here would collide.
/// The assertions below state the CORRECT behaviour, so this test goes green
/// the moment that branch is underneath this one — **verified, not assumed**:
/// applying that commit's `value_codec` hunk on top of this branch turns it
/// green, and reverting turns it red again.
///
/// It is written this way on purpose. A test that encoded today's wrong answer
/// would have to be found and rewritten later; a test that encodes the right
/// one is a merge-order gate that cannot be forgotten.
///
/// Without this row the five-variant round trip passes while the members are
/// being rewritten underneath it, because `SetListpack -> SetIntset` still
/// looks like "a compact encoding survived".
#[test]
fn a_cold_set_of_non_canonical_integer_spellings_keeps_its_bytes() {
    // All-integer under `parse::<i64>()`, none of them canonical: this is the
    // fixture that reaches the intset arm. `+5` parses to 5 and `-0` to 0, so
    // an intset both REWRITES bytes and COLLAPSES members (6 -> 4).
    let non_canonical: &[&[u8]] = &[b"5", b"12345", b"-7", b"+5", b"000000012345", b"-0"];
    // The same set plus a non-integer, which cannot reach the intset arm at
    // all. It guards the other direction: a future change that routes mixed
    // sets through intset would break this row first.
    let mixed: &[&[u8]] = &[b"5", b"12345", b"-7", b"+5", b"000000012345", b"-0", b"abc"];

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path();
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut cold_index = ColdIndex::new();

    for (i, (key, members)) in [("s_noncanon", non_canonical), ("s_mixed", mixed)]
        .iter()
        .enumerate()
    {
        let entry = entry_of(RedisValue::SetListpack(listpack_of(members)));
        spill_to_datafile(
            shard_dir,
            70 + i as u64,
            key.as_bytes(),
            &entry,
            0,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();
    }

    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = Some(cold_index);
    let now_ms = db.now_ms();

    for (key, members) in [("s_noncanon", non_canonical), ("s_mixed", mixed)] {
        assert!(db.promote_cold_if_present(key.as_bytes(), now_ms));
        let promoted = db.get(key.as_bytes()).unwrap().value.to_redis_value();

        let mut want: Vec<Vec<u8>> = members.iter().map(|m| m.to_vec()).collect();
        want.sort();

        assert_eq!(
            element_count(&promoted),
            members.len(),
            "{key}: promoting back collapsed distinct members — an intset stores \
             integers, so `+5` and `5` (and `-0` and `0`) become one. \
             moon#903: `compact_after_decode` decides 'all integers' with a bare \
             `parse::<i64>()` instead of `numeric::canonical_i64`. Fixed on \
             perf/encoding-authority-896-899; RED until that is under this \
             branch."
        );
        assert_eq!(
            fingerprint(&promoted),
            want,
            "{key}: promoting back REWROTE the stored bytes (moon#795 — `+5` -> \
             `5`, `000000012345` -> `12345`, `-0` -> `0`). See moon#903."
        );
        assert_eq!(
            promoted.encoding_name(),
            "listpack",
            "{key}: a set holding a non-canonical integer spelling must come back \
             as a LISTPACK, which stores bytes, never as an intset. See moon#903."
        );
    }
}
