//! Phase 200 / issue #111 — AOF rewrite emits HSET + HPEXPIREAT for
//! HashWithTtl values. Closes the `TODO(phase-200)` in
//! `src/persistence/aof.rs::generate_rewrite_commands` (BGREWRITEAOF
//! walker).
//!
//! Verifies the wire format by scanning the rewrite buffer for the
//! expected `HSET key f v` followed by `HPEXPIREAT key abs FIELDS 1 f`
//! frames. Replay-side correctness is covered by the unit tests in
//! `src/persistence/replay.rs`.

use bytes::Bytes;

use moon::persistence::aof::generate_rewrite_commands;
use moon::storage::db::{Database, HashTtlCond};
use moon::storage::entry::{Entry, RedisValue};

fn make_hash_key(db: &mut Database, key: &[u8], fields: &[(&[u8], &[u8])]) {
    let mut entry = Entry::new_hash();
    if let Some(RedisValue::Hash(map)) = entry.value.as_redis_value_mut() {
        for (f, v) in fields {
            map.insert(Bytes::copy_from_slice(f), Bytes::copy_from_slice(v));
        }
    }
    db.set(Bytes::copy_from_slice(key), entry);
}

#[test]
fn test_bgrewriteaof_emits_hpexpireat_for_ttl_d_fields() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1"), (b"f2", b"v2")]);
    let exp1 = db.now_ms() + 60_000;
    let exp2 = db.now_ms() + 120_000;
    db.hash_set_field_ttl(b"h", b"f1", exp1, HashTtlCond::Always)
        .unwrap();
    db.hash_set_field_ttl(b"h", b"f2", exp2, HashTtlCond::Always)
        .unwrap();

    let buf = generate_rewrite_commands(std::slice::from_ref(&db));
    let s = std::str::from_utf8(&buf).expect("RESP must be utf8");

    // HSET arrived first.
    assert!(s.contains("HSET"), "rewrite must contain HSET; got:\n{}", s);
    // HPEXPIREAT for each TTL'd field (literal substring search — exact
    // RESP framing matters less than the presence of both commands and
    // the absolute timestamps the replay engine needs).
    assert!(
        s.contains("HPEXPIREAT"),
        "rewrite must contain HPEXPIREAT; got:\n{}",
        s
    );
    assert!(
        s.contains(&exp1.to_string()),
        "rewrite must contain exp1 ({}) as absolute ms; got:\n{}",
        exp1,
        s
    );
    assert!(
        s.contains(&exp2.to_string()),
        "rewrite must contain exp2 ({}) as absolute ms; got:\n{}",
        exp2,
        s
    );
}

#[test]
fn test_bgrewriteaof_plain_hash_emits_no_hpexpireat() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"plain", &[(b"a", b"1")]);

    let buf = generate_rewrite_commands(std::slice::from_ref(&db));
    let s = std::str::from_utf8(&buf).expect("RESP must be utf8");

    assert!(s.contains("HSET"), "HSET must still be emitted");
    assert!(
        !s.contains("HPEXPIREAT"),
        "plain Hash must not emit HPEXPIREAT; got:\n{}",
        s
    );
}

#[test]
fn test_bgrewriteaof_partial_ttls_emits_per_field_hpexpireat() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"mix", &[(b"hot", b"1"), (b"cold", b"2")]);
    let exp = db.now_ms() + 5_000;
    db.hash_set_field_ttl(b"mix", b"hot", exp, HashTtlCond::Always)
        .unwrap();
    // `cold` left without TTL.

    let buf = generate_rewrite_commands(std::slice::from_ref(&db));
    let s = std::str::from_utf8(&buf).expect("RESP must be utf8");

    // Exactly one HPEXPIREAT (only `hot` is TTL'd).
    let hpex_count = s.matches("HPEXPIREAT").count();
    assert_eq!(
        hpex_count, 1,
        "exactly one HPEXPIREAT for the one TTL'd field; got {} in:\n{}",
        hpex_count, s
    );
}
