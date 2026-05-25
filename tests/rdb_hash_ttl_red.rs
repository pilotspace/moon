//! Phase 200 / issue #111 — RED tests for RDB v2 hash-field TTL roundtrip.
//!
//! These tests fail today (RDB writer drops the `ttls` sidecar — see the
//! `TODO(phase-200)` in `src/persistence/rdb.rs::write_entry`). The next
//! phase-200 commit bumps RDB_VERSION 1 → 2, appends a per-hash TTL trailer,
//! and these tests flip GREEN.
//!
//! Scope:
//! - HashWithTtl survives save/load roundtrip with TTLs intact.
//! - Plain Hash continues to roundtrip without spurious TTL data.
//! - HashWithTtl with mixed TTL'd and TTL-less fields preserves which is which.
//! - Multiple HashWithTtl entries in one RDB file each keep their own TTL maps.

use bytes::Bytes;

use moon::persistence::rdb::{load_from_bytes, save_to_bytes};
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
fn test_rdb_save_and_load_preserves_field_ttls() {
    // Build a database with one HashWithTtl key.
    let mut db = Database::new();
    make_hash_key(&mut db, b"h1", &[(b"f1", b"v1"), (b"f2", b"v2")]);

    let exp1 = db.now_ms() + 60_000;
    let exp2 = db.now_ms() + 120_000;
    db.hash_set_field_ttl(b"h1", b"f1", exp1, HashTtlCond::Always).unwrap();
    db.hash_set_field_ttl(b"h1", b"f2", exp2, HashTtlCond::Always).unwrap();

    // Save → bytes
    let dbs = vec![db];
    let buf = save_to_bytes(&dbs).expect("save_to_bytes");

    // Load → fresh databases
    let mut loaded = vec![Database::new()];
    let (_total, _len) = load_from_bytes(&mut loaded, &buf).expect("load_from_bytes");

    // Both TTLs must survive the roundtrip.
    let got1 = loaded[0].hash_get_field_ttl_ms(b"h1", b"f1");
    let got2 = loaded[0].hash_get_field_ttl_ms(b"h1", b"f2");
    assert_eq!(got1, Some(exp1), "f1 TTL must survive RDB roundtrip");
    assert_eq!(got2, Some(exp2), "f2 TTL must survive RDB roundtrip");
}

#[test]
fn test_rdb_roundtrip_preserves_field_values_alongside_ttls() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"k1", b"hello"), (b"k2", b"world")]);
    let exp = db.now_ms() + 5_000;
    db.hash_set_field_ttl(b"h", b"k1", exp, HashTtlCond::Always).unwrap();

    let dbs = vec![db];
    let buf = save_to_bytes(&dbs).unwrap();
    let mut loaded = vec![Database::new()];
    load_from_bytes(&mut loaded, &buf).unwrap();

    // Both values still present.
    let data = loaded[0].data();
    let entry = data.get(b"h".as_ref()).expect("key h present");
    match entry.value.as_redis_value() {
        moon::storage::compact_value::RedisValueRef::HashWithTtl { fields, .. } => {
            assert_eq!(fields.get(b"k1".as_ref()).map(|v| v.as_ref()), Some(b"hello".as_ref()));
            assert_eq!(fields.get(b"k2".as_ref()).map(|v| v.as_ref()), Some(b"world".as_ref()));
        }
        other => panic!("expected HashWithTtl after roundtrip, got {:?}", other.encoding_name()),
    }
}

#[test]
fn test_rdb_plain_hash_roundtrips_without_ttls() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"plain", &[(b"a", b"1"), (b"b", b"2")]);

    let dbs = vec![db];
    let buf = save_to_bytes(&dbs).unwrap();
    let mut loaded = vec![Database::new()];
    load_from_bytes(&mut loaded, &buf).unwrap();

    // No TTL — both fields visible as plain Hash, no spurious TTL data.
    assert_eq!(loaded[0].hash_get_field_ttl_ms(b"plain", b"a"), None);
    assert_eq!(loaded[0].hash_get_field_ttl_ms(b"plain", b"b"), None);
}

#[test]
fn test_rdb_mixed_ttl_and_no_ttl_fields_in_same_hash() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"mix", &[(b"hot", b"1"), (b"cold", b"2"), (b"warm", b"3")]);
    let exp = db.now_ms() + 30_000;
    db.hash_set_field_ttl(b"mix", b"hot", exp, HashTtlCond::Always).unwrap();
    // `cold` and `warm` remain TTL-less.

    let dbs = vec![db];
    let buf = save_to_bytes(&dbs).unwrap();
    let mut loaded = vec![Database::new()];
    load_from_bytes(&mut loaded, &buf).unwrap();

    assert_eq!(loaded[0].hash_get_field_ttl_ms(b"mix", b"hot"), Some(exp));
    assert_eq!(loaded[0].hash_get_field_ttl_ms(b"mix", b"cold"), None);
    assert_eq!(loaded[0].hash_get_field_ttl_ms(b"mix", b"warm"), None);
}

#[test]
fn test_rdb_multiple_hashes_each_keep_own_ttl_map() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h_a", &[(b"f", b"a")]);
    make_hash_key(&mut db, b"h_b", &[(b"f", b"b")]);
    let exp_a = db.now_ms() + 1_000;
    let exp_b = db.now_ms() + 999_000;
    db.hash_set_field_ttl(b"h_a", b"f", exp_a, HashTtlCond::Always).unwrap();
    db.hash_set_field_ttl(b"h_b", b"f", exp_b, HashTtlCond::Always).unwrap();

    let dbs = vec![db];
    let buf = save_to_bytes(&dbs).unwrap();
    let mut loaded = vec![Database::new()];
    load_from_bytes(&mut loaded, &buf).unwrap();

    assert_eq!(loaded[0].hash_get_field_ttl_ms(b"h_a", b"f"), Some(exp_a));
    assert_eq!(loaded[0].hash_get_field_ttl_ms(b"h_b", b"f"), Some(exp_b));
}
