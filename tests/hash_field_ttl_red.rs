//! Phase 195 / issue #106 — RED tests for the HashWithTtl storage primitive.
//!
//! These tests fail today (stubs return placeholder sentinels); the next
//! phase-195 implementation chunk turns the stubs into real logic and these
//! tests flip to GREEN.
//!
//! Scope of this RED suite mirrors the §"Test Plan" section of
//! `.planning/milestones/v0.2.0-phases/195-hash-with-ttl-primitive.md`:
//!
//! - promotion / downgrade identity
//! - NX / XX / GT / LT conditional gates
//! - missing-field / no-TTL / set-TTL tri-state
//! - HSET-clears-TTL interaction (deferred — covered by phase 196 tests)
//!
//! Memory benchmarks are deferred to a separate criterion bench.

use bytes::Bytes;

use moon::storage::db::{Database, FieldState, HashTtlCond};
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

fn make_string_key(db: &mut Database, key: &[u8], value: &[u8]) {
    let entry = Entry::new_string(Bytes::copy_from_slice(value));
    db.set(Bytes::copy_from_slice(key), entry);
}

#[test]
fn test_hexpire_sets_ttl_on_existing_field_returns_one() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let r = db
        .hash_set_field_ttl(b"h", b"f1", db.now_ms() + 60_000, HashTtlCond::Always)
        .expect("hash type");
    assert_eq!(r, 1, "HEXPIRE on existing field must return 1");
}

#[test]
fn test_hexpire_returns_zero_on_missing_field() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let r = db
        .hash_set_field_ttl(b"h", b"missing", db.now_ms() + 60_000, HashTtlCond::Always)
        .expect("hash type");
    assert_eq!(r, 0, "HEXPIRE on missing field must return 0");
}

#[test]
fn test_hexpire_returns_two_when_expiry_in_past() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let r = db
        .hash_set_field_ttl(b"h", b"f1", 1, HashTtlCond::Always)
        .expect("hash type");
    assert_eq!(r, 2, "expiry in past must delete field, return 2");
}

#[test]
fn test_hexpire_nx_blocks_when_ttl_already_present() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    // First, set a TTL.
    let _ = db
        .hash_set_field_ttl(b"h", b"f1", db.now_ms() + 60_000, HashTtlCond::Always)
        .expect("hash type");

    // NX must refuse — return -2.
    let r = db
        .hash_set_field_ttl(b"h", b"f1", db.now_ms() + 120_000, HashTtlCond::Nx)
        .expect("hash type");
    assert_eq!(r, -2, "NX must return -2 when TTL already exists");
}

#[test]
fn test_hexpire_xx_blocks_when_no_ttl() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let r = db
        .hash_set_field_ttl(b"h", b"f1", db.now_ms() + 60_000, HashTtlCond::Xx)
        .expect("hash type");
    assert_eq!(r, -2, "XX must return -2 when no TTL on field");
}

#[test]
fn test_hexpire_gt_blocks_when_new_less_than_current() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let now = db.now_ms();
    let _ = db
        .hash_set_field_ttl(b"h", b"f1", now + 120_000, HashTtlCond::Always)
        .unwrap();

    let r = db
        .hash_set_field_ttl(b"h", b"f1", now + 60_000, HashTtlCond::Gt)
        .unwrap();
    assert_eq!(r, -2, "GT must refuse a lower expiry");
}

#[test]
fn test_hexpire_lt_blocks_when_new_greater_than_current() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let now = db.now_ms();
    let _ = db
        .hash_set_field_ttl(b"h", b"f1", now + 60_000, HashTtlCond::Always)
        .unwrap();

    let r = db
        .hash_set_field_ttl(b"h", b"f1", now + 120_000, HashTtlCond::Lt)
        .unwrap();
    assert_eq!(r, -2, "LT must refuse a higher expiry");
}

#[test]
fn test_hexpire_wrong_type_returns_err() {
    let mut db = Database::new();
    make_string_key(&mut db, b"s", b"hello");

    let err = db.hash_set_field_ttl(b"s", b"f", db.now_ms() + 1000, HashTtlCond::Always);
    assert!(err.is_err(), "string key must yield WrongType");
}

#[test]
fn test_hash_field_state_missing_for_unknown_key() {
    let db = Database::new();
    let st = db.hash_field_state(b"nope", b"f", db.now_ms());
    assert_eq!(st, FieldState::Missing);
}

#[test]
fn test_hash_field_state_no_ttl_for_field_without_ttl() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let st = db.hash_field_state(b"h", b"f1", db.now_ms());
    assert_eq!(st, FieldState::NoTtl);
}

#[test]
fn test_hash_field_state_ttl_after_set() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let exp_ms = db.now_ms() + 60_000;
    let _ = db
        .hash_set_field_ttl(b"h", b"f1", exp_ms, HashTtlCond::Always)
        .unwrap();

    let st = db.hash_field_state(b"h", b"f1", db.now_ms());
    assert_eq!(st, FieldState::Ttl(exp_ms));
}

#[test]
fn test_hash_get_field_ttl_ms_returns_some_after_set() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let exp_ms = db.now_ms() + 60_000;
    let _ = db
        .hash_set_field_ttl(b"h", b"f1", exp_ms, HashTtlCond::Always)
        .unwrap();

    assert_eq!(db.hash_get_field_ttl_ms(b"h", b"f1"), Some(exp_ms));
}

#[test]
fn test_hpersist_removes_ttl_returns_true() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let _ = db
        .hash_set_field_ttl(b"h", b"f1", db.now_ms() + 60_000, HashTtlCond::Always)
        .unwrap();
    assert!(db.hash_persist_field(b"h", b"f1"), "had-ttl returns true");
    assert_eq!(
        db.hash_get_field_ttl_ms(b"h", b"f1"),
        None,
        "TTL must be gone after persist"
    );
}

#[test]
fn test_hpersist_returns_false_when_no_ttl() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    assert!(
        !db.hash_persist_field(b"h", b"f1"),
        "no-ttl yields false"
    );
}

#[test]
fn test_object_encoding_promotes_to_hashtable_after_hexpire() {
    // OBJECT ENCODING semantics — phase 195 commits HashWithTtl reports as
    // `hashtable` (already wired in `RedisValue::encoding_name`). This test
    // anchors the contract from phase 196 onward.
    let mut db = Database::new();
    // Insert a listpack-eligible small hash.
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);

    let _ = db
        .hash_set_field_ttl(b"h", b"f1", db.now_ms() + 60_000, HashTtlCond::Always)
        .unwrap();

    // After HEXPIRE on a listpack/Hash, encoding must be `hashtable`.
    // This requires the stub to actually mutate — RED today.
    let enc = db.data().get(b"h").map(|e| e.value.as_redis_value().encoding_name());
    assert_eq!(enc, Some("hashtable"));
}
