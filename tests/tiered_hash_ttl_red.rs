//! Phase 200 / issue #111 — RED tests for tiered KV serde HashWithTtl
//! roundtrip. Pairs with `tests/rdb_hash_ttl_red.rs` and closes the
//! `TODO(phase-200)` in `src/storage/tiered/kv_serde.rs::serialize_collection`.
//!
//! The tiered codec serializes per-value (no type-tag prefix — caller passes
//! `ValueType`). Phase 200 extends every hash body with a
//! `[ttl_count u32][field, ttl_ms u64]*` trailer, mirroring the RDB v2 format.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use moon::persistence::kv_page::ValueType;
use moon::storage::compact_value::RedisValueRef;
use moon::storage::entry::RedisValue;
use moon::storage::tiered::kv_serde::{deserialize_collection, serialize_collection};

#[test]
fn test_tiered_serde_roundtrip_preserves_hash_field_ttls() {
    let mut fields = HashMap::new();
    fields.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
    fields.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));

    let mut ttls = BTreeMap::new();
    ttls.insert(Bytes::from_static(b"f1"), 1_700_000_000_000u64);
    ttls.insert(Bytes::from_static(b"f2"), 1_700_000_999_000u64);

    let view = RedisValueRef::HashWithTtl {
        fields: &fields,
        ttls: &ttls,
    };
    let bytes = serialize_collection(&view).expect("serialize HashWithTtl");

    let decoded = deserialize_collection(&bytes, ValueType::Hash).expect("decode");
    match decoded {
        RedisValue::HashWithTtl { fields, ttls } => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields.get(b"f1".as_ref()), Some(&Bytes::from_static(b"v1")));
            assert_eq!(fields.get(b"f2".as_ref()), Some(&Bytes::from_static(b"v2")));
            assert_eq!(ttls.get(b"f1".as_ref()), Some(&1_700_000_000_000u64));
            assert_eq!(ttls.get(b"f2".as_ref()), Some(&1_700_000_999_000u64));
        }
        other => panic!("expected HashWithTtl, got {:?}", other.encoding_name()),
    }
}

#[test]
fn test_tiered_serde_plain_hash_unchanged_by_trailer() {
    // A plain Hash also gains a ttl_count=0 trailer on disk, but decodes
    // back to Hash (not HashWithTtl) — caller observes identical behavior.
    let mut fields = HashMap::new();
    fields.insert(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
    let view = RedisValueRef::Hash(&fields);

    let bytes = serialize_collection(&view).expect("serialize Hash");
    let decoded = deserialize_collection(&bytes, ValueType::Hash).expect("decode");
    match decoded {
        RedisValue::Hash(m) => assert_eq!(m.len(), 1),
        other => panic!("expected Hash, got {:?}", other.encoding_name()),
    }
}

#[test]
fn test_tiered_serde_partial_ttls_in_hash() {
    let mut fields = HashMap::new();
    fields.insert(Bytes::from_static(b"hot"), Bytes::from_static(b"1"));
    fields.insert(Bytes::from_static(b"cold"), Bytes::from_static(b"2"));
    let mut ttls = BTreeMap::new();
    ttls.insert(Bytes::from_static(b"hot"), 1_700_000_000_000u64);
    let view = RedisValueRef::HashWithTtl {
        fields: &fields,
        ttls: &ttls,
    };

    let bytes = serialize_collection(&view).expect("serialize partial");
    let decoded = deserialize_collection(&bytes, ValueType::Hash).expect("decode");
    match decoded {
        RedisValue::HashWithTtl { fields, ttls } => {
            assert_eq!(fields.len(), 2);
            assert_eq!(ttls.len(), 1);
            assert_eq!(ttls.get(b"hot".as_ref()), Some(&1_700_000_000_000u64));
            assert!(ttls.get(b"cold".as_ref()).is_none());
        }
        other => panic!("expected HashWithTtl, got {:?}", other.encoding_name()),
    }
}
