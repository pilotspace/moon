//! Collection serialization/deserialization for KV disk offload.
//!
//! Thin wrapper over [`crate::storage::value_codec`] — the wire format is the
//! same value-body format RDB uses, minus the type tag prefix (stored
//! separately in the KvLeafPage entry header). Before the W1 unification this
//! file was a ~330-line hand-maintained copy of `rdb::write_entry`'s value
//! section; the codec module is now the single implementation.

use std::io::Cursor;

use crate::persistence::kv_page::ValueType;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::RedisValue;
use crate::storage::value_codec::{self, HashTtlTrailer};

/// Serialize a collection `RedisValueRef` into bytes for KvLeafPage storage.
///
/// Returns `None` for String type (strings go directly as value bytes) and
/// for values that fail to encode (in-memory corruption, e.g. an unparseable
/// sorted-set listpack score — logged loudly; the caller must treat this as
/// "cannot spill", never as an empty value).
pub fn serialize_collection(value: &RedisValueRef<'_>) -> Option<Vec<u8>> {
    if matches!(value, RedisValueRef::String(_)) {
        return None;
    }
    let mut buf = Vec::with_capacity(256);
    match value_codec::encode_value_body(value, &mut buf) {
        Ok(()) => Some(buf),
        Err(e) => {
            tracing::error!(
                error = %e,
                "kv_serde: refusing to serialize corrupt value for spill"
            );
            None
        }
    }
}

/// Deserialize collection bytes back into a `RedisValue`.
///
/// `value_type` determines which collection format to parse.
/// Returns `None` for String type or on parse failure (logged). Legacy
/// pre-trailer hash blobs (old in-process spill) decode as plain `Hash` —
/// see [`HashTtlTrailer::Lenient`].
pub fn deserialize_collection(data: &[u8], value_type: ValueType) -> Option<RedisValue> {
    if value_type == ValueType::String {
        return None;
    }
    let mut cursor = Cursor::new(data);
    match value_codec::decode_value_body(&mut cursor, value_type, HashTtlTrailer::Lenient) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(
                error = %e,
                ?value_type,
                "kv_serde: corrupt spill value body"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ordered_float::OrderedFloat;
    use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

    use crate::storage::stream::{Stream as StreamData, StreamId};

    #[test]
    fn test_hash_roundtrip() {
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"field1"), Bytes::from_static(b"value1"));
        map.insert(Bytes::from_static(b"field2"), Bytes::from_static(b"value2"));
        let val_ref = RedisValueRef::Hash(&map);

        let serialized = serialize_collection(&val_ref).expect("should serialize");
        let deserialized =
            deserialize_collection(&serialized, ValueType::Hash).expect("should deserialize");

        match deserialized {
            RedisValue::Hash(result_map) => {
                assert_eq!(result_map.len(), 2);
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"field1")).unwrap(),
                    &Bytes::from_static(b"value1")
                );
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"field2")).unwrap(),
                    &Bytes::from_static(b"value2")
                );
            }
            other => panic!("expected Hash, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_list_roundtrip() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"a"));
        list.push_back(Bytes::from_static(b"b"));
        list.push_back(Bytes::from_static(b"c"));
        let val_ref = RedisValueRef::List(&list);

        let serialized = serialize_collection(&val_ref).expect("should serialize");
        let deserialized =
            deserialize_collection(&serialized, ValueType::List).expect("should deserialize");

        match deserialized {
            RedisValue::List(result_list) => {
                assert_eq!(result_list.len(), 3);
                assert_eq!(result_list[0], Bytes::from_static(b"a"));
                assert_eq!(result_list[1], Bytes::from_static(b"b"));
                assert_eq!(result_list[2], Bytes::from_static(b"c"));
            }
            other => panic!("expected List, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_set_roundtrip() {
        let mut set = HashSet::new();
        set.insert(Bytes::from_static(b"x"));
        set.insert(Bytes::from_static(b"y"));
        let val_ref = RedisValueRef::Set(&set);

        let serialized = serialize_collection(&val_ref).expect("should serialize");
        let deserialized =
            deserialize_collection(&serialized, ValueType::Set).expect("should deserialize");

        match deserialized {
            RedisValue::Set(result_set) => {
                assert_eq!(result_set.len(), 2);
                assert!(result_set.contains(&Bytes::from_static(b"x")));
                assert!(result_set.contains(&Bytes::from_static(b"y")));
            }
            other => panic!("expected Set, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_zset_roundtrip() {
        let mut members = HashMap::new();
        members.insert(Bytes::from_static(b"m1"), 1.5f64);
        members.insert(Bytes::from_static(b"m2"), 2.5f64);
        let mut scores = BTreeMap::new();
        scores.insert((OrderedFloat(1.5), Bytes::from_static(b"m1")), ());
        scores.insert((OrderedFloat(2.5), Bytes::from_static(b"m2")), ());
        let val_ref = RedisValueRef::SortedSet {
            members: &members,
            scores: &scores,
        };

        let serialized = serialize_collection(&val_ref).expect("should serialize");
        let deserialized =
            deserialize_collection(&serialized, ValueType::ZSet).expect("should deserialize");

        match deserialized {
            RedisValue::SortedSetBPTree {
                members: result_members,
                ..
            } => {
                assert_eq!(result_members.len(), 2);
                assert_eq!(
                    *result_members.get(&Bytes::from_static(b"m1")).unwrap(),
                    1.5
                );
                assert_eq!(
                    *result_members.get(&Bytes::from_static(b"m2")).unwrap(),
                    2.5
                );
            }
            other => panic!("expected SortedSetBPTree, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_stream_roundtrip() {
        let mut stream = StreamData::new();
        let id = StreamId { ms: 1000, seq: 1 };
        stream.entries.insert(
            id,
            vec![(Bytes::from_static(b"name"), Bytes::from_static(b"alice"))],
        );
        stream.length = 1;
        stream.last_id = id;

        let val_ref = RedisValueRef::Stream(&stream);
        let serialized = serialize_collection(&val_ref).expect("should serialize");
        let deserialized =
            deserialize_collection(&serialized, ValueType::Stream).expect("should deserialize");

        match deserialized {
            RedisValue::Stream(result_stream) => {
                assert_eq!(result_stream.entries.len(), 1);
                assert_eq!(result_stream.last_id.ms, 1000);
                assert_eq!(result_stream.last_id.seq, 1);
                let entry = result_stream.entries.get(&id).unwrap();
                assert_eq!(entry.len(), 1);
                assert_eq!(entry[0].0, Bytes::from_static(b"name"));
                assert_eq!(entry[0].1, Bytes::from_static(b"alice"));
            }
            other => panic!("expected Stream, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_empty_collections() {
        // Empty hash
        let map = HashMap::new();
        let val_ref = RedisValueRef::Hash(&map);
        let serialized = serialize_collection(&val_ref).unwrap();
        let deserialized = deserialize_collection(&serialized, ValueType::Hash).unwrap();
        match deserialized {
            RedisValue::Hash(m) => assert!(m.is_empty()),
            _ => panic!("expected empty Hash"),
        }

        // Empty list
        let list = VecDeque::new();
        let val_ref = RedisValueRef::List(&list);
        let serialized = serialize_collection(&val_ref).unwrap();
        let deserialized = deserialize_collection(&serialized, ValueType::List).unwrap();
        match deserialized {
            RedisValue::List(l) => assert!(l.is_empty()),
            _ => panic!("expected empty List"),
        }

        // Empty set
        let set = HashSet::new();
        let val_ref = RedisValueRef::Set(&set);
        let serialized = serialize_collection(&val_ref).unwrap();
        let deserialized = deserialize_collection(&serialized, ValueType::Set).unwrap();
        match deserialized {
            RedisValue::Set(s) => assert!(s.is_empty()),
            _ => panic!("expected empty Set"),
        }

        // Empty zset
        let members = HashMap::new();
        let scores = BTreeMap::new();
        let val_ref = RedisValueRef::SortedSet {
            members: &members,
            scores: &scores,
        };
        let serialized = serialize_collection(&val_ref).unwrap();
        let deserialized = deserialize_collection(&serialized, ValueType::ZSet).unwrap();
        match deserialized {
            RedisValue::SortedSetBPTree { members: m, .. } => assert!(m.is_empty()),
            _ => panic!("expected empty ZSet"),
        }
    }

    #[test]
    fn test_string_returns_none() {
        let s: &[u8] = b"hello";
        let val_ref = RedisValueRef::String(s);
        assert!(serialize_collection(&val_ref).is_none());
        assert!(deserialize_collection(b"anything", ValueType::String).is_none());
    }

    #[test]
    fn test_corrupt_value_returns_none_not_empty() {
        // W1 fail-closed pin: a sorted-set listpack with an unparseable score
        // must serialize to None (cannot spill), never to a 0.0-score body.
        let mut lp = crate::storage::listpack::Listpack::new();
        lp.push_back(b"member");
        lp.push_back(b"not-a-number");
        let val_ref = RedisValueRef::SortedSetListpack(&lp);
        assert!(serialize_collection(&val_ref).is_none());
    }
}
