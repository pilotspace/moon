//! Collection serialization/deserialization for KV disk offload.
//!
//! Converts between `RedisValueRef` / `RedisValue` and a compact binary format
//! for storage in KvLeafPage entries. The wire format mirrors rdb.rs but omits
//! the type tag prefix (stored separately in the KvLeafPage entry header).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{self, Cursor, Read, Write};

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::persistence::kv_page::ValueType;
use crate::storage::bptree::BPTree;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::RedisValue;
use crate::storage::stream::{Consumer, ConsumerGroup, PendingEntry, Stream as StreamData, StreamId};

// ── Helpers (local, avoids coupling to rdb module internals) ──

#[inline]
fn write_len_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}

#[inline]
fn read_len_bytes(cursor: &mut Cursor<&[u8]>) -> io::Result<Bytes> {
    let mut len_buf = [0u8; 4];
    cursor.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let pos = cursor.position() as usize;
    let data = cursor.get_ref();
    if pos + len > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated data"));
    }
    let result = Bytes::copy_from_slice(&data[pos..pos + len]);
    cursor.set_position((pos + len) as u64);
    Ok(result)
}

#[inline]
fn read_u32_le(cursor: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

#[inline]
fn read_u64_le(cursor: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[inline]
fn read_f64_le(cursor: &mut Cursor<&[u8]>) -> io::Result<f64> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(f64::from_le_bytes(buf))
}

// ── Public API ──

/// Serialize a collection `RedisValueRef` into bytes for KvLeafPage storage.
///
/// Uses a binary format identical to rdb.rs `write_entry` value section
/// (u32-length-prefixed fields) but without the type tag prefix.
///
/// Returns `None` for String type (strings go directly as value bytes).
pub fn serialize_collection(value: &RedisValueRef<'_>) -> Option<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    match value {
        RedisValueRef::String(_) => return None,

        RedisValueRef::Hash(map) => {
            buf.write_all(&(map.len() as u32).to_le_bytes()).ok()?;
            for (field, val) in map.iter() {
                write_len_bytes(&mut buf, field);
                write_len_bytes(&mut buf, val);
            }
        }
        RedisValueRef::HashListpack(lp) => {
            let map = lp.to_hash_map();
            buf.write_all(&(map.len() as u32).to_le_bytes()).ok()?;
            for (field, val) in &map {
                write_len_bytes(&mut buf, field);
                write_len_bytes(&mut buf, val);
            }
        }
        RedisValueRef::List(list) => {
            buf.write_all(&(list.len() as u32).to_le_bytes()).ok()?;
            for elem in list.iter() {
                write_len_bytes(&mut buf, elem);
            }
        }
        RedisValueRef::ListListpack(lp) => {
            let list = lp.to_vec_deque();
            buf.write_all(&(list.len() as u32).to_le_bytes()).ok()?;
            for elem in &list {
                write_len_bytes(&mut buf, elem);
            }
        }
        RedisValueRef::Set(set) => {
            buf.write_all(&(set.len() as u32).to_le_bytes()).ok()?;
            for member in set.iter() {
                write_len_bytes(&mut buf, member);
            }
        }
        RedisValueRef::SetListpack(lp) => {
            let set = lp.to_hash_set();
            buf.write_all(&(set.len() as u32).to_le_bytes()).ok()?;
            for member in &set {
                write_len_bytes(&mut buf, member);
            }
        }
        RedisValueRef::SetIntset(is) => {
            let set = is.to_hash_set();
            buf.write_all(&(set.len() as u32).to_le_bytes()).ok()?;
            for member in &set {
                write_len_bytes(&mut buf, member);
            }
        }
        RedisValueRef::SortedSet { members, .. }
        | RedisValueRef::SortedSetBPTree { members, .. } => {
            buf.write_all(&(members.len() as u32).to_le_bytes()).ok()?;
            for (member, score) in members.iter() {
                write_len_bytes(&mut buf, member);
                buf.write_all(&score.to_le_bytes()).ok()?;
            }
        }
        RedisValueRef::SortedSetListpack(lp) => {
            let pairs: Vec<_> = lp.iter_pairs().collect();
            let count_pos = buf.len();
            buf.write_all(&0u32.to_le_bytes()).ok()?;
            let mut count: u32 = 0;
            for (member_entry, score_entry) in &pairs {
                let member_bytes = member_entry.as_bytes();
                let score_bytes = score_entry.as_bytes();
                let score: f64 = std::str::from_utf8(&score_bytes)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                write_len_bytes(&mut buf, &member_bytes);
                buf.write_all(&score.to_le_bytes()).ok()?;
                count += 1;
            }
            buf[count_pos..count_pos + 4].copy_from_slice(&count.to_le_bytes());
        }
        RedisValueRef::Stream(stream) => {
            // Entry count + last_id
            buf.write_all(&(stream.entries.len() as u64).to_le_bytes()).ok()?;
            buf.write_all(&stream.last_id.ms.to_le_bytes()).ok()?;
            buf.write_all(&stream.last_id.seq.to_le_bytes()).ok()?;
            // Entries
            for (id, fields) in &stream.entries {
                buf.write_all(&id.ms.to_le_bytes()).ok()?;
                buf.write_all(&id.seq.to_le_bytes()).ok()?;
                buf.write_all(&(fields.len() as u32).to_le_bytes()).ok()?;
                for (field, value) in fields {
                    write_len_bytes(&mut buf, field);
                    write_len_bytes(&mut buf, value);
                }
            }
            // Consumer groups
            buf.write_all(&(stream.groups.len() as u32).to_le_bytes()).ok()?;
            for (group_name, group) in &stream.groups {
                write_len_bytes(&mut buf, group_name);
                buf.write_all(&group.last_delivered_id.ms.to_le_bytes()).ok()?;
                buf.write_all(&group.last_delivered_id.seq.to_le_bytes()).ok()?;
                // PEL
                buf.write_all(&(group.pel.len() as u32).to_le_bytes()).ok()?;
                for (id, pe) in &group.pel {
                    buf.write_all(&id.ms.to_le_bytes()).ok()?;
                    buf.write_all(&id.seq.to_le_bytes()).ok()?;
                    write_len_bytes(&mut buf, &pe.consumer);
                    buf.write_all(&pe.delivery_time.to_le_bytes()).ok()?;
                    buf.write_all(&pe.delivery_count.to_le_bytes()).ok()?;
                }
                // Consumers
                buf.write_all(&(group.consumers.len() as u32).to_le_bytes()).ok()?;
                for (cname, consumer) in &group.consumers {
                    write_len_bytes(&mut buf, cname);
                    buf.write_all(&consumer.seen_time.to_le_bytes()).ok()?;
                    buf.write_all(&(consumer.pending.len() as u32).to_le_bytes()).ok()?;
                    for (id, _) in &consumer.pending {
                        buf.write_all(&id.ms.to_le_bytes()).ok()?;
                        buf.write_all(&id.seq.to_le_bytes()).ok()?;
                    }
                }
            }
        }
    }
    Some(buf)
}

/// Deserialize collection bytes back into a `RedisValue`.
///
/// `value_type` determines which collection format to parse.
/// Returns `None` for String type or on parse failure.
pub fn deserialize_collection(data: &[u8], value_type: ValueType) -> Option<RedisValue> {
    if value_type == ValueType::String {
        return None;
    }
    let mut cursor = Cursor::new(data);
    match value_type {
        ValueType::String => None,
        ValueType::Hash => {
            let count = read_u32_le(&mut cursor).ok()? as usize;
            let mut map = HashMap::with_capacity(count);
            for _ in 0..count {
                let field = read_len_bytes(&mut cursor).ok()?;
                let val = read_len_bytes(&mut cursor).ok()?;
                map.insert(field, val);
            }
            Some(RedisValue::Hash(map))
        }
        ValueType::List => {
            let count = read_u32_le(&mut cursor).ok()? as usize;
            let mut list = VecDeque::with_capacity(count);
            for _ in 0..count {
                list.push_back(read_len_bytes(&mut cursor).ok()?);
            }
            Some(RedisValue::List(list))
        }
        ValueType::Set => {
            let count = read_u32_le(&mut cursor).ok()? as usize;
            let mut set = HashSet::with_capacity(count);
            for _ in 0..count {
                set.insert(read_len_bytes(&mut cursor).ok()?);
            }
            Some(RedisValue::Set(set))
        }
        ValueType::ZSet => {
            let count = read_u32_le(&mut cursor).ok()? as usize;
            let mut members = HashMap::with_capacity(count);
            let mut tree = BPTree::new();
            for _ in 0..count {
                let member = read_len_bytes(&mut cursor).ok()?;
                let score = read_f64_le(&mut cursor).ok()?;
                members.insert(member.clone(), score);
                tree.insert(OrderedFloat(score), member);
            }
            Some(RedisValue::SortedSetBPTree { tree, members })
        }
        ValueType::Stream => {
            let entry_count = read_u64_le(&mut cursor).ok()? as usize;
            let last_id_ms = read_u64_le(&mut cursor).ok()?;
            let last_id_seq = read_u64_le(&mut cursor).ok()?;
            let last_id = StreamId { ms: last_id_ms, seq: last_id_seq };

            let mut stream = StreamData::new();
            stream.last_id = last_id;

            for _ in 0..entry_count {
                let ms = read_u64_le(&mut cursor).ok()?;
                let seq = read_u64_le(&mut cursor).ok()?;
                let id = StreamId { ms, seq };
                let field_count = read_u32_le(&mut cursor).ok()? as usize;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    let field = read_len_bytes(&mut cursor).ok()?;
                    let value = read_len_bytes(&mut cursor).ok()?;
                    fields.push((field, value));
                }
                stream.entries.insert(id, fields);
                stream.length += 1;
            }

            // Consumer groups
            let group_count = read_u32_le(&mut cursor).ok()? as usize;
            for _ in 0..group_count {
                let group_name = read_len_bytes(&mut cursor).ok()?;
                let gld_ms = read_u64_le(&mut cursor).ok()?;
                let gld_seq = read_u64_le(&mut cursor).ok()?;
                let last_delivered_id = StreamId { ms: gld_ms, seq: gld_seq };

                let pel_count = read_u32_le(&mut cursor).ok()? as usize;
                let mut pel = BTreeMap::new();
                for _ in 0..pel_count {
                    let pid_ms = read_u64_le(&mut cursor).ok()?;
                    let pid_seq = read_u64_le(&mut cursor).ok()?;
                    let pid = StreamId { ms: pid_ms, seq: pid_seq };
                    let consumer_name = read_len_bytes(&mut cursor).ok()?;
                    let delivery_time = read_u64_le(&mut cursor).ok()?;
                    let delivery_count = read_u64_le(&mut cursor).ok()?;
                    pel.insert(pid, PendingEntry {
                        consumer: consumer_name,
                        delivery_time,
                        delivery_count,
                    });
                }

                let consumer_count = read_u32_le(&mut cursor).ok()? as usize;
                let mut consumers = HashMap::new();
                for _ in 0..consumer_count {
                    let cname = read_len_bytes(&mut cursor).ok()?;
                    let seen_time = read_u64_le(&mut cursor).ok()?;
                    let pending_count = read_u32_le(&mut cursor).ok()? as usize;
                    let mut pending = BTreeMap::new();
                    for _ in 0..pending_count {
                        let cid_ms = read_u64_le(&mut cursor).ok()?;
                        let cid_seq = read_u64_le(&mut cursor).ok()?;
                        pending.insert(StreamId { ms: cid_ms, seq: cid_seq }, ());
                    }
                    consumers.insert(cname.clone(), Consumer {
                        name: cname,
                        pending,
                        seen_time,
                    });
                }

                stream.groups.insert(group_name, ConsumerGroup {
                    last_delivered_id,
                    pel,
                    consumers,
                });
            }

            Some(RedisValue::Stream(Box::new(stream)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_roundtrip() {
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"field1"), Bytes::from_static(b"value1"));
        map.insert(Bytes::from_static(b"field2"), Bytes::from_static(b"value2"));
        let val_ref = RedisValueRef::Hash(&map);

        let serialized = serialize_collection(&val_ref).expect("should serialize");
        let deserialized = deserialize_collection(&serialized, ValueType::Hash)
            .expect("should deserialize");

        match deserialized {
            RedisValue::Hash(result_map) => {
                assert_eq!(result_map.len(), 2);
                assert_eq!(result_map.get(&Bytes::from_static(b"field1")).unwrap(), &Bytes::from_static(b"value1"));
                assert_eq!(result_map.get(&Bytes::from_static(b"field2")).unwrap(), &Bytes::from_static(b"value2"));
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
        let deserialized = deserialize_collection(&serialized, ValueType::List)
            .expect("should deserialize");

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
        let deserialized = deserialize_collection(&serialized, ValueType::Set)
            .expect("should deserialize");

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
        let deserialized = deserialize_collection(&serialized, ValueType::ZSet)
            .expect("should deserialize");

        match deserialized {
            RedisValue::SortedSetBPTree { members: result_members, .. } => {
                assert_eq!(result_members.len(), 2);
                assert_eq!(*result_members.get(&Bytes::from_static(b"m1")).unwrap(), 1.5);
                assert_eq!(*result_members.get(&Bytes::from_static(b"m2")).unwrap(), 2.5);
            }
            other => panic!("expected SortedSetBPTree, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_stream_roundtrip() {
        let mut stream = StreamData::new();
        let id = StreamId { ms: 1000, seq: 1 };
        stream.entries.insert(id, vec![
            (Bytes::from_static(b"name"), Bytes::from_static(b"alice")),
        ]);
        stream.length = 1;
        stream.last_id = id;

        let val_ref = RedisValueRef::Stream(&stream);
        let serialized = serialize_collection(&val_ref).expect("should serialize");
        let deserialized = deserialize_collection(&serialized, ValueType::Stream)
            .expect("should deserialize");

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
        let val_ref = RedisValueRef::SortedSet { members: &members, scores: &scores };
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
}
