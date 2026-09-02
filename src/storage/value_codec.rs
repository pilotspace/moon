//! Single value-body codec shared by RDB snapshots and KV disk-offload spill.
//!
//! Historically this format existed twice — `rdb::write_entry`'s value section
//! and `kv_serde::serialize_collection` — kept bit-compatible by hand ("format
//! identical to rdb.rs" was a doc comment, not a compiler guarantee). Every
//! format evolution (e.g. the v2 hash-TTL trailer) had to be applied to both
//! copies. This module is now the only implementation; both call sites
//! delegate here.
//!
//! # Wire format (little-endian throughout)
//!
//! The *value body* excludes any type tag / key / entry-TTL framing — those
//! belong to the container (RDB entry header or `KvLeafPage` slot header).
//!
//! | Type   | Layout                                                          |
//! |--------|-----------------------------------------------------------------|
//! | Hash   | `count:u32` then `count` × (`len:u32 field` `len:u32 value`), then TTL trailer `ttl_count:u32` + `ttl_count` × (`len:u32 field` `ttl_ms:u64`) |
//! | List   | `count:u32` then `count` × `len:u32 elem`                       |
//! | Set    | `count:u32` then `count` × `len:u32 member`                     |
//! | ZSet   | `count:u32` then `count` × (`len:u32 member` `score:f64`)       |
//! | Stream | `entry_count:u64` `last_id.ms:u64` `last_id.seq:u64`, entries, then `group_count:u32` + groups (see code) |
//!
//! Strings have no collection body — the container stores their bytes
//! directly. [`encode_value_body`] returns [`ValueCodecError::StringHasNoBody`]
//! if handed one, so a caller can never silently persist an empty body for a
//! string.
//!
//! # Decode hardening
//!
//! Every count field is validated against the remaining input length *before*
//! allocation (`validate_count`), mirroring the RDB-DoS fix — previously the
//! spill decoder (`kv_serde`) was missed by that fix and fed attacker-length
//! counts straight into `Vec::with_capacity`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{Cursor, Read};

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::persistence::kv_page::ValueType;
use crate::storage::bptree::BPTree;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::RedisValue;
use crate::storage::stream::{
    Consumer, ConsumerGroup, PendingEntry, Stream as StreamData, StreamId,
};

/// How the hash per-field-TTL trailer is handled on decode.
///
/// Writers always emit the trailer (v2 format). Readers differ by container:
/// RDB files are versioned (v1 = no trailer, v2 = trailer required), while
/// spill blobs are unversioned — pre-trailer blobs simply end after the
/// fields, so a missing trailer is forgiven.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashTtlTrailer {
    /// v1 RDB: no trailer bytes exist; do not attempt to read one.
    Absent,
    /// v2 RDB: trailer must be present; truncation is corruption.
    Required,
    /// Spill blobs: attempt the trailer, treat clean EOF at the trailer
    /// count as a legacy pre-trailer blob (plain `Hash`).
    Lenient,
}

/// Error from [`encode_value_body`] / [`decode_value_body`].
#[derive(Debug, thiserror::Error)]
pub enum ValueCodecError {
    /// Strings carry no collection body; the container stores their bytes.
    #[error("string values have no collection body")]
    StringHasNoBody,
    /// A sorted-set listpack held a score that does not parse as f64.
    ///
    /// This is fail-closed by design: the old duplicated encoders wrote
    /// `0.0` for such scores (silent corruption of the persisted zset).
    #[error("unparseable sorted-set listpack score (in-memory corruption?)")]
    CorruptScore,
    /// Truncated or self-inconsistent input.
    #[error("corrupted value body: {detail}")]
    Corrupted { detail: String },
}

impl ValueCodecError {
    fn truncated(what: &str) -> Self {
        ValueCodecError::Corrupted {
            detail: format!("truncated reading {what}"),
        }
    }
}

/// On-disk [`ValueType`] tag for a hot value.
///
/// Exhaustive over `RedisValueRef` by design: adding a new value variant
/// without a `ValueType` mapping must be a compile error, never a silent
/// mis-typed spill. Single source of truth — the former copies in
/// `kv_spill` and (inline, twice) in `eviction.rs` delegate here.
pub fn value_type_of(val: &RedisValueRef) -> ValueType {
    match val {
        RedisValueRef::String(_) => ValueType::String,
        RedisValueRef::Hash(_)
        | RedisValueRef::HashListpack(_)
        | RedisValueRef::HashWithTtl { .. } => ValueType::Hash,
        RedisValueRef::List(_) | RedisValueRef::ListListpack(_) => ValueType::List,
        RedisValueRef::Set(_) | RedisValueRef::SetListpack(_) | RedisValueRef::SetIntset(_) => {
            ValueType::Set
        }
        RedisValueRef::SortedSet { .. }
        | RedisValueRef::SortedSetBPTree { .. }
        | RedisValueRef::SortedSetListpack(_) => ValueType::ZSet,
        RedisValueRef::Stream(_) => ValueType::Stream,
    }
}

// ── Encode helpers ──

#[inline]
fn put_len_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}

#[inline]
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
fn put_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Encode the collection body of `value` into `buf` (appended).
///
/// Compact variants (listpack / intset) expand to the element-level format —
/// the encoding is canonical per logical type, so a value round-trips to its
/// full-size representation regardless of its in-memory encoding.
pub fn encode_value_body(
    value: &RedisValueRef<'_>,
    buf: &mut Vec<u8>,
) -> Result<(), ValueCodecError> {
    match value {
        RedisValueRef::String(_) => return Err(ValueCodecError::StringHasNoBody),

        RedisValueRef::Hash(map) => {
            put_u32(buf, map.len() as u32);
            for (field, val) in map.iter() {
                put_len_bytes(buf, field);
                put_len_bytes(buf, val);
            }
            // v2 trailer: plain hashes carry no per-field TTLs.
            put_u32(buf, 0);
        }
        RedisValueRef::HashWithTtl { fields, ttls, .. } => {
            put_u32(buf, fields.len() as u32);
            for (field, val) in fields.iter() {
                put_len_bytes(buf, field);
                put_len_bytes(buf, val);
            }
            // v2 trailer: per-field TTL sidecar.
            put_u32(buf, ttls.len() as u32);
            for (field, ttl_ms) in ttls.iter() {
                put_len_bytes(buf, field);
                put_u64(buf, *ttl_ms);
            }
        }
        RedisValueRef::HashListpack(lp) => {
            let map = lp.to_hash_map();
            put_u32(buf, map.len() as u32);
            for (field, val) in &map {
                put_len_bytes(buf, field);
                put_len_bytes(buf, val);
            }
            // Listpack hashes can never carry per-field TTLs.
            put_u32(buf, 0);
        }
        RedisValueRef::List(list) => {
            put_u32(buf, list.len() as u32);
            for elem in list.iter() {
                put_len_bytes(buf, elem);
            }
        }
        RedisValueRef::ListListpack(lp) => {
            let list = lp.to_vec_deque();
            put_u32(buf, list.len() as u32);
            for elem in &list {
                put_len_bytes(buf, elem);
            }
        }
        RedisValueRef::Set(set) => {
            put_u32(buf, set.len() as u32);
            for member in set.iter() {
                put_len_bytes(buf, member);
            }
        }
        RedisValueRef::SetListpack(lp) => {
            let set = lp.to_set_value();
            put_u32(buf, set.len() as u32);
            for member in &set {
                put_len_bytes(buf, member);
            }
        }
        RedisValueRef::SetIntset(is) => {
            let set = is.to_set_value();
            put_u32(buf, set.len() as u32);
            for member in &set {
                put_len_bytes(buf, member);
            }
        }
        RedisValueRef::SortedSet { members, .. }
        | RedisValueRef::SortedSetBPTree { members, .. } => {
            put_u32(buf, members.len() as u32);
            for (member, score) in members.iter() {
                put_len_bytes(buf, member);
                put_f64(buf, *score);
            }
        }
        RedisValueRef::SortedSetListpack(lp) => {
            let pairs: Vec<_> = lp.iter_pairs().collect();
            put_u32(buf, pairs.len() as u32);
            for (member_entry, score_entry) in &pairs {
                let member_bytes = member_entry.as_bytes();
                let score_bytes = score_entry.as_bytes();
                // Fail-closed: an unparseable score means in-memory
                // corruption — refuse to persist rather than write 0.0.
                let score: f64 = std::str::from_utf8(&score_bytes)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(ValueCodecError::CorruptScore)?;
                put_len_bytes(buf, &member_bytes);
                put_f64(buf, score);
            }
        }
        RedisValueRef::Stream(stream) => {
            put_u64(buf, stream.entries.len() as u64);
            put_u64(buf, stream.last_id.ms);
            put_u64(buf, stream.last_id.seq);
            for (id, fields) in &stream.entries {
                put_u64(buf, id.ms);
                put_u64(buf, id.seq);
                put_u32(buf, fields.len() as u32);
                for (field, value) in fields {
                    put_len_bytes(buf, field);
                    put_len_bytes(buf, value);
                }
            }
            put_u32(buf, stream.groups.len() as u32);
            for (group_name, group) in &stream.groups {
                put_len_bytes(buf, group_name);
                put_u64(buf, group.last_delivered_id.ms);
                put_u64(buf, group.last_delivered_id.seq);
                put_u32(buf, group.pel.len() as u32);
                for (id, pe) in &group.pel {
                    put_u64(buf, id.ms);
                    put_u64(buf, id.seq);
                    put_len_bytes(buf, &pe.consumer);
                    put_u64(buf, pe.delivery_time);
                    put_u64(buf, pe.delivery_count);
                }
                put_u32(buf, group.consumers.len() as u32);
                for (cname, consumer) in &group.consumers {
                    put_len_bytes(buf, cname);
                    put_u64(buf, consumer.seen_time);
                    put_u32(buf, consumer.pending.len() as u32);
                    for (id, _) in &consumer.pending {
                        put_u64(buf, id.ms);
                        put_u64(buf, id.seq);
                    }
                }
            }
        }
    }
    Ok(())
}

// ── Decode helpers ──

#[inline]
fn get_u32(cursor: &mut Cursor<&[u8]>, what: &str) -> Result<u32, ValueCodecError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| ValueCodecError::truncated(what))?;
    Ok(u32::from_le_bytes(buf))
}

#[inline]
fn get_u64(cursor: &mut Cursor<&[u8]>, what: &str) -> Result<u64, ValueCodecError> {
    let mut buf = [0u8; 8];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| ValueCodecError::truncated(what))?;
    Ok(u64::from_le_bytes(buf))
}

#[inline]
fn get_f64(cursor: &mut Cursor<&[u8]>, what: &str) -> Result<f64, ValueCodecError> {
    let mut buf = [0u8; 8];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| ValueCodecError::truncated(what))?;
    Ok(f64::from_le_bytes(buf))
}

#[inline]
fn get_len_bytes(cursor: &mut Cursor<&[u8]>, what: &str) -> Result<Bytes, ValueCodecError> {
    let len = get_u32(cursor, what)? as usize;
    let pos = cursor.position() as usize;
    let data = cursor.get_ref();
    let remaining = data.len().saturating_sub(pos);
    if len > remaining {
        return Err(ValueCodecError::Corrupted {
            detail: format!("{what}: length {len} exceeds remaining {remaining}"),
        });
    }
    let result = Bytes::copy_from_slice(&data[pos..pos + len]);
    cursor.set_position((pos + len) as u64);
    Ok(result)
}

/// Validate a collection count against remaining input before allocating.
///
/// `min_bytes_per_item` is the minimum wire size of one item; a count that
/// could not possibly fit in the remaining bytes is rejected up front so a
/// corrupt length field can never drive `Vec::with_capacity` (allocation
/// DoS — the fix rdb.rs received that the spill decoder previously lacked).
fn validate_count(
    cursor: &Cursor<&[u8]>,
    count: usize,
    min_bytes_per_item: usize,
    kind: &str,
) -> Result<(), ValueCodecError> {
    let remaining = cursor
        .get_ref()
        .len()
        .saturating_sub(cursor.position() as usize);
    if min_bytes_per_item > 0 && count > remaining / min_bytes_per_item {
        return Err(ValueCodecError::Corrupted {
            detail: format!("{kind} count {count} exceeds remaining data ({remaining} bytes)"),
        });
    }
    Ok(())
}

/// Decode a collection body of logical type `value_type` from `cursor`.
///
/// The cursor is left positioned at the first byte after the body, so RDB
/// entry streams can continue reading subsequent entries.
///
/// Hashes decode to `Hash` or `HashWithTtl` depending on the TTL trailer —
/// see [`HashTtlTrailer`] for per-container trailer semantics. Sorted sets
/// always decode to `SortedSetBPTree` (the canonical full-size form).
pub fn decode_value_body(
    cursor: &mut Cursor<&[u8]>,
    value_type: ValueType,
    trailer: HashTtlTrailer,
) -> Result<RedisValue, ValueCodecError> {
    match value_type {
        ValueType::String => Err(ValueCodecError::StringHasNoBody),
        ValueType::Hash => {
            let count = get_u32(cursor, "hash count")? as usize;
            validate_count(cursor, count, 8, "hash")?;
            let mut map = HashMap::with_capacity(count);
            for _ in 0..count {
                let field = get_len_bytes(cursor, "hash field")?;
                let val = get_len_bytes(cursor, "hash value")?;
                map.insert(field, val);
            }
            let ttl_count = match trailer {
                HashTtlTrailer::Absent => 0,
                HashTtlTrailer::Required => get_u32(cursor, "hash ttl trailer")? as usize,
                // Legacy pre-trailer spill blobs end exactly here; forgive a
                // clean EOF at the trailer count (and only there).
                HashTtlTrailer::Lenient => match get_u32(cursor, "hash ttl trailer") {
                    Ok(n) => n as usize,
                    Err(_) => 0,
                },
            };
            if ttl_count == 0 {
                return Ok(RedisValue::Hash(map));
            }
            validate_count(cursor, ttl_count, 12, "hash_ttls")?;
            let mut ttls = HashMap::with_capacity(ttl_count);
            for _ in 0..ttl_count {
                let field = get_len_bytes(cursor, "hash ttl field")?;
                let ttl_ms = get_u64(cursor, "hash ttl value")?;
                ttls.insert(field, ttl_ms);
            }
            // min_expiry_ms is purely in-memory; recompute after decode.
            let min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
            Ok(RedisValue::HashWithTtl {
                fields: map,
                ttls,
                min_expiry_ms,
            })
        }
        ValueType::List => {
            let count = get_u32(cursor, "list count")? as usize;
            validate_count(cursor, count, 4, "list")?;
            let mut list = VecDeque::with_capacity(count);
            for _ in 0..count {
                list.push_back(get_len_bytes(cursor, "list elem")?);
            }
            Ok(RedisValue::List(list))
        }
        ValueType::Set => {
            let count = get_u32(cursor, "set count")? as usize;
            validate_count(cursor, count, 4, "set")?;
            let mut set = crate::storage::entry::SetValue::with_capacity(count);
            for _ in 0..count {
                set.insert(get_len_bytes(cursor, "set member")?);
            }
            Ok(RedisValue::Set(set))
        }
        ValueType::ZSet => {
            let count = get_u32(cursor, "zset count")? as usize;
            validate_count(cursor, count, 12, "sorted_set")?;
            let mut members = HashMap::with_capacity(count);
            let mut tree = BPTree::new();
            for _ in 0..count {
                let member = get_len_bytes(cursor, "zset member")?;
                let score = get_f64(cursor, "zset score")?;
                members.insert(member.clone(), score);
                tree.insert(OrderedFloat(score), member);
            }
            Ok(RedisValue::SortedSetBPTree { tree, members })
        }
        ValueType::Stream => {
            let entry_count = get_u64(cursor, "stream entry count")? as usize;
            let last_id = StreamId {
                ms: get_u64(cursor, "stream last_id.ms")?,
                seq: get_u64(cursor, "stream last_id.seq")?,
            };
            let mut stream = StreamData::new();
            stream.last_id = last_id;

            validate_count(cursor, entry_count, 20, "stream_entries")?;
            for _ in 0..entry_count {
                let id = StreamId {
                    ms: get_u64(cursor, "stream id.ms")?,
                    seq: get_u64(cursor, "stream id.seq")?,
                };
                let field_count = get_u32(cursor, "stream field count")? as usize;
                validate_count(cursor, field_count, 8, "stream_fields")?;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    let field = get_len_bytes(cursor, "stream field")?;
                    let value = get_len_bytes(cursor, "stream value")?;
                    fields.push((field, value));
                }
                stream.entries.insert(id, fields);
                stream.length += 1;
            }

            let group_count = get_u32(cursor, "stream group count")? as usize;
            validate_count(cursor, group_count, 28, "stream_groups")?;
            for _ in 0..group_count {
                let group_name = get_len_bytes(cursor, "group name")?;
                let last_delivered_id = StreamId {
                    ms: get_u64(cursor, "group last_delivered.ms")?,
                    seq: get_u64(cursor, "group last_delivered.seq")?,
                };

                let pel_count = get_u32(cursor, "group pel count")? as usize;
                validate_count(cursor, pel_count, 36, "stream_pel")?;
                let mut pel = BTreeMap::new();
                for _ in 0..pel_count {
                    let pid = StreamId {
                        ms: get_u64(cursor, "pel id.ms")?,
                        seq: get_u64(cursor, "pel id.seq")?,
                    };
                    let consumer_name = get_len_bytes(cursor, "pel consumer")?;
                    let delivery_time = get_u64(cursor, "pel delivery_time")?;
                    let delivery_count = get_u64(cursor, "pel delivery_count")?;
                    pel.insert(
                        pid,
                        PendingEntry {
                            consumer: consumer_name,
                            delivery_time,
                            delivery_count,
                        },
                    );
                }

                let consumer_count = get_u32(cursor, "group consumer count")? as usize;
                validate_count(cursor, consumer_count, 16, "stream_consumers")?;
                let mut consumers = HashMap::new();
                for _ in 0..consumer_count {
                    let cname = get_len_bytes(cursor, "consumer name")?;
                    let seen_time = get_u64(cursor, "consumer seen_time")?;
                    let pending_count = get_u32(cursor, "consumer pending count")? as usize;
                    validate_count(cursor, pending_count, 16, "stream_pending")?;
                    let mut pending = BTreeMap::new();
                    for _ in 0..pending_count {
                        pending.insert(
                            StreamId {
                                ms: get_u64(cursor, "pending id.ms")?,
                                seq: get_u64(cursor, "pending id.seq")?,
                            },
                            (),
                        );
                    }
                    consumers.insert(
                        cname.clone(),
                        Consumer {
                            name: cname,
                            pending,
                            seen_time,
                        },
                    );
                }

                stream.groups.insert(
                    group_name,
                    ConsumerGroup {
                        last_delivered_id,
                        pel,
                        consumers,
                    },
                );
            }

            Ok(RedisValue::Stream(Box::new(stream)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::listpack::Listpack;

    fn decode(data: &[u8], vt: ValueType, trailer: HashTtlTrailer) -> RedisValue {
        let mut cursor = Cursor::new(data);
        decode_value_body(&mut cursor, vt, trailer).expect("decode")
    }

    /// Independent byte-level spec builders. These reconstruct the wire
    /// format by hand so the tests pin the FORMAT, not "whatever the encoder
    /// currently does" — any accidental format drift in the encoder fails
    /// these before it can corrupt cross-version compatibility.
    fn spec_len_bytes(out: &mut Vec<u8>, b: &[u8]) {
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    }

    #[test]
    fn golden_hash_single_field_matches_spec() {
        let mut map = std::collections::HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        let mut buf = Vec::new();
        encode_value_body(&RedisValueRef::Hash(&map), &mut buf).unwrap();

        let mut expect = Vec::new();
        expect.extend_from_slice(&1u32.to_le_bytes());
        spec_len_bytes(&mut expect, b"f1");
        spec_len_bytes(&mut expect, b"v1");
        expect.extend_from_slice(&0u32.to_le_bytes()); // v2 trailer, no TTLs
        assert_eq!(buf, expect);

        match decode(&buf, ValueType::Hash, HashTtlTrailer::Lenient) {
            RedisValue::Hash(m) => {
                assert_eq!(m.get(b"f1".as_ref()).unwrap(), &Bytes::from_static(b"v1"));
            }
            other => panic!("expected Hash, got {other:?}"),
        }
    }

    #[test]
    fn golden_hash_with_ttl_matches_spec() {
        let mut fields = std::collections::HashMap::new();
        fields.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        let mut ttls = std::collections::HashMap::new();
        ttls.insert(Bytes::from_static(b"f1"), 5000u64);
        let mut buf = Vec::new();
        encode_value_body(
            &RedisValueRef::HashWithTtl {
                fields: &fields,
                ttls: &ttls,
                min_expiry_ms: 5000,
            },
            &mut buf,
        )
        .unwrap();

        let mut expect = Vec::new();
        expect.extend_from_slice(&1u32.to_le_bytes());
        spec_len_bytes(&mut expect, b"f1");
        spec_len_bytes(&mut expect, b"v1");
        expect.extend_from_slice(&1u32.to_le_bytes());
        spec_len_bytes(&mut expect, b"f1");
        expect.extend_from_slice(&5000u64.to_le_bytes());
        assert_eq!(buf, expect);

        match decode(&buf, ValueType::Hash, HashTtlTrailer::Required) {
            RedisValue::HashWithTtl {
                fields: f,
                ttls: t,
                min_expiry_ms,
            } => {
                assert_eq!(f.len(), 1);
                assert_eq!(t.get(b"f1".as_ref()), Some(&5000));
                assert_eq!(min_expiry_ms, 5000);
            }
            other => panic!("expected HashWithTtl, got {other:?}"),
        }
    }

    #[test]
    fn golden_list_matches_spec() {
        let list: std::collections::VecDeque<Bytes> =
            [Bytes::from_static(b"a"), Bytes::from_static(b"bb")]
                .into_iter()
                .collect();
        let mut buf = Vec::new();
        encode_value_body(&RedisValueRef::List(&list), &mut buf).unwrap();

        let mut expect = Vec::new();
        expect.extend_from_slice(&2u32.to_le_bytes());
        spec_len_bytes(&mut expect, b"a");
        spec_len_bytes(&mut expect, b"bb");
        assert_eq!(buf, expect);

        match decode(&buf, ValueType::List, HashTtlTrailer::Lenient) {
            RedisValue::List(l) => assert_eq!(l.len(), 2),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn golden_set_single_member_matches_spec() {
        let mut set = crate::storage::entry::SetValue::new();
        set.insert(Bytes::from_static(b"m"));
        let mut buf = Vec::new();
        encode_value_body(&RedisValueRef::Set(&set), &mut buf).unwrap();

        let mut expect = Vec::new();
        expect.extend_from_slice(&1u32.to_le_bytes());
        spec_len_bytes(&mut expect, b"m");
        assert_eq!(buf, expect);
    }

    #[test]
    fn golden_zset_single_member_matches_spec() {
        let mut members = std::collections::HashMap::new();
        members.insert(Bytes::from_static(b"m"), 1.5f64);
        let mut tree = BPTree::new();
        tree.insert(OrderedFloat(1.5), Bytes::from_static(b"m"));
        let mut buf = Vec::new();
        encode_value_body(
            &RedisValueRef::SortedSetBPTree {
                tree: &tree,
                members: &members,
            },
            &mut buf,
        )
        .unwrap();

        let mut expect = Vec::new();
        expect.extend_from_slice(&1u32.to_le_bytes());
        spec_len_bytes(&mut expect, b"m");
        expect.extend_from_slice(&1.5f64.to_le_bytes());
        assert_eq!(buf, expect);

        match decode(&buf, ValueType::ZSet, HashTtlTrailer::Lenient) {
            RedisValue::SortedSetBPTree { members: m, .. } => {
                assert_eq!(m.get(b"m".as_ref()), Some(&1.5));
            }
            other => panic!("expected SortedSetBPTree, got {other:?}"),
        }
    }

    #[test]
    fn golden_stream_matches_spec() {
        let mut stream = StreamData::new();
        let id = StreamId { ms: 1, seq: 1 };
        stream.last_id = id;
        stream.entries.insert(
            id,
            vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
        );
        stream.length = 1;
        let mut pel = BTreeMap::new();
        pel.insert(
            id,
            PendingEntry {
                consumer: Bytes::from_static(b"c"),
                delivery_time: 7,
                delivery_count: 2,
            },
        );
        let mut pending = BTreeMap::new();
        pending.insert(id, ());
        let mut consumers = HashMap::new();
        consumers.insert(
            Bytes::from_static(b"c"),
            Consumer {
                name: Bytes::from_static(b"c"),
                pending,
                seen_time: 9,
            },
        );
        stream.groups.insert(
            Bytes::from_static(b"g"),
            ConsumerGroup {
                last_delivered_id: id,
                pel,
                consumers,
            },
        );

        let mut buf = Vec::new();
        encode_value_body(&RedisValueRef::Stream(&stream), &mut buf).unwrap();

        let mut expect = Vec::new();
        expect.extend_from_slice(&1u64.to_le_bytes()); // entry_count
        expect.extend_from_slice(&1u64.to_le_bytes()); // last_id.ms
        expect.extend_from_slice(&1u64.to_le_bytes()); // last_id.seq
        expect.extend_from_slice(&1u64.to_le_bytes()); // entry id.ms
        expect.extend_from_slice(&1u64.to_le_bytes()); // entry id.seq
        expect.extend_from_slice(&1u32.to_le_bytes()); // field_count
        spec_len_bytes(&mut expect, b"f");
        spec_len_bytes(&mut expect, b"v");
        expect.extend_from_slice(&1u32.to_le_bytes()); // group_count
        spec_len_bytes(&mut expect, b"g");
        expect.extend_from_slice(&1u64.to_le_bytes()); // last_delivered.ms
        expect.extend_from_slice(&1u64.to_le_bytes()); // last_delivered.seq
        expect.extend_from_slice(&1u32.to_le_bytes()); // pel_count
        expect.extend_from_slice(&1u64.to_le_bytes()); // pel id.ms
        expect.extend_from_slice(&1u64.to_le_bytes()); // pel id.seq
        spec_len_bytes(&mut expect, b"c");
        expect.extend_from_slice(&7u64.to_le_bytes()); // delivery_time
        expect.extend_from_slice(&2u64.to_le_bytes()); // delivery_count
        expect.extend_from_slice(&1u32.to_le_bytes()); // consumer_count
        spec_len_bytes(&mut expect, b"c");
        expect.extend_from_slice(&9u64.to_le_bytes()); // seen_time
        expect.extend_from_slice(&1u32.to_le_bytes()); // pending_count
        expect.extend_from_slice(&1u64.to_le_bytes()); // pending id.ms
        expect.extend_from_slice(&1u64.to_le_bytes()); // pending id.seq
        assert_eq!(buf, expect);

        match decode(&buf, ValueType::Stream, HashTtlTrailer::Lenient) {
            RedisValue::Stream(s) => {
                assert_eq!(s.entries.len(), 1);
                let g = s.groups.get(b"g".as_ref()).unwrap();
                assert_eq!(g.pel.len(), 1);
                assert_eq!(g.consumers.len(), 1);
            }
            other => panic!("expected Stream, got {other:?}"),
        }
    }

    #[test]
    fn corrupt_listpack_score_is_fail_closed() {
        // A sorted-set listpack holding a non-numeric score must refuse to
        // encode (the old duplicated encoders silently wrote 0.0).
        let mut lp = Listpack::new();
        lp.push_back(b"member");
        lp.push_back(b"not-a-number");
        let mut buf = Vec::new();
        let err = encode_value_body(&RedisValueRef::SortedSetListpack(&lp), &mut buf)
            .expect_err("corrupt score must not encode");
        assert!(matches!(err, ValueCodecError::CorruptScore));
    }

    #[test]
    fn string_has_no_body() {
        let mut buf = Vec::new();
        assert!(matches!(
            encode_value_body(&RedisValueRef::String(b"x"), &mut buf),
            Err(ValueCodecError::StringHasNoBody)
        ));
        let mut cursor = Cursor::new(&b"anything"[..]);
        assert!(matches!(
            decode_value_body(&mut cursor, ValueType::String, HashTtlTrailer::Lenient),
            Err(ValueCodecError::StringHasNoBody)
        ));
    }

    #[test]
    fn legacy_pre_trailer_hash_blob_decodes_lenient_only() {
        // Body that ends right after the fields (no trailer): old spill blobs.
        let mut blob = Vec::new();
        blob.extend_from_slice(&1u32.to_le_bytes());
        spec_len_bytes(&mut blob, b"f");
        spec_len_bytes(&mut blob, b"v");

        // Lenient (spill): decodes as plain Hash.
        match decode(&blob, ValueType::Hash, HashTtlTrailer::Lenient) {
            RedisValue::Hash(m) => assert_eq!(m.len(), 1),
            other => panic!("expected Hash, got {other:?}"),
        }
        // Required (RDB v2): truncation is corruption.
        let mut cursor = Cursor::new(blob.as_slice());
        assert!(decode_value_body(&mut cursor, ValueType::Hash, HashTtlTrailer::Required).is_err());
        // Absent (RDB v1): trailer bytes are not consumed at all.
        let mut cursor = Cursor::new(blob.as_slice());
        assert!(matches!(
            decode_value_body(&mut cursor, ValueType::Hash, HashTtlTrailer::Absent).unwrap(),
            RedisValue::Hash(_)
        ));
        assert_eq!(cursor.position() as usize, blob.len());
    }

    #[test]
    fn oversized_count_rejected_before_allocation() {
        // count=u32::MAX with a 4-byte body: must error, not with_capacity(4B).
        let mut blob = Vec::new();
        blob.extend_from_slice(&u32::MAX.to_le_bytes());
        blob.extend_from_slice(b"xxxx");
        for vt in [
            ValueType::Hash,
            ValueType::List,
            ValueType::Set,
            ValueType::ZSet,
        ] {
            let mut cursor = Cursor::new(blob.as_slice());
            assert!(
                decode_value_body(&mut cursor, vt, HashTtlTrailer::Lenient).is_err(),
                "{vt:?} must reject oversized count"
            );
        }
    }

    #[test]
    fn compact_variants_encode_canonical_format() {
        // A listpack hash and a HashMap hash with identical single-field
        // content must produce identical bytes.
        let mut lp = Listpack::new();
        lp.push_back(b"f1");
        lp.push_back(b"v1");
        let mut lp_buf = Vec::new();
        encode_value_body(&RedisValueRef::HashListpack(&lp), &mut lp_buf).unwrap();

        let mut map = std::collections::HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        let mut map_buf = Vec::new();
        encode_value_body(&RedisValueRef::Hash(&map), &mut map_buf).unwrap();

        assert_eq!(lp_buf, map_buf);
    }

    #[test]
    fn round_trip_multi_element_collections() {
        // Hash 3 fields
        let mut map = std::collections::HashMap::new();
        for i in 0..3u8 {
            map.insert(
                Bytes::copy_from_slice(&[b'f', b'0' + i]),
                Bytes::copy_from_slice(&[b'v', b'0' + i]),
            );
        }
        let mut buf = Vec::new();
        encode_value_body(&RedisValueRef::Hash(&map), &mut buf).unwrap();
        match decode(&buf, ValueType::Hash, HashTtlTrailer::Lenient) {
            RedisValue::Hash(m) => assert_eq!(m, map),
            other => panic!("expected Hash, got {other:?}"),
        }

        // ZSet 3 members
        let mut members = std::collections::HashMap::new();
        let mut tree = BPTree::new();
        for i in 0..3u8 {
            let member = Bytes::copy_from_slice(&[b'm', b'0' + i]);
            members.insert(member.clone(), i as f64 * 0.5);
            tree.insert(OrderedFloat(i as f64 * 0.5), member);
        }
        let mut buf = Vec::new();
        encode_value_body(
            &RedisValueRef::SortedSetBPTree {
                tree: &tree,
                members: &members,
            },
            &mut buf,
        )
        .unwrap();
        match decode(&buf, ValueType::ZSet, HashTtlTrailer::Lenient) {
            RedisValue::SortedSetBPTree { members: m, .. } => assert_eq!(m, members),
            other => panic!("expected SortedSetBPTree, got {other:?}"),
        }
    }
}
