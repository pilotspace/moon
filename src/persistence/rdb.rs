//! RDB binary snapshot format: serialize/deserialize all Redis data types with CRC32 checksum.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read, Write};
use std::path::Path;

use anyhow::{bail, Context};
use bytes::Bytes;
use crc32fast::Hasher;
use ordered_float::OrderedFloat;

use crate::storage::bptree::BPTree;
use crate::storage::db::Database;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::{current_secs, current_time_ms, Entry, RedisValue};
use crate::storage::stream::{Stream as StreamData, StreamId};

// Format constants
const RDB_MAGIC: &[u8] = b"RUSTREDIS";
const RDB_VERSION: u8 = 1;

// Type tags
pub(crate) const TYPE_STRING: u8 = 0;
pub(crate) const TYPE_HASH: u8 = 1;
pub(crate) const TYPE_LIST: u8 = 2;
pub(crate) const TYPE_SET: u8 = 3;
pub(crate) const TYPE_SORTED_SET: u8 = 4;
pub(crate) const TYPE_STREAM: u8 = 5;

// Control bytes
const DB_SELECTOR: u8 = 0xFE;
const EOF_MARKER: u8 = 0xFF;

/// Save all databases to an RDB file at `path`.
///
/// Uses atomic write (write to .tmp, then rename) for crash safety.
/// Expired keys are skipped. Empty databases are skipped.
/// Footer contains CRC32 checksum of all preceding bytes.
pub fn save(databases: &[Database], path: &Path) -> anyhow::Result<()> {
    let mut buf = Vec::new();

    // Header
    buf.write_all(RDB_MAGIC)?;
    buf.write_all(&[RDB_VERSION])?;

    let now_ms = current_time_ms();

    // Databases
    for (db_idx, db) in databases.iter().enumerate() {
        let base_ts = db.base_timestamp();
        let data = db.data();
        // Collect non-expired entries
        let live: Vec<_> = data
            .iter()
            .filter(|(_, entry)| !entry.is_expired_at(base_ts, now_ms))
            .collect();
        if live.is_empty() {
            continue;
        }

        buf.write_all(&[DB_SELECTOR])?;
        buf.write_all(&[db_idx as u8])?;

        for (key, entry) in live {
            write_entry(&mut buf, key, entry, base_ts)?;
        }
    }

    // Footer
    buf.write_all(&[EOF_MARKER])?;

    // CRC32 of all bytes so far
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let checksum = hasher.finalize();
    buf.write_all(&checksum.to_le_bytes())?;

    // Atomic write: write to tmp, then rename
    let tmp_path = path.with_extension("rdb.tmp");
    std::fs::write(&tmp_path, &buf).context("Failed to write temporary RDB file")?;
    std::fs::rename(&tmp_path, path).context("Failed to rename temporary RDB file")?;

    Ok(())
}

/// Save from pre-cloned snapshot data (used by BGSAVE to avoid holding the lock).
///
/// Each element in `snapshot` is a Vec of (key, entry, base_ts) for a database index.
pub fn save_from_snapshot(snapshot: &[(Vec<(Bytes, Entry)>, u32)], path: &Path) -> anyhow::Result<()> {
    let mut buf = Vec::new();

    // Header
    buf.write_all(RDB_MAGIC)?;
    buf.write_all(&[RDB_VERSION])?;

    let now_ms = current_time_ms();

    for (db_idx, (entries, base_ts)) in snapshot.iter().enumerate() {
        // Filter expired and skip empty
        let live: Vec<_> = entries.iter().filter(|(_, e)| !e.is_expired_at(*base_ts, now_ms)).collect();
        if live.is_empty() {
            continue;
        }

        buf.write_all(&[DB_SELECTOR])?;
        buf.write_all(&[db_idx as u8])?;

        for (key, entry) in live {
            write_entry(&mut buf, key, entry, *base_ts)?;
        }
    }

    // Footer
    buf.write_all(&[EOF_MARKER])?;

    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let checksum = hasher.finalize();
    buf.write_all(&checksum.to_le_bytes())?;

    let tmp_path = path.with_extension("rdb.tmp");
    std::fs::write(&tmp_path, &buf).context("Failed to write temporary RDB file")?;
    std::fs::rename(&tmp_path, path).context("Failed to rename temporary RDB file")?;

    Ok(())
}

/// Load an RDB file and populate databases. Returns total keys loaded.
///
/// On any error (missing file, corrupt data, bad checksum), returns Err.
/// Caller decides whether to start with empty databases.
pub fn load(databases: &mut [Database], path: &Path) -> anyhow::Result<usize> {
    let data = std::fs::read(path).context("Failed to read RDB file")?;

    if data.len() < RDB_MAGIC.len() + 1 + 1 + 4 {
        bail!("RDB file too small");
    }

    // Verify CRC32: all bytes except last 4 vs last 4 bytes
    let (payload, checksum_bytes) = data.split_at(data.len() - 4);
    let stored_checksum = u32::from_le_bytes([
        checksum_bytes[0],
        checksum_bytes[1],
        checksum_bytes[2],
        checksum_bytes[3],
    ]);
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let computed_checksum = hasher.finalize();
    if stored_checksum != computed_checksum {
        bail!(
            "RDB checksum mismatch: stored={:#010x}, computed={:#010x}",
            stored_checksum,
            computed_checksum
        );
    }

    let mut cursor = Cursor::new(payload);

    // Verify magic
    let mut magic = [0u8; 9]; // "RUSTREDIS" is 9 bytes
    cursor.read_exact(&mut magic)?;
    if &magic != RDB_MAGIC {
        bail!("Invalid RDB magic header");
    }

    // Verify version
    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != RDB_VERSION {
        bail!("Unsupported RDB version: {}", version[0]);
    }

    let now_ms = current_time_ms();

    let mut total_keys = 0usize;
    let mut current_db: usize = 0;

    loop {
        let mut tag = [0u8; 1];
        cursor.read_exact(&mut tag)?;

        match tag[0] {
            EOF_MARKER => break,
            DB_SELECTOR => {
                let mut db_idx = [0u8; 1];
                cursor.read_exact(&mut db_idx)?;
                current_db = db_idx[0] as usize;
                if current_db >= databases.len() {
                    bail!("RDB references database {} but only {} configured", current_db, databases.len());
                }
            }
            type_tag => {
                let (key, entry) = read_entry(&mut cursor, type_tag)?;
                // Skip entries whose TTL is already in the past
                if entry.has_expiry() && entry.is_expired_at(current_secs(), now_ms) {
                    continue;
                }
                if current_db < databases.len() {
                    databases[current_db].set(key, entry);
                    total_keys += 1;
                }
            }
        }
    }

    Ok(total_keys)
}

/// Distribute keys from loaded databases to the correct per-shard databases.
///
/// After loading an RDB file into temporary databases, this function routes each key
/// to its target shard based on `key_to_shard()`. Called during bootstrap BEFORE
/// shard threads start, so no cross-shard dispatch is needed.
///
/// `shard_dbs[shard_id][db_index]` is the database layout.
pub fn distribute_loaded_to_shards(
    loaded_dbs: Vec<Database>,
    num_shards: usize,
    shard_dbs: &mut [Vec<Database>],
) {
    use crate::shard::dispatch::key_to_shard;

    for (db_idx, db) in loaded_dbs.into_iter().enumerate() {
        for (key, entry) in db.data().iter() {
            let target_shard = key_to_shard(key, num_shards);
            if target_shard < shard_dbs.len() && db_idx < shard_dbs[target_shard].len() {
                shard_dbs[target_shard][db_idx].set(key.clone(), entry.clone());
            }
        }
    }
}

/// Merge per-shard snapshots into a single snapshot suitable for `save_from_snapshot()`.
///
/// Each shard provides `Vec<(Vec<(Bytes, Entry)>, u32)>` -- one entry per database.
/// This function merges all shards' data for each database index into a combined snapshot.
pub fn merge_shard_snapshots(
    shard_snapshots: Vec<Vec<(Vec<(Bytes, Entry)>, u32)>>,
    num_databases: usize,
) -> Vec<(Vec<(Bytes, Entry)>, u32)> {
    let mut merged: Vec<(Vec<(Bytes, Entry)>, u32)> = (0..num_databases)
        .map(|_| (Vec::new(), 0u32))
        .collect();

    for shard_snap in shard_snapshots {
        for (db_idx, (entries, base_ts)) in shard_snap.into_iter().enumerate() {
            if db_idx < merged.len() {
                // Use the base_ts from the first shard that provides data for this db
                if merged[db_idx].0.is_empty() {
                    merged[db_idx].1 = base_ts;
                }
                merged[db_idx].0.extend(entries);
            }
        }
    }

    merged
}

pub(crate) fn write_entry(
    buf: &mut Vec<u8>,
    key: &Bytes,
    entry: &Entry,
    base_ts: u32,
) -> anyhow::Result<()> {
    // Type tag -- compact variants serialize as the same type as their full-size counterparts
    let type_tag = match entry.value.as_redis_value() {
        RedisValueRef::String(_) => TYPE_STRING,
        RedisValueRef::Hash(_) | RedisValueRef::HashListpack(_) => TYPE_HASH,
        RedisValueRef::List(_) | RedisValueRef::ListListpack(_) => TYPE_LIST,
        RedisValueRef::Set(_) | RedisValueRef::SetListpack(_) | RedisValueRef::SetIntset(_) => {
            TYPE_SET
        }
        RedisValueRef::SortedSet { .. }
        | RedisValueRef::SortedSetBPTree { .. }
        | RedisValueRef::SortedSetListpack(_) => TYPE_SORTED_SET,
        RedisValueRef::Stream(_) => TYPE_STREAM,
    };
    buf.write_all(&[type_tag])?;

    // Key
    write_bytes(buf, key)?;

    // TTL as unix millis (0 = no expiry)
    let ttl_ms: i64 = if entry.has_expiry() {
        entry.expires_at_ms(base_ts) as i64
    } else {
        0
    };
    buf.write_all(&ttl_ms.to_le_bytes())?;

    // Value data -- compact variants expand to element-level format for persistence
    match entry.value.as_redis_value() {
        RedisValueRef::String(s) => {
            write_bytes(buf, s)?;
        }
        RedisValueRef::Hash(map) => {
            buf.write_all(&(map.len() as u32).to_le_bytes())?;
            for (field, val) in map.iter() {
                write_bytes(buf, field)?;
                write_bytes(buf, val)?;
            }
        }
        RedisValueRef::HashListpack(lp) => {
            let map = lp.to_hash_map();
            buf.write_all(&(map.len() as u32).to_le_bytes())?;
            for (field, val) in &map {
                write_bytes(buf, field)?;
                write_bytes(buf, val)?;
            }
        }
        RedisValueRef::List(list) => {
            buf.write_all(&(list.len() as u32).to_le_bytes())?;
            for elem in list.iter() {
                write_bytes(buf, elem)?;
            }
        }
        RedisValueRef::ListListpack(lp) => {
            let list = lp.to_vec_deque();
            buf.write_all(&(list.len() as u32).to_le_bytes())?;
            for elem in &list {
                write_bytes(buf, elem)?;
            }
        }
        RedisValueRef::Set(set) => {
            buf.write_all(&(set.len() as u32).to_le_bytes())?;
            for member in set.iter() {
                write_bytes(buf, member)?;
            }
        }
        RedisValueRef::SetListpack(lp) => {
            let set = lp.to_hash_set();
            buf.write_all(&(set.len() as u32).to_le_bytes())?;
            for member in &set {
                write_bytes(buf, member)?;
            }
        }
        RedisValueRef::SetIntset(is) => {
            let set = is.to_hash_set();
            buf.write_all(&(set.len() as u32).to_le_bytes())?;
            for member in &set {
                write_bytes(buf, member)?;
            }
        }
        RedisValueRef::SortedSet { members, .. }
        | RedisValueRef::SortedSetBPTree { members, .. } => {
            buf.write_all(&(members.len() as u32).to_le_bytes())?;
            for (member, score) in members.iter() {
                write_bytes(buf, member)?;
                buf.write_all(&score.to_le_bytes())?;
            }
        }
        RedisValueRef::SortedSetListpack(lp) => {
            // Listpack stores sorted set as [member, score, member, score, ...]
            let mut count: u32 = 0;
            let pairs: Vec<_> = lp.iter_pairs().collect();
            // Write count placeholder, then entries
            let count_pos = buf.len();
            buf.write_all(&0u32.to_le_bytes())?;
            for (member_entry, score_entry) in &pairs {
                let member_bytes = member_entry.as_bytes();
                let score_bytes = score_entry.as_bytes();
                let score: f64 = std::str::from_utf8(&score_bytes)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                write_bytes(buf, &member_bytes)?;
                buf.write_all(&score.to_le_bytes())?;
                count += 1;
            }
            // Patch count
            let count_bytes = count.to_le_bytes();
            buf[count_pos..count_pos + 4].copy_from_slice(&count_bytes);
        }
        RedisValueRef::Stream(stream) => {
            // Entry count + last_id
            buf.write_all(&(stream.entries.len() as u64).to_le_bytes())?;
            buf.write_all(&stream.last_id.ms.to_le_bytes())?;
            buf.write_all(&stream.last_id.seq.to_le_bytes())?;
            // Entries
            for (id, fields) in &stream.entries {
                buf.write_all(&id.ms.to_le_bytes())?;
                buf.write_all(&id.seq.to_le_bytes())?;
                buf.write_all(&(fields.len() as u32).to_le_bytes())?;
                for (field, value) in fields {
                    write_bytes(buf, field)?;
                    write_bytes(buf, value)?;
                }
            }
            // Consumer groups
            buf.write_all(&(stream.groups.len() as u32).to_le_bytes())?;
            for (group_name, group) in &stream.groups {
                write_bytes(buf, group_name)?;
                buf.write_all(&group.last_delivered_id.ms.to_le_bytes())?;
                buf.write_all(&group.last_delivered_id.seq.to_le_bytes())?;
                // PEL
                buf.write_all(&(group.pel.len() as u32).to_le_bytes())?;
                for (id, pe) in &group.pel {
                    buf.write_all(&id.ms.to_le_bytes())?;
                    buf.write_all(&id.seq.to_le_bytes())?;
                    write_bytes(buf, &pe.consumer)?;
                    buf.write_all(&pe.delivery_time.to_le_bytes())?;
                    buf.write_all(&pe.delivery_count.to_le_bytes())?;
                }
                // Consumers
                buf.write_all(&(group.consumers.len() as u32).to_le_bytes())?;
                for (cname, consumer) in &group.consumers {
                    write_bytes(buf, cname)?;
                    buf.write_all(&consumer.seen_time.to_le_bytes())?;
                    buf.write_all(&(consumer.pending.len() as u32).to_le_bytes())?;
                    for (id, _) in &consumer.pending {
                        buf.write_all(&id.ms.to_le_bytes())?;
                        buf.write_all(&id.seq.to_le_bytes())?;
                    }
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn read_entry(
    cursor: &mut Cursor<&[u8]>,
    type_tag: u8,
) -> anyhow::Result<(Bytes, Entry)> {
    // Key
    let key = read_bytes(cursor)?;

    // TTL
    let mut ttl_buf = [0u8; 8];
    cursor.read_exact(&mut ttl_buf)?;
    let ttl_ms = i64::from_le_bytes(ttl_buf);

    // expires_at_ms: if ttl_ms > 0 it's already absolute unix millis
    let expires_at_ms = if ttl_ms > 0 {
        ttl_ms as u64
    } else {
        0
    };

    // Value
    let value = match type_tag {
        TYPE_STRING => {
            let data = read_bytes(cursor)?;
            RedisValue::String(data)
        }
        TYPE_HASH => {
            let count = read_u32(cursor)? as usize;
            let mut map = HashMap::with_capacity(count);
            for _ in 0..count {
                let field = read_bytes(cursor)?;
                let val = read_bytes(cursor)?;
                map.insert(field, val);
            }
            RedisValue::Hash(map)
        }
        TYPE_LIST => {
            let count = read_u32(cursor)? as usize;
            let mut list = VecDeque::with_capacity(count);
            for _ in 0..count {
                list.push_back(read_bytes(cursor)?);
            }
            RedisValue::List(list)
        }
        TYPE_SET => {
            let count = read_u32(cursor)? as usize;
            let mut set = HashSet::with_capacity(count);
            for _ in 0..count {
                set.insert(read_bytes(cursor)?);
            }
            RedisValue::Set(set)
        }
        TYPE_SORTED_SET => {
            let count = read_u32(cursor)? as usize;
            let mut members = HashMap::with_capacity(count);
            let mut tree = BPTree::new();
            for _ in 0..count {
                let member = read_bytes(cursor)?;
                let mut score_buf = [0u8; 8];
                cursor.read_exact(&mut score_buf)?;
                let score = f64::from_le_bytes(score_buf);
                members.insert(member.clone(), score);
                tree.insert(OrderedFloat(score), member);
            }
            RedisValue::SortedSetBPTree { tree, members }
        }
        TYPE_STREAM => {
            let mut entry_count_buf = [0u8; 8];
            cursor.read_exact(&mut entry_count_buf)?;
            let entry_count = u64::from_le_bytes(entry_count_buf) as usize;

            let mut last_id_ms_buf = [0u8; 8];
            let mut last_id_seq_buf = [0u8; 8];
            cursor.read_exact(&mut last_id_ms_buf)?;
            cursor.read_exact(&mut last_id_seq_buf)?;
            let last_id = StreamId {
                ms: u64::from_le_bytes(last_id_ms_buf),
                seq: u64::from_le_bytes(last_id_seq_buf),
            };

            let mut stream = StreamData::new();
            stream.last_id = last_id;

            for _ in 0..entry_count {
                let mut ms_buf = [0u8; 8];
                let mut seq_buf = [0u8; 8];
                cursor.read_exact(&mut ms_buf)?;
                cursor.read_exact(&mut seq_buf)?;
                let id = StreamId {
                    ms: u64::from_le_bytes(ms_buf),
                    seq: u64::from_le_bytes(seq_buf),
                };
                let field_count = read_u32(cursor)? as usize;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    let field = read_bytes(cursor)?;
                    let value = read_bytes(cursor)?;
                    fields.push((field, value));
                }
                stream.entries.insert(id, fields);
                stream.length += 1;
            }

            // Consumer groups
            let group_count = read_u32(cursor)? as usize;
            for _ in 0..group_count {
                let group_name = read_bytes(cursor)?;
                let mut gld_ms = [0u8; 8];
                let mut gld_seq = [0u8; 8];
                cursor.read_exact(&mut gld_ms)?;
                cursor.read_exact(&mut gld_seq)?;
                let last_delivered_id = StreamId {
                    ms: u64::from_le_bytes(gld_ms),
                    seq: u64::from_le_bytes(gld_seq),
                };

                let pel_count = read_u32(cursor)? as usize;
                let mut pel = BTreeMap::new();
                for _ in 0..pel_count {
                    let mut pid_ms = [0u8; 8];
                    let mut pid_seq = [0u8; 8];
                    cursor.read_exact(&mut pid_ms)?;
                    cursor.read_exact(&mut pid_seq)?;
                    let pid = StreamId {
                        ms: u64::from_le_bytes(pid_ms),
                        seq: u64::from_le_bytes(pid_seq),
                    };
                    let consumer_name = read_bytes(cursor)?;
                    let mut dt_buf = [0u8; 8];
                    let mut dc_buf = [0u8; 8];
                    cursor.read_exact(&mut dt_buf)?;
                    cursor.read_exact(&mut dc_buf)?;
                    pel.insert(pid, crate::storage::stream::PendingEntry {
                        consumer: consumer_name,
                        delivery_time: u64::from_le_bytes(dt_buf),
                        delivery_count: u64::from_le_bytes(dc_buf),
                    });
                }

                let consumer_count = read_u32(cursor)? as usize;
                let mut consumers = HashMap::new();
                for _ in 0..consumer_count {
                    let cname = read_bytes(cursor)?;
                    let mut st_buf = [0u8; 8];
                    cursor.read_exact(&mut st_buf)?;
                    let seen_time = u64::from_le_bytes(st_buf);
                    let pending_count = read_u32(cursor)? as usize;
                    let mut pending = BTreeMap::new();
                    for _ in 0..pending_count {
                        let mut cid_ms = [0u8; 8];
                        let mut cid_seq = [0u8; 8];
                        cursor.read_exact(&mut cid_ms)?;
                        cursor.read_exact(&mut cid_seq)?;
                        pending.insert(
                            StreamId {
                                ms: u64::from_le_bytes(cid_ms),
                                seq: u64::from_le_bytes(cid_seq),
                            },
                            (),
                        );
                    }
                    consumers.insert(
                        cname.clone(),
                        crate::storage::stream::Consumer {
                            name: cname,
                            pending,
                            seen_time,
                        },
                    );
                }

                stream.groups.insert(
                    group_name,
                    crate::storage::stream::ConsumerGroup {
                        last_delivered_id,
                        pel,
                        consumers,
                    },
                );
            }

            RedisValue::Stream(Box::new(stream))
        }
        _ => bail!("Unknown type tag: {}", type_tag),
    };

    // Use current_secs() as base_ts for loaded entries (matches Database::new())
    let base_ts = current_secs();
    let mut entry = if expires_at_ms > 0 {
        Entry::new_string(Bytes::new()) // placeholder, we'll replace value below
    } else {
        Entry::new_string(Bytes::new())
    };
    // Replace value with the correct one via CompactValue
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(value);
    if expires_at_ms > 0 {
        entry.set_expires_at_ms(base_ts, expires_at_ms);
    }
    entry.set_last_access(current_secs());
    entry.set_access_counter(5);

    Ok((key, entry))
}

pub(crate) fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) -> anyhow::Result<()> {
    buf.write_all(&(data.len() as u32).to_le_bytes())?;
    buf.write_all(data)?;
    Ok(())
}

pub(crate) fn read_bytes(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<Bytes> {
    let len = read_u32(cursor)? as usize;
    let mut data = vec![0u8; len];
    cursor.read_exact(&mut data)?;
    Ok(Bytes::from(data))
}

pub(crate) fn read_u32(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compact_value::RedisValueRef;
    use tempfile::tempdir;

    /// Helper: create a temp path for RDB testing.
    fn rdb_path() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dump.rdb");
        (dir, path)
    }

    #[test]
    fn test_round_trip_string_no_ttl() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"hello"), Bytes::from_static(b"world"));

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"hello").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::String(v) => assert_eq!(v, b"world"),
            _ => panic!("Expected string"),
        }
        assert!(!entry.has_expiry());
    }

    #[test]
    fn test_round_trip_string_with_ttl() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        let future_ms = current_time_ms() + 3_600_000;
        dbs[0].set_string_with_expiry(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"val"),
            future_ms,
        );

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let base_ts = loaded[0].base_timestamp();
        let entry = loaded[0].get(b"key").unwrap();
        assert!(entry.has_expiry());
        // TTL should be approximately 3600 seconds in the future (allow 5s tolerance)
        let now_ms = current_time_ms();
        let remaining_secs = (entry.expires_at_ms(base_ts) - now_ms) / 1000;
        assert!(remaining_secs > 3580 && remaining_secs <= 3600);
    }

    #[test]
    fn test_round_trip_hash() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        {
            let map = dbs[0].get_or_create_hash(b"myhash").unwrap();
            map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
            map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"myhash").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::Hash(map) => {
                assert_eq!(map.len(), 2);
                assert_eq!(map.get(&Bytes::from_static(b"f1")).unwrap().as_ref(), b"v1");
                assert_eq!(map.get(&Bytes::from_static(b"f2")).unwrap().as_ref(), b"v2");
            }
            _ => panic!("Expected hash"),
        }
    }

    #[test]
    fn test_round_trip_list() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        {
            let list = dbs[0].get_or_create_list(b"mylist").unwrap();
            list.push_back(Bytes::from_static(b"a"));
            list.push_back(Bytes::from_static(b"b"));
            list.push_back(Bytes::from_static(b"c"));
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"mylist").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::List(list) => {
                assert_eq!(list.len(), 3);
                assert_eq!(list[0].as_ref(), b"a");
                assert_eq!(list[1].as_ref(), b"b");
                assert_eq!(list[2].as_ref(), b"c");
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn test_round_trip_set() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        {
            let set = dbs[0].get_or_create_set(b"myset").unwrap();
            set.insert(Bytes::from_static(b"x"));
            set.insert(Bytes::from_static(b"y"));
            set.insert(Bytes::from_static(b"z"));
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"myset").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::Set(set) => {
                assert_eq!(set.len(), 3);
                assert!(set.contains(&Bytes::from_static(b"x")));
                assert!(set.contains(&Bytes::from_static(b"y")));
                assert!(set.contains(&Bytes::from_static(b"z")));
            }
            _ => panic!("Expected set"),
        }
    }

    #[test]
    fn test_round_trip_sorted_set() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        {
            let (members, tree) = dbs[0].get_or_create_sorted_set(b"myzset").unwrap();
            members.insert(Bytes::from_static(b"alice"), 1.5);
            tree.insert(OrderedFloat(1.5), Bytes::copy_from_slice(b"alice"));
            members.insert(Bytes::from_static(b"bob"), 2.7);
            tree.insert(OrderedFloat(2.7), Bytes::copy_from_slice(b"bob"));
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"myzset").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::SortedSetBPTree { members, tree } => {
                assert_eq!(members.len(), 2);
                assert_eq!(*members.get(&Bytes::from_static(b"alice")).unwrap(), 1.5);
                assert_eq!(*members.get(&Bytes::from_static(b"bob")).unwrap(), 2.7);
                assert_eq!(tree.len(), 2);
            }
            _ => panic!("Expected sorted set"),
        }
    }

    #[test]
    fn test_round_trip_mixed_all_types() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];

        // String
        dbs[0].set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
        // String with TTL
        let future_ms = current_time_ms() + 600_000;
        dbs[0].set_string_with_expiry(
            Bytes::from_static(b"str_ttl"),
            Bytes::from_static(b"expiring"),
            future_ms,
        );
        // Hash
        {
            let map = dbs[0].get_or_create_hash(b"h").unwrap();
            map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        }
        // List
        {
            let list = dbs[0].get_or_create_list(b"l").unwrap();
            list.push_back(Bytes::from_static(b"item"));
        }
        // Set
        {
            let set = dbs[0].get_or_create_set(b"s").unwrap();
            set.insert(Bytes::from_static(b"m"));
        }
        // Sorted set
        {
            let (members, scores) = dbs[0].get_or_create_sorted_set(b"z").unwrap();
            members.insert(Bytes::from_static(b"a"), 1.0);
            scores.insert(OrderedFloat(1.0), Bytes::from_static(b"a"));
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 6);

        // Verify each type
        assert_eq!(loaded[0].get(b"str").unwrap().value.type_name(), "string");
        assert!(loaded[0].get(b"str_ttl").unwrap().has_expiry());
        assert_eq!(loaded[0].get(b"h").unwrap().value.type_name(), "hash");
        assert_eq!(loaded[0].get(b"l").unwrap().value.type_name(), "list");
        assert_eq!(loaded[0].get(b"s").unwrap().value.type_name(), "set");
        assert_eq!(loaded[0].get(b"z").unwrap().value.type_name(), "zset");
    }

    #[test]
    fn test_expired_keys_skipped_during_save() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];

        // Live key
        dbs[0].set_string(Bytes::from_static(b"live"), Bytes::from_static(b"yes"));
        // Expired key
        let past_ms = current_time_ms() - 1000;
        let base_ts = dbs[0].base_timestamp();
        dbs[0].set(
            Bytes::from_static(b"dead"),
            Entry::new_string_with_expiry(Bytes::from_static(b"no"), past_ms, base_ts),
        );

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        assert!(loaded[0].get(b"live").is_some());
        assert!(loaded[0].get(b"dead").is_none());
    }

    #[test]
    fn test_crc32_catches_corruption() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));

        save(&dbs, &path).unwrap();

        // Corrupt a byte in the file
        let mut data = std::fs::read(&path).unwrap();
        data[RDB_MAGIC.len() + 2] ^= 0xFF; // flip a byte after header
        std::fs::write(&path, &data).unwrap();

        let mut loaded = vec![Database::new()];
        let result = load(&mut loaded, &path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }

    #[test]
    fn test_multi_database_round_trip() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new(), Database::new(), Database::new()];

        // DB 0
        dbs[0].set_string(Bytes::from_static(b"k0"), Bytes::from_static(b"v0"));
        // DB 1 is empty -- should be skipped
        // DB 2
        dbs[2].set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new(), Database::new(), Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 2);

        assert!(loaded[0].get(b"k0").is_some());
        assert_eq!(loaded[1].len(), 0); // DB 1 should be empty
        assert!(loaded[2].get(b"k2").is_some());
    }

    #[test]
    fn test_missing_file_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.rdb");
        let mut dbs = vec![Database::new()];
        let result = load(&mut dbs, &path);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_database_produces_valid_rdb() {
        let (_dir, path) = rdb_path();
        let dbs = vec![Database::new()];
        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 0);
        assert_eq!(loaded[0].len(), 0);
    }

    #[test]
    fn test_round_trip_stream() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        {
            let stream = dbs[0].get_or_create_stream(b"mystream").unwrap();
            stream.add(
                StreamId { ms: 1000, seq: 0 },
                vec![
                    (Bytes::from_static(b"name"), Bytes::from_static(b"alice")),
                    (Bytes::from_static(b"age"), Bytes::from_static(b"30")),
                ],
            );
            stream.add(
                StreamId { ms: 1001, seq: 0 },
                vec![
                    (Bytes::from_static(b"name"), Bytes::from_static(b"bob")),
                    (Bytes::from_static(b"age"), Bytes::from_static(b"25")),
                ],
            );
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);

        let entry = loaded[0].get(b"mystream").unwrap();
        assert_eq!(entry.value.type_name(), "stream");
        match entry.value.as_redis_value() {
            RedisValueRef::Stream(stream) => {
                assert_eq!(stream.entries.len(), 2);
                assert_eq!(stream.length, 2);
                assert_eq!(stream.last_id, StreamId { ms: 1001, seq: 0 });
                let fields = stream.entries.get(&StreamId { ms: 1000, seq: 0 }).unwrap();
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0.as_ref(), b"name");
                assert_eq!(fields[0].1.as_ref(), b"alice");
            }
            _ => panic!("Expected Stream"),
        }
    }
}
