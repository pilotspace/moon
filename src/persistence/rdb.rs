//! RDB binary snapshot format: serialize/deserialize all Redis data types with CRC32 checksum.
//!
//! ## Unwrap Classification
//!
//! | Context | Classification | Rationale |
//! |---------|---------------|-----------|
//! | `save`, `save_from_snapshot` | **should-recover** (`Result<_, MoonError>`) | Save failure should not crash server |
//! | `load` | **should-recover** (`Result<_, MoonError>`) | Load failure at startup = log + continue empty |
//! | `read_entry` | **should-recover** (`Result<_, MoonError>`) | Individual entry parse failure |
//! | `write_entry`, `write_bytes`, `read_bytes`, `read_u32` | **should-recover** (`Result<_, MoonError>`) | I/O helpers |
//! | All `unwrap()` calls (54) | **test-only** | Only appear in `#[cfg(test)]` module |

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read, Write};
use std::path::Path;

use bytes::Bytes;
use crc32fast::Hasher;
use ordered_float::OrderedFloat;

use crate::error::{MoonError, RdbError};
use crate::persistence::kv_page::ValueType;
use crate::storage::bptree::BPTree;
use crate::storage::compact_key::CompactKey;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, RedisValue, current_secs, current_time_ms};
use crate::storage::stream::{Stream as StreamData, StreamId};
use crate::storage::value_codec::{self, HashTtlTrailer};

// Format constants
const RDB_MAGIC: &[u8] = b"MOON";
/// RDB file format version.
///
/// v1: original format — hash entries had no per-field TTL trailer.
/// v2: appends `[ttl_count u32][field, ttl_ms u64]*` after every hash body
///     (count is 0 for non-TTL hashes). New writers emit v2; readers accept
///     both v1 and v2.
const RDB_VERSION: u8 = 2;
const RDB_VERSION_V1: u8 = 1;

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
/// Serialize all databases to RDB format in memory.
///
/// Returns the complete RDB byte stream (header + entries + footer + CRC32).
/// Used by both `save()` (file) and AOF RDB-preamble rewrite.
pub fn save_to_bytes<D: std::borrow::Borrow<Database>>(
    databases: &[D],
) -> Result<Vec<u8>, MoonError> {
    let mut buf = Vec::new();

    // Header
    buf.write_all(RDB_MAGIC)?;
    buf.write_all(&[RDB_VERSION])?;

    let now_ms = current_time_ms();

    // Databases
    for (db_idx, db) in databases.iter().enumerate() {
        let db: &Database = db.borrow();
        let data = db.data();
        let live: Vec<_> = data
            .iter()
            .filter(|(_, entry)| !entry.is_expired_at(now_ms))
            .collect();
        if live.is_empty() {
            continue;
        }

        buf.write_all(&[DB_SELECTOR])?;
        buf.write_all(&[db_idx as u8])?;

        for (key, entry) in live {
            write_entry(&mut buf, key.as_bytes(), entry)?;
        }
    }

    // Footer
    buf.write_all(&[EOF_MARKER])?;

    // CRC32 of all bytes so far
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let checksum = hasher.finalize();
    buf.write_all(&checksum.to_le_bytes())?;

    Ok(buf)
}

pub fn save(databases: &[Database], path: &Path) -> Result<(), MoonError> {
    let buf = save_to_bytes(databases)?;

    // Atomic write: temp + fsync + rename + dir-fsync via the shared K3
    // primitive (task #49) -- kill-9 between write and rename can never
    // surface a torn or empty RDB at `path`.
    crate::persistence::atomic::atomic_write_durable(path, &buf).map_err(|e| RdbError::Io {
        path: path.to_path_buf(),
        source: e.into(),
    })?;

    Ok(())
}

/// Save from pre-cloned snapshot data (used by BGSAVE to avoid holding the lock).
///
/// Each element in `snapshot` is a Vec of (key, entry) for a database index.
pub fn save_from_snapshot(
    snapshot: &[Vec<(CompactKey, Entry)>],
    path: &Path,
) -> Result<(), MoonError> {
    let mut buf = Vec::new();

    // Header
    buf.write_all(RDB_MAGIC)?;
    buf.write_all(&[RDB_VERSION])?;

    let now_ms = current_time_ms();

    for (db_idx, entries) in snapshot.iter().enumerate() {
        // Filter expired and skip empty
        let live: Vec<_> = entries
            .iter()
            .filter(|(_, e)| !e.is_expired_at(now_ms))
            .collect();
        if live.is_empty() {
            continue;
        }

        buf.write_all(&[DB_SELECTOR])?;
        buf.write_all(&[db_idx as u8])?;

        for (key, entry) in live {
            write_entry(&mut buf, key.as_bytes(), entry)?;
        }
    }

    // Footer
    buf.write_all(&[EOF_MARKER])?;

    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let checksum = hasher.finalize();
    buf.write_all(&checksum.to_le_bytes())?;

    crate::persistence::atomic::atomic_write_durable(path, &buf).map_err(|e| RdbError::Io {
        path: path.to_path_buf(),
        source: e.into(),
    })?;

    Ok(())
}

/// Serialize snapshot data (with correct base_ts per database) to RDB bytes in memory.
///
/// Unlike `save_to_bytes(&[Database])`, this takes pre-cloned entry Vecs —
/// used by AOF rewrite where entries are cloned into temporary storage.
pub fn save_snapshot_to_bytes(snapshot: &[Vec<(CompactKey, Entry)>]) -> Result<Vec<u8>, MoonError> {
    let mut buf = Vec::new();

    buf.write_all(RDB_MAGIC)?;
    buf.write_all(&[RDB_VERSION])?;

    let now_ms = current_time_ms();

    for (db_idx, entries) in snapshot.iter().enumerate() {
        let live: Vec<_> = entries
            .iter()
            .filter(|(_, e)| !e.is_expired_at(now_ms))
            .collect();
        if live.is_empty() {
            continue;
        }

        buf.write_all(&[DB_SELECTOR])?;
        buf.write_all(&[db_idx as u8])?;

        for (key, entry) in live {
            write_entry(&mut buf, key.as_bytes(), entry)?;
        }
    }

    buf.write_all(&[EOF_MARKER])?;
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    buf.write_all(&hasher.finalize().to_le_bytes())?;

    Ok(buf)
}

/// Load an RDB file and populate databases. Returns total keys loaded.
///
/// On any error (missing file, corrupt data, bad checksum), returns Err.
/// Caller decides whether to start with empty databases.
///
/// Mid-stream corruption (individual entry parse failures, unsupported type tags)
/// is handled with log+skip: the corrupted entry is skipped and loading continues.
/// Header, version, and checksum failures remain hard errors since the whole file is suspect.
pub fn load(databases: &mut [Database], path: &Path) -> Result<usize, MoonError> {
    let data = std::fs::read(path).map_err(|e| RdbError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;

    if data.len() < RDB_MAGIC.len() + 1 + 1 + 4 {
        return Err(RdbError::Corrupted {
            detail: "RDB file too small".into(),
        }
        .into());
    }

    // Wrap in Bytes for zero-copy slicing (shared refcount, no copy)
    let shared_buf = Bytes::from(data);

    // Verify CRC32: all bytes except last 4 vs last 4 bytes
    let payload_len = shared_buf.len() - 4;
    let stored_checksum = u32::from_le_bytes([
        shared_buf[payload_len],
        shared_buf[payload_len + 1],
        shared_buf[payload_len + 2],
        shared_buf[payload_len + 3],
    ]);
    let mut hasher = Hasher::new();
    hasher.update(&shared_buf[..payload_len]);
    let computed_checksum = hasher.finalize();
    if stored_checksum != computed_checksum {
        return Err(RdbError::ChecksumMismatch.into());
    }

    let mut cursor = Cursor::new(&shared_buf[..payload_len] as &[u8]);

    // Verify magic
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic).map_err(|e| RdbError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    if &magic != RDB_MAGIC {
        return Err(RdbError::Corrupted {
            detail: "invalid RDB magic header".into(),
        }
        .into());
    }

    // Verify version
    let mut version = [0u8; 1];
    cursor.read_exact(&mut version).map_err(|e| RdbError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let file_version = version[0];
    if file_version != RDB_VERSION && file_version != RDB_VERSION_V1 {
        return Err(RdbError::UnsupportedVersion {
            version: file_version as u32,
        }
        .into());
    }
    let has_hash_ttl_trailer = file_version >= RDB_VERSION;

    // Cache timestamps once (Fix #4: avoid syscall per entry)
    let now_ms = current_time_ms();
    let now_secs = (now_ms / 1000) as u32;

    // Load into temporary databases so old keys not present in the RDB
    // snapshot are discarded. An RDB file is a full point-in-time snapshot
    // and must replace state, not merge into it. Loading into temps also
    // provides atomicity: on failure, the live databases are untouched.
    let db_count = databases.len();
    let mut temp_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();

    // First pass: count entries per database for pre-sizing
    let entry_counts = count_entries_per_db(&cursor, db_count, has_hash_ttl_trailer);
    for (db_idx, &count) in entry_counts.iter().enumerate() {
        if count > 0 && db_idx < db_count {
            temp_dbs[db_idx].reserve(count);
        }
    }

    let mut total_keys = 0usize;
    let mut current_db: usize = 0;

    loop {
        let mut tag = [0u8; 1];
        if cursor.read_exact(&mut tag).is_err() {
            tracing::warn!(
                "RDB load: truncated tail after {} keys (no EOF marker)",
                total_keys
            );
            break;
        }

        match tag[0] {
            EOF_MARKER => break,
            DB_SELECTOR => {
                let mut db_idx = [0u8; 1];
                cursor.read_exact(&mut db_idx).map_err(|e| RdbError::Io {
                    path: path.to_path_buf(),
                    source: e,
                })?;
                current_db = db_idx[0] as usize;
                if current_db >= db_count {
                    return Err(RdbError::Corrupted {
                        detail: format!(
                            "RDB references database {} but only {} configured",
                            current_db, db_count
                        ),
                    }
                    .into());
                }
            }
            type_tag => {
                match read_entry_zero_copy(&mut cursor, type_tag, now_secs, has_hash_ttl_trailer) {
                    Ok((key, entry)) => {
                        if entry.has_expiry() && entry.is_expired_at(now_ms) {
                            continue;
                        }
                        if current_db < db_count {
                            temp_dbs[current_db].insert_for_load(key, entry);
                            total_keys += 1;
                        }
                    }
                    Err(e) => {
                        // Do NOT swap partially-loaded temp_dbs into live databases.
                        // A corrupted-but-checksummed RDB must not commit partial state.
                        return Err(RdbError::Corrupted {
                        detail: format!(
                            "RDB load: corrupted entry at offset {}: {}. {} keys loaded before failure.",
                            cursor.position(),
                            e,
                            total_keys
                        ),
                    }
                    .into());
                    }
                }
            }
        }
    }

    // Recalculate memory on temp databases, then swap into live ones.
    // Only reached if all entries parsed successfully — no partial state.
    for (live, mut temp) in databases.iter_mut().zip(temp_dbs.into_iter()) {
        temp.recalculate_memory();
        *live = temp;
    }

    Ok(total_keys)
}

/// Fast first-pass: count entries per database without parsing values.
/// Scans type tags and skips over entry payloads to count keys per db_idx.
fn count_entries_per_db(
    cursor: &Cursor<&[u8]>,
    db_count: usize,
    has_hash_ttl_trailer: bool,
) -> Vec<usize> {
    let mut counts = vec![0usize; db_count];
    let data = cursor.get_ref();
    let mut pos = cursor.position() as usize;
    let mut current_db = 0usize;

    while pos < data.len() {
        let tag = data[pos];
        pos += 1;

        match tag {
            EOF_MARKER => break,
            DB_SELECTOR => {
                if pos < data.len() {
                    current_db = data[pos] as usize;
                    pos += 1;
                } else {
                    break;
                }
            }
            TYPE_STRING | TYPE_HASH | TYPE_LIST | TYPE_SET | TYPE_SORTED_SET | TYPE_STREAM => {
                if current_db < db_count {
                    counts[current_db] += 1;
                }
                // Skip over the entry payload without parsing
                if let Some(new_pos) = skip_entry(data, pos, tag, has_hash_ttl_trailer) {
                    pos = new_pos;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }

    counts
}

/// Skip over an RDB entry's bytes without allocating or parsing values.
/// Returns the new position after the entry, or None if data is truncated.
fn skip_entry(
    data: &[u8],
    mut pos: usize,
    type_tag: u8,
    has_hash_ttl_trailer: bool,
) -> Option<usize> {
    // Skip key
    pos = skip_bytes_field(data, pos)?;
    // Skip TTL (8 bytes)
    pos = pos.checked_add(8)?;
    if pos > data.len() {
        return None;
    }

    match type_tag {
        TYPE_STRING => {
            pos = skip_bytes_field(data, pos)?;
        }
        TYPE_HASH => {
            let count = read_u32_raw(data, pos)?;
            pos += 4;
            for _ in 0..count {
                pos = skip_bytes_field(data, pos)?; // field
                pos = skip_bytes_field(data, pos)?; // value
            }
            // v2 RDB: per-field TTL trailer follows every hash body.
            //   [ttl_count u32][field, ttl_ms u64]*
            // v1 files skip this entirely.
            if has_hash_ttl_trailer {
                let ttl_count = read_u32_raw(data, pos)?;
                pos += 4;
                for _ in 0..ttl_count {
                    pos = skip_bytes_field(data, pos)?; // field name
                    pos = pos.checked_add(8)?; // ttl_ms u64
                    if pos > data.len() {
                        return None;
                    }
                }
            }
        }
        TYPE_LIST | TYPE_SET => {
            let count = read_u32_raw(data, pos)?;
            pos += 4;
            for _ in 0..count {
                pos = skip_bytes_field(data, pos)?;
            }
        }
        TYPE_SORTED_SET => {
            let count = read_u32_raw(data, pos)?;
            pos += 4;
            for _ in 0..count {
                pos = skip_bytes_field(data, pos)?; // member
                pos = pos.checked_add(8)?; // f64 score
                if pos > data.len() {
                    return None;
                }
            }
        }
        TYPE_STREAM => {
            // entry_count(8) + last_id(16)
            pos = pos.checked_add(24)?;
            if pos > data.len() {
                return None;
            }
            let entry_count =
                u64::from_le_bytes(data[pos - 24..pos - 16].try_into().ok()?) as usize;
            for _ in 0..entry_count {
                pos = pos.checked_add(16)?; // StreamId (ms + seq)
                if pos > data.len() {
                    return None;
                }
                let field_count = read_u32_raw(data, pos)?;
                pos += 4;
                for _ in 0..field_count {
                    pos = skip_bytes_field(data, pos)?;
                    pos = skip_bytes_field(data, pos)?;
                }
            }
            // Consumer groups
            let group_count = read_u32_raw(data, pos)?;
            pos += 4;
            for _ in 0..group_count {
                pos = skip_bytes_field(data, pos)?; // group name
                pos = pos.checked_add(16)?; // last_delivered_id
                if pos > data.len() {
                    return None;
                }
                let pel_count = read_u32_raw(data, pos)?;
                pos += 4;
                for _ in 0..pel_count {
                    pos = pos.checked_add(16)?; // StreamId
                    if pos > data.len() {
                        return None;
                    }
                    pos = skip_bytes_field(data, pos)?; // consumer name
                    pos = pos.checked_add(16)?; // delivery_time + delivery_count
                    if pos > data.len() {
                        return None;
                    }
                }
                let consumer_count = read_u32_raw(data, pos)?;
                pos += 4;
                for _ in 0..consumer_count {
                    pos = skip_bytes_field(data, pos)?; // consumer name
                    pos = pos.checked_add(8)?; // seen_time
                    if pos > data.len() {
                        return None;
                    }
                    let pending_count = read_u32_raw(data, pos)?;
                    pos += 4;
                    for _ in 0..pending_count {
                        pos = pos.checked_add(16)?; // StreamId
                        if pos > data.len() {
                            return None;
                        }
                    }
                }
            }
        }
        _ => return None,
    }

    Some(pos)
}

/// Read u32 LE from raw bytes without cursor overhead.
#[inline]
fn read_u32_raw(data: &[u8], pos: usize) -> Option<usize> {
    if pos + 4 > data.len() {
        return None;
    }
    Some(u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize)
}

/// Skip a length-prefixed bytes field (4-byte LE length + payload).
#[inline]
fn skip_bytes_field(data: &[u8], pos: usize) -> Option<usize> {
    let len = read_u32_raw(data, pos)?;
    let new_pos = pos.checked_add(4)?.checked_add(len)?;
    if new_pos > data.len() {
        None
    } else {
        Some(new_pos)
    }
}

/// Variant of read_entry using cached timestamps to avoid per-entry syscalls.
///
/// Earlier revisions threaded a `shared_buf: &Bytes` through this path for
/// zero-copy slicing via `read_bytes_zero_copy`, but that helper was never
/// wired up — `read_bytes` currently always heap-allocates. The parameter
/// and the caller-side `Bytes::copy_from_slice(data)` that fed it have been
/// removed; restoring true zero-copy should add it back as part of a single
/// landed change, not as vestigial plumbing.
fn read_entry_zero_copy(
    cursor: &mut Cursor<&[u8]>,
    type_tag: u8,
    cached_secs: u32,
    has_hash_ttl_trailer: bool,
) -> Result<(Bytes, Entry), MoonError> {
    let key = read_bytes(cursor)?;

    let mut ttl_buf = [0u8; 8];
    cursor.read_exact(&mut ttl_buf)?;
    let ttl_ms = i64::from_le_bytes(ttl_buf);
    let expires_at_ms = if ttl_ms > 0 { ttl_ms as u64 } else { 0 };

    let value = match type_tag {
        TYPE_STRING => {
            // Fast path: build CompactValue directly from Vec, skipping RedisValue intermediate.
            // This avoids: Vec → Bytes → RedisValue::String → from_redis_value → heap_string_vec
            // and instead does: Vec → CompactValue directly (one Box alloc, zero copy).
            let vec = read_bytes_vec(cursor)?;
            let cv = if vec.len() <= 12 {
                crate::storage::compact_value::CompactValue::from_redis_value(RedisValue::String(
                    Bytes::from(vec),
                ))
            } else {
                crate::storage::compact_value::CompactValue::heap_string_vec_direct(vec)
            };
            let mut entry = Entry::new_string(Bytes::new());
            entry.value = cv;
            if expires_at_ms > 0 {
                entry.set_expires_at_ms(expires_at_ms);
            }
            entry.set_last_access(cached_secs);
            entry.set_access_counter(5);
            return Ok((key, entry));
        }
        TYPE_HASH => {
            let count = read_u32(cursor)? as usize;
            validate_count(cursor, count, 8, "hash")?;
            let mut map = HashMap::with_capacity(count);
            for _ in 0..count {
                let field = read_bytes(cursor)?;
                let val = read_bytes(cursor)?;
                map.insert(field, val);
            }
            // v2 RDB trailer: per-field TTL sidecar (ttl_count u32 + pairs).
            // v1 files have no trailer — treat as plain Hash (0 TTLs).
            if has_hash_ttl_trailer {
                let ttl_count = read_u32(cursor)? as usize;
                validate_count(cursor, ttl_count, 12, "hash_ttls")?;
                if ttl_count > 0 {
                    let mut ttls = HashMap::with_capacity(ttl_count);
                    for _ in 0..ttl_count {
                        let field = read_bytes(cursor)?;
                        let mut ttl_buf = [0u8; 8];
                        cursor.read_exact(&mut ttl_buf)?;
                        let ttl_ms = u64::from_le_bytes(ttl_buf);
                        ttls.insert(field, ttl_ms);
                    }
                    // min_expiry_ms is purely in-memory; recompute from the
                    // decoded ttls map (not stored in the RDB file).
                    let min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                    RedisValue::HashWithTtl {
                        fields: map,
                        ttls,
                        min_expiry_ms,
                    }
                } else {
                    RedisValue::Hash(map)
                }
            } else {
                RedisValue::Hash(map)
            }
        }
        TYPE_LIST => {
            let count = read_u32(cursor)? as usize;
            validate_count(cursor, count, 4, "list")?;
            let mut list = VecDeque::with_capacity(count);
            for _ in 0..count {
                list.push_back(read_bytes(cursor)?);
            }
            RedisValue::List(list)
        }
        TYPE_SET => {
            let count = read_u32(cursor)? as usize;
            validate_count(cursor, count, 4, "set")?;
            let mut set = HashSet::with_capacity(count);
            for _ in 0..count {
                set.insert(read_bytes(cursor)?);
            }
            RedisValue::Set(set)
        }
        TYPE_SORTED_SET => {
            let count = read_u32(cursor)? as usize;
            validate_count(cursor, count, 12, "sorted_set")?;
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
            // Stream parsing: reuse read_bytes (not zero-copy for this rare type)
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
            validate_count(cursor, entry_count, 20, "stream_entries")?;
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
                validate_count(cursor, field_count, 8, "stream_fields")?;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    fields.push((read_bytes(cursor)?, read_bytes(cursor)?));
                }
                stream.entries.insert(id, fields);
                stream.length += 1;
            }
            let group_count = read_u32(cursor)? as usize;
            // min 4 (name len) + 16 (last_delivered_id) + 4 (pel_count) + 4 (consumer_count)
            validate_count(cursor, group_count, 28, "stream_groups")?;
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
                // min 16 (StreamId) + 4 (consumer name len) + 16 (delivery_time+delivery_count)
                validate_count(cursor, pel_count, 36, "stream_pel")?;
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
                    pel.insert(
                        pid,
                        crate::storage::stream::PendingEntry {
                            consumer: consumer_name,
                            delivery_time: u64::from_le_bytes(dt_buf),
                            delivery_count: u64::from_le_bytes(dc_buf),
                        },
                    );
                }
                let consumer_count = read_u32(cursor)? as usize;
                // min 4 (name len) + 8 (seen_time) + 4 (pending_count)
                validate_count(cursor, consumer_count, 16, "stream_consumers")?;
                let mut consumers = HashMap::new();
                for _ in 0..consumer_count {
                    let cname = read_bytes(cursor)?;
                    let mut st_buf = [0u8; 8];
                    cursor.read_exact(&mut st_buf)?;
                    let seen_time = u64::from_le_bytes(st_buf);
                    let pending_count = read_u32(cursor)? as usize;
                    // min 16 (StreamId)
                    validate_count(cursor, pending_count, 16, "stream_pending")?;
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
        _ => return Err(RdbError::UnsupportedType { type_tag }.into()),
    };

    let mut entry = Entry::new_string(Bytes::new());
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(value);
    if expires_at_ms > 0 {
        entry.set_expires_at_ms(expires_at_ms);
    }
    entry.set_last_access(cached_secs);
    entry.set_access_counter(5);

    Ok((key, entry))
}

/// Load an RDB snapshot from a byte slice (for AOF RDB-preamble format).
///
/// Returns `(keys_loaded, bytes_consumed)`. The caller can use `bytes_consumed`
/// to find the start of any RESP commands appended after the RDB preamble.
pub fn load_from_bytes(
    databases: &mut [Database],
    data: &[u8],
) -> Result<(usize, usize), MoonError> {
    if data.len() < RDB_MAGIC.len() + 1 + 1 + 4 {
        return Err(RdbError::Corrupted {
            detail: "RDB preamble too small".into(),
        }
        .into());
    }

    // Find EOF_MARKER to determine RDB section length.
    // The RDB section is: header + entries + EOF_MARKER(1) + CRC32(4).
    // We scan for EOF_MARKER (0xFF) — the first one after the header that's
    // immediately followed by a valid CRC32 of the preceding bytes.
    //
    // Single-pass: maintain a running CRC hasher updated byte-by-byte.
    // When we hit a candidate EOF_MARKER at position i (i >= 5), clone
    // the hasher (which includes data[0..i]), finalize with the EOF byte,
    // and compare against the stored CRC at data[i+1..i+5]. This avoids
    // re-hashing the entire prefix for each candidate (O(n) vs O(n²)).
    let mut rdb_end = None;
    let mut running_hasher = Hasher::new();
    // Feed bytes 0..5 (header) into the running hasher
    if data.len() > 5 {
        running_hasher.update(&data[..5]);
    }
    for i in 5..data.len().saturating_sub(4) {
        if data[i] == EOF_MARKER {
            // Clone running hasher (covers data[0..i]), then finalize with EOF byte
            let mut candidate = running_hasher.clone();
            candidate.update(&[EOF_MARKER]);
            if let Some(checksum_bytes) = data.get(i + 1..i + 5) {
                let stored = u32::from_le_bytes([
                    checksum_bytes[0],
                    checksum_bytes[1],
                    checksum_bytes[2],
                    checksum_bytes[3],
                ]);
                if candidate.finalize() == stored {
                    rdb_end = Some(i + 5); // past CRC32
                    break;
                }
            }
        }
        // Feed this byte into the running hasher for the next iteration
        running_hasher.update(&data[i..i + 1]);
    }

    let rdb_len = rdb_end.ok_or_else(|| {
        MoonError::from(RdbError::Corrupted {
            detail: "RDB preamble: no valid EOF+CRC found".into(),
        })
    })?;

    // Load using the same logic as `load`, but from the byte slice
    let payload = &data[..rdb_len - 4]; // exclude CRC32
    let mut cursor = Cursor::new(payload);

    // Skip magic + version
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic).map_err(|e| RdbError::Io {
        path: std::path::PathBuf::from("<aof-preamble>"),
        source: e,
    })?;
    if &magic != RDB_MAGIC {
        return Err(RdbError::Corrupted {
            detail: "invalid RDB magic in AOF preamble".into(),
        }
        .into());
    }
    let mut version = [0u8; 1];
    cursor.read_exact(&mut version).map_err(|e| RdbError::Io {
        path: std::path::PathBuf::from("<aof-preamble>"),
        source: e,
    })?;
    let file_version = version[0];
    if file_version != RDB_VERSION && file_version != RDB_VERSION_V1 {
        return Err(RdbError::UnsupportedVersion {
            version: file_version as u32,
        }
        .into());
    }
    let has_hash_ttl_trailer = file_version >= RDB_VERSION;

    let now_ms = current_time_ms();
    let now_secs = (now_ms / 1000) as u32;
    let mut total_keys = 0usize;
    let mut current_db: usize = 0;

    // Load into temporary databases so that:
    // (a) If the load fails partway, original state is untouched.
    // (b) Old keys not present in the RDB snapshot don't survive — an RDB
    //     preamble is a full point-in-time snapshot and must replace state,
    //     not merge into it.
    let db_count = databases.len();
    let mut temp_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();

    // Pre-size DashTables on the temporary databases
    let entry_counts = count_entries_per_db(&cursor, db_count, has_hash_ttl_trailer);
    for (db_idx, &count) in entry_counts.iter().enumerate() {
        if count > 0 && db_idx < db_count {
            temp_dbs[db_idx].reserve(count);
        }
    }

    loop {
        let mut tag = [0u8; 1];
        if cursor.read_exact(&mut tag).is_err() {
            break;
        }
        match tag[0] {
            EOF_MARKER => break,
            DB_SELECTOR => {
                let mut db_idx = [0u8; 1];
                cursor.read_exact(&mut db_idx).map_err(|e| RdbError::Io {
                    path: std::path::PathBuf::from("<aof-preamble>"),
                    source: e,
                })?;
                current_db = db_idx[0] as usize;
                if current_db >= db_count {
                    return Err(RdbError::Corrupted {
                        detail: format!(
                            "RDB preamble references database {} but only {} configured",
                            current_db, db_count
                        ),
                    }
                    .into());
                }
            }
            type_tag => {
                match read_entry_zero_copy(&mut cursor, type_tag, now_secs, has_hash_ttl_trailer) {
                    Ok((key, entry)) => {
                        if entry.has_expiry() && entry.is_expired_at(now_ms) {
                            continue;
                        }
                        if current_db < db_count {
                            temp_dbs[current_db].insert_for_load(key, entry);
                            total_keys += 1;
                        }
                    }
                    Err(e) => {
                        return Err(RdbError::Corrupted {
                        detail: format!(
                            "RDB preamble: corrupted entry at offset {}: {}. {} keys loaded before failure.",
                            cursor.position(),
                            e,
                            total_keys
                        ),
                    }
                    .into());
                    }
                }
            }
        }
    }

    // Recalculate memory on temp databases, then swap into the live ones.
    // Only reached if all entries parsed successfully — no partial state.
    for (live, mut temp) in databases.iter_mut().zip(temp_dbs.into_iter()) {
        temp.recalculate_memory();
        *live = temp;
    }

    Ok((total_keys, rdb_len))
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
            let target_shard = key_to_shard(key.as_bytes(), num_shards);
            if target_shard < shard_dbs.len() && db_idx < shard_dbs[target_shard].len() {
                shard_dbs[target_shard][db_idx].set(key.as_ref(), entry.clone());
            }
        }
    }
}

pub(crate) fn write_entry(buf: &mut Vec<u8>, key: &[u8], entry: &Entry) -> Result<(), MoonError> {
    let val_ref = entry.value.as_redis_value();

    // Type tag -- compact variants serialize as the same type as their full-size counterparts
    let type_tag = match value_codec::value_type_of(&val_ref) {
        ValueType::String => TYPE_STRING,
        ValueType::Hash => TYPE_HASH,
        ValueType::List => TYPE_LIST,
        ValueType::Set => TYPE_SET,
        ValueType::ZSet => TYPE_SORTED_SET,
        ValueType::Stream => TYPE_STREAM,
    };
    buf.write_all(&[type_tag])?;

    // Key
    write_bytes(buf, key)?;

    // TTL as unix millis (0 = no expiry)
    let ttl_ms: i64 = if entry.has_expiry() {
        entry.expires_at_ms() as i64
    } else {
        0
    };
    buf.write_all(&ttl_ms.to_le_bytes())?;

    // Value data -- strings inline, collections via the shared value codec
    // (compact variants expand to the canonical element-level format).
    match val_ref {
        RedisValueRef::String(s) => {
            write_bytes(buf, s)?;
        }
        ref other => {
            value_codec::encode_value_body(other, buf).map_err(|e| RdbError::Corrupted {
                detail: format!(
                    "encoding value for key {}: {e}",
                    String::from_utf8_lossy(key)
                ),
            })?;
        }
    }

    Ok(())
}

pub(crate) fn read_entry(
    cursor: &mut Cursor<&[u8]>,
    type_tag: u8,
    has_hash_ttl_trailer: bool,
) -> Result<(Bytes, Entry), MoonError> {
    // Key
    let key = read_bytes(cursor)?;

    // TTL
    let mut ttl_buf = [0u8; 8];
    cursor.read_exact(&mut ttl_buf)?;
    let ttl_ms = i64::from_le_bytes(ttl_buf);

    // expires_at_ms: if ttl_ms > 0 it's already absolute unix millis
    let expires_at_ms = if ttl_ms > 0 { ttl_ms as u64 } else { 0 };

    // Value -- strings inline, collections via the shared value codec.
    let value = match type_tag {
        TYPE_STRING => {
            let data = read_bytes(cursor)?;
            RedisValue::String(data)
        }
        _ => {
            let value_type = match type_tag {
                TYPE_HASH => ValueType::Hash,
                TYPE_LIST => ValueType::List,
                TYPE_SET => ValueType::Set,
                TYPE_SORTED_SET => ValueType::ZSet,
                TYPE_STREAM => ValueType::Stream,
                _ => return Err(RdbError::UnsupportedType { type_tag }.into()),
            };
            // v1 files predate the hash-TTL trailer; v2 requires it.
            let trailer = if has_hash_ttl_trailer {
                HashTtlTrailer::Required
            } else {
                HashTtlTrailer::Absent
            };
            value_codec::decode_value_body(cursor, value_type, trailer).map_err(|e| {
                MoonError::from(RdbError::Corrupted {
                    detail: e.to_string(),
                })
            })?
        }
    };
    let mut entry = if expires_at_ms > 0 {
        Entry::new_string(Bytes::new()) // placeholder, we'll replace value below
    } else {
        Entry::new_string(Bytes::new())
    };
    // Replace value with the correct one via CompactValue
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(value);
    if expires_at_ms > 0 {
        entry.set_expires_at_ms(expires_at_ms);
    }
    entry.set_last_access(current_secs());
    entry.set_access_counter(5);

    Ok((key, entry))
}

pub(crate) fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) -> Result<(), MoonError> {
    buf.write_all(&(data.len() as u32).to_le_bytes())?;
    buf.write_all(data)?;
    Ok(())
}

/// Validate a collection count against remaining cursor data before allocating.
/// `min_bytes_per_item` is the minimum bytes each item will read from the cursor.
pub(crate) fn validate_count(
    cursor: &Cursor<&[u8]>,
    count: usize,
    min_bytes_per_item: usize,
    kind: &str,
) -> Result<(), MoonError> {
    let remaining = cursor
        .get_ref()
        .len()
        .saturating_sub(cursor.position() as usize);
    if min_bytes_per_item > 0 && count > remaining / min_bytes_per_item {
        return Err(RdbError::Corrupted {
            detail: format!(
                "{} count {} exceeds remaining data ({} bytes)",
                kind, count, remaining
            ),
        }
        .into());
    }
    Ok(())
}

pub(crate) fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Bytes, MoonError> {
    let len = read_u32(cursor)? as usize;
    let pos = cursor.position() as usize;
    let remaining = cursor.get_ref().len() - pos;
    if len > remaining {
        return Err(RdbError::Corrupted {
            detail: format!(
                "read_bytes: length field {} exceeds remaining data {}",
                len, remaining
            ),
        }
        .into());
    }
    let slice = &cursor.get_ref()[pos..pos + len];
    cursor.set_position((pos + len) as u64);
    Ok(Bytes::copy_from_slice(slice))
}

/// Read bytes as owned Vec<u8> — avoids Bytes intermediate for RDB load path.
/// Single allocation directly to the right size, no refcount overhead.
pub(crate) fn read_bytes_vec(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, MoonError> {
    let len = read_u32(cursor)? as usize;
    let pos = cursor.position() as usize;
    let remaining = cursor.get_ref().len() - pos;
    if len > remaining {
        return Err(RdbError::Corrupted {
            detail: format!(
                "read_bytes_vec: length {} exceeds remaining {}",
                len, remaining
            ),
        }
        .into());
    }
    let slice = &cursor.get_ref()[pos..pos + len];
    cursor.set_position((pos + len) as u64);
    Ok(slice.to_vec())
}

pub(crate) fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, MoonError> {
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
        dbs[0].set_string(b"hello", Bytes::from_static(b"world"));

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
        dbs[0].set_string_with_expiry(b"key", Bytes::from_static(b"val"), future_ms);

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"key").unwrap();
        assert!(entry.has_expiry());
        // W3: expiry round-trips with full millisecond fidelity — the old
        // seconds-truncated storage needed a 5s tolerance here.
        assert_eq!(
            entry.expires_at_ms(),
            future_ms,
            "RDB round-trip must preserve the exact millisecond expiry"
        );
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
        dbs[0].set_string(b"str", Bytes::from_static(b"val"));
        // String with TTL
        let future_ms = current_time_ms() + 600_000;
        dbs[0].set_string_with_expiry(b"str_ttl", Bytes::from_static(b"expiring"), future_ms);
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
        dbs[0].set_string(b"live", Bytes::from_static(b"yes"));
        // Expired key
        let past_ms = current_time_ms() - 1000;
        dbs[0].set(
            b"dead",
            Entry::new_string_with_expiry(Bytes::from_static(b"no"), past_ms),
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
        dbs[0].set_string(b"k", Bytes::from_static(b"v"));

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
        dbs[0].set_string(b"k0", Bytes::from_static(b"v0"));
        // DB 1 is empty -- should be skipped
        // DB 2
        dbs[2].set_string(b"k2", Bytes::from_static(b"v2"));

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new(), Database::new(), Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 2);

        assert!(loaded[0].get(b"k0").is_some());
        assert_eq!(loaded[1].len(), 0); // DB 1 should be empty
        assert!(loaded[2].get(b"k2").is_some());
    }

    /// Task #49: `save` (and `save_from_snapshot`) must go through
    /// `atomic_write_durable` instead of a bare tmp-write + rename.
    /// Regression pin: no leftover `.rdb.tmp` after a successful save.
    #[test]
    fn test_save_leaves_no_leftover_temp_file() {
        let (dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(b"k", Bytes::from_static(b"v"));

        save(&dbs, &path).unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("dump.rdb")]);
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

    /// Build the byte body consumed by `read_entry` / `read_entry_zero_copy`
    /// for a single entry: `[key_len(4)+key][ttl_ms i64(8, 0 = no ttl)][value_body]`.
    /// Both functions expect the cursor positioned at the key field, with
    /// the type tag passed separately — this mirrors that contract exactly.
    fn build_entry_bytes(key: &[u8], value_body: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&0i64.to_le_bytes()); // ttl_ms = 0 (no expiry)
        buf.extend_from_slice(value_body);
        buf
    }

    /// Assert that a crafted `TYPE_STREAM` value body — a lying count paired
    /// with insufficient trailing data to back it — is rejected by BOTH
    /// `read_entry` (used by `load_from_bytes`, the AOF-preamble path) and
    /// `read_entry_zero_copy` (used by `load`, the boot-time RDB path).
    /// They are literal duplicates for this branch, so both must reject.
    fn assert_stream_value_rejected(value_body: &[u8]) {
        let entry_bytes = build_entry_bytes(b"k", value_body);

        let mut cursor = Cursor::new(&entry_bytes[..]);
        assert!(
            read_entry(&mut cursor, TYPE_STREAM, false).is_err(),
            "read_entry accepted a stream count exceeding remaining data \
             (untrusted-length DoS gap)"
        );

        let mut cursor2 = Cursor::new(&entry_bytes[..]);
        assert!(
            read_entry_zero_copy(&mut cursor2, TYPE_STREAM, 0, false).is_err(),
            "read_entry_zero_copy accepted a stream count exceeding remaining data \
             (untrusted-length DoS gap)"
        );
    }

    #[test]
    fn test_stream_group_count_dos_rejected() {
        // entry_count=0, last_id=(0,0), then group_count lies about ~4B groups
        // with zero trailing bytes to back even one.
        let mut body = Vec::new();
        body.extend_from_slice(&0u64.to_le_bytes()); // entry_count
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.seq
        body.extend_from_slice(&u32::MAX.to_le_bytes()); // group_count: lies
        assert_stream_value_rejected(&body);
    }

    #[test]
    fn test_stream_pel_count_dos_rejected() {
        // One real (minimal) group, then pel_count lies with no data behind it.
        let mut body = Vec::new();
        body.extend_from_slice(&0u64.to_le_bytes()); // entry_count
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.seq
        body.extend_from_slice(&1u32.to_le_bytes()); // group_count = 1
        body.extend_from_slice(&0u32.to_le_bytes()); // group_name len = 0
        body.extend_from_slice(&0u64.to_le_bytes()); // last_delivered_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_delivered_id.seq
        body.extend_from_slice(&u32::MAX.to_le_bytes()); // pel_count: lies
        assert_stream_value_rejected(&body);
    }

    #[test]
    fn test_stream_consumer_count_dos_rejected() {
        // One real group with an empty (real) PEL, then consumer_count lies.
        let mut body = Vec::new();
        body.extend_from_slice(&0u64.to_le_bytes()); // entry_count
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.seq
        body.extend_from_slice(&1u32.to_le_bytes()); // group_count = 1
        body.extend_from_slice(&0u32.to_le_bytes()); // group_name len = 0
        body.extend_from_slice(&0u64.to_le_bytes()); // last_delivered_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_delivered_id.seq
        body.extend_from_slice(&0u32.to_le_bytes()); // pel_count = 0 (real)
        body.extend_from_slice(&u32::MAX.to_le_bytes()); // consumer_count: lies
        assert_stream_value_rejected(&body);
    }

    #[test]
    fn test_stream_pending_count_dos_rejected() {
        // One real group + one real (empty-pending) consumer, then
        // pending_count lies with no data behind it.
        let mut body = Vec::new();
        body.extend_from_slice(&0u64.to_le_bytes()); // entry_count
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.seq
        body.extend_from_slice(&1u32.to_le_bytes()); // group_count = 1
        body.extend_from_slice(&0u32.to_le_bytes()); // group_name len = 0
        body.extend_from_slice(&0u64.to_le_bytes()); // last_delivered_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_delivered_id.seq
        body.extend_from_slice(&0u32.to_le_bytes()); // pel_count = 0 (real)
        body.extend_from_slice(&1u32.to_le_bytes()); // consumer_count = 1 (real)
        body.extend_from_slice(&0u32.to_le_bytes()); // consumer name len = 0
        body.extend_from_slice(&0u64.to_le_bytes()); // seen_time
        body.extend_from_slice(&u32::MAX.to_le_bytes()); // pending_count: lies
        assert_stream_value_rejected(&body);
    }

    /// Control test: a small but fully-populated consumer group (1 group,
    /// 1 PEL entry, 1 consumer, 1 pending id) must NOT be rejected by the
    /// new validate_count guards — confirms the chosen min-bytes-per-item
    /// bounds are the true structural minimum and never reject a valid file.
    #[test]
    fn test_stream_consumer_group_round_trip_not_rejected() {
        let mut body = Vec::new();
        body.extend_from_slice(&0u64.to_le_bytes()); // entry_count = 0
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_id.seq
        body.extend_from_slice(&1u32.to_le_bytes()); // group_count = 1
        body.extend_from_slice(&1u32.to_le_bytes()); // group_name len = 1
        body.extend_from_slice(b"g");
        body.extend_from_slice(&5u64.to_le_bytes()); // last_delivered_id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // last_delivered_id.seq
        body.extend_from_slice(&1u32.to_le_bytes()); // pel_count = 1
        body.extend_from_slice(&5u64.to_le_bytes()); // pel[0].id.ms
        body.extend_from_slice(&0u64.to_le_bytes()); // pel[0].id.seq
        body.extend_from_slice(&1u32.to_le_bytes()); // pel[0].consumer name len = 1
        body.extend_from_slice(b"c");
        body.extend_from_slice(&1u64.to_le_bytes()); // delivery_time
        body.extend_from_slice(&1u64.to_le_bytes()); // delivery_count
        body.extend_from_slice(&1u32.to_le_bytes()); // consumer_count = 1
        body.extend_from_slice(&1u32.to_le_bytes()); // consumer name len = 1
        body.extend_from_slice(b"c");
        body.extend_from_slice(&1u64.to_le_bytes()); // seen_time
        body.extend_from_slice(&1u32.to_le_bytes()); // pending_count = 1
        body.extend_from_slice(&5u64.to_le_bytes()); // pending[0].ms
        body.extend_from_slice(&0u64.to_le_bytes()); // pending[0].seq

        let entry_bytes = build_entry_bytes(b"k", &body);

        let mut cursor = Cursor::new(&entry_bytes[..]);
        let (_key, entry) = read_entry(&mut cursor, TYPE_STREAM, false)
            .expect("legit 1-group/1-consumer/1-pel/1-pending stream must not be rejected");
        match entry.value.as_redis_value() {
            RedisValueRef::Stream(stream) => {
                assert_eq!(stream.groups.len(), 1);
                let group = stream.groups.get(&Bytes::from_static(b"g")).unwrap();
                assert_eq!(group.pel.len(), 1);
                assert_eq!(group.consumers.len(), 1);
                assert_eq!(
                    group
                        .consumers
                        .get(&Bytes::from_static(b"c"))
                        .unwrap()
                        .pending
                        .len(),
                    1
                );
            }
            _ => panic!("Expected Stream"),
        }

        // Same body must also be accepted by the zero-copy variant.
        let mut cursor2 = Cursor::new(&entry_bytes[..]);
        assert!(read_entry_zero_copy(&mut cursor2, TYPE_STREAM, 0, false).is_ok());
    }
}
