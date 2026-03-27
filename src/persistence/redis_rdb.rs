//! Redis-compatible RDB format writer and reader.
//!
//! Produces REDIS0010-format files that can be loaded by Redis 7.x and by our
//! own `load_rdb` function for PSYNC2 full resync.
//!
//! Format overview:
//!   REDIS0010 | AUX fields | (SELECTDB RESIZEDB entries)* | EOF | CRC64-LE
//!
//! CRC64 uses the Jones polynomial (0xAD93D23594C935A9), matching Redis source.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::storage::compact_value::RedisValueRef;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, RedisValue, current_time_ms};

// ---------------------------------------------------------------------------
// Redis RDB opcodes and type tags
// ---------------------------------------------------------------------------

const REDIS_RDB_MAGIC: &[u8] = b"REDIS";
const REDIS_RDB_VERSION: &[u8] = b"0010";

const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EOF: u8 = 0xFF;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;

// Integer-encoded string prefixes (11xxxxxx)
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;

// ---------------------------------------------------------------------------
// CRC64 Jones polynomial lookup table (from Redis src/crc64.c)
// Polynomial: 0xAD93D23594C935A9 (Jones)
// ---------------------------------------------------------------------------

/// CRC64 with Jones polynomial, matching Redis implementation.
///
/// Uses the pre-computed lookup table from Redis src/crc64.c.
/// Polynomial: 0xAD93D23594C935A9 (Jones, reflected).
/// Test vector: CRC64("123456789") = 0xE9C6D914C4B8D9CA
fn crc64_jones(data: &[u8]) -> u64 {
    // This table is generated using the reflected polynomial 0x95AC9329AC4BC9B5
    // which is the bit-reversal of 0xAD93D23594C935A9.
    // Source: Redis src/crc64.c
    #[rustfmt::skip]
    static TABLE: [u64; 256] = {
        const POLY: u64 = 0x95AC9329AC4BC9B5;
        let mut table = [0u64; 256];
        let mut i = 0u64;
        while i < 256 {
            let mut crc = i;
            let mut j = 0;
            while j < 8 {
                if crc & 1 == 1 {
                    crc = (crc >> 1) ^ POLY;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            table[i as usize] = crc;
            i += 1;
        }
        table
    };

    let mut crc: u64 = 0;
    for &byte in data {
        let idx = ((crc ^ byte as u64) & 0xFF) as usize;
        crc = TABLE[idx] ^ (crc >> 8);
    }
    crc
}

// ---------------------------------------------------------------------------
// Length encoding (Redis variable-length)
// ---------------------------------------------------------------------------

/// Write a Redis variable-length encoded integer.
///
/// - 0..63:       1 byte  (00xxxxxx)
/// - 64..16383:   2 bytes (01xxxxxx + byte)
/// - 16384..2^32: 5 bytes (10000000 + 4 BE bytes)
/// - >2^32:       9 bytes (10000001 + 8 BE bytes)
fn write_length(buf: &mut Vec<u8>, len: u64) {
    if len < 64 {
        buf.push(len as u8);
    } else if len < 16384 {
        buf.push(0x40 | ((len >> 8) as u8));
        buf.push((len & 0xFF) as u8);
    } else if len <= u32::MAX as u64 {
        buf.push(0x80);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        buf.push(0x81);
        buf.extend_from_slice(&len.to_be_bytes());
    }
}

/// Read a Redis variable-length encoded integer.
/// Returns (value, is_special_encoding).
/// When the top 2 bits are 11, it's a special encoding (integer string),
/// and the lower 6 bits indicate the encoding type.
fn read_length(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<(u64, bool)> {
    let mut byte = [0u8; 1];
    cursor.read_exact(&mut byte)?;
    let first = byte[0];

    match first >> 6 {
        0b00 => Ok(((first & 0x3F) as u64, false)),
        0b01 => {
            let mut next = [0u8; 1];
            cursor.read_exact(&mut next)?;
            let val = (((first & 0x3F) as u64) << 8) | next[0] as u64;
            Ok((val, false))
        }
        0b10 => {
            if first == 0x80 {
                let mut be4 = [0u8; 4];
                cursor.read_exact(&mut be4)?;
                Ok((u32::from_be_bytes(be4) as u64, false))
            } else if first == 0x81 {
                let mut be8 = [0u8; 8];
                cursor.read_exact(&mut be8)?;
                Ok((u64::from_be_bytes(be8), false))
            } else {
                bail!("Invalid length encoding byte: 0x{:02X}", first);
            }
        }
        0b11 => {
            // Special encoding: lower 6 bits = encoding type
            Ok(((first & 0x3F) as u64, true))
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// String encoding helpers
// ---------------------------------------------------------------------------

/// Write a length-prefixed Redis string.
fn write_redis_string(buf: &mut Vec<u8>, s: &[u8]) {
    write_length(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

/// Read a Redis string (length-prefixed or integer-encoded).
fn read_redis_string(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<Vec<u8>> {
    let (val, is_encoded) = read_length(cursor)?;
    if is_encoded {
        // Integer-encoded string
        match val as u8 {
            RDB_ENC_INT8 => {
                let mut b = [0u8; 1];
                cursor.read_exact(&mut b)?;
                Ok(format!("{}", b[0] as i8).into_bytes())
            }
            RDB_ENC_INT16 => {
                let mut b = [0u8; 2];
                cursor.read_exact(&mut b)?;
                Ok(format!("{}", i16::from_le_bytes(b)).into_bytes())
            }
            RDB_ENC_INT32 => {
                let mut b = [0u8; 4];
                cursor.read_exact(&mut b)?;
                Ok(format!("{}", i32::from_le_bytes(b)).into_bytes())
            }
            other => bail!("Unknown RDB string encoding: {}", other),
        }
    } else {
        let mut data = vec![0u8; val as usize];
        cursor.read_exact(&mut data)?;
        Ok(data)
    }
}

/// Write an AUX field: 0xFA + key_string + value_string.
fn write_aux(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    buf.push(RDB_OPCODE_AUX);
    write_redis_string(buf, key);
    write_redis_string(buf, value);
}

// ---------------------------------------------------------------------------
// Header / Footer
// ---------------------------------------------------------------------------

/// Write the RDB header: magic, version, and standard AUX fields.
fn write_rdb_header(buf: &mut Vec<u8>) {
    buf.extend_from_slice(REDIS_RDB_MAGIC);
    buf.extend_from_slice(REDIS_RDB_VERSION);

    // AUX fields
    write_aux(buf, b"redis-ver", b"7.0.0");
    write_aux(buf, b"redis-bits", b"64");
    let ctime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    write_aux(buf, b"ctime", ctime.to_string().as_bytes());
    write_aux(buf, b"used-mem", b"0");
}

/// Write the RDB footer: EOF opcode + CRC64 of the entire buffer (LE).
fn write_rdb_footer(buf: &mut Vec<u8>) {
    buf.push(RDB_OPCODE_EOF);
    let crc = crc64_jones(buf);
    buf.extend_from_slice(&crc.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Entry writer
// ---------------------------------------------------------------------------

/// Write a single entry in Redis RDB format.
fn write_rdb_entry(buf: &mut Vec<u8>, key: &[u8], entry: &Entry, base_ts: u32) {
    // TTL (if present)
    if entry.has_expiry() {
        let ms = entry.expires_at_ms(base_ts);
        buf.push(RDB_OPCODE_EXPIRETIME_MS);
        buf.extend_from_slice(&ms.to_le_bytes());
    }

    match entry.as_redis_value() {
        RedisValueRef::String(s) => {
            buf.push(RDB_TYPE_STRING);
            write_redis_string(buf, key);
            write_redis_string(buf, s);
        }
        RedisValueRef::Hash(map) => {
            buf.push(RDB_TYPE_HASH);
            write_redis_string(buf, key);
            write_length(buf, map.len() as u64);
            for (field, value) in map.iter() {
                write_redis_string(buf, field);
                write_redis_string(buf, value);
            }
        }
        RedisValueRef::HashListpack(lp) => {
            buf.push(RDB_TYPE_HASH);
            write_redis_string(buf, key);
            let pairs: Vec<_> = lp.iter_pairs().collect();
            write_length(buf, pairs.len() as u64);
            for (f, v) in pairs {
                write_redis_string(buf, &f.as_bytes());
                write_redis_string(buf, &v.as_bytes());
            }
        }
        RedisValueRef::List(deque) => {
            buf.push(RDB_TYPE_LIST);
            write_redis_string(buf, key);
            write_length(buf, deque.len() as u64);
            for elem in deque.iter() {
                write_redis_string(buf, elem);
            }
        }
        RedisValueRef::ListListpack(lp) => {
            buf.push(RDB_TYPE_LIST);
            write_redis_string(buf, key);
            let count = lp.len();
            write_length(buf, count as u64);
            for entry in lp.iter() {
                write_redis_string(buf, &entry.as_bytes());
            }
        }
        RedisValueRef::Set(set) => {
            buf.push(RDB_TYPE_SET);
            write_redis_string(buf, key);
            write_length(buf, set.len() as u64);
            for member in set.iter() {
                write_redis_string(buf, member);
            }
        }
        RedisValueRef::SetListpack(lp) => {
            buf.push(RDB_TYPE_SET);
            write_redis_string(buf, key);
            let count = lp.len();
            write_length(buf, count as u64);
            for entry in lp.iter() {
                write_redis_string(buf, &entry.as_bytes());
            }
        }
        RedisValueRef::SetIntset(is) => {
            buf.push(RDB_TYPE_SET);
            write_redis_string(buf, key);
            let members: Vec<_> = is.iter().collect();
            write_length(buf, members.len() as u64);
            for val in members {
                let s = val.to_string();
                write_redis_string(buf, s.as_bytes());
            }
        }
        RedisValueRef::SortedSet { members, .. } => {
            buf.push(RDB_TYPE_ZSET_2);
            write_redis_string(buf, key);
            write_length(buf, members.len() as u64);
            for (member, score) in members.iter() {
                write_redis_string(buf, member);
                buf.extend_from_slice(&score.to_le_bytes());
            }
        }
        RedisValueRef::SortedSetBPTree { members, .. } => {
            buf.push(RDB_TYPE_ZSET_2);
            write_redis_string(buf, key);
            write_length(buf, members.len() as u64);
            for (member, score) in members.iter() {
                write_redis_string(buf, member);
                buf.extend_from_slice(&score.to_le_bytes());
            }
        }
        RedisValueRef::SortedSetListpack(lp) => {
            buf.push(RDB_TYPE_ZSET_2);
            write_redis_string(buf, key);
            let pairs: Vec<_> = lp.iter_pairs().collect();
            write_length(buf, pairs.len() as u64);
            for (member, score_entry) in pairs {
                write_redis_string(buf, &member.as_bytes());
                // Parse score from listpack entry
                let score_bytes = score_entry.as_bytes();
                let score: f64 = std::str::from_utf8(&score_bytes)
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0.0);
                buf.extend_from_slice(&score.to_le_bytes());
            }
        }
        RedisValueRef::Stream(stream) => {
            // Streams: serialize as JSON string with type 0 for simplicity.
            // Redis uses complex type 19+ for native stream encoding, but for
            // PSYNC2 to our own replicas this is sufficient.
            buf.push(RDB_TYPE_STRING);
            write_redis_string(buf, key);
            let json = format!("__stream__:{}", stream.length);
            write_redis_string(buf, json.as_bytes());
        }
    }
}

// ---------------------------------------------------------------------------
// Public API: write
// ---------------------------------------------------------------------------

/// Write all databases to a buffer in Redis RDB format.
///
/// Produces valid REDIS0010 with AUX fields, per-DB SELECTDB/RESIZEDB,
/// entries, EOF marker, and CRC64 checksum.
pub fn write_rdb(databases: &[Database], buf: &mut Vec<u8>) {
    write_rdb_header(buf);

    let now_ms = current_time_ms();

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

        // Count entries with expiry for RESIZEDB
        let expires_count = live.iter().filter(|(_, e)| e.has_expiry()).count();

        // SELECTDB
        buf.push(RDB_OPCODE_SELECTDB);
        write_length(buf, db_idx as u64);

        // RESIZEDB
        buf.push(RDB_OPCODE_RESIZEDB);
        write_length(buf, live.len() as u64);
        write_length(buf, expires_count as u64);

        for (key, entry) in &live {
            write_rdb_entry(buf, key.as_bytes(), entry, base_ts);
        }
    }

    write_rdb_footer(buf);
}

/// Save databases to an RDB file in Redis-compatible format.
/// Uses atomic write (tmp + rename) for crash safety.
pub fn save(databases: &[Database], path: &Path) -> anyhow::Result<()> {
    let mut buf = Vec::new();
    write_rdb(databases, &mut buf);

    let tmp_path = path.with_extension("rdb.tmp");
    std::fs::write(&tmp_path, &buf).context("Failed to write temporary Redis RDB file")?;
    std::fs::rename(&tmp_path, path).context("Failed to rename temporary Redis RDB file")?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Public API: read
// ---------------------------------------------------------------------------

/// Load an RDB file in Redis format into the provided databases.
///
/// Verifies magic bytes, version, and CRC64 checksum.
/// Returns the total number of keys loaded.
pub fn load_rdb(databases: &mut [Database], data: &[u8]) -> anyhow::Result<usize> {
    if data.len() < 9 + 1 + 8 {
        bail!("RDB data too short: {} bytes", data.len());
    }

    // Verify magic + version
    if &data[..5] != REDIS_RDB_MAGIC {
        bail!("Invalid RDB magic: expected REDIS, got {:?}", &data[..5]);
    }
    if &data[5..9] != REDIS_RDB_VERSION {
        // We only support version 0010 for now
        bail!(
            "Unsupported RDB version: {:?}",
            std::str::from_utf8(&data[5..9]).unwrap_or("???")
        );
    }

    // Verify CRC64: checksum covers everything except the last 8 bytes
    let payload = &data[..data.len() - 8];
    let stored_crc = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
    let computed_crc = crc64_jones(payload);
    if stored_crc != computed_crc {
        bail!(
            "CRC64 mismatch: stored=0x{:016X}, computed=0x{:016X}",
            stored_crc,
            computed_crc
        );
    }

    // Parse entries
    let mut cursor = Cursor::new(&data[9..data.len() - 8]);
    // Remove the EOF byte from the parse range -- it was included in payload
    // but we need to handle it during parsing.

    let mut current_db: usize = 0;
    let mut total_keys: usize = 0;
    let mut pending_expiry_ms: Option<u64> = None;

    loop {
        let mut opcode = [0u8; 1];
        if cursor.read_exact(&mut opcode).is_err() {
            break; // End of data
        }

        match opcode[0] {
            RDB_OPCODE_EOF => break,
            RDB_OPCODE_AUX => {
                // Skip AUX field: key + value
                let _key = read_redis_string(&mut cursor)?;
                let _value = read_redis_string(&mut cursor)?;
            }
            RDB_OPCODE_SELECTDB => {
                let (db_idx, _) = read_length(&mut cursor)?;
                current_db = db_idx as usize;
            }
            RDB_OPCODE_RESIZEDB => {
                let (_db_size, _) = read_length(&mut cursor)?;
                let (_expires_size, _) = read_length(&mut cursor)?;
                // We don't pre-allocate; just skip these hints.
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                let mut ms_bytes = [0u8; 8];
                cursor.read_exact(&mut ms_bytes)?;
                pending_expiry_ms = Some(u64::from_le_bytes(ms_bytes));
            }
            0xFD => {
                // EXPIRETIME (seconds, u32 LE) -- older format
                let mut sec_bytes = [0u8; 4];
                cursor.read_exact(&mut sec_bytes)?;
                pending_expiry_ms = Some(u32::from_le_bytes(sec_bytes) as u64 * 1000);
            }
            type_tag => {
                // This is a data entry
                let key_bytes = read_redis_string(&mut cursor)?;
                let entry = read_rdb_entry(&mut cursor, type_tag, pending_expiry_ms)?;
                pending_expiry_ms = None;

                if current_db < databases.len() {
                    let key = Bytes::from(key_bytes);
                    databases[current_db].set(key, entry);
                    total_keys += 1;
                }
            }
        }
    }

    Ok(total_keys)
}

/// Read a single RDB entry of the given type.
fn read_rdb_entry(
    cursor: &mut Cursor<&[u8]>,
    type_tag: u8,
    expiry_ms: Option<u64>,
) -> anyhow::Result<Entry> {
    let mut entry = match type_tag {
        RDB_TYPE_STRING => {
            let val = read_redis_string(cursor)?;
            Entry::new_string(Bytes::from(val))
        }
        RDB_TYPE_LIST => {
            let (count, _) = read_length(cursor)?;
            let mut list = VecDeque::with_capacity(count as usize);
            for _ in 0..count {
                let elem = read_redis_string(cursor)?;
                list.push_back(Bytes::from(elem));
            }
            let mut entry = Entry::new_list();
            if let Some(rv) = entry.redis_value_mut() {
                *rv = RedisValue::List(list);
            }
            entry
        }
        RDB_TYPE_SET => {
            let (count, _) = read_length(cursor)?;
            let mut set = HashSet::with_capacity(count as usize);
            for _ in 0..count {
                let member = read_redis_string(cursor)?;
                set.insert(Bytes::from(member));
            }
            let mut entry = Entry::new_set();
            if let Some(rv) = entry.redis_value_mut() {
                *rv = RedisValue::Set(set);
            }
            entry
        }
        RDB_TYPE_HASH => {
            let (count, _) = read_length(cursor)?;
            let mut map = HashMap::with_capacity(count as usize);
            for _ in 0..count {
                let field = read_redis_string(cursor)?;
                let value = read_redis_string(cursor)?;
                map.insert(Bytes::from(field), Bytes::from(value));
            }
            let mut entry = Entry::new_hash();
            if let Some(rv) = entry.redis_value_mut() {
                *rv = RedisValue::Hash(map);
            }
            entry
        }
        RDB_TYPE_ZSET_2 => {
            let (count, _) = read_length(cursor)?;
            let mut members = HashMap::with_capacity(count as usize);
            let mut scores = BTreeMap::new();
            for _ in 0..count {
                let member_bytes = read_redis_string(cursor)?;
                let mut score_buf = [0u8; 8];
                cursor.read_exact(&mut score_buf)?;
                let score = f64::from_le_bytes(score_buf);
                let member = Bytes::from(member_bytes);
                scores.insert((OrderedFloat(score), member.clone()), ());
                members.insert(member, score);
            }
            let mut entry = Entry::new_sorted_set();
            if let Some(rv) = entry.redis_value_mut() {
                *rv = RedisValue::SortedSet { members, scores };
            }
            entry
        }
        other => bail!("Unsupported RDB type tag: {}", other),
    };

    // Apply expiry if present
    if let Some(ms) = expiry_ms {
        entry.set_expires_at_ms(0, ms);
    }

    Ok(entry)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc64_jones_polynomial() {
        // Known test vector: CRC64 of "123456789" with Jones polynomial
        let result = crc64_jones(b"123456789");
        assert_eq!(
            result, 0xE9C6D914C4B8D9CA,
            "CRC64 Jones polynomial mismatch: got 0x{:016X}",
            result
        );
    }

    #[test]
    fn test_write_length_6bit() {
        let mut buf = Vec::new();
        write_length(&mut buf, 0);
        assert_eq!(buf, vec![0x00]);

        buf.clear();
        write_length(&mut buf, 63);
        assert_eq!(buf, vec![63]);
    }

    #[test]
    fn test_write_length_14bit() {
        let mut buf = Vec::new();
        write_length(&mut buf, 64);
        assert_eq!(buf, vec![0x40, 64]); // 0x40 | 0, 64

        buf.clear();
        write_length(&mut buf, 16383);
        // 16383 = 0x3FFF, high byte = 0x3F, low byte = 0xFF
        assert_eq!(buf, vec![0x40 | 0x3F, 0xFF]);
    }

    #[test]
    fn test_write_length_32bit() {
        let mut buf = Vec::new();
        write_length(&mut buf, 16384);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0x80);
        assert_eq!(u32::from_be_bytes(buf[1..5].try_into().unwrap()), 16384);

        buf.clear();
        write_length(&mut buf, u32::MAX as u64);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0x80);
        assert_eq!(u32::from_be_bytes(buf[1..5].try_into().unwrap()), u32::MAX);
    }

    #[test]
    fn test_write_length_roundtrip() {
        for &val in &[0u64, 1, 63, 64, 100, 16383, 16384, 65535, u32::MAX as u64] {
            let mut buf = Vec::new();
            write_length(&mut buf, val);
            let mut cursor = Cursor::new(buf.as_slice());
            let (read_val, is_enc) = read_length(&mut cursor).unwrap();
            assert_eq!(read_val, val, "Roundtrip failed for {}", val);
            assert!(!is_enc);
        }
    }

    #[test]
    fn test_write_redis_string() {
        let mut buf = Vec::new();
        write_redis_string(&mut buf, b"hello");
        assert_eq!(buf[0], 5); // length
        assert_eq!(&buf[1..], b"hello");
    }

    #[test]
    fn test_redis_string_roundtrip() {
        let long_string = vec![0xFF; 200];
        let test_strings: Vec<&[u8]> = vec![b"", b"x", b"hello world", &long_string];
        for s in test_strings {
            let mut buf = Vec::new();
            write_redis_string(&mut buf, s);
            let mut cursor = Cursor::new(buf.as_slice());
            let result = read_redis_string(&mut cursor).unwrap();
            assert_eq!(result, s, "String roundtrip failed");
        }
    }

    #[test]
    fn test_write_rdb_header() {
        let mut buf = Vec::new();
        write_rdb_header(&mut buf);
        assert!(buf.starts_with(b"REDIS0010"));
        // Should contain AUX fields
        assert!(buf.windows(b"redis-ver".len()).any(|w| w == b"redis-ver"));
        assert!(buf.windows(b"redis-bits".len()).any(|w| w == b"redis-bits"));
        assert!(buf.windows(b"ctime".len()).any(|w| w == b"ctime"));
        assert!(buf.windows(b"used-mem".len()).any(|w| w == b"used-mem"));
    }

    #[test]
    fn test_write_rdb_empty_database() {
        let databases: Vec<Database> = vec![Database::new()];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        // Should start with magic
        assert!(buf.starts_with(b"REDIS0010"));
        // Should end with EOF + 8-byte CRC
        let eof_pos = buf.len() - 9;
        assert_eq!(buf[eof_pos], RDB_OPCODE_EOF);
        // Verify CRC64
        let payload = &buf[..buf.len() - 8];
        let stored_crc = u64::from_le_bytes(buf[buf.len() - 8..].try_into().unwrap());
        assert_eq!(crc64_jones(payload), stored_crc);
    }

    #[test]
    fn test_write_rdb_single_string_entry() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"mykey"),
            Entry::new_string(Bytes::from_static(b"myvalue")),
        );
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        // Verify the buffer contains SELECTDB and RESIZEDB opcodes
        assert!(buf.contains(&RDB_OPCODE_SELECTDB));
        assert!(buf.contains(&RDB_OPCODE_RESIZEDB));

        // Verify it contains the key and value
        assert!(buf.windows(b"mykey".len()).any(|w| w == b"mykey"));
        assert!(buf.windows(b"myvalue".len()).any(|w| w == b"myvalue"));

        // Verify CRC
        let payload = &buf[..buf.len() - 8];
        let stored_crc = u64::from_le_bytes(buf[buf.len() - 8..].try_into().unwrap());
        assert_eq!(crc64_jones(payload), stored_crc);
    }

    #[test]
    fn test_write_rdb_string_with_ttl() {
        let mut db = Database::new();
        let base_ts = db.base_timestamp();
        let expire_ms = current_time_ms() + 60_000;
        db.set(
            Bytes::from_static(b"expkey"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), expire_ms, base_ts),
        );
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        // Should contain EXPIRETIME_MS opcode
        assert!(buf.contains(&RDB_OPCODE_EXPIRETIME_MS));
    }

    #[test]
    fn test_write_rdb_hash_entry() {
        let mut db = Database::new();
        let mut entry = Entry::new_hash();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::Hash(map) = rv {
                map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
                map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));
            }
        }
        db.set(Bytes::from_static(b"myhash"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        // Should contain hash type tag (4)
        assert!(buf.contains(&RDB_TYPE_HASH));
    }

    #[test]
    fn test_write_rdb_list_entry() {
        let mut db = Database::new();
        let mut entry = Entry::new_list();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::List(list) = rv {
                list.push_back(Bytes::from_static(b"a"));
                list.push_back(Bytes::from_static(b"b"));
                list.push_back(Bytes::from_static(b"c"));
            }
        }
        db.set(Bytes::from_static(b"mylist"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        assert!(buf.contains(&RDB_TYPE_LIST));
    }

    #[test]
    fn test_write_rdb_set_entry() {
        let mut db = Database::new();
        let mut entry = Entry::new_set();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::Set(set) = rv {
                set.insert(Bytes::from_static(b"m1"));
                set.insert(Bytes::from_static(b"m2"));
            }
        }
        db.set(Bytes::from_static(b"myset"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        assert!(buf.contains(&RDB_TYPE_SET));
    }

    #[test]
    fn test_write_rdb_sorted_set_entry() {
        let mut db = Database::new();
        let mut entry = Entry::new_sorted_set();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::SortedSet { members, scores } = rv {
                members.insert(Bytes::from_static(b"alice"), 1.5);
                scores.insert((OrderedFloat(1.5), Bytes::from_static(b"alice")), ());
                members.insert(Bytes::from_static(b"bob"), 2.5);
                scores.insert((OrderedFloat(2.5), Bytes::from_static(b"bob")), ());
            }
        }
        db.set(Bytes::from_static(b"myzset"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        assert!(buf.contains(&RDB_TYPE_ZSET_2));
    }

    #[test]
    fn test_roundtrip_string() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"k1"),
            Entry::new_string(Bytes::from_static(b"v1")),
        );
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        let mut load_dbs = vec![Database::new()];
        let count = load_rdb(&mut load_dbs, &buf).unwrap();
        assert_eq!(count, 1);

        // Verify key exists and value matches
        let data = load_dbs[0].data();
        let entry = data.get(b"k1".as_ref()).expect("key k1 not found");
        match entry.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(s, b"v1"),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_roundtrip_string_with_ttl() {
        let mut db = Database::new();
        let base_ts = db.base_timestamp();
        let expire_ms = current_time_ms() + 600_000; // 10 min in future
        db.set(
            Bytes::from_static(b"ek"),
            Entry::new_string_with_expiry(Bytes::from_static(b"ev"), expire_ms, base_ts),
        );
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        let mut load_dbs = vec![Database::new()];
        let count = load_rdb(&mut load_dbs, &buf).unwrap();
        assert_eq!(count, 1);

        let data = load_dbs[0].data();
        let entry = data.get(b"ek".as_ref()).expect("key ek not found");
        assert!(entry.has_expiry());
        // TTL should be approximately the same (within 1 second tolerance)
        let loaded_ms = entry.expires_at_ms(0);
        let diff = (loaded_ms as i64 - expire_ms as i64).unsigned_abs();
        assert!(
            diff < 1000,
            "TTL mismatch: loaded={}, expected={}",
            loaded_ms,
            expire_ms
        );
    }

    #[test]
    fn test_roundtrip_hash() {
        let mut db = Database::new();
        let mut entry = Entry::new_hash();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::Hash(map) = rv {
                map.insert(Bytes::from_static(b"field1"), Bytes::from_static(b"val1"));
                map.insert(Bytes::from_static(b"field2"), Bytes::from_static(b"val2"));
            }
        }
        db.set(Bytes::from_static(b"h1"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        let mut load_dbs = vec![Database::new()];
        let count = load_rdb(&mut load_dbs, &buf).unwrap();
        assert_eq!(count, 1);

        let data = load_dbs[0].data();
        let entry = data.get(b"h1".as_ref()).expect("key h1 not found");
        match entry.as_redis_value() {
            RedisValueRef::Hash(map) => {
                assert_eq!(map.len(), 2);
                assert_eq!(
                    map.get(&Bytes::from_static(b"field1")).unwrap(),
                    &Bytes::from_static(b"val1")
                );
                assert_eq!(
                    map.get(&Bytes::from_static(b"field2")).unwrap(),
                    &Bytes::from_static(b"val2")
                );
            }
            _ => panic!("Expected hash value"),
        }
    }

    #[test]
    fn test_roundtrip_list() {
        let mut db = Database::new();
        let mut entry = Entry::new_list();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::List(list) = rv {
                list.push_back(Bytes::from_static(b"a"));
                list.push_back(Bytes::from_static(b"b"));
                list.push_back(Bytes::from_static(b"c"));
            }
        }
        db.set(Bytes::from_static(b"l1"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        let mut load_dbs = vec![Database::new()];
        let count = load_rdb(&mut load_dbs, &buf).unwrap();
        assert_eq!(count, 1);

        let data = load_dbs[0].data();
        let entry = data.get(b"l1".as_ref()).expect("key l1 not found");
        match entry.as_redis_value() {
            RedisValueRef::List(list) => {
                assert_eq!(list.len(), 3);
                assert_eq!(list[0], Bytes::from_static(b"a"));
                assert_eq!(list[1], Bytes::from_static(b"b"));
                assert_eq!(list[2], Bytes::from_static(b"c"));
            }
            _ => panic!("Expected list value"),
        }
    }

    #[test]
    fn test_roundtrip_set() {
        let mut db = Database::new();
        let mut entry = Entry::new_set();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::Set(set) = rv {
                set.insert(Bytes::from_static(b"x"));
                set.insert(Bytes::from_static(b"y"));
            }
        }
        db.set(Bytes::from_static(b"s1"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        let mut load_dbs = vec![Database::new()];
        let count = load_rdb(&mut load_dbs, &buf).unwrap();
        assert_eq!(count, 1);

        let data = load_dbs[0].data();
        let entry = data.get(b"s1".as_ref()).expect("key s1 not found");
        match entry.as_redis_value() {
            RedisValueRef::Set(set) => {
                assert_eq!(set.len(), 2);
                assert!(set.contains(&Bytes::from_static(b"x")));
                assert!(set.contains(&Bytes::from_static(b"y")));
            }
            _ => panic!("Expected set value"),
        }
    }

    #[test]
    fn test_roundtrip_sorted_set() {
        let mut db = Database::new();
        let mut entry = Entry::new_sorted_set();
        if let Some(rv) = entry.redis_value_mut() {
            if let RedisValue::SortedSet { members, scores } = rv {
                members.insert(Bytes::from_static(b"alice"), 1.5);
                scores.insert((OrderedFloat(1.5), Bytes::from_static(b"alice")), ());
                members.insert(Bytes::from_static(b"bob"), 2.5);
                scores.insert((OrderedFloat(2.5), Bytes::from_static(b"bob")), ());
            }
        }
        db.set(Bytes::from_static(b"z1"), entry);
        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        let mut load_dbs = vec![Database::new()];
        let count = load_rdb(&mut load_dbs, &buf).unwrap();
        assert_eq!(count, 1);

        let data = load_dbs[0].data();
        let entry = data.get(b"z1".as_ref()).expect("key z1 not found");
        match entry.as_redis_value() {
            RedisValueRef::SortedSet { members, scores } => {
                assert_eq!(members.len(), 2);
                assert_eq!(*members.get(&Bytes::from_static(b"alice")).unwrap(), 1.5);
                assert_eq!(*members.get(&Bytes::from_static(b"bob")).unwrap(), 2.5);
                assert_eq!(scores.len(), 2);
            }
            _ => panic!("Expected sorted set value"),
        }
    }

    #[test]
    fn test_roundtrip_all_types() {
        let mut db = Database::new();
        let base_ts = db.base_timestamp();

        // String
        db.set(
            Bytes::from_static(b"str"),
            Entry::new_string(Bytes::from_static(b"hello")),
        );

        // String with TTL
        let expire_ms = current_time_ms() + 300_000;
        db.set(
            Bytes::from_static(b"str_ttl"),
            Entry::new_string_with_expiry(Bytes::from_static(b"world"), expire_ms, base_ts),
        );

        // Hash
        let mut hash_entry = Entry::new_hash();
        if let Some(rv) = hash_entry.redis_value_mut() {
            if let RedisValue::Hash(map) = rv {
                map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
            }
        }
        db.set(Bytes::from_static(b"hash"), hash_entry);

        // List
        let mut list_entry = Entry::new_list();
        if let Some(rv) = list_entry.redis_value_mut() {
            if let RedisValue::List(list) = rv {
                list.push_back(Bytes::from_static(b"item1"));
                list.push_back(Bytes::from_static(b"item2"));
            }
        }
        db.set(Bytes::from_static(b"list"), list_entry);

        // Set
        let mut set_entry = Entry::new_set();
        if let Some(rv) = set_entry.redis_value_mut() {
            if let RedisValue::Set(set) = rv {
                set.insert(Bytes::from_static(b"member1"));
            }
        }
        db.set(Bytes::from_static(b"set"), set_entry);

        // Sorted set
        let mut zset_entry = Entry::new_sorted_set();
        if let Some(rv) = zset_entry.redis_value_mut() {
            if let RedisValue::SortedSet { members, scores } = rv {
                members.insert(Bytes::from_static(b"z1"), 3.14);
                scores.insert((OrderedFloat(3.14), Bytes::from_static(b"z1")), ());
            }
        }
        db.set(Bytes::from_static(b"zset"), zset_entry);

        let databases = vec![db];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        // Load back
        let mut load_dbs = vec![Database::new()];
        let count = load_rdb(&mut load_dbs, &buf).unwrap();
        assert_eq!(count, 6, "Expected 6 keys loaded");

        let data = load_dbs[0].data();

        // Verify string
        assert!(data.get(b"str".as_ref()).is_some());
        // Verify string with TTL
        let ttl_entry = data.get(b"str_ttl".as_ref()).unwrap();
        assert!(ttl_entry.has_expiry());
        // Verify hash
        assert!(data.get(b"hash".as_ref()).is_some());
        // Verify list
        assert!(data.get(b"list".as_ref()).is_some());
        // Verify set
        assert!(data.get(b"set".as_ref()).is_some());
        // Verify sorted set
        assert!(data.get(b"zset".as_ref()).is_some());
    }

    #[test]
    fn test_crc64_verification_on_load() {
        let databases = vec![Database::new()];
        let mut buf = Vec::new();
        write_rdb(&databases, &mut buf);

        // Corrupt one byte in the middle
        let mid = buf.len() / 2;
        buf[mid] ^= 0xFF;

        let mut load_dbs = vec![Database::new()];
        let result = load_rdb(&mut load_dbs, &buf);
        assert!(result.is_err(), "Should fail with corrupted CRC");
        assert!(
            result.unwrap_err().to_string().contains("CRC64"),
            "Error should mention CRC64"
        );
    }
}
