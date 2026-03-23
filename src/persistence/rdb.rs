//! RDB binary snapshot format: serialize/deserialize all Redis data types with CRC32 checksum.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use bytes::Bytes;
use crc32fast::Hasher;
use ordered_float::OrderedFloat;

use crate::storage::db::Database;
use crate::storage::entry::{Entry, RedisValue};

// Format constants
const RDB_MAGIC: &[u8] = b"RUSTREDIS";
const RDB_VERSION: u8 = 1;

// Type tags
const TYPE_STRING: u8 = 0;
const TYPE_HASH: u8 = 1;
const TYPE_LIST: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_SORTED_SET: u8 = 4;

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

    // Databases
    for (db_idx, db) in databases.iter().enumerate() {
        let data = db.data();
        // Collect non-expired entries
        let live: Vec<_> = data
            .iter()
            .filter(|(_, entry)| !is_expired(entry))
            .collect();
        if live.is_empty() {
            continue;
        }

        buf.write_all(&[DB_SELECTOR])?;
        buf.write_all(&[db_idx as u8])?;

        let now_instant = Instant::now();
        let now_system = SystemTime::now();

        for (key, entry) in live {
            write_entry(&mut buf, key, entry, now_instant, now_system)?;
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
/// Each element in `snapshot` is a Vec of (key, entry) pairs for a database index.
pub fn save_from_snapshot(snapshot: &[Vec<(Bytes, Entry)>], path: &Path) -> anyhow::Result<()> {
    let mut buf = Vec::new();

    // Header
    buf.write_all(RDB_MAGIC)?;
    buf.write_all(&[RDB_VERSION])?;

    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    for (db_idx, entries) in snapshot.iter().enumerate() {
        // Filter expired and skip empty
        let live: Vec<_> = entries.iter().filter(|(_, e)| !is_expired(e)).collect();
        if live.is_empty() {
            continue;
        }

        buf.write_all(&[DB_SELECTOR])?;
        buf.write_all(&[db_idx as u8])?;

        for (key, entry) in live {
            write_entry(&mut buf, key, entry, now_instant, now_system)?;
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

    let now_instant = Instant::now();
    let now_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

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
                let (key, entry) = read_entry(&mut cursor, type_tag, now_instant, now_unix_ms)?;
                // Skip entries whose TTL is already in the past
                if let Some(exp) = entry.expires_at {
                    if Instant::now() >= exp {
                        continue;
                    }
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

fn is_expired(entry: &Entry) -> bool {
    entry
        .expires_at
        .is_some_and(|exp| Instant::now() >= exp)
}

fn write_entry(
    buf: &mut Vec<u8>,
    key: &Bytes,
    entry: &Entry,
    now_instant: Instant,
    now_system: SystemTime,
) -> anyhow::Result<()> {
    // Type tag
    let type_tag = match &entry.value {
        RedisValue::String(_) => TYPE_STRING,
        RedisValue::Hash(_) => TYPE_HASH,
        RedisValue::List(_) => TYPE_LIST,
        RedisValue::Set(_) => TYPE_SET,
        RedisValue::SortedSet { .. } => TYPE_SORTED_SET,
    };
    buf.write_all(&[type_tag])?;

    // Key
    write_bytes(buf, key)?;

    // TTL as unix millis (0 = no expiry)
    let ttl_ms: i64 = match entry.expires_at {
        Some(exp) => {
            if exp > now_instant {
                let remaining = exp - now_instant;
                let unix_ms = now_system
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
                    + remaining.as_millis() as i64;
                unix_ms
            } else {
                0 // expired, will be skipped by caller but handle defensively
            }
        }
        None => 0,
    };
    buf.write_all(&ttl_ms.to_le_bytes())?;

    // Value data
    match &entry.value {
        RedisValue::String(s) => {
            write_bytes(buf, s)?;
        }
        RedisValue::Hash(map) => {
            buf.write_all(&(map.len() as u32).to_le_bytes())?;
            for (field, val) in map {
                write_bytes(buf, field)?;
                write_bytes(buf, val)?;
            }
        }
        RedisValue::List(list) => {
            buf.write_all(&(list.len() as u32).to_le_bytes())?;
            for elem in list {
                write_bytes(buf, elem)?;
            }
        }
        RedisValue::Set(set) => {
            buf.write_all(&(set.len() as u32).to_le_bytes())?;
            for member in set {
                write_bytes(buf, member)?;
            }
        }
        RedisValue::SortedSet { members, .. } => {
            buf.write_all(&(members.len() as u32).to_le_bytes())?;
            for (member, score) in members {
                write_bytes(buf, member)?;
                buf.write_all(&score.to_le_bytes())?;
            }
        }
    }

    Ok(())
}

fn read_entry(
    cursor: &mut Cursor<&[u8]>,
    type_tag: u8,
    now_instant: Instant,
    now_unix_ms: i64,
) -> anyhow::Result<(Bytes, Entry)> {
    // Key
    let key = read_bytes(cursor)?;

    // TTL
    let mut ttl_buf = [0u8; 8];
    cursor.read_exact(&mut ttl_buf)?;
    let ttl_ms = i64::from_le_bytes(ttl_buf);

    let expires_at = if ttl_ms > 0 {
        let remaining_ms = ttl_ms - now_unix_ms;
        if remaining_ms > 0 {
            Some(now_instant + Duration::from_millis(remaining_ms as u64))
        } else {
            // Expired - we still parse, caller checks
            Some(now_instant - Duration::from_millis(1))
        }
    } else {
        None
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
            let mut scores = BTreeMap::new();
            for _ in 0..count {
                let member = read_bytes(cursor)?;
                let mut score_buf = [0u8; 8];
                cursor.read_exact(&mut score_buf)?;
                let score = f64::from_le_bytes(score_buf);
                members.insert(member.clone(), score);
                scores.insert((OrderedFloat(score), member), ());
            }
            RedisValue::SortedSet { members, scores }
        }
        _ => bail!("Unknown type tag: {}", type_tag),
    };

    let now = Instant::now();
    let entry = Entry {
        value,
        expires_at,
        created_at: now,
        version: 0,
        last_access: now,
        access_counter: 5,
    };

    Ok((key, entry))
}

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) -> anyhow::Result<()> {
    buf.write_all(&(data.len() as u32).to_le_bytes())?;
    buf.write_all(data)?;
    Ok(())
}

fn read_bytes(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<Bytes> {
    let len = read_u32(cursor)? as usize;
    let mut data = vec![0u8; len];
    cursor.read_exact(&mut data)?;
    Ok(Bytes::from(data))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
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
        match &entry.value {
            RedisValue::String(v) => assert_eq!(v.as_ref(), b"world"),
            _ => panic!("Expected string"),
        }
        assert!(entry.expires_at.is_none());
    }

    #[test]
    fn test_round_trip_string_with_ttl() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];
        let future = Instant::now() + Duration::from_secs(3600);
        dbs[0].set_string_with_expiry(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"val"),
            future,
        );

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        // TTL should be approximately 3600 seconds in the future (allow 5s tolerance)
        let remaining = entry.expires_at.unwrap().duration_since(Instant::now());
        assert!(remaining.as_secs() > 3590 && remaining.as_secs() <= 3600);
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
        match &entry.value {
            RedisValue::Hash(map) => {
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
        match &entry.value {
            RedisValue::List(list) => {
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
        match &entry.value {
            RedisValue::Set(set) => {
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
            let (members, scores) = dbs[0].get_or_create_sorted_set(b"myzset").unwrap();
            members.insert(Bytes::from_static(b"alice"), 1.5);
            scores.insert((OrderedFloat(1.5), Bytes::from_static(b"alice")), ());
            members.insert(Bytes::from_static(b"bob"), 2.7);
            scores.insert((OrderedFloat(2.7), Bytes::from_static(b"bob")), ());
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        let entry = loaded[0].get(b"myzset").unwrap();
        match &entry.value {
            RedisValue::SortedSet { members, scores } => {
                assert_eq!(members.len(), 2);
                assert_eq!(*members.get(&Bytes::from_static(b"alice")).unwrap(), 1.5);
                assert_eq!(*members.get(&Bytes::from_static(b"bob")).unwrap(), 2.7);
                assert_eq!(scores.len(), 2);
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
        let future = Instant::now() + Duration::from_secs(600);
        dbs[0].set_string_with_expiry(
            Bytes::from_static(b"str_ttl"),
            Bytes::from_static(b"expiring"),
            future,
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
            scores.insert((OrderedFloat(1.0), Bytes::from_static(b"a")), ());
        }

        save(&dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = load(&mut loaded, &path).unwrap();
        assert_eq!(count, 6);

        // Verify each type
        assert!(matches!(loaded[0].get(b"str").unwrap().value, RedisValue::String(_)));
        assert!(loaded[0].get(b"str_ttl").unwrap().expires_at.is_some());
        assert!(matches!(loaded[0].get(b"h").unwrap().value, RedisValue::Hash(_)));
        assert!(matches!(loaded[0].get(b"l").unwrap().value, RedisValue::List(_)));
        assert!(matches!(loaded[0].get(b"s").unwrap().value, RedisValue::Set(_)));
        assert!(matches!(loaded[0].get(b"z").unwrap().value, RedisValue::SortedSet { .. }));
    }

    #[test]
    fn test_expired_keys_skipped_during_save() {
        let (_dir, path) = rdb_path();
        let mut dbs = vec![Database::new()];

        // Live key
        dbs[0].set_string(Bytes::from_static(b"live"), Bytes::from_static(b"yes"));
        // Expired key
        let past = Instant::now() - Duration::from_secs(1);
        dbs[0].set(
            Bytes::from_static(b"dead"),
            Entry::new_string_with_expiry(Bytes::from_static(b"no"), past),
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
}
