//! Forkless compartmentalized snapshot engine.
//!
//! Serializes DashTable segments one at a time as a cooperative task within the
//! shard event loop. Keys modified in not-yet-serialized segments have their old
//! values captured in a per-snapshot overflow buffer (segment-level COW).

use std::collections::HashSet;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use bytes::Bytes;
use crc32fast::Hasher;

use crate::persistence::rdb;
use crate::storage::dashtable::DashTable;
use crate::storage::db::Database;
use crate::storage::entry::{current_secs, current_time_ms, Entry};

// Per-shard snapshot format constants
const SHARD_RDB_MAGIC: &[u8] = b"RRDSHARD";
const SHARD_RDB_VERSION: u8 = 1;
const EOF_MARKER: u8 = 0xFF;
const SEGMENT_BLOCK_MARKER: u8 = 0xFD;
const DB_SELECTOR: u8 = 0xFE;

/// State machine for cooperative segment-by-segment snapshot.
///
/// Created when a snapshot epoch begins. Advanced one segment per tick.
/// Completed when all segments across all databases have been serialized.
pub struct SnapshotState {
    /// Snapshot epoch number.
    pub epoch: u64,
    /// Current database index being serialized (0..num_databases).
    current_db: usize,
    /// Current segment index within the current database.
    current_segment: usize,
    /// Total segment count for the current database (captured at epoch start).
    segments_in_current_db: usize,
    /// Total number of databases to snapshot.
    num_databases: usize,
    /// Segment counts per database, captured at epoch start.
    segment_counts: Vec<usize>,
    /// Base timestamps per database.
    base_timestamps: Vec<u32>,
    /// Output buffer accumulating serialized bytes.
    output_buf: Vec<u8>,
    /// COW overflow buffer: (db_index, segment_storage_idx, key, entry) for entries
    /// modified before their segment was serialized.
    overflow: Vec<(usize, usize, Bytes, Entry)>,
    /// Per-database sets of segment storage indices that have already been serialized.
    /// Outer index = db_index. Used to determine if a segment is still "pending".
    serialized_segments: Vec<Vec<bool>>,
    /// Shard ID for the snapshot file header.
    shard_id: u16,
    /// Output file path.
    file_path: PathBuf,
    /// Whether the header has been written.
    header_written: bool,
    /// Whether we need to write a DB selector for the current database.
    db_selector_written: Vec<bool>,
}

impl SnapshotState {
    /// Create a new snapshot state machine.
    ///
    /// Captures segment counts and base timestamps from each database at epoch start.
    pub fn new(shard_id: u16, epoch: u64, databases: &[Database], file_path: PathBuf) -> Self {
        let num_databases = databases.len();
        let segment_counts: Vec<usize> = databases
            .iter()
            .map(|db| db.data().segment_count())
            .collect();
        let base_timestamps: Vec<u32> = databases
            .iter()
            .map(|db| db.base_timestamp())
            .collect();
        let serialized_segments: Vec<Vec<bool>> = segment_counts
            .iter()
            .map(|&count| vec![false; count])
            .collect();
        let segments_in_current_db = segment_counts.first().copied().unwrap_or(0);

        SnapshotState {
            epoch,
            current_db: 0,
            current_segment: 0,
            segments_in_current_db,
            num_databases,
            segment_counts,
            base_timestamps,
            output_buf: Vec::with_capacity(4096),
            overflow: Vec::new(),
            serialized_segments,
            shard_id,
            file_path,
            header_written: false,
            db_selector_written: vec![false; num_databases],
        }
    }

    /// Check if a segment in a given database has NOT yet been serialized.
    ///
    /// Returns true if the snapshot is active for this db and the segment is pending.
    #[inline]
    pub fn is_segment_pending(&self, db_index: usize, segment_storage_idx: usize) -> bool {
        if db_index > self.current_db {
            return true;
        }
        if db_index < self.current_db {
            return false;
        }
        // db_index == current_db
        if segment_storage_idx < self.serialized_segments[db_index].len() {
            !self.serialized_segments[db_index][segment_storage_idx]
        } else {
            false
        }
    }

    /// Capture an old entry value before overwrite for COW.
    ///
    /// Called when a write targets a segment that hasn't been serialized yet.
    pub fn capture_cow(
        &mut self,
        db_index: usize,
        segment_storage_idx: usize,
        key: Bytes,
        old_entry: Entry,
    ) {
        self.overflow
            .push((db_index, segment_storage_idx, key, old_entry));
    }

    /// Advance the snapshot by one segment. Returns true when all segments are done.
    ///
    /// Serializes the current segment's entries (overflow first, then live data),
    /// writes per-segment CRC32, and advances to the next segment.
    pub fn advance_one_segment(&mut self, databases: &[Database]) -> bool {
        // Write header on first call
        if !self.header_written {
            self.output_buf.extend_from_slice(SHARD_RDB_MAGIC);
            self.output_buf.push(SHARD_RDB_VERSION);
            self.output_buf.extend_from_slice(&self.shard_id.to_le_bytes());
            self.output_buf.extend_from_slice(&self.epoch.to_le_bytes());
            self.header_written = true;
        }

        // Check if done
        if self.current_db >= self.num_databases {
            return true;
        }

        let db = &databases[self.current_db];
        let base_ts = self.base_timestamps[self.current_db];
        let now_ms = current_time_ms();

        // Write DB selector if this is the first segment of a new database
        if !self.db_selector_written[self.current_db] {
            self.output_buf.push(DB_SELECTOR);
            self.output_buf.push(self.current_db as u8);
            self.db_selector_written[self.current_db] = true;
        }

        let seg_idx = self.current_segment;
        let segment = db.data().segment(seg_idx);

        // Collect overflow entries for this segment
        let overflow_entries: Vec<(Bytes, Entry)> = self
            .overflow
            .iter()
            .filter(|(db_idx, s_idx, _, _)| *db_idx == self.current_db && *s_idx == seg_idx)
            .map(|(_, _, k, e)| (k.clone(), e.clone()))
            .collect();
        let overflow_keys: HashSet<Bytes> = overflow_entries
            .iter()
            .map(|(k, _)| k.clone())
            .collect();

        // Remove consumed overflow entries
        self.overflow
            .retain(|(db_idx, s_idx, _, _)| !(*db_idx == self.current_db && *s_idx == seg_idx));

        // Collect all entries for this segment: overflow + live (non-overlap, non-expired)
        let mut segment_entries: Vec<u8> = Vec::new();
        let mut entry_count: u32 = 0;

        // Write overflow entries first (these represent old values before modification)
        for (key, entry) in &overflow_entries {
            if entry.has_expiry() && entry.is_expired_at(base_ts, now_ms) {
                continue;
            }
            rdb::write_entry(&mut segment_entries, key, entry, base_ts)
                .expect("segment entry serialization failed");
            entry_count += 1;
        }

        // Write live entries, skipping those already in overflow and expired ones
        for (key, entry) in segment.iter_occupied() {
            if overflow_keys.contains(key) {
                continue;
            }
            if entry.has_expiry() && entry.is_expired_at(base_ts, now_ms) {
                continue;
            }
            rdb::write_entry(&mut segment_entries, key, entry, base_ts)
                .expect("segment entry serialization failed");
            entry_count += 1;
        }

        // Write segment block: marker + segment_idx + entry_count + data + CRC32
        self.output_buf.push(SEGMENT_BLOCK_MARKER);
        self.output_buf
            .extend_from_slice(&(seg_idx as u32).to_le_bytes());
        self.output_buf
            .extend_from_slice(&entry_count.to_le_bytes());

        // Per-segment CRC32 covers the entry data
        let mut hasher = Hasher::new();
        hasher.update(&segment_entries);
        let crc = hasher.finalize();

        self.output_buf.extend_from_slice(&segment_entries);
        self.output_buf.extend_from_slice(&crc.to_le_bytes());

        // Mark this segment as serialized
        self.serialized_segments[self.current_db][seg_idx] = true;

        // Advance to next segment
        self.current_segment += 1;
        if self.current_segment >= self.segments_in_current_db {
            self.current_db += 1;
            self.current_segment = 0;
            if self.current_db < self.num_databases {
                self.segments_in_current_db = self.segment_counts[self.current_db];
            }
        }

        self.current_db >= self.num_databases
    }

    /// Finalize the snapshot: write EOF marker, global CRC32, and atomically write to disk.
    pub fn finalize(&mut self) -> anyhow::Result<()> {
        // Write EOF marker
        self.output_buf.push(EOF_MARKER);

        // Global CRC32 of entire output_buf
        let mut hasher = Hasher::new();
        hasher.update(&self.output_buf);
        let global_crc = hasher.finalize();
        self.output_buf.extend_from_slice(&global_crc.to_le_bytes());

        // Atomic write: write to .tmp, then rename
        let tmp_path = self.file_path.with_extension("rrdshard.tmp");
        std::fs::write(&tmp_path, &self.output_buf)
            .context("Failed to write temporary snapshot file")?;
        std::fs::rename(&tmp_path, &self.file_path)
            .context("Failed to rename temporary snapshot file")?;

        Ok(())
    }

    /// Check if all databases have been fully serialized.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.current_db >= self.num_databases
    }
}

/// Synchronous all-at-once snapshot save for testing and simple use cases.
///
/// Creates a SnapshotState, runs advance_one_segment in a loop until complete,
/// then finalizes and writes to disk.
pub fn shard_snapshot_save(
    shard_id: u16,
    epoch: u64,
    databases: &[Database],
    path: &Path,
) -> anyhow::Result<()> {
    let mut state = SnapshotState::new(shard_id, epoch, databases, path.to_path_buf());
    while !state.advance_one_segment(databases) {}
    state.finalize()
}

/// Load a per-shard snapshot file and populate databases. Returns total keys loaded.
///
/// Reads RRDSHARD format with per-segment CRC32 verification.
pub fn shard_snapshot_load(
    databases: &mut [Database],
    path: &Path,
) -> anyhow::Result<usize> {
    let data = std::fs::read(path).context("Failed to read snapshot file")?;

    // Minimum size: magic(8) + version(1) + shard_id(2) + epoch(8) + eof(1) + global_crc(4) = 24
    if data.len() < 24 {
        bail!("Snapshot file too small");
    }

    // Verify global CRC32: all bytes except last 4 vs last 4 bytes
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
            "Snapshot global checksum mismatch: stored={:#010x}, computed={:#010x}",
            stored_checksum,
            computed_checksum
        );
    }

    let mut cursor = Cursor::new(payload);

    // Verify magic
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic != SHARD_RDB_MAGIC {
        bail!("Invalid RRDSHARD magic header");
    }

    // Verify version
    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != SHARD_RDB_VERSION {
        bail!("Unsupported RRDSHARD version: {}", version[0]);
    }

    // Read shard_id and epoch (for logging/verification)
    let mut shard_id_buf = [0u8; 2];
    cursor.read_exact(&mut shard_id_buf)?;
    let _shard_id = u16::from_le_bytes(shard_id_buf);

    let mut epoch_buf = [0u8; 8];
    cursor.read_exact(&mut epoch_buf)?;
    let _epoch = u64::from_le_bytes(epoch_buf);

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
                    bail!(
                        "Snapshot references database {} but only {} configured",
                        current_db,
                        databases.len()
                    );
                }
            }
            SEGMENT_BLOCK_MARKER => {
                // Read segment index and entry count
                let seg_idx = rdb::read_u32(&mut cursor)? as usize;
                let entry_count = rdb::read_u32(&mut cursor)?;
                let _ = seg_idx; // segment index is informational during load

                // Read all entry bytes for CRC verification
                let data_start = cursor.position() as usize;

                // We need to read entries and compute CRC simultaneously
                // First pass: read entries, tracking bytes consumed
                let mut entries: Vec<(Bytes, Entry)> = Vec::with_capacity(entry_count as usize);
                for _ in 0..entry_count {
                    let mut type_tag = [0u8; 1];
                    cursor.read_exact(&mut type_tag)?;
                    let (key, entry) = rdb::read_entry(&mut cursor, type_tag[0])?;
                    entries.push((key, entry));
                }

                let data_end = cursor.position() as usize;

                // Read and verify per-segment CRC32
                let mut crc_buf = [0u8; 4];
                cursor.read_exact(&mut crc_buf)?;
                let stored_crc = u32::from_le_bytes(crc_buf);

                let segment_data = &payload[data_start..data_end];
                let mut seg_hasher = Hasher::new();
                seg_hasher.update(segment_data);
                let computed_crc = seg_hasher.finalize();
                if stored_crc != computed_crc {
                    bail!(
                        "Segment CRC mismatch at segment {}: stored={:#010x}, computed={:#010x}",
                        seg_idx,
                        stored_crc,
                        computed_crc
                    );
                }

                // Insert non-expired entries into the database
                for (key, entry) in entries {
                    if entry.has_expiry() && entry.is_expired_at(current_secs(), now_ms) {
                        continue;
                    }
                    if current_db < databases.len() {
                        databases[current_db].set(key, entry);
                        total_keys += 1;
                    }
                }
            }
            other => {
                bail!("Unknown tag in snapshot: {:#04x}", other);
            }
        }
    }

    Ok(total_keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compact_value::RedisValueRef;
    use ordered_float::OrderedFloat;
    use tempfile::tempdir;

    fn snap_path() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dump.rrdshard");
        (dir, path)
    }

    #[test]
    fn test_snapshot_round_trip_string() {
        let (_dir, path) = snap_path();
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        dbs[0].set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));
        dbs[0].set_string(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"));

        shard_snapshot_save(0, 1, &dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = shard_snapshot_load(&mut loaded, &path).unwrap();
        assert_eq!(count, 3);
        for key in &[b"k1", b"k2", b"k3"] {
            let entry = loaded[0].get(*key).unwrap();
            match entry.value.as_redis_value() {
                RedisValueRef::String(_) => {}
                _ => panic!("Expected string for key {:?}", key),
            }
        }
    }

    #[test]
    fn test_snapshot_round_trip_all_types() {
        let (_dir, path) = snap_path();
        let mut dbs = vec![Database::new()];

        // String
        dbs[0].set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
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

        shard_snapshot_save(0, 1, &dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = shard_snapshot_load(&mut loaded, &path).unwrap();
        assert_eq!(count, 5);
        assert_eq!(loaded[0].get(b"str").unwrap().value.type_name(), "string");
        assert_eq!(loaded[0].get(b"h").unwrap().value.type_name(), "hash");
        assert_eq!(loaded[0].get(b"l").unwrap().value.type_name(), "list");
        assert_eq!(loaded[0].get(b"s").unwrap().value.type_name(), "set");
        assert_eq!(loaded[0].get(b"z").unwrap().value.type_name(), "zset");
    }

    #[test]
    fn test_snapshot_with_ttl() {
        let (_dir, path) = snap_path();
        let mut dbs = vec![Database::new()];

        // Key with future TTL
        let future_ms = current_time_ms() + 3_600_000;
        dbs[0].set_string_with_expiry(
            Bytes::from_static(b"live"),
            Bytes::from_static(b"yes"),
            future_ms,
        );
        // Key with past TTL (should be skipped)
        let past_ms = current_time_ms() - 1000;
        let base_ts = dbs[0].base_timestamp();
        dbs[0].set(
            Bytes::from_static(b"dead"),
            Entry::new_string_with_expiry(Bytes::from_static(b"no"), past_ms, base_ts),
        );

        shard_snapshot_save(0, 1, &dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = shard_snapshot_load(&mut loaded, &path).unwrap();
        assert_eq!(count, 1);
        assert!(loaded[0].get(b"live").is_some());
        assert!(loaded[0].get(b"dead").is_none());
    }

    #[test]
    fn test_snapshot_cow_captures_old_value() {
        let (_dir, path) = snap_path();
        let mut dbs = vec![Database::new()];
        // Insert enough entries across at least 2 segments
        for i in 0..100 {
            dbs[0].set_string(
                Bytes::from(format!("cow_{:04}", i)),
                Bytes::from(format!("val_{:04}", i)),
            );
        }

        let seg_count = dbs[0].data().segment_count();
        assert!(seg_count > 1, "Need multiple segments for COW test");

        let mut state = SnapshotState::new(0, 1, &dbs, path.to_path_buf());

        // Advance past segment 0
        let done = state.advance_one_segment(&dbs);
        assert!(!done, "Should not be done after first segment");

        // Capture a COW entry for segment 1 (which hasn't been serialized yet)
        // Find a key that lives in segment 1
        let seg1 = dbs[0].data().segment(1);
        let (cow_key, cow_old_entry) = seg1.iter_occupied().next().unwrap();
        let cow_key = cow_key.clone();
        let cow_old_entry = cow_old_entry.clone();

        assert!(state.is_segment_pending(0, 1));
        state.capture_cow(0, 1, cow_key.clone(), cow_old_entry);

        // Now overwrite the key in the live database (simulating a write during snapshot)
        dbs[0].set_string(cow_key.clone(), Bytes::from_static(b"NEW_VALUE"));

        // Continue advancing until done
        while !state.advance_one_segment(&dbs) {}
        state.finalize().unwrap();

        // Load and verify the COW captured the old value (not the new one)
        let mut loaded = vec![Database::new()];
        let _count = shard_snapshot_load(&mut loaded, &path).unwrap();
        let entry = loaded[0].get(&cow_key).unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::String(s) => {
                // The snapshot should have the OLD value from COW, not "NEW_VALUE"
                assert_ne!(s.as_ref(), b"NEW_VALUE", "COW should have captured old value");
            }
            _ => panic!("Expected string"),
        }
    }

    #[test]
    fn test_snapshot_per_segment_crc32() {
        let (_dir, path) = snap_path();
        let mut dbs = vec![Database::new()];
        // Use a longer value so we can corrupt a data byte without hitting a tag
        dbs[0].set_string(
            Bytes::from_static(b"testkey"),
            Bytes::from_static(b"testvalue_long_enough"),
        );

        shard_snapshot_save(0, 1, &dbs, &path).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        // Layout: header(19) + DB_SELECTOR(1) + db_idx(1) + SEGMENT_BLOCK_MARKER(1)
        //       + seg_idx(4) + entry_count(4) + [entry_data...] + seg_crc(4) + EOF(1) + global_crc(4)
        // Entry data starts at offset 30. The entry:
        //   type_tag(1) + key_len(4) + "testkey"(7) + ttl(8) + val_len(4) + "testvalue_long_enough"(21) = 45 bytes
        // Value string bytes start at offset 30+1+4+7+8+4 = 54, end at 75
        // Corrupt a byte in the value string area (offset 60)
        let corrupt_offset = 60;
        assert!(corrupt_offset < data.len() - 8, "File too small for corruption test");
        data[corrupt_offset] ^= 0xFF;

        // Recalculate the global CRC to isolate the segment CRC check
        let payload_len = data.len() - 4;
        let mut hasher = Hasher::new();
        hasher.update(&data[..payload_len]);
        let new_global_crc = hasher.finalize();
        data[payload_len..].copy_from_slice(&new_global_crc.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let mut loaded = vec![Database::new()];
        let result = shard_snapshot_load(&mut loaded, &path);
        assert!(result.is_err(), "Should fail on corrupted segment CRC");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CRC") || err.contains("mismatch"),
            "Error should mention CRC: {}",
            err
        );
    }

    #[test]
    fn test_snapshot_multi_database() {
        let (_dir, path) = snap_path();
        let mut dbs = vec![Database::new(), Database::new()];
        dbs[0].set_string(Bytes::from_static(b"db0_k"), Bytes::from_static(b"v0"));
        dbs[1].set_string(Bytes::from_static(b"db1_k"), Bytes::from_static(b"v1"));

        shard_snapshot_save(0, 1, &dbs, &path).unwrap();

        let mut loaded = vec![Database::new(), Database::new()];
        let count = shard_snapshot_load(&mut loaded, &path).unwrap();
        assert_eq!(count, 2);
        assert!(loaded[0].get(b"db0_k").is_some());
        assert!(loaded[1].get(b"db1_k").is_some());
    }

    #[test]
    fn test_snapshot_empty_database() {
        let (_dir, path) = snap_path();
        let dbs = vec![Database::new()];

        shard_snapshot_save(0, 1, &dbs, &path).unwrap();

        let mut loaded = vec![Database::new()];
        let count = shard_snapshot_load(&mut loaded, &path).unwrap();
        assert_eq!(count, 0);
        assert_eq!(loaded[0].len(), 0);
    }

    #[test]
    fn test_advance_one_segment_yields_between_segments() {
        let mut dbs = vec![Database::new()];
        // Insert enough entries to have multiple segments
        for i in 0..100 {
            dbs[0].set_string(
                Bytes::from(format!("yield_{:04}", i)),
                Bytes::from(format!("v_{:04}", i)),
            );
        }

        let seg_count = dbs[0].data().segment_count();
        assert!(seg_count > 1, "Need multiple segments, got {}", seg_count);

        let (_dir, path) = snap_path();
        let mut state = SnapshotState::new(0, 1, &dbs, path.to_path_buf());

        // First advance should return false (not done -- more segments to process)
        let done = state.advance_one_segment(&dbs);
        assert!(!done, "First advance should not complete with {} segments", seg_count);

        // Advance all remaining segments
        let mut advances = 1;
        while !state.advance_one_segment(&dbs) {
            advances += 1;
        }
        advances += 1; // count the final true-returning call... actually the last true call is when advance_one_segment returned true

        // Total advances should equal segment count
        assert_eq!(
            advances, seg_count,
            "Should need exactly {} advances, got {}",
            seg_count, advances
        );

        assert!(state.is_complete());
    }
}
