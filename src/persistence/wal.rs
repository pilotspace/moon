//! Per-shard Write-Ahead Log (WAL) writer -- v2 format.
//!
//! Each shard maintains its own WAL file containing RESP-encoded write commands
//! wrapped in CRC32-checksummed block frames with a 32-byte versioned header.
//!
//! **WAL v2 on-disk format:**
//! ```text
//! [Header: 32 bytes]
//!   magic: "RRDWAL" (6B)
//!   version: u8 = 2 (1B)
//!   shard_id: u16 LE (2B)
//!   epoch: u64 LE (8B)
//!   reserved: [0u8; 15] (15B)
//!
//! [Block frame] (repeated per flush)
//!   block_len: u32 LE -- covers cmd_count + db_idx + payload + crc32
//!   cmd_count: u16 LE
//!   db_idx: u8
//!   payload: [u8; payload_len] -- raw RESP bytes
//!   crc32: u32 LE -- CRC32 over cmd_count(2B) + db_idx(1B) + payload
//! ```
//!
//! Writes are buffered in memory and flushed/fsynced on a configurable interval
//! (default 1ms for batched fsync). On Linux, uses direct file I/O; on macOS,
//! uses std::fs as fallback.
//!
//! Replaces the global AOF writer from Phase 11 with per-shard locality.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tracing::info;

use crate::runtime::{TokioFileIo, traits::FileIo};
use crate::storage::db::Database;

/// WAL v2 format constants
const WAL_MAGIC: &[u8; 6] = b"RRDWAL";
const WAL_VERSION: u8 = 2;
const WAL_HEADER_SIZE: usize = 32;
/// Block overhead: block_len(4) + cmd_count(2) + db_idx(1) + crc32(4) = 11 bytes
#[allow(dead_code)]
const BLOCK_OVERHEAD: usize = 11;

/// Per-shard WAL writer with batched fsync.
///
/// Accumulates RESP-encoded command bytes between fsync intervals.
/// Flush is called explicitly by the shard event loop on its 1ms tick.
/// Produces CRC32-checksummed block frames for corruption detection.
pub struct WalWriter {
    shard_id: usize,
    /// Buffered RESP bytes awaiting flush.
    buf: Vec<u8>,
    /// File handle for the WAL.
    file: Option<std::fs::File>,
    /// Path to the WAL file.
    file_path: PathBuf,
    /// Current write offset in the file.
    write_offset: u64,
    /// Timestamp of last fsync.
    last_fsync: Instant,
    /// Current snapshot epoch (for WAL truncation after snapshot).
    epoch: u64,
    /// Total bytes written since last truncation.
    bytes_written: u64,
    /// Number of commands appended since last flush (for block framing).
    cmd_count: u16,
    /// Whether the v2 header has been written to the current file.
    header_written: bool,
}

impl WalWriter {
    /// Create a new WAL writer for the given shard.
    ///
    /// WAL file path: `{dir}/shard-{shard_id}.wal`
    /// Opens file in append+create mode. Pre-allocates 8KB buffer.
    /// Writes v2 header on fresh files.
    pub fn new(shard_id: usize, dir: &Path) -> std::io::Result<Self> {
        let file_path = wal_path(dir, shard_id);
        let file = TokioFileIo::open_append(&file_path)?;

        let write_offset = file.metadata()?.len();
        let header_written = write_offset > 0;

        let mut writer = Self {
            shard_id,
            buf: Vec::with_capacity(8192),
            file: Some(file),
            file_path,
            write_offset,
            last_fsync: Instant::now(),
            epoch: 0,
            bytes_written: 0,
            cmd_count: 0,
            header_written,
        };

        // Write v2 header on fresh files
        if write_offset == 0 {
            writer.write_header()?;
        }

        Ok(writer)
    }

    /// Write the 32-byte v2 header directly to the file.
    ///
    /// Layout: RRDWAL(6B) + version(1B) + shard_id(2B LE) + epoch(8B LE) + reserved(15B)
    fn write_header(&mut self) -> std::io::Result<()> {
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..6].copy_from_slice(WAL_MAGIC);
        header[6] = WAL_VERSION;
        header[7..9].copy_from_slice(&(self.shard_id as u16).to_le_bytes());
        header[9..17].copy_from_slice(&self.epoch.to_le_bytes());
        // bytes 17..32 remain zero (reserved)

        if let Some(ref mut file) = self.file {
            file.write_all(&header)?;
            self.write_offset += WAL_HEADER_SIZE as u64;
            self.bytes_written += WAL_HEADER_SIZE as u64;
            self.header_written = true;
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WAL file handle is closed",
            ))
        }
    }

    /// Append RESP-encoded command bytes to the in-memory buffer.
    ///
    /// No I/O occurs here -- this is the hot path called on every write command.
    /// Only adds a saturating_add for cmd_count tracking.
    #[inline]
    pub fn append(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        self.cmd_count = self.cmd_count.saturating_add(1);
    }

    /// Flush buffered data to OS page cache if the buffer is non-empty.
    ///
    /// Called on the shard's 1ms tick. Only does write_all() (fast, goes to
    /// OS page cache), NOT fsync. Use sync_to_disk() for durability.
    pub fn flush_if_needed(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.do_write()
    }

    /// Force flush with fsync. Used on shutdown and before snapshot.
    pub fn flush_sync(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.do_write()?;
        self.do_sync()
    }

    /// Sync file data to disk (fsync). Called on a less-frequent interval
    /// (e.g. every second for "everysec" mode) to avoid blocking the shard
    /// on every 1ms tick.
    pub fn sync_to_disk(&mut self) -> std::io::Result<()> {
        if let Some(ref mut file) = self.file {
            file.sync_data()?;
            self.last_fsync = Instant::now();
            Ok(())
        } else {
            Ok(()) // No file open, nothing to sync
        }
    }

    /// Internal write: produce a CRC32-framed block from the buffer, write to file.
    ///
    /// Block layout: [block_len:4 LE][cmd_count:2 LE][db_idx:1][payload:var][crc32:4 LE]
    /// CRC32 covers: cmd_count(2B) + db_idx(1B) + payload bytes.
    fn do_write(&mut self) -> std::io::Result<()> {
        if let Some(ref mut file) = self.file {
            // Ensure header is written before any block data
            if !self.header_written {
                // Can't call self.write_header() due to borrow, inline it
                let mut header = [0u8; WAL_HEADER_SIZE];
                header[0..6].copy_from_slice(WAL_MAGIC);
                header[6] = WAL_VERSION;
                header[7..9].copy_from_slice(&(self.shard_id as u16).to_le_bytes());
                header[9..17].copy_from_slice(&self.epoch.to_le_bytes());
                file.write_all(&header)?;
                self.write_offset += WAL_HEADER_SIZE as u64;
                self.bytes_written += WAL_HEADER_SIZE as u64;
                self.header_written = true;
            }

            let cmd_count_bytes = self.cmd_count.to_le_bytes();
            let db_idx: u8 = 0;

            // Compute CRC32 over cmd_count(2B) + db_idx(1B) + payload
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&cmd_count_bytes);
            hasher.update(&[db_idx]);
            hasher.update(&self.buf);
            let crc = hasher.finalize();

            // block_len = cmd_count(2) + db_idx(1) + payload.len() + crc32(4)
            let block_len = (2 + 1 + self.buf.len() + 4) as u32;

            // Write block frame: 7-byte header on stack, then payload, then CRC
            let mut block_header = [0u8; 7];
            block_header[0..4].copy_from_slice(&block_len.to_le_bytes());
            block_header[4..6].copy_from_slice(&cmd_count_bytes);
            block_header[6] = db_idx;

            file.write_all(&block_header)?;
            file.write_all(&self.buf)?;
            file.write_all(&crc.to_le_bytes())?;

            let total_written = 4 + block_len as u64; // block_len prefix + block_len contents
            self.write_offset += total_written;
            self.bytes_written += total_written;
            self.cmd_count = 0;
            self.buf.clear(); // clear but keep allocation
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WAL file handle is closed",
            ))
        }
    }

    /// Internal sync: fsync only.
    fn do_sync(&mut self) -> std::io::Result<()> {
        if let Some(ref mut file) = self.file {
            file.sync_data()?;
            self.last_fsync = Instant::now();
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WAL file handle is closed",
            ))
        }
    }

    /// Truncate WAL after snapshot completion.
    ///
    /// Flushes pending data, renames current WAL to `.wal.old` (backup),
    /// then opens a fresh WAL file with v2 header. Resets counters and updates epoch.
    pub fn truncate_after_snapshot(&mut self, new_epoch: u64) -> std::io::Result<()> {
        // Flush any pending data first
        if !self.buf.is_empty() {
            self.flush_sync()?;
        }

        // Close current file
        self.file.take();

        // Rename current WAL to .wal.old (overwrites previous backup)
        let old_path = self.file_path.with_extension("wal.old");
        if self.file_path.exists() {
            std::fs::rename(&self.file_path, &old_path)?;
        }

        // Open a fresh WAL file
        let file = TokioFileIo::open_append(&self.file_path)?;
        self.file = Some(file);

        // Reset counters
        self.write_offset = 0;
        self.bytes_written = 0;
        self.epoch = new_epoch;
        self.cmd_count = 0;
        self.header_written = false;

        // Write v2 header on the fresh file
        self.write_header()?;

        info!(
            "WAL truncated for shard {} at epoch {}",
            self.shard_id, new_epoch
        );
        Ok(())
    }

    /// Graceful shutdown: flush pending data, fsync, close file handle.
    pub fn shutdown(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            self.flush_sync()?;
        }
        // Drop file handle explicitly
        self.file.take();
        info!("WAL writer for shard {} shut down", self.shard_id);
        Ok(())
    }

    /// Returns the path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.file_path
    }

    /// Returns total bytes written since last truncation.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Replay a per-shard WAL file into the given databases.
///
/// Uses the same RESP parsing and command dispatch as `aof::replay_aof`.
/// Returns the number of commands successfully replayed.
///
/// NOTE: This replay function only understands v1 (raw RESP) format.
/// Plan 02 will implement v2-aware replay that understands block framing.
pub fn replay_wal(databases: &mut [Database], path: &Path) -> anyhow::Result<usize> {
    // Delegate to aof::replay_aof since the format is identical (RESP wire format)
    crate::persistence::aof::replay_aof(databases, path)
}

/// Construct the WAL file path for a given shard.
pub fn wal_path(dir: &Path, shard_id: usize) -> PathBuf {
    dir.join(format!("shard-{}.wal", shard_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use tempfile::tempdir;

    use crate::persistence::aof::serialize_command;
    use crate::protocol::Frame;
    use crate::protocol::serialize;

    fn make_command(parts: &[&[u8]]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
                .collect(),
        )
    }

    fn serialize_to_bytes(frame: &Frame) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize::serialize(frame, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn test_wal_append_and_flush() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        let cmd = make_command(&[b"SET", b"k", b"v"]);
        let data = serialize_to_bytes(&cmd);

        writer.append(&data);
        writer.flush_if_needed().unwrap();

        // Read file back -- v2 format: header(32) + block frame
        let contents = std::fs::read(writer.path()).unwrap();
        // File starts with RRDWAL header
        assert_eq!(&contents[0..6], b"RRDWAL");
        assert_eq!(contents[6], WAL_VERSION);
        // After header, the block contains the RESP payload
        let block_start = WAL_HEADER_SIZE;
        let block_len = u32::from_le_bytes(contents[block_start..block_start + 4].try_into().unwrap());
        // Extract payload from block: skip block_len(4) + cmd_count(2) + db_idx(1) = 7 bytes
        let payload_start = block_start + 7;
        let payload_len = block_len as usize - 2 - 1 - 4; // subtract cmd_count + db_idx + crc
        let payload = &contents[payload_start..payload_start + payload_len];
        assert_eq!(payload, &data[..]);
    }

    #[test]
    #[ignore] // TODO: Plan 02 implements v2 replay
    fn test_wal_replay_round_trip() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        // Append multiple SET commands via serialize_command
        let cmd1 = make_command(&[b"SET", b"key1", b"val1"]);
        let cmd2 = make_command(&[b"SET", b"key2", b"val2"]);
        let cmd3 = make_command(&[b"SET", b"key3", b"val3"]);

        writer.append(&serialize_command(&cmd1));
        writer.append(&serialize_command(&cmd2));
        writer.append(&serialize_command(&cmd3));
        writer.flush_sync().unwrap();

        // Replay into databases
        let mut dbs = vec![Database::new()];
        let count = replay_wal(&mut dbs, writer.path()).unwrap();
        assert_eq!(count, 3);

        assert_eq!(dbs[0].get(b"key1").unwrap().value.as_bytes().unwrap(), b"val1");
        assert_eq!(dbs[0].get(b"key2").unwrap().value.as_bytes().unwrap(), b"val2");
        assert_eq!(dbs[0].get(b"key3").unwrap().value.as_bytes().unwrap(), b"val3");
    }

    #[test]
    fn test_wal_truncate_after_snapshot() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        // Write some data
        let cmd = make_command(&[b"SET", b"k", b"v"]);
        let data = serialize_to_bytes(&cmd);
        writer.append(&data);
        writer.flush_sync().unwrap();

        // Verify data was written (header + block)
        let contents_before = std::fs::read(writer.path()).unwrap();
        assert!(!contents_before.is_empty());
        assert_eq!(&contents_before[0..6], b"RRDWAL");

        // Truncate
        writer.truncate_after_snapshot(1).unwrap();

        // New WAL file should have just the v2 header (32 bytes)
        let contents_after = std::fs::read(writer.path()).unwrap();
        assert_eq!(contents_after.len(), WAL_HEADER_SIZE);
        assert_eq!(&contents_after[0..6], b"RRDWAL");
        // Epoch in the new header should be 1
        let epoch = u64::from_le_bytes(contents_after[9..17].try_into().unwrap());
        assert_eq!(epoch, 1);

        // Old data should be in .wal.old
        let old_path = writer.path().with_extension("wal.old");
        let old_contents = std::fs::read(&old_path).unwrap();
        assert_eq!(&old_contents[0..6], b"RRDWAL");

        // bytes_written includes header bytes
        assert_eq!(writer.bytes_written(), WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_wal_flush_empty_is_noop() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        // Flush on empty buffer should return Ok without any block I/O
        writer.flush_if_needed().unwrap();
        writer.flush_sync().unwrap();

        // File should contain only the header (written in new())
        let contents = std::fs::read(writer.path()).unwrap();
        assert_eq!(contents.len(), WAL_HEADER_SIZE);
        assert_eq!(&contents[0..6], b"RRDWAL");
    }

    #[test]
    fn test_wal_multiple_appends_batched() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        // Append 100 small commands
        let mut expected_payload = Vec::new();
        for i in 0..100 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            let cmd = make_command(&[b"SET", key.as_bytes(), val.as_bytes()]);
            let data = serialize_to_bytes(&cmd);
            expected_payload.extend_from_slice(&data);
            writer.append(&data);
        }

        // Single flush
        writer.flush_sync().unwrap();

        // File starts with header, then a single block containing all 100 commands
        let contents = std::fs::read(writer.path()).unwrap();
        assert_eq!(&contents[0..6], b"RRDWAL");

        // Parse the block
        let block_start = WAL_HEADER_SIZE;
        let block_len = u32::from_le_bytes(contents[block_start..block_start + 4].try_into().unwrap());
        let cmd_count = u16::from_le_bytes(contents[block_start + 4..block_start + 6].try_into().unwrap());
        assert_eq!(cmd_count, 100);

        // Extract payload
        let payload_start = block_start + 7;
        let payload_len = block_len as usize - 2 - 1 - 4;
        let payload = &contents[payload_start..payload_start + payload_len];
        assert_eq!(payload, &expected_payload[..]);

        assert!(writer.bytes_written() > 0);
    }

    #[test]
    fn test_wal_path_construction() {
        let dir = Path::new("/tmp/wal");
        assert_eq!(wal_path(dir, 0), PathBuf::from("/tmp/wal/shard-0.wal"));
        assert_eq!(wal_path(dir, 3), PathBuf::from("/tmp/wal/shard-3.wal"));
        assert_eq!(wal_path(dir, 15), PathBuf::from("/tmp/wal/shard-15.wal"));
    }

    #[test]
    #[ignore] // TODO: Plan 02 implements v2 replay
    fn test_wal_replay_with_collections() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        // HSET
        let hset = make_command(&[b"HSET", b"myhash", b"f1", b"v1"]);
        writer.append(&serialize_command(&hset));

        // LPUSH
        let lpush = make_command(&[b"LPUSH", b"mylist", b"a", b"b"]);
        writer.append(&serialize_command(&lpush));

        // SADD
        let sadd = make_command(&[b"SADD", b"myset", b"x", b"y"]);
        writer.append(&serialize_command(&sadd));

        // ZADD
        let zadd = make_command(&[b"ZADD", b"myzset", b"1.5", b"alice"]);
        writer.append(&serialize_command(&zadd));

        writer.flush_sync().unwrap();

        // Replay
        let mut dbs = vec![Database::new()];
        let count = replay_wal(&mut dbs, writer.path()).unwrap();
        assert_eq!(count, 4);

        // Verify hash
        let hash = dbs[0].get_hash(b"myhash").unwrap().unwrap();
        assert_eq!(hash.get(&Bytes::from_static(b"f1")).unwrap().as_ref(), b"v1");

        // Verify list
        let list = dbs[0].get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list.len(), 2);

        // Verify set
        let set = dbs[0].get_set(b"myset").unwrap().unwrap();
        assert_eq!(set.len(), 2);

        // Verify sorted set
        let (members, _) = dbs[0].get_sorted_set(b"myzset").unwrap().unwrap();
        assert_eq!(*members.get(&Bytes::from_static(b"alice")).unwrap(), 1.5);
    }

    #[test]
    fn test_wal_v2_header_format() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(3, dir.path()).unwrap();

        // Append and flush one command to ensure file has content beyond header
        let cmd = make_command(&[b"SET", b"x", b"y"]);
        let data = serialize_to_bytes(&cmd);
        writer.append(&data);
        writer.flush_if_needed().unwrap();

        let contents = std::fs::read(writer.path()).unwrap();
        assert!(contents.len() >= WAL_HEADER_SIZE);

        // Verify header fields
        assert_eq!(&contents[0..6], b"RRDWAL", "magic bytes");
        assert_eq!(contents[6], 2, "version");
        let shard_id = u16::from_le_bytes(contents[7..9].try_into().unwrap());
        assert_eq!(shard_id, 3, "shard_id");
        let epoch = u64::from_le_bytes(contents[9..17].try_into().unwrap());
        assert_eq!(epoch, 0, "epoch");
        // Reserved bytes should all be zero
        assert_eq!(&contents[17..32], &[0u8; 15], "reserved bytes");
    }

    #[test]
    fn test_wal_v2_block_crc_valid() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        let cmd = make_command(&[b"SET", b"mykey", b"myval"]);
        let data = serialize_to_bytes(&cmd);
        writer.append(&data);
        writer.flush_if_needed().unwrap();

        let contents = std::fs::read(writer.path()).unwrap();

        // Parse block after 32-byte header
        let bs = WAL_HEADER_SIZE;
        let block_len = u32::from_le_bytes(contents[bs..bs + 4].try_into().unwrap());
        let cmd_count_bytes = &contents[bs + 4..bs + 6];
        let cmd_count = u16::from_le_bytes(cmd_count_bytes.try_into().unwrap());
        assert_eq!(cmd_count, 1);
        let db_idx = contents[bs + 6];
        assert_eq!(db_idx, 0);

        // Extract payload and stored CRC
        let payload_len = block_len as usize - 2 - 1 - 4;
        let payload = &contents[bs + 7..bs + 7 + payload_len];
        let crc_offset = bs + 7 + payload_len;
        let stored_crc = u32::from_le_bytes(contents[crc_offset..crc_offset + 4].try_into().unwrap());

        // Recompute CRC over cmd_count + db_idx + payload
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(cmd_count_bytes);
        hasher.update(&[db_idx]);
        hasher.update(payload);
        let computed_crc = hasher.finalize();

        assert_eq!(stored_crc, computed_crc, "CRC32 checksum mismatch");

        // Verify payload is the original RESP data
        assert_eq!(payload, &data[..]);
    }
}
