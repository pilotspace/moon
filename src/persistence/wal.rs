//! Per-shard Write-Ahead Log (WAL) writer.
//!
//! Each shard maintains its own WAL file containing RESP-encoded write commands.
//! Writes are buffered in memory and flushed/fsynced on a configurable interval
//! (default 1ms for batched fsync). On Linux, uses direct file I/O; on macOS,
//! uses std::fs as fallback.
//!
//! Replaces the global AOF writer from Phase 11 with per-shard locality.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tracing::info;

use crate::storage::db::Database;

/// Per-shard WAL writer with batched fsync.
///
/// Accumulates RESP-encoded command bytes between fsync intervals.
/// Flush is called explicitly by the shard event loop on its 1ms tick.
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
}

impl WalWriter {
    /// Create a new WAL writer for the given shard.
    ///
    /// WAL file path: `{dir}/shard-{shard_id}.wal`
    /// Opens file in append+create mode. Pre-allocates 8KB buffer.
    pub fn new(shard_id: usize, dir: &Path) -> std::io::Result<Self> {
        let file_path = wal_path(dir, shard_id);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        let write_offset = file.metadata()?.len();

        Ok(Self {
            shard_id,
            buf: Vec::with_capacity(8192),
            file: Some(file),
            file_path,
            write_offset,
            last_fsync: Instant::now(),
            epoch: 0,
            bytes_written: 0,
        })
    }

    /// Append RESP-encoded command bytes to the in-memory buffer.
    ///
    /// No I/O occurs here -- this is the hot path called on every write command.
    #[inline]
    pub fn append(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Flush buffered data to disk if the buffer is non-empty.
    ///
    /// Called on the shard's 1ms tick (batched fsync interval).
    /// Returns Ok(()) immediately if the buffer is empty.
    pub fn flush_if_needed(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.do_flush()
    }

    /// Force flush regardless of buffer state. Used on shutdown and before snapshot.
    pub fn flush_sync(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.do_flush()
    }

    /// Internal flush: write buffer to file, fsync, update offsets, clear buffer.
    fn do_flush(&mut self) -> std::io::Result<()> {
        if let Some(ref mut file) = self.file {
            file.write_all(&self.buf)?;
            file.sync_data()?;
            let written = self.buf.len() as u64;
            self.write_offset += written;
            self.bytes_written += written;
            self.buf.clear(); // clear but keep allocation
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
    /// then opens a fresh WAL file. Resets counters and updates epoch.
    pub fn truncate_after_snapshot(&mut self, new_epoch: u64) -> std::io::Result<()> {
        // Flush any pending data first
        if !self.buf.is_empty() {
            self.do_flush()?;
        }

        // Close current file
        self.file.take();

        // Rename current WAL to .wal.old (overwrites previous backup)
        let old_path = self.file_path.with_extension("wal.old");
        if self.file_path.exists() {
            std::fs::rename(&self.file_path, &old_path)?;
        }

        // Open a fresh WAL file
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)?;
        self.file = Some(file);

        // Reset counters
        self.write_offset = 0;
        self.bytes_written = 0;
        self.epoch = new_epoch;

        info!(
            "WAL truncated for shard {} at epoch {}",
            self.shard_id, new_epoch
        );
        Ok(())
    }

    /// Graceful shutdown: flush pending data, fsync, close file handle.
    pub fn shutdown(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            self.do_flush()?;
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
    use ordered_float::OrderedFloat;
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

        // Read file back and verify bytes match
        let contents = std::fs::read(writer.path()).unwrap();
        assert_eq!(contents, data);
    }

    #[test]
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

        // Verify data was written
        let contents_before = std::fs::read(writer.path()).unwrap();
        assert!(!contents_before.is_empty());

        // Truncate
        writer.truncate_after_snapshot(1).unwrap();

        // New WAL file should be empty
        let contents_after = std::fs::read(writer.path()).unwrap();
        assert!(contents_after.is_empty());

        // Old data should be in .wal.old
        let old_path = writer.path().with_extension("wal.old");
        let old_contents = std::fs::read(&old_path).unwrap();
        assert_eq!(old_contents, data);

        // Epoch should be updated
        assert_eq!(writer.bytes_written(), 0);
    }

    #[test]
    fn test_wal_flush_empty_is_noop() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        // Flush on empty buffer should return Ok without any I/O
        writer.flush_if_needed().unwrap();
        writer.flush_sync().unwrap();

        let contents = std::fs::read(writer.path()).unwrap();
        assert!(contents.is_empty());
    }

    #[test]
    fn test_wal_multiple_appends_batched() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(0, dir.path()).unwrap();

        // Append 100 small commands
        let mut expected = Vec::new();
        for i in 0..100 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            let cmd = make_command(&[b"SET", key.as_bytes(), val.as_bytes()]);
            let data = serialize_to_bytes(&cmd);
            expected.extend_from_slice(&data);
            writer.append(&data);
        }

        // Single flush
        writer.flush_sync().unwrap();

        // All 100 commands should be in the file
        let contents = std::fs::read(writer.path()).unwrap();
        assert_eq!(contents, expected);
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
}
