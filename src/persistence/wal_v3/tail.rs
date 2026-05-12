//! WAL v3 tail-reader — synchronous pull-based iterator over committed
//! records. The foundation of CDC, replication tailers, and any other
//! downstream consumer that wants to follow the WAL live.
//!
//! Design:
//! - **Pull, not push.** Callers invoke `read_next()` in a loop. When no
//!   new bytes are durable yet, the reader returns `Ok(None)` — the caller
//!   decides how to back off (poll interval, runtime-specific wait).
//! - **Cursor-resumable.** `TailCursor` snapshots `(segment_seq, byte_offset,
//!   last_lsn)`. A fresh reader constructed from a saved cursor picks up
//!   exactly where the previous one left off, even across process restarts.
//! - **Torn-write safe.** Each call re-reads the file metadata to learn the
//!   current durable length. Records straddling the durable tail are
//!   treated as "not ready yet" and return `Ok(None)`.
//! - **Rotation aware.** When the current segment runs dry but a higher
//!   sequence number exists on disk, the cursor advances to the next
//!   segment automatically.
//!
//! Why sync? CDC fan-out runs inside the shard event loop where every async
//! point costs a runtime hop. A sync iterator with explicit poll-or-yield
//! semantics maps cleanly onto both monoio and tokio without dragging in
//! runtime-specific dependencies here.

use std::fs;
use std::path::{Path, PathBuf};

use super::record::{WalRecord, read_wal_v3_record};
use super::segment::{WAL_V3_HEADER_SIZE, WAL_V3_MAGIC, WAL_V3_VERSION, WalSegment};

/// Resumable position inside a per-shard WAL stream.
///
/// Construct with `TailCursor::start()` for a fresh tailer, or persist this
/// struct (e.g. JSON-serialize the three fields) and rebuild it later for a
/// resumable consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailCursor {
    /// Sequence number of the segment we're currently reading.
    pub segment_seq: u64,
    /// Byte offset into that segment. Always points at the start of a
    /// record (or the next record boundary after the last yielded one).
    pub byte_offset: u64,
    /// LSN of the most recently yielded record (0 if no record yielded yet).
    pub last_lsn: u64,
}

impl TailCursor {
    /// Cursor positioned at the very beginning of segment 1.
    ///
    /// Records start after the 64-byte segment header.
    #[inline]
    pub const fn start() -> Self {
        Self {
            segment_seq: 1,
            byte_offset: WAL_V3_HEADER_SIZE as u64,
            last_lsn: 0,
        }
    }

    /// Cursor positioned at the start of a specific segment.
    ///
    /// `byte_offset` is set to `WAL_V3_HEADER_SIZE` (right past the header).
    /// `last_lsn` is 0 until the first record is yielded.
    #[inline]
    pub const fn at_segment(segment_seq: u64) -> Self {
        Self {
            segment_seq,
            byte_offset: WAL_V3_HEADER_SIZE as u64,
            last_lsn: 0,
        }
    }
}

/// Pull-based WAL tail reader.
///
/// One instance per consumer. Not `Send` across threads concurrently — the
/// internal cursor mutates on every `read_next`; wrap in a mutex if shared
/// access is required.
pub struct WalTailReader {
    wal_dir: PathBuf,
    cursor: TailCursor,
}

impl WalTailReader {
    /// Create a tail reader starting from `cursor`.
    pub fn new(wal_dir: impl AsRef<Path>, cursor: TailCursor) -> Self {
        Self {
            wal_dir: wal_dir.as_ref().to_path_buf(),
            cursor,
        }
    }

    /// Create a tail reader from the beginning of segment 1.
    pub fn from_origin(wal_dir: impl AsRef<Path>) -> Self {
        Self::new(wal_dir, TailCursor::start())
    }

    /// Snapshot of the current cursor — persist this for resumption.
    #[inline]
    pub fn cursor(&self) -> TailCursor {
        self.cursor
    }

    /// Return the next durable record, advance the cursor, or `Ok(None)` if
    /// no new records are available yet.
    ///
    /// Semantics:
    /// - Yields `Some(record)` and advances the cursor past it.
    /// - Returns `Ok(None)` when the current segment has been read up to its
    ///   durable tail and no higher-sequence segment exists yet. The caller
    ///   should back off (sleep / yield) and call again.
    /// - On segment rotation (next segment exists), advances the cursor to
    ///   the new segment and continues seamlessly.
    /// - On a corrupt record at the tail, returns `Ok(None)` and leaves the
    ///   cursor at the corrupt boundary so the caller can detect the stall.
    ///   (We never silently skip over corrupt records — that would mask
    ///   real durability bugs.)
    pub fn read_next(&mut self) -> std::io::Result<Option<WalRecord>> {
        loop {
            let seg_path = WalSegment::segment_path(&self.wal_dir, self.cursor.segment_seq);
            let seg_meta = match fs::metadata(&seg_path) {
                Ok(m) => m,
                Err(_) => {
                    // Current segment doesn't exist. Look for any later
                    // segment that does — that lets us survive a recycled
                    // segment 1 after WAL truncation.
                    if let Some(next) = self.find_segment_after(self.cursor.segment_seq) {
                        self.cursor.segment_seq = next;
                        self.cursor.byte_offset = WAL_V3_HEADER_SIZE as u64;
                        continue;
                    }
                    return Ok(None);
                }
            };
            let durable_len = seg_meta.len();

            // Need at least 4 bytes for the length prefix.
            if self.cursor.byte_offset + 4 > durable_len {
                // No more data in the current segment. If a higher-sequence
                // segment exists, jump to it; otherwise return None.
                let next_seq = self.cursor.segment_seq + 1;
                if WalSegment::segment_path(&self.wal_dir, next_seq).exists() {
                    self.cursor.segment_seq = next_seq;
                    self.cursor.byte_offset = WAL_V3_HEADER_SIZE as u64;
                    continue;
                }
                return Ok(None);
            }

            // Header sanity check on the first read of a new segment — guards
            // against pointing the cursor at a non-WAL file.
            if self.cursor.byte_offset == WAL_V3_HEADER_SIZE as u64 {
                let mut hdr = [0u8; 7];
                let bytes_read = read_at(&seg_path, 0, &mut hdr)?;
                if bytes_read < 7 || &hdr[..6] != WAL_V3_MAGIC || hdr[6] != WAL_V3_VERSION {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "not a WAL v3 segment: {:?}",
                            seg_path.file_name().unwrap_or_default()
                        ),
                    ));
                }
            }

            // Read just enough to learn the record length.
            let mut len_buf = [0u8; 4];
            let n = read_at(&seg_path, self.cursor.byte_offset, &mut len_buf)?;
            if n < 4 {
                return Ok(None);
            }
            let record_len = u32::from_le_bytes(len_buf) as u64;
            if record_len == 0 {
                // Zero-padded tail — treat as "no record here yet".
                return Ok(None);
            }
            if self.cursor.byte_offset + record_len > durable_len {
                // Record length declared but the tail isn't durable yet.
                return Ok(None);
            }

            // Pull the full record into a buffer (records are small — bytes,
            // not pages — so this allocation is cheap and isolated).
            let mut buf = vec![0u8; record_len as usize];
            let n = read_at(&seg_path, self.cursor.byte_offset, &mut buf)?;
            if (n as u64) < record_len {
                return Ok(None);
            }
            let record = match read_wal_v3_record(&buf) {
                Some(r) => r,
                None => {
                    // Corrupt record — don't advance past it. Surface as
                    // "no data" so the caller can decide (panic, alert,
                    // skip-with-warning). Leaving the cursor pinned at the
                    // corrupt offset is the safe default.
                    return Ok(None);
                }
            };
            self.cursor.byte_offset += record_len;
            if record.lsn > self.cursor.last_lsn {
                self.cursor.last_lsn = record.lsn;
            }
            return Ok(Some(record));
        }
    }

    /// Find the smallest segment sequence strictly greater than `after_seq`.
    fn find_segment_after(&self, after_seq: u64) -> Option<u64> {
        let entries = fs::read_dir(&self.wal_dir).ok()?;
        let mut best: Option<u64> = None;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(stem) = name_str.strip_suffix(".wal") {
                if let Ok(seq) = stem.parse::<u64>() {
                    if seq > after_seq && best.map(|b| seq < b).unwrap_or(true) {
                        best = Some(seq);
                    }
                }
            }
        }
        best
    }
}

/// Pread helper — reads up to `buf.len()` bytes at `offset` from `path`.
/// Returns the number of bytes actually read (may be < buf.len() if the
/// file ended). Reuses a fresh `File` per call to avoid holding file
/// descriptors across `read_next` invocations; segments are small in number
/// and the kernel page cache absorbs the open cost.
fn read_at(path: &Path, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut total = 0;
    while total < buf.len() {
        match file.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::record::{WalRecordType, write_wal_v3_record};
    use super::super::segment::{DEFAULT_SEGMENT_SIZE, WalWriterV3};

    /// C1 — happy path: write 10 records, tail must yield all 10 in LSN order.
    #[test]
    fn test_tail_reads_appended_records_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");

        {
            let mut writer =
                WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();
            for _ in 0..10 {
                writer.append(WalRecordType::Command, b"SET k v");
            }
            writer.flush_sync().unwrap();
        }

        let mut tail = WalTailReader::from_origin(&wal_dir);
        let mut lsns = Vec::new();
        while let Some(rec) = tail.read_next().unwrap() {
            lsns.push(rec.lsn);
        }
        assert_eq!(lsns, (1..=10).collect::<Vec<_>>());
        // Subsequent call must return None (no new data).
        assert!(tail.read_next().unwrap().is_none());
    }

    /// C1 — segment rotation: writes that cross segment boundaries must be
    /// followed transparently. The tailer advances `segment_seq` itself.
    #[test]
    fn test_tail_advances_across_segment_rotation() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");

        let mut last_lsn = 0u64;
        {
            // Small segment forces rotation after a handful of records.
            let mut writer = WalWriterV3::new(0, &wal_dir, 512).unwrap();
            for i in 0..30 {
                last_lsn = writer.append(WalRecordType::Command, b"SET k v");
                if (i + 1) % 3 == 0 {
                    writer.flush_sync().unwrap();
                }
            }
            writer.flush_sync().unwrap();
            assert!(
                writer.current_segment_sequence() >= 2,
                "test expects rotation, got seq {}",
                writer.current_segment_sequence(),
            );
        }

        let mut tail = WalTailReader::from_origin(&wal_dir);
        let mut count = 0usize;
        let mut max_lsn = 0u64;
        while let Some(rec) = tail.read_next().unwrap() {
            count += 1;
            max_lsn = rec.lsn;
        }
        assert_eq!(count, 30, "tail must yield records from every segment");
        assert_eq!(max_lsn, last_lsn);
        // Cursor must have advanced past segment 1.
        assert!(tail.cursor().segment_seq >= 2);
    }

    /// C1 — cursor resume: saving and restoring the cursor must produce
    /// byte-identical streams for the second half.
    #[test]
    fn test_tail_resumes_from_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");

        {
            let mut writer =
                WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();
            for _ in 0..20 {
                writer.append(WalRecordType::Command, b"SET k v");
            }
            writer.flush_sync().unwrap();
        }

        // Read the first 10, snapshot cursor, drop reader.
        let saved_cursor = {
            let mut tail = WalTailReader::from_origin(&wal_dir);
            for _ in 0..10 {
                assert!(tail.read_next().unwrap().is_some());
            }
            tail.cursor()
        };
        assert_eq!(saved_cursor.last_lsn, 10);

        // Resume from the cursor — next 10 records should come out as LSNs 11..=20.
        let mut tail2 = WalTailReader::new(&wal_dir, saved_cursor);
        let mut resumed: Vec<u64> = Vec::new();
        while let Some(rec) = tail2.read_next().unwrap() {
            resumed.push(rec.lsn);
        }
        assert_eq!(resumed, (11..=20).collect::<Vec<_>>());
    }

    /// C1 — empty dir: tail must return None without erroring.
    #[test]
    fn test_tail_empty_dir_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        // Create dir but no segments.
        std::fs::create_dir_all(&wal_dir).unwrap();
        let mut tail = WalTailReader::from_origin(&wal_dir);
        assert!(tail.read_next().unwrap().is_none());
    }

    /// C1 — partial record at the tail: declared length exceeds file length.
    /// Reader must return None (wait for more data) and NOT advance the cursor
    /// past the partial record.
    #[test]
    fn test_tail_handles_partial_record_at_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Build a segment with one complete record + truncated trailer.
        let mut data: Vec<u8> = vec![0u8; WAL_V3_HEADER_SIZE];
        data[0..6].copy_from_slice(WAL_V3_MAGIC);
        data[6] = WAL_V3_VERSION;
        data[7] = 0x01;
        write_wal_v3_record(&mut data, 1, WalRecordType::Command, b"x");
        // Now write a partial record_len that claims 100 more bytes (file ends).
        data.extend_from_slice(&100u32.to_le_bytes());
        // (Intentionally omit the rest — that's the "torn write" we simulate.)
        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        std::fs::write(&seg_path, &data).unwrap();

        let mut tail = WalTailReader::from_origin(&wal_dir);
        let first = tail.read_next().unwrap().expect("first record present");
        assert_eq!(first.lsn, 1);
        // Second call sees the partial record_len trailer -> wait, not advance.
        let cursor_before = tail.cursor();
        assert!(tail.read_next().unwrap().is_none());
        let cursor_after = tail.cursor();
        assert_eq!(
            cursor_before, cursor_after,
            "cursor must not advance past a partial record at the tail",
        );
    }
}
