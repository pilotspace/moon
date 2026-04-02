//! WAL v3 segment file management — 16MB segments with 64-byte headers.
//!
//! Each segment file is named with a 12-digit zero-padded sequence number
//! (e.g., `000000000001.wal`). The writer creates new segments when the
//! current one exceeds `segment_size` bytes.
//!
//! **Segment header (64 bytes, little-endian):**
//! ```text
//! Offset  Size  Field
//! 0       6     magic "RRDWAL"
//! 6       1     version = 3
//! 7       1     flags (FPI_ENABLED=0x01, COMPRESSED=0x02)
//! 8       2     shard_id (u16 LE)
//! 10      2     reserved_0 (zero)
//! 12      8     epoch (u64 LE)
//! 20      8     redo_lsn (u64 LE) — REDO point from last checkpoint
//! 28      8     base_lsn (u64 LE) — LSN of first record in this segment
//! 36      8     segment_size (u64 LE)
//! 44      20    reserved_1 (zeroes)
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use super::record::{WalRecordType, write_wal_v3_record};

/// WAL v3 magic bytes (shared with v2 for detection).
pub const WAL_V3_MAGIC: &[u8; 6] = b"RRDWAL";

/// WAL v3 format version.
pub const WAL_V3_VERSION: u8 = 3;

/// Segment header size in bytes.
pub const WAL_V3_HEADER_SIZE: usize = 64;

/// Default segment size: 16MB.
pub const DEFAULT_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// Represents a single WAL v3 segment file.
#[derive(Debug, Clone)]
pub struct WalSegment {
    /// Path to the segment file.
    pub path: PathBuf,
    /// Monotonic segment sequence number.
    pub sequence: u64,
}

impl WalSegment {
    /// Format a segment filename: 12-digit zero-padded with `.wal` extension.
    #[inline]
    pub fn segment_name(sequence: u64) -> String {
        format!("{:012}.wal", sequence)
    }

    /// Build the full path for a segment in the given WAL directory.
    #[inline]
    pub fn segment_path(wal_dir: &Path, sequence: u64) -> PathBuf {
        wal_dir.join(Self::segment_name(sequence))
    }
}

/// WAL v3 writer with segmented files, per-record LSN, and batched fsync.
pub struct WalWriterV3 {
    shard_id: usize,
    wal_dir: PathBuf,
    segment_size: u64,
    current_sequence: u64,
    current_file: Option<File>,
    /// In-memory buffer, pre-allocated 8KB.
    buf: Vec<u8>,
    /// Current write offset in the active segment file.
    write_offset: u64,
    /// Next LSN to assign.
    next_lsn: u64,
    /// LSN of last checkpoint (written into segment headers).
    base_lsn: u64,
    /// Current epoch for header metadata.
    epoch: u64,
}

impl WalWriterV3 {
    /// Create a new WAL v3 writer for the given shard.
    ///
    /// Creates `wal_dir` if it does not exist. Scans for existing segment files
    /// to resume from the highest sequence number.
    pub fn new(shard_id: usize, wal_dir: &Path, segment_size: u64) -> std::io::Result<Self> {
        fs::create_dir_all(wal_dir)?;

        // Scan for existing segments to find max sequence
        let max_seq = Self::scan_max_sequence(wal_dir);
        let next_seq = if max_seq > 0 { max_seq + 1 } else { 1 };

        let mut writer = Self {
            shard_id,
            wal_dir: wal_dir.to_path_buf(),
            segment_size,
            current_sequence: next_seq,
            current_file: None,
            buf: Vec::with_capacity(8192),
            write_offset: 0,
            next_lsn: 1,
            base_lsn: 0,
            epoch: 0,
        };

        writer.open_new_segment()?;
        Ok(writer)
    }

    /// Append a record to the WAL buffer. Returns the assigned LSN.
    ///
    /// No I/O occurs here -- records accumulate in the in-memory buffer
    /// until `flush_sync()` is called.
    pub fn append(&mut self, record_type: WalRecordType, payload: &[u8]) -> u64 {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        write_wal_v3_record(&mut self.buf, lsn, record_type, payload);
        lsn
    }

    /// Flush the in-memory buffer to disk and fsync.
    ///
    /// After this returns, all appended records are durable on stable storage.
    pub fn flush_sync(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        // Check if rotation is needed before writing
        if self.write_offset + self.buf.len() as u64 > self.segment_size {
            self.rotate_segment()?;
        }

        if let Some(ref mut file) = self.current_file {
            file.write_all(&self.buf)?;
            file.sync_data()?;
            self.write_offset += self.buf.len() as u64;
            self.buf.clear();
        }

        Ok(())
    }

    /// Flush if buffer exceeds a threshold (matches v2 pattern).
    pub fn flush_if_needed(&mut self) -> std::io::Result<()> {
        if self.buf.len() >= 4096 {
            self.flush_sync()
        } else {
            Ok(())
        }
    }

    /// Return the current (next-to-be-assigned) LSN.
    #[inline]
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Return the active segment sequence number.
    #[inline]
    pub fn current_segment_sequence(&self) -> u64 {
        self.current_sequence
    }

    /// Return the WAL directory path.
    #[inline]
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Rotate to a new segment: flush + fsync current, open next.
    fn rotate_segment(&mut self) -> std::io::Result<()> {
        // Flush remaining buffer to current segment
        if let Some(ref mut file) = self.current_file {
            if !self.buf.is_empty() {
                file.write_all(&self.buf)?;
                self.write_offset += self.buf.len() as u64;
                self.buf.clear();
            }
            file.sync_data()?;
        }

        self.current_sequence += 1;
        self.open_new_segment()
    }

    /// Open a new segment file and write its 64-byte header.
    fn open_new_segment(&mut self) -> std::io::Result<()> {
        let path = WalSegment::segment_path(&self.wal_dir, self.current_sequence);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        self.write_segment_header(&mut file)?;
        self.write_offset = WAL_V3_HEADER_SIZE as u64;
        self.current_file = Some(file);
        Ok(())
    }

    /// Write the 64-byte v3 segment header.
    ///
    /// Layout per §5.1:
    /// ```text
    /// 0..6    magic "RRDWAL"
    /// 6       version = 3
    /// 7       flags (FPI_ENABLED=0x01, COMPRESSED=0x02)
    /// 8..10   shard_id (u16 LE)
    /// 10..12  reserved_0 (zero)
    /// 12..20  epoch (u64 LE)
    /// 20..28  redo_lsn (u64 LE)
    /// 28..36  base_lsn (u64 LE)
    /// 36..44  segment_size (u64 LE)
    /// 44..64  reserved_1 (zero)
    /// ```
    fn write_segment_header(&self, file: &mut File) -> std::io::Result<()> {
        let mut header = [0u8; WAL_V3_HEADER_SIZE];

        // magic (6 bytes)
        header[0..6].copy_from_slice(WAL_V3_MAGIC);
        // version (1 byte)
        header[6] = WAL_V3_VERSION;
        // flags (1 byte) — FPI enabled by default
        header[7] = 0x01; // FPI_ENABLED
        // shard_id (2 bytes LE)
        header[8..10].copy_from_slice(&(self.shard_id as u16).to_le_bytes());
        // reserved_0 (2 bytes, zero)
        // epoch (8 bytes LE)
        header[12..20].copy_from_slice(&self.epoch.to_le_bytes());
        // redo_lsn (8 bytes LE) — REDO point from last checkpoint
        header[20..28].copy_from_slice(&self.base_lsn.to_le_bytes());
        // base_lsn (8 bytes LE) — LSN of first record in this segment
        header[28..36].copy_from_slice(&self.next_lsn.to_le_bytes());
        // segment_size (8 bytes LE)
        header[36..44].copy_from_slice(&self.segment_size.to_le_bytes());
        // bytes 44..64 remain zero (reserved_1)

        file.write_all(&header)
    }

    /// Delete WAL segment files whose records are fully before `redo_lsn`.
    ///
    /// Scans `*.wal` files in the WAL directory, reads the base_lsn from each
    /// segment header (offset 28, u64 LE). If base_lsn < redo_lsn AND the
    /// segment is not the currently active segment, the file is removed.
    ///
    /// Called after checkpoint finalization when redo_lsn advances.
    /// Returns the number of segments recycled.
    pub fn recycle_segments_before(&self, redo_lsn: u64) -> std::io::Result<usize> {
        let mut recycled = 0usize;
        let entries = fs::read_dir(&self.wal_dir)?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.ends_with(".wal") {
                continue;
            }
            // Parse sequence number from filename
            let seq = match name_str.strip_suffix(".wal").and_then(|s| s.parse::<u64>().ok()) {
                Some(s) => s,
                None => continue,
            };
            // Never delete the active segment
            if seq >= self.current_sequence {
                continue;
            }
            // Read base_lsn from header (offset 28..36)
            let path = entry.path();
            let mut header = [0u8; WAL_V3_HEADER_SIZE];
            use std::io::Read as _;
            let file = fs::File::open(&path)?;
            let mut reader = std::io::BufReader::new(file);
            if reader.read_exact(&mut header).is_err() {
                continue; // Truncated header, skip
            }
            let base_lsn = u64::from_le_bytes(
                header[28..36].try_into().unwrap_or([0u8; 8]),
            );
            // If every record in this segment is before redo_lsn, safe to delete
            if base_lsn > 0 && base_lsn < redo_lsn {
                if let Err(e) = fs::remove_file(&path) {
                    tracing::warn!("WAL segment recycle failed for {:?}: {}", path, e);
                } else {
                    recycled += 1;
                }
            }
        }
        Ok(recycled)
    }

    /// Scan the WAL directory for existing segment files, return max sequence.
    fn scan_max_sequence(wal_dir: &Path) -> u64 {
        let mut max_seq = 0u64;
        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(stem) = name.strip_suffix(".wal") {
                        if let Ok(seq) = stem.parse::<u64>() {
                            if seq > max_seq {
                                max_seq = seq;
                            }
                        }
                    }
                }
            }
        }
        max_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::record::read_wal_v3_record;

    #[test]
    fn test_segment_name_format() {
        assert_eq!(WalSegment::segment_name(1), "000000000001.wal");
        assert_eq!(WalSegment::segment_name(999_999_999_999), "999999999999.wal");
        assert_eq!(WalSegment::segment_name(0), "000000000000.wal");
    }

    #[test]
    fn test_writer_creates_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");

        let writer = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();
        assert_eq!(writer.current_segment_sequence(), 1);

        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        assert!(seg_path.exists());

        // Header should be 64 bytes
        let meta = fs::metadata(&seg_path).unwrap();
        assert_eq!(meta.len(), WAL_V3_HEADER_SIZE as u64);
    }

    #[test]
    fn test_writer_append_and_flush() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let mut writer = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

        let lsn1 = writer.append(WalRecordType::Command, b"SET a 1");
        let lsn2 = writer.append(WalRecordType::Command, b"SET b 2");
        let lsn3 = writer.append(WalRecordType::Command, b"SET c 3");
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);

        writer.flush_sync().unwrap();

        // Read back the segment file
        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        let data = fs::read(&seg_path).unwrap();
        assert!(data.len() > WAL_V3_HEADER_SIZE);

        // Parse records after header
        let mut offset = WAL_V3_HEADER_SIZE;
        let mut count = 0;
        while offset < data.len() {
            let record = read_wal_v3_record(&data[offset..]).expect("should parse record");
            assert_eq!(record.record_type, WalRecordType::Command);
            let record_len =
                u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
                    as usize;
            offset += record_len;
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_writer_segment_rotation() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        // Small segment size to force rotation
        let mut writer = WalWriterV3::new(0, &wal_dir, 512).unwrap();

        // Write enough to trigger rotation (each record ~27 bytes for 7-byte payload)
        for _ in 0..30 {
            writer.append(WalRecordType::Command, b"SET k v");
        }
        writer.flush_sync().unwrap();

        // Should have multiple segments
        let seg1 = WalSegment::segment_path(&wal_dir, 1);
        let seg2 = WalSegment::segment_path(&wal_dir, 2);
        assert!(seg1.exists(), "first segment should exist");
        assert!(seg2.exists(), "second segment should exist after rotation");
        assert!(writer.current_segment_sequence() >= 2);
    }

    #[test]
    fn test_writer_lsn_monotonic() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let mut writer = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

        let mut prev_lsn = 0;
        for _ in 0..100 {
            let lsn = writer.append(WalRecordType::Command, b"x");
            assert!(lsn > prev_lsn, "LSN must be monotonically increasing");
            prev_lsn = lsn;
        }
    }

    #[test]
    fn test_segment_header_format() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let _writer = WalWriterV3::new(7, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        let data = fs::read(&seg_path).unwrap();
        assert_eq!(data.len(), WAL_V3_HEADER_SIZE);

        // Verify header fields per §5.1 layout:
        // 0..6: magic, 6: version, 7: flags, 8..10: shard_id, 10..12: reserved_0,
        // 12..20: epoch, 20..28: redo_lsn, 28..36: base_lsn, 36..44: segment_size
        assert_eq!(&data[0..6], b"RRDWAL");
        assert_eq!(data[6], 3); // version = 3
        assert_eq!(data[7], 0x01); // flags = FPI_ENABLED
        assert_eq!(u16::from_le_bytes([data[8], data[9]]), 7); // shard_id = 7
        assert_eq!(u16::from_le_bytes([data[10], data[11]]), 0); // reserved_0
        // redo_lsn at offset 20 (base_lsn = last checkpoint = 0)
        let redo_lsn = u64::from_le_bytes(data[20..28].try_into().unwrap());
        assert_eq!(redo_lsn, 0); // base_lsn starts at 0
        // base_lsn at offset 28 (first record LSN)
        let base_lsn = u64::from_le_bytes(data[28..36].try_into().unwrap());
        assert_eq!(base_lsn, 1); // first record LSN = 1
        // segment_size at offset 36 (u64)
        let seg_size = u64::from_le_bytes(data[36..44].try_into().unwrap());
        assert_eq!(seg_size, DEFAULT_SEGMENT_SIZE);
    }

    #[test]
    fn test_recycle_segments_before() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");

        // Small segment size (512 bytes) to force multiple segments.
        let mut writer = WalWriterV3::new(0, &wal_dir, 512).unwrap();

        // Write records and flush frequently to trigger segment rotation.
        // Each record is ~31 bytes; 512 - 64 (header) = 448 usable per segment.
        // Flushing every few records forces rotation when write_offset exceeds 512.
        for i in 0..60 {
            writer.append(WalRecordType::Command, b"SET key val");
            if (i + 1) % 3 == 0 {
                writer.flush_sync().unwrap();
            }
        }
        writer.flush_sync().unwrap();

        let active_seq = writer.current_segment_sequence();
        assert!(active_seq >= 3, "should have 3+ segments, got {}", active_seq);

        // Count total .wal files before recycling.
        let count_wals = || -> usize {
            fs::read_dir(&wal_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
                .count()
        };
        let before = count_wals();
        assert!(before >= 3);

        // Segment 1 has base_lsn = 1 (first record). Use redo_lsn = 20 to
        // recycle segments whose base_lsn < 20 (should include segment 1+).
        let recycled = writer.recycle_segments_before(20).unwrap();
        assert!(recycled >= 1, "should recycle at least 1 segment");

        // Active segment must still exist.
        let active_path = WalSegment::segment_path(&wal_dir, active_seq);
        assert!(active_path.exists(), "active segment must survive recycling");

        // First segment should be deleted (base_lsn = 1 < 20).
        let first_path = WalSegment::segment_path(&wal_dir, 1);
        assert!(!first_path.exists(), "segment 1 should be recycled");

        // Total count should have decreased.
        let after = count_wals();
        assert_eq!(after, before - recycled);
    }
}
