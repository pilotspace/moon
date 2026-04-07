//! Shard control file — the recovery entry point for each shard.
//!
//! A single 4KB page containing shard state, LSN positions, and UUID.
//! Written atomically (single-sector write + fsync) and verified on read
//! via CRC32C checksum.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::persistence::fsync::fsync_directory;
use crate::persistence::page::{MOONPAGE_HEADER_SIZE, MoonPageHeader, PAGE_4K, PageType};

/// Control file payload size: 1 + 8 + 8 + 8 + 8 + 8 + 16 = 57 bytes.
const CONTROL_PAYLOAD_SIZE: u32 = 57;

/// Shard operational state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShardState {
    /// Shard is running normally.
    Running = 1,
    /// Shard is in graceful shutdown.
    ShuttingDown = 2,
    /// Shard is replaying WAL (recovery mode).
    Recovery = 3,
    /// Shard crashed (detected on next startup).
    Crashed = 4,
}

impl ShardState {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Running),
            2 => Some(Self::ShuttingDown),
            3 => Some(Self::Recovery),
            4 => Some(Self::Crashed),
            _ => None,
        }
    }
}

/// Shard control file — persisted as a single 4KB MoonPage.
///
/// This is the first thing read during recovery to determine the shard's
/// last known state, checkpoint position, and WAL flush position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardControlFile {
    /// Current shard operational state.
    pub shard_state: ShardState,
    /// LSN of the last completed checkpoint.
    pub last_checkpoint_lsn: u64,
    /// Epoch counter for the last checkpoint (monotonically increasing).
    pub last_checkpoint_epoch: u64,
    /// LSN up to which the WAL has been durably flushed.
    pub wal_flush_lsn: u64,
    /// Next transaction ID to be assigned.
    pub next_txn_id: u64,
    /// Next page ID to be assigned.
    pub next_page_id: u64,
    /// Unique shard identifier (UUID bytes).
    pub shard_uuid: [u8; 16],
}

impl ShardControlFile {
    /// Create a new control file with Running state and all counters at zero.
    pub fn new(shard_uuid: [u8; 16]) -> Self {
        Self {
            shard_state: ShardState::Running,
            last_checkpoint_lsn: 0,
            last_checkpoint_epoch: 0,
            wal_flush_lsn: 0,
            next_txn_id: 0,
            next_page_id: 0,
            shard_uuid,
        }
    }

    /// Write the control file atomically to disk.
    ///
    /// Produces exactly 4096 bytes (one PAGE_4K). Uses the standard
    /// temp-file + fsync + `rename(2)` + parent-fsync sequence so that a
    /// crash mid-write cannot leave the canonical control file in a
    /// truncated or partial state. On Linux, `rename` over an existing file
    /// is atomic, and the parent-directory fsync makes the new directory
    /// entry durable.
    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        let mut buf = [0u8; PAGE_4K];

        // Build header
        let mut hdr = MoonPageHeader::new(PageType::ControlPage, 0, 0);
        hdr.payload_bytes = CONTROL_PAYLOAD_SIZE;
        hdr.write_to(&mut buf);

        // Write payload at offset 64
        let p = MOONPAGE_HEADER_SIZE;
        buf[p] = self.shard_state as u8;
        buf[p + 1..p + 9].copy_from_slice(&self.last_checkpoint_lsn.to_le_bytes());
        buf[p + 9..p + 17].copy_from_slice(&self.last_checkpoint_epoch.to_le_bytes());
        buf[p + 17..p + 25].copy_from_slice(&self.wal_flush_lsn.to_le_bytes());
        buf[p + 25..p + 33].copy_from_slice(&self.next_txn_id.to_le_bytes());
        buf[p + 33..p + 41].copy_from_slice(&self.next_page_id.to_le_bytes());
        buf[p + 41..p + 57].copy_from_slice(&self.shard_uuid);

        // Compute CRC32C over payload and embed in header
        MoonPageHeader::compute_checksum(&mut buf);

        // 1. Write to a temp sibling file and fsync its data.
        let tmp_path = control_tmp_path(path);
        {
            let mut tmp = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp.write_all(&buf)?;
            tmp.sync_data()?;
        }

        // 2. Atomic rename over the canonical path.
        std::fs::rename(&tmp_path, path)?;

        // 3. Fsync the parent directory so the new dirent is durable.
        if let Some(parent) = path.parent() {
            fsync_directory(parent)?;
        }

        Ok(())
    }

    /// Read and verify a control file from disk.
    ///
    /// Returns an error if:
    /// - File doesn't exist or can't be read
    /// - File is smaller than 4096 bytes
    /// - Magic mismatch or page_type != Control
    /// - CRC32C verification fails
    pub fn read(path: &Path) -> std::io::Result<Self> {
        let buf = std::fs::read(path)?;

        if buf.len() < PAGE_4K {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "control file too small: {} bytes, expected {}",
                    buf.len(),
                    PAGE_4K
                ),
            ));
        }

        // Verify header
        let hdr = MoonPageHeader::read_from(&buf).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid MoonPage header (magic mismatch or bad page_type)",
            )
        })?;

        if hdr.page_type != PageType::ControlPage {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected Control page type, got {:?}", hdr.page_type),
            ));
        }

        // Verify CRC32C
        if !MoonPageHeader::verify_checksum(&buf) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "control file CRC32C checksum mismatch",
            ));
        }

        // Parse payload
        let p = MOONPAGE_HEADER_SIZE;
        let shard_state = ShardState::from_u8(buf[p]).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid shard state: {}", buf[p]),
            )
        })?;

        let last_checkpoint_lsn = u64::from_le_bytes(buf[p + 1..p + 9].try_into().unwrap());
        let last_checkpoint_epoch = u64::from_le_bytes(buf[p + 9..p + 17].try_into().unwrap());
        let wal_flush_lsn = u64::from_le_bytes(buf[p + 17..p + 25].try_into().unwrap());
        let next_txn_id = u64::from_le_bytes(buf[p + 25..p + 33].try_into().unwrap());
        let next_page_id = u64::from_le_bytes(buf[p + 33..p + 41].try_into().unwrap());

        let mut shard_uuid = [0u8; 16];
        shard_uuid.copy_from_slice(&buf[p + 41..p + 57]);

        Ok(Self {
            shard_state,
            last_checkpoint_lsn,
            last_checkpoint_epoch,
            wal_flush_lsn,
            next_txn_id,
            next_page_id,
            shard_uuid,
        })
    }

    /// Compute the standard control file path for a given shard.
    pub fn control_path(shard_dir: &Path, shard_id: usize) -> PathBuf {
        shard_dir.join(format!("shard-{shard_id}.control"))
    }
}

/// Build the temp-file sibling path used by the atomic write sequence.
#[inline]
fn control_tmp_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".tmp");
    PathBuf::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_all_fields() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let uuid = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut ctl = ShardControlFile::new(uuid);
        ctl.shard_state = ShardState::Recovery;
        ctl.last_checkpoint_lsn = 42_000;
        ctl.last_checkpoint_epoch = 7;
        ctl.wal_flush_lsn = 43_000;
        ctl.next_txn_id = 100;
        ctl.next_page_id = 500;

        ctl.write(&path).unwrap();
        let read_back = ShardControlFile::read(&path).unwrap();
        assert_eq!(read_back, ctl);
    }

    #[test]
    fn test_shard_state_variants() {
        let tmp = tempfile::tempdir().unwrap();

        let states = [
            ShardState::Running,
            ShardState::ShuttingDown,
            ShardState::Recovery,
            ShardState::Crashed,
        ];

        for state in states {
            let path = tmp.path().join(format!("state-{}.control", state as u8));
            let mut ctl = ShardControlFile::new([0u8; 16]);
            ctl.shard_state = state;
            ctl.write(&path).unwrap();

            let read_back = ShardControlFile::read(&path).unwrap();
            assert_eq!(read_back.shard_state, state);
        }
    }

    #[test]
    fn test_corrupted_crc_detected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let ctl = ShardControlFile::new([0xAA; 16]);
        ctl.write(&path).unwrap();

        // Corrupt a payload byte
        let mut buf = std::fs::read(&path).unwrap();
        buf[MOONPAGE_HEADER_SIZE + 5] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        let result = ShardControlFile::read(&path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("CRC32C"),
            "error should mention CRC32C: {}",
            err
        );
    }

    #[test]
    fn test_read_nonexistent_file() {
        let result = ShardControlFile::read(Path::new("/nonexistent/shard-0.control"));
        assert!(result.is_err());
    }

    #[test]
    fn test_write_produces_exactly_4096_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let ctl = ShardControlFile::new([0u8; 16]);
        ctl.write(&path).unwrap();

        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), PAGE_4K as u64);
    }

    #[test]
    fn test_lsn_fields_survive_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let mut ctl = ShardControlFile::new([0xFF; 16]);
        ctl.last_checkpoint_lsn = u64::MAX;
        ctl.wal_flush_lsn = u64::MAX - 1;
        ctl.next_txn_id = u64::MAX - 2;
        ctl.next_page_id = u64::MAX - 3;
        ctl.last_checkpoint_epoch = u64::MAX - 4;

        ctl.write(&path).unwrap();
        let read_back = ShardControlFile::read(&path).unwrap();

        assert_eq!(read_back.last_checkpoint_lsn, u64::MAX);
        assert_eq!(read_back.wal_flush_lsn, u64::MAX - 1);
        assert_eq!(read_back.next_txn_id, u64::MAX - 2);
        assert_eq!(read_back.next_page_id, u64::MAX - 3);
        assert_eq!(read_back.last_checkpoint_epoch, u64::MAX - 4);
    }

    #[test]
    fn test_control_path() {
        let dir = Path::new("/data/moon");
        let path = ShardControlFile::control_path(dir, 3);
        assert_eq!(path, PathBuf::from("/data/moon/shard-3.control"));
    }

    #[test]
    fn test_shard_state_from_u8() {
        assert_eq!(ShardState::from_u8(1), Some(ShardState::Running));
        assert_eq!(ShardState::from_u8(2), Some(ShardState::ShuttingDown));
        assert_eq!(ShardState::from_u8(3), Some(ShardState::Recovery));
        assert_eq!(ShardState::from_u8(4), Some(ShardState::Crashed));
        assert_eq!(ShardState::from_u8(0), None);
        assert_eq!(ShardState::from_u8(5), None);
        assert_eq!(ShardState::from_u8(255), None);
    }

    #[test]
    fn test_atomic_write_overwrites_existing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-7.control");

        // First commit.
        let mut ctl_a = ShardControlFile::new([0xAA; 16]);
        ctl_a.last_checkpoint_lsn = 100;
        ctl_a.write(&path).unwrap();

        // Second commit must atomically replace the first.
        let mut ctl_b = ShardControlFile::new([0xBB; 16]);
        ctl_b.last_checkpoint_lsn = 200;
        ctl_b.write(&path).unwrap();

        // Tmp sibling must be gone (consumed by rename).
        let tmp_sibling = control_tmp_path(&path);
        assert!(
            !tmp_sibling.exists(),
            "tmp sibling should not exist after successful write"
        );

        let read_back = ShardControlFile::read(&path).unwrap();
        assert_eq!(read_back.last_checkpoint_lsn, 200);
        assert_eq!(read_back.shard_uuid, [0xBB; 16]);
    }

    #[test]
    fn test_corrupted_control_file_recovers_via_manual_replace() {
        // Simulate a partially-written control file: truncate to 2 KB.
        // After the atomic-rename fix, a real crash mid-write would leave
        // the canonical path untouched (the tmp sibling is what's torn).
        // This test confirms that even if the canonical file IS corrupted
        // out-of-band, ShardControlFile::read still rejects it cleanly via
        // CRC and an admin can replace it from the tmp sibling.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let ctl = ShardControlFile::new([0x42; 16]);
        ctl.write(&path).unwrap();

        // Corrupt the canonical file.
        let buf = std::fs::read(&path).unwrap();
        std::fs::write(&path, &buf[..2048]).unwrap();
        assert!(ShardControlFile::read(&path).is_err());

        // A subsequent successful write must heal the file.
        ctl.write(&path).unwrap();
        let read_back = ShardControlFile::read(&path).unwrap();
        assert_eq!(read_back.shard_uuid, [0x42; 16]);
    }
}
