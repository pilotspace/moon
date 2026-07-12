//! Shard control file — the recovery entry point for each shard.
//!
//! A single 4KB page containing shard state, LSN positions, and UUID.
//! Written atomically (single-sector write + fsync) and verified on read
//! via CRC32C checksum.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::persistence::fsync::fsync_directory;
use crate::persistence::page::{MOONPAGE_HEADER_SIZE, MoonPageHeader, PAGE_4K, PageType};

/// Legacy (pre-kernel-M3-K2) control file payload size: 1 + 8 + 8 + 8 + 8 +
/// 8 + 16 = 57 bytes. Named purely to anchor the backward-compat reader
/// logic and its round-trip test — [`CONTROL_PAYLOAD_SIZE`] is what
/// [`ShardControlFile::write`] actually emits today.
#[cfg(test)]
const LEGACY_CONTROL_PAYLOAD_SIZE: u32 = 57;

/// Control file payload size: legacy 57 bytes + kernel M3 K2's per-plane
/// floor register (3 x `u64` = 24 bytes: `graph_floor_lsn`, `ws_floor_lsn`,
/// `mq_floor_lsn`) = 81 bytes.
///
/// Additive, `payload_bytes`-driven format bump — see
/// `.planning/reviews/kernel-m3-brief-2026-07-12.md` §Stage 2, Risk #4. A
/// control file written by a pre-K2 binary has `payload_bytes == 57` and
/// nothing meaningful past byte 57 (the page is still zero-padded out to
/// 4096, but the old writer never touched that region). [`ShardControlFile::read`]
/// treats a payload shorter than a given trailing field's end offset as
/// "field absent" and defaults it to the sentinel `0` ("no floor
/// published") rather than trusting whatever bytes happen to already be
/// there — this is what makes a hand-built legacy payload (not just an
/// honestly-zero-padded one) parse safely. An OLD binary reading a NEW
/// 81-byte file also still works: it only ever reads bytes `0..57` and
/// ignores the rest.
const CONTROL_PAYLOAD_SIZE: u32 = 81;

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
    /// Kernel M3 K2 — WAL v3 LSN covered by the last durably-persisted graph
    /// snapshot, mirrored here from `GraphStore::snapshot_lsn()` at the
    /// exact moment [`crate::graph::recovery::persist_graph_at_checkpoint`]
    /// wrote it (same tick, same local variable — see the Finalize call
    /// site; never a second, independently-recomputed LSN, per the brief's
    /// Risk #1). `graph_metadata.json` remains the graph engine's own
    /// replay-skip authority; this field is a *recycle-decision mirror*
    /// only, so `min(last_checkpoint_lsn, graph_floor_lsn)` — not
    /// `last_checkpoint_lsn`/`wal.current_lsn()` alone — is the safe floor
    /// to pass to WAL segment recycling.
    ///
    /// Sentinel `0` = "no floor published yet" (legacy/non-disk-offload
    /// mode always stays at this sentinel — Finalize, the only writer of
    /// this field, never runs there). This is deliberately ambiguous with a
    /// legitimately-zero LSN: recycling nothing at shard creation is
    /// indistinguishable in effect from "no floor," so the ambiguity is
    /// harmless (Open Decision #2 in the brief; a tagged `Option<u64>`
    /// encoding was considered and rejected as unneeded complexity for a
    /// case where `0` already means the same thing either way).
    pub graph_floor_lsn: u64,
    /// Kernel M3 K2 — reserved for a future WS (workspace registry)
    /// snapshot floor. Workspaces have no snapshot format in this
    /// milestone (brief §4 Non-Goals / Open Decision #1) so this field is
    /// unconditionally the sentinel `0` ("no floor") and MUST be excluded
    /// from the min-across-planes recycle computation — including it would
    /// make `min(kv, graph, ws=0)` collapse to `0` the instant a shard has
    /// ever seen a WS record, which is strictly worse than today's
    /// per-segment `segment_holds_plane_history` content scan (Risk #2).
    pub ws_floor_lsn: u64,
    /// Kernel M3 K2 — reserved for a future MQ (streams/PEL/DLQ/triggers)
    /// snapshot floor. Same sentinel-0/excluded-from-min contract as
    /// [`Self::ws_floor_lsn`]; MQ snapshot format is out of scope for this
    /// milestone (comparable in size to the graph engine's own format
    /// work).
    pub mq_floor_lsn: u64,
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
            graph_floor_lsn: 0,
            ws_floor_lsn: 0,
            mq_floor_lsn: 0,
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
        // Kernel M3 K2 — per-plane floor register (payload bytes 57..81).
        buf[p + 57..p + 65].copy_from_slice(&self.graph_floor_lsn.to_le_bytes());
        buf[p + 65..p + 73].copy_from_slice(&self.ws_floor_lsn.to_le_bytes());
        buf[p + 73..p + 81].copy_from_slice(&self.mq_floor_lsn.to_le_bytes());

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

        let read_u64 = |slice: &[u8]| -> std::io::Result<u64> {
            let arr: [u8; 8] = slice.try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "control payload truncated")
            })?;
            Ok(u64::from_le_bytes(arr))
        };
        let last_checkpoint_lsn = read_u64(&buf[p + 1..p + 9])?;
        let last_checkpoint_epoch = read_u64(&buf[p + 9..p + 17])?;
        let wal_flush_lsn = read_u64(&buf[p + 17..p + 25])?;
        let next_txn_id = read_u64(&buf[p + 25..p + 33])?;
        let next_page_id = read_u64(&buf[p + 33..p + 41])?;

        let mut shard_uuid = [0u8; 16];
        shard_uuid.copy_from_slice(&buf[p + 41..p + 57]);

        // Kernel M3 K2 — per-plane floor register, backward-compat-parsed.
        // `hdr.payload_bytes` (not the always-4096-byte buffer length) is
        // the honest marker of how much of the page a given writer
        // actually populated; a pre-K2 writer stamps `payload_bytes = 57`
        // and never touches bytes 57..81, so trust that marker per field
        // rather than assuming the trailing region happens to be zero
        // (defends against a deliberately hand-built legacy-shaped test
        // payload, not just an honestly-short one — brief Risk #4).
        let payload_len = hdr.payload_bytes as usize;
        let graph_floor_lsn = if payload_len >= 65 {
            read_u64(&buf[p + 57..p + 65])?
        } else {
            0
        };
        let ws_floor_lsn = if payload_len >= 73 {
            read_u64(&buf[p + 65..p + 73])?
        } else {
            0
        };
        let mq_floor_lsn = if payload_len >= 81 {
            read_u64(&buf[p + 73..p + 81])?
        } else {
            0
        };

        Ok(Self {
            shard_state,
            last_checkpoint_lsn,
            last_checkpoint_epoch,
            wal_flush_lsn,
            next_txn_id,
            next_page_id,
            shard_uuid,
            graph_floor_lsn,
            ws_floor_lsn,
            mq_floor_lsn,
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

    // -- Kernel M3 K2: per-plane floor register -----------------------------

    #[test]
    fn test_graph_ws_mq_floor_lsn_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let mut ctl = ShardControlFile::new([9u8; 16]);
        ctl.last_checkpoint_lsn = 500;
        ctl.graph_floor_lsn = 480;
        ctl.ws_floor_lsn = 0; // sentinel: no WS snapshot format this milestone
        ctl.mq_floor_lsn = 0; // sentinel: no MQ snapshot format this milestone

        ctl.write(&path).unwrap();
        let read_back = ShardControlFile::read(&path).unwrap();
        assert_eq!(read_back, ctl);
        assert_eq!(read_back.graph_floor_lsn, 480);
        assert_eq!(read_back.ws_floor_lsn, 0);
        assert_eq!(read_back.mq_floor_lsn, 0);
    }

    #[test]
    fn test_graph_floor_lsn_survives_max_values() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let mut ctl = ShardControlFile::new([1u8; 16]);
        ctl.graph_floor_lsn = u64::MAX;
        ctl.ws_floor_lsn = u64::MAX - 1;
        ctl.mq_floor_lsn = u64::MAX - 2;

        ctl.write(&path).unwrap();
        let read_back = ShardControlFile::read(&path).unwrap();
        assert_eq!(read_back.graph_floor_lsn, u64::MAX);
        assert_eq!(read_back.ws_floor_lsn, u64::MAX - 1);
        assert_eq!(read_back.mq_floor_lsn, u64::MAX - 2);
    }

    #[test]
    fn test_write_produces_81_byte_payload_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");
        ShardControlFile::new([0u8; 16]).write(&path).unwrap();

        let buf = std::fs::read(&path).unwrap();
        let hdr = MoonPageHeader::read_from(&buf).unwrap();
        assert_eq!(hdr.payload_bytes, CONTROL_PAYLOAD_SIZE);
        assert_eq!(CONTROL_PAYLOAD_SIZE, 81);
        // File itself is still exactly one 4KB page — the format bump adds
        // fields inside the existing headroom, not a new page size.
        assert_eq!(buf.len(), PAGE_4K);
    }

    /// Brief Risk #4: a control file written by a PRE-K2 binary
    /// (`payload_bytes == 57`, nothing meaningful past byte 57 — not even
    /// honestly zeroed, to prove the reader trusts `payload_bytes` and not
    /// the buffer's happenstance contents) must still parse cleanly, with
    /// the three new floor fields defaulting to the sentinel `0`.
    #[test]
    fn test_backward_compat_57_byte_legacy_payload_parses() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.control");

        let mut buf = [0u8; PAGE_4K];
        let mut hdr = MoonPageHeader::new(PageType::ControlPage, 0, 0);
        hdr.payload_bytes = LEGACY_CONTROL_PAYLOAD_SIZE;
        hdr.write_to(&mut buf);

        let p = MOONPAGE_HEADER_SIZE;
        buf[p] = ShardState::Running as u8;
        buf[p + 1..p + 9].copy_from_slice(&777u64.to_le_bytes()); // last_checkpoint_lsn
        buf[p + 9..p + 17].copy_from_slice(&3u64.to_le_bytes()); // last_checkpoint_epoch
        buf[p + 17..p + 25].copy_from_slice(&778u64.to_le_bytes()); // wal_flush_lsn
        buf[p + 25..p + 33].copy_from_slice(&10u64.to_le_bytes()); // next_txn_id
        buf[p + 33..p + 41].copy_from_slice(&20u64.to_le_bytes()); // next_page_id
        buf[p + 41..p + 57].copy_from_slice(&[0x77u8; 16]); // shard_uuid
        // Deliberately poison bytes 57..81 with non-zero garbage — a
        // correct reader must IGNORE these because payload_bytes says the
        // legacy writer never populated them, not merely happen to see
        // zeros there.
        buf[p + 57..p + 81].copy_from_slice(&[0xEEu8; 24]);

        MoonPageHeader::compute_checksum(&mut buf);
        std::fs::write(&path, buf).unwrap();

        let read_back = ShardControlFile::read(&path).unwrap();
        assert_eq!(read_back.last_checkpoint_lsn, 777);
        assert_eq!(read_back.last_checkpoint_epoch, 3);
        assert_eq!(read_back.wal_flush_lsn, 778);
        assert_eq!(read_back.next_txn_id, 10);
        assert_eq!(read_back.next_page_id, 20);
        assert_eq!(read_back.shard_uuid, [0x77u8; 16]);
        // The K2 fields must default to the sentinel, NOT the poisoned
        // 0xEE bytes physically present past payload_bytes=57.
        assert_eq!(read_back.graph_floor_lsn, 0);
        assert_eq!(read_back.ws_floor_lsn, 0);
        assert_eq!(read_back.mq_floor_lsn, 0);
    }
}
