//! ShardManifest — dual-root atomic metadata store for shard file tracking.
//!
//! Uses LMDB-style alternating 4KB root pages at offsets 0 and 4096.
//! A single `sync_data()` call is the atomic commit point.
//! CRC32C checksum via MoonPageHeader ensures crash-safe recovery.

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::persistence::page::{MOONPAGE_HEADER_SIZE, MoonPageHeader, PAGE_4K, PageType};

/// File lifecycle status within the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FileStatus {
    /// File is active and serving reads.
    Active = 1,
    /// File is being built (not yet readable).
    Building = 2,
    /// File is sealed (immutable, compaction candidate).
    Sealed = 3,
    /// File is undergoing compaction.
    Compacting = 4,
    /// File is logically deleted (physical removal pending).
    Tombstone = 5,
    /// File has been moved to archive storage.
    Archived = 6,
}

impl FileStatus {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Active),
            2 => Some(Self::Building),
            3 => Some(Self::Sealed),
            4 => Some(Self::Compacting),
            5 => Some(Self::Tombstone),
            6 => Some(Self::Archived),
            _ => None,
        }
    }
}

/// Storage tier for tiered storage placement.
///
/// Discriminant values match MOONSTORE-V2-COMPREHENSIVE-DESIGN.md §4.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageTier {
    /// Data in RAM (file is WAL/snapshot only).
    Hot = 0x01,
    /// File is mmap'd, OS page cache manages residency.
    Warm = 0x02,
    /// File on SSD, accessed via io_uring / direct I/O.
    Cold = 0x03,
    /// Object storage (S3), accessed via HTTP range reads.
    Archive = 0x04,
}

impl StorageTier {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Hot),
            0x02 => Some(Self::Warm),
            0x03 => Some(Self::Cold),
            0x04 => Some(Self::Archive),
            _ => None,
        }
    }
}

/// Fixed-size 56-byte file entry in the shard manifest (format_version=2).
///
/// **v0.2 — PITR/CDC support.** Adds `last_modified_lsn` so recovery can
/// pick the right files for a `--recovery-target-lsn`. Legacy 48-byte (v1)
/// entries are decoded with `last_modified_lsn = created_lsn` (lossless
/// fallback — the file's only known LSN reference point is when it was
/// created).
///
/// Byte layout (all little-endian):
/// ```text
/// Offset  Size  Field
/// 0..8    8     file_id (u64 LE)
/// 8       1     file_type (PageType discriminant)
/// 9       1     status (FileStatus as u8)
/// 10      1     tier (StorageTier as u8)
/// 11      1     page_size_log2 (e.g. 12 for 4KB, 16 for 64KB)
/// 12..16  4     page_count (u32 LE)
/// 16..24  8     byte_size (u64 LE)
/// 24..32  8     created_lsn (u64 LE)
/// 32..40  8     min_key_hash (u64 LE)
/// 40..48  8     max_key_hash (u64 LE)
/// 48..56  8     last_modified_lsn (u64 LE)  -- v2 only
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub file_id: u64,
    pub file_type: u8,
    pub status: FileStatus,
    pub tier: StorageTier,
    pub page_size_log2: u8,
    pub page_count: u32,
    pub byte_size: u64,
    pub created_lsn: u64,
    pub min_key_hash: u64,
    pub max_key_hash: u64,
    /// LSN of the last mutation to this file (v2). For files imported from
    /// a v1 manifest this is set equal to `created_lsn`. Used by PITR to
    /// select the file set valid at `--recovery-target-lsn`.
    pub last_modified_lsn: u64,
}

impl FileEntry {
    /// On-disk size of a v2 FileEntry (with `last_modified_lsn`).
    pub const SIZE: usize = 56;

    /// On-disk size of a legacy v1 FileEntry (without `last_modified_lsn`).
    pub const SIZE_V1: usize = 48;

    /// Serialize this entry as v2 (56 bytes) into `buf` (must be >= 56 bytes).
    ///
    /// # Panics
    ///
    /// Panics if `buf.len() < 56`.
    pub fn write_to(&self, buf: &mut [u8]) {
        assert!(
            buf.len() >= Self::SIZE,
            "buffer too small for FileEntry: {} < {}",
            buf.len(),
            Self::SIZE,
        );

        buf[0..8].copy_from_slice(&self.file_id.to_le_bytes());
        buf[8] = self.file_type;
        buf[9] = self.status as u8;
        buf[10] = self.tier as u8;
        buf[11] = self.page_size_log2;
        buf[12..16].copy_from_slice(&self.page_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.byte_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.created_lsn.to_le_bytes());
        buf[32..40].copy_from_slice(&self.min_key_hash.to_le_bytes());
        buf[40..48].copy_from_slice(&self.max_key_hash.to_le_bytes());
        buf[48..56].copy_from_slice(&self.last_modified_lsn.to_le_bytes());
    }

    /// Deserialize a v2 FileEntry (56 bytes) from `buf`.
    ///
    /// Returns `None` if `buf.len() < 56`.
    pub fn read_from(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let mut entry = Self::read_v1(buf)?;
        entry.last_modified_lsn = u64::from_le_bytes([
            buf[48], buf[49], buf[50], buf[51], buf[52], buf[53], buf[54], buf[55],
        ]);
        Some(entry)
    }

    /// Deserialize a legacy v1 FileEntry (48 bytes) from `buf`.
    ///
    /// `last_modified_lsn` is synthesized as `created_lsn` — the only known
    /// LSN reference point for files written under the v1 schema.
    pub fn read_v1(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE_V1 {
            return None;
        }

        let file_id = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let file_type = buf[8];
        let status = FileStatus::from_u8(buf[9])?;
        let tier = StorageTier::from_u8(buf[10])?;
        let page_size_log2 = buf[11];
        let page_count = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let byte_size = u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        let created_lsn = u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]);
        let min_key_hash = u64::from_le_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]);
        let max_key_hash = u64::from_le_bytes([
            buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47],
        ]);

        Some(Self {
            file_id,
            file_type,
            status,
            tier,
            page_size_log2,
            page_count,
            byte_size,
            created_lsn,
            min_key_hash,
            max_key_hash,
            // v1 fallback — synthesize from created_lsn so existing manifests
            // remain readable and PITR target_lsn comparisons stay sane.
            last_modified_lsn: created_lsn,
        })
    }
}

/// Offset of Root A page within the manifest file.
const ROOT_A_OFFSET: u64 = 0;

/// Offset of Root B page within the manifest file.
const ROOT_B_OFFSET: u64 = PAGE_4K as u64;

/// Payload starts after 64-byte MoonPageHeader.
/// Layout per §4.2: epoch(8) + redo_lsn(8) + wal_flush_lsn(8) + file_count(4) +
/// entry_page_count(4) + snapshot_lsn(8) + created_at(8) + shard_uuid(16) = 64 bytes,
/// then file_count * FileEntry::SIZE bytes of FileEntry records.
const ROOT_META_SIZE: usize = 64;

/// Manifest format version embedded in `MoonPageHeader.format_version`.
/// - 1 = legacy 48-byte FileEntry layout (read-only fallback).
/// - 2 = current 56-byte layout with `last_modified_lsn` for PITR.
pub const MANIFEST_FORMAT_V1: u8 = 1;
pub const MANIFEST_FORMAT_V2: u8 = 2;

/// Maximum inline FileEntry records per root page (v2 layout).
/// (4096 - 64 header - 64 meta) / 56 = 70.
pub const MAX_INLINE_ENTRIES: usize =
    (PAGE_4K - MOONPAGE_HEADER_SIZE - ROOT_META_SIZE) / FileEntry::SIZE;

/// In-memory representation of one manifest root page.
///
/// Fields match MOONSTORE-V2-COMPREHENSIVE-DESIGN.md §4.2.
#[derive(Debug, Clone)]
pub struct ManifestRoot {
    /// Monotonically increasing epoch (commit counter).
    pub epoch: u64,
    /// WAL REDO point from last checkpoint.
    pub redo_lsn: u64,
    /// Highest durable WAL LSN.
    pub wal_flush_lsn: u64,
    /// Number of file entries.
    pub file_count: u32,
    /// Number of overflow ManifestEntry pages.
    pub entry_page_count: u32,
    /// LSN of latest completed snapshot.
    pub snapshot_lsn: u64,
    /// Unix timestamp (seconds).
    pub created_at: u64,
    /// Unique shard identifier (must match control file).
    pub shard_uuid: [u8; 16],
    /// File entries tracked by this root.
    pub entries: Vec<FileEntry>,
}

/// Dual-root atomic manifest for tracking shard files.
///
/// Uses LMDB-style alternating root pages: writes go to the inactive
/// slot, and a single `sync_data()` is the atomic commit point.
#[derive(Debug)]
pub struct ShardManifest {
    /// File handle opened for read/write.
    file: std::fs::File,
    /// Path to the manifest file on disk.
    path: PathBuf,
    /// Currently active root (the last successfully committed state).
    active_root: ManifestRoot,
    /// Which slot is currently active: 0 = Root A (offset 0), 1 = Root B (offset 4096).
    active_slot: u8,
}

impl ShardManifest {
    /// Create a new manifest file with an empty Root A at epoch 1.
    ///
    /// The file will be exactly 8192 bytes (two 4KB root pages).
    pub fn create(path: &Path) -> std::io::Result<Self> {
        let mut buf = vec![0u8; 2 * PAGE_4K];

        // Build Root A at offset 0 with epoch=1, file_count=0
        let root = ManifestRoot {
            epoch: 1,
            redo_lsn: 0,
            wal_flush_lsn: 0,
            file_count: 0,
            entry_page_count: 0,
            snapshot_lsn: 0,
            created_at: 0,
            shard_uuid: [0u8; 16],
            entries: Vec::new(),
        };
        Self::serialize_root(&root, &mut buf[..PAGE_4K]);

        // Write file
        std::fs::write(path, &buf)?;

        // Open for R/W and sync
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        file.sync_data()?;

        // fsync parent directory for metadata durability
        if let Some(parent) = path.parent() {
            crate::persistence::fsync::fsync_directory(parent)?;
        }

        Ok(Self {
            file,
            path: path.to_path_buf(),
            active_root: root,
            active_slot: 0,
        })
    }

    /// Open an existing manifest file and recover the latest valid root.
    ///
    /// Reads both root pages, validates CRC32C, and picks the one with
    /// the higher epoch. If both are corrupted, returns an error.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let buf = std::fs::read(path)?;
        if buf.len() < 2 * PAGE_4K {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "manifest file too small: {} bytes, expected at least {}",
                    buf.len(),
                    2 * PAGE_4K,
                ),
            ));
        }

        let root_a = Self::try_parse_root(&buf[..PAGE_4K]);
        let root_b = Self::try_parse_root(&buf[PAGE_4K..2 * PAGE_4K]);

        let (active_root, active_slot) = match (root_a, root_b) {
            (Some(a), Some(b)) => {
                if b.epoch >= a.epoch {
                    (b, 1u8)
                } else {
                    (a, 0u8)
                }
            }
            (Some(a), None) => (a, 0),
            (None, Some(b)) => (b, 1),
            (None, None) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "both manifest root pages are corrupted",
                ));
            }
        };

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
            active_root,
            active_slot,
        })
    }

    /// Commit the current state to the inactive root page.
    ///
    /// 1. Increment epoch
    /// 2. Serialize to the inactive slot
    /// 3. `sync_data()` — this is the atomic commit point
    /// 4. Flip active_slot
    pub fn commit(&mut self) -> std::io::Result<()> {
        if self.active_root.entries.len() > MAX_INLINE_ENTRIES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "too many entries for inline root page: {} > {}",
                    self.active_root.entries.len(),
                    MAX_INLINE_ENTRIES,
                ),
            ));
        }

        self.active_root.epoch += 1;
        self.active_root.file_count = self.active_root.entries.len() as u32;

        let mut page = [0u8; PAGE_4K];
        Self::serialize_root(&self.active_root, &mut page);

        // Write to the inactive slot
        let write_offset = if self.active_slot == 0 {
            ROOT_B_OFFSET
        } else {
            ROOT_A_OFFSET
        };

        self.file.seek(SeekFrom::Start(write_offset))?;
        self.file.write_all(&page)?;
        self.file.sync_data()?; // ATOMIC COMMIT POINT

        // Flip active slot
        self.active_slot = if self.active_slot == 0 { 1 } else { 0 };

        Ok(())
    }

    /// Add a file entry to the manifest (in-memory only until commit).
    pub fn add_file(&mut self, entry: FileEntry) {
        self.active_root.entries.push(entry);
    }

    /// Mark a file as Tombstone by file_id (in-memory only until commit).
    pub fn remove_file(&mut self, file_id: u64) {
        for entry in &mut self.active_root.entries {
            if entry.file_id == file_id {
                entry.status = FileStatus::Tombstone;
            }
        }
    }

    /// Update a file entry in-place (in-memory only until commit).
    pub fn update_file(&mut self, file_id: u64, f: impl FnOnce(&mut FileEntry)) {
        for entry in &mut self.active_root.entries {
            if entry.file_id == file_id {
                f(entry);
                return;
            }
        }
    }

    /// Return a reference to the active file entries.
    pub fn files(&self) -> &[FileEntry] {
        &self.active_root.entries
    }

    /// Return the current epoch.
    pub fn epoch(&self) -> u64 {
        self.active_root.epoch
    }

    /// Return the currently active slot (0 = Root A, 1 = Root B).
    pub fn active_slot(&self) -> u8 {
        self.active_slot
    }

    /// Return the path to the manifest file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Serialize a ManifestRoot into a 4KB page buffer (always v2 format).
    ///
    /// Layout per §4.2: epoch(8) + redo_lsn(8) + wal_flush_lsn(8) + file_count(4) +
    /// entry_page_count(4) + snapshot_lsn(8) + created_at(8) + shard_uuid(16) = 64 bytes,
    /// then file_count * 56-byte FileEntry records.
    fn serialize_root(root: &ManifestRoot, page: &mut [u8]) {
        assert!(page.len() >= PAGE_4K);

        // Zero the page
        page[..PAGE_4K].fill(0);

        // Payload: 64 bytes meta + file_count * 56 bytes entries (v2)
        let payload_bytes = ROOT_META_SIZE + root.entries.len() * FileEntry::SIZE;

        // Header — stamp v2 format so readers know to expect 56-byte entries.
        let mut hdr = MoonPageHeader::new(PageType::ManifestRoot, 0, 0);
        hdr.format_version = MANIFEST_FORMAT_V2;
        hdr.payload_bytes = payload_bytes as u32;
        hdr.entry_count = root.entries.len() as u32;
        hdr.write_to(page);

        // Manifest-specific metadata after header (64 bytes)
        let p = MOONPAGE_HEADER_SIZE;
        page[p..p + 8].copy_from_slice(&root.epoch.to_le_bytes());
        page[p + 8..p + 16].copy_from_slice(&root.redo_lsn.to_le_bytes());
        page[p + 16..p + 24].copy_from_slice(&root.wal_flush_lsn.to_le_bytes());
        page[p + 24..p + 28].copy_from_slice(&root.file_count.to_le_bytes());
        page[p + 28..p + 32].copy_from_slice(&root.entry_page_count.to_le_bytes());
        page[p + 32..p + 40].copy_from_slice(&root.snapshot_lsn.to_le_bytes());
        page[p + 40..p + 48].copy_from_slice(&root.created_at.to_le_bytes());
        page[p + 48..p + 64].copy_from_slice(&root.shard_uuid);

        // FileEntry records
        let entries_start = p + ROOT_META_SIZE;
        for (i, entry) in root.entries.iter().enumerate() {
            let offset = entries_start + i * FileEntry::SIZE;
            entry.write_to(&mut page[offset..offset + FileEntry::SIZE]);
        }

        // Compute CRC32C over payload region
        MoonPageHeader::compute_checksum(page);
    }

    /// Try to parse a root page from a 4KB buffer.
    ///
    /// Returns `None` if magic/type mismatch or CRC32C fails. Recognizes both
    /// v1 (48-byte entries, format_version=1) and v2 (56-byte entries,
    /// format_version=2). v1 entries are upgraded in-memory with
    /// `last_modified_lsn = created_lsn` so the rest of the system sees a
    /// uniform v2 view.
    fn try_parse_root(page: &[u8]) -> Option<ManifestRoot> {
        if page.len() < PAGE_4K {
            return None;
        }

        // Verify header
        let hdr = MoonPageHeader::read_from(page)?;
        if hdr.page_type != PageType::ManifestRoot {
            return None;
        }

        // Verify CRC32C
        if !MoonPageHeader::verify_checksum(page) {
            return None;
        }

        // Pick the entry size based on the on-disk format_version.
        // Unknown versions are rejected (defensive — better to fail loudly
        // than misinterpret a future format).
        let entry_size = match hdr.format_version {
            MANIFEST_FORMAT_V1 => FileEntry::SIZE_V1,
            MANIFEST_FORMAT_V2 => FileEntry::SIZE,
            _ => return None,
        };

        // Parse metadata (64 bytes)
        let p = MOONPAGE_HEADER_SIZE;
        let epoch = u64::from_le_bytes(page[p..p + 8].try_into().ok()?);
        let redo_lsn = u64::from_le_bytes(page[p + 8..p + 16].try_into().ok()?);
        let wal_flush_lsn = u64::from_le_bytes(page[p + 16..p + 24].try_into().ok()?);
        let file_count = u32::from_le_bytes(page[p + 24..p + 28].try_into().ok()?);
        let entry_page_count = u32::from_le_bytes(page[p + 28..p + 32].try_into().ok()?);

        // Validate payload framing: root metadata + declared entries must match
        // the authenticated payload_bytes and entry_count in the header. This
        // prevents reading unchecked trailing bytes on a corrupted root page.
        let expected_payload =
            ROOT_META_SIZE.checked_add((file_count as usize).checked_mul(entry_size)?)?;
        if hdr.payload_bytes as usize != expected_payload {
            return None;
        }
        if hdr.entry_count != file_count {
            return None;
        }
        let snapshot_lsn = u64::from_le_bytes(page[p + 32..p + 40].try_into().ok()?);
        let created_at = u64::from_le_bytes(page[p + 40..p + 48].try_into().ok()?);
        let mut shard_uuid = [0u8; 16];
        shard_uuid.copy_from_slice(&page[p + 48..p + 64]);

        // Parse entries. Use the size dictated by format_version so v1
        // manifests remain readable; FileEntry::read_v1 synthesizes
        // `last_modified_lsn = created_lsn` for the upgraded in-memory view.
        let entries_start = p + ROOT_META_SIZE;
        let mut entries = Vec::with_capacity(file_count as usize);
        for i in 0..file_count as usize {
            let offset = entries_start + i * entry_size;
            let entry = if entry_size == FileEntry::SIZE_V1 {
                FileEntry::read_v1(&page[offset..])?
            } else {
                FileEntry::read_from(&page[offset..])?
            };
            entries.push(entry);
        }

        Some(ManifestRoot {
            epoch,
            redo_lsn,
            wal_flush_lsn,
            file_count,
            entry_page_count,
            snapshot_lsn,
            created_at,
            shard_uuid,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_entry_roundtrip_all_fields() {
        let entry = FileEntry {
            file_id: 0x0102_0304_0506_0708,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1000,
            byte_size: 4_096_000,
            created_lsn: 42,
            min_key_hash: 0x1111_2222_3333_4444,
            max_key_hash: 0xAAAA_BBBB_CCCC_DDDD,
            last_modified_lsn: 4242,
        };

        let mut buf = [0u8; FileEntry::SIZE];
        entry.write_to(&mut buf);

        let parsed = FileEntry::read_from(&buf).expect("should parse");
        assert_eq!(parsed, entry);
    }

    #[test]
    fn file_entry_exactly_56_bytes() {
        let entry = FileEntry {
            file_id: 1,
            file_type: PageType::VecCodes as u8,
            status: FileStatus::Sealed,
            tier: StorageTier::Warm,
            page_size_log2: 16,
            page_count: 500,
            byte_size: 32_768_000,
            created_lsn: 100,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: 200,
        };

        let mut buf = [0xFFu8; 64];
        entry.write_to(&mut buf);

        // First 56 bytes get written (v2 layout); bytes 56..64 must stay 0xFF.
        assert_eq!(FileEntry::SIZE, 56);
        assert_eq!(buf[56..64], [0xFF; 8]);
    }

    /// P1 — last_modified_lsn must roundtrip independently of created_lsn.
    #[test]
    fn file_entry_last_modified_lsn_independent_of_created() {
        let entry = FileEntry {
            file_id: 7,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Warm,
            page_size_log2: 12,
            page_count: 10,
            byte_size: 40960,
            created_lsn: 1,
            min_key_hash: 0,
            max_key_hash: 0,
            last_modified_lsn: 99_999,
        };

        let mut buf = [0u8; FileEntry::SIZE];
        entry.write_to(&mut buf);
        let parsed = FileEntry::read_from(&buf).expect("should parse v2");
        assert_eq!(parsed.created_lsn, 1);
        assert_eq!(parsed.last_modified_lsn, 99_999);
    }

    /// P1 — legacy v1 (48-byte) entries must decode with
    /// `last_modified_lsn = created_lsn` as a lossless fallback. This is the
    /// contract that lets existing on-disk manifests survive the upgrade.
    #[test]
    fn file_entry_v1_decodes_with_synthesized_last_modified() {
        // Build a 48-byte v1 entry by hand (no last_modified_lsn trailer).
        let mut buf = [0u8; FileEntry::SIZE_V1];
        buf[0..8].copy_from_slice(&123u64.to_le_bytes()); // file_id
        buf[8] = PageType::KvLeaf as u8;
        buf[9] = FileStatus::Active as u8;
        buf[10] = StorageTier::Hot as u8;
        buf[11] = 12;
        buf[12..16].copy_from_slice(&50u32.to_le_bytes()); // page_count
        buf[16..24].copy_from_slice(&(50u64 * 4096).to_le_bytes()); // byte_size
        buf[24..32].copy_from_slice(&777u64.to_le_bytes()); // created_lsn
        buf[32..40].copy_from_slice(&0u64.to_le_bytes());
        buf[40..48].copy_from_slice(&u64::MAX.to_le_bytes());

        let parsed = FileEntry::read_v1(&buf).expect("v1 decode");
        assert_eq!(parsed.file_id, 123);
        assert_eq!(parsed.created_lsn, 777);
        assert_eq!(
            parsed.last_modified_lsn, 777,
            "v1 fallback must synthesize last_modified_lsn = created_lsn",
        );
    }

    #[test]
    fn file_status_all_variants() {
        assert_eq!(FileStatus::from_u8(1), Some(FileStatus::Active));
        assert_eq!(FileStatus::from_u8(2), Some(FileStatus::Building));
        assert_eq!(FileStatus::from_u8(3), Some(FileStatus::Sealed));
        assert_eq!(FileStatus::from_u8(4), Some(FileStatus::Compacting));
        assert_eq!(FileStatus::from_u8(5), Some(FileStatus::Tombstone));
        assert_eq!(FileStatus::from_u8(6), Some(FileStatus::Archived));
        assert_eq!(FileStatus::from_u8(0), None);
        assert_eq!(FileStatus::from_u8(7), None);
        assert_eq!(FileStatus::from_u8(255), None);
    }

    #[test]
    fn file_storage_tier_all_variants() {
        assert_eq!(StorageTier::from_u8(0x01), Some(StorageTier::Hot));
        assert_eq!(StorageTier::from_u8(0x02), Some(StorageTier::Warm));
        assert_eq!(StorageTier::from_u8(0x03), Some(StorageTier::Cold));
        assert_eq!(StorageTier::from_u8(0x04), Some(StorageTier::Archive));
        assert_eq!(StorageTier::from_u8(0), None);
        assert_eq!(StorageTier::from_u8(5), None);
        assert_eq!(StorageTier::from_u8(255), None);
    }

    #[test]
    fn file_entry_page_size_variants() {
        // 4KB pages
        let entry_4k = FileEntry {
            file_id: 10,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 100,
            byte_size: 409_600,
            created_lsn: 1,
            min_key_hash: 0,
            max_key_hash: 0,
            last_modified_lsn: 1,
        };
        let mut buf = [0u8; FileEntry::SIZE];
        entry_4k.write_to(&mut buf);
        let parsed = FileEntry::read_from(&buf).unwrap();
        assert_eq!(parsed.page_size_log2, 12);

        // 64KB pages
        let entry_64k = FileEntry {
            page_size_log2: 16,
            file_type: PageType::VecCodes as u8,
            ..entry_4k
        };
        entry_64k.write_to(&mut buf);
        let parsed = FileEntry::read_from(&buf).unwrap();
        assert_eq!(parsed.page_size_log2, 16);
    }

    #[test]
    fn file_entry_read_from_short_buffer() {
        // v2 read needs >= 56 bytes
        let buf = [0u8; 55];
        assert!(FileEntry::read_from(&buf).is_none());
        // v1 read needs >= 48 bytes
        let buf = [0u8; 47];
        assert!(FileEntry::read_v1(&buf).is_none());
    }

    // --- ShardManifest tests ---

    fn make_entry(id: u64) -> FileEntry {
        FileEntry {
            file_id: id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 100,
            byte_size: 409_600,
            created_lsn: id,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: id,
        }
    }

    #[test]
    fn test_manifest_create_and_open() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let m = ShardManifest::create(&path).unwrap();
        assert_eq!(m.epoch(), 1);
        assert_eq!(m.active_slot(), 0);
        assert!(m.files().is_empty());

        // File should be exactly 8192 bytes
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 8192);

        // Re-open should recover same state
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 1);
        assert!(m2.files().is_empty());
    }

    #[test]
    fn test_manifest_alternating_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        assert_eq!(m.active_slot(), 0); // Root A is active after create

        // First commit: writes to Root B (inactive), then flips active to 1
        m.add_file(make_entry(1));
        m.commit().unwrap();
        assert_eq!(m.epoch(), 2);
        assert_eq!(m.active_slot(), 1); // Now Root B is active

        // Second commit: writes to Root A (inactive), then flips active to 0
        m.add_file(make_entry(2));
        m.commit().unwrap();
        assert_eq!(m.epoch(), 3);
        assert_eq!(m.active_slot(), 0); // Back to Root A

        // Verify recovery picks epoch 3
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 3);
        assert_eq!(m2.files().len(), 2);
    }

    #[test]
    fn test_manifest_recovery_picks_higher_epoch() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        // epoch 1 on Root A

        m.add_file(make_entry(1));
        m.commit().unwrap(); // epoch 2 on Root B

        m.add_file(make_entry(2));
        m.commit().unwrap(); // epoch 3 on Root A

        m.add_file(make_entry(3));
        m.commit().unwrap(); // epoch 4 on Root B

        m.add_file(make_entry(4));
        m.commit().unwrap(); // epoch 5 on Root A

        m.add_file(make_entry(5));
        m.commit().unwrap(); // epoch 6 on Root B

        // Root A has epoch 5 (entries 1-4), Root B has epoch 6 (entries 1-5)
        // Recovery should pick Root B (higher epoch)
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 6);
        assert_eq!(m2.active_slot(), 1);
        assert_eq!(m2.files().len(), 5);
    }

    #[test]
    fn test_manifest_recovery_corrupt_root_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();

        m.add_file(make_entry(1));
        m.commit().unwrap(); // epoch 2 on Root B

        m.add_file(make_entry(2));
        m.commit().unwrap(); // epoch 3 on Root A

        // Corrupt Root A (offset 0) payload
        let mut buf = std::fs::read(&path).unwrap();
        buf[MOONPAGE_HEADER_SIZE + 5] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        // Should fallback to Root B (epoch 2)
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 2);
        assert_eq!(m2.active_slot(), 1);
        assert_eq!(m2.files().len(), 1);
    }

    #[test]
    fn test_manifest_both_corrupt_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let m = ShardManifest::create(&path).unwrap();
        drop(m);

        // Corrupt both roots
        let mut buf = std::fs::read(&path).unwrap();
        // Corrupt Root A payload
        buf[MOONPAGE_HEADER_SIZE + 3] ^= 0xFF;
        // Corrupt Root B payload
        buf[PAGE_4K + MOONPAGE_HEADER_SIZE + 3] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        let result = ShardManifest::open(&path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("corrupted"),
            "error should mention corruption: {}",
            err,
        );
    }

    #[test]
    fn test_manifest_max_inline_entries() {
        // v2: (4096 - 64 header - 64 meta) / 56 = 70.
        // Capacity drops from 82 (v1) → 70 (v2) as the price of adding
        // last_modified_lsn for PITR. Overflow pages (entry_page_count) are
        // the long-term answer beyond this ceiling.
        assert_eq!(MAX_INLINE_ENTRIES, 70);

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();

        // Add exactly 70 entries
        for i in 0..70u64 {
            m.add_file(make_entry(i + 1));
        }
        m.commit().unwrap();

        // Verify recovery
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files().len(), 70);

        // Adding one more should fail on commit
        drop(m2);
        let mut m3 = ShardManifest::open(&path).unwrap();
        m3.add_file(make_entry(71));
        let result = m3.commit();
        assert!(result.is_err());
    }

    /// P1 — manifest written today must always stamp format_version = 2.
    /// This is the contract that lets future readers know to expect 56-byte
    /// FileEntry records.
    #[test]
    fn test_manifest_writes_v2_format_version() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        m.add_file(make_entry(1));
        m.commit().unwrap();
        drop(m);

        let buf = std::fs::read(&path).unwrap();
        // After the create + one commit, the active root has the latest data.
        // Either Root A or Root B should carry format_version = 2 — just
        // assert that at least one slot is stamped v2 (the active one).
        let v2_at_a = buf[4] == MANIFEST_FORMAT_V2;
        let v2_at_b = buf[PAGE_4K + 4] == MANIFEST_FORMAT_V2;
        assert!(
            v2_at_a || v2_at_b,
            "expected v2 format_version on at least one root; got A={} B={}",
            buf[4],
            buf[PAGE_4K + 4],
        );
    }

    /// P1 — a hand-crafted v1 manifest page (48-byte entries, format_version=1)
    /// must be readable by the current code, with `last_modified_lsn`
    /// synthesized from `created_lsn`. This guards the upgrade path for any
    /// pre-existing on-disk manifests.
    #[test]
    fn test_manifest_v1_format_compat() {
        use crate::persistence::page::{MoonPageHeader, PageType};

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        // Build an 8KB buffer with a v1 Root A and an empty Root B.
        let mut buf = vec![0u8; 2 * PAGE_4K];

        // --- Root A (v1) ---
        // 3 v1 entries of 48 bytes each.
        let entry_size_v1 = FileEntry::SIZE_V1;
        let n_entries = 3usize;
        let payload_bytes = ROOT_META_SIZE + n_entries * entry_size_v1;
        let mut hdr = MoonPageHeader::new(PageType::ManifestRoot, 0, 0);
        hdr.format_version = MANIFEST_FORMAT_V1;
        hdr.payload_bytes = payload_bytes as u32;
        hdr.entry_count = n_entries as u32;
        hdr.write_to(&mut buf[..PAGE_4K]);

        // Manifest meta: epoch=1, everything else zero.
        let p = MOONPAGE_HEADER_SIZE;
        buf[p..p + 8].copy_from_slice(&1u64.to_le_bytes()); // epoch
        buf[p + 24..p + 28].copy_from_slice(&(n_entries as u32).to_le_bytes()); // file_count

        // Three v1 entries with distinct created_lsn values.
        let entries_start = p + ROOT_META_SIZE;
        for i in 0..n_entries {
            let off = entries_start + i * entry_size_v1;
            let created_lsn = 100u64 + i as u64;
            buf[off..off + 8].copy_from_slice(&((i as u64) + 1).to_le_bytes());
            buf[off + 8] = PageType::KvLeaf as u8;
            buf[off + 9] = FileStatus::Active as u8;
            buf[off + 10] = StorageTier::Hot as u8;
            buf[off + 11] = 12;
            buf[off + 12..off + 16].copy_from_slice(&10u32.to_le_bytes());
            buf[off + 16..off + 24].copy_from_slice(&40960u64.to_le_bytes());
            buf[off + 24..off + 32].copy_from_slice(&created_lsn.to_le_bytes());
            buf[off + 32..off + 40].copy_from_slice(&0u64.to_le_bytes());
            buf[off + 40..off + 48].copy_from_slice(&u64::MAX.to_le_bytes());
        }
        MoonPageHeader::compute_checksum(&mut buf[..PAGE_4K]);
        // Root B stays zeroed → invalid on parse, will be ignored.

        std::fs::write(&path, &buf).unwrap();

        // Now open with the current (v2) code and verify v1 compat.
        let m = ShardManifest::open(&path).unwrap();
        assert_eq!(m.epoch(), 1);
        assert_eq!(m.files().len(), n_entries);
        for (i, entry) in m.files().iter().enumerate() {
            assert_eq!(entry.created_lsn, 100 + i as u64);
            assert_eq!(
                entry.last_modified_lsn, entry.created_lsn,
                "v1 entry must synthesize last_modified_lsn = created_lsn",
            );
        }
    }

    #[test]
    fn test_manifest_add_remove_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();

        m.add_file(make_entry(1));
        m.add_file(make_entry(2));
        m.add_file(make_entry(3));
        m.commit().unwrap();

        // Remove file 2
        m.remove_file(2);
        m.commit().unwrap();

        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files().len(), 3); // Still 3 entries, one is tombstoned
        assert_eq!(m2.files()[1].status, FileStatus::Tombstone);
        assert_eq!(m2.files()[0].status, FileStatus::Active);
        assert_eq!(m2.files()[2].status, FileStatus::Active);
    }

    #[test]
    fn test_manifest_update_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        m.add_file(make_entry(1));
        m.commit().unwrap();

        m.update_file(1, |e| {
            e.status = FileStatus::Sealed;
            e.tier = StorageTier::Warm;
        });
        m.commit().unwrap();

        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files()[0].status, FileStatus::Sealed);
        assert_eq!(m2.files()[0].tier, StorageTier::Warm);
    }
}
