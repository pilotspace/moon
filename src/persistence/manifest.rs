//! ShardManifest — dual-root atomic metadata store for shard file tracking.
//!
//! Uses LMDB-style alternating 4KB root pages at offsets 0 and 4096.
//! A single `sync_data()` call is the atomic commit point.
//! CRC32C checksum via MoonPageHeader ensures crash-safe recovery.

use crate::persistence::page::PageType;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageTier {
    /// In-memory / fastest storage.
    Hot = 0,
    /// SSD / local NVMe.
    Warm = 1,
    /// Slower disk or networked storage.
    Cold = 2,
    /// Long-term archival storage.
    Archive = 3,
}

impl StorageTier {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Hot),
            1 => Some(Self::Warm),
            2 => Some(Self::Cold),
            3 => Some(Self::Archive),
            _ => None,
        }
    }
}

/// Fixed-size 48-byte file entry in the shard manifest.
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
}

impl FileEntry {
    /// On-disk size of a single FileEntry.
    pub const SIZE: usize = 48;

    /// Serialize this entry into `buf` (must be >= 48 bytes).
    ///
    /// # Panics
    ///
    /// Panics if `buf.len() < 48`.
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
    }

    /// Deserialize a FileEntry from `buf`.
    ///
    /// Returns `None` if `buf.len() < 48`.
    pub fn read_from(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
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
            file_type: PageType::KvData as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1000,
            byte_size: 4_096_000,
            created_lsn: 42,
            min_key_hash: 0x1111_2222_3333_4444,
            max_key_hash: 0xAAAA_BBBB_CCCC_DDDD,
        };

        let mut buf = [0u8; 48];
        entry.write_to(&mut buf);

        let parsed = FileEntry::read_from(&buf).expect("should parse");
        assert_eq!(parsed, entry);
    }

    #[test]
    fn file_entry_exactly_48_bytes() {
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
        };

        let mut buf = [0xFFu8; 64];
        entry.write_to(&mut buf);

        // Only first 48 bytes should be written; bytes 48..64 should remain 0xFF
        assert_eq!(buf[48..64], [0xFF; 16]);
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
        assert_eq!(StorageTier::from_u8(0), Some(StorageTier::Hot));
        assert_eq!(StorageTier::from_u8(1), Some(StorageTier::Warm));
        assert_eq!(StorageTier::from_u8(2), Some(StorageTier::Cold));
        assert_eq!(StorageTier::from_u8(3), Some(StorageTier::Archive));
        assert_eq!(StorageTier::from_u8(4), None);
        assert_eq!(StorageTier::from_u8(255), None);
    }

    #[test]
    fn file_entry_page_size_variants() {
        // 4KB pages
        let entry_4k = FileEntry {
            file_id: 10,
            file_type: PageType::KvData as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 100,
            byte_size: 409_600,
            created_lsn: 1,
            min_key_hash: 0,
            max_key_hash: 0,
        };
        let mut buf = [0u8; 48];
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
        let buf = [0u8; 47];
        assert!(FileEntry::read_from(&buf).is_none());
    }
}
