//! MoonPage format — universal 64-byte header for all persistent pages.
//!
//! Every on-disk page in MoonStore v2 starts with this header.
//! CRC32C checksum is computed over the payload region `[64..64+payload_bytes]`.

/// Magic bytes: "MNPG" in little-endian.
pub const MOONPAGE_MAGIC: u32 = 0x4D4E_5047;

/// Header size in bytes — fixed at 64.
pub const MOONPAGE_HEADER_SIZE: usize = 64;

/// Standard 4KB page size (KV, graph, MVCC, metadata, control).
pub const PAGE_4K: usize = 4096;

/// Large 64KB page size (VecCodes, VecFull).
pub const PAGE_64K: usize = 65536;

/// Page type discriminant — determines page size and interpretation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Key-value data page (4KB).
    KvData = 0x01,
    /// Vector quantized codes page (64KB).
    VecCodes = 0x10,
    /// Vector full-precision page (64KB).
    VecFull = 0x11,
    /// Vector HNSW graph adjacency page (4KB).
    VecGraph = 0x12,
    /// Vector MVCC metadata page (4KB).
    VecMvcc = 0x13,
    /// General metadata page (4KB).
    Metadata = 0x20,
    /// Shard control file page (4KB).
    Control = 0x30,
    /// Manifest root page (4KB).
    ManifestRoot = 0x31,
    /// CLOG commit-log page (4KB) — 2-bit transaction status.
    ClogPage = 0x32,
}

impl PageType {
    /// Returns the on-disk page size for this page type.
    #[inline]
    pub fn page_size(self) -> usize {
        match self {
            Self::VecCodes | Self::VecFull => PAGE_64K,
            _ => PAGE_4K,
        }
    }

    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::KvData),
            0x10 => Some(Self::VecCodes),
            0x11 => Some(Self::VecFull),
            0x12 => Some(Self::VecGraph),
            0x13 => Some(Self::VecMvcc),
            0x20 => Some(Self::Metadata),
            0x30 => Some(Self::Control),
            0x31 => Some(Self::ManifestRoot),
            0x32 => Some(Self::ClogPage),
            _ => None,
        }
    }
}

/// Bitflags for page-level flags (u16).
pub mod page_flags {
    /// Page contains a full-page image (FPI) for torn-page defense.
    pub const FPI: u16 = 1 << 0;
    /// Page payload is LZ4-compressed.
    pub const COMPRESSED: u16 = 1 << 1;
    /// Page has been dirtied since last checkpoint.
    pub const DIRTY: u16 = 1 << 2;
}

/// Universal 64-byte MoonPage header.
///
/// Byte layout (all little-endian):
/// ```text
/// Offset  Size  Field
/// 0       4     magic (0x4D4E5047 LE)
/// 4       1     format_version (1)
/// 5       1     page_type (PageType as u8)
/// 6       2     flags (u16 LE)
/// 8       8     page_lsn (u64 LE)
/// 16      4     checksum (u32 LE, CRC32C of payload)
/// 20      4     payload_bytes (u32 LE)
/// 24      8     page_id (u64 LE)
/// 32      8     file_id (u64 LE)
/// 40      4     prev_page (u32 LE)
/// 44      4     next_page (u32 LE)
/// 48      8     txn_id (u64 LE)
/// 56      4     entry_count (u32 LE)
/// 60      4     reserved (u32 LE, always 0)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoonPageHeader {
    pub magic: u32,
    pub format_version: u8,
    pub page_type: PageType,
    pub flags: u16,
    pub page_lsn: u64,
    pub checksum: u32,
    pub payload_bytes: u32,
    pub page_id: u64,
    pub file_id: u64,
    pub prev_page: u32,
    pub next_page: u32,
    pub txn_id: u64,
    pub entry_count: u32,
    pub reserved: u32,
}

impl MoonPageHeader {
    /// Create a new header with default values.
    ///
    /// Sets magic, format_version=1, and zeroes all other fields.
    pub fn new(page_type: PageType, page_id: u64, file_id: u64) -> Self {
        Self {
            magic: MOONPAGE_MAGIC,
            format_version: 1,
            page_type,
            flags: 0,
            page_lsn: 0,
            checksum: 0,
            payload_bytes: 0,
            page_id,
            file_id,
            prev_page: 0,
            next_page: 0,
            txn_id: 0,
            entry_count: 0,
            reserved: 0,
        }
    }

    /// Serialize the header into the first 64 bytes of `buf`.
    ///
    /// # Panics
    ///
    /// Panics if `buf.len() < 64`.
    pub fn write_to(&self, buf: &mut [u8]) {
        assert!(
            buf.len() >= MOONPAGE_HEADER_SIZE,
            "buffer too small for MoonPageHeader: {} < {}",
            buf.len(),
            MOONPAGE_HEADER_SIZE,
        );

        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4] = self.format_version;
        buf[5] = self.page_type as u8;
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.page_lsn.to_le_bytes());
        buf[16..20].copy_from_slice(&self.checksum.to_le_bytes());
        buf[20..24].copy_from_slice(&self.payload_bytes.to_le_bytes());
        buf[24..32].copy_from_slice(&self.page_id.to_le_bytes());
        buf[32..40].copy_from_slice(&self.file_id.to_le_bytes());
        buf[40..44].copy_from_slice(&self.prev_page.to_le_bytes());
        buf[44..48].copy_from_slice(&self.next_page.to_le_bytes());
        buf[48..56].copy_from_slice(&self.txn_id.to_le_bytes());
        buf[56..60].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[60..64].copy_from_slice(&self.reserved.to_le_bytes());
    }

    /// Deserialize a header from the first 64 bytes of `buf`.
    ///
    /// Returns `None` if the buffer is too small or magic doesn't match.
    pub fn read_from(buf: &[u8]) -> Option<Self> {
        if buf.len() < MOONPAGE_HEADER_SIZE {
            return None;
        }

        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != MOONPAGE_MAGIC {
            return None;
        }

        let format_version = buf[4];
        let page_type = PageType::from_u8(buf[5])?;
        let flags = u16::from_le_bytes([buf[6], buf[7]]);
        let page_lsn = u64::from_le_bytes(buf[8..16].try_into().ok()?);
        let checksum = u32::from_le_bytes(buf[16..20].try_into().ok()?);
        let payload_bytes = u32::from_le_bytes(buf[20..24].try_into().ok()?);
        let page_id = u64::from_le_bytes(buf[24..32].try_into().ok()?);
        let file_id = u64::from_le_bytes(buf[32..40].try_into().ok()?);
        let prev_page = u32::from_le_bytes(buf[40..44].try_into().ok()?);
        let next_page = u32::from_le_bytes(buf[44..48].try_into().ok()?);
        let txn_id = u64::from_le_bytes(buf[48..56].try_into().ok()?);
        let entry_count = u32::from_le_bytes(buf[56..60].try_into().ok()?);
        let reserved = u32::from_le_bytes(buf[60..64].try_into().ok()?);

        Some(Self {
            magic,
            format_version,
            page_type,
            flags,
            page_lsn,
            checksum,
            payload_bytes,
            page_id,
            file_id,
            prev_page,
            next_page,
            txn_id,
            entry_count,
            reserved,
        })
    }

    /// Compute CRC32C over the payload region and write it into the header.
    ///
    /// Reads `payload_bytes` from offset 20..24, computes CRC32C over
    /// `page[64..64+payload_bytes]`, and writes the result to offset 16..20.
    ///
    /// # Panics
    ///
    /// Panics if the page buffer is too small for header + payload.
    pub fn compute_checksum(page: &mut [u8]) {
        let payload_bytes =
            u32::from_le_bytes([page[20], page[21], page[22], page[23]]) as usize;
        let end = MOONPAGE_HEADER_SIZE + payload_bytes;
        assert!(
            page.len() >= end,
            "page buffer too small for checksum: {} < {}",
            page.len(),
            end,
        );

        let crc = crc32c::crc32c(&page[MOONPAGE_HEADER_SIZE..end]);
        page[16..20].copy_from_slice(&crc.to_le_bytes());
    }

    /// Verify the CRC32C checksum stored in the header against the payload.
    ///
    /// Returns `true` if the stored checksum matches the recomputed value.
    pub fn verify_checksum(page: &[u8]) -> bool {
        if page.len() < MOONPAGE_HEADER_SIZE {
            return false;
        }

        let payload_bytes =
            u32::from_le_bytes([page[20], page[21], page[22], page[23]]) as usize;
        let end = MOONPAGE_HEADER_SIZE + payload_bytes;
        if page.len() < end {
            return false;
        }

        let stored = u32::from_le_bytes([page[16], page[17], page[18], page[19]]);
        let computed = crc32c::crc32c(&page[MOONPAGE_HEADER_SIZE..end]);
        stored == computed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_to_produces_64_bytes_with_correct_magic() {
        let hdr = MoonPageHeader::new(PageType::KvData, 42, 7);
        let mut buf = [0u8; 128];
        hdr.write_to(&mut buf);

        // Magic at offset 0..4
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(magic, 0x4D4E_5047);

        // Exactly 64 bytes of header (rest should be untouched zeros)
        assert_eq!(buf[64..128], [0u8; 64]);
    }

    #[test]
    fn test_read_from_roundtrips_all_fields() {
        let mut hdr = MoonPageHeader::new(PageType::VecGraph, 100, 200);
        hdr.format_version = 1;
        hdr.flags = 0x0003;
        hdr.page_lsn = 999_999;
        hdr.checksum = 0xDEAD_BEEF;
        hdr.payload_bytes = 512;
        hdr.prev_page = 10;
        hdr.next_page = 20;
        hdr.txn_id = 77;
        hdr.entry_count = 33;
        hdr.reserved = 0;

        let mut buf = [0u8; 64];
        hdr.write_to(&mut buf);

        let parsed = MoonPageHeader::read_from(&buf).expect("should parse");
        assert_eq!(parsed, hdr);
    }

    #[test]
    fn test_compute_checksum_embeds_crc32c() {
        let mut page = vec![0u8; PAGE_4K];
        let mut hdr = MoonPageHeader::new(PageType::KvData, 1, 1);
        hdr.payload_bytes = 100;
        hdr.write_to(&mut page);

        // Write some payload
        for i in 0..100 {
            page[MOONPAGE_HEADER_SIZE + i] = (i & 0xFF) as u8;
        }
        // Re-write payload_bytes (already there from write_to)

        MoonPageHeader::compute_checksum(&mut page);

        // Checksum at offset 16..20 should be non-zero
        let stored = u32::from_le_bytes([page[16], page[17], page[18], page[19]]);
        assert_ne!(stored, 0);

        // Verify it matches CRC32C of the payload region
        let expected = crc32c::crc32c(&page[64..164]);
        assert_eq!(stored, expected);
    }

    #[test]
    fn test_verify_checksum_valid_and_corrupted() {
        let mut page = vec![0u8; PAGE_4K];
        let mut hdr = MoonPageHeader::new(PageType::Metadata, 5, 5);
        hdr.payload_bytes = 200;
        hdr.write_to(&mut page);

        // Fill payload
        for i in 0..200 {
            page[MOONPAGE_HEADER_SIZE + i] = ((i * 7) & 0xFF) as u8;
        }

        MoonPageHeader::compute_checksum(&mut page);
        assert!(MoonPageHeader::verify_checksum(&page));

        // Corrupt a payload byte
        page[MOONPAGE_HEADER_SIZE + 50] ^= 0xFF;
        assert!(!MoonPageHeader::verify_checksum(&page));
    }

    #[test]
    fn test_page_type_sizes() {
        assert_eq!(PageType::KvData.page_size(), PAGE_4K);
        assert_eq!(PageType::VecGraph.page_size(), PAGE_4K);
        assert_eq!(PageType::VecMvcc.page_size(), PAGE_4K);
        assert_eq!(PageType::Metadata.page_size(), PAGE_4K);
        assert_eq!(PageType::Control.page_size(), PAGE_4K);
        assert_eq!(PageType::ManifestRoot.page_size(), PAGE_4K);
        assert_eq!(PageType::VecCodes.page_size(), PAGE_64K);
        assert_eq!(PageType::VecFull.page_size(), PAGE_64K);
    }

    #[test]
    fn test_edge_lsn_values() {
        // page_lsn = 0
        let mut hdr = MoonPageHeader::new(PageType::Control, 0, 0);
        hdr.page_lsn = 0;
        let mut buf = [0u8; 64];
        hdr.write_to(&mut buf);
        let parsed = MoonPageHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.page_lsn, 0);

        // page_lsn = u64::MAX
        hdr.page_lsn = u64::MAX;
        hdr.write_to(&mut buf);
        let parsed = MoonPageHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.page_lsn, u64::MAX);
    }

    #[test]
    fn test_read_from_rejects_bad_magic() {
        let mut buf = [0u8; 64];
        buf[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        assert!(MoonPageHeader::read_from(&buf).is_none());
    }

    #[test]
    fn test_read_from_rejects_short_buffer() {
        let buf = [0u8; 32];
        assert!(MoonPageHeader::read_from(&buf).is_none());
    }

    #[test]
    fn test_page_type_from_u8_roundtrip() {
        let types = [
            PageType::KvData,
            PageType::VecCodes,
            PageType::VecFull,
            PageType::VecGraph,
            PageType::VecMvcc,
            PageType::Metadata,
            PageType::Control,
            PageType::ManifestRoot,
        ];
        for pt in types {
            assert_eq!(PageType::from_u8(pt as u8), Some(pt));
        }
        assert_eq!(PageType::from_u8(0xFF), None);
    }
}
