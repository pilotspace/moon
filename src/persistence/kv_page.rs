//! KvLeaf slotted page format and DataFile (.mpf) reader/writer.
//!
//! Implements the on-disk KV storage format per MOONSTORE-V2-COMPREHENSIVE-DESIGN.md section 6.
//! This is FORMAT ONLY -- no hot-path integration.
//!
//! Page layout (4KB):
//! ```text
//! [MoonPage Header 64B][KV Header 16B][Slot Array ->][<- free space ->][<- Entries]
//! ```

use std::fmt;
use std::io;
use std::path::Path;

use crate::persistence::page::{
    MoonPageHeader, PageType, MOONPAGE_HEADER_SIZE, PAGE_4K,
};

/// Size of the KV-specific page header (offsets 64..80).
pub const KV_PAGE_HEADER_SIZE: usize = 16;

/// Size of a single slot entry (offset:u16 + len:u16).
pub const SLOT_SIZE: usize = 4;

/// Start of KV payload area (after MoonPage header + KV header).
const KV_DATA_START: usize = MOONPAGE_HEADER_SIZE + KV_PAGE_HEADER_SIZE;

// ── KV page header field offsets (relative to MOONPAGE_HEADER_SIZE = 64) ──

const OFF_FREE_START: usize = MOONPAGE_HEADER_SIZE;      // u16 at 64
const OFF_FREE_END: usize = MOONPAGE_HEADER_SIZE + 2;    // u16 at 66
const _OFF_KV_FLAGS: usize = MOONPAGE_HEADER_SIZE + 4;    // u16 at 68
const OFF_SLOT_COUNT: usize = MOONPAGE_HEADER_SIZE + 6;  // u16 at 70
const _OFF_BASE_TS: usize = MOONPAGE_HEADER_SIZE + 8;     // u32 at 72
const _OFF_COMPACT_GEN: usize = MOONPAGE_HEADER_SIZE + 12; // u32 at 76

// ── Value type discriminant ─────────────────────────────

/// Type of the stored value. Matches Redis type semantics.
///
/// Discriminants are part of the on-disk format and MUST NOT change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    String = 0,
    Hash   = 1,
    List   = 2,
    Set    = 3,
    ZSet   = 4,
    Stream = 5,
}

impl ValueType {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::String),
            1 => Some(Self::Hash),
            2 => Some(Self::List),
            3 => Some(Self::Set),
            4 => Some(Self::ZSet),
            5 => Some(Self::Stream),
            _ => None,
        }
    }
}

// ── Entry flags (bitfield) ──────────────────────────────

/// Bitflags for per-entry metadata.
pub mod entry_flags {
    /// TTL field is present (8 bytes).
    pub const HAS_TTL: u8 = 0x01;
    /// Value payload is LZ4-compressed.
    pub const COMPRESSED: u8 = 0x02;
    /// Value is an overflow pointer (file_id:u64 + page_id:u32 = 12 bytes).
    pub const OVERFLOW: u8 = 0x04;
    /// Entry is a tombstone (pending compaction). value_len = 0.
    pub const TOMBSTONE: u8 = 0x08;
}

// ── KvEntry (decoded view) ──────────────────────────────

/// Decoded key-value entry returned by [`KvLeafPage::get`].
///
/// This is a read-side view -- allocations (Vec) are acceptable since this
/// is the cold tier read path, not the hot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub value_type: ValueType,
    pub flags: u8,
    pub ttl_ms: Option<u64>,
}

// ── PageFull error ──────────────────────────────────────

/// Error returned when a page has insufficient free space for an insert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageFull;

impl fmt::Display for PageFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("page full: insufficient free space for entry + slot")
    }
}

impl std::error::Error for PageFull {}

// ── KvLeafPage ──────────────────────────────────────────

/// A 4KB slotted page for KV storage.
///
/// Slot array grows downward from offset 80; entries grow upward from the
/// bottom of the page. Free space is the gap between slot array end and
/// entry area start.
pub struct KvLeafPage {
    data: [u8; PAGE_4K],
}

impl KvLeafPage {
    /// Create a new empty KvLeaf page with the given identifiers.
    pub fn new(page_id: u64, file_id: u64) -> Self {
        let mut data = [0u8; PAGE_4K];

        // Write MoonPage universal header
        let hdr = MoonPageHeader::new(PageType::KvLeaf, page_id, file_id);
        hdr.write_to(&mut data);

        // Write KV page header
        let free_start = KV_DATA_START as u16; // 80
        let free_end = PAGE_4K as u16;         // 4096
        data[OFF_FREE_START..OFF_FREE_START + 2]
            .copy_from_slice(&free_start.to_le_bytes());
        data[OFF_FREE_END..OFF_FREE_END + 2]
            .copy_from_slice(&free_end.to_le_bytes());
        // kv_flags, slot_count, base_timestamp, compaction_gen: all zero

        Self { data }
    }

    // ── KV header accessors ─────────────────────────────

    #[inline]
    fn free_start(&self) -> u16 {
        u16::from_le_bytes([self.data[OFF_FREE_START], self.data[OFF_FREE_START + 1]])
    }

    #[inline]
    fn set_free_start(&mut self, v: u16) {
        self.data[OFF_FREE_START..OFF_FREE_START + 2].copy_from_slice(&v.to_le_bytes());
    }

    #[inline]
    fn free_end(&self) -> u16 {
        u16::from_le_bytes([self.data[OFF_FREE_END], self.data[OFF_FREE_END + 1]])
    }

    #[inline]
    fn set_free_end(&mut self, v: u16) {
        self.data[OFF_FREE_END..OFF_FREE_END + 2].copy_from_slice(&v.to_le_bytes());
    }

    /// Number of live slot entries in this page.
    #[inline]
    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes([self.data[OFF_SLOT_COUNT], self.data[OFF_SLOT_COUNT + 1]])
    }

    #[inline]
    fn set_slot_count(&mut self, v: u16) {
        self.data[OFF_SLOT_COUNT..OFF_SLOT_COUNT + 2].copy_from_slice(&v.to_le_bytes());
    }

    /// Remaining free bytes in this page.
    #[inline]
    pub fn free_space(&self) -> usize {
        let fs = self.free_start() as usize;
        let fe = self.free_end() as usize;
        fe.saturating_sub(fs)
    }

    // ── Entry size computation ──────────────────────────

    /// Compute the serialized size of an entry (excluding slot).
    #[inline]
    fn entry_size(key_len: usize, value_len: usize, flags: u8) -> usize {
        let ttl_size = if flags & entry_flags::HAS_TTL != 0 { 8 } else { 0 };
        2 /* key_len */ + 1 /* value_type */ + 1 /* flags */ + ttl_size + key_len + 4 /* value_len */ + value_len
    }

    // ── Insert ──────────────────────────────────────────

    /// Insert a key-value entry into the page.
    ///
    /// Returns the slot index on success, or `Err(PageFull)` if there is
    /// insufficient space.
    pub fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        value_type: ValueType,
        flags: u8,
        ttl_ms: Option<u64>,
    ) -> Result<u16, PageFull> {
        // Compute actual flags: set HAS_TTL if ttl provided
        let mut actual_flags = flags;
        if ttl_ms.is_some() {
            actual_flags |= entry_flags::HAS_TTL;
        }

        // If TOMBSTONE, value_len must be 0
        let value_bytes = if actual_flags & entry_flags::TOMBSTONE != 0 {
            &[] as &[u8]
        } else {
            value
        };

        let e_size = Self::entry_size(key.len(), value_bytes.len(), actual_flags);
        let needed = e_size + SLOT_SIZE;

        let fs = self.free_start() as usize;
        let fe = self.free_end() as usize;

        if fe < fs + needed {
            return Err(PageFull);
        }

        // Write entry at (free_end - entry_size)..free_end (entries grow up from bottom)
        let entry_offset = fe - e_size;
        let mut cursor = entry_offset;

        // key_len: u16 LE
        self.data[cursor..cursor + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        cursor += 2;

        // value_type: u8
        self.data[cursor] = value_type as u8;
        cursor += 1;

        // entry_flags: u8
        self.data[cursor] = actual_flags;
        cursor += 1;

        // optional ttl_ms: u64 LE
        if let Some(ttl) = ttl_ms {
            self.data[cursor..cursor + 8].copy_from_slice(&ttl.to_le_bytes());
            cursor += 8;
        }

        // key bytes
        self.data[cursor..cursor + key.len()].copy_from_slice(key);
        cursor += key.len();

        // value_len: u32 LE
        self.data[cursor..cursor + 4].copy_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        cursor += 4;

        // value bytes
        if !value_bytes.is_empty() {
            self.data[cursor..cursor + value_bytes.len()].copy_from_slice(value_bytes);
        }

        // Write slot at free_start position: offset:u16 + len:u16
        let slot_offset = fs;
        self.data[slot_offset..slot_offset + 2]
            .copy_from_slice(&(entry_offset as u16).to_le_bytes());
        self.data[slot_offset + 2..slot_offset + 4]
            .copy_from_slice(&(e_size as u16).to_le_bytes());

        // Update page metadata
        let new_slot_count = self.slot_count() + 1;
        self.set_free_start((fs + SLOT_SIZE) as u16);
        self.set_free_end(entry_offset as u16);
        self.set_slot_count(new_slot_count);

        // Update entry_count in MoonPageHeader (offset 56..60)
        self.data[56..60].copy_from_slice(&(new_slot_count as u32).to_le_bytes());

        Ok(new_slot_count - 1)
    }

    // ── Get ─────────────────────────────────────────────

    /// Retrieve a decoded entry by slot index.
    ///
    /// Returns `None` if `slot_index >= slot_count`.
    pub fn get(&self, slot_index: u16) -> Option<KvEntry> {
        if slot_index >= self.slot_count() {
            return None;
        }

        // Read slot: offset at KV_DATA_START + slot_index * SLOT_SIZE
        let slot_pos = KV_DATA_START + (slot_index as usize) * SLOT_SIZE;
        let entry_offset = u16::from_le_bytes([
            self.data[slot_pos],
            self.data[slot_pos + 1],
        ]) as usize;
        let _entry_len = u16::from_le_bytes([
            self.data[slot_pos + 2],
            self.data[slot_pos + 3],
        ]) as usize;

        let mut cursor = entry_offset;

        // key_len: u16 LE
        let key_len = u16::from_le_bytes([
            self.data[cursor],
            self.data[cursor + 1],
        ]) as usize;
        cursor += 2;

        // value_type: u8
        let vt = ValueType::from_u8(self.data[cursor])?;
        cursor += 1;

        // entry_flags: u8
        let flags = self.data[cursor];
        cursor += 1;

        // optional ttl_ms
        let ttl_ms = if flags & entry_flags::HAS_TTL != 0 {
            let ttl = u64::from_le_bytes(
                self.data[cursor..cursor + 8].try_into().ok()?,
            );
            cursor += 8;
            Some(ttl)
        } else {
            None
        };

        // key bytes
        let key = self.data[cursor..cursor + key_len].to_vec();
        cursor += key_len;

        // value_len: u32 LE
        let value_len = u32::from_le_bytes(
            self.data[cursor..cursor + 4].try_into().ok()?,
        ) as usize;
        cursor += 4;

        // value bytes
        let value = self.data[cursor..cursor + value_len].to_vec();

        Some(KvEntry {
            key,
            value,
            value_type: vt,
            flags,
            ttl_ms,
        })
    }

    /// Return the raw page bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; PAGE_4K] {
        &self.data
    }

    /// Construct a page from raw bytes, validating the header.
    ///
    /// Returns `None` if magic or page_type is invalid.
    pub fn from_bytes(data: [u8; PAGE_4K]) -> Option<Self> {
        let hdr = MoonPageHeader::read_from(&data)?;
        if hdr.page_type != PageType::KvLeaf {
            return None;
        }
        Some(Self { data })
    }

    /// Finalize the page: set payload_bytes in MoonPageHeader and compute
    /// CRC32C checksum over the payload region.
    pub fn finalize(&mut self) {
        let payload_bytes = (PAGE_4K - MOONPAGE_HEADER_SIZE) as u32;
        self.data[20..24].copy_from_slice(&payload_bytes.to_le_bytes());
        MoonPageHeader::compute_checksum(&mut self.data);
    }
}

// ── DataFile I/O ────────────────────────────────────────

/// Write a sequence of KvLeaf pages to a `.mpf` DataFile.
///
/// Each page is written as a raw 4KB block. The file is fsynced after writing.
pub fn write_datafile(path: &Path, pages: &[&KvLeafPage]) -> io::Result<()> {
    use std::io::Write;

    let mut file = std::fs::File::create(path)?;
    for page in pages {
        file.write_all(&page.data)?;
    }
    file.sync_all()?;
    Ok(())
}

/// Read a `.mpf` DataFile into a vector of KvLeaf pages.
///
/// Validates each 4KB chunk as a KvLeaf page. Returns an error if any
/// page fails validation or the file size is not a multiple of 4KB.
pub fn read_datafile(path: &Path) -> io::Result<Vec<KvLeafPage>> {
    let contents = std::fs::read(path)?;
    if contents.len() % PAGE_4K != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DataFile size is not a multiple of 4KB",
        ));
    }

    let mut pages = Vec::with_capacity(contents.len() / PAGE_4K);
    for chunk in contents.chunks_exact(PAGE_4K) {
        let mut buf = [0u8; PAGE_4K];
        buf.copy_from_slice(chunk);
        let page = KvLeafPage::from_bytes(buf).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid KvLeaf page in DataFile")
        })?;
        pages.push(page);
    }

    Ok(pages)
}

// ── Tests ───────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_get_roundtrip_basic() {
        let mut page = KvLeafPage::new(1, 1);
        let idx = page.insert(b"key1", b"value1", ValueType::String, 0, None)
            .expect("insert should succeed");
        assert_eq!(idx, 0);
        assert_eq!(page.slot_count(), 1);

        let entry = page.get(0).expect("get should succeed");
        assert_eq!(entry.key, b"key1");
        assert_eq!(entry.value, b"value1");
        assert_eq!(entry.value_type, ValueType::String);
        assert_eq!(entry.flags, 0);
        assert_eq!(entry.ttl_ms, None);
    }

    #[test]
    fn test_insert_with_ttl() {
        let mut page = KvLeafPage::new(2, 1);
        let ttl = 60_000u64; // 60 seconds
        page.insert(b"ephemeral", b"data", ValueType::String, 0, Some(ttl))
            .expect("insert should succeed");

        let entry = page.get(0).unwrap();
        assert_eq!(entry.flags & entry_flags::HAS_TTL, entry_flags::HAS_TTL);
        assert_eq!(entry.ttl_ms, Some(60_000));
    }

    #[test]
    fn test_insert_overflow_pointer() {
        let mut page = KvLeafPage::new(3, 1);
        // Overflow pointer: file_id(u64) + page_id(u32) = 12 bytes
        let mut overflow_val = [0u8; 12];
        overflow_val[..8].copy_from_slice(&42u64.to_le_bytes());   // file_id
        overflow_val[8..12].copy_from_slice(&100u32.to_le_bytes()); // page_id

        page.insert(b"big_key", &overflow_val, ValueType::Hash, entry_flags::OVERFLOW, None)
            .expect("insert should succeed");

        let entry = page.get(0).unwrap();
        assert_eq!(entry.flags & entry_flags::OVERFLOW, entry_flags::OVERFLOW);
        assert_eq!(entry.value.len(), 12);
        let file_id = u64::from_le_bytes(entry.value[..8].try_into().unwrap());
        let pg_id = u32::from_le_bytes(entry.value[8..12].try_into().unwrap());
        assert_eq!(file_id, 42);
        assert_eq!(pg_id, 100);
    }

    #[test]
    fn test_insert_tombstone() {
        let mut page = KvLeafPage::new(4, 1);
        page.insert(b"deleted_key", b"ignored", ValueType::String, entry_flags::TOMBSTONE, None)
            .expect("insert should succeed");

        let entry = page.get(0).unwrap();
        assert_eq!(entry.flags & entry_flags::TOMBSTONE, entry_flags::TOMBSTONE);
        assert_eq!(entry.value.len(), 0);
    }

    #[test]
    fn test_value_type_roundtrip() {
        let types = [
            ValueType::String,
            ValueType::Hash,
            ValueType::List,
            ValueType::Set,
            ValueType::ZSet,
            ValueType::Stream,
        ];
        let mut page = KvLeafPage::new(5, 1);
        for (i, vt) in types.iter().enumerate() {
            let key = format!("key_{i}");
            page.insert(key.as_bytes(), b"v", *vt, 0, None)
                .expect("insert should succeed");
        }
        for (i, vt) in types.iter().enumerate() {
            let entry = page.get(i as u16).unwrap();
            assert_eq!(entry.value_type, *vt, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_page_full() {
        let mut page = KvLeafPage::new(6, 1);
        // Available space: 4096 - 80 = 4016 bytes
        // First insert: 3(key) + 3990(val) + 8(overhead) + 4(slot) = 4005 bytes
        let big_value = vec![0xAB; 3990];
        page.insert(b"big", &big_value, ValueType::String, 0, None)
            .expect("first big insert should fit");

        // Remaining: 4016 - 4005 = 11 bytes. Second needs at least 4(slot) + 8(overhead) + key + val = 22
        let result = page.insert(b"another", b"val", ValueType::String, 0, None);
        assert_eq!(result, Err(PageFull));
    }

    #[test]
    fn test_multiple_inserts_all_retrievable() {
        let mut page = KvLeafPage::new(7, 1);
        let count = 50;
        for i in 0..count {
            let key = format!("key_{i:04}");
            let val = format!("val_{i:04}");
            page.insert(key.as_bytes(), val.as_bytes(), ValueType::String, 0, None)
                .unwrap_or_else(|_| panic!("insert {i} should succeed"));
        }
        assert_eq!(page.slot_count(), count);

        for i in 0..count {
            let entry = page.get(i).unwrap_or_else(|| panic!("get {i} should succeed"));
            let expected_key = format!("key_{i:04}");
            let expected_val = format!("val_{i:04}");
            assert_eq!(entry.key, expected_key.as_bytes());
            assert_eq!(entry.value, expected_val.as_bytes());
        }
    }

    #[test]
    fn test_get_out_of_bounds() {
        let page = KvLeafPage::new(8, 1);
        assert!(page.get(0).is_none());
        assert!(page.get(100).is_none());
    }

    #[test]
    fn test_finalize_checksum() {
        let mut page = KvLeafPage::new(9, 1);
        page.insert(b"foo", b"bar", ValueType::String, 0, None).unwrap();
        page.finalize();

        assert!(MoonPageHeader::verify_checksum(&page.data));

        // Corrupt a byte and verify checksum fails
        page.data[100] ^= 0xFF;
        assert!(!MoonPageHeader::verify_checksum(&page.data));
    }

    #[test]
    fn test_from_bytes_valid() {
        let mut page = KvLeafPage::new(10, 2);
        page.insert(b"test", b"data", ValueType::List, 0, None).unwrap();
        page.finalize();

        let bytes = *page.as_bytes();
        let restored = KvLeafPage::from_bytes(bytes).expect("should parse valid page");
        let entry = restored.get(0).unwrap();
        assert_eq!(entry.key, b"test");
        assert_eq!(entry.value, b"data");
        assert_eq!(entry.value_type, ValueType::List);
    }

    #[test]
    fn test_from_bytes_rejects_bad_type() {
        let mut data = [0u8; PAGE_4K];
        let hdr = MoonPageHeader::new(PageType::KvOverflow, 1, 1);
        hdr.write_to(&mut data);

        assert!(KvLeafPage::from_bytes(data).is_none());
    }

    #[test]
    fn test_datafile_roundtrip() {
        let dir = std::env::temp_dir().join("moon_test_datafile");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test-heap.mpf");

        let mut p1 = KvLeafPage::new(0, 1);
        p1.insert(b"k1", b"v1", ValueType::String, 0, None).unwrap();
        p1.finalize();

        let mut p2 = KvLeafPage::new(1, 1);
        p2.insert(b"k2", b"v2", ValueType::Hash, 0, Some(5000)).unwrap();
        p2.finalize();

        write_datafile(&path, &[&p1, &p2]).expect("write should succeed");

        let pages = read_datafile(&path).expect("read should succeed");
        assert_eq!(pages.len(), 2);

        let e1 = pages[0].get(0).unwrap();
        assert_eq!(e1.key, b"k1");
        assert_eq!(e1.value, b"v1");

        let e2 = pages[1].get(0).unwrap();
        assert_eq!(e2.key, b"k2");
        assert_eq!(e2.value, b"v2");
        assert_eq!(e2.ttl_ms, Some(5000));

        // Cleanup
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_free_space_decreases() {
        let mut page = KvLeafPage::new(11, 1);
        let initial = page.free_space();
        assert_eq!(initial, PAGE_4K - KV_DATA_START); // 4096 - 80 = 4016

        page.insert(b"k", b"v", ValueType::String, 0, None).unwrap();
        let after = page.free_space();
        assert!(after < initial);
    }

    #[test]
    fn test_insert_with_ttl_and_overflow() {
        let mut page = KvLeafPage::new(12, 1);
        let mut ptr = [0u8; 12];
        ptr[..8].copy_from_slice(&99u64.to_le_bytes());
        ptr[8..12].copy_from_slice(&7u32.to_le_bytes());

        page.insert(
            b"combo_key",
            &ptr,
            ValueType::ZSet,
            entry_flags::OVERFLOW,
            Some(120_000),
        ).unwrap();

        let entry = page.get(0).unwrap();
        assert_eq!(entry.flags & entry_flags::HAS_TTL, entry_flags::HAS_TTL);
        assert_eq!(entry.flags & entry_flags::OVERFLOW, entry_flags::OVERFLOW);
        assert_eq!(entry.ttl_ms, Some(120_000));
        assert_eq!(entry.value.len(), 12);
    }

    #[test]
    fn test_value_type_from_u8() {
        assert_eq!(ValueType::from_u8(0), Some(ValueType::String));
        assert_eq!(ValueType::from_u8(5), Some(ValueType::Stream));
        assert_eq!(ValueType::from_u8(6), None);
        assert_eq!(ValueType::from_u8(255), None);
    }
}
