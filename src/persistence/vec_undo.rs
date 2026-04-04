//! VecUndo page — variable-length undo log records for vector metadata updates.
//!
//! Enables MVCC without copying full 3KB+ vectors. Only changed metadata fields
//! are stored, reducing write amplification by ~100x for the common case.
//!
//! On-disk layout per MOONSTORE-V2-COMPREHENSIVE-DESIGN.md Section 7.6:
//! ```text
//! [MoonPage Header, 64 bytes, type=VecUndo]
//! UndoPage Header (8 bytes):
//!   write_offset: u32       next free byte in page
//!   record_count: u32
//!
//! Undo Records (variable length):
//!   prev_undo_ptr: u32      chain to older version (0 = end)
//!   txn_id: u64             transaction that created this undo record
//!   vector_id: u32          which vector this belongs to
//!   flags: u16              UNDO_INSERT=1 UNDO_UPDATE=2 UNDO_DELETE=3
//!   old_data_len: u16       length of before-image
//!   old_data: [u8]          only changed fields (NOT the full vector)
//! ```

use crate::persistence::page::{MoonPageHeader, PageType, MOONPAGE_HEADER_SIZE};

/// Undo record operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum UndoFlags {
    /// Vector was inserted — undo = remove it.
    Insert = 1,
    /// Vector metadata was updated — undo = restore old fields.
    Update = 2,
    /// Vector was deleted — undo = restore it.
    Delete = 3,
}

impl UndoFlags {
    /// Deserialize from a raw u16.
    #[inline]
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::Insert),
            2 => Some(Self::Update),
            3 => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Fixed-size portion of each undo record: 18 bytes.
/// `prev_undo_ptr(4) + txn_id(8) + vector_id(4) + flags(2) = 18`
const UNDO_RECORD_HEADER: usize = 18;

/// Size of the `old_data_len` field: 2 bytes (u16 LE).
const UNDO_DATA_LEN_SIZE: usize = 2;

/// An undo record parsed from a VecUndoPage.
///
/// Contains only the changed metadata fields (not the full vector embedding),
/// enabling ~100x write amplification reduction for metadata-only updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoRecord {
    /// Byte offset of the previous undo record in the chain (0 = end of chain).
    pub prev_undo_ptr: u32,
    /// Transaction ID that created this undo record.
    pub txn_id: u64,
    /// Vector ID this undo record belongs to.
    pub vector_id: u32,
    /// Operation type (insert, update, delete).
    pub flags: UndoFlags,
    /// Before-image of changed fields only. Empty for delete tombstones.
    pub old_data: Vec<u8>,
}

/// Undo page header: `write_offset(4) + record_count(4) = 8 bytes`.
const UNDO_PAGE_HEADER: usize = 8;

/// Usable data region: `4096 - 64 (MoonPage header) - 8 (undo page header) = 4024 bytes`.
const UNDO_DATA_CAPACITY: usize = 4096 - MOONPAGE_HEADER_SIZE - UNDO_PAGE_HEADER;

/// First record starts at offset 1 (not 0) so that `prev_undo_ptr == 0`
/// unambiguously means "end of chain" per the design spec (Section 7.6).
const UNDO_DATA_START_OFFSET: u32 = 1;

/// Variable-length undo log page for vector metadata updates.
///
/// Each page is 4KB and contains a sequence of variable-length undo records.
/// Records are chained via `prev_undo_ptr` to form version chains for MVCC.
pub struct VecUndoPage {
    page_index: u64,
    file_id: u64,
    write_offset: u32,
    record_count: u32,
    data: [u8; UNDO_DATA_CAPACITY],
}

impl VecUndoPage {
    /// Create a new empty VecUndoPage.
    ///
    /// Write offset starts at 1 (not 0) so that `prev_undo_ptr == 0`
    /// is an unambiguous end-of-chain sentinel.
    pub fn new(page_index: u64, file_id: u64) -> Self {
        Self {
            page_index,
            file_id,
            write_offset: UNDO_DATA_START_OFFSET,
            record_count: 0,
            data: [0u8; UNDO_DATA_CAPACITY],
        }
    }

    /// Append an undo record. Returns the byte offset of the record within the
    /// data region, or `None` if the page cannot fit the record.
    pub fn append_record(&mut self, record: &UndoRecord) -> Option<u32> {
        let total_size = UNDO_RECORD_HEADER + UNDO_DATA_LEN_SIZE + record.old_data.len();
        if self.write_offset as usize + total_size > UNDO_DATA_CAPACITY {
            return None;
        }

        let offset = self.write_offset;
        let base = offset as usize;

        // Write fixed header fields (LE)
        self.data[base..base + 4].copy_from_slice(&record.prev_undo_ptr.to_le_bytes());
        self.data[base + 4..base + 12].copy_from_slice(&record.txn_id.to_le_bytes());
        self.data[base + 12..base + 16].copy_from_slice(&record.vector_id.to_le_bytes());
        self.data[base + 16..base + 18].copy_from_slice(&(record.flags as u16).to_le_bytes());

        // Write variable-length old_data
        let data_len = record.old_data.len() as u16;
        self.data[base + 18..base + 20].copy_from_slice(&data_len.to_le_bytes());
        if !record.old_data.is_empty() {
            self.data[base + 20..base + 20 + record.old_data.len()]
                .copy_from_slice(&record.old_data);
        }

        self.write_offset += total_size as u32;
        self.record_count += 1;
        Some(offset)
    }

    /// Read an undo record at the given byte offset within the data region.
    ///
    /// Returns `None` if the offset is out of bounds or the record is malformed.
    pub fn read_record(&self, offset: u32) -> Option<UndoRecord> {
        let base = offset as usize;
        if base + UNDO_RECORD_HEADER + UNDO_DATA_LEN_SIZE > self.write_offset as usize {
            return None;
        }

        let prev_undo_ptr = u32::from_le_bytes(
            self.data[base..base + 4].try_into().ok()?,
        );
        let txn_id = u64::from_le_bytes(
            self.data[base + 4..base + 12].try_into().ok()?,
        );
        let vector_id = u32::from_le_bytes(
            self.data[base + 12..base + 16].try_into().ok()?,
        );
        let flags_raw = u16::from_le_bytes(
            self.data[base + 16..base + 18].try_into().ok()?,
        );
        let flags = UndoFlags::from_u16(flags_raw)?;
        let data_len = u16::from_le_bytes(
            self.data[base + 18..base + 20].try_into().ok()?,
        ) as usize;

        if base + 20 + data_len > self.write_offset as usize {
            return None;
        }

        let old_data = self.data[base + 20..base + 20 + data_len].to_vec();
        Some(UndoRecord {
            prev_undo_ptr,
            txn_id,
            vector_id,
            flags,
            old_data,
        })
    }

    /// Traverse the undo chain starting from `start_offset`, collecting all
    /// records from newest to oldest.
    ///
    /// Follows `prev_undo_ptr` links until reaching 0 (end of chain).
    /// Includes a cycle guard at 1000 records to prevent infinite loops.
    pub fn chain_records(&self, start_offset: u32) -> Vec<UndoRecord> {
        let mut result = Vec::new();
        let mut current = start_offset;
        while let Some(record) = self.read_record(current) {
            let next = record.prev_undo_ptr;
            result.push(record);
            if next == current || next == 0 {
                // Self-referential or end-of-chain -- stop traversal.
                break;
            }
            current = next;
            // Cycle guard: undo chains should never be this long in a single page.
            if result.len() >= 1000 {
                break;
            }
        }
        result
    }

    /// Number of undo records in this page.
    #[inline]
    pub fn record_count(&self) -> u32 {
        self.record_count
    }

    /// Current write offset (next free byte in the data region).
    #[inline]
    pub fn write_offset(&self) -> u32 {
        self.write_offset
    }

    /// Serialize this page to a 4KB MoonPage buffer with CRC32C checksum.
    pub fn to_page(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        let mut hdr = MoonPageHeader::new(PageType::VecUndo, self.page_index, self.file_id);
        hdr.payload_bytes = self.write_offset + UNDO_PAGE_HEADER as u32;
        hdr.entry_count = self.record_count;
        hdr.write_to(&mut buf);

        let ph = MOONPAGE_HEADER_SIZE;
        buf[ph..ph + 4].copy_from_slice(&self.write_offset.to_le_bytes());
        buf[ph + 4..ph + 8].copy_from_slice(&self.record_count.to_le_bytes());
        let copy_len = self.write_offset as usize;
        buf[ph + 8..ph + 8 + copy_len].copy_from_slice(&self.data[..copy_len]);

        MoonPageHeader::compute_checksum(&mut buf);
        buf
    }

    /// Deserialize a VecUndoPage from a 4KB MoonPage buffer.
    ///
    /// Returns `None` if the page type is not `VecUndo` or the header is invalid.
    /// Checksum verification is the caller's responsibility via
    /// `MoonPageHeader::verify_checksum`.
    pub fn from_page(buf: &[u8; 4096]) -> Option<Self> {
        let hdr = MoonPageHeader::read_from(buf)?;
        if hdr.page_type != PageType::VecUndo {
            return None;
        }

        let ph = MOONPAGE_HEADER_SIZE;
        let write_offset = u32::from_le_bytes(buf[ph..ph + 4].try_into().ok()?);
        let record_count = u32::from_le_bytes(buf[ph + 4..ph + 8].try_into().ok()?);

        let mut data = [0u8; UNDO_DATA_CAPACITY];
        let copy_len = (write_offset as usize).min(UNDO_DATA_CAPACITY);
        data[..copy_len].copy_from_slice(&buf[ph + 8..ph + 8 + copy_len]);

        Some(Self {
            page_index: hdr.page_id,
            file_id: hdr.file_id,
            write_offset,
            record_count,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_flags_roundtrip() {
        assert_eq!(UndoFlags::from_u16(1), Some(UndoFlags::Insert));
        assert_eq!(UndoFlags::from_u16(2), Some(UndoFlags::Update));
        assert_eq!(UndoFlags::from_u16(3), Some(UndoFlags::Delete));
        assert_eq!(UndoFlags::from_u16(0), None);
        assert_eq!(UndoFlags::from_u16(4), None);
        assert_eq!(UndoFlags::from_u16(u16::MAX), None);
    }

    #[test]
    fn test_append_and_read_roundtrip() {
        let mut page = VecUndoPage::new(1, 100);
        let record = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 42,
            vector_id: 7,
            flags: UndoFlags::Insert,
            old_data: vec![1, 2, 3, 4],
        };

        let offset = page.append_record(&record);
        assert!(offset.is_some());
        let offset = offset.unwrap();
        assert_eq!(offset, 1); // First record at offset 1 (0 reserved as end-of-chain sentinel)
        assert_eq!(page.record_count(), 1);

        let read_back = page.read_record(offset);
        assert!(read_back.is_some());
        assert_eq!(read_back.unwrap(), record);
    }

    #[test]
    fn test_append_multiple_records() {
        let mut page = VecUndoPage::new(1, 100);

        let r1 = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 10,
            vector_id: 1,
            flags: UndoFlags::Insert,
            old_data: vec![0xAA; 8],
        };
        let off1 = page.append_record(&r1).unwrap();

        let r2 = UndoRecord {
            prev_undo_ptr: off1,
            txn_id: 20,
            vector_id: 2,
            flags: UndoFlags::Update,
            old_data: vec![0xBB; 16],
        };
        let off2 = page.append_record(&r2).unwrap();
        assert!(off2 > off1);

        let r3 = UndoRecord {
            prev_undo_ptr: off2,
            txn_id: 30,
            vector_id: 3,
            flags: UndoFlags::Delete,
            old_data: vec![],
        };
        let off3 = page.append_record(&r3).unwrap();
        assert!(off3 > off2);

        assert_eq!(page.record_count(), 3);

        // Read all back
        assert_eq!(page.read_record(off1).unwrap(), r1);
        assert_eq!(page.read_record(off2).unwrap(), r2);
        assert_eq!(page.read_record(off3).unwrap(), r3);
    }

    #[test]
    fn test_chain_traversal() {
        let mut page = VecUndoPage::new(1, 100);

        let r1 = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 100,
            vector_id: 5,
            flags: UndoFlags::Insert,
            old_data: vec![1, 2],
        };
        let off1 = page.append_record(&r1).unwrap();

        let r2 = UndoRecord {
            prev_undo_ptr: off1,
            txn_id: 200,
            vector_id: 5,
            flags: UndoFlags::Update,
            old_data: vec![3, 4, 5],
        };
        let off2 = page.append_record(&r2).unwrap();

        let r3 = UndoRecord {
            prev_undo_ptr: off2,
            txn_id: 300,
            vector_id: 5,
            flags: UndoFlags::Update,
            old_data: vec![6, 7, 8, 9],
        };
        let off3 = page.append_record(&r3).unwrap();

        // Traverse from newest to oldest
        let chain = page.chain_records(off3);
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].txn_id, 300);
        assert_eq!(chain[1].txn_id, 200);
        assert_eq!(chain[2].txn_id, 100);
    }

    #[test]
    fn test_chain_single_record() {
        let mut page = VecUndoPage::new(1, 100);
        let r = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 42,
            vector_id: 1,
            flags: UndoFlags::Delete,
            old_data: vec![],
        };
        let off = page.append_record(&r).unwrap();
        let chain = page.chain_records(off);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0], r);
    }

    #[test]
    fn test_page_full_detection() {
        let mut page = VecUndoPage::new(1, 100);
        // Each record with 200 bytes of old_data costs 20 (header+len) + 200 = 220 bytes.
        // UNDO_DATA_CAPACITY = 4096 - 64 - 8 = 4024. So 4024 / 220 = ~18 records fit.
        let big_data = vec![0xFF; 200];
        let mut count = 0u32;
        loop {
            let r = UndoRecord {
                prev_undo_ptr: 0,
                txn_id: count as u64,
                vector_id: count,
                flags: UndoFlags::Update,
                old_data: big_data.clone(),
            };
            if page.append_record(&r).is_none() {
                break;
            }
            count += 1;
        }
        // Should have fit ~18 records
        assert!(count >= 15);
        assert!(count <= 20);
        assert_eq!(page.record_count(), count);
    }

    #[test]
    fn test_to_page_from_page_roundtrip() {
        let mut page = VecUndoPage::new(42, 777);

        let r1 = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 100,
            vector_id: 1,
            flags: UndoFlags::Insert,
            old_data: vec![10, 20, 30],
        };
        let off1 = page.append_record(&r1).unwrap();

        let r2 = UndoRecord {
            prev_undo_ptr: off1,
            txn_id: 200,
            vector_id: 1,
            flags: UndoFlags::Update,
            old_data: vec![40, 50],
        };
        page.append_record(&r2).unwrap();

        let serialized = page.to_page();
        assert_eq!(serialized.len(), 4096);

        let deserialized = VecUndoPage::from_page(&serialized);
        assert!(deserialized.is_some());
        let deserialized = deserialized.unwrap();

        assert_eq!(deserialized.record_count(), 2);
        assert_eq!(deserialized.write_offset(), page.write_offset());
        assert_eq!(deserialized.read_record(off1).unwrap(), r1);
    }

    #[test]
    fn test_from_page_rejects_wrong_type() {
        use crate::persistence::page::{MoonPageHeader, PageType};

        let mut buf = [0u8; 4096];
        let hdr = MoonPageHeader::new(PageType::KvLeaf, 1, 1);
        hdr.write_to(&mut buf);
        MoonPageHeader::compute_checksum(&mut buf);

        assert!(VecUndoPage::from_page(&buf).is_none());
    }

    #[test]
    fn test_from_page_verifies_checksum() {
        let mut page = VecUndoPage::new(1, 1);
        let r = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 1,
            vector_id: 1,
            flags: UndoFlags::Insert,
            old_data: vec![1],
        };
        page.append_record(&r).unwrap();

        let mut serialized = page.to_page();
        // Corrupt a payload byte
        serialized[100] ^= 0xFF;

        // from_page should still parse the header, but checksum won't match.
        // Our current impl doesn't verify checksum in from_page (header-only),
        // so this test documents that behavior. Checksum verification is caller's
        // responsibility via MoonPageHeader::verify_checksum.
        // The header magic and type are still valid, so from_page succeeds.
        let result = VecUndoPage::from_page(&serialized);
        // The page deserializes but data may be corrupted -- checksum check is separate.
        assert!(result.is_some());
    }

    #[test]
    fn test_empty_old_data() {
        let mut page = VecUndoPage::new(1, 1);
        let r = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 1,
            vector_id: 1,
            flags: UndoFlags::Delete,
            old_data: vec![],
        };
        let off = page.append_record(&r).unwrap();
        let read_back = page.read_record(off).unwrap();
        assert_eq!(read_back.old_data.len(), 0);
        assert_eq!(read_back.flags, UndoFlags::Delete);
    }

    #[test]
    fn test_read_record_invalid_offset() {
        let page = VecUndoPage::new(1, 1);
        // Empty page, no records
        assert!(page.read_record(0).is_none());
        assert!(page.read_record(100).is_none());
        assert!(page.read_record(u32::MAX).is_none());
    }

    #[test]
    fn test_new_page_initial_state() {
        let page = VecUndoPage::new(5, 10);
        assert_eq!(page.record_count(), 0);
        assert_eq!(page.write_offset(), 1); // Offset 0 reserved as end-of-chain sentinel
    }

    #[test]
    fn test_serialization_preserves_page_metadata() {
        let mut page = VecUndoPage::new(99, 42);
        let r = UndoRecord {
            prev_undo_ptr: 0,
            txn_id: 1,
            vector_id: 1,
            flags: UndoFlags::Insert,
            old_data: vec![0xDE, 0xAD],
        };
        page.append_record(&r).unwrap();

        let buf = page.to_page();

        use crate::persistence::page::{MoonPageHeader, PageType};
        let hdr = MoonPageHeader::read_from(&buf).unwrap();
        assert_eq!(hdr.page_type, PageType::VecUndo);
        assert_eq!(hdr.page_id, 99);
        assert_eq!(hdr.file_id, 42);
        assert_eq!(hdr.entry_count, 1);
        assert!(MoonPageHeader::verify_checksum(&buf));
    }
}
