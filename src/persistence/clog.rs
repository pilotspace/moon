//! CLOG — Persistent 2-bit-per-transaction commit log.
//!
//! Each ClogPage stores status for 16,128 transactions in 4,032 data bytes
//! (4KB page minus 64-byte MoonPageHeader). Status is packed 4 transactions
//! per byte using 2-bit encoding:
//!   - 0b00: InProgress
//!   - 0b01: Committed
//!   - 0b10: Aborted
//!   - 0b11: SubCommitted

use crate::persistence::page::{MOONPAGE_HEADER_SIZE, MoonPageHeader, PageType};

/// Transaction status: 2 bits per transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TxnStatus {
    InProgress = 0b00,
    Committed = 0b01,
    Aborted = 0b10,
    SubCommitted = 0b11,
}

impl TxnStatus {
    /// Decode a 2-bit value into a `TxnStatus`.
    #[inline]
    pub fn from_bits(bits: u8) -> Self {
        match bits & 0b11 {
            0b00 => Self::InProgress,
            0b01 => Self::Committed,
            0b10 => Self::Aborted,
            _ => Self::SubCommitted,
        }
    }
}

/// Data region size in a 4KB ClogPage (4096 - 64 header = 4032 bytes).
const CLOG_DATA_SIZE: usize = 4096 - MOONPAGE_HEADER_SIZE;

/// Transactions per ClogPage: 4032 bytes * 4 txns/byte = 16,128.
pub const TXNS_PER_PAGE: u64 = (CLOG_DATA_SIZE * 4) as u64;

/// Persistent 2-bit-per-transaction commit log page.
///
/// Packs transaction status at 4 transactions per byte. A fresh page
/// is all zeros, meaning every transaction defaults to `InProgress`.
pub struct ClogPage {
    page_index: u64,
    data: [u8; CLOG_DATA_SIZE],
}

impl ClogPage {
    /// Create a new empty ClogPage (all transactions InProgress).
    pub fn new(page_index: u64) -> Self {
        Self {
            page_index,
            data: [0u8; CLOG_DATA_SIZE],
        }
    }

    /// Which ClogPage index holds a given transaction ID.
    #[inline]
    pub fn page_for_txn(txn_id: u64) -> u64 {
        txn_id / TXNS_PER_PAGE
    }

    /// Offset within a page for a given transaction ID.
    #[inline]
    fn local_offset(txn_id: u64) -> usize {
        (txn_id % TXNS_PER_PAGE) as usize
    }

    /// Get the status of a transaction within this page.
    #[inline]
    pub fn get_status(&self, txn_id: u64) -> TxnStatus {
        let local = Self::local_offset(txn_id);
        let byte_idx = local / 4;
        let shift = (local % 4) * 2;
        TxnStatus::from_bits((self.data[byte_idx] >> shift) & 0b11)
    }

    /// Set the status of a transaction within this page.
    #[inline]
    pub fn set_status(&mut self, txn_id: u64, status: TxnStatus) {
        let local = Self::local_offset(txn_id);
        let byte_idx = local / 4;
        let shift = (local % 4) * 2;
        self.data[byte_idx] &= !(0b11 << shift);
        self.data[byte_idx] |= (status as u8) << shift;
    }

    /// Serialize to a 4KB buffer with MoonPage header and CRC32C checksum.
    pub fn to_page(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        let mut hdr = MoonPageHeader::new(PageType::ClogPage, self.page_index, 0);
        hdr.payload_bytes = CLOG_DATA_SIZE as u32;
        hdr.write_to(&mut buf);
        buf[MOONPAGE_HEADER_SIZE..MOONPAGE_HEADER_SIZE + CLOG_DATA_SIZE]
            .copy_from_slice(&self.data);
        MoonPageHeader::compute_checksum(&mut buf);
        buf
    }

    /// Deserialize from a 4KB buffer, verifying magic, page type, and CRC32C.
    pub fn from_page(buf: &[u8; 4096]) -> Option<Self> {
        if !MoonPageHeader::verify_checksum(buf) {
            return None;
        }
        let hdr = MoonPageHeader::read_from(buf)?;
        if hdr.page_type != PageType::ClogPage {
            return None;
        }
        let mut data = [0u8; CLOG_DATA_SIZE];
        data.copy_from_slice(&buf[MOONPAGE_HEADER_SIZE..MOONPAGE_HEADER_SIZE + CLOG_DATA_SIZE]);
        Some(Self {
            page_index: hdr.page_id,
            data,
        })
    }

    /// Returns the page index this ClogPage represents.
    #[inline]
    pub fn page_index(&self) -> u64 {
        self.page_index
    }
}

/// Scan a directory for CLOG page files (`clog-NNNNNN.page` format),
/// load each, and return a `Vec<ClogPage>` sorted by page_index.
pub fn scan_clog_dir(clog_dir: &std::path::Path) -> std::io::Result<Vec<ClogPage>> {
    let mut pages = Vec::new();
    if !clog_dir.exists() {
        return Ok(pages);
    }
    for entry in std::fs::read_dir(clog_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".page") {
            continue;
        }
        let data = std::fs::read(entry.path())?;
        if data.len() < 4096 {
            continue;
        }
        let buf: [u8; 4096] = match data[..4096].try_into() {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Some(page) = ClogPage::from_page(&buf) {
            pages.push(page);
        }
    }
    pages.sort_by_key(|p| p.page_index());
    Ok(pages)
}

/// Write a ClogPage to `{clog_dir}/clog-{page_index:06}.page`.
pub fn write_clog_page(clog_dir: &std::path::Path, page: &ClogPage) -> std::io::Result<()> {
    std::fs::create_dir_all(clog_dir)?;
    let path = clog_dir.join(format!("clog-{:06}.page", page.page_index()));
    std::fs::write(&path, page.to_page())?;
    crate::persistence::fsync::fsync_file(&path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txns_per_page_is_16128() {
        assert_eq!(TXNS_PER_PAGE, 16128);
    }

    #[test]
    fn new_page_all_in_progress() {
        let page = ClogPage::new(0);
        for txn_id in [0u64, 1, 100, 8000, 16127] {
            assert_eq!(page.get_status(txn_id), TxnStatus::InProgress);
        }
    }

    #[test]
    fn set_get_committed() {
        let mut page = ClogPage::new(0);
        page.set_status(0, TxnStatus::Committed);
        assert_eq!(page.get_status(0), TxnStatus::Committed);
    }

    #[test]
    fn set_get_aborted() {
        let mut page = ClogPage::new(0);
        page.set_status(42, TxnStatus::Aborted);
        assert_eq!(page.get_status(42), TxnStatus::Aborted);
    }

    #[test]
    fn set_get_sub_committed() {
        let mut page = ClogPage::new(0);
        page.set_status(999, TxnStatus::SubCommitted);
        assert_eq!(page.get_status(999), TxnStatus::SubCommitted);
    }

    #[test]
    fn boundary_last_txn_in_page() {
        let mut page = ClogPage::new(0);
        page.set_status(16127, TxnStatus::Aborted);
        assert_eq!(page.get_status(16127), TxnStatus::Aborted);
        // Verify adjacent txn unaffected
        assert_eq!(page.get_status(16126), TxnStatus::InProgress);
    }

    #[test]
    fn overwrite_status() {
        let mut page = ClogPage::new(0);
        page.set_status(5, TxnStatus::Committed);
        assert_eq!(page.get_status(5), TxnStatus::Committed);
        page.set_status(5, TxnStatus::Aborted);
        assert_eq!(page.get_status(5), TxnStatus::Aborted);
    }

    #[test]
    fn adjacent_txns_independent() {
        let mut page = ClogPage::new(0);
        // Set all 4 statuses in adjacent positions within one byte
        page.set_status(0, TxnStatus::InProgress);
        page.set_status(1, TxnStatus::Committed);
        page.set_status(2, TxnStatus::Aborted);
        page.set_status(3, TxnStatus::SubCommitted);

        assert_eq!(page.get_status(0), TxnStatus::InProgress);
        assert_eq!(page.get_status(1), TxnStatus::Committed);
        assert_eq!(page.get_status(2), TxnStatus::Aborted);
        assert_eq!(page.get_status(3), TxnStatus::SubCommitted);
    }

    #[test]
    fn page_for_txn_arithmetic() {
        assert_eq!(ClogPage::page_for_txn(0), 0);
        assert_eq!(ClogPage::page_for_txn(16127), 0);
        assert_eq!(ClogPage::page_for_txn(16128), 1);
        assert_eq!(ClogPage::page_for_txn(32255), 1);
        assert_eq!(ClogPage::page_for_txn(32256), 2);
    }

    #[test]
    fn txn_status_from_bits_all_values() {
        assert_eq!(TxnStatus::from_bits(0b00), TxnStatus::InProgress);
        assert_eq!(TxnStatus::from_bits(0b01), TxnStatus::Committed);
        assert_eq!(TxnStatus::from_bits(0b10), TxnStatus::Aborted);
        assert_eq!(TxnStatus::from_bits(0b11), TxnStatus::SubCommitted);
        // High bits masked off
        assert_eq!(TxnStatus::from_bits(0b1100), TxnStatus::InProgress);
        assert_eq!(TxnStatus::from_bits(0xFF), TxnStatus::SubCommitted);
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let mut page = ClogPage::new(7);
        page.set_status(0, TxnStatus::Committed);
        page.set_status(100, TxnStatus::Aborted);
        page.set_status(16127, TxnStatus::SubCommitted);

        let buf = page.to_page();
        assert_eq!(buf.len(), 4096);

        let restored = ClogPage::from_page(&buf).expect("deserialization should succeed");
        assert_eq!(restored.page_index(), 7);
        assert_eq!(restored.get_status(0), TxnStatus::Committed);
        assert_eq!(restored.get_status(100), TxnStatus::Aborted);
        assert_eq!(restored.get_status(16127), TxnStatus::SubCommitted);
        assert_eq!(restored.get_status(1), TxnStatus::InProgress);
    }

    #[test]
    fn from_page_rejects_wrong_page_type() {
        let page = ClogPage::new(0);
        let mut buf = page.to_page();
        // Corrupt the page type byte (offset 5)
        buf[5] = PageType::KvLeaf as u8;
        // Recompute checksum so it passes CRC check
        MoonPageHeader::compute_checksum(&mut buf);
        assert!(ClogPage::from_page(&buf).is_none());
    }

    #[test]
    fn from_page_rejects_corrupt_checksum() {
        let page = ClogPage::new(0);
        let mut buf = page.to_page();
        // Corrupt a data byte
        buf[100] ^= 0xFF;
        assert!(ClogPage::from_page(&buf).is_none());
    }

    #[test]
    fn stress_all_positions() {
        let mut page = ClogPage::new(0);
        // Set every position to Committed
        for i in 0..TXNS_PER_PAGE {
            page.set_status(i, TxnStatus::Committed);
        }
        // Verify all
        for i in 0..TXNS_PER_PAGE {
            assert_eq!(page.get_status(i), TxnStatus::Committed, "txn {i}");
        }
        // Overwrite every other to Aborted
        for i in (0..TXNS_PER_PAGE).step_by(2) {
            page.set_status(i, TxnStatus::Aborted);
        }
        for i in 0..TXNS_PER_PAGE {
            let expected = if i % 2 == 0 {
                TxnStatus::Aborted
            } else {
                TxnStatus::Committed
            };
            assert_eq!(page.get_status(i), expected, "txn {i}");
        }
    }

    #[test]
    fn test_scan_clog_dir_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let clog_dir = tmp.path().join("clog");

        // Write 2 ClogPages to disk
        let mut page0 = ClogPage::new(0);
        page0.set_status(5, TxnStatus::Committed);
        page0.set_status(10, TxnStatus::Aborted);
        write_clog_page(&clog_dir, &page0).unwrap();

        let mut page1 = ClogPage::new(1);
        page1.set_status(TXNS_PER_PAGE + 3, TxnStatus::SubCommitted);
        write_clog_page(&clog_dir, &page1).unwrap();

        // Scan and verify
        let pages = scan_clog_dir(&clog_dir).unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].page_index(), 0);
        assert_eq!(pages[1].page_index(), 1);
        assert_eq!(pages[0].get_status(5), TxnStatus::Committed);
        assert_eq!(pages[0].get_status(10), TxnStatus::Aborted);
        assert_eq!(
            pages[1].get_status(TXNS_PER_PAGE + 3),
            TxnStatus::SubCommitted
        );
    }

    #[test]
    fn test_scan_clog_dir_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let clog_dir = tmp.path().join("nonexistent");
        let pages = scan_clog_dir(&clog_dir).unwrap();
        assert!(pages.is_empty());
    }
}
