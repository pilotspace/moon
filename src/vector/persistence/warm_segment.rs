//! MoonPage-format .mpf file I/O for warm vector segments.
//!
//! Warm segments store vector data in page-aligned .mpf files that can be
//! memory-mapped for zero-copy access. Each file contains a sequence of
//! pages (no file-level header) with MoonPage headers and CRC32C checksums.
//!
//! File types:
//! - `codes.mpf`   — TQ quantized codes (64KB pages, VecCodes)
//! - `graph.mpf`   — HNSW graph adjacency (4KB pages, VecGraph)
//! - `vectors.mpf` — Full-precision f32 vectors (64KB pages, VecFull)
//! - `mvcc.mpf`    — MVCC metadata entries (4KB pages, VecMvcc)

use std::io::Write;
use std::path::Path;

use crate::persistence::fsync::fsync_file;
use crate::persistence::page::{
    MoonPageHeader, PageType, MOONPAGE_HEADER_SIZE, PAGE_4K, PAGE_64K,
};

/// Generic helper to write data as a sequence of MoonPage-format pages.
///
/// Splits `data` into pages of `page_size`, each with a 64-byte MoonPage
/// header. The payload region is `page_size - 64` bytes. Pages are zero-padded.
/// CRC32C is computed over each page's payload region.
fn write_mpf_pages(
    path: &Path,
    file_id: u64,
    page_type: PageType,
    data: &[u8],
) -> std::io::Result<()> {
    let page_size = page_type.page_size();
    let payload_capacity = page_size - MOONPAGE_HEADER_SIZE;

    let page_count = if data.is_empty() {
        1 // Write at least one page even for empty data
    } else {
        (data.len() + payload_capacity - 1) / payload_capacity
    };

    let mut file = std::fs::File::create(path)?;
    let mut page_buf = vec![0u8; page_size];

    for page_idx in 0..page_count {
        // Zero the page buffer
        page_buf.fill(0);

        let data_offset = page_idx * payload_capacity;
        let data_end = data.len().min(data_offset + payload_capacity);
        let payload_len = if data_offset < data.len() {
            data_end - data_offset
        } else {
            0
        };

        // Build header
        let mut hdr = MoonPageHeader::new(page_type, page_idx as u64, file_id);
        hdr.payload_bytes = payload_len as u32;

        // For MVCC pages, compute entry count (24 bytes per entry)
        if page_type == PageType::VecMvcc {
            hdr.entry_count = (payload_len / 24) as u32;
        }

        hdr.write_to(&mut page_buf);

        // Copy payload data
        if payload_len > 0 {
            page_buf[MOONPAGE_HEADER_SIZE..MOONPAGE_HEADER_SIZE + payload_len]
                .copy_from_slice(&data[data_offset..data_end]);
        }

        // Compute CRC32C over payload region
        MoonPageHeader::compute_checksum(&mut page_buf);

        file.write_all(&page_buf)?;
    }

    file.flush()?;
    drop(file);
    fsync_file(path)?;

    Ok(())
}

/// Write TQ quantized codes to a .mpf file with 64KB VecCodes pages.
///
/// Each page holds up to 65472 bytes of payload (65536 - 64 header).
pub fn write_codes_mpf(path: &Path, file_id: u64, codes_data: &[u8]) -> std::io::Result<()> {
    write_mpf_pages(path, file_id, PageType::VecCodes, codes_data)
}

/// Write HNSW graph adjacency data to a .mpf file with 4KB VecGraph pages.
///
/// Each page holds up to 4032 bytes of payload (4096 - 64 header).
pub fn write_graph_mpf(path: &Path, file_id: u64, graph_data: &[u8]) -> std::io::Result<()> {
    write_mpf_pages(path, file_id, PageType::VecGraph, graph_data)
}

/// Write full-precision f32 vectors to a .mpf file with 64KB VecFull pages.
///
/// Each page holds up to 65472 bytes of payload (65536 - 64 header).
pub fn write_vectors_mpf(
    path: &Path,
    file_id: u64,
    vectors_data: &[u8],
) -> std::io::Result<()> {
    write_mpf_pages(path, file_id, PageType::VecFull, vectors_data)
}

/// Write MVCC metadata entries to a .mpf file with 4KB VecMvcc pages.
///
/// Each 24-byte entry: internal_id(4) + global_id(4) + insert_lsn(8) +
/// delete_lsn(4) + undo_ptr(4). Each page holds 167 entries max
/// ((4096 - 64) / 24 = 167, with 24 bytes unused for alignment).
pub fn write_mvcc_mpf(path: &Path, file_id: u64, mvcc_data: &[u8]) -> std::io::Result<()> {
    write_mpf_pages(path, file_id, PageType::VecMvcc, mvcc_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::page::MOONPAGE_MAGIC;

    #[test]
    fn test_write_codes_mpf_page_format() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("codes.mpf");

        // Write 100KB of codes -- should produce 2 pages (64KB each, 65472 payload)
        let data = vec![0xABu8; 100_000];
        write_codes_mpf(&path, 42, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        // Should be exactly 2 * 64KB = 131072 bytes
        assert_eq!(file_bytes.len(), 2 * PAGE_64K);

        // Verify page 0 header
        let hdr0 = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr0.magic, MOONPAGE_MAGIC);
        assert_eq!(hdr0.page_type, PageType::VecCodes);
        assert_eq!(hdr0.page_id, 0);
        assert_eq!(hdr0.file_id, 42);
        assert_eq!(hdr0.payload_bytes as usize, PAGE_64K - MOONPAGE_HEADER_SIZE);

        // Verify page 0 CRC32C
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_64K]));

        // Verify page 1 header
        let hdr1 = MoonPageHeader::read_from(&file_bytes[PAGE_64K..PAGE_64K + MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr1.page_type, PageType::VecCodes);
        assert_eq!(hdr1.page_id, 1);
        assert_eq!(hdr1.payload_bytes as usize, 100_000 - (PAGE_64K - MOONPAGE_HEADER_SIZE));

        // Verify page 1 CRC32C
        assert!(MoonPageHeader::verify_checksum(&file_bytes[PAGE_64K..2 * PAGE_64K]));
    }

    #[test]
    fn test_write_graph_mpf_page_format() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("graph.mpf");

        // Write 5000 bytes of graph data -- should produce 2 pages (4KB each, 4032 payload)
        let data = vec![0xCDu8; 5000];
        write_graph_mpf(&path, 7, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert_eq!(file_bytes.len(), 2 * PAGE_4K);

        // Verify page 0
        let hdr0 = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr0.page_type, PageType::VecGraph);
        assert_eq!(hdr0.page_id, 0);
        assert_eq!(hdr0.file_id, 7);
        assert_eq!(hdr0.payload_bytes as usize, PAGE_4K - MOONPAGE_HEADER_SIZE);
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_4K]));

        // Verify page 1
        let hdr1 = MoonPageHeader::read_from(&file_bytes[PAGE_4K..PAGE_4K + MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr1.page_type, PageType::VecGraph);
        assert_eq!(hdr1.page_id, 1);
        assert_eq!(hdr1.payload_bytes as usize, 5000 - (PAGE_4K - MOONPAGE_HEADER_SIZE));
        assert!(MoonPageHeader::verify_checksum(&file_bytes[PAGE_4K..2 * PAGE_4K]));
    }

    #[test]
    fn test_write_mvcc_mpf_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mvcc.mpf");

        // Write 200 entries * 24 bytes = 4800 bytes
        let entry_count = 200;
        let mut data = Vec::with_capacity(entry_count * 24);
        for i in 0..entry_count as u32 {
            data.extend_from_slice(&i.to_le_bytes());           // internal_id: 4
            data.extend_from_slice(&(i + 1000).to_le_bytes());  // global_id: 4
            data.extend_from_slice(&(i as u64 * 10).to_le_bytes()); // insert_lsn: 8
            data.extend_from_slice(&0u32.to_le_bytes());        // delete_lsn: 4
            data.extend_from_slice(&0u32.to_le_bytes());        // undo_ptr: 4
        }
        assert_eq!(data.len(), 4800);

        write_mvcc_mpf(&path, 100, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        // 4800 bytes / 4032 payload per page = 2 pages
        assert_eq!(file_bytes.len(), 2 * PAGE_4K);

        // Page 0: 4032 bytes = 168 entries (168 * 24 = 4032)
        let hdr0 = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr0.page_type, PageType::VecMvcc);
        assert_eq!(hdr0.entry_count, 168); // 4032 / 24 = 168
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_4K]));

        // Page 1: remaining 768 bytes = 32 entries
        let hdr1 = MoonPageHeader::read_from(&file_bytes[PAGE_4K..PAGE_4K + MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr1.page_type, PageType::VecMvcc);
        assert_eq!(hdr1.entry_count, 32); // 768 / 24 = 32
        assert!(MoonPageHeader::verify_checksum(&file_bytes[PAGE_4K..2 * PAGE_4K]));
    }

    #[test]
    fn test_mpf_no_file_header() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("codes.mpf");

        let data = vec![0u8; 1000];
        write_codes_mpf(&path, 1, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();

        // First 4 bytes should be MOONPAGE_MAGIC (no file-level header)
        let magic = u32::from_le_bytes([
            file_bytes[0], file_bytes[1], file_bytes[2], file_bytes[3],
        ]);
        assert_eq!(magic, MOONPAGE_MAGIC, "first bytes must be MoonPage magic, not a file header");
    }

    #[test]
    fn test_write_vectors_mpf_page_format() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("vectors.mpf");

        let data = vec![0x42u8; 2000];
        write_vectors_mpf(&path, 5, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert_eq!(file_bytes.len(), PAGE_64K); // fits in one page

        let hdr = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr.page_type, PageType::VecFull);
        assert_eq!(hdr.payload_bytes, 2000);
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_64K]));
    }

    #[test]
    fn test_write_codes_mpf_small_data() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("codes.mpf");

        // Small data that fits in a single page
        let data = vec![0xFFu8; 100];
        write_codes_mpf(&path, 3, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert_eq!(file_bytes.len(), PAGE_64K);

        let hdr = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr.payload_bytes, 100);
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_64K]));

        // Verify payload content
        assert_eq!(&file_bytes[MOONPAGE_HEADER_SIZE..MOONPAGE_HEADER_SIZE + 100], &[0xFFu8; 100]);
    }
}
