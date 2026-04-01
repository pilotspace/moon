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
use crate::storage::tiered::SegmentHandle;

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

/// Memory-mapped warm segment files for zero-copy access.
///
/// Each file is a sequence of MoonPage-format pages. The `SegmentHandle`
/// prevents the segment directory from being deleted while mmaps are active.
pub struct WarmSegmentFiles {
    /// Memory-mapped codes.mpf (VecCodes, 64KB pages).
    pub codes: memmap2::Mmap,
    /// Memory-mapped graph.mpf (VecGraph, 4KB pages).
    pub graph: memmap2::Mmap,
    /// Memory-mapped vectors.mpf (VecFull, 64KB pages). Optional for f16 reranking.
    pub vectors: Option<memmap2::Mmap>,
    /// Memory-mapped mvcc.mpf (VecMvcc, 4KB pages).
    pub mvcc: memmap2::Mmap,
    /// Segment handle prevents deletion while mapped.
    _handle: SegmentHandle,
}

impl WarmSegmentFiles {
    /// Open and mmap all .mpf files in a warm segment directory.
    ///
    /// Applies madvise policies:
    /// - codes.mpf: Sequential (scanned during search), optionally mlocked
    /// - graph.mpf: Random (HNSW traversal is pointer-chasing)
    /// - mvcc.mpf: Sequential, mlocked (small, always needed)
    /// - vectors.mpf: Sequential (optional)
    ///
    /// Verifies CRC32C on the first page of each file.
    pub fn open(
        segment_dir: &Path,
        handle: SegmentHandle,
        mlock_codes: bool,
    ) -> std::io::Result<Self> {
        // codes.mpf
        let codes_file = std::fs::File::open(segment_dir.join("codes.mpf"))?;
        // SAFETY: File is a sealed immutable segment. SegmentHandle refcount
        // prevents directory deletion while mapped. No concurrent writers exist.
        let codes = unsafe { memmap2::MmapOptions::new().map(&codes_file)? };
        codes.advise(memmap2::Advice::Sequential)?;
        #[cfg(unix)]
        if mlock_codes {
            codes.lock()?;
        }

        // graph.mpf
        let graph_file = std::fs::File::open(segment_dir.join("graph.mpf"))?;
        // SAFETY: Same invariants as codes -- sealed, immutable, refcount-protected.
        let graph = unsafe { memmap2::MmapOptions::new().map(&graph_file)? };
        graph.advise(memmap2::Advice::Random)?;

        // mvcc.mpf
        let mvcc_file = std::fs::File::open(segment_dir.join("mvcc.mpf"))?;
        // SAFETY: Same invariants as codes -- sealed, immutable, refcount-protected.
        let mvcc = unsafe { memmap2::MmapOptions::new().map(&mvcc_file)? };
        mvcc.advise(memmap2::Advice::Sequential)?;

        // vectors.mpf (optional)
        let vectors = match std::fs::File::open(segment_dir.join("vectors.mpf")) {
            Ok(vf) => {
                // SAFETY: Same invariants as codes -- sealed, immutable, refcount-protected.
                let v = unsafe { memmap2::MmapOptions::new().map(&vf)? };
                v.advise(memmap2::Advice::Sequential)?;
                Some(v)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };

        // Verify CRC32C on first page of each mandatory file
        if !MoonPageHeader::verify_checksum(
            &codes[..codes.len().min(PAGE_64K)],
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "codes.mpf first page CRC32C verification failed",
            ));
        }
        if !MoonPageHeader::verify_checksum(
            &graph[..graph.len().min(PAGE_4K)],
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "graph.mpf first page CRC32C verification failed",
            ));
        }
        if !MoonPageHeader::verify_checksum(
            &mvcc[..mvcc.len().min(PAGE_4K)],
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mvcc.mpf first page CRC32C verification failed",
            ));
        }

        Ok(Self {
            codes,
            graph,
            vectors,
            mvcc,
            _handle: handle,
        })
    }

    /// Return the payload bytes of a codes page (skipping the 64-byte header).
    ///
    /// # Panics
    ///
    /// Panics if `page_index` is out of range.
    pub fn codes_data(&self, page_index: usize) -> &[u8] {
        let start = page_index * PAGE_64K + MOONPAGE_HEADER_SIZE;
        let end = (page_index + 1) * PAGE_64K;
        &self.codes[start..end]
    }

    /// Return the payload bytes of a graph page (skipping the 64-byte header).
    ///
    /// # Panics
    ///
    /// Panics if `page_index` is out of range.
    pub fn graph_data(&self, page_index: usize) -> &[u8] {
        let start = page_index * PAGE_4K + MOONPAGE_HEADER_SIZE;
        let end = (page_index + 1) * PAGE_4K;
        &self.graph[start..end]
    }

    /// Number of 64KB pages in codes.mpf.
    pub fn page_count_codes(&self) -> usize {
        self.codes.len() / PAGE_64K
    }

    /// Number of 4KB pages in graph.mpf.
    pub fn page_count_graph(&self) -> usize {
        self.graph.len() / PAGE_4K
    }
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

    // --- WarmSegmentFiles tests ---

    /// Helper: write .mpf files into a segment directory for testing.
    fn write_test_segment(seg_dir: &Path, file_id: u64, codes: &[u8], graph: &[u8], mvcc: &[u8]) {
        std::fs::create_dir_all(seg_dir).unwrap();
        write_codes_mpf(&seg_dir.join("codes.mpf"), file_id, codes).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), file_id, graph).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), file_id, mvcc).unwrap();
    }

    #[test]
    fn test_warm_segment_open_and_read() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-1");

        let codes = vec![0xAAu8; 1000];
        let graph = vec![0xBBu8; 500];
        let mvcc = vec![0u8; 24 * 10]; // 10 entries
        write_test_segment(&seg_dir, 1, &codes, &graph, &mvcc);

        let handle = SegmentHandle::new(1, seg_dir.clone());
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();

        // codes_data should return payload (skip header)
        let page0_data = ws.codes_data(0);
        assert_eq!(page0_data.len(), PAGE_64K - MOONPAGE_HEADER_SIZE);
        // First 1000 bytes should be our data
        assert_eq!(&page0_data[..1000], &[0xAAu8; 1000]);

        assert_eq!(ws.page_count_codes(), 1);
    }

    #[test]
    fn test_warm_segment_crc_verification() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-2");

        let codes = vec![0x42u8; 500];
        let graph = vec![0x43u8; 200];
        let mvcc = vec![0u8; 24 * 5];
        write_test_segment(&seg_dir, 2, &codes, &graph, &mvcc);

        let handle = SegmentHandle::new(2, seg_dir.clone());
        // Should succeed -- CRC verification passes
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();
        assert_eq!(ws.page_count_graph(), 1);
    }

    #[test]
    fn test_warm_segment_crc_corruption_detected() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-3");

        let codes = vec![0x42u8; 500];
        let graph = vec![0x43u8; 200];
        let mvcc = vec![0u8; 24 * 5];
        write_test_segment(&seg_dir, 3, &codes, &graph, &mvcc);

        // Corrupt codes.mpf payload
        let codes_path = seg_dir.join("codes.mpf");
        let mut data = std::fs::read(&codes_path).unwrap();
        data[MOONPAGE_HEADER_SIZE + 10] ^= 0xFF;
        std::fs::write(&codes_path, &data).unwrap();

        let handle = SegmentHandle::new(3, seg_dir.clone());
        let result = WarmSegmentFiles::open(&seg_dir, handle, false);
        match result {
            Err(e) => {
                assert!(e.to_string().contains("codes.mpf"), "error should mention codes.mpf: {e}");
            }
            Ok(_) => panic!("expected CRC verification error, got Ok"),
        }
    }

    #[test]
    fn test_warm_segment_page_counts() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-4");

        // codes: 100KB = 2 pages (64KB each)
        let codes = vec![0u8; 100_000];
        // graph: 5000 bytes = 2 pages (4KB each)
        let graph = vec![0u8; 5000];
        let mvcc = vec![0u8; 24 * 10];
        write_test_segment(&seg_dir, 4, &codes, &graph, &mvcc);

        let handle = SegmentHandle::new(4, seg_dir.clone());
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();

        assert_eq!(ws.page_count_codes(), 2);
        assert_eq!(ws.page_count_graph(), 2);
    }

    #[test]
    fn test_warm_segment_without_vectors() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-5");

        let codes = vec![0u8; 500];
        let graph = vec![0u8; 200];
        let mvcc = vec![0u8; 24 * 5];
        write_test_segment(&seg_dir, 5, &codes, &graph, &mvcc);
        // No vectors.mpf written

        let handle = SegmentHandle::new(5, seg_dir.clone());
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();
        assert!(ws.vectors.is_none());
    }

    #[test]
    fn test_warm_segment_with_vectors() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-6");

        let codes = vec![0u8; 500];
        let graph = vec![0u8; 200];
        let mvcc = vec![0u8; 24 * 5];
        write_test_segment(&seg_dir, 6, &codes, &graph, &mvcc);
        // Also write vectors.mpf
        write_vectors_mpf(&seg_dir.join("vectors.mpf"), 6, &vec![0u8; 3000]).unwrap();

        let handle = SegmentHandle::new(6, seg_dir.clone());
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();
        assert!(ws.vectors.is_some());
    }

    #[test]
    fn test_warm_segment_data_accessors_correct_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-7");

        // Fill codes with a known pattern
        let mut codes = vec![0u8; 500];
        for (i, b) in codes.iter_mut().enumerate() {
            *b = (i & 0xFF) as u8;
        }
        let graph = vec![0xEEu8; 200];
        let mvcc = vec![0u8; 24 * 5];
        write_test_segment(&seg_dir, 7, &codes, &graph, &mvcc);

        let handle = SegmentHandle::new(7, seg_dir.clone());
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();

        // codes_data(0) should skip the 64-byte header
        let cd = ws.codes_data(0);
        for i in 0..500 {
            assert_eq!(cd[i], (i & 0xFF) as u8, "codes byte {i} mismatch");
        }

        // graph_data(0) should skip the 64-byte header
        let gd = ws.graph_data(0);
        assert_eq!(&gd[..200], &[0xEEu8; 200]);
    }
}
