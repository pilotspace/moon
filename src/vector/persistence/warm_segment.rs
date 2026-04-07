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
    MOONPAGE_HEADER_SIZE, MoonPageHeader, PAGE_4K, PAGE_64K, PageType, page_flags,
};
use crate::storage::tiered::SegmentHandle;

// ── Per-page-type sub-header sizes (design §7.2-7.5) ──────────────────

/// VecCodes sub-header size in bytes (design Section 7.2). Follows MoonPageHeader.
pub const VEC_CODES_SUB_HEADER_SIZE: usize = 32;

/// VecFull sub-header size in bytes (design Section 7.3). Follows MoonPageHeader.
pub const VEC_FULL_SUB_HEADER_SIZE: usize = 24;

/// VecGraph sub-header size in bytes (design Section 7.4). Follows MoonPageHeader.
pub const VEC_GRAPH_SUB_HEADER_SIZE: usize = 16;

/// VecMvcc sub-header size in bytes (design Section 7.5). Follows MoonPageHeader.
pub const VEC_MVCC_SUB_HEADER_SIZE: usize = 8;

/// Return the sub-header size for a given page type.
/// Returns 0 for page types without sub-headers.
#[inline]
pub fn sub_header_size(page_type: PageType) -> usize {
    match page_type {
        PageType::VecCodes => VEC_CODES_SUB_HEADER_SIZE,
        PageType::VecFull => VEC_FULL_SUB_HEADER_SIZE,
        PageType::VecGraph => VEC_GRAPH_SUB_HEADER_SIZE,
        PageType::VecMvcc => VEC_MVCC_SUB_HEADER_SIZE,
        _ => 0,
    }
}

/// Write VecCodes sub-header (32 bytes) into `buf` starting at offset 0.
///
/// Layout (design Section 7.2):
/// ```text
/// 0..8    collection_id  (u64 LE)
/// 8..16   base_vector_id (u64 LE)
/// 16..18  dimension      (u16 LE)
/// 18..20  padded_dimension (u16 LE)
/// 20      quantization   (u8)
/// 21..23  bytes_per_code (u16 LE)
/// 23..25  vector_count   (u16 LE)
/// 25      has_sub_signs  (u8, 0 or 1)
/// 26..32  reserved       (zeroed)
/// ```
pub fn write_vec_codes_sub_header(
    buf: &mut [u8],
    collection_id: u64,
    base_vector_id: u64,
    dimension: u16,
    padded_dimension: u16,
    quantization: u8,
    bytes_per_code: u16,
    vector_count: u16,
    has_sub_signs: bool,
) {
    buf[0..8].copy_from_slice(&collection_id.to_le_bytes());
    buf[8..16].copy_from_slice(&base_vector_id.to_le_bytes());
    buf[16..18].copy_from_slice(&dimension.to_le_bytes());
    buf[18..20].copy_from_slice(&padded_dimension.to_le_bytes());
    buf[20] = quantization;
    buf[21..23].copy_from_slice(&bytes_per_code.to_le_bytes());
    buf[23..25].copy_from_slice(&vector_count.to_le_bytes());
    buf[25] = if has_sub_signs { 1 } else { 0 };
    // buf[26..32] reserved, already zeroed
}

/// Write VecFull sub-header (24 bytes) into `buf`.
///
/// Layout (design Section 7.3):
/// ```text
/// 0..8    collection_id    (u64 LE)
/// 8..16   base_vector_id   (u64 LE)
/// 16..18  dimension        (u16 LE)
/// 18      element_type     (u8: F32=1 F16=2 BF16=3)
/// 19      element_size     (u8)
/// 20..22  vectors_per_page (u16 LE)
/// 22..24  reserved         (zeroed)
/// ```
pub fn write_vec_full_sub_header(
    buf: &mut [u8],
    collection_id: u64,
    base_vector_id: u64,
    dimension: u16,
    element_type: u8,
    element_size: u8,
    vectors_per_page: u16,
) {
    buf[0..8].copy_from_slice(&collection_id.to_le_bytes());
    buf[8..16].copy_from_slice(&base_vector_id.to_le_bytes());
    buf[16..18].copy_from_slice(&dimension.to_le_bytes());
    buf[18] = element_type;
    buf[19] = element_size;
    buf[20..22].copy_from_slice(&vectors_per_page.to_le_bytes());
    // buf[22..24] reserved, already zeroed
}

/// Write VecGraph sub-header (16 bytes) into `buf`.
///
/// Layout (design Section 7.4):
/// ```text
/// 0..4   base_node_id    (u32 LE)
/// 4..6   nodes_per_page  (u16 LE)
/// 6..8   max_degree      (u16 LE)
/// 8      graph_type      (u8: HNSW=1 Vamana=2)
/// 9      layer           (u8)
/// 10..16 reserved        (zeroed)
/// ```
pub fn write_vec_graph_sub_header(
    buf: &mut [u8],
    base_node_id: u32,
    nodes_per_page: u16,
    max_degree: u16,
    graph_type: u8,
    layer: u8,
) {
    buf[0..4].copy_from_slice(&base_node_id.to_le_bytes());
    buf[4..6].copy_from_slice(&nodes_per_page.to_le_bytes());
    buf[6..8].copy_from_slice(&max_degree.to_le_bytes());
    buf[8] = graph_type;
    buf[9] = layer;
    // buf[10..16] reserved, already zeroed
}

/// Write VecMvcc sub-header (8 bytes) into `buf`.
///
/// Layout (design Section 7.5):
/// ```text
/// 0..4  base_vector_id (u32 LE)
/// 4..8  mvcc_count     (u32 LE)
/// ```
pub fn write_vec_mvcc_sub_header(buf: &mut [u8], base_vector_id: u32, mvcc_count: u32) {
    buf[0..4].copy_from_slice(&base_vector_id.to_le_bytes());
    buf[4..8].copy_from_slice(&mvcc_count.to_le_bytes());
}

/// Generic helper to write data as a sequence of MoonPage-format pages.
///
/// Splits `data` into pages of `page_size`, each with a 64-byte MoonPage
/// header followed by a type-specific sub-header (size determined by
/// `sub_header_size(page_type)`). The data payload follows the sub-header.
/// Effective data capacity per page is `page_size - 64 - sub_hdr_size`.
///
/// The `payload_bytes` field in MoonPageHeader includes both the sub-header
/// and the data bytes so that CRC32C covers the entire region after the
/// 64-byte header (sub-header + data).
///
/// `sub_header_fn` is called for each page to populate the sub-header region.
/// It receives `(sub_header_slice, page_index, data_bytes_in_page)`.
fn write_mpf_pages(
    path: &Path,
    file_id: u64,
    page_type: PageType,
    data: &[u8],
    sub_header_fn: Option<&dyn Fn(&mut [u8], usize, usize)>,
) -> std::io::Result<()> {
    let page_size = page_type.page_size();
    let sub_hdr_size = sub_header_size(page_type);
    let data_capacity = page_size - MOONPAGE_HEADER_SIZE - sub_hdr_size;

    let page_count = if data.is_empty() {
        1 // Write at least one page even for empty data
    } else {
        (data.len() + data_capacity - 1) / data_capacity
    };

    let mut file = std::fs::File::create(path)?;
    let mut page_buf = vec![0u8; page_size];

    for page_idx in 0..page_count {
        // Zero the page buffer
        page_buf.fill(0);

        let data_offset = page_idx * data_capacity;
        let data_end = data.len().min(data_offset + data_capacity);
        let data_len = if data_offset < data.len() {
            data_end - data_offset
        } else {
            0
        };

        // Build header -- payload_bytes covers sub-header + data
        let mut hdr = MoonPageHeader::new(page_type, page_idx as u64, file_id);
        hdr.payload_bytes = (sub_hdr_size + data_len) as u32;

        // For MVCC pages, compute entry count (24 bytes per entry)
        if page_type == PageType::VecMvcc {
            hdr.entry_count = (data_len / 24) as u32;
        }

        hdr.write_to(&mut page_buf);

        // Write sub-header (region is already zeroed)
        if sub_hdr_size > 0 {
            if let Some(f) = sub_header_fn {
                let sub_start = MOONPAGE_HEADER_SIZE;
                let sub_end = sub_start + sub_hdr_size;
                f(&mut page_buf[sub_start..sub_end], page_idx, data_len);
            }
        }

        // Copy data after sub-header, optionally LZ4-compressing large payloads.
        // The sub-header is NEVER compressed -- only the data region after it.
        if data_len > 256 {
            let compressed = lz4_flex::compress_prepend_size(&data[data_offset..data_end]);
            if compressed.len() < data_len {
                // Compression helped -- write compressed data and set flag
                let payload_start = MOONPAGE_HEADER_SIZE + sub_hdr_size;
                page_buf[payload_start..payload_start + compressed.len()]
                    .copy_from_slice(&compressed);
                // Update header: set COMPRESSED flag and adjust payload_bytes
                let new_payload = (sub_hdr_size + compressed.len()) as u32;
                // Re-write flags with COMPRESSED bit
                let flags = page_flags::COMPRESSED;
                page_buf[6..8].copy_from_slice(&flags.to_le_bytes());
                // Re-write payload_bytes
                page_buf[20..24].copy_from_slice(&new_payload.to_le_bytes());
            } else {
                // Compression didn't help -- write raw data
                let payload_start = MOONPAGE_HEADER_SIZE + sub_hdr_size;
                page_buf[payload_start..payload_start + data_len]
                    .copy_from_slice(&data[data_offset..data_end]);
            }
        } else if data_len > 0 {
            let payload_start = MOONPAGE_HEADER_SIZE + sub_hdr_size;
            page_buf[payload_start..payload_start + data_len]
                .copy_from_slice(&data[data_offset..data_end]);
        }

        // Compute CRC32C over payload region (sub-header + data)
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
/// Each page holds up to 65440 bytes of data (65536 - 64 header - 32 sub-header).
/// The 32-byte VecCodes sub-header is written with default values (zeroed
/// collection/dimension fields). Callers can use `write_codes_mpf_with_meta`
/// for populated sub-headers once collection metadata is available at write time.
pub fn write_codes_mpf(path: &Path, file_id: u64, codes_data: &[u8]) -> std::io::Result<()> {
    let sub_fn = |buf: &mut [u8], _page_idx: usize, data_len: usize| {
        // Default sub-header: vector_count derived from data, rest zeroed
        // quantization=4 (TQ4 default), bytes_per_code=0
        write_vec_codes_sub_header(buf, 0, 0, 0, 0, 4, 0, data_len as u16, false);
    };
    write_mpf_pages(path, file_id, PageType::VecCodes, codes_data, Some(&sub_fn))
}

/// Write HNSW graph adjacency data to a .mpf file with 4KB VecGraph pages.
///
/// Each page holds up to 4016 bytes of data (4096 - 64 header - 16 sub-header).
/// The 16-byte VecGraph sub-header is written with graph_type=1 (HNSW), layer=0.
pub fn write_graph_mpf(path: &Path, file_id: u64, graph_data: &[u8]) -> std::io::Result<()> {
    let sub_fn = |buf: &mut [u8], _page_idx: usize, _data_len: usize| {
        write_vec_graph_sub_header(buf, 0, 0, 0, 1, 0); // HNSW=1, layer=0
    };
    write_mpf_pages(path, file_id, PageType::VecGraph, graph_data, Some(&sub_fn))
}

/// Write full-precision vectors to a .mpf file with 64KB VecFull pages.
///
/// Each page holds up to 65448 bytes of data (65536 - 64 header - 24 sub-header).
/// The 24-byte VecFull sub-header is written with element_type=2 (F16),
/// element_size=2.
pub fn write_vectors_mpf(path: &Path, file_id: u64, vectors_data: &[u8]) -> std::io::Result<()> {
    let sub_fn = |buf: &mut [u8], _page_idx: usize, _data_len: usize| {
        write_vec_full_sub_header(buf, 0, 0, 0, 2, 2, 0); // F16=2, elem_size=2
    };
    write_mpf_pages(
        path,
        file_id,
        PageType::VecFull,
        vectors_data,
        Some(&sub_fn),
    )
}

/// Write MVCC metadata entries to a .mpf file with 4KB VecMvcc pages.
///
/// Each 24-byte entry: internal_id(4) + global_id(4) + insert_lsn(8) +
/// delete_lsn(4) + undo_ptr(4). Each page holds 167 entries max
/// ((4096 - 64 - 8) / 24 = 167, with 16 bytes unused for alignment).
pub fn write_mvcc_mpf(path: &Path, file_id: u64, mvcc_data: &[u8]) -> std::io::Result<()> {
    let sub_fn = |buf: &mut [u8], _page_idx: usize, data_len: usize| {
        let entry_count = (data_len / 24) as u32;
        write_vec_mvcc_sub_header(buf, 0, entry_count);
    };
    write_mpf_pages(path, file_id, PageType::VecMvcc, mvcc_data, Some(&sub_fn))
}

/// Write collection metadata to a .mpf file with 4KB VecMeta pages.
pub fn write_meta_mpf(path: &Path, file_id: u64, meta_data: &[u8]) -> std::io::Result<()> {
    write_mpf_pages(path, file_id, PageType::VecMeta, meta_data, None)
}

/// Write an empty undo.mpf file as a VecUndo placeholder.
///
/// The undo log starts empty for new warm segments — populated when
/// metadata updates occur (future).
pub fn write_undo_mpf(path: &Path, file_id: u64) -> std::io::Result<()> {
    // Write a single page with just the header (no undo records yet)
    write_mpf_pages(path, file_id, PageType::VecUndo, &[], None)
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
        if !MoonPageHeader::verify_checksum(&codes[..codes.len().min(PAGE_64K)]) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "codes.mpf first page CRC32C verification failed",
            ));
        }
        if !MoonPageHeader::verify_checksum(&graph[..graph.len().min(PAGE_4K)]) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "graph.mpf first page CRC32C verification failed",
            ));
        }
        if !MoonPageHeader::verify_checksum(&mvcc[..mvcc.len().min(PAGE_4K)]) {
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

    /// Return the data bytes of a codes page (skipping header + sub-header).
    ///
    /// # Panics
    ///
    /// Panics if `page_index` is out of range.
    pub fn codes_data(&self, page_index: usize) -> &[u8] {
        let start = page_index * PAGE_64K + MOONPAGE_HEADER_SIZE + VEC_CODES_SUB_HEADER_SIZE;
        let end = (page_index + 1) * PAGE_64K;
        &self.codes[start..end]
    }

    /// Return the data bytes of a graph page (skipping header + sub-header).
    ///
    /// # Panics
    ///
    /// Panics if `page_index` is out of range.
    pub fn graph_data(&self, page_index: usize) -> &[u8] {
        let start = page_index * PAGE_4K + MOONPAGE_HEADER_SIZE + VEC_GRAPH_SUB_HEADER_SIZE;
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

    /// Generate pseudo-random incompressible data using a simple LCG.
    /// This ensures LZ4 compression does NOT reduce size, so tests that
    /// verify exact payload_bytes values exercise the uncompressed path.
    fn incompressible_data(len: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(len);
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for _ in 0..len {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            data.push((state >> 33) as u8);
        }
        data
    }

    #[test]
    fn test_write_codes_mpf_page_format() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("codes.mpf");

        // Data capacity per page = 65536 - 64 - 32 = 65440
        let data_cap = PAGE_64K - MOONPAGE_HEADER_SIZE - VEC_CODES_SUB_HEADER_SIZE;
        assert_eq!(data_cap, 65440);

        // Write 100KB of incompressible codes -- should produce 2 pages
        let data = incompressible_data(100_000);
        write_codes_mpf(&path, 42, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert_eq!(file_bytes.len(), 2 * PAGE_64K);

        // Verify page 0 header (payload_bytes = sub_hdr + data = 32 + 65440 = 65472)
        let hdr0 = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr0.magic, MOONPAGE_MAGIC);
        assert_eq!(hdr0.page_type, PageType::VecCodes);
        assert_eq!(hdr0.page_id, 0);
        assert_eq!(hdr0.file_id, 42);
        assert_eq!(
            hdr0.payload_bytes as usize,
            VEC_CODES_SUB_HEADER_SIZE + data_cap,
        );

        // Verify page 0 CRC32C
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_64K]));

        // Verify page 1 header (remaining data = 100000 - 65440 = 34560)
        let hdr1 =
            MoonPageHeader::read_from(&file_bytes[PAGE_64K..PAGE_64K + MOONPAGE_HEADER_SIZE])
                .unwrap();
        assert_eq!(hdr1.page_type, PageType::VecCodes);
        assert_eq!(hdr1.page_id, 1);
        assert_eq!(
            hdr1.payload_bytes as usize,
            VEC_CODES_SUB_HEADER_SIZE + (100_000 - data_cap),
        );

        // Verify page 1 CRC32C
        assert!(MoonPageHeader::verify_checksum(
            &file_bytes[PAGE_64K..2 * PAGE_64K]
        ));
    }

    #[test]
    fn test_write_graph_mpf_page_format() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("graph.mpf");

        // Data capacity per page = 4096 - 64 - 16 = 4016
        let data_cap = PAGE_4K - MOONPAGE_HEADER_SIZE - VEC_GRAPH_SUB_HEADER_SIZE;
        assert_eq!(data_cap, 4016);

        // Write 5000 bytes of incompressible graph data -- should produce 2 pages
        let data = incompressible_data(5000);
        write_graph_mpf(&path, 7, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert_eq!(file_bytes.len(), 2 * PAGE_4K);

        // Verify page 0 (payload_bytes = sub_hdr + data = 16 + 4016 = 4032)
        let hdr0 = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr0.page_type, PageType::VecGraph);
        assert_eq!(hdr0.page_id, 0);
        assert_eq!(hdr0.file_id, 7);
        assert_eq!(
            hdr0.payload_bytes as usize,
            VEC_GRAPH_SUB_HEADER_SIZE + data_cap,
        );
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_4K]));

        // Verify page 1 (remaining data = 5000 - 4016 = 984)
        let hdr1 = MoonPageHeader::read_from(&file_bytes[PAGE_4K..PAGE_4K + MOONPAGE_HEADER_SIZE])
            .unwrap();
        assert_eq!(hdr1.page_type, PageType::VecGraph);
        assert_eq!(hdr1.page_id, 1);
        assert_eq!(
            hdr1.payload_bytes as usize,
            VEC_GRAPH_SUB_HEADER_SIZE + (5000 - data_cap),
        );
        assert!(MoonPageHeader::verify_checksum(
            &file_bytes[PAGE_4K..2 * PAGE_4K]
        ));
    }

    #[test]
    fn test_write_mvcc_mpf_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mvcc.mpf");

        // Data capacity per page = 4096 - 64 - 8 = 4024
        let data_cap = PAGE_4K - MOONPAGE_HEADER_SIZE - VEC_MVCC_SUB_HEADER_SIZE;
        assert_eq!(data_cap, 4024);

        // Write 200 entries * 24 bytes = 4800 bytes
        let entry_count = 200;
        let mut data = Vec::with_capacity(entry_count * 24);
        for i in 0..entry_count as u32 {
            data.extend_from_slice(&i.to_le_bytes()); // internal_id: 4
            data.extend_from_slice(&(i + 1000).to_le_bytes()); // global_id: 4
            data.extend_from_slice(&(i as u64 * 10).to_le_bytes()); // insert_lsn: 8
            data.extend_from_slice(&0u32.to_le_bytes()); // delete_lsn: 4
            data.extend_from_slice(&0u32.to_le_bytes()); // undo_ptr: 4
        }
        assert_eq!(data.len(), 4800);

        write_mvcc_mpf(&path, 100, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        // 4800 bytes / 4024 data-cap per page = 2 pages
        assert_eq!(file_bytes.len(), 2 * PAGE_4K);

        // Page 0: data = 4024, entries = 4024/24 = 167
        let hdr0 = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr0.page_type, PageType::VecMvcc);
        assert_eq!(hdr0.entry_count, 167); // 4024 / 24 = 167
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_4K]));

        // Page 1: remaining 776 bytes = 32 entries (776 / 24 = 32)
        let hdr1 = MoonPageHeader::read_from(&file_bytes[PAGE_4K..PAGE_4K + MOONPAGE_HEADER_SIZE])
            .unwrap();
        assert_eq!(hdr1.page_type, PageType::VecMvcc);
        assert_eq!(hdr1.entry_count, 32); // 776 / 24 = 32
        assert!(MoonPageHeader::verify_checksum(
            &file_bytes[PAGE_4K..2 * PAGE_4K]
        ));
    }

    #[test]
    fn test_mpf_no_file_header() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("codes.mpf");

        let data = vec![0u8; 1000];
        write_codes_mpf(&path, 1, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();

        // First 4 bytes should be MOONPAGE_MAGIC (no file-level header)
        let magic =
            u32::from_le_bytes([file_bytes[0], file_bytes[1], file_bytes[2], file_bytes[3]]);
        assert_eq!(
            magic, MOONPAGE_MAGIC,
            "first bytes must be MoonPage magic, not a file header"
        );
    }

    #[test]
    fn test_write_vectors_mpf_page_format() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("vectors.mpf");

        let data = incompressible_data(2000);
        write_vectors_mpf(&path, 5, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert_eq!(file_bytes.len(), PAGE_64K); // fits in one page

        let hdr = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(hdr.page_type, PageType::VecFull);
        // payload_bytes = sub_hdr(24) + data(2000) = 2024
        assert_eq!(hdr.payload_bytes as usize, VEC_FULL_SUB_HEADER_SIZE + 2000);
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
        // payload_bytes = sub_hdr(32) + data(100) = 132
        assert_eq!(hdr.payload_bytes as usize, VEC_CODES_SUB_HEADER_SIZE + 100);
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_64K]));

        // Verify data content (after header + sub-header)
        let data_start = MOONPAGE_HEADER_SIZE + VEC_CODES_SUB_HEADER_SIZE;
        assert_eq!(&file_bytes[data_start..data_start + 100], &[0xFFu8; 100]);
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

        let codes = incompressible_data(1000);
        let graph = incompressible_data(500);
        let mvcc = vec![0u8; 24 * 10]; // 10 entries
        write_test_segment(&seg_dir, 1, &codes, &graph, &mvcc);

        let handle = SegmentHandle::new(1, seg_dir.clone());
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();

        // codes_data should return data only (skip header + sub-header)
        let page0_data = ws.codes_data(0);
        assert_eq!(
            page0_data.len(),
            PAGE_64K - MOONPAGE_HEADER_SIZE - VEC_CODES_SUB_HEADER_SIZE
        );
        // First 1000 bytes should be our data
        assert_eq!(&page0_data[..1000], &codes[..1000]);

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
                assert!(
                    e.to_string().contains("codes.mpf"),
                    "error should mention codes.mpf: {e}"
                );
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

        // Fill codes with incompressible data
        let codes = incompressible_data(500);
        let graph = incompressible_data(200);
        let mvcc = vec![0u8; 24 * 5];
        write_test_segment(&seg_dir, 7, &codes, &graph, &mvcc);

        let handle = SegmentHandle::new(7, seg_dir.clone());
        let ws = WarmSegmentFiles::open(&seg_dir, handle, false).unwrap();

        // codes_data(0) should skip the 64-byte header + 32-byte sub-header
        let cd = ws.codes_data(0);
        assert_eq!(&cd[..500], &codes[..], "codes data mismatch");

        // graph_data(0) should skip the 64-byte header + 16-byte sub-header
        let gd = ws.graph_data(0);
        assert_eq!(&gd[..200], &graph[..]);
    }

    #[test]
    fn test_write_mpf_compressed_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("graph.mpf");

        // 2KB of highly compressible repeating pattern
        let mut data = Vec::with_capacity(2048);
        for i in 0..2048 {
            data.push((i % 4) as u8);
        }

        write_graph_mpf(&path, 1, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        // Should produce 1 page (4016 data capacity > 2048)
        assert_eq!(file_bytes.len(), PAGE_4K);

        let hdr = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        // COMPRESSED flag should be set since data_len=2048 > 256 and pattern is compressible
        assert_ne!(
            hdr.flags & page_flags::COMPRESSED,
            0,
            "COMPRESSED flag should be set for compressible data > 256 bytes"
        );
        // payload_bytes should be less than uncompressed (sub_hdr + 2048)
        assert!(
            (hdr.payload_bytes as usize) < VEC_GRAPH_SUB_HEADER_SIZE + 2048,
            "compressed payload_bytes ({}) should be less than uncompressed ({})",
            hdr.payload_bytes,
            VEC_GRAPH_SUB_HEADER_SIZE + 2048,
        );
        // CRC should still be valid
        assert!(MoonPageHeader::verify_checksum(&file_bytes[..PAGE_4K]));
    }

    #[test]
    fn test_write_mpf_small_payload_not_compressed() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("graph.mpf");

        // 100 bytes -- below 256 threshold
        let data = vec![0xABu8; 100];
        write_graph_mpf(&path, 2, &data).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert_eq!(file_bytes.len(), PAGE_4K);

        let hdr = MoonPageHeader::read_from(&file_bytes[..MOONPAGE_HEADER_SIZE]).unwrap();
        assert_eq!(
            hdr.flags & page_flags::COMPRESSED,
            0,
            "COMPRESSED flag should NOT be set for small payloads"
        );
        // payload_bytes = sub_hdr(16) + 100 = 116
        assert_eq!(hdr.payload_bytes as usize, VEC_GRAPH_SUB_HEADER_SIZE + 100);
    }
}
