//! WarmSearchSegment -- mmap-backed search over warm tier segments.
//!
//! Provides the same search interface as `ImmutableSegment` but reads TQ codes
//! and HNSW graph from mmap'd .mpf files instead of in-memory buffers.
//! This is the critical piece that makes warm-transitioned segments searchable.

use std::path::Path;
use std::sync::Arc;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::persistence::page::{MoonPageHeader, MOONPAGE_HEADER_SIZE, PAGE_4K, PAGE_64K, page_flags};
use crate::vector::persistence::warm_segment::{
    VEC_CODES_SUB_HEADER_SIZE, VEC_GRAPH_SUB_HEADER_SIZE, VEC_MVCC_SUB_HEADER_SIZE,
};
use crate::storage::tiered::SegmentHandle;
use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::hnsw::search::{SearchScratch, hnsw_search_filtered};
use crate::vector::turbo_quant::collection::CollectionMetadata;
use crate::vector::types::{SearchResult, VectorId};

/// Read-only warm segment backed by mmap'd .mpf files.
///
/// Provides the same two-stage HNSW search as `ImmutableSegment`:
/// TQ-ADC beam search + optional reranking. The TQ codes and HNSW graph
/// are deserialized from MoonPage-format files at construction time.
///
/// Lifetime: the mmap'd files remain valid as long as this struct lives.
/// The `SegmentHandle` prevents the segment directory from being deleted.
pub struct WarmSearchSegment {
    /// Segment ID for logging and debugging.
    segment_id: u64,
    /// Contiguous TQ codes extracted from codes.mpf page payloads.
    /// Codes are in BFS order, same layout as ImmutableSegment.vectors_tq.
    codes_data: Vec<u8>,
    /// HNSW graph deserialized from graph.mpf page payloads.
    graph: HnswGraph,
    /// Collection metadata (needed for TQ-ADC distance computation).
    collection_meta: Arc<CollectionMetadata>,
    /// Total vector count in this segment.
    total_count: u32,
    /// Global ID offset for result remapping (MVCC headers from mvcc.mpf).
    /// Maps BFS position -> global vector ID.
    global_ids: Vec<u32>,
    /// Segment handle prevents directory deletion while this struct is alive.
    _handle: SegmentHandle,
    /// Timestamp when this warm segment was created (for cold tier aging).
    created_at: std::time::Instant,
}

/// Extract contiguous data bytes from a mmap'd .mpf file, skipping sub-headers.
///
/// MoonPage files interleave 64-byte headers with payload data. Each page type
/// has a type-specific sub-header between the MoonPageHeader and the actual data
/// (VecCodes: 32B, VecFull: 24B, VecGraph: 16B, VecMvcc: 8B). This function
/// reads each page header, skips the sub-header, and concatenates all data
/// regions into a contiguous buffer.
///
/// `sub_hdr_size` is the size of the per-page-type sub-header to skip.
fn extract_payloads(mmap: &memmap2::Mmap, page_size: usize, sub_hdr_size: usize) -> Vec<u8> {
    let total_header = MOONPAGE_HEADER_SIZE + sub_hdr_size;
    let data_capacity = page_size - total_header;
    let page_count = mmap.len() / page_size;
    let mut result = Vec::with_capacity(page_count * data_capacity);

    for page_idx in 0..page_count {
        let page_start = page_idx * page_size;
        let page_slice = &mmap[page_start..page_start + page_size];

        // Read the header to get actual payload length (includes sub-header)
        if let Some(hdr) = MoonPageHeader::read_from(&page_slice[..MOONPAGE_HEADER_SIZE]) {
            let total_payload = hdr.payload_bytes as usize;
            // Subtract sub-header to get actual data length (possibly compressed)
            let data_len = if total_payload > sub_hdr_size {
                (total_payload - sub_hdr_size).min(data_capacity)
            } else {
                0
            };

            if data_len == 0 {
                continue;
            }

            let data_region = &page_slice[total_header..total_header + data_len];

            if hdr.flags & page_flags::COMPRESSED != 0 {
                // LZ4-compressed page: decompress data region
                match lz4_flex::decompress_size_prepended(data_region) {
                    Ok(decompressed) => result.extend_from_slice(&decompressed),
                    Err(e) => {
                        tracing::warn!(
                            "LZ4 decompression failed for page {page_idx}: {e}, skipping"
                        );
                    }
                }
            } else {
                // Uncompressed page: copy raw data
                result.extend_from_slice(data_region);
            }
        }
    }

    result
}

/// Parse MVCC entries from mvcc.mpf payload bytes to extract global IDs.
///
/// Each MVCC entry is 24 bytes: internal_id(4) + global_id(4) + insert_lsn(8)
/// + delete_lsn(4) + undo_ptr(4). We only need the global_id for remapping.
fn parse_global_ids(mvcc_payload: &[u8]) -> Vec<u32> {
    const ENTRY_SIZE: usize = 24;
    let count = mvcc_payload.len() / ENTRY_SIZE;
    let mut ids = Vec::with_capacity(count);

    for i in 0..count {
        let offset = i * ENTRY_SIZE + 4; // skip internal_id (4 bytes)
        if offset + 4 <= mvcc_payload.len() {
            let global_id = u32::from_le_bytes([
                mvcc_payload[offset],
                mvcc_payload[offset + 1],
                mvcc_payload[offset + 2],
                mvcc_payload[offset + 3],
            ]);
            ids.push(global_id);
        }
    }

    ids
}

impl WarmSearchSegment {
    /// Construct a WarmSearchSegment from .mpf files in a segment directory.
    ///
    /// Opens codes.mpf and graph.mpf via mmap, extracts payload data, and
    /// deserializes the HNSW graph. The codes remain as a contiguous Vec<u8>
    /// for direct use in TQ-ADC distance computation.
    ///
    /// # Arguments
    /// * `segment_dir` - Path to the warm segment directory containing .mpf files
    /// * `segment_id` - Unique segment identifier
    /// * `collection_meta` - Collection metadata for TQ-ADC distance
    /// * `handle` - Segment handle preventing directory deletion
    /// * `mlock_codes` - Whether to mlock codes.mpf pages in RAM
    pub fn from_files(
        segment_dir: &Path,
        segment_id: u64,
        collection_meta: Arc<CollectionMetadata>,
        handle: SegmentHandle,
        mlock_codes: bool,
    ) -> std::io::Result<Self> {
        // Open and mmap codes.mpf (64KB pages)
        let codes_file = std::fs::File::open(segment_dir.join("codes.mpf"))?;
        // SAFETY: File is a sealed immutable warm segment. SegmentHandle refcount
        // prevents directory deletion while mapped. No concurrent writers exist.
        let codes_mmap = unsafe { memmap2::MmapOptions::new().map(&codes_file)? };
        codes_mmap.advise(memmap2::Advice::Sequential)?;
        if mlock_codes {
            if let Err(e) = codes_mmap.lock() {
                tracing::warn!("mlock codes.mpf failed for segment {segment_id}: {e}");
            }
        }

        // Open and mmap graph.mpf (4KB pages)
        let graph_file = std::fs::File::open(segment_dir.join("graph.mpf"))?;
        // SAFETY: Same invariants as codes -- sealed, immutable, refcount-protected.
        let graph_mmap = unsafe { memmap2::MmapOptions::new().map(&graph_file)? };
        graph_mmap.advise(memmap2::Advice::Random)?;

        // Open and mmap mvcc.mpf (4KB pages)
        let mvcc_file = std::fs::File::open(segment_dir.join("mvcc.mpf"))?;
        // SAFETY: Same invariants as codes -- sealed, immutable, refcount-protected.
        let mvcc_mmap = unsafe { memmap2::MmapOptions::new().map(&mvcc_file)? };
        mvcc_mmap.advise(memmap2::Advice::Sequential)?;
        // Lock mvcc pages in RAM -- visibility checks run on every query (design S14).
        // Failure is non-fatal: mlock may fail in containers or when RLIMIT_MEMLOCK is low.
        if let Err(e) = mvcc_mmap.lock() {
            tracing::warn!("mlock mvcc.mpf failed for segment {segment_id}: {e} (continuing without mlock)");
        }

        // Extract contiguous data from each file (skipping per-page sub-headers)
        let codes_data = extract_payloads(&codes_mmap, PAGE_64K, VEC_CODES_SUB_HEADER_SIZE);
        let graph_payload = extract_payloads(&graph_mmap, PAGE_4K, VEC_GRAPH_SUB_HEADER_SIZE);
        let mvcc_payload = extract_payloads(&mvcc_mmap, PAGE_4K, VEC_MVCC_SUB_HEADER_SIZE);

        // Auto-detect compressed vs uncompressed graph format.
        // Compressed format (Phase 84+) has version_tag=0x01 at byte offset 15.
        // Uncompressed format has layer0_len (u32 LE) starting at offset 15.
        // Detect by checking: if byte 15 is 0x01, try compressed first;
        // fall back to uncompressed for legacy segments.
        let graph = if graph_payload.len() > 15 && graph_payload[15] == 0x01 {
            HnswGraph::from_bytes_compressed(&graph_payload).or_else(|_| {
                HnswGraph::from_bytes(&graph_payload)
            })
        } else {
            HnswGraph::from_bytes(&graph_payload)
        }.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("graph deserialization failed: {e}"),
            )
        })?;

        let total_count = graph.num_nodes();
        let global_ids = parse_global_ids(&mvcc_payload);

        Ok(Self {
            segment_id,
            codes_data,
            graph,
            collection_meta,
            total_count,
            global_ids,
            _handle: handle,
            created_at: std::time::Instant::now(),
        })
    }

    /// HNSW search over mmap-backed TQ codes. Same algorithm as ImmutableSegment.
    ///
    /// Stage 1: HNSW beam search with TQ-ADC distance on codes from mmap.
    /// Results are remapped to global IDs for cross-segment merging.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
    ) -> SmallVec<[SearchResult; 32]> {
        self.search_filtered(query, k, ef_search, scratch, None)
    }

    /// HNSW search with optional filter bitmap.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
        allow_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        if self.total_count == 0 {
            return SmallVec::new();
        }

        // Use hnsw_search_filtered (same function ImmutableSegment uses).
        // No sub-centroid signs available for warm segments (not persisted in .mpf).
        let empty_sub_signs: &[u8] = &[];
        let mut candidates = hnsw_search_filtered(
            &self.graph,
            &self.codes_data,
            query,
            &self.collection_meta,
            ef_search,
            ef_search,
            scratch,
            allow_bitmap,
            empty_sub_signs,
            0,
        );

        candidates.truncate(k);
        self.remap_to_global_ids(&mut candidates);
        candidates
    }

    /// Total vector count in this warm segment.
    #[inline]
    pub fn total_count(&self) -> u32 {
        self.total_count
    }

    /// Segment ID for debugging.
    #[inline]
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Segment age in seconds since creation (used for cold tier transition).
    #[inline]
    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Read-only access to the raw TQ codes (for PQ training during cold transition).
    #[inline]
    pub fn codes_data(&self) -> &[u8] {
        &self.codes_data
    }

    /// Read-only access to the HNSW graph (for Vamana warm-start during cold transition).
    #[inline]
    pub fn graph(&self) -> &HnswGraph {
        &self.graph
    }

    /// Read-only access to collection metadata.
    #[inline]
    pub fn collection_meta(&self) -> &CollectionMetadata {
        &self.collection_meta
    }

    /// Mark this segment's on-disk directory for deletion.
    ///
    /// The directory is only removed once all `SegmentHandle` clones are dropped
    /// (i.e., no in-flight searches hold a reference). This enables safe cleanup
    /// after compaction or index drop without racing with concurrent readers.
    pub fn mark_tombstoned(&self) {
        self._handle.mark_tombstoned();
    }

    /// Remap per-segment internal IDs to globally unique IDs.
    ///
    /// HNSW search returns VectorId(original_id). We convert through BFS mapping
    /// to global IDs stored in the MVCC data, same pattern as ImmutableSegment.
    fn remap_to_global_ids(&self, candidates: &mut SmallVec<[SearchResult; 32]>) {
        for c in candidates.iter_mut() {
            let bfs_pos = self.graph.to_bfs(c.id.0);
            if (bfs_pos as usize) < self.global_ids.len() {
                c.id = VectorId(self.global_ids[bfs_pos as usize]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::persistence::warm_segment::{
        write_codes_mpf, write_graph_mpf, write_mvcc_mpf,
    };
    use crate::vector::turbo_quant::collection::QuantizationConfig;
    use crate::vector::types::DistanceMetric;

    /// Write test .mpf files from raw data.
    fn write_test_mpf_segment(
        seg_dir: &Path,
        file_id: u64,
        codes: &[u8],
        graph_bytes: &[u8],
        mvcc_bytes: &[u8],
    ) {
        std::fs::create_dir_all(seg_dir).unwrap();
        write_codes_mpf(&seg_dir.join("codes.mpf"), file_id, codes).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), file_id, graph_bytes).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), file_id, mvcc_bytes).unwrap();
    }

    #[test]
    fn test_warm_search_segment_creation() {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));

        // Build a minimal empty graph
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph_bytes = empty_graph.to_bytes();

        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-1");
        write_test_mpf_segment(&seg_dir, 1, &[], &graph_bytes, &[]);

        let handle = SegmentHandle::new(1, seg_dir.clone());
        let warm = WarmSearchSegment::from_files(&seg_dir, 1, collection, handle, false).unwrap();

        assert_eq!(warm.total_count(), 0);
        assert_eq!(warm.segment_id(), 1);
    }

    #[test]
    fn test_warm_search_empty_returns_no_results() {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));

        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph_bytes = empty_graph.to_bytes();

        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-2");
        write_test_mpf_segment(&seg_dir, 2, &[], &graph_bytes, &[]);

        let handle = SegmentHandle::new(2, seg_dir.clone());
        let warm = WarmSearchSegment::from_files(&seg_dir, 2, collection, handle, false).unwrap();

        let query = vec![0.0f32; 128];
        let mut scratch = SearchScratch::new(0, 128);
        let results = warm.search(&query, 5, 64, &mut scratch);
        assert!(results.is_empty());
    }

    #[test]
    fn test_parse_global_ids() {
        // Build 3 MVCC entries (24 bytes each)
        let mut mvcc_data = Vec::with_capacity(72);
        for i in 0u32..3 {
            mvcc_data.extend_from_slice(&i.to_le_bytes()); // internal_id
            mvcc_data.extend_from_slice(&(i + 100).to_le_bytes()); // global_id
            mvcc_data.extend_from_slice(&0u64.to_le_bytes()); // insert_lsn
            mvcc_data.extend_from_slice(&0u32.to_le_bytes()); // delete_lsn
            mvcc_data.extend_from_slice(&0u32.to_le_bytes()); // undo_ptr
        }

        let ids = parse_global_ids(&mvcc_data);
        assert_eq!(ids, vec![100, 101, 102]);
    }

    #[test]
    fn test_compressed_warm_segment_roundtrip() {
        use crate::persistence::page::PAGE_4K;
        use crate::vector::persistence::warm_segment::{
            write_graph_mpf, VEC_GRAPH_SUB_HEADER_SIZE,
        };

        // 4KB of repeating compressible pattern (will span 2 pages at 4016 data cap)
        let mut graph_data = Vec::with_capacity(4096);
        for i in 0..4096 {
            graph_data.push((i % 7) as u8);
        }

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("graph.mpf");
        write_graph_mpf(&path, 10, &graph_data).unwrap();

        // Open via mmap and extract payloads (should decompress transparently)
        let file = std::fs::File::open(&path).unwrap();
        // SAFETY: test-only, file is immutable after write.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };

        let extracted = extract_payloads(&mmap, PAGE_4K, VEC_GRAPH_SUB_HEADER_SIZE);
        assert_eq!(
            extracted, graph_data,
            "decompressed data must match original input"
        );
    }

    #[test]
    fn test_extract_payloads_handles_mixed_compressed_uncompressed() {
        use crate::persistence::page::PAGE_4K;
        use crate::vector::persistence::warm_segment::{
            write_graph_mpf, VEC_GRAPH_SUB_HEADER_SIZE,
        };

        // Test 1: Large compressible data (>256 bytes) -- should be compressed
        {
            let mut data = Vec::with_capacity(1024);
            for i in 0..1024 {
                data.push((i % 3) as u8);
            }
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("graph.mpf");
            write_graph_mpf(&path, 20, &data).unwrap();

            let file = std::fs::File::open(&path).unwrap();
            // SAFETY: test-only, file is immutable after write.
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
            let extracted = extract_payloads(&mmap, PAGE_4K, VEC_GRAPH_SUB_HEADER_SIZE);
            assert_eq!(extracted, data, "large compressible data roundtrip failed");
        }

        // Test 2: Small data (<=256 bytes) -- should NOT be compressed
        {
            let data = vec![0xCDu8; 100];
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("graph.mpf");
            write_graph_mpf(&path, 21, &data).unwrap();

            let file = std::fs::File::open(&path).unwrap();
            // SAFETY: test-only, file is immutable after write.
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
            let extracted = extract_payloads(&mmap, PAGE_4K, VEC_GRAPH_SUB_HEADER_SIZE);
            assert_eq!(extracted, data, "small uncompressed data roundtrip failed");
        }
    }
}
