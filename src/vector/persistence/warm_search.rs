//! WarmSearchSegment -- mmap-backed search over warm tier segments.
//!
//! Provides the same search interface as `ImmutableSegment` but reads TQ codes
//! and HNSW graph from mmap'd .mpf files instead of in-memory buffers.
//! This is the critical piece that makes warm-transitioned segments searchable.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::persistence::page::{
    MOONPAGE_HEADER_SIZE, MoonPageHeader, PAGE_4K, PAGE_64K, page_flags,
};
use crate::storage::tiered::SegmentHandle;
use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::hnsw::search::{SearchScratch, hnsw_search_filtered};
use crate::vector::persistence::warm_segment::{
    VEC_CODES_SUB_HEADER_SIZE, VEC_FULL_SUB_HEADER_SIZE, VEC_GRAPH_SUB_HEADER_SIZE,
    VEC_MVCC_SUB_HEADER_SIZE,
};
use crate::vector::segment::raw_f16_store::RawF16Store;
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
    /// Original key hash per BFS position (MVCC headers from mvcc.mpf) --
    /// propagated onto `SearchResult.key_hash` so the FT.SEARCH response
    /// layer's `key_hash_to_key` lookup resolves the real Redis key instead
    /// of falling back to a synthetic `vec:<id>` (see `parse_mvcc_ids`).
    key_hashes: Vec<u64>,
    /// Segment handle prevents directory deletion while this struct is alive.
    _handle: SegmentHandle,
    /// Timestamp when this warm segment was created (for cold tier aging).
    created_at: std::time::Instant,
    /// Microseconds since epoch of the last search access.
    /// Updated atomically on every search call. Used by MmapBudget for
    /// LRU ordering without requiring a mutable reference to the budget.
    /// Relaxed ordering is sufficient: approximate recency is all we need.
    last_access_micros: AtomicU64,
    /// Exact-rerank sidecar (HQ-1 parity, WS3): f16 copy of each original
    /// vector, BFS-ordered, `dimension` halves per entry, extracted from
    /// `vectors.mpf` if the HOT->WARM transition wrote one (see
    /// `VectorIndex::try_warm_transitions`). `None` for segments transitioned
    /// before this sidecar was threaded through, or reloaded from a directory
    /// that predates it — search then silently falls back to the quantized
    /// TQ-ADC beam distance, exactly like a pre-sidecar `ImmutableSegment`.
    raw_f16: Option<RawF16Store>,
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

/// Parse MVCC entries from mvcc.mpf payload bytes to extract global IDs and
/// key hashes.
///
/// Each MVCC entry is 32 bytes: internal_id(u32 LE) + global_id(u32 LE) +
/// key_hash(u64 LE) + insert_lsn(u64 LE) + delete_lsn(u64 LE) — this MUST
/// match `ImmutableSegment::mvcc_raw_bytes`'s layout exactly, since that is
/// the writer for the bytes this function reads back from `mvcc.mpf`
/// (`VectorIndex::try_warm_transitions`). A prior version of this function
/// assumed a 24-byte entry with no `key_hash` field at all (stale relative to
/// `mvcc_raw_bytes`, which had grown a `key_hash` field) — every entry past
/// the first was misaligned, and `key_hash` was silently dropped entirely,
/// so `SearchResult.key_hash` stayed `0` for every WARM-tier hit and
/// `key_hash_to_key` lookups downstream fell back to a synthetic `vec:<id>`
/// key instead of the real one (caught by
/// `tests/vector_idle_unload.rs`'s post-transition FT.SEARCH assertion).
fn parse_mvcc_ids(mvcc_payload: &[u8]) -> (Vec<u32>, Vec<u64>) {
    const ENTRY_SIZE: usize = 32;
    let count = mvcc_payload.len() / ENTRY_SIZE;
    let mut global_ids = Vec::with_capacity(count);
    let mut key_hashes = Vec::with_capacity(count);

    for i in 0..count {
        let base = i * ENTRY_SIZE;
        let global_off = base + 4; // skip internal_id (4 bytes)
        let key_hash_off = global_off + 4; // skip global_id (4 bytes)
        if key_hash_off + 8 <= mvcc_payload.len() {
            let global_id = u32::from_le_bytes(
                mvcc_payload[global_off..global_off + 4]
                    .try_into()
                    .expect("4-byte slice"),
            );
            let key_hash = u64::from_le_bytes(
                mvcc_payload[key_hash_off..key_hash_off + 8]
                    .try_into()
                    .expect("8-byte slice"),
            );
            global_ids.push(global_id);
            key_hashes.push(key_hash);
        }
    }

    (global_ids, key_hashes)
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
        // Open and mmap codes.mpf (64KB pages).
        //
        // The mmaps below live only for the duration of `open()` -- payload
        // bytes are extracted into owned `Vec<u8>` (`codes_data`, `global_ids`)
        // before this function returns, and the mmaps are dropped at scope
        // exit. So the mmap-validity window is bounded by a single function
        // call against an atomically-renamed sealed file. See
        // `WarmSegmentFiles` for the long-lived-mmap variant and the full
        // invariant chain it relies on.
        // Sealed-after-rename warm-segment files; see
        // `vector::persistence::sealed_mmap` module docs for the seal contract.
        // The mmaps live only for the duration of `open()` — payload bytes are
        // copied into owned `Vec<u8>` before this function returns.
        use crate::vector::persistence::sealed_mmap::{
            AccessPattern, advise_pattern, lock_resident, map_sealed_file,
        };

        let codes_mmap = map_sealed_file(&segment_dir.join("codes.mpf"))?;
        advise_pattern(&codes_mmap, AccessPattern::Sequential)?;
        if mlock_codes {
            if let Err(e) = lock_resident(&codes_mmap) {
                tracing::warn!("mlock codes.mpf failed for segment {segment_id}: {e}");
            }
        }

        // Open and mmap graph.mpf (4KB pages)
        let graph_mmap = map_sealed_file(&segment_dir.join("graph.mpf"))?;
        advise_pattern(&graph_mmap, AccessPattern::Random)?;

        // Open and mmap mvcc.mpf (4KB pages)
        let mvcc_mmap = map_sealed_file(&segment_dir.join("mvcc.mpf"))?;
        advise_pattern(&mvcc_mmap, AccessPattern::Sequential)?;
        // Lock mvcc pages in RAM -- visibility checks run on every query (design S14).
        // Failure is non-fatal: mlock may fail in containers or when RLIMIT_MEMLOCK is low.
        if let Err(e) = lock_resident(&mvcc_mmap) {
            tracing::warn!(
                "mlock mvcc.mpf failed for segment {segment_id}: {e} (continuing without mlock)"
            );
        }

        // Extract contiguous data from each file (skipping per-page sub-headers)
        let codes_data = extract_payloads(&codes_mmap, PAGE_64K, VEC_CODES_SUB_HEADER_SIZE);
        let graph_payload = extract_payloads(&graph_mmap, PAGE_4K, VEC_GRAPH_SUB_HEADER_SIZE);
        let mvcc_payload = extract_payloads(&mvcc_mmap, PAGE_4K, VEC_MVCC_SUB_HEADER_SIZE);

        // Optional exact-rerank sidecar (WS3): vectors.mpf only exists when
        // the HOT->WARM transition had a raw_f16 buffer to carry over. A
        // missing file is the expected, silent "no sidecar" case (matches
        // ImmutableSegment's own `RawF16Store::map_file` contract) — never
        // fails segment load, only degrades to ADC-only distances.
        let raw_f16 = match map_sealed_file(&segment_dir.join("vectors.mpf")) {
            Ok(vectors_mmap) => {
                let payload = extract_payloads(&vectors_mmap, PAGE_64K, VEC_FULL_SUB_HEADER_SIZE);
                if payload.len() % 2 != 0 {
                    tracing::warn!(
                        "vectors.mpf for warm segment {segment_id} has an odd byte length \
                         ({} bytes) — discarding malformed sidecar, falling back to ADC",
                        payload.len()
                    );
                    None
                } else {
                    let halves: Vec<u16> = payload
                        .chunks_exact(2)
                        .map(|c| u16::from_le_bytes([c[0], c[1]]))
                        .collect();
                    Some(RawF16Store::Owned(halves))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                tracing::warn!(
                    "failed to open vectors.mpf for warm segment {segment_id}: {e} \
                     (continuing without exact-rerank sidecar)"
                );
                None
            }
        };

        // Auto-detect compressed vs uncompressed graph format.
        // Compressed format (Phase 84+) has version_tag=0x01 at byte offset 15.
        // Uncompressed format has layer0_len (u32 LE) starting at offset 15.
        // Detect by checking: if byte 15 is 0x01, try compressed first;
        // fall back to uncompressed for legacy segments.
        let graph = if graph_payload.len() > 15 && graph_payload[15] == 0x01 {
            HnswGraph::from_bytes_compressed(&graph_payload)
                .or_else(|_| HnswGraph::from_bytes(&graph_payload))
        } else {
            HnswGraph::from_bytes(&graph_payload)
        }
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("graph deserialization failed: {e}"),
            )
        })?;

        let total_count = graph.num_nodes();
        let (global_ids, key_hashes) = parse_mvcc_ids(&mvcc_payload);

        Ok(Self {
            segment_id,
            codes_data,
            graph,
            collection_meta,
            total_count,
            global_ids,
            key_hashes,
            _handle: handle,
            created_at: std::time::Instant::now(),
            raw_f16,
            last_access_micros: AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64,
            ),
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
        // Record this search so the mmap budget can make accurate LRU decisions.
        self.touch_last_access();
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

        // WS3 / HQ-1 parity: exact rerank from the f16 sidecar when the
        // HOT->WARM transition carried one over. No-op (falls back to ADC)
        // when `raw_f16` is `None` — same contract as `ImmutableSegment`.
        self.rerank_exact(&mut candidates, query, k);

        candidates.truncate(k);
        self.remap_to_global_ids(&mut candidates);
        candidates
    }

    /// Exact rerank (HQ-1 parity): re-score the top `4*k` ADC-ranked
    /// candidates with (near-)exact distances decoded from the f16 sidecar,
    /// then re-sort ascending. No-op when this segment has no sidecar.
    ///
    /// Mirrors `ImmutableSegment::rerank_exact` exactly (same distance
    /// conventions: true squared L2 for `DistanceMetric::L2`, `2 - 2*cos`
    /// for the unit-sphere metrics) so cross-segment merge stays consistent
    /// whether a candidate came from a HOT or WARM segment.
    fn rerank_exact(&self, candidates: &mut SmallVec<[SearchResult; 32]>, query: &[f32], k: usize) {
        let Some(store) = self.raw_f16.as_ref() else {
            return;
        };
        let raw = store.as_slice();
        if candidates.is_empty() || raw.is_empty() {
            return;
        }
        let rerank_n = (4 * k.max(1)).min(candidates.len());
        let dim = self.collection_meta.dimension as usize;
        let is_l2 = self.collection_meta.metric == crate::vector::types::DistanceMetric::L2;

        let mut q_unit: Vec<f32> = Vec::new();
        let q_ref: &[f32] = if is_l2 {
            query
        } else {
            let norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                let inv = 1.0 / norm;
                q_unit.extend(query.iter().map(|x| x * inv));
            } else {
                q_unit.extend_from_slice(query);
            }
            &q_unit
        };

        let dist_table = crate::vector::distance::table();
        for result in candidates[..rerank_n].iter_mut() {
            let bfs_pos = self.graph.to_bfs(result.id.0) as usize;
            let start = bfs_pos * dim;
            let Some(vec_f16) = raw.get(start..start + dim) else {
                continue; // Out-of-range id: keep the ADC estimate.
            };
            if is_l2 {
                result.distance = (dist_table.f16_l2)(q_ref, vec_f16);
            } else {
                let (dot, xsq) = (dist_table.f16_dot_normsq)(q_ref, vec_f16);
                if xsq > 0.0 {
                    let cos = (dot / xsq.sqrt()).clamp(-1.0, 1.0);
                    result.distance = 2.0 - 2.0 * cos;
                }
            }
        }
        candidates.sort_unstable();
    }

    /// Read-only access to the exact-rerank f16 sidecar, if present.
    #[inline]
    pub fn raw_f16(&self) -> Option<&[u16]> {
        self.raw_f16.as_ref().map(RawF16Store::as_slice)
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

    /// Microseconds since UNIX epoch of the last search access.
    ///
    /// Used by `MmapBudget::enforce_budget` for LRU ordering. Higher value =
    /// more recently accessed. Read with `Relaxed` ordering — approximate
    /// recency is sufficient for eviction decisions.
    #[inline]
    pub fn last_access_micros(&self) -> u64 {
        self.last_access_micros.load(Ordering::Relaxed)
    }

    /// Bump the last-access timestamp. Called on every search.
    #[inline]
    fn touch_last_access(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        self.last_access_micros.store(now, Ordering::Relaxed);
    }

    /// Estimated resident bytes for this warm segment.
    ///
    /// Accounts for the owned `codes_data` buffer, the HNSW graph heap, the
    /// `global_ids` vec, and (WS3) the exact-rerank sidecar if present. This
    /// is the figure tracked by [`crate::vector::persistence::mmap_budget::MmapBudget`].
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.codes_data.len()
            + self.graph.resident_bytes()
            + self.global_ids.len() * std::mem::size_of::<u32>()
            + self.key_hashes.len() * std::mem::size_of::<u64>()
            + self.raw_f16.as_ref().map_or(0, RawF16Store::resident_bytes)
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

    /// Remap per-segment internal IDs to globally unique IDs, and propagate
    /// each result's original key hash (see the `key_hashes` field doc).
    ///
    /// HNSW search returns VectorId(original_id). We convert through BFS mapping
    /// to global IDs stored in the MVCC data, same pattern as ImmutableSegment.
    fn remap_to_global_ids(&self, candidates: &mut SmallVec<[SearchResult; 32]>) {
        for c in candidates.iter_mut() {
            let bfs_pos = self.graph.to_bfs(c.id.0);
            let bfs_pos = bfs_pos as usize;
            if bfs_pos < self.global_ids.len() {
                c.id = VectorId(self.global_ids[bfs_pos]);
            }
            if bfs_pos < self.key_hashes.len() {
                c.key_hash = self.key_hashes[bfs_pos];
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
        assert!(
            warm.raw_f16().is_none(),
            "no vectors.mpf written -- sidecar must be absent, not an error"
        );
    }

    /// WS3: a `vectors.mpf` sidecar written by the HOT->WARM transition
    /// (`crate::storage::tiered::warm_tier::transition_to_warm`) must survive
    /// the round trip through `WarmSearchSegment::from_files` byte-for-byte
    /// -- this is the exact-rerank recall guarantee the idle-unload path
    /// depends on (see `VectorIndex::try_warm_transitions_idle`).
    #[test]
    fn test_warm_search_segment_loads_vectors_mpf_sidecar() {
        use crate::vector::persistence::warm_segment::write_vectors_mpf;
        use crate::vector::segment::raw_f16_store::RawF16Store;

        distance::init();
        let dim = 8usize;
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
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

        // Two BFS-ordered "vectors" of `dim` f16 halves each (values chosen
        // to exercise the full u16 byte range, catching any endianness bug).
        let halves: Vec<u16> = (0..dim as u16 * 2).map(|i| i.wrapping_mul(4001)).collect();
        let vectors_bytes = RawF16Store::le_bytes(&halves);

        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-7");
        write_test_mpf_segment(&seg_dir, 7, &[], &graph_bytes, &[]);
        write_vectors_mpf(&seg_dir.join("vectors.mpf"), 7, &vectors_bytes).unwrap();

        let handle = SegmentHandle::new(7, seg_dir.clone());
        let warm = WarmSearchSegment::from_files(&seg_dir, 7, collection, handle, false).unwrap();

        let loaded = warm
            .raw_f16()
            .expect("vectors.mpf was written -- sidecar must load");
        assert_eq!(
            loaded,
            halves.as_slice(),
            "sidecar halves must round-trip byte-for-byte through the .mpf page format"
        );
        assert!(
            warm.resident_bytes() >= halves.len() * 2,
            "sidecar bytes must count toward resident_bytes (MmapBudget accounting)"
        );
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
    fn test_parse_mvcc_ids() {
        // Build 3 MVCC entries matching `ImmutableSegment::mvcc_raw_bytes`'s
        // actual 32-byte layout: internal_id(4) + global_id(4) + key_hash(8)
        // + insert_lsn(8) + delete_lsn(8).
        let mut mvcc_data = Vec::with_capacity(96);
        for i in 0u32..3 {
            mvcc_data.extend_from_slice(&i.to_le_bytes()); // internal_id
            mvcc_data.extend_from_slice(&(i + 100).to_le_bytes()); // global_id
            mvcc_data.extend_from_slice(&(0xBEEF_0000_u64 + i as u64).to_le_bytes()); // key_hash
            mvcc_data.extend_from_slice(&0u64.to_le_bytes()); // insert_lsn
            mvcc_data.extend_from_slice(&0u64.to_le_bytes()); // delete_lsn
        }

        let (ids, key_hashes) = parse_mvcc_ids(&mvcc_data);
        assert_eq!(ids, vec![100, 101, 102]);
        assert_eq!(key_hashes, vec![0xBEEF_0000, 0xBEEF_0001, 0xBEEF_0002]);
    }

    #[test]
    fn test_compressed_warm_segment_roundtrip() {
        use crate::persistence::page::PAGE_4K;
        use crate::vector::persistence::warm_segment::{
            VEC_GRAPH_SUB_HEADER_SIZE, write_graph_mpf,
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
            VEC_GRAPH_SUB_HEADER_SIZE, write_graph_mpf,
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

    #[test]
    fn test_compressed_graph_mpf_size_reduction() {
        use crate::vector::hnsw::graph::SENTINEL;

        // Build a realistic graph: 100 nodes, m0=32, with sequential neighbor IDs
        // (delta-friendly: sorted ascending, small deltas -> high compression)
        let m0: u8 = 32;
        let num_nodes: u32 = 100;
        let total_slots = num_nodes as usize * m0 as usize;
        let mut layer0 = vec![SENTINEL; total_slots];
        for node in 0..num_nodes as usize {
            // Each node connects to ~16 neighbors in its vicinity
            let neighbors_count = 16.min(num_nodes as usize);
            for j in 0..neighbors_count {
                let neighbor = (node + j + 1) % num_nodes as usize;
                layer0[node * m0 as usize + j] = neighbor as u32;
            }
            // Sort the neighbor slice (required for delta encoding)
            let start = node * m0 as usize;
            let end = start + neighbors_count;
            layer0[start..end].sort_unstable();
        }

        let graph = HnswGraph::new(
            num_nodes,
            16,
            m0,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::from_vec(layer0),
            (0..num_nodes).collect(),
            (0..num_nodes).collect(),
            vec![smallvec::SmallVec::new(); num_nodes as usize],
            vec![0; num_nodes as usize],
            68,
        );

        // Serialize both ways
        let raw = graph.to_bytes();
        let compressed = graph.to_bytes_compressed();

        eprintln!(
            "Graph size: raw={} bytes, compressed={} bytes, ratio={:.2}x",
            raw.len(),
            compressed.len(),
            raw.len() as f64 / compressed.len() as f64
        );
        assert!(
            compressed.len() < raw.len(),
            "compressed ({}) should be smaller than raw ({})",
            compressed.len(),
            raw.len()
        );

        // Write both to graph.mpf and compare file sizes
        let tmp = tempfile::tempdir().unwrap();

        let raw_path = tmp.path().join("graph_raw.mpf");
        write_graph_mpf(&raw_path, 1, &raw).unwrap();
        let raw_file_size = std::fs::metadata(&raw_path).unwrap().len();

        let comp_path = tmp.path().join("graph_comp.mpf");
        write_graph_mpf(&comp_path, 2, &compressed).unwrap();
        let comp_file_size = std::fs::metadata(&comp_path).unwrap().len();

        eprintln!(
            "graph.mpf size: raw={} bytes, compressed={} bytes, ratio={:.2}x",
            raw_file_size,
            comp_file_size,
            raw_file_size as f64 / comp_file_size as f64
        );
        assert!(
            comp_file_size < raw_file_size,
            "compressed graph.mpf ({}) should be smaller than raw ({})",
            comp_file_size,
            raw_file_size
        );

        // Verify roundtrip through compressed format
        let restored = HnswGraph::from_bytes_compressed(&compressed).unwrap();
        assert_eq!(restored.num_nodes(), graph.num_nodes());
        // Verify neighbor data preserved for a few nodes
        for node in [0u32, 1, 50, 99] {
            let orig = graph.neighbors_l0(node);
            let rest = restored.neighbors_l0(node);
            assert_eq!(orig, rest, "neighbors mismatch for node {node}");
        }
    }
}
