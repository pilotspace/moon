//! Read-only segment with HNSW graph and TurboQuant codes.
//!
//! Truly immutable after construction -- no locks needed for search.

use std::sync::Arc;
use std::time::Instant;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::hnsw::search::{
    SearchScratch, hnsw_search, hnsw_search_filtered, hnsw_search_subcent,
};
#[allow(unused_imports)]
use crate::vector::hnsw::search_sq::hnsw_search_f32;
use crate::vector::turbo_quant::collection::CollectionMetadata;
use crate::vector::turbo_quant::inner_product::{prepare_query_prod, score_l2_prod};
use crate::vector::turbo_quant::sub_centroid;
use crate::vector::types::SearchResult;
use crate::vector::types::VectorId;

/// MVCC header for immutable segment entries.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MvccHeader {
    pub internal_id: u32,
    /// Global vector index from the original mutable segment.
    /// Used to produce globally unique VectorIds in search results.
    pub global_id: u32,
    pub key_hash: u64,
    pub insert_lsn: u64,
    pub delete_lsn: u64,
    /// CLOG hint bit: 1 = transaction is known committed, skip CLOG lookup.
    /// 0 = unknown, must check CLOG. Set lazily on first successful CLOG lookup.
    pub hint_committed: u8,
}

/// Read-only segment. Truly immutable after construction -- no locks needed.
///
/// Two-stage search: HNSW beam search with TQ-ADC (fast candidate retrieval),
/// then TurboQuant_prod reranking (unbiased L2 distance estimation).
/// No f32 vectors stored — only TQ codes + QJL sign bits.
pub struct ImmutableSegment {
    graph: HnswGraph,
    vectors_tq: AlignedBuffer<u8>,
    /// QJL sign bits per vector, contiguous, qjl_bytes_per_vec per entry.
    qjl_signs: Vec<u8>,
    /// Residual norms per vector (one f32 each).
    residual_norms: Vec<f32>,
    qjl_bytes_per_vec: usize,
    /// Sub-centroid sign bits per vector (ceil(padded_dim/8) bytes each).
    /// For sign-bit refinement reranking (2× effective quantization resolution).
    sub_centroid_signs: Vec<u8>,
    sub_sign_bytes_per_vec: usize,
    mvcc: Vec<MvccHeader>,
    collection_meta: Arc<CollectionMetadata>,
    live_count: u32,
    total_count: u32,
    /// Timestamp when this segment was created (for warm tier age-based transition).
    created_at: Instant,
}

impl ImmutableSegment {
    /// Construct from compaction output.
    pub fn new(
        graph: HnswGraph,
        vectors_tq: AlignedBuffer<u8>,
        qjl_signs: Vec<u8>,
        residual_norms: Vec<f32>,
        qjl_bytes_per_vec: usize,
        sub_centroid_signs: Vec<u8>,
        sub_sign_bytes_per_vec: usize,
        mvcc: Vec<MvccHeader>,
        collection_meta: Arc<CollectionMetadata>,
        live_count: u32,
        total_count: u32,
    ) -> Self {
        Self {
            graph,
            vectors_tq,
            qjl_signs,
            residual_norms,
            qjl_bytes_per_vec,
            sub_centroid_signs,
            sub_sign_bytes_per_vec,
            mvcc,
            collection_meta,
            live_count,
            total_count,
            created_at: Instant::now(),
        }
    }

    /// Two-stage HNSW search: TQ-ADC beam + TurboQuant_prod reranking.
    ///
    /// Stage 1: HNSW beam search with TQ-ADC distance → ef candidates.
    /// Stage 2: Rerank candidates using TurboQuant_prod inner product estimator
    ///   for unbiased L2 distance. No f32 needed.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
    ) -> SmallVec<[SearchResult; 32]> {
        // Use sub-centroid signs during beam (32-level LUT) when available.
        // This eliminates the separate rerank pass — beam itself is high-accuracy.
        // Note: passing ef_search for both k and ef_search is intentional.
        // HNSW returns up to `ef_search` candidates (no early truncation to k).
        // This preserves candidates for cross-segment merging in the caller,
        // which does the final top-k selection after merging all segments.
        let mut candidates = if !self.sub_centroid_signs.is_empty() {
            hnsw_search_subcent(
                &self.graph,
                self.vectors_tq.as_slice(),
                query,
                &self.collection_meta,
                ef_search,
                ef_search,
                scratch,
                &self.sub_centroid_signs,
                self.sub_sign_bytes_per_vec,
            )
        } else {
            let mut cands = hnsw_search(
                &self.graph,
                self.vectors_tq.as_slice(),
                query,
                &self.collection_meta,
                ef_search,
                ef_search,
                scratch,
            );
            // Fallback: rerank with TQ_prod when no sub-centroid data
            self.rerank_with_prod(&mut cands, query);
            cands
        };
        candidates.truncate(k);
        self.remap_to_global_ids(&mut candidates);
        candidates
    }

    /// Two-stage HNSW search with filter bitmap.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
        allow_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        // Note: passing ef_search for both k and ef_search is intentional
        // (see comment in search() method above).
        let mut candidates = hnsw_search_filtered(
            &self.graph,
            self.vectors_tq.as_slice(),
            query,
            &self.collection_meta,
            ef_search,
            ef_search,
            scratch,
            allow_bitmap,
            &self.sub_centroid_signs,
            self.sub_sign_bytes_per_vec,
        );

        // When sub-centroid signs are used in beam, no rerank needed.
        // Only rerank if beam used standard 16-level scoring.
        if self.sub_centroid_signs.is_empty() {
            self.rerank_with_prod(&mut candidates, query);
        }
        candidates.truncate(k);
        self.remap_to_global_ids(&mut candidates);
        candidates
    }

    /// Remap per-segment internal IDs to globally unique IDs.
    /// HNSW search returns VectorId(original_id) where original_id is the index
    /// within this segment's live_entries. We convert to the global sequential ID
    /// stored in MvccHeader so results can be correctly merged across segments.
    fn remap_to_global_ids(&self, candidates: &mut SmallVec<[SearchResult; 32]>) {
        for c in candidates.iter_mut() {
            let orig_id = c.id.0;
            let bfs_pos = self.graph.to_bfs(orig_id);
            if (bfs_pos as usize) < self.mvcc.len() {
                let hdr = &self.mvcc[bfs_pos as usize];
                c.id = VectorId(hdr.global_id);
                c.key_hash = hdr.key_hash;
            }
        }
    }

    /// Rerank candidates using sub-centroid sign-bit refinement.
    ///
    /// 2× effective quantization resolution (32 levels at 4-bit) without
    /// QJL matrix overhead. Better recall than TQ-ADC for the same cost.
    #[allow(dead_code)]
    fn rerank_with_sub_centroid(
        &self,
        candidates: &mut SmallVec<[SearchResult; 32]>,
        query: &[f32],
    ) {
        if candidates.is_empty() || self.sub_centroid_signs.is_empty() {
            return;
        }

        let sub_table = match &self.collection_meta.sub_centroid_table {
            Some(t) => t,
            None => return,
        };

        let dim = self.collection_meta.dimension as usize;
        let padded = self.collection_meta.padded_dimension as usize;
        let bytes_per_code = self.graph.bytes_per_code() as usize;
        let code_len = bytes_per_code - 4;
        let sub_bpv = self.sub_sign_bytes_per_vec;

        // Prepare FWHT-rotated query
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(query);
        let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rotated[..dim].iter_mut() {
                *v *= inv;
            }
        }
        crate::vector::turbo_quant::fwht::fwht(
            &mut q_rotated,
            self.collection_meta.fwht_sign_flips.as_slice(),
        );

        let tq_buf = self.vectors_tq.as_slice();

        for result in candidates.iter_mut() {
            let bfs_pos = self.graph.to_bfs(result.id.0) as usize;
            let tq_offset = bfs_pos * bytes_per_code;
            let tq_code = &tq_buf[tq_offset..tq_offset + code_len];
            let norm_bytes = &tq_buf[tq_offset + code_len..tq_offset + bytes_per_code];
            let norm =
                f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);

            let sub_offset = bfs_pos * sub_bpv;
            let sign_bits = &self.sub_centroid_signs[sub_offset..sub_offset + sub_bpv];

            result.distance =
                sub_centroid::tq_sign_l2_adc(&q_rotated, tq_code, sign_bits, norm, sub_table);
        }
        candidates.sort_unstable();
    }

    /// Rerank candidates using TurboQuant_prod unbiased inner product estimator.
    ///
    /// For each candidate: compute L2 distance via
    ///   ||q - x||² = ||q||² + ||x||² - 2 * (<q, x_mse> + QJL_correction)
    ///
    /// Term 1 (<q, x_mse>) computed in rotated space: O(padded_dim).
    /// Term 2 (QJL correction) uses precomputed S*y: O(dim).
    /// Total per candidate: O(padded_dim) — same cost as TQ-ADC.
    fn rerank_with_prod(&self, candidates: &mut SmallVec<[SearchResult; 32]>, query: &[f32]) {
        if candidates.is_empty() || self.qjl_signs.is_empty() {
            return;
        }

        let dim = self.collection_meta.dimension as usize;
        let padded = self.collection_meta.padded_dimension as usize;
        let centroids = self.collection_meta.codebook_16();
        let bytes_per_code = self.graph.bytes_per_code() as usize;
        let code_len = bytes_per_code - 4;
        let qjl_bpv = self.qjl_bytes_per_vec;

        // Precompute query state: M × S_m*y (O(M*d²)) + q_rotated (O(d log d))
        let query_state = prepare_query_prod(
            query,
            &self.collection_meta.qjl_matrices,
            self.collection_meta.fwht_sign_flips.as_slice(),
            padded,
        );

        let tq_buf = self.vectors_tq.as_slice();
        let single_qjl_bpv = (dim + 7) / 8;

        for result in candidates.iter_mut() {
            let bfs_pos = self.graph.to_bfs(result.id.0) as usize;
            let tq_offset = bfs_pos * bytes_per_code;
            let tq_code = &tq_buf[tq_offset..tq_offset + code_len];
            let norm_bytes = &tq_buf[tq_offset + code_len..tq_offset + bytes_per_code];
            let norm =
                f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);

            let qjl_offset = bfs_pos * qjl_bpv;
            let qjl_signs = &self.qjl_signs[qjl_offset..qjl_offset + qjl_bpv];
            let residual_norm = self.residual_norms[bfs_pos];

            result.distance = score_l2_prod(
                &query_state,
                tq_code,
                norm,
                qjl_signs,
                residual_norm,
                centroids,
                dim,
                single_qjl_bpv,
            );
        }
        candidates.sort_unstable();
    }

    /// Access the HNSW graph.
    pub fn graph(&self) -> &HnswGraph {
        &self.graph
    }

    /// Access the TQ code buffer.
    pub fn vectors_tq(&self) -> &AlignedBuffer<u8> {
        &self.vectors_tq
    }

    // vectors_sq and vectors_f32 removed — TurboQuant_prod used for reranking.

    /// Access MVCC headers.
    pub fn mvcc_headers(&self) -> &[MvccHeader] {
        &self.mvcc
    }

    /// Decode the TQ code at the given internal id back to an approximate f32 vector.
    ///
    /// Used for segment merging: existing immutable segments are decoded, then re-encoded
    /// in a single fresh segment to consolidate many small HNSW graphs into one big graph.
    /// This is lossy (TQ4 reconstruction error) but acceptable when the alternative is
    /// searching N segments at N× the cost.
    ///
    /// Returns the decoded f32 vector (length = original dimension).
    pub fn decode_vector(&self, internal_id: u32) -> Vec<f32> {
        let bfs_pos = self.graph.to_bfs(internal_id) as usize;
        let bytes_per_code = self.graph.bytes_per_code() as usize;
        let code_len = bytes_per_code - 4;
        let offset = bfs_pos * bytes_per_code;
        let code_bytes = self.vectors_tq.as_slice()[offset..offset + code_len].to_vec();
        let norm_bytes = &self.vectors_tq.as_slice()[offset + code_len..offset + bytes_per_code];
        let norm = f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);

        let tq_code = crate::vector::turbo_quant::encoder::TqCode {
            codes: code_bytes,
            norm,
        };
        let dim = self.collection_meta.dimension as usize;
        let padded = self.collection_meta.padded_dimension as usize;
        let centroids = self.collection_meta.codebook_16();
        let sign_flips = self.collection_meta.fwht_sign_flips.as_slice();
        let mut work_buf = vec![0.0f32; padded];
        crate::vector::turbo_quant::encoder::decode_tq_mse_scaled(
            &tq_code,
            sign_flips,
            centroids,
            dim,
            &mut work_buf,
        )
    }

    /// Iterate live (non-tombstoned) entries as `(key_hash, decoded_f32)` tuples.
    /// Skips entries marked deleted in MVCC headers.
    pub fn iter_live_decoded(&self) -> impl Iterator<Item = (u64, Vec<f32>)> + '_ {
        self.mvcc.iter().enumerate().filter_map(move |(idx, hdr)| {
            if hdr.delete_lsn != 0 {
                None
            } else {
                Some((hdr.key_hash, self.decode_vector(idx as u32)))
            }
        })
    }

    /// Map a BFS-reordered position to the globally unique key_hash.
    /// Used for building search results that are comparable across segments.
    #[inline]
    pub fn key_hash_for_bfs_pos(&self, bfs_pos: u32) -> u64 {
        self.mvcc[bfs_pos as usize].key_hash
    }

    /// Access collection metadata.
    pub fn collection_meta(&self) -> &Arc<CollectionMetadata> {
        &self.collection_meta
    }

    /// Number of live (non-deleted) entries.
    pub fn live_count(&self) -> u32 {
        self.live_count
    }

    /// Total entries (including deleted).
    pub fn total_count(&self) -> u32 {
        self.total_count
    }

    /// Fraction of dead entries: (total - live) / total.
    pub fn dead_fraction(&self) -> f32 {
        if self.total_count == 0 {
            0.0
        } else {
            (self.total_count - self.live_count) as f32 / self.total_count as f32
        }
    }

    /// Timestamp when this segment was created (compaction time).
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Segment age in seconds since creation.
    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Serialize MVCC headers to raw bytes for warm tier .mpf writing.
    ///
    /// Each entry: internal_id(u32 LE) + global_id(u32 LE) + key_hash(u64 LE) +
    /// insert_lsn(u64 LE) + delete_lsn(u64 LE) = 32 bytes.
    pub fn mvcc_raw_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.mvcc.len() * 32);
        for h in &self.mvcc {
            buf.extend_from_slice(&h.internal_id.to_le_bytes());
            buf.extend_from_slice(&h.global_id.to_le_bytes());
            buf.extend_from_slice(&h.key_hash.to_le_bytes());
            buf.extend_from_slice(&h.insert_lsn.to_le_bytes());
            buf.extend_from_slice(&h.delete_lsn.to_le_bytes());
        }
        buf
    }

    /// Set the CLOG hint-committed bit for an entry, avoiding future CLOG lookups.
    ///
    /// Called after a successful CLOG lookup confirms Committed status.
    pub fn set_hint_committed(&mut self, internal_id: u32) {
        if let Some(h) = self.mvcc.get_mut(internal_id as usize) {
            if h.hint_committed == 0 {
                h.hint_committed = 1;
            }
        }
    }

    /// Check if the CLOG hint-committed bit is set for an entry.
    #[inline]
    pub fn is_hint_committed(&self, internal_id: u32) -> bool {
        self.mvcc
            .get(internal_id as usize)
            .map_or(false, |h| h.hint_committed != 0)
    }

    /// Serialize MVCC headers to raw bytes (v2 format, includes hint_committed).
    ///
    /// Each entry: internal_id(u32 LE) + global_id(u32 LE) + key_hash(u64 LE) +
    /// insert_lsn(u64 LE) + delete_lsn(u64 LE) + hint_committed(u8) = 33 bytes.
    pub fn mvcc_raw_bytes_v2(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.mvcc.len() * 33);
        for h in &self.mvcc {
            buf.extend_from_slice(&h.internal_id.to_le_bytes());
            buf.extend_from_slice(&h.global_id.to_le_bytes());
            buf.extend_from_slice(&h.key_hash.to_le_bytes());
            buf.extend_from_slice(&h.insert_lsn.to_le_bytes());
            buf.extend_from_slice(&h.delete_lsn.to_le_bytes());
            buf.push(h.hint_committed);
        }
        buf
    }

    /// Flat TQ-ADC scan: brute-force over all 4-bit codes. 100% recall.
    ///
    /// Skips HNSW entirely — sequential scan of nibble-packed TQ codes.
    /// Ideal for N < 100K where the codes fit in L2/L3 cache (~256 bytes/vec at 512d).
    ///
    /// Cost: O(N × padded_dim) with 8x compression vs f32.
    /// At 30K/512d on M4 Pro: ~4ms per query, 100% recall.
    pub fn flat_scan(&self, query: &[f32], k: usize) -> SmallVec<[SearchResult; 32]> {
        use crate::vector::turbo_quant::fwht;
        use crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;
        use std::collections::BinaryHeap;

        let n = self.total_count as usize;
        if n == 0 || k == 0 {
            return SmallVec::new();
        }

        let dim = self.collection_meta.dimension as usize;
        let padded = self.collection_meta.padded_dimension as usize;
        let centroids = self.collection_meta.codebook_16();
        let bytes_per_code = self.graph.bytes_per_code() as usize;
        let code_len = bytes_per_code - 4; // nibble-packed codes without norm

        // Prepare FWHT-rotated query (same as TQ-ADC)
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(query);
        let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rotated[..dim].iter_mut() {
                *v *= inv;
            }
        }
        fwht::fwht(
            &mut q_rotated,
            self.collection_meta.fwht_sign_flips.as_slice(),
        );

        // Brute-force scan with max-heap for top-K.
        // TQ codes are in BFS order — use graph.to_original(bfs_pos) for original ID.
        let tq_buf = self.vectors_tq.as_slice();
        let mut heap: BinaryHeap<(ordered_float::OrderedFloat<f32>, u32)> = BinaryHeap::new();

        for bfs_pos in 0..n {
            let offset = bfs_pos * bytes_per_code;
            let code = &tq_buf[offset..offset + code_len];
            let norm_bytes = &tq_buf[offset + code_len..offset + bytes_per_code];
            let norm =
                f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);

            // Map BFS position → original ID (same mapping HNSW search uses)
            let original_id = self.graph.to_original(bfs_pos as u32);

            let dist = tq_l2_adc_scaled(&q_rotated, code, norm, centroids);

            if heap.len() < k {
                heap.push((ordered_float::OrderedFloat(dist), original_id));
            } else if let Some(&(worst, _)) = heap.peek() {
                if dist < worst.0 {
                    heap.pop();
                    heap.push((ordered_float::OrderedFloat(dist), original_id));
                }
            }
        }

        let mut results: Vec<_> = heap.into_iter().collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
            .into_iter()
            .map(|(d, id)| SearchResult::new(d.0, crate::vector::types::VectorId(id)))
            .collect()
    }

    /// Mark an entry as deleted by setting its MVCC delete_lsn.
    pub fn mark_deleted(&mut self, internal_id: u32, delete_lsn: u64) {
        if let Some(h) = self.mvcc.get_mut(internal_id as usize) {
            if h.delete_lsn == 0 {
                h.delete_lsn = delete_lsn;
                self.live_count = self.live_count.saturating_sub(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::aligned_buffer::AlignedBuffer;
    use crate::vector::distance;
    use crate::vector::turbo_quant::collection::QuantizationConfig;
    use crate::vector::types::DistanceMetric;

    #[test]
    fn test_immutable_segment_has_created_at() {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let empty_graph = HnswGraph::new(
            0, 16, 32, 0, 0,
            AlignedBuffer::new(0), Vec::new(), Vec::new(), Vec::new(), Vec::new(), 68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));

        let seg = ImmutableSegment::new(
            graph, AlignedBuffer::new(0), Vec::new(), Vec::new(), 16,
            Vec::new(), 16, Vec::new(), collection, 0, 0,
        );
        // created_at should be very recent
        assert!(seg.age_secs() < 2);
        // created_at() should be accessible
        let _t = seg.created_at();
    }

    #[test]
    fn test_mvcc_raw_bytes_roundtrip() {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1, 128, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        ));
        let empty_graph = HnswGraph::new(
            0, 16, 32, 0, 0,
            AlignedBuffer::new(0), Vec::new(), Vec::new(), Vec::new(), Vec::new(), 68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));

        let mvcc = vec![
            MvccHeader { internal_id: 0, global_id: 10, key_hash: 0xDEAD, insert_lsn: 1, delete_lsn: 0, hint_committed: 0 },
            MvccHeader { internal_id: 1, global_id: 11, key_hash: 0xBEEF, insert_lsn: 2, delete_lsn: 5, hint_committed: 0 },
        ];
        let seg = ImmutableSegment::new(
            graph, AlignedBuffer::new(0), Vec::new(), Vec::new(), 16,
            Vec::new(), 16, mvcc, collection, 2, 2,
        );

        let raw = seg.mvcc_raw_bytes();
        // 2 entries * 32 bytes each = 64 bytes
        assert_eq!(raw.len(), 64);

        // Verify first entry
        let id0 = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        assert_eq!(id0, 0);
        let gid0 = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
        assert_eq!(gid0, 10);
        let kh0 = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(kh0, 0xDEAD);
    }

    #[test]
    fn test_immutable_segment_created() {
        distance::init();
        // Basic smoke test — just verify construction doesn't panic
        let collection = Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        // Build an empty graph: 0 nodes, serialize then deserialize
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68, // bytes_per_code = 128/2 + 4
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));

        let _seg = ImmutableSegment::new(
            graph,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            16, // 128/8 = qjl_bytes_per_vec
            Vec::new(),
            16, // 128/8 = sub_sign_bytes_per_vec
            Vec::new(),
            collection,
            0,
            0,
        );
    }

    #[test]
    fn test_hint_committed_default_zero() {
        let h = MvccHeader {
            internal_id: 0,
            global_id: 0,
            key_hash: 0,
            insert_lsn: 1,
            delete_lsn: 0,
            hint_committed: 0,
        };
        assert_eq!(h.hint_committed, 0);
    }

    #[test]
    fn test_set_hint_committed() {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1, 128, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        ));
        let empty_graph = HnswGraph::new(
            0, 16, 32, 0, 0,
            AlignedBuffer::new(0), Vec::new(), Vec::new(), Vec::new(), Vec::new(), 68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));

        let mvcc = vec![
            MvccHeader { internal_id: 0, global_id: 0, key_hash: 0, insert_lsn: 1, delete_lsn: 0, hint_committed: 0 },
            MvccHeader { internal_id: 1, global_id: 1, key_hash: 0, insert_lsn: 2, delete_lsn: 0, hint_committed: 0 },
        ];
        let mut seg = ImmutableSegment::new(
            graph, AlignedBuffer::new(0), Vec::new(), Vec::new(), 16,
            Vec::new(), 16, mvcc, collection, 2, 2,
        );

        // Neither should be hint-committed initially
        assert!(!seg.is_hint_committed(0));
        assert!(!seg.is_hint_committed(1));

        // Set hint on entry 0
        seg.set_hint_committed(0);
        assert!(seg.is_hint_committed(0));
        assert!(!seg.is_hint_committed(1));

        // Out-of-bounds should return false
        assert!(!seg.is_hint_committed(99));
    }

    #[test]
    fn test_mvcc_raw_bytes_v2_includes_hint() {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1, 128, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        ));
        let empty_graph = HnswGraph::new(
            0, 16, 32, 0, 0,
            AlignedBuffer::new(0), Vec::new(), Vec::new(), Vec::new(), Vec::new(), 68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));

        let mvcc = vec![
            MvccHeader { internal_id: 0, global_id: 10, key_hash: 0xAA, insert_lsn: 1, delete_lsn: 0, hint_committed: 1 },
        ];
        let seg = ImmutableSegment::new(
            graph, AlignedBuffer::new(0), Vec::new(), Vec::new(), 16,
            Vec::new(), 16, mvcc, collection, 1, 1,
        );

        let v1 = seg.mvcc_raw_bytes();
        assert_eq!(v1.len(), 32); // v1 format unchanged

        let v2 = seg.mvcc_raw_bytes_v2();
        assert_eq!(v2.len(), 33); // v2 format includes hint byte
        assert_eq!(v2[32], 1);    // hint_committed byte
    }
}
