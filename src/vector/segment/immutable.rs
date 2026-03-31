//! Read-only segment with HNSW graph and TurboQuant codes.
//!
//! Truly immutable after construction -- no locks needed for search.

use std::sync::Arc;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::hnsw::search::{SearchScratch, hnsw_search, hnsw_search_filtered};
#[allow(unused_imports)]
use crate::vector::hnsw::search_sq::hnsw_search_f32;
use crate::vector::turbo_quant::collection::CollectionMetadata;
use crate::vector::turbo_quant::inner_product::{prepare_query_prod, score_l2_prod};
use crate::vector::types::SearchResult;

/// MVCC header for immutable segment entries.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MvccHeader {
    pub internal_id: u32,
    pub insert_lsn: u64,
    pub delete_lsn: u64,
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
    mvcc: Vec<MvccHeader>,
    collection_meta: Arc<CollectionMetadata>,
    live_count: u32,
    total_count: u32,
}

impl ImmutableSegment {
    /// Construct from compaction output.
    pub fn new(
        graph: HnswGraph,
        vectors_tq: AlignedBuffer<u8>,
        qjl_signs: Vec<u8>,
        residual_norms: Vec<f32>,
        qjl_bytes_per_vec: usize,
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
            mvcc,
            collection_meta,
            live_count,
            total_count,
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
        let mut candidates = hnsw_search(
            &self.graph,
            self.vectors_tq.as_slice(),
            query,
            &self.collection_meta,
            ef_search,
            ef_search,
            scratch,
        );

        self.rerank_with_prod(&mut candidates, query);
        candidates.truncate(k);
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
        let mut candidates = hnsw_search_filtered(
            &self.graph,
            self.vectors_tq.as_slice(),
            query,
            &self.collection_meta,
            ef_search,
            ef_search,
            scratch,
            allow_bitmap,
        );

        self.rerank_with_prod(&mut candidates, query);
        candidates.truncate(k);
        candidates
    }

    /// Rerank candidates using TurboQuant_prod unbiased inner product estimator.
    ///
    /// For each candidate: compute L2 distance via
    ///   ||q - x||² = ||q||² + ||x||² - 2 * (<q, x_mse> + QJL_correction)
    ///
    /// Term 1 (<q, x_mse>) computed in rotated space: O(padded_dim).
    /// Term 2 (QJL correction) uses precomputed S*y: O(dim).
    /// Total per candidate: O(padded_dim) — same cost as TQ-ADC.
    fn rerank_with_prod(
        &self,
        candidates: &mut SmallVec<[SearchResult; 32]>,
        query: &[f32],
    ) {
        if candidates.is_empty() || self.qjl_signs.is_empty() {
            return;
        }

        let dim = self.collection_meta.dimension as usize;
        let padded = self.collection_meta.padded_dimension as usize;
        let centroids = self.collection_meta.codebook_16();
        let bytes_per_code = self.graph.bytes_per_code() as usize;
        let code_len = bytes_per_code - 4;
        let qjl_bpv = self.qjl_bytes_per_vec;

        // Precompute query state: S*y (O(d²)) + q_rotated (O(d log d))
        let qjl_matrix = self.collection_meta.qjl_matrix.as_deref().unwrap();
        let query_state = prepare_query_prod(
            query,
            qjl_matrix,
            self.collection_meta.fwht_sign_flips.as_slice(),
            padded,
        );

        let tq_buf = self.vectors_tq.as_slice();

        for result in candidates.iter_mut() {
            let bfs_pos = self.graph.to_bfs(result.id.0) as usize;
            let tq_offset = bfs_pos * bytes_per_code;
            let tq_code = &tq_buf[tq_offset..tq_offset + code_len];
            let norm_bytes = &tq_buf[tq_offset + code_len..tq_offset + bytes_per_code];
            let norm = f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);

            let qjl_offset = bfs_pos * qjl_bpv;
            let qjl_signs = &self.qjl_signs[qjl_offset..qjl_offset + qjl_bpv];
            let residual_norm = self.residual_norms[bfs_pos];

            result.distance = score_l2_prod(
                &query_state, tq_code, norm, qjl_signs, residual_norm, centroids, dim,
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
    fn test_immutable_segment_created() {
        distance::init();
        // Basic smoke test — just verify construction doesn't panic
        let collection = Arc::new(CollectionMetadata::new(
            1, 128, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        ));
        // Build an empty graph: 0 nodes, serialize then deserialize
        let empty_graph = HnswGraph::new(
            0, 16, 32, 0, 0,
            AlignedBuffer::new(0),
            Vec::new(), Vec::new(),
            Vec::new(), Vec::new(), 68, // bytes_per_code = 128/2 + 4
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
            collection,
            0,
            0,
        );
    }
}
