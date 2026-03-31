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
/// Two-stage search: HNSW beam search with TQ-ADC (fast, 8x compressed),
/// then rerank top candidates with exact f32 L2 for high recall.
/// SQ8 vectors are dropped (not needed).
pub struct ImmutableSegment {
    graph: HnswGraph,
    vectors_tq: AlignedBuffer<u8>,
    vectors_f32: AlignedBuffer<f32>,
    mvcc: Vec<MvccHeader>,
    collection_meta: Arc<CollectionMetadata>,
    live_count: u32,
    total_count: u32,
}

impl ImmutableSegment {
    /// Construct from compaction output.
    ///
    /// SQ8 vectors are dropped (not needed). f32 kept for reranking.
    pub fn new(
        graph: HnswGraph,
        vectors_tq: AlignedBuffer<u8>,
        _vectors_sq: AlignedBuffer<i8>,
        vectors_f32: AlignedBuffer<f32>,
        mvcc: Vec<MvccHeader>,
        collection_meta: Arc<CollectionMetadata>,
        live_count: u32,
        total_count: u32,
    ) -> Self {
        Self {
            graph,
            vectors_tq,
            vectors_f32,
            mvcc,
            collection_meta,
            live_count,
            total_count,
        }
    }

    /// Two-stage HNSW search: TQ-ADC beam search + f32 reranking.
    ///
    /// Stage 1: HNSW beam search with TQ-ADC distance (4-bit quantized).
    ///   Returns `ef_search` candidates — fast but approximate distances.
    /// Stage 2: Rerank candidates with exact f32 L2 distance.
    ///   Returns top-k with exact ordering — high recall.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
    ) -> SmallVec<[SearchResult; 32]> {
        // Stage 1: TQ-ADC HNSW beam search (returns ef candidates)
        let mut candidates = hnsw_search(
            &self.graph,
            self.vectors_tq.as_slice(),
            query,
            &self.collection_meta,
            ef_search, // fetch ef candidates, not just k
            ef_search,
            scratch,
        );

        // Stage 2: Rerank with exact f32 L2 distance
        if !self.vectors_f32.as_slice().is_empty() {
            let dim = self.collection_meta.dimension as usize;
            let l2_f32 = crate::vector::distance::table().l2_f32;

            for result in candidates.iter_mut() {
                let bfs_pos = self.graph.to_bfs(result.id.0);
                let offset = bfs_pos as usize * dim;
                let vec_f32 = &self.vectors_f32.as_slice()[offset..offset + dim];
                result.distance = l2_f32(query, vec_f32);
            }
            candidates.sort_unstable();
        }

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

        if !self.vectors_f32.as_slice().is_empty() {
            let dim = self.collection_meta.dimension as usize;
            let l2_f32 = crate::vector::distance::table().l2_f32;

            for result in candidates.iter_mut() {
                let bfs_pos = self.graph.to_bfs(result.id.0);
                let offset = bfs_pos as usize * dim;
                let vec_f32 = &self.vectors_f32.as_slice()[offset..offset + dim];
                result.distance = l2_f32(query, vec_f32);
            }
            candidates.sort_unstable();
        }

        candidates.truncate(k);
        candidates
    }

    /// Access the HNSW graph.
    pub fn graph(&self) -> &HnswGraph {
        &self.graph
    }

    /// Access the TQ code buffer.
    pub fn vectors_tq(&self) -> &AlignedBuffer<u8> {
        &self.vectors_tq
    }

    // vectors_sq and vectors_f32 removed — TQ-ADC is used for search.
    // This saves ~5x memory per vector (3072 + 768 bytes/vec at dim=768).

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

    /// Total entries including deleted.
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

    /// Brute-force TQ-ADC scan over all vectors in this segment.
    ///
    /// Used for small segments, IVF posting lists, or when exhaustive search
    /// is preferred over approximate HNSW traversal. Vector IDs in results
    /// are original IDs (not BFS positions).
    pub fn brute_force_search(
        &self,
        query: &[f32],
        k: usize,
    ) -> SmallVec<[SearchResult; 32]> {
        use crate::vector::turbo_quant::tq_adc::brute_force_tq_adc;
        use crate::vector::types::VectorId;
        let mut results = brute_force_tq_adc(
            query,
            self.vectors_tq.as_slice(),
            self.total_count as usize,
            &self.collection_meta,
            k,
        );
        // Map BFS positions back to original IDs
        for r in results.iter_mut() {
            r.id = VectorId(self.graph.to_original(r.id.0));
        }
        results
    }

    /// Mark an entry as deleted. Only called during vacuum rebuild setup.
    pub fn mark_deleted(&mut self, internal_id: u32, delete_lsn: u64) {
        if let Some(header) = self.mvcc.get_mut(internal_id as usize) {
            if header.delete_lsn == 0 {
                header.delete_lsn = delete_lsn;
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
    use crate::vector::hnsw::build::HnswBuilder;
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::turbo_quant::encoder::{encode_tq_mse_scaled, padded_dimension};
    use crate::vector::turbo_quant::fwht;
    use crate::vector::types::DistanceMetric;

    fn lcg_f32(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn normalize(v: &mut [f32]) -> f32 {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            v.iter_mut().for_each(|x| *x *= inv);
        }
        norm
    }

    fn build_immutable_segment(
        n: usize,
        dim: usize,
    ) -> (ImmutableSegment, Vec<Vec<f32>>) {
        distance::init();

        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let padded = collection.padded_dimension as usize;
        let signs = collection.fwht_sign_flips.as_slice();
        let bytes_per_code = padded / 2 + 4;

        let mut vectors = Vec::with_capacity(n);
        let mut codes = Vec::new();
        let mut sq_vectors: Vec<i8> = Vec::new();
        let mut work = vec![0.0f32; padded];

        for i in 0..n {
            let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut v);
            let boundaries = collection.codebook_boundaries_15();
            let code = encode_tq_mse_scaled(&v, signs, boundaries, &mut work);
            // SQ: simple scalar quantization to i8
            for &val in &v {
                sq_vectors.push((val * 127.0).clamp(-128.0, 127.0) as i8);
            }
            codes.push(code);
            vectors.push(v);
        }

        let dist_table = distance::table();

        // Build flat TQ buffer in insertion order
        let mut tq_buffer_orig: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
        for code in &codes {
            tq_buffer_orig.extend_from_slice(&code.codes);
            tq_buffer_orig.extend_from_slice(&code.norm.to_le_bytes());
        }

        // Precompute rotated queries for pairwise oracle
        let mut all_rotated: Vec<Vec<f32>> = Vec::with_capacity(n);
        let mut q_rot_buf = vec![0.0f32; padded];
        for i in 0..n {
            q_rot_buf[..dim].copy_from_slice(&vectors[i]);
            for v in q_rot_buf[dim..padded].iter_mut() {
                *v = 0.0;
            }
            fwht::fwht(&mut q_rot_buf[..padded], signs);
            all_rotated.push(q_rot_buf[..padded].to_vec());
        }

        let codebook = collection.codebook_16();
        let mut builder = HnswBuilder::new(16, 200, 12345);
        for _i in 0..n {
            builder.insert(|a: u32, b: u32| {
                let q_rot = &all_rotated[a as usize];
                let offset = b as usize * bytes_per_code;
                let code_slice = &tq_buffer_orig[offset..offset + bytes_per_code - 4];
                let norm_bytes =
                    &tq_buffer_orig[offset + bytes_per_code - 4..offset + bytes_per_code];
                let norm = f32::from_le_bytes([
                    norm_bytes[0],
                    norm_bytes[1],
                    norm_bytes[2],
                    norm_bytes[3],
                ]);
                (dist_table.tq_l2)(q_rot, code_slice, norm, codebook)
            });
        }

        let graph = builder.build(bytes_per_code as u32);

        // Rearrange TQ buffer into BFS order
        let mut tq_buffer_bfs = vec![0u8; n * bytes_per_code];
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let src = orig_id * bytes_per_code;
            let dst = bfs_pos * bytes_per_code;
            tq_buffer_bfs[dst..dst + bytes_per_code]
                .copy_from_slice(&tq_buffer_orig[src..src + bytes_per_code]);
        }

        // BFS reorder f32 vectors for HNSW search
        let mut f32_bfs = vec![0.0f32; n * dim];
        for orig in 0..n {
            let bfs = graph.to_bfs(orig as u32) as usize;
            f32_bfs[bfs * dim..(bfs + 1) * dim].copy_from_slice(&vectors[orig]);
        }

        let mvcc: Vec<MvccHeader> = (0..n as u32)
            .map(|i| MvccHeader {
                internal_id: i,
                insert_lsn: i as u64 + 1,
                delete_lsn: 0,
            })
            .collect();

        let segment = ImmutableSegment::new(
            graph,
            AlignedBuffer::from_vec(tq_buffer_bfs),
            AlignedBuffer::from_vec(sq_vectors),
            AlignedBuffer::from_vec(f32_bfs),
            mvcc,
            collection.clone(),
            n as u32,
            n as u32,
        );

        (segment, vectors)
    }

    #[test]
    fn test_immutable_search_returns_results() {
        let (segment, vectors) = build_immutable_segment(50, 64);
        let padded = segment.collection_meta().padded_dimension;
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(
            segment.graph().num_nodes(), padded,
        );
        let results = segment.search(&vectors[0], 5, 64, &mut scratch);
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_immutable_live_count() {
        let (segment, _) = build_immutable_segment(50, 64);
        assert_eq!(segment.live_count(), 50);
        assert_eq!(segment.total_count(), 50);
    }

    #[test]
    fn test_immutable_dead_fraction_zero() {
        let (segment, _) = build_immutable_segment(50, 64);
        assert_eq!(segment.dead_fraction(), 0.0);
    }

    #[test]
    fn test_immutable_dead_fraction_after_delete() {
        let (mut segment, _) = build_immutable_segment(10, 64);
        segment.mark_deleted(0, 100);
        segment.mark_deleted(1, 101);
        assert_eq!(segment.live_count(), 8);
        assert_eq!(segment.total_count(), 10);
        let frac = segment.dead_fraction();
        assert!((frac - 0.2).abs() < 1e-6);
    }

    #[test]
    fn test_immutable_dead_fraction_empty() {
        // Edge case: zero-count segment
        let graph = HnswBuilder::new(16, 200, 42)
            .build((padded_dimension(64) / 2 + 4) as u32);
        let collection = Arc::new(CollectionMetadata::new(
            1,
            64,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let segment = ImmutableSegment::new(
            graph,
            AlignedBuffer::new(0),
            AlignedBuffer::new(0),
            AlignedBuffer::new(0),
            Vec::new(),
            collection,
            0,
            0,
        );
        assert_eq!(segment.dead_fraction(), 0.0);
    }

    #[test]
    fn test_immutable_mark_deleted_idempotent() {
        let (mut segment, _) = build_immutable_segment(10, 64);
        segment.mark_deleted(0, 100);
        assert_eq!(segment.live_count(), 9);
        // Second delete of same entry should not decrement further
        segment.mark_deleted(0, 200);
        assert_eq!(segment.live_count(), 9);
    }
}
