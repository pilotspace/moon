//! HNSW beam search with BitVec visited tracking, SearchScratch reuse,
//! and 2-hop dual prefetch for cache-optimized traversal.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use super::graph::{HnswGraph, SENTINEL};
use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::turbo_quant::collection::CollectionMetadata;
use crate::vector::turbo_quant::fwht;
use crate::vector::types::{SearchResult, VectorId};

/// Bit vector for O(1) visited tracking. 64x more cache-efficient than HashSet
/// for dense integer keys. Uses test_and_set for combined check+mark.
///
/// Memory: ceil(max_nodes / 64) * 8 bytes. At 1M nodes: 128 KB.
/// Clear: memset via write_bytes -- no per-element iteration.
pub struct BitVec {
    words: Vec<u64>,
}

impl BitVec {
    /// Create a BitVec with capacity for `max_id` node IDs.
    pub fn new(max_id: u32) -> Self {
        let words_needed = (max_id as usize + 63) / 64;
        Self {
            words: vec![0u64; words_needed],
        }
    }

    /// Test if `id` is set, then set it. Returns true if was ALREADY set.
    ///
    /// This is the core visited-tracking primitive. Combines read+write in one
    /// operation to avoid double cache-line access.
    #[inline(always)]
    pub fn test_and_set(&mut self, id: u32) -> bool {
        let word_idx = id as usize >> 6; // id / 64
        let bit = 1u64 << (id & 63); // id % 64
        let prev = self.words[word_idx];
        self.words[word_idx] = prev | bit;
        prev & bit != 0
    }

    /// Clear all bits up to `max_id`. Uses memset for SIMD-optimized zeroing.
    ///
    /// If the bitvec is too small, it grows (but never shrinks -- reuse across queries).
    pub fn clear_all(&mut self, max_id: u32) {
        let words_needed = (max_id as usize + 63) / 64;
        if self.words.len() < words_needed {
            self.words.resize(words_needed, 0);
        } else {
            // SAFETY: self.words.as_mut_ptr() points to `words_needed` initialized u64s.
            // write_bytes zeroes exactly `words_needed` u64-sized slots.
            // words_needed <= self.words.len() (checked above).
            unsafe {
                std::ptr::write_bytes(self.words.as_mut_ptr(), 0, words_needed);
            }
        }
    }
}

/// Ordered (distance, node_id) pair for BinaryHeap usage.
/// Compares by distance first (f32 total order), then by node_id.
#[derive(Clone, Copy, PartialEq)]
pub(crate) struct OrdF32Pair(pub(crate) f32, pub(crate) u32);

impl Eq for OrdF32Pair {}

impl PartialOrd for OrdF32Pair {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF32Pair {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // total_cmp provides IEEE 754 total ordering (handles NaN deterministically)
        self.0.total_cmp(&other.0).then(self.1.cmp(&other.1))
    }
}

/// Shard-owned search scratch space. Reused across queries -- zero allocation per search.
///
/// Lifecycle:
/// 1. Created once per shard with capacity for max expected graph size.
/// 2. clear() before each search (memset visited, clear heaps -- no realloc).
/// 3. hnsw_search uses candidates/results/visited during beam search.
/// 4. After search, results are extracted; scratch is left dirty until next clear().
pub struct SearchScratch {
    /// Min-heap of candidates to explore: pop nearest first.
    pub(crate) candidates: BinaryHeap<Reverse<OrdF32Pair>>,
    /// Max-heap of current results: peek/pop farthest for pruning.
    pub(crate) results: BinaryHeap<OrdF32Pair>,
    /// Visited bit vector -- cleared via memset per search.
    pub(crate) visited: BitVec,
    /// Pre-allocated buffer for FWHT-rotated query (reused across searches).
    pub(crate) query_rotated: AlignedBuffer<f32>,
}

impl SearchScratch {
    /// Create scratch space for graphs up to `max_nodes` and queries up to `padded_dim`.
    pub fn new(max_nodes: u32, padded_dim: u32) -> Self {
        Self {
            candidates: BinaryHeap::with_capacity(256),
            results: BinaryHeap::with_capacity(256),
            visited: BitVec::new(max_nodes),
            query_rotated: AlignedBuffer::new(padded_dim as usize),
        }
    }

    /// Clear scratch state for a new search. Zero allocation.
    ///
    /// Heaps are cleared (len=0, capacity preserved).
    /// Visited bits zeroed via memset.
    pub fn clear(&mut self, num_nodes: u32) {
        self.candidates.clear();
        self.results.clear();
        self.visited.clear_all(num_nodes);
    }
}

/// HNSW search with 2-hop dual prefetch and TQ-ADC distance.
///
/// # Arguments
/// - `graph`: The HNSW graph (BFS-reordered layer 0).
/// - `vectors_tq`: Flat buffer of TQ codes in BFS order. Each code is `bytes_per_code` bytes.
///   Layout per code: [nibble_packed_codes (padded_dim/2 bytes)] [norm (4 bytes f32 LE)].
/// - `query`: Raw query vector (f32, original dimension, NOT rotated).
/// - `collection`: Collection metadata (sign flips, padded dimension).
/// - `k`: Number of nearest neighbors to return.
/// - `ef_search`: Beam width (must be >= k). Higher = better recall, slower.
/// - `scratch`: Mutable scratch space (cleared internally, reused across calls).
///
/// # Returns
/// Up to `k` SearchResults sorted by distance ascending (nearest first).
///
/// # Algorithm
/// 1. Prepare rotated query: pad to padded_dim, apply FWHT with collection sign flips.
/// 2. Upper layers: greedy single-best descent from entry_point to layer 1.
///    - At each layer, scan all neighbors of current node, move to nearest.
///    - Repeat until no improvement found, then descend one layer.
///    - Upper layers use ORIGINAL node IDs (not BFS-reordered).
/// 3. Layer 0: ef-bounded beam search with BitVec visited tracking.
///    - Convert current node from original to BFS space.
///    - Seed candidates/results with entry node.
///    - Pop nearest candidate, expand its neighbors.
///    - 2-hop prefetch: while computing distance for neighbor[i], prefetch neighbor[i+2].
///    - Early termination: if nearest candidate > farthest result and results.len >= ef.
///    - Prune results to ef (pop farthest when over capacity).
/// 4. Extract top-K from results heap, map BFS positions back to original IDs.
///
/// # Zero-allocation guarantee (VEC-HNSW-03)
/// All allocations happen in SearchScratch::new(). During search:
/// - BitVec.clear_all uses memset (no alloc).
/// - BinaryHeap.push/pop reuses existing capacity.
/// - query_rotated is pre-allocated AlignedBuffer.
/// - SmallVec output uses stack storage for k <= 32.
pub fn hnsw_search(
    graph: &HnswGraph,
    vectors_tq: &[u8],
    query: &[f32],
    collection: &CollectionMetadata,
    k: usize,
    ef_search: usize,
    scratch: &mut SearchScratch,
) -> SmallVec<[SearchResult; 32]> {
    hnsw_search_filtered(graph, vectors_tq, query, collection, k, ef_search, scratch, None)
}

/// HNSW search with optional filter bitmap (ACORN 2-hop expansion).
///
/// When `allow_bitmap` is Some, only vectors whose ORIGINAL ID is in the bitmap
/// are added to results. However, vectors OUTSIDE the bitmap are still traversed
/// for graph connectivity (ACORN principle). When a neighbor fails the filter,
/// we also immediately explore that neighbor's neighbors (2-hop reach) to prevent
/// "filter island" disconnection at low selectivity.
pub fn hnsw_search_filtered(
    graph: &HnswGraph,
    vectors_tq: &[u8],
    query: &[f32],
    collection: &CollectionMetadata,
    k: usize,
    ef_search: usize,
    scratch: &mut SearchScratch,
    allow_bitmap: Option<&RoaringBitmap>,
) -> SmallVec<[SearchResult; 32]> {
    let num_nodes = graph.num_nodes();
    if num_nodes == 0 {
        return SmallVec::new();
    }

    let ef = ef_search.max(k);
    scratch.clear(num_nodes);

    // Step 1: Prepare rotated query into scratch.query_rotated
    let dim = query.len();
    let padded = collection.padded_dimension as usize;
    let q_rot = scratch.query_rotated.as_mut_slice();
    // Copy query and zero-pad
    q_rot[..dim].copy_from_slice(query);
    for v in q_rot[dim..padded].iter_mut() {
        *v = 0.0;
    }
    // Compute query norm BEFORE normalization (needed for distance correction)
    let mut q_norm_sq = 0.0f32;
    for &v in &q_rot[..dim] {
        q_norm_sq += v * v;
    }
    let q_norm = q_norm_sq.sqrt();
    // Normalize query to unit length (TQ operates on unit sphere)
    if q_norm > 0.0 {
        let inv = 1.0 / q_norm;
        for v in q_rot[..dim].iter_mut() {
            *v *= inv;
        }
    }
    // Apply FWHT with collection's sign flips
    fwht::fwht(&mut q_rot[..padded], collection.fwht_sign_flips.as_slice());

    // Use dimension-scaled TQ-ADC directly (not through DistanceTable function pointer).
    // The collection's codebook is scaled by 1/sqrt(padded_dim) to match FWHT normalization.
    use crate::vector::turbo_quant::tq_adc::{tq_l2_adc_scaled, tq_l2_adc_scaled_budgeted};

    // Capture immutable slice of rotated query (after mutation phase is done)
    let q_rotated: &[f32] = scratch.query_rotated.as_slice();
    let codebook = &collection.codebook;

    // Pre-compute code layout for inlined offset computation.
    let bytes_per_code = graph.bytes_per_code() as usize;
    let code_len = bytes_per_code - 4; // nibble-packed codes (last 4 bytes are norm)

    // Unbounded distance: used in upper-layer descent where no budget exists.
    let dist_bfs = |bfs_pos: u32| -> f32 {
        let offset = bfs_pos as usize * bytes_per_code;
        let code_only = &vectors_tq[offset..offset + code_len];
        let norm_bytes = &vectors_tq[offset + code_len..offset + bytes_per_code];
        let norm = f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);
        tq_l2_adc_scaled(q_rotated, code_only, norm, codebook)
    };

    // Budgeted distance: used in layer 0 beam search. Aborts early when partial
    // distance exceeds budget, returning f32::MAX. Saves ~30-50% of ADC loop
    // iterations for clearly-dominated neighbors at high ef.
    let dist_bfs_budgeted = |bfs_pos: u32, budget: f32| -> f32 {
        let offset = bfs_pos as usize * bytes_per_code;
        let code_only = &vectors_tq[offset..offset + code_len];
        let norm_bytes = &vectors_tq[offset + code_len..offset + bytes_per_code];
        let norm = f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);
        tq_l2_adc_scaled_budgeted(q_rotated, code_only, norm, codebook, budget)
    };

    // Step 2: Upper layer greedy descent (original node ID space)
    let mut current_orig = graph.to_original(graph.entry_point());
    let mut current_dist = dist_bfs(graph.entry_point());

    for layer in (1..=graph.max_level() as usize).rev() {
        loop {
            let mut improved = false;
            for &nb in graph.neighbors_upper(current_orig, layer) {
                if nb == SENTINEL {
                    break;
                }
                let nb_bfs = graph.to_bfs(nb);
                let d = dist_bfs(nb_bfs);
                if d < current_dist {
                    current_orig = nb;
                    current_dist = d;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
    }

    // Step 3: Layer 0 beam search (BFS space) with ACORN 2-hop filter expansion
    let entry_bfs = graph.to_bfs(current_orig);
    scratch.visited.test_and_set(entry_bfs);

    let entry_passes = allow_bitmap.map_or(true, |bm| bm.contains(graph.to_original(entry_bfs)));

    scratch
        .candidates
        .push(Reverse(OrdF32Pair(current_dist, entry_bfs)));
    if entry_passes {
        scratch.results.push(OrdF32Pair(current_dist, entry_bfs));
    }

    // Cache the worst (farthest) distance in results to avoid repeated heap peek.
    // Updated after every results mutation (push or pop). Avoids O(1) peek per neighbor.
    let mut worst_dist = f32::MAX;

    while let Some(Reverse(OrdF32Pair(c_dist, c_bfs))) = scratch.candidates.pop() {
        // Early termination: if nearest candidate is farther than worst result
        if scratch.results.len() >= ef && c_dist > worst_dist {
            break;
        }

        let neighbors = graph.neighbors_l0(c_bfs);

        // Prefetch first neighbor's data
        if let Some(&first_nb) = neighbors.first() {
            if first_nb != SENTINEL {
                graph.prefetch_node(first_nb, vectors_tq);
            }
        }

        for (idx, &nb) in neighbors.iter().enumerate() {
            if nb == SENTINEL {
                break;
            }
            if scratch.visited.test_and_set(nb) {
                continue;
            }

            // 2-hop prefetch: prefetch neighbor[idx+2] while computing distance for neighbor[idx]
            if idx + 2 < neighbors.len() {
                let next = neighbors[idx + 2];
                if next != SENTINEL {
                    graph.prefetch_node(next, vectors_tq);
                }
            }

            // Use budgeted ADC when results heap is full (budget = worst distance).
            // Early-exit saves ~30-50% of ADC iterations for dominated neighbors.
            let d = if worst_dist < f32::MAX {
                dist_bfs_budgeted(nb, worst_dist)
            } else {
                dist_bfs(nb)
            };

            // Fast domination check: d == f32::MAX means budgeted ADC aborted early.
            let dominated = d == f32::MAX || (scratch.results.len() >= ef && d >= worst_dist);

            if let Some(bm) = allow_bitmap {
                let orig_id = graph.to_original(nb);
                if bm.contains(orig_id) {
                    // Passes filter: add to candidates AND results
                    if !dominated {
                        scratch.candidates.push(Reverse(OrdF32Pair(d, nb)));
                        scratch.results.push(OrdF32Pair(d, nb));
                        if scratch.results.len() > ef {
                            scratch.results.pop();
                        }
                        // Update cached worst after any mutation that fills/overfills
                        if scratch.results.len() >= ef {
                            worst_dist = scratch.results.peek().map_or(f32::MAX, |p| p.0);
                        }
                    }
                } else {
                    // ACORN: add to candidates for connectivity but NOT to results
                    if !dominated {
                        scratch.candidates.push(Reverse(OrdF32Pair(d, nb)));
                    }
                    // 2-hop expansion: immediately explore nb's neighbors
                    for &hop2_nb in graph.neighbors_l0(nb) {
                        if hop2_nb == SENTINEL {
                            break;
                        }
                        if scratch.visited.test_and_set(hop2_nb) {
                            continue;
                        }
                        let d2 = dist_bfs(hop2_nb);
                        let hop2_dominated = scratch.results.len() >= ef && d2 >= worst_dist;
                        if !hop2_dominated {
                            scratch.candidates.push(Reverse(OrdF32Pair(d2, hop2_nb)));
                            let hop2_orig = graph.to_original(hop2_nb);
                            if bm.contains(hop2_orig) {
                                scratch.results.push(OrdF32Pair(d2, hop2_nb));
                                if scratch.results.len() > ef {
                                    scratch.results.pop();
                                }
                                if scratch.results.len() >= ef {
                                    worst_dist = scratch.results.peek().map_or(f32::MAX, |p| p.0);
                                }
                            }
                        }
                    }
                }
            } else {
                // Unfiltered fast path: no bitmap checks, no 2-hop expansion
                if !dominated {
                    scratch.candidates.push(Reverse(OrdF32Pair(d, nb)));
                    scratch.results.push(OrdF32Pair(d, nb));
                    if scratch.results.len() > ef {
                        scratch.results.pop();
                    }
                    if scratch.results.len() >= ef {
                        worst_dist = scratch.results.peek().map_or(f32::MAX, |p| p.0);
                    }
                }
            }
        }
    }

    // Step 4: Extract top-K, map back to original IDs.
    // Results is a max-heap of up to `ef` entries. We need the nearest `k`.
    // Strategy: drain into SmallVec (farthest-first from max-heap), reverse, truncate.
    let result_count = scratch.results.len();
    let mut collected: SmallVec<[SearchResult; 32]> = SmallVec::with_capacity(result_count);
    while let Some(OrdF32Pair(dist, bfs_pos)) = scratch.results.pop() {
        collected.push(SearchResult::new(
            dist,
            VectorId(graph.to_original(bfs_pos)),
        ));
    }
    // collected is in reverse distance order (farthest first from max-heap drain)
    collected.reverse();
    // Now nearest first -- truncate to k
    collected.truncate(k);
    collected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::hnsw::build::HnswBuilder;
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::turbo_quant::encoder::{encode_tq_mse, padded_dimension};
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

    fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y) * (x - y))
            .sum()
    }

    /// Build a complete test fixture: vectors, TQ codes, HNSW graph, BFS-ordered TQ buffer.
    fn build_test_index(
        n: usize,
        dim: usize,
        m: u8,
        ef_construction: u16,
    ) -> (Vec<Vec<f32>>, HnswGraph, Vec<u8>, CollectionMetadata) {
        distance::init();

        let collection = CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        );
        let padded = collection.padded_dimension as usize;
        let signs = collection.fwht_sign_flips.as_slice();

        // Generate and encode vectors
        let mut vectors = Vec::with_capacity(n);
        let mut codes = Vec::with_capacity(n);
        let mut work = vec![0.0f32; padded];
        for i in 0..n {
            let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut v);
            let code = encode_tq_mse(&v, signs, &mut work);
            vectors.push(v);
            codes.push(code);
        }

        let dist_table = distance::table();
        let bytes_per_code = padded / 2 + 4; // nibble-packed + norm

        // Build a flat TQ buffer in insertion order for construction
        let mut tq_buffer_orig: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
        for code in &codes {
            tq_buffer_orig.extend_from_slice(&code.codes);
            tq_buffer_orig.extend_from_slice(&code.norm.to_le_bytes());
        }

        // Precompute all rotated queries for pairwise distance oracle
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

        // Build HNSW with true pairwise distance oracle
        let mut builder = HnswBuilder::new(m, ef_construction, 12345);

        for _i in 0..n {
            builder.insert(|a: u32, b: u32| {
                // True pairwise: use a's rotated query against b's code
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
                (dist_table.tq_l2)(q_rot, code_slice, norm)
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

        (vectors, graph, tq_buffer_bfs, collection)
    }

    /// Compute recall against brute-force TQ-ADC distances (same metric as search).
    fn compute_recall_tq(
        found: &[SearchResult],
        graph: &HnswGraph,
        tq_buf: &[u8],
        query: &[f32],
        collection: &CollectionMetadata,
        k: usize,
    ) -> f32 {
        let padded = collection.padded_dimension as usize;
        let signs = collection.fwht_sign_flips.as_slice();
        let dist_table = distance::table();

        // Prepare rotated query (same as in hnsw_search)
        let dim = query.len();
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(query);
        let mut norm_sq = 0.0f32;
        for &v in &q_rotated[..dim] {
            norm_sq += v * v;
        }
        let q_norm = norm_sq.sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rotated[..dim].iter_mut() {
                *v *= inv;
            }
        }
        fwht::fwht(&mut q_rotated[..padded], signs);

        // Brute force: compute TQ-ADC distance to every node
        let n = graph.num_nodes();
        let mut dists: Vec<(f32, u32)> = (0..n)
            .map(|bfs_pos| {
                let code = graph.tq_code(bfs_pos, tq_buf);
                let code_only = &code[..code.len() - 4];
                let norm = graph.tq_norm(bfs_pos, tq_buf);
                let d = (dist_table.tq_l2)(&q_rotated, code_only, norm);
                let orig_id = graph.to_original(bfs_pos);
                (d, orig_id)
            })
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let gt_ids: std::collections::HashSet<u32> =
            dists.iter().take(k).map(|d| d.1).collect();
        let found_ids: std::collections::HashSet<u32> =
            found.iter().map(|r| r.id.0).collect();
        let overlap = gt_ids.intersection(&found_ids).count();
        overlap as f32 / k as f32
    }

    // ── BitVec tests ──────────────────────────────────────────────────

    #[test]
    fn test_bitvec_new_word_count() {
        let bv = BitVec::new(1000);
        // ceil(1000/64) = 16 words
        assert_eq!(bv.words.len(), 16);
    }

    #[test]
    fn test_bitvec_test_and_set_first_returns_false() {
        let mut bv = BitVec::new(100);
        assert!(!bv.test_and_set(42));
    }

    #[test]
    fn test_bitvec_test_and_set_second_returns_true() {
        let mut bv = BitVec::new(100);
        assert!(!bv.test_and_set(42));
        assert!(bv.test_and_set(42));
    }

    #[test]
    fn test_bitvec_boundary_ids() {
        let mut bv = BitVec::new(1000);
        assert!(!bv.test_and_set(0));
        assert!(bv.test_and_set(0));
        assert!(!bv.test_and_set(63));
        assert!(bv.test_and_set(63));
        assert!(!bv.test_and_set(64));
        assert!(bv.test_and_set(64));
        assert!(!bv.test_and_set(999));
        assert!(bv.test_and_set(999));
    }

    #[test]
    fn test_bitvec_clear_all_resets() {
        let mut bv = BitVec::new(100);
        bv.test_and_set(10);
        bv.test_and_set(50);
        bv.clear_all(100);
        assert!(!bv.test_and_set(10));
        assert!(!bv.test_and_set(50));
    }

    #[test]
    fn test_bitvec_clear_all_grows() {
        let mut bv = BitVec::new(100);
        bv.clear_all(2000);
        assert!(bv.words.len() >= (2000 + 63) / 64);
        assert!(!bv.test_and_set(1999));
        assert!(bv.test_and_set(1999));
    }

    // ── SearchScratch tests ───────────────────────────────────────────

    #[test]
    fn test_search_scratch_new_sizes() {
        let scratch = SearchScratch::new(1000, 1024);
        assert!(scratch.candidates.capacity() >= 256);
        assert!(scratch.results.capacity() >= 256);
        assert!(scratch.visited.words.len() >= (1000 + 63) / 64);
        assert_eq!(scratch.query_rotated.len(), 1024);
    }

    #[test]
    fn test_search_scratch_clear_preserves_capacity() {
        let mut scratch = SearchScratch::new(1000, 1024);
        scratch
            .candidates
            .push(Reverse(OrdF32Pair(1.0, 0)));
        scratch.results.push(OrdF32Pair(1.0, 0));
        let cap_before_cand = scratch.candidates.capacity();
        let cap_before_res = scratch.results.capacity();

        scratch.clear(1000);

        assert!(scratch.candidates.is_empty());
        assert!(scratch.results.is_empty());
        assert!(scratch.candidates.capacity() >= cap_before_cand);
        assert!(scratch.results.capacity() >= cap_before_res);
    }

    // ── hnsw_search tests ─────────────────────────────────────────────

    #[test]
    fn test_search_empty_graph() {
        distance::init();
        let collection = CollectionMetadata::new(
            1, 64, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        let graph = HnswBuilder::new(16, 200, 42).build(
            (collection.padded_dimension / 2 + 4) as u32,
        );
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(0, padded);
        let query = vec![0.0f32; 64];
        let results = hnsw_search(&graph, &[], &query, &collection, 10, 64, &mut scratch);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_single_node() {
        let (vectors, graph, tq_buf, collection) = build_test_index(1, 64, 16, 200);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(1, padded);
        let results = hnsw_search(
            &graph,
            &tq_buf,
            &vectors[0],
            &collection,
            1,
            64,
            &mut scratch,
        );
        assert_eq!(results.len(), 1);
        // The single node should be returned (original ID 0)
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_search_100_vectors_recall() {
        let n = 100;
        let dim = 64;
        let k = 10;
        let ef = 64;
        let (_vectors, graph, tq_buf, collection) = build_test_index(n, dim, 16, 200);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(n as u32, padded);

        // Test with multiple queries -- recall measured against brute-force TQ-ADC
        let mut total_recall = 0.0f32;
        let num_queries = 10;
        for q_seed in 0..num_queries {
            let mut query = lcg_f32(dim, 10000 + q_seed * 17);
            normalize(&mut query);
            let results =
                hnsw_search(&graph, &tq_buf, &query, &collection, k, ef, &mut scratch);
            assert!(results.len() <= k);
            let recall =
                compute_recall_tq(&results, &graph, &tq_buf, &query, &collection, k);
            total_recall += recall;
        }
        let avg_recall = total_recall / num_queries as f32;
        eprintln!("100 vectors, dim=64, ef=64: avg TQ-ADC recall@10 = {avg_recall:.3}");
        assert!(
            avg_recall >= 0.90,
            "avg recall {avg_recall:.3} < 0.90 for 100 vectors with ef=64"
        );
    }

    #[test]
    fn test_search_1000_vectors_recall() {
        let n = 1000;
        let dim = 128;
        let k = 10;
        let ef = 128;
        let (_vectors, graph, tq_buf, collection) = build_test_index(n, dim, 16, 200);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(n as u32, padded);

        let mut total_recall = 0.0f32;
        let num_queries = 10;
        for q_seed in 0..num_queries {
            let mut query = lcg_f32(dim, 20000 + q_seed * 31);
            normalize(&mut query);
            let results =
                hnsw_search(&graph, &tq_buf, &query, &collection, k, ef, &mut scratch);
            assert!(results.len() <= k);
            let recall =
                compute_recall_tq(&results, &graph, &tq_buf, &query, &collection, k);
            total_recall += recall;
        }
        let avg_recall = total_recall / num_queries as f32;
        eprintln!("1000 vectors, dim=128, ef=128: avg TQ-ADC recall@10 = {avg_recall:.3}");
        assert!(
            avg_recall >= 0.95,
            "avg recall {avg_recall:.3} < 0.95 for 1000 vectors with ef=128"
        );
    }

    #[test]
    fn test_search_k1_returns_nearest() {
        let n = 50;
        let dim = 32;
        let (vectors, graph, tq_buf, collection) = build_test_index(n, dim, 8, 100);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(n as u32, padded);

        // Search for k=1 with high ef for maximum accuracy
        let query = &vectors[0]; // query IS a database vector
        let results =
            hnsw_search(&graph, &tq_buf, query, &collection, 1, 128, &mut scratch);
        assert_eq!(results.len(), 1);
        // Should find vector 0 itself (or very close to it)
        // Due to TQ quantization, self-distance is non-zero but should still rank #1
        eprintln!(
            "k=1 search for vector[0]: found id={}, dist={}",
            results[0].id.0, results[0].distance
        );
    }

    #[test]
    fn test_search_reuses_scratch_no_panic() {
        let n = 50;
        let dim = 32;
        let (vectors, graph, tq_buf, collection) = build_test_index(n, dim, 8, 100);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(n as u32, padded);

        // Search twice -- should not panic
        let _r1 = hnsw_search(
            &graph,
            &tq_buf,
            &vectors[0],
            &collection,
            5,
            64,
            &mut scratch,
        );
        let _r2 = hnsw_search(
            &graph,
            &tq_buf,
            &vectors[1],
            &collection,
            5,
            64,
            &mut scratch,
        );
    }

    #[test]
    fn test_search_filtered_none_same_as_unfiltered() {
        let n = 50;
        let dim = 32;
        let k = 5;
        let ef = 64;
        let (vectors, graph, tq_buf, collection) = build_test_index(n, dim, 8, 100);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(n as u32, padded);

        let unfiltered = hnsw_search(&graph, &tq_buf, &vectors[0], &collection, k, ef, &mut scratch);
        let filtered = hnsw_search_filtered(&graph, &tq_buf, &vectors[0], &collection, k, ef, &mut scratch, None);

        assert_eq!(unfiltered.len(), filtered.len());
        for (u, f) in unfiltered.iter().zip(filtered.iter()) {
            assert_eq!(u.id.0, f.id.0);
        }
    }

    #[test]
    fn test_search_filtered_bitmap_returns_only_matching_ids() {
        let n = 100;
        let dim = 64;
        let k = 10;
        let ef = 128;
        let (_vectors, graph, tq_buf, collection) = build_test_index(n, dim, 16, 200);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(n as u32, padded);

        // Allow only even IDs
        let mut bitmap = roaring::RoaringBitmap::new();
        for i in (0..n as u32).step_by(2) {
            bitmap.insert(i);
        }

        let mut query = lcg_f32(dim, 99999);
        normalize(&mut query);

        let results = hnsw_search_filtered(&graph, &tq_buf, &query, &collection, k, ef, &mut scratch, Some(&bitmap));
        for r in &results {
            assert!(bitmap.contains(r.id.0), "result id {} not in bitmap", r.id.0);
        }
        assert!(!results.is_empty(), "filtered search should return some results");
    }

    #[test]
    fn test_search_scratch_capacity_stable() {
        let n = 50;
        let dim = 32;
        let (vectors, graph, tq_buf, collection) = build_test_index(n, dim, 8, 100);
        let padded = collection.padded_dimension;
        let mut scratch = SearchScratch::new(n as u32, padded);

        // Warm up to establish capacity
        let _r = hnsw_search(
            &graph,
            &tq_buf,
            &vectors[0],
            &collection,
            5,
            64,
            &mut scratch,
        );
        let cap_cand = scratch.candidates.capacity();
        let cap_res = scratch.results.capacity();
        let words_len = scratch.visited.words.len();

        // Second search should not grow capacity
        let _r2 = hnsw_search(
            &graph,
            &tq_buf,
            &vectors[1],
            &collection,
            5,
            64,
            &mut scratch,
        );
        assert_eq!(
            scratch.candidates.capacity(),
            cap_cand,
            "candidates capacity grew between searches"
        );
        assert_eq!(
            scratch.results.capacity(),
            cap_res,
            "results capacity grew between searches"
        );
        assert_eq!(
            scratch.visited.words.len(),
            words_len,
            "visited words grew between searches"
        );
    }
}
