//! IVF (Inverted File) segment with FAISS-interleaved posting lists.
//!
//! Stores vectors partitioned by cluster centroids, with TQ codes in
//! FAISS-interleaved layout (32-vector blocks, dimension-interleaved) for
//! VPSHUFB FastScan distance computation.

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::distance::fastscan;
use crate::vector::turbo_quant::codebook::CENTROIDS;
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::types::SearchResult;

/// Quantization method used within IVF posting lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IvfQuantization {
    /// TurboQuant 4-bit: each coordinate quantized to 4-bit Lloyd-Max centroid.
    TurboQuant4Bit,
    /// Product Quantization with `m` sub-quantizers.
    PQ { m: u8 },
}

/// Number of vectors per interleaved block (matches FAISS FastScan convention).
pub const BLOCK_SIZE: usize = 32;

/// A posting list for one IVF cluster.
///
/// TQ codes are stored in FAISS-interleaved layout: 32-vector blocks where
/// each sub-dimension's nibble-packed bytes for all 32 vectors are contiguous.
/// This enables VPSHUFB to process 32 vectors per instruction.
pub struct PostingList {
    /// TQ codes in FAISS-interleaved layout.
    /// Layout per block: for each sub-dim d (0..dim_half), 32 bytes
    /// (one byte per vector, nibble-packed pair of coordinates).
    /// Total size: ceil(count/32) * dim_half * 32.
    pub codes: AlignedBuffer<u8>,
    /// Vector IDs in insertion order.
    pub ids: Vec<u32>,
    /// Precomputed L2 norms per vector.
    pub norms: Vec<f32>,
    /// Number of vectors in this posting list.
    pub count: u32,
}

impl PostingList {
    /// Create an empty posting list.
    pub fn new() -> Self {
        Self {
            codes: AlignedBuffer::new(0),
            ids: Vec::new(),
            norms: Vec::new(),
            count: 0,
        }
    }
}

/// Transpose a block of up to 32 nibble-packed TQ codes into FAISS-interleaved layout.
///
/// Input: `codes` is a flat slice where each vector's nibble-packed code is `dim_half`
/// bytes long, laid out contiguously: `[vec0_byte0..vec0_byte(dim_half-1), vec1_byte0..]`.
/// `n_vectors` is the actual count (<= 32).
///
/// Output: written to `out[..dim_half * 32]`. For each sub-dim d, 32 contiguous bytes
/// contain the nibble-packed byte of each vector (zero-padded if n_vectors < 32).
///
/// This is a transpose from [vector][dim] to [dim][vector] ordering.
///
/// No allocations. Caller provides `out` buffer of at least `dim_half * 32` bytes.
#[inline]
pub fn interleave_block(
    codes: &[u8],
    n_vectors: usize,
    dim_half: usize,
    out: &mut [u8],
) {
    debug_assert!(n_vectors <= BLOCK_SIZE);
    debug_assert!(out.len() >= dim_half * BLOCK_SIZE);

    // Zero the output first (handles padding for n_vectors < 32).
    for b in out[..dim_half * BLOCK_SIZE].iter_mut() {
        *b = 0;
    }

    // Transpose: codes[v * dim_half + d] -> out[d * 32 + v]
    for v in 0..n_vectors {
        let src_base = v * dim_half;
        if src_base + dim_half > codes.len() {
            break;
        }
        for d in 0..dim_half {
            out[d * BLOCK_SIZE + v] = codes[src_base + d];
        }
    }
}

/// Build a PostingList from a set of nibble-packed TQ codes, IDs, and norms.
///
/// Divides vectors into blocks of 32, interleaves each block, and concatenates
/// into a single AlignedBuffer.
pub fn interleave_posting_list(
    packed_codes: &[Vec<u8>],
    ids: &[u32],
    norms: &[f32],
) -> PostingList {
    let count = packed_codes.len();
    if count == 0 {
        return PostingList::new();
    }

    let dim_half = packed_codes[0].len();
    let n_blocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE;
    let block_bytes = dim_half * BLOCK_SIZE;
    let total_bytes = n_blocks * block_bytes;

    // Flatten codes for each block and interleave.
    let mut all_interleaved = vec![0u8; total_bytes];

    for block_idx in 0..n_blocks {
        let start = block_idx * BLOCK_SIZE;
        let end = count.min(start + BLOCK_SIZE);
        let n_in_block = end - start;

        // Flatten this block's codes contiguously.
        let mut flat = vec![0u8; n_in_block * dim_half];
        for (i, code) in packed_codes[start..end].iter().enumerate() {
            flat[i * dim_half..(i + 1) * dim_half].copy_from_slice(code);
        }

        let out_start = block_idx * block_bytes;
        interleave_block(
            &flat,
            n_in_block,
            dim_half,
            &mut all_interleaved[out_start..out_start + block_bytes],
        );
    }

    PostingList {
        codes: AlignedBuffer::from_vec(all_interleaved),
        ids: ids.to_vec(),
        norms: norms.to_vec(),
        count: count as u32,
    }
}

/// Maximum possible single-coordinate squared distance for LUT quantization.
///
/// Conservative bound: the largest FWHT coordinate for a unit vector is bounded,
/// and the largest centroid is CENTROIDS[15]. We use a generous bound.
const MAX_SINGLE_COORD_DIST_SQ: f32 = 0.03;

/// Scale factor for quantizing float distances to u8.
const LUT_SCALE: f32 = 240.0 / MAX_SINGLE_COORD_DIST_SQ;

/// Quantize a single float squared distance to u8 [0, 255].
#[inline]
fn quantize_dist_to_u8(dist_sq: f32) -> u8 {
    let scaled = dist_sq * LUT_SCALE;
    if scaled >= 255.0 {
        255
    } else if scaled <= 0.0 {
        0
    } else {
        scaled as u8
    }
}

/// Precompute u8 distance LUT from a rotated query vector.
///
/// For each coordinate `coord` in `0..padded_dim`, produces 16 entries:
/// `lut_out[coord * 16 + k] = quantize_dist_to_u8((q_rotated[coord] - CENTROIDS[k])^2)`
///
/// `lut_out` must have length >= `padded_dim * 16`.
///
/// No allocations. Caller provides output buffer.
#[inline]
pub fn precompute_lut(q_rotated: &[f32], lut_out: &mut [u8]) {
    let padded_dim = q_rotated.len();
    debug_assert!(lut_out.len() >= padded_dim * 16);

    for coord in 0..padded_dim {
        let q_val = q_rotated[coord];
        let base = coord * 16;
        for k in 0..16 {
            let diff = q_val - CENTROIDS[k];
            lut_out[base + k] = quantize_dist_to_u8(diff * diff);
        }
    }
}

/// An IVF segment: cluster centroids + posting lists of quantized vectors.
pub struct IvfSegment {
    /// Flat array of cluster centroids: n_clusters * dimension floats.
    centroids: AlignedBuffer<f32>,
    /// One posting list per cluster.
    posting_lists: Vec<PostingList>,
    /// Number of clusters (partitions).
    n_clusters: u32,
    /// Quantization method for posting list codes.
    quantization: IvfQuantization,
    /// Original vector dimension.
    dimension: u32,
    /// Padded dimension (next power of 2).
    padded_dim: u32,
    /// FWHT sign flips used to rotate queries before LUT precomputation.
    sign_flips: AlignedBuffer<f32>,
}

impl IvfSegment {
    /// Create a new IVF segment.
    pub fn new(
        centroids: AlignedBuffer<f32>,
        posting_lists: Vec<PostingList>,
        n_clusters: u32,
        quantization: IvfQuantization,
        dimension: u32,
        sign_flips: AlignedBuffer<f32>,
    ) -> Self {
        Self {
            centroids,
            posting_lists,
            n_clusters,
            quantization,
            dimension,
            padded_dim: padded_dimension(dimension),
            sign_flips,
        }
    }

    /// Number of IVF clusters.
    #[inline]
    pub fn n_clusters(&self) -> u32 {
        self.n_clusters
    }

    /// Original vector dimension.
    #[inline]
    pub fn dimension(&self) -> u32 {
        self.dimension
    }

    /// Padded dimension (for FWHT / interleaving).
    #[inline]
    pub fn padded_dim(&self) -> u32 {
        self.padded_dim
    }

    /// Quantization method.
    #[inline]
    pub fn quantization(&self) -> IvfQuantization {
        self.quantization
    }

    /// Reference to cluster centroids.
    #[inline]
    pub fn centroids(&self) -> &[f32] {
        self.centroids.as_slice()
    }

    /// Reference to posting lists.
    #[inline]
    pub fn posting_lists(&self) -> &[PostingList] {
        &self.posting_lists
    }

    /// Total number of vectors across all posting lists.
    pub fn total_vectors(&self) -> u64 {
        self.posting_lists.iter().map(|pl| pl.count as u64).sum()
    }

    /// Reference to the FWHT sign flips for query rotation.
    #[inline]
    pub fn sign_flips(&self) -> &[f32] {
        self.sign_flips.as_slice()
    }

    /// Search this IVF segment: precompute LUT, probe nprobe clusters, merge top-k.
    ///
    /// `query_f32`: raw f32 query vector (original dimension).
    /// `q_rotated`: pre-rotated query for LUT precomputation (padded_dim).
    /// `k`: number of results to return.
    /// `nprobe`: number of clusters to probe.
    /// `lut_buf`: caller-provided LUT buffer (padded_dim * 16 bytes).
    ///
    /// No heap allocations for typical nprobe/k values (SmallVec stack).
    pub fn search(
        &self,
        query_f32: &[f32],
        q_rotated: &[f32],
        k: usize,
        nprobe: usize,
        lut_buf: &mut [u8],
    ) -> SmallVec<[SearchResult; 32]> {
        // Precompute u8 distance LUT from rotated query.
        precompute_lut(q_rotated, lut_buf);

        let dim = self.dimension as usize;
        let pdim = self.padded_dim as usize;
        let dim_half = pdim / 2;

        // Find the nprobe closest centroids.
        let probed = find_nprobe_nearest(
            query_f32,
            self.centroids.as_slice(),
            dim,
            self.n_clusters as usize,
            nprobe,
        );

        let mut results: SmallVec<[SearchResult; 32]> = SmallVec::new();

        for &cluster_idx in &probed {
            let pl = &self.posting_lists[cluster_idx as usize];
            if pl.count == 0 {
                continue;
            }
            fastscan::scan_posting_list(
                pl.codes.as_slice(),
                lut_buf,
                dim_half,
                &pl.ids,
                &pl.norms,
                pl.count,
                k,
                &mut results,
            );
        }

        // Final merge: sort and truncate to k across all probed clusters.
        results.sort_unstable();
        if results.len() > k {
            results.truncate(k);
        }
        results
    }

    /// Search with a RoaringBitmap filter: only return results whose IDs are in the bitmap.
    ///
    /// Post-filtering approach: scan clusters as normal, then filter results.
    pub fn search_filtered(
        &self,
        query_f32: &[f32],
        q_rotated: &[f32],
        k: usize,
        nprobe: usize,
        lut_buf: &mut [u8],
        filter: &RoaringBitmap,
    ) -> SmallVec<[SearchResult; 32]> {
        // Get unfiltered results (with oversampling to compensate for filtering).
        let oversample_k = k * 3;
        let mut raw = self.search(query_f32, q_rotated, oversample_k, nprobe, lut_buf);

        // Post-filter: keep only IDs in the bitmap.
        raw.retain(|r| filter.contains(r.id.0));
        if raw.len() > k {
            raw.truncate(k);
        }
        raw
    }
}

// ---------------------------------------------------------------------------
// k-means clustering (runs at compaction time, NOT on hot path)
// ---------------------------------------------------------------------------

/// LCG PRNG (Knuth MMIX). Not cryptographic -- for reproducible k-means init only.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }

    /// Random usize in [0, bound).
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// Lloyd's k-means clustering. Returns centroids as flat f32 array (n_clusters * dim).
///
/// `vectors`: flat f32 array (n_vectors * dim).
/// `dim`: vector dimension.
/// `n_clusters`: number of clusters.
/// `max_iters`: iteration limit.
/// `seed`: for reproducible initialization (random subset selection).
///
/// This runs at compaction time -- allocations are fine.
pub fn kmeans_lloyd(
    vectors: &[f32],
    dim: usize,
    n_clusters: usize,
    max_iters: usize,
    seed: u64,
) -> Vec<f32> {
    let n_vectors = vectors.len() / dim;
    let actual_k = n_clusters.min(n_vectors);

    // Initialize centroids via random subset selection.
    let mut rng = Lcg::new(seed);
    let mut centroids = vec![0.0f32; actual_k * dim];
    let mut chosen = Vec::with_capacity(actual_k);

    for i in 0..actual_k {
        let mut idx = rng.next_usize(n_vectors);
        // Simple retry to avoid duplicates (acceptable for init).
        let mut attempts = 0;
        while chosen.contains(&idx) && attempts < 100 {
            idx = rng.next_usize(n_vectors);
            attempts += 1;
        }
        chosen.push(idx);
        centroids[i * dim..(i + 1) * dim]
            .copy_from_slice(&vectors[idx * dim..(idx + 1) * dim]);
    }

    let l2_f32 = crate::vector::distance::table().l2_f32;

    // Assignments: cluster index for each vector.
    let mut assignments = vec![0u32; n_vectors];

    for _iter in 0..max_iters {
        let mut changed = false;

        // Assign each vector to nearest centroid.
        for v in 0..n_vectors {
            let vec_slice = &vectors[v * dim..(v + 1) * dim];
            let mut best_cluster = 0u32;
            let mut best_dist = f32::MAX;
            for c in 0..actual_k {
                let centroid_slice = &centroids[c * dim..(c + 1) * dim];
                let dist = l2_f32(vec_slice, centroid_slice);
                if dist < best_dist {
                    best_dist = dist;
                    best_cluster = c as u32;
                }
            }
            if assignments[v] != best_cluster {
                assignments[v] = best_cluster;
                changed = true;
            }
        }

        if !changed {
            break;
        }

        // Recompute centroids as mean of assigned vectors.
        let mut sums = vec![0.0f32; actual_k * dim];
        let mut counts = vec![0u32; actual_k];

        for v in 0..n_vectors {
            let c = assignments[v] as usize;
            counts[c] += 1;
            let base = c * dim;
            let vec_base = v * dim;
            for d in 0..dim {
                sums[base + d] += vectors[vec_base + d];
            }
        }

        for c in 0..actual_k {
            if counts[c] > 0 {
                let inv = 1.0 / counts[c] as f32;
                let base = c * dim;
                for d in 0..dim {
                    centroids[base + d] = sums[base + d] * inv;
                }
            }
            // Empty cluster: keep previous centroid (no update).
        }
    }

    centroids
}

/// Find the nprobe closest centroids to a query vector by L2 distance.
///
/// Returns cluster indices sorted by ascending distance.
pub fn find_nprobe_nearest(
    query: &[f32],
    centroids: &[f32],
    dim: usize,
    n_clusters: usize,
    nprobe: usize,
) -> SmallVec<[u32; 64]> {
    let l2_f32 = crate::vector::distance::table().l2_f32;
    let effective_nprobe = nprobe.min(n_clusters);

    // Compute distances to all centroids.
    let mut dists: SmallVec<[(f32, u32); 64]> = SmallVec::with_capacity(n_clusters);
    for c in 0..n_clusters {
        let centroid = &centroids[c * dim..(c + 1) * dim];
        let dist = l2_f32(query, centroid);
        dists.push((dist, c as u32));
    }

    // Partial sort would be optimal but full sort is fine for typical n_clusters.
    dists.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    dists.iter().take(effective_nprobe).map(|&(_, idx)| idx).collect()
}

/// Build an IvfSegment from raw vectors, TQ codes, norms, and IDs.
///
/// Runs k-means, assigns vectors to clusters, builds interleaved posting lists.
/// This is a compaction-time operation -- allocations are acceptable.
pub fn build_ivf_segment(
    vectors_f32: &[f32],
    tq_codes: &[Vec<u8>],
    norms: &[f32],
    ids: &[u32],
    dim: usize,
    n_clusters: usize,
    sign_flips: &[f32],
) -> IvfSegment {
    let n_vectors = vectors_f32.len() / dim;
    let actual_k = n_clusters.min(n_vectors);

    // Run k-means to compute centroids.
    let centroids_flat = kmeans_lloyd(vectors_f32, dim, actual_k, 50, 42);

    let l2_f32 = crate::vector::distance::table().l2_f32;

    // Assign each vector to nearest centroid.
    let mut cluster_assignments = Vec::with_capacity(n_vectors);
    for v in 0..n_vectors {
        let vec_slice = &vectors_f32[v * dim..(v + 1) * dim];
        let mut best = 0usize;
        let mut best_dist = f32::MAX;
        for c in 0..actual_k {
            let centroid = &centroids_flat[c * dim..(c + 1) * dim];
            let dist = l2_f32(vec_slice, centroid);
            if dist < best_dist {
                best_dist = dist;
                best = c;
            }
        }
        cluster_assignments.push(best);
    }

    // Group by cluster and build posting lists.
    let mut cluster_codes: Vec<Vec<Vec<u8>>> = (0..actual_k).map(|_| Vec::new()).collect();
    let mut cluster_ids: Vec<Vec<u32>> = (0..actual_k).map(|_| Vec::new()).collect();
    let mut cluster_norms: Vec<Vec<f32>> = (0..actual_k).map(|_| Vec::new()).collect();

    for v in 0..n_vectors {
        let c = cluster_assignments[v];
        cluster_codes[c].push(tq_codes[v].clone());
        cluster_ids[c].push(ids[v]);
        cluster_norms[c].push(norms[v]);
    }

    let mut posting_lists = Vec::with_capacity(actual_k);
    for c in 0..actual_k {
        posting_lists.push(interleave_posting_list(
            &cluster_codes[c],
            &cluster_ids[c],
            &cluster_norms[c],
        ));
    }

    let mut sf_buf = AlignedBuffer::new(sign_flips.len());
    sf_buf.as_mut_slice().copy_from_slice(sign_flips);

    IvfSegment::new(
        AlignedBuffer::from_vec(centroids_flat),
        posting_lists,
        actual_k as u32,
        IvfQuantization::TurboQuant4Bit,
        dim as u32,
        sf_buf,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Generate deterministic sign flips (+/-1.0) for tests.
    fn test_sign_flips(len: usize, seed: u32) -> Vec<f32> {
        let mut flips = Vec::with_capacity(len);
        let mut s = seed;
        for _ in 0..len {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            if s & 1 == 0 { flips.push(1.0); } else { flips.push(-1.0); }
        }
        flips
    }

    /// Generate deterministic f32 vector via LCG.
    fn det_f32(dim: usize, seed: u64) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed as u32;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    #[test]
    fn test_posting_list_new_empty() {
        let pl = PostingList::new();
        assert_eq!(pl.count, 0);
        assert!(pl.ids.is_empty());
        assert!(pl.norms.is_empty());
        assert!(pl.codes.is_empty());
    }

    #[test]
    fn test_ivf_quantization_enum() {
        let tq = IvfQuantization::TurboQuant4Bit;
        let pq = IvfQuantization::PQ { m: 32 };
        assert_ne!(tq, pq);
        assert_eq!(tq, IvfQuantization::TurboQuant4Bit);
        if let IvfQuantization::PQ { m } = pq {
            assert_eq!(m, 32);
        }
    }

    #[test]
    fn test_interleave_block_full_32() {
        // 32 vectors, dim_half=4 (i.e. 8 coordinates, 4 packed bytes each).
        let dim_half = 4;
        let n = 32;
        // Each vector's packed code: [v, v+1, v+2, v+3] mod 256
        let mut codes = vec![0u8; n * dim_half];
        for v in 0..n {
            for d in 0..dim_half {
                codes[v * dim_half + d] = ((v + d) & 0xFF) as u8;
            }
        }

        let mut out = vec![0u8; dim_half * BLOCK_SIZE];
        interleave_block(&codes, n, dim_half, &mut out);

        // Verify transpose: out[d * 32 + v] == codes[v * dim_half + d]
        for v in 0..n {
            for d in 0..dim_half {
                assert_eq!(
                    out[d * BLOCK_SIZE + v],
                    codes[v * dim_half + d],
                    "mismatch at v={v}, d={d}"
                );
            }
        }
    }

    #[test]
    fn test_interleave_block_partial_zero_pads() {
        // 5 vectors, dim_half=2
        let dim_half = 2;
        let n = 5;
        let mut codes = vec![0u8; n * dim_half];
        for v in 0..n {
            codes[v * dim_half] = (v * 10) as u8;
            codes[v * dim_half + 1] = (v * 10 + 1) as u8;
        }

        let mut out = vec![0xFFu8; dim_half * BLOCK_SIZE]; // fill with 0xFF to detect zero-padding
        interleave_block(&codes, n, dim_half, &mut out);

        // First 5 positions should have data, rest should be 0
        for v in 0..n {
            assert_eq!(out[0 * BLOCK_SIZE + v], (v * 10) as u8);
            assert_eq!(out[1 * BLOCK_SIZE + v], (v * 10 + 1) as u8);
        }
        for v in n..BLOCK_SIZE {
            assert_eq!(out[0 * BLOCK_SIZE + v], 0, "not zero-padded at d=0 v={v}");
            assert_eq!(out[1 * BLOCK_SIZE + v], 0, "not zero-padded at d=1 v={v}");
        }
    }

    #[test]
    fn test_interleave_posting_list_roundtrip() {
        let dim_half = 4;
        let n = 40; // 1 full block + 8 in partial block

        let mut packed_codes = Vec::with_capacity(n);
        let mut ids = Vec::with_capacity(n);
        let mut norms = Vec::with_capacity(n);

        for v in 0..n {
            let code: Vec<u8> = (0..dim_half).map(|d| ((v * dim_half + d) & 0xFF) as u8).collect();
            packed_codes.push(code);
            ids.push(v as u32);
            norms.push(1.0 + v as f32 * 0.01);
        }

        let pl = interleave_posting_list(&packed_codes, &ids, &norms);
        assert_eq!(pl.count, 40);
        assert_eq!(pl.ids.len(), 40);
        assert_eq!(pl.norms.len(), 40);

        // Should have 2 blocks worth of interleaved data
        let expected_bytes = 2 * dim_half * BLOCK_SIZE;
        assert_eq!(pl.codes.len(), expected_bytes);

        // Verify first block's data
        for v in 0..BLOCK_SIZE {
            for d in 0..dim_half {
                assert_eq!(
                    pl.codes.as_slice()[d * BLOCK_SIZE + v],
                    packed_codes[v][d],
                    "block 0 mismatch at v={v}, d={d}"
                );
            }
        }
    }

    #[test]
    fn test_precompute_lut_known_query() {
        // Query: all zeros -> distance to each centroid k = CENTROIDS[k]^2
        let padded_dim = 4;
        let q = vec![0.0f32; padded_dim];
        let mut lut = vec![0u8; padded_dim * 16];
        precompute_lut(&q, &mut lut);

        // For each coord (all zero), LUT entry k = quantize(CENTROIDS[k]^2)
        for coord in 0..padded_dim {
            for k in 0..16 {
                let expected_dist = CENTROIDS[k] * CENTROIDS[k];
                let expected_u8 = quantize_dist_to_u8(expected_dist);
                assert_eq!(
                    lut[coord * 16 + k], expected_u8,
                    "LUT mismatch at coord={coord}, k={k}: dist={expected_dist}"
                );
            }
            // Centroid 7 and 8 are near zero, should have smallest distances
            assert!(lut[coord * 16 + 7] <= lut[coord * 16 + 0]);
            assert!(lut[coord * 16 + 8] <= lut[coord * 16 + 15]);
        }
    }

    #[test]
    fn test_precompute_lut_symmetry() {
        // Query at zero: CENTROIDS are symmetric, so LUT[k] == LUT[15-k]
        let padded_dim = 2;
        let q = vec![0.0f32; padded_dim];
        let mut lut = vec![0u8; padded_dim * 16];
        precompute_lut(&q, &mut lut);

        for coord in 0..padded_dim {
            for k in 0..16 {
                assert_eq!(
                    lut[coord * 16 + k],
                    lut[coord * 16 + (15 - k)],
                    "LUT symmetry broken at coord={coord}, k={k}"
                );
            }
        }
    }

    #[test]
    fn test_ivf_segment_struct() {
        let dim = 768u32;
        let n_clusters = 4u32;
        let centroids = AlignedBuffer::new((n_clusters * dim) as usize);

        let posting_lists: Vec<PostingList> = (0..n_clusters)
            .map(|_| PostingList::new())
            .collect();

        let seg = IvfSegment::new(
            centroids,
            posting_lists,
            n_clusters,
            IvfQuantization::TurboQuant4Bit,
            dim,
            AlignedBuffer::new(1024),
        );

        assert_eq!(seg.n_clusters(), 4);
        assert_eq!(seg.dimension(), 768);
        assert_eq!(seg.padded_dim(), 1024);
        assert_eq!(seg.quantization(), IvfQuantization::TurboQuant4Bit);
        assert_eq!(seg.total_vectors(), 0);
        assert_eq!(seg.centroids().len(), (4 * 768) as usize);
    }

    #[test]
    fn test_ivf_segment_total_vectors() {
        let dim = 128u32;
        let n_clusters = 2u32;
        let centroids = AlignedBuffer::new((n_clusters * dim) as usize);

        // Create posting lists with some vectors
        let dim_half = padded_dimension(dim) as usize / 2;
        let codes1: Vec<Vec<u8>> = (0..10).map(|v| vec![v as u8; dim_half]).collect();
        let ids1: Vec<u32> = (0..10).collect();
        let norms1 = vec![1.0f32; 10];
        let pl1 = interleave_posting_list(&codes1, &ids1, &norms1);

        let codes2: Vec<Vec<u8>> = (0..20).map(|v| vec![v as u8; dim_half]).collect();
        let ids2: Vec<u32> = (10..30).collect();
        let norms2 = vec![1.0f32; 20];
        let pl2 = interleave_posting_list(&codes2, &ids2, &norms2);

        let seg = IvfSegment::new(
            centroids,
            vec![pl1, pl2],
            n_clusters,
            IvfQuantization::TurboQuant4Bit,
            dim,
            AlignedBuffer::new(padded_dimension(dim) as usize),
        );

        assert_eq!(seg.total_vectors(), 30);
    }

    #[test]
    fn test_quantize_dist_to_u8_range() {
        // Zero distance -> 0
        assert_eq!(quantize_dist_to_u8(0.0), 0);
        // Max distance -> 240
        assert_eq!(quantize_dist_to_u8(MAX_SINGLE_COORD_DIST_SQ), 240);
        // Over max -> clamped to 255
        assert_eq!(quantize_dist_to_u8(1.0), 255);
        // Negative -> 0
        assert_eq!(quantize_dist_to_u8(-0.1), 0);
    }

    // -----------------------------------------------------------------------
    // k-means tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_kmeans_lloyd_convergence() {
        crate::vector::distance::init();
        let dim = 128;
        let n = 1000;
        let n_clusters = 16;

        // Generate random vectors.
        let mut vectors = Vec::with_capacity(n * dim);
        for i in 0..n {
            vectors.extend(det_f32(dim, i as u64 + 1));
        }

        let centroids = kmeans_lloyd(&vectors, dim, n_clusters, 50, 12345);

        // Should produce n_clusters * dim floats.
        assert_eq!(centroids.len(), n_clusters * dim);

        // Verify all 16 centroids are non-degenerate (not all identical).
        let mut unique = 0;
        for c in 0..n_clusters {
            let slice = &centroids[c * dim..(c + 1) * dim];
            let mag: f32 = slice.iter().map(|x| x * x).sum();
            if mag > 0.0 {
                unique += 1;
            }
        }
        assert_eq!(unique, n_clusters, "all centroids should be non-degenerate");
    }

    #[test]
    fn test_find_nprobe_nearest_correctness() {
        crate::vector::distance::init();
        let dim = 4;
        // 3 centroids at known positions.
        let centroids = vec![
            0.0, 0.0, 0.0, 0.0, // cluster 0 at origin
            10.0, 0.0, 0.0, 0.0, // cluster 1 at (10,0,0,0)
            0.0, 10.0, 0.0, 0.0, // cluster 2 at (0,10,0,0)
        ];

        // Query near cluster 0.
        let query = vec![0.1, 0.1, 0.0, 0.0];
        let nearest = find_nprobe_nearest(&query, &centroids, dim, 3, 2);
        assert_eq!(nearest.len(), 2);
        assert_eq!(nearest[0], 0, "cluster 0 should be closest");
    }

    #[test]
    fn test_find_nprobe_nearest_sorted_by_distance() {
        crate::vector::distance::init();
        let dim = 4;
        let centroids = vec![
            0.0, 0.0, 0.0, 0.0,
            1.0, 0.0, 0.0, 0.0,
            2.0, 0.0, 0.0, 0.0,
            3.0, 0.0, 0.0, 0.0,
        ];
        let query = vec![0.0, 0.0, 0.0, 0.0];
        let nearest = find_nprobe_nearest(&query, &centroids, dim, 4, 4);
        assert_eq!(nearest.as_slice(), &[0, 1, 2, 3]);
    }

    #[test]
    fn test_ivf_search_nprobe_1_single_cluster() {
        crate::vector::distance::init();
        let dim = 8;
        let pdim = padded_dimension(dim as u32) as usize;
        let dim_half = pdim / 2;

        // Build 2 clusters, each with some vectors.
        let signs = test_sign_flips(pdim, 42);

        // Cluster 0: vectors 0-3, cluster 1: vectors 4-7.
        let codes0: Vec<Vec<u8>> = (0..4).map(|v| vec![(v & 0xF) as u8; dim_half]).collect();
        let ids0: Vec<u32> = (0..4).collect();
        let norms0 = vec![1.0f32; 4];
        let pl0 = interleave_posting_list(&codes0, &ids0, &norms0);

        let codes1: Vec<Vec<u8>> = (4..8).map(|v| vec![(v & 0xF) as u8; dim_half]).collect();
        let ids1: Vec<u32> = (4..8).collect();
        let norms1 = vec![1.0f32; 4];
        let pl1 = interleave_posting_list(&codes1, &ids1, &norms1);

        // Centroids: cluster 0 at origin, cluster 1 far away.
        let mut centroids_data = vec![0.0f32; 2 * dim];
        for d in 0..dim {
            centroids_data[dim + d] = 100.0;
        }

        let mut sf_buf = AlignedBuffer::new(pdim);
        sf_buf.as_mut_slice().copy_from_slice(&signs);

        let seg = IvfSegment::new(
            AlignedBuffer::from_vec(centroids_data),
            vec![pl0, pl1],
            2,
            IvfQuantization::TurboQuant4Bit,
            dim as u32,
            sf_buf,
        );

        // Query near origin -> should probe cluster 0 only.
        let query = vec![0.0f32; dim];
        let q_rotated = vec![0.0f32; pdim];
        let mut lut_buf = vec![0u8; pdim * 16];

        let results = seg.search(&query, &q_rotated, 4, 1, &mut lut_buf);

        // All results should be from cluster 0 (ids 0-3).
        for r in &results {
            assert!(r.id.0 < 4, "nprobe=1 should only return cluster 0 vectors, got id={}", r.id.0);
        }
    }

    #[test]
    fn test_ivf_search_nprobe_all_matches_brute_force() {
        crate::vector::distance::init();
        let dim = 8;
        let pdim = padded_dimension(dim as u32) as usize;
        let dim_half = pdim / 2;

        let signs = test_sign_flips(pdim, 42);

        // 2 clusters, 4 vectors each.
        let codes0: Vec<Vec<u8>> = (0..4).map(|v| vec![(v & 0xF) as u8; dim_half]).collect();
        let ids0: Vec<u32> = (0..4).collect();
        let norms0 = vec![1.0f32; 4];
        let pl0 = interleave_posting_list(&codes0, &ids0, &norms0);

        let codes1: Vec<Vec<u8>> = (4..8).map(|v| vec![(v & 0xF) as u8; dim_half]).collect();
        let ids1: Vec<u32> = (4..8).collect();
        let norms1 = vec![1.0f32; 4];
        let pl1 = interleave_posting_list(&codes1, &ids1, &norms1);

        let centroids_data = vec![0.0f32; 2 * dim];

        let mut sf_buf = AlignedBuffer::new(pdim);
        sf_buf.as_mut_slice().copy_from_slice(&signs);

        let seg = IvfSegment::new(
            AlignedBuffer::from_vec(centroids_data),
            vec![pl0, pl1],
            2,
            IvfQuantization::TurboQuant4Bit,
            dim as u32,
            sf_buf,
        );

        let query = vec![0.0f32; dim];
        let q_rotated = vec![0.0f32; pdim];
        let mut lut_buf = vec![0u8; pdim * 16];

        // nprobe = n_clusters: scan all clusters.
        let results = seg.search(&query, &q_rotated, 8, 2, &mut lut_buf);

        // Should return all 8 vectors (or at least k=8).
        assert_eq!(results.len(), 8, "nprobe=all should return all vectors");

        // Verify all IDs present.
        let mut ids: Vec<u32> = results.iter().map(|r| r.id.0).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_ivf_search_filtered_respects_bitmap() {
        crate::vector::distance::init();
        let dim = 8;
        let pdim = padded_dimension(dim as u32) as usize;
        let dim_half = pdim / 2;

        let signs = test_sign_flips(pdim, 42);

        let codes: Vec<Vec<u8>> = (0..8).map(|v| vec![(v & 0xF) as u8; dim_half]).collect();
        let ids: Vec<u32> = (0..8).collect();
        let norms = vec![1.0f32; 8];
        let pl = interleave_posting_list(&codes, &ids, &norms);

        let centroids_data = vec![0.0f32; 1 * dim];

        let mut sf_buf = AlignedBuffer::new(pdim);
        sf_buf.as_mut_slice().copy_from_slice(&signs);

        let seg = IvfSegment::new(
            AlignedBuffer::from_vec(centroids_data),
            vec![pl],
            1,
            IvfQuantization::TurboQuant4Bit,
            dim as u32,
            sf_buf,
        );

        let query = vec![0.0f32; dim];
        let q_rotated = vec![0.0f32; pdim];
        let mut lut_buf = vec![0u8; pdim * 16];

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(2);
        bitmap.insert(5);

        let results = seg.search_filtered(&query, &q_rotated, 8, 1, &mut lut_buf, &bitmap);
        for r in &results {
            assert!(bitmap.contains(r.id.0), "filtered result id {} not in bitmap", r.id.0);
        }
    }

    #[test]
    fn test_build_ivf_segment_creates_valid_segment() {
        crate::vector::distance::init();
        let dim = 8;
        let pdim = padded_dimension(dim as u32) as usize;
        let dim_half = pdim / 2;
        let n = 100;
        let n_clusters = 4;
        let signs = test_sign_flips(pdim, 42);

        let mut vectors = Vec::with_capacity(n * dim);
        let mut tq_codes = Vec::with_capacity(n);
        let mut norms = Vec::with_capacity(n);
        let ids: Vec<u32> = (0..n as u32).collect();

        for i in 0..n {
            let v = det_f32(dim, i as u64 + 1);
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            norms.push(norm);
            vectors.extend_from_slice(&v);
            // Simple fake TQ code (just hash of vector index).
            tq_codes.push(vec![(i & 0xFF) as u8; dim_half]);
        }

        let seg = build_ivf_segment(&vectors, &tq_codes, &norms, &ids, dim, n_clusters, &signs);
        assert_eq!(seg.n_clusters() as usize, n_clusters);
        assert_eq!(seg.total_vectors(), n as u64);
        assert_eq!(seg.dimension(), dim as u32);
    }

    #[test]
    fn test_recall_at_10_nprobe_32() {
        // Recall test: 10K vectors from 256 synthetic Gaussian clusters.
        // nprobe=32 should achieve >= 0.90 recall@10.
        crate::vector::distance::init();

        let dim = 32;
        let pdim = padded_dimension(dim as u32) as usize;
        let _dim_half = pdim / 2;
        let n_vectors = 10_000;
        let n_clusters = 256;
        let n_queries = 100;
        let k = 10;
        let nprobe = 32;
        let signs = test_sign_flips(pdim, 42);

        // Generate clustered data: 256 clusters, ~39 vectors per cluster.
        let mut rng = Lcg::new(9999);
        let mut vectors = Vec::with_capacity(n_vectors * dim);
        let mut cluster_means = Vec::with_capacity(n_clusters * dim);

        // Generate cluster means.
        for _ in 0..n_clusters {
            for _ in 0..dim {
                let val = (rng.next_u64() as f32 / u64::MAX as f32) * 20.0 - 10.0;
                cluster_means.push(val);
            }
        }

        // Assign vectors to clusters with small noise.
        for i in 0..n_vectors {
            let c = i % n_clusters;
            for d in 0..dim {
                let noise = (rng.next_u64() as f32 / u64::MAX as f32) * 0.2 - 0.1;
                vectors.push(cluster_means[c * dim + d] + noise);
            }
        }

        // Compute norms and fake TQ codes.
        let mut norms = Vec::with_capacity(n_vectors);
        let mut tq_codes = Vec::with_capacity(n_vectors);
        let ids: Vec<u32> = (0..n_vectors as u32).collect();

        for i in 0..n_vectors {
            let v = &vectors[i * dim..(i + 1) * dim];
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            norms.push(if norm > 0.0 { norm } else { 1.0 });

            // Create TQ codes: encode using real encoder for accurate recall.
            let mut work_buf = vec![0.0f32; pdim];
            let code = crate::vector::turbo_quant::encoder::encode_tq_mse(v, &signs, &mut work_buf);
            tq_codes.push(code.codes);
        }

        // Build IVF segment.
        let seg = build_ivf_segment(&vectors, &tq_codes, &norms, &ids, dim, n_clusters, &signs);

        // Ground truth: IVF search with nprobe = ALL clusters (exhaustive).
        // Recall measures partition quality: how many true top-k (by IVF metric)
        // are found when probing only nprobe out of n_clusters.
        let mut total_recall = 0.0f64;

        for q_idx in 0..n_queries {
            let query = det_f32(dim, 100_000 + q_idx as u64);

            // Rotate query for LUT precomputation.
            let mut q_rotated = vec![0.0f32; pdim];
            q_rotated[..dim].copy_from_slice(&query);
            let qnorm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
            if qnorm > 0.0 {
                let inv = 1.0 / qnorm;
                for v in q_rotated[..dim].iter_mut() {
                    *v *= inv;
                }
            }
            crate::vector::turbo_quant::fwht::fwht(&mut q_rotated, &signs);

            let mut lut_buf = vec![0u8; pdim * 16];

            // Ground truth: exhaustive scan of ALL clusters.
            let gt_results = seg.search(&query, &q_rotated, k, n_clusters, &mut lut_buf);
            let gt_ids: Vec<u32> = gt_results.iter().map(|r| r.id.0).collect();

            // IVF search with limited nprobe.
            let results = seg.search(&query, &q_rotated, k, nprobe, &mut lut_buf);

            // Count recall: how many of our top-k are in ground truth top-k.
            let result_ids: Vec<u32> = results.iter().map(|r| r.id.0).collect();
            let hits = result_ids.iter().filter(|id| gt_ids.contains(id)).count();
            total_recall += hits as f64 / k as f64;
        }

        let avg_recall = total_recall / n_queries as f64;
        assert!(
            avg_recall >= 0.90,
            "recall@10 = {avg_recall:.4} < 0.90 at nprobe={nprobe}"
        );
    }

    #[test]
    fn test_lcg_deterministic() {
        let mut rng1 = Lcg::new(42);
        let mut rng2 = Lcg::new(42);
        for _ in 0..100 {
            assert_eq!(rng1.next_u64(), rng2.next_u64());
        }
    }
}
