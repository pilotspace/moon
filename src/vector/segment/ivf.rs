//! IVF (Inverted File) segment with FAISS-interleaved posting lists.
//!
//! Stores vectors partitioned by cluster centroids, with TQ codes in
//! FAISS-interleaved layout (32-vector blocks, dimension-interleaved) for
//! VPSHUFB FastScan distance computation.

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::turbo_quant::codebook::CENTROIDS;
use crate::vector::turbo_quant::encoder::padded_dimension;

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
}

impl IvfSegment {
    /// Create a new IVF segment.
    pub fn new(
        centroids: AlignedBuffer<f32>,
        posting_lists: Vec<PostingList>,
        n_clusters: u32,
        quantization: IvfQuantization,
        dimension: u32,
    ) -> Self {
        Self {
            centroids,
            posting_lists,
            n_clusters,
            quantization,
            dimension,
            padded_dim: padded_dimension(dimension),
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
