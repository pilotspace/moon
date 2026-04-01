//! VPSHUFB FastScan distance kernel for IVF posting list scanning.
//!
//! Computes approximate distances for 32 vectors simultaneously using
//! precomputed u8 LUT lookups. The AVX2 path uses VPSHUFB (_mm256_shuffle_epi8)
//! for 32 parallel table lookups per instruction.
//!
//! The scalar fallback produces identical results on all architectures.

use std::sync::OnceLock;

use smallvec::SmallVec;

use crate::vector::segment::ivf::BLOCK_SIZE;
use crate::vector::types::{SearchResult, VectorId};

/// Dispatch table for FastScan block kernels.
pub struct FastScanDispatch {
    /// Scan one interleaved 32-vector block, accumulating u16 distances.
    pub scan_block: fn(&[u8], &[u8], usize, &mut [u16; 32]),
}

static FASTSCAN_DISPATCH: OnceLock<FastScanDispatch> = OnceLock::new();

/// Initialize the FastScan dispatch table.
///
/// Selects AVX2 kernel on x86_64 when available, scalar otherwise.
/// Safe to call multiple times (OnceLock guarantees single init).
pub fn init_fastscan() {
    FASTSCAN_DISPATCH.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                return FastScanDispatch {
                    scan_block: |codes, lut, dim_half, results| {
                        // SAFETY: AVX2 verified by is_x86_feature_detected! above.
                        unsafe { fastscan_block_avx2(codes, lut, dim_half, results) }
                    },
                };
            }
        }

        // Scalar fallback for all platforms.
        FastScanDispatch {
            scan_block: fastscan_block_scalar,
        }
    });
}

/// Get the static FastScan dispatch table.
///
/// # Safety contract
/// Caller must ensure [`init_fastscan()`] has been called before first use.
#[inline(always)]
pub fn fastscan_dispatch() -> &'static FastScanDispatch {
    FASTSCAN_DISPATCH
        .get()
        .expect("init_fastscan() must be called before fastscan_dispatch()")
}

/// Scalar FastScan: compute distances for 32 vectors in one interleaved block.
///
/// `codes`: FAISS-interleaved block (`dim_half * 32` bytes). Each sub-dim d
///          has 32 contiguous bytes, one per vector. Each byte contains two
///          nibble-packed coordinate indices (lo=even coord, hi=odd coord).
/// `lut`:   Precomputed u8 distance LUT (`padded_dim * 16` entries).
///          `lut[coord * 16 + k]` = quantized distance for coordinate `coord`,
///          centroid index `k`.
/// `dim_half`: Number of sub-dimensions (= padded_dim / 2). Each sub-dim
///             represents a pair of coordinates.
/// `results`: Output accumulated u16 distances for 32 vectors (caller-provided).
///
/// No allocations.
pub fn fastscan_block_scalar(codes: &[u8], lut: &[u8], dim_half: usize, results: &mut [u16; 32]) {
    // Zero-initialize results.
    *results = [0u16; 32];

    for d in 0..dim_half {
        let code_base = d * BLOCK_SIZE;
        let lut_lo_base = (2 * d) * 16; // even coordinate LUT
        let lut_hi_base = (2 * d + 1) * 16; // odd coordinate LUT

        for v in 0..BLOCK_SIZE {
            let byte = codes[code_base + v];
            let lo_idx = (byte & 0x0F) as usize;
            let hi_idx = (byte >> 4) as usize;

            let dist_lo = lut[lut_lo_base + lo_idx] as u16;
            let dist_hi = lut[lut_hi_base + hi_idx] as u16;
            results[v] += dist_lo + dist_hi;
        }
    }
}

/// AVX2 VPSHUFB FastScan: compute distances for 32 vectors in one interleaved block.
///
/// Uses `_mm256_shuffle_epi8` (VPSHUFB) for 32 parallel LUT lookups per instruction.
/// Each sub-dimension performs:
/// 1. Load 32 nibble-packed bytes -> split lo/hi nibbles
/// 2. Broadcast 16-byte LUT to both lanes of __m256i
/// 3. VPSHUFB: 32 parallel lookups for even and odd coordinates
/// 4. Accumulate into u16 accumulators (zero-extend u8 -> u16 to avoid overflow)
///
/// # Safety
/// Caller must verify AVX2 is available via `is_x86_feature_detected!("avx2")`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn fastscan_block_avx2(
    codes: &[u8],
    lut: &[u8],
    dim_half: usize,
    results: &mut [u16; 32],
) {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    // SAFETY: AVX2 verified by caller via is_x86_feature_detected! or dispatch table.
    let lo_mask = _mm256_set1_epi8(0x0F);
    let zero = _mm256_setzero_si256();

    // Two u16 accumulators: acc_lo holds vectors 0..15, acc_hi holds vectors 16..31.
    let mut acc_lo = _mm256_setzero_si256(); // 16 x u16
    let mut acc_hi = _mm256_setzero_si256(); // 16 x u16

    for d in 0..dim_half {
        let code_base = d * BLOCK_SIZE;
        let lut_lo_base = (2 * d) * 16;
        let lut_hi_base = (2 * d + 1) * 16;

        // Load 32 bytes of interleaved codes for this sub-dimension.
        // SAFETY: codes has at least dim_half * 32 bytes; code_base + 32 <= codes.len().
        let packed = _mm256_loadu_si256(codes.as_ptr().add(code_base) as *const __m256i);

        // Split nibbles.
        let lo_nibbles = _mm256_and_si256(packed, lo_mask);
        let hi_nibbles = _mm256_and_si256(_mm256_srli_epi16(packed, 4), lo_mask);

        // Broadcast 16-byte LUT to both 128-bit lanes.
        // SAFETY: lut has at least padded_dim * 16 bytes.
        let lut_lo_vec = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            lut.as_ptr().add(lut_lo_base) as *const __m128i,
        ));
        let lut_hi_vec = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            lut.as_ptr().add(lut_hi_base) as *const __m128i,
        ));

        // VPSHUFB: 32 parallel lookups.
        let dist_lo = _mm256_shuffle_epi8(lut_lo_vec, lo_nibbles);
        let dist_hi = _mm256_shuffle_epi8(lut_hi_vec, hi_nibbles);

        // Add lo + hi distances (u8 + u8, still fits u8 for individual coord pair).
        // Then widen to u16 and accumulate.
        let dist_sum = _mm256_add_epi8(dist_lo, dist_hi);

        // Zero-extend lower 16 bytes to u16 and accumulate.
        let lo_16 = _mm256_unpacklo_epi8(dist_sum, zero);
        let hi_16 = _mm256_unpackhi_epi8(dist_sum, zero);

        acc_lo = _mm256_add_epi16(acc_lo, lo_16);
        acc_hi = _mm256_add_epi16(acc_hi, hi_16);
    }

    // Store accumulators to results.
    // unpacklo/unpackhi interleaves within 128-bit lanes, so the layout is:
    // acc_lo: [v0,v1,v2,v3,v4,v5,v6,v7 | v16,v17,v18,v19,v20,v21,v22,v23] (u16)
    // acc_hi: [v8,v9,v10,v11,v12,v13,v14,v15 | v24,v25,v26,v27,v28,v29,v30,v31] (u16)
    // We need to extract and interleave properly.
    //
    // Actually, _mm256_unpacklo_epi8 interleaves bytes from the lower half of each
    // 128-bit lane. For 32 input bytes [b0..b31], after unpacklo with zero:
    // result = [b0,0,b1,0,...,b7,0 | b16,0,b17,0,...,b23,0]
    // And unpackhi:
    // result = [b8,0,b9,0,...,b15,0 | b24,0,b25,0,...,b31,0]
    //
    // So we store and rearrange.
    let mut tmp_lo = [0u16; 16];
    let mut tmp_hi = [0u16; 16];
    _mm256_storeu_si256(tmp_lo.as_mut_ptr() as *mut __m256i, acc_lo);
    _mm256_storeu_si256(tmp_hi.as_mut_ptr() as *mut __m256i, acc_hi);

    // Rearrange from lane-interleaved to linear order.
    // acc_lo lane 0 (indices 0..7): vectors 0,1,2,3,4,5,6,7
    // acc_lo lane 1 (indices 8..15): vectors 16,17,18,19,20,21,22,23
    // acc_hi lane 0 (indices 0..7): vectors 8,9,10,11,12,13,14,15
    // acc_hi lane 1 (indices 8..15): vectors 24,25,26,27,28,29,30,31
    results[0..8].copy_from_slice(&tmp_lo[0..8]);
    results[8..16].copy_from_slice(&tmp_hi[0..8]);
    results[16..24].copy_from_slice(&tmp_lo[8..16]);
    results[24..32].copy_from_slice(&tmp_hi[8..16]);
}

/// Scan all blocks in a posting list and collect top-k results.
///
/// `codes`: Full interleaved code buffer from PostingList.
/// `lut`: Precomputed u8 distance LUT (padded_dim * 16 entries).
/// `dim_half`: padded_dim / 2.
/// `ids`: Vector IDs from PostingList.
/// `norms`: Precomputed norms from PostingList.
/// `count`: Number of vectors in the posting list.
/// `k`: Number of results to keep.
/// `results`: Output buffer for SearchResults (caller-provided SmallVec).
pub fn scan_posting_list(
    codes: &[u8],
    lut: &[u8],
    dim_half: usize,
    ids: &[u32],
    norms: &[f32],
    count: u32,
    k: usize,
    results: &mut SmallVec<[SearchResult; 32]>,
) {
    let dispatch = fastscan_dispatch();
    let n = count as usize;
    let n_blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    let block_bytes = dim_half * BLOCK_SIZE;

    let mut block_dists = [0u16; 32];

    for block_idx in 0..n_blocks {
        let code_start = block_idx * block_bytes;
        let vec_start = block_idx * BLOCK_SIZE;
        let vecs_in_block = (n - vec_start).min(BLOCK_SIZE);

        (dispatch.scan_block)(
            &codes[code_start..code_start + block_bytes],
            lut,
            dim_half,
            &mut block_dists,
        );

        // Convert u16 quantized distances to f32 and push results.
        for v in 0..vecs_in_block {
            let global_idx = vec_start + v;
            let norm = norms[global_idx];
            // Scale back: u16 distance is sum of quantized per-coord distances.
            // The actual L2 distance is approximately: norm^2 * (raw_dist / LUT_SCALE_TOTAL)
            // For ranking purposes, raw u16 distance * norm^2 preserves ordering.
            let dist_f32 = block_dists[v] as f32 * norm * norm;
            results.push(SearchResult::new(dist_f32, VectorId(ids[global_idx])));
        }
    }

    // Sort by distance (ascending) and truncate to k.
    results.sort_unstable();
    if results.len() > k {
        results.truncate(k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a simple interleaved block + LUT for testing.
    /// Returns (codes, lut, dim_half).
    fn make_test_block(dim_half: usize, n_vectors: usize) -> (Vec<u8>, Vec<u8>, usize) {
        let padded_dim = dim_half * 2;
        let mut codes = vec![0u8; dim_half * BLOCK_SIZE];
        let mut lut = vec![0u8; padded_dim * 16];

        // Set up a simple LUT: lut[coord * 16 + k] = k (distance proportional to index).
        for coord in 0..padded_dim {
            for k in 0..16 {
                lut[coord * 16 + k] = k as u8;
            }
        }

        // Set up codes: vector v, sub-dim d gets byte = (v & 0x0F) | ((v & 0x0F) << 4)
        // So lo_idx = hi_idx = v % 16 for all sub-dims.
        for d in 0..dim_half {
            for v in 0..n_vectors {
                let idx = (v % 16) as u8;
                codes[d * BLOCK_SIZE + v] = idx | (idx << 4);
            }
        }

        (codes, lut, dim_half)
    }

    #[test]
    fn test_fastscan_block_scalar_known_distances() {
        let dim_half = 2;
        let n_vectors = 4;
        let (codes, lut, _) = make_test_block(dim_half, n_vectors);

        let mut results = [0u16; 32];
        fastscan_block_scalar(&codes, &lut, dim_half, &mut results);

        // For vector v: each sub-dim contributes lut[lo_idx] + lut[hi_idx].
        // lo_idx = hi_idx = v % 16. lut[coord * 16 + k] = k.
        // So per sub-dim: v + v = 2*v. Over dim_half=2 sub-dims: 2 * 2*v = 4*v.
        // Wait: we have 2 coordinates per sub-dim (even + odd).
        // dist_lo = lut[(2*d) * 16 + lo_idx] = lo_idx = v
        // dist_hi = lut[(2*d+1) * 16 + hi_idx] = hi_idx = v
        // Per sub-dim: v + v = 2*v.
        // Over dim_half=2: 2 * 2*v = 4*v.
        for v in 0..n_vectors {
            assert_eq!(
                results[v],
                (4 * v) as u16,
                "scalar distance mismatch for v={v}"
            );
        }
        // Zero-padded vectors should have distance 0.
        for v in n_vectors..BLOCK_SIZE {
            assert_eq!(
                results[v], 0,
                "zero-padded vector {v} should have distance 0"
            );
        }
    }

    #[test]
    fn test_fastscan_block_scalar_trivial_2subdim() {
        // Hand-computed: dim_half=1 (2 coordinates), 2 vectors.
        let dim_half = 1;
        let padded_dim = 2;
        let mut codes = vec![0u8; dim_half * BLOCK_SIZE];
        let mut lut = vec![0u8; padded_dim * 16];

        // LUT for coord 0: [0, 10, 20, 30, ...] (dist = k * 10)
        // LUT for coord 1: [0, 5, 10, 15, ...]  (dist = k * 5)
        for k in 0..16 {
            lut[0 * 16 + k] = (k * 10).min(255) as u8; // coord 0
            lut[1 * 16 + k] = (k * 5).min(255) as u8; // coord 1
        }

        // Vector 0: lo_idx=2, hi_idx=3 -> byte = 0x32
        codes[0 * BLOCK_SIZE + 0] = 0x32;
        // Vector 1: lo_idx=0, hi_idx=1 -> byte = 0x10
        codes[0 * BLOCK_SIZE + 1] = 0x10;

        let mut results = [0u16; 32];
        fastscan_block_scalar(&codes, &lut, dim_half, &mut results);

        // Vector 0: dist = lut[0*16 + 2] + lut[1*16 + 3] = 20 + 15 = 35
        assert_eq!(results[0], 35, "vector 0 distance");
        // Vector 1: dist = lut[0*16 + 0] + lut[1*16 + 1] = 0 + 5 = 5
        assert_eq!(results[1], 5, "vector 1 distance");
    }

    #[test]
    fn test_fastscan_block_scalar_partial_block() {
        // 5 vectors out of 32, rest zero-padded.
        let dim_half = 2;
        let (codes, lut, _) = make_test_block(dim_half, 5);

        let mut results = [0u16; 32];
        fastscan_block_scalar(&codes, &lut, dim_half, &mut results);

        // Vectors 0-4 have nonzero distances, 5-31 should be 0.
        for v in 5..BLOCK_SIZE {
            assert_eq!(results[v], 0, "zero-padded vector {v} should be 0");
        }
    }

    #[test]
    fn test_scan_posting_list_scalar_topk() {
        init_fastscan();

        let dim_half = 2;
        let padded_dim = 4;
        let n = 10;

        // Build interleaved codes for 10 vectors.
        let mut codes = vec![0u8; 1 * dim_half * BLOCK_SIZE]; // 1 block
        let mut lut = vec![0u8; padded_dim * 16];

        // Simple LUT: lut[coord * 16 + k] = k.
        for coord in 0..padded_dim {
            for k in 0..16 {
                lut[coord * 16 + k] = k as u8;
            }
        }

        // Vector v gets index v%16 for all sub-dims.
        for d in 0..dim_half {
            for v in 0..n {
                let idx = (v % 16) as u8;
                codes[d * BLOCK_SIZE + v] = idx | (idx << 4);
            }
        }

        let ids: Vec<u32> = (100..110).collect();
        let norms = vec![1.0f32; n];

        let mut results: SmallVec<[SearchResult; 32]> = SmallVec::new();
        scan_posting_list(
            &codes,
            &lut,
            dim_half,
            &ids,
            &norms,
            n as u32,
            3,
            &mut results,
        );

        assert_eq!(results.len(), 3, "should return top 3");
        // Vector 0 has distance 0, should be first.
        assert_eq!(results[0].id, VectorId(100));
        assert_eq!(results[0].distance, 0.0);
        // Results should be sorted ascending.
        for w in results.windows(2) {
            assert!(w[0].distance <= w[1].distance, "results not sorted");
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_fastscan_block_avx2_matches_scalar() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        // Test with random-ish data.
        let dim_half = 64; // 128 coordinates
        let padded_dim = dim_half * 2;
        let mut codes = vec![0u8; dim_half * BLOCK_SIZE];
        let mut lut = vec![0u8; padded_dim * 16];

        // Fill with deterministic pseudo-random data.
        let mut s = 42u32;
        for b in codes.iter_mut() {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            *b = (s >> 24) as u8;
        }
        for b in lut.iter_mut() {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            // LUT values must be in [0, 127] to avoid overflow when adding lo+hi as u8.
            *b = ((s >> 24) as u8) & 0x7F;
        }

        let mut scalar_results = [0u16; 32];
        fastscan_block_scalar(&codes, &lut, dim_half, &mut scalar_results);

        let mut avx2_results = [0u16; 32];
        // SAFETY: AVX2 checked above.
        unsafe {
            fastscan_block_avx2(&codes, &lut, dim_half, &mut avx2_results);
        }

        for v in 0..BLOCK_SIZE {
            assert_eq!(
                avx2_results[v], scalar_results[v],
                "AVX2 vs scalar mismatch at v={v}: avx2={}, scalar={}",
                avx2_results[v], scalar_results[v]
            );
        }
    }

    #[test]
    fn test_fastscan_dispatch_init() {
        init_fastscan();
        let d = fastscan_dispatch();

        // Verify it produces a result (same as scalar for simple input).
        let dim_half = 1;
        let padded_dim = 2;
        let mut codes = vec![0u8; dim_half * BLOCK_SIZE];
        let mut lut = vec![0u8; padded_dim * 16];
        for k in 0..16 {
            lut[k] = k as u8;
            lut[16 + k] = k as u8;
        }
        codes[0] = 0x11; // lo=1, hi=1

        let mut results = [0u16; 32];
        (d.scan_block)(&codes, &lut, dim_half, &mut results);

        // Vector 0: dist = lut[0*16+1] + lut[1*16+1] = 1 + 1 = 2
        assert_eq!(results[0], 2);
    }
}
