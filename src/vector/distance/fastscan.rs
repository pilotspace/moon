//! FastScan distance kernels: register-resident 4-bit LUT accumulation.
//!
//! Implements the FAISS FastScan technique (André et al., VLDB'15): TQ4 codes
//! are stored in interleaved 32-vector blocks and approximate distances for
//! all 32 vectors are accumulated simultaneously via in-register SIMD table
//! lookups — VPSHUFB (`_mm256_shuffle_epi8`) on x86_64, TBL (`vqtbl1q_u8`) on
//! aarch64. Each coordinate's 16-entry u8 LUT fits exactly one SIMD register,
//! so the inner loop never touches RAM for table lookups.
//!
//! Distance semantics (all kernels are bit-identical):
//! - per sub-dim, the two nibble distances are widened to u16 and summed
//!   exactly (max 255 + 255 = 510, no wrap-around),
//! - the running per-vector accumulator is u16 with *saturating* addition,
//!   so pathological high-dim/high-scale combinations clamp at 65535 instead
//!   of wrapping (a clamped distance ranks last, which is the correct failure
//!   mode for a far-away candidate).
//!
//! LUT construction ([`build_quantized_lut`]) follows the FAISS scheme: a
//! per-coordinate bias (the minimum distance for that coordinate) is
//! subtracted before quantization and a single global scale maps the residual
//! range to u8, chosen so that the worst-case accumulated sum also fits u16.
//! The true float distance is reconstructed as `acc / scale + bias`.
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
/// Kernel selection: NEON TBL on aarch64 (baseline feature, no runtime
/// detection needed), AVX2 VPSHUFB on x86_64 when available, scalar otherwise.
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

        #[cfg(target_arch = "aarch64")]
        {
            return FastScanDispatch {
                scan_block: fastscan_block_neon,
            };
        }

        // Scalar fallback for all other platforms.
        #[allow(unreachable_code)]
        FastScanDispatch {
            scan_block: fastscan_block_scalar,
        }
    });
}

/// Get the static FastScan dispatch table.
///
/// Auto-initializes on first use if [`init_fastscan()`] was not called explicitly.
/// After the first call the hot path is two atomic loads (both always succeed).
#[inline(always)]
pub fn fastscan_dispatch() -> &'static FastScanDispatch {
    if FASTSCAN_DISPATCH.get().is_none() {
        init_fastscan();
    }
    FASTSCAN_DISPATCH
        .get()
        .expect("fastscan dispatch initialized by init_fastscan()")
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
/// Accumulation is saturating (see module docs). No allocations.
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
            results[v] = results[v].saturating_add(dist_lo + dist_hi);
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
/// 4. Widen each lookup result to u16 BEFORE adding (u8 + u8 can exceed 255),
///    then accumulate with saturating u16 adds — bit-identical to the scalar
///    kernel for the full u8 LUT range.
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

    // Two u16 accumulators covering 32 vectors (lane-interleaved layout,
    // untangled at store time).
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

        // Widen each u8 lookup to u16 first, then add exactly (max 510),
        // then accumulate with saturating u16 adds. Adding as u8 would wrap
        // for LUT pairs summing past 255.
        let lo_w0 = _mm256_unpacklo_epi8(dist_lo, zero);
        let lo_w1 = _mm256_unpackhi_epi8(dist_lo, zero);
        let hi_w0 = _mm256_unpacklo_epi8(dist_hi, zero);
        let hi_w1 = _mm256_unpackhi_epi8(dist_hi, zero);

        let sum_w0 = _mm256_add_epi16(lo_w0, hi_w0);
        let sum_w1 = _mm256_add_epi16(lo_w1, hi_w1);

        acc_lo = _mm256_adds_epu16(acc_lo, sum_w0);
        acc_hi = _mm256_adds_epu16(acc_hi, sum_w1);
    }

    // Store accumulators to results.
    // _mm256_unpacklo_epi8 interleaves bytes within each 128-bit lane, so:
    // acc_lo: [v0..v7   | v16..v23] (u16 lanes)
    // acc_hi: [v8..v15  | v24..v31] (u16 lanes)
    let mut tmp_lo = [0u16; 16];
    let mut tmp_hi = [0u16; 16];
    _mm256_storeu_si256(tmp_lo.as_mut_ptr() as *mut __m256i, acc_lo);
    _mm256_storeu_si256(tmp_hi.as_mut_ptr() as *mut __m256i, acc_hi);

    results[0..8].copy_from_slice(&tmp_lo[0..8]);
    results[8..16].copy_from_slice(&tmp_hi[0..8]);
    results[16..24].copy_from_slice(&tmp_lo[8..16]);
    results[24..32].copy_from_slice(&tmp_hi[8..16]);
}

/// NEON TBL FastScan: compute distances for 32 vectors in one interleaved block.
///
/// Each coordinate's 16-entry u8 LUT fits exactly one 128-bit `q` register;
/// `vqtbl1q_u8` performs 16 parallel lookups per instruction. A 32-vector
/// block is processed as two 16-vector halves. Widening uses `vaddl_u8` /
/// `vaddl_high_u8` (exact u8+u8 -> u16), accumulation uses `vqaddq_u16`
/// (saturating) — bit-identical to the scalar kernel.
///
/// NEON is a baseline feature on aarch64 — no runtime detection required
/// (same convention as `src/vector/distance/neon.rs`).
#[cfg(target_arch = "aarch64")]
pub fn fastscan_block_neon(codes: &[u8], lut: &[u8], dim_half: usize, results: &mut [u16; 32]) {
    use std::arch::aarch64::*;

    // Runtime-enforced (release builds included): the unsafe block below does
    // raw-pointer loads and must never trust caller discipline alone. Cost is
    // two predicted branches per 32-vector block — noise vs the scan itself.
    assert!(codes.len() >= dim_half * BLOCK_SIZE);
    assert!(lut.len() >= dim_half * 32);

    // SAFETY: NEON is mandatory baseline on aarch64 (guaranteed by the
    // target_arch cfg above). All pointer arithmetic stays within the
    // caller-provided slices: per iteration we read 16 bytes at
    // codes[d*32 + half*16], bounded by the `codes.len() >= dim_half*32`
    // assert above, and 16 bytes at lut[(2d)*16] / lut[(2d+1)*16], bounded by
    // the `lut.len() >= dim_half*32` assert above (= padded_dim*16).
    unsafe {
        let nibble_mask = vdupq_n_u8(0x0F);
        // Four u16x8 accumulators in linear vector order: v0..7, v8..15, v16..23, v24..31.
        let mut acc0 = vdupq_n_u16(0);
        let mut acc1 = vdupq_n_u16(0);
        let mut acc2 = vdupq_n_u16(0);
        let mut acc3 = vdupq_n_u16(0);

        for d in 0..dim_half {
            let code_ptr = codes.as_ptr().add(d * BLOCK_SIZE);
            let lut_lo = vld1q_u8(lut.as_ptr().add((2 * d) * 16));
            let lut_hi = vld1q_u8(lut.as_ptr().add((2 * d + 1) * 16));

            // First half: vectors 0..15.
            let packed_a = vld1q_u8(code_ptr);
            let lo_a = vandq_u8(packed_a, nibble_mask);
            let hi_a = vshrq_n_u8(packed_a, 4);
            let dist_lo_a = vqtbl1q_u8(lut_lo, lo_a);
            let dist_hi_a = vqtbl1q_u8(lut_hi, hi_a);
            acc0 = vqaddq_u16(
                acc0,
                vaddl_u8(vget_low_u8(dist_lo_a), vget_low_u8(dist_hi_a)),
            );
            acc1 = vqaddq_u16(acc1, vaddl_high_u8(dist_lo_a, dist_hi_a));

            // Second half: vectors 16..31.
            let packed_b = vld1q_u8(code_ptr.add(16));
            let lo_b = vandq_u8(packed_b, nibble_mask);
            let hi_b = vshrq_n_u8(packed_b, 4);
            let dist_lo_b = vqtbl1q_u8(lut_lo, lo_b);
            let dist_hi_b = vqtbl1q_u8(lut_hi, hi_b);
            acc2 = vqaddq_u16(
                acc2,
                vaddl_u8(vget_low_u8(dist_lo_b), vget_low_u8(dist_hi_b)),
            );
            acc3 = vqaddq_u16(acc3, vaddl_high_u8(dist_lo_b, dist_hi_b));
        }

        vst1q_u16(results.as_mut_ptr(), acc0);
        vst1q_u16(results.as_mut_ptr().add(8), acc1);
        vst1q_u16(results.as_mut_ptr().add(16), acc2);
        vst1q_u16(results.as_mut_ptr().add(24), acc3);
    }
}

/// Reconstruction parameters returned by [`build_quantized_lut`].
///
/// The true float ADC distance is approximately `acc as f32 / scale + bias`
/// where `acc` is the u16 accumulator produced by a FastScan block kernel.
#[derive(Debug, Clone, Copy)]
pub struct LutParams {
    /// Global scale factor applied to (dist - coord_min) before u8 quantization.
    pub scale: f32,
    /// Sum of per-coordinate minimum distances (subtracted during quantization).
    pub bias: f32,
}

/// Build a quantized u8 distance LUT from a rotated query (FAISS scheme).
///
/// For each coordinate `c` in `0..padded_dim` the 16 candidate distances
/// `(q_rotated[c] - centroids[k])^2` are computed; the per-coordinate minimum
/// is subtracted (its sum across coordinates becomes `bias`) and the residual
/// is scaled by a single global factor into u8. The scale is the largest
/// value that (a) keeps every entry <= 255 and (b) keeps the worst-case
/// accumulated sum <= u16::MAX, so kernel saturation cannot trigger for
/// in-range data.
///
/// `centroids` must be the same dimension-scaled codebook used to encode the
/// stored TQ codes (`scaled_centroids(padded_dim)`), NOT the legacy
/// 1/sqrt(768) table — asymmetric codebooks silently destroy recall.
///
/// `lut_out` must have length >= `q_rotated.len() * 16`. No allocations.
pub fn build_quantized_lut(
    q_rotated: &[f32],
    centroids: &[f32; 16],
    lut_out: &mut [u8],
) -> LutParams {
    let padded_dim = q_rotated.len();
    debug_assert!(lut_out.len() >= padded_dim * 16);

    // Pass 1: per-coordinate min/max to derive bias and the global scale.
    let mut bias = 0.0f32;
    let mut sum_range = 0.0f32;
    let mut max_range = 0.0f32;
    for &q_val in q_rotated.iter() {
        let mut coord_min = f32::MAX;
        let mut coord_max = 0.0f32;
        for &c in centroids.iter() {
            let diff = q_val - c;
            let d = diff * diff;
            coord_min = coord_min.min(d);
            coord_max = coord_max.max(d);
        }
        bias += coord_min;
        let range = coord_max - coord_min;
        sum_range += range;
        max_range = max_range.max(range);
    }

    let scale = if max_range > 0.0 {
        (255.0 / max_range).min(65535.0 / sum_range.max(f32::MIN_POSITIVE))
    } else {
        1.0
    };

    // Pass 2: quantize. Recomputing the 16 distances per coordinate is cheaper
    // than allocating a padded_dim*16 f32 scratch on the query path.
    for (coord, &q_val) in q_rotated.iter().enumerate() {
        let mut coord_min = f32::MAX;
        let mut dists = [0.0f32; 16];
        for (k, &c) in centroids.iter().enumerate() {
            let diff = q_val - c;
            let d = diff * diff;
            dists[k] = d;
            coord_min = coord_min.min(d);
        }
        let base = coord * 16;
        for (k, &d) in dists.iter().enumerate() {
            let scaled = (d - coord_min) * scale + 0.5;
            lut_out[base + k] = if scaled >= 255.0 { 255 } else { scaled as u8 };
        }
    }

    LutParams { scale, bias }
}

/// Scan all blocks in a posting list and collect top-k results.
///
/// `codes`: Full interleaved code buffer from PostingList.
/// `lut`: Quantized u8 distance LUT (padded_dim * 16 entries).
/// `params`: Scale/bias from [`build_quantized_lut`] for f32 reconstruction.
/// `dim_half`: padded_dim / 2.
/// `ids`: Vector IDs from PostingList.
/// `norms`: Precomputed norms from PostingList.
/// `count`: Number of vectors in the posting list.
/// `k`: Number of results to keep.
/// `results`: Output buffer for SearchResults (caller-provided SmallVec).
#[allow(clippy::too_many_arguments)]
pub fn scan_posting_list(
    codes: &[u8],
    lut: &[u8],
    params: LutParams,
    dim_half: usize,
    ids: &[u32],
    norms: &[f32],
    count: u32,
    k: usize,
    results: &mut SmallVec<[SearchResult; 32]>,
) {
    let dispatch = fastscan_dispatch();
    let n = count as usize;
    let n_blocks = n.div_ceil(BLOCK_SIZE);
    let block_bytes = dim_half * BLOCK_SIZE;
    let inv_scale = 1.0 / params.scale;

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

        // Reconstruct approximate f32 ADC distances and push results.
        for v in 0..vecs_in_block {
            let global_idx = vec_start + v;
            let norm = norms[global_idx];
            let adc = block_dists[v] as f32 * inv_scale + params.bias;
            let dist_f32 = adc * norm * norm;
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

    /// Deterministic full-range pseudo-random codes + LUT (no 0x7F masking —
    /// exercises the widen-before-add and saturating-accumulate paths).
    fn make_random_block(dim_half: usize, seed: u32) -> (Vec<u8>, Vec<u8>) {
        let padded_dim = dim_half * 2;
        let mut codes = vec![0u8; dim_half * BLOCK_SIZE];
        let mut lut = vec![0u8; padded_dim * 16];
        let mut s = seed;
        for b in codes.iter_mut() {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            *b = (s >> 24) as u8;
        }
        for b in lut.iter_mut() {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            *b = (s >> 24) as u8; // FULL 0..=255 range
        }
        (codes, lut)
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
        // dist_lo = lut[(2*d) * 16 + lo_idx] = lo_idx = v
        // dist_hi = lut[(2*d+1) * 16 + hi_idx] = hi_idx = v
        // Per sub-dim: v + v = 2*v. Over dim_half=2: 2 * 2*v = 4*v.
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
        #[allow(clippy::identity_op, clippy::erasing_op)]
        for k in 0..16 {
            lut[0 * 16 + k] = (k * 10).min(255) as u8; // coord 0
            lut[1 * 16 + k] = (k * 5).min(255) as u8; // coord 1
        }

        // Vector 0: lo_idx=2, hi_idx=3 -> byte = 0x32
        #[allow(clippy::identity_op, clippy::erasing_op)]
        {
            codes[0 * BLOCK_SIZE + 0] = 0x32;
            // Vector 1: lo_idx=0, hi_idx=1 -> byte = 0x10
            codes[0 * BLOCK_SIZE + 1] = 0x10;
        }

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
    fn test_fastscan_scalar_saturates_instead_of_wrapping() {
        // All-255 LUT with enough sub-dims to exceed u16::MAX: 200 sub-dims
        // * 510 per sub-dim = 102000 > 65535. Accumulator must clamp, not wrap.
        let dim_half = 200;
        let padded_dim = dim_half * 2;
        let codes = vec![0xFFu8; dim_half * BLOCK_SIZE]; // all nibbles = 15
        let lut = vec![255u8; padded_dim * 16];

        let mut results = [0u16; 32];
        fastscan_block_scalar(&codes, &lut, dim_half, &mut results);
        for v in 0..BLOCK_SIZE {
            assert_eq!(results[v], u16::MAX, "v={v} must saturate at u16::MAX");
        }
    }

    #[test]
    fn test_scan_posting_list_scalar_topk() {
        init_fastscan();

        let dim_half = 2;
        let padded_dim = 4;
        let n = 10;

        // Build interleaved codes for 10 vectors.
        let mut codes = vec![0u8; dim_half * BLOCK_SIZE]; // 1 block
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
            LutParams {
                scale: 1.0,
                bias: 0.0,
            },
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
    fn test_fastscan_block_avx2_matches_scalar_full_range() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        // Full-range LUT (0..=255): locks in the widen-before-add fix — the
        // old kernel wrapped u8 pairs summing past 255.
        for (dim_half, seed) in [(1usize, 7u32), (3, 11), (64, 42), (200, 99)] {
            let (codes, lut) = make_random_block(dim_half, seed);

            let mut scalar_results = [0u16; 32];
            fastscan_block_scalar(&codes, &lut, dim_half, &mut scalar_results);

            let mut avx2_results = [0u16; 32];
            // SAFETY: AVX2 checked above.
            unsafe {
                fastscan_block_avx2(&codes, &lut, dim_half, &mut avx2_results);
            }

            assert_eq!(
                avx2_results, scalar_results,
                "AVX2 vs scalar mismatch (dim_half={dim_half})"
            );
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn test_fastscan_block_neon_matches_scalar_full_range() {
        // Full-range LUT (0..=255) across several dims, including a
        // saturating case (dim_half=200 with high LUT values).
        for (dim_half, seed) in [(1usize, 7u32), (3, 11), (64, 42), (200, 99)] {
            let (codes, lut) = make_random_block(dim_half, seed);

            let mut scalar_results = [0u16; 32];
            fastscan_block_scalar(&codes, &lut, dim_half, &mut scalar_results);

            let mut neon_results = [0u16; 32];
            fastscan_block_neon(&codes, &lut, dim_half, &mut neon_results);

            assert_eq!(
                neon_results, scalar_results,
                "NEON vs scalar mismatch (dim_half={dim_half})"
            );
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn test_fastscan_block_neon_saturates() {
        let dim_half = 200;
        let codes = vec![0xFFu8; dim_half * BLOCK_SIZE];
        let lut = vec![255u8; dim_half * 2 * 16];
        let mut results = [0u16; 32];
        fastscan_block_neon(&codes, &lut, dim_half, &mut results);
        for v in 0..BLOCK_SIZE {
            assert_eq!(results[v], u16::MAX, "v={v} must saturate at u16::MAX");
        }
    }

    #[test]
    fn test_build_quantized_lut_reconstruction_accuracy() {
        use crate::vector::turbo_quant::codebook::scaled_centroids;

        // Reconstructed FastScan distance must track the exact f32 ADC
        // distance closely for a realistic rotated-unit-vector magnitude.
        let padded_dim = 128usize;
        let dim_half = padded_dim / 2;
        let centroids = scaled_centroids(padded_dim as u32);
        let sigma = 1.0 / (padded_dim as f32).sqrt();

        // Deterministic query: coordinates in +/- 2*sigma (typical for FWHT output).
        let mut s = 12345u32;
        let mut q = vec![0.0f32; padded_dim];
        for v in q.iter_mut() {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            *v = ((s as f32) / (u32::MAX as f32) * 4.0 - 2.0) * sigma;
        }

        let mut lut = vec![0u8; padded_dim * 16];
        let params = build_quantized_lut(&q, &centroids, &mut lut);
        assert!(params.scale > 0.0, "scale must be positive");

        // Random codes for 32 vectors, exact f32 ADC vs reconstructed.
        let mut codes_per_vec = vec![vec![0u8; dim_half]; 32];
        for cv in codes_per_vec.iter_mut() {
            for b in cv.iter_mut() {
                s = s.wrapping_mul(1664525).wrapping_add(1013904223);
                *b = (s >> 24) as u8;
            }
        }

        // Interleave manually: out[d*32 + v] = codes_per_vec[v][d].
        let mut interleaved = vec![0u8; dim_half * BLOCK_SIZE];
        for (v, cv) in codes_per_vec.iter().enumerate() {
            for (d, &byte) in cv.iter().enumerate() {
                interleaved[d * BLOCK_SIZE + v] = byte;
            }
        }

        let mut acc = [0u16; 32];
        fastscan_block_scalar(&interleaved, &lut, dim_half, &mut acc);

        for (v, cv) in codes_per_vec.iter().enumerate() {
            // Exact f32 ADC for this code.
            let mut exact = 0.0f32;
            for (d, &byte) in cv.iter().enumerate() {
                let lo = (byte & 0x0F) as usize;
                let hi = (byte >> 4) as usize;
                let d_lo = q[2 * d] - centroids[lo];
                let d_hi = q[2 * d + 1] - centroids[hi];
                exact += d_lo * d_lo + d_hi * d_hi;
            }

            let reconstructed = acc[v] as f32 / params.scale + params.bias;
            let err = (reconstructed - exact).abs();
            // Quantization error bound: each coordinate contributes at most
            // ~0.5/scale rounding error.
            let bound = padded_dim as f32 / params.scale;
            assert!(
                err <= bound,
                "v={v}: reconstructed {reconstructed} vs exact {exact} (err {err} > bound {bound})"
            );
        }
    }

    #[test]
    fn test_build_quantized_lut_preserves_ranking() {
        use crate::vector::turbo_quant::codebook::scaled_centroids;

        // A code identical to the query's own quantization must rank strictly
        // better than a code with every nibble maximally wrong.
        let padded_dim = 64usize;
        let dim_half = padded_dim / 2;
        let centroids = scaled_centroids(padded_dim as u32);

        // Query sitting exactly on centroid 4 in every coordinate.
        let q = vec![centroids[4]; padded_dim];
        let mut lut = vec![0u8; padded_dim * 16];
        let params = build_quantized_lut(&q, &centroids, &mut lut);

        let good_code = vec![0x44u8; dim_half]; // both nibbles = 4
        let bad_code = vec![0xFFu8; dim_half]; // both nibbles = 15 (far centroid)

        let mut interleaved = vec![0u8; dim_half * BLOCK_SIZE];
        for (d, &b) in good_code.iter().enumerate() {
            interleaved[d * BLOCK_SIZE] = b;
        }
        for (d, &b) in bad_code.iter().enumerate() {
            interleaved[d * BLOCK_SIZE + 1] = b;
        }

        let mut acc = [0u16; 32];
        fastscan_block_scalar(&interleaved, &lut, dim_half, &mut acc);

        let good = acc[0] as f32 / params.scale + params.bias;
        let bad = acc[1] as f32 / params.scale + params.bias;
        assert!(
            good < bad,
            "exact-match code ({good}) must rank before far code ({bad})"
        );
        // The exact-match code's residual is 0 in every coordinate, so its
        // accumulator must be exactly 0 (all its LUT entries are per-coord minima).
        assert_eq!(acc[0], 0, "exact-match accumulator must be zero");
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

    #[test]
    fn test_fastscan_dispatch_matches_scalar_full_range() {
        // Whatever kernel the dispatch selected on this machine must be
        // bit-identical to the scalar reference across the full LUT range.
        init_fastscan();
        let d = fastscan_dispatch();

        for (dim_half, seed) in [(3usize, 5u32), (64, 21), (200, 77)] {
            let (codes, lut) = make_random_block(dim_half, seed);

            let mut scalar_results = [0u16; 32];
            fastscan_block_scalar(&codes, &lut, dim_half, &mut scalar_results);

            let mut dispatch_results = [0u16; 32];
            (d.scan_block)(&codes, &lut, dim_half, &mut dispatch_results);

            assert_eq!(
                dispatch_results, scalar_results,
                "dispatch vs scalar mismatch (dim_half={dim_half})"
            );
        }
    }
}
