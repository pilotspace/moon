//! AVX2 + FMA distance kernels with 4x loop unrolling.
//!
//! All functions require AVX2 and FMA CPU features. The caller (DistanceTable
//! init) verifies these via `is_x86_feature_detected!` before installing the
//! function pointers.

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

// ── Horizontal reduction helpers ────────────────────────────────────────

/// Horizontal sum of 8 packed f32 lanes in a `__m256`.
///
/// Reduces 8 floats to a single scalar: extract high 128, add to low 128,
/// then shuffle-add within the remaining 4 lanes.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[target_feature(enable = "avx2,fma")]
unsafe fn hsum_f32_avx2(v: __m256) -> f32 {
    // SAFETY: Caller guarantees AVX2 is available via target_feature.
    let hi128 = _mm256_extractf128_ps(v, 1);
    let lo128 = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(lo128, hi128);
    let shuf = _mm_movehdup_ps(sum128); // [1,1,3,3]
    let sums = _mm_add_ps(sum128, shuf); // [0+1, -, 2+3, -]
    let shuf2 = _mm_movehl_ps(sums, sums); // [2+3, -, -, -]
    let result = _mm_add_ss(sums, shuf2);
    _mm_cvtss_f32(result)
}

/// Horizontal sum of 8 packed i32 lanes in a `__m256i`.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[target_feature(enable = "avx2,fma")]
unsafe fn hsum_i32_avx2(v: __m256i) -> i32 {
    // SAFETY: Caller guarantees AVX2 is available via target_feature.
    let hi128 = _mm256_extracti128_si256(v, 1);
    let lo128 = _mm256_castsi256_si128(v);
    let sum128 = _mm_add_epi32(lo128, hi128);
    let shuf = _mm_shuffle_epi32(sum128, 0b_00_11_00_01); // swap pairs
    let sums = _mm_add_epi32(sum128, shuf);
    let shuf2 = _mm_shuffle_epi32(sums, 0b_00_00_00_10); // move lane 2 to 0
    let result = _mm_add_epi32(sums, shuf2);
    _mm_cvtsi128_si32(result)
}

// ── Distance kernels ────────────────────────────────────────────────────

/// Squared L2 distance for f32 vectors (AVX2+FMA, 4x unrolled).
///
/// Processes 32 floats per iteration (4 x 8-lane __m256).
/// Scalar tail loop handles remaining elements.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,fma")]
pub unsafe fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "l2_f32: dimension mismatch");

    let n = a.len();
    let mut sum0 = _mm256_setzero_ps();
    let mut sum1 = _mm256_setzero_ps();
    let mut sum2 = _mm256_setzero_ps();
    let mut sum3 = _mm256_setzero_ps();

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32.
        // Pointers are valid f32 slices. Using unaligned loads.
        let a0 = _mm256_loadu_ps(pa.add(i));
        let b0 = _mm256_loadu_ps(pb.add(i));
        let d0 = _mm256_sub_ps(a0, b0);
        sum0 = _mm256_fmadd_ps(d0, d0, sum0);

        let a1 = _mm256_loadu_ps(pa.add(i + 8));
        let b1 = _mm256_loadu_ps(pb.add(i + 8));
        let d1 = _mm256_sub_ps(a1, b1);
        sum1 = _mm256_fmadd_ps(d1, d1, sum1);

        let a2 = _mm256_loadu_ps(pa.add(i + 16));
        let b2 = _mm256_loadu_ps(pb.add(i + 16));
        let d2 = _mm256_sub_ps(a2, b2);
        sum2 = _mm256_fmadd_ps(d2, d2, sum2);

        let a3 = _mm256_loadu_ps(pa.add(i + 24));
        let b3 = _mm256_loadu_ps(pb.add(i + 24));
        let d3 = _mm256_sub_ps(a3, b3);
        sum3 = _mm256_fmadd_ps(d3, d3, sum3);

        i += 32;
    }

    // Reduce 4 accumulators into one
    sum0 = _mm256_add_ps(sum0, sum1);
    sum2 = _mm256_add_ps(sum2, sum3);
    sum0 = _mm256_add_ps(sum0, sum2);

    // SAFETY: hsum_f32_avx2 requires AVX2, which we have via target_feature.
    let mut result = hsum_f32_avx2(sum0);

    // Scalar tail for remaining elements
    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        result += d * d;
        i += 1;
    }

    result
}

/// Squared L2 distance for i8 vectors (AVX2).
///
/// Widens i8 to i16, subtracts, then uses `madd_epi16` to compute sum of
/// squared differences as i32. Processes 32 i8 elements per iteration.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,fma")]
pub unsafe fn l2_i8(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len(), "l2_i8: dimension mismatch");

    let n = a.len();
    let mut acc = _mm256_setzero_si256();

    let pa = a.as_ptr() as *const u8;
    let pb = b.as_ptr() as *const u8;

    let chunks = n / 16;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16.
        // Loading 16 bytes (128 bits) then widening to 256-bit i16.
        let a_128 = _mm_loadu_si128(pa.add(i) as *const __m128i);
        let b_128 = _mm_loadu_si128(pb.add(i) as *const __m128i);

        // Widen i8 -> i16 (sign-extend)
        let a_16 = _mm256_cvtepi8_epi16(a_128);
        let b_16 = _mm256_cvtepi8_epi16(b_128);

        // diff in i16
        let diff = _mm256_sub_epi16(a_16, b_16);

        // madd_epi16: multiply adjacent i16 pairs, accumulate as i32
        // diff[0]*diff[0] + diff[1]*diff[1] in each i32 lane
        let sq = _mm256_madd_epi16(diff, diff);
        acc = _mm256_add_epi32(acc, sq);

        i += 16;
    }

    // SAFETY: hsum_i32_avx2 requires AVX2, which we have via target_feature.
    let mut result = hsum_i32_avx2(acc);

    // Scalar tail
    while i < n {
        let d = *a.get_unchecked(i) as i32 - *b.get_unchecked(i) as i32;
        result += d * d;
        i += 1;
    }

    result
}

/// Dot product for f32 vectors (AVX2+FMA, 4x unrolled).
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,fma")]
pub unsafe fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "dot_f32: dimension mismatch");

    let n = a.len();
    let mut sum0 = _mm256_setzero_ps();
    let mut sum1 = _mm256_setzero_ps();
    let mut sum2 = _mm256_setzero_ps();
    let mut sum3 = _mm256_setzero_ps();

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32.
        let a0 = _mm256_loadu_ps(pa.add(i));
        let b0 = _mm256_loadu_ps(pb.add(i));
        sum0 = _mm256_fmadd_ps(a0, b0, sum0);

        let a1 = _mm256_loadu_ps(pa.add(i + 8));
        let b1 = _mm256_loadu_ps(pb.add(i + 8));
        sum1 = _mm256_fmadd_ps(a1, b1, sum1);

        let a2 = _mm256_loadu_ps(pa.add(i + 16));
        let b2 = _mm256_loadu_ps(pb.add(i + 16));
        sum2 = _mm256_fmadd_ps(a2, b2, sum2);

        let a3 = _mm256_loadu_ps(pa.add(i + 24));
        let b3 = _mm256_loadu_ps(pb.add(i + 24));
        sum3 = _mm256_fmadd_ps(a3, b3, sum3);

        i += 32;
    }

    sum0 = _mm256_add_ps(sum0, sum1);
    sum2 = _mm256_add_ps(sum2, sum3);
    sum0 = _mm256_add_ps(sum0, sum2);

    // SAFETY: hsum_f32_avx2 requires AVX2, which we have via target_feature.
    let mut result = hsum_f32_avx2(sum0);

    // Scalar tail
    while i < n {
        result += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }

    result
}

/// Cosine distance for f32 vectors (AVX2+FMA).
///
/// Computes `1.0 - dot(a,b) / (||a|| * ||b||)` in a single pass.
/// Returns 1.0 if either vector has zero norm.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,fma")]
pub unsafe fn cosine_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_f32: dimension mismatch");

    let n = a.len();
    let mut dot0 = _mm256_setzero_ps();
    let mut dot1 = _mm256_setzero_ps();
    let mut na0 = _mm256_setzero_ps();
    let mut na1 = _mm256_setzero_ps();
    let mut nb0 = _mm256_setzero_ps();
    let mut nb1 = _mm256_setzero_ps();

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 16;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16.
        let a0 = _mm256_loadu_ps(pa.add(i));
        let b0 = _mm256_loadu_ps(pb.add(i));
        dot0 = _mm256_fmadd_ps(a0, b0, dot0);
        na0 = _mm256_fmadd_ps(a0, a0, na0);
        nb0 = _mm256_fmadd_ps(b0, b0, nb0);

        let a1 = _mm256_loadu_ps(pa.add(i + 8));
        let b1 = _mm256_loadu_ps(pb.add(i + 8));
        dot1 = _mm256_fmadd_ps(a1, b1, dot1);
        na1 = _mm256_fmadd_ps(a1, a1, na1);
        nb1 = _mm256_fmadd_ps(b1, b1, nb1);

        i += 16;
    }

    dot0 = _mm256_add_ps(dot0, dot1);
    na0 = _mm256_add_ps(na0, na1);
    nb0 = _mm256_add_ps(nb0, nb1);

    // SAFETY: hsum_f32_avx2 requires AVX2, which we have via target_feature.
    let mut dot_sum = hsum_f32_avx2(dot0);
    let mut norm_a_sq = hsum_f32_avx2(na0);
    let mut norm_b_sq = hsum_f32_avx2(nb0);

    // Scalar tail
    while i < n {
        let av = *a.get_unchecked(i);
        let bv = *b.get_unchecked(i);
        dot_sum += av * bv;
        norm_a_sq += av * av;
        norm_b_sq += bv * bv;
        i += 1;
    }

    let norm_a = norm_a_sq.sqrt();
    let norm_b = norm_b_sq.sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    1.0 - dot_sum / (norm_a * norm_b)
}

#[cfg(test)]
#[cfg(target_arch = "x86_64")]
mod tests {
    use super::*;
    use crate::vector::distance::scalar;

    /// Generate deterministic f32 vector of given length.
    fn gen_f32(len: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(len);
        let mut s = seed;
        for _ in 0..len {
            // Simple LCG for determinism
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    /// Generate deterministic i8 vector of given length.
    fn gen_i8(len: usize, seed: u32) -> Vec<i8> {
        let mut v = Vec::with_capacity(len);
        let mut s = seed;
        for _ in 0..len {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s >> 24) as i8);
        }
        v
    }

    fn has_avx2_fma() -> bool {
        is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma")
    }

    #[test]
    fn test_l2_f32_matches_scalar() {
        if !has_avx2_fma() {
            return;
        }
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::l2_f32(&a, &b);
        // SAFETY: AVX2+FMA verified above.
        let got = unsafe { l2_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-4,
            "l2_f32 mismatch: scalar={expected}, avx2={got}, rel={rel}"
        );
    }

    #[test]
    fn test_l2_i8_matches_scalar() {
        if !has_avx2_fma() {
            return;
        }
        let a = gen_i8(768, 42);
        let b = gen_i8(768, 99);
        let expected = scalar::l2_i8(&a, &b);
        // SAFETY: AVX2+FMA verified above.
        let got = unsafe { l2_i8(&a, &b) };
        assert_eq!(
            got, expected,
            "l2_i8 mismatch: scalar={expected}, avx2={got}"
        );
    }

    #[test]
    fn test_dot_f32_matches_scalar() {
        if !has_avx2_fma() {
            return;
        }
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::dot_f32(&a, &b);
        // SAFETY: AVX2+FMA verified above.
        let got = unsafe { dot_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-4,
            "dot_f32 mismatch: scalar={expected}, avx2={got}, rel={rel}"
        );
    }

    #[test]
    fn test_cosine_f32_matches_scalar() {
        if !has_avx2_fma() {
            return;
        }
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::cosine_f32(&a, &b);
        // SAFETY: AVX2+FMA verified above.
        let got = unsafe { cosine_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-3,
            "cosine_f32 mismatch: scalar={expected}, avx2={got}, rel={rel}"
        );
    }

    #[test]
    fn test_tail_handling() {
        if !has_avx2_fma() {
            return;
        }
        for len in [1, 3, 7, 13, 15, 17, 31, 33, 100] {
            let a = gen_f32(len, 42);
            let b = gen_f32(len, 99);

            let expected_l2 = scalar::l2_f32(&a, &b);
            // SAFETY: AVX2+FMA verified above.
            let got_l2 = unsafe { l2_f32(&a, &b) };
            let rel = (got_l2 - expected_l2).abs() / expected_l2.abs().max(1e-10);
            assert!(
                rel < 1e-4,
                "l2 tail len={len}: scalar={expected_l2}, avx2={got_l2}"
            );

            let expected_dot = scalar::dot_f32(&a, &b);
            // SAFETY: AVX2+FMA verified at test entry.
            let got_dot = unsafe { dot_f32(&a, &b) };
            let rel = (got_dot - expected_dot).abs() / expected_dot.abs().max(1e-10);
            assert!(
                rel < 1e-4,
                "dot tail len={len}: scalar={expected_dot}, avx2={got_dot}"
            );

            let ai = gen_i8(len, 42);
            let bi = gen_i8(len, 99);
            let expected_i8 = scalar::l2_i8(&ai, &bi);
            // SAFETY: AVX2+FMA verified at test entry.
            let got_i8 = unsafe { l2_i8(&ai, &bi) };
            assert_eq!(got_i8, expected_i8, "l2_i8 tail len={len}");
        }
    }

    #[test]
    fn test_empty_vectors() {
        if !has_avx2_fma() {
            return;
        }
        let a: &[f32] = &[];
        let b: &[f32] = &[];
        // SAFETY: AVX2+FMA verified above.
        unsafe {
            assert_eq!(l2_f32(a, b), 0.0);
            assert_eq!(dot_f32(a, b), 0.0);
            assert_eq!(cosine_f32(a, b), 1.0);
        }

        let ai: &[i8] = &[];
        let bi: &[i8] = &[];
        // SAFETY: AVX2+FMA verified above.
        unsafe {
            assert_eq!(l2_i8(ai, bi), 0);
        }
    }
}
