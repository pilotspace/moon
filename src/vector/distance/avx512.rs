//! AVX-512 distance kernels with 2x loop unrolling.
//!
//! All functions require AVX-512F at minimum. The i8 L2 kernel uses
//! `avx512bw` for byte-width operations. VNNI (`_mm512_dpwssd_epi32`) is not
//! yet stabilized in `core::arch::x86_64`, so we use the portable
//! `cvtepi8_epi16` + `madd_epi16` widening approach instead.
//!
//! The caller (DistanceTable init) verifies AVX-512F via
//! `is_x86_feature_detected!` before installing these function pointers.

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

// ── Distance kernels ────────────────────────────────────────────────────

/// Squared L2 distance for f32 vectors (AVX-512F, 2x unrolled).
///
/// Processes 32 floats per iteration (2 x 16-lane __m512).
/// Uses `_mm512_reduce_add_ps` for horizontal reduction.
///
/// # Safety
/// Caller must ensure AVX-512F CPU feature is available.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx512f")]
pub unsafe fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "l2_f32: dimension mismatch");

    let n = a.len();
    let mut sum0 = _mm512_setzero_ps();
    let mut sum1 = _mm512_setzero_ps();

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32.
        // Pointers are valid f32 slices. Using unaligned loads.
        let a0 = _mm512_loadu_ps(pa.add(i));
        let b0 = _mm512_loadu_ps(pb.add(i));
        let d0 = _mm512_sub_ps(a0, b0);
        sum0 = _mm512_fmadd_ps(d0, d0, sum0);

        let a1 = _mm512_loadu_ps(pa.add(i + 16));
        let b1 = _mm512_loadu_ps(pb.add(i + 16));
        let d1 = _mm512_sub_ps(a1, b1);
        sum1 = _mm512_fmadd_ps(d1, d1, sum1);

        i += 32;
    }

    sum0 = _mm512_add_ps(sum0, sum1);

    // SAFETY: _mm512_reduce_add_ps requires AVX-512F, verified via target_feature.
    let mut result = _mm512_reduce_add_ps(sum0);

    // Scalar tail for remaining elements
    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        result += d * d;
        i += 1;
    }

    result
}

/// Squared L2 distance for i8 vectors (AVX-512BW).
///
/// Uses `_mm512_cvtepi8_epi16` widening + `_mm512_madd_epi16` for squared
/// differences accumulated as i32. Processes 32 i8 elements per iteration.
///
/// Note: VNNI `_mm512_dpwssd_epi32` is not yet stabilized in `core::arch`,
/// so we use the portable widening approach instead. When VNNI intrinsics
/// stabilize, this can be upgraded for ~2x throughput on Ice Lake+.
///
/// # Safety
/// Caller must ensure AVX-512F and AVX-512BW CPU features are available.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
pub unsafe fn l2_i8_vnni(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len(), "l2_i8_vnni: dimension mismatch");

    let n = a.len();
    let mut acc = _mm512_setzero_si512();

    let pa = a.as_ptr() as *const u8;
    let pb = b.as_ptr() as *const u8;

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32.
        // Load 32 bytes (256 bits) then widen to 512-bit i16.
        let a_256 = _mm256_loadu_si256(pa.add(i) as *const __m256i);
        let b_256 = _mm256_loadu_si256(pb.add(i) as *const __m256i);

        // Widen i8 -> i16 (sign-extend)
        let a_16 = _mm512_cvtepi8_epi16(a_256);
        let b_16 = _mm512_cvtepi8_epi16(b_256);

        // Subtract in i16
        let diff = _mm512_sub_epi16(a_16, b_16);

        // madd_epi16: multiply adjacent i16 pairs, add as i32
        let sq = _mm512_madd_epi16(diff, diff);
        acc = _mm512_add_epi32(acc, sq);

        i += 32;
    }

    // SAFETY: _mm512_reduce_add_epi32 requires AVX-512F, verified via target_feature.
    let mut result = _mm512_reduce_add_epi32(acc);

    // Scalar tail
    while i < n {
        let d = *a.get_unchecked(i) as i32 - *b.get_unchecked(i) as i32;
        result += d * d;
        i += 1;
    }

    result
}

/// Dot product for f32 vectors (AVX-512F, 2x unrolled).
///
/// # Safety
/// Caller must ensure AVX-512F CPU feature is available.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx512f")]
pub unsafe fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "dot_f32: dimension mismatch");

    let n = a.len();
    let mut sum0 = _mm512_setzero_ps();
    let mut sum1 = _mm512_setzero_ps();

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32.
        let a0 = _mm512_loadu_ps(pa.add(i));
        let b0 = _mm512_loadu_ps(pb.add(i));
        sum0 = _mm512_fmadd_ps(a0, b0, sum0);

        let a1 = _mm512_loadu_ps(pa.add(i + 16));
        let b1 = _mm512_loadu_ps(pb.add(i + 16));
        sum1 = _mm512_fmadd_ps(a1, b1, sum1);

        i += 32;
    }

    sum0 = _mm512_add_ps(sum0, sum1);

    // SAFETY: _mm512_reduce_add_ps requires AVX-512F, verified via target_feature.
    let mut result = _mm512_reduce_add_ps(sum0);

    // Scalar tail
    while i < n {
        result += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }

    result
}

/// Cosine distance for f32 vectors (AVX-512F).
///
/// Computes `1.0 - dot(a,b) / (||a|| * ||b||)` in a single pass.
/// Returns 1.0 if either vector has zero norm.
///
/// # Safety
/// Caller must ensure AVX-512F CPU feature is available.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx512f")]
pub unsafe fn cosine_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_f32: dimension mismatch");

    let n = a.len();
    let mut dot0 = _mm512_setzero_ps();
    let mut dot1 = _mm512_setzero_ps();
    let mut na0 = _mm512_setzero_ps();
    let mut na1 = _mm512_setzero_ps();
    let mut nb0 = _mm512_setzero_ps();
    let mut nb1 = _mm512_setzero_ps();

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32.
        let a0 = _mm512_loadu_ps(pa.add(i));
        let b0 = _mm512_loadu_ps(pb.add(i));
        dot0 = _mm512_fmadd_ps(a0, b0, dot0);
        na0 = _mm512_fmadd_ps(a0, a0, na0);
        nb0 = _mm512_fmadd_ps(b0, b0, nb0);

        let a1 = _mm512_loadu_ps(pa.add(i + 16));
        let b1 = _mm512_loadu_ps(pb.add(i + 16));
        dot1 = _mm512_fmadd_ps(a1, b1, dot1);
        na1 = _mm512_fmadd_ps(a1, a1, na1);
        nb1 = _mm512_fmadd_ps(b1, b1, nb1);

        i += 32;
    }

    dot0 = _mm512_add_ps(dot0, dot1);
    na0 = _mm512_add_ps(na0, na1);
    nb0 = _mm512_add_ps(nb0, nb1);

    // SAFETY: _mm512_reduce_add_ps requires AVX-512F, verified via target_feature.
    let mut dot_sum = _mm512_reduce_add_ps(dot0);
    let mut norm_a_sq = _mm512_reduce_add_ps(na0);
    let mut norm_b_sq = _mm512_reduce_add_ps(nb0);

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

/// SQ8 ADC (HQ-2) per-candidate statistics (AVX-512F): `(Σ(q_i·c_i), Σc_i, Σc_i²)`.
///
/// Widens 32 u8 codes per iteration to f32 (`VPMOVZXBD` via
/// `_mm512_cvtepu8_epi32`, 16 lanes/call, 2x unrolled, then
/// `_mm512_cvtepi32_ps`) and FMAs against the f32 query. See
/// `turbo_quant::sq8` module docs for how these three running sums combine
/// (O(1), architecture-independent) into the final ADC distance.
///
/// # Safety
/// Caller must ensure AVX-512F CPU feature is available.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx512f")]
pub unsafe fn sq8_stats(query: &[f32], codes: &[u8]) -> (f32, f32, f32) {
    debug_assert_eq!(query.len(), codes.len(), "sq8_stats: dimension mismatch");

    let n = query.len();
    let mut dot0 = _mm512_setzero_ps();
    let mut dot1 = _mm512_setzero_ps();
    let mut sumc0 = _mm512_setzero_ps();
    let mut sumc1 = _mm512_setzero_ps();
    let mut sumsqc0 = _mm512_setzero_ps();
    let mut sumsqc1 = _mm512_setzero_ps();

    let pq = query.as_ptr();
    let pc = codes.as_ptr();

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32. `_mm_loadu_si128`
        // reads 16 bytes (unaligned); `_mm512_cvtepu8_epi32` zero-extends all
        // 16 lanes to i32, which `_mm512_cvtepi32_ps` converts exactly (u8
        // values 0..=255 have exact f32 representations).
        let c0_u8 = _mm_loadu_si128(pc.add(i) as *const __m128i);
        let c0_f32 = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(c0_u8));
        let q0 = _mm512_loadu_ps(pq.add(i));
        dot0 = _mm512_fmadd_ps(q0, c0_f32, dot0);
        sumc0 = _mm512_add_ps(sumc0, c0_f32);
        sumsqc0 = _mm512_fmadd_ps(c0_f32, c0_f32, sumsqc0);

        let c1_u8 = _mm_loadu_si128(pc.add(i + 16) as *const __m128i);
        let c1_f32 = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(c1_u8));
        let q1 = _mm512_loadu_ps(pq.add(i + 16));
        dot1 = _mm512_fmadd_ps(q1, c1_f32, dot1);
        sumc1 = _mm512_add_ps(sumc1, c1_f32);
        sumsqc1 = _mm512_fmadd_ps(c1_f32, c1_f32, sumsqc1);

        i += 32;
    }

    dot0 = _mm512_add_ps(dot0, dot1);
    sumc0 = _mm512_add_ps(sumc0, sumc1);
    sumsqc0 = _mm512_add_ps(sumsqc0, sumsqc1);

    // SAFETY: _mm512_reduce_add_ps requires AVX-512F, verified via target_feature.
    let mut dot_sum = _mm512_reduce_add_ps(dot0);
    let mut sum_c_sum = _mm512_reduce_add_ps(sumc0);
    let mut sumsq_c_sum = _mm512_reduce_add_ps(sumsqc0);

    // Scalar tail — safe indexing; bounds-checked slices cost nothing here
    // (at most one sub-vector-width pass) and keep the unsafe surface to the
    // intrinsics above (UNSAFE_POLICY).
    for (&c, &qv) in codes[i..n].iter().zip(query[i..n].iter()) {
        let cf = c as f32;
        dot_sum += qv * cf;
        sum_c_sum += cf;
        sumsq_c_sum += cf * cf;
    }

    (dot_sum, sum_c_sum, sumsq_c_sum)
}

#[cfg(test)]
#[cfg(target_arch = "x86_64")]
mod tests {
    use super::*;
    use crate::vector::distance::scalar;

    fn gen_f32(len: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(len);
        let mut s = seed;
        for _ in 0..len {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn gen_i8(len: usize, seed: u32) -> Vec<i8> {
        let mut v = Vec::with_capacity(len);
        let mut s = seed;
        for _ in 0..len {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s >> 24) as i8);
        }
        v
    }

    fn has_avx512f() -> bool {
        is_x86_feature_detected!("avx512f")
    }

    fn has_avx512bw() -> bool {
        is_x86_feature_detected!("avx512bw")
    }

    #[test]
    fn test_l2_f32_matches_scalar() {
        if !has_avx512f() {
            return;
        }
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::l2_f32(&a, &b);
        // SAFETY: AVX-512F verified above.
        let got = unsafe { l2_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-4,
            "l2_f32 mismatch: scalar={expected}, avx512={got}, rel={rel}"
        );
    }

    #[test]
    fn test_l2_i8_matches_scalar() {
        if !has_avx512f() || !has_avx512bw() {
            return;
        }
        let a = gen_i8(768, 42);
        let b = gen_i8(768, 99);
        let expected = scalar::l2_i8(&a, &b);
        // SAFETY: AVX-512F + AVX-512BW verified above.
        let got = unsafe { l2_i8_vnni(&a, &b) };
        assert_eq!(
            got, expected,
            "l2_i8 mismatch: scalar={expected}, avx512={got}"
        );
    }

    #[test]
    fn test_dot_f32_matches_scalar() {
        if !has_avx512f() {
            return;
        }
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::dot_f32(&a, &b);
        // SAFETY: AVX-512F verified above.
        let got = unsafe { dot_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-4,
            "dot_f32 mismatch: scalar={expected}, avx512={got}, rel={rel}"
        );
    }

    #[test]
    fn test_cosine_f32_matches_scalar() {
        if !has_avx512f() {
            return;
        }
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::cosine_f32(&a, &b);
        // SAFETY: AVX-512F verified above.
        let got = unsafe { cosine_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-3,
            "cosine_f32 mismatch: scalar={expected}, avx512={got}, rel={rel}"
        );
    }

    #[test]
    fn test_tail_handling() {
        if !has_avx512f() {
            return;
        }
        for len in [1, 3, 7, 13, 15, 17, 31, 33, 100] {
            let a = gen_f32(len, 42);
            let b = gen_f32(len, 99);

            let expected_l2 = scalar::l2_f32(&a, &b);
            // SAFETY: AVX-512F verified above.
            let got_l2 = unsafe { l2_f32(&a, &b) };
            let rel = (got_l2 - expected_l2).abs() / expected_l2.abs().max(1e-10);
            assert!(
                rel < 1e-4,
                "l2 tail len={len}: scalar={expected_l2}, avx512={got_l2}"
            );

            let expected_dot = scalar::dot_f32(&a, &b);
            // SAFETY: AVX-512 feature is enabled via target_feature on the caller;
            // slices a and b have equal length.
            let got_dot = unsafe { dot_f32(&a, &b) };
            let rel = (got_dot - expected_dot).abs() / expected_dot.abs().max(1e-10);
            assert!(
                rel < 1e-4,
                "dot tail len={len}: scalar={expected_dot}, avx512={got_dot}"
            );
        }
    }

    // ── HQ-2: SQ8 ADC stats (AVX-512 vs scalar reference) ───────────────

    fn gen_u8(len: usize, seed: u32) -> Vec<u8> {
        let mut v = Vec::with_capacity(len);
        let mut s = seed;
        for _ in 0..len {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s >> 24) as u8);
        }
        v
    }

    #[test]
    fn test_sq8_stats_matches_scalar() {
        if !has_avx512f() {
            return;
        }
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar;
        let q = gen_f32(768, 42);
        let c = gen_u8(768, 99);
        let expected = sq8_candidate_stats_scalar(&q, &c);
        // SAFETY: AVX-512F verified above.
        let got = unsafe { sq8_stats(&q, &c) };
        let rel_dot = (got.0 - expected.0).abs() / expected.0.abs().max(1.0);
        let rel_sum = (got.1 - expected.1).abs() / expected.1.abs().max(1.0);
        let rel_sq = (got.2 - expected.2).abs() / expected.2.abs().max(1.0);
        assert!(
            rel_dot < 1e-3 && rel_sum < 1e-3 && rel_sq < 1e-3,
            "sq8_stats mismatch: scalar={expected:?}, avx512={got:?}"
        );
    }

    #[test]
    fn test_sq8_stats_tail_handling() {
        if !has_avx512f() {
            return;
        }
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar;
        for len in [0, 1, 3, 7, 13, 15, 31, 32, 33, 63, 100] {
            let q = gen_f32(len, 42);
            let c = gen_u8(len, 99);
            let expected = sq8_candidate_stats_scalar(&q, &c);
            // SAFETY: AVX-512F verified above.
            let got = unsafe { sq8_stats(&q, &c) };
            let rel_dot = (got.0 - expected.0).abs() / expected.0.abs().max(1.0);
            let rel_sum = (got.1 - expected.1).abs() / expected.1.abs().max(1.0);
            let rel_sq = (got.2 - expected.2).abs() / expected.2.abs().max(1.0);
            assert!(
                rel_dot < 1e-3 && rel_sum < 1e-3 && rel_sq < 1e-3,
                "sq8_stats tail len={len}: scalar={expected:?}, avx512={got:?}"
            );
        }
    }
}
