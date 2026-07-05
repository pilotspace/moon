//! ARM NEON distance kernels with 4x loop unrolling.
//!
//! All functions require AArch64 NEON (baseline on all AArch64 CPUs).
//! The caller (DistanceTable init) installs these on `aarch64` targets.

#[cfg(target_arch = "aarch64")]
use core::arch::aarch64::*;

// ── Distance kernels ────────────────────────────────────────────────────

/// Squared L2 distance for f32 vectors (NEON, 4x unrolled).
///
/// Processes 16 floats per iteration (4 x 4-lane float32x4_t).
/// Uses `vfmaq_f32` for fused multiply-add and `vaddvq_f32` for horizontal sum.
///
/// # Safety
/// Caller must ensure the CPU supports NEON (baseline on all AArch64).
#[cfg(target_arch = "aarch64")]
#[inline]
#[target_feature(enable = "neon")]
pub unsafe fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "l2_f32: dimension mismatch");

    let n = a.len();
    let mut sum0 = vdupq_n_f32(0.0);
    let mut sum1 = vdupq_n_f32(0.0);
    let mut sum2 = vdupq_n_f32(0.0);
    let mut sum3 = vdupq_n_f32(0.0);

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 16;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16.
        // Pointers are valid f32 slices.
        let a0 = vld1q_f32(pa.add(i));
        let b0 = vld1q_f32(pb.add(i));
        let d0 = vsubq_f32(a0, b0);
        sum0 = vfmaq_f32(sum0, d0, d0);

        let a1 = vld1q_f32(pa.add(i + 4));
        let b1 = vld1q_f32(pb.add(i + 4));
        let d1 = vsubq_f32(a1, b1);
        sum1 = vfmaq_f32(sum1, d1, d1);

        let a2 = vld1q_f32(pa.add(i + 8));
        let b2 = vld1q_f32(pb.add(i + 8));
        let d2 = vsubq_f32(a2, b2);
        sum2 = vfmaq_f32(sum2, d2, d2);

        let a3 = vld1q_f32(pa.add(i + 12));
        let b3 = vld1q_f32(pb.add(i + 12));
        let d3 = vsubq_f32(a3, b3);
        sum3 = vfmaq_f32(sum3, d3, d3);

        i += 16;
    }

    // Reduce 4 accumulators
    sum0 = vaddq_f32(sum0, sum1);
    sum2 = vaddq_f32(sum2, sum3);
    sum0 = vaddq_f32(sum0, sum2);

    // SAFETY: vaddvq_f32 requires NEON, which we have via target_feature.
    let mut result = vaddvq_f32(sum0);

    // Scalar tail
    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        result += d * d;
        i += 1;
    }

    result
}

/// Squared L2 distance for i8 vectors (NEON).
///
/// Widens i8 to i16 via `vmovl_s8`, subtracts, then uses `vmlal_s16`
/// to accumulate squared differences as i32. Processes 16 i8 per iteration.
///
/// # Safety
/// Caller must ensure the CPU supports NEON (baseline on all AArch64).
#[cfg(target_arch = "aarch64")]
#[inline]
#[target_feature(enable = "neon")]
pub unsafe fn l2_i8(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len(), "l2_i8: dimension mismatch");

    let n = a.len();
    let mut acc = vdupq_n_s32(0);

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 16;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16.
        let a_vec = vld1q_s8(pa.add(i));
        let b_vec = vld1q_s8(pb.add(i));

        // Low half: first 8 i8 elements
        let a_lo = vget_low_s8(a_vec);
        let b_lo = vget_low_s8(b_vec);
        let a16_lo = vmovl_s8(a_lo);
        let b16_lo = vmovl_s8(b_lo);
        let diff_lo = vsubq_s16(a16_lo, b16_lo);

        // Squared accumulate low: split to 4-lane halves for vmlal_s16
        let diff_lo_lo = vget_low_s16(diff_lo);
        let diff_lo_hi = vget_high_s16(diff_lo);
        acc = vmlal_s16(acc, diff_lo_lo, diff_lo_lo);
        acc = vmlal_s16(acc, diff_lo_hi, diff_lo_hi);

        // High half: last 8 i8 elements
        let a_hi = vget_high_s8(a_vec);
        let b_hi = vget_high_s8(b_vec);
        let a16_hi = vmovl_s8(a_hi);
        let b16_hi = vmovl_s8(b_hi);
        let diff_hi = vsubq_s16(a16_hi, b16_hi);

        let diff_hi_lo = vget_low_s16(diff_hi);
        let diff_hi_hi = vget_high_s16(diff_hi);
        acc = vmlal_s16(acc, diff_hi_lo, diff_hi_lo);
        acc = vmlal_s16(acc, diff_hi_hi, diff_hi_hi);

        i += 16;
    }

    // SAFETY: vaddvq_s32 requires NEON, which we have via target_feature.
    let mut result = vaddvq_s32(acc);

    // Scalar tail
    while i < n {
        let d = *a.get_unchecked(i) as i32 - *b.get_unchecked(i) as i32;
        result += d * d;
        i += 1;
    }

    result
}

/// Dot product for f32 vectors (NEON, 4x unrolled).
///
/// # Safety
/// Caller must ensure the CPU supports NEON (baseline on all AArch64).
#[cfg(target_arch = "aarch64")]
#[inline]
#[target_feature(enable = "neon")]
pub unsafe fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "dot_f32: dimension mismatch");

    let n = a.len();
    let mut sum0 = vdupq_n_f32(0.0);
    let mut sum1 = vdupq_n_f32(0.0);
    let mut sum2 = vdupq_n_f32(0.0);
    let mut sum3 = vdupq_n_f32(0.0);

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 16;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16.
        let a0 = vld1q_f32(pa.add(i));
        let b0 = vld1q_f32(pb.add(i));
        sum0 = vfmaq_f32(sum0, a0, b0);

        let a1 = vld1q_f32(pa.add(i + 4));
        let b1 = vld1q_f32(pb.add(i + 4));
        sum1 = vfmaq_f32(sum1, a1, b1);

        let a2 = vld1q_f32(pa.add(i + 8));
        let b2 = vld1q_f32(pb.add(i + 8));
        sum2 = vfmaq_f32(sum2, a2, b2);

        let a3 = vld1q_f32(pa.add(i + 12));
        let b3 = vld1q_f32(pb.add(i + 12));
        sum3 = vfmaq_f32(sum3, a3, b3);

        i += 16;
    }

    sum0 = vaddq_f32(sum0, sum1);
    sum2 = vaddq_f32(sum2, sum3);
    sum0 = vaddq_f32(sum0, sum2);

    // SAFETY: vaddvq_f32 requires NEON, which we have via target_feature.
    let mut result = vaddvq_f32(sum0);

    // Scalar tail
    while i < n {
        result += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }

    result
}

/// Cosine distance for f32 vectors (NEON).
///
/// Computes `1.0 - dot(a,b) / (||a|| * ||b||)` in a single pass.
/// Returns 1.0 if either vector has zero norm.
///
/// # Safety
/// Caller must ensure the CPU supports NEON (baseline on all AArch64).
#[cfg(target_arch = "aarch64")]
#[inline]
#[target_feature(enable = "neon")]
pub unsafe fn cosine_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_f32: dimension mismatch");

    let n = a.len();
    let mut dot0 = vdupq_n_f32(0.0);
    let mut dot1 = vdupq_n_f32(0.0);
    let mut na0 = vdupq_n_f32(0.0);
    let mut na1 = vdupq_n_f32(0.0);
    let mut nb0 = vdupq_n_f32(0.0);
    let mut nb1 = vdupq_n_f32(0.0);

    let pa = a.as_ptr();
    let pb = b.as_ptr();

    let chunks = n / 8;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 8 <= n guaranteed by chunks = n / 8.
        let a0 = vld1q_f32(pa.add(i));
        let b0 = vld1q_f32(pb.add(i));
        dot0 = vfmaq_f32(dot0, a0, b0);
        na0 = vfmaq_f32(na0, a0, a0);
        nb0 = vfmaq_f32(nb0, b0, b0);

        let a1 = vld1q_f32(pa.add(i + 4));
        let b1 = vld1q_f32(pb.add(i + 4));
        dot1 = vfmaq_f32(dot1, a1, b1);
        na1 = vfmaq_f32(na1, a1, a1);
        nb1 = vfmaq_f32(nb1, b1, b1);

        i += 8;
    }

    dot0 = vaddq_f32(dot0, dot1);
    na0 = vaddq_f32(na0, na1);
    nb0 = vaddq_f32(nb0, nb1);

    // SAFETY: vaddvq_f32 requires NEON, which we have via target_feature.
    let mut dot_sum = vaddvq_f32(dot0);
    let mut norm_a_sq = vaddvq_f32(na0);
    let mut norm_b_sq = vaddvq_f32(nb0);

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

/// SQ8 ADC (HQ-2) per-candidate statistics (NEON): `(Σ(q_i·c_i), Σc_i, Σc_i²)`.
///
/// Widens 16 u8 codes per iteration to f32 (`u8 -> u16 -> u32 -> f32` via
/// `vmovl`/`vcvtq_f32_u32`) and FMAs against the f32 query with three
/// independent accumulators. See `turbo_quant::sq8` module docs for how
/// these three running sums combine (O(1), architecture-independent) into
/// the final asymmetric L2/inner-product ADC distance — that combine step
/// deliberately has no SIMD variant, only this stats pass does.
///
/// # Safety
/// Caller must ensure the CPU supports NEON (baseline on all AArch64).
#[cfg(target_arch = "aarch64")]
#[inline]
#[target_feature(enable = "neon")]
pub unsafe fn sq8_stats(query: &[f32], codes: &[u8]) -> (f32, f32, f32) {
    debug_assert_eq!(query.len(), codes.len(), "sq8_stats: dimension mismatch");

    let n = query.len();
    let mut dot = vdupq_n_f32(0.0);
    let mut sum_c = vdupq_n_f32(0.0);
    let mut sumsq_c = vdupq_n_f32(0.0);

    let pq = query.as_ptr();
    let pc = codes.as_ptr();

    let chunks = n / 16;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16. Pointers are
        // valid for 16 elements (f32 query / u8 codes) at this offset.
        let c_u8 = vld1q_u8(pc.add(i));
        let c_u16_lo = vmovl_u8(vget_low_u8(c_u8));
        let c_u16_hi = vmovl_u8(vget_high_u8(c_u8));

        // Widen each group of 4 lanes u16 -> u32 -> f32.
        let c0 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(c_u16_lo)));
        let c1 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(c_u16_lo)));
        let c2 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(c_u16_hi)));
        let c3 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(c_u16_hi)));

        let q0 = vld1q_f32(pq.add(i));
        let q1 = vld1q_f32(pq.add(i + 4));
        let q2 = vld1q_f32(pq.add(i + 8));
        let q3 = vld1q_f32(pq.add(i + 12));

        dot = vfmaq_f32(dot, q0, c0);
        dot = vfmaq_f32(dot, q1, c1);
        dot = vfmaq_f32(dot, q2, c2);
        dot = vfmaq_f32(dot, q3, c3);

        sum_c = vaddq_f32(sum_c, c0);
        sum_c = vaddq_f32(sum_c, c1);
        sum_c = vaddq_f32(sum_c, c2);
        sum_c = vaddq_f32(sum_c, c3);

        sumsq_c = vfmaq_f32(sumsq_c, c0, c0);
        sumsq_c = vfmaq_f32(sumsq_c, c1, c1);
        sumsq_c = vfmaq_f32(sumsq_c, c2, c2);
        sumsq_c = vfmaq_f32(sumsq_c, c3, c3);

        i += 16;
    }

    // SAFETY: vaddvq_f32 requires NEON, which we have via target_feature.
    let mut dot_sum = vaddvq_f32(dot);
    let mut sum_c_sum = vaddvq_f32(sum_c);
    let mut sumsq_c_sum = vaddvq_f32(sumsq_c);

    // Scalar tail
    while i < n {
        let cf = *codes.get_unchecked(i) as f32;
        let qv = *query.get_unchecked(i);
        dot_sum += qv * cf;
        sum_c_sum += cf;
        sumsq_c_sum += cf * cf;
        i += 1;
    }

    (dot_sum, sum_c_sum, sumsq_c_sum)
}

#[cfg(test)]
#[cfg(target_arch = "aarch64")]
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

    #[test]
    fn test_l2_f32_matches_scalar() {
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::l2_f32(&a, &b);
        // SAFETY: NEON is baseline on AArch64.
        let got = unsafe { l2_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-4,
            "l2_f32 mismatch: scalar={expected}, neon={got}, rel={rel}"
        );
    }

    #[test]
    fn test_l2_i8_matches_scalar() {
        let a = gen_i8(768, 42);
        let b = gen_i8(768, 99);
        let expected = scalar::l2_i8(&a, &b);
        // SAFETY: NEON is baseline on AArch64.
        let got = unsafe { l2_i8(&a, &b) };
        assert_eq!(
            got, expected,
            "l2_i8 mismatch: scalar={expected}, neon={got}"
        );
    }

    #[test]
    fn test_dot_f32_matches_scalar() {
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::dot_f32(&a, &b);
        // SAFETY: NEON is baseline on AArch64.
        let got = unsafe { dot_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-4,
            "dot_f32 mismatch: scalar={expected}, neon={got}, rel={rel}"
        );
    }

    #[test]
    fn test_cosine_f32_matches_scalar() {
        let a = gen_f32(768, 42);
        let b = gen_f32(768, 99);
        let expected = scalar::cosine_f32(&a, &b);
        // SAFETY: NEON is baseline on AArch64.
        let got = unsafe { cosine_f32(&a, &b) };
        let rel = (got - expected).abs() / expected.abs().max(1e-10);
        assert!(
            rel < 1e-3,
            "cosine_f32 mismatch: scalar={expected}, neon={got}, rel={rel}"
        );
    }

    #[test]
    fn test_tail_handling() {
        for len in [1, 3, 7, 13, 15, 17, 31, 33, 100] {
            let a = gen_f32(len, 42);
            let b = gen_f32(len, 99);

            let expected_l2 = scalar::l2_f32(&a, &b);
            // SAFETY: NEON is baseline on AArch64.
            let got_l2 = unsafe { l2_f32(&a, &b) };
            let rel = (got_l2 - expected_l2).abs() / expected_l2.abs().max(1e-10);
            assert!(
                rel < 1e-4,
                "l2 tail len={len}: scalar={expected_l2}, neon={got_l2}"
            );

            let expected_dot = scalar::dot_f32(&a, &b);
            // SAFETY: NEON is baseline on AArch64.
            let got_dot = unsafe { dot_f32(&a, &b) };
            let rel = (got_dot - expected_dot).abs() / expected_dot.abs().max(1e-10);
            assert!(
                rel < 1e-4,
                "dot tail len={len}: scalar={expected_dot}, neon={got_dot}"
            );

            let ai = gen_i8(len, 42);
            let bi = gen_i8(len, 99);
            let expected_i8 = scalar::l2_i8(&ai, &bi);
            // SAFETY: NEON is baseline on AArch64.
            let got_i8 = unsafe { l2_i8(&ai, &bi) };
            assert_eq!(got_i8, expected_i8, "l2_i8 tail len={len}");
        }
    }

    #[test]
    fn test_empty_vectors() {
        let a: &[f32] = &[];
        let b: &[f32] = &[];
        // SAFETY: NEON is baseline on AArch64.
        unsafe {
            assert_eq!(l2_f32(a, b), 0.0);
            assert_eq!(dot_f32(a, b), 0.0);
            assert_eq!(cosine_f32(a, b), 1.0);
        }

        let ai: &[i8] = &[];
        let bi: &[i8] = &[];
        // SAFETY: NEON is baseline on AArch64.
        unsafe {
            assert_eq!(l2_i8(ai, bi), 0);
        }
    }

    // ── HQ-2: SQ8 ADC stats (NEON vs scalar reference) ──────────────────

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
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar;
        let q = gen_f32(768, 42);
        let c = gen_u8(768, 99);
        let expected = sq8_candidate_stats_scalar(&q, &c);
        // SAFETY: NEON is baseline on AArch64.
        let got = unsafe { sq8_stats(&q, &c) };
        let rel_dot = (got.0 - expected.0).abs() / expected.0.abs().max(1.0);
        let rel_sum = (got.1 - expected.1).abs() / expected.1.abs().max(1.0);
        let rel_sq = (got.2 - expected.2).abs() / expected.2.abs().max(1.0);
        assert!(
            rel_dot < 1e-3 && rel_sum < 1e-3 && rel_sq < 1e-3,
            "sq8_stats mismatch: scalar={expected:?}, neon={got:?}"
        );
    }

    #[test]
    fn test_sq8_stats_tail_handling() {
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar;
        for len in [0, 1, 3, 7, 13, 15, 16, 17, 31, 33, 100] {
            let q = gen_f32(len, 42);
            let c = gen_u8(len, 99);
            let expected = sq8_candidate_stats_scalar(&q, &c);
            // SAFETY: NEON is baseline on AArch64.
            let got = unsafe { sq8_stats(&q, &c) };
            let rel_dot = (got.0 - expected.0).abs() / expected.0.abs().max(1.0);
            let rel_sum = (got.1 - expected.1).abs() / expected.1.abs().max(1.0);
            let rel_sq = (got.2 - expected.2).abs() / expected.2.abs().max(1.0);
            assert!(
                rel_dot < 1e-3 && rel_sum < 1e-3 && rel_sq < 1e-3,
                "sq8_stats tail len={len}: scalar={expected:?}, neon={got:?}"
            );
        }
    }

    #[test]
    fn test_sq8_stats_empty() {
        let q: &[f32] = &[];
        let c: &[u8] = &[];
        // SAFETY: NEON is baseline on AArch64.
        let got = unsafe { sq8_stats(q, c) };
        assert_eq!(got, (0.0, 0.0, 0.0));
    }
}
