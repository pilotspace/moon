//! SIMD-accelerated cosine similarity with platform dispatch.
//!
//! Dispatches to the best available instruction set:
//! - **aarch64**: NEON FMA (4 accumulators x 4 lanes = 16 floats/iter)
//! - **x86_64**: AVX2+FMA (4 accumulators x 8 lanes = 32 floats/iter, runtime detected)
//! - **fallback**: scalar f64 accumulation
//!
//! All paths return identical results within floating-point tolerance (~1e-5 for
//! normalized 384-dim embeddings).

// ---------------------------------------------------------------------------
// Scalar fallback (always compiled, used as reference + fallback)
// ---------------------------------------------------------------------------

#[inline]
fn cosine_similarity_scalar(a: &[f32], b: &[f32]) -> f64 {
    let mut dot: f64 = 0.0;
    let mut norm_a: f64 = 0.0;
    let mut norm_b: f64 = 0.0;
    for i in 0..a.len() {
        let ai = a[i] as f64;
        let bi = b[i] as f64;
        dot += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom < f64::EPSILON {
        return 0.0;
    }
    dot / denom
}

/// Scalar implementation exposed for benchmark comparison.
#[inline]
pub fn cosine_similarity_scalar_pub(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    cosine_similarity_scalar(a, b)
}

// ---------------------------------------------------------------------------
// aarch64 NEON
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[inline]
#[target_feature(enable = "neon")]
unsafe fn cosine_similarity_neon(a: &[f32], b: &[f32]) -> f64 {
    use std::arch::aarch64::*;

    let len = a.len();
    let chunks = len / 16; // 4 accumulators x 4 lanes
    let remainder = len % 16;

    // 4 independent accumulator registers to hide FMA latency (4-cycle on Cortex-A76+).
    let mut dot0 = vdupq_n_f32(0.0);
    let mut dot1 = vdupq_n_f32(0.0);
    let mut dot2 = vdupq_n_f32(0.0);
    let mut dot3 = vdupq_n_f32(0.0);
    let mut na0 = vdupq_n_f32(0.0);
    let mut na1 = vdupq_n_f32(0.0);
    let mut na2 = vdupq_n_f32(0.0);
    let mut na3 = vdupq_n_f32(0.0);
    let mut nb0 = vdupq_n_f32(0.0);
    let mut nb1 = vdupq_n_f32(0.0);
    let mut nb2 = vdupq_n_f32(0.0);
    let mut nb3 = vdupq_n_f32(0.0);

    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();

    for i in 0..chunks {
        let base = i * 16;
        // SAFETY: `base + 15 < chunks * 16 <= len`, so all loads are in-bounds.
        let a0 = vld1q_f32(a_ptr.add(base));
        let a1 = vld1q_f32(a_ptr.add(base + 4));
        let a2 = vld1q_f32(a_ptr.add(base + 8));
        let a3 = vld1q_f32(a_ptr.add(base + 12));
        let b0_v = vld1q_f32(b_ptr.add(base));
        let b1_v = vld1q_f32(b_ptr.add(base + 4));
        let b2_v = vld1q_f32(b_ptr.add(base + 8));
        let b3_v = vld1q_f32(b_ptr.add(base + 12));

        dot0 = vfmaq_f32(dot0, a0, b0_v);
        dot1 = vfmaq_f32(dot1, a1, b1_v);
        dot2 = vfmaq_f32(dot2, a2, b2_v);
        dot3 = vfmaq_f32(dot3, a3, b3_v);
        na0 = vfmaq_f32(na0, a0, a0);
        na1 = vfmaq_f32(na1, a1, a1);
        na2 = vfmaq_f32(na2, a2, a2);
        na3 = vfmaq_f32(na3, a3, a3);
        nb0 = vfmaq_f32(nb0, b0_v, b0_v);
        nb1 = vfmaq_f32(nb1, b1_v, b1_v);
        nb2 = vfmaq_f32(nb2, b2_v, b2_v);
        nb3 = vfmaq_f32(nb3, b3_v, b3_v);
    }

    // Horizontal reduce: sum 4 accumulator registers, then reduce lanes.
    let dot_sum = vaddvq_f32(vaddq_f32(vaddq_f32(dot0, dot1), vaddq_f32(dot2, dot3)));
    let na_sum = vaddvq_f32(vaddq_f32(vaddq_f32(na0, na1), vaddq_f32(na2, na3)));
    let nb_sum = vaddvq_f32(vaddq_f32(vaddq_f32(nb0, nb1), vaddq_f32(nb2, nb3)));

    // Process remainder with scalar arithmetic.
    let mut dot_r: f32 = 0.0;
    let mut na_r: f32 = 0.0;
    let mut nb_r: f32 = 0.0;
    let tail_start = chunks * 16;
    for i in 0..remainder {
        // SAFETY: `tail_start + i < tail_start + remainder == len`.
        let ai = *a_ptr.add(tail_start + i);
        let bi = *b_ptr.add(tail_start + i);
        dot_r += ai * bi;
        na_r += ai * ai;
        nb_r += bi * bi;
    }

    let dot_total = (dot_sum + dot_r) as f64;
    let na_total = (na_sum + na_r) as f64;
    let nb_total = (nb_sum + nb_r) as f64;

    let denom = na_total.sqrt() * nb_total.sqrt();
    if denom < f64::EPSILON {
        return 0.0;
    }
    dot_total / denom
}

// ---------------------------------------------------------------------------
// x86_64 AVX2 + FMA
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn cosine_similarity_avx2(a: &[f32], b: &[f32]) -> f64 {
    use std::arch::x86_64::*;

    let len = a.len();
    let chunks = len / 32; // 4 accumulators x 8 lanes
    let remainder = len % 32;

    // 4 independent 256-bit accumulators to hide FMA latency.
    let mut dot0 = _mm256_setzero_ps();
    let mut dot1 = _mm256_setzero_ps();
    let mut dot2 = _mm256_setzero_ps();
    let mut dot3 = _mm256_setzero_ps();
    let mut na0 = _mm256_setzero_ps();
    let mut na1 = _mm256_setzero_ps();
    let mut na2 = _mm256_setzero_ps();
    let mut na3 = _mm256_setzero_ps();
    let mut nb0 = _mm256_setzero_ps();
    let mut nb1 = _mm256_setzero_ps();
    let mut nb2 = _mm256_setzero_ps();
    let mut nb3 = _mm256_setzero_ps();

    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();

    for i in 0..chunks {
        let base = i * 32;
        // SAFETY: `base + 31 < chunks * 32 <= len`, so all loads are in-bounds.
        let av0 = _mm256_loadu_ps(a_ptr.add(base));
        let av1 = _mm256_loadu_ps(a_ptr.add(base + 8));
        let av2 = _mm256_loadu_ps(a_ptr.add(base + 16));
        let av3 = _mm256_loadu_ps(a_ptr.add(base + 24));
        let bv0 = _mm256_loadu_ps(b_ptr.add(base));
        let bv1 = _mm256_loadu_ps(b_ptr.add(base + 8));
        let bv2 = _mm256_loadu_ps(b_ptr.add(base + 16));
        let bv3 = _mm256_loadu_ps(b_ptr.add(base + 24));

        dot0 = _mm256_fmadd_ps(av0, bv0, dot0);
        dot1 = _mm256_fmadd_ps(av1, bv1, dot1);
        dot2 = _mm256_fmadd_ps(av2, bv2, dot2);
        dot3 = _mm256_fmadd_ps(av3, bv3, dot3);
        na0 = _mm256_fmadd_ps(av0, av0, na0);
        na1 = _mm256_fmadd_ps(av1, av1, na1);
        na2 = _mm256_fmadd_ps(av2, av2, na2);
        na3 = _mm256_fmadd_ps(av3, av3, na3);
        nb0 = _mm256_fmadd_ps(bv0, bv0, nb0);
        nb1 = _mm256_fmadd_ps(bv1, bv1, nb1);
        nb2 = _mm256_fmadd_ps(bv2, bv2, nb2);
        nb3 = _mm256_fmadd_ps(bv3, bv3, nb3);
    }

    // Horizontal sum: combine 4 accumulators, then reduce 8 lanes to scalar.
    let dot_combined = _mm256_add_ps(
        _mm256_add_ps(dot0, dot1),
        _mm256_add_ps(dot2, dot3),
    );
    let na_combined = _mm256_add_ps(
        _mm256_add_ps(na0, na1),
        _mm256_add_ps(na2, na3),
    );
    let nb_combined = _mm256_add_ps(
        _mm256_add_ps(nb0, nb1),
        _mm256_add_ps(nb2, nb3),
    );

    let dot_sum = hsum256_ps(dot_combined);
    let na_sum = hsum256_ps(na_combined);
    let nb_sum = hsum256_ps(nb_combined);

    // Process remainder with scalar arithmetic.
    let mut dot_r: f32 = 0.0;
    let mut na_r: f32 = 0.0;
    let mut nb_r: f32 = 0.0;
    let tail_start = chunks * 32;
    for i in 0..remainder {
        // SAFETY: `tail_start + i < tail_start + remainder == len`.
        let ai = *a_ptr.add(tail_start + i);
        let bi = *b_ptr.add(tail_start + i);
        dot_r += ai * bi;
        na_r += ai * ai;
        nb_r += bi * bi;
    }

    let dot_total = (dot_sum + dot_r) as f64;
    let na_total = (na_sum + na_r) as f64;
    let nb_total = (nb_sum + nb_r) as f64;

    let denom = na_total.sqrt() * nb_total.sqrt();
    if denom < f64::EPSILON {
        return 0.0;
    }
    dot_total / denom
}

/// Horizontal sum of all 8 lanes of a `__m256`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn hsum256_ps(v: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;
    // Sum high and low 128-bit halves.
    let hi128 = _mm256_extractf128_ps(v, 1);
    let lo128 = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(lo128, hi128);
    // Horizontal add within 128 bits: [a+b, c+d, a+b, c+d].
    let shuf = _mm_movehdup_ps(sum128);
    let sums = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let result = _mm_add_ss(sums, shuf2);
    _mm_cvtss_f32(result)
}

// ---------------------------------------------------------------------------
// Public dispatch
// ---------------------------------------------------------------------------

/// SIMD-accelerated cosine similarity. Dispatches to the best available
/// instruction set at compile time (aarch64 NEON) or runtime (x86_64 AVX2).
/// Returns 0.0 on degenerate input (empty, mismatched lengths, zero-norm).
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is mandatory on ARMv8-A (aarch64). Every aarch64 CPU
        // supports these intrinsics unconditionally.
        return unsafe { cosine_similarity_neon(a, b) };
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            // SAFETY: runtime feature detection confirmed AVX2+FMA available.
            return unsafe { cosine_similarity_avx2(a, b) };
        }
    }

    #[allow(unreachable_code)]
    cosine_similarity_scalar(a, b)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical_vectors() {
        let v = [1.0f32, 2.0, 3.0, 4.0];
        let sim = cosine_similarity(&v, &v);
        assert!(
            (sim - 1.0).abs() < 1e-6,
            "identical vectors should have cosine 1.0, got {sim}"
        );
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = [1.0f32, 0.0, 0.0, 0.0];
        let b = [0.0f32, 1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-6, "orthogonal vectors should have cosine 0.0, got {sim}");
    }

    #[test]
    fn test_cosine_opposite() {
        let a = [1.0f32, 2.0, 3.0, 4.0];
        let b: Vec<f32> = a.iter().map(|x| -x).collect();
        let sim = cosine_similarity(&a, &b);
        assert!(
            (sim + 1.0).abs() < 1e-6,
            "opposite vectors should have cosine -1.0, got {sim}"
        );
    }

    #[test]
    fn test_cosine_empty() {
        let sim = cosine_similarity(&[], &[]);
        assert!(sim.abs() < f64::EPSILON, "empty vectors should return 0.0");
    }

    #[test]
    fn test_cosine_mismatched_len() {
        let a = [1.0f32, 2.0];
        let b = [1.0f32, 2.0, 3.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < f64::EPSILON, "mismatched lengths should return 0.0");
    }

    #[test]
    fn test_cosine_384dim_matches_scalar() {
        // Deterministic pseudo-random vectors (LCG).
        let dim = 384;
        let a: Vec<f32> = (0..dim).map(|i| ((i as f32) * 0.017).sin()).collect();
        let b: Vec<f32> = (0..dim).map(|i| ((i as f32) * 0.031).cos()).collect();

        let simd_result = cosine_similarity(&a, &b);
        let scalar_result = cosine_similarity_scalar(&a, &b);

        let diff = (simd_result - scalar_result).abs();
        assert!(
            diff < 1e-5,
            "384-dim SIMD vs scalar mismatch: simd={simd_result}, scalar={scalar_result}, diff={diff}"
        );
    }

    #[test]
    fn test_cosine_non_multiple_length() {
        // 385 = 24*16 + 1 (NEON remainder = 1), 12*32 + 1 (AVX2 remainder = 1)
        let dim = 385;
        let a: Vec<f32> = (0..dim).map(|i| ((i as f32) * 0.013).sin()).collect();
        let b: Vec<f32> = (0..dim).map(|i| ((i as f32) * 0.029).cos()).collect();

        let simd_result = cosine_similarity(&a, &b);
        let scalar_result = cosine_similarity_scalar(&a, &b);

        let diff = (simd_result - scalar_result).abs();
        assert!(
            diff < 1e-5,
            "385-dim SIMD vs scalar mismatch: simd={simd_result}, scalar={scalar_result}, diff={diff}"
        );
    }

    #[test]
    fn test_cosine_768dim_matches_scalar() {
        let dim = 768;
        let a: Vec<f32> = (0..dim).map(|i| ((i as f32) * 0.011).sin()).collect();
        let b: Vec<f32> = (0..dim).map(|i| ((i as f32) * 0.023).cos()).collect();

        let simd_result = cosine_similarity(&a, &b);
        let scalar_result = cosine_similarity_scalar(&a, &b);

        let diff = (simd_result - scalar_result).abs();
        assert!(
            diff < 1e-5,
            "768-dim SIMD vs scalar mismatch: simd={simd_result}, scalar={scalar_result}, diff={diff}"
        );
    }

    #[test]
    fn test_cosine_scalar_pub_identical() {
        let v = [1.0f32, 2.0, 3.0];
        let sim = cosine_similarity_scalar_pub(&v, &v);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_scalar_pub_degenerate() {
        assert!(cosine_similarity_scalar_pub(&[], &[]).abs() < f64::EPSILON);
        assert!(cosine_similarity_scalar_pub(&[1.0], &[1.0, 2.0]).abs() < f64::EPSILON);
    }
}
