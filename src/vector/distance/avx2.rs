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
#[inline]
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
#[inline]
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
///
/// # Safety
/// Caller must ensure AVX2 and FMA CPU features are available.
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
///
/// # Safety
/// Caller must ensure AVX2 and FMA CPU features are available.
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
///
/// # Safety
/// Caller must ensure AVX2 and FMA CPU features are available.
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
///
/// # Safety
/// Caller must ensure AVX2 and FMA CPU features are available.
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

/// SQ8 ADC (HQ-2) per-candidate statistics (AVX2+FMA): `(Σ(q_i·c_i), Σc_i, Σc_i²)`.
///
/// Widens 16 u8 codes per iteration to f32 (`VPMOVZXBD` via
/// `_mm256_cvtepu8_epi32`, 8 lanes/call, 2x unrolled, then `_mm256_cvtepi32_ps`)
/// and FMAs against the f32 query. See `turbo_quant::sq8` module docs for how
/// these three running sums combine (O(1), architecture-independent) into
/// the final asymmetric L2/inner-product ADC distance — that combine step
/// deliberately has no SIMD variant, only this stats pass does.
///
/// # Safety
/// Caller must ensure AVX2 and FMA CPU features are available.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,fma")]
pub unsafe fn sq8_stats(query: &[f32], codes: &[u8]) -> (f32, f32, f32) {
    debug_assert_eq!(query.len(), codes.len(), "sq8_stats: dimension mismatch");

    let n = query.len();
    let mut dot0 = _mm256_setzero_ps();
    let mut dot1 = _mm256_setzero_ps();
    let mut sumc0 = _mm256_setzero_ps();
    let mut sumc1 = _mm256_setzero_ps();
    let mut sumsqc0 = _mm256_setzero_ps();
    let mut sumsqc1 = _mm256_setzero_ps();

    let pq = query.as_ptr();
    let pc = codes.as_ptr();

    let chunks = n / 16;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16. `_mm_loadl_epi64`
        // reads 8 bytes (unaligned); `_mm256_cvtepu8_epi32` zero-extends the
        // low 8 lanes to i32, which `_mm256_cvtepi32_ps` converts exactly
        // (u8 values 0..=255 have exact f32 representations).
        let c0_u8 = _mm_loadl_epi64(pc.add(i) as *const __m128i);
        let c0_f32 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(c0_u8));
        let q0 = _mm256_loadu_ps(pq.add(i));
        dot0 = _mm256_fmadd_ps(q0, c0_f32, dot0);
        sumc0 = _mm256_add_ps(sumc0, c0_f32);
        sumsqc0 = _mm256_fmadd_ps(c0_f32, c0_f32, sumsqc0);

        let c1_u8 = _mm_loadl_epi64(pc.add(i + 8) as *const __m128i);
        let c1_f32 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(c1_u8));
        let q1 = _mm256_loadu_ps(pq.add(i + 8));
        dot1 = _mm256_fmadd_ps(q1, c1_f32, dot1);
        sumc1 = _mm256_add_ps(sumc1, c1_f32);
        sumsqc1 = _mm256_fmadd_ps(c1_f32, c1_f32, sumsqc1);

        i += 16;
    }

    dot0 = _mm256_add_ps(dot0, dot1);
    sumc0 = _mm256_add_ps(sumc0, sumc1);
    sumsqc0 = _mm256_add_ps(sumsqc0, sumsqc1);

    // SAFETY: hsum_f32_avx2 requires AVX2, which we have via target_feature.
    let mut dot_sum = hsum_f32_avx2(dot0);
    let mut sum_c_sum = hsum_f32_avx2(sumc0);
    let mut sumsq_c_sum = hsum_f32_avx2(sumsqc0);

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

/// Int8 symmetric ADC per-candidate statistics (task #13): `(Σ qi8_i·c_i,
/// Σc_i, Σc_i²)` as exact `i64` integers, given an already-quantized `qi8`
/// query (see `turbo_quant::sq8::sq8_quantize_query_scalar`).
///
/// The design doc (tmp/INT8-ADC-CONTEXT.md) originally called for
/// `VPMADDUBSW` (u8×i8 → i16 pairwise-add) directly, clamping the query to
/// `±63` to avoid its saturation trap (`255·127·2 = 64770 > i16::MAX`).
/// Recall A/B testing (`turbo_quant::sq8::tests::test_int8_adc_recall_ab_*`)
/// showed that clamp measurably regresses recall (R@10 delta ~0.017,
/// overlap ~0.975 vs the ≥0.98 gate) while the full `±127` range passes
/// comfortably (delta ~0.003-0.005, overlap ~0.988+). This kernel instead
/// avoids `VPMADDUBSW` entirely: widen both operands to i16 first
/// (`_mm256_cvtepu8_epi16` / `_mm256_cvtepi8_epi16` — exact, since u8 and
/// the `[-127,127]` qi8 range both fit i16 losslessly), multiply with
/// `_mm256_mullo_epi16` (exact — `|q·c| ≤ 127·255 = 32385 < i16::MAX`, a
/// single-element product never saturates), then widen-accumulate to i32
/// via `_mm256_madd_epi16` against an all-ones vector (`VPMADDWD` genuinely
/// produces a 32-bit result with no saturation, unlike `VPMADDUBSW`'s
/// 16-bit output). No offset trick needed here (unlike the NEON kernel):
/// `cvtepu8_epi16`/`cvtepi8_epi16` already widen with the correct sign
/// handling for u8 vs i8 — `sum_qi8` is accepted only for signature parity
/// with [`super::neon::sq8_i8_stats`] / the scalar reference.
///
/// `sum_c`/`sumsq_c` reuse the same widened `c_lo16`/`c_hi16` registers (no
/// extra widen pass), since both are query-independent.
///
/// # Safety
/// Caller must ensure AVX2 is available (checked via
/// `is_x86_feature_detected!("avx2")` at table-init time).
///
/// # Panics (debug only)
/// `debug_assert_eq!(qi8.len(), codes.len())`.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2")]
pub unsafe fn sq8_i8_stats(qi8: &[i8], codes: &[u8], _sum_qi8: i32) -> (i64, i64, i64) {
    debug_assert_eq!(qi8.len(), codes.len(), "sq8_i8_stats: dimension mismatch");

    let n = qi8.len();
    let pq = qi8.as_ptr();
    let pc = codes.as_ptr();

    let mut dot_acc = _mm256_setzero_si256();
    let mut sum_c_acc = _mm256_setzero_si256();
    let mut sumsq_c_acc = _mm256_setzero_si256();
    let ones16 = _mm256_set1_epi16(1);

    let chunks = n / 32;
    let mut i = 0usize;

    for _ in 0..chunks {
        // SAFETY: i + 32 <= n guaranteed by chunks = n / 32. Both loads are
        // unaligned 32-byte reads within bounds (i8 query / u8 codes).
        let qv = _mm256_loadu_si256(pq.add(i) as *const __m256i);
        let cv = _mm256_loadu_si256(pc.add(i) as *const __m256i);

        let q_lo16 = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(qv));
        let q_hi16 = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(qv, 1));
        let c_lo16 = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(cv));
        let c_hi16 = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(cv, 1));

        let dot_lo16 = _mm256_mullo_epi16(q_lo16, c_lo16);
        let dot_hi16 = _mm256_mullo_epi16(q_hi16, c_hi16);
        dot_acc = _mm256_add_epi32(dot_acc, _mm256_madd_epi16(dot_lo16, ones16));
        dot_acc = _mm256_add_epi32(dot_acc, _mm256_madd_epi16(dot_hi16, ones16));

        sum_c_acc = _mm256_add_epi32(sum_c_acc, _mm256_madd_epi16(c_lo16, ones16));
        sum_c_acc = _mm256_add_epi32(sum_c_acc, _mm256_madd_epi16(c_hi16, ones16));

        sumsq_c_acc = _mm256_add_epi32(sumsq_c_acc, _mm256_madd_epi16(c_lo16, c_lo16));
        sumsq_c_acc = _mm256_add_epi32(sumsq_c_acc, _mm256_madd_epi16(c_hi16, c_hi16));

        i += 32;
    }

    // SAFETY: hsum_i32_avx2 requires AVX2, which we have via target_feature.
    let mut dot: i64 = hsum_i32_avx2(dot_acc) as i64;
    let mut sum_c: i64 = hsum_i32_avx2(sum_c_acc) as i64;
    let mut sumsq_c: i64 = hsum_i32_avx2(sumsq_c_acc) as i64;

    // Scalar tail.
    while i < n {
        // SAFETY: i < n, within bounds of both equal-length slices.
        let q = *qi8.get_unchecked(i) as i64;
        let c = *codes.get_unchecked(i) as i64;
        dot += q * c;
        sum_c += c;
        sumsq_c += c * c;
        i += 1;
    }

    (dot, sum_c, sumsq_c)
}

// ── f16 sidecar kernels (exact-rerank HQ-1) ─────────────────────────────

/// Squared L2 between an f32 query and an f16-encoded sidecar vector.
/// F16C `vcvtph2ps` decodes 8 halves per step (hardware IEEE semantics —
/// subnormals, Inf, and NaN match scalar `f16_to_f32` exactly); FMA
/// accumulates. 16 halves per iteration, scalar tail.
///
/// # Safety
/// Caller must ensure the CPU supports AVX2, F16C, and FMA
/// (verified via `is_x86_feature_detected!` at DistanceTable init).
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,f16c,fma")]
pub unsafe fn f16_l2(query: &[f32], vec_f16: &[u16]) -> f32 {
    debug_assert_eq!(query.len(), vec_f16.len(), "f16_l2: dimension mismatch");
    let n = query.len();
    let mut sum0 = _mm256_setzero_ps();
    let mut sum1 = _mm256_setzero_ps();
    let pq = query.as_ptr();
    let px = vec_f16.as_ptr();
    let chunks = n / 16;
    let mut i = 0usize;
    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16. Pointers are
        // valid slices (f32 query / u16 halves) at this offset; loadu allows
        // unaligned access.
        let x0 = _mm256_cvtph_ps(_mm_loadu_si128(px.add(i) as *const __m128i));
        let x1 = _mm256_cvtph_ps(_mm_loadu_si128(px.add(i + 8) as *const __m128i));
        let q0 = _mm256_loadu_ps(pq.add(i));
        let q1 = _mm256_loadu_ps(pq.add(i + 8));
        let d0 = _mm256_sub_ps(q0, x0);
        let d1 = _mm256_sub_ps(q1, x1);
        sum0 = _mm256_fmadd_ps(d0, d0, sum0);
        sum1 = _mm256_fmadd_ps(d1, d1, sum1);
        i += 16;
    }
    // SAFETY: hsum_f32_avx2 requires AVX2, which we have via target_feature.
    let mut result = hsum_f32_avx2(_mm256_add_ps(sum0, sum1));
    // Scalar tail
    while i < n {
        let d = *query.get_unchecked(i) - crate::vector::f16::f16_to_f32(*vec_f16.get_unchecked(i));
        result += d * d;
        i += 1;
    }
    result
}

/// Fused `(Σ q_i·x_i, Σ x_i²)` between an f32 query and an f16-encoded
/// sidecar vector (F16C decode + FMA, 16 halves per iteration; scalar
/// tail). Feeds the unit-sphere (Cosine/InnerProduct) exact-rerank
/// distance.
///
/// # Safety
/// Caller must ensure the CPU supports AVX2, F16C, and FMA
/// (verified via `is_x86_feature_detected!` at DistanceTable init).
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,f16c,fma")]
pub unsafe fn f16_dot_normsq(query: &[f32], vec_f16: &[u16]) -> (f32, f32) {
    debug_assert_eq!(
        query.len(),
        vec_f16.len(),
        "f16_dot_normsq: dimension mismatch"
    );
    let n = query.len();
    let mut dot0 = _mm256_setzero_ps();
    let mut dot1 = _mm256_setzero_ps();
    let mut xsq0 = _mm256_setzero_ps();
    let mut xsq1 = _mm256_setzero_ps();
    let pq = query.as_ptr();
    let px = vec_f16.as_ptr();
    let chunks = n / 16;
    let mut i = 0usize;
    for _ in 0..chunks {
        // SAFETY: i + 16 <= n guaranteed by chunks = n / 16. Pointers are
        // valid slices (f32 query / u16 halves) at this offset; loadu allows
        // unaligned access.
        let x0 = _mm256_cvtph_ps(_mm_loadu_si128(px.add(i) as *const __m128i));
        let x1 = _mm256_cvtph_ps(_mm_loadu_si128(px.add(i + 8) as *const __m128i));
        let q0 = _mm256_loadu_ps(pq.add(i));
        let q1 = _mm256_loadu_ps(pq.add(i + 8));
        dot0 = _mm256_fmadd_ps(q0, x0, dot0);
        dot1 = _mm256_fmadd_ps(q1, x1, dot1);
        xsq0 = _mm256_fmadd_ps(x0, x0, xsq0);
        xsq1 = _mm256_fmadd_ps(x1, x1, xsq1);
        i += 16;
    }
    // SAFETY: hsum_f32_avx2 requires AVX2, which we have via target_feature.
    let mut dot = hsum_f32_avx2(_mm256_add_ps(dot0, dot1));
    let mut xsq = hsum_f32_avx2(_mm256_add_ps(xsq0, xsq1));
    // Scalar tail
    while i < n {
        let x = crate::vector::f16::f16_to_f32(*vec_f16.get_unchecked(i));
        dot += *query.get_unchecked(i) * x;
        xsq += x * x;
        i += 1;
    }
    (dot, xsq)
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

    // ── HQ-2: SQ8 ADC stats (AVX2 vs scalar reference) ──────────────────
    //
    // Gated on `has_avx2_fma()` like every other test in this file: on this
    // aarch64 dev host these compile (proving the cfg discipline holds) but
    // no-op at runtime. They exercise for real on any x86_64 host/CI runner
    // with AVX2+FMA.

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
        if !has_avx2_fma() {
            return;
        }
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar;
        let q = gen_f32(768, 42);
        let c = gen_u8(768, 99);
        let expected = sq8_candidate_stats_scalar(&q, &c);
        // SAFETY: AVX2+FMA verified above.
        let got = unsafe { sq8_stats(&q, &c) };
        let rel_dot = (got.0 - expected.0).abs() / expected.0.abs().max(1.0);
        let rel_sum = (got.1 - expected.1).abs() / expected.1.abs().max(1.0);
        let rel_sq = (got.2 - expected.2).abs() / expected.2.abs().max(1.0);
        assert!(
            rel_dot < 1e-3 && rel_sum < 1e-3 && rel_sq < 1e-3,
            "sq8_stats mismatch: scalar={expected:?}, avx2={got:?}"
        );
    }

    #[test]
    fn test_sq8_stats_tail_handling() {
        if !has_avx2_fma() {
            return;
        }
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar;
        for len in [0, 1, 3, 7, 13, 15, 16, 17, 31, 33, 100] {
            let q = gen_f32(len, 42);
            let c = gen_u8(len, 99);
            let expected = sq8_candidate_stats_scalar(&q, &c);
            // SAFETY: AVX2+FMA verified above.
            let got = unsafe { sq8_stats(&q, &c) };
            let rel_dot = (got.0 - expected.0).abs() / expected.0.abs().max(1.0);
            let rel_sum = (got.1 - expected.1).abs() / expected.1.abs().max(1.0);
            let rel_sq = (got.2 - expected.2).abs() / expected.2.abs().max(1.0);
            assert!(
                rel_dot < 1e-3 && rel_sum < 1e-3 && rel_sq < 1e-3,
                "sq8_stats tail len={len}: scalar={expected:?}, avx2={got:?}"
            );
        }
    }

    #[test]
    fn test_sq8_stats_empty() {
        if !has_avx2_fma() {
            return;
        }
        let q: &[f32] = &[];
        let c: &[u8] = &[];
        // SAFETY: AVX2+FMA verified above.
        let got = unsafe { sq8_stats(q, c) };
        assert_eq!(got, (0.0, 0.0, 0.0));
    }

    // ── task #13: int8 symmetric ADC stats (AVX2 vs scalar i64 oracle) ───
    //
    // Gated on `has_avx2_fma()` — compiles on this aarch64 dev host (proving
    // the cfg discipline holds) but no-ops at runtime; exercises for real on
    // x86_64 CI/GCE runners.

    fn gen_i8_query(len: usize, seed: u32, qmax: i32) -> Vec<i8> {
        let mut v = Vec::with_capacity(len);
        let mut s = seed;
        for _ in 0..len {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            let raw = (s >> 24) as i8 as i32;
            v.push((raw % (qmax + 1)) as i8);
        }
        v
    }

    #[test]
    fn test_sq8_i8_stats_matches_scalar_oracle() {
        if !has_avx2_fma() {
            return;
        }
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_i8_scalar;
        let qi8 = gen_i8_query(768, 42, 127);
        let c = gen_u8(768, 99);
        let sum_qi8: i32 = qi8.iter().map(|&x| x as i32).sum();
        let expected = sq8_candidate_stats_i8_scalar(&qi8, &c, sum_qi8);
        // SAFETY: AVX2 verified above.
        let got = unsafe { sq8_i8_stats(&qi8, &c, sum_qi8) };
        assert_eq!(
            got, expected,
            "sq8_i8_stats mismatch (exact int arithmetic): scalar={expected:?}, avx2={got:?}"
        );
    }

    #[test]
    fn test_sq8_i8_stats_tail_handling() {
        if !has_avx2_fma() {
            return;
        }
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_i8_scalar;
        for len in [0, 1, 3, 7, 13, 15, 16, 17, 31, 32, 33, 63, 64, 100] {
            let qi8 = gen_i8_query(len, 42, 127);
            let c = gen_u8(len, 99);
            let sum_qi8: i32 = qi8.iter().map(|&x| x as i32).sum();
            let expected = sq8_candidate_stats_i8_scalar(&qi8, &c, sum_qi8);
            // SAFETY: AVX2 verified above.
            let got = unsafe { sq8_i8_stats(&qi8, &c, sum_qi8) };
            assert_eq!(
                got, expected,
                "sq8_i8_stats tail len={len}: scalar={expected:?}, avx2={got:?}"
            );
        }
    }

    #[test]
    fn test_sq8_i8_stats_empty() {
        if !has_avx2_fma() {
            return;
        }
        let qi8: &[i8] = &[];
        let c: &[u8] = &[];
        // SAFETY: AVX2 verified above.
        let got = unsafe { sq8_i8_stats(qi8, c, 0) };
        assert_eq!(got, (0, 0, 0));
    }

    #[test]
    fn test_sq8_i8_stats_extreme_values() {
        if !has_avx2_fma() {
            return;
        }
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_i8_scalar;
        let dim = 768;
        let qi8: Vec<i8> = (0..dim)
            .map(|i| if i % 2 == 0 { 127i8 } else { -127i8 })
            .collect();
        let c: Vec<u8> = vec![255u8; dim];
        let sum_qi8: i32 = qi8.iter().map(|&x| x as i32).sum();
        let expected = sq8_candidate_stats_i8_scalar(&qi8, &c, sum_qi8);
        // SAFETY: AVX2 verified above.
        let got = unsafe { sq8_i8_stats(&qi8, &c, sum_qi8) };
        assert_eq!(
            got, expected,
            "extreme-value mismatch: scalar={expected:?}, avx2={got:?}"
        );
    }

    fn has_f16c_kernel_features() -> bool {
        is_x86_feature_detected!("avx2")
            && is_x86_feature_detected!("fma")
            && is_x86_feature_detected!("f16c")
    }

    #[test]
    fn test_f16_kernels_match_scalar_all_value_classes() {
        use crate::vector::f16::{dot_normsq_f16, f32_to_f16, l2_sq_f16};
        if !has_f16c_kernel_features() {
            return;
        }
        // Normals, f16 subnormals, zero, and odd tails -- F16C hardware decode
        // must agree with scalar f16_to_f32 everywhere.
        for len in [0, 1, 3, 7, 8, 9, 15, 16, 17, 31, 33, 100, 384] {
            let q = gen_f32(len, 21);
            let x: Vec<u16> = gen_f32(len, 77)
                .iter()
                .enumerate()
                .map(|(j, &v)| {
                    if j % 5 == 4 {
                        f32_to_f16(3e-6 + (j as f32) * 1.1e-7) // subnormal f16
                    } else if j % 7 == 6 {
                        f32_to_f16(0.0)
                    } else {
                        f32_to_f16(v)
                    }
                })
                .collect();
            let exp_l2 = l2_sq_f16(&q, &x);
            // SAFETY: AVX2+F16C+FMA verified above.
            let got_l2 = unsafe { f16_l2(&q, &x) };
            let rel = (got_l2 - exp_l2).abs() / exp_l2.abs().max(1e-10);
            assert!(
                rel < 1e-4 || len == 0,
                "f16_l2 len={len}: scalar={exp_l2}, avx2={got_l2}"
            );

            let (ed, en) = dot_normsq_f16(&q, &x);
            // SAFETY: AVX2+F16C+FMA verified above.
            let (gd, gn) = unsafe { f16_dot_normsq(&q, &x) };
            assert!(
                (gd - ed).abs() / ed.abs().max(1e-6) < 1e-3,
                "f16_dot len={len}: scalar={ed}, avx2={gd}"
            );
            assert!(
                (gn - en).abs() / en.abs().max(1e-6) < 1e-4,
                "f16_normsq len={len}: scalar={en}, avx2={gn}"
            );
        }
    }

    #[test]
    fn test_f16_kernels_inf_nan_propagate() {
        use crate::vector::f16::f32_to_f16;
        if !has_f16c_kernel_features() {
            return;
        }
        let q = gen_f32(24, 5);
        let mut x: Vec<u16> = gen_f32(24, 9).iter().map(|&v| f32_to_f16(v)).collect();
        x[10] = f32_to_f16(f32::INFINITY);
        // SAFETY: AVX2+F16C+FMA verified above.
        assert_eq!(unsafe { f16_l2(&q, &x) }, f32::INFINITY);
        x[10] = f32_to_f16(f32::NAN);
        // SAFETY: AVX2+F16C+FMA verified above.
        assert!(unsafe { f16_l2(&q, &x) }.is_nan());
        // SAFETY: AVX2+F16C+FMA verified above.
        let (_, xsq) = unsafe { f16_dot_normsq(&q, &x) };
        assert!(xsq.is_nan());
    }
}
