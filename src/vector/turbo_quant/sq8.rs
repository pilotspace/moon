//! SQ8: per-vector asymmetric 8-bit scalar quantization.
//!
//! Unlike TurboQuant (FWHT rotation + a shared sub-byte codebook), SQ8 quantizes
//! each vector independently against its OWN `[min, max]` range. This needs no
//! global calibration pass, which fits moon's per-vector streaming `append` model
//! exactly: a vector can be encoded the instant it arrives, with no dependence on
//! the rest of the dataset.
//!
//! ## Code layout (per vector)
//!
//! `dim` code bytes (one `u8` per dimension) followed by an 8-byte trailer of
//! `(min: f32, scale: f32)`, both little-endian. Total `dim + 8` bytes.
//! (Compare TQ4: `dim/2 + 4`. SQ8 is ~2× the bytes but carries true 8-bit
//! fidelity → near-fp32 recall, which is the point of SQ8 at high dimension where
//! TQ4's concentration-of-distances error bites.)
//!
//! ## Distance
//!
//! Asymmetric (ADC): the query stays full-precision `f32`; only the database
//! vector is dequantized on the fly (`min + code * scale`). [`sq8_l2_adc`] returns
//! squared L2, which is also the correct ranking for **Cosine** when vectors are
//! unit-normalized (`||q-x||² = 2 - 2·cos∠` is monotonic in cosine).
//! [`sq8_ip_adc`] returns the raw inner product for the InnerProduct metric.

/// Bytes appended after the `dim` code bytes: `min: f32` then `scale: f32`, LE.
pub const SQ8_PARAMS_BYTES: usize = 8;

/// Total SQ8 code-slot size for a given dimension: `dim` codes + 8-byte trailer.
#[inline]
pub fn sq8_code_bytes(dim: usize) -> usize {
    dim + SQ8_PARAMS_BYTES
}

/// Encode `vector` into a freshly allocated `dim + 8` byte SQ8 code slot.
///
/// Layout: `[code_0 .. code_{dim-1}][min: f32 LE][scale: f32 LE]`.
pub fn encode_sq8(vector: &[f32]) -> Vec<u8> {
    let mut out = vec![0u8; sq8_code_bytes(vector.len())];
    let (min, scale) = encode_sq8_into(vector, &mut out[..vector.len()]);
    out[vector.len()..vector.len() + 4].copy_from_slice(&min.to_le_bytes());
    out[vector.len() + 4..vector.len() + 8].copy_from_slice(&scale.to_le_bytes());
    out
}

/// Encode `vector` into the caller-provided code byte slice (`codes.len() == dim`)
/// and return the `(min, scale)` pair the caller must store alongside.
///
/// Hot-path friendly: no allocation, the caller owns the destination buffer.
pub fn encode_sq8_into(vector: &[f32], codes: &mut [u8]) -> (f32, f32) {
    debug_assert_eq!(codes.len(), vector.len());
    let mut min = f32::INFINITY;
    let mut max = f32::NEG_INFINITY;
    for &x in vector {
        // Any non-finite coordinate (NaN/±inf) makes the whole vector untrustworthy
        // (a single NaN silently slips past `<`/`>`); zero-encode so no NaN ever
        // leaks into distance math. Empty vectors fall through to the same guard
        // via the unchanged INFINITY sentinels.
        if !x.is_finite() {
            for c in codes.iter_mut() {
                *c = 0;
            }
            return (0.0, 0.0);
        }
        if x < min {
            min = x;
        }
        if x > max {
            max = x;
        }
    }
    // Empty-vector guard: sentinels never updated ⇒ produce a defined zero encoding.
    if !min.is_finite() || !max.is_finite() {
        for c in codes.iter_mut() {
            *c = 0;
        }
        return (0.0, 0.0);
    }
    let range = max - min;
    // Constant vector → scale 0: every value decodes back to `min` exactly.
    let scale = if range > 0.0 { range / 255.0 } else { 0.0 };
    let inv = if scale > 0.0 { 1.0 / scale } else { 0.0 };
    for (c, &x) in codes.iter_mut().zip(vector) {
        // round-to-nearest; clamp guards FP edge cases at the boundaries.
        let q = ((x - min) * inv).round().clamp(0.0, 255.0);
        *c = q as u8;
    }
    (min, scale)
}

/// Read the `(min, scale)` trailer from a full `dim + 8` byte SQ8 code slot.
#[inline]
pub fn sq8_params(slot: &[u8], dim: usize) -> (f32, f32) {
    let min = f32::from_le_bytes([slot[dim], slot[dim + 1], slot[dim + 2], slot[dim + 3]]);
    let scale = f32::from_le_bytes([slot[dim + 4], slot[dim + 5], slot[dim + 6], slot[dim + 7]]);
    (min, scale)
}

/// Dequantize a single code byte: `min + code * scale`.
#[inline]
pub fn sq8_decode_one(code: u8, min: f32, scale: f32) -> f32 {
    min + code as f32 * scale
}

/// Decode the `dim` codes back to an approximate `f32` vector.
pub fn decode_sq8(codes: &[u8], min: f32, scale: f32) -> Vec<f32> {
    codes
        .iter()
        .map(|&c| sq8_decode_one(c, min, scale))
        .collect()
}

/// Asymmetric squared-L2 distance: `||query - decode(codes)||²`.
///
/// `query` stays full precision; each DB coordinate is dequantized on the fly.
/// Correct ranking for both **L2** and (unit-normalized) **Cosine**.
#[inline]
pub fn sq8_l2_adc(query: &[f32], codes: &[u8], min: f32, scale: f32) -> f32 {
    debug_assert_eq!(query.len(), codes.len());
    // 4-way unrolled accumulation to break the serial dependency chain.
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let n = codes.len();
    let chunks = n / 4;
    for c in 0..chunks {
        let i = c * 4;
        let d0 = query[i] - (min + codes[i] as f32 * scale);
        let d1 = query[i + 1] - (min + codes[i + 1] as f32 * scale);
        let d2 = query[i + 2] - (min + codes[i + 2] as f32 * scale);
        let d3 = query[i + 3] - (min + codes[i + 3] as f32 * scale);
        s0 += d0 * d0;
        s1 += d1 * d1;
        s2 += d2 * d2;
        s3 += d3 * d3;
    }
    for i in (chunks * 4)..n {
        let d = query[i] - (min + codes[i] as f32 * scale);
        s0 += d * d;
    }
    s0 + s1 + s2 + s3
}

/// Asymmetric inner product: `<query, decode(codes)>`.
///
/// `<q, x> = min·Σq + scale·Σ(q_i·code_i)`. For the InnerProduct metric, callers
/// negate (smaller = closer) to fit the min-heap HNSW ordering.
#[inline]
pub fn sq8_ip_adc(query: &[f32], codes: &[u8], min: f32, scale: f32) -> f32 {
    debug_assert_eq!(query.len(), codes.len());
    let mut q_sum = 0.0f32;
    let mut dot_code = 0.0f32;
    for (i, &c) in codes.iter().enumerate() {
        q_sum += query[i];
        dot_code += query[i] * c as f32;
    }
    min * q_sum + scale * dot_code
}

// ── SIMD-ready ADC decomposition (HQ-2) ─────────────────────────────────
//
// `sq8_l2_adc`/`sq8_ip_adc` above touch every dimension with a query-and-min
// dependent expression (`query[i] - (min + code[i]*scale)`), which vectorizes
// fine on its own but doesn't factor apart the parts that are pure
// per-QUERY constants — those get recomputed for every beam-search candidate.
//
// Algebraic expansion (finding HQ-2, tmp/VECTOR-DEEP-REVIEW.md): let
// `a_i = q_i - min`, `s = scale`.
//
//   d = Σ(q_i - min - s·c_i)²
//     = Σa_i² - 2s·Σ(a_i·c_i) + s²·Σc_i²
//     = Σa_i² - 2s·(Σq_i·c_i - min·Σc_i) + s²·Σc_i²
//
//   Σa_i² = Σ(q_i - min)² = Σq_i² - 2·min·Σq_i + n·min²
//
// `Σq_i` and `Σq_i²` depend ONLY on the query — compute once per query via
// [`sq8_query_stats`]. The remaining per-candidate work is exactly THREE
// running sums over `dim` elements — `Σ(q_i·c_i)`, `Σc_i`, `Σc_i²` — computed
// together in [`sq8_candidate_stats_scalar`] (SIMD-dispatched via
// `distance::table().sq8_stats`). [`sq8_l2_from_stats`] does the final O(1)
// combine. Same asymptotic work as the naive loop (one pass over `dim` per
// candidate), but every summand is now a clean widen+FMA reduction — the
// shape NEON/AVX2/LLVM vectorize well — instead of an interleaved
// subtract-then-square chain that re-touches `min` and `scale` per element.

/// Per-query precomputation for the fast ADC decomposition: `(Σq_i, Σq_i²)`.
///
/// Call **once per query** (not per candidate) and thread the result through
/// [`sq8_l2_from_stats`] / [`sq8_ip_from_stats`] for every candidate in the
/// beam. `O(dim)`, but paid once instead of once per beam candidate.
#[inline]
pub fn sq8_query_stats(query: &[f32]) -> (f32, f32) {
    let mut sum = 0.0f32;
    let mut sumsq = 0.0f32;
    for &q in query {
        sum += q;
        sumsq += q * q;
    }
    (sum, sumsq)
}

/// Per-candidate ADC statistics: `(Σ(q_i·c_i), Σc_i, Σc_i²)`.
///
/// This is the **only** per-candidate pass over `dim` elements in the fast
/// ADC path — cleanly vectorizable (u8 codes widened to f32, multiplied
/// against the f32 query with FMA, three independent running sums). This
/// scalar version is both the portable fallback (installed into
/// `DistanceTable::sq8_stats` when no SIMD tier is available) and the
/// correctness oracle the NEON/AVX2 kernels are checked against.
///
/// # Panics (debug only)
/// `debug_assert_eq!(query.len(), codes.len())`.
#[inline]
pub fn sq8_candidate_stats_scalar(query: &[f32], codes: &[u8]) -> (f32, f32, f32) {
    debug_assert_eq!(query.len(), codes.len());
    let (mut dot0, mut dot1, mut dot2, mut dot3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut sc0, mut sc1, mut sc2, mut sc3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut sq0, mut sq1, mut sq2, mut sq3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let n = codes.len();
    let chunks = n / 4;
    for c in 0..chunks {
        let i = c * 4;
        let c0 = codes[i] as f32;
        let c1 = codes[i + 1] as f32;
        let c2 = codes[i + 2] as f32;
        let c3 = codes[i + 3] as f32;
        dot0 += query[i] * c0;
        dot1 += query[i + 1] * c1;
        dot2 += query[i + 2] * c2;
        dot3 += query[i + 3] * c3;
        sc0 += c0;
        sc1 += c1;
        sc2 += c2;
        sc3 += c3;
        sq0 += c0 * c0;
        sq1 += c1 * c1;
        sq2 += c2 * c2;
        sq3 += c3 * c3;
    }
    for i in (chunks * 4)..n {
        let c0 = codes[i] as f32;
        dot0 += query[i] * c0;
        sc0 += c0;
        sq0 += c0 * c0;
    }
    (
        (dot0 + dot1) + (dot2 + dot3),
        (sc0 + sc1) + (sc2 + sc3),
        (sq0 + sq1) + (sq2 + sq3),
    )
}

/// Combine per-query stats ([`sq8_query_stats`]) with per-candidate stats
/// ([`sq8_candidate_stats_scalar`] or a SIMD equivalent) into the asymmetric
/// squared-L2 ADC distance. `O(1)` — see the module-level algebra derivation
/// above. Architecture-independent: no SIMD needed here, only in the stats
/// pass.
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn sq8_l2_from_stats(
    dim: usize,
    min: f32,
    scale: f32,
    q_sum: f32,
    q_sumsq: f32,
    dot_qc: f32,
    sum_c: f32,
    sumsq_c: f32,
) -> f32 {
    let n = dim as f32;
    let a_sumsq = q_sumsq - 2.0 * min * q_sum + n * min * min;
    a_sumsq - 2.0 * scale * (dot_qc - min * sum_c) + scale * scale * sumsq_c
}

/// Combine per-query `Σq_i` with per-candidate `Σ(q_i·c_i)` into the
/// asymmetric inner product ADC: `<q, x> = min·Σq_i + scale·Σ(q_i·c_i)`.
/// `O(1)`.
#[inline]
pub fn sq8_ip_from_stats(min: f32, scale: f32, q_sum: f32, dot_qc: f32) -> f32 {
    min * q_sum + scale * dot_qc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random f32 vector in [-1, 1] (no rng dependency).
    fn pseudo_vec(seed: u64, dim: usize) -> Vec<f32> {
        let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        (0..dim)
            .map(|_| {
                s = s
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((s >> 33) as f32 / u32::MAX as f32) * 2.0 - 1.0
            })
            .collect()
    }

    fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    #[test]
    fn test_code_bytes() {
        assert_eq!(sq8_code_bytes(384), 392);
        assert_eq!(sq8_code_bytes(0), 8);
    }

    #[test]
    fn test_roundtrip_error_bounded_by_half_scale() {
        let v = pseudo_vec(7, 384);
        let slot = encode_sq8(&v);
        let (min, scale) = sq8_params(&slot, v.len());
        let dec = decode_sq8(&slot[..v.len()], min, scale);
        // round-to-nearest ⇒ per-coordinate error ≤ scale/2 (+ fp slack).
        for (orig, d) in v.iter().zip(&dec) {
            assert!(
                (orig - d).abs() <= scale * 0.5 + 1e-5,
                "err {} exceeds half-scale {}",
                (orig - d).abs(),
                scale * 0.5
            );
        }
    }

    #[test]
    fn test_exact_match_distance_near_zero() {
        let v = pseudo_vec(11, 256);
        let slot = encode_sq8(&v);
        let (min, scale) = sq8_params(&slot, v.len());
        // ||v - decode(encode(v))||² ≤ Σ(scale/2)² = dim·(scale/2)².
        let bound = v.len() as f32 * (scale * 0.5) * (scale * 0.5) + 1e-4;
        let d = sq8_l2_adc(&v, &slot[..v.len()], min, scale);
        assert!(d <= bound, "self-distance {d} exceeds bound {bound}");
    }

    #[test]
    fn test_l2_adc_approximates_true_distance() {
        let q = pseudo_vec(1, 384);
        let v = pseudo_vec(2, 384);
        let slot = encode_sq8(&v);
        let (min, scale) = sq8_params(&slot, v.len());
        let approx = sq8_l2_adc(&q, &slot[..v.len()], min, scale);
        let truth = l2_sq(&q, &v);
        // Quantization perturbs each db coord by ≤ scale/2; the distance error is
        // bounded but generous here — we only need it close enough to rank well.
        assert!(
            (approx - truth).abs() / truth.max(1e-6) < 0.05,
            "approx {approx} vs truth {truth} differ >5%"
        );
    }

    #[test]
    fn test_nearest_neighbor_ranking_preserved() {
        // The property that actually drives recall: the SQ8-ADC nearest match to a
        // query equals the exact-f32 nearest match, on a small set.
        let q = pseudo_vec(100, 128);
        let db: Vec<Vec<f32>> = (0..50).map(|i| pseudo_vec(200 + i, 128)).collect();
        let exact_best = (0..db.len())
            .min_by(|&a, &b| l2_sq(&q, &db[a]).total_cmp(&l2_sq(&q, &db[b])))
            .unwrap();
        let sq8_best = (0..db.len())
            .min_by(|&a, &b| {
                let sa = encode_sq8(&db[a]);
                let sb = encode_sq8(&db[b]);
                let (mna, sca) = sq8_params(&sa, 128);
                let (mnb, scb) = sq8_params(&sb, 128);
                sq8_l2_adc(&q, &sa[..128], mna, sca).total_cmp(&sq8_l2_adc(
                    &q,
                    &sb[..128],
                    mnb,
                    scb,
                ))
            })
            .unwrap();
        assert_eq!(
            exact_best, sq8_best,
            "SQ8 nearest neighbor diverged from exact"
        );
    }

    #[test]
    fn test_constant_vector_is_lossless() {
        let v = vec![0.42f32; 64];
        let slot = encode_sq8(&v);
        let (min, scale) = sq8_params(&slot, v.len());
        assert_eq!(scale, 0.0, "constant vector must yield zero scale");
        let dec = decode_sq8(&slot[..v.len()], min, scale);
        for d in &dec {
            assert!((d - 0.42).abs() < 1e-6, "constant decode lost value: {d}");
        }
        assert!(sq8_l2_adc(&v, &slot[..v.len()], min, scale) < 1e-6);
    }

    #[test]
    fn test_ip_adc_approximates_dot() {
        let q = pseudo_vec(5, 256);
        let v = pseudo_vec(6, 256);
        let slot = encode_sq8(&v);
        let (min, scale) = sq8_params(&slot, v.len());
        let approx = sq8_ip_adc(&q, &slot[..v.len()], min, scale);
        let truth: f32 = q.iter().zip(&v).map(|(a, b)| a * b).sum();
        assert!(
            (approx - truth).abs() < 0.05 * truth.abs().max(1.0),
            "ip approx {approx} vs truth {truth}"
        );
    }

    #[test]
    fn test_non_finite_guard() {
        let v = vec![f32::NAN, 1.0, 2.0];
        let slot = encode_sq8(&v);
        let (min, scale) = sq8_params(&slot, v.len());
        assert_eq!((min, scale), (0.0, 0.0));
        assert!(slot[..3].iter().all(|&c| c == 0));
    }

    #[test]
    fn test_encode_into_matches_encode() {
        let v = pseudo_vec(99, 100);
        let slot = encode_sq8(&v);
        let mut codes = vec![0u8; v.len()];
        let (min, scale) = encode_sq8_into(&v, &mut codes);
        assert_eq!(&slot[..v.len()], &codes[..]);
        assert_eq!(sq8_params(&slot, v.len()), (min, scale));
    }

    // ── HQ-2: stats-decomposition parity (scalar reference) ─────────────
    //
    // These prove the ALGEBRA is correct in isolation from any SIMD
    // concerns: `sq8_l2_from_stats(sq8_query_stats(q), sq8_candidate_stats_scalar(q, c))`
    // must reproduce the original naive `sq8_l2_adc` (same math, reassociated
    // for vectorization) to tight float tolerance. NEON/AVX2 kernels are
    // checked against `sq8_candidate_stats_scalar` directly in
    // `distance::neon` / `distance::avx2`, so passing here is a precondition
    // for those to mean anything.

    fn l2_via_stats(query: &[f32], codes: &[u8], min: f32, scale: f32) -> f32 {
        let (q_sum, q_sumsq) = sq8_query_stats(query);
        let (dot_qc, sum_c, sumsq_c) = sq8_candidate_stats_scalar(query, codes);
        sq8_l2_from_stats(
            query.len(),
            min,
            scale,
            q_sum,
            q_sumsq,
            dot_qc,
            sum_c,
            sumsq_c,
        )
    }

    fn ip_via_stats(query: &[f32], codes: &[u8], min: f32, scale: f32) -> f32 {
        let (q_sum, _) = sq8_query_stats(query);
        let (dot_qc, _, _) = sq8_candidate_stats_scalar(query, codes);
        sq8_ip_from_stats(min, scale, q_sum, dot_qc)
    }

    #[test]
    fn test_stats_l2_matches_naive_adc() {
        for &dim in &[1usize, 2, 3, 7, 8, 15, 16, 31, 32, 63, 100, 128, 384, 768] {
            let q = pseudo_vec(1000 + dim as u64, dim);
            let v = pseudo_vec(2000 + dim as u64, dim);
            let slot = encode_sq8(&v);
            let (min, scale) = sq8_params(&slot, dim);
            let naive = sq8_l2_adc(&q, &slot[..dim], min, scale);
            let fast = l2_via_stats(&q, &slot[..dim], min, scale);
            let rel = (naive - fast).abs() / naive.max(1e-6);
            assert!(
                rel < 1e-3,
                "dim={dim}: naive={naive} fast(stats)={fast} rel={rel}"
            );
        }
    }

    #[test]
    fn test_stats_negative_min_and_extreme_scale_matches_naive() {
        // sq8_params stores the vector's literal min/max — for zero-centered
        // embeddings (MiniLM-style, the real workload per CLAUDE.md) `min` is
        // routinely negative. Also stress the `scale` extremes: near-zero
        // (near-constant vector -> tiny max-min spread) and large (values
        // scaled up 1000x -> big spread), since `scale` is squared in the
        // `s²·Σc²` term of the expansion and errors there compound fastest.
        let dim = 384usize;
        let base = pseudo_vec(5000, dim); // negative-min by construction ([-1,1) range)
        let q = pseudo_vec(6000, dim);

        let cases: [(&str, Vec<f32>); 3] = [
            ("normal_negative_min", base.clone()),
            ("large_scale", base.iter().map(|&x| x * 1000.0).collect()),
            (
                "tiny_scale_near_constant",
                base.iter().map(|&x| 5.0 + x * 1e-4).collect(),
            ),
        ];

        for (name, v) in &cases {
            let slot = encode_sq8(v);
            let (min, scale) = sq8_params(&slot, dim);
            assert!(
                min < 0.0 || *name != "normal_negative_min",
                "case {name}: expected negative min, got {min}"
            );
            let naive = sq8_l2_adc(&q, &slot[..dim], min, scale);
            let fast = l2_via_stats(&q, &slot[..dim], min, scale);
            let rel = (naive - fast).abs() / naive.max(1e-6);
            assert!(
                rel < 1e-3,
                "case {name}: naive={naive} fast(stats)={fast} rel={rel} min={min} scale={scale}"
            );
        }
    }

    #[test]
    fn test_stats_ip_matches_naive_adc() {
        for &dim in &[1usize, 7, 16, 100, 256, 384] {
            let q = pseudo_vec(3000 + dim as u64, dim);
            let v = pseudo_vec(4000 + dim as u64, dim);
            let slot = encode_sq8(&v);
            let (min, scale) = sq8_params(&slot, dim);
            let naive = sq8_ip_adc(&q, &slot[..dim], min, scale);
            let fast = ip_via_stats(&q, &slot[..dim], min, scale);
            let rel = (naive - fast).abs() / naive.abs().max(1.0);
            assert!(
                rel < 1e-3,
                "dim={dim}: naive={naive} fast(stats)={fast} rel={rel}"
            );
        }
    }

    #[test]
    fn test_stats_self_distance_near_zero_matches_naive_absolute() {
        // Advisor must-fix: the algebraic expansion `d = Σq² - 2·min·Σq +
        // n·min² - 2s(Σqc - min·Σc) + s²·Σc²` is a difference of large
        // near-equal terms — catastrophic cancellation is worst exactly
        // where the true distance is near zero, i.e. for the near-neighbor
        // candidates whose ranking determines recall. The naive
        // `sq8_l2_adc` (direct sum-of-squares) is unconditionally
        // well-conditioned there. All other stats-path parity tests above
        // use far-apart vectors + *relative* tolerance, which structurally
        // hides this failure mode. Assert *absolute* agreement in the
        // exact-match regime instead, mirroring
        // `test_exact_match_distance_near_zero`'s bound.
        for &dim in &[16usize, 64, 128, 256, 384, 768] {
            let v = pseudo_vec(9000 + dim as u64, dim);
            let slot = encode_sq8(&v);
            let (min, scale) = sq8_params(&slot, dim);
            let naive = sq8_l2_adc(&v, &slot[..dim], min, scale);
            let fast = l2_via_stats(&v, &slot[..dim], min, scale);
            // Same quantization bound as test_exact_match_distance_near_zero,
            // plus slack for the expansion's extra cancellation error.
            let bound = dim as f32 * (scale * 0.5) * (scale * 0.5) + 1e-3;
            assert!(
                fast <= bound,
                "dim={dim}: stats self-distance {fast} exceeds bound {bound} (naive={naive})"
            );
            assert!(
                fast >= -1e-3,
                "dim={dim}: stats self-distance {fast} is meaningfully negative (naive={naive})"
            );
            let abs_err = (naive - fast).abs();
            assert!(
                abs_err < 1e-2,
                "dim={dim}: naive={naive} fast(stats)={fast} abs_err={abs_err} exceeds absolute bound"
            );
        }
    }

    #[test]
    fn test_stats_zero_dim_is_zero() {
        let (q_sum, q_sumsq) = sq8_query_stats(&[]);
        assert_eq!((q_sum, q_sumsq), (0.0, 0.0));
        let (dot, sc, sq) = sq8_candidate_stats_scalar(&[], &[]);
        assert_eq!((dot, sc, sq), (0.0, 0.0, 0.0));
        assert_eq!(sq8_l2_from_stats(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 0.0);
    }

    #[test]
    fn test_stats_nearest_neighbor_ranking_preserved() {
        // Same property as `test_nearest_neighbor_ranking_preserved` but for
        // the stats-decomposed distance — this is what the beam search will
        // actually rank candidates with once dispatched.
        let dim = 128;
        let q = pseudo_vec(100, dim);
        let db: Vec<Vec<f32>> = (0..50).map(|i| pseudo_vec(200 + i, dim)).collect();
        let exact_best = (0..db.len())
            .min_by(|&a, &b| l2_sq(&q, &db[a]).total_cmp(&l2_sq(&q, &db[b])))
            .unwrap();
        let (q_sum, q_sumsq) = sq8_query_stats(&q);
        let sq8_best = (0..db.len())
            .min_by(|&a, &b| {
                let sa = encode_sq8(&db[a]);
                let sb = encode_sq8(&db[b]);
                let (mna, sca) = sq8_params(&sa, dim);
                let (mnb, scb) = sq8_params(&sb, dim);
                let (dot_a, sc_a, sq_a) = sq8_candidate_stats_scalar(&q, &sa[..dim]);
                let (dot_b, sc_b, sq_b) = sq8_candidate_stats_scalar(&q, &sb[..dim]);
                sq8_l2_from_stats(dim, mna, sca, q_sum, q_sumsq, dot_a, sc_a, sq_a).total_cmp(
                    &sq8_l2_from_stats(dim, mnb, scb, q_sum, q_sumsq, dot_b, sc_b, sq_b),
                )
            })
            .unwrap();
        assert_eq!(
            exact_best, sq8_best,
            "stats-decomposed SQ8 nearest neighbor diverged from exact"
        );
    }
}
