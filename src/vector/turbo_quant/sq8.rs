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
}
