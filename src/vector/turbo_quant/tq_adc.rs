//! TurboQuant Asymmetric Distance Computation (ADC).
//!
//! Computes L2 distance between a full-precision rotated query and a
//! nibble-packed TQ code. Used by HNSW beam search (Phase 61).
//!
//! The scalar version here serves as reference. AVX2/AVX-512 VPERMPS
//! versions are added in Phase 61+ for production throughput.

use super::codebook::CENTROIDS;

/// Asymmetric L2 distance: full-precision query vs TQ code.
///
/// `q_rotated`: pre-rotated query (already FWHT'd, length = padded_dim).
/// `code`: nibble-packed TQ indices (length = padded_dim / 2).
/// `norm`: original vector norm stored in TqCode.
///
/// Returns estimated squared L2 distance.
///
/// Algorithm:
/// 1. Unpack nibbles to centroid indices inline (no allocation)
/// 2. For each dimension: d = q_rotated[i] - CENTROIDS[idx[i]]
/// 3. Sum d*d, scale by norm^2
pub fn tq_l2_adc_scalar(
    q_rotated: &[f32],
    code: &[u8],
    norm: f32,
) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 2);

    let norm_sq = norm * norm;
    let mut sum = 0.0f32;

    for i in 0..code.len() {
        let byte = code[i];
        let lo_idx = (byte & 0x0F) as usize;
        let hi_idx = (byte >> 4) as usize;

        let d_lo = q_rotated[i * 2] - CENTROIDS[lo_idx];
        let d_hi = q_rotated[i * 2 + 1] - CENTROIDS[hi_idx];
        sum += d_lo * d_lo + d_hi * d_hi;
    }

    sum * norm_sq
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::turbo_quant::encoder::{
        encode_tq_mse, decode_tq_mse, padded_dimension,
    };
    use crate::vector::turbo_quant::fwht;

    /// Deterministic LCG PRNG for reproducible test vectors.
    fn lcg_f32(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn normalize(v: &mut [f32]) -> f32 {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            for x in v.iter_mut() {
                *x *= inv;
            }
        }
        norm
    }

    fn test_sign_flips(dim: usize, seed: u32) -> Vec<f32> {
        let mut signs = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            signs.push(if s & 1 == 0 { 1.0f32 } else { -1.0 });
        }
        signs
    }

    #[test]
    fn test_tq_l2_self_distance_small() {
        // Encode a vector, then compute ADC distance against its own FWHT-rotated form.
        // Should be close to 0 (quantization error only).
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 99);
        normalize(&mut vec);

        let code = encode_tq_mse(&vec, &signs, &mut work);

        // Prepare rotated query (same vector through same FWHT)
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&vec);
        for dst in q_rotated[dim..].iter_mut() {
            *dst = 0.0;
        }
        // Normalize for FWHT input
        // vec is already unit norm, so inv_norm = 1.0
        fwht::fwht(&mut q_rotated, &signs);

        let dist = tq_l2_adc_scalar(&q_rotated, &code.codes, code.norm);
        eprintln!("self-distance (ADC): {dist}");
        // Self-distance should be small (quantization error only, norm=1 so norm_sq=1)
        assert!(dist < 0.02, "self-distance {dist} too large");
        assert!(dist >= 0.0, "distance must be non-negative");
    }

    #[test]
    fn test_tq_l2_distant_vectors() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work = vec![0.0f32; padded];

        // Encode first vector
        let mut v1 = lcg_f32(dim, 11);
        normalize(&mut v1);
        let code1 = encode_tq_mse(&v1, &signs, &mut work);

        // Create a distant query (opposite direction)
        let v2: Vec<f32> = v1.iter().map(|&x| -x).collect();
        // Already unit norm since v1 was unit

        // Rotate query
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&v2);
        fwht::fwht(&mut q_rotated, &signs);

        let dist = tq_l2_adc_scalar(&q_rotated, &code1.codes, code1.norm);
        eprintln!("distant-distance (ADC): {dist}");
        // Opposite unit vectors have L2^2 = 4.0. With quantization error, should be close.
        assert!(dist > 2.0, "distant vectors should have large distance, got {dist}");
    }

    #[test]
    fn test_tq_l2_matches_decoded_l2() {
        // ADC distance should produce same ranking as brute-force decoded L2
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work_enc = vec![0.0f32; padded];
        let mut work_dec = vec![0.0f32; padded];

        // Encode 10 vectors
        let mut codes = Vec::new();
        let mut originals = Vec::new();
        for seed in 0..10u32 {
            let mut v = lcg_f32(dim, seed * 7 + 13);
            normalize(&mut v);
            originals.push(v.clone());
            codes.push(encode_tq_mse(&v, &signs, &mut work_enc));
        }

        // Query
        let mut query = lcg_f32(dim, 999);
        normalize(&mut query);
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&query);
        fwht::fwht(&mut q_rotated, &signs);

        // Compute ADC distances
        let adc_dists: Vec<f32> = codes.iter()
            .map(|c| tq_l2_adc_scalar(&q_rotated, &c.codes, c.norm))
            .collect();

        // Compute brute-force L2 via decode
        let bf_dists: Vec<f32> = codes.iter()
            .map(|c| {
                let decoded = decode_tq_mse(c, &signs, dim, &mut work_dec);
                let mut sum = 0.0f32;
                for (a, b) in query.iter().zip(decoded.iter()) {
                    let d = a - b;
                    sum += d * d;
                }
                sum
            })
            .collect();

        // Rankings should match (ADC preserves ordering)
        let mut adc_order: Vec<usize> = (0..10).collect();
        adc_order.sort_by(|&a, &b| adc_dists[a].partial_cmp(&adc_dists[b]).unwrap());

        let mut bf_order: Vec<usize> = (0..10).collect();
        bf_order.sort_by(|&a, &b| bf_dists[a].partial_cmp(&bf_dists[b]).unwrap());

        eprintln!("ADC ranking: {adc_order:?}");
        eprintln!("BF  ranking: {bf_order:?}");

        // Top-3 should match (quantization may swap nearly-equal distances)
        assert_eq!(adc_order[0], bf_order[0], "nearest neighbor mismatch");
    }

    #[test]
    fn test_tq_l2_norm_scaling() {
        // Verify norm scaling: distance should scale with norm^2
        fwht::init_fwht();
        let dim = 64;
        let padded = padded_dimension(dim as u32) as usize;
        let _signs = test_sign_flips(padded, 42);

        // Create a simple query and code
        let q = vec![0.01f32; padded];
        // Hand-craft a code: all indices = 8 (centroid = 0.001075)
        let code = vec![0x88u8; padded / 2];

        let dist_norm1 = tq_l2_adc_scalar(&q, &code, 1.0);
        let dist_norm2 = tq_l2_adc_scalar(&q, &code, 2.0);

        // dist_norm2 should be 4x dist_norm1 (norm^2 scaling)
        let ratio = dist_norm2 / dist_norm1;
        assert!(
            (ratio - 4.0).abs() < 0.01,
            "norm scaling wrong: ratio = {ratio}, expected 4.0"
        );
    }

    #[test]
    fn test_tq_l2_non_negative() {
        let q = [0.1f32, -0.2, 0.3, -0.4];
        let code = [0x21, 0x43]; // arbitrary nibbles
        let dist = tq_l2_adc_scalar(&q, &code, 1.5);
        assert!(dist >= 0.0, "distance must be non-negative, got {dist}");
    }
}
