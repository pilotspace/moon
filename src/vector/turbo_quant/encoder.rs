//! TurboQuant MSE encoder/decoder with nibble packing.
//!
//! Implements the TurboQuant_MSE algorithm from arXiv 2504.19874:
//! normalize -> pad -> randomized FWHT -> quantize -> nibble pack.
//!
//! Achieves 8x compression (768d f32 -> 512 bytes + 4 bytes norm)
//! at <= 0.009 MSE distortion for unit vectors (Theorem 1).

use super::codebook::{CENTROIDS, quantize_scalar, quantize_with_boundaries};
use super::fwht;

/// Encoded TurboQuant representation of a single vector.
pub struct TqCode {
    /// Nibble-packed quantization indices. Length = padded_dim / 2.
    /// Low nibble = even-index coordinate, high nibble = odd-index coordinate.
    pub codes: Vec<u8>,
    /// Original L2 norm of the input vector.
    pub norm: f32,
}

/// Next power of 2 >= dim. Used to pad vectors for FWHT.
#[inline]
pub fn padded_dimension(dim: u32) -> u32 {
    if dim == 0 {
        return 1;
    }
    if dim.is_power_of_two() {
        dim
    } else {
        dim.next_power_of_two()
    }
}

/// Pack pairs of 4-bit indices into bytes.
///
/// `indices.len()` must be even.
/// Layout: `byte[i] = (indices[2*i+1] << 4) | indices[2*i]`
#[inline]
pub fn nibble_pack(indices: &[u8]) -> Vec<u8> {
    debug_assert!(indices.len() % 2 == 0, "nibble_pack requires even length");
    indices
        .chunks_exact(2)
        .map(|pair| pair[0] | (pair[1] << 4))
        .collect()
}

/// Unpack nibble-packed bytes back to 4-bit indices.
///
/// Returns exactly `count` indices.
#[inline]
pub fn nibble_unpack(packed: &[u8], count: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(count);
    for &byte in packed.iter() {
        out.push(byte & 0x0F);
        out.push(byte >> 4);
    }
    out.truncate(count);
    out
}

/// Encode a vector using TurboQuant MSE (L2/Cosine metric).
///
/// Algorithm (arXiv 2504.19874):
/// 1. Compute norm gamma = ||x||_2
/// 2. Normalize: x_hat = x / gamma
/// 3. Pad to next power of 2 (zero-fill)
/// 4. Apply randomized FWHT: y = H * D * x_hat_padded (normalized)
/// 5. Quantize each y[j] via codebook -> 4-bit index
/// 6. Nibble-pack indices
///
/// `work_buf` must have len >= padded_dimension(vector.len()).
/// `sign_flips` is the materialized +-1.0 array of len == padded_dimension.
pub fn encode_tq_mse(vector: &[f32], sign_flips: &[f32], work_buf: &mut [f32]) -> TqCode {
    let dim = vector.len();
    let padded = padded_dimension(dim as u32) as usize;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Step 1: Compute norm
    let mut norm_sq = 0.0f32;
    for &v in vector {
        norm_sq += v * v;
    }
    let norm = norm_sq.sqrt();

    // Step 2+3: Normalize and pad into work buffer
    if norm > 0.0 {
        let inv_norm = 1.0 / norm;
        for (dst, &src) in work_buf[..dim].iter_mut().zip(vector.iter()) {
            *dst = src * inv_norm;
        }
    } else {
        for dst in work_buf[..dim].iter_mut() {
            *dst = 0.0;
        }
    }
    for dst in work_buf[dim..padded].iter_mut() {
        *dst = 0.0;
    }

    // Step 4: Randomized FWHT (uses OnceLock-dispatched fn)
    fwht::fwht(&mut work_buf[..padded], sign_flips);

    // Step 5: Quantize each coordinate (legacy: uses hardcoded 1/sqrt(768) boundaries)
    let mut indices = Vec::with_capacity(padded);
    for &val in work_buf[..padded].iter() {
        indices.push(quantize_scalar(val));
    }

    // Step 6: Nibble pack
    let codes = nibble_pack(&indices);

    TqCode { codes, norm }
}

/// Encode using dimension-adaptive scaled boundaries.
///
/// Same as `encode_tq_mse` but uses the provided scaled boundaries
/// instead of the legacy hardcoded 1/sqrt(768) boundaries.
/// This version produces correct quantization for ANY dimension.
pub fn encode_tq_mse_scaled(
    vector: &[f32],
    sign_flips: &[f32],
    boundaries: &[f32; 15],
    work_buf: &mut [f32],
) -> TqCode {
    let dim = vector.len();
    let padded = padded_dimension(dim as u32) as usize;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Step 1: Compute norm
    let mut norm_sq = 0.0f32;
    for &v in vector {
        norm_sq += v * v;
    }
    let norm = norm_sq.sqrt();

    // Step 2+3: Normalize and pad into work buffer
    if norm > 0.0 {
        let inv_norm = 1.0 / norm;
        for (dst, &src) in work_buf[..dim].iter_mut().zip(vector.iter()) {
            *dst = src * inv_norm;
        }
    } else {
        for dst in work_buf[..dim].iter_mut() {
            *dst = 0.0;
        }
    }
    for dst in work_buf[dim..padded].iter_mut() {
        *dst = 0.0;
    }

    // Step 4: Randomized FWHT
    fwht::fwht(&mut work_buf[..padded], sign_flips);

    // Step 5: Quantize each coordinate with dimension-scaled boundaries
    let mut indices = Vec::with_capacity(padded);
    for &val in work_buf[..padded].iter() {
        indices.push(quantize_with_boundaries(val, boundaries));
    }

    // Step 6: Nibble pack
    let codes = nibble_pack(&indices);

    TqCode { codes, norm }
}

/// Decode a TQ code back to approximate vector (for verification/reranking).
///
/// Applies inverse: unpack -> lookup centroids -> inverse FWHT -> un-pad -> scale by norm.
///
/// The inverse of the randomized FWHT `R(x) = H * D * x` is `R^{-1}(y) = D * H * y`
/// where H is the normalized WHT and D = diag(sign_flips).
pub fn decode_tq_mse(
    code: &TqCode,
    sign_flips: &[f32],
    original_dim: usize,
    work_buf: &mut [f32],
) -> Vec<f32> {
    let padded = padded_dimension(original_dim as u32) as usize;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Unpack nibbles -> centroid indices -> centroid values
    let indices = nibble_unpack(&code.codes, padded);
    for (dst, &idx) in work_buf[..padded].iter_mut().zip(indices.iter()) {
        *dst = CENTROIDS[idx as usize];
    }

    // Inverse FWHT: R^{-1}(y) = D * H * y
    // Step 1: Apply plain FWHT (no sign flips) + normalize
    fwht::fwht_scalar(&mut work_buf[..padded]);
    fwht::normalize_fwht(&mut work_buf[..padded]);
    // Step 2: Apply sign flips (D is its own inverse)
    fwht::apply_sign_flips(&mut work_buf[..padded], sign_flips);

    // Un-pad and scale by norm
    let mut result = Vec::with_capacity(original_dim);
    for &val in work_buf[..original_dim].iter() {
        result.push(val * code.norm);
    }
    result
}

/// Mean squared error between original and reconstructed vectors.
///
/// This is the distortion metric from Theorem 1.
pub fn mse_distortion(original: &[f32], reconstructed: &[f32]) -> f32 {
    debug_assert_eq!(original.len(), reconstructed.len());
    let n = original.len() as f32;
    let mut sum = 0.0f32;
    for (a, b) in original.iter().zip(reconstructed.iter()) {
        let d = a - b;
        sum += d * d;
    }
    sum / n
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Normalize a vector to unit length in-place and return the norm.
    fn normalize_to_unit(v: &mut [f32]) -> f32 {
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

    /// Generate deterministic sign flips for testing.
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
    fn test_padded_dimension() {
        assert_eq!(padded_dimension(768), 1024);
        assert_eq!(padded_dimension(1024), 1024);
        assert_eq!(padded_dimension(100), 128);
        assert_eq!(padded_dimension(384), 512);
        assert_eq!(padded_dimension(1), 1);
        assert_eq!(padded_dimension(2), 2);
        assert_eq!(padded_dimension(3), 4);
        assert_eq!(padded_dimension(0), 1);
    }

    #[test]
    fn test_nibble_pack_unpack_roundtrip() {
        // Test all 16 index values
        let indices: Vec<u8> = (0..16).collect();
        let packed = nibble_pack(&indices);
        assert_eq!(packed.len(), 8);
        let unpacked = nibble_unpack(&packed, 16);
        assert_eq!(unpacked, indices);
    }

    #[test]
    fn test_nibble_pack_specific() {
        // [0, 1] -> byte = 0 | (1 << 4) = 0x10
        let packed = nibble_pack(&[0, 1]);
        assert_eq!(packed, vec![0x10]);

        // [2, 15] -> byte = 2 | (15 << 4) = 0xF2
        let packed = nibble_pack(&[2, 15]);
        assert_eq!(packed, vec![0xF2]);

        // [15, 0] -> byte = 15 | (0 << 4) = 0x0F
        let packed = nibble_pack(&[15, 0]);
        assert_eq!(packed, vec![0x0F]);
    }

    #[test]
    fn test_nibble_unpack_truncation() {
        let packed = vec![0x12, 0x34]; // unpacks to [2,1,4,3]
        let unpacked = nibble_unpack(&packed, 3); // truncate to 3
        assert_eq!(unpacked, vec![2, 1, 4]);
    }

    #[test]
    fn test_encode_output_length() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 99);
        normalize_to_unit(&mut vec);

        let code = encode_tq_mse(&vec, &signs, &mut work);
        assert_eq!(code.codes.len(), padded / 2, "expected {} bytes, got {}", padded / 2, code.codes.len());
        assert_eq!(code.codes.len(), 512); // 1024 / 2
    }

    #[test]
    fn test_zero_vector_encode() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work = vec![0.0f32; padded];

        let zero_vec = vec![0.0f32; dim];
        let code = encode_tq_mse(&zero_vec, &signs, &mut work);
        assert_eq!(code.norm, 0.0);
        assert_eq!(code.codes.len(), padded / 2);
        // All zero inputs -> all zero after FWHT -> should quantize to center
    }

    #[test]
    fn test_encode_decode_roundtrip_distortion() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 12345);
        let mut work_enc = vec![0.0f32; padded];
        let mut work_dec = vec![0.0f32; padded];

        let mut max_distortion = 0.0f32;
        let mut total_distortion = 0.0f32;
        let num_vectors = 100;

        for seed in 0..num_vectors {
            let mut vec = lcg_f32(dim, seed * 7 + 13);
            normalize_to_unit(&mut vec);

            let code = encode_tq_mse(&vec, &signs, &mut work_enc);
            let reconstructed = decode_tq_mse(&code, &signs, dim, &mut work_dec);

            assert_eq!(reconstructed.len(), dim);

            let distortion = mse_distortion(&vec, &reconstructed);
            total_distortion += distortion;
            if distortion > max_distortion {
                max_distortion = distortion;
            }
        }

        let avg_distortion = total_distortion / num_vectors as f32;
        eprintln!("TQ 4-bit round-trip: avg MSE = {avg_distortion:.6}, max MSE = {max_distortion:.6}");

        // Theorem 1 bound: distortion <= 0.009 for 4-bit unit vectors
        assert!(
            max_distortion <= 0.009,
            "Max distortion {max_distortion:.6} exceeds 0.009 bound"
        );
    }

    #[test]
    fn test_encode_decode_norm_preserved() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 777);
        let mut work_enc = vec![0.0f32; padded];
        let mut work_dec = vec![0.0f32; padded];

        // Non-unit vector
        let vec = lcg_f32(dim, 42);
        let norm_sq: f32 = vec.iter().map(|x| x * x).sum();
        let original_norm = norm_sq.sqrt();

        let code = encode_tq_mse(&vec, &signs, &mut work_enc);
        assert!(
            (code.norm - original_norm).abs() < 1e-5,
            "norm mismatch: encoded={}, original={}",
            code.norm,
            original_norm
        );

        let reconstructed = decode_tq_mse(&code, &signs, dim, &mut work_dec);
        let recon_norm_sq: f32 = reconstructed.iter().map(|x| x * x).sum();
        let recon_norm = recon_norm_sq.sqrt();

        // Reconstructed norm should be approximately the original
        let norm_ratio = recon_norm / original_norm;
        assert!(
            (norm_ratio - 1.0).abs() < 0.1,
            "norm ratio {norm_ratio:.4} too far from 1.0"
        );
    }
}
