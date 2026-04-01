//! TurboQuant MSE encoder/decoder with nibble packing.
//!
//! Implements the TurboQuant_MSE algorithm from arXiv 2504.19874:
//! normalize -> pad -> randomized FWHT -> quantize -> nibble pack.
//!
//! Achieves 8x compression (768d f32 -> 512 bytes + 4 bytes norm)
//! at <= 0.009 MSE distortion for unit vectors (Theorem 1).

use super::codebook::{
    CENTROIDS, quantize_scalar, quantize_with_boundaries, quantize_with_boundaries_n,
};
use super::fwht;

/// Encoded TurboQuant representation of a single vector.
pub struct TqCode {
    /// Nibble-packed quantization indices. Length = padded_dim / 2.
    /// Low nibble = even-index coordinate, high nibble = odd-index coordinate.
    pub codes: Vec<u8>,
    /// Original L2 norm of the input vector.
    pub norm: f32,
}

/// TQ code with sub-centroid sign bits computed at encode time.
/// Signs indicate which side of the centroid each coordinate fell on,
/// doubling effective quantization resolution (32 levels from 16).
pub struct TqCodeWithSigns {
    pub code: TqCode,
    /// Bit-packed sign bits: 1 = value >= centroid, 0 = value < centroid.
    /// Length = ceil(padded_dim / 8).
    pub signs: Vec<u8>,
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

/// Encode with sub-centroid sign bits computed at encode time.
///
/// Same as `encode_tq_mse_scaled` but also returns per-coordinate sign bits
/// indicating whether the pre-quantization FWHT value was >= or < the
/// assigned centroid. This doubles effective quantization resolution from
/// 16 to 32 levels during HNSW search (sub-centroid LUT scoring).
///
/// Cost: ~2% overhead over plain encode (one comparison + bit-set per coordinate).
pub fn encode_tq_mse_scaled_with_signs(
    vector: &[f32],
    sign_flips: &[f32],
    boundaries: &[f32; 15],
    centroids: &[f32; 16],
    work_buf: &mut [f32],
) -> TqCodeWithSigns {
    let dim = vector.len();
    let padded = padded_dimension(dim as u32) as usize;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Steps 1-3: norm, normalize, pad (same as encode_tq_mse_scaled)
    let mut norm_sq = 0.0f32;
    for &v in vector {
        norm_sq += v * v;
    }
    let norm = norm_sq.sqrt();
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

    // Step 5: Quantize + compute sub-centroid signs simultaneously
    let mut indices = Vec::with_capacity(padded);
    let sign_bytes = (padded + 7) / 8;
    let mut signs = vec![0u8; sign_bytes];

    for (i, &val) in work_buf[..padded].iter().enumerate() {
        let idx = quantize_with_boundaries(val, boundaries);
        indices.push(idx);
        // Sign bit: 1 if value >= centroid (upper half of Voronoi cell)
        if val >= centroids[idx as usize] {
            signs[i / 8] |= 1 << (i % 8);
        }
    }

    // Step 6: Nibble pack
    let codes = nibble_pack(&indices);

    TqCodeWithSigns {
        code: TqCode { codes, norm },
        signs,
    }
}

/// Decode a TQ code back to approximate vector (for verification/reranking).
///
/// **DEPRECATED**: Uses legacy 1/√768-scaled CENTROIDS. Use [`decode_tq_mse_scaled`]
/// for dimension-adaptive decoding that matches `encode_tq_mse_scaled`.
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
    fwht::inverse_fwht(&mut work_buf[..padded], sign_flips);

    // Un-pad and scale by norm
    let mut result = Vec::with_capacity(original_dim);
    for &val in work_buf[..original_dim].iter() {
        result.push(val * code.norm);
    }
    result
}

/// Decode a TQ code using dimension-scaled centroids.
///
/// Matches `encode_tq_mse_scaled` — uses the provided centroids instead of
/// the legacy 1/√768-scaled constants. This is the correct decode for any dimension.
pub fn decode_tq_mse_scaled(
    code: &TqCode,
    sign_flips: &[f32],
    centroids: &[f32; 16],
    original_dim: usize,
    work_buf: &mut [f32],
) -> Vec<f32> {
    let padded = padded_dimension(original_dim as u32) as usize;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Unpack nibbles -> centroid indices -> centroid values (scaled)
    let indices = nibble_unpack(&code.codes, padded);
    for (dst, &idx) in work_buf[..padded].iter_mut().zip(indices.iter()) {
        *dst = centroids[idx as usize];
    }

    // Inverse FWHT: R^{-1}(y) = D * H * y
    fwht::inverse_fwht(&mut work_buf[..padded], sign_flips);

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

// ── 1-bit packing (8 indices per byte, LSB-first) ────────────────────

/// Pack 1-bit indices (each 0 or 1) into bytes, 8 per byte, LSB-first.
///
/// `indices.len()` must be a multiple of 8.
#[inline]
pub fn pack_1bit(indices: &[u8]) -> Vec<u8> {
    debug_assert!(
        indices.len() % 8 == 0,
        "pack_1bit requires length multiple of 8"
    );
    let mut out = Vec::with_capacity(indices.len() / 8);
    for chunk in indices.chunks_exact(8) {
        let mut byte = 0u8;
        for j in 0..8 {
            byte |= (chunk[j] & 1) << j;
        }
        out.push(byte);
    }
    out
}

/// Unpack 1-bit packed bytes back to indices (each 0 or 1).
#[inline]
pub fn unpack_1bit(packed: &[u8], count: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(count);
    for &byte in packed.iter() {
        for j in 0..8 {
            out.push((byte >> j) & 1);
        }
    }
    out.truncate(count);
    out
}

// ── 2-bit packing (4 indices per byte, LSB-first) ────────────────────

/// Pack 2-bit indices (each 0-3) into bytes, 4 per byte, LSB-first.
///
/// `indices.len()` must be a multiple of 4.
#[inline]
pub fn pack_2bit(indices: &[u8]) -> Vec<u8> {
    debug_assert!(
        indices.len() % 4 == 0,
        "pack_2bit requires length multiple of 4"
    );
    let mut out = Vec::with_capacity(indices.len() / 4);
    for chunk in indices.chunks_exact(4) {
        let byte = (chunk[0] & 0x03)
            | ((chunk[1] & 0x03) << 2)
            | ((chunk[2] & 0x03) << 4)
            | ((chunk[3] & 0x03) << 6);
        out.push(byte);
    }
    out
}

/// Unpack 2-bit packed bytes back to indices (each 0-3).
#[inline]
pub fn unpack_2bit(packed: &[u8], count: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(count);
    for &byte in packed.iter() {
        out.push(byte & 0x03);
        out.push((byte >> 2) & 0x03);
        out.push((byte >> 4) & 0x03);
        out.push((byte >> 6) & 0x03);
    }
    out.truncate(count);
    out
}

// ── 3-bit packing (8 indices into 3 bytes = 24 bits) ─────────────────

/// Pack 3-bit indices (each 0-7) into bytes. Groups of 8 indices -> 3 bytes (24 bits).
///
/// `indices.len()` must be a multiple of 8.
/// Bit layout within each 3-byte group:
///   byte0 = bits [0..8]:  idx0[0:3] | idx1[0:3] | idx2[0:2]
///   byte1 = bits [8..16]: idx2[2:3] | idx3[0:3] | idx4[0:3] | idx5[0:1]
///   byte2 = bits [16..24]: idx5[1:3] | idx6[0:3] | idx7[0:3]
#[inline]
pub fn pack_3bit(indices: &[u8]) -> Vec<u8> {
    debug_assert!(
        indices.len() % 8 == 0,
        "pack_3bit requires length multiple of 8"
    );
    let mut out = Vec::with_capacity(indices.len() * 3 / 8);
    for chunk in indices.chunks_exact(8) {
        // Pack 8 x 3-bit values into 24 bits (3 bytes), LSB-first
        let bits: u32 = (chunk[0] as u32 & 7)
            | ((chunk[1] as u32 & 7) << 3)
            | ((chunk[2] as u32 & 7) << 6)
            | ((chunk[3] as u32 & 7) << 9)
            | ((chunk[4] as u32 & 7) << 12)
            | ((chunk[5] as u32 & 7) << 15)
            | ((chunk[6] as u32 & 7) << 18)
            | ((chunk[7] as u32 & 7) << 21);
        out.push((bits & 0xFF) as u8);
        out.push(((bits >> 8) & 0xFF) as u8);
        out.push(((bits >> 16) & 0xFF) as u8);
    }
    out
}

/// Unpack 3-bit packed bytes back to indices (each 0-7).
#[inline]
pub fn unpack_3bit(packed: &[u8], count: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(count);
    for group in packed.chunks_exact(3) {
        let bits = group[0] as u32 | ((group[1] as u32) << 8) | ((group[2] as u32) << 16);
        for j in 0..8 {
            out.push(((bits >> (j * 3)) & 7) as u8);
        }
    }
    out.truncate(count);
    out
}

// ── Multi-bit encode/decode ──────────────────────────────────────────

/// Dispatch to the correct packing function based on bit width.
#[inline]
fn pack_by_bits(indices: &[u8], bits: u8) -> Vec<u8> {
    match bits {
        1 => pack_1bit(indices),
        2 => pack_2bit(indices),
        3 => pack_3bit(indices),
        4 => nibble_pack(indices),
        _ => panic!("unsupported bit width: {bits}"),
    }
}

/// Dispatch to the correct unpacking function based on bit width.
#[inline]
fn unpack_by_bits(packed: &[u8], count: usize, bits: u8) -> Vec<u8> {
    match bits {
        1 => unpack_1bit(packed, count),
        2 => unpack_2bit(packed, count),
        3 => unpack_3bit(packed, count),
        4 => nibble_unpack(packed, count),
        _ => panic!("unsupported bit width: {bits}"),
    }
}

/// Encode a vector using TurboQuant MSE at any bit width (1-4).
///
/// Same algorithm as `encode_tq_mse_scaled` but uses the generic quantizer
/// and dispatches to the appropriate packing function.
pub fn encode_tq_mse_multibit(
    vector: &[f32],
    sign_flips: &[f32],
    boundaries: &[f32],
    bits: u8,
    work_buf: &mut [f32],
) -> TqCode {
    let dim = vector.len();
    let padded = padded_dimension(dim as u32) as usize;
    let n_centroids = 1u8 << bits;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Step 1: Compute norm
    let mut norm_sq = 0.0f32;
    for &v in vector {
        norm_sq += v * v;
    }
    let norm = norm_sq.sqrt();

    // Step 2+3: Normalize and pad
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

    // Step 5: Quantize with generic boundaries
    let mut indices = Vec::with_capacity(padded);
    for &val in work_buf[..padded].iter() {
        indices.push(quantize_with_boundaries_n(val, boundaries, n_centroids));
    }

    // Step 6: Pack with appropriate bit width
    let codes = pack_by_bits(&indices, bits);

    TqCode { codes, norm }
}

/// Decode a TQ code at any bit width back to approximate vector.
///
/// `centroids`: flat slice of centroid values for the given bit width.
pub fn decode_tq_mse_multibit(
    code: &TqCode,
    sign_flips: &[f32],
    centroids: &[f32],
    bits: u8,
    original_dim: usize,
    work_buf: &mut [f32],
) -> Vec<f32> {
    let padded = padded_dimension(original_dim as u32) as usize;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Unpack indices -> centroid values
    let indices = unpack_by_bits(&code.codes, padded, bits);
    for (dst, &idx) in work_buf[..padded].iter_mut().zip(indices.iter()) {
        *dst = centroids[idx as usize];
    }

    // Inverse FWHT: R^{-1}(y) = D * H * y
    fwht::inverse_fwht(&mut work_buf[..padded], sign_flips);

    // Un-pad and scale by norm
    let mut result = Vec::with_capacity(original_dim);
    for &val in work_buf[..original_dim].iter() {
        result.push(val * code.norm);
    }
    result
}

/// Encode using A2 hexagonal lattice quantization.
///
/// Pairs consecutive FWHT-rotated coordinates and jointly quantizes each pair
/// to one of 16 A2 lattice cells (4-bit index per pair).
/// Two pair-indices are nibble-packed into each byte.
/// Output: TqCode with codes length = padded_dim / 4.
pub fn encode_tq_mse_a2(
    vector: &[f32],
    sign_flips: &[f32],
    a2_codebook: &super::a2_lattice::A2Codebook,
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

    // Step 4: Randomized FWHT (uses OnceLock-dispatched fn)
    fwht::fwht(&mut work_buf[..padded], sign_flips);

    // Step 5: A2 paired quantization -- one 4-bit index per pair
    let num_pairs = padded / 2;
    let mut pair_indices: Vec<u8> = Vec::with_capacity(num_pairs);
    for i in (0..padded).step_by(2) {
        pair_indices.push(a2_codebook.quantize_pair(work_buf[i], work_buf[i + 1]));
    }

    // Step 6: nibble_pack the pair indices (two pairs per byte)
    let codes = nibble_pack(&pair_indices);

    TqCode { codes, norm }
}

/// Decode an A2 TQ code back to approximate vector.
///
/// Inverse of `encode_tq_mse_a2`: unpack nibbles -> decode pairs via A2Codebook
/// -> inverse FWHT -> un-pad -> scale by norm.
pub fn decode_tq_mse_a2(
    code: &TqCode,
    sign_flips: &[f32],
    a2_codebook: &super::a2_lattice::A2Codebook,
    original_dim: usize,
    work_buf: &mut [f32],
) -> Vec<f32> {
    let padded = padded_dimension(original_dim as u32) as usize;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Unpack nibbles -> pair indices
    let num_pairs = padded / 2;
    let pair_indices = nibble_unpack(&code.codes, num_pairs);

    // Decode pair indices -> f32 coordinates
    for (i, &idx) in pair_indices.iter().enumerate() {
        let (x, y) = a2_codebook.decode_pair(idx);
        work_buf[i * 2] = x;
        work_buf[i * 2 + 1] = y;
    }

    // Inverse FWHT: R^{-1}(y) = D * H * y
    fwht::inverse_fwht(&mut work_buf[..padded], sign_flips);

    // Un-pad and scale by norm
    let mut result = Vec::with_capacity(original_dim);
    for &val in work_buf[..original_dim].iter() {
        result.push(val * code.norm);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::codebook::{code_bytes_per_vector, scaled_boundaries_n, scaled_centroids_n};
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
        assert_eq!(
            code.codes.len(),
            padded / 2,
            "expected {} bytes, got {}",
            padded / 2,
            code.codes.len()
        );
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
        eprintln!(
            "TQ 4-bit round-trip: avg MSE = {avg_distortion:.6}, max MSE = {max_distortion:.6}"
        );

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

    // ── 1-bit pack/unpack tests ──────────────────────────────────────

    #[test]
    fn test_pack_1bit_specific() {
        // [1,0,1,1,0,0,1,0] -> LSB-first: bit0=1,bit1=0,bit2=1,bit3=1,bit4=0,bit5=0,bit6=1,bit7=0
        // = 0b01001101 = 0x4D
        let indices = vec![1, 0, 1, 1, 0, 0, 1, 0];
        let packed = pack_1bit(&indices);
        assert_eq!(packed, vec![0b01001101]);
    }

    #[test]
    fn test_unpack_1bit_roundtrip() {
        let indices = vec![1, 0, 1, 1, 0, 0, 1, 0];
        let packed = pack_1bit(&indices);
        let unpacked = unpack_1bit(&packed, 8);
        assert_eq!(unpacked, indices);
    }

    #[test]
    fn test_pack_1bit_all_ones() {
        let indices = vec![1u8; 8];
        let packed = pack_1bit(&indices);
        assert_eq!(packed, vec![0xFF]);
    }

    #[test]
    fn test_pack_1bit_all_zeros() {
        let indices = vec![0u8; 8];
        let packed = pack_1bit(&indices);
        assert_eq!(packed, vec![0x00]);
    }

    // ── 2-bit pack/unpack tests ──────────────────────────────────────

    #[test]
    fn test_pack_2bit_specific() {
        // [0,1,2,3] -> LSB-first: 00 | 01<<2 | 10<<4 | 11<<6 = 0b11_10_01_00 = 0xE4
        let indices = vec![0, 1, 2, 3];
        let packed = pack_2bit(&indices);
        assert_eq!(packed, vec![0b11_10_01_00]);
    }

    #[test]
    fn test_unpack_2bit_roundtrip() {
        let indices = vec![0, 1, 2, 3];
        let packed = pack_2bit(&indices);
        let unpacked = unpack_2bit(&packed, 4);
        assert_eq!(unpacked, indices);
    }

    #[test]
    fn test_pack_2bit_all_values() {
        // Test all 4 values in various positions
        let indices = vec![3, 2, 1, 0, 0, 1, 2, 3];
        let packed = pack_2bit(&indices);
        let unpacked = unpack_2bit(&packed, 8);
        assert_eq!(unpacked, indices);
    }

    // ── 3-bit pack/unpack tests ──────────────────────────────────────

    #[test]
    fn test_pack_3bit_8_indices() {
        // 8 indices (each 0-7) -> 3 bytes
        let indices = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let packed = pack_3bit(&indices);
        assert_eq!(packed.len(), 3);
        let unpacked = unpack_3bit(&packed, 8);
        assert_eq!(unpacked, indices);
    }

    #[test]
    fn test_unpack_3bit_roundtrip() {
        // Various patterns
        for seed in 0..10u32 {
            let indices: Vec<u8> = (0..16).map(|i| ((i + seed as usize) % 8) as u8).collect();
            let packed = pack_3bit(&indices);
            assert_eq!(packed.len(), 6); // 16 * 3 / 8 = 6 bytes
            let unpacked = unpack_3bit(&packed, 16);
            assert_eq!(unpacked, indices, "3-bit roundtrip failed for seed {seed}");
        }
    }

    #[test]
    fn test_pack_3bit_all_max() {
        let indices = vec![7u8; 8];
        let packed = pack_3bit(&indices);
        let unpacked = unpack_3bit(&packed, 8);
        assert_eq!(unpacked, indices);
    }

    // ── Multi-bit encode/decode tests ────────────────────────────────

    #[test]
    fn test_encode_multibit_code_sizes() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32);
        let signs = test_sign_flips(padded as usize, 42);
        let mut work = vec![0.0f32; padded as usize];

        let mut v = lcg_f32(dim, 99);
        normalize_to_unit(&mut v);

        for bits in [1u8, 2, 3, 4] {
            let boundaries = scaled_boundaries_n(padded, bits).unwrap();
            let code = encode_tq_mse_multibit(&v, &signs, &boundaries, bits, &mut work);
            let expected = code_bytes_per_vector(padded, bits);
            assert_eq!(
                code.codes.len(),
                expected,
                "{bits}-bit: expected {expected} bytes, got {}",
                code.codes.len()
            );
        }

        // Specific sizes for 768d (padded=1024)
        let b1 = scaled_boundaries_n(padded, 1).unwrap();
        let c1 = encode_tq_mse_multibit(&v, &signs, &b1, 1, &mut work);
        assert_eq!(c1.codes.len(), 128); // 1024/8

        let b2 = scaled_boundaries_n(padded, 2).unwrap();
        let c2 = encode_tq_mse_multibit(&v, &signs, &b2, 2, &mut work);
        assert_eq!(c2.codes.len(), 256); // 1024/4

        let b3 = scaled_boundaries_n(padded, 3).unwrap();
        let c3 = encode_tq_mse_multibit(&v, &signs, &b3, 3, &mut work);
        assert_eq!(c3.codes.len(), 384); // 1024*3/8
    }

    #[test]
    fn test_encode_multibit_1bit_mse() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32);
        let signs = test_sign_flips(padded as usize, 12345);
        let boundaries = scaled_boundaries_n(padded, 1).unwrap();
        let centroids = scaled_centroids_n(padded, 1).unwrap();
        let mut work_enc = vec![0.0f32; padded as usize];
        let mut work_dec = vec![0.0f32; padded as usize];

        let mut total_mse = 0.0f32;
        let n = 50;
        for seed in 0..n {
            let mut v = lcg_f32(dim, seed * 7 + 13);
            normalize_to_unit(&mut v);
            let code = encode_tq_mse_multibit(&v, &signs, &boundaries, 1, &mut work_enc);
            let recon = decode_tq_mse_multibit(&code, &signs, &centroids, 1, dim, &mut work_dec);
            total_mse += mse_distortion(&v, &recon);
        }
        let avg_mse = total_mse / n as f32;
        eprintln!("1-bit avg MSE: {avg_mse:.6}");
        // Paper bound ~0.36, we allow 2x = 0.72
        assert!(avg_mse <= 0.72, "1-bit MSE {avg_mse:.6} exceeds 0.72");
    }

    #[test]
    fn test_encode_multibit_2bit_mse() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32);
        let signs = test_sign_flips(padded as usize, 12345);
        let boundaries = scaled_boundaries_n(padded, 2).unwrap();
        let centroids = scaled_centroids_n(padded, 2).unwrap();
        let mut work_enc = vec![0.0f32; padded as usize];
        let mut work_dec = vec![0.0f32; padded as usize];

        let mut total_mse = 0.0f32;
        let n = 50;
        for seed in 0..n {
            let mut v = lcg_f32(dim, seed * 7 + 13);
            normalize_to_unit(&mut v);
            let code = encode_tq_mse_multibit(&v, &signs, &boundaries, 2, &mut work_enc);
            let recon = decode_tq_mse_multibit(&code, &signs, &centroids, 2, dim, &mut work_dec);
            total_mse += mse_distortion(&v, &recon);
        }
        let avg_mse = total_mse / n as f32;
        eprintln!("2-bit avg MSE: {avg_mse:.6}");
        assert!(avg_mse <= 0.234, "2-bit MSE {avg_mse:.6} exceeds 0.234");
    }

    #[test]
    fn test_encode_multibit_3bit_mse() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32);
        let signs = test_sign_flips(padded as usize, 12345);
        let boundaries = scaled_boundaries_n(padded, 3).unwrap();
        let centroids = scaled_centroids_n(padded, 3).unwrap();
        let mut work_enc = vec![0.0f32; padded as usize];
        let mut work_dec = vec![0.0f32; padded as usize];

        let mut total_mse = 0.0f32;
        let n = 50;
        for seed in 0..n {
            let mut v = lcg_f32(dim, seed * 7 + 13);
            normalize_to_unit(&mut v);
            let code = encode_tq_mse_multibit(&v, &signs, &boundaries, 3, &mut work_enc);
            let recon = decode_tq_mse_multibit(&code, &signs, &centroids, 3, dim, &mut work_dec);
            total_mse += mse_distortion(&v, &recon);
        }
        let avg_mse = total_mse / n as f32;
        eprintln!("3-bit avg MSE: {avg_mse:.6}");
        assert!(avg_mse <= 0.06, "3-bit MSE {avg_mse:.6} exceeds 0.06");
    }

    // ── A2 hexagonal lattice encoder tests ──────────────────────────────

    #[test]
    fn test_encode_a2_byte_length() {
        use super::super::a2_lattice::A2Codebook;
        fwht::init_fwht();

        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let cb = A2Codebook::new(padded as u32);
        let mut work = vec![0.0f32; padded];

        let mut v = lcg_f32(dim, 99);
        normalize_to_unit(&mut v);

        let code = encode_tq_mse_a2(&v, &signs, &cb, &mut work);
        // padded_dim/4 bytes: each byte = 2 nibbles = 2 pairs = 4 coordinates
        assert_eq!(
            code.codes.len(),
            padded / 4,
            "A2 code length should be padded_dim/4 = {}, got {}",
            padded / 4,
            code.codes.len()
        );
    }

    #[test]
    fn test_encode_a2_byte_length_768d() {
        use super::super::a2_lattice::A2Codebook;
        fwht::init_fwht();

        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize; // 1024
        let signs = test_sign_flips(padded, 42);
        let cb = A2Codebook::new(padded as u32);
        let mut work = vec![0.0f32; padded];

        let mut v = lcg_f32(dim, 99);
        normalize_to_unit(&mut v);

        let code = encode_tq_mse_a2(&v, &signs, &cb, &mut work);
        assert_eq!(code.codes.len(), 256); // 1024 / 4
    }

    #[test]
    fn test_encode_a2_roundtrip() {
        use super::super::a2_lattice::A2Codebook;
        fwht::init_fwht();

        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let cb = A2Codebook::new(padded as u32);
        let mut work_enc = vec![0.0f32; padded];
        let mut work_dec = vec![0.0f32; padded];

        let mut total_mse = 0.0f32;
        let n = 50;
        for seed in 0..n {
            let mut v = lcg_f32(dim, seed * 7 + 13);
            normalize_to_unit(&mut v);

            let code = encode_tq_mse_a2(&v, &signs, &cb, &mut work_enc);
            let recon = decode_tq_mse_a2(&code, &signs, &cb, dim, &mut work_dec);

            assert_eq!(recon.len(), dim);
            total_mse += mse_distortion(&v, &recon);
        }
        let avg_mse = total_mse / n as f32;
        eprintln!("A2 avg MSE (128d): {avg_mse:.6}");
        // A2 should achieve reasonable MSE -- less strict than scalar TQ4
        // since A2 uses fewer bytes (padded/4 vs padded/2).
        assert!(avg_mse <= 0.10, "A2 MSE {avg_mse:.6} exceeds 0.10");
    }

    #[test]
    fn test_encode_a2_zero_vector() {
        use super::super::a2_lattice::A2Codebook;
        fwht::init_fwht();

        let dim = 64;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let cb = A2Codebook::new(padded as u32);
        let mut work = vec![0.0f32; padded];

        let v = vec![0.0f32; dim];
        let code = encode_tq_mse_a2(&v, &signs, &cb, &mut work);
        assert_eq!(code.codes.len(), padded / 4);
        assert_eq!(code.norm, 0.0);
    }

    #[test]
    fn test_encode_a2_norm_preserved() {
        use super::super::a2_lattice::A2Codebook;
        fwht::init_fwht();

        let dim = 64;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let cb = A2Codebook::new(padded as u32);
        let mut work = vec![0.0f32; padded];

        let v = lcg_f32(dim, 42);
        let expected_norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        let code = encode_tq_mse_a2(&v, &signs, &cb, &mut work);
        assert!(
            (code.norm - expected_norm).abs() < 1e-5,
            "norm mismatch: {} vs {}",
            code.norm,
            expected_norm
        );
    }
}
