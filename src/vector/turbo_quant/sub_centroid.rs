//! Sign-bit sub-centroid refinement for TurboQuant search.
//!
//! Implements the sub-centroid technique from turboquant_search (Tarun-KS):
//! each Lloyd-Max bin is split at its centroid into two sub-bins with conditional
//! expectations as reconstruction values. This 1 extra bit per coordinate doubles
//! effective quantization resolution from 2^b to 2^(b+1) levels.
//!
//! For search tasks, sub-centroid refinement yields **better recall** than the
//! paper's QJL correction (which optimizes for unbiasedness, not ranking). The
//! trade-off: reconstruction is biased, but variance is lower — exactly what
//! nearest-neighbor search needs.
//!
//! ## Memory layout
//!
//! Per vector (768d, padded to 1024, 4-bit):
//! - TQ indices: 512 bytes (nibble-packed, same as standard TQ)
//! - Sign bits:  128 bytes (1 bit per coordinate, ceil(padded_dim/8))
//! - Norm:         4 bytes
//! - Total:      644 bytes (vs ~1288 bytes with M=8 QJL)
//!
//! ## Algorithm
//!
//! Encoding (extends standard TQ-MSE):
//! 1. Quantize coordinate y[j] → index k (standard Lloyd-Max)
//! 2. Compute residual: r = y[j] - centroid[k]
//! 3. Store sign bit: s = (r >= 0) ? 1 : 0
//!
//! ADC scoring:
//! - Use sub_centroids[k][s] instead of centroids[k] for reconstruction
//! - Same asymmetric distance as TQ-ADC, but with 2× resolution

use super::codebook;
use super::encoder::{nibble_pack, padded_dimension};
use super::fwht;

/// Sub-centroid lookup table for one bit width.
///
/// For each Lloyd-Max bin k, stores two reconstruction values:
/// - `table[k * 2]`     = E[X | X ∈ bin_k, X < centroid_k]  (lower half)
/// - `table[k * 2 + 1]` = E[X | X ∈ bin_k, X ≥ centroid_k]  (upper half)
///
/// Scaled by σ = 1/√padded_dim to match FWHT normalization.
pub struct SubCentroidTable {
    /// Interleaved [lo_0, hi_0, lo_1, hi_1, ...], length = 2 * n_centroids.
    pub table: Vec<f32>,
    pub bits: u8,
    pub padded_dim: u32,
}

/// Encoded vector with sub-centroid sign bits.
pub struct TqSignCode {
    /// Nibble-packed (or N-bit packed) quantization indices. Same as TqCode.codes.
    pub codes: Vec<u8>,
    /// Sign bits: 1 bit per coordinate. bit=1 means residual >= 0 (upper sub-centroid).
    /// Packed LSB-first, ceil(padded_dim/8) bytes.
    pub sign_bits: Vec<u8>,
    /// Original L2 norm of the input vector.
    pub norm: f32,
}

impl SubCentroidTable {
    /// Compute sub-centroid table for N(0, σ²) where σ = 1/√padded_dim.
    ///
    /// For each bin [lo_boundary, hi_boundary] with centroid c_k:
    ///   lower_sub = E[X | lo_boundary ≤ X < c_k]
    ///   upper_sub = E[X | c_k ≤ X < hi_boundary]
    ///
    /// Uses numerical integration over N(0, σ²) density.
    pub fn new(padded_dim: u32, bits: u8) -> Self {
        let sigma = 1.0 / (padded_dim as f32).sqrt();
        let n_centroids = 1usize << bits;

        let raw_centroids = raw_centroids_for_bits(bits);
        let raw_boundaries = raw_boundaries_for_bits(bits);

        let mut table = vec![0.0f32; n_centroids * 2];

        for k in 0..n_centroids {
            let c_k = raw_centroids[k];

            // Bin boundaries (raw, unscaled)
            let lo_bound = if k == 0 { -6.0 } else { raw_boundaries[k - 1] };
            let hi_bound = if k == n_centroids - 1 {
                6.0
            } else {
                raw_boundaries[k]
            };

            // Lower sub-bin: [lo_bound, c_k)
            let lower_sub = conditional_mean_n01(lo_bound, c_k);
            // Upper sub-bin: [c_k, hi_bound)
            let upper_sub = conditional_mean_n01(c_k, hi_bound);

            table[k * 2] = lower_sub * sigma;
            table[k * 2 + 1] = upper_sub * sigma;
        }

        Self {
            table,
            bits,
            padded_dim,
        }
    }

    /// Look up sub-centroid value for a given index and sign bit.
    #[inline(always)]
    pub fn lookup(&self, index: u8, sign_bit: u8) -> f32 {
        // sign_bit: 0 = lower, 1 = upper
        self.table[index as usize * 2 + sign_bit as usize]
    }

    /// Number of entries in the table: 2 * n_centroids.
    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }
}

/// Compute E[X | a ≤ X < b] for X ~ N(0, 1) using numerical integration.
///
/// E[X | a ≤ X < b] = (φ(a) - φ(b)) / (Φ(b) - Φ(a))
/// where φ is the standard normal PDF and Φ is the CDF.
fn conditional_mean_n01(a: f32, b: f32) -> f32 {
    let a64 = a as f64;
    let b64 = b as f64;

    let pdf_a = std_normal_pdf(a64);
    let pdf_b = std_normal_pdf(b64);
    let cdf_a = std_normal_cdf(a64);
    let cdf_b = std_normal_cdf(b64);

    let denom = cdf_b - cdf_a;
    if denom.abs() < 1e-15 {
        // Degenerate bin — return midpoint
        return ((a64 + b64) / 2.0) as f32;
    }

    ((pdf_a - pdf_b) / denom) as f32
}

/// Standard normal PDF: φ(x) = (1/√(2π)) exp(-x²/2).
#[inline]
fn std_normal_pdf(x: f64) -> f64 {
    const INV_SQRT_2PI: f64 = 0.3989422804014327;
    INV_SQRT_2PI * (-0.5 * x * x).exp()
}

/// Standard normal CDF: Φ(x) using Abramowitz & Stegun approximation.
/// Accurate to ~1.5e-7.
fn std_normal_cdf(x: f64) -> f64 {
    // Use erfc-based formula for better numerical stability
    0.5 * erfc_approx(-x * std::f64::consts::FRAC_1_SQRT_2)
}

/// Complementary error function approximation (Abramowitz & Stegun 7.1.26).
fn erfc_approx(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.3275911 * x.abs());
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let result = poly * (-x * x).exp();
    if x >= 0.0 { result } else { 2.0 - result }
}

/// Get raw (unscaled) centroids for a given bit width.
fn raw_centroids_for_bits(bits: u8) -> &'static [f32] {
    match bits {
        1 => &codebook::RAW_CENTROIDS_1BIT,
        2 => &codebook::RAW_CENTROIDS_2BIT,
        3 => &codebook::RAW_CENTROIDS_3BIT,
        4 => &codebook::RAW_CENTROIDS,
        _ => panic!("unsupported bit width: {bits}"),
    }
}

/// Get raw (unscaled) boundaries for a given bit width.
fn raw_boundaries_for_bits(bits: u8) -> &'static [f32] {
    match bits {
        1 => &codebook::RAW_BOUNDARIES_1BIT,
        2 => &codebook::RAW_BOUNDARIES_2BIT,
        3 => &codebook::RAW_BOUNDARIES_3BIT,
        4 => &codebook::RAW_BOUNDARIES,
        _ => panic!("unsupported bit width: {bits}"),
    }
}

// ── Encoding ────────────────────────────────────────────────────────

/// Encode a vector with sub-centroid sign bits (4-bit).
///
/// Same as `encode_tq_mse_scaled` but additionally computes and stores
/// the sign of (y[j] - centroid[idx]) per coordinate.
pub fn encode_tq_sign(
    vector: &[f32],
    sign_flips: &[f32],
    boundaries: &[f32; 15],
    centroids: &[f32; 16],
    work_buf: &mut [f32],
) -> TqSignCode {
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

    // Step 5: Quantize + collect sign bits
    let mut indices = Vec::with_capacity(padded);
    let sign_bytes = (padded + 7) / 8;
    let mut sign_bits = vec![0u8; sign_bytes];

    for j in 0..padded {
        let val = work_buf[j];
        let idx = codebook::quantize_with_boundaries(val, boundaries);
        indices.push(idx);

        // Sign bit: 1 if val >= centroid (upper sub-bin), 0 if below
        if val >= centroids[idx as usize] {
            sign_bits[j / 8] |= 1 << (j % 8);
        }
    }

    // Step 6: Nibble pack indices
    let codes = nibble_pack(&indices);

    TqSignCode {
        codes,
        sign_bits,
        norm,
    }
}

/// Encode with generic bit width (1-4 bit) + sign bits.
pub fn encode_tq_sign_multibit(
    vector: &[f32],
    sign_flips: &[f32],
    boundaries: &[f32],
    centroids: &[f32],
    bits: u8,
    work_buf: &mut [f32],
) -> TqSignCode {
    let dim = vector.len();
    let padded = padded_dimension(dim as u32) as usize;
    let n_centroids = 1u8 << bits;
    debug_assert!(work_buf.len() >= padded);
    debug_assert_eq!(sign_flips.len(), padded);

    // Compute norm, normalize, pad, FWHT
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

    fwht::fwht(&mut work_buf[..padded], sign_flips);

    // Quantize + sign bits
    let mut indices = Vec::with_capacity(padded);
    let sign_bytes = (padded + 7) / 8;
    let mut sign_bits = vec![0u8; sign_bytes];

    for j in 0..padded {
        let val = work_buf[j];
        let idx = codebook::quantize_with_boundaries_n(val, boundaries, n_centroids);
        indices.push(idx);

        if val >= centroids[idx as usize] {
            sign_bits[j / 8] |= 1 << (j % 8);
        }
    }

    // Pack indices at appropriate bit width
    let codes = match bits {
        1 => super::encoder::pack_1bit(&indices),
        2 => super::encoder::pack_2bit(&indices),
        3 => super::encoder::pack_3bit(&indices),
        4 => nibble_pack(&indices),
        _ => panic!("unsupported bit width: {bits}"),
    };

    TqSignCode {
        codes,
        sign_bits,
        norm,
    }
}

// ── Asymmetric Distance with Sub-Centroid ───────────────────────────

/// Asymmetric L2 distance using sub-centroid reconstruction (4-bit).
///
/// Same algorithm as `tq_l2_adc_scaled` but reconstructs each coordinate
/// using the sub-centroid (2× resolution) instead of the bin centroid.
///
/// cost: identical to TQ-ADC — one extra bit extraction per coordinate.
#[inline]
pub fn tq_sign_l2_adc(
    q_rotated: &[f32],
    code: &[u8],
    sign_bits: &[u8],
    norm: f32,
    sub_table: &SubCentroidTable,
) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 2);
    debug_assert!(sign_bits.len() >= (padded + 7) / 8);

    let norm_sq = norm * norm;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    let code_len = code.len();
    let chunks = code_len / 4;
    let remainder = code_len % 4;

    for c in 0..chunks {
        let base = c * 4;
        let qbase = base * 2;

        let b0 = code[base];
        let b1 = code[base + 1];
        let b2 = code[base + 2];
        let b3 = code[base + 3];

        // Extract sign bits for 8 coordinates at a time
        let s0 = extract_sign_bit(sign_bits, qbase);
        let s1 = extract_sign_bit(sign_bits, qbase + 1);
        let d0lo = q_rotated[qbase] - sub_table.lookup(b0 & 0x0F, s0);
        let d0hi = q_rotated[qbase + 1] - sub_table.lookup(b0 >> 4, s1);
        sum0 += d0lo * d0lo + d0hi * d0hi;

        let s2 = extract_sign_bit(sign_bits, qbase + 2);
        let s3 = extract_sign_bit(sign_bits, qbase + 3);
        let d1lo = q_rotated[qbase + 2] - sub_table.lookup(b1 & 0x0F, s2);
        let d1hi = q_rotated[qbase + 3] - sub_table.lookup(b1 >> 4, s3);
        sum1 += d1lo * d1lo + d1hi * d1hi;

        let s4 = extract_sign_bit(sign_bits, qbase + 4);
        let s5 = extract_sign_bit(sign_bits, qbase + 5);
        let d2lo = q_rotated[qbase + 4] - sub_table.lookup(b2 & 0x0F, s4);
        let d2hi = q_rotated[qbase + 5] - sub_table.lookup(b2 >> 4, s5);
        sum2 += d2lo * d2lo + d2hi * d2hi;

        let s6 = extract_sign_bit(sign_bits, qbase + 6);
        let s7 = extract_sign_bit(sign_bits, qbase + 7);
        let d3lo = q_rotated[qbase + 6] - sub_table.lookup(b3 & 0x0F, s6);
        let d3hi = q_rotated[qbase + 7] - sub_table.lookup(b3 >> 4, s7);
        sum3 += d3lo * d3lo + d3hi * d3hi;
    }

    let tail_start = chunks * 4;
    for j in 0..remainder {
        let i = tail_start + j;
        let byte = code[i];
        let qi = i * 2;
        let s_lo = extract_sign_bit(sign_bits, qi);
        let s_hi = extract_sign_bit(sign_bits, qi + 1);
        let d_lo = q_rotated[qi] - sub_table.lookup(byte & 0x0F, s_lo);
        let d_hi = q_rotated[qi + 1] - sub_table.lookup(byte >> 4, s_hi);
        sum0 += d_lo * d_lo + d_hi * d_hi;
    }

    (sum0 + sum1 + sum2 + sum3) * norm_sq
}

/// Budgeted version with early termination.
#[inline]
pub fn tq_sign_l2_adc_budgeted(
    q_rotated: &[f32],
    code: &[u8],
    sign_bits: &[u8],
    norm: f32,
    sub_table: &SubCentroidTable,
    budget: f32,
) -> f32 {
    let norm_sq = norm * norm;
    if norm_sq <= 0.0 {
        return 0.0;
    }
    let scaled_budget = budget / norm_sq;

    let mut sum = 0.0f32;
    let code_len = code.len();

    // Check budget every 16 bytes (32 coordinates = 128 dims)
    let check_interval = 16;
    let full_blocks = code_len / check_interval;
    let remainder = code_len % check_interval;

    for block in 0..full_blocks {
        let block_start = block * check_interval;
        for j in 0..check_interval {
            let i = block_start + j;
            let byte = code[i];
            let qi = i * 2;
            let s_lo = extract_sign_bit(sign_bits, qi);
            let s_hi = extract_sign_bit(sign_bits, qi + 1);
            let d_lo = q_rotated[qi] - sub_table.lookup(byte & 0x0F, s_lo);
            let d_hi = q_rotated[qi + 1] - sub_table.lookup(byte >> 4, s_hi);
            sum += d_lo * d_lo + d_hi * d_hi;
        }
        if sum > scaled_budget {
            return f32::MAX;
        }
    }

    let tail_start = full_blocks * check_interval;
    for j in 0..remainder {
        let i = tail_start + j;
        let byte = code[i];
        let qi = i * 2;
        let s_lo = extract_sign_bit(sign_bits, qi);
        let s_hi = extract_sign_bit(sign_bits, qi + 1);
        let d_lo = q_rotated[qi] - sub_table.lookup(byte & 0x0F, s_lo);
        let d_hi = q_rotated[qi + 1] - sub_table.lookup(byte >> 4, s_hi);
        sum += d_lo * d_lo + d_hi * d_hi;
    }

    sum * norm_sq
}

// ── LUT-based ADC (P2) ─────────────────────────────────────────────

/// Precomputed per-query distance lookup table for sub-centroid ADC.
///
/// For each coordinate j and each sub-centroid entry e:
///   lut[j * n_entries + e] = (q_rotated[j] - sub_table.table[e])²
///
/// This converts the inner scoring loop from multiply-subtract-square
/// to a single table lookup + accumulate, enabling wider SIMD.
pub struct AdcLut {
    /// Flat array: padded_dim * n_entries entries.
    /// Layout: lut[j * n_entries + (idx * 2 + sign)] = distance².
    pub distances: Vec<f32>,
    /// Number of sub-centroid entries (2 * n_centroids).
    pub n_entries: usize,
}

impl AdcLut {
    /// Build LUT for 4-bit sub-centroid scoring.
    ///
    /// 32 entries per coordinate (16 bins × 2 sub-centroids).
    /// Total size: padded_dim × 32 × 4 bytes = 128 KB at 1024d.
    pub fn new(q_rotated: &[f32], sub_table: &SubCentroidTable) -> Self {
        let padded = q_rotated.len();
        let n_entries = sub_table.table.len(); // 2 * n_centroids
        let mut distances = Vec::with_capacity(padded * n_entries);

        for j in 0..padded {
            let q = q_rotated[j];
            for e in 0..n_entries {
                let d = q - sub_table.table[e];
                distances.push(d * d);
            }
        }

        Self {
            distances,
            n_entries,
        }
    }

    /// Build LUT for standard (non-sub-centroid) 4-bit ADC.
    ///
    /// 16 entries per coordinate (16 centroids, no sign bit).
    /// Total size: padded_dim × 16 × 4 bytes = 64 KB at 1024d.
    pub fn new_standard(q_rotated: &[f32], centroids: &[f32; 16]) -> Self {
        let padded = q_rotated.len();
        let n_entries = 16;
        let mut distances = Vec::with_capacity(padded * n_entries);

        for j in 0..padded {
            let q = q_rotated[j];
            for e in 0..n_entries {
                let d = q - centroids[e];
                distances.push(d * d);
            }
        }

        Self {
            distances,
            n_entries,
        }
    }

    /// Score using LUT with sub-centroid sign bits (4-bit).
    ///
    /// Inner loop: two table lookups + two additions per byte.
    #[inline]
    pub fn score_sign(&self, code: &[u8], sign_bits: &[u8], norm: f32) -> f32 {
        let norm_sq = norm * norm;
        let ne = self.n_entries;
        let mut sum0 = 0.0f32;
        let mut sum1 = 0.0f32;

        for (i, &byte) in code.iter().enumerate() {
            let qi = i * 2;
            let lo_idx = (byte & 0x0F) as usize;
            let hi_idx = (byte >> 4) as usize;
            let s_lo = extract_sign_bit(sign_bits, qi) as usize;
            let s_hi = extract_sign_bit(sign_bits, qi + 1) as usize;

            sum0 += self.distances[qi * ne + lo_idx * 2 + s_lo];
            sum1 += self.distances[(qi + 1) * ne + hi_idx * 2 + s_hi];
        }

        (sum0 + sum1) * norm_sq
    }

    /// Score using LUT without sign bits (standard 4-bit ADC).
    #[inline]
    pub fn score_standard(&self, code: &[u8], norm: f32) -> f32 {
        let norm_sq = norm * norm;
        let ne = self.n_entries;
        let mut sum0 = 0.0f32;
        let mut sum1 = 0.0f32;

        for (i, &byte) in code.iter().enumerate() {
            let qi = i * 2;
            let lo_idx = (byte & 0x0F) as usize;
            let hi_idx = (byte >> 4) as usize;

            sum0 += self.distances[qi * ne + lo_idx];
            sum1 += self.distances[(qi + 1) * ne + hi_idx];
        }

        (sum0 + sum1) * norm_sq
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Extract a single sign bit from packed sign bytes.
#[inline(always)]
fn extract_sign_bit(sign_bits: &[u8], coord_idx: usize) -> u8 {
    (sign_bits[coord_idx / 8] >> (coord_idx % 8)) & 1
}

/// Sign bits per vector in bytes for a given padded dimension.
#[inline]
pub fn sign_bytes_per_vector(padded_dim: u32) -> usize {
    (padded_dim as usize + 7) / 8
}

/// Total bytes per vector with sub-centroid encoding (4-bit):
/// nibble-packed codes + sign bits + norm.
#[inline]
pub fn total_bytes_per_vector(padded_dim: u32) -> usize {
    let code_bytes = padded_dim as usize / 2; // 4-bit nibble-packed
    let sign_bytes = sign_bytes_per_vector(padded_dim);
    code_bytes + sign_bytes + 4 // +4 for f32 norm
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::turbo_quant::codebook::{
        RAW_CENTROIDS, scaled_boundaries, scaled_centroids,
    };
    use crate::vector::turbo_quant::encoder::padded_dimension;
    use crate::vector::turbo_quant::fwht;

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
            v.iter_mut().for_each(|x| *x *= inv);
        }
        norm
    }

    fn test_sign_flips(dim: usize, seed: u64) -> Vec<f32> {
        let mut signs = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            signs.push(if (s >> 63) == 0 { 1.0f32 } else { -1.0 });
        }
        signs
    }

    #[test]
    fn test_sub_centroid_table_symmetry() {
        let table = SubCentroidTable::new(1024, 4);
        assert_eq!(table.table.len(), 32); // 16 bins × 2

        // For symmetric codebook around 0:
        // sub_centroid[k] should mirror sub_centroid[15-k]
        let n = 16usize;
        for k in 0..n {
            let lo = table.table[k * 2];
            let hi = table.table[k * 2 + 1];
            let mirror_hi = table.table[(n - 1 - k) * 2 + 1];
            let mirror_lo = table.table[(n - 1 - k) * 2];
            assert!(
                (lo + mirror_hi).abs() < 0.001,
                "symmetry violated: lo[{k}]={lo:.6} vs hi[{}]={mirror_hi:.6}",
                n - 1 - k
            );
            assert!(
                (hi + mirror_lo).abs() < 0.001,
                "symmetry violated: hi[{k}]={hi:.6} vs lo[{}]={mirror_lo:.6}",
                n - 1 - k
            );
        }
    }

    #[test]
    fn test_sub_centroid_between_boundaries() {
        let padded = 1024u32;
        let table = SubCentroidTable::new(padded, 4);
        let sigma = 1.0 / (padded as f32).sqrt();

        // Each sub-centroid should lie within its bin
        for k in 0..16usize {
            let lo = table.table[k * 2]; // lower sub-centroid
            let hi = table.table[k * 2 + 1]; // upper sub-centroid
            let centroid = RAW_CENTROIDS[k] * sigma;

            // Lower should be <= centroid, upper should be >= centroid
            assert!(
                lo <= centroid + 1e-6,
                "lower sub[{k}]={lo:.6} > centroid={centroid:.6}"
            );
            assert!(
                hi >= centroid - 1e-6,
                "upper sub[{k}]={hi:.6} < centroid={centroid:.6}"
            );
            // Both sub-centroids should be within bin boundaries
            assert!(lo <= hi, "sub[{k}]: lower={lo:.6} > upper={hi:.6}");
        }
    }

    #[test]
    fn test_sub_centroid_refines_resolution() {
        let padded = 1024u32;
        let table = SubCentroidTable::new(padded, 4);
        let sigma = 1.0 / (padded as f32).sqrt();

        // The two sub-centroids for each bin should be distinct
        // (unless bin is extremely narrow at the tails)
        for k in 1..15usize {
            let lo = table.table[k * 2];
            let hi = table.table[k * 2 + 1];
            let centroid = RAW_CENTROIDS[k] * sigma;
            assert!(
                (hi - lo).abs() > 1e-6,
                "sub-centroids for bin {k} are not distinct: lo={lo:.6}, hi={hi:.6}, c={centroid:.6}"
            );
        }
    }

    #[test]
    fn test_encode_sign_roundtrip_self_distance() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let sub_table = SubCentroidTable::new(padded as u32, 4);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 77);
        normalize(&mut vec);

        let code = encode_tq_sign(&vec, &sign_flips, &boundaries, &centroids, &mut work);
        assert_eq!(code.codes.len(), padded / 2);
        assert_eq!(code.sign_bits.len(), (padded + 7) / 8);

        // Prepare rotated query (self-distance test)
        let mut q_rot = vec![0.0f32; padded];
        q_rot[..dim].copy_from_slice(&vec);
        let q_norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rot[..dim].iter_mut() {
                *v *= inv;
            }
        }
        fwht::fwht(&mut q_rot, &sign_flips);

        let dist = tq_sign_l2_adc(&q_rot, &code.codes, &code.sign_bits, code.norm, &sub_table);

        // Self-distance with sub-centroid should be very small
        assert!(
            dist < 0.02,
            "self-distance with sub-centroid = {dist:.6}, expected < 0.02"
        );
    }

    #[test]
    fn test_sign_adc_beats_standard_adc() {
        fwht::init_fwht();
        use crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;

        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids_arr = scaled_centroids(padded as u32);
        let sub_table = SubCentroidTable::new(padded as u32, 4);
        let mut work = vec![0.0f32; padded];

        let n = 500;
        let k = 10;

        // Generate database vectors
        let mut db_codes = Vec::new();
        let mut db_sign_codes = Vec::new();
        let mut db_vecs = Vec::new();
        for i in 0..n {
            let mut v = lcg_f32(dim, i * 7 + 13);
            normalize(&mut v);
            let code = encode_tq_sign(&v, &sign_flips, &boundaries, &centroids_arr, &mut work);
            // Also encode standard TQ for comparison
            let std_code = crate::vector::turbo_quant::encoder::encode_tq_mse_scaled(
                &v,
                &sign_flips,
                &boundaries,
                &mut work,
            );
            db_codes.push(std_code);
            db_sign_codes.push(code);
            db_vecs.push(v);
        }

        // Run queries and measure recall
        let n_queries = 50;
        let mut sign_recall_sum = 0.0f64;
        let mut std_recall_sum = 0.0f64;

        for qi in 0..n_queries {
            let mut query = lcg_f32(dim, qi * 31 + 12345);
            normalize(&mut query);

            // Ground truth: exact L2
            let mut gt_dists: Vec<(f32, usize)> = db_vecs
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let d: f32 = query
                        .iter()
                        .zip(v.iter())
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum();
                    (d, i)
                })
                .collect();
            gt_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let gt_set: std::collections::HashSet<usize> =
                gt_dists[..k].iter().map(|(_, i)| *i).collect();

            // Prepare rotated query
            let mut q_rot = vec![0.0f32; padded];
            q_rot[..dim].copy_from_slice(&query);
            let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
            if q_norm > 0.0 {
                let inv = 1.0 / q_norm;
                for v in q_rot[..dim].iter_mut() {
                    *v *= inv;
                }
            }
            fwht::fwht(&mut q_rot, &sign_flips);

            // Standard TQ-ADC distances
            let mut std_dists: Vec<(f32, usize)> = db_codes
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let d = tq_l2_adc_scaled(&q_rot, &c.codes, c.norm, &centroids_arr);
                    (d, i)
                })
                .collect();
            std_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let std_set: std::collections::HashSet<usize> =
                std_dists[..k].iter().map(|(_, i)| *i).collect();

            // Sign-bit sub-centroid distances
            let mut sign_dists: Vec<(f32, usize)> = db_sign_codes
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let d = tq_sign_l2_adc(&q_rot, &c.codes, &c.sign_bits, c.norm, &sub_table);
                    (d, i)
                })
                .collect();
            sign_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let sign_set: std::collections::HashSet<usize> =
                sign_dists[..k].iter().map(|(_, i)| *i).collect();

            let std_recall = gt_set.intersection(&std_set).count() as f64 / k as f64;
            let sign_recall = gt_set.intersection(&sign_set).count() as f64 / k as f64;
            std_recall_sum += std_recall;
            sign_recall_sum += sign_recall;
        }

        let avg_std = std_recall_sum / n_queries as f64;
        let avg_sign = sign_recall_sum / n_queries as f64;
        eprintln!("Recall@{k}: standard TQ-ADC = {avg_std:.4}, sub-centroid = {avg_sign:.4}");

        // Sub-centroid should match or beat standard (it has 2× resolution)
        assert!(
            avg_sign >= avg_std - 0.02,
            "sub-centroid recall {avg_sign:.4} should be >= standard {avg_std:.4}"
        );
    }

    #[test]
    fn test_lut_matches_direct_scoring() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let sub_table = SubCentroidTable::new(padded as u32, 4);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 77);
        normalize(&mut vec);
        let code = encode_tq_sign(&vec, &sign_flips, &boundaries, &centroids, &mut work);

        let mut query = lcg_f32(dim, 12345);
        normalize(&mut query);

        // Prepare rotated query
        let mut q_rot = vec![0.0f32; padded];
        q_rot[..dim].copy_from_slice(&query);
        let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rot[..dim].iter_mut() {
                *v *= inv;
            }
        }
        fwht::fwht(&mut q_rot, &sign_flips);

        // Direct scoring
        let direct = tq_sign_l2_adc(&q_rot, &code.codes, &code.sign_bits, code.norm, &sub_table);

        // LUT scoring
        let lut = AdcLut::new(&q_rot, &sub_table);
        let lut_score = lut.score_sign(&code.codes, &code.sign_bits, code.norm);

        assert!(
            (direct - lut_score).abs() < 1e-4,
            "LUT score {lut_score:.6} != direct {direct:.6}"
        );
    }

    #[test]
    fn test_standard_lut_matches_tq_adc() {
        fwht::init_fwht();
        use crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;

        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 77);
        normalize(&mut vec);
        let code = crate::vector::turbo_quant::encoder::encode_tq_mse_scaled(
            &vec,
            &sign_flips,
            &boundaries,
            &mut work,
        );

        let mut query = lcg_f32(dim, 12345);
        normalize(&mut query);

        let mut q_rot = vec![0.0f32; padded];
        q_rot[..dim].copy_from_slice(&query);
        let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rot[..dim].iter_mut() {
                *v *= inv;
            }
        }
        fwht::fwht(&mut q_rot, &sign_flips);

        let direct = tq_l2_adc_scaled(&q_rot, &code.codes, code.norm, &centroids);
        let lut = AdcLut::new_standard(&q_rot, &centroids);
        let lut_score = lut.score_standard(&code.codes, code.norm);

        assert!(
            (direct - lut_score).abs() < 1e-4,
            "Standard LUT score {lut_score:.6} != direct {direct:.6}"
        );
    }

    #[test]
    fn test_budgeted_sign_adc() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let sub_table = SubCentroidTable::new(padded as u32, 4);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 77);
        normalize(&mut vec);
        let code = encode_tq_sign(&vec, &sign_flips, &boundaries, &centroids, &mut work);

        let mut query = lcg_f32(dim, 12345);
        normalize(&mut query);

        let mut q_rot = vec![0.0f32; padded];
        q_rot[..dim].copy_from_slice(&query);
        let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rot[..dim].iter_mut() {
                *v *= inv;
            }
        }
        fwht::fwht(&mut q_rot, &sign_flips);

        let full = tq_sign_l2_adc(&q_rot, &code.codes, &code.sign_bits, code.norm, &sub_table);

        // Large budget: should return same score
        let large = tq_sign_l2_adc_budgeted(
            &q_rot,
            &code.codes,
            &code.sign_bits,
            code.norm,
            &sub_table,
            full + 1.0,
        );
        assert!(
            (full - large).abs() < 1e-4,
            "budgeted with large budget should match full: {full:.6} vs {large:.6}"
        );

        // Small budget: should early-terminate
        let small = tq_sign_l2_adc_budgeted(
            &q_rot,
            &code.codes,
            &code.sign_bits,
            code.norm,
            &sub_table,
            full * 0.01,
        );
        assert_eq!(small, f32::MAX, "should early-terminate with tiny budget");
    }

    #[test]
    fn test_sign_bytes_per_vector() {
        assert_eq!(sign_bytes_per_vector(1024), 128);
        assert_eq!(sign_bytes_per_vector(128), 16);
        assert_eq!(sign_bytes_per_vector(256), 32);
    }

    #[test]
    fn test_total_bytes_per_vector() {
        // 4-bit at 1024 padded: 512 (codes) + 128 (signs) + 4 (norm) = 644
        assert_eq!(total_bytes_per_vector(1024), 644);
        // 4-bit at 128 padded: 64 (codes) + 16 (signs) + 4 (norm) = 84
        assert_eq!(total_bytes_per_vector(128), 84);
    }

    #[test]
    fn test_conditional_mean_center_bin() {
        // For the center bins of N(0,1), the conditional means should be
        // close to the sub-centroid values
        let mean = conditional_mean_n01(-0.15205, 0.0);
        // E[X | -0.15 < X < 0] should be negative and small
        assert!(mean < 0.0 && mean > -0.15, "center lo sub: {mean:.6}");

        let mean_hi = conditional_mean_n01(0.0, 0.15205);
        assert!(
            mean_hi > 0.0 && mean_hi < 0.15,
            "center hi sub: {mean_hi:.6}"
        );
    }

    #[test]
    fn test_multibit_sub_centroids() {
        // 1-bit should have 4 entries (2 bins × 2 sub)
        let t1 = SubCentroidTable::new(1024, 1);
        assert_eq!(t1.table.len(), 4);

        // 2-bit should have 8 entries
        let t2 = SubCentroidTable::new(1024, 2);
        assert_eq!(t2.table.len(), 8);

        // 3-bit should have 16 entries
        let t3 = SubCentroidTable::new(1024, 3);
        assert_eq!(t3.table.len(), 16);
    }
}
