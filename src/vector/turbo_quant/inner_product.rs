//! TurboQuant inner-product mode (TurboQuant_prod).
//!
//! Implements Algorithm 2 from arXiv 2504.19874:
//! 1. MSE encode at (b-1) bits (use 4-bit = standard TQ MSE)
//! 2. Compute residual r = x - DeQuant_mse(idx)
//! 3. QJL encode: sign(S * r), store ||r||
//! 4. Score: <y, x_hat> = <y, x_mse> + sqrt(pi/2)/d * ||r|| * <S*y, sign(S*r)>

use super::encoder::{TqCode, decode_tq_mse_scaled, encode_tq_mse_scaled, padded_dimension};
use super::qjl;

/// Encoded TurboQuant inner-product representation.
pub struct TqProdCode {
    /// MSE-quantized codes (nibble-packed, same as TqCode.codes).
    pub mse_codes: Vec<u8>,
    /// Original vector L2 norm.
    pub original_norm: f32,
    /// QJL sign bits: sign(S * residual). Length = ceil(dim/8) bytes.
    pub qjl_signs: Vec<u8>,
    /// L2 norm of the residual: ||x - DeQuant_mse(mse_codes)||.
    pub residual_norm: f32,
}

/// Encode a vector using TurboQuant_prod (inner-product mode).
///
/// Algorithm 2 from arXiv 2504.19874:
/// 1. idx = Quant_mse(x)
/// 2. r = x - DeQuant_mse(idx)
/// 3. qjl_signs = sign(S * r)
/// 4. Store: (idx, qjl_signs, ||r||, ||x||)
///
/// `vector`: original f32 vector (dim dimensions).
/// `sign_flips`: FWHT sign flips (padded_dim elements).
/// `boundaries`: scaled quantization boundaries.
/// `centroids`: dimension-scaled centroids (must match boundaries).
/// `qjl_matrix`: d x d Gaussian matrix (dim * dim elements, row-major).
/// `work_buf`: scratch buffer (>= padded_dim elements).
pub fn encode_tq_prod(
    vector: &[f32],
    sign_flips: &[f32],
    boundaries: &[f32; 15],
    centroids: &[f32; 16],
    qjl_matrix: &[f32],
    work_buf: &mut [f32],
) -> TqProdCode {
    let dim = vector.len();

    // Step 1: MSE encode
    let mse_code = encode_tq_mse_scaled(vector, sign_flips, boundaries, work_buf);

    // Step 2: Decode with MATCHING scaled centroids and compute residual
    let reconstructed = decode_tq_mse_scaled(&mse_code, sign_flips, centroids, dim, work_buf);
    let mut residual = Vec::with_capacity(dim);
    let mut r_norm_sq = 0.0f32;
    for i in 0..dim {
        let r = vector[i] - reconstructed[i];
        residual.push(r);
        r_norm_sq += r * r;
    }
    let residual_norm = r_norm_sq.sqrt();

    // Step 3: QJL encode the residual
    let qjl_signs = qjl::qjl_encode(qjl_matrix, &residual, dim);

    TqProdCode {
        mse_codes: mse_code.codes,
        original_norm: mse_code.norm,
        qjl_signs,
        residual_norm,
    }
}

/// Encode using paper-correct bit budget: (b-1)-bit MSE + 1-bit QJL.
///
/// Paper Algorithm 2: "Instantiate TurboQuant_mse with bit-width b-1"
/// For 4-bit total: 3-bit MSE (8 centroids) + 1-bit QJL sign per coordinate.
/// Total storage: (b-1)*d + d + 32 = b*d + 32 bits (same budget as TQ_mse at b bits).
pub fn encode_tq_prod_v2(
    vector: &[f32],
    sign_flips: &[f32],
    boundaries_bm1: &[f32],
    centroids_bm1: &[f32],
    bits_mse: u8,
    qjl_matrix: &[f32],
    work_buf: &mut [f32],
) -> TqProdCode {
    use super::encoder::encode_tq_mse_multibit;
    let dim = vector.len();
    let padded = padded_dimension(dim as u32) as usize;

    // Step 1: MSE encode at (b-1) bits
    let mse_code = encode_tq_mse_multibit(vector, sign_flips, boundaries_bm1, bits_mse, work_buf);
    let norm = mse_code.norm;

    // Step 2: Decode MSE to compute residual
    let code_bytes = &mse_code.codes;

    match bits_mse {
        3 => {
            let indices = super::encoder::unpack_3bit(code_bytes, padded);
            for j in 0..padded {
                work_buf[j] = centroids_bm1[indices[j] as usize];
            }
        }
        2 => {
            let indices = super::encoder::unpack_2bit(code_bytes, padded);
            for j in 0..padded {
                work_buf[j] = centroids_bm1[indices[j] as usize];
            }
        }
        1 => {
            let indices = super::encoder::unpack_1bit(code_bytes, padded);
            for j in 0..padded {
                work_buf[j] = centroids_bm1[indices[j] as usize];
            }
        }
        4 => {
            for j in 0..code_bytes.len() {
                let byte = code_bytes[j];
                work_buf[j * 2] = centroids_bm1[(byte & 0x0F) as usize];
                work_buf[j * 2 + 1] = centroids_bm1[(byte >> 4) as usize];
            }
        }
        _ => {
            let indices = super::encoder::nibble_unpack(code_bytes, padded);
            for j in 0..padded {
                work_buf[j] = centroids_bm1
                    .get(indices[j] as usize)
                    .copied()
                    .unwrap_or(0.0);
            }
        }
    }
    super::fwht::inverse_fwht(&mut work_buf[..padded], sign_flips);

    let mut r_norm_sq = 0.0f32;
    for i in 0..dim {
        let r = vector[i] - norm * work_buf[i];
        work_buf[i] = r;
        r_norm_sq += r * r;
    }
    let residual_norm = r_norm_sq.sqrt();

    let qjl_signs = qjl::qjl_encode(qjl_matrix, &work_buf[..dim], dim);

    TqProdCode {
        mse_codes: mse_code.codes,
        original_norm: norm,
        qjl_signs,
        residual_norm,
    }
}

/// Score inner product <query, x_hat> using TurboQuant_prod.
///
/// <y, x_hat> = <y, x_mse> + sqrt(pi/2)/d * ||r|| * <S*y, sign(S*r)>
///
/// `query`: raw f32 query vector (dim dimensions).
/// `code`: TqProdCode from encode_tq_prod.
/// `sign_flips`: FWHT sign flips (padded_dim elements).
/// `centroids`: dimension-scaled centroids (must match those used at encode time).
/// `qjl_matrix`: d x d Gaussian matrix (same one used for encoding).
///
/// Returns estimated inner product (higher = more similar for IP metric).
pub fn score_inner_product(
    query: &[f32],
    code: &TqProdCode,
    sign_flips: &[f32],
    centroids: &[f32; 16],
    qjl_matrix: &[f32],
    work_buf: &mut [f32],
) -> f32 {
    let dim = query.len();

    // Term 1: <y, x_mse> via decode — borrow codes directly, no clone
    let mse_code = TqCode {
        codes: code.mse_codes.clone(),
        norm: code.original_norm,
    };
    let x_mse = decode_tq_mse_scaled(&mse_code, sign_flips, centroids, dim, work_buf);
    let mut dot_mse = 0.0f32;
    for i in 0..dim {
        dot_mse += query[i] * x_mse[i];
    }

    // Term 2: sqrt(pi/2)/d * ||r|| * <S*y, sign(S*r)>
    // Reuse work_buf for S*y (padded_dim >= dim, only need dim elements)
    for row in 0..dim {
        let row_start = row * dim;
        let mut dot = 0.0f32;
        for col in 0..dim {
            dot += qjl_matrix[row_start + col] * query[col];
        }
        work_buf[row] = dot;
    }

    // Compute <S*y, sign(S*r)> where sign values are +1/-1
    let mut dot_qjl = 0.0f32;
    for row in 0..dim {
        let sign_val = if code.qjl_signs[row / 8] & (1 << (row % 8)) != 0 {
            1.0f32
        } else {
            -1.0f32
        };
        dot_qjl += work_buf[row] * sign_val;
    }

    let scale = (std::f32::consts::PI / 2.0).sqrt() / dim as f32;
    dot_mse + scale * code.residual_norm * dot_qjl
}

// ── Optimized scoring for HNSW search ────────────────────────────────

/// Precomputed query projection for TurboQuant_prod scoring.
///
/// Computed once per query, reused across all candidates. Avoids O(M*d²)
/// matrix-vector multiply per candidate.
pub struct TqProdQueryState {
    /// S_m * y for each of M projection matrices (M × d elements).
    pub s_y_list: Vec<Vec<f32>>,
    /// Number of projections M.
    pub num_projections: usize,
    /// q_rotated values (padded_dim elements) for Term 1 in rotated space.
    pub q_rotated: Vec<f32>,
    /// ||query||² — constant term for L2 conversion.
    pub q_norm_sq: f32,
}

/// Precompute query state for M-projection TurboQuant_prod scoring.
///
/// Uses dense Gaussian S_m · y (required for QJL unbiasedness proof).
/// Cost: O(M × d²) per query. At M=4, d=768: ~2.4M ops, ~0.4ms on M4.
/// Done once per query, amortized across all candidates.
pub fn prepare_query_prod(
    query: &[f32],
    qjl_matrices: &[Vec<f32>],
    sign_flips: &[f32],
    padded_dim: usize,
) -> TqProdQueryState {
    let dim = query.len();

    // 1. Compute S_m * y for each projection — O(M × d²) total
    let s_y_list: Vec<Vec<f32>> = qjl_matrices
        .iter()
        .map(|matrix| {
            let mut s_y = vec![0.0f32; dim];
            for row in 0..dim {
                let row_start = row * dim;
                let mut dot = 0.0f32;
                for col in 0..dim {
                    dot += matrix[row_start + col] * query[col];
                }
                s_y[row] = dot;
            }
            s_y
        })
        .collect();

    // 2. Compute FWHT-rotated query
    let mut q_rotated = vec![0.0f32; padded_dim];
    q_rotated[..dim].copy_from_slice(query);
    let q_norm_sq: f32 = query.iter().map(|x| x * x).sum();
    let q_norm = q_norm_sq.sqrt();
    if q_norm > 0.0 {
        let inv = 1.0 / q_norm;
        for v in q_rotated[..dim].iter_mut() {
            *v *= inv;
        }
    }
    super::fwht::fwht(&mut q_rotated[..padded_dim], sign_flips);

    let num_projections = s_y_list.len();
    TqProdQueryState {
        s_y_list,
        num_projections,
        q_rotated,
        q_norm_sq,
    }
}

/// Score L2 distance using TurboQuant_prod estimator (unbiased).
///
/// `||q - x||² ≈ ||q||² + ||x||² - 2 * <q, x>_prod`
///
/// where `<q, x>_prod = <q, x_mse> + sqrt(pi/2)/d * ||r|| * <S*y, sign(S*r)>`
///
/// Term 1 (`<q, x_mse>`): computed in rotated space as
///   `norm * Σ q_rot[i] * centroids[code[i]]` — O(padded_dim), no inverse FWHT.
///
/// Term 2 (QJL correction): `<S*y, sign(S*r)>` — O(dim) dot with sign bits.
///   S*y is precomputed in TqProdQueryState.
///
/// Total per-candidate cost: O(padded_dim) — same as TQ-ADC.
/// Score L2 distance using M-projection TurboQuant_prod estimator.
///
/// Averages M independent QJL corrections to reduce variance by sqrt(M).
/// Variance: π/(2dM) · ||r||² · ||y||² (Theorem 2 extended).
///
/// `qjl_signs`: M * qjl_bytes_per_vec contiguous sign bits.
/// `qjl_bytes_per_vec`: ceil(dim/8) bytes per single projection.
#[inline]
pub fn score_l2_prod(
    state: &TqProdQueryState,
    tq_code: &[u8],     // nibble-packed TQ codes (padded_dim/2 bytes)
    norm: f32,          // ||x|| stored with code
    qjl_signs: &[u8],   // M * ceil(dim/8) sign bits, contiguous
    residual_norm: f32, // ||r|| stored with code
    centroids: &[f32; 16],
    dim: usize,
    qjl_bytes_per_vec: usize, // ceil(dim/8)
) -> f32 {
    // Term 1: <q, x_mse> in rotated space — exact, no noise
    let mut dot_mse = 0.0f32;
    for (j, &byte) in tq_code.iter().enumerate() {
        let lo_idx = (byte & 0x0F) as usize;
        let hi_idx = (byte >> 4) as usize;
        dot_mse += state.q_rotated[j * 2] * centroids[lo_idx];
        dot_mse += state.q_rotated[j * 2 + 1] * centroids[hi_idx];
    }
    dot_mse *= norm;

    // Term 2: Average M QJL corrections for variance reduction
    let m = state.num_projections;
    let mut avg_dot_qjl = 0.0f32;
    for proj in 0..m {
        let signs_offset = proj * qjl_bytes_per_vec;
        let proj_signs = &qjl_signs[signs_offset..signs_offset + qjl_bytes_per_vec];
        let s_y = &state.s_y_list[proj];

        let mut dot_qjl = 0.0f32;
        for row in 0..dim {
            let sign_val = if proj_signs[row / 8] & (1 << (row % 8)) != 0 {
                1.0f32
            } else {
                -1.0f32
            };
            dot_qjl += s_y[row] * sign_val;
        }
        avg_dot_qjl += dot_qjl;
    }
    if m > 0 {
        avg_dot_qjl /= m as f32;
    }

    let scale = (std::f32::consts::PI / 2.0).sqrt() / dim as f32;
    let ip_estimate = dot_mse + scale * residual_norm * avg_dot_qjl;

    // L2 = ||q||² + ||x||² - 2<q, x>
    let x_norm_sq = norm * norm;
    state.q_norm_sq + x_norm_sq - 2.0 * ip_estimate
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::turbo_quant::codebook::{scaled_boundaries, scaled_centroids};
    use crate::vector::turbo_quant::encoder::padded_dimension;
    use crate::vector::turbo_quant::fwht;
    use crate::vector::turbo_quant::qjl::generate_qjl_matrix;

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

    /// Normalize a vector to unit length.
    fn normalize(v: &mut [f32]) -> f32 {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            v.iter_mut().for_each(|x| *x *= inv);
        }
        norm
    }

    /// Generate deterministic sign flips for testing.
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
    fn test_encode_tq_prod_fields() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let qjl_matrix = generate_qjl_matrix(dim, 999);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 77);
        normalize(&mut vec);

        let code = encode_tq_prod(
            &vec,
            &sign_flips,
            &boundaries,
            &centroids,
            &qjl_matrix,
            &mut work,
        );

        assert!(!code.mse_codes.is_empty(), "MSE codes should be non-empty");
        assert!(!code.qjl_signs.is_empty(), "QJL signs should be non-empty");
        assert_eq!(
            code.qjl_signs.len(),
            (dim + 7) / 8,
            "QJL signs should be ceil(dim/8) bytes"
        );
        assert!(
            code.original_norm > 0.0,
            "norm should be positive for non-zero vector"
        );
        assert!(
            code.residual_norm >= 0.0,
            "residual norm should be non-negative"
        );
        // Residual norm should be smaller than original norm (MSE distortion is bounded)
        assert!(
            code.residual_norm < code.original_norm,
            "residual norm {:.4} should be less than original norm {:.4}",
            code.residual_norm,
            code.original_norm
        );
    }

    #[test]
    fn test_inner_product_unbiased_estimator() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let qjl_matrix = generate_qjl_matrix(dim, 999);
        let mut work = vec![0.0f32; padded];

        // Random query vector
        let mut query = lcg_f32(dim, 12345);
        normalize(&mut query);

        let n = 1000;
        let mut sum_true_ip = 0.0f64;
        let mut sum_est_ip = 0.0f64;
        let mut sum_abs_true_ip = 0.0f64;

        for seed in 0..n {
            let mut vec = lcg_f32(dim, seed * 7 + 13);
            normalize(&mut vec);

            // True inner product
            let true_ip: f32 = query.iter().zip(vec.iter()).map(|(a, b)| a * b).sum();

            // Encode and score
            let code = encode_tq_prod(
                &vec,
                &sign_flips,
                &boundaries,
                &centroids,
                &qjl_matrix,
                &mut work,
            );
            let est_ip = score_inner_product(
                &query,
                &code,
                &sign_flips,
                &centroids,
                &qjl_matrix,
                &mut work,
            );

            sum_true_ip += true_ip as f64;
            sum_est_ip += est_ip as f64;
            sum_abs_true_ip += (true_ip as f64).abs();
        }

        let bias = (sum_est_ip - sum_true_ip) / sum_abs_true_ip;
        eprintln!(
            "TurboQuant_prod unbiased test: mean_true_ip={:.6}, mean_est_ip={:.6}, bias={:.6}",
            sum_true_ip / n as f64,
            sum_est_ip / n as f64,
            bias
        );

        assert!(
            bias.abs() < 0.05,
            "inner-product estimator bias {:.4} exceeds 5% tolerance (over {} vectors)",
            bias,
            n
        );
    }

    #[test]
    fn test_inner_product_self_score() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let qjl_matrix = generate_qjl_matrix(dim, 999);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 77);
        normalize(&mut vec);

        let norm_sq: f32 = vec.iter().map(|x| x * x).sum();
        let code = encode_tq_prod(
            &vec,
            &sign_flips,
            &boundaries,
            &centroids,
            &qjl_matrix,
            &mut work,
        );
        let self_score =
            score_inner_product(&vec, &code, &sign_flips, &centroids, &qjl_matrix, &mut work);

        // <x, x> should approximately equal ||x||^2 = 1.0 for unit vectors
        let relative_err = (self_score - norm_sq).abs() / norm_sq;
        eprintln!(
            "Self-score: expected={:.6}, got={:.6}, relative_err={:.6}",
            norm_sq, self_score, relative_err
        );
        assert!(
            relative_err < 0.15,
            "self-score relative error {:.4} exceeds 15% tolerance",
            relative_err
        );
    }

    #[test]
    fn test_inner_product_orthogonal_near_zero() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries(padded as u32);
        let centroids = scaled_centroids(padded as u32);
        let qjl_matrix = generate_qjl_matrix(dim, 999);
        let mut work = vec![0.0f32; padded];

        // Construct near-orthogonal vectors: e_0 and e_1
        let mut v1 = vec![0.0f32; dim];
        v1[0] = 1.0;
        let mut v2 = vec![0.0f32; dim];
        v2[1] = 1.0;

        let code = encode_tq_prod(
            &v2,
            &sign_flips,
            &boundaries,
            &centroids,
            &qjl_matrix,
            &mut work,
        );
        let score =
            score_inner_product(&v1, &code, &sign_flips, &centroids, &qjl_matrix, &mut work);

        eprintln!("Orthogonal score: {:.6} (expected ~0.0)", score);
        assert!(
            score.abs() < 0.3,
            "orthogonal vectors should score near 0, got {:.4}",
            score
        );
    }

    #[test]
    fn test_encode_tq_prod_v2_saves_bits() {
        fwht::init_fwht();
        let dim = 128;
        let padded = padded_dimension(dim as u32) as usize;
        let sign_flips = test_sign_flips(padded, 42);
        let qjl_matrix = generate_qjl_matrix(dim, 999);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 77);
        normalize(&mut vec);

        // v1: 4-bit MSE + QJL signs
        let boundaries_4 = scaled_boundaries(padded as u32);
        let centroids_4 = scaled_centroids(padded as u32);
        let code_v1 = encode_tq_prod(
            &vec,
            &sign_flips,
            &boundaries_4,
            &centroids_4,
            &qjl_matrix,
            &mut work,
        );
        let v1_bytes = code_v1.mse_codes.len() + code_v1.qjl_signs.len();

        // v2: 3-bit MSE + QJL signs (paper-correct)
        let boundaries_3 =
            crate::vector::turbo_quant::codebook::scaled_boundaries_n(padded as u32, 3).unwrap();
        let centroids_3 =
            crate::vector::turbo_quant::codebook::scaled_centroids_n(padded as u32, 3).unwrap();
        let code_v2 = encode_tq_prod_v2(
            &vec,
            &sign_flips,
            &boundaries_3,
            &centroids_3,
            3,
            &qjl_matrix,
            &mut work,
        );
        let v2_bytes = code_v2.mse_codes.len() + code_v2.qjl_signs.len();

        // v2 should use fewer bytes for MSE codes
        assert!(
            v2_bytes < v1_bytes,
            "v2 ({v2_bytes} bytes) should be smaller than v1 ({v1_bytes} bytes)"
        );
        assert!(code_v2.residual_norm >= 0.0);
        assert!(code_v2.original_norm > 0.0);
    }
}
