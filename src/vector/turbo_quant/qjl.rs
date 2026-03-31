//! QJL (Quantized Johnson-Lindenstrauss) transform.
//!
//! Implements the sign-bit random projection from arXiv 2504.19874 Section 3.2.
//! Given a random Gaussian matrix S (d x d), stores sign(S * x) as d bits.
//! Used by TurboQuant_prod for unbiased inner-product estimation.

/// Generate a d x d random Gaussian matrix (row-major) using LCG PRNG.
///
/// Each element is drawn from approximate N(0, 1) via Box-Muller.
/// The matrix is stored once per collection (~d^2 * 4 bytes, e.g., 2.25 MB for d=768).
/// Seed is deterministic for reproducibility.
pub fn generate_qjl_matrix(dim: usize, seed: u64) -> Vec<f32> {
    let n = dim * dim;
    let mut matrix = Vec::with_capacity(n);
    let mut state = seed;

    let mut i = 0;
    while i < n {
        // LCG (Knuth MMIX constants)
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let u1 = ((state >> 40) as f32 / (1u64 << 24) as f32).max(1e-7);
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let u2 = (state >> 40) as f32 / (1u64 << 24) as f32;

        let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
        let z1 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).sin();

        matrix.push(z0);
        i += 1;
        if i < n {
            matrix.push(z1);
            i += 1;
        }
    }
    matrix
}

/// Compute sign(S * x) and pack into bits.
///
/// `matrix_s`: d x d row-major Gaussian matrix.
/// `vector`: d-dimensional input vector.
/// `dim`: dimension d.
///
/// Returns packed sign bits: dim bits = ceil(dim/8) bytes.
/// Bit layout: byte[i] bit j = sign of (S * x)[i*8 + j], 1 = positive/zero, 0 = negative.
pub fn qjl_encode(matrix_s: &[f32], vector: &[f32], dim: usize) -> Vec<u8> {
    debug_assert_eq!(matrix_s.len(), dim * dim);
    debug_assert_eq!(vector.len(), dim);

    let num_bytes = (dim + 7) / 8;
    let mut signs = vec![0u8; num_bytes];

    for row in 0..dim {
        // Compute dot product: S[row, :] . vector
        let row_start = row * dim;
        let mut dot = 0.0f32;
        for col in 0..dim {
            dot += matrix_s[row_start + col] * vector[col];
        }
        // Store sign bit: 1 = non-negative, 0 = negative
        if dot >= 0.0 {
            signs[row / 8] |= 1 << (row % 8);
        }
    }
    signs
}

/// Compute the QJL correction vector: sqrt(pi/2)/d * residual_norm * S^T * signs.
///
/// `matrix_s`: d x d row-major Gaussian matrix.
/// `signs`: packed sign bits from qjl_encode (ceil(dim/8) bytes).
/// `residual_norm`: ||r|| where r = x - DeQuant_mse(idx).
/// `dim`: dimension d.
///
/// Returns d-dimensional correction vector to add to MSE reconstruction.
pub fn qjl_decode_correction(
    matrix_s: &[f32],
    signs: &[u8],
    residual_norm: f32,
    dim: usize,
) -> Vec<f32> {
    debug_assert_eq!(matrix_s.len(), dim * dim);

    let scale = (std::f32::consts::PI / 2.0).sqrt() / dim as f32 * residual_norm;
    let mut correction = vec![0.0f32; dim];

    // S^T * sign_vector:
    // correction[col] = sum over row of S[row, col] * sign_val[row]
    // where sign_val[row] = +1.0 if bit set, -1.0 if not
    for row in 0..dim {
        let sign_val = if signs[row / 8] & (1 << (row % 8)) != 0 {
            1.0f32
        } else {
            -1.0f32
        };
        let row_start = row * dim;
        for col in 0..dim {
            correction[col] += matrix_s[row_start + col] * sign_val;
        }
    }

    // Scale by sqrt(pi/2)/d * ||r||
    for v in correction.iter_mut() {
        *v *= scale;
    }
    correction
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_qjl_matrix_deterministic() {
        let m1 = generate_qjl_matrix(64, 42);
        let m2 = generate_qjl_matrix(64, 42);
        assert_eq!(m1, m2, "same seed must produce identical matrix");
    }

    #[test]
    fn test_generate_qjl_matrix_size() {
        let m = generate_qjl_matrix(128, 99);
        assert_eq!(
            m.len(),
            128 * 128,
            "128x128 matrix should have 16384 elements"
        );
    }

    #[test]
    fn test_qjl_encode_zero_vector() {
        let dim = 64;
        let matrix = generate_qjl_matrix(dim, 42);
        let zero = vec![0.0f32; dim];
        let signs = qjl_encode(&matrix, &zero, dim);

        // S * 0 = 0, and 0.0 >= 0.0 is true, so all bits should be set
        assert_eq!(signs.len(), dim / 8);
        for &byte in &signs {
            assert_eq!(byte, 0xFF, "zero vector should produce all-positive signs");
        }
    }

    #[test]
    fn test_qjl_encode_output_size() {
        let dim = 128;
        let matrix = generate_qjl_matrix(dim, 7);
        let vec = vec![1.0f32; dim];
        let signs = qjl_encode(&matrix, &vec, dim);
        assert_eq!(signs.len(), 16, "128 bits = 16 bytes");
    }

    #[test]
    fn test_qjl_encode_decode_roundtrip() {
        let dim = 128;
        let matrix = generate_qjl_matrix(dim, 12345);

        // Create a random-ish vector as "residual"
        let mut residual = Vec::with_capacity(dim);
        let mut state = 777u32;
        for _ in 0..dim {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            residual.push((state as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }

        let r_norm: f32 = residual.iter().map(|x| x * x).sum::<f32>().sqrt();
        let signs = qjl_encode(&matrix, &residual, dim);
        let correction = qjl_decode_correction(&matrix, &signs, r_norm, dim);

        // Correction vector norm should be proportional to residual_norm
        let c_norm: f32 = correction.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(c_norm > 0.0, "correction vector should be non-zero");
        // The correction norm should be in a reasonable range relative to residual_norm
        // sqrt(pi/2)/d * ||r|| * ||S^T * signs|| -- ||S^T * signs|| ~ sqrt(d) * sqrt(d) = d for Gaussian S
        // So c_norm ~ sqrt(pi/2)/d * ||r|| * d = sqrt(pi/2) * ||r|| ~ 1.25 * ||r||
        let ratio = c_norm / r_norm;
        assert!(
            ratio > 0.3 && ratio < 5.0,
            "correction/residual norm ratio {ratio} out of expected range [0.3, 5.0]"
        );
    }
}
