//! QJL (Quantized Johnson-Lindenstrauss) transform.
//!
//! Implements the sign-bit random projection from arXiv 2504.19874 Section 3.2.
//! Given a random Gaussian matrix S (d x d), stores sign(S * x) as d bits.
//! Used by TurboQuant_prod for unbiased inner-product estimation.

/// Generate a d x d random Gaussian matrix (row-major) using LCG PRNG.
///
/// Each element is drawn from approximate N(0, 1) via Box-Muller.
/// Seed is deterministic for reproducibility. `d² · 4` bytes (2.25 MB at
/// d=768): collections no longer hold these (moon#1213) — see
/// [`for_each_qjl_chunk`] for the allocation-free stream the metadata
/// checksum uses.
pub fn generate_qjl_matrix(dim: usize, seed: u64) -> Vec<f32> {
    let mut matrix = Vec::with_capacity(dim * dim);
    for_each_qjl_chunk(dim, seed, |chunk| matrix.extend_from_slice(chunk));
    matrix
}

/// Stream the `dim * dim` values of [`generate_qjl_matrix`]`(dim, seed)`, in
/// order, to `sink` in chunks of at most [`QJL_STREAM_CHUNK`] values —
/// without materializing the matrix. [`generate_qjl_matrix`] is this stream
/// collected, so the two can never drift apart.
pub fn for_each_qjl_chunk(dim: usize, seed: u64, mut sink: impl FnMut(&[f32])) {
    let n = dim * dim;
    let mut buf = [0.0f32; QJL_STREAM_CHUNK];
    let mut len = 0usize;
    let mut state = seed;

    let mut i = 0;
    while i < n {
        if len + 2 > QJL_STREAM_CHUNK {
            sink(&buf[..len]);
            len = 0;
        }
        // LCG (Knuth MMIX constants)
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let u1 = ((state >> 40) as f32 / (1u64 << 24) as f32).max(1e-7);
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let u2 = (state >> 40) as f32 / (1u64 << 24) as f32;

        // Box-Muller; the radius is computed once (same value both times).
        let r = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f32::consts::PI * u2;
        buf[len] = r * theta.cos();
        len += 1;
        i += 1;
        if i < n {
            buf[len] = r * theta.sin();
            len += 1;
            i += 1;
        }
    }
    if len > 0 {
        sink(&buf[..len]);
    }
}

/// Maximum values per [`for_each_qjl_chunk`] callback (16 KiB of f32).
pub const QJL_STREAM_CHUNK: usize = 4096;

/// Compute sign(S * x) and pack into bits.
///
/// `matrix_s`: d x d row-major Gaussian matrix.
/// `vector`: d-dimensional input vector.
/// `dim`: dimension d.
///
/// Returns packed sign bits: dim bits = ceil(dim/8) bytes.
/// Bit layout: byte[i] bit j = sign of (S * x)[i*8 + j], 1 = positive/zero, 0 = negative.
pub fn qjl_encode(matrix_s: &[f32], vector: &[f32], dim: usize) -> Vec<u8> {
    let mut signs = vec![0u8; dim.div_ceil(8)];
    qjl_encode_into(matrix_s, vector, dim, &mut signs);
    signs
}

/// [`qjl_encode`] into a caller-provided, ZEROED `out` (`ceil(dim/8)` bytes).
///
/// Each row's `S[row, :] · x` runs on the runtime-dispatched SIMD `dot_f32`
/// kernel (`distance::table()`, scalar fallback on every target) instead of
/// a serial scalar loop (moon#1192). SIMD reassociation can flip the sign of
/// a row whose dot product is within rounding of zero — such a row carries
/// no information about the residual's direction either way.
pub fn qjl_encode_into(matrix_s: &[f32], vector: &[f32], dim: usize, out: &mut [u8]) {
    debug_assert_eq!(matrix_s.len(), dim * dim);
    debug_assert_eq!(vector.len(), dim);
    debug_assert!(out.len() >= dim.div_ceil(8));
    if dim == 0 {
        return;
    }
    let dot = crate::vector::distance::table().dot_f32;
    for (row, s_row) in matrix_s.chunks_exact(dim).take(dim).enumerate() {
        // Store sign bit: 1 = non-negative, 0 = negative
        if dot(s_row, vector) >= 0.0 {
            out[row / 8] |= 1 << (row % 8);
        }
    }
}

/// The pre-moon#1192 scalar matvec, kept as the reference for tests.
#[cfg(test)]
pub(crate) fn qjl_encode_scalar_reference(matrix_s: &[f32], vector: &[f32], dim: usize) -> Vec<u8> {
    let mut signs = vec![0u8; dim.div_ceil(8)];
    for row in 0..dim {
        let row_start = row * dim;
        let mut dot = 0.0f32;
        for col in 0..dim {
            dot += matrix_s[row_start + col] * vector[col];
        }
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
    fn qjl_stream_chunks_concatenate_to_the_reference_generator() {
        // Reference: the pre-moon#1213 generator, verbatim.
        fn reference(dim: usize, seed: u64) -> Vec<f32> {
            let n = dim * dim;
            let mut matrix = Vec::with_capacity(n);
            let mut state = seed;
            let mut i = 0;
            while i < n {
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
        // Odd d² (last pair truncated), chunk-boundary sizes, and 0.
        for (dim, seed) in [(0usize, 1u64), (1, 2), (7, 3), (64, 4), (65, 5), (91, 6)] {
            let want = reference(dim, seed);
            let mut got = Vec::new();
            let mut max_chunk = 0;
            for_each_qjl_chunk(dim, seed, |c| {
                max_chunk = max_chunk.max(c.len());
                got.extend_from_slice(c);
            });
            assert!(max_chunk <= QJL_STREAM_CHUNK);
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            assert_eq!(bits(&got), bits(&want), "dim={dim}");
            assert_eq!(bits(&generate_qjl_matrix(dim, seed)), bits(&want));
        }
    }

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
