//! Lloyd-Max 16-centroid codebook for TurboQuant 4-bit quantization.
//!
//! After randomized FWHT of a unit vector in R^d (padded to next power of 2),
//! each coordinate follows approximately N(0, 1/sqrt(padded_dim)). The Lloyd-Max
//! algorithm finds centroids that minimize mean squared error for this
//! distribution.
//!
//! The standard Lloyd-Max centroids for N(0,1) at 16 levels are stored
//! UNSCALED. Scaling by sigma = 1/sqrt(padded_dim) happens at runtime
//! via `scaled_centroids()` and `scaled_boundaries()`, which are stored
//! in CollectionMetadata per collection.
//!
//! CRITICAL: The previous version hardcoded 1/sqrt(768) scaling, which was
//! WRONG for any dimension != 768 (e.g., 128 pads to 128, 768 pads to 1024).
//! The FWHT normalization uses 1/sqrt(padded_dim), so the codebook must match.

/// Codebook version for forward compatibility.
/// Bumped to 2: dimension-adaptive scaling (fixes recall bug from v1).
pub const CODEBOOK_VERSION: u8 = 2;

/// Standard N(0,1) Lloyd-Max 16-level centroids (Panter & Dite, 1951).
/// UNSCALED — must be multiplied by sigma = 1/sqrt(padded_dim) before use.
///
///   +/-2.4008, +/-1.8435, +/-1.4371, +/-1.0993,
///   +/-0.7990, +/-0.5282, +/-0.2743, +/-0.0298
///
/// Invariants:
/// - Sorted ascending
/// - Symmetric: `RAW_CENTROIDS[i] == -RAW_CENTROIDS[15-i]`
pub const RAW_CENTROIDS: [f32; 16] = [
    -2.4008, -1.8435, -1.4371, -1.0993,
    -0.7990, -0.5282, -0.2743, -0.0298,
     0.0298,  0.2743,  0.5282,  0.7990,
     1.0993,  1.4371,  1.8435,  2.4008,
];

/// Raw N(0,1) decision boundaries (midpoints between adjacent RAW_CENTROIDS).
pub const RAW_BOUNDARIES: [f32; 15] = [
    -2.12215,  // mid(-2.4008, -1.8435)
    -1.6403,   // mid(-1.8435, -1.4371)
    -1.2682,   // mid(-1.4371, -1.0993)
    -0.94915,  // mid(-1.0993, -0.7990)
    -0.6636,   // mid(-0.7990, -0.5282)
    -0.40125,  // mid(-0.5282, -0.2743)
    -0.15205,  // mid(-0.2743, -0.0298)
     0.0,      // mid(-0.0298,  0.0298) — exact zero by symmetry
     0.15205,  // mid( 0.0298,  0.2743)
     0.40125,  // mid( 0.2743,  0.5282)
     0.6636,   // mid( 0.5282,  0.7990)
     0.94915,  // mid( 0.7990,  1.0993)
     1.2682,   // mid( 1.0993,  1.4371)
     1.6403,   // mid( 1.4371,  1.8435)
     2.12215,  // mid( 1.8435,  2.4008)
];

/// Compute dimension-scaled centroids for a given padded dimension.
/// sigma = 1/sqrt(padded_dim), which matches the FWHT normalization.
pub fn scaled_centroids(padded_dim: u32) -> [f32; 16] {
    let sigma = 1.0 / (padded_dim as f32).sqrt();
    let mut c = [0.0f32; 16];
    for i in 0..16 {
        c[i] = RAW_CENTROIDS[i] * sigma;
    }
    c
}

/// Compute dimension-scaled boundaries for a given padded dimension.
pub fn scaled_boundaries(padded_dim: u32) -> [f32; 15] {
    let sigma = 1.0 / (padded_dim as f32).sqrt();
    let mut b = [0.0f32; 15];
    for i in 0..15 {
        b[i] = RAW_BOUNDARIES[i] * sigma;
    }
    b
}

/// Legacy constants for backward compatibility with codebook_version=1.
/// Scaled by 1/sqrt(768) — ONLY correct for dim=768 with no padding.
pub const CENTROIDS: [f32; 16] = [
    -0.086_643, -0.066_523, -0.051_858, -0.039_666,
    -0.028_829, -0.019_060, -0.009_897, -0.001_075,
     0.001_075,  0.009_897,  0.019_060,  0.028_829,
     0.039_666,  0.051_858,  0.066_523,  0.086_643,
];

/// Legacy boundaries for backward compatibility.
pub const BOUNDARIES: [f32; 15] = [
    -0.076_583, -0.059_190_5, -0.045_762, -0.034_247_5,
    -0.023_944_5, -0.014_478_5, -0.005_486, 0.0,
     0.005_486,  0.014_478_5,  0.023_944_5,  0.034_247_5,
     0.045_762,  0.059_190_5,  0.076_583,
];

// ── 1-bit Lloyd-Max codebook for N(0,1) ──────────────────────────────

/// 1-bit (2 centroids): +/- sqrt(2/pi) for N(0,1).
pub const RAW_CENTROIDS_1BIT: [f32; 2] = [-0.7979, 0.7979];

/// 1-bit boundary: single threshold at zero.
pub const RAW_BOUNDARIES_1BIT: [f32; 1] = [0.0];

// ── 2-bit Lloyd-Max codebook for N(0,1) ──────────────────────────────

/// 2-bit (4 centroids): Lloyd-Max optimal for N(0,1) with 4 levels.
pub const RAW_CENTROIDS_2BIT: [f32; 4] = [-1.5104, -0.4528, 0.4528, 1.5104];

/// 2-bit boundaries: midpoints between adjacent 2-bit centroids.
pub const RAW_BOUNDARIES_2BIT: [f32; 3] = [-0.9816, 0.0, 0.9816];

// ── 3-bit Lloyd-Max codebook for N(0,1) ──────────────────────────────

/// 3-bit (8 centroids): Lloyd-Max optimal for N(0,1) with 8 levels.
pub const RAW_CENTROIDS_3BIT: [f32; 8] = [
    -2.1520, -1.3440, -0.7560, -0.2451,
     0.2451,  0.7560,  1.3440,  2.1520,
];

/// 3-bit boundaries: midpoints between adjacent 3-bit centroids.
pub const RAW_BOUNDARIES_3BIT: [f32; 7] = [
    -1.7480, -1.0500, -0.5006, 0.0, 0.5006, 1.0500, 1.7480,
];

/// Compute dimension-scaled centroids for any bit width (1-4).
///
/// Returns a Vec because the size varies by bit width.
/// sigma = 1/sqrt(padded_dim), matching FWHT normalization.
pub fn scaled_centroids_n(padded_dim: u32, bits: u8) -> Vec<f32> {
    let sigma = 1.0 / (padded_dim as f32).sqrt();
    match bits {
        1 => RAW_CENTROIDS_1BIT.iter().map(|&c| c * sigma).collect(),
        2 => RAW_CENTROIDS_2BIT.iter().map(|&c| c * sigma).collect(),
        3 => RAW_CENTROIDS_3BIT.iter().map(|&c| c * sigma).collect(),
        4 => {
            let sc = scaled_centroids(padded_dim);
            sc.to_vec()
        }
        _ => panic!("unsupported bit width: {bits}"),
    }
}

/// Compute dimension-scaled boundaries for any bit width (1-4).
pub fn scaled_boundaries_n(padded_dim: u32, bits: u8) -> Vec<f32> {
    let sigma = 1.0 / (padded_dim as f32).sqrt();
    match bits {
        1 => RAW_BOUNDARIES_1BIT.iter().map(|&b| b * sigma).collect(),
        2 => RAW_BOUNDARIES_2BIT.iter().map(|&b| b * sigma).collect(),
        3 => RAW_BOUNDARIES_3BIT.iter().map(|&b| b * sigma).collect(),
        4 => {
            let sb = scaled_boundaries(padded_dim);
            sb.to_vec()
        }
        _ => panic!("unsupported bit width: {bits}"),
    }
}

/// Generic quantizer for any bit width. Scans boundaries linearly.
///
/// For 1-bit this is equivalent to `if val >= 0.0 { 1 } else { 0 }`.
#[inline]
pub fn quantize_with_boundaries_n(val: f32, boundaries: &[f32], n_centroids: u8) -> u8 {
    let _ = n_centroids; // used for debug_assert below
    debug_assert_eq!(boundaries.len(), (n_centroids - 1) as usize);
    let mut idx = 0u8;
    for &b in boundaries.iter() {
        if val >= b {
            idx += 1;
        } else {
            break;
        }
    }
    idx
}

/// Compute packed code size in bytes for a given dimension and bit width.
///
/// 1-bit: pdim/8, 2-bit: pdim/4, 3-bit: (pdim*3+7)/8, 4-bit: pdim/2.
#[inline]
pub fn code_bytes_per_vector(padded_dim: u32, bits: u8) -> usize {
    let pd = padded_dim as usize;
    match bits {
        1 => pd / 8,
        2 => pd / 4,
        3 => (pd * 3 + 7) / 8,
        4 => pd / 2,
        _ => panic!("unsupported bit width: {bits}"),
    }
}

/// Quantize a single f32 value using LEGACY boundaries (1/sqrt(768) scaling).
/// DEPRECATED: Use `quantize_with_boundaries` for dimension-adaptive quantization.
#[inline]
pub fn quantize_scalar(val: f32) -> u8 {
    quantize_with_boundaries(val, &BOUNDARIES)
}

/// Quantize a single f32 value to its nearest centroid index (0..15)
/// using the provided dimension-scaled boundaries.
///
/// Uses linear scan through boundaries. For 15 comparisons this is faster
/// than binary search due to branch prediction on the sorted data.
#[inline]
pub fn quantize_with_boundaries(val: f32, boundaries: &[f32; 15]) -> u8 {
    let mut idx = 0u8;
    for &b in boundaries.iter() {
        if val >= b {
            idx += 1;
        } else {
            break;
        }
    }
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_centroids_count() {
        assert_eq!(CENTROIDS.len(), 16);
    }

    #[test]
    fn test_boundaries_count() {
        assert_eq!(BOUNDARIES.len(), 15);
    }

    #[test]
    fn test_centroids_sorted_ascending() {
        for i in 1..16 {
            assert!(
                CENTROIDS[i] > CENTROIDS[i - 1],
                "CENTROIDS not sorted at index {i}: {} <= {}",
                CENTROIDS[i],
                CENTROIDS[i - 1]
            );
        }
    }

    #[test]
    fn test_centroids_symmetric() {
        for i in 0..16 {
            let diff = (CENTROIDS[i] + CENTROIDS[15 - i]).abs();
            assert!(
                diff < 1e-6,
                "Symmetry violated: C[{i}]={} != -C[{}]={}",
                CENTROIDS[i],
                15 - i,
                CENTROIDS[15 - i]
            );
        }
    }

    #[test]
    fn test_boundaries_are_midpoints() {
        for i in 0..15 {
            let expected = (CENTROIDS[i] + CENTROIDS[i + 1]) / 2.0;
            let diff = (BOUNDARIES[i] - expected).abs();
            assert!(
                diff < 1e-5,
                "Boundary[{i}]={} != midpoint({}, {}) = {}",
                BOUNDARIES[i],
                CENTROIDS[i],
                CENTROIDS[i + 1],
                expected
            );
        }
    }

    #[test]
    fn test_quantize_centroids_are_fixed_points() {
        for k in 0..16u8 {
            let idx = quantize_scalar(CENTROIDS[k as usize]);
            assert_eq!(
                idx, k,
                "quantize_scalar(CENTROIDS[{k}]={}) = {idx}, expected {k}",
                CENTROIDS[k as usize]
            );
        }
    }

    #[test]
    fn test_quantize_extreme_values() {
        // Very negative -> index 0
        assert_eq!(quantize_scalar(-1.0), 0);
        // Very positive -> index 15
        assert_eq!(quantize_scalar(1.0), 15);
        // Zero -> index 8 (center boundary is 0.0, so >= 0.0 -> idx 8)
        assert_eq!(quantize_scalar(0.0), 8);
    }

    #[test]
    fn test_quantize_just_below_boundary() {
        // Just below first boundary should give index 0
        let val = BOUNDARIES[0] - 1e-7;
        assert_eq!(quantize_scalar(val), 0);
    }

    #[test]
    fn test_codebook_version() {
        assert_eq!(CODEBOOK_VERSION, 2);
    }

    // ── Multi-bit codebook tests ──────────────────────────────────────

    #[test]
    fn test_1bit_centroids() {
        assert_eq!(RAW_CENTROIDS_1BIT.len(), 2);
        // Symmetric around 0
        assert!((RAW_CENTROIDS_1BIT[0] + RAW_CENTROIDS_1BIT[1]).abs() < 1e-6);
        // Values = +/- sqrt(2/pi) ~ 0.7979
        assert!((RAW_CENTROIDS_1BIT[1] - 0.7979).abs() < 0.001);
    }

    #[test]
    fn test_1bit_boundaries() {
        assert_eq!(RAW_BOUNDARIES_1BIT.len(), 1);
        assert_eq!(RAW_BOUNDARIES_1BIT[0], 0.0);
    }

    #[test]
    fn test_2bit_centroids() {
        assert_eq!(RAW_CENTROIDS_2BIT.len(), 4);
        // Symmetric
        for i in 0..4 {
            let diff = (RAW_CENTROIDS_2BIT[i] + RAW_CENTROIDS_2BIT[3 - i]).abs();
            assert!(diff < 1e-6, "2-bit symmetry violated at {i}");
        }
        // Specific values
        assert!((RAW_CENTROIDS_2BIT[0] - (-1.5104)).abs() < 0.001);
        assert!((RAW_CENTROIDS_2BIT[1] - (-0.4528)).abs() < 0.001);
    }

    #[test]
    fn test_2bit_boundaries() {
        assert_eq!(RAW_BOUNDARIES_2BIT.len(), 3);
        assert!((RAW_BOUNDARIES_2BIT[0] - (-0.9816)).abs() < 0.001);
        assert_eq!(RAW_BOUNDARIES_2BIT[1], 0.0);
        assert!((RAW_BOUNDARIES_2BIT[2] - 0.9816).abs() < 0.001);
    }

    #[test]
    fn test_3bit_centroids() {
        assert_eq!(RAW_CENTROIDS_3BIT.len(), 8);
        // Symmetric
        for i in 0..8 {
            let diff = (RAW_CENTROIDS_3BIT[i] + RAW_CENTROIDS_3BIT[7 - i]).abs();
            assert!(diff < 1e-4, "3-bit symmetry violated at {i}: {} vs {}", RAW_CENTROIDS_3BIT[i], RAW_CENTROIDS_3BIT[7 - i]);
        }
        // Sorted ascending
        for i in 1..8 {
            assert!(RAW_CENTROIDS_3BIT[i] > RAW_CENTROIDS_3BIT[i - 1]);
        }
    }

    #[test]
    fn test_3bit_boundaries() {
        assert_eq!(RAW_BOUNDARIES_3BIT.len(), 7);
        // Symmetric
        for i in 0..7 {
            let diff = (RAW_BOUNDARIES_3BIT[i] + RAW_BOUNDARIES_3BIT[6 - i]).abs();
            assert!(diff < 1e-4, "3-bit boundary symmetry violated at {i}");
        }
        // Center boundary is 0
        assert_eq!(RAW_BOUNDARIES_3BIT[3], 0.0);
    }

    #[test]
    fn test_scaled_centroids_n_sizes() {
        let pdim = 1024u32;
        assert_eq!(scaled_centroids_n(pdim, 1).len(), 2);
        assert_eq!(scaled_centroids_n(pdim, 2).len(), 4);
        assert_eq!(scaled_centroids_n(pdim, 3).len(), 8);
        assert_eq!(scaled_centroids_n(pdim, 4).len(), 16);
    }

    #[test]
    fn test_scaled_centroids_n_values() {
        let pdim = 1024u32;
        let sigma = 1.0 / (pdim as f32).sqrt();
        let c1 = scaled_centroids_n(pdim, 1);
        assert!((c1[1] - 0.7979 * sigma).abs() < 1e-6);
        let c2 = scaled_centroids_n(pdim, 2);
        assert!((c2[3] - 1.5104 * sigma).abs() < 1e-5);
    }

    #[test]
    fn test_quantize_with_boundaries_n_1bit() {
        let b = &RAW_BOUNDARIES_1BIT[..];
        assert_eq!(quantize_with_boundaries_n(-1.0, b, 2), 0);
        assert_eq!(quantize_with_boundaries_n(0.5, b, 2), 1);
        assert_eq!(quantize_with_boundaries_n(0.0, b, 2), 1); // >= 0.0 -> 1
    }

    #[test]
    fn test_quantize_with_boundaries_n_2bit() {
        let b = &RAW_BOUNDARIES_2BIT[..];
        assert_eq!(quantize_with_boundaries_n(-2.0, b, 4), 0);
        assert_eq!(quantize_with_boundaries_n(-0.5, b, 4), 1);
        assert_eq!(quantize_with_boundaries_n(0.5, b, 4), 2);
        assert_eq!(quantize_with_boundaries_n(2.0, b, 4), 3);
    }

    #[test]
    fn test_quantize_with_boundaries_n_3bit() {
        let b = &RAW_BOUNDARIES_3BIT[..];
        assert_eq!(quantize_with_boundaries_n(-3.0, b, 8), 0);
        assert_eq!(quantize_with_boundaries_n(3.0, b, 8), 7);
        assert_eq!(quantize_with_boundaries_n(0.0, b, 8), 4); // >= 0.0
    }

    #[test]
    fn test_code_bytes_per_vector() {
        let pdim = 1024u32;
        assert_eq!(code_bytes_per_vector(pdim, 1), 128);  // 1024/8
        assert_eq!(code_bytes_per_vector(pdim, 2), 256);  // 1024/4
        assert_eq!(code_bytes_per_vector(pdim, 3), 384);  // (1024*3+7)/8 = 384
        assert_eq!(code_bytes_per_vector(pdim, 4), 512);  // 1024/2
    }
}
