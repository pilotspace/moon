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
        assert_eq!(CODEBOOK_VERSION, 1);
    }
}
