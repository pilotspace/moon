//! Lloyd-Max 16-centroid codebook for TurboQuant 4-bit quantization.
//!
//! After randomized FWHT of a unit vector in R^d (d=768, padded to 1024),
//! each coordinate follows approximately N(0, 1/sqrt(d)). The Lloyd-Max
//! algorithm finds centroids that minimize mean squared error for this
//! distribution.
//!
//! The standard Lloyd-Max centroids for N(0,1) at 16 levels are scaled
//! by sigma = 1/sqrt(768) to match the FWHT output distribution.

/// Codebook version for forward compatibility.
///
/// Checked at segment load time. Future codebook changes use versioned decode.
pub const CODEBOOK_VERSION: u8 = 1;

/// Lloyd-Max optimal 16-centroid codebook for FWHT-rotated unit vectors.
///
/// Standard N(0,1) Lloyd-Max 16-level centroids (Panter & Dite, 1951):
///   +/-2.4008, +/-1.8435, +/-1.4371, +/-1.0993,
///   +/-0.7990, +/-0.5282, +/-0.2743, +/-0.0298
///
/// Scaled by sigma = 1/sqrt(768) = 0.036084...
///
/// Invariants:
/// - Sorted ascending
/// - Symmetric: `CENTROIDS[i] == -CENTROIDS[15-i]`
/// - `quantize_scalar(CENTROIDS[k]) == k` for all k (fixed-point property)
pub const CENTROIDS: [f32; 16] = [
    -0.086_643, // -2.4008 / sqrt(768)
    -0.066_523, // -1.8435 / sqrt(768)
    -0.051_858, // -1.4371 / sqrt(768)
    -0.039_666, // -1.0993 / sqrt(768)
    -0.028_829, // -0.7990 / sqrt(768)
    -0.019_060, // -0.5282 / sqrt(768)
    -0.009_897, // -0.2743 / sqrt(768)
    -0.001_075, // -0.0298 / sqrt(768)
    0.001_075,  //  0.0298 / sqrt(768)
    0.009_897,  //  0.2743 / sqrt(768)
    0.019_060,  //  0.5282 / sqrt(768)
    0.028_829,  //  0.7990 / sqrt(768)
    0.039_666,  //  1.0993 / sqrt(768)
    0.051_858,  //  1.4371 / sqrt(768)
    0.066_523,  //  1.8435 / sqrt(768)
    0.086_643,  //  2.4008 / sqrt(768)
];

/// Decision boundaries: midpoints between adjacent centroids.
///
/// `quantize_scalar(x) = k` where `BOUNDARIES[k-1] <= x < BOUNDARIES[k]`,
/// with implicit `-inf` at the left and `+inf` at the right.
pub const BOUNDARIES: [f32; 15] = [
    -0.076_583, // mid(C[0], C[1])
    -0.059_190_5, // mid(C[1], C[2])
    -0.045_762, // mid(C[2], C[3])
    -0.034_247_5, // mid(C[3], C[4])
    -0.023_944_5, // mid(C[4], C[5])
    -0.014_478_5, // mid(C[5], C[6])
    -0.005_486, // mid(C[6], C[7])
    0.0,        // mid(C[7], C[8]) — exact zero by symmetry
    0.005_486,  // mid(C[8], C[9])
    0.014_478_5, // mid(C[9], C[10])
    0.023_944_5, // mid(C[10], C[11])
    0.034_247_5, // mid(C[11], C[12])
    0.045_762,  // mid(C[12], C[13])
    0.059_190_5, // mid(C[13], C[14])
    0.076_583,  // mid(C[14], C[15])
];

/// Quantize a single f32 value to its nearest centroid index (0..15).
///
/// Uses linear scan through boundaries. For 15 comparisons this is faster
/// than binary search due to branch prediction on the sorted data.
#[inline]
pub fn quantize_scalar(val: f32) -> u8 {
    let mut idx = 0u8;
    for &b in BOUNDARIES.iter() {
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
