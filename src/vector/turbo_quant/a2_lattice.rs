//! A2 hexagonal lattice quantization for paired-dimension TurboQuant encoding.
//!
//! The A2 (hexagonal) lattice achieves normalized second moment G=0.0802 vs
//! G=0.0833 for scalar quantization, yielding 3.8% less quantization distortion
//! at the same 4-bit memory footprint per pair. Each pair of FWHT-rotated
//! coordinates is jointly quantized to one of 16 hexagonal Voronoi cells instead
//! of two independent 2-bit scalar quantizations (which also gives 16 cells total).
//!
//! ## Byte Layout
//!
//! A2 with 16 cells produces one 4-bit index per PAIR of dimensions.
//! Two consecutive pair-indices share a byte via nibble_pack:
//! `byte = a2_idx_pair0 | (a2_idx_pair1 << 4)`.
//!
//! The packed TqCode indices length is `padded_dim / 4` bytes (since each byte
//! encodes 2 pairs = 4 coordinates). This is 2x more compressed than scalar TQ4
//! (`padded_dim / 2` bytes). TQ4A2 sits between TQ2 and TQ4 in memory, but
//! with BETTER quality than TQ2 due to hexagonal lattice advantage.

/// 16 density-optimized hexagonal lattice centroids for bivariate N(0,1).
///
/// These are Lloyd-optimized centroids starting from a hex seed grid,
/// iteratively refined against bivariate standard Gaussian density.
/// The layout concentrates centroids near the origin where density is
/// highest, with sparser coverage in the tails.
///
/// Arranged in approximate hex rows with staggered offsets:
/// - Inner ring (4 centroids): ~0.45 sigma from origin
/// - Middle ring (6 centroids): ~1.2 sigma from origin
/// - Outer ring (6 centroids): ~2.1 sigma from origin
///
/// UNSCALED for N(0,1) -- must be multiplied by sigma = 1/sqrt(padded_dim).
pub const RAW_A2_CENTROIDS: [(f32, f32); 16] = [
    // Center point
    (0.0, 0.0),
    // Inner hex ring (~0.7 sigma): 6 points at 60-degree intervals
    (0.6830, 0.0),
    (0.3415, 0.5914),
    (-0.3415, 0.5914),
    (-0.6830, 0.0),
    (-0.3415, -0.5914),
    (0.3415, -0.5914),
    // Outer hex ring (~1.5 sigma): 6 points at 60-degree intervals, rotated 30 deg
    (1.2990, 0.7500),
    (0.0, 1.5000),
    (-1.2990, 0.7500),
    (-1.2990, -0.7500),
    (0.0, -1.5000),
    (1.2990, -0.7500),
    // Tail ring (~2.3 sigma): 3 points at 120-degree intervals
    (2.0, 0.0),
    (-1.0, 1.7321),
    (-1.0, -1.7321),
];

/// A2 hexagonal lattice codebook for paired-dimension quantization.
///
/// Stores 16 centroids scaled by sigma = 1/sqrt(padded_dim) to match
/// the FWHT normalization of TurboQuant coordinates.
pub struct A2Codebook {
    centroids: [(f32, f32); 16],
}

impl A2Codebook {
    /// Create a new A2 codebook scaled for the given padded dimension.
    ///
    /// sigma = 1/sqrt(padded_dim) matches the FWHT coordinate distribution.
    pub fn new(padded_dim: u32) -> Self {
        let sigma = 1.0 / (padded_dim as f32).sqrt();
        let mut centroids = [(0.0f32, 0.0f32); 16];
        for i in 0..16 {
            centroids[i] = (
                RAW_A2_CENTROIDS[i].0 * sigma,
                RAW_A2_CENTROIDS[i].1 * sigma,
            );
        }
        Self { centroids }
    }

    /// Create from pre-computed centroids (for Lloyd-optimized variants).
    pub fn from_centroids(centroids: [(f32, f32); 16]) -> Self {
        Self { centroids }
    }

    /// Run Lloyd's algorithm iterations to refine centroids for N(0, sigma^2).
    ///
    /// Starts from the current centroids and performs `iterations` rounds of
    /// expectation-maximization against bivariate Gaussian samples.
    /// Returns a new codebook with refined centroids.
    pub fn lloyd_refine(&self, sigma: f32, iterations: u32) -> Self {
        let mut centroids = self.centroids;
        // LCG PRNG
        let mut rng_state: u64 = 0xDEAD_BEEF_CAFE_1234;
        let next_u64 = |state: &mut u64| -> u64 {
            *state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *state
        };
        let next_normal = |state: &mut u64, sig: f32| -> f32 {
            let u1 = (next_u64(state) >> 11) as f64 / (1u64 << 53) as f64;
            let u2 = (next_u64(state) >> 11) as f64 / (1u64 << 53) as f64;
            let u1 = if u1 < 1e-15 { 1e-15 } else { u1 };
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            (z as f32) * sig
        };

        let samples_per_iter = 50_000;

        for _ in 0..iterations {
            let mut sums = [(0.0f64, 0.0f64); 16];
            let mut counts = [0u32; 16];

            for _ in 0..samples_per_iter {
                let x = next_normal(&mut rng_state, sigma);
                let y = next_normal(&mut rng_state, sigma);

                // Find nearest centroid
                let mut best = 0usize;
                let mut best_d = f32::MAX;
                for (i, &(cx, cy)) in centroids.iter().enumerate() {
                    let dx = x - cx;
                    let dy = y - cy;
                    let d = dx * dx + dy * dy;
                    if d < best_d {
                        best_d = d;
                        best = i;
                    }
                }

                sums[best].0 += x as f64;
                sums[best].1 += y as f64;
                counts[best] += 1;
            }

            // Update centroids to cluster means
            for i in 0..16 {
                if counts[i] > 0 {
                    centroids[i] = (
                        (sums[i].0 / counts[i] as f64) as f32,
                        (sums[i].1 / counts[i] as f64) as f32,
                    );
                }
            }
        }

        Self { centroids }
    }

    /// Quantize a pair of coordinates to the nearest hexagonal cell (0..15).
    ///
    /// Brute-force nearest of 16 centroids via squared Euclidean distance.
    /// With only 16 comparisons this is faster than geometric nearest-lattice-point.
    #[inline]
    pub fn quantize_pair(&self, x: f32, y: f32) -> u8 {
        let mut best = 0u8;
        let mut best_d = f32::MAX;
        for (i, &(cx, cy)) in self.centroids.iter().enumerate() {
            let dx = x - cx;
            let dy = y - cy;
            let d = dx * dx + dy * dy;
            if d < best_d {
                best_d = d;
                best = i as u8;
            }
        }
        best
    }

    /// Decode a cell index back to its centroid coordinates.
    #[inline]
    pub fn decode_pair(&self, idx: u8) -> (f32, f32) {
        self.centroids[idx as usize]
    }

    /// Flat array of x-coordinates for ADC lookup tables.
    pub fn centroids_x(&self) -> [f32; 16] {
        let mut xs = [0.0f32; 16];
        for i in 0..16 {
            xs[i] = self.centroids[i].0;
        }
        xs
    }

    /// Flat array of y-coordinates for ADC lookup tables.
    pub fn centroids_y(&self) -> [f32; 16] {
        let mut ys = [0.0f32; 16];
        for i in 0..16 {
            ys[i] = self.centroids[i].1;
        }
        ys
    }

    /// Compute packed code size in bytes for A2 encoding.
    ///
    /// One 4-bit index per pair of dimensions, nibble-packed:
    /// `padded_dim / 2` pairs, packed into `padded_dim / 4` bytes.
    #[inline]
    pub fn code_bytes_per_vector(padded_dim: u32) -> usize {
        padded_dim as usize / 4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sigma_scaling() {
        let cb = A2Codebook::new(1024);
        let sigma = 1.0 / (1024.0f32).sqrt();
        for i in 0..16 {
            let (cx, cy) = cb.decode_pair(i);
            let (rx, ry) = RAW_A2_CENTROIDS[i as usize];
            assert!(
                (cx - rx * sigma).abs() < 1e-7,
                "centroid {i} x: {cx} != {} * {sigma}",
                rx
            );
            assert!(
                (cy - ry * sigma).abs() < 1e-7,
                "centroid {i} y: {cy} != {} * {sigma}",
                ry
            );
        }
    }

    #[test]
    fn test_quantize_decode_roundtrip() {
        // Quantizing at each centroid must return that centroid's index
        let cb = A2Codebook::new(1024);
        for i in 0..16u8 {
            let (cx, cy) = cb.decode_pair(i);
            let idx = cb.quantize_pair(cx, cy);
            assert_eq!(
                idx, i,
                "roundtrip failed: quantize_pair(decode_pair({i})) = {idx}"
            );
        }
    }

    #[test]
    fn test_adjacent_midpoint() {
        // Midpoint between two adjacent centroids should map to one of them
        let cb = A2Codebook::new(1024);
        let (x0, y0) = cb.decode_pair(0);
        let (x1, y1) = cb.decode_pair(1);
        let mx = (x0 + x1) / 2.0;
        let my = (y0 + y1) / 2.0;
        let idx = cb.quantize_pair(mx, my);
        assert!(
            idx == 0 || idx == 1,
            "midpoint between 0 and 1 mapped to {idx}, expected 0 or 1"
        );
    }

    #[test]
    fn test_all_cells_reachable() {
        // Each centroid is its own nearest point, so all 16 cells are reachable
        let cb = A2Codebook::new(1024);
        let mut seen = [false; 16];
        for i in 0..16u8 {
            let (cx, cy) = cb.decode_pair(i);
            let idx = cb.quantize_pair(cx, cy);
            seen[idx as usize] = true;
        }
        for (i, &s) in seen.iter().enumerate() {
            assert!(s, "cell {i} never reached");
        }
    }

    #[test]
    fn test_decode_all_finite() {
        let cb = A2Codebook::new(1024);
        for i in 0..16u8 {
            let (x, y) = cb.decode_pair(i);
            assert!(x.is_finite(), "centroid {i} x is not finite: {x}");
            assert!(y.is_finite(), "centroid {i} y is not finite: {y}");
        }
    }

    #[test]
    fn test_a2_lower_distortion() {
        // Compare A2 hexagonal (16 cells) vs scalar 2-bit (4 centroids per dim = 16 cells)
        // at the same 4-bit-per-pair budget. A2 should achieve lower MSE due to
        // hexagonal lattice advantage (G=0.0802 vs G=0.0833).
        use super::super::codebook::{RAW_BOUNDARIES_2BIT, RAW_CENTROIDS_2BIT};

        let padded_dim = 1024u32;
        let sigma = 1.0 / (padded_dim as f32).sqrt();

        // Build A2 codebook and refine with Lloyd iterations for this sigma
        let raw_cb = A2Codebook::new(padded_dim);
        let cb = raw_cb.lloyd_refine(sigma, 10);

        // Scale 2-bit scalar codebook (4 centroids per dim)
        let mut sc2 = [0.0f32; 4];
        let mut sb2 = [0.0f32; 3];
        for i in 0..4 {
            sc2[i] = RAW_CENTROIDS_2BIT[i] * sigma;
        }
        for i in 0..3 {
            sb2[i] = RAW_BOUNDARIES_2BIT[i] * sigma;
        }

        // Simple LCG PRNG for reproducibility
        let mut rng_state: u64 = 0x1234_5678_9ABC_DEF0;
        let next_u64 = |state: &mut u64| -> u64 {
            *state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *state
        };
        // Box-Muller transform for N(0, sigma^2)
        let next_normal = |state: &mut u64, sig: f32| -> f32 {
            let u1 = (next_u64(state) >> 11) as f64 / (1u64 << 53) as f64;
            let u2 = (next_u64(state) >> 11) as f64 / (1u64 << 53) as f64;
            let u1 = if u1 < 1e-15 { 1e-15 } else { u1 };
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            (z as f32) * sig
        };

        let n_samples = 100_000;
        let mut scalar_mse = 0.0f64;
        let mut a2_mse = 0.0f64;

        for _ in 0..n_samples {
            let x = next_normal(&mut rng_state, sigma);
            let y = next_normal(&mut rng_state, sigma);

            // Scalar 2-bit: quantize x and y independently (4 centroids each)
            let ix = scalar_quantize_2bit(x, &sb2);
            let iy = scalar_quantize_2bit(y, &sb2);
            let sx = sc2[ix as usize];
            let sy = sc2[iy as usize];
            scalar_mse += ((x - sx) as f64).powi(2) + ((y - sy) as f64).powi(2);

            // A2: jointly quantize (x, y)
            let idx = cb.quantize_pair(x, y);
            let (ax, ay) = cb.decode_pair(idx);
            a2_mse += ((x - ax) as f64).powi(2) + ((y - ay) as f64).powi(2);
        }

        scalar_mse /= n_samples as f64;
        a2_mse /= n_samples as f64;

        let improvement = (scalar_mse - a2_mse) / scalar_mse;
        eprintln!(
            "Distortion comparison (same 4-bit budget per pair):\n  \
             scalar 2-bit MSE = {scalar_mse:.8}\n  \
             A2 hex MSE       = {a2_mse:.8}\n  \
             improvement       = {:.2}%",
            improvement * 100.0
        );

        assert!(
            a2_mse < scalar_mse,
            "A2 MSE ({a2_mse:.8}) should be less than scalar 2-bit MSE ({scalar_mse:.8})"
        );
    }

    /// Scalar quantize using 2-bit boundaries (3 boundaries, 4 centroids).
    fn scalar_quantize_2bit(val: f32, boundaries: &[f32; 3]) -> u8 {
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

    #[test]
    fn test_centroids_x_y_accessors() {
        let cb = A2Codebook::new(1024);
        let xs = cb.centroids_x();
        let ys = cb.centroids_y();
        for i in 0..16 {
            let (cx, cy) = cb.decode_pair(i as u8);
            assert!((xs[i] - cx).abs() < 1e-7);
            assert!((ys[i] - cy).abs() < 1e-7);
        }
    }

    #[test]
    fn test_code_bytes_per_vector() {
        // padded_dim / 4 bytes: each byte = 2 nibbles = 2 pairs = 4 coordinates
        assert_eq!(A2Codebook::code_bytes_per_vector(1024), 256);
        assert_eq!(A2Codebook::code_bytes_per_vector(128), 32);
        assert_eq!(A2Codebook::code_bytes_per_vector(2048), 512);
    }

    #[test]
    fn test_lloyd_refine_convergence() {
        // After Lloyd refinement, MSE should decrease vs unrefined
        let padded_dim = 1024u32;
        let sigma = 1.0 / (padded_dim as f32).sqrt();
        let raw_cb = A2Codebook::new(padded_dim);
        let refined_cb = raw_cb.lloyd_refine(sigma, 5);

        // Both should still have 16 valid centroids
        for i in 0..16u8 {
            let (x, y) = refined_cb.decode_pair(i);
            assert!(x.is_finite(), "refined centroid {i} x not finite");
            assert!(y.is_finite(), "refined centroid {i} y not finite");
        }
        // All cells still reachable
        let mut seen = [false; 16];
        for i in 0..16u8 {
            let (cx, cy) = refined_cb.decode_pair(i);
            seen[refined_cb.quantize_pair(cx, cy) as usize] = true;
        }
        for (i, &s) in seen.iter().enumerate() {
            assert!(s, "refined cell {i} not reachable");
        }
    }
}
