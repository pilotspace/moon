//! Distance computation — OnceLock dispatch table with scalar/SIMD kernels.
//!
//! Call [`init()`] once at startup (before any search operation). Then use
//! [`table()`] to get the static `DistanceTable` with the best available
//! kernel for the current CPU.

pub mod fastscan;
pub mod scalar;

#[cfg(target_arch = "x86_64")]
pub mod avx2;
#[cfg(target_arch = "x86_64")]
pub mod avx512;
#[cfg(target_arch = "aarch64")]
pub mod neon;

use std::sync::OnceLock;

/// Static dispatch table for distance kernels.
///
/// Each field is a function pointer to the best available implementation
/// (AVX-512 > AVX2+FMA > NEON > scalar) selected at init time.
pub struct DistanceTable {
    /// Squared L2 distance for f32 vectors.
    pub l2_f32: fn(&[f32], &[f32]) -> f32,
    /// Squared L2 distance for i8 vectors (accumulates in i32).
    pub l2_i8: fn(&[i8], &[i8]) -> i32,
    /// Dot product for f32 vectors.
    pub dot_f32: fn(&[f32], &[f32]) -> f32,
    /// Cosine distance for f32 vectors (1 - similarity).
    pub cosine_f32: fn(&[f32], &[f32]) -> f32,
    /// TurboQuant asymmetric L2: (rotated_query, nibble_packed_code, norm, centroids) -> distance.
    /// Centroids must be dimension-scaled (from CollectionMetadata.codebook_16()).
    /// All tiers use scalar ADC for now; AVX2/AVX-512 VPERMPS ADC is Phase 61+ work.
    pub tq_l2: fn(&[f32], &[u8], f32, &[f32; 16]) -> f32,
}

static DISTANCE_TABLE: OnceLock<DistanceTable> = OnceLock::new();

/// Initialize the distance dispatch table.
///
/// Detects CPU features at runtime and selects the fastest kernel tier:
/// AVX-512 > AVX2+FMA > NEON > scalar.
///
/// Safe to call multiple times (OnceLock guarantees single initialization).
///
/// Must be called before any call to [`table()`].
pub fn init() {
    // Initialize FWHT dispatch alongside distance dispatch.
    crate::vector::turbo_quant::fwht::init_fwht();
    // Initialize FastScan dispatch (AVX2 VPSHUFB or scalar fallback).
    fastscan::init_fastscan();

    DISTANCE_TABLE.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
                return DistanceTable {
                    l2_f32: |a, b| {
                        // SAFETY: AVX-512F verified by is_x86_feature_detected! above.
                        unsafe { avx512::l2_f32(a, b) }
                    },
                    l2_i8: |a, b| {
                        // SAFETY: AVX-512F+BW verified by is_x86_feature_detected! above.
                        unsafe { avx512::l2_i8_vnni(a, b) }
                    },
                    dot_f32: |a, b| {
                        // SAFETY: AVX-512F verified by is_x86_feature_detected! above.
                        unsafe { avx512::dot_f32(a, b) }
                    },
                    cosine_f32: |a, b| {
                        // SAFETY: AVX-512F verified by is_x86_feature_detected! above.
                        unsafe { avx512::cosine_f32(a, b) }
                    },
                    tq_l2: crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled,
                };
            }
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return DistanceTable {
                    l2_f32: |a, b| {
                        // SAFETY: AVX2+FMA verified by is_x86_feature_detected! above.
                        unsafe { avx2::l2_f32(a, b) }
                    },
                    l2_i8: |a, b| {
                        // SAFETY: AVX2+FMA verified by is_x86_feature_detected! above.
                        unsafe { avx2::l2_i8(a, b) }
                    },
                    dot_f32: |a, b| {
                        // SAFETY: AVX2+FMA verified by is_x86_feature_detected! above.
                        unsafe { avx2::dot_f32(a, b) }
                    },
                    cosine_f32: |a, b| {
                        // SAFETY: AVX2+FMA verified by is_x86_feature_detected! above.
                        unsafe { avx2::cosine_f32(a, b) }
                    },
                    tq_l2: crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled,
                };
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            // NEON is baseline on all AArch64 CPUs — always available.
            return DistanceTable {
                l2_f32: |a, b| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::l2_f32(a, b) }
                },
                // Use scalar l2_i8: the compiler auto-vectorizes with SDOT/SADALP
                // which is 3.5x faster than our explicit vmovl+vmlal NEON chain.
                // The explicit NEON l2_i8 widens i8->i16->i32 (6 instructions per 16
                // elements) while LLVM's auto-vectorization uses SADALP (2 instructions).
                l2_i8: scalar::l2_i8,
                dot_f32: |a, b| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::dot_f32(a, b) }
                },
                cosine_f32: |a, b| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::cosine_f32(a, b) }
                },
                tq_l2: crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled,
            };
        }

        // Scalar fallback — works on every platform.
        #[allow(unreachable_code)]
        DistanceTable {
            l2_f32: scalar::l2_f32,
            l2_i8: scalar::l2_i8,
            dot_f32: scalar::dot_f32,
            cosine_f32: scalar::cosine_f32,
            tq_l2: crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled,
        }
    });
}

/// Get the static distance dispatch table.
///
/// Returns the table initialized by [`init()`]. This is a single pointer load
/// followed by a direct function call — at most 1 cache miss per call site.
///
/// # Safety contract
/// Caller must ensure [`init()`] has been called before the first call to `table()`.
/// In practice, `init()` is called from `main()` at startup.
#[inline(always)]
pub fn table() -> &'static DistanceTable {
    // SAFETY: init() is called from main() at startup before any search operation.
    // The OnceLock is guaranteed to be initialized by the time any search
    // path reaches this function. Using unwrap_unchecked avoids a branch
    // on the hot path.
    debug_assert!(
        DISTANCE_TABLE.get().is_some(),
        "distance::init() was not called before table()"
    );
    unsafe { DISTANCE_TABLE.get().unwrap_unchecked() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_table_init() {
        init();
        let t = table();

        // Verify all function pointers work correctly
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, 5.0, 6.0];
        assert_eq!((t.l2_f32)(&a, &b), 27.0);

        let ai = [1i8, 2, 3];
        let bi = [4i8, 5, 6];
        assert_eq!((t.l2_i8)(&ai, &bi), 27);

        assert_eq!((t.dot_f32)(&a, &b), 32.0);

        let same = [1.0f32, 0.0, 0.0];
        let dist = (t.cosine_f32)(&same, &same);
        assert!(dist.abs() < 1e-6);

        // Quick TQ ADC smoke test — use dummy centroids for basic sanity check
        let q = [0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
        let code = [0x10, 0x32, 0x54, 0x76]; // nibble-packed indices 0-7
        let centroids = crate::vector::turbo_quant::codebook::scaled_centroids(8);
        let dist = (t.tq_l2)(&q, &code, 1.0, &centroids);
        assert!(dist >= 0.0, "tq_l2 should be non-negative, got {dist}");
    }

    #[test]
    fn test_init_idempotent() {
        init();
        init(); // second call should be a no-op
        let t = table();
        let a = [1.0f32, 0.0];
        let b = [0.0f32, 1.0];
        assert_eq!((t.dot_f32)(&a, &b), 0.0);
    }

    #[test]
    fn test_dispatch_selects_simd() {
        init();
        let t = table();

        // Verify the dispatch table produces correct results for a known input.
        // On x86_64 with AVX2+FMA: SIMD kernels are active.
        // On aarch64: NEON kernels are active.
        // Either way, results must match scalar.
        let a = [1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b = [8.0f32, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];

        let expected_l2 = scalar::l2_f32(&a, &b);
        let expected_dot = scalar::dot_f32(&a, &b);
        let expected_cosine = scalar::cosine_f32(&a, &b);

        assert_eq!((t.l2_f32)(&a, &b), expected_l2);
        assert_eq!((t.dot_f32)(&a, &b), expected_dot);

        let cosine_diff = ((t.cosine_f32)(&a, &b) - expected_cosine).abs();
        assert!(cosine_diff < 1e-6, "cosine mismatch: {cosine_diff}");

        let ai = [1i8, 2, 3, 4, 5, 6, 7, 8];
        let bi = [8i8, 7, 6, 5, 4, 3, 2, 1];
        let expected_i8 = scalar::l2_i8(&ai, &bi);
        assert_eq!((t.l2_i8)(&ai, &bi), expected_i8);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Deterministic f32 vector via LCG PRNG, values in [-1.0, 1.0].
    fn deterministic_f32(dim: usize, seed: u64) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed as u32;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    /// Deterministic i8 vector via LCG PRNG, values in [-128, 127].
    fn deterministic_i8(dim: usize, seed: u64) -> Vec<i8> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed as u32;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s >> 24) as i8);
        }
        v
    }

    /// Relative tolerance check for f32 values.
    fn approx_eq_f32(a: f32, b: f32, rel_tol: f32) -> bool {
        (a - b).abs() <= rel_tol * a.abs().max(b.abs()).max(1e-6)
    }

    const TEST_DIMS: &[usize] = &[
        1, 2, 3, 7, 8, 15, 16, 31, 32, 63, 64, 100, 128, 256, 384, 768, 1024,
    ];

    #[test]
    fn test_simd_matches_scalar_l2_f32() {
        init();
        let t = table();
        for &dim in TEST_DIMS {
            let a = deterministic_f32(dim, 42);
            let b = deterministic_f32(dim, 99);
            let scalar_result = scalar::l2_f32(&a, &b);
            let dispatch_result = (t.l2_f32)(&a, &b);
            assert!(
                approx_eq_f32(scalar_result, dispatch_result, 1e-4),
                "l2_f32 mismatch at dim={dim}: scalar={scalar_result}, dispatch={dispatch_result}"
            );
        }
    }

    #[test]
    fn test_simd_matches_scalar_l2_i8() {
        init();
        let t = table();
        for &dim in TEST_DIMS {
            let a = deterministic_i8(dim, 42);
            let b = deterministic_i8(dim, 99);
            assert_eq!(
                scalar::l2_i8(&a, &b),
                (t.l2_i8)(&a, &b),
                "l2_i8 mismatch at dim={dim}"
            );
        }
    }

    #[test]
    fn test_simd_matches_scalar_dot_f32() {
        init();
        let t = table();
        for &dim in TEST_DIMS {
            let a = deterministic_f32(dim, 42);
            let b = deterministic_f32(dim, 99);
            let scalar_result = scalar::dot_f32(&a, &b);
            let dispatch_result = (t.dot_f32)(&a, &b);
            assert!(
                approx_eq_f32(scalar_result, dispatch_result, 1e-4),
                "dot_f32 mismatch at dim={dim}: scalar={scalar_result}, dispatch={dispatch_result}"
            );
        }
    }

    #[test]
    fn test_simd_matches_scalar_cosine_f32() {
        init();
        let t = table();
        for &dim in TEST_DIMS {
            let a = deterministic_f32(dim, 42);
            let b = deterministic_f32(dim, 99);
            let scalar_result = scalar::cosine_f32(&a, &b);
            let dispatch_result = (t.cosine_f32)(&a, &b);
            assert!(
                approx_eq_f32(scalar_result, dispatch_result, 1e-4),
                "cosine_f32 mismatch at dim={dim}: scalar={scalar_result}, dispatch={dispatch_result}"
            );
        }
    }

    #[test]
    fn test_identical_vectors_l2() {
        init();
        let t = table();
        for &dim in &[1, 768, 1024] {
            let a = deterministic_f32(dim, 42);
            let scalar_result = scalar::l2_f32(&a, &a);
            let dispatch_result = (t.l2_f32)(&a, &a);
            assert_eq!(
                scalar_result, 0.0,
                "scalar l2 of identical vectors != 0 at dim={dim}"
            );
            assert_eq!(
                dispatch_result, 0.0,
                "dispatch l2 of identical vectors != 0 at dim={dim}"
            );
        }
    }

    #[test]
    fn test_zero_vector_cosine() {
        init();
        let t = table();
        let zero = vec![0.0f32; 128];
        let nonzero = deterministic_f32(128, 42);
        // Zero vector should return 1.0 (max distance) for both scalar and dispatch
        assert_eq!(scalar::cosine_f32(&zero, &nonzero), 1.0);
        assert_eq!((t.cosine_f32)(&zero, &nonzero), 1.0);
        assert_eq!(scalar::cosine_f32(&nonzero, &zero), 1.0);
        assert_eq!((t.cosine_f32)(&nonzero, &zero), 1.0);
    }

    #[test]
    fn test_single_element() {
        init();
        let t = table();
        let a = [0.5f32];
        let b = [0.8f32];

        // L2: (0.5 - 0.8)^2 = 0.09
        let l2_s = scalar::l2_f32(&a, &b);
        let l2_d = (t.l2_f32)(&a, &b);
        assert!(
            approx_eq_f32(l2_s, l2_d, 1e-6),
            "single-element l2_f32 mismatch"
        );

        // Dot: 0.5 * 0.8 = 0.4
        let dot_s = scalar::dot_f32(&a, &b);
        let dot_d = (t.dot_f32)(&a, &b);
        assert!(
            approx_eq_f32(dot_s, dot_d, 1e-6),
            "single-element dot_f32 mismatch"
        );

        // Cosine: 1 - (0.4 / (0.5 * 0.8)) = 0.0
        let cos_s = scalar::cosine_f32(&a, &b);
        let cos_d = (t.cosine_f32)(&a, &b);
        assert!(
            approx_eq_f32(cos_s, cos_d, 1e-6),
            "single-element cosine_f32 mismatch"
        );

        // i8 single element
        let ai = [42i8];
        let bi = [-10i8];
        assert_eq!(scalar::l2_i8(&ai, &bi), (t.l2_i8)(&ai, &bi));
    }
}
