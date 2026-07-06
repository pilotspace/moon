//! Distance computation — OnceLock dispatch table with scalar/SIMD kernels.
//!
//! Call [`init()`] once at startup (before any search operation). Then use
//! [`table()`] to get the static `DistanceTable` with the best available
//! kernel for the current CPU.

pub mod fastscan;
pub mod scalar;

#[cfg(target_arch = "x86_64")]
pub mod avx2;
#[cfg(all(target_arch = "x86_64", feature = "simd-avx512"))]
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
    /// SQ8 asymmetric-distance-code (ADC) per-candidate statistics:
    /// `(query, codes) -> (Σ(q_i·c_i), Σc_i, Σc_i²)`.
    ///
    /// This is the SIMD-accelerated inner loop of the HQ-2 fix (see
    /// `turbo_quant::sq8` module docs for the algebraic decomposition): u8
    /// codes are widened to f32 and FMA'd against the query in one pass.
    /// Combine with `turbo_quant::sq8::sq8_l2_from_stats` /
    /// `sq8_ip_from_stats` (using per-query `turbo_quant::sq8::sq8_query_stats`)
    /// to get the final ADC distance — that combine step is O(1) arithmetic,
    /// architecture-independent, and deliberately NOT part of this table.
    pub sq8_stats: fn(&[f32], &[u8]) -> (f32, f32, f32),
    /// Int8 symmetric ADC per-candidate stats (task #13): `(qi8, codes,
    /// sum_qi8) -> (Σ qi8_i·c_i, Σc_i, Σc_i²)` as exact `i64` integers.
    /// `None` means the int8 tier is unavailable (no matching SIMD kernel
    /// for this CPU, or the `MOON_SQ8_INT8_ADC=0` escape hatch is set) —
    /// callers must fall back to `sq8_stats` (f32) in that case. See
    /// `turbo_quant::sq8` module docs for the query-quantization step
    /// ([`crate::vector::turbo_quant::sq8::sq8_quantize_query_scalar`],
    /// [`crate::vector::turbo_quant::sq8::SQ8_INT8_QMAX`]) that must run
    /// once per query before calling this.
    pub sq8_i8_stats: Option<crate::vector::turbo_quant::sq8::Sq8I8StatsFn>,
    /// Squared L2 between an f32 query and an f16-encoded vector — the
    /// exact-rerank sidecar (HQ-1) decode fused with the distance loop.
    /// SIMD tiers decode 8 halves per step (NEON integer rescale / F16C
    /// `vcvtph2ps`); scalar falls back to `vector::f16::l2_sq_f16`.
    pub f16_l2: fn(&[f32], &[u16]) -> f32,
    /// Fused `(Σ q_i·x_i, Σ x_i²)` between an f32 query and an f16-encoded
    /// vector — the unit-sphere (Cosine/IP) rerank pass needs both in one
    /// decode sweep. Same tiering as [`Self::f16_l2`].
    pub f16_dot_normsq: fn(&[f32], &[u16]) -> (f32, f32),
}

/// Signature aliases for the f16 sidecar kernels (keeps branch wiring terse).
#[cfg(target_arch = "x86_64")]
type F16L2Fn = fn(&[f32], &[u16]) -> f32;
#[cfg(target_arch = "x86_64")]
type F16DotNormFn = fn(&[f32], &[u16]) -> (f32, f32);

static DISTANCE_TABLE: OnceLock<DistanceTable> = OnceLock::new();

/// Escape hatch for the int8 symmetric ADC path (task #13): `MOON_SQ8_INT8_ADC=0`
/// forces every tier back to the f32 `sq8_stats` kernel (bench/diagnostic
/// convention, like the `MOON_XSHARD_*` knobs — see CLAUDE.md env var list).
/// Cached in a `OnceLock<bool>` so the env lookup happens at most once.
fn int8_adc_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MOON_SQ8_INT8_ADC")
            .map(|v| v != "0")
            .unwrap_or(true)
    })
}

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
            // f16 sidecar kernels: F16C is a separate cpuid bit from AVX2
            // (present on virtually every AVX2 CPU, but checked explicitly —
            // and AVX2 is NOT implied by F16C+FMA: AMD Piledriver has both
            // without AVX2). Both AVX2 and AVX-512 tiers share this kernel.
            let (f16_l2, f16_dot_normsq): (F16L2Fn, F16DotNormFn) =
                if is_x86_feature_detected!("f16c")
                    && is_x86_feature_detected!("fma")
                    && is_x86_feature_detected!("avx2")
                {
                    (
                        |q, x| {
                            // SAFETY: AVX2+F16C+FMA verified by is_x86_feature_detected! above.
                            unsafe { avx2::f16_l2(q, x) }
                        },
                        |q, x| {
                            // SAFETY: AVX2+F16C+FMA verified by is_x86_feature_detected! above.
                            unsafe { avx2::f16_dot_normsq(q, x) }
                        },
                    )
                } else {
                    (
                        crate::vector::f16::l2_sq_f16 as F16L2Fn,
                        crate::vector::f16::dot_normsq_f16 as F16DotNormFn,
                    )
                };
            #[cfg(feature = "simd-avx512")]
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
                let sq8_i8_stats = if int8_adc_enabled() && is_x86_feature_detected!("avx512vnni") {
                    Some(
                        (|q, c, s| {
                            // SAFETY: AVX-512F+BW+VNNI verified by is_x86_feature_detected! above.
                            unsafe { avx512::sq8_i8_stats(q, c, s) }
                        }) as crate::vector::turbo_quant::sq8::Sq8I8StatsFn,
                    )
                } else {
                    None
                };
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
                    sq8_stats: |q, c| {
                        // SAFETY: AVX-512F verified by is_x86_feature_detected! above.
                        unsafe { avx512::sq8_stats(q, c) }
                    },
                    sq8_i8_stats,
                    f16_l2,
                    f16_dot_normsq,
                };
            }
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                let sq8_i8_stats = if int8_adc_enabled() {
                    Some(
                        (|q, c, s| {
                            // SAFETY: AVX2 verified by is_x86_feature_detected! above.
                            unsafe { avx2::sq8_i8_stats(q, c, s) }
                        }) as crate::vector::turbo_quant::sq8::Sq8I8StatsFn,
                    )
                } else {
                    None
                };
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
                    sq8_stats: |q, c| {
                        // SAFETY: AVX2+FMA verified by is_x86_feature_detected! above.
                        unsafe { avx2::sq8_stats(q, c) }
                    },
                    sq8_i8_stats,
                    f16_l2,
                    f16_dot_normsq,
                };
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            // NEON is baseline on all AArch64 CPUs — always available.
            let sq8_i8_stats = if int8_adc_enabled() {
                Some(
                    (|q, c, s| {
                        // SAFETY: NEON is guaranteed on AArch64.
                        unsafe { neon::sq8_i8_stats(q, c, s) }
                    }) as crate::vector::turbo_quant::sq8::Sq8I8StatsFn,
                )
            } else {
                None
            };
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
                sq8_stats: |q, c| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::sq8_stats(q, c) }
                },
                sq8_i8_stats,
                f16_l2: |q, x| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::f16_l2(q, x) }
                },
                f16_dot_normsq: |q, x| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::f16_dot_normsq(q, x) }
                },
            };
        }

        // Scalar fallback — works on every platform. No int8 SIMD tier
        // exists for pure-scalar builds (no proven throughput win without
        // SIMD widening); sq8_i8_stats is None, callers use sq8_stats (f32).
        #[allow(unreachable_code)]
        DistanceTable {
            l2_f32: scalar::l2_f32,
            l2_i8: scalar::l2_i8,
            dot_f32: scalar::dot_f32,
            cosine_f32: scalar::cosine_f32,
            tq_l2: crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled,
            sq8_stats: crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar,
            sq8_i8_stats: None,
            f16_l2: crate::vector::f16::l2_sq_f16,
            f16_dot_normsq: crate::vector::f16::dot_normsq_f16,
        }
    });
}

/// Get the static distance dispatch table.
///
/// Returns the table initialized by [`init()`]. This is a single pointer load
/// followed by a direct function call — at most 1 cache miss per call site.
///
/// Auto-initializes on first use if [`init()`] was not called explicitly.
/// After the first call the hot path is two atomic loads (both always succeed).
#[inline(always)]
pub fn table() -> &'static DistanceTable {
    if DISTANCE_TABLE.get().is_none() {
        init();
    }
    // After init(), DISTANCE_TABLE is guaranteed to be set.
    DISTANCE_TABLE
        .get()
        .expect("distance table initialized by init()")
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

        // SQ8 ADC stats smoke test (HQ-2): dot=1*10+2*20+3*30=140, sum_c=60, sumsq_c=1400.
        let codes: [u8; 3] = [10, 20, 30];
        let (dot, sum_c, sumsq_c) = (t.sq8_stats)(&a[..3], &codes);
        assert!((dot - 140.0).abs() < 1e-4, "sq8_stats dot={dot}");
        assert!((sum_c - 60.0).abs() < 1e-4, "sq8_stats sum_c={sum_c}");
        assert!(
            (sumsq_c - 1400.0).abs() < 1e-4,
            "sq8_stats sumsq_c={sumsq_c}"
        );
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

    /// Deterministic u8 codes via LCG PRNG, full 0..=255 range.
    fn deterministic_u8(dim: usize, seed: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed as u32;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s >> 24) as u8);
        }
        v
    }

    /// HQ-2: the SIMD-dispatched `sq8_stats` kernel must match the scalar
    /// reference (`turbo_quant::sq8::sq8_candidate_stats_scalar`) at every
    /// dimension, including SIMD-width tail remainders. On x86_64 this
    /// exercises whichever tier `is_x86_feature_detected!` selected at
    /// `init()` time (AVX-512 > AVX2+FMA); on aarch64, NEON (always
    /// available); elsewhere, the scalar fallback trivially matches itself.
    #[test]
    fn test_simd_matches_scalar_sq8_stats() {
        use crate::vector::turbo_quant::sq8::sq8_candidate_stats_scalar;

        init();
        let t = table();
        for &dim in TEST_DIMS {
            let q = deterministic_f32(dim, 42);
            let codes = deterministic_u8(dim, 99);
            let expected = sq8_candidate_stats_scalar(&q, &codes);
            let got = (t.sq8_stats)(&q, &codes);
            assert!(
                approx_eq_f32(expected.0, got.0, 1e-3),
                "sq8_stats dot mismatch at dim={dim}: scalar={:?} dispatch={:?}",
                expected,
                got
            );
            assert!(
                approx_eq_f32(expected.1, got.1, 1e-3),
                "sq8_stats sum_c mismatch at dim={dim}: scalar={:?} dispatch={:?}",
                expected,
                got
            );
            assert!(
                approx_eq_f32(expected.2, got.2, 1e-3),
                "sq8_stats sumsq_c mismatch at dim={dim}: scalar={:?} dispatch={:?}",
                expected,
                got
            );
        }
    }

    /// End-to-end: dispatched `sq8_stats` combined via
    /// `sq8_l2_from_stats`/`sq8_ip_from_stats` must reproduce the original
    /// naive per-element `sq8_l2_adc`/`sq8_ip_adc` ADC (same math,
    /// reassociated for vectorization).
    #[test]
    fn test_sq8_dispatched_adc_matches_naive() {
        use crate::vector::turbo_quant::sq8::{
            sq8_ip_adc, sq8_ip_from_stats, sq8_l2_adc, sq8_l2_from_stats, sq8_query_stats,
        };

        init();
        let t = table();
        for &dim in TEST_DIMS {
            let q = deterministic_f32(dim, 7);
            let codes = deterministic_u8(dim, 13);
            let min = -0.37f32;
            let scale = 0.0123f32;

            let naive_l2 = sq8_l2_adc(&q, &codes, min, scale);
            let (q_sum, q_sumsq) = sq8_query_stats(&q);
            let (dot_qc, sum_c, sumsq_c) = (t.sq8_stats)(&q, &codes);
            let fast_l2 =
                sq8_l2_from_stats(dim, min, scale, q_sum, q_sumsq, dot_qc, sum_c, sumsq_c);
            assert!(
                approx_eq_f32(naive_l2, fast_l2, 1e-3),
                "l2 mismatch at dim={dim}: naive={naive_l2} dispatched={fast_l2}"
            );

            let naive_ip = sq8_ip_adc(&q, &codes, min, scale);
            let fast_ip = sq8_ip_from_stats(min, scale, q_sum, dot_qc);
            assert!(
                approx_eq_f32(naive_ip, fast_ip, 1e-3),
                "ip mismatch at dim={dim}: naive={naive_ip} dispatched={fast_ip}"
            );
        }
    }

    /// Task #13: if this CPU/build has an int8 SIMD tier installed
    /// (`sq8_i8_stats.is_some()`), it must match the scalar i64 oracle
    /// exactly — same bit-for-bit contract as `sq8_stats` above, but exact
    /// integers instead of float tolerance. On this dev host that means the
    /// NEON tier; on a CPU/build with the tier unavailable (or
    /// `MOON_SQ8_INT8_ADC=0`) the table has `None` and the test is a no-op
    /// (asserting absence is not meaningful — the whole point of `None` is
    /// "fall back to sq8_stats", verified by the call-site wiring instead).
    #[test]
    fn test_simd_matches_scalar_sq8_i8_stats() {
        use crate::vector::turbo_quant::sq8::{
            SQ8_INT8_QMAX, sq8_candidate_stats_i8_scalar, sq8_quantize_query_scalar,
        };

        init();
        let t = table();
        let Some(i8_stats_fn) = t.sq8_i8_stats else {
            return;
        };
        for &dim in TEST_DIMS {
            let q = deterministic_f32(dim, 42);
            let codes = deterministic_u8(dim, 99);
            let mut qi8 = vec![0i8; dim];
            let (_q_scale, sum_qi8) = sq8_quantize_query_scalar(&q, SQ8_INT8_QMAX, &mut qi8);
            let expected = sq8_candidate_stats_i8_scalar(&qi8, &codes, sum_qi8);
            let got = i8_stats_fn(&qi8, &codes, sum_qi8);
            assert_eq!(
                got, expected,
                "sq8_i8_stats mismatch at dim={dim}: scalar={expected:?} dispatch={got:?}"
            );
        }
    }

    /// Task #13: `MOON_SQ8_INT8_ADC` escape hatch — this test only asserts
    /// the env var parses without panicking and is idempotent across calls
    /// (the table itself is a `OnceLock` initialized once per process, so
    /// this test cannot flip live dispatch — that is exercised manually /
    /// by CI running the suite once with the var set, see CLAUDE.md).
    #[test]
    fn test_int8_adc_enabled_reads_env_without_panicking() {
        let _ = super::int8_adc_enabled();
        let _ = super::int8_adc_enabled();
    }

    /// Encode a deterministic f32 vector as f16 bits (rerank sidecar layout).
    fn deterministic_f16(dim: usize, seed: u64) -> Vec<u16> {
        let mut out = Vec::new();
        crate::vector::f16::encode_f16_slice(&deterministic_f32(dim, seed), &mut out);
        out
    }

    #[test]
    fn test_dispatched_f16_kernels_match_scalar() {
        init();
        let t = table();
        for &dim in TEST_DIMS {
            let q = deterministic_f32(dim, 21);
            let x16 = deterministic_f16(dim, 42);

            let scalar_l2 = crate::vector::f16::l2_sq_f16(&q, &x16);
            let fast_l2 = (t.f16_l2)(&q, &x16);
            assert!(
                approx_eq_f32(scalar_l2, fast_l2, 1e-4),
                "f16_l2 mismatch at dim={dim}: scalar={scalar_l2} dispatched={fast_l2}"
            );

            let (sd, sn) = crate::vector::f16::dot_normsq_f16(&q, &x16);
            let (fd, fn_) = (t.f16_dot_normsq)(&q, &x16);
            assert!(
                approx_eq_f32(sd, fd, 1e-4),
                "f16_dot mismatch at dim={dim}: scalar={sd} dispatched={fd}"
            );
            assert!(
                approx_eq_f32(sn, fn_, 1e-4),
                "f16_normsq mismatch at dim={dim}: scalar={sn} dispatched={fn_}"
            );
        }
    }

    #[test]
    fn test_dispatched_f16_kernels_subnormal_and_special() {
        use crate::vector::f16::f32_to_f16;
        init();
        let t = table();

        // Subnormal-heavy vector: 3e-6..6e-5 land in the f16 subnormal range —
        // the integer-rescale decode must handle them exactly.
        let dim = 40;
        let q = deterministic_f32(dim, 5);
        let tiny: Vec<u16> = (0..dim)
            .map(|i| f32_to_f16(3e-6 + (i as f32) * 1.5e-6))
            .collect();
        let scalar_l2 = crate::vector::f16::l2_sq_f16(&q, &tiny);
        let fast_l2 = (t.f16_l2)(&q, &tiny);
        assert!(
            approx_eq_f32(scalar_l2, fast_l2, 1e-4),
            "subnormal f16_l2: scalar={scalar_l2} dispatched={fast_l2}"
        );

        // Infinity in the encoded vector must propagate to an infinite
        // distance / dot, exactly like the scalar reference.
        let mut with_inf = deterministic_f16(dim, 9);
        with_inf[17] = f32_to_f16(f32::INFINITY);
        assert_eq!((t.f16_l2)(&q, &with_inf), f32::INFINITY);
        let (_, xsq) = (t.f16_dot_normsq)(&q, &with_inf);
        assert_eq!(xsq, f32::INFINITY);

        // NaN must propagate as NaN.
        let mut with_nan = deterministic_f16(dim, 11);
        with_nan[3] = f32_to_f16(f32::NAN);
        assert!((t.f16_l2)(&q, &with_nan).is_nan());
    }
}
