//! Distance computation — OnceLock dispatch table with scalar/SIMD kernels.
//!
//! Call [`init()`] once at startup (before any search operation). Then use
//! [`table()`] to get the static `DistanceTable` with the best available
//! kernel for the current CPU.

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
                l2_i8: |a, b| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::l2_i8(a, b) }
                },
                dot_f32: |a, b| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::dot_f32(a, b) }
                },
                cosine_f32: |a, b| {
                    // SAFETY: NEON is guaranteed on AArch64.
                    unsafe { neon::cosine_f32(a, b) }
                },
            };
        }

        // Scalar fallback — works on every platform.
        #[allow(unreachable_code)]
        DistanceTable {
            l2_f32: scalar::l2_f32,
            l2_i8: scalar::l2_i8,
            dot_f32: scalar::dot_f32,
            cosine_f32: scalar::cosine_f32,
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
    // SAFETY: init() is called at startup before any search operation.
    // The OnceLock is guaranteed to be initialized by the time any search
    // path reaches this function. Using unwrap_unchecked avoids a branch
    // on the hot path.
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
