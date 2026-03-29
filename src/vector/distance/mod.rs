//! Distance computation — OnceLock dispatch table with scalar/SIMD kernels.
//!
//! Call [`init()`] once at startup (before any search operation). Then use
//! [`table()`] to get the static `DistanceTable` with the best available
//! kernel for the current CPU.

pub mod scalar;

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
/// Detects CPU features at runtime and selects the fastest kernel tier.
/// Safe to call multiple times (OnceLock guarantees single initialization).
///
/// Must be called before any call to [`table()`].
pub fn init() {
    DISTANCE_TABLE.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                // AVX-512 kernels will be added in Plan 02.
                // Fall through to scalar for now.
            }
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                // AVX2+FMA kernels will be added in Plan 02.
                // Fall through to scalar for now.
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            // NEON kernels will be added in Plan 02.
            // Fall through to scalar for now.
        }

        // Scalar fallback — works on every platform.
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
}
