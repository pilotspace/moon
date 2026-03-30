//! Fast Walsh-Hadamard Transform (FWHT) with scalar and AVX2 kernels.
//!
//! The FWHT is a self-inverse linear transform (up to normalization).
//! For a vector of length `n` (power of 2): `FWHT(FWHT(x)) = n * x`.
//! The normalized form divides by `sqrt(n)` and is exactly self-inverse.
//!
//! Used by TurboQuant to rotate unit vectors into a distribution where
//! each coordinate is approximately i.i.d. N(0, 1/sqrt(d)), enabling
//! scalar quantization with a universal codebook.

use std::sync::OnceLock;

/// In-place unnormalized Fast Walsh-Hadamard Transform.
///
/// After this call, `data` contains the WHT coefficients scaled by `sqrt(n)`
/// relative to the normalized form. `data.len()` MUST be a power of 2.
///
/// Butterfly pattern: for each step h = 1, 2, 4, ..., n/2, process pairs
/// `(data[j], data[j+h])` as `(x+y, x-y)`.
#[inline]
pub fn fwht_scalar(data: &mut [f32]) {
    let n = data.len();
    debug_assert!(n.is_power_of_two(), "FWHT requires power-of-2 length, got {n}");
    let mut h = 1;
    while h < n {
        let mut i = 0;
        while i < n {
            for j in i..i + h {
                let x = data[j];
                let y = data[j + h];
                data[j] = x + y;
                data[j + h] = x - y;
            }
            i += h * 2;
        }
        h *= 2;
    }
}

/// Normalize FWHT output in-place by dividing by `sqrt(n)`.
#[inline]
pub fn normalize_fwht(data: &mut [f32]) {
    let scale = 1.0 / (data.len() as f32).sqrt();
    for v in data.iter_mut() {
        *v *= scale;
    }
}

/// Apply sign flips element-wise: `data[i] *= sign_flips[i]`.
///
/// `sign_flips` must contain only +1.0 or -1.0 values (materialized, not seeds).
#[inline]
pub fn apply_sign_flips(data: &mut [f32], sign_flips: &[f32]) {
    debug_assert_eq!(data.len(), sign_flips.len());
    for (d, s) in data.iter_mut().zip(sign_flips.iter()) {
        *d *= *s;
    }
}

/// Randomized normalized FWHT (scalar): apply sign flips, FWHT, normalize.
///
/// This is the full TurboQuant rotation: after this, each coordinate of a
/// unit vector follows approximately N(0, 1/sqrt(d)).
#[inline]
pub fn randomized_fwht_scalar(data: &mut [f32], sign_flips: &[f32]) {
    apply_sign_flips(data, sign_flips);
    fwht_scalar(data);
    normalize_fwht(data);
}

// ── NEON FWHT ─────────────────────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
use core::arch::aarch64::*;

/// NEON-accelerated randomized normalized FWHT.
///
/// Processes 4 butterflies per SIMD instruction for passes where h >= 4.
/// Falls back to scalar for h = 1, 2 passes (only need 1-2 element operations).
///
/// # Safety
/// Caller must ensure the CPU supports NEON (baseline on all AArch64).
/// Pointer arithmetic stays within slice bounds (guaranteed by loop structure
/// and power-of-2 invariant).
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn fwht_neon(data: &mut [f32], sign_flips: &[f32]) {
    let n = data.len();
    debug_assert!(n.is_power_of_two());
    debug_assert_eq!(data.len(), sign_flips.len());

    // SAFETY: NEON is baseline on all AArch64 CPUs. All pointer arithmetic
    // stays within `data` and `sign_flips` bounds (loop indices bounded by n,
    // which equals both slice lengths, and n is a power of 2).

    // Step 1: Apply sign flips via NEON vmulq_f32 (4 floats at a time)
    let mut i = 0;
    while i + 4 <= n {
        let d = vld1q_f32(data.as_ptr().add(i));
        let s = vld1q_f32(sign_flips.as_ptr().add(i));
        vst1q_f32(data.as_mut_ptr().add(i), vmulq_f32(d, s));
        i += 4;
    }
    // Scalar remainder for sign flips
    while i < n {
        *data.get_unchecked_mut(i) *= *sign_flips.get_unchecked(i);
        i += 1;
    }

    // Step 2: Butterfly passes
    let mut h = 1;
    while h < n {
        let mut j = 0;
        while j < n {
            let mut k = j;
            // NEON path: process 4 butterflies when h >= 4
            while k + 4 <= j + h && k + h + 4 <= n {
                let a = vld1q_f32(data.as_ptr().add(k));
                let b = vld1q_f32(data.as_ptr().add(k + h));
                vst1q_f32(data.as_mut_ptr().add(k), vaddq_f32(a, b));
                vst1q_f32(data.as_mut_ptr().add(k + h), vsubq_f32(a, b));
                k += 4;
            }
            // Scalar remainder
            while k < j + h {
                let x = *data.get_unchecked(k);
                let y = *data.get_unchecked(k + h);
                *data.get_unchecked_mut(k) = x + y;
                *data.get_unchecked_mut(k + h) = x - y;
                k += 1;
            }
            j += h * 2;
        }
        h *= 2;
    }

    // Step 3: Normalize by 1/sqrt(n)
    let scale_val = 1.0 / (n as f32).sqrt();
    let scale = vdupq_n_f32(scale_val);
    i = 0;
    while i + 4 <= n {
        let d = vld1q_f32(data.as_ptr().add(i));
        vst1q_f32(data.as_mut_ptr().add(i), vmulq_f32(d, scale));
        i += 4;
    }
    // Scalar remainder for normalization
    while i < n {
        *data.get_unchecked_mut(i) *= scale_val;
        i += 1;
    }
}

// ── AVX2 FWHT ─────────────────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

/// AVX2-accelerated randomized normalized FWHT.
///
/// Processes 8 butterflies per SIMD instruction for passes where h >= 8.
/// Falls back to scalar for the first 3 passes (h = 1, 2, 4).
///
/// # Safety
/// Caller must ensure AVX2 is available (checked via OnceLock dispatch).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn fwht_avx2(data: &mut [f32], sign_flips: &[f32]) {
    let n = data.len();
    debug_assert!(n.is_power_of_two());
    debug_assert_eq!(data.len(), sign_flips.len());

    // SAFETY: AVX2 verified by caller via is_x86_feature_detected!.
    // All pointer arithmetic stays within the bounds of `data` and `sign_flips`
    // slices (checked by loop bounds and power-of-2 invariant).

    // Step 1: Apply sign flips via SIMD multiply
    let mut i = 0;
    while i + 8 <= n {
        let d = _mm256_loadu_ps(data.as_ptr().add(i));
        let s = _mm256_loadu_ps(sign_flips.as_ptr().add(i));
        _mm256_storeu_ps(data.as_mut_ptr().add(i), _mm256_mul_ps(d, s));
        i += 8;
    }
    // Scalar remainder for sign flips
    while i < n {
        *data.get_unchecked_mut(i) *= *sign_flips.get_unchecked(i);
        i += 1;
    }

    // Step 2: Butterfly passes
    let mut h = 1;
    while h < n {
        let mut j = 0;
        while j < n {
            let mut k = j;
            // SIMD path: process 8 butterflies when h >= 8
            while k + 8 <= j + h && k + h + 8 <= n {
                let a = _mm256_loadu_ps(data.as_ptr().add(k));
                let b = _mm256_loadu_ps(data.as_ptr().add(k + h));
                _mm256_storeu_ps(data.as_mut_ptr().add(k), _mm256_add_ps(a, b));
                _mm256_storeu_ps(data.as_mut_ptr().add(k + h), _mm256_sub_ps(a, b));
                k += 8;
            }
            // Scalar remainder
            while k < j + h {
                let x = *data.get_unchecked(k);
                let y = *data.get_unchecked(k + h);
                *data.get_unchecked_mut(k) = x + y;
                *data.get_unchecked_mut(k + h) = x - y;
                k += 1;
            }
            j += h * 2;
        }
        h *= 2;
    }

    // Step 3: Normalize by 1/sqrt(n)
    let scale = _mm256_set1_ps(1.0 / (n as f32).sqrt());
    i = 0;
    while i + 8 <= n {
        let d = _mm256_loadu_ps(data.as_ptr().add(i));
        _mm256_storeu_ps(data.as_mut_ptr().add(i), _mm256_mul_ps(d, scale));
        i += 8;
    }
    // Scalar remainder for normalization
    let scale_s = 1.0 / (n as f32).sqrt();
    while i < n {
        *data.get_unchecked_mut(i) *= scale_s;
        i += 1;
    }
}

// ── OnceLock dispatch ──────────────────────────────────────────────────

/// Function pointer type for randomized normalized FWHT.
type FwhtFn = fn(&mut [f32], &[f32]);

static FWHT_FN: OnceLock<FwhtFn> = OnceLock::new();

/// Initialize the FWHT dispatch, selecting the fastest available kernel.
///
/// Safe to call multiple times (OnceLock). Must be called before [`fwht()`].
pub fn init_fwht() {
    FWHT_FN.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                return |data: &mut [f32], signs: &[f32]| {
                    // SAFETY: AVX2 availability verified by is_x86_feature_detected! above.
                    unsafe { fwht_avx2(data, signs) }
                };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            // NEON is baseline on all AArch64 CPUs — no feature detection needed.
            return |data: &mut [f32], signs: &[f32]| {
                // SAFETY: NEON is guaranteed on all AArch64 processors.
                unsafe { fwht_neon(data, signs) }
            };
        }
        #[allow(unreachable_code)]
        (randomized_fwht_scalar as FwhtFn)
    });
}

/// Dispatched randomized normalized FWHT.
///
/// Uses the fastest available kernel (AVX2 on x86_64, scalar otherwise).
/// [`init_fwht()`] must have been called before first use.
#[inline(always)]
pub fn fwht(data: &mut [f32], sign_flips: &[f32]) {
    // SAFETY: init_fwht() is called at startup before any encode/search operation.
    // The OnceLock is guaranteed to be initialized by the time any TurboQuant
    // path reaches this function.
    (unsafe { *FWHT_FN.get().unwrap_unchecked() })(data, sign_flips);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create all-ones sign flips (identity rotation, for testing FWHT alone).
    fn ones(n: usize) -> Vec<f32> {
        vec![1.0f32; n]
    }

    #[test]
    fn test_fwht_scalar_known_4_all_ones() {
        // WHT of [1,1,1,1] unnormalized = [4,0,0,0]
        // Normalized (div by sqrt(4)=2): [2,0,0,0]
        let mut data = [1.0f32, 1.0, 1.0, 1.0];
        let signs = ones(4);
        randomized_fwht_scalar(&mut data, &signs);
        assert!((data[0] - 2.0).abs() < 1e-6, "expected 2.0, got {}", data[0]);
        for i in 1..4 {
            assert!(data[i].abs() < 1e-6, "expected 0.0 at [{i}], got {}", data[i]);
        }
    }

    #[test]
    fn test_fwht_scalar_known_4_delta() {
        // WHT of [1,0,0,0] unnormalized = [1,1,1,1]
        // Normalized (div by 2): [0.5, 0.5, 0.5, 0.5]
        let mut data = [1.0f32, 0.0, 0.0, 0.0];
        let signs = ones(4);
        randomized_fwht_scalar(&mut data, &signs);
        for i in 0..4 {
            assert!(
                (data[i] - 0.5).abs() < 1e-6,
                "expected 0.5 at [{i}], got {}",
                data[i]
            );
        }
    }

    #[test]
    fn test_fwht_scalar_self_inverse() {
        // Normalized FWHT is self-inverse: FWHT(FWHT(x)) == x
        for &dim in &[4, 8, 16, 64, 1024] {
            let signs = ones(dim);
            let original: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01 - 0.5).collect();
            let mut data = original.clone();

            // Apply normalized FWHT twice
            randomized_fwht_scalar(&mut data, &signs);
            randomized_fwht_scalar(&mut data, &signs);

            for i in 0..dim {
                assert!(
                    (data[i] - original[i]).abs() < 1e-4,
                    "self-inverse failed at dim={dim}, idx={i}: got {}, expected {}",
                    data[i],
                    original[i]
                );
            }
        }
    }

    #[test]
    fn test_sign_flips_application() {
        let mut data = [1.0f32, 2.0, -3.0, 4.0];
        let signs = [1.0f32, -1.0, -1.0, 1.0];
        apply_sign_flips(&mut data, &signs);
        assert_eq!(data, [1.0, -2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_fwht_with_random_signs_inverse() {
        // Randomized FWHT: R(x) = H * D * x where D = diag(signs)
        // Inverse: R^{-1}(y) = D * H * y  (since D^-1 = D, H^-1 = H for normalized WHT)
        // So: forward = apply_signs then fwht then normalize
        //     inverse = fwht then normalize then apply_signs
        let dim = 64;
        let signs: Vec<f32> = (0..dim)
            .map(|i| if i % 3 == 0 { -1.0 } else { 1.0 })
            .collect();
        let original: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.02 - 0.6).collect();
        let mut data = original.clone();

        // Forward: signs then FWHT then normalize
        randomized_fwht_scalar(&mut data, &signs);

        // Inverse: FWHT then normalize then signs
        let ones = vec![1.0f32; dim];
        randomized_fwht_scalar(&mut data, &ones);
        apply_sign_flips(&mut data, &signs);

        for i in 0..dim {
            assert!(
                (data[i] - original[i]).abs() < 1e-4,
                "sign-flip inverse failed at idx={i}: got {}, expected {}",
                data[i],
                original[i]
            );
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_avx2_matches_scalar() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        let dim = 1024;
        let signs: Vec<f32> = (0..dim)
            .map(|i| if (i * 7 + 3) % 5 < 2 { -1.0 } else { 1.0 })
            .collect();
        let original: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.001 - 0.5).collect();

        // Scalar path
        let mut scalar_data = original.clone();
        randomized_fwht_scalar(&mut scalar_data, &signs);

        // AVX2 path
        let mut avx2_data = original.clone();
        // SAFETY: AVX2 verified above.
        unsafe { fwht_avx2(&mut avx2_data, &signs) };

        for i in 0..dim {
            assert!(
                (scalar_data[i] - avx2_data[i]).abs() < 1e-6,
                "AVX2 mismatch at [{i}]: scalar={}, avx2={}",
                scalar_data[i],
                avx2_data[i]
            );
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn test_neon_matches_scalar() {
        for &dim in &[4, 8, 16, 64, 256, 1024] {
            let signs: Vec<f32> = (0..dim)
                .map(|i| if (i * 7 + 3) % 5 < 2 { -1.0 } else { 1.0 })
                .collect();
            let original: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.001 - 0.5).collect();

            // Scalar path
            let mut scalar_data = original.clone();
            randomized_fwht_scalar(&mut scalar_data, &signs);

            // NEON path
            let mut neon_data = original.clone();
            // SAFETY: NEON is baseline on AArch64.
            unsafe { fwht_neon(&mut neon_data, &signs) };

            for i in 0..dim {
                assert!(
                    (scalar_data[i] - neon_data[i]).abs() < 1e-6,
                    "NEON mismatch at dim={dim} [{i}]: scalar={}, neon={}",
                    scalar_data[i],
                    neon_data[i]
                );
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn test_neon_self_inverse() {
        let dim = 1024;
        let signs: Vec<f32> = (0..dim)
            .map(|i| if (i * 11 + 5) % 3 == 0 { -1.0 } else { 1.0 })
            .collect();
        let original: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.002 - 1.0).collect();
        let mut data = original.clone();

        // Apply NEON FWHT twice (self-inverse with identity signs)
        let ones_signs = vec![1.0f32; dim];
        // SAFETY: NEON is baseline on AArch64.
        unsafe {
            fwht_neon(&mut data, &signs);
            // Inverse: FWHT then normalize then apply signs
            fwht_neon(&mut data, &ones_signs);
        }
        apply_sign_flips(&mut data, &signs);

        for i in 0..dim {
            assert!(
                (data[i] - original[i]).abs() < 1e-4,
                "NEON self-inverse failed at [{i}]: got {}, expected {}",
                data[i],
                original[i]
            );
        }
    }

    #[test]
    fn test_dispatch_init_and_call() {
        init_fwht();
        let dim = 16;
        let signs = ones(dim);
        let original: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
        let mut data = original.clone();

        fwht(&mut data, &signs);
        fwht(&mut data, &signs);

        for i in 0..dim {
            assert!(
                (data[i] - original[i]).abs() < 1e-4,
                "dispatch self-inverse failed at [{i}]: got {}, expected {}",
                data[i],
                original[i]
            );
        }
    }
}
