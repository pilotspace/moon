//! Portable scalar distance kernels — reference implementations.
//!
//! These serve as:
//! 1. Correctness reference for SIMD kernel validation
//! 2. Universal fallback on platforms without SIMD support
//!
//! All distance functions return *squared* L2 distance (no sqrt) for comparison use,
//! or cosine *distance* (1 - similarity) for angular metrics.

/// Squared L2 distance between two f32 slices.
///
/// Returns `sum((a[i] - b[i])^2)` — no square root (cheaper for comparison).
///
/// # Panics (debug only)
/// Debug-asserts that `a.len() == b.len()`.
#[inline]
pub fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "l2_f32: dimension mismatch");
    let mut sum = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = x - y;
        sum += d * d;
    }
    sum
}

/// Squared L2 distance between two i8 slices.
///
/// Accumulates in `i32` to avoid overflow (max per-element: (127 - (-128))^2 = 65025).
///
/// # Panics (debug only)
/// Debug-asserts that `a.len() == b.len()`.
#[inline]
pub fn l2_i8(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len(), "l2_i8: dimension mismatch");
    let mut sum = 0i32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = *x as i32 - *y as i32;
        sum += d * d;
    }
    sum
}

/// Dot product of two f32 slices.
///
/// Returns `sum(a[i] * b[i])`.
///
/// # Panics (debug only)
/// Debug-asserts that `a.len() == b.len()`.
#[inline]
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "dot_f32: dimension mismatch");
    let mut sum = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        sum += x * y;
    }
    sum
}

/// Cosine distance between two f32 slices.
///
/// Returns `1.0 - dot(a, b) / (||a|| * ||b||)`.
/// Range: [0.0, 2.0] where 0.0 = identical direction, 2.0 = opposite.
///
/// If either vector has zero norm, returns 1.0 (maximum meaningful distance).
///
/// # Panics (debug only)
/// Debug-asserts that `a.len() == b.len()`.
#[inline]
pub fn cosine_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_f32: dimension mismatch");
    let mut dot = 0.0f32;
    let mut norm_a_sq = 0.0f32;
    let mut norm_b_sq = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a_sq += x * x;
        norm_b_sq += y * y;
    }
    let norm_a = norm_a_sq.sqrt();
    let norm_b = norm_b_sq.sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    1.0 - dot / (norm_a * norm_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_f32_basic() {
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, 5.0, 6.0];
        // (1-4)^2 + (2-5)^2 + (3-6)^2 = 9 + 9 + 9 = 27
        assert_eq!(l2_f32(&a, &b), 27.0);
    }

    #[test]
    fn test_l2_f32_identical() {
        let a = [1.0f32, 2.0, 3.0, 4.0];
        assert_eq!(l2_f32(&a, &a), 0.0);
    }

    #[test]
    fn test_l2_i8_basic() {
        let a = [1i8, 2, 3];
        let b = [4i8, 5, 6];
        // (1-4)^2 + (2-5)^2 + (3-6)^2 = 9 + 9 + 9 = 27
        assert_eq!(l2_i8(&a, &b), 27);
    }

    #[test]
    fn test_l2_i8_extreme() {
        // Verify no overflow: max diff = 127 - (-128) = 255, squared = 65025
        let a = [127i8];
        let b = [-128i8];
        assert_eq!(l2_i8(&a, &b), 65025);
    }

    #[test]
    fn test_dot_f32_basic() {
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, 5.0, 6.0];
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        assert_eq!(dot_f32(&a, &b), 32.0);
    }

    #[test]
    fn test_dot_f32_orthogonal() {
        let a = [1.0f32, 0.0, 0.0];
        let b = [0.0f32, 1.0, 0.0];
        assert_eq!(dot_f32(&a, &b), 0.0);
    }

    #[test]
    fn test_cosine_f32_identical() {
        let a = [1.0f32, 2.0, 3.0];
        let dist = cosine_f32(&a, &a);
        assert!(
            (dist - 0.0).abs() < 1e-6,
            "identical vectors should have distance ~0, got {dist}"
        );
    }

    #[test]
    fn test_cosine_f32_opposite() {
        let a = [1.0f32, 2.0, 3.0];
        let b = [-1.0f32, -2.0, -3.0];
        let dist = cosine_f32(&a, &b);
        assert!(
            (dist - 2.0).abs() < 1e-6,
            "opposite vectors should have distance ~2, got {dist}"
        );
    }

    #[test]
    fn test_cosine_f32_zero_norm() {
        let a = [0.0f32, 0.0, 0.0];
        let b = [1.0f32, 2.0, 3.0];
        assert_eq!(cosine_f32(&a, &b), 1.0);
        assert_eq!(cosine_f32(&b, &a), 1.0);
    }

    #[test]
    fn test_cosine_f32_orthogonal() {
        let a = [1.0f32, 0.0];
        let b = [0.0f32, 1.0];
        let dist = cosine_f32(&a, &b);
        assert!(
            (dist - 1.0).abs() < 1e-6,
            "orthogonal vectors should have distance ~1, got {dist}"
        );
    }
}
