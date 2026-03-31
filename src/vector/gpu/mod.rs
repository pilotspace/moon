//! GPU acceleration module for vector search operations.
//!
//! This module is only compiled when the `gpu-cuda` feature is enabled.
//! It provides GPU-accelerated HNSW graph construction (via CAGRA) and
//! batch FWHT computation for TurboQuant encoding.
//!
//! All functions gracefully return errors when CUDA operations fail,
//! allowing the caller to fall back to CPU implementations.
//!
//! ## Integration pattern
//!
//! The compaction pipeline calls [`try_gpu_build_hnsw`] and [`try_gpu_batch_fwht`]
//! which handle GPU context creation and error logging internally. On any failure
//! they return `None` / `false`, allowing the caller to fall through to the CPU path.

mod cagra;
mod context;
mod error;
mod fwht_kernel;
pub mod memory_pool;

pub use cagra::{gpu_build_hnsw, MIN_VECTORS_FOR_GPU};
pub use context::GpuContext;
pub use error::GpuBuildError;
pub use fwht_kernel::{gpu_batch_fwht, MIN_BATCH_FOR_GPU};
pub use memory_pool::GpuMemoryPool;

use super::hnsw::graph::HnswGraph;

/// Attempt GPU HNSW build, return `None` on any failure (caller uses CPU path).
///
/// Creates a fresh `GpuContext` on device 0, invokes CAGRA build, and returns
/// the resulting graph. Logs failures via `tracing::warn` (build errors) or
/// `tracing::debug` (device unavailable -- expected in CI).
///
/// The returned `HnswGraph` has valid BFS order/inverse mappings and is
/// compatible with the compaction pipeline's TQ buffer reorder step.
pub fn try_gpu_build_hnsw(
    vectors_f32: &[f32],
    dim: usize,
    m: u8,
    ef_construction: u16,
    seed: u64,
) -> Option<HnswGraph> {
    match GpuContext::new(0) {
        Ok(ctx) => match gpu_build_hnsw(&ctx, vectors_f32, dim, m, ef_construction, seed) {
            Ok(graph) => Some(graph),
            Err(e) => {
                tracing::warn!("GPU HNSW build failed, falling back to CPU: {e}");
                None
            }
        },
        Err(e) => {
            tracing::debug!("GPU not available for HNSW build: {e}");
            None
        }
    }
}

/// Attempt GPU batch FWHT using a pre-initialized pool.
///
/// Unlike [`try_gpu_batch_fwht`], this does NOT create a new `GpuContext` per call.
/// The caller (compaction pipeline) should maintain a [`GpuMemoryPool`] on
/// `VectorStore`, lazily initialized on first compaction.
pub fn try_gpu_batch_fwht_pooled(
    pool: &GpuMemoryPool,
    vectors: &mut [f32],
    sign_flips: &[f32],
    padded_dim: usize,
) -> bool {
    match gpu_batch_fwht(pool.context(), vectors, sign_flips, padded_dim) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!("GPU batch FWHT (pooled) failed, falling back to CPU: {e}");
            false
        }
    }
}

/// Attempt GPU batch FWHT, return `false` on failure (caller uses CPU path).
///
/// Creates a fresh `GpuContext` on device 0, runs the batch FWHT kernel in-place
/// on `vectors`. On success the slice is modified and `true` is returned. On any
/// failure the slice is left unmodified and `false` is returned.
///
/// **Deprecated:** Prefer [`try_gpu_batch_fwht_pooled`] with a persistent `GpuMemoryPool`.
pub fn try_gpu_batch_fwht(
    vectors: &mut [f32],
    sign_flips: &[f32],
    padded_dim: usize,
) -> bool {
    match GpuContext::new(0) {
        Ok(ctx) => match gpu_batch_fwht(&ctx, vectors, sign_flips, padded_dim) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("GPU batch FWHT failed, falling back to CPU: {e}");
                false
            }
        },
        Err(e) => {
            tracing::debug!("GPU not available for batch FWHT: {e}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_fwht_fallback_no_panic() {
        // Verifies the try_* wrapper returns false (not panic) when GPU unavailable.
        let mut vectors = vec![1.0f32; 1024];
        let sign_flips = vec![1.0f32; 1024];
        let result = try_gpu_batch_fwht(&mut vectors, &sign_flips, 1024);
        // On CI without GPU: result is false. On GPU machine: result is true.
        // Either way, no panic.
        let _ = result;
    }

    #[test]
    fn test_gpu_hnsw_fallback_no_panic() {
        // Verifies try_gpu_build_hnsw returns None (not panic) when GPU unavailable.
        let dim = 128;
        let n = 100;
        let vectors = vec![0.0f32; n * dim];
        let result = try_gpu_build_hnsw(&vectors, dim, 16, 200, 42);
        // On CI: None (either CudaNotAvailable or below MIN_VECTORS_FOR_GPU).
        // On GPU machine with gpu-cagra: may still be None (100 < 10000 threshold).
        let _ = result;
    }

    #[test]
    fn test_gpu_context_availability_check() {
        // GpuContext::is_available() should not panic regardless of CUDA presence.
        let available = GpuContext::is_available();
        // Just verify it returns a bool without crashing.
        let _ = available;
    }

    #[test]
    fn test_min_batch_thresholds() {
        // MIN_BATCH_FOR_GPU is from fwht_kernel.rs (gpu-cuda).
        assert_eq!(MIN_BATCH_FOR_GPU, 1_000);
        // MIN_VECTORS_FOR_GPU is from cagra.rs (also gpu-cuda, not gated behind gpu-cagra).
        assert_eq!(MIN_VECTORS_FOR_GPU, 10_000);
    }
}
