//! GPU-accelerated HNSW graph construction via NVIDIA CAGRA.
//!
//! CAGRA (CUDA Accelerated Graph-based Retrieval Algorithm) builds a
//! k-nearest-neighbor graph on the GPU, then converts it to an HNSW-compatible
//! format for CPU-based search serving.
//!
//! ## Intended flow
//!
//! 1. Upload `vectors_f32` to GPU device memory.
//! 2. Run CAGRA graph construction kernel (builds optimized kNN graph).
//! 3. Export kNN graph to HNSW layer-0 format (reindex, pad neighbor lists).
//! 4. Build upper layers on CPU (CAGRA only builds the base layer).
//! 5. Download completed graph, BFS-reorder, return `HnswGraph`.
//! 6. Caller runs recall verification against brute-force sample.
//!
//! ## Current status
//!
//! This module defines the API surface only. The actual cuVS CAGRA integration
//! requires the cuVS SDK which does not yet have stable Rust bindings. The
//! function returns `CudaNotAvailable` until the SDK is integrated.

use super::context::GpuContext;
use super::error::GpuBuildError;
use crate::vector::hnsw::graph::HnswGraph;

/// Minimum number of vectors for GPU build to be worthwhile.
/// Below this threshold, CPU HNSW construction is faster due to
/// host-device transfer overhead and kernel launch latency.
pub const MIN_VECTORS_FOR_GPU: usize = 10_000;

/// Build an HNSW graph on the GPU using CAGRA.
///
/// **Current status: SCAFFOLD ONLY.**
/// Returns `CudaNotAvailable` unconditionally. Actual CAGRA kNN graph
/// construction will be implemented in a future phase once the cuvs
/// Rust API is validated against real hardware.
///
/// When `gpu-cagra` feature is enabled and cuVS is linked, the plan is:
/// 1. Upload vectors to GPU via `ManagedTensor`
/// 2. Build kNN graph via CAGRA `Index::build` (base layer)
/// 3. Export kNN graph, convert to HNSW layer-0 format
/// 4. Build upper layers on CPU
/// 5. BFS-reorder and return `HnswGraph`
///
/// Without `gpu-cagra` feature: returns `CudaNotAvailable`.
///
/// # Arguments
///
/// * `ctx` - GPU context (device must be initialized)
/// * `vectors_f32` - Flat array of `f32` vectors, length = `num_vectors * dim`
/// * `dim` - Dimensionality of each vector
/// * `m` - HNSW connectivity parameter (neighbors per node on upper layers)
/// * `ef_construction` - Search width during construction
/// * `seed` - Random seed for reproducibility
///
/// # Errors
///
/// Returns `GpuBuildError::CudaNotAvailable` (cuVS integration pending).
/// Future errors include `OutOfMemory`, `KernelLaunchFailed`, and
/// `RecallBelowThreshold` if post-build verification fails.
///
/// # Panics
///
/// Debug-asserts that `vectors_f32.len() % dim == 0`.
#[allow(unused_variables)]
pub fn gpu_build_hnsw(
    ctx: &GpuContext,
    vectors_f32: &[f32],
    dim: usize,
    m: u8,
    ef_construction: u16,
    seed: u64,
) -> Result<HnswGraph, GpuBuildError> {
    debug_assert_eq!(
        vectors_f32.len() % dim,
        0,
        "vectors_f32 length must be a multiple of dim"
    );

    #[cfg(feature = "gpu-cagra")]
    {
        // Validate cuvs types are importable under this feature gate.
        // This ensures the feature flag + dependency wiring is correct.
        #[allow(unused_imports)]
        use cuvs::cagra::IndexParams;

        // TODO(future-phase): Implement actual CAGRA kNN graph construction:
        //   1. Create ManagedTensor from vectors_f32 with shape [n, dim]
        //   2. let res = Resources::new()?;
        //   3. let params = IndexParams::new()?;
        //   4. let index = Index::build(&res, &params, &dataset)?;
        //   5. Extract kNN graph from index
        //   6. Convert kNN adjacency list to HnswGraph layer-0 format
        //   7. Build upper layers on CPU using existing HNSW builder
        //   8. BFS reorder

        // For now, return CudaNotAvailable as scaffold placeholder.
        return Err(GpuBuildError::CudaNotAvailable);
    }

    #[cfg(not(feature = "gpu-cagra"))]
    {
        // cuVS not linked -- CAGRA graph build unavailable.
        Err(GpuBuildError::CudaNotAvailable)
    }
}
