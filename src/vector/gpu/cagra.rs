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

    // TODO: Integrate cuVS CAGRA when Rust bindings are available.
    //
    // Implementation outline:
    //   1. let dev_vectors = ctx.device().htod_sync_copy(vectors_f32)?;
    //   2. let cagra_params = CagraParams { m, ef_construction, .. };
    //   3. let knn_graph = cagra_build(ctx.device(), &dev_vectors, dim, &cagra_params)?;
    //   4. let hnsw = convert_knn_to_hnsw(knn_graph, m, seed)?;
    //   5. Ok(hnsw)
    Err(GpuBuildError::CudaNotAvailable)
}
