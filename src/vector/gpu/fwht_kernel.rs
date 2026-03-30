//! GPU-accelerated batch Fast Walsh-Hadamard Transform for TurboQuant encoding.
//!
//! When encoding a large batch of vectors during segment compaction, the FWHT
//! can be offloaded to the GPU. The CUDA kernel in
//! `src/gpu/kernels/turbo_quant_wht.cu` implements the butterfly pattern using
//! shared memory, processing all vectors in the batch in parallel.
//!
//! ## Current status
//!
//! This module defines the API surface only. The CUDA kernel template exists
//! at `src/gpu/kernels/turbo_quant_wht.cu` but is not yet compiled by build.rs.
//! The function returns `CudaNotAvailable` until kernel compilation is wired up.

use super::context::GpuContext;
use super::error::GpuBuildError;

/// Minimum batch size for GPU FWHT to be worthwhile.
/// Below this threshold, CPU FWHT (scalar or AVX2) is faster due to
/// host-device transfer overhead and kernel launch latency.
pub const MIN_BATCH_FOR_GPU: usize = 1_000;

/// Apply randomized FWHT to a batch of vectors on the GPU.
///
/// Each vector in the batch has `padded_dim` elements. The `vectors` slice
/// contains `batch_size * padded_dim` floats laid out contiguously.
/// `sign_flips` has `padded_dim` elements (shared across all vectors).
///
/// The transform is applied in-place: on return, `vectors` contains the
/// FWHT-rotated values (normalized, with sign flips applied).
///
/// # Arguments
///
/// * `ctx` - GPU context (device must be initialized)
/// * `vectors` - Flat mutable slice of `batch_size * padded_dim` floats
/// * `sign_flips` - Sign flip array of length `padded_dim` (values +1.0 or -1.0)
/// * `padded_dim` - Padded dimensionality (must be a power of 2)
///
/// # Errors
///
/// Returns `GpuBuildError::CudaNotAvailable` (CUDA kernel not yet compiled).
/// Future errors include `OutOfMemory` and `KernelLaunchFailed`.
///
/// # Panics
///
/// Debug-asserts that `padded_dim` is a power of 2 and `sign_flips.len() == padded_dim`.
#[allow(unused_variables)]
pub fn gpu_batch_fwht(
    ctx: &GpuContext,
    vectors: &mut [f32],
    sign_flips: &[f32],
    padded_dim: usize,
) -> Result<(), GpuBuildError> {
    debug_assert!(
        padded_dim.is_power_of_two(),
        "padded_dim must be a power of 2, got {padded_dim}"
    );
    debug_assert_eq!(
        sign_flips.len(),
        padded_dim,
        "sign_flips length must equal padded_dim"
    );

    // TODO: Compile and load turbo_quant_wht.cu kernel, then:
    //
    //   1. let batch_size = vectors.len() / padded_dim;
    //   2. let dev_vectors = ctx.device().htod_sync_copy(vectors)?;
    //   3. let dev_flips = ctx.device().htod_sync_copy(sign_flips)?;
    //   4. launch batch_randomized_fwht kernel (grid=batch_size, block=padded_dim/2)
    //   5. ctx.device().dtoh_sync_copy_into(&dev_vectors, vectors)?;
    //   6. Ok(())
    Err(GpuBuildError::CudaNotAvailable)
}
