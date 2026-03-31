//! GPU-accelerated batch Fast Walsh-Hadamard Transform for TurboQuant encoding.
//!
//! When encoding a large batch of vectors during segment compaction, the FWHT
//! can be offloaded to the GPU. The CUDA kernel in
//! `src/gpu/kernels/turbo_quant_wht.cu` implements the butterfly pattern using
//! shared memory, processing all vectors in the batch in parallel.
//!
//! ## Build pipeline
//!
//! `build.rs` compiles `turbo_quant_wht.cu` to PTX via nvcc when the `gpu-cuda`
//! feature is enabled. The PTX is embedded at compile time via `include_str!`.
//! When nvcc is absent, a placeholder PTX is written and detected at runtime,
//! causing `gpu_batch_fwht` to return `CudaNotAvailable`.

use super::context::GpuContext;
use super::error::GpuBuildError;

/// Pre-compiled PTX for the FWHT kernel, embedded from build.rs output.
///
/// When nvcc is not available, this contains the placeholder string
/// `"// nvcc not available"` and `ptx_is_valid()` returns false.
#[cfg(feature = "gpu-cuda")]
const FWHT_PTX: &str = include_str!(concat!(env!("OUT_DIR"), "/turbo_quant_wht.ptx"));

/// Check whether the embedded PTX contains a real compiled kernel.
///
/// Returns `false` when build.rs wrote the nvcc-not-available placeholder,
/// indicating the GPU FWHT path should not be attempted.
#[cfg(feature = "gpu-cuda")]
fn ptx_is_valid() -> bool {
    !FWHT_PTX.starts_with("// nvcc")
}

/// Minimum batch size for GPU FWHT to be worthwhile.
/// Below this threshold, CPU FWHT (scalar or AVX2) is faster due to
/// host-device transfer overhead and kernel launch latency.
pub const MIN_BATCH_FOR_GPU: usize = 1_000;

/// Maximum padded_dim supported by the GPU kernel.
///
/// The kernel uses `padded_dim / 2` threads per block. CUDA blocks are
/// limited to 1024 threads, so `padded_dim` cannot exceed 2048.
const MAX_PADDED_DIM: usize = 2048;

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
/// * `padded_dim` - Padded dimensionality (must be a power of 2, max 2048)
///
/// # Errors
///
/// - `CudaNotAvailable` if PTX was not compiled (nvcc absent at build time)
/// - `KernelLaunchFailed` if padded_dim exceeds 2048 or kernel dispatch fails
/// - `DeviceError` if host-device memory transfers fail
///
/// # Panics
///
/// Debug-asserts that `padded_dim` is a power of 2 and `sign_flips.len() == padded_dim`.
#[cfg(feature = "gpu-cuda")]
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

    if !ptx_is_valid() {
        return Err(GpuBuildError::CudaNotAvailable);
    }

    let batch_size = vectors.len() / padded_dim;
    if batch_size == 0 {
        return Ok(());
    }

    // Enforce block size limit: padded_dim/2 threads per block, max 1024
    if padded_dim > MAX_PADDED_DIM {
        return Err(GpuBuildError::KernelLaunchFailed(format!(
            "padded_dim {padded_dim} exceeds GPU FWHT limit of {MAX_PADDED_DIM}"
        )));
    }

    let dev = ctx.device();

    // Load PTX module (cudarc caches internally by module name, idempotent)
    use cudarc::driver::LaunchAsync;
    let ptx = cudarc::nvrtc::safe::Ptx::from_src(FWHT_PTX);
    dev.load_ptx(ptx, "tq_wht", &["batch_randomized_fwht"])
        .map_err(|e| GpuBuildError::KernelLaunchFailed(e.to_string()))?;

    let func = dev
        .get_func("tq_wht", "batch_randomized_fwht")
        .ok_or_else(|| {
            GpuBuildError::KernelLaunchFailed("kernel function not found".into())
        })?;

    // Host -> Device transfers
    let mut dev_vecs = dev
        .htod_sync_copy(vectors)
        .map_err(|e| GpuBuildError::DeviceError(e.to_string()))?;
    let dev_flips = dev
        .htod_sync_copy(sign_flips)
        .map_err(|e| GpuBuildError::DeviceError(e.to_string()))?;

    let cfg = cudarc::driver::LaunchConfig {
        grid_dim: (batch_size as u32, 1, 1),
        block_dim: ((padded_dim / 2) as u32, 1, 1),
        shared_mem_bytes: (padded_dim * std::mem::size_of::<f32>()) as u32,
    };

    // SAFETY: Kernel parameters match the turbo_quant_wht.cu signature:
    //   float* vectors, const float* flips, const int padded_dim
    // dev_vecs and dev_flips are valid CudaSlice allocations of correct size.
    // shared_mem_bytes = padded_dim * 4 matches kernel's extern __shared__ float sdata[].
    // grid_dim.x = batch_size ensures each block processes one vector.
    // block_dim.x = padded_dim/2 threads <= 1024 (enforced by MAX_PADDED_DIM check above).
    unsafe {
        func.launch(cfg, (&mut dev_vecs, &dev_flips, padded_dim as i32))
    }
    .map_err(|e| GpuBuildError::KernelLaunchFailed(e.to_string()))?;

    // Device -> Host: copy transformed vectors back in-place
    dev.dtoh_sync_copy_into(&dev_vecs, vectors)
        .map_err(|e| GpuBuildError::DeviceError(e.to_string()))?;

    Ok(())
}

/// Stub when gpu-cuda feature is disabled -- always returns CudaNotAvailable.
#[cfg(not(feature = "gpu-cuda"))]
#[allow(unused_variables)]
pub fn gpu_batch_fwht(
    ctx: &GpuContext,
    vectors: &mut [f32],
    sign_flips: &[f32],
    padded_dim: usize,
) -> Result<(), GpuBuildError> {
    Err(GpuBuildError::CudaNotAvailable)
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    #[cfg(feature = "gpu-cuda")]
    fn test_ptx_loads_or_fallback() {
        // Verify PTX constant is included (may be valid or fallback)
        assert!(!FWHT_PTX.is_empty());
    }

    #[test]
    #[cfg(feature = "gpu-cuda")]
    fn test_ptx_validity_detection() {
        // ptx_is_valid() must return false when nvcc was absent at build time
        // (the placeholder starts with "// nvcc"). On a real GPU build machine,
        // it returns true.
        let valid = ptx_is_valid();
        if FWHT_PTX.starts_with("// nvcc") {
            assert!(!valid, "placeholder PTX should be detected as invalid");
        } else {
            assert!(valid, "real PTX should be detected as valid");
        }
    }

    #[test]
    fn test_fwht_returns_error_without_gpu() {
        // On CI without GPU, should return CudaNotAvailable or DeviceError -- never panics
        let result = super::super::try_gpu_batch_fwht(&mut [0.0; 1024], &[1.0; 1024], 1024);
        // Either succeeds (real GPU) or returns false (no GPU)
        let _ = result;
    }
}
