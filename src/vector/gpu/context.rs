//! GPU context wrapper around cudarc device management.
//!
//! `GpuContext` manages a single CUDA device and provides methods for
//! querying device properties. It is the entry point for all GPU operations
//! in the vector search pipeline.

use super::error::GpuBuildError;
use cudarc::driver::CudaDevice;
use std::sync::Arc;

/// Wrapper around a cudarc CUDA device, providing a stable API surface
/// for GPU-accelerated vector operations.
///
/// Each `GpuContext` owns a reference to a single GPU device. Multiple
/// contexts can share the same physical device (cudarc handles this via
/// `Arc<CudaDevice>`).
pub struct GpuContext {
    device: Arc<CudaDevice>,
}

impl GpuContext {
    /// Create a new GPU context for the given device ordinal.
    ///
    /// # Errors
    ///
    /// Returns `GpuBuildError::CudaNotAvailable` if CUDA is not initialized,
    /// or `GpuBuildError::DeviceError` if the specified device cannot be opened.
    pub fn new(device_ordinal: usize) -> Result<Self, GpuBuildError> {
        let device = CudaDevice::new(device_ordinal).map_err(|e| {
            GpuBuildError::DeviceError(format!("failed to open device {device_ordinal}: {e}"))
        })?;
        Ok(Self { device })
    }

    /// Check whether any CUDA device is accessible.
    ///
    /// This attempts to open device 0. Returns `true` if successful.
    /// Useful as a quick probe before attempting GPU-accelerated operations.
    pub fn is_available() -> bool {
        CudaDevice::new(0).is_ok()
    }

    /// Return the device name string (e.g. "NVIDIA A100-SXM4-80GB").
    pub fn device_name(&self) -> Result<String, GpuBuildError> {
        self.device
            .name()
            .map_err(|e| GpuBuildError::DeviceError(format!("failed to query device name: {e}")))
    }

    /// Return the total global memory on this device in bytes.
    pub fn total_memory(&self) -> Result<usize, GpuBuildError> {
        self.device
            .total_memory()
            .map_err(|e| GpuBuildError::DeviceError(format!("failed to query memory: {e}")))
    }

    /// Borrow the underlying cudarc device for direct API calls.
    ///
    /// Used internally by cagra and fwht_kernel modules.
    pub(super) fn device(&self) -> &Arc<CudaDevice> {
        &self.device
    }
}
