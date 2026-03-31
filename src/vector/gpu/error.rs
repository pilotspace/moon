//! Error types for GPU-accelerated vector operations.

use std::fmt;

/// Errors that can occur during GPU-accelerated build operations.
#[derive(Debug)]
pub enum GpuBuildError {
    /// CUDA runtime or device is not available on this system.
    CudaNotAvailable,

    /// A CUDA device error occurred (driver failure, device reset, etc.).
    DeviceError(String),

    /// GPU ran out of memory during the operation.
    OutOfMemory {
        /// Bytes requested by the operation.
        requested: usize,
        /// Bytes available on the device at time of failure.
        available: usize,
    },

    /// CAGRA-built graph did not meet the recall threshold after verification.
    RecallBelowThreshold {
        /// Measured recall from verification sampling.
        actual: f32,
        /// Minimum acceptable recall.
        threshold: f32,
    },

    /// A CUDA kernel failed to launch.
    KernelLaunchFailed(String),

    /// Device synchronization failed after kernel execution.
    SynchronizationFailed(String),
}

impl fmt::Display for GpuBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CudaNotAvailable => write!(f, "CUDA runtime not available"),
            Self::DeviceError(msg) => write!(f, "CUDA device error: {msg}"),
            Self::OutOfMemory {
                requested,
                available,
            } => write!(
                f,
                "GPU out of memory: requested {requested} bytes, {available} bytes available"
            ),
            Self::RecallBelowThreshold { actual, threshold } => {
                write!(f, "recall {actual:.4} below threshold {threshold:.4}")
            }
            Self::KernelLaunchFailed(msg) => write!(f, "kernel launch failed: {msg}"),
            Self::SynchronizationFailed(msg) => write!(f, "device sync failed: {msg}"),
        }
    }
}

impl std::error::Error for GpuBuildError {}
