//! Persistent GPU context wrapper for batch vector operations.
//!
//! Wraps a single `GpuContext` that is created once and reused across
//! FWHT batch operations, avoiding the overhead of per-call context
//! creation (CUDA context init is ~50-100ms).
//!
//! NOTE: Device memory (`CudaSlice`) is still allocated per batch via
//! `htod_sync_copy` in this phase. Persistent device buffer pooling
//! is a future optimization.

use super::context::GpuContext;
use super::error::GpuBuildError;

/// Persistent GPU context for reuse across batch FWHT calls.
///
/// The pool holds a single `GpuContext` created once. Callers use
/// `pool.context()` to get the underlying device for kernel launches.
/// This avoids the per-call `GpuContext::new(0)` overhead in the
/// compaction pipeline.
pub struct GpuMemoryPool {
    ctx: GpuContext,
}

impl GpuMemoryPool {
    /// Create a pool wrapping a persistent `GpuContext` on the given device.
    pub fn new(device_ordinal: usize) -> Result<Self, GpuBuildError> {
        let ctx = GpuContext::new(device_ordinal)?;
        Ok(Self { ctx })
    }

    /// Access the underlying GPU context for kernel launches.
    pub fn context(&self) -> &GpuContext {
        &self.ctx
    }
}
