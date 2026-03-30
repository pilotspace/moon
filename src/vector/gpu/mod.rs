//! GPU acceleration module for vector search operations.
//!
//! This module is only compiled when the `gpu-cuda` feature is enabled.
//! It provides GPU-accelerated HNSW graph construction (via CAGRA) and
//! batch FWHT computation for TurboQuant encoding.
//!
//! All functions gracefully return errors when CUDA operations fail,
//! allowing the caller to fall back to CPU implementations.

mod cagra;
mod context;
mod error;
mod fwht_kernel;

pub use cagra::{gpu_build_hnsw, MIN_VECTORS_FOR_GPU};
pub use context::GpuContext;
pub use error::GpuBuildError;
pub use fwht_kernel::{gpu_batch_fwht, MIN_BATCH_FOR_GPU};
