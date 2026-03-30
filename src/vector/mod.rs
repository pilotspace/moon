//! Vector search engine — distance computation, aligned buffers, and SIMD kernels.

pub mod aligned_buffer;
pub mod distance;
pub mod hnsw;
pub mod segment;
pub mod turbo_quant;
pub mod filter;
pub mod store;
pub mod types;
pub mod mvcc;
pub mod persistence;
pub mod metrics;

#[cfg(feature = "gpu-cuda")]
pub mod gpu;

