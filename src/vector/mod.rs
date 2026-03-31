//! Vector search engine — distance computation, aligned buffers, and SIMD kernels.

pub mod aligned_buffer;
pub mod distance;
pub mod filter;
pub mod hnsw;
pub mod metrics;
pub mod mvcc;
pub mod persistence;
pub mod segment;
pub mod store;
pub mod turbo_quant;
pub mod types;

#[cfg(feature = "gpu-cuda")]
pub mod gpu;
