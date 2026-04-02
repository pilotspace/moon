//! DiskANN scaffold -- Vamana graph, Product Quantization, and co-located page format.
//!
//! This module provides cold-tier vector search data structures per MoonStore v2
//! design sections 7.4 and 11.2. Scaffold only -- no io_uring or O_DIRECT.

pub mod page;
pub mod pq;
pub mod vamana;
