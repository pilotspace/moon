//! HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search.
//!
//! Single-threaded, cache-optimized with BFS reordering and dual prefetch.

pub mod build;
pub mod graph;
pub mod search;
