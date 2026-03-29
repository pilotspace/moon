//! HNSW index builder — single-threaded construction with BFS reorder.
//!
//! Constructs an `HnswGraph` via incremental insertion, then applies BFS
//! reordering for cache-friendly layer-0 traversal.
