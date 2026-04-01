// CAGRA graph construction — placeholder.
//
// CAGRA (CUDA Accelerated Graph-based Retrieval Algorithm) is provided by
// NVIDIA's cuVS library, not as a custom kernel. This file exists as a
// documentation placeholder.
//
// Integration plan:
//   - Use cudarc to call cuVS C API via FFI when Rust bindings mature.
//   - cuVS handles: kNN graph construction, graph optimization, export.
//   - Moon handles: kNN-to-HNSW conversion, upper layer construction,
//     BFS reorder, recall verification.
//
// No custom CUDA kernel is needed for CAGRA — the cuVS library provides
// the full graph build pipeline.
