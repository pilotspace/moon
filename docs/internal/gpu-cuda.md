# GPU / CUDA acceleration — rules and integration pattern

> Internal engineering reference, moved out of `CLAUDE.md` (2026-08-27).
> Feature-gated behind `--features gpu-cuda`; never in the default feature set.

## When to Use GPU
- Vector distance computation (L2, cosine, dot product) on batches > 1000 vectors.
- Bulk SIMD operations that exceed CPU SIMD width benefits (e.g., 10K+ float32 comparisons).
- Never for single-key operations — CPU + SIMD is always faster for individual lookups.

## CUDA Integration Pattern
- Use `cudarc` crate for safe Rust CUDA bindings (no raw FFI).
- Feature-gated: `--features gpu-cuda` — never in the default feature set.
- Kernels live in `src/gpu/kernels/` as `.cu` files, compiled at build time via `build.rs`.
- CPU fallback is mandatory — GPU path is an optimization, not a requirement.
- Device memory management: use pinned memory (`cuMemAllocHost`) for host-device transfers.
- Batch operations: accumulate work in a queue, dispatch to GPU when batch is full or timeout fires.

## GPU Memory Rules
- Never allocate GPU memory per-request — use a pre-allocated pool.
- Transfer data in batches (≥64KB) to amortize PCIe latency.
- Pin host memory for DMA transfers when throughput matters.
- Free GPU memory on shard shutdown, not per-operation.

## Vector Search (Future)
- Per-shard HNSW index — no cross-shard GPU sharing.
- Distance kernels: `f32` precision, SIMD on CPU, CUDA on GPU.
- Index building on GPU, serving on CPU (unless batch query mode).
- Use half-precision (`f16`) for storage, promote to `f32` for computation.

## Build Requirements
- CUDA Toolkit ≥ 12.0, compute capability ≥ 7.0 (Volta+).
- `build.rs` detects CUDA availability — graceful fallback to CPU if absent.
- CI runs CPU-only (`--no-default-features --features runtime-tokio,jemalloc`) — GPU tested separately.
