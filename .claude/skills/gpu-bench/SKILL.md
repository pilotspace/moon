---
name: gpu-bench
description: Benchmark GPU-accelerated operations (vector distance, batch computation) against CPU baselines. Requires CUDA toolkit. Args: vector|distance|batch|all.
---

GPU vs CPU benchmark suite for moon acceleration paths.

## Usage

- `/gpu-bench vector` — vector distance computation (L2, cosine, dot)
- `/gpu-bench batch` — batch operation throughput
- `/gpu-bench all` — full GPU benchmark suite

## Prerequisites

1. Verify CUDA availability:
   ```bash
   nvcc --version
   nvidia-smi
   ```

2. Build with GPU feature:
   ```bash
   RUSTFLAGS="-C target-cpu=native" cargo build --release --features gpu-cuda
   ```

## Benchmark Matrix

| Operation | Dimensions | Batch Size | Measure |
|-----------|-----------|------------|---------|
| L2 distance | 128/256/512/1024 | 1/100/1K/10K | ops/sec, latency |
| Cosine similarity | 128/256/512/1024 | 1/100/1K/10K | ops/sec, latency |
| Dot product | 128/256/512/1024 | 1/100/1K/10K | ops/sec, latency |
| HNSW search | 128d, 100K vectors | 1/10/100 queries | recall@10, latency |
| Batch MGET | — | 100/1K/10K keys | throughput |

## Steps

1. Run CPU baseline (SIMD path):
   ```bash
   RUSTFLAGS="-C target-cpu=native" cargo bench --bench gpu_distance -- --baseline cpu
   ```

2. Run GPU path:
   ```bash
   cargo bench --bench gpu_distance --features gpu-cuda
   ```

3. Generate comparison table with:
   - Crossover point (batch size where GPU beats CPU)
   - Memory transfer overhead percentage
   - Kernel launch latency
   - Peak GFLOPS utilization

4. Recommend optimal batch sizes for production use.
