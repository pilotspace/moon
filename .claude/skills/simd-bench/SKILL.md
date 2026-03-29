---
name: simd-bench
description: Benchmark SIMD vs scalar paths in dashtable probing, vector distance, and protocol parsing. Args: dashtable|distance|parse|all.
---

SIMD performance verification suite for moon.

## Usage

- `/simd-bench dashtable` — SSE2 group matching vs scalar fallback
- `/simd-bench distance` — SIMD distance kernels vs scalar
- `/simd-bench parse` — RESP protocol parsing with SIMD
- `/simd-bench all` — full SIMD benchmark suite

## Steps

### dashtable

1. Run existing Criterion benchmarks that exercise dashtable:
   ```bash
   RUSTFLAGS="-C target-cpu=native" cargo bench --bench get_hotpath -- "3_dashtable"
   ```

2. Check if AVX2 is available:
   ```bash
   sysctl -a 2>/dev/null | grep avx2 || lscpu 2>/dev/null | grep avx2
   ```

3. Compare SSE2 baseline vs scalar (force scalar via cfg):
   ```bash
   # Baseline (SIMD enabled)
   cargo bench --bench get_hotpath -- "3_dashtable" --save-baseline simd
   # Report improvement factor
   ```

### distance (for vector search)

1. Benchmark L2/cosine/dot with dimensions 128/256/512/1024:
   ```bash
   RUSTFLAGS="-C target-cpu=native" cargo bench --bench vector_distance 2>/dev/null || echo "No vector_distance bench yet — skip"
   ```

### parse

1. Benchmark RESP parsing:
   ```bash
   RUSTFLAGS="-C target-cpu=native" cargo bench --bench resp_parsing
   ```

## Expected Results

- SIMD dashtable probing: ≥2x faster than scalar for group match operations
- SSE2 is baseline x86_64 — always available
- AVX2 (if detected): potentially 2x over SSE2 for wider group matching
