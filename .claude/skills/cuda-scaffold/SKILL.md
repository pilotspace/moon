---
name: cuda-scaffold
description: Scaffold a new CUDA kernel with Rust integration via cudarc. Args: kernel_name [f32|f16]. Creates .cu kernel, Rust wrapper, CPU fallback, and benchmark.
---

Scaffold GPU kernel integration for moon.

## Usage

- `/cuda-scaffold vector_l2_distance f32` — L2 distance kernel
- `/cuda-scaffold batch_cosine f16` — half-precision cosine kernel

## Generated Artifacts

1. `src/gpu/kernels/{name}.cu` — CUDA kernel source
2. `src/gpu/{name}.rs` — Rust wrapper using cudarc
3. `src/gpu/fallback/{name}.rs` — CPU scalar + SIMD fallback
4. `build.rs` entry for kernel compilation (if not present)
5. Feature gate: `#[cfg(feature = "gpu-cuda")]`
6. Criterion benchmark in `benches/gpu_{name}.rs`

## CUDA Kernel Template

```cuda
#include <cuda_runtime.h>

__global__ void {name}_kernel(
    const float* __restrict__ a,
    const float* __restrict__ b,
    float* __restrict__ out,
    const int n,
    const int dim
) {
    const int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;

    const float* va = a + idx * dim;
    const float* vb = b + idx * dim;
    float acc = 0.0f;

    for (int d = 0; d < dim; d += 4) {
        float4 fa = *reinterpret_cast<const float4*>(va + d);
        float4 fb = *reinterpret_cast<const float4*>(vb + d);
        acc += fa.x * fb.x + fa.y * fb.y + fa.z * fb.z + fa.w * fb.w;
    }

    out[idx] = acc;
}
```

## Rust Wrapper Template

```rust
#[cfg(feature = "gpu-cuda")]
pub fn {name}_gpu(a: &[f32], b: &[f32], dim: usize) -> Vec<f32> {
    use cudarc::driver::*;
    let dev = CudaDevice::new(0).expect("CUDA device");
    let module = dev.load_ptx(PTX_SRC.into(), "{name}", &["{name}_kernel"]).unwrap();
    let f = module.get_fn("{name}_kernel").unwrap();
    // ... launch kernel, copy results
}

/// CPU fallback — always available, no CUDA dependency
pub fn {name}_cpu(a: &[f32], b: &[f32], dim: usize) -> Vec<f32> {
    let n = a.len() / dim;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let va = &a[i * dim..(i + 1) * dim];
        let vb = &b[i * dim..(i + 1) * dim];
        out.push(va.iter().zip(vb).map(|(x, y)| x * y).sum());
    }
    out
}
```

## Steps

1. Create directory structure: `src/gpu/kernels/`, `src/gpu/fallback/`
2. Generate CUDA kernel from template
3. Generate Rust wrapper with cudarc bindings
4. Generate CPU fallback (scalar + optional SIMD)
5. Add feature flag `gpu-cuda` to Cargo.toml if not present
6. Add build.rs kernel compilation step
7. Generate Criterion benchmark
8. Verify CPU fallback compiles without CUDA: `cargo check`
9. If CUDA available, verify GPU path: `cargo check --features gpu-cuda`
