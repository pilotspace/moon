//! Criterion benchmarks for scalar vs SIMD distance kernels.
//!
//! Validates VEC-SIMD-02: SIMD dispatch achieves >=3x speedup over scalar
//! at standard embedding dimensions (384, 768, 1024).

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use moon::vector::distance;
use std::hint::black_box;

// ── Deterministic vector generators (LCG, seed-based) ──────────────────

fn make_f32_vectors(dim: usize, seed: u64) -> (Vec<f32>, Vec<f32>) {
    let mut s1 = seed as u32;
    let mut s2 = (seed.wrapping_mul(6364136223846793005)) as u32;
    let mut a = Vec::with_capacity(dim);
    let mut b = Vec::with_capacity(dim);
    for _ in 0..dim {
        s1 = s1.wrapping_mul(1664525).wrapping_add(1013904223);
        a.push((s1 as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        s2 = s2.wrapping_mul(1664525).wrapping_add(1013904223);
        b.push((s2 as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    (a, b)
}

fn make_i8_vectors(dim: usize, seed: u64) -> (Vec<i8>, Vec<i8>) {
    let mut s1 = seed as u32;
    let mut s2 = (seed.wrapping_mul(6364136223846793005)) as u32;
    let mut a = Vec::with_capacity(dim);
    let mut b = Vec::with_capacity(dim);
    for _ in 0..dim {
        s1 = s1.wrapping_mul(1664525).wrapping_add(1013904223);
        a.push((s1 >> 24) as i8);
        s2 = s2.wrapping_mul(1664525).wrapping_add(1013904223);
        b.push((s2 >> 24) as i8);
    }
    (a, b)
}

// ── Benchmark groups ───────────────────────────────────────────────────

const DIMS: &[usize] = &[128, 384, 768, 1024];
const TAIL_DIMS: &[usize] = &[1, 3, 13, 97, 100];

fn bench_l2_f32(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("l2_f32");

    for &dim in DIMS {
        let (a, b) = make_f32_vectors(dim, 42);
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| distance::scalar::l2_f32(black_box(&a), black_box(&b)));
        });
        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |bench, _| {
            bench.iter(|| (distance::table().l2_f32)(black_box(&a), black_box(&b)));
        });
    }
    group.finish();
}

fn bench_l2_i8(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("l2_i8");

    for &dim in DIMS {
        let (a, b) = make_i8_vectors(dim, 42);
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| distance::scalar::l2_i8(black_box(&a), black_box(&b)));
        });
        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |bench, _| {
            bench.iter(|| (distance::table().l2_i8)(black_box(&a), black_box(&b)));
        });
    }
    group.finish();
}

fn bench_dot_f32(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("dot_f32");

    for &dim in DIMS {
        let (a, b) = make_f32_vectors(dim, 42);
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| distance::scalar::dot_f32(black_box(&a), black_box(&b)));
        });
        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |bench, _| {
            bench.iter(|| (distance::table().dot_f32)(black_box(&a), black_box(&b)));
        });
    }
    group.finish();
}

fn bench_cosine_f32(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("cosine_f32");

    for &dim in DIMS {
        let (a, b) = make_f32_vectors(dim, 42);
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| distance::scalar::cosine_f32(black_box(&a), black_box(&b)));
        });
        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |bench, _| {
            bench.iter(|| (distance::table().cosine_f32)(black_box(&a), black_box(&b)));
        });
    }
    group.finish();
}

fn bench_l2_f32_tail(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("l2_f32_tail");

    for &dim in TAIL_DIMS {
        let (a, b) = make_f32_vectors(dim, 42);
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| distance::scalar::l2_f32(black_box(&a), black_box(&b)));
        });
        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |bench, _| {
            bench.iter(|| (distance::table().l2_f32)(black_box(&a), black_box(&b)));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_l2_f32,
    bench_l2_i8,
    bench_dot_f32,
    bench_cosine_f32,
    bench_l2_f32_tail
);
criterion_main!(benches);
