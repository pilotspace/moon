//! Criterion benchmarks for FWHT transform and TQ encoding pipelines.
//!
//! Measures scalar vs dispatched FWHT at standard embedding dimensions
//! (128, 256, 512, 768, 1024) and full randomized FWHT pipeline.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

use moon::vector::turbo_quant::fwht;

// ── Deterministic vector generator ────────────────────────────────────

fn make_f32_data(dim: usize, seed: u32) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed;
    for _ in 0..dim {
        s = s.wrapping_mul(1664525).wrapping_add(1013904223);
        v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

fn make_sign_flips(dim: usize, seed: u64) -> Vec<f32> {
    let mut flips = Vec::with_capacity(dim);
    let mut state = seed;
    for _ in 0..dim {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        flips.push(if (state >> 63) == 0 { 1.0 } else { -1.0 });
    }
    flips
}

// ── Benchmark groups ──────────────────────────────────────────────────

const DIMS: &[usize] = &[128, 256, 512, 768, 1024];
const SEARCH_DIMS: &[usize] = &[128, 384, 768];

fn bench_fwht_transform(c: &mut Criterion) {
    fwht::init_fwht();
    let mut group = c.benchmark_group("fwht_transform");

    for &dim in DIMS {
        // FWHT requires power-of-2 dimensions
        let padded = dim.next_power_of_two();
        let sign_flips = make_sign_flips(padded, 42);

        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            let mut data = make_f32_data(padded, 99);
            bench.iter(|| {
                // Reset data each iteration (FWHT is destructive)
                for (i, v) in data.iter_mut().enumerate() {
                    *v = (i as f32) * 0.001 - 0.5;
                }
                fwht::randomized_fwht_scalar(black_box(&mut data), black_box(&sign_flips));
                black_box(&data);
            });
        });

        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |bench, _| {
            let mut data = make_f32_data(padded, 99);
            bench.iter(|| {
                for (i, v) in data.iter_mut().enumerate() {
                    *v = (i as f32) * 0.001 - 0.5;
                }
                fwht::fwht(black_box(&mut data), black_box(&sign_flips));
                black_box(&data);
            });
        });
    }
    group.finish();
}

fn bench_randomized_fwht(c: &mut Criterion) {
    fwht::init_fwht();
    let mut group = c.benchmark_group("randomized_fwht");

    for &dim in SEARCH_DIMS {
        let padded = dim.next_power_of_two();
        let sign_flips = make_sign_flips(padded, 42);

        group.bench_with_input(
            BenchmarkId::new("full_pipeline", dim),
            &dim,
            |bench, _| {
                let mut data = make_f32_data(padded, 99);
                bench.iter(|| {
                    for (i, v) in data.iter_mut().enumerate() {
                        *v = (i as f32) * 0.001 - 0.5;
                    }
                    fwht::randomized_fwht_scalar(black_box(&mut data), black_box(&sign_flips));
                    black_box(&data);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_fwht_transform, bench_randomized_fwht);
criterion_main!(benches);
