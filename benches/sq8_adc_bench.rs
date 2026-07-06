//! Criterion benchmarks for SQ8 asymmetric-distance-code (ADC) kernels.
//!
//! Validates finding HQ-2 (tmp/VECTOR-DEEP-REVIEW.md): SIMD-dispatched ADC
//! stats (NEON/AVX2/AVX-512 widen-u8-to-f32 + FMA) beat the naive per-element
//! scalar `sq8_l2_adc` at standard embedding dimensions (128/384/768).
//!
//! Three variants compared per dimension:
//! - `naive_scalar`: the original per-candidate scalar loop (baseline).
//! - `stats_scalar`: the algebraic stats decomposition, scalar stats kernel
//!   (isolates the win from the ALGEBRA alone, no SIMD).
//! - `stats_dispatch`: the algebraic decomposition with the SIMD-dispatched
//!   stats kernel (NEON on aarch64, AVX2/AVX-512 on x86_64) — this is what
//!   the beam-search / brute-force hot paths now call.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use moon::vector::distance;
use moon::vector::turbo_quant::sq8::{
    sq8_candidate_stats_scalar, sq8_l2_adc, sq8_l2_from_stats, sq8_query_stats,
};
use std::hint::black_box;

fn make_f32_vector(dim: usize, seed: u64) -> Vec<f32> {
    let mut s = seed as u32;
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        s = s.wrapping_mul(1664525).wrapping_add(1013904223);
        v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

fn make_u8_codes(dim: usize, seed: u64) -> Vec<u8> {
    let mut s = seed as u32;
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        s = s.wrapping_mul(1664525).wrapping_add(1013904223);
        v.push((s >> 24) as u8);
    }
    v
}

const DIMS: &[usize] = &[128, 384, 768];

fn bench_sq8_adc(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("sq8_l2_adc");

    for &dim in DIMS {
        let query = make_f32_vector(dim, 42);
        let codes = make_u8_codes(dim, 99);
        let min = -0.5f32;
        let scale = 0.004f32;

        group.bench_with_input(BenchmarkId::new("naive_scalar", dim), &dim, |bench, _| {
            bench.iter(|| {
                sq8_l2_adc(
                    black_box(&query),
                    black_box(&codes),
                    black_box(min),
                    black_box(scale),
                )
            });
        });

        group.bench_with_input(BenchmarkId::new("stats_scalar", dim), &dim, |bench, _| {
            // Per-query stats hoisted outside the timed loop, matching how the
            // real beam-search / brute-force call sites use it.
            let (q_sum, q_sumsq) = sq8_query_stats(&query);
            bench.iter(|| {
                let (dot_qc, sum_c, sumsq_c) =
                    sq8_candidate_stats_scalar(black_box(&query), black_box(&codes));
                sq8_l2_from_stats(dim, min, scale, q_sum, q_sumsq, dot_qc, sum_c, sumsq_c)
            });
        });

        group.bench_with_input(BenchmarkId::new("stats_dispatch", dim), &dim, |bench, _| {
            let stats_fn = distance::table().sq8_stats;
            let (q_sum, q_sumsq) = sq8_query_stats(&query);
            bench.iter(|| {
                let (dot_qc, sum_c, sumsq_c) = stats_fn(black_box(&query), black_box(&codes));
                sq8_l2_from_stats(dim, min, scale, q_sum, q_sumsq, dot_qc, sum_c, sumsq_c)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_sq8_adc);
criterion_main!(benches);
