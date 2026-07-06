//! Criterion benchmarks for SQ8 asymmetric-distance-code (ADC) kernels.
//!
//! Validates finding HQ-2 (tmp/VECTOR-DEEP-REVIEW.md): SIMD-dispatched ADC
//! stats (NEON/AVX2/AVX-512 widen-u8-to-f32 + FMA) beat the naive per-element
//! scalar `sq8_l2_adc` at standard embedding dimensions (128/384/768).
//!
//! Four variants compared per dimension:
//! - `naive_scalar`: the original per-candidate scalar loop (baseline).
//! - `stats_scalar`: the algebraic stats decomposition, scalar stats kernel
//!   (isolates the win from the ALGEBRA alone, no SIMD).
//! - `stats_dispatch`: the algebraic decomposition with the SIMD-dispatched
//!   f32 stats kernel (NEON on aarch64, AVX2/AVX-512 on x86_64) — this is
//!   what the beam-search / brute-force hot paths called before task #13.
//! - `int8_dispatch`: task #13 — the SIMD-dispatched INT8 symmetric ADC
//!   stats kernel (per-query quantization hoisted outside the timed loop,
//!   matching the real call sites in `hnsw/search.rs` /
//!   `segment/mutable.rs`). `None` (no int8 tier for this CPU/build, or
//!   `MOON_SQ8_INT8_ADC=0`) falls back silently to `stats_dispatch`'s f32
//!   kernel so the group always has 4 comparable bars.
//!
//! Local numbers only (per tmp/INT8-ADC-CONTEXT.md: no production claims
//! from a Mac dev box — GCE cross-arch validation is task #14).

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use moon::vector::distance;
use moon::vector::turbo_quant::sq8::{
    SQ8_INT8_QMAX, sq8_candidate_stats_scalar, sq8_int8_dot_to_f32, sq8_l2_adc, sq8_l2_from_stats,
    sq8_quantize_query_scalar, sq8_query_stats,
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

        group.bench_with_input(BenchmarkId::new("int8_dispatch", dim), &dim, |bench, _| {
            let (q_sum, q_sumsq) = sq8_query_stats(&query);
            let f32_stats_fn = distance::table().sq8_stats;
            match distance::table().sq8_i8_stats {
                Some(i8_stats_fn) => {
                    // Per-query quantization hoisted outside the timed loop,
                    // matching the real hnsw/search.rs and segment/mutable.rs
                    // call sites (quantize once per search, not per candidate).
                    let mut qi8 = vec![0i8; dim];
                    let (q_scale, sum_qi8) =
                        sq8_quantize_query_scalar(&query, SQ8_INT8_QMAX, &mut qi8);
                    bench.iter(|| {
                        let (dot_int, sum_c_int, sumsq_c_int) =
                            i8_stats_fn(black_box(&qi8), black_box(&codes), sum_qi8);
                        let dot_qc = sq8_int8_dot_to_f32(q_scale, dot_int);
                        sq8_l2_from_stats(
                            dim,
                            min,
                            scale,
                            q_sum,
                            q_sumsq,
                            dot_qc,
                            sum_c_int as f32,
                            sumsq_c_int as f32,
                        )
                    });
                }
                None => {
                    // No int8 SIMD tier for this CPU/build (or
                    // MOON_SQ8_INT8_ADC=0) — fall back to the f32 dispatch
                    // path so the group always reports 4 comparable bars.
                    bench.iter(|| {
                        let (dot_qc, sum_c, sumsq_c) =
                            f32_stats_fn(black_box(&query), black_box(&codes));
                        sq8_l2_from_stats(dim, min, scale, q_sum, q_sumsq, dot_qc, sum_c, sumsq_c)
                    });
                }
            }
        });
    }
    group.finish();
}

criterion_group!(benches, bench_sq8_adc);
criterion_main!(benches);
