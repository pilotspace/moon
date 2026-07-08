//! Criterion benchmarks for the FastScan block kernels.
//!
//! Compares three ways of computing approximate TQ4 ADC distances for a
//! 32-vector batch:
//! - `block_scalar`: the scalar FastScan reference (interleaved layout, u8 LUT)
//! - `block_dispatch`: the SIMD FastScan kernel selected for this machine
//!   (NEON TBL on aarch64, AVX2 VPSHUFB on x86_64)
//! - `tq_adc_x32`: the current per-candidate scalar path (`tq_l2_adc_scaled`
//!   with an f32 LUT-free form) called 32 times — what HNSW beam search and
//!   the mutable-segment brute-force scan do today.
//!
//! Also measures `build_quantized_lut` (the per-query fixed cost).

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use moon::vector::distance::fastscan;
use moon::vector::segment::ivf::BLOCK_SIZE;
use moon::vector::turbo_quant::codebook::scaled_centroids;
use moon::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;
use std::hint::black_box;

/// Padded dimensions to benchmark (128 stays 128, 384 pads to 512, 768 to 1024).
const PADDED_DIMS: &[usize] = &[128, 512, 1024];

fn make_query(padded_dim: usize, seed: u32) -> Vec<f32> {
    let sigma = 1.0 / (padded_dim as f32).sqrt();
    let mut s = seed;
    let mut q = Vec::with_capacity(padded_dim);
    for _ in 0..padded_dim {
        s = s.wrapping_mul(1664525).wrapping_add(1013904223);
        q.push(((s as f32) / (u32::MAX as f32) * 4.0 - 2.0) * sigma);
    }
    q
}

/// Random nibble-packed codes for 32 vectors, both vector-major (for the
/// per-candidate path) and interleaved (for FastScan).
fn make_codes(dim_half: usize, seed: u32) -> (Vec<Vec<u8>>, Vec<u8>) {
    let mut s = seed;
    let mut per_vec = vec![vec![0u8; dim_half]; BLOCK_SIZE];
    for cv in per_vec.iter_mut() {
        for b in cv.iter_mut() {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            *b = (s >> 24) as u8;
        }
    }
    let mut interleaved = vec![0u8; dim_half * BLOCK_SIZE];
    for (v, cv) in per_vec.iter().enumerate() {
        for (d, &byte) in cv.iter().enumerate() {
            interleaved[d * BLOCK_SIZE + v] = byte;
        }
    }
    (per_vec, interleaved)
}

fn bench_fastscan_block(c: &mut Criterion) {
    fastscan::init_fastscan();
    let mut group = c.benchmark_group("fastscan_block_32vec");

    for &padded_dim in PADDED_DIMS {
        let dim_half = padded_dim / 2;
        let centroids = scaled_centroids(padded_dim as u32);
        let q = make_query(padded_dim, 42);
        let (per_vec, interleaved) = make_codes(dim_half, 7);

        let mut lut = vec![0u8; padded_dim * 16];
        let _params = fastscan::build_quantized_lut(&q, &centroids, &mut lut);

        group.bench_with_input(
            BenchmarkId::new("block_scalar", padded_dim),
            &padded_dim,
            |bench, _| {
                let mut results = [0u16; 32];
                bench.iter(|| {
                    fastscan::fastscan_block_scalar(
                        black_box(&interleaved),
                        black_box(&lut),
                        dim_half,
                        &mut results,
                    );
                    black_box(results[0])
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("block_dispatch", padded_dim),
            &padded_dim,
            |bench, _| {
                let scan = fastscan::fastscan_dispatch().scan_block;
                let mut results = [0u16; 32];
                bench.iter(|| {
                    scan(
                        black_box(&interleaved),
                        black_box(&lut),
                        dim_half,
                        &mut results,
                    );
                    black_box(results[0])
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tq_adc_x32", padded_dim),
            &padded_dim,
            |bench, _| {
                bench.iter(|| {
                    let mut acc = 0.0f32;
                    for cv in per_vec.iter() {
                        acc += tq_l2_adc_scaled(black_box(&q), black_box(cv), 1.0, &centroids);
                    }
                    black_box(acc)
                });
            },
        );
    }
    group.finish();
}

fn bench_build_lut(c: &mut Criterion) {
    let mut group = c.benchmark_group("fastscan_build_lut");

    for &padded_dim in PADDED_DIMS {
        let centroids = scaled_centroids(padded_dim as u32);
        let q = make_query(padded_dim, 42);
        let mut lut = vec![0u8; padded_dim * 16];

        group.bench_with_input(
            BenchmarkId::new("build_quantized_lut", padded_dim),
            &padded_dim,
            |bench, _| {
                bench.iter(|| {
                    let params = fastscan::build_quantized_lut(black_box(&q), &centroids, &mut lut);
                    black_box(params.scale)
                });
            },
        );
    }
    group.finish();
}

/// End-to-end mutable-segment scan: FastScan-filtered MVCC path vs the plain
/// per-candidate loop (`brute_force_search_filtered`, same TQ-ADC estimator).
fn bench_mutable_scan(c: &mut Criterion) {
    use moon::vector::segment::mutable::MutableSegment;
    use moon::vector::turbo_quant::collection::{
        BuildMode, CollectionMetadata, QuantizationConfig,
    };
    use moon::vector::types::DistanceMetric;
    use std::sync::Arc;

    moon::vector::distance::init();

    let mut group = c.benchmark_group("mutable_scan_topk");
    group.sample_size(30);

    for &(dim, n) in &[(128usize, 20_000u32), (768, 5_000)] {
        let col = Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
            BuildMode::Light,
        ));
        let seg = MutableSegment::new(dim as u32, col);
        let mut s = 7u32;
        let mut v = vec![0.0f32; dim];
        for i in 0..n {
            for x in v.iter_mut() {
                s = s.wrapping_mul(1664525).wrapping_add(1013904223);
                *x = (s as f32) / (u32::MAX as f32) * 2.0 - 1.0;
            }
            seg.append(i as u64, &v, i as u64 + 1);
        }
        let query = make_query(dim, 42);
        let committed = roaring::RoaringTreemap::new();

        group.bench_with_input(
            BenchmarkId::new(format!("fastscan_mvcc_d{dim}"), n),
            &n,
            |bench, _| {
                bench.iter(|| {
                    let r = seg.brute_force_search_mvcc(
                        black_box(&query),
                        None,
                        10,
                        None,
                        u64::MAX,
                        0,
                        &committed,
                        0,
                        n as usize,
                    );
                    black_box(r.len())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new(format!("plain_adc_d{dim}"), n),
            &n,
            |bench, _| {
                bench.iter(|| {
                    let r = seg.brute_force_search_filtered(black_box(&query), None, 10, None);
                    black_box(r.len())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_fastscan_block,
    bench_build_lut,
    bench_mutable_scan
);
criterion_main!(benches);
