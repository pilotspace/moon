//! Criterion benchmarks for HNSW build + search at multiple scales.
//!
//! Validates baseline performance: build throughput and search QPS
//! at dimensions (128d) and scales (1K, 5K, 10K).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

use moon::vector::distance;
use moon::vector::hnsw::build::HnswBuilder;
use moon::vector::hnsw::search::{hnsw_search, SearchScratch};
use moon::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use moon::vector::turbo_quant::encoder::{encode_tq_mse, padded_dimension};
use moon::vector::types::DistanceMetric;

// ── Deterministic vector generator (LCG, same pattern as distance_bench.rs) ──

fn make_f32_vector(dim: usize, seed: u32) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed;
    for _ in 0..dim {
        s = s.wrapping_mul(1664525).wrapping_add(1013904223);
        v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

/// Build a complete HNSW graph with TQ-encoded vectors for benchmarking search.
/// Returns (graph, vectors_tq buffer, collection metadata).
fn build_test_graph(
    n: u32,
    dim: usize,
) -> (
    moon::vector::hnsw::graph::HnswGraph,
    Vec<u8>,
    CollectionMetadata,
) {
    let padded = padded_dimension(dim as u32) as usize;
    let collection =
        CollectionMetadata::new(1, dim as u32, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42);

    // Generate and encode all vectors
    let mut tq_codes: Vec<Vec<u8>> = Vec::with_capacity(n as usize);
    let mut tq_norms: Vec<f32> = Vec::with_capacity(n as usize);
    let mut work_buf = vec![0.0f32; padded];

    for i in 0..n {
        let vec_f32 = make_f32_vector(dim, i * 7 + 13);
        let tq = encode_tq_mse(&vec_f32, collection.fwht_sign_flips.as_slice(), &mut work_buf);
        tq_codes.push(tq.codes);
        tq_norms.push(tq.norm);
    }

    // Build HNSW using pairwise L2 on raw f32 vectors for construction
    let vecs: Vec<Vec<f32>> = (0..n).map(|i| make_f32_vector(dim, i * 7 + 13)).collect();
    let mut builder = HnswBuilder::new(16, 200, 42);
    for _i in 0..n {
        builder.insert(|a, b| {
            let va = &vecs[a as usize];
            let vb = &vecs[b as usize];
            va.iter().zip(vb.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
        });
    }

    // bytes_per_code = padded_dim/2 (nibble-packed) + 4 (norm f32)
    let bytes_per_code = (padded / 2 + 4) as u32;
    let graph = builder.build(bytes_per_code);

    // Build the flat TQ buffer in BFS order
    let mut vectors_tq = vec![0u8; n as usize * bytes_per_code as usize];
    for orig_id in 0..n {
        let bfs_pos = graph.to_bfs(orig_id);
        let offset = bfs_pos as usize * bytes_per_code as usize;
        let code = &tq_codes[orig_id as usize];
        vectors_tq[offset..offset + code.len()].copy_from_slice(code);
        let norm_bytes = tq_norms[orig_id as usize].to_le_bytes();
        vectors_tq[offset + code.len()..offset + code.len() + 4].copy_from_slice(&norm_bytes);
    }

    (graph, vectors_tq, collection)
}

// ── Benchmark groups ──────────────────────────────────────────────────

const SCALES: &[u32] = &[1000, 5000, 10000];
const DIM: usize = 128;
const DIM_768: usize = 768;
const SCALES_768: &[u32] = &[1000, 5000, 10000];

fn bench_hnsw_build(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("hnsw_build");

    for &n in SCALES {
        let vecs: Vec<Vec<f32>> = (0..n).map(|i| make_f32_vector(DIM, i * 7 + 13)).collect();
        let padded = padded_dimension(DIM as u32) as usize;
        let bytes_per_code = (padded / 2 + 4) as u32;

        group.bench_with_input(BenchmarkId::new("build", n), &n, |bench, &n| {
            bench.iter(|| {
                let mut builder = HnswBuilder::new(16, 200, 42);
                for _i in 0..n {
                    builder.insert(|a, b| {
                        let va = &vecs[a as usize];
                        let vb = &vecs[b as usize];
                        va.iter().zip(vb.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
                    });
                }
                black_box(builder.build(bytes_per_code))
            });
        });
    }
    group.finish();
}

fn bench_hnsw_search(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("hnsw_search");

    for &n in SCALES {
        let (graph, vectors_tq, collection) = build_test_graph(n, DIM);
        let query = make_f32_vector(DIM, 999_999);
        let padded = padded_dimension(DIM as u32);
        let mut scratch = SearchScratch::new(n, padded);

        group.bench_with_input(BenchmarkId::new("search", n), &n, |bench, _| {
            bench.iter(|| {
                scratch.clear(n);
                let results = hnsw_search(
                    black_box(&graph),
                    black_box(&vectors_tq),
                    black_box(&query),
                    &collection,
                    10,
                    64,
                    &mut scratch,
                );
                black_box(results)
            });
        });
    }
    group.finish();
}

fn bench_hnsw_search_ef(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("hnsw_search_ef");

    let n = 5000u32;
    let (graph, vectors_tq, collection) = build_test_graph(n, DIM);
    let query = make_f32_vector(DIM, 999_999);
    let padded = padded_dimension(DIM as u32);
    let mut scratch = SearchScratch::new(n, padded);

    for &ef in &[32usize, 64, 128, 256] {
        group.bench_with_input(BenchmarkId::new("ef", ef), &ef, |bench, &ef| {
            bench.iter(|| {
                scratch.clear(n);
                let results = hnsw_search(
                    black_box(&graph),
                    black_box(&vectors_tq),
                    black_box(&query),
                    &collection,
                    10,
                    ef,
                    &mut scratch,
                );
                black_box(results)
            });
        });
    }
    group.finish();
}

fn bench_hnsw_build_768d(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("hnsw_build_768d");
    // 768d builds are substantially slower; extend measurement time
    group.measurement_time(std::time::Duration::from_secs(30));

    for &n in SCALES_768 {
        let vecs: Vec<Vec<f32>> = (0..n).map(|i| make_f32_vector(DIM_768, i * 7 + 13)).collect();
        let padded = padded_dimension(DIM_768 as u32) as usize;
        let bytes_per_code = (padded / 2 + 4) as u32;

        group.bench_with_input(BenchmarkId::new("build_768d", n), &n, |bench, &n| {
            bench.iter(|| {
                let mut builder = HnswBuilder::new(16, 200, 42);
                for _i in 0..n {
                    builder.insert(|a, b| {
                        let va = &vecs[a as usize];
                        let vb = &vecs[b as usize];
                        va.iter().zip(vb.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
                    });
                }
                black_box(builder.build(bytes_per_code))
            });
        });
    }
    group.finish();
}

fn bench_hnsw_search_768d(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("hnsw_search_768d");
    // 768d search uses larger TQ codes; extend measurement for stability
    group.measurement_time(std::time::Duration::from_secs(20));

    for &n in SCALES_768 {
        let (graph, vectors_tq, collection) = build_test_graph(n, DIM_768);
        let query = make_f32_vector(DIM_768, 999_999);
        let padded = padded_dimension(DIM_768 as u32);
        let mut scratch = SearchScratch::new(n, padded);

        group.bench_with_input(BenchmarkId::new("search_768d", n), &n, |bench, _| {
            bench.iter(|| {
                scratch.clear(n);
                let results = hnsw_search(
                    black_box(&graph),
                    black_box(&vectors_tq),
                    black_box(&query),
                    &collection,
                    10,
                    64,
                    &mut scratch,
                );
                black_box(results)
            });
        });
    }
    group.finish();
}

fn bench_hnsw_search_ef_768d(c: &mut Criterion) {
    distance::init();
    let mut group = c.benchmark_group("hnsw_search_ef_768d");
    group.measurement_time(std::time::Duration::from_secs(20));

    let n = 10000u32;
    let (graph, vectors_tq, collection) = build_test_graph(n, DIM_768);
    let query = make_f32_vector(DIM_768, 999_999);
    let padded = padded_dimension(DIM_768 as u32);
    let mut scratch = SearchScratch::new(n, padded);

    for &ef in &[32usize, 64, 128, 256] {
        group.bench_with_input(BenchmarkId::new("ef_768d", ef), &ef, |bench, &ef| {
            bench.iter(|| {
                scratch.clear(n);
                let results = hnsw_search(
                    black_box(&graph),
                    black_box(&vectors_tq),
                    black_box(&query),
                    &collection,
                    10,
                    ef,
                    &mut scratch,
                );
                black_box(results)
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_hnsw_build,
    bench_hnsw_search,
    bench_hnsw_search_ef,
    bench_hnsw_build_768d,
    bench_hnsw_search_768d,
    bench_hnsw_search_ef_768d
);
criterion_main!(benches);
