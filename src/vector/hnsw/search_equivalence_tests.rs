//! Old-vs-new equivalence tests for the HNSW search kernels
//! (moon#1193 budgeted 16-level ADC arm).
//!
//! Lives outside `search.rs` (already past the file-size ceiling). Builds one
//! deterministic TQ4 fixture and runs the SAME searches through the legacy
//! and the new code paths.

use crate::vector::distance;
use crate::vector::hnsw::build::HnswBuilder;
use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::hnsw::search::{LEGACY_BUDGETED_ADC, SearchScratch, hnsw_search};
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::encoder::encode_tq_mse_scaled;
use crate::vector::turbo_quant::fwht;
use crate::vector::types::DistanceMetric;

/// Clustered (low intrinsic dimension) vectors — closer to real embeddings
/// than i.i.d. noise, so neighbour gaps are small and ties are exercised.
fn clustered(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut s = seed;
    let mut next = move || {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let centers: Vec<Vec<f32>> = (0..16)
        .map(|_| (0..dim).map(|_| next()).collect())
        .collect();
    (0..n)
        .map(|i| {
            let c = &centers[i % centers.len()];
            c.iter().map(|&x| x + 0.35 * next()).collect()
        })
        .collect()
}

pub(crate) struct Fixture {
    pub vectors: Vec<Vec<f32>>,
    pub graph: HnswGraph,
    pub tq_bfs: Vec<u8>,
    pub collection: CollectionMetadata,
}

/// TQ4 fixture built the way `compact()` builds a light segment: codes at
/// insert time, HNSW over decoded-centroid L2, BFS-reordered code buffer.
pub(crate) fn build_fixture(n: usize, dim: usize, metric: DistanceMetric) -> Fixture {
    distance::init();
    fwht::init_fwht();
    let collection =
        CollectionMetadata::new(7, dim as u32, metric, QuantizationConfig::TurboQuant4, 99);
    let padded = collection.padded_dimension as usize;
    let signs = collection.fwht_sign_flips.as_slice();
    let bytes_per_code = padded / 2 + 4;
    let vectors = clustered(n, dim, 0xA5A5_1234);
    let mut work = vec![0.0f32; padded];
    let mut tq_orig = Vec::with_capacity(n * bytes_per_code);
    for v in &vectors {
        let code = encode_tq_mse_scaled(v, signs, collection.codebook_boundaries_15(), &mut work);
        tq_orig.extend_from_slice(&code.codes);
        tq_orig.extend_from_slice(&code.norm.to_le_bytes());
    }
    let codebook = collection.codebook_16();
    let decoded: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            let code = &tq_orig[i * bytes_per_code..i * bytes_per_code + padded / 2];
            let mut out = Vec::with_capacity(padded);
            for &b in code {
                out.push(codebook[(b & 0x0F) as usize]);
                out.push(codebook[(b >> 4) as usize]);
            }
            out
        })
        .collect();
    let dt = distance::table();
    let mut builder = HnswBuilder::new(16, 64, 4242);
    for _ in 0..n {
        builder.insert(|a: u32, b: u32| (dt.l2_f32)(&decoded[a as usize], &decoded[b as usize]));
    }
    let graph = builder.build(bytes_per_code as u32);
    let mut tq_bfs = vec![0u8; n * bytes_per_code];
    for bfs in 0..n {
        let orig = graph.to_original(bfs as u32) as usize;
        tq_bfs[bfs * bytes_per_code..(bfs + 1) * bytes_per_code]
            .copy_from_slice(&tq_orig[orig * bytes_per_code..(orig + 1) * bytes_per_code]);
    }
    Fixture {
        vectors,
        graph,
        tq_bfs,
        collection,
    }
}

/// Queries: perturbed data points (realistic near-duplicate recall shape).
pub(crate) fn queries(fx: &Fixture, count: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|i| {
            let base = &fx.vectors[(i * 37) % fx.vectors.len()];
            base.iter()
                .enumerate()
                .map(|(j, &x)| x + 0.05 * (((i * 31 + j * 17) % 13) as f32 / 13.0 - 0.5))
                .collect()
        })
        .collect()
}

fn run(fx: &Fixture, q: &[f32], k: usize, ef: usize, legacy: bool) -> Vec<(u32, f32)> {
    LEGACY_BUDGETED_ADC.with(|c| c.set(legacy));
    let mut scratch = SearchScratch::new(0, fx.collection.padded_dimension);
    let out = hnsw_search(
        &fx.graph,
        &fx.tq_bfs,
        q,
        &fx.collection,
        k,
        ef,
        &mut scratch,
    );
    LEGACY_BUDGETED_ADC.with(|c| c.set(false));
    out.iter().map(|r| (r.id.0, r.distance)).collect()
}

#[test]
fn budgeted_adc_rewrite_keeps_topk_identical_on_fixture() {
    // moon#1193: the 16-level budgeted arm (hot for WARM + sub-sign-less HOT
    // segments) was rewritten from a serial accumulator to 8 accumulators.
    // Summation order changed, so distances may move by f32 reassociation
    // error — the top-k (ids AND order) must not.
    for (dim, metric) in [
        (384usize, DistanceMetric::L2),
        (384, DistanceMetric::Cosine),
        (768, DistanceMetric::L2),
        (100, DistanceMetric::L2),    // padded 128: code_len 64
        (20, DistanceMetric::Cosine), // padded 32: code_len 16 (single block)
        (6, DistanceMetric::L2),      // padded 8: code_len 4 (tail only)
    ] {
        let n = if dim >= 768 { 300 } else { 500 };
        let fx = build_fixture(n, dim, metric);
        let mut compared = 0usize;
        for q in queries(&fx, 16) {
            for (k, ef) in [(10usize, 64usize), (1, 24), (32, 200)] {
                let old = run(&fx, &q, k, ef, true);
                let new = run(&fx, &q, k, ef, false);
                let old_ids: Vec<u32> = old.iter().map(|r| r.0).collect();
                let new_ids: Vec<u32> = new.iter().map(|r| r.0).collect();
                assert_eq!(
                    new_ids, old_ids,
                    "dim={dim} {metric:?} k={k} ef={ef}: top-k changed"
                );
                for (a, b) in new.iter().zip(old.iter()) {
                    let tol = 1e-5 * a.1.abs().max(b.1.abs()).max(1e-6);
                    assert!(
                        (a.1 - b.1).abs() <= tol,
                        "dim={dim}: distance {} vs {} beyond reassociation tolerance",
                        a.1,
                        b.1
                    );
                }
                compared += 1;
            }
        }
        assert_eq!(compared, 48);
    }
}
