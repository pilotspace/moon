//! HQ-1 (vector deep review 2026-07-05): exact rerank stage.
//!
//! Compacted (immutable) segments rank candidates purely by quantized ADC
//! distance — SQ8/TQ4 estimates with per-coordinate error around scale/2.
//! The exact-rerank sidecar keeps an f16 copy of each original vector and
//! re-scores the beam with (near-)exact distances before top-k truncation:
//! returned distances must match the true metric to f16 tolerance (~1e-3
//! relative), and recall@k against exact ground truth must be ~1.0.
//!
//! RED before the sidecar lands: returned distances are ADC estimates whose
//! relative error is dominated by quantization (≫ 1e-3), and recall@10 on the
//! seeded workload is measurably below 1.0.

use bytes::Bytes;

use moon::vector::distance;
use moon::vector::segment::compaction::MergeMode;
use moon::vector::store::{IndexMeta, VectorStore};
use moon::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::{DistanceMetric, SearchResult};

const DIM: usize = 32;

/// xorshift64* PRNG — deterministic, no rng dependency.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
    fn next_f32(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }
    fn vec(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.next_f32()).collect()
    }
}

fn make_meta(name: &str, quant: QuantizationConfig, metric: DistanceMetric) -> IndexMeta {
    IndexMeta {
        name: Bytes::from(name.to_owned()),
        dimension: DIM as u32,
        padded_dimension: padded_dimension(DIM as u32),
        metric,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 0,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: quant,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: MergeMode::GraphUnion,
        keep_raw: false,
        db_index: 0,
    }
}

/// Build a store with `n` seeded vectors in one compacted immutable segment.
fn build_compacted(
    name: &str,
    quant: QuantizationConfig,
    metric: DistanceMetric,
    n: usize,
    seed: u64,
) -> (VectorStore, Vec<Vec<f32>>) {
    distance::init();
    let mut store = VectorStore::new();
    store
        .create_index(make_meta(name, quant, metric))
        .expect("create_index");
    let mut rng = Rng::new(seed);
    let mut vecs = Vec::with_capacity(n);
    for i in 0..n {
        let v = rng.vec(DIM);
        let key = Bytes::from(format!("doc:{i}"));
        let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
        store
            .insert_vector(name.as_bytes(), &v, key_hash, key)
            .expect("insert");
        vecs.push(v);
    }
    store
        .force_compact_index(name.as_bytes())
        .expect("force_compact");
    assert_eq!(
        store.immutable_segment_count(name.as_bytes()),
        Some(1),
        "expected exactly one immutable segment"
    );
    (store, vecs)
}

fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn normalize(v: &[f32]) -> Vec<f32> {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        v.iter().map(|x| x / n).collect()
    } else {
        v.to_vec()
    }
}

fn segment_search(store: &mut VectorStore, name: &str, q: &[f32], k: usize) -> Vec<SearchResult> {
    let idx = store.get_index_mut(name.as_bytes()).expect("index");
    idx.segments
        .search(q, k, 128, &mut idx.scratch)
        .into_iter()
        .collect()
}

// ── 1. L2: returned distances are exact (f16 tolerance) ─────────────────────

#[test]
fn sq8_compacted_l2_distances_are_exact() {
    let (mut store, vecs) = build_compacted(
        "xr_l2",
        QuantizationConfig::Sq8,
        DistanceMetric::L2,
        64,
        0xA11CE,
    );
    let mut rng = Rng::new(0xBEEF);
    for _ in 0..5 {
        let q = rng.vec(DIM);
        let results = segment_search(&mut store, "xr_l2", &q, 10);
        assert!(!results.is_empty());
        for r in &results {
            let truth = l2_sq(&q, &vecs[r.id.0 as usize]);
            let rel = (r.distance - truth).abs() / truth.max(1e-6);
            assert!(
                rel < 1.5e-3,
                "id={} returned={} exact={} rel={rel} — distance is a quantized \
                 estimate, exact rerank missing",
                r.id.0,
                r.distance,
                truth
            );
        }
    }
}

// ── 2. Cosine: distances are exact normalized-pair L2² ──────────────────────

#[test]
fn sq8_compacted_cosine_distances_are_exact() {
    let (mut store, vecs) = build_compacted(
        "xr_cos",
        QuantizationConfig::Sq8,
        DistanceMetric::Cosine,
        64,
        0xC051,
    );
    let mut rng = Rng::new(0xF00D);
    for _ in 0..5 {
        let q = rng.vec(DIM);
        let qn = normalize(&q);
        let results = segment_search(&mut store, "xr_cos", &q, 10);
        assert!(!results.is_empty());
        for r in &results {
            let vn = normalize(&vecs[r.id.0 as usize]);
            let truth = l2_sq(&qn, &vn);
            let rel = (r.distance - truth).abs() / truth.max(1e-6);
            assert!(
                rel < 1.5e-3,
                "id={} returned={} exact={} rel={rel} (cosine convention: \
                 normalized-pair squared L2)",
                r.id.0,
                r.distance,
                truth
            );
        }
    }
}

// ── 2b. TQ4: the coarser 4-bit codes make the red dramatic (~percent-level) ──

#[test]
fn tq4_compacted_l2_distances_are_exact() {
    let (mut store, vecs) = build_compacted(
        "xr_tq4",
        QuantizationConfig::TurboQuant4,
        DistanceMetric::L2,
        64,
        0x71B4,
    );
    let mut rng = Rng::new(0xCAFE);
    for _ in 0..5 {
        let q = rng.vec(DIM);
        let results = segment_search(&mut store, "xr_tq4", &q, 10);
        assert!(!results.is_empty());
        for r in &results {
            let truth = l2_sq(&q, &vecs[r.id.0 as usize]);
            let rel = (r.distance - truth).abs() / truth.max(1e-6);
            assert!(
                rel < 5e-3,
                "TQ4 id={} returned={} exact={} rel={rel} — distance is a \
                 quantized estimate, exact rerank missing",
                r.id.0,
                r.distance,
                truth
            );
        }
    }
}

// ── 3. Recall@10 vs exact ground truth ───────────────────────────────────────

#[test]
fn sq8_rerank_recall_at_10_near_perfect() {
    // 200 vectors / 20 queries, all seeded. Pre-rerank SQ8 ADC misranks some
    // near-ties (quantization error scale/2 per coord); with exact rerank the
    // only residual error is f16 rounding (~1e-3 relative), far below typical
    // neighbor gaps on this workload.
    let (mut store, vecs) = build_compacted(
        "xr_rec",
        QuantizationConfig::Sq8,
        DistanceMetric::L2,
        200,
        0x5EED,
    );
    let mut rng = Rng::new(0xDEAD);
    let mut hit = 0usize;
    let mut total = 0usize;
    for _ in 0..20 {
        let q = rng.vec(DIM);
        let mut truth: Vec<(f32, u32)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (l2_sq(&q, v), i as u32))
            .collect();
        truth.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let truth_ids: std::collections::HashSet<u32> =
            truth[..10].iter().map(|(_, i)| *i).collect();
        let results = segment_search(&mut store, "xr_rec", &q, 10);
        total += 10;
        hit += results
            .iter()
            .filter(|r| truth_ids.contains(&r.id.0))
            .count();
    }
    let recall = hit as f32 / total as f32;
    assert!(
        recall >= 0.99,
        "recall@10 = {recall} (pre-rerank SQ8 ADC baseline measured ~0.9x on \
         this seed; exact rerank must reach ≥0.99)"
    );
}

// ── 4. Persistence: sidecar survives segment write/read roundtrip ───────────

#[test]
fn rerank_sidecar_survives_persistence_roundtrip() {
    use moon::vector::persistence::segment_io::{read_immutable_segment, write_immutable_segment};

    let (mut store, vecs) = build_compacted(
        "xr_per",
        QuantizationConfig::Sq8,
        DistanceMetric::L2,
        64,
        0x9E12,
    );

    let dir = std::env::temp_dir().join(format!("moon-xr-per-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Write the compacted segment, then read it back fresh.
    {
        let idx = store.get_index_mut(b"xr_per").expect("index");
        let snap = idx.segments.load();
        write_immutable_segment(&dir, 7, &snap.immutable[0], &idx.collection)
            .expect("write segment");
    }
    let (seg, _meta) = read_immutable_segment(&dir, 7).expect("read segment");

    let mut rng = Rng::new(0x0DDB);
    let q = rng.vec(DIM);
    let mut scratch = moon::vector::hnsw::search::SearchScratch::new(seg.total_count(), 64);
    let results = seg.search(&q, 10, 128, &mut scratch);
    assert!(!results.is_empty());
    for r in &results {
        let truth = l2_sq(&q, &vecs[r.id.0 as usize]);
        let rel = (r.distance - truth).abs() / truth.max(1e-6);
        assert!(
            rel < 1.5e-3,
            "after disk roundtrip: id={} returned={} exact={} rel={rel} — \
             sidecar not persisted",
            r.id.0,
            r.distance,
            truth
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 5. Legacy segment dirs (no sidecar file) still load and search ──────────

#[test]
fn legacy_segment_without_sidecar_still_searches() {
    use moon::vector::persistence::segment_io::{read_immutable_segment, write_immutable_segment};

    let (mut store, _vecs) = build_compacted(
        "xr_leg",
        QuantizationConfig::Sq8,
        DistanceMetric::L2,
        32,
        0x1E64,
    );

    let dir = std::env::temp_dir().join(format!("moon-xr-leg-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let idx = store.get_index_mut(b"xr_leg").expect("index");
        let snap = idx.segments.load();
        write_immutable_segment(&dir, 3, &snap.immutable[0], &idx.collection).expect("write");
    }
    // Simulate a pre-sidecar segment directory.
    let _ = std::fs::remove_file(dir.join("segment-3").join("raw_f16.bin"));

    let (seg, _meta) = read_immutable_segment(&dir, 3).expect("read legacy segment");
    let mut scratch = moon::vector::hnsw::search::SearchScratch::new(seg.total_count(), 64);
    let mut rng = Rng::new(0x7E57);
    let q = rng.vec(DIM);
    let results = seg.search(&q, 5, 64, &mut scratch);
    assert!(
        !results.is_empty(),
        "legacy segment without sidecar must still search (ADC distances)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 6. GraphUnion merge preserves the sidecar ────────────────────────────────

#[test]
fn graph_union_merge_preserves_exact_distances() {
    distance::init();
    let mut store = VectorStore::new();
    store
        .create_index(make_meta(
            "xr_mrg",
            QuantizationConfig::Sq8,
            DistanceMetric::L2,
        ))
        .expect("create_index");

    let mut rng = Rng::new(0x4E46);
    let mut vecs = Vec::new();
    for seg in 0..3 {
        for i in 0..40 {
            let v = rng.vec(DIM);
            let key = Bytes::from(format!("doc:{}", seg * 40 + i));
            let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
            store
                .insert_vector(b"xr_mrg", &v, key_hash, key)
                .expect("insert");
            vecs.push(v);
        }
        store.force_compact_index(b"xr_mrg").expect("compact");
    }
    assert_eq!(store.immutable_segment_count(b"xr_mrg"), Some(3));

    store
        .force_merge_index_with_tolerance(b"xr_mrg", 0.5)
        .expect("merge");
    assert_eq!(store.immutable_segment_count(b"xr_mrg"), Some(1));

    // Post-merge ids are remapped; match results by key_hash instead.
    let key_hash_to_pos: std::collections::HashMap<u64, usize> = (0..vecs.len())
        .map(|i| {
            let key = format!("doc:{i}");
            (xxhash_rust::xxh64::xxh64(key.as_bytes(), 0), i)
        })
        .collect();

    let mut qrng = Rng::new(0xAB1E);
    let q = qrng.vec(DIM);
    let results = segment_search(&mut store, "xr_mrg", &q, 10);
    assert!(!results.is_empty());
    for r in &results {
        let pos = key_hash_to_pos[&r.key_hash];
        let truth = l2_sq(&q, &vecs[pos]);
        let rel = (r.distance - truth).abs() / truth.max(1e-6);
        assert!(
            rel < 1.5e-3,
            "after merge: key pos={pos} returned={} exact={} rel={rel} — merged \
             segment lost the rerank sidecar",
            r.distance,
            truth
        );
    }
}

// ── 8. Tombstones must not consume the exact-rerank budget ──────────────────

#[test]
fn tombstoned_candidates_do_not_eat_rerank_budget() {
    // Regression: rerank_exact used to run BEFORE the liveness filter, so the
    // top-4·k ADC candidates it re-scored could all be tombstones — the live
    // results that survived the filter kept quantized ADC estimates (and the
    // post-rerank sort mixed exact and ADC scores). Deleting the 30 nearest
    // vectors with k=5 (budget 20) makes that failure deterministic.
    let (mut store, vecs) = build_compacted(
        "xr_tomb",
        QuantizationConfig::Sq8,
        DistanceMetric::L2,
        64,
        0x70B5,
    );
    let mut rng = Rng::new(0xD00D);
    let q = rng.vec(DIM);

    let mut order: Vec<usize> = (0..vecs.len()).collect();
    order.sort_by(|&a, &b| l2_sq(&q, &vecs[a]).total_cmp(&l2_sq(&q, &vecs[b])));
    let deleted: std::collections::HashSet<usize> = order[..30].iter().copied().collect();
    for &i in &deleted {
        store.mark_deleted_for_key(format!("doc:{i}").as_bytes());
    }

    let results = segment_search(&mut store, "xr_tomb", &q, 5);
    assert_eq!(results.len(), 5, "expected k live results");
    for r in &results {
        let id = r.id.0 as usize;
        assert!(!deleted.contains(&id), "tombstoned id {id} returned");
        let truth = l2_sq(&q, &vecs[id]);
        let rel = (r.distance - truth).abs() / truth.max(1e-6);
        assert!(
            rel < 1.5e-3,
            "id={id} returned={} exact={truth} rel={rel} — live candidate kept \
             its ADC estimate: tombstones consumed the rerank budget",
            r.distance
        );
    }
    // The 5 results must be the true 5 nearest live vectors, in order.
    let expect: Vec<usize> = order[30..35].to_vec();
    let got: Vec<usize> = results.iter().map(|r| r.id.0 as usize).collect();
    assert_eq!(got, expect, "live top-k mismatch after tombstone filtering");
}
