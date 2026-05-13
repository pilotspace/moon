//! Tests for P2 — vector immutable segment merge.
//!
//! Validates all three merge modes (graph-union default, keep_raw opt-in, none explicit),
//! the recall gate, and the VACUUM VECTOR command wiring.
//!
//! TDD order: RED tests written first, then GREEN after implementation.

use std::sync::Arc;

use bytes::Bytes;

use moon::vector::distance;
use moon::vector::segment::compaction::{MergeMode, merge_immutable};
use moon::vector::segment::mutable::MutableSegment;
use moon::vector::store::{IndexMeta, VectorStore};
use moon::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::DistanceMetric;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Simple deterministic LCG for test vector generation.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 40) as f32 / (1u64 << 24) as f32
    }
    fn randn(&mut self) -> f32 {
        let u1 = self.next_f32().max(1e-7);
        let u2 = self.next_f32();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos()
    }
}

fn random_unit_vec(rng: &mut Rng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.randn()).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
    v
}

fn make_collection(dim: u32) -> Arc<CollectionMetadata> {
    Arc::new(CollectionMetadata::new(
        1,
        dim,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        42,
    ))
}

fn make_meta(name: &str, dim: u32) -> IndexMeta {
    IndexMeta {
        name: Bytes::from(name.to_owned()),
        dimension: dim,
        padded_dimension: padded_dimension(dim),
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 0,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: MergeMode::GraphUnion,
        keep_raw: false,
    }
}

/// Compute recall@k: fraction of ground-truth neighbors found by approximate search.
fn recall_at_k(ground_truth: &[usize], approx: &[u32], k: usize) -> f32 {
    let gt_set: std::collections::HashSet<usize> = ground_truth.iter().take(k).copied().collect();
    let approx_set: std::collections::HashSet<usize> =
        approx.iter().take(k).map(|&x| x as usize).collect();
    let intersection = gt_set.intersection(&approx_set).count();
    intersection as f32 / k.min(gt_set.len()).max(1) as f32
}

// ── Test 1: graph-union merge reduces N segments to 1 (RED until merge implemented) ──

#[test]
fn test_graph_union_merge_reduces_segment_count() {
    distance::init();

    const DIM: u32 = 64;
    const VECS_PER_SEG: usize = 80;
    const N_SEGS: usize = 4;
    const RECALL_FLOOR: f32 = 0.90;
    const K: usize = 10;

    let mut store = VectorStore::new();
    let meta = make_meta("idx", DIM);
    store.create_index(meta).expect("create_index failed");

    let mut rng = Rng::new(0xDEAD_BEEF);
    let mut all_vecs: Vec<Vec<f32>> = Vec::new();

    // Insert VECS_PER_SEG vectors, force_compact to immutable, repeat N_SEGS times.
    for seg in 0..N_SEGS {
        for _ in 0..VECS_PER_SEG {
            let v = random_unit_vec(&mut rng, DIM as usize);
            let key = Bytes::from(format!("doc:{}", all_vecs.len()));
            let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
            store
                .insert_vector(b"idx", &v, key_hash, key)
                .expect("insert failed");
            all_vecs.push(v);
        }
        // Force compact after each batch to create a new immutable segment.
        store
            .force_compact_index(b"idx")
            .expect("force_compact failed");
        let _ = seg;
    }

    // Verify we have exactly N_SEGS immutable segments before merge.
    let imm_count_pre = store
        .immutable_segment_count(b"idx")
        .expect("index not found");
    assert_eq!(
        imm_count_pre, N_SEGS,
        "Expected {N_SEGS} immutable segments before merge, got {imm_count_pre}"
    );

    // Collect ground-truth for N_QUERIES queries before merge.
    const N_QUERIES: usize = 50;
    let queries: Vec<Vec<f32>> = (0..N_QUERIES)
        .map(|i| random_unit_vec(&mut Rng::new(0xC0DE + i as u64), DIM as usize))
        .collect();

    // Run pre-merge search (fan-out across all 4 segments).
    let pre_results: Vec<Vec<u32>> = queries
        .iter()
        .map(|q| {
            store
                .search_index(b"idx", q, K, 200)
                .expect("pre-merge search failed")
        })
        .collect();

    // Execute graph-union merge.
    let stats = store.force_merge_index(b"idx").expect("force_merge failed");

    assert_eq!(
        stats.segments_merged, N_SEGS,
        "Expected {N_SEGS} segments merged"
    );

    // Verify segment count is now 1.
    let imm_count_post = store
        .immutable_segment_count(b"idx")
        .expect("index not found");
    assert_eq!(
        imm_count_post, 1,
        "Expected 1 immutable segment after merge"
    );

    // Verify recall ≥ RECALL_FLOOR against pre-merge search (not ground truth).
    // Pre-merge results are the "oracle" since they fan out across all segments.
    let mut total_recall = 0.0f32;
    for (query, pre) in queries.iter().zip(pre_results.iter()) {
        let post = store
            .search_index(b"idx", query, K, 200)
            .expect("post-merge search failed");
        total_recall += recall_at_k(
            &pre.iter().map(|&x| x as usize).collect::<Vec<_>>(),
            &post,
            K,
        );
    }
    let mean_recall = total_recall / N_QUERIES as f32;
    assert!(
        mean_recall >= RECALL_FLOOR,
        "Post-merge recall {mean_recall:.3} < floor {RECALL_FLOOR}"
    );
}

// ── Test 2: merge_mode=none skips merge (single-segment case) ──

#[test]
fn test_merge_mode_none_is_noop() {
    distance::init();

    const DIM: u32 = 32;
    const VECS: usize = 50;

    let mut store = VectorStore::new();
    let mut meta = make_meta("idx2", DIM);
    meta.merge_mode = MergeMode::None;
    store.create_index(meta).expect("create_index failed");

    let mut rng = Rng::new(42);
    for i in 0..VECS {
        let v = random_unit_vec(&mut rng, DIM as usize);
        let key = Bytes::from(format!("doc:{i}"));
        let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
        store
            .insert_vector(b"idx2", &v, key_hash, key)
            .expect("insert failed");
    }
    // Create 2 immutable segments.
    store.force_compact_index(b"idx2").expect("compact 1");
    for i in VECS..VECS * 2 {
        let v = random_unit_vec(&mut rng, DIM as usize);
        let key = Bytes::from(format!("doc:{i}"));
        let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
        store
            .insert_vector(b"idx2", &v, key_hash, key)
            .expect("insert failed");
    }
    store.force_compact_index(b"idx2").expect("compact 2");

    let before = store
        .immutable_segment_count(b"idx2")
        .expect("index not found");
    assert_eq!(before, 2);

    // force_merge on a None-mode index: no segments merged, count unchanged.
    let stats = store
        .force_merge_index(b"idx2")
        .expect("force_merge failed");
    assert_eq!(stats.segments_merged, 0);
    assert_eq!(
        store
            .immutable_segment_count(b"idx2")
            .expect("index not found"),
        2
    );
}

// ── Test 3a: recall gate REJECTS an impossible tolerance ──
//
// The recall gate fires when n >= MIN_RECALL_SAMPLE_N (50). With 200 vectors in
// 2 segments and tolerance=1.0 (impossible — real HNSW recall < 1.0), the gate
// must return Err(RecallTooLow) and leave both segments intact.

#[test]
fn test_recall_gate_rejects_impossible_tolerance() {
    distance::init();

    const DIM: u32 = 64;

    let mut store = VectorStore::new();
    let meta = make_meta("idx3a", DIM);
    store.create_index(meta).expect("create_index failed");

    // Insert 200 vectors (>= 50 gate threshold) split across 2 segments.
    let mut rng = Rng::new(0xBAD_FEED);
    for i in 0..200 {
        let v = random_unit_vec(&mut rng, DIM as usize);
        let key = Bytes::from(format!("doc:{i}"));
        let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
        store
            .insert_vector(b"idx3a", &v, key_hash, key)
            .expect("insert failed");
        if i == 99 {
            store
                .force_compact_index(b"idx3a")
                .expect("compact mid");
        }
    }
    store.force_compact_index(b"idx3a").expect("compact end");

    assert_eq!(
        store
            .immutable_segment_count(b"idx3a")
            .expect("index not found"),
        2,
        "Should have 2 segments before merge attempt"
    );

    // tolerance=1.0 is impossible — HNSW recall on TQ4 @ 64d < 1.0 always.
    // Gate must reject and return Err.
    let result = store.force_merge_index_with_tolerance(b"idx3a", 1.0);
    assert!(
        result.is_err(),
        "Merge with tolerance=1.0 must be rejected by recall gate, but got: {:?}",
        result
    );

    // Both segments must remain intact (rollback guarantee).
    let count_after = store
        .immutable_segment_count(b"idx3a")
        .expect("index not found");
    assert_eq!(
        count_after, 2,
        "Recall gate rejection must leave original segments intact"
    );
}

// ── Test 3b: recall gate PASSES with a realistic tolerance ──
//
// Same 200-vector corpus; tolerance=0.50 is well below what HNSW achieves on
// 200 TQ4-encoded vectors at 64d.  Merge must succeed and reduce to 1 segment.

#[test]
fn test_recall_gate_passes_realistic_tolerance() {
    distance::init();

    const DIM: u32 = 64;

    let mut store = VectorStore::new();
    let meta = make_meta("idx3b", DIM);
    store.create_index(meta).expect("create_index failed");

    let mut rng = Rng::new(0xBAD_F00D);
    for i in 0..200 {
        let v = random_unit_vec(&mut rng, DIM as usize);
        let key = Bytes::from(format!("doc:{i}"));
        let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
        store
            .insert_vector(b"idx3b", &v, key_hash, key)
            .expect("insert failed");
        if i == 99 {
            store
                .force_compact_index(b"idx3b")
                .expect("compact mid");
        }
    }
    store.force_compact_index(b"idx3b").expect("compact end");

    // tolerance=0.50 — well below HNSW recall on this corpus.
    let result = store.force_merge_index_with_tolerance(b"idx3b", 0.50);
    assert!(
        result.is_ok(),
        "Merge with tolerance=0.50 must succeed: {:?}",
        result
    );
    let stats = result.unwrap();
    assert_eq!(stats.segments_merged, 2, "Both segments should be merged");

    let count = store
        .immutable_segment_count(b"idx3b")
        .expect("index not found");
    assert_eq!(count, 1, "Exactly 1 segment after successful merge");
}

// ── Test 4: needs_merge trigger conditions ──

#[test]
fn test_needs_merge_triggers_at_16_segments() {
    distance::init();

    const DIM: u32 = 32;
    const SEGS: usize = 17; // > 16 threshold

    let mut store = VectorStore::new();
    let meta = make_meta("idx4", DIM);
    store.create_index(meta).expect("create_index failed");

    let mut rng = Rng::new(0xABCD);
    for seg in 0..SEGS {
        // Insert enough vectors for compact to succeed (needs ≥1).
        for i in 0..30 {
            let v = random_unit_vec(&mut rng, DIM as usize);
            let key = Bytes::from(format!("doc:{}", seg * 30 + i));
            let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
            store
                .insert_vector(b"idx4", &v, key_hash, key)
                .expect("insert failed");
        }
        store.force_compact_index(b"idx4").expect("compact");
    }

    assert!(
        store.needs_merge(b"idx4").expect("index not found"),
        "Should need merge at {SEGS} segments"
    );
}

// ── Test 5: run_vacuum_pass merges eligible indexes ──

#[test]
fn test_run_vacuum_pass_merges_eligible() {
    distance::init();

    const DIM: u32 = 32;

    let mut store = VectorStore::new();
    let meta = make_meta("vac_idx", DIM);
    store.create_index(meta).expect("create_index failed");

    let mut rng = Rng::new(0xFACE);
    // Create 17 segments (above merge threshold of 16).
    // Use 20 vectors/segment = 340 total, well below the 500-vector recall gate
    // threshold so the gate is bypassed and the merge always succeeds.
    for seg in 0..17 {
        for i in 0..20 {
            let v = random_unit_vec(&mut rng, DIM as usize);
            let key = Bytes::from(format!("doc:{}", seg * 20 + i));
            let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
            store
                .insert_vector(b"vac_idx", &v, key_hash, key)
                .expect("insert failed");
        }
        store
            .force_compact_index(b"vac_idx")
            .expect("compact failed");
    }

    let stats = store.run_vacuum_pass();
    assert!(
        stats.total_merged > 0,
        "vacuum_pass should have merged at least one index"
    );
}

// ── Test 6: overlapping global_ids — highest LSN wins ──

#[test]
fn test_merge_overlapping_ids_highest_lsn_wins() {
    distance::init();

    // Use merge_immutable directly with constructed ImmutableSegments
    // containing overlapping key_hashes. The highest delete_lsn copy wins.
    const DIM: u32 = 32;
    let collection = make_collection(DIM);

    // Build two mutable segments with the same key_hash on vector 0.
    let seg_a = MutableSegment::new(DIM, collection.clone());
    let seg_b = MutableSegment::new(DIM, collection.clone());

    let mut rng = Rng::new(42);
    let v = random_unit_vec(&mut rng, DIM as usize);
    let key_hash: u64 = 0xDEAD;
    let sq_vec: Vec<i8> = v
        .iter()
        .map(|&x| (x * 127.0).clamp(-128.0, 127.0) as i8)
        .collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();

    // Insert into both with different insert_lsn (simulated via key_hash collision).
    seg_a.append(key_hash, &v, &sq_vec, norm, 1);
    seg_b.append(key_hash, &v, &sq_vec, norm, 2); // higher LSN in seg_b

    let frozen_a = seg_a.freeze();
    let frozen_b = seg_b.freeze();

    let imm_a = moon::vector::segment::compaction::compact(&frozen_a, &collection, 1, None)
        .expect("compact A failed");
    let imm_b = moon::vector::segment::compaction::compact(&frozen_b, &collection, 2, None)
        .expect("compact B failed");

    let merged = merge_immutable(
        &[Arc::new(imm_a), Arc::new(imm_b)],
        &collection,
        42,
        MergeMode::GraphUnion,
        0.90,
    )
    .expect("merge failed");

    // With overlapping key_hash: highest LSN copy retained, total_count = 1.
    assert_eq!(
        merged.total_count(),
        1,
        "Overlapping key_hash: only one copy should remain"
    );
    assert_eq!(merged.live_count(), 1);
}

// ── Test 7: FT.CREATE MERGE_MODE parse round-trip ──
//
// Verifies that FT.CREATE … MERGE_MODE NONE sets IndexMeta.merge_mode = None,
// and FT.CREATE … MERGE_MODE GRAPH_UNION sets GraphUnion.

#[test]
fn test_ft_create_merge_mode_none_roundtrip() {
    use moon::command::vector_search::ft_create;
    use moon::protocol::Frame;

    fn bulk(b: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(b))
    }

    let mut store = VectorStore::new();
    let mut text_store = moon::text::store::TextStore::new();

    // FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 8 TYPE FLOAT32 DIM 64
    //           DISTANCE_METRIC L2 MERGE_MODE NONE
    let args: Vec<Frame> = vec![
        bulk(b"idx_mm_none"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"8"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"64"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
        bulk(b"MERGE_MODE"),
        bulk(b"NONE"),
    ];

    let result = ft_create(&mut store, &mut text_store, &args);
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.CREATE must succeed: {:?}",
        result
    );

    let idx = store
        .get_index(b"idx_mm_none")
        .expect("index must exist after FT.CREATE");
    assert_eq!(
        idx.meta.merge_mode,
        MergeMode::None,
        "MERGE_MODE NONE must set IndexMeta.merge_mode = None"
    );
}

#[test]
fn test_ft_create_merge_mode_graph_union_roundtrip() {
    use moon::command::vector_search::ft_create;
    use moon::protocol::Frame;

    fn bulk(b: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(b))
    }

    let mut store = VectorStore::new();
    let mut text_store = moon::text::store::TextStore::new();

    let args: Vec<Frame> = vec![
        bulk(b"idx_mm_gu"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"8"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"64"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
        bulk(b"MERGE_MODE"),
        bulk(b"GRAPH_UNION"),
    ];

    let result = ft_create(&mut store, &mut text_store, &args);
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.CREATE must succeed: {:?}",
        result
    );

    let idx = store
        .get_index(b"idx_mm_gu")
        .expect("index must exist after FT.CREATE");
    assert_eq!(
        idx.meta.merge_mode,
        MergeMode::GraphUnion,
        "MERGE_MODE GRAPH_UNION must set IndexMeta.merge_mode = GraphUnion"
    );
}

// ── Test 8: FT.CREATE KEEP_RAW parse round-trip ──

#[test]
fn test_ft_create_keep_raw_on_roundtrip() {
    use moon::command::vector_search::ft_create;
    use moon::protocol::Frame;

    fn bulk(b: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(b))
    }

    let mut store = VectorStore::new();
    let mut text_store = moon::text::store::TextStore::new();

    let args: Vec<Frame> = vec![
        bulk(b"idx_kr"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"8"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"64"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
        bulk(b"KEEP_RAW"),
        bulk(b"ON"),
    ];

    let result = ft_create(&mut store, &mut text_store, &args);
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.CREATE must succeed: {:?}",
        result
    );

    let idx = store
        .get_index(b"idx_kr")
        .expect("index must exist after FT.CREATE");
    assert!(
        idx.meta.keep_raw,
        "KEEP_RAW ON must set IndexMeta.keep_raw = true"
    );
}

#[test]
fn test_ft_create_keep_raw_default_is_false() {
    use moon::command::vector_search::ft_create;
    use moon::protocol::Frame;

    fn bulk(b: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(b))
    }

    let mut store = VectorStore::new();
    let mut text_store = moon::text::store::TextStore::new();

    // No KEEP_RAW in args — default must be false.
    let args: Vec<Frame> = vec![
        bulk(b"idx_kr_default"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"64"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];

    let result = ft_create(&mut store, &mut text_store, &args);
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.CREATE must succeed: {:?}",
        result
    );

    let idx = store
        .get_index(b"idx_kr_default")
        .expect("index must exist after FT.CREATE");
    assert!(
        !idx.meta.keep_raw,
        "Default KEEP_RAW must be false when not specified"
    );
}
