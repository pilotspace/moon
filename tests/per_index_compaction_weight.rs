//! W3-deep — Per-index compaction priority weights tests.
//!
//! Red/Green TDD: these tests are written BEFORE the implementation.
//!
//! ## Design under test
//!
//! `CompactionEntity` gains a `weight: f32` field (default 1.0).
//! `weighted_rate()` = `dead_bytes_rate() * weight as f64`.
//! `CompactionScheduler::pop_next()` uses `weighted_rate()` for comparison,
//! not `dead_bytes_rate()` directly.
//!
//! Starvation cap still fires based on `last_compaction.elapsed()` alone —
//! weight=0.0 cannot suppress the starvation safety path.
//!
//! ## Test cases
//!
//! 1. `test_weight_multiplies_priority` — idx_hot (weight=10) vs idx_cold (weight=0.1),
//!    same dead_bytes_rate. Over 10 pops, idx_hot must get ≥ 9×.
//! 2. `test_weight_zero_does_not_prevent_starvation` — weight=0 entity eventually
//!    runs once starvation cap elapses.
//! 3. `test_weight_default_is_one` — newly created entity behaves identically to MA4
//!    (backward compat: weight=1.0 → same ordering as before).
//! 4. `test_weight_high_cold_beats_low_hot` — same dead_bytes per time, but
//!    weight=100 on cold beats weight=1 on hot.
//! 5. `test_ft_config_weight_roundtrip` — set weight via FT.CONFIG and read it back.
//! 6. `test_vacuum_vector_weight_sets_and_reads` — VACUUM VECTOR idx WEIGHT n sets the
//!    weight on the VectorIndex.

use moon::shard::autovacuum::{CompactionEntity, CompactionScheduler};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// 1. weight multiplies priority: hot (weight=10) >> cold (weight=0.1)
// ---------------------------------------------------------------------------

#[test]
fn test_weight_multiplies_priority() {
    let starvation_cap = Duration::from_secs(3600); // never fires in this test

    let base = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();

    let mut scheduler = CompactionScheduler::new(starvation_cap);
    // Both have identical dead_bytes (1000) and identical elapsed (1s).
    // dead_bytes_rate = 1000/1 = 1000 for both.
    // But weights differ: hot=10.0, cold=0.1 → weighted_rate: hot=10000, cold=100.
    scheduler.upsert(CompactionEntity {
        id: "idx_hot".to_string(),
        bytes_dead: 1000,
        last_compaction: base,
        weight: 10.0,
    });
    scheduler.upsert(CompactionEntity {
        id: "idx_cold".to_string(),
        bytes_dead: 1000,
        last_compaction: base,
        weight: 0.1,
    });

    let mut hot_count = 0u32;
    let mut cold_count = 0u32;

    for _ in 0..10 {
        match scheduler.pop_next().as_deref() {
            Some("idx_hot") => {
                hot_count += 1;
                // Re-upsert with a fresh timestamp so its rate drops to near-zero
                // but the weight still differentiates.
                scheduler.upsert(CompactionEntity {
                    id: "idx_hot".to_string(),
                    bytes_dead: 1000,
                    last_compaction: Instant::now()
                        .checked_sub(Duration::from_millis(1))
                        .unwrap(),
                    weight: 10.0,
                });
                // Also re-upsert cold with the original elapsed so its rate stays constant.
                scheduler.upsert(CompactionEntity {
                    id: "idx_cold".to_string(),
                    bytes_dead: 1000,
                    last_compaction: base,
                    weight: 0.1,
                });
            }
            Some("idx_cold") => {
                cold_count += 1;
                scheduler.upsert(CompactionEntity {
                    id: "idx_cold".to_string(),
                    bytes_dead: 1000,
                    last_compaction: Instant::now()
                        .checked_sub(Duration::from_millis(1))
                        .unwrap(),
                    weight: 0.1,
                });
                scheduler.upsert(CompactionEntity {
                    id: "idx_hot".to_string(),
                    bytes_dead: 1000,
                    last_compaction: base,
                    weight: 10.0,
                });
            }
            _ => {}
        }
    }

    assert!(
        hot_count >= 9 * cold_count.max(1),
        "idx_hot (weight=10) should run ≥ 9× more than idx_cold (weight=0.1), got hot={hot_count} cold={cold_count}"
    );
}

// ---------------------------------------------------------------------------
// 2. weight=0 does not prevent starvation cap from firing
// ---------------------------------------------------------------------------

#[test]
fn test_weight_zero_does_not_prevent_starvation() {
    let starvation_cap = Duration::from_millis(500);

    let mut scheduler = CompactionScheduler::new(starvation_cap);

    // hot: massive weight, tiny elapsed (rate stays near-zero)
    let hot_base = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    // zero_weight: weight=0, stale (beyond starvation cap)
    let stale_base = Instant::now()
        .checked_sub(Duration::from_millis(600))
        .unwrap(); // > 500ms starvation_cap

    scheduler.upsert(CompactionEntity {
        id: "dominant".to_string(),
        bytes_dead: 1_000_000_000,
        last_compaction: hot_base,
        weight: 1.0,
    });
    scheduler.upsert(CompactionEntity {
        id: "zero_weight".to_string(),
        bytes_dead: 1,
        last_compaction: stale_base,
        weight: 0.0, // weight=0 → weighted_rate = 0
    });

    // zero_weight MUST still eventually run because starvation_cap elapsed.
    let mut zero_ran = false;
    for _ in 0..20 {
        match scheduler.pop_next().as_deref() {
            Some("zero_weight") => {
                zero_ran = true;
                break;
            }
            Some(_) => {
                // Re-upsert dominant with fresh timestamp.
                scheduler.upsert(CompactionEntity {
                    id: "dominant".to_string(),
                    bytes_dead: 1_000_000_000,
                    last_compaction: Instant::now(),
                    weight: 1.0,
                });
            }
            None => break,
        }
    }

    assert!(
        zero_ran,
        "weight=0 entity must still run when starvation cap expires"
    );
}

// ---------------------------------------------------------------------------
// 3. Default weight=1.0 is backward-compatible with MA4 ordering
// ---------------------------------------------------------------------------

#[test]
fn test_weight_default_is_one() {
    let starvation_cap = Duration::from_secs(3600);
    let base = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();

    let mut scheduler = CompactionScheduler::new(starvation_cap);
    // A has higher bytes_dead, B has lower. With weight=1.0 (default), A wins.
    scheduler.upsert(CompactionEntity {
        id: "A".to_string(),
        bytes_dead: 5000,
        last_compaction: base,
        weight: 1.0,
    });
    scheduler.upsert(CompactionEntity {
        id: "B".to_string(),
        bytes_dead: 1000,
        last_compaction: base,
        weight: 1.0,
    });

    let first = scheduler.pop_next();
    assert_eq!(
        first.as_deref(),
        Some("A"),
        "weight=1.0 default must preserve MA4 dead_bytes_rate ordering"
    );
}

// ---------------------------------------------------------------------------
// 4. High weight on a cold index beats a low-weight hot index
// ---------------------------------------------------------------------------

#[test]
fn test_weight_high_cold_beats_low_hot() {
    let starvation_cap = Duration::from_secs(3600);
    let base = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();

    let mut scheduler = CompactionScheduler::new(starvation_cap);
    // hot_low: 1000 bytes/s × weight=1 → 1000 effective
    scheduler.upsert(CompactionEntity {
        id: "hot_low".to_string(),
        bytes_dead: 1000,
        last_compaction: base,
        weight: 1.0,
    });
    // cold_boosted: 100 bytes/s × weight=100 → 10000 effective
    scheduler.upsert(CompactionEntity {
        id: "cold_boosted".to_string(),
        bytes_dead: 100,
        last_compaction: base,
        weight: 100.0,
    });

    let first = scheduler.pop_next();
    assert_eq!(
        first.as_deref(),
        Some("cold_boosted"),
        "weight=100 on cold index must override a weight=1 hot index"
    );
}

// ---------------------------------------------------------------------------
// 5. FT.CONFIG SET / GET COMPACTION_WEIGHT roundtrip (unit — no server needed)
// ---------------------------------------------------------------------------

#[test]
fn test_ft_config_weight_roundtrip() {
    use bytes::Bytes;
    use moon::vector::store::{IndexMeta, VectorStore};
    use moon::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
    use moon::vector::types::DistanceMetric;

    let mut store = VectorStore::new();
    let meta = IndexMeta {
        name: Bytes::from_static(b"idx"),
        dimension: 64,
        padded_dimension: 64,
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 1000,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: moon::vector::segment::compaction::MergeMode::GraphUnion,
        keep_raw: false,
    };
    store.create_index(meta).unwrap();

    // Default weight is 1.0.
    let idx = store.get_index(b"idx").unwrap();
    assert!(
        (idx.compaction_weight - 1.0f32).abs() < 1e-6,
        "default compaction_weight must be 1.0"
    );

    // Set weight to 7.5.
    let idx_mut = store.get_index_mut(b"idx").unwrap();
    idx_mut.set_compaction_weight(7.5);
    let idx = store.get_index(b"idx").unwrap();
    assert!(
        (idx.compaction_weight() - 7.5f32).abs() < 1e-6,
        "compaction_weight must round-trip through set_compaction_weight"
    );

    // Clamp: value > 100.0 is rejected.
    let idx_mut = store.get_index_mut(b"idx").unwrap();
    let result = idx_mut.try_set_compaction_weight(200.0);
    assert!(result.is_err(), "weight > 100.0 must be rejected");

    // Clamp: negative value is rejected.
    let result = idx_mut.try_set_compaction_weight(-1.0);
    assert!(result.is_err(), "negative weight must be rejected");

    // Boundary: weight=0.0 is allowed (disables auto-compact).
    let result = idx_mut.try_set_compaction_weight(0.0);
    assert!(result.is_ok(), "weight=0.0 must be accepted");

    // Boundary: weight=100.0 is allowed (max).
    let result = idx_mut.try_set_compaction_weight(100.0);
    assert!(result.is_ok(), "weight=100.0 must be accepted");
}

// ---------------------------------------------------------------------------
// 6. VACUUM VECTOR <idx> WEIGHT <n> sets weight on VectorIndex (unit)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 7. Persistence roundtrip: weight set via API survives a VectorStore reload
// ---------------------------------------------------------------------------

#[test]
fn test_compaction_weight_persists_across_reload() {
    use bytes::Bytes;
    use moon::vector::index_persist::load_index_metadata_with_weights;
    use moon::vector::store::{IndexMeta, VectorStore};
    use moon::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
    use moon::vector::types::DistanceMetric;

    let dir = tempfile::tempdir().expect("tempdir");
    let persist_path = dir.path().to_path_buf();

    // Build and configure a store with persist_dir set.
    let mut store = VectorStore::new();
    store.set_persist_dir(persist_path.clone());

    let meta = IndexMeta {
        name: Bytes::from_static(b"persist_idx"),
        dimension: 64,
        padded_dimension: 64,
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 1000,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: moon::vector::segment::compaction::MergeMode::GraphUnion,
        keep_raw: false,
    };
    store.create_index(meta.clone()).unwrap();

    // Set a non-default weight and flush sidecar.
    {
        let idx = store.get_index_mut(b"persist_idx").unwrap();
        idx.try_set_compaction_weight(42.0)
            .expect("42.0 is in range");
    }
    store.save_index_meta_sidecar();

    // Reload: read the sidecar file from disk and verify the weight is present.
    let loaded = load_index_metadata_with_weights(&persist_path)
        .expect("sidecar must be readable after save");

    let (_, loaded_weight) = loaded
        .iter()
        .find(|(m, _)| m.name == Bytes::from_static(b"persist_idx"))
        .expect("persist_idx must be in the sidecar");

    assert!(
        (*loaded_weight - 42.0f32).abs() < 1e-5,
        "compaction_weight must survive a sidecar roundtrip, got {loaded_weight}"
    );
}

#[test]
fn test_vacuum_vector_weight_sets_and_reads() {
    use bytes::Bytes;
    use moon::protocol::Frame;
    use moon::vector::store::{IndexMeta, VectorStore};
    use moon::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
    use moon::vector::types::DistanceMetric;

    let mut store = VectorStore::new();
    let meta = IndexMeta {
        name: Bytes::from_static(b"myidx"),
        dimension: 64,
        padded_dimension: 64,
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 1000,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: moon::vector::segment::compaction::MergeMode::GraphUnion,
        keep_raw: false,
    };
    store.create_index(meta).unwrap();

    // Simulate: VACUUM VECTOR myidx WEIGHT 5.0
    let args = vec![
        Frame::BulkString(Bytes::from_static(b"myidx")),
        Frame::BulkString(Bytes::from_static(b"WEIGHT")),
        Frame::BulkString(Bytes::from_static(b"5.0")),
    ];
    let result = moon::command::server_admin::vacuum_vector(&mut store, &args);
    match &result {
        Frame::SimpleString(s) => {
            assert!(
                s.starts_with(b"OK"),
                "VACUUM VECTOR myidx WEIGHT 5.0 must return OK, got: {:?}",
                s
            );
        }
        other => panic!("expected SimpleString, got: {:?}", other),
    }

    // Weight must have been updated.
    let idx = store.get_index(b"myidx").unwrap();
    assert!(
        (idx.compaction_weight() - 5.0f32).abs() < 1e-6,
        "compaction_weight must be 5.0 after VACUUM VECTOR myidx WEIGHT 5.0"
    );

    // Out-of-range weight (> 100) must return an error.
    let bad_args = vec![
        Frame::BulkString(Bytes::from_static(b"myidx")),
        Frame::BulkString(Bytes::from_static(b"WEIGHT")),
        Frame::BulkString(Bytes::from_static(b"999")),
    ];
    let err_result = moon::command::server_admin::vacuum_vector(&mut store, &bad_args);
    assert!(
        matches!(err_result, Frame::Error(_)),
        "out-of-range weight must return error"
    );
}
