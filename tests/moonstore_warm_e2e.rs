//! End-to-end test: insert vectors -> compact -> warm transition -> verify.
//!
//! Tests the full HOT->WARM lifecycle at the component level:
//! 1. Create VectorStore + index
//! 2. Insert enough vectors to trigger compaction
//! 3. Compact (creates ImmutableSegment)
//! 4. Verify immutable segment exists
//! 5. Call try_warm_transitions with warm_after_secs=0 (immediate)
//! 6. Verify immutable segment was removed from in-memory list
//! 7. Verify .mpf files exist on disk
//! 8. Verify manifest has warm tier entry

use bytes::Bytes;

use moon::persistence::manifest::{ShardManifest, StorageTier};
use moon::vector::distance;
use moon::vector::store::{IndexMeta, VectorStore};
use moon::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::DistanceMetric;

fn make_test_meta(name: &str, dim: u32, compact_threshold: u32) -> IndexMeta {
    IndexMeta {
        name: Bytes::from(name.to_owned()),
        dimension: dim,
        padded_dimension: padded_dimension(dim),
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
    }
}

/// Full lifecycle: insert -> compact -> warm transition -> verify .mpf on disk.
#[test]
fn test_warm_transition_end_to_end() {
    distance::init();

    // 1. Setup temp directory and manifest
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();
    let manifest_path = shard_dir.join("shard-0.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    // 2. Create VectorStore with an index (compact_threshold=100)
    let mut store = VectorStore::new();
    let meta = make_test_meta("idx", 128, 100);
    store.create_index(meta).unwrap();

    // 3. Insert 150 vectors (above compact threshold of 100)
    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        for i in 0..150u32 {
            let f32_vec: Vec<f32> = (0..128).map(|d| (i * 128 + d) as f32 * 0.001).collect();
            let sq_vec: Vec<i8> = f32_vec.iter().map(|v| (v * 100.0) as i8).collect();
            snap.mutable
                .append(i as u64, &f32_vec, &sq_vec, 1.0, i as u64);
        }
    }

    // Verify mutable segment has 150 entries
    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert_eq!(
            snap.mutable.len(),
            150,
            "mutable segment should have 150 vectors"
        );
        assert!(
            snap.immutable.is_empty(),
            "no immutable segments before compaction"
        );
    }

    // 4. Compact
    {
        let idx = store.get_index_mut(b"idx").unwrap();
        idx.try_compact();
    }

    // 5. Verify immutable segment was created
    let imm_count_before;
    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert!(
            !snap.immutable.is_empty(),
            "compaction should create immutable segment"
        );
        imm_count_before = snap.immutable.len();
    }

    // 6. Warm transition with warm_after_secs=0 (everything qualifies immediately)
    let mut next_file_id = 1u64;
    let idx = store.get_index(b"idx").unwrap();
    let transitioned = idx.try_warm_transitions(
        &shard_dir,
        &mut manifest,
        0, // warm_after_secs=0 means everything qualifies
        &mut next_file_id,
        &mut None, // no WAL writer in test
    );
    assert!(transitioned > 0, "should transition at least one segment");

    // 7. Verify immutable list is shorter
    {
        let snap = idx.segments.load();
        assert_eq!(
            snap.immutable.len(),
            imm_count_before - transitioned,
            "immutable list should shrink by transitioned count"
        );
    }

    // 8. Verify .mpf files on disk
    let vectors_dir = shard_dir.join("vectors");
    assert!(
        vectors_dir.exists(),
        "vectors directory should exist after warm transition"
    );

    let seg_dirs: Vec<_> = std::fs::read_dir(&vectors_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().is_dir() && e.file_name().to_str().unwrap_or("").starts_with("segment-")
        })
        .collect();
    assert!(
        !seg_dirs.is_empty(),
        "should have at least one segment directory on disk"
    );
    for seg_dir in &seg_dirs {
        assert!(
            seg_dir.path().join("codes.mpf").exists(),
            "codes.mpf missing in {:?}",
            seg_dir.path()
        );
        assert!(
            seg_dir.path().join("graph.mpf").exists(),
            "graph.mpf missing in {:?}",
            seg_dir.path()
        );
        assert!(
            seg_dir.path().join("mvcc.mpf").exists(),
            "mvcc.mpf missing in {:?}",
            seg_dir.path()
        );
    }

    // 9. Verify manifest has warm tier entries
    assert!(
        !manifest.files().is_empty(),
        "manifest should have entries after warm transition"
    );
    let warm_entries: Vec<_> = manifest
        .files()
        .iter()
        .filter(|f| f.tier == StorageTier::Warm)
        .collect();
    assert!(
        !warm_entries.is_empty(),
        "should have warm tier entries in manifest"
    );
}

/// Verify that warm transition respects the age threshold -- newly created
/// segments should NOT transition when warm_after_secs is very high.
#[test]
fn test_warm_transition_respects_age_threshold() {
    distance::init();

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();
    let manifest_path = shard_dir.join("shard-0.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    let mut store = VectorStore::new();
    store.create_index(make_test_meta("idx", 128, 100)).unwrap();

    // Insert 150 vectors and compact
    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        for i in 0..150u32 {
            let f32_vec: Vec<f32> = (0..128).map(|d| (i * 128 + d) as f32 * 0.001).collect();
            let sq_vec: Vec<i8> = f32_vec.iter().map(|v| (v * 100.0) as i8).collect();
            snap.mutable
                .append(i as u64, &f32_vec, &sq_vec, 1.0, i as u64);
        }
    }
    {
        let idx = store.get_index_mut(b"idx").unwrap();
        idx.try_compact();
    }

    // Verify we have immutable segments
    let idx = store.get_index(b"idx").unwrap();
    let imm_before = idx.segments.load().immutable.len();
    assert!(
        imm_before > 0,
        "should have immutable segments after compaction"
    );

    // Try warm transition with very high age threshold (segments are brand new)
    let mut next_file_id = 1u64;
    let transitioned = idx.try_warm_transitions(
        &shard_dir,
        &mut manifest,
        999_999, // 999999 seconds ~ 11.5 days -- nothing qualifies
        &mut next_file_id,
        &mut None, // no WAL writer in test
    );
    assert_eq!(
        transitioned, 0,
        "no segments should qualify with high age threshold"
    );

    // Immutable list should be unchanged
    assert_eq!(
        idx.segments.load().immutable.len(),
        imm_before,
        "immutable list should be unchanged when nothing transitions"
    );
    assert!(
        manifest.files().is_empty(),
        "manifest should have no entries when nothing transitions"
    );
}

/// After warm-transitioning immutable segments, search on the mutable
/// segment should still work correctly (no regression).
#[test]
fn test_warm_transition_search_still_works_on_mutable() {
    distance::init();

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();
    let manifest_path = shard_dir.join("shard-0.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    let mut store = VectorStore::new();
    store.create_index(make_test_meta("idx", 128, 100)).unwrap();

    // Insert 150 vectors and compact
    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        for i in 0..150u32 {
            let f32_vec: Vec<f32> = (0..128).map(|d| (i * 128 + d) as f32 * 0.001).collect();
            let sq_vec: Vec<i8> = f32_vec.iter().map(|v| (v * 100.0) as i8).collect();
            snap.mutable
                .append(i as u64, &f32_vec, &sq_vec, 1.0, i as u64);
        }
    }
    {
        let idx = store.get_index_mut(b"idx").unwrap();
        idx.try_compact();
    }

    // Warm-transition all immutable segments
    {
        let idx = store.get_index(b"idx").unwrap();
        let mut next_file_id = 1u64;
        let transitioned =
            idx.try_warm_transitions(&shard_dir, &mut manifest, 0, &mut next_file_id, &mut None);
        assert!(transitioned > 0, "should transition at least one segment");
    }

    // Now insert MORE vectors into the new mutable segment
    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        for i in 200..210u32 {
            let f32_vec: Vec<f32> = (0..128).map(|d| (i * 128 + d) as f32 * 0.001).collect();
            let sq_vec: Vec<i8> = f32_vec.iter().map(|v| (v * 100.0) as i8).collect();
            snap.mutable
                .append(i as u64, &f32_vec, &sq_vec, 1.0, i as u64);
        }
        // Mutable segment should have the new vectors
        assert!(
            snap.mutable.len() >= 10,
            "mutable segment should have new vectors"
        );
    }

    // Brute force search on the mutable segment should work
    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        let query: Vec<f32> = (0..128).map(|d| (205 * 128 + d) as f32 * 0.001).collect();
        let results = snap.mutable.brute_force_search(&query, None, 5);
        assert!(
            !results.is_empty(),
            "brute force search on mutable should return results after warm transition"
        );
    }
}
