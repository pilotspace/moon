//! Stress tests for the vector engine.
//!
//! Simulates a compressed 24-hour workload: interleaved insert/search/delete/compact
//! over 10,000 cycles. Single-threaded (matches shard model). Validates zero panics
//! and data integrity under adversarial operation ordering.

use std::sync::Arc;

use moon::vector::distance;
use moon::vector::segment::mutable::MutableSegment;
use moon::vector::store::{IndexMeta, VectorStore};
use moon::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::DistanceMetric;

use bytes::Bytes;

const DIM: usize = 128;
const ITERATIONS: usize = 10_000;

/// Seeded LCG (Knuth MMIX) for deterministic random vectors.
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.state >> 32) as u32
    }

    fn next_f32(&mut self) -> f32 {
        (self.next_u32() as f32) / (u32::MAX as f32) * 2.0 - 1.0
    }
}

fn make_index_meta(name: &str, dim: u32) -> IndexMeta {
    IndexMeta {
        name: Bytes::from(name.to_owned()),
        dimension: dim,
        padded_dimension: padded_dimension(dim),
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 10000,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
    }
}

fn make_test_collection(dim: u32) -> Arc<CollectionMetadata> {
    Arc::new(CollectionMetadata::with_build_mode(
        1,
        dim,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        42,
        BuildMode::Light,
    ))
}

fn fill_vectors(rng: &mut Lcg, f32_buf: &mut Vec<f32>, sq_buf: &mut Vec<i8>, dim: usize) {
    f32_buf.clear();
    sq_buf.clear();
    for _ in 0..dim {
        let val = rng.next_f32();
        f32_buf.push(val);
        let clamped = val.clamp(-1.0, 1.0);
        sq_buf.push((clamped * 127.0) as i8);
    }
}

#[test]
fn test_stress_10k_interleaved_operations() {
    distance::init();

    let mut store = VectorStore::new();
    store
        .create_index(make_index_meta("stress_idx", DIM as u32))
        .unwrap();

    let idx = store.get_index_mut(b"stress_idx").unwrap();
    let snap = idx.segments.load();
    let mutable = &snap.mutable;

    let mut rng = Lcg::new(42);
    let mut inserted_ids: Vec<u32> = Vec::with_capacity(ITERATIONS);
    let mut deleted_count: usize = 0;

    // Reusable buffers -- zero allocation in the hot loop
    let mut f32_buf: Vec<f32> = Vec::with_capacity(DIM);
    let mut sq_buf: Vec<i8> = Vec::with_capacity(DIM);
    let mut query_f32: Vec<f32> = Vec::with_capacity(DIM);

    for i in 0..ITERATIONS {
        let op = rng.next_u32() % 100;

        if op < 40 {
            // INSERT (40%)
            fill_vectors(&mut rng, &mut f32_buf, &mut sq_buf, DIM);
            let norm = f32_buf.iter().map(|x| x * x).sum::<f32>().sqrt();
            let id = mutable.append(i as u64, &f32_buf, &sq_buf, norm, i as u64);
            inserted_ids.push(id);
        } else if op < 70 {
            // SEARCH (30%)
            if !inserted_ids.is_empty() {
                // Generate a random query
                query_f32.clear();
                for _ in 0..DIM {
                    query_f32.push(rng.next_f32());
                }
                let results = mutable.brute_force_search(&query_f32, None, 10);
                assert!(results.len() <= 10, "result count exceeds k");
                for r in &results {
                    assert!(r.distance >= 0.0, "negative distance at iteration {i}");
                }
                // Prevent dead code elimination
                std::hint::black_box(&results);
            }
        } else if op < 90 {
            // DELETE (20%)
            if !inserted_ids.is_empty() {
                let idx_to_del = rng.next_u32() as usize % inserted_ids.len();
                let id = inserted_ids.swap_remove(idx_to_del);
                mutable.mark_deleted(id, i as u64 + 1);
                deleted_count += 1;
            }
        } else {
            // COMPACT-CHECK (10%)
            if mutable.is_full() {
                let frozen = mutable.freeze();
                assert!(
                    !frozen.entries.is_empty(),
                    "frozen segment should be non-empty"
                );
                std::hint::black_box(&frozen);
            }
        }
    }

    // Final assertions
    let total_appended = mutable.len();
    let expected_live = total_appended - deleted_count;
    assert_eq!(
        inserted_ids.len(),
        expected_live,
        "tracked live IDs ({}) != total appended ({}) - deleted ({})",
        inserted_ids.len(),
        total_appended,
        deleted_count
    );

    // Final search should not panic and should return valid results
    if !inserted_ids.is_empty() {
        query_f32.clear();
        for _ in 0..DIM {
            query_f32.push(0.0f32);
        }
        let final_results = mutable.brute_force_search(&query_f32, None, 10);
        // At minimum we should get some results (there are live vectors)
        // Could be fewer than 10 if many were deleted
        assert!(
            final_results.len() <= 10,
            "final search result count exceeds k"
        );
        for r in &final_results {
            assert!(r.distance >= 0.0, "negative distance in final search");
        }
        std::hint::black_box(&final_results);
    }
}

#[test]
fn test_stress_interleaved_search_during_compaction() {
    distance::init();

    let dim: usize = 64;
    let collection = make_test_collection(dim as u32);
    let seg = MutableSegment::new(dim as u32, collection);

    let mut rng = Lcg::new(123);
    let mut f32_buf: Vec<f32> = Vec::with_capacity(dim);
    let mut sq_buf: Vec<i8> = Vec::with_capacity(dim);

    // Fill segment with enough vectors to exercise freeze path
    let insert_count = 5000;
    for i in 0..insert_count {
        fill_vectors(&mut rng, &mut f32_buf, &mut sq_buf, dim);
        let norm = f32_buf.iter().map(|x| x * x).sum::<f32>().sqrt();
        seg.append(i as u64, &f32_buf, &sq_buf, norm, i as u64);
    }

    assert_eq!(seg.len(), insert_count);

    // Freeze the segment -- snapshot for compaction pipeline
    let frozen = seg.freeze();
    assert_eq!(frozen.entries.len(), insert_count);
    assert_eq!(frozen.dimension, dim as u32);

    // Immediately search the original mutable segment while "compaction" holds the frozen snapshot.
    // This simulates concurrent search during compaction state transition.
    let mut query_f32: Vec<f32> = Vec::with_capacity(dim);
    for _ in 0..dim {
        query_f32.push(rng.next_f32());
    }
    let results = seg.brute_force_search(&query_f32, None, 10);
    assert!(results.len() <= 10);
    assert!(
        !results.is_empty(),
        "search should find vectors in non-empty segment"
    );
    for r in &results {
        assert!(
            r.distance >= 0.0,
            "negative distance during compaction search"
        );
    }

    // Search the frozen snapshot too -- validates no stale pointer issues
    // FrozenSegment doesn't have search, but we can verify data integrity
    assert!(!frozen.tq_codes.is_empty());

    // Verify all entries have valid internal_ids
    for (i, entry) in frozen.entries.iter().enumerate() {
        assert_eq!(entry.internal_id, i as u32);
    }
    // Verify TQ codes have correct total length
    let bytes_per_code = frozen.bytes_per_code;
    assert_eq!(frozen.tq_codes.len(), insert_count * bytes_per_code);

    std::hint::black_box(&results);
    std::hint::black_box(&frozen);
}
