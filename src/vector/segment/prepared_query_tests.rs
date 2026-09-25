//! Per-query search redundancy (moon#1196): one prepared rotation/LUT per
//! query across segments, one tombstone guard per segment search.

use std::sync::Arc;

use bytes::Bytes;

use crate::vector::distance;
use crate::vector::hnsw::prepared::{LUT_BUILDS, PreparedTqQuery};
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::segment::holder::{MvccContext, SegmentHolder};
use crate::vector::segment::immutable::TOMBSTONE_GUARDS;
use crate::vector::store::VectorStore;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::types::{DistanceMetric, SearchTuning};

fn random_vec(dim: usize, seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_add(1);
    (0..dim)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0
        })
        .collect()
}

/// Index with `segs` compacted immutable segments + a mutable tail.
fn build_store(dim: u32, segs: usize, per_seg: usize, tail: usize) -> VectorStore {
    build_store_metric(dim, segs, per_seg, tail, DistanceMetric::L2)
}

fn build_store_metric(
    dim: u32,
    segs: usize,
    per_seg: usize,
    tail: usize,
    metric: DistanceMetric,
) -> VectorStore {
    distance::init();
    let mut store = VectorStore::new();
    let mut meta = crate::vector::store::test_index_meta(dim);
    meta.metric = metric;
    store.create_index(meta).unwrap();
    let mut n = 0u64;
    let put = |store: &mut VectorStore, n: &mut u64| {
        let key = format!("doc:{n}");
        let hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
        store
            .insert_vector(
                b"idx",
                &random_vec(dim as usize, *n),
                hash,
                Bytes::from(key),
            )
            .unwrap();
        *n += 1;
    };
    for _ in 0..segs {
        for _ in 0..per_seg {
            put(&mut store, &mut n);
        }
        store.force_compact_index(b"idx").unwrap();
    }
    for _ in 0..tail {
        put(&mut store, &mut n);
    }
    store
}

fn mvcc_search(holder: &SegmentHolder, q: &[f32], k: usize) -> Vec<(u32, u32)> {
    let committed = roaring::RoaringTreemap::new();
    let ctx = MvccContext {
        snapshot_lsn: 0,
        my_txn_id: 0,
        committed: &committed,
        dirty_set: &[],
        dimension: q.len() as u32,
        ef_defaulted: false,
        tuning: SearchTuning::default(),
    };
    let mut scratch = SearchScratch::new(
        0,
        crate::vector::turbo_quant::encoder::padded_dimension(q.len() as u32),
    );
    holder
        .search_mvcc(q, k, 80, &mut scratch, None, &ctx)
        .iter()
        .map(|r| (r.id.0, r.distance.to_bits()))
        .collect()
}

#[test]
fn prepared_query_is_bit_identical_per_segment() {
    for metric in [DistanceMetric::L2, DistanceMetric::Cosine] {
        prepared_identity_for(metric);
    }
}

fn prepared_identity_for(metric: DistanceMetric) {
    let dim = 48u32;
    let mut store = build_store_metric(dim, 5, 80, 10, metric);
    let idx = store.get_index_mut(b"idx").unwrap();
    let list = idx.segments.load_full();
    assert_eq!(list.immutable.len(), 5);
    let col = list.mutable.collection().clone();
    for qi in 0..10u64 {
        let q = random_vec(dim as usize, 9_000 + qi);
        let p = PreparedTqQuery::new(&q, &col).expect("TQ collection");
        for seg in &list.immutable {
            assert!(p.matches(seg.collection_meta()));
            let mut s1 = SearchScratch::new(
                0,
                crate::vector::turbo_quant::encoder::padded_dimension(q.len() as u32),
            );
            let mut s2 = SearchScratch::new(
                0,
                crate::vector::turbo_quant::encoder::padded_dimension(q.len() as u32),
            );
            for tuning in [
                SearchTuning::default(),
                SearchTuning {
                    rerank_mult: 4,
                    exact_beam: true,
                },
            ] {
                let a = seg.search_prepared(&q, Some(&p), 10, 64, &mut s1, None, tuning);
                let b = seg.search_prepared(&q, None, 10, 64, &mut s2, None, tuning);
                let bits = |v: &smallvec::SmallVec<[crate::vector::types::SearchResult; 32]>| {
                    v.iter()
                        .map(|r| (r.id.0, r.distance.to_bits()))
                        .collect::<Vec<_>>()
                };
                assert_eq!(
                    bits(&a),
                    bits(&b),
                    "q={qi} exact_beam={}",
                    tuning.exact_beam
                );
            }
        }
    }
}

#[test]
fn one_lut_build_per_query_not_per_segment() {
    // moon#1196 op-count red test: HEAD rotated the query and rebuilt the
    // ADC LUT inside every per-segment beam search (5 builds for 5
    // segments); the prepared query builds it once (per LUT width).
    let dim = 48u32;
    let mut store = build_store(dim, 5, 80, 10);
    let idx = store.get_index_mut(b"idx").unwrap();
    let q = random_vec(dim as usize, 4242);
    // Warm the thread-local scratch cache etc.
    let _ = mvcc_search(&idx.segments, &q, 10);
    LUT_BUILDS.with(|c| c.set(0));
    let results = mvcc_search(&idx.segments, &q, 10);
    let builds = LUT_BUILDS.with(std::cell::Cell::get);
    assert!(!results.is_empty());
    assert_eq!(
        builds, 1,
        "expected one shared LUT build for 5 segments, got {builds}"
    );
}

#[test]
fn prepared_state_for_another_collection_is_ignored() {
    // A segment whose collection differs (other sign flips) must NOT use the
    // query's prepared rotation/LUT.
    let dim = 32u32;
    let mut store = build_store(dim, 1, 60, 0);
    let idx = store.get_index_mut(b"idx").unwrap();
    let seg = Arc::clone(&idx.segments.load().immutable[0]);
    let other = Arc::new(CollectionMetadata::new(
        999,
        dim,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        31337,
    ));
    let q = random_vec(dim as usize, 5);
    let wrong = PreparedTqQuery::new(&q, &other).unwrap();
    assert!(!wrong.matches(seg.collection_meta()));
    let mut s1 = SearchScratch::new(
        0,
        crate::vector::turbo_quant::encoder::padded_dimension(q.len() as u32),
    );
    let mut s2 = SearchScratch::new(
        0,
        crate::vector::turbo_quant::encoder::padded_dimension(q.len() as u32),
    );
    let a = seg.search_prepared(
        &q,
        Some(&wrong),
        5,
        40,
        &mut s1,
        None,
        SearchTuning::default(),
    );
    let b = seg.search_prepared(&q, None, 5, 40, &mut s2, None, SearchTuning::default());
    assert_eq!(
        a.iter()
            .map(|r| (r.id.0, r.distance.to_bits()))
            .collect::<Vec<_>>(),
        b.iter()
            .map(|r| (r.id.0, r.distance.to_bits()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn tombstone_guard_taken_once_per_segment_search() {
    // moon#1196 op-count red test: once a segment had any steady-state
    // tombstone, HEAD took `tombstoned_keys.read()` (+ a SipHash probe) per
    // candidate — `ef` acquisitions per search. Now one per search.
    let dim = 32u32;
    let mut store = build_store(dim, 1, 120, 0);
    let idx = store.get_index_mut(b"idx").unwrap();
    let seg = Arc::clone(&idx.segments.load().immutable[0]);
    let victim = seg.mvcc_headers()[7].key_hash;
    assert_eq!(seg.mark_deleted_by_key_hash(victim), 1);
    let q = random_vec(dim as usize, 7);
    let mut scratch = SearchScratch::new(
        0,
        crate::vector::turbo_quant::encoder::padded_dimension(q.len() as u32),
    );
    TOMBSTONE_GUARDS.with(|c| c.set(0));
    let hits = seg.search_prepared(
        &q,
        None,
        10,
        100,
        &mut scratch,
        None,
        SearchTuning::default(),
    );
    assert_eq!(TOMBSTONE_GUARDS.with(std::cell::Cell::get), 1);
    assert!(
        hits.iter().all(|r| r.key_hash != victim),
        "tombstoned key returned"
    );
    assert_eq!(hits.len(), 10);
}

#[test]
fn prepared_state_without_the_sub_centroid_table_falls_back_to_a_local_lut() {
    // moon#1226 red test: a collection that `matches` the segment's (same
    // id, dimension, quantization, metric and checksum) can still lack the
    // sub-centroid table — the checksum does not cover it — so the prepared
    // `lut32()` is `None` while the segment scores 32-level. HEAD read that as
    // an EMPTY LUT and the search returned no results at all, silently.
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let dim = 64u32;
    let make = || {
        CollectionMetadata::new(
            77,
            dim,
            DistanceMetric::Cosine,
            QuantizationConfig::TurboQuant4,
            77,
        )
    };
    let col = Arc::new(make());
    assert!(col.sub_centroid_table.is_some(), "TQ4 builds the table");
    let seg = crate::vector::segment::mutable::MutableSegment::new(dim, Arc::clone(&col));
    for i in 0..300u64 {
        seg.append(i, &random_vec(dim as usize, 50_000 + i), i + 1);
    }
    let imm =
        crate::vector::segment::compaction::compact(&seg.freeze(), &col, 5, None).expect("compact");
    assert!(
        !imm.sub_centroid_signs().is_empty(),
        "the segment must score with the 32-level LUT"
    );

    let mut stripped = make();
    stripped.sub_centroid_table = None;
    let stripped = Arc::new(stripped);
    let q = random_vec(dim as usize, 9);
    let p = PreparedTqQuery::new(&q, &stripped).expect("TQ collection");
    assert!(p.matches(imm.collection_meta()), "checksum-equal twin");
    assert!(p.lut32().is_none(), "no table, no 32-level LUT");

    let padded = crate::vector::turbo_quant::encoder::padded_dimension(dim);
    for tuning in [
        SearchTuning::default(),
        SearchTuning {
            rerank_mult: 1,
            exact_beam: false,
        },
    ] {
        let mut s1 = SearchScratch::new(0, padded);
        let mut s2 = SearchScratch::new(0, padded);
        let with = imm.search_prepared(&q, Some(&p), 10, 64, &mut s1, None, tuning);
        let without = imm.search_prepared(&q, None, 10, 64, &mut s2, None, tuning);
        assert_eq!(without.len(), 10);
        assert_eq!(
            with.iter()
                .map(|r| (r.id.0, r.distance.to_bits()))
                .collect::<Vec<_>>(),
            without
                .iter()
                .map(|r| (r.id.0, r.distance.to_bits()))
                .collect::<Vec<_>>(),
            "a prepared state without the table must answer like no prepared state"
        );
    }
}

#[test]
fn a_single_graph_segment_query_builds_no_prepared_state() {
    // moon#1226 allocation red test: HEAD built a PreparedTqQuery — a heap
    // rotated query, unit query and a zeroed 32–128 KB LUT — for EVERY query
    // that visits a graph segment, even when only one segment exists to use
    // it; that segment's own path fills the reused scratch without
    // allocating. Now it is built only when ≥ 2 graph segments share it.
    use crate::vector::hnsw::prepared::PREPARED_BUILDS;
    let dim = 48u32;
    for (segs, want_prepared) in [(1usize, 0usize), (2, 1), (4, 1)] {
        let mut store = build_store(dim, segs, 80, 10);
        let idx = store.get_index_mut(b"idx").unwrap();
        let q = random_vec(dim as usize, 777);
        let _ = mvcc_search(&idx.segments, &q, 10); // warm-up
        PREPARED_BUILDS.with(|c| c.set(0));
        LUT_BUILDS.with(|c| c.set(0));
        let results = mvcc_search(&idx.segments, &q, 10);
        assert_eq!(results.len(), 10);
        assert_eq!(
            PREPARED_BUILDS.with(std::cell::Cell::get),
            want_prepared,
            "{segs} graph segment(s)"
        );
        // Either way the LUT is built exactly once per query.
        assert_eq!(LUT_BUILDS.with(std::cell::Cell::get), 1, "{segs} segs");
    }
}
