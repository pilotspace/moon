//! Edge case and FT.* command hardening tests for the vector engine.
//!
//! Tests boundary conditions (zero vectors, max dimension, empty index, mismatched
//! dimension, k=0, k>N) and verifies all FT.* commands reject invalid arguments
//! with appropriate Frame::Error responses.

use std::sync::Arc;

use bytes::Bytes;

use moon::command::vector_search::{
    ft_create, ft_dropindex, ft_info, ft_search, quantize_f32_to_sq,
};
use moon::protocol::Frame;
use moon::vector::distance;
use moon::vector::segment::mutable::MutableSegment;
use moon::vector::store::{IndexMeta, VectorStore};
use moon::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::DistanceMetric;

// -- Helpers --

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::from(s.to_vec()))
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
        compact_threshold: 10000,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
    }
}

fn ft_create_args(name: &str, dim: u32) -> Vec<Frame> {
    vec![
        bulk(name.as_bytes()),
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
        bulk(dim.to_string().as_bytes()),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ]
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

fn make_sq_vec(f32_vec: &[f32]) -> Vec<i8> {
    let mut sq = vec![0i8; f32_vec.len()];
    quantize_f32_to_sq(f32_vec, &mut sq);
    sq
}

fn assert_is_error(frame: &Frame, context: &str) {
    match frame {
        Frame::Error(_) => {}
        other => panic!("{context}: expected Frame::Error, got {other:?}"),
    }
}

// ============================================================
// Edge case tests (1-9)
// ============================================================

#[test]
fn test_zero_vector_insert_and_search() {
    distance::init();

    let dim = 128;
    let collection = make_test_collection(dim as u32);
    let seg = MutableSegment::new(dim as u32, collection);
    let zeros_f32 = vec![0.0f32; dim];
    let zeros_sq = vec![0i8; dim];

    seg.append(1, &zeros_f32, &zeros_sq, 0.0, 1);

    let results = seg.brute_force_search(&zeros_f32, None, 1);
    assert_eq!(results.len(), 1, "should find the zero vector");
    assert_eq!(results[0].id.0, 0);
}

#[test]
fn test_max_dimension_3072() {
    distance::init();

    let dim: usize = 3072;
    let collection = make_test_collection(dim as u32);
    let seg = MutableSegment::new(dim as u32, collection);

    let mut f32_vec = Vec::with_capacity(dim);
    let mut sq_vec = Vec::with_capacity(dim);
    let mut seed: u32 = 7;
    for _ in 0..dim {
        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        let val = (seed as f32) / (u32::MAX as f32) * 2.0 - 1.0;
        f32_vec.push(val);
        sq_vec.push((val.clamp(-1.0, 1.0) * 127.0) as i8);
    }

    let norm = f32_vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    seg.append(1, &f32_vec, &sq_vec, norm, 1);
    assert_eq!(seg.len(), 1);

    let results = seg.brute_force_search(&f32_vec, None, 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id.0, 0);
}

#[test]
fn test_empty_index_search() {
    distance::init();

    let dim = 128;
    let collection = make_test_collection(dim as u32);
    let seg = MutableSegment::new(dim as u32, collection);
    let query = vec![0.0f32; dim];

    let results = seg.brute_force_search(&query, None, 10);
    assert!(
        results.is_empty(),
        "search on empty segment should return empty"
    );
}

#[test]
fn test_search_k_zero() {
    distance::init();

    let dim = 16;
    let collection = make_test_collection(dim as u32);
    let seg = MutableSegment::new(dim as u32, collection);
    let f32_v = vec![1.0f32; dim];
    let sq_v = vec![1i8; dim];
    seg.append(1, &f32_v, &sq_v, 1.0, 1);

    let results = seg.brute_force_search(&f32_v, None, 0);
    assert!(results.is_empty(), "k=0 should return empty results");
}

#[test]
fn test_search_k_larger_than_index() {
    distance::init();

    let dim = 16;
    let collection = make_test_collection(dim as u32);
    let seg = MutableSegment::new(dim as u32, collection);
    for i in 0..5u32 {
        let f32_v: Vec<f32> = (0..dim)
            .map(|d| (i * 10 + d as u32) as f32 / 100.0)
            .collect();
        let sq_v = make_sq_vec(&f32_v);
        seg.append(i as u64, &f32_v, &sq_v, 1.0, i as u64);
    }

    let query = vec![0.0f32; dim];
    let results = seg.brute_force_search(&query, None, 100);
    assert_eq!(
        results.len(),
        5,
        "k=100 with 5 vectors should return all 5, got {}",
        results.len()
    );
}

#[test]
fn test_delete_nonexistent_id() {
    let collection = make_test_collection(128);
    let seg = MutableSegment::new(128, collection);
    // Mark-delete ID 999 that was never inserted -- should not panic
    seg.mark_deleted(999, 1);
    assert_eq!(seg.len(), 0);
}

#[test]
fn test_duplicate_index_create() {
    let mut store = VectorStore::new();
    let meta1 = make_meta("idx1", 128);
    assert!(store.create_index(meta1).is_ok());

    let meta2 = make_meta("idx1", 128);
    let result = store.create_index(meta2);
    assert!(result.is_err(), "duplicate create should return Err");
    assert_eq!(store.len(), 1);
}

#[test]
fn test_drop_nonexistent_index() {
    let mut store = VectorStore::new();
    let dropped = store.drop_index(b"nonexistent");
    assert!(!dropped, "dropping nonexistent index should return false");
}

// ============================================================
// FT.* command argument hardening (10-16)
// ============================================================

#[test]
fn test_ft_create_missing_args() {
    let mut store = VectorStore::new();
    // Fewer than 10 args
    let args = vec![bulk(b"myidx"), bulk(b"ON"), bulk(b"HASH")];
    let result = ft_create(&mut store, &args);
    assert_is_error(&result, "ft_create with < 10 args");
}

#[test]
fn test_ft_create_invalid_dim() {
    let mut store = VectorStore::new();

    // DIM = 0
    let args = vec![
        bulk(b"idx0"),
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
        bulk(b"0"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];
    let result = ft_create(&mut store, &args);
    assert_is_error(&result, "ft_create with DIM=0");

    // DIM = non-numeric
    let args2 = vec![
        bulk(b"idx_nan"),
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
        bulk(b"notanumber"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];
    let result2 = ft_create(&mut store, &args2);
    assert_is_error(&result2, "ft_create with DIM=notanumber");
}

#[test]
fn test_ft_create_missing_schema() {
    let mut store = VectorStore::new();
    // Replace SCHEMA with something else
    let args = vec![
        bulk(b"idx_noschema"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"NOTSCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"128"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];
    let result = ft_create(&mut store, &args);
    assert_is_error(&result, "ft_create without SCHEMA keyword");
}

#[test]
fn test_ft_search_missing_query_vector() {
    distance::init();

    let mut store = VectorStore::new();
    let create_args = ft_create_args("search_idx", 128);
    ft_create(&mut store, &create_args);

    // Only index name and query string, no PARAMS section
    let search_args = vec![bulk(b"search_idx"), bulk(b"*=>[KNN 10 @vec $query]")];
    let result = ft_search(&mut store, &search_args);
    assert_is_error(&result, "ft_search without query vector");
}

#[test]
fn test_ft_search_nonexistent_index() {
    let mut store = VectorStore::new();
    let search_args = vec![
        bulk(b"no_such_index"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(vec![0u8; 128 * 4])),
    ];
    let result = ft_search(&mut store, &search_args);
    assert_is_error(&result, "ft_search on nonexistent index");
}

#[test]
fn test_ft_info_nonexistent_index() {
    let store = VectorStore::new();
    let result = ft_info(&store, &[bulk(b"no_such_index")]);
    assert_is_error(&result, "ft_info on nonexistent index");
}

#[test]
fn test_ft_dropindex_missing_args() {
    let mut store = VectorStore::new();
    let result = ft_dropindex(&mut store, &[]);
    assert_is_error(&result, "ft_dropindex with no args");
}

// ============================================================
// Additional robustness: dimension mismatch via FT.SEARCH
// ============================================================

#[test]
fn test_ft_search_dimension_mismatch_returns_error() {
    distance::init();

    let mut store = VectorStore::new();
    let create_args = ft_create_args("dim_idx", 128);
    ft_create(&mut store, &create_args);

    // Send a query blob that is 4 bytes (1 float) instead of 128*4
    let search_args = vec![
        bulk(b"dim_idx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(vec![0u8; 4])),
    ];
    let result = ft_search(&mut store, &search_args);
    assert_is_error(&result, "ft_search with wrong dimension blob");
}
