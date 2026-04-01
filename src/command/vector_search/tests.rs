use super::*;

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::from(s.to_vec()))
}

/// Build a valid FT.CREATE argument list.
fn ft_create_args() -> Vec<Frame> {
    vec![
        bulk(b"myidx"), // index name
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"), // 6 params = 3 key-value pairs
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"128"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ]
}

#[test]
fn test_ft_create_parse_full_syntax() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    let result = ft_create(&mut store, &args);
    match &result {
        Frame::SimpleString(s) => assert_eq!(&s[..], b"OK"),
        other => panic!("expected OK, got {other:?}"),
    }
    assert_eq!(store.len(), 1);
    let idx = store.get_index(b"myidx").unwrap();
    assert_eq!(idx.meta.dimension, 128);
    assert_eq!(idx.meta.metric, DistanceMetric::L2);
    assert_eq!(idx.meta.key_prefixes.len(), 1);
    assert_eq!(&idx.meta.key_prefixes[0][..], b"doc:");
}

#[test]
fn test_ft_create_missing_dim() {
    let mut store = VectorStore::new();
    // Remove DIM param pair: keep TYPE FLOAT32 and DISTANCE_METRIC L2 (4 params = 2 pairs)
    let args = vec![
        bulk(b"myidx"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"4"), // 4 params = 2 key-value pairs
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];
    let result = ft_create(&mut store, &args);
    match &result {
        Frame::Error(_) => {} // expected
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_create_duplicate() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    let r1 = ft_create(&mut store, &args);
    assert!(matches!(r1, Frame::SimpleString(_)));

    let args2 = ft_create_args();
    let r2 = ft_create(&mut store, &args2);
    match &r2 {
        Frame::Error(e) => assert!(e.starts_with(b"ERR")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_dropindex() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &args);

    // Drop existing
    let result = ft_dropindex(&mut store, &[bulk(b"myidx")]);
    assert!(matches!(result, Frame::SimpleString(_)));
    assert!(store.is_empty());

    // Drop non-existing
    let result = ft_dropindex(&mut store, &[bulk(b"myidx")]);
    assert!(matches!(result, Frame::Error(_)));
}

#[test]
fn test_parse_knn_query() {
    let query = b"*=>[KNN 10 @vec $query]";
    let (k, param) = parse_knn_query(query).unwrap();
    assert_eq!(k, 10);
    assert_eq!(&param[..], b"query");
}

#[test]
fn test_parse_knn_query_different_k() {
    let query = b"*=>[KNN 5 @embedding $blob]";
    let (k, param) = parse_knn_query(query).unwrap();
    assert_eq!(k, 5);
    assert_eq!(&param[..], b"blob");
}

#[test]
fn test_parse_knn_query_invalid() {
    assert!(parse_knn_query(b"*").is_none());
    assert!(parse_knn_query(b"*=>[NOTAKNN]").is_none());
}

#[test]
fn test_extract_param_blob() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        bulk(b"blobdata"),
    ];
    let blob = extract_param_blob(&args, b"query").unwrap();
    assert_eq!(&blob[..], b"blobdata");
}

#[test]
fn test_extract_param_blob_missing() {
    let args = vec![bulk(b"idx"), bulk(b"*=>[KNN 10 @vec $query]")];
    assert!(extract_param_blob(&args, b"query").is_none());
}

#[test]
fn test_quantize_f32_to_sq() {
    let input = [0.0, 1.0, -1.0, 0.5, -0.5, 2.0, -2.0];
    let mut output = [0i8; 7];
    quantize_f32_to_sq(&input, &mut output);
    assert_eq!(output[0], 0); // 0.0 -> 0
    assert_eq!(output[1], 127); // 1.0 -> 127
    assert_eq!(output[2], -127); // -1.0 -> -127
    assert_eq!(output[3], 63); // 0.5 -> 63 (truncated from 63.5)
    assert_eq!(output[4], -63); // -0.5 -> -63
    assert_eq!(output[5], 127); // 2.0 clamped to 1.0 -> 127
    assert_eq!(output[6], -127); // -2.0 clamped to -1.0 -> -127
}

#[test]
fn test_merge_search_results_combines_shards() {
    // Shard 0 returns: [2, "vec:0", ["__vec_score", "0.1"], "vec:1", ["__vec_score", "0.5"]]
    // Shard 1 returns: [2, "vec:10", ["__vec_score", "0.3"], "vec:11", ["__vec_score", "0.9"]]
    // Global top-2 should be: vec:0 (0.1), vec:10 (0.3)

    let shard0 = Frame::Array(
        vec![
            Frame::Integer(2),
            bulk(b"vec:0"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.1")].into()),
            bulk(b"vec:1"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.5")].into()),
        ]
        .into(),
    );

    let shard1 = Frame::Array(
        vec![
            Frame::Integer(2),
            bulk(b"vec:10"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.3")].into()),
            bulk(b"vec:11"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.9")].into()),
        ]
        .into(),
    );

    let result = merge_search_results(&[shard0, shard1], 2);
    match result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(2));
            assert_eq!(items[1], Frame::BulkString(Bytes::from("vec:0")));
            assert_eq!(items[3], Frame::BulkString(Bytes::from("vec:10")));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_merge_search_results_handles_errors() {
    // One shard returns error, one returns valid results
    let shard0 = Frame::Error(Bytes::from_static(b"ERR shard unavailable"));
    let shard1 = Frame::Array(
        vec![
            Frame::Integer(1),
            bulk(b"vec:5"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.2")].into()),
        ]
        .into(),
    );

    let result = merge_search_results(&[shard0, shard1], 5);
    match result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(1));
            assert_eq!(items[1], Frame::BulkString(Bytes::from("vec:5")));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_merge_search_results_empty() {
    // No results from any shard
    let shard0 = Frame::Array(vec![Frame::Integer(0)].into());
    let shard1 = Frame::Array(vec![Frame::Integer(0)].into());

    let result = merge_search_results(&[shard0, shard1], 10);
    match result {
        Frame::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Frame::Integer(0));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_ft_search_dimension_mismatch() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &args);

    // Build a query with wrong dimension (4 bytes instead of 128*4)
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        bulk(b"tooshort"),
    ];
    let result = ft_search(&mut store, &search_args);
    match &result {
        Frame::Error(e) => assert!(
            e.starts_with(b"ERR query vector dimension"),
            "expected dimension mismatch error, got {:?}",
            std::str::from_utf8(e)
        ),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_search_empty_index() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &args);

    // Build valid query for dim=128
    let query_vec: Vec<u8> = vec![0u8; 128 * 4]; // 128 floats, all zero
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    crate::vector::distance::init();
    let result = ft_search(&mut store, &search_args);
    match result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(0)); // no results
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_ft_info() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &args);

    let result = ft_info(&store, &[bulk(b"myidx")]);
    match result {
        Frame::Array(items) => {
            // Should have 20 items (10 key-value pairs)
            assert!(
                items.len() >= 20,
                "FT.INFO should return at least 20 items, got {}",
                items.len()
            );
            assert_eq!(
                items[0],
                Frame::BulkString(Bytes::from_static(b"index_name"))
            );
            assert_eq!(items[1], Frame::BulkString(Bytes::from("myidx")));
            assert_eq!(items[5], Frame::Integer(0)); // num_docs = 0
            assert_eq!(items[7], Frame::Integer(128)); // dimension
            // New fields
            assert_eq!(items[10], Frame::BulkString(Bytes::from_static(b"M")));
            assert_eq!(items[11], Frame::Integer(16)); // default M
            assert_eq!(
                items[14],
                Frame::BulkString(Bytes::from_static(b"EF_RUNTIME"))
            );
        }
        other => panic!("expected Array, got {other:?}"),
    }

    // Non-existing index
    let result = ft_info(&store, &[bulk(b"nonexistent")]);
    assert!(matches!(result, Frame::Error(_)));
}

/// Helper to build FT.CREATE args with custom parameters.
fn build_ft_create_args(
    name: &str,
    prefix: &str,
    field: &str,
    dim: u32,
    metric: &str,
) -> Vec<Frame> {
    vec![
        Frame::BulkString(Bytes::from(name.to_owned())),
        Frame::BulkString(Bytes::from_static(b"ON")),
        Frame::BulkString(Bytes::from_static(b"HASH")),
        Frame::BulkString(Bytes::from_static(b"PREFIX")),
        Frame::BulkString(Bytes::from_static(b"1")),
        Frame::BulkString(Bytes::from(prefix.to_owned())),
        Frame::BulkString(Bytes::from_static(b"SCHEMA")),
        Frame::BulkString(Bytes::from(field.to_owned())),
        Frame::BulkString(Bytes::from_static(b"VECTOR")),
        Frame::BulkString(Bytes::from_static(b"HNSW")),
        Frame::BulkString(Bytes::from_static(b"6")),
        Frame::BulkString(Bytes::from_static(b"TYPE")),
        Frame::BulkString(Bytes::from_static(b"FLOAT32")),
        Frame::BulkString(Bytes::from_static(b"DIM")),
        Frame::BulkString(Bytes::from(dim.to_string())),
        Frame::BulkString(Bytes::from_static(b"DISTANCE_METRIC")),
        Frame::BulkString(Bytes::from(metric.to_owned())),
    ]
}

#[test]
fn test_end_to_end_create_insert_search() {
    // Initialize distance functions (required before any search)
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let dim: usize = 4;

    // 1. FT.CREATE
    let create_args = build_ft_create_args("e2eidx", "doc:", "embedding", dim as u32, "L2");
    let result = ft_create(&mut store, &create_args);
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.CREATE should return OK, got {result:?}"
    );

    // 2. Insert vectors directly into the mutable segment
    let idx = store.get_index_mut(b"e2eidx").unwrap();
    let vectors: Vec<[f32; 4]> = vec![
        [1.0, 0.0, 0.0, 0.0],  // vec:0 -- exact match for query (L2=0)
        [-1.0, 0.0, 0.0, 0.0], // vec:1 -- opposite direction (L2=4.0)
        [0.5, 0.0, 0.0, 0.0],  // vec:2 -- same direction, half magnitude (L2=0.25)
    ];

    let snap = idx.segments.load();
    for (i, v) in vectors.iter().enumerate() {
        let mut sq = vec![0i8; dim];
        quantize_f32_to_sq(v, &mut sq);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        snap.mutable.append(i as u64, v, &sq, norm, i as u64);
    }
    drop(snap);

    // 3. FT.SEARCH for vector close to [1.0, 0.0, 0.0, 0.0]
    let query_vec: [f32; 4] = [1.0, 0.0, 0.0, 0.0];
    let query_blob: Vec<u8> = query_vec.iter().flat_map(|f| f.to_le_bytes()).collect();

    let search_args = vec![
        Frame::BulkString(Bytes::from_static(b"e2eidx")),
        Frame::BulkString(Bytes::from_static(b"*=>[KNN 2 @embedding $query]")),
        Frame::BulkString(Bytes::from_static(b"PARAMS")),
        Frame::BulkString(Bytes::from_static(b"2")),
        Frame::BulkString(Bytes::from_static(b"query")),
        Frame::BulkString(Bytes::from(query_blob)),
    ];

    let result = ft_search(&mut store, &search_args);
    match &result {
        Frame::Array(items) => {
            // First element is count
            assert!(
                matches!(&items[0], Frame::Integer(n) if *n >= 1),
                "Should find at least 1 result, got {result:?}"
            );
            // vec:0 should be in top-2 results (at dim=4, TQ-4bit quantization
            // noise can swap rankings of very close vectors in Light mode)
            let mut found_vec0 = false;
            for idx in [1, 3].iter() {
                if let Some(Frame::BulkString(doc_id)) = items.get(*idx) {
                    if doc_id.as_ref() == b"vec:0" {
                        found_vec0 = true;
                    }
                }
            }
            assert!(
                found_vec0,
                "vec:0 should be in top-2 results, got {result:?}"
            );
            // vec:2 should be in top-2 (at dim=4, TQ noise may reorder)
            let mut found_vec2 = false;
            for idx in [1, 3].iter() {
                if let Some(Frame::BulkString(doc_id)) = items.get(*idx) {
                    if doc_id.as_ref() == b"vec:2" {
                        found_vec2 = true;
                    }
                }
            }
            assert!(
                found_vec2,
                "vec:2 should be in top-2 results, got {result:?}"
            );
        }
        Frame::Error(e) => panic!("FT.SEARCH returned error: {:?}", std::str::from_utf8(e)),
        _ => panic!("FT.SEARCH should return Array, got {result:?}"),
    }
}

#[test]
fn test_ft_info_returns_correct_data() {
    let mut store = VectorStore::new();
    let args = build_ft_create_args("testidx", "test:", "vec", 128, "COSINE");
    ft_create(&mut store, &args);

    let info_args = [Frame::BulkString(Bytes::from_static(b"testidx"))];
    let result = ft_info(&store, &info_args);
    match result {
        Frame::Array(items) => {
            assert!(items.len() >= 6, "FT.INFO should return at least 6 items");
            // Check dimension
            let mut found_dim = false;
            for pair in items.chunks(2) {
                if let Frame::BulkString(key) = &pair[0] {
                    if key.as_ref() == b"dimension" {
                        if let Frame::Integer(d) = &pair[1] {
                            assert_eq!(*d, 128);
                            found_dim = true;
                        }
                    }
                }
            }
            assert!(found_dim, "FT.INFO should return dimension");
        }
        other => panic!("FT.INFO should return Array, got {other:?}"),
    }
}

#[test]
fn test_ft_search_unknown_index() {
    let mut store = VectorStore::new();
    let args = [
        Frame::BulkString(Bytes::from_static(b"nonexistent")),
        Frame::BulkString(Bytes::from_static(b"*=>[KNN 5 @vec $query]")),
        Frame::BulkString(Bytes::from_static(b"PARAMS")),
        Frame::BulkString(Bytes::from_static(b"2")),
        Frame::BulkString(Bytes::from_static(b"query")),
        Frame::BulkString(Bytes::from(vec![0u8; 16])),
    ];
    let result = ft_search(&mut store, &args);
    assert!(
        matches!(result, Frame::Error(_)),
        "Should error on unknown index, got {result:?}"
    );
}

#[test]
fn test_parse_filter_clause_tag() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@category:{electronics}"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"q"),
        bulk(b"blob"),
    ];
    let filter = parse_filter_clause(&args);
    assert!(filter.is_some(), "should parse @category:{{electronics}}");
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::TagEq { field, value } => {
            assert_eq!(&field[..], b"category");
            assert_eq!(&value[..], b"electronics");
        }
        other => panic!("expected TagEq, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_clause_numeric_range() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@price:[10 100]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"q"),
        bulk(b"blob"),
    ];
    let filter = parse_filter_clause(&args);
    assert!(filter.is_some());
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::NumRange { field, min, max } => {
            assert_eq!(&field[..], b"price");
            assert_eq!(*min, 10.0);
            assert_eq!(*max, 100.0);
        }
        other => panic!("expected NumRange, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_clause_numeric_eq() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@price:[50 50]"),
    ];
    let filter = parse_filter_clause(&args);
    assert!(filter.is_some());
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::NumEq { field, value } => {
            assert_eq!(&field[..], b"price");
            assert_eq!(*value, 50.0);
        }
        other => panic!("expected NumEq, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_clause_compound() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@a:{x} @b:[1 10]"),
    ];
    let filter = parse_filter_clause(&args);
    assert!(filter.is_some());
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::And(left, right) => {
            assert!(matches!(
                *left,
                crate::vector::filter::FilterExpr::TagEq { .. }
            ));
            assert!(matches!(
                *right,
                crate::vector::filter::FilterExpr::NumRange { .. }
            ));
        }
        other => panic!("expected And, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_clause_none() {
    // No FILTER keyword
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"q"),
        bulk(b"blob"),
    ];
    let filter = parse_filter_clause(&args);
    assert!(filter.is_none());
}

#[test]
fn test_ft_search_with_filter_no_regression() {
    // Unfiltered FT.SEARCH still works identically
    crate::vector::distance::init();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &args);

    let query_vec: Vec<u8> = vec![0u8; 128 * 4];
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    let result = ft_search(&mut store, &search_args);
    match result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(0));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_vector_index_has_payload_index() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &args);
    let idx = store.get_index(b"myidx").unwrap();
    // payload_index should exist -- insert and evaluate should work
    let _ = &idx.payload_index;
}

#[test]
fn test_vector_metrics_increment_decrement() {
    use std::sync::atomic::Ordering;

    // Capture before-snapshot immediately before each operation to handle
    // parallel test interference on global atomics.
    let mut store = VectorStore::new();
    let args = ft_create_args();

    // FT.CREATE should increment VECTOR_INDEXES
    let before_create = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    ft_create(&mut store, &args);
    let after_create = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    assert!(
        after_create > before_create,
        "FT.CREATE should increment VECTOR_INDEXES"
    );

    // FT.SEARCH should increment VECTOR_SEARCH_TOTAL
    crate::vector::distance::init();
    let before_search = crate::vector::metrics::VECTOR_SEARCH_TOTAL.load(Ordering::Relaxed);
    let query_vec: Vec<u8> = vec![0u8; 128 * 4];
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    ft_search(&mut store, &search_args);
    let after_search = crate::vector::metrics::VECTOR_SEARCH_TOTAL.load(Ordering::Relaxed);
    assert!(
        after_search > before_search,
        "FT.SEARCH should increment VECTOR_SEARCH_TOTAL"
    );

    // FT.DROPINDEX should decrement VECTOR_INDEXES
    let before_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    ft_dropindex(&mut store, &[bulk(b"myidx")]);
    let after_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    assert!(
        after_drop < before_drop,
        "FT.DROPINDEX should decrement VECTOR_INDEXES"
    );
}
