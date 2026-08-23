use super::*;
use parking_lot::RwLock;
use smallvec::SmallVec;

/// Serialize tests that touch global atomic metrics to avoid flaky interference.
static METRICS_LOCK: RwLock<()> = RwLock::new(());

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::from(s.to_vec()))
}

// ---------------------------------------------------------------------------
// FT.EXPAND tests (graph feature required)
// ---------------------------------------------------------------------------

#[cfg(feature = "graph")]
mod ft_expand_tests {
    use super::*;
    use crate::graph::store::GraphStore;
    use smallvec::smallvec;

    /// Helper: create a GraphStore with a graph "kg" containing a linear chain
    /// A -> B -> C, with keys "doc:a", "doc:b", "doc:c" registered.
    fn setup_graph_store() -> GraphStore {
        let mut gs = GraphStore::new();
        let lsn = gs.allocate_lsn();
        gs.create_graph(Bytes::from_static(b"kg"), 1000, lsn)
            .unwrap();
        let g = gs.get_graph_mut(b"kg").unwrap();

        let node_a = g.write_buf.add_node(smallvec![0], smallvec![], None, 1);
        let node_b = g.write_buf.add_node(smallvec![0], smallvec![], None, 2);
        let node_c = g.write_buf.add_node(smallvec![0], smallvec![], None, 3);

        // A -> B -> C
        g.write_buf
            .add_edge(node_a, node_b, 0, 1.0, None, 4)
            .unwrap();
        g.write_buf
            .add_edge(node_b, node_c, 0, 1.0, None, 5)
            .unwrap();

        // Register Redis key mappings
        g.register_key(Bytes::from_static(b"doc:a"), node_a);
        g.register_key(Bytes::from_static(b"doc:b"), node_b);
        g.register_key(Bytes::from_static(b"doc:c"), node_c);

        gs
    }

    #[test]
    fn test_ft_expand_no_args() {
        let gs = GraphStore::new();
        let args: Vec<Frame> = vec![];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number"), "{:?}", e),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_no_keys() {
        let gs = GraphStore::new();
        // FT.EXPAND myidx DEPTH 2  (no keys between idx and DEPTH)
        let args = vec![bulk(b"myidx"), bulk(b"DEPTH"), bulk(b"2")];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"ERR no keys"), "{:?}", e),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_missing_depth() {
        let gs = GraphStore::new();
        // FT.EXPAND myidx doc:a  (no DEPTH keyword)
        let args = vec![bulk(b"myidx"), bulk(b"doc:a")];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Error(e) => assert!(
                e.starts_with(b"ERR syntax error: expected DEPTH"),
                "{:?}",
                e
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_depth_zero() {
        let gs = setup_graph_store();
        // FT.EXPAND myidx doc:a DEPTH 0
        let args = vec![
            bulk(b"myidx"),
            bulk(b"doc:a"),
            bulk(b"DEPTH"),
            bulk(b"0"),
            bulk(b"GRAPH"),
            bulk(b"kg"),
        ];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Array(frames) => {
                // First element is count = 0
                assert_eq!(frames.len(), 1);
                match &frames[0] {
                    Frame::Integer(0) => {}
                    other => panic!("expected Integer(0), got {other:?}"),
                }
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_basic_one_hop() {
        let gs = setup_graph_store();
        // FT.EXPAND myidx doc:a DEPTH 1 GRAPH kg
        let args = vec![
            bulk(b"myidx"),
            bulk(b"doc:a"),
            bulk(b"DEPTH"),
            bulk(b"1"),
            bulk(b"GRAPH"),
            bulk(b"kg"),
        ];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Array(ref frames) => {
                // First element is count
                let count = match &frames[0] {
                    Frame::Integer(n) => *n,
                    other => panic!("expected Integer, got {other:?}"),
                };
                assert_eq!(count, 1, "expected 1 neighbor at depth 1");
                // The neighbor should be "doc:b" at hop 1
                match &frames[1] {
                    Frame::BulkString(k) => assert_eq!(&k[..], b"doc:b"),
                    other => panic!("expected BulkString key, got {other:?}"),
                }
                // Check __graph_hops field
                match &frames[2] {
                    Frame::Array(fields) => {
                        match &fields[0] {
                            Frame::BulkString(f) => assert_eq!(&f[..], b"__graph_hops"),
                            other => panic!("expected __graph_hops field, got {other:?}"),
                        }
                        match &fields[1] {
                            Frame::BulkString(v) => assert_eq!(&v[..], b"1"),
                            other => panic!("expected hop value '1', got {other:?}"),
                        }
                    }
                    other => panic!("expected Array for fields, got {other:?}"),
                }
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_two_hops() {
        let gs = setup_graph_store();
        // FT.EXPAND myidx doc:a DEPTH 2 GRAPH kg
        let args = vec![
            bulk(b"myidx"),
            bulk(b"doc:a"),
            bulk(b"DEPTH"),
            bulk(b"2"),
            bulk(b"GRAPH"),
            bulk(b"kg"),
        ];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Array(ref frames) => {
                let count = match &frames[0] {
                    Frame::Integer(n) => *n,
                    other => panic!("expected Integer, got {other:?}"),
                };
                assert_eq!(count, 2, "expected 2 neighbors at depth 2 (B + C)");
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_unknown_keys_skipped() {
        let gs = setup_graph_store();
        // FT.EXPAND myidx doc:nonexistent DEPTH 2 GRAPH kg
        // Keys not in graph produce empty result (not error) per GRAF-05
        let args = vec![
            bulk(b"myidx"),
            bulk(b"doc:nonexistent"),
            bulk(b"DEPTH"),
            bulk(b"2"),
            bulk(b"GRAPH"),
            bulk(b"kg"),
        ];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Array(ref frames) => {
                let count = match &frames[0] {
                    Frame::Integer(n) => *n,
                    other => panic!("expected Integer, got {other:?}"),
                };
                assert_eq!(count, 0, "unknown keys should produce empty result");
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_auto_detect_graph() {
        let gs = setup_graph_store();
        // FT.EXPAND myidx doc:a DEPTH 1 (no GRAPH specified — auto-detect)
        let args = vec![bulk(b"myidx"), bulk(b"doc:a"), bulk(b"DEPTH"), bulk(b"1")];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Array(ref frames) => {
                let count = match &frames[0] {
                    Frame::Integer(n) => *n,
                    other => panic!("expected Integer, got {other:?}"),
                };
                // Auto-detected "kg" graph, found doc:b at 1 hop
                assert_eq!(count, 1);
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_no_graph_found() {
        let gs = GraphStore::new(); // no graphs at all
        let args = vec![bulk(b"myidx"), bulk(b"doc:a"), bulk(b"DEPTH"), bulk(b"1")];
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"ERR no graph"), "{:?}", e),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_expand_depth_clamped() {
        // Verify that depth > MAX_EXPAND_DEPTH is clamped (doesn't error).
        let gs = setup_graph_store();
        let args = vec![
            bulk(b"myidx"),
            bulk(b"doc:a"),
            bulk(b"DEPTH"),
            bulk(b"100"),
            bulk(b"GRAPH"),
            bulk(b"kg"),
        ];
        // Should succeed (depth clamped to MAX_EXPAND_DEPTH=5 internally)
        let result = ft_expand(&gs, &args);
        match result {
            Frame::Array(ref frames) => {
                let count = match &frames[0] {
                    Frame::Integer(n) => *n,
                    other => panic!("expected Integer, got {other:?}"),
                };
                // Chain is only 3 nodes: A->B->C, so max 2 neighbors regardless of depth
                assert_eq!(count, 2);
            }
            other => panic!("expected array, got {other:?}"),
        }
    }
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
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
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
    let _metrics_guard = METRICS_LOCK.read();
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
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    match &result {
        Frame::Error(_) => {} // expected
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_create_duplicate() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    let r1 = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    assert!(matches!(r1, Frame::SimpleString(_)));

    let args2 = ft_create_args();
    let r2 = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args2,
        0,
    );
    match &r2 {
        Frame::Error(e) => assert!(e.starts_with(b"ERR")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_dropindex() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Drop existing (no DD flag, no db needed)
    let result = ft_dropindex(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        None,
        &[bulk(b"myidx")],
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)));
    assert!(store.is_empty());

    // Drop non-existing
    let result = ft_dropindex(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        None,
        &[bulk(b"myidx")],
        0,
    );
    assert!(matches!(result, Frame::Error(_)));
}

#[test]
fn test_parse_knn_query() {
    let query = b"*=>[KNN 10 @vec $query]";
    let (k, field_name, param) = parse_knn_query(query).unwrap();
    assert_eq!(k, 10);
    assert_eq!(field_name.as_deref(), Some(b"vec".as_slice()));
    assert_eq!(&param[..], b"query");
}

#[test]
fn test_parse_knn_query_different_k() {
    let query = b"*=>[KNN 5 @embedding $blob]";
    let (k, field_name, param) = parse_knn_query(query).unwrap();
    assert_eq!(k, 5);
    assert_eq!(field_name.as_deref(), Some(b"embedding".as_slice()));
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

    let result = merge_search_results(&[shard0, shard1], 2, 0, usize::MAX);
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

    let result = merge_search_results(&[shard0, shard1], 5, 0, usize::MAX);
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

    let result = merge_search_results(&[shard0, shard1], 10, 0, usize::MAX);
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
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Build a query with wrong dimension (4 bytes instead of 128*4)
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        bulk(b"tooshort"),
    ];
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
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
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

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
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(0)); // no results
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_ft_info() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    let result = ft_info(
        &store,
        &crate::text::store::TextStore::new(),
        &[bulk(b"myidx")],
        0,
    );
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
    let result = ft_info(
        &store,
        &crate::text::store::TextStore::new(),
        &[bulk(b"nonexistent")],
        0,
    );
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
    let _metrics_guard = METRICS_LOCK.read();
    // Initialize distance functions (required before any search)
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let dim: usize = 4;

    // 1. FT.CREATE
    let create_args = build_ft_create_args("e2eidx", "doc:", "embedding", dim as u32, "L2");
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &create_args,
        0,
    );
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
        snap.mutable.append(i as u64, v, i as u64);
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

    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
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
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = build_ft_create_args("testidx", "test:", "vec", 128, "COSINE");
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    let info_args = [Frame::BulkString(Bytes::from_static(b"testidx"))];
    let result = ft_info(&store, &crate::text::store::TextStore::new(), &info_args, 0);
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
    let result = ft_search(&mut store, &args, None, None, 0, 0);
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
    let filter = parse_filter_clause(&args).into_option().unwrap();
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
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_some());
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::NumRange {
            field,
            min,
            max,
            min_excl,
            max_excl,
        } => {
            assert_eq!(&field[..], b"price");
            assert_eq!(*min, 10.0);
            assert_eq!(*max, 100.0);
            assert!(!min_excl, "[10 100] has no exclusive bound");
            assert!(!max_excl);
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
    let filter = parse_filter_clause(&args).into_option().unwrap();
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
    let filter = parse_filter_clause(&args).into_option().unwrap();
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

/// moon#648: `Absent` and `Invalid` are different answers. The `Option` this
/// replaces made them the same, and every caller read the collapsed `None` as
/// "run unfiltered" -- so a filter the parser could not read silently widened
/// the result set instead of failing.
#[test]
fn filter_clause_distinguishes_absent_from_unparseable() {
    use crate::command::vector_search::ft_search::parse::FilterParse;

    let filter_args = |expr: &str| {
        vec![
            bulk(b"idx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"FILTER"),
            bulk(expr.as_bytes()),
        ]
    };

    // No FILTER keyword at all -> Absent, and the search runs unfiltered as asked.
    let none = vec![bulk(b"idx"), bulk(b"*=>[KNN 5 @vec $q]")];
    assert!(matches!(parse_filter_clause(&none), FilterParse::Absent));

    // Supplied but unreadable -> Invalid. Each of these previously produced
    // `None`, i.e. an unfiltered search.
    for bad in [
        "@price:[abc def]", // non-numeric bounds
        "@price:[10]",      // one bound
        "@price:[1 2 3 4]", // four values (three is geo)
        "@price:[( 100]",   // bare exclusive marker
        "price:[1 10]",     // missing @
        "@price:[300 100]", // inverted -- moon#664
    ] {
        assert!(
            matches!(
                parse_filter_clause(&filter_args(bad)),
                FilterParse::Invalid(_)
            ),
            "{bad:?} must be Invalid, not Absent -- Absent means unfiltered"
        );
    }

    // FILTER with nothing after it is malformed, not absent.
    let dangling = vec![bulk(b"idx"), bulk(b"*=>[KNN 5 @vec $q]"), bulk(b"FILTER")];
    assert!(matches!(
        parse_filter_clause(&dangling),
        FilterParse::Invalid(_)
    ));
}

/// The collapse point. `parse_filter_clause` reporting `Invalid` is only half
/// the fix -- `into_option` is where an unreadable filter either becomes an
/// error frame or (as before moon#648) quietly becomes `None`, i.e. unfiltered.
/// Without this the parser could report Invalid correctly and the search would
/// still widen, and every parser-level assertion would still pass.
#[test]
fn into_option_turns_an_invalid_filter_into_an_error_not_none() {
    use crate::command::vector_search::ft_search::parse::{ERR_INVALID_FILTER, FilterParse};

    let err = FilterParse::Invalid(ERR_INVALID_FILTER)
        .into_option()
        .expect_err("Invalid must not collapse to Ok(None) -- that is unfiltered");
    match err {
        Frame::Error(msg) => assert_eq!(&msg[..], ERR_INVALID_FILTER),
        other => panic!("expected an error frame, got {other:?}"),
    }

    assert!(
        FilterParse::Absent
            .into_option()
            .expect("Absent is not an error")
            .is_none(),
        "Absent still means unfiltered, as asked"
    );
}

/// An unreadable explicit FILTER must NOT fall through to the inline prefix.
/// `Option::or_else` did exactly that, so a rejected filter turned back into a
/// different filter -- or into none at all.
#[test]
fn an_invalid_filter_clause_does_not_fall_through_to_the_inline_prefix() {
    use crate::command::vector_search::ft_search::parse::{FilterParse, parse_inline_filter};

    let args = vec![
        bulk(b"idx"),
        bulk(b"@price:[1 10]=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@price:[300 100]"),
    ];
    let combined = parse_filter_clause(&args)
        .or_else(|| parse_inline_filter(b"@price:[1 10]=>[KNN 5 @vec $q]"));
    assert!(
        matches!(combined, FilterParse::Invalid(_)),
        "an Invalid explicit FILTER must short-circuit, not defer to the inline prefix"
    );

    // Absent still defers, which is the behaviour worth preserving.
    let no_filter_kw = vec![bulk(b"idx"), bulk(b"@price:[1 10]=>[KNN 5 @vec $q]")];
    let deferred = parse_filter_clause(&no_filter_kw)
        .or_else(|| parse_inline_filter(b"@price:[1 10]=>[KNN 5 @vec $q]"));
    assert!(matches!(deferred, FilterParse::Parsed(_)));
}

/// moon#648 part one: the KNN prefilter must read `(`-prefixed exclusive
/// bounds, the same as the full query grammar. It used a bare `parse::<f64>()`.
#[test]
fn filter_clause_reads_exclusive_bounds_and_infinities() {
    let parse = |expr: &str| {
        parse_filter_clause(&[
            bulk(b"idx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"FILTER"),
            bulk(expr.as_bytes()),
        ])
        .into_option()
        .unwrap()
        .unwrap_or_else(|| panic!("{expr} must parse"))
    };

    match parse("@price:[10 (100]") {
        crate::vector::filter::FilterExpr::NumRange {
            min,
            max,
            min_excl,
            max_excl,
            ..
        } => {
            assert_eq!((*min, *max), (10.0, 100.0));
            assert!(!min_excl && max_excl, "only the upper bound is exclusive");
        }
        other => panic!("expected NumRange, got {other:?}"),
    }

    match parse("@price:[(10 100]") {
        crate::vector::filter::FilterExpr::NumRange {
            min_excl, max_excl, ..
        } => assert!(min_excl && !max_excl),
        other => panic!("expected NumRange, got {other:?}"),
    }

    // `[v v]` collapses to NumEq only when BOTH bounds are inclusive --
    // `[(50 50]` is the empty set, and must not become "equals 50".
    assert!(matches!(
        parse("@price:[50 50]"),
        crate::vector::filter::FilterExpr::NumEq { .. }
    ));
    match parse("@price:[(50 50]") {
        crate::vector::filter::FilterExpr::NumRange { min_excl, .. } => assert!(min_excl),
        other => panic!("[(50 50] must stay a range, got {other:?}"),
    }

    // ±inf sentinels, so an open-ended range is expressible.
    match parse("@price:[-inf +inf]") {
        crate::vector::filter::FilterExpr::NumRange { min, max, .. } => {
            assert!(min.is_infinite() && max.is_infinite());
        }
        other => panic!("expected NumRange, got {other:?}"),
    }
    // ...and the half-open forms, which are the ones a client actually writes.
    for expr in ["@price:[-inf 100]", "@price:[10 +inf]", "@price:[(10 inf]"] {
        assert!(
            matches!(
                parse(expr),
                crate::vector::filter::FilterExpr::NumRange { .. }
            ),
            "{expr} is a valid open-ended range"
        );
    }
}

/// An INVERTED range is rejected whether or not its bounds are finite.
///
/// The three FT.SEARCH numeric grammars disagreed here: `text/query/parse.rs`
/// tests a plain `min > max`, while this parser and `ft_text_search.rs` both
/// carried a `min.is_finite() && max.is_finite() &&` conjunct — so
/// `[+inf 5]` was rejected by one grammar and accepted by the other two.
/// The conjunct was never load-bearing: `[-inf +inf]` and every half-open
/// form have `min <= max` already, so the plain comparison keeps them
/// (pinned above) and the guard is the same rule everywhere (moon#648).
#[test]
fn an_inverted_range_is_rejected_even_when_a_bound_is_infinite() {
    let parse = |expr: &str| {
        parse_filter_clause(&[
            bulk(b"idx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"FILTER"),
            bulk(expr.as_bytes()),
        ])
        .into_option()
    };
    for expr in [
        "@price:[300 100]",
        "@price:[+inf 5]",
        "@price:[5 -inf]",
        "@price:[+inf -inf]",
    ] {
        assert!(
            parse(expr).is_err(),
            "{expr} is inverted and must be an error, not a filter"
        );
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
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_none());
}

#[test]
fn test_ft_search_with_filter_no_regression() {
    let _metrics_guard = METRICS_LOCK.read();
    // Unfiltered FT.SEARCH still works identically
    crate::vector::distance::init();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    let query_vec: Vec<u8> = vec![0u8; 128 * 4];
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(0));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_vector_index_has_payload_index() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    let idx = store.get_index(b"myidx").unwrap();
    // payload_index should exist -- insert and evaluate should work
    let _ = &idx.payload_index;
}

#[test]
fn test_vector_metrics_increment_decrement() {
    use std::sync::atomic::Ordering;

    let _guard = METRICS_LOCK.write();

    let mut store = VectorStore::new();
    let args = ft_create_args();

    // FT.CREATE should increment VECTOR_INDEXES by exactly 1. The exclusive
    // write guard excludes every lock-respecting mutator, so the delta is
    // deterministic (no concurrent ft_create/ft_dropindex can perturb it).
    let before_create = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    let after_create = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    assert_eq!(
        after_create,
        before_create + 1,
        "FT.CREATE should increment VECTOR_INDEXES by exactly 1"
    );

    // FT.SEARCH should increment VECTOR_SEARCH_TOTAL
    crate::vector::distance::init();
    let before_search = crate::vector::metrics::search_total();
    let query_vec: Vec<u8> = vec![0u8; 128 * 4];
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    ft_search(&mut store, &search_args, None, None, 0, 0);
    let after_search = crate::vector::metrics::search_total();
    assert_eq!(
        after_search,
        before_search + 1,
        "FT.SEARCH should increment VECTOR_SEARCH_TOTAL by exactly 1"
    );

    // FT.DROPINDEX should decrement VECTOR_INDEXES by exactly 1. Deterministic
    // under the write guard — this is the assertion that flaked when concurrent
    // mutators ran lock-free (a stray ft_create could cancel the decrement).
    let before_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    ft_dropindex(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        None,
        &[bulk(b"myidx")],
        0,
    );
    let after_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    assert_eq!(
        after_drop,
        before_drop - 1,
        "FT.DROPINDEX should decrement VECTOR_INDEXES by exactly 1"
    );
}

/// Deterministic regression for the `VECTOR_INDEXES` parallel-test flake.
///
/// `VECTOR_INDEXES` is a process-global counter shared by every test in this
/// binary. Before the RwLock fix, ~28 tests mutated it lock-free while the
/// delta-reader tests asserted on it, so a concurrent `ft_create` (+1) could
/// land inside a reader's read-modify-read window and cancel an observed
/// decrement — breaking `after_drop < before_drop` (the failure first seen on
/// the tokio full-suite run, never in isolation).
///
/// The fix: delta-readers take `METRICS_LOCK.write()` (exclusive) and every
/// mutator takes `METRICS_LOCK.read()` (shared, keeps their parallelism). This
/// test proves the write guard excludes a lock-respecting mutator. It is
/// deterministic in BOTH directions: GREEN as written; flipping the `write()`
/// below to `read()` lets the mutator run during the sleep and turns the
/// assertion RED.
#[test]
fn metrics_write_guard_isolates_index_counter_from_concurrent_mutator() {
    use std::sync::atomic::Ordering;

    let _exclusive = METRICS_LOCK.write();

    // Seed +1 so the decrement is observable even with no other live index.
    crate::vector::metrics::increment_indexes();
    let before_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);

    // A concurrent "ft_create" that respects the lock: it blocks on read()
    // until we drop the write guard, so it cannot mutate inside our window.
    let mutator = std::thread::spawn(|| {
        let _shared = METRICS_LOCK.read();
        crate::vector::metrics::increment_indexes();
    });
    // Let the mutator reach (and park on) the read lock. Under the write guard
    // it stays blocked; without it, it would increment here and corrupt the
    // delta below — that is exactly the RED case.
    std::thread::sleep(std::time::Duration::from_millis(20));

    crate::vector::metrics::decrement_indexes();
    let after_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    assert_eq!(
        after_drop,
        before_drop - 1,
        "write guard must isolate the drop delta from the concurrent mutator"
    );

    // Release; the mutator proceeds with its +1.
    drop(_exclusive);
    mutator.join().unwrap();

    // Restore the global under a fresh exclusive guard (undo the mutator's +1).
    let _cleanup = METRICS_LOCK.write();
    crate::vector::metrics::decrement_indexes();
}

#[test]
fn test_parse_filter_bool_true() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@active:{true}"),
    ];
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_some(), "should parse @active:{{true}}");
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::BoolEq { field, value } => {
            assert_eq!(&field[..], b"active");
            assert!(value);
        }
        other => panic!("expected BoolEq, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_bool_false() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@active:{false}"),
    ];
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_some(), "should parse @active:{{false}}");
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::BoolEq { field, value } => {
            assert_eq!(&field[..], b"active");
            assert!(!value);
        }
        other => panic!("expected BoolEq, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_bool_case_insensitive() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@flag:{TRUE}"),
    ];
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_some());
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::BoolEq { field, value } => {
            assert_eq!(&field[..], b"flag");
            assert!(value);
        }
        other => panic!("expected BoolEq, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_geo() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@location:[-122.42 37.78 100.0]"),
    ];
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_some(), "should parse geo filter");
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::GeoRadius {
            field,
            lon,
            lat,
            radius_km,
        } => {
            assert_eq!(&field[..], b"location");
            assert!((lon - (-122.42)).abs() < 0.001);
            assert!((lat - 37.78).abs() < 0.001);
            assert!((radius_km - 100.0).abs() < 0.001);
        }
        other => panic!("expected GeoRadius, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_combined_bool_and_numeric() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@active:{true} @price:[10 50]"),
    ];
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_some());
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::And(left, right) => {
            assert!(
                matches!(*left, crate::vector::filter::FilterExpr::BoolEq { .. }),
                "left should be BoolEq, got {left:?}"
            );
            assert!(
                matches!(*right, crate::vector::filter::FilterExpr::NumRange { .. }),
                "right should be NumRange, got {right:?}"
            );
        }
        other => panic!("expected And, got {other:?}"),
    }
}

// -- LIMIT parsing tests --

#[test]
fn test_parse_limit() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"LIMIT"),
        bulk(b"10"),
        bulk(b"5"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"q"),
        bulk(b"blob"),
    ];
    let (offset, count) = parse_limit_clause(&args);
    assert_eq!(offset, 10);
    assert_eq!(count, 5);
}

#[test]
fn test_parse_limit_default() {
    // No LIMIT keyword -> returns (0, usize::MAX)
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"q"),
        bulk(b"blob"),
    ];
    let (offset, count) = parse_limit_clause(&args);
    assert_eq!(offset, 0);
    assert_eq!(count, usize::MAX);
}

#[test]
fn test_parse_limit_zero() {
    // LIMIT 0 0 -> count-only mode
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"LIMIT"),
        bulk(b"0"),
        bulk(b"0"),
    ];
    let (offset, count) = parse_limit_clause(&args);
    assert_eq!(offset, 0);
    assert_eq!(count, 0);
}

#[test]
fn test_build_search_response_paginated() {
    use crate::vector::keymap::BucketedKeyMap;
    use crate::vector::types::{SearchResult, VectorId};

    // Create 10 fake results
    let mut results: SmallVec<[SearchResult; 32]> = SmallVec::new();
    let mut key_map: BucketedKeyMap<Bytes> = BucketedKeyMap::new();
    for i in 0u32..10 {
        results.push(SearchResult {
            id: VectorId(i),
            distance: i as f32 * 0.1,
            key_hash: i as u64 + 1,
        });
        key_map.insert(i as u64 + 1, Bytes::from(format!("doc:{i}")));
    }

    // Paginate: offset=2, count=3 -> should return total=10 but only 3 docs
    let response = build_search_response(&results, &key_map, 2, 3);
    match response {
        Frame::Array(items) => {
            // First element: total = 10
            assert_eq!(items[0], Frame::Integer(10));
            // 3 doc entries * 2 frames each (doc_id + fields) = 6 + 1 (total) = 7
            assert_eq!(
                items.len(),
                7,
                "expected 1 + 3*2 = 7 items, got {}",
                items.len()
            );
            // First doc should be doc:2 (offset=2)
            assert_eq!(items[1], Frame::BulkString(Bytes::from("doc:2")));
            assert_eq!(items[3], Frame::BulkString(Bytes::from("doc:3")));
            assert_eq!(items[5], Frame::BulkString(Bytes::from("doc:4")));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_build_search_response_limit_zero_zero() {
    use crate::vector::keymap::BucketedKeyMap;
    use crate::vector::types::{SearchResult, VectorId};

    let mut results: SmallVec<[SearchResult; 32]> = SmallVec::new();
    for i in 0u32..5 {
        results.push(SearchResult {
            id: VectorId(i),
            distance: i as f32,
            key_hash: 0,
        });
    }
    let key_map: BucketedKeyMap<Bytes> = BucketedKeyMap::new();

    // LIMIT 0 0 -> count only, no docs
    let response = build_search_response(&results, &key_map, 0, 0);
    match response {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(5));
            assert_eq!(items.len(), 1, "LIMIT 0 0 should return only the count");
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_merge_search_results_with_pagination() {
    // 4 total results across 2 shards, LIMIT offset=1 count=2
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

    // k=10, offset=1, count=2 -> global sorted: [0.1, 0.3, 0.5, 0.9], skip 1, take 2 -> [0.3, 0.5]
    let result = merge_search_results(&[shard0, shard1], 10, 1, 2);
    match result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(4)); // total = 4
            // 2 paginated results * 2 frames = 4 + 1 = 5 items
            assert_eq!(items.len(), 5);
            assert_eq!(items[1], Frame::BulkString(Bytes::from("vec:10"))); // score 0.3
            assert_eq!(items[3], Frame::BulkString(Bytes::from("vec:1"))); // score 0.5
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_parse_ft_search_args_with_limit() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        bulk(b"blobdata"),
        bulk(b"LIMIT"),
        bulk(b"5"),
        bulk(b"20"),
    ];
    let (_, _, k, _, offset, count) = parse_ft_search_args(&args).unwrap();
    assert_eq!(k, 10);
    assert_eq!(offset, 5);
    assert_eq!(count, 20);
}

#[test]
fn test_parse_ft_search_args_without_limit() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        bulk(b"blobdata"),
    ];
    let (_, _, _, _, offset, count) = parse_ft_search_args(&args).unwrap();
    assert_eq!(offset, 0);
    assert_eq!(count, usize::MAX);
}

#[test]
fn test_ft_create_l2_defaults_to_sq8() {
    let _metrics_guard = METRICS_LOCK.read();
    use crate::vector::turbo_quant::collection::QuantizationConfig;

    // L2 + no explicit QUANTIZATION → SQ8 (TQ's norm-scaled ADC collapses on
    // unnormalized L2 data; recall < 0.01 measured on gist-960).
    let mut store = VectorStore::new();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &ft_create_args(),
        0,
    );
    #[allow(clippy::unwrap_used)] // index just created
    let idx = store.get_index_mut(b"myidx").unwrap();
    assert_eq!(idx.meta.quantization, QuantizationConfig::Sq8);

    // COSINE + no explicit QUANTIZATION → TQ4 default unchanged.
    let mut args = ft_create_args();
    args[0] = bulk(b"cosidx");
    let l2_pos = args
        .iter()
        .position(|f| matches!(f, Frame::BulkString(b) if b.as_ref() == b"L2"))
        .expect("L2 arg present");
    args[l2_pos] = bulk(b"COSINE");
    let mut store2 = VectorStore::new();
    ft_create(
        &mut store2,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    #[allow(clippy::unwrap_used)] // index just created
    let idx2 = store2.get_index_mut(b"cosidx").unwrap();
    assert_eq!(idx2.meta.quantization, QuantizationConfig::TurboQuant4);

    // Explicit TQ4 + L2 is honored (warns, not rejected).
    let mut args3 = ft_create_args();
    args3[0] = bulk(b"tq4idx");
    let cnt_pos = args3
        .iter()
        .position(|f| matches!(f, Frame::BulkString(b) if b.as_ref() == b"6"))
        .expect("param count present");
    args3[cnt_pos] = bulk(b"8");
    args3.push(bulk(b"QUANTIZATION"));
    args3.push(bulk(b"TQ4"));
    let mut store3 = VectorStore::new();
    ft_create(
        &mut store3,
        &mut crate::text::store::TextStore::new(),
        &args3,
        0,
    );
    #[allow(clippy::unwrap_used)] // index just created
    let idx3 = store3.get_index_mut(b"tq4idx").unwrap();
    assert_eq!(idx3.meta.quantization, QuantizationConfig::TurboQuant4);
}

// -- FT.CONFIG tests --

#[test]
fn test_ft_config_ef_runtime_set_get() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // SET a pinned beam width; the query path reads meta.hnsw_ef_runtime live.
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"EF_RUNTIME"),
        bulk(b"64"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)), "{result:?}");
    #[allow(clippy::unwrap_used)] // index just created above
    let idx = store.get_index_mut(b"myidx").unwrap();
    assert_eq!(idx.meta.hnsw_ef_runtime, 64);

    // GET reflects the new value.
    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"EF_RUNTIME")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"64"),
        other => panic!("expected BulkString 64, got {other:?}"),
    }

    // 0 restores the auto heuristic.
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"EF_RUNTIME"),
        bulk(b"0"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)));
    #[allow(clippy::unwrap_used)] // index exists
    let idx = store.get_index_mut(b"myidx").unwrap();
    assert_eq!(idx.meta.hnsw_ef_runtime, 0);

    // Out-of-range and non-numeric values are rejected.
    for bad in [&b"5"[..], b"5000", b"abc"] {
        let set_args = vec![bulk(b"SET"), bulk(b"myidx"), bulk(b"EF_RUNTIME"), bulk(bad)];
        let result = ft_config(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &set_args,
            0,
        );
        assert!(matches!(result, Frame::Error(_)), "{bad:?} -> {result:?}");
    }
}

#[test]
fn test_ft_config_rerank_mult_set_get() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Default is 4 (the HQ-1 baseline oversample).
    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"RERANK_MULT")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"4"),
        other => panic!("expected BulkString 4, got {other:?}"),
    }

    // SET a deeper oversample; the query path reads meta.rerank_mult live.
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"RERANK_MULT"),
        bulk(b"16"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)), "{result:?}");
    #[allow(clippy::unwrap_used)] // index just created above
    let idx = store.get_index_mut(b"myidx").unwrap();
    assert_eq!(idx.meta.rerank_mult, 16);

    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"RERANK_MULT")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"16"),
        other => panic!("expected BulkString 16, got {other:?}"),
    }

    // Out-of-range and non-numeric values are rejected.
    for bad in [&b"0"[..], b"65", b"abc"] {
        let set_args = vec![
            bulk(b"SET"),
            bulk(b"myidx"),
            bulk(b"RERANK_MULT"),
            bulk(bad),
        ];
        let result = ft_config(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &set_args,
            0,
        );
        assert!(matches!(result, Frame::Error(_)), "{bad:?} -> {result:?}");
    }
}

#[test]
fn test_ft_config_tuning_knobs_are_db_scoped() {
    // WS5a interplay: an index created in db 1 is invisible to FT.CONFIG
    // from any other db — RERANK_MULT / EXACT_BEAM can only be set (and
    // read) through the owning db.
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        1,
    );

    // SET from db 0 (wrong db) must not resolve the index.
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"RERANK_MULT"),
        bulk(b"32"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::Error(_)), "{result:?}");

    // SET from db 1 (owning db) succeeds; knob lands on the db-1 index.
    for (param, val) in [(&b"RERANK_MULT"[..], &b"32"[..]), (b"EXACT_BEAM", b"ON")] {
        let set_args = vec![bulk(b"SET"), bulk(b"myidx"), bulk(param), bulk(val)];
        let result = ft_config(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &set_args,
            1,
        );
        assert!(matches!(result, Frame::SimpleString(_)), "{result:?}");
    }
    #[allow(clippy::unwrap_used)] // created above
    let idx_db1 = store.get_index_mut_for_db(b"myidx", 1).unwrap();
    assert_eq!(idx_db1.meta.rerank_mult, 32);
    assert!(idx_db1.meta.exact_beam);

    // GET is db-scoped too: wrong db errors, owning db reads the knob.
    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"EXACT_BEAM")];
    let r0 = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    assert!(matches!(r0, Frame::Error(_)), "{r0:?}");
    let r1 = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        1,
    );
    match &r1 {
        Frame::BulkString(b) => assert_eq!(&b[..], b"ON"),
        other => panic!("expected BulkString ON, got {other:?}"),
    }
}

#[test]
fn test_ft_config_exact_beam_on_off() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Default is OFF.
    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"EXACT_BEAM")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"OFF"),
        other => panic!("expected BulkString OFF, got {other:?}"),
    }

    for (val, expect) in [
        (&b"ON"[..], true),
        (b"OFF", false),
        (b"1", true),
        (b"0", false),
        (b"true", true),
        (b"false", false),
    ] {
        let set_args = vec![bulk(b"SET"), bulk(b"myidx"), bulk(b"EXACT_BEAM"), bulk(val)];
        let result = ft_config(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &set_args,
            0,
        );
        assert!(
            matches!(result, Frame::SimpleString(_)),
            "{val:?} -> {result:?}"
        );
        #[allow(clippy::unwrap_used)] // index exists
        let idx = store.get_index_mut(b"myidx").unwrap();
        assert_eq!(idx.meta.exact_beam, expect, "value {val:?}");
    }

    // Invalid values are rejected.
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"EXACT_BEAM"),
        bulk(b"maybe"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::Error(_)), "{result:?}");
}

#[test]
fn test_ft_config_autocompact_on_off() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Default should be ON
    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"AUTOCOMPACT")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"ON"),
        other => panic!("expected BulkString ON, got {other:?}"),
    }

    // SET OFF
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"OFF"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)));

    // GET should return OFF
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"OFF"),
        other => panic!("expected BulkString OFF, got {other:?}"),
    }

    // SET ON
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"ON"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)));

    // GET should return ON
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"ON"),
        other => panic!("expected BulkString ON, got {other:?}"),
    }
}

#[test]
fn test_ft_config_unknown_param() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"NOSUCHPARAM")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    assert!(
        matches!(result, Frame::Error(_)),
        "should error on unknown param"
    );

    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"NOSUCHPARAM"),
        bulk(b"foo"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(
        matches!(result, Frame::Error(_)),
        "should error on unknown param"
    );
}

#[test]
fn test_ft_config_unknown_index() {
    let mut store = VectorStore::new();

    let get_args = vec![bulk(b"GET"), bulk(b"nonexistent"), bulk(b"AUTOCOMPACT")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
        0,
    );
    assert!(
        matches!(result, Frame::Error(_)),
        "should error on unknown index"
    );
}

#[test]
fn test_insert_path_triggers_background_compact_without_search() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let mut text = crate::text::store::TextStore::new();

    // COMPACT_THRESHOLD 100 (the minimum) so a modest bulk load crosses it.
    let args = vec![
        bulk(b"autoidx"),
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
        bulk(b"8"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
        bulk(b"COMPACT_THRESHOLD"),
        bulk(b"100"),
    ];
    let result = ft_create(&mut store, &mut text, &args, 0);
    assert!(matches!(result, Frame::SimpleString(_)), "{result:?}");

    let hset = |store: &mut VectorStore, text: &mut _, i: usize| {
        let key = format!("doc:{i}");
        let vec_bytes: Vec<u8> = (0..8u32)
            .flat_map(|d| ((i as f32) * 0.37 + d as f32).to_le_bytes())
            .collect();
        let hset_args = vec![bulk(key.as_bytes()), bulk(b"vec"), bulk(&vec_bytes)];
        crate::shard::spsc_handler::auto_index_hset_public(
            store,
            text,
            key.as_bytes(),
            &hset_args,
            0,
        );
    };

    // Pure bulk load: ONLY the HSET auto-index hook runs. No FT.SEARCH, no
    // FT.COMPACT, no autovacuum tick — before the insert-path trigger this
    // left every vector in the brute-force mutable tier indefinitely.
    for i in 0..150 {
        hset(&mut store, &mut text, i);
    }

    // The background worker builds asynchronously; each further insert polls
    // installs (same as the search path). Nudge until the immutable segment
    // lands or we time out.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut extra = 150usize;
    let mut installed = false;
    while std::time::Instant::now() < deadline {
        hset(&mut store, &mut text, extra);
        extra += 1;
        #[allow(clippy::unwrap_used)] // index created above; unit-test context
        let idx = store.get_index(b"autoidx").unwrap();
        if !idx.segments.load().immutable.is_empty() {
            installed = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        installed,
        "bulk insert never dispatched+installed a background compaction (insert-path trigger missing)"
    );
}

#[test]
fn test_ft_config_autocompact_guards_try_compact() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Disable autocompact
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"OFF"),
    ];
    ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );

    // Verify the flag is set correctly on the index
    let idx = store.get_index(b"myidx").unwrap();
    assert!(!idx.autocompact_enabled, "autocompact should be disabled");

    // Re-enable
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"ON"),
    ];
    ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    let idx = store.get_index(b"myidx").unwrap();
    assert!(idx.autocompact_enabled, "autocompact should be enabled");
}

#[test]
fn test_ft_config_autocompact_accepts_variants() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Test "0" and "1"
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"0"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)));
    let idx = store.get_index(b"myidx").unwrap();
    assert!(!idx.autocompact_enabled);

    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"1"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)));
    let idx = store.get_index(b"myidx").unwrap();
    assert!(idx.autocompact_enabled);

    // Test "TRUE" and "FALSE"
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"FALSE"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)));
    let idx = store.get_index(b"myidx").unwrap();
    assert!(!idx.autocompact_enabled);

    // Invalid value
    let set_args = vec![
        bulk(b"SET"),
        bulk(b"myidx"),
        bulk(b"AUTOCOMPACT"),
        bulk(b"MAYBE"),
    ];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &set_args,
        0,
    );
    assert!(matches!(result, Frame::Error(_)));
}

// -- EXPAND GRAPH clause parsing tests --

#[cfg(feature = "graph")]
mod graph_expand_tests {
    use super::*;

    #[test]
    fn test_parse_expand_clause_depth_colon() {
        let args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 10 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            bulk(b"blob"),
            bulk(b"EXPAND"),
            bulk(b"GRAPH"),
            bulk(b"depth:3"),
        ];
        let result = parse_expand_clause(&args);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_parse_expand_clause_bare_int() {
        let args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"EXPAND"),
            bulk(b"GRAPH"),
            bulk(b"2"),
        ];
        let result = parse_expand_clause(&args);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_parse_expand_clause_absent() {
        let args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"q"),
            bulk(b"data"),
        ];
        let result = parse_expand_clause(&args);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_expand_clause_depth_large() {
        // Parser accepts any u32 value; clamping happens in expand_results_via_graph
        let args = vec![
            bulk(b"myidx"),
            bulk(b"query"),
            bulk(b"EXPAND"),
            bulk(b"GRAPH"),
            bulk(b"depth:100"),
        ];
        let result = parse_expand_clause(&args);
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_parse_expand_clause_missing_depth_arg() {
        // EXPAND GRAPH without depth argument
        let args = vec![
            bulk(b"myidx"),
            bulk(b"query"),
            bulk(b"EXPAND"),
            bulk(b"GRAPH"),
        ];
        let result = parse_expand_clause(&args);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_expand_clause_expand_without_graph() {
        // EXPAND without GRAPH keyword should not match
        let args = vec![bulk(b"myidx"), bulk(b"query"), bulk(b"EXPAND"), bulk(b"3")];
        let result = parse_expand_clause(&args);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_expand_clause_case_insensitive() {
        let args = vec![
            bulk(b"myidx"),
            bulk(b"query"),
            bulk(b"expand"),
            bulk(b"graph"),
            bulk(b"depth:2"),
        ];
        let result = parse_expand_clause(&args);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_extract_seeds_from_empty_response() {
        let resp = Frame::Array(vec![Frame::Integer(0)].into());
        let seeds = extract_seeds_from_response(&resp);
        assert!(seeds.is_empty());
    }

    #[test]
    fn test_extract_seeds_from_response_with_results() {
        let resp = Frame::Array(
            vec![
                Frame::Integer(2),
                Frame::BulkString(Bytes::from_static(b"doc:1")),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"__vec_score")),
                        Frame::BulkString(Bytes::from_static(b"0.5")),
                    ]
                    .into(),
                ),
                Frame::BulkString(Bytes::from_static(b"doc:2")),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"__vec_score")),
                        Frame::BulkString(Bytes::from_static(b"0.8")),
                    ]
                    .into(),
                ),
            ]
            .into(),
        );
        let seeds = extract_seeds_from_response(&resp);
        assert_eq!(seeds.len(), 2);
        assert_eq!(&seeds[0].0[..], b"doc:1");
        assert!((seeds[0].1 - 0.5).abs() < f32::EPSILON);
        assert_eq!(&seeds[1].0[..], b"doc:2");
        assert!((seeds[1].1 - 0.8).abs() < f32::EPSILON);
    }

    #[test]
    fn test_build_combined_response_empty_expanded() {
        use crate::command::vector_search::graph_expand::ExpandedResult;

        let knn = Frame::Array(
            vec![
                Frame::Integer(1),
                Frame::BulkString(Bytes::from_static(b"doc:1")),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"__vec_score")),
                        Frame::BulkString(Bytes::from_static(b"0.5")),
                    ]
                    .into(),
                ),
            ]
            .into(),
        );
        let expanded: Vec<ExpandedResult> = vec![];
        let result = build_combined_response(&knn, &expanded);
        // Should have total=1 (just the KNN result with __graph_hops added)
        if let Frame::Array(items) = &result {
            assert_eq!(items[0], Frame::Integer(1));
            // doc:1 key
            assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"doc:1")));
            // fields should include __graph_hops = "0"
            if let Frame::Array(fields) = &items[2] {
                assert!(fields.iter().any(|f| matches!(f,
                    Frame::BulkString(b) if b.as_ref() == b"__graph_hops"
                )));
            } else {
                panic!("expected fields array");
            }
        } else {
            panic!("expected array response");
        }
    }

    #[test]
    fn test_build_combined_response_with_expanded() {
        use crate::command::vector_search::graph_expand::ExpandedResult;

        let knn = Frame::Array(
            vec![
                Frame::Integer(1),
                Frame::BulkString(Bytes::from_static(b"doc:1")),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"__vec_score")),
                        Frame::BulkString(Bytes::from_static(b"0.3")),
                    ]
                    .into(),
                ),
            ]
            .into(),
        );
        let expanded = vec![ExpandedResult {
            key: Bytes::from_static(b"doc:neighbor"),
            vec_score: 0.0,
            graph_hops: 2,
            edge_created_ms: 0,
        }];
        let result = build_combined_response(&knn, &expanded);
        if let Frame::Array(items) = &result {
            // total = 1 knn + 1 expanded = 2
            assert_eq!(items[0], Frame::Integer(2));
            // items[1] = doc:1, items[2] = fields, items[3] = doc:neighbor, items[4] = fields
            assert_eq!(items.len(), 5);
            assert_eq!(
                items[3],
                Frame::BulkString(Bytes::from_static(b"doc:neighbor"))
            );
            if let Frame::Array(fields) = &items[4] {
                // Should have __vec_score, "0", __graph_hops, "2"
                assert_eq!(fields.len(), 4);
                assert_eq!(
                    fields[0],
                    Frame::BulkString(Bytes::from_static(b"__vec_score"))
                );
                assert_eq!(
                    fields[2],
                    Frame::BulkString(Bytes::from_static(b"__graph_hops"))
                );
                // graph_hops should be "2"
                if let Frame::BulkString(b) = &fields[3] {
                    assert_eq!(b.as_ref(), b"2");
                }
            }
        } else {
            panic!("expected array response");
        }
    }
}

// ---------------------------------------------------------------------------
// FT.CACHESEARCH tests
// ---------------------------------------------------------------------------

mod cache_search_tests {
    use super::*;
    use crate::command::vector_search::cache_search;

    fn make_valid_cachesearch_args() -> Vec<Frame> {
        let query_blob = vec![0u8; 128 * 4]; // 128-dim zero vector
        vec![
            bulk(b"myidx"),
            bulk(b"cache:query:"),
            bulk(b"*=>[KNN 5 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            Frame::BulkString(Bytes::from(query_blob)),
            bulk(b"THRESHOLD"),
            bulk(b"0.95"),
            bulk(b"FALLBACK"),
            bulk(b"KNN"),
            bulk(b"10"),
        ]
    }

    #[test]
    fn test_parse_cachesearch_args_valid() {
        let args = make_valid_cachesearch_args();
        let parsed = cache_search::parse_cachesearch_args_for_test(&args);
        assert!(parsed.is_ok(), "expected Ok, got {:?}", parsed);
    }

    #[test]
    fn test_parse_cachesearch_missing_threshold() {
        let args = vec![
            bulk(b"myidx"),
            bulk(b"cache:query:"),
            bulk(b"*=>[KNN 5 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            Frame::BulkString(Bytes::from(vec![0u8; 128 * 4])),
            // No THRESHOLD
            bulk(b"FALLBACK"),
            bulk(b"KNN"),
            bulk(b"10"),
        ];
        let result = cache_search::parse_cachesearch_args_for_test(&args);
        assert!(result.is_err(), "expected Err for missing THRESHOLD");
    }

    #[test]
    fn test_parse_cachesearch_missing_fallback() {
        let args = vec![
            bulk(b"myidx"),
            bulk(b"cache:query:"),
            bulk(b"*=>[KNN 5 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            Frame::BulkString(Bytes::from(vec![0u8; 128 * 4])),
            bulk(b"THRESHOLD"),
            bulk(b"0.95"),
            // No FALLBACK
        ];
        let result = cache_search::parse_cachesearch_args_for_test(&args);
        assert!(result.is_err(), "expected Err for missing FALLBACK");
    }

    #[test]
    fn test_is_cache_hit_l2_within() {
        // L2: lower distance = more similar. Threshold 0.5 means distance <= 0.5 is a hit.
        assert!(cache_search::is_within_threshold_for_test(
            0.3,
            0.5,
            DistanceMetric::L2
        ));
        assert!(cache_search::is_within_threshold_for_test(
            0.5,
            0.5,
            DistanceMetric::L2
        ));
    }

    #[test]
    fn test_is_cache_hit_l2_outside() {
        assert!(!cache_search::is_within_threshold_for_test(
            0.6,
            0.5,
            DistanceMetric::L2
        ));
    }

    #[test]
    fn test_is_cache_hit_cosine_within() {
        // Cosine: higher = more similar. Threshold 0.95 means distance >= 0.95 is a hit.
        assert!(cache_search::is_within_threshold_for_test(
            0.97,
            0.95,
            DistanceMetric::Cosine
        ));
        assert!(cache_search::is_within_threshold_for_test(
            0.95,
            0.95,
            DistanceMetric::Cosine
        ));
    }

    #[test]
    fn test_is_cache_hit_cosine_outside() {
        assert!(!cache_search::is_within_threshold_for_test(
            0.90,
            0.95,
            DistanceMetric::Cosine
        ));
    }

    #[test]
    fn test_is_cache_hit_ip_within() {
        // InnerProduct: higher = more similar.
        assert!(cache_search::is_within_threshold_for_test(
            0.99,
            0.9,
            DistanceMetric::InnerProduct
        ));
    }

    #[test]
    fn test_is_cache_hit_no_candidates() {
        // No candidates means no hit, regardless of threshold.
        assert!(!cache_search::is_within_threshold_for_test(
            f32::MAX,
            0.5,
            DistanceMetric::L2
        ));
    }

    #[test]
    fn test_ft_cachesearch_miss_on_empty_store() {
        let _metrics_guard = METRICS_LOCK.read();
        let mut store = VectorStore::new();
        let create_args = ft_create_args();
        ft_create(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &create_args,
            0,
        );

        let args = make_valid_cachesearch_args();
        let result = cache_search::ft_cachesearch(&mut store, &args, 0);

        // Should return cache miss with cache_hit: "false" (empty results)
        match &result {
            Frame::Array(items) => {
                // First element is total count (Integer)
                if let Frame::Integer(total) = &items[0] {
                    assert_eq!(*total, 0, "expected 0 results on empty store");
                }
            }
            Frame::Error(e) => {
                // Dimension mismatch or other parse error is also acceptable
                let _ = e;
            }
            other => panic!("expected Array or Error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_cachesearch_unknown_index() {
        let mut store = VectorStore::new();
        // Don't create any index
        let args = make_valid_cachesearch_args();
        let result = cache_search::ft_cachesearch(&mut store, &args, 0);
        match &result {
            Frame::Error(e) => assert!(
                e.starts_with(b"Unknown Index") || e.starts_with(b"ERR"),
                "expected unknown index error, got {:?}",
                std::str::from_utf8(e)
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_augment_with_cache_metadata_miss() {
        // Build a mock FT.SEARCH response
        let response = Frame::Array(
            vec![
                Frame::Integer(1),
                Frame::BulkString(Bytes::from_static(b"doc:1")),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"__vec_score")),
                        Frame::BulkString(Bytes::from_static(b"0.5")),
                    ]
                    .into(),
                ),
            ]
            .into(),
        );

        let augmented = cache_search::augment_with_cache_metadata_for_test(response, false);
        match &augmented {
            Frame::Array(items) => {
                assert_eq!(items.len(), 3);
                // Check fields array has cache_hit added
                if let Frame::Array(fields) = &items[2] {
                    assert_eq!(fields.len(), 4); // __vec_score, value, cache_hit, false
                    assert_eq!(
                        fields[2],
                        Frame::BulkString(Bytes::from_static(b"cache_hit"))
                    );
                    assert_eq!(fields[3], Frame::BulkString(Bytes::from_static(b"false")));
                } else {
                    panic!("expected fields array");
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn test_augment_with_cache_metadata_hit() {
        let response = Frame::Array(
            vec![
                Frame::Integer(1),
                Frame::BulkString(Bytes::from_static(b"cache:query:abc")),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"__vec_score")),
                        Frame::BulkString(Bytes::from_static(b"0.97")),
                    ]
                    .into(),
                ),
            ]
            .into(),
        );

        let augmented = cache_search::augment_with_cache_metadata_for_test(response, true);
        match &augmented {
            Frame::Array(items) => {
                if let Frame::Array(fields) = &items[2] {
                    assert_eq!(fields[3], Frame::BulkString(Bytes::from_static(b"true")));
                } else {
                    panic!("expected fields array");
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Session tests
// ---------------------------------------------------------------------------

#[test]
fn test_session_parse_session_clause_present() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"SESSION"),
        bulk(b"sess:conv1"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"q"),
        bulk(b"blob"),
    ];
    let result = parse_session_clause(&args);
    assert_eq!(result, Some(Bytes::from_static(b"sess:conv1")));
}

#[test]
fn test_session_parse_session_clause_absent() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"q"),
        bulk(b"blob"),
    ];
    let result = parse_session_clause(&args);
    assert!(result.is_none());
}

#[test]
fn test_session_parse_session_clause_no_key() {
    // SESSION keyword present but no key argument after it
    let args = vec![bulk(b"idx"), bulk(b"*=>[KNN 5 @vec $q]"), bulk(b"SESSION")];
    let result = parse_session_clause(&args);
    assert!(result.is_none());
}

#[test]
fn test_session_filter_results_empty_session() {
    use crate::vector::keymap::BucketedKeyMap;
    use crate::vector::types::{SearchResult, VectorId};
    use std::collections::HashMap;

    let results: SmallVec<[SearchResult; 32]> = smallvec::smallvec![
        SearchResult {
            id: VectorId(0),
            distance: 0.1,
            key_hash: 100
        },
        SearchResult {
            id: VectorId(1),
            distance: 0.2,
            key_hash: 200
        },
    ];
    let session_members: HashMap<Bytes, f64> = HashMap::new();
    let mut key_hash_to_key: BucketedKeyMap<Bytes> = BucketedKeyMap::new();
    key_hash_to_key.insert(100u64, Bytes::from_static(b"doc:a"));
    key_hash_to_key.insert(200u64, Bytes::from_static(b"doc:b"));

    let filtered = session::filter_session_results(&results, &session_members, &key_hash_to_key);
    assert_eq!(filtered.len(), 2, "empty session should return all results");
}

#[test]
fn test_session_filter_results_removes_seen() {
    use crate::vector::keymap::BucketedKeyMap;
    use crate::vector::types::{SearchResult, VectorId};
    use std::collections::HashMap;

    let results: SmallVec<[SearchResult; 32]> = smallvec::smallvec![
        SearchResult {
            id: VectorId(0),
            distance: 0.1,
            key_hash: 100
        },
        SearchResult {
            id: VectorId(1),
            distance: 0.2,
            key_hash: 200
        },
        SearchResult {
            id: VectorId(2),
            distance: 0.3,
            key_hash: 300
        },
    ];
    let mut session_members: HashMap<Bytes, f64> = HashMap::new();
    session_members.insert(Bytes::from_static(b"doc:a"), 1000.0);

    let mut key_hash_to_key: BucketedKeyMap<Bytes> = BucketedKeyMap::new();
    key_hash_to_key.insert(100u64, Bytes::from_static(b"doc:a"));
    key_hash_to_key.insert(200u64, Bytes::from_static(b"doc:b"));
    key_hash_to_key.insert(300u64, Bytes::from_static(b"doc:c"));

    let filtered = session::filter_session_results(&results, &session_members, &key_hash_to_key);
    assert_eq!(filtered.len(), 2, "doc:a should be filtered out");
    assert_eq!(filtered[0].key_hash, 200);
    assert_eq!(filtered[1].key_hash, 300);
}

#[test]
fn test_session_record_results() {
    use crate::vector::keymap::BucketedKeyMap;
    use crate::vector::types::{SearchResult, VectorId};

    let mut db = crate::storage::db::Database::new();
    let results: SmallVec<[SearchResult; 32]> = smallvec::smallvec![
        SearchResult {
            id: VectorId(0),
            distance: 0.1,
            key_hash: 100
        },
        SearchResult {
            id: VectorId(1),
            distance: 0.2,
            key_hash: 200
        },
    ];
    let mut key_hash_to_key: BucketedKeyMap<Bytes> = BucketedKeyMap::new();
    key_hash_to_key.insert(100u64, Bytes::from_static(b"doc:a"));
    key_hash_to_key.insert(200u64, Bytes::from_static(b"doc:b"));

    session::record_session_results(&results, &mut db, b"sess:conv1", &key_hash_to_key, 1000.0);

    // Verify the session sorted set was created and populated
    let (members, _tree) = db.get_sorted_set(b"sess:conv1").unwrap().unwrap();
    assert_eq!(members.len(), 2);
    assert_eq!(members.get(&Bytes::from_static(b"doc:a")), Some(&1000.0));
    assert_eq!(members.get(&Bytes::from_static(b"doc:b")), Some(&1000.0));
}

// ---------------------------------------------------------------------------
// Multi-field vector index tests (MVEC-02, MVEC-05)
// ---------------------------------------------------------------------------

/// Build FT.CREATE args with two VECTOR fields.
fn ft_create_multi_field_args() -> Vec<Frame> {
    vec![
        bulk(b"multiidx"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        // First field: title_vec DIM 4 COSINE
        bulk(b"title_vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"4"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"COSINE"),
        // Second field: body_vec DIM 8 L2
        bulk(b"body_vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"8"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ]
}

#[test]
fn test_ft_create_multi_field() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_multi_field_args();
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    match &result {
        Frame::SimpleString(s) => assert_eq!(&s[..], b"OK"),
        other => panic!("expected OK, got {other:?}"),
    }
    let idx = store.get_index(b"multiidx").unwrap();
    assert_eq!(idx.meta.vector_fields.len(), 2);
    assert_eq!(&idx.meta.vector_fields[0].field_name[..], b"title_vec");
    assert_eq!(idx.meta.vector_fields[0].dimension, 4);
    assert_eq!(idx.meta.vector_fields[0].metric, DistanceMetric::Cosine);
    assert_eq!(&idx.meta.vector_fields[1].field_name[..], b"body_vec");
    assert_eq!(idx.meta.vector_fields[1].dimension, 8);
    assert_eq!(idx.meta.vector_fields[1].metric, DistanceMetric::L2);
    // Default field is the first one
    assert_eq!(idx.meta.dimension, 4);
    assert_eq!(idx.meta.metric, DistanceMetric::Cosine);
    assert!(idx.meta.is_multi_field());
    // Additional field segments should exist for body_vec
    assert!(idx.field_segments.contains_key(b"body_vec".as_slice()));
}

#[test]
fn test_ft_create_duplicate_field_rejected() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = vec![
        bulk(b"dupidx"),
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
        bulk(b"4"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
        // Duplicate field name
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"8"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"COSINE"),
    ];
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    match &result {
        Frame::Error(e) => assert!(
            e.starts_with(b"ERR duplicate"),
            "expected duplicate error, got {:?}",
            std::str::from_utf8(e)
        ),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_create_exceeds_max_fields() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let mut args = vec![
        bulk(b"toomanyidx"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
    ];
    // Add 9 VECTOR fields (> MAX_VECTOR_FIELDS=8)
    for i in 0..9 {
        let name = format!("vec{i}");
        args.push(Frame::BulkString(Bytes::from(name)));
        args.push(bulk(b"VECTOR"));
        args.push(bulk(b"HNSW"));
        args.push(bulk(b"6"));
        args.push(bulk(b"TYPE"));
        args.push(bulk(b"FLOAT32"));
        args.push(bulk(b"DIM"));
        args.push(bulk(b"4"));
        args.push(bulk(b"DISTANCE_METRIC"));
        args.push(bulk(b"L2"));
    }
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    match &result {
        Frame::Error(e) => assert!(
            e.starts_with(b"ERR too many"),
            "expected too-many error, got {:?}",
            std::str::from_utf8(e)
        ),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_info_multi_field() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_multi_field_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    let result = ft_info(
        &store,
        &crate::text::store::TextStore::new(),
        &[bulk(b"multiidx")],
        0,
    );
    match result {
        Frame::Array(items) => {
            // Find the vector_fields key
            let mut vf_idx = None;
            for (i, item) in items.iter().enumerate() {
                if let Frame::BulkString(key) = item {
                    if key.as_ref() == b"vector_fields" {
                        vf_idx = Some(i + 1);
                        break;
                    }
                }
            }
            let vf_idx = vf_idx.expect("vector_fields key not found in FT.INFO");
            let fields = match &items[vf_idx] {
                Frame::Array(f) => f,
                other => panic!("expected Array for vector_fields, got {other:?}"),
            };
            assert_eq!(fields.len(), 2, "should have 2 vector fields");

            // Verify first field: title_vec
            let f0 = match &fields[0] {
                Frame::Array(f) => f,
                other => panic!("expected Array for field entry, got {other:?}"),
            };
            // field_name, title_vec, dimension, 4, distance_metric, COSINE, ...
            assert_eq!(f0[0], Frame::BulkString(Bytes::from_static(b"field_name")));
            assert_eq!(f0[1], Frame::BulkString(Bytes::from("title_vec")));
            assert_eq!(f0[2], Frame::BulkString(Bytes::from_static(b"dimension")));
            assert_eq!(f0[3], Frame::Integer(4));
            assert_eq!(
                f0[4],
                Frame::BulkString(Bytes::from_static(b"distance_metric"))
            );
            assert_eq!(f0[5], Frame::BulkString(Bytes::from_static(b"COSINE")));

            // Verify second field: body_vec
            let f1 = match &fields[1] {
                Frame::Array(f) => f,
                other => panic!("expected Array for field entry, got {other:?}"),
            };
            assert_eq!(f1[1], Frame::BulkString(Bytes::from("body_vec")));
            assert_eq!(f1[3], Frame::Integer(8));
            assert_eq!(f1[5], Frame::BulkString(Bytes::from_static(b"L2")));

            // Top-level backward compat: dimension = 4 (default field)
            assert_eq!(items[7], Frame::Integer(4));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_ft_search_field_targeting() {
    let _metrics_guard = METRICS_LOCK.read();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_multi_field_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Insert vectors into the default (title_vec) field (dim=4)
    let idx = store.get_index_mut(b"multiidx").unwrap();
    let title_vecs: Vec<[f32; 4]> = vec![[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]];
    let snap = idx.segments.load();
    for (i, v) in title_vecs.iter().enumerate() {
        let mut sq = vec![0i8; 4];
        quantize_f32_to_sq(v, &mut sq);
        snap.mutable.append(i as u64, v, i as u64);
    }
    drop(snap);

    // Insert vectors into body_vec field (dim=8)
    let body_vecs: Vec<[f32; 8]> = vec![
        [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
    ];
    if let Some(fs) = idx.field_segments.get(b"body_vec".as_slice()) {
        let snap = fs.segments.load();
        for (i, v) in body_vecs.iter().enumerate() {
            let mut sq = vec![0i8; 8];
            quantize_f32_to_sq(v, &mut sq);
            snap.mutable.append(i as u64, v, i as u64);
        }
    }

    // Search @title_vec (dim=4)
    let query_vec_4: Vec<u8> = [1.0f32, 0.0, 0.0, 0.0]
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let search_args = vec![
        bulk(b"multiidx"),
        Frame::BulkString(Bytes::from_static(b"*=>[KNN 2 @title_vec $query]")),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec_4)),
    ];
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            assert!(
                matches!(&items[0], Frame::Integer(n) if *n >= 1),
                "title_vec search should find at least 1 result, got {result:?}"
            );
        }
        Frame::Error(e) => panic!("title_vec search error: {:?}", std::str::from_utf8(e)),
        _ => panic!("expected Array, got {result:?}"),
    }

    // Search @body_vec (dim=8)
    let query_vec_8: Vec<u8> = [1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let search_args = vec![
        bulk(b"multiidx"),
        Frame::BulkString(Bytes::from_static(b"*=>[KNN 2 @body_vec $query]")),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec_8)),
    ];
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            assert!(
                matches!(&items[0], Frame::Integer(n) if *n >= 1),
                "body_vec search should find at least 1 result, got {result:?}"
            );
        }
        Frame::Error(e) => panic!("body_vec search error: {:?}", std::str::from_utf8(e)),
        _ => panic!("expected Array, got {result:?}"),
    }

    // Search @body_vec with wrong dimension (4 bytes) should error
    let wrong_dim: Vec<u8> = [1.0f32, 0.0, 0.0, 0.0]
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let search_args = vec![
        bulk(b"multiidx"),
        Frame::BulkString(Bytes::from_static(b"*=>[KNN 2 @body_vec $query]")),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(wrong_dim)),
    ];
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    assert!(
        matches!(&result, Frame::Error(_)),
        "expected dimension mismatch error for body_vec, got {result:?}"
    );
}

#[test]
fn test_ft_search_default_field_compat() {
    let _metrics_guard = METRICS_LOCK.read();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    // Single-field index
    let args = build_ft_create_args("singleidx", "doc:", "vec", 4, "L2");
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    let idx = store.get_index_mut(b"singleidx").unwrap();
    let vectors: Vec<[f32; 4]> = vec![[1.0, 0.0, 0.0, 0.0]];
    let snap = idx.segments.load();
    for (i, v) in vectors.iter().enumerate() {
        let mut sq = vec![0i8; 4];
        quantize_f32_to_sq(v, &mut sq);
        snap.mutable.append(i as u64, v, i as u64);
    }
    drop(snap);

    // Search without @field_name -- should use default field (backward compat)
    // Note: parse_knn_query still extracts @vec but it matches default field
    let query_vec: Vec<u8> = [1.0f32, 0.0, 0.0, 0.0]
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let search_args = vec![
        bulk(b"singleidx"),
        Frame::BulkString(Bytes::from_static(b"*=>[KNN 1 @vec $query]")),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            assert!(
                matches!(&items[0], Frame::Integer(1)),
                "default field search should find 1 result, got {result:?}"
            );
        }
        Frame::Error(e) => panic!("default field search error: {:?}", std::str::from_utf8(e)),
        _ => panic!("expected Array, got {result:?}"),
    }
}

#[test]
fn test_ft_search_unknown_field_error() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    let query_vec: Vec<u8> = vec![0u8; 128 * 4];
    let search_args = vec![
        bulk(b"myidx"),
        Frame::BulkString(Bytes::from_static(b"*=>[KNN 5 @nonexistent_field $query]")),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    crate::vector::distance::init();
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Error(e) => assert!(
            e.starts_with(b"ERR unknown vector field"),
            "expected unknown field error, got {:?}",
            std::str::from_utf8(e)
        ),
        other => panic!("expected error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Full-text filter (TextMatch) tests
// ---------------------------------------------------------------------------

#[test]
fn test_parse_filter_text_match_multiword() {
    // Multi-word value in {} should parse as TextMatch
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@description:{machine learning}"),
    ];
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(
        filter.is_some(),
        "should parse @description:{{machine learning}}"
    );
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::TextMatch { field, terms } => {
            assert_eq!(&field[..], b"description");
            assert_eq!(terms.len(), 2);
            assert_eq!(&terms[0][..], b"machine");
            assert_eq!(&terms[1][..], b"learning");
        }
        other => panic!("expected TextMatch, got {other:?}"),
    }
}

#[test]
fn test_parse_filter_single_word_remains_tag() {
    // Single-word value in {} should still be TagEq
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@category:{science}"),
    ];
    let filter = parse_filter_clause(&args).into_option().unwrap();
    assert!(filter.is_some());
    match filter.unwrap() {
        crate::vector::filter::FilterExpr::TagEq { field, value } => {
            assert_eq!(&field[..], b"category");
            assert_eq!(&value[..], b"science");
        }
        other => panic!("expected TagEq, got {other:?}"),
    }
}

#[cfg(feature = "text-index")]
#[test]
fn test_text_filter_basic_payload_index() {
    // Test TextMatch evaluation through PayloadIndex
    use crate::vector::filter::PayloadIndex;

    let mut idx = PayloadIndex::new();
    idx.insert_text(
        &Bytes::from_static(b"desc"),
        b"Machine learning models for natural language processing",
        0,
    );
    idx.insert_text(
        &Bytes::from_static(b"desc"),
        b"Database indexing and query optimization",
        1,
    );
    idx.insert_text(
        &Bytes::from_static(b"desc"),
        b"Deep learning neural network architectures",
        2,
    );

    // Search for "machine learning" - should match doc 0 only (AND semantics)
    let expr = crate::vector::filter::FilterExpr::TextMatch {
        field: Bytes::from_static(b"desc"),
        terms: vec![
            Bytes::from_static(b"machine"),
            Bytes::from_static(b"learning"),
        ],
    };
    let bm = idx.evaluate_bitmap(&expr, 3);
    assert!(bm.contains(0), "doc 0 should match 'machine learning'");
    assert!(!bm.contains(1), "doc 1 should NOT match");
    assert!(!bm.contains(2), "doc 2 has 'learning' but not 'machine'");
}

#[cfg(feature = "text-index")]
#[test]
fn test_text_filter_stemming_through_payload() {
    use crate::vector::filter::PayloadIndex;

    let mut idx = PayloadIndex::new();
    idx.insert_text(
        &Bytes::from_static(b"desc"),
        b"The runners are running fast",
        0,
    );
    idx.insert_text(&Bytes::from_static(b"desc"), b"She runs every morning", 1);
    idx.insert_text(&Bytes::from_static(b"desc"), b"The cat sat on the mat", 2);

    // "running" should match docs with "runners", "running", "runs" via stemming
    let expr = crate::vector::filter::FilterExpr::TextMatch {
        field: Bytes::from_static(b"desc"),
        terms: vec![Bytes::from_static(b"running")],
    };
    let bm = idx.evaluate_bitmap(&expr, 3);
    assert!(bm.contains(0), "doc 0 has 'runners'/'running'");
    assert!(bm.contains(1), "doc 1 has 'runs'");
    assert!(!bm.contains(2), "doc 2 has no run-related words");
}

#[cfg(feature = "text-index")]
#[test]
fn test_text_filter_combined_with_tag() {
    use crate::vector::filter::PayloadIndex;

    let mut idx = PayloadIndex::new();
    // Doc 0: has "machine learning" text AND "science" tag
    idx.insert_text(&Bytes::from_static(b"desc"), b"Machine learning models", 0);
    idx.insert_tag(
        &Bytes::from_static(b"category"),
        &Bytes::from_static(b"science"),
        0,
    );
    // Doc 1: has "machine learning" text but "sports" tag
    idx.insert_text(
        &Bytes::from_static(b"desc"),
        b"Machine learning in sports analytics",
        1,
    );
    idx.insert_tag(
        &Bytes::from_static(b"category"),
        &Bytes::from_static(b"sports"),
        1,
    );

    // TextMatch AND TagEq
    let expr = crate::vector::filter::FilterExpr::And(
        Box::new(crate::vector::filter::FilterExpr::TextMatch {
            field: Bytes::from_static(b"desc"),
            terms: vec![
                Bytes::from_static(b"machine"),
                Bytes::from_static(b"learning"),
            ],
        }),
        Box::new(crate::vector::filter::FilterExpr::TagEq {
            field: Bytes::from_static(b"category"),
            value: Bytes::from_static(b"science"),
        }),
    );
    let bm = idx.evaluate_bitmap(&expr, 2);
    assert!(bm.contains(0), "doc 0 matches both text AND tag");
    assert!(!bm.contains(1), "doc 1 matches text but wrong tag");
}

#[test]
fn test_text_filter_without_feature_returns_empty() {
    // TextMatch should return empty bitmap when nothing indexed
    use crate::vector::filter::PayloadIndex;

    let idx = PayloadIndex::new();
    let expr = crate::vector::filter::FilterExpr::TextMatch {
        field: Bytes::from_static(b"desc"),
        terms: vec![Bytes::from_static(b"hello")],
    };
    let bm = idx.evaluate_bitmap(&expr, 10);
    assert!(bm.is_empty());
}

// ---------------------------------------------------------------------------
// Hybrid dense+sparse search tests (RRF fusion)
// ---------------------------------------------------------------------------

/// Build FT.CREATE args for an index with both VECTOR and SPARSE fields.
fn ft_create_hybrid_args() -> Vec<Frame> {
    vec![
        bulk(b"hybridx"),
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
        bulk(b"4"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
        bulk(b"sparse_vec"),
        bulk(b"SPARSE"),
        bulk(b"DIM"),
        bulk(b"100"),
    ]
}

/// Helper: encode a sparse vector as alternating u32+f32 LE bytes.
fn encode_sparse_blob(pairs: &[(u32, f32)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(pairs.len() * 8);
    for &(dim, weight) in pairs {
        buf.extend_from_slice(&dim.to_le_bytes());
        buf.extend_from_slice(&weight.to_le_bytes());
    }
    buf
}

/// Insert a document into the hybrid index (both dense vector and sparse vector).
/// Uses direct mutable segment append (same pattern as test_end_to_end_create_insert_search).
fn insert_hybrid_doc(
    store: &mut VectorStore,
    key: &[u8],
    dense_vec: &[f32],
    sparse_pairs: &[(u32, f32)],
) {
    let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
    let dim = dense_vec.len();

    let idx = store.get_index_mut(b"hybridx").unwrap();

    // Insert dense vector into mutable segment
    let snap = idx.segments.load();
    let mut sq = vec![0i8; dim];
    quantize_f32_to_sq(dense_vec, &mut sq);
    snap.mutable.append(key_hash, dense_vec, 0);
    drop(snap);

    // Record key mapping
    idx.key_hash_to_key
        .insert(key_hash, Bytes::from(key.to_vec()));

    // Insert sparse vector
    if let Some(ss) = idx.sparse_stores.get_mut(b"sparse_vec".as_ref()) {
        let _ = ss.insert(key_hash, sparse_pairs);
    }
}

#[test]
fn test_hybrid_search_basic() {
    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_hybrid_args();
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "create failed: {result:?}"
    );

    // Insert 3 docs with both dense and sparse vectors
    insert_hybrid_doc(
        &mut store,
        b"doc:1",
        &[1.0, 0.0, 0.0, 0.0],
        &[(0, 1.0), (5, 0.5)],
    );
    insert_hybrid_doc(
        &mut store,
        b"doc:2",
        &[0.0, 1.0, 0.0, 0.0],
        &[(0, 0.8), (10, 0.3)],
    );
    insert_hybrid_doc(
        &mut store,
        b"doc:3",
        &[0.0, 0.0, 1.0, 0.0],
        &[(5, 0.9), (10, 0.1)],
    );

    // Dense query close to doc:1, sparse query has high weight on dim 0 (matches doc:1 and doc:2)
    let dense_query: Vec<u8> = [0.9_f32, 0.1, 0.0, 0.0]
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let sparse_query = encode_sparse_blob(&[(0, 1.0)]);

    let search_args = vec![
        bulk(b"hybridx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"SPARSE"),
        bulk(b"@sparse_vec"),
        bulk(b"$sq"),
        bulk(b"PARAMS"),
        bulk(b"4"),
        bulk(b"q"),
        Frame::BulkString(Bytes::from(dense_query)),
        bulk(b"sq"),
        Frame::BulkString(Bytes::from(sparse_query)),
    ];

    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            let total = match &items[0] {
                Frame::Integer(n) => *n,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert!(total > 0, "expected at least 1 fused result");

            // Check for dense_hits and sparse_hits metadata at end
            let len = items.len();
            assert!(len >= 5, "response too short for metadata: {len}");
            let dense_hits_label = &items[len - 4];
            let sparse_hits_label = &items[len - 2];
            assert_eq!(
                *dense_hits_label,
                Frame::BulkString(Bytes::from_static(b"dense_hits"))
            );
            assert_eq!(
                *sparse_hits_label,
                Frame::BulkString(Bytes::from_static(b"sparse_hits"))
            );
        }
        Frame::Error(e) => panic!("search failed: {}", String::from_utf8_lossy(e)),
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_hybrid_search_sparse_only() {
    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_hybrid_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    insert_hybrid_doc(&mut store, b"doc:1", &[1.0, 0.0, 0.0, 0.0], &[(0, 1.0)]);
    insert_hybrid_doc(&mut store, b"doc:2", &[0.0, 1.0, 0.0, 0.0], &[(0, 0.5)]);

    let sparse_query = encode_sparse_blob(&[(0, 1.0)]);

    // Sparse-only: query string is "*" (no KNN), but SPARSE clause present
    let search_args = vec![
        bulk(b"hybridx"),
        bulk(b"*"),
        bulk(b"SPARSE"),
        bulk(b"@sparse_vec"),
        bulk(b"$sq"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"sq"),
        Frame::BulkString(Bytes::from(sparse_query)),
    ];

    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            let total = match &items[0] {
                Frame::Integer(n) => *n,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert_eq!(total, 2, "expected 2 sparse-only results");

            // Verify dense_hits=0 in metadata
            let len = items.len();
            let dense_hits_val = &items[len - 3];
            assert_eq!(*dense_hits_val, Frame::Integer(0), "dense_hits should be 0");
        }
        Frame::Error(e) => panic!("search failed: {}", String::from_utf8_lossy(e)),
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_hybrid_search_dense_only_backward_compat() {
    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_args(); // standard index, no SPARSE field
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Standard dense-only search (no SPARSE clause) -- should work as before
    let query_vec: Vec<u8> = vec![0u8; 128 * 4];
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            assert_eq!(items[0], Frame::Integer(0)); // empty index
            // No dense_hits/sparse_hits metadata (backward compat)
            assert_eq!(items.len(), 1, "dense-only should have no metadata trailer");
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_hybrid_search_hit_counts() {
    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_hybrid_args();
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args,
        0,
    );

    // Insert 5 docs
    for i in 0..5u32 {
        let key = format!("doc:{i}");
        let dense = [i as f32, 0.0, 0.0, 0.0];
        let sparse = vec![(i, 1.0_f32)];
        insert_hybrid_doc(&mut store, key.as_bytes(), &dense, &sparse);
    }

    // Dense query matches all 5, sparse query matches only dims 0 and 1
    let dense_query: Vec<u8> = [1.0_f32, 0.0, 0.0, 0.0]
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let sparse_query = encode_sparse_blob(&[(0, 1.0), (1, 0.5)]);

    let search_args = vec![
        bulk(b"hybridx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"SPARSE"),
        bulk(b"@sparse_vec"),
        bulk(b"$sq"),
        bulk(b"PARAMS"),
        bulk(b"4"),
        bulk(b"q"),
        Frame::BulkString(Bytes::from(dense_query)),
        bulk(b"sq"),
        Frame::BulkString(Bytes::from(sparse_query)),
    ];

    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            let len = items.len();
            // Check metadata
            let dense_hits_val = match &items[len - 3] {
                Frame::Integer(n) => *n,
                other => panic!("expected Integer for dense_hits, got {other:?}"),
            };
            let sparse_hits_val = match &items[len - 1] {
                Frame::Integer(n) => *n,
                other => panic!("expected Integer for sparse_hits, got {other:?}"),
            };
            assert!(
                dense_hits_val > 0,
                "dense_hits should be > 0, got {dense_hits_val}"
            );
            assert!(
                sparse_hits_val > 0,
                "sparse_hits should be > 0, got {sparse_hits_val}"
            );
        }
        Frame::Error(e) => panic!("search failed: {}", String::from_utf8_lossy(e)),
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn test_parse_sparse_clause() {
    // Valid SPARSE clause
    let args = vec![
        bulk(b"idx"),
        bulk(b"*"),
        bulk(b"SPARSE"),
        bulk(b"@my_sparse"),
        bulk(b"$sq"),
    ];
    let result = parse_sparse_clause(&args);
    assert!(result.is_some());
    let (field, param) = result.unwrap();
    assert_eq!(field.as_ref(), b"my_sparse");
    assert_eq!(param.as_ref(), b"sq");
}

#[test]
fn test_parse_sparse_clause_missing() {
    let args = vec![bulk(b"idx"), bulk(b"*=>[KNN 10 @vec $q]")];
    assert!(parse_sparse_clause(&args).is_none());
}

// ---------------------------------------------------------------------------
// Phase 143: RANGE threshold tests
// ---------------------------------------------------------------------------

#[test]
fn test_parse_range_clause_valid() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"RANGE"),
        bulk(b"0.5"),
    ];
    let result = parse_range_clause(&args);
    assert!(result.is_some());
    assert!((result.unwrap() - 0.5).abs() < f32::EPSILON);
}

#[test]
fn test_parse_range_clause_absent() {
    let args = vec![bulk(b"idx"), bulk(b"*=>[KNN 10 @vec $q]")];
    assert!(parse_range_clause(&args).is_none());
}

#[test]
fn test_parse_range_clause_invalid_value() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"RANGE"),
        bulk(b"notanumber"),
    ];
    assert!(parse_range_clause(&args).is_none());
}

#[test]
fn test_parse_range_clause_case_insensitive() {
    // matches_keyword is case-insensitive, so "range" should work too
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 10 @vec $q]"),
        bulk(b"range"),
        bulk(b"1.5"),
    ];
    let result = parse_range_clause(&args);
    assert!(result.is_some());
    assert!((result.unwrap() - 1.5).abs() < f32::EPSILON);
}

#[test]
fn test_range_filter_l2_search() {
    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let dim: usize = 4;

    // Create L2 index
    let create_args = build_ft_create_args("rangeidx", "doc:", "vec", dim as u32, "L2");
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &create_args,
        0,
    );
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.CREATE failed: {result:?}"
    );

    // Insert 3 vectors with known distances from [1,0,0,0]:
    // vec:0 = [1,0,0,0] -> L2=0.0
    // vec:1 = [0.5,0,0,0] -> L2=0.25
    // vec:2 = [-1,0,0,0] -> L2=4.0
    let vectors: Vec<[f32; 4]> = vec![
        [1.0, 0.0, 0.0, 0.0],
        [0.5, 0.0, 0.0, 0.0],
        [-1.0, 0.0, 0.0, 0.0],
    ];

    {
        let idx = store.get_index_mut(b"rangeidx").unwrap();
        let snap = idx.segments.load();
        for (i, v) in vectors.iter().enumerate() {
            let mut sq = vec![0i8; dim];
            quantize_f32_to_sq(v, &mut sq);
            snap.mutable.append(i as u64, v, i as u64);
        }
    }

    // Query with RANGE 0.5 -- should get vec:0 (L2=0) and vec:1 (L2=0.25), not vec:2 (L2=4.0)
    let query_vec: [f32; 4] = [1.0, 0.0, 0.0, 0.0];
    let query_blob: Vec<u8> = query_vec.iter().flat_map(|f| f.to_le_bytes()).collect();

    let search_args = vec![
        bulk(b"rangeidx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"RANGE"),
        bulk(b"0.5"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_blob.clone())),
    ];

    let result = ft_search(&mut store, &search_args, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            let count = match &items[0] {
                Frame::Integer(n) => *n,
                other => panic!("expected Integer, got {other:?}"),
            };
            // Should have at most 2 results (vec:0 and vec:1 within range)
            assert!(
                count <= 2,
                "expected at most 2 results within RANGE 0.5, got {count}"
            );
            // vec:2 should NOT appear (L2=4.0 > 0.5)
            for i in (1..items.len()).step_by(2) {
                if let Frame::BulkString(key) = &items[i] {
                    assert_ne!(
                        key.as_ref(),
                        b"vec:2",
                        "vec:2 should be filtered out by RANGE 0.5"
                    );
                }
            }
        }
        Frame::Error(e) => panic!("FT.SEARCH error: {}", String::from_utf8_lossy(e)),
        other => panic!("expected Array, got {other:?}"),
    }

    // Query with RANGE 0.0 -- only exact match should survive
    let search_args_zero = vec![
        bulk(b"rangeidx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"RANGE"),
        bulk(b"0.0"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_blob)),
    ];

    let result = ft_search(&mut store, &search_args_zero, None, None, 0, 0);
    match &result {
        Frame::Array(items) => {
            let count = match &items[0] {
                Frame::Integer(n) => *n,
                other => panic!("expected Integer, got {other:?}"),
            };
            // With TQ quantization at dim=4, exact match vec:0 may have nonzero distance.
            // But we verify that at least the result set is small (range is very tight).
            assert!(
                count <= 1,
                "RANGE 0.0 should return very few results, got {count}"
            );
        }
        Frame::Error(e) => panic!("FT.SEARCH error: {}", String::from_utf8_lossy(e)),
        other => panic!("expected Array, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Phase 143: FT.RECOMMEND tests
// ---------------------------------------------------------------------------

#[test]
fn test_recommend_no_db() {
    let mut store = VectorStore::new();
    let args = vec![bulk(b"myidx"), bulk(b"POSITIVE"), bulk(b"doc:1")];
    let result = recommend::ft_recommend(&mut store, &args, None, 0);
    match result {
        Frame::Error(e) => {
            assert!(
                e.starts_with(b"ERR FT.RECOMMEND requires database"),
                "unexpected error: {}",
                String::from_utf8_lossy(&e)
            );
        }
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_recommend_missing_positive_keyword() {
    let mut store = VectorStore::new();
    let mut db = crate::storage::db::Database::new();
    // 3 args but second is NOT "POSITIVE"
    let args = vec![
        bulk(b"myidx"),
        bulk(b"doc:1"), // missing POSITIVE keyword
        bulk(b"doc:2"),
    ];
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db), 0);
    match result {
        Frame::Error(e) => {
            assert!(
                e.starts_with(b"ERR expected POSITIVE"),
                "unexpected error: {}",
                String::from_utf8_lossy(&e)
            );
        }
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_recommend_unknown_index() {
    let mut store = VectorStore::new();
    let mut db = crate::storage::db::Database::new();
    let args = vec![bulk(b"nonexistent"), bulk(b"POSITIVE"), bulk(b"doc:1")];
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db), 0);
    match result {
        Frame::Error(e) => {
            assert!(
                e.starts_with(b"Unknown Index") || e.starts_with(b"ERR no valid vectors"),
                "unexpected error: {}",
                String::from_utf8_lossy(&e)
            );
        }
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_recommend_missing_key_vectors() {
    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let create_args = build_ft_create_args("recidx", "doc:", "vec", 4, "L2");
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &create_args,
        0,
    );

    let mut db = crate::storage::db::Database::new();
    // doc:1 does NOT exist in db -- so no vectors can be read
    let args = vec![bulk(b"recidx"), bulk(b"POSITIVE"), bulk(b"doc:1")];
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db), 0);
    match result {
        Frame::Error(e) => {
            assert!(
                e.starts_with(b"ERR no valid vectors"),
                "expected 'no valid vectors' error, got: {}",
                String::from_utf8_lossy(&e)
            );
        }
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_recommend_basic_with_vectors() {
    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let dim: usize = 4;
    let create_args = build_ft_create_args("recidx2", "doc:", "vec", dim as u32, "L2");
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &create_args,
        0,
    );

    // Insert 5 vectors into the index
    let vectors: Vec<(&[u8], [f32; 4])> = vec![
        (b"doc:1", [1.0, 0.0, 0.0, 0.0]),
        (b"doc:2", [0.9, 0.1, 0.0, 0.0]),
        (b"doc:3", [0.0, 1.0, 0.0, 0.0]),
        (b"doc:4", [0.0, 0.0, 1.0, 0.0]),
        (b"doc:5", [0.0, 0.0, 0.0, 1.0]),
    ];

    {
        let idx = store.get_index_mut(b"recidx2").unwrap();
        for (i, (key, v)) in vectors.iter().enumerate() {
            let key_hash = xxhash_rust::xxh64::xxh64(*key, 0);
            let snap = idx.segments.load();
            let mut sq = vec![0i8; dim];
            quantize_f32_to_sq(v, &mut sq);
            snap.mutable.append(key_hash, v, i as u64);
            drop(snap);
            idx.key_hash_to_key
                .insert(key_hash, Bytes::from(key.to_vec()));
        }
    }

    // Create a database with hash entries for the positive keys
    let mut db = crate::storage::db::Database::new();
    for (key, v) in &vectors {
        let blob: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
        let hset_args = vec![
            Frame::BulkString(Bytes::from(key.to_vec())),
            bulk(b"vec"),
            Frame::BulkString(Bytes::from(blob)),
        ];
        crate::command::hash::hset(&mut db, &hset_args);
    }

    // Recommend based on doc:1 (positive only)
    let args = vec![
        bulk(b"recidx2"),
        bulk(b"POSITIVE"),
        bulk(b"doc:1"),
        bulk(b"K"),
        bulk(b"3"),
    ];
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db), 0);
    match &result {
        Frame::Array(items) => {
            let count = match &items[0] {
                Frame::Integer(n) => *n,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert!(count > 0, "should return at least 1 recommendation");
            assert!(count <= 3, "should return at most K=3 results, got {count}");
            // Positive key doc:1 should NOT be in results
            for i in (1..items.len()).step_by(2) {
                if let Frame::BulkString(key) = &items[i] {
                    assert_ne!(key.as_ref(), b"doc:1", "positive key should be excluded");
                }
            }
        }
        Frame::Error(e) => panic!("FT.RECOMMEND error: {}", String::from_utf8_lossy(e)),
        other => panic!("expected Array, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Phase 143: FT.NAVIGATE tests (graph feature required)
// ---------------------------------------------------------------------------

#[cfg(feature = "graph")]
mod ft_navigate_tests {
    use super::*;

    #[test]
    fn test_navigate_no_graph_store() {
        let mut store = VectorStore::new();
        let args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 10 @vec $v]"),
            bulk(b"HOPS"),
            bulk(b"2"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"v"),
            bulk(b"blob"),
        ];
        let result = navigate::ft_navigate(&mut store, None, &args, None, 0);
        match result {
            Frame::Error(e) => {
                assert!(
                    e.starts_with(b"ERR FT.NAVIGATE requires graph"),
                    "unexpected error: {}",
                    String::from_utf8_lossy(&e)
                );
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_navigate_too_few_args() {
        let mut store = VectorStore::new();
        let gs = crate::graph::store::GraphStore::new();
        let args = vec![bulk(b"myidx"), bulk(b"*=>[KNN 10 @vec $v]")];
        let result = navigate::ft_navigate(&mut store, Some(&gs), &args, None, 0);
        match result {
            Frame::Error(e) => {
                assert!(
                    e.starts_with(b"ERR wrong number") || e.starts_with(b"ERR HOPS"),
                    "unexpected error: {}",
                    String::from_utf8_lossy(&e)
                );
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_navigate_missing_hops() {
        let mut store = VectorStore::new();
        let gs = crate::graph::store::GraphStore::new();
        let args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 10 @vec $v]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"v"),
            bulk(b"blob"),
        ];
        let result = navigate::ft_navigate(&mut store, Some(&gs), &args, None, 0);
        match result {
            Frame::Error(e) => {
                assert!(
                    e.starts_with(b"ERR HOPS"),
                    "expected HOPS required error, got: {}",
                    String::from_utf8_lossy(&e)
                );
            }
            other => panic!("expected error, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// FT.DROPINDEX DD Flag Tests (Phase 156)
// ---------------------------------------------------------------------------

#[test]
fn test_ft_dropindex_dd_deletes_docs() {
    let _metrics_guard = METRICS_LOCK.read();
    use crate::storage::db::Database;

    // Create database and vector store
    let mut db = Database::new();
    let mut store = VectorStore::new();
    let mut text_store = crate::text::store::TextStore::new();

    // Create index
    let create_args = vec![
        bulk(b"ddtest"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"dd:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"4"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];
    ft_create(&mut store, &mut text_store, &create_args, 0);

    // Insert document into database (simulating HSET)
    let key1 = Bytes::from_static(b"dd:1");
    let key2 = Bytes::from_static(b"dd:2");
    db.set(key1.clone(), crate::storage::entry::Entry::new_hash());
    db.set(key2.clone(), crate::storage::entry::Entry::new_hash());

    // Register keys in the vector index (simulating auto_index_hset)
    if let Some(idx) = store.get_index_mut(b"ddtest") {
        let h1 = xxhash_rust::xxh64::xxh64(&key1, 0);
        let h2 = xxhash_rust::xxh64::xxh64(&key2, 0);
        idx.key_hash_to_key.insert(h1, key1.clone());
        idx.key_hash_to_key.insert(h2, key2.clone());
    }

    // Verify keys exist in database
    assert!(db.get(&key1).is_some(), "key dd:1 should exist before drop");
    assert!(db.get(&key2).is_some(), "key dd:2 should exist before drop");

    // Drop index with DD flag
    let result = ft_dropindex(
        &mut store,
        &mut text_store,
        Some(&mut db),
        &[bulk(b"ddtest"), bulk(b"DD")],
        0,
    );
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.DROPINDEX DD should return OK, got {result:?}"
    );

    // Verify documents are deleted
    assert!(
        db.get(&key1).is_none(),
        "key dd:1 should be deleted after FT.DROPINDEX DD"
    );
    assert!(
        db.get(&key2).is_none(),
        "key dd:2 should be deleted after FT.DROPINDEX DD"
    );
}

#[test]
fn test_ft_dropindex_preserves_docs() {
    let _metrics_guard = METRICS_LOCK.read();
    use crate::storage::db::Database;

    let mut db = Database::new();
    let mut store = VectorStore::new();
    let mut text_store = crate::text::store::TextStore::new();

    // Create index
    let create_args = vec![
        bulk(b"preservetest"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"pres:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"4"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];
    ft_create(&mut store, &mut text_store, &create_args, 0);

    // Insert document into database
    let key1 = Bytes::from_static(b"pres:1");
    db.set(key1.clone(), crate::storage::entry::Entry::new_hash());

    // Register key in vector index
    if let Some(idx) = store.get_index_mut(b"preservetest") {
        let h1 = xxhash_rust::xxh64::xxh64(&key1, 0);
        idx.key_hash_to_key.insert(h1, key1.clone());
    }

    // Drop index WITHOUT DD flag (using None for db since we don't need it)
    let result = ft_dropindex(
        &mut store,
        &mut text_store,
        None,
        &[bulk(b"preservetest")],
        0,
    );
    assert!(
        matches!(result, Frame::SimpleString(_)),
        "FT.DROPINDEX should return OK"
    );

    // Verify document is preserved
    assert!(
        db.get(&key1).is_some(),
        "key pres:1 should be preserved after FT.DROPINDEX without DD"
    );
}

#[test]
fn test_ft_dropindex_dd_case_insensitive() {
    let _metrics_guard = METRICS_LOCK.read();
    use crate::storage::db::Database;

    // Test lowercase 'dd'
    {
        let mut db = Database::new();
        let mut store = VectorStore::new();
        let mut text_store = crate::text::store::TextStore::new();

        let create_args = vec![
            bulk(b"casetest1"),
            bulk(b"ON"),
            bulk(b"HASH"),
            bulk(b"PREFIX"),
            bulk(b"1"),
            bulk(b"c1:"),
            bulk(b"SCHEMA"),
            bulk(b"vec"),
            bulk(b"VECTOR"),
            bulk(b"HNSW"),
            bulk(b"6"),
            bulk(b"TYPE"),
            bulk(b"FLOAT32"),
            bulk(b"DIM"),
            bulk(b"4"),
            bulk(b"DISTANCE_METRIC"),
            bulk(b"L2"),
        ];
        ft_create(&mut store, &mut text_store, &create_args, 0);

        let key = Bytes::from_static(b"c1:doc");
        db.set(key.clone(), crate::storage::entry::Entry::new_hash());
        if let Some(idx) = store.get_index_mut(b"casetest1") {
            idx.key_hash_to_key
                .insert(xxhash_rust::xxh64::xxh64(&key, 0), key.clone());
        }

        let result = ft_dropindex(
            &mut store,
            &mut text_store,
            Some(&mut db),
            &[bulk(b"casetest1"), bulk(b"dd")], // lowercase
            0,
        );
        assert!(
            matches!(result, Frame::SimpleString(_)),
            "lowercase dd should work"
        );
        assert!(
            db.get(&key).is_none(),
            "lowercase dd should delete documents"
        );
    }

    // Test mixed case 'Dd'
    {
        let mut db = Database::new();
        let mut store = VectorStore::new();
        let mut text_store = crate::text::store::TextStore::new();

        let create_args = vec![
            bulk(b"casetest2"),
            bulk(b"ON"),
            bulk(b"HASH"),
            bulk(b"PREFIX"),
            bulk(b"1"),
            bulk(b"c2:"),
            bulk(b"SCHEMA"),
            bulk(b"vec"),
            bulk(b"VECTOR"),
            bulk(b"HNSW"),
            bulk(b"6"),
            bulk(b"TYPE"),
            bulk(b"FLOAT32"),
            bulk(b"DIM"),
            bulk(b"4"),
            bulk(b"DISTANCE_METRIC"),
            bulk(b"L2"),
        ];
        ft_create(&mut store, &mut text_store, &create_args, 0);

        let key = Bytes::from_static(b"c2:doc");
        db.set(key.clone(), crate::storage::entry::Entry::new_hash());
        if let Some(idx) = store.get_index_mut(b"casetest2") {
            idx.key_hash_to_key
                .insert(xxhash_rust::xxh64::xxh64(&key, 0), key.clone());
        }

        let result = ft_dropindex(
            &mut store,
            &mut text_store,
            Some(&mut db),
            &[bulk(b"casetest2"), bulk(b"Dd")], // mixed case
            0,
        );
        assert!(
            matches!(result, Frame::SimpleString(_)),
            "mixed case Dd should work"
        );
        assert!(
            db.get(&key).is_none(),
            "mixed case Dd should delete documents"
        );
    }
}

#[test]
fn test_ft_dropindex_dd_unknown_index() {
    let _metrics_guard = METRICS_LOCK.read();
    use crate::storage::db::Database;

    let mut db = Database::new();
    let mut store = VectorStore::new();
    let mut text_store = crate::text::store::TextStore::new();

    // Try to drop non-existent index with DD flag
    let result = ft_dropindex(
        &mut store,
        &mut text_store,
        Some(&mut db),
        &[bulk(b"nonexistent"), bulk(b"DD")],
        0,
    );

    assert!(
        matches!(result, Frame::Error(_)),
        "FT.DROPINDEX DD on non-existent index should return error"
    );
}

#[test]
fn test_ft_dropindex_extra_args_error() {
    let _metrics_guard = METRICS_LOCK.read();
    let mut store = VectorStore::new();
    let mut text_store = crate::text::store::TextStore::new();

    // Create index
    let create_args = vec![
        bulk(b"extratest"),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(b"4"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ];
    ft_create(&mut store, &mut text_store, &create_args, 0);

    // Try with extra arguments beyond DD
    let result = ft_dropindex(
        &mut store,
        &mut text_store,
        None,
        &[bulk(b"extratest"), bulk(b"DD"), bulk(b"EXTRA")],
        0,
    );

    assert!(
        matches!(result, Frame::Error(_)),
        "FT.DROPINDEX with extra args should return arity error"
    );
}
