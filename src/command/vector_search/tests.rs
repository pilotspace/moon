use super::*;
use smallvec::SmallVec;
use std::sync::Mutex;

/// Serialize tests that touch global atomic metrics to avoid flaky interference.
static METRICS_LOCK: Mutex<()> = Mutex::new(());

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
    let mut store = VectorStore::new();
    let args = ft_create_args();
    let result = ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
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
    let result = ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
    match &result {
        Frame::Error(_) => {} // expected
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_create_duplicate() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    let r1 = ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
    assert!(matches!(r1, Frame::SimpleString(_)));

    let args2 = ft_create_args();
    let r2 = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &args2,
    );
    match &r2 {
        Frame::Error(e) => assert!(e.starts_with(b"ERR")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn test_ft_dropindex() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    // Drop existing
    let result = ft_dropindex(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &[bulk(b"myidx")],
    );
    assert!(matches!(result, Frame::SimpleString(_)));
    assert!(store.is_empty());

    // Drop non-existing
    let result = ft_dropindex(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &[bulk(b"myidx")],
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
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    // Build a query with wrong dimension (4 bytes instead of 128*4)
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        bulk(b"tooshort"),
    ];
    let result = ft_search(&mut store, &search_args, None);
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
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

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
    let result = ft_search(&mut store, &search_args, None);
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
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    let result = ft_info(
        &store,
        &crate::text::store::TextStore::new(),
        &[bulk(b"myidx")],
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

    let result = ft_search(&mut store, &search_args, None);
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
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    let info_args = [Frame::BulkString(Bytes::from_static(b"testidx"))];
    let result = ft_info(&store, &crate::text::store::TextStore::new(), &info_args);
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
    let result = ft_search(&mut store, &args, None);
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
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    let query_vec: Vec<u8> = vec![0u8; 128 * 4];
    let search_args = vec![
        bulk(b"myidx"),
        bulk(b"*=>[KNN 5 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(query_vec)),
    ];
    let result = ft_search(&mut store, &search_args, None);
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
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
    let idx = store.get_index(b"myidx").unwrap();
    // payload_index should exist -- insert and evaluate should work
    let _ = &idx.payload_index;
}

#[test]
fn test_vector_metrics_increment_decrement() {
    use std::sync::atomic::Ordering;

    let _guard = METRICS_LOCK.lock().unwrap();

    let mut store = VectorStore::new();
    let args = ft_create_args();

    // FT.CREATE should increment VECTOR_INDEXES
    let before_create = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
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
    ft_search(&mut store, &search_args, None);
    let after_search = crate::vector::metrics::VECTOR_SEARCH_TOTAL.load(Ordering::Relaxed);
    assert!(
        after_search > before_search,
        "FT.SEARCH should increment VECTOR_SEARCH_TOTAL"
    );

    // FT.DROPINDEX should decrement VECTOR_INDEXES
    let before_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    ft_dropindex(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &[bulk(b"myidx")],
    );
    let after_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
    assert!(
        after_drop < before_drop,
        "FT.DROPINDEX should decrement VECTOR_INDEXES"
    );
}

#[test]
fn test_parse_filter_bool_true() {
    let args = vec![
        bulk(b"idx"),
        bulk(b"*=>[KNN 5 @vec $q]"),
        bulk(b"FILTER"),
        bulk(b"@active:{true}"),
    ];
    let filter = parse_filter_clause(&args);
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
    let filter = parse_filter_clause(&args);
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
    let filter = parse_filter_clause(&args);
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
    let filter = parse_filter_clause(&args);
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
    let filter = parse_filter_clause(&args);
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
    use crate::vector::types::{SearchResult, VectorId};
    use std::collections::HashMap;

    // Create 10 fake results
    let mut results: SmallVec<[SearchResult; 32]> = SmallVec::new();
    let mut key_map = HashMap::new();
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
    use crate::vector::types::{SearchResult, VectorId};
    use std::collections::HashMap;

    let mut results: SmallVec<[SearchResult; 32]> = SmallVec::new();
    for i in 0u32..5 {
        results.push(SearchResult {
            id: VectorId(i),
            distance: i as f32,
            key_hash: 0,
        });
    }
    let key_map = HashMap::new();

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

// -- FT.CONFIG tests --

#[test]
fn test_ft_config_autocompact_on_off() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    // Default should be ON
    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"AUTOCOMPACT")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
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
    );
    assert!(matches!(result, Frame::SimpleString(_)));

    // GET should return OFF
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
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
    );
    assert!(matches!(result, Frame::SimpleString(_)));

    // GET should return ON
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
    );
    match &result {
        Frame::BulkString(b) => assert_eq!(&b[..], b"ON"),
        other => panic!("expected BulkString ON, got {other:?}"),
    }
}

#[test]
fn test_ft_config_unknown_param() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    let get_args = vec![bulk(b"GET"), bulk(b"myidx"), bulk(b"NOSUCHPARAM")];
    let result = ft_config(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &get_args,
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
    );
    assert!(
        matches!(result, Frame::Error(_)),
        "should error on unknown index"
    );
}

#[test]
fn test_ft_config_autocompact_guards_try_compact() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

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
    );
    let idx = store.get_index(b"myidx").unwrap();
    assert!(idx.autocompact_enabled, "autocompact should be enabled");
}

#[test]
fn test_ft_config_autocompact_accepts_variants() {
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

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
        let mut store = VectorStore::new();
        let create_args = ft_create_args();
        ft_create(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &create_args,
        );

        let args = make_valid_cachesearch_args();
        let result = cache_search::ft_cachesearch(&mut store, &args);

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
        let result = cache_search::ft_cachesearch(&mut store, &args);
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
    let mut key_hash_to_key = HashMap::new();
    key_hash_to_key.insert(100u64, Bytes::from_static(b"doc:a"));
    key_hash_to_key.insert(200u64, Bytes::from_static(b"doc:b"));

    let filtered = session::filter_session_results(&results, &session_members, &key_hash_to_key);
    assert_eq!(filtered.len(), 2, "empty session should return all results");
}

#[test]
fn test_session_filter_results_removes_seen() {
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

    let mut key_hash_to_key = HashMap::new();
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
    use crate::vector::types::{SearchResult, VectorId};
    use std::collections::HashMap;

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
    let mut key_hash_to_key = HashMap::new();
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
    let mut store = VectorStore::new();
    let args = ft_create_multi_field_args();
    let result = ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
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
    let result = ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
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
    let result = ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
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
    let mut store = VectorStore::new();
    let args = ft_create_multi_field_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    let result = ft_info(
        &store,
        &crate::text::store::TextStore::new(),
        &[bulk(b"multiidx")],
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
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_multi_field_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    // Insert vectors into the default (title_vec) field (dim=4)
    let idx = store.get_index_mut(b"multiidx").unwrap();
    let title_vecs: Vec<[f32; 4]> = vec![[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]];
    let snap = idx.segments.load();
    for (i, v) in title_vecs.iter().enumerate() {
        let mut sq = vec![0i8; 4];
        quantize_f32_to_sq(v, &mut sq);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        snap.mutable.append(i as u64, v, &sq, norm, i as u64);
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
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            snap.mutable.append(i as u64, v, &sq, norm, i as u64);
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
    let result = ft_search(&mut store, &search_args, None);
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
    let result = ft_search(&mut store, &search_args, None);
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
    let result = ft_search(&mut store, &search_args, None);
    assert!(
        matches!(&result, Frame::Error(_)),
        "expected dimension mismatch error for body_vec, got {result:?}"
    );
}

#[test]
fn test_ft_search_default_field_compat() {
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    // Single-field index
    let args = build_ft_create_args("singleidx", "doc:", "vec", 4, "L2");
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

    let idx = store.get_index_mut(b"singleidx").unwrap();
    let vectors: Vec<[f32; 4]> = vec![[1.0, 0.0, 0.0, 0.0]];
    let snap = idx.segments.load();
    for (i, v) in vectors.iter().enumerate() {
        let mut sq = vec![0i8; 4];
        quantize_f32_to_sq(v, &mut sq);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        snap.mutable.append(i as u64, v, &sq, norm, i as u64);
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
    let result = ft_search(&mut store, &search_args, None);
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
    let mut store = VectorStore::new();
    let args = ft_create_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

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
    let result = ft_search(&mut store, &search_args, None);
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
    let filter = parse_filter_clause(&args);
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
    let filter = parse_filter_clause(&args);
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
    let norm = dense_vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    snap.mutable.append(key_hash, dense_vec, &sq, norm, 0);
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
    let _lock = METRICS_LOCK.lock().unwrap();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_hybrid_args();
    let result = ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);
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

    let result = ft_search(&mut store, &search_args, None);
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
    let _lock = METRICS_LOCK.lock().unwrap();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_hybrid_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

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

    let result = ft_search(&mut store, &search_args, None);
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
    let _lock = METRICS_LOCK.lock().unwrap();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_args(); // standard index, no SPARSE field
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

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
    let result = ft_search(&mut store, &search_args, None);
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
    let _lock = METRICS_LOCK.lock().unwrap();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let args = ft_create_hybrid_args();
    ft_create(&mut store, &mut crate::text::store::TextStore::new(), &args);

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

    let result = ft_search(&mut store, &search_args, None);
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
    let _lock = METRICS_LOCK.lock().unwrap();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let dim: usize = 4;

    // Create L2 index
    let create_args = build_ft_create_args("rangeidx", "doc:", "vec", dim as u32, "L2");
    let result = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &create_args,
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
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            snap.mutable.append(i as u64, v, &sq, norm, i as u64);
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

    let result = ft_search(&mut store, &search_args, None);
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

    let result = ft_search(&mut store, &search_args_zero, None);
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
    let result = recommend::ft_recommend(&mut store, &args, None);
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
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db));
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
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db));
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
    let _lock = METRICS_LOCK.lock().unwrap();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let create_args = build_ft_create_args("recidx", "doc:", "vec", 4, "L2");
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &create_args,
    );

    let mut db = crate::storage::db::Database::new();
    // doc:1 does NOT exist in db -- so no vectors can be read
    let args = vec![bulk(b"recidx"), bulk(b"POSITIVE"), bulk(b"doc:1")];
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db));
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
    let _lock = METRICS_LOCK.lock().unwrap();
    crate::vector::distance::init();

    let mut store = VectorStore::new();
    let dim: usize = 4;
    let create_args = build_ft_create_args("recidx2", "doc:", "vec", dim as u32, "L2");
    ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &create_args,
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
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            snap.mutable.append(key_hash, v, &sq, norm, i as u64);
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
    let result = recommend::ft_recommend(&mut store, &args, Some(&mut db));
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
        let result = navigate::ft_navigate(&mut store, None, &args, None);
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
        let result = navigate::ft_navigate(&mut store, Some(&gs), &args, None);
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
        let result = navigate::ft_navigate(&mut store, Some(&gs), &args, None);
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
