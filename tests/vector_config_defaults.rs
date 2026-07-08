//! Server-wide FT.CREATE tuning defaults (`--vector-ef-runtime`,
//! `--vector-rerank-mult`, `--vector-exact-beam`) and the graph result-cache
//! limits (`--graph-result-cache-*`).
//!
//! Lives in its own integration binary ON PURPOSE: the defaults are
//! process-wide `OnceLock`s installed once at startup, so exercising the
//! installed path in the shared lib-test binary would leak the values into
//! every other vector test. Here the process is ours alone.

use bytes::Bytes;

use moon::command::vector_search::{ft_config, ft_create};
use moon::protocol::Frame;
use moon::vector::store::{VectorCreateDefaults, VectorStore, set_vector_create_defaults};

fn bulk(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

fn ft_create_args(name: &[u8]) -> Vec<Frame> {
    vec![
        bulk(name),
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
        bulk(b"8"),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ]
}

#[test]
fn server_defaults_apply_to_new_indexes_and_ft_config_overrides() {
    // Simulates the startup install (main.rs / run_embedded).
    set_vector_create_defaults(VectorCreateDefaults {
        ef_runtime: 512,
        rerank_mult: 16,
        exact_beam: true,
    });

    let mut store = VectorStore::new();
    let mut text = moon::text::store::TextStore::new();
    let result = ft_create(&mut store, &mut text, &ft_create_args(b"defidx"), 0);
    assert!(matches!(result, Frame::SimpleString(_)), "{result:?}");

    #[allow(clippy::unwrap_used)] // index just created
    let idx = store.get_index_mut_for_db(b"defidx", 0).unwrap();
    assert_eq!(idx.meta.hnsw_ef_runtime, 512, "--vector-ef-runtime default");
    assert_eq!(idx.meta.rerank_mult, 16, "--vector-rerank-mult default");
    assert!(idx.meta.exact_beam, "--vector-exact-beam default");

    // FT.CONFIG GET reads the server-installed starting values.
    let get = |store: &mut VectorStore, text: &mut moon::text::store::TextStore, param: &[u8]| {
        ft_config(
            store,
            text,
            &[bulk(b"GET"), bulk(b"defidx"), bulk(param)],
            0,
        )
    };
    match get(&mut store, &mut text, b"RERANK_MULT") {
        Frame::BulkString(b) => assert_eq!(&b[..], b"16"),
        other => panic!("expected 16, got {other:?}"),
    }
    match get(&mut store, &mut text, b"EXACT_BEAM") {
        Frame::BulkString(b) => assert_eq!(&b[..], b"ON"),
        other => panic!("expected ON, got {other:?}"),
    }

    // Per-index FT.CONFIG SET still overrides the server default.
    let result = ft_config(
        &mut store,
        &mut text,
        &[
            bulk(b"SET"),
            bulk(b"defidx"),
            bulk(b"EXACT_BEAM"),
            bulk(b"OFF"),
        ],
        0,
    );
    assert!(matches!(result, Frame::SimpleString(_)), "{result:?}");
    #[allow(clippy::unwrap_used)] // index exists
    let idx = store.get_index_mut_for_db(b"defidx", 0).unwrap();
    assert!(!idx.meta.exact_beam);

    // Explicit EF_RUNTIME in FT.CREATE wins over the server default.
    let mut args = ft_create_args(b"efidx");
    // Grow the param count (6 -> 8) and append EF_RUNTIME 64.
    let hnsw_cnt_pos = args
        .iter()
        .position(|f| matches!(f, Frame::BulkString(b) if &b[..] == b"6"))
        .expect("param count");
    args[hnsw_cnt_pos] = bulk(b"8");
    args.push(bulk(b"EF_RUNTIME"));
    args.push(bulk(b"64"));
    let result = ft_create(&mut store, &mut text, &args, 0);
    assert!(matches!(result, Frame::SimpleString(_)), "{result:?}");
    #[allow(clippy::unwrap_used)] // index just created
    let idx = store.get_index_mut_for_db(b"efidx", 0).unwrap();
    assert_eq!(idx.meta.hnsw_ef_runtime, 64, "explicit EF_RUNTIME must win");
}

#[cfg(feature = "graph")]
#[test]
fn graph_result_cache_limits_are_configurable() {
    use moon::graph::cypher::result_cache;

    // Before any install: compiled-in defaults.
    // (This runs in the same process as the vector test above, but the two
    // OnceLocks are independent; only this test touches the graph one. Test
    // ordering within the binary does not matter for that reason.)
    result_cache::set_configured_limits(64, 1 << 20);
    assert_eq!(result_cache::configured_limits(), (64, 1 << 20));

    // Second install is a no-op (first write wins).
    result_cache::set_configured_limits(9999, 9999);
    assert_eq!(result_cache::configured_limits(), (64, 1 << 20));
}
