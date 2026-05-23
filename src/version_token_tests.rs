//! RED + GREEN tests for per-engine monotonic `version_token`.
//!
//! Covers:
//! - `version_token_starts_at_zero` — each engine initialises to 0.
//! - `version_token_bumps_on_write` — successful write increments the counter.
//! - `version_token_no_bump_on_failed_write` — failed writes leave counter unchanged.
//! - `version_token_monotonic_under_concurrent_writes` — concurrent bumps produce
//!   the correct final value (exercises atomicity of `AtomicU64`).
//! - `version_token_persists_in_info_response` — FT.INFO / GRAPH.INFO include the
//!   `version_token` / `vector_version_token` field with a u64 value.
//!
//! Feature gates:
//! - Vector / TextStore tests: no extra feature (always compiled).
//! - TextStore mutation tests: `#[cfg(feature = "text-index")]`.
//! - Graph tests: `#[cfg(feature = "graph")]`.

#[cfg(test)]
mod version_token_tests {
    use bytes::Bytes;
    use crate::protocol::Frame;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    #[cfg(feature = "graph")]
    fn make_graph_cmd(parts: &[&[u8]]) -> Frame {
        use crate::protocol::FrameVec;
        let frames: Vec<Frame> = parts.iter().map(|p| bulk(p)).collect();
        Frame::Array(FrameVec::from_vec(frames))
    }

    // -----------------------------------------------------------------------
    // VectorStore — version_token field + accessors
    // -----------------------------------------------------------------------

    #[test]
    fn vector_store_version_token_starts_at_zero() {
        let store = crate::vector::store::VectorStore::new();
        assert_eq!(store.version_token(), 0, "VectorStore token must start at 0");
    }

    #[test]
    fn vector_store_bump_version_increments() {
        let store = crate::vector::store::VectorStore::new();
        assert_eq!(store.bump_version(), 1);
        assert_eq!(store.bump_version(), 2);
        assert_eq!(store.version_token(), 2);
    }

    /// After `create_index` succeeds, version_token must be > 0.
    ///
    /// RED: passes structurally (AtomicU64 field exists), but the bump inside
    /// `create_index` is not yet wired, so `version_token()` stays 0 → FAIL.
    #[test]
    fn vector_store_version_token_bumps_on_create_index() {
        use crate::vector::store::VectorStore;

        let mut store = VectorStore::new();
        let before = store.version_token();

        // Build a minimal IndexMeta; non-critical fields use defaults.
        let args: Vec<Frame> = [
            b"idx".as_ref(),
            b"ON", b"HASH",
            b"PREFIX", b"1", b"doc:",
            b"SCHEMA",
            b"vec", b"VECTOR", b"HNSW", b"6",
            b"TYPE", b"FLOAT32",
            b"DIM", b"4",
            b"DISTANCE_METRIC", b"L2",
        ]
        .iter()
        .map(|s| bulk(s))
        .collect();

        let result = crate::command::vector_search::ft_create(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &args,
        );
        assert!(
            matches!(result, Frame::SimpleString(_)),
            "ft_create must return OK, got {result:?}"
        );

        assert_eq!(
            store.version_token(),
            before + 1,
            "VectorStore token must increment after successful ft_create"
        );
    }

    /// After `drop_index` succeeds, version_token must have incremented.
    ///
    /// RED: drop_index bump not yet wired → FAIL.
    #[test]
    fn vector_store_version_token_bumps_on_drop_index() {
        use crate::vector::store::VectorStore;

        let mut store = VectorStore::new();
        let create_args: Vec<Frame> = [
            b"idx2".as_ref(),
            b"ON", b"HASH",
            b"PREFIX", b"1", b"doc:",
            b"SCHEMA",
            b"vec", b"VECTOR", b"HNSW", b"6",
            b"TYPE", b"FLOAT32",
            b"DIM", b"4",
            b"DISTANCE_METRIC", b"L2",
        ]
        .iter()
        .map(|s| bulk(s))
        .collect();

        crate::command::vector_search::ft_create(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &create_args,
        );

        let before = store.version_token();
        let dropped = store.drop_index(b"idx2");
        assert!(dropped, "drop_index returned false");

        assert_eq!(
            store.version_token(),
            before + 1,
            "VectorStore token must increment after successful drop_index"
        );
    }

    /// Duplicate create must fail and NOT bump the token.
    #[test]
    fn vector_store_version_token_no_bump_on_failed_write() {
        use crate::vector::store::VectorStore;

        let mut store = VectorStore::new();
        let create_args: Vec<Frame> = [
            b"dup".as_ref(),
            b"ON", b"HASH",
            b"PREFIX", b"1", b"x:",
            b"SCHEMA",
            b"vec", b"VECTOR", b"HNSW", b"6",
            b"TYPE", b"FLOAT32",
            b"DIM", b"4",
            b"DISTANCE_METRIC", b"L2",
        ]
        .iter()
        .map(|s| bulk(s))
        .collect();

        crate::command::vector_search::ft_create(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &create_args,
        );
        let after_first = store.version_token();

        // Duplicate — must return error and not bump.
        let result = crate::command::vector_search::ft_create(
            &mut store,
            &mut crate::text::store::TextStore::new(),
            &create_args,
        );
        assert!(
            matches!(result, Frame::Error(_)),
            "duplicate ft_create must return error"
        );
        assert_eq!(
            store.version_token(),
            after_first,
            "VectorStore token must NOT increment on failed ft_create"
        );
    }

    /// 8 tasks × 100 `bump_version` calls each → final token must equal 800.
    #[test]
    fn vector_store_version_token_monotonic_concurrent() {
        use crate::vector::store::VectorStore;
        use std::sync::Arc;

        let store = Arc::new(VectorStore::new());
        let tasks = 8u64;
        let writes_per_task = 100u64;

        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let s = Arc::clone(&store);
                let n = writes_per_task;
                std::thread::spawn(move || {
                    for _ in 0..n {
                        s.bump_version();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        assert_eq!(
            store.version_token(),
            tasks * writes_per_task,
            "VectorStore: concurrent bumps must produce the exact expected total"
        );
    }

    // -----------------------------------------------------------------------
    // TextStore — version_token field + accessors
    // -----------------------------------------------------------------------

    #[test]
    fn text_store_version_token_starts_at_zero() {
        let store = crate::text::store::TextStore::new();
        assert_eq!(store.version_token(), 0, "TextStore token must start at 0");
    }

    #[test]
    fn text_store_bump_version_increments() {
        let store = crate::text::store::TextStore::new();
        assert_eq!(store.bump_version(), 1);
        assert_eq!(store.bump_version(), 2);
        assert_eq!(store.version_token(), 2);
    }

    /// After `create_index` succeeds on TextStore, version_token must increment.
    ///
    /// RED: bump not yet wired inside create_index → FAIL.
    #[cfg(feature = "text-index")]
    #[test]
    fn text_store_version_token_bumps_on_create_index() {
        use crate::text::store::{TextIndex, TextStore};

        let mut store = TextStore::new();
        let before = store.version_token();

        // TEXT-only FT.CREATE: name, prefixes, fields, bm25 config.
        let idx = TextIndex::new(
            Bytes::from_static(b"ftidx"),
            vec![Bytes::from_static(b"doc:")],
            vec![],
            Default::default(),
        );
        store
            .create_index(Bytes::from_static(b"ftidx"), idx)
            .expect("create_index failed");

        assert_eq!(
            store.version_token(),
            before + 1,
            "TextStore token must increment after successful create_index"
        );
    }

    /// After `drop_index` succeeds on TextStore, version_token must increment.
    ///
    /// RED: bump not yet wired inside drop_index → FAIL.
    #[cfg(feature = "text-index")]
    #[test]
    fn text_store_version_token_bumps_on_drop_index() {
        use crate::text::store::{TextIndex, TextStore};

        let mut store = TextStore::new();
        let idx = TextIndex::new(
            Bytes::from_static(b"ftidx2"),
            vec![Bytes::from_static(b"doc:")],
            vec![],
            Default::default(),
        );
        store
            .create_index(Bytes::from_static(b"ftidx2"), idx)
            .expect("create_index failed");
        let before = store.version_token();

        let dropped = store.drop_index(b"ftidx2");
        assert!(dropped, "drop_index returned false");

        assert_eq!(
            store.version_token(),
            before + 1,
            "TextStore token must increment after successful drop_index"
        );
    }

    /// Duplicate create must NOT bump TextStore token.
    #[cfg(feature = "text-index")]
    #[test]
    fn text_store_version_token_no_bump_on_duplicate_create() {
        use crate::text::store::{TextIndex, TextStore};

        let mut store = TextStore::new();
        let idx1 = TextIndex::new(
            Bytes::from_static(b"dup_ft"),
            vec![],
            vec![],
            Default::default(),
        );
        store
            .create_index(Bytes::from_static(b"dup_ft"), idx1)
            .expect("first create failed");
        let after_first = store.version_token();

        // Build a second index with the same name — create_index must reject it.
        let idx2 = TextIndex::new(
            Bytes::from_static(b"dup_ft"),
            vec![],
            vec![],
            Default::default(),
        );
        let result = store.create_index(Bytes::from_static(b"dup_ft"), idx2);
        assert!(result.is_err(), "duplicate create_index should fail");
        assert_eq!(
            store.version_token(),
            after_first,
            "TextStore token must NOT increment on failed create_index"
        );
    }

    /// 8 tasks × 100 `bump_version` calls each → final token must equal 800.
    #[test]
    fn text_store_version_token_monotonic_concurrent() {
        use crate::text::store::TextStore;
        use std::sync::Arc;

        let store = Arc::new(TextStore::new());
        let tasks = 8u64;
        let writes_per_task = 100u64;

        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let s = Arc::clone(&store);
                let n = writes_per_task;
                std::thread::spawn(move || {
                    for _ in 0..n {
                        s.bump_version();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        assert_eq!(
            store.version_token(),
            tasks * writes_per_task,
            "TextStore: concurrent bumps must produce the exact expected total"
        );
    }

    // -----------------------------------------------------------------------
    // GraphStore — version_token field + accessors
    // -----------------------------------------------------------------------

    #[cfg(feature = "graph")]
    #[test]
    fn graph_store_version_token_starts_at_zero() {
        let store = crate::graph::store::GraphStore::new();
        assert_eq!(store.version_token(), 0, "GraphStore token must start at 0");
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_store_bump_version_increments() {
        let store = crate::graph::store::GraphStore::new();
        assert_eq!(store.bump_version(), 1);
        assert_eq!(store.bump_version(), 2);
        assert_eq!(store.version_token(), 2);
    }

    /// After `GRAPH.CREATE` succeeds, version_token must increment.
    ///
    /// RED: bump not yet wired inside graph_create handler → FAIL.
    #[cfg(feature = "graph")]
    #[test]
    fn graph_store_version_token_bumps_on_create_graph() {
        use crate::command::graph::dispatch_graph_command;
        use crate::graph::store::GraphStore;

        let mut store = GraphStore::new();
        let before = store.version_token();

        let resp = dispatch_graph_command(
            &mut store,
            &make_graph_cmd(&[b"GRAPH.CREATE", b"g1"]),
        );
        assert!(
            matches!(resp, Frame::SimpleString(_)),
            "GRAPH.CREATE must return OK, got {resp:?}"
        );

        assert_eq!(
            store.version_token(),
            before + 1,
            "GraphStore token must increment after successful GRAPH.CREATE"
        );
    }

    /// After `GRAPH.ADDNODE` (a mutation), version_token must increment.
    ///
    /// RED: allocate_lsn bump not yet wired → FAIL.
    #[cfg(feature = "graph")]
    #[test]
    fn graph_store_version_token_bumps_on_addnode() {
        use crate::command::graph::dispatch_graph_command;
        use crate::graph::store::GraphStore;

        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_graph_cmd(&[b"GRAPH.CREATE", b"gn"]));
        let before = store.version_token();

        let resp = dispatch_graph_command(
            &mut store,
            &make_graph_cmd(&[b"GRAPH.ADDNODE", b"gn", b"Person"]),
        );
        assert!(
            matches!(resp, Frame::Integer(_)),
            "GRAPH.ADDNODE must return integer node ID, got {resp:?}"
        );

        assert!(
            store.version_token() > before,
            "GraphStore token must increment after GRAPH.ADDNODE"
        );
    }

    /// Duplicate GRAPH.CREATE must NOT bump the token.
    #[cfg(feature = "graph")]
    #[test]
    fn graph_store_version_token_no_bump_on_duplicate_create() {
        use crate::command::graph::dispatch_graph_command;
        use crate::graph::store::GraphStore;

        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_graph_cmd(&[b"GRAPH.CREATE", b"dup_g"]));
        let after_first = store.version_token();

        let resp = dispatch_graph_command(
            &mut store,
            &make_graph_cmd(&[b"GRAPH.CREATE", b"dup_g"]),
        );
        assert!(
            matches!(resp, Frame::Error(_)),
            "duplicate GRAPH.CREATE must return error"
        );
        assert_eq!(
            store.version_token(),
            after_first,
            "GraphStore token must NOT increment on failed GRAPH.CREATE"
        );
    }

    /// 8 tasks × 100 `bump_version` calls each → final token must equal 800.
    #[cfg(feature = "graph")]
    #[test]
    fn graph_store_version_token_monotonic_concurrent() {
        use crate::graph::store::GraphStore;
        use std::sync::Arc;

        let store = Arc::new(GraphStore::new());
        let tasks = 8u64;
        let writes_per_task = 100u64;

        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let s = Arc::clone(&store);
                let n = writes_per_task;
                std::thread::spawn(move || {
                    for _ in 0..n {
                        s.bump_version();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        assert_eq!(
            store.version_token(),
            tasks * writes_per_task,
            "GraphStore: concurrent bumps must produce the exact expected total"
        );
    }

    // -----------------------------------------------------------------------
    // INFO response round-trip: version_token present in wire output
    // -----------------------------------------------------------------------

    /// GRAPH.INFO must include a `version_token` field with an integer value.
    ///
    /// RED: version_token not yet appended to graph_info response → FAIL.
    #[cfg(feature = "graph")]
    #[test]
    fn graph_info_contains_version_token_field() {
        use crate::command::graph::dispatch_graph_command;
        use crate::graph::store::GraphStore;

        let mut store = GraphStore::new();
        dispatch_graph_command(&mut store, &make_graph_cmd(&[b"GRAPH.CREATE", b"mygraph"]));

        let resp = dispatch_graph_command(
            &mut store,
            &make_graph_cmd(&[b"GRAPH.INFO", b"mygraph"]),
        );

        let Frame::Map(pairs) = resp else {
            panic!("GRAPH.INFO must return Frame::Map, got {resp:?}");
        };

        let token_entry = pairs.iter().find(|(k, _)| {
            matches!(k, Frame::SimpleString(b) if b.as_ref() == b"version_token")
        });
        assert!(
            token_entry.is_some(),
            "GRAPH.INFO response must include 'version_token' field"
        );

        let (_, val) = token_entry.unwrap();
        assert!(
            matches!(val, Frame::Integer(_)),
            "version_token value must be Frame::Integer, got {val:?}"
        );
    }

    /// FT.INFO for a vector index must include `vector_version_token`.
    ///
    /// RED: field not yet appended to ft_info response → FAIL.
    #[cfg(all(feature = "graph", feature = "text-index"))]
    #[test]
    fn ft_info_vector_index_contains_vector_version_token() {
        use crate::command::vector_search::{ft_create, ft_info};
        use crate::text::store::TextStore;
        use crate::vector::store::VectorStore;

        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();

        let create_args: Vec<Frame> = [
            b"vidx".as_ref(),
            b"ON", b"HASH",
            b"PREFIX", b"1", b"doc:",
            b"SCHEMA",
            b"vec", b"VECTOR", b"HNSW", b"6",
            b"TYPE", b"FLOAT32",
            b"DIM", b"4",
            b"DISTANCE_METRIC", b"L2",
        ]
        .iter()
        .map(|s| bulk(s))
        .collect();

        let r = ft_create(&mut vs, &mut ts, &create_args);
        assert!(matches!(r, Frame::SimpleString(_)), "ft_create must return OK");

        let info_args: Vec<Frame> = vec![bulk(b"vidx")];
        let resp = ft_info(&vs, &ts, &info_args);

        let Frame::Array(items) = resp else {
            panic!("FT.INFO must return Frame::Array, got {resp:?}");
        };

        // Items are flat key-value pairs: [k0, v0, k1, v1, ...]
        let mut found_vector_version = false;
        let mut i = 0;
        while i + 1 < items.len() {
            if let Frame::BulkString(key) = &items[i] {
                if key.as_ref() == b"vector_version_token" {
                    found_vector_version = true;
                    let val = &items[i + 1];
                    assert!(
                        matches!(val, Frame::Integer(_)),
                        "vector_version_token must be Frame::Integer, got {val:?}"
                    );
                }
            }
            i += 2;
        }

        assert!(
            found_vector_version,
            "FT.INFO response must include 'vector_version_token' field; \
             got fields: {:?}",
            items
                .iter()
                .step_by(2)
                .filter_map(|f| {
                    if let Frame::BulkString(b) = f {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        );
    }

    /// FT.INFO for a TEXT-only index must include `text_version_token`.
    ///
    /// RED: field not yet appended to ft_info_text_only response → FAIL.
    #[cfg(feature = "text-index")]
    #[test]
    fn ft_info_text_index_contains_text_version_token() {
        use crate::command::vector_search::{ft_create, ft_info};
        use crate::text::store::TextStore;
        use crate::vector::store::VectorStore;

        let mut vs = VectorStore::new();
        let mut ts = TextStore::new();

        // TEXT-only FT.CREATE (no VECTOR field)
        let create_args: Vec<Frame> = [
            b"tidx".as_ref(),
            b"ON", b"HASH",
            b"PREFIX", b"1", b"post:",
            b"SCHEMA",
            b"body", b"TEXT",
        ]
        .iter()
        .map(|s| bulk(s))
        .collect();

        let r = ft_create(&mut vs, &mut ts, &create_args);
        assert!(
            matches!(r, Frame::SimpleString(_)),
            "TEXT-only ft_create must return OK, got {r:?}"
        );

        let info_args: Vec<Frame> = vec![bulk(b"tidx")];
        let resp = ft_info(&vs, &ts, &info_args);

        let Frame::Array(items) = resp else {
            panic!("FT.INFO (text-only) must return Frame::Array, got {resp:?}");
        };

        let mut found_text_version = false;
        let mut i = 0;
        while i + 1 < items.len() {
            if let Frame::BulkString(key) = &items[i] {
                if key.as_ref() == b"text_version_token" {
                    found_text_version = true;
                    let val = &items[i + 1];
                    assert!(
                        matches!(val, Frame::Integer(_)),
                        "text_version_token must be Frame::Integer, got {val:?}"
                    );
                }
            }
            i += 2;
        }

        assert!(
            found_text_version,
            "FT.INFO (text-only) response must include 'text_version_token' field; \
             got fields: {:?}",
            items
                .iter()
                .step_by(2)
                .filter_map(|f| {
                    if let Frame::BulkString(b) = f {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        );
    }
}
