//! Cypher result cache (Task #32, design doc `tmp/DESIGN-RESULT-CACHE-FTS.md`
//! Part A) invalidation-matrix suite.
//!
//! Every test drives the REAL command handlers directly against a
//! `GraphStore` (no server/connection plumbing needed -- same pattern as
//! `tests/graph_freeze_boundary.rs`). The key discriminator used throughout:
//! a cache HIT returns `Frame::PreSerialized(bytes)` (`graph_query_readonly`
//! returns early with pre-encoded wire bytes); a MISS (cold run OR a
//! deliberately-invalidated stale entry) always returns `Frame::Array(..)`
//! via `exec_result_to_frame`. Asserting on the *Frame variant* -- not just
//! row content -- is what actually proves "this reply came from the cache,"
//! since a miss that happens to recompute an unchanged answer would produce
//! byte-identical ROWS but a structurally different Frame.

#![cfg(feature = "graph")]

use bytes::{Bytes, BytesMut};
use moon::command::graph::graph_read::{graph_query, graph_query_or_write};
use moon::command::graph::graph_write::{graph_addedge, graph_addnode};
use moon::command::temporal::apply_invalidate;
use moon::graph::store::GraphStore;
use moon::protocol::Frame;
use moon::transaction::GraphIntent;
use moon::transaction::abort::apply_graph_rollback;

const GRAPH: &str = "g";

fn bs(s: &str) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
}

fn setup() -> GraphStore {
    let mut store = GraphStore::new();
    let lsn = store.allocate_lsn();
    store
        .create_graph(Bytes::from_static(GRAPH.as_bytes()), 64_000, lsn)
        .expect("create graph");
    store
}

fn add_node(store: &mut GraphStore, id: u64) -> u64 {
    let args = vec![bs(GRAPH), bs("N"), bs("id"), bs(&id.to_string())];
    match graph_addnode(store, &args) {
        Frame::Integer(ext) => ext as u64,
        other => panic!("ADDNODE failed: {other:?}"),
    }
}

fn add_edge(store: &mut GraphStore, src: u64, dst: u64) -> Frame {
    let args = vec![
        bs(GRAPH),
        bs(&src.to_string()),
        bs(&dst.to_string()),
        bs("E"),
    ];
    graph_addedge(store, &args)
}

fn encode(frame: &Frame, protocol_version: u8) -> Vec<u8> {
    let mut buf = BytesMut::new();
    if protocol_version >= 3 {
        moon::protocol::serialize_resp3(frame, &mut buf);
    } else {
        moon::protocol::serialize(frame, &mut buf);
    }
    buf.to_vec()
}

fn is_hit(frame: &Frame) -> bool {
    matches!(frame, Frame::PreSerialized(_))
}

fn is_miss(frame: &Frame) -> bool {
    matches!(frame, Frame::Array(_))
}

const POINT_QUERY: &str = "MATCH (a:N {id: 1}) RETURN a.id";

// ---------------------------------------------------------------------------
// (a) Hit returns byte-identical reply to a cold run -- RESP2 and RESP3.
// ---------------------------------------------------------------------------

#[test]
fn test_result_cache_hit_byte_identical_to_cold_run_resp2() {
    let mut store = setup();
    add_node(&mut store, 1);
    add_node(&mut store, 2);
    add_edge(&mut store, 1, 2);

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(POINT_QUERY)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(POINT_QUERY)], Some(2));
    assert!(is_miss(&cold), "first call must be a cold miss: {cold:?}");

    let hit = graph_query(&store, &[bs(GRAPH), bs(POINT_QUERY)], Some(2));
    assert!(
        is_hit(&hit),
        "second identical call must be a cache hit: {hit:?}"
    );

    assert_eq!(
        encode(&cold, 2),
        encode(&hit, 2),
        "cache hit must be byte-identical to the cold run (RESP2)"
    );
}

#[test]
fn test_result_cache_hit_byte_identical_to_cold_run_resp3() {
    let mut store = setup();
    add_node(&mut store, 1);
    add_node(&mut store, 2);
    add_edge(&mut store, 1, 2);

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(POINT_QUERY)], Some(3));
    let cold = graph_query(&store, &[bs(GRAPH), bs(POINT_QUERY)], Some(3));
    assert!(is_miss(&cold), "first call must be a cold miss: {cold:?}");

    let hit = graph_query(&store, &[bs(GRAPH), bs(POINT_QUERY)], Some(3));
    assert!(
        is_hit(&hit),
        "second identical call must be a cache hit: {hit:?}"
    );

    assert_eq!(
        encode(&cold, 3),
        encode(&hit, 3),
        "cache hit must be byte-identical to the cold run (RESP3)"
    );
}

#[test]
fn test_result_cache_miss_on_different_literal() {
    let mut store = setup();
    add_node(&mut store, 1);
    add_node(&mut store, 2);

    let r1 = graph_query(
        &store,
        &[bs(GRAPH), bs("MATCH (a:N {id: 1}) RETURN a.id")],
        Some(2),
    );
    let r2 = graph_query(
        &store,
        &[bs(GRAPH), bs("MATCH (a:N {id: 2}) RETURN a.id")],
        Some(2),
    );
    // Different raw query text (different literal) -- both cold misses,
    // never sharing an entry.
    assert!(is_miss(&r1) && is_miss(&r2));
}

// ---------------------------------------------------------------------------
// (b) Every mutation site invalidates.
// ---------------------------------------------------------------------------

#[test]
fn test_result_cache_invalidated_by_addnode() {
    let mut store = setup();
    add_node(&mut store, 1);
    let q = "MATCH (n:N) RETURN n.id";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit), "must be a hit before the new ADDNODE");

    add_node(&mut store, 2);

    let after = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_miss(&after),
        "ADDNODE must invalidate the cache (write_gen bump), got {after:?}"
    );
    if let Frame::Array(items) = &after {
        if let Frame::Array(rows) = &items[1] {
            assert_eq!(rows.len(), 2, "fresh result must reflect the new node");
        }
    }
}

#[test]
fn test_result_cache_invalidated_by_addedge() {
    let mut store = setup();
    add_node(&mut store, 1);
    add_node(&mut store, 2);
    let q = "MATCH (a)-[:E]->(b) RETURN b.id";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit));

    let r = add_edge(&mut store, 1, 2);
    assert!(matches!(r, Frame::Integer(_)), "ADDEDGE failed: {r:?}");

    let after = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_miss(&after),
        "ADDEDGE must invalidate the cache, got {after:?}"
    );
}

#[test]
fn test_result_cache_invalidated_by_cypher_create_via_query_path() {
    let mut store = setup();
    add_node(&mut store, 1);
    let q = "MATCH (n:N) RETURN n.id";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit));

    // Production write dispatch: GRAPH.QUERY with a CREATE clause.
    let (write_resp, _intents, _undo) =
        graph_query_or_write(&mut store, &[bs(GRAPH), bs("CREATE (:N {id: 99})")]);
    assert!(!matches!(write_resp, Frame::Error(_)), "{write_resp:?}");

    let after = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_miss(&after),
        "Cypher CREATE must invalidate the cache, got {after:?}"
    );
    if let Frame::Array(items) = &after {
        if let Frame::Array(rows) = &items[1] {
            assert_eq!(rows.len(), 2, "fresh result must include the CREATEd node");
        }
    }
}

#[test]
fn test_result_cache_idempotent_merge_does_not_invalidate() {
    // A MERGE match-branch (no mutation records) must NOT pay an
    // invalidation for a no-op -- the cache entry must survive.
    let mut store = setup();
    add_node(&mut store, 1);
    let (r, _, _) = graph_query_or_write(
        &mut store,
        &[bs(GRAPH), bs("MERGE (n:N {id: 1}) RETURN n.id")],
    );
    assert!(!matches!(r, Frame::Error(_)), "{r:?}");

    let q = "MATCH (n:N) RETURN n.id";
    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit1 = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit1));

    // Re-run the SAME idempotent MERGE (matches the existing node -- zero
    // mutation records).
    let (r2, _, _) = graph_query_or_write(
        &mut store,
        &[bs(GRAPH), bs("MERGE (n:N {id: 1}) RETURN n.id")],
    );
    assert!(!matches!(r2, Frame::Error(_)), "{r2:?}");

    let hit2 = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_hit(&hit2),
        "idempotent MERGE match-branch must NOT invalidate the cache, got {hit2:?}"
    );
}

#[test]
fn test_result_cache_invalidated_by_cypher_set() {
    let mut store = setup();
    add_node(&mut store, 1);
    let q = "MATCH (n:N {id: 1}) RETURN n.tag";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit));

    let (r, _, _) = graph_query_or_write(
        &mut store,
        &[bs(GRAPH), bs("MATCH (n:N {id: 1}) SET n.tag = 'x'")],
    );
    assert!(!matches!(r, Frame::Error(_)), "{r:?}");

    let after = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_miss(&after),
        "Cypher SET must invalidate the cache, got {after:?}"
    );
}

#[test]
fn test_result_cache_invalidated_by_cypher_delete() {
    let mut store = setup();
    add_node(&mut store, 1);
    add_node(&mut store, 2);
    let q = "MATCH (n:N) RETURN n.id";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit));

    let (r, _, _) =
        graph_query_or_write(&mut store, &[bs(GRAPH), bs("MATCH (n:N {id: 2}) DELETE n")]);
    assert!(!matches!(r, Frame::Error(_)), "{r:?}");

    let after = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_miss(&after),
        "Cypher DELETE must invalidate the cache, got {after:?}"
    );
    if let Frame::Array(items) = &after {
        if let Frame::Array(rows) = &items[1] {
            assert_eq!(
                rows.len(),
                1,
                "deleted node must be gone from the fresh result"
            );
        }
    }
}

/// TEMPORAL.INVALIDATE mutates `valid_to` directly on `write_buf` -- only
/// observable through an explicit `VALID_AT` filter (an omitted VALID_AT
/// means "no time filter," per `ExecutionContext::valid_time_as_of` docs).
/// `VALID_AT` is far in the future so it's inside every node's default
/// `valid_to = i64::MAX` BEFORE invalidation, and outside the post-
/// invalidation `valid_to = wall_ms` (now) AFTER.
#[test]
fn test_result_cache_invalidated_by_temporal_invalidate() {
    let mut store = setup();
    let node_id = add_node(&mut store, 1);
    // ~ year 2255 in Unix ms: comfortably beyond "now" but far short of i64::MAX.
    const FAR_FUTURE_MS: &str = "9000000000000";
    let args = vec![
        bs(GRAPH),
        bs("MATCH (n:N) RETURN n.id"),
        bs("VALID_AT"),
        bs(FAR_FUTURE_MS),
    ];

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &args, Some(2));
    let cold = graph_query(&store, &args, Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &args, Some(2));
    assert!(is_hit(&hit), "must be a hit before TEMPORAL.INVALIDATE");
    if let Frame::Array(items) = &cold {
        if let Frame::Array(rows) = &items[1] {
            assert_eq!(rows.len(), 1, "node must be visible before invalidation");
        }
    }

    let graph_name = Bytes::from_static(GRAPH.as_bytes());
    let wall_ms = moon::command::temporal::capture_wall_ms();
    apply_invalidate(&mut store, node_id, true, &graph_name, wall_ms)
        .expect("apply_invalidate must succeed");

    let after = graph_query(&store, &args, Some(2));
    assert!(
        is_miss(&after),
        "TEMPORAL.INVALIDATE must invalidate the result cache, got {after:?}"
    );
    if let Frame::Array(items) = &after {
        if let Frame::Array(rows) = &items[1] {
            assert_eq!(
                rows.len(),
                0,
                "node must no longer be visible at the far-future VALID_AT"
            );
        }
    }
}

/// TXN.ABORT rollback of a create-intent (GraphIntent) must invalidate: a
/// cached query that already returned rows for the now-rolled-back entity
/// must not keep serving them.
#[test]
fn test_result_cache_invalidated_by_txn_abort_create_intent() {
    let mut store = setup();
    add_node(&mut store, 1);
    let new_id = add_node(&mut store, 2);
    let q = "MATCH (n:N) RETURN n.id";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit));
    if let Frame::Array(items) = &cold {
        if let Frame::Array(rows) = &items[1] {
            assert_eq!(rows.len(), 2);
        }
    }

    // Simulate TXN.ABORT rolling back the second ADDNODE's create-intent.
    let intents = vec![GraphIntent {
        graph_name: Bytes::from_static(GRAPH.as_bytes()),
        entity_id: new_id,
        is_node: true,
    }];
    let _wal = apply_graph_rollback(&mut store, /* txn_id */ 1, &[], &intents);

    let after = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_miss(&after),
        "TXN.ABORT create-intent rollback must invalidate the cache, got {after:?}"
    );
    if let Frame::Array(items) = &after {
        if let Frame::Array(rows) = &items[1] {
            assert_eq!(rows.len(), 1, "rolled-back node must be gone");
        }
    }
}

/// TXN.ABORT rollback of a RestoreProperty undo op must invalidate: a
/// cached query filtered on the property's pre-abort (uncommitted) value
/// must not keep serving stale rows once the abort restores the old value.
/// Uses the REAL undo op emitted by `graph_query_or_write`'s SET handling
/// (`execute_write_plan` returns it) rather than hand-rolling internals.
#[test]
fn test_result_cache_invalidated_by_txn_abort_restore_property() {
    let mut store = setup();
    add_node(&mut store, 1);

    let (r, _intents, undo_ops) = graph_query_or_write(
        &mut store,
        &[bs(GRAPH), bs("MATCH (n:N {id: 1}) SET n.tag = 42")],
    );
    assert!(!matches!(r, Frame::Error(_)), "{r:?}");
    assert_eq!(
        undo_ops.len(),
        1,
        "a single-node SET must emit exactly one undo op: {undo_ops:?}"
    );

    let q = "MATCH (n:N {id: 1}) RETURN n.tag";
    // The SET above already bumped write_gen, so this is a cold run.
    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit), "must be a hit before the abort");

    let _wal = apply_graph_rollback(&mut store, 2, &undo_ops, &[]);

    let after = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_miss(&after),
        "TXN.ABORT RestoreProperty rollback must invalidate the cache, got {after:?}"
    );
}

/// TXN.ABORT with NO graph-side undo/intents (KV-only or vector-only
/// transaction) must NOT touch an unrelated graph's cache -- a hit before
/// the abort must remain a hit after.
#[test]
fn test_result_cache_txn_abort_with_no_graph_ops_does_not_invalidate() {
    let mut store = setup();
    add_node(&mut store, 1);
    let q = "MATCH (n:N) RETURN n.id";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit1 = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit1));

    // Empty undo/intents -- mirrors a KV-only TXN.ABORT (apply_graph_rollback
    // is only even called when graph_undo/graph_intents are non-empty in
    // production; calling it here with empty slices exercises the no-op
    // path directly).
    let _wal = apply_graph_rollback(&mut store, 3, &[], &[]);

    let hit2 = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_hit(&hit2),
        "a graph-op-free TXN.ABORT must not invalidate an unrelated cache entry, got {hit2:?}"
    );
}

// ---------------------------------------------------------------------------
// (c) freeze/compact does NOT invalidate.
// ---------------------------------------------------------------------------

#[test]
fn test_result_cache_not_invalidated_by_freeze_compact() {
    let mut store = setup();
    add_node(&mut store, 1);
    add_node(&mut store, 2);
    let q = "MATCH (n:N) RETURN n.id";

    // Doorkeeper warm-up: the FIRST sighting of a key only records its
    // admission fingerprint (result_cache::should_admit) -- the entry is
    // stored on the second sighting and hits from the third.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let cold = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_miss(&cold));
    let hit1 = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(is_hit(&hit1), "must be a hit before freeze");

    let write_gen_before = store.get_graph(GRAPH.as_bytes()).unwrap().write_gen;
    let compact_lsn = store.allocate_lsn();
    let compacted = store
        .get_graph_mut(GRAPH.as_bytes())
        .unwrap()
        .freeze_and_compact(compact_lsn);
    assert!(
        compacted,
        "freeze_and_compact must succeed on non-empty data"
    );
    let write_gen_after = store.get_graph(GRAPH.as_bytes()).unwrap().write_gen;
    assert_eq!(
        write_gen_before, write_gen_after,
        "freeze_and_compact must NOT bump write_gen"
    );

    let hit2 = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        is_hit(&hit2),
        "the cache entry must survive freeze/compact, got {hit2:?}"
    );
    assert_eq!(
        encode(&hit1, 2),
        encode(&hit2, 2),
        "post-freeze hit must still be the identical cached bytes"
    );
}

// ---------------------------------------------------------------------------
// (d) decay queries are never cached.
// ---------------------------------------------------------------------------

#[test]
fn test_result_cache_never_caches_decay_query() {
    let mut store = setup();
    let a = add_node(&mut store, 1);
    let b = add_node(&mut store, 2);
    let _ = (a, b);
    add_edge(&mut store, a, b);

    let q = "MATCH p = shortestPath((a:N {id: 1})-[*..3]->(b:N {id: 2})) RETURN p";
    let args = vec![bs(GRAPH), bs(q), bs("--decay"), bs("0.01")];

    let r1 = graph_query(&store, &args, Some(2));
    assert!(!matches!(r1, Frame::Error(_)), "{r1:?}");
    assert!(
        is_miss(&r1),
        "decay query must never be a cache hit: {r1:?}"
    );

    let r2 = graph_query(&store, &args, Some(2));
    assert!(!matches!(r2, Frame::Error(_)), "{r2:?}");
    assert!(
        is_miss(&r2),
        "repeated decay query must STILL never be a cache hit: {r2:?}"
    );
}

// ---------------------------------------------------------------------------
// (e) error/timeout results are never cached.
// ---------------------------------------------------------------------------

#[test]
fn test_result_cache_never_caches_timeout_error() {
    // Larger threshold than `setup()` so the 400-node chain below never
    // freezes mid-build (mirrors tests/graph_freeze_boundary.rs::timeout_fixture).
    let mut store = GraphStore::new();
    let lsn = store.allocate_lsn();
    store
        .create_graph(Bytes::from_static(GRAPH.as_bytes()), 1_000_000, lsn)
        .expect("create graph");
    let handles: Vec<u64> = (0..400).map(|i| add_node(&mut store, i)).collect();
    for w in handles.windows(2) {
        let r = add_edge(&mut store, w[0], w[1]);
        assert!(matches!(r, Frame::Integer(_)), "chain edge failed: {r:?}");
    }

    const TIMEOUT_QUERY: &str = "MATCH p = shortestPath((a:N)-[*..20]->(b:N)) RETURN p";
    let timeout_args = vec![bs(GRAPH), bs(TIMEOUT_QUERY), bs("TIMEOUT"), bs("1")];

    let r = graph_query(&store, &timeout_args, Some(2));
    let Frame::Error(e) = &r else {
        panic!("TIMEOUT 1 must abort the all-pairs traversal, got {r:?}");
    };
    assert!(
        String::from_utf8_lossy(e).contains("traversal timeout"),
        "unexpected error: {e:?}"
    );

    // Re-run with a large timeout: must still execute fully (Frame::Array),
    // never a cache hit -- the errored attempt must not have populated
    // anything under this key.
    let unlimited_args = vec![bs(GRAPH), bs(TIMEOUT_QUERY), bs("TIMEOUT"), bs("0")];
    let after = graph_query(&store, &unlimited_args, Some(2));
    assert!(
        is_miss(&after) && !matches!(after, Frame::Error(_)),
        "post-timeout re-run must execute fresh, not be a leaked cache entry: {after:?}"
    );
}

// ---------------------------------------------------------------------------
// (f) resident_bytes accounting is visible to GraphStore::resident_bytes.
// ---------------------------------------------------------------------------

#[test]
fn test_result_cache_resident_bytes_visible_to_graphstore() {
    let mut store = setup();
    add_node(&mut store, 1);
    add_node(&mut store, 2);

    let before = store.resident_bytes();
    let q = "MATCH (n:N) RETURN n.id";
    // Two calls: the first only records the doorkeeper fingerprint, the
    // second actually populates the cache entry.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let after_populate = store.resident_bytes();
    assert!(
        after_populate > before,
        "resident_bytes must grow after a cache entry is populated: {before} -> {after_populate}"
    );

    store
        .get_graph(GRAPH.as_bytes())
        .unwrap()
        .result_cache
        .lock()
        .clear();
    let after_clear = store.resident_bytes();
    assert!(
        after_clear < after_populate,
        "resident_bytes must shrink after the cache is cleared: {after_populate} -> {after_clear}"
    );
}

// ---------------------------------------------------------------------------
// (g) GRAPH.DELETE clears (the whole NamedGraph -- and its cache -- goes
// away with it).
// ---------------------------------------------------------------------------

#[test]
fn test_result_cache_cleared_by_graph_delete() {
    let mut store = setup();
    add_node(&mut store, 1);
    let q = "MATCH (n:N) RETURN n.id";
    // Two calls: doorkeeper fingerprint first, cache population second.
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    let _ = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
    assert!(
        store
            .get_graph(GRAPH.as_bytes())
            .unwrap()
            .result_cache
            .lock()
            .resident_bytes()
            > 0
    );

    // GRAPH.DELETE removes the whole NamedGraph -- and its cache -- from
    // the HashMap; there is nothing left to serve a stale hit from.
    store.drop_graph(GRAPH.as_bytes()).expect("drop graph");
    assert!(store.get_graph(GRAPH.as_bytes()).is_none());
}
