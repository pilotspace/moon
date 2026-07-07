//! Phase-0 freeze-boundary correctness suite (2026-07 graph deep review).
//!
//! `MemGraph::freeze()` DRAINS all live nodes/edges into a CSR segment when a
//! graph crosses `edge_threshold` (production trigger: GRAPH.ADDEDGE →
//! `NamedGraph::freeze_and_compact`). Every test here drives the REAL command
//! handlers with a tiny threshold so the freeze fires after a handful of
//! inserts, then asserts that compacted data remains fully queryable.
//!
//! Written red-first: at review time (tmp/GRAPH-DEEP-REVIEW-2026-07.md §2/P0-1)
//! MATCH scans, property access, GRAPH.NEIGHBORS, GRAPH.ADDEDGE-to-frozen-node,
//! GRAPH.PROFILE, and GRAPH.HYBRID all silently lose or reject compacted data.
#![cfg(feature = "graph")]

use bytes::Bytes;
use moon::command::graph::graph_read::{
    graph_hybrid, graph_neighbors, graph_profile, graph_query, graph_query_or_write,
    graph_query_write,
};
use moon::command::graph::graph_write::{graph_addedge, graph_addnode};
use moon::graph::store::GraphStore;
use moon::protocol::Frame;

/// Freeze after 8 live edges (production default is 64_000).
const THRESHOLD: usize = 8;
const GRAPH: &str = "g";

fn bs(s: &str) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
}

fn blob(bytes: Vec<u8>) -> Frame {
    Frame::BulkString(Bytes::from(bytes))
}

/// f32 slice → little-endian byte blob (GRAPH.ADDNODE VECTOR format).
fn vec_blob(v: &[f32]) -> Frame {
    let mut out = Vec::with_capacity(v.len() * 4);
    for f in v {
        out.extend_from_slice(&f.to_le_bytes());
    }
    blob(out)
}

fn setup() -> GraphStore {
    let mut store = GraphStore::new();
    let lsn = store.allocate_lsn();
    store
        .create_graph(Bytes::from_static(GRAPH.as_bytes()), THRESHOLD, lsn)
        .expect("create graph");
    store
}

/// ADDNODE with an `id` property and a 4-dim embedding; returns the external id.
fn add_node(store: &mut GraphStore, id: u64) -> u64 {
    let args = vec![
        bs(GRAPH),
        bs("N"),
        bs("id"),
        bs(&id.to_string()),
        bs("VECTOR"),
        bs("emb"),
        vec_blob(&[id as f32, 1.0, 0.0, 0.0]),
    ];
    match graph_addnode(store, &args) {
        Frame::Integer(ext) => ext as u64,
        other => panic!("ADDNODE failed: {other:?}"),
    }
}

fn add_edge(store: &mut GraphStore, src: u64, dst: u64) -> Frame {
    graph_addedge(
        store,
        &[
            bs(GRAPH),
            bs(&src.to_string()),
            bs(&dst.to_string()),
            bs("E"),
        ],
    )
}

fn immutable_segment_count(store: &GraphStore) -> usize {
    store
        .get_graph(GRAPH.as_bytes())
        .expect("graph exists")
        .segments
        .load()
        .immutable
        .len()
}

/// Seed 6 nodes (ids 0..=5, `id` property + embedding) in a ring plus two
/// chords (8 edges total) so the freeze fires on the 8th ADDEDGE. Returns the
/// external handles indexed by logical id.
///
/// Post-seed invariant (asserted): exactly one immutable CSR segment exists and
/// the mutable write buffer has been drained.
fn seed_across_freeze(store: &mut GraphStore) -> Vec<u64> {
    let handles: Vec<u64> = (0..6).map(|i| add_node(store, i)).collect();
    let edges: [(u64, u64); 8] = [
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 0),
        (0, 2),
        (0, 3),
    ];
    for (s, d) in edges {
        let r = add_edge(store, handles[s as usize], handles[d as usize]);
        assert!(
            matches!(r, Frame::Integer(_)),
            "seed ADDEDGE {s}->{d} failed: {r:?}"
        );
    }
    assert_eq!(
        immutable_segment_count(store),
        1,
        "freeze must fire at edge_threshold={THRESHOLD}"
    );
    let g = store.get_graph(GRAPH.as_bytes()).expect("graph");
    assert_eq!(
        g.write_buf.node_count(),
        0,
        "freeze() drains the mutable write buffer"
    );
    handles
}

/// Extract the rows array from a GRAPH.QUERY reply: Array[headers, rows, stats].
fn query_rows(frame: &Frame) -> Vec<Frame> {
    match frame {
        Frame::Array(items) => match items.get(1) {
            Some(Frame::Array(rows)) => rows.iter().cloned().collect(),
            other => panic!("malformed query reply, rows slot = {other:?}"),
        },
        Frame::Error(e) => panic!("query returned error: {}", String::from_utf8_lossy(e)),
        other => panic!("malformed query reply: {other:?}"),
    }
}

fn run_query(store: &GraphStore, cypher: &str) -> Vec<Frame> {
    query_rows(&graph_query(store, &[bs(GRAPH), bs(cypher)], Some(2)))
}

/// Flatten a single-column row set into cell frames.
fn single_cells(rows: &[Frame]) -> Vec<Frame> {
    rows.iter()
        .map(|r| match r {
            Frame::Array(cells) => cells.first().cloned().expect("row has a cell"),
            other => panic!("malformed row: {other:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 1. Mechanism sanity (documents the drain — passes before and after Phase 0)
// ---------------------------------------------------------------------------

#[test]
fn freeze_fires_and_drains_write_buffer() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
}

// ---------------------------------------------------------------------------
// 2. Cypher visibility across the freeze boundary
// ---------------------------------------------------------------------------

#[test]
fn match_label_scan_sees_frozen_nodes() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let rows = run_query(&store, "MATCH (n:N) RETURN n.id");
    assert_eq!(
        rows.len(),
        6,
        "label scan must see all 6 compacted nodes, got {} rows",
        rows.len()
    );
}

#[test]
fn match_property_filter_finds_frozen_node() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let rows = run_query(&store, "MATCH (a:N {id:3}) RETURN a.id");
    assert_eq!(rows.len(), 1, "point query must find the compacted node");
}

#[test]
fn return_property_value_from_frozen_node() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let rows = run_query(&store, "MATCH (a:N {id:3}) RETURN a.id");
    let cells = single_cells(&rows);
    assert_eq!(cells.len(), 1);
    match &cells[0] {
        Frame::BulkString(b) => assert_eq!(&b[..], b"3", "property value must survive freeze"),
        Frame::Integer(i) => assert_eq!(*i, 3, "property value must survive freeze"),
        other => panic!("property lost at freeze (got {other:?})"),
    }
}

#[test]
fn one_hop_expand_from_frozen_point_query() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    // Node 0 has out-edges to 1, 2, 3.
    let rows = run_query(&store, "MATCH (a:N {id:0})-[:E]->(b) RETURN b.id");
    assert_eq!(
        rows.len(),
        3,
        "expand from compacted node must see CSR edges"
    );
}

#[test]
fn mixed_tier_match_sees_frozen_and_live_nodes() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    // One post-freeze node lives in the fresh write buffer.
    let _ = add_node(&mut store, 6);
    let rows = run_query(&store, "MATCH (n:N) RETURN n.id");
    assert_eq!(
        rows.len(),
        7,
        "scan must union the mutable tier and CSR segments"
    );
}

// ---------------------------------------------------------------------------
// 3. Native command surface across the boundary
// ---------------------------------------------------------------------------

#[test]
fn neighbors_works_on_frozen_node() {
    let mut store = setup();
    let handles = seed_across_freeze(&mut store);
    let reply = graph_neighbors(&store, &[bs(GRAPH), bs(&handles[0].to_string())]);
    match reply {
        Frame::Error(e) => panic!(
            "GRAPH.NEIGHBORS rejected a compacted node: {}",
            String::from_utf8_lossy(&e)
        ),
        Frame::Array(items) => assert!(
            !items.is_empty(),
            "compacted node 0 has degree 5 (out 1,2,3 + in 5; Both), got empty reply"
        ),
        other => panic!("unexpected NEIGHBORS reply: {other:?}"),
    }
}

#[test]
fn neighbors_direction_arg_is_honored() {
    let mut store = setup();
    let handles = seed_across_freeze(&mut store);
    // Reply layout: one edge frame + one node frame per neighbor.
    let count = |dir: &str| -> usize {
        let reply = graph_neighbors(
            &store,
            &[
                bs(GRAPH),
                bs(&handles[0].to_string()),
                bs("DIRECTION"),
                bs(dir),
            ],
        );
        match reply {
            Frame::Array(items) => items.len() / 2,
            other => panic!("NEIGHBORS DIRECTION {dir} failed: {other:?}"),
        }
    };
    assert_eq!(count("OUT"), 3, "node 0 out-neighbors: 1, 2, 3");
    assert_eq!(count("IN"), 1, "node 0 in-neighbor: 5");
    assert_eq!(count("BOTH"), 4, "union of both directions");
    let bad = graph_neighbors(
        &store,
        &[
            bs(GRAPH),
            bs(&handles[0].to_string()),
            bs("DIRECTION"),
            bs("SIDEWAYS"),
        ],
    );
    assert!(
        matches!(bad, Frame::Error(_)),
        "invalid DIRECTION must error, got {bad:?}"
    );
}

#[test]
fn addedge_between_frozen_nodes_succeeds() {
    let mut store = setup();
    let handles = seed_across_freeze(&mut store);
    // Both endpoints were drained into the CSR segment.
    let reply = add_edge(&mut store, handles[4], handles[1]);
    assert!(
        matches!(reply, Frame::Integer(_)),
        "ADDEDGE between compacted endpoints must succeed, got {reply:?}"
    );
    // And the new edge must be immediately traversable.
    let rows = run_query(&store, "MATCH (a:N {id:4})-[:E]->(b) RETURN b.id");
    assert_eq!(rows.len(), 2, "old CSR edge 4->5 plus new edge 4->1");
}

// ---------------------------------------------------------------------------
// 4. PROFILE parity across the boundary
// ---------------------------------------------------------------------------

#[test]
fn profile_rows_correct_post_freeze() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let reply = graph_profile(
        &store,
        &[bs(GRAPH), bs("MATCH (a:N {id:0})-[:E]->(b) RETURN b.id")],
    );
    // ProfileResult frame = Array[exec_result_frame, operator_profiles].
    let exec_frame = match &reply {
        Frame::Array(items) => items.first().cloned().expect("profile has exec result"),
        Frame::Error(e) => panic!("PROFILE errored: {}", String::from_utf8_lossy(e)),
        other => panic!("malformed PROFILE reply: {other:?}"),
    };
    let rows = query_rows(&exec_frame);
    assert_eq!(
        rows.len(),
        3,
        "PROFILE must execute against CSR segments like GRAPH.QUERY does"
    );
}

// ---------------------------------------------------------------------------
// 5. Hybrid graph+vector across the boundary
// ---------------------------------------------------------------------------

#[test]
fn hybrid_filter_sees_frozen_candidates() {
    let mut store = setup();
    let handles = seed_across_freeze(&mut store);
    // GRAPH.HYBRID g FILTER <start_id> <hops> <k> <vector>
    let reply = graph_hybrid(
        &store,
        &[
            bs(GRAPH),
            bs("FILTER"),
            bs(&handles[0].to_string()),
            bs("2"),
            bs("5"),
            vec_blob(&[0.0, 1.0, 0.0, 0.0]),
        ],
    );
    match reply {
        Frame::Error(e) => panic!(
            "GRAPH.HYBRID rejected/lost compacted data: {}",
            String::from_utf8_lossy(&e)
        ),
        Frame::Array(items) => assert!(
            !items.is_empty(),
            "hybrid FILTER from compacted node 0 must score its 2-hop neighborhood"
        ),
        other => panic!("unexpected HYBRID reply: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 6. Copy-up writes (W2-2): SET / DELETE / MERGE against frozen rows
// ---------------------------------------------------------------------------

fn run_write(store: &mut GraphStore, cypher: &str) -> Frame {
    graph_query_write(store, &[bs(GRAPH), bs(cypher)])
}

/// Assert a write reply is not an error frame.
fn assert_write_ok(reply: &Frame, what: &str) {
    if let Frame::Error(e) = reply {
        panic!("{what} failed: {}", String::from_utf8_lossy(e));
    }
}

#[test]
fn set_property_on_frozen_node_copies_up() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let r = run_write(&mut store, "MATCH (n:N {id:3}) SET n.score = 42");
    assert_write_ok(&r, "SET on frozen node");
    let rows = run_query(&store, "MATCH (n:N {id:3}) RETURN n.score");
    let cells = single_cells(&rows);
    assert_eq!(cells.len(), 1, "SET target must remain matchable");
    match &cells[0] {
        Frame::Integer(i) => assert_eq!(*i, 42, "copy-up SET must be visible"),
        Frame::BulkString(b) => assert_eq!(&b[..], b"42", "copy-up SET must be visible"),
        other => panic!("SET on frozen node was silently dropped (got {other:?})"),
    }
    // Untouched frozen properties survive the copy-up.
    let rows = run_query(&store, "MATCH (n:N {id:3}) RETURN n.id");
    let cells = single_cells(&rows);
    match &cells[0] {
        Frame::Integer(i) => assert_eq!(*i, 3),
        Frame::BulkString(b) => assert_eq!(&b[..], b"3"),
        other => panic!("copy-up lost original properties (got {other:?})"),
    }
}

#[test]
fn set_label_on_frozen_node_copies_up() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let r = run_write(&mut store, "MATCH (n:N {id:2}) SET n:Extra");
    assert_write_ok(&r, "SET label on frozen node");
    let rows = run_query(&store, "MATCH (m:Extra) RETURN m.id");
    assert_eq!(
        rows.len(),
        1,
        "label added on a frozen node must be scannable"
    );
}

#[test]
fn delete_frozen_node_hides_from_scan_and_neighbors() {
    let mut store = setup();
    let handles = seed_across_freeze(&mut store);
    let r = run_write(&mut store, "MATCH (n:N {id:0}) DELETE n");
    assert_write_ok(&r, "DELETE frozen node");

    let rows = run_query(&store, "MATCH (n:N) RETURN n.id");
    assert_eq!(rows.len(), 5, "deleted frozen node must vanish from scans");

    // Ring: node 1 touches 0 and 2 (Both). After deleting 0 → only 2.
    // GRAPH.NEIGHBORS emits 2 frames per neighbor (edge + node).
    let reply = graph_neighbors(&store, &[bs(GRAPH), bs(&handles[1].to_string())]);
    match reply {
        Frame::Array(items) => assert_eq!(
            items.len(),
            2,
            "deleted frozen node must vanish from CSR-backed traversal (got {} frames)",
            items.len()
        ),
        other => panic!("unexpected NEIGHBORS reply: {other:?}"),
    }
}

#[test]
fn deleted_frozen_node_stays_deleted_after_refreeze() {
    let mut store = setup();
    let handles = seed_across_freeze(&mut store);
    let r = run_write(&mut store, "MATCH (n:N {id:0}) DELETE n");
    assert_write_ok(&r, "DELETE frozen node");

    // Trigger a SECOND freeze: 6 new nodes in a ring + two chords (8 edges).
    let new_handles: Vec<u64> = (10..16).map(|i| add_node(&mut store, i)).collect();
    let edges: [(usize, usize); 8] = [
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 0),
        (0, 2),
        (0, 3),
    ];
    for (s, d) in edges {
        let r = add_edge(&mut store, new_handles[s], new_handles[d]);
        assert!(
            matches!(r, Frame::Integer(_)),
            "second-wave ADDEDGE failed: {r:?}"
        );
    }
    assert_eq!(
        immutable_segment_count(&store),
        2,
        "second freeze must fire"
    );

    let rows = run_query(&store, "MATCH (n:N) RETURN n.id");
    assert_eq!(
        rows.len(),
        11,
        "tombstone must survive refreeze (node 0 stays deleted; 5 old + 6 new)"
    );

    let reply = graph_neighbors(&store, &[bs(GRAPH), bs(&handles[1].to_string())]);
    match reply {
        Frame::Array(items) => assert_eq!(
            items.len(),
            2,
            "tombstone must keep hiding node 0 from traversal after refreeze"
        ),
        other => panic!("unexpected NEIGHBORS reply: {other:?}"),
    }
}

#[test]
fn merge_on_frozen_node_matches_instead_of_duplicating() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let r = run_write(&mut store, "MERGE (n:N {id:4})");
    assert_write_ok(&r, "MERGE on frozen node");
    let rows = run_query(&store, "MATCH (n:N {id:4}) RETURN n.id");
    assert_eq!(
        rows.len(),
        1,
        "MERGE must match the frozen node, not create a duplicate"
    );
}

#[test]
fn cypher_delete_emits_removenode_wal_record() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    store.drain_wal(); // discard seed records
    let r = run_write(&mut store, "MATCH (n:N {id:5}) DELETE n");
    assert_write_ok(&r, "DELETE frozen node");
    let records = store.drain_wal();
    let has_remove = records.iter().any(|rec| {
        rec.windows(b"GRAPH.REMOVENODE".len())
            .any(|w| w.eq_ignore_ascii_case(b"GRAPH.REMOVENODE"))
    });
    assert!(
        has_remove,
        "Cypher DELETE must WAL a REMOVENODE record or the delete is lost at restart ({} records)",
        records.len()
    );
}

#[test]
fn set_on_frozen_node_then_refreeze_does_not_duplicate() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store);
    let r = run_write(&mut store, "MATCH (n:N {id:3}) SET n.score = 7");
    assert_write_ok(&r, "SET on frozen node");

    // Second freeze: the live copy-up shadow gets frozen into a NEW segment
    // while the stale row remains in the old one — scans must not emit the
    // node twice.
    let new_handles: Vec<u64> = (10..16).map(|i| add_node(&mut store, i)).collect();
    let edges: [(usize, usize); 8] = [
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 0),
        (0, 2),
        (0, 3),
    ];
    for (s, d) in edges {
        let r = add_edge(&mut store, new_handles[s], new_handles[d]);
        assert!(
            matches!(r, Frame::Integer(_)),
            "second-wave ADDEDGE failed: {r:?}"
        );
    }
    assert_eq!(
        immutable_segment_count(&store),
        2,
        "second freeze must fire"
    );

    let rows = run_query(&store, "MATCH (n:N {id:3}) RETURN n.score");
    assert_eq!(
        rows.len(),
        1,
        "re-frozen shadow must not duplicate its stale row"
    );
    let cells = single_cells(&rows);
    match &cells[0] {
        Frame::Integer(i) => assert_eq!(*i, 7, "newest segment must win"),
        Frame::BulkString(b) => assert_eq!(&b[..], b"7", "newest segment must win"),
        other => panic!("SET lost across refreeze (got {other:?})"),
    }

    let rows = run_query(&store, "MATCH (n:N) RETURN n.id");
    assert_eq!(rows.len(), 12, "6 originals + 6 new, no duplicates");
}

// ---------------------------------------------------------------------------
// 7. WHERE range predicates (W2-3): index pruning + openCypher comparison
//    semantics (cross-type ordering is Null, not rank-ordered)
// ---------------------------------------------------------------------------

#[test]
fn where_range_query_correct_across_tiers_and_types() {
    let mut store = setup();
    let _ = seed_across_freeze(&mut store); // ids 0..=5 frozen
    let _ = add_node(&mut store, 6); // mutable tier
    // A node whose `id` is a STRING must never satisfy a numeric range.
    let r = graph_addnode(&mut store, &[bs(GRAPH), bs("N"), bs("id"), bs("zzz")]);
    assert!(
        matches!(r, Frame::Integer(_)),
        "string-prop ADDNODE failed: {r:?}"
    );

    let rows = run_query(&store, "MATCH (n:N) WHERE n.id > 3 RETURN n.id");
    assert_eq!(
        rows.len(),
        3,
        "ids 4,5 (frozen) + 6 (mutable); string id must NOT satisfy a numeric >"
    );

    let rows = run_query(
        &store,
        "MATCH (n:N) WHERE n.id >= 1 AND n.id < 3 RETURN n.id",
    );
    assert_eq!(rows.len(), 2, "ids 1,2");
}

// ---------------------------------------------------------------------------
// 8. Binary-safe string values (W2-4): RESP bulk strings are arbitrary bytes;
//    a non-UTF8 property must round-trip through RETURN, not degrade to "".
// ---------------------------------------------------------------------------

#[test]
fn binary_property_roundtrips_through_return() {
    let mut store = setup();
    let raw: &[u8] = &[0xff, 0xfe, b'!'];
    let r = graph_addnode(
        &mut store,
        &[bs(GRAPH), bs("N"), bs("data"), blob(raw.to_vec())],
    );
    assert!(matches!(r, Frame::Integer(_)), "ADDNODE failed: {r:?}");

    let cells = single_cells(&run_query(&store, "MATCH (n:N) RETURN n.data"));
    assert_eq!(cells.len(), 1);
    match &cells[0] {
        Frame::BulkString(b) => assert_eq!(
            b.as_ref(),
            raw,
            "non-UTF8 property bytes must survive the reply path"
        ),
        other => panic!("expected bulk string, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 9. Write-side plan cache (W2-7): repeated write shapes skip parse/compile;
//    the cached plan must resolve EACH run's literals, not replay the first.
// ---------------------------------------------------------------------------

#[test]
fn write_plans_cached_and_resolve_fresh_literals() {
    let mut store = setup();

    let r1 = graph_query_write(&mut store, &[bs(GRAPH), bs("CREATE (n:W {id: 7})")]);
    assert!(
        !matches!(r1, Frame::Error(_)),
        "first CREATE failed: {r1:?}"
    );
    let cached = store
        .get_graph(GRAPH.as_bytes())
        .expect("graph")
        .plan_cache
        .lock()
        .len();
    assert!(cached >= 1, "write plan must land in the plan cache");

    // Same normalized shape, different literal — must hit the cache AND
    // store 8, not a replay of 7.
    let r2 = graph_query_write(&mut store, &[bs(GRAPH), bs("CREATE (n:W {id: 8})")]);
    assert!(
        !matches!(r2, Frame::Error(_)),
        "second CREATE failed: {r2:?}"
    );
    assert_eq!(
        store
            .get_graph(GRAPH.as_bytes())
            .expect("graph")
            .plan_cache
            .lock()
            .len(),
        cached,
        "second literal variant must reuse the cached plan, not add one"
    );

    let cells = single_cells(&run_query(&store, "MATCH (n:W) RETURN n.id"));
    assert_eq!(cells.len(), 2, "both CREATEs must have executed");
    let hit = single_cells(&run_query(&store, "MATCH (n:W) WHERE n.id = 8 RETURN n.id"));
    assert_eq!(hit.len(), 1, "cached plan must resolve the SECOND literal");
}

#[test]
fn or_write_cache_hit_still_produces_wal_and_intents() {
    let mut store = setup();

    // First run compiles + caches; second run takes the W2-7 write-hit path.
    for id in [11, 12] {
        let q = format!("CREATE (n:W {{id: {id}}})");
        let (frame, intents, _undo) = graph_query_or_write(&mut store, &[bs(GRAPH), bs(&q)]);
        assert!(!matches!(frame, Frame::Error(_)), "CREATE {id}: {frame:?}");
        assert_eq!(
            intents.len(),
            1,
            "CREATE {id} must record a txn rollback intent (cache hit included)"
        );
        assert!(
            !store.drain_wal().is_empty(),
            "CREATE {id} must WAL its mutation (cache hit included)"
        );
    }
    let cells = single_cells(&run_query(&store, "MATCH (n:W) RETURN n.id"));
    assert_eq!(cells.len(), 2);
}

// ---------------------------------------------------------------------------
// 10. W2-8: per-query TIMEOUT + configurable default traversal timeout
// ---------------------------------------------------------------------------

/// 400-node chain, no freeze (huge threshold). The all-pairs shortestPath
/// query below runs 160k per-row BFS probes with a guard check per row —
/// several milliseconds of work at minimum (the `Instant` reads alone exceed
/// 1ms), so `TIMEOUT 1` must abort while the unlimited control completes.
fn timeout_fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let lsn = store.allocate_lsn();
    store
        .create_graph(Bytes::from_static(GRAPH.as_bytes()), 1_000_000, lsn)
        .expect("create graph");
    let handles: Vec<u64> = (0..400)
        .map(|i| {
            let args = vec![bs(GRAPH), bs("N"), bs("id"), bs(&i.to_string())];
            match graph_addnode(&mut store, &args) {
                Frame::Integer(ext) => ext as u64,
                other => panic!("ADDNODE failed: {other:?}"),
            }
        })
        .collect();
    for w in handles.windows(2) {
        let r = add_edge(&mut store, w[0], w[1]);
        assert!(matches!(r, Frame::Integer(_)), "chain edge failed: {r:?}");
    }
    store
}

const TIMEOUT_QUERY: &str = "MATCH p = shortestPath((a:N)-[*..20]->(b:N)) RETURN p";

#[test]
fn per_query_timeout_aborts_traversal() {
    let store = timeout_fixture();
    let r = graph_query(
        &store,
        &[bs(GRAPH), bs(TIMEOUT_QUERY), bs("TIMEOUT"), bs("1")],
        Some(2),
    );
    let Frame::Error(e) = r else {
        panic!("TIMEOUT 1 must abort the all-pairs traversal, got {r:?}");
    };
    let msg = String::from_utf8_lossy(&e);
    assert!(msg.contains("traversal timeout"), "unexpected error: {msg}");
}

#[test]
fn timeout_zero_is_unlimited_and_default_completes() {
    let store = timeout_fixture();
    // TIMEOUT 0 = no limit (RedisGraph parity).
    let rows = query_rows(&graph_query(
        &store,
        &[bs(GRAPH), bs(TIMEOUT_QUERY), bs("TIMEOUT"), bs("0")],
        Some(2),
    ));
    assert!(!rows.is_empty(), "chain must yield shortest paths");
    // No TIMEOUT arg: configured default (30s) — also completes.
    let rows_default = run_query(&store, TIMEOUT_QUERY);
    assert_eq!(rows.len(), rows_default.len());
}

#[test]
fn timeout_argument_rejects_garbage() {
    let store = setup();
    for bad in ["abc", "-5", "1.5", ""] {
        let r = graph_query(
            &store,
            &[
                bs(GRAPH),
                bs("MATCH (n:N) RETURN n"),
                bs("TIMEOUT"),
                bs(bad),
            ],
            Some(2),
        );
        assert!(
            matches!(r, Frame::Error(_)),
            "TIMEOUT {bad:?} must be rejected, got {r:?}"
        );
    }
    // Dangling keyword with no value.
    let r = graph_query(
        &store,
        &[bs(GRAPH), bs("MATCH (n:N) RETURN n"), bs("TIMEOUT")],
        Some(2),
    );
    assert!(
        matches!(r, Frame::Error(_)),
        "dangling TIMEOUT must be rejected, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// 11. W2-12: Cypher aggregations (count/sum/avg/min/max/collect + grouping)
// ---------------------------------------------------------------------------

/// 5 nodes labeled C: city 'a' scores {10,20,30}, city 'b' scores {5,15}.
fn agg_fixture() -> GraphStore {
    let mut store = setup();
    for (city, score) in [("a", 10), ("a", 20), ("a", 30), ("b", 5), ("b", 15)] {
        let q = format!("CREATE (n:C {{city: '{city}', score: {score}}})");
        let (frame, _, _) = graph_query_or_write(&mut store, &[bs(GRAPH), bs(&q)]);
        assert!(
            !matches!(frame, Frame::Error(_)),
            "CREATE failed: {frame:?}"
        );
    }
    store
}

/// Decode a cell that may arrive as Integer, Double, or numeric BulkString.
fn as_f64(cell: &Frame) -> f64 {
    match cell {
        Frame::Integer(i) => *i as f64,
        Frame::Double(d) => *d,
        Frame::BulkString(b) => String::from_utf8_lossy(b)
            .parse()
            .unwrap_or_else(|_| panic!("non-numeric cell {cell:?}")),
        other => panic!("non-numeric cell {other:?}"),
    }
}

#[test]
fn aggregate_count_star_and_expr() {
    let store = agg_fixture();
    for q in ["MATCH (n:C) RETURN count(*)", "MATCH (n:C) RETURN count(n)"] {
        let rows = run_query(&store, q);
        assert_eq!(rows.len(), 1, "{q}: one global group");
        let cells = single_cells(&rows);
        assert_eq!(as_f64(&cells[0]), 5.0, "{q}");
    }
}

#[test]
fn aggregate_implicit_grouping_count_and_sum() {
    let store = agg_fixture();
    let rows = run_query(
        &store,
        "MATCH (n:C) RETURN n.city, count(n), sum(n.score) ORDER BY n.city",
    );
    assert_eq!(rows.len(), 2, "two city groups");
    let decode = |r: &Frame| -> (String, f64, f64) {
        let Frame::Array(cells) = r else {
            panic!("malformed row {r:?}")
        };
        let city = match &cells[0] {
            Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("city cell {other:?}"),
        };
        (city, as_f64(&cells[1]), as_f64(&cells[2]))
    };
    assert_eq!(decode(&rows[0]), ("a".into(), 3.0, 60.0));
    assert_eq!(decode(&rows[1]), ("b".into(), 2.0, 20.0));
}

#[test]
fn aggregate_sum_avg_min_max() {
    let store = agg_fixture();
    let rows = run_query(
        &store,
        "MATCH (n:C) RETURN sum(n.score), avg(n.score), min(n.score), max(n.score)",
    );
    assert_eq!(rows.len(), 1);
    let Frame::Array(cells) = &rows[0] else {
        panic!("malformed row");
    };
    assert_eq!(as_f64(&cells[0]), 80.0, "sum");
    assert!((as_f64(&cells[1]) - 16.0).abs() < 1e-9, "avg");
    assert_eq!(as_f64(&cells[2]), 5.0, "min");
    assert_eq!(as_f64(&cells[3]), 30.0, "max");
}

#[test]
fn aggregate_collect_distinct() {
    let store = agg_fixture();
    let rows = run_query(&store, "MATCH (n:C) RETURN collect(DISTINCT n.city)");
    assert_eq!(rows.len(), 1);
    let cells = single_cells(&rows);
    let Frame::Array(items) = &cells[0] else {
        panic!("collect must return a list, got {:?}", cells[0]);
    };
    assert_eq!(items.len(), 2, "two distinct cities");
}

#[test]
fn aggregate_over_zero_rows() {
    let store = agg_fixture();
    // No grouping key → exactly one row with the empty-input aggregate.
    let rows = run_query(&store, "MATCH (n:Nope) RETURN count(n)");
    assert_eq!(rows.len(), 1, "global count over zero rows is one row");
    assert_eq!(as_f64(&single_cells(&rows)[0]), 0.0);
    // With a grouping key → zero rows.
    let rows = run_query(&store, "MATCH (n:Nope) RETURN n.city, count(n)");
    assert!(rows.is_empty(), "grouped aggregate over zero rows is empty");
}

// ---------------------------------------------------------------------------
// 12. W2-13: OPTIONAL MATCH null-padding + WITH mid-pipeline rebinding
// ---------------------------------------------------------------------------

/// 3 nodes (logical ids 1..=3, label N) with a single edge 1->2.
fn optional_fixture() -> GraphStore {
    let mut store = setup();
    let handles: Vec<u64> = (1..=3).map(|i| add_node(&mut store, i)).collect();
    let r = add_edge(&mut store, handles[0], handles[1]);
    assert!(matches!(r, Frame::Integer(_)), "ADDEDGE failed: {r:?}");
    store
}

/// Decode a two-column row as (numeric, Option<numeric>) — Null in the second
/// column maps to None.
fn row_pair(row: &Frame) -> (f64, Option<f64>) {
    let Frame::Array(cells) = row else {
        panic!("malformed row {row:?}")
    };
    let second = match &cells[1] {
        Frame::Null => None,
        other => Some(as_f64(other)),
    };
    (as_f64(&cells[0]), second)
}

#[test]
fn optional_match_null_pads_missing_expansion() {
    let store = optional_fixture();
    let rows = run_query(
        &store,
        "MATCH (n:N) OPTIONAL MATCH (n)-[:E]->(m) RETURN n.id, m.id ORDER BY n.id",
    );
    assert_eq!(rows.len(), 3, "every source row survives OPTIONAL MATCH");
    assert_eq!(row_pair(&rows[0]), (1.0, Some(2.0)));
    assert_eq!(row_pair(&rows[1]), (2.0, None));
    assert_eq!(row_pair(&rows[2]), (3.0, None));
}

#[test]
fn optional_match_binds_edge_variable_or_null() {
    let store = optional_fixture();
    // Node 3 has no out-edge: n survives, m (and r) are Null.
    let rows = run_query(
        &store,
        "MATCH (n:N {id: 3}) OPTIONAL MATCH (n)-[r:E]->(m) RETURN n.id, m.id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(row_pair(&rows[0]), (3.0, None));
}

#[test]
fn optional_match_unsupported_shapes_are_loud_errors() {
    let store = optional_fixture();
    for q in [
        // Standalone pattern: first variable not previously bound.
        "OPTIONAL MATCH (x:N)-[:E]->(y) RETURN x",
        // Multi-relationship chain (whole-pattern null semantics unimplemented).
        "MATCH (n:N) OPTIONAL MATCH (n)-[:E]->(a)-[:E]->(b) RETURN b",
        // Inline properties on the optional target.
        "MATCH (n:N) OPTIONAL MATCH (n)-[:E]->(m {id: 2}) RETURN m",
        // Labels on the already-bound first node.
        "MATCH (n:N) OPTIONAL MATCH (n:N)-[:E]->(m) RETURN m",
    ] {
        let r = graph_query(&store, &[bs(GRAPH), bs(q)], Some(2));
        assert!(
            matches!(r, Frame::Error(_)),
            "{q} must be rejected loudly, got {r:?}"
        );
    }
}

#[test]
fn with_aggregates_then_having_filter() {
    let store = agg_fixture();
    let rows = run_query(
        &store,
        "MATCH (n:C) WITH n.city AS city, sum(n.score) AS total WHERE total > 30 \
         RETURN city, total",
    );
    assert_eq!(rows.len(), 1, "only city 'a' (60) exceeds 30");
    let Frame::Array(cells) = &rows[0] else {
        panic!("malformed row {:?}", rows[0])
    };
    match &cells[0] {
        Frame::BulkString(b) => assert_eq!(&b[..], b"a"),
        other => panic!("city cell {other:?}"),
    }
    assert_eq!(as_f64(&cells[1]), 60.0);
}

#[test]
fn with_passthrough_binding_keeps_entity_access() {
    let store = agg_fixture();
    let rows = run_query(
        &store,
        "MATCH (n:C) WITH n AS m WHERE m.score > 12 RETURN m.score ORDER BY m.score",
    );
    let scores: Vec<f64> = single_cells(&rows).iter().map(as_f64).collect();
    assert_eq!(scores, vec![15.0, 20.0, 30.0]);
}

#[test]
fn with_order_by_limit_pipeline() {
    let store = agg_fixture();
    let rows = run_query(
        &store,
        "MATCH (n:C) WITH n.score AS s ORDER BY s DESC LIMIT 2 RETURN s",
    );
    let scores: Vec<f64> = single_cells(&rows).iter().map(as_f64).collect();
    assert_eq!(scores, vec![30.0, 20.0]);
}

#[test]
fn with_distinct_dedups() {
    let store = agg_fixture();
    let rows = run_query(
        &store,
        "MATCH (n:C) WITH DISTINCT n.city AS city RETURN city ORDER BY city",
    );
    assert_eq!(rows.len(), 2, "two distinct cities");
}

#[test]
fn with_star_is_rejected() {
    let store = agg_fixture();
    let r = graph_query(
        &store,
        &[bs(GRAPH), bs("MATCH (n:C) WITH * RETURN n")],
        Some(2),
    );
    assert!(
        matches!(r, Frame::Error(_)),
        "WITH * must be a loud error, got {r:?}"
    );
}
