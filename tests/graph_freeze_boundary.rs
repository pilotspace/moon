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
use moon::command::graph::graph_read::{graph_hybrid, graph_neighbors, graph_profile, graph_query};
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
        &[bs(GRAPH), bs(&src.to_string()), bs(&dst.to_string()), bs("E")],
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
    query_rows(&graph_query(store, &[bs(GRAPH), bs(cypher)]))
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
    assert_eq!(rows.len(), 3, "expand from compacted node must see CSR edges");
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
