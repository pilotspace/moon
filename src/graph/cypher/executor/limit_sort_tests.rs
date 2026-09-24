//! moon#1197 differential + cost tests for LIMIT early exit and fused ORDER BY + LIMIT.
//!
//! Oracle: `execute_profile`, which still evaluates every operator over the full row vector
//! exactly as the pre-#1197 `execute` did (full scan / projection, full stable sort with cloned
//! keys, truncate at LIMIT). Every query must return the SAME columns and rows, in the same
//! order — ties included — through `execute`.

use std::collections::HashMap;

use bytes::Bytes;
use smallvec::SmallVec;

use super::*;
use crate::graph::store::GraphStore;

/// A two-tier graph: `n_frozen` `L` nodes frozen into a CSR segment, then `n_live` more in the
/// write buffer. Properties use DISTINCT, asymmetric values with deliberate ties (x repeats every
/// 97 ids, y every 5), some nodes lack `x` (Null), a few carry a Float `x`. Each node has 1-3
/// `R` edges.
fn graph_store(n_frozen: usize, n_live: usize) -> GraphStore {
    let mut store = GraphStore::new();
    store
        .create_graph(Bytes::from_static(b"g"), 10_000_000, 0)
        .expect("create");
    let graph = store.get_graph_mut(b"g").expect("graph");
    let l = label_to_id(b"L");
    let other = label_to_id(b"Other");
    let r = label_to_id(b"R");
    let (px, py, pz) = (label_to_id(b"x"), label_to_id(b"y"), label_to_id(b"z"));
    let add = |graph: &mut crate::graph::store::NamedGraph, i: usize, lsn: u64| {
        let mut props: PropertyMap = SmallVec::new();
        if i % 11 != 3 {
            let x = if i % 13 == 0 {
                PropertyValue::Float((i % 97) as f64 + 0.5)
            } else {
                PropertyValue::Int(((i * 7) % 97) as i64)
            };
            props.push((px, x));
        }
        props.push((
            py,
            PropertyValue::String(Bytes::from(format!("s{}", i % 5))),
        ));
        props.push((pz, PropertyValue::Int(i as i64)));
        let labels: SmallVec<[u16; 4]> = if i % 17 == 0 {
            SmallVec::from_elem(other, 1)
        } else {
            SmallVec::from_elem(l, 1)
        };
        graph.write_buf.add_node(labels, props, None, lsn)
    };
    let mut keys = Vec::new();
    for i in 0..n_frozen {
        keys.push(add(graph, i, 1));
    }
    for (i, &k) in keys.iter().enumerate() {
        for j in 1..=(1 + i % 3) {
            let dst = keys[(i * 31 + j * 7) % keys.len()];
            let _ = graph.write_buf.add_edge(k, dst, r, 1.0, None, 1);
        }
    }
    if n_frozen > 0 {
        assert!(graph.freeze_and_compact(1), "freeze");
    }
    let mut live = Vec::new();
    for i in n_frozen..n_frozen + n_live {
        live.push(add(graph, i, 2));
    }
    for (i, &k) in live.iter().enumerate() {
        let dst = live[(i * 13 + 5) % live.len()];
        let _ = graph.write_buf.add_edge(k, dst, r, 1.0, None, 2);
    }
    store
}

fn run(store: &GraphStore, q: &str, params: &HashMap<String, Value>) -> (ExecResult, ExecResult) {
    let parsed = crate::graph::cypher::parse_cypher(q.as_bytes()).expect("parse");
    let plan = crate::graph::cypher::planner::compile(&parsed).expect("compile");
    let graph = store.get_graph(b"g").expect("graph");
    let ctx = ExecutionContext::default();
    let live = execute(graph, &plan, params, &ctx).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let oracle = execute_profile(graph, &plan, params, &ctx)
        .unwrap_or_else(|e| panic!("{q} (oracle): {e:?}"))
        .exec_result;
    (live, oracle)
}

fn render(r: &ExecResult) -> (Vec<String>, Vec<String>) {
    (
        r.columns.clone(),
        r.rows.iter().map(|row| format!("{row:?}")).collect(),
    )
}

const QUERIES: &[&str] = &[
    // LIMIT early exit (streamed scan prefix).
    "MATCH (n:L) RETURN n.x LIMIT 10",
    "MATCH (n:L) RETURN n.x, n.y SKIP 7 LIMIT 13",
    "MATCH (n:L) RETURN n LIMIT 1",
    "MATCH (n:L) RETURN n.z LIMIT 0",
    "MATCH (n:L) RETURN n.z SKIP 5000 LIMIT 3",
    "MATCH (n:L) WHERE n.x > 90 RETURN n.z LIMIT 5",
    "MATCH (n:L) WHERE n.x = -1 RETURN n.z LIMIT 5",
    "MATCH (n:Missing) RETURN n.x LIMIT 5",
    "MATCH (n) RETURN n.z LIMIT 25",
    "MATCH (n:L)-[:R]->(m) RETURN n.z, m.z LIMIT 12",
    "MATCH (n:L)-[e:R]->(m) WHERE m.x < 20 RETURN n.z, m.z SKIP 3 LIMIT 9",
    "MATCH (n:L)-[:R*1..3]->(m) RETURN m.z LIMIT 40",
    // moon#1221 review: row-dropping ops after a var-length Expand, *0..N, *2..N.
    "MATCH (n:L)-[:R*1..3]->(m) WHERE m.x < 30 RETURN n.z, m.z SKIP 2 LIMIT 9",
    "MATCH (n:L)-[:R*2..3]->(m) WHERE m.z > 100 RETURN m.z LIMIT 6",
    "MATCH (n:L)-[:R*0..2]->(m) RETURN m.z LIMIT 7",
    "MATCH (n:L) UNWIND [1, 2, 3] AS k RETURN n.z, k LIMIT 8",
    "MATCH (n:L) WITH n LIMIT 6 MATCH (n)-[:R]->(m) RETURN n.z, m.z",
    "MATCH (n:L) RETURN n.z LIMIT $k",
    "MATCH (n:L) RETURN n.z SKIP $s LIMIT $k",
    // Not streamable: aggregation / DISTINCT need every row.
    "MATCH (n:L) RETURN count(n) LIMIT 1",
    "MATCH (n:L) RETURN n.y, count(n) LIMIT 2",
    "MATCH (n:L) RETURN DISTINCT n.y LIMIT 3",
    // ORDER BY (+ LIMIT): ties on x, Null x, Int/Float mix, multi-key, DESC.
    "MATCH (n:L) RETURN n.x ORDER BY n.x LIMIT 10",
    "MATCH (n:L) RETURN n.x, n.z ORDER BY n.x DESC LIMIT 17",
    "MATCH (n:L) RETURN n.x, n.y, n.z ORDER BY n.y, n.x DESC SKIP 4 LIMIT 11",
    "MATCH (n:L) RETURN n.x, n.z ORDER BY n.x",
    "MATCH (n:L) RETURN n.z ORDER BY n.x LIMIT 6",
    "MATCH (n:L) WITH n ORDER BY n.x DESC LIMIT 9 RETURN n.x, n.z",
    "MATCH (n:L) WITH n, n.y AS y ORDER BY y, n.z LIMIT 5 RETURN y, n.z",
    "MATCH (n:L) RETURN n.y AS y, count(n) AS c ORDER BY c DESC, y LIMIT 3",
    "MATCH (n:L)-[:R]->(m) RETURN n.z, m.x ORDER BY m.x, n.z LIMIT 15",
    "MATCH (n:L) RETURN n.x ORDER BY n.x LIMIT 0",
];

#[test]
fn limit_and_order_by_match_full_evaluation_across_tiers() {
    let mut params = HashMap::new();
    params.insert("k".to_owned(), Value::Int(7));
    params.insert("s".to_owned(), Value::Int(3));
    let (mut compared, mut nonempty) = (0, 0);
    for (frozen, live) in [(0usize, 700usize), (600, 500), (900, 0)] {
        let store = graph_store(frozen, live);
        for q in QUERIES {
            let (got, want) = run(&store, q, &params);
            assert_eq!(
                render(&got),
                render(&want),
                "{q} (frozen={frozen} live={live})"
            );
            compared += 1;
            nonempty += usize::from(!want.rows.is_empty());
        }
    }
    assert_eq!(compared, 3 * QUERIES.len());
    // A differential that only compares empty results proves nothing (CONVENTIONS).
    assert!(
        nonempty * 10 >= compared * 7,
        "{nonempty}/{compared} non-empty"
    );
}

/// NaN / ±2^53-int-next-to-float sort keys make `compare_values` intransitive: the fused
/// ORDER BY + LIMIT must then fall back to the full stable sort, which reproduces HEAD exactly.
#[test]
fn order_by_with_nan_and_huge_ints_matches_full_evaluation() {
    let mut store = GraphStore::new();
    store
        .create_graph(Bytes::from_static(b"g"), 1_000_000, 0)
        .expect("create");
    let graph = store.get_graph_mut(b"g").expect("graph");
    let l = label_to_id(b"L");
    let (px, pz) = (label_to_id(b"x"), label_to_id(b"z"));
    for i in 0..400i64 {
        let x = match i % 6 {
            0 => PropertyValue::Float(f64::NAN),
            1 => PropertyValue::Int((1i64 << 60) + i),
            2 => PropertyValue::Float((1i64 << 60) as f64),
            3 => PropertyValue::Int(i % 9),
            _ => PropertyValue::Float((i % 9) as f64 - 0.5),
        };
        let props: PropertyMap = SmallVec::from_vec(vec![(px, x), (pz, PropertyValue::Int(i))]);
        graph
            .write_buf
            .add_node(SmallVec::from_elem(l, 1), props, None, 1);
    }
    for q in [
        "MATCH (n:L) RETURN n.x, n.z ORDER BY n.x LIMIT 10",
        "MATCH (n:L) RETURN n.x, n.z ORDER BY n.x DESC LIMIT 25",
        "MATCH (n:L) WITH n ORDER BY n.x LIMIT 12 RETURN n.z",
    ] {
        let (got, want) = run(&store, q, &HashMap::new());
        assert_eq!(render(&got), render(&want), "{q}");
    }
}

fn best_of(n: usize, mut f: impl FnMut()) -> std::time::Duration {
    (0..n)
        .map(|_| {
            let t = std::time::Instant::now();
            f();
            t.elapsed()
        })
        .min()
        .unwrap_or_default()
}

/// Red on HEAD: `MATCH (n:L) RETURN n.x LIMIT 10` scanned, cloned and projected every node
/// of the label. Now the scan stops after the first chunk.
#[test]
fn limit_stops_the_label_scan_early() {
    let store = graph_store(0, 20_000);
    let q = "MATCH (n:L) RETURN n.x LIMIT 10";
    let (got, want) = run(&store, q, &HashMap::new());
    assert_eq!(render(&got), render(&want));
    assert!(
        got.nodes_scanned < 100,
        "LIMIT 10 scanned {} nodes (label has ~19K)",
        got.nodes_scanned
    );
    assert!(want.nodes_scanned > 18_000);
    let parsed = crate::graph::cypher::parse_cypher(q.as_bytes()).expect("parse");
    let plan = crate::graph::cypher::planner::compile(&parsed).expect("compile");
    let graph = store.get_graph(b"g").expect("graph");
    let ctx = ExecutionContext::default();
    let params = HashMap::new();
    let live = best_of(5, || {
        std::hint::black_box(execute(graph, &plan, &params, &ctx).expect("exec"));
    });
    let full = best_of(5, || {
        std::hint::black_box(execute_profile(graph, &plan, &params, &ctx).expect("exec"));
    });
    assert!(
        live * 20 < full,
        "LIMIT 10 took {live:?} vs full evaluation {full:?}"
    );
}

/// Red on HEAD: a pre-projection ORDER BY evaluated both sort expressions inside EVERY
/// comparison (O(n log n) property lookups) and sorted every row before LIMIT. Now each key is
/// evaluated once per row and only the page is selected + sorted.
#[test]
fn order_by_limit_evaluates_keys_once_and_selects_the_page() {
    let store = graph_store(0, 20_000);
    let graph = store.get_graph(b"g").expect("graph");
    let ctx = ExecutionContext::default();
    let params = HashMap::new();
    let q = "MATCH (n:L) WITH n ORDER BY n.y, n.z DESC LIMIT 10 RETURN n.z";
    let parsed = crate::graph::cypher::parse_cypher(q.as_bytes()).expect("parse");
    let plan = crate::graph::cypher::planner::compile(&parsed).expect("compile");
    let (got, want) = run(&store, q, &params);
    assert_eq!(render(&got), render(&want), "{q}");
    let live = best_of(5, || {
        std::hint::black_box(execute(graph, &plan, &params, &ctx).expect("exec"));
    });
    let full = best_of(5, || {
        std::hint::black_box(execute_profile(graph, &plan, &params, &ctx).expect("exec"));
    });
    assert!(
        live.as_secs_f64() * 2.0 < full.as_secs_f64(),
        "{q}: {live:?} not clearly cheaper than the full sort {full:?}"
    );
}

/// 48 `S` sources; the first 16 reach a hub with 7,000 leaves (7,001 rows each at `*1..2`), the
/// other 32 reach a node `a` and then `b` with `t = 1`. `hubs` more sources point at the hub
/// after them, and none of hub/leaves/`a` carries `t`.
fn var_length_store(hubs: usize) -> GraphStore {
    let mut store = GraphStore::new();
    store
        .create_graph(Bytes::from_static(b"g"), 10_000_000, 0)
        .expect("create");
    let graph = store.get_graph_mut(b"g").expect("graph");
    let (s, r, pt) = (label_to_id(b"S"), label_to_id(b"R"), label_to_id(b"t"));
    let node = |graph: &mut crate::graph::store::NamedGraph, label: u16, props: PropertyMap| {
        graph
            .write_buf
            .add_node(SmallVec::from_elem(label, 1), props, None, 1)
    };
    let other = label_to_id(b"O");
    let sources: Vec<_> = (0..48 + hubs)
        .map(|_| node(graph, s, SmallVec::new()))
        .collect();
    let hub = node(graph, other, SmallVec::new());
    for _ in 0..7_000 {
        let leaf = node(graph, other, SmallVec::new());
        let _ = graph.write_buf.add_edge(hub, leaf, r, 1.0, None, 1);
    }
    for (i, &src) in sources.iter().enumerate() {
        if !(16..48).contains(&i) {
            let _ = graph.write_buf.add_edge(src, hub, r, 1.0, None, 1);
        } else {
            let a = node(graph, other, SmallVec::new());
            let mut props: PropertyMap = SmallVec::new();
            props.push((pt, PropertyValue::Int(1)));
            let b = node(graph, other, props);
            let _ = graph.write_buf.add_edge(src, a, r, 1.0, None, 1);
            let _ = graph.write_buf.add_edge(a, b, r, 1.0, None, 1);
        }
    }
    store
}

/// `execute` on `q`, returning the rows every variable-length Expand emitted on this thread.
fn var_len_rows_of(store: &GraphStore, q: &str) -> (ExecResult, usize) {
    let parsed = crate::graph::cypher::parse_cypher(q.as_bytes()).expect("parse");
    let plan = crate::graph::cypher::planner::compile(&parsed).expect("compile");
    let graph = store.get_graph(b"g").expect("graph");
    super::read::VAR_LEN_ROWS.with(|n| n.set(0));
    let got = execute(graph, &plan, &HashMap::new(), &ExecutionContext::default()).expect("exec");
    (got, super::read::VAR_LEN_ROWS.with(std::cell::Cell::get))
}

/// moon#1221 review (refs moon#1197): the variable-length Expand caps its output at 100K rows
/// across ALL its input rows. The streamed prefix used to invoke it once per scan chunk with a
/// fresh count, so a Filter after it saw rows full evaluation never produced: `execute` returned
/// `[[1]]` where GRAPH.PROFILE (full evaluation) returns `[]`. The sink now carries the count.
#[test]
fn var_length_cap_spans_streamed_chunks() {
    let store = var_length_store(0);
    let q = "MATCH (s:S)-[:R*1..2]->(b) WHERE b.t = 1 RETURN b.t LIMIT 1";
    let (got, want) = run(&store, q, &HashMap::new());
    assert!(want.rows.is_empty(), "full evaluation hits the cap first");
    assert_eq!(
        render(&got),
        render(&want),
        "{q}: streamed != full evaluation"
    );
    // Same for the other shapes the stream serves: SKIP, LIMIT 0, *0..N, *1..1, a projection.
    for q in [
        "MATCH (s:S)-[:R*1..2]->(b) WHERE b.t = 1 RETURN b.t SKIP 1 LIMIT 2",
        "MATCH (s:S)-[:R*1..2]->(b) WHERE b.t = 1 RETURN b.t LIMIT 0",
        "MATCH (s:S)-[:R*0..2]->(b) WHERE b.t = 1 RETURN b.t LIMIT 3",
        "MATCH (s:S)-[:R*1..1]->(b) RETURN b.t LIMIT 5",
        "MATCH (s:S)-[:R*2..2]->(b) RETURN b.t LIMIT 5",
        "MATCH (s:S)-[:R*1..2]->(b) RETURN b.t LIMIT 20",
    ] {
        let (got, want) = run(&store, q, &HashMap::new());
        assert_eq!(render(&got), render(&want), "{q}");
    }
}

/// The DoS bound: however many chunks the stream feeds, the variable-length Expand emits at most
/// its cap plus one row per source row after the cap (HEAD's one-shot bound) — not cap × chunks.
/// Here 64 extra hub sources after the first chunk pushed a per-chunk cap past 200K expansions.
#[test]
fn var_length_expansion_stays_within_one_cap_when_streamed() {
    let sources = 48 + 64;
    let store = var_length_store(64);
    let q = "MATCH (s:S)-[:R*1..2]->(b) WHERE b.t = 7 RETURN b.t LIMIT 1";
    let (got, want) = run(&store, q, &HashMap::new());
    assert_eq!(render(&got), render(&want));
    assert_eq!(
        got.nodes_scanned, sources as u64,
        "the filter never fills the page"
    );
    let (_, expanded) = var_len_rows_of(&store, q);
    assert!(
        (100_000..=100_000 + sources).contains(&expanded),
        "streamed var-length expansion emitted {expanded} rows (cap 100,000 + {sources} sources)"
    );
}
