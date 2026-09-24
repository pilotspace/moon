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
    // moon#1220: RETURN … ORDER BY … LIMIT projects only the kept rows — streamed from the
    // scan through Filter / Unwind / single-hop Expand, or over materialised rows otherwise.
    "MATCH (n:L) RETURN n.z, n.y, n.x ORDER BY n.x LIMIT 10",
    "MATCH (n:L) RETURN n.x AS a, n.z ORDER BY a DESC LIMIT 7",
    "MATCH (n:L) RETURN n.x AS a, n.z ORDER BY n.x LIMIT 7",
    "MATCH (n:L) RETURN n, n.x ORDER BY n.x DESC, n.z LIMIT 5",
    "MATCH (n:L) RETURN n.y, n.x, n.y ORDER BY n.y DESC, n.x LIMIT 9",
    "MATCH (n:L) RETURN n.x, n.z ORDER BY n.x SKIP 5 LIMIT 10",
    "MATCH (n:L) RETURN n.x, n.z ORDER BY n.x DESC LIMIT $k",
    "MATCH (n:L) RETURN n.z, n.x ORDER BY n.x LIMIT 100000",
    "MATCH (n:L) WHERE n.x > 10 RETURN n.z, n.x ORDER BY n.x DESC, n.z LIMIT 9",
    "MATCH (n:L) WHERE n.z > 100 RETURN n.z, n.y ORDER BY n.y, n.z DESC LIMIT 12",
    "MATCH (n:L) UNWIND [3, 1, 2] AS k RETURN n.z, k ORDER BY k, n.z DESC LIMIT 8",
    "MATCH (n:L)-[:R]->(m) RETURN n.z, m.y, m.x ORDER BY m.y DESC, m.x LIMIT 13",
    "MATCH (n:L)-[:R*1..2]->(m) RETURN m.z, n.z ORDER BY m.z DESC, n.z LIMIT 20",
    "MATCH (n:L) WITH n, n.x AS x WHERE x > 5 RETURN n.z, x ORDER BY x, n.z LIMIT 11",
    "MATCH (n) RETURN n.z, n.x ORDER BY n.x DESC LIMIT 6",
    "MATCH (n:Missing) RETURN n.x ORDER BY n.x LIMIT 5",
    "MATCH (n:L) RETURN DISTINCT n.y ORDER BY n.y LIMIT 2",
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

/// moon#1220: the fused RETURN + ORDER BY top-k is TAKEN where it should be (streamed from a
/// scan through row-local ops, or over materialised rows) and declined where it must be — so the
/// oracle comparison above covers the new path, not just the old one.
#[test]
fn return_order_by_limit_takes_the_fused_top_k() {
    let store = graph_store(600, 500);
    let mut params = HashMap::new();
    params.insert("k".to_owned(), Value::Int(7));
    // (query, streamed runs, materialised runs)
    let cases: &[(&str, usize, usize)] = &[
        (
            "MATCH (n:L) RETURN n.z, n.y, n.x ORDER BY n.x LIMIT 10",
            1,
            0,
        ),
        (
            "MATCH (n:L) WHERE n.x > 10 RETURN n.z, n.x ORDER BY n.x LIMIT 9",
            1,
            0,
        ),
        (
            "MATCH (n:L) WHERE n.z > 100 RETURN n.z, n.y ORDER BY n.y LIMIT 12",
            1,
            0,
        ),
        (
            "MATCH (n:L) UNWIND [3, 1, 2] AS k RETURN n.z, k ORDER BY k LIMIT 8",
            1,
            0,
        ),
        (
            "MATCH (n:L)-[:R]->(m) RETURN n.z, m.x ORDER BY m.x LIMIT 13",
            1,
            0,
        ),
        (
            "MATCH (n:L) RETURN n.x, n.z ORDER BY n.x SKIP 5 LIMIT $k",
            1,
            0,
        ),
        (
            "MATCH (n:L)-[:R*1..2]->(m) RETURN m.z, n.z ORDER BY m.z LIMIT 20",
            0,
            1,
        ),
        (
            "MATCH (n:L) WITH n, n.x AS x RETURN n.z, x ORDER BY x LIMIT 11",
            0,
            1,
        ),
        // Declined: no LIMIT, LIMIT 0, DISTINCT, aggregation, pre-projection ORDER BY.
        ("MATCH (n:L) RETURN n.x, n.z ORDER BY n.x", 0, 0),
        ("MATCH (n:L) RETURN n.x ORDER BY n.x LIMIT 0", 0, 0),
        ("MATCH (n:L) RETURN DISTINCT n.y ORDER BY n.y LIMIT 2", 0, 0),
        (
            "MATCH (n:L) RETURN n.y, count(n) AS c ORDER BY c LIMIT 2",
            0,
            0,
        ),
        ("MATCH (n:L) WITH n ORDER BY n.x LIMIT 9 RETURN n.z", 0, 0),
    ];
    for &(q, streamed, materialised) in cases {
        let before = fused_counts();
        let (got, want) = run(&store, q, &params);
        let after = fused_counts();
        assert_eq!(render(&got), render(&want), "{q}");
        assert_eq!(
            (after.0 - before.0, after.1 - before.1),
            (streamed, materialised),
            "{q}: (streamed, materialised) fused runs"
        );
        assert!(
            !want.rows.is_empty() || q.contains("LIMIT 0"),
            "{q}: empty oracle"
        );
    }
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
        // moon#1220: the fused RETURN top-k must decline (fall back) on these keys too, both
        // streamed and over materialised rows.
        "MATCH (n:L) RETURN n.z, n.x ORDER BY n.x DESC, n.z LIMIT 9",
        "MATCH (n:L) WITH n, n.z AS z RETURN z, n.x ORDER BY n.x LIMIT 14",
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
