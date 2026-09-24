//! moon#1197 red → green through PUBLIC APIs only (compiles unchanged against HEAD `935c555`,
//! where it fails): `MATCH (n:L) RETURN n.x LIMIT 10` must stop the label scan early, and a
//! pre-projection `ORDER BY … LIMIT` must not re-evaluate its keys inside every comparison. Both
//! are checked against `execute_profile` (full per-operator evaluation) for identical rows.

#![cfg(feature = "graph")]

use std::collections::HashMap;

use bytes::Bytes;
use moon::graph::cypher::executor::{ExecutionContext, execute, execute_profile};
use moon::graph::cypher::{parse_cypher, planner};
use moon::graph::store::GraphStore;
use moon::graph::types::PropertyValue;
use smallvec::SmallVec;

/// FNV-1a/16 — the label/property id hash GRAPH.ADDNODE uses.
fn id(name: &[u8]) -> u16 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in name {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    (h & 0xFFFF) as u16
}

fn store(n: i64) -> GraphStore {
    let mut store = GraphStore::new();
    store
        .create_graph(Bytes::from_static(b"g"), 10_000_000, 0)
        .expect("create");
    let g = store.get_graph_mut(b"g").expect("graph");
    for i in 0..n {
        let mut props = SmallVec::new();
        props.push((id(b"x"), PropertyValue::Int((i * 7919) % 1000)));
        props.push((id(b"y"), PropertyValue::Int(i)));
        g.write_buf
            .add_node(SmallVec::from_elem(id(b"L"), 1), props, None, 1);
    }
    store
}

fn check(store: &GraphStore, q: &str, min_speedup: f64) {
    let plan = planner::compile(&parse_cypher(q.as_bytes()).expect("parse")).expect("plan");
    let graph = store.get_graph(b"g").expect("graph");
    let (params, ctx) = (HashMap::new(), ExecutionContext::default());
    let live = execute(graph, &plan, &params, &ctx).expect("exec");
    let full = execute_profile(graph, &plan, &params, &ctx)
        .expect("exec")
        .exec_result;
    assert_eq!(live.columns, full.columns, "{q}");
    assert_eq!(
        format!("{:?}", live.rows),
        format!("{:?}", full.rows),
        "{q}: rows"
    );
    assert_eq!(live.rows.len(), 10, "{q}");
    let t_live = best_of(5, || {
        std::hint::black_box(execute(graph, &plan, &params, &ctx).expect("exec"));
    });
    let t_full = best_of(5, || {
        std::hint::black_box(execute_profile(graph, &plan, &params, &ctx).expect("exec"));
    });
    assert!(
        t_live.as_secs_f64() * min_speedup < t_full.as_secs_f64(),
        "{q}: {t_live:?} vs full evaluation {t_full:?}"
    );
}

/// moon#1197.
#[test]
fn limit_and_order_by_limit_do_not_materialize_the_label() {
    let store = store(20_000);
    check(&store, "MATCH (n:L) RETURN n.x LIMIT 10", 20.0);
    check(
        &store,
        "MATCH (n:L) WHERE n.x > 500 RETURN n.y LIMIT 10",
        1.5,
    );
    check(
        &store,
        "MATCH (n:L) WITH n ORDER BY n.x, n.y DESC LIMIT 10 RETURN n.y",
        2.0,
    );
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
