//! moon#1220 (item 4) red → green through PUBLIC APIs only (compiles unchanged against the
//! wave-1 base `f32546c`, where it fails): `RETURN … ORDER BY … LIMIT k` used to project EVERY
//! row (each RETURN item evaluated, one row vector per match) before selecting the page. Now only
//! the columns ORDER BY reads are evaluated per row, the page is kept in a bounded buffer, and
//! the scan is streamed — so it must be clearly cheaper than full evaluation (`execute_profile`,
//! which still projects and sorts everything) and return identical rows.
//!
//! Timing is best-of-N, same graph, back to back. Calibration (unoptimised `cargo test` build,
//! 20K nodes, shared 4-vCPU box), full evaluation / live: the wide RETURN (4 columns, ORDER BY
//! reads 2) base 1.46x -> 2.74x; the two-column shape where every column is a sort key base
//! 1.51x -> 1.79x (there only row materialisation and the per-row vectors are saved). The bound
//! is on the wide query; the narrow one must merely not lose to full evaluation.

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

#[test]
fn return_order_by_limit_projects_only_the_page() {
    let mut store = GraphStore::new();
    store
        .create_graph(Bytes::from_static(b"g"), 10_000_000, 0)
        .expect("create");
    let g = store.get_graph_mut(b"g").expect("graph");
    for i in 0..20_000i64 {
        let mut props = SmallVec::new();
        // Distinct, asymmetric values with ties (x repeats every 1000 ids).
        props.push((id(b"x"), PropertyValue::Int((i * 7919) % 1000)));
        props.push((id(b"y"), PropertyValue::Int(i)));
        props.push((
            id(b"name"),
            PropertyValue::String(Bytes::from(format!("node-{i}"))),
        ));
        props.push((id(b"w"), PropertyValue::Float(i as f64 / 3.0)));
        g.write_buf
            .add_node(SmallVec::from_elem(id(b"L"), 1), props, None, 1);
    }
    let graph = store.get_graph(b"g").expect("graph");
    let (params, ctx) = (HashMap::new(), ExecutionContext::default());
    let mut ratios = Vec::new();
    // (query, minimum full/live ratio)
    for (q, bound) in [
        (
            "MATCH (n:L) RETURN n.x, n.y ORDER BY n.x DESC, n.y LIMIT 10",
            1.0,
        ),
        (
            "MATCH (n:L) RETURN n.name, n.w, n.y, n.x ORDER BY n.x, n.y DESC LIMIT 10",
            2.0,
        ),
    ] {
        let plan = planner::compile(&parse_cypher(q.as_bytes()).expect("parse")).expect("plan");
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
        let ratio = t_full.as_secs_f64() / t_live.as_secs_f64();
        eprintln!("{q}: {t_live:?} vs full evaluation {t_full:?} = {ratio:.2}x");
        ratios.push((q, bound, ratio, t_live, t_full));
    }
    for (q, bound, ratio, t_live, t_full) in ratios {
        assert!(
            ratio > bound,
            "{q}: {t_live:?} vs full evaluation {t_full:?} ({ratio:.2}x): still projecting every row"
        );
    }
}
