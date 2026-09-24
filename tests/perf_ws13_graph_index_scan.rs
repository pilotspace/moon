//! moon#1220 (item 5) red → green through PUBLIC APIs only (compiles unchanged against the
//! wave-1 base `f32546c`, where it fails): a LIMIT-bounded range `IndexScan`
//! (`MATCH (n:L) WHERE n.v >= $x RETURN … LIMIT k`) collected EVERY matching key — the node
//! lookup, label, visibility and property check of each candidate, on both tiers — before
//! streaming the first chunk. Now the index scan is a visitor the LIMIT stops, like the label
//! scan (moon#1197), so its cost no longer follows the number of matches. Checked against
//! `execute_profile` (full evaluation) for identical rows.
//!
//! Timing is best-of-N, same graph, back to back. Calibration (unoptimised `cargo test` build,
//! 15K frozen + 15K live nodes, shared 4-vCPU box), full evaluation / live: before 3.3x and
//! 3.7x (47.8 ms / 6.9 ms), streamed 286.6x and 70.9x (0.54 ms / 0.36 ms); bound 15x.

#![cfg(feature = "graph")]

use std::collections::HashMap;

use bytes::Bytes;
use moon::graph::cypher::executor::{ExecutionContext, Value, execute, execute_profile};
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
fn limited_range_index_scan_stops_early_on_both_tiers() {
    let mut store = GraphStore::new();
    store
        .create_graph(Bytes::from_static(b"g"), 10_000_000, 0)
        .expect("create");
    let g = store.get_graph_mut(b"g").expect("graph");
    let add = |g: &mut moon::graph::store::NamedGraph, i: i64, lsn: u64| {
        let mut props = SmallVec::new();
        // Distinct, asymmetric: v has ties (every 7th value repeats), w is unique.
        props.push((id(b"v"), PropertyValue::Int(i - i % 7)));
        props.push((id(b"w"), PropertyValue::Int(i * 3 + 1)));
        g.write_buf
            .add_node(SmallVec::from_elem(id(b"L"), 1), props, None, lsn);
    };
    for i in 0..15_000i64 {
        add(g, i, 1);
    }
    assert!(g.freeze_and_compact(1), "freeze the first tier");
    for i in 15_000..30_000i64 {
        add(g, i, 2);
    }
    let graph = store.get_graph(b"g").expect("graph");
    let ctx = ExecutionContext::default();
    let mut params = HashMap::new();
    params.insert("x".to_owned(), Value::Int(700));
    let mut ratios = Vec::new();
    for q in [
        "MATCH (n:L) WHERE n.v >= $x RETURN n.v, n.w LIMIT 10",
        "MATCH (n:L) WHERE n.v > 20000 RETURN n.w SKIP 3 LIMIT 5",
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
        assert!(!live.rows.is_empty(), "{q}");
        let t_live = best_of(5, || {
            std::hint::black_box(execute(graph, &plan, &params, &ctx).expect("exec"));
        });
        let t_full = best_of(5, || {
            std::hint::black_box(execute_profile(graph, &plan, &params, &ctx).expect("exec"));
        });
        let ratio = t_full.as_secs_f64() / t_live.as_secs_f64();
        eprintln!("{q}: {t_live:?} vs full evaluation {t_full:?} = {ratio:.1}x");
        ratios.push((q, ratio, t_live, t_full));
    }
    for (q, ratio, t_live, t_full) in ratios {
        assert!(
            ratio > 15.0,
            "{q}: {t_live:?} vs full evaluation {t_full:?} ({ratio:.1}x): the index scan still \
             resolves every match before the LIMIT"
        );
    }
}
