//! Criterion benchmarks comparing parallel vs sequential BFS.
//!
//! Validates that `ParallelBfs` outperforms `BoundedBfs` on large-frontier
//! graphs where frontier exceeds PARALLEL_THRESHOLD (256 nodes).

use criterion::{Criterion, criterion_group, criterion_main};
use smallvec::smallvec;
use std::hint::black_box;
use std::sync::Arc;

use moon::graph::csr::CsrStorage;
use moon::graph::memgraph::MemGraph;
use moon::graph::traversal::{BoundedBfs, ParallelBfs, SegmentMergeReader};
use moon::graph::types::{Direction, NodeKey, PropertyMap};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Empty property map (no allocations).
fn empty_props() -> PropertyMap {
    smallvec![]
}

/// Build a MemGraph with `n` nodes, each connected to ~`degree` random
/// neighbors. Uses a deterministic LCG for reproducible benchmarks.
/// Identical to graph_bench.rs builder.
fn build_memgraph(n: usize, degree: usize) -> (MemGraph, Vec<NodeKey>) {
    let edge_threshold = n * degree + 1; // prevent auto-freeze
    let mut g = MemGraph::new(edge_threshold);

    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let nk = g.add_node(smallvec![0], empty_props(), None, i as u64 + 1);
        nodes.push(nk);
    }

    // Deterministic pseudo-random via LCG.
    let mut rng_state: u32 = 42;
    for i in 0..n {
        for _ in 0..degree {
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let target = (rng_state as usize) % n;
            if target == i {
                continue;
            }
            let lsn = (i * degree + 1) as u64;
            let _ = g.add_edge(nodes[i], nodes[target], 1, 1.0, None, lsn);
        }
    }

    (g, nodes)
}

// ---------------------------------------------------------------------------
// Parallel vs Sequential BFS benchmark
// ---------------------------------------------------------------------------

fn bench_parallel_vs_sequential_bfs(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_bfs");

    // 10K nodes, degree 50 -> ~500K edges, frontier at depth 1 has ~50 nodes
    // from node 0, depth 2 grows beyond 256 triggering parallel path.
    let (g, nodes) = build_memgraph(10_000, 50);
    let seed = nodes[500]; // middle node for representative connectivity
    let csr_segments: Vec<Arc<CsrStorage>> = Vec::new();

    // Pre-check: verify frontier is large enough for meaningful comparison.
    {
        let reader = SegmentMergeReader::new(
            Some(&g),
            &csr_segments,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );
        let seq = BoundedBfs::new(3).execute(&reader, seed).expect("ok");
        eprintln!(
            "BFS precheck: 10K nodes, degree 50, depth 3 -> {} nodes visited",
            seq.visited.len()
        );
    }

    group.bench_function("sequential_bfs_10k_depth3", |b| {
        b.iter(|| {
            let reader = SegmentMergeReader::new(
                Some(&g),
                &csr_segments,
                Direction::Outgoing,
                u64::MAX - 1,
                None,
            );
            let bfs = BoundedBfs::new(3);
            let result = bfs.execute(&reader, black_box(seed));
            black_box(&result);
            result
        })
    });

    group.bench_function("parallel_bfs_10k_depth3", |b| {
        b.iter(|| {
            let reader = SegmentMergeReader::new(
                Some(&g),
                &csr_segments,
                Direction::Outgoing,
                u64::MAX - 1,
                None,
            );
            let bfs = ParallelBfs::new(3);
            let result = bfs.execute(&reader, black_box(seed));
            black_box(&result);
            result
        })
    });

    // Verify correctness: both produce identical result sets.
    {
        let reader = SegmentMergeReader::new(
            Some(&g),
            &csr_segments,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );
        let seq = BoundedBfs::new(3).execute(&reader, seed).expect("ok");
        let par = ParallelBfs::new(3).execute(&reader, seed).expect("ok");

        use slotmap::Key;
        let mut seq_keys: Vec<u64> = seq.visited.iter().map(|v| v.0.data().as_ffi()).collect();
        let mut par_keys: Vec<u64> = par.visited.iter().map(|v| v.0.data().as_ffi()).collect();
        seq_keys.sort();
        par_keys.sort();
        assert_eq!(
            seq_keys, par_keys,
            "parallel and sequential BFS must produce identical results"
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Frozen-tier BFS benchmarks (W2-10)
//
// The CSR row-space fast path (`row_bfs`) only engages on a fully-frozen
// graph — the mutable-only fixtures above never exercise it. These fixtures
// freeze the SAME 10K/50 graph so the three production shapes are directly
// comparable:
//   frozen_single — one CSR segment: full row-BFS (parallel levels + Beamer)
//   frozen_multi2 — the graph frozen twice (identical clone segments, the
//                   W2-2 copy-up aftermath shape): W2-6 multi-segment row
//                   path with worst-case key-level boundary sync (every node
//                   resident in both segments)
//   mixed_tier    — same segment + a non-empty mutable tail: the gate
//                   declines and BFS falls back to the SegmentMergeReader
//                   path (what row-BFS saves)
// ---------------------------------------------------------------------------

fn bench_frozen_tier_bfs(c: &mut Criterion) {
    use moon::graph::csr::CsrSegment;
    use slotmap::Key;

    let mut group = c.benchmark_group("frozen_bfs");

    const N: usize = 10_000;
    const DEGREE: usize = 50;

    // Freeze the standard fixture once → single segment.
    let (mut g, nodes) = build_memgraph(N, DEGREE);
    let seed = nodes[500];
    let seg_old = Arc::new(CsrStorage::from(
        CsrSegment::from_frozen(g.freeze().expect("freeze"), 3).expect("csr"),
    ));

    // Re-materialize every node at its ORIGINAL key and re-add the same LCG
    // edges, then freeze again → an identical clone segment (the shape a
    // W2-2 copy-up + re-freeze leaves behind). Every node is resident in
    // BOTH segments — worst-case boundary sync for the multi-segment path.
    g.thaw();
    for &nk in &nodes {
        g.add_node_with_id(nk.data().as_ffi(), smallvec![0], empty_props(), None, 4);
    }
    let mut rng_state: u32 = 42;
    for i in 0..N {
        for _ in 0..DEGREE {
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let target = (rng_state as usize) % N;
            if target == i {
                continue;
            }
            let _ = g.add_edge(nodes[i], nodes[target], 1, 1.0, None, 5);
        }
    }
    let seg_new = Arc::new(CsrStorage::from(
        CsrSegment::from_frozen(g.freeze().expect("freeze"), 6).expect("csr"),
    ));

    let single: Vec<Arc<CsrStorage>> = vec![seg_old.clone()];
    let multi: Vec<Arc<CsrStorage>> = vec![seg_new, seg_old.clone()];

    // Mixed tier: one live node in the write buffer flips the gate off and
    // forces the reader fallback over the same frozen data.
    let mut tail = MemGraph::new(usize::MAX >> 1);
    let _ = tail.add_node(smallvec![0], empty_props(), None, 7);

    let run = |mg: Option<&MemGraph>, segs: &[Arc<CsrStorage>]| {
        let reader = SegmentMergeReader::new(mg, segs, Direction::Outgoing, u64::MAX - 1, None);
        BoundedBfs::new(3).execute(&reader, seed)
    };

    // Correctness: all three shapes must visit the same node set (the clone
    // segment adds no new reachability).
    {
        let key_set = |r: &moon::graph::traversal::BfsResult| {
            let mut v: Vec<u64> = r.visited.iter().map(|e| e.0.data().as_ffi()).collect();
            v.sort_unstable();
            v
        };
        let s = run(None, &single).expect("single ok");
        let m = run(None, &multi).expect("multi ok");
        let x = run(Some(&tail), &single).expect("mixed ok");
        assert_eq!(key_set(&s), key_set(&m), "multi must match single");
        assert_eq!(key_set(&s), key_set(&x), "reader fallback must match");
        eprintln!(
            "frozen BFS precheck: 10K nodes, degree 50, depth 3 -> {} nodes visited",
            s.visited.len()
        );
    }

    group.bench_function("row_bfs_frozen_single_10k_depth3", |b| {
        b.iter(|| black_box(run(None, black_box(&single))))
    });

    group.bench_function("row_bfs_frozen_multi2_10k_depth3", |b| {
        b.iter(|| black_box(run(None, black_box(&multi))))
    });

    group.bench_function("reader_bfs_mixed_tier_10k_depth3", |b| {
        b.iter(|| black_box(run(Some(black_box(&tail)), black_box(&single))))
    });

    // Apples-to-apples W2-6 baseline: the reader path over the SAME two
    // segments (multi2 doubles the edge probes vs `single`, so only this
    // pairing isolates the multi-segment row path's win).
    group.bench_function("reader_bfs_frozen_multi2_10k_depth3", |b| {
        b.iter(|| black_box(run(Some(black_box(&tail)), black_box(&multi))))
    });

    group.finish();
}

criterion_group!(
    graph_traversal,
    bench_parallel_vs_sequential_bfs,
    bench_frozen_tier_bfs
);
criterion_main!(graph_traversal);
