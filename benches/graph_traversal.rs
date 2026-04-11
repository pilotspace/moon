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

criterion_group!(graph_traversal, bench_parallel_vs_sequential_bfs);
criterion_main!(graph_traversal);
