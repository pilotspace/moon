//! Criterion benchmarks for graph operations.
//!
//! Validates performance targets (PERF-01 through PERF-06):
//! - 1-hop CSR neighbor lookup (degree 50): < 1us
//! - 2-hop BFS expansion: < 100us at 1K nodes
//! - Edge insertion into MemGraph: < 10us
//! - CSR freeze (64K edges): < 5ms
//! - Command-level overhead for ADDNODE, ADDEDGE, NEIGHBORS

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use smallvec::smallvec;
use std::hint::black_box;
use std::sync::Arc;

use moon::graph::csr::CsrSegment;
use moon::graph::memgraph::MemGraph;
use moon::graph::traversal::{BoundedBfs, SegmentMergeReader};
use moon::graph::types::{Direction, NodeKey, PropertyMap};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Empty property map (no allocations).
fn empty_props() -> PropertyMap {
    smallvec![]
}

/// Build a MemGraph with `n` nodes, each connected to ~`degree` random neighbors.
/// Uses a deterministic LCG for reproducible benchmarks.
fn build_memgraph(n: usize, degree: usize) -> (MemGraph, Vec<NodeKey>) {
    let edge_threshold = n * degree + 1; // prevent auto-freeze
    let mut g = MemGraph::new(edge_threshold);

    // Insert nodes.
    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let nk = g.add_node(smallvec![0], empty_props(), None, i as u64 + 1);
        nodes.push(nk);
    }

    // Insert edges: deterministic pseudo-random via LCG.
    let mut rng_state: u32 = 42;
    for i in 0..n {
        for _ in 0..degree {
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
            let target = (rng_state as usize) % n;
            if target == i {
                continue; // skip self-loops
            }
            let lsn = (i * degree + 1) as u64;
            // Ignore errors (duplicate edges, etc.) -- we just want density.
            let _ = g.add_edge(nodes[i], nodes[target], 1, 1.0, None, lsn);
        }
    }

    (g, nodes)
}

/// Build a CSR segment from a MemGraph with `n` nodes and ~`degree` edges per node.
fn build_csr(n: usize, degree: usize) -> (CsrSegment, Vec<NodeKey>) {
    let (mut g, nodes) = build_memgraph(n, degree);
    let frozen = g.freeze().expect("freeze should succeed");
    let csr = CsrSegment::from_frozen(frozen, 1_000_000).expect("CSR build should succeed");
    (csr, nodes)
}

// ---------------------------------------------------------------------------
// PERF-01: 1-hop CSR neighbor lookup (degree 50) < 1us
// ---------------------------------------------------------------------------

fn bench_csr_neighbor_1hop(c: &mut Criterion) {
    let (csr, nodes) = build_csr(1000, 50);
    // Pick a node in the middle for representative degree.
    let target_key = nodes[500];
    let row = csr.lookup_node(target_key).expect("node should exist in CSR");

    c.bench_function("graph_neighbor_1hop_csr", |b| {
        b.iter(|| {
            let neighbors = black_box(&csr).neighbors_out(black_box(row));
            black_box(neighbors.len())
        })
    });
}

// ---------------------------------------------------------------------------
// PERF-01 (MemGraph variant): 1-hop neighbor lookup via MemGraph
// ---------------------------------------------------------------------------

fn bench_memgraph_neighbor_1hop(c: &mut Criterion) {
    let (g, nodes) = build_memgraph(1000, 50);
    let target_key = nodes[500];

    c.bench_function("graph_neighbor_1hop_memgraph", |b| {
        b.iter(|| {
            let count = black_box(&g)
                .neighbors(black_box(target_key), Direction::Outgoing, u64::MAX)
                .count();
            black_box(count)
        })
    });
}

// ---------------------------------------------------------------------------
// PERF-02: 2-hop BFS expansion < 100us at 1K nodes
// ---------------------------------------------------------------------------

fn bench_bfs_2hop(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_expansion_2hop");

    for &node_count in &[1_000usize, 10_000] {
        let (g, nodes) = build_memgraph(node_count, 10);
        let seed = nodes[0];
        let csr_segments: Vec<Arc<CsrSegment>> = Vec::new();

        group.bench_with_input(
            BenchmarkId::new("bfs_memgraph", node_count),
            &node_count,
            |b, _| {
                b.iter(|| {
                    let reader = SegmentMergeReader::new(
                        Some(black_box(&g)),
                        &csr_segments,
                        Direction::Outgoing,
                        u64::MAX,
                        None,
                    );
                    let bfs = BoundedBfs::new(2);
                    let result = bfs.execute(&reader, black_box(seed));
                    black_box(result)
                })
            },
        );
    }

    // Also test 2-hop on CSR segment.
    for &node_count in &[1_000usize, 10_000] {
        let (csr, nodes) = build_csr(node_count, 10);
        let seed = nodes[0];
        let csr_segments = vec![Arc::new(csr)];

        group.bench_with_input(
            BenchmarkId::new("bfs_csr", node_count),
            &node_count,
            |b, _| {
                b.iter(|| {
                    let reader = SegmentMergeReader::new(
                        None,
                        &csr_segments,
                        Direction::Outgoing,
                        u64::MAX,
                        None,
                    );
                    let bfs = BoundedBfs::new(2);
                    let result = bfs.execute(&reader, black_box(seed));
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// PERF-04: Edge insertion into MemGraph < 10us
// ---------------------------------------------------------------------------

fn bench_edge_insert(c: &mut Criterion) {
    c.bench_function("graph_edge_insert", |b| {
        // Pre-create a MemGraph with 2 nodes, re-create per iteration batch.
        b.iter_custom(|iters| {
            let mut g = MemGraph::new(iters as usize + 10);
            // Create enough distinct nodes to avoid self-loop rejections.
            let mut nodes = Vec::with_capacity(iters as usize + 1);
            for _i in 0..=(iters as usize) {
                nodes.push(g.add_node(smallvec![0], empty_props(), None, 1));
            }

            let start = std::time::Instant::now();
            for i in 0..iters as usize {
                let _ = black_box(g.add_edge(
                    nodes[i],
                    nodes[i + 1],
                    1,
                    1.0,
                    None,
                    black_box(i as u64 + 2),
                ));
            }
            start.elapsed()
        })
    });
}

// ---------------------------------------------------------------------------
// PERF-05: CSR freeze (64K edges) < 5ms
// ---------------------------------------------------------------------------

fn bench_csr_freeze(c: &mut Criterion) {
    c.bench_function("graph_csr_freeze_64k", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                // Build a MemGraph with ~64K edges.
                let (mut g, _nodes) = build_memgraph(2000, 32);
                let start = std::time::Instant::now();
                let frozen = g.freeze().expect("freeze ok");
                let csr = CsrSegment::from_frozen(frozen, 999);
                black_box(&csr);
                total += start.elapsed();
            }
            total
        })
    });
}

// ---------------------------------------------------------------------------
// PERF-06: Command-level benchmarks (ADDNODE, ADDEDGE, NEIGHBORS)
// ---------------------------------------------------------------------------

fn bench_addnode_command(c: &mut Criterion) {
    c.bench_function("graph_addnode_command", |b| {
        let mut g = MemGraph::new(1_000_000);
        let mut lsn = 1u64;
        b.iter(|| {
            lsn += 1;
            let nk = g.add_node(
                black_box(smallvec![1, 2]),
                black_box(empty_props()),
                None,
                black_box(lsn),
            );
            black_box(nk)
        })
    });
}

fn bench_addedge_command(c: &mut Criterion) {
    // Pre-build a graph with many nodes so we can keep inserting edges.
    let mut g = MemGraph::new(10_000_000);
    let mut nodes = Vec::with_capacity(100_000);
    for i in 0..100_000u64 {
        nodes.push(g.add_node(smallvec![0], empty_props(), None, i + 1));
    }
    let mut idx = 0usize;

    c.bench_function("graph_addedge_command", |b| {
        b.iter(|| {
            let src = nodes[idx % nodes.len()];
            let dst = nodes[(idx + 1) % nodes.len()];
            idx += 2;
            let result = g.add_edge(src, dst, 1, 1.0, None, black_box(idx as u64));
            black_box(result)
        })
    });
}

fn bench_neighbors_command(c: &mut Criterion) {
    let (g, nodes) = build_memgraph(1000, 50);
    let target = nodes[500];

    c.bench_function("graph_neighbors_command", |b| {
        b.iter(|| {
            let neighbors: Vec<_> = black_box(&g)
                .neighbors(black_box(target), Direction::Both, u64::MAX)
                .collect();
            black_box(neighbors.len())
        })
    });
}

// ---------------------------------------------------------------------------
// Criterion groups and main
// ---------------------------------------------------------------------------

criterion_group!(
    graph_benchmarks,
    bench_csr_neighbor_1hop,
    bench_memgraph_neighbor_1hop,
    bench_bfs_2hop,
    bench_edge_insert,
    bench_csr_freeze,
    bench_addnode_command,
    bench_addedge_command,
    bench_neighbors_command,
);

criterion_main!(graph_benchmarks);
