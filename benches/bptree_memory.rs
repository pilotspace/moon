use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;
use std::hint::black_box;

use moon::storage::bptree::{BPTree, INTERNAL_NODE_BYTES, NODE_BYTES};

/// Benchmark: Insert 1M (score, member) pairs into BPTree.
/// Measures throughput and provides baseline for memory comparison.
fn bench_bptree_1m_insert(c: &mut Criterion) {
    c.bench_function("BPTree 1M sorted set entries", |b| {
        b.iter(|| {
            let mut tree = BPTree::new();
            for i in 0u64..1_000_000 {
                let score = OrderedFloat(i as f64);
                let member = Bytes::from(format!("member:{:08}", i));
                tree.insert(score, member);
            }
            assert_eq!(tree.len(), 1_000_000);
            black_box(&tree);
            tree
        });
    });
}

/// Benchmark: Insert 1M (score, member) pairs into BTreeMap (old implementation).
fn bench_btreemap_1m_insert(c: &mut Criterion) {
    c.bench_function("BTreeMap 1M sorted set entries", |b| {
        b.iter(|| {
            let mut map: BTreeMap<(OrderedFloat<f64>, Bytes), ()> = BTreeMap::new();
            for i in 0u64..1_000_000 {
                let score = OrderedFloat(i as f64);
                let member = Bytes::from(format!("member:{:08}", i));
                map.insert((score, member), ());
            }
            assert_eq!(map.len(), 1_000_000);
            black_box(&map);
            map
        });
    });
}

/// Arena footprint and leaf fill for 1M members (moon#1189).
///
/// Prints, once, for rising scores (timestamps / counters — the shape that
/// used to leave every leaf at 7/14) and random scores:
///   fill            = len / leaf_count
///   floor B/entry   = node_capacity() * NODE_BYTES / len
///   billed B/entry  = memory_bytes() / len  (what `used_memory` is charged)
/// then times the build. Measured before/after in the WS2 SUMMARY.
fn bench_arena_fill(c: &mut Criterion) {
    fn build(pattern: &str) -> BPTree {
        let mut tree = BPTree::new();
        let mut state = 42u64;
        for i in 0u64..1_000_000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let score = if pattern == "rising" {
                i as f64
            } else {
                (state >> 11) as f64
            };
            tree.insert(OrderedFloat(score), Bytes::from(format!("member:{i:08}")));
        }
        tree
    }
    for pattern in ["rising", "random"] {
        let tree = build(pattern);
        let len = tree.len() as f64;
        eprintln!(
            "bptree_memory {pattern}: len={} fill={:.2} floor={:.1} B/entry billed={:.1} B/entry \
             (NODE_BYTES={}, INTERNAL_NODE_BYTES={})",
            tree.len(),
            len / tree.leaf_count() as f64,
            (tree.node_capacity() * NODE_BYTES) as f64 / len,
            tree.memory_bytes() as f64 / len,
            NODE_BYTES,
            INTERNAL_NODE_BYTES,
        );
        c.bench_function(&format!("BPTree 1M {pattern} build"), |b| {
            b.iter(|| black_box(build(black_box(pattern))));
        });
    }
}

/// Memory comparison test: BPTree vs BTreeMap for 1M entries.
/// Measures struct-level memory (not allocator-level RSS).
fn bench_memory_comparison(c: &mut Criterion) {
    c.bench_function("memory comparison 1M entries", |b| {
        b.iter(|| {
            let mut tree = BPTree::new();
            for i in 0u64..1_000_000 {
                let score = OrderedFloat(i as f64);
                let member = Bytes::from(format!("m:{:06}", i));
                tree.insert(score, member);
            }
            let mut btree: BTreeMap<(OrderedFloat<f64>, Bytes), ()> = BTreeMap::new();
            for i in 0u64..1_000_000 {
                let score = OrderedFloat(i as f64);
                let member = Bytes::from(format!("m:{:06}", i));
                btree.insert((score, member), ());
            }
            assert_eq!(tree.len(), btree.len());
            black_box((&tree, &btree));
        });
    });
}

criterion_group!(
    benches,
    bench_bptree_1m_insert,
    bench_btreemap_1m_insert,
    bench_arena_fill,
    bench_memory_comparison,
);
criterion_main!(benches);
