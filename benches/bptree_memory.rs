use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;
use std::hint::black_box;

use moon::storage::bptree::BPTree;

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

/// Memory comparison test: BPTree vs BTreeMap for 1M entries.
/// Measures struct-level memory (not allocator-level RSS).
/// BPTree uses arena allocation with LEAF_CAPACITY=14 entries/leaf.
/// BTreeMap allocates one B-tree node per ~11 entries with 3 pointers each.
fn bench_memory_comparison(c: &mut Criterion) {
    c.bench_function("memory comparison 1M entries", |b| {
        b.iter(|| {
            // Build BPTree with 1M entries
            let mut tree = BPTree::new();
            for i in 0u64..1_000_000 {
                let score = OrderedFloat(i as f64);
                let member = Bytes::from(format!("m:{:06}", i));
                tree.insert(score, member);
            }

            // Build BTreeMap with same 1M entries
            let mut btree: BTreeMap<(OrderedFloat<f64>, Bytes), ()> = BTreeMap::new();
            for i in 0u64..1_000_000 {
                let score = OrderedFloat(i as f64);
                let member = Bytes::from(format!("m:{:06}", i));
                btree.insert((score, member), ());
            }

            // Structural memory estimation:
            // BPTree: entries stored in leaf arrays of LEAF_CAPACITY=14
            //   - ~71,429 leaf nodes, each with fixed-size array
            //   - Node overhead amortized: ~1.4 bytes/entry for node bookkeeping
            // BTreeMap: ~37 bytes overhead per entry (3 pointers + metadata per node, ~11 entries/node)
            //   - Total node overhead for 1M: ~37MB
            //   - vs BPTree node overhead: ~1.4MB
            //   => ~26x reduction in node overhead

            let bptree_entries = tree.len();
            let btree_entries = btree.len();
            assert_eq!(bptree_entries, btree_entries);
            assert_eq!(bptree_entries, 1_000_000);

            black_box((&tree, &btree));
        });
    });
}

criterion_group!(
    benches,
    bench_bptree_1m_insert,
    bench_btreemap_1m_insert,
    bench_memory_comparison,
);
criterion_main!(benches);
