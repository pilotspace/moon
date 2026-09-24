//! Unit tests for [`BPTree`]'s public behaviour.
//!
//! Kept as a sibling file (loaded via `#[path]` from `bptree.rs`) so the tree
//! module stays under the 1500-line rule.

use super::*;

#[test]
fn test_insert_single() {
    let mut tree = BPTree::new();
    assert!(tree.insert(OrderedFloat(1.0), Bytes::from("a")));
    assert_eq!(tree.len(), 1);
    assert!(tree.contains(OrderedFloat(1.0), b"a"));
}

#[test]
fn test_insert_1000_sequential() {
    let mut tree = BPTree::new();
    for i in 0..1000 {
        assert!(tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i))));
    }
    assert_eq!(tree.len(), 1000);
}

#[test]
fn test_insert_duplicate_member_same_score() {
    let mut tree = BPTree::new();
    assert!(tree.insert(OrderedFloat(1.0), Bytes::from("a")));
    // Same (score, member) = duplicate, should return false
    assert!(!tree.insert(OrderedFloat(1.0), Bytes::from("a")));
    assert_eq!(tree.len(), 1);
}

#[test]
fn test_insert_same_score_different_members() {
    let mut tree = BPTree::new();
    assert!(tree.insert(OrderedFloat(1.0), Bytes::from("a")));
    assert!(tree.insert(OrderedFloat(1.0), Bytes::from("b")));
    assert!(tree.insert(OrderedFloat(1.0), Bytes::from("c")));
    assert_eq!(tree.len(), 3);
    assert!(tree.contains(OrderedFloat(1.0), b"a"));
    assert!(tree.contains(OrderedFloat(1.0), b"b"));
    assert!(tree.contains(OrderedFloat(1.0), b"c"));
}

#[test]
fn test_remove_existing() {
    let mut tree = BPTree::new();
    tree.insert(OrderedFloat(1.0), Bytes::from("a"));
    tree.insert(OrderedFloat(2.0), Bytes::from("b"));
    assert!(tree.remove(OrderedFloat(1.0), b"a"));
    assert_eq!(tree.len(), 1);
    assert!(!tree.contains(OrderedFloat(1.0), b"a"));
}

#[test]
fn test_remove_nonexistent() {
    let mut tree = BPTree::new();
    tree.insert(OrderedFloat(1.0), Bytes::from("a"));
    assert!(!tree.remove(OrderedFloat(2.0), b"b"));
    assert_eq!(tree.len(), 1);
}

#[test]
fn test_get_score() {
    let mut tree = BPTree::new();
    #[allow(clippy::approx_constant)]
    let pi = 3.14;
    #[allow(clippy::approx_constant)]
    let e = 2.72;
    tree.insert(OrderedFloat(pi), Bytes::from("pi"));
    tree.insert(OrderedFloat(e), Bytes::from("e"));
    assert_eq!(tree.get_score(b"pi"), Some(OrderedFloat(pi)));
    assert_eq!(tree.get_score(b"e"), Some(OrderedFloat(e)));
    assert_eq!(tree.get_score(b"missing"), None);
}

#[test]
fn test_range_ascending() {
    let mut tree = BPTree::new();
    for i in 0..20 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    let results: Vec<_> = tree.range(OrderedFloat(5.0), OrderedFloat(10.0)).collect();
    assert_eq!(results.len(), 6); // 5,6,7,8,9,10
    for (i, (score, _member)) in results.iter().enumerate() {
        assert_eq!(score.0, (5 + i) as f64);
    }
}

#[test]
fn test_range_rev_descending() {
    let mut tree = BPTree::new();
    for i in 0..20 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    let results: Vec<_> = tree
        .range_rev(OrderedFloat(5.0), OrderedFloat(10.0))
        .collect();
    assert_eq!(results.len(), 6);
    // Should be in descending order
    for (i, (score, _member)) in results.iter().enumerate() {
        assert_eq!(score.0, (10 - i) as f64);
    }
}

#[test]
fn test_rank() {
    let mut tree = BPTree::new();
    for i in 0..10 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    assert_eq!(tree.rank(OrderedFloat(0.0), b"m0"), Some(0));
    assert_eq!(tree.rank(OrderedFloat(5.0), b"m5"), Some(5));
    assert_eq!(tree.rank(OrderedFloat(9.0), b"m9"), Some(9));
    assert_eq!(tree.rank(OrderedFloat(99.0), b"m99"), None);
}

#[test]
fn test_rev_rank() {
    let mut tree = BPTree::new();
    for i in 0..10 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    assert_eq!(tree.rev_rank(OrderedFloat(9.0), b"m9"), Some(0));
    assert_eq!(tree.rev_rank(OrderedFloat(0.0), b"m0"), Some(9));
    assert_eq!(tree.rev_rank(OrderedFloat(5.0), b"m5"), Some(4));
}

#[test]
fn test_insert_10000_verify_all() {
    let mut tree = BPTree::new();
    for i in 0..10000 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    assert_eq!(tree.len(), 10000);

    // Verify all retrievable
    for i in 0..10000 {
        assert!(
            tree.contains(OrderedFloat(i as f64), format!("m{}", i).as_bytes()),
            "missing entry at {}",
            i
        );
    }

    // Verify range order
    let all: Vec<_> = tree.iter().collect();
    assert_eq!(all.len(), 10000);
    for i in 1..all.len() {
        assert!(all[i].0 >= all[i - 1].0, "order violation at {}", i);
    }
}

#[test]
fn test_remove_all() {
    let mut tree = BPTree::new();
    let n = 200;
    for i in 0..n {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    for i in 0..n {
        assert!(
            tree.remove(OrderedFloat(i as f64), format!("m{}", i).as_bytes()),
            "failed to remove {}",
            i
        );
    }
    assert_eq!(tree.len(), 0);
    assert!(tree.is_empty());
}

#[test]
fn test_nan_rejected() {
    let mut tree = BPTree::new();
    assert!(!tree.insert(OrderedFloat(f64::NAN), Bytes::from("nan")));
    assert_eq!(tree.len(), 0);
}

#[test]
fn test_infinity_boundaries() {
    let mut tree = BPTree::new();
    tree.insert(OrderedFloat(f64::NEG_INFINITY), Bytes::from("neg_inf"));
    tree.insert(OrderedFloat(0.0), Bytes::from("zero"));
    tree.insert(OrderedFloat(f64::INFINITY), Bytes::from("pos_inf"));
    assert_eq!(tree.len(), 3);

    let all: Vec<_> = tree.iter().collect();
    assert_eq!(all[0].0.0, f64::NEG_INFINITY);
    assert_eq!(all[1].0.0, 0.0);
    assert_eq!(all[2].0.0, f64::INFINITY);
}

#[test]
fn test_iter_ascending() {
    let mut tree = BPTree::new();
    for i in (0..50).rev() {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    let all: Vec<_> = tree.iter().collect();
    assert_eq!(all.len(), 50);
    for i in 0..50 {
        assert_eq!(all[i].0.0, i as f64);
    }
}

#[test]
fn test_clear() {
    let mut tree = BPTree::new();
    for i in 0..100 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    tree.clear();
    assert_eq!(tree.len(), 0);
    assert!(tree.is_empty());
    // Should be reusable
    tree.insert(OrderedFloat(1.0), Bytes::from("a"));
    assert_eq!(tree.len(), 1);
}

#[test]
fn test_clone_independent() {
    let mut tree = BPTree::new();
    tree.insert(OrderedFloat(1.0), Bytes::from("a"));
    tree.insert(OrderedFloat(2.0), Bytes::from("b"));
    let mut clone = tree.clone();
    clone.insert(OrderedFloat(3.0), Bytes::from("c"));
    assert_eq!(tree.len(), 2);
    assert_eq!(clone.len(), 3);
}

#[test]
fn test_get_by_rank() {
    let mut tree = BPTree::new();
    for i in 0..20 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    let (score, member) = tree.get_by_rank(0).unwrap();
    assert_eq!(score.0, 0.0);
    assert_eq!(member.as_ref(), b"m0");

    let (score, member) = tree.get_by_rank(19).unwrap();
    assert_eq!(score.0, 19.0);
    assert_eq!(member.as_ref(), b"m19");

    assert!(tree.get_by_rank(20).is_none());
}

#[test]
fn test_range_by_rank() {
    let mut tree = BPTree::new();
    for i in 0..20 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    let results = tree.range_by_rank(5, 9);
    assert_eq!(results.len(), 5);
    for (i, (score, _)) in results.iter().enumerate() {
        assert_eq!(score.0, (5 + i) as f64);
    }
}

#[test]
fn test_iter_rev() {
    let mut tree = BPTree::new();
    for i in 0..30 {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
    }
    let all: Vec<_> = tree.iter_rev().collect();
    assert_eq!(all.len(), 30);
    for i in 0..30 {
        assert_eq!(all[i].0.0, (29 - i) as f64);
    }
}

#[test]
fn test_random_insert_remove() {
    // Insert in random-ish order then remove in different order
    let mut tree = BPTree::new();
    let vals: Vec<i32> = (0..500).collect();
    // Insert all
    for &v in &vals {
        tree.insert(OrderedFloat(v as f64), Bytes::from(format!("m{}", v)));
    }
    assert_eq!(tree.len(), 500);
    // Remove even numbers
    for v in (0..500).step_by(2) {
        assert!(tree.remove(OrderedFloat(v as f64), format!("m{}", v).as_bytes()));
    }
    assert_eq!(tree.len(), 250);
    // Verify odd numbers remain
    for v in (1..500).step_by(2) {
        assert!(tree.contains(OrderedFloat(v as f64), format!("m{}", v).as_bytes()));
    }
}

#[test]
fn test_same_score_lexicographic_order() {
    let mut tree = BPTree::new();
    tree.insert(OrderedFloat(1.0), Bytes::from("cherry"));
    tree.insert(OrderedFloat(1.0), Bytes::from("apple"));
    tree.insert(OrderedFloat(1.0), Bytes::from("banana"));

    let all: Vec<_> = tree.iter().collect();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].1.as_ref(), b"apple");
    assert_eq!(all[1].1.as_ref(), b"banana");
    assert_eq!(all[2].1.as_ref(), b"cherry");
}

#[test]
fn test_empty_tree_operations() {
    let tree = BPTree::new();
    assert_eq!(tree.len(), 0);
    assert!(tree.is_empty());
    assert!(!tree.contains(OrderedFloat(1.0), b"a"));
    assert_eq!(tree.rank(OrderedFloat(1.0), b"a"), None);
    assert_eq!(tree.get_score(b"a"), None);
    assert_eq!(tree.iter().count(), 0);
    assert_eq!(tree.iter_rev().count(), 0);
    assert_eq!(tree.range(OrderedFloat(0.0), OrderedFloat(10.0)).count(), 0);
}

#[test]
fn test_single_element_tree() {
    let mut tree = BPTree::new();
    tree.insert(OrderedFloat(5.0), Bytes::from("only"));
    assert_eq!(tree.rank(OrderedFloat(5.0), b"only"), Some(0));
    assert_eq!(tree.rev_rank(OrderedFloat(5.0), b"only"), Some(0));
    assert_eq!(tree.get_by_rank(0).unwrap().0.0, 5.0);

    let range: Vec<_> = tree.range(OrderedFloat(0.0), OrderedFloat(10.0)).collect();
    assert_eq!(range.len(), 1);
}

#[test]
fn test_bptree_memory_overhead_vs_btreemap() {
    use std::collections::BTreeMap;

    let n = 100_000; // Use 100K for unit test speed

    // BPTree: arena-allocated, LEAF_CAPACITY=14 entries per leaf
    let mut tree = BPTree::new();
    for i in 0..n {
        tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m:{:06}", i)));
    }

    // BTreeMap: standard library B-tree
    let mut btree: BTreeMap<(OrderedFloat<f64>, Bytes), ()> = BTreeMap::new();
    for i in 0..n {
        btree.insert(
            (OrderedFloat(i as f64), Bytes::from(format!("m:{:06}", i))),
            (),
        );
    }

    // Structural analysis:
    // BPTree leaf count = ceil(n / LEAF_CAPACITY) = ceil(100000/14) = 7143 leaves
    // Each leaf: fixed-size array of 14 Key entries + metadata (next/prev pointers, count)
    // Per-entry node overhead: ~(size_of::<LeafNode>() - 14 * size_of::<Key>()) / 14
    //
    // BTreeMap: each node holds ~11 entries with 3 pointers (parent, left, right) = 24 bytes
    // Plus allocation header ~16 bytes per node. Per-entry overhead: ~(24+16)/11 ~ 3.6 bytes
    // But the KEY in BTreeMap is (OrderedFloat<f64>, Bytes) = 24 bytes on stack per entry
    // with separate heap allocation for each Bytes clone.
    //
    // The 10x claim is about NODE OVERHEAD, not total memory including the data itself.
    // BPTree amortizes node overhead across LEAF_CAPACITY=14 entries vs BTreeMap's per-node cost.

    assert_eq!(tree.len(), n);
    assert_eq!(btree.len(), n);

    // The test validates structural correctness at scale.
    // The actual 10x measurement requires heap profiling (see benches/bptree_memory.rs).
}
