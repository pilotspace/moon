//! Structural invariant checker for [`BPTree`], plus the randomized
//! insert/remove stress that drives it (test-only).
//!
//! Kept as a sibling file (loaded via `#[path]` from `bptree.rs`) so the
//! tree module stays under the 1500-line rule; as a child module it can read
//! the arena directly.
//!
//! The checker exists because two structural bugs lived in this tree for its
//! whole life, invisible to every test that only inserted in ascending order
//! (the only order whose splits never take the broken path):
//!
//! * `split_internal_and_insert` dropped the separator it displaced and the
//!   right-most child pointer whenever the split child was not the last one —
//!   i.e. on any non-append insert once a node had 17 children. The dropped
//!   subtree stayed in the leaf chain (so iteration still saw it) but was
//!   unreachable by descent: on HEAD `935c555`, 300 random-score `ZADD`s left
//!   69 members for which `ZRANK` answered nil, and `ZREM` of half of them
//!   left `ZRANGE 0 -1` returning 172 entries for a 150-member zset.
//! * `borrow_from_right_internal` moved a child without its subtree count, so
//!   every ancestor count (the order statistics `ZRANK`/`ZRANGE` by rank
//!   stand on) went stale after that rebalance.

use super::*;

impl BPTree {
    /// Panic with a description of the first violated invariant.
    ///
    /// O(N). Checks: uniform leaf depth; per-internal-node separator
    /// ordering and bounds (`children[i] < keys[i] <= children[i + 1]`);
    /// `counts[i]` equal to the real size of `children[i]`'s subtree; strictly
    /// increasing leaf entries; the leaf chain (both directions, head and
    /// tail) equal to the in-order leaf sequence; no empty non-root node; and
    /// `len` equal to the number of entries reachable by descent.
    pub(crate) fn check_invariants(&self) {
        let mut leaves = Vec::new();
        let total = self.check_node(self.root, 1, None, None, &mut leaves);
        assert_eq!(
            total, self.len,
            "len {} != reachable entries {}",
            self.len, total
        );
        // Leaf chain vs in-order leaf sequence.
        assert_eq!(
            self.leaf_head, leaves[0],
            "leaf_head is not the left-most leaf"
        );
        assert_eq!(
            self.leaf_tail,
            *leaves.last().expect("a tree always has a leaf"),
            "leaf_tail is not the right-most leaf"
        );
        for (i, &id) in leaves.iter().enumerate() {
            let l = self.leaf(id);
            let want_next = leaves.get(i + 1).copied();
            let want_prev = if i == 0 { None } else { Some(leaves[i - 1]) };
            assert_eq!(l.next(), want_next, "leaf {i}: next pointer broken");
            assert_eq!(l.prev(), want_prev, "leaf {i}: prev pointer broken");
        }
    }

    fn check_node(
        &self,
        id: NodeId,
        depth: usize,
        lo: Option<&Key>,
        hi: Option<&Key>,
        leaves: &mut Vec<NodeId>,
    ) -> usize {
        let is_root = id == self.root;
        if depth == self.height {
            let l = self.leaf(id);
            let n = l.entry_count();
            assert!(is_root || n > 0, "empty non-root leaf");
            let live = &l.entries[..n];
            for w in live.windows(2) {
                assert!(w[0] < w[1], "leaf entries not strictly increasing");
            }
            if let (Some(lo), Some(first)) = (lo, live.first()) {
                assert!(first >= lo, "leaf entry below its separator bound");
            }
            if let (Some(hi), Some(last)) = (hi, live.last()) {
                assert!(last < hi, "leaf entry at/above its separator bound");
            }
            leaves.push(id);
            return n;
        }
        let node = self.internal(id);
        let n = node.key_count();
        assert!(n > 0, "internal node with no keys (depth {depth})");
        for w in node.keys[..n].windows(2) {
            assert!(w[0] < w[1], "separators not strictly increasing");
        }
        if let Some(lo) = lo {
            assert!(&node.keys[0] >= lo, "separator below parent bound");
        }
        if let Some(hi) = hi {
            assert!(&node.keys[n - 1] < hi, "separator at/above parent bound");
        }
        let mut sum = 0usize;
        for i in 0..=n {
            let child_lo = if i == 0 { lo } else { Some(&node.keys[i - 1]) };
            let child_hi = if i == n { hi } else { Some(&node.keys[i]) };
            let c = node.children[i];
            assert_ne!(c, NIL, "NIL child {i} of {n}+1");
            let size = self.check_node(c, depth + 1, child_lo, child_hi, leaves);
            assert_eq!(
                node.counts[i] as usize, size,
                "counts[{i}] = {} but the subtree holds {size} (depth {depth})",
                node.counts[i]
            );
            sum += size;
        }
        sum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Pattern {
        Random,
        Ascending,
        Descending,
        /// Ascending scores with a random member — the timestamp shape.
        Rising,
    }

    fn key_for(p: Pattern, i: u64, rng: &mut Rng) -> (OrderedFloat<f64>, Bytes) {
        match p {
            Pattern::Random => (
                OrderedFloat((rng.next() % 1000) as f64),
                Bytes::from(format!("m{}", rng.next() % 100_000)),
            ),
            Pattern::Ascending => (OrderedFloat(i as f64), Bytes::from(format!("m{i:08}"))),
            Pattern::Descending => (
                OrderedFloat(-(i as f64)),
                Bytes::from(format!("m{:08}", u64::MAX - i)),
            ),
            Pattern::Rising => (
                OrderedFloat(i as f64),
                Bytes::from(format!("m{}", rng.next() % 100_000)),
            ),
        }
    }

    /// Every operation, in every insertion pattern, against a `BTreeSet`
    /// model — with the full structural check along the way. RED on HEAD
    /// `935c555` (the random pattern breaks `counts`/separators within the
    /// first few hundred inserts).
    #[test]
    fn random_insert_remove_keeps_every_invariant() {
        for (seed, pattern) in [
            (1u64, Pattern::Random),
            (2, Pattern::Ascending),
            (3, Pattern::Descending),
            (4, Pattern::Rising),
            (5, Pattern::Random),
        ] {
            let mut rng = Rng(seed);
            let mut tree = BPTree::new();
            let mut model: BTreeSet<(OrderedFloat<f64>, Bytes)> = BTreeSet::new();
            let n = 6000u64;
            for i in 0..n {
                let (s, m) = key_for(pattern, i, &mut rng);
                assert_eq!(
                    tree.insert(s, m.clone()),
                    model.insert((s, m)),
                    "{pattern:?} insert {i}"
                );
                if i % 500 == 0 {
                    tree.check_invariants();
                }
            }
            tree.check_invariants();
            // Remove in random order, re-inserting some to exercise
            // borrow/merge interleaved with splits.
            let mut all: Vec<_> = model.iter().cloned().collect();
            let mut k = 0usize;
            while !all.is_empty() {
                let idx = (rng.next() % all.len() as u64) as usize;
                let (s, m) = all.swap_remove(idx);
                assert!(
                    tree.remove(s, &m),
                    "{pattern:?}: remove of a present key failed"
                );
                model.remove(&(s, m.clone()));
                if k % 7 == 0 {
                    let (s2, m2) = key_for(Pattern::Random, k as u64, &mut rng);
                    if tree.insert(s2, m2.clone()) {
                        model.insert((s2, m2.clone()));
                        all.push((s2, m2));
                    }
                }
                if k % 400 == 0 {
                    tree.check_invariants();
                    let got: Vec<_> = tree.iter().map(|(s, m)| (s, m.clone())).collect();
                    let want: Vec<_> = model.iter().cloned().collect();
                    assert_eq!(got, want, "{pattern:?}: contents diverged at step {k}");
                    for (r, (s, m)) in model.iter().enumerate().step_by(97) {
                        assert_eq!(tree.rank(*s, m), Some(r), "{pattern:?}: rank");
                        assert_eq!(
                            tree.get_by_rank(r).map(|(s, m)| (s, m.clone())),
                            Some((*s, m.clone()))
                        );
                    }
                }
                k += 1;
            }
            tree.check_invariants();
            assert_eq!(tree.len(), 0);
        }
    }
}

/// moon#1189 — arena layout, fill and growth.
#[cfg(test)]
mod memory_1189 {
    use super::*;

    fn build(n: u64, pattern: &str) -> BPTree {
        let mut tree = BPTree::new();
        let mut state = 42u64;
        for i in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let s = match pattern {
                "rising" => i as f64,
                "falling" => -(i as f64),
                _ => (state >> 11) as f64,
            };
            tree.insert(OrderedFloat(s), Bytes::from(format!("member:{i:08}")));
        }
        tree
    }

    /// Rising (timestamp / counter) scores used to split every leaf 7/7 and
    /// leave the arena half empty; the lopsided split at the chain's ends
    /// fills them. Falling scores mirror it at the head. Random inserts keep
    /// the classic ~70% fill. RED on HEAD `935c555` (fill 7.00 rising).
    #[test]
    fn end_of_chain_inserts_fill_leaves_completely() {
        for (pattern, min_fill) in [("rising", 13.9), ("falling", 13.9), ("random", 9.0)] {
            let tree = build(50_000, pattern);
            tree.check_invariants();
            let fill = tree.len() as f64 / tree.leaf_count() as f64;
            assert!(
                fill >= min_fill,
                "{pattern}: leaf fill {fill:.2} < {min_fill}"
            );
        }
    }

    /// A leaf slot no longer pays for the (larger) internal-node variant,
    /// and `node_capacity() * NODE_BYTES` stays a floor under the exact
    /// `memory_bytes()` the ledger bills (moon#788's accounting contract).
    #[test]
    fn leaf_slots_are_leaf_sized_and_accounting_stays_exact() {
        const {
            assert!(NODE_BYTES < INTERNAL_NODE_BYTES);
            assert!(
                NODE_BYTES <= LEAF_CAPACITY * std::mem::size_of::<Key>() + 16,
                "leaf padded past its payload"
            );
        }
        for pattern in ["rising", "random"] {
            let tree = build(40_000, pattern);
            let floor = tree.node_capacity() * NODE_BYTES;
            let exact = tree.memory_bytes();
            assert!(floor <= exact, "{pattern}: floor {floor} > billed {exact}");
            let live = tree.leaves.len() * NODE_BYTES + tree.internals.len() * INTERNAL_NODE_BYTES;
            // Chunking bounds the unused tail at one chunk per arena (plus
            // size-class rounding), where a doubling Vec wasted up to half.
            let slack = exact - live;
            let bound = (1usize << LEAF_CHUNK_SHIFT) * NODE_BYTES
                + (1usize << INTERNAL_CHUNK_SHIFT) * INTERNAL_NODE_BYTES
                + exact / 8;
            assert!(slack <= bound, "{pattern}: {slack} B of slack > {bound}");
        }
    }

    /// Growth never moves a leaf that already sits in a full chunk: there is
    /// no whole-arena `realloc` on the shard thread as the zset grows.
    #[test]
    fn growth_does_not_move_full_chunks() {
        let mut tree = build(20_000, "random");
        let probe = std::ptr::from_ref(tree.leaves.get(300));
        for i in 0..20_000u64 {
            tree.insert(
                OrderedFloat(1e15 + i as f64),
                Bytes::from(format!("late:{i}")),
            );
        }
        assert_eq!(probe, std::ptr::from_ref(tree.leaves.get(300)));
        tree.check_invariants();
    }
}
