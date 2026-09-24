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
            assert_eq!(l.next, want_next, "leaf {i}: next pointer broken");
            assert_eq!(l.prev, want_prev, "leaf {i}: prev pointer broken");
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
