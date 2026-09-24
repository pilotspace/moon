use bytes::Bytes;
use ordered_float::OrderedFloat;

use super::bptree::{BPTree, NodeId};

// ---------------------------------------------------------------------------
// Lookups
// ---------------------------------------------------------------------------

impl BPTree {
    /// Whether `(score, member)` is stored. The probe is borrowed — no
    /// `Bytes::copy_from_slice` per lookup (moon#1189).
    pub fn contains(&self, score: OrderedFloat<f64>, member: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }
        let leaf_id = self.find_leaf_by(score.0, member);
        self.leaf_pub(leaf_id).search_by(score.0, member).is_ok()
    }

    /// Get score for a member by doing a linear scan of all leaves.
    /// For O(1) member->score lookup, use the external HashMap (like Redis).
    /// This is mainly for testing convenience.
    pub fn get_score(&self, member: &[u8]) -> Option<OrderedFloat<f64>> {
        let mut leaf = Some(self.leaf_head());
        while let Some(leaf_id) = leaf {
            let l = self.leaf_pub(leaf_id);
            for i in 0..l.entry_count() {
                if l.entries()[i].1 == member {
                    return Some(l.entries()[i].0);
                }
            }
            leaf = l.next();
        }
        None
    }

    // -----------------------------------------------------------------------
    // Rank queries
    // -----------------------------------------------------------------------

    /// Return 0-based rank of (score, member) in ascending order, or None if
    /// not found. One descent summing the counts left of the path; the probe
    /// is borrowed (moon#1189).
    pub fn rank(&self, score: OrderedFloat<f64>, member: &[u8]) -> Option<usize> {
        let mut cur = self.root_id();
        let mut left = 0usize;
        for _ in 1..self.height() {
            let node = self.internal_pub(cur);
            let child_idx = node.search_by(score.0, member);
            left += node.counts()[..child_idx]
                .iter()
                .map(|&c| c as usize)
                .sum::<usize>();
            cur = node.children()[child_idx];
        }
        self.leaf_pub(cur)
            .search_by(score.0, member)
            .ok()
            .map(|idx| left + idx)
    }

    /// Return 0-based rank from the end (descending order).
    pub fn rev_rank(&self, score: OrderedFloat<f64>, member: &[u8]) -> Option<usize> {
        self.rank(score, member).map(|r| self.len() - 1 - r)
    }

    // -----------------------------------------------------------------------
    // Order statistics (moon#1170)
    // -----------------------------------------------------------------------

    /// The number of entries in the longest PREFIX of the `(score, member)`
    /// order for which `before` holds. O(log N): one root-to-leaf descent.
    ///
    /// `before` must be monotone over the tree's order — true for a prefix,
    /// false for the rest — which is what every range bound is: "score is
    /// below the minimum", "score is at most the maximum", "member sorts
    /// before the lex minimum" (the last only when every score is equal, see
    /// the BYLEX callers). The result is then the rank at which the bound
    /// cuts the order, so a range `[lo, hi)` in rank space is two calls and
    /// ZCOUNT is their difference — the same shape as Redis's
    /// `zslGetRank`-based `zcountCommand`.
    ///
    /// # Why the separators are enough
    ///
    /// Every internal node keeps `children[i] < keys[i] <= children[i + 1]`
    /// (a separator is the first key of its right subtree when it is created,
    /// and a delete never moves an entry across one). If `before(keys[i])`
    /// holds, every entry of `children[0..=i]` is `< keys[i]` and so — by
    /// monotonicity — satisfies it too. The cut therefore lies in the child
    /// just after the last separator that satisfies `before`, and the counts
    /// of the children left of it are summed without being visited. A
    /// separator whose entry has since been deleted is still a valid bound,
    /// so no descent ever reads a stale value.
    pub fn count_while(&self, before: impl Fn(f64, &[u8]) -> bool) -> usize {
        let mut cur = self.root_id();
        let mut acc = 0usize;
        for _ in 1..self.height() {
            let node = self.internal_pub(cur);
            let j = node
                .live_keys()
                .partition_point(|(s, m)| before(s.0, m.as_ref()));
            acc += node.counts()[..j]
                .iter()
                .map(|&c| c as usize)
                .sum::<usize>();
            cur = node.children()[j];
        }
        acc + self
            .leaf_pub(cur)
            .live_entries()
            .partition_point(|(s, m)| before(s.0, m.as_ref()))
    }

    /// `(leaf, index)` of the entry at 0-based ascending `rank`, in ONE
    /// descent through the subtree counts. `None` when `rank >= len()`.
    fn locate_rank(&self, rank: usize) -> Option<(NodeId, usize)> {
        if rank >= self.len() {
            return None;
        }
        let mut cur = self.root_id();
        let mut remaining = rank;
        for _ in 1..self.height() {
            let node = self.internal_pub(cur);
            let n = node.key_count() + 1;
            let mut next = None;
            for i in 0..n {
                let c = node.counts()[i] as usize;
                if remaining < c {
                    next = Some(node.children()[i]);
                    break;
                }
                remaining -= c;
            }
            cur = next?;
        }
        (remaining < self.leaf_pub(cur).entry_count()).then_some((cur, remaining))
    }

    pub fn get_by_rank(&self, rank: usize) -> Option<(OrderedFloat<f64>, &Bytes)> {
        let (leaf, idx) = self.locate_rank(rank)?;
        let (score, member) = &self.leaf_pub(leaf).entries()[idx];
        Some((*score, member))
    }

    /// Ascending iterator starting AT `rank` (0-based) and running to the end
    /// of the tree — bound it with `.take(n)`. One descent, then the leaf
    /// chain: O(log N + n) for `n` items, where calling [`Self::get_by_rank`]
    /// per item re-descended from the root every time (moon#1170).
    pub fn iter_from_rank(&self, rank: usize) -> BPTreeIter<'_> {
        let (leaf, index) = match self.locate_rank(rank) {
            Some((l, i)) => (Some(l), i),
            None => (None, 0),
        };
        BPTreeIter {
            tree: self,
            leaf,
            index,
            max: OrderedFloat(f64::INFINITY),
        }
    }

    /// Descending iterator starting AT ascending `rank` and running towards
    /// rank 0 — bound it with `.take(n)`. The mirror of
    /// [`Self::iter_from_rank`].
    pub fn iter_rev_from_rank(&self, rank: usize) -> BPTreeRevIter<'_> {
        let (leaf, index) = match self.locate_rank(rank) {
            Some((l, i)) => (Some(l), i),
            None => (None, 0),
        };
        BPTreeRevIter {
            tree: self,
            leaf,
            index,
            min: OrderedFloat(f64::NEG_INFINITY),
        }
    }

    /// Entries at ascending ranks `start..=end` (clamped to the tree), via
    /// [`Self::iter_from_rank`]: one descent plus a leaf walk.
    pub fn range_by_rank(&self, start: usize, end: usize) -> Vec<(OrderedFloat<f64>, &Bytes)> {
        let end = end.min(self.len().saturating_sub(1));
        if start > end || start >= self.len() {
            return Vec::new();
        }
        let n = end - start + 1;
        let mut result = Vec::with_capacity(n);
        result.extend(self.iter_from_rank(start).take(n));
        result
    }

    // -----------------------------------------------------------------------
    // Range iteration
    // -----------------------------------------------------------------------

    /// Find the leaf and index for the first entry >= min_key.
    fn find_start(&self, min: OrderedFloat<f64>) -> (Option<NodeId>, usize) {
        // `(min, b"")` sorts before every stored `(min, member)`.
        let leaf_id = self.find_leaf_by(min.0, b"");
        let leaf = self.leaf_pub(leaf_id);
        match leaf.search_by(min.0, b"") {
            Ok(idx) | Err(idx) => {
                if idx < leaf.entry_count() {
                    (Some(leaf_id), idx)
                } else {
                    // Move to next leaf
                    match leaf.next() {
                        Some(next) => (Some(next), 0),
                        None => (None, 0),
                    }
                }
            }
        }
    }

    pub fn range(&self, min: OrderedFloat<f64>, max: OrderedFloat<f64>) -> BPTreeIter<'_> {
        let (leaf, index) = self.find_start(min);
        BPTreeIter {
            tree: self,
            leaf,
            index,
            max,
        }
    }

    /// Descending iterator over `min <= score <= max`.
    ///
    /// Starts at the LAST entry whose score is `<= max`, found by rank
    /// (`count_while`). It used to seek to the synthetic key
    /// `(max, [0xff; 32])` and scan backwards from that leaf, which silently
    /// skipped every `score == max` member sorting after 32 `0xff` bytes —
    /// they live in a later leaf than the one the seek landed on.
    pub fn range_rev(&self, min: OrderedFloat<f64>, max: OrderedFloat<f64>) -> BPTreeRevIter<'_> {
        let upper = self.count_while(|s, _| s <= max.0);
        if upper == 0 {
            return BPTreeRevIter {
                tree: self,
                leaf: None,
                index: 0,
                min,
            };
        }
        let mut it = self.iter_rev_from_rank(upper - 1);
        it.min = min;
        it
    }

    pub fn iter(&self) -> BPTreeIter<'_> {
        let leaf = if !self.is_empty() {
            Some(self.leaf_head())
        } else {
            // Check if leaf_head has entries
            let l = self.leaf_pub(self.leaf_head());
            if l.entry_count() > 0 {
                Some(self.leaf_head())
            } else {
                None
            }
        };
        BPTreeIter {
            tree: self,
            leaf,
            index: 0,
            max: OrderedFloat(f64::INFINITY),
        }
    }

    pub fn iter_rev(&self) -> BPTreeRevIter<'_> {
        if self.is_empty() {
            return BPTreeRevIter {
                tree: self,
                leaf: None,
                index: 0,
                min: OrderedFloat(f64::NEG_INFINITY),
            };
        }
        let tail = self.leaf_tail();
        let n = self.leaf_pub(tail).entry_count();
        BPTreeRevIter {
            tree: self,
            leaf: Some(tail),
            index: if n > 0 { n - 1 } else { 0 },
            min: OrderedFloat(f64::NEG_INFINITY),
        }
    }
}

// ---------------------------------------------------------------------------
// Iterators
// ---------------------------------------------------------------------------

pub struct BPTreeIter<'a> {
    tree: &'a BPTree,
    leaf: Option<NodeId>,
    index: usize,
    max: OrderedFloat<f64>,
}

impl<'a> Iterator for BPTreeIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_id = self.leaf?;
            let leaf = self.tree.leaf_pub(leaf_id);
            if self.index < leaf.entry_count() {
                let (score, member) = &leaf.entries()[self.index];
                if *score > self.max {
                    self.leaf = None;
                    return None;
                }
                self.index += 1;
                return Some((*score, member));
            }
            // Move to next leaf
            self.leaf = leaf.next();
            self.index = 0;
        }
    }
}

pub struct BPTreeRevIter<'a> {
    tree: &'a BPTree,
    leaf: Option<NodeId>,
    index: usize, // current index to yield (counts down)
    min: OrderedFloat<f64>,
}

impl<'a> Iterator for BPTreeRevIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let leaf_id = self.leaf?;
        let leaf = self.tree.leaf_pub(leaf_id);
        let n = leaf.entry_count();
        if n == 0 {
            self.leaf = None;
            return None;
        }
        if self.index < n {
            let (score, member) = &leaf.entries()[self.index];
            if *score < self.min {
                self.leaf = None;
                return None;
            }
            if self.index == 0 {
                // Move to prev leaf
                self.leaf = leaf.prev();
                if let Some(prev_id) = self.leaf {
                    let prev = self.tree.leaf_pub(prev_id);
                    let pn = prev.entry_count();
                    self.index = if pn > 0 { pn - 1 } else { 0 };
                }
            } else {
                self.index -= 1;
            }
            return Some((*score, member));
        }
        // index >= n, shouldn't happen normally but handle gracefully
        self.leaf = None;
        None
    }
}

// ---------------------------------------------------------------------------
// moon#1170 — order-statistic primitives, differential against a sorted Vec
// ---------------------------------------------------------------------------

#[cfg(test)]
mod order_statistics_1170 {
    use super::*;

    /// SplitMix64: deterministic, dependency-free test randomness.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// A small score alphabet so ties (and ±0 / ±inf) are common: the rank
    /// arithmetic is only interesting where many entries share a score.
    const SCORES: [f64; 9] = [
        f64::NEG_INFINITY,
        -2.5,
        -0.0,
        0.0,
        1.0,
        1.5,
        7.0,
        1e300,
        f64::INFINITY,
    ];

    fn build(rng: &mut Rng, n: usize, churn: bool) -> (BPTree, Vec<(OrderedFloat<f64>, Bytes)>) {
        let mut tree = BPTree::new();
        let mut model: Vec<(OrderedFloat<f64>, Bytes)> = Vec::new();
        for i in 0..n {
            let s = OrderedFloat(SCORES[rng.below(SCORES.len() as u64) as usize]);
            let m = Bytes::from(format!("m{:05}", rng.below(4 * n as u64 + 1) + i as u64));
            if tree.insert(s, m.clone()) {
                model.push((s, m));
            }
        }
        if churn {
            // Remove about a third, so separators name deleted entries and
            // leaves have been through borrow/merge.
            let mut i = 0;
            model.retain(|(s, m)| {
                i += 1;
                if i % 3 == 0 {
                    assert!(tree.remove(*s, m));
                    false
                } else {
                    true
                }
            });
        }
        model.sort();
        (tree, model)
    }

    #[test]
    fn count_while_matches_partition_point_on_random_trees() {
        let mut rng = Rng(0x1170);
        for &(n, churn) in &[
            (0, false),
            (1, false),
            (13, false),
            (200, true),
            (3000, true),
        ] {
            let (tree, model) = build(&mut rng, n, churn);
            assert_eq!(tree.len(), model.len());
            for &v in SCORES.iter().chain([-1.0, 0.5, 2.0, 1e308].iter()) {
                let lt = model.partition_point(|(s, _)| s.0 < v);
                let le = model.partition_point(|(s, _)| s.0 <= v);
                assert_eq!(tree.count_while(|s, _| s < v), lt, "n={n} score<{v}");
                assert_eq!(tree.count_while(|s, _| s <= v), le, "n={n} score<={v}");
            }
            // A (score, member) predicate, as the lex seek uses.
            for probe in ["m", "m00100", "m01000", "m99999"] {
                let p = probe.as_bytes();
                let want =
                    model.partition_point(|(s, m)| s.0 < 1.0 || (s.0 == 1.0 && m.as_ref() < p));
                let got = tree.count_while(|s, m| s < 1.0 || (s == 1.0 && m < p));
                assert_eq!(got, want, "n={n} lex probe {probe}");
            }
        }
    }

    #[test]
    fn rank_iterators_walk_the_same_sequence_as_the_sorted_model() {
        let mut rng = Rng(0xBEEF);
        for &(n, churn) in &[(1, false), (15, false), (500, true), (4000, true)] {
            let (tree, model) = build(&mut rng, n, churn);
            let len = model.len();
            for _ in 0..60 {
                let r = rng.below(len as u64 + 2) as usize;
                let k = rng.below(40) as usize;
                let fwd: Vec<_> = tree
                    .iter_from_rank(r)
                    .take(k)
                    .map(|(s, m)| (s, m.clone()))
                    .collect();
                let want: Vec<_> = model.iter().skip(r).take(k).cloned().collect();
                assert_eq!(fwd, want, "n={n} iter_from_rank({r}).take({k})");

                let rev: Vec<_> = tree
                    .iter_rev_from_rank(r)
                    .take(k)
                    .map(|(s, m)| (s, m.clone()))
                    .collect();
                let want_rev: Vec<_> = if r < len {
                    model[..=r].iter().rev().take(k).cloned().collect()
                } else {
                    Vec::new()
                };
                assert_eq!(rev, want_rev, "n={n} iter_rev_from_rank({r}).take({k})");

                let got = tree.get_by_rank(r).map(|(s, m)| (s, m.clone()));
                assert_eq!(got, model.get(r).cloned(), "n={n} get_by_rank({r})");
            }
            let a = rng.below(len as u64 + 1) as usize;
            let b = rng.below(len as u64 + 1) as usize;
            let by_rank: Vec<_> = tree
                .range_by_rank(a, b)
                .into_iter()
                .map(|(s, m)| (s, m.clone()))
                .collect();
            let want: Vec<_> = if a <= b && a < len {
                model[a..=b.min(len - 1)].to_vec()
            } else {
                Vec::new()
            };
            assert_eq!(by_rank, want, "n={n} range_by_rank({a}, {b})");
        }
    }

    /// `range_rev` used to seek to the synthetic key `(max, [0xff; 32])`, so a
    /// `score == max` member sorting AFTER 32 `0xff` bytes — which a split
    /// can place in a later leaf than the seek lands on — was never yielded.
    #[test]
    fn range_rev_yields_members_past_the_old_0xff_sentinel() {
        let mut tree = BPTree::new();
        for i in 0..40u8 {
            tree.insert(OrderedFloat(5.0), Bytes::from(vec![0xff, i]));
        }
        let long = Bytes::from(vec![0xff; 40]);
        for i in 0..40u8 {
            let mut m = long.to_vec();
            m.push(i);
            tree.insert(OrderedFloat(5.0), Bytes::from(m));
        }
        let fwd = tree.range(OrderedFloat(5.0), OrderedFloat(5.0)).count();
        let rev = tree.range_rev(OrderedFloat(5.0), OrderedFloat(5.0)).count();
        assert_eq!(fwd, 80);
        assert_eq!(rev, 80, "range_rev lost the members past [0xff; 32]");
        let last = tree.range_rev(OrderedFloat(0.0), OrderedFloat(9.0)).next();
        assert_eq!(last.map(|(_, m)| m.len()), Some(41));
    }
}
