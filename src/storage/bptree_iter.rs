use bytes::Bytes;
use ordered_float::OrderedFloat;

use super::bptree::{BPTree, NodeId};

// ---------------------------------------------------------------------------
// Lookups
// ---------------------------------------------------------------------------

impl BPTree {
    pub fn contains(&self, score: OrderedFloat<f64>, member: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }
        let key = (score, Bytes::copy_from_slice(member));
        let leaf_id = self.find_leaf_pub(&key);
        self.leaf_pub(leaf_id).search(&key).is_ok()
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

    /// Return 0-based rank of (score, member) in ascending order, or None if not found.
    pub fn rank(&self, score: OrderedFloat<f64>, member: &[u8]) -> Option<usize> {
        let key = (score, Bytes::copy_from_slice(member));
        self.rank_internal(self.root_id(), &key, self.height())
    }

    fn rank_internal(
        &self,
        node_id: NodeId,
        key: &(OrderedFloat<f64>, Bytes),
        level: usize,
    ) -> Option<usize> {
        if level == 1 {
            // Leaf
            let leaf = self.leaf_pub(node_id);
            match leaf.search(key) {
                Ok(idx) => Some(idx),
                Err(_) => None,
            }
        } else {
            let node = self.internal_pub(node_id);
            let child_idx = node.search(key);
            // Sum counts of all children to the left
            let left_count: usize = (0..child_idx).map(|i| node.counts()[i] as usize).sum();
            let child_id = node.children()[child_idx];
            self.rank_internal(child_id, key, level - 1)
                .map(|r| left_count + r)
        }
    }

    /// Return 0-based rank from the end (descending order).
    pub fn rev_rank(&self, score: OrderedFloat<f64>, member: &[u8]) -> Option<usize> {
        self.rank(score, member).map(|r| self.len() - 1 - r)
    }

    // -----------------------------------------------------------------------
    // Index access (by rank)
    // -----------------------------------------------------------------------

    pub fn get_by_rank(&self, rank: usize) -> Option<(OrderedFloat<f64>, &Bytes)> {
        if rank >= self.len() {
            return None;
        }
        self.get_by_rank_internal(self.root_id(), rank, self.height())
    }

    fn get_by_rank_internal(
        &self,
        node_id: NodeId,
        rank: usize,
        level: usize,
    ) -> Option<(OrderedFloat<f64>, &Bytes)> {
        if level == 1 {
            let leaf = self.leaf_pub(node_id);
            if rank < leaf.entry_count() {
                let (score, member) = &leaf.entries()[rank];
                return Some((*score, member));
            }
            return None;
        }
        let node = self.internal_pub(node_id);
        let mut remaining = rank;
        let n = node.key_count() + 1;
        for i in 0..n {
            let c = node.counts()[i] as usize;
            if remaining < c {
                return self.get_by_rank_internal(node.children()[i], remaining, level - 1);
            }
            remaining -= c;
        }
        None
    }

    pub fn range_by_rank(&self, start: usize, end: usize) -> Vec<(OrderedFloat<f64>, &Bytes)> {
        let mut result = Vec::new();
        let end = end.min(self.len().saturating_sub(1));
        if start > end || start >= self.len() {
            return result;
        }
        for r in start..=end {
            if let Some(entry) = self.get_by_rank(r) {
                result.push(entry);
            }
        }
        result
    }

    // -----------------------------------------------------------------------
    // Range iteration
    // -----------------------------------------------------------------------

    /// Find the leaf and index for the first entry >= min_key.
    fn find_start(&self, min: OrderedFloat<f64>) -> (Option<NodeId>, usize) {
        let min_key = (min, Bytes::new());
        let leaf_id = self.find_leaf_pub(&min_key);
        let leaf = self.leaf_pub(leaf_id);
        match leaf.search(&min_key) {
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

    /// Find the leaf and index for the last entry <= max_key.
    fn find_end(&self, max: OrderedFloat<f64>) -> (Option<NodeId>, usize) {
        // Use a key that's just past max with a very large member
        // We want the last entry with score <= max
        // Find leaf for (max, MAX_BYTES)
        let max_key = (max, Bytes::from_static(&[0xff; 32]));
        let leaf_id = self.find_leaf_pub(&max_key);
        let leaf = self.leaf_pub(leaf_id);
        // Find the last index <= max score
        let n = leaf.entry_count();
        if n == 0 {
            // Try prev leaf
            return match leaf.prev() {
                Some(prev_id) => {
                    let prev = self.leaf_pub(prev_id);
                    let pn = prev.entry_count();
                    if pn > 0 {
                        (Some(prev_id), pn - 1)
                    } else {
                        (None, 0)
                    }
                }
                None => (None, 0),
            };
        }

        // Find last entry with score <= max
        // Binary search for insertion point of (max+epsilon)
        let mut last_valid = None;
        for i in (0..n).rev() {
            if leaf.entries()[i].0 <= max {
                last_valid = Some(i);
                break;
            }
        }

        match last_valid {
            Some(idx) => (Some(leaf_id), idx),
            None => {
                // All entries in this leaf are > max, try prev
                match leaf.prev() {
                    Some(prev_id) => {
                        let prev = self.leaf_pub(prev_id);
                        let pn = prev.entry_count();
                        if pn > 0 && prev.entries()[pn - 1].0 <= max {
                            (Some(prev_id), pn - 1)
                        } else {
                            (None, 0)
                        }
                    }
                    None => (None, 0),
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

    pub fn range_rev(&self, min: OrderedFloat<f64>, max: OrderedFloat<f64>) -> BPTreeRevIter<'_> {
        let (leaf, index) = self.find_end(max);
        BPTreeRevIter {
            tree: self,
            leaf,
            index,
            min,
        }
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
