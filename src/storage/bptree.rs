use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::cmp::Ordering;

pub use super::bptree_iter::{BPTreeIter, BPTreeRevIter};

#[path = "bptree_arena.rs"]
mod arena;
use arena::Arena;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INTERNAL_FANOUT: usize = 16; // 16 separator keys, 17 children
const LEAF_CAPACITY: usize = 14; // 14 (score, member) entries per leaf

/// Arena chunk sizes (log2): 256 leaves (~144 KiB) and 64 internal nodes
/// (~49 KiB). See `bptree_arena.rs`.
const LEAF_CHUNK_SHIFT: u32 = 8;
const INTERNAL_CHUNK_SHIFT: u32 = 6;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A node handle. Leaves and internal nodes live in SEPARATE arenas
/// (moon#1189), so the handle carries which one: bit 31 set = a leaf. The
/// kind is decided by the tag alone, never by what the slot holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeId(u32);

const NIL: NodeId = NodeId(u32::MAX);
const LEAF_TAG: u32 = 1 << 31;

impl NodeId {
    #[inline]
    fn leaf_at(idx: usize) -> Self {
        debug_assert!(idx < (LEAF_TAG - 1) as usize, "leaf arena index overflow");
        NodeId(idx as u32 | LEAF_TAG)
    }

    #[inline]
    fn internal_at(idx: usize) -> Self {
        debug_assert!(idx < LEAF_TAG as usize, "internal arena index overflow");
        NodeId(idx as u32)
    }

    #[inline]
    fn is_leaf(self) -> bool {
        self.0 & LEAF_TAG != 0 && self != NIL
    }

    #[inline]
    fn index(self) -> usize {
        (self.0 & !LEAF_TAG) as usize
    }
}

type Key = (OrderedFloat<f64>, Bytes);

/// Compare a stored key against a BORROWED `(score, member)` probe — the
/// tree's order (`OrderedFloat` on the score, then bytes) without building an
/// owned `Bytes` for the probe. Every lookup used to pay a
/// `Bytes::copy_from_slice(member)` (an allocation) for this (moon#1189).
#[inline]
fn cmp_key(k: &Key, score: f64, member: &[u8]) -> Ordering {
    k.0.cmp(&OrderedFloat(score))
        .then_with(|| k.1.as_ref().cmp(member))
}

fn default_keys<const N: usize>() -> [Key; N] {
    // `Bytes::new()` is the static empty buffer: no allocation per slot.
    std::array::from_fn(|_| Key::default())
}

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct InternalNode {
    len: u16, // number of keys (children = len + 1)
    keys: [Key; INTERNAL_FANOUT],
    children: [NodeId; INTERNAL_FANOUT + 1],
    counts: [u32; INTERNAL_FANOUT + 1], // subtree counts per child
}

impl InternalNode {
    fn new() -> Self {
        Self {
            len: 0,
            keys: default_keys(),
            children: [NIL; INTERNAL_FANOUT + 1],
            counts: [0; INTERNAL_FANOUT + 1],
        }
    }

    #[inline]
    pub(crate) fn key_count(&self) -> usize {
        self.len as usize
    }

    /// The child index to descend into for the probe `(score, member)`: the
    /// number of separators `<=` it, i.e. `keys[i-1] <= probe < keys[i]`.
    #[inline]
    pub(crate) fn search_by(&self, score: f64, member: &[u8]) -> usize {
        self.live_keys()
            .partition_point(|k| cmp_key(k, score, member) != Ordering::Greater)
    }

    fn total_count(&self) -> u32 {
        let n = self.key_count() + 1;
        self.counts[..n].iter().sum()
    }

    #[inline]
    pub(crate) fn children(&self) -> &[NodeId; INTERNAL_FANOUT + 1] {
        &self.children
    }

    /// The live separator keys, `keys[..key_count()]`. Every entry in
    /// `children[i]` is `< keys[i]` and every entry in `children[i + 1]` is
    /// `>= keys[i]` — the invariant the order-statistic descents
    /// (`BPTree::count_while`) partition on.
    #[inline]
    pub(crate) fn live_keys(&self) -> &[Key] {
        &self.keys[..self.key_count()]
    }

    #[inline]
    pub(crate) fn counts(&self) -> &[u32; INTERNAL_FANOUT + 1] {
        &self.counts
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LeafNode {
    len: u16,
    entries: [Key; LEAF_CAPACITY],
    // `NIL`-terminated rather than `Option<NodeId>`: 4 bytes each instead of
    // 8, on the node kind that makes up ~94% of the arena.
    next: NodeId,
    prev: NodeId,
}

impl LeafNode {
    fn new() -> Self {
        Self {
            len: 0,
            entries: default_keys(),
            next: NIL,
            prev: NIL,
        }
    }

    #[inline]
    pub(crate) fn entry_count(&self) -> usize {
        self.len as usize
    }

    /// Binary search for the probe. `Ok(idx)` on an exact match, `Err(idx)`
    /// for the insertion point.
    #[inline]
    pub(crate) fn search_by(&self, score: f64, member: &[u8]) -> Result<usize, usize> {
        self.live_entries()
            .binary_search_by(|e| cmp_key(e, score, member))
    }

    #[inline]
    pub(crate) fn entries(&self) -> &[Key; LEAF_CAPACITY] {
        &self.entries
    }

    /// The live entries, `entries[..entry_count()]`, in `(score, member)`
    /// order.
    #[inline]
    pub(crate) fn live_entries(&self) -> &[Key] {
        &self.entries[..self.entry_count()]
    }

    #[inline]
    pub(crate) fn next(&self) -> Option<NodeId> {
        (self.next != NIL).then_some(self.next)
    }

    #[inline]
    pub(crate) fn prev(&self) -> Option<NodeId> {
        (self.prev != NIL).then_some(self.prev)
    }
}

/// Bytes of one LEAF slot — the smallest slot either arena deals in.
///
/// Leaves and internal nodes used to share one `Vec<Node>` arena of an enum
/// sized by the larger variant, so every leaf (~94% of all nodes) carried
/// ~26% padding (moon#1189). They now live in separate arenas, so
/// `node_capacity() * NODE_BYTES` is a FLOOR on the arena's real size (an
/// internal slot, [`INTERNAL_NODE_BYTES`], is larger) and
/// [`BPTree::memory_bytes`] is the exact figure (moon#788).
pub const NODE_BYTES: usize = std::mem::size_of::<LeafNode>();

/// Bytes of one internal-node slot.
pub const INTERNAL_NODE_BYTES: usize = std::mem::size_of::<InternalNode>();

// ---------------------------------------------------------------------------
// BPTree
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BPTree {
    root: NodeId,
    leaves: Arena<LeafNode, LEAF_CHUNK_SHIFT>,
    internals: Arena<InternalNode, INTERNAL_CHUNK_SHIFT>,
    free_leaves: Vec<u32>,
    free_internals: Vec<u32>,
    len: usize,
    height: usize,
    leaf_head: NodeId, // first leaf (leftmost)
    leaf_tail: NodeId, // last leaf (rightmost) for reverse iteration
}

impl Default for BPTree {
    fn default() -> Self {
        Self::new()
    }
}

impl BPTree {
    pub fn new() -> Self {
        let mut tree = Self {
            root: NIL,
            leaves: Arena::new(),
            internals: Arena::new(),
            free_leaves: Vec::new(),
            free_internals: Vec::new(),
            len: 0,
            height: 0,
            leaf_head: NIL,
            leaf_tail: NIL,
        };
        // Allocate initial root leaf
        let root = tree.alloc_leaf();
        tree.root = root;
        tree.leaf_head = root;
        tree.leaf_tail = root;
        tree.height = 1;
        tree
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes this tree's node arenas really cost (moon#788).
    ///
    /// Two chunked arenas since moon#1189 (`bptree_arena.rs`): leaves at
    /// [`NODE_BYTES`] per slot and internal nodes at [`INTERNAL_NODE_BYTES`],
    /// each billed by its CAPACITY (what the allocator holds), plus the free
    /// lists. An empty `BPTree` still owns a real allocation — its root leaf.
    ///
    /// The estimator used to charge `tree.len() * 80` — per *entry*, where
    /// the allocation is per *node* — which is the 5.75x under-report moon#788
    /// measured on Linux.
    ///
    /// O(1): capacity reads, one cached sum per arena, integer arithmetic.
    /// Safe to snapshot before/after every mutation on the write path.
    #[inline]
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        use crate::storage::mem_size::vec_bytes;
        self.leaves.bytes()
            + self.internals.bytes()
            + vec_bytes(self.free_leaves.capacity(), std::mem::size_of::<u32>())
            + vec_bytes(self.free_internals.capacity(), std::mem::size_of::<u32>())
    }

    /// Number of node slots the arenas have allocated (live + free), leaves
    /// and internal nodes together. Exposed for the accounting tests, which
    /// assert the ledger covers the real arena rather than a per-entry
    /// approximation; `node_capacity() * NODE_BYTES` is a floor on it.
    #[inline]
    #[must_use]
    pub fn node_capacity(&self) -> usize {
        self.leaves.capacity() + self.internals.capacity()
    }

    /// Number of leaves in the chain. O(leaves) — for fill measurements
    /// (`benches/bptree_memory.rs`), never for a command path.
    pub fn leaf_count(&self) -> usize {
        let mut n = 0;
        let mut cur = Some(self.leaf_head);
        while let Some(id) = cur {
            n += 1;
            cur = self.leaf(id).next();
        }
        n
    }

    #[inline]
    pub(crate) fn root_id(&self) -> NodeId {
        self.root
    }

    #[inline]
    pub(crate) fn height(&self) -> usize {
        self.height
    }

    #[inline]
    pub(crate) fn leaf_head(&self) -> NodeId {
        self.leaf_head
    }

    #[inline]
    pub(crate) fn leaf_tail(&self) -> NodeId {
        self.leaf_tail
    }

    #[inline]
    pub(crate) fn leaf_pub(&self, id: NodeId) -> &LeafNode {
        self.leaf(id)
    }

    #[inline]
    pub(crate) fn internal_pub(&self, id: NodeId) -> &InternalNode {
        self.internal(id)
    }

    /// Descend to the leaf that holds (or would hold) the probe.
    #[inline]
    pub(crate) fn find_leaf_by(&self, score: f64, member: &[u8]) -> NodeId {
        let mut cur = self.root;
        for _ in 1..self.height {
            let node = self.internal(cur);
            cur = node.children[node.search_by(score, member)];
        }
        cur
    }

    pub fn clear(&mut self) {
        self.leaves.clear();
        self.internals.clear();
        self.free_leaves.clear();
        self.free_internals.clear();
        self.len = 0;
        let root = self.alloc_leaf();
        self.root = root;
        self.leaf_head = root;
        self.leaf_tail = root;
        self.height = 1;
    }

    // -----------------------------------------------------------------------
    // Node allocation
    // -----------------------------------------------------------------------

    fn alloc_leaf(&mut self) -> NodeId {
        if let Some(idx) = self.free_leaves.pop() {
            *self.leaves.get_mut(idx as usize) = LeafNode::new();
            NodeId::leaf_at(idx as usize)
        } else {
            NodeId::leaf_at(self.leaves.push(LeafNode::new()))
        }
    }

    fn alloc_internal(&mut self) -> NodeId {
        if let Some(idx) = self.free_internals.pop() {
            *self.internals.get_mut(idx as usize) = InternalNode::new();
            NodeId::internal_at(idx as usize)
        } else {
            NodeId::internal_at(self.internals.push(InternalNode::new()))
        }
    }

    fn free_node(&mut self, id: NodeId) {
        if id.is_leaf() {
            self.free_leaves.push(id.index() as u32);
        } else {
            self.free_internals.push(id.index() as u32);
        }
    }

    #[inline]
    fn leaf(&self, id: NodeId) -> &LeafNode {
        if !id.is_leaf() {
            panic!("expected leaf");
        }
        self.leaves.get(id.index())
    }

    #[inline]
    fn leaf_mut(&mut self, id: NodeId) -> &mut LeafNode {
        if !id.is_leaf() {
            panic!("expected leaf");
        }
        self.leaves.get_mut(id.index())
    }

    #[inline]
    fn internal(&self, id: NodeId) -> &InternalNode {
        if id.is_leaf() || id == NIL {
            panic!("expected internal");
        }
        self.internals.get(id.index())
    }

    #[inline]
    fn internal_mut(&mut self, id: NodeId) -> &mut InternalNode {
        if id.is_leaf() || id == NIL {
            panic!("expected internal");
        }
        self.internals.get_mut(id.index())
    }

    /// Two distinct leaves, both mutably — what lets a split or merge MOVE
    /// entries between siblings (`mem::take`) instead of cloning them.
    fn two_leaves_mut(&mut self, a: NodeId, b: NodeId) -> (&mut LeafNode, &mut LeafNode) {
        assert!(a.is_leaf() && b.is_leaf(), "two leaves");
        self.leaves.get2_mut(a.index(), b.index())
    }

    /// Two distinct internal nodes, both mutably. See [`Self::two_leaves_mut`].
    fn two_internals_mut(
        &mut self,
        a: NodeId,
        b: NodeId,
    ) -> (&mut InternalNode, &mut InternalNode) {
        assert!(!a.is_leaf() && !b.is_leaf(), "two internals");
        self.internals.get2_mut(a.index(), b.index())
    }

    /// Entries (leaf) or separator keys (internal) held by `id`.
    #[inline]
    fn node_len(&self, id: NodeId) -> usize {
        if id.is_leaf() {
            self.leaf(id).entry_count()
        } else {
            self.internal(id).key_count()
        }
    }

    // -----------------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------------

    /// Insert (score, member). Returns true if new entry (not update).
    pub fn insert(&mut self, score: OrderedFloat<f64>, member: Bytes) -> bool {
        // Reject NaN
        if score.0.is_nan() {
            return false;
        }
        let result = self.insert_recursive(self.root, score, member, self.height, true, true);
        match result {
            InsertResult::Done(is_new) => {
                if is_new {
                    self.len += 1;
                }
                is_new
            }
            InsertResult::Split {
                new_node,
                separator,
                is_new,
            } => {
                // Root split: create new root
                let old_root = self.root;
                let new_root = self.alloc_internal();
                {
                    let r = self.internal_mut(new_root);
                    r.keys[0] = separator;
                    r.children[0] = old_root;
                    r.children[1] = new_node;
                    r.len = 1;
                }
                // Recompute counts for new root
                self.recompute_child_count(new_root, 0);
                self.recompute_child_count(new_root, 1);
                self.root = new_root;
                self.height += 1;
                if is_new {
                    self.len += 1;
                }
                is_new
            }
        }
    }

    /// `left_spine` / `right_spine`: every step so far took the first / last
    /// child, i.e. this node is the left-most / right-most at its level —
    /// where a split can be lopsided (see [`Self::split_internal_and_insert`]).
    fn insert_recursive(
        &mut self,
        node_id: NodeId,
        score: OrderedFloat<f64>,
        member: Bytes,
        level: usize,
        left_spine: bool,
        right_spine: bool,
    ) -> InsertResult {
        if level == 1 {
            return self.insert_into_leaf(node_id, score, member);
        }

        let (child_idx, child_id, n) = {
            let node = self.internal(node_id);
            let i = node.search_by(score.0, &member);
            (i, node.children[i], node.key_count())
        };
        let result = self.insert_recursive(
            child_id,
            score,
            member,
            level - 1,
            left_spine && child_idx == 0,
            right_spine && child_idx == n,
        );
        match result {
            InsertResult::Done(is_new) => {
                if is_new {
                    self.internal_mut(node_id).counts[child_idx] += 1;
                }
                InsertResult::Done(is_new)
            }
            InsertResult::Split {
                new_node,
                separator,
                is_new,
            } => {
                if is_new {
                    // Recomputed right below, from the two halves.
                    self.internal_mut(node_id).counts[child_idx] += 1;
                }
                self.insert_into_internal(
                    node_id,
                    child_idx,
                    separator,
                    new_node,
                    is_new,
                    left_spine,
                    right_spine,
                )
            }
        }
    }

    fn insert_into_leaf(
        &mut self,
        leaf_id: NodeId,
        score: OrderedFloat<f64>,
        member: Bytes,
    ) -> InsertResult {
        let idx = match self.leaf(leaf_id).search_by(score.0, &member) {
            // Duplicate (score, member) -- already exists
            Ok(_) => return InsertResult::Done(false),
            Err(idx) => idx,
        };
        let n = self.leaf(leaf_id).entry_count();
        if n < LEAF_CAPACITY {
            let leaf = self.leaf_mut(leaf_id);
            // `entries[n]` is an empty placeholder; rotate it to `idx`.
            leaf.entries[idx..=n].rotate_right(1);
            leaf.entries[idx] = (score, member);
            leaf.len += 1;
            InsertResult::Done(true)
        } else {
            self.split_leaf_and_insert(leaf_id, idx, (score, member))
        }
    }

    /// Split a full leaf around the insertion of `key` at `insert_idx`,
    /// MOVING entries into the new right sibling (no temporary `Vec`, no
    /// clones — moon#1189).
    ///
    /// The split point is 7/8 in general, but LOPSIDED at the ends of the
    /// chain: an append past the last entry of the TAIL leaf keeps all 14 in
    /// the old leaf and starts the new one with just the new key, and a
    /// prepend before the first entry of the HEAD leaf mirrors that. Rising
    /// scores (timestamps, counters) used to leave every leaf at 7/14 — half
    /// the arena empty for the commonest zset shape; they now fill leaves
    /// completely. Removal rebalancing is unchanged (a 1-entry leaf borrows
    /// or merges on its next delete like any under-filled leaf).
    fn split_leaf_and_insert(
        &mut self,
        leaf_id: NodeId,
        insert_idx: usize,
        key: Key,
    ) -> InsertResult {
        let total = LEAF_CAPACITY + 1;
        let (is_head, is_tail) = {
            let l = self.leaf(leaf_id);
            (l.prev == NIL, l.next == NIL)
        };
        let left_n = if is_tail && insert_idx == LEAF_CAPACITY {
            LEAF_CAPACITY
        } else if is_head && insert_idx == 0 {
            1
        } else {
            total / 2
        };

        let new_leaf_id = self.alloc_leaf();
        let old_next = {
            let (old, new) = self.two_leaves_mut(leaf_id, new_leaf_id);
            if insert_idx < left_n {
                // The key lands on the left: move old[left_n-1..] right.
                for (j, i) in (left_n - 1..LEAF_CAPACITY).enumerate() {
                    new.entries[j] = std::mem::take(&mut old.entries[i]);
                }
                old.entries[insert_idx..left_n].rotate_right(1);
                old.entries[insert_idx] = key;
            } else {
                // The key lands on the right at `insert_idx - left_n`.
                let r_idx = insert_idx - left_n;
                let mut j = 0;
                for i in left_n..LEAF_CAPACITY {
                    if j == r_idx {
                        j += 1;
                    }
                    new.entries[j] = std::mem::take(&mut old.entries[i]);
                    j += 1;
                }
                new.entries[r_idx] = key;
            }
            old.len = left_n as u16;
            new.len = (total - left_n) as u16;

            // Link leaves: old -> new -> old.next
            let old_next = old.next;
            new.next = old_next;
            new.prev = leaf_id;
            old.next = new_leaf_id;
            old_next
        };
        if old_next != NIL {
            self.leaf_mut(old_next).prev = new_leaf_id;
        } else {
            // new_leaf is the new tail
            self.leaf_tail = new_leaf_id;
        }

        // A refcount bump, not a copy: the separator shares the member's
        // buffer with the entry it names.
        let separator = self.leaf(new_leaf_id).entries[0].clone();

        InsertResult::Split {
            new_node: new_leaf_id,
            separator,
            is_new: true,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_into_internal(
        &mut self,
        node_id: NodeId,
        child_idx: usize,
        separator: Key,
        new_child: NodeId,
        is_new: bool,
        left_spine: bool,
        right_spine: bool,
    ) -> InsertResult {
        let n = self.internal(node_id).key_count();
        if n < INTERNAL_FANOUT {
            let node = self.internal_mut(node_id);
            // Shift keys and children right, by rotation: no clones.
            node.keys[child_idx..=n].rotate_right(1);
            node.keys[child_idx] = separator;
            node.children.copy_within(child_idx + 1..=n, child_idx + 2);
            node.counts.copy_within(child_idx + 1..=n, child_idx + 2);
            node.children[child_idx + 1] = new_child;
            node.len += 1;
            self.recompute_child_count(node_id, child_idx);
            self.recompute_child_count(node_id, child_idx + 1);
            InsertResult::Done(is_new)
        } else {
            self.split_internal_and_insert(
                node_id,
                child_idx,
                separator,
                new_child,
                is_new,
                left_spine,
                right_spine,
            )
        }
    }

    /// Split a full internal node around the insertion of `separator` /
    /// `new_child`, staging the merged sequence on the STACK by moving keys
    /// out of the node (no heap `Vec`, no clones — moon#1189).
    ///
    /// Lopsided on the spines, like the leaf split: when the child that split
    /// was the LAST child of a right-spine node (an append), the left half
    /// keeps 15 keys and the right starts with 1; the left-spine prepend
    /// mirrors it. An even split would leave every internal node of a
    /// rising-score tree half empty.
    #[allow(clippy::too_many_arguments)]
    fn split_internal_and_insert(
        &mut self,
        node_id: NodeId,
        child_idx: usize,
        separator: Key,
        new_child: NodeId,
        is_new: bool,
        left_spine: bool,
        right_spine: bool,
    ) -> InsertResult {
        const TOTAL: usize = INTERNAL_FANOUT + 1; // keys after the insert
        let mut keys: [Key; TOTAL] = default_keys();
        let mut children = [NIL; TOTAL + 1];
        {
            let node = self.internal_mut(node_id);
            let old_n = node.key_count();
            debug_assert_eq!(old_n, INTERNAL_FANOUT);
            for (dst, src) in keys.iter_mut().zip(node.keys[..old_n].iter_mut()) {
                *dst = std::mem::take(src);
            }
            keys[old_n] = separator;
            keys[child_idx..=old_n].rotate_right(1);
            children[..=old_n].copy_from_slice(&node.children[..=old_n]);
            children[old_n + 1] = new_child;
            children[child_idx + 1..=old_n + 1].rotate_right(1);
            node.children = [NIL; INTERNAL_FANOUT + 1];
            node.counts = [0; INTERNAL_FANOUT + 1];
            node.len = 0;
        }

        let mid = if right_spine && child_idx == INTERNAL_FANOUT {
            TOTAL - 2
        } else if left_spine && child_idx == 0 {
            1
        } else {
            TOTAL / 2
        };
        let right_n = TOTAL - mid - 1;
        let promote_key = std::mem::take(&mut keys[mid]);

        // Left: keys[0..mid], children[0..=mid]
        {
            let node = self.internal_mut(node_id);
            for (dst, src) in node.keys.iter_mut().zip(keys[..mid].iter_mut()) {
                *dst = std::mem::take(src);
            }
            node.children[..=mid].copy_from_slice(&children[..=mid]);
            node.len = mid as u16;
        }

        // Right: keys[mid+1..], children[mid+1..]
        let new_node_id = self.alloc_internal();
        {
            let new_node = self.internal_mut(new_node_id);
            for (dst, src) in new_node.keys.iter_mut().zip(keys[mid + 1..].iter_mut()) {
                *dst = std::mem::take(src);
            }
            new_node.children[..=right_n].copy_from_slice(&children[mid + 1..]);
            new_node.len = right_n as u16;
        }

        // Recompute counts
        for i in 0..=mid {
            self.recompute_child_count(node_id, i);
        }
        for i in 0..=right_n {
            self.recompute_child_count(new_node_id, i);
        }

        InsertResult::Split {
            new_node: new_node_id,
            separator: promote_key,
            is_new,
        }
    }

    fn recompute_child_count(&mut self, node_id: NodeId, child_idx: usize) {
        let child_id = self.internal(node_id).children[child_idx];
        let count = self.subtree_count(child_id);
        self.internal_mut(node_id).counts[child_idx] = count;
    }

    fn subtree_count(&self, node_id: NodeId) -> u32 {
        if node_id.is_leaf() {
            self.leaf(node_id).len as u32
        } else {
            self.internal(node_id).total_count()
        }
    }

    // -----------------------------------------------------------------------
    // Remove
    // -----------------------------------------------------------------------

    /// Remove entry by (score, member). Returns true if existed.
    ///
    /// The probe is BORROWED end to end (moon#1189): this used to build a
    /// `Bytes::copy_from_slice(member)` — an allocation on every ZREM and
    /// every rescore — just to compare against.
    pub fn remove(&mut self, score: OrderedFloat<f64>, member: &[u8]) -> bool {
        if self.len == 0 {
            return false;
        }
        let removed = self.remove_recursive(self.root, score.0, member, self.height);
        if removed {
            self.len -= 1;
            // Shrink root if internal with single child
            while self.height > 1 {
                let (keys, first) = {
                    let n = self.internal(self.root);
                    (n.key_count(), n.children[0])
                };
                if keys == 0 {
                    let old_root = self.root;
                    self.root = first;
                    self.free_node(old_root);
                    self.height -= 1;
                    continue;
                }
                break;
            }
        }
        removed
    }

    fn remove_recursive(
        &mut self,
        node_id: NodeId,
        score: f64,
        member: &[u8],
        level: usize,
    ) -> bool {
        if level == 1 {
            return self.remove_from_leaf(node_id, score, member);
        }

        let child_idx = self.internal(node_id).search_by(score, member);
        let child_id = self.internal(node_id).children[child_idx];
        let removed = self.remove_recursive(child_id, score, member, level - 1);

        if removed {
            self.internal_mut(node_id).counts[child_idx] -= 1;
            // Check if child is underflowing
            self.rebalance_child(node_id, child_idx, level - 1);
        }
        removed
    }

    fn remove_from_leaf(&mut self, leaf_id: NodeId, score: f64, member: &[u8]) -> bool {
        match self.leaf(leaf_id).search_by(score, member) {
            Ok(idx) => {
                let leaf = self.leaf_mut(leaf_id);
                let n = leaf.entry_count();
                leaf.entries[idx..n].rotate_left(1);
                // Drops the removed entry's `Bytes` handle.
                leaf.entries[n - 1] = Key::default();
                leaf.len -= 1;
                true
            }
            Err(_) => false,
        }
    }

    fn rebalance_child(&mut self, parent_id: NodeId, child_idx: usize, child_level: usize) {
        let child_id = self.internal(parent_id).children[child_idx];
        let is_leaf_child = child_level == 1;
        let min_keys = if is_leaf_child {
            // leaf: allow going down to ~half - 1
            (LEAF_CAPACITY + 1) / 2 - 1
        } else {
            (INTERNAL_FANOUT + 1) / 2 - 1
        };

        if self.node_len(child_id) >= min_keys {
            return; // No underflow
        }

        let parent_key_count = self.internal(parent_id).key_count();

        // Try borrow from left sibling
        if child_idx > 0 {
            let left_id = self.internal(parent_id).children[child_idx - 1];
            if self.node_len(left_id) > min_keys {
                if is_leaf_child {
                    self.borrow_from_left_leaf(parent_id, child_idx);
                } else {
                    self.borrow_from_left_internal(parent_id, child_idx);
                }
                return;
            }
        }

        // Try borrow from right sibling
        if child_idx < parent_key_count {
            let right_id = self.internal(parent_id).children[child_idx + 1];
            if self.node_len(right_id) > min_keys {
                if is_leaf_child {
                    self.borrow_from_right_leaf(parent_id, child_idx);
                } else {
                    self.borrow_from_right_internal(parent_id, child_idx);
                }
                return;
            }
        }

        // Merge: prefer merging with left
        if child_idx > 0 {
            if is_leaf_child {
                self.merge_leaves(parent_id, child_idx - 1);
            } else {
                self.merge_internals(parent_id, child_idx - 1);
            }
        } else if child_idx < parent_key_count {
            if is_leaf_child {
                self.merge_leaves(parent_id, child_idx);
            } else {
                self.merge_internals(parent_id, child_idx);
            }
        }
    }

    fn borrow_from_left_leaf(&mut self, parent_id: NodeId, child_idx: usize) {
        let left_id = self.internal(parent_id).children[child_idx - 1];
        let child_id = self.internal(parent_id).children[child_idx];

        let new_sep = {
            let (left, child) = self.two_leaves_mut(left_id, child_id);
            let left_n = left.entry_count();
            let borrowed = std::mem::take(&mut left.entries[left_n - 1]);
            left.len -= 1;
            // Insert at front of child
            let child_n = child.entry_count();
            child.entries[..=child_n].rotate_right(1);
            child.entries[0] = borrowed;
            child.len += 1;
            child.entries[0].clone()
        };
        // Update parent separator
        self.internal_mut(parent_id).keys[child_idx - 1] = new_sep;

        // Update counts
        self.recompute_child_count(parent_id, child_idx - 1);
        self.recompute_child_count(parent_id, child_idx);
    }

    fn borrow_from_right_leaf(&mut self, parent_id: NodeId, child_idx: usize) {
        let right_id = self.internal(parent_id).children[child_idx + 1];
        let child_id = self.internal(parent_id).children[child_idx];

        let new_sep = {
            let (right, child) = self.two_leaves_mut(right_id, child_id);
            let right_n = right.entry_count();
            let borrowed = std::mem::take(&mut right.entries[0]);
            right.entries[..right_n].rotate_left(1);
            right.len -= 1;
            // Append to child
            let child_n = child.entry_count();
            child.entries[child_n] = borrowed;
            child.len += 1;
            right.entries[0].clone()
        };
        // Update parent separator
        self.internal_mut(parent_id).keys[child_idx] = new_sep;

        self.recompute_child_count(parent_id, child_idx);
        self.recompute_child_count(parent_id, child_idx + 1);
    }

    fn borrow_from_left_internal(&mut self, parent_id: NodeId, child_idx: usize) {
        let left_id = self.internal(parent_id).children[child_idx - 1];
        let child_id = self.internal(parent_id).children[child_idx];

        let parent_sep = std::mem::take(&mut self.internal_mut(parent_id).keys[child_idx - 1]);
        let borrowed_key = {
            let (left, child) = self.two_internals_mut(left_id, child_id);
            let left_n = left.key_count();
            let borrowed_key = std::mem::take(&mut left.keys[left_n - 1]);
            let borrowed_child = left.children[left_n];
            let borrowed_count = left.counts[left_n];
            left.children[left_n] = NIL;
            left.counts[left_n] = 0;
            left.len -= 1;

            // Insert at front of child
            let child_n = child.key_count();
            child.keys[..=child_n].rotate_right(1);
            child.keys[0] = parent_sep;
            child.children.copy_within(0..=child_n, 1);
            child.counts.copy_within(0..=child_n, 1);
            child.children[0] = borrowed_child;
            child.counts[0] = borrowed_count;
            child.len += 1;
            borrowed_key
        };

        // Update parent separator
        self.internal_mut(parent_id).keys[child_idx - 1] = borrowed_key;

        self.recompute_child_count(parent_id, child_idx - 1);
        self.recompute_child_count(parent_id, child_idx);
    }

    fn borrow_from_right_internal(&mut self, parent_id: NodeId, child_idx: usize) {
        let right_id = self.internal(parent_id).children[child_idx + 1];
        let child_id = self.internal(parent_id).children[child_idx];

        let parent_sep = std::mem::take(&mut self.internal_mut(parent_id).keys[child_idx]);
        let borrowed_key = {
            let (right, child) = self.two_internals_mut(right_id, child_id);
            let right_n = right.key_count();
            let borrowed_key = std::mem::take(&mut right.keys[0]);
            let borrowed_child = right.children[0];
            let borrowed_count = right.counts[0];
            // Remove from right (shift left)
            right.keys[..right_n].rotate_left(1);
            right.children.copy_within(1..=right_n, 0);
            right.counts.copy_within(1..=right_n, 0);
            right.children[right_n] = NIL;
            right.counts[right_n] = 0;
            right.len -= 1;

            // Append to child. The moved subtree's size moves with it.
            let child_n = child.key_count();
            child.keys[child_n] = parent_sep;
            child.children[child_n + 1] = borrowed_child;
            child.counts[child_n + 1] = borrowed_count;
            child.len += 1;
            borrowed_key
        };

        // Update parent separator
        self.internal_mut(parent_id).keys[child_idx] = borrowed_key;

        self.recompute_child_count(parent_id, child_idx);
        self.recompute_child_count(parent_id, child_idx + 1);
    }

    /// Remove separator `keys[left_idx]` and child `children[left_idx + 1]`
    /// from `parent` after that child was merged into its left sibling.
    fn drop_separator(&mut self, parent_id: NodeId, left_idx: usize) {
        let parent = self.internal_mut(parent_id);
        let parent_n = parent.key_count();
        parent.keys[left_idx..parent_n].rotate_left(1);
        parent.keys[parent_n - 1] = Key::default();
        parent
            .children
            .copy_within(left_idx + 2..=parent_n, left_idx + 1);
        parent
            .counts
            .copy_within(left_idx + 2..=parent_n, left_idx + 1);
        parent.children[parent_n] = NIL;
        parent.counts[parent_n] = 0;
        parent.len -= 1;
    }

    /// Merge child[left_idx] and child[left_idx+1] in parent, removing separator key.
    fn merge_leaves(&mut self, parent_id: NodeId, left_idx: usize) {
        let left_id = self.internal(parent_id).children[left_idx];
        let right_id = self.internal(parent_id).children[left_idx + 1];

        // Move right entries into left.
        let right_next = {
            let (left, right) = self.two_leaves_mut(left_id, right_id);
            let left_n = left.entry_count();
            let right_n = right.entry_count();
            for i in 0..right_n {
                left.entries[left_n + i] = std::mem::take(&mut right.entries[i]);
            }
            left.len = (left_n + right_n) as u16;
            right.len = 0;
            left.next = right.next;
            right.next
        };

        // Update linked list
        if right_next != NIL {
            self.leaf_mut(right_next).prev = left_id;
        } else {
            self.leaf_tail = left_id;
        }

        self.free_node(right_id);
        self.drop_separator(parent_id, left_idx);
        self.recompute_child_count(parent_id, left_idx);
    }

    fn merge_internals(&mut self, parent_id: NodeId, left_idx: usize) {
        let left_id = self.internal(parent_id).children[left_idx];
        let right_id = self.internal(parent_id).children[left_idx + 1];

        let parent_sep = std::mem::take(&mut self.internal_mut(parent_id).keys[left_idx]);
        {
            let (left, right) = self.two_internals_mut(left_id, right_id);
            let left_n = left.key_count();
            let right_n = right.key_count();
            // Append separator + right keys/children to left
            left.keys[left_n] = parent_sep;
            for i in 0..right_n {
                left.keys[left_n + 1 + i] = std::mem::take(&mut right.keys[i]);
            }
            left.children[left_n + 1..=left_n + 1 + right_n]
                .copy_from_slice(&right.children[..=right_n]);
            left.counts[left_n + 1..=left_n + 1 + right_n]
                .copy_from_slice(&right.counts[..=right_n]);
            left.len = (left_n + 1 + right_n) as u16;
            right.len = 0;
        }

        self.free_node(right_id);
        self.drop_separator(parent_id, left_idx);
        self.recompute_child_count(parent_id, left_idx);
    }
}

enum InsertResult {
    Done(bool), // bool = is_new
    Split {
        new_node: NodeId,
        separator: Key,
        is_new: bool,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "bptree_invariants.rs"]
mod invariants;

#[cfg(test)]
#[path = "bptree_tests.rs"]
mod tests;
