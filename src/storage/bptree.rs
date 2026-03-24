use bytes::Bytes;
use ordered_float::OrderedFloat;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INTERNAL_FANOUT: usize = 16; // 16 separator keys, 17 children
const LEAF_CAPACITY: usize = 14; // 14 (score, member) entries per leaf

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeId(u32);

const NIL: NodeId = NodeId(u32::MAX);

type Key = (OrderedFloat<f64>, Bytes);

fn default_keys() -> [Key; INTERNAL_FANOUT] {
    std::array::from_fn(|_| (OrderedFloat(0.0), Bytes::new()))
}

fn default_entries() -> [Key; LEAF_CAPACITY] {
    std::array::from_fn(|_| (OrderedFloat(0.0), Bytes::new()))
}

fn default_children() -> [NodeId; INTERNAL_FANOUT + 1] {
    [NIL; INTERNAL_FANOUT + 1]
}

fn default_counts() -> [u32; INTERNAL_FANOUT + 1] {
    [0; INTERNAL_FANOUT + 1]
}

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct InternalNode {
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
            children: default_children(),
            counts: default_counts(),
        }
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.len as usize
    }

    /// Binary search for the child index to descend into for `key`.
    /// Returns index i such that keys[i-1] <= key < keys[i] (conceptually).
    fn search(&self, key: &Key) -> usize {
        let n = self.key_count();
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if key >= &self.keys[mid] {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn total_count(&self) -> u32 {
        let n = self.key_count() + 1;
        self.counts[..n].iter().sum()
    }
}

#[derive(Debug, Clone)]
struct LeafNode {
    len: u16,
    entries: [Key; LEAF_CAPACITY],
    next: Option<NodeId>,
    prev: Option<NodeId>,
}

impl LeafNode {
    fn new() -> Self {
        Self {
            len: 0,
            entries: default_entries(),
            next: None,
            prev: None,
        }
    }

    #[inline]
    fn entry_count(&self) -> usize {
        self.len as usize
    }

    /// Binary search within leaf. Returns Ok(idx) if exact match, Err(idx) for insertion point.
    fn search(&self, key: &Key) -> Result<usize, usize> {
        let n = self.entry_count();
        let slice = &self.entries[..n];
        slice.binary_search_by(|e| e.cmp(key))
    }
}

#[derive(Debug, Clone)]
enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

// ---------------------------------------------------------------------------
// BPTree
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BPTree {
    root: NodeId,
    nodes: Vec<Node>,
    free_list: Vec<NodeId>,
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
            nodes: Vec::new(),
            free_list: Vec::new(),
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

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.free_list.clear();
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
        if let Some(id) = self.free_list.pop() {
            self.nodes[id.0 as usize] = Node::Leaf(LeafNode::new());
            id
        } else {
            let id = NodeId(self.nodes.len() as u32);
            self.nodes.push(Node::Leaf(LeafNode::new()));
            id
        }
    }

    fn alloc_internal(&mut self) -> NodeId {
        if let Some(id) = self.free_list.pop() {
            self.nodes[id.0 as usize] = Node::Internal(InternalNode::new());
            id
        } else {
            let id = NodeId(self.nodes.len() as u32);
            self.nodes.push(Node::Internal(InternalNode::new()));
            id
        }
    }

    fn free_node(&mut self, id: NodeId) {
        self.free_list.push(id);
    }

    #[inline]
    fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id.0 as usize]
    }

    #[inline]
    fn leaf(&self, id: NodeId) -> &LeafNode {
        match &self.nodes[id.0 as usize] {
            Node::Leaf(l) => l,
            _ => panic!("expected leaf"),
        }
    }

    #[inline]
    fn leaf_mut(&mut self, id: NodeId) -> &mut LeafNode {
        match &mut self.nodes[id.0 as usize] {
            Node::Leaf(l) => l,
            _ => panic!("expected leaf"),
        }
    }

    #[inline]
    fn internal(&self, id: NodeId) -> &InternalNode {
        match &self.nodes[id.0 as usize] {
            Node::Internal(n) => n,
            _ => panic!("expected internal"),
        }
    }

    #[inline]
    fn internal_mut(&mut self, id: NodeId) -> &mut InternalNode {
        match &mut self.nodes[id.0 as usize] {
            Node::Internal(n) => n,
            _ => panic!("expected internal"),
        }
    }

    // -----------------------------------------------------------------------
    // Find leaf
    // -----------------------------------------------------------------------

    /// Descend from root to find the leaf containing `key`.
    fn find_leaf(&self, key: &Key) -> NodeId {
        let mut cur = self.root;
        for _ in 1..self.height {
            let idx = self.internal(cur).search(key);
            cur = self.internal(cur).children[idx];
        }
        cur
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
        let key = (score, member);
        let result = self.insert_recursive(self.root, &key, self.height);
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

    fn insert_recursive(&mut self, node_id: NodeId, key: &Key, level: usize) -> InsertResult {
        if level == 1 {
            // Leaf level
            return self.insert_into_leaf(node_id, key);
        }

        // Internal node: find child
        let child_idx = self.internal(node_id).search(key);
        let child_id = self.internal(node_id).children[child_idx];

        let result = self.insert_recursive(child_id, key, level - 1);
        match result {
            InsertResult::Done(is_new) => {
                if is_new {
                    let n = self.internal_mut(node_id);
                    n.counts[child_idx] += 1;
                }
                InsertResult::Done(is_new)
            }
            InsertResult::Split {
                new_node,
                separator,
                is_new,
            } => {
                // Insert separator into this internal node
                if is_new {
                    let n = self.internal_mut(node_id);
                    n.counts[child_idx] += 1; // will be recomputed after insert
                }
                self.insert_into_internal(node_id, child_idx, separator, new_node, is_new)
            }
        }
    }

    fn insert_into_leaf(&mut self, leaf_id: NodeId, key: &Key) -> InsertResult {
        let leaf = self.leaf(leaf_id);
        match leaf.search(key) {
            Ok(_idx) => {
                // Duplicate (score, member) -- already exists
                InsertResult::Done(false)
            }
            Err(idx) => {
                let n = self.leaf(leaf_id).entry_count();
                if n < LEAF_CAPACITY {
                    // Room to insert
                    let leaf = self.leaf_mut(leaf_id);
                    // Shift right
                    for i in (idx..n).rev() {
                        leaf.entries[i + 1] = leaf.entries[i].clone();
                    }
                    leaf.entries[idx] = key.clone();
                    leaf.len += 1;
                    InsertResult::Done(true)
                } else {
                    // Must split
                    self.split_leaf_and_insert(leaf_id, idx, key)
                }
            }
        }
    }

    fn split_leaf_and_insert(
        &mut self,
        leaf_id: NodeId,
        insert_idx: usize,
        key: &Key,
    ) -> InsertResult {
        // Collect all entries + new one
        let old_n = self.leaf(leaf_id).entry_count();
        let mut all: Vec<Key> = Vec::with_capacity(old_n + 1);
        {
            let leaf = self.leaf(leaf_id);
            for i in 0..old_n {
                if i == insert_idx {
                    all.push(key.clone());
                }
                all.push(leaf.entries[i].clone());
            }
            if insert_idx == old_n {
                all.push(key.clone());
            }
        }

        let total = all.len();
        let left_n = total / 2;

        let new_leaf_id = self.alloc_leaf();

        // Copy right half to new leaf
        {
            let new_leaf = self.leaf_mut(new_leaf_id);
            for (i, entry) in all[left_n..].iter().enumerate() {
                new_leaf.entries[i] = entry.clone();
            }
            new_leaf.len = (total - left_n) as u16;
        }

        // Update old leaf with left half
        {
            let leaf = self.leaf_mut(leaf_id);
            for i in 0..LEAF_CAPACITY {
                if i < left_n {
                    leaf.entries[i] = all[i].clone();
                } else {
                    leaf.entries[i] = (OrderedFloat(0.0), Bytes::new());
                }
            }
            leaf.len = left_n as u16;
        }

        // Link leaves: old -> new -> old.next
        let old_next = self.leaf(leaf_id).next;
        {
            let new_leaf = self.leaf_mut(new_leaf_id);
            new_leaf.next = old_next;
            new_leaf.prev = Some(leaf_id);
        }
        {
            let leaf = self.leaf_mut(leaf_id);
            leaf.next = Some(new_leaf_id);
        }
        if let Some(next_id) = old_next {
            self.leaf_mut(next_id).prev = Some(new_leaf_id);
        } else {
            // new_leaf is the new tail
            self.leaf_tail = new_leaf_id;
        }

        let separator = self.leaf(new_leaf_id).entries[0].clone();

        InsertResult::Split {
            new_node: new_leaf_id,
            separator,
            is_new: true,
        }
    }

    fn insert_into_internal(
        &mut self,
        node_id: NodeId,
        child_idx: usize,
        separator: Key,
        new_child: NodeId,
        is_new: bool,
    ) -> InsertResult {
        let n = self.internal(node_id).key_count();
        if n < INTERNAL_FANOUT {
            // Room to insert
            let node = self.internal_mut(node_id);
            // Shift keys and children right
            for i in (child_idx..n).rev() {
                node.keys[i + 1] = node.keys[i].clone();
                node.children[i + 2] = node.children[i + 1];
                node.counts[i + 2] = node.counts[i + 1];
            }
            node.keys[child_idx] = separator;
            node.children[child_idx + 1] = new_child;
            node.len += 1;
            // Recompute counts for the two affected children
            drop(node);
            self.recompute_child_count(node_id, child_idx);
            self.recompute_child_count(node_id, child_idx + 1);
            InsertResult::Done(is_new)
        } else {
            // Split internal node
            self.split_internal_and_insert(node_id, child_idx, separator, new_child, is_new)
        }
    }

    fn split_internal_and_insert(
        &mut self,
        node_id: NodeId,
        child_idx: usize,
        separator: Key,
        new_child: NodeId,
        is_new: bool,
    ) -> InsertResult {
        // Gather all keys and children including the new one
        let old_n = self.internal(node_id).key_count();

        let mut all_keys: Vec<Key> = Vec::with_capacity(old_n + 1);
        let mut all_children: Vec<NodeId> = Vec::with_capacity(old_n + 2);

        {
            let node = self.internal(node_id);
            for i in 0..old_n {
                if i == child_idx {
                    all_keys.push(separator.clone());
                    all_children.push(node.children[i]);
                    all_children.push(new_child);
                } else {
                    all_keys.push(node.keys[i].clone());
                    if i < child_idx {
                        all_children.push(node.children[i]);
                    } else {
                        // i > child_idx
                        all_children.push(node.children[i]);
                    }
                }
            }
            if child_idx == old_n {
                all_keys.push(separator.clone());
                all_children.push(node.children[old_n]);
                all_children.push(new_child);
            } else {
                all_children.push(node.children[old_n]);
            }
        }

        let total_keys = all_keys.len(); // old_n + 1
        let mid = total_keys / 2;
        let promote_key = all_keys[mid].clone();

        // Left: keys[0..mid], children[0..mid+1]
        {
            let node = self.internal_mut(node_id);
            for i in 0..INTERNAL_FANOUT {
                if i < mid {
                    node.keys[i] = all_keys[i].clone();
                } else {
                    node.keys[i] = (OrderedFloat(0.0), Bytes::new());
                }
            }
            for i in 0..=INTERNAL_FANOUT {
                if i <= mid {
                    node.children[i] = all_children[i];
                } else {
                    node.children[i] = NIL;
                    node.counts[i] = 0;
                }
            }
            node.len = mid as u16;
        }

        // Right: keys[mid+1..], children[mid+1..]
        let new_node_id = self.alloc_internal();
        let right_key_count = total_keys - mid - 1;
        {
            let new_node = self.internal_mut(new_node_id);
            for i in 0..right_key_count {
                new_node.keys[i] = all_keys[mid + 1 + i].clone();
            }
            for i in 0..=right_key_count {
                new_node.children[i] = all_children[mid + 1 + i];
            }
            new_node.len = right_key_count as u16;
        }

        // Recompute counts
        for i in 0..=mid {
            self.recompute_child_count(node_id, i);
        }
        for i in 0..=right_key_count {
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
        match self.node(node_id) {
            Node::Leaf(l) => l.len as u32,
            Node::Internal(n) => n.total_count(),
        }
    }

    // -----------------------------------------------------------------------
    // Remove
    // -----------------------------------------------------------------------

    /// Remove entry by (score, member). Returns true if existed.
    pub fn remove(&mut self, score: OrderedFloat<f64>, member: &[u8]) -> bool {
        if self.len == 0 {
            return false;
        }
        let key = (score, Bytes::copy_from_slice(member));
        let removed = self.remove_recursive(self.root, &key, self.height);
        if removed {
            self.len -= 1;
            // Shrink root if internal with single child
            while self.height > 1 {
                if let Node::Internal(ref n) = self.nodes[self.root.0 as usize] {
                    if n.key_count() == 0 {
                        let old_root = self.root;
                        self.root = n.children[0];
                        self.free_node(old_root);
                        self.height -= 1;
                        continue;
                    }
                }
                break;
            }
        }
        removed
    }

    fn remove_recursive(&mut self, node_id: NodeId, key: &Key, level: usize) -> bool {
        if level == 1 {
            return self.remove_from_leaf(node_id, key);
        }

        let child_idx = self.internal(node_id).search(key);
        let child_id = self.internal(node_id).children[child_idx];
        let removed = self.remove_recursive(child_id, key, level - 1);

        if removed {
            self.internal_mut(node_id).counts[child_idx] -= 1;
            // Check if child is underflowing
            self.rebalance_child(node_id, child_idx, level - 1);
        }
        removed
    }

    fn remove_from_leaf(&mut self, leaf_id: NodeId, key: &Key) -> bool {
        let leaf = self.leaf(leaf_id);
        match leaf.search(key) {
            Ok(idx) => {
                let n = leaf.entry_count();
                let leaf = self.leaf_mut(leaf_id);
                for i in idx..n - 1 {
                    leaf.entries[i] = leaf.entries[i + 1].clone();
                }
                leaf.entries[n - 1] = (OrderedFloat(0.0), Bytes::new());
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

        let child_count = match self.node(child_id) {
            Node::Leaf(l) => l.entry_count(),
            Node::Internal(n) => n.key_count(),
        };

        if child_count >= min_keys {
            return; // No underflow
        }

        let parent_key_count = self.internal(parent_id).key_count();

        // Try borrow from left sibling
        if child_idx > 0 {
            let left_id = self.internal(parent_id).children[child_idx - 1];
            let left_count = match self.node(left_id) {
                Node::Leaf(l) => l.entry_count(),
                Node::Internal(n) => n.key_count(),
            };
            if left_count > min_keys {
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
            let right_count = match self.node(right_id) {
                Node::Leaf(l) => l.entry_count(),
                Node::Internal(n) => n.key_count(),
            };
            if right_count > min_keys {
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

        let left_n = self.leaf(left_id).entry_count();
        let borrowed = self.leaf(left_id).entries[left_n - 1].clone();

        // Remove from left
        self.leaf_mut(left_id).entries[left_n - 1] = (OrderedFloat(0.0), Bytes::new());
        self.leaf_mut(left_id).len -= 1;

        // Insert at front of child
        let child_n = self.leaf(child_id).entry_count();
        let child = self.leaf_mut(child_id);
        for i in (0..child_n).rev() {
            child.entries[i + 1] = child.entries[i].clone();
        }
        child.entries[0] = borrowed;
        child.len += 1;

        // Update parent separator
        let new_sep = self.leaf(child_id).entries[0].clone();
        self.internal_mut(parent_id).keys[child_idx - 1] = new_sep;

        // Update counts
        self.recompute_child_count(parent_id, child_idx - 1);
        self.recompute_child_count(parent_id, child_idx);
    }

    fn borrow_from_right_leaf(&mut self, parent_id: NodeId, child_idx: usize) {
        let right_id = self.internal(parent_id).children[child_idx + 1];
        let child_id = self.internal(parent_id).children[child_idx];

        let borrowed = self.leaf(right_id).entries[0].clone();

        // Remove from right (shift left)
        let right_n = self.leaf(right_id).entry_count();
        let right = self.leaf_mut(right_id);
        for i in 0..right_n - 1 {
            right.entries[i] = right.entries[i + 1].clone();
        }
        right.entries[right_n - 1] = (OrderedFloat(0.0), Bytes::new());
        right.len -= 1;

        // Append to child
        let child_n = self.leaf(child_id).entry_count();
        self.leaf_mut(child_id).entries[child_n] = borrowed;
        self.leaf_mut(child_id).len += 1;

        // Update parent separator
        let new_sep = self.leaf(right_id).entries[0].clone();
        self.internal_mut(parent_id).keys[child_idx] = new_sep;

        self.recompute_child_count(parent_id, child_idx);
        self.recompute_child_count(parent_id, child_idx + 1);
    }

    fn borrow_from_left_internal(&mut self, parent_id: NodeId, child_idx: usize) {
        let left_id = self.internal(parent_id).children[child_idx - 1];
        let child_id = self.internal(parent_id).children[child_idx];

        let left_n = self.internal(left_id).key_count();
        let parent_sep = self.internal(parent_id).keys[child_idx - 1].clone();
        let borrowed_key = self.internal(left_id).keys[left_n - 1].clone();
        let borrowed_child = self.internal(left_id).children[left_n];
        let borrowed_count = self.internal(left_id).counts[left_n];

        // Remove from left
        self.internal_mut(left_id).keys[left_n - 1] = (OrderedFloat(0.0), Bytes::new());
        self.internal_mut(left_id).children[left_n] = NIL;
        self.internal_mut(left_id).counts[left_n] = 0;
        self.internal_mut(left_id).len -= 1;

        // Insert at front of child
        let child_n = self.internal(child_id).key_count();
        let child = self.internal_mut(child_id);
        for i in (0..child_n).rev() {
            child.keys[i + 1] = child.keys[i].clone();
            child.children[i + 2] = child.children[i + 1];
            child.counts[i + 2] = child.counts[i + 1];
        }
        child.children[1] = child.children[0];
        child.counts[1] = child.counts[0];
        child.keys[0] = parent_sep;
        child.children[0] = borrowed_child;
        child.counts[0] = borrowed_count;
        child.len += 1;

        // Update parent separator
        self.internal_mut(parent_id).keys[child_idx - 1] = borrowed_key;

        self.recompute_child_count(parent_id, child_idx - 1);
        self.recompute_child_count(parent_id, child_idx);
    }

    fn borrow_from_right_internal(&mut self, parent_id: NodeId, child_idx: usize) {
        let right_id = self.internal(parent_id).children[child_idx + 1];
        let child_id = self.internal(parent_id).children[child_idx];

        let parent_sep = self.internal(parent_id).keys[child_idx].clone();
        let borrowed_key = self.internal(right_id).keys[0].clone();
        let borrowed_child = self.internal(right_id).children[0];
        let _borrowed_count = self.internal(right_id).counts[0];

        // Remove from right (shift left)
        let right_n = self.internal(right_id).key_count();
        let right = self.internal_mut(right_id);
        for i in 0..right_n - 1 {
            right.keys[i] = right.keys[i + 1].clone();
            right.children[i] = right.children[i + 1];
            right.counts[i] = right.counts[i + 1];
        }
        right.children[right_n - 1] = right.children[right_n];
        right.counts[right_n - 1] = right.counts[right_n];
        right.keys[right_n - 1] = (OrderedFloat(0.0), Bytes::new());
        right.children[right_n] = NIL;
        right.counts[right_n] = 0;
        right.len -= 1;

        // Append to child
        let child_n = self.internal(child_id).key_count();
        let child = self.internal_mut(child_id);
        child.keys[child_n] = parent_sep;
        child.children[child_n + 1] = borrowed_child;
        child.len += 1;

        // Update parent separator
        self.internal_mut(parent_id).keys[child_idx] = borrowed_key;

        self.recompute_child_count(parent_id, child_idx);
        self.recompute_child_count(parent_id, child_idx + 1);
    }

    /// Merge child[left_idx] and child[left_idx+1] in parent, removing separator key.
    fn merge_leaves(&mut self, parent_id: NodeId, left_idx: usize) {
        let left_id = self.internal(parent_id).children[left_idx];
        let right_id = self.internal(parent_id).children[left_idx + 1];

        let left_n = self.leaf(left_id).entry_count();
        let right_n = self.leaf(right_id).entry_count();

        // Copy right entries into left
        {
            let right_entries: Vec<Key> = (0..right_n)
                .map(|i| self.leaf(right_id).entries[i].clone())
                .collect();
            let left = self.leaf_mut(left_id);
            for (i, entry) in right_entries.into_iter().enumerate() {
                left.entries[left_n + i] = entry;
            }
            left.len = (left_n + right_n) as u16;
        }

        // Update linked list
        let right_next = self.leaf(right_id).next;
        self.leaf_mut(left_id).next = right_next;
        if let Some(next_id) = right_next {
            self.leaf_mut(next_id).prev = Some(left_id);
        } else {
            self.leaf_tail = left_id;
        }

        self.free_node(right_id);

        // Remove separator from parent
        let parent_n = self.internal(parent_id).key_count();
        let parent = self.internal_mut(parent_id);
        for i in left_idx..parent_n - 1 {
            parent.keys[i] = parent.keys[i + 1].clone();
            parent.children[i + 1] = parent.children[i + 2];
            parent.counts[i + 1] = parent.counts[i + 2];
        }
        parent.keys[parent_n - 1] = (OrderedFloat(0.0), Bytes::new());
        parent.children[parent_n] = NIL;
        parent.counts[parent_n] = 0;
        parent.len -= 1;

        self.recompute_child_count(parent_id, left_idx);
    }

    fn merge_internals(&mut self, parent_id: NodeId, left_idx: usize) {
        let left_id = self.internal(parent_id).children[left_idx];
        let right_id = self.internal(parent_id).children[left_idx + 1];

        let parent_sep = self.internal(parent_id).keys[left_idx].clone();
        let left_n = self.internal(left_id).key_count();
        let right_n = self.internal(right_id).key_count();

        // Append separator + right keys/children to left
        {
            let right_keys: Vec<Key> = (0..right_n)
                .map(|i| self.internal(right_id).keys[i].clone())
                .collect();
            let right_children: Vec<NodeId> = (0..=right_n)
                .map(|i| self.internal(right_id).children[i])
                .collect();
            let right_counts: Vec<u32> = (0..=right_n)
                .map(|i| self.internal(right_id).counts[i])
                .collect();

            let left = self.internal_mut(left_id);
            left.keys[left_n] = parent_sep;
            for (i, key) in right_keys.into_iter().enumerate() {
                left.keys[left_n + 1 + i] = key;
            }
            for (i, (child, count)) in right_children
                .into_iter()
                .zip(right_counts)
                .enumerate()
            {
                left.children[left_n + 1 + i] = child;
                left.counts[left_n + 1 + i] = count;
            }
            left.len = (left_n + 1 + right_n) as u16;
        }

        self.free_node(right_id);

        // Remove separator from parent
        let parent_n = self.internal(parent_id).key_count();
        let parent = self.internal_mut(parent_id);
        for i in left_idx..parent_n - 1 {
            parent.keys[i] = parent.keys[i + 1].clone();
            parent.children[i + 1] = parent.children[i + 2];
            parent.counts[i + 1] = parent.counts[i + 2];
        }
        parent.keys[parent_n - 1] = (OrderedFloat(0.0), Bytes::new());
        parent.children[parent_n] = NIL;
        parent.counts[parent_n] = 0;
        parent.len -= 1;

        self.recompute_child_count(parent_id, left_idx);
    }

    // -----------------------------------------------------------------------
    // Lookups
    // -----------------------------------------------------------------------

    pub fn contains(&self, score: OrderedFloat<f64>, member: &[u8]) -> bool {
        if self.len == 0 {
            return false;
        }
        let key = (score, Bytes::copy_from_slice(member));
        let leaf_id = self.find_leaf(&key);
        self.leaf(leaf_id).search(&key).is_ok()
    }

    /// Get score for a member by doing a linear scan of all leaves.
    /// For O(1) member->score lookup, use the external HashMap (like Redis).
    /// This is mainly for testing convenience.
    pub fn get_score(&self, member: &[u8]) -> Option<OrderedFloat<f64>> {
        let mut leaf = Some(self.leaf_head);
        while let Some(leaf_id) = leaf {
            let l = self.leaf(leaf_id);
            for i in 0..l.entry_count() {
                if l.entries[i].1 == member {
                    return Some(l.entries[i].0);
                }
            }
            leaf = l.next;
        }
        None
    }

    // -----------------------------------------------------------------------
    // Rank queries
    // -----------------------------------------------------------------------

    /// Return 0-based rank of (score, member) in ascending order, or None if not found.
    pub fn rank(&self, score: OrderedFloat<f64>, member: &[u8]) -> Option<usize> {
        let key = (score, Bytes::copy_from_slice(member));
        self.rank_internal(self.root, &key, self.height)
    }

    fn rank_internal(&self, node_id: NodeId, key: &Key, level: usize) -> Option<usize> {
        if level == 1 {
            // Leaf
            let leaf = self.leaf(node_id);
            match leaf.search(key) {
                Ok(idx) => Some(idx),
                Err(_) => None,
            }
        } else {
            let node = self.internal(node_id);
            let child_idx = node.search(key);
            // Sum counts of all children to the left
            let left_count: usize = (0..child_idx)
                .map(|i| node.counts[i] as usize)
                .sum();
            let child_id = node.children[child_idx];
            self.rank_internal(child_id, key, level - 1)
                .map(|r| left_count + r)
        }
    }

    /// Return 0-based rank from the end (descending order).
    pub fn rev_rank(&self, score: OrderedFloat<f64>, member: &[u8]) -> Option<usize> {
        self.rank(score, member)
            .map(|r| self.len - 1 - r)
    }

    // -----------------------------------------------------------------------
    // Index access (by rank)
    // -----------------------------------------------------------------------

    pub fn get_by_rank(&self, rank: usize) -> Option<(OrderedFloat<f64>, &Bytes)> {
        if rank >= self.len {
            return None;
        }
        self.get_by_rank_internal(self.root, rank, self.height)
    }

    fn get_by_rank_internal(
        &self,
        node_id: NodeId,
        rank: usize,
        level: usize,
    ) -> Option<(OrderedFloat<f64>, &Bytes)> {
        if level == 1 {
            let leaf = self.leaf(node_id);
            if rank < leaf.entry_count() {
                let (score, member) = &leaf.entries[rank];
                return Some((*score, member));
            }
            return None;
        }
        let node = self.internal(node_id);
        let mut remaining = rank;
        let n = node.key_count() + 1;
        for i in 0..n {
            let c = node.counts[i] as usize;
            if remaining < c {
                return self.get_by_rank_internal(node.children[i], remaining, level - 1);
            }
            remaining -= c;
        }
        None
    }

    pub fn range_by_rank(&self, start: usize, end: usize) -> Vec<(OrderedFloat<f64>, &Bytes)> {
        let mut result = Vec::new();
        let end = end.min(self.len.saturating_sub(1));
        if start > end || start >= self.len {
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
        let leaf_id = self.find_leaf(&min_key);
        let leaf = self.leaf(leaf_id);
        match leaf.search(&min_key) {
            Ok(idx) | Err(idx) => {
                if idx < leaf.entry_count() {
                    (Some(leaf_id), idx)
                } else {
                    // Move to next leaf
                    match leaf.next {
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
        let leaf_id = self.find_leaf(&max_key);
        let leaf = self.leaf(leaf_id);
        // Find the last index <= max score
        let n = leaf.entry_count();
        if n == 0 {
            // Try prev leaf
            return match leaf.prev {
                Some(prev_id) => {
                    let prev = self.leaf(prev_id);
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
            if leaf.entries[i].0 <= max {
                last_valid = Some(i);
                break;
            }
        }

        match last_valid {
            Some(idx) => (Some(leaf_id), idx),
            None => {
                // All entries in this leaf are > max, try prev
                match leaf.prev {
                    Some(prev_id) => {
                        let prev = self.leaf(prev_id);
                        let pn = prev.entry_count();
                        if pn > 0 && prev.entries[pn - 1].0 <= max {
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

    pub fn range_rev(
        &self,
        min: OrderedFloat<f64>,
        max: OrderedFloat<f64>,
    ) -> BPTreeRevIter<'_> {
        let (leaf, index) = self.find_end(max);
        BPTreeRevIter {
            tree: self,
            leaf,
            index,
            min,
        }
    }

    pub fn iter(&self) -> BPTreeIter<'_> {
        let leaf = if self.len > 0 {
            Some(self.leaf_head)
        } else {
            // Check if leaf_head has entries
            let l = self.leaf(self.leaf_head);
            if l.entry_count() > 0 {
                Some(self.leaf_head)
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
        if self.len == 0 {
            return BPTreeRevIter {
                tree: self,
                leaf: None,
                index: 0,
                min: OrderedFloat(f64::NEG_INFINITY),
            };
        }
        let tail = self.leaf_tail;
        let n = self.leaf(tail).entry_count();
        BPTreeRevIter {
            tree: self,
            leaf: Some(tail),
            index: if n > 0 { n - 1 } else { 0 },
            min: OrderedFloat(f64::NEG_INFINITY),
        }
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
            let leaf = self.tree.leaf(leaf_id);
            if self.index < leaf.entry_count() {
                let (score, member) = &leaf.entries[self.index];
                if *score > self.max {
                    self.leaf = None;
                    return None;
                }
                self.index += 1;
                return Some((*score, member));
            }
            // Move to next leaf
            self.leaf = leaf.next;
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
        loop {
            let leaf_id = self.leaf?;
            let leaf = self.tree.leaf(leaf_id);
            let n = leaf.entry_count();
            if n == 0 {
                self.leaf = None;
                return None;
            }
            if self.index < n {
                let (score, member) = &leaf.entries[self.index];
                if *score < self.min {
                    self.leaf = None;
                    return None;
                }
                if self.index == 0 {
                    // Move to prev leaf
                    self.leaf = leaf.prev;
                    if let Some(prev_id) = self.leaf {
                        let prev = self.tree.leaf(prev_id);
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
            return None;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
        tree.insert(OrderedFloat(3.14), Bytes::from("pi"));
        tree.insert(OrderedFloat(2.72), Bytes::from("e"));
        assert_eq!(tree.get_score(b"pi"), Some(OrderedFloat(3.14)));
        assert_eq!(tree.get_score(b"e"), Some(OrderedFloat(2.72)));
        assert_eq!(tree.get_score(b"missing"), None);
    }

    #[test]
    fn test_range_ascending() {
        let mut tree = BPTree::new();
        for i in 0..20 {
            tree.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
        }
        let results: Vec<_> = tree
            .range(OrderedFloat(5.0), OrderedFloat(10.0))
            .collect();
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
                "missing entry at {}", i
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
                "failed to remove {}", i
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
        assert_eq!(all[0].0 .0, f64::NEG_INFINITY);
        assert_eq!(all[1].0 .0, 0.0);
        assert_eq!(all[2].0 .0, f64::INFINITY);
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
            assert_eq!(all[i].0 .0, i as f64);
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
            assert_eq!(all[i].0 .0, (29 - i) as f64);
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
        assert_eq!(tree.get_by_rank(0).unwrap().0 .0, 5.0);

        let range: Vec<_> = tree.range(OrderedFloat(0.0), OrderedFloat(10.0)).collect();
        assert_eq!(range.len(), 1);
    }
}
