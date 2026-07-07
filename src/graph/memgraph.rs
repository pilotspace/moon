//! MemGraph -- mutable adjacency-list write buffer with stable external ids.
//!
//! Absorbs graph writes at O(1) amortized cost per insert. Freezes into a
//! `FrozenMemGraph` when the edge threshold is reached, enabling CSR conversion.
//!
//! Storage is `FxHashMap` keyed by monotonically allocated ids (in slotmap
//! ffi encoding) rather than a `SlotMap`: WAL replay must re-materialize
//! nodes/edges under the EXACT ids that were logged, and slot maps cannot
//! insert at a chosen (index, generation) pair. Insertion order is kept in
//! side vectors so iteration and freeze remain deterministic.

use crate::graph::fasthash::FxHashMap;
use smallvec::SmallVec;

use crate::graph::types::{Direction, EdgeKey, MutableEdge, MutableNode, NodeKey, PropertyMap};

/// First id handed out by a fresh MemGraph: index 1, version 1.
///
/// Ids live in slotmap's ffi encoding (`version << 32 | index`) so they
/// round-trip through `KeyData::from_ffi`/`as_ffi` unchanged. slotmap 1.1
/// forces the version odd on `from_ffi` (`(value >> 32) | 1`), so allocation
/// stays in odd-version space; index 0 is skipped to avoid the encoding's
/// degenerate all-zero key.
const FIRST_ID: u64 = (1 << 32) | 1;

/// Next id after `id` in odd-version ffi space. Rolls the 32-bit index over
/// into `version + 2` (stays odd) before it can reach `u32::MAX` (slotmap's
/// null-key index).
#[inline]
const fn next_id(id: u64) -> u64 {
    if id as u32 >= u32::MAX - 1 {
        ((id >> 32) + 2) << 32 | 1
    } else {
        id + 1
    }
}

/// Errors returned by MemGraph operations.
#[derive(Debug, PartialEq, Eq)]
pub enum GraphError {
    /// Referenced node does not exist or has been deleted.
    NodeNotFound,
    /// MemGraph has already been frozen.
    AlreadyFrozen,
    /// Self-loops are not allowed.
    SelfLoop,
}

/// Frozen snapshot of a MemGraph, consumed by CSR conversion.
#[derive(Debug)]
pub struct FrozenMemGraph {
    pub nodes: Vec<(NodeKey, MutableNode)>,
    pub edges: Vec<(EdgeKey, MutableEdge)>,
}

/// Mutable graph segment keyed by stable, monotonically allocated ids.
#[derive(Debug)]
pub struct MemGraph {
    nodes: FxHashMap<NodeKey, MutableNode>,
    edges: FxHashMap<EdgeKey, MutableEdge>,
    /// Insertion order of live-at-insert node keys (deterministic iteration
    /// and freeze). May contain keys later removed from `nodes`.
    node_order: Vec<NodeKey>,
    /// Insertion order of edge keys (same contract as `node_order`).
    edge_order: Vec<EdgeKey>,
    /// Next node id to hand out (slotmap ffi encoding, odd version).
    /// Monotonic across freeze/thaw — never reset, so post-freeze inserts can
    /// never alias the external_ids of frozen CSR rows.
    next_node_id: u64,
    /// Next edge id to hand out (same encoding and monotonicity contract).
    next_edge_id: u64,
    /// Adjacency (outgoing / incoming) for cross-tier "delta" edges whose
    /// endpoint was frozen into a CSR segment. A frozen endpoint has no
    /// `MutableNode` to carry inline adjacency, so its edge keys live here,
    /// keyed by the frozen NodeKey. Rebuilt at each freeze (see `freeze`).
    ghost_out: std::collections::HashMap<NodeKey, SmallVec<[EdgeKey; 4]>>,
    ghost_in: std::collections::HashMap<NodeKey, SmallVec<[EdgeKey; 4]>>,
    /// Count of live (non-deleted) nodes.
    live_node_count: usize,
    /// Count of live (non-deleted) edges.
    live_edge_count: usize,
    /// Edge count threshold that triggers freeze.
    edge_threshold: usize,
    frozen: bool,
}

impl MemGraph {
    /// Create an empty MemGraph with the given freeze threshold.
    pub fn new(edge_threshold: usize) -> Self {
        Self {
            nodes: FxHashMap::default(),
            edges: FxHashMap::default(),
            node_order: Vec::new(),
            edge_order: Vec::new(),
            next_node_id: FIRST_ID,
            next_edge_id: FIRST_ID,
            ghost_out: std::collections::HashMap::new(),
            ghost_in: std::collections::HashMap::new(),
            live_node_count: 0,
            live_edge_count: 0,
            edge_threshold,
            frozen: false,
        }
    }

    /// Insert a new node under a freshly allocated stable id.
    pub fn add_node(
        &mut self,
        labels: SmallVec<[u16; 4]>,
        properties: PropertyMap,
        embedding: Option<Vec<f32>>,
        lsn: u64,
    ) -> NodeKey {
        let id = self.next_node_id;
        self.next_node_id = next_id(id);
        self.insert_node_at(
            NodeKey::from(slotmap::KeyData::from_ffi(id)),
            labels,
            properties,
            embedding,
            lsn,
        )
    }

    /// Insert a new node under a CALLER-CHOSEN id (WAL replay: the id that
    /// was logged at original execution). Bumps the allocation floor past
    /// `node_id` so later `add_node` calls can never alias it. Replaces any
    /// existing entry under the same id (replay is authoritative).
    pub fn add_node_with_id(
        &mut self,
        node_id: u64,
        labels: SmallVec<[u16; 4]>,
        properties: PropertyMap,
        embedding: Option<Vec<f32>>,
        lsn: u64,
    ) -> NodeKey {
        self.ensure_node_id_floor(node_id);
        self.insert_node_at(
            NodeKey::from(slotmap::KeyData::from_ffi(node_id)),
            labels,
            properties,
            embedding,
            lsn,
        )
    }

    /// Raise the node-id allocation floor above `node_id` (no-op if already
    /// above). Used when frozen CSR external_ids are re-seeded at recovery.
    pub fn ensure_node_id_floor(&mut self, node_id: u64) {
        if node_id >= self.next_node_id {
            self.next_node_id = next_id(node_id);
        }
    }

    /// Raise the edge-id allocation floor above `edge_id`.
    pub fn ensure_edge_id_floor(&mut self, edge_id: u64) {
        if edge_id >= self.next_edge_id {
            self.next_edge_id = next_id(edge_id);
        }
    }

    /// Current allocation cursors `(next_node_id, next_edge_id)` — persisted
    /// in the graph manifest so a restart resumes allocation past every id
    /// ever handed out (WAL-truncation-safe).
    pub fn id_cursors(&self) -> (u64, u64) {
        (self.next_node_id, self.next_edge_id)
    }

    /// Restore persisted allocation cursors. Values are `next_*` cursors
    /// (NOT handed-out ids — see `ensure_*_id_floor` for those). Only ever
    /// raises; `0` (pre-cursor manifest) is a no-op.
    pub fn restore_id_cursors(&mut self, next_node_id: u64, next_edge_id: u64) {
        if next_node_id > self.next_node_id {
            self.next_node_id = next_node_id;
        }
        if next_edge_id > self.next_edge_id {
            self.next_edge_id = next_edge_id;
        }
    }

    fn insert_node_at(
        &mut self,
        key: NodeKey,
        labels: SmallVec<[u16; 4]>,
        properties: PropertyMap,
        embedding: Option<Vec<f32>>,
        lsn: u64,
    ) -> NodeKey {
        let prev = self.nodes.insert(
            key,
            MutableNode {
                labels,
                outgoing: SmallVec::new(),
                incoming: SmallVec::new(),
                properties,
                embedding,
                created_lsn: lsn,
                deleted_lsn: u64::MAX,
                txn_id: 0,
                valid_from: 0,
                valid_to: i64::MAX,
            },
        );
        match prev {
            Some(old) => {
                // Replaced-in-place (replay overwrite): order entry already
                // present; only fix the live count if the old entry was dead.
                if old.deleted_lsn != u64::MAX {
                    self.live_node_count += 1;
                }
            }
            None => {
                self.node_order.push(key);
                self.live_node_count += 1;
            }
        }
        key
    }

    /// Insert a new edge between `src` and `dst`. Validates both exist and are alive.
    pub fn add_edge(
        &mut self,
        src: NodeKey,
        dst: NodeKey,
        edge_type: u16,
        weight: f64,
        properties: Option<PropertyMap>,
        lsn: u64,
    ) -> Result<EdgeKey, GraphError> {
        if self.frozen {
            return Err(GraphError::AlreadyFrozen);
        }
        if src == dst {
            return Err(GraphError::SelfLoop);
        }
        // Validate both nodes exist and are alive.
        let src_alive = self
            .nodes
            .get(&src)
            .map_or(false, |n| n.deleted_lsn == u64::MAX);
        let dst_alive = self
            .nodes
            .get(&dst)
            .map_or(false, |n| n.deleted_lsn == u64::MAX);
        if !src_alive || !dst_alive {
            return Err(GraphError::NodeNotFound);
        }

        let ek = self.insert_edge_fresh(MutableEdge {
            src,
            dst,
            edge_type,
            weight,
            properties,
            created_lsn: lsn,
            deleted_lsn: u64::MAX,
            txn_id: 0,
            valid_from: 0,
            valid_to: i64::MAX,
            // Shard-cached clock read (1ms tick) -- no syscall on the insert
            // path. Powers temporal-decay traversal scoring.
            created_ms: crate::storage::entry::current_time_ms(),
        });

        // Push edge key into src.outgoing and dst.incoming.
        // Both are validated alive above, so get_mut is safe.
        if let Some(src_node) = self.nodes.get_mut(&src) {
            src_node.outgoing.push(ek);
        }
        if let Some(dst_node) = self.nodes.get_mut(&dst) {
            dst_node.incoming.push(ek);
        }
        self.live_edge_count += 1;
        Ok(ek)
    }

    /// Insert an edge under a freshly allocated stable id and record its
    /// insertion order. Does NOT touch adjacency or live counts.
    fn insert_edge_fresh(&mut self, edge: MutableEdge) -> EdgeKey {
        let id = self.next_edge_id;
        self.next_edge_id = next_id(id);
        let ek = EdgeKey::from(slotmap::KeyData::from_ffi(id));
        self.edges.insert(ek, edge);
        self.edge_order.push(ek);
        ek
    }

    /// Insert an edge whose endpoints may live in the frozen CSR tier
    /// (a "delta" edge). Resident endpoints are validated alive and get
    /// inline adjacency; non-resident endpoints get ghost adjacency.
    ///
    /// The CALLER is responsible for having verified that each non-resident
    /// endpoint exists and is alive in an immutable segment (e.g. via
    /// `MergedNodeView::is_visible`) — MemGraph cannot see the CSR tier.
    pub fn add_edge_across_tiers(
        &mut self,
        src: NodeKey,
        dst: NodeKey,
        edge_type: u16,
        weight: f64,
        properties: Option<PropertyMap>,
        lsn: u64,
    ) -> Result<EdgeKey, GraphError> {
        self.add_edge_across_tiers_at(None, src, dst, edge_type, weight, properties, lsn)
    }

    /// `add_edge_across_tiers` under a CALLER-CHOSEN edge id (WAL replay).
    /// Bumps the edge-id allocation floor past `edge_id`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_edge_across_tiers_with_id(
        &mut self,
        edge_id: u64,
        src: NodeKey,
        dst: NodeKey,
        edge_type: u16,
        weight: f64,
        properties: Option<PropertyMap>,
        lsn: u64,
    ) -> Result<EdgeKey, GraphError> {
        self.ensure_edge_id_floor(edge_id);
        let ek = EdgeKey::from(slotmap::KeyData::from_ffi(edge_id));
        self.add_edge_across_tiers_at(Some(ek), src, dst, edge_type, weight, properties, lsn)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_edge_across_tiers_at(
        &mut self,
        chosen: Option<EdgeKey>,
        src: NodeKey,
        dst: NodeKey,
        edge_type: u16,
        weight: f64,
        properties: Option<PropertyMap>,
        lsn: u64,
    ) -> Result<EdgeKey, GraphError> {
        if self.frozen {
            return Err(GraphError::AlreadyFrozen);
        }
        if src == dst {
            return Err(GraphError::SelfLoop);
        }
        // Resident endpoints must be alive; non-resident are caller-verified.
        for key in [src, dst] {
            if let Some(n) = self.nodes.get(&key) {
                if n.deleted_lsn != u64::MAX {
                    return Err(GraphError::NodeNotFound);
                }
            }
        }

        let edge = MutableEdge {
            src,
            dst,
            edge_type,
            weight,
            properties,
            created_lsn: lsn,
            deleted_lsn: u64::MAX,
            txn_id: 0,
            valid_from: 0,
            valid_to: i64::MAX,
            created_ms: crate::storage::entry::current_time_ms(),
        };
        let ek = match chosen {
            Some(ek) => {
                match self.edges.insert(ek, edge) {
                    None => self.edge_order.push(ek),
                    Some(old) => {
                        // Replay overwrite of an identical logged edge id:
                        // adjacency already points at `ek`; keep counts
                        // consistent (the +1 below re-adds a live entry).
                        if old.deleted_lsn == u64::MAX {
                            self.live_edge_count = self.live_edge_count.saturating_sub(1);
                        }
                        // Skip the adjacency pushes — already linked.
                        self.live_edge_count += 1;
                        return Ok(ek);
                    }
                }
                ek
            }
            None => self.insert_edge_fresh(edge),
        };

        match self.nodes.get_mut(&src) {
            Some(src_node) => src_node.outgoing.push(ek),
            None => self.ghost_out.entry(src).or_default().push(ek),
        }
        match self.nodes.get_mut(&dst) {
            Some(dst_node) => dst_node.incoming.push(ek),
            None => self.ghost_in.entry(dst).or_default().push(ek),
        }
        self.live_edge_count += 1;
        Ok(ek)
    }

    /// Soft-delete a node and all its incident edges.
    pub fn remove_node(&mut self, key: NodeKey, lsn: u64) -> bool {
        let Some(node) = self.nodes.get_mut(&key) else {
            return false;
        };
        if node.deleted_lsn != u64::MAX {
            return false; // already deleted
        }
        node.deleted_lsn = lsn;
        self.live_node_count = self.live_node_count.saturating_sub(1);

        // Collect incident edge keys (both outgoing and incoming).
        let edge_keys: SmallVec<[EdgeKey; 16]> = node
            .outgoing
            .iter()
            .chain(node.incoming.iter())
            .copied()
            .collect();

        // Soft-delete all incident edges.
        for ek in edge_keys {
            if let Some(edge) = self.edges.get_mut(&ek) {
                if edge.deleted_lsn == u64::MAX {
                    edge.deleted_lsn = lsn;
                    self.live_edge_count = self.live_edge_count.saturating_sub(1);
                }
            }
        }
        true
    }

    /// Soft-delete a single edge.
    pub fn remove_edge(&mut self, key: EdgeKey, lsn: u64) -> bool {
        let Some(edge) = self.edges.get_mut(&key) else {
            return false;
        };
        if edge.deleted_lsn != u64::MAX {
            return false; // already deleted
        }
        edge.deleted_lsn = lsn;
        self.live_edge_count = self.live_edge_count.saturating_sub(1);
        true
    }

    /// Remove an edge by its external u64 id (used during WAL replay).
    /// Reconstructs the EdgeKey from the ffi representation and delegates
    /// to `remove_edge`.
    pub fn remove_edge_by_id(&mut self, edge_id: u64, lsn: u64) -> bool {
        let key_data = slotmap::KeyData::from_ffi(edge_id);
        let edge_key = EdgeKey::from(key_data);
        self.remove_edge(edge_key, lsn)
    }

    /// O(1) node lookup by key.
    pub fn get_node(&self, key: NodeKey) -> Option<&MutableNode> {
        self.nodes.get(&key)
    }

    /// O(1) mutable node lookup by key.
    pub fn get_node_mut(&mut self, key: NodeKey) -> Option<&mut MutableNode> {
        self.nodes.get_mut(&key)
    }

    /// O(1) edge lookup by key.
    pub fn get_edge(&self, key: EdgeKey) -> Option<&MutableEdge> {
        self.edges.get(&key)
    }

    /// O(1) mutable edge lookup by key.
    pub fn get_edge_mut(&mut self, key: EdgeKey) -> Option<&mut MutableEdge> {
        self.edges.get_mut(&key)
    }

    /// Returns neighbors of `node` visible at the given `lsn`, filtered by direction.
    ///
    /// Yields `(EdgeKey, NodeKey)` pairs -- the edge and the neighbor node.
    /// No heap allocation: iterates over borrowed SmallVec adjacency lists.
    pub fn neighbors(&self, node: NodeKey, direction: Direction, lsn: u64) -> NeighborIter<'_> {
        let Some(n) = self.nodes.get(&node) else {
            // Non-resident (frozen) node: serve delta-edge adjacency from the
            // ghost maps so cross-tier edges are traversable from BOTH ends.
            let ghost_out = match direction {
                Direction::Outgoing | Direction::Both => self
                    .ghost_out
                    .get(&node)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]),
                Direction::Incoming => &[],
            };
            let ghost_in = match direction {
                Direction::Incoming | Direction::Both => self
                    .ghost_in
                    .get(&node)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]),
                Direction::Outgoing => &[],
            };
            return NeighborIter {
                edges: &self.edges,
                out_iter: ghost_out.iter(),
                in_iter: ghost_in.iter(),
                lsn,
                source: node,
            };
        };

        let (out_slice, in_slice) = match direction {
            Direction::Outgoing => (n.outgoing.as_slice(), &[][..]),
            Direction::Incoming => (&[][..], n.incoming.as_slice()),
            Direction::Both => (n.outgoing.as_slice(), n.incoming.as_slice()),
        };

        NeighborIter {
            edges: &self.edges,
            out_iter: out_slice.iter(),
            in_iter: in_slice.iter(),
            lsn,
            source: node,
        }
    }

    /// Iterate over all live (non-deleted) nodes in insertion order.
    /// Yields `(NodeKey, &MutableNode)`.
    pub fn iter_nodes(&self) -> impl Iterator<Item = (NodeKey, &MutableNode)> {
        self.node_order
            .iter()
            .filter_map(move |k| self.nodes.get(k).map(|n| (*k, n)))
            .filter(|(_, n)| n.deleted_lsn == u64::MAX)
    }

    /// Iterate over all live (non-deleted) edges in insertion order.
    /// Yields `(EdgeKey, &MutableEdge)`.
    pub fn iter_edges(&self) -> impl Iterator<Item = (EdgeKey, &MutableEdge)> {
        self.edge_order
            .iter()
            .filter_map(move |k| self.edges.get(k).map(|e| (*k, e)))
            .filter(|(_, e)| e.deleted_lsn == u64::MAX)
    }

    /// Iterate over all soft-deleted nodes in insertion order (copy-up
    /// tombstone bookkeeping at freeze time).
    pub fn iter_dead_nodes(&self) -> impl Iterator<Item = (NodeKey, &MutableNode)> {
        self.node_order
            .iter()
            .filter_map(move |k| self.nodes.get(k).map(|n| (*k, n)))
            .filter(|(_, n)| n.deleted_lsn != u64::MAX)
    }

    /// Number of live (non-deleted) nodes. O(1) via maintained counter.
    pub fn node_count(&self) -> usize {
        self.live_node_count
    }

    /// Number of live (non-deleted) edges.
    pub fn edge_count(&self) -> usize {
        self.live_edge_count
    }

    /// Phase 174 FIX-01: Increment live node count (used by TXN.ABORT
    /// un-soft-delete to restore the count decremented by `remove_node`).
    #[inline]
    pub fn inc_live_node_count(&mut self) {
        self.live_node_count += 1;
    }

    /// Phase 174 FIX-01: Increment live edge count (used by TXN.ABORT
    /// un-soft-delete to restore the count decremented by `remove_edge`).
    #[inline]
    pub fn inc_live_edge_count(&mut self) {
        self.live_edge_count += 1;
    }

    /// Phase 174 FIX-01: Un-soft-delete all incident edges of `node` that
    /// were cascade-deleted at `lsn` by `remove_node`. Restores `deleted_lsn`
    /// to `u64::MAX` and increments `live_edge_count` for each restored edge.
    pub fn undelete_edges_at_lsn(&mut self, node: NodeKey, lsn: u64) {
        let Some(n) = self.nodes.get(&node) else {
            return;
        };
        let edge_keys: SmallVec<[EdgeKey; 16]> = n
            .outgoing
            .iter()
            .chain(n.incoming.iter())
            .copied()
            .collect();
        for ek in edge_keys {
            if let Some(edge) = self.edges.get_mut(&ek) {
                if edge.deleted_lsn == lsn {
                    edge.deleted_lsn = u64::MAX;
                    self.live_edge_count += 1;
                }
            }
        }
    }

    /// Resident bytes used by the in-memory adjacency maps (nodes + edges).
    /// Approximation based on map capacity and struct sizes.
    pub fn resident_bytes(&self) -> usize {
        let node_bytes = self.nodes.capacity()
            * (std::mem::size_of::<MutableNode>() + std::mem::size_of::<NodeKey>())
            + self.node_order.capacity() * std::mem::size_of::<NodeKey>();
        let edge_bytes = self.edges.capacity()
            * (std::mem::size_of::<MutableEdge>() + std::mem::size_of::<EdgeKey>())
            + self.edge_order.capacity() * std::mem::size_of::<EdgeKey>();
        node_bytes + edge_bytes
    }

    /// Whether the MemGraph should be frozen (threshold reached).
    pub fn should_freeze(&self) -> bool {
        self.live_edge_count >= self.edge_threshold && !self.frozen
    }

    /// Freeze the MemGraph, returning a FrozenMemGraph with all data for CSR conversion.
    /// Only includes live (non-deleted) nodes and edges.
    ///
    /// Cross-tier "delta" edges — edges with at least one endpoint that was
    /// frozen into an EARLIER segment — are RETAINED in the mutable tier
    /// (with their EdgeKeys intact) instead of being handed to CSR
    /// conversion: `CsrSegment::from_frozen` can only encode edges whose
    /// endpoint rows exist in the segment being built, and would silently
    /// drop them. Their ghost adjacency is rebuilt for the post-freeze world
    /// (every endpoint is non-resident once the nodes drain).
    pub fn freeze(&mut self) -> Result<FrozenMemGraph, GraphError> {
        if self.frozen {
            return Err(GraphError::AlreadyFrozen);
        }
        self.frozen = true;

        // Partition edges BEFORE draining nodes (residency check needs the
        // node map): live + both endpoints resident → freeze into CSR;
        // live + any frozen-elsewhere endpoint → retain as delta;
        // dead → drop. Insertion order preserved via edge_order.
        let mut freeze_keys: Vec<EdgeKey> = Vec::new();
        let mut dead_keys: Vec<EdgeKey> = Vec::new();
        for ek in &self.edge_order {
            let Some(e) = self.edges.get(ek) else {
                continue;
            };
            if e.deleted_lsn != u64::MAX {
                dead_keys.push(*ek);
            } else if self.nodes.contains_key(&e.src) && self.nodes.contains_key(&e.dst) {
                freeze_keys.push(*ek);
            }
        }
        for ek in dead_keys {
            self.edges.remove(&ek);
        }
        let edges: Vec<(EdgeKey, MutableEdge)> = freeze_keys
            .into_iter()
            .filter_map(|ek| self.edges.remove(&ek).map(|e| (ek, e)))
            .collect();
        self.edge_order.retain(|ek| self.edges.contains_key(ek));

        let mut nodes: Vec<(NodeKey, MutableNode)> = Vec::with_capacity(self.nodes.len());
        for nk in self.node_order.drain(..) {
            if let Some(n) = self.nodes.remove(&nk) {
                if n.deleted_lsn == u64::MAX {
                    nodes.push((nk, n));
                }
            }
        }

        // Rebuild ghost adjacency for the retained delta edges: with the
        // node map drained, EVERY endpoint is now non-resident.
        self.ghost_out.clear();
        self.ghost_in.clear();
        for ek in &self.edge_order {
            if let Some(e) = self.edges.get(ek) {
                self.ghost_out.entry(e.src).or_default().push(*ek);
                self.ghost_in.entry(e.dst).or_default().push(*ek);
            }
        }

        Ok(FrozenMemGraph { nodes, edges })
    }

    /// Re-arm a drained MemGraph for writes after a successful freeze.
    ///
    /// The monotonic id counters are NOT reset, so keys handed out after
    /// thaw can never collide with the external_ids of frozen CSR rows.
    /// (Replacing the MemGraph with a fresh one instead would restart
    /// allocation at FIRST_ID and silently alias new nodes onto frozen rows.)
    pub fn thaw(&mut self) {
        self.frozen = false;
        // Post-freeze contents: zero nodes, retained delta edges (all live —
        // freeze removed dead ones).
        self.live_node_count = 0;
        self.live_edge_count = self.edges.len();
    }
}

/// Zero-allocation neighbor iterator. Borrows from MemGraph's SmallVec adjacency lists.
pub struct NeighborIter<'a> {
    edges: &'a FxHashMap<EdgeKey, MutableEdge>,
    out_iter: core::slice::Iter<'a, EdgeKey>,
    in_iter: core::slice::Iter<'a, EdgeKey>,
    lsn: u64,
    /// The source node (retained for future direction-aware queries).
    #[allow(dead_code)]
    source: NodeKey,
}

impl<'a> Iterator for NeighborIter<'a> {
    type Item = (EdgeKey, NodeKey);

    fn next(&mut self) -> Option<Self::Item> {
        // Process outgoing edges first, then incoming.
        // Visibility rule: edge is visible at `lsn` if:
        //   created_lsn <= lsn  AND  deleted_lsn > lsn
        // Special case: lsn == u64::MAX means "see all live edges".
        // Live edges have deleted_lsn = u64::MAX, so for lsn = u64::MAX
        // we check deleted_lsn == u64::MAX (alive) instead of deleted_lsn > lsn
        // (which would be false since nothing is > u64::MAX).
        let is_visible = |edge: &MutableEdge| -> bool {
            if edge.created_lsn > self.lsn {
                return false;
            }
            if self.lsn == u64::MAX {
                // "See everything alive" — only filter out deleted edges.
                edge.deleted_lsn == u64::MAX
            } else {
                edge.deleted_lsn > self.lsn
            }
        };
        loop {
            if let Some(&ek) = self.out_iter.next() {
                if let Some(edge) = self.edges.get(&ek) {
                    if is_visible(edge) {
                        return Some((ek, edge.dst));
                    }
                }
                continue;
            }
            if let Some(&ek) = self.in_iter.next() {
                if let Some(edge) = self.edges.get(&ek) {
                    if is_visible(edge) {
                        return Some((ek, edge.src));
                    }
                }
                continue;
            }
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn empty_props() -> PropertyMap {
        SmallVec::new()
    }

    #[test]
    fn test_insert_node_and_retrieve() {
        let mut g = MemGraph::new(1000);
        let nk = g.add_node(smallvec![1, 2], empty_props(), None, 1);
        let node = g.get_node(nk).expect("node should exist");
        assert_eq!(node.labels.as_slice(), &[1, 2]);
        assert_eq!(node.created_lsn, 1);
        assert_eq!(node.deleted_lsn, u64::MAX);
    }

    use crate::storage::entry::ClockPin;

    #[test]
    fn test_add_edge_stamps_created_ms_from_cached_clock() {
        let _pin = ClockPin::set(5, 5_000);
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        let ek = g.add_edge(a, b, 1, 1.0, None, 2).expect("edge ok");

        let edge = g.get_edge(ek).expect("edge should exist");
        assert_eq!(
            edge.created_ms, 5_000,
            "add_edge must stamp created_ms from the shard-cached clock"
        );
    }

    #[test]
    fn test_insert_edge_and_adjacency() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        let ek = g.add_edge(a, b, 1, 1.0, None, 2).expect("edge ok");

        let edge = g.get_edge(ek).expect("edge should exist");
        assert_eq!(edge.src, a);
        assert_eq!(edge.dst, b);
        assert_eq!(g.edge_count(), 1);

        // Verify adjacency via neighbors
        let out: Vec<_> = g.neighbors(a, Direction::Outgoing, 10).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], (ek, b));

        let inc: Vec<_> = g.neighbors(b, Direction::Incoming, 10).collect();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0], (ek, a));
    }

    #[test]
    fn test_soft_delete_node_cascades() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        let c = g.add_node(smallvec![0], empty_props(), None, 1);
        g.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        g.add_edge(a, c, 1, 1.0, None, 2).expect("ok");
        assert_eq!(g.edge_count(), 2);

        g.remove_node(a, 5);
        assert_eq!(g.edge_count(), 0);
        assert_eq!(g.node_count(), 2); // b and c still alive

        // Deleted node should still be returned by get_node (soft-deleted).
        let node = g.get_node(a).expect("still in slotmap");
        assert_eq!(node.deleted_lsn, 5);
    }

    #[test]
    fn test_neighbors_respect_lsn_visibility() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        let c = g.add_node(smallvec![0], empty_props(), None, 1);

        // Edge a->b at lsn 5
        g.add_edge(a, b, 1, 1.0, None, 5).expect("ok");
        // Edge a->c at lsn 10
        g.add_edge(a, c, 1, 1.0, None, 10).expect("ok");

        // At lsn 7, only a->b is visible.
        let neighbors_at_7: Vec<_> = g.neighbors(a, Direction::Outgoing, 7).collect();
        assert_eq!(neighbors_at_7.len(), 1);

        // At lsn 15, both are visible.
        let neighbors_at_15: Vec<_> = g.neighbors(a, Direction::Outgoing, 15).collect();
        assert_eq!(neighbors_at_15.len(), 2);
    }

    #[test]
    fn test_should_freeze_at_threshold() {
        let mut g = MemGraph::new(3);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        let c = g.add_node(smallvec![0], empty_props(), None, 1);
        let d = g.add_node(smallvec![0], empty_props(), None, 1);

        assert!(!g.should_freeze());
        g.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        g.add_edge(a, c, 1, 1.0, None, 2).expect("ok");
        assert!(!g.should_freeze());
        g.add_edge(a, d, 1, 1.0, None, 2).expect("ok");
        assert!(g.should_freeze());
    }

    #[test]
    fn test_freeze_returns_live_data() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        let c = g.add_node(smallvec![0], empty_props(), None, 1);
        g.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        g.add_edge(a, c, 1, 1.0, None, 2).expect("ok");

        // Delete node c (and its incident edge)
        g.remove_node(c, 5);

        let frozen = g.freeze().expect("freeze ok");
        assert_eq!(frozen.nodes.len(), 2); // a and b
        assert_eq!(frozen.edges.len(), 1); // only a->b

        // Double freeze should fail.
        assert_eq!(g.freeze().unwrap_err(), GraphError::AlreadyFrozen);
    }

    #[test]
    fn test_delta_edge_across_freeze_traversable_both_ends() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        g.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        let frozen = g.freeze().expect("freeze");
        assert_eq!(frozen.nodes.len(), 2);
        g.thaw();

        // Plain add_edge between frozen endpoints must still refuse (caller
        // hasn't verified segment existence)...
        assert_eq!(
            g.add_edge(a, b, 2, 1.0, None, 3).unwrap_err(),
            GraphError::NodeNotFound
        );
        // ...while the cross-tier insert succeeds and is traversable from
        // BOTH frozen endpoints via ghost adjacency.
        let ek = g
            .add_edge_across_tiers(a, b, 2, 1.0, None, 3)
            .expect("delta edge");
        let out: Vec<_> = g.neighbors(a, Direction::Outgoing, u64::MAX).collect();
        assert_eq!(out, vec![(ek, b)]);
        let inc: Vec<_> = g.neighbors(b, Direction::Incoming, u64::MAX).collect();
        assert_eq!(inc, vec![(ek, a)]);
        assert!(
            g.neighbors(a, Direction::Incoming, u64::MAX)
                .next()
                .is_none()
        );
    }

    #[test]
    fn test_freeze_retains_delta_edges_with_stable_keys() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        g.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        g.freeze().expect("freeze 1");
        g.thaw();

        // Delta edge between frozen endpoints + a fresh resident pair.
        let ek_delta = g
            .add_edge_across_tiers(a, b, 2, 1.5, None, 3)
            .expect("delta");
        let c = g.add_node(smallvec![0], empty_props(), None, 4);
        let d = g.add_node(smallvec![0], empty_props(), None, 4);
        let ek_res = g.add_edge(c, d, 1, 1.0, None, 5).expect("resident");

        // Second freeze: the resident pair + edge freezes; the delta edge is
        // RETAINED (CSR cannot host an edge without its endpoint rows) with
        // the SAME EdgeKey, and stays traversable.
        let frozen = g.freeze().expect("freeze 2");
        g.thaw();
        assert_eq!(frozen.nodes.len(), 2, "c and d freeze");
        assert_eq!(frozen.edges.len(), 1, "only the resident edge freezes");
        assert_eq!(frozen.edges[0].0, ek_res);

        assert!(g.get_edge(ek_delta).is_some(), "delta edge key stable");
        let out: Vec<_> = g.neighbors(a, Direction::Outgoing, u64::MAX).collect();
        assert_eq!(out, vec![(ek_delta, b)]);
        // c/d froze normally: no delta adjacency left behind for them.
        assert!(g.neighbors(c, Direction::Both, u64::MAX).next().is_none());
        assert_eq!(g.edge_count(), 1, "live count = retained delta edge");
    }

    #[test]
    fn test_self_loop_rejected() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        assert_eq!(
            g.add_edge(a, a, 1, 1.0, None, 2).unwrap_err(),
            GraphError::SelfLoop
        );
    }

    #[test]
    fn test_edge_to_nonexistent_node() {
        let mut g = MemGraph::new(1000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        // Create a fake NodeKey by removing a node.
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        g.remove_node(b, 2);
        assert_eq!(
            g.add_edge(a, b, 1, 1.0, None, 3).unwrap_err(),
            GraphError::NodeNotFound
        );
    }
}
