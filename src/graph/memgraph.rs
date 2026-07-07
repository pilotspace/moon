//! MemGraph -- mutable adjacency-list write buffer backed by SlotMap.
//!
//! Absorbs graph writes at O(1) amortized cost per insert. Freezes into a
//! `FrozenMemGraph` when the edge threshold is reached, enabling CSR conversion.

use slotmap::{Key, SlotMap};
use smallvec::SmallVec;

use crate::graph::types::{Direction, EdgeKey, MutableEdge, MutableNode, NodeKey, PropertyMap};

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

/// Mutable graph segment backed by generational SlotMap indices.
#[derive(Debug)]
pub struct MemGraph {
    nodes: SlotMap<NodeKey, MutableNode>,
    edges: SlotMap<EdgeKey, MutableEdge>,
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
    /// Index-space watermark added to the raw SlotMap index of every
    /// NodeKey this MemGraph hands out or accepts. See `with_id_offset` for
    /// the full soundness argument. Zero (the default via `new`) makes the
    /// translation the identity function -- the overwhelmingly common case
    /// for graphs with no persisted (pre-restart) history.
    id_offset: u32,
}

impl MemGraph {
    /// Create an empty MemGraph with the given freeze threshold.
    pub fn new(edge_threshold: usize) -> Self {
        Self::with_id_offset(edge_threshold, 0)
    }

    /// Create an empty MemGraph whose NodeKeys are all minted `id_offset`
    /// above the raw SlotMap index.
    ///
    /// # Why this exists (soundness argument -- graph NodeKey aliasing, P0)
    ///
    /// `slotmap::SlotMap` key allocation is fully deterministic and a fresh
    /// map's index counter always starts at 0
    /// (`slotmap::basic::SlotMap::insert`, free_head/grow-path). After a
    /// restart, WAL replay works against a fresh `MemGraph`
    /// (`replay.rs::take_memgraph`), while CSR segments loaded from disk
    /// carry `NodeMeta::external_id` values that are the raw
    /// `slotmap::KeyData::as_ffi()` bits minted by the PRE-CRASH process's
    /// SlotMap -- which also started at index 0. Without an offset, the
    /// first node the fresh MemGraph mints gets `(idx=0, version=1)`,
    /// bit-for-bit IDENTICAL to the pre-crash process's first-ever node key
    /// if that node is still resident in a loaded CSR segment (the common
    /// case whenever an AOF fold/WAL checkpoint truncates pre-freeze
    /// history so replay only sees post-freeze commands). Every merged read
    /// path (`MergedNodeView`) checks the mutable tier first, so the
    /// aliasing new node would permanently and silently shadow the real
    /// frozen node.
    ///
    /// ## Fix
    ///
    /// Shift the INDEX component (low 32 bits of `KeyData::as_ffi()`, see
    /// `slotmap::KeyData::{as_ffi, from_ffi}` -- version occupies the high
    /// 32 bits) of every key this MemGraph hands out by `id_offset`. The
    /// caller (recovery.rs) chooses `id_offset` to be `> ` the largest index
    /// component among ALL `external_id`s in the CSR segments it just
    /// loaded for this graph:
    /// - Outgoing (`add_node` return value, `iter_nodes` keys): raw SlotMap
    ///   index + `id_offset` (see `to_public_key`).
    /// - Incoming (any NodeKey parameter): raw index = public index -
    ///   `id_offset`; underflow (public index < `id_offset`) means the key
    ///   was never minted by THIS MemGraph -- it is a CSR-only (or foreign)
    ///   key -- and is treated as "not resident", exactly the existing
    ///   ghost / `NodeNotFound` semantics already used for non-resident
    ///   endpoints (see `to_internal_key`).
    ///
    /// This costs two `u64` shifts per NodeKey touched (zero when
    /// `id_offset == 0`) and precisely **zero** extra permanent memory:
    /// the alternative of pre-consuming `id_offset` SlotMap slots via a
    /// dummy-insert/remove cycle would permanently pin `id_offset`
    /// live-or-vacant slot entries in the SlotMap's backing `Vec` (slotmap
    /// never shrinks its storage) -- exactly the O(watermark) leak this
    /// design avoids. It is also the only sound option: a dummy-cycle
    /// approach only bumps a slot's *generation*, and persisted external_ids
    /// may already carry an arbitrarily-bumped generation from pre-crash
    /// slot churn, so a fixed number of dummy cycles cannot be proven to
    /// out-run every possible persisted generation for a given index.
    ///
    /// ## Overflow
    ///
    /// If `idx + id_offset` would exceed `u32::MAX`, the public key
    /// saturates at `u32::MAX` instead of wrapping (a wrapped index could
    /// re-enter `[0, id_offset)` and alias a persisted `external_id` again).
    /// This is an astronomical corner case (>4 billion prior nodes on one
    /// graph) and degrades to "new inserts stop being independently
    /// addressable" rather than corrupting existing data.
    pub fn with_id_offset(edge_threshold: usize, id_offset: u32) -> Self {
        Self {
            nodes: SlotMap::with_key(),
            edges: SlotMap::with_key(),
            ghost_out: std::collections::HashMap::new(),
            ghost_in: std::collections::HashMap::new(),
            live_node_count: 0,
            live_edge_count: 0,
            edge_threshold,
            frozen: false,
            id_offset,
        }
    }

    /// Translate a raw internal SlotMap `NodeKey` to the PUBLIC key handed
    /// to callers (index + `id_offset`, version unchanged). Identity when
    /// `id_offset == 0`. See `with_id_offset` for the soundness argument.
    #[inline]
    fn to_public_key(id_offset: u32, key: NodeKey) -> NodeKey {
        if id_offset == 0 {
            return key;
        }
        let ffi = key.data().as_ffi();
        let idx = ffi as u32;
        let version = ffi >> 32;
        // Saturate rather than wrap: a wrapped index could re-enter
        // [0, id_offset) and alias a persisted external_id again.
        let public_idx = idx.saturating_add(id_offset);
        NodeKey::from(slotmap::KeyData::from_ffi(
            (version << 32) | u64::from(public_idx),
        ))
    }

    /// Translate a PUBLIC `NodeKey` (offset applied) to the raw internal
    /// SlotMap key used to index `self.nodes`. Returns `None` if the public
    /// index is below `id_offset` -- such a key was never minted by this
    /// MemGraph (it belongs to a CSR segment or a foreign graph) and must be
    /// treated as non-resident, matching the existing ghost / NodeNotFound
    /// semantics for non-resident endpoints. Identity when `id_offset == 0`.
    #[inline]
    fn to_internal_key(id_offset: u32, key: NodeKey) -> Option<NodeKey> {
        if id_offset == 0 {
            return Some(key);
        }
        let ffi = key.data().as_ffi();
        let idx = ffi as u32;
        let version = ffi >> 32;
        let internal_idx = idx.checked_sub(id_offset)?;
        Some(NodeKey::from(slotmap::KeyData::from_ffi(
            (version << 32) | u64::from(internal_idx),
        )))
    }

    /// Insert a new node. Returns the generational (public) key.
    pub fn add_node(
        &mut self,
        labels: SmallVec<[u16; 4]>,
        properties: PropertyMap,
        embedding: Option<Vec<f32>>,
        lsn: u64,
    ) -> NodeKey {
        let key = self.nodes.insert(MutableNode {
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
        });
        self.live_node_count += 1;
        Self::to_public_key(self.id_offset, key)
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
        // Translate PUBLIC keys to raw internal SlotMap keys. Translation
        // failure (offset underflow) means the key was never minted by this
        // MemGraph -- treat exactly like "not resident" (`add_edge` does not
        // support cross-tier endpoints; use `add_edge_across_tiers`).
        let Some(src_i) = Self::to_internal_key(self.id_offset, src) else {
            return Err(GraphError::NodeNotFound);
        };
        let Some(dst_i) = Self::to_internal_key(self.id_offset, dst) else {
            return Err(GraphError::NodeNotFound);
        };
        // Validate both nodes exist and are alive.
        let src_alive = self
            .nodes
            .get(src_i)
            .map_or(false, |n| n.deleted_lsn == u64::MAX);
        let dst_alive = self
            .nodes
            .get(dst_i)
            .map_or(false, |n| n.deleted_lsn == u64::MAX);
        if !src_alive || !dst_alive {
            return Err(GraphError::NodeNotFound);
        }

        let ek = self.edges.insert(MutableEdge {
            // Stored as PUBLIC keys: `MutableEdge.src/dst` are the identity
            // callers (freeze(), neighbors()) compare against, matching
            // `add_node`'s return value and CSR `external_id`s.
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
        if let Some(src_node) = self.nodes.get_mut(src_i) {
            src_node.outgoing.push(ek);
        }
        if let Some(dst_node) = self.nodes.get_mut(dst_i) {
            dst_node.incoming.push(ek);
        }
        self.live_edge_count += 1;
        Ok(ek)
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
        if self.frozen {
            return Err(GraphError::AlreadyFrozen);
        }
        if src == dst {
            return Err(GraphError::SelfLoop);
        }
        // Translate PUBLIC keys to internal SlotMap keys where possible.
        // `None` (translation underflow, or a key genuinely absent from this
        // MemGraph) means non-resident -- caller-verified via the CSR tier.
        let src_i = Self::to_internal_key(self.id_offset, src);
        let dst_i = Self::to_internal_key(self.id_offset, dst);
        // Resident endpoints must be alive; non-resident are caller-verified.
        for key_i in [src_i, dst_i].into_iter().flatten() {
            if let Some(n) = self.nodes.get(key_i) {
                if n.deleted_lsn != u64::MAX {
                    return Err(GraphError::NodeNotFound);
                }
            }
        }

        let ek = self.edges.insert(MutableEdge {
            // PUBLIC keys: ghost_out/ghost_in are keyed by public identity
            // too (a non-resident endpoint has no internal key at all).
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
        });

        match src_i.and_then(|k| self.nodes.get_mut(k)) {
            Some(src_node) => src_node.outgoing.push(ek),
            None => self.ghost_out.entry(src).or_default().push(ek),
        }
        match dst_i.and_then(|k| self.nodes.get_mut(k)) {
            Some(dst_node) => dst_node.incoming.push(ek),
            None => self.ghost_in.entry(dst).or_default().push(ek),
        }
        self.live_edge_count += 1;
        Ok(ek)
    }

    /// Soft-delete a node and all its incident edges.
    pub fn remove_node(&mut self, key: NodeKey, lsn: u64) -> bool {
        let Some(internal) = Self::to_internal_key(self.id_offset, key) else {
            return false;
        };
        let Some(node) = self.nodes.get_mut(internal) else {
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
            if let Some(edge) = self.edges.get_mut(ek) {
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
        let Some(edge) = self.edges.get_mut(key) else {
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
        let internal = Self::to_internal_key(self.id_offset, key)?;
        self.nodes.get(internal)
    }

    /// O(1) mutable node lookup by key.
    pub fn get_node_mut(&mut self, key: NodeKey) -> Option<&mut MutableNode> {
        let internal = Self::to_internal_key(self.id_offset, key)?;
        self.nodes.get_mut(internal)
    }

    /// O(1) edge lookup by key.
    pub fn get_edge(&self, key: EdgeKey) -> Option<&MutableEdge> {
        self.edges.get(key)
    }

    /// O(1) mutable edge lookup by key.
    pub fn get_edge_mut(&mut self, key: EdgeKey) -> Option<&mut MutableEdge> {
        self.edges.get_mut(key)
    }

    /// Returns neighbors of `node` visible at the given `lsn`, filtered by direction.
    ///
    /// Yields `(EdgeKey, NodeKey)` pairs -- the edge and the neighbor node.
    /// No heap allocation: iterates over borrowed SmallVec adjacency lists.
    pub fn neighbors(&self, node: NodeKey, direction: Direction, lsn: u64) -> NeighborIter<'_> {
        let internal = Self::to_internal_key(self.id_offset, node);
        let Some(n) = internal.and_then(|k| self.nodes.get(k)) else {
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

    /// Iterate over all live (non-deleted) nodes. Yields `(NodeKey, &MutableNode)`
    /// with PUBLIC keys (offset applied) -- matching `add_node`'s return value.
    pub fn iter_nodes(&self) -> impl Iterator<Item = (NodeKey, &MutableNode)> {
        let id_offset = self.id_offset;
        self.nodes
            .iter()
            .filter(|(_, n)| n.deleted_lsn == u64::MAX)
            .map(move |(k, n)| (Self::to_public_key(id_offset, k), n))
    }

    /// Iterate over all live (non-deleted) edges. Yields `(EdgeKey, &MutableEdge)`.
    pub fn iter_edges(&self) -> impl Iterator<Item = (EdgeKey, &MutableEdge)> {
        self.edges.iter().filter(|(_, e)| e.deleted_lsn == u64::MAX)
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
        let Some(internal) = Self::to_internal_key(self.id_offset, node) else {
            return;
        };
        let Some(n) = self.nodes.get(internal) else {
            return;
        };
        let edge_keys: SmallVec<[EdgeKey; 16]> = n
            .outgoing
            .iter()
            .chain(n.incoming.iter())
            .copied()
            .collect();
        for ek in edge_keys {
            if let Some(edge) = self.edges.get_mut(ek) {
                if edge.deleted_lsn == lsn {
                    edge.deleted_lsn = u64::MAX;
                    self.live_edge_count += 1;
                }
            }
        }
    }

    /// Resident bytes used by the in-memory adjacency lists (nodes + edges
    /// slot maps). Approximation based on slot-map capacity and struct sizes.
    pub fn resident_bytes(&self) -> usize {
        let node_bytes = self.nodes.capacity() * std::mem::size_of::<MutableNode>();
        let edge_bytes = self.edges.capacity() * std::mem::size_of::<MutableEdge>();
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
        // slot map): live + both endpoints resident → freeze into CSR;
        // live + any frozen-elsewhere endpoint → retain as delta;
        // dead → drop.
        let mut freeze_keys: Vec<EdgeKey> = Vec::new();
        let mut dead_keys: Vec<EdgeKey> = Vec::new();
        let id_offset = self.id_offset;
        // `e.src`/`e.dst` are PUBLIC keys; translate to internal before
        // checking slot-map residency.
        let is_resident = |nodes: &SlotMap<NodeKey, MutableNode>, key: NodeKey| {
            Self::to_internal_key(id_offset, key).is_some_and(|k| nodes.contains_key(k))
        };
        for (ek, e) in self.edges.iter() {
            if e.deleted_lsn != u64::MAX {
                dead_keys.push(ek);
            } else if is_resident(&self.nodes, e.src) && is_resident(&self.nodes, e.dst) {
                freeze_keys.push(ek);
            }
        }
        for ek in dead_keys {
            self.edges.remove(ek);
        }
        let edges: Vec<(EdgeKey, MutableEdge)> = freeze_keys
            .into_iter()
            .filter_map(|ek| self.edges.remove(ek).map(|e| (ek, e)))
            .collect();

        // `drain()` yields raw internal keys -- translate to PUBLIC before
        // handing them to CSR conversion (they become `NodeMeta::external_id`
        // and must match the identity every other reference to this node
        // uses: node_map, ghost adjacency, edge endpoints).
        let nodes: Vec<(NodeKey, MutableNode)> = self
            .nodes
            .drain()
            .filter(|(_, n)| n.deleted_lsn == u64::MAX)
            .map(|(k, n)| (Self::to_public_key(id_offset, k), n))
            .collect();

        // Rebuild ghost adjacency for the retained delta edges: with the
        // node slot map drained, EVERY endpoint is now non-resident.
        self.ghost_out.clear();
        self.ghost_in.clear();
        for (ek, e) in self.edges.iter() {
            self.ghost_out.entry(e.src).or_default().push(ek);
            self.ghost_in.entry(e.dst).or_default().push(ek);
        }

        Ok(FrozenMemGraph { nodes, edges })
    }

    /// Re-arm a drained MemGraph for writes after a successful freeze.
    ///
    /// Keeps the SAME slot maps (drain bumps each vacated slot's generation,
    /// so keys handed out after thaw can never collide with the external_ids
    /// of frozen CSR rows). Replacing the MemGraph with a fresh one instead
    /// would restart SlotMap allocation at the same (index, generation) pairs
    /// and silently alias new nodes onto frozen rows.
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
    edges: &'a SlotMap<EdgeKey, MutableEdge>,
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
                if let Some(edge) = self.edges.get(ek) {
                    if is_visible(edge) {
                        return Some((ek, edge.dst));
                    }
                }
                continue;
            }
            if let Some(&ek) = self.in_iter.next() {
                if let Some(edge) = self.edges.get(ek) {
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
