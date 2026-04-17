//! MemGraph -- mutable adjacency-list write buffer backed by SlotMap.
//!
//! Absorbs graph writes at O(1) amortized cost per insert. Freezes into a
//! `FrozenMemGraph` when the edge threshold is reached, enabling CSR conversion.

use slotmap::SlotMap;
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
            nodes: SlotMap::with_key(),
            edges: SlotMap::with_key(),
            live_node_count: 0,
            live_edge_count: 0,
            edge_threshold,
            frozen: false,
        }
    }

    /// Insert a new node. Returns the generational key.
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
            .get(src)
            .map_or(false, |n| n.deleted_lsn == u64::MAX);
        let dst_alive = self
            .nodes
            .get(dst)
            .map_or(false, |n| n.deleted_lsn == u64::MAX);
        if !src_alive || !dst_alive {
            return Err(GraphError::NodeNotFound);
        }

        let ek = self.edges.insert(MutableEdge {
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
        });

        // Push edge key into src.outgoing and dst.incoming.
        // Both are validated alive above, so get_mut is safe.
        if let Some(src_node) = self.nodes.get_mut(src) {
            src_node.outgoing.push(ek);
        }
        if let Some(dst_node) = self.nodes.get_mut(dst) {
            dst_node.incoming.push(ek);
        }
        self.live_edge_count += 1;
        Ok(ek)
    }

    /// Soft-delete a node and all its incident edges.
    pub fn remove_node(&mut self, key: NodeKey, lsn: u64) -> bool {
        let Some(node) = self.nodes.get_mut(key) else {
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
        self.nodes.get(key)
    }

    /// O(1) mutable node lookup by key.
    pub fn get_node_mut(&mut self, key: NodeKey) -> Option<&mut MutableNode> {
        self.nodes.get_mut(key)
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
        let Some(n) = self.nodes.get(node) else {
            return NeighborIter {
                edges: &self.edges,
                out_iter: [].iter(),
                in_iter: [].iter(),
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

    /// Iterate over all live (non-deleted) nodes. Yields `(NodeKey, &MutableNode)`.
    pub fn iter_nodes(&self) -> impl Iterator<Item = (NodeKey, &MutableNode)> {
        self.nodes.iter().filter(|(_, n)| n.deleted_lsn == u64::MAX)
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

    /// Whether the MemGraph should be frozen (threshold reached).
    pub fn should_freeze(&self) -> bool {
        self.live_edge_count >= self.edge_threshold && !self.frozen
    }

    /// Freeze the MemGraph, returning a FrozenMemGraph with all data for CSR conversion.
    /// Only includes live (non-deleted) nodes and edges.
    pub fn freeze(&mut self) -> Result<FrozenMemGraph, GraphError> {
        if self.frozen {
            return Err(GraphError::AlreadyFrozen);
        }
        self.frozen = true;

        let nodes: Vec<(NodeKey, MutableNode)> = self
            .nodes
            .drain()
            .filter(|(_, n)| n.deleted_lsn == u64::MAX)
            .collect();

        let edges: Vec<(EdgeKey, MutableEdge)> = self
            .edges
            .drain()
            .filter(|(_, e)| e.deleted_lsn == u64::MAX)
            .collect();

        Ok(FrozenMemGraph { nodes, edges })
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
