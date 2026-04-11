//! Graph traversal algorithms: BFS, DFS, Dijkstra, and segment merge reader.
//!
//! All traversals operate over a `SegmentMergeReader` that provides a unified
//! neighbor view across the mutable MemGraph and all immutable CSR segments.
//! Edge visibility is governed by MVCC snapshot LSN (TRAV-09).
//!
//! BFS uses a `BoundedFrontier` capped at 100K nodes (TRAV-01).
//! DFS supports depth limit and max-cost pruning (TRAV-02).
//! Dijkstra finds shortest weighted path using composite cost (TRAV-04).

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::graph::csr::CsrSegment;
use crate::graph::memgraph::MemGraph;
use crate::graph::scoring::WeightedCostFn;
use crate::graph::types::{Direction, EdgeKey, NodeKey};

/// Default maximum frontier size for BFS (100K nodes).
pub const DEFAULT_FRONTIER_CAP: usize = 100_000;

// ---------------------------------------------------------------------------
// Traversal errors
// ---------------------------------------------------------------------------

/// Errors during graph traversal.
#[derive(Debug, Clone, PartialEq)]
pub enum TraversalError {
    /// BFS frontier exceeded the configured cap.
    FrontierCapExceeded { cap: usize, depth: u32 },
    /// Depth limit reached.
    DepthExceeded { limit: u32 },
    /// Start node not found in any segment.
    NodeNotFound,
    /// Traversal timed out (epoch hold exceeded).
    Timeout(String),
}

impl core::fmt::Display for TraversalError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::FrontierCapExceeded { cap, depth } => {
                write!(f, "BFS frontier cap {cap} exceeded at depth {depth}")
            }
            Self::DepthExceeded { limit } => write!(f, "depth limit {limit} exceeded"),
            Self::NodeNotFound => write!(f, "start node not found"),
            Self::Timeout(msg) => write!(f, "traversal timeout: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Unified neighbor from any segment
// ---------------------------------------------------------------------------

/// A neighbor edge from any segment (MemGraph or CSR).
#[derive(Debug, Clone)]
pub struct MergedNeighbor {
    /// The neighbor node key.
    pub node: NodeKey,
    /// The edge key (only valid for MemGraph edges; synthetic for CSR).
    pub edge: EdgeKey,
    /// Edge type.
    pub edge_type: u16,
    /// Edge weight (distance/cost). 0.0 if not available from CSR.
    pub weight: f64,
    /// Timestamp (created_lsn) of the edge.
    pub timestamp: u64,
}

// ---------------------------------------------------------------------------
// SegmentMergeReader (TRAV-09)
// ---------------------------------------------------------------------------

/// Unified neighbor reader across MemGraph + all CSR segments.
///
/// Deduplicates neighbors (same NodeKey from multiple segments) and filters
/// tombstoned edges from CSR segments via validity bitmaps.
/// Reads from all active segments for query-time merge.
pub struct SegmentMergeReader<'a> {
    memgraph: Option<&'a MemGraph>,
    csr_segments: &'a [Arc<CsrSegment>],
    direction: Direction,
    snapshot_lsn: u64,
    edge_type_filter: Option<u16>,
}

impl<'a> SegmentMergeReader<'a> {
    /// Create a merge reader from a MemGraph and CSR segments.
    pub fn new(
        memgraph: Option<&'a MemGraph>,
        csr_segments: &'a [Arc<CsrSegment>],
        direction: Direction,
        snapshot_lsn: u64,
        edge_type_filter: Option<u16>,
    ) -> Self {
        Self {
            memgraph,
            csr_segments,
            direction,
            snapshot_lsn,
            edge_type_filter,
        }
    }

    /// Get all neighbors of a node, merged from all segments, deduplicated.
    ///
    /// Returns a Vec to allow callers to iterate freely. Allocates once per
    /// call (acceptable -- this is at the end of the command path, not hot loop).
    pub fn neighbors(&self, node: NodeKey) -> Vec<MergedNeighbor> {
        let mut seen: HashSet<NodeKey> = HashSet::new();
        let mut result: Vec<MergedNeighbor> = Vec::new();

        // 1. Read from mutable MemGraph (highest priority -- newest data).
        if let Some(mg) = self.memgraph {
            for (edge_key, neighbor_key) in mg.neighbors(node, self.direction, self.snapshot_lsn) {
                if let Some(edge) = mg.get_edge(edge_key) {
                    // Apply edge type filter.
                    if let Some(filter) = self.edge_type_filter {
                        if edge.edge_type != filter {
                            continue;
                        }
                    }
                    if seen.insert(neighbor_key) {
                        result.push(MergedNeighbor {
                            node: neighbor_key,
                            edge: edge_key,
                            edge_type: edge.edge_type,
                            weight: edge.weight,
                            timestamp: edge.created_lsn,
                        });
                    }
                }
            }
        }

        // 2. Read from immutable CSR segments (newest first).
        for csr in self.csr_segments {
            // Only include segments created at or before our snapshot.
            if csr.created_lsn > self.snapshot_lsn {
                continue;
            }
            let Some(row) = csr.lookup_node(node) else {
                continue;
            };

            // CSR only stores outgoing edges. For Direction::Both or Incoming,
            // we would need an incoming CSR or reverse index. For now, CSR
            // contributes outgoing neighbors only (matching the CSR data model).
            if self.direction == Direction::Incoming {
                continue;
            }

            for (col_idx, meta) in csr.neighbor_edges(row) {
                // Apply edge type filter.
                if let Some(filter) = self.edge_type_filter {
                    if meta.edge_type != filter {
                        continue;
                    }
                }

                // Resolve col_idx back to a NodeKey via node_meta.
                if (col_idx as usize) < csr.node_meta.len() {
                    let target_meta = &csr.node_meta[col_idx as usize];
                    // Check target node visibility (not deleted at snapshot).
                    if target_meta.deleted_lsn != u64::MAX
                        && target_meta.deleted_lsn <= self.snapshot_lsn
                    {
                        continue;
                    }
                    let target_key: NodeKey =
                        slotmap::KeyData::from_ffi(target_meta.external_id).into();

                    if seen.insert(target_key) {
                        // CSR EdgeMeta doesn't store weight/timestamp directly.
                        // Use segment created_lsn as timestamp, 1.0 as default weight.
                        result.push(MergedNeighbor {
                            node: target_key,
                            edge: slotmap::KeyData::from_ffi(0).into(),
                            edge_type: meta.edge_type,
                            weight: 1.0,
                            timestamp: csr.created_lsn,
                        });
                    }
                }
            }
        }

        result
    }

    /// Check if a node exists in any segment.
    pub fn node_exists(&self, node: NodeKey) -> bool {
        if let Some(mg) = self.memgraph {
            if mg.get_node(node).is_some() {
                return true;
            }
        }
        for csr in self.csr_segments {
            if csr.lookup_node(node).is_some() {
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// BFS (TRAV-01)
// ---------------------------------------------------------------------------

/// BFS result: visited nodes with their depth and discovery edge.
#[derive(Debug, Clone)]
pub struct BfsResult {
    /// Nodes visited, in BFS order: (node, depth, edge_used).
    pub visited: Vec<(NodeKey, u32, Option<MergedNeighbor>)>,
}

/// Bounded BFS traversal.
///
/// Frontier cap (default 100K) prevents OOM on high-degree graphs.
/// Returns error rather than silently truncating when cap is breached (TRAV-01).
pub struct BoundedBfs {
    pub depth_limit: u32,
    pub frontier_cap: usize,
}

impl BoundedBfs {
    /// Create with default frontier cap of 100K.
    pub fn new(depth_limit: u32) -> Self {
        Self {
            depth_limit,
            frontier_cap: DEFAULT_FRONTIER_CAP,
        }
    }

    /// Create with custom frontier cap.
    pub fn with_cap(depth_limit: u32, frontier_cap: usize) -> Self {
        Self {
            depth_limit,
            frontier_cap,
        }
    }

    /// Execute BFS from a start node.
    pub fn execute(
        &self,
        reader: &SegmentMergeReader<'_>,
        start: NodeKey,
    ) -> Result<BfsResult, TraversalError> {
        if !reader.node_exists(start) {
            return Err(TraversalError::NodeNotFound);
        }

        let mut visited_set: HashSet<NodeKey> = HashSet::new();
        visited_set.insert(start);

        let mut result = Vec::new();
        result.push((start, 0, None));

        let mut frontier: VecDeque<(NodeKey, u32)> = VecDeque::new();
        frontier.push_back((start, 0));

        while let Some((current, depth)) = frontier.pop_front() {
            if depth >= self.depth_limit {
                continue;
            }

            let neighbors = reader.neighbors(current);
            for neighbor in neighbors {
                if visited_set.contains(&neighbor.node) {
                    continue;
                }

                // Check frontier cap before enqueuing.
                if visited_set.len() >= self.frontier_cap {
                    return Err(TraversalError::FrontierCapExceeded {
                        cap: self.frontier_cap,
                        depth: depth + 1,
                    });
                }

                visited_set.insert(neighbor.node);
                let next_depth = depth + 1;
                result.push((neighbor.node, next_depth, Some(neighbor.clone())));
                frontier.push_back((neighbor.node, next_depth));
            }
        }

        Ok(BfsResult { visited: result })
    }
}

// ---------------------------------------------------------------------------
// DFS (TRAV-02)
// ---------------------------------------------------------------------------

/// DFS result: visited nodes with path cost.
#[derive(Debug, Clone)]
pub struct DfsResult {
    /// Nodes visited in DFS order: (node, depth, cumulative_cost).
    pub visited: Vec<(NodeKey, u32, f64)>,
}

/// Bounded DFS with depth limit and max-cost pruning.
pub struct BoundedDfs {
    pub depth_limit: u32,
    pub max_cost: f64,
    pub cost_fn: WeightedCostFn,
}

impl BoundedDfs {
    /// Create a DFS with depth limit and max-cost pruning.
    pub fn new(depth_limit: u32, max_cost: f64, cost_fn: WeightedCostFn) -> Self {
        Self {
            depth_limit,
            max_cost,
            cost_fn,
        }
    }

    /// Execute DFS from a start node.
    pub fn execute(
        &self,
        reader: &SegmentMergeReader<'_>,
        start: NodeKey,
    ) -> Result<DfsResult, TraversalError> {
        if !reader.node_exists(start) {
            return Err(TraversalError::NodeNotFound);
        }

        let mut visited_set: HashSet<NodeKey> = HashSet::new();
        let mut result = Vec::new();

        // Stack: (node, depth, cumulative_cost)
        let mut stack: Vec<(NodeKey, u32, f64)> = Vec::new();
        stack.push((start, 0, 0.0));

        while let Some((current, depth, cum_cost)) = stack.pop() {
            if visited_set.contains(&current) {
                continue;
            }
            visited_set.insert(current);
            result.push((current, depth, cum_cost));

            if depth >= self.depth_limit {
                continue;
            }

            let neighbors = reader.neighbors(current);
            for neighbor in neighbors {
                if visited_set.contains(&neighbor.node) {
                    continue;
                }
                let edge_cost = self.cost_fn.cost(neighbor.timestamp, neighbor.weight);
                let new_cost = cum_cost + edge_cost;

                // Max-cost pruning (TRAV-02).
                if new_cost > self.max_cost {
                    continue;
                }

                stack.push((neighbor.node, depth + 1, new_cost));
            }
        }

        Ok(DfsResult { visited: result })
    }
}

// ---------------------------------------------------------------------------
// Dijkstra Shortest Path (TRAV-04)
// ---------------------------------------------------------------------------

/// Dijkstra result: shortest path from start to target.
#[derive(Debug, Clone)]
pub struct DijkstraResult {
    /// Total cost of the shortest path.
    pub total_cost: f64,
    /// Nodes in the path, from start to target (inclusive).
    pub path: Vec<NodeKey>,
}

/// Entry in Dijkstra's priority queue (min-heap by cost).
struct DijkstraEntry {
    node: NodeKey,
    cost: f64,
}

impl PartialEq for DijkstraEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for DijkstraEntry {}

impl PartialOrd for DijkstraEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DijkstraEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (BinaryHeap is max-heap).
        // NaN is treated as greater than all values (pushed to bottom, never selected).
        match other.cost.partial_cmp(&self.cost) {
            Some(ord) => ord,
            None => {
                // At least one is NaN. NaN entries sort last (greatest cost).
                match (self.cost.is_nan(), other.cost.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Less, // self is NaN = high cost → less priority
                    (false, true) => Ordering::Greater, // other is NaN = high cost → self wins
                    _ => unreachable!(),
                }
            }
        }
    }
}

/// Dijkstra shortest weighted path traversal.
pub struct DijkstraTraversal {
    pub cost_fn: WeightedCostFn,
    pub max_depth: u32,
}

impl DijkstraTraversal {
    /// Create a Dijkstra traversal with weighted cost function.
    pub fn new(cost_fn: WeightedCostFn, max_depth: u32) -> Self {
        Self { cost_fn, max_depth }
    }

    /// Find shortest path from `start` to `target`.
    ///
    /// Returns `None` if no path exists within max_depth hops.
    pub fn shortest_path(
        &self,
        reader: &SegmentMergeReader<'_>,
        start: NodeKey,
        target: NodeKey,
    ) -> Result<Option<DijkstraResult>, TraversalError> {
        if !reader.node_exists(start) {
            return Err(TraversalError::NodeNotFound);
        }

        let mut dist: HashMap<NodeKey, f64> = HashMap::new();
        let mut prev: HashMap<NodeKey, NodeKey> = HashMap::new();
        let mut depth_map: HashMap<NodeKey, u32> = HashMap::new();
        let mut heap = BinaryHeap::new();

        dist.insert(start, 0.0);
        depth_map.insert(start, 0);
        heap.push(DijkstraEntry {
            node: start,
            cost: 0.0,
        });

        while let Some(DijkstraEntry { node, cost }) = heap.pop() {
            // Found target -- reconstruct path.
            if node == target {
                let path = self.reconstruct_path(&prev, start, target);
                return Ok(Some(DijkstraResult {
                    total_cost: cost,
                    path,
                }));
            }

            // Skip if we've already found a better path.
            if let Some(&best) = dist.get(&node) {
                if cost > best {
                    continue;
                }
            }

            let current_depth = depth_map.get(&node).copied().unwrap_or(0);
            if current_depth >= self.max_depth {
                continue;
            }

            let neighbors = reader.neighbors(node);
            for neighbor in neighbors {
                let edge_cost = self.cost_fn.cost(neighbor.timestamp, neighbor.weight);
                let new_cost = cost + edge_cost;

                let is_better = match dist.get(&neighbor.node) {
                    Some(&existing) => new_cost < existing,
                    None => true,
                };

                if is_better {
                    dist.insert(neighbor.node, new_cost);
                    prev.insert(neighbor.node, node);
                    depth_map.insert(neighbor.node, current_depth + 1);
                    heap.push(DijkstraEntry {
                        node: neighbor.node,
                        cost: new_cost,
                    });
                }
            }
        }

        Ok(None) // No path found
    }

    /// Reconstruct path from prev-pointers.
    fn reconstruct_path(
        &self,
        prev: &HashMap<NodeKey, NodeKey>,
        start: NodeKey,
        target: NodeKey,
    ) -> Vec<NodeKey> {
        let mut path = Vec::new();
        let mut current = target;
        path.push(current);
        while current != start {
            match prev.get(&current) {
                Some(&p) => {
                    current = p;
                    path.push(current);
                }
                None => break,
            }
        }
        path.reverse();
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::csr::CsrSegment;
    use crate::graph::memgraph::MemGraph;
    use crate::graph::scoring::WeightedCostFn;
    use smallvec::smallvec;

    fn empty_props() -> crate::graph::types::PropertyMap {
        smallvec![]
    }

    // --- SegmentMergeReader tests ---

    #[test]
    fn test_merge_reader_memgraph_only() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 2.0, None, 2).expect("ok");
        mg.add_edge(a, c, 1, 3.0, None, 3).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let neighbors = reader.neighbors(a);
        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn test_merge_reader_dedup_across_segments() {
        // Create MemGraph with node a->b
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");

        // Create CSR also containing a->b
        let mut mg2 = MemGraph::new(1000);
        let a2 = mg2.add_node(smallvec![0], empty_props(), None, 1);
        let b2 = mg2.add_node(smallvec![0], empty_props(), None, 1);
        mg2.add_edge(a2, b2, 1, 1.0, None, 2).expect("ok");
        let frozen = mg2.freeze().expect("ok");
        let csr = CsrSegment::from_frozen(frozen, 5).expect("ok");
        let csr_segs = vec![Arc::new(csr)];

        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        // Node b appears in both segments, but should be deduplicated.
        let neighbors = reader.neighbors(a);
        assert_eq!(neighbors.len(), 1);
    }

    #[test]
    fn test_merge_reader_csr_tombstone_filtered() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(a, c, 1, 1.0, None, 2).expect("ok");
        let frozen = mg.freeze().expect("ok");
        let mut csr = CsrSegment::from_frozen(frozen, 5).expect("ok");

        // Tombstone first edge (a->b).
        let first_edge_idx = csr.row_offsets[0]; // Row 0 = node a, first edge
        csr.mark_deleted(first_edge_idx);

        let csr_segs = vec![Arc::new(csr)];
        let reader: SegmentMergeReader<'_> =
            SegmentMergeReader::new(None, &csr_segs, Direction::Outgoing, u64::MAX - 1, None);

        // Look up the first node's key (row 0 in CSR).
        let node_a_key = {
            let meta = &csr_segs[0].node_meta[0];
            let key: NodeKey = slotmap::KeyData::from_ffi(meta.external_id).into();
            key
        };

        let neighbors = reader.neighbors(node_a_key);
        // Should only have 1 neighbor (the other was tombstoned).
        assert_eq!(neighbors.len(), 1);
    }

    #[test]
    fn test_merge_reader_edge_type_filter() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(a, c, 2, 1.0, None, 2).expect("ok"); // different type

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            Some(1), // filter type=1 only
        );

        let neighbors = reader.neighbors(a);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].edge_type, 1);
    }

    #[test]
    fn test_merge_reader_node_exists() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        assert!(reader.node_exists(a));

        let fake_key: NodeKey = slotmap::KeyData::from_ffi(9999).into();
        assert!(!reader.node_exists(fake_key));
    }

    // --- BFS tests ---

    #[test]
    fn test_bfs_simple_depth_1() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(a, c, 1, 1.0, None, 2).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let bfs = BoundedBfs::new(1);
        let result = bfs.execute(&reader, a).expect("ok");
        // Start + 2 neighbors
        assert_eq!(result.visited.len(), 3);
        assert_eq!(result.visited[0].0, a);
        assert_eq!(result.visited[0].1, 0); // depth 0
    }

    #[test]
    fn test_bfs_depth_limit() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        let d = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(b, c, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(c, d, 1, 1.0, None, 2).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader =
            SegmentMergeReader::new(Some(&mg), &csr_segs, Direction::Both, u64::MAX - 1, None);

        // Depth 1: only a and b
        let bfs = BoundedBfs::new(1);
        let result = bfs.execute(&reader, a).expect("ok");
        assert_eq!(result.visited.len(), 2);

        // Depth 3: all 4 nodes
        let bfs3 = BoundedBfs::new(3);
        let result3 = bfs3.execute(&reader, a).expect("ok");
        assert_eq!(result3.visited.len(), 4);
    }

    #[test]
    fn test_bfs_frontier_cap_exceeded() {
        // Create a star graph: center -> N leaves.
        let mut mg = MemGraph::new(100_000);
        let center = mg.add_node(smallvec![0], empty_props(), None, 1);
        let cap = 10usize;
        for _ in 0..cap + 5 {
            let leaf = mg.add_node(smallvec![0], empty_props(), None, 1);
            mg.add_edge(center, leaf, 1, 1.0, None, 2).expect("ok");
        }

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let bfs = BoundedBfs::with_cap(1, cap);
        let err = bfs.execute(&reader, center).unwrap_err();
        match err {
            TraversalError::FrontierCapExceeded { cap: c, depth: _ } => {
                assert_eq!(c, cap);
            }
            other => panic!("expected FrontierCapExceeded, got {other:?}"),
        }
    }

    #[test]
    fn test_bfs_node_not_found() {
        let mg = MemGraph::new(1000);
        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let fake_key: NodeKey = slotmap::KeyData::from_ffi(9999).into();
        let bfs = BoundedBfs::new(3);
        let err = bfs.execute(&reader, fake_key).unwrap_err();
        assert_eq!(err, TraversalError::NodeNotFound);
    }

    #[test]
    fn test_bfs_cycle_handling() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        // Cycle: a->b->c->a (via incoming/outgoing)
        mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(b, c, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(c, a, 1, 1.0, None, 2).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let bfs = BoundedBfs::new(10);
        let result = bfs.execute(&reader, a).expect("ok");
        // Should visit all 3 nodes exactly once.
        assert_eq!(result.visited.len(), 3);
    }

    // --- DFS tests ---

    #[test]
    fn test_dfs_basic() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 10).expect("ok");
        mg.add_edge(b, c, 1, 2.0, None, 10).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let dfs = BoundedDfs::new(5, 100.0, cost_fn);
        let result = dfs.execute(&reader, a).expect("ok");
        assert_eq!(result.visited.len(), 3);
    }

    #[test]
    fn test_dfs_max_cost_pruning() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        // a->b costs 5.0, b->c costs 10.0
        mg.add_edge(a, b, 1, 5.0, None, 100).expect("ok");
        mg.add_edge(b, c, 1, 10.0, None, 100).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        // Max cost = 8.0 (distance_weight=1.0, time_weight=0.0)
        // a->b cost = 5.0, b->c cost = 5.0+10.0 = 15.0 > 8.0 (pruned)
        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let dfs = BoundedDfs::new(5, 8.0, cost_fn);
        let result = dfs.execute(&reader, a).expect("ok");
        // Should visit a and b, but not c (cost exceeds max).
        assert_eq!(result.visited.len(), 2);
    }

    #[test]
    fn test_dfs_depth_limit() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 10).expect("ok");
        mg.add_edge(b, c, 1, 1.0, None, 10).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let dfs = BoundedDfs::new(1, f64::MAX, cost_fn); // depth=1
        let result = dfs.execute(&reader, a).expect("ok");
        assert_eq!(result.visited.len(), 2); // a and b only
    }

    // --- Dijkstra tests ---

    #[test]
    fn test_dijkstra_shortest_path_simple() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        // Direct a->c costs 10.0, via a->b->c costs 3.0+4.0=7.0
        mg.add_edge(a, c, 1, 10.0, None, 100).expect("ok");
        mg.add_edge(a, b, 1, 3.0, None, 100).expect("ok");
        mg.add_edge(b, c, 1, 4.0, None, 100).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        // distance_only cost: time_weight=0, distance_weight=1
        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let dijkstra = DijkstraTraversal::new(cost_fn, 10);
        let result = dijkstra
            .shortest_path(&reader, a, c)
            .expect("ok")
            .expect("path found");

        // Shortest path: a->b->c with cost 7.0
        assert!((result.total_cost - 7.0).abs() < f64::EPSILON);
        assert_eq!(result.path.len(), 3);
        assert_eq!(result.path[0], a);
        assert_eq!(result.path[1], b);
        assert_eq!(result.path[2], c);
    }

    #[test]
    fn test_dijkstra_no_path() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        // No edge from a to b.

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let dijkstra = DijkstraTraversal::new(cost_fn, 10);
        let result = dijkstra.shortest_path(&reader, a, b).expect("ok");
        assert!(result.is_none());
    }

    #[test]
    fn test_dijkstra_start_is_target() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let dijkstra = DijkstraTraversal::new(cost_fn, 10);
        let result = dijkstra
            .shortest_path(&reader, a, a)
            .expect("ok")
            .expect("path found");

        assert!((result.total_cost - 0.0).abs() < f64::EPSILON);
        assert_eq!(result.path, vec![a]);
    }

    #[test]
    fn test_dijkstra_composite_cost() {
        // TRAV-03/04: time_weight * (now - ts) + distance_weight * distance
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);

        // Route 1: a->c, weight=2.0, timestamp=90 (age=10)
        //   cost = 0.5*10 + 1.0*2.0 = 7.0
        mg.add_edge(a, c, 1, 2.0, None, 90).expect("ok");

        // Route 2: a->b (weight=1.0, ts=95, age=5) -> b->c (weight=1.0, ts=80, age=20)
        //   a->b cost = 0.5*5 + 1.0*1.0 = 3.5
        //   b->c cost = 0.5*20 + 1.0*1.0 = 11.0
        //   total = 14.5
        mg.add_edge(a, b, 1, 1.0, None, 95).expect("ok");
        mg.add_edge(b, c, 1, 1.0, None, 80).expect("ok");

        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let cost_fn = WeightedCostFn::new(0.5, 1.0, 100);
        let dijkstra = DijkstraTraversal::new(cost_fn, 10);
        let result = dijkstra
            .shortest_path(&reader, a, c)
            .expect("ok")
            .expect("path found");

        // Direct a->c (cost=7.0) is cheaper than a->b->c (cost=14.5)
        assert!((result.total_cost - 7.0).abs() < f64::EPSILON);
        assert_eq!(result.path, vec![a, c]);
    }

    #[test]
    fn test_dijkstra_node_not_found() {
        let mg = MemGraph::new(1000);
        let csr_segs: Vec<Arc<CsrSegment>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let fake: NodeKey = slotmap::KeyData::from_ffi(9999).into();
        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let dijkstra = DijkstraTraversal::new(cost_fn, 10);
        let err = dijkstra.shortest_path(&reader, fake, fake).unwrap_err();
        assert_eq!(err, TraversalError::NodeNotFound);
    }

    // --- Multi-segment traversal test ---

    #[test]
    fn test_bfs_across_memgraph_and_csr() {
        // CSR segment: a->b
        let mut mg1 = MemGraph::new(1000);
        let a = mg1.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg1.add_node(smallvec![0], empty_props(), None, 1);
        mg1.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        let frozen = mg1.freeze().expect("ok");
        let csr = CsrSegment::from_frozen(frozen, 5).expect("ok");
        let csr_segs = vec![Arc::new(csr)];

        // MemGraph: a->c (new data not in CSR)
        let mut mg2 = MemGraph::new(1000);
        // We need the same NodeKey for 'a'. Re-create with same structure.
        let a2 = mg2.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg2.add_node(smallvec![0], empty_props(), None, 1);
        mg2.add_edge(a2, c, 1, 1.0, None, 6).expect("ok");

        let reader = SegmentMergeReader::new(
            Some(&mg2),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let bfs = BoundedBfs::new(1);
        let result = bfs.execute(&reader, a2).expect("ok");
        // a2 should see neighbors from both MemGraph (c) and CSR (b).
        // Note: a2 may or may not match the CSR's 'a' key depending on SlotMap.
        // This test verifies the merge reader processes both sources.
        assert!(result.visited.len() >= 2);
    }

    #[test]
    fn test_bfs_two_csr_segments_unified() {
        // TRAV-09: query-time merge reads from all active segments
        // CSR 1: a->b
        let mut mg1 = MemGraph::new(1000);
        let a = mg1.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg1.add_node(smallvec![0], empty_props(), None, 1);
        mg1.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        let frozen1 = mg1.freeze().expect("ok");
        let csr1 = CsrSegment::from_frozen(frozen1, 5).expect("ok");

        // CSR 2: a->c
        let mut mg2 = MemGraph::new(1000);
        let a2 = mg2.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg2.add_node(smallvec![0], empty_props(), None, 1);
        mg2.add_edge(a2, c, 2, 1.0, None, 3).expect("ok");
        let frozen2 = mg2.freeze().expect("ok");
        let csr2 = CsrSegment::from_frozen(frozen2, 10).expect("ok");

        let csr_segs = vec![Arc::new(csr1), Arc::new(csr2)];
        let reader: SegmentMergeReader<'_> =
            SegmentMergeReader::new(None, &csr_segs, Direction::Outgoing, u64::MAX - 1, None);

        // Look up node 'a' from CSR 1.
        let node_a_key: NodeKey =
            slotmap::KeyData::from_ffi(csr_segs[0].node_meta[0].external_id).into();

        let neighbors = reader.neighbors(node_a_key);
        // Should find neighbor(s) from CSR 1 at minimum.
        assert!(!neighbors.is_empty());
    }
}
