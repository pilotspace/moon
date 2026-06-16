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

use dashmap::DashSet;
use parking_lot::Mutex;

use crate::graph::csr::CsrStorage;
use crate::graph::memgraph::MemGraph;
use crate::graph::scoring::WeightedCostFn;
use crate::graph::types::{Direction, EdgeKey, NodeKey};

/// Default maximum frontier size for BFS (100K nodes).
pub const DEFAULT_FRONTIER_CAP: usize = 100_000;

/// Frontier size threshold above which parallel BFS is used.
pub const PARALLEL_THRESHOLD: usize = 256;

/// Number of nodes per morsel in parallel BFS expansion.
pub const MORSEL_SIZE: usize = 128;

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
#[derive(Debug, Clone, Copy)]
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
    /// Wall-clock creation stamp (Unix millis) of the edge. 0 = unknown
    /// (pre-upgrade data or CSR segments without per-edge stamps); decay
    /// scoring treats 0 as neutral (no age penalty).
    pub created_ms: u64,
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
    csr_segments: &'a [Arc<CsrStorage>],
    direction: Direction,
    snapshot_lsn: u64,
    edge_type_filter: Option<u16>,
}

impl<'a> SegmentMergeReader<'a> {
    /// Create a merge reader from a MemGraph and CSR segments.
    pub fn new(
        memgraph: Option<&'a MemGraph>,
        csr_segments: &'a [Arc<CsrStorage>],
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
    /// Allocates per call. For BFS/DFS hot loops, prefer [`neighbors_into`]
    /// which reuses caller-provided scratch buffers.
    pub fn neighbors(&self, node: NodeKey) -> Vec<MergedNeighbor> {
        let mut seen: HashSet<NodeKey> = HashSet::new();
        let mut result: Vec<MergedNeighbor> = Vec::new();
        self.neighbors_inner(node, &mut seen, &mut result);
        result
    }

    /// Zero-allocation neighbor query: clears and fills caller-provided buffers.
    ///
    /// `seen` and `out` are cleared at the start, then reused across calls
    /// to avoid per-node HashSet/Vec allocation in BFS/DFS inner loops.
    #[inline]
    pub fn neighbors_into(
        &self,
        node: NodeKey,
        seen: &mut HashSet<NodeKey>,
        out: &mut Vec<MergedNeighbor>,
    ) {
        seen.clear();
        out.clear();
        self.neighbors_inner(node, seen, out);
    }

    /// Shared implementation for neighbors / neighbors_into.
    fn neighbors_inner(
        &self,
        node: NodeKey,
        seen: &mut HashSet<NodeKey>,
        result: &mut Vec<MergedNeighbor>,
    ) {
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
                            created_ms: edge.created_ms,
                        });
                    }
                }
            }
        }

        // 2. Read from immutable CSR segments (newest first).
        for csr in self.csr_segments {
            // Only include segments created at or before our snapshot.
            if csr.created_lsn() > self.snapshot_lsn {
                continue;
            }
            let Some(row) = csr.lookup_node(node) else {
                continue;
            };

            let edge_type_filter = self.edge_type_filter;
            let snapshot_lsn = self.snapshot_lsn;
            let node_meta = csr.node_meta();
            let csr_lsn = csr.created_lsn();

            // Resolve a CSR row to a visible NodeKey, push as a MergedNeighbor.
            // Shared by the outgoing (target row) and incoming (source row) loops
            // so both apply the same edge-type filter, node visibility, and dedup.
            let mut push_neighbor =
                |row_idx: u32, meta: crate::graph::types::EdgeMeta, created_ms: u64| {
                    if let Some(filter) = edge_type_filter {
                        if meta.edge_type != filter {
                            return;
                        }
                    }
                    if (row_idx as usize) < node_meta.len() {
                        let meta_n = &node_meta[row_idx as usize];
                        // Skip nodes deleted at/before the snapshot.
                        if meta_n.deleted_lsn != u64::MAX && meta_n.deleted_lsn <= snapshot_lsn {
                            return;
                        }
                        let key: NodeKey = slotmap::KeyData::from_ffi(meta_n.external_id).into();
                        if seen.insert(key) {
                            result.push(MergedNeighbor {
                                node: key,
                                edge: slotmap::KeyData::from_ffi(0).into(),
                                edge_type: meta.edge_type,
                                weight: 1.0,
                                timestamp: csr_lsn,
                                // Real per-edge wall-clock stamp from the version
                                // >= 3 segment format; 0 = unknown (pre-v3 file),
                                // which decay treats as neutral.
                                created_ms,
                            });
                        }
                    }
                };

            // Outgoing successors (Outgoing | Both): the forward CSR row.
            if self.direction != Direction::Incoming {
                csr.for_each_neighbor_edge_ms(row, |col_idx, meta, created_ms| {
                    push_neighbor(col_idx, meta, created_ms);
                });
            }

            // Incoming predecessors (Incoming | Both): the DERIVED reverse index.
            // CSR stores only outgoing adjacency; for_each_incoming_edge_ms serves
            // predecessors from a lazily-built reverse index over the same forward
            // arrays, so post-compaction Incoming/Both no longer drop these edges.
            if self.direction != Direction::Outgoing {
                csr.for_each_incoming_edge_ms(row, |src_idx, meta, created_ms| {
                    push_neighbor(src_idx, meta, created_ms);
                });
            }
        }
    }

    /// Hint sequential access on all CSR segments (for BFS/DFS traversals).
    pub fn madvise_sequential(&self) {
        for csr in self.csr_segments {
            csr.madvise_sequential();
        }
    }

    /// Hint random access on all CSR segments (for point lookups).
    pub fn madvise_random(&self) {
        for csr in self.csr_segments {
            csr.madvise_random();
        }
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

        // Hint the OS for sequential page access on mmap'd CSR segments.
        reader.madvise_sequential();

        let mut visited_set: HashSet<NodeKey> = HashSet::new();
        visited_set.insert(start);

        let mut result = Vec::new();
        result.push((start, 0, None));

        let mut frontier: VecDeque<(NodeKey, u32)> = VecDeque::new();
        frontier.push_back((start, 0));

        // Scratch buffers reused across all neighbor lookups (avoids per-node alloc).
        let mut nb_seen: HashSet<NodeKey> = HashSet::new();
        let mut nb_buf: Vec<MergedNeighbor> = Vec::new();

        while let Some((current, depth)) = frontier.pop_front() {
            if depth >= self.depth_limit {
                continue;
            }

            reader.neighbors_into(current, &mut nb_seen, &mut nb_buf);
            for &neighbor in &nb_buf {
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
                result.push((neighbor.node, next_depth, Some(neighbor)));
                frontier.push_back((neighbor.node, next_depth));
            }
        }

        Ok(BfsResult { visited: result })
    }
}

// ---------------------------------------------------------------------------
// Parallel BFS (PAR-01 through PAR-04)
// ---------------------------------------------------------------------------

/// Morsel-driven parallel BFS for large frontiers.
///
/// When the frontier exceeds [`PARALLEL_THRESHOLD`] (256) nodes, neighbor-list
/// processing is split into [`MORSEL_SIZE`] (128) node morsels handled in
/// parallel via `std::thread::scope`. Results merge into a shared
/// `DashSet<NodeKey>` visited set. Falls back to sequential BFS for small
/// frontiers (PAR-03), avoiding any parallelism overhead.
///
/// **Design note:** `SegmentMergeReader` borrows `&MemGraph` which is `!Send`,
/// so `reader.neighbors()` calls remain on the main thread. The parallel phase
/// processes the *already-collected* neighbor lists (owned `Vec<MergedNeighbor>`)
/// across worker threads for visited-set filtering and result building.
pub struct ParallelBfs {
    pub depth_limit: u32,
    pub frontier_cap: usize,
}

impl ParallelBfs {
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

    /// Execute parallel BFS from a start node.
    ///
    /// For frontiers larger than [`PARALLEL_THRESHOLD`], neighbor lists are
    /// collected sequentially (reader is `!Send`) then processed in parallel
    /// morsels via `std::thread::scope`.
    pub fn execute(
        &self,
        reader: &SegmentMergeReader<'_>,
        start: NodeKey,
    ) -> Result<BfsResult, TraversalError> {
        if !reader.node_exists(start) {
            return Err(TraversalError::NodeNotFound);
        }

        reader.madvise_sequential();

        let mut visited_set: HashSet<NodeKey> = HashSet::new();
        visited_set.insert(start);

        let mut result: Vec<(NodeKey, u32, Option<MergedNeighbor>)> = vec![(start, 0, None)];
        let mut frontier: Vec<NodeKey> = vec![start];
        let mut depth: u32 = 0;

        // Scratch buffers reused across all neighbor lookups.
        let mut nb_seen: HashSet<NodeKey> = HashSet::new();
        let mut nb_buf: Vec<MergedNeighbor> = Vec::new();

        while !frontier.is_empty() {
            if depth >= self.depth_limit {
                break;
            }

            let next_depth = depth + 1;

            if frontier.len() <= PARALLEL_THRESHOLD {
                // --- Sequential path: plain HashSet, no DashSet/Mutex overhead ---
                let mut next_frontier = Vec::new();
                for &node in &frontier {
                    reader.neighbors_into(node, &mut nb_seen, &mut nb_buf);
                    for &neighbor in &nb_buf {
                        if visited_set.insert(neighbor.node) {
                            result.push((neighbor.node, next_depth, Some(neighbor)));
                            next_frontier.push(neighbor.node);
                        }
                    }
                }
                frontier = next_frontier;
            } else {
                // --- Parallel path: collect neighbors sequentially (reader is !Send),
                //     then process in parallel morsels via std::thread::scope ---
                let neighbor_lists: Vec<Vec<MergedNeighbor>> = frontier
                    .iter()
                    .map(|&node| {
                        reader.neighbors_into(node, &mut nb_seen, &mut nb_buf);
                        nb_buf.clone()
                    })
                    .collect();

                // Upgrade to DashSet only for the parallel phase.
                let dash_visited: DashSet<NodeKey> = DashSet::with_capacity(visited_set.len());
                for &k in &visited_set {
                    dash_visited.insert(k);
                }

                let per_morsel_results: Mutex<Vec<Vec<(NodeKey, u32, Option<MergedNeighbor>)>>> =
                    Mutex::new(Vec::new());

                std::thread::scope(|s| {
                    for chunk in neighbor_lists.chunks(MORSEL_SIZE) {
                        let visited_ref = &dash_visited;
                        let per_morsel_ref = &per_morsel_results;
                        s.spawn(move || {
                            let mut local: Vec<(NodeKey, u32, Option<MergedNeighbor>)> = Vec::new();
                            for neighbors in chunk {
                                for &neighbor in neighbors {
                                    if visited_ref.insert(neighbor.node) {
                                        local.push((neighbor.node, next_depth, Some(neighbor)));
                                    }
                                }
                            }
                            per_morsel_ref.lock().push(local);
                        });
                    }
                });

                // Merge morsel results back into sequential structures.
                let morsel_vecs = per_morsel_results.into_inner();
                let mut next_frontier = Vec::new();
                for morsel in &morsel_vecs {
                    for entry in morsel {
                        next_frontier.push(entry.0);
                        visited_set.insert(entry.0);
                        result.push(*entry);
                    }
                }
                frontier = next_frontier;
            }

            // Check frontier cap.
            if visited_set.len() >= self.frontier_cap {
                return Err(TraversalError::FrontierCapExceeded {
                    cap: self.frontier_cap,
                    depth: next_depth,
                });
            }

            depth = next_depth;
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

        // Scratch buffers reused across all neighbor lookups.
        let mut nb_seen: HashSet<NodeKey> = HashSet::new();
        let mut nb_buf: Vec<MergedNeighbor> = Vec::new();

        while let Some((current, depth, cum_cost)) = stack.pop() {
            if visited_set.contains(&current) {
                continue;
            }
            visited_set.insert(current);
            result.push((current, depth, cum_cost));

            if depth >= self.depth_limit {
                continue;
            }

            reader.neighbors_into(current, &mut nb_seen, &mut nb_buf);
            for &neighbor in &nb_buf {
                if visited_set.contains(&neighbor.node) {
                    continue;
                }
                let edge_cost = self.cost_fn.cost_ms(neighbor.created_ms, neighbor.weight);
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
                    // Both NaN checks returned false — logically unreachable since
                    // partial_cmp returns None only when at least one operand is NaN.
                    _ => Ordering::Equal,
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

        // Scratch buffers reused across all neighbor lookups.
        let mut nb_seen: HashSet<NodeKey> = HashSet::new();
        let mut nb_buf: Vec<MergedNeighbor> = Vec::new();

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

            reader.neighbors_into(node, &mut nb_seen, &mut nb_buf);
            for &neighbor in &nb_buf {
                let edge_cost = self.cost_fn.cost_ms(neighbor.created_ms, neighbor.weight);
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
    use crate::graph::csr::{CsrSegment, CsrStorage};
    use crate::graph::memgraph::MemGraph;
    use crate::graph::scoring::WeightedCostFn;
    use slotmap::Key;
    use smallvec::smallvec;

    fn empty_props() -> crate::graph::types::PropertyMap {
        smallvec![]
    }

    // --- SegmentMergeReader tests ---

    #[test]
    fn test_dijkstra_decay_prefers_fresh_path() {
        // Topology: direct a->c (weight 1.0, STALE, created at t=1s) vs
        // detour a->b->c (weight 0.6 each, FRESH, created at t=99s).
        //
        // Decay OFF (time_weight = 0): direct wins (1.0 < 1.2).
        // Decay ON  (time_weight = 0.01/sec, now = 100s):
        //   direct: 1.0 + 0.01 * 99s            = 1.99
        //   detour: 1.2 + 0.01 * (1s + 1s)      = 1.22  -> detour wins.
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);

        {
            let _pin = crate::storage::entry::ClockPin::set(1, 1_000);
            mg.add_edge(a, c, 1, 1.0, None, 2).expect("stale direct");
        }
        {
            let _pin = crate::storage::entry::ClockPin::set(99, 99_000);
            mg.add_edge(a, b, 1, 0.6, None, 3).expect("fresh hop 1");
            mg.add_edge(b, c, 1, 0.6, None, 4).expect("fresh hop 2");
        }

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        // Decay off: direct path.
        let off = DijkstraTraversal::new(WeightedCostFn::new(0.0, 1.0, 0), 10)
            .shortest_path(&reader, a, c)
            .expect("ok")
            .expect("path");
        assert_eq!(
            off.path,
            vec![a, c],
            "decay off must keep distance-only behavior"
        );

        // Decay on: fresh detour wins.
        let on = DijkstraTraversal::new(WeightedCostFn::new(0.01, 1.0, 100_000), 10)
            .shortest_path(&reader, a, c)
            .expect("ok")
            .expect("path");
        assert_eq!(
            on.path,
            vec![a, b, c],
            "decay must steer toward fresh edges"
        );
    }

    #[test]
    fn test_merge_reader_propagates_created_ms() {
        // MemGraph edges must surface their wall-clock creation stamp through
        // MergedNeighbor so decay-aware cost functions can age them.
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        {
            let _pin = crate::storage::entry::ClockPin::set(7, 7_000);
            mg.add_edge(a, b, 1, 2.0, None, 2).expect("ok");
        }

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let neighbors = reader.neighbors(a);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(
            neighbors[0].created_ms, 7_000,
            "MergedNeighbor must carry the edge's created_ms"
        );
    }

    #[test]
    fn test_merge_reader_memgraph_only() {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 2.0, None, 2).expect("ok");
        mg.add_edge(a, c, 1, 3.0, None, 3).expect("ok");

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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
        let csr_segs = vec![Arc::new(CsrStorage::from(csr))];

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

        let csr_segs = vec![Arc::new(CsrStorage::from(csr))];
        let reader: SegmentMergeReader<'_> =
            SegmentMergeReader::new(None, &csr_segs, Direction::Outgoing, u64::MAX - 1, None);

        // Look up the first node's key (row 0 in CSR).
        let node_a_key = {
            let meta = &csr_segs[0].node_meta()[0];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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
        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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
        // TRAV-03/04: time_weight * age_seconds + distance_weight * distance.
        // Edge ages come from wall-clock created_ms (pinned cached clock);
        // cost_ms converts ms -> seconds.
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);

        // Route 1: a->c, weight=2.0, created at t=90s (age=10s at now=100s)
        //   cost = 0.5*10 + 1.0*2.0 = 7.0
        {
            let _pin = crate::storage::entry::ClockPin::set(90, 90_000);
            mg.add_edge(a, c, 1, 2.0, None, 90).expect("ok");
        }

        // Route 2: a->b (weight=1.0, t=95s, age=5s) -> b->c (weight=1.0, t=80s, age=20s)
        //   a->b cost = 0.5*5 + 1.0*1.0 = 3.5
        //   b->c cost = 0.5*20 + 1.0*1.0 = 11.0
        //   total = 14.5
        {
            let _pin = crate::storage::entry::ClockPin::set(95, 95_000);
            mg.add_edge(a, b, 1, 1.0, None, 95).expect("ok");
        }
        {
            let _pin = crate::storage::entry::ClockPin::set(80, 80_000);
            mg.add_edge(b, c, 1, 1.0, None, 80).expect("ok");
        }

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let cost_fn = WeightedCostFn::new(0.5, 1.0, 100_000);
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
        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
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
        let csr_segs = vec![Arc::new(CsrStorage::from(csr))];

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

        let csr_segs = vec![
            Arc::new(CsrStorage::from(csr1)),
            Arc::new(CsrStorage::from(csr2)),
        ];
        let reader: SegmentMergeReader<'_> =
            SegmentMergeReader::new(None, &csr_segs, Direction::Outgoing, u64::MAX - 1, None);

        // Look up node 'a' from CSR 1.
        let node_a_key: NodeKey =
            slotmap::KeyData::from_ffi(csr_segs[0].node_meta()[0].external_id).into();

        let neighbors = reader.neighbors(node_a_key);
        // Should find neighbor(s) from CSR 1 at minimum.
        assert!(!neighbors.is_empty());
    }

    #[test]
    fn test_csr_neighbors_surface_edge_created_ms() {
        // Edges frozen into a CSR segment keep their wall-clock stamps (v3
        // format) — the merge reader must surface them on MergedNeighbor so
        // decay scoring sees true edge age after compaction.
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        {
            let _pin = crate::storage::entry::ClockPin::set(42, 42_000);
            mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        }

        let frozen = mg.freeze().expect("ok");
        let csr = CsrSegment::from_frozen(frozen, 5).expect("ok");
        let csr_segs = vec![Arc::new(CsrStorage::from(csr))];
        let reader: SegmentMergeReader<'_> =
            SegmentMergeReader::new(None, &csr_segs, Direction::Outgoing, u64::MAX - 1, None);

        let neighbors = reader.neighbors(a);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(
            neighbors[0].created_ms, 42_000,
            "CSR neighbor must carry the persisted per-edge stamp"
        );
    }

    // --- ParallelBfs tests ---

    #[test]
    fn test_parallel_bfs_small_frontier_matches_sequential() {
        // Build a small graph (10 nodes in a chain with some branches).
        let mut mg = MemGraph::new(1000);
        let mut nodes = Vec::new();
        for _ in 0..10 {
            nodes.push(mg.add_node(smallvec![0], empty_props(), None, 1));
        }
        // Chain: 0->1->2->3->4
        for i in 0..4 {
            mg.add_edge(nodes[i], nodes[i + 1], 1, 1.0, None, 2)
                .expect("ok");
        }
        // Branch: 0->5, 0->6, 1->7, 2->8, 3->9
        mg.add_edge(nodes[0], nodes[5], 1, 1.0, None, 2)
            .expect("ok");
        mg.add_edge(nodes[0], nodes[6], 1, 1.0, None, 2)
            .expect("ok");
        mg.add_edge(nodes[1], nodes[7], 1, 1.0, None, 2)
            .expect("ok");
        mg.add_edge(nodes[2], nodes[8], 1, 1.0, None, 2)
            .expect("ok");
        mg.add_edge(nodes[3], nodes[9], 1, 1.0, None, 2)
            .expect("ok");

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let seq_bfs = BoundedBfs::new(3);
        let seq_result = seq_bfs.execute(&reader, nodes[0]).expect("ok");

        let par_bfs = super::ParallelBfs::new(3);
        let par_result = par_bfs.execute(&reader, nodes[0]).expect("ok");

        // Compare visited sets (ignore order).
        let mut seq_nodes: Vec<NodeKey> = seq_result.visited.iter().map(|v| v.0).collect();
        let mut par_nodes: Vec<NodeKey> = par_result.visited.iter().map(|v| v.0).collect();
        seq_nodes.sort_by_key(|k| k.data().as_ffi());
        par_nodes.sort_by_key(|k| k.data().as_ffi());
        assert_eq!(seq_nodes, par_nodes);
    }

    #[test]
    fn test_parallel_bfs_large_frontier_triggers_parallel() {
        // Build a hub node connected to 350+ nodes (> PARALLEL_THRESHOLD).
        let mut mg = MemGraph::new(100_000);
        let hub = mg.add_node(smallvec![0], empty_props(), None, 1);
        let leaf_count = 350;
        let mut leaves = Vec::with_capacity(leaf_count);
        for i in 0..leaf_count {
            let leaf = mg.add_node(smallvec![0], empty_props(), None, (i + 2) as u64);
            mg.add_edge(hub, leaf, 1, 1.0, None, 2).expect("ok");
            leaves.push(leaf);
        }

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let par_bfs = super::ParallelBfs::new(1);
        let result = par_bfs.execute(&reader, hub).expect("ok");

        // Hub + all 350 leaves = 351
        assert_eq!(result.visited.len(), leaf_count + 1);
    }

    #[test]
    fn test_parallel_bfs_respects_frontier_cap() {
        // Set cap to 50, build a star that exceeds it.
        let mut mg = MemGraph::new(100_000);
        let hub = mg.add_node(smallvec![0], empty_props(), None, 1);
        for i in 0..100 {
            let leaf = mg.add_node(smallvec![0], empty_props(), None, (i + 2) as u64);
            mg.add_edge(hub, leaf, 1, 1.0, None, 2).expect("ok");
        }

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let par_bfs = super::ParallelBfs::with_cap(2, 50);
        let err = par_bfs.execute(&reader, hub).unwrap_err();
        match err {
            TraversalError::FrontierCapExceeded { cap, depth: _ } => {
                assert_eq!(cap, 50);
            }
            other => panic!("expected FrontierCapExceeded, got {other:?}"),
        }
    }

    #[test]
    fn test_parallel_bfs_sequential_fallback() {
        // Frontier < 256 nodes -- verify correctness matches BoundedBfs.
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        let d = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, b, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(a, c, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(b, d, 1, 1.0, None, 2).expect("ok");
        mg.add_edge(c, d, 1, 1.0, None, 2).expect("ok");

        let csr_segs: Vec<Arc<CsrStorage>> = vec![];
        let reader = SegmentMergeReader::new(
            Some(&mg),
            &csr_segs,
            Direction::Outgoing,
            u64::MAX - 1,
            None,
        );

        let seq = BoundedBfs::new(3);
        let seq_result = seq.execute(&reader, a).expect("ok");

        let par = super::ParallelBfs::new(3);
        let par_result = par.execute(&reader, a).expect("ok");

        let mut seq_nodes: Vec<NodeKey> = seq_result.visited.iter().map(|v| v.0).collect();
        let mut par_nodes: Vec<NodeKey> = par_result.visited.iter().map(|v| v.0).collect();
        seq_nodes.sort_by_key(|k| k.data().as_ffi());
        par_nodes.sort_by_key(|k| k.data().as_ffi());
        assert_eq!(seq_nodes, par_nodes);
    }

    // ─── graph-incoming-edges (v3-2): Incoming/Both on a compacted CSR segment ─────
    // Frozen contract: SegmentMergeReader returns predecessors for Direction::Incoming
    // and the predecessor∪successor union for Direction::Both on a compacted segment,
    // via a derived (not persisted) reverse index. RED until the fix — today the CSR
    // branch `continue`s on Incoming and drops the incoming half of Both.
    //
    // Corpus G: A-[R1]->C, B-[R2]->C, C-[R1]->D  (R1=1, R2=2), frozen into ONE segment.
    // Predecessors(C)={A,B}; Successors(C)={D}; Predecessors(A)={}.

    fn build_corpus_g_csr(created_lsn: u64) -> (CsrSegment, NodeKey, NodeKey, NodeKey, NodeKey) {
        let mut mg = MemGraph::new(1000);
        let a = mg.add_node(smallvec![0], empty_props(), None, 1);
        let b = mg.add_node(smallvec![0], empty_props(), None, 1);
        let c = mg.add_node(smallvec![0], empty_props(), None, 1);
        let d = mg.add_node(smallvec![0], empty_props(), None, 1);
        mg.add_edge(a, c, 1, 1.0, None, 2).expect("A-[R1]->C");
        mg.add_edge(b, c, 2, 1.0, None, 2).expect("B-[R2]->C");
        mg.add_edge(c, d, 1, 1.0, None, 2).expect("C-[R1]->D");
        let frozen = mg.freeze().expect("freeze");
        let csr = CsrSegment::from_frozen(frozen, created_lsn).expect("from_frozen");
        (csr, a, b, c, d)
    }

    fn incoming_set(
        segs: &[Arc<CsrStorage>],
        node: NodeKey,
        dir: Direction,
        snapshot_lsn: u64,
        filter: Option<u16>,
    ) -> std::collections::HashSet<NodeKey> {
        let reader = SegmentMergeReader::new(None, segs, dir, snapshot_lsn, filter);
        reader.neighbors(node).into_iter().map(|n| n.node).collect()
    }

    #[test]
    fn test_incoming_returns_predecessors_post_compaction() {
        // M1 — Incoming(C) on a compacted segment returns C's predecessors {A, B}.
        let (csr, a, b, c, _d) = build_corpus_g_csr(5);
        let segs = vec![Arc::new(CsrStorage::from(csr))];
        let got = incoming_set(&segs, c, Direction::Incoming, u64::MAX - 1, None);
        assert_eq!(
            got,
            std::collections::HashSet::from([a, b]),
            "Incoming(C) must be predecessors {{A,B}} post-compaction; got {got:?}"
        );
    }

    #[test]
    fn test_both_returns_union_of_pred_and_succ() {
        // M2 — Both(C) returns predecessors {A,B} ∪ successors {D}.
        let (csr, a, b, c, d) = build_corpus_g_csr(5);
        let segs = vec![Arc::new(CsrStorage::from(csr))];
        let got = incoming_set(&segs, c, Direction::Both, u64::MAX - 1, None);
        assert_eq!(
            got,
            std::collections::HashSet::from([a, b, d]),
            "Both(C) must be {{A,B,D}} (predecessors ∪ successors); got {got:?}"
        );
    }

    #[test]
    fn test_incoming_honors_edge_type_validity_and_node_visibility() {
        // M3 — edge-type filter, tombstone, and deleted-node visibility on the incoming path.
        // (a) edge_type filter R1=1 keeps only A (B's edge into C is R2=2).
        let (csr, a, _b, c, _d) = build_corpus_g_csr(5);
        let segs = vec![Arc::new(CsrStorage::from(csr))];
        let reader =
            SegmentMergeReader::new(None, &segs, Direction::Incoming, u64::MAX - 1, Some(1));
        let inc = reader.neighbors(c);
        let got: std::collections::HashSet<NodeKey> = inc.iter().map(|n| n.node).collect();
        assert_eq!(
            got,
            std::collections::HashSet::from([a]),
            "edge_type=R1 filter keeps only A on the incoming path; got {got:?}"
        );
        assert!(
            inc.iter().all(|n| n.edge_type == 1),
            "each incoming result carries its real edge_type (R1)"
        );

        // (b) tombstone A->C (clear its validity bit): Incoming(C) -> {B}.
        let (mut csr2, a2, b2, c2, _d2) = build_corpus_g_csr(5);
        let a_row = csr2.lookup_node(a2).expect("A is in the segment");
        let a_first_edge = csr2.row_offsets[a_row as usize]; // A's only out-edge is A->C
        csr2.mark_deleted(a_first_edge);
        let segs2 = vec![Arc::new(CsrStorage::from(csr2))];
        assert_eq!(
            incoming_set(&segs2, c2, Direction::Incoming, u64::MAX - 1, None),
            std::collections::HashSet::from([b2]),
            "after tombstoning A->C, Incoming(C) = {{B}}"
        );

        // (c) soft-delete node A (deleted_lsn <= snapshot): Incoming(C) excludes A.
        let (mut csr3, a3, b3, c3, _d3) = build_corpus_g_csr(5);
        let a_row3 = csr3.lookup_node(a3).expect("A is in the segment") as usize;
        csr3.node_meta[a_row3].deleted_lsn = 1; // deleted at lsn 1 <= snapshot
        let segs3 = vec![Arc::new(CsrStorage::from(csr3))];
        assert_eq!(
            incoming_set(&segs3, c3, Direction::Incoming, u64::MAX - 1, None),
            std::collections::HashSet::from([b3]),
            "a predecessor deleted at/before the snapshot is excluded from Incoming(C)"
        );
    }

    #[test]
    fn test_incoming_respects_segment_snapshot() {
        // M4 — a segment with created_lsn=10 is skipped at snapshot 5, included at 10.
        let (csr, a, b, c, _d) = build_corpus_g_csr(10);
        let segs = vec![Arc::new(CsrStorage::from(csr))];
        assert!(
            incoming_set(&segs, c, Direction::Incoming, 5, None).is_empty(),
            "segment created_lsn=10 must be skipped at snapshot 5 (same rule as outgoing)"
        );
        assert_eq!(
            incoming_set(&segs, c, Direction::Incoming, 10, None),
            std::collections::HashSet::from([a, b]),
            "at snapshot 10 the segment contributes {{A,B}}"
        );
    }

    #[test]
    fn test_outgoing_unchanged() {
        // M5 (green-pin) — Direction::Outgoing of C is {D}, unchanged by this work.
        let (csr, _a, _b, c, d) = build_corpus_g_csr(5);
        let segs = vec![Arc::new(CsrStorage::from(csr))];
        assert_eq!(
            incoming_set(&segs, c, Direction::Outgoing, u64::MAX - 1, None),
            std::collections::HashSet::from([d]),
            "Outgoing(C) = {{D}}, byte-identical to today"
        );
    }

    #[test]
    fn test_incoming_after_to_bytes_from_bytes_roundtrip() {
        // M6 — Incoming works on a heap segment round-tripped through to_bytes/from_bytes,
        // proving the incoming answer is DERIVED from the existing forward arrays (not from
        // any newly-persisted incoming data) and the on-disk format/version is unchanged.
        let (csr, a, b, c, _d) = build_corpus_g_csr(5);
        let bytes = csr.to_bytes();
        let reloaded = CsrSegment::from_bytes(&bytes).expect("from_bytes parses unchanged format");
        let segs = vec![Arc::new(CsrStorage::from(reloaded))];
        assert_eq!(
            incoming_set(&segs, c, Direction::Incoming, u64::MAX - 1, None),
            std::collections::HashSet::from([a, b]),
            "Incoming(C) = {{A,B}} after a to_bytes/from_bytes round-trip"
        );
    }

    #[test]
    fn test_incoming_degenerate_no_panic() {
        // Reject / robustness (green-pin) — degenerate Incoming queries must not panic.
        // NOTE: self-loops are unreachable here — MemGraph::add_edge rejects src==dst with
        // `SelfLoop`, so a self-loop CSR segment cannot exist. We exercise the reachable
        // degenerate shapes instead: a node with NO predecessors and a node absent from the
        // segment. Both return an empty incoming contribution without panicking.
        let (csr, a, _b, _c, _d) = build_corpus_g_csr(5);
        let segs = vec![Arc::new(CsrStorage::from(csr))];

        // A has no incoming edges -> empty, no panic.
        assert!(
            incoming_set(&segs, a, Direction::Incoming, u64::MAX - 1, None).is_empty(),
            "a node with no predecessors yields an empty incoming set; must not panic"
        );
        // A node absent from the segment contributes nothing, no panic.
        let absent: NodeKey = slotmap::KeyData::from_ffi(0xDEAD_BEEF).into();
        assert!(
            incoming_set(&segs, absent, Direction::Incoming, u64::MAX - 1, None).is_empty(),
            "an absent node yields an empty incoming contribution, no panic"
        );
    }
}
