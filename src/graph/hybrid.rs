//! Hybrid graph+vector query engine.
//!
//! Four query patterns (HYB-01 through HYB-04):
//! 1. Graph-filtered vector search: traverse N hops -> collect candidates -> score
//! 2. Vector-to-graph expansion: score candidates -> expand N hops for context
//! 3. Vector-guided walk: beam search guided by embedding distance
//! 4. Automatic strategy selection based on candidate set size threshold
//!
//! All functions take references to MemGraph (graph data + embeddings) and operate
//! without unsafe code or unwrap. The shard command handler passes MemGraph directly
//! since both GraphStore and VectorStore are per-shard, single-owner.

use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

use super::simd;

use crate::graph::memgraph::MemGraph;
use crate::graph::types::{Direction, NodeKey};

/// Default threshold for switching between brute-force and HNSW pre-filter.
pub const DEFAULT_STRATEGY_THRESHOLD: usize = 10_000;

// ---------------------------------------------------------------------------
// Error and result types
// ---------------------------------------------------------------------------

/// Errors from hybrid query execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HybridError {
    /// Start node not found in graph.
    NodeNotFound,
    /// No embedding found on a candidate node.
    NoEmbedding,
    /// Query vector dimension mismatch.
    DimensionMismatch { expected: usize, got: usize },
    /// Traversal exceeded frontier cap.
    FrontierCapExceeded { cap: usize },
    /// Empty query vector.
    EmptyQueryVector,
}

impl core::fmt::Display for HybridError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NodeNotFound => write!(f, "start node not found"),
            Self::NoEmbedding => write!(f, "candidate node has no embedding"),
            Self::DimensionMismatch { expected, got } => {
                write!(f, "dimension mismatch: expected {expected}, got {got}")
            }
            Self::FrontierCapExceeded { cap } => {
                write!(f, "frontier cap {cap} exceeded")
            }
            Self::EmptyQueryVector => write!(f, "empty query vector"),
        }
    }
}

/// A single hybrid query result: a scored node with optional graph context.
#[derive(Debug, Clone)]
pub struct HybridResult {
    /// The result node key.
    pub node: NodeKey,
    /// Combined score (higher = better).
    pub score: f64,
    /// Graph distance in hops from the origin (if applicable).
    pub graph_distance: Option<u32>,
    /// Context neighbors discovered during expansion (HYB-02).
    pub context: Vec<ContextNode>,
}

/// A context node discovered during graph expansion.
#[derive(Debug, Clone)]
pub struct ContextNode {
    /// Node key.
    pub node: NodeKey,
    /// Edge type used to reach this node.
    pub edge_type: u16,
    /// Hops from the result node.
    pub hops: u32,
}

// ---------------------------------------------------------------------------
// HYB-04: Strategy selector
// ---------------------------------------------------------------------------

/// Strategy for scoring candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStrategy {
    /// Score all candidates via brute-force cosine similarity.
    BruteForce,
    /// Use HNSW pre-filter (skip non-candidates during search).
    /// For now, falls back to brute-force with candidate filtering since
    /// the graph MemGraph embeddings are not indexed in HNSW.
    HnswPreFilter,
}

/// Select the scoring strategy based on candidate set size.
///
/// When candidates < threshold: brute-force (fast for small sets).
/// When candidates >= threshold: HNSW pre-filter (avoids scanning all).
#[inline]
pub fn select_strategy(candidate_count: usize, threshold: usize) -> FilterStrategy {
    if candidate_count < threshold {
        FilterStrategy::BruteForce
    } else {
        FilterStrategy::HnswPreFilter
    }
}

// ---------------------------------------------------------------------------
// HYB-01: Graph-filtered vector search
// ---------------------------------------------------------------------------

/// Configuration for graph-filtered vector search.
pub struct GraphFilteredSearch {
    /// Start node for graph traversal.
    pub start_node: NodeKey,
    /// Maximum traversal depth (hops).
    pub hops: u32,
    /// Optional edge type filter.
    pub edge_type_filter: Option<u16>,
    /// Query vector for similarity scoring.
    pub query_vector: Vec<f32>,
    /// Number of top results to return.
    pub k: usize,
    /// Strategy selection threshold (default 10K).
    pub threshold: usize,
    /// Maximum frontier size to prevent OOM.
    pub frontier_cap: usize,
}

impl GraphFilteredSearch {
    /// Create with defaults (threshold=10K, frontier_cap=100K).
    pub fn new(start_node: NodeKey, hops: u32, query_vector: Vec<f32>, k: usize) -> Self {
        Self {
            start_node,
            hops,
            edge_type_filter: None,
            query_vector,
            k,
            threshold: DEFAULT_STRATEGY_THRESHOLD,
            frontier_cap: 100_000,
        }
    }

    /// Execute graph-filtered vector search.
    ///
    /// 1. BFS N hops from start_node -> collect candidate NodeKeys
    /// 2. Auto-select strategy (brute-force vs pre-filter)
    /// 3. Score candidates by cosine similarity to query_vector
    /// 4. Return top-K results
    pub fn execute(&self, memgraph: &MemGraph, lsn: u64) -> Result<Vec<HybridResult>, HybridError> {
        if self.query_vector.is_empty() {
            return Err(HybridError::EmptyQueryVector);
        }

        // Verify start node exists.
        if memgraph.get_node(self.start_node).is_none() {
            return Err(HybridError::NodeNotFound);
        }

        // Step 1: BFS to collect candidates with graph distance.
        let candidates = bfs_collect(
            memgraph,
            self.start_node,
            self.hops,
            self.edge_type_filter,
            self.frontier_cap,
            lsn,
        )?;

        // Step 2: Select strategy.
        let _strategy = select_strategy(candidates.len(), self.threshold);
        // Both strategies score the same way for MemGraph embeddings.
        // HNSW pre-filter would be used when an external VectorStore index exists.
        // For MemGraph-embedded vectors, brute-force is always used.

        // Step 3: Score candidates by cosine similarity.
        let mut scored: Vec<HybridResult> = Vec::with_capacity(candidates.len());

        for (node_key, graph_dist) in &candidates {
            let Some(node) = memgraph.get_node(*node_key) else {
                continue;
            };
            let Some(embedding) = node.embedding.as_ref() else {
                continue; // Skip nodes without embeddings.
            };

            let sim = simd::cosine_similarity(embedding, &self.query_vector);
            scored.push(HybridResult {
                node: *node_key,
                score: sim,
                graph_distance: Some(*graph_dist),
                context: Vec::new(),
            });
        }

        // Step 4: Sort descending by score, take top-K.
        scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        scored.truncate(self.k);

        Ok(scored)
    }
}

// ---------------------------------------------------------------------------
// HYB-02: Vector-to-graph expansion
// ---------------------------------------------------------------------------

/// Configuration for vector-to-graph expansion.
pub struct VectorToGraphExpansion {
    /// Query vector for initial similarity search.
    pub query_vector: Vec<f32>,
    /// Number of top vector results before expansion.
    pub k: usize,
    /// Expansion depth (hops from each result).
    pub expansion_hops: u32,
    /// Optional edge type filter for expansion.
    pub edge_type_filter: Option<u16>,
}

impl VectorToGraphExpansion {
    /// Create with defaults.
    pub fn new(query_vector: Vec<f32>, k: usize, expansion_hops: u32) -> Self {
        Self {
            query_vector,
            k,
            expansion_hops,
            edge_type_filter: None,
        }
    }

    /// Execute vector-to-graph expansion.
    ///
    /// 1. Brute-force search all nodes with embeddings for top-K by similarity
    /// 2. For each result, BFS expand N hops for context
    /// 3. Return results with context neighbors
    ///
    /// `candidate_nodes` is a pre-collected list of all node keys to search.
    /// The caller should provide this (e.g., from MemGraph iteration or label index).
    pub fn execute(
        &self,
        memgraph: &MemGraph,
        candidate_nodes: &[NodeKey],
        lsn: u64,
    ) -> Result<Vec<HybridResult>, HybridError> {
        if self.query_vector.is_empty() {
            return Err(HybridError::EmptyQueryVector);
        }

        // Step 1: Score all candidate nodes by cosine similarity.
        let mut scored: Vec<(NodeKey, f64)> = Vec::with_capacity(candidate_nodes.len());

        for &node_key in candidate_nodes {
            let Some(node) = memgraph.get_node(node_key) else {
                continue;
            };
            if node.deleted_lsn != u64::MAX {
                continue;
            }
            let Some(embedding) = node.embedding.as_ref() else {
                continue;
            };

            let sim = simd::cosine_similarity(embedding, &self.query_vector);
            scored.push((node_key, sim));
        }

        // Top-K by similarity.
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        scored.truncate(self.k);

        // Step 2: Expand each result by N hops.
        let mut results: Vec<HybridResult> = Vec::with_capacity(scored.len());

        for (node_key, score) in scored {
            let context = if self.expansion_hops > 0 {
                collect_context(
                    memgraph,
                    node_key,
                    self.expansion_hops,
                    self.edge_type_filter,
                    lsn,
                )
            } else {
                Vec::new()
            };

            results.push(HybridResult {
                node: node_key,
                score,
                graph_distance: None,
                context,
            });
        }

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// HYB-03: Vector-guided walk (beam search)
// ---------------------------------------------------------------------------

/// Configuration for vector-guided graph walk.
pub struct VectorGuidedWalk {
    /// Seed node to start the walk.
    pub seed_node: NodeKey,
    /// Query vector: walk toward neighbors most similar to this.
    pub query_vector: Vec<f32>,
    /// Maximum walk depth.
    pub max_depth: u32,
    /// Beam width: how many candidates to expand at each step.
    pub beam_width: usize,
    /// Minimum similarity threshold: stop walking if best neighbor is below this.
    pub min_similarity: f64,
}

impl VectorGuidedWalk {
    /// Create with defaults (beam_width=5, min_similarity=0.0).
    pub fn new(seed_node: NodeKey, query_vector: Vec<f32>, max_depth: u32) -> Self {
        Self {
            seed_node,
            query_vector,
            max_depth,
            beam_width: 5,
            min_similarity: 0.0,
        }
    }

    /// Execute vector-guided walk.
    ///
    /// At each step, expand all neighbors of the current beam, score by cosine
    /// similarity, and keep the top `beam_width` for the next step. Returns the
    /// walk path: all visited nodes with their cumulative scores.
    pub fn execute(&self, memgraph: &MemGraph, lsn: u64) -> Result<Vec<HybridResult>, HybridError> {
        if self.query_vector.is_empty() {
            return Err(HybridError::EmptyQueryVector);
        }

        if memgraph.get_node(self.seed_node).is_none() {
            return Err(HybridError::NodeNotFound);
        }

        let mut visited: HashSet<NodeKey> = HashSet::new();
        visited.insert(self.seed_node);

        // Score the seed node.
        let seed_score = memgraph
            .get_node(self.seed_node)
            .and_then(|n| n.embedding.as_ref())
            .map(|emb| simd::cosine_similarity(emb, &self.query_vector))
            .unwrap_or(0.0);

        let mut results: Vec<HybridResult> = Vec::new();
        results.push(HybridResult {
            node: self.seed_node,
            score: seed_score,
            graph_distance: Some(0),
            context: Vec::new(),
        });

        // Current beam: (node_key, score).
        let mut beam: Vec<(NodeKey, f64)> = vec![(self.seed_node, seed_score)];

        for depth in 1..=self.max_depth {
            let mut candidates: Vec<(NodeKey, f64)> = Vec::new();

            for &(current, _) in &beam {
                // Expand neighbors.
                for (edge_key, neighbor_key) in memgraph.neighbors(current, Direction::Both, lsn) {
                    if visited.contains(&neighbor_key) {
                        continue;
                    }

                    let sim = memgraph
                        .get_node(neighbor_key)
                        .and_then(|n| n.embedding.as_ref())
                        .map(|emb| simd::cosine_similarity(emb, &self.query_vector))
                        .unwrap_or(0.0);

                    // Use edge_key to avoid unused variable warning.
                    let _ = edge_key;

                    candidates.push((neighbor_key, sim));
                }
            }

            if candidates.is_empty() {
                break; // No more unvisited neighbors.
            }

            // Sort by similarity descending, take top beam_width.
            candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            candidates.truncate(self.beam_width);

            // Check minimum similarity threshold.
            let best_sim = candidates.first().map(|c| c.1).unwrap_or(0.0);
            if best_sim < self.min_similarity {
                break; // Best candidate below threshold.
            }

            // Add to results and prepare next beam.
            beam.clear();
            for (node_key, sim) in &candidates {
                if visited.insert(*node_key) {
                    results.push(HybridResult {
                        node: *node_key,
                        score: *sim,
                        graph_distance: Some(depth),
                        context: Vec::new(),
                    });
                    beam.push((*node_key, *sim));
                }
            }

            if beam.is_empty() {
                break;
            }
        }

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// BFS collect helper (shared by HYB-01)
// ---------------------------------------------------------------------------

/// BFS from a start node, collecting (NodeKey, graph_distance) pairs.
/// Excludes the start node itself from results.
fn bfs_collect(
    memgraph: &MemGraph,
    start: NodeKey,
    max_depth: u32,
    edge_type_filter: Option<u16>,
    frontier_cap: usize,
    lsn: u64,
) -> Result<Vec<(NodeKey, u32)>, HybridError> {
    let mut visited: HashSet<NodeKey> = HashSet::new();
    visited.insert(start);

    let mut results: Vec<(NodeKey, u32)> = Vec::new();
    let mut frontier: VecDeque<(NodeKey, u32)> = VecDeque::new();
    frontier.push_back((start, 0));

    while let Some((current, depth)) = frontier.pop_front() {
        if depth >= max_depth {
            continue;
        }

        for (edge_key, neighbor_key) in memgraph.neighbors(current, Direction::Both, lsn) {
            // Apply edge type filter.
            if let Some(filter_type) = edge_type_filter {
                if let Some(edge) = memgraph.get_edge(edge_key) {
                    if edge.edge_type != filter_type {
                        continue;
                    }
                }
            }

            if visited.contains(&neighbor_key) {
                continue;
            }

            if visited.len() >= frontier_cap {
                return Err(HybridError::FrontierCapExceeded { cap: frontier_cap });
            }

            visited.insert(neighbor_key);
            let next_depth = depth + 1;
            results.push((neighbor_key, next_depth));
            frontier.push_back((neighbor_key, next_depth));
        }
    }

    Ok(results)
}

/// Collect context neighbors for a node via BFS expansion.
fn collect_context(
    memgraph: &MemGraph,
    start: NodeKey,
    max_hops: u32,
    edge_type_filter: Option<u16>,
    lsn: u64,
) -> Vec<ContextNode> {
    let mut visited: HashSet<NodeKey> = HashSet::new();
    visited.insert(start);

    let mut context: Vec<ContextNode> = Vec::new();
    let mut frontier: VecDeque<(NodeKey, u32)> = VecDeque::new();
    frontier.push_back((start, 0));

    while let Some((current, depth)) = frontier.pop_front() {
        if depth >= max_hops {
            continue;
        }

        for (edge_key, neighbor_key) in memgraph.neighbors(current, Direction::Both, lsn) {
            let edge_type = memgraph
                .get_edge(edge_key)
                .map(|e| e.edge_type)
                .unwrap_or(0);

            // Apply edge type filter.
            if let Some(filter_type) = edge_type_filter {
                if edge_type != filter_type {
                    continue;
                }
            }

            if visited.contains(&neighbor_key) {
                continue;
            }

            visited.insert(neighbor_key);
            let next_depth = depth + 1;
            context.push(ContextNode {
                node: neighbor_key,
                edge_type,
                hops: next_depth,
            });
            frontier.push_back((neighbor_key, next_depth));
        }
    }

    context
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::PropertyMap;
    use smallvec::smallvec;

    fn empty_props() -> PropertyMap {
        smallvec::SmallVec::new()
    }

    /// Build a test graph: A -> B -> C -> D, each with embeddings.
    fn build_test_graph() -> (MemGraph, NodeKey, NodeKey, NodeKey, NodeKey) {
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), Some(vec![1.0, 0.0, 0.0]), 1);
        let b = g.add_node(smallvec![0], empty_props(), Some(vec![0.8, 0.6, 0.0]), 1);
        let c = g.add_node(smallvec![0], empty_props(), Some(vec![0.0, 1.0, 0.0]), 1);
        let d = g.add_node(smallvec![0], empty_props(), Some(vec![0.0, 0.0, 1.0]), 1);

        g.add_edge(a, b, 1, 1.0, None, 2).expect("edge a->b");
        g.add_edge(b, c, 1, 1.0, None, 2).expect("edge b->c");
        g.add_edge(c, d, 1, 1.0, None, 2).expect("edge c->d");

        (g, a, b, c, d)
    }

    /// Build a star graph: center -> [s1, s2, ..., sn], each with embeddings.
    fn build_star_graph(n: usize) -> (MemGraph, NodeKey, Vec<NodeKey>) {
        let mut g = MemGraph::new(100_000);
        let center = g.add_node(smallvec![0], empty_props(), Some(vec![1.0, 0.0, 0.0]), 1);

        let mut spokes = Vec::with_capacity(n);
        for i in 0..n {
            let angle = (i as f32) * std::f32::consts::PI * 2.0 / (n as f32);
            let emb = vec![angle.cos(), angle.sin(), 0.0];
            let s = g.add_node(smallvec![0], empty_props(), Some(emb), 1);
            g.add_edge(center, s, 1, 1.0, None, 2).expect("edge");
            spokes.push(s);
        }

        (g, center, spokes)
    }

    // --- Cosine similarity tests ---

    #[test]
    fn test_cosine_identical() {
        let a = [1.0f32, 2.0, 3.0];
        let b = [1.0f32, 2.0, 3.0];
        let sim = simd::cosine_similarity(&a, &b);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = [1.0f32, 0.0, 0.0];
        let b = [0.0f32, 1.0, 0.0];
        let sim = simd::cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_opposite() {
        let a = [1.0f32, 0.0];
        let b = [-1.0f32, 0.0];
        let sim = simd::cosine_similarity(&a, &b);
        assert!((sim + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_empty() {
        let sim = simd::cosine_similarity(&[], &[]);
        assert!(sim.abs() < f64::EPSILON);
    }

    #[test]
    fn test_cosine_mismatched_dims() {
        let a = [1.0f32, 0.0];
        let b = [1.0f32, 0.0, 0.0];
        let sim = simd::cosine_similarity(&a, &b);
        assert!(sim.abs() < f64::EPSILON);
    }

    // --- Strategy selector tests ---

    #[test]
    fn test_strategy_brute_force_below_threshold() {
        assert_eq!(
            select_strategy(5_000, DEFAULT_STRATEGY_THRESHOLD),
            FilterStrategy::BruteForce
        );
    }

    #[test]
    fn test_strategy_hnsw_at_threshold() {
        assert_eq!(
            select_strategy(10_000, DEFAULT_STRATEGY_THRESHOLD),
            FilterStrategy::HnswPreFilter
        );
    }

    #[test]
    fn test_strategy_hnsw_above_threshold() {
        assert_eq!(
            select_strategy(50_000, DEFAULT_STRATEGY_THRESHOLD),
            FilterStrategy::HnswPreFilter
        );
    }

    #[test]
    fn test_strategy_custom_threshold() {
        assert_eq!(select_strategy(500, 1_000), FilterStrategy::BruteForce);
        assert_eq!(select_strategy(1_000, 1_000), FilterStrategy::HnswPreFilter);
    }

    // --- HYB-01: Graph-filtered vector search ---

    #[test]
    fn test_graph_filtered_basic() {
        let (g, a, b, _c, _d) = build_test_graph();

        // Search within 1 hop of A, query vector close to B's embedding.
        let search = GraphFilteredSearch::new(a, 1, vec![0.8, 0.6, 0.0], 10);
        let results = search.execute(&g, u64::MAX - 1).expect("search ok");

        // Only B is within 1 hop, and B has embedding [0.8, 0.6, 0.0].
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node, b);
        assert!((results[0].score - 1.0).abs() < 1e-6); // identical vector
        assert_eq!(results[0].graph_distance, Some(1));
    }

    #[test]
    fn test_graph_filtered_2_hops() {
        let (g, a, _b, c, _d) = build_test_graph();

        // Search within 2 hops of A, query vector closest to C.
        let search = GraphFilteredSearch::new(a, 2, vec![0.0, 1.0, 0.0], 10);
        let results = search.execute(&g, u64::MAX - 1).expect("search ok");

        // B and C are within 2 hops. C should rank first (identical to query).
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].node, c);
        assert!((results[0].score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_graph_filtered_k_limit() {
        let (g, center, _spokes) = build_star_graph(20);

        // All spokes are 1 hop from center. Take top 3.
        let search = GraphFilteredSearch::new(center, 1, vec![1.0, 0.0, 0.0], 3);
        let results = search.execute(&g, u64::MAX - 1).expect("search ok");

        assert_eq!(results.len(), 3);
        // Scores should be descending.
        for w in results.windows(2) {
            assert!(w[0].score >= w[1].score);
        }
    }

    #[test]
    fn test_graph_filtered_empty_query() {
        let (g, a, _, _, _) = build_test_graph();
        let search = GraphFilteredSearch::new(a, 1, vec![], 10);
        let result = search.execute(&g, u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::EmptyQueryVector)));
    }

    #[test]
    fn test_graph_filtered_node_not_found() {
        let g = MemGraph::new(100_000);
        let fake_key: NodeKey = slotmap::KeyData::from_ffi(999).into();
        let search = GraphFilteredSearch::new(fake_key, 1, vec![1.0, 0.0], 10);
        let result = search.execute(&g, u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::NodeNotFound)));
    }

    #[test]
    fn test_graph_filtered_no_embeddings() {
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        g.add_edge(a, b, 1, 1.0, None, 2).expect("edge");

        let search = GraphFilteredSearch::new(a, 1, vec![1.0, 0.0], 10);
        let results = search.execute(&g, u64::MAX - 1).expect("ok");
        assert!(results.is_empty()); // No embeddings -> no results.
    }

    #[test]
    fn test_graph_filtered_frontier_cap() {
        let (g, center, _) = build_star_graph(20);
        let mut search = GraphFilteredSearch::new(center, 1, vec![1.0, 0.0, 0.0], 10);
        search.frontier_cap = 5; // Very small cap.
        let result = search.execute(&g, u64::MAX - 1);
        assert!(matches!(
            result,
            Err(HybridError::FrontierCapExceeded { .. })
        ));
    }

    // --- HYB-02: Vector-to-graph expansion ---

    #[test]
    fn test_vector_expansion_basic() {
        let (g, a, b, c, d) = build_test_graph();
        let all_nodes = vec![a, b, c, d];

        // Query closest to C. Expand 1 hop.
        let expansion = VectorToGraphExpansion::new(vec![0.0, 1.0, 0.0], 1, 1);
        let results = expansion.execute(&g, &all_nodes, u64::MAX - 1).expect("ok");

        assert_eq!(results.len(), 1); // Top-1
        assert_eq!(results[0].node, c);
        assert!((results[0].score - 1.0).abs() < 1e-6);
        // C has neighbors B and D (1 hop).
        assert!(!results[0].context.is_empty());
    }

    #[test]
    fn test_vector_expansion_top_k() {
        let (g, a, b, c, d) = build_test_graph();
        let all_nodes = vec![a, b, c, d];

        let expansion = VectorToGraphExpansion::new(vec![0.0, 1.0, 0.0], 3, 1);
        let results = expansion.execute(&g, &all_nodes, u64::MAX - 1).expect("ok");

        assert_eq!(results.len(), 3);
        // Scores descending.
        for w in results.windows(2) {
            assert!(w[0].score >= w[1].score);
        }
    }

    #[test]
    fn test_vector_expansion_no_hops() {
        let (g, a, b, c, d) = build_test_graph();
        let all_nodes = vec![a, b, c, d];

        let expansion = VectorToGraphExpansion::new(vec![1.0, 0.0, 0.0], 2, 0);
        let results = expansion.execute(&g, &all_nodes, u64::MAX - 1).expect("ok");

        // No expansion: context should be empty.
        for r in &results {
            assert!(r.context.is_empty());
        }
    }

    #[test]
    fn test_vector_expansion_empty_query() {
        let g = MemGraph::new(100_000);
        let expansion = VectorToGraphExpansion::new(vec![], 1, 1);
        let result = expansion.execute(&g, &[], u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::EmptyQueryVector)));
    }

    // --- HYB-03: Vector-guided walk ---

    #[test]
    fn test_vector_walk_basic() {
        let (g, a, _b, _c, _d) = build_test_graph();

        // Walk from A toward [0.0, 1.0, 0.0] (closest to C).
        let walk = VectorGuidedWalk::new(a, vec![0.0, 1.0, 0.0], 3);
        let results = walk.execute(&g, u64::MAX - 1).expect("walk ok");

        // Should visit A, then expand toward B/C/D based on similarity.
        assert!(!results.is_empty());
        assert_eq!(results[0].node, a); // Seed node first.
        assert_eq!(results[0].graph_distance, Some(0));

        // Verify monotonic graph distance.
        let mut last_depth = 0;
        for r in &results {
            let d = r.graph_distance.unwrap_or(0);
            assert!(d >= last_depth || d == 0);
            last_depth = d;
        }
    }

    #[test]
    fn test_vector_walk_min_similarity() {
        let (g, a, _, _, _) = build_test_graph();

        // High min_similarity: should stop early.
        let mut walk = VectorGuidedWalk::new(a, vec![0.0, 0.0, 1.0], 10);
        walk.min_similarity = 0.99; // Very high -- only near-identical passes.
        let results = walk.execute(&g, u64::MAX - 1).expect("ok");

        // Only seed node (nothing else is similar enough at 0.99).
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_vector_walk_beam_width() {
        let (g, center, _spokes) = build_star_graph(10);

        let mut walk = VectorGuidedWalk::new(center, vec![1.0, 0.0, 0.0], 1);
        walk.beam_width = 3;
        let results = walk.execute(&g, u64::MAX - 1).expect("ok");

        // Seed + up to 3 (beam_width) spokes.
        assert!(results.len() <= 4);
        assert!(results.len() >= 2); // At least seed + 1 neighbor.
    }

    #[test]
    fn test_vector_walk_node_not_found() {
        let g = MemGraph::new(100_000);
        let fake_key: NodeKey = slotmap::KeyData::from_ffi(999).into();
        let walk = VectorGuidedWalk::new(fake_key, vec![1.0, 0.0], 3);
        let result = walk.execute(&g, u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::NodeNotFound)));
    }

    #[test]
    fn test_vector_walk_empty_query() {
        let (g, a, _, _, _) = build_test_graph();
        let walk = VectorGuidedWalk::new(a, vec![], 3);
        let result = walk.execute(&g, u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::EmptyQueryVector)));
    }

    #[test]
    fn test_vector_walk_no_neighbors() {
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), Some(vec![1.0, 0.0]), 1);
        // Isolated node.
        let walk = VectorGuidedWalk::new(a, vec![1.0, 0.0], 3);
        let results = walk.execute(&g, u64::MAX - 1).expect("ok");
        assert_eq!(results.len(), 1); // Only seed.
    }

    // --- BFS collect helper tests ---

    #[test]
    fn test_bfs_collect_basic() {
        let (g, a, b, c, _d) = build_test_graph();
        let candidates = bfs_collect(&g, a, 2, None, 100_000, u64::MAX - 1).expect("ok");

        // 2 hops from A: B (1 hop), C (2 hops).
        assert_eq!(candidates.len(), 2);
        let keys: HashSet<NodeKey> = candidates.iter().map(|(k, _)| *k).collect();
        assert!(keys.contains(&b));
        assert!(keys.contains(&c));
    }

    #[test]
    fn test_bfs_collect_with_edge_filter() {
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        let c = g.add_node(smallvec![0], empty_props(), None, 1);

        g.add_edge(a, b, 1, 1.0, None, 2).expect("edge"); // type 1
        g.add_edge(a, c, 2, 1.0, None, 2).expect("edge"); // type 2

        // Filter to edge type 1 only.
        let candidates = bfs_collect(&g, a, 1, Some(1), 100_000, u64::MAX - 1).expect("ok");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, b);
    }

    // --- Integration tests: combined patterns ---

    #[test]
    fn test_graph_filtered_scores_only_reachable() {
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), Some(vec![1.0, 0.0]), 1);
        let b = g.add_node(smallvec![0], empty_props(), Some(vec![0.9, 0.1]), 1);
        // Disconnected node with perfect similarity.
        let _c = g.add_node(smallvec![0], empty_props(), Some(vec![1.0, 0.0]), 1);
        g.add_edge(a, b, 1, 1.0, None, 2).expect("edge");

        // C is disconnected from A. Only B should appear.
        let search = GraphFilteredSearch::new(a, 1, vec![1.0, 0.0], 10);
        let results = search.execute(&g, u64::MAX - 1).expect("ok");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node, b);
    }

    #[test]
    fn test_expansion_context_has_correct_hops() {
        let (g, a, b, c, d) = build_test_graph();
        let all_nodes = vec![a, b, c, d];

        // Find C, expand 2 hops.
        let expansion = VectorToGraphExpansion::new(vec![0.0, 1.0, 0.0], 1, 2);
        let results = expansion.execute(&g, &all_nodes, u64::MAX - 1).expect("ok");

        // C is result[0]. Context should include B (1 hop) and D (1 hop),
        // and A (2 hops from C via B).
        let context = &results[0].context;
        assert!(!context.is_empty());

        // Verify hops are valid.
        for cn in context {
            assert!(cn.hops >= 1 && cn.hops <= 2);
        }
    }
}
