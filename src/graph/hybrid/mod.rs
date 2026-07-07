//! Hybrid graph+vector query engine.
//!
//! Four query patterns (HYB-01 through HYB-04):
//! 1. Graph-filtered vector search: traverse N hops -> collect candidates -> score
//! 2. Vector-to-graph expansion: score candidates -> expand N hops for context
//! 3. Vector-guided walk: beam search guided by embedding distance
//! 4. Automatic strategy selection based on candidate set size threshold
//!
//! All functions operate on BOTH graph tiers — the mutable MemGraph write
//! buffer and the immutable CSR segments (traversal via `SegmentMergeReader`,
//! node existence/embeddings via `MergedNodeView`) — without unsafe code or
//! unwrap. The shard command handler passes the write buffer plus a loaded
//! segment snapshot; both stores are per-shard, single-owner. Pass `&[]` for
//! segments to operate on a bare MemGraph (tests, pre-freeze graphs).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use super::simd;

use crate::graph::csr::CsrStorage;
use crate::graph::memgraph::MemGraph;
use crate::graph::traversal::SegmentMergeReader;
use crate::graph::types::{Direction, NodeKey};
use crate::graph::view::MergedNodeView;

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
    /// Route CSR-resident candidates through the per-segment HNSW bridge
    /// (`CsrStorage::hnsw_bridge`, built lazily over v5 embeddings) instead
    /// of scoring each one. Approximate: a bridge beam that under-fills k
    /// falls back to exact scoring of that group. Mutable-tier candidates
    /// and rows without a bridge entry are always scored exactly.
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

mod search;
mod walk_rerank;

pub use search::*;
pub use walk_rerank::*;

// ---------------------------------------------------------------------------
// BFS collect helper (shared by HYB-01, HYB-04)
// ---------------------------------------------------------------------------

/// BFS from a start node, collecting (NodeKey, graph_distance) pairs.
/// Excludes the start node itself from results. Traverses BOTH tiers via
/// `SegmentMergeReader` (frozen CSR edges + mutable/delta edges).
fn bfs_collect(
    memgraph: &MemGraph,
    csr_segs: &[Arc<CsrStorage>],
    start: NodeKey,
    max_depth: u32,
    edge_type_filter: Option<u16>,
    frontier_cap: usize,
    lsn: u64,
) -> Result<Vec<(NodeKey, u32)>, HybridError> {
    let reader = SegmentMergeReader::new(
        Some(memgraph),
        csr_segs,
        Direction::Both,
        lsn,
        edge_type_filter,
    );

    let mut visited: HashSet<NodeKey> = HashSet::new();
    visited.insert(start);

    let mut results: Vec<(NodeKey, u32)> = Vec::new();
    let mut frontier: VecDeque<(NodeKey, u32)> = VecDeque::new();
    frontier.push_back((start, 0));

    while let Some((current, depth)) = frontier.pop_front() {
        if depth >= max_depth {
            continue;
        }

        for merged in reader.neighbors(current) {
            let neighbor_key = merged.node;
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

/// Collect context neighbors for a node via BFS expansion (both tiers).
fn collect_context(
    memgraph: &MemGraph,
    csr_segs: &[Arc<CsrStorage>],
    start: NodeKey,
    max_hops: u32,
    edge_type_filter: Option<u16>,
    lsn: u64,
) -> Vec<ContextNode> {
    let reader = SegmentMergeReader::new(
        Some(memgraph),
        csr_segs,
        Direction::Both,
        lsn,
        edge_type_filter,
    );

    let mut visited: HashSet<NodeKey> = HashSet::new();
    visited.insert(start);

    let mut context: Vec<ContextNode> = Vec::new();
    let mut frontier: VecDeque<(NodeKey, u32)> = VecDeque::new();
    frontier.push_back((start, 0));

    while let Some((current, depth)) = frontier.pop_front() {
        if depth >= max_hops {
            continue;
        }

        for merged in reader.neighbors(current) {
            let neighbor_key = merged.node;
            if visited.contains(&neighbor_key) {
                continue;
            }

            visited.insert(neighbor_key);
            let next_depth = depth + 1;
            context.push(ContextNode {
                node: neighbor_key,
                edge_type: merged.edge_type,
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
        let results = search.execute(&g, &[], u64::MAX - 1).expect("search ok");

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
        let results = search.execute(&g, &[], u64::MAX - 1).expect("search ok");

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
        let results = search.execute(&g, &[], u64::MAX - 1).expect("search ok");

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
        let result = search.execute(&g, &[], u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::EmptyQueryVector)));
    }

    #[test]
    fn test_graph_filtered_node_not_found() {
        let g = MemGraph::new(100_000);
        let fake_key: NodeKey = slotmap::KeyData::from_ffi(999).into();
        let search = GraphFilteredSearch::new(fake_key, 1, vec![1.0, 0.0], 10);
        let result = search.execute(&g, &[], u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::NodeNotFound)));
    }

    #[test]
    fn test_graph_filtered_no_embeddings() {
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);
        let b = g.add_node(smallvec![0], empty_props(), None, 1);
        g.add_edge(a, b, 1, 1.0, None, 2).expect("edge");

        let search = GraphFilteredSearch::new(a, 1, vec![1.0, 0.0], 10);
        let results = search.execute(&g, &[], u64::MAX - 1).expect("ok");
        assert!(results.is_empty()); // No embeddings -> no results.
    }

    #[test]
    fn test_graph_filtered_frontier_cap() {
        let (g, center, _) = build_star_graph(20);
        let mut search = GraphFilteredSearch::new(center, 1, vec![1.0, 0.0, 0.0], 10);
        search.frontier_cap = 5; // Very small cap.
        let result = search.execute(&g, &[], u64::MAX - 1);
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
        let results = expansion
            .execute(&g, &[], &all_nodes, u64::MAX - 1)
            .expect("ok");

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
        let results = expansion
            .execute(&g, &[], &all_nodes, u64::MAX - 1)
            .expect("ok");

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
        let results = expansion
            .execute(&g, &[], &all_nodes, u64::MAX - 1)
            .expect("ok");

        // No expansion: context should be empty.
        for r in &results {
            assert!(r.context.is_empty());
        }
    }

    #[test]
    fn test_vector_expansion_empty_query() {
        let g = MemGraph::new(100_000);
        let expansion = VectorToGraphExpansion::new(vec![], 1, 1);
        let result = expansion.execute(&g, &[], &[], u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::EmptyQueryVector)));
    }

    // --- HYB-03: Vector-guided walk ---

    #[test]
    fn test_vector_walk_basic() {
        let (g, a, _b, _c, _d) = build_test_graph();

        // Walk from A toward [0.0, 1.0, 0.0] (closest to C).
        let walk = VectorGuidedWalk::new(a, vec![0.0, 1.0, 0.0], 3);
        let results = walk.execute(&g, &[], u64::MAX - 1).expect("walk ok");

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
        let results = walk.execute(&g, &[], u64::MAX - 1).expect("ok");

        // Only seed node (nothing else is similar enough at 0.99).
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_vector_walk_beam_width() {
        let (g, center, _spokes) = build_star_graph(10);

        let mut walk = VectorGuidedWalk::new(center, vec![1.0, 0.0, 0.0], 1);
        walk.beam_width = 3;
        let results = walk.execute(&g, &[], u64::MAX - 1).expect("ok");

        // Seed + up to 3 (beam_width) spokes.
        assert!(results.len() <= 4);
        assert!(results.len() >= 2); // At least seed + 1 neighbor.
    }

    #[test]
    fn test_vector_walk_node_not_found() {
        let g = MemGraph::new(100_000);
        let fake_key: NodeKey = slotmap::KeyData::from_ffi(999).into();
        let walk = VectorGuidedWalk::new(fake_key, vec![1.0, 0.0], 3);
        let result = walk.execute(&g, &[], u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::NodeNotFound)));
    }

    #[test]
    fn test_vector_walk_empty_query() {
        let (g, a, _, _, _) = build_test_graph();
        let walk = VectorGuidedWalk::new(a, vec![], 3);
        let result = walk.execute(&g, &[], u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::EmptyQueryVector)));
    }

    #[test]
    fn test_vector_walk_no_neighbors() {
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), Some(vec![1.0, 0.0]), 1);
        // Isolated node.
        let walk = VectorGuidedWalk::new(a, vec![1.0, 0.0], 3);
        let results = walk.execute(&g, &[], u64::MAX - 1).expect("ok");
        assert_eq!(results.len(), 1); // Only seed.
    }

    // --- BFS collect helper tests ---

    #[test]
    fn test_bfs_collect_basic() {
        let (g, a, b, c, _d) = build_test_graph();
        let candidates = bfs_collect(&g, &[], a, 2, None, 100_000, u64::MAX - 1).expect("ok");

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
        let candidates = bfs_collect(&g, &[], a, 1, Some(1), 100_000, u64::MAX - 1).expect("ok");
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
        let results = search.execute(&g, &[], u64::MAX - 1).expect("ok");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node, b);
    }

    #[test]
    fn test_expansion_context_has_correct_hops() {
        let (g, a, b, c, d) = build_test_graph();
        let all_nodes = vec![a, b, c, d];

        // Find C, expand 2 hops.
        let expansion = VectorToGraphExpansion::new(vec![0.0, 1.0, 0.0], 1, 2);
        let results = expansion
            .execute(&g, &[], &all_nodes, u64::MAX - 1)
            .expect("ok");

        // C is result[0]. Context should include B (1 hop) and D (1 hop),
        // and A (2 hops from C via B).
        let context = &results[0].context;
        assert!(!context.is_empty());

        // Verify hops are valid.
        for cn in context {
            assert!(cn.hops >= 1 && cn.hops <= 2);
        }
    }

    // --- HYB-04: Graph-constrained re-ranking ---

    /// Build a 5-node chain: A - B - C - D - E with distinct embeddings.
    fn build_rerank_chain() -> (MemGraph, NodeKey, NodeKey, NodeKey, NodeKey, NodeKey) {
        let mut g = MemGraph::new(100_000);
        // A: very similar to query [1,0,0]
        let a = g.add_node(smallvec![0], empty_props(), Some(vec![1.0, 0.0, 0.0]), 1);
        // B: moderately similar
        let b = g.add_node(smallvec![0], empty_props(), Some(vec![0.7, 0.7, 0.0]), 1);
        // C: less similar
        let c = g.add_node(smallvec![0], empty_props(), Some(vec![0.0, 1.0, 0.0]), 1);
        // D: even less similar
        let d = g.add_node(smallvec![0], empty_props(), Some(vec![0.0, 0.5, 0.87]), 1);
        // E: least similar (opposite direction)
        let e = g.add_node(smallvec![0], empty_props(), Some(vec![-1.0, 0.0, 0.0]), 1);

        g.add_edge(a, b, 1, 1.0, None, 2).expect("edge a->b");
        g.add_edge(b, c, 1, 1.0, None, 2).expect("edge b->c");
        g.add_edge(c, d, 1, 1.0, None, 2).expect("edge c->d");
        g.add_edge(d, e, 1, 1.0, None, 2).expect("edge d->e");

        (g, a, b, c, d, e)
    }

    #[test]
    fn test_rerank_alpha_one_pure_vector() {
        // alpha=1.0: graph distance is ignored, ranking == pure vector search.
        let (g, a, _b, _c, _d, _e) = build_rerank_chain();

        let reranker = GraphConstrainedReRanker::new(a, 5, 1.0, vec![1.0, 0.0, 0.0], 5);
        let results = reranker.execute(&g, &[], u64::MAX - 1).expect("ok");

        // A has embedding [1,0,0], query is [1,0,0] => score 1.0 (highest).
        // B has [0.7,0.7,0] => ~0.707
        // Ranking should be: A first (perfect match), then B, then rest.
        assert!(!results.is_empty());
        assert_eq!(results[0].node, a);
        assert!((results[0].score - 1.0).abs() < 1e-5);

        // Verify descending scores.
        for w in results.windows(2) {
            assert!(w[0].score >= w[1].score - 1e-9);
        }
    }

    #[test]
    fn test_rerank_alpha_zero_pure_graph() {
        // alpha=0.0: vector similarity is ignored, ranking == graph proximity.
        let (g, a, b, c, _d, _e) = build_rerank_chain();

        let reranker = GraphConstrainedReRanker::new(a, 5, 0.0, vec![1.0, 0.0, 0.0], 5);
        let results = reranker.execute(&g, &[], u64::MAX - 1).expect("ok");

        // Graph distances: A=0, B=1, C=2, D=3, E=4.
        // Graph score = 1/(1+d): A=1.0, B=0.5, C=0.333, D=0.25, E=0.2.
        // So A should rank first (distance 0), then B (1), C (2), D (3), E (4).
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].node, a);
        assert_eq!(results[0].graph_distance, Some(0));
        assert_eq!(results[1].node, b);
        assert_eq!(results[1].graph_distance, Some(1));
        assert_eq!(results[2].node, c);
        assert_eq!(results[2].graph_distance, Some(2));
    }

    #[test]
    fn test_rerank_alpha_mixed_reorders() {
        // alpha=0.7: vector-similar but far node (E with embedding flipped)
        // vs graph-close but less similar node.
        // Build: ref=C (middle). B is 1 hop, D is 1 hop.
        // Give B a vector close to query, D a vector far from query.
        // With alpha=0.7, B (close + similar) should beat A (far + similar).
        let (g, a, b, c, _d, _e) = build_rerank_chain();

        // Query vector [1,0,0]: A is most similar but 2 hops from C.
        // B is 1 hop from C and moderately similar.
        let reranker = GraphConstrainedReRanker::new(c, 2, 0.3, vec![1.0, 0.0, 0.0], 5);
        let results = reranker.execute(&g, &[], u64::MAX - 1).expect("ok");

        // A: vector_score ~1.0, graph_dist=2, graph_score=1/3=0.333
        //    combined = 0.3*1.0 + 0.7*0.333 = 0.300 + 0.233 = 0.533
        // B: vector_score ~0.707, graph_dist=1, graph_score=0.5
        //    combined = 0.3*0.707 + 0.7*0.5 = 0.212 + 0.350 = 0.562
        // C: vector_score ~0.0, graph_dist=0, graph_score=1.0
        //    combined = 0.3*0.0 + 0.7*1.0 = 0.700
        // With alpha=0.3 (heavy graph weight), B should rank above A
        // because B is closer in graph even though A is more similar.

        // Find positions of A and B.
        let pos_a = results.iter().position(|r| r.node == a);
        let pos_b = results.iter().position(|r| r.node == b);
        // B should be ranked higher (lower index) than A.
        if let (Some(pa), Some(pb)) = (pos_a, pos_b) {
            assert!(
                pb < pa,
                "B (1 hop, moderate similarity) should rank above A (2 hops, high similarity) with alpha=0.3"
            );
        }
    }

    #[test]
    fn test_rerank_unreachable_penalty() {
        // Nodes not reachable within max_hops get graph_distance = max_hops + 1.
        let (g, a, _b, _c, _d, e) = build_rerank_chain();

        // max_hops=2: A can reach B(1), C(2). D and E are unreachable.
        let reranker = GraphConstrainedReRanker::new(a, 2, 0.5, vec![1.0, 0.0, 0.0], 10);
        let results = reranker.execute(&g, &[], u64::MAX - 1).expect("ok");

        // E should be reachable result with graph_distance = max_hops+1 = 3.
        let e_result = results.iter().find(|r| r.node == e);
        assert!(e_result.is_some(), "E should appear in results");
        assert_eq!(
            e_result.map(|r| r.graph_distance),
            Some(Some(3)),
            "unreachable E should get penalty distance 3 (max_hops+1)"
        );
    }

    #[test]
    fn test_rerank_empty_candidates() {
        // Graph with no embeddings: should return empty results.
        let mut g = MemGraph::new(100_000);
        let a = g.add_node(smallvec![0], empty_props(), None, 1);

        let reranker = GraphConstrainedReRanker::new(a, 3, 0.5, vec![1.0, 0.0, 0.0], 10);
        let results = reranker.execute(&g, &[], u64::MAX - 1).expect("ok");
        assert!(results.is_empty());
    }

    #[test]
    fn test_rerank_node_not_found() {
        let g = MemGraph::new(100_000);
        let fake_key: NodeKey = slotmap::KeyData::from_ffi(999).into();
        let reranker = GraphConstrainedReRanker::new(fake_key, 3, 0.5, vec![1.0, 0.0, 0.0], 10);
        let result = reranker.execute(&g, &[], u64::MAX - 1);
        assert!(matches!(result, Err(HybridError::NodeNotFound)));
    }

    // --- HnswPreFilter via the per-segment bridge ---

    /// Deterministic pseudo-random embedding (LCG — no RNG in tests).
    fn det_embedding(i: u64, dim: usize) -> Vec<f32> {
        let mut state = i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((state >> 33) as f32 / (1u64 << 31) as f32) - 0.5
            })
            .collect()
    }

    /// Frozen star (center -> 299 embedded spokes) as a CSR segment.
    fn frozen_star_segment() -> (MemGraph, Arc<CsrStorage>, NodeKey) {
        let mut g = MemGraph::new(1_000_000);
        let center = g.add_node(smallvec![0], empty_props(), Some(det_embedding(0, 8)), 1);
        for i in 1..300u64 {
            let s = g.add_node(smallvec![0], empty_props(), Some(det_embedding(i, 8)), 1);
            g.add_edge(center, s, 1, 1.0, None, 2).expect("edge");
        }
        let frozen = g.freeze().expect("freeze");
        let seg = crate::graph::csr::CsrSegment::from_frozen(frozen, 10).expect("csr");
        (
            MemGraph::new(1_000_000),
            Arc::new(CsrStorage::from(seg)),
            center,
        )
    }

    #[test]
    fn test_hnsw_prefilter_matches_brute_force() {
        let (mg, seg, center) = frozen_star_segment();
        // Pre-build the bridge below the production minimum so the
        // HnswPreFilter path actually engages on a 300-node fixture.
        assert!(seg.hnsw_bridge_for_test().is_some(), "bridge must build");

        let query = det_embedding(9999, 8);
        let mut hnsw = GraphFilteredSearch::new(center, 1, query.clone(), 5);
        hnsw.threshold = 1; // force HnswPreFilter
        let mut brute = GraphFilteredSearch::new(center, 1, query.clone(), 5);
        brute.threshold = usize::MAX; // force BruteForce

        let segs = vec![seg.clone()];
        let hnsw_res = hnsw.execute(&mg, &segs, u64::MAX - 1).expect("hnsw ok");
        let brute_res = brute.execute(&mg, &segs, u64::MAX - 1).expect("brute ok");

        assert_eq!(hnsw_res.len(), 5);
        assert_eq!(brute_res.len(), 5);
        // Every HNSW score must be the node's REAL cosine similarity and
        // carry the BFS graph distance.
        let view = MergedNodeView::new(&mg, &segs);
        for r in &hnsw_res {
            let emb = view.embedding(r.node).expect("embedding");
            let exact = simd::cosine_similarity(&emb, &query);
            assert!(
                (r.score - exact).abs() < 1e-4,
                "score {} vs exact {exact}",
                r.score
            );
            assert_eq!(r.graph_distance, Some(1));
        }
        // Top-5 overlap with the exact ranking (approximate search).
        let brute_set: HashSet<NodeKey> = brute_res.iter().map(|r| r.node).collect();
        let overlap = hnsw_res
            .iter()
            .filter(|r| brute_set.contains(&r.node))
            .count();
        assert!(
            overlap >= 4,
            "HNSW/brute top-5 overlap too low: {overlap}/5"
        );
    }

    #[test]
    fn test_hnsw_prefilter_without_bridge_is_exact() {
        // No test bridge pre-built: the production accessor refuses a
        // 300-vector segment (min 4096), so every candidate is residual
        // and results must EXACTLY equal brute force.
        let (mg, seg, center) = frozen_star_segment();
        let query = det_embedding(777, 8);

        let mut hnsw = GraphFilteredSearch::new(center, 1, query.clone(), 5);
        hnsw.threshold = 1;
        let mut brute = GraphFilteredSearch::new(center, 1, query, 5);
        brute.threshold = usize::MAX;

        let segs = vec![seg.clone()];
        let hnsw_res = hnsw.execute(&mg, &segs, u64::MAX - 1).expect("ok");
        let brute_res = brute.execute(&mg, &segs, u64::MAX - 1).expect("ok");

        let a: Vec<(NodeKey, u64)> = hnsw_res
            .iter()
            .map(|r| (r.node, r.score.to_bits()))
            .collect();
        let b: Vec<(NodeKey, u64)> = brute_res
            .iter()
            .map(|r| (r.node, r.score.to_bits()))
            .collect();
        assert_eq!(
            a, b,
            "bridge-less HnswPreFilter must be bit-exact brute force"
        );
    }

    #[test]
    fn test_hnsw_prefilter_partition_routes_tiers() {
        // Partition contract of hnsw_prefilter_score: mutable-tier
        // candidates land in `residual` (exact scoring), bridge-covered
        // frozen candidates surface through `scored` as the group top-k.
        let (mut mg, seg, _center) = frozen_star_segment();
        assert!(seg.hnsw_bridge_for_test().is_some(), "bridge must build");

        // A fresh MemGraph's first key ALIASES a frozen key (same slotmap
        // sequence) -- exactly the mutable-tier-wins case the partition
        // must route to residual.
        let hot = mg.add_node(
            smallvec![0],
            empty_props(),
            Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            20,
        );

        // Candidates: the mutable node + every OTHER frozen row.
        let mut candidates: Vec<(NodeKey, u32)> = vec![(hot, 1)];
        for meta in seg.node_meta() {
            let key: NodeKey = slotmap::KeyData::from_ffi(meta.external_id).into();
            if key != hot {
                candidates.push((key, 1));
            }
        }
        let total = candidates.len();

        let query = det_embedding(55, 8);
        let segs = vec![seg.clone()];
        let mut scored: Vec<HybridResult> = Vec::new();
        let residual = hnsw_prefilter_score(&mg, &segs, candidates, &query, 5, &mut scored);

        // Mutable node: residual, never bridge-scored.
        assert!(residual.iter().any(|&(k, _)| k == hot));
        assert!(scored.iter().all(|r| r.node != hot));
        assert_eq!(
            residual.len(),
            1,
            "all frozen candidates are bridge-covered"
        );
        // Bridge returned the group's top-k with exact cosines.
        assert_eq!(scored.len(), 5);
        let view = MergedNodeView::new(&mg, &segs);
        for r in &scored {
            let emb = view.embedding(r.node).expect("embedding");
            let exact = simd::cosine_similarity(&emb, &query);
            assert!((r.score - exact).abs() < 1e-4);
            assert_eq!(r.graph_distance, Some(1));
        }
        assert!(
            total > scored.len() + residual.len(),
            "prefilter must prune"
        );
    }
}
