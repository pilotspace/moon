//! HYB-01 graph-filtered vector search + HYB-02 vector-to-graph
//! expansion (split from hybrid.rs per the 1500-line module rule).

use super::*;

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
    pub fn execute(
        &self,
        memgraph: &MemGraph,
        csr_segs: &[Arc<CsrStorage>],
        lsn: u64,
    ) -> Result<Vec<HybridResult>, HybridError> {
        if self.query_vector.is_empty() {
            return Err(HybridError::EmptyQueryVector);
        }

        let view = MergedNodeView::new(memgraph, csr_segs);

        // Verify start node exists in EITHER tier.
        if !view.contains(self.start_node) {
            return Err(HybridError::NodeNotFound);
        }

        // Step 1: BFS to collect candidates with graph distance.
        let candidates = bfs_collect(
            memgraph,
            csr_segs,
            self.start_node,
            self.hops,
            self.edge_type_filter,
            self.frontier_cap,
            lsn,
        )?;

        // Step 2: Select strategy.
        let strategy = select_strategy(candidates.len(), self.threshold);

        // Step 3: Score candidates. HnswPreFilter routes CSR-resident
        // candidates through each segment's HNSW bridge and leaves the
        // rest (mutable tier, bridge-less rows, dim mismatch) in
        // `residual` for exact scoring below.
        let mut scored: Vec<HybridResult> = Vec::with_capacity(candidates.len().min(4096));
        let residual: Vec<(NodeKey, u32)> = match strategy {
            FilterStrategy::BruteForce => candidates,
            FilterStrategy::HnswPreFilter => hnsw_prefilter_score(
                memgraph,
                csr_segs,
                candidates,
                &self.query_vector,
                self.k,
                &mut scored,
            ),
        };

        // Exact scoring for whatever the pre-filter did not cover
        // (everything, under BruteForce): embedding resolved from the
        // mutable tier or the CSR v5 blob.
        for (node_key, graph_dist) in &residual {
            let Some(embedding) = view.embedding(*node_key) else {
                continue; // Skip nodes without embeddings.
            };

            let sim = simd::cosine_similarity(&embedding, &self.query_vector);
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

/// Route candidates through per-segment HNSW bridges (HnswPreFilter).
///
/// Partitions `candidates` into per-segment groups (a candidate joins a
/// group when its segment has a bridge covering its row at the query's
/// dimension) and searches each bridge with an allow-filter over the
/// group's rows. Bridge hits are pushed to `scored` with their EXACT
/// cosine (bridge vectors are unit-normalized copies of the originals).
/// Returns the residual candidates for exact scoring: mutable-tier nodes,
/// bridge-less rows, dim mismatches, and any whole group whose beam
/// under-filled k (approximate-search rescue — never silently truncate).
pub(super) fn hnsw_prefilter_score(
    memgraph: &MemGraph,
    csr_segs: &[Arc<CsrStorage>],
    candidates: Vec<(NodeKey, u32)>,
    query: &[f32],
    k: usize,
    scored: &mut Vec<HybridResult>,
) -> Vec<(NodeKey, u32)> {
    use crate::graph::fasthash::FxHashMap;

    let mut residual: Vec<(NodeKey, u32)> = Vec::new();
    let mut groups: Vec<FxHashMap<u32, (NodeKey, u32)>> =
        (0..csr_segs.len()).map(|_| FxHashMap::default()).collect();

    'cand: for (key, gdist) in candidates {
        // Mutable tier wins (same precedence as MergedNodeView::embedding).
        if memgraph.get_node(key).is_some() {
            residual.push((key, gdist));
            continue;
        }
        for (i, seg) in csr_segs.iter().enumerate() {
            if let Some(row) = seg.lookup_node(key) {
                if let Some(bridge) = seg.hnsw_bridge() {
                    if bridge.dim() == query.len() && bridge.contains_row(row) {
                        groups[i].insert(row, (key, gdist));
                        continue 'cand;
                    }
                }
                // Resident here but not bridge-searchable: score exactly.
                residual.push((key, gdist));
                continue 'cand;
            }
        }
        residual.push((key, gdist));
    }

    for (i, group) in groups.iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let Some(bridge) = csr_segs[i].hnsw_bridge() else {
            // Unreachable by construction; stay exact if it ever isn't.
            residual.extend(group.values().copied());
            continue;
        };
        let hits = bridge.search(query, k, |row| group.contains_key(&row));
        if hits.len() < k.min(group.len()) {
            // Beam under-filled the ask: rescue with exact scoring.
            residual.extend(group.values().copied());
            continue;
        }
        for (row, sim) in hits {
            if let Some(&(key, gdist)) = group.get(&row) {
                scored.push(HybridResult {
                    node: key,
                    score: sim,
                    graph_distance: Some(gdist),
                    context: Vec::new(),
                });
            }
        }
    }

    residual
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
        csr_segs: &[Arc<CsrStorage>],
        candidate_nodes: &[NodeKey],
        lsn: u64,
    ) -> Result<Vec<HybridResult>, HybridError> {
        if self.query_vector.is_empty() {
            return Err(HybridError::EmptyQueryVector);
        }

        let view = MergedNodeView::new(memgraph, csr_segs);
        let committed = roaring::RoaringBitmap::new();

        // Step 1: Score all candidate nodes by cosine similarity.
        let mut scored: Vec<(NodeKey, f64)> = Vec::with_capacity(candidate_nodes.len());

        for &node_key in candidate_nodes {
            if !view.is_visible(node_key, 0, 0, &committed, None) {
                continue;
            }
            let Some(embedding) = view.embedding(node_key) else {
                continue;
            };

            let sim = simd::cosine_similarity(&embedding, &self.query_vector);
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
                    csr_segs,
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
