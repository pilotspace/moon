//! HYB-03 vector-guided walk + HYB-04 graph-constrained re-ranking
//! (split from hybrid.rs per the 1500-line module rule).

use super::*;

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
        let reader = SegmentMergeReader::new(Some(memgraph), csr_segs, Direction::Both, lsn, None);

        if !view.contains(self.seed_node) {
            return Err(HybridError::NodeNotFound);
        }

        let mut visited: HashSet<NodeKey> = HashSet::new();
        visited.insert(self.seed_node);

        // Score the seed node.
        let seed_score = view
            .embedding(self.seed_node)
            .map(|emb| simd::cosine_similarity(&emb, &self.query_vector))
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
                // Expand neighbors across both tiers.
                for merged in reader.neighbors(current) {
                    let neighbor_key = merged.node;
                    if visited.contains(&neighbor_key) {
                        continue;
                    }

                    let sim = view
                        .embedding(neighbor_key)
                        .map(|emb| simd::cosine_similarity(&emb, &self.query_vector))
                        .unwrap_or(0.0);

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
// HYB-04: Graph-constrained re-ranking
// ---------------------------------------------------------------------------

/// Configuration for graph-constrained re-ranking.
///
/// Re-ranks ALL nodes with embeddings by a combined score:
///   `alpha * vector_score + (1 - alpha) * 1 / (1 + graph_distance)`
///
/// Graph distances are computed via a single batch BFS from the reference node,
/// giving O(frontier) cost instead of O(k * frontier) for per-candidate BFS.
/// Nodes not reachable within `max_hops` receive a penalty distance of
/// `max_hops + 1`.
pub struct GraphConstrainedReRanker {
    /// Reference node for graph distance computation.
    pub reference_node: NodeKey,
    /// Maximum BFS depth for distance computation.
    pub max_hops: u32,
    /// Weight for vector similarity score (0.0 = graph only, 1.0 = vector only).
    pub alpha: f64,
    /// Number of top results to return.
    pub k: usize,
    /// Query vector for cosine similarity scoring.
    pub query_vector: Vec<f32>,
    /// Maximum frontier size for BFS to prevent OOM.
    pub frontier_cap: usize,
    /// Strategy selection threshold (default 10K): at or above this many
    /// total nodes, frozen segments answer through their HNSW bridge (W2-5)
    /// instead of a full scan.
    pub threshold: usize,
}

impl GraphConstrainedReRanker {
    /// Create with defaults (frontier_cap=100K).
    pub fn new(
        reference_node: NodeKey,
        max_hops: u32,
        alpha: f64,
        query_vector: Vec<f32>,
        k: usize,
    ) -> Self {
        Self {
            reference_node,
            max_hops,
            alpha,
            k,
            query_vector,
            frontier_cap: 100_000,
            threshold: DEFAULT_STRATEGY_THRESHOLD,
        }
    }

    /// Execute graph-constrained re-ranking.
    ///
    /// 1. Validate inputs (reference node exists, non-empty vector, alpha in range).
    /// 2. Single batch BFS from reference node to compute graph distances.
    /// 3. Iterate ALL nodes with embeddings, compute cosine similarity.
    /// 4. Combine: `alpha * vector_score + (1-alpha) * 1/(1+graph_dist)`.
    /// 5. Sort descending, return top-K.
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

        // Validate reference node exists in EITHER tier.
        if !view.contains(self.reference_node) {
            return Err(HybridError::NodeNotFound);
        }

        // Clamp alpha to [0.0, 1.0].
        let alpha = self.alpha.clamp(0.0, 1.0);
        let penalty_dist = self.max_hops + 1;

        // Step 1: Single batch BFS from reference node — O(frontier).
        let bfs_results = bfs_collect(
            memgraph,
            csr_segs,
            self.reference_node,
            self.max_hops,
            None,
            self.frontier_cap,
            lsn,
        )?;

        // Build O(1) distance lookup map.
        let mut distance_map: HashMap<NodeKey, u32> = HashMap::with_capacity(bfs_results.len() + 1);
        distance_map.insert(self.reference_node, 0);
        for (node_key, dist) in &bfs_results {
            distance_map.insert(*node_key, *dist);
        }

        // Step 2: score nodes with embeddings across both tiers. Every node
        // OUTSIDE the BFS frontier shares the same graph term (the penalty
        // distance), so within that class the combined ranking is monotone
        // in cosine — which lets a large frozen segment answer with its
        // HNSW bridge's top-k instead of a full scan (W2-5). Frontier nodes
        // (bounded by frontier_cap) and the mutable tier (bounded by
        // edge_threshold) are always scored exactly.
        let committed = roaring::RoaringBitmap::new();
        let mut scored: Vec<HybridResult> = Vec::with_capacity(distance_map.len());

        let total_nodes: usize = memgraph.node_count()
            + csr_segs
                .iter()
                .map(|s| s.node_count() as usize)
                .sum::<usize>();
        let use_bridge = matches!(
            select_strategy(total_nodes, self.threshold),
            FilterStrategy::HnswPreFilter
        );

        if !use_bridge {
            view.for_each_visible_node(None, 0, 0, &committed, None, |node_key| {
                let Some(embedding) = view.embedding(node_key) else {
                    return; // Skip nodes without embeddings.
                };

                let vector_score = simd::cosine_similarity(&embedding, &self.query_vector);
                let graph_dist = distance_map.get(&node_key).copied().unwrap_or(penalty_dist);
                let graph_score = 1.0 / (1.0 + graph_dist as f64);
                let combined = alpha * vector_score + (1.0 - alpha) * graph_score;

                scored.push(HybridResult {
                    node: node_key,
                    score: combined,
                    graph_distance: Some(graph_dist),
                    context: Vec::new(),
                });
            });
        } else {
            let push = |scored: &mut Vec<HybridResult>, node_key: NodeKey, vector_score: f64| {
                let graph_dist = distance_map.get(&node_key).copied().unwrap_or(penalty_dist);
                let graph_score = 1.0 / (1.0 + graph_dist as f64);
                scored.push(HybridResult {
                    node: node_key,
                    score: alpha * vector_score + (1.0 - alpha) * graph_score,
                    graph_distance: Some(graph_dist),
                    context: Vec::new(),
                });
            };

            // Mutable tier: exact.
            for (key, node) in memgraph.iter_nodes() {
                if !crate::graph::visibility::is_node_visible(node, 0, 0, &committed, None) {
                    continue;
                }
                let Some(embedding) = view.embedding(key) else {
                    continue;
                };
                push(
                    &mut scored,
                    key,
                    simd::cosine_similarity(&embedding, &self.query_vector),
                );
            }

            for (i, seg) in csr_segs.iter().enumerate() {
                // Row filter shared by every path below: MVCC-visible, not
                // shadowed by a mutable copy-up (W2-2), and the NEWEST
                // resident copy across segments (list is newest-first).
                let row_admissible = |row: u32| -> Option<NodeKey> {
                    let meta = seg.node_meta().get(row as usize)?;
                    if !crate::graph::visibility::is_meta_visible(meta, 0, 0, &committed, None) {
                        return None;
                    }
                    let key = NodeKey::from(slotmap::KeyData::from_ffi(meta.external_id));
                    if memgraph.get_node(key).is_some() {
                        return None;
                    }
                    if csr_segs[..i]
                        .iter()
                        .any(|newer| newer.lookup_node(key).is_some())
                    {
                        return None;
                    }
                    Some(key)
                };
                let exact_row = |scored: &mut Vec<HybridResult>, row: u32, key: NodeKey| {
                    if let Some(emb) = seg.node_embedding(row) {
                        push(
                            scored,
                            key,
                            simd::cosine_similarity(&emb, &self.query_vector),
                        );
                    }
                };

                let bridge = seg
                    .hnsw_bridge()
                    .filter(|b| b.dim() == self.query_vector.len());
                let Some(bridge) = bridge else {
                    // No usable bridge (not built yet / too small / dim
                    // mismatch): exact scan of this segment.
                    for row in 0..seg.node_count() as u32 {
                        if let Some(key) = row_admissible(row) {
                            exact_row(&mut scored, row, key);
                        }
                    }
                    continue;
                };

                // (a) Frontier rows resident in this segment: exact —
                // O(frontier) lookups, not a row scan.
                for &frontier_key in distance_map.keys() {
                    let Some(row) = seg.lookup_node(frontier_key) else {
                        continue;
                    };
                    if row_admissible(row) == Some(frontier_key) {
                        exact_row(&mut scored, row, frontier_key);
                    }
                }

                // (b) Embedded rows the bridge could not index: exact.
                for &row in bridge.uncovered_embedded_rows() {
                    let Some(key) = row_admissible(row) else {
                        continue;
                    };
                    if !distance_map.contains_key(&key) {
                        exact_row(&mut scored, row, key);
                    }
                }

                // (c) The rest: bridge top-k among non-frontier rows.
                let hits = bridge.search(&self.query_vector, self.k, |row| {
                    row_admissible(row).is_some_and(|key| !distance_map.contains_key(&key))
                });
                if hits.len() < self.k.min(bridge.len()) {
                    // Beam under-filled the ask (or the allowed set is just
                    // small): rescue with an exact scan of the covered
                    // non-frontier rows — never silently truncate.
                    for row in 0..seg.node_count() as u32 {
                        if !bridge.contains_row(row) {
                            continue; // unembedded, or already exact in (b)
                        }
                        let Some(key) = row_admissible(row) else {
                            continue;
                        };
                        if !distance_map.contains_key(&key) {
                            exact_row(&mut scored, row, key);
                        }
                    }
                    continue;
                }
                for (row, sim) in hits {
                    if let Some(key) = row_admissible(row) {
                        push(&mut scored, key, sim);
                    }
                }
            }
        }

        // Step 3: Sort descending by combined score, take top-K.
        scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        scored.truncate(self.k);

        Ok(scored)
    }
}
