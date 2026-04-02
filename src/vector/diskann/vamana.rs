//! Vamana graph construction and greedy search for DiskANN cold tier.
//!
//! Implements the DiskANN algorithm: build a Vamana graph from raw vectors
//! (or warm-start from an HNSW layer-0 graph), then support greedy beam search.
//! Uses scalar L2 distance -- this runs at build time, not on the hot search path.

use crate::vector::hnsw::graph::HnswGraph;

/// Scalar squared-L2 distance. Build-time only -- not on hot search path.
#[inline]
fn l2_distance(a: &[f32], b: &[f32], dim: usize) -> f32 {
    let mut sum = 0.0_f32;
    for i in 0..dim {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

/// Vamana graph for DiskANN cold-tier vector search.
///
/// Each node has at most `max_degree` (R) neighbors. The entry point is the
/// medoid (node closest to dataset centroid). Built via two-pass alpha-pruning
/// refinement per the DiskANN paper.
pub struct VamanaGraph {
    num_nodes: u32,
    max_degree: u32,
    entry_point: u32,
    adjacency: Vec<Vec<u32>>,
}

impl VamanaGraph {
    /// Build a Vamana graph from raw vectors.
    ///
    /// * `vectors` -- flat f32 array of `n * dim` elements
    /// * `dim` -- vector dimensionality
    /// * `r` -- max degree (R parameter)
    /// * `l` -- search list size (L parameter, must be >= r)
    pub fn build(vectors: &[f32], dim: usize, r: u32, l: u32) -> Self {
        let n = vectors.len() / dim;
        assert!(n > 0, "need at least one vector");
        assert!(l >= r, "L must be >= R");

        // Compute centroid
        let mut centroid = vec![0.0_f32; dim];
        for i in 0..n {
            let v = &vectors[i * dim..(i + 1) * dim];
            for (j, &val) in v.iter().enumerate() {
                centroid[j] += val;
            }
        }
        let inv_n = 1.0 / n as f32;
        for c in &mut centroid {
            *c *= inv_n;
        }

        // Find medoid (closest to centroid)
        let entry_point = find_medoid(vectors, dim, &centroid, n);

        // Initialize adjacency with random neighbors
        let mut adjacency = init_random_adjacency(n, r);

        // Two-pass Vamana refinement: alpha=1.0 then alpha=1.2
        let pass_order = deterministic_permutation(n, 42);
        vamana_pass(vectors, dim, r, l, 1.0, &pass_order, entry_point, &mut adjacency);
        let pass_order2 = deterministic_permutation(n, 137);
        vamana_pass(vectors, dim, r, l, 1.2, &pass_order2, entry_point, &mut adjacency);

        Self {
            num_nodes: n as u32,
            max_degree: r,
            entry_point,
            adjacency,
        }
    }

    /// Build a Vamana graph warm-started from an HNSW layer-0 graph.
    ///
    /// Initializes adjacency from HNSW L0 neighbors (truncated to R), then
    /// runs the standard two-pass Vamana refinement.
    pub fn build_from_hnsw(hnsw: &HnswGraph, vectors: &[f32], dim: usize, r: u32, l: u32) -> Self {
        let n = hnsw.num_nodes() as usize;
        assert!(n > 0, "HNSW graph must have at least one node");
        assert_eq!(vectors.len(), n * dim, "vector count must match HNSW node count");
        assert!(l >= r, "L must be >= R");

        // Compute centroid and medoid
        let mut centroid = vec![0.0_f32; dim];
        for i in 0..n {
            let v = &vectors[i * dim..(i + 1) * dim];
            for (j, &val) in v.iter().enumerate() {
                centroid[j] += val;
            }
        }
        let inv_n = 1.0 / n as f32;
        for c in &mut centroid {
            *c *= inv_n;
        }
        let entry_point = find_medoid(vectors, dim, &centroid, n);

        // Initialize from HNSW layer-0 neighbors
        let mut adjacency: Vec<Vec<u32>> = Vec::with_capacity(n);
        for orig_id in 0..n as u32 {
            let bfs_pos = hnsw.to_bfs(orig_id);
            let hnsw_neighbors = hnsw.neighbors_l0(bfs_pos);
            let mut neighbors = Vec::with_capacity(r as usize);
            for &nbr in hnsw_neighbors {
                if nbr == crate::vector::hnsw::graph::SENTINEL {
                    break;
                }
                let orig_nbr = hnsw.to_original(nbr);
                if neighbors.len() < r as usize {
                    neighbors.push(orig_nbr);
                }
            }
            adjacency.push(neighbors);
        }

        // Two-pass Vamana refinement
        let pass_order = deterministic_permutation(n, 42);
        vamana_pass(vectors, dim, r, l, 1.0, &pass_order, entry_point, &mut adjacency);
        let pass_order2 = deterministic_permutation(n, 137);
        vamana_pass(vectors, dim, r, l, 1.2, &pass_order2, entry_point, &mut adjacency);

        Self {
            num_nodes: n as u32,
            max_degree: r,
            entry_point,
            adjacency,
        }
    }

    /// Greedy beam search starting from the entry point.
    ///
    /// Returns up to `l` nearest neighbors as `(node_id, distance)` pairs sorted
    /// by ascending distance.
    pub fn greedy_search(
        &self,
        query: &[f32],
        vectors: &[f32],
        dim: usize,
        l: u32,
    ) -> Vec<(u32, f32)> {
        let n = self.num_nodes as usize;
        let l = l as usize;

        // Two separate bitsets: "seen" (distance computed) and "expanded" (neighbors visited)
        let mut seen = vec![false; n];
        let mut expanded = vec![false; n];

        let ep = self.entry_point as usize;
        let ep_dist = l2_distance(query, &vectors[ep * dim..(ep + 1) * dim], dim);
        seen[ep] = true;

        // Candidate list: (distance, node_id)
        let mut candidates: Vec<(f32, u32)> = vec![(ep_dist, self.entry_point)];

        loop {
            // Find best unexpanded candidate in the current list
            let mut best_idx = None;
            let mut best_dist = f32::MAX;
            for (i, &(dist, node)) in candidates.iter().enumerate() {
                if dist < best_dist && !expanded[node as usize] {
                    best_dist = dist;
                    best_idx = Some(i);
                }
            }

            let Some(idx) = best_idx else { break };
            let (_, node) = candidates[idx];
            expanded[node as usize] = true;

            // Expand neighbors
            for &nbr in &self.adjacency[node as usize] {
                if nbr >= n as u32 || seen[nbr as usize] {
                    continue;
                }
                seen[nbr as usize] = true;
                let d = l2_distance(
                    query,
                    &vectors[nbr as usize * dim..(nbr as usize + 1) * dim],
                    dim,
                );
                candidates.push((d, nbr));
            }

            // Keep only best L candidates
            candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            candidates.truncate(l);
        }

        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        candidates.iter().map(|&(d, id)| (id, d)).collect()
    }

    /// Get the neighbor list for a given node.
    #[inline]
    pub fn neighbors(&self, node_id: u32) -> &[u32] {
        &self.adjacency[node_id as usize]
    }

    /// Total number of nodes in the graph.
    #[inline]
    pub fn num_nodes(&self) -> u32 {
        self.num_nodes
    }

    /// Graph entry point (medoid).
    #[inline]
    pub fn entry_point(&self) -> u32 {
        self.entry_point
    }

    /// Maximum degree (R parameter).
    #[inline]
    pub fn max_degree(&self) -> u32 {
        self.max_degree
    }
}

// ---- Internal helpers ----

/// Find the node closest to the centroid (medoid).
fn find_medoid(vectors: &[f32], dim: usize, centroid: &[f32], n: usize) -> u32 {
    let mut best = 0u32;
    let mut best_dist = f32::MAX;
    for i in 0..n {
        let d = l2_distance(&vectors[i * dim..(i + 1) * dim], centroid, dim);
        if d < best_dist {
            best_dist = d;
            best = i as u32;
        }
    }
    best
}

/// Initialize adjacency with deterministic pseudo-random neighbors.
fn init_random_adjacency(n: usize, r: u32) -> Vec<Vec<u32>> {
    let r = r as usize;
    let mut adjacency: Vec<Vec<u32>> = Vec::with_capacity(n);
    for i in 0..n {
        let mut neighbors = Vec::with_capacity(r.min(n - 1));
        // Use a simple deterministic hash to pick neighbors
        let mut seed = (i as u32).wrapping_mul(2654435761);
        let mut count = 0;
        while count < r && count < n - 1 {
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            let candidate = (seed % n as u32) as usize;
            if candidate != i && !neighbors.contains(&(candidate as u32)) {
                neighbors.push(candidate as u32);
                count += 1;
            }
        }
        adjacency.push(neighbors);
    }
    adjacency
}

/// Create a deterministic permutation of [0..n) using Fisher-Yates with LCG.
fn deterministic_permutation(n: usize, seed: u32) -> Vec<u32> {
    let mut perm: Vec<u32> = (0..n as u32).collect();
    let mut rng = seed;
    for i in (1..n).rev() {
        rng = rng.wrapping_mul(1664525).wrapping_add(1013904223);
        let j = (rng as usize) % (i + 1);
        perm.swap(i, j);
    }
    perm
}

/// Run one pass of Vamana index construction.
fn vamana_pass(
    vectors: &[f32],
    dim: usize,
    r: u32,
    l: u32,
    alpha: f32,
    order: &[u32],
    entry_point: u32,
    adjacency: &mut [Vec<u32>],
) {
    let n = adjacency.len();
    for &p in order {
        // Greedy search for p's vector from entry_point
        let query = &vectors[p as usize * dim..(p as usize + 1) * dim];
        let mut candidates = greedy_search_internal(
            query, vectors, dim, l as usize, entry_point, adjacency, n,
        );

        // Add current neighbors to candidate set
        for &nbr in &adjacency[p as usize] {
            let d = l2_distance(query, &vectors[nbr as usize * dim..(nbr as usize + 1) * dim], dim);
            if !candidates.iter().any(|&(_, id)| id == nbr) {
                candidates.push((d, nbr));
            }
        }

        // Remove p from candidates
        candidates.retain(|&(_, id)| id != p);

        // Robust prune
        let new_neighbors = robust_prune(&candidates, vectors, dim, alpha, r);
        adjacency[p as usize] = new_neighbors.clone();

        // Add reverse edges and prune if needed
        for &nbr in &new_neighbors {
            if nbr >= n as u32 {
                continue;
            }
            let nbr_adj = &adjacency[nbr as usize];
            if !nbr_adj.contains(&p) {
                if nbr_adj.len() < r as usize {
                    adjacency[nbr as usize].push(p);
                } else {
                    // Need to robust_prune the neighbor
                    let nbr_vec = &vectors[nbr as usize * dim..(nbr as usize + 1) * dim];
                    let mut nbr_candidates: Vec<(f32, u32)> = adjacency[nbr as usize]
                        .iter()
                        .map(|&id| {
                            let d = l2_distance(nbr_vec, &vectors[id as usize * dim..(id as usize + 1) * dim], dim);
                            (d, id)
                        })
                        .collect();
                    let d_p = l2_distance(nbr_vec, &vectors[p as usize * dim..(p as usize + 1) * dim], dim);
                    nbr_candidates.push((d_p, p));
                    adjacency[nbr as usize] = robust_prune(&nbr_candidates, vectors, dim, alpha, r);
                }
            }
        }
    }
}

/// Internal greedy search used during graph construction.
fn greedy_search_internal(
    query: &[f32],
    vectors: &[f32],
    dim: usize,
    l: usize,
    entry_point: u32,
    adjacency: &[Vec<u32>],
    n: usize,
) -> Vec<(f32, u32)> {
    let mut visited = vec![false; n];
    let ep_dist = l2_distance(query, &vectors[entry_point as usize * dim..(entry_point as usize + 1) * dim], dim);
    visited[entry_point as usize] = true;

    let mut candidates: Vec<(f32, u32)> = vec![(ep_dist, entry_point)];
    let mut expanded = vec![false; n];

    loop {
        // Find best unexpanded candidate
        let mut best_idx = None;
        let mut best_dist = f32::MAX;
        for (i, &(dist, node)) in candidates.iter().enumerate() {
            if dist < best_dist && !expanded[node as usize] {
                best_dist = dist;
                best_idx = Some(i);
            }
        }

        let Some(idx) = best_idx else { break };
        let (_, node) = candidates[idx];
        expanded[node as usize] = true;

        // Expand
        for &nbr in &adjacency[node as usize] {
            if nbr >= n as u32 || visited[nbr as usize] {
                continue;
            }
            visited[nbr as usize] = true;
            let d = l2_distance(query, &vectors[nbr as usize * dim..(nbr as usize + 1) * dim], dim);
            candidates.push((d, nbr));
        }

        // Prune to L
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        candidates.truncate(l);
    }

    candidates
}

/// DiskANN robust prune: select neighbors with good angular diversity.
///
/// Greedily picks the closest candidate, then removes any candidate that is
/// alpha-dominated by the selected neighbor. Ensures degree <= R.
fn robust_prune(
    candidates: &[(f32, u32)],
    vectors: &[f32],
    dim: usize,
    alpha: f32,
    r: u32,
) -> Vec<u32> {
    let mut sorted: Vec<(f32, u32)> = candidates.to_vec();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    let mut result: Vec<u32> = Vec::with_capacity(r as usize);
    let mut remaining = sorted;

    while !remaining.is_empty() && result.len() < r as usize {
        let (_, best) = remaining[0];
        result.push(best);

        let best_vec = &vectors[best as usize * dim..(best as usize + 1) * dim];

        // Remove candidates alpha-dominated by `best`
        remaining = remaining[1..]
            .iter()
            .filter(|&&(dist_to_query, cand)| {
                let dist_cand_to_best = l2_distance(
                    &vectors[cand as usize * dim..(cand as usize + 1) * dim],
                    best_vec,
                    dim,
                );
                // Keep if NOT alpha-dominated: dist(cand, best) >= dist(cand, query) / alpha
                // Equivalently: alpha * dist(cand, best) >= dist(cand, query)
                alpha * dist_cand_to_best >= dist_to_query
            })
            .copied()
            .collect();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic f32 vector via LCG PRNG, values in [-1.0, 1.0].
    fn deterministic_f32(dim: usize, seed: u64) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed as u32;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    /// Generate n random vectors of given dimension.
    fn random_vectors(n: usize, dim: usize, base_seed: u64) -> Vec<f32> {
        let mut all = Vec::with_capacity(n * dim);
        for i in 0..n {
            all.extend(deterministic_f32(dim, base_seed + i as u64));
        }
        all
    }

    /// Brute-force nearest neighbor.
    fn brute_force_nn(query: &[f32], vectors: &[f32], dim: usize) -> u32 {
        let n = vectors.len() / dim;
        let mut best = 0u32;
        let mut best_dist = f32::MAX;
        for i in 0..n {
            let d = l2_distance(query, &vectors[i * dim..(i + 1) * dim], dim);
            if d < best_dist {
                best_dist = d;
                best = i as u32;
            }
        }
        best
    }

    #[test]
    fn test_build_correct_node_count() {
        let n = 100;
        let dim = 128;
        let vectors = random_vectors(n, dim, 1000);
        let graph = VamanaGraph::build(&vectors, dim, 32, 50);
        assert_eq!(graph.num_nodes(), n as u32);
    }

    #[test]
    fn test_all_nodes_degree_le_r() {
        let n = 100;
        let dim = 128;
        let r = 32;
        let vectors = random_vectors(n, dim, 2000);
        let graph = VamanaGraph::build(&vectors, dim, r, 50);
        for i in 0..n {
            assert!(
                graph.neighbors(i as u32).len() <= r as usize,
                "node {} has degree {} > R={}",
                i,
                graph.neighbors(i as u32).len(),
                r,
            );
        }
    }

    #[test]
    fn test_entry_point_is_medoid() {
        let n = 100;
        let dim = 128;
        let vectors = random_vectors(n, dim, 3000);

        // Compute centroid
        let mut centroid = vec![0.0_f32; dim];
        for i in 0..n {
            let v = &vectors[i * dim..(i + 1) * dim];
            for (j, &val) in v.iter().enumerate() {
                centroid[j] += val;
            }
        }
        let inv_n = 1.0 / n as f32;
        for c in &mut centroid {
            *c *= inv_n;
        }

        let expected_medoid = find_medoid(&vectors, dim, &centroid, n);
        let graph = VamanaGraph::build(&vectors, dim, 32, 50);
        assert_eq!(graph.entry_point(), expected_medoid);
    }

    #[test]
    fn test_greedy_search_recall() {
        let n = 100;
        let dim = 128;
        let vectors = random_vectors(n, dim, 4000);
        let graph = VamanaGraph::build(&vectors, dim, 32, 50);

        // Run 50 queries, check recall@1
        let mut correct = 0;
        let num_queries = 50;
        for q in 0..num_queries {
            let query = deterministic_f32(dim, 5000 + q);
            let results = graph.greedy_search(&query, &vectors, dim, 50);
            let true_nn = brute_force_nn(&query, &vectors, dim);
            if !results.is_empty() && results[0].0 == true_nn {
                correct += 1;
            }
        }

        let recall = correct as f64 / num_queries as f64;
        assert!(
            recall >= 0.80,
            "recall@1 = {recall:.2} < 0.80 (correct={correct}/{num_queries})",
        );
    }

    #[test]
    fn test_max_degree_accessor() {
        let vectors = random_vectors(10, 8, 6000);
        let graph = VamanaGraph::build(&vectors, 8, 5, 5);
        assert_eq!(graph.max_degree(), 5);
    }
}
