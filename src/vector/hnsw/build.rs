//! HNSW index builder — single-threaded construction with BFS reorder.
//!
//! Constructs an `HnswGraph` via incremental insertion, then applies BFS
//! reordering for cache-friendly layer-0 traversal.

use super::graph::{bfs_reorder, rearrange_layer0, HnswGraph, SENTINEL};
use crate::vector::aligned_buffer::AlignedBuffer;
use smallvec::SmallVec;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

/// Wrapper for (f32, u32) that implements Ord (by distance, then by ID).
#[derive(Clone, Copy, PartialEq)]
struct OrdF32Pair(f32, u32);

impl Eq for OrdF32Pair {}

impl PartialOrd for OrdF32Pair {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF32Pair {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.1.cmp(&other.1))
    }
}

/// Select the `max_neighbors` nearest candidates (simple strategy).
/// Assumes candidates are sorted by distance ascending.
fn select_neighbors_simple(candidates: &[(f32, u32)], max_neighbors: usize) -> Vec<(f32, u32)> {
    candidates.iter().take(max_neighbors).copied().collect()
}

/// Single-threaded HNSW index builder.
///
/// Usage:
/// 1. `HnswBuilder::new(m, ef_construction, seed)` to create builder
/// 2. `builder.insert(distance_fn)` for each vector (sequential IDs starting at 0)
/// 3. `builder.build(bytes_per_code)` to finalize with BFS reorder
pub struct HnswBuilder {
    m: u8,
    m0: u8,
    ef_construction: u16,
    ml: f64, // 1.0 / ln(M)

    /// Layer 0 neighbors in original insertion order.
    /// Flat array: node i at [i*m0 .. (i+1)*m0], SENTINEL-padded.
    layer0_flat: Vec<u32>,

    /// Upper layer neighbors indexed by node ID.
    upper_layers: Vec<SmallVec<[u32; 32]>>,

    /// Per-node levels.
    levels: Vec<u8>,

    /// Current entry point (highest-level node).
    entry_point: u32,

    /// Maximum level in the graph.
    max_level: u8,

    /// Number of inserted nodes.
    num_nodes: u32,

    /// LCG PRNG state for random_level.
    rng_state: u64,
}

impl HnswBuilder {
    /// Create a new HNSW builder.
    ///
    /// - `m`: max neighbors per node on upper layers (layer 0 uses 2*m)
    /// - `ef_construction`: search beam width during construction
    /// - `seed`: PRNG seed for deterministic level generation
    pub fn new(m: u8, ef_construction: u16, seed: u64) -> Self {
        let m0 = m * 2;
        let ml = 1.0 / (m as f64).ln();
        Self {
            m,
            m0,
            ef_construction,
            ml,
            layer0_flat: Vec::new(),
            upper_layers: Vec::new(),
            levels: Vec::new(),
            entry_point: 0,
            max_level: 0,
            num_nodes: 0,
            rng_state: seed,
        }
    }

    /// Generate random level using exponential distribution.
    /// P(level=l) = (1/M)^l * (1 - 1/M).
    /// Uses LCG PRNG (Knuth MMIX) for deterministic, fast generation.
    fn random_level(&mut self) -> u8 {
        // LCG: state = state * 6364136223846793005 + 1442695040888963407
        self.rng_state = self
            .rng_state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // Convert to uniform [0, 1)
        let uniform = (self.rng_state >> 33) as f64 / (1u64 << 31) as f64;
        // Avoid log(0) which is -inf
        if uniform <= 0.0 {
            return 0;
        }
        // level = floor(-ln(uniform) * ml)
        let level = (-uniform.ln() * self.ml).floor() as u8;
        level.min(32) // cap at 32 to prevent pathological cases
    }

    /// Insert a single vector into the index.
    ///
    /// `dist_fn`: closure that computes distance between any two nodes.
    ///   Signature: `|a: u32, b: u32| -> f32`
    ///
    /// Nodes must be inserted sequentially (node_id = 0, 1, 2, ...).
    pub fn insert(&mut self, dist_fn: impl Fn(u32, u32) -> f32) {
        let node_id = self.num_nodes;
        let level = self.random_level();

        // Allocate neighbor slots for new node
        let m0 = self.m0 as usize;
        self.layer0_flat.extend(std::iter::repeat_n(SENTINEL, m0));
        self.levels.push(level);

        // Allocate upper layer storage if needed
        if level > 0 {
            let upper_slots = level as usize * self.m as usize;
            let mut sv = SmallVec::with_capacity(upper_slots);
            sv.extend(std::iter::repeat_n(SENTINEL, upper_slots));
            self.upper_layers.push(sv);
        } else {
            self.upper_layers.push(SmallVec::new());
        }

        self.num_nodes += 1;

        // First node: just set as entry point
        if node_id == 0 {
            self.entry_point = 0;
            self.max_level = level;
            return;
        }

        // distance from new node to any other
        let distance_to = |other: u32| dist_fn(node_id, other);

        // Greedy descent from entry point to the level of the new node
        let mut current = self.entry_point;
        {
            let mut current_dist = distance_to(current);
            for lev in (level as usize + 1..=self.max_level as usize).rev() {
                loop {
                    let mut improved = false;
                    let neighbors = self.get_neighbors(current, lev);
                    for &nb in neighbors {
                        if nb == SENTINEL {
                            break;
                        }
                        let d = distance_to(nb);
                        if d < current_dist {
                            current = nb;
                            current_dist = d;
                            improved = true;
                        }
                    }
                    if !improved {
                        break;
                    }
                }
            }
        }

        // Insert at each level from min(level, max_level) down to 0
        let insert_from = level.min(self.max_level);
        for lev in (0..=insert_from as usize).rev() {
            let max_neighbors = if lev == 0 {
                self.m0 as usize
            } else {
                self.m as usize
            };
            let ef = self.ef_construction as usize;

            // Search layer for ef nearest neighbors
            let candidates = self.search_layer(current, &distance_to, ef, lev);

            // Select neighbors using simple heuristic (nearest M)
            let selected = select_neighbors_simple(&candidates, max_neighbors);

            // Connect new node -> selected neighbors
            self.set_neighbors(node_id, lev, &selected);

            // Connect selected neighbors -> new node (bidirectional), with pruning
            for &(_, nb_id) in &selected {
                self.add_neighbor_with_prune(nb_id, node_id, lev, &dist_fn);
            }

            // Update entry for next lower level
            if !candidates.is_empty() {
                current = candidates[0].1; // nearest node found
                let _ = candidates[0].0; // distance tracked for greedy descent
            }
        }

        // Update entry point if new node has higher level
        if level > self.max_level {
            self.entry_point = node_id;
            self.max_level = level;
        }
    }

    /// Search a single layer starting from `entry` for `ef` nearest neighbors.
    /// Returns Vec<(distance, node_id)> sorted by distance ascending.
    fn search_layer(
        &self,
        entry: u32,
        distance_to: &impl Fn(u32) -> f32,
        ef: usize,
        level: usize,
    ) -> Vec<(f32, u32)> {
        let entry_dist = distance_to(entry);

        // candidates: min-heap (closest first for processing)
        let mut candidates: BinaryHeap<Reverse<OrdF32Pair>> = BinaryHeap::new();
        // results: max-heap (farthest first for pruning)
        let mut results: BinaryHeap<OrdF32Pair> = BinaryHeap::new();
        // visited set (acceptable during construction, not on search hot path)
        let mut visited = HashSet::new();

        candidates.push(Reverse(OrdF32Pair(entry_dist, entry)));
        results.push(OrdF32Pair(entry_dist, entry));
        visited.insert(entry);

        while let Some(Reverse(OrdF32Pair(c_dist, c_id))) = candidates.pop() {
            // Early termination: if closest candidate is farther than farthest result
            if results.len() >= ef {
                if let Some(&OrdF32Pair(worst, _)) = results.peek() {
                    if c_dist > worst {
                        break;
                    }
                }
            }

            let neighbors = self.get_neighbors(c_id, level);
            for &nb in neighbors {
                if nb == SENTINEL {
                    break;
                }
                if !visited.insert(nb) {
                    continue;
                }

                let d = distance_to(nb);
                let should_add = results.len() < ef || d < results.peek().map_or(f32::MAX, |p| p.0);
                if should_add {
                    candidates.push(Reverse(OrdF32Pair(d, nb)));
                    results.push(OrdF32Pair(d, nb));
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        // Drain results into sorted vec
        let mut out: Vec<(f32, u32)> = results
            .into_vec()
            .into_iter()
            .map(|OrdF32Pair(d, id)| (d, id))
            .collect();
        out.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        });
        out
    }

    /// Get neighbors of `node_id` at `level` (reads from build-time storage).
    fn get_neighbors(&self, node_id: u32, level: usize) -> &[u32] {
        if level == 0 {
            let start = node_id as usize * self.m0 as usize;
            &self.layer0_flat[start..start + self.m0 as usize]
        } else {
            let sv = &self.upper_layers[node_id as usize];
            if sv.is_empty() {
                return &[];
            }
            let start = (level - 1) * self.m as usize;
            let end = start + self.m as usize;
            if end > sv.len() {
                return &[];
            }
            &sv[start..end]
        }
    }

    /// Set neighbors for node_id at level.
    fn set_neighbors(&mut self, node_id: u32, level: usize, neighbors: &[(f32, u32)]) {
        if level == 0 {
            let start = node_id as usize * self.m0 as usize;
            for (i, &(_, nb_id)) in neighbors.iter().enumerate() {
                self.layer0_flat[start + i] = nb_id;
            }
        } else {
            let sv = &mut self.upper_layers[node_id as usize];
            let start = (level - 1) * self.m as usize;
            for (i, &(_, nb_id)) in neighbors.iter().enumerate() {
                if start + i < sv.len() {
                    sv[start + i] = nb_id;
                }
            }
        }
    }

    /// Add node_id as a neighbor of target. If target's neighbor list is full,
    /// replace the farthest existing neighbor if node_id is closer to target.
    fn add_neighbor_with_prune(
        &mut self,
        target: u32,
        node_id: u32,
        level: usize,
        dist_fn: &impl Fn(u32, u32) -> f32,
    ) {
        let (start, max_nb) = if level == 0 {
            (target as usize * self.m0 as usize, self.m0 as usize)
        } else {
            let s = (level - 1) * self.m as usize;
            (s, self.m as usize)
        };

        // Try to find an empty sentinel slot first
        let neighbors = if level == 0 {
            &mut self.layer0_flat[start..start + max_nb]
        } else {
            let sv = &mut self.upper_layers[target as usize];
            let end = (start + max_nb).min(sv.len());
            &mut sv[start..end]
        };

        for slot in neighbors.iter_mut() {
            if *slot == SENTINEL {
                *slot = node_id;
                return;
            }
        }

        // Full: find farthest neighbor and replace if new node is closer to target
        let new_dist = dist_fn(target, node_id);
        let mut worst_dist = 0.0f32;
        let mut worst_idx = 0;

        let neighbors = if level == 0 {
            &self.layer0_flat[start..start + max_nb]
        } else {
            let sv = &self.upper_layers[target as usize];
            let end = (start + max_nb).min(sv.len());
            &sv[start..end]
        };

        for (i, &nb) in neighbors.iter().enumerate() {
            if nb == SENTINEL {
                break;
            }
            let d = dist_fn(target, nb);
            if d > worst_dist {
                worst_dist = d;
                worst_idx = i;
            }
        }

        if new_dist < worst_dist {
            if level == 0 {
                self.layer0_flat[start + worst_idx] = node_id;
            } else {
                self.upper_layers[target as usize][start + worst_idx] = node_id;
            }
        }
    }

    /// Finalize construction: apply BFS reorder and return immutable HnswGraph.
    ///
    /// `bytes_per_code`: size of each TQ code in the vector data buffer
    ///   (typically padded_dim / 2 for nibble-packed codes, but caller decides layout).
    pub fn build(self, bytes_per_code: u32) -> HnswGraph {
        if self.num_nodes == 0 {
            return HnswGraph::new(
                0,
                self.m,
                self.m0,
                0,
                0,
                AlignedBuffer::new(0),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                bytes_per_code,
            );
        }

        let (bfs_order, bfs_inverse) =
            bfs_reorder(self.num_nodes, self.m0, self.entry_point, &self.layer0_flat);

        let layer0 = rearrange_layer0(
            self.num_nodes,
            self.m0,
            &self.layer0_flat,
            &bfs_order,
            &bfs_inverse,
        );

        // Entry point in BFS space
        let bfs_entry = bfs_order[self.entry_point as usize];

        HnswGraph::new(
            self.num_nodes,
            self.m,
            self.m0,
            bfs_entry,
            self.max_level,
            layer0,
            bfs_order,
            bfs_inverse,
            self.upper_layers,
            self.levels,
            bytes_per_code,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::hnsw::graph::SENTINEL;

    /// Simple L2 distance between f32 slices (for build tests only).
    fn l2_vecs(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    /// LCG PRNG for deterministic test vectors, values in [-1.0, 1.0].
    fn lcg_f32(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    #[test]
    fn test_build_empty_graph() {
        let builder = HnswBuilder::new(16, 200, 42);
        let graph = builder.build(8);
        assert_eq!(graph.num_nodes(), 0);
    }

    #[test]
    fn test_build_single_vector() {
        let mut builder = HnswBuilder::new(16, 200, 42);
        builder.insert(|_, _| 0.0); // single vector, distance is never called meaningfully
        let graph = builder.build(8);
        assert_eq!(graph.num_nodes(), 1);
        assert_eq!(graph.entry_point(), 0); // BFS pos of entry = 0 for single node
    }

    #[test]
    fn test_build_100_vectors_all_reachable() {
        let dim = 64;
        let n = 100u32;
        let vecs: Vec<Vec<f32>> = (0..n).map(|i| lcg_f32(dim, i * 7 + 13)).collect();

        let mut builder = HnswBuilder::new(16, 200, 42);
        for _i in 0..n {
            builder.insert(|a, b| l2_vecs(&vecs[a as usize], &vecs[b as usize]));
        }
        let graph = builder.build(8);

        assert_eq!(graph.num_nodes(), n);

        // BFS from entry point should reach all nodes
        let mut visited = vec![false; n as usize];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(graph.entry_point());
        visited[graph.entry_point() as usize] = true;
        let mut count = 1u32;

        while let Some(pos) = queue.pop_front() {
            let neighbors = graph.neighbors_l0(pos);
            for &nb in neighbors {
                if nb == SENTINEL {
                    break;
                }
                if !visited[nb as usize] {
                    visited[nb as usize] = true;
                    count += 1;
                    queue.push_back(nb);
                }
            }
        }

        assert_eq!(count, n, "not all nodes reachable from entry point via BFS");
    }

    #[test]
    fn test_random_level_distribution() {
        let mut builder = HnswBuilder::new(16, 200, 42);
        let mut level_counts = [0u32; 5];
        let total = 10_000;

        for _ in 0..total {
            let level = builder.random_level() as usize;
            if level < level_counts.len() {
                level_counts[level] += 1;
            }
        }

        // With M=16, ml = 1/ln(16) ~ 0.3607
        // P(level=0) = 1 - 1/M = 15/16 = 0.9375 => ~9375
        // P(level=1) ~ 1/16 * 15/16 ~ 0.0586 => ~586
        // P(level>=2) ~ 0.0039 => ~39
        let level0_pct = level_counts[0] as f64 / total as f64;
        let level1_pct = level_counts[1] as f64 / total as f64;

        // Allow generous tolerances for 10K samples
        assert!(
            level0_pct > 0.88 && level0_pct < 0.98,
            "level 0 should be ~93.75%, got {:.2}%",
            level0_pct * 100.0
        );
        assert!(
            level1_pct > 0.02 && level1_pct < 0.10,
            "level 1 should be ~5.8%, got {:.2}%",
            level1_pct * 100.0
        );
    }

    #[test]
    fn test_build_500_vectors_neighbor_bounds() {
        let dim = 32;
        let n = 500u32;
        let m: u8 = 16;
        let m0 = m * 2;
        let vecs: Vec<Vec<f32>> = (0..n).map(|i| lcg_f32(dim, i * 3 + 7)).collect();

        let mut builder = HnswBuilder::new(m, 200, 123);
        for i in 0..n {
            builder.insert(|a, b| l2_vecs(&vecs[a as usize], &vecs[b as usize]));
        }
        let graph = builder.build(8);

        // Check all layer-0 neighbor counts are <= M0
        for bfs_pos in 0..n {
            let neighbors = graph.neighbors_l0(bfs_pos);
            let count = neighbors.iter().filter(|&&nb| nb != SENTINEL).count();
            assert!(
                count <= m0 as usize,
                "node {} has {} layer-0 neighbors, max is {}",
                bfs_pos,
                count,
                m0
            );
        }
    }

    #[test]
    fn test_bfs_reorder_valid_permutation() {
        let dim = 16;
        let n = 50u32;
        let vecs: Vec<Vec<f32>> = (0..n).map(|i| lcg_f32(dim, i * 11 + 5)).collect();

        let mut builder = HnswBuilder::new(8, 100, 99);
        for i in 0..n {
            builder.insert(|a, b| l2_vecs(&vecs[a as usize], &vecs[b as usize]));
        }
        let graph = builder.build(8);

        // Verify BFS inverse is a valid permutation
        let mut ids: Vec<u32> = (0..n).map(|pos| graph.to_original(pos)).collect();
        ids.sort();
        let expected: Vec<u32> = (0..n).collect();
        assert_eq!(ids, expected, "bfs_inverse should be a permutation of 0..n");
    }

    #[test]
    fn test_select_neighbors_simple_bounds() {
        let candidates: Vec<(f32, u32)> = (0..10).map(|i| (i as f32, i)).collect();
        let selected = select_neighbors_simple(&candidates, 4);
        assert_eq!(selected.len(), 4);
        // Should be the first 4 (nearest, since candidates are sorted)
        assert_eq!(selected[0].1, 0);
        assert_eq!(selected[1].1, 1);
        assert_eq!(selected[2].1, 2);
        assert_eq!(selected[3].1, 3);
    }

    #[test]
    fn test_select_neighbors_simple_fewer_than_max() {
        let candidates: Vec<(f32, u32)> = vec![(1.0, 0), (2.0, 1)];
        let selected = select_neighbors_simple(&candidates, 4);
        assert_eq!(selected.len(), 2);
    }
}
