//! HNSW bridge for graph-embedded vectors (hybrid HnswPreFilter).
//!
//! `GRAPH.HYBRID`'s strategy selector picks `FilterStrategy::HnswPreFilter`
//! at >= 10K candidates, but until this module the executor silently
//! brute-forced anyway. The bridge builds a real HNSW graph (the vector
//! engine's `HnswBuilder`/`HnswGraph`) over a CSR segment's v5 node
//! embeddings, searched with raw-f32 cosine distances — no TurboQuant
//! codes involved.
//!
//! Lifecycle: built lazily per segment on the FIRST HnswPreFilter query
//! (`CsrStorage::hnsw_bridge`, `OnceLock` — same pattern as the incoming
//! index and the property index) and only when the segment holds at least
//! [`BRIDGE_MIN_VECTORS`] embeddings; below that, brute force wins and the
//! build cost is never paid. In-memory only — never persisted.
//!
//! Search is APPROXIMATE (standard HNSW ef-beam, filter applied to the
//! collected beam). The hybrid caller compensates: a beam that under-fills
//! the requested k falls back to exact brute-force scoring of that
//! candidate group, so results are never silently truncated.

use crate::graph::csr::CsrStorage;
use crate::graph::fasthash::FxHashMap;
use crate::vector::hnsw::build::HnswBuilder;
use crate::vector::hnsw::graph::{HnswGraph, SENTINEL};

/// Minimum embedded-node count before a segment earns an HNSW bridge.
/// Below this, brute-force cosine over the candidates is faster than the
/// build + beam cost (and the strategy selector picks BruteForce for
/// sub-10K candidate sets anyway).
pub const BRIDGE_MIN_VECTORS: usize = 4096;

/// HNSW upper-layer fanout (layer 0 uses 2*m).
const BRIDGE_M: u8 = 16;
/// Construction beam width.
const BRIDGE_EF_CONSTRUCTION: u16 = 128;
/// Deterministic level-generation seed ("moon").
const BRIDGE_SEED: u64 = 0x6d6f_6f6e;
/// Floor for the search beam width.
const MIN_EF_SEARCH: usize = 64;

/// An HNSW index over one CSR segment's node embeddings.
///
/// Bridge ids are dense `0..len` in insertion (CSR row) order; `rows` maps
/// a bridge id back to its CSR row. Embeddings are copied once at build
/// time and unit-normalized, so cosine similarity is a plain dot product
/// (identical value to `simd::cosine_similarity` on the raw vectors).
pub struct GraphHnsw {
    graph: HnswGraph,
    /// bridge id -> CSR row.
    rows: Vec<u32>,
    /// CSR row -> bridge id (allow-filter membership).
    row_to_id: FxHashMap<u32, u32>,
    /// Flat unit-normalized embeddings, bridge-id-major.
    vecs: Vec<f32>,
    dim: usize,
}

impl core::fmt::Debug for GraphHnsw {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GraphHnsw")
            .field("len", &self.rows.len())
            .field("dim", &self.dim)
            .finish()
    }
}

impl GraphHnsw {
    /// Build an HNSW bridge over a segment's v5 embeddings.
    ///
    /// Returns `None` when fewer than `min_vectors` usable embeddings exist.
    /// A usable embedding has the segment's majority dimension (the first
    /// one seen) and a non-zero norm; rows that don't qualify are simply
    /// absent from the bridge (`contains_row` false) and the hybrid caller
    /// scores them exactly instead.
    pub fn build(seg: &CsrStorage, min_vectors: usize) -> Option<GraphHnsw> {
        let node_count = seg.node_count();
        let mut rows: Vec<u32> = Vec::new();
        let mut vecs: Vec<f32> = Vec::new();
        let mut dim = 0usize;

        for row in 0..node_count {
            let Some(mut emb) = seg.node_embedding(row) else {
                continue;
            };
            if emb.is_empty() {
                continue;
            }
            if dim == 0 {
                dim = emb.len();
            }
            if emb.len() != dim {
                continue;
            }
            let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
            if !(norm.is_finite() && norm > 0.0) {
                continue;
            }
            for x in &mut emb {
                *x /= norm;
            }
            rows.push(row);
            vecs.extend_from_slice(&emb);
        }

        if rows.is_empty() || rows.len() < min_vectors.max(1) {
            return None;
        }

        let n = rows.len();
        let start = std::time::Instant::now();
        let mut builder = HnswBuilder::new(BRIDGE_M, BRIDGE_EF_CONSTRUCTION, BRIDGE_SEED);
        for _ in 0..n {
            // dist(a, b) = cosine distance between two already-inserted
            // bridge ids; vectors are unit-normalized so 1 - dot suffices.
            builder.insert(|a, b| {
                1.0 - dot(
                    &vecs[a as usize * dim..(a as usize + 1) * dim],
                    &vecs[b as usize * dim..(b as usize + 1) * dim],
                )
            });
        }
        let graph = builder.build((dim * 4) as u32);
        tracing::info!(
            vectors = n,
            dim,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "graph hnsw_bridge built"
        );

        let mut row_to_id = FxHashMap::default();
        row_to_id.reserve(n);
        for (id, &row) in rows.iter().enumerate() {
            row_to_id.insert(row, id as u32);
        }

        Some(GraphHnsw {
            graph,
            rows,
            row_to_id,
            vecs,
            dim,
        })
    }

    /// Number of indexed embeddings.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the bridge indexes no embeddings (never true post-build).
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Embedding dimensionality this bridge was built for.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Whether a CSR row is indexed in this bridge.
    #[inline]
    pub fn contains_row(&self, row: u32) -> bool {
        self.row_to_id.contains_key(&row)
    }

    #[inline]
    fn vec_of(&self, id: u32) -> &[f32] {
        &self.vecs[id as usize * self.dim..(id as usize + 1) * self.dim]
    }

    #[inline]
    fn dist_to_query(&self, q: &[f32], id: u32) -> f32 {
        1.0 - dot(q, self.vec_of(id))
    }

    /// Approximate top-k rows by cosine similarity among rows passing
    /// `allow`. Returns `(csr_row, cosine_similarity)` sorted descending;
    /// may return FEWER than k when the beam doesn't surface enough allowed
    /// rows — the caller decides whether to fall back to exact scoring.
    pub fn search(&self, query: &[f32], k: usize, allow: impl Fn(u32) -> bool) -> Vec<(u32, f64)> {
        if k == 0 || query.len() != self.dim || self.rows.is_empty() {
            return Vec::new();
        }
        let norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if !(norm.is_finite() && norm > 0.0) {
            return Vec::new();
        }
        let mut q = query.to_vec();
        for x in &mut q {
            *x /= norm;
        }

        let g = &self.graph;
        let n = g.num_nodes();

        // Greedy descent through upper layers (ORIGINAL id space).
        let mut cur = g.to_original(g.entry_point());
        let mut cur_dist = self.dist_to_query(&q, cur);
        for layer in (1..=g.max_level() as usize).rev() {
            loop {
                let mut improved = false;
                for &nb in g.neighbors_upper(cur, layer) {
                    if nb == SENTINEL {
                        continue;
                    }
                    let d = self.dist_to_query(&q, nb);
                    if d < cur_dist {
                        cur = nb;
                        cur_dist = d;
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
        }

        // Layer-0 ef-beam (BFS-reordered space). The beam ignores the
        // filter for NAVIGATION (a disallowed node still routes toward
        // allowed ones); allowed nodes are collected from the visited set.
        let ef = (k * 4).max(MIN_EF_SEARCH).min(n as usize);
        let mut visited = vec![0u64; (n as usize).div_ceil(64)];
        let mark = |bits: &mut [u64], i: u32| -> bool {
            let (w, b) = (i as usize / 64, i as usize % 64);
            let seen = bits[w] & (1 << b) != 0;
            bits[w] |= 1 << b;
            !seen
        };

        // candidates: min-heap by distance; beam: max-heap capped at ef.
        let mut candidates: std::collections::BinaryHeap<std::cmp::Reverse<OrdDist>> =
            std::collections::BinaryHeap::new();
        let mut beam: std::collections::BinaryHeap<OrdDist> = std::collections::BinaryHeap::new();
        let entry_bfs = g.to_bfs(cur);
        mark(&mut visited, entry_bfs);
        candidates.push(std::cmp::Reverse(OrdDist(cur_dist, entry_bfs)));
        beam.push(OrdDist(cur_dist, entry_bfs));

        let mut collected: Vec<(f32, u32)> = Vec::new(); // (dist, bridge id), allowed only
        let entry_orig = g.to_original(entry_bfs);
        if allow(self.rows[entry_orig as usize]) {
            collected.push((cur_dist, entry_orig));
        }

        while let Some(std::cmp::Reverse(OrdDist(c_dist, c_bfs))) = candidates.pop() {
            if beam.len() >= ef {
                if let Some(&OrdDist(worst, _)) = beam.peek() {
                    if c_dist > worst {
                        break;
                    }
                }
            }
            for &nb in g.neighbors_l0(c_bfs) {
                if nb == SENTINEL || !mark(&mut visited, nb) {
                    continue;
                }
                let nb_orig = g.to_original(nb);
                let d = self.dist_to_query(&q, nb_orig);
                let worst = beam.peek().map_or(f32::INFINITY, |o| o.0);
                if beam.len() < ef || d < worst {
                    candidates.push(std::cmp::Reverse(OrdDist(d, nb)));
                    beam.push(OrdDist(d, nb));
                    if beam.len() > ef {
                        beam.pop();
                    }
                    if allow(self.rows[nb_orig as usize]) {
                        collected.push((d, nb_orig));
                    }
                }
            }
        }

        collected.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        collected.truncate(k);
        collected
            .into_iter()
            .map(|(d, id)| (self.rows[id as usize], f64::from(1.0 - d)))
            .collect()
    }
}

/// (distance, node) ordered by distance — max-heap element.
#[derive(Clone, Copy, PartialEq)]
struct OrdDist(f32, u32);

impl Eq for OrdDist {}

impl PartialOrd for OrdDist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdDist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.1.cmp(&other.1))
    }
}

#[inline]
fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::csr::CsrSegment;
    use crate::graph::memgraph::MemGraph;
    use crate::graph::types::NodeKey;
    use smallvec::smallvec;

    /// Deterministic pseudo-random embedding (no Math.random in tests).
    fn embedding(i: u64, dim: usize) -> Vec<f32> {
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

    /// Freeze a graph of `n` embedded nodes into a CsrStorage.
    fn frozen_segment(n: usize, dim: usize) -> (CsrStorage, Vec<NodeKey>) {
        let mut g = MemGraph::new(1_000_000);
        let keys: Vec<NodeKey> = (0..n)
            .map(|i| {
                g.add_node(
                    smallvec![0u16],
                    smallvec::SmallVec::new(),
                    Some(embedding(i as u64, dim)),
                    1,
                )
            })
            .collect();
        // A few edges so the frozen graph is non-degenerate.
        for i in 0..n.saturating_sub(1) {
            g.add_edge(keys[i], keys[i + 1], 1, 1.0, None, 2)
                .expect("edge");
        }
        let frozen = g.freeze().expect("freeze");
        let seg = CsrSegment::from_frozen(frozen, 10).expect("csr");
        (CsrStorage::from(seg), keys)
    }

    /// Exact top-k (row, cosine) via the segment's embeddings.
    fn brute_force_topk(seg: &CsrStorage, query: &[f32], k: usize) -> Vec<(u32, f64)> {
        let mut scored: Vec<(u32, f64)> = (0..seg.node_count())
            .filter_map(|row| {
                let emb = seg.node_embedding(row)?;
                Some((row, crate::graph::simd::cosine_similarity(&emb, query)))
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    #[test]
    fn test_bridge_build_and_unfiltered_recall() {
        let (seg, _) = frozen_segment(300, 8);
        let bridge = GraphHnsw::build(&seg, 1).expect("bridge");
        assert_eq!(bridge.len(), 300);
        assert_eq!(bridge.dim(), 8);

        let query = embedding(9999, 8);
        let exact = brute_force_topk(&seg, &query, 10);
        let hits = bridge.search(&query, 10, |_| true);

        assert!(!hits.is_empty());
        // Scores must be REAL cosines (parity with simd::cosine_similarity).
        for &(row, sim) in &hits {
            let emb = seg.node_embedding(row).expect("emb");
            let expect = crate::graph::simd::cosine_similarity(&emb, &query);
            assert!(
                (sim - expect).abs() < 1e-4,
                "row {row}: bridge {sim} vs exact {expect}"
            );
        }
        // Recall: at least 8/10 of the exact top-10 surfaced.
        let exact_rows: std::collections::HashSet<u32> = exact.iter().map(|&(r, _)| r).collect();
        let overlap = hits
            .iter()
            .filter(|&&(r, _)| exact_rows.contains(&r))
            .count();
        assert!(overlap >= 8, "recall too low: {overlap}/10");
    }

    #[test]
    fn test_bridge_filtered_search_respects_allow() {
        let (seg, _) = frozen_segment(300, 8);
        let bridge = GraphHnsw::build(&seg, 1).expect("bridge");

        let query = embedding(4242, 8);
        let hits = bridge.search(&query, 10, |row| row % 2 == 0);
        assert!(!hits.is_empty());
        for &(row, _) in &hits {
            assert_eq!(row % 2, 0, "disallowed row {row} leaked through filter");
        }
        // Descending scores.
        for w in hits.windows(2) {
            assert!(w[0].1 >= w[1].1);
        }
    }

    #[test]
    fn test_bridge_min_vectors_gate() {
        let (seg, _) = frozen_segment(300, 8);
        assert!(GraphHnsw::build(&seg, 10_000).is_none());
        assert!(GraphHnsw::build(&seg, 300).is_some());
    }

    #[test]
    fn test_bridge_degenerate_queries() {
        let (seg, _) = frozen_segment(64, 8);
        let bridge = GraphHnsw::build(&seg, 1).expect("bridge");
        // Dimension mismatch and zero-norm queries return empty, not junk.
        assert!(bridge.search(&[1.0; 4], 5, |_| true).is_empty());
        assert!(bridge.search(&[0.0; 8], 5, |_| true).is_empty());
        assert!(bridge.search(&embedding(1, 8), 0, |_| true).is_empty());
    }

    #[test]
    fn test_bridge_skips_unembedded_nodes() {
        let mut g = MemGraph::new(1_000_000);
        let mut keys = Vec::new();
        for i in 0..50u64 {
            let emb = if i % 2 == 0 {
                Some(embedding(i, 8))
            } else {
                None
            };
            keys.push(g.add_node(smallvec![0u16], smallvec::SmallVec::new(), emb, 1));
        }
        for i in 0..49 {
            g.add_edge(keys[i], keys[i + 1], 1, 1.0, None, 2)
                .expect("edge");
        }
        let seg = CsrStorage::from(
            CsrSegment::from_frozen(g.freeze().expect("freeze"), 10).expect("csr"),
        );
        let bridge = GraphHnsw::build(&seg, 1).expect("bridge");
        assert_eq!(bridge.len(), 25);
        // Every indexed row must actually have an embedding.
        for row in 0..seg.node_count() {
            assert_eq!(
                bridge.contains_row(row),
                seg.node_embedding(row).is_some(),
                "row {row} membership mismatch"
            );
        }
    }
}
