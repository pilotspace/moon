//! HNSW search using f32 L2 distance for graph traversal.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use super::graph::{HnswGraph, SENTINEL};
use crate::vector::distance;
use crate::vector::types::{SearchResult, VectorId};

/// HNSW search using f32 L2 distance.
///
/// `vectors_f32`: f32 vectors in BFS order, flat layout.
/// `dim`: f32 elements per vector.
pub fn hnsw_search_f32(
    graph: &HnswGraph,
    vectors_f32: &[f32],
    dim: usize,
    query: &[f32],
    k: usize,
    ef_search: usize,
    allow_bitmap: Option<&RoaringBitmap>,
) -> SmallVec<[SearchResult; 32]> {
    let num_nodes = graph.num_nodes();
    if num_nodes == 0 {
        return SmallVec::new();
    }

    let ef = ef_search.max(k);
    let l2_fn = distance::table().l2_f32;

    let dist_bfs = |bfs_pos: u32| -> f32 {
        let offset = bfs_pos as usize * dim;
        (l2_fn)(query, &vectors_f32[offset..offset + dim])
    };

    // Upper layer descent
    let mut current_orig = graph.to_original(graph.entry_point());
    let mut current_dist = dist_bfs(graph.entry_point());

    for layer in (1..=graph.max_level() as usize).rev() {
        loop {
            let mut improved = false;
            for &nb in graph.neighbors_upper(current_orig, layer) {
                if nb == SENTINEL {
                    break;
                }
                let nb_bfs = graph.to_bfs(nb);
                let d = dist_bfs(nb_bfs);
                if d < current_dist {
                    current_orig = nb;
                    current_dist = d;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
    }

    // Layer 0 beam search using simple Vec<bool> for visited tracking
    // (BitVec had potential issues — use simple approach for correctness)
    let entry_bfs = graph.to_bfs(current_orig);
    let mut visited = vec![false; num_nodes as usize];
    visited[entry_bfs as usize] = true;

    let mut candidates: BinaryHeap<Reverse<(OrderedFloat<f32>, u32)>> = BinaryHeap::new();
    let mut results: BinaryHeap<(OrderedFloat<f32>, u32)> = BinaryHeap::new();

    candidates.push(Reverse((OrderedFloat(current_dist), entry_bfs)));

    let passes = |bfs_pos: u32| -> bool {
        match &allow_bitmap {
            None => true,
            Some(bm) => bm.contains(graph.to_original(bfs_pos)),
        }
    };

    if passes(entry_bfs) {
        results.push((OrderedFloat(current_dist), entry_bfs));
    }

    while let Some(Reverse((OrderedFloat(c_dist), c_bfs))) = candidates.pop() {
        if results.len() >= ef {
            if let Some(&(OrderedFloat(worst), _)) = results.peek() {
                if c_dist > worst {
                    break;
                }
            }
        }

        for &nb_bfs in graph.neighbors_l0(c_bfs) {
            if nb_bfs == SENTINEL {
                break;
            }
            if nb_bfs >= num_nodes {
                continue;
            }
            if visited[nb_bfs as usize] {
                continue;
            }
            visited[nb_bfs as usize] = true;

            let d = dist_bfs(nb_bfs);

            let dominated = results.len() >= ef && d >= results.peek().unwrap().0.0;
            if !dominated {
                candidates.push(Reverse((OrderedFloat(d), nb_bfs)));
                if passes(nb_bfs) {
                    results.push((OrderedFloat(d), nb_bfs));
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
    }

    // Extract top-K
    let mut collected: Vec<(f32, u32)> = results
        .into_iter()
        .map(|(d, b)| (d.0, graph.to_original(b)))
        .collect();
    collected.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    collected.truncate(k);

    collected
        .into_iter()
        .map(|(d, orig)| SearchResult::new(d, VectorId(orig)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::hnsw::build::HnswBuilder;

    fn gen_unit_vectors(n: usize, d: usize, seed: u64) -> Vec<f32> {
        let mut rng = seed;
        let mut vecs = Vec::with_capacity(n * d);
        for _ in 0..n {
            let mut v: Vec<f32> = (0..d)
                .map(|_| {
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let u1 = ((rng >> 40) as f32 / (1u64 << 24) as f32).max(1e-7);
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let u2 = (rng >> 40) as f32 / (1u64 << 24) as f32;
                    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos()
                })
                .collect();
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                for x in v.iter_mut() {
                    *x /= norm;
                }
            }
            vecs.extend_from_slice(&v);
        }
        vecs
    }

    fn measure_recall(n: u32, d: usize, nq: usize, ef: usize, k: usize) -> f64 {
        distance::init();
        let vectors = gen_unit_vectors(n as usize, d, 42);
        let queries = gen_unit_vectors(nq, d, 999);
        let l2_fn = distance::table().l2_f32;

        let mut builder = HnswBuilder::new(16, 200, 42);
        for _ in 0..n {
            builder.insert(|a, b| {
                (l2_fn)(
                    &vectors[a as usize * d..(a as usize + 1) * d],
                    &vectors[b as usize * d..(b as usize + 1) * d],
                )
            });
        }
        let graph = builder.build(d as u32);

        // BFS-reorder
        let mut vf = vec![0.0f32; n as usize * d];
        for orig in 0..n as usize {
            let bfs = graph.to_bfs(orig as u32) as usize;
            vf[bfs * d..(bfs + 1) * d].copy_from_slice(&vectors[orig * d..(orig + 1) * d]);
        }

        let mut total = 0.0;
        for qi in 0..nq {
            let q = &queries[qi * d..(qi + 1) * d];
            let mut bf: Vec<(f32, u32)> = (0..n)
                .map(|i| {
                    (
                        (l2_fn)(q, &vectors[i as usize * d..(i as usize + 1) * d]),
                        i,
                    )
                })
                .collect();
            bf.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let gt: Vec<u32> = bf[..k].iter().map(|x| x.1).collect();

            let results = hnsw_search_f32(&graph, &vf, d, q, k, ef, None);
            let pred: Vec<u32> = results.iter().map(|r| r.id.0).collect();
            let tp = pred.iter().filter(|id| gt.contains(id)).count();
            total += tp as f64 / k as f64;
        }
        total / nq as f64
    }

    #[test]
    fn test_f32_recall_1k_128d() {
        let recall = measure_recall(1000, 128, 100, 128, 10);
        println!("F32 HNSW Recall@10 (1K/128d ef=128): {recall:.4}");
        assert!(recall >= 0.95, "F32 recall {recall} below 0.95");
    }

    #[test]
    fn test_f32_recall_10k_128d() {
        let recall = measure_recall(10000, 128, 50, 200, 10);
        println!("F32 HNSW Recall@10 (10K/128d ef=200): {recall:.4}");
        assert!(recall >= 0.90, "F32 recall {recall} below 0.90");
    }

    #[test]
    fn test_f32_recall_1k_768d() {
        let recall = measure_recall(1000, 768, 50, 128, 10);
        println!("F32 HNSW Recall@10 (1K/768d ef=128): {recall:.4}");
        assert!(recall >= 0.95, "F32 recall {recall} below 0.95");
    }
}
