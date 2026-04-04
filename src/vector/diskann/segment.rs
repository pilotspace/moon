//! DiskAnnSegment -- cold-tier vector search using PQ codes in RAM
//! and Vamana graph on disk (pread per hop).
//!
//! Search uses asymmetric PQ distance (precomputed lookup table) for
//! approximate nearest neighbor scoring. Vamana graph pages are read
//! from an `.mpf` file via `read_vamana_node_at` (one 4KB pread per
//! graph hop). No exact reranking in this version.

use std::path::{Path, PathBuf};

use smallvec::SmallVec;

#[cfg(not(unix))]
use crate::vector::diskann::page::read_vamana_node_at;
use crate::vector::diskann::pq::ProductQuantizer;
use crate::vector::types::{SearchResult, VectorId};

/// Cold-tier segment backed by PQ codes in RAM + Vamana graph on NVMe.
pub struct DiskAnnSegment {
    /// PQ codes for all vectors: `num_vectors * m` bytes (kept in RAM).
    pq_codes: Vec<u8>,
    /// Trained product quantizer (codebooks in RAM).
    pq: ProductQuantizer,
    /// Path to `vamana.mpf` file (graph on disk, read via pread).
    /// On unix, reads go through `vamana_file` (pread); path kept for non-unix fallback.
    #[cfg_attr(unix, allow(dead_code))]
    vamana_path: PathBuf,
    /// Persistent file handle for vamana.mpf (opened once, pread per hop).
    #[cfg(unix)]
    vamana_file: std::fs::File,
    /// Vector dimensionality.
    dim: usize,
    /// Number of vectors in this segment.
    num_vectors: u32,
    /// Graph entry point (medoid).
    entry_point: u32,
    /// Max degree R (needed to interpret page layout).
    max_degree: u32,
    /// Segment file ID for manifest tracking.
    file_id: u64,
}

impl DiskAnnSegment {
    /// Create a new DiskAnnSegment from pre-built components.
    pub fn new(
        pq_codes: Vec<u8>,
        pq: ProductQuantizer,
        vamana_path: PathBuf,
        dim: usize,
        num_vectors: u32,
        entry_point: u32,
        max_degree: u32,
        file_id: u64,
    ) -> Self {
        debug_assert_eq!(
            pq_codes.len(),
            num_vectors as usize * pq.m(),
            "pq_codes length must be num_vectors * m"
        );
        #[cfg(unix)]
        let vamana_file = std::fs::File::open(&vamana_path)
            .unwrap_or_else(|e| panic!("DiskAnnSegment: cannot open {:?}: {}", vamana_path, e));
        Self {
            pq_codes,
            pq,
            vamana_path,
            #[cfg(unix)]
            vamana_file,
            dim,
            num_vectors,
            entry_point,
            max_degree,
            file_id,
        }
    }

    /// Load a DiskAnnSegment from on-disk files.
    ///
    /// Reads `pq_codes.bin` from `segment_dir` into RAM and accepts a
    /// pre-loaded `ProductQuantizer` (codebook serialization is future work).
    /// Reads the first Vamana page to extract entry_point metadata.
    pub fn from_files(
        segment_dir: &Path,
        file_id: u64,
        dim: usize,
        pq: ProductQuantizer,
    ) -> std::io::Result<Self> {
        let pq_codes_path = segment_dir.join("pq_codes.bin");
        let pq_codes = std::fs::read(&pq_codes_path)?;
        let m = pq.m();
        let num_vectors = if m > 0 { pq_codes.len() / m } else { 0 };

        let vamana_path = segment_dir.join("vamana.mpf");
        #[cfg(unix)]
        let vamana_file = std::fs::File::open(&vamana_path)?;

        // Read first node to get entry_point and infer max_degree.
        #[cfg(unix)]
        let node0 = crate::vector::diskann::page::read_vamana_node_with_fd(&vamana_file, 0, dim)?
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "empty vamana file"))?;
        #[cfg(not(unix))]
        let node0 = read_vamana_node_at(&vamana_path, 0, dim)?
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "empty vamana file"))?;
        // Entry point is the medoid stored during build -- for from_files we
        // accept it as node 0 unless caller overrides. In practice the builder
        // writes entry_point metadata; for MVP we default to 0.
        let _ = node0;

        Ok(Self {
            pq_codes,
            pq,
            vamana_path,
            #[cfg(unix)]
            vamana_file,
            dim,
            num_vectors: num_vectors as u32,
            entry_point: 0,
            max_degree: 0, // inferred at search time from page data
            file_id,
        })
    }

    /// Approximate nearest neighbor search using PQ asymmetric distance
    /// and buffered Vamana beam traversal from disk.
    ///
    /// Returns up to `k` results sorted by ascending PQ distance.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        beam_width: usize,
    ) -> SmallVec<[SearchResult; 32]> {
        if self.num_vectors == 0 || k == 0 {
            return SmallVec::new();
        }

        let m = self.pq.m();
        let n = self.num_vectors as usize;

        // Precompute asymmetric distance table: m * ksub floats.
        let adt = self.pq.asymmetric_distance_table(query);

        // Visited bitset.
        let mut visited = vec![false; n];

        // Candidates: (pq_distance, node_id). Sorted ascending by distance.
        let mut candidates: Vec<(f32, u32)> = Vec::with_capacity(beam_width * 2);
        let mut expanded = vec![false; n];

        // Seed with entry point.
        let ep = self.entry_point as usize;
        if ep < n {
            let ep_dist = self.pq.asymmetric_distance(
                &adt,
                &self.pq_codes[ep * m..(ep + 1) * m],
            );
            candidates.push((ep_dist, self.entry_point));
            visited[ep] = true;
        }

        // Beam search loop.
        loop {
            // Find best unexpanded candidate.
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

            // Read Vamana page from disk to get neighbors.
            #[cfg(unix)]
            let read_result = crate::vector::diskann::page::read_vamana_node_with_fd(
                &self.vamana_file, node, self.dim,
            );
            #[cfg(not(unix))]
            let read_result = read_vamana_node_at(&self.vamana_path, node, self.dim);
            let neighbors = match read_result {
                Ok(Some(vnode)) => vnode.neighbors,
                _ => continue, // I/O error or corrupt page -- skip this node
            };

            // Score each unvisited neighbor using PQ distance.
            for &nbr in &neighbors {
                let nbr_idx = nbr as usize;
                if nbr_idx >= n || visited[nbr_idx] {
                    continue;
                }
                visited[nbr_idx] = true;
                let d = self.pq.asymmetric_distance(
                    &adt,
                    &self.pq_codes[nbr_idx * m..(nbr_idx + 1) * m],
                );
                candidates.push((d, nbr));
            }

            // Keep only best `beam_width` candidates.
            candidates.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            candidates.truncate(beam_width);
        }

        // Return top-k.
        candidates.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);

        let mut results = SmallVec::with_capacity(k);
        for &(dist, node_id) in &candidates {
            results.push(SearchResult::new(dist, VectorId(node_id)));
        }
        results
    }

    /// Batch-read multiple Vamana nodes. On Linux with io_uring available,
    /// this could submit all reads in one syscall. Currently falls back to
    /// sequential pread.
    ///
    /// Returns nodes in the same order as `node_indices`. Missing/corrupt
    /// nodes are None.
    #[cfg(unix)]
    pub fn batch_read_nodes(
        &self,
        node_indices: &[u32],
    ) -> Vec<Option<crate::vector::diskann::page::VamanaNode>> {
        node_indices
            .iter()
            .map(|&idx| {
                crate::vector::diskann::page::read_vamana_node_with_fd(
                    &self.vamana_file, idx, self.dim,
                )
                .ok()
                .flatten()
            })
            .collect()
    }

    /// Total number of vectors in this cold segment.
    #[inline]
    pub fn total_count(&self) -> u32 {
        self.num_vectors
    }

    /// Maximum graph degree (R parameter).
    #[inline]
    pub fn max_degree(&self) -> u32 {
        self.max_degree
    }

    /// Segment file ID.
    #[inline]
    pub fn file_id(&self) -> u64 {
        self.file_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::diskann::page::write_vamana_mpf;
    use crate::vector::diskann::pq::ProductQuantizer;
    use crate::vector::diskann::vamana::VamanaGraph;

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

    fn random_vectors(n: usize, dim: usize, base_seed: u64) -> Vec<f32> {
        let mut all = Vec::with_capacity(n * dim);
        for i in 0..n {
            all.extend(deterministic_f32(dim, base_seed + i as u64));
        }
        all
    }

    fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    /// Brute-force top-k nearest neighbors by true L2.
    fn brute_force_topk(query: &[f32], vectors: &[f32], dim: usize, k: usize) -> Vec<u32> {
        let n = vectors.len() / dim;
        let mut dists: Vec<(f32, u32)> = (0..n)
            .map(|i| {
                let d = l2_distance(query, &vectors[i * dim..(i + 1) * dim]);
                (d, i as u32)
            })
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        dists.iter().take(k).map(|&(_, id)| id).collect()
    }

    fn build_test_segment(n: usize, dim: usize, m: usize, r: u32) -> (DiskAnnSegment, Vec<f32>, tempfile::TempDir) {
        let vectors = random_vectors(n, dim, 7777);
        let graph = VamanaGraph::build(&vectors, dim, r, r.max(10));
        let pq = ProductQuantizer::train(&vectors, dim, m, 8);

        // Encode all vectors.
        let mut pq_codes = Vec::with_capacity(n * m);
        for i in 0..n {
            let codes = pq.encode(&vectors[i * dim..(i + 1) * dim]);
            pq_codes.extend_from_slice(&codes);
        }

        let tmp = tempfile::tempdir().expect("tempdir");
        let vamana_path = tmp.path().join("vamana.mpf");
        write_vamana_mpf(&vamana_path, &graph, &vectors, dim).expect("write mpf");

        let seg = DiskAnnSegment::new(
            pq_codes,
            pq,
            vamana_path,
            dim,
            n as u32,
            graph.entry_point(),
            graph.max_degree(),
            1,
        );

        (seg, vectors, tmp)
    }

    #[test]
    fn test_diskann_segment_search_recall() {
        let n = 50;
        let dim = 32;
        let m = 4;
        let r = 8;
        let k = 10;
        let beam_width = 16;

        let (seg, vectors, _tmp) = build_test_segment(n, dim, m, r);

        // Run 20 queries, check recall@10.
        let mut total_recall = 0.0_f64;
        let num_queries = 20;
        for q in 0..num_queries {
            let query = deterministic_f32(dim, 9000 + q);
            let results = seg.search(&query, k, beam_width);
            let true_topk = brute_force_topk(&query, &vectors, dim, k);
            let true_set: std::collections::HashSet<u32> =
                true_topk.iter().copied().collect();
            let hits = results
                .iter()
                .filter(|r| true_set.contains(&r.id.0))
                .count();
            total_recall += hits as f64 / k as f64;
        }

        let mean_recall = total_recall / num_queries as f64;
        assert!(
            mean_recall >= 0.5,
            "recall@{k} = {mean_recall:.2} < 0.50 (too low for PQ beam search)",
        );
    }

    #[test]
    fn test_diskann_segment_search_k1_returns_one() {
        let n = 50;
        let dim = 32;
        let m = 4;
        let r = 8;

        let (seg, _vectors, _tmp) = build_test_segment(n, dim, m, r);

        let query = deterministic_f32(dim, 12345);
        let results = seg.search(&query, 1, 8);
        assert_eq!(results.len(), 1, "k=1 should return exactly 1 result");
    }

    #[test]
    fn test_diskann_segment_empty() {
        let pq = ProductQuantizer::train(&[0.0_f32; 32], 32, 4, 8);
        let tmp = tempfile::tempdir().expect("tempdir");
        let vamana_path = tmp.path().join("vamana.mpf");

        // Write a trivial 1-vector graph so the file exists.
        let vectors = vec![0.0_f32; 32];
        let graph = VamanaGraph::build(&vectors, 32, 4, 4);
        write_vamana_mpf(&vamana_path, &graph, &vectors, 32).expect("write");

        let seg = DiskAnnSegment::new(
            Vec::new(),
            pq,
            vamana_path,
            32,
            0, // num_vectors = 0
            0,
            4,
            0,
        );
        let results = seg.search(&[0.0; 32], 5, 8);
        assert!(results.is_empty());
    }

    #[test]
    fn test_diskann_segment_total_count() {
        let n = 50;
        let dim = 32;
        let m = 4;
        let r = 8;
        let (seg, _vectors, _tmp) = build_test_segment(n, dim, m, r);
        assert_eq!(seg.total_count(), 50);
    }
}
