//! DiskAnnSegment -- cold-tier vector search using PQ codes in RAM
//! and Vamana graph on disk (pread per hop).
//!
//! Search uses asymmetric PQ distance (precomputed lookup table) for
//! approximate nearest neighbor scoring. Vamana graph pages are read
//! from an `.mpf` file via `read_vamana_node_at` (one 4KB pread per
//! graph hop). No exact reranking in this version.
//!
//! On Linux, each segment optionally holds a dedicated `DiskAnnUring`
//! ring for io_uring-based batch reads with O_DIRECT (bypassing the
//! page cache). The pread fallback is always available.

use std::path::{Path, PathBuf};

use smallvec::SmallVec;

#[cfg(not(unix))]
use crate::vector::diskann::page::read_vamana_node_at;
use crate::vector::diskann::pq::ProductQuantizer;
use crate::vector::types::{SearchResult, VectorId};

/// Cold-tier segment backed by PQ codes in RAM + Vamana graph on NVMe.
///
/// On Linux, optionally holds a dedicated `DiskAnnUring` for io_uring-based
/// batch reads with O_DIRECT. Falls back to pread on non-Linux or when
/// O_DIRECT is unsupported (e.g., tmpfs in tests).
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
    /// Dedicated io_uring ring for batch O_DIRECT reads (Linux only).
    /// `None` when O_DIRECT is unsupported (tmpfs, non-ext4/xfs) or on non-Linux.
    ///
    /// Wrapped in `parking_lot::Mutex` so the type is genuinely `Send + Sync`
    /// without resorting to `unsafe impl`. The submit/complete cycle is the
    /// per-segment bottleneck (microseconds of disk I/O), so the lock cost is
    /// in the noise. This also makes the code correct under both monoio
    /// (thread-per-core) and tokio (multi-thread) runtimes.
    #[cfg(target_os = "linux")]
    uring: parking_lot::Mutex<Option<super::uring_search::DiskAnnUring>>,
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

// `DiskAnnSegment` is `Send + Sync` automatically: every field is either
// owned data or `parking_lot::Mutex`. No `unsafe impl` needed.

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

        // Try to open with O_DIRECT for io_uring beam search. Falls back
        // gracefully on filesystems that don't support O_DIRECT (e.g., tmpfs
        // used in tests) -- pread path remains available via `vamana_file`.
        #[cfg(target_os = "linux")]
        let uring = match super::uring_search::open_vamana_direct(&vamana_path) {
            Ok(fd) => match super::uring_search::DiskAnnUring::new(fd, 32) {
                Ok(u) => Some(u),
                Err(_e) => {
                    // io_uring setup failed -- close the FD and fall back.
                    // SAFETY: `fd` is a valid FD we just opened.
                    unsafe {
                        libc::close(fd);
                    }
                    None
                }
            },
            Err(_e) => {
                // O_DIRECT not supported on this filesystem -- fall back to pread.
                None
            }
        };

        Self {
            pq_codes,
            pq,
            vamana_path,
            #[cfg(unix)]
            vamana_file,
            #[cfg(target_os = "linux")]
            uring: parking_lot::Mutex::new(uring),
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
            .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "empty vamana file")
        })?;
        #[cfg(not(unix))]
        let node0 = read_vamana_node_at(&vamana_path, 0, dim)?.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "empty vamana file")
        })?;
        // Entry point is the medoid stored during build -- for from_files we
        // accept it as node 0 unless caller overrides. In practice the builder
        // writes entry_point metadata; for MVP we default to 0.
        let _ = node0;

        // Try O_DIRECT + io_uring (same pattern as new()).
        #[cfg(target_os = "linux")]
        let uring = match super::uring_search::open_vamana_direct(&vamana_path) {
            Ok(fd) => match super::uring_search::DiskAnnUring::new(fd, 32) {
                Ok(u) => Some(u),
                Err(_e) => {
                    // SAFETY: `fd` is a valid FD we just opened.
                    unsafe {
                        libc::close(fd);
                    }
                    None
                }
            },
            Err(_e) => None,
        };

        Ok(Self {
            pq_codes,
            pq,
            vamana_path,
            #[cfg(unix)]
            vamana_file,
            #[cfg(target_os = "linux")]
            uring: parking_lot::Mutex::new(uring),
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
    /// On Linux with io_uring available, dispatches to `search_uring` which
    /// batch-submits all unexpanded candidates per iteration via io_uring SQEs.
    /// Otherwise falls back to `search_pread` (one pread syscall per hop).
    ///
    /// Returns up to `k` results sorted by ascending PQ distance.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        beam_width: usize,
    ) -> SmallVec<[SearchResult; 32]> {
        #[cfg(target_os = "linux")]
        {
            if self.uring.lock().is_some() {
                return self.search_uring(query, k, beam_width);
            }
        }
        self.search_pread(query, k, beam_width)
    }

    /// Pread-based beam search (one syscall per graph hop).
    ///
    /// This is the portable fallback used on non-Linux platforms and when
    /// O_DIRECT / io_uring is unavailable (e.g., tmpfs in tests).
    pub fn search_pread(
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
            let ep_dist = self
                .pq
                .asymmetric_distance(&adt, &self.pq_codes[ep * m..(ep + 1) * m]);
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
                &self.vamana_file,
                node,
                self.dim,
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
                let d = self
                    .pq
                    .asymmetric_distance(&adt, &self.pq_codes[nbr_idx * m..(nbr_idx + 1) * m]);
                candidates.push((d, nbr));
            }

            // Keep only best `beam_width` candidates.
            candidates.sort_unstable_by(|a, b| {
                a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
            });
            candidates.truncate(beam_width);
        }

        // Return top-k.
        candidates
            .sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);

        let mut results = SmallVec::with_capacity(k);
        for &(dist, node_id) in &candidates {
            results.push(SearchResult::new(dist, VectorId(node_id)));
        }
        results
    }

    /// io_uring batch beam search: submits all unexpanded candidates per
    /// iteration in a single `submit_and_wait()`, then processes CQEs.
    ///
    /// With beam_width W, this reduces from ~W pread syscalls per iteration
    /// to 1 submit_and_wait. On NVMe, the kernel can issue all reads in
    /// parallel via the NVMe submission queue.
    #[cfg(target_os = "linux")]
    fn search_uring(
        &self,
        query: &[f32],
        k: usize,
        beam_width: usize,
    ) -> SmallVec<[SearchResult; 32]> {
        use crate::persistence::page::PAGE_4K;
        use crate::vector::diskann::page::read_vamana_node;

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
            let ep_dist = self
                .pq
                .asymmetric_distance(&adt, &self.pq_codes[ep * m..(ep + 1) * m]);
            candidates.push((ep_dist, self.entry_point));
            visited[ep] = true;
        }

        // Batch beam search loop: expand ALL unexpanded candidates per iteration.
        loop {
            // Collect all unexpanded candidates (up to beam_width).
            let mut to_expand: SmallVec<[u32; 32]> = SmallVec::new();
            for &(_, node) in &candidates {
                if !expanded[node as usize] {
                    to_expand.push(node);
                }
            }
            if to_expand.is_empty() {
                break;
            }

            // Mark all as expanded before I/O.
            for &node in &to_expand {
                expanded[node as usize] = true;
            }

            // BATCH READ: submit all node reads via io_uring (BATCH-SQE-SUBMIT).
            // The ring is owned by this segment; the lock is per-segment and
            // serializes concurrent searches against the same ring.
            let mut guard = self.uring.lock();
            let uring = match guard.as_mut() {
                Some(u) => u,
                None => break, // io_uring not initialized -- caller should use search_pread
            };
            let submitted = match uring.submit_reads(&to_expand) {
                Ok(count) => count,
                Err(_) => {
                    // io_uring submission failed -- fall back to pread for
                    // remaining iterations by clearing uring and recursing
                    // into search_pread. This is a rare error path.
                    break;
                }
            };

            if submitted == 0 {
                break;
            }

            // COLLECT COMPLETIONS (CQE-COMPLETION).
            let completions = uring.collect_completions(submitted);

            // Parse each completed read buffer into VamanaNode.
            for &(buf_idx, result) in &completions {
                if (result as usize) < PAGE_4K {
                    // Short read or error -- skip this node.
                    uring.reclaim_buf(buf_idx);
                    continue;
                }
                let buf = uring.read_buf(buf_idx);
                // The buffer is exactly PAGE_4K bytes from the aligned pool.
                let page: &[u8; PAGE_4K] = match buf.try_into() {
                    Ok(p) => p,
                    Err(_) => {
                        uring.reclaim_buf(buf_idx);
                        continue;
                    }
                };
                if let Some(vnode) = read_vamana_node(page, self.dim) {
                    // Score each unvisited neighbor using PQ distance.
                    for &nbr in &vnode.neighbors {
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
                }
                uring.reclaim_buf(buf_idx);
            }

            // Keep only best `beam_width` candidates.
            candidates.sort_unstable_by(|a, b| {
                a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
            });
            candidates.truncate(beam_width);
        }

        // Return top-k.
        candidates
            .sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
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
                    &self.vamana_file,
                    idx,
                    self.dim,
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

    /// Whether the io_uring ring was successfully initialized for this segment.
    ///
    /// Returns `false` if O_DIRECT was not available (e.g., tmpfs) or io_uring
    /// setup failed. The pread fallback is always available regardless.
    #[cfg(target_os = "linux")]
    #[inline]
    pub fn has_uring(&self) -> bool {
        self.uring.lock().is_some()
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

    fn build_test_segment(
        n: usize,
        dim: usize,
        m: usize,
        r: u32,
    ) -> (DiskAnnSegment, Vec<f32>, tempfile::TempDir) {
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
            let true_set: std::collections::HashSet<u32> = true_topk.iter().copied().collect();
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

    /// Explicitly test the pread path (even on Linux where uring may be
    /// available) to verify the portable fallback works correctly.
    #[test]
    fn test_diskann_search_pread_recall() {
        let n = 50;
        let dim = 32;
        let m = 4;
        let r = 8;
        let k = 10;
        let beam_width = 16;

        let (seg, vectors, _tmp) = build_test_segment(n, dim, m, r);

        // Run 20 queries via search_pread, check recall@10.
        let mut total_recall = 0.0_f64;
        let num_queries = 20;
        for q in 0..num_queries {
            let query = deterministic_f32(dim, 9000 + q);
            let results = seg.search_pread(&query, k, beam_width);
            let true_topk = brute_force_topk(&query, &vectors, dim, k);
            let true_set: std::collections::HashSet<u32> = true_topk.iter().copied().collect();
            let hits = results
                .iter()
                .filter(|r| true_set.contains(&r.id.0))
                .count();
            total_recall += hits as f64 / k as f64;
        }

        let mean_recall = total_recall / num_queries as f64;
        assert!(
            mean_recall >= 0.5,
            "pread recall@{k} = {mean_recall:.2} < 0.50 (too low)",
        );
    }

    /// Test io_uring beam search path on Linux.
    ///
    /// Builds a segment on a real filesystem (not tmpfs) so O_DIRECT succeeds.
    /// If O_DIRECT is unavailable (e.g., tmpfs in containers), the segment's
    /// uring field will be None and the test skips gracefully.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_diskann_search_uring_recall() {
        let n = 50;
        let dim = 32;
        let m = 4;
        let r = 8;
        let k = 10;
        let beam_width = 16;

        let vectors = random_vectors(n, dim, 7777);
        let graph = VamanaGraph::build(&vectors, dim, r, r.max(10));
        let pq = ProductQuantizer::train(&vectors, dim, m, 8);

        let mut pq_codes = Vec::with_capacity(n * m);
        for i in 0..n {
            let codes = pq.encode(&vectors[i * dim..(i + 1) * dim]);
            pq_codes.extend_from_slice(&codes);
        }

        // Write to /tmp which is typically ext4 (not tmpfs) on most Linux setups.
        let dir = std::path::PathBuf::from("/tmp/moon_test_uring_beam");
        let _ = std::fs::create_dir_all(&dir);
        let vamana_path = dir.join("vamana.mpf");
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

        // If uring is None (tmpfs / O_DIRECT unsupported), skip gracefully.
        if !seg.has_uring() {
            eprintln!("SKIP: io_uring not available (O_DIRECT unsupported on this FS)");
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }

        // Run 20 queries via search_uring, check recall@10.
        let mut total_recall = 0.0_f64;
        let num_queries = 20;
        for q in 0..num_queries {
            let query = deterministic_f32(dim, 9000 + q);
            let results = seg.search_uring(&query, k, beam_width);
            let true_topk = brute_force_topk(&query, &vectors, dim, k);
            let true_set: std::collections::HashSet<u32> = true_topk.iter().copied().collect();
            let hits = results
                .iter()
                .filter(|r| true_set.contains(&r.id.0))
                .count();
            total_recall += hits as f64 / k as f64;
        }

        let mean_recall = total_recall / num_queries as f64;
        assert!(
            mean_recall >= 0.5,
            "uring recall@{k} = {mean_recall:.2} < 0.50 (too low for io_uring beam search)",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
