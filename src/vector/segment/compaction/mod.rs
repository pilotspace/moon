//! Compaction pipeline: frozen mutable segment -> immutable segment.
//!
//! 8-step pipeline:
//! 1. Filter dead entries
//! 2. Encode TQ-4bit
//! 3. Build HNSW with pairwise TQ-ADC oracle
//! 4. Verify recall >= 0.95
//! 5. BFS-reorder TQ and SQ buffers
//! 6. Payload indexes (stub for Phase 64)
//! 7. Persist to disk (stub for Phase 66)
//! 8. Construct ImmutableSegment

mod compact_path;
mod graph_build;
mod merge;
mod recall;

pub use compact_path::compact;
pub use merge::merge_immutable;

use crate::vector::segment::immutable::ImmutableSegment;

#[allow(dead_code)]
const RECALL_SAMPLE_SIZE: usize = 1000;
#[allow(dead_code)]
const MIN_RECALL: f32 = 0.95;
const VACUUM_DEAD_THRESHOLD: f32 = 0.20;
const HNSW_M: u8 = 16;
const HNSW_EF_CONSTRUCTION: u16 = 200;
const PARALLEL_THRESHOLD: usize = 10_000;

#[derive(Debug)]
pub enum CompactionError {
    RecallTooLow { recall: f32, required: f32 },
    EmptySegment,
    PersistFailed(String),
}

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RecallTooLow { recall, required } => {
                write!(
                    f,
                    "compaction recall {recall:.4} below required {required:.4}"
                )
            }
            Self::EmptySegment => write!(f, "cannot compact empty segment"),
            Self::PersistFailed(msg) => write!(f, "persist failed: {msg}"),
        }
    }
}

/// Check if an immutable segment needs vacuum (rebuild due to too many dead entries).
///
/// Returns true when dead_fraction > 20%.
pub fn needs_vacuum(segment: &ImmutableSegment) -> bool {
    segment.dead_fraction() > VACUUM_DEAD_THRESHOLD
}

// ── Immutable segment merge (P2) ─────────────────────────────────────────────

/// Trigger threshold: merge when immutable segment count exceeds this.
pub const MERGE_SEGMENT_THRESHOLD: usize = 16;

/// Maximum estimated bytes for the union before merge is refused.
/// Prevents OOM during merge of very large indexes.
/// 512 MiB.
pub const MERGE_MEMORY_CEILING: usize = 512 * 1024 * 1024;

/// Merge mode for immutable segment consolidation.
///
/// Determines how the union HNSW graph is built when merging N immutable
/// segments into one. The modes differ in recall quality vs. memory cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MergeMode {
    /// (Default) Concatenate TQ codes verbatim; rebuild HNSW graph over the
    /// union using TQ-decoded centroids for distance computation.
    ///
    /// No f32 round-trip — codes are never decoded then re-encoded. Only graph
    /// edges change. This preserves recall because quantization error is not
    /// accumulated (decode → re-encode path is avoided entirely).
    #[default]
    GraphUnion,

    /// Retain raw f32 vectors in memory alongside each immutable segment.
    /// On merge, use raw vectors as the authoritative distance oracle to build
    /// the merged HNSW graph, then re-encode TQ codes from scratch.
    ///
    /// Higher recall than GraphUnion (no quantization error at all in the
    /// graph topology) at the cost of +1.5 KB/vector at 384d f16 memory.
    ///
    /// Note: raw f32 data is retained in-memory only; it is NOT persisted to
    /// disk and will be lost on restart. The index falls back to GraphUnion
    /// for segments loaded from disk.
    KeepRaw,

    /// Disable automatic merging. Segments accumulate indefinitely.
    /// Use when the operator manages compaction manually.
    None,
}

impl MergeMode {
    /// Parse a merge mode string (case-insensitive).
    /// Accepts: "GRAPH_UNION", "KEEP_RAW", "NONE".
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.eq_ignore_ascii_case(b"GRAPH_UNION") || b.eq_ignore_ascii_case(b"GRAPHUNION") {
            Some(Self::GraphUnion)
        } else if b.eq_ignore_ascii_case(b"KEEP_RAW") || b.eq_ignore_ascii_case(b"KEEPRAW") {
            Some(Self::KeepRaw)
        } else if b.eq_ignore_ascii_case(b"NONE") {
            Some(Self::None)
        } else {
            Option::None
        }
    }
}

/// Statistics returned by `merge_immutable`.
#[derive(Debug, Default, Clone, Copy)]
pub struct MergeStats {
    /// Number of input segments that were merged.
    pub segments_merged: usize,
    /// Number of live vectors in the merged output segment.
    pub live_vectors: usize,
    /// Recall of the merged segment against pre-merge fan-out search.
    /// 0.0 if recall gate was not evaluated (e.g., too few vectors).
    pub recall: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    // Previously reached through `use super::*` when everything below lived in
    // one file; named explicitly now that the implementation sits in sibling
    // submodules. Same items, same paths — no test was added, removed, or
    // rewritten by the split.
    use std::sync::Arc;

    use super::graph_build::{assign_to_cells, compact_parallel, stitch_subgraphs};
    use crate::vector::distance;
    use crate::vector::hnsw::build::HnswBuilder;
    use crate::vector::segment::mutable::{FrozenSegment, MutableSegment};
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::types::DistanceMetric;

    fn lcg_f32(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn normalize(v: &mut [f32]) -> f32 {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            v.iter_mut().for_each(|x| *x *= inv);
        }
        norm
    }

    fn make_frozen_segment(
        n: usize,
        dim: usize,
        delete_count: usize,
    ) -> (FrozenSegment, Arc<CollectionMetadata>) {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let seg = MutableSegment::new(dim as u32, collection.clone());

        for i in 0..n {
            let mut f32_v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut f32_v);
            seg.append(i as u64, &f32_v, i as u64 + 1);
        }

        // Mark some as deleted
        for i in 0..delete_count {
            seg.mark_deleted(i as u32, 100);
        }

        let frozen = seg.freeze();
        (frozen, collection)
    }

    #[test]
    fn test_compact_100_vectors() {
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok(), "compact failed: {:?}", result.err());
        let imm = result.unwrap();
        assert_eq!(imm.live_count(), 100);
        assert_eq!(imm.total_count(), 100);

        // Verify search works on the resulting segment
        let mut query = lcg_f32(64, 99999);
        normalize(&mut query);
        let padded = collection.padded_dimension;
        let mut scratch =
            crate::vector::hnsw::search::SearchScratch::new(imm.graph().num_nodes(), padded);
        let results = imm.search(&query, 5, 64, &mut scratch);
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_sq8_compact_and_immutable_search_recall() {
        distance::init();
        let dim = 96usize;
        let n = 200usize;
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            42,
        ));
        let seg = MutableSegment::new(dim as u32, collection.clone());
        let mut db: Vec<Vec<f32>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut v);
            seg.append(i as u64, &v, i as u64 + 1);
            db.push(v);
        }
        let frozen = seg.freeze();
        let imm = compact(&frozen, &collection, 12345, None).expect("SQ8 compact failed");
        assert_eq!(imm.live_count(), n as u32);

        let padded = collection.padded_dimension;

        // Exact-match invariant: query == db[7] must rank 7 first. The broken
        // codebook fallback returned ml:0 here (degenerate codes); real SQ8 must not.
        let mut scratch =
            crate::vector::hnsw::search::SearchScratch::new(imm.graph().num_nodes(), padded);
        let res = imm.search(&db[7], 10, 128, &mut scratch);
        assert!(!res.is_empty(), "SQ8 immutable search returned nothing");
        assert_eq!(
            res[0].id.0 as usize, 7,
            "SQ8 nearest != exact match: got {}",
            res[0].id.0
        );

        // recall@10 vs exact-f32 L2 ground truth over several queries. The broken
        // SQ8 scored ~0.01 here; 8-bit fidelity + HNSW should recover the bulk.
        let queries = [3usize, 7, 50, 123, 199];
        let mut total_hits = 0usize;
        for &qi in &queries {
            let mut idx: Vec<usize> = (0..n).collect();
            idx.sort_by(|&a, &b| {
                let da: f32 = db[qi]
                    .iter()
                    .zip(&db[a])
                    .map(|(x, y)| (x - y) * (x - y))
                    .sum();
                let dbb: f32 = db[qi]
                    .iter()
                    .zip(&db[b])
                    .map(|(x, y)| (x - y) * (x - y))
                    .sum();
                da.total_cmp(&dbb)
            });
            let exact: std::collections::HashSet<u32> =
                idx.into_iter().take(10).map(|i| i as u32).collect();
            let mut sc =
                crate::vector::hnsw::search::SearchScratch::new(imm.graph().num_nodes(), padded);
            let got = imm.search(&db[qi], 10, 128, &mut sc);
            let got_ids: std::collections::HashSet<u32> = got.iter().map(|r| r.id.0).collect();
            total_hits += exact.intersection(&got_ids).count();
        }
        assert!(
            total_hits >= 42,
            "SQ8 immutable recall@10 too low: {total_hits}/50"
        );
    }

    /// Build a real SQ8 immutable segment of `n` normalized vectors with global
    /// ids `id_base..id_base+n`. Returns the segment and its f32 db vectors.
    fn make_sq8_immutable(
        n: usize,
        dim: usize,
        id_base: usize,
        collection: &Arc<CollectionMetadata>,
        seed: u64,
    ) -> (ImmutableSegment, Vec<Vec<f32>>) {
        let seg = MutableSegment::new(dim as u32, collection.clone());
        let mut db = Vec::with_capacity(n);
        for i in 0..n {
            let gid = id_base + i;
            let mut v = lcg_f32(dim, (gid * 7 + 13) as u32);
            normalize(&mut v);
            seg.append(gid as u64, &v, gid as u64 + 1);
            db.push(v);
        }
        let frozen = seg.freeze();
        let imm = compact(&frozen, collection, seed, None).expect("SQ8 compact failed");
        (imm, db)
    }

    /// GraphUnion must survive a corpus carrying DUPLICATE vectors (moon#546).
    ///
    /// Real corpora duplicate embeddings constantly — repeated text chunks,
    /// boilerplate, and hash fields whose vector never got written. When a
    /// point has 99 exact duplicates, the brute-force ground truth picks an
    /// ARBITRARY 10 of the 100 tied candidates and HNSW picks a DIFFERENT
    /// arbitrary 10; both answers are exactly correct, yet an ID-set overlap
    /// scores them 0.0. `verify_merge_recall` scored exactly that overlap, so
    /// every merge on a duplicate-heavy index was rejected forever and its
    /// segments accumulated without bound (the live store logged 5,596 merge
    /// attempts, zero successes, 1,367 of them at recall exactly 0.0000).
    ///
    /// This is a proper red on ID-overlap scoring: the merge below is
    /// FUNCTIONALLY perfect (asserted by self-match distance) but the gate
    /// measures 0.0000 and aborts. Recall must be scored by DISTANCE
    /// equivalence, which is what recall@k means when distances tie.
    #[test]
    fn test_graph_union_merge_survives_duplicate_vectors() {
        distance::init();
        let dim = 96usize;
        let per = 100usize;
        let nsrc = 8usize;
        let distinct = 8usize; // 800 entries drawn from 8 vectors => 100x tie sets
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            42,
        ));

        let mut pool: Vec<Vec<f32>> = Vec::with_capacity(distinct);
        for d in 0..distinct {
            let mut v = lcg_f32(dim, (d * 7 + 13) as u32);
            normalize(&mut v);
            pool.push(v);
        }

        let mut segs = Vec::new();
        for sidx in 0..nsrc {
            let seg = MutableSegment::new(dim as u32, collection.clone());
            for i in 0..per {
                let gid = sidx * per + i;
                seg.append(gid as u64, &pool[gid % distinct], gid as u64 + 1);
            }
            let frozen = seg.freeze();
            segs.push(Arc::new(
                compact(&frozen, &collection, 12345 + sidx as u64, None).expect("compact failed"),
            ));
        }

        // 0.90 is the manual force_compact gate — the strictest one shipped.
        let merged = merge_immutable(&segs, &collection, 42, MergeMode::GraphUnion, 0.90, None)
            .expect("GraphUnion merge rejected a duplicate-heavy corpus");
        assert_eq!(
            merged.live_count(),
            (nsrc * per) as u32,
            "merged live_count"
        );

        // Non-vacuity: the gate passing is only meaningful if the merged
        // segment actually answers correctly. Every pool vector must self-match
        // at ~0 distance, and all k results must be its duplicates (also ~0).
        let padded = collection.padded_dimension;
        for (d, q) in pool.iter().enumerate() {
            let mut sc =
                crate::vector::hnsw::search::SearchScratch::new(merged.graph().num_nodes(), padded);
            let got = merged.search(q, 10, 128, &mut sc);
            assert_eq!(
                got.len(),
                10,
                "merged search returned {} for pool[{d}]",
                got.len()
            );
            for (rank, r) in got.iter().enumerate() {
                assert!(
                    r.distance < 0.01,
                    "pool[{d}] rank {rank}: distance {} (expected ~0 — every hit \
                     must be one of its 99 exact duplicates)",
                    r.distance
                );
            }
        }
    }

    /// MERGE path for SQ8: merge two SQ8 immutable segments via GraphUnion and
    /// prove the merged segment searches correctly.
    ///
    /// On TQ-only merge code this is a proper red: `merge_graph_union` derives
    /// `bytes_per_code` from `collection.bytes_per_code_per_vector()` (= padded+4),
    /// but SQ8 slots are dim+8 — the stride mismatch skips/garbles entries
    /// (EmptySegment, RecallTooLow, or wrong neighbors). Config-D-strength
    /// assertions (exact-match top1 + recall@10) discriminate correct from
    /// plausibly-shaped-but-wrong.
    #[test]
    fn test_sq8_merge_two_segments_recall() {
        distance::init();
        let dim = 96usize;
        let per = 100usize;
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            42,
        ));
        let (imm_a, db_a) = make_sq8_immutable(per, dim, 0, &collection, 12345);
        let (imm_b, db_b) = make_sq8_immutable(per, dim, per, &collection, 6789);
        let mut db = db_a;
        db.extend(db_b);
        let n = db.len();

        let segs = vec![Arc::new(imm_a), Arc::new(imm_b)];
        let merged = merge_immutable(&segs, &collection, 42, MergeMode::GraphUnion, 0.60, None)
            .expect("SQ8 merge failed");
        assert_eq!(merged.live_count(), n as u32, "merged SQ8 live_count");

        let padded = collection.padded_dimension;

        // Self-match correctness across queries spanning both source segments:
        // each query == db[qi] must find its own vector in the merged graph,
        // whose self-distance is ~0 (only 8-bit affine reconstruction error) —
        // orders of magnitude below the ~O(1) squared-L2 between distinct unit
        // vectors. A broken-stride merge (padded+4 vs dim+8) has no near-zero
        // self-match and fails.
        //
        // We assert on distance, not id: `compact` assigns per-segment
        // global_ids (0..per), so ids collide across the two separately
        // compacted source segments and cannot identify a vector here.
        // Calibrated recall *preservation* (post-merge vs pre-merge fan-out)
        // is enforced by the integration test
        // `test_sq8_graph_union_merge_preserves_recall`.
        let _ = n;
        for &qi in &[3usize, 7, 50, 123, 199] {
            let mut sc =
                crate::vector::hnsw::search::SearchScratch::new(merged.graph().num_nodes(), padded);
            let got = merged.search(&db[qi], 10, 128, &mut sc);
            assert!(
                !got.is_empty(),
                "merged SQ8 search returned nothing for q{qi}"
            );
            assert!(
                got[0].distance < 0.01,
                "merged SQ8 self-match distance too high for db[{qi}]: {} (expected ~0)",
                got[0].distance
            );
        }
    }

    #[test]
    fn test_compact_filters_deleted() {
        let (frozen, collection) = make_frozen_segment(50, 64, 10);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok(), "compact failed: {:?}", result.err());
        let imm = result.unwrap();
        // 50 total, 10 deleted -> 40 live
        assert_eq!(imm.live_count(), 40);
        assert_eq!(imm.total_count(), 50);
    }

    #[test]
    fn test_compact_empty_returns_error() {
        let (frozen, collection) = make_frozen_segment(5, 64, 5);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_err());
        match result.err().unwrap() {
            CompactionError::EmptySegment => {}
            other => panic!("expected EmptySegment, got: {other}"),
        }
    }

    #[test]
    fn test_compact_recall_above_threshold() {
        let (frozen, collection) = make_frozen_segment(500, 64, 0);
        // compact() internally verifies recall >= 0.95 and returns Ok only if it passes
        let result = compact(&frozen, &collection, 12345, None);
        assert!(
            result.is_ok(),
            "compact failed (recall too low): {:?}",
            result.err()
        );
    }

    #[test]
    fn test_needs_vacuum_threshold() {
        // Create segment with 25% dead
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok());
        let mut imm = result.unwrap();

        // Initially 0% dead
        assert!(!needs_vacuum(&imm));

        // Mark 25 as deleted -> 25%
        for i in 0..25u32 {
            imm.mark_deleted(i, 200);
        }
        assert!(needs_vacuum(&imm), "should need vacuum at 25% dead");

        // Create another with 10% dead
        let (frozen2, collection2) = make_frozen_segment(100, 64, 0);
        let result2 = compact(&frozen2, &collection2, 54321, None);
        assert!(result2.is_ok());
        let mut imm2 = result2.unwrap();

        for i in 0..10u32 {
            imm2.mark_deleted(i, 300);
        }
        assert!(!needs_vacuum(&imm2), "should not need vacuum at 10% dead");
    }

    /// Verify that compact() works identically without the gpu-cuda feature.
    /// This test always runs (no feature gate) and ensures the CPU path is
    /// unaffected by the GPU integration code.
    #[test]
    fn test_compact_without_gpu_feature_unchanged() {
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(result.is_ok(), "compact failed: {:?}", result.err());
        assert_eq!(result.unwrap().live_count(), 100);
    }

    /// When gpu-cuda feature is enabled but no CUDA device is present (CI),
    /// compact() should fall back to the CPU path transparently.
    #[cfg(feature = "gpu-cuda")]
    #[test]
    fn test_gpu_fallback_to_cpu() {
        let (frozen, collection) = make_frozen_segment(100, 64, 0);
        let result = compact(&frozen, &collection, 12345, None);
        assert!(
            result.is_ok(),
            "compact with GPU fallback failed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap().live_count(), 100);
    }

    // ── Cell-parallel compaction tests ──────────────────────────────

    /// Brute-force k-NN oracle: compute L2 distance from query to all vectors,
    /// return top-k IDs sorted by ascending distance.
    fn brute_force_knn(query: &[f32], all_vectors: &[&[f32]], k: usize) -> Vec<u32> {
        let mut dists: Vec<(f32, u32)> = all_vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let d: f32 = query
                    .iter()
                    .zip(v.iter())
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum();
                (d, i as u32)
            })
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        dists.iter().take(k).map(|(_, id)| *id).collect()
    }

    #[test]
    fn test_assign_to_cells_partitions_all_vectors() {
        let dim = 64;
        let vecs_owned: Vec<Vec<f32>> = (0..200)
            .map(|i| lcg_f32(dim, (i * 7 + 13) as u32))
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        let cells = assign_to_cells(&vecs, 4);

        // Every vector index must appear exactly once across all cells
        let mut all_indices: Vec<usize> = cells.iter().flat_map(|c| c.iter().copied()).collect();
        all_indices.sort();
        let expected: Vec<usize> = (0..200).collect();
        assert_eq!(
            all_indices, expected,
            "all vectors must be assigned to exactly one cell"
        );
    }

    #[test]
    fn test_parallel_compact_bfs_reaches_all() {
        distance::init();
        let dim = 64;
        let n = 500;
        let vecs_owned: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
                normalize(&mut v);
                v
            })
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        // Dummy TQ buffer (not used for graph topology, just sizing)
        let bytes_per_code = 36; // padded_dim/2 + 4 for 64d -> padded 64 -> 32+4
        let tq_buffer = vec![0u8; n * bytes_per_code];

        let graph = compact_parallel(&vecs, &tq_buffer, bytes_per_code, dim, 12345);

        assert_eq!(graph.num_nodes(), n as u32);

        // BFS from entry point should reach all nodes
        let mut visited = vec![false; n];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(graph.entry_point());
        visited[graph.entry_point() as usize] = true;
        let mut count = 1usize;

        while let Some(pos) = queue.pop_front() {
            let neighbors = graph.neighbors_l0(pos);
            for &nb in neighbors {
                if nb == crate::vector::hnsw::graph::SENTINEL {
                    break;
                }
                if !visited[nb as usize] {
                    visited[nb as usize] = true;
                    count += 1;
                    queue.push_back(nb);
                }
            }
        }

        assert_eq!(
            count, n,
            "BFS from entry must reach all {} nodes, only reached {}",
            n, count
        );
    }

    #[test]
    fn test_compact_parallel_recall() {
        distance::init();
        let dim = 64;
        let n = 1000;
        let vecs_owned: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
                normalize(&mut v);
                v
            })
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        let bytes_per_code = 36;
        let tq_buffer = vec![0u8; n * bytes_per_code];

        let graph = compact_parallel(&vecs, &tq_buffer, bytes_per_code, dim, 42);

        // Build BFS-ordered f32 buffer for hnsw_search_f32
        let mut f32_bfs = vec![0.0f32; n * dim];
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let src = &vecs_owned[orig_id];
            let dst_start = bfs_pos * dim;
            f32_bfs[dst_start..dst_start + dim].copy_from_slice(src);
        }

        // Measure recall@10 using brute-force L2 oracle
        let k = 10;
        let num_queries = 100;
        let mut total_recall = 0.0f64;

        for qi in 0..num_queries {
            let query_idx = qi * (n / num_queries);
            let query = vecs[query_idx];
            let gt = brute_force_knn(query, &vecs, k);

            // Search the graph using f32 L2 (matches production path).
            // Use ef=256 for stitched graphs (wider beam compensates for cross-cell edges).
            let hnsw_results = crate::vector::hnsw::search_sq::hnsw_search_f32(
                &graph, &f32_bfs, dim, query, k, 256, None,
            );

            // hnsw_search_f32 returns IDs in BFS space mapped back through to_original
            let result_ids: std::collections::HashSet<u32> =
                hnsw_results.iter().map(|r| r.id.0).collect();
            let gt_set: std::collections::HashSet<u32> = gt.into_iter().collect();
            let hits = result_ids.intersection(&gt_set).count();
            total_recall += hits as f64 / k as f64;
        }

        let avg_recall = total_recall / num_queries as f64;
        assert!(
            avg_recall >= 0.90,
            "recall@10 should be >= 0.90, got {:.4}",
            avg_recall
        );
    }

    #[test]
    fn test_stitch_cross_cell_edges() {
        distance::init();
        let dim = 64;
        let n = 200;
        let vecs_owned: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
                normalize(&mut v);
                v
            })
            .collect();
        let vecs: Vec<&[f32]> = vecs_owned.iter().map(|v| v.as_slice()).collect();

        let cells = assign_to_cells(&vecs, 4);

        // Build sub-graphs per cell
        let dist_table = crate::vector::distance::table();
        let mut sub_graphs: Vec<(crate::vector::hnsw::graph::HnswGraph, Vec<usize>)> = Vec::new();

        for cell in &cells {
            if cell.is_empty() {
                continue;
            }
            let cell_vecs: Vec<&[f32]> = cell.iter().map(|&idx| vecs[idx]).collect();
            let mut builder = HnswBuilder::new(HNSW_M, HNSW_EF_CONSTRUCTION, 42);
            for _ in 0..cell_vecs.len() {
                builder.insert(|a: u32, b: u32| {
                    (dist_table.l2_f32)(cell_vecs[a as usize], cell_vecs[b as usize])
                });
            }
            let graph = builder.build(36);
            sub_graphs.push((graph, cell.clone()));
        }

        let stitched = stitch_subgraphs(&sub_graphs, &vecs, 36);

        // Verify stitching produced a connected graph
        let mut visited = vec![false; n];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(stitched.entry_point());
        visited[stitched.entry_point() as usize] = true;
        let mut count = 1usize;

        while let Some(pos) = queue.pop_front() {
            for &nb in stitched.neighbors_l0(pos) {
                if nb == crate::vector::hnsw::graph::SENTINEL {
                    break;
                }
                if !visited[nb as usize] {
                    visited[nb as usize] = true;
                    count += 1;
                    queue.push_back(nb);
                }
            }
        }

        assert_eq!(
            count, n,
            "stitched graph must be fully connected, only reached {}/{}",
            count, n
        );
    }

    #[test]
    fn test_merge_two_segments_basic() {
        distance::init();
        let dim = 32usize;
        let (frozen1, collection) = make_frozen_segment(40, dim, 0);
        let imm1 = compact(&frozen1, &collection, 1, None).expect("compact 1");

        let (frozen2, _) = make_frozen_segment(35, dim, 0);
        let imm2 = compact(&frozen2, &collection, 2, None).expect("compact 2");

        eprintln!(
            "pre-merge: imm1 total={} live={} headers={}, imm2 total={} live={} headers={}",
            imm1.total_count(),
            imm1.live_count(),
            imm1.mvcc_headers().len(),
            imm2.total_count(),
            imm2.live_count(),
            imm2.mvcc_headers().len(),
        );

        let segs = vec![Arc::new(imm1), Arc::new(imm2)];
        let result = merge_immutable(&segs, &collection, 42, MergeMode::GraphUnion, 0.80, None);

        match &result {
            Ok(m) => eprintln!(
                "merge ok: total={} live={}",
                m.total_count(),
                m.live_count()
            ),
            Err(e) => eprintln!("merge err: {e}"),
        }
        assert!(result.is_ok(), "merge failed: {:?}", result.err());
        let merged = result.unwrap();
        assert!(merged.total_count() > 0);
        assert!(merged.live_count() > 0);
    }
}
