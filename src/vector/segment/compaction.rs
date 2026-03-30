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

use std::path::Path;
use std::sync::Arc;

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::build::HnswBuilder;
use crate::vector::hnsw::search::{hnsw_search, SearchScratch};
use crate::vector::persistence::segment_io;
use crate::vector::turbo_quant::collection::CollectionMetadata;
use crate::vector::turbo_quant::encoder::encode_tq_mse;
use crate::vector::turbo_quant::fwht;

use super::immutable::{ImmutableSegment, MvccHeader};
use super::mutable::FrozenSegment;

const RECALL_SAMPLE_SIZE: usize = 1000;
const MIN_RECALL: f32 = 0.95;
const VACUUM_DEAD_THRESHOLD: f32 = 0.20;
const HNSW_M: u8 = 16;
const HNSW_EF_CONSTRUCTION: u16 = 200;

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
                write!(f, "compaction recall {recall:.4} below required {required:.4}")
            }
            Self::EmptySegment => write!(f, "cannot compact empty segment"),
            Self::PersistFailed(msg) => write!(f, "persist failed: {msg}"),
        }
    }
}

/// Convert a frozen mutable segment into an optimized immutable segment.
///
/// Steps: filter dead -> encode TQ -> build HNSW -> verify recall -> BFS reorder ->
/// persist (optional) -> construct ImmutableSegment.
///
/// `persist`: when `Some((dir, segment_id))`, writes the segment to disk after construction.
///
/// Returns `Err(CompactionError::RecallTooLow)` if recall < 0.95.
/// Returns `Err(CompactionError::EmptySegment)` if all entries are deleted.
pub fn compact(
    frozen: &FrozenSegment,
    collection: &Arc<CollectionMetadata>,
    seed: u64,
    persist: Option<(&Path, u64)>,
) -> Result<ImmutableSegment, CompactionError> {
    let dim = frozen.dimension as usize;
    let padded = collection.padded_dimension as usize;
    let signs = collection.fwht_sign_flips.as_slice();

    // ── Step 1: Filter dead entries ──────────────────────────────────
    let mut live_entries = Vec::new();
    let mut live_f32_vecs: Vec<f32> = Vec::new();
    let mut live_sq_vecs: Vec<i8> = Vec::new();

    for entry in &frozen.entries {
        if entry.delete_lsn != 0 {
            continue;
        }
        let offset = entry.internal_id as usize * dim;
        live_f32_vecs.extend_from_slice(&frozen.vectors_f32[offset..offset + dim]);
        live_sq_vecs.extend_from_slice(&frozen.vectors_sq[offset..offset + dim]);
        live_entries.push(entry);
    }

    let n = live_entries.len();
    if n == 0 {
        return Err(CompactionError::EmptySegment);
    }

    // ── Step 2: Encode TQ ────────────────────────────────────────────
    let bytes_per_code = padded / 2 + 4; // nibble-packed codes + 4 bytes norm
    let mut tq_codes_raw: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut tq_norms: Vec<f32> = Vec::with_capacity(n);
    let mut work_buf = vec![0.0f32; padded];

    for i in 0..n {
        let vec_slice = &live_f32_vecs[i * dim..(i + 1) * dim];
        let code = encode_tq_mse(vec_slice, signs, &mut work_buf);
        tq_codes_raw.push(code.codes);
        tq_norms.push(code.norm);
    }

    // Build flat TQ buffer in insertion order (codes + norm per entry)
    let mut tq_buffer_orig: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
    for i in 0..n {
        tq_buffer_orig.extend_from_slice(&tq_codes_raw[i]);
        tq_buffer_orig.extend_from_slice(&tq_norms[i].to_le_bytes());
    }

    // ── Step 3: Build HNSW ───────────────────────────────────────────

    // --- GPU HNSW build path (feature-gated) ---
    // When gpu-cuda is enabled and the batch is large enough, attempt a
    // GPU-accelerated HNSW construction via CAGRA. On any failure the GPU
    // path returns None and we fall through to the CPU builder below.
    #[cfg(feature = "gpu-cuda")]
    let gpu_graph: Option<crate::vector::hnsw::graph::HnswGraph> = {
        use crate::vector::gpu::{try_gpu_build_hnsw, MIN_VECTORS_FOR_GPU};
        if n >= MIN_VECTORS_FOR_GPU {
            try_gpu_build_hnsw(&live_f32_vecs, dim, HNSW_M, HNSW_EF_CONSTRUCTION, seed)
        } else {
            None
        }
    };

    // Determine whether we need the CPU path. When GPU succeeded we skip
    // the expensive all_rotated precomputation and HnswBuilder entirely.
    #[cfg(feature = "gpu-cuda")]
    let need_cpu_build = gpu_graph.is_none();
    #[cfg(not(feature = "gpu-cuda"))]
    let need_cpu_build = true;

    // Precompute all rotated queries for pairwise distance oracle (CPU path only)
    let all_rotated: Vec<Vec<f32>> = if need_cpu_build {
        let mut rotated: Vec<Vec<f32>> = Vec::with_capacity(n);
        let mut q_rot_buf = vec![0.0f32; padded];

        // --- GPU batch FWHT path (feature-gated) ---
        // Attempt to accelerate the FWHT rotation of all query vectors on the GPU.
        // Build a contiguous buffer of normalized, zero-padded vectors, run GPU FWHT,
        // then split back into per-vector slices.
        #[cfg(feature = "gpu-cuda")]
        let gpu_fwht_done = {
            use crate::vector::gpu::{try_gpu_batch_fwht, MIN_BATCH_FOR_GPU};
            if n >= MIN_BATCH_FOR_GPU {
                // Build contiguous padded buffer: normalize + zero-pad each vector
                let mut batch_buf = vec![0.0f32; n * padded];
                for i in 0..n {
                    let vec_slice = &live_f32_vecs[i * dim..(i + 1) * dim];
                    let mut norm_sq = 0.0f32;
                    for &v in vec_slice {
                        norm_sq += v * v;
                    }
                    let norm = norm_sq.sqrt();
                    let dst = &mut batch_buf[i * padded..i * padded + dim];
                    dst.copy_from_slice(vec_slice);
                    if norm > 0.0 {
                        let inv = 1.0 / norm;
                        for v in dst.iter_mut() {
                            *v *= inv;
                        }
                    }
                    // padded tail already zero from vec! initialization
                }

                if try_gpu_batch_fwht(&mut batch_buf, signs, padded) {
                    // GPU succeeded: split batch buffer into per-vector vecs
                    for i in 0..n {
                        rotated.push(batch_buf[i * padded..(i + 1) * padded].to_vec());
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };

        #[cfg(feature = "gpu-cuda")]
        let skip_cpu_fwht = gpu_fwht_done;
        #[cfg(not(feature = "gpu-cuda"))]
        let skip_cpu_fwht = false;

        if !skip_cpu_fwht {
            for i in 0..n {
                let vec_slice = &live_f32_vecs[i * dim..(i + 1) * dim];
                // Normalize
                let mut norm_sq = 0.0f32;
                for &v in vec_slice {
                    norm_sq += v * v;
                }
                let norm = norm_sq.sqrt();

                q_rot_buf[..dim].copy_from_slice(vec_slice);
                if norm > 0.0 {
                    let inv = 1.0 / norm;
                    for v in q_rot_buf[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                for v in q_rot_buf[dim..padded].iter_mut() {
                    *v = 0.0;
                }
                fwht::fwht(&mut q_rot_buf[..padded], signs);
                rotated.push(q_rot_buf[..padded].to_vec());
            }
        }
        rotated
    } else {
        Vec::new()
    };

    let graph = if need_cpu_build {
        let dist_table = crate::vector::distance::table();
        let mut builder = HnswBuilder::new(HNSW_M, HNSW_EF_CONSTRUCTION, seed);

        for _i in 0..n {
            builder.insert(|a: u32, b: u32| {
                let q_rot = &all_rotated[a as usize];
                let offset = b as usize * bytes_per_code;
                let code_slice = &tq_buffer_orig[offset..offset + bytes_per_code - 4];
                let norm_bytes =
                    &tq_buffer_orig[offset + bytes_per_code - 4..offset + bytes_per_code];
                let norm = f32::from_le_bytes([
                    norm_bytes[0],
                    norm_bytes[1],
                    norm_bytes[2],
                    norm_bytes[3],
                ]);
                (dist_table.tq_l2)(q_rot, code_slice, norm)
            });
        }

        builder.build(bytes_per_code as u32)
    } else {
        #[cfg(feature = "gpu-cuda")]
        {
            // SAFETY: gpu_graph is Some when need_cpu_build is false
            gpu_graph.expect("gpu_graph must be Some when need_cpu_build is false")
        }
        #[cfg(not(feature = "gpu-cuda"))]
        {
            unreachable!("need_cpu_build is always true without gpu-cuda feature")
        }
    };

    // ── Step 5: BFS reorder TQ and SQ buffers ────────────────────────
    // (Step 5 before Step 4 because verify_recall needs BFS-ordered buffer)
    let mut tq_bfs = vec![0u8; n * bytes_per_code];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let src = orig_id * bytes_per_code;
        let dst = bfs_pos * bytes_per_code;
        tq_bfs[dst..dst + bytes_per_code]
            .copy_from_slice(&tq_buffer_orig[src..src + bytes_per_code]);
    }

    // BFS reorder SQ vectors
    let mut sq_bfs = vec![0i8; n * dim];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let src = orig_id * dim;
        let dst = bfs_pos * dim;
        sq_bfs[dst..dst + dim].copy_from_slice(&live_sq_vecs[src..src + dim]);
    }

    // BFS reorder f32 vectors for HNSW search
    let mut f32_bfs = vec![0.0f32; n * dim];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let src = orig_id * dim;
        let dst = bfs_pos * dim;
        f32_bfs[dst..dst + dim].copy_from_slice(&live_f32_vecs[src..src + dim]);
    }

    // ── Step 4: Verify recall ────────────────────────────────────────
    let recall = verify_recall(
        &graph,
        &tq_bfs,
        &live_f32_vecs,
        collection,
        frozen.dimension,
    );
    if recall < MIN_RECALL {
        return Err(CompactionError::RecallTooLow {
            recall,
            required: MIN_RECALL,
        });
    }

    // ── Step 6: Payload indexes (stub for Phase 64) ──────────────────
    // No-op.

    // ── Step 7: Persist to disk ────────────────────────────────────────
    // Deferred to after ImmutableSegment construction so we can pass the
    // complete segment to write_immutable_segment.

    // ── Step 8: Create ImmutableSegment ──────────────────────────────
    // Build MVCC headers in BFS order
    let mvcc: Vec<MvccHeader> = (0..n)
        .map(|bfs_pos| {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let entry = live_entries[orig_id];
            MvccHeader {
                internal_id: bfs_pos as u32,
                insert_lsn: entry.insert_lsn,
                delete_lsn: entry.delete_lsn,
            }
        })
        .collect();

    let total_count = frozen.entries.len() as u32;
    let live_count = n as u32;

    let segment = ImmutableSegment::new(
        graph,
        AlignedBuffer::from_vec(tq_bfs),
        AlignedBuffer::from_vec(sq_bfs),
        AlignedBuffer::from_vec(f32_bfs),
        mvcc,
        collection.clone(),
        live_count,
        total_count,
    );

    // Step 7 (continued): persist to disk if requested
    if let Some((dir, segment_id)) = persist {
        segment_io::write_immutable_segment(dir, segment_id, &segment, collection)
            .map_err(|e| CompactionError::PersistFailed(format!("{e}")))?;
    }

    Ok(segment)
}

/// Verify recall of the HNSW graph against brute-force TQ-ADC ground truth.
///
/// Samples min(RECALL_SAMPLE_SIZE, n) queries deterministically and measures
/// recall@10. Returns average recall across all sampled queries.
fn verify_recall(
    graph: &crate::vector::hnsw::graph::HnswGraph,
    tq_buffer_bfs: &[u8],
    live_vectors: &[f32],
    collection: &Arc<CollectionMetadata>,
    dimension: u32,
) -> f32 {
    let n = graph.num_nodes() as usize;
    if n == 0 {
        return 1.0;
    }

    let dim = dimension as usize;
    let padded = collection.padded_dimension as usize;
    let signs = collection.fwht_sign_flips.as_slice();
    let dist_table = crate::vector::distance::table();
    let k = 10.min(n);
    let ef_verify = 64;

    // Determine sample indices (deterministic)
    let sample_size = RECALL_SAMPLE_SIZE.min(n);
    let step = if n > sample_size { n / sample_size } else { 1 };
    let sample_indices: Vec<usize> = (0..n).step_by(step).take(sample_size).collect();

    let mut scratch = SearchScratch::new(n as u32, collection.padded_dimension);
    let mut total_recall = 0.0f32;

    for &query_orig_idx in &sample_indices {
        let query_slice = &live_vectors[query_orig_idx * dim..(query_orig_idx + 1) * dim];

        // HNSW search
        let hnsw_results = hnsw_search(
            graph,
            tq_buffer_bfs,
            query_slice,
            collection,
            k,
            ef_verify,
            &mut scratch,
        );

        // Brute-force TQ-ADC ground truth
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(query_slice);
        // Normalize
        let mut norm_sq = 0.0f32;
        for &v in &q_rotated[..dim] {
            norm_sq += v * v;
        }
        let q_norm = norm_sq.sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rotated[..dim].iter_mut() {
                *v *= inv;
            }
        }
        for v in q_rotated[dim..padded].iter_mut() {
            *v = 0.0;
        }
        fwht::fwht(&mut q_rotated[..padded], signs);

        // Compute distance to every node
        let mut dists: Vec<(f32, u32)> = (0..n as u32)
            .map(|bfs_pos| {
                let code = graph.tq_code(bfs_pos, tq_buffer_bfs);
                let code_only = &code[..code.len() - 4];
                let norm = graph.tq_norm(bfs_pos, tq_buffer_bfs);
                let d = (dist_table.tq_l2)(&q_rotated, code_only, norm);
                let orig_id = graph.to_original(bfs_pos);
                (d, orig_id)
            })
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        let gt_ids: std::collections::HashSet<u32> =
            dists.iter().take(k).map(|d| d.1).collect();
        let found_ids: std::collections::HashSet<u32> =
            hnsw_results.iter().map(|r| r.id.0).collect();
        let overlap = gt_ids.intersection(&found_ids).count();
        total_recall += overlap as f32 / k as f32;
    }

    total_recall / sample_indices.len() as f32
}

/// Check if an immutable segment needs vacuum (rebuild due to too many dead entries).
///
/// Returns true when dead_fraction > 20%.
pub fn needs_vacuum(segment: &ImmutableSegment) -> bool {
    segment.dead_fraction() > VACUUM_DEAD_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::segment::mutable::MutableSegment;
    use crate::vector::turbo_quant::collection::QuantizationConfig;
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

    fn make_frozen_segment(n: usize, dim: usize, delete_count: usize) -> (FrozenSegment, Arc<CollectionMetadata>) {
        distance::init();
        let seg = MutableSegment::new(dim as u32);

        for i in 0..n {
            let mut f32_v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut f32_v);
            let sq_v: Vec<i8> = f32_v.iter().map(|&x| (x * 127.0).clamp(-128.0, 127.0) as i8).collect();
            seg.append(i as u64, &f32_v, &sq_v, 1.0, i as u64 + 1);
        }

        // Mark some as deleted
        for i in 0..delete_count {
            seg.mark_deleted(i as u32, 100);
        }

        let frozen = seg.freeze();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
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
        let results = imm.search(&query, 5, 64);
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
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
        assert!(result.is_ok(), "compact failed (recall too low): {:?}", result.err());
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
        assert!(result.is_ok(), "compact with GPU fallback failed: {:?}", result.err());
        assert_eq!(result.unwrap().live_count(), 100);
    }
}
