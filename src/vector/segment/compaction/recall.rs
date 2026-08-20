//! Recall verification for both compaction paths.
//!
//! Split out of the former single-file `compaction.rs` (moon#479, file-size
//! ceiling). Holds `verify_recall` (compact path) and `verify_merge_recall`
//! (GraphUnion merge gate) unchanged, including the moon#546 / moon#588
//! distance-scoring rationale recorded on `verify_merge_recall`.

use std::sync::Arc;

use crate::vector::hnsw::search_sq::hnsw_search_f32;
use crate::vector::segment::compaction::RECALL_SAMPLE_SIZE;
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::sq8::{SQ8_PARAMS_BYTES, decode_sq8, sq8_params};

/// Verify recall of the HNSW graph using f32 L2 search against brute-force
/// f32 L2 ground truth.
///
/// Since ImmutableSegment now delegates HNSW traversal to hnsw_search_f32
/// (TQ-ADC is reserved for brute-force scan), verification must also use
/// f32 L2 to match the production search path.
///
/// Samples min(RECALL_SAMPLE_SIZE, n) queries deterministically and measures
/// recall@10. Returns average recall across all sampled queries.
#[allow(dead_code)]
fn verify_recall(
    graph: &crate::vector::hnsw::graph::HnswGraph,
    _tq_buffer_bfs: &[u8],
    live_vectors: &[f32],
    _collection: &Arc<CollectionMetadata>,
    dimension: u32,
) -> f32 {
    let n = graph.num_nodes() as usize;
    if n == 0 {
        return 1.0;
    }

    let dim = dimension as usize;
    let l2_fn = crate::vector::distance::table().l2_f32;
    let k = 10.min(n);
    let ef_verify = 128;

    // BFS-reorder f32 vectors for hnsw_search_f32
    let mut f32_bfs = vec![0.0f32; n * dim];
    for bfs_pos in 0..n {
        let orig_id = graph.to_original(bfs_pos as u32) as usize;
        let src = orig_id * dim;
        let dst = bfs_pos * dim;
        f32_bfs[dst..dst + dim].copy_from_slice(&live_vectors[src..src + dim]);
    }

    // Determine sample indices (deterministic)
    let sample_size = RECALL_SAMPLE_SIZE.min(n);
    let step = if n > sample_size { n / sample_size } else { 1 };
    let sample_indices: Vec<usize> = (0..n).step_by(step).take(sample_size).collect();

    let mut total_recall = 0.0f32;

    for &query_orig_idx in &sample_indices {
        let query_slice = &live_vectors[query_orig_idx * dim..(query_orig_idx + 1) * dim];

        // HNSW search using f32 L2 (matches production path)
        let hnsw_results = hnsw_search_f32(graph, &f32_bfs, dim, query_slice, k, ef_verify, None);

        // Brute-force f32 L2 ground truth
        let mut dists: Vec<(f32, u32)> = (0..n as u32)
            .map(|i| {
                let v = &live_vectors[i as usize * dim..(i as usize + 1) * dim];
                (l2_fn(query_slice, v), i)
            })
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        let gt_ids: std::collections::HashSet<u32> = dists.iter().take(k).map(|d| d.1).collect();
        let found_ids: std::collections::HashSet<u32> =
            hnsw_results.iter().map(|r| r.id.0).collect();
        let overlap = gt_ids.intersection(&found_ids).count();
        total_recall += overlap as f32 / k as f32;
    }

    total_recall / sample_indices.len() as f32
}

/// Verify recall of the merged HNSW graph against brute-force over the merged
/// decoded centroid vectors.
///
/// Both ground-truth and HNSW search operate in the same merged coordinate
/// space (sequential IDs 0..n-1 in BFS order), so recall is well-defined.
///
/// Algorithm:
/// 1. Decode all n merged TQ codes to centroid f32 vectors (FWHT-rotated).
/// 2. For each sampled query (one of those decoded centroids), compute brute-
///    force top-K by L2 as ground truth.
/// 3. Run HNSW search on the merged graph using the same query.
/// 4. Recall@K = |HNSW_topk ∩ brute_force_topk| / K.
///
/// This is the same methodology as the existing `verify_recall()` for
/// mutable→immutable compaction.
///
/// Returns 1.0 if n < MIN_RECALL_SAMPLE (too few vectors for reliable measurement).
///
/// Minimum sample threshold: 50 vectors. Below this, HNSW can't form reliable
/// graph topology (fewer candidates than ef_construction demands). The gate still
/// fires for sizes ≥50, which is low enough that tests can exercise rejection
/// paths without spinning up 500-vector corpora.
///
/// Production merges happen at ≥16 segments × ≥1000 vectors = ≥16K total vectors.
const MIN_RECALL_SAMPLE_N: usize = 50;

pub(super) fn verify_merge_recall(
    graph: &crate::vector::hnsw::graph::HnswGraph,
    tq_bfs: &[u8],
    _pre_segments: &[Arc<ImmutableSegment>],
    collection: &Arc<CollectionMetadata>,
    dim: usize,
    n: usize,
    seed: u64,
) -> f32 {
    if n < MIN_RECALL_SAMPLE_N {
        return 1.0; // too few vectors for reliable measurement
    }
    let k = 10.min(n / 2).max(1);
    if n < k * 2 {
        return 1.0; // too few vectors
    }

    let is_sq8 = collection.quantization == QuantizationConfig::Sq8;
    let bytes_per_code = if is_sq8 {
        dim + SQ8_PARAMS_BYTES
    } else {
        collection.bytes_per_code_per_vector() as usize
    };
    let code_len = if is_sq8 { dim } else { bytes_per_code - 4 };
    let padded = collection.padded_dimension as usize;
    // Effective f32 dimensionality of the decoded recall oracle: `dim` for SQ8
    // (affine decode → dim-length vectors), `padded` for TQ (codebook decode →
    // padded-length FWHT-rotated space). The flat buffer stride and the
    // hnsw_search_f32 dimension must both use this, or the oracle mis-strides.
    let eff_dim = if is_sq8 { dim } else { padded };
    let sample_size = RECALL_SAMPLE_SIZE.min(n / 2).max(1);
    let step = (n / sample_size).max(1);

    let is_a2 = collection.quantization
        == crate::vector::turbo_quant::collection::QuantizationConfig::TurboQuant4A2;
    let a2_cb = if is_a2 {
        Some(crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
            collection.padded_dimension,
        ))
    } else {
        Option::None
    };

    /// Decode one TQ code slice to a centroid f32 vector (FWHT-rotated space).
    fn decode_code(
        code_slice: &[u8],
        padded: usize,
        is_a2: bool,
        a2_cb: Option<&crate::vector::turbo_quant::a2_lattice::A2Codebook>,
        codebook: Option<&[f32; 16]>,
    ) -> Vec<f32> {
        let mut q_rot = Vec::with_capacity(padded);
        if is_a2 {
            if let Some(cb) = a2_cb {
                for &byte in code_slice {
                    let (x0, y0) = cb.decode_pair(byte & 0x0F);
                    let (x1, y1) = cb.decode_pair(byte >> 4);
                    q_rot.push(x0);
                    q_rot.push(y0);
                    q_rot.push(x1);
                    q_rot.push(y1);
                }
            }
        } else if let Some(cb) = codebook {
            for &byte in code_slice {
                q_rot.push(cb[(byte & 0x0F) as usize]);
                q_rot.push(cb[(byte >> 4) as usize]);
            }
        }
        q_rot.truncate(padded);
        q_rot
    }

    let codebook = if !is_a2 {
        collection.try_codebook_16()
    } else {
        None
    };

    // Decode all n centroid vectors once.
    let all_decoded: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            let offset = i * bytes_per_code;
            if is_sq8 {
                let slot = &tq_bfs[offset..offset + bytes_per_code];
                let (min, scale) = sq8_params(slot, dim);
                decode_sq8(&slot[..dim], min, scale)
            } else {
                let code_slice = &tq_bfs[offset..offset + code_len];
                decode_code(code_slice, padded, is_a2, a2_cb.as_ref(), codebook)
            }
        })
        .collect();

    let l2_fn = crate::vector::distance::table().l2_f32;
    let ef_verify = (k * 15).max(128);

    // Build the flat f32 BFS buffer once (amortize allocation over all queries).
    // Each all_decoded[i] has `padded` elements in BFS order.
    let f32_bfs_flat: Vec<f32> = all_decoded.iter().flatten().copied().collect();

    let mut total_recall = 0.0f32;
    let mut sample_count = 0usize;

    // Deterministic sample: evenly spaced through BFS order, offset by seed.
    let offset = (seed as usize) % step.max(1);
    let sample_indices: Vec<usize> = (offset..n).step_by(step).take(sample_size).collect();

    for &query_bfs in &sample_indices {
        let query = &all_decoded[query_bfs];
        if query.len() < eff_dim / 2 {
            continue; // skip degenerate empty decode
        }

        // Brute-force top-K in decoded centroid space (ground truth).
        let mut dists: Vec<(f32, u32)> = (0..n as u32)
            .filter(|&i| i != query_bfs as u32)
            .map(|i| (l2_fn(query, &all_decoded[i as usize]), i))
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        // Score by DISTANCE, not by id (moon#546). Duplicate vectors are
        // ubiquitous in real corpora, and when a point has many exact
        // duplicates the ground truth takes an ARBITRARY k of the tied
        // candidates while HNSW takes a DIFFERENT arbitrary k — both answers
        // are exactly correct, but an id-set overlap scores them 0.0 and the
        // gate rejects a perfect merge forever. The k-th ground-truth distance
        // is the acceptance threshold: any neighbour at least that close is a
        // correct answer, whichever of the tied ids it happens to be.
        let gt_len = dists.len().min(k);
        let gt_kth = match dists.get(gt_len.wrapping_sub(1)) {
            Some(d) => d.0,
            None => continue, // no comparable neighbours; nothing to measure
        };
        // Relative + absolute slack absorbs float non-determinism between the
        // brute-force and graph distance paths (zero distances need the
        // absolute term; the relative term carries large-magnitude ones).
        let tol = gt_kth.abs() * 1e-4 + 1e-6;

        // HNSW search on the merged graph using f32 decoded centroids.
        // f32_bfs_flat has BFS-ordered vectors, `eff_dim` elements each.
        //
        // The query IS a database point (distance 0 → always rank 1), but the
        // ground truth above excludes it. Request k+1 and drop the self-point
        // so both sides compare k non-self neighbors — with a plain top-k the
        // self-point consumes one slot and caps measurable recall at (k-1)/k
        // = 0.90, which the manual force_merge gate (0.90) can never pass.
        let hnsw_results =
            hnsw_search_f32(graph, &f32_bfs_flat, eff_dim, query, k + 1, ef_verify, None);
        // hnsw_search_f32 returns original IDs (pre-BFS); convert to BFS positions
        // so they match the ground-truth set (which indexes all_decoded by BFS pos).
        let hnsw_ids: std::collections::HashSet<u32> = hnsw_results
            .iter()
            .map(|r| graph.to_bfs(r.id.0))
            .filter(|&b| b != query_bfs as u32)
            .take(k)
            .collect();

        let hits = hnsw_ids
            .iter()
            .filter(|&&b| l2_fn(query, &all_decoded[b as usize]) <= gt_kth + tol)
            .count();
        total_recall += hits as f32 / gt_len.max(1) as f32;
        sample_count += 1;
    }

    if sample_count == 0 {
        return 1.0;
    }
    total_recall / sample_count as f32
}
