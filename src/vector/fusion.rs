//! Reciprocal Rank Fusion (RRF) for combining dense and sparse search results.
//!
//! RRF merges two independently ranked result lists into a single fused list.
//! The algorithm assigns each document a score of `sum(1 / (k + rank))` across
//! all lists it appears in, where `k` is a constant (default 60) that dampens
//! the influence of high-ranked items. Documents appearing in both lists
//! naturally receive higher fused scores.
//!
//! Phase 152 adds `rrf_fuse_three` — three-way weighted RRF for hybrid
//! BM25 + dense + sparse retrieval, per CONTEXT.md D-14 / D-15 / D-16 / D-17.

use std::collections::HashMap;

use crate::vector::types::{SearchResult, VectorId};

/// RRF constant `k` — controls rank decay. Standard value from the original
/// Cormack, Clarke & Buettcher (2009) paper.
const RRF_K: f32 = 60.0;

/// Fuse two ranked result lists using Reciprocal Rank Fusion.
///
/// Returns `(fused_results, dense_hit_count, sparse_hit_count)`.
/// Each result's `distance` field is set to the **negative** RRF score
/// so that lower distance = better, consistent with `SearchResult::Ord`.
///
/// Documents with `key_hash == 0` are skipped (unknown keys).
pub fn rrf_fuse(
    dense_results: &[SearchResult],
    sparse_results: &[SearchResult],
    top_k: usize,
) -> (Vec<SearchResult>, usize, usize) {
    let dense_count = dense_results.len();
    let sparse_count = sparse_results.len();

    if dense_count == 0 && sparse_count == 0 {
        return (Vec::new(), 0, 0);
    }

    // Accumulate RRF scores keyed by key_hash.
    // Value: (accumulated_score, VectorId from first encounter).
    let capacity = dense_count + sparse_count;
    let mut scores: HashMap<u64, (f32, VectorId)> = HashMap::with_capacity(capacity);

    for (rank, r) in dense_results.iter().enumerate() {
        if r.key_hash == 0 {
            continue;
        }
        let rrf_score = 1.0 / (RRF_K + rank as f32 + 1.0);
        scores
            .entry(r.key_hash)
            .and_modify(|(s, _)| *s += rrf_score)
            .or_insert((rrf_score, r.id));
    }

    for (rank, r) in sparse_results.iter().enumerate() {
        if r.key_hash == 0 {
            continue;
        }
        let rrf_score = 1.0 / (RRF_K + rank as f32 + 1.0);
        scores
            .entry(r.key_hash)
            .and_modify(|(s, _)| *s += rrf_score)
            .or_insert((rrf_score, r.id));
    }

    // Collect and sort by descending RRF score (negative distance = lower is better).
    let mut fused: Vec<SearchResult> = scores
        .into_iter()
        .map(|(key_hash, (score, id))| SearchResult {
            distance: -score,
            id,
            key_hash,
        })
        .collect();

    fused.sort_unstable_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    fused.truncate(top_k);

    (fused, dense_count, sparse_count)
}

/// Three-way weighted Reciprocal Rank Fusion for hybrid BM25 + dense + sparse retrieval.
///
/// Generalizes `rrf_fuse` (two-way, unweighted) to three streams with per-stream weights.
/// Per CONTEXT.md D-14:   `score(doc) = Σ_s w_s * (1 / (k + rank_s + 1))`
/// Per CONTEXT.md D-16:   any stream may be empty (length 0).
/// Per CONTEXT.md D-17:   caller validates weights are finite and non-negative.
///                        Any weight `<= 0.0` (or NaN) silently skips that stream inside
///                        this function — defensive, since the caller's validation is
///                        the primary gate (`parse_hybrid_modifier` rejects negative/NaN).
///
/// `distance = -score` so that lower distance = better (matches `SearchResult::Ord`).
/// Documents with `key_hash == 0` are skipped (fusion's unknown-key convention).
///
/// Returns `(fused_results, bm25_count, dense_count, sparse_count)`.
pub fn rrf_fuse_three(
    bm25_results: &[SearchResult],
    dense_results: &[SearchResult],
    sparse_results: &[SearchResult],
    weights: [f32; 3],
    top_k: usize,
) -> (Vec<SearchResult>, usize, usize, usize) {
    let bm25_count = bm25_results.len();
    let dense_count = dense_results.len();
    let sparse_count = sparse_results.len();

    if bm25_count == 0 && dense_count == 0 && sparse_count == 0 {
        return (Vec::new(), 0, 0, 0);
    }

    // Accumulate RRF scores keyed by key_hash.
    // Value: (accumulated_score, VectorId from first encounter).
    let capacity = bm25_count + dense_count + sparse_count;
    let mut scores: HashMap<u64, (f32, VectorId)> = HashMap::with_capacity(capacity);

    // Inline contribution of a single stream — avoids any heap allocation in the hot loop.
    // The `w.is_nan() || w <= 0.0` guard gives defensive skip per D-17 second sentence
    // (NaN, negative, and zero weights all silently skip the stream — caller validates
    // at the parser boundary; this is a redundant internal gate).
    let mut contribute = |results: &[SearchResult], w: f32| {
        if w.is_nan() || w <= 0.0 {
            return;
        }
        for (rank, r) in results.iter().enumerate() {
            if r.key_hash == 0 {
                continue;
            }
            let rrf = w * (1.0 / (RRF_K + rank as f32 + 1.0));
            scores
                .entry(r.key_hash)
                .and_modify(|(s, _)| *s += rrf)
                .or_insert((rrf, r.id));
        }
    };
    contribute(bm25_results, weights[0]);
    contribute(dense_results, weights[1]);
    contribute(sparse_results, weights[2]);

    // Collect and sort by descending RRF score (negative distance = lower is better).
    let mut fused: Vec<SearchResult> = scores
        .into_iter()
        .map(|(key_hash, (score, id))| SearchResult {
            distance: -score,
            id,
            key_hash,
        })
        .collect();

    fused.sort_unstable_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    fused.truncate(top_k);

    (fused, bm25_count, dense_count, sparse_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::types::{SearchResult, VectorId};

    fn sr(distance: f32, id: u32, key_hash: u64) -> SearchResult {
        SearchResult::with_key_hash(distance, VectorId(id), key_hash)
    }

    #[test]
    fn test_rrf_both_empty() {
        let (results, dc, sc) = rrf_fuse(&[], &[], 10);
        assert!(results.is_empty());
        assert_eq!(dc, 0);
        assert_eq!(sc, 0);
    }

    #[test]
    fn test_rrf_dense_only() {
        let dense = vec![sr(0.1, 1, 100), sr(0.2, 2, 200), sr(0.3, 3, 300)];
        let (results, dc, sc) = rrf_fuse(&dense, &[], 10);
        assert_eq!(dc, 3);
        assert_eq!(sc, 0);
        assert_eq!(results.len(), 3);
        // First result should have highest RRF score (rank 0 → 1/61)
        assert_eq!(results[0].key_hash, 100);
    }

    #[test]
    fn test_rrf_sparse_only() {
        let sparse = vec![sr(5.0, 10, 1000), sr(3.0, 11, 1001)];
        let (results, dc, sc) = rrf_fuse(&[], &sparse, 10);
        assert_eq!(dc, 0);
        assert_eq!(sc, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key_hash, 1000);
    }

    #[test]
    fn test_rrf_overlap_scores_higher() {
        // Doc with key_hash=100 appears in both lists → should rank highest
        let dense = vec![sr(0.1, 1, 100), sr(0.2, 2, 200)];
        let sparse = vec![sr(5.0, 10, 100), sr(3.0, 11, 300)];
        let (results, dc, sc) = rrf_fuse(&dense, &sparse, 10);
        assert_eq!(dc, 2);
        assert_eq!(sc, 2);
        assert_eq!(results.len(), 3); // 100, 200, 300
        // key_hash=100 appears in both → highest combined score
        assert_eq!(results[0].key_hash, 100);
        // Its score = 1/(61) + 1/(61) = 2/61 ≈ 0.0328
        let expected_score = 2.0 / 61.0;
        assert!((results[0].distance + expected_score).abs() < 1e-6);
    }

    #[test]
    fn test_rrf_top_k_truncation() {
        let dense: Vec<SearchResult> = (0..20)
            .map(|i| sr(i as f32 * 0.1, i, (i + 1) as u64 * 100))
            .collect();
        let (results, _, _) = rrf_fuse(&dense, &[], 5);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_rrf_preserves_key_hash() {
        let dense = vec![sr(0.1, 1, 42)];
        let sparse = vec![sr(5.0, 10, 99)];
        let (results, _, _) = rrf_fuse(&dense, &sparse, 10);
        let hashes: Vec<u64> = results.iter().map(|r| r.key_hash).collect();
        assert!(hashes.contains(&42));
        assert!(hashes.contains(&99));
    }

    #[test]
    fn test_rrf_skips_zero_key_hash() {
        let dense = vec![sr(0.1, 1, 0), sr(0.2, 2, 200)];
        let sparse = vec![sr(5.0, 10, 0)];
        let (results, dc, sc) = rrf_fuse(&dense, &sparse, 10);
        assert_eq!(dc, 2);
        assert_eq!(sc, 1);
        assert_eq!(results.len(), 1); // only key_hash=200 survives
        assert_eq!(results[0].key_hash, 200);
    }

    #[test]
    fn test_rrf_rank_ordering() {
        // Verify that rank position matters: rank-0 doc gets higher score than rank-1
        let dense = vec![sr(0.1, 1, 100), sr(0.2, 2, 200)];
        let (results, _, _) = rrf_fuse(&dense, &[], 10);
        // distance is negative score, so result[0].distance < result[1].distance
        assert!(results[0].distance < results[1].distance);
        assert_eq!(results[0].key_hash, 100); // rank-0 → highest score
    }

    // ── rrf_fuse_three tests (Phase 152) ──────────────────────────────────────

    #[test]
    fn test_rrf_fuse_three_all_empty() {
        let (results, bc, dc, sc) = rrf_fuse_three(&[], &[], &[], [1.0, 1.0, 1.0], 10);
        assert!(results.is_empty());
        assert_eq!(bc, 0);
        assert_eq!(dc, 0);
        assert_eq!(sc, 0);
    }

    #[test]
    fn test_rrf_fuse_three_only_bm25() {
        let bm25 = vec![sr(-10.0, 1, 100), sr(-8.0, 2, 200), sr(-5.0, 3, 300)];
        let (results, bc, dc, sc) = rrf_fuse_three(&bm25, &[], &[], [1.0, 1.0, 1.0], 10);
        assert_eq!(bc, 3);
        assert_eq!(dc, 0);
        assert_eq!(sc, 0);
        assert_eq!(results.len(), 3);
        // rank-0 in BM25 stream → highest score → first
        assert_eq!(results[0].key_hash, 100);
        assert_eq!(results[1].key_hash, 200);
        assert_eq!(results[2].key_hash, 300);
    }

    #[test]
    fn test_rrf_fuse_three_sparse_empty_matches_two_way() {
        // Two populated streams with weights [0.0, 1.0, 1.0] and empty BM25
        // MUST return the same fused scores as rrf_fuse(dense, sparse). Proves
        // additivity + zero-weight silent disable together.
        let dense = vec![sr(0.1, 1, 100), sr(0.2, 2, 200)];
        let sparse = vec![sr(5.0, 10, 100), sr(3.0, 11, 300)];

        let (baseline, _, _) = rrf_fuse(&dense, &sparse, 10);
        let (three, bc, dc, sc) =
            rrf_fuse_three(&[], &dense, &sparse, [0.0, 1.0, 1.0], 10);
        assert_eq!(bc, 0);
        assert_eq!(dc, 2);
        assert_eq!(sc, 2);
        assert_eq!(baseline.len(), three.len());

        // Compare as hash→distance maps (tie-break order is non-deterministic
        // across the two code paths; only the score-per-key mapping is load-bearing).
        let b_map: std::collections::HashMap<u64, f32> =
            baseline.iter().map(|r| (r.key_hash, r.distance)).collect();
        let t_map: std::collections::HashMap<u64, f32> =
            three.iter().map(|r| (r.key_hash, r.distance)).collect();
        assert_eq!(b_map.len(), t_map.len());
        for (k, bd) in &b_map {
            let td = t_map.get(k).copied().expect("key present in three-way");
            assert!((bd - td).abs() < 1e-6, "score mismatch for key {k}");
        }
    }

    #[test]
    fn test_rrf_fuse_three_overlap_scores_higher() {
        // Doc 100 appears in ALL three streams; others appear once.
        let bm25 = vec![sr(-10.0, 1, 100), sr(-5.0, 2, 200)];
        let dense = vec![sr(0.1, 3, 100), sr(0.3, 4, 300)];
        let sparse = vec![sr(5.0, 5, 100), sr(3.0, 6, 400)];
        let (results, bc, dc, sc) =
            rrf_fuse_three(&bm25, &dense, &sparse, [1.0, 1.0, 1.0], 10);
        assert_eq!(bc, 2);
        assert_eq!(dc, 2);
        assert_eq!(sc, 2);
        // 4 distinct keys: 100, 200, 300, 400
        assert_eq!(results.len(), 4);
        // Doc 100 gets 3x the rank-0 contribution → highest
        assert_eq!(results[0].key_hash, 100);
        // Score = 3 * (1/61) ≈ 0.04918
        let expected = 3.0 / 61.0;
        assert!((results[0].distance + expected).abs() < 1e-6);
    }

    #[test]
    fn test_rrf_fuse_three_weights_shift_ranking() {
        // weights=[2.0, 1.0, 0.5]: BM25-only doc at rank 0 must beat
        // a sparse-only doc at rank 0.
        let bm25 = vec![sr(-10.0, 1, 100)];
        let dense = vec![];
        let sparse = vec![sr(5.0, 2, 200)];
        let (results, _, _, _) =
            rrf_fuse_three(&bm25, &dense, &sparse, [2.0, 1.0, 0.5], 10);
        assert_eq!(results.len(), 2);
        // BM25 doc (weight 2.0 at rank 0) should rank before sparse (weight 0.5 at rank 0).
        assert_eq!(results[0].key_hash, 100);
        assert_eq!(results[1].key_hash, 200);
    }

    #[test]
    fn test_rrf_fuse_three_zero_weight_disables_stream() {
        // weights=[0.0, 1.0, 1.0]: BM25 docs that appear ONLY in BM25 are skipped.
        let bm25 = vec![sr(-10.0, 1, 100)]; // bm25-only doc
        let dense = vec![sr(0.1, 2, 200)];
        let sparse = vec![sr(5.0, 3, 300)];
        let (results, bc, dc, sc) =
            rrf_fuse_three(&bm25, &dense, &sparse, [0.0, 1.0, 1.0], 10);
        // Counts still reflect raw input lengths (100% computed).
        assert_eq!(bc, 1);
        assert_eq!(dc, 1);
        assert_eq!(sc, 1);
        // But key_hash=100 must NOT appear (weight 0 → stream skipped).
        let hashes: Vec<u64> = results.iter().map(|r| r.key_hash).collect();
        assert!(!hashes.contains(&100), "bm25-only doc must be disabled at w=0");
        assert!(hashes.contains(&200));
        assert!(hashes.contains(&300));
    }

    #[test]
    fn test_rrf_fuse_three_skips_zero_key_hash() {
        let bm25 = vec![sr(-10.0, 1, 0), sr(-5.0, 2, 200)];
        let dense = vec![sr(0.1, 3, 0)];
        let sparse = vec![sr(5.0, 4, 400)];
        let (results, bc, dc, sc) =
            rrf_fuse_three(&bm25, &dense, &sparse, [1.0, 1.0, 1.0], 10);
        assert_eq!(bc, 2);
        assert_eq!(dc, 1);
        assert_eq!(sc, 1);
        // key_hash=0 entries dropped; only 200 and 400 survive.
        assert_eq!(results.len(), 2);
        let hashes: Vec<u64> = results.iter().map(|r| r.key_hash).collect();
        assert!(!hashes.contains(&0));
        assert!(hashes.contains(&200));
        assert!(hashes.contains(&400));
    }

    #[test]
    fn test_rrf_fuse_three_top_k_truncation() {
        // 30 distinct docs across three streams, top_k=5 → exactly 5 returned.
        let bm25: Vec<SearchResult> = (0..10)
            .map(|i| sr(-(i as f32), i, (i + 1) as u64 * 100))
            .collect();
        let dense: Vec<SearchResult> = (0..10)
            .map(|i| sr(i as f32 * 0.1, 100 + i, (i + 11) as u64 * 100))
            .collect();
        let sparse: Vec<SearchResult> = (0..10)
            .map(|i| sr(i as f32, 200 + i, (i + 21) as u64 * 100))
            .collect();
        let (results, _, _, _) =
            rrf_fuse_three(&bm25, &dense, &sparse, [1.0, 1.0, 1.0], 5);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_rrf_fuse_three_negative_weight_skipped_internally() {
        // Per D-17: caller validates. BUT if a negative slips through, the
        // internal `!(w > 0.0)` guard treats it the same as 0.0 (skip).
        let bm25 = vec![sr(-10.0, 1, 100)];
        let dense = vec![sr(0.1, 2, 200)];
        let sparse = vec![sr(5.0, 3, 300)];
        let (results, _, _, _) =
            rrf_fuse_three(&bm25, &dense, &sparse, [1.0, -0.5, 1.0], 10);
        let hashes: Vec<u64> = results.iter().map(|r| r.key_hash).collect();
        // Dense stream (w=-0.5) disabled → doc 200 absent.
        assert!(!hashes.contains(&200));
        assert!(hashes.contains(&100));
        assert!(hashes.contains(&300));
    }

    #[test]
    fn test_rrf_fuse_three_nan_weight_skipped_internally() {
        // NaN comparisons are always false, so `!(w > 0.0)` is true → skip.
        let bm25 = vec![sr(-10.0, 1, 100)];
        let dense = vec![sr(0.1, 2, 200)];
        let sparse = vec![sr(5.0, 3, 300)];
        let (results, _, _, _) =
            rrf_fuse_three(&bm25, &dense, &sparse, [1.0, f32::NAN, 1.0], 10);
        let hashes: Vec<u64> = results.iter().map(|r| r.key_hash).collect();
        assert!(!hashes.contains(&200));
        assert!(hashes.contains(&100));
        assert!(hashes.contains(&300));
    }

    #[test]
    fn test_rrf_fuse_three_preserves_key_hash() {
        let bm25 = vec![sr(-10.0, 1, 42)];
        let dense = vec![sr(0.1, 2, 99)];
        let sparse = vec![sr(5.0, 3, 77)];
        let (results, _, _, _) =
            rrf_fuse_three(&bm25, &dense, &sparse, [1.0, 1.0, 1.0], 10);
        let hashes: Vec<u64> = results.iter().map(|r| r.key_hash).collect();
        assert!(hashes.contains(&42));
        assert!(hashes.contains(&99));
        assert!(hashes.contains(&77));
    }
}
