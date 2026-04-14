//! Reciprocal Rank Fusion (RRF) for combining dense and sparse search results.
//!
//! RRF merges two independently ranked result lists into a single fused list.
//! The algorithm assigns each document a score of `sum(1 / (k + rank))` across
//! all lists it appears in, where `k` is a constant (default 60) that dampens
//! the influence of high-ranked items. Documents appearing in both lists
//! naturally receive higher fused scores.

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
}
