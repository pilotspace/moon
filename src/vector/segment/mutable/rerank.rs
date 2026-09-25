//! Exact rerank of the mutable segment's brute-force scan (moon#1226).
//!
//! Immutable segments re-score their top `rerank_mult · k` beam candidates
//! against the f16 sidecar (HQ-1, `ImmutableSegment::rerank_exact`). The
//! mutable segment keeps the very same f16 rows (`raw_f16`, both build modes,
//! every quantizer — it is what compaction hands the sidecar) but ranked by
//! quantized ADC alone. moon#1207/#1208 made the mutable scan TQ-ADC in every
//! build mode, and TQ-ADC's `‖ĉ‖²` term lets quantization noise dominate when
//! the true neighbours are barely similar: far / out-of-distribution queries
//! lost recall (EXACT far R@10 0.831 → 0.728 at server level; WS11 NOTES
//! reproduced 0.675 at 384d and 0.669 at 768d on an embedding-shaped corpus,
//! and an emulated f16 rerank of the top 4·k recovered 0.98).
//!
//! Every mutable scan entry point of `SegmentHolder` (sync unfiltered and
//! filtered, MVCC, and the chunked cooperative-yield scan) now keeps the ADC
//! top `rerank_mult · k`, re-scores them from `raw_f16` with the SAME kernel
//! and distance convention as the immutable rerank
//! (`hnsw::prepared::exact_f16_distance`), and truncates to `k`. So the
//! mutable leg is as accurate as a compacted segment, and its distances are on
//! the same scale as the immutable legs it is merged with (it used to merge
//! ADC estimates against exact distances).
//!
//! When the rows are not all present (never expected: `raw_f16` is appended
//! with every entry) the ADC order is kept — the top `k` of the ADC top
//! `mult·k` is exactly the ADC top `k`.

use smallvec::SmallVec;

use super::MutableSegment;
use crate::vector::types::{DistanceMetric, SearchResult};

/// Candidates the mutable scan keeps for the exact rerank: `rerank_mult · k`
/// (`rerank_mult` ≥ 1, FT.CONFIG RERANK_MULT, default 4).
#[inline]
pub(crate) fn exact_rerank_depth(k: usize, rerank_mult: u32) -> usize {
    k.saturating_mul(rerank_mult.max(1) as usize)
}

impl MutableSegment {
    /// Re-score `candidates` (global ids, as the scans return them) with
    /// exact distances from `raw_f16`, re-sort ascending and keep the best
    /// `k`. See the module docs.
    pub fn rerank_exact(
        &self,
        candidates: &mut SmallVec<[SearchResult; 32]>,
        query: &[f32],
        k: usize,
    ) {
        if !candidates.is_empty() {
            let inner = self.inner.read();
            let dim = inner.dimension as usize;
            let rows_complete = inner.raw_f16.len() >= inner.entries.len().saturating_mul(dim);
            if dim > 0 && query.len() == dim && rows_complete {
                let is_l2 = self.collection.metric == DistanceMetric::L2;
                let mut q_unit: SmallVec<[f32; 512]> = SmallVec::new();
                let q: &[f32] = if is_l2 {
                    query
                } else {
                    crate::vector::hnsw::prepared::unit_query_into(query, &mut q_unit);
                    &q_unit
                };
                let kernels = crate::vector::distance::table();
                for r in candidates.iter_mut() {
                    let Some(internal) = r.id.0.checked_sub(inner.global_id_base) else {
                        continue;
                    };
                    let start = internal as usize * dim;
                    let Some(row) = inner.raw_f16.get(start..start + dim) else {
                        continue; // keep the ADC estimate
                    };
                    if let Some(d) =
                        crate::vector::hnsw::prepared::exact_f16_distance(kernels, q, row, is_l2)
                    {
                        r.distance = d;
                    }
                }
                candidates.sort_unstable();
            }
        }
        candidates.truncate(k);
    }

    /// The unfiltered / filtered brute-force scan with the exact rerank: ADC
    /// top `rerank_mult · k` (restricted to `allow_bitmap`), re-scored and
    /// truncated to `k`.
    pub fn brute_force_search_reranked(
        &self,
        query_f32: &[f32],
        k: usize,
        allow_bitmap: Option<&roaring::RoaringBitmap>,
        rerank_mult: u32,
    ) -> SmallVec<[SearchResult; 32]> {
        let mut out = self.brute_force_search_filtered(
            query_f32,
            None,
            exact_rerank_depth(k, rerank_mult),
            allow_bitmap,
        );
        self.rerank_exact(&mut out, query_f32, k);
        out
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::vector::distance;
    use crate::vector::hnsw::search::SearchScratch;
    use crate::vector::segment::holder::{MvccContext, SearchSnapshot, SegmentHolder, YieldBudget};
    use crate::vector::test_support::EmbeddingLike;
    use crate::vector::turbo_quant::collection::{
        BuildMode, CollectionMetadata, QuantizationConfig,
    };
    use crate::vector::turbo_quant::encoder::padded_dimension;
    use crate::vector::types::{DistanceMetric, SearchTuning};

    const K: usize = 10;

    fn exact_top_k(docs: &[Vec<f32>], q: &[f32]) -> Vec<u32> {
        // Unit vectors: cosine distance order == dot product order (desc).
        let mut d: Vec<(f32, u32)> = docs
            .iter()
            .enumerate()
            .map(|(i, v)| (-v.iter().zip(q).map(|(a, b)| a * b).sum::<f32>(), i as u32))
            .collect();
        d.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        d.into_iter().take(K).map(|x| x.1).collect()
    }

    fn recall(got: &[u32], want: &[u32]) -> f64 {
        got.iter().filter(|g| want.contains(g)).count() as f64 / want.len() as f64
    }

    /// moon#1226 red → green: far / out-of-distribution queries against a
    /// mutable-only EXACT index on an embedding-shaped corpus. The mutable
    /// leg ranked by TQ-ADC alone (moon#1207/#1208) lost far-query recall;
    /// every holder entry point (sync, MVCC, cooperative-yield) now exact-
    /// reranks the ADC top `4·k` from `raw_f16` and must agree bit for bit.
    /// Relative evidence only — random embedding-shaped data, not MiniLM
    /// (CLAUDE.md gotcha); MiniLM validation is deferred.
    #[test]
    fn mutable_leg_exact_rerank_recovers_far_query_recall() {
        distance::init();
        crate::vector::turbo_quant::fwht::init_fwht();
        let (dim, n) = (384usize, 3_000usize);
        let col = Arc::new(CollectionMetadata::with_build_mode(
            41,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::TurboQuant4,
            41,
            BuildMode::Exact,
        ));
        let holder = SegmentHolder::new(dim as u32, Arc::clone(&col));
        let mut g = EmbeddingLike::new(dim, 64, 23);
        let docs = g.docs(n);
        {
            let list = holder.load();
            for (i, v) in docs.iter().enumerate() {
                list.mutable.append(i as u64 + 1, v, i as u64 + 1);
            }
        }
        let committed = roaring::RoaringTreemap::new();
        let ctx = MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
            ef_defaulted: false,
            tuning: SearchTuning::default(),
        };
        let budget = YieldBudget {
            max_segments_per_chunk: usize::MAX,
            max_graph_nodes_per_chunk: usize::MAX,
            // No yield point under `block_on` (monoio's cooperative_yield
            // needs its runtime); the chunked scan still runs.
            max_brute_force_vecs_per_chunk: usize::MAX,
        };
        let classes: [(&str, Vec<Vec<f32>>); 2] = [
            ("far", (0..40).map(|_| g.far()).collect()),
            ("in", (0..40).map(|_| g.doc()).collect()),
        ];
        for (class, queries) in classes {
            let (mut adc, mut reranked) = (0.0f64, 0.0f64);
            for q in &queries {
                let want = exact_top_k(&docs, q);
                let ids = |r: &smallvec::SmallVec<[crate::vector::types::SearchResult; 32]>| {
                    r.iter().map(|x| x.id.0).collect::<Vec<u32>>()
                };
                let bits = |r: &smallvec::SmallVec<[crate::vector::types::SearchResult; 32]>| {
                    r.iter()
                        .map(|x| (x.id.0, x.distance.to_bits()))
                        .collect::<Vec<_>>()
                };
                // HEAD's mutable leg: the ADC top k.
                let list = holder.load();
                adc += recall(&ids(&list.mutable.brute_force_search(q, None, K)), &want);

                let mut scratch = SearchScratch::new(0, padded_dimension(dim as u32));
                let sync = holder.search_filtered(q, K, 64, &mut scratch, None);
                let mvcc = holder.search_mvcc(q, K, 64, &mut scratch, None, &ctx);
                let mut snap = SearchSnapshot {
                    segments: holder.load_full(),
                    query_f32: q.clone(),
                    k: K,
                    ef_search: 64,
                    filter_bitmap: None,
                    filter_strategy: crate::vector::filter::selectivity::FilterStrategy::Unfiltered,
                    snapshot_lsn: 0,
                    my_txn_id: 0,
                    committed: Arc::new(roaring::RoaringTreemap::new()),
                    dimension: dim as u32,
                    mutable_len: n,
                    scratch: SearchScratch::new(0, padded_dimension(dim as u32)),
                    key_hash_to_key: Default::default(),
                    ef_defaulted: false,
                    tuning: SearchTuning::default(),
                    pending_reloads: Vec::new(),
                };
                let yielded = futures::executor::block_on(
                    SegmentHolder::search_mvcc_yielding_with_pool(&mut snap, budget, None),
                );
                assert_eq!(bits(&sync), bits(&mvcc), "{class}: sync vs MVCC");
                assert_eq!(bits(&mvcc), bits(&yielded), "{class}: MVCC vs yielding");
                reranked += recall(&ids(&mvcc), &want);
            }
            let nq = queries.len() as f64;
            let (adc, reranked) = (adc / nq, reranked / nq);
            println!("moon#1226 mutable R@{K} {class}: ADC {adc:.3} -> reranked {reranked:.3}");
            assert!(
                reranked >= 0.95,
                "{class}: exact-reranked mutable R@{K} {reranked:.3} (ADC-only {adc:.3})"
            );
            assert!(
                reranked >= adc,
                "{class}: the rerank must not lose recall ({adc:.3} -> {reranked:.3})"
            );
        }
    }

    #[test]
    fn rerank_depth_is_mult_times_k() {
        assert_eq!(super::exact_rerank_depth(10, 4), 40);
        assert_eq!(super::exact_rerank_depth(10, 0), 10, "mult is at least 1");
        assert_eq!(super::exact_rerank_depth(usize::MAX, 4), usize::MAX);
    }
}
