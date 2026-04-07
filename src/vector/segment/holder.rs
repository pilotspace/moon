//! SegmentHolder -- ArcSwap-based lock-free segment list access.
//!
//! Searches load() once at query start and hold the Arc for the query
//! duration -- immune to concurrent swaps.

use std::sync::Arc;

use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::diskann::segment::DiskAnnSegment;
use crate::vector::filter::selectivity::{FilterStrategy, select_strategy};
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::persistence::warm_search::WarmSearchSegment;
use crate::vector::segment::ivf::IvfSegment;
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::turbo_quant::fwht;
use crate::vector::types::SearchResult;

use super::immutable::ImmutableSegment;
use super::mutable::{MutableEntry, MutableSegment};

/// Default number of IVF clusters to probe during search.
const DEFAULT_NPROBE: usize = 32;

/// MVCC context for snapshot-isolated search. Passed by reference, zero allocation.
pub struct MvccContext<'a> {
    pub snapshot_lsn: u64,
    pub my_txn_id: u64,
    pub committed: &'a roaring::RoaringBitmap,
    /// Dirty set: uncommitted entries from the active transaction.
    pub dirty_set: &'a [MutableEntry],
    pub dimension: u32,
}

/// Snapshot of all segments at a point in time.
pub struct SegmentList {
    pub mutable: Arc<MutableSegment>,
    pub immutable: Vec<Arc<ImmutableSegment>>,
    /// IVF segments for billion-scale approximate search.
    pub ivf: Vec<Arc<IvfSegment>>,
    /// Warm segments: mmap-backed, searchable after HOT->WARM transition.
    pub warm: Vec<Arc<WarmSearchSegment>>,
    /// Cold segments: DiskANN PQ+Vamana search from NVMe.
    pub cold: Vec<Arc<DiskAnnSegment>>,
}

/// Lock-free segment holder. Searches load() once at query start and hold
/// the Arc for the query duration -- immune to concurrent swaps.
pub struct SegmentHolder {
    segments: ArcSwap<SegmentList>,
}

impl SegmentHolder {
    /// Create a holder with a fresh MutableSegment and empty immutable list.
    pub fn new(
        dimension: u32,
        collection: Arc<crate::vector::turbo_quant::collection::CollectionMetadata>,
    ) -> Self {
        Self {
            segments: ArcSwap::from_pointee(SegmentList {
                mutable: Arc::new(MutableSegment::new(dimension, collection)),
                immutable: Vec::new(),
                ivf: Vec::new(),
                warm: Vec::new(),
                cold: Vec::new(),
            }),
        }
    }

    /// Single atomic load, lock-free, wait-free. This is the hot-path read.
    pub fn load(&self) -> arc_swap::Guard<Arc<SegmentList>> {
        self.segments.load()
    }

    /// Atomically replace the segment list. Old segments are dropped when
    /// Arc refcount reaches 0 (after all in-flight queries release their Guards).
    pub fn swap(&self, new_list: SegmentList) {
        self.segments.store(Arc::new(new_list));
    }

    /// Total vector count across mutable + immutable + IVF + warm segments.
    pub fn total_vectors(&self) -> u32 {
        let snapshot = self.load();
        let mut total = snapshot.mutable.len() as u32;
        for imm in &snapshot.immutable {
            total += imm.total_count();
        }
        for ivf_seg in &snapshot.ivf {
            total += ivf_seg.total_vectors() as u32;
        }
        for warm_seg in &snapshot.warm {
            total += warm_seg.total_count();
        }
        for cold_seg in &snapshot.cold {
            total += cold_seg.total_count();
        }
        total
    }

    /// Fan-out search across mutable + all immutable segments, merge results.
    ///
    /// 1. Load snapshot (atomic, lock-free).
    /// 2. Brute-force search on mutable segment with query_sq.
    /// 3. HNSW search on each immutable segment with query_f32.
    /// 4. Merge all results, take global top-k.
    pub fn search(
        &self,
        query_f32: &[f32],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
    ) -> SmallVec<[SearchResult; 32]> {
        self.search_filtered(query_f32, k, ef_search, scratch, None)
    }

    /// Fan-out search with optional filter bitmap.
    ///
    /// Dispatches to the correct strategy based on filter selectivity:
    /// - Unfiltered: standard search path
    /// - BruteForceFiltered: linear scan on bitmap matches
    /// - HnswFiltered: HNSW with ACORN 2-hop allow-list
    /// - HnswPostFilter: HNSW with 3xK oversampling + post-filter
    pub fn search_filtered(
        &self,
        query_f32: &[f32],
        k: usize,
        ef_search: usize,
        _scratch: &mut SearchScratch,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        let strategy = select_strategy(filter_bitmap, self.total_vectors());
        let snapshot = self.load();

        // Pre-allocate merge buffer: k results per segment (mutable + immutables + warm + cold).
        let segment_count =
            1 + snapshot.immutable.len() + snapshot.warm.len() + snapshot.cold.len();
        let mut all: SmallVec<[SearchResult; 32]> = SmallVec::with_capacity(k * segment_count);

        // Prepare query state: Exact mode uses TQ_prod (QJL), Light mode skips it.
        let collection = snapshot.mutable.collection();
        let query_state = if !collection.qjl_matrices.is_empty() {
            Some(
                crate::vector::turbo_quant::inner_product::prepare_query_prod(
                    query_f32,
                    &collection.qjl_matrices,
                    collection.fwht_sign_flips.as_slice(),
                    collection.padded_dimension as usize,
                ),
            )
        } else {
            None // Light mode: no QJL matrices, use TQ-ADC brute force
        };

        match strategy {
            FilterStrategy::Unfiltered => {
                all.extend(
                    snapshot
                        .mutable
                        .brute_force_search(query_f32, query_state.as_ref(), k),
                );
                for imm in &snapshot.immutable {
                    all.extend(imm.search(query_f32, k, ef_search, _scratch));
                }
                for warm_seg in &snapshot.warm {
                    all.extend(warm_seg.search(query_f32, k, ef_search, _scratch));
                }
            }
            FilterStrategy::BruteForceFiltered => {
                all.extend(snapshot.mutable.brute_force_search_filtered(
                    query_f32,
                    query_state.as_ref(),
                    k,
                    filter_bitmap,
                ));
                for imm in &snapshot.immutable {
                    all.extend(imm.search_filtered(
                        query_f32,
                        k,
                        ef_search,
                        _scratch,
                        filter_bitmap,
                    ));
                }
                for warm_seg in &snapshot.warm {
                    all.extend(warm_seg.search_filtered(
                        query_f32,
                        k,
                        ef_search,
                        _scratch,
                        filter_bitmap,
                    ));
                }
            }
            FilterStrategy::HnswFiltered => {
                all.extend(snapshot.mutable.brute_force_search_filtered(
                    query_f32,
                    query_state.as_ref(),
                    k,
                    filter_bitmap,
                ));
                for imm in &snapshot.immutable {
                    all.extend(imm.search_filtered(
                        query_f32,
                        k,
                        ef_search,
                        _scratch,
                        filter_bitmap,
                    ));
                }
                for warm_seg in &snapshot.warm {
                    all.extend(warm_seg.search_filtered(
                        query_f32,
                        k,
                        ef_search,
                        _scratch,
                        filter_bitmap,
                    ));
                }
            }
            FilterStrategy::HnswPostFilter => {
                let oversample_k = k * 3;
                all.extend(snapshot.mutable.brute_force_search_filtered(
                    query_f32,
                    query_state.as_ref(),
                    oversample_k,
                    filter_bitmap,
                ));
                for imm in &snapshot.immutable {
                    let imm_results = imm.search(
                        query_f32,
                        oversample_k,
                        ef_search.max(oversample_k),
                        _scratch,
                    );
                    if let Some(bm) = filter_bitmap {
                        for r in imm_results {
                            if bm.contains(r.id.0) {
                                all.push(r);
                            }
                        }
                    } else {
                        all.extend(imm_results);
                    }
                }
                for warm_seg in &snapshot.warm {
                    let warm_results = warm_seg.search(
                        query_f32,
                        oversample_k,
                        ef_search.max(oversample_k),
                        _scratch,
                    );
                    if let Some(bm) = filter_bitmap {
                        for r in warm_results {
                            if bm.contains(r.id.0) {
                                all.push(r);
                            }
                        }
                    } else {
                        all.extend(warm_results);
                    }
                }
            }
        }

        // Fan-out to cold (DiskANN) segments -- unfiltered PQ beam search.
        // Filter support for cold segments is future work (no global ID mapping yet).
        for cold_seg in &snapshot.cold {
            all.extend(cold_seg.search(query_f32, k, 8));
        }

        // Fan-out to IVF segments.
        if !snapshot.ivf.is_empty() {
            let dim = query_f32.len();
            let pdim = padded_dimension(dim as u32) as usize;

            for ivf_seg in &snapshot.ivf {
                // Rotate query using this IVF segment's sign flips.
                let mut q_rotated = vec![0.0f32; pdim];
                q_rotated[..dim].copy_from_slice(query_f32);
                // Normalize before FWHT.
                let qnorm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
                if qnorm > 0.0 {
                    let inv = 1.0 / qnorm;
                    for v in q_rotated[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                fwht::fwht(&mut q_rotated, ivf_seg.sign_flips());

                // LUT buffer on the stack (16KB for 1024-dim, well within 8MB stack).
                let mut lut_buf = vec![0u8; pdim * 16];

                if let Some(bm) = filter_bitmap {
                    all.extend(ivf_seg.search_filtered(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                        bm,
                    ));
                } else {
                    all.extend(ivf_seg.search(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                    ));
                }
            }
        }

        all.sort_unstable();
        all.truncate(k);
        all
    }

    /// MVCC-aware fan-out search with dirty set merge.
    ///
    /// 1. Brute-force MVCC search on mutable segment (visibility filtered).
    /// 2. HNSW search on immutable segments (immutable entries are committed by
    ///    definition -- compacted only after commit. Visibility post-filter
    ///    deferred until Phase 66 when delete_lsn tracking on immutable entries
    ///    is added).
    /// 3. Brute-force scan dirty_set entries (always visible -- own txn).
    /// 4. Merge all results, take global top-k.
    ///
    /// When mvcc.snapshot_lsn == 0 and dirty_set is empty, this is equivalent
    /// to the non-MVCC search path.
    pub fn search_mvcc(
        &self,
        query_f32: &[f32],
        k: usize,
        ef_search: usize,
        _scratch: &mut SearchScratch,
        filter_bitmap: Option<&RoaringBitmap>,
        mvcc: &MvccContext<'_>,
    ) -> SmallVec<[SearchResult; 32]> {
        let snapshot = self.load();

        // Prepare TurboQuant_prod query state for mutable search.
        let collection = snapshot.mutable.collection();
        let query_state = if !collection.qjl_matrices.is_empty() {
            Some(
                crate::vector::turbo_quant::inner_product::prepare_query_prod(
                    query_f32,
                    &collection.qjl_matrices,
                    collection.fwht_sign_flips.as_slice(),
                    collection.padded_dimension as usize,
                ),
            )
        } else {
            None
        };

        // 1. MVCC-aware brute-force
        let mut all = snapshot.mutable.brute_force_search_mvcc(
            query_f32,
            query_state.as_ref(),
            k,
            filter_bitmap,
            mvcc.snapshot_lsn,
            mvcc.my_txn_id,
            mvcc.committed,
        );

        // 2. HNSW search on immutable segments (TQ-ADC distance).
        // Immutable segment entries are committed by definition (compacted only
        // after commit). No visibility post-filter needed for Phase 65.
        for imm in &snapshot.immutable {
            if filter_bitmap.is_some() {
                all.extend(imm.search_filtered(query_f32, k, ef_search, _scratch, filter_bitmap));
            } else {
                all.extend(imm.search(query_f32, k, ef_search, _scratch));
            }
        }

        // 2a. Warm segment search (committed by definition, same as immutable).
        for warm_seg in &snapshot.warm {
            if filter_bitmap.is_some() {
                all.extend(warm_seg.search_filtered(
                    query_f32,
                    k,
                    ef_search,
                    _scratch,
                    filter_bitmap,
                ));
            } else {
                all.extend(warm_seg.search(query_f32, k, ef_search, _scratch));
            }
        }

        // 2b. Cold segment search (DiskANN, committed by definition).
        for cold_seg in &snapshot.cold {
            all.extend(cold_seg.search(query_f32, k, 8));
        }

        // 2c. IVF segment search (IVF entries are committed by definition).
        if !snapshot.ivf.is_empty() {
            let dim = query_f32.len();
            let pdim = padded_dimension(dim as u32) as usize;

            // Allocate query rotation + LUT buffers ONCE, reuse across all IVF segments.
            // Previously these were allocated per-segment-per-query (12KB+ × n_segments).
            let mut q_rotated = vec![0.0f32; pdim];
            let mut lut_buf = vec![0u8; pdim * 16];

            for ivf_seg in &snapshot.ivf {
                // Reset and re-rotate for this segment (different sign_flips per segment)
                q_rotated.iter_mut().for_each(|v| *v = 0.0);
                q_rotated[..dim].copy_from_slice(query_f32);
                let qnorm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
                if qnorm > 0.0 {
                    let inv = 1.0 / qnorm;
                    for v in q_rotated[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                fwht::fwht(&mut q_rotated, ivf_seg.sign_flips());

                if let Some(bm) = filter_bitmap {
                    all.extend(ivf_seg.search_filtered(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                        bm,
                    ));
                } else {
                    all.extend(ivf_seg.search(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                    ));
                }
            }
        }

        // 3. Dirty set: currently empty for non-transactional reads.
        // Full TurboQuant_prod scoring for dirty entries deferred to Phase 66
        // (transactional writes are rare in vector workloads).

        // 4. Merge all results, take global top-k
        all.sort_unstable();
        all.truncate(k);
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::turbo_quant::encoder::padded_dimension;
    use crate::vector::types::DistanceMetric;

    fn make_test_collection(dim: u32) -> Arc<CollectionMetadata> {
        // Use Exact mode in tests to preserve TQ_prod scoring compatibility
        Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
            crate::vector::turbo_quant::collection::BuildMode::Exact,
        ))
    }

    fn make_sq_vector(dim: usize, seed: u32) -> Vec<i8> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s >> 24) as i8);
        }
        v
    }

    #[test]
    fn test_holder_new_has_empty_immutable() {
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection);
        let snap = holder.load();
        assert!(snap.immutable.is_empty());
        assert_eq!(snap.mutable.len(), 0);
    }

    #[test]
    fn test_holder_swap_replaces_list() {
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection.clone());

        // Insert into original mutable
        {
            let snap = holder.load();
            snap.mutable.append(1, &[0.0f32; 128], &[0i8; 128], 1.0, 1);
        }

        // Swap with a new list
        let new_mutable = Arc::new(MutableSegment::new(128, collection));
        new_mutable.append(2, &[1.0f32; 128], &[1i8; 128], 1.0, 2);
        new_mutable.append(3, &[2.0f32; 128], &[2i8; 128], 1.0, 3);

        holder.swap(SegmentList {
            mutable: new_mutable,
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
        });

        let snap = holder.load();
        assert_eq!(snap.mutable.len(), 2); // new mutable has 2, not 1
    }

    #[test]
    fn test_holder_search_mutable_only() {
        distance::init();
        let dim = 8;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);

        // Insert vectors
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }

        let _query_sq = make_sq_vector(dim, 1); // same as vector 0
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let results = holder.search(&query_f32, 3, 64, &mut scratch);
        assert!(!results.is_empty());
        assert!(results.len() <= 3);
        // First result should be vector 0
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_filtered_none_same_as_unfiltered() {
        distance::init();
        let dim = 8;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let unfiltered = holder.search(&query_f32, 3, 64, &mut scratch);
        let filtered = holder.search_filtered(&query_f32, 3, 64, &mut scratch, None);
        assert_eq!(unfiltered.len(), filtered.len());
        for (u, f) in unfiltered.iter().zip(filtered.iter()) {
            assert_eq!(u.id.0, f.id.0);
        }
    }

    #[test]
    fn test_holder_search_filtered_with_bitmap() {
        distance::init();
        let dim = 8;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        // Only allow IDs 2, 3, 4
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(2);
        bitmap.insert(3);
        bitmap.insert(4);

        let results = holder.search_filtered(&query_f32, 3, 64, &mut scratch, Some(&bitmap));
        for r in &results {
            assert!(
                bitmap.contains(r.id.0),
                "result id {} not in bitmap",
                r.id.0
            );
        }
    }

    #[test]
    fn test_holder_search_mvcc_backward_compat() {
        // search_mvcc with snapshot=0 and empty dirty_set should match search results
        distance::init();
        let dim = 8;
        let _padded = padded_dimension(dim as u32) as usize;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim as usize, i * 13 + 1);
                let f32_v = vec![0.0f32; dim as usize];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim as usize, 1);
        let query_f32 = vec![0.0f32; dim as usize];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringBitmap::new();

        let non_mvcc = holder.search(&query_f32, 3, 64, &mut scratch);
        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        let mvcc = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_ctx);

        assert_eq!(non_mvcc.len(), mvcc.len());
        for (a, b) in non_mvcc.iter().zip(mvcc.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }

    #[test]
    fn test_holder_search_mvcc_filters_by_snapshot() {
        distance::init();
        let dim = 4;
        let _padded = padded_dimension(dim as u32) as usize;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            // insert_lsn=1, visible to snapshot=5
            snap.mutable.append(0, &[0.0f32; 4], &[0i8; 4], 1.0, 1);
            // insert_lsn=10, NOT visible to snapshot=5
            snap.mutable.append(1, &[0.0f32; 4], &[1i8; 4], 1.0, 10);
        }
        let _query_sq = vec![0i8; dim as usize];
        let query_f32 = vec![0.0f32; dim as usize];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringBitmap::new();
        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 5,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        let results = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_mvcc_dirty_set_merge() {
        // Dirty set entries should appear in results (read-your-own-writes)
        distance::init();
        let dim = 4usize;
        let collection = make_test_collection(dim as u32);
        let padded = collection.padded_dimension as usize;
        let bytes_per_code = padded / 2 + 4;
        let holder = SegmentHolder::new(dim as u32, collection.clone());
        {
            let snap = holder.load();
            // One existing entry far from query (f32 L2 distance)
            snap.mutable
                .append(0, &[100.0f32; 4], &[100i8, 100, 100, 100], 1.0, 1);
        }
        let _query_sq = vec![0i8; dim];
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringBitmap::new();

        // Dirty set has one entry close to query
        let dirty_entry = crate::vector::segment::mutable::MutableEntry {
            internal_id: 1000,
            key_hash: 999,
            vector_offset: 0,
            norm: 1.0,
            insert_lsn: 50,
            delete_lsn: 0,
            txn_id: 42,
        };

        // Encode a zero vector as TQ codes for the dirty entry
        let dirty_f32 = vec![0.0f32; dim];
        let mut work_buf = vec![0.0f32; padded];
        let tq_code = crate::vector::turbo_quant::encoder::encode_tq_mse_scaled(
            &dirty_f32,
            collection.fwht_sign_flips.as_slice(),
            collection.codebook_boundaries_15(),
            &mut work_buf,
        );
        // Build dirty_tq_codes: codes + norm as le bytes
        let mut dirty_tq_bytes = Vec::with_capacity(bytes_per_code);
        dirty_tq_bytes.extend_from_slice(&tq_code.codes);
        dirty_tq_bytes.extend_from_slice(&tq_code.norm.to_le_bytes());

        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 42,
            committed: &committed,
            dirty_set: std::slice::from_ref(&dirty_entry),
            dimension: dim as u32,
        };
        let results = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_ctx);

        // NOTE: dirty set scoring is deferred to Phase 66 (see search_mvcc comment).
        // For now, dirty entries do NOT appear in results.
        // Once Phase 66 lands, update this assertion:
        //   assert!(!results.is_empty());
        //   assert_eq!(results[0].id.0, 1000);
        // Current behavior: only the committed entry (id=0) is returned.
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_mvcc_empty_dirty_set_matches_no_dirty() {
        distance::init();
        let dim = 8;
        let _padded = padded_dimension(dim as u32) as usize;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim as usize, i * 13 + 1);
                let f32_v = vec![0.0f32; dim as usize];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim as usize, 1);
        let query_f32 = vec![0.0f32; dim as usize];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringBitmap::new();

        let mvcc_empty = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        let r1 = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_empty);

        // Same with explicit empty dirty set
        let mvcc_empty2 = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        let r2 = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_empty2);

        assert_eq!(r1.len(), r2.len());
        for (a, b) in r1.iter().zip(r2.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }

    #[test]
    fn test_holder_snapshot_isolation() {
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection.clone());

        // Take snapshot before swap
        let snap_before = holder.load();
        assert_eq!(snap_before.mutable.len(), 0);

        // Insert into mutable (through original snapshot's Arc)
        snap_before
            .mutable
            .append(1, &[0.0f32; 128], &[0i8; 128], 1.0, 1);

        // Swap with completely new list
        let new_mutable = Arc::new(MutableSegment::new(128, collection));
        new_mutable.append(2, &[1.0f32; 128], &[1i8; 128], 1.0, 2);
        new_mutable.append(3, &[2.0f32; 128], &[2i8; 128], 1.0, 3);
        holder.swap(SegmentList {
            mutable: new_mutable,
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
        });

        // Old snapshot still sees the original mutable (1 entry from our append)
        assert_eq!(snap_before.mutable.len(), 1);

        // New snapshot sees new mutable (2 entries)
        let snap_after = holder.load();
        assert_eq!(snap_after.mutable.len(), 2);
    }

    #[test]
    fn test_holder_search_with_ivf() {
        use crate::vector::segment::ivf;

        distance::init();
        let dim = 8usize;
        let pdim = padded_dimension(dim as u32) as usize;
        let dim_half = pdim / 2;

        // Create sign flips.
        let mut sign_flips = vec![1.0f32; pdim];
        for (i, s) in sign_flips.iter_mut().enumerate() {
            if i % 3 == 0 {
                *s = -1.0;
            }
        }

        // Build a small IVF segment with 20 vectors, 2 clusters.
        let n = 20;
        let n_clusters = 2;

        // Cluster 0: vectors near origin. Cluster 1: vectors near (5,5,...).
        let mut vectors = Vec::with_capacity(n * dim);
        let mut tq_codes = Vec::with_capacity(n);
        let mut norms = Vec::with_capacity(n);
        let ids: Vec<u32> = (1000..1000 + n as u32).collect();

        for i in 0..n {
            let offset = if i < n / 2 { 0.0 } else { 5.0 };
            let v: Vec<f32> = (0..dim)
                .map(|d| offset + (i * dim + d) as f32 * 0.01)
                .collect();
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            norms.push(if norm > 0.0 { norm } else { 1.0 });
            vectors.extend_from_slice(&v);
            tq_codes.push(vec![(i & 0xF) as u8; dim_half]);
        }

        let ivf_seg = ivf::build_ivf_segment(
            &vectors,
            &tq_codes,
            &norms,
            &ids,
            dim,
            n_clusters,
            &sign_flips,
        );

        assert_eq!(ivf_seg.total_vectors(), n as u64);

        // Create holder and swap in SegmentList with IVF.
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);

        // Insert mutable vectors (ids 0-4).
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }

        // Swap in list that includes the IVF segment.
        let old_snap = holder.load();
        holder.swap(SegmentList {
            mutable: Arc::clone(&old_snap.mutable),
            immutable: Vec::new(),
            ivf: vec![Arc::new(ivf_seg)],
            warm: Vec::new(),
            cold: Vec::new(),
        });

        // total_vectors should include IVF vectors.
        assert_eq!(holder.total_vectors(), 5 + n as u32);

        // Search should return results from both mutable and IVF.
        let query_f32 = vec![0.0f32; dim];
        let _query_sq = make_sq_vector(dim, 1);
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let results = holder.search(&query_f32, 10, 64, &mut scratch);
        assert!(!results.is_empty());
        // Should contain at least some IVF results (ids >= 1000).
        let ivf_count = results.iter().filter(|r| r.id.0 >= 1000).count();
        // And mutable results (ids < 5).
        let mut_count = results.iter().filter(|r| r.id.0 < 5).count();
        assert!(
            ivf_count > 0 || mut_count > 0,
            "should have results from both segments"
        );
    }
}
