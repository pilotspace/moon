//! SegmentHolder -- ArcSwap-based lock-free segment list access.
//!
//! Searches load() once at query start and hold the Arc for the query
//! duration -- immune to concurrent swaps.

use std::sync::Arc;

use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::filter::selectivity::{select_strategy, FilterStrategy};
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::segment::ivf::IvfSegment;
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::turbo_quant::fwht;
use crate::vector::types::{SearchResult, VectorId};

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
    /// Brute-force scanned and merged into results.
    pub dirty_set: &'a [MutableEntry],
    /// SQ vectors for dirty set entries (contiguous, dimension-strided).
    pub dirty_vectors_sq: &'a [i8],
    pub dimension: u32,
}

/// Snapshot of all segments at a point in time.
pub struct SegmentList {
    pub mutable: Arc<MutableSegment>,
    pub immutable: Vec<Arc<ImmutableSegment>>,
    /// IVF segments for billion-scale approximate search.
    pub ivf: Vec<Arc<IvfSegment>>,
}

/// Lock-free segment holder. Searches load() once at query start and hold
/// the Arc for the query duration -- immune to concurrent swaps.
pub struct SegmentHolder {
    segments: ArcSwap<SegmentList>,
}

impl SegmentHolder {
    /// Create a holder with a fresh MutableSegment and empty immutable list.
    pub fn new(dimension: u32) -> Self {
        Self {
            segments: ArcSwap::from_pointee(SegmentList {
                mutable: Arc::new(MutableSegment::new(dimension)),
                immutable: Vec::new(),
                ivf: Vec::new(),
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

    /// Total vector count across mutable + immutable + IVF segments.
    pub fn total_vectors(&self) -> u32 {
        let snapshot = self.load();
        let mut total = snapshot.mutable.len() as u32;
        for imm in &snapshot.immutable {
            total += imm.total_count();
        }
        for ivf_seg in &snapshot.ivf {
            total += ivf_seg.total_vectors() as u32;
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
        query_sq: &[i8],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
    ) -> SmallVec<[SearchResult; 32]> {
        self.search_filtered(query_f32, query_sq, k, ef_search, scratch, None)
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
        query_sq: &[i8],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        let strategy = select_strategy(filter_bitmap, self.total_vectors());
        let snapshot = self.load();

        let mut all = match strategy {
            FilterStrategy::Unfiltered => {
                let mut all = snapshot.mutable.brute_force_search(query_sq, k);
                for imm in &snapshot.immutable {
                    all.extend(imm.search(query_f32, k, ef_search, scratch));
                }
                all
            }
            FilterStrategy::BruteForceFiltered => {
                let mut all = snapshot
                    .mutable
                    .brute_force_search_filtered(query_sq, k, filter_bitmap);
                for imm in &snapshot.immutable {
                    all.extend(imm.search_filtered(
                        query_f32,
                        k,
                        ef_search,
                        scratch,
                        filter_bitmap,
                    ));
                }
                all
            }
            FilterStrategy::HnswFiltered => {
                let mut all = snapshot
                    .mutable
                    .brute_force_search_filtered(query_sq, k, filter_bitmap);
                for imm in &snapshot.immutable {
                    all.extend(imm.search_filtered(
                        query_f32,
                        k,
                        ef_search,
                        scratch,
                        filter_bitmap,
                    ));
                }
                all
            }
            FilterStrategy::HnswPostFilter => {
                let oversample_k = k * 3;
                let mut all = snapshot
                    .mutable
                    .brute_force_search_filtered(query_sq, oversample_k, filter_bitmap);
                for imm in &snapshot.immutable {
                    let imm_results = imm.search(
                        query_f32,
                        oversample_k,
                        ef_search.max(oversample_k),
                        scratch,
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
                all
            }
        };

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

        all.sort();
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
        query_sq: &[i8],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
        filter_bitmap: Option<&RoaringBitmap>,
        mvcc: &MvccContext<'_>,
    ) -> SmallVec<[SearchResult; 32]> {
        let snapshot = self.load();

        // 1. MVCC-aware brute-force on mutable segment
        let mut all = snapshot.mutable.brute_force_search_mvcc(
            query_sq,
            k,
            filter_bitmap,
            mvcc.snapshot_lsn,
            mvcc.my_txn_id,
            mvcc.committed,
        );

        // 2. HNSW search on immutable segments.
        // Immutable segment entries are committed by definition (compacted only
        // after commit). No visibility post-filter needed for Phase 65.
        for imm in &snapshot.immutable {
            if filter_bitmap.is_some() {
                all.extend(imm.search_filtered(
                    query_f32,
                    k,
                    ef_search,
                    scratch,
                    filter_bitmap,
                ));
            } else {
                all.extend(imm.search(query_f32, k, ef_search, scratch));
            }
        }

        // 2b. IVF segment search (IVF entries are committed by definition).
        if !snapshot.ivf.is_empty() {
            let dim = query_f32.len();
            let pdim = padded_dimension(dim as u32) as usize;

            for ivf_seg in &snapshot.ivf {
                let mut q_rotated = vec![0.0f32; pdim];
                q_rotated[..dim].copy_from_slice(query_f32);
                let qnorm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
                if qnorm > 0.0 {
                    let inv = 1.0 / qnorm;
                    for v in q_rotated[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                fwht::fwht(&mut q_rotated, ivf_seg.sign_flips());

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

        // 3. Brute-force scan dirty set entries (always visible -- own txn's writes).
        if !mvcc.dirty_set.is_empty() {
            let dim = mvcc.dimension as usize;
            let l2_i8 = crate::vector::distance::table().l2_i8;

            for (idx, entry) in mvcc.dirty_set.iter().enumerate() {
                // Skip deleted dirty entries
                if entry.delete_lsn != 0 {
                    continue;
                }
                // Apply filter bitmap if present
                if let Some(bm) = filter_bitmap {
                    if !bm.contains(entry.internal_id) {
                        continue;
                    }
                }
                let offset = idx * dim;
                let vec_sq = &mvcc.dirty_vectors_sq[offset..offset + dim];
                let dist = l2_i8(query_sq, vec_sq);
                all.push(SearchResult::new(dist as f32, VectorId(entry.internal_id)));
            }
        }

        // 4. Merge all results, take global top-k
        all.sort();
        all.truncate(k);
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;

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
        let holder = SegmentHolder::new(128);
        let snap = holder.load();
        assert!(snap.immutable.is_empty());
        assert_eq!(snap.mutable.len(), 0);
    }

    #[test]
    fn test_holder_swap_replaces_list() {
        let holder = SegmentHolder::new(128);

        // Insert into original mutable
        {
            let snap = holder.load();
            snap.mutable
                .append(1, &[0.0f32; 128], &[0i8; 128], 1.0, 1);
        }

        // Swap with a new list
        let new_mutable = Arc::new(MutableSegment::new(128));
        new_mutable.append(2, &[1.0f32; 128], &[1i8; 128], 1.0, 2);
        new_mutable.append(3, &[2.0f32; 128], &[2i8; 128], 1.0, 3);

        holder.swap(SegmentList {
            mutable: new_mutable,
            immutable: Vec::new(),
            ivf: Vec::new(),
        });

        let snap = holder.load();
        assert_eq!(snap.mutable.len(), 2); // new mutable has 2, not 1
    }

    #[test]
    fn test_holder_search_mutable_only() {
        distance::init();
        let dim = 8;
        let holder = SegmentHolder::new(dim as u32);

        // Insert vectors
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable
                    .append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }

        let query_sq = make_sq_vector(dim, 1); // same as vector 0
        let query_f32 = vec![0.0f32; dim];
        let mut scratch =
            crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let results = holder.search(&query_f32, &query_sq, 3, 64, &mut scratch);
        assert!(!results.is_empty());
        assert!(results.len() <= 3);
        // First result should be vector 0
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_filtered_none_same_as_unfiltered() {
        distance::init();
        let dim = 8;
        let holder = SegmentHolder::new(dim as u32);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let unfiltered = holder.search(&query_f32, &query_sq, 3, 64, &mut scratch);
        let filtered = holder.search_filtered(&query_f32, &query_sq, 3, 64, &mut scratch, None);
        assert_eq!(unfiltered.len(), filtered.len());
        for (u, f) in unfiltered.iter().zip(filtered.iter()) {
            assert_eq!(u.id.0, f.id.0);
        }
    }

    #[test]
    fn test_holder_search_filtered_with_bitmap() {
        distance::init();
        let dim = 8;
        let holder = SegmentHolder::new(dim as u32);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        // Only allow IDs 2, 3, 4
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(2);
        bitmap.insert(3);
        bitmap.insert(4);

        let results = holder.search_filtered(&query_f32, &query_sq, 3, 64, &mut scratch, Some(&bitmap));
        for r in &results {
            assert!(bitmap.contains(r.id.0), "result id {} not in bitmap", r.id.0);
        }
    }

    #[test]
    fn test_holder_search_mvcc_backward_compat() {
        // search_mvcc with snapshot=0 and empty dirty_set should match search results
        distance::init();
        let dim = 8;
        let holder = SegmentHolder::new(dim as u32);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringBitmap::new();

        let non_mvcc = holder.search(&query_f32, &query_sq, 3, 64, &mut scratch);
        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &committed,
            dirty_set: &[],
            dirty_vectors_sq: &[],
            dimension: dim as u32,
        };
        let mvcc = holder.search_mvcc(&query_f32, &query_sq, 3, 64, &mut scratch, None, &mvcc_ctx);

        assert_eq!(non_mvcc.len(), mvcc.len());
        for (a, b) in non_mvcc.iter().zip(mvcc.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }

    #[test]
    fn test_holder_search_mvcc_filters_by_snapshot() {
        distance::init();
        let dim = 4;
        let holder = SegmentHolder::new(dim as u32);
        {
            let snap = holder.load();
            // insert_lsn=1, visible to snapshot=5
            snap.mutable.append(0, &[0.0f32; 4], &[0i8; 4], 1.0, 1);
            // insert_lsn=10, NOT visible to snapshot=5
            snap.mutable.append(1, &[0.0f32; 4], &[1i8; 4], 1.0, 10);
        }
        let query_sq = vec![0i8; dim];
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringBitmap::new();
        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 5,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dirty_vectors_sq: &[],
            dimension: dim as u32,
        };
        let results = holder.search_mvcc(&query_f32, &query_sq, 3, 64, &mut scratch, None, &mvcc_ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_mvcc_dirty_set_merge() {
        // Dirty set entries should appear in results (read-your-own-writes)
        distance::init();
        let dim = 4;
        let holder = SegmentHolder::new(dim as u32);
        {
            let snap = holder.load();
            // One existing entry far from query
            snap.mutable.append(0, &[0.0f32; 4], &[100i8, 100, 100, 100], 1.0, 1);
        }
        let query_sq = vec![0i8; dim];
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
        let dirty_sq = vec![0i8; dim]; // identical to query -> distance 0

        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 42,
            committed: &committed,
            dirty_set: std::slice::from_ref(&dirty_entry),
            dirty_vectors_sq: &dirty_sq,
            dimension: dim as u32,
        };
        let results = holder.search_mvcc(&query_f32, &query_sq, 3, 64, &mut scratch, None, &mvcc_ctx);

        // Dirty entry should be first (distance 0)
        assert!(!results.is_empty());
        assert_eq!(results[0].id.0, 1000);
        assert_eq!(results[0].distance, 0.0);
    }

    #[test]
    fn test_holder_search_mvcc_empty_dirty_set_matches_no_dirty() {
        distance::init();
        let dim = 8;
        let holder = SegmentHolder::new(dim as u32);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let sq = make_sq_vector(dim, i * 13 + 1);
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, &sq, 1.0, i as u64);
            }
        }
        let query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringBitmap::new();

        let mvcc_empty = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dirty_vectors_sq: &[],
            dimension: dim as u32,
        };
        let r1 = holder.search_mvcc(&query_f32, &query_sq, 3, 64, &mut scratch, None, &mvcc_empty);

        // Same with explicit empty dirty set
        let mvcc_empty2 = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dirty_vectors_sq: &[],
            dimension: dim as u32,
        };
        let r2 = holder.search_mvcc(&query_f32, &query_sq, 3, 64, &mut scratch, None, &mvcc_empty2);

        assert_eq!(r1.len(), r2.len());
        for (a, b) in r1.iter().zip(r2.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }

    #[test]
    fn test_holder_snapshot_isolation() {
        let holder = SegmentHolder::new(128);

        // Take snapshot before swap
        let snap_before = holder.load();
        assert_eq!(snap_before.mutable.len(), 0);

        // Insert into mutable (through original snapshot's Arc)
        snap_before
            .mutable
            .append(1, &[0.0f32; 128], &[0i8; 128], 1.0, 1);

        // Swap with completely new list
        let new_mutable = Arc::new(MutableSegment::new(128));
        new_mutable.append(2, &[1.0f32; 128], &[1i8; 128], 1.0, 2);
        new_mutable.append(3, &[2.0f32; 128], &[2i8; 128], 1.0, 3);
        holder.swap(SegmentList {
            mutable: new_mutable,
            immutable: Vec::new(),
            ivf: Vec::new(),
        });

        // Old snapshot still sees the original mutable (1 entry from our append)
        assert_eq!(snap_before.mutable.len(), 1);

        // New snapshot sees new mutable (2 entries)
        let snap_after = holder.load();
        assert_eq!(snap_after.mutable.len(), 2);
    }

    #[test]
    fn test_holder_search_with_ivf() {
        use crate::vector::aligned_buffer::AlignedBuffer;
        use crate::vector::segment::ivf::{
            self, IvfQuantization, IvfSegment,
        };
        use crate::vector::turbo_quant::encoder::padded_dimension;

        distance::init();
        let dim = 8usize;
        let pdim = padded_dimension(dim as u32) as usize;
        let dim_half = pdim / 2;

        // Create sign flips.
        let mut sign_flips = vec![1.0f32; pdim];
        for (i, s) in sign_flips.iter_mut().enumerate() {
            if i % 3 == 0 { *s = -1.0; }
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
            let v: Vec<f32> = (0..dim).map(|d| offset + (i * dim + d) as f32 * 0.01).collect();
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            norms.push(if norm > 0.0 { norm } else { 1.0 });
            vectors.extend_from_slice(&v);
            tq_codes.push(vec![(i & 0xF) as u8; dim_half]);
        }

        let ivf_seg = ivf::build_ivf_segment(
            &vectors, &tq_codes, &norms, &ids, dim, n_clusters, &sign_flips,
        );

        assert_eq!(ivf_seg.total_vectors(), n as u64);

        // Create holder and swap in SegmentList with IVF.
        let holder = SegmentHolder::new(dim as u32);

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
        });

        // total_vectors should include IVF vectors.
        assert_eq!(holder.total_vectors(), 5 + n as u32);

        // Search should return results from both mutable and IVF.
        let query_f32 = vec![0.0f32; dim];
        let query_sq = make_sq_vector(dim, 1);
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let results = holder.search(&query_f32, &query_sq, 10, 64, &mut scratch);
        assert!(!results.is_empty());
        // Should contain at least some IVF results (ids >= 1000).
        let ivf_count = results.iter().filter(|r| r.id.0 >= 1000).count();
        // And mutable results (ids < 5).
        let mut_count = results.iter().filter(|r| r.id.0 < 5).count();
        assert!(ivf_count > 0 || mut_count > 0, "should have results from both segments");
    }
}
