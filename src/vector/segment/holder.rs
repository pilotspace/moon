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
use crate::vector::types::SearchResult;

use super::immutable::ImmutableSegment;
use super::mutable::MutableSegment;

/// Snapshot of all segments at a point in time.
pub struct SegmentList {
    pub mutable: Arc<MutableSegment>,
    pub immutable: Vec<Arc<ImmutableSegment>>,
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

    /// Total vector count across mutable + all immutable segments.
    pub fn total_vectors(&self) -> u32 {
        let snapshot = self.load();
        let mut total = snapshot.mutable.len() as u32;
        for imm in &snapshot.immutable {
            total += imm.total_count();
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

        match strategy {
            FilterStrategy::Unfiltered => {
                // Existing path -- no bitmap
                let mut all = snapshot.mutable.brute_force_search(query_sq, k);
                for imm in &snapshot.immutable {
                    all.extend(imm.search(query_f32, k, ef_search, scratch));
                }
                all.sort();
                all.truncate(k);
                all
            }
            FilterStrategy::BruteForceFiltered => {
                // Linear scan on mutable + immutable -- bitmap narrows to few vectors
                let mut all = snapshot
                    .mutable
                    .brute_force_search_filtered(query_sq, k, filter_bitmap);
                // Immutable segments: use HNSW filtered (still correct, bitmap handles it)
                for imm in &snapshot.immutable {
                    all.extend(imm.search_filtered(
                        query_f32,
                        k,
                        ef_search,
                        scratch,
                        filter_bitmap,
                    ));
                }
                all.sort();
                all.truncate(k);
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
                all.sort();
                all.truncate(k);
                all
            }
            FilterStrategy::HnswPostFilter => {
                // 3x oversampling then post-filter
                let oversample_k = k * 3;
                let mut all = snapshot
                    .mutable
                    .brute_force_search_filtered(query_sq, oversample_k, filter_bitmap);
                for imm in &snapshot.immutable {
                    // Search with 3x k, no filter in HNSW, filter results after
                    let imm_results = imm.search(
                        query_f32,
                        oversample_k,
                        ef_search.max(oversample_k),
                        scratch,
                    );
                    // Post-filter
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
                all.sort();
                all.truncate(k);
                all
            }
        }
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
        let mvcc_ctx = super::holder::MvccContext {
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
        let mvcc_ctx = super::holder::MvccContext {
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
        let dirty_entry = super::mutable::MutableEntry {
            internal_id: 1000,
            key_hash: 999,
            vector_offset: 0,
            norm: 1.0,
            insert_lsn: 50,
            delete_lsn: 0,
            txn_id: 42,
        };
        let dirty_sq = vec![0i8; dim]; // identical to query -> distance 0

        let mvcc_ctx = super::holder::MvccContext {
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

        let mvcc_empty = super::holder::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dirty_vectors_sq: &[],
            dimension: dim as u32,
        };
        let r1 = holder.search_mvcc(&query_f32, &query_sq, 3, 64, &mut scratch, None, &mvcc_empty);

        // Same with explicit empty dirty set
        let mvcc_empty2 = super::holder::MvccContext {
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
        });

        // Old snapshot still sees the original mutable (1 entry from our append)
        assert_eq!(snap_before.mutable.len(), 1);

        // New snapshot sees new mutable (2 entries)
        let snap_after = holder.load();
        assert_eq!(snap_after.mutable.len(), 2);
    }
}
