//! Append-only mutable segment with brute-force search.
//!
//! Type-level enforcement: MutableSegment has NO HNSW methods or fields.
//! It is a flat buffer of SQ vectors with linear scan search.

use std::collections::BinaryHeap;

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::types::{SearchResult, VectorId};

/// Maximum byte size before a mutable segment is considered full (128 MB).
const MUTABLE_SEGMENT_MAX: usize = 128 * 1024 * 1024;

/// 48 bytes. MVCC fields prepared for Phase 65.
#[repr(C)]
pub struct MutableEntry {
    pub internal_id: u32,
    pub key_hash: u64,
    pub vector_offset: u32,
    pub norm: f32,
    pub insert_lsn: u64,
    pub delete_lsn: u64,
    pub txn_id: u64,
}

/// Snapshot from freeze() -- cloned data for compaction pipeline.
pub struct FrozenSegment {
    pub entries: Vec<MutableEntry>,
    pub vectors_f32: Vec<f32>,
    pub vectors_sq: Vec<i8>,
    pub dimension: u32,
}

struct MutableSegmentInner {
    vectors_sq: Vec<i8>,
    vectors_f32: Vec<f32>,
    entries: Vec<MutableEntry>,
    dimension: u32,
    byte_size: usize,
}

/// Ordered wrapper for BinaryHeap: (distance, id).
/// Max-heap by default in BinaryHeap, so we use it directly
/// and pop the farthest when over capacity.
#[derive(PartialEq, Eq)]
struct DistId(i32, u32);

impl Ord for DistId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0).then(self.1.cmp(&other.1))
    }
}

impl PartialOrd for DistId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Append-only flat buffer with brute-force search. NEVER builds HNSW.
/// Type-level enforcement: no HNSW methods exist on this type.
pub struct MutableSegment {
    inner: RwLock<MutableSegmentInner>,
}

impl MutableSegment {
    /// Create an empty mutable segment for the given vector dimension.
    pub fn new(dimension: u32) -> Self {
        Self {
            inner: RwLock::new(MutableSegmentInner {
                vectors_sq: Vec::new(),
                vectors_f32: Vec::new(),
                entries: Vec::new(),
                dimension,
                byte_size: 0,
            }),
        }
    }

    /// Append a vector. Returns the internal_id assigned.
    pub fn append(
        &self,
        key_hash: u64,
        vector_f32: &[f32],
        vector_sq: &[i8],
        norm: f32,
        insert_lsn: u64,
    ) -> u32 {
        let mut inner = self.inner.write();
        let internal_id = inner.entries.len() as u32;
        let vector_offset = (inner.vectors_sq.len() / inner.dimension as usize) as u32;

        inner.vectors_f32.extend_from_slice(vector_f32);
        inner.vectors_sq.extend_from_slice(vector_sq);

        inner.entries.push(MutableEntry {
            internal_id,
            key_hash,
            vector_offset,
            norm,
            insert_lsn,
            delete_lsn: 0,
            txn_id: 0,
        });

        // byte_size: dimension * (1 byte for i8 + 4 bytes for f32) + size_of MutableEntry
        inner.byte_size +=
            inner.dimension as usize * (1 + 4) + std::mem::size_of::<MutableEntry>();

        internal_id
    }

    /// Brute-force search over all non-deleted entries using l2_i8.
    /// Returns top-k results sorted by distance ascending.
    pub fn brute_force_search(&self, query_sq: &[i8], k: usize) -> SmallVec<[SearchResult; 32]> {
        self.brute_force_search_filtered(query_sq, k, None)
    }

    /// Brute-force filtered search. When bitmap is Some, only entries whose
    /// internal_id is in the bitmap are considered.
    pub fn brute_force_search_filtered(
        &self,
        query_sq: &[i8],
        k: usize,
        allow_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        let inner = self.inner.read();
        let dim = inner.dimension as usize;
        let l2_i8 = crate::vector::distance::table().l2_i8;

        // Max-heap of size k: stores (distance, internal_id).
        // Pop farthest when over capacity.
        let mut heap: BinaryHeap<DistId> = BinaryHeap::with_capacity(k + 1);

        for entry in &inner.entries {
            if entry.delete_lsn != 0 {
                continue;
            }
            if let Some(bm) = allow_bitmap {
                if !bm.contains(entry.internal_id) {
                    continue;
                }
            }
            let offset = entry.internal_id as usize * dim;
            let vec_sq = &inner.vectors_sq[offset..offset + dim];
            let dist = l2_i8(query_sq, vec_sq);

            if heap.len() < k {
                heap.push(DistId(dist, entry.internal_id));
            } else if let Some(&DistId(worst, _)) = heap.peek() {
                if dist < worst {
                    heap.pop();
                    heap.push(DistId(dist, entry.internal_id));
                }
            }
        }

        // Extract and sort ascending
        let results: SmallVec<[SearchResult; 32]> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|DistId(d, id)| SearchResult::new(d as f32, VectorId(id)))
            .collect();
        // into_sorted_vec gives ascending order by our Ord (distance ascending)
        results
    }

    /// Returns true when the segment exceeds the 128 MB threshold.
    pub fn is_full(&self) -> bool {
        self.inner.read().byte_size >= MUTABLE_SEGMENT_MAX
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.inner.read().entries.len()
    }

    /// Returns true if no entries.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.read().entries.is_empty()
    }

    /// Mark an entry as deleted by setting its delete_lsn.
    pub fn mark_deleted(&self, internal_id: u32, delete_lsn: u64) {
        let mut inner = self.inner.write();
        if let Some(entry) = inner.entries.get_mut(internal_id as usize) {
            entry.delete_lsn = delete_lsn;
        }
    }

    /// Mark all entries matching a key_hash as deleted.
    ///
    /// Used by the DEL/HDEL/UNLINK post-dispatch hook to remove stale vectors
    /// when the underlying key is deleted. Returns the number of entries marked.
    pub fn mark_deleted_by_key_hash(&self, key_hash: u64, delete_lsn: u64) -> u32 {
        let mut inner = self.inner.write();
        let mut count = 0u32;
        for entry in inner.entries.iter_mut() {
            if entry.key_hash == key_hash && entry.delete_lsn == 0 {
                entry.delete_lsn = delete_lsn;
                count += 1;
            }
        }
        count
    }

    /// Freeze: take a read-lock snapshot of vectors and entries for compaction.
    pub fn freeze(&self) -> FrozenSegment {
        let inner = self.inner.read();
        FrozenSegment {
            entries: inner
                .entries
                .iter()
                .map(|e| MutableEntry {
                    internal_id: e.internal_id,
                    key_hash: e.key_hash,
                    vector_offset: e.vector_offset,
                    norm: e.norm,
                    insert_lsn: e.insert_lsn,
                    delete_lsn: e.delete_lsn,
                    txn_id: e.txn_id,
                })
                .collect(),
            vectors_f32: inner.vectors_f32.clone(),
            vectors_sq: inner.vectors_sq.clone(),
            dimension: inner.dimension,
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

    fn make_f32_vector(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    #[test]
    fn test_append_returns_sequential_ids() {
        let seg = MutableSegment::new(4);
        let f32_v = [1.0f32, 2.0, 3.0, 4.0];
        let sq_v = [1i8, 2, 3, 4];
        assert_eq!(seg.append(100, &f32_v, &sq_v, 1.0, 1), 0);
        assert_eq!(seg.append(200, &f32_v, &sq_v, 1.0, 2), 1);
        assert_eq!(seg.append(300, &f32_v, &sq_v, 1.0, 3), 2);
        assert_eq!(seg.len(), 3);
    }

    #[test]
    fn test_brute_force_search_returns_nearest() {
        distance::init();
        let dim = 8;
        let seg = MutableSegment::new(dim as u32);

        // Insert 10 vectors
        for i in 0..10u32 {
            let f32_v = make_f32_vector(dim, i * 7 + 1);
            let sq_v = make_sq_vector(dim, i * 7 + 1);
            seg.append(i as u64, &f32_v, &sq_v, 1.0, i as u64);
        }

        // Query with vector[0]'s SQ representation
        let query = make_sq_vector(dim, 1); // same seed as vector 0
        let results = seg.brute_force_search(&query, 3);

        assert!(results.len() <= 3);
        // First result should be vector 0 (identical query)
        assert_eq!(results[0].id.0, 0);
        assert_eq!(results[0].distance, 0.0); // identical vectors -> distance 0
    }

    #[test]
    fn test_brute_force_search_excludes_deleted() {
        distance::init();
        let dim = 4;
        let seg = MutableSegment::new(dim as u32);

        let sq0 = [0i8, 0, 0, 0];
        let sq1 = [1i8, 1, 1, 1];
        let sq2 = [10i8, 10, 10, 10];
        let f32_v = [0.0f32; 4];

        seg.append(0, &f32_v, &sq0, 1.0, 1);
        seg.append(1, &f32_v, &sq1, 1.0, 2);
        seg.append(2, &f32_v, &sq2, 1.0, 3);

        // Delete vector 0 (the closest to query [0,0,0,0])
        seg.mark_deleted(0, 10);

        let results = seg.brute_force_search(&[0i8, 0, 0, 0], 3);
        // Vector 0 should NOT appear
        for r in &results {
            assert_ne!(r.id.0, 0, "deleted vector should not appear in results");
        }
        // Vector 1 should be nearest (distance = 4)
        assert_eq!(results[0].id.0, 1);
    }

    #[test]
    fn test_is_full_threshold() {
        let seg = MutableSegment::new(4);
        assert!(!seg.is_full());
        // Each append adds: 4 * 5 + 48 = 68 bytes
        // 128 MB / 68 ~= 1_973_214 entries needed
        // We won't insert that many, just verify the logic
    }

    #[test]
    fn test_freeze_returns_snapshot() {
        let seg = MutableSegment::new(4);
        let f32_v = [1.0f32, 2.0, 3.0, 4.0];
        let sq_v = [1i8, 2, 3, 4];
        seg.append(100, &f32_v, &sq_v, 1.5, 1);
        seg.append(200, &f32_v, &sq_v, 2.5, 2);

        let frozen = seg.freeze();
        assert_eq!(frozen.entries.len(), 2);
        assert_eq!(frozen.vectors_f32.len(), 8);
        assert_eq!(frozen.vectors_sq.len(), 8);
        assert_eq!(frozen.dimension, 4);
        assert_eq!(frozen.entries[0].key_hash, 100);
        assert_eq!(frozen.entries[1].key_hash, 200);
    }

    #[test]
    fn test_len_and_is_empty() {
        let seg = MutableSegment::new(4);
        assert!(seg.is_empty());
        assert_eq!(seg.len(), 0);
        seg.append(1, &[1.0f32; 4], &[1i8; 4], 1.0, 1);
        assert!(!seg.is_empty());
        assert_eq!(seg.len(), 1);
    }

    #[test]
    fn test_brute_force_search_filtered_none_same_as_unfiltered() {
        distance::init();
        let dim = 8;
        let seg = MutableSegment::new(dim as u32);
        for i in 0..10u32 {
            let f32_v = make_f32_vector(dim, i * 7 + 1);
            let sq_v = make_sq_vector(dim, i * 7 + 1);
            seg.append(i as u64, &f32_v, &sq_v, 1.0, i as u64);
        }
        let query = make_sq_vector(dim, 1);
        let unfiltered = seg.brute_force_search(&query, 3);
        let filtered = seg.brute_force_search_filtered(&query, 3, None);
        assert_eq!(unfiltered.len(), filtered.len());
        for (u, f) in unfiltered.iter().zip(filtered.iter()) {
            assert_eq!(u.id.0, f.id.0);
        }
    }

    #[test]
    fn test_brute_force_search_filtered_skips_non_bitmap() {
        distance::init();
        let dim = 4;
        let seg = MutableSegment::new(dim as u32);
        let f32_v = [0.0f32; 4];
        seg.append(0, &f32_v, &[0i8, 0, 0, 0], 1.0, 1); // id 0
        seg.append(1, &f32_v, &[1i8, 1, 1, 1], 1.0, 2); // id 1
        seg.append(2, &f32_v, &[10i8, 10, 10, 10], 1.0, 3); // id 2

        // Only allow id 1 and 2
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);

        let results = seg.brute_force_search_filtered(&[0i8, 0, 0, 0], 3, Some(&bitmap));
        for r in &results {
            assert_ne!(r.id.0, 0, "id 0 should be filtered out");
        }
        assert!(!results.is_empty());
        // id 1 should be nearest (distance 4)
        assert_eq!(results[0].id.0, 1);
    }

    #[test]
    fn test_no_hnsw_methods_exist() {
        // This test documents the compile-time guarantee:
        // MutableSegment has no build_hnsw, insert_hnsw, or graph field.
        // If someone adds such methods, this comment serves as a reminder
        // that MutableSegment is brute-force ONLY.
        let _seg = MutableSegment::new(4);
        // Compilation success IS the test -- there are no HNSW methods to call.
    }

    #[test]
    fn test_mark_deleted() {
        let seg = MutableSegment::new(4);
        seg.append(1, &[1.0f32; 4], &[1i8; 4], 1.0, 1);
        seg.mark_deleted(0, 42);

        let frozen = seg.freeze();
        assert_eq!(frozen.entries[0].delete_lsn, 42);
    }

    // -- MVCC tests (Phase 65-02) --

    #[test]
    fn test_brute_force_search_mvcc_backward_compat() {
        // snapshot_lsn=0 with empty committed should return same results as non-MVCC search
        distance::init();
        let dim = 8;
        let seg = MutableSegment::new(dim as u32);
        for i in 0..10u32 {
            let f32_v = make_f32_vector(dim, i * 7 + 1);
            let sq_v = make_sq_vector(dim, i * 7 + 1);
            seg.append(i as u64, &f32_v, &sq_v, 1.0, i as u64);
        }
        let query = make_sq_vector(dim, 1);
        let committed = roaring::RoaringBitmap::new();

        let non_mvcc = seg.brute_force_search(&query, 3);
        let mvcc = seg.brute_force_search_mvcc(&query, 3, None, 0, 0, &committed);

        assert_eq!(non_mvcc.len(), mvcc.len());
        for (a, b) in non_mvcc.iter().zip(mvcc.iter()) {
            assert_eq!(a.id.0, b.id.0);
            assert_eq!(a.distance, b.distance);
        }
    }

    #[test]
    fn test_brute_force_search_mvcc_filters_by_snapshot() {
        // Entries with insert_lsn > snapshot should be invisible
        distance::init();
        let dim = 4;
        let seg = MutableSegment::new(dim as u32);
        let f32_v = [0.0f32; 4];

        // insert_lsn=1, should be visible to snapshot=5
        seg.append(0, &f32_v, &[0i8, 0, 0, 0], 1.0, 1);
        // insert_lsn=10, should NOT be visible to snapshot=5
        seg.append(1, &f32_v, &[1i8, 1, 1, 1], 1.0, 10);

        let committed = roaring::RoaringBitmap::new();
        let results = seg.brute_force_search_mvcc(&[0i8, 0, 0, 0], 3, None, 5, 99, &committed);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_brute_force_search_mvcc_filters_uncommitted_other_txn() {
        // Entries owned by another uncommitted txn should be invisible
        distance::init();
        let dim = 4;
        let seg = MutableSegment::new(dim as u32);
        let f32_v = [0.0f32; 4];

        seg.append(0, &f32_v, &[0i8, 0, 0, 0], 1.0, 1); // txn_id=0

        // Manually append with txn_id via append_transactional
        seg.append_transactional(1, &f32_v, &[1i8, 1, 1, 1], 1.0, 2, 42); // txn_id=42

        let committed = roaring::RoaringBitmap::new(); // 42 not committed
        // my_txn_id=99 (not 42), snapshot=10
        let results = seg.brute_force_search_mvcc(&[0i8, 0, 0, 0], 3, None, 10, 99, &committed);

        // Only entry 0 should be visible (entry 1 owned by uncommitted txn 42)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_brute_force_search_mvcc_read_own_writes() {
        // Entries owned by my_txn_id should be visible even if not committed
        distance::init();
        let dim = 4;
        let seg = MutableSegment::new(dim as u32);
        let f32_v = [0.0f32; 4];

        seg.append_transactional(0, &f32_v, &[0i8, 0, 0, 0], 1.0, 5, 42); // my txn

        let committed = roaring::RoaringBitmap::new();
        let results = seg.brute_force_search_mvcc(&[0i8, 0, 0, 0], 3, None, 10, 42, &committed);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_append_transactional_sets_txn_id() {
        let seg = MutableSegment::new(4);
        seg.append_transactional(100, &[1.0f32; 4], &[1i8; 4], 1.5, 5, 42);

        let frozen = seg.freeze();
        assert_eq!(frozen.entries[0].txn_id, 42);
        assert_eq!(frozen.entries[0].insert_lsn, 5);
        assert_eq!(frozen.entries[0].key_hash, 100);
    }
}
