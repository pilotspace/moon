//! HNSW beam search with BitVec visited tracking, SearchScratch reuse,
//! and 2-hop dual prefetch for cache-optimized traversal.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::vector::aligned_buffer::AlignedBuffer;

/// Bit vector for O(1) visited tracking. 64x more cache-efficient than HashSet
/// for dense integer keys. Uses test_and_set for combined check+mark.
///
/// Memory: ceil(max_nodes / 64) * 8 bytes. At 1M nodes: 128 KB.
/// Clear: memset via write_bytes -- no per-element iteration.
pub struct BitVec {
    words: Vec<u64>,
}

impl BitVec {
    /// Create a BitVec with capacity for `max_id` node IDs.
    pub fn new(max_id: u32) -> Self {
        let words_needed = (max_id as usize + 63) / 64;
        Self {
            words: vec![0u64; words_needed],
        }
    }

    /// Test if `id` is set, then set it. Returns true if was ALREADY set.
    ///
    /// This is the core visited-tracking primitive. Combines read+write in one
    /// operation to avoid double cache-line access.
    #[inline(always)]
    pub fn test_and_set(&mut self, id: u32) -> bool {
        let word_idx = id as usize >> 6; // id / 64
        let bit = 1u64 << (id & 63); // id % 64
        let prev = self.words[word_idx];
        self.words[word_idx] = prev | bit;
        prev & bit != 0
    }

    /// Clear all bits up to `max_id`. Uses memset for SIMD-optimized zeroing.
    ///
    /// If the bitvec is too small, it grows (but never shrinks -- reuse across queries).
    pub fn clear_all(&mut self, max_id: u32) {
        let words_needed = (max_id as usize + 63) / 64;
        if self.words.len() < words_needed {
            self.words.resize(words_needed, 0);
        } else {
            // SAFETY: self.words.as_mut_ptr() points to `words_needed` initialized u64s.
            // write_bytes zeroes exactly `words_needed` u64-sized slots.
            // words_needed <= self.words.len() (checked above).
            unsafe {
                std::ptr::write_bytes(self.words.as_mut_ptr(), 0, words_needed);
            }
        }
    }
}

/// Ordered (distance, node_id) pair for BinaryHeap usage.
/// Compares by distance first (f32 total order), then by node_id.
#[derive(Clone, Copy, PartialEq)]
pub(crate) struct OrdF32Pair(pub(crate) f32, pub(crate) u32);

impl Eq for OrdF32Pair {}

impl PartialOrd for OrdF32Pair {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF32Pair {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // total_cmp provides IEEE 754 total ordering (handles NaN deterministically)
        self.0.total_cmp(&other.0).then(self.1.cmp(&other.1))
    }
}

/// Shard-owned search scratch space. Reused across queries -- zero allocation per search.
///
/// Lifecycle:
/// 1. Created once per shard with capacity for max expected graph size.
/// 2. clear() before each search (memset visited, clear heaps -- no realloc).
/// 3. hnsw_search uses candidates/results/visited during beam search.
/// 4. After search, results are extracted; scratch is left dirty until next clear().
pub struct SearchScratch {
    /// Min-heap of candidates to explore: pop nearest first.
    pub(crate) candidates: BinaryHeap<Reverse<OrdF32Pair>>,
    /// Max-heap of current results: peek/pop farthest for pruning.
    pub(crate) results: BinaryHeap<OrdF32Pair>,
    /// Visited bit vector -- cleared via memset per search.
    pub(crate) visited: BitVec,
    /// Pre-allocated buffer for FWHT-rotated query (reused across searches).
    pub(crate) query_rotated: AlignedBuffer<f32>,
}

impl SearchScratch {
    /// Create scratch space for graphs up to `max_nodes` and queries up to `padded_dim`.
    pub fn new(max_nodes: u32, padded_dim: u32) -> Self {
        Self {
            candidates: BinaryHeap::with_capacity(256),
            results: BinaryHeap::with_capacity(256),
            visited: BitVec::new(max_nodes),
            query_rotated: AlignedBuffer::new(padded_dim as usize),
        }
    }

    /// Clear scratch state for a new search. Zero allocation.
    ///
    /// Heaps are cleared (len=0, capacity preserved).
    /// Visited bits zeroed via memset.
    pub fn clear(&mut self, num_nodes: u32) {
        self.candidates.clear();
        self.results.clear();
        self.visited.clear_all(num_nodes);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_bitvec_new_word_count() {
        let bv = super::BitVec::new(1000);
        // ceil(1000/64) = 16 words
        assert_eq!(bv.words.len(), 16);
    }

    #[test]
    fn test_bitvec_test_and_set_first_returns_false() {
        let mut bv = super::BitVec::new(100);
        assert!(!bv.test_and_set(42));
    }

    #[test]
    fn test_bitvec_test_and_set_second_returns_true() {
        let mut bv = super::BitVec::new(100);
        assert!(!bv.test_and_set(42));
        assert!(bv.test_and_set(42));
    }

    #[test]
    fn test_bitvec_boundary_ids() {
        let mut bv = super::BitVec::new(1000);
        // ID 0
        assert!(!bv.test_and_set(0));
        assert!(bv.test_and_set(0));
        // ID 63 (last bit of first word)
        assert!(!bv.test_and_set(63));
        assert!(bv.test_and_set(63));
        // ID 64 (first bit of second word)
        assert!(!bv.test_and_set(64));
        assert!(bv.test_and_set(64));
        // ID 999 (near max)
        assert!(!bv.test_and_set(999));
        assert!(bv.test_and_set(999));
    }

    #[test]
    fn test_bitvec_clear_all_resets() {
        let mut bv = super::BitVec::new(100);
        bv.test_and_set(10);
        bv.test_and_set(50);
        bv.clear_all(100);
        // After clear, test_and_set should return false again
        assert!(!bv.test_and_set(10));
        assert!(!bv.test_and_set(50));
    }

    #[test]
    fn test_bitvec_clear_all_grows() {
        let mut bv = super::BitVec::new(100);
        // Grow to 2000
        bv.clear_all(2000);
        assert!(bv.words.len() >= (2000 + 63) / 64);
        // Should still work for high IDs
        assert!(!bv.test_and_set(1999));
        assert!(bv.test_and_set(1999));
    }

    #[test]
    fn test_search_scratch_new_sizes() {
        use crate::vector::aligned_buffer::AlignedBuffer;
        let scratch = super::SearchScratch::new(1000, 1024);
        assert!(scratch.candidates.capacity() >= 256);
        assert!(scratch.results.capacity() >= 256);
        assert!(scratch.visited.words.len() >= (1000 + 63) / 64);
        assert_eq!(scratch.query_rotated.len(), 1024);
    }

    #[test]
    fn test_search_scratch_clear_preserves_capacity() {
        let mut scratch = super::SearchScratch::new(1000, 1024);
        // Push some items
        scratch.candidates.push(std::cmp::Reverse(super::OrdF32Pair(1.0, 0)));
        scratch.results.push(super::OrdF32Pair(1.0, 0));
        let cap_before_cand = scratch.candidates.capacity();
        let cap_before_res = scratch.results.capacity();

        scratch.clear(1000);

        assert!(scratch.candidates.is_empty());
        assert!(scratch.results.is_empty());
        // Capacity must not shrink
        assert!(scratch.candidates.capacity() >= cap_before_cand);
        assert!(scratch.results.capacity() >= cap_before_res);
    }
}
