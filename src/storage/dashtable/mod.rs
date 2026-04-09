//! DashTable: segmented hash table with Swiss Table SIMD probing.
//!
//! A custom hash table that combines Dragonfly's DashTable macro-architecture
//! (directory -> segments -> buckets) with hashbrown's Swiss Table SIMD
//! micro-optimization (control byte groups with parallel comparison).
//!
//! # Architecture
//!
//! ```text
//! Directory (Vec<usize>)   -- indices into segments store
//!   |
//! Segments (SegmentSlab)        -- slab-allocated segment storage
//!   |
//!   +-- Segment 0: [ctrl: 64 bytes] [keys: 60 slots] [values: 60 slots]
//!   +-- Segment 1: ...
//! ```
//!
//! Hash routing:
//! - H1 (full hash): segment index (high bits) + home bucket selection (mid bits)
//! - H2 (top 7 bits): control byte fingerprint for SIMD matching

pub mod iter;
pub mod segment;
pub mod simd;

use super::compact_key::CompactKey;

use iter::{Iter, IterMut, Keys, Values};
use segment::{InsertResult, Segment, h2, home_buckets};

/// Compute the xxh64 hash of a byte slice.
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(key, 0)
}

/// Issue a software prefetch hint for a segment's memory.
///
/// Called after computing the segment index but before computing h2/home_buckets.
/// The ~6ns hash computation overlaps with the prefetch latency (~10ns on L2/L3 miss),
/// so the segment data is likely in L1 by the time we access it.
#[inline(always)]
fn prefetch_segment<K, V>(segment: &Segment<K, V>) {
    let ptr = segment as *const Segment<K, V> as *const u8;
    // SAFETY: ptr points to a valid, aligned Segment obtained from the slab.
    // Prefetch is a performance hint; it does not dereference the pointer.
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
    // SAFETY: Same as above — ptr is a valid Segment address used as a prefetch hint.
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{ptr}]", ptr = in(reg) ptr, options(nostack, preserves_flags));
    }
}

/// Compute the segment directory index from a hash and the current global depth.
#[inline]
fn segment_index(hash: u64, depth: u32) -> usize {
    if depth == 0 {
        0
    } else {
        (hash >> (64 - depth)) as usize
    }
}

/// Slab allocator for DashTable segments.
///
/// Pre-allocates segments in contiguous Vec "slabs" instead of individual Box
/// allocations. Benefits: eliminates per-segment allocator metadata (~16-32B per
/// segment), improves cache locality during segment scans, reduces heap
/// fragmentation.
///
/// Segments are addressed by a flat index. New slabs are allocated with a
/// doubling growth strategy (capped at 1024 segments per slab) so existing
/// segment pointers within earlier slabs are never invalidated by growth.
struct SegmentSlab<K, V> {
    /// Contiguous blocks of segments. Each inner Vec is one slab.
    slabs: Vec<Vec<Segment<K, V>>>,
    /// Flat index -> (slab_idx, slot_idx) for O(1) lookup without
    /// division/modulo (slabs may have different capacities).
    index_map: Vec<(u32, u32)>,
    /// Number of segments per next slab allocation (doubles each time).
    next_slab_capacity: usize,
}

impl<K, V> SegmentSlab<K, V> {
    fn new() -> Self {
        SegmentSlab {
            slabs: Vec::new(),
            index_map: Vec::new(),
            next_slab_capacity: 16,
        }
    }

    /// Add a segment, returning its flat index.
    fn push(&mut self, segment: Segment<K, V>) -> usize {
        // Check if current last slab has room
        let needs_new_slab = self.slabs.is_empty()
            || self.slabs.last().unwrap().len() >= self.slabs.last().unwrap().capacity();

        if needs_new_slab {
            let cap = self.next_slab_capacity;
            self.slabs.push(Vec::with_capacity(cap));
            // Double for next time, cap at 1024
            self.next_slab_capacity = (cap * 2).min(1024);
        }

        let slab_idx = self.slabs.len() - 1;
        let slot_idx = self.slabs[slab_idx].len();
        self.slabs[slab_idx].push(segment);

        let flat_idx = self.index_map.len();
        self.index_map.push((slab_idx as u32, slot_idx as u32));
        flat_idx
    }

    #[inline]
    fn len(&self) -> usize {
        self.index_map.len()
    }

    #[inline]
    fn get(&self, idx: usize) -> &Segment<K, V> {
        let (si, sli) = self.index_map[idx];
        &self.slabs[si as usize][sli as usize]
    }

    #[inline]
    fn get_mut(&mut self, idx: usize) -> &mut Segment<K, V> {
        let (si, sli) = self.index_map[idx];
        &mut self.slabs[si as usize][sli as usize]
    }

    /// Collect immutable references to all segments (for iterator construction).
    fn collect_refs(&self) -> Vec<&Segment<K, V>> {
        self.index_map
            .iter()
            .map(|&(si, sli)| &self.slabs[si as usize][sli as usize])
            .collect()
    }

    /// Collect mutable references to all segments (for iterator construction).
    ///
    /// SAFETY: Each index_map entry refers to a unique (slab_idx, slot_idx) pair,
    /// so no two mutable references alias. We use raw pointers to work around
    /// the borrow checker's inability to prove non-aliasing across Vec indexing.
    fn collect_mut_refs(&mut self) -> Vec<&mut Segment<K, V>> {
        let slabs_ptr = self.slabs.as_mut_ptr();
        self.index_map
            .iter()
            .map(|&(si, sli)| {
                // SAFETY: Each index_map entry refers to a unique (slab_idx, slot_idx) pair,
                // so no two mutable references alias. Raw pointer arithmetic is used to work
                // around the borrow checker; both slab and slot indices are in bounds.
                unsafe {
                    let slab = &mut *slabs_ptr.add(si as usize);
                    &mut *slab.as_mut_ptr().add(sli as usize)
                }
            })
            .collect()
    }
}

/// A segmented hash table with Swiss Table SIMD probing.
///
/// Provides a HashMap-compatible API with per-segment incremental rehashing
/// (no memory spike on resize) and SIMD-accelerated 16-way parallel key lookup.
pub struct DashTable<K, V> {
    /// Segment storage: slab-allocated for contiguous memory layout.
    segments: SegmentSlab<K, V>,
    /// Directory: maps hash-derived indices to segment storage indices.
    /// Multiple directory entries may point to the same segment (extendible hashing).
    directory: Vec<usize>,
    /// Global depth: log2 of directory size.
    depth: u32,
    /// Total entry count across all segments.
    len: usize,
}

impl<V> DashTable<CompactKey, V> {
    /// Create a new empty DashTable with one segment.
    pub fn new() -> Self {
        let mut segments = SegmentSlab::new();
        segments.push(Segment::new(0));
        DashTable {
            segments,
            directory: vec![0],
            depth: 0,
            len: 0,
        }
    }

    /// Create a DashTable pre-sized for approximately `cap` entries.
    pub fn with_capacity(cap: usize) -> Self {
        if cap == 0 {
            return Self::new();
        }
        let num_segments = (cap + segment::LOAD_THRESHOLD - 1) / segment::LOAD_THRESHOLD;
        let depth = if num_segments <= 1 {
            0
        } else {
            (num_segments as f64).log2().ceil() as u32
        };
        let dir_size = 1usize << depth;
        let mut segments = SegmentSlab::new();
        let mut directory = Vec::with_capacity(dir_size);
        for i in 0..dir_size {
            segments.push(Segment::new(depth));
            directory.push(i);
        }
        DashTable {
            segments,
            directory,
            depth,
            len: 0,
        }
    }

    /// Return the number of entries in the table.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return true if the table contains no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the number of unique segments in the segment store.
    #[inline]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Return an immutable reference to a segment by storage index.
    ///
    /// # Panics
    /// Panics if `idx >= segment_count()`.
    #[inline]
    pub fn segment(&self, idx: usize) -> &Segment<CompactKey, V> {
        self.segments.get(idx)
    }

    /// Determine which segment storage index a key hash maps to.
    ///
    /// Uses the directory indirection: hash -> directory index -> segment store index.
    #[inline]
    pub fn segment_index_for_hash(&self, hash: u64) -> usize {
        let dir_idx = segment_index(hash, self.depth);
        self.directory[dir_idx]
    }

    /// Look up a key and return an immutable reference to its value.
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];

        // Prefetch segment data while computing home bucket (overlaps ~10ns L2/L3 miss)
        prefetch_segment(self.segments.get(seg_idx));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        self.segments.get(seg_idx).get(h2_val, key, ba, bb)
    }

    /// Look up a key and return a mutable reference to its value.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];

        // Prefetch segment data while computing home bucket
        prefetch_segment(self.segments.get(seg_idx));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        self.segments.get_mut(seg_idx).get_mut(h2_val, key, ba, bb)
    }

    /// Check if the table contains the given key.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Insert a key-value pair. Returns `Some(old_value)` if the key existed.
    pub fn insert(&mut self, key: CompactKey, value: V) -> Option<V> {
        let hash = hash_key(key.as_ref());
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];

        // Prefetch segment data while computing home bucket
        prefetch_segment(self.segments.get(seg_idx));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        match self
            .segments
            .get_mut(seg_idx)
            .insert(h2_val, key, value, ba, bb)
        {
            InsertResult::Inserted => {
                self.len += 1;
                None
            }
            InsertResult::Replaced(old) => Some(old),
            InsertResult::NeedsSplit(key, value) => {
                // Split the segment, then retry insert
                self.split_segment(dir_idx);
                self.insert(key, value)
            }
        }
    }

    /// Remove a key from the table. Returns `Some(value)` if the key existed.
    ///
    /// Matches HashMap's `remove` semantics: returns only the value, dropping the key.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];

        // Prefetch segment data while computing home bucket
        prefetch_segment(self.segments.get(seg_idx));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        self.segments
            .get_mut(seg_idx)
            .remove(h2_val, key, ba, bb)
            .map(|(_k, v)| {
                self.len -= 1;
                v
            })
    }

    /// Remove a key and return both key and value.
    #[allow(dead_code)]
    pub fn remove_entry(&mut self, key: &[u8]) -> Option<(CompactKey, V)> {
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];

        // Prefetch segment data while computing home bucket
        prefetch_segment(self.segments.get(seg_idx));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        self.segments
            .get_mut(seg_idx)
            .remove(h2_val, key, ba, bb)
            .map(|(k, v)| {
                self.len -= 1;
                (k, v)
            })
    }

    /// Return an iterator over `(&Bytes, &V)` pairs.
    pub fn iter(&self) -> Iter<'_, CompactKey, V> {
        Iter::new(self.segments.collect_refs(), self.len)
    }

    /// Return a mutable iterator over `(&Bytes, &mut V)` pairs.
    pub fn iter_mut(&mut self) -> IterMut<'_, CompactKey, V> {
        let total = self.len;
        IterMut::new(self.segments.collect_mut_refs(), total)
    }

    /// Return an iterator over keys.
    pub fn keys(&self) -> Keys<'_, CompactKey, V> {
        Keys(self.iter())
    }

    /// Return an iterator over values.
    pub fn values(&self) -> Values<'_, CompactKey, V> {
        Values(self.iter())
    }

    /// Split the segment referenced by the given directory index.
    ///
    /// Algorithm:
    /// 1. Call segment.split(hasher) to produce a new segment
    /// 2. If new segment's depth > global depth, double the directory
    /// 3. Update directory entries to point to the new segment
    fn split_segment(&mut self, dir_idx: usize) {
        let seg_store_idx = self.directory[dir_idx];
        let hasher = |k: &CompactKey| hash_key(k.as_ref());
        let new_seg = self.segments.get_mut(seg_store_idx).split(&hasher);
        let new_depth = new_seg.depth();

        // Add new segment to the slab store
        let new_store_idx = self.segments.push(new_seg);

        // Double directory if needed
        while new_depth > self.depth {
            let old_len = self.directory.len();
            let mut new_dir = Vec::with_capacity(old_len * 2);
            for &idx in &self.directory {
                new_dir.push(idx);
                new_dir.push(idx);
            }
            self.directory = new_dir;
            self.depth += 1;
        }

        // Update directory entries: entries that should point to the new segment
        // are those whose index has bit (new_depth-1) set when looking at the
        // portion of the index that routes to this segment.
        let bit_pos = new_depth - 1;
        for i in 0..self.directory.len() {
            if self.directory[i] == seg_store_idx {
                // Check if this directory index should route to the new segment.
                // The directory index's bit at position `bit_pos` (from MSB of the
                // depth-bit index) determines which segment to use.
                // In our scheme, directory index `i` corresponds to the top `depth`
                // bits of the hash. Bit at position `bit_pos` from the top maps to
                // bit `(depth - 1 - bit_pos)` in the directory index.
                let bit_in_idx = self.depth - 1 - bit_pos;
                if (i >> bit_in_idx) & 1 == 1 {
                    self.directory[i] = new_store_idx;
                }
            }
        }
    }
}

impl<'a, V> IntoIterator for &'a DashTable<CompactKey, V> {
    type Item = (&'a CompactKey, &'a V);
    type IntoIter = Iter<'a, CompactKey, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::segment::TOTAL_SLOTS;
    use super::*;

    fn test_value(n: u32) -> String {
        format!("value_{}", n)
    }

    #[test]
    fn test_new_empty() {
        let table: DashTable<CompactKey, String> = DashTable::new();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        assert_eq!(table.get(b"anything"), None);
    }

    #[test]
    fn test_insert_and_get() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();

        for i in 0..10 {
            let key = CompactKey::from(format!("key_{}", i));
            let val = test_value(i);
            assert_eq!(table.insert(key, val), None);
        }

        assert_eq!(table.len(), 10);

        for i in 0..10 {
            let key = format!("key_{}", i);
            let val = table.get(key.as_bytes());
            assert_eq!(val, Some(&test_value(i)), "Missing key_{}", i);
        }
    }

    #[test]
    fn test_insert_replace() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        let key = CompactKey::from("mykey");

        assert_eq!(table.insert(key.clone(), "first".into()), None);
        assert_eq!(table.len(), 1);

        let old = table.insert(key.clone(), "second".into());
        assert_eq!(old, Some("first".into()));
        assert_eq!(table.len(), 1);

        assert_eq!(table.get(b"mykey"), Some(&"second".to_string()));
    }

    #[test]
    fn test_remove() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        let key = CompactKey::from("remove_me");
        table.insert(key.clone(), "value".into());
        assert_eq!(table.len(), 1);

        let removed = table.remove(b"remove_me");
        assert_eq!(removed, Some("value".to_string()));
        assert_eq!(table.len(), 0);
        assert_eq!(table.get(b"remove_me"), None);

        assert_eq!(table.remove(b"remove_me"), None);
    }

    #[test]
    fn test_contains_key() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        let key = CompactKey::from("exists");
        table.insert(key.clone(), "yes".into());
        assert!(table.contains_key(b"exists"));
        assert!(!table.contains_key(b"nope"));

        table.remove(b"exists");
        assert!(!table.contains_key(b"exists"));
    }

    #[test]
    fn test_keys_iter() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        let mut expected_keys: Vec<String> = Vec::new();

        for i in 0..5 {
            let key = CompactKey::from(format!("k{}", i));
            expected_keys.push(format!("k{}", i));
            table.insert(key, test_value(i));
        }

        let mut actual_keys: Vec<String> = table
            .keys()
            .map(|k| String::from_utf8_lossy(k.as_bytes()).to_string())
            .collect();
        actual_keys.sort();
        expected_keys.sort();
        assert_eq!(actual_keys, expected_keys);
    }

    #[test]
    fn test_iter() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..8 {
            table.insert(CompactKey::from(format!("iter_{}", i)), test_value(i));
        }

        let count = table.iter().count();
        assert_eq!(count, 8);
        assert_eq!(count, table.len());
    }

    #[test]
    fn test_iter_mut() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..5 {
            table.insert(CompactKey::from(format!("mut_{}", i)), test_value(i));
        }

        for (_k, v) in table.iter_mut() {
            *v = format!("modified_{}", v);
        }

        for i in 0..5 {
            let key = format!("mut_{}", i);
            let val = table.get(key.as_bytes()).unwrap();
            assert!(val.starts_with("modified_"), "Value not modified: {}", val);
        }
    }

    #[test]
    fn test_large_insert_triggers_split() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();

        for i in 0..100 {
            let key = CompactKey::from(format!("large_{:04}", i));
            table.insert(key, test_value(i));
        }

        assert_eq!(table.len(), 100);

        for i in 0..100 {
            let key = format!("large_{:04}", i);
            let val = table.get(key.as_bytes());
            assert_eq!(val, Some(&test_value(i)), "Missing large_{:04}", i);
        }

        // Should have more than 1 segment after splits
        assert!(table.segment_count() > 1, "Expected splits to occur");
    }

    #[test]
    fn test_1000_entries() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();

        for i in 0..1000 {
            let key = CompactKey::from(format!("stress_{:06}", i));
            table.insert(key, test_value(i));
        }

        assert_eq!(table.len(), 1000);

        for i in 0..1000 {
            let key = format!("stress_{:06}", i);
            let val = table.get(key.as_bytes());
            assert_eq!(val, Some(&test_value(i)), "Missing stress_{:06}", i);
        }

        for i in 0..500 {
            let key = format!("stress_{:06}", i);
            let removed = table.remove(key.as_bytes());
            assert!(removed.is_some(), "Failed to remove stress_{:06}", i);
        }

        assert_eq!(table.len(), 500);

        for i in 500..1000 {
            let key = format!("stress_{:06}", i);
            let val = table.get(key.as_bytes());
            assert_eq!(
                val,
                Some(&test_value(i)),
                "Missing stress_{:06} after removes",
                i
            );
        }

        for i in 0..500 {
            let key = format!("stress_{:06}", i);
            assert_eq!(
                table.get(key.as_bytes()),
                None,
                "stress_{:06} should be removed",
                i
            );
        }
    }

    #[test]
    fn test_memory_overhead() {
        // Verify structural overhead per entry is <= 16 bytes.
        // Segment overhead: 64 bytes ctrl + 8 bytes metadata = 72 bytes for 60 slots.
        // Per slot: 72 / 60 = 1.2 bytes. Well under 16.
        let ctrl_bytes = 64usize;
        let meta_bytes = 8usize; // count(4) + depth(4)
        let per_slot = (ctrl_bytes + meta_bytes) as f64 / TOTAL_SLOTS as f64;
        assert!(
            per_slot <= 16.0,
            "Per-slot overhead {:.1} exceeds 16 bytes",
            per_slot
        );
    }

    #[test]
    fn test_with_capacity() {
        let table: DashTable<CompactKey, String> = DashTable::with_capacity(1000);
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn test_iter_empty() {
        let table: DashTable<CompactKey, String> = DashTable::new();
        assert_eq!(table.iter().count(), 0);
    }

    #[test]
    fn test_iter_count_matches_len() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..50 {
            table.insert(CompactKey::from(format!("cnt_{}", i)), test_value(i));
        }
        assert_eq!(table.iter().count(), table.len());
    }

    #[test]
    fn test_iter_after_removes() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..20 {
            table.insert(CompactKey::from(format!("rem_{}", i)), test_value(i));
        }
        for i in 0..10 {
            table.remove(format!("rem_{}", i).as_bytes());
        }
        assert_eq!(table.len(), 10);
        assert_eq!(table.iter().count(), 10);
    }

    #[test]
    fn test_values_iter() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..5 {
            table.insert(CompactKey::from(format!("v_{}", i)), test_value(i));
        }
        assert_eq!(table.values().count(), 5);
    }

    #[test]
    fn test_directory_doubling() {
        // Insert enough to force multiple splits and directory doublings
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..200 {
            table.insert(CompactKey::from(format!("dd_{:06}", i)), test_value(i));
        }

        // Directory should have grown
        assert!(table.directory.len() > 1);
        assert_eq!(table.len(), 200);

        // All entries retrievable
        for i in 0..200 {
            assert!(
                table.get(format!("dd_{:06}", i).as_bytes()).is_some(),
                "Missing dd_{:06}",
                i
            );
        }
    }

    #[test]
    fn test_segment_iter_occupied() {
        use segment::Segment;

        let mut seg: Segment<CompactKey, String> = Segment::new(0);
        // Insert 5 entries using the segment's insert method
        for i in 0..5 {
            let key = CompactKey::from(format!("seg_key_{}", i));
            let val = format!("seg_val_{}", i);
            let hash = hash_key(key.as_ref());
            let h2_val = segment::h2(hash);
            let (ba, bb) = segment::home_buckets(hash);
            seg.insert(h2_val, key, val, ba, bb);
        }

        let occupied: Vec<_> = seg.iter_occupied().collect();
        assert_eq!(
            occupied.len(),
            5,
            "iter_occupied should yield exactly 5 pairs"
        );

        // Verify all keys are present
        let keys: Vec<String> = occupied
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k.as_bytes()).to_string())
            .collect();
        for i in 0..5 {
            let expected_key = format!("seg_key_{}", i);
            assert!(
                keys.contains(&expected_key),
                "Missing key: {}",
                expected_key
            );
        }
    }

    #[test]
    fn test_segment_count_grows_after_split() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        assert_eq!(table.segment_count(), 1);

        for i in 0..100 {
            table.insert(CompactKey::from(format!("sc_{:04}", i)), test_value(i));
        }

        assert!(
            table.segment_count() > 1,
            "segment_count should grow after splits, got {}",
            table.segment_count()
        );
    }

    #[test]
    fn test_segment_index_for_hash_matches_get() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..50 {
            table.insert(CompactKey::from(format!("si_{:04}", i)), test_value(i));
        }

        // For each key, verify segment_index_for_hash points to the segment containing it
        for i in 0..50 {
            let key = format!("si_{:04}", i);
            let hash = hash_key(key.as_bytes());
            let seg_idx = table.segment_index_for_hash(hash);
            let seg = table.segment(seg_idx);

            // The segment should contain this key in its iter_occupied
            let found = seg
                .iter_occupied()
                .any(|(k, _)| k.as_ref() == key.as_bytes());
            assert!(
                found,
                "Key {} not found in segment {} (segment_count={})",
                key,
                seg_idx,
                table.segment_count()
            );
        }
    }

    /// Regression test: insert followed by get_mut must always succeed.
    ///
    /// This verifies the fix for the "overflow slot" bug where insert's
    /// last-resort linear scan could place a key in a group that find()
    /// didn't check (only group_a, group_b, and stash were searched).
    #[test]
    fn test_insert_then_get_mut_always_finds() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();

        for i in 0..2000 {
            let key = CompactKey::from(format!("regress_{:06}", i));
            let val = test_value(i);
            table.insert(key, val);

            // Immediately verify the key is findable
            let lookup_key = format!("regress_{:06}", i);
            assert!(
                table.get_mut(lookup_key.as_bytes()).is_some(),
                "get_mut returned None immediately after insert for regress_{:06} (table len={})",
                i,
                table.len()
            );
        }

        // Verify all keys are still accessible
        for i in 0..2000 {
            let key = format!("regress_{:06}", i);
            assert!(
                table.get(key.as_bytes()).is_some(),
                "get returned None for regress_{:06}",
                i,
            );
        }
    }
}
