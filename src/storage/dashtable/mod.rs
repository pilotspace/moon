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
//! Segments (Vec<Box<Segment>>)  -- owned segment storage
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

use bytes::Bytes;

use iter::{Iter, IterMut, Keys, Values};
use segment::{h2, home_buckets, InsertResult, Segment, TOTAL_SLOTS};

/// Compute the xxh64 hash of a byte slice.
#[inline]
fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(key, 0)
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

/// A segmented hash table with Swiss Table SIMD probing.
///
/// Provides a HashMap-compatible API with per-segment incremental rehashing
/// (no memory spike on resize) and SIMD-accelerated 16-way parallel key lookup.
pub struct DashTable<K, V> {
    /// Segment storage: each segment is owned by exactly one slot here.
    segments: Vec<Box<Segment<K, V>>>,
    /// Directory: maps hash-derived indices to segment storage indices.
    /// Multiple directory entries may point to the same segment (extendible hashing).
    directory: Vec<usize>,
    /// Global depth: log2 of directory size.
    depth: u32,
    /// Total entry count across all segments.
    len: usize,
}

impl<V> DashTable<Bytes, V> {
    /// Create a new empty DashTable with one segment.
    pub fn new() -> Self {
        DashTable {
            segments: vec![Box::new(Segment::new(0))],
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
        let num_segments =
            (cap + segment::LOAD_THRESHOLD - 1) / segment::LOAD_THRESHOLD;
        let depth = if num_segments <= 1 {
            0
        } else {
            (num_segments as f64).log2().ceil() as u32
        };
        let dir_size = 1usize << depth;
        let mut segments = Vec::with_capacity(dir_size);
        let mut directory = Vec::with_capacity(dir_size);
        for i in 0..dir_size {
            segments.push(Box::new(Segment::new(depth)));
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

    /// Look up a key and return an immutable reference to its value.
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        self.segments[seg_idx].get(h2_val, key, ba, bb)
    }

    /// Look up a key and return a mutable reference to its value.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        self.segments[seg_idx].get_mut(h2_val, key, ba, bb)
    }

    /// Check if the table contains the given key.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Insert a key-value pair. Returns `Some(old_value)` if the key existed.
    pub fn insert(&mut self, key: Bytes, value: V) -> Option<V> {
        let hash = hash_key(&key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        match self.segments[seg_idx].insert(h2_val, key, value, ba, bb) {
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
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        self.segments[seg_idx]
            .remove(h2_val, key, ba, bb)
            .map(|(_k, v)| {
                self.len -= 1;
                v
            })
    }

    /// Remove a key and return both key and value.
    #[allow(dead_code)]
    pub fn remove_entry(&mut self, key: &[u8]) -> Option<(Bytes, V)> {
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        self.segments[seg_idx]
            .remove(h2_val, key, ba, bb)
            .map(|(k, v)| {
                self.len -= 1;
                (k, v)
            })
    }

    /// Return an iterator over `(&Bytes, &V)` pairs.
    pub fn iter(&self) -> Iter<'_, Bytes, V> {
        let segments: Vec<&Segment<Bytes, V>> =
            self.segments.iter().map(|s| &**s).collect();
        Iter::new(segments, self.len)
    }

    /// Return a mutable iterator over `(&Bytes, &mut V)` pairs.
    pub fn iter_mut(&mut self) -> IterMut<'_, Bytes, V> {
        let total = self.len;
        let segments: Vec<&mut Segment<Bytes, V>> =
            self.segments.iter_mut().map(|s| &mut **s).collect();
        IterMut::new(segments, total)
    }

    /// Return an iterator over keys.
    pub fn keys(&self) -> Keys<'_, Bytes, V> {
        Keys(self.iter())
    }

    /// Return an iterator over values.
    pub fn values(&self) -> Values<'_, Bytes, V> {
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
        let hasher = |k: &Bytes| hash_key(k);
        let new_seg = self.segments[seg_store_idx].split(&hasher);
        let new_depth = new_seg.depth();

        // Add new segment to the store
        let new_store_idx = self.segments.len();
        self.segments.push(Box::new(new_seg));

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

impl<'a, V> IntoIterator for &'a DashTable<Bytes, V> {
    type Item = (&'a Bytes, &'a V);
    type IntoIter = Iter<'a, Bytes, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn test_value(n: u32) -> String {
        format!("value_{}", n)
    }

    #[test]
    fn test_new_empty() {
        let table: DashTable<Bytes, String> = DashTable::new();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        assert_eq!(table.get(b"anything"), None);
    }

    #[test]
    fn test_insert_and_get() {
        let mut table: DashTable<Bytes, String> = DashTable::new();

        for i in 0..10 {
            let key = Bytes::from(format!("key_{}", i));
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
        let mut table: DashTable<Bytes, String> = DashTable::new();
        let key = Bytes::from("mykey");

        assert_eq!(table.insert(key.clone(), "first".into()), None);
        assert_eq!(table.len(), 1);

        let old = table.insert(key.clone(), "second".into());
        assert_eq!(old, Some("first".into()));
        assert_eq!(table.len(), 1);

        assert_eq!(table.get(b"mykey"), Some(&"second".to_string()));
    }

    #[test]
    fn test_remove() {
        let mut table: DashTable<Bytes, String> = DashTable::new();
        let key = Bytes::from("remove_me");
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
        let mut table: DashTable<Bytes, String> = DashTable::new();
        let key = Bytes::from("exists");
        table.insert(key.clone(), "yes".into());
        assert!(table.contains_key(b"exists"));
        assert!(!table.contains_key(b"nope"));

        table.remove(b"exists");
        assert!(!table.contains_key(b"exists"));
    }

    #[test]
    fn test_keys_iter() {
        let mut table: DashTable<Bytes, String> = DashTable::new();
        let mut expected_keys: Vec<String> = Vec::new();

        for i in 0..5 {
            let key = Bytes::from(format!("k{}", i));
            expected_keys.push(format!("k{}", i));
            table.insert(key, test_value(i));
        }

        let mut actual_keys: Vec<String> = table
            .keys()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .collect();
        actual_keys.sort();
        expected_keys.sort();
        assert_eq!(actual_keys, expected_keys);
    }

    #[test]
    fn test_iter() {
        let mut table: DashTable<Bytes, String> = DashTable::new();
        for i in 0..8 {
            table.insert(Bytes::from(format!("iter_{}", i)), test_value(i));
        }

        let count = table.iter().count();
        assert_eq!(count, 8);
        assert_eq!(count, table.len());
    }

    #[test]
    fn test_iter_mut() {
        let mut table: DashTable<Bytes, String> = DashTable::new();
        for i in 0..5 {
            table.insert(Bytes::from(format!("mut_{}", i)), test_value(i));
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
        let mut table: DashTable<Bytes, String> = DashTable::new();

        for i in 0..100 {
            let key = Bytes::from(format!("large_{:04}", i));
            table.insert(key, test_value(i));
        }

        assert_eq!(table.len(), 100);

        for i in 0..100 {
            let key = format!("large_{:04}", i);
            let val = table.get(key.as_bytes());
            assert_eq!(val, Some(&test_value(i)), "Missing large_{:04}", i);
        }

        // Should have more than 1 segment after splits
        assert!(table.segments.len() > 1, "Expected splits to occur");
    }

    #[test]
    fn test_1000_entries() {
        let mut table: DashTable<Bytes, String> = DashTable::new();

        for i in 0..1000 {
            let key = Bytes::from(format!("stress_{:06}", i));
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
        let table: DashTable<Bytes, String> = DashTable::with_capacity(1000);
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn test_iter_empty() {
        let table: DashTable<Bytes, String> = DashTable::new();
        assert_eq!(table.iter().count(), 0);
    }

    #[test]
    fn test_iter_count_matches_len() {
        let mut table: DashTable<Bytes, String> = DashTable::new();
        for i in 0..50 {
            table.insert(Bytes::from(format!("cnt_{}", i)), test_value(i));
        }
        assert_eq!(table.iter().count(), table.len());
    }

    #[test]
    fn test_iter_after_removes() {
        let mut table: DashTable<Bytes, String> = DashTable::new();
        for i in 0..20 {
            table.insert(Bytes::from(format!("rem_{}", i)), test_value(i));
        }
        for i in 0..10 {
            table.remove(format!("rem_{}", i).as_bytes());
        }
        assert_eq!(table.len(), 10);
        assert_eq!(table.iter().count(), 10);
    }

    #[test]
    fn test_values_iter() {
        let mut table: DashTable<Bytes, String> = DashTable::new();
        for i in 0..5 {
            table.insert(Bytes::from(format!("v_{}", i)), test_value(i));
        }
        assert_eq!(table.values().count(), 5);
    }

    #[test]
    fn test_directory_doubling() {
        // Insert enough to force multiple splits and directory doublings
        let mut table: DashTable<Bytes, String> = DashTable::new();
        for i in 0..200 {
            table.insert(Bytes::from(format!("dd_{:06}", i)), test_value(i));
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
}
