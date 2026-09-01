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
use segment::{InsertResult, Segment, SegmentInsertOrUpdate, h2, home_buckets};

/// Outcome of [`DashTable::insert_or_update`].
pub enum InsertOrUpdate<'a, V> {
    /// Key was new and has just been inserted.
    Inserted(&'a mut V),
    /// Key existed; the user-supplied closure was invoked.
    Updated(&'a mut V),
}

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

/// Largest slab, in segments. Growth doubles up to this and then repeats it.
const MAX_SLAB_SEGMENTS: usize = 1024;

impl<K, V> SegmentSlab<K, V> {
    /// A slab store whose FIRST slab holds exactly `first_slab` segments.
    ///
    /// Callers that know their segment count up front (`DashTable::with_capacity`)
    /// pass it and get a single right-sized slab. Callers that do not
    /// (`DashTable::new`) pass 1 and pay for exactly the one segment they push.
    ///
    /// # Why this is not a fixed 16
    ///
    /// It was, and that made an EMPTY table reserve 16 slots to hold one
    /// segment. `size_of::<Segment<CompactKey, CompactEntry>>()` is 3,456 B, so
    /// an empty `DashTable` reserved 55,296 B to store 3,456 B — and moon
    /// creates `--databases` (16) of them **per shard** at boot, all empty:
    /// 884,736 B of reservation per shard, 93.75% of it for segments that
    /// never exist on an idle server. It was the single largest allocation in
    /// the whole startup path, four times the size of the entire SPSC mesh.
    ///
    /// The doubling below restores the original growth curve by the fifth
    /// slab, so a table that actually fills sees the same amortised behaviour.
    fn with_first_slab(first_slab: usize) -> Self {
        SegmentSlab {
            slabs: Vec::new(),
            index_map: Vec::new(),
            next_slab_capacity: first_slab.clamp(1, MAX_SLAB_SEGMENTS),
        }
    }

    fn new() -> Self {
        Self::with_first_slab(1)
    }

    /// Add a segment, returning its flat index.
    fn push(&mut self, segment: Segment<K, V>) -> usize {
        // Check if current last slab has room
        let needs_new_slab = self
            .slabs
            .last()
            .map_or(true, |last| last.len() >= last.capacity());

        if needs_new_slab {
            let cap = self.next_slab_capacity;
            self.slabs.push(Vec::with_capacity(cap));
            // Double for next time, cap at MAX_SLAB_SEGMENTS
            self.next_slab_capacity = (cap * 2).min(MAX_SLAB_SEGMENTS);
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

    /// Segment SLOTS reserved across every slab — allocated capacity, not
    /// occupancy. `reserved() - len()` slots are memory the allocator has
    /// handed out for segments that do not exist yet, and each slot is a full
    /// `size_of::<Segment<K, V>>()` (3,456 B for the KV table).
    #[inline]
    fn reserved(&self) -> usize {
        self.slabs.iter().map(Vec::capacity).sum()
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
    /// Cumulative number of `split_segment` invocations since construction.
    /// Used by perf-regression tests and `MEMORY DOCTOR` to verify pre-sizing
    /// successfully eliminated split cost on production keyspaces.
    split_count: u64,
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
            split_count: 0,
        }
    }

    /// Create a DashTable pre-sized for approximately `cap` entries.
    ///
    /// Allocates one extra depth level (2x segments) beyond the strict
    /// `cap / LOAD_THRESHOLD` formula to absorb birthday-paradox hash
    /// distribution variance. Without headroom, the most-loaded segment
    /// can exceed `LOAD_THRESHOLD` under real xxh64 distribution at ~98%
    /// fill, defeating the zero-split guarantee that pre-sizing exists
    /// to provide.
    pub fn with_capacity(cap: usize) -> Self {
        if cap == 0 {
            return Self::new();
        }
        let num_segments = (cap + segment::LOAD_THRESHOLD - 1) / segment::LOAD_THRESHOLD;
        let base_depth = if num_segments <= 1 {
            0
        } else {
            (num_segments as f64).log2().ceil() as u32
        };
        // Add +1 depth level (2x segments) to absorb birthday-paradox tail:
        // with N segments and M keys, the most-loaded segment has approximately
        // M/N + sqrt(2 * M/N * ln(N)) entries. At base_depth the average load
        // is close to LOAD_THRESHOLD, so the tail easily exceeds it. One extra
        // depth level halves the average load, keeping the max well below the
        // split threshold for production keyspace sizes (100K-10M keys).
        // Cost: ~2x structural overhead (~22 KB per 1M hint), negligible vs
        // the data itself and recouped by eliminating all split_segment CPU cost.
        let depth = base_depth + 1;
        let dir_size = 1usize << depth;
        // Exactly `dir_size` segments are pushed below, so size the first slab
        // to hold all of them: one allocation, none spare. Letting the doubling
        // growth reach `dir_size` instead would both fragment the segments
        // across ~log2(dir_size) slabs and over-reserve the last one.
        let mut segments = SegmentSlab::with_first_slab(dir_size);
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
            split_count: 0,
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

    /// Segment slots the slab allocator has **reserved** — including slots no
    /// segment occupies yet. Every reserved slot costs a full
    /// `size_of::<Segment<K, V>>()` whether or not it holds a segment, so
    /// `reserved_segment_slots() - segment_count()` is dead weight carried by
    /// every table in the process.
    ///
    /// Exposed because a `Database` is created 16 times per shard at boot and
    /// every one of them starts empty: over-reserving here multiplies by
    /// `16 x shards` before a single key exists.
    #[inline]
    pub fn reserved_segment_slots(&self) -> usize {
        self.segments.reserved()
    }

    /// Total number of `split_segment` invocations since construction.
    /// Used by perf-regression tests and `MEMORY DOCTOR` to verify pre-sizing
    /// successfully eliminated split cost on production keyspaces.
    #[inline]
    pub fn split_count(&self) -> u64 {
        self.split_count
    }

    /// Global directory depth (log2 of directory size).
    ///
    /// SCAN's 48-bit cursor mapping (`Database::scan_hot_page`) relies on
    /// `depth <= 48` so that equal-top-48-bit hashes always route to the
    /// same segment; a depth beyond 48 would need a 2^48-entry directory,
    /// unreachable on real hardware.
    #[inline]
    pub fn directory_depth(&self) -> u32 {
        self.depth
    }

    /// Resident bytes used by the DashTable structural overhead (segments +
    /// directory + index map). Does NOT include per-entry key/value data --
    /// that is tracked separately by `Database::used_memory`.
    ///
    /// O(1): `segment_count * size_of::<Segment>() + directory.len() * 8 + index_map overhead`.
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        let seg_bytes = self.segments.len() * std::mem::size_of::<Segment<CompactKey, V>>();
        let dir_bytes = self.directory.len() * std::mem::size_of::<usize>();
        let idx_bytes = self.segments.len() * std::mem::size_of::<(u32, u32)>();
        seg_bytes + dir_bytes + idx_bytes
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

    /// Find OR insert in a single SIMD probe (vs two for `get_mut` + `insert`).
    ///
    /// On hit: `update(&mut existing)` runs in place; returns `Updated`.
    /// On miss: `make_value()` produces the new value, then it's inserted at the
    /// already-located free slot in the segment that was just probed.
    ///
    /// On `NeedsSplit`: split the segment, then retry. The segment helper returns
    /// the unconsumed closures so we can reuse them after the split.
    pub fn insert_or_update<F, G>(
        &mut self,
        key: CompactKey,
        update: F,
        make_value: G,
    ) -> InsertOrUpdate<'_, V>
    where
        F: FnOnce(&mut V),
        G: FnOnce() -> V,
    {
        let hash = hash_key(key.as_ref());
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];

        // Prefetch segment data while computing h2/home buckets
        prefetch_segment(self.segments.get(seg_idx));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        // The key_ref borrow must not overlap with the move into `make`.
        // We pass key_lookup as &[u8] to the segment helper, and wrap `key`
        // into the `make` closure so it's only consumed on miss.
        // key.as_ref() returns &[u8] — we need to hold the borrow before
        // moving key into the closure. Use a raw pointer to break the overlap.
        let key_ptr = key.as_ref().as_ptr();
        let key_len = key.as_ref().len();

        let segment = self.segments.get_mut(seg_idx);
        // SAFETY: key_ptr/key_len from key.as_ref() are valid for key's lifetime.
        // key moves into `make` closure which runs AFTER the scan completes.
        let key_lookup = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };

        let outcome = segment.insert_or_update_at(h2_val, key_lookup, ba, bb, update, move || {
            (key, make_value())
        });

        match outcome {
            SegmentInsertOrUpdate::Inserted { slot } => {
                self.len += 1;
                // SAFETY: just-inserted slot has a FULL ctrl byte and an initialized
                // value (mirrors find at segment.rs:277 — FULL ctrl => values[slot]
                // initialized).
                InsertOrUpdate::Inserted(unsafe { self.segments.get_mut(seg_idx).value_mut(slot) })
            }
            SegmentInsertOrUpdate::Updated { slot } => {
                // SAFETY: matched-and-updated slot has a FULL ctrl byte
                // (mirrors find at segment.rs:277).
                InsertOrUpdate::Updated(unsafe { self.segments.get_mut(seg_idx).value_mut(slot) })
            }
            SegmentInsertOrUpdate::NeedsSplit { update, make } => {
                // Split and retry until the key's target segment has room,
                // mirroring `insert`'s recursive retry. A single split does
                // NOT guarantee room: when the overflowing segment's keys are
                // skewed on the next directory bit they can all land in the
                // same child, which is then still over LOAD_THRESHOLD.
                // Each split raises the target's local depth, so the loop
                // terminates once the colliding keys separate.
                let mut update = update;
                let mut make = make;
                let mut split_dir_idx = dir_idx;
                let (final_seg_idx, slot, inserted) = loop {
                    self.split_segment(split_dir_idx);

                    // After split, the directory may have doubled and the key
                    // now routes to a different segment. Recompute.
                    split_dir_idx = segment_index(hash, self.depth);
                    let new_seg_idx = self.directory[split_dir_idx];
                    let new_segment = self.segments.get_mut(new_seg_idx);

                    match new_segment.insert_or_update_at(h2_val, key_lookup, ba, bb, update, make)
                    {
                        SegmentInsertOrUpdate::Inserted { slot } => {
                            break (new_seg_idx, slot, true);
                        }
                        SegmentInsertOrUpdate::Updated { slot } => {
                            break (new_seg_idx, slot, false);
                        }
                        SegmentInsertOrUpdate::NeedsSplit { update: u, make: m } => {
                            update = u;
                            make = m;
                        }
                    }
                };
                if inserted {
                    self.len += 1;
                    // SAFETY: just-inserted (mirrors find at segment.rs:277).
                    InsertOrUpdate::Inserted(unsafe {
                        self.segments.get_mut(final_seg_idx).value_mut(slot)
                    })
                } else {
                    // SAFETY: matched-and-updated (mirrors find at segment.rs:277).
                    InsertOrUpdate::Updated(unsafe {
                        self.segments.get_mut(final_seg_idx).value_mut(slot)
                    })
                }
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

    /// Hash-ordered page collection for SCAN (#368 O(COUNT) walk).
    ///
    /// Because the extendible-hashing directory is indexed by the hash's
    /// TOP `global_depth` bits (`segment_index`), ascending directory order
    /// is ascending hash order, and directory entry `d` covers exactly the
    /// hash range `[d << (64-D), (d+1) << (64-D))`. Segments are therefore
    /// range-partitioned: every entry of the segment at a lower directory
    /// index hashes below every entry at a higher one. This walk starts at
    /// the segment covering `from_hash` and visits segments in ascending
    /// range order, stopping as soon as `want` qualifying entries are
    /// collected — later segments can only contain larger hashes, so the
    /// result is complete without touching the rest of the table.
    ///
    /// A segment with `local_depth < global_depth` occupies a CONTIGUOUS
    /// run of directory slots, so alias-dedup is a consecutive store-index
    /// comparison.
    ///
    /// Returns entries with `hash_key(key) >= from_hash` passing `alive`,
    /// sorted ascending by `(hash, key)`, plus `true` if the walk stopped
    /// with unvisited segments remaining (i.e. more entries may exist).
    ///
    /// Split/merge/directory-doubling between calls is safe by
    /// construction: the caller's cursor is a position in hash space, and
    /// structural churn only changes WHICH segment covers that position,
    /// never the set of keys at or above it.
    pub fn hash_page<F: Fn(&CompactKey, &V) -> bool>(
        &self,
        from_hash: u64,
        want: usize,
        alive: F,
    ) -> (Vec<(u64, CompactKey)>, bool) {
        let mut out: Vec<(u64, CompactKey)> = Vec::with_capacity(want.min(1024));
        let start = segment_index(from_hash, self.depth);
        let mut last_store_idx = usize::MAX;
        let mut dir_idx = start;
        while dir_idx < self.directory.len() {
            let store_idx = self.directory[dir_idx];
            if store_idx != last_store_idx {
                last_store_idx = store_idx;
                if out.len() >= want {
                    // Enough collected and at least one unvisited segment
                    // remains; everything in it hashes above what we have.
                    return (Self::finish_page(out), true);
                }
                let seg = self.segments.get(store_idx);
                for (k, v) in seg.iter_occupied() {
                    let h = hash_key(k.as_ref());
                    if h >= from_hash && alive(k, v) {
                        out.push((h, k.clone()));
                    }
                }
            }
            dir_idx += 1;
        }
        (Self::finish_page(out), false)
    }

    fn finish_page(mut out: Vec<(u64, CompactKey)>) -> Vec<(u64, CompactKey)> {
        out.sort_unstable_by(|a, b| (a.0, a.1.as_bytes()).cmp(&(b.0, b.1.as_bytes())));
        out
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
        self.split_count += 1;
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
    use super::segment::{LOAD_THRESHOLD, TOTAL_SLOTS};
    use super::*;

    fn test_value(n: u32) -> String {
        format!("value_{}", n)
    }

    /// Structural cost per stored key, pinned so a layout change cannot silently
    /// inflate RSS on a 100M-key dataset.
    ///
    /// A `Segment<CompactKey, CompactEntry>` is the unit of allocation: one
    /// cache line of control bytes, 8 bytes of metadata, then `TOTAL_SLOTS`
    /// key slots and `TOTAL_SLOTS` value slots. Segments live in slab `Vec`s,
    /// so there is no per-segment allocator rounding — the slab pays it once.
    ///
    /// Divide by the achieved fill to get bytes/key. A split halves a segment,
    /// so live segments hold `LOAD_THRESHOLD/2 .. LOAD_THRESHOLD` keys and the
    /// population mean sits at ~3/4 of the threshold. Measured over 200k
    /// 16-byte keys (the `redis-benchmark -r` shape), the fill ratio is 0.7503
    /// at threshold 54 and 0.7690 at 56 -- so the 3/4 rule is real, not assumed.
    ///
    ///   * threshold 56 -> 43.07 keys/segment -> ~70 bytes/key;
    ///   * `with_capacity` deliberately over-allocates one depth level, halving
    ///     the fill -> ~107 bytes/key.
    ///
    /// Redis 7.0.15 for comparison, per key: `dictEntry` 24 B in jemalloc's
    /// 32-byte class, plus ~12 B of `dictEntry*` bucket array (the table doubles
    /// at load factor 1.0, so buckets/key averages ~1.5), plus a separately
    /// allocated key `sds` — which moon does not pay at all for keys <= 23 bytes
    /// because `CompactKey` inlines them into the slot counted here.
    #[test]
    fn segment_structural_cost_per_key_is_pinned() {
        use crate::storage::entry::CompactEntry;

        let seg = std::mem::size_of::<Segment<CompactKey, CompactEntry>>();

        assert_eq!(std::mem::size_of::<CompactKey>(), 24);
        assert_eq!(
            std::mem::size_of::<CompactEntry>(),
            24,
            "CompactEntry must stay 24 B: the 8-byte `ttl_ms` moved to the              Database-owned `expires` map (Redis's `db->expires`), because it              was charged to EVERY slot -- 60 per segment -- while fewer than              1 key in 1000 of a cache workload has a TTL at all."
        );
        assert_eq!(
            TOTAL_SLOTS, 61,
            "61, not 60: the 64-byte alignment rounds the segment up to 3008 B \
             either way, so the 60-slot layout left 48 B of tail padding -- \
             exactly one more (key, value) pair. See segment_wastes_no_tail_padding."
        );
        assert_eq!(
            LOAD_THRESHOLD, 56,
            "56, not 54: raising it lifts the measured fill from 40.52 to 43.07 \
             keys/segment, worth 4.39 B/key. It is affordable only because the \
             free 61st slot keeps overflow headroom at 5 -- non-home segments \
             measured 1.10% here versus 1.42% at the old (60, 54)."
        );

        // 64 (ctrl) + 8 (count/depth) + 1 (has_non_home_keys) + padding
        // + 61*24 (keys) + 61*24 (values) = 3008 exactly, no tail padding.
        assert_eq!(
            seg, 3008,
            "Segment layout changed; recompute the per-key memory ledger"
        );

        // Bytes/key at the three fills that actually occur.
        assert_eq!(
            seg / LOAD_THRESHOLD,
            53,
            "best case, at the split threshold"
        );
        assert_eq!(
            seg * 4 / (LOAD_THRESHOLD * 3),
            71,
            "organic fill; measured 69.85 at 43.07 keys/segment. Was 85 with the \
             32-byte entry and 74 after the entry shrink alone."
        );
        assert_eq!(
            seg * 2 / LOAD_THRESHOLD,
            107,
            "--initial-keyspace-hint: with_capacity adds a depth level, halving fill"
        );
    }

    #[test]
    fn test_insert_or_update_survives_skewed_double_split() {
        // Keys sharing the top 12 hash bits route to the same segment for
        // every split up to depth 12, so one split cannot separate them.
        // Filling a segment past LOAD_THRESHOLD with such keys makes the
        // post-split retry inside insert_or_update hit NeedsSplit again —
        // the old code declared that unreachable and panicked (reproduced
        // live replaying a 219k-key WAL checkpoint on recovery).
        const SKEW_BITS: u32 = 12;
        let want = LOAD_THRESHOLD + 2;
        let target = hash_key(b"skew_0") >> (64 - SKEW_BITS);
        let mut keys = Vec::with_capacity(want);
        let mut i = 0u64;
        while keys.len() < want {
            let k = format!("skew_{i}");
            if hash_key(k.as_bytes()) >> (64 - SKEW_BITS) == target {
                keys.push(k);
            }
            i += 1;
        }

        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for (n, k) in keys.iter().enumerate() {
            let mut updated = false;
            table.insert_or_update(
                CompactKey::from(k.clone()),
                |_| updated = true,
                || format!("v{n}"),
            );
            assert!(!updated, "unexpected in-place update for fresh key {k}");
        }

        assert_eq!(table.len(), want);
        for (n, k) in keys.iter().enumerate() {
            assert_eq!(
                table.get(k.as_bytes()),
                Some(&format!("v{n}")),
                "missing {k} after skewed splits"
            );
        }
    }

    #[test]
    fn hash_page_empty_table_is_terminal() {
        let table: DashTable<CompactKey, String> = DashTable::new();
        let (page, more) = table.hash_page(0, 16, |_, _| true);
        assert!(page.is_empty());
        assert!(!more);
    }

    #[test]
    fn hash_page_alive_filter_and_more_flag() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..2000u32 {
            table.insert(CompactKey::from(format!("af_{i}")), test_value(i));
        }
        // Filter out every odd value: only evens may appear.
        let (page, _) = table.hash_page(0, 200, |_, v| {
            let n: u32 = v.trim_start_matches("value_").parse().unwrap();
            n % 2 == 0
        });
        assert!(!page.is_empty());
        for (_, k) in &page {
            let n: u32 = std::str::from_utf8(k.as_ref())
                .unwrap()
                .trim_start_matches("af_")
                .parse()
                .unwrap();
            assert_eq!(n % 2, 0, "alive filter leaked odd key {n}");
        }
        // A want larger than the table drains everything in one page.
        let (all, more) = table.hash_page(0, usize::MAX, |_, _| true);
        assert_eq!(all.len(), 2000);
        assert!(!more, "full drain must report no further segments");
    }

    #[test]
    fn hash_page_drains_in_hash_order_under_split_churn() {
        // 4000 keys force many segment splits + directory doublings; paging
        // with concurrent inserts between pages exercises the split-safety
        // claim (cursor is a hash-space position, not a structure position).
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        let mut original: Vec<String> = Vec::with_capacity(4000);
        for i in 0..4000u32 {
            let k = format!("hp_{i}");
            table.insert(CompactKey::from(k.clone()), test_value(i));
            original.push(k);
        }

        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut cursor = 0u64;
        let mut churn = 0u32;
        loop {
            let (page, more) = table.hash_page(cursor, 64, |_, _| true);
            let mut prev: Option<(u64, &[u8])> = None;
            for (h, k) in &page {
                assert!(*h >= cursor, "entry below cursor");
                assert_eq!(*h, hash_key(k.as_ref()), "stale hash in page");
                if let Some((ph, pk)) = prev {
                    assert!(
                        (ph, pk) < (*h, k.as_ref()),
                        "page not ascending by (hash, key)"
                    );
                }
                prev = Some((*h, k.as_ref()));
            }
            if page.is_empty() {
                assert!(!more, "empty page must be terminal");
                break;
            }
            for (_, k) in &page {
                assert!(
                    seen.insert(k.as_ref().to_vec()),
                    "duplicate key across pages: {:?}",
                    String::from_utf8_lossy(k.as_ref())
                );
            }
            #[allow(clippy::unwrap_used)] // page verified non-empty above
            let last = page.last().unwrap().0;
            cursor = last + 1;
            if !more {
                break;
            }
            // Structural churn between pages: force splits mid-walk.
            for _ in 0..50 {
                table.insert(
                    CompactKey::from(format!("churn_{churn}")),
                    test_value(churn),
                );
                churn += 1;
            }
        }

        for k in &original {
            assert!(
                seen.contains(k.as_bytes()),
                "stable key {k} lost during split churn"
            );
        }
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

    /// An EMPTY table must not pre-pay for segments it does not have.
    ///
    /// moon boots `--databases 16` tables **per shard**, all empty, so every
    /// reserved-but-unoccupied segment slot is multiplied by `16 x shards`
    /// before a single key exists. At `size_of::<Segment<CompactKey,
    /// CompactEntry>>() == 3,456 B`, the historical 16-slot first slab cost
    /// 55,296 B per database — 93.75% of it for segments that would never
    /// exist on an idle server — i.e. ~874 KB per shard.
    #[test]
    fn empty_table_does_not_over_reserve_segment_slots() {
        let table: DashTable<CompactKey, String> = DashTable::new();
        assert_eq!(table.segment_count(), 1, "an empty table holds one segment");
        assert!(
            table.reserved_segment_slots() <= 2,
            "an empty DashTable reserved {} segment slots for {} live segment(s); \
             every spare slot is a full Segment of dead weight, charged 16x per shard",
            table.reserved_segment_slots(),
            table.segment_count()
        );
    }

    /// Pre-sizing must reserve for the segments it actually creates, not round
    /// up to a fixed slab size. `with_capacity(100)` needs 4 segments; a
    /// 16-slot first slab reserves four times that.
    #[test]
    fn presized_table_does_not_over_reserve_segment_slots() {
        let table: DashTable<CompactKey, String> = DashTable::with_capacity(100);
        let live = table.segment_count();
        assert!(live > 0, "pre-sizing must allocate at least one segment");
        assert!(
            table.reserved_segment_slots() <= live * 2,
            "with_capacity(100) reserved {} segment slots for {live} live segments",
            table.reserved_segment_slots()
        );
    }

    /// Growth must stay pointer-stable: a slab is never reallocated, so a new
    /// slab is pushed whenever the last one is full. Shrinking the first slab
    /// must not break that — walk a table well past several slab boundaries
    /// and confirm every key is still findable.
    #[test]
    fn growth_across_slab_boundaries_keeps_every_key() {
        let mut table: DashTable<CompactKey, u64> = DashTable::new();
        for i in 0..20_000u64 {
            table.insert(CompactKey::from(format!("growth:key:{i}")), i);
        }
        assert_eq!(table.len(), 20_000);
        assert!(
            table.segment_count() > 16,
            "the fixture must cross several slab boundaries, got {} segments",
            table.segment_count()
        );
        for i in 0..20_000u64 {
            let key = format!("growth:key:{i}");
            assert_eq!(
                table.get(key.as_bytes()),
                Some(&i),
                "key {i} lost across slab growth"
            );
        }
        assert!(
            table.reserved_segment_slots() < table.segment_count() * 2,
            "growth reserved {} slots for {} segments",
            table.reserved_segment_slots(),
            table.segment_count()
        );
    }

    #[test]
    fn test_split_count_starts_at_zero() {
        let table: DashTable<CompactKey, String> = DashTable::new();
        assert_eq!(table.split_count(), 0);
    }

    #[test]
    fn test_split_count_with_capacity_starts_at_zero() {
        // Pre-sized allocation must NOT count as splits.
        let table: DashTable<CompactKey, String> = DashTable::with_capacity(1_000_000);
        assert_eq!(table.split_count(), 0);
        assert!(
            table.segment_count() > 1,
            "with_capacity must allocate >1 segment for 1M hint"
        );
    }

    #[test]
    fn test_split_count_grows_under_load_without_capacity() {
        let mut table: DashTable<CompactKey, String> = DashTable::new();
        for i in 0..2000 {
            table.insert(
                CompactKey::from(format!("split_count_{:06}", i)),
                format!("v_{}", i),
            );
        }
        assert!(
            table.split_count() > 0,
            "Expected splits after 2000 inserts on a default-sized table; got {}",
            table.split_count()
        );
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

    /// A segment is 64-byte aligned, so its footprint is rounded up. Any bytes
    /// past the last value slot are pure waste: they are mapped, they are paid
    /// for on every segment, and they store nothing.
    ///
    /// This pins the waste below one slot's worth. If it ever reaches a full
    /// `(key, value)` pair, the segment is carrying a slot it refuses to use —
    /// per-key overhead we are paying and not spending.
    #[test]
    fn segment_wastes_no_tail_padding() {
        use crate::storage::entry::CompactEntry;

        let seg = std::mem::size_of::<Segment<CompactKey, CompactEntry>>();
        let values_at = std::mem::offset_of!(Segment<CompactKey, CompactEntry>, values);
        let used = values_at + TOTAL_SLOTS * std::mem::size_of::<CompactEntry>();
        let waste = seg - used;
        let slot = std::mem::size_of::<CompactKey>() + std::mem::size_of::<CompactEntry>();

        assert!(
            waste < slot,
            "segment is {seg} B, uses {used} B, wastes {waste} B of tail padding — \
             enough for {} more slot(s) of {slot} B at zero extra memory",
            waste / slot
        );
    }
}
