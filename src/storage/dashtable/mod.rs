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
//! Hash routing (one xxh64, three disjoint consumers — moon#1159):
//! - directory: the TOP `depth` bits pick the segment
//! - home buckets: `(hash >> 8) % 56` and `(hash >> 16) % 56`
//! - H2 (bits 32..=38, [`segment::H2_SHIFT`]): control byte fingerprint for
//!   SIMD matching. It must not overlap the directory bits, or every key in a
//!   deep segment shares one fingerprint and the SIMD filter matches them all.

pub mod iter;
pub mod segment;
pub mod simd;

use super::compact_key::CompactKey;

use iter::{Iter, IterMut, Keys, Values};
pub use segment::RemoveIf;
use segment::{InsertResult, Segment, UpsertProbe, h2, home_buckets};

/// Outcome of [`DashTable::insert_or_update`].
pub enum InsertOrUpdate<'a, V> {
    /// Key was new and has just been inserted.
    Inserted(&'a mut V),
    /// Key existed; the user-supplied closure was invoked.
    Updated(&'a mut V),
}

/// The key an upsert probes with: owned (moved in on a miss) or borrowed
/// (copied into a `CompactKey` only on a miss).
enum UpsertKey<'k> {
    Owned(CompactKey),
    Borrowed(&'k [u8]),
}

impl UpsertKey<'_> {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            UpsertKey::Owned(k) => k.as_bytes(),
            UpsertKey::Borrowed(b) => b,
        }
    }

    /// The owned key to store. Called on the miss path only.
    #[inline]
    fn into_key(self) -> CompactKey {
        note_upsert_key_build();
        match self {
            UpsertKey::Owned(k) => k,
            UpsertKey::Borrowed(b) => CompactKey::from(b),
        }
    }
}

// Per-thread count of owned keys an upsert STORED (test builds only). An
// upsert that hits must store nothing — the moon#1159 follow-up that stopped
// `Database::set` building a `CompactKey` per overwrite. Plain `//`: a doc
// comment on a macro invocation trips `unused_doc_comments`.
#[cfg(test)]
thread_local! {
    static UPSERT_KEY_BUILDS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
#[inline]
fn note_upsert_key_build() {
    UPSERT_KEY_BUILDS.with(|c| c.set(c.get() + 1));
}

#[cfg(not(test))]
#[inline(always)]
fn note_upsert_key_build() {}

/// Read and reset the per-thread upsert key-build counter.
#[cfg(test)]
pub(crate) fn take_upsert_key_builds() -> u32 {
    UPSERT_KEY_BUILDS.with(|c| c.replace(0))
}

/// Compute the xxh64 hash of a byte slice.
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(key, 0)
}

// Per-thread count of DashTable KEY LOOKUPS (test builds only).
//
// One "lookup" is one walk of a segment for a key: `get`, `get_mut`, `insert`,
// `insert_or_update`, `remove` and `remove_entry`. `contains_key` delegates to
// `get` and is therefore counted once, not twice.
//
// A SPLIT-AND-RETRY is counted again, in both shapes. `insert` retries by
// recursing, so its entry-point call site covers it; `insert_or_update` retries
// in a loop that re-enters `Segment::insert_or_update_at` WITHOUT re-entering
// `insert_or_update`, so that loop carries its own call. Counting only the
// first would make the fused path look cheaper than the legacy one under a
// split purely because of how each spells its retry — the exact false
// comparison PERF-08 exists to prevent. Note the retry reuses the already
// computed `hash`, so it is a segment walk without a rehash; the counter does
// not distinguish the two, and neither does any claim made from it.
//
// This is COARSER than `segment::take_simd_probes`, deliberately. The SIMD
// counter measures control-byte GROUP SCANS, which vary with a segment's
// occupancy and pin PERF-08's fusion claim. This one measures how many times a
// caller hashes a key and walks a segment at all — the quantity the moon#942
// accessor audit is about, and the only one that is a property of the ACCESSOR
// rather than of the table's current fill.
//
// Plain `//` comments, not `///`: a doc comment on a macro invocation trips
// `unused_doc_comments`, which is denied in CI.
//
// Compiles to nothing outside `cfg(test)` — it sits in the hottest lookups in
// the codebase (moon#789 set this precedent for `note_simd_probe`).
#[cfg(test)]
thread_local! {
    static KEY_LOOKUPS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Record one DashTable key lookup. No-op outside test builds.
#[cfg(test)]
#[inline]
fn note_key_lookup() {
    KEY_LOOKUPS.with(|c| c.set(c.get() + 1));
}

/// No-op in non-test builds — zero production cost.
#[cfg(not(test))]
#[inline(always)]
fn note_key_lookup() {}

/// Read and reset the per-thread key-lookup counter.
///
/// `pub(crate)` so the storage accessors' probe-budget tests
/// (`storage::db::probe_budget`) can measure a whole accessor call, which is
/// where moon#942's probe budget actually lives.
#[cfg(test)]
pub(crate) fn take_key_lookups() -> u32 {
    KEY_LOOKUPS.with(|c| c.replace(0))
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
    /// Read by the perf-regression tests and `examples/dashtable_growth.rs`
    /// to verify pre-sizing (`with_capacity`) eliminated split cost; nothing
    /// on the command path reads it.
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
    ///
    /// Read by the perf-regression tests and `examples/dashtable_growth.rs`
    /// (the G1/L4 growth micro-bench) to verify pre-sizing eliminated split
    /// cost, and by `split_segment`'s own `cfg(test)` differential check.
    /// Reachable from a `Database` as `db.data().split_count()`.
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
        note_key_lookup();
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
        note_key_lookup();
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
        note_key_lookup();
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
    /// On a full segment: split, then retry — see [`Self::upsert`].
    ///
    /// Takes the key OWNED. A caller that only holds the bytes should use
    /// [`Self::insert_or_update_slice`], which builds the `CompactKey` only on
    /// a miss instead of on every call.
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
        self.upsert(UpsertKey::Owned(key), update, make_value)
    }

    /// [`Self::insert_or_update`] keyed by a borrowed slice (moon#1159
    /// follow-up): the owned `CompactKey` is built ONLY when the key is new.
    ///
    /// `Database::set` used to call `insert_or_update(CompactKey::from(key),
    /// ..)`, so every SET overwrite of a key longer than 23 bytes allocated a
    /// heap key block just to drop it again after the probe found the key.
    pub fn insert_or_update_slice<F, G>(
        &mut self,
        key: &[u8],
        update: F,
        make_value: G,
    ) -> InsertOrUpdate<'_, V>
    where
        F: FnOnce(&mut V),
        G: FnOnce() -> V,
    {
        self.upsert(UpsertKey::Borrowed(key), update, make_value)
    }

    /// The one upsert implementation behind both entry points.
    ///
    /// Phase 1 (`Segment::probe_for_upsert`) scans the key's home groups once
    /// and answers with plain slot indexes; phase 2 either updates the found
    /// value or writes the new pair into the located free slot. Because the
    /// probe holds no borrow of the key, the owned key moves in only after the
    /// scan — which is what retired the raw-pointer `from_raw_parts` view of a
    /// key that was simultaneously being moved into a closure.
    ///
    /// A full segment is split and the probe retried until the key's target
    /// segment has room. A single split does NOT guarantee room: when the
    /// overflowing segment's keys are skewed on the next directory bit they
    /// can all land in the same child, which is then still over
    /// LOAD_THRESHOLD. Each split raises the target's local depth, so the loop
    /// terminates once the colliding keys separate.
    fn upsert<F, G>(
        &mut self,
        key: UpsertKey<'_>,
        update: F,
        make_value: G,
    ) -> InsertOrUpdate<'_, V>
    where
        F: FnOnce(&mut V),
        G: FnOnce() -> V,
    {
        note_key_lookup();
        let hash = hash_key(key.bytes());
        let mut dir_idx = segment_index(hash, self.depth);

        // Prefetch segment data while computing h2/home buckets
        prefetch_segment(self.segments.get(self.directory[dir_idx]));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        let (seg_idx, probe) = loop {
            let seg_idx = self.directory[dir_idx];
            match self
                .segments
                .get_mut(seg_idx)
                .probe_for_upsert(h2_val, key.bytes(), ba, bb)
            {
                UpsertProbe::Full => {
                    self.split_segment(dir_idx);
                    // The retry re-walks a segment. It reuses `hash`, so it
                    // is cheaper than a fresh lookup — but it is a segment
                    // walk, which is what the counter counts, and leaving it
                    // out would make `Database::set` (fused) and
                    // `DashTable::insert` (recursive, and therefore counted
                    // again) disagree under a split for no reason.
                    note_key_lookup();
                    // After split, the directory may have doubled and the key
                    // now routes to a different segment. Recompute.
                    dir_idx = segment_index(hash, self.depth);
                }
                found_or_vacant => break (seg_idx, found_or_vacant),
            }
        };

        let segment = self.segments.get_mut(seg_idx);
        match probe {
            UpsertProbe::Found(slot) => {
                // SAFETY: `probe_for_upsert` answers `Found` only for a FULL slot
                // (ctrl byte matched H2, key compared equal), so its value is
                // initialized; nothing has touched the segment since the probe.
                let existing = unsafe { segment.value_mut(slot) };
                update(&mut *existing);
                InsertOrUpdate::Updated(existing)
            }
            UpsertProbe::Vacant(slot) => {
                let value =
                    segment.write_vacant(slot, h2_val, key.into_key(), make_value(), ba, bb);
                self.len += 1;
                InsertOrUpdate::Inserted(value)
            }
            UpsertProbe::Full => unreachable!("the probe loop only exits on Found or Vacant"),
        }
    }

    /// Remove a key from the table. Returns `Some(value)` if the key existed.
    ///
    /// Matches HashMap's `remove` semantics: returns only the value, dropping the key.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        note_key_lookup();
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

    /// Remove `key` only if `pred(&value)` agrees, in ONE probe (moon#1189:
    /// the expiry sweep's "remove it if it is still the expired incarnation"
    /// used to cost a `get` and then a `remove`, i.e. two hashes and two
    /// segment walks per expired key).
    pub fn remove_if(&mut self, key: &[u8], pred: impl FnOnce(&V) -> bool) -> RemoveIf<V> {
        note_key_lookup();
        let hash = hash_key(key);
        let dir_idx = segment_index(hash, self.depth);
        let seg_idx = self.directory[dir_idx];

        // Prefetch segment data while computing home bucket
        prefetch_segment(self.segments.get(seg_idx));

        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        match self
            .segments
            .get_mut(seg_idx)
            .remove_if(h2_val, key, ba, bb, pred)
        {
            RemoveIf::Removed((_k, v)) => {
                self.len -= 1;
                RemoveIf::Removed(v)
            }
            RemoveIf::Kept => RemoveIf::Kept,
            RemoveIf::Absent => RemoveIf::Absent,
        }
    }

    /// Remove a key and return both key and value.
    #[allow(dead_code)]
    pub fn remove_entry(&mut self, key: &[u8]) -> Option<(CompactKey, V)> {
        note_key_lookup();
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
    /// 3. Repoint the upper half of the split segment's directory block at
    ///    the new segment
    ///
    /// Step 3 is O(block), not O(directory). Extendible hashing keeps the
    /// invariant the whole table rests on: the directory slots routing to a
    /// segment `S` of local depth `d` are exactly the aligned block of
    /// `2^(depth - d)` consecutive slots whose top `d` bits are `S`'s prefix.
    /// `Segment::split` raises `S` to depth `d + 1` and moves the keys whose
    /// next hash bit is 1, so the new segment owns the upper half of that
    /// block — the slots with bit `(depth - (d + 1))` set — and no other slot
    /// changes. Before G1/L4 this scanned every directory slot per split,
    /// O(2^depth): on a 2M-key fill that scan was 65% of the wall clock and
    /// grew as O(N^2 / segment_capacity) (moon-bench-x86, G1 §6.4).
    ///
    /// Under `cfg(test)` the old scan is kept as the oracle
    /// (`repoint_by_full_scan`) and every split is checked against it.
    fn split_segment(&mut self, dir_idx: usize) {
        self.split_count += 1;
        let seg_store_idx = self.directory[dir_idx];
        let hasher = |k: &CompactKey| hash_key(k.as_ref());
        let new_seg = self.segments.get_mut(seg_store_idx).split(&hasher);
        let new_depth = new_seg.depth();

        // Add new segment to the slab store
        let new_store_idx = self.segments.push(new_seg);

        // Double directory if needed. Each doubling maps slot `i` to `2i` and
        // `2i + 1`, so `dir_idx` lands at `dir_idx << doublings` afterwards.
        // At most one doubling happens per split (`new_depth <= depth + 1`).
        let mut doublings = 0u32;
        while new_depth > self.depth {
            let old_len = self.directory.len();
            let mut new_dir = Vec::with_capacity(old_len * 2);
            for &idx in &self.directory {
                new_dir.push(idx);
                new_dir.push(idx);
            }
            self.directory = new_dir;
            self.depth += 1;
            doublings += 1;
        }

        #[cfg(test)]
        let reference = repoint_by_full_scan(
            &self.directory,
            self.depth,
            new_depth,
            seg_store_idx,
            new_store_idx,
        );

        // The split segment had local depth `new_depth - 1`, so its slots are
        // the aligned block of `span = 2^(depth - (new_depth - 1))` containing
        // `dir_idx` (mapped through the doubling). The upper half of that
        // block — bit `(depth - new_depth)` set — now routes to the new
        // segment; the lower half keeps routing to the old one.
        let old_local_depth = new_depth - 1;
        let span = 1usize << (self.depth - old_local_depth);
        let block_start = (dir_idx << doublings) & !(span - 1);
        for slot in &mut self.directory[block_start + span / 2..block_start + span] {
            debug_assert_eq!(
                *slot, seg_store_idx,
                "directory block invariant violated: a slot in the split segment's block \
                 routed elsewhere (dir_idx={dir_idx}, span={span}, block_start={block_start})"
            );
            *slot = new_store_idx;
        }

        #[cfg(test)]
        {
            if let Some(i) = (0..self.directory.len()).find(|&i| self.directory[i] != reference[i])
            {
                panic!(
                    "split #{}: directory repoint diverged from the full-scan reference at slot {i} \
                     (got {}, reference {}; dir_idx={dir_idx}, depth={}, new_depth={new_depth}, \
                     seg={seg_store_idx}, new={new_store_idx})",
                    self.split_count, self.directory[i], reference[i], self.depth
                );
            }
            DIFFERENTIAL_CHECKS.with(|c| c.set(c.get() + 1));
        }
    }
}

/// The pre-G1/L4 directory update — a scan of EVERY directory slot per
/// split — kept verbatim as the differential oracle for `split_segment`.
/// Returns what that scan would have produced from the already-doubled
/// directory. Test-only: it is O(2^depth) per split, which is exactly the
/// cost L4 removed.
#[cfg(test)]
fn repoint_by_full_scan(
    directory: &[usize],
    depth: u32,
    new_depth: u32,
    seg_store_idx: usize,
    new_store_idx: usize,
) -> Vec<usize> {
    let mut out = directory.to_vec();
    let bit_pos = new_depth - 1;
    for (i, slot) in out.iter_mut().enumerate() {
        if *slot == seg_store_idx {
            let bit_in_idx = depth - 1 - bit_pos;
            if (i >> bit_in_idx) & 1 == 1 {
                *slot = new_store_idx;
            }
        }
    }
    out
}

#[cfg(test)]
thread_local! {
    /// Number of `split_segment` calls on this thread whose directory was
    /// checked against `repoint_by_full_scan`. Lets the differential test
    /// prove the comparison ran once per split rather than trusting that it
    /// did.
    static DIFFERENTIAL_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl<'a, V> IntoIterator for &'a DashTable<CompactKey, V> {
    type Item = (&'a CompactKey, &'a V);
    type IntoIter = Iter<'a, CompactKey, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests;
