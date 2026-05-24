//! Segment: the core unit of DashTable with 56 regular + 4 stash buckets.
//!
//! Each segment holds up to 60 key-value pairs with Swiss Table control byte
//! probing. Control bytes are organized into 4 groups of 16 bytes (64 total).
//! Groups 0-3 cover slots 0-63; only slots 0-59 are usable (60-63 are padding,
//! always EMPTY).
//!
//! Slots 0-55: regular buckets (addressed by H1 hash bits)
//! Slots 56-59: stash buckets (overflow, linear scanned on every lookup)
//!
//! SAFETY INVARIANTS:
//! - A slot's `keys[i]` and `values[i]` are initialized if and only if
//!   `ctrl_byte(i)` is a valid H2 fingerprint (0x00..0x7F).
//! - EMPTY (0xFF) and DELETED (0x80) slots have uninitialized MaybeUninit data.
//! - Drop MUST iterate all FULL slots and call `assume_init_drop` on both key and value.
//!
//! Module layout:
//! - `mod.rs` — types, constants, Segment struct + basic accessors, Drop, Send/Sync, tests
//! - `find.rs` — find / get / get_mut / get_key_value / find_slot_mut
//! - `insert.rs` — insert / insert_or_update_at + helpers
//! - `ops.rs` — remove / split / insert_during_split / home_buckets

use std::mem::MaybeUninit;

use super::simd::{DELETED, EMPTY, Group};

mod find;
mod insert;
mod ops;

pub use ops::home_buckets;

/// Number of regular (home) bucket slots per segment.
pub const REGULAR_SLOTS: usize = 56;

/// Number of stash (overflow) bucket slots per segment.
pub const STASH_SLOTS: usize = 4;

/// Total slots per segment: 56 regular + 4 stash = 60.
pub const TOTAL_SLOTS: usize = REGULAR_SLOTS + STASH_SLOTS;

/// Number of control byte groups (each group = 16 bytes for SIMD).
pub(super) const NUM_GROUPS: usize = 4;

/// Padded control byte count: 4 groups x 16 = 64 bytes.
/// Slots 60-63 are padding (always EMPTY).
pub(super) const CTRL_BYTES: usize = NUM_GROUPS * 16;

/// Load threshold: 90% of 60 = 54 slots. Triggers split when reached.
/// Higher threshold improves average fill factor (~67% vs ~62% at 85%),
/// reducing per-key memory overhead by ~8% with minimal impact on probe length.
pub const LOAD_THRESHOLD: usize = 54;

/// Extract the H2 fingerprint from a hash: top 7 bits, ensuring MSB is 0
/// so the value (0x00..0x7F) is distinguishable from EMPTY (0xFF) and
/// DELETED (0x80).
#[inline]
pub fn h2(hash: u64) -> u8 {
    (hash >> 57) as u8 & 0x7F
}

/// Result of an insert operation on a segment.
pub enum InsertResult<K, V> {
    /// Key was new; entry was inserted successfully.
    Inserted,
    /// Key already existed; the old value is returned.
    Replaced(V),
    /// Segment is full; key and value returned so caller can split and retry.
    NeedsSplit(K, V),
}

/// Result of `Segment::insert_or_update_at`. The DashTable layer translates
/// this to `InsertOrUpdate<V>` after the slot lookup is borrow-checker-safe.
///
/// Generic over `F` and `G` so that on `NeedsSplit` the unconsumed closures
/// are returned to the caller for retry after splitting.
pub enum SegmentInsertOrUpdate<F, G> {
    /// New entry written at this slot.
    Inserted { slot: usize },
    /// Existing entry at this slot was passed to the update closure.
    Updated { slot: usize },
    /// Segment is at LOAD_THRESHOLD and the key is new — caller must split & retry.
    /// The unconsumed closures are returned so the caller can retry without
    /// re-constructing them.
    NeedsSplit { update: F, make: G },
}

/// A segment holding up to 60 key-value pairs with Swiss Table control bytes.
///
/// Memory layout (cache-line optimized):
/// - Cache line 0: `ctrl` -- 4 aligned groups of 16 control bytes (64 bytes total, hot read path)
/// - Cache line 1: `count` + `depth` (read-mostly metadata, 8 bytes)
/// - Remaining: `keys` + `values` arrays (accessed only on H2 match)
///
/// The `align(64)` ensures the ctrl array starts on a cache-line boundary,
/// preventing false sharing between segments owned by different shards.
#[repr(C, align(64))]
pub struct Segment<K, V> {
    // --- Cache line 0: control bytes (hot, read on every lookup) ---
    pub(super) ctrl: [Group; NUM_GROUPS], // 64 bytes exactly = 1 cache line
    // --- Cache line 1+: metadata ---
    pub(super) count: u32,
    pub(super) depth: u32,
    /// True if any key was placed via the "any free slot" fallback path
    /// during insert or split. When false, `find` can skip the expensive
    /// fallback scan of non-home groups (PERF-09 optimization).
    pub(super) has_non_home_keys: bool,
    /// Cumulative SIMD probe count for perf instrumentation (test-only).
    #[cfg(test)]
    pub(super) probe_count: u32,
    // --- Remaining cache lines: key/value data (accessed only on H2 hit) ---
    pub(super) keys: [MaybeUninit<K>; TOTAL_SLOTS],
    pub(super) values: [MaybeUninit<V>; TOTAL_SLOTS],
}

// Compile-time assertion: Segment must be cache-line aligned (64 bytes minimum).
const _: () = {
    assert!(std::mem::align_of::<Segment<u64, u64>>() >= 64);
};

/// Prefetch the key data at the given slot index into L1 cache.
///
/// On x86_64: uses `_mm_prefetch` with `_MM_HINT_T0` (all cache levels).
/// On other architectures (aarch64/macOS): no-op.
///
/// NOTE: An aarch64 `prfm pldl1keep` variant was measured and found to
/// INCREASE `Segment::find` self-time by ~3pp on ARM (tight LSU
/// back-pressure in the hot probe loop). Kept as a no-op on aarch64
/// until a smarter placement (only prefetching slots that are likely
/// to need a full memcmp) is implemented.
#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) unsafe fn prefetch_ptr(ptr: *const u8) {
    use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
    // SAFETY: `_mm_prefetch` is always safe to call with any pointer — it is
    // a CPU hint that ignores invalid addresses. The `unsafe` is required by
    // the intrinsic signature, not by the operation.
    unsafe { _mm_prefetch(ptr as *const i8, _MM_HINT_T0) };
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
pub(super) fn prefetch_ptr(_ptr: *const u8) {
    // No-op on non-x86_64 (macOS aarch64, etc.)
}

impl<K, V> Segment<K, V> {
    /// Create a new empty segment with the given local depth.
    pub fn new(depth: u32) -> Self {
        // Initialize all control bytes to EMPTY (including padding slots 60-63).
        let ctrl = [
            Group::new_empty(),
            Group::new_empty(),
            Group::new_empty(),
            Group::new_empty(),
        ];

        // SAFETY: MaybeUninit does not require initialization.
        let keys = unsafe { MaybeUninit::<[MaybeUninit<K>; TOTAL_SLOTS]>::uninit().assume_init() };
        let values =
            unsafe { MaybeUninit::<[MaybeUninit<V>; TOTAL_SLOTS]>::uninit().assume_init() };

        Segment {
            ctrl,
            count: 0,
            depth,
            has_non_home_keys: false,
            #[cfg(test)]
            probe_count: 0,
            keys,
            values,
        }
    }

    /// Return the current local depth.
    #[inline]
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// Return the number of occupied slots.
    #[inline]
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Check if the segment has reached the load threshold and needs splitting.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.count as usize >= LOAD_THRESHOLD
    }

    /// True if any key in this segment was placed in a non-home group via the
    /// "any free slot" fallback path. When false, `find` can skip the fallback
    /// scan of non-home groups entirely.
    #[inline]
    pub fn has_non_home_keys(&self) -> bool {
        self.has_non_home_keys
    }

    /// Read the control byte at the given slot index.
    ///
    /// # Panics
    /// Panics if `slot >= CTRL_BYTES` (debug only).
    #[inline]
    pub fn ctrl_byte(&self, slot: usize) -> u8 {
        debug_assert!(slot < CTRL_BYTES);
        let group = slot / 16;
        let pos = slot % 16;
        self.ctrl[group].0[pos]
    }

    /// Write a control byte at the given slot index.
    #[inline]
    pub fn set_ctrl_byte(&mut self, slot: usize, byte: u8) {
        debug_assert!(slot < CTRL_BYTES);
        let group = slot / 16;
        let pos = slot % 16;
        self.ctrl[group].0[pos] = byte;
    }

    /// Check if a control byte represents a FULL (occupied) slot.
    #[inline]
    pub(super) fn is_full_ctrl(byte: u8) -> bool {
        byte & 0x80 == 0 // H2 values are 0x00..0x7F (bit 7 clear)
    }

    /// Public version of is_full_ctrl for use by iterators.
    #[inline]
    pub fn is_full_ctrl_pub(byte: u8) -> bool {
        Self::is_full_ctrl(byte)
    }

    /// Iterate over all occupied (key, value) pairs in this segment.
    /// Returns references to initialized keys and values.
    pub fn iter_occupied(&self) -> impl Iterator<Item = (&K, &V)> {
        (0..TOTAL_SLOTS).filter_map(move |slot| {
            if Self::is_full_ctrl(self.ctrl_byte(slot)) {
                // SAFETY: slot is FULL, so key and value are initialized.
                Some(unsafe {
                    (
                        self.keys[slot].assume_init_ref(),
                        self.values[slot].assume_init_ref(),
                    )
                })
            } else {
                None
            }
        })
    }

    /// Get an immutable reference to the key at the given slot.
    ///
    /// # Safety
    /// Caller must ensure the slot is FULL (control byte in 0x00..0x7F).
    #[inline]
    pub unsafe fn key_ref(&self, slot: usize) -> &K {
        // SAFETY: Caller guarantees slot is FULL (initialized). See fn-level safety doc.
        unsafe { self.keys[slot].assume_init_ref() }
    }

    /// Get an immutable reference to the value at the given slot.
    ///
    /// # Safety
    /// Caller must ensure the slot is FULL (control byte in 0x00..0x7F).
    #[inline]
    pub unsafe fn value_ref(&self, slot: usize) -> &V {
        // SAFETY: Caller guarantees slot is FULL (initialized). See fn-level safety doc.
        unsafe { self.values[slot].assume_init_ref() }
    }

    /// Get a mutable reference to the value at the given slot.
    ///
    /// # Safety
    /// Caller must ensure the slot is FULL (control byte in 0x00..0x7F).
    #[inline]
    pub unsafe fn value_mut(&mut self, slot: usize) -> &mut V {
        // SAFETY: Caller guarantees slot is FULL (initialized). See fn-level safety doc.
        unsafe { self.values[slot].assume_init_mut() }
    }
}

impl<K, V> Drop for Segment<K, V> {
    fn drop(&mut self) {
        // SAFETY: We must drop all initialized slots. A slot is initialized
        // if and only if its control byte is a valid H2 (0x00..0x7F).
        for slot in 0..TOTAL_SLOTS {
            if Self::is_full_ctrl(self.ctrl_byte(slot)) {
                // SAFETY: ctrl byte is FULL, so key and value at this slot are initialized.
                unsafe {
                    self.keys[slot].assume_init_drop();
                    self.values[slot].assume_init_drop();
                }
            }
        }
    }
}

// SAFETY: Segment is Send if K and V are Send (no interior aliasing).
unsafe impl<K: Send, V: Send> Send for Segment<K, V> {}
// SAFETY: Segment is Sync if K and V are Sync (no interior mutability).
unsafe impl<K: Sync, V: Sync> Sync for Segment<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_new() {
        let seg: Segment<Vec<u8>, u64> = Segment::new(0);
        assert_eq!(seg.count(), 0);
        assert_eq!(seg.depth(), 0);
        assert!(!seg.is_full());

        // All 60 usable slots should be EMPTY
        for i in 0..TOTAL_SLOTS {
            assert_eq!(seg.ctrl_byte(i), EMPTY);
        }
        // Padding slots (60-63) should also be EMPTY
        for i in TOTAL_SLOTS..CTRL_BYTES {
            assert_eq!(seg.ctrl_byte(i), EMPTY);
        }
    }

    #[test]
    fn test_segment_insert_and_get() {
        let mut seg: Segment<Vec<u8>, String> = Segment::new(0);

        let keys = [b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let vals = ["val1".to_string(), "val2".to_string(), "val3".to_string()];

        for (k, v) in keys.iter().zip(vals.iter()) {
            let hash = simple_hash(k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            match seg.insert(h2_val, k.clone(), v.clone(), ba, bb) {
                InsertResult::Inserted => {}
                other => panic!("Expected Inserted, got {:?}", insert_result_name(&other)),
            }
        }

        assert_eq!(seg.count(), 3);

        // Retrieve each
        for (k, v) in keys.iter().zip(vals.iter()) {
            let hash = simple_hash(k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            let result = seg.get(h2_val, k.as_slice(), ba, bb);
            assert_eq!(result, Some(v));
        }

        // Non-existent key
        let hash = simple_hash(b"missing");
        let (ba, bb) = home_buckets(hash);
        assert_eq!(seg.get(h2(hash), b"missing".as_slice(), ba, bb), None);
    }

    #[test]
    fn test_segment_insert_replace() {
        let mut seg: Segment<Vec<u8>, String> = Segment::new(0);
        let k = b"mykey".to_vec();
        let hash = simple_hash(&k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        // First insert
        match seg.insert(h2_val, k.clone(), "first".to_string(), ba, bb) {
            InsertResult::Inserted => {}
            _ => panic!("Expected Inserted"),
        }
        assert_eq!(seg.count(), 1);

        // Replace
        match seg.insert(h2_val, k.clone(), "second".to_string(), ba, bb) {
            InsertResult::Replaced(old) => assert_eq!(old, "first"),
            _ => panic!("Expected Replaced"),
        }
        assert_eq!(seg.count(), 1); // count unchanged

        // Verify new value
        assert_eq!(
            seg.get(h2_val, k.as_slice(), ba, bb),
            Some(&"second".to_string())
        );
    }

    #[test]
    fn test_segment_remove() {
        let mut seg: Segment<Vec<u8>, String> = Segment::new(0);
        let k = b"removekey".to_vec();
        let hash = simple_hash(&k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        seg.insert(h2_val, k.clone(), "value".to_string(), ba, bb);
        assert_eq!(seg.count(), 1);

        // Remove
        let result = seg.remove(h2_val, k.as_slice(), ba, bb);
        assert_eq!(result, Some((k.clone(), "value".to_string())));
        assert_eq!(seg.count(), 0);

        // Get after remove
        assert_eq!(seg.get(h2_val, k.as_slice(), ba, bb), None);

        // Remove non-existent
        assert_eq!(seg.remove(h2_val, k.as_slice(), ba, bb), None);
    }

    #[test]
    fn test_segment_stash_overflow() {
        // Insert many entries that hash to the same groups to force stash usage.
        let mut seg: Segment<Vec<u8>, u32> = Segment::new(0);

        // Insert enough entries -- with our simple hash, they'll scatter but
        // eventually some will land in stash once groups fill up.
        let mut inserted = 0;
        for i in 0..LOAD_THRESHOLD {
            let k = format!("key_{:04}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            match seg.insert(h2_val, k, i as u32, ba, bb) {
                InsertResult::Inserted => inserted += 1,
                InsertResult::Replaced(_) => {}
                InsertResult::NeedsSplit(_, _) => break,
            }
        }
        assert!(inserted > 0);

        // Verify all inserted entries are retrievable
        for i in 0..inserted {
            let k = format!("key_{:04}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            let val = seg.get(h2_val, k.as_slice(), ba, bb);
            assert_eq!(val, Some(&(i as u32)), "Missing key_{:04}", i);
        }

        // Check if any stash slots were used
        let mut stash_used = 0;
        for slot in REGULAR_SLOTS..TOTAL_SLOTS {
            if Segment::<Vec<u8>, u32>::is_full_ctrl(seg.ctrl_byte(slot)) {
                stash_used += 1;
            }
        }
        // With 51 entries in 56 regular slots + 4 stash, stash may or may not be used
        // depending on distribution. Just verify the count is correct.
        assert_eq!(seg.count() as usize, inserted);
        let _ = stash_used; // may be 0 if distribution is lucky
    }

    #[test]
    fn test_segment_split() {
        let mut seg: Segment<Vec<u8>, u32> = Segment::new(0);

        // Insert entries up to threshold
        let mut count = 0;
        for i in 0..LOAD_THRESHOLD {
            let k = format!("split_{:04}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            match seg.insert(h2_val, k, i as u32, ba, bb) {
                InsertResult::Inserted => count += 1,
                InsertResult::Replaced(_) => {}
                InsertResult::NeedsSplit(_, _) => break,
            }
        }
        assert!(count > 0);
        let pre_split_count = seg.count();

        // Split
        let new_seg = seg.split(&|k| simple_hash(k));

        // Both segments should have new depth
        assert_eq!(seg.depth(), 1);
        assert_eq!(new_seg.depth(), 1);

        // Total entries should be preserved
        assert_eq!(
            seg.count() + new_seg.count(),
            pre_split_count,
            "Split lost entries: {} + {} != {}",
            seg.count(),
            new_seg.count(),
            pre_split_count
        );

        // Both segments should have fewer entries than before
        assert!(seg.count() < pre_split_count || new_seg.count() < pre_split_count);

        // All entries should be retrievable from the correct segment
        for i in 0..count {
            let k = format!("split_{:04}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);

            let in_old = seg.get(h2_val, k.as_slice(), ba, bb);
            let in_new = new_seg.get(h2_val, k.as_slice(), ba, bb);

            // Should be in exactly one segment
            assert!(
                in_old.is_some() || in_new.is_some(),
                "Entry split_{:04} not found in either segment",
                i
            );
            if let Some(v) = in_old {
                assert_eq!(*v, i as u32);
            }
            if let Some(v) = in_new {
                assert_eq!(*v, i as u32);
            }
        }
    }

    #[test]
    fn test_segment_drop_safety() {
        // Insert entries with heap-allocating types to verify Drop doesn't leak.
        {
            let mut seg: Segment<String, String> = Segment::new(0);
            for i in 0..20 {
                let k = format!("drop_test_key_{}", i);
                let v = format!("drop_test_value_{}", i);
                let hash = simple_hash(k.as_bytes());
                let h2_val = h2(hash);
                let (ba, bb) = home_buckets(hash);
                seg.insert(h2_val, k, v, ba, bb);
            }
            // seg drops here -- Drop impl must free all 20 String keys + values
        }
        // If we get here without a double-free or leak, the test passes.
        // Run with `cargo +nightly miri test` for definitive leak detection.
    }

    #[test]
    fn test_segment_get_key_value() {
        let mut seg: Segment<Vec<u8>, String> = Segment::new(0);
        let k = b"kvtest".to_vec();
        let hash = simple_hash(&k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        seg.insert(h2_val, k.clone(), "hello".to_string(), ba, bb);

        let result = seg.get_key_value(h2_val, k.as_slice(), ba, bb);
        assert!(result.is_some());
        let (rk, rv) = result.unwrap();
        assert_eq!(rk, &k);
        assert_eq!(rv, "hello");
    }

    #[test]
    fn test_segment_get_mut() {
        let mut seg: Segment<Vec<u8>, String> = Segment::new(0);
        let k = b"muttest".to_vec();
        let hash = simple_hash(&k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        seg.insert(h2_val, k.clone(), "original".to_string(), ba, bb);

        // Mutate via get_mut
        if let Some(v) = seg.get_mut(h2_val, k.as_slice(), ba, bb) {
            *v = "modified".to_string();
        }

        assert_eq!(
            seg.get(h2_val, k.as_slice(), ba, bb),
            Some(&"modified".to_string())
        );
    }

    // Simple hash function for tests (xxh64 is in the dashtable mod, not available here directly)
    pub(super) fn simple_hash(data: &[u8]) -> u64 {
        // FNV-1a for tests
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in data {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    fn insert_result_name<K, V>(_r: &InsertResult<K, V>) -> &'static str {
        match _r {
            InsertResult::Inserted => "Inserted",
            InsertResult::Replaced(_) => "Replaced",
            InsertResult::NeedsSplit(_, _) => "NeedsSplit",
        }
    }

    #[test]
    fn test_segment_insert_or_update_at_inserts_new_key() {
        let mut seg: Segment<Vec<u8>, String> = Segment::new(0);
        let k = b"new_key".to_vec();
        let hash = simple_hash(&k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);

        let mut update_called = false;
        let result = seg.insert_or_update_at(
            h2_val,
            k.as_slice(),
            ba,
            bb,
            |_v: &mut String| {
                update_called = true;
            },
            || (k.clone(), "new_value".to_string()),
        );
        assert!(matches!(result, SegmentInsertOrUpdate::Inserted { .. }));
        assert!(!update_called, "update closure must NOT run on miss");
        assert_eq!(seg.count(), 1);
        assert_eq!(
            seg.get(h2_val, k.as_slice(), ba, bb),
            Some(&"new_value".to_string())
        );
    }

    #[test]
    fn test_segment_insert_or_update_at_updates_existing_key() {
        let mut seg: Segment<Vec<u8>, String> = Segment::new(0);
        let k = b"existing".to_vec();
        let hash = simple_hash(&k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        seg.insert(h2_val, k.clone(), "old".to_string(), ba, bb);

        let mut make_called = false;
        let result = seg.insert_or_update_at(
            h2_val,
            k.as_slice(),
            ba,
            bb,
            |v: &mut String| *v = format!("{}_updated", v),
            || {
                make_called = true;
                (k.clone(), "should_not_be_used".to_string())
            },
        );
        assert!(matches!(result, SegmentInsertOrUpdate::Updated { .. }));
        assert!(!make_called, "make closure must NOT run on hit");
        assert_eq!(seg.count(), 1, "Updated must NOT grow count");
        assert_eq!(
            seg.get(h2_val, k.as_slice(), ba, bb),
            Some(&"old_updated".to_string())
        );
    }

    #[test]
    fn test_segment_insert_or_update_at_returns_needs_split_on_full() {
        let mut seg: Segment<Vec<u8>, u32> = Segment::new(0);
        let mut inserted = 0u32;
        for i in 0..LOAD_THRESHOLD as u32 {
            let k = format!("k_{:04}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            match seg.insert(h2_val, k, i, ba, bb) {
                InsertResult::Inserted => inserted += 1,
                _ => break,
            }
        }
        assert!(inserted >= 1);

        // New key on a full segment must return NeedsSplit, NOT Inserted.
        let new_k = b"trigger_split".to_vec();
        let hash = simple_hash(&new_k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        let result = seg.insert_or_update_at(
            h2_val,
            new_k.as_slice(),
            ba,
            bb,
            |_| panic!("update on miss-into-full"),
            || (new_k.clone(), 999u32),
        );
        assert!(
            matches!(result, SegmentInsertOrUpdate::NeedsSplit { .. }),
            "Expected NeedsSplit on full segment with new key"
        );
    }

    #[test]
    fn test_segment_insert_or_update_at_updates_existing_on_full() {
        // Even when the segment is at LOAD_THRESHOLD, updating an existing
        // key must return Updated (NOT NeedsSplit).
        let mut seg: Segment<Vec<u8>, u32> = Segment::new(0);
        let target_key = b"update_me".to_vec();
        let target_hash = simple_hash(&target_key);
        let target_h2 = h2(target_hash);
        let (target_ba, target_bb) = home_buckets(target_hash);
        seg.insert(target_h2, target_key.clone(), 42, target_ba, target_bb);

        // Fill the rest up to LOAD_THRESHOLD
        let mut i = 0u32;
        while (seg.count() as usize) < LOAD_THRESHOLD {
            let k = format!("fill_{:06}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            seg.insert(h2_val, k, i, ba, bb);
            i += 1;
        }
        assert!(seg.is_full());

        // Update the existing key — must work even though segment is full
        let result = seg.insert_or_update_at(
            target_h2,
            target_key.as_slice(),
            target_ba,
            target_bb,
            |v| *v = 99,
            || panic!("make should not be called on update"),
        );
        assert!(matches!(result, SegmentInsertOrUpdate::Updated { .. }));
        assert_eq!(
            seg.get(target_h2, target_key.as_slice(), target_ba, target_bb),
            Some(&99)
        );
    }

    #[test]
    fn test_segment_insert_or_update_at_probes_at_most_two_groups_on_miss() {
        // On an empty segment, insert_or_update_at should scan at most 2 groups
        // for h2 + 2 for empty = 4 SIMD probes. With fallback groups it can go
        // up to 6. Assert ≤ 6.
        let mut seg: Segment<Vec<u8>, u32> = Segment::new(0);
        let probes_before = seg.probe_count();
        let k = b"probe_test".to_vec();
        let hash = simple_hash(&k);
        let h2_val = h2(hash);
        let (ba, bb) = home_buckets(hash);
        let _ = seg.insert_or_update_at(h2_val, k.as_slice(), ba, bb, |_| {}, || (k.clone(), 1u32));
        let probes_after = seg.probe_count();
        let delta = probes_after - probes_before;
        assert!(
            delta <= 6,
            "insert_or_update_at on empty segment did {} SIMD probes; expected <= 6",
            delta
        );
    }

    #[test]
    fn test_fallback_placement_ratio() {
        // PERF-09 attribution: measure how many keys land in non-home groups
        // (which require the expensive fallback scan in find).
        // We fill a single segment to near LOAD_THRESHOLD and check each key.
        let mut seg: Segment<Vec<u8>, u32> = Segment::new(0);
        let mut keys_and_hashes: Vec<(Vec<u8>, u64)> = Vec::new();

        for i in 0..LOAD_THRESHOLD {
            let k = format!("fb_{:06}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            match seg.insert(h2_val, k.clone(), i as u32, ba, bb) {
                InsertResult::Inserted => {
                    keys_and_hashes.push((k, hash));
                }
                _ => break,
            }
        }

        let total = keys_and_hashes.len();
        let mut in_fallback = 0usize;
        for (k, hash) in &keys_and_hashes {
            let h2_val = h2(*hash);
            let (ba, bb) = home_buckets(*hash);
            if let Some(true) = seg.is_in_non_home_group(h2_val, k.as_slice(), ba, bb) {
                in_fallback += 1;
            }
        }

        let ratio = if total > 0 {
            in_fallback as f64 / total as f64
        } else {
            0.0
        };
        eprintln!(
            "[PERF-09 attribution] total={}, in_fallback={}, ratio={:.4} ({:.2}%)",
            total,
            in_fallback,
            ratio,
            ratio * 100.0
        );
        // This test is observational — it measures but does not assert a threshold.
        // The ratio drives the fix selection in 189-03-INVESTIGATION.md.
    }

    #[test]
    fn test_has_non_home_keys_starts_false() {
        let seg: Segment<Vec<u8>, u32> = Segment::new(0);
        assert!(
            !seg.has_non_home_keys(),
            "new segment must not have non-home keys"
        );
    }

    #[test]
    fn test_has_non_home_keys_stays_false_under_normal_insert() {
        // Normal inserts at low load should never trigger the "any free slot" fallback.
        let mut seg: Segment<Vec<u8>, u32> = Segment::new(0);
        for i in 0..30u32 {
            let k = format!("nhk_{:04}", i).into_bytes();
            let hash = simple_hash(&k);
            let h2_val = h2(hash);
            let (ba, bb) = home_buckets(hash);
            seg.insert(h2_val, k, i, ba, bb);
        }
        assert!(
            !seg.has_non_home_keys(),
            "30 inserts into 60-slot segment should not trigger non-home placement"
        );
    }
}
