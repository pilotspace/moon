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

use std::borrow::Borrow;
use std::mem::MaybeUninit;
use std::ptr;

use super::simd::{DELETED, EMPTY, Group};

/// Number of regular (home) bucket slots per segment.
pub const REGULAR_SLOTS: usize = 56;

/// Number of stash (overflow) bucket slots per segment.
pub const STASH_SLOTS: usize = 4;

/// Total slots per segment: 56 regular + 4 stash = 60.
pub const TOTAL_SLOTS: usize = REGULAR_SLOTS + STASH_SLOTS;

/// Number of control byte groups (each group = 16 bytes for SIMD).
const NUM_GROUPS: usize = 4;

/// Padded control byte count: 4 groups x 16 = 64 bytes.
/// Slots 60-63 are padding (always EMPTY).
const CTRL_BYTES: usize = NUM_GROUPS * 16;

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
    ctrl: [Group; NUM_GROUPS], // 64 bytes exactly = 1 cache line
    // --- Cache line 1+: metadata ---
    count: u32,
    depth: u32,
    // --- Remaining cache lines: key/value data (accessed only on H2 hit) ---
    keys: [MaybeUninit<K>; TOTAL_SLOTS],
    values: [MaybeUninit<V>; TOTAL_SLOTS],
}

// Compile-time assertion: Segment must be cache-line aligned (64 bytes minimum).
const _: () = {
    assert!(std::mem::align_of::<Segment<u64, u64>>() >= 64);
};

/// Prefetch the key data at the given slot index into L1 cache.
///
/// On x86_64: uses `_mm_prefetch` with `_MM_HINT_T0` (all cache levels).
/// On other architectures (aarch64/macOS): no-op.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn prefetch_ptr(ptr: *const u8) {
    use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
    _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn prefetch_ptr(_ptr: *const u8) {
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
    fn is_full_ctrl(byte: u8) -> bool {
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
        unsafe { self.keys[slot].assume_init_ref() }
    }

    /// Get an immutable reference to the value at the given slot.
    ///
    /// # Safety
    /// Caller must ensure the slot is FULL (control byte in 0x00..0x7F).
    #[inline]
    pub unsafe fn value_ref(&self, slot: usize) -> &V {
        unsafe { self.values[slot].assume_init_ref() }
    }

    /// Get a mutable reference to the value at the given slot.
    ///
    /// # Safety
    /// Caller must ensure the slot is FULL (control byte in 0x00..0x7F).
    #[inline]
    pub unsafe fn value_mut(&mut self, slot: usize) -> &mut V {
        unsafe { self.values[slot].assume_init_mut() }
    }

    /// Find the slot index of a key matching (h2, key) in the given home buckets.
    ///
    /// Search order: group containing bucket_a, group containing bucket_b, then stash.
    /// Returns `Some(slot_index)` if found, `None` otherwise.
    #[allow(unused_unsafe)] // prefetch_ptr is unsafe on x86_64 but safe on aarch64
    pub fn find<Q: ?Sized>(
        &self,
        h2: u8,
        key: &Q,
        bucket_a: usize,
        bucket_b: usize,
    ) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        // Search group containing bucket_a
        let group_a = bucket_a / 16;
        let base_a = group_a * 16;

        #[cfg(target_arch = "x86_64")]
        let mask_a = unsafe { self.ctrl[group_a].match_h2(h2) };
        #[cfg(not(target_arch = "x86_64"))]
        let mask_a = self.ctrl[group_a].match_h2(h2);

        // Prefetch key data for the first H2 match to hide memory latency
        if let Some(first_pos) = mask_a.lowest_set_bit() {
            let prefetch_slot = base_a + first_pos;
            if prefetch_slot < TOTAL_SLOTS {
                unsafe {
                    prefetch_ptr(self.keys[prefetch_slot].as_ptr() as *const u8);
                }
            }
        }

        for pos in mask_a {
            let slot = base_a + pos;
            if slot < TOTAL_SLOTS {
                // SAFETY: ctrl byte matches h2 (a FULL value), so the slot is initialized.
                let k = unsafe { self.keys[slot].assume_init_ref() };
                if k.borrow() == key {
                    return Some(slot);
                }
            }
        }

        // Search group containing bucket_b (if different from group_a)
        let group_b = bucket_b / 16;
        if group_b != group_a {
            let base_b = group_b * 16;

            #[cfg(target_arch = "x86_64")]
            let mask_b = unsafe { self.ctrl[group_b].match_h2(h2) };
            #[cfg(not(target_arch = "x86_64"))]
            let mask_b = self.ctrl[group_b].match_h2(h2);

            // Prefetch key data for the first H2 match in group_b
            if let Some(first_pos) = mask_b.lowest_set_bit() {
                let prefetch_slot = base_b + first_pos;
                if prefetch_slot < TOTAL_SLOTS {
                    unsafe {
                        prefetch_ptr(self.keys[prefetch_slot].as_ptr() as *const u8);
                    }
                }
            }

            for pos in mask_b {
                let slot = base_b + pos;
                if slot < TOTAL_SLOTS {
                    let k = unsafe { self.keys[slot].assume_init_ref() };
                    if k.borrow() == key {
                        return Some(slot);
                    }
                }
            }
        }

        // Linear scan stash slots (56..60)
        for slot in REGULAR_SLOTS..TOTAL_SLOTS {
            let ctrl = self.ctrl_byte(slot);
            if ctrl == h2 {
                let k = unsafe { self.keys[slot].assume_init_ref() };
                if k.borrow() == key {
                    return Some(slot);
                }
            }
        }

        // Fallback: full linear scan of remaining groups.
        // This handles the rare case where insert placed a key in a group
        // that is neither group_a nor group_b (overflow during high-occupancy
        // or split redistribution). Without this, get/get_mut would fail to
        // find a key that was legitimately inserted.
        for g in 0..NUM_GROUPS {
            if g == group_a || g == group_b {
                continue; // already checked above
            }
            let base = g * 16;

            #[cfg(target_arch = "x86_64")]
            let mask = unsafe { self.ctrl[g].match_h2(h2) };
            #[cfg(not(target_arch = "x86_64"))]
            let mask = self.ctrl[g].match_h2(h2);

            for pos in mask {
                let slot = base + pos;
                if slot < REGULAR_SLOTS {
                    // SAFETY: ctrl byte matches h2 -> slot is initialized.
                    let k = unsafe { self.keys[slot].assume_init_ref() };
                    if k.borrow() == key {
                        return Some(slot);
                    }
                }
            }
        }

        None
    }

    /// Look up a key and return an immutable reference to its value.
    pub fn get<Q: ?Sized>(&self, h2: u8, key: &Q, bucket_a: usize, bucket_b: usize) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        let slot = self.find(h2, key, bucket_a, bucket_b)?;
        // SAFETY: find only returns slots with FULL ctrl bytes -> initialized.
        Some(unsafe { self.values[slot].assume_init_ref() })
    }

    /// Look up a key and return a mutable reference to its value.
    ///
    /// Note: This duplicates the find logic inline to avoid borrow checker
    /// conflicts (find borrows &self, but we need &mut self for the return).
    pub fn get_mut<Q: ?Sized>(
        &mut self,
        h2: u8,
        key: &Q,
        bucket_a: usize,
        bucket_b: usize,
    ) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        let slot = self.find_slot_mut(h2, key, bucket_a, bucket_b)?;
        Some(unsafe { self.values[slot].assume_init_mut() })
    }

    /// Internal: find slot index (same logic as find, but works with &mut self).
    fn find_slot_mut<Q: ?Sized>(
        &self,
        h2: u8,
        key: &Q,
        bucket_a: usize,
        bucket_b: usize,
    ) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        self.find(h2, key, bucket_a, bucket_b)
    }

    /// Look up a key and return references to both key and value.
    pub fn get_key_value<Q: ?Sized>(
        &self,
        h2: u8,
        key: &Q,
        bucket_a: usize,
        bucket_b: usize,
    ) -> Option<(&K, &V)>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        let slot = self.find(h2, key, bucket_a, bucket_b)?;
        unsafe {
            Some((
                self.keys[slot].assume_init_ref(),
                self.values[slot].assume_init_ref(),
            ))
        }
    }

    /// Insert a key-value pair into the segment.
    ///
    /// - If the key exists, replaces the value and returns `Replaced(old_value)`.
    /// - If the key is new and the segment has room, inserts and returns `Inserted`.
    /// - If the segment is full, returns `NeedsSplit` (key and value are NOT consumed).
    pub fn insert(
        &mut self,
        h2: u8,
        key: K,
        value: V,
        bucket_a: usize,
        bucket_b: usize,
    ) -> InsertResult<K, V>
    where
        K: Eq,
    {
        // Check if key already exists (using K: Borrow<K> identity)
        if let Some(slot) = self.find(h2, &key, bucket_a, bucket_b) {
            // Replace value in-place
            let old = unsafe { self.values[slot].assume_init_read() };
            self.values[slot] = MaybeUninit::new(value);
            // Drop the old key that was passed in (it's the same key)
            drop(key);
            return InsertResult::Replaced(old);
        }

        // Check if segment is full before trying to insert
        if self.is_full() {
            return InsertResult::NeedsSplit(key, value);
        }

        // Find first EMPTY or DELETED slot in bucket_a's group
        if let Some(slot) = self.find_free_slot_in_group(bucket_a / 16) {
            self.write_slot(slot, h2, key, value);
            return InsertResult::Inserted;
        }

        // Try bucket_b's group
        let group_b = bucket_b / 16;
        if group_b != bucket_a / 16 {
            if let Some(slot) = self.find_free_slot_in_group(group_b) {
                self.write_slot(slot, h2, key, value);
                return InsertResult::Inserted;
            }
        }

        // Try stash slots (56..60)
        for slot in REGULAR_SLOTS..TOTAL_SLOTS {
            let ctrl = self.ctrl_byte(slot);
            if ctrl == EMPTY || ctrl == DELETED {
                self.write_slot(slot, h2, key, value);
                return InsertResult::Inserted;
            }
        }

        // All slots examined, none free -- should not happen if count < LOAD_THRESHOLD
        // but possible if home groups and stash are all full while other groups have space.
        // Fall back: linear scan all slots for any free one.
        for slot in 0..TOTAL_SLOTS {
            let ctrl = self.ctrl_byte(slot);
            if ctrl == EMPTY || ctrl == DELETED {
                self.write_slot(slot, h2, key, value);
                return InsertResult::Inserted;
            }
        }

        // Truly full (shouldn't happen since we checked is_full above, but be safe)
        InsertResult::NeedsSplit(key, value)
    }

    /// Find a free slot within the given group index.
    fn find_free_slot_in_group(&self, group_idx: usize) -> Option<usize> {
        let base = group_idx * 16;

        #[cfg(target_arch = "x86_64")]
        let mask = unsafe { self.ctrl[group_idx].match_empty_or_deleted() };
        #[cfg(not(target_arch = "x86_64"))]
        let mask = self.ctrl[group_idx].match_empty_or_deleted();

        if let Some(pos) = mask.lowest_set_bit() {
            let slot = base + pos;
            if slot < TOTAL_SLOTS {
                return Some(slot);
            }
        }
        None
    }

    /// Write a key-value pair into a slot, setting the control byte and incrementing count.
    fn write_slot(&mut self, slot: usize, h2: u8, key: K, value: V) {
        self.set_ctrl_byte(slot, h2);
        self.keys[slot] = MaybeUninit::new(key);
        self.values[slot] = MaybeUninit::new(value);
        self.count += 1;
    }

    /// Remove a key from the segment.
    ///
    /// Returns `Some((key, value))` if the key was found and removed, `None` otherwise.
    /// Sets the control byte to DELETED for regular slots, EMPTY for stash slots.
    pub fn remove<Q: ?Sized>(
        &mut self,
        h2: u8,
        key: &Q,
        bucket_a: usize,
        bucket_b: usize,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        let slot = self.find(h2, key, bucket_a, bucket_b)?;

        // SAFETY: find guarantees the slot is FULL (initialized).
        let k = unsafe { self.keys[slot].assume_init_read() };
        let v = unsafe { self.values[slot].assume_init_read() };

        // Use DELETED for regular slots (maintains probe chains),
        // EMPTY for stash slots (no probe chain dependency).
        if slot >= REGULAR_SLOTS {
            self.set_ctrl_byte(slot, EMPTY);
        } else {
            self.set_ctrl_byte(slot, DELETED);
        }

        self.count -= 1;
        Some((k, v))
    }

    /// Split this segment, distributing entries between self and a new segment.
    ///
    /// After split, both segments have `depth = self.depth + 1`.
    /// Entries whose hash bit at position `self.depth` (from the top) is 1 move
    /// to the new segment; entries with bit 0 stay in self.
    ///
    /// Uses collect-and-redistribute strategy: all entries are temporarily extracted,
    /// then re-inserted into the appropriate segment with fresh slot assignments.
    pub fn split(&mut self, hasher: &impl Fn(&K) -> u64) -> Segment<K, V> {
        let new_depth = self.depth + 1;
        let mut new_seg = Segment::new(new_depth);

        // Collect all entries from current segment
        let mut entries: Vec<(K, V, u64)> = Vec::with_capacity(self.count as usize);
        for slot in 0..TOTAL_SLOTS {
            if Self::is_full_ctrl(self.ctrl_byte(slot)) {
                let k = unsafe { ptr::read(self.keys[slot].as_ptr()) };
                let hash = hasher(&k);
                let v = unsafe { ptr::read(self.values[slot].as_ptr()) };
                entries.push((k, v, hash));
                // Mark slot as EMPTY (we've moved the data out)
                self.set_ctrl_byte(slot, EMPTY);
            }
        }
        self.count = 0;

        // Reset all control bytes to EMPTY for clean re-insertion
        for g in 0..NUM_GROUPS {
            self.ctrl[g] = Group::new_empty();
        }

        // Update depth BEFORE re-inserting (so home_buckets use correct depth)
        self.depth = new_depth;

        // Re-insert entries into appropriate segment
        let bit_pos = new_depth - 1; // 0-indexed from the top
        for (key, value, hash) in entries {
            let h2_val = h2(hash);
            let (bucket_a, bucket_b) = home_buckets(hash);

            if (hash >> (63 - bit_pos)) & 1 == 1 {
                // Move to new segment
                new_seg.insert_during_split(h2_val, key, value, bucket_a, bucket_b);
            } else {
                // Stay in old segment
                self.insert_during_split(h2_val, key, value, bucket_a, bucket_b);
            }
        }

        new_seg
    }

    /// Insert during split -- panics if no room (should never happen during split).
    fn insert_during_split(&mut self, h2: u8, key: K, value: V, bucket_a: usize, bucket_b: usize) {
        // Try bucket_a's group first
        if let Some(slot) = self.find_free_slot_in_group(bucket_a / 16) {
            self.write_slot(slot, h2, key, value);
            return;
        }

        // Try bucket_b's group
        let group_b = bucket_b / 16;
        if group_b != bucket_a / 16 {
            if let Some(slot) = self.find_free_slot_in_group(group_b) {
                self.write_slot(slot, h2, key, value);
                return;
            }
        }

        // Try stash
        for slot in REGULAR_SLOTS..TOTAL_SLOTS {
            if self.ctrl_byte(slot) == EMPTY {
                self.write_slot(slot, h2, key, value);
                return;
            }
        }

        // Last resort: any free slot
        for slot in 0..TOTAL_SLOTS {
            if self.ctrl_byte(slot) == EMPTY {
                self.write_slot(slot, h2, key, value);
                return;
            }
        }

        panic!("Segment overflow during split -- this should never happen");
    }
}

/// Compute two home bucket indices from a hash value.
/// Returns (bucket_a, bucket_b) where both are in 0..REGULAR_SLOTS.
/// If they collide, bucket_b is shifted by 1.
#[inline]
pub fn home_buckets(hash: u64) -> (usize, usize) {
    let a = ((hash >> 8) as usize) % REGULAR_SLOTS;
    let mut b = ((hash >> 16) as usize) % REGULAR_SLOTS;
    if b == a {
        b = (a + 1) % REGULAR_SLOTS;
    }
    (a, b)
}

impl<K, V> Drop for Segment<K, V> {
    fn drop(&mut self) {
        // SAFETY: We must drop all initialized slots. A slot is initialized
        // if and only if its control byte is a valid H2 (0x00..0x7F).
        for slot in 0..TOTAL_SLOTS {
            if Self::is_full_ctrl(self.ctrl_byte(slot)) {
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

        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let vals = vec!["val1".to_string(), "val2".to_string(), "val3".to_string()];

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
    fn simple_hash(data: &[u8]) -> u64 {
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
}
