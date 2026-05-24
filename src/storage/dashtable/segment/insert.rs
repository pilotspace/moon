//! Insert / update operations on `Segment`.

use std::borrow::Borrow;
use std::mem::MaybeUninit;

use super::{
    DELETED, EMPTY, InsertResult, REGULAR_SLOTS, Segment, SegmentInsertOrUpdate, TOTAL_SLOTS,
    prefetch_ptr,
};

impl<K, V> Segment<K, V> {
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
            // SAFETY: find guarantees the slot is FULL (initialized), so reading the old value is valid.
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
        // Mark that a key was placed in a non-home group so find() knows to scan all groups.
        for slot in 0..TOTAL_SLOTS {
            let ctrl = self.ctrl_byte(slot);
            if ctrl == EMPTY || ctrl == DELETED {
                self.has_non_home_keys = true;
                self.write_slot(slot, h2, key, value);
                return InsertResult::Inserted;
            }
        }

        // Truly full (shouldn't happen since we checked is_full above, but be safe)
        InsertResult::NeedsSplit(key, value)
    }

    /// Find a free slot within the given group index.
    pub(super) fn find_free_slot_in_group(&self, group_idx: usize) -> Option<usize> {
        let base = group_idx * 16;

        // SAFETY: group_idx is bounded by NUM_GROUPS. SSE2 is baseline on x86_64.
        // The Group is 16-byte aligned and fully initialized at segment creation.
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
    pub(super) fn write_slot(&mut self, slot: usize, h2: u8, key: K, value: V) {
        self.set_ctrl_byte(slot, h2);
        self.keys[slot] = MaybeUninit::new(key);
        self.values[slot] = MaybeUninit::new(value);
        self.count += 1;
    }

    /// Return the cumulative SIMD probe count (test-only instrumentation).
    #[cfg(test)]
    pub fn probe_count(&self) -> u32 {
        self.probe_count
    }

    /// Increment the probe counter (test-only instrumentation).
    #[cfg(test)]
    #[inline]
    fn bump_probe_count(&mut self) {
        self.probe_count += 1;
    }

    /// No-op in non-test builds.
    #[cfg(not(test))]
    #[inline(always)]
    fn bump_probe_count(&mut self) {}

    /// Single-probe find OR insert. Fuses the `find` + `insert` paths into a
    /// single pass over the control byte groups, eliminating the redundant second
    /// probe that `get_mut` + `insert` would perform on a miss.
    ///
    /// On hit: invokes `update(&mut existing_value)` in place.
    /// On miss + room: calls `make()` to produce `(K, V)`, writes to a free slot.
    /// On miss + full: returns `NeedsSplit` with the unconsumed closures.
    ///
    /// Hot path: one `match_h2` + one `match_empty_or_deleted` per group scanned.
    #[allow(unused_unsafe)] // prefetch_ptr is unsafe on x86_64 but safe on aarch64
    pub fn insert_or_update_at<Q: ?Sized, F, G>(
        &mut self,
        h2: u8,
        key_lookup: &Q,
        bucket_a: usize,
        bucket_b: usize,
        update: F,
        make: G,
    ) -> SegmentInsertOrUpdate<F, G>
    where
        K: Borrow<Q> + Eq,
        Q: Eq,
        F: FnOnce(&mut V),
        G: FnOnce() -> (K, V),
    {
        // Track the first free slot found during our scan so we can reuse it on miss.
        let mut first_free: Option<usize> = None;

        // --- Group A: the home group for bucket_a ---
        let group_a = bucket_a / 16;
        let base_a = group_a * 16;

        // SAFETY: group_a < NUM_GROUPS (bucket_a < REGULAR_SLOTS, 16 per group).
        // SSE2 is baseline on x86_64; Group is 16-byte aligned and initialized.
        #[cfg(target_arch = "x86_64")]
        let mask_a = unsafe { self.ctrl[group_a].match_h2(h2) };
        #[cfg(not(target_arch = "x86_64"))]
        let mask_a = self.ctrl[group_a].match_h2(h2);
        self.bump_probe_count();

        // Prefetch key data for first h2 match
        if let Some(first_pos) = mask_a.lowest_set_bit() {
            let prefetch_slot = base_a + first_pos;
            if prefetch_slot < TOTAL_SLOTS {
                // SAFETY: prefetch_slot < TOTAL_SLOTS, so keys[prefetch_slot] is in bounds.
                // Prefetch is a hint — no memory safety requirement on the data being initialized.
                unsafe {
                    prefetch_ptr(self.keys[prefetch_slot].as_ptr() as *const u8);
                }
            }
        }

        for pos in mask_a {
            let slot = base_a + pos;
            if slot < TOTAL_SLOTS {
                // SAFETY: ctrl byte matches h2 (a FULL value), so the slot is initialized.
                // Mirrors find at segment/find.rs.
                let k = unsafe { self.keys[slot].assume_init_ref() };
                if k.borrow() == key_lookup {
                    // SAFETY: ctrl byte matches h2 and key compares equal (mirrors find),
                    // so values[slot] is initialized.
                    let v = unsafe { self.values[slot].assume_init_mut() };
                    update(v);
                    return SegmentInsertOrUpdate::Updated { slot };
                }
            }
        }

        // SAFETY: group_a < NUM_GROUPS. SSE2 is baseline on x86_64.
        // Group is 16-byte aligned and initialized at segment creation.
        #[cfg(target_arch = "x86_64")]
        let free_mask_a = unsafe { self.ctrl[group_a].match_empty_or_deleted() };
        #[cfg(not(target_arch = "x86_64"))]
        let free_mask_a = self.ctrl[group_a].match_empty_or_deleted();
        self.bump_probe_count();

        if let Some(pos) = free_mask_a.lowest_set_bit() {
            let slot = base_a + pos;
            if slot < TOTAL_SLOTS && first_free.is_none() {
                first_free = Some(slot);
            }
        }

        // --- Group B: the home group for bucket_b (if different) ---
        let group_b = bucket_b / 16;
        if group_b != group_a {
            let base_b = group_b * 16;

            // SAFETY: group_b < NUM_GROUPS (bucket_b < REGULAR_SLOTS).
            // SSE2 is baseline on x86_64; Group is 16-byte aligned and initialized.
            #[cfg(target_arch = "x86_64")]
            let mask_b = unsafe { self.ctrl[group_b].match_h2(h2) };
            #[cfg(not(target_arch = "x86_64"))]
            let mask_b = self.ctrl[group_b].match_h2(h2);
            self.bump_probe_count();

            if let Some(first_pos) = mask_b.lowest_set_bit() {
                let prefetch_slot = base_b + first_pos;
                if prefetch_slot < TOTAL_SLOTS {
                    // SAFETY: prefetch_slot < TOTAL_SLOTS, so keys[prefetch_slot] is in bounds.
                    // Prefetch is a hint — no memory safety requirement on the data being initialized.
                    unsafe {
                        prefetch_ptr(self.keys[prefetch_slot].as_ptr() as *const u8);
                    }
                }
            }

            for pos in mask_b {
                let slot = base_b + pos;
                if slot < TOTAL_SLOTS {
                    // SAFETY: ctrl byte matches h2 (mirrors find).
                    let k = unsafe { self.keys[slot].assume_init_ref() };
                    if k.borrow() == key_lookup {
                        // SAFETY: key match confirmed (mirrors find).
                        let v = unsafe { self.values[slot].assume_init_mut() };
                        update(v);
                        return SegmentInsertOrUpdate::Updated { slot };
                    }
                }
            }

            // SAFETY: group_b < NUM_GROUPS. SSE2 is baseline on x86_64.
            // Group is 16-byte aligned and initialized at segment creation.
            #[cfg(target_arch = "x86_64")]
            let free_mask_b = unsafe { self.ctrl[group_b].match_empty_or_deleted() };
            #[cfg(not(target_arch = "x86_64"))]
            let free_mask_b = self.ctrl[group_b].match_empty_or_deleted();
            self.bump_probe_count();

            if first_free.is_none() {
                if let Some(pos) = free_mask_b.lowest_set_bit() {
                    let slot = base_b + pos;
                    if slot < TOTAL_SLOTS {
                        first_free = Some(slot);
                    }
                }
            }
        }

        // --- Stash slots (56..60): linear scan for h2 matches ---
        for slot in REGULAR_SLOTS..TOTAL_SLOTS {
            let ctrl = self.ctrl_byte(slot);
            if ctrl == h2 {
                // SAFETY: ctrl byte matches h2 (mirrors find).
                let k = unsafe { self.keys[slot].assume_init_ref() };
                if k.borrow() == key_lookup {
                    // SAFETY: key match confirmed (mirrors find).
                    let v = unsafe { self.values[slot].assume_init_mut() };
                    update(v);
                    return SegmentInsertOrUpdate::Updated { slot };
                }
            } else if (ctrl == EMPTY || ctrl == DELETED) && first_free.is_none() {
                first_free = Some(slot);
            }
        }

        // --- Fallback: scan remaining groups for h2 matches (rare overflow path) ---
        // This handles keys placed in non-home groups during high occupancy or
        // split redistribution (mirrors find).
        //
        // PERF-09: Skip when has_non_home_keys is false — no key was placed in a
        // non-home group, so the scan cannot find a match. Still need to find a
        // free slot for insertion below, but the home groups already provided one.
        if self.has_non_home_keys {
            for g in 0..super::NUM_GROUPS {
                if g == group_a || g == group_b {
                    continue;
                }
                let base = g * 16;

                // SAFETY: g is bounded by NUM_GROUPS. SSE2 is baseline on x86_64.
                // Group is 16-byte aligned and initialized at segment creation.
                #[cfg(target_arch = "x86_64")]
                let mask = unsafe { self.ctrl[g].match_h2(h2) };
                #[cfg(not(target_arch = "x86_64"))]
                let mask = self.ctrl[g].match_h2(h2);
                self.bump_probe_count();

                for pos in mask {
                    let slot = base + pos;
                    if slot < REGULAR_SLOTS {
                        // SAFETY: ctrl byte matches h2 -> slot is initialized
                        // (mirrors find).
                        let k = unsafe { self.keys[slot].assume_init_ref() };
                        if k.borrow() == key_lookup {
                            // SAFETY: key match confirmed (mirrors find).
                            let v = unsafe { self.values[slot].assume_init_mut() };
                            update(v);
                            return SegmentInsertOrUpdate::Updated { slot };
                        }
                    }
                }

                // Also check for free slots in fallback groups
                if first_free.is_none() {
                    // SAFETY: g is bounded by NUM_GROUPS. SSE2 is baseline on x86_64.
                    // Group is 16-byte aligned and initialized at segment creation.
                    #[cfg(target_arch = "x86_64")]
                    let free_mask = unsafe { self.ctrl[g].match_empty_or_deleted() };
                    #[cfg(not(target_arch = "x86_64"))]
                    let free_mask = self.ctrl[g].match_empty_or_deleted();
                    self.bump_probe_count();

                    if let Some(pos) = free_mask.lowest_set_bit() {
                        let slot = base + pos;
                        if slot < TOTAL_SLOTS {
                            first_free = Some(slot);
                        }
                    }
                }
            }
        } // end PERF-09 has_non_home_keys guard

        // --- Key not found: decide insert vs NeedsSplit ---
        if self.is_full() {
            return SegmentInsertOrUpdate::NeedsSplit { update, make };
        }

        // We have room. Use the first free slot found, or do a linear scan.
        let free_slot = first_free.unwrap_or_else(|| {
            // Last resort: linear scan for any free slot (should rarely happen
            // since we scanned all groups above, but handles edge cases).
            for slot in 0..TOTAL_SLOTS {
                let ctrl = self.ctrl_byte(slot);
                if ctrl == EMPTY || ctrl == DELETED {
                    return slot;
                }
            }
            // Should never reach here since !is_full() guarantees a free slot.
            unreachable!("Segment not full but no free slot found")
        });

        let (k, v) = make();
        self.write_slot(free_slot, h2, k, v);
        SegmentInsertOrUpdate::Inserted { slot: free_slot }
    }
}
