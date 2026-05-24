//! Find / lookup operations on `Segment`.

use std::borrow::Borrow;

use super::{NUM_GROUPS, REGULAR_SLOTS, Segment, TOTAL_SLOTS, prefetch_ptr};

impl<K, V> Segment<K, V> {
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

        // SAFETY: group_a < NUM_GROUPS (bucket_a < REGULAR_SLOTS, 16 per group).
        // SSE2 is baseline on x86_64; Group is 16-byte aligned and initialized.
        #[cfg(target_arch = "x86_64")]
        let mask_a = unsafe { self.ctrl[group_a].match_h2(h2) };
        #[cfg(not(target_arch = "x86_64"))]
        let mask_a = self.ctrl[group_a].match_h2(h2);

        // Prefetch key data for the first H2 match to hide memory latency
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

            // SAFETY: group_b < NUM_GROUPS (bucket_b < REGULAR_SLOTS).
            // SSE2 is baseline on x86_64; Group is 16-byte aligned and initialized.
            #[cfg(target_arch = "x86_64")]
            let mask_b = unsafe { self.ctrl[group_b].match_h2(h2) };
            #[cfg(not(target_arch = "x86_64"))]
            let mask_b = self.ctrl[group_b].match_h2(h2);

            // Prefetch key data for the first H2 match in group_b
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
                    // SAFETY: ctrl byte matches h2 (a FULL value), so the slot is initialized.
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
                // SAFETY: ctrl byte matches h2 (a FULL value), so the slot is initialized.
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
        //
        // PERF-09: Skip the fallback when has_non_home_keys is false — no key
        // was ever placed in a non-home group, so the scan is guaranteed to
        // find nothing. This eliminates 2 wasted SIMD probes on every miss.
        if !self.has_non_home_keys {
            return None;
        }
        for g in 0..NUM_GROUPS {
            if g == group_a || g == group_b {
                continue; // already checked above
            }
            let base = g * 16;

            // `g` is bounded by NUM_GROUPS, so `self.ctrl[g]` is a valid Group.
            // SAFETY: Group is 16-byte aligned and initialized; SSE2 is baseline on x86_64.
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
        // SAFETY: find_slot_mut only returns slots with FULL ctrl bytes, so the value is initialized.
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
        // SAFETY: find only returns slots with FULL ctrl bytes, so key and value are initialized.
        unsafe {
            Some((
                self.keys[slot].assume_init_ref(),
                self.values[slot].assume_init_ref(),
            ))
        }
    }

    /// Check if a key was placed in a non-home group (test-only perf attribution).
    ///
    /// Returns `Some(true)` if the key is found in a group that is NOT group_a,
    /// group_b, or the stash — meaning `find` would need the fallback scan to
    /// locate it. Returns `Some(false)` if found in a home group or stash,
    /// `None` if the key is not found at all.
    #[cfg(test)]
    pub fn is_in_non_home_group<Q: ?Sized>(
        &self,
        h2: u8,
        key: &Q,
        bucket_a: usize,
        bucket_b: usize,
    ) -> Option<bool>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        let group_a = bucket_a / 16;
        let group_b = bucket_b / 16;

        // Check group_a
        let base_a = group_a * 16;
        for slot in base_a..(base_a + 16).min(TOTAL_SLOTS) {
            if self.ctrl_byte(slot) == h2 {
                // SAFETY: ctrl byte matches h2 (FULL), so key is initialized.
                let k = unsafe { self.keys[slot].assume_init_ref() };
                if k.borrow() == key {
                    return Some(false); // found in home group
                }
            }
        }

        // Check group_b
        if group_b != group_a {
            let base_b = group_b * 16;
            for slot in base_b..(base_b + 16).min(TOTAL_SLOTS) {
                if self.ctrl_byte(slot) == h2 {
                    // SAFETY: ctrl byte matches h2 (a FULL value), so the slot is initialized.
                    let k = unsafe { self.keys[slot].assume_init_ref() };
                    if k.borrow() == key {
                        return Some(false); // found in home group
                    }
                }
            }
        }

        // Check stash
        for slot in REGULAR_SLOTS..TOTAL_SLOTS {
            if self.ctrl_byte(slot) == h2 {
                // SAFETY: ctrl byte matches h2 (a FULL value), so the slot is initialized.
                let k = unsafe { self.keys[slot].assume_init_ref() };
                if k.borrow() == key {
                    return Some(false); // found in stash (not fallback)
                }
            }
        }

        // Check remaining groups (fallback path)
        for g in 0..NUM_GROUPS {
            if g == group_a || g == group_b {
                continue;
            }
            let base = g * 16;
            for slot in base..(base + 16).min(REGULAR_SLOTS) {
                if self.ctrl_byte(slot) == h2 {
                    // SAFETY: ctrl byte matches h2 (a FULL value), so the slot is initialized.
                    let k = unsafe { self.keys[slot].assume_init_ref() };
                    if k.borrow() == key {
                        return Some(true); // found in NON-home group (fallback required)
                    }
                }
            }
        }

        None // not found
    }
}
