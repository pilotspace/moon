//! Remove / split operations on `Segment`, plus the free function `home_buckets`.

use std::borrow::Borrow;
use std::ptr;

use super::{EMPTY, NUM_GROUPS, REGULAR_SLOTS, Segment, TOTAL_SLOTS, h2};
use crate::storage::dashtable::simd::Group;

impl<K, V> Segment<K, V> {
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
            self.set_ctrl_byte(slot, super::DELETED);
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
                // SAFETY: ctrl byte is FULL, so key and value are initialized. We immediately
                // mark the slot EMPTY after reading, preventing double-read or double-drop.
                let k = unsafe { ptr::read(self.keys[slot].as_ptr()) };
                let hash = hasher(&k);
                // SAFETY: Same as above — slot is FULL, so value is initialized.
                let v = unsafe { ptr::read(self.values[slot].as_ptr()) };
                entries.push((k, v, hash));
                // Mark slot as EMPTY (we've moved the data out)
                self.set_ctrl_byte(slot, EMPTY);
            }
        }
        self.count = 0;
        // Reset non-home-keys flag; insert_during_split will set it if needed.
        self.has_non_home_keys = false;

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

        // Last resort: any free slot (non-home placement)
        // Mark the flag so find() knows it must scan all groups.
        for slot in 0..TOTAL_SLOTS {
            if self.ctrl_byte(slot) == EMPTY {
                self.has_non_home_keys = true;
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
