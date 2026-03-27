//! SIMD group operations for Swiss Table control byte probing.
//!
//! A `Group` holds 16 control bytes. `match_h2` finds slots matching an H2
//! fingerprint via SIMD comparison (SSE2 on x86_64) or scalar fallback.
//!
//! Control byte encoding:
//! - `EMPTY`   = 0xFF  -- slot is unused
//! - `DELETED` = 0x80  -- tombstone
//! - `FULL`    = 0x00..0x7F -- H2 fingerprint (top 7 bits of hash)

/// Marker for an empty (unused) slot.
pub const EMPTY: u8 = 0xFF;

/// Marker for a deleted (tombstone) slot.
pub const DELETED: u8 = 0x80;

/// A group of 16 control bytes for Swiss Table SIMD probing.
///
/// Aligned to 16 bytes for efficient SIMD loads.
#[repr(C, align(16))]
#[derive(Clone)]
pub struct Group(pub [u8; 16]);

impl Group {
    /// Create a new group with all slots set to EMPTY.
    #[inline]
    pub fn new_empty() -> Self {
        Group([EMPTY; 16])
    }

    /// Find slots matching the given H2 fingerprint.
    ///
    /// Returns a `BitMask` where bit `i` is set if `ctrl[i] == h2`.
    ///
    /// # Safety
    /// Requires x86_64 with SSE2 support (baseline on all x86_64 CPUs).
    ///
    /// # SSE2 path (x86_64)
    /// Uses `_mm_load_si128` + `_mm_cmpeq_epi8` + `_mm_movemask_epi8` for
    /// 16-way parallel comparison in a single instruction sequence.
    #[cfg(target_arch = "x86_64")]
    #[inline]
    #[target_feature(enable = "sse2")]
    pub unsafe fn match_h2(&self, h2: u8) -> BitMask {
        use core::arch::x86_64::*;
        let ctrl = _mm_load_si128(self.0.as_ptr() as *const __m128i);
        let needle = _mm_set1_epi8(h2 as i8);
        let cmp = _mm_cmpeq_epi8(ctrl, needle);
        BitMask(_mm_movemask_epi8(cmp) as u16)
    }

    /// Scalar fallback for non-x86_64 platforms (aarch64, etc.).
    #[cfg(not(target_arch = "x86_64"))]
    #[inline]
    pub fn match_h2(&self, h2: u8) -> BitMask {
        let mut mask = 0u16;
        for i in 0..16 {
            if self.0[i] == h2 {
                mask |= 1 << i;
            }
        }
        BitMask(mask)
    }

    /// Find empty slots (control byte == EMPTY).
    #[inline]
    pub fn match_empty(&self) -> BitMask {
        // SAFETY: SSE2 is baseline on x86_64.
        #[cfg(target_arch = "x86_64")]
        {
            unsafe { self.match_h2(EMPTY) }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            self.match_h2(EMPTY)
        }
    }

    /// Find empty or deleted slots (control byte has top bit set).
    ///
    /// Both EMPTY (0xFF) and DELETED (0x80) have bit 7 set.
    /// All FULL slots have H2 values in 0x00..0x7F (bit 7 clear).
    ///
    /// # Safety
    /// Requires x86_64 with SSE2 support (baseline on all x86_64 CPUs).
    #[cfg(target_arch = "x86_64")]
    #[inline]
    #[target_feature(enable = "sse2")]
    pub unsafe fn match_empty_or_deleted(&self) -> BitMask {
        use core::arch::x86_64::*;
        let ctrl = _mm_load_si128(self.0.as_ptr() as *const __m128i);
        // movemask extracts the top bit of each byte -- exactly what we need.
        BitMask(_mm_movemask_epi8(ctrl) as u16)
    }

    /// Scalar fallback: find empty or deleted slots.
    #[cfg(not(target_arch = "x86_64"))]
    #[inline]
    pub fn match_empty_or_deleted(&self) -> BitMask {
        let mut mask = 0u16;
        for i in 0..16 {
            if self.0[i] & 0x80 != 0 {
                mask |= 1 << i;
            }
        }
        BitMask(mask)
    }
}

/// Bitmask of matching positions within a `Group`.
///
/// Each set bit `i` means position `i` in the group matched.
/// Implements `Iterator` to yield each set bit position.
#[derive(Clone, Copy, Debug)]
pub struct BitMask(pub u16);

impl BitMask {
    /// Returns true if any bit is set.
    #[inline]
    pub fn any_set(&self) -> bool {
        self.0 != 0
    }

    /// Returns the index of the lowest set bit, or `None` if empty.
    #[inline]
    pub fn lowest_set_bit(&self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some(self.0.trailing_zeros() as usize)
        }
    }
}

impl Iterator for BitMask {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        let pos = self.0.trailing_zeros() as usize;
        self.0 &= self.0 - 1; // clear lowest set bit
        Some(pos)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let count = self.0.count_ones() as usize;
        (count, Some(count))
    }
}

impl ExactSizeIterator for BitMask {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_match_h2() {
        let mut g = Group::new_empty();
        g.0[0] = 0x42;
        g.0[5] = 0x42;
        g.0[15] = 0x42;

        #[cfg(target_arch = "x86_64")]
        let mask = unsafe { g.match_h2(0x42) };
        #[cfg(not(target_arch = "x86_64"))]
        let mask = g.match_h2(0x42);

        let positions: Vec<usize> = BitMask(mask.0).collect();
        assert_eq!(positions, vec![0, 5, 15]);
    }

    #[test]
    fn test_group_match_h2_no_match() {
        let g = Group::new_empty();

        #[cfg(target_arch = "x86_64")]
        let mask = unsafe { g.match_h2(0x42) };
        #[cfg(not(target_arch = "x86_64"))]
        let mask = g.match_h2(0x42);

        assert!(!mask.any_set());
    }

    #[test]
    fn test_group_match_empty() {
        let mut g = Group::new_empty();
        // Set some slots to non-empty
        g.0[3] = 0x10;
        g.0[7] = 0x20;

        let mask = g.match_empty();
        // 16 slots, 2 filled -> 14 empty
        assert_eq!(mask.0.count_ones(), 14);
        // Filled slots should NOT be in the mask
        let positions: Vec<usize> = BitMask(mask.0).collect();
        assert!(!positions.contains(&3));
        assert!(!positions.contains(&7));
    }

    #[test]
    fn test_group_match_empty_all_empty() {
        let g = Group::new_empty();
        let mask = g.match_empty();
        assert_eq!(mask.0, 0xFFFF);
    }

    #[test]
    fn test_group_match_empty_or_deleted() {
        let mut g = Group::new_empty();
        g.0[0] = 0x10; // FULL
        g.0[1] = DELETED; // DELETED (0x80)
        g.0[2] = 0x7F; // FULL (max H2)
        // Rest are EMPTY (0xFF)

        #[cfg(target_arch = "x86_64")]
        let mask = unsafe { g.match_empty_or_deleted() };
        #[cfg(not(target_arch = "x86_64"))]
        let mask = g.match_empty_or_deleted();

        // Slot 0 (0x10) and slot 2 (0x7F) are FULL -- bit 7 clear
        // Slot 1 (DELETED=0x80) and slots 3-15 (EMPTY=0xFF) have bit 7 set
        assert_eq!(mask.0.count_ones(), 14); // 16 - 2 FULL = 14
        let positions: Vec<usize> = BitMask(mask.0).collect();
        assert!(!positions.contains(&0));
        assert!(positions.contains(&1));
        assert!(!positions.contains(&2));
    }

    #[test]
    fn test_bitmask_iterator() {
        let mask = BitMask(0b1010_0101_0000_1001);
        let positions: Vec<usize> = mask.collect();
        assert_eq!(positions, vec![0, 3, 8, 10, 13, 15]);
    }

    #[test]
    fn test_bitmask_empty() {
        let mask = BitMask(0);
        assert!(!mask.any_set());
        assert_eq!(mask.lowest_set_bit(), None);
        assert_eq!(mask.count(), 0);
    }

    #[test]
    fn test_bitmask_lowest_set_bit() {
        assert_eq!(BitMask(0b1000).lowest_set_bit(), Some(3));
        assert_eq!(BitMask(0b0001).lowest_set_bit(), Some(0));
        assert_eq!(BitMask(0b0110).lowest_set_bit(), Some(1));
    }

    #[test]
    fn test_bitmask_size_hint() {
        let mask = BitMask(0b1010_1010);
        assert_eq!(mask.len(), 4);
    }
}
