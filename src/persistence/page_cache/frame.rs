//! Frame descriptor for the PageCache buffer manager.
//!
//! Each frame in the buffer pool has a `FrameDescriptor` that tracks:
//! - Atomic packed state (refcount, usage_count, flags) in a single `AtomicU32`
//! - Page identity (file_id, page_offset)
//! - Page LSN for WAL-before-data invariant enforcement
//!
//! Bit layout of the packed `AtomicU32` state:
//! ```text
//! Bits 31..16  refcount (u16, max 65535 concurrent pins)
//! Bits 15..8   usage_count (u8, for clock-sweep, capped at MAX_USAGE_COUNT)
//! Bits  7..0   flags (u8)
//! ```

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Maximum usage count for clock-sweep (higher = harder to evict).
pub const MAX_USAGE_COUNT: u8 = 3;

/// Frame is dirty — contains unflushed modifications.
pub const FLAG_DIRTY: u8 = 0x01;
/// Frame contains valid page data (has been read from disk or initialized).
pub const FLAG_VALID: u8 = 0x02;
/// An I/O operation is currently in progress on this frame.
pub const FLAG_IO_IN_PROGRESS: u8 = 0x04;

/// Packed atomic state for a single buffer frame.
///
/// All operations are lock-free using CAS loops on the underlying `AtomicU32`.
pub struct FrameState {
    state: AtomicU32,
}

impl FrameState {
    /// Create a new frame state, fully zeroed (refcount=0, usage=0, flags=0).
    #[inline]
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    /// Pack refcount, usage_count, and flags into a single u32.
    #[inline]
    pub fn pack(refcount: u16, usage: u8, flags: u8) -> u32 {
        ((refcount as u32) << 16) | ((usage as u32) << 8) | (flags as u32)
    }

    /// Unpack a u32 into (refcount, usage_count, flags).
    #[inline]
    pub fn unpack(val: u32) -> (u16, u8, u8) {
        let refcount = (val >> 16) as u16;
        let usage = ((val >> 8) & 0xFF) as u8;
        let flags = (val & 0xFF) as u8;
        (refcount, usage, flags)
    }

    /// Atomically increment the refcount. Returns the new refcount.
    ///
    /// Uses a CAS loop with Acquire load / Release store ordering.
    #[inline]
    pub fn pin(&self) -> u16 {
        loop {
            let old = self.state.load(Ordering::Acquire);
            let (rc, usage, flags) = Self::unpack(old);
            let new_rc = rc.wrapping_add(1);
            let new = Self::pack(new_rc, usage, flags);
            if self
                .state
                .compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return new_rc;
            }
        }
    }

    /// Atomically decrement the refcount. Returns the new refcount.
    ///
    /// Uses a CAS loop with Release ordering.
    #[inline]
    pub fn unpin(&self) -> u16 {
        loop {
            let old = self.state.load(Ordering::Acquire);
            let (rc, usage, flags) = Self::unpack(old);
            debug_assert!(rc > 0, "unpin called with refcount=0");
            let new_rc = rc.saturating_sub(1);
            let new = Self::pack(new_rc, usage, flags);
            if self
                .state
                .compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return new_rc;
            }
        }
    }

    /// Bump usage_count, capped at `MAX_USAGE_COUNT`.
    ///
    /// Uses Relaxed ordering (advisory hint for clock-sweep).
    #[inline]
    pub fn touch(&self) {
        loop {
            let old = self.state.load(Ordering::Relaxed);
            let (rc, usage, flags) = Self::unpack(old);
            let new_usage = if usage < MAX_USAGE_COUNT {
                usage + 1
            } else {
                MAX_USAGE_COUNT
            };
            if new_usage == usage {
                return; // already at max
            }
            let new = Self::pack(rc, new_usage, flags);
            if self
                .state
                .compare_exchange_weak(old, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Check if the DIRTY flag is set.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        let val = self.state.load(Ordering::Acquire);
        let (_, _, flags) = Self::unpack(val);
        flags & FLAG_DIRTY != 0
    }

    /// Set the DIRTY flag.
    #[inline]
    pub fn set_dirty(&self) {
        loop {
            let old = self.state.load(Ordering::Acquire);
            let new = old | (FLAG_DIRTY as u32);
            if old == new {
                return;
            }
            if self
                .state
                .compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Clear the DIRTY flag, preserving all other bits.
    #[inline]
    pub fn clear_dirty(&self) {
        loop {
            let old = self.state.load(Ordering::Acquire);
            let new = old & !(FLAG_DIRTY as u32);
            if old == new {
                return;
            }
            if self
                .state
                .compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Set the VALID flag.
    #[inline]
    pub fn set_valid(&self) {
        loop {
            let old = self.state.load(Ordering::Acquire);
            let new = old | (FLAG_VALID as u32);
            if old == new {
                return;
            }
            if self
                .state
                .compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Check if this frame can be evicted:
    /// refcount == 0, usage_count == 0, and IO_IN_PROGRESS not set.
    #[inline]
    pub fn is_evictable(&self) -> bool {
        let val = self.state.load(Ordering::Acquire);
        let (rc, usage, flags) = Self::unpack(val);
        rc == 0 && usage == 0 && (flags & FLAG_IO_IN_PROGRESS == 0)
    }

    /// Decrement usage_count by 1 (saturating). Returns the new usage_count.
    ///
    /// Used by clock-sweep: each pass decrements usage until it reaches 0.
    #[inline]
    pub fn decrement_usage(&self) -> u8 {
        loop {
            let old = self.state.load(Ordering::Relaxed);
            let (rc, usage, flags) = Self::unpack(old);
            let new_usage = usage.saturating_sub(1);
            if new_usage == usage {
                return usage; // already 0
            }
            let new = Self::pack(rc, new_usage, flags);
            if self
                .state
                .compare_exchange_weak(old, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return new_usage;
            }
        }
    }

    /// Load the raw packed u32 with Acquire ordering.
    #[inline]
    pub fn load(&self) -> u32 {
        self.state.load(Ordering::Acquire)
    }

    /// Store a raw packed u32 with Release ordering.
    #[inline]
    pub fn store(&self, val: u32) {
        self.state.store(val, Ordering::Release);
    }
}

/// Descriptor for a single frame in the buffer pool.
///
/// Tracks the packed atomic state alongside page identity and LSN.
pub struct FrameDescriptor {
    /// Packed atomic state (refcount | usage | flags).
    pub state: FrameState,
    /// File ID this frame belongs to (0 = unassigned).
    pub file_id: AtomicU64,
    /// Page offset within the file (0 = unassigned).
    pub page_offset: AtomicU64,
    /// LSN of the most recent modification to this page.
    /// Used by flush_page to enforce WAL-before-data invariant.
    pub page_lsn: AtomicU64,
}

impl FrameDescriptor {
    /// Create a new, zero-initialized frame descriptor.
    pub fn new() -> Self {
        Self {
            state: FrameState::new(),
            file_id: AtomicU64::new(0),
            page_offset: AtomicU64::new(0),
            page_lsn: AtomicU64::new(0),
        }
    }

    /// Reset this frame for reuse with a new page identity.
    ///
    /// Clears all state (refcount, usage, flags) and sets new identity.
    pub fn reset(&self, file_id: u64, page_offset: u64) {
        self.state.store(0);
        self.file_id.store(file_id, Ordering::Release);
        self.page_offset.store(page_offset, Ordering::Release);
        self.page_lsn.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_roundtrip() {
        let packed = FrameState::pack(5, 3, FLAG_DIRTY);
        let (rc, usage, flags) = FrameState::unpack(packed);
        assert_eq!(rc, 5);
        assert_eq!(usage, 3);
        assert_eq!(flags, FLAG_DIRTY);
    }

    #[test]
    fn test_pin_increments_refcount() {
        let state = FrameState::new();
        assert_eq!(state.pin(), 1);
        assert_eq!(state.pin(), 2);
        assert_eq!(state.pin(), 3);
        let (rc, _, _) = FrameState::unpack(state.load());
        assert_eq!(rc, 3);
    }

    #[test]
    fn test_unpin_decrements_refcount() {
        let state = FrameState::new();
        state.pin();
        state.pin();
        assert_eq!(state.unpin(), 1);
        assert_eq!(state.unpin(), 0);
    }

    #[test]
    fn test_touch_caps_at_max_usage() {
        let state = FrameState::new();
        state.touch();
        state.touch();
        state.touch();
        state.touch(); // should not exceed MAX_USAGE_COUNT
        let (_, usage, _) = FrameState::unpack(state.load());
        assert_eq!(usage, MAX_USAGE_COUNT);
    }

    #[test]
    fn test_clear_dirty_preserves_other_bits() {
        let state = FrameState::new();
        // Pin twice, touch once, set dirty
        state.pin();
        state.pin();
        state.touch();
        state.set_dirty();

        // Verify dirty
        assert!(state.is_dirty());

        // Clear dirty
        state.clear_dirty();
        assert!(!state.is_dirty());

        // Verify refcount and usage preserved
        let (rc, usage, flags) = FrameState::unpack(state.load());
        assert_eq!(rc, 2);
        assert_eq!(usage, 1);
        assert_eq!(flags & FLAG_DIRTY, 0);
    }

    #[test]
    fn test_initial_state_is_zeroed() {
        let state = FrameState::new();
        let (rc, usage, flags) = FrameState::unpack(state.load());
        assert_eq!(rc, 0);
        assert_eq!(usage, 0);
        assert_eq!(flags, 0);
    }

    #[test]
    fn test_frame_descriptor_stores_identity() {
        let fd = FrameDescriptor::new();
        fd.reset(42, 8192);
        assert_eq!(fd.file_id.load(Ordering::Acquire), 42);
        assert_eq!(fd.page_offset.load(Ordering::Acquire), 8192);

        // State should be cleared
        let (rc, usage, flags) = FrameState::unpack(fd.state.load());
        assert_eq!(rc, 0);
        assert_eq!(usage, 0);
        assert_eq!(flags, 0);

        // Set page_lsn
        fd.page_lsn.store(999, Ordering::Release);
        assert_eq!(fd.page_lsn.load(Ordering::Acquire), 999);
    }

    #[test]
    fn test_is_evictable() {
        let state = FrameState::new();
        // Fresh frame is evictable
        assert!(state.is_evictable());

        // Pinned frame is not evictable
        state.pin();
        assert!(!state.is_evictable());

        // Unpin, but touch -> not evictable (usage > 0)
        state.unpin();
        state.touch();
        assert!(!state.is_evictable());

        // Decrement usage to 0 -> evictable again
        state.decrement_usage();
        assert!(state.is_evictable());
    }

    #[test]
    fn test_decrement_usage() {
        let state = FrameState::new();
        state.touch(); // usage = 1
        state.touch(); // usage = 2
        assert_eq!(state.decrement_usage(), 1);
        assert_eq!(state.decrement_usage(), 0);
        assert_eq!(state.decrement_usage(), 0); // saturates at 0
    }

    #[test]
    fn test_io_in_progress_prevents_eviction() {
        let state = FrameState::new();
        // Manually set IO_IN_PROGRESS via store
        state.store(FrameState::pack(0, 0, FLAG_IO_IN_PROGRESS));
        assert!(!state.is_evictable());
    }
}
