//! Clock-sweep eviction algorithm for the PageCache.
//!
//! Implements PostgreSQL-style clock-sweep: a circular scan that decrements
//! usage counts and evicts the first frame with usage=0 and refcount=0.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::frame::FrameDescriptor;

/// Clock-sweep eviction scanner.
///
/// Maintains a clock hand that sweeps through the frame array. On each
/// call to `find_victim`, it scans up to `2 * num_frames` positions
/// (two full sweeps). For each frame:
/// - If evictable (refcount=0, usage=0, no IO): return it as victim
/// - Else: decrement usage_count and advance
///
/// If no victim is found after two full sweeps, all frames are pinned.
pub struct ClockSweep {
    clock_hand: AtomicUsize,
    num_frames: usize,
}

impl ClockSweep {
    /// Create a new clock sweep for a pool of `num_frames` frames.
    pub fn new(num_frames: usize) -> Self {
        Self {
            clock_hand: AtomicUsize::new(0),
            num_frames,
        }
    }

    /// Find a victim frame for eviction.
    ///
    /// Returns `Some(frame_index)` if a victim was found, `None` if all
    /// frames are pinned or in-use (after two full sweeps).
    pub fn find_victim(&self, frames: &[FrameDescriptor]) -> Option<usize> {
        let max_scan = 2 * self.num_frames;
        for _ in 0..max_scan {
            let pos = self.clock_hand.fetch_add(1, Ordering::Relaxed) % self.num_frames;
            let frame = &frames[pos];

            if frame.state.is_evictable() {
                return Some(pos);
            }

            // Decrement usage count (clock hand gives second chances)
            frame.state.decrement_usage();
        }

        None // all frames pinned or in-use after 2 sweeps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_sweep_finds_evictable() {
        // 4 frames: pin 0 and 1, touch 2, leave 3 untouched
        let frames: Vec<FrameDescriptor> = (0..4).map(|_| FrameDescriptor::new()).collect();
        frames[0].state.pin();
        frames[1].state.pin();
        frames[2].state.touch();
        // frame 3 is untouched -> evictable

        let sweep = ClockSweep::new(4);
        let victim = sweep.find_victim(&frames);
        // Frame 3 should be the victim (0,1 are pinned, 2 has usage>0 on first pass)
        assert_eq!(victim, Some(3));
    }

    #[test]
    fn test_clock_sweep_wraps_around() {
        let frames: Vec<FrameDescriptor> = (0..4).map(|_| FrameDescriptor::new()).collect();
        // Pin all except frame 0, but start clock hand past frame 0
        frames[1].state.pin();
        frames[2].state.pin();
        frames[3].state.pin();

        let sweep = ClockSweep::new(4);
        // Advance hand past frame 0
        sweep.clock_hand.store(1, Ordering::Relaxed);

        let victim = sweep.find_victim(&frames);
        // Should wrap around and find frame 0
        assert_eq!(victim, Some(0));
    }

    #[test]
    fn test_pinned_frames_never_evicted() {
        let frames: Vec<FrameDescriptor> = (0..4).map(|_| FrameDescriptor::new()).collect();
        // Pin all frames
        for f in &frames {
            f.state.pin();
        }

        let sweep = ClockSweep::new(4);
        let victim = sweep.find_victim(&frames);
        assert!(victim.is_none());
    }

    #[test]
    fn test_clock_sweep_decrements_usage_to_find_victim() {
        let frames: Vec<FrameDescriptor> = (0..2).map(|_| FrameDescriptor::new()).collect();
        // Both frames have usage=1, not pinned
        frames[0].state.touch(); // usage=1
        frames[1].state.touch(); // usage=1

        let sweep = ClockSweep::new(2);
        let victim = sweep.find_victim(&frames);
        // First pass decrements both to 0, second pass finds one evictable
        assert!(victim.is_some());
    }
}
