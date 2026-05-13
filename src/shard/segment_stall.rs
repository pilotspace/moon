//! MA1 — Write-stall on immutable segment backlog.
//!
//! This module implements Moon's analog of RocksDB's `level0_stop_writes_trigger`.
//! When the total immutable segment count across all vector/graph indexes on a
//! shard exceeds `--max-unflushed-immutable-segments`, foreground write commands
//! are refused with `MOONERR busy: compaction backlog` until compaction clears
//! the backlog.
//!
//! ## Separation of concerns
//!
//! - **MA12 (disk monitor):** sets `RECL_WRITE_STALL_ACTIVE` on disk-pressure events.
//! - **MA1 (this module):** sets `RECL_SEGMENT_STALL_ACTIVE` on segment-backlog events.
//! - **INFO `write_stall_active`:** emits `true` if EITHER bit is non-zero (OR of both).
//! - **Dispatch `try_enforce_write_stall`:** calls `is_any_write_stall_active()` which
//!   ORs `is_write_paused()` (MA12) with `is_segment_stall_active()` (MA1).
//!
//! ## Hot-path cost
//!
//! `is_segment_stall_active()` is a single `AtomicU64::load(Relaxed)` — identical
//! overhead to the existing MA12 `AtomicBool::load(Relaxed)` in `is_write_paused()`.
//! The segment count itself is updated on the 1s sweep tick, not on every write.
//!
//! ## Background compaction exemption
//!
//! Only foreground client writes (`metadata::is_write`) are checked. The
//! `FT.COMPACT` / `GRAPH.COMPACT` commands bypass this guard so compaction can
//! always proceed to drain the backlog.

use std::sync::atomic::Ordering;

use crate::command::info_reclamation::RECL_SEGMENT_STALL_ACTIVE;

/// Returns `true` if the segment-backlog stall bit is set.
///
/// **Hot-path function.** Single `AtomicU64::load(Relaxed)` — no allocation,
/// no lock. Updated by the 1s MVCC sweep tick via `update_segment_stall`.
#[inline]
pub fn is_segment_stall_active() -> bool {
    RECL_SEGMENT_STALL_ACTIVE.load(Ordering::Relaxed) != 0
}

/// Returns `true` if ANY write stall is active (disk-pressure OR segment-backlog).
///
/// This is the unified check used by both dispatch paths. OR-merges MA12 and MA1:
///
/// - `is_write_paused()` — MA12 disk-free monitor (AtomicBool, set every 5s).
/// - `is_segment_stall_active()` — MA1 segment backlog (AtomicU64, set every 1s).
#[inline]
pub fn is_any_write_stall_active() -> bool {
    crate::shard::disk_monitor::is_write_paused() || is_segment_stall_active()
}

/// Update the segment-stall atomic based on the current immutable segment count.
///
/// Called from the 1s MVCC sweep tick (`timers::run_mvcc_sweep`).
/// Sets `RECL_SEGMENT_STALL_ACTIVE` to 1 when `count > threshold`, 0 otherwise.
/// No-ops when `threshold == 0` (guard disabled).
#[inline]
pub fn update_segment_stall(count: usize, threshold: u64) {
    if threshold == 0 {
        RECL_SEGMENT_STALL_ACTIVE.store(0, Ordering::Relaxed);
        return;
    }
    let stalled = if count as u64 > threshold { 1 } else { 0 };
    RECL_SEGMENT_STALL_ACTIVE.store(stalled, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_segment_stall_sets_active_above_threshold() {
        update_segment_stall(21, 20);
        assert!(is_segment_stall_active());
        // Restore
        update_segment_stall(0, 20);
    }

    #[test]
    fn test_update_segment_stall_clears_below_threshold() {
        RECL_SEGMENT_STALL_ACTIVE.store(1, Ordering::Relaxed);
        update_segment_stall(19, 20);
        assert!(!is_segment_stall_active());
    }

    #[test]
    fn test_update_segment_stall_disabled_when_threshold_zero() {
        RECL_SEGMENT_STALL_ACTIVE.store(1, Ordering::Relaxed);
        update_segment_stall(9999, 0);
        // threshold == 0 means guard is off — stall cleared
        assert!(!is_segment_stall_active());
    }

    #[test]
    fn test_update_segment_stall_at_exact_threshold_is_not_stalled() {
        update_segment_stall(20, 20);
        // Strictly greater than threshold triggers stall
        assert!(
            !is_segment_stall_active(),
            "count == threshold must NOT stall"
        );
        update_segment_stall(0, 20);
    }
}
