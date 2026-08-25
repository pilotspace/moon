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
//! - **Wave 3 (mem monitor):** sets `RECL_MEM_WATCHDOG_ACTIVE` on RSS-pressure events.
//! - **INFO `write_stall_active`:** emits `true` if ANY bit is non-zero (OR of all three).
//! - **Dispatch `try_enforce_write_stall`:** calls `is_any_write_stall_active()` which
//!   ORs `is_write_paused()` (MA12) with `is_segment_stall_active()` (MA1) with
//!   `mem_monitor::is_write_paused()` (Wave 3).
//!
//! ## Hot-path cost
//!
//! `is_segment_stall_active()` is a single `AtomicU64::load(Relaxed)` — identical
//! overhead to the existing MA12 `AtomicBool::load(Relaxed)` in `is_write_paused()`.
//! The segment count itself is updated on the 1s sweep tick, not on every write.
//!
//! ## Background compaction exemption
//!
//! Only foreground client writes (`metadata::is_write`) are checked, so the
//! background merge task is never gated by the backlog it exists to drain.
//!
//! Client commands are a different matter, and this doc used to claim more than
//! the code did: it said `FT.COMPACT` / `GRAPH.COMPACT` "bypass this guard",
//! but the registry flags `FT.COMPACT` `W`, so it was refused by the very
//! backlog it drains — and `GRAPH.COMPACT` is not a command at all. See
//! [`stall_refusal`] and moon#718 for the exemption as it now actually exists.

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

/// Returns `true` if ANY write stall is active (disk-pressure OR segment-backlog
/// OR RSS memory pressure).
///
/// This is the unified check used by both dispatch paths. OR-merges MA12, MA1,
/// and Wave 3:
///
/// - `disk_monitor::is_write_paused()` — MA12 disk-free monitor (AtomicBool, set every 5s).
/// - `is_segment_stall_active()` — MA1 segment backlog (AtomicU64, set every 1s).
/// - `mem_monitor::is_write_paused()` — Wave 3 RSS watchdog (AtomicBool, set every 5s).
#[inline]
pub fn is_any_write_stall_active() -> bool {
    crate::shard::disk_monitor::is_write_paused()
        || is_segment_stall_active()
        || crate::shard::mem_monitor::is_write_paused()
}

/// Which stall sources are currently active.
///
/// Parameterised rather than read from the atomics inside [`stall_refusal`] so
/// the decision is a pure function: every combination is unit-testable without
/// mutating process-global state that other tests share.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StallSources {
    /// The data directory was removed (moon#366). Also sets `disk`.
    pub dir_lost: bool,
    /// MA12 disk-free monitor.
    pub disk: bool,
    /// Wave 3 RSS watchdog.
    pub mem: bool,
    /// MA1 immutable-segment backlog.
    pub segment: bool,
}

impl StallSources {
    /// Read the live atomics.
    #[inline]
    #[must_use]
    pub fn current() -> Self {
        Self {
            dir_lost: crate::shard::disk_monitor::is_dir_lost(),
            disk: crate::shard::disk_monitor::is_write_paused(),
            mem: crate::shard::mem_monitor::is_write_paused(),
            segment: is_segment_stall_active(),
        }
    }
}

/// The commands that are the REMEDY for a segment backlog, and so must never be
/// refused by one (moon#718).
///
/// `FT.COMPACT` drains the backlog. `FT.CONFIG SET <idx> MERGE_RECALL_TOLERANCE`
/// relaxes the recall gate that a repeatedly-failing merge is stuck behind —
/// and it is the remedy the rejection log line itself recommends.
///
/// Without this, a store whose background merges keep failing reaches a
/// permanent write outage with no in-band recovery: segments accumulate to
/// `--max-unflushed-immutable-segments`, every foreground write is refused, and
/// both documented escapes are refused too because the registry flags them `W`.
/// The only exit was a restart, which replays into the same state.
///
/// This is deliberately narrow. It exempts these commands from the SEGMENT
/// backlog only — [`stall_refusal`] answers for `dirmissing`, `diskfull` and
/// `memfull` BEFORE consulting it, so a compaction that would write new segment
/// files onto a full disk is still refused. A remedy for one stall is not a
/// licence to ignore the others.
///
/// `VACUUM` is absent because it is already exempt by construction: the registry
/// flags it `A` (admin), not `W`, so `is_write` is false and no stall sees it.
#[inline]
#[must_use]
fn remedies_a_segment_backlog(cmd: &[u8]) -> bool {
    match cmd.len() {
        9 => cmd.eq_ignore_ascii_case(b"FT.CONFIG"),
        10 => cmd.eq_ignore_ascii_case(b"FT.COMPACT"),
        _ => false,
    }
}

/// The error a write stall owes this command, or `None` to let it through.
///
/// Single source of truth for both dispatch paths, which previously carried
/// byte-identical copies of the message ladder.
///
/// Order matters: `dir_lost` is reported before `disk` because it also sets
/// the disk bit, and "diskfull" would send an operator hunting for free space
/// instead of the missing data directory (moon#366).
#[must_use]
pub fn stall_refusal(cmd: &[u8], sources: StallSources) -> Option<&'static [u8]> {
    if sources.dir_lost {
        return Some(
            b"MOONERR dirmissing: data directory was removed; writes refused until it is restored",
        );
    }
    if sources.disk {
        return Some(b"MOONERR diskfull: writes paused until free space recovers");
    }
    if sources.mem {
        return Some(b"MOONERR memfull: writes paused until memory pressure recovers");
    }
    if sources.segment {
        // moon#718: the backlog must not refuse its own remedy.
        if remedies_a_segment_backlog(cmd) {
            return None;
        }
        return Some(b"MOONERR busy: compaction backlog; too many unflushed immutable segments");
    }
    None
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

    use super::{StallSources, stall_refusal};

    const BUSY: &[u8] = b"MOONERR busy: compaction backlog; too many unflushed immutable segments";

    fn only_segment() -> StallSources {
        StallSources {
            segment: true,
            ..StallSources::default()
        }
    }

    /// moon#718: the whole outage. A store whose merges keep failing refuses
    /// every foreground write, and BOTH documented escapes are foreground
    /// writes — `FT.COMPACT` drains the backlog, `FT.CONFIG SET …
    /// MERGE_RECALL_TOLERANCE 0` relaxes the gate the merge is stuck behind,
    /// and the rejection log line recommends exactly that command. With both
    /// refused there is no in-band exit at all: restarting replays into the
    /// same state.
    #[test]
    fn a_segment_backlog_does_not_refuse_its_own_remedy() {
        assert_eq!(stall_refusal(b"FT.COMPACT", only_segment()), None);
        assert_eq!(stall_refusal(b"FT.CONFIG", only_segment()), None);
        // Case-insensitive, like every other command compare in the codebase.
        assert_eq!(stall_refusal(b"ft.compact", only_segment()), None);
        assert_eq!(stall_refusal(b"Ft.Config", only_segment()), None);
    }

    /// The exemption must not become a general write bypass — that would turn a
    /// backpressure guard into a suggestion.
    #[test]
    fn ordinary_writes_are_still_refused_by_the_backlog() {
        assert_eq!(stall_refusal(b"SET", only_segment()), Some(BUSY));
        assert_eq!(stall_refusal(b"HSET", only_segment()), Some(BUSY));
        // Neighbours in the same family are NOT exempt: they add documents,
        // which is what built the backlog.
        assert_eq!(stall_refusal(b"FT.CREATE", only_segment()), Some(BUSY));
        assert_eq!(stall_refusal(b"FT.SEARCH", only_segment()), Some(BUSY));
        // A name that merely shares a prefix or a length must not slip through.
        assert_eq!(stall_refusal(b"FT.CONFIGX", only_segment()), Some(BUSY));
        assert_eq!(stall_refusal(b"FT.CONFI", only_segment()), Some(BUSY));
        assert_eq!(stall_refusal(b"FT.COMPACTX", only_segment()), Some(BUSY));
    }

    /// A remedy for ONE stall is not a licence to ignore the others. Compaction
    /// writes new segment files, so it must still be refused when the disk is
    /// full or the data directory is gone.
    #[test]
    fn the_exemption_is_scoped_to_the_segment_stall_alone() {
        for (label, src) in [
            (
                "dir_lost",
                StallSources {
                    dir_lost: true,
                    disk: true,
                    segment: true,
                    ..StallSources::default()
                },
            ),
            (
                "disk",
                StallSources {
                    disk: true,
                    segment: true,
                    ..StallSources::default()
                },
            ),
            (
                "mem",
                StallSources {
                    mem: true,
                    segment: true,
                    ..StallSources::default()
                },
            ),
        ] {
            assert!(
                stall_refusal(b"FT.COMPACT", src).is_some(),
                "{label} must still refuse FT.COMPACT: the backlog exemption is \
                 not a bypass for disk or memory pressure"
            );
        }
    }

    /// The message ladder both dispatch paths used to duplicate. `dir_lost` is
    /// reported before `diskfull` because it also sets the disk bit, and
    /// "diskfull" would send an operator hunting free space instead of the
    /// missing data directory (moon#366).
    #[test]
    fn the_message_ladder_reports_the_most_specific_source() {
        let dir = StallSources {
            dir_lost: true,
            disk: true,
            mem: true,
            segment: true,
        };
        assert!(
            stall_refusal(b"SET", dir)
                .expect("stalled")
                .starts_with(b"MOONERR dirmissing")
        );
        let disk = StallSources {
            disk: true,
            mem: true,
            segment: true,
            ..StallSources::default()
        };
        assert!(
            stall_refusal(b"SET", disk)
                .expect("stalled")
                .starts_with(b"MOONERR diskfull")
        );
        let mem = StallSources {
            mem: true,
            segment: true,
            ..StallSources::default()
        };
        assert!(
            stall_refusal(b"SET", mem)
                .expect("stalled")
                .starts_with(b"MOONERR memfull")
        );
    }

    /// No stall, no refusal — including for the exempt commands, which must not
    /// acquire any special status on the healthy path.
    #[test]
    fn nothing_is_refused_when_no_stall_is_active() {
        let calm = StallSources::default();
        for cmd in [&b"SET"[..], b"FT.COMPACT", b"FT.CONFIG", b"FT.SEARCH"] {
            assert_eq!(stall_refusal(cmd, calm), None);
        }
    }
}
