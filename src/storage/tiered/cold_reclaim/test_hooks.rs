//! Test-only fault injection for the no-AOF cold reclaim (moon#1297).
//!
//! Driven by `MOON_TEST_*` environment variables no production deployment
//! sets, like `MOON_TEST_SNAPSHOT_HOLD_FILE` (`shard::test_hooks`) and
//! `MOON_TEST_AOF_INIT_CRASH` (`persistence::aof_manifest::test_hooks`). Each
//! variable is read ONCE per process; with it unset a hook point costs one
//! cached `Option` check, and every hook point sits on the reclaim's own
//! path (a tick with a compaction to apply, a snapshot start with one
//! pending), never on a command.
//!
//! - `MOON_TEST_COLD_RECLAIM_CRASH=<point>` stops the process with
//!   [`RECLAIM_CRASH_EXIT_CODE`] at that point of a no-AOF compaction's life
//!   ([`ReclaimCrashPoint`]). Every write before the point is complete (the
//!   compacted file fsynced, the listing's commit acked), so the directory is
//!   what a SIGKILL at that instant leaves
//!   (`tests/cold_block_reclaim_no_aof_1297.rs`).
//! - `MOON_TEST_COLD_RECLAIM_HOLD_FILE=<path>`: while `<path>` exists no new
//!   no-AOF compaction starts, so a test can make deletions durable before
//!   the compaction reads a file, by construction instead of racing the tick.

use std::path::PathBuf;
use std::sync::OnceLock;

/// Exit status of a process stopped by [`crash_point`]: distinct from a panic
/// (101), a refused start (1/2) and `MOON_TEST_AOF_INIT_CRASH` (86).
pub const RECLAIM_CRASH_EXIT_CODE: i32 = 87;

/// Where a no-AOF compaction may be stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimCrashPoint {
    /// `compacted`: the compaction's output `F'` is written and fsynced, and
    /// recorded; nothing lists it.
    Compacted,
    /// `snapshot_start`: a snapshot is starting while a compaction waits
    /// (its trailer is built; nothing of it is written yet).
    SnapshotStart,
    /// `adopt_ready`: a snapshot that started after the compaction has
    /// committed; `F'` is not listed yet.
    AdoptReady,
    /// `listed`: the manifest listing `F'` is durable; no survivor is
    /// re-pointed and `F` is still on disk and listed.
    Listed,
    /// `unlinked`: the survivors are re-pointed and `F` is removed; its
    /// tombstone commit was handed to the manifest-sync thread.
    Unlinked,
}

impl ReclaimCrashPoint {
    fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "compacted" => Some(Self::Compacted),
            "snapshot_start" => Some(Self::SnapshotStart),
            "adopt_ready" => Some(Self::AdoptReady),
            "listed" => Some(Self::Listed),
            "unlinked" => Some(Self::Unlinked),
            _ => None,
        }
    }
}

/// Stop the process here if `MOON_TEST_COLD_RECLAIM_CRASH` names `at`.
pub(crate) fn crash_point(at: ReclaimCrashPoint) {
    static POINT: OnceLock<Option<ReclaimCrashPoint>> = OnceLock::new();
    let wanted = POINT.get_or_init(|| {
        std::env::var("MOON_TEST_COLD_RECLAIM_CRASH")
            .ok()
            .and_then(|s| ReclaimCrashPoint::parse(&s))
    });
    if *wanted == Some(at) {
        tracing::error!(
            "MOON_TEST_COLD_RECLAIM_CRASH: stopping the process at {at:?} (test only, moon#1297)"
        );
        eprintln!("MOON_TEST_COLD_RECLAIM_CRASH: stopping the process at {at:?}");
        std::process::exit(RECLAIM_CRASH_EXIT_CODE);
    }
}

/// Whether `MOON_TEST_COLD_RECLAIM_HOLD_FILE` holds new compactions back.
pub(crate) fn compaction_held_for_test() -> bool {
    static HOLD: OnceLock<Option<PathBuf>> = OnceLock::new();
    HOLD.get_or_init(|| std::env::var_os("MOON_TEST_COLD_RECLAIM_HOLD_FILE").map(PathBuf::from))
        .as_ref()
        .is_some_and(|path| path.exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_point_parses_and_nothing_else_does() {
        for (s, p) in [
            ("compacted", ReclaimCrashPoint::Compacted),
            ("snapshot_start", ReclaimCrashPoint::SnapshotStart),
            (" adopt_ready\n", ReclaimCrashPoint::AdoptReady),
            ("listed", ReclaimCrashPoint::Listed),
            ("unlinked", ReclaimCrashPoint::Unlinked),
        ] {
            assert_eq!(ReclaimCrashPoint::parse(s), Some(p));
        }
        assert_eq!(ReclaimCrashPoint::parse(""), None);
        assert_eq!(ReclaimCrashPoint::parse("list"), None);
    }
}
