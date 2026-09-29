//! Test-only crash injection for the boot-time creation of a fresh AOF
//! generation (moon#1293).
//!
//! Driven by a `MOON_TEST_*` environment variable no production deployment
//! sets, like `MOON_TEST_SNAPSHOT_HOLD_FILE` (`shard::test_hooks`). The
//! variable is read ONCE per process; with it unset each hook point costs one
//! cached `Option` check, and the hook points run only while a boot creates a
//! generation (never per write).

use std::sync::OnceLock;

/// Exit status of a process stopped by [`crash_point`] — distinct from a
/// panic (101) or a refused start (1/2), so a test can tell the hook fired.
pub const INIT_CRASH_EXIT_CODE: i32 = 86;

/// Where a boot that creates a fresh AOF generation may be stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InitCrashPoint {
    /// Right after shard `.0`'s generation head is written and fsynced.
    AfterHead(u16),
    /// Right after the generation's manifest is durably committed.
    AfterCommit,
}

/// `MOON_TEST_AOF_INIT_CRASH=after_commit | after_head:<shard>` (test-only):
/// the process exits with [`INIT_CRASH_EXIT_CODE`] at that point of the
/// boot-time generation creation (`AofManifest::prepare*` + head + commit),
/// before it serves a client. Every write before the point is complete and
/// fsynced, so the directory is exactly what a SIGKILL at that instant
/// leaves (`tests/crash_aof_init_generation_1293.rs`).
pub(crate) fn crash_point(at: InitCrashPoint) {
    static POINT: OnceLock<Option<InitCrashPoint>> = OnceLock::new();
    let wanted = POINT.get_or_init(|| {
        let spec = std::env::var("MOON_TEST_AOF_INIT_CRASH").ok()?;
        let spec = spec.trim();
        if spec == "after_commit" {
            return Some(InitCrashPoint::AfterCommit);
        }
        let shard = spec.strip_prefix("after_head:")?.trim().parse().ok()?;
        Some(InitCrashPoint::AfterHead(shard))
    });
    if *wanted == Some(at) {
        tracing::error!(
            "MOON_TEST_AOF_INIT_CRASH: stopping the process at {at:?} (test only, moon#1293)"
        );
        eprintln!("MOON_TEST_AOF_INIT_CRASH: stopping the process at {at:?}");
        std::process::exit(INIT_CRASH_EXIT_CODE);
    }
}
