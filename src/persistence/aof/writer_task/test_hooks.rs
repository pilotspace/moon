//! Test-only fault injection for the per-shard AOF writer tasks.
//!
//! Driven by a `MOON_TEST_*` environment variable no production deployment
//! sets, like `MOON_TEST_SNAPSHOT_HOLD_FILE` (`shard::test_hooks`). The
//! variable is read ONCE per process, so with it unset the hook costs one
//! cached `Option` check per writer start — never per append.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use crate::runtime::cancel::CancellationToken;

/// `MOON_TEST_AOF_WRITER_HOLD=<shard>:<path>` (test-only): per-shard writer
/// `<shard>` does not start — does not load the manifest, open its incr file
/// or read its channel — while `<path>` exists. Every other writer starts
/// normally, and messages for the held one queue in its channel.
///
/// This is how `tests/perf_ws21_aof_writer_start.rs` reproduces moon#1271
/// deterministically: a writer that starts late, after a per-shard rewrite
/// has been dispatched, instead of racing boot under load. Removing the file
/// (or cancelling the writer) releases it.
pub(super) fn hold_writer_start(shard_id: u16, cancel: &CancellationToken) {
    static HOLD: OnceLock<Option<(u16, PathBuf)>> = OnceLock::new();
    let hold = HOLD.get_or_init(|| {
        let spec = std::env::var("MOON_TEST_AOF_WRITER_HOLD").ok()?;
        let (shard, path) = spec.split_once(':')?;
        Some((shard.trim().parse().ok()?, PathBuf::from(path)))
    });
    let Some((held, path)) = hold.as_ref() else {
        return;
    };
    if *held != shard_id || !path.exists() {
        return;
    }
    tracing::warn!(
        "AOF writer shard {shard_id}: start held until {} is removed (MOON_TEST_AOF_WRITER_HOLD)",
        path.display()
    );
    while path.exists() && !cancel.is_cancelled() {
        std::thread::sleep(Duration::from_millis(5));
    }
    tracing::warn!("AOF writer shard {shard_id}: start released");
}
