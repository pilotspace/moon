//! Test-only fault-injection hooks read by the shard's persistence tick.
//!
//! Every hook here is driven by a `MOON_TEST_*` environment variable that no
//! production deployment sets, like `MOON_TEST_SLOW_SHARD_START_MS` in the
//! event loop. Each one reads its variable ONCE, so with the variable unset a
//! hook costs one cached `Option` check.

use std::path::PathBuf;
use std::sync::OnceLock;

/// `MOON_TEST_SNAPSHOT_HOLD_FILE=<path>` (test-only): while a snapshot
/// (BGSAVE / auto-save) is in progress AND `<path>` exists, the shard does
/// not advance the snapshot's segments. The epoch stays armed and its
/// copy-on-write capture keeps running, so a test can put writes inside the
/// capture window by construction instead of racing the save
/// (`tests/perf_ws8_mset_bgsave_capture.rs`). Removing the file releases it.
///
/// With the variable unset the cost is one cached `Option` check per tick
/// while a snapshot runs; the file is stat'ed only when the variable is set.
pub(crate) fn snapshot_hold_requested_for_test() -> bool {
    static HOLD: OnceLock<Option<PathBuf>> = OnceLock::new();
    HOLD.get_or_init(|| std::env::var_os("MOON_TEST_SNAPSHOT_HOLD_FILE").map(PathBuf::from))
        .as_ref()
        .is_some_and(|path| path.exists())
}
