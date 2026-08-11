//! #433 — automatic AOF rewrite (Redis parity: `auto-aof-rewrite-percentage`
//! / `auto-aof-rewrite-min-size`).
//!
//! The AOF is append-only: without compaction it grows with write volume,
//! not dataset size (observed in production: 4.8 GB appendonlydir over a
//! 2.43 GB dataset, ~1 GB/day), ending in the diskfull guard pausing writes.
//! This module adds the missing serverCron-equivalent: a small monitor
//! thread samples the on-disk AOF size once a second and dispatches the same
//! rewrite entry point the `BGREWRITEAOF` command uses when the growth
//! threshold is crossed.
//!
//! Semantics (mirrors Redis):
//! - trigger when `current >= min_size` AND
//!   `(current - base) * 100 / max(base, 1) >= percentage`
//! - `percentage == 0` disables automatic rewrites entirely.
//! - `base` is the total AOF size right after boot recovery and after each
//!   completed rewrite.
//!
//! Design-for-failure:
//! - A failed dispatch (backpressure, unsupported layout, …) arms a 60 s
//!   cooldown so the monitor cannot livelock hot-retrying a rewrite that
//!   keeps failing (lesson from the vector merge-backoff livelock).
//! - The monitor is skipped while a rewrite or BGSAVE is already running.
//! - Everything here is off the hot path: one directory walk per second.
//!
//! The sampled sizes double as the backing store for the `INFO persistence`
//! fields `aof_enabled` / `aof_base_size` / `aof_current_size` (#432 fixed
//! the hardcoded zeros).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tracing::{info, warn};

/// Whether AOF persistence is enabled at all (set once at boot when the
/// writer pool is created). Backs `INFO persistence` `aof_enabled` (#432).
pub static AOF_ENABLED: AtomicBool = AtomicBool::new(false);

/// Total on-disk AOF size (bytes) at boot / after the last completed
/// rewrite. Backs `INFO persistence` `aof_base_size`.
pub static AOF_BASE_SIZE: AtomicU64 = AtomicU64::new(0);

/// Most recently sampled total on-disk AOF size (bytes). Backs
/// `INFO persistence` `aof_current_size`. Refreshed by the monitor tick and
/// by `refresh_current_size` (INFO reads between ticks / monitor disabled).
pub static AOF_CURRENT_SIZE: AtomicU64 = AtomicU64::new(0);

/// The `appendonlydir` root the sizes are measured from.
static AOF_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Cooldown after a failed auto-rewrite dispatch. One minute mirrors the
/// "don't hot-retry a deterministic failure" backoff floor used elsewhere.
const FAILED_DISPATCH_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(60);

/// Monitor sampling cadence.
const TICK: std::time::Duration = std::time::Duration::from_secs(1);

/// Legacy single-file AOF path (`<dir>/<appendfilename>`), used by the tokio
/// TopLevel writer, which appends to one flat file instead of the
/// `appendonlydir` manifest layout the monoio writers use. Both are measured;
/// whichever exists contributes (they never coexist for one server).
static AOF_LEGACY_FILE: OnceLock<PathBuf> = OnceLock::new();

/// Record the AOF locations (`<dir>/appendonlydir` + the legacy
/// `<dir>/<appendfilename>` flat file) and initialize the base / current
/// sizes from what recovery just replayed. Call once at boot, after AOF
/// recovery, when appendonly is enabled.
pub fn init(persistence_dir: &Path, appendfilename: &str) {
    let _ = AOF_DIR.set(persistence_dir.join("appendonlydir"));
    let _ = AOF_LEGACY_FILE.set(persistence_dir.join(appendfilename));
    AOF_ENABLED.store(true, Ordering::Relaxed);
    record_base_size();
}

/// Total on-disk AOF size: every file under the manifest root (base RDBs +
/// incr logs + manifest, all generations, both manifest layouts) PLUS the
/// legacy flat file when present. 0 when uninitialized.
pub fn measure_total_size() -> u64 {
    fn walk(p: &Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(p) else {
            return;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, total);
            } else if let Ok(md) = e.metadata() {
                *total += md.len();
            }
        }
    }
    let mut total = 0;
    if let Some(dir) = AOF_DIR.get() {
        walk(dir, &mut total);
    }
    if let Some(file) = AOF_LEGACY_FILE.get()
        && let Ok(md) = std::fs::metadata(file)
    {
        total += md.len();
    }
    total
}

/// Re-measure and store BOTH base and current size — the AOF was just
/// compacted (or just recovered), so "now" is the new growth baseline.
pub fn record_base_size() {
    let size = measure_total_size();
    AOF_BASE_SIZE.store(size, Ordering::Relaxed);
    AOF_CURRENT_SIZE.store(size, Ordering::Relaxed);
}

/// Re-measure the current size only (INFO freshness between monitor ticks).
pub fn refresh_current_size() -> u64 {
    let size = measure_total_size();
    AOF_CURRENT_SIZE.store(size, Ordering::Relaxed);
    size
}

/// The Redis auto-rewrite predicate. Pure so the boundary math is unit
/// tested without a filesystem.
pub fn should_trigger(current: u64, base: u64, percentage: u64, min_size: u64) -> bool {
    if percentage == 0 || current < min_size {
        return false;
    }
    // Redis uses max(base, 1) so a never-rewritten (base 0) AOF still
    // triggers once it crosses min_size.
    let base = base.max(1);
    current.saturating_sub(base).saturating_mul(100) / base >= percentage
}

/// Spawn the auto-rewrite monitor thread. `percentage == 0` still spawns the
/// sampler (INFO size freshness) but never dispatches a rewrite.
///
/// Dispatches through [`crate::command::persistence::bgrewriteaof_start_sharded`],
/// the exact entry the `BGREWRITEAOF` command uses — CAS on the in-progress
/// flag, per-shard fan-out vs TopLevel routing, and error mapping included.
pub fn spawn_monitor(
    pool: Arc<super::AofWriterPool>,
    shard_databases: Arc<crate::shard::shared_databases::ShardDatabases>,
    percentage: u64,
    min_size: u64,
) {
    let spawned = std::thread::Builder::new()
        .name("aof-auto-rewrite".to_string())
        .spawn(move || {
            monitor_loop(&pool, &shard_databases, percentage, min_size);
        });
    if let Err(e) = spawned {
        // Non-fatal: manual BGREWRITEAOF still works; sizes go stale.
        warn!("aof-auto-rewrite monitor failed to spawn: {e}");
    }
}

fn monitor_loop(
    pool: &super::AofWriterPool,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    percentage: u64,
    min_size: u64,
) {
    use crate::command::persistence::{AOF_REWRITE_IN_PROGRESS, SAVE_IN_PROGRESS};
    use crate::protocol::Frame;

    let mut cooldown_until = std::time::Instant::now();
    let mut saw_in_progress = false;
    info!(
        "aof-auto-rewrite monitor started (percentage={}%, min_size={} bytes)",
        percentage, min_size
    );
    loop {
        std::thread::sleep(TICK);

        let in_progress = AOF_REWRITE_IN_PROGRESS.load(Ordering::SeqCst);
        let current = refresh_current_size();

        // A rewrite completed since the last tick (ours or a manual
        // BGREWRITEAOF): the compacted size is the new growth baseline.
        // The `current < base` clause catches rewrites so fast that both
        // the set and the clear of the flag fell between two ticks — the
        // only other way the AOF shrinks is a rewrite committing + pruning.
        let completed_since_last_tick = saw_in_progress && !in_progress;
        let shrunk_below_base = current < AOF_BASE_SIZE.load(Ordering::Relaxed);
        saw_in_progress = in_progress;
        if completed_since_last_tick || (!in_progress && shrunk_below_base) {
            record_base_size();
            continue;
        }

        if percentage == 0
            || in_progress
            || SAVE_IN_PROGRESS.load(Ordering::SeqCst)
            || std::time::Instant::now() < cooldown_until
        {
            continue;
        }

        let base = AOF_BASE_SIZE.load(Ordering::Relaxed);
        if !should_trigger(current, base, percentage, min_size) {
            continue;
        }

        info!(
            "aof-auto-rewrite: triggering BGREWRITEAOF (current={} base={} \
             growth>={}%, min_size={})",
            current, base, percentage, min_size
        );
        match crate::command::persistence::bgrewriteaof_start_sharded(pool, shard_databases.clone())
        {
            Frame::Error(e) => {
                warn!(
                    "aof-auto-rewrite: dispatch failed ({}); retrying in {:?}",
                    String::from_utf8_lossy(&e),
                    FAILED_DISPATCH_COOLDOWN
                );
                cooldown_until = std::time::Instant::now() + FAILED_DISPATCH_COOLDOWN;
            }
            _ => {
                // Started. Wait for completion here (bounded) so the rebase
                // is deterministic even when the whole rewrite fits inside
                // one tick; the transition/shrink detection above is the
                // fallback for manual rewrites.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
                while AOF_REWRITE_IN_PROGRESS.load(Ordering::SeqCst)
                    && std::time::Instant::now() < deadline
                {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                record_base_size();
                saw_in_progress = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::should_trigger;

    #[test]
    fn percentage_zero_never_triggers() {
        assert!(!should_trigger(u64::MAX, 0, 0, 0));
        assert!(!should_trigger(1 << 30, 64, 0, 1024));
    }

    #[test]
    fn min_size_floor_holds() {
        // 100% growth but below the floor: no trigger.
        assert!(!should_trigger(1000, 500, 100, 4096));
        // At the floor: trigger.
        assert!(should_trigger(4096, 2048, 100, 4096));
    }

    #[test]
    fn growth_boundary_is_inclusive() {
        // base 1000, +100% => current 2000 triggers, 1999 does not.
        assert!(should_trigger(2000, 1000, 100, 0));
        assert!(!should_trigger(1999, 1000, 100, 0));
        // 50% threshold.
        assert!(should_trigger(1500, 1000, 50, 0));
        assert!(!should_trigger(1499, 1000, 50, 0));
    }

    #[test]
    fn zero_base_uses_one_like_redis() {
        // Never-rewritten AOF: any current >= min_size is astronomically
        // over any percentage of base=1.
        assert!(should_trigger(4096, 0, 100, 4096));
        assert!(!should_trigger(4095, 0, 100, 4096));
    }

    #[test]
    fn no_overflow_at_extremes() {
        assert!(should_trigger(u64::MAX, 1, 100, 0));
        assert!(!should_trigger(0, u64::MAX, 100, 0));
    }
}
