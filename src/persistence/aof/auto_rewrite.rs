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
//! - `base` is the size of the AOF's *compacted form*: at boot, the base
//!   RDB(s) the committed manifest names (never the uncompacted incr — #868:
//!   seeding from the directory total raised the bar by the incr on every
//!   restart, permanently, so a restart-prone host never rewrote); after a
//!   completed rewrite, the freshly compacted directory, whose incr is empty.
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{info, warn};

use crate::persistence::aof_manifest::{AofLayout, AofManifest};

/// Whether AOF persistence is enabled at all (set once at boot when the
/// writer pool is created). Backs `INFO persistence` `aof_enabled` (#432).
pub static AOF_ENABLED: AtomicBool = AtomicBool::new(false);

/// Compacted AOF size (bytes): the base RDB(s) at boot, the total on-disk
/// size after the last completed rewrite. Backs `INFO persistence`
/// `aof_base_size`.
pub static AOF_BASE_SIZE: AtomicU64 = AtomicU64::new(0);

/// Most recently sampled total on-disk AOF size (bytes). Backs
/// `INFO persistence` `aof_current_size`. Refreshed by the monitor tick and
/// by `refresh_current_size` (INFO reads between ticks / monitor disabled).
pub static AOF_CURRENT_SIZE: AtomicU64 = AtomicU64::new(0);

/// The `appendonlydir` root the sizes are measured from. Written once by
/// [`init`] at boot; a lock rather than a `OnceLock` so unit tests can
/// re-seed it per scratch directory (the monitor reads it once a second).
static AOF_DIR: parking_lot::RwLock<Option<PathBuf>> = parking_lot::RwLock::new(None);

/// Cooldown after a failed auto-rewrite dispatch. One minute mirrors the
/// "don't hot-retry a deterministic failure" backoff floor used elsewhere.
const FAILED_DISPATCH_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(60);

/// Monitor sampling cadence.
const TICK: std::time::Duration = std::time::Duration::from_secs(1);

/// Legacy single-file AOF path (`<dir>/<appendfilename>`), used by the tokio
/// TopLevel writer, which appends to one flat file instead of the
/// `appendonlydir` manifest layout the monoio writers use. Both are measured;
/// whichever exists contributes (they never coexist for one server).
static AOF_LEGACY_FILE: parking_lot::RwLock<Option<PathBuf>> = parking_lot::RwLock::new(None);

/// Record the AOF locations (`<dir>/appendonlydir` + the legacy
/// `<dir>/<appendfilename>` flat file) and initialize the base / current
/// sizes from what recovery just replayed. Call once at boot, after AOF
/// recovery, when appendonly is enabled.
///
/// Base is seeded from the compacted form only ([`measure_base_size_at`]);
/// current from the whole directory. The two differ by exactly the incr
/// recovery just replayed, which is growth the monitor must still see.
pub fn init(persistence_dir: &Path, appendfilename: &str) {
    let legacy_file = persistence_dir.join(appendfilename);
    *AOF_DIR.write() = Some(persistence_dir.join("appendonlydir"));
    *AOF_LEGACY_FILE.write() = Some(legacy_file.clone());
    AOF_ENABLED.store(true, Ordering::Relaxed);
    let base = measure_base_size_at(persistence_dir, &legacy_file);
    AOF_BASE_SIZE.store(base, Ordering::Relaxed);
    AOF_CURRENT_SIZE.store(measure_total_size(), Ordering::Relaxed);
}

/// Size of the AOF's compacted form: the base RDB(s) the committed
/// `moon.aof.manifest` names — one file in the TopLevel layout, one per
/// shard in PerShard, always at the manifest's current `seq` — plus the
/// legacy flat file `legacy_file` when present (the tokio TopLevel writer's
/// single `appendonly.aof` has no separable base, so the whole file is its
/// base, exactly as Redis treats a pre-multipart AOF). Incr logs, the
/// manifest itself and any orphaned generation on disk are not base.
///
/// Every degraded case resolves toward a *smaller* base, never a larger one,
/// so the scheduler can only become more willing to compact:
/// - no manifest: nothing has been compacted, base is 0 (Redis `max(base,1)`
///   then fires at the first crossing of `min_size`);
/// - the manifest names a base file that is not on disk: that file
///   contributes 0 and a warning is logged;
/// - the manifest is unreadable: 0 and a warning. Recovery already treats a
///   corrupt manifest as fatal before this runs, so this arm is defensive.
///
/// `AofManifest::load` also re-runs its best-effort orphan sweep; recovery
/// ran the same sweep moments earlier, so at boot it is a no-op.
pub fn measure_base_size_at(persistence_dir: &Path, legacy_file: &Path) -> u64 {
    fn file_len(path: &Path) -> u64 {
        match std::fs::metadata(path) {
            Ok(md) => md.len(),
            Err(e) => {
                warn!(
                    "aof-auto-rewrite: base file {} named by the manifest is unreadable ({e}); \
                     counting it as 0 bytes of base",
                    path.display()
                );
                0
            }
        }
    }

    let mut base = 0u64;
    match AofManifest::load(persistence_dir) {
        Ok(Some(m)) => match m.layout {
            AofLayout::TopLevel => base += file_len(&m.base_path()),
            AofLayout::PerShard => {
                for shard in &m.shards {
                    base += file_len(&m.shard_base_path(shard.shard_id));
                }
            }
        },
        Ok(None) => {}
        Err(e) => warn!(
            "aof-auto-rewrite: cannot read the AOF manifest under {} ({e}); \
             seeding base size as 0",
            persistence_dir.display()
        ),
    }
    if let Ok(md) = std::fs::metadata(legacy_file) {
        base += md.len();
    }
    base
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
    if let Some(dir) = AOF_DIR.read().as_deref() {
        walk(dir, &mut total);
    }
    if let Some(file) = AOF_LEGACY_FILE.read().as_deref()
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

    // ── #868: init() must seed the base from the compacted base file(s), not
    // the whole directory, or every restart ratchets the trigger up by the
    // uncompacted incr. These tests share the module statics, so they run
    // serialized under INIT_LOCK.

    use super::{AOF_BASE_SIZE, AOF_CURRENT_SIZE, init};
    use crate::persistence::aof_manifest::AofManifest;
    use std::path::Path;
    use std::sync::atomic::Ordering;

    static INIT_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    const APPENDFILENAME: &str = "appendonly.aof";

    fn write_bytes(path: &Path, n: usize) {
        std::fs::write(path, vec![b'x'; n]).expect("write fixture file");
    }

    fn base_size() -> u64 {
        AOF_BASE_SIZE.load(Ordering::Relaxed)
    }

    fn current_size() -> u64 {
        AOF_CURRENT_SIZE.load(Ordering::Relaxed)
    }

    /// The reported base must be the base file's size — not base + incr.
    #[test]
    fn init_seeds_base_from_manifest_base_file_not_directory_total() {
        let _g = INIT_LOCK.lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let m = AofManifest::initialize(tmp.path()).expect("initialize manifest");
        write_bytes(&m.base_path(), 100_000);
        write_bytes(&m.incr_path(), 60_000);

        init(tmp.path(), APPENDFILENAME);

        assert_eq!(base_size(), 100_000, "base must be the base RDB alone");
        assert!(
            current_size() >= 160_000,
            "current must still count base + incr (+ manifest), got {}",
            current_size()
        );
    }

    /// The user-visible bug: a restart with an unchanged dataset must not
    /// move the trigger point (issue: 16,280,244 -> 23,600,244 across one
    /// restart on the same directory).
    #[test]
    fn restart_does_not_ratchet_the_trigger_point() {
        let _g = INIT_LOCK.lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let m = AofManifest::initialize(tmp.path()).expect("initialize manifest");
        write_bytes(&m.base_path(), 100_000);
        init(tmp.path(), APPENDFILENAME);
        // Whatever the first boot seeds is the reference; the contamination
        // itself is asserted by `init_seeds_base_from_manifest_base_file…`.
        // This test is only about the bar moving between boots.
        let base_after_rewrite = base_size();

        // Incr grows to 45% of base: under the 100% trigger.
        write_bytes(&m.incr_path(), 45_000);
        let current = super::refresh_current_size();
        assert!(!should_trigger(current, base_size(), 100, 0));

        // "Restart": same directory, nothing compacted, nothing written.
        init(tmp.path(), APPENDFILENAME);

        assert_eq!(
            base_size(),
            base_after_rewrite,
            "restart moved the base (and therefore the trigger point)"
        );
        assert!(
            !should_trigger(current_size(), base_size(), 100, 0),
            "still under the threshold after restart"
        );
        // And the very next growth past 2x base fires — the bar did not move.
        write_bytes(&m.incr_path(), 100_000);
        assert!(should_trigger(
            super::refresh_current_size(),
            base_size(),
            100,
            0
        ));
    }

    /// Guard against "fixing" this by never firing: growth past the
    /// threshold, measured against the seeded base, still triggers.
    #[test]
    fn rewrite_still_fires_when_growth_crosses_threshold() {
        let _g = INIT_LOCK.lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let m = AofManifest::initialize(tmp.path()).expect("initialize manifest");
        write_bytes(&m.base_path(), 100_000);
        write_bytes(&m.incr_path(), 100_000);

        init(tmp.path(), APPENDFILENAME);

        assert!(should_trigger(current_size(), base_size(), 100, 0));
        // The min-size floor is still honoured.
        assert!(!should_trigger(current_size(), base_size(), 100, 1 << 30));
    }

    /// First boot: no manifest, no legacy file — nothing has been compacted,
    /// so base is 0 (Redis `max(base, 1)` fires at min_size).
    #[test]
    fn no_manifest_yet_seeds_zero() {
        let _g = INIT_LOCK.lock();
        let tmp = tempfile::tempdir().expect("tempdir");

        init(tmp.path(), APPENDFILENAME);

        assert_eq!(base_size(), 0);
        assert_eq!(current_size(), 0);
    }

    /// Manifest names a base file that is not on disk: contributes 0 rather
    /// than falling back to the directory total.
    #[test]
    fn manifest_naming_missing_base_file_seeds_zero_base() {
        let _g = INIT_LOCK.lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let m = AofManifest::initialize(tmp.path()).expect("initialize manifest");
        write_bytes(&m.incr_path(), 70_000);
        std::fs::remove_file(m.base_path()).expect("remove base");

        init(tmp.path(), APPENDFILENAME);

        assert_eq!(base_size(), 0);
        assert!(current_size() >= 70_000);
    }

    /// Legacy flat-file layout (tokio TopLevel writer): no manifest, one
    /// `appendonly.aof`. There is no separable base, so the whole file is
    /// the base — the same number recovery replayed.
    #[test]
    fn legacy_flat_file_is_its_own_base() {
        let _g = INIT_LOCK.lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        write_bytes(&tmp.path().join(APPENDFILENAME), 5_000);

        init(tmp.path(), APPENDFILENAME);

        assert_eq!(base_size(), 5_000);
        assert_eq!(current_size(), 5_000);
    }

    /// PerShard layout: "the base" is the sum of every shard's base RDB at
    /// the manifest's committed seq. Incr files and the manifest are not
    /// base; an older generation left on disk is an orphan, not base.
    #[test]
    fn per_shard_layout_sums_every_shards_base() {
        let _g = INIT_LOCK.lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let m = AofManifest::initialize_multi(tmp.path(), 3).expect("initialize_multi");
        for (shard, n) in [(0u16, 100_000usize), (1, 200_000), (2, 300_000)] {
            write_bytes(&m.shard_base_path(shard), n);
            write_bytes(&m.shard_incr_path(shard), 50_000);
        }

        init(tmp.path(), APPENDFILENAME);

        assert_eq!(base_size(), 600_000);
        assert!(current_size() >= 750_000);
    }
}
