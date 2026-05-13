//! Disk free-space monitor with write-pause / hysteresis.
//!
//! # Design
//!
//! One `Arc<DiskMonitor>` is created per server (not per shard). Shard 0's event
//! loop polls the filesystem every 5 seconds via `poll`. All other shards share the
//! same `Arc` and call `paused()` / `free_bytes()` on the hot path.
//!
//! ## Hot-path cost
//!
//! `paused()` is a single `AtomicBool::load(Relaxed)`. No syscall, no heap
//! allocation, no lock.
//!
//! ## Hysteresis
//!
//! Writes are paused when free space < `min_pct`. They resume only when free space
//! rises above `min_pct + 5` percentage points. This prevents flapping around the
//! threshold when compaction reclaims space incrementally.
//!
//! ## Platform support
//!
//! - **Linux** and **macOS**: both use `libc::statvfs` (POSIX; available on both).
//!   `nix` crate only has the `net` + `socket` features enabled, so we call `libc`
//!   directly — no extra dependencies required.
//! - **Other Unix** (BSDs, etc.): also uses `libc::statvfs`.
//! - **Non-Unix** (compile-time stubs): `poll` is a no-op, `free_bytes` returns `u64::MAX`.

use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Hysteresis gap: writes resume when free > `min_pct + HYSTERESIS_PCT`.
const HYSTERESIS_PCT: u8 = 5;

/// Disk free-space monitor.
///
/// Create one instance per server, wrap in `Arc`, share across shards.
///
/// ```
/// # use moon::shard::disk_monitor::DiskMonitor;
/// # use std::sync::Arc;
/// let monitor = Arc::new(DiskMonitor::new(5, "/"));
/// // Shard 0 timer: monitor.poll();
/// // Hot path: if monitor.paused() { return error; }
/// ```
pub struct DiskMonitor {
    /// Latest measured free bytes on the monitored volume.
    free_bytes: AtomicU64,
    /// True when writes should be refused (free < min_pct).
    paused: AtomicBool,
    /// Minimum free-space percentage before writes are paused (inclusive).
    min_pct: u8,
    /// Path whose volume is monitored (typically the WAL / data directory).
    monitored_path: Box<Path>,
}

impl DiskMonitor {
    /// Create a monitor.
    ///
    /// * `min_pct` — pause writes when free % drops below this.
    /// * `path`    — any path on the volume to monitor (typically `persistence_dir`).
    ///
    /// Starts unpaused with `free_bytes = u64::MAX` (optimistic: no pause before
    /// the first `poll` fires).
    pub fn new(min_pct: u8, path: impl AsRef<Path>) -> Self {
        Self {
            free_bytes: AtomicU64::new(u64::MAX),
            paused: AtomicBool::new(false),
            min_pct,
            monitored_path: path.as_ref().into(),
        }
    }

    /// Latest free bytes on the monitored volume.
    ///
    /// Returns `u64::MAX` until the first `poll` completes successfully.
    #[inline]
    pub fn free_bytes(&self) -> u64 {
        self.free_bytes.load(Ordering::Relaxed)
    }

    /// Returns `true` when writes should be refused.
    ///
    /// Extremely cheap: single `AtomicBool::load(Relaxed)`.
    #[inline]
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Current configured minimum free-space percentage.
    #[inline]
    pub fn min_pct(&self) -> u8 {
        self.min_pct
    }

    /// Sample filesystem free space and update internal state.
    ///
    /// Called by shard 0's timer every 5 seconds. Safe to call from any shard,
    /// but must not be called on every write (it is not free).
    pub fn poll(&self) {
        match query_free_bytes(&self.monitored_path) {
            Some((free, total)) => {
                self.free_bytes.store(free, Ordering::Relaxed);
                self.update_paused(free, total);
                // Wire P10 INFO metrics (MA12 → RECL_*).
                crate::command::info_reclamation::RECL_DISK_FREE_BYTES
                    .store(free, Ordering::Relaxed);
                crate::command::info_reclamation::RECL_WRITE_STALL_ACTIVE.store(
                    if self.paused.load(Ordering::Relaxed) {
                        1
                    } else {
                        0
                    },
                    Ordering::Relaxed,
                );
            }
            None => {
                // Cannot read filesystem info — leave previous state unchanged
                // to avoid spurious pauses due to transient errors.
                tracing::warn!(
                    path = %self.monitored_path.display(),
                    "disk_monitor: statvfs failed; retaining previous pause state",
                );
            }
        }
    }

    /// Inject a known (free, total) pair — used by tests to bypass the syscall.
    ///
    /// Not part of the public API surface; only `pub(crate)` for unit tests.
    #[cfg(test)]
    pub fn inject(&self, free: u64, total: u64) {
        self.free_bytes.store(free, Ordering::Relaxed);
        self.update_paused(free, total);
    }

    /// Core hysteresis state machine. Must only be called from `poll`/`inject`.
    fn update_paused(&self, free: u64, total: u64) {
        if total == 0 {
            return;
        }
        // Compute free percentage using integer arithmetic to avoid fp issues.
        // Multiply by 100 first to keep precision for small totals.
        let free_pct = (free.saturating_mul(100) / total) as u8;
        let currently_paused = self.paused.load(Ordering::Relaxed);

        if !currently_paused && free_pct < self.min_pct {
            // Enter paused state
            self.paused.store(true, Ordering::Release);
            tracing::warn!(
                free_pct,
                min_pct = self.min_pct,
                free_bytes = free,
                "disk_monitor: write stall ENGAGED — disk nearly full",
            );
        } else if currently_paused {
            let resume_pct = self.min_pct.saturating_add(HYSTERESIS_PCT);
            if free_pct >= resume_pct {
                self.paused.store(false, Ordering::Release);
                tracing::info!(
                    free_pct,
                    resume_pct,
                    "disk_monitor: write stall CLEARED — free space recovered",
                );
            }
        }
    }
}

// ── Process-global singleton ────────────────────────────────────────────────
//
// One `Arc<DiskMonitor>` per server process, initialised once at startup by
// `init_global`. All shards read `GLOBAL_DISK_MONITOR` on the hot path; shard 0
// calls `poll_global` every 5 seconds from its timer.
//
// Using a global avoids threading `Arc<DiskMonitor>` through `ConnectionContext`
// (4 call sites in conn_accept.rs) and keeps connection-level code unchanged.

static GLOBAL_DISK_MONITOR: OnceLock<Arc<DiskMonitor>> = OnceLock::new();

/// Initialise the process-global disk monitor.
///
/// Must be called once at server startup (before any shard event loops start).
/// Calling it more than once is a no-op — the first call wins.
///
/// * `min_pct` — from `ServerConfig::disk_free_min_pct`.
/// * `path`    — the WAL / persistence directory (or `"."` if none configured).
pub fn init_global(min_pct: u8, path: impl AsRef<Path>) {
    let monitor = Arc::new(DiskMonitor::new(min_pct, path));
    // Ignore the error: if already set, the existing instance remains.
    let _ = GLOBAL_DISK_MONITOR.set(monitor);
}

/// Return a clone of the global `Arc<DiskMonitor>`.
///
/// Returns `None` if `init_global` has not been called yet (should not happen
/// in production; tests that don't call `init_global` use direct `DiskMonitor`
/// instances instead).
pub fn global() -> Option<Arc<DiskMonitor>> {
    GLOBAL_DISK_MONITOR.get().cloned()
}

/// Poll the global monitor (shard 0 calls this every 5 seconds).
///
/// No-op if `init_global` has not been called or if `min_pct == 0`
/// (monitoring disabled).
pub fn poll_global() {
    if let Some(m) = GLOBAL_DISK_MONITOR.get() {
        if m.min_pct > 0 {
            m.poll();
        }
    }
}

/// Returns `true` if writes should be refused due to low disk space.
///
/// **Hot-path function.** Single `AtomicBool::load(Relaxed)` when monitoring
/// is active; returns `false` immediately when monitoring is disabled or the
/// global has not been initialised.
#[inline]
pub fn is_write_paused() -> bool {
    match GLOBAL_DISK_MONITOR.get() {
        Some(m) => m.paused(),
        None => false,
    }
}

/// Returns the latest free bytes on the monitored volume (for INFO output).
///
/// Returns `u64::MAX` when monitoring is disabled or not yet initialised.
#[inline]
pub fn global_free_bytes() -> u64 {
    match GLOBAL_DISK_MONITOR.get() {
        Some(m) => m.free_bytes(),
        None => u64::MAX,
    }
}

// ── Platform-specific statvfs wrapper ──────────────────────────────────────

/// Returns `Some((free_bytes, total_bytes))` for the volume containing `path`.
/// Returns `None` on any error.
#[cfg(unix)]
fn query_free_bytes(path: &Path) -> Option<(u64, u64)> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    // Convert the path to a C string.  Paths with interior NULs are pathological
    // — treat them as unreadable.
    let c_path = CString::new(path.as_os_str().as_encoded_bytes()).ok()?;

    // SAFETY: `statvfs` is a POSIX C function. We pass a properly terminated C
    // string and a properly sized output buffer. `MaybeUninit` avoids reading
    // uninitialised memory before the syscall writes to it. The return value is
    // checked before the struct is accessed.
    let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: statvfs returned 0 (success), so the struct is fully initialised.
    let stat = unsafe { stat.assume_init() };

    // `f_bavail` is blocks available to unprivileged users (conservative).
    // `f_blocks` is total usable blocks (excluding FS reserved blocks).
    // Both multiplied by `f_bsize` (or `f_frsize` on some systems — `f_frsize`
    // is the fundamental block size; use it when non-zero, else fall back to
    // `f_bsize`).
    let bsize = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };

    let free = stat.f_bavail as u64 * bsize;
    let total = stat.f_blocks as u64 * bsize;
    Some((free, total))
}

/// Non-Unix stub: monitoring is a no-op, writes are never paused by disk.
#[cfg(not(unix))]
fn query_free_bytes(_path: &Path) -> Option<(u64, u64)> {
    Some((u64::MAX, u64::MAX))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper that creates a monitor and injects (free, total) via the test
    /// backdoor, then returns the monitor so callers can assert state.
    fn monitor_with(min_pct: u8, free: u64, total: u64) -> DiskMonitor {
        let m = DiskMonitor::new(min_pct, "/tmp");
        m.inject(free, total);
        m
    }

    // ── Rejection threshold ────────────────────────────────────────────────

    #[test]
    fn test_paused_when_below_threshold() {
        // 4% free, threshold 5% → should pause
        let m = monitor_with(5, 4, 100);
        assert!(m.paused(), "should be paused when free < min_pct");
    }

    #[test]
    fn test_not_paused_when_above_threshold() {
        // 6% free, threshold 5% → should NOT pause
        let m = monitor_with(5, 6, 100);
        assert!(!m.paused(), "should not be paused when free > min_pct");
    }

    #[test]
    fn test_not_paused_at_exact_threshold() {
        // Exactly 5% free, threshold 5% → strictly less than; should NOT pause
        let m = monitor_with(5, 5, 100);
        assert!(
            !m.paused(),
            "free == min_pct should not pause (< is strict)"
        );
    }

    #[test]
    fn test_free_bytes_accessor() {
        let m = monitor_with(5, 42_000_000, 1_000_000_000);
        assert_eq!(m.free_bytes(), 42_000_000);
    }

    // ── Hysteresis ─────────────────────────────────────────────────────────

    #[test]
    fn test_hysteresis_no_resume_at_threshold() {
        let m = DiskMonitor::new(5, "/tmp");
        // Enter paused state: 2% free
        m.inject(2, 100);
        assert!(m.paused(), "should be paused at 2%");

        // Recover to exactly min_pct (5%) — NOT above min_pct + HYSTERESIS (10%)
        m.inject(5, 100);
        assert!(
            m.paused(),
            "should remain paused at exactly min_pct (hysteresis prevents premature resume)"
        );
    }

    #[test]
    fn test_hysteresis_resume_above_resume_threshold() {
        let m = DiskMonitor::new(5, "/tmp");
        // Enter paused state
        m.inject(2, 100);
        assert!(m.paused(), "should be paused at 2%");

        // Recover to min_pct + HYSTERESIS (10%) — should resume
        let resume_pct = 5u64 + HYSTERESIS_PCT as u64;
        m.inject(resume_pct, 100);
        assert!(
            !m.paused(),
            "should resume when free >= min_pct + HYSTERESIS_PCT"
        );
    }

    #[test]
    fn test_hysteresis_flap_prevention() {
        let m = DiskMonitor::new(5, "/tmp");

        // Enter paused
        m.inject(3, 100);
        assert!(m.paused());

        // Yo-yo between 4% and 9% — stays paused because 9 < min_pct(5) + 5
        m.inject(9, 100);
        assert!(m.paused(), "9% < 10% resume threshold, should stay paused");
        m.inject(4, 100);
        assert!(m.paused());
        m.inject(9, 100);
        assert!(m.paused(), "still paused — never crossed 10%");

        // Cross 10% — should resume
        m.inject(10, 100);
        assert!(!m.paused(), "10% >= 10% resume threshold, should clear");
    }

    #[test]
    fn test_no_flap_after_resume() {
        let m = DiskMonitor::new(5, "/tmp");

        // Normal operation: free is healthy
        m.inject(50, 100);
        assert!(!m.paused());

        // Drop below threshold
        m.inject(3, 100);
        assert!(m.paused());

        // Recover past hysteresis
        m.inject(11, 100);
        assert!(!m.paused());

        // Another small dip to min_pct — should pause again
        m.inject(4, 100);
        assert!(m.paused(), "re-enter paused on another dip below threshold");
    }

    // ── Edge cases ─────────────────────────────────────────────────────────

    #[test]
    fn test_zero_total_does_not_panic() {
        let m = DiskMonitor::new(5, "/tmp");
        // inject a zero total — should not panic, state should remain unpaused
        m.inject(0, 0);
        assert!(
            !m.paused(),
            "zero-total volume must not cause panic or false pause"
        );
    }

    #[test]
    fn test_large_volume() {
        // 4 TB total, 100 GB free = ~2.4% → below 5% threshold
        let total: u64 = 4 * 1024 * 1024 * 1024 * 1024;
        let free: u64 = 100 * 1024 * 1024 * 1024;
        let m = monitor_with(5, free, total);
        assert!(m.paused(), "2.4% free on large volume should trigger pause");
    }

    #[test]
    fn test_initial_state_optimistic() {
        // Before first poll, monitor should be unpaused (optimistic default).
        let m = DiskMonitor::new(5, "/tmp");
        assert!(!m.paused(), "initial state must be unpaused");
        assert_eq!(m.free_bytes(), u64::MAX, "initial free_bytes must be MAX");
    }

    /// Smoke test: poll() on the real "/" path must not panic and must update
    /// free_bytes from the u64::MAX sentinel to some smaller value.
    #[test]
    fn test_poll_real_path_smoke() {
        let m = DiskMonitor::new(5, "/");
        m.poll();
        // After a real poll, free_bytes should no longer be MAX (unless the
        // machine genuinely has > 16 exabytes free, which is implausible).
        #[cfg(unix)]
        assert!(
            m.free_bytes() < u64::MAX,
            "real statvfs must update free_bytes"
        );
    }

    #[test]
    fn test_arc_send_sync() {
        // DiskMonitor must be Send + Sync for cross-shard sharing via Arc.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DiskMonitor>();
    }
}
