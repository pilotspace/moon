//! Proactive RSS memory watchdog with write-pause / hysteresis.
//!
//! Memory analogue of [`crate::shard::disk_monitor`] (MA12): pauses writes
//! before the kernel OOM-killer can pick this process, by tracking ACTUAL
//! process RSS against the detected system/cgroup memory limit — NOT the
//! configured `--maxmemory`, which can be an unconfigured `0` (unlimited).
//!
//! # Design
//!
//! One `Arc<MemMonitor>` is created per server (not per shard). Shard 0's
//! event loop polls RSS every 5 seconds via `poll`. All other shards share
//! the same `Arc` and call `paused()` on the hot path.
//!
//! ## Hot-path cost
//!
//! `paused()` is a single `AtomicBool::load(Relaxed)`. No syscall, no heap
//! allocation, no lock.
//!
//! ## Hysteresis — direction INVERTED vs the disk monitor
//!
//! The disk monitor pauses when free space is LOW (bad) and resumes when it
//! rises. This monitor pauses when RSS is HIGH (bad) and resumes when it
//! FALLS. Concretely:
//!
//! - Writes pause once `rss * 100 / limit >= pause_pct`.
//! - Writes resume only once `rss * 100 / limit <= pause_pct - HYSTERESIS_PCT`.
//!
//! The gap prevents flapping around the threshold as allocator arenas and
//! GC-style reclamation shrink RSS incrementally.
//!
//! ## Fixed limit, unlike the disk monitor's per-poll total
//!
//! `disk_monitor::poll` re-queries both free AND total bytes every 5s because
//! a volume can be resized. The memory limit is different: detecting it can
//! shell out (`sysctl` on macOS) or read `/proc` + `/sys/fs/cgroup` (Linux),
//! and the caller-supplied context requires this NEVER happen on the hot
//! poll path. So `limit_bytes` is resolved ONCE at [`init_global`] time and
//! held fixed for the process lifetime; only `rss_bytes` is re-sampled.
//!
//! ## Read failures
//!
//! [`crate::admin::metrics_setup::get_rss_bytes`] returns `0` both on read
//! failure AND (theoretically) on platforms with no RSS probe. Since a live
//! server process never legitimately has 0 RSS, a `0` reading is always
//! treated as "read failed — retain previous state" (mirrors
//! `disk_monitor::poll`'s `None` arm).
//!
//! ## Accepted trade-off (same as MA12)
//!
//! Read-only commands are never blocked (the write gate already handles
//! this). `DEL` / `UNLINK` / `EXPIRE` / `FLUSHALL` are write-flagged and ARE
//! blocked while paused, exactly like the diskfull guard — no allowlist in
//! this module; that would need separate policy design.
//!
//! ## Test/diagnostic override: `MOON_MEM_LIMIT_BYTES`
//!
//! [`init_global`] honors `MOON_MEM_LIMIT_BYTES` (parsed as `u64`) in place
//! of [`crate::config::detect_memory_limit_bytes`] when set. This exists so
//! integration tests can deterministically engage the watchdog without
//! controlling real process RSS or faking `/proc`/`/sys/fs/cgroup` — the same
//! rationale as the `MOON_XSHARD_*` bench-diagnostic knobs (see CLAUDE.md).
//! Not a production tuning knob: operators size the real limit via cgroups /
//! container memory requests, not this env var.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Hysteresis gap: writes resume when `rss_pct <= pause_pct - HYSTERESIS_PCT`.
const HYSTERESIS_PCT: u8 = 5;

/// Test/diagnostic override for the detected memory limit. See module docs.
const MEM_LIMIT_OVERRIDE_ENV: &str = "MOON_MEM_LIMIT_BYTES";

/// Proactive RSS memory watchdog.
///
/// Create one instance per server, wrap in `Arc`, share across shards.
///
/// ```
/// # use moon::shard::mem_monitor::MemMonitor;
/// # use std::sync::Arc;
/// let monitor = Arc::new(MemMonitor::new(95, 16 * 1024 * 1024 * 1024));
/// // Shard 0 timer: monitor.poll();
/// // Hot path: if monitor.paused() { return error; }
/// ```
pub struct MemMonitor {
    /// Latest measured RSS in bytes.
    rss_bytes: AtomicU64,
    /// True when writes should be refused (rss% >= pause_pct).
    paused: AtomicBool,
    /// RSS percentage of `limit_bytes` at which writes pause (inclusive).
    pause_pct: u8,
    /// Detected system/cgroup memory limit in bytes. Fixed for the process
    /// lifetime (resolved once by `init_global`). `0` means "undetectable" —
    /// monitoring is permanently disabled regardless of `pause_pct`.
    limit_bytes: u64,
}

impl MemMonitor {
    /// Create a monitor.
    ///
    /// * `pause_pct`   — pause writes when `rss * 100 / limit_bytes >= pause_pct`.
    /// * `limit_bytes` — detected system/cgroup memory limit; `0` disables
    ///   monitoring entirely (limit is undetectable).
    ///
    /// Starts unpaused with `rss_bytes = 0` (optimistic: no pause before the
    /// first `poll` fires — mirrors `DiskMonitor::new`'s optimistic default,
    /// just inverted: 0 bytes used is the "healthy" extreme for RSS).
    pub fn new(pause_pct: u8, limit_bytes: u64) -> Self {
        Self {
            rss_bytes: AtomicU64::new(0),
            paused: AtomicBool::new(false),
            pause_pct,
            limit_bytes,
        }
    }

    /// Latest measured RSS in bytes.
    ///
    /// Returns `0` until the first `poll` completes successfully.
    #[inline]
    pub fn rss_bytes(&self) -> u64 {
        self.rss_bytes.load(Ordering::Relaxed)
    }

    /// Returns `true` when writes should be refused.
    ///
    /// Extremely cheap: single `AtomicBool::load(Relaxed)`.
    #[inline]
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Current configured pause-percentage threshold.
    #[inline]
    pub fn pause_pct(&self) -> u8 {
        self.pause_pct
    }

    /// Detected memory limit in bytes (`0` = undetectable / disabled).
    #[inline]
    pub fn limit_bytes(&self) -> u64 {
        self.limit_bytes
    }

    /// Sample process RSS and update internal state.
    ///
    /// Called by shard 0's timer every 5 seconds. Safe to call from any
    /// shard, but must not be called on every write (it is not free).
    pub fn poll(&self) {
        let rss = crate::admin::metrics_setup::get_rss_bytes();
        if rss == 0 {
            // Read failure (or a platform with no RSS probe) — leave previous
            // state unchanged to avoid spurious pauses due to transient
            // errors. Mirrors disk_monitor::poll's `None` arm.
            tracing::warn!("mem_monitor: get_rss_bytes returned 0; retaining previous pause state");
            return;
        }
        self.rss_bytes.store(rss, Ordering::Relaxed);
        self.update_paused(rss);
        // Wire P10 INFO metrics (mirrors disk_monitor's RECL_* wiring).
        crate::command::info_reclamation::RECL_MEM_RSS_BYTES.store(rss, Ordering::Relaxed);
        crate::command::info_reclamation::RECL_MEM_WATCHDOG_ACTIVE.store(
            if self.paused.load(Ordering::Relaxed) {
                1
            } else {
                0
            },
            Ordering::Relaxed,
        );
    }

    /// Inject a known RSS reading — used by tests to bypass the syscall.
    ///
    /// Not part of the public API surface; only `pub(crate)` for unit tests.
    #[cfg(test)]
    pub fn inject(&self, rss: u64) {
        self.rss_bytes.store(rss, Ordering::Relaxed);
        self.update_paused(rss);
    }

    /// Core hysteresis state machine. Must only be called from `poll`/`inject`.
    ///
    /// Direction is INVERTED vs `DiskMonitor::update_paused`: high RSS is
    /// bad, so the enter-pause comparison is `>=` (inclusive at the
    /// threshold — unlike the disk monitor's strict `<`), and the resume
    /// comparison is `<=` a LOWER threshold.
    fn update_paused(&self, rss: u64) {
        if self.limit_bytes == 0 || self.pause_pct == 0 {
            // Undetectable limit or explicitly disabled — never pause.
            return;
        }
        // Clamp before the u64->u8 cast: RSS can exceed limit_bytes (a
        // stale/under-detected cgroup limit, or a transient spike right
        // before the kernel OOM-killer would act), and an unclamped
        // percentage > 255 would truncate-wrap through `as u8` into a
        // small, misleadingly "healthy" value.
        let rss_pct = (rss.saturating_mul(100) / self.limit_bytes).min(255) as u8;
        let currently_paused = self.paused.load(Ordering::Relaxed);

        if !currently_paused && rss_pct >= self.pause_pct {
            // Enter paused state
            self.paused.store(true, Ordering::Release);
            tracing::warn!(
                rss_pct,
                pause_pct = self.pause_pct,
                rss_bytes = rss,
                limit_bytes = self.limit_bytes,
                "mem_monitor: write stall ENGAGED — RSS approaching memory limit",
            );
        } else if currently_paused {
            let resume_pct = self.pause_pct.saturating_sub(HYSTERESIS_PCT);
            if rss_pct <= resume_pct {
                self.paused.store(false, Ordering::Release);
                tracing::info!(
                    rss_pct,
                    resume_pct,
                    "mem_monitor: write stall CLEARED — RSS pressure recovered",
                );
            }
        }
    }
}

// ── Process-global singleton ────────────────────────────────────────────────
//
// One `Arc<MemMonitor>` per server process, initialised once at startup by
// `init_global`. All shards read `GLOBAL_MEM_MONITOR` on the hot path; shard 0
// calls `poll_global` every 5 seconds from its timer (same tick as MA12).

static GLOBAL_MEM_MONITOR: OnceLock<Arc<MemMonitor>> = OnceLock::new();

/// Initialise the process-global memory monitor.
///
/// Must be called once at server startup (before any shard event loops
/// start). Calling it more than once is a no-op — the first call wins.
///
/// Resolves `limit_bytes` from `MOON_MEM_LIMIT_BYTES` (test/diagnostic
/// override, see module docs) if set, else
/// [`crate::config::detect_memory_limit_bytes`] (ONE call — this may shell
/// out on macOS, so it must never run on the per-poll path).
///
/// Runs one immediate `poll()` before publishing the monitor so the startup
/// recovery peak (AOF replay + segment load) is visible to the guard before
/// the first 5s timer tick.
///
/// * `pause_pct` — from `ServerConfig::mem_full_pct`. `0` disables the guard.
pub fn init_global(pause_pct: u8) {
    let limit_bytes = std::env::var(MEM_LIMIT_OVERRIDE_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| crate::config::detect_memory_limit_bytes().map(|v| v as u64))
        .unwrap_or(0);
    let monitor = Arc::new(MemMonitor::new(pause_pct, limit_bytes));
    if pause_pct > 0 {
        monitor.poll();
    }
    // Ignore the error: if already set, the existing instance remains.
    let _ = GLOBAL_MEM_MONITOR.set(monitor);
}

/// Poll the global monitor (shard 0 calls this every 5 seconds).
///
/// No-op if `init_global` has not been called or if `pause_pct == 0`
/// (monitoring disabled).
pub fn poll_global() {
    if let Some(m) = GLOBAL_MEM_MONITOR.get() {
        if m.pause_pct > 0 {
            m.poll();
        }
    }
}

/// Returns `true` if writes should be refused due to memory pressure.
///
/// **Hot-path function.** Single `AtomicBool::load(Relaxed)` when monitoring
/// is active; returns `false` immediately when monitoring is disabled or the
/// global has not been initialised.
#[inline]
pub fn is_write_paused() -> bool {
    match GLOBAL_MEM_MONITOR.get() {
        Some(m) => m.paused(),
        None => false,
    }
}

/// Returns the latest measured RSS in bytes (for INFO output).
///
/// Returns `0` when monitoring is disabled or not yet initialised.
#[inline]
pub fn global_rss_bytes() -> u64 {
    match GLOBAL_MEM_MONITOR.get() {
        Some(m) => m.rss_bytes(),
        None => 0,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper that creates a monitor and injects an RSS reading via the test
    /// backdoor, then returns the monitor so callers can assert state.
    fn monitor_with(pause_pct: u8, rss: u64, limit: u64) -> MemMonitor {
        let m = MemMonitor::new(pause_pct, limit);
        m.inject(rss);
        m
    }

    // ── Pause threshold (INVERTED vs disk: high RSS is bad) ────────────────

    #[test]
    fn test_paused_when_above_threshold() {
        // 60% used, threshold 50% → should pause
        let m = monitor_with(50, 60, 100);
        assert!(m.paused(), "should be paused when rss% >= pause_pct");
    }

    #[test]
    fn test_not_paused_when_below_threshold() {
        // 40% used, threshold 50% → should NOT pause
        let m = monitor_with(50, 40, 100);
        assert!(!m.paused(), "should not be paused when rss% < pause_pct");
    }

    #[test]
    fn test_paused_at_exact_threshold() {
        // Exactly 50% used, threshold 50% → INVERTED vs disk: >= is inclusive,
        // so this SHOULD pause (disk's exact-threshold case does NOT pause).
        let m = monitor_with(50, 50, 100);
        assert!(
            m.paused(),
            "rss% == pause_pct MUST pause (>= is inclusive, inverted vs disk's < )"
        );
    }

    #[test]
    fn test_rss_bytes_accessor() {
        let m = monitor_with(50, 42_000_000, 1_000_000_000);
        assert_eq!(m.rss_bytes(), 42_000_000);
    }

    // ── Hysteresis ─────────────────────────────────────────────────────────

    #[test]
    fn test_hysteresis_no_resume_at_pause_threshold() {
        let m = MemMonitor::new(50, 100);
        // Enter paused state: 60% used
        m.inject(60);
        assert!(m.paused(), "should be paused at 60%");

        // Drop to exactly pause_pct (50%) — NOT below pause_pct - HYSTERESIS (45%)
        m.inject(50);
        assert!(
            m.paused(),
            "should remain paused at exactly pause_pct (hysteresis prevents premature resume)"
        );
    }

    #[test]
    fn test_hysteresis_resume_below_resume_threshold() {
        let m = MemMonitor::new(50, 100);
        // Enter paused state
        m.inject(60);
        assert!(m.paused(), "should be paused at 60%");

        // Drop to pause_pct - HYSTERESIS (45%) — should resume
        let resume_pct = 50u64 - HYSTERESIS_PCT as u64;
        m.inject(resume_pct);
        assert!(
            !m.paused(),
            "should resume when rss% <= pause_pct - HYSTERESIS_PCT"
        );
    }

    #[test]
    fn test_hysteresis_flap_prevention() {
        let m = MemMonitor::new(50, 100);

        // Enter paused
        m.inject(70);
        assert!(m.paused());

        // Yo-yo between 46% and 49% — stays paused because 46 > resume(45)
        m.inject(46);
        assert!(m.paused(), "46% > 45% resume threshold, should stay paused");
        m.inject(49);
        assert!(m.paused());
        m.inject(46);
        assert!(m.paused(), "still paused — never crossed 45%");

        // Cross 45% — should resume
        m.inject(45);
        assert!(!m.paused(), "45% <= 45% resume threshold, should clear");
    }

    #[test]
    fn test_no_flap_after_resume() {
        let m = MemMonitor::new(50, 100);

        // Normal operation: rss is healthy
        m.inject(10);
        assert!(!m.paused());

        // Rise above threshold
        m.inject(70);
        assert!(m.paused());

        // Recover past hysteresis
        m.inject(30);
        assert!(!m.paused());

        // Another rise to pause_pct — should pause again
        m.inject(55);
        assert!(
            m.paused(),
            "re-enter paused on another rise above threshold"
        );
    }

    // ── Edge cases ─────────────────────────────────────────────────────────

    #[test]
    fn test_zero_limit_disabled_does_not_panic() {
        let m = MemMonitor::new(50, 0);
        // inject a huge rss against a zero limit — should not panic (no
        // division-by-zero), and monitoring must stay disabled.
        m.inject(u64::MAX);
        assert!(
            !m.paused(),
            "limit_bytes == 0 (undetectable) must permanently disable the guard"
        );
    }

    #[test]
    fn test_pause_pct_zero_disabled() {
        let m = MemMonitor::new(0, 100);
        // Even at 100% RSS usage, pause_pct == 0 means "disabled".
        m.inject(100);
        assert!(!m.paused(), "pause_pct == 0 must disable the guard");
    }

    #[test]
    fn test_rss_exceeding_limit_does_not_wrap_u8() {
        // rss is 10x the limit → naive `as u8` on an unclamped percentage
        // (1000) would truncate-wrap to 1000 % 256 = 232, which happens to
        // still be >= most thresholds, but at other multiples the wrap can
        // land BELOW pause_pct and falsely clear the guard. Pin the case
        // that would silently regress if the `.min(255)` clamp is removed.
        let m = MemMonitor::new(50, 100);
        m.inject(2560); // 2560*100/100 = 2560 -> wraps to 0 without clamping
        assert!(
            m.paused(),
            "rss far exceeding limit_bytes must still register as paused, \
             not wrap through u8 truncation into a false 'healthy' reading"
        );
    }

    #[test]
    fn test_large_memory_host() {
        // 64 GiB limit, 40 GiB RSS = 62.5% → above a 50% threshold.
        let limit: u64 = 64 * 1024 * 1024 * 1024;
        let rss: u64 = 40 * 1024 * 1024 * 1024;
        let m = monitor_with(50, rss, limit);
        assert!(
            m.paused(),
            "62.5% used on a large host should trigger pause"
        );
    }

    #[test]
    fn test_initial_state_optimistic() {
        // Before first poll, monitor should be unpaused (optimistic default).
        let m = MemMonitor::new(50, 100);
        assert!(!m.paused(), "initial state must be unpaused");
        assert_eq!(m.rss_bytes(), 0, "initial rss_bytes must be 0");
    }

    /// Smoke test: poll() on the real process must not panic and must update
    /// rss_bytes from the 0 sentinel to some positive value (a live server
    /// process never legitimately has 0 RSS).
    #[test]
    fn test_poll_real_process_smoke() {
        let m = MemMonitor::new(50, u64::MAX);
        m.poll();
        assert!(
            m.rss_bytes() > 0,
            "real get_rss_bytes() must update rss_bytes on a live process"
        );
    }

    #[test]
    fn test_arc_send_sync() {
        // MemMonitor must be Send + Sync for cross-shard sharing via Arc.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MemMonitor>();
    }
}
