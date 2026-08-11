//! Adaptive busy-poll contention governor (digest O3).
//!
//! `--io-busy-poll-us` is a measured p=1 win ONLY on an effectively-dedicated
//! core (GCE pinned: ARM 0.95→1.21×, x86 1.06→1.66× vs Redis); on a shared
//! core the burned spin budget steals time from whatever is contending and
//! the win inverts (documented OrbStack/laptop regression). That made the
//! flag operator-judgment-only. This governor removes the judgment call: each
//! shard thread watches its OWN involuntary-preemption rate
//! (`nonvoluntary_ctxt_switches` in `/proc/thread-self/status` — the direct
//! kernel signal for "someone else needed this core") and flips the vendored
//! driver's per-thread contention gate (`monoio::set_legacy_spin_contended`).
//!
//! Policy (hysteresis, asymmetric on purpose):
//! - ONE window over the preemption threshold → gate the spin immediately
//!   (contended cores stop burning budget within a window).
//! - [`CLEAN_WINDOWS_TO_ENABLE`] consecutive clean windows → ungate (slow
//!   re-enable so a briefly-quiet interloper doesn't cause flapping).
//! - Start state: NOT contended — identical to pre-O3 behavior until
//!   evidence arrives, so pinned-core benchmarks see the win from request
//!   one and a shared-core host pays at most ~one window of spin.
//!
//! The governor ticks from the shard's 1s chore cadence (a multiple of the
//! 10ms stretched idle tick, per the #373 divisor rule). Windows are
//! measured with real elapsed time, so idle-park stretching just lengthens
//! a window rather than skewing the rate.
//!
//! Idle cost is NOT this module's job: the driver's idle-disengage
//! (`MOON_SPIN_IDLE_DISENGAGE_US`, default 10ms) already stops spinning on
//! quiet threads.
//!
//! Knobs (diagnostics; not for production tuning):
//! - `MOON_SPIN_ADAPTIVE=0` — disable the governor entirely (always-spin,
//!   exact pre-O3 behavior; the same-binary A/B knob).
//! - `MOON_SPIN_MAX_PREEMPTS_PER_SEC` — threshold, default 25/s. A pinned
//!   thread alone on its core measures ~0-5/s (timer/IPI noise); sharing
//!   with one busy thread measures hundreds/s under CFS.
//!
//! Non-Linux: `/proc` does not exist → [`SpinGovernor::tick`] returns `None`
//! forever and the gate is never touched (pre-O3 behavior).

use std::time::Instant;

/// Consecutive clean 1s windows required to re-enable spinning.
const CLEAN_WINDOWS_TO_ENABLE: u32 = 5;

/// Default involuntary-preemption threshold (per second).
const DEFAULT_MAX_PREEMPTS_PER_SEC: f64 = 25.0;

/// Whether the adaptive governor is enabled (`MOON_SPIN_ADAPTIVE` != "0").
pub fn adaptive_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("MOON_SPIN_ADAPTIVE").is_none_or(|v| v != "0"))
}

fn max_preempts_per_sec() -> f64 {
    static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MOON_SPIN_MAX_PREEMPTS_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| *v >= 0.0)
            .unwrap_or(DEFAULT_MAX_PREEMPTS_PER_SEC)
    })
}

/// Read the calling thread's involuntary context-switch counter.
#[cfg(target_os = "linux")]
fn read_nonvoluntary_switches() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/thread-self/status").ok()?;
    parse_nonvoluntary(&status)
}

#[cfg(not(target_os = "linux"))]
fn read_nonvoluntary_switches() -> Option<u64> {
    None
}

/// Parse `nonvoluntary_ctxt_switches:\t<N>` out of a /proc status blob.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))] // non-Linux lib target: only tests call it
fn parse_nonvoluntary(status: &str) -> Option<u64> {
    status
        .lines()
        .find(|l| l.starts_with("nonvoluntary_ctxt_switches:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
}

/// Pure hysteresis decision: `Some(new_contended)` when the state flips.
fn decide(
    contended: bool,
    clean_windows: &mut u32,
    preempts_per_sec: f64,
    threshold: f64,
) -> Option<bool> {
    if preempts_per_sec > threshold {
        *clean_windows = 0;
        if !contended {
            return Some(true); // fast off: one bad window gates the spin
        }
        return None;
    }
    if contended {
        *clean_windows += 1;
        if *clean_windows >= CLEAN_WINDOWS_TO_ENABLE {
            *clean_windows = 0;
            return Some(false); // slow on: sustained quiet re-enables
        }
    }
    None
}

/// Per-shard-thread governor state. Construct once per event loop; call
/// [`tick`](Self::tick) from the 1s chore.
pub struct SpinGovernor {
    last_count: Option<u64>,
    last_read: Instant,
    clean_windows: u32,
    contended: bool,
}

impl SpinGovernor {
    pub fn new() -> Self {
        Self {
            last_count: None,
            last_read: Instant::now(),
            clean_windows: 0,
            contended: false,
        }
    }

    /// Sample the preemption counter and run the hysteresis. Returns
    /// `Some(contended)` when the gate should flip (caller pushes it to the
    /// driver); `None` on no change or when the signal is unavailable.
    pub fn tick(&mut self) -> Option<bool> {
        if !adaptive_enabled() {
            return None;
        }
        let now = Instant::now();
        let count = read_nonvoluntary_switches()?;
        let Some(prev) = self.last_count else {
            self.last_count = Some(count); // first sample: baseline only
            self.last_read = now;
            return None;
        };
        let elapsed = now.duration_since(self.last_read).as_secs_f64();
        if elapsed <= 0.05 {
            // Degenerate window: keep the previous baseline so the preemptions
            // in this span fold into the next window instead of being dropped.
            return None;
        }
        self.last_count = Some(count);
        self.last_read = now;
        let rate = count.saturating_sub(prev) as f64 / elapsed;
        let flip = decide(
            self.contended,
            &mut self.clean_windows,
            rate,
            max_preempts_per_sec(),
        );
        if let Some(c) = flip {
            self.contended = c;
        }
        flip
    }

    /// Current gate state.
    #[cfg(test)]
    fn contended(&self) -> bool {
        self.contended
    }
}

impl Default for SpinGovernor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_nonvoluntary_extracts_counter() {
        let blob =
            "Name:\tmoon\nvoluntary_ctxt_switches:\t123\nnonvoluntary_ctxt_switches:\t4567\n";
        assert_eq!(parse_nonvoluntary(blob), Some(4567));
        assert_eq!(parse_nonvoluntary("Name:\tmoon\n"), None);
        assert_eq!(parse_nonvoluntary(""), None);
    }

    #[test]
    fn decide_one_bad_window_gates_immediately() {
        let mut clean = 0;
        assert_eq!(decide(false, &mut clean, 100.0, 25.0), Some(true));
    }

    #[test]
    fn decide_clean_windows_below_threshold_do_not_flip_uncontended() {
        let mut clean = 0;
        for _ in 0..10 {
            assert_eq!(decide(false, &mut clean, 1.0, 25.0), None);
        }
    }

    #[test]
    fn decide_reenables_only_after_sustained_quiet() {
        let mut clean = 0;
        // 4 clean windows: still gated.
        for _ in 0..CLEAN_WINDOWS_TO_ENABLE - 1 {
            assert_eq!(decide(true, &mut clean, 1.0, 25.0), None);
        }
        // 5th clean window: ungate.
        assert_eq!(decide(true, &mut clean, 1.0, 25.0), Some(false));
        assert_eq!(clean, 0, "counter resets after re-enable");
    }

    #[test]
    fn decide_bad_window_resets_clean_streak() {
        let mut clean = 0;
        for _ in 0..3 {
            assert_eq!(decide(true, &mut clean, 1.0, 25.0), None);
        }
        assert_eq!(clean, 3);
        // Interloper returns: streak resets, stays contended (no flip event).
        assert_eq!(decide(true, &mut clean, 200.0, 25.0), None);
        assert_eq!(clean, 0);
        // Needs the full quiet run again.
        for _ in 0..CLEAN_WINDOWS_TO_ENABLE - 1 {
            assert_eq!(decide(true, &mut clean, 1.0, 25.0), None);
        }
        assert_eq!(decide(true, &mut clean, 1.0, 25.0), Some(false));
    }

    #[test]
    fn decide_boundary_rate_is_clean() {
        let mut clean = 0;
        // Exactly at threshold: not "over" — clean.
        assert_eq!(decide(false, &mut clean, 25.0, 25.0), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn read_nonvoluntary_switches_works_on_linux() {
        assert!(read_nonvoluntary_switches().is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn governor_sub_window_tick_preserves_baseline() {
        let mut g = SpinGovernor::new();
        assert_eq!(g.tick(), None); // baseline sample
        let baseline_count = g.last_count;
        let baseline_read = g.last_read;
        // A second tick <50ms later is a degenerate window: it must NOT
        // advance the baseline, so the skipped span's preemptions fold into
        // the next full window instead of being silently discarded.
        assert_eq!(g.tick(), None);
        assert_eq!(g.last_count, baseline_count);
        assert_eq!(g.last_read, baseline_read);
    }

    #[test]
    fn governor_first_tick_is_baseline_only() {
        let mut g = SpinGovernor::new();
        // First tick can never flip (no previous sample). On non-Linux this
        // is None for lack of signal; on Linux for lack of a baseline.
        assert_eq!(g.tick(), None);
        assert!(!g.contended());
    }
}
