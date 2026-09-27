//! Elapsed-time cadences for the shard's periodic chores (moon#1280).
//!
//! The shard loop runs its chores off one 1 ms periodic tick. On monoio they
//! used to be COUNT based — `monoio_tick_counter % N == 0` — which is only
//! "every N ms" while every tick is delivered. Once the tick skips missed
//! deadlines instead of bursting them ([`crate::runtime::interval`]), a count
//! falls behind wall time by every stall, and under a loop that keeps running
//! more than 5 ms late (the runtimes' catch-up grace) a "1 s" chore — WAL
//! fsync, idle-client timeout, MVCC sweep — would stretch to many seconds.
//! A [`Cadence`] is due by ELAPSED time on a monotonic clock instead, so a
//! stall costs a chore at most one catch-up run, and a loaded loop can only
//! make a chore late by one loop iteration, never slow its rate.
//!
//! Duties that owe work proportional to elapsed time (active expiry: keys
//! kept expiring during the stall) scale that one catch-up run with
//! [`catch_up_scale`], under a hard cap, so a skipped tick loses granularity,
//! never correctness — and the one catch-up run stays bounded.
//!
//! [`TickLateness`] counts the ticks that fired late, and the largest number
//! of ticks one stall fired back to back (INFO `shard_tick_late_total` /
//! `shard_tick_burst_max`). Before moon#1280 a 3 s stall showed up as a burst
//! of ~3,000; with the Skip policy it is one tick per stall.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A periodic chore, due by elapsed time since the loop started.
///
/// Times are milliseconds on a monotonic clock ([`LoopClock`]). The first run
/// is due one full period after creation — the same as the counter-based
/// chores it replaces, which never fired at tick 0 (the tokio intervals do
/// fire at t=0; that divergence is deliberate and unchanged here).
#[derive(Debug, Clone)]
// The tokio loop only books runs its own intervals decided (`mark_run`);
// `poll` — and the schedule it keeps — drives the monoio loop's chores.
#[cfg_attr(not(feature = "runtime-monoio"), allow(dead_code))]
pub(crate) struct Cadence {
    period_ms: u64,
    next_due_ms: u64,
    last_run_ms: u64,
}

impl Cadence {
    pub(crate) fn new(now_ms: u64, period_ms: u64) -> Self {
        let period_ms = period_ms.max(1);
        Self {
            period_ms,
            next_due_ms: now_ms.saturating_add(period_ms),
            last_run_ms: now_ms,
        }
    }

    /// `Some(ms since this chore's previous run)` when the chore is due at
    /// `now_ms` (and books the run), `None` otherwise.
    ///
    /// Scheduling: a run on time, or late by less than one period, keeps the
    /// fixed grid (`next = due + period`, no drift under jitter). A run late
    /// by a period or more — the chore was skipped over by a stall — re-anchors
    /// to `now + period`: ONE catch-up run, never a burst of them.
    #[inline]
    #[cfg_attr(not(feature = "runtime-monoio"), allow(dead_code))]
    pub(crate) fn poll(&mut self, now_ms: u64) -> Option<u64> {
        if now_ms < self.next_due_ms {
            return None;
        }
        let late = now_ms - self.next_due_ms;
        self.next_due_ms = if late >= self.period_ms {
            now_ms.saturating_add(self.period_ms)
        } else {
            self.next_due_ms + self.period_ms
        };
        Some(self.mark_run(now_ms))
    }

    /// Book a run at `now_ms` that something else (a runtime interval) decided
    /// was due; returns the ms since the previous run.
    #[inline]
    pub(crate) fn mark_run(&mut self, now_ms: u64) -> u64 {
        let elapsed = now_ms.saturating_sub(self.last_run_ms);
        self.last_run_ms = now_ms;
        elapsed
    }

    #[cfg(test)]
    fn next_due_ms(&self) -> u64 {
        self.next_due_ms
    }
}

/// How many periods' worth of work one catch-up run of a `period_ms` chore
/// owes after `elapsed_ms`: `elapsed / period`, at least 1, at most `cap`.
///
/// Rounds DOWN, so an on-time or slightly late run (the norm) is exactly 1;
/// only a run that skipped whole periods scales.
#[inline]
pub(crate) fn catch_up_scale(elapsed_ms: u64, period_ms: u64, cap: u32) -> u32 {
    let periods = elapsed_ms / period_ms.max(1);
    periods.clamp(1, u64::from(cap.max(1))) as u32
}

/// Active expiry's cadence (ms) — both runtimes run it every 100 ms.
pub(crate) const EXPIRY_PERIOD_MS: u64 = 100;

/// Hard cap on active expiry's catch-up scale: one run after a stall may
/// spend at most this many cycles' budget (4 × the 1 ms per-database
/// budget), however long the stall was. Before moon#1280 a 3 s stall replayed
/// 30 cycles back to back.
pub(crate) const EXPIRY_CATCH_UP_MAX_SCALE: u32 = 4;

/// Monotonic milliseconds since the shard loop started.
///
/// Chores run off this, not the shard's cached wall clock: a wall-clock step
/// (NTP) must neither fire every chore at once nor starve them until the
/// clock catches back up.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LoopClock {
    epoch: Instant,
}

impl LoopClock {
    pub(crate) fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    /// Milliseconds from the loop's start to `at`.
    #[inline]
    pub(crate) fn ms_at(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.epoch).as_millis() as u64
    }

    /// Milliseconds from the loop's start to now (one clock read).
    #[inline]
    pub(crate) fn now_ms(&self) -> u64 {
        self.ms_at(Instant::now())
    }
}

/// A periodic tick counts as LATE when it fires more than this after its
/// scheduled deadline. Equal to both runtimes' own grace: an interval late by
/// up to 5 ms still catches up tick by tick; past it, it skips.
pub(crate) const LATE_TICK_THRESHOLD: Duration = Duration::from_millis(5);

static TICK_LATE_TOTAL: AtomicU64 = AtomicU64::new(0);
static TICK_BURST_MAX: AtomicU64 = AtomicU64::new(0);

/// Per-shard observer of the periodic tick's lateness.
///
/// A BURST is the catch-up moon#1280 is about: the late tick that ends a
/// stall, followed by every late tick that the interval REPLAYED — scheduled
/// within [`LATE_TICK_THRESHOLD`] of its predecessor's deadline although that
/// predecessor fired more than the threshold late, so it was already due
/// when its predecessor fired and ran back to back with it. Under Burst the
/// replayed deadlines are one period apart (a 3 s stall: ~3,000 of them).
/// Under Skip a tick late by more than the grace re-schedules the next one
/// into the future (past `now`, hence past `deadline + threshold`), so a
/// stall of any length yields a burst of ONE.
///
/// The test is on the two DEADLINES, not on when the loop got around to
/// observing the tick: the monoio loop drains the SPSC rings between the
/// timer firing and this observation, and that gap must not make a
/// rescheduled future tick look replayed.
///
/// A loop that is merely busy (every tick late, each re-scheduled into the
/// future) is not a burst either: each late tick starts a new one.
///
/// Cost: one relaxed atomic RMW per late tick; nothing on an on-time tick
/// beyond two comparisons. The caller supplies `now` (it reads the clock
/// once per tick for the chore cadences anyway).
#[derive(Debug, Default)]
pub(crate) struct TickLateness {
    /// Ticks in the current burst (0 after an on-time tick).
    burst: u64,
    /// The previous tick's scheduled deadline.
    prev_deadline: Option<Instant>,
    /// When the previous tick fired (the per-tick duties' catch-up scale).
    prev_fired: Option<Instant>,
}

/// Cap on how many 1 ms ticks' budget the per-tick duties (the lazy-free
/// drain, the snapshot walk) take in one tick that fires late (moon#1280
/// review round 2, MAJOR-1): with missed ticks skipped, a loop whose every
/// round runs long (back-to-back ~25 ms EVALs) otherwise gave them one
/// slice per round instead of one per millisecond — measured 10-21x slower
/// frees of ~380 MB of UNLINKed values than an idle loop, and a BGSAVE
/// 0.75 s -> 4.1 s. 8 slices (2 ms of lazy free) bounds what one tick can
/// add to a command's latency while keeping them near 1/period; Burst
/// replayed every missed tick.
pub(crate) const PER_TICK_CATCH_UP_MAX_SCALE: u32 = 8;

impl TickLateness {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record one fired periodic tick scheduled for `deadline`, firing at `now`.
    /// Returns how many 1 ms ticks' budget the per-tick duties owe this tick:
    /// the milliseconds since the previous tick fired, at least 1, at most
    /// [`PER_TICK_CATCH_UP_MAX_SCALE`].
    #[inline]
    pub(crate) fn observe(&mut self, deadline: Instant, now: Instant) -> u32 {
        let scale = self.prev_fired.map_or(1, |prev| {
            let elapsed = now.saturating_duration_since(prev).as_millis() as u64;
            catch_up_scale(elapsed, 1, PER_TICK_CATCH_UP_MAX_SCALE)
        });
        self.prev_fired = Some(now);
        self.observe_lateness(deadline, now);
        scale
    }

    fn observe_lateness(&mut self, deadline: Instant, now: Instant) {
        let late = now.saturating_duration_since(deadline) > LATE_TICK_THRESHOLD;
        // Replayed: scheduled right after a predecessor that itself fired
        // late (the burst is non-empty only then), rather than re-scheduled
        // past the moment that predecessor fired.
        let replayed = self
            .prev_deadline
            .is_some_and(|prev| deadline <= prev + LATE_TICK_THRESHOLD);
        self.prev_deadline = Some(deadline);
        if !late {
            self.burst = 0;
            return;
        }
        self.burst = if self.burst > 0 && replayed {
            self.burst + 1
        } else {
            1
        };
        TICK_LATE_TOTAL.fetch_add(1, Ordering::Relaxed);
        TICK_BURST_MAX.fetch_max(self.burst, Ordering::Relaxed);
    }
}

/// Periodic ticks, across all shards, that fired more than
/// [`LATE_TICK_THRESHOLD`] after their deadline (INFO `shard_tick_late_total`).
pub fn tick_late_total() -> u64 {
    TICK_LATE_TOTAL.load(Ordering::Relaxed)
}

/// The most periodic ticks one stall fired back to back on any shard since
/// start (INFO `shard_tick_burst_max`; see [`TickLateness`]). 1 with the Skip
/// policy; ~stall/period before moon#1280.
pub fn tick_burst_max() -> u64 {
    TICK_BURST_MAX.load(Ordering::Relaxed)
}

/// The monoio shard loop's chores, each on its own elapsed-time cadence.
/// (The tokio loop gives each chore its own Skip interval instead.)
#[cfg(feature = "runtime-monoio")]
pub(crate) struct ChoreCadences {
    /// Blocked-client timeouts (10 ms).
    pub(crate) block_timeout: Cadence,
    /// Active expiry + eviction + MQ triggers (100 ms).
    pub(crate) expiry: Cadence,
    /// WAL fsync, idle-client timeout, MVCC sweep, text postings, spin
    /// governor, ops/sec sample (1 s).
    pub(crate) second: Cadence,
    /// Warm-tier transition check.
    pub(crate) warm: Cadence,
    /// Disk / RSS watchdog poll (5 s, shard 0).
    pub(crate) disk: Cadence,
    /// Autovacuum daemon.
    pub(crate) autovacuum: Cadence,
    /// Cold-tier orphan sweep; `None` when disabled (interval 0).
    pub(crate) orphan: Option<Cadence>,
}

#[cfg(feature = "runtime-monoio")]
impl ChoreCadences {
    pub(crate) fn new(
        now_ms: u64,
        warm_poll_ms: u64,
        autovacuum_interval_secs: u64,
        orphan_sweep_interval_secs: u64,
    ) -> Self {
        Self {
            block_timeout: Cadence::new(now_ms, 10),
            expiry: Cadence::new(now_ms, EXPIRY_PERIOD_MS),
            second: Cadence::new(now_ms, 1_000),
            warm: Cadence::new(now_ms, warm_poll_ms),
            disk: Cadence::new(now_ms, 5_000),
            autovacuum: Cadence::new(now_ms, autovacuum_interval_secs.saturating_mul(1_000)),
            orphan: (orphan_sweep_interval_secs > 0)
                .then(|| Cadence::new(now_ms, orphan_sweep_interval_secs.saturating_mul(1_000))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cadence_first_run_is_one_period_after_creation() {
        let mut c = Cadence::new(1_000, 100);
        assert_eq!(
            c.poll(1_000),
            None,
            "no run at t=0 (the counter chores never had one)"
        );
        assert_eq!(c.poll(1_099), None);
        assert_eq!(c.poll(1_100), Some(100));
        assert_eq!(c.poll(1_101), None);
    }

    #[test]
    fn cadence_keeps_its_grid_under_jitter() {
        let mut c = Cadence::new(0, 100);
        // Each run a little late: the grid does not drift.
        assert_eq!(c.poll(103), Some(103));
        assert_eq!(c.next_due_ms(), 200);
        assert_eq!(c.poll(199), None);
        assert_eq!(c.poll(250), Some(147));
        assert_eq!(c.next_due_ms(), 300);
        // Ten runs at the nominal rate take ten periods.
        let mut runs = 0;
        for t in 300..1_300 {
            if c.poll(t).is_some() {
                runs += 1;
            }
        }
        assert_eq!(runs, 10);
    }

    /// THE moon#1280 property at the chore level: a stall that skipped many
    /// periods yields ONE run (reporting the whole elapsed time), then the
    /// cadence resumes a full period later — it does not replay the missed
    /// runs on the following ticks.
    #[test]
    fn cadence_stall_costs_one_catch_up_run() {
        let mut c = Cadence::new(0, 10);
        assert_eq!(c.poll(10), Some(10));
        // 3 s stall: 300 periods missed. One run, carrying the elapsed time.
        assert_eq!(c.poll(3_010), Some(3_000));
        // The following 1 ms ticks do not replay the missed periods.
        let replays = (3_011..3_020).filter(|&t| c.poll(t).is_some()).count();
        assert_eq!(replays, 0, "no back-to-back catch-up runs");
        assert_eq!(c.poll(3_020), Some(10), "back on cadence one period later");
    }

    #[test]
    fn mark_run_reports_elapsed_since_previous_run() {
        let mut c = Cadence::new(50, 1_000);
        assert_eq!(c.mark_run(50), 0);
        assert_eq!(c.mark_run(1_050), 1_000);
        assert_eq!(c.mark_run(4_050), 3_000);
    }

    #[test]
    fn catch_up_scale_is_one_on_time_and_capped_after_a_stall() {
        assert_eq!(catch_up_scale(0, 100, 4), 1, "t=0 tokio first tick");
        assert_eq!(catch_up_scale(100, 100, 4), 1);
        assert_eq!(
            catch_up_scale(199, 100, 4),
            1,
            "late but not a whole period"
        );
        assert_eq!(catch_up_scale(200, 100, 4), 2);
        assert_eq!(catch_up_scale(3_000, 100, 4), 4, "3 s stall is capped");
        assert_eq!(
            catch_up_scale(5, 0, 4),
            4,
            "zero period cannot divide by zero"
        );
        assert_eq!(catch_up_scale(5, 1, 0), 1, "a zero cap still runs once");
    }

    fn ms(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    /// Burst: after a 100 ms stall, the 1 ms ticks owed during it fire at
    /// the resume instant, each already due when the previous one fired.
    #[test]
    fn tick_lateness_counts_a_replayed_burst() {
        let b = Instant::now();
        let before_total = tick_late_total();
        let mut t = TickLateness::new();
        t.observe(ms(b, 1), ms(b, 1)); // on time
        t.observe(ms(b, 2), ms(b, 2) + LATE_TICK_THRESHOLD); // at the grace: on time
        assert_eq!(t.burst, 0);
        // Stall from 3 ms to 103 ms; Burst replays deadlines 3..=97 (the
        // tail within 5 ms of the resume is on time by definition).
        for d in 3..=97 {
            t.observe(ms(b, d), ms(b, 103));
        }
        assert_eq!(t.burst, 95);
        assert!(tick_burst_max() >= 95);
        t.observe(ms(b, 98), ms(b, 103));
        assert_eq!(t.burst, 0, "a tick within the grace ends the burst");
        // Other tests may run concurrently and add to the global total.
        assert!(tick_late_total() >= before_total + 95);
    }

    /// Skip: the same stall fires ONE late tick; the next is scheduled in
    /// the future and waited for.
    #[test]
    fn tick_lateness_counts_one_per_stall_with_skip() {
        let b = Instant::now();
        let mut t = TickLateness::new();
        t.observe(ms(b, 2), ms(b, 2));
        t.observe(ms(b, 3), ms(b, 103)); // the stall's one catch-up tick
        assert_eq!(t.burst, 1);
        // Skip re-scheduled the next tick past 103 ms. Even observed late
        // (the loop was busy between the timer and the observation), it is
        // not a replay of the stall.
        t.observe(ms(b, 104), ms(b, 111));
        assert_eq!(t.burst, 1, "a re-scheduled tick starts a new burst");
        t.observe(ms(b, 112), ms(b, 112));
        assert_eq!(t.burst, 0);
    }

    /// A busy loop — every tick late, each scheduled after the previous fired
    /// (Skip re-schedules into the future) — is not a burst.
    #[test]
    fn tick_lateness_busy_loop_is_not_a_burst() {
        let b = Instant::now();
        let mut t = TickLateness::new();
        let mut fire = 0;
        for _ in 0..10 {
            let deadline = fire + 1; // re-scheduled past the previous fire
            fire = deadline + 8; // but fired 8 ms late
            t.observe(ms(b, deadline), ms(b, fire));
            assert_eq!(t.burst, 1, "each late tick starts a new burst of one");
        }
    }

    #[test]
    fn loop_clock_is_monotonic_ms_since_start() {
        let clock = LoopClock::new();
        let before = clock.epoch - Duration::from_millis(1);
        assert_eq!(clock.ms_at(before), 0, "saturates before the epoch");
        assert_eq!(
            clock.ms_at(clock.epoch + Duration::from_millis(1_500)),
            1_500
        );
    }

    #[cfg(feature = "runtime-monoio")]
    #[test]
    fn chore_cadences_match_the_counter_periods_they_replace() {
        let mut c = ChoreCadences::new(0, 10_000, 30, 0);
        assert!(c.orphan.is_none(), "interval 0 disables the orphan sweep");
        assert_eq!(c.block_timeout.poll(10), Some(10));
        assert_eq!(c.expiry.poll(99), None);
        assert_eq!(c.expiry.poll(100), Some(100));
        assert_eq!(c.second.poll(1_000), Some(1_000));
        assert_eq!(c.warm.poll(9_999), None);
        assert_eq!(c.warm.poll(10_000), Some(10_000));
        assert_eq!(c.disk.poll(5_000), Some(5_000));
        assert_eq!(c.autovacuum.poll(29_999), None);
        assert_eq!(c.autovacuum.poll(30_000), Some(30_000));
        let c = ChoreCadences::new(0, 1_000, 1, 300);
        assert_eq!(c.orphan.map(|o| o.next_due_ms()), Some(300_000));
    }

    /// moon#1280 review MAJOR-1: a late tick owes the per-tick duties the
    /// ticks it skipped, capped; an on-time tick owes exactly one.
    #[test]
    fn a_late_tick_scales_the_per_tick_duties_up_to_the_cap() {
        let t0 = Instant::now();
        let ms = |n: u64| t0 + std::time::Duration::from_millis(n);
        let mut t = TickLateness::new();
        assert_eq!(t.observe(ms(0), ms(0)), 1, "first tick");
        assert_eq!(t.observe(ms(1), ms(1)), 1, "on time");
        assert_eq!(
            t.observe(ms(2), ms(26)),
            PER_TICK_CATCH_UP_MAX_SCALE,
            "25 ms round: capped"
        );
        assert_eq!(t.observe(ms(27), ms(30)), 4, "4 ms since the previous tick");
        assert_eq!(t.observe(ms(31), ms(31)), 1);
    }
}
