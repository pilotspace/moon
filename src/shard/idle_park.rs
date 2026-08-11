//! Adaptive idle park for the shard event loop (issue #373 phase 2).
//!
//! The monoio shard loop races a periodic timer against the SPSC notify.
//! At the historical fixed 1ms period, an idle 4-shard server still burned
//! ~2.4% CPU purely on timer wakeups and their chore checks (measured after
//! the phase-1 cheap wins; the remainder was `clock_gettime` + park/unpark
//! syscalls from the 1ms cadence itself).
//!
//! This module holds the pure decision state machine: after
//! [`IdleParkState::ENTER_STREAK`] consecutive provably-no-op 1ms ticks, the
//! loop stretches its park to [`IDLE_PARK_MS`] (10ms). The stretched period
//! is chosen so that NO chore cadence changes: every sub-timer dispatch is
//! `counter % {10,100,1000,5000}`, all multiples of 10, and idle entry is
//! gated on a `counter % 10 == 0` boundary — stepping the counter by 10 per
//! idle tick therefore hits every chore boundary exactly as the 1ms walk
//! did. BLPOP timeout expiry (the tightest cadence, 10ms) is unaffected.
//!
//! What DOES change while parked idle, by design and documented in the
//! issue: the shard's cached clock refreshes every 10ms instead of every
//! 1ms, so a command arriving mid-park on an existing LOCAL connection can
//! read a clock up to 10ms stale for lazy-expiry checks (active expiry
//! keeps its 100ms cadence). Cross-shard commands are exempt on both race
//! outcomes: a notify-win wake exits idle and refreshes the clock before
//! draining, and a timer-win wake with pending SPSC messages refreshes the
//! clock before its drain too. Replica-applied commands never read the
//! cached clock at all (`apply.rs` uses `refresh_now`).
//!
//! Eligibility is deliberately conservative — ALL of:
//! - no commands were counted on this shard thread since the last tick
//!   (per-thread total-commands slot: local connection dispatch AND
//!   replica-applied commands, which bump it via `record_replica_apply`),
//! - no cross-shard SPSC message was pending at this tick's drain
//!   (queue-occupancy probe — cross-shard commands are counted by their
//!   ORIGIN shard's handler, so the counter alone cannot see them),
//! - the WAL append buffer is empty and the local WAL channel is empty,
//! - no snapshot is in progress or requested, no checkpoint is active,
//! - `appendfsync != always`, no CDC subscribers,
//! - the loop woke on the timer (an SPSC notify wake resets the streak).
//!
//! Escape hatch: `MOON_IDLE_PARK=0` pins the loop to the fixed 1ms period
//! (same-binary A/B knob, mirroring the other `MOON_*` diagnostics).

/// Stretched park period while idle, in milliseconds. Must divide every
/// counter-based chore cadence (10/100/1000/5000) and equal the tightest
/// one (block-timeout, 10ms) so no chore's timing changes while idle.
pub(crate) const IDLE_PARK_MS: u64 = 10;

/// Pure state machine deciding when the shard loop may stretch its park.
#[derive(Debug)]
pub(crate) struct IdleParkState {
    /// Consecutive no-op 1ms ticks observed (only counted while not idle).
    streak: u32,
    /// Currently parking at the stretched period.
    idle: bool,
    /// Per-thread command-counter value at the last tick.
    last_commands: u64,
    /// `MOON_IDLE_PARK=0` escape hatch (checked once at construction).
    enabled: bool,
}

impl IdleParkState {
    /// No-op 1ms ticks required before stretching (64ms of proven quiet).
    /// Large enough that bursty traffic never oscillates the mode; small
    /// enough that a genuinely idle server reaches the stretched park fast.
    pub(crate) const ENTER_STREAK: u32 = 64;

    pub(crate) fn new() -> Self {
        let enabled = std::env::var("MOON_IDLE_PARK")
            .map(|v| v != "0")
            .unwrap_or(true);
        Self {
            streak: 0,
            idle: false,
            last_commands: 0,
            enabled,
        }
    }

    #[cfg(test)]
    fn new_enabled() -> Self {
        Self {
            streak: 0,
            idle: false,
            last_commands: 0,
            enabled: true,
        }
    }

    /// True while the loop should park at [`IDLE_PARK_MS`] instead of 1ms.
    #[inline]
    pub(crate) fn is_idle(&self) -> bool {
        self.idle
    }

    /// Counter step for this tick: 10 while idle-parked, else 1.
    #[inline]
    pub(crate) fn counter_step(&self) -> u64 {
        if self.idle { IDLE_PARK_MS } else { 1 }
    }

    /// The loop woke for a reason other than the timer (SPSC notify): work
    /// arrived, so drop out of idle immediately and restart the streak.
    /// Returns true if the loop WAS idle-parked (caller refreshes the
    /// cached clock before draining, closing the staleness window for
    /// cross-shard commands).
    #[inline]
    pub(crate) fn note_notify_wake(&mut self) -> bool {
        let was_idle = self.idle;
        self.idle = false;
        self.streak = 0;
        was_idle
    }

    /// Timer tick observed. `commands_now` is the per-thread command
    /// counter; `quiet` is the conjunction of every no-op condition the
    /// caller can see (WAL buffer empty, no snapshot/checkpoint, …);
    /// `aligned` is `counter % IDLE_PARK_MS == 0` AFTER this tick's step
    /// was applied. Returns the new idle decision for the NEXT park.
    pub(crate) fn on_timer_tick(&mut self, commands_now: u64, quiet: bool, aligned: bool) -> bool {
        let no_commands = commands_now == self.last_commands;
        self.last_commands = commands_now;
        if !self.enabled || !quiet || !no_commands {
            self.idle = false;
            self.streak = 0;
            return false;
        }
        if self.idle {
            // Still quiet: stay stretched.
            return true;
        }
        self.streak = self.streak.saturating_add(1);
        if self.streak >= Self::ENTER_STREAK && aligned {
            self.idle = true;
        }
        self.idle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_quiet_ticks(s: &mut IdleParkState, n: u32) {
        for _ in 0..n {
            s.on_timer_tick(0, true, true);
        }
    }

    #[test]
    fn enters_idle_only_after_full_streak() {
        let mut s = IdleParkState::new_enabled();
        run_quiet_ticks(&mut s, IdleParkState::ENTER_STREAK - 1);
        assert!(!s.is_idle(), "one tick short of the streak must stay fast");
        s.on_timer_tick(0, true, true);
        assert!(s.is_idle(), "full streak of quiet ticks enters idle");
        assert_eq!(s.counter_step(), IDLE_PARK_MS);
    }

    #[test]
    fn misaligned_counter_defers_entry_without_losing_streak() {
        let mut s = IdleParkState::new_enabled();
        run_quiet_ticks(&mut s, IdleParkState::ENTER_STREAK + 5);
        assert!(s.is_idle());

        let mut s = IdleParkState::new_enabled();
        for _ in 0..IdleParkState::ENTER_STREAK + 5 {
            s.on_timer_tick(0, true, false); // never on a %10 boundary
        }
        assert!(!s.is_idle(), "entry must wait for a chore-aligned counter");
        s.on_timer_tick(0, true, true);
        assert!(
            s.is_idle(),
            "first aligned tick after the streak enters idle"
        );
    }

    #[test]
    fn command_activity_resets_streak_and_exits_idle() {
        let mut s = IdleParkState::new_enabled();
        run_quiet_ticks(&mut s, IdleParkState::ENTER_STREAK);
        assert!(s.is_idle());
        // Commands moved: exit immediately.
        s.on_timer_tick(7, true, true);
        assert!(!s.is_idle());
        assert_eq!(s.counter_step(), 1);
        // Streak restarts from zero — one quiet tick is not enough.
        s.on_timer_tick(7, true, true);
        assert!(!s.is_idle());
    }

    #[test]
    fn non_quiet_conditions_block_and_exit_idle() {
        let mut s = IdleParkState::new_enabled();
        run_quiet_ticks(&mut s, IdleParkState::ENTER_STREAK);
        assert!(s.is_idle());
        s.on_timer_tick(0, false, true); // e.g. WAL buffer non-empty
        assert!(!s.is_idle());
    }

    #[test]
    fn notify_wake_exits_idle_and_reports_prior_state() {
        let mut s = IdleParkState::new_enabled();
        run_quiet_ticks(&mut s, IdleParkState::ENTER_STREAK);
        assert!(s.is_idle());
        assert!(
            s.note_notify_wake(),
            "must report it WAS idle (clock refresh)"
        );
        assert!(!s.is_idle());
        assert!(!s.note_notify_wake(), "already fast: no refresh needed");
    }

    #[test]
    fn disabled_via_env_never_enters_idle() {
        let mut s = IdleParkState {
            streak: 0,
            idle: false,
            last_commands: 0,
            enabled: false,
        };
        for _ in 0..IdleParkState::ENTER_STREAK * 4 {
            s.on_timer_tick(0, true, true);
        }
        assert!(!s.is_idle());
        assert_eq!(s.counter_step(), 1);
    }
}
