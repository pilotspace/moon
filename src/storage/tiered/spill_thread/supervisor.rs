//! Restart policy of a shard's spill thread (moon#1265): bounded exponential
//! backoff and a restart budget per window, then a declared degraded mode.
//!
//! A plain state machine owned by the shard thread (behind the
//! `SpillThread`'s worker lock): no atomics, no clock of its own. Every
//! transition takes `now_ms` from the caller, which reads the shard-cached
//! clock on its tick, and nothing here sleeps — a backoff is a due time the
//! tick compares against, never a wait on the shard thread.
//!
//! ```text
//!              death, budget left                 due, spawn ok
//!   Running ─────────────────────────▶ Backoff ──────────────────▶ Running
//!      │                                  │ ▲
//!      │ death, budget spent              │ │ due, spawn failed, budget left
//!      ▼                                  ▼ │
//!   Degraded ◀─────────────────────────── (spawn failed, budget spent)
//! ```
//!
//! `Degraded` is terminal for the process: the shard stops spilling (its
//! evictions take the no-spill path) until a restart.

use std::collections::VecDeque;

/// Tunables of [`RestartSupervisor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestartPolicy {
    /// Backoff before the first respawn of a streak.
    pub(crate) base_backoff_ms: u64,
    /// The backoff never exceeds this.
    pub(crate) max_backoff_ms: u64,
    /// Respawns (successful or not) allowed inside one `window_ms`.
    pub(crate) max_restarts: usize,
    /// The sliding window the budget counts over.
    pub(crate) window_ms: u64,
    /// A thread that ran this long before it died starts a new backoff
    /// streak (its death is not part of a crash loop).
    pub(crate) healthy_run_ms: u64,
}

impl RestartPolicy {
    /// 100 ms doubling to 30 s, 5 respawns per 10 minutes, and a minute of
    /// uptime resets the streak.
    pub(crate) const DEFAULT: Self = Self {
        base_backoff_ms: 100,
        max_backoff_ms: 30_000,
        max_restarts: 5,
        window_ms: 10 * 60 * 1000,
        healthy_run_ms: 60 * 1000,
    };
}

/// Where the supervised thread is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// A thread was started at `since_ms` (it may have died since: the shard
    /// observes that and calls [`RestartSupervisor::on_death`]).
    Running { since_ms: u64 },
    /// Dead; respawn once the shard clock reaches `due_ms`.
    Backoff { due_ms: u64 },
    /// The budget is spent: no thread, and none will be started.
    Degraded,
}

/// What the shard does about a death.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Respawn at this shard-clock time.
    RespawnAt(u64),
    /// Stop spilling for good.
    Degrade,
}

/// The restart state machine. See the module doc.
#[derive(Debug)]
pub(crate) struct RestartSupervisor {
    policy: RestartPolicy,
    phase: Phase,
    /// Shard-clock times of the respawn attempts inside the current window.
    attempts: VecDeque<u64>,
    /// Deaths in the current backoff streak (the backoff's exponent).
    streak: u32,
}

impl RestartSupervisor {
    pub(crate) fn new(policy: RestartPolicy, now_ms: u64) -> Self {
        Self {
            policy,
            phase: Phase::Running { since_ms: now_ms },
            attempts: VecDeque::with_capacity(policy.max_restarts + 1),
            streak: 0,
        }
    }

    #[inline]
    pub(crate) fn phase(&self) -> Phase {
        self.phase
    }

    /// The running thread died (observed at `now_ms`).
    pub(crate) fn on_death(&mut self, now_ms: u64) -> Verdict {
        if let Phase::Running { since_ms } = self.phase
            && now_ms.saturating_sub(since_ms) >= self.policy.healthy_run_ms
        {
            self.streak = 0;
        }
        self.decide(now_ms)
    }

    /// Whether a respawn is due at `now_ms`. A due time further ahead than
    /// the longest backoff means the clock went backwards: due now, rather
    /// than leaving the shard without a spill thread for the size of the jump.
    pub(crate) fn respawn_due(&self, now_ms: u64) -> bool {
        match self.phase {
            Phase::Backoff { due_ms } => {
                now_ms >= due_ms || due_ms - now_ms > self.policy.max_backoff_ms
            }
            Phase::Running { .. } | Phase::Degraded => false,
        }
    }

    /// A respawn was attempted at `now_ms` and the thread started.
    pub(crate) fn on_respawned(&mut self, now_ms: u64) {
        self.attempts.push_back(now_ms);
        self.phase = Phase::Running { since_ms: now_ms };
    }

    /// A respawn was attempted at `now_ms` and the thread could not be
    /// started. The attempt counts against the budget like a successful one.
    pub(crate) fn on_spawn_failed(&mut self, now_ms: u64) -> Verdict {
        self.attempts.push_back(now_ms);
        self.decide(now_ms)
    }

    /// Respawn attempts inside the window ending at `now_ms`.
    pub(crate) fn attempts_in_window(&mut self, now_ms: u64) -> usize {
        self.forget_before(now_ms);
        self.attempts.len()
    }

    fn forget_before(&mut self, now_ms: u64) {
        let horizon = now_ms.saturating_sub(self.policy.window_ms);
        while self
            .attempts
            .front()
            .is_some_and(|&t| t < horizon || t > now_ms)
        {
            self.attempts.pop_front();
        }
    }

    fn decide(&mut self, now_ms: u64) -> Verdict {
        if self.attempts_in_window(now_ms) >= self.policy.max_restarts {
            self.phase = Phase::Degraded;
            return Verdict::Degrade;
        }
        let backoff = self
            .policy
            .base_backoff_ms
            .checked_shl(self.streak)
            .unwrap_or(u64::MAX)
            .min(self.policy.max_backoff_ms);
        self.streak = self.streak.saturating_add(1);
        let due_ms = now_ms.saturating_add(backoff);
        self.phase = Phase::Backoff { due_ms };
        Verdict::RespawnAt(due_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: RestartPolicy = RestartPolicy::DEFAULT;

    /// Die and respawn as soon as allowed; returns each backoff taken.
    fn crash_loop(s: &mut RestartSupervisor, mut now: u64, deaths: usize) -> (Vec<u64>, u64) {
        let mut backoffs = Vec::new();
        for _ in 0..deaths {
            match s.on_death(now) {
                Verdict::RespawnAt(due) => {
                    backoffs.push(due - now);
                    assert!(!s.respawn_due(due - 1), "due before its time");
                    assert!(s.respawn_due(due));
                    now = due;
                    s.on_respawned(now);
                    now += 1;
                }
                Verdict::Degrade => break,
            }
        }
        (backoffs, now)
    }

    #[test]
    fn backoff_doubles_from_100ms_and_the_budget_degrades_the_sixth_death() {
        let mut s = RestartSupervisor::new(P, 1_000);
        let (backoffs, now) = crash_loop(&mut s, 1_000, 5);
        assert_eq!(backoffs, vec![100, 200, 400, 800, 1_600]);
        assert_eq!(s.on_death(now), Verdict::Degrade, "5 respawns per window");
        assert_eq!(s.phase(), Phase::Degraded);
        assert!(!s.respawn_due(u64::MAX), "degraded is terminal");
    }

    #[test]
    fn the_backoff_is_capped_at_30s() {
        let policy = RestartPolicy {
            max_restarts: 100,
            window_ms: 1,
            ..P
        };
        let mut s = RestartSupervisor::new(policy, 0);
        let (backoffs, _) = crash_loop(&mut s, 0, 12);
        assert_eq!(
            backoffs[..9],
            [100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600]
        );
        assert!(backoffs[9..].iter().all(|&b| b == 30_000), "{backoffs:?}");
    }

    #[test]
    fn deaths_spread_wider_than_the_window_never_degrade() {
        let mut s = RestartSupervisor::new(P, 0);
        let mut now = 0u64;
        for i in 0..50 {
            // Every death comes after a healthy run and a window's gap.
            now += P.window_ms / 4;
            let v = s.on_death(now);
            assert_eq!(v, Verdict::RespawnAt(now + 100), "death {i}: streak reset");
            now += 100;
            s.on_respawned(now);
            assert!(s.attempts_in_window(now) <= 5);
        }
    }

    #[test]
    fn a_healthy_run_resets_the_streak_but_not_the_budget() {
        let mut s = RestartSupervisor::new(P, 0);
        let (_, now) = crash_loop(&mut s, 0, 3);
        // Ran a full minute: the backoff starts over, the window still
        // holds the three respawns.
        let later = now + P.healthy_run_ms;
        assert_eq!(s.on_death(later), Verdict::RespawnAt(later + 100));
        s.on_respawned(later + 100);
        assert_eq!(s.attempts_in_window(later + 100), 4);
        assert_eq!(s.on_death(later + 200), Verdict::RespawnAt(later + 400));
        s.on_respawned(later + 400);
        assert_eq!(s.on_death(later + 500), Verdict::Degrade);
    }

    #[test]
    fn a_failed_spawn_backs_off_further_and_counts_against_the_budget() {
        let mut s = RestartSupervisor::new(P, 0);
        assert_eq!(s.on_death(0), Verdict::RespawnAt(100));
        for (i, want) in [300u64, 700, 1_500, 3_100].into_iter().enumerate() {
            let now = match s.phase() {
                Phase::Backoff { due_ms } => due_ms,
                p => panic!("attempt {i}: {p:?}"),
            };
            assert_eq!(s.on_spawn_failed(now), Verdict::RespawnAt(want));
        }
        assert_eq!(s.on_spawn_failed(3_100), Verdict::Degrade, "fifth attempt");
    }

    #[test]
    fn a_clock_that_jumps_back_does_not_strand_the_respawn() {
        let mut s = RestartSupervisor::new(P, 1_000_000);
        assert_eq!(s.on_death(1_000_000), Verdict::RespawnAt(1_000_100));
        assert!(!s.respawn_due(1_000_050));
        assert!(s.respawn_due(1_000), "a due time an hour ahead is due now");
        s.on_respawned(1_000);
        // The respawn stamped before the jump is not counted twice or kept
        // forever: it lies in the future of the new clock and is forgotten.
        assert_eq!(s.attempts_in_window(1_000), 1);
    }
}
