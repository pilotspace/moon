//! Periodic intervals that skip missed ticks instead of bursting them
//! (moon#1280).
//!
//! Both runtimes' `interval()` default to `MissedTickBehavior::Burst`: when
//! the thread driving an interval is held up past a tick's deadline — a
//! descheduled process (SIGSTOP, a CI runner hiccup, a VM steal), a long
//! command, a long startup — every missed tick fires back to back on resume.
//! A shard's tick bodies are each bounded (a 250 µs lazy-free slice, a 1 ms
//! expiry budget, a snapshot-walk byte budget), but a burst of N catch-up
//! ticks runs N of those bounds in one go. Measured before this module
//! (release-fast, `--shards 1`, 3 s SIGSTOP with an 8-hash / 4M-field UNLINK
//! backlog queued): the first PING after SIGCONT waited 178–211 ms on monoio
//! (the whole backlog drained in one piece — monoio fires an elapsed timer
//! synchronously on re-registration, so the shard task never yields) and
//! 36–50 ms on tokio (coop budget splits the burst into 128-tick chunks).
//!
//! **Policy: `Skip`.** After a late tick the next deadline is the next
//! period-aligned instant in the future, so a stall of any length costs at
//! most ONE catch-up tick. Chosen over the alternatives:
//! - `Burst` (the default) is the bug.
//! - `Delay` (`next = now + period`) would also bound the catch-up to one
//!   tick, but it re-phases the interval on every late tick; `Skip` keeps a
//!   fixed-rate grid, so a chore's long-run rate stays exactly `1/period`.
//!   For the 1 ms shard tick the two are indistinguishable.
//!
//! Both runtimes keep a 5 ms grace: a tick late by ≤ 5 ms still schedules
//! `deadline + period` (tokio `time/interval.rs`, vendored monoio
//! `src/time/interval.rs` — identical logic), so a sub-5 ms hiccup can fire
//! at most ~5 one-millisecond ticks back to back. That is bounded and cheap;
//! anything longer is skipped.
//!
//! What a skipped tick may lose is GRANULARITY, never correctness: every
//! duty the shard drives from a tick either works off absolute state (the
//! WAL buffer, blocked-client deadlines, the memory ledger, idle-client
//! timestamps, `last_save`) or measures its own elapsed time (the spin
//! governor, autovacuum, active expiry's catch-up scale — see
//! `crate::shard::tick_cadence`).
//!
//! Every periodic interval in the crate MUST come from here (or from
//! [`crate::runtime::TimerImpl::interval`], which delegates here).
//! `clippy.toml` disallows the raw constructors so a new interval cannot
//! forget the policy.

use std::time::Duration;

/// A `tokio::time::Interval` that skips missed ticks (moon#1280).
///
/// Available on both runtime legs: the admin HTTP server, the cluster gossip
/// ticker, the metrics publishers and the non-sharded expiry task run on
/// tokio even in a monoio build.
///
/// Like `tokio::time::interval`, the first tick completes immediately.
pub fn tokio_interval(period: Duration) -> tokio::time::Interval {
    #[allow(clippy::disallowed_methods)] // the one sanctioned constructor
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

/// A `monoio::time::Interval` that skips missed ticks (moon#1280).
///
/// The vendored monoio carries tokio's `MissedTickBehavior` verbatim, so no
/// runtime patch is needed. Like `monoio::time::interval`, the first tick
/// completes immediately.
#[cfg(feature = "runtime-monoio")]
pub fn monoio_interval(period: Duration) -> monoio::time::Interval {
    #[allow(clippy::disallowed_methods)] // the one sanctioned constructor
    let mut interval = monoio::time::interval(period);
    interval.set_missed_tick_behavior(monoio::time::MissedTickBehavior::Skip);
    interval
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    /// Length of the simulated stall. Long enough that a Burst interval owes
    /// ~200 one-millisecond ticks, short enough to keep the test fast.
    const STALL: Duration = Duration::from_millis(200);

    /// Count the CATCH-UP ticks after a stall: ticks whose scheduled deadline
    /// fell inside the stall (before `resumed`). Stops at the first tick
    /// scheduled at or after `resumed` — from there on the interval is back
    /// on its grid. Scheduling noise after the resume cannot inflate the
    /// count: a tick that is late for any other reason has a deadline after
    /// `resumed`.
    ///
    /// Burst (pre-moon#1280): every missed tick is owed → ~STALL/1ms.
    /// Skip: exactly one, the tick that was pending when the stall began.
    fn count_catch_up(deadlines: impl Iterator<Item = Instant>, resumed: Instant) -> u32 {
        let mut n = 0;
        for d in deadlines {
            if d >= resumed {
                break;
            }
            n += 1;
            assert!(n < 10_000, "interval never returned to its grid");
        }
        n
    }

    #[test]
    fn tokio_interval_fires_one_catch_up_tick_after_a_stall() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime");
        let catch_up = rt.block_on(async {
            let mut iv = super::tokio_interval(Duration::from_millis(1));
            iv.tick().await; // immediate first tick
            iv.tick().await; // now on the grid
            std::thread::sleep(STALL); // the stall: the driving thread is held
            let resumed = Instant::now();
            let mut deadlines = Vec::new();
            loop {
                let d = iv.tick().await.into_std();
                deadlines.push(d);
                if d >= resumed || deadlines.len() >= 10_000 {
                    break;
                }
            }
            count_catch_up(deadlines.into_iter(), resumed)
        });
        assert!(
            catch_up <= 1,
            "a {STALL:?} stall must cost at most one catch-up tick, got {catch_up} \
             (MissedTickBehavior::Burst replays every missed 1 ms tick)"
        );
    }

    #[cfg(feature = "runtime-monoio")]
    fn monoio_catch_up_after_stall<D>(mut rt: monoio::Runtime<D>) -> u32
    where
        D: monoio::Driver,
    {
        rt.block_on(async {
            let mut iv = super::monoio_interval(Duration::from_millis(1));
            iv.tick().await;
            iv.tick().await;
            std::thread::sleep(STALL);
            let resumed = Instant::now();
            let mut deadlines = Vec::new();
            loop {
                let d = iv.tick().await.into_std();
                deadlines.push(d);
                if d >= resumed || deadlines.len() >= 10_000 {
                    break;
                }
            }
            count_catch_up(deadlines.into_iter(), resumed)
        })
    }

    /// The legacy (epoll/kqueue) driver — `MOON_NO_URING=1` / macOS.
    #[cfg(feature = "runtime-monoio")]
    #[test]
    fn monoio_interval_fires_one_catch_up_tick_after_a_stall_legacy_driver() {
        let rt = monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio legacy runtime");
        let catch_up = monoio_catch_up_after_stall(rt);
        assert!(
            catch_up <= 1,
            "a {STALL:?} stall must cost at most one catch-up tick, got {catch_up} \
             (monoio re-fires an elapsed interval synchronously: Burst replays \
             every missed 1 ms tick without the task ever yielding)"
        );
    }

    /// The io_uring driver (the Linux default). The timer wheel is shared, but
    /// the park path differs, so both are pinned.
    #[cfg(all(feature = "runtime-monoio", target_os = "linux"))]
    #[test]
    fn monoio_interval_fires_one_catch_up_tick_after_a_stall_uring_driver() {
        let Ok(rt) = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
        else {
            // io_uring unavailable (old kernel / seccomp): the legacy-driver
            // test above still pins the timer semantics.
            return;
        };
        let catch_up = monoio_catch_up_after_stall(rt);
        assert!(
            catch_up <= 1,
            "a {STALL:?} stall must cost at most one catch-up tick, got {catch_up}"
        );
    }

    /// Sanity (green before and after): without a stall the interval keeps
    /// its period — Skip must not slow the fast path down.
    #[test]
    fn tokio_interval_keeps_its_period_without_a_stall() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime");
        let (first, last) = rt.block_on(async {
            let mut iv = super::tokio_interval(Duration::from_millis(2));
            let first = iv.tick().await.into_std();
            let mut last = first;
            for _ in 0..10 {
                last = iv.tick().await.into_std();
            }
            (first, last)
        });
        // Deadlines stay on the 2 ms grid whether or not a tick ran late.
        let span = last.duration_since(first);
        assert!(
            span >= Duration::from_millis(20),
            "10 ticks of a 2 ms interval span at least 20 ms of deadlines, got {span:?}"
        );
    }
}
