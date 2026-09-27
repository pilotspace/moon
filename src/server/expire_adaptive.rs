//! Adaptive active expiry (moon#1288).
//!
//! The 100 ms slow cycle spends 1 ms per cycle: 1% duty. That keeps a steady
//! trickle of expiries current, but an expired BACKLOG — a burst of keys
//! sharing a deadline, or a stalled process (SIGSTOP, VM pause, a long
//! command) resuming past thousands of deadlines — drained at ~7K keys/s:
//! 1.84M keys took ~4 minutes, their memory charged the whole time. redis's
//! `activeExpireCycle` adapts instead: while a sample stays >10% expired it
//! keeps sweeping, up to 25% CPU, and runs 1 ms "fast cycles" between event
//! loop iterations.
//!
//! Here: whenever a cycle ends on its budget with due work left (the exact
//! deadline-index analogue of redis's expired ratio, see
//! `expire_cycle_direct_budget`), the shard latches `backlog`, and every 1 ms
//! periodic tick runs a FAST slice until the backlog is gone. The slices are
//! paid for from a token bucket that earns `EXPIRE_FAST_DUTY_PCT`% of wall
//! time and holds at most `EXPIRE_FAST_SLICE_MAX` — so the fast cycle takes
//! at most ~25% of the shard, spread evenly (≈250 µs per on-time 1 ms tick),
//! and no single tick blocks for more than 1 ms: foreground requests wait at
//! most one slice behind it. A saturated loop ticks late and runs fewer
//! slices, so foreground load keeps priority.

use std::time::{Duration, Instant};

/// Share of shard wall time the fast expiry cycle may spend (moon#1288).
/// redis's slow-cycle cap (`ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC`).
pub const EXPIRE_FAST_DUTY_PCT: u32 = 25;

/// Longest single fast slice, and the token bucket's capacity (moon#1288):
/// redis's `ACTIVE_EXPIRE_CYCLE_FAST_DURATION`. Bounds how long one 1 ms
/// tick can hold the shard, however late the tick is.
pub const EXPIRE_FAST_SLICE_MAX: Duration = Duration::from_millis(1);

/// A fast slice is not worth its clock reads below this much credit.
const EXPIRE_FAST_SLICE_MIN: Duration = Duration::from_micros(50);

/// Per-shard-thread state of the adaptive fast expiry cycle (moon#1288).
struct ActiveExpireState {
    /// Some database of this shard ended its last cycle with due work left.
    backlog: std::cell::Cell<bool>,
    /// Token bucket, in nanoseconds of shard time (≤ `EXPIRE_FAST_SLICE_MAX`).
    credit_ns: std::cell::Cell<u64>,
    /// When the bucket was last topped up; `None` while no backlog exists.
    last_refill: std::cell::Cell<Option<Instant>>,
    /// First database the next fast slice visits (rotates, like the
    /// lazy-free drain, so a huge backlog in db 0 cannot starve db 15's).
    next_db: std::cell::Cell<usize>,
}

thread_local! {
    static ACTIVE_EXPIRE: ActiveExpireState = const {
        ActiveExpireState {
            backlog: std::cell::Cell::new(false),
            credit_ns: std::cell::Cell::new(0),
            last_refill: std::cell::Cell::new(None),
            next_db: std::cell::Cell::new(0),
        }
    };
}

/// Whether this shard has an expired backlog the fast cycle should drain
/// (moon#1288). One thread-local `Cell` read — the 1 ms tick's gate, and the
/// monoio idle park's "not quiet" term (a 10 ms park would cut the fast
/// cycle's duty tenfold).
#[inline]
pub fn expire_backlog_pending() -> bool {
    ACTIVE_EXPIRE.with(|s| s.backlog.get())
}

/// Latch (or clear) this shard's expired-backlog flag from a cycle's result.
pub fn note_expire_backlog(pending: bool) {
    ACTIVE_EXPIRE.with(|s| {
        if pending && !s.backlog.get() {
            // First slice gets one on-time tick's worth of credit.
            s.credit_ns
                .set(fast_slice_earned(Duration::from_millis(1)).as_nanos() as u64);
            s.last_refill.set(None);
        }
        s.backlog.set(pending);
        if !pending {
            s.last_refill.set(None);
        }
    });
}

/// What `elapsed` wall time earns the bucket: `EXPIRE_FAST_DUTY_PCT`% of it.
#[inline]
fn fast_slice_earned(elapsed: Duration) -> Duration {
    elapsed * EXPIRE_FAST_DUTY_PCT / 100
}

/// Top up the bucket for the time since the last refill and return the
/// slice this tick may spend, or `None` when the credit is too small
/// (moon#1288). `now` is the one clock read the fast tick pays.
pub fn take_fast_expire_slice(now: Instant) -> Option<Duration> {
    ACTIVE_EXPIRE.with(|s| {
        let cap = EXPIRE_FAST_SLICE_MAX.as_nanos() as u64;
        let mut credit = s.credit_ns.get();
        if let Some(last) = s.last_refill.get() {
            let earned = fast_slice_earned(now.saturating_duration_since(last));
            credit = credit.saturating_add(earned.as_nanos() as u64);
        }
        credit = credit.min(cap);
        s.credit_ns.set(credit);
        s.last_refill.set(Some(now));
        (credit >= EXPIRE_FAST_SLICE_MIN.as_nanos() as u64).then(|| Duration::from_nanos(credit))
    })
}

/// Charge the bucket for a fast slice that actually ran for `spent`.
pub fn charge_fast_expire_slice(spent: Duration) {
    ACTIVE_EXPIRE.with(|s| {
        let spent = u64::try_from(spent.as_nanos()).unwrap_or(u64::MAX);
        s.credit_ns.set(s.credit_ns.get().saturating_sub(spent));
    });
}

/// The database the next fast slice starts at, advancing the rotation.
pub fn next_fast_expire_db(db_count: usize) -> usize {
    ACTIVE_EXPIRE.with(|s| {
        let start = s.next_db.get() % db_count.max(1);
        s.next_db.set((start + 1) % db_count.max(1));
        start
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run `f` on a fresh thread: the bucket is thread-local shard state.
    fn on_fresh_shard_thread<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
        std::thread::spawn(f).join().expect("test thread")
    }

    /// moon#1288: over a long run of on-time 1 ms ticks whose slices are
    /// spent in full, the fast cycle takes EXPIRE_FAST_DUTY_PCT% of wall
    /// time — no more (the cap), and not much less (the drain rate).
    #[test]
    fn the_bucket_grants_the_duty_share_and_no_more() {
        on_fresh_shard_thread(|| {
            note_expire_backlog(true);
            let t0 = Instant::now();
            let mut granted = Duration::ZERO;
            for ms in 0..1_000u64 {
                if let Some(slice) = take_fast_expire_slice(t0 + Duration::from_millis(ms)) {
                    granted += slice;
                    charge_fast_expire_slice(slice);
                }
            }
            let wall = Duration::from_millis(1_000);
            let share = fast_slice_earned(wall);
            assert!(
                granted <= share + EXPIRE_FAST_SLICE_MAX,
                "granted {granted:?} over {wall:?}: above the {EXPIRE_FAST_DUTY_PCT}% cap"
            );
            assert!(
                granted >= share * 9 / 10,
                "granted {granted:?} over {wall:?}: the drain is starved"
            );
        });
    }

    /// A late tick (a stalled or saturated loop) is owed at most ONE
    /// `EXPIRE_FAST_SLICE_MAX` — no single tick may block the shard longer.
    #[test]
    fn a_late_tick_gets_at_most_one_max_slice() {
        on_fresh_shard_thread(|| {
            note_expire_backlog(true);
            let t0 = Instant::now();
            let first = take_fast_expire_slice(t0).expect("first slice");
            assert_eq!(first, fast_slice_earned(Duration::from_millis(1)));
            charge_fast_expire_slice(first);
            let late = take_fast_expire_slice(t0 + Duration::from_secs(3)).expect("late slice");
            assert_eq!(late, EXPIRE_FAST_SLICE_MAX);
        });
    }

    /// A slice that overran its grant leaves the bucket empty: the next
    /// on-time tick is skipped rather than handed a slice on credit.
    #[test]
    fn an_empty_bucket_skips_the_tick() {
        on_fresh_shard_thread(|| {
            note_expire_backlog(true);
            let t0 = Instant::now();
            let s = take_fast_expire_slice(t0).expect("first slice");
            charge_fast_expire_slice(s * 4);
            assert_eq!(take_fast_expire_slice(t0 + Duration::from_micros(10)), None);
        });
    }

    /// The latch: pending until a cycle reports nothing left; clearing it
    /// forgets the refill point so idle time is never banked.
    #[test]
    fn the_backlog_latch_follows_the_cycle_result() {
        on_fresh_shard_thread(|| {
            assert!(!expire_backlog_pending());
            note_expire_backlog(true);
            assert!(expire_backlog_pending());
            note_expire_backlog(true);
            assert!(expire_backlog_pending());
            note_expire_backlog(false);
            assert!(!expire_backlog_pending());
            ACTIVE_EXPIRE.with(|s| assert!(s.last_refill.get().is_none()));
        });
    }

    #[test]
    fn the_db_rotation_visits_every_database() {
        on_fresh_shard_thread(|| {
            let seen: Vec<usize> = (0..6).map(|_| next_fast_expire_db(3)).collect();
            assert_eq!(seen, vec![0, 1, 2, 0, 1, 2]);
            assert_eq!(
                next_fast_expire_db(0),
                0,
                "zero databases must not divide by zero"
            );
        });
    }
}
