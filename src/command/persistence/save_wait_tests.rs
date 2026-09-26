//! Review F4: a save wait is bounded by a STALL, never by the save's length
//! (`save_wait`). On a virtual clock: `sleep` advances it and plays the
//! shards (progress, a save ending), and the wait observes the injected
//! progress counter, not the process-wide one other tests bump.

use std::cell::{Cell, RefCell};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use super::tests::BGSAVE_TEST_LOCK;
use super::*;

const STALL: Duration = Duration::from_millis(SAVE_STALL_MS);
const POLL: Duration = Duration::from_millis(SHUTDOWN_SAVE_POLL_MS);

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

/// Run `shutdown_save_within` to completion. `shards(t, progress)` runs at
/// every poll with the virtual time since the start. Returns the reply and
/// the virtual time the wait took.
fn run(
    patience: Patience<'_>,
    shards: impl Fn(Duration, &Cell<u64>),
) -> (Result<(), Frame>, Duration) {
    let (tx, _rx) = crate::runtime::channel::watch(0u64);
    let start = Instant::now();
    let clock = Cell::new(start);
    let progress = Cell::new(0u64);
    let sleep = |d: Duration| {
        clock.set(clock.get() + d);
        shards(clock.get() - start, &progress);
        std::future::ready(())
    };
    let observe = Observe {
        now: &|| clock.get(),
        progress: &|| progress.get(),
    };
    let mut fut = std::pin::pin!(shutdown_save_within(&tx, 1, sleep, &observe, patience));
    let reply = match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(r) => r,
        Poll::Pending => panic!("every sleep is ready at once"),
    };
    (reply, clock.get() - start)
}

/// Another client's save is running (not one this wait started).
fn someone_elses_save() {
    SAVE_IN_PROGRESS.store(true, Ordering::SeqCst);
    BGSAVE_SHARDS_REMAINING.store(0, Ordering::SeqCst);
}

/// This wait's own save is running (`bgsave_start_sharded` set one shard).
fn own_save_running() -> bool {
    SAVE_IN_PROGRESS.load(Ordering::SeqCst) && BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst) == 1
}

/// A long save that keeps progressing is waited out, however long: here a
/// running save of 45 s and then SHUTDOWN's own of 60 s, with progress every
/// 15 s. Red before (one 20 s deadline): "timed out" at 20 s.
#[test]
fn a_progressing_save_is_waited_out_however_long() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    someone_elses_save();
    let own_since = Cell::new(None);
    let (reply, took) = run(Patience::UntilStalled(STALL), |t, progress| {
        if t.as_millis() % 15_000 == 0 {
            progress.set(progress.get() + 1);
        }
        if BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst) == 0 && t >= secs(45) {
            SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        }
        if own_save_running() {
            let since = own_since.get().unwrap_or(t);
            own_since.set(Some(since));
            if t - since >= secs(60) {
                bgsave_shard_done(true);
            }
        }
    });
    assert!(reply.is_ok(), "{reply:?}");
    assert!(took >= secs(105), "took {took:?}");
}

/// A save that makes no progress for the stall limit fails the SHUTDOWN at
/// that limit (the server stays up), and no save of its own starts.
#[test]
fn a_stalled_save_fails_the_shutdown_at_the_stall_limit() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    someone_elses_save();
    let (reply, took) = run(Patience::UntilStalled(STALL), |_, _| {});
    let started_own = BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst) != 0;
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    assert!(
        matches!(&reply, Err(Frame::Error(e)) if e.as_ref().ends_with(b"no progress for 20 s, check logs")),
        "{reply:?}"
    );
    assert!(
        STALL <= took && took <= STALL + POLL,
        "gave up after {took:?}"
    );
    assert!(!started_own, "a stalled wait started a save of its own");
    assert_eq!(shutdown_abort::pending_for_test(), 0);
}

/// SHUTDOWN's OWN save that completes in the poll crossing the stall limit
/// counts: the snapshot is durable.
#[test]
fn an_own_save_completing_at_the_stall_limit_counts() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    let (reply, took) = run(Patience::UntilStalled(STALL), |t, _| {
        if own_save_running() && t >= STALL {
            bgsave_shard_done(true);
        }
    });
    assert!(reply.is_ok(), "{reply:?}");
    assert!(took <= STALL + POLL, "{took:?}");
}

/// A signal's final save never gives up (review F4: the stop used to be
/// dropped after 20 s). The running save stalls for 50 s: the wait reports
/// each stall period (20 s, 40 s), keeps the stop armed, and once that save
/// ends runs its own and succeeds.
#[test]
fn a_signal_save_reports_stalls_and_never_gives_up() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    someone_elses_save();
    let reports = RefCell::new(Vec::new());
    let report = |stalled: Duration| reports.borrow_mut().push(stalled);
    let (reply, took) = run(Patience::Forever(STALL, &report), |t, _| {
        if BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst) == 0 && t >= secs(50) {
            SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        }
        if own_save_running() {
            bgsave_shard_done(true);
        }
    });
    assert!(reply.is_ok(), "{reply:?}");
    assert!(took >= secs(50), "{took:?}");
    assert_eq!(*reports.borrow(), vec![STALL, 2 * STALL]);
}
