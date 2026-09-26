//! Waiting for a sharded save: `SHUTDOWN`'s, a signal's final save
//! (moon#1263) and `FLUSHALL`'s (moon#1264).
//!
//! A cooperative snapshot cannot be killed mid-shard, so a caller that needs
//! one waits: first for a save someone else started (an auto-save, a
//! BGSAVE), then for its own. How long is the question review F4 raised.
//! redis has no deadline: `prepareForShutdown` and `flushallCommand` save
//! synchronously, however long that takes, and a signal's stop is never
//! dropped because the dataset is large. What must not happen is a wedged
//! shard holding a caller forever. So the bound is on a STALL — no progress
//! for [`SAVE_STALL_MS`] — never on the save's length:
//!
//! - `SHUTDOWN` and `FLUSHALL` ([`Patience::UntilStalled`]) wait while the
//!   save progresses, and give up after 20 s without progress (the client
//!   gets an error, the server stays up — as when the save fails);
//! - a signal ([`Patience::Forever`]) never gives up: every 20 s without
//!   progress it logs redis's shutdown errors and says the stop stays armed,
//!   and the server exits once the save is on disk. A second SIGINT still
//!   exits at once, `SHUTDOWN ABORT` still cancels.
//!
//! Progress is [`SAVE_PROGRESS`]: every shard bumps it each time its walk
//! advances (`snapshot_cow::note_progress`), and every save start and shard
//! completion bumps it too.
//!
//! Before, one 20 s deadline covered the whole wait (review 5 had made it
//! one deadline instead of one per deferral): a save over ~20 s (about 4 GB
//! at 200 MB/s, less behind a running BGSAVE) failed SHUTDOWN outright, and
//! a SIGTERM was dropped — the server kept running after its save finished.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tracing::error;

use super::{
    BGSAVE_LAST_STATUS, SAVE_ALREADY_IN_PROGRESS_ERR, SAVE_IN_PROGRESS, bgsave_start_sharded,
    shutdown_abort, shutdown_default_should_save,
};
use crate::protocol::Frame;

/// Poll interval while waiting for a save.
pub const SHUTDOWN_SAVE_POLL_MS: u64 = 5;

/// How long a save may make no progress before a wait for it counts as
/// stalled (review F4): long enough for any healthy segment or fsync, short
/// enough that an operator is not left hanging on a wedged shard.
pub const SAVE_STALL_MS: u64 = 20_000;

/// How many times `SHUTDOWN` or `FLUSHALL` defers to a save someone else
/// started before giving up. (A signal defers as often as it takes.)
const SHUTDOWN_SAVE_ATTEMPTS: usize = 3;

/// Every sign of a save moving: a shard's walk advancing, a save starting, a
/// shard finishing. A wait sees a stall as this standing still.
static SAVE_PROGRESS: AtomicU64 = AtomicU64::new(0);

/// A save made progress (called by the shards, and by save start / finish).
#[inline]
pub(crate) fn note_save_progress() {
    SAVE_PROGRESS.fetch_add(1, Ordering::Relaxed);
}

fn save_progress() -> u64 {
    SAVE_PROGRESS.load(Ordering::Relaxed)
}

/// What a wait observes: the time and the saves' progress. Tests pass
/// virtual ones; the server passes [`Observe::REAL`].
pub(super) struct Observe<'a> {
    pub(super) now: &'a dyn Fn() -> Instant,
    pub(super) progress: &'a dyn Fn() -> u64,
}

impl Observe<'static> {
    const REAL: Observe<'static> = Observe {
        now: &Instant::now,
        progress: &save_progress,
    };
}

/// How a wait treats a save that stops making progress.
#[derive(Clone, Copy)]
pub(super) enum Patience<'a> {
    /// Give up once it has made no progress for this long: `SHUTDOWN`,
    /// `FLUSHALL`.
    UntilStalled(Duration),
    /// Never give up (a signal's stop stays armed); run the callback once
    /// per this long without progress, with the time stalled so far.
    Forever(Duration, &'a dyn Fn(Duration)),
}

/// How long the save has made no progress, from one poll to the next.
struct Stall<'a> {
    observe: &'a Observe<'a>,
    seen: u64,
    since: Instant,
    reported: u128,
}

impl<'a> Stall<'a> {
    fn new(observe: &'a Observe<'a>) -> Self {
        Stall {
            observe,
            seen: (observe.progress)(),
            since: (observe.now)(),
            reported: 0,
        }
    }

    /// `Err` once `patience` says the wait is over.
    fn check(&mut self, patience: Patience<'_>) -> Result<(), Stop> {
        let (progress, now) = ((self.observe.progress)(), (self.observe.now)());
        if progress != self.seen {
            (self.seen, self.since, self.reported) = (progress, now, 0);
        }
        let stalled = now.saturating_duration_since(self.since);
        match patience {
            Patience::UntilStalled(limit) if stalled >= limit => Err(Stop::Stalled),
            Patience::UntilStalled(_) => Ok(()),
            Patience::Forever(every, report) => {
                let periods = stalled.as_millis() / every.as_millis().max(1);
                if periods > self.reported {
                    self.reported = periods;
                    report(stalled);
                }
                Ok(())
            }
        }
    }
}

/// `SHUTDOWN`'s save in sharded / monoio mode: a cooperative per-shard
/// BGSAVE, polled to completion with `sleep` (the caller's runtime timer).
///
/// A save already running — an auto-save, or a client's BGSAVE — is not a
/// reason to refuse (moon#1232 review): redis's `prepareForShutdown` kills
/// the saving child and saves synchronously, and SHUTDOWN never fails
/// because a save is in progress. This waits for it and then saves again:
/// the running snapshot predates the writes made since it started, and a
/// SHUTDOWN save must hold them.
///
/// `Err` carries the reply that refuses the SHUTDOWN (the server stays up):
/// a save that failed, one that made no progress for [`SAVE_STALL_MS`], one
/// that could not start (no persistence directory), or a `SHUTDOWN ABORT`.
pub async fn shutdown_save<S, F>(
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
    num_shards: usize,
    sleep: S,
) -> Result<(), Frame>
where
    S: Fn(Duration) -> F,
    F: std::future::Future<Output = ()>,
{
    let patience = Patience::UntilStalled(Duration::from_millis(SAVE_STALL_MS));
    shutdown_save_within(
        snapshot_trigger,
        num_shards,
        sleep,
        &Observe::REAL,
        patience,
    )
    .await
}

/// A signal's final save (moon#1263): [`shutdown_save`] with no stall limit.
/// `stalled` runs once per [`SAVE_STALL_MS`] without progress; the wait goes
/// on until the save is on disk, fails, or `SHUTDOWN ABORT` cancels it.
pub async fn shutdown_save_until_done<S, F>(
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
    num_shards: usize,
    sleep: S,
    stalled: &dyn Fn(Duration),
) -> Result<(), Frame>
where
    S: Fn(Duration) -> F,
    F: std::future::Future<Output = ()>,
{
    let patience = Patience::Forever(Duration::from_millis(SAVE_STALL_MS), stalled);
    shutdown_save_within(
        snapshot_trigger,
        num_shards,
        sleep,
        &Observe::REAL,
        patience,
    )
    .await
}

/// [`shutdown_save`] with what it observes and its patience passed in (tests
/// run it on a virtual clock the `sleep` advances). From entry to return the
/// shutdown is pending: `SHUTDOWN ABORT` cancels it (moon#1264).
pub(super) async fn shutdown_save_within<S, F>(
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
    num_shards: usize,
    sleep: S,
    observe: &Observe<'_>,
    patience: Patience<'_>,
) -> Result<(), Frame>
where
    S: Fn(Duration) -> F,
    F: std::future::Future<Output = ()>,
{
    let pending = shutdown_abort::Pending::enter();
    let wait = Wait {
        observe,
        patience,
        pending: Some(&pending),
    };
    save_and_wait(snapshot_trigger, num_shards, sleep, wait).await?;
    // Review F2: saved — exit, unless an abort took this shutdown first.
    pending.commit()
}

/// Why a wait for a save stopped before it ended.
enum Stop {
    Stalled,
    Aborted,
}

/// How one save wait runs.
struct Wait<'a> {
    observe: &'a Observe<'a>,
    patience: Patience<'a>,
    /// The shutdown this save belongs to, if any: a `SHUTDOWN ABORT` stops
    /// the wait at the next poll.
    pending: Option<&'a shutdown_abort::Pending>,
}

/// Start a sharded save and wait for it: first for a save someone else
/// started, then for this one.
async fn save_and_wait<S, F>(
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
    num_shards: usize,
    sleep: S,
    wait: Wait<'_>,
) -> Result<(), Frame>
where
    S: Fn(Duration) -> F,
    F: std::future::Future<Output = ()>,
{
    let aborted = || wait.pending.is_some_and(shutdown_abort::Pending::aborted);
    let stopped = |why: Stop| match why {
        Stop::Stalled => Frame::Error(Bytes::from_static(
            b"ERR SHUTDOWN failed: background save made no progress for 20 s, check logs",
        )),
        Stop::Aborted => shutdown_abort::aborted_reply(),
    };
    let bounded = matches!(wait.patience, Patience::UntilStalled(_));
    let mut stall = Stall::new(wait.observe);
    let mut deferred = 0;
    loop {
        if aborted() {
            return Err(stopped(Stop::Aborted));
        }
        match bgsave_start_sharded(snapshot_trigger, num_shards) {
            Frame::Error(e) if e.as_ref() == SAVE_ALREADY_IN_PROGRESS_ERR => {
                // Someone else's save is running: wait for it, then save.
                if bounded && deferred == SHUTDOWN_SAVE_ATTEMPTS {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR SHUTDOWN failed: other background saves kept starting, check logs",
                    )));
                }
                deferred += 1;
                wait_for_save(&sleep, &mut stall, wait.patience, &aborted)
                    .await
                    .map_err(stopped)?;
            }
            // This save cannot run at all (no persistence directory):
            // answer what BGSAVE would.
            Frame::Error(e) => return Err(Frame::Error(e)),
            _ => {
                wait_for_save(&sleep, &mut stall, wait.patience, &aborted)
                    .await
                    .map_err(stopped)?;
                return save_outcome();
            }
        }
    }
}

/// Poll until no save is in progress; stop when `patience` runs out or
/// `aborted` says so.
async fn wait_for_save<S, F>(
    sleep: &S,
    stall: &mut Stall<'_>,
    patience: Patience<'_>,
    aborted: &dyn Fn() -> bool,
) -> Result<(), Stop>
where
    S: Fn(Duration) -> F,
    F: std::future::Future<Output = ()>,
{
    let poll = Duration::from_millis(SHUTDOWN_SAVE_POLL_MS);
    while SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
        if aborted() {
            return Err(Stop::Aborted);
        }
        stall.check(patience)?;
        sleep(poll).await;
    }
    Ok(())
}

/// The outcome of the save just waited for.
fn save_outcome() -> Result<(), Frame> {
    if BGSAVE_LAST_STATUS.load(Ordering::Relaxed) {
        Ok(())
    } else {
        Err(Frame::Error(Bytes::from_static(
            b"ERR SHUTDOWN failed: background save error, check logs",
        )))
    }
}

/// `FLUSHALL` with save points saves the empty dataset before it replies
/// (moon#1264), as redis's `flushallCommand` does: `flushAllDataAndResetRDB`
/// runs a synchronous `rdbSave` whenever save points are configured, so the
/// snapshot on disk matches the empty keyspace at once. Without it the
/// previous snapshot stayed until a rule fired, and a crash in between
/// brought every flushed key back.
///
/// Called by the connection that issued the flush once EVERY shard is
/// flushed (a plain `FLUSHALL`, an `EXEC` that ran one, a script that
/// called one), on both runtimes. Waits for a save already running first —
/// the flush aborted it (moon#1228: FLUSHALL fails an unfinished epoch, as
/// redis kills its child) — with `SHUTDOWN`'s stall rule. As in redis, a
/// save that fails is logged and the flush still answers `+OK`; the
/// previous snapshot then stays on disk. `SHUTDOWN ABORT` does not apply.
pub async fn save_after_flushall(
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
    num_shards: usize,
    save_points: Option<&str>,
) {
    use crate::runtime::traits::RuntimeTimer;
    if !shutdown_default_should_save(save_points) {
        return;
    }
    let wait = Wait {
        observe: &Observe::REAL,
        patience: Patience::UntilStalled(Duration::from_millis(SAVE_STALL_MS)),
        pending: None,
    };
    let sleep = crate::runtime::TimerImpl::sleep;
    let outcome = save_and_wait(snapshot_trigger, num_shards, sleep, wait).await;
    if let Err(Frame::Error(e)) = outcome {
        error!(
            "FLUSHALL: saving the flushed dataset failed ({}); the previous snapshot \
             is still on disk until the next successful save",
            String::from_utf8_lossy(&e)
        );
    }
}

/// [`save_after_flushall`] for an `EXEC` whose body flushed: `exec_flushes`
/// is its `(result index, command, db)` list of successful flushes.
pub async fn save_after_txn_flushes(
    exec_flushes: &[(usize, Frame, usize)],
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
    num_shards: usize,
    save_points: Option<&str>,
) {
    let flushall = exec_flushes.iter().any(|(_, command, _)| match command {
        Frame::Array(parts) => matches!(
            parts.first(),
            Some(Frame::BulkString(name)) if name.eq_ignore_ascii_case(b"FLUSHALL")
        ),
        _ => false,
    });
    if flushall {
        save_after_flushall(snapshot_trigger, num_shards, save_points).await;
    }
}
