//! SIGTERM / SIGINT with save points: save the dataset, then exit (moon#1263).
//!
//! `systemctl stop` / `launchctl stop` send SIGTERM; Ctrl-C sends SIGINT. Both
//! used to cancel the server at once, so with RDB-only persistence
//! (`--appendonly no`, save points set) every write since the last automatic
//! save was lost on a routine stop. redis's `prepareForShutdown` treats both
//! signals like a plain `SHUTDOWN`: with save points configured it writes a
//! final snapshot first, and exits only once it is on disk.
//!
//! Here a signal takes the same path as `SHUTDOWN` with no argument
//! ([`super::shutdown_save`]): wait for a save already running (auto-save or
//! BGSAVE), then save every shard, within the one overall deadline
//! ([`super::SHUTDOWN_SAVE_DEADLINE_MS`]), and only then cancel the server.
//! The signal thread never blocks: the save runs on its own thread.
//!
//! As in redis 7.0.15:
//! - no save points: exit at once, no save (unchanged);
//! - the final save fails: log it and KEEP RUNNING ("Error trying to save the
//!   DB, can't exit"), so the operator can fix the disk and stop the server
//!   again; the next SIGTERM / SIGINT retries;
//! - a second SIGINT while the final save runs: exit at once, unsaved
//!   ("You insist... exiting now"); a second SIGTERM is logged.
//!
//! `SHUTDOWN ABORT` cancels a signal's final save like a `SHUTDOWN`'s
//! (moon#1264): the server keeps running.
//!
//! A signal that arrives before [`arm`] (the server is still booting) stops it
//! at once, as before: the dataset is not loaded yet, and a save then would
//! replace a good snapshot with a partial one.

use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{error, info, warn};

use crate::protocol::Frame;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel::WatchSender;

/// The signal that asked the server to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// SIGTERM (`systemctl stop`, `kill`).
    Term,
    /// SIGINT (Ctrl-C).
    Int,
}

impl ShutdownSignal {
    fn name(self) -> &'static str {
        match self {
            ShutdownSignal::Term => "SIGTERM",
            ShutdownSignal::Int => "SIGINT",
        }
    }
}

/// What a signal needs to save: registered once the shards serve.
struct Armed {
    snapshot_trigger: WatchSender<u64>,
    num_shards: usize,
    /// Save points are configured (`--save` with at least one rule).
    save: bool,
    shutdown: CancellationToken,
}

static ARMED: OnceLock<Armed> = OnceLock::new();

/// A signal-initiated final save is running.
static SAVING: AtomicBool = AtomicBool::new(false);

/// Called once by `main.rs` after the shard threads are spawned: from here a
/// shutdown signal saves first when `save_points` has a rule
/// ([`super::shutdown_default_should_save`], the rule plain `SHUTDOWN`
/// follows).
pub fn arm(
    snapshot_trigger: WatchSender<u64>,
    num_shards: usize,
    save_points: Option<&str>,
    shutdown: CancellationToken,
) {
    let _ = ARMED.set(Armed {
        snapshot_trigger,
        num_shards,
        save: super::shutdown_default_should_save(save_points),
        shutdown,
    });
}

/// Handle one SIGTERM / SIGINT. `shutdown` is the server's token, cancelled
/// directly when no save is due. Returns at once: a save runs on a
/// `shutdown-save` thread, which cancels the server once the save is on
/// disk.
pub fn on_signal(signal: ShutdownSignal, shutdown: &CancellationToken) {
    let Some(armed) = ARMED.get().filter(|a| a.save) else {
        info!("{} received: shutting down", signal.name());
        shutdown.cancel();
        return;
    };
    if SAVING.swap(true, Ordering::AcqRel) {
        if signal == ShutdownSignal::Int {
            // redis: "You insist... exiting now."
            error!("SIGINT received again while saving the final snapshot: exiting now, unsaved");
            std::process::exit(1);
        }
        warn!("SIGTERM received while the final snapshot is being saved; still saving");
        return;
    }
    info!(
        "{} received: saving the final snapshot before exiting",
        signal.name()
    );
    let spawned = std::thread::Builder::new()
        .name("shutdown-save".to_string())
        .spawn(move || save_then_exit(armed));
    if let Err(e) = spawned {
        // No thread to save on: save here rather than exit unsaved.
        warn!("cannot spawn the shutdown-save thread ({e}); saving on the signal thread");
        save_then_exit(armed);
    }
}

/// The final save; the server is cancelled only once it is on disk.
fn save_then_exit(armed: &Armed) {
    match save_blocking(&armed.snapshot_trigger, armed.num_shards) {
        Ok(()) => {
            info!("final snapshot saved; shutting down");
            armed.shutdown.cancel();
        }
        // `SHUTDOWN ABORT` (moon#1264) cancelled it: keep running, as redis
        // does after "Shutdown manually aborted".
        Err(Frame::Error(e)) if e.as_ref() == super::SHUTDOWN_ABORTED_ERR => {
            warn!("the signal's shutdown was aborted; the server keeps running");
            SAVING.store(false, Ordering::Release);
        }
        Err(reply) => {
            let why = match &reply {
                Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
                _ => String::new(),
            };
            error!("Error trying to save the DB, can't exit: {why}");
            error!("Errors trying to shut down the server. Check the logs for more information.");
            SAVING.store(false, Ordering::Release);
        }
    }
}

/// [`super::shutdown_save`] on the calling OS thread: its polls sleep that
/// thread (`std::thread::sleep`), so every await resolves at once.
fn save_blocking(snapshot_trigger: &WatchSender<u64>, num_shards: usize) -> Result<(), Frame> {
    block_on_ready(super::shutdown_save(snapshot_trigger, num_shards, |d| {
        std::thread::sleep(d);
        std::future::ready(())
    }))
}

/// Drive a future whose awaits all resolve at once to completion on this
/// thread. A `Pending` (none of this module's futures returns one) yields
/// and polls again rather than spinning hot.
fn block_on_ready<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
        std::thread::yield_now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_ready_drives_a_sleeping_future_to_completion() {
        let started = std::time::Instant::now();
        let out = block_on_ready(async {
            for _ in 0..3 {
                std::thread::sleep(std::time::Duration::from_millis(1));
                std::future::ready(()).await;
            }
            7
        });
        assert_eq!(out, 7);
        assert!(started.elapsed() >= std::time::Duration::from_millis(3));
    }
}
