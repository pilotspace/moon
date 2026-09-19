//! Off-loop fsync of the heap data files a checkpoint wrote (#452).
//!
//! Before a checkpoint publishes a new redo point, every heap file it
//! `pwrite`d a page into must be durable: the control-file update makes the
//! redo point the replay start and the WAL below it is recycled, so a page
//! that is only in the kernel's page cache at that instant rolls back on
//! power loss with nothing left to redo it.
//!
//! The fsyncs run on a helper thread, never on the shard thread — the same
//! discipline as the WAL's `WalSyncAgent`, which moves only the fsync off
//! the event loop and publishes completion for the shard to observe. The
//! shard thread starts a batch ([`DataFileSyncer::start`]) and observes the
//! result on a later tick with a non-blocking `try_recv`
//! ([`DataFileSyncer::poll`]). Two properties keep a dead disk from turning
//! into a second outage:
//!
//! - **At most one helper per shard.** A batch is only started when none is
//!   outstanding; a hung fsync leaves exactly one thread stuck, however many
//!   finalize attempts run meanwhile.
//! - **The periodic tick never waits.** `poll` is a `try_recv`. The only
//!   blocking wait ([`DataFileSyncer::wait`]) is for the explicitly
//!   synchronous forced checkpoint, and it is bounded by a budget measured
//!   from the helper's START — so a hung helper can stall its shard for at
//!   most that budget in total, not once per forced checkpoint.
//!
//! The helper thread is detached. It sends its report and exits; nothing
//! joins it, so shutdown never waits on an fsync that will not return (the
//! next boot replays the WAL from the redo point that was never advanced).
//!
//! Per-file outcomes are reported separately because the checkpoint must
//! treat them differently — see [`DataSyncOutcome`].

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The fsync primitive the helper applies to each file (`File::sync_data`
/// in production; tests inject failing or hanging variants).
pub type DataSyncFn = fn(&std::fs::File) -> std::io::Result<()>;

/// One heap file to make durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSyncJob {
    /// The heap file's id (`heap-{file_id:06}.mpf`).
    pub file_id: u64,
    /// The write generation the caller recorded for this file when the
    /// batch was taken, echoed back so a file written again after the batch
    /// started is never cleared by this batch's result.
    pub generation: u64,
    /// Where the file lives.
    pub path: PathBuf,
}

/// What happened to one file of a batch.
#[derive(Debug)]
pub enum DataSyncOutcome {
    /// The fsync returned `Ok`: every byte written to the file before the
    /// batch started is durable.
    Synced,
    /// `open` failed with `NotFound`: the file no longer exists. No fsync
    /// can reach the inode that was written any more; whether that is
    /// expected is the caller's call (it knows whether the file was
    /// retired).
    Vanished,
    /// `open` failed with any other error: nothing was synced and nothing
    /// failed — a later attempt can still prove durability. Retryable.
    OpenFailed(std::io::Error),
    /// The fsync RETURNED an error. After a failed fsync the kernel may have
    /// dropped the dirty pages, and a retry can report success for data
    /// that is gone (fsyncgate), so the helper has already set the poison
    /// flag. Permanent.
    SyncFailed(std::io::Error),
    /// Not attempted: an earlier file's fsync failed and poisoned the shard.
    NotAttempted,
}

/// The helper's report: one outcome per job, in job order.
#[derive(Debug)]
pub struct DataSyncReport {
    /// `(job, outcome)` for every job of the batch.
    pub results: Vec<(DataSyncJob, DataSyncOutcome)>,
}

/// Result of polling the syncer.
#[derive(Debug)]
pub enum DataSyncPoll {
    /// No batch is outstanding.
    Idle,
    /// A batch is outstanding. `overdue` is `Some(outstanding)` exactly once
    /// per batch — on the first poll after it has been outstanding longer
    /// than the threshold passed to [`DataFileSyncer::poll`] — so the caller
    /// can warn about a slow disk once instead of on every tick.
    Pending {
        /// See the variant doc.
        overdue: Option<Duration>,
    },
    /// The batch finished; here is its report.
    Done(DataSyncReport),
    /// The helper exited without a report (it panicked). Nothing is known
    /// to be durable; the caller retries.
    Lost,
}

struct InFlight {
    rx: flume::Receiver<DataSyncReport>,
    started: Instant,
    warned: bool,
}

/// Owner of the (at most one) outstanding data-file fsync batch of a shard.
pub struct DataFileSyncer {
    in_flight: Option<InFlight>,
    /// A report [`Self::wait`] received, handed out by the next `poll`.
    completed: Option<DataSyncReport>,
    sync_fn: DataSyncFn,
    helpers_started: u64,
}

impl Default for DataFileSyncer {
    fn default() -> Self {
        Self::new()
    }
}

impl DataFileSyncer {
    /// A syncer using `File::sync_data`.
    pub fn new() -> Self {
        Self::with_sync_fn(|f| f.sync_data())
    }

    /// A syncer applying `sync_fn` to each file (tests inject failures).
    pub fn with_sync_fn(sync_fn: DataSyncFn) -> Self {
        Self {
            in_flight: None,
            completed: None,
            sync_fn,
            helpers_started: 0,
        }
    }

    /// Replace the fsync primitive (tests only).
    #[cfg(test)]
    pub fn set_sync_fn(&mut self, sync_fn: DataSyncFn) {
        self.sync_fn = sync_fn;
    }

    /// Whether a batch is outstanding or its report is waiting to be polled.
    #[inline]
    pub fn is_busy(&self) -> bool {
        self.in_flight.is_some() || self.completed.is_some()
    }

    /// Helper threads started over this syncer's lifetime.
    #[inline]
    pub fn helpers_started(&self) -> u64 {
        self.helpers_started
    }

    /// Start fsyncing `jobs` on a helper thread. Never blocks.
    ///
    /// `poison` is set BY THE HELPER the moment an fsync returns an error, so
    /// a failure is recorded even if nobody ever polls this batch.
    ///
    /// # Errors
    ///
    /// Refuses while a batch is outstanding (at most one helper per shard),
    /// and reports a failed thread spawn; in both cases nothing started.
    pub fn start(
        &mut self,
        jobs: Vec<DataSyncJob>,
        poison: Arc<AtomicBool>,
    ) -> std::io::Result<()> {
        if self.is_busy() {
            return Err(std::io::Error::other(
                "a data-file fsync batch is already outstanding",
            ));
        }
        let (tx, rx) = flume::bounded::<DataSyncReport>(1);
        let sync_fn = self.sync_fn;
        std::thread::Builder::new()
            .name("moon-ckpt-data-sync".to_string())
            .spawn(move || {
                // Spawned from the pinned shard thread: leave its single-core
                // mask before doing any I/O (same as the WAL sync agent).
                crate::shard::numa::pin_current_aux_thread("moon-ckpt-data-sync");
                let report = run_batch(jobs, sync_fn, &poison);
                // The shard may have been dropped meanwhile; the poison flag
                // above is how an fsync failure still reaches it.
                let _ = tx.send(report);
            })?;
        self.helpers_started += 1;
        self.in_flight = Some(InFlight {
            rx,
            started: Instant::now(),
            warned: false,
        });
        Ok(())
    }

    /// Observe the outstanding batch without blocking (`try_recv`).
    pub fn poll(&mut self, warn_after: Duration) -> DataSyncPoll {
        if let Some(report) = self.completed.take() {
            return DataSyncPoll::Done(report);
        }
        let Some(flight) = self.in_flight.as_mut() else {
            return DataSyncPoll::Idle;
        };
        match flight.rx.try_recv() {
            Ok(report) => {
                self.in_flight = None;
                DataSyncPoll::Done(report)
            }
            Err(flume::TryRecvError::Empty) => {
                let outstanding = flight.started.elapsed();
                let overdue = if !flight.warned && outstanding >= warn_after {
                    flight.warned = true;
                    Some(outstanding)
                } else {
                    None
                };
                DataSyncPoll::Pending { overdue }
            }
            Err(flume::TryRecvError::Disconnected) => {
                self.in_flight = None;
                DataSyncPoll::Lost
            }
        }
    }

    /// BLOCK until the outstanding batch reports, for at most what is left
    /// of `budget` measured from the batch's START. Only for the forced
    /// (synchronous) checkpoint; the periodic tick uses [`Self::poll`].
    ///
    /// Returns `true` when there is nothing left to wait for (no batch, or
    /// its report is ready for the next `poll`), `false` when the budget ran
    /// out with the batch still outstanding.
    pub fn wait(&mut self, budget: Duration) -> bool {
        let Some(flight) = self.in_flight.as_ref() else {
            return true;
        };
        let remaining = budget.saturating_sub(flight.started.elapsed());
        match flight.rx.recv_timeout(remaining) {
            Ok(report) => {
                self.in_flight = None;
                self.completed = Some(report);
                true
            }
            Err(flume::RecvTimeoutError::Timeout) => false,
            // Lost helper: the next poll reports it.
            Err(flume::RecvTimeoutError::Disconnected) => true,
        }
    }
}

/// The helper's body: open and fsync each file, in order.
fn run_batch(jobs: Vec<DataSyncJob>, sync_fn: DataSyncFn, poison: &AtomicBool) -> DataSyncReport {
    let mut results = Vec::with_capacity(jobs.len());
    let mut poisoned = false;
    for job in jobs {
        if poisoned {
            results.push((job, DataSyncOutcome::NotAttempted));
            continue;
        }
        // Write access: Windows' FlushFileBuffers requires it.
        let outcome = match std::fs::OpenOptions::new().write(true).open(&job.path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => DataSyncOutcome::Vanished,
            Err(e) => DataSyncOutcome::OpenFailed(e),
            Ok(file) => match sync_fn(&file) {
                Ok(()) => DataSyncOutcome::Synced,
                Err(e) => {
                    poison.store(true, Ordering::Release);
                    poisoned = true;
                    DataSyncOutcome::SyncFailed(e)
                }
            },
        };
        results.push((job, outcome));
    }
    DataSyncReport { results }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(dir: &std::path::Path, file_id: u64) -> DataSyncJob {
        DataSyncJob {
            file_id,
            generation: file_id * 10,
            path: dir.join(format!("heap-{file_id:06}.mpf")),
        }
    }

    fn poll_until_done(syncer: &mut DataFileSyncer) -> DataSyncReport {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match syncer.poll(Duration::from_secs(60)) {
                DataSyncPoll::Done(report) => return report,
                DataSyncPoll::Pending { .. } => {}
                other => panic!("unexpected poll result {other:?}"),
            }
            assert!(Instant::now() < deadline, "helper did not report");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Each file is reported by what happened to it: synced, vanished
    /// (`NotFound`), or unopenable for another reason (retryable). None of
    /// those poisons.
    #[test]
    fn outcomes_distinguish_synced_vanished_and_unopenable() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("heap-000001.mpf"), b"page").unwrap();
        std::fs::create_dir(tmp.path().join("heap-000003.mpf")).unwrap();
        let poison = Arc::new(AtomicBool::new(false));
        let mut syncer = DataFileSyncer::new();
        syncer
            .start(
                vec![job(tmp.path(), 1), job(tmp.path(), 2), job(tmp.path(), 3)],
                Arc::clone(&poison),
            )
            .unwrap();

        let report = poll_until_done(&mut syncer);
        let outcomes: Vec<(u64, u64, &DataSyncOutcome)> = report
            .results
            .iter()
            .map(|(j, o)| (j.file_id, j.generation, o))
            .collect();
        assert!(matches!(outcomes[0], (1, 10, DataSyncOutcome::Synced)));
        assert!(matches!(outcomes[1], (2, 20, DataSyncOutcome::Vanished)));
        assert!(matches!(
            outcomes[2],
            (3, 30, DataSyncOutcome::OpenFailed(_))
        ));
        assert!(!poison.load(Ordering::Acquire));
        assert!(matches!(syncer.poll(Duration::ZERO), DataSyncPoll::Idle));
    }

    /// fsyncgate: an fsync that RETURNS an error poisons at once — set by
    /// the helper itself — and the rest of the batch is not attempted.
    #[test]
    fn an_fsync_error_poisons_and_stops_the_batch() {
        let tmp = tempfile::tempdir().unwrap();
        for id in [1, 2] {
            std::fs::write(tmp.path().join(format!("heap-{id:06}.mpf")), b"p").unwrap();
        }
        let poison = Arc::new(AtomicBool::new(false));
        let mut syncer = DataFileSyncer::with_sync_fn(|_| Err(std::io::Error::other("EIO")));
        syncer
            .start(
                vec![job(tmp.path(), 1), job(tmp.path(), 2)],
                Arc::clone(&poison),
            )
            .unwrap();
        let report = poll_until_done(&mut syncer);
        assert!(matches!(
            report.results[0].1,
            DataSyncOutcome::SyncFailed(_)
        ));
        assert!(matches!(report.results[1].1, DataSyncOutcome::NotAttempted));
        assert!(poison.load(Ordering::Acquire));
    }

    /// At most one helper: a second batch is refused while the first is
    /// outstanding, and again while its report waits to be polled.
    #[test]
    fn a_second_batch_is_refused_while_one_is_outstanding() {
        let tmp = tempfile::tempdir().unwrap();
        let poison = Arc::new(AtomicBool::new(false));
        let mut syncer = DataFileSyncer::new();
        syncer
            .start(vec![job(tmp.path(), 1)], Arc::clone(&poison))
            .unwrap();
        assert!(
            syncer
                .start(vec![job(tmp.path(), 2)], Arc::clone(&poison))
                .is_err()
        );
        assert!(syncer.wait(Duration::from_secs(10)));
        assert!(syncer.is_busy(), "the report still waits to be polled");
        assert!(
            syncer
                .start(vec![job(tmp.path(), 2)], Arc::clone(&poison))
                .is_err()
        );
        assert!(matches!(syncer.poll(Duration::ZERO), DataSyncPoll::Done(_)));
        assert!(!syncer.is_busy());
        syncer.start(vec![job(tmp.path(), 2)], poison).unwrap();
        assert_eq!(syncer.helpers_started(), 2);
    }

    static SLOW_RELEASED: AtomicBool = AtomicBool::new(false);

    fn slow_sync(_f: &std::fs::File) -> std::io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !SLOW_RELEASED.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        Ok(())
    }

    /// `poll` never blocks and flags an overdue batch exactly once; `wait`
    /// is bounded by a budget measured from the batch's start, so a second
    /// wait on the same hung batch returns at once.
    #[test]
    fn poll_is_non_blocking_and_wait_is_bounded_from_the_batch_start() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("heap-000001.mpf"), b"p").unwrap();
        let mut syncer = DataFileSyncer::with_sync_fn(slow_sync);
        syncer
            .start(vec![job(tmp.path(), 1)], Arc::new(AtomicBool::new(false)))
            .unwrap();

        let t = Instant::now();
        assert!(matches!(
            syncer.poll(Duration::from_secs(60)),
            DataSyncPoll::Pending { overdue: None }
        ));
        assert!(t.elapsed() < Duration::from_millis(250), "poll blocked");

        assert!(!syncer.wait(Duration::from_millis(600)));
        let t = Instant::now();
        assert!(
            !syncer.wait(Duration::from_millis(600)),
            "budget already spent"
        );
        assert!(
            t.elapsed() < Duration::from_millis(250),
            "a second wait on the same batch must not wait again"
        );
        assert!(matches!(
            syncer.poll(Duration::ZERO),
            DataSyncPoll::Pending { overdue: Some(_) }
        ));
        assert!(matches!(
            syncer.poll(Duration::ZERO),
            DataSyncPoll::Pending { overdue: None }
        ));

        SLOW_RELEASED.store(true, Ordering::Release);
        assert!(matches!(
            poll_until_done(&mut syncer).results[0].1,
            DataSyncOutcome::Synced
        ));
        assert_eq!(syncer.helpers_started(), 1);
    }
}
