//! Fuzzy checkpoint protocol (PostgreSQL-style) for the disk-offload path.
//!
//! CheckpointManager is a **state machine** — all I/O (page flush, WAL write,
//! manifest commit, control file update) is performed by the caller (event loop).
//! This keeps the checkpoint logic testable without I/O mocking. The one
//! exception is the data-file fsync of step 4: the manager OWNS the handle of
//! the off-loop helper that performs it ([`DataFileSyncer`]), so a shard can
//! never have more than one outstanding, but the caller decides when to start
//! it and polls it without blocking.
//!
//! Protocol:
//! 1. `begin(current_lsn, dirty_count)` — record REDO_LSN, compute pages_per_tick
//! 2. `advance_tick()` returns `FlushPages(n)` until all dirty pages flushed
//! 3. `advance_tick()` returns `Finalize { redo_lsn }` when all pages done
//! 4. Caller makes every heap data file the flush wrote durable (off-loop
//!    fsync), then writes the WAL checkpoint record, commits the manifest and
//!    updates the control file
//! 5. `complete()` — reset to Idle, reset trigger timer
//!
//! Step 4's data-file fsync is what makes advancing the redo point safe: the
//! control-file update publishes `redo_lsn` as the replay start, and the WAL
//! recycle that follows deletes every record (and every FullPageImage) below
//! it. A page that was `pwrite`n but never fsynced lives only in the page
//! cache of the kernel at that instant, so a power loss after the recycle
//! would roll the page back with nothing left in the WAL to redo it (#452).
//! The manager tracks which files need that fsync
//! ([`note_data_file_written`](CheckpointManager::note_data_file_written)) and
//! whether any page failed to flush
//! ([`note_page_flush_failed`](CheckpointManager::note_page_flush_failed)); the
//! caller refuses to finalize until both are clean.
//!
//! A heap file that no longer exists leaves the pending set instead of
//! holding the redo point back forever
//! ([`forget_data_file`](CheckpointManager::forget_data_file)); the caller
//! counts it ([`note_data_file_vanished`](CheckpointManager::note_data_file_vanished))
//! and says whether that was expected. The proof that dropping it is safe is
//! on `shard::checkpoint_heap_files::stop_waiting_on_vanished_heap_file`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::persistence::data_file_sync::{DataFileSyncer, DataSyncJob, DataSyncPoll};

/// Determines when a checkpoint should be triggered.
pub struct CheckpointTrigger {
    /// Seconds between automatic checkpoints (default 300).
    timeout_secs: u64,
    /// Maximum WAL bytes before forced checkpoint (default 256MB).
    max_wal_bytes: u64,
    /// Fraction of checkpoint interval to spread dirty page flushes (default 0.9).
    completion_fraction: f64,
    /// Epoch-ms timestamp of the last completed checkpoint, read from the
    /// shard's cached clock. Second-granularity timeout does not need
    /// `Instant` precision, and `should_checkpoint` runs every 1ms tick —
    /// the cached read avoids a `clock_gettime` per tick per shard
    /// (issue #373 idle-CPU work).
    last_checkpoint_ms: u64,
}

impl CheckpointTrigger {
    /// Create a new trigger with the given configuration.
    pub fn new(timeout_secs: u64, max_wal_bytes: u64, completion_fraction: f64) -> Self {
        Self {
            timeout_secs,
            max_wal_bytes,
            completion_fraction,
            last_checkpoint_ms: crate::storage::entry::current_time_ms(),
        }
    }

    /// Returns true if a checkpoint should be triggered.
    ///
    /// Triggers on either:
    /// - Elapsed time exceeds `timeout_secs`
    /// - WAL bytes since last checkpoint exceeds `max_wal_bytes`
    pub fn should_checkpoint(&self, wal_bytes_since_checkpoint: u64) -> bool {
        if wal_bytes_since_checkpoint >= self.max_wal_bytes {
            return true;
        }
        // saturating_sub: a wall-clock step backwards (NTP) reads as "no time
        // elapsed" and merely delays the timeout leg; the WAL-bytes leg above
        // is unaffected.
        crate::storage::entry::current_time_ms().saturating_sub(self.last_checkpoint_ms)
            >= self.timeout_secs.saturating_mul(1000)
    }

    /// Reset the trigger timer (called after checkpoint completes).
    pub fn reset(&mut self) {
        self.last_checkpoint_ms = crate::storage::entry::current_time_ms();
    }

    /// Return the timeout in seconds.
    #[inline]
    pub fn timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    /// Return the completion fraction.
    #[inline]
    pub fn completion_fraction(&self) -> f64 {
        self.completion_fraction
    }
}

/// Internal state of the checkpoint protocol.
///
/// All fields are scalar — `Copy` keeps `advance_tick` (1ms tick path)
/// free of `clone()` calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointState {
    /// No checkpoint in progress.
    Idle,
    /// Fuzzy checkpoint in progress: flushing dirty pages spread over time.
    InProgress {
        /// WAL LSN at checkpoint start — the REDO point for recovery.
        redo_lsn: u64,
        /// Total number of dirty pages at checkpoint start.
        dirty_count: usize,
        /// Number of pages flushed so far.
        flushed: usize,
        /// Pages to flush per tick (clamped to [1, 16]).
        pages_per_tick: usize,
    },
    /// All dirty pages flushed, awaiting finalization.
    Finalizing {
        /// WAL LSN at checkpoint start.
        redo_lsn: u64,
    },
}

/// Action returned by `advance_tick()` telling the caller what to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointAction {
    /// No work to do this tick.
    Nothing,
    /// Flush this many dirty pages this tick.
    FlushPages(usize),
    /// All pages flushed — finalize: write WAL checkpoint record, commit manifest,
    /// update control file.
    Finalize {
        /// The REDO LSN recorded at checkpoint start.
        redo_lsn: u64,
    },
}

/// State machine for the fuzzy checkpoint protocol.
///
/// Performs no I/O itself — the caller interprets `CheckpointAction` and
/// drives the actual page flushes, WAL writes, and metadata updates. The
/// heap data-file fsync runs on the off-loop helper this manager owns.
pub struct CheckpointManager {
    state: CheckpointState,
    trigger: CheckpointTrigger,
    /// Backoff deadline for the Finalize step (prod-hardening #13). When a
    /// finalize attempt fails, the state stays `Finalizing`, so without this
    /// the next 1ms tick re-runs finalize (re-appending a WAL Checkpoint
    /// record) immediately — flooding the WAL every millisecond under a
    /// sustained failure. `None` = ready now; `Some(t)` = the next attempt is
    /// gated until `t`.
    finalize_retry_at: Option<Instant>,
    /// Consecutive failed finalize attempts, driving exponential backoff.
    finalize_attempts: u32,
    /// Heap data files that a flush has `pwrite`n pages into and that no
    /// successful fsync has covered yet: `file_id` -> the write generation of
    /// its latest write. Finalize must fsync every one of them before it may
    /// publish a new redo point. The generation lets a sync result clear a
    /// file only if nothing wrote to it after the sync batch was taken.
    unsynced_data_files: BTreeMap<u64, u64>,
    /// Source of write generations (monotonic).
    data_write_generation: u64,
    /// A page flush step (FPI append, WAL durability wait or data `pwrite`)
    /// failed during the current checkpoint: some page that was dirty when
    /// the checkpoint began may be neither on disk nor imaged in the WAL
    /// above `redo_lsn`, so this checkpoint must not finalize as-is.
    page_flush_failed: bool,
    /// Sticky: a data-file fsync returned an error. POSIX leaves the state of
    /// the dirty pages undefined after a failed fsync and a retry can report
    /// success for pages that were already dropped (the fsyncgate class), so
    /// this shard never publishes another redo point — the WAL keeps every
    /// record and recovery replays from the last good checkpoint. Set by
    /// the off-loop fsync helper itself, so a failure is recorded even if
    /// nobody polls that batch again.
    data_sync_poisoned: Arc<AtomicBool>,
    /// The off-loop data-file fsync (at most one batch outstanding).
    data_sync: DataFileSyncer,
    /// Heap files that vanished before the checkpoint could make them
    /// durable (see [`VanishedDataFiles`]).
    vanished_data_files: VanishedDataFiles,
}

/// Heap data files that no longer existed when the checkpoint came to write
/// or fsync them, so it stopped waiting on them. Counted, never silent.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VanishedDataFiles {
    /// The manifest no longer lists the file as live: the cold-tier GC (or
    /// the boot orphan sweep) retired it, which is the expected cause.
    pub retired: u64,
    /// The manifest still lists the file as live: something other than the
    /// GC removed it. Its keys are lost whatever the checkpoint does; this
    /// count is the evidence.
    pub still_registered: u64,
}

impl CheckpointManager {
    /// Create a new CheckpointManager in the Idle state.
    pub fn new(trigger: CheckpointTrigger) -> Self {
        Self {
            state: CheckpointState::Idle,
            trigger,
            finalize_retry_at: None,
            finalize_attempts: 0,
            unsynced_data_files: BTreeMap::new(),
            data_write_generation: 0,
            page_flush_failed: false,
            data_sync_poisoned: Arc::new(AtomicBool::new(false)),
            data_sync: DataFileSyncer::new(),
            vanished_data_files: VanishedDataFiles::default(),
        }
    }

    /// Replace the data-file fsync primitive (tests inject hanging or
    /// failing variants).
    #[cfg(test)]
    pub fn set_data_sync_fn(&mut self, f: crate::persistence::data_file_sync::DataSyncFn) {
        self.data_sync.set_sync_fn(f);
    }

    /// Record that a flush `pwrite`d a page into heap file `file_id`; the
    /// file joins the set Finalize must fsync before advancing the redo
    /// point.
    pub fn note_data_file_written(&mut self, file_id: u64) {
        self.data_write_generation += 1;
        self.unsynced_data_files
            .insert(file_id, self.data_write_generation);
    }

    /// Heap data files written since the last successful data-file fsync, as
    /// `(file_id, generation)` in ascending `file_id` order.
    pub fn unsynced_data_files(&self) -> Vec<(u64, u64)> {
        self.unsynced_data_files
            .iter()
            .map(|(&file_id, &generation)| (file_id, generation))
            .collect()
    }

    /// Heap file `file_id` is durable as of write `generation`: drop it from
    /// the pending set unless it was written again since (a newer
    /// generation), in which case that newer write still needs an fsync.
    pub fn note_data_file_synced(&mut self, file_id: u64, generation: u64) {
        if self.unsynced_data_files.get(&file_id) == Some(&generation) {
            self.unsynced_data_files.remove(&file_id);
        }
    }

    /// Heap file `file_id` no longer exists: no fsync can reach the inode
    /// that was written, so stop waiting on it. Returns whether it was
    /// pending.
    pub fn forget_data_file(&mut self, file_id: u64) -> bool {
        self.unsynced_data_files.remove(&file_id).is_some()
    }

    /// Count a heap file the checkpoint stopped waiting on because it no
    /// longer exists; `still_registered` = the manifest still lists it live.
    pub fn note_data_file_vanished(&mut self, still_registered: bool) {
        if still_registered {
            self.vanished_data_files.still_registered += 1;
        } else {
            self.vanished_data_files.retired += 1;
        }
    }

    /// Heap files that vanished before the checkpoint could make them
    /// durable, by cause.
    #[inline]
    pub fn vanished_data_files(&self) -> VanishedDataFiles {
        self.vanished_data_files
    }

    /// Start fsyncing `jobs` off the shard thread. Never blocks; refuses
    /// while a batch is outstanding (at most one helper per shard).
    pub fn start_data_sync(&mut self, jobs: Vec<DataSyncJob>) -> std::io::Result<()> {
        let poison = Arc::clone(&self.data_sync_poisoned);
        self.data_sync.start(jobs, poison)
    }

    /// Observe the outstanding data-file fsync batch without blocking.
    pub fn poll_data_sync(&mut self, warn_after: Duration) -> DataSyncPoll {
        self.data_sync.poll(warn_after)
    }

    /// Whether a data-file fsync batch is outstanding (or its report waits
    /// to be polled).
    #[inline]
    pub fn data_sync_busy(&self) -> bool {
        self.data_sync.is_busy()
    }

    /// Data-file fsync helper threads started over this manager's lifetime.
    #[inline]
    pub fn data_sync_helpers_started(&self) -> u64 {
        self.data_sync.helpers_started()
    }

    /// Times the shard thread BLOCKED on an outstanding data-file fsync
    /// batch ([`Self::wait_data_sync`]); only a shutdown checkpoint may.
    #[inline]
    pub fn data_sync_blocking_waits(&self) -> u64 {
        self.data_sync.blocking_waits()
    }

    /// BLOCK for the outstanding data-file fsync batch, for at most what is
    /// left of `budget` measured from the batch's start. Only for the forced
    /// (synchronous) checkpoint. `false` = still outstanding.
    pub fn wait_data_sync(&mut self, budget: Duration) -> bool {
        self.data_sync.wait(budget)
    }

    /// Record that a page flush step failed this checkpoint (see
    /// `page_flush_failed`).
    pub fn note_page_flush_failed(&mut self) {
        self.page_flush_failed = true;
    }

    /// Whether a page flush step failed since the checkpoint's flush phase
    /// (re)started.
    #[inline]
    pub fn page_flush_failed(&self) -> bool {
        self.page_flush_failed
    }

    /// Re-run the flush phase of the CURRENT checkpoint over the pages that
    /// are still dirty, keeping its `redo_lsn`. Used when a page failed to
    /// flush: the pages that failed are still dirty (and still FPI-pending),
    /// so flushing them again completes the checkpoint's contract without
    /// giving up the redo point it began with. No-op while idle.
    pub fn restart_flush(&mut self, dirty_count: usize) {
        let redo_lsn = match self.state {
            CheckpointState::Idle => return,
            CheckpointState::InProgress { redo_lsn, .. }
            | CheckpointState::Finalizing { redo_lsn } => redo_lsn,
        };
        self.page_flush_failed = false;
        self.state = self.flush_state(redo_lsn, dirty_count);
    }

    /// Handle for the off-loop data-file sync to poison this manager on an
    /// fsync error (see `data_sync_poisoned`).
    pub fn data_sync_poison(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.data_sync_poisoned)
    }

    /// Whether a data-file fsync ever failed on this shard.
    #[inline]
    pub fn is_data_sync_poisoned(&self) -> bool {
        self.data_sync_poisoned.load(Ordering::Acquire)
    }

    /// The state a flush phase starts in for `dirty_count` pages.
    fn flush_state(&self, redo_lsn: u64, dirty_count: usize) -> CheckpointState {
        // If no dirty pages, go straight to Finalizing (still need WAL record + manifest)
        if dirty_count == 0 {
            return CheckpointState::Finalizing { redo_lsn };
        }
        // Compute how many ticks we have to spread the page flushes over.
        // ticks = timeout_secs * completion_fraction * 1000 (since tick is 1ms)
        let ticks =
            (self.trigger.timeout_secs as f64 * self.trigger.completion_fraction * 1000.0) as usize;
        let pages_per_tick = (dirty_count / ticks.max(1)).clamp(1, 16);
        CheckpointState::InProgress {
            redo_lsn,
            dirty_count,
            flushed: 0,
            pages_per_tick,
        }
    }

    /// Whether the Finalize step may be attempted at `now` (prod-hardening
    /// #13). Returns `true` when no backoff is pending or the backoff window
    /// has elapsed. The caller checks this BEFORE doing any finalize I/O
    /// (notably the WAL checkpoint-record append) so a stuck finalize retries
    /// on a bounded schedule instead of every tick.
    #[inline]
    pub fn finalize_ready(&self, now: Instant) -> bool {
        self.finalize_retry_at.map_or(true, |t| now >= t)
    }

    /// Record a failed finalize attempt and arm an exponential backoff
    /// (50ms, 100ms, … capped at 5s) before the next attempt is allowed.
    pub fn note_finalize_failed(&mut self, now: Instant) {
        self.finalize_attempts = self.finalize_attempts.saturating_add(1);
        // 50ms << (attempts-1), capped at 5s.
        let shift = (self.finalize_attempts - 1).min(7);
        let backoff =
            std::time::Duration::from_millis(50u64 << shift).min(std::time::Duration::from_secs(5));
        self.finalize_retry_at = Some(now + backoff);
    }

    /// Clear any finalize backoff (called on success / when leaving Finalizing).
    #[inline]
    fn reset_finalize_backoff(&mut self) {
        self.finalize_retry_at = None;
        self.finalize_attempts = 0;
    }

    /// Begin a new checkpoint.
    ///
    /// Records the REDO LSN and computes `pages_per_tick` based on the number
    /// of dirty pages and the target completion fraction of the checkpoint interval.
    ///
    /// Returns `true` if the checkpoint was started, `false` if one is already in progress.
    pub fn begin(&mut self, current_lsn: u64, dirty_count: usize) -> bool {
        if self.state != CheckpointState::Idle {
            return false;
        }
        self.page_flush_failed = false;
        self.state = self.flush_state(current_lsn, dirty_count);
        true
    }

    /// Advance the checkpoint by one tick.
    ///
    /// Returns the action the caller should take:
    /// - `Nothing` — checkpoint is idle
    /// - `FlushPages(n)` — flush n dirty pages
    /// - `Finalize { redo_lsn }` — all pages done, write WAL checkpoint record
    pub fn advance_tick(&mut self) -> CheckpointAction {
        match self.state {
            CheckpointState::Idle => CheckpointAction::Nothing,
            CheckpointState::InProgress {
                redo_lsn,
                dirty_count,
                flushed,
                pages_per_tick,
            } => {
                let new_flushed = flushed + pages_per_tick;
                if new_flushed >= dirty_count {
                    // All pages will be flushed — transition to Finalizing
                    self.state = CheckpointState::Finalizing { redo_lsn };
                    // Flush remaining pages
                    let remaining = dirty_count - flushed;
                    CheckpointAction::FlushPages(remaining)
                } else {
                    self.state = CheckpointState::InProgress {
                        redo_lsn,
                        dirty_count,
                        flushed: new_flushed,
                        pages_per_tick,
                    };
                    CheckpointAction::FlushPages(pages_per_tick)
                }
            }
            CheckpointState::Finalizing { redo_lsn } => CheckpointAction::Finalize { redo_lsn },
        }
    }

    /// Complete the checkpoint, resetting to Idle and resetting the trigger timer.
    ///
    /// Called by the event loop after WAL checkpoint record, manifest commit,
    /// and control file update are all done.
    pub fn complete(&mut self) {
        self.state = CheckpointState::Idle;
        self.page_flush_failed = false;
        self.trigger.reset();
        self.reset_finalize_backoff();
    }

    /// Force-begin a checkpoint regardless of trigger conditions.
    ///
    /// Used by BGSAVE and graceful shutdown to ensure a clean checkpoint
    /// even when the normal time/WAL-size triggers haven't fired.
    /// Returns `true` if started, `false` if one is already active.
    pub fn force_begin(&mut self, current_lsn: u64, dirty_count: usize) -> bool {
        self.begin(current_lsn, dirty_count)
    }

    /// Returns true if a checkpoint is currently in progress.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.state != CheckpointState::Idle
    }

    /// Return a reference to the trigger for checking should_checkpoint.
    #[inline]
    pub fn trigger(&self) -> &CheckpointTrigger {
        &self.trigger
    }

    /// Return a reference to the current state (for testing/debugging).
    #[inline]
    pub fn state(&self) -> &CheckpointState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_trigger(timeout_secs: u64, max_wal_bytes: u64, completion: f64) -> CheckpointTrigger {
        CheckpointTrigger::new(timeout_secs, max_wal_bytes, completion)
    }

    /// A page flush failure re-runs the flush phase of the SAME checkpoint:
    /// the redo point it began with is kept, the failure flag clears.
    #[test]
    fn restart_flush_keeps_the_redo_point() {
        let mut mgr = CheckpointManager::new(make_trigger(300, u64::MAX, 0.9));
        assert!(mgr.begin(42, 1));
        assert_eq!(mgr.advance_tick(), CheckpointAction::FlushPages(1));
        mgr.note_page_flush_failed();
        assert!(mgr.page_flush_failed());
        assert_eq!(
            mgr.advance_tick(),
            CheckpointAction::Finalize { redo_lsn: 42 }
        );

        mgr.restart_flush(1);
        assert!(!mgr.page_flush_failed());
        assert_eq!(mgr.advance_tick(), CheckpointAction::FlushPages(1));
        assert_eq!(
            mgr.advance_tick(),
            CheckpointAction::Finalize { redo_lsn: 42 }
        );

        // Nothing left dirty: straight back to Finalizing, same redo point.
        mgr.restart_flush(0);
        assert_eq!(
            mgr.advance_tick(),
            CheckpointAction::Finalize { redo_lsn: 42 }
        );

        mgr.complete();
        mgr.restart_flush(5);
        assert_eq!(
            mgr.advance_tick(),
            CheckpointAction::Nothing,
            "no-op while idle"
        );
    }

    /// Written heap files stay pending until a sync covering their latest
    /// write is reported: a file written again after the sync batch was
    /// taken stays pending, and so does a file the batch never covered.
    #[test]
    fn unsynced_data_files_clear_only_what_was_synced() {
        let mut mgr = CheckpointManager::new(make_trigger(300, u64::MAX, 0.9));
        mgr.note_data_file_written(3);
        mgr.note_data_file_written(1);
        mgr.note_data_file_written(3);
        let taken = mgr.unsynced_data_files();
        assert_eq!(taken, vec![(1, 2), (3, 3)]);
        mgr.note_data_file_written(2);
        mgr.note_data_file_written(1); // written again after the batch
        for &(file_id, generation) in &taken {
            mgr.note_data_file_synced(file_id, generation);
        }
        assert_eq!(mgr.unsynced_data_files(), vec![(1, 5), (2, 4)]);

        assert!(mgr.forget_data_file(2));
        assert!(!mgr.forget_data_file(2));
        assert_eq!(mgr.unsynced_data_files(), vec![(1, 5)]);
        mgr.note_data_file_vanished(false);
        mgr.note_data_file_vanished(true);
        mgr.note_data_file_vanished(false);
        assert_eq!(
            mgr.vanished_data_files(),
            VanishedDataFiles {
                retired: 2,
                still_registered: 1
            }
        );
        assert!(!mgr.is_data_sync_poisoned());
        mgr.data_sync_poison()
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(mgr.is_data_sync_poisoned());
    }

    #[test]
    fn test_checkpoint_trigger_timeout() {
        let trigger = CheckpointTrigger {
            timeout_secs: 0, // Immediate trigger
            max_wal_bytes: u64::MAX,
            completion_fraction: 0.9,
            last_checkpoint_ms: crate::storage::entry::current_time_ms().saturating_sub(1000),
        };
        assert!(trigger.should_checkpoint(0));
    }

    #[test]
    fn test_checkpoint_trigger_wal_size() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        // Below threshold
        assert!(!trigger.should_checkpoint(100));
        // At threshold
        assert!(trigger.should_checkpoint(256 * 1024 * 1024));
        // Above threshold
        assert!(trigger.should_checkpoint(256 * 1024 * 1024 + 1));
    }

    #[test]
    fn test_checkpoint_trigger_no_trigger() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        // Just created, well within timeout, low WAL bytes
        assert!(!trigger.should_checkpoint(1024));
    }

    #[test]
    fn test_checkpoint_begin_sets_redo_lsn() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        assert!(mgr.begin(100, 1000));
        match mgr.state() {
            CheckpointState::InProgress {
                redo_lsn,
                dirty_count,
                flushed,
                ..
            } => {
                assert_eq!(*redo_lsn, 100);
                assert_eq!(*dirty_count, 1000);
                assert_eq!(*flushed, 0);
            }
            _ => panic!("expected InProgress state"),
        }
    }

    #[test]
    fn test_checkpoint_pages_per_tick() {
        // dirty=1000, timeout=300s, completion=0.9
        // ticks = 300 * 0.9 * 1000 = 270000
        // pages_per_tick = (1000 / 270000).clamp(1, 16) = 1
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        mgr.begin(100, 1000);
        match mgr.state() {
            CheckpointState::InProgress { pages_per_tick, .. } => {
                assert_eq!(*pages_per_tick, 1);
            }
            _ => panic!("expected InProgress state"),
        }

        // Large dirty count: dirty=1_000_000, timeout=10s, completion=0.9
        // ticks = 10 * 0.9 * 1000 = 9000
        // pages_per_tick = (1_000_000 / 9000).clamp(1, 16) = 16 (capped)
        let trigger2 = make_trigger(10, 256 * 1024 * 1024, 0.9);
        let mut mgr2 = CheckpointManager::new(trigger2);
        mgr2.begin(200, 1_000_000);
        match mgr2.state() {
            CheckpointState::InProgress { pages_per_tick, .. } => {
                assert_eq!(*pages_per_tick, 16);
            }
            _ => panic!("expected InProgress state"),
        }
    }

    #[test]
    fn test_finalize_backoff_gates_retries() {
        // Prod-hardening #13: after a failed finalize, finalize_ready must
        // return false until the exponential backoff window elapses, so the
        // caller does not re-append a WAL checkpoint record every tick.
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        let t0 = Instant::now();
        // No failures yet → always ready.
        assert!(mgr.finalize_ready(t0));

        // First failure arms a 50ms backoff.
        mgr.note_finalize_failed(t0);
        assert!(!mgr.finalize_ready(t0), "immediately after failure: gated");
        assert!(
            !mgr.finalize_ready(t0 + Duration::from_millis(49)),
            "still within the 50ms window"
        );
        assert!(
            mgr.finalize_ready(t0 + Duration::from_millis(50)),
            "ready once the 50ms window elapses"
        );

        // Second consecutive failure doubles the backoff to 100ms.
        let t1 = t0 + Duration::from_millis(50);
        mgr.note_finalize_failed(t1);
        assert!(!mgr.finalize_ready(t1 + Duration::from_millis(99)));
        assert!(mgr.finalize_ready(t1 + Duration::from_millis(100)));

        // complete() clears the backoff so the next checkpoint starts clean.
        mgr.complete();
        assert!(mgr.finalize_ready(t1));
    }

    #[test]
    fn test_finalize_backoff_caps_at_5s() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);
        let t = Instant::now();
        // Many consecutive failures must saturate at the 5s cap, not overflow.
        for _ in 0..40 {
            mgr.note_finalize_failed(t);
        }
        assert!(!mgr.finalize_ready(t + Duration::from_millis(4999)));
        assert!(mgr.finalize_ready(t + Duration::from_secs(5)));
    }

    #[test]
    fn test_checkpoint_advance_flush_then_finalize() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        // 5 dirty pages, pages_per_tick will be 1 (5/270000 clamped to 1)
        mgr.begin(42, 5);

        // Advance 4 ticks: each flushes 1 page
        for i in 0..4 {
            let action = mgr.advance_tick();
            assert_eq!(
                action,
                CheckpointAction::FlushPages(1),
                "tick {} should flush 1 page",
                i
            );
        }

        // 5th tick: flush last page AND transition to Finalizing
        let action = mgr.advance_tick();
        assert_eq!(action, CheckpointAction::FlushPages(1));

        // Next tick: should be Finalize
        let action = mgr.advance_tick();
        assert_eq!(action, CheckpointAction::Finalize { redo_lsn: 42 });
    }

    #[test]
    fn test_checkpoint_complete_resets_to_idle() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        // Begin and advance to Finalizing
        mgr.begin(50, 1);
        let _ = mgr.advance_tick(); // flush 1 page -> Finalizing
        let action = mgr.advance_tick();
        assert_eq!(action, CheckpointAction::Finalize { redo_lsn: 50 });

        // Complete
        mgr.complete();
        assert!(!mgr.is_active());
        assert_eq!(*mgr.state(), CheckpointState::Idle);
        assert_eq!(mgr.advance_tick(), CheckpointAction::Nothing);
    }

    #[test]
    fn test_checkpoint_double_begin_rejected() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        assert!(mgr.begin(100, 10));
        assert!(!mgr.begin(200, 20)); // Already in progress
        assert!(mgr.is_active());

        // Original checkpoint state preserved
        match mgr.state() {
            CheckpointState::InProgress { redo_lsn, .. } => {
                assert_eq!(*redo_lsn, 100);
            }
            _ => panic!("expected InProgress"),
        }
    }

    #[test]
    fn test_checkpoint_zero_dirty_pages() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        // Zero dirty pages should go straight to Finalizing
        assert!(mgr.begin(999, 0));
        let action = mgr.advance_tick();
        assert_eq!(action, CheckpointAction::Finalize { redo_lsn: 999 });
    }

    #[test]
    fn test_force_begin_bypasses_trigger() {
        // High timeout + high max_wal_bytes: normal trigger would NOT fire
        let trigger = make_trigger(999_999, u64::MAX, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        // force_begin should start checkpoint regardless
        assert!(mgr.force_begin(100, 10));
        assert!(mgr.is_active());
        match mgr.state() {
            CheckpointState::InProgress {
                redo_lsn,
                dirty_count,
                ..
            } => {
                assert_eq!(*redo_lsn, 100);
                assert_eq!(*dirty_count, 10);
            }
            _ => panic!("expected InProgress state"),
        }

        // Second force_begin should fail (already active)
        assert!(!mgr.force_begin(200, 20));
    }

    #[test]
    fn test_full_checkpoint_cycle() {
        let trigger = make_trigger(300, 256 * 1024 * 1024, 0.9);
        let mut mgr = CheckpointManager::new(trigger);

        // Start idle
        assert!(!mgr.is_active());

        // Begin checkpoint
        assert!(mgr.begin(100, 3));
        assert!(mgr.is_active());

        // Flush all 3 pages (pages_per_tick = 1)
        assert_eq!(mgr.advance_tick(), CheckpointAction::FlushPages(1));
        assert_eq!(mgr.advance_tick(), CheckpointAction::FlushPages(1));
        assert_eq!(mgr.advance_tick(), CheckpointAction::FlushPages(1));

        // Finalize
        assert_eq!(
            mgr.advance_tick(),
            CheckpointAction::Finalize { redo_lsn: 100 }
        );

        // Complete
        mgr.complete();
        assert!(!mgr.is_active());

        // Can start a new checkpoint
        assert!(mgr.begin(200, 1));
        assert!(mgr.is_active());
    }
}
