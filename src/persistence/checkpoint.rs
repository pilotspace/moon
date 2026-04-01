//! Fuzzy checkpoint protocol (PostgreSQL-style) for the disk-offload path.
//!
//! CheckpointManager is a **pure state machine** — all I/O (page flush, WAL write,
//! manifest commit, control file update) is performed by the caller (event loop).
//! This keeps the checkpoint logic testable without I/O mocking.
//!
//! Protocol:
//! 1. `begin(current_lsn, dirty_count)` — record REDO_LSN, compute pages_per_tick
//! 2. `advance_tick()` returns `FlushPages(n)` until all dirty pages flushed
//! 3. `advance_tick()` returns `Finalize { redo_lsn }` when all pages done
//! 4. Caller writes WAL checkpoint record, commits manifest, updates control file
//! 5. `complete()` — reset to Idle, reset trigger timer

use std::time::Instant;

/// Determines when a checkpoint should be triggered.
pub struct CheckpointTrigger {
    /// Seconds between automatic checkpoints (default 300).
    timeout_secs: u64,
    /// Maximum WAL bytes before forced checkpoint (default 256MB).
    max_wal_bytes: u64,
    /// Fraction of checkpoint interval to spread dirty page flushes (default 0.9).
    completion_fraction: f64,
    /// Timestamp of the last completed checkpoint.
    last_checkpoint_time: Instant,
}

impl CheckpointTrigger {
    /// Create a new trigger with the given configuration.
    pub fn new(timeout_secs: u64, max_wal_bytes: u64, completion_fraction: f64) -> Self {
        Self {
            timeout_secs,
            max_wal_bytes,
            completion_fraction,
            last_checkpoint_time: Instant::now(),
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
        self.last_checkpoint_time.elapsed().as_secs() >= self.timeout_secs
    }

    /// Reset the trigger timer (called after checkpoint completes).
    pub fn reset(&mut self) {
        self.last_checkpoint_time = Instant::now();
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Pure state machine for the fuzzy checkpoint protocol.
///
/// Does NOT perform any I/O — the caller interprets `CheckpointAction` and
/// drives the actual page flushes, WAL writes, and metadata updates.
pub struct CheckpointManager {
    state: CheckpointState,
    trigger: CheckpointTrigger,
}

impl CheckpointManager {
    /// Create a new CheckpointManager in the Idle state.
    pub fn new(trigger: CheckpointTrigger) -> Self {
        Self {
            state: CheckpointState::Idle,
            trigger,
        }
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

        // If no dirty pages, go straight to Finalizing (still need WAL record + manifest)
        if dirty_count == 0 {
            self.state = CheckpointState::Finalizing {
                redo_lsn: current_lsn,
            };
            return true;
        }

        // Compute how many ticks we have to spread the page flushes over.
        // ticks = timeout_secs * completion_fraction * 1000 (since tick is 1ms)
        let ticks = (self.trigger.timeout_secs as f64
            * self.trigger.completion_fraction
            * 1000.0) as usize;
        let pages_per_tick = (dirty_count / ticks.max(1)).clamp(1, 16);

        self.state = CheckpointState::InProgress {
            redo_lsn: current_lsn,
            dirty_count,
            flushed: 0,
            pages_per_tick,
        };
        true
    }

    /// Advance the checkpoint by one tick.
    ///
    /// Returns the action the caller should take:
    /// - `Nothing` — checkpoint is idle
    /// - `FlushPages(n)` — flush n dirty pages
    /// - `Finalize { redo_lsn }` — all pages done, write WAL checkpoint record
    pub fn advance_tick(&mut self) -> CheckpointAction {
        match self.state.clone() {
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
            CheckpointState::Finalizing { redo_lsn } => {
                CheckpointAction::Finalize { redo_lsn }
            }
        }
    }

    /// Complete the checkpoint, resetting to Idle and resetting the trigger timer.
    ///
    /// Called by the event loop after WAL checkpoint record, manifest commit,
    /// and control file update are all done.
    pub fn complete(&mut self) {
        self.state = CheckpointState::Idle;
        self.trigger.reset();
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

    fn make_trigger(timeout_secs: u64, max_wal_bytes: u64, completion: f64) -> CheckpointTrigger {
        CheckpointTrigger::new(timeout_secs, max_wal_bytes, completion)
    }

    #[test]
    fn test_checkpoint_trigger_timeout() {
        let trigger = CheckpointTrigger {
            timeout_secs: 0, // Immediate trigger
            max_wal_bytes: u64::MAX,
            completion_fraction: 0.9,
            last_checkpoint_time: Instant::now() - std::time::Duration::from_secs(1),
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
