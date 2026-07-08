//! Off-loop fsync agent for the WAL v3 writer.
//!
//! `WalWriterV3::flush_sync()` runs `fdatasync` synchronously on the shard
//! event-loop thread — every fsync stalls the shard (no SPSC drain, no conn
//! I/O, no CDC fan-out until the disk acks). Measured on GCE pd
//! (tmp/WALV3-OFFLOOP-FSYNC.md §8): 10–16 ms max probe stalls once per
//! second under `appendfsync everysec` with real WAL bytes, and −20% RPS /
//! ~2× tail under `always`.
//!
//! Design (spec §3): the writer keeps encode/buffer/page-cache-write/rotate
//! on the shard thread; ONLY the fsync moves. A per-shard `std::thread`
//! receives fd-dup'd sync requests over a bounded flume channel and
//! publishes a monotonic durable-LSN watermark after each successful
//! `fdatasync`:
//!
//! - `WalWriterV3::request_sync()` — non-blocking; channel-full falls back
//!   to an inline fsync (backpressure never drops a durability request).
//! - `WalWriterV3::wait_durable(lsn, timeout)` — blocking; used ONLY by the
//!   two checkpoint ordering invariants (log-before-data, WAL-before-
//!   manifest) and shutdown.
//!
//! fd-dup correctness: the dup'd fd shares the file description, and the
//! shard thread's `write_all` happens-before the `try_clone()` on the same
//! thread — the agent's fsync therefore covers every byte written before
//! the request. Rotation is safe because `rotate_segment` fsyncs the old
//! segment inline before switching files.
//!
//! Failure policy: an fsync error POISONS the agent permanently (POSIX
//! leaves post-error fsync semantics undefined — fail loud, PR #211
//! precedent). Poisoned agents fail every subsequent `request_sync` /
//! `wait_durable`; the checkpoint protocol then refuses to advance
//! `redo_lsn`, so no data-loss window opens silently.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

/// One durability request: fsync `file`, then publish `upto_lsn`.
pub(crate) struct SyncRequest {
    /// Dup'd fd of the segment to sync (shares the file description with
    /// the writer's handle, so all previously written bytes are covered).
    pub(crate) file: std::fs::File,
    /// Watermark value to publish after a successful fsync.
    pub(crate) upto_lsn: u64,
}

/// Shared state between the shard thread and the agent thread.
///
/// This is the atomic state machine loom-modeled in
/// `tests/loom_wal_sync_agent.rs` — keep transitions in sync with the model.
pub(crate) struct SyncShared {
    /// Highest LSN known durable. Monotonic (fetch_max publish).
    pub(crate) durable_lsn: AtomicU64,
    /// Set (never cleared) on the first fsync error.
    pub(crate) poisoned: AtomicBool,
    /// Wakes `wait_durable` after each watermark publish / poison.
    pub(crate) mutex: Mutex<()>,
    pub(crate) condvar: Condvar,
}

impl SyncShared {
    fn new() -> Self {
        Self {
            durable_lsn: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    /// Publish `lsn` as durable (monotonic max) and wake waiters.
    pub(crate) fn publish(&self, lsn: u64) {
        self.durable_lsn.fetch_max(lsn, Ordering::Release);
        let _g = self.mutex.lock();
        self.condvar.notify_all();
    }

    /// Poison the agent and wake waiters so they observe the failure.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        let _g = self.mutex.lock();
        self.condvar.notify_all();
    }
}

/// Handle owned by `WalWriterV3` (shard thread side).
pub(crate) struct WalSyncAgent {
    tx: flume::Sender<SyncRequest>,
    pub(crate) shared: Arc<SyncShared>,
    /// Joined on drop so shutdown never leaks a mid-fsync thread.
    thread: Option<std::thread::JoinHandle<()>>,
}

/// Bounded queue depth. Full ⇒ caller falls back to inline fsync.
const SYNC_QUEUE_DEPTH: usize = 8;

impl WalSyncAgent {
    /// Spawn the agent thread with the real `fdatasync` backend.
    ///
    /// `Err` when the OS refuses a thread — the caller falls back to
    /// inline fsync (current behavior) and logs once.
    pub(crate) fn spawn(shard_id: usize) -> std::io::Result<Self> {
        Self::spawn_with_backend(shard_id, |file| file.sync_data())
    }

    /// Spawn with an injectable fsync backend (tests gate/fail it).
    pub(crate) fn spawn_with_backend<F>(shard_id: usize, backend: F) -> std::io::Result<Self>
    where
        F: Fn(&std::fs::File) -> std::io::Result<()> + Send + 'static,
    {
        let (tx, rx) = flume::bounded::<SyncRequest>(SYNC_QUEUE_DEPTH);
        let shared = Arc::new(SyncShared::new());
        let shared_agent = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name(format!("moon-wal-sync-{shard_id}"))
            .spawn(move || {
                // Requests arrive in LSN order (single producer); fetch_max
                // in publish() guards the watermark even if that ever
                // changes.
                while let Ok(req) = rx.recv() {
                    if shared_agent.poisoned.load(Ordering::Acquire) {
                        // Drain without acting: post-error fsync semantics
                        // are undefined; durability can no longer be
                        // promised on this WAL.
                        continue;
                    }
                    match backend(&req.file) {
                        Ok(()) => shared_agent.publish(req.upto_lsn),
                        Err(e) => {
                            tracing::error!(
                                shard_id,
                                upto_lsn = req.upto_lsn,
                                "WAL v3 off-loop fsync FAILED — agent poisoned, \
                                 durability can no longer be guaranteed: {e}"
                            );
                            shared_agent.poison();
                        }
                    }
                }
                // Channel disconnected: writer dropped — exit.
            })?;
        Ok(Self {
            tx,
            shared,
            thread: Some(thread),
        })
    }

    /// Non-blocking enqueue. `Err(req)` hands the request back when the
    /// queue is full or the agent thread is gone — the caller MUST fsync
    /// inline (a durability request is never dropped).
    pub(crate) fn try_send(&self, req: SyncRequest) -> Result<(), SyncRequest> {
        if self.shared.poisoned.load(Ordering::Acquire) {
            // Poisoned: inline fallback would also be a lie (see module
            // docs) — surface via the poisoned check at the call site.
            return Err(req);
        }
        self.tx.try_send(req).map_err(|e| match e {
            flume::TrySendError::Full(r) | flume::TrySendError::Disconnected(r) => r,
        })
    }

    pub(crate) fn durable_lsn(&self) -> u64 {
        self.shared.durable_lsn.load(Ordering::Acquire)
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.shared.poisoned.load(Ordering::Acquire)
    }

    /// Block until `durable_lsn >= lsn`, the agent poisons, or `timeout`.
    ///
    /// The caller is responsible for having enqueued a sync request that
    /// covers `lsn` (see `WalWriterV3::wait_durable`).
    pub(crate) fn wait_watermark(&self, lsn: u64, timeout: Duration) -> std::io::Result<()> {
        let deadline = Instant::now() + timeout;
        let mut guard = self.shared.mutex.lock();
        loop {
            // Check under the lock: publish() takes the lock before
            // notify_all, so a watermark stored before we locked is
            // visible here — no lost-wakeup window.
            if self.shared.durable_lsn.load(Ordering::Acquire) >= lsn {
                return Ok(());
            }
            if self.shared.poisoned.load(Ordering::Acquire) {
                return Err(std::io::Error::other(
                    "WAL v3 sync agent poisoned by a prior fsync failure",
                ));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("WAL v3 wait_durable({lsn}) timed out"),
                ));
            }
            self.shared
                .condvar
                .wait_until(&mut guard, now + (deadline - now));
        }
    }
}

impl Drop for WalSyncAgent {
    fn drop(&mut self) {
        // Close the channel so the thread's recv() loop exits, then join —
        // never leak a thread mid-fsync at shutdown.
        // (self.tx is dropped as part of Self; explicitly drop first so the
        // join below cannot deadlock on a still-open channel.)
        let (dead_tx, _) = flume::bounded(0);
        self.tx = dead_tx;
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn temp_file() -> std::fs::File {
        tempfile::tempfile().expect("tempfile")
    }

    /// Gate that lets tests hold fsync open and observe call counts.
    struct Gate {
        allowed: Mutex<bool>,
        cv: Condvar,
        calls: AtomicUsize,
    }

    impl Gate {
        fn new(open: bool) -> Arc<Self> {
            Arc::new(Self {
                allowed: Mutex::new(open),
                cv: Condvar::new(),
                calls: AtomicUsize::new(0),
            })
        }
        fn open(&self) {
            *self.allowed.lock() = true;
            self.cv.notify_all();
        }
        fn backend(
            self: &Arc<Self>,
        ) -> impl Fn(&std::fs::File) -> std::io::Result<()> + Send + use<> {
            let gate = Arc::clone(self);
            move |_f| {
                gate.calls.fetch_add(1, Ordering::SeqCst);
                let mut allowed = gate.allowed.lock();
                while !*allowed {
                    gate.cv.wait(&mut allowed);
                }
                Ok(())
            }
        }
    }

    #[test]
    fn test_durable_lsn_advances_only_after_fsync() {
        let gate = Gate::new(false);
        let agent = WalSyncAgent::spawn_with_backend(0, gate.backend()).expect("spawn agent");
        agent
            .try_send(SyncRequest {
                file: temp_file(),
                upto_lsn: 7,
            })
            .unwrap_or_else(|_| panic!("queue accepts first request"));

        // fsync is gated: the watermark must NOT advance.
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(agent.durable_lsn(), 0, "watermark advanced before fsync");

        gate.open();
        agent
            .wait_watermark(7, Duration::from_secs(5))
            .expect("watermark reaches 7 after fsync released");
        assert!(agent.durable_lsn() >= 7);
    }

    #[test]
    fn test_wait_watermark_blocks_then_returns() {
        let gate = Gate::new(false);
        let agent =
            Arc::new(WalSyncAgent::spawn_with_backend(0, gate.backend()).expect("spawn agent"));
        agent
            .try_send(SyncRequest {
                file: temp_file(),
                upto_lsn: 3,
            })
            .unwrap_or_else(|_| panic!("queue accepts request"));

        let waiter = {
            let agent = Arc::clone(&agent);
            std::thread::spawn(move || agent.wait_watermark(3, Duration::from_secs(5)))
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished(), "wait returned before fsync");
        gate.open();
        waiter.join().unwrap().expect("wait_watermark Ok");
    }

    #[test]
    fn test_wait_watermark_timeout_errs() {
        let gate = Gate::new(false); // never opened
        let agent = WalSyncAgent::spawn_with_backend(0, gate.backend()).expect("spawn agent");
        agent
            .try_send(SyncRequest {
                file: temp_file(),
                upto_lsn: 1,
            })
            .unwrap_or_else(|_| panic!("queue accepts request"));
        let err = agent
            .wait_watermark(1, Duration::from_millis(100))
            .expect_err("gated fsync must time out");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        gate.open(); // release so Drop's join doesn't hang
    }

    #[test]
    fn test_poisoned_agent_fails_loud() {
        let agent =
            WalSyncAgent::spawn_with_backend(0, |_f| Err(std::io::Error::other("disk gone")))
                .expect("spawn agent");
        agent
            .try_send(SyncRequest {
                file: temp_file(),
                upto_lsn: 1,
            })
            .unwrap_or_else(|_| panic!("queue accepts request"));

        // wait_watermark must observe the poison, not hang until timeout.
        let err = agent
            .wait_watermark(1, Duration::from_secs(5))
            .expect_err("poisoned agent must fail wait");
        assert_ne!(
            err.kind(),
            std::io::ErrorKind::TimedOut,
            "failed via poison, not timeout"
        );
        assert!(agent.is_poisoned());

        // Subsequent requests are refused (caller handles loudly).
        let refused = agent.try_send(SyncRequest {
            file: temp_file(),
            upto_lsn: 2,
        });
        assert!(refused.is_err(), "poisoned agent must refuse new requests");
        assert_eq!(agent.durable_lsn(), 0, "no watermark after poison");
    }

    #[test]
    fn test_try_send_backpressure_hands_request_back() {
        let gate = Gate::new(false);
        let agent = WalSyncAgent::spawn_with_backend(0, gate.backend()).expect("spawn agent");
        // First request parks the agent thread inside the gated fsync; the
        // queue then has SYNC_QUEUE_DEPTH free slots. Fill everything.
        let mut accepted = 0usize;
        for lsn in 1..=(SYNC_QUEUE_DEPTH as u64 + 1) {
            if agent
                .try_send(SyncRequest {
                    file: temp_file(),
                    upto_lsn: lsn,
                })
                .is_ok()
            {
                accepted += 1;
            }
        }
        // Wait until the agent has consumed one (parked in the gate), then
        // one more must bounce.
        while gate.calls.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
        // Queue may briefly have a slot from the consumed request; fill it,
        // tracking the highest LSN actually accepted.
        let mut max_accepted = accepted as u64;
        let mut next_lsn = 50u64;
        while agent
            .try_send(SyncRequest {
                file: temp_file(),
                upto_lsn: next_lsn,
            })
            .is_ok()
        {
            accepted += 1;
            max_accepted = next_lsn;
            next_lsn += 1;
        }
        let bounced = agent.try_send(SyncRequest {
            file: temp_file(),
            upto_lsn: 100,
        });
        assert!(bounced.is_err(), "full queue must hand the request back");
        assert!(accepted >= SYNC_QUEUE_DEPTH);
        gate.open();
        agent
            .wait_watermark(max_accepted, Duration::from_secs(5))
            .expect("all accepted requests drain after gate opens");
    }

    #[test]
    fn test_drop_drains_pending_syncs() {
        let gate = Gate::new(true);
        let calls = Arc::clone(&gate);
        let agent = WalSyncAgent::spawn_with_backend(0, gate.backend()).expect("spawn agent");
        for lsn in 1..=4u64 {
            agent
                .try_send(SyncRequest {
                    file: temp_file(),
                    upto_lsn: lsn,
                })
                .unwrap_or_else(|_| panic!("queue accepts request {lsn}"));
        }
        drop(agent); // Drop closes the channel and JOINS the thread.
        assert_eq!(
            calls.calls.load(Ordering::SeqCst),
            4,
            "every pending request must be fsynced before drop returns"
        );
    }

    #[test]
    fn test_watermark_monotonic_under_reordered_publish() {
        // Even if publishes arrive out of order (future multi-producer),
        // fetch_max keeps the watermark monotonic.
        let shared = SyncShared::new();
        shared.publish(10);
        shared.publish(3);
        assert_eq!(shared.durable_lsn.load(Ordering::Acquire), 10);
    }
}
