//! The hand-off between an AOF writer and its everysec fsync agent
//! (moon#1266 Option 3).
//!
//! The writer thread appends with `write(2)`; once per second under
//! `appendfsync everysec` it hands an `fdatasync` of its file to a
//! per-writer agent thread instead of running it inline, so a slow disk no
//! longer stops the writer from draining its channel (redis's
//! `BIO_AOF_FSYNC` model). At most ONE fsync is in flight per writer: when
//! the deadline comes round while the previous fsync is still running, the
//! writer does not queue a second one — the hand-off is *postponed* (counted
//! in INFO `aof_delayed_fsync`) and retried on the writer's next wake. The
//! writer keeps writing meanwhile; this is where moon differs from redis,
//! which postpones the WRITE for up to 2 s instead (so a process crash
//! there can lose those 2 s; here it cannot).
//!
//! The protocol is one atomic word:
//!
//! ```text
//!            writer: try_begin (CAS)              agent: finish(ok)
//!   IDLE ──────────────────────────────▶ IN_FLIGHT ─────────────────▶ IDLE
//!     ▲                                      │
//!     └──────── writer: abort (the agent is gone; the writer fsyncs
//!                                inline instead — never dropped)
//! ```
//!
//! * `try_begin` is the only way into `IN_FLIGHT`, and only the writer calls
//!   it: a job is sent to the agent only after a successful `try_begin`, and
//!   the state returns to `IDLE` only once the agent has taken that job and
//!   settled it (or the writer took it back with `abort`). So at most one job
//!   exists at a time and the agent's queue of depth 1 can never be full.
//! * `finish` stores the outcome BEFORE releasing `IDLE` (Release), and
//!   `try_begin` acquires: a writer that sees `IDLE` also sees the outcome of
//!   the fsync that ended, so `last_failed` is never stale by one fsync.
//! * Every byte the writer wrote before a successful `try_begin` is covered
//!   by that job's fsync: the writer's `write(2)`s happen-before the job send
//!   on its own thread, the send happens-before the agent's receive, and
//!   `fdatasync` covers the inode's dirty pages whichever fd it is issued on.
//!
//! This file is compiled into `tests/loom_aof_fsync_agent.rs` through
//! `#[path]`, where it takes loom's atomics, so the model checks the code
//! that ships. Keep it self-contained: `std` / `loom` only.

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

const IDLE: u8 = 0;
const IN_FLIGHT: u8 = 1;

/// Outcome of [`FsyncHandoff::try_begin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Begin {
    /// The caller owns the next fsync: send the job (or [`FsyncHandoff::abort`]).
    Owned,
    /// The previous fsync is still running: postponed, counted in
    /// [`FsyncHandoff::delayed`]. Retry on a later wake.
    Postponed,
}

/// Shared state between one AOF writer and its fsync agent.
pub(crate) struct FsyncHandoff {
    state: AtomicU8,
    /// Outcome of the most recent SETTLED fsync (`true` = it failed).
    last_failed: AtomicBool,
    /// Hand-offs postponed because an fsync was still in flight.
    delayed: AtomicU64,
    /// Fsyncs settled by the agent (ok or failed).
    settled: AtomicU64,
}

impl FsyncHandoff {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(IDLE),
            last_failed: AtomicBool::new(false),
            delayed: AtomicU64::new(0),
            settled: AtomicU64::new(0),
        }
    }

    /// Writer: claim the next fsync, or learn that one is still running.
    pub(crate) fn try_begin(&self) -> Begin {
        match self
            .state
            .compare_exchange(IDLE, IN_FLIGHT, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Begin::Owned,
            Err(_) => {
                self.delayed.fetch_add(1, Ordering::Relaxed);
                Begin::Postponed
            }
        }
    }

    /// Writer: give back a claim whose job never reached the agent (the
    /// agent is gone). The caller then fsyncs inline.
    pub(crate) fn abort(&self) {
        self.state.store(IDLE, Ordering::Release);
    }

    /// Agent: the fsync of the job it took has returned.
    pub(crate) fn finish(&self, ok: bool) {
        self.last_failed.store(!ok, Ordering::Release);
        self.settled.fetch_add(1, Ordering::Relaxed);
        // Release: the outcome above is visible to whoever acquires IDLE.
        self.state.store(IDLE, Ordering::Release);
    }

    /// True while an fsync is in flight.
    #[allow(dead_code)] // read by the unit tests and the loom model only
    pub(crate) fn in_flight(&self) -> bool {
        self.state.load(Ordering::Acquire) == IN_FLIGHT
    }

    /// The most recent settled fsync failed (the writer retries it at its
    /// next deadline even with nothing new written).
    pub(crate) fn last_failed(&self) -> bool {
        self.last_failed.load(Ordering::Acquire)
    }

    /// Hand-offs postponed so far.
    #[allow(dead_code)] // read by the unit tests and the loom model only
    pub(crate) fn delayed(&self) -> u64 {
        self.delayed.load(Ordering::Relaxed)
    }

    /// Fsyncs the agent has settled so far.
    #[allow(dead_code)] // read by the unit tests and the loom model only
    pub(crate) fn settled(&self) -> u64 {
        self.settled.load(Ordering::Relaxed)
    }
}
