//! Loom model for the AOF everysec fsync hand-off (moon#1266 Option 3).
//!
//! The state machine is the REAL one: `src/persistence/aof/fsync_handoff.rs`
//! is compiled into this test crate through `#[path]`, and under `cfg(loom)`
//! that file takes loom's atomics. Around it, the thread bodies mirror
//! `src/persistence/aof/fsync_agent.rs`: the writer claims (`try_begin`),
//! sends a job through the agent's depth-1 queue (a mutex-guarded slot here,
//! flume's bounded(1) channel in production — both order the send before the
//! receive) or `abort`s when the agent is gone; the agent takes the job,
//! "fsyncs" and `finish`es.
//!
//! The kernel part that matters is modeled too: an `fdatasync` covers every
//! byte written to the inode before it runs, whichever fd it is issued on.
//! The writer's `write(2)` is a Relaxed store of the bytes-written count; the
//! agent's fsync reads it.
//!
//! Verified under every interleaving of one writer and one agent:
//!   1. at most one job exists at a time — the depth-1 queue is never full
//!      when the writer sends (so a claimed fsync is never dropped);
//!   2. every byte written before a successful claim is covered by that
//!      job's fsync (the writes happen-before the agent's fsync);
//!   3. a claim made after an fsync settled sees that fsync's outcome
//!      (`last_failed`), so a failure is never missed for the retry;
//!   4. a deadline that finds an fsync in flight is postponed, never lost:
//!      the writer's retry loop always gets its claim once the agent settles;
//!   5. an aborted claim (agent gone) frees the state for the inline fallback.
//!
//! Run with (from the repo root, in a target dir of its own):
//!   cargo rustc --release --test loom_aof_fsync_agent -- --cfg loom
//! then run the built test binary (see tests/loom_wal_sync_agent.rs for why
//! not `RUSTFLAGS`). Without `--cfg loom` the same models run repeatedly on
//! std threads as a smoke test.

#![allow(unexpected_cfgs)]

#[path = "../src/persistence/aof/fsync_handoff.rs"]
#[allow(dead_code)]
mod fsync_handoff;

use fsync_handoff::{Begin, FsyncHandoff};

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Mutex};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::{Arc, Mutex};

/// The writer, the agent's depth-1 queue and the file.
struct World {
    handoff: FsyncHandoff,
    /// The agent's queue: the bytes written when the job was sent.
    slot: Mutex<Option<u64>>,
    /// Bytes the writer has `write(2)`-n (the page cache).
    written: AtomicU64,
    /// The most any fsync covered.
    durable: AtomicU64,
}

impl World {
    fn new() -> Self {
        Self {
            handoff: FsyncHandoff::new(),
            slot: Mutex::new(None),
            written: AtomicU64::new(0),
            durable: AtomicU64::new(0),
        }
    }

    /// `write(2)` of one batch.
    fn write(&self) {
        let w = self.written.load(Ordering::Relaxed);
        self.written.store(w + 1, Ordering::Relaxed);
    }

    /// `EverysecSync::dispatch` after `Claim::Owned`: send the job.
    fn send(&self) {
        let mut slot = self.slot.lock().unwrap();
        assert!(slot.is_none(), "(1) the depth-1 queue was full at a send");
        *slot = Some(self.written.load(Ordering::Relaxed));
    }

    /// The writer's everysec deadline, retried on every wake until it owns
    /// the fsync (a postponed deadline stays armed). Returns the
    /// `last_failed` it saw when it got the claim.
    fn deadline(&self) -> bool {
        loop {
            match self.handoff.try_begin() {
                Begin::Owned => {
                    let saw_failed = self.handoff.last_failed();
                    self.send();
                    return saw_failed;
                }
                Begin::Postponed => yield_now(),
            }
        }
    }

    /// One iteration of the agent loop; `ok` is the fsync's outcome.
    /// Returns false when the queue was empty.
    fn agent_step(&self, ok: bool) -> bool {
        let job = self.slot.lock().unwrap().take();
        let Some(upto) = job else {
            return false;
        };
        // The fsync: covers everything written to the inode by now.
        let covered = self.written.load(Ordering::Relaxed);
        assert!(
            covered >= upto,
            "(2) the fsync missed bytes written before its claim ({covered} < {upto})"
        );
        if ok {
            self.durable.fetch_max(covered, Ordering::Relaxed);
        }
        self.handoff.finish(ok);
        true
    }
}

#[cfg(loom)]
fn yield_now() {
    loom::thread::yield_now();
}

#[cfg(not(loom))]
fn yield_now() {
    std::thread::yield_now();
}

#[cfg(loom)]
fn thread_spawn<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> loom::thread::JoinHandle<T> {
    loom::thread::spawn(f)
}

#[cfg(not(loom))]
fn thread_spawn<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(f)
}

/// Two deadlines, the first fsync FAILS: the second claim must see the
/// failure (3.), every byte written before each claim is covered (2.), the
/// second deadline is only postponed, never lost (4.).
fn model_two_deadlines_first_fsync_fails() {
    let w = Arc::new(World::new());
    let agent = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            let mut done = 0;
            let mut outcome = false; // the first fsync fails
            while done < 2 {
                if w.agent_step(outcome) {
                    done += 1;
                    outcome = true;
                } else {
                    yield_now();
                }
            }
        })
    };
    let writer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            w.write();
            let first_saw_failed = w.deadline();
            w.write(); // written while the first fsync may be in flight
            let second_saw_failed = w.deadline();
            (first_saw_failed, second_saw_failed)
        })
    };
    let (first, second) = writer.join().unwrap();
    agent.join().unwrap();
    assert!(!first, "nothing had failed before the first claim");
    assert!(
        second,
        "(3) the second claim did not see the first fsync's failure"
    );
    assert_eq!(
        w.durable.load(Ordering::Relaxed),
        2,
        "(2)/(4) the second fsync covers both writes"
    );
    assert!(!w.handoff.in_flight());
}

/// A deadline raced against a settling fsync: the claim is either postponed
/// (and retried) or owned, and the writer never sees an fsync in flight
/// after `finish` published IDLE with a stale outcome.
fn model_postponed_deadline_is_retried_not_dropped() {
    let w = Arc::new(World::new());
    // An fsync is already in flight when the writer's next deadline comes.
    w.write();
    assert_eq!(w.handoff.try_begin(), Begin::Owned);
    w.send();
    let agent = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            let mut done = 0;
            while done < 2 {
                if w.agent_step(true) {
                    done += 1;
                } else {
                    yield_now();
                }
            }
        })
    };
    let writer = {
        let w = Arc::clone(&w);
        thread_spawn(move || {
            w.write();
            w.deadline()
        })
    };
    let saw_failed = writer.join().unwrap();
    agent.join().unwrap();
    assert!(!saw_failed);
    assert_eq!(w.durable.load(Ordering::Relaxed), 2);
}

/// The agent is gone: the writer's claim is aborted and the state is free
/// for the inline fallback and every later claim (5.).
fn model_aborted_claim_frees_the_state() {
    let w = World::new();
    assert_eq!(w.handoff.try_begin(), Begin::Owned);
    w.handoff.abort();
    assert!(!w.handoff.in_flight());
    assert_eq!(w.handoff.try_begin(), Begin::Owned);
    assert_eq!(w.handoff.delayed(), 0);
}

#[cfg(loom)]
mod loom_tests {
    use super::*;

    #[test]
    fn loom_two_deadlines_first_fsync_fails() {
        loom::model(model_two_deadlines_first_fsync_fails);
    }

    #[test]
    fn loom_postponed_deadline_is_retried_not_dropped() {
        loom::model(model_postponed_deadline_is_retried_not_dropped);
    }

    #[test]
    fn loom_aborted_claim_frees_the_state() {
        loom::model(model_aborted_claim_frees_the_state);
    }
}

#[cfg(not(loom))]
mod smoke {
    use super::*;

    const ROUNDS: usize = 2_000;

    #[test]
    fn smoke_two_deadlines_first_fsync_fails() {
        for _ in 0..ROUNDS {
            model_two_deadlines_first_fsync_fails();
        }
    }

    #[test]
    fn smoke_postponed_deadline_is_retried_not_dropped() {
        for _ in 0..ROUNDS {
            model_postponed_deadline_is_retried_not_dropped();
        }
    }

    #[test]
    fn smoke_aborted_claim_frees_the_state() {
        model_aborted_claim_frees_the_state();
    }
}
