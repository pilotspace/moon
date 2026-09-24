//! Loom model for the WAL v3 sync-agent watermark (moon#1188, moon#1221
//! review R2).
//!
//! The decision code is the REAL one: `src/persistence/wal_v3/watermark.rs`
//! is compiled into this test crate through `#[path]`, and under `cfg(loom)`
//! that file takes loom's atomics. Every durability decision below — the
//! poison-first `check`, the agent's publish-or-poison settle, the writer's
//! inline publish, the pending rotation's `rotation_step` — is the function
//! that ships. Around it, `Shared` mirrors `SyncShared`'s monitor
//! (`src/persistence/wal_v3/sync_agent.rs`: every outcome is notified under
//! one mutex, waiters re-check under it) and the thread bodies mirror the
//! agent loop and the writer's inline fallback in `segment.rs`.
//!
//! The kernel is modeled too, because R2 lives there: a file description
//! reports a writeback error to the FIRST fsync that checks it and then
//! clears it (Linux `errseq_t` per `struct file`; the agent's fd is a dup of
//! the writer's, so they share one). A later fsync succeeds whatever reached
//! the disk.
//!
//! Verified under every interleaving of one agent thread and one writer
//! (shard) thread:
//!   1. no lost wakeup — a waiter parked below the watermark is always woken
//!      by a later publish or poison (the publisher notifies under the mutex
//!      the waiter re-checks under);
//!   2. the watermark is monotonic under `fetch_max`;
//!   3. a poison is never missed by a parked waiter, and outranks an
//!      earlier publish: nothing is reported durable after it;
//!   4. a rotation opens the next segment on the watermark only after the
//!      old segment's fsync happened-before (Release publish / Acquire load);
//!   5. a poisoned rotation never opens on the watermark, never retries the
//!      fsync, and no durability check succeeds after the poison — the
//!      writer's poll runs here with its real decision, which is what would
//!      have caught R2 (the PR head retried the fsync inline and published
//!      the watermark over the failed segment);
//!   6. the writer's inline fsync never publishes over an error that an
//!      agent fsync of the same description consumed first — whether it
//!      waits for the agent (`flush_sync`) or not (a rotation past its
//!      bound) — and when the writer's own fsync gets the error, the agent's
//!      error-blind success is never observed as durability;
//!   7. a healthy inline fsync next to a healthy agent always publishes
//!      (the settle it waits for is never a lost wakeup).
//!
//! Run with (from the repo root, in a target dir of its own):
//!   cargo rustc --release --test loom_wal_sync_agent -- --cfg loom
//! then run the built test binary. `RUSTFLAGS="--cfg loom"` would apply the
//! cfg to the `moon` lib as well, where `src/blocking/claim.rs` switches to a
//! loom the lib cannot see (loom is a dev-dependency) — the same reason as
//! tests/loom_blocking_claim.rs. Without `--cfg loom` the same models run
//! repeatedly on std threads as a smoke test.

#![allow(unexpected_cfgs)]

#[path = "../src/persistence/wal_v3/watermark.rs"]
#[allow(dead_code)]
mod watermark;

use watermark::{Durability, InlinePublish, RotationStep, Watermark, rotation_step};

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Condvar, Mutex};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::{Arc, Condvar, Mutex};

const AGENT: usize = 1;
const WRITER: usize = 2;

/// One open file description of a segment (the writer's fd and the agent's
/// dup share it), with the kernel's once-only error reporting.
struct Description {
    /// A writeback error recorded and not yet reported to any fsync.
    unreported_error: Mutex<bool>,
    /// Which thread's fsync got the error (0 none, `AGENT`, `WRITER`).
    reported_to: AtomicUsize,
    /// fsyncs issued after the error was reported: a retry proves nothing.
    fsyncs_after_report: AtomicUsize,
    /// The data the fsync covers really is on disk (4.).
    on_disk: AtomicBool,
}

impl Description {
    fn new(failing: bool) -> Self {
        Self {
            unreported_error: Mutex::new(failing),
            reported_to: AtomicUsize::new(0),
            fsyncs_after_report: AtomicUsize::new(0),
            on_disk: AtomicBool::new(false),
        }
    }

    fn fsync(&self, who: usize) -> Result<(), ()> {
        let mut unreported = self.unreported_error.lock().unwrap();
        if self.reported_to.load(Ordering::Relaxed) != 0 {
            self.fsyncs_after_report.fetch_add(1, Ordering::Relaxed);
        }
        if *unreported {
            *unreported = false;
            self.reported_to.store(who, Ordering::Relaxed);
            return Err(());
        }
        if self.reported_to.load(Ordering::Relaxed) == 0 {
            // No error anywhere: the data is on disk. Relaxed on purpose —
            // only the watermark's Release/Acquire may publish it (4.).
            self.on_disk.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn error_reported(&self) -> bool {
        let _kernel = self.unreported_error.lock().unwrap();
        self.reported_to.load(Ordering::Relaxed) != 0
    }
}

/// Mirror of `SyncShared`: the real `Watermark` plus the monitor every
/// outcome is notified under (std/loom `Condvar` here, parking_lot in
/// production — same monitor semantics).
struct Shared {
    wm: Watermark,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl Shared {
    fn new() -> Self {
        Self {
            wm: Watermark::new(),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    fn notify(&self) {
        let _g = self.mutex.lock().unwrap();
        self.condvar.notify_all();
    }

    /// `SyncShared::poison`.
    fn poison(&self) {
        self.wm.poison();
        self.notify();
    }

    /// One request of the agent loop (`WalSyncAgent::spawn_with_backend`).
    fn agent_fsync(&self, file: &Description, upto: u64) {
        if self.wm.is_poisoned() {
            return; // a poisoned agent drains without acting
        }
        self.wm.agent_fsync_started();
        let result = file.fsync(AGENT);
        self.wm.agent_fsync_settled(result.is_ok(), upto);
        self.notify();
    }

    /// `WalSyncAgent::wait_watermark` without the timeout (a liveness
    /// bound, not part of the correctness argument).
    fn wait(&self, lsn: u64) -> Result<(), ()> {
        let mut guard = self.mutex.lock().unwrap();
        loop {
            match self.wm.check(lsn) {
                Durability::Durable => return Ok(()),
                Durability::Poisoned => return Err(()),
                Durability::Pending => guard = self.condvar.wait(guard).unwrap(),
            }
        }
    }

    /// `SyncShared::publish_after_inline_fsync` (the waiting form, used by
    /// `flush_sync`), without the timeout.
    fn publish_after_inline_fsync(&self, lsn: u64) -> InlinePublish {
        let started = self.wm.fsyncs_started();
        let mut guard = self.mutex.lock().unwrap();
        loop {
            match self.wm.try_publish_inline(lsn, started) {
                InlinePublish::AgentInFlight => guard = self.condvar.wait(guard).unwrap(),
                verdict => {
                    if verdict == InlinePublish::Published {
                        self.condvar.notify_all();
                    }
                    return verdict;
                }
            }
        }
    }

    /// The writer's inline fsync of `file` covering `lsn`
    /// (`WalWriterV3::fsync_inline` + the publish): a failure poisons, a
    /// success publishes only through the real decision. `wait` is
    /// `flush_sync`'s waiting form; otherwise a rotation's non-blocking one
    /// (`try_publish_inline` once; an in-flight agent fsync skips the
    /// publish and leaves it to the agent).
    fn writer_inline_fsync(&self, file: &Description, lsn: u64, wait: bool) -> InlinePublish {
        if file.fsync(WRITER).is_err() {
            self.poison();
            return InlinePublish::Poisoned;
        }
        if wait {
            self.publish_after_inline_fsync(lsn)
        } else {
            let started = self.wm.fsyncs_started();
            self.wm.try_publish_inline(lsn, started)
        }
    }

    /// One non-blocking poll of a pending rotation whose old segment holds
    /// every LSN `<= upto` (`WalWriterV3::poll_pending_rotation`).
    fn writer_poll_rotation(
        &self,
        file: &Description,
        upto: u64,
        over_bound: bool,
    ) -> RotationStep {
        let step = rotation_step(Some(&self.wm), upto, false, over_bound);
        match step {
            RotationStep::OpenOnWatermark => assert!(
                file.on_disk.load(Ordering::Relaxed),
                "next segment opened on the watermark before the old one was on disk"
            ),
            RotationStep::InlineFsync => {
                let _ = self.writer_inline_fsync(file, upto, false);
            }
            RotationStep::Wait | RotationStep::FailPoisoned | RotationStep::DegradedOpen => {}
        }
        step
    }
}

fn model_publish_wakes_waiter() {
    let shared = Arc::new(Shared::new());
    let file = Arc::new(Description::new(false));
    let agent = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.agent_fsync(&file, 5))
    };
    let waiter = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.wait(5))
    };
    agent.join().unwrap();
    assert_eq!(
        waiter.join().unwrap(),
        Ok(()),
        "waiter must observe the publish"
    );
    assert!(shared.wm.durable_lsn() >= 5);
}

fn model_poison_wakes_waiter() {
    let shared = Arc::new(Shared::new());
    let file = Arc::new(Description::new(true));
    let agent = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.agent_fsync(&file, 5))
    };
    let waiter = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.wait(5))
    };
    agent.join().unwrap();
    assert_eq!(
        waiter.join().unwrap(),
        Err(()),
        "waiter must observe the poison"
    );
}

/// Poison outranks the watermark: an LSN a genuine fsync published is not
/// reported durable once a later fsync failed (the module docs' "fail every
/// subsequent wait_durable" — the PR head's fast path checked the
/// watermark first).
fn model_poison_outranks_an_earlier_publish() {
    let shared = Arc::new(Shared::new());
    let agent = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || {
            shared.agent_fsync(&Description::new(false), 5);
            shared.agent_fsync(&Description::new(true), 9);
        })
    };
    let waiter = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.wait(3))
    };
    agent.join().unwrap();
    let _ = waiter.join().unwrap(); // Ok before the poison, Err after
    assert_eq!(shared.wm.check(3), Durability::Poisoned);
    assert_eq!(shared.wait(3), Err(()));
}

fn model_watermark_monotonic() {
    let shared = Arc::new(Shared::new());
    let a = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.agent_fsync(&Description::new(false), 10))
    };
    let b = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.agent_fsync(&Description::new(false), 3))
    };
    a.join().unwrap();
    b.join().unwrap();
    assert_eq!(
        shared.wm.durable_lsn(),
        10,
        "fetch_max publish must never regress the watermark"
    );
}

fn model_rotation_opens_next_segment_only_after_old_is_durable() {
    let shared = Arc::new(Shared::new());
    let file = Arc::new(Description::new(false));
    let agent = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.agent_fsync(&file, 7))
    };
    let writer = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.writer_poll_rotation(&file, 7, false))
    };
    agent.join().unwrap();
    let step = writer.join().unwrap();
    assert!(
        matches!(step, RotationStep::OpenOnWatermark | RotationStep::Wait),
        "healthy agent, within the bound: {step:?}"
    );
    // After the agent finished, a later poll always opens the segment.
    assert_eq!(
        shared.writer_poll_rotation(&file, 7, false),
        RotationStep::OpenOnWatermark
    );
}

/// R2 as reviewed: the agent's fsync of the old segment fails while the
/// writer polls the pending rotation and then asks whether LSN 7 is durable
/// (the checkpoint's log-before-data wait). Never Durable, never an fsync
/// retry, never an open on the watermark.
fn model_poisoned_rotation_never_claims_durability() {
    let shared = Arc::new(Shared::new());
    let file = Arc::new(Description::new(true));
    let agent = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.agent_fsync(&file, 7))
    };
    let writer = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || {
            let step = shared.writer_poll_rotation(&file, 7, false);
            (step, shared.wm.check(7))
        })
    };
    agent.join().unwrap();
    let (step, seen) = writer.join().unwrap();
    assert_ne!(step, RotationStep::OpenOnWatermark);
    assert_ne!(
        seen,
        Durability::Durable,
        "LSN 7 reported durable after its fsync failed"
    );
    // Once the poison is visible: fail loudly, degrade only past the bound,
    // never retry the fsync, never report anything durable.
    assert_eq!(
        shared.writer_poll_rotation(&file, 7, false),
        RotationStep::FailPoisoned
    );
    assert_eq!(
        shared.writer_poll_rotation(&file, 7, true),
        RotationStep::DegradedOpen
    );
    assert_eq!(
        file.fsyncs_after_report.load(Ordering::Relaxed),
        0,
        "the fsync was retried after it failed"
    );
    assert_eq!(shared.wm.check(7), Durability::Poisoned);
    assert_eq!(
        shared.wait(1),
        Err(()),
        "poison is checked before the watermark"
    );
}

/// R2's finer race: the agent's fsync of the old segment is in flight when
/// the writer fsyncs the same description inline (a rotation past its bound,
/// or `flush_sync`). Whichever fsync checks first gets the error; nothing is
/// ever reported durable over it.
fn inline_fallback_never_publishes_over_a_consumed_error(wait: bool) {
    let shared = Arc::new(Shared::new());
    let file = Arc::new(Description::new(true));
    let agent = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.agent_fsync(&file, 7))
    };
    let writer = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || {
            let verdict = shared.writer_inline_fsync(&file, 7, wait);
            (verdict, shared.wm.check(7))
        })
    };
    agent.join().unwrap();
    let (verdict, seen) = writer.join().unwrap();
    assert!(
        file.error_reported(),
        "the model's error is always reported"
    );
    assert_ne!(
        verdict,
        InlinePublish::Published,
        "the writer published over an fsync error"
    );
    assert_ne!(
        seen,
        Durability::Durable,
        "LSN 7 reported durable after its fsync failed"
    );
    assert_eq!(shared.wm.check(7), Durability::Poisoned);
}

fn model_inline_publish_waits_out_the_agent() {
    inline_fallback_never_publishes_over_a_consumed_error(true);
}

fn model_inline_rotation_never_publishes_over_the_agent() {
    inline_fallback_never_publishes_over_a_consumed_error(false);
}

/// A healthy inline fsync next to a healthy agent fsync: the waiting form
/// always publishes (the settle it may wait for is never a lost wakeup).
fn model_inline_publish_completes_when_healthy() {
    let shared = Arc::new(Shared::new());
    let file = Arc::new(Description::new(false));
    let agent = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.agent_fsync(&file, 5))
    };
    let writer = {
        let (shared, file) = (Arc::clone(&shared), Arc::clone(&file));
        thread_spawn(move || shared.writer_inline_fsync(&file, 7, true))
    };
    agent.join().unwrap();
    assert_eq!(writer.join().unwrap(), InlinePublish::Published);
    assert_eq!(shared.wm.check(7), Durability::Durable);
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

#[cfg(loom)]
mod loom_tests {
    use super::*;

    #[test]
    fn loom_publish_wakes_waiter() {
        loom::model(model_publish_wakes_waiter);
    }

    #[test]
    fn loom_poison_wakes_waiter() {
        loom::model(model_poison_wakes_waiter);
    }

    #[test]
    fn loom_poison_outranks_an_earlier_publish() {
        loom::model(model_poison_outranks_an_earlier_publish);
    }

    #[test]
    fn loom_watermark_monotonic() {
        loom::model(model_watermark_monotonic);
    }

    #[test]
    fn loom_rotation_opens_next_segment_only_after_old_is_durable() {
        loom::model(model_rotation_opens_next_segment_only_after_old_is_durable);
    }

    #[test]
    fn loom_poisoned_rotation_never_claims_durability() {
        loom::model(model_poisoned_rotation_never_claims_durability);
    }

    #[test]
    fn loom_inline_publish_waits_out_the_agent() {
        loom::model(model_inline_publish_waits_out_the_agent);
    }

    #[test]
    fn loom_inline_rotation_never_publishes_over_the_agent() {
        loom::model(model_inline_rotation_never_publishes_over_the_agent);
    }

    #[test]
    fn loom_inline_publish_completes_when_healthy() {
        loom::model(model_inline_publish_completes_when_healthy);
    }
}

#[cfg(not(loom))]
mod std_smoke {
    use super::*;

    fn repeat(model: fn()) {
        for _ in 0..100 {
            model();
        }
    }

    #[test]
    fn smoke_publish_wakes_waiter() {
        repeat(model_publish_wakes_waiter);
    }

    #[test]
    fn smoke_poison_wakes_waiter() {
        repeat(model_poison_wakes_waiter);
    }

    #[test]
    fn smoke_poison_outranks_an_earlier_publish() {
        repeat(model_poison_outranks_an_earlier_publish);
    }

    #[test]
    fn smoke_watermark_monotonic() {
        repeat(model_watermark_monotonic);
    }

    #[test]
    fn smoke_rotation_opens_next_segment_only_after_old_is_durable() {
        repeat(model_rotation_opens_next_segment_only_after_old_is_durable);
    }

    #[test]
    fn smoke_poisoned_rotation_never_claims_durability() {
        repeat(model_poisoned_rotation_never_claims_durability);
    }

    #[test]
    fn smoke_inline_publish_waits_out_the_agent() {
        repeat(model_inline_publish_waits_out_the_agent);
    }

    #[test]
    fn smoke_inline_rotation_never_publishes_over_the_agent() {
        repeat(model_inline_rotation_never_publishes_over_the_agent);
    }

    #[test]
    fn smoke_inline_publish_completes_when_healthy() {
        repeat(model_inline_publish_completes_when_healthy);
    }
}
