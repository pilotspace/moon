//! The WAL v3 durability watermark and every decision the writer takes from
//! it (moon#1188, moon#1221 review R2).
//!
//! One off-loop agent thread fsyncs segments for the shard's writer and
//! publishes a monotonic "every LSN `<=` this is on stable storage"
//! watermark; the writer also fsyncs inline on its own thread (no agent, a
//! full agent queue, a rotation over its memory or time bound, shutdown).
//! Both fsync the SAME file description (the agent gets a dup of the
//! writer's fd), and that is what makes the watermark delicate:
//!
//! * **An fsync error is reported once per file description.** Whichever
//!   thread's fsync checks the description's error state first gets the
//!   error and clears it; every later fsync on the description succeeds,
//!   whatever reached the disk ("fsyncgate"). So an inline fsync that
//!   succeeds proves nothing if an agent fsync of the same file could have
//!   consumed an error first — and a retry after a failure proves nothing
//!   at all. Once any fsync failed, the WAL is POISONED for good: nothing is
//!   ever reported durable again ([`Watermark::check`] tests poison first),
//!   a poisoned agent drains its queue without acting, and a pending
//!   rotation fails loudly instead of retrying ([`rotation_step`]).
//! * **An inline publish waits out the agent's in-flight fsyncs.** The agent
//!   counts the fsyncs it starts and settles (outcome stored). The writer
//!   snapshots the started count AFTER its own fsync returned: an agent fsync
//!   that consumed an error before the writer's fsync checked must have
//!   started before that point. Only when every one of them has settled
//!   without poisoning may the writer publish ([`Watermark::try_publish_inline`]).
//!   The kernel orders the two checks under its own lock; the SeqCst
//!   counter operations carry that order into this state machine.
//!
//! This file is compiled into `tests/loom_wal_sync_agent.rs` through
//! `#[path]`, where it takes loom's atomics, so the model checks the decision
//! code that ships rather than a copy. Keep it self-contained: `std` / `loom`
//! only, nothing else from the crate.

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Whether an LSN is durable, as far as the watermark can say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Durability {
    /// An fsync on this WAL failed: no LSN is ever reported durable again.
    Poisoned,
    /// Covered by the watermark.
    Durable,
    /// Not covered yet.
    Pending,
}

/// Outcome of [`Watermark::try_publish_inline`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InlinePublish {
    /// Published.
    Published,
    /// The WAL is poisoned: nothing may be published.
    Poisoned,
    /// An agent fsync that may have consumed an error before the writer's
    /// own fsync checked has not settled yet: publishing now could claim
    /// durability for data whose fsync failed.
    AgentInFlight,
}

/// What a pending segment rotation does next ([`rotation_step`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RotationStep {
    /// The watermark covers the old segment: open the next one.
    OpenOnWatermark,
    /// Not durable yet and within the memory / time bound: keep buffering.
    Wait,
    /// Past the bound (or no agent to wait for): fsync the old segment
    /// inline on the writer's thread, then open the next one.
    InlineFsync,
    /// Poisoned, still within the bound: fail loudly. Never retry the fsync
    /// — after a failure a retry succeeds without proving anything.
    FailPoisoned,
    /// Poisoned and past the bound: open the next segment WITHOUT any
    /// durability claim so memory stays bounded — still an error.
    DegradedOpen,
}

/// The atomic state shared by a WAL v3 writer and its sync agent.
pub(crate) struct Watermark {
    /// Highest LSN known durable. Monotonic (`fetch_max`).
    durable_lsn: AtomicU64,
    /// Set (never cleared) by the first failed fsync, on either thread.
    poisoned: AtomicBool,
    /// Agent fsyncs begun.
    fsyncs_started: AtomicU64,
    /// Agent fsyncs whose outcome (publish or poison) is stored.
    fsyncs_settled: AtomicU64,
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermark {
    pub(crate) fn new() -> Self {
        Self {
            durable_lsn: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            fsyncs_started: AtomicU64::new(0),
            fsyncs_settled: AtomicU64::new(0),
        }
    }

    /// The raw watermark (instrumentation and tests; durability decisions go
    /// through [`Self::check`], which puts the poison first).
    pub(crate) fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Poison the WAL: an fsync on it failed. Permanent.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Is `lsn` durable? The poison is tested FIRST: a watermark published
    /// before the poison became visible must not vouch for anything after it.
    pub(crate) fn check(&self, lsn: u64) -> Durability {
        if self.poisoned.load(Ordering::Acquire) {
            Durability::Poisoned
        } else if self.durable_lsn.load(Ordering::Acquire) >= lsn {
            Durability::Durable
        } else {
            Durability::Pending
        }
    }

    /// Agent: an fsync is about to start. Must precede the fsync call.
    pub(crate) fn agent_fsync_started(&self) {
        self.fsyncs_started.fetch_add(1, Ordering::SeqCst);
    }

    /// Agent: the fsync that covers `upto_lsn` returned `ok`. Stores the
    /// outcome — publish, or poison — and only then counts it settled.
    pub(crate) fn agent_fsync_settled(&self, ok: bool, upto_lsn: u64) {
        if !ok {
            self.poison();
        } else if !self.poisoned.load(Ordering::Acquire) {
            // A poison stored by the writer (its inline fsync got the error
            // this fsync's success is blind to) forbids the publish.
            self.durable_lsn.fetch_max(upto_lsn, Ordering::Release);
        }
        self.fsyncs_settled.fetch_add(1, Ordering::SeqCst);
    }

    /// Writer: how many agent fsyncs have started. Snapshot it AFTER the
    /// writer's own fsync returned, and hand it to
    /// [`Self::try_publish_inline`].
    pub(crate) fn fsyncs_started(&self) -> u64 {
        self.fsyncs_started.load(Ordering::SeqCst)
    }

    /// Writer: its own inline fsync covering `lsn` succeeded, with
    /// `started_before` agent fsyncs begun by then. Publish `lsn` unless the
    /// WAL is poisoned or one of those fsyncs has not settled.
    pub(crate) fn try_publish_inline(&self, lsn: u64, started_before: u64) -> InlinePublish {
        if self.fsyncs_settled.load(Ordering::SeqCst) < started_before {
            return InlinePublish::AgentInFlight;
        }
        if self.poisoned.load(Ordering::Acquire) {
            return InlinePublish::Poisoned;
        }
        self.durable_lsn.fetch_max(lsn, Ordering::Release);
        InlinePublish::Published
    }
}

/// The next step of a pending rotation whose old segment holds every record
/// `<= upto_lsn`.
///
/// `watermark` is the agent's (`None`: no agent), `writer_poisoned` a failed
/// inline fsync on the writer's own thread, `over_bound` the writer's memory
/// or time bound for the buffered appends. Poison is decided first, exactly
/// as in [`Watermark::check`].
pub(crate) fn rotation_step(
    watermark: Option<&Watermark>,
    upto_lsn: u64,
    writer_poisoned: bool,
    over_bound: bool,
) -> RotationStep {
    let poisoned = writer_poisoned || watermark.is_some_and(Watermark::is_poisoned);
    if poisoned {
        return if over_bound {
            RotationStep::DegradedOpen
        } else {
            RotationStep::FailPoisoned
        };
    }
    match watermark {
        Some(w) if w.durable_lsn() >= upto_lsn => RotationStep::OpenOnWatermark,
        Some(_) if !over_bound => RotationStep::Wait,
        // Over the bound, or no agent that could ever publish.
        _ => RotationStep::InlineFsync,
    }
}
