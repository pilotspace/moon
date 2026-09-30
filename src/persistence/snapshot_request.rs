//! Ask for a snapshot from inside the server, rate limited (moon#1289).
//!
//! `BGSAVE` is a command and `--save` a rule; some background work needs a
//! snapshot for its own reasons, with nobody at a keyboard. The first is the
//! cold tier without an AOF: a dead spill file the snapshot hold covers
//! (`storage::tiered::snapshot_hold`) is released only by a snapshot that
//! started after it went zero-ref, and nothing else asks for one. The second
//! is moon#1297 (the no-AOF block reclaim): a compaction is committed only by
//! a snapshot that started after it. This module is the shared trigger, so
//! each caller supplies a [`SnapshotReason`] and a spacing and none
//! re-implements the gate.
//!
//! # The gate
//!
//! One process-wide [`SnapshotGate`]: a snapshot is a whole-keyspace write, so
//! two reasons asking within one spacing are served by one snapshot, and a
//! caller on every shard (each shard's sweep asks) cannot start a storm. A
//! request that finds a save already running is [`SnapshotRequest::Busy`] and
//! does NOT consume the slot: the caller asks again on its next tick, and the
//! running save is not evidence that a new one is unwanted. A request the
//! server refuses (no persistence directory) does consume it, so a server
//! that can never snapshot is asked once per spacing, not once per tick.
//!
//! The request goes through `bgsave_start_sharded`, the entry `BGSAVE` and
//! the auto-save use: `SAVE_IN_PROGRESS` and the per-shard fan-in are armed,
//! `LASTSAVE` moves on success, and a `BGSAVE` issued meanwhile is refused as
//! redis refuses it.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::runtime::channel::WatchSender;

/// Why a snapshot was requested. One counter per reason (INFO), so a
/// surprising snapshot names its cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotReason {
    /// A held cold spill file waited for a snapshot to release it
    /// (moon#1289).
    HeldColdFiles,
    /// A no-AOF cold reclaim compaction waited for the snapshot that commits
    /// it (moon#1297).
    ColdReclaim,
}

const REASONS: usize = 2;

impl SnapshotReason {
    const fn index(self) -> usize {
        match self {
            SnapshotReason::HeldColdFiles => 0,
            SnapshotReason::ColdReclaim => 1,
        }
    }
}

/// What [`request`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRequest {
    /// A snapshot was started.
    Started,
    /// The last request is less than one spacing old. Nothing was asked.
    RateLimited,
    /// A save is already running. Nothing was asked; try again later.
    Busy,
    /// The server refused (for instance it has no persistence directory).
    Refused,
}

/// How the start attempt ended, as [`SnapshotGate::try_request`] needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartOutcome {
    Started,
    Busy,
    Refused,
}

/// Time of the last request, on the process clock.
#[derive(Debug)]
pub struct SnapshotGate {
    /// Milliseconds on [`process_ms`]; [`NEVER`] before the first request.
    last_ms: AtomicU64,
}

const NEVER: u64 = u64::MAX;

impl SnapshotGate {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last_ms: AtomicU64::new(NEVER),
        }
    }

    /// Whether a request at `now_ms` clears the spacing after the last one.
    /// Pure, so the boundary is unit tested without a clock.
    #[must_use]
    pub fn spacing_allows(last_ms: u64, now_ms: u64, spacing: Duration) -> bool {
        last_ms == NEVER || now_ms.saturating_sub(last_ms) >= spacing.as_millis() as u64
    }

    /// Take the slot at `now_ms` and run `start`. Exactly one of several
    /// racing callers gets the slot; a [`StartOutcome::Busy`] gives it back.
    pub fn try_request(
        &self,
        now_ms: u64,
        spacing: Duration,
        start: impl FnOnce() -> StartOutcome,
    ) -> SnapshotRequest {
        let last = self.last_ms.load(Ordering::Acquire);
        if !Self::spacing_allows(last, now_ms, spacing) {
            return SnapshotRequest::RateLimited;
        }
        if self
            .last_ms
            .compare_exchange(last, now_ms, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Another shard took the slot between the load and here: its
            // snapshot serves this request too.
            return SnapshotRequest::RateLimited;
        }
        match start() {
            StartOutcome::Started => SnapshotRequest::Started,
            StartOutcome::Busy => {
                let _ = self.last_ms.compare_exchange(
                    now_ms,
                    last,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                SnapshotRequest::Busy
            }
            StartOutcome::Refused => SnapshotRequest::Refused,
        }
    }
}

impl Default for SnapshotGate {
    fn default() -> Self {
        Self::new()
    }
}

static GATE: SnapshotGate = SnapshotGate::new();

/// Snapshots started per reason, process-wide.
static STARTED: [AtomicU64; REASONS] = [AtomicU64::new(0), AtomicU64::new(0)];

fn process_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Snapshots [`request`] started for `reason` (INFO).
#[must_use]
pub fn started(reason: SnapshotReason) -> u64 {
    STARTED[reason.index()].load(Ordering::Relaxed)
}

/// Request a snapshot of every shard for `reason`, at most one per `spacing`
/// across the whole process. Callable from any thread (a shard's tick, the
/// AOF monitor); `snapshot_trigger` is the sender `BGSAVE` uses.
pub fn request(
    snapshot_trigger: &WatchSender<u64>,
    num_shards: usize,
    reason: SnapshotReason,
    spacing: Duration,
) -> SnapshotRequest {
    use crate::command::persistence::{SAVE_IN_PROGRESS, bgsave_start_sharded};
    use crate::protocol::Frame;
    let outcome = GATE.try_request(process_ms(), spacing, || {
        if SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
            return StartOutcome::Busy;
        }
        match bgsave_start_sharded(snapshot_trigger, num_shards) {
            Frame::SimpleString(_) => StartOutcome::Started,
            // Lost the race for `SAVE_IN_PROGRESS` to a BGSAVE or an
            // auto-save: that save is running, ask again later.
            _ if SAVE_IN_PROGRESS.load(Ordering::SeqCst) => StartOutcome::Busy,
            _ => StartOutcome::Refused,
        }
    });
    if outcome == SnapshotRequest::Started {
        STARTED[reason.index()].fetch_add(1, Ordering::Relaxed);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPACING: Duration = Duration::from_secs(10);

    #[test]
    fn the_first_request_is_never_rate_limited() {
        assert!(SnapshotGate::spacing_allows(NEVER, 0, SPACING));
        let gate = SnapshotGate::new();
        assert_eq!(
            gate.try_request(0, SPACING, || StartOutcome::Started),
            SnapshotRequest::Started
        );
    }

    #[test]
    fn a_second_request_inside_the_spacing_starts_nothing() {
        let gate = SnapshotGate::new();
        assert_eq!(
            gate.try_request(1_000, SPACING, || StartOutcome::Started),
            SnapshotRequest::Started
        );
        let mut asked = false;
        for now in [1_000, 5_000, 10_999] {
            assert_eq!(
                gate.try_request(now, SPACING, || {
                    asked = true;
                    StartOutcome::Started
                }),
                SnapshotRequest::RateLimited,
                "at {now} ms"
            );
        }
        assert!(!asked, "a rate limited request must not touch the server");
        assert_eq!(
            gate.try_request(11_000, SPACING, || StartOutcome::Started),
            SnapshotRequest::Started,
            "one spacing after the last request the slot is free again"
        );
    }

    /// A save already running is not a reason to wait a whole spacing.
    #[test]
    fn a_busy_server_does_not_consume_the_slot() {
        let gate = SnapshotGate::new();
        assert_eq!(
            gate.try_request(1_000, SPACING, || StartOutcome::Busy),
            SnapshotRequest::Busy
        );
        assert_eq!(
            gate.try_request(1_001, SPACING, || StartOutcome::Started),
            SnapshotRequest::Started,
            "the next tick may ask again at once"
        );
    }

    /// A server that cannot snapshot is asked once per spacing, not per tick.
    #[test]
    fn a_refusal_consumes_the_slot() {
        let gate = SnapshotGate::new();
        assert_eq!(
            gate.try_request(1_000, SPACING, || StartOutcome::Refused),
            SnapshotRequest::Refused
        );
        assert_eq!(
            gate.try_request(2_000, SPACING, || StartOutcome::Started),
            SnapshotRequest::RateLimited
        );
    }

    /// Every shard's sweep asks: of N racing callers exactly one starts a
    /// snapshot.
    #[test]
    fn racing_callers_start_exactly_one_snapshot() {
        let gate = std::sync::Arc::new(SnapshotGate::new());
        let started = std::sync::Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (gate, started) = (gate.clone(), started.clone());
                std::thread::spawn(move || {
                    gate.try_request(5_000, SPACING, || {
                        started.fetch_add(1, Ordering::SeqCst);
                        StartOutcome::Started
                    })
                })
            })
            .collect();
        let served = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|r| *r == SnapshotRequest::Started)
            .count();
        assert_eq!(served, 1);
        assert_eq!(started.load(Ordering::SeqCst), 1);
    }
}
