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
//! # Open transactions (until moon#1300)
//!
//! A snapshot taken while a `TXN` is open keeps that transaction's
//! uncommitted writes, and without an AOF a later `TXN ABORT` has nothing
//! durable to compensate them with: a crash-restart from that image brings
//! the aborted writes back (moon#1300 F3). `BGSAVE`, `SAVE` and the save
//! rules share that flaw and are WS42's to fix; an AUTOMATIC reason
//! ([`SnapshotReason::waits_for_open_txns`]: the held-file snapshot and the
//! no-AOF reclaim's) must not add a new way to hit it, so while any shard
//! has an open transaction [`request`] answers [`SnapshotRequest::TxnOpen`]
//! without touching the gate: the slot is not consumed, and the caller's
//! next tick asks again. The signal is the
//! process-wide published view behind `INFO txn_open`
//! (`transaction::isolation::info`), never another shard's thread-local.
//!
//! That check is only a cheap pre-filter: it is one instant on the
//! requesting thread, and each shard starts its part later, at its own tick
//! (review N1: a busy `TXN` workload slipped into that window). The
//! guarantee is [`txn_round`]: each shard re-checks its own hold table as it
//! starts its part, and one hold abandons the whole round — nothing is
//! published, no held file is released, the slot is given back.
//!
//! The request goes through `bgsave_start_sharded`, the entry `BGSAVE` and
//! the auto-save use: `SAVE_IN_PROGRESS` and the per-shard fan-in are armed,
//! `LASTSAVE` moves on success, and a `BGSAVE` issued meanwhile is refused as
//! redis refuses it.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::runtime::channel::WatchSender;

pub(crate) mod txn_round;

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

    /// Whether this reason is automatic — nobody at a keyboard asked — and so
    /// waits while any transaction is open and registers its round for the
    /// per-shard start check (see the module docs and [`txn_round`]). Every
    /// reason is automatic today: the held-file snapshot (moon#1289) and the
    /// snapshot that commits a no-AOF compaction (moon#1297) both run with
    /// nobody asking, and either one publishing an open `TXN`'s writes would
    /// bring them back after a crash. A caller acting on an explicit request
    /// (none yet) would answer `false`.
    #[must_use]
    pub const fn waits_for_open_txns(self) -> bool {
        match self {
            SnapshotReason::HeldColdFiles | SnapshotReason::ColdReclaim => true,
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
    /// A transaction is open on some shard and the reason is automatic.
    /// Nothing was asked and the slot was not consumed; try again later.
    TxnOpen,
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

impl SnapshotGate {
    /// Give back the slot a request took at `taken_ms`: the next request is
    /// not rate limited by it. No-op when another request took the slot
    /// since (`last_ms` moved on).
    pub fn give_back(&self, taken_ms: u64) {
        let _ = self
            .last_ms
            .compare_exchange(taken_ms, NEVER, Ordering::AcqRel, Ordering::Acquire);
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

/// Requests deferred per reason because a transaction was open. A plain
/// statistics counter (not a state machine).
static DEFERRED: [AtomicU64; REASONS] = [AtomicU64::new(0), AtomicU64::new(0)];

/// Rounds abandoned per reason because a shard held an uncommitted `TXN`
/// write at its start ([`txn_round`]). A statistics counter.
static ABANDONED: [AtomicU64; REASONS] = [AtomicU64::new(0), AtomicU64::new(0)];

/// Is a `TXN` open on any shard? Reads each shard's published view (the
/// `INFO txn_open` source): one short registry lock, once per caller tick.
fn any_txn_open() -> bool {
    crate::transaction::isolation::info(0).open > 0
}

fn process_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Snapshots [`request`] started for `reason` (INFO).
#[must_use]
pub fn started(reason: SnapshotReason) -> u64 {
    STARTED[reason.index()].load(Ordering::Relaxed)
}

/// `CONFIG RESETSTAT`: zero the per-reason started / deferred statistics.
/// The gate's spacing slot is state, not a statistic, and is kept.
pub fn reset_stats() {
    for counter in STARTED
        .iter()
        .chain(DEFERRED.iter())
        .chain(ABANDONED.iter())
    {
        counter.store(0, Ordering::Relaxed);
    }
}

/// Requests for `reason` deferred because a transaction was open (INFO).
#[must_use]
pub fn deferred_for_open_txn(reason: SnapshotReason) -> u64 {
    DEFERRED[reason.index()].load(Ordering::Relaxed)
}

/// Rounds for `reason` abandoned because a shard held an uncommitted `TXN`
/// write as it started its part (INFO).
#[must_use]
pub fn abandoned_for_open_txn(reason: SnapshotReason) -> u64 {
    ABANDONED[reason.index()].load(Ordering::Relaxed)
}

/// A round for `reason` was abandoned ([`txn_round`]): count it, and give
/// back the spacing slot its request took at `slot_ms` so a later sweep asks
/// again. A slot a newer request has taken since is left alone.
fn note_abandoned(reason: SnapshotReason, slot_ms: u64) {
    ABANDONED[reason.index()].fetch_add(1, Ordering::Relaxed);
    GATE.give_back(slot_ms);
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
    use crate::command::persistence::{SAVE_IN_PROGRESS, bgsave_start_sharded_announcing};
    use crate::protocol::Frame;
    let now_ms = process_ms();
    let outcome = gated(&GATE, now_ms, spacing, reason, any_txn_open, || {
        if SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
            return StartOutcome::Busy;
        }
        // Registered before the epoch is broadcast, so no shard can start
        // this round's part without its start check.
        let announce = |epoch: u64| {
            if reason.waits_for_open_txns() {
                txn_round::register(epoch, reason, num_shards, now_ms);
            }
        };
        match bgsave_start_sharded_announcing(snapshot_trigger, num_shards, announce) {
            Frame::SimpleString(_) => StartOutcome::Started,
            // Lost the race for `SAVE_IN_PROGRESS` to a BGSAVE or an
            // auto-save: that save is running, ask again later.
            _ if SAVE_IN_PROGRESS.load(Ordering::SeqCst) => StartOutcome::Busy,
            _ => StartOutcome::Refused,
        }
    });
    match outcome {
        SnapshotRequest::Started => {
            STARTED[reason.index()].fetch_add(1, Ordering::Relaxed);
        }
        SnapshotRequest::TxnOpen => {
            DEFERRED[reason.index()].fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
    outcome
}

/// [`request`]'s decision, with the clock, the transaction probe and the
/// start injected so it is unit tested: an automatic reason with a
/// transaction open answers [`SnapshotRequest::TxnOpen`] before the gate is
/// touched, so the deferral never consumes the spacing slot.
fn gated(
    gate: &SnapshotGate,
    now_ms: u64,
    spacing: Duration,
    reason: SnapshotReason,
    txn_open: impl FnOnce() -> bool,
    start: impl FnOnce() -> StartOutcome,
) -> SnapshotRequest {
    if reason.waits_for_open_txns() && txn_open() {
        return SnapshotRequest::TxnOpen;
    }
    gate.try_request(now_ms, spacing, start)
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

    /// moon#1300 until WS42: an automatic snapshot waits while a TXN is open,
    /// starts nothing, and leaves the slot free for the first tick after the
    /// transaction ends.
    #[test]
    fn an_open_txn_defers_an_automatic_snapshot_without_consuming_the_slot() {
        let gate = SnapshotGate::new();
        let reason = SnapshotReason::HeldColdFiles;
        assert!(reason.waits_for_open_txns());
        let mut asked = false;
        for now in [1_000, 1_001, 2_000] {
            assert_eq!(
                gated(
                    &gate,
                    now,
                    SPACING,
                    reason,
                    || true,
                    || {
                        asked = true;
                        StartOutcome::Started
                    }
                ),
                SnapshotRequest::TxnOpen,
                "at {now} ms"
            );
        }
        assert!(!asked, "a deferred request must not touch the server");
        assert_eq!(
            gated(
                &gate,
                2_001,
                SPACING,
                reason,
                || false,
                || { StartOutcome::Started }
            ),
            SnapshotRequest::Started,
            "the transaction ended: the very next tick starts the snapshot"
        );
        assert_eq!(
            gated(
                &gate,
                2_002,
                SPACING,
                reason,
                || false,
                || { StartOutcome::Started }
            ),
            SnapshotRequest::RateLimited,
            "and that start consumed the slot as usual"
        );
    }

    /// moon#1297 × moon#1289: the cold reclaim's snapshot is as automatic as
    /// the held-file one. Both defer while a TXN is open without touching the
    /// gate, and both register their round for the per-shard start check
    /// (`request` registers exactly the reasons that wait).
    #[test]
    fn every_automatic_reason_defers_while_a_txn_is_open() {
        for reason in [SnapshotReason::HeldColdFiles, SnapshotReason::ColdReclaim] {
            assert!(reason.waits_for_open_txns(), "{reason:?}");
            let gate = SnapshotGate::new();
            let mut asked = false;
            assert_eq!(
                gated(
                    &gate,
                    1_000,
                    SPACING,
                    reason,
                    || true,
                    || {
                        asked = true;
                        StartOutcome::Started
                    }
                ),
                SnapshotRequest::TxnOpen,
                "{reason:?}"
            );
            assert!(
                !asked,
                "{reason:?}: a deferred request must not touch the server"
            );
            assert_eq!(
                gated(
                    &gate,
                    1_001,
                    SPACING,
                    reason,
                    || false,
                    || StartOutcome::Started
                ),
                SnapshotRequest::Started,
                "{reason:?}: the deferral did not consume the slot"
            );
        }
    }

    /// review N1: an abandoned round gives its slot back — the next sweep
    /// asks again at once — but never a slot a newer request took.
    #[test]
    fn an_abandoned_round_gives_its_slot_back() {
        let gate = SnapshotGate::new();
        assert_eq!(
            gate.try_request(1_000, SPACING, || StartOutcome::Started),
            SnapshotRequest::Started
        );
        assert_eq!(
            gate.try_request(1_500, SPACING, || StartOutcome::Started),
            SnapshotRequest::RateLimited
        );
        gate.give_back(1_000);
        assert_eq!(
            gate.try_request(1_600, SPACING, || StartOutcome::Started),
            SnapshotRequest::Started,
            "the slot was given back"
        );
        gate.give_back(1_000);
        assert_eq!(
            gate.try_request(1_700, SPACING, || StartOutcome::Started),
            SnapshotRequest::RateLimited,
            "a stale give-back leaves the newer request's slot alone"
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
