//! AOF **group commit** — coalesce concurrent pending writes into ONE fsync.
//!
//! Under `appendfsync=always` the writer used to `flush()+sync_data()` once per
//! `AppendSync` message (the ~11× write penalty: 135K → 12K ops/s). When N
//! per-connection durable writers are in flight, N `AppendSync` messages queue at
//! one writer while it is busy on the first fsync. This module factors out the
//! pure *batching seam* so the writer can drain the ready queue, write every
//! message, make them durable with a **single** `flush()+sync_data()`, and only
//! THEN ack every waiter `Synced` — amortizing the fsync cost across write
//! concurrency without weakening the fsync-before-ack durability contract.
//!
//! Two seams, both unit-testable with no file / no runtime:
//!   - [`collect_group_commit_batch`] — the *pure drain*: pull queued data
//!     messages until the queue empties, a control message appears (deferred,
//!     NEVER batched), or a bounded cap is hit. Control-message ordering and the
//!     bounded-drain guarantee live here.
//!   - [`commit_group_commit_batch`] — the *durability invariant*: write all
//!     bytes in order, ONE fsync after all writes (Always only), then ack every
//!     `AppendSync`. The single fsync also makes every preceding fire-and-forget
//!     `Append` in the batch durable (the H1-BARRIER ordered-channel property).
//!
//! The four writer loops (TopLevel/PerShard × monoio/tokio) share
//! [`collect_group_commit_batch`] and the ack/durability ordering in
//! [`ack_batch`]. The sync monoio TopLevel path calls [`commit_group_commit_batch`]
//! directly through a [`GroupCommitSink`] over its `std::fs::File`; the async
//! tokio paths and the framed per-shard paths replicate the SAME three-step
//! invariant inline (async I/O cannot implement a sync sink, and the per-shard
//! framed format prepends a `[u64 lsn][u32 len]` header per record) but ack
//! through the same [`ack_batch`] so the durability ordering is single-sourced.

use super::{AofAck, AofMessage};

/// Hard cap on the number of messages coalesced into one batch. A write flood
/// cannot delay an early waiter's fsync unboundedly: the drain stops at the cap,
/// flushes, fsyncs, acks, and a fresh batch begins on the next loop iteration.
pub const AOF_GROUP_COMMIT_MAX_BATCH: usize = 1024;

/// Hard cap on the bytes buffered into one batch (whole-message granularity — a
/// message is never split). Bounds the write buffer's high-water mark so a burst
/// of large values cannot grow memory without limit before the fsync.
pub const AOF_GROUP_COMMIT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// One drained group-commit batch: the data messages to write (in channel
/// order) plus the single control message that TERMINATED the drain, if any.
///
/// `deferred_control` is the structural guarantee behind `batch_straddles_control`:
/// a control / rewrite message is NEVER placed in `data`, so the caller always
/// flushes (writes + ONE fsync + acks all) the in-progress batch BEFORE handling
/// the control message — message order is preserved exactly.
pub struct GroupCommitBatch {
    /// `Append` + `AppendSync` messages, in channel order. These are the bytes
    /// to write; every `AppendSync` carries an ack waiter to signal after fsync.
    pub data: Vec<AofMessage>,
    /// A control message (`Rewrite` / `RewriteSharded` / `RewritePerShard` /
    /// `Shutdown`) that ended the drain. Handled by the caller AFTER the batch is
    /// committed — never absorbed into `data`.
    pub deferred_control: Option<AofMessage>,
}

/// True for the non-data control messages that must break (never join) a batch.
#[inline]
fn is_control(msg: &AofMessage) -> bool {
    matches!(
        msg,
        AofMessage::Rewrite(_)
            | AofMessage::RewriteSharded(_)
            | AofMessage::RewritePerShard { .. }
            | AofMessage::Shutdown
    )
}

/// Payload byte length of a data message (0 for control / zero-length barrier).
#[inline]
fn msg_body_len(msg: &AofMessage) -> usize {
    match msg {
        AofMessage::Append { bytes, .. } | AofMessage::AppendSync { bytes, .. } => bytes.len(),
        _ => 0,
    }
}

/// Borrow the payload bytes of a data message (empty for control / a zero-length
/// H1-BARRIER `AppendSync`). Used by [`commit_group_commit_batch`] and the inline
/// writer paths to feed the sink in channel order.
#[inline]
pub(crate) fn msg_body(msg: &AofMessage) -> &[u8] {
    match msg {
        AofMessage::Append { bytes, .. } | AofMessage::AppendSync { bytes, .. } => bytes,
        _ => &[],
    }
}

/// Drain a ready group-commit batch from one message plus a non-blocking puller.
///
/// `first` is the message just `recv()`'d (data or control); `try_next` is a
/// non-blocking channel drain (e.g. `|| rx.try_recv().ok()`).
///
/// - `first` is a control message → empty `data`, `deferred_control = Some(first)`
///   (no batch; the caller handles the control directly).
/// - otherwise → pull data messages until ONE of: the queue empties · a control
///   message appears (→ `deferred_control`, drain stops) · `data.len() == max_batch`
///   · accumulated bytes `>= max_bytes` (whole-message granularity: the crossing
///   message is included, then the drain stops).
///
/// Channel order is preserved exactly and a control message is NEVER placed in
/// `data`.
pub fn collect_group_commit_batch(
    first: AofMessage,
    mut try_next: impl FnMut() -> Option<AofMessage>,
    max_batch: usize,
    max_bytes: usize,
) -> GroupCommitBatch {
    if is_control(&first) {
        return GroupCommitBatch {
            data: Vec::new(),
            deferred_control: Some(first),
        };
    }

    let mut total_bytes = msg_body_len(&first);
    let mut data = Vec::with_capacity(8);
    data.push(first);
    let mut deferred_control = None;

    // Stop conditions are checked at the loop head so the message that crosses a
    // cap is already included (the cap is a "stop after", not "stop before").
    while data.len() < max_batch && total_bytes < max_bytes {
        match try_next() {
            None => break, // queue drained
            Some(msg) if is_control(&msg) => {
                deferred_control = Some(msg);
                break;
            }
            Some(msg) => {
                total_bytes += msg_body_len(&msg);
                data.push(msg);
            }
        }
    }

    GroupCommitBatch {
        data,
        deferred_control,
    }
}

/// Sync, mockable realization of the frozen `<W: Write>` + `flush()+sync_data()`
/// commit target. Splitting `write_all` from `sync` is what makes the single
/// per-batch fsync countable in tests (the durability behavior is unchanged).
pub trait GroupCommitSink {
    /// Append `buf` to the underlying file. An empty `buf` (a zero-length
    /// H1-BARRIER `AppendSync`) is a no-op write.
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
    /// `flush()` then `sync_data()` — the single durability point for the batch.
    fn sync(&mut self) -> std::io::Result<()>;
}

/// Result of committing one batch — reported back so the caller can drive its
/// `write_error` latch and surface failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitOutcome {
    /// Number of `AppendSync` waiters acked `Synced` (0 on any failure, and 0 for
    /// an `Append`-only batch).
    pub synced: usize,
    /// A `write_all` for some message in the batch failed; the latch must engage.
    pub write_failed: bool,
    /// The single `flush()+sync_data()` failed; every waiter acked `FsyncFailed`.
    pub fsync_failed: bool,
}

/// Verdict applied uniformly to every `AppendSync` waiter in a committed batch.
#[derive(Clone, Copy)]
pub(crate) enum BatchAck {
    Synced,
    WriteFailed,
    FsyncFailed,
}

/// Drain the batch's data, acking every `AppendSync` waiter with `verdict`
/// (`Append` messages have no waiter). This is the SINGLE source of the
/// durability ack ordering: it is called ONLY in commit step 3 (after the fsync)
/// or on a terminal write/fsync failure — never before the fsync result is known,
/// so `ack_before_fsync` is unreachable by construction. Shared by the sync
/// [`commit_group_commit_batch`] and the async/framed inline writer paths so all
/// four writer loops ack identically.
pub(crate) fn ack_batch(batch: &mut GroupCommitBatch, verdict: BatchAck) -> CommitOutcome {
    let mut synced = 0usize;
    for msg in batch.data.drain(..) {
        if let AofMessage::AppendSync { ack, .. } = msg {
            let a = match verdict {
                BatchAck::Synced => {
                    synced += 1;
                    AofAck::Synced
                }
                BatchAck::WriteFailed => AofAck::WriteFailed,
                BatchAck::FsyncFailed => AofAck::FsyncFailed,
            };
            // The receiver may be gone (caller dropped it after an F2 timeout);
            // a failed send is benign — the caller already moved on.
            let _ = ack.send(a);
        }
    }
    CommitOutcome {
        synced,
        write_failed: matches!(verdict, BatchAck::WriteFailed),
        fsync_failed: matches!(verdict, BatchAck::FsyncFailed),
    }
}

/// Commit one batch through a [`GroupCommitSink`] — the durability invariant:
///
///   1. `write_all` every data message's bytes IN ORDER;
///   2. if `do_fsync` (true under `Always`): exactly ONE `sync()` AFTER all writes;
///   3. THEN ack every `AppendSync` waiter `Synced`.
///
/// Reject mappings (one response per §1 reject code):
///   - a `write_all` error → `batch_write_failed`: every `AppendSync` acked
///     `WriteFailed`, none `Synced` (`outcome.write_failed = true`). The caller
///     engages its `write_error` latch — the stream may be torn.
///   - a `sync` error → `batch_fsync_failed`: every `AppendSync` acked
///     `FsyncFailed`, none `Synced` (`outcome.fsync_failed = true`).
///   - `ack_before_fsync` is impossible: acks happen only in step 3, after the
///     fsync returns.
///
/// `do_fsync == false` (everysec/no) writes the batch without a per-batch fsync —
/// the writer's deadline flush governs durability. Such batches are `Append`-only
/// (an `AppendSync` is enqueued only under `Always`, see `pool::try_send_append_durable`
/// / `pool::fsync_barrier`), so `synced == 0`.
pub fn commit_group_commit_batch<S: GroupCommitSink + ?Sized>(
    sink: &mut S,
    batch: &mut GroupCommitBatch,
    do_fsync: bool,
) -> CommitOutcome {
    // Step 1 — write every data message's bytes in channel order.
    for msg in &batch.data {
        if sink.write_all(msg_body(msg)).is_err() {
            return ack_batch(batch, BatchAck::WriteFailed);
        }
    }
    // Step 2 — exactly ONE fsync, AFTER all writes, only under Always.
    if do_fsync && sink.sync().is_err() {
        return ack_batch(batch, BatchAck::FsyncFailed);
    }
    // Step 3 — ack every AppendSync (only now, after the fsync has returned).
    ack_batch(batch, BatchAck::Synced)
}
