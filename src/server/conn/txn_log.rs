//! A cross-store `TXN`'s block in the AOF and the replication stream — the
//! connection side of moon#1300 (the grammar and the replay are in
//! `persistence::replay::{pseudo, txn}`).
//!
//! - Every AOF record a transaction's write produces carries the
//!   transaction's id ([`stamp`]); the writer brackets it in
//!   `MOON.TXN BEGIN|PAUSE <id>` records (`aof::record_ctx`).
//! - A replicated record of the transaction is pushed as ONE unit
//!   `[BEGIN id][record][PAUSE id]` ([`repl_record`]): a multi-shard master
//!   merges its shards' streams per push, so the three cannot be split by
//!   another shard's record.
//! - The transaction ends with `MOON.TXN END <id>` in both ([`log_end`]):
//!   after its last record on commit, after its compensation on abort.
//!   Without it a crash (or a promotion, on a replica) rolls the block back.
//!
//! Nothing here runs for a connection with no transaction open, and a
//! transaction that wrote no keyspace key logs no END.

use bytes::{BufMut, Bytes, BytesMut};

use crate::persistence::aof::{AppendStamp, txn_end_record};
use crate::persistence::replay::pseudo::{TxnMarker, TxnRecord};
use crate::server::conn::core::ConnectionContext;
use crate::server::conn::txn_abort::ReplicationRecorder;

/// The stamp of a record transaction `txn_id` (0: none) writes now: the
/// writer's fold epoch and clock (read in the mutation's synchronous
/// section, #455 / moon#1283) and the transaction.
#[inline]
pub(crate) fn stamp(ctx: &ConnectionContext, txn_id: u64) -> AppendStamp {
    ctx.aof_pool
        .as_ref()
        .map_or(AppendStamp::INITIAL, |pool| pool.fold_stamp(ctx.shard_id))
        .in_txn(txn_id)
}

/// `record` as transaction `txn_id`'s replicated write: one buffer
/// `[MOON.TXN BEGIN id][record][MOON.TXN PAUSE id]` (see the module doc).
pub(crate) fn repl_record(txn_id: u64, record: &[u8]) -> Bytes {
    let begin = TxnRecord::new(TxnMarker::Begin(txn_id));
    let pause = TxnRecord::new(TxnMarker::Pause(txn_id));
    let mut out =
        BytesMut::with_capacity(begin.as_bytes().len() + record.len() + pause.as_bytes().len());
    out.put_slice(begin.as_bytes());
    out.put_slice(record);
    out.put_slice(pause.as_bytes());
    out.freeze()
}

/// End transaction `txn_id` in the logs: `MOON.TXN END <id>` to this shard's
/// AOF and, with `replicate`, to the replication stream — then `release`
/// (the hold release). The AOF record is stamped at its enqueue instant and
/// `release` runs in that same synchronous section
/// (`AofWriterPool::append_then_apply_in_txn`): a fold snapshot falls either
/// before both — the holds are still there, so the fold re-opens the block in
/// its new generation and this END lands after it — or after both. Under
/// `appendfsync always` the END is on disk when this returns `Ok`.
///
/// `release` runs whatever the outcome. `Err(reply)`: the END was refused
/// (writer backlog, dead writer, failed fsync) — the transaction stands in
/// memory, but a crash before the next fold rolls it back: the caller
/// answers the refusal instead of `+OK`, as for any write whose record was
/// refused.
pub(crate) async fn log_end(
    ctx: &ConnectionContext,
    txn_id: u64,
    db: usize,
    replicate: Option<ReplicationRecorder>,
    release: impl FnOnce(),
) -> Result<(), &'static [u8]> {
    let end = txn_end_record(txn_id);
    let mut release = Some(release);
    let mut run = |end: &Bytes| {
        // Replicated in the same section, before the keys are free.
        if let Some(record) = replicate {
            record(ctx, db, end.clone());
        }
        if let Some(release) = release.take() {
            release();
        }
    };
    let Some(pool) = ctx.aof_pool.as_ref() else {
        run(&end);
        return Ok(());
    };
    // A marker, like the writer's own (`lsn = 0`): it never moves the
    // replication offset a replay recovers.
    let txn = AppendStamp::INITIAL.end_of(txn_id).txn;
    let outcome = pool
        .append_then_apply_in_txn(ctx.shard_id, 0, db, end.clone(), txn, || run(&end))
        .await;
    // Refused: nothing was enqueued and `run` did not run — release anyway.
    if let Some(release) = release.take() {
        release();
    }
    match outcome {
        Ok(((), true)) => pool
            .fsync_barrier(ctx.shard_id)
            .await
            .map_err(crate::persistence::aof::barrier_refusal_reply),
        Ok(((), false)) => Ok(()),
        Err(ack) => {
            tracing::warn!(
                txn_id,
                "TXN end record refused by the AOF: the transaction stands in memory, but a \
                 crash before the next rewrite rolls it back"
            );
            Err(crate::persistence::aof::append_refusal_reply(ack))
        }
    }
}

/// Append a rolled-back transaction's compensating records (`records`, in
/// order) to this shard's AOF INSIDE its block — tagged with `stamp`'s
/// transaction, read in the undo's synchronous section — under the MULTI/EXEC
/// group commit: enqueued one by one, no fsync each (moon#1285). The END and
/// the single barrier follow in [`log_end`].
///
/// `MOON_TEST_TXN_ABORT_CRASH_AFTER_RECORDS=<n>` (test-only): once the first
/// `n` records are on disk the process aborts — a crash between a hash's
/// `RESTORE` and its `HPEXPIREAT` (`tests/txn_crash_atomicity_1300.rs`).
pub(crate) async fn persist_compensation(
    ctx: &ConnectionContext,
    records: Vec<(usize, Bytes)>,
    repl_recorded: bool,
    stamp: AppendStamp,
) -> Result<(), &'static [u8]> {
    let Some(pool) = ctx.aof_pool.as_ref() else {
        return Ok(());
    };
    let crash_after = crash_after_records_for_test();
    for (i, (db, bytes)) in records.into_iter().enumerate() {
        if crash_after == Some(i) {
            let _ = pool.fsync_barrier(ctx.shard_id).await;
            tracing::error!("MOON_TEST_TXN_ABORT_CRASH_AFTER_RECORDS: aborting the process");
            std::process::abort();
        }
        let lsn = if repl_recorded {
            0
        } else {
            ctx.issue_append_lsn(bytes.len())
        };
        if let Err(ack) = pool
            .send_append_group(ctx.shard_id, lsn, db, bytes, stamp)
            .await
        {
            return Err(crate::persistence::aof::append_refusal_reply(ack));
        }
    }
    Ok(())
}

/// See [`persist_compensation`]. Read once; unset in production.
fn crash_after_records_for_test() -> Option<usize> {
    static AFTER: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *AFTER.get_or_init(|| {
        std::env::var("MOON_TEST_TXN_ABORT_CRASH_AFTER_RECORDS")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}
