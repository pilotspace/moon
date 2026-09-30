//! `TXN.ABORT` with its rollback logged (moon#1285).
//!
//! The one place a connection rolls a cross-store transaction back — explicit
//! `TXN.ABORT`, a `TXN.COMMIT` refused for rejected ops (#499), and
//! disconnect cleanup, on both runtimes. The shared
//! [`abort_local`](crate::transaction::abort::abort_local) rolls every plane
//! back on this shard and returns the records that make the rollback durable;
//! this logs them the way the connection's generic write path logs a write:
//!
//! 1. the undo and everything below it up to the first await form ONE
//!    synchronous stretch: the AOF fold epoch is read, the graph WAL records
//!    are appended — checked, a refused record fails the reply (PR #1301
//!    review), with no capacity limit on this thread (moon#1302) — and then
//!    the replication records are recorded (monoio), the graph ones only as
//!    far as the WAL accepted them;
//! 2. the KV records are appended to this shard's AOF through the MULTI/EXEC
//!    group commit ([`persist_txn_aof`]) — one `fsync` barrier under
//!    `appendfsync always`, so an `+OK` means the abort is on disk;
//! 3. only then do the remote graph legs of a multi-shard abort await.
//!
//! [`persist_txn_aof`]: crate::server::conn::shared::persist_txn_aof

use bytes::Bytes;

use crate::server::conn::core::ConnectionContext;
use crate::transaction::CrossStoreTxn;

/// Why a transaction is being rolled back — decides who learns that the
/// rollback's AOF records were refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AbortCause {
    /// `TXN.ABORT`: the client is answered the refusal instead of `+OK`.
    Explicit,
    /// A `TXN.COMMIT` refused for rejected ops (#499): the client is
    /// answered the commit error, which says nothing about the rollback's
    /// durability.
    DirtyCommit,
    /// Disconnect cleanup: there is no client left to tell.
    Disconnect,
}

impl AbortCause {
    fn as_str(self) -> &'static str {
        match self {
            AbortCause::Explicit => "TXN.ABORT",
            AbortCause::DirtyCommit => "TXN.COMMIT rollback (rejected ops)",
            AbortCause::Disconnect => "disconnect rollback",
        }
    }
}

/// Records one command in the replication stream under a database —
/// `handler_monoio::ft::record_local_write_db` on the runtime that serves
/// replicas.
pub(crate) type ReplicationRecorder = fn(&ConnectionContext, usize, Bytes);

/// Roll `txn` back and log the rollback. `replicate` is `Some` when this
/// connection's writes are replicated (the caller's
/// `replication_fanout_active`); the tokio runtime passes `None`.
///
/// `Err(reply)` when the AOF refused the records or their barrier, when the
/// shard's WAL writer refused a graph rollback record (local or on a remote
/// leg's owner — PR #1301 review; since moon#1302 only a writer that is gone
/// refuses, there is no capacity limit), or when a remote graph leg was not delivered or
/// acknowledged — the stores ARE rolled back either way (a remote leg that
/// was never delivered excepted, which its reply says). A refusal is never
/// silent (wave-1 review MINOR 5): it is counted where it happens
/// (`aof_append_backpressure_refusals` / `aof_backpressure_dropped` for a
/// backlogged or dead AOF writer, `aof_fsync_failures` for a failed fsync,
/// `txn_rollback_wal_dropped` for graph WAL records), and this logs it with
/// its `cause` — at WARN for an explicit `TXN.ABORT`, whose client is
/// answered the refusal, and at ERROR for a dirty-commit or disconnect
/// rollback, where no client learns of it and the master's AOF may lack KV
/// records its replicas already received (the graph records are replicated
/// only as far as the WAL accepted them, moon#1302).
///
/// Durability parity (PR #1301 review): the graph records get what the
/// FORWARD graph writes get — enqueued in the no-await stretch, drained into
/// WAL-v3 on the 1 ms tick, fsynced off-loop; neither waits for a durable WAL
/// LSN before replying. The KV records keep the AOF barrier.
pub(crate) async fn abort_logged(
    ctx: &ConnectionContext,
    txn: CrossStoreTxn,
    replicate: Option<ReplicationRecorder>,
    cause: AbortCause,
) -> Result<(), Bytes> {
    let txn_id = txn.txn_id;
    // moon#1299: the transaction's keys stay held until the restore is
    // applied AND its compensating records are enqueued (or refused and
    // reported) — released earlier, another client's write in the window
    // could reach the log AHEAD of the compensation that overwrites it on
    // replay. Released when this guard drops: at the end of this function,
    // or if the future is dropped mid-await, so a cancelled abort can never
    // leave the keys held forever.
    let _release = crate::transaction::isolation::EndOnDrop::new(txn_id);
    let graph_db = txn.db_index;
    let (log, remote) = crate::transaction::abort::abort_local(ctx.shard_id, ctx.num_shards, txn);

    // --- the no-await stretch of the undo ---------------------------------
    // #455: a fold that snapshots this shard after the undo but before the
    // records are enqueued must drop them, not replay them on its base.
    let fold_stamp = ctx
        .aof_pool
        .as_ref()
        .map_or(crate::persistence::aof::AppendStamp::INITIAL, |pool| {
            pool.fold_stamp(ctx.shard_id)
        });
    // moon#1302: the graph records are appended FIRST and only the accepted
    // prefix is replicated. Replicated first (the pre-fix order), a refusal
    // left the replica holding the whole rollback while this node's WAL held
    // a prefix — after a restart the two disagreed for good. The append has
    // no capacity limit on this thread, so a refusal now means the WAL writer
    // is gone; it is still counted, logged and answered (PR #1301 review).
    let graph_wal = crate::transaction::abort::append_graph_rollback_wal(
        &ctx.shard_databases,
        ctx.shard_id,
        txn_id,
        &log.graph,
    );
    let graph_logged = match graph_wal {
        Ok(()) => log.graph.len(),
        Err(refused) => refused.accepted,
    };
    if let Some(record) = replicate {
        for (db, bytes) in &log.kv {
            record(ctx, *db, bytes.clone());
        }
        // Graph replication is single-shard scope, exactly like the forward
        // GRAPH.* leg (`try_handle_graph_command`).
        if ctx.num_shards == 1 {
            for bytes in &log.graph[..graph_logged] {
                record(ctx, graph_db, bytes.clone());
            }
        }
    }
    // -----------------------------------------------------------------------

    let persisted =
        crate::server::conn::shared::persist_txn_aof(ctx, log.kv, replicate.is_some(), fold_stamp)
            .await;
    let remote_legs = crate::transaction::abort::send_remote_graph_rollbacks(
        ctx.shard_id,
        txn_id,
        &ctx.dispatch_tx,
        &ctx.spsc_notifiers,
        remote,
    )
    .await;
    // The first refusal in log order (KV AOF, local graph WAL, remote graph
    // legs) is the reply; every one was already counted and logged where it
    // happened.
    let outcome = persisted
        .and(graph_wal.map_err(|_| crate::transaction::abort::ROLLBACK_WAL_REFUSED_ERR))
        .map_err(Bytes::from_static)
        .and(remote_legs);
    if let Err(reply) = &outcome {
        let reply = String::from_utf8_lossy(reply);
        if cause == AbortCause::Explicit {
            tracing::warn!(
                txn_id,
                cause = cause.as_str(),
                reply = %reply,
                "TXN rollback applied but some of its log records were refused; the client was answered the refusal"
            );
        } else {
            tracing::error!(
                txn_id,
                cause = cause.as_str(),
                reply = %reply,
                "TXN rollback applied but some of its log records were refused and NO client \
                 was told: a restart may replay the rolled-back writes, and replicas may hold \
                 records this node's logs lack"
            );
        }
    }
    outcome
}
