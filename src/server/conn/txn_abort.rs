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
//!    synchronous stretch: the AOF fold epoch is read, the replication
//!    records are recorded (monoio), the graph WAL records are appended;
//! 2. the KV records are appended to this shard's AOF through the MULTI/EXEC
//!    group commit ([`persist_txn_aof`]) — one `fsync` barrier under
//!    `appendfsync always`, so an `+OK` means the abort is on disk;
//! 3. only then do the remote graph legs of a multi-shard abort await.
//!
//! [`persist_txn_aof`]: crate::server::conn::shared::persist_txn_aof

use bytes::Bytes;

use crate::server::conn::core::ConnectionContext;
use crate::transaction::CrossStoreTxn;

/// Records one command in the replication stream under a database —
/// `handler_monoio::ft::record_local_write_db` on the runtime that serves
/// replicas.
pub(crate) type ReplicationRecorder = fn(&ConnectionContext, usize, Bytes);

/// Roll `txn` back and log the rollback. `replicate` is `Some` when this
/// connection's writes are replicated (the caller's
/// `replication_fanout_active`); the tokio runtime passes `None`.
///
/// `Err(reply)` when the AOF refused the records or their barrier — the
/// keyspace IS rolled back either way; the caller reports that the abort's
/// durability could not be guaranteed.
pub(crate) async fn abort_logged(
    ctx: &ConnectionContext,
    txn: CrossStoreTxn,
    replicate: Option<ReplicationRecorder>,
) -> Result<(), &'static [u8]> {
    let txn_id = txn.txn_id;
    let graph_db = txn.db_index;
    let (log, remote) = crate::transaction::abort::abort_local(ctx.shard_id, ctx.num_shards, txn);

    // --- the no-await stretch of the undo ---------------------------------
    // #455: a fold that snapshots this shard after the undo but before the
    // records are enqueued must drop them, not replay them on its base.
    let fold_stamp = ctx
        .aof_pool
        .as_ref()
        .map_or(crate::persistence::aof::FoldEpoch::INITIAL, |pool| {
            pool.fold_stamp(ctx.shard_id)
        });
    if let Some(record) = replicate {
        for (db, bytes) in &log.kv {
            record(ctx, *db, bytes.clone());
        }
        // Graph replication is single-shard scope, exactly like the forward
        // GRAPH.* leg (`try_handle_graph_command`).
        if ctx.num_shards == 1 {
            for bytes in &log.graph {
                record(ctx, graph_db, bytes.clone());
            }
        }
    }
    for bytes in log.graph {
        ctx.shard_databases.wal_append(
            ctx.shard_id,
            crate::persistence::wal_v3::record::WalRecordType::Command,
            bytes,
        );
    }
    // -----------------------------------------------------------------------

    let persisted =
        crate::server::conn::shared::persist_txn_aof(ctx, log.kv, replicate.is_some(), fold_stamp)
            .await;
    crate::transaction::abort::send_remote_graph_rollbacks(
        ctx.shard_id,
        txn_id,
        &ctx.dispatch_tx,
        &ctx.spsc_notifiers,
        remote,
    )
    .await;
    if let Err(reply) = persisted {
        tracing::error!(
            txn_id,
            reply = %String::from_utf8_lossy(reply),
            "TXN.ABORT: the rollback is applied but its AOF records were refused"
        );
    }
    persisted
}
