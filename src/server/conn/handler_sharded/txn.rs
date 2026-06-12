//! TXN.BEGIN / TXN.COMMIT / TXN.ABORT + TEMPORAL.SNAPSHOT_AT / TEMPORAL.INVALIDATE handlers.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use bytes::Bytes;

use crate::command::temporal::{
    capture_wall_ms, is_temporal_invalidate, is_temporal_snapshot_at, validate_invalidate,
    validate_snapshot_at,
};
use crate::command::transaction::{
    is_txn_abort, is_txn_begin, is_txn_commit, txn_abort_validate, txn_begin_validate,
    txn_commit_validate,
};
use crate::protocol::Frame;
use crate::server::conn::core::ConnectionContext;
use crate::server::conn::core::ConnectionState;
use crate::transaction::CrossStoreTxn;

/// Handle TXN.BEGIN — returns `true` if the command was consumed.
pub(super) fn try_handle_txn_begin(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_txn_begin(cmd, cmd_args) {
        return false;
    }
    match txn_begin_validate(conn.in_multi, conn.in_cross_txn()) {
        Ok(()) => {
            // Get next txn_id and snapshot_lsn from vector store's transaction manager.
            // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
            let active = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| s.vector_store.txn_manager_mut().begin())
            } else {
                let mut vector_store = ctx.shard_databases.vector_store(ctx.shard_id);
                vector_store.txn_manager_mut().begin()
            };
            conn.active_cross_txn = Some(CrossStoreTxn::new(active.txn_id, active.snapshot_lsn));
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        }
        Err(e) => responses.push(e),
    }
    true
}

/// Handle TXN.COMMIT — returns `true` if the command was consumed.
pub(super) fn try_handle_txn_commit(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_txn_commit(cmd, cmd_args) {
        return false;
    }
    match txn_commit_validate(conn.in_cross_txn()) {
        Ok(()) => {
            if let Some(txn) = conn.active_cross_txn.take() {
                // MA2: reject commit if the snapshot was killed (by operator KILL SNAPSHOT
                // or by the automatic old_snapshot_threshold sweep). A killed snapshot may
                // have been excluded from oldest_snapshot, allowing prune_committed to
                // advance past its LSN. Committing with a stale read set is undefined
                // behaviour — force the client to restart the transaction.
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                // Closure returns true iff the snapshot was killed (caller emits MOONERR
                // and short-circuits the commit path).
                let was_killed = if crate::shard::slice::is_initialized() {
                    crate::shard::slice::with_shard(|s| {
                        if s.vector_store.txn_manager().is_killed(txn.txn_id) {
                            s.vector_store.txn_manager_mut().abort_killed(txn.txn_id);
                            true
                        } else {
                            s.vector_store.txn_manager_mut().commit(txn.txn_id);
                            false
                        }
                    })
                } else {
                    let mut vector_store = ctx.shard_databases.vector_store(ctx.shard_id);
                    let killed = if vector_store.txn_manager().is_killed(txn.txn_id) {
                        vector_store.txn_manager_mut().abort_killed(txn.txn_id);
                        true
                    } else {
                        vector_store.txn_manager_mut().commit(txn.txn_id);
                        false
                    };
                    drop(vector_store);
                    killed
                };
                if was_killed {
                    tracing::warn!(
                        txn_id = txn.txn_id,
                        "TXN.COMMIT rejected: snapshot was killed (snapshot too old)"
                    );
                    let mut msg = bytes::BytesMut::new();
                    use std::fmt::Write as _;
                    let _ = write!(msg, "MOONERR snapshot too old: {}", txn.txn_id);
                    responses.push(Frame::Error(msg.freeze()));
                    return true;
                }

                // Write XactCommit WAL record with committed KV state
                let txn_id = txn.txn_id;
                if !txn.kv_undo.is_empty() {
                    // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                    let payload = if crate::shard::slice::is_initialized() {
                        crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                            crate::persistence::wal_v3::record::encode_xact_commit_payload(
                                txn_id,
                                txn.kv_undo.records(),
                                db,
                            )
                        })
                    } else {
                        let db_guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
                        let payload =
                            crate::persistence::wal_v3::record::encode_xact_commit_payload(
                                txn_id,
                                txn.kv_undo.records(),
                                &*db_guard,
                            );
                        drop(db_guard);
                        payload
                    };
                    let mut wal_buf = Vec::new();
                    crate::persistence::wal_v3::record::write_wal_v3_record(
                        &mut wal_buf,
                        txn_id,
                        crate::persistence::wal_v3::record::WalRecordType::XactCommit,
                        &payload,
                    );
                    ctx.shard_databases
                        .wal_append(ctx.shard_id, bytes::Bytes::from(wal_buf));
                }

                // Release KV write intents from shard side-table
                ctx.shard_databases
                    .kv_intents(ctx.shard_id)
                    .release_txn(txn_id);

                // Drain deferred HNSW inserts (post-commit hook).
                // The drain prevents phantom neighbors on abort.
                // Actual HNSW graph insertion happens during compaction,
                // not at commit time (point is already in mutable segment).
                let drain_count = ctx
                    .shard_databases
                    .hnsw_queue(ctx.shard_id)
                    .drain_for_txn(txn_id)
                    .count();
                if drain_count > 0 {
                    tracing::debug!(txn_id, count = drain_count, "Drained deferred HNSW inserts");
                }

                // Materialize MQ intents: enqueue deferred MQ.PUBLISH messages.
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                if !txn.mq_intents.is_empty() {
                    let materialize = |db: &mut crate::storage::db::Database| {
                        for intent in &txn.mq_intents {
                            if let Ok(Some(stream)) = db.get_stream_mut(&intent.queue_key) {
                                if stream.durable {
                                    let msg_id = stream.next_auto_id();
                                    stream.add(msg_id, intent.fields.clone());
                                }
                            }
                        }
                    };
                    if crate::shard::slice::is_initialized() {
                        crate::shard::slice::with_shard_db(conn.selected_db, materialize);
                    } else {
                        // Each queue lives on the shard owning its key, which
                        // may differ from the connection's shard (and per
                        // intent) — acquire the owner's db per queue.
                        for intent in &txn.mq_intents {
                            let owner = crate::shard::dispatch::key_to_shard(
                                &intent.queue_key,
                                ctx.num_shards,
                            );
                            let mut db_guard =
                                ctx.shard_databases.write_db(owner, conn.selected_db);
                            if let Ok(Some(stream)) = db_guard.get_stream_mut(&intent.queue_key) {
                                if stream.durable {
                                    let msg_id = stream.next_auto_id();
                                    stream.add(msg_id, intent.fields.clone());
                                }
                            }
                        }
                    }
                }

                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
            } else {
                responses.push(Frame::Error(Bytes::from_static(b"ERR not in transaction")));
            }
        }
        Err(e) => responses.push(e),
    }
    true
}

/// Handle TXN.ABORT — returns `true` if the command was consumed.
pub(super) async fn try_handle_txn_abort(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_txn_abort(cmd, cmd_args) {
        return false;
    }
    match txn_abort_validate(conn.in_cross_txn()) {
        Ok(()) => {
            if let Some(txn) = conn.active_cross_txn.take() {
                // Shared rollback (Phase 166 Plan 03):
                // KV undo -> graph intents reverse -> vector
                // tombstone -> side-table release. See
                // src/transaction/abort.rs for lock ordering.
                // Multi-shard: graph legs route to the shards owning
                // each graph name via ShardMessage::GraphRollback.
                crate::transaction::abort::abort_cross_store_txn_routed(
                    &ctx.shard_databases,
                    ctx.shard_id,
                    conn.selected_db,
                    ctx.num_shards,
                    &ctx.dispatch_tx,
                    &ctx.spsc_notifiers,
                    txn,
                )
                .await;
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
            } else {
                responses.push(Frame::Error(Bytes::from_static(b"ERR not in transaction")));
            }
        }
        Err(e) => responses.push(e),
    }
    true
}

/// Handle TEMPORAL.SNAPSHOT_AT — returns `true` if the command was consumed.
pub(super) fn try_handle_temporal_snapshot_at(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_temporal_snapshot_at(cmd) {
        return false;
    }
    match validate_snapshot_at(cmd_args) {
        Ok(()) => {
            let wall_ms = capture_wall_ms();
            // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
            let lsn = if crate::shard::slice::is_initialized() {
                crate::shard::slice::with_shard(|s| s.vector_store.txn_manager().current_lsn())
            } else {
                let vector_store = ctx.shard_databases.vector_store(ctx.shard_id);
                vector_store.txn_manager().current_lsn()
            };
            {
                let mut guard = ctx.shard_databases.temporal_registry(ctx.shard_id);
                let registry =
                    guard.get_or_insert_with(|| Box::new(crate::temporal::TemporalRegistry::new()));
                registry.record(wall_ms, lsn);
            }
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        }
        Err(e) => responses.push(e),
    }
    true
}

/// Handle TEMPORAL.INVALIDATE — returns `true` if the command was consumed.
pub(super) async fn try_handle_temporal_invalidate(
    cmd: &[u8],
    cmd_args: &[Frame],
    frame: &Frame,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_temporal_invalidate(cmd) {
        return false;
    }
    match validate_invalidate(cmd_args) {
        Ok((entity_id, is_node, graph_name)) => {
            #[cfg(feature = "graph")]
            {
                // Multi-shard: the graph lives on the shard that owns its
                // NAME. Ship non-local invalidations there via GraphCommand —
                // the shard-side handler applies the mutation and drains the
                // graph WAL on the owning shard.
                if ctx.num_shards > 1 {
                    let owner = crate::shard::dispatch::graph_to_shard(&graph_name, ctx.num_shards);
                    if owner != ctx.shard_id {
                        let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
                        let msg = crate::shard::dispatch::ShardMessage::GraphCommand {
                            command: std::sync::Arc::new(frame.clone()),
                            reply_tx,
                        };
                        crate::shard::coordinator::spsc_send(
                            &ctx.dispatch_tx,
                            ctx.shard_id,
                            owner,
                            msg,
                            &ctx.spsc_notifiers,
                        )
                        .await;
                        let response = match reply_rx.recv().await {
                            Ok(f) => f,
                            Err(_) => Frame::Error(Bytes::from_static(
                                b"ERR cross-shard reply channel closed",
                            )),
                        };
                        responses.push(response);
                        return true;
                    }
                }
                let wall_ms = capture_wall_ms();
                // Phase 2f: gate on is_initialized(); both branches are
                // semantically identical, the new path uses
                // ShardSlice::graph_store directly (no lock).
                let (result, wal_records) = if crate::shard::slice::is_initialized() {
                    crate::shard::slice::with_shard(|s| {
                        let gs = &mut s.graph_store;
                        let r = crate::command::temporal::apply_invalidate(
                            gs,
                            entity_id,
                            is_node,
                            &graph_name,
                            wall_ms,
                        );
                        let recs = if r.is_ok() {
                            gs.drain_wal()
                        } else {
                            Vec::new()
                        };
                        (r, recs)
                    })
                } else {
                    let mut gs = ctx.shard_databases.graph_store_write(ctx.shard_id);
                    let r = crate::command::temporal::apply_invalidate(
                        &mut gs,
                        entity_id,
                        is_node,
                        &graph_name,
                        wall_ms,
                    );
                    let recs = if r.is_ok() {
                        gs.drain_wal()
                    } else {
                        Vec::new()
                    };
                    (r, recs)
                };
                match result {
                    Ok(()) => {
                        for record in wal_records {
                            ctx.shard_databases
                                .wal_append(ctx.shard_id, Bytes::from(record));
                        }
                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                    }
                    Err(err) => responses.push(Frame::Error(Bytes::from_static(err))),
                }
            }
            #[cfg(not(feature = "graph"))]
            {
                let _ = (entity_id, is_node, graph_name, frame, ctx);
                responses.push(Frame::Error(Bytes::from_static(
                    b"ERR graph feature not enabled",
                )));
            }
        }
        Err(e) => responses.push(e),
    }
    true
}
