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
    _ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_txn_begin(cmd, cmd_args) {
        return false;
    }
    match txn_begin_validate(conn.in_multi, conn.in_cross_txn()) {
        Ok(()) => {
            // Get next txn_id and snapshot_lsn from vector store's transaction manager
            let active =
                crate::shard::slice::with_shard(|s| s.vector_store.txn_manager_mut().begin());
            conn.active_cross_txn = Some(CrossStoreTxn::new(active.txn_id, active.snapshot_lsn));
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        }
        Err(e) => responses.push(e),
    }
    true
}

/// Handle TXN.COMMIT — returns `true` if the command was consumed.
pub(super) async fn try_handle_txn_commit(
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
                // Scope the vector_store guard so it is DROPPED before any .await
                // in the MQ-materialize hop below (lock-across-await rule).
                let killed = crate::shard::slice::with_shard(|s| {
                    if s.vector_store.txn_manager().is_killed(txn.txn_id) {
                        s.vector_store.txn_manager_mut().abort_killed(txn.txn_id);
                        true
                    } else {
                        s.vector_store.txn_manager_mut().commit(txn.txn_id);
                        false
                    }
                }); // with_shard borrow released here
                if killed {
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
                    let payload =
                        crate::shard::slice::with_shard_db(conn.selected_db as usize, |db| {
                            crate::persistence::wal_v3::record::encode_xact_commit_payload(
                                txn_id,
                                txn.kv_undo.records(),
                                db,
                            )
                        });
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

                // Release KV write intents and drain HNSW queue via thread-local slice.
                let drain_count = crate::shard::slice::with_shard(|s| {
                    s.kv_write_intents.release_txn(txn_id);
                    s.deferred_hnsw_inserts.drain_for_txn(txn_id).count()
                });
                if drain_count > 0 {
                    tracing::debug!(txn_id, count = drain_count, "Drained deferred HNSW inserts");
                }

                // Materialize MQ intents: enqueue deferred MQ.PUBLISH messages.
                // Owner-route via MqTxnMaterialize: self-owned intents fold locally;
                // foreign intents hop to the owning shard and await ack before reply.
                if !txn.mq_intents.is_empty() {
                    use std::collections::HashMap;
                    let mut by_shard: HashMap<usize, Vec<crate::transaction::MqIntent>> =
                        HashMap::new();
                    for intent in txn.mq_intents.iter().cloned() {
                        let owner =
                            crate::shard::dispatch::key_to_shard(&intent.queue_key, ctx.num_shards);
                        by_shard.entry(owner).or_default().push(intent);
                    }
                    for (owner, intents) in by_shard {
                        if owner == ctx.shard_id {
                            // Self: apply locally via slice.
                            crate::shard::slice::with_shard_db(conn.selected_db as usize, |db| {
                                for intent in &intents {
                                    if let Ok(Some(stream)) = db.get_stream_mut(&intent.queue_key) {
                                        if stream.durable {
                                            let msg_id = stream.next_auto_id();
                                            stream.add(msg_id, intent.fields.clone());
                                        }
                                    }
                                }
                            });
                        } else {
                            // Foreign: send MqTxnMaterialize hop and await ack.
                            let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
                            let msg = crate::shard::dispatch::ShardMessage::MqTxnMaterialize {
                                db_index: conn.selected_db as usize,
                                intents,
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
                            // Await the ack before replying OK to the client.
                            let _ = reply_rx.recv().await;
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
    _ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_temporal_snapshot_at(cmd) {
        return false;
    }
    match validate_snapshot_at(cmd_args) {
        Ok(()) => {
            let wall_ms = capture_wall_ms();
            crate::shard::slice::with_shard(|s| {
                let lsn = s.vector_store.txn_manager().current_lsn();
                let registry = s
                    .temporal_registry
                    .get_or_insert_with(|| Box::new(crate::temporal::TemporalRegistry::new()));
                registry.record(wall_ms, lsn);
            });
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
                // Unconditional slice path: apply temporal invalidation via
                // ShardSlice::graph_store directly (no lock).
                let (result, wal_records) = crate::shard::slice::with_shard(|s| {
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
                });
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
