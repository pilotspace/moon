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
            // Get next txn_id and snapshot_lsn from vector store's transaction manager.
            // Unconditional slice path: ShardSlice is always initialized.
            let active =
                crate::shard::slice::with_shard(|s| s.vector_store.txn_manager_mut().begin());
            conn.active_cross_txn = Some(CrossStoreTxn::new(
                active.txn_id,
                active.snapshot_lsn,
                conn.selected_db,
            ));
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
                // Unconditional slice path: returns true iff the snapshot was killed.
                let was_killed = crate::shard::slice::with_shard(|s| {
                    if s.vector_store.txn_manager().is_killed(txn.txn_id) {
                        s.vector_store.txn_manager_mut().abort_killed(txn.txn_id);
                        true
                    } else {
                        s.vector_store.txn_manager_mut().commit(txn.txn_id);
                        false
                    }
                });
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
                // (unframed, real type — K1a). The record's LSN is now
                // assigned by the WAL writer itself at drain time — the old
                // pre-framing passed `txn_id` into `write_wal_v3_record`'s
                // `lsn` parameter, mislabeling the transaction id as the WAL
                // LSN in the (nested, now-deleted) inner frame.
                let txn_id = txn.txn_id;
                if !txn.kv_undo.is_empty() {
                    // Unconditional slice path: ShardSlice is always initialized.
                    let payload = crate::shard::slice::with_shard_db(txn.db_index, |db| {
                        crate::persistence::wal_v3::record::encode_xact_commit_payload(
                            txn_id,
                            txn.db_index as u32,
                            txn.kv_undo.records(),
                            db,
                        )
                    });
                    ctx.shard_databases.wal_append(
                        ctx.shard_id,
                        crate::persistence::wal_v3::record::WalRecordType::XactCommit,
                        bytes::Bytes::from(payload),
                    );
                }

                // Release KV write intents from shard side-table (ShardSlice path).
                crate::shard::slice::with_shard(|s| {
                    s.kv_write_intents.release_txn(txn_id);
                });

                // Drain deferred HNSW inserts (post-commit hook).
                // The drain prevents phantom neighbors on abort.
                // Actual HNSW graph insertion happens during compaction,
                // not at commit time (point is already in mutable segment).
                let drain_count = crate::shard::slice::with_shard(|s| {
                    s.deferred_hnsw_inserts.drain_for_txn(txn_id).count()
                });
                if drain_count > 0 {
                    tracing::debug!(txn_id, count = drain_count, "Drained deferred HNSW inserts");
                }

                // Materialize MQ intents: enqueue deferred MQ.PUBLISH messages.
                // Unconditional slice path (Wave B2): group intents by owning shard.
                // Self-shard intents applied locally; foreign groups sent via MqTxnMaterialize.
                if !txn.mq_intents.is_empty() {
                    let mut self_intents: Vec<crate::transaction::MqIntent> = Vec::new();
                    let mut foreign: std::collections::HashMap<
                        usize,
                        Vec<crate::transaction::MqIntent>,
                    > = std::collections::HashMap::new();
                    for intent in txn.mq_intents.iter().cloned() {
                        let owner =
                            crate::shard::dispatch::key_to_shard(&intent.queue_key, ctx.num_shards);
                        if owner == ctx.shard_id {
                            self_intents.push(intent);
                        } else {
                            foreign.entry(owner).or_default().push(intent);
                        }
                    }
                    // Apply self-shard intents synchronously (no borrow
                    // across .await). Collect WAL payloads inside the
                    // closure (encoding is a pure function call, not a
                    // nested `with_shard*` call) and emit them AFTER the
                    // closure returns — each MqPush record captures the
                    // ASSIGNED id so replay is outcome-deterministic (Wave B
                    // stage 2a: MQ.PUBLISH materialization durability).
                    if !self_intents.is_empty() {
                        let db_index = conn.selected_db;
                        let payloads: Vec<Vec<u8>> =
                            crate::shard::slice::with_shard_db(db_index, |db| {
                                let mut payloads = Vec::with_capacity(self_intents.len());
                                for intent in &self_intents {
                                    if let Ok(Some(stream)) = db.get_stream_mut(&intent.queue_key) {
                                        if stream.durable {
                                            let msg_id = stream.next_auto_id();
                                            payloads.push(crate::mq::wal::encode_mq_push(
                                                db_index as u32,
                                                &intent.queue_key,
                                                msg_id.ms,
                                                msg_id.seq,
                                                &intent.fields,
                                            ));
                                            stream.add(msg_id, intent.fields.clone());
                                        }
                                    }
                                }
                                payloads
                            });
                        for payload in payloads {
                            crate::shard::mq_exec::wal_append_on_slice(
                                crate::persistence::wal_v3::record::WalRecordType::MqPush,
                                Bytes::from(payload),
                            );
                        }
                    }
                    // Send MqTxnMaterialize to each foreign shard and await all
                    // acks. The commit is already WAL-durable at this point, so
                    // a dropped/failed leg cannot fail the commit — but it must
                    // fail LOUD at the client, never a silent `+OK` with lost
                    // MQ messages.
                    let mut mq_lost: Option<(usize, usize)> = None; // (shard, intents)
                    for (owner, intents) in foreign {
                        let intent_count = intents.len();
                        let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
                        let msg = crate::shard::dispatch::ShardMessage::MqTxnMaterialize {
                            db_index: conn.selected_db,
                            intents,
                            reply_tx,
                        };
                        let outcome = crate::shard::coordinator::spsc_send(
                            &ctx.dispatch_tx,
                            ctx.shard_id,
                            owner,
                            msg,
                            &ctx.spsc_notifiers,
                        )
                        .await;
                        if outcome != crate::shard::dispatch::PushOutcome::Pushed {
                            // Message dropped (target ring never drained): the
                            // intents were NEVER delivered. Escalate.
                            tracing::error!(
                                owner,
                                intent_count,
                                "TXN.COMMIT MQ materialize: dispatch backpressure — \
                                 intents dropped, commit reported as partial"
                            );
                            mq_lost.get_or_insert((owner, intent_count));
                            continue;
                        }
                        match crate::shard::coordinator::recv_reply_bounded(reply_rx).await {
                            Ok(()) => {}
                            Err(_) => {
                                tracing::warn!(
                                    "TXN.COMMIT MQ materialize: reply channel closed \
                                     for shard {}",
                                    owner
                                );
                            }
                        }
                    }
                    if let Some((owner, intent_count)) = mq_lost {
                        // KV/vector/graph legs of the txn ARE committed and
                        // durable; only foreign MQ materialization was lost.
                        responses.push(Frame::Error(Bytes::from(format!(
                            "MOONERR TXN.COMMIT partial: committed, but {intent_count} \
                             MQ intent(s) for shard {owner} were dropped under \
                             dispatch backpressure"
                        ))));
                        return true;
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
            // Unconditional slice path: ShardSlice is always initialized.
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
                        let _ = crate::shard::coordinator::spsc_send(
                            &ctx.dispatch_tx,
                            ctx.shard_id,
                            owner,
                            msg,
                            &ctx.spsc_notifiers,
                        )
                        .await;
                        let response =
                            match crate::shard::coordinator::recv_reply_bounded(reply_rx).await {
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
                // Unconditional slice path: ShardSlice is always initialized.
                //
                // K1a: `apply_invalidate` returns the unframed `GraphTemporal`
                // payload directly instead of pre-framing it with
                // `write_wal_v3_record` and stashing it in `gs.wal_pending` —
                // the typed `wal_append` channel below carries the real type.
                let result = crate::shard::slice::with_shard(|s| {
                    crate::command::temporal::apply_invalidate(
                        &mut s.graph_store,
                        entity_id,
                        is_node,
                        &graph_name,
                        wall_ms,
                    )
                });
                match result {
                    Ok(payload) => {
                        ctx.shard_databases.wal_append(
                            ctx.shard_id,
                            crate::persistence::wal_v3::record::WalRecordType::GraphTemporal,
                            Bytes::from(payload),
                        );
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
