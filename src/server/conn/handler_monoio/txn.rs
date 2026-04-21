//! TXN.BEGIN / TXN.COMMIT / TXN.ABORT + TEMPORAL.SNAPSHOT_AT / TEMPORAL.INVALIDATE handlers.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use bytes::Bytes;

#[cfg(feature = "graph")]
use crate::command::temporal::{ERR_ENTITY_NOT_FOUND, ERR_GRAPH_NOT_FOUND};
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
            // Get next txn_id and snapshot_lsn from vector store's transaction manager
            let mut vector_store = ctx.shard_databases.vector_store(ctx.shard_id);
            let active = vector_store.txn_manager_mut().begin();
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
                let mut vector_store = ctx.shard_databases.vector_store(ctx.shard_id);
                vector_store.txn_manager_mut().commit(txn.txn_id);
                drop(vector_store);

                // Write XactCommit WAL record with committed KV state
                let txn_id = txn.txn_id;
                if !txn.kv_undo.is_empty() {
                    let db_guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
                    let payload = crate::persistence::wal_v3::record::encode_xact_commit_payload(
                        txn_id,
                        txn.kv_undo.records(),
                        &*db_guard,
                    );
                    drop(db_guard);
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

                // Materialize MQ intents: enqueue deferred MQ.PUBLISH messages
                if !txn.mq_intents.is_empty() {
                    let mut db_guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                    for intent in &txn.mq_intents {
                        if let Ok(Some(stream)) = db_guard.get_stream_mut(&intent.queue_key) {
                            if stream.durable {
                                let msg_id = stream.next_auto_id();
                                stream.add(msg_id, intent.fields.clone());
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

/// Handle TXN.ABORT ��� returns `true` if the command was consumed.
pub(super) fn try_handle_txn_abort(
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
                crate::transaction::abort::abort_cross_store_txn(
                    &ctx.shard_databases,
                    ctx.shard_id,
                    conn.selected_db,
                    txn,
                );
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
            let lsn = {
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
pub(super) fn try_handle_temporal_invalidate(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_temporal_invalidate(cmd) {
        return false;
    }
    match validate_invalidate(cmd_args) {
        Ok((entity_id, is_node, graph_name)) => {
            let wall_ms = capture_wall_ms();
            #[cfg(feature = "graph")]
            {
                let mut gs = ctx.shard_databases.graph_store_write(ctx.shard_id);
                if let Some(named_graph) = gs.get_graph_mut(&graph_name) {
                    let mutated = if is_node {
                        let node_key: crate::graph::types::NodeKey =
                            slotmap::KeyData::from_ffi(entity_id).into();
                        if let Some(node) = named_graph.write_buf.get_node_mut(node_key) {
                            node.valid_to = wall_ms;
                            true
                        } else {
                            false
                        }
                    } else {
                        let edge_key: crate::graph::types::EdgeKey =
                            slotmap::KeyData::from_ffi(entity_id).into();
                        if let Some(edge) = named_graph.write_buf.get_edge_mut(edge_key) {
                            edge.valid_to = wall_ms;
                            true
                        } else {
                            false
                        }
                    };
                    if mutated {
                        let payload = crate::persistence::wal_v3::record::encode_graph_temporal(
                            entity_id, is_node, wall_ms, wall_ms,
                        );
                        gs.wal_pending.push(payload);
                        let wal_records = gs.drain_wal();
                        drop(gs);
                        for record in wal_records {
                            ctx.shard_databases
                                .wal_append(ctx.shard_id, Bytes::from(record));
                        }
                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                    } else {
                        drop(gs);
                        responses.push(Frame::Error(Bytes::from_static(ERR_ENTITY_NOT_FOUND)));
                    }
                } else {
                    drop(gs);
                    responses.push(Frame::Error(Bytes::from_static(ERR_GRAPH_NOT_FOUND)));
                }
            }
            #[cfg(not(feature = "graph"))]
            {
                let _ = (entity_id, is_node, graph_name, wall_ms, ctx);
                responses.push(Frame::Error(Bytes::from_static(
                    b"ERR graph feature not enabled",
                )));
            }
        }
        Err(e) => responses.push(e),
    }
    true
}
