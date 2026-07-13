//! Write-path command handlers: WS.*, MQ.*, MULTI/EXEC/DISCARD, GRAPH.*.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use bytes::Bytes;

use crate::command::mq::{
    ERR_MQ_UNKNOWN_SUB, parse_mq_subcommand, validate_mq_ack, validate_mq_create,
    validate_mq_dlqlen, validate_mq_pop, validate_mq_publish, validate_mq_push,
    validate_mq_trigger,
};
use crate::command::transaction::ERR_MULTI_TXN_CONFLICT;
use crate::command::workspace::{
    ERR_WS_ALREADY_BOUND, ERR_WS_NOT_FOUND, ERR_WS_UNKNOWN_SUB, parse_workspace_id_from_bytes,
    parse_ws_subcommand, validate_ws_auth, validate_ws_create, validate_ws_drop, validate_ws_info,
    validate_ws_list,
};
use crate::mq::is_mq_command;
use crate::protocol::Frame;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
#[cfg(feature = "graph")]
use crate::server::conn::util::extract_bytes;
#[cfg(feature = "graph")]
use crate::workspace::strip_workspace_prefix_from_response;
use crate::workspace::{WorkspaceId, is_ws_command};

use super::execute_transaction_sharded;

/// Handle WS.* workspace commands. Returns `true` if consumed.
pub(super) async fn try_handle_ws_command(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_ws_command(cmd) {
        return false;
    }
    let sub = match parse_ws_subcommand(cmd_args) {
        Ok(s) => s,
        Err(e) => {
            responses.push(e);
            return true;
        }
    };

    if sub.eq_ignore_ascii_case(b"CREATE") {
        match validate_ws_create(cmd_args) {
            Ok(ws_name) => {
                let ws_id = WorkspaceId::new_v7();
                let created_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let meta = crate::workspace::WorkspaceMetadata {
                    id: ws_id,
                    name: ws_name.clone(),
                    created_at,
                };
                {
                    let mut guard = ctx.shard_databases.workspace_registry();
                    let reg = guard.get_or_insert_with(|| {
                        Box::new(crate::workspace::WorkspaceRegistry::new())
                    });
                    reg.insert(ws_id, meta);
                }
                // WAL: WorkspaceCreate record (unframed, real type — K1a). The
                // registry is global, so its WAL stream is pinned to shard 0 —
                // one stream gives replay a total order over Create/Drop
                // regardless of which connection issued them. K1b: the payload
                // carries `created_at` so it survives a restart.
                let payload = crate::workspace::wal::encode_workspace_create(
                    ws_id.as_bytes(),
                    &ws_name,
                    created_at,
                );
                ctx.shard_databases.wal_append(
                    0,
                    crate::persistence::wal_v3::record::WalRecordType::WorkspaceCreate,
                    Bytes::from(payload),
                );
                responses.push(Frame::BulkString(Bytes::from(ws_id.to_string())));
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"DROP") {
        match validate_ws_drop(cmd_args) {
            Ok(ws_id_raw) => match parse_workspace_id_from_bytes(&ws_id_raw) {
                Some(ws_id) => {
                    let removed = {
                        let mut guard = ctx.shard_databases.workspace_registry();
                        match guard.as_mut() {
                            Some(reg) => reg.remove(&ws_id).is_some(),
                            None => false,
                        }
                    };
                    if removed {
                        // WAL: WorkspaceDrop record (unframed, real type — K1a).
                        let payload =
                            crate::workspace::wal::encode_workspace_drop(ws_id.as_bytes());
                        ctx.shard_databases.wal_append(
                            0,
                            crate::persistence::wal_v3::record::WalRecordType::WorkspaceDrop,
                            Bytes::from(payload),
                        );
                        // Best-effort cleanup: delete all KV keys with ws
                        // prefix (WS-03). Unconditional slice path.
                        let prefix = format!("{{{}}}:", ws_id.as_hex());
                        // The {wsid} hash tag co-locates every workspace key on ONE shard —
                        // derive the owner shard from the prefix.
                        let cleanup_owner =
                            crate::shard::dispatch::key_to_shard(prefix.as_bytes(), ctx.num_shards);
                        // Unconditional slice path: ShardSlice is always initialized.
                        if cleanup_owner == ctx.shard_id {
                            // Owner is this shard — operate directly on the
                            // slice. Sweep EVERY logical db on this shard, not
                            // just db 0 — a workspace-bound connection can
                            // SELECT to any db before writing (WS AUTH and
                            // SELECT are orthogonal), so workspace keys can
                            // legitimately live outside db 0. A db-0-only
                            // sweep leaked those keys forever after WS DROP
                            // (found during the WS5b hardening sweep; see
                            // docs/guides/isolation.md). Cost note:
                            // synchronous O(keys × --databases) full scan on
                            // this shard's event-loop thread — see the fuller
                            // comment on the `WsDropCleanup` handler in
                            // src/shard/spsc_handler.rs.
                            crate::shard::slice::with_shard(|s| {
                                for db in s.databases.iter_mut() {
                                    let keys_to_delete: Vec<Vec<u8>> = db
                                        .keys()
                                        .filter(|k| k.as_bytes().starts_with(prefix.as_bytes()))
                                        .map(|k| k.as_bytes().to_vec())
                                        .collect();
                                    for key in &keys_to_delete {
                                        db.remove(key);
                                    }
                                }
                            });
                        } else {
                            // Foreign shard: hop via WsDropCleanup message.
                            let prefix_bytes = Bytes::from(prefix.into_bytes());
                            let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
                            let msg = crate::shard::dispatch::ShardMessage::WsDropCleanup {
                                prefix: prefix_bytes,
                                reply_tx,
                            };
                            let _ = crate::shard::coordinator::spsc_send(
                                &ctx.dispatch_tx,
                                ctx.shard_id,
                                cleanup_owner,
                                msg,
                                &ctx.spsc_notifiers,
                            )
                            .await;
                            match crate::shard::coordinator::recv_reply_bounded(reply_rx).await {
                                Ok(count) => {
                                    tracing::debug!(
                                        "WS.DROP cleanup: deleted {} keys on shard {}",
                                        count,
                                        cleanup_owner
                                    );
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        "WS.DROP cleanup: reply channel closed for shard {}",
                                        cleanup_owner
                                    );
                                }
                            }
                        }
                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                    } else {
                        responses.push(Frame::Error(Bytes::from_static(ERR_WS_NOT_FOUND)));
                    }
                }
                None => responses.push(Frame::Error(Bytes::from_static(
                    crate::command::workspace::ERR_WS_INVALID_ID,
                ))),
            },
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"LIST") {
        match validate_ws_list(cmd_args) {
            Ok(()) => {
                let guard = ctx.shard_databases.workspace_registry();
                let entries: Vec<Frame> = match guard.as_ref() {
                    Some(reg) => reg
                        .iter()
                        .map(|(id, meta)| {
                            Frame::Array(
                                vec![
                                    Frame::BulkString(Bytes::from(id.to_string())),
                                    Frame::BulkString(meta.name.clone()),
                                    Frame::Integer(meta.created_at),
                                ]
                                .into(),
                            )
                        })
                        .collect(),
                    None => vec![],
                };
                responses.push(Frame::Array(entries.into()));
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"INFO") {
        match validate_ws_info(cmd_args) {
            Ok(ws_id_raw) => match parse_workspace_id_from_bytes(&ws_id_raw) {
                Some(ws_id) => {
                    let guard = ctx.shard_databases.workspace_registry();
                    let found = guard.as_ref().and_then(|reg| reg.get(&ws_id));
                    match found {
                        Some(meta) => {
                            responses.push(Frame::Array(
                                vec![
                                    Frame::BulkString(Bytes::from_static(b"id")),
                                    Frame::BulkString(Bytes::from(meta.id.to_string())),
                                    Frame::BulkString(Bytes::from_static(b"name")),
                                    Frame::BulkString(meta.name.clone()),
                                    Frame::BulkString(Bytes::from_static(b"created_at")),
                                    Frame::Integer(meta.created_at),
                                ]
                                .into(),
                            ));
                        }
                        None => responses.push(Frame::Error(Bytes::from_static(ERR_WS_NOT_FOUND))),
                    }
                }
                None => responses.push(Frame::Error(Bytes::from_static(
                    crate::command::workspace::ERR_WS_INVALID_ID,
                ))),
            },
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"AUTH") {
        match validate_ws_auth(cmd_args) {
            Ok(ws_id_raw) => {
                if conn.workspace_id.is_some() {
                    responses.push(Frame::Error(Bytes::from_static(ERR_WS_ALREADY_BOUND)));
                } else {
                    match parse_workspace_id_from_bytes(&ws_id_raw) {
                        Some(ws_id) => {
                            let found = {
                                let guard = ctx.shard_databases.workspace_registry();
                                guard
                                    .as_ref()
                                    .map_or(false, |reg| reg.get(&ws_id).is_some())
                            };
                            if found {
                                conn.workspace_id = Some(ws_id);
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            } else {
                                responses.push(Frame::Error(Bytes::from_static(ERR_WS_NOT_FOUND)));
                            }
                        }
                        None => responses.push(Frame::Error(Bytes::from_static(
                            crate::command::workspace::ERR_WS_INVALID_ID,
                        ))),
                    }
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    // Unknown WS subcommand
    responses.push(Frame::Error(Bytes::from_static(ERR_WS_UNKNOWN_SUB)));
    true
}

/// Build the workspace key-prefix bytes for MQ dispatch payloads.
///
/// Returns `"{ws_hex}:"` as `Bytes` when the connection is workspace-bound,
/// or `Bytes::new()` otherwise. Mirrors the prefix that `workspace_key()`
/// prepends to queue keys — passed to `MqCommandPayload.key_prefix` so the
/// owner shard can reconstruct effective keys without re-deriving them.
#[inline]
fn mq_key_prefix(workspace_id: Option<&crate::workspace::WorkspaceId>) -> bytes::Bytes {
    match workspace_id {
        None => bytes::Bytes::new(),
        Some(ws_id) => {
            let ws_hex = ws_id.as_hex();
            let mut buf = Vec::with_capacity(ws_hex.len() + 3); // '{' + hex + '}' + ':'
            buf.push(b'{');
            buf.extend_from_slice(ws_hex.as_bytes());
            buf.push(b'}');
            buf.push(b':');
            bytes::Bytes::from(buf)
        }
    }
}

/// Dispatch one MQ.* command to its owning shard via the SPSC hop.
///
/// If `owner == ctx.shard_id` (this shard owns the queue), executes
/// `execute_mq_on_owner` directly on the current thread (no channel round-
/// trip). Otherwise sends `ShardMessage::MqCommand` and awaits the reply.
///
/// Mirrors the GraphCommand precedent in `try_handle_graph_command`.
async fn mq_dispatch_to_owner(
    frame: &Frame,
    key_prefix: bytes::Bytes,
    owner: usize,
    db_index: usize,
    ctx: &ConnectionContext,
) -> Frame {
    let command_arc = std::sync::Arc::new(frame.clone());
    if owner == ctx.shard_id {
        // Self-shard: execute directly — no channel allocation needed.
        crate::shard::mq_exec::execute_mq_on_owner(db_index, key_prefix, command_arc)
    } else {
        // Foreign shard: send via SPSC and await the oneshot reply.
        let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
        let payload = crate::shard::dispatch::MqCommandPayload {
            db_index,
            key_prefix,
            command: command_arc,
            reply_tx,
        };
        let msg = crate::shard::dispatch::ShardMessage::MqCommand(Box::new(payload));
        let _ = crate::shard::coordinator::spsc_send(
            &ctx.dispatch_tx,
            ctx.shard_id,
            owner,
            msg,
            &ctx.spsc_notifiers,
        )
        .await;
        match crate::shard::coordinator::recv_reply_bounded(reply_rx).await {
            Ok(f) => f,
            Err(_) => Frame::Error(bytes::Bytes::from_static(
                b"ERR cross-shard MQ reply channel closed",
            )),
        }
    }
}

/// Handle MQ.* message queue commands. Returns `true` if consumed.
pub(super) async fn try_handle_mq_command(
    cmd: &[u8],
    cmd_args: &[Frame],
    frame: &Frame,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !is_mq_command(cmd) {
        return false;
    }
    let sub = match parse_mq_subcommand(cmd_args) {
        Ok(s) => s,
        Err(e) => {
            responses.push(e);
            return true;
        }
    };

    if sub.eq_ignore_ascii_case(b"CREATE") {
        match validate_mq_create(cmd_args) {
            Ok((queue_key, _max_delivery_count, _debounce_ms)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // A durable queue lives on the shard that owns its key — the
                // stream, registry entry, trigger entry, and WAL records must
                // all target `owner`, not the connection's shard, or fresh
                // connections landing elsewhere (SO_REUSEPORT) can't see the
                // queue. Owner-routing via MqCommand hop.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Unconditional slice path: owner-route via MqCommand hop (Wave B2).
                {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"PUSH") {
        match validate_mq_push(cmd_args) {
            Ok((queue_key, _fields)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Unconditional slice path: owner-route via MqCommand hop (Wave B2).
                {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"POP") {
        match validate_mq_pop(cmd_args) {
            Ok((queue_key, _count)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Unconditional slice path: owner-route via MqCommand hop (Wave B2).
                let response = {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await
                };
                responses.push(response);
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"ACK") {
        match validate_mq_ack(cmd_args) {
            Ok((queue_key, _msg_ids)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Unconditional slice path: owner-route via MqCommand hop (Wave B2).
                {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"DLQLEN") {
        match validate_mq_dlqlen(cmd_args) {
            Ok(queue_key) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner of the QUEUE key, not the dlq_key: POP creates the
                // DLQ stream in the same db as the queue it drains.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Unconditional slice path: owner-route via MqCommand hop (Wave B2).
                {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"TRIGGER") {
        match validate_mq_trigger(cmd_args) {
            Ok((queue_key, _callback_cmd, _debounce_ms)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner's registry: its event-loop tick fires triggers
                // (timers.rs documents the home shard as authoritative).
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Unconditional slice path: owner-route via MqCommand hop (Wave B2).
                {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"PUBLISH") {
        match validate_mq_publish(cmd_args) {
            Ok((queue_key, fields)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                if let Some(ref mut txn) = conn.active_cross_txn {
                    txn.record_mq(effective_key, fields);
                    responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                } else {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR MQ PUBLISH requires an active transaction (use TXN BEGIN first)",
                    )));
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    // Unknown MQ subcommand
    responses.push(Frame::Error(Bytes::from_static(ERR_MQ_UNKNOWN_SUB)));
    true
}

/// Handle MULTI/EXEC/DISCARD commands. Returns `true` if consumed.
///
/// `async` because EXEC now persists the transaction body to the shard AOF via
/// the same group-commit path as normal writes (previously this path logged
/// nothing, so every transactional write was lost on restart).
pub(super) async fn try_handle_multi_exec(
    cmd: &[u8],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
    exec_publishes: &mut Vec<(usize, Bytes, Bytes)>,
) -> bool {
    // --- MULTI ---
    if cmd.eq_ignore_ascii_case(b"MULTI") {
        if conn.in_cross_txn() {
            responses.push(Frame::Error(Bytes::from_static(ERR_MULTI_TXN_CONFLICT)));
        } else if conn.in_multi {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR MULTI calls can not be nested",
            )));
        } else {
            conn.in_multi = true;
            conn.command_queue.clear();
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        }
        return true;
    }

    // --- EXEC ---
    if cmd.eq_ignore_ascii_case(b"EXEC") {
        if !conn.in_multi {
            responses.push(Frame::Error(Bytes::from_static(b"ERR EXEC without MULTI")));
        } else {
            conn.in_multi = false;
            // The body runs on THIS shard with no per-key routing, so a
            // foreign-owned key would be silently misplaced. Classify locality:
            //  - CrossShard: genuinely spans shards — a shared-nothing engine
            //    can't commit it atomically, so reject with CROSSSLOT.
            //  - SingleShard(other): every key is owned by ONE remote shard —
            //    Phase B routes the whole body there for atomic execution + AOF
            //    on the owner (instead of the Phase-A CROSSSLOT rejection).
            //  - Keyless / SingleShard(self): fall through to local execution.
            if ctx.num_shards > 1 {
                match crate::server::conn::shared::analyze_txn_locality(
                    &conn.command_queue,
                    ctx.num_shards,
                ) {
                    crate::server::conn::shared::TxnLocality::SingleShard(s)
                        if s != ctx.shard_id =>
                    {
                        let commands: Vec<Frame> = conn.command_queue.to_vec();
                        conn.command_queue.clear();
                        // Keep a copy of the body for CLIENT TRACKING
                        // invalidation ONLY when tracking is active — the routed
                        // reply carries results, not the command frames, and the
                        // owner does not invalidate (the tracking table is
                        // process-global; invalidation is issued originating-side).
                        let tracking_cmds =
                            crate::tracking::tracking_active().then(|| commands.clone());
                        let reply = crate::shard::coordinator::execute_txn_on_owner(
                            s,
                            ctx.shard_id,
                            conn.selected_db,
                            commands,
                            &ctx.dispatch_tx,
                            &ctx.spsc_notifiers,
                        )
                        .await;
                        match reply {
                            Some(r) => {
                                if r.append_lost {
                                    exec_publishes.clear();
                                    responses.push(Frame::Error(Bytes::from_static(
                                        crate::shard::spsc_handler::AOF_APPEND_LOST_ERR,
                                    )));
                                    return true;
                                }
                                // appendfsync=always: confirm the owner's queued
                                // appends are on disk before acking (H1-BARRIER
                                // parity for normal cross-shard writes). No-op
                                // under everysec/no.
                                if r.wrote {
                                    if let Some(ref pool) = ctx.aof_pool {
                                        if pool.fsync_barrier(s).await.is_err() {
                                            exec_publishes.clear();
                                            responses.push(Frame::Error(Bytes::from_static(
                                                crate::persistence::aof::AOF_FSYNC_ERR,
                                            )));
                                            return true;
                                        }
                                    }
                                }
                                // CLIENT TRACKING: invalidate every key written by
                                // the routed body, same as the local EXEC path
                                // (which the early return would otherwise skip).
                                if let Some(cmds) = tracking_cmds.as_ref() {
                                    if let Frame::Array(ref txn_results) = r.result {
                                        for (i, cmd_frame) in cmds.iter().enumerate() {
                                            if i >= txn_results.len()
                                                || matches!(txn_results[i], Frame::Error(_))
                                            {
                                                continue;
                                            }
                                            if let Some((c, a)) =
                                                crate::server::conn::util::extract_command(
                                                    cmd_frame,
                                                )
                                            {
                                                crate::tracking::invalidation::invalidate_after_write(
                                                    &ctx.tracking_table,
                                                    c,
                                                    a,
                                                    conn.client_id,
                                                );
                                            }
                                        }
                                    }
                                }
                                // Adopt the owner's deferred PUBLISH fan-out; the
                                // caller's post-EXEC loop patches placeholders +
                                // scatters from this (originating) shard.
                                exec_publishes.extend(r.exec_publishes);
                                responses.push(r.result);
                            }
                            None => {
                                responses.push(Frame::Error(Bytes::from_static(
                                    b"CROSSSLOT MULTI/EXEC owner shard unavailable; retry",
                                )));
                            }
                        }
                        return true;
                    }
                    crate::server::conn::shared::TxnLocality::CrossShard => {
                        conn.command_queue.clear();
                        responses.push(Frame::Error(Bytes::from_static(
                            b"CROSSSLOT Keys in MULTI/EXEC don't hash to the same shard",
                        )));
                        return true;
                    }
                    _ => {}
                }
            }
            let (result, aof_entries, graph_records) = execute_transaction_sharded(
                &ctx.shard_databases,
                ctx.shard_id,
                &conn.command_queue,
                conn.selected_db,
                &ctx.cached_clock,
                exec_publishes,
            );
            // task #52: flush the graph-leg wal-v3 records collected by the
            // txn executor. Replication is monoio-only by design (see
            // `handler_monoio::write`'s EXEC handling) — this tokio/sharded
            // caller has no replication plane to fan out to, so it only
            // needs the local durability leg.
            #[cfg(feature = "graph")]
            for (_entry_db, record) in graph_records {
                ctx.shard_databases.wal_append(
                    ctx.shard_id,
                    crate::persistence::wal_v3::record::WalRecordType::Command,
                    bytes::Bytes::from(record),
                );
            }
            #[cfg(not(feature = "graph"))]
            let _ = graph_records;
            // DURABILITY: append every successful write in the body to THIS
            // shard's AOF via the same group-commit path as normal writes, then
            // issue ONE fsync barrier under appendfsync=always before acking.
            // All keys are local here (Phase A rejected foreign-owned bodies),
            // so ctx.shard_id is the correct AOF target. On barrier failure we
            // surface AOF_FSYNC_ERR instead of a false EXEC success — parity
            // with the normal write path.
            if crate::server::conn::shared::persist_txn_aof(ctx, aof_entries, false)
                .await
                .is_err()
            {
                conn.command_queue.clear();
                // Durability could not be guaranteed: report the error and
                // suppress any queued PUBLISH fan-out — the client sees EXEC
                // fail, so it must not observe the txn's pub/sub side effects.
                exec_publishes.clear();
                responses.push(Frame::Error(Bytes::from_static(
                    crate::persistence::aof::AOF_FSYNC_ERR,
                )));
                return true;
            }
            // CLIENT TRACKING: invalidate keys written inside the txn, same as
            // the normal write path (EXEC previously bypassed this). Self-gated
            // on tracking_active(); must run before command_queue is cleared.
            if crate::tracking::tracking_active() {
                if let Frame::Array(ref txn_results) = result {
                    for (i, cmd_frame) in conn.command_queue.iter().enumerate() {
                        if i >= txn_results.len() || matches!(txn_results[i], Frame::Error(_)) {
                            continue;
                        }
                        if let Some((c, a)) = crate::server::conn::util::extract_command(cmd_frame)
                        {
                            crate::tracking::invalidation::invalidate_after_write(
                                &ctx.tracking_table,
                                c,
                                a,
                                conn.client_id,
                            );
                        }
                    }
                }
            }
            conn.command_queue.clear();
            responses.push(result);
        }
        return true;
    }

    // --- DISCARD ---
    if cmd.eq_ignore_ascii_case(b"DISCARD") {
        if !conn.in_multi {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR DISCARD without MULTI",
            )));
        } else {
            conn.in_multi = false;
            conn.command_queue.clear();
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        }
        return true;
    }

    false
}

/// Handle GRAPH.* graph commands. Returns `true` if consumed.
#[cfg(feature = "graph")]
pub(super) async fn try_handle_graph_command(
    cmd: &[u8],
    cmd_args: &[Frame],
    frame: &Frame,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if cmd.len() <= 6 || !cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
        return false;
    }
    // Multi-shard: a graph lives on the shard that owns its NAME (same xxh64
    // + {tag} hashing as key routing). The per-shard graph store is
    // thread-local once ShardSlice is initialized, so non-owner commands MUST
    // hop via ShardMessage::GraphCommand — the shard-side handler dispatches
    // on its own store and drains graph WAL records locally.
    // GRAPH.LIST has no name argument: scatter to EVERY shard and union the
    // names (a local-only answer listed roughly 1/N of the graphs).
    if ctx.num_shards > 1 && cmd.eq_ignore_ascii_case(b"GRAPH.LIST") {
        let mut receivers = Vec::with_capacity(ctx.num_shards - 1);
        for target in 0..ctx.num_shards {
            if target == ctx.shard_id {
                continue;
            }
            let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
            let msg = crate::shard::dispatch::ShardMessage::GraphCommand {
                command: std::sync::Arc::new(frame.clone()),
                reply_tx,
            };
            let _ = crate::shard::coordinator::spsc_send(
                &ctx.dispatch_tx,
                ctx.shard_id,
                target,
                msg,
                &ctx.spsc_notifiers,
            )
            .await;
            receivers.push(reply_rx);
        }
        let mut remotes = Vec::with_capacity(receivers.len());
        for rx in receivers {
            if let Ok(f) = crate::shard::coordinator::recv_reply_bounded(rx).await {
                remotes.push(f);
            }
        }
        let local = crate::shard::slice::with_shard(|s| {
            crate::command::graph::dispatch_graph_read(&s.graph_store, cmd, cmd_args, None)
        });
        responses.push(crate::command::graph::merge_graph_list_responses(
            local, &remotes,
        ));
        return true;
    }
    if ctx.num_shards > 1 && !cmd.eq_ignore_ascii_case(b"GRAPH.LIST") {
        if let Some(name) = cmd_args.first().and_then(extract_bytes) {
            let owner = crate::shard::dispatch::graph_to_shard(&name, ctx.num_shards);
            if owner != ctx.shard_id {
                // Cypher WRITE queries inside a cross-store TXN cannot ship
                // their undo intents back across the hop — reject like the
                // other two-domain TXN cases (MOVE, COPY ... DB).
                if conn.in_cross_txn()
                    && cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
                    && crate::command::graph::is_cypher_write_query(cmd_args)
                {
                    responses.push(Frame::Error(bytes::Bytes::from_static(
                        crate::command::transaction::ERR_TXN_CROSS_SHARD,
                    )));
                    return true;
                }
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
                let mut response =
                    match crate::shard::coordinator::recv_reply_bounded(reply_rx).await {
                        Ok(f) => f,
                        Err(_) => Frame::Error(bytes::Bytes::from_static(
                            b"ERR cross-shard reply channel closed",
                        )),
                    };
                // Phase 166: explicit ADDNODE/ADDEDGE intents are captured
                // from the routed RESPONSE id, exactly like the local path;
                // the abort path routes the rollback back to the owner.
                if let Some(txn) = conn.active_cross_txn.as_mut() {
                    let is_node = cmd.eq_ignore_ascii_case(b"GRAPH.ADDNODE");
                    let is_edge = cmd.eq_ignore_ascii_case(b"GRAPH.ADDEDGE");
                    if is_node || is_edge {
                        if let Frame::Integer(id) = &response {
                            txn.record_graph(*id as u64, is_node, name.clone());
                        }
                    }
                }
                if let Some(ws_id) = conn.workspace_id.as_ref() {
                    strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                }
                responses.push(response);
                return true;
            }
        }
    }
    let is_write = crate::command::graph::is_graph_write_cmd(cmd)
        || (cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
            && crate::command::graph::is_cypher_write_query(cmd_args));
    // Unconditional slice path: ShardSlice is always initialized.
    let (response, wal_records, cypher_intents, cypher_undo_ops) =
        crate::shard::slice::with_shard(|s| {
            if is_write {
                let (resp, cypher_intents, undo_ops) = if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
                    crate::command::graph::graph_query_or_write(&mut s.graph_store, cmd_args)
                } else {
                    (
                        crate::command::graph::dispatch_graph_write(
                            &mut s.graph_store,
                            cmd,
                            cmd_args,
                        ),
                        Vec::new(),
                        Vec::new(),
                    )
                };
                let records = s.graph_store.drain_wal();
                (resp, records, cypher_intents, undo_ops)
            } else {
                // Task #32: connection-local read path -- `conn.protocol_version`
                // is known here, so the Cypher result cache is wired in.
                let resp = crate::command::graph::dispatch_graph_read(
                    &s.graph_store,
                    cmd,
                    cmd_args,
                    Some(conn.protocol_version),
                );
                (resp, Vec::new(), Vec::new(), Vec::new())
            }
        });
    // Phase 166: record graph intent for TXN rollback.
    if let Some(txn) = conn.active_cross_txn.as_mut() {
        let is_node = cmd.eq_ignore_ascii_case(b"GRAPH.ADDNODE");
        let is_edge = cmd.eq_ignore_ascii_case(b"GRAPH.ADDEDGE");
        if is_node || is_edge {
            if let Frame::Integer(id) = &response {
                if let Some(gname) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                    txn.record_graph(*id as u64, is_node, gname);
                }
            }
        }
        if !cypher_intents.is_empty() {
            if let Some(gname) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                for intent in &cypher_intents {
                    txn.record_graph(intent.entity_id, intent.is_node, gname.clone());
                }
            }
        }
        // Phase 174 FIX-01: push undo ops for SET/DELETE/MERGE rollback.
        for undo_op in cypher_undo_ops {
            txn.record_graph_undo(undo_op);
        }
    }
    for record in wal_records {
        ctx.shard_databases.wal_append(
            ctx.shard_id,
            crate::persistence::wal_v3::record::WalRecordType::Command,
            bytes::Bytes::from(record),
        );
    }
    let mut response = response;
    if let Some(ws_id) = conn.workspace_id.as_ref() {
        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
    }
    responses.push(response);
    true
}
