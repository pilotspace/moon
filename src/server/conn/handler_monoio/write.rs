//! Write-path command handlers: WS.*, MQ.*, MULTI/EXEC/DISCARD, GRAPH.*.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use std::cell::RefCell;
use std::rc::Rc;

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
use crate::storage::stream::StreamId;
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
                // Wave B ws-plane: the registry is process-global, so the
                // WHOLE mutation+WAL+replication-record sequence pins to
                // shard 0's own thread — the replication offset advance must
                // stay in the SAME synchronous stretch as the mutation (see
                // `record_local_write`'s snapshot-consistency argument). A
                // connection whose OWN shard is 0 runs the sequence inline
                // (no hop, already on that thread); every other connection
                // sends `WsRegistryCreate` to shard 0 and awaits the reply —
                // same dual-path shape as `WsDropCleanup` below, except the
                // "owner" here is always shard 0.
                if ctx.shard_id == 0 {
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
                    // WAL: WorkspaceCreate record (unframed, real type — K1a).
                    // K1b: the payload carries `created_at` so it survives a
                    // restart.
                    let wal_payload = crate::workspace::wal::encode_workspace_create(
                        ws_id.as_bytes(),
                        &ws_name,
                        created_at,
                    );
                    ctx.shard_databases.wal_append(
                        0,
                        crate::persistence::wal_v3::record::WalRecordType::WorkspaceCreate,
                        Bytes::from(wal_payload.clone()),
                    );
                    // Wave B ws-plane: live replication push (same payload as
                    // the WAL record, wrapped as the internal
                    // WS.CREATE.APPLY pseudo-command — see
                    // `workspace::repl`). Gated on fanout-active exactly like
                    // the graph plane's `record_local_write_db` call.
                    if super::ft::replication_fanout_active(ctx) {
                        let record =
                            crate::workspace::repl::serialize_ws_create_apply(&wal_payload);
                        super::ft::record_local_write_db(
                            ctx,
                            conn.selected_db,
                            Bytes::from(record),
                        );
                    }
                    responses.push(Frame::BulkString(Bytes::from(ws_id.to_string())));
                } else {
                    let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
                    let msg = crate::shard::dispatch::ShardMessage::WsRegistryCreate(Box::new(
                        crate::shard::dispatch::WsRegistryCreatePayload {
                            ws_id: *ws_id.as_bytes(),
                            name: ws_name.clone(),
                            created_at,
                            reply_tx,
                        },
                    ));
                    let _ = crate::shard::coordinator::spsc_send(
                        &ctx.dispatch_tx,
                        ctx.shard_id,
                        0,
                        msg,
                        &ctx.spsc_notifiers,
                    )
                    .await;
                    // Fail CLOSED on hop failure (adversarial-review P0): a
                    // wedged ring or reply timeout must not report a phantom
                    // ws_id the registry never saw. The registry itself is
                    // process-global, so a post-failure probe disambiguates
                    // "shard 0 executed but the reply was slow/lost" (entry
                    // present → success) from "the mutation never happened"
                    // (entry absent → retryable error).
                    let hop_ok = crate::shard::coordinator::recv_reply_bounded(reply_rx)
                        .await
                        .is_ok();
                    let created = hop_ok || {
                        let guard = ctx.shard_databases.workspace_registry();
                        guard.as_ref().is_some_and(|reg| reg.get(&ws_id).is_some())
                    };
                    if created {
                        responses.push(Frame::BulkString(Bytes::from(ws_id.to_string())));
                    } else {
                        responses.push(Frame::Error(Bytes::from_static(
                            b"ERR workspace create failed (shard-0 registry hop timed out); not created, safe to retry",
                        )));
                    }
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"DROP") {
        match validate_ws_drop(cmd_args) {
            Ok(ws_id_raw) => {
                match parse_workspace_id_from_bytes(&ws_id_raw) {
                    Some(ws_id) => {
                        // Wave B ws-plane: same shard-0 hop as WS.CREATE above.
                        let removed = if ctx.shard_id == 0 {
                            let removed = {
                                let mut guard = ctx.shard_databases.workspace_registry();
                                match guard.as_mut() {
                                    Some(reg) => reg.remove(&ws_id).is_some(),
                                    None => false,
                                }
                            };
                            if removed {
                                // WAL: WorkspaceDrop record (unframed, real type — K1a).
                                let wal_payload =
                                    crate::workspace::wal::encode_workspace_drop(ws_id.as_bytes());
                                ctx.shard_databases.wal_append(
                                    0,
                                    crate::persistence::wal_v3::record::WalRecordType::WorkspaceDrop,
                                    Bytes::from(wal_payload.clone()),
                                );
                                if super::ft::replication_fanout_active(ctx) {
                                    let record = crate::workspace::repl::serialize_ws_drop_apply(
                                        &wal_payload,
                                    );
                                    super::ft::record_local_write_db(
                                        ctx,
                                        conn.selected_db,
                                        Bytes::from(record),
                                    );
                                }
                            }
                            removed
                        } else {
                            let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
                            let msg = crate::shard::dispatch::ShardMessage::WsRegistryDrop {
                                ws_id: *ws_id.as_bytes(),
                                reply_tx,
                            };
                            let _ = crate::shard::coordinator::spsc_send(
                                &ctx.dispatch_tx,
                                ctx.shard_id,
                                0,
                                msg,
                                &ctx.spsc_notifiers,
                            )
                            .await;
                            match crate::shard::coordinator::recv_reply_bounded(reply_rx).await {
                                Ok(removed) => removed,
                                // Timeout/err ≠ failure (adversarial-review
                                // P1): shard 0 may have completed the drop
                                // and only the REPLY was lost — resolving
                                // `false` here skipped the `{ws_hex}:` key
                                // sweep below FOREVER (the registry entry is
                                // gone, so no WS.DROP retry can re-trigger
                                // it). The registry is process-global:
                                // probe it — entry gone → the drop landed
                                // (run the sweep; it is idempotent), entry
                                // still present → the hop genuinely never
                                // executed → fall through to
                                // ERR_WS_NOT_FOUND-side handling as before.
                                Err(_) => {
                                    let guard = ctx.shard_databases.workspace_registry();
                                    guard.as_ref().is_none_or(|reg| reg.get(&ws_id).is_none())
                                }
                            }
                        };
                        if removed {
                            // Best-effort cleanup: delete all KV keys with ws prefix (WS-03).
                            // Owner-route the cleanup via WsDropCleanup hop. The {wsid} hash
                            // tag co-locates every workspace key on ONE shard.
                            {
                                let prefix = format!("{{{}}}:", ws_id.as_hex());
                                let prefix_bytes = Bytes::from(prefix.into_bytes());
                                let owner = crate::shard::dispatch::key_to_shard(
                                    &prefix_bytes,
                                    ctx.num_shards,
                                );
                                if owner == ctx.shard_id {
                                    // Self: execute locally (we ARE the owner).
                                    // Sweep EVERY logical db on this shard, not
                                    // just db 0 — a workspace-bound connection
                                    // can SELECT to any db before writing (WS
                                    // AUTH and SELECT are orthogonal), so
                                    // workspace keys can legitimately live
                                    // outside db 0. A db-0-only sweep leaked
                                    // those keys forever after WS DROP (found
                                    // during the WS5b hardening sweep; see
                                    // docs/guides/isolation.md). Cost note:
                                    // synchronous O(keys × --databases) full
                                    // scan on this shard's event-loop thread
                                    // — see the fuller comment on the
                                    // `WsDropCleanup` handler in
                                    // src/shard/spsc_handler.rs.
                                    crate::shard::slice::with_shard(|s| {
                                        for db in s.databases.iter_mut() {
                                            let keys_to_delete: Vec<Vec<u8>> = db
                                                .keys()
                                                .filter(|k| {
                                                    k.as_bytes().starts_with(prefix_bytes.as_ref())
                                                })
                                                .map(|k| k.as_bytes().to_vec())
                                                .collect();
                                            for key in &keys_to_delete {
                                                db.remove(key);
                                            }
                                        }
                                    });
                                } else {
                                    // Foreign: send WsDropCleanup hop to owner.
                                    let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
                                    let msg = crate::shard::dispatch::ShardMessage::WsDropCleanup {
                                        prefix: prefix_bytes,
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
                                    let _ = crate::shard::coordinator::recv_reply_bounded(reply_rx)
                                        .await;
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
                }
            }
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

    // Wave B stage 2b: MQ mutations now replicate live (see
    // `shard::mq_exec::replicate_mq_record`) whenever `ctx.num_shards == 1`
    // — the same single-shard gate graph's own live stream uses — so the
    // round-2 `warn_mq_unreplicated` fail-loud no longer applies and is
    // retired (its WS.* half was already retired by the WS-plane work).
    // A multi-shard master still doesn't live-stream MQ writes (out of
    // scope, tracked alongside graph's own un-warned multi-shard gap), but
    // per graph precedent that gap is accepted silently rather than warned;
    // a durable queue stays WAL-durable and FULLRESYNC-covered either way.

    if sub.eq_ignore_ascii_case(b"CREATE") {
        match validate_mq_create(cmd_args) {
            Ok((queue_key, _max_delivery_count, _debounce_ms)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // A durable queue lives on the shard that owns its key — the
                // stream, registry entry, trigger entry, and WAL records must
                // all target `owner`, not the connection's shard, or fresh
                // connections landing elsewhere (SO_REUSEPORT) can't see the
                // queue. Lock-mode ShardDatabases permits direct cross-shard
                // access; the ShardSlice branches stay conn-local because
                // slice mode is never initialized yet (owner-routing there is
                // the shardslice-migration task).
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Owner-routes via MqCommand hop (execute_mq_on_owner
                // handles stream create, registry insert, and WAL in one step).
                {
                    let key_prefix = mq_ws_prefix(conn.workspace_id.as_ref());
                    let response = mq_hop_or_local(
                        owner,
                        conn.selected_db,
                        ctx,
                        key_prefix,
                        std::sync::Arc::new(frame.clone()),
                    )
                    .await;
                    responses.push(response);
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
                // Owner-routes via MqCommand hop (execute_mq_on_owner
                // handles stream push, trigger debounce in one step).
                {
                    let key_prefix = mq_ws_prefix(conn.workspace_id.as_ref());
                    let response = mq_hop_or_local(
                        owner,
                        conn.selected_db,
                        ctx,
                        key_prefix,
                        std::sync::Arc::new(frame.clone()),
                    )
                    .await;
                    responses.push(response);
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
                let _group_name = Bytes::from_static(b"__mq_consumers");
                let _consumer_name = Bytes::from_static(b"__mq_default");

                // Owner-routes via MqCommand hop (execute_mq_on_owner
                // handles claim, DLQ routing in one step).
                {
                    let key_prefix = mq_ws_prefix(conn.workspace_id.as_ref());
                    let response = mq_hop_or_local(
                        owner,
                        conn.selected_db,
                        ctx,
                        key_prefix,
                        std::sync::Arc::new(frame.clone()),
                    )
                    .await;
                    responses.push(response);
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"ACK") {
        match validate_mq_ack(cmd_args) {
            Ok((queue_key, msg_ids)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                let _ids: Vec<StreamId> = msg_ids
                    .iter()
                    .map(|(ms, seq)| StreamId { ms: *ms, seq: *seq })
                    .collect();
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Owner-routes via MqCommand hop (execute_mq_on_owner
                // handles xack and WAL records in one step).
                {
                    let key_prefix = mq_ws_prefix(conn.workspace_id.as_ref());
                    let response = mq_hop_or_local(
                        owner,
                        conn.selected_db,
                        ctx,
                        key_prefix,
                        std::sync::Arc::new(frame.clone()),
                    )
                    .await;
                    responses.push(response);
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
                let _dlq_key = {
                    let mut buf = Vec::with_capacity(effective_key.len() + 8);
                    buf.extend_from_slice(&effective_key);
                    buf.extend_from_slice(b"::mq:dlq");
                    Bytes::from(buf)
                };
                // Owner-routes via MqCommand hop.
                {
                    let key_prefix = mq_ws_prefix(conn.workspace_id.as_ref());
                    let response = mq_hop_or_local(
                        owner,
                        conn.selected_db,
                        ctx,
                        key_prefix,
                        std::sync::Arc::new(frame.clone()),
                    )
                    .await;
                    responses.push(response);
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
                // Owner-routes via MqCommand hop (execute_mq_on_owner
                // registers the trigger in the owner's slice registry).
                {
                    let key_prefix = mq_ws_prefix(conn.workspace_id.as_ref());
                    let response = mq_hop_or_local(
                        owner,
                        conn.selected_db,
                        ctx,
                        key_prefix,
                        std::sync::Arc::new(frame.clone()),
                    )
                    .await;
                    responses.push(response);
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

// ── shardslice-migration Wave B1 helpers ─────────────────────────────────────

/// Build the workspace prefix `"{ws_hex}:"` used as `MqCommandPayload.key_prefix`.
///
/// When the connection is not workspace-bound this returns `Bytes::new()`,
/// matching `MqCommandPayload`'s "empty = no prefix" contract.
#[inline]
fn mq_ws_prefix(workspace_id: Option<&crate::workspace::WorkspaceId>) -> Bytes {
    match workspace_id {
        None => Bytes::new(),
        Some(ws_id) => {
            let ws_hex = ws_id.as_hex();
            // "{" + 32 hex + "}" + ":" = 35 bytes
            let mut buf = Vec::with_capacity(35);
            buf.push(b'{');
            buf.extend_from_slice(ws_hex.as_bytes());
            buf.push(b'}');
            buf.push(b':');
            Bytes::from(buf)
        }
    }
}

/// Send an MQ command to the owning shard (or run locally if self).
///
/// When `owner == ctx.shard_id` the command runs synchronously via
/// `mq_exec::execute_mq_on_owner` (same thread, slice live — no hop overhead).
/// Otherwise a `ShardMessage::MqCommand` is pushed to the SPSC ring and the
/// caller awaits the oneshot reply.
///
/// `db_index` = `conn.selected_db`; `owner` = `key_to_shard(effective_key)`.
///
/// Returns the `Frame` response to push to the client.
async fn mq_hop_or_local(
    owner: usize,
    db_index: usize,
    ctx: &ConnectionContext,
    key_prefix: Bytes,
    command: std::sync::Arc<crate::protocol::Frame>,
) -> crate::protocol::Frame {
    if owner == ctx.shard_id {
        // Self-short-circuit: run directly on this shard's slice.
        crate::shard::mq_exec::execute_mq_on_owner(db_index, key_prefix, command)
    } else {
        // Cross-shard hop via MqCommand SPSC message (GraphCommand precedent).
        let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
        let payload = crate::shard::dispatch::MqCommandPayload {
            db_index,
            key_prefix,
            command,
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
            Err(_) => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR cross-shard MQ reply channel closed",
            )),
        }
    }
}

/// Handle MULTI/EXEC/DISCARD commands. Returns `true` if consumed.
///
/// `async` because EXEC now persists the transaction body to the shard AOF via
/// the same group-commit path as normal writes (previously this path logged
/// nothing, so every transactional write was lost on restart — including at
/// `--shards 1` under the monoio TopLevel writer).
#[allow(clippy::too_many_arguments)]
pub(super) async fn try_handle_multi_exec(
    cmd: &[u8],
    args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
    exec_publishes: &mut Vec<(usize, Bytes, Bytes)>,
    // moon#639: EXEC now runs the queued connection-level intercepts itself.
    // `SCRIPT LOAD` needs the shutdown token for its bounded shard fan-out and
    // `HELLO` needs the codec to switch protocol, so both travel down here.
    shutdown: &crate::runtime::cancel::CancellationToken,
    codec: &mut crate::server::codec::RespCodec,
    // moon#697: FUNCTION joined the connection-level intercepts, and its
    // registry is per-SHARD-THREAD (the `RefCell` is shared with that thread's
    // SPSC drain loop, which applies inbound fan-outs). It therefore has to
    // travel down here rather than be rebuilt: a fresh registry would be
    // invisible to the fan-out that makes FUNCTION LOAD server-wide.
    func_registry: &Rc<RefCell<Option<crate::scripting::FunctionRegistry>>>,
) -> bool {
    // --- WATCH / UNWATCH ---
    // Before the MULTI queueing step below, so `WATCH` inside MULTI is refused
    // rather than queued. Shared with the other production handler on purpose:
    // two copies of this arm is how the paths drifted in the first place.
    if crate::server::conn::watch::try_handle_watch_unwatch(cmd, args, conn, ctx, responses).await {
        return true;
    }

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
            // A transaction poisoned at queue time executes NOTHING. Redis's
            // CLIENT_DIRTY_EXEC: the client was already told which command was
            // bad; EXEC's job is to make sure none of the others ran.
            if std::mem::take(&mut conn.multi_dirty) {
                conn.command_queue.clear();
                conn.watched_keys.clear();
                responses.push(crate::server::conn::shared::execabort_frame());
                return true;
            }
            // Taken, not borrowed: EXEC must clear its watches on BOTH the
            // committed and the aborted outcome, and a stale watch surviving
            // an abort is how a CAS retry loop livelocks.
            let watched = std::mem::take(&mut conn.watched_keys);
            // The body runs on THIS shard with no per-key routing, so a
            // foreign-owned key would be silently misplaced. Classify locality:
            //  - CrossShard: genuinely spans shards — a shared-nothing engine
            //    can't commit it atomically, so reject with CROSSSLOT.
            //  - SingleShard(other): every key is owned by ONE remote shard —
            //    Phase B routes the whole body there for atomic execution + AOF
            //    on the owner (instead of the Phase-A CROSSSLOT rejection).
            //  - Keyless / SingleShard(self): fall through to local execution.
            if ctx.num_shards > 1 {
                match crate::server::conn::shared::merge_locality(
                    crate::server::conn::shared::analyze_txn_locality(
                        &conn.command_queue,
                        ctx.num_shards,
                    ),
                    // A watched key owned by another shard cannot be validated
                    // where the body commits — refuse rather than fabricate a
                    // conflict the client can never clear.
                    crate::server::conn::shared::analyze_watch_locality(&watched, ctx.num_shards),
                ) {
                    crate::server::conn::shared::TxnLocality::SingleShard(s)
                        if s != ctx.shard_id =>
                    {
                        let commands: Vec<Frame> = conn.command_queue.to_vec();
                        conn.command_queue.clear();
                        // moon#639: the owner shard cannot run connection-level
                        // intercepts (no connection there), so it leaves a
                        // placeholder per queued one and we fill them when the
                        // reply comes back. Cloned only when the body actually
                        // contains one, so an ordinary routed EXEC is unchanged.
                        let queued_for_intercepts =
                            crate::server::conn::shared::txn_intercept_snapshot(&commands);
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
                            conn.protocol_version,
                            // The CAS check runs where the body runs. Cloned
                            // rather than borrowed because the payload crosses
                            // an SPSC hop and must own its tokens; the map is
                            // empty for every transaction that did not WATCH.
                            watched.clone(),
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
                                // c10k E2: the owner flushed only ITS slice.
                                // Broadcast to the rest from here (the owner
                                // cannot fan out from inside its own message
                                // loop) and patch the result on a failed leg.
                                let mut routed_result = r.result;
                                // moon#639: the owner shard ran the keyspace
                                // body and left a placeholder for every queued
                                // connection-level intercept — it has no
                                // connection to run them on. Fill them here,
                                // where the connection lives.
                                super::dispatch::fill_txn_intercept_slots(
                                    &mut routed_result,
                                    &queued_for_intercepts,
                                    conn,
                                    ctx,
                                    shutdown,
                                    codec,
                                    // The outer index this EXEC reply will
                                    // occupy — a queued HELLO records its
                                    // protocol switch there, not at 0.
                                    responses.len(),
                                    func_registry,
                                )
                                .await;
                                crate::shard::coordinator::broadcast_txn_flushes(
                                    &mut routed_result,
                                    &r.exec_flushes,
                                    // Sender is THIS connection's shard; the
                                    // owner `s` ran the body and is the leg
                                    // already flushed (moon#705).
                                    ctx.shard_id,
                                    s,
                                    ctx.num_shards,
                                    &ctx.dispatch_tx,
                                    &ctx.spsc_notifiers,
                                )
                                .await;
                                responses.push(routed_result);
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
            // c10k E2: a queued FLUSHDB/FLUSHALL clears only this shard.
            let mut exec_flushes: Vec<(usize, Frame, usize)> = Vec::new();
            // moon#606: keys the body wrote that a blocked client may be on.
            let mut exec_wakes: Vec<(usize, bytes::Bytes, crate::blocking::WaitFamily)> =
                Vec::new();
            let (mut result, aof_entries, graph_records) = execute_transaction_sharded(
                &ctx.shard_databases,
                ctx.shard_id,
                &conn.command_queue,
                conn.selected_db,
                conn.protocol_version,
                &ctx.cached_clock,
                exec_publishes,
                &mut exec_flushes,
                &mut exec_wakes,
                &watched,
            );
            // moon#639: fill the slots the executor left for connection-level
            // intercepts. Snapshotted first because filling takes `&mut conn`
            // and the queue lives on it; the clone happens only when the body
            // actually contains an intercept.
            {
                let queued_for_intercepts =
                    crate::server::conn::shared::txn_intercept_snapshot(&conn.command_queue);
                super::dispatch::fill_txn_intercept_slots(
                    &mut result,
                    &queued_for_intercepts,
                    conn,
                    ctx,
                    shutdown,
                    codec,
                    responses.len(),
                    func_registry,
                )
                .await;
            }
            // moon#606: raise the wakes the body recorded. A producer queued
            // inside MULTI reaches none of the live write path's hooks, so
            // without this a `MULTI ; LPUSH k v ; EXEC` left a client blocked
            // on `k` asleep until its own timeout.
            //
            // Positioned exactly where the live path's hooks sit relative to
            // the AOF barrier, and raised whether or not that barrier later
            // fails: the elements are in the keyspace either way (an EXEC that
            // cannot be persisted is reported as an error, not rolled back), so
            // a waiter left asleep would answer null for a key that
            // demonstrably has data.
            for (wake_db, wake_key, family) in exec_wakes.drain(..) {
                let mut reg = ctx.blocking_registry.borrow_mut();
                crate::shard::slice::with_shard_db(wake_db, |db| {
                    crate::blocking::wakeup::wake_family(&mut reg, db, wake_db, &wake_key, family);
                });
            }

            // v0.7 REPLICATION (adversarial-review P0-1): the txn body must
            // reach replicas like any other successful local write. This was
            // the ONE local write path that skipped the replication plane —
            // `MULTI/SET/EXEC` at shards=1 committed on the master and never
            // reached the replica (silent deterministic divergence). Record
            // each body entry HERE, in the same synchronous stretch as the
            // just-returned (fully synchronous) `execute_transaction_sharded`
            // — atomic w.r.t. the inline PSYNC snapshot capture — and tell
            // `persist_txn_aof` not to double-advance the offset (lsn = 0,
            // same contract as the single-command legs).
            let repl_active = super::ft::replication_fanout_active(ctx);
            if repl_active {
                // Round-2 finding G (throughput note): each entry pushes one
                // ReplicaLiveFanout onto the self queue, and the drain
                // preamble processes the whole burst before the shard's
                // bounded SPSC consumers — a very large EXEC body is a tail-
                // latency vector for cross-shard traffic sharing this thread.
                // Each drain iteration is just a try_send per replica, so the
                // burst is cheap; revisit only if EXEC bodies grow unbounded.
                // PR #282 review: per-entry db — a SELECT queued inside the
                // body redirects the commands after it, so the replication
                // stream must bind each record to ITS execution db.
                for (entry_db, bytes) in &aof_entries {
                    super::ft::record_local_write_db(ctx, *entry_db, bytes.clone());
                }
            }
            // task #52 review round 2 (P1): the graph-leg records need the
            // SAME replicate-then-append treatment as the live single-command
            // graph path (`try_handle_graph_command`, ~line 1123 below) —
            // `execute_transaction_sharded` only collects them (it has no
            // `ConnectionContext`/replication access), so the fan-out and the
            // wal_append both happen HERE, in the same synchronous stretch as
            // the txn body that just ran. Graph replication is single-shard
            // scope only (multi-shard graph replication rides the R2
            // broadcast redesign), matching the live path's `num_shards == 1`
            // gate exactly — this is NOT relaxed to the KV leg's
            // `repl_active` alone.
            #[cfg(feature = "graph")]
            {
                let graph_repl_active = ctx.num_shards == 1 && repl_active;
                for (entry_db, record) in &graph_records {
                    if graph_repl_active {
                        super::ft::record_local_write_db(
                            ctx,
                            *entry_db,
                            bytes::Bytes::from(record.clone()),
                        );
                    }
                    ctx.shard_databases.wal_append(
                        ctx.shard_id,
                        crate::persistence::wal_v3::record::WalRecordType::Command,
                        bytes::Bytes::from(record.clone()),
                    );
                }
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
            if crate::server::conn::shared::persist_txn_aof(ctx, aof_entries, repl_active)
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
            // c10k E2: the body cleared only THIS shard's slice. Broadcast the
            // flushes now — after the local body, its AOF and its replication
            // leg, so the ordering matches the live path — and patch the
            // result if any leg fails, so a `+OK` for a flush inside a
            // transaction can be trusted.
            crate::shard::coordinator::broadcast_txn_flushes(
                &mut result,
                &exec_flushes,
                ctx.shard_id,
                // Local EXEC: the body ran on this connection's own shard.
                ctx.shard_id,
                ctx.num_shards,
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
            )
            .await;
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
            // DISCARD must clear the poison too, or the NEXT transaction on
            // this connection aborts for a fault that was never its own.
            conn.multi_dirty = false;
            conn.watched_keys.clear();
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
                    // #499: poison the txn so COMMIT cannot report OK.
                    conn.mark_cross_txn_rejected(cmd);
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
    // Unconditional slice path: dispatch to ShardSlice::graph_store directly.
    let (response, wal_records, cypher_intents, cypher_undo_ops) =
        crate::shard::slice::with_shard(|s| {
            if crate::command::graph::is_graph_write_cmd(cmd)
                || (cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
                    && crate::command::graph::is_cypher_write_query(cmd_args))
            {
                let gs = &mut s.graph_store;
                let (resp, cypher_intents, undo_ops) = if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
                    crate::command::graph::graph_query_or_write(gs, cmd_args)
                } else {
                    (
                        crate::command::graph::dispatch_graph_write(gs, cmd, cmd_args),
                        Vec::new(),
                        Vec::new(),
                    )
                };
                let records = gs.drain_wal();
                (resp, records, cypher_intents, undo_ops)
            } else {
                let gs = &s.graph_store;
                // Task #32: connection-local read path -- `conn.protocol_version`
                // is known here, so the Cypher result cache is wired in.
                let resp = crate::command::graph::dispatch_graph_read(
                    gs,
                    cmd,
                    cmd_args,
                    Some(conn.protocol_version),
                );
                (resp, Vec::new(), Vec::new(), Vec::new())
            }
        });
    // Phase 166: record graph intent for TXN rollback.
    // Captures explicit ADDNODE/ADDEDGE by response id plus
    // Phase 167 Cypher CREATE/MERGE via intents returned from
    // graph_query_or_write.
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
    // v0.7 graph replication: the drained WAL records are the DETERMINISTIC,
    // id-pinned form of this mutation (GRAPH.ADDNODE <g> <node_id> …, FNV-
    // hashed u16 label/prop ids — `label_to_id` is stateless, so master and
    // replica agree). Record them verbatim in the replication plane
    // (`record_local_write`: backlog + offset synchronously, live fan-out at
    // the next drain), which replicas replay via `GraphReplayCollector`
    // without re-allocating ids. Only the replication legs — the WAL copy is
    // the local `wal_append` below, so nothing double-logs. Single-shard
    // scope, matching the R0/R0.5 FT.* leg (multi-shard graph replication
    // rides the R2 broadcast redesign).
    let wal_records: Vec<bytes::Bytes> = wal_records.into_iter().map(bytes::Bytes::from).collect();
    if !wal_records.is_empty() && ctx.num_shards == 1 && super::ft::replication_fanout_active(ctx) {
        // Recording in the same synchronous stretch as the graph mutation
        // keeps mutation + replication record atomic w.r.t. the inline PSYNC
        // task's snapshot capture on this thread.
        for record in &wal_records {
            super::ft::record_local_write_db(ctx, conn.selected_db, record.clone());
        }
    }
    for record in wal_records {
        ctx.shard_databases.wal_append(
            ctx.shard_id,
            crate::persistence::wal_v3::record::WalRecordType::Command,
            record,
        );
    }
    let mut response = response;
    if let Some(ws_id) = conn.workspace_id.as_ref() {
        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
    }
    responses.push(response);
    true
}
