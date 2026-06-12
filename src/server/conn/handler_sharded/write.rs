//! Write-path command handlers: WS.*, MQ.*, MULTI/EXEC/DISCARD, GRAPH.*.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use bytes::Bytes;

use crate::command::mq::{
    ERR_MQ_NOT_DURABLE, ERR_MQ_UNKNOWN_SUB, parse_mq_subcommand, validate_mq_ack,
    validate_mq_create, validate_mq_dlqlen, validate_mq_pop, validate_mq_publish, validate_mq_push,
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
                // WAL: WorkspaceCreate record. The registry is global, so its
                // WAL stream is pinned to shard 0 — one stream gives replay a
                // total order over Create/Drop regardless of which connection
                // issued them.
                let payload =
                    crate::workspace::wal::encode_workspace_create(ws_id.as_bytes(), &ws_name);
                let mut wal_buf = Vec::new();
                crate::persistence::wal_v3::record::write_wal_v3_record(
                    &mut wal_buf,
                    0,
                    crate::persistence::wal_v3::record::WalRecordType::WorkspaceCreate,
                    &payload,
                );
                ctx.shard_databases.wal_append(0, Bytes::from(wal_buf));
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
                        // WAL: WorkspaceDrop record
                        let payload =
                            crate::workspace::wal::encode_workspace_drop(ws_id.as_bytes());
                        let mut wal_buf = Vec::new();
                        crate::persistence::wal_v3::record::write_wal_v3_record(
                            &mut wal_buf,
                            0,
                            crate::persistence::wal_v3::record::WalRecordType::WorkspaceDrop,
                            &payload,
                        );
                        ctx.shard_databases.wal_append(0, Bytes::from(wal_buf));
                        // Best-effort cleanup: delete all KV keys with ws
                        // prefix (WS-03).
                        // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                        let prefix = format!("{{{}}}:", ws_id.as_hex());
                        // The {wsid} hash tag co-locates every workspace key on ONE shard —
                        // compute owner before the gate so both arms share the derivation.
                        let cleanup_owner =
                            crate::shard::dispatch::key_to_shard(prefix.as_bytes(), ctx.num_shards);
                        if crate::shard::slice::is_initialized() {
                            if cleanup_owner == ctx.shard_id {
                                // Owner is this shard — operate directly on the slice.
                                crate::shard::slice::with_shard_db(0, |db| {
                                    let keys_to_delete: Vec<Vec<u8>> = db
                                        .keys()
                                        .filter(|k| k.as_bytes().starts_with(prefix.as_bytes()))
                                        .map(|k| k.as_bytes().to_vec())
                                        .collect();
                                    for key in &keys_to_delete {
                                        db.remove(key);
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
                                crate::shard::coordinator::spsc_send(
                                    &ctx.dispatch_tx,
                                    ctx.shard_id,
                                    cleanup_owner,
                                    msg,
                                    &ctx.spsc_notifiers,
                                )
                                .await;
                                match reply_rx.recv().await {
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
                        } else {
                            // Lock path (byte-identical to pre-migration).
                            let mut db_guard = ctx.shard_databases.write_db(cleanup_owner, 0);
                            let keys_to_delete: Vec<Vec<u8>> = db_guard
                                .keys()
                                .filter(|k| k.as_bytes().starts_with(prefix.as_bytes()))
                                .map(|k| k.as_bytes().to_vec())
                                .collect();
                            for key in &keys_to_delete {
                                db_guard.remove(key);
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
        crate::shard::coordinator::spsc_send(
            &ctx.dispatch_tx,
            ctx.shard_id,
            owner,
            msg,
            &ctx.spsc_notifiers,
        )
        .await;
        match reply_rx.recv().await {
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
            Ok((queue_key, max_delivery_count, _debounce_ms)) => {
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
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                // Slice path: owner-route via MqCommand hop (shardslice-migration Wave B2).
                if crate::shard::slice::is_initialized() {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
                // Lock path (byte-identical to pre-migration).
                let create_result: Result<(), Frame> = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    let r = match db_guard.get_or_create_stream(&effective_key) {
                        Ok(stream) => {
                            stream.durable = true;
                            stream.max_delivery_count = max_delivery_count;
                            let group_name = Bytes::from_static(b"__mq_consumers");
                            let _ = stream.create_group(group_name, StreamId::ZERO);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    };
                    drop(db_guard);
                    r
                };
                if let Err(e) = create_result {
                    responses.push(e);
                    return true;
                }

                let config =
                    crate::mq::DurableStreamConfig::new(effective_key.clone(), max_delivery_count);
                {
                    let mut guard = ctx.shard_databases.durable_queue_registry(owner);
                    let reg = guard
                        .get_or_insert_with(|| Box::new(crate::mq::DurableQueueRegistry::new()));
                    reg.insert(effective_key.clone(), config);
                }

                // Owner's WAL so replay restores the registry on the shard
                // that also holds the stream.
                let payload = crate::mq::wal::encode_mq_create(&effective_key, max_delivery_count);
                let mut wal_buf = Vec::new();
                crate::persistence::wal_v3::record::write_wal_v3_record(
                    &mut wal_buf,
                    0,
                    crate::persistence::wal_v3::record::WalRecordType::MqCreate,
                    &payload,
                );
                ctx.shard_databases.wal_append(owner, Bytes::from(wal_buf));
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"PUSH") {
        match validate_mq_push(cmd_args) {
            Ok((queue_key, fields)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                // Slice path: owner-route via MqCommand hop (shardslice-migration Wave B2).
                if crate::shard::slice::is_initialized() {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
                // Lock path (byte-identical to pre-migration).
                // Push outcome: Ok(Some(msg_id)) = pushed; Ok(None) = not durable; Err(e) = stream error.
                type PushOutcome = Result<Option<crate::storage::stream::StreamId>, Frame>;
                let push_outcome: PushOutcome = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    let r = match db_guard.get_stream_mut(&effective_key) {
                        Ok(Some(stream)) => {
                            if !stream.durable {
                                Ok(None)
                            } else {
                                let msg_id = stream.next_auto_id();
                                let msg_id = stream.add(msg_id, fields);
                                Ok(Some(msg_id))
                            }
                        }
                        Ok(None) => Ok(None),
                        Err(e) => Err(e),
                    };
                    drop(db_guard);
                    r
                };
                match push_outcome {
                    Ok(Some(msg_id)) => {
                        // Owner's registry: its event-loop tick fires triggers.
                        {
                            let mut trig_guard = ctx.shard_databases.trigger_registry(owner);
                            if let Some(reg) = trig_guard.as_mut() {
                                let trig_key = if let Some(ws_id) = conn.workspace_id.as_ref() {
                                    let ws_hex = ws_id.as_hex();
                                    let mut k =
                                        Vec::with_capacity(ws_hex.len() + 1 + queue_key.len());
                                    k.extend_from_slice(ws_hex.as_bytes());
                                    k.push(b':');
                                    k.extend_from_slice(&queue_key);
                                    Bytes::from(k)
                                } else {
                                    queue_key.clone()
                                };
                                if let Some(trig_entry) = reg.get_mut(&trig_key) {
                                    if trig_entry.pending_fire_ms == 0 {
                                        let fire_at =
                                            ctx.cached_clock.ms() + trig_entry.debounce_ms;
                                        trig_entry.pending_fire_ms = fire_at;
                                    }
                                }
                            }
                        }
                        responses.push(Frame::BulkString(Bytes::from(format!(
                            "{}-{}",
                            msg_id.ms, msg_id.seq
                        ))));
                    }
                    Ok(None) => {
                        responses.push(Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)));
                    }
                    Err(e) => responses.push(e),
                }
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"POP") {
        match validate_mq_pop(cmd_args) {
            Ok((queue_key, count)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                let group_name = Bytes::from_static(b"__mq_consumers");
                let consumer_name = Bytes::from_static(b"__mq_default");

                // The POP body needs the same Database guard across multiple steps
                // (mdc lookup, read_group_new, DLQ stream creation). Refactor into a
                // closure that takes &mut Database and produces a Frame to push.
                //
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                let pop_body = |db: &mut crate::storage::db::Database| -> Frame {
                    let mdc = match db.get_stream_mut(&effective_key) {
                        Ok(Some(stream)) => {
                            if !stream.durable {
                                return Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE));
                            }
                            stream.max_delivery_count
                        }
                        Ok(None) => {
                            return Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE));
                        }
                        Err(e) => return e,
                    };

                    let request_count = count + (mdc as usize);
                    let stream = match db.get_stream_mut(&effective_key) {
                        Ok(Some(s)) => s,
                        _ => {
                            return Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE));
                        }
                    };
                    let claimed = match stream.read_group_new(
                        &group_name,
                        &consumer_name,
                        Some(request_count),
                        false,
                    ) {
                        Ok(entries) => entries,
                        Err(_) => {
                            return Frame::Array(vec![].into());
                        }
                    };

                    let mut results = Vec::new();
                    let mut dlq_entries: Vec<(StreamId, Vec<(Bytes, Bytes)>)> = Vec::new();
                    let mut dlq_ack_ids: Vec<StreamId> = Vec::new();
                    for (id, fields) in &claimed {
                        let delivery_count = stream
                            .groups
                            .get(group_name.as_ref())
                            .and_then(|g| g.pel.get(id))
                            .map(|pe| pe.delivery_count)
                            .unwrap_or(1);
                        if mdc > 0 && delivery_count >= mdc as u64 {
                            dlq_entries.push((*id, fields.clone()));
                            dlq_ack_ids.push(*id);
                        } else if results.len() < count {
                            results.push((*id, fields.clone()));
                        }
                    }

                    if !dlq_ack_ids.is_empty() {
                        let _ = stream.xack(&group_name, &dlq_ack_ids);
                    }

                    if !dlq_entries.is_empty() {
                        let dlq_key = {
                            let mut buf = Vec::with_capacity(effective_key.len() + 8);
                            buf.extend_from_slice(&effective_key);
                            buf.extend_from_slice(b"::mq:dlq");
                            Bytes::from(buf)
                        };
                        if let Ok(dlq_stream) = db.get_or_create_stream(&dlq_key) {
                            for (_id, fields) in dlq_entries {
                                let dlq_id = dlq_stream.next_auto_id();
                                dlq_stream.add(dlq_id, fields);
                            }
                        }
                    }

                    let result_frames: Vec<Frame> = results
                        .iter()
                        .map(|(id, fields)| {
                            let mut entry_frames = Vec::with_capacity(2);
                            entry_frames.push(Frame::BulkString(Bytes::from(format!(
                                "{}-{}",
                                id.ms, id.seq
                            ))));
                            let field_frames: Vec<Frame> = fields
                                .iter()
                                .flat_map(|(f, v)| {
                                    vec![Frame::BulkString(f.clone()), Frame::BulkString(v.clone())]
                                })
                                .collect();
                            entry_frames.push(Frame::Array(field_frames.into()));
                            Frame::Array(entry_frames.into())
                        })
                        .collect();
                    Frame::Array(result_frames.into())
                };

                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                // Slice path: owner-route via MqCommand hop (shardslice-migration Wave B2).
                let response = if crate::shard::slice::is_initialized() {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await
                } else {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    let r = pop_body(&mut *db_guard);
                    drop(db_guard);
                    r
                };
                responses.push(response);
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
                let ids: Vec<StreamId> = msg_ids
                    .iter()
                    .map(|(ms, seq)| StreamId { ms: *ms, seq: *seq })
                    .collect();
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                // Slice path: owner-route via MqCommand hop (shardslice-migration Wave B2).
                if crate::shard::slice::is_initialized() {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
                // Lock path (byte-identical to pre-migration).
                // Closure returns Some(acked_count) on success, None on any error/miss.
                let ack_body = |db: &mut crate::storage::db::Database| -> Option<u64> {
                    match db.get_stream_mut(&effective_key) {
                        Ok(Some(stream)) => {
                            let group_name = Bytes::from_static(b"__mq_consumers");
                            match stream.xack(&group_name, &ids) {
                                Ok(acked_count) => Some(acked_count),
                                Err(_) => None,
                            }
                        }
                        Ok(None) => None,
                        Err(_) => None,
                    }
                };
                let acked = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    let r = ack_body(&mut *db_guard);
                    drop(db_guard);
                    r
                };
                match acked {
                    Some(acked_count) => {
                        for (ms, seq) in &msg_ids {
                            let payload = crate::mq::wal::encode_mq_ack(&effective_key, *ms, *seq);
                            let mut wal_buf = Vec::new();
                            crate::persistence::wal_v3::record::write_wal_v3_record(
                                &mut wal_buf,
                                0,
                                crate::persistence::wal_v3::record::WalRecordType::MqAck,
                                &payload,
                            );
                            ctx.shard_databases.wal_append(owner, Bytes::from(wal_buf));
                        }
                        responses.push(Frame::Integer(acked_count as i64));
                    }
                    None => responses.push(Frame::Integer(0)),
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
                let dlq_key = {
                    let mut buf = Vec::with_capacity(effective_key.len() + 8);
                    buf.extend_from_slice(&effective_key);
                    buf.extend_from_slice(b"::mq:dlq");
                    Bytes::from(buf)
                };
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                // Slice path: owner-route via MqCommand hop (shardslice-migration Wave B2).
                if crate::shard::slice::is_initialized() {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
                // Lock path (byte-identical to pre-migration).
                let dlq_body = |db: &mut crate::storage::db::Database| -> i64 {
                    match db.get_stream_mut(&dlq_key) {
                        Ok(Some(stream)) => stream.length as i64,
                        _ => 0i64,
                    }
                };
                let len = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    let r = dlq_body(&mut *db_guard);
                    drop(db_guard);
                    r
                };
                responses.push(Frame::Integer(len));
            }
            Err(e) => responses.push(e),
        }
        return true;
    }

    if sub.eq_ignore_ascii_case(b"TRIGGER") {
        match validate_mq_trigger(cmd_args) {
            Ok((queue_key, callback_cmd, debounce_ms)) => {
                let effective_key =
                    crate::workspace::workspace_key(conn.workspace_id.as_ref(), &queue_key);
                // Owner's registry: its event-loop tick fires triggers
                // (timers.rs documents the home shard as authoritative).
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                // Slice path: owner-route via MqCommand hop (shardslice-migration Wave B2).
                if crate::shard::slice::is_initialized() {
                    let key_prefix = mq_key_prefix(conn.workspace_id.as_ref());
                    let response =
                        mq_dispatch_to_owner(frame, key_prefix, owner, conn.selected_db, ctx).await;
                    responses.push(response);
                    return true;
                }
                // Lock path (byte-identical to pre-migration).
                let trig_key = if let Some(ws_id) = conn.workspace_id.as_ref() {
                    let ws_hex = ws_id.as_hex();
                    let mut k = Vec::with_capacity(ws_hex.len() + 1 + queue_key.len());
                    k.extend_from_slice(ws_hex.as_bytes());
                    k.push(b':');
                    k.extend_from_slice(&queue_key);
                    Bytes::from(k)
                } else {
                    queue_key.clone()
                };
                let entry = crate::mq::TriggerEntry {
                    queue_key: effective_key,
                    callback_cmd,
                    debounce_ms,
                    last_fire_ms: 0,
                    pending_fire_ms: 0,
                };
                {
                    let mut guard = ctx.shard_databases.trigger_registry(owner);
                    let reg =
                        guard.get_or_insert_with(|| Box::new(crate::mq::TriggerRegistry::new()));
                    reg.register(trig_key, entry);
                }
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
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
pub(super) fn try_handle_multi_exec(
    cmd: &[u8],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
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
            let result = execute_transaction_sharded(
                &ctx.shard_databases,
                ctx.shard_id,
                &conn.command_queue,
                conn.selected_db,
                &ctx.cached_clock,
            );
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
    // GRAPH.LIST has no name argument and stays connection-local (it reports
    // this shard's graphs only — recorded as a v3 observe delta).
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
                crate::shard::coordinator::spsc_send(
                    &ctx.dispatch_tx,
                    ctx.shard_id,
                    owner,
                    msg,
                    &ctx.spsc_notifiers,
                )
                .await;
                let mut response = match reply_rx.recv().await {
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
    // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
    let (response, wal_records, cypher_intents, cypher_undo_ops) =
        if crate::shard::slice::is_initialized() {
            crate::shard::slice::with_shard(|s| {
                if is_write {
                    let (resp, cypher_intents, undo_ops) = if cmd
                        .eq_ignore_ascii_case(b"GRAPH.QUERY")
                    {
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
                    let resp =
                        crate::command::graph::dispatch_graph_read(&s.graph_store, cmd, cmd_args);
                    (resp, Vec::new(), Vec::new(), Vec::new())
                }
            })
        } else if is_write {
            let mut gs = ctx.shard_databases.graph_store_write(ctx.shard_id);
            let (resp, cypher_intents, undo_ops) = if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
                crate::command::graph::graph_query_or_write(&mut gs, cmd_args)
            } else {
                (
                    crate::command::graph::dispatch_graph_write(&mut gs, cmd, cmd_args),
                    Vec::new(),
                    Vec::new(),
                )
            };
            let records = gs.drain_wal();
            (resp, records, cypher_intents, undo_ops)
        } else {
            let gs = ctx.shard_databases.graph_store_read(ctx.shard_id);
            let resp = crate::command::graph::dispatch_graph_read(&gs, cmd, cmd_args);
            (resp, Vec::new(), Vec::new(), Vec::new())
        };
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
        ctx.shard_databases
            .wal_append(ctx.shard_id, bytes::Bytes::from(record));
    }
    let mut response = response;
    if let Some(ws_id) = conn.workspace_id.as_ref() {
        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
    }
    responses.push(response);
    true
}
