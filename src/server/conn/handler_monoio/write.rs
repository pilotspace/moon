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
            Ok(ws_id_raw) => {
                match parse_workspace_id_from_bytes(&ws_id_raw) {
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
                            // Best-effort cleanup: delete all KV keys with ws prefix (WS-03).
                            // Phase 2a: gate on is_initialized(); new path uses ShardSlice.
                            {
                                let prefix = format!("{{{}}}:", ws_id.as_hex());
                                if crate::shard::slice::is_initialized() {
                                    // shardslice-migration Wave B1: owner-route the
                                    // cleanup via WsDropCleanup hop. The {wsid} hash
                                    // tag co-locates every workspace key on ONE shard.
                                    let prefix_bytes = Bytes::from(prefix.into_bytes());
                                    let owner = crate::shard::dispatch::key_to_shard(
                                        &prefix_bytes,
                                        ctx.num_shards,
                                    );
                                    if owner == ctx.shard_id {
                                        // Self: execute locally (we ARE the owner).
                                        crate::shard::slice::with_shard_db(0, |db| {
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
                                        });
                                    } else {
                                        // Foreign: send WsDropCleanup hop to owner.
                                        let (reply_tx, reply_rx) =
                                            crate::runtime::channel::oneshot();
                                        let msg =
                                            crate::shard::dispatch::ShardMessage::WsDropCleanup {
                                                prefix: prefix_bytes,
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
                                        let _ = reply_rx.recv().await;
                                    }
                                } else {
                                    // The {wsid} hash tag co-locates every
                                    // workspace key on ONE shard — clean up
                                    // there, not on the connection's shard.
                                    let owner = crate::shard::dispatch::key_to_shard(
                                        prefix.as_bytes(),
                                        ctx.num_shards,
                                    );
                                    let mut db_guard = ctx.shard_databases.write_db(owner, 0);
                                    let keys_to_delete: Vec<Vec<u8>> = db_guard
                                        .keys()
                                        .filter(|k| k.as_bytes().starts_with(prefix.as_bytes()))
                                        .map(|k| k.as_bytes().to_vec())
                                        .collect();
                                    for key in &keys_to_delete {
                                        db_guard.remove(key);
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
                // Phase 2a / shardslice-migration Wave B1: gate on is_initialized();
                // slice path owner-routes via MqCommand hop (execute_mq_on_owner
                // handles stream create, registry insert, and WAL in one step).
                if crate::shard::slice::is_initialized() {
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
                    return true;
                }

                let create_result: Result<(), Frame> = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    match db_guard.get_or_create_stream(&effective_key) {
                        Ok(stream) => {
                            stream.durable = true;
                            stream.max_delivery_count = max_delivery_count;
                            let group_name = Bytes::from_static(b"__mq_consumers");
                            let _ = stream.create_group(group_name, StreamId::ZERO);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                };
                if let Err(e) = create_result {
                    responses.push(e);
                    return true;
                }

                // Store config in the owning shard's registry
                let config =
                    crate::mq::DurableStreamConfig::new(effective_key.clone(), max_delivery_count);
                {
                    let mut guard = ctx.shard_databases.durable_queue_registry(owner);
                    let reg = guard
                        .get_or_insert_with(|| Box::new(crate::mq::DurableQueueRegistry::new()));
                    reg.insert(effective_key.clone(), config);
                }

                // WAL: MqCreate record — owner's WAL so replay restores the
                // registry on the shard that also holds the stream.
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
                // Phase 2a / shardslice-migration Wave B1: gate on is_initialized();
                // slice path owner-routes via MqCommand hop (execute_mq_on_owner
                // handles stream push, trigger debounce in one step).
                if crate::shard::slice::is_initialized() {
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
                    return true;
                }

                let push_result: Result<crate::storage::stream::StreamId, Frame> = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    match db_guard.get_stream_mut(&effective_key) {
                        Ok(Some(stream)) => {
                            if !stream.durable {
                                Err(Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)))
                            } else {
                                let id = stream.next_auto_id();
                                Ok(stream.add(id, fields))
                            }
                        }
                        Ok(None) => Err(Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE))),
                        Err(e) => Err(e),
                    }
                };
                match push_result {
                    Ok(msg_id) => {
                        // trigger_registry: not in Phase 2a scope — old path only.
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

                // Phase 2a / shardslice-migration Wave B1: gate on is_initialized();
                // slice path owner-routes via MqCommand hop (execute_mq_on_owner
                // handles claim, DLQ routing in one step).
                if crate::shard::slice::is_initialized() {
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
                    return true;
                }

                let pop_frame: Result<Frame, Frame> = (|| {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    let mdc = match db_guard.get_stream_mut(&effective_key) {
                        Ok(Some(stream)) => {
                            if !stream.durable {
                                return Err(Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)));
                            }
                            stream.max_delivery_count
                        }
                        Ok(None) => {
                            return Err(Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)));
                        }
                        Err(e) => return Err(e),
                    };
                    let request_count = count + (mdc as usize);
                    let stream = match db_guard.get_stream_mut(&effective_key) {
                        Ok(Some(s)) => s,
                        _ => {
                            return Err(Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)));
                        }
                    };
                    let claimed = match stream.read_group_new(
                        &group_name,
                        &consumer_name,
                        Some(request_count),
                        false,
                    ) {
                        Ok(entries) => entries,
                        Err(_) => return Ok(Frame::Array(vec![].into())),
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
                        if let Ok(dlq_stream) = db_guard.get_or_create_stream(&dlq_key) {
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
                    Ok(Frame::Array(result_frames.into()))
                })();
                match pop_frame {
                    Ok(frame) => responses.push(frame),
                    Err(e) => {
                        responses.push(e);
                        return true;
                    }
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
                let ids: Vec<StreamId> = msg_ids
                    .iter()
                    .map(|(ms, seq)| StreamId { ms: *ms, seq: *seq })
                    .collect();
                // Owner-shard targeting — see MQ CREATE above.
                let owner = crate::shard::dispatch::key_to_shard(&effective_key, ctx.num_shards);
                // Phase 2a / shardslice-migration Wave B1: gate on is_initialized();
                // slice path owner-routes via MqCommand hop (execute_mq_on_owner
                // handles xack and WAL records in one step).
                if crate::shard::slice::is_initialized() {
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
                    return true;
                }

                let acked_count: i64 = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    match db_guard.get_stream_mut(&effective_key) {
                        Ok(Some(stream)) => {
                            let group_name = Bytes::from_static(b"__mq_consumers");
                            stream
                                .xack(&group_name, &ids)
                                .map(|c| c as i64)
                                .unwrap_or(0)
                        }
                        _ => 0i64,
                    }
                };
                if acked_count > 0 {
                    // Emit MqAck WAL record for each acked ID (WAL stays outside closure)
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
                }
                responses.push(Frame::Integer(acked_count));
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
                // Phase 2a / shardslice-migration Wave B1: gate on is_initialized();
                // slice path owner-routes via MqCommand hop.
                if crate::shard::slice::is_initialized() {
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
                    return true;
                }

                let len: i64 = {
                    let mut db_guard = ctx.shard_databases.write_db(owner, conn.selected_db);
                    match db_guard.get_stream_mut(&dlq_key) {
                        Ok(Some(stream)) => stream.length as i64,
                        _ => 0i64,
                    }
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
                // shardslice-migration Wave B1: slice path owner-routes via MqCommand hop
                // (execute_mq_on_owner registers the trigger in the owner's slice registry).
                if crate::shard::slice::is_initialized() {
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
                    return true;
                }

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
            Err(_) => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR cross-shard MQ reply channel closed",
            )),
        }
    }
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
    // Phase 2a: gate on is_initialized(); new path uses ShardSlice::graph_store directly.
    let (response, wal_records, cypher_intents, cypher_undo_ops) =
        if crate::shard::slice::is_initialized() {
            crate::shard::slice::with_shard(|s| {
                if crate::command::graph::is_graph_write_cmd(cmd)
                    || (cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
                        && crate::command::graph::is_cypher_write_query(cmd_args))
                {
                    let gs = &mut s.graph_store;
                    let (resp, cypher_intents, undo_ops) =
                        if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
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
                    let resp = crate::command::graph::dispatch_graph_read(gs, cmd, cmd_args);
                    (resp, Vec::new(), Vec::new(), Vec::new())
                }
            })
        } else if crate::command::graph::is_graph_write_cmd(cmd)
            || (cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
                && crate::command::graph::is_cypher_write_query(cmd_args))
        {
            let mut gs = ctx.shard_databases.graph_store_write(ctx.shard_id);
            let (resp, cypher_intents, undo_ops) = if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
                // Phase 167 (CYP-01/02): capture Cypher-created
                // nodes/edges so TXN.ABORT can roll them back via
                // CrossStoreTxn::record_graph.
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
