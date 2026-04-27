//! Connection-level command dispatchers: CLIENT subcommands, CONFIG, SLOWLOG,
//! REPLICAOF/REPLCONF, INFO, READONLY, BGSAVE/SAVE/LASTSAVE/BGREWRITEAOF,
//! cross-shard KEYS/SCAN/DBSIZE.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use bytes::Bytes;
use std::sync::Arc;

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::protocol::Frame;
use crate::runtime::channel;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::util::extract_bytes;
use crate::tracking::TrackingState;
use crate::workspace::strip_workspace_prefix_from_response;

use super::handle_config;

/// Handle CLIENT subcommands. Returns `true` if consumed.
pub(super) fn try_handle_client_command(
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CLIENT") {
        return false;
    }
    if let Some(sub) = cmd_args.first() {
        if let Some(sub_bytes) = extract_bytes(sub) {
            if sub_bytes.eq_ignore_ascii_case(b"ID") {
                responses.push(conn_cmd::client_id(client_id));
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"SETNAME") {
                if cmd_args.len() != 2 {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR wrong number of arguments for 'CLIENT SETNAME' command",
                    )));
                } else {
                    conn.client_name = extract_bytes(&cmd_args[1]);
                    let name_str = conn
                        .client_name
                        .as_ref()
                        .map(|b| String::from_utf8_lossy(b).to_string());
                    crate::client_registry::update(client_id, |e| {
                        e.name = name_str;
                    });
                    responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                }
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"GETNAME") {
                responses.push(match &conn.client_name {
                    Some(name) => Frame::BulkString(name.clone()),
                    None => Frame::Null,
                });
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"TRACKING") {
                match crate::command::client::parse_tracking_args(cmd_args) {
                    Ok(config_parsed) => {
                        if config_parsed.enable {
                            conn.tracking_state.enabled = true;
                            conn.tracking_state.bcast = config_parsed.bcast;
                            conn.tracking_state.noloop = config_parsed.noloop;
                            conn.tracking_state.optin = config_parsed.optin;
                            conn.tracking_state.optout = config_parsed.optout;
                            if conn.tracking_rx.is_none() {
                                let (tx, rx) = channel::mpsc_bounded::<Frame>(256);
                                conn.tracking_state.invalidation_tx = Some(tx.clone());
                                conn.tracking_rx = Some(rx);
                                let mut table = ctx.tracking_table.borrow_mut();
                                table.register_client(client_id, tx);
                                if let Some(target) = config_parsed.redirect {
                                    table.set_redirect(client_id, target);
                                }
                                for prefix in &config_parsed.prefixes {
                                    table.register_prefix(
                                        client_id,
                                        prefix.clone(),
                                        config_parsed.noloop,
                                    );
                                }
                            }
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        } else {
                            conn.tracking_state = TrackingState::default();
                            ctx.tracking_table.borrow_mut().untrack_all(client_id);
                            conn.tracking_rx = None;
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        }
                        return true;
                    }
                    Err(err_frame) => {
                        responses.push(err_frame);
                        return true;
                    }
                }
            }
            if sub_bytes.eq_ignore_ascii_case(b"LIST") {
                // Update our own entry before listing
                crate::client_registry::update(client_id, |e| {
                    e.db = conn.selected_db;
                    e.last_cmd_at = std::time::Instant::now();
                    e.flags = crate::client_registry::ClientFlags {
                        subscriber: conn.subscription_count > 0,
                        in_multi: conn.in_multi,
                        blocked: false,
                    };
                });
                let list = crate::client_registry::client_list();
                responses.push(Frame::BulkString(Bytes::from(list)));
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"INFO") {
                crate::client_registry::update(client_id, |e| {
                    e.db = conn.selected_db;
                    e.last_cmd_at = std::time::Instant::now();
                });
                let info = crate::client_registry::client_info(client_id).unwrap_or_default();
                responses.push(Frame::BulkString(Bytes::from(info)));
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"KILL") {
                let raw_args: Vec<&[u8]> = cmd_args[1..]
                    .iter()
                    .filter_map(|f| match f {
                        Frame::BulkString(b) => Some(b.as_ref()),
                        Frame::SimpleString(b) => Some(b.as_ref()),
                        _ => None,
                    })
                    .collect();
                match crate::client_registry::parse_kill_args(&raw_args) {
                    Some(filter) => {
                        let count = crate::client_registry::kill_clients(&filter);
                        responses.push(Frame::Integer(count as i64));
                    }
                    None => {
                        responses.push(Frame::Error(Bytes::from_static(
                            b"ERR syntax error. Usage: CLIENT KILL [ID id] [ADDR addr] [USER user]",
                        )));
                    }
                }
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"PAUSE") {
                if cmd_args.len() < 2 {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR wrong number of arguments for 'CLIENT PAUSE' command",
                    )));
                } else {
                    let timeout_bytes = match &cmd_args[1] {
                        Frame::BulkString(b) => Some(b.as_ref()),
                        Frame::SimpleString(b) => Some(b.as_ref()),
                        _ => None,
                    };
                    match timeout_bytes
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        Some(ms) => {
                            let mode = if cmd_args.len() > 2 {
                                match &cmd_args[2] {
                                    Frame::BulkString(b) | Frame::SimpleString(b)
                                        if b.eq_ignore_ascii_case(b"WRITE") =>
                                    {
                                        crate::client_pause::PauseMode::Write
                                    }
                                    _ => crate::client_pause::PauseMode::All,
                                }
                            } else {
                                crate::client_pause::PauseMode::All
                            };
                            crate::client_pause::pause(ms, mode);
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        }
                        None => {
                            responses.push(Frame::Error(Bytes::from_static(
                                b"ERR timeout is not a valid integer or out of range",
                            )));
                        }
                    }
                }
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"UNPAUSE") {
                crate::client_pause::unpause();
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"NO-EVICT")
                || sub_bytes.eq_ignore_ascii_case(b"NO-TOUCH")
            {
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                return true;
            }
            responses.push(Frame::Error(Bytes::from(format!(
                "ERR unknown subcommand '{}'",
                String::from_utf8_lossy(&sub_bytes)
            ))));
            return true;
        }
    }
    responses.push(Frame::Error(Bytes::from_static(
        b"ERR wrong number of arguments for 'client' command",
    )));
    true
}

/// Handle CONFIG command. Returns `true` if consumed.
pub(super) fn try_handle_config(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CONFIG") {
        return false;
    }
    responses.push(handle_config(cmd_args, &ctx.runtime_config, &ctx.config));
    true
}

/// Handle REPLICAOF / SLAVEOF. Returns `true` if consumed.
pub(super) fn try_handle_replicaof(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"REPLICAOF") && !cmd.eq_ignore_ascii_case(b"SLAVEOF") {
        return false;
    }
    use crate::command::connection::{ReplicaofAction, replicaof};
    let (resp, action) = replicaof(cmd_args);
    if let Some(action) = action {
        if let Some(ref rs) = ctx.repl_state {
            match action {
                ReplicaofAction::StartReplication { host, port } => {
                    if let Ok(mut rs_guard) = rs.write() {
                        rs_guard.set_role(crate::replication::state::ReplicationRole::Replica {
                            host: host.clone(),
                            port,
                            state:
                                crate::replication::handshake::ReplicaHandshakeState::PingPending,
                        });
                    }
                    let rs_clone = Arc::clone(rs);
                    let cfg = crate::replication::replica::ReplicaTaskConfig {
                        master_host: host,
                        master_port: port,
                        repl_state: rs_clone,
                        num_shards: ctx.num_shards,
                        persistence_dir: None,
                        listening_port: 0,
                    };
                    tokio::task::spawn_local(crate::replication::replica::run_replica_task(cfg));
                }
                ReplicaofAction::PromoteToMaster => {
                    use crate::replication::state::generate_repl_id;
                    if let Ok(mut rs_guard) = rs.write() {
                        rs_guard.repl_id2 = rs_guard.repl_id.clone();
                        rs_guard.repl_id = generate_repl_id();
                        rs_guard.set_role(crate::replication::state::ReplicationRole::Master);
                    }
                }
                ReplicaofAction::NoOp => {}
            }
        }
    }
    responses.push(resp);
    true
}

/// Handle INFO command. Returns `true` if consumed.
pub(super) fn try_handle_info(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"INFO") {
        return false;
    }
    let guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
    let response_text = {
        let resp_frame = conn_cmd::info_readonly(&guard, cmd_args);
        match resp_frame {
            Frame::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
            _ => String::new(),
        }
    };
    drop(guard);
    let mut response_text = response_text;
    if let Some(ref rs) = ctx.repl_state {
        if let Ok(rs_guard) = rs.try_read() {
            response_text.push_str(&crate::replication::handshake::build_info_replication(
                &rs_guard,
            ));
        }
    }
    responses.push(Frame::BulkString(Bytes::from(response_text)));
    true
}

/// Handle persistence commands (BGSAVE, SAVE, LASTSAVE, BGREWRITEAOF).
/// Returns `true` if consumed.
pub(super) fn try_handle_persistence(
    cmd: &[u8],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if cmd.eq_ignore_ascii_case(b"BGSAVE") {
        responses.push(crate::command::persistence::bgsave_start_sharded(
            &ctx.snapshot_trigger_tx,
            ctx.num_shards,
        ));
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"SAVE") {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR SAVE not supported in sharded mode, use BGSAVE",
        )));
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"LASTSAVE") {
        responses.push(crate::command::persistence::handle_lastsave());
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"BGREWRITEAOF") {
        if let Some(ref tx) = ctx.aof_tx {
            responses.push(crate::command::persistence::bgrewriteaof_start_sharded(
                tx,
                ctx.shard_databases.clone(),
            ));
        } else {
            responses.push(Frame::Error(Bytes::from_static(b"ERR AOF is not enabled")));
        }
        return true;
    }
    false
}

/// Handle cross-shard KEYS, SCAN, DBSIZE aggregation.
/// Returns `true` if consumed.
pub(super) async fn try_handle_cross_shard_scan(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if cmd.eq_ignore_ascii_case(b"KEYS") {
        let mut response = crate::shard::coordinator::coordinate_keys(
            cmd_args,
            ctx.shard_id,
            ctx.num_shards,
            conn.selected_db,
            &ctx.shard_databases,
            &ctx.dispatch_tx,
            &ctx.spsc_notifiers,
            &ctx.cached_clock,
            &(),
        )
        .await;
        if let Some(ws_id) = conn.workspace_id.as_ref() {
            strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
        }
        responses.push(response);
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"SCAN") {
        let mut response = crate::shard::coordinator::coordinate_scan(
            cmd_args,
            ctx.shard_id,
            ctx.num_shards,
            conn.selected_db,
            &ctx.shard_databases,
            &ctx.dispatch_tx,
            &ctx.spsc_notifiers,
            &ctx.cached_clock,
            &(),
        )
        .await;
        if let Some(ws_id) = conn.workspace_id.as_ref() {
            strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
        }
        responses.push(response);
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"DBSIZE") {
        let response = crate::shard::coordinator::coordinate_dbsize(
            ctx.shard_id,
            ctx.num_shards,
            conn.selected_db,
            &ctx.shard_databases,
            &ctx.dispatch_tx,
            &ctx.spsc_notifiers,
            &(),
        )
        .await;
        responses.push(response);
        return true;
    }
    false
}

/// Handle READONLY enforcement for replicas.
/// Returns `true` if the command was blocked (caller should `continue`).
///
/// S3.5a (2026-04-27): see `handler_monoio::dispatch::try_enforce_readonly`
/// for the rationale — lock-free `AtomicBool` mirror avoids per-command
/// `RwLock::try_read` CAS.
#[inline]
pub(super) fn try_enforce_readonly(
    cmd: &[u8],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    let Some(ref mirror) = ctx.is_replica_mirror else {
        return false;
    };
    if !mirror.load(std::sync::atomic::Ordering::Acquire) {
        return false;
    }
    if metadata::is_write(cmd) {
        responses.push(Frame::Error(Bytes::from_static(
            b"READONLY You can't write against a read only replica.",
        )));
        return true;
    }
    false
}

/// Handle SLOWLOG command. Returns `true` if consumed.
pub(super) fn try_handle_slowlog(
    cmd: &[u8],
    cmd_args: &[Frame],
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"SLOWLOG") {
        return false;
    }
    let sl = crate::admin::metrics_setup::global_slowlog();
    responses.push(crate::admin::slowlog::handle_slowlog(sl, cmd_args));
    true
}

/// Handle REPLCONF command. Returns `true` if consumed.
pub(super) fn try_handle_replconf(
    cmd: &[u8],
    cmd_args: &[Frame],
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"REPLCONF") {
        return false;
    }
    responses.push(crate::command::connection::replconf(cmd_args));
    true
}
