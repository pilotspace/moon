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
                        b"ERR wrong number of arguments for 'client|setname' command",
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
                                let mut table = ctx.tracking_table.lock();
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
                            ctx.tracking_table.lock().untrack_all(client_id);
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
                    e.live.touch(
                        conn.selected_db,
                        crate::client_registry::ClientFlags {
                            subscriber: conn.subscription_count > 0,
                            in_multi: conn.in_multi,
                            // Executing CLIENT LIST/INFO means not blocked.
                            blocked: false,
                            replica: conn.saw_replconf,
                        },
                        crate::storage::entry::current_time_ms(),
                    );
                });
                let list = crate::client_registry::client_list();
                responses.push(Frame::BulkString(Bytes::from(list)));
                return true;
            }
            if sub_bytes.eq_ignore_ascii_case(b"INFO") {
                // Derive flags from CURRENT conn state (same as the LIST path
                // above) — reloading e.live.flags would freeze stale bits.
                crate::client_registry::update(client_id, |e| {
                    e.live.touch(
                        conn.selected_db,
                        crate::client_registry::ClientFlags {
                            subscriber: conn.subscription_count > 0,
                            in_multi: conn.in_multi,
                            // Executing CLIENT LIST/INFO means not blocked.
                            blocked: false,
                            replica: conn.saw_replconf,
                        },
                        crate::storage::entry::current_time_ms(),
                    );
                });
                let info = crate::client_registry::client_info(client_id).unwrap_or_default();
                // RESP3 sends CLIENT INFO as a Verbatim string. This intercept
                // never reaches the generic dispatch exit, so it converts here.
                responses.push(crate::protocol::resp3::apply_shape(
                    crate::protocol::resp3::Resp3Shape::Verbatim,
                    Frame::BulkString(Bytes::from(info)),
                    conn.protocol_version,
                ));
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
                        let count = crate::client_registry::kill_clients(&filter, Some(client_id));
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
                        b"ERR wrong number of arguments for 'client|pause' command",
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
    proto: u8,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CONFIG") {
        return false;
    }
    // This intercept short-circuits the generic dispatch exit, so it must
    // apply the RESP3 conversion itself — otherwise `CONFIG GET` reaches the
    // wire as a flat Array where Redis sends a Map. That omission is exactly
    // why CONFIG was the one command the old converter could never fix.
    let reply = handle_config(cmd_args, &ctx.runtime_config, &ctx.config);
    responses.push(crate::server::conn::util::apply_resp3_conversion(
        cmd, cmd_args, reply, proto,
    ));
    true
}

/// Handle REPLICAOF / SLAVEOF. Returns `true` if consumed.
/// CDC.READ — polling-based change data capture (C3 v1).
///
/// Dispatches `CDC.READ <wal_dir> <from_lsn> [LIMIT N]` to
/// `crate::command::cdc::cdc_read`. Stateless / synchronous — no shard
/// state involved, just reads WAL files from disk and decodes them into
/// Debezium JSON envelopes. The push-based CDC.SUBSCRIBE variant (C3b)
/// will live alongside this.
pub(super) fn try_handle_cdc_read(
    cmd: &[u8],
    cmd_args: &[Frame],
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CDC.READ") {
        return false;
    }
    responses.push(crate::command::cdc::cdc_read(cmd_args));
    true
}

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
                    rs.write()
                        .set_role(crate::replication::state::ReplicationRole::Replica {
                            host: host.clone(),
                            port,
                            state:
                                crate::replication::handshake::ReplicaHandshakeState::PingPending,
                        });
                    let rs_clone = Arc::clone(rs);
                    // Bump the task generation FIRST: any previously spawned
                    // replica task (old REPLICAOF target) sees itself
                    // superseded and exits instead of double-applying the
                    // stream alongside the new task.
                    let epoch = crate::replication::replica::bump_replica_task_epoch();
                    let cfg = crate::replication::replica::ReplicaTaskConfig {
                        master_host: host,
                        master_port: port,
                        repl_state: rs_clone,
                        num_shards: ctx.num_shards,
                        persistence_dir: None,
                        listening_port: 0,
                        epoch,
                        stream_db: std::sync::atomic::AtomicUsize::new(0),
                        shard_databases: ctx.shard_databases.clone(),
                    };
                    tokio::task::spawn_local(crate::replication::replica::run_replica_task(cfg));
                }
                ReplicaofAction::PromoteToMaster => {
                    use crate::replication::state::generate_repl_id;
                    // Kill the running replica task — flipping the role alone
                    // left it streaming + applying forever (each NO ONE →
                    // re-attach cycle stacked one more live applier).
                    let _ = crate::replication::replica::bump_replica_task_epoch();
                    let mut rs_guard = rs.write();
                    rs_guard.repl_id2 = rs_guard.repl_id.clone();
                    rs_guard.repl_id = generate_repl_id();
                    rs_guard.set_role(crate::replication::state::ReplicationRole::Master);
                }
                ReplicaofAction::NoOp => {}
            }
        }
    }
    responses.push(resp);
    true
}

/// Handle INFO command. Returns `true` if consumed.
pub(super) async fn try_handle_info(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"INFO") {
        return false;
    }
    // # Keyspace parity: per-db (keys, expires) summed across ALL shards —
    // previously the section reported the selected db's LOCAL count as db0
    // (other dbs invisible, other shards uncounted).
    let keyspace = crate::shard::coordinator::coordinate_keyspace_info(
        ctx.shard_id,
        ctx.num_shards,
        &ctx.dispatch_tx,
        &ctx.spsc_notifiers,
    )
    .await;
    // ShardSlice path: access the local shard's database via thread-local.
    // Passed in rather than appended — see the monoio handler for why the
    // append produced a duplicate `# Replication`.
    let real_repl = ctx
        .repl_state
        .as_ref()
        .and_then(|rs| rs.try_read())
        .map(|rs_guard| crate::replication::handshake::build_info_replication(&rs_guard));
    // Instance-wide pub/sub counts, unioned across every shard's registry —
    // the same gather `PUBSUB CHANNELS`/`NUMPAT` do, so INFO cannot disagree
    // with them.
    let (pubsub_channels, pubsub_patterns) =
        crate::pubsub::instance_pubsub_counts(&ctx.all_pubsub_registries);
    let pubsub_facts = conn_cmd::InstanceFacts {
        pubsub_channels,
        pubsub_patterns,
        tcp_port: ctx.config_port,
    };
    let resp_frame = crate::shard::slice::with_shard_db(conn.selected_db, |db| {
        conn_cmd::info_with_facts(db, cmd_args, &keyspace, real_repl.as_deref(), &pubsub_facts)
    });
    responses.push(resp_frame);
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
        if let Some(ref pool) = ctx.aof_pool {
            responses.push(crate::command::persistence::bgrewriteaof_start_sharded(
                pool,
                ctx.shard_databases.clone(),
            ));
        } else {
            responses.push(Frame::Error(Bytes::from_static(b"ERR AOF is not enabled")));
        }
        return true;
    }
    false
}

/// Outcome of `try_handle_shutdown`.
pub(super) enum ShutdownOutcome {
    /// Not a SHUTDOWN command -- caller should keep dispatching.
    NotShutdown,
    /// SHUTDOWN was rejected (syntax error, forced SAVE failed) -- an error
    /// frame was pushed onto `responses`; caller should `continue`.
    Rejected,
    /// SHUTDOWN succeeded and the graceful shutdown sequence has been
    /// triggered -- Redis parity: no reply is sent, caller must close the
    /// connection (`should_quit = true; break;`) without pushing anything
    /// onto `responses`.
    Exiting,
}

/// Handle SHUTDOWN [NOSAVE|SAVE] in sharded mode.
///
/// Sharded mode has no single-threaded synchronous SAVE path (see plain
/// SAVE's "not supported" rejection above), so a forced save here uses the
/// same cooperative per-shard BGSAVE snapshot (`bgsave_start_sharded`) and
/// polls for its completion with a bounded timeout -- a wedged shard must
/// not hang SHUTDOWN forever; timing out fails the command (server stays
/// up) rather than exiting with a torn snapshot.
pub(super) async fn try_handle_shutdown(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    responses: &mut Vec<Frame>,
) -> ShutdownOutcome {
    if !cmd.eq_ignore_ascii_case(b"SHUTDOWN") {
        return ShutdownOutcome::NotShutdown;
    }
    use crate::command::persistence::{self, ShutdownSaveMode};

    let mode = match persistence::parse_shutdown_args(cmd_args) {
        Ok(m) => m,
        Err(e) => {
            responses.push(e);
            return ShutdownOutcome::Rejected;
        }
    };
    let should_save = match mode {
        ShutdownSaveMode::Save => true,
        ShutdownSaveMode::NoSave => false,
        ShutdownSaveMode::Default => {
            persistence::shutdown_default_should_save(ctx.config.save.as_deref())
        }
    };
    if should_save {
        match persistence::bgsave_start_sharded(&ctx.snapshot_trigger_tx, ctx.num_shards) {
            Frame::Error(e) => {
                responses.push(Frame::Error(e));
                return ShutdownOutcome::Rejected;
            }
            _ => {}
        }
        let start = std::time::Instant::now();
        loop {
            if !persistence::SAVE_IN_PROGRESS.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            if start.elapsed().as_millis() as u64 > persistence::SHUTDOWN_SAVE_TIMEOUT_MS {
                responses.push(Frame::Error(Bytes::from_static(
                    b"ERR SHUTDOWN failed: background save timed out, check logs",
                )));
                return ShutdownOutcome::Rejected;
            }
            tokio::time::sleep(std::time::Duration::from_millis(
                persistence::SHUTDOWN_SAVE_POLL_MS,
            ))
            .await;
        }
        if !persistence::BGSAVE_LAST_STATUS.load(std::sync::atomic::Ordering::Relaxed) {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR SHUTDOWN failed: background save error, check logs",
            )));
            return ShutdownOutcome::Rejected;
        }
    }
    tracing::info!("SHUTDOWN command received -- initiating graceful shutdown");
    shutdown.cancel();
    ShutdownOutcome::Exiting
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
    if cmd.eq_ignore_ascii_case(b"HOTKEYS") {
        let response = match crate::command::server_admin::parse_hotkeys_count(cmd_args) {
            Ok(count) => {
                crate::shard::coordinator::coordinate_hotkeys(
                    count,
                    ctx.shard_id,
                    ctx.num_shards,
                    conn.selected_db,
                    &ctx.shard_databases,
                    &ctx.dispatch_tx,
                    &ctx.spsc_notifiers,
                    &(),
                )
                .await
            }
            Err(e) => e,
        };
        responses.push(response);
        return true;
    }
    false
}

/// Handle SWAPDB — atomically exchange two databases across all shards.
///
/// Validates arguments, enforces the BGREWRITEAOF guard, handles the same-index
/// no-op, then delegates to `coordinate_swapdb` for multi-shard broadcast.
/// Returns `true` if consumed (caller should `continue`).
pub(super) async fn try_handle_swapdb(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"SWAPDB") {
        return false;
    }

    // Reject inside MULTI/EXEC queue.
    if conn.in_multi {
        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
        return true;
    }

    // TXN guard: SWAPDB rewrites entire DB contents and has no undo path —
    // reject during an active cross-store TXN so TXN.ABORT remains coherent.
    if conn.in_cross_txn() {
        // #499: poison the txn so COMMIT cannot report OK.
        conn.mark_cross_txn_rejected(cmd);
        responses.push(Frame::Error(Bytes::from_static(
            crate::command::transaction::ERR_TXN_CROSS_SHARD,
        )));
        return true;
    }

    // Exact arity check first — Redis returns the wrong-arity error for
    // anything other than SWAPDB <db1> <db2>.
    if cmd_args.len() != 2 {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'swapdb' command",
        )));
        return true;
    }
    let parse_db_index = |f: &Frame| -> Option<usize> {
        match f {
            Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
            Frame::Integer(n) => usize::try_from(*n).ok(),
            _ => None,
        }
    };
    let a = cmd_args.first().and_then(parse_db_index);
    let b = cmd_args.get(1).and_then(parse_db_index);
    let (a, b) = match (a, b) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
            return true;
        }
    };

    let db_count = ctx.shard_databases.db_count();
    if a >= db_count || b >= db_count {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR DB index is out of range",
        )));
        return true;
    }

    if a == b {
        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        return true;
    }

    if crate::command::persistence::AOF_REWRITE_IN_PROGRESS
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR cannot SWAPDB during BGREWRITEAOF",
        )));
        return true;
    }

    let response = crate::shard::coordinator::coordinate_swapdb(
        a,
        b,
        ctx.shard_id,
        ctx.num_shards,
        &ctx.shard_databases,
        &ctx.dispatch_tx,
        &ctx.spsc_notifiers,
        ctx.aof_pool.as_ref(),
        &ctx.repl_state,
    )
    .await;
    responses.push(response);
    true
}

/// Handle WAIT (R1, task #19): block until N replicas acknowledge the current
/// master offset or the timeout expires; reply with the acked count. Runs at
/// the connection layer because it awaits — the generic dispatch path is
/// synchronous (it used to hard-code `:0`). Returns `true` if consumed.
pub(super) async fn try_handle_wait(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"WAIT") {
        return false;
    }
    let int_arg = |f: &Frame| -> Option<u64> {
        match f {
            Frame::BulkString(b) | Frame::SimpleString(b) => {
                std::str::from_utf8(b).ok()?.trim().parse().ok()
            }
            Frame::Integer(n) if *n >= 0 => Some(*n as u64),
            _ => None,
        }
    };
    let (Some(num_required), Some(timeout_ms)) = (
        cmd_args.first().and_then(int_arg),
        cmd_args.get(1).and_then(int_arg),
    ) else {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'wait' command",
        )));
        return true;
    };
    // Redis: timeout 0 = block until satisfied. The poll loop is cooperative
    // (10ms ticks), so an effectively-unbounded deadline cannot starve the
    // shard thread — cap at one year rather than a literal infinity.
    let timeout_ms = if timeout_ms == 0 {
        31_536_000_000
    } else {
        timeout_ms
    };
    let count = match ctx.repl_state.as_ref() {
        Some(rs) => {
            crate::replication::master::wait_for_replicas(num_required as usize, timeout_ms, rs)
                .await
        }
        None => 0,
    };
    responses.push(Frame::Integer(count as i64));
    true
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
    cmd_args: &[Frame],
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
        // SELECT is flagged W but only mutates CONNECTION state — Redis
        // serves it on replicas (task #23, see handler_monoio::dispatch).
        if cmd.eq_ignore_ascii_case(b"SELECT") {
            return false;
        }
        // GRAPH.QUERY is blanket-W (Cypher CAN write); serve read-only
        // MATCH/RETURN on replicas. The classifier never false-negatives
        // for a write query — see handler_monoio::dispatch.
        #[cfg(feature = "graph")]
        if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
            && !crate::command::graph::is_cypher_write_query(cmd_args)
        {
            return false;
        }
        // WS and MQ are blanket-W (same reason as GRAPH.QUERY above — mixed
        // read/write subcommands under one command name). Wave B
        // readonly-enforcement fix (task #34 follow-up, see
        // wave-b-ws-mq-scope-2026-07-12.md finding #2); see
        // handler_monoio::dispatch for the full rationale.
        if cmd.eq_ignore_ascii_case(b"WS")
            && crate::command::workspace::is_ws_readonly_subcommand(cmd_args)
        {
            return false;
        }
        if cmd.eq_ignore_ascii_case(b"MQ")
            && crate::command::mq::is_mq_readonly_subcommand(cmd_args)
        {
            return false;
        }
        responses.push(Frame::Error(Bytes::from_static(
            b"READONLY You can't write against a read only replica.",
        )));
        return true;
    }
    false
}

/// MA12 + MA1 + Wave 3: Refuse write commands when any write stall is active.
///
/// Returns `true` if the command was blocked (caller should `continue`).
///
/// Stall sources (OR-merged):
/// - MA12 disk-pressure monitor (`is_write_paused`) — set every 5s.
/// - MA1 segment-backlog stall (`is_segment_stall_active`) — set every 1s.
/// - Wave 3 RSS memory watchdog (`mem_monitor::is_write_paused`) — set every 5s.
///
/// Hot path: three `Atomic::load(Relaxed)` — no allocation, no lock.
/// Read-only commands pass through unaffected; only writes are stalled.
/// Background compaction (FT.COMPACT, GRAPH.COMPACT) is exempt.
#[inline]
pub(super) fn try_enforce_disk_full(cmd: &[u8], responses: &mut Vec<Frame>) -> bool {
    if metadata::is_write(cmd) && crate::shard::segment_stall::is_any_write_stall_active() {
        // Distinguish the stall source for operator clarity. dir-lost first:
        // it also sets `is_write_paused`, and "diskfull" would send the
        // operator hunting free space instead of the missing data dir (#366).
        let msg: &'static [u8] = if crate::shard::disk_monitor::is_dir_lost() {
            b"MOONERR dirmissing: data directory was removed; writes refused until it is restored"
        } else if crate::shard::disk_monitor::is_write_paused() {
            b"MOONERR diskfull: writes paused until free space recovers"
        } else if crate::shard::mem_monitor::is_write_paused() {
            b"MOONERR memfull: writes paused until memory pressure recovers"
        } else {
            b"MOONERR busy: compaction backlog; too many unflushed immutable segments"
        };
        responses.push(Frame::Error(Bytes::from_static(msg)));
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

/// RFC v0.2-R3 (2A): master-side PSYNC is monoio-only — the tokio runtime has
/// no connection-hijack path. Answer with a clear error instead of the
/// generic unknown-command reply so an attaching replica's log says WHY.
pub(super) fn try_handle_psync_unsupported(cmd: &[u8], responses: &mut Vec<Frame>) -> bool {
    if !cmd.eq_ignore_ascii_case(b"PSYNC") {
        return false;
    }
    responses.push(Frame::Error(bytes::Bytes::from_static(
        b"ERR PSYNC requires runtime-monoio on the master (this build runs runtime-tokio)",
    )));
    true
}
