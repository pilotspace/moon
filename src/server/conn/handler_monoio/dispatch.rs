//! Connection-level command dispatchers for the monoio handler.
//!
//! AUTH gate, HELLO, ACL, CLIENT subcommands, CONFIG, BGSAVE/SAVE/LASTSAVE/BGREWRITEAOF,
//! REPLICAOF/REPLCONF, INFO, READONLY enforcement, CLUSTER, EVAL/EVALSHA/SCRIPT,
//! FUNCTION/FCALL/FCALL_RO, ACL permission gate, cross-shard KEYS/SCAN/DBSIZE,
//! multi-key commands, and blocking commands.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use bytes::Bytes;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::protocol::Frame;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::util::extract_bytes;
use crate::tracking::TrackingState;
use crate::workspace::strip_workspace_prefix_from_response;

use super::{extract_command, handle_blocking_command_monoio, handle_config, is_multi_key_command};

/// Result of the AUTH gate check.
pub(super) enum AuthGateResult {
    /// Command consumed (AUTH, HELLO succeeded/failed). Caller should `continue`.
    Consumed,
    /// QUIT received while not authenticated. Caller should set should_quit and `break`.
    Quit,
    /// Not an AUTH/HELLO/QUIT command. Caller should push NOAUTH and `continue`.
    NotAuth,
    /// Already authenticated -- AUTH gate does not apply.
    Authenticated,
}

/// Check the pre-authentication gate. Returns the action the caller should take.
pub(super) fn check_auth_gate(
    frame: &Frame,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    client_id: u64,
    // NOT an `InterceptReplies`: this gate runs BEFORE the command name is
    // extracted, so there is nothing to classify a shape from. It answers only
    // `AUTH` and `HELLO`, neither of which carries a shape — see
    // `conn::intercept`.
    responses: &mut Vec<Frame>,
    auth_delay_ms: &mut u64,
    codec: &mut crate::server::codec::RespCodec,
) -> AuthGateResult {
    if conn.authenticated {
        return AuthGateResult::Authenticated;
    }
    // MONITOR: feed the pre-auth AUTH/HELLO here, because this gate `continue`s
    // and the main feed hook further down is never reached for them. That makes
    // the FIRST AUTH of a session — the only one carrying a credential on a
    // password-protected server — the one command the feed would otherwise miss
    // entirely. Redaction happens in the formatter, so nothing leaks.
    if let Some((cmd, cmd_args)) = extract_command(frame)
        && (cmd.eq_ignore_ascii_case(b"AUTH") || cmd.eq_ignore_ascii_case(b"HELLO"))
    {
        crate::monitor::feed_frames(conn.selected_db, peer_addr, cmd, cmd_args);
    }
    match extract_command(frame) {
        Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"AUTH") => {
            let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &ctx.acl_table);
            if let Some(uname) = opt_user {
                conn.authenticated = true;
                conn.adopt_user(uname, &ctx.acl_table);
                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                    crate::auth_ratelimit::record_success(addr.ip());
                }
            } else {
                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                    *auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
                }
                conn.acl_log.push(crate::acl::AclLogEntry {
                    reason: "auth".to_string(),
                    object: "AUTH".to_string(),
                    username: conn.current_user.clone(),
                    client_addr: peer_addr.to_string(),
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                });
            }
            responses.push(response);
            AuthGateResult::Consumed
        }
        Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"HELLO") => {
            let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                cmd_args,
                conn.protocol_version,
                client_id,
                &ctx.acl_table,
                &mut conn.authenticated,
                crate::command::identity::hello_role_and_mode(
                    ctx.repl_state.as_ref(),
                    ctx.cluster_state.is_some(),
                ),
            );
            if !matches!(&response, Frame::Error(_)) {
                // MUST come before `conn.protocol_version` moves: the helper
                // reads it to learn what this batch STARTED in. Recorded at
                // `responses.len()`, the index this reply will occupy, so the
                // switch covers HELLO's own answer; replies already queued were
                // produced under the OLD protocol and keep it. See
                // `shared::encode_response_batch`.
                crate::server::conn::shared::note_protocol_switch(conn, responses.len(), new_proto);
                conn.protocol_version = new_proto;
                // Keep the wire codec in lockstep for single-frame encodes.
                codec.set_protocol_version(new_proto);
            }
            if let Some(name) = new_name {
                conn.client_name = Some(name);
            }
            if let Some(ref uname) = opt_user {
                conn.adopt_user(uname.clone(), &ctx.acl_table);
            }
            // HELLO AUTH rate limiting
            if matches!(&response, Frame::Error(_)) {
                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                    *auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
                }
            } else if opt_user.is_some() {
                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                    crate::auth_ratelimit::record_success(addr.ip());
                }
            }
            responses.push(response);
            AuthGateResult::Consumed
        }
        Some((cmd, _)) if cmd.eq_ignore_ascii_case(b"QUIT") => {
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
            AuthGateResult::Quit
        }
        _ => AuthGateResult::NotAuth,
    }
}

/// Handle CLUSTER subcommands. Returns `true` if consumed.
///
/// `#[inline]` so the "cmd != CLUSTER" early-return (the hot path for every
/// non-CLUSTER command in a pipeline batch) compiles to a single length + byte
/// compare inline at the call site, avoiding per-command call/ret overhead.
#[inline]
pub(super) fn try_handle_cluster(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CLUSTER") {
        return false;
    }
    if let Some(ref cs) = ctx.cluster_state {
        #[allow(clippy::unwrap_used)] // Fallback "127.0.0.1:6379" is a valid literal
        let self_addr: std::net::SocketAddr = format!("127.0.0.1:{}", ctx.config_port)
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:6379".parse().unwrap());
        let resp = crate::cluster::command::handle_cluster_command(cmd_args, cs, self_addr);
        // CLUSTER REPLICATE must actually replicate, not merely relabel the
        // node: the role it sets is read nowhere outside `src/cluster/`, so a
        // cluster replica held no data and could serve no read. Start the same
        // replica task REPLICAOF starts.
        if matches!(resp, Frame::SimpleString(ref ok) if ok.as_ref() == b"OK")
            && let Some((host, port)) =
                crate::cluster::command::cluster_replicate_target(cmd_args, cs)
            && let Some(ref rs) = ctx.repl_state
        {
            rs.write()
                .set_role(crate::replication::state::ReplicationRole::Replica {
                    host: host.clone(),
                    port,
                    state: crate::replication::handshake::ReplicaHandshakeState::PingPending,
                });
            // Bump the generation FIRST so any previously spawned replica task
            // sees itself superseded and exits instead of double-applying.
            let epoch = crate::replication::replica::bump_replica_task_epoch();
            let cfg = crate::replication::replica::ReplicaTaskConfig {
                master_host: host,
                master_port: port,
                repl_state: Arc::clone(rs),
                num_shards: ctx.num_shards,
                persistence_dir: None,
                listening_port: 0,
                epoch,
                stream_db: std::sync::atomic::AtomicUsize::new(0),
                shard_databases: ctx.shard_databases.clone(),
            };
            monoio::spawn(crate::replication::replica::run_replica_task(cfg));
        }
        responses.push(resp);
    } else {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR This instance has cluster support disabled",
        )));
    }
    true
}

/// Handle EVALSHA command. Returns `true` if consumed.
///
/// `#[inline]`: see `try_handle_cluster` rationale — name check inlines to the
/// caller so non-EVALSHA commands cost only a length + byte compare.
#[inline]
pub(super) async fn try_handle_evalsha(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    // `EVALSHA_RO` is `EVALSHA` with writes refused, and shares every step
    // below — resolving the caller, routing, the cached body. The ONE
    // difference is the flag handed to the executor.
    let read_only = cmd.eq_ignore_ascii_case(b"EVALSHA_RO");
    if !read_only && !cmd.eq_ignore_ascii_case(b"EVALSHA") {
        return false;
    }
    // moon#569: resolve the caller ONCE per script, then let every inner
    // `redis.call` be authorized against it (locally or on the shard this
    // script routes to).
    let script_acl = crate::acl::ScriptAcl::for_user(&ctx.acl_table, &conn.current_user);
    if let Some(routed) = crate::server::conn::shared::route_script_elsewhere(
        cmd,
        cmd_args,
        conn.selected_db,
        &script_acl,
        ctx,
    )
    .await
    {
        responses.push(routed);
        return true;
    }
    let (response, pending_flush) = crate::shard::slice::with_shard(|s| {
        let db_count = s.databases.len();
        // moon#685: `run_and_complete`, not a bare index, so a script's flush
        // finishes on the other fifteen databases — and reports what is left
        // for `finish_script_flush` to broadcast once this borrow has ended.
        crate::scripting::pending_flush::run_and_complete(s, conn.selected_db, |db| {
            crate::scripting::handle_evalsha(
                &ctx.lua,
                &ctx.script_cache,
                cmd_args,
                db,
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
                &script_acl,
                read_only,
            )
        })
    });
    let response = crate::server::conn::shared::finish_script_flush(
        pending_flush,
        response,
        conn.selected_db,
        // The script ran on THIS connection's shard, so that is the
        // leg already cleared (moon#705).
        ctx.shard_id,
        ctx,
    )
    .await;
    responses.push(response);
    true
}

/// Handle the Redis EVAL command. Returns `true` if consumed.
///
/// `#[inline]`: see `try_handle_cluster` rationale — name check inlines so
/// non-matching commands cost only a length + byte compare.
#[inline]
pub(super) async fn try_handle_eval(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    // `EVAL_RO` — see `try_handle_evalsha`.
    let read_only = cmd.eq_ignore_ascii_case(b"EVAL_RO");
    if !read_only && !cmd.eq_ignore_ascii_case(b"EVAL") {
        return false;
    }
    // moon#515: Redis caches an EVAL'd body server-wide, so `EVAL` once then
    // `EVALSHA` by sha is a supported idiom. Fan the body out on first sight
    // — BEFORE routing, so the load and the execution reach the target shard
    // in that order over the same SPSC ring.
    crate::server::conn::shared::eval_script_fanout(ctx, shutdown, cmd_args).await;
    // moon#569: see `try_handle_evalsha`.
    let script_acl = crate::acl::ScriptAcl::for_user(&ctx.acl_table, &conn.current_user);
    if let Some(routed) = crate::server::conn::shared::route_script_elsewhere(
        cmd,
        cmd_args,
        conn.selected_db,
        &script_acl,
        ctx,
    )
    .await
    {
        responses.push(routed);
        return true;
    }
    let (response, pending_flush) = crate::shard::slice::with_shard(|s| {
        let db_count = s.databases.len();
        // moon#685: see `try_handle_evalsha`.
        crate::scripting::pending_flush::run_and_complete(s, conn.selected_db, |db| {
            crate::scripting::handle_eval(
                &ctx.lua,
                &ctx.script_cache,
                cmd_args,
                db,
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
                &script_acl,
                read_only,
            )
        })
    });
    let response = crate::server::conn::shared::finish_script_flush(
        pending_flush,
        response,
        conn.selected_db,
        // The script ran on THIS connection's shard, so that is the
        // leg already cleared (moon#705).
        ctx.shard_id,
        ctx,
    )
    .await;
    responses.push(response);
    true
}

/// Handle SCRIPT subcommands (LOAD, EXISTS, FLUSH). Returns `true` if consumed.
///
/// Async since E3: the SCRIPT LOAD fan-out retries a full ring with bounded
/// backpressure instead of silently dropping the load (divergent per-shard
/// script caches). Cold path — SCRIPT is never hot.
pub(super) async fn try_handle_script(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"SCRIPT") {
        return false;
    }
    let (response, fanout) =
        crate::scripting::handle_script_subcommand(&ctx.script_cache, cmd_args);
    if let Some((sha1, script_bytes)) = fanout {
        crate::server::conn::shared::script_fanout_bounded(ctx, shutdown, &sha1, &script_bytes)
            .await;
    }
    responses.push(response);
    true
}

/// Handle cluster slot routing (pre-dispatch).
/// Returns `true` if the command was redirected (MOVED/ASK/CROSSSLOT) and should be skipped.
#[inline]
pub(super) fn try_handle_cluster_routing(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !crate::cluster::cluster_enabled() {
        return false;
    }
    let Some(ref cs) = ctx.cluster_state else {
        return false;
    };
    let was_asking = conn.asking;
    conn.asking = false;

    let maybe_key = super::extract_primary_key(cmd, cmd_args);
    if let Some(key) = maybe_key {
        let slot = crate::cluster::slots::slot_for_key(key);
        let route = cs.read().route_slot_for(
            slot,
            was_asking,
            conn.readonly,
            crate::command::metadata::is_write(cmd),
        );
        match route {
            crate::cluster::SlotRoute::Local => {} // proceed
            other => {
                let err_frame = other.into_error_frame(slot);
                responses.push(err_frame);
                return true;
            }
        }

        // CROSSSLOT check for multi-key commands
        if is_multi_key_command(cmd, cmd_args) {
            let first_slot = slot;
            let mut cross_slot = false;
            // COPY's keys are exactly args[0..2]; trailing args are the
            // REPLACE literal, which must not be slot-checked.
            let key_args: &[Frame] = if cmd.eq_ignore_ascii_case(b"COPY") {
                &cmd_args[..cmd_args.len().min(2)]
            } else {
                cmd_args
            };
            for arg in key_args.iter().skip(1) {
                if let Some(k) = match arg {
                    Frame::BulkString(b) => Some(b.as_ref()),
                    _ => None,
                } {
                    if crate::cluster::slots::slot_for_key(k) != first_slot {
                        cross_slot = true;
                        break;
                    }
                }
            }
            if cross_slot {
                responses.push(Frame::Error(Bytes::from_static(
                    b"CROSSSLOT Keys in request don't hash to the same slot",
                )));
                return true;
            }
        }
    }
    false
}

/// Handle AUTH command (when already authenticated). Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_auth(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    auth_delay_ms: &mut u64,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"AUTH") {
        return false;
    }
    let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &ctx.acl_table);
    if let Some(uname) = opt_user {
        conn.adopt_user(uname, &ctx.acl_table);
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            crate::auth_ratelimit::record_success(addr.ip());
        }
    } else if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
        *auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
    }
    responses.push(response);
    true
}

/// Handle HELLO command (protocol negotiation, ACL-aware). Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_hello(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    client_id: u64,
    peer_addr: &str,
    auth_delay_ms: &mut u64,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
    codec: &mut crate::server::codec::RespCodec,
    switch_index: Option<usize>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"HELLO") {
        return false;
    }
    let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
        cmd_args,
        conn.protocol_version,
        client_id,
        &ctx.acl_table,
        &mut conn.authenticated,
        crate::command::identity::hello_role_and_mode(
            ctx.repl_state.as_ref(),
            ctx.cluster_state.is_some(),
        ),
    );
    if !matches!(&response, Frame::Error(_)) {
        // MUST come before `conn.protocol_version` moves: the helper reads it
        // to learn what this batch STARTED in. Recorded at `responses.len()`,
        // the index this reply will occupy, so the switch covers HELLO's own
        // answer; replies already queued were produced under the OLD protocol
        // and keep it. See `shared::encode_response_batch`.
        //
        // `switch_index` overrides that and is `Some` only when this HELLO ran
        // from inside an `EXEC` post-pass (moon#639): there the sink wraps a
        // LOCAL one-element vec, so `responses.len()` is 0 and would record the
        // switch at the START of the outer batch — re-encoding replies produced
        // before HELLO was even queued. The caller passes the outer index the
        // EXEC reply itself occupies. Switching is whole-reply granular, so the
        // entire EXEC array encodes in the new protocol; the sharded handler
        // makes the identical approximation.
        let at = switch_index.unwrap_or_else(|| responses.len());
        crate::server::conn::shared::note_protocol_switch(conn, at, new_proto);
        conn.protocol_version = new_proto;
        // Keep the wire codec in lockstep for single-frame encodes.
        codec.set_protocol_version(new_proto);
    }
    if let Some(name) = new_name {
        conn.client_name = Some(name);
    }
    if let Some(ref uname) = opt_user {
        conn.adopt_user(uname.clone(), &ctx.acl_table);
    }
    if matches!(&response, Frame::Error(_)) {
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            *auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
        }
    } else if opt_user.is_some() {
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            crate::auth_ratelimit::record_success(addr.ip());
        }
    }
    responses.push(response);
    true
}

/// Handle ACL command (intercepted at connection level). Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_acl(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"ACL") {
        return false;
    }
    let response = crate::command::acl::handle_acl(
        cmd_args,
        &ctx.acl_table,
        &mut conn.acl_log,
        &conn.current_user,
        peer_addr,
        &ctx.runtime_config,
        conn.client_id,
    );
    responses.push(response);
    true
}

/// Handle CONFIG GET/SET. Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_config(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CONFIG") {
        return false;
    }
    // `CONFIG GET` reaches the wire as a Map, not the flat Array it is built
    // as — applied by `responses`, which is an `InterceptReplies`. The
    // hand-written conversion that used to sit here is what every future
    // intercept would have had to copy (moon#462).
    responses.push(handle_config(cmd_args, &ctx.runtime_config, &ctx.config));
    true
}

/// Handle REPLICAOF / SLAVEOF. Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_replicaof(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
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
                    monoio::spawn(crate::replication::replica::run_replica_task(cfg));
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

/// Handle REPLCONF command. Returns `true` if consumed.
///
/// Side effect: when REPLCONF is observed on a master, eagerly allocate the
/// per-shard replication backlogs so that subsequent writes between now and
/// PSYNC arrival are captured for partial resync. This fixes the
/// chicken-and-egg gap where the original code only allocated on
/// `RegisterReplica` (after PSYNC), causing the master to buffer zero bytes
/// during the handshake window.
///
/// Task #70: allocation-only — does NOT activate the sticky `FANOUT_HINT`.
/// A bare REPLCONF (health-checker probe, or a handshake that never sends
/// PSYNC) must not permanently tax every subsequent write with the
/// replication serialize+SPSC round trip. The hint is activated on the
/// actual PSYNC arrival instead (`try_handle_psync` above).
#[inline]
pub(super) fn try_handle_replconf(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"REPLCONF") {
        return false;
    }
    if let Some(ref rs) = ctx.repl_state {
        let g = rs.read();
        if matches!(g.role, crate::replication::state::ReplicationRole::Master) {
            g.ensure_backlogs_allocated();
        }
    }
    responses.push(crate::command::connection::replconf(cmd_args));
    true
}

/// CDC.READ — polling-based change data capture (C3 v1).
///
/// Stateless / synchronous — reads WAL files from disk, no shard state
/// involved. Mirrors the identical function in handler_sharded/dispatch.rs.
pub(super) fn try_handle_cdc_read(
    cmd: &[u8],
    cmd_args: &[Frame],
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CDC.READ") {
        return false;
    }
    responses.push(crate::command::cdc::cdc_read(cmd_args));
    true
}

/// Handle PSYNC command. Returns `Some((repl_id, offset))` when this PSYNC
/// arrival should hijack the connection. The caller breaks out of the dispatch
/// loop and returns the stream so the master replication driver can take over.
///
/// Returns `None` for non-PSYNC commands.
/// Returns `Some((..))` for every accepted PSYNC — the accept loop routes the
/// hijacked stream to the single-shard inline handler or, at num_shards > 1,
/// to the R2 multi-shard handler (`handle_psync_inline_multi_shard`).
pub(super) fn try_handle_psync(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> Option<(String, i64)> {
    if !cmd.eq_ignore_ascii_case(b"PSYNC") {
        return None;
    }
    if cmd_args.len() != 2 {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'psync' command",
        )));
        return None;
    }
    let Some(ref rs) = ctx.repl_state else {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR replication is not enabled on this server",
        )));
        return None;
    };
    {
        let g = rs.read();
        let is_master = matches!(g.role, crate::replication::state::ReplicationRole::Master);
        if !is_master {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR PSYNC is only valid on a master",
            )));
            return None;
        }
        g.ensure_backlogs_allocated();
        // Task #70: this is the actual-PSYNC arrival — activate the sticky
        // fanout hint HERE, not on a bare REPLCONF (see
        // `ReplicationState::ensure_backlogs_allocated` doc comment). A
        // handshake that reaches PSYNC is a genuine replica attaching; a
        // REPLCONF-only probe never reaches this line.
        crate::replication::state::mark_fanout_active();
        // Correctness fix (orchestrator-caught regression on the above):
        // a bare REPLCONF may have already allocated this shard's backlog
        // and left it skewed stale during the FANOUT_HINT-false window (see
        // `ReplicationState::realign_backlog` doc comment). Realign BEFORE
        // any snapshot/cut offset is captured below — this function runs on
        // the connection task, which at shards=1 (the only case this
        // single-shard inline leg handles) IS the owning shard's own
        // event-loop thread, so the offset read inside `realign_backlog`
        // and this shard's own append/advance sequence cannot interleave.
        g.realign_backlog(ctx.shard_id);
    }
    let repl_id = match &cmd_args[0] {
        Frame::BulkString(b) | Frame::SimpleString(b) => String::from_utf8_lossy(b).into_owned(),
        _ => {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR PSYNC: invalid replid",
            )));
            return None;
        }
    };
    let offset_bytes = match &cmd_args[1] {
        Frame::BulkString(b) | Frame::SimpleString(b) => b.as_ref(),
        _ => {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR PSYNC: invalid offset",
            )));
            return None;
        }
    };
    let offset_str = match std::str::from_utf8(offset_bytes) {
        Ok(s) => s,
        Err(_) => {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR PSYNC: offset must be an integer",
            )));
            return None;
        }
    };
    let offset: i64 = match offset_str.parse() {
        Ok(n) => n,
        Err(_) => {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR PSYNC: offset must be an integer",
            )));
            return None;
        }
    };
    Some((repl_id, offset))
}

/// Handle INFO command. Returns `true` if consumed.
#[inline]
pub(super) async fn try_handle_info(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
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
    // The real replication section is PASSED IN, not appended: `info()` writes
    // a stub `# Replication`, so appending produced the section twice and left
    // section filtering unable to see the final set.
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
    // Leaves as a VerbatimString on RESP3 — Redis answers `INFO` through
    // `addReplyVerbatim` in every section form. Applied by `responses`; this
    // intercept carried no conversion at all until moon#462, which is the bug
    // the sink exists to make unwritable.
    responses.push(resp_frame);
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
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
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

/// Handle READONLY enforcement: reject writes on replicas.
/// Returns `true` if the command was blocked.
///
/// S3.5a (2026-04-27): hot path now reads `ctx.is_replica_mirror` (a single
/// `AtomicBool::load(Acquire)`) instead of taking `ctx.repl_state.try_read()`
/// per command. ARM perf annotate showed `RwLock::try_read` was a CAS
/// (`mov w8, #0xfffd; cmp w11, w9` = ~84% self-time inside this fn) — the
/// mirror eliminates it. `ReplicationState::set_role()` is the single owner
/// of the mirror invariant.
#[inline]
pub(super) fn try_enforce_readonly(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    let Some(ref mirror) = ctx.is_replica_mirror else {
        return false;
    };
    if !mirror.load(std::sync::atomic::Ordering::Acquire) {
        return false;
    }
    if metadata::is_write(cmd) {
        // SELECT is flagged W in the metadata table (it routes through the
        // write dispatch paths) but only mutates CONNECTION state — Redis
        // serves it on replicas, and a client cannot read a replica's
        // non-zero dbs without it (task #23).
        if cmd.eq_ignore_ascii_case(b"SELECT") {
            return false;
        }
        // GRAPH.QUERY is blanket-W in the metadata table because Cypher CAN
        // write; a read-only MATCH/RETURN must still be served by a replica.
        // Reuse the token-scan classifier the write dispatch path branches
        // on — it can false-POSITIVE (blocks a weird read) but never
        // false-negative (lets a write through).
        #[cfg(feature = "graph")]
        if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
            && !crate::command::graph::is_cypher_write_query(cmd_args)
        {
            return false;
        }
        // WS and MQ are blanket-W (same reason as GRAPH.QUERY: the command
        // name carries mixed read/write subcommands — WS CREATE/DROP and MQ
        // CREATE/PUSH/POP/ACK/TRIGGER/PUBLISH mutate, but WS LIST/INFO/AUTH
        // and MQ DLQLEN are reads). Wave B readonly-enforcement fix (task
        // #34 follow-up, see wave-b-ws-mq-scope-2026-07-12.md finding #2).
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
/// Read-only commands pass through unaffected. Background compaction is exempt.
#[inline]
pub(super) fn try_enforce_disk_full(
    cmd: &[u8],
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if metadata::is_write(cmd) {
        // moon#718: the ladder AND the backlog exemption live in
        // `segment_stall::stall_refusal`, so both dispatch paths cannot drift
        // apart — they previously carried byte-identical copies of the ladder,
        // and a fix applied to one would have silently missed the other.
        if let Some(msg) = crate::shard::segment_stall::stall_refusal(
            cmd,
            crate::shard::segment_stall::StallSources::current(),
        ) {
            responses.push(Frame::Error(Bytes::from_static(msg)));
            return true;
        }
    }
    false
}

/// Handle CLIENT subcommands that are safe to run BEFORE the ACL gate:
/// ID, SETNAME, GETNAME (connection-local metadata, like Redis's NO-AUTH
/// connection commands). TRACKING is intentionally NOT here — it mutates
/// server-side invalidation state and is gated post-ACL by
/// `try_handle_client_tracking` (H-3).
/// Returns `true` if a subcommand was consumed (caller should `continue`).
/// Returns `false` for TRACKING and admin subcommands (LIST, INFO, KILL, PAUSE,
/// UNPAUSE, NO-EVICT, NO-TOUCH) which must pass through the ACL gate first.
#[inline]
pub(super) fn try_handle_client_early(
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    conn: &mut ConnectionState,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
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
            // CLIENT TRACKING is handled post-ACL by
            // `try_handle_client_tracking` (H-3) — it mutates server-side
            // invalidation state, so it must not be exempt from the ACL gate.
            // Admin CLIENT subcommands (LIST, INFO, KILL, PAUSE, UNPAUSE,
            // NO-EVICT, NO-TOUCH) also fall through to the ACL gate below.
        }
    }
    // Fall through -- admin subcommands handled after ACL check.
    false
}

/// Handle `CLIENT TRACKING` — placed AFTER the ACL gate (H-3) because it
/// registers/tears down server-side invalidation state and must therefore be
/// deniable like any other privileged command. Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_client_tracking(
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CLIENT") {
        return false;
    }
    let Some(sub) = cmd_args.first() else {
        return false;
    };
    let Some(sub_bytes) = extract_bytes(sub) else {
        return false;
    };
    if !sub_bytes.eq_ignore_ascii_case(b"TRACKING") {
        return false;
    }
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
                        table.register_prefix(client_id, prefix.clone(), config_parsed.noloop);
                    }
                }
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
            } else {
                conn.tracking_state = TrackingState::default();
                ctx.tracking_table.lock().untrack_all(client_id);
                conn.tracking_rx = None;
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
            }
            true
        }
        Err(err_frame) => {
            responses.push(err_frame);
            true
        }
    }
}

/// Handle CLIENT admin subcommands (LIST, INFO, KILL, PAUSE, UNPAUSE, NO-EVICT, NO-TOUCH).
/// Placed AFTER ACL check so restricted users cannot access admin ops.
/// Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_client_admin(
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    conn: &ConnectionState,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CLIENT") {
        return false;
    }
    if let Some(sub) = cmd_args.first() {
        if let Some(sub_bytes) = extract_bytes(sub) {
            // TRACKING is owned by `try_handle_client_tracking`, which runs
            // AFTER this handler in the frame loop (both are post-ACL). The
            // unknown-subcommand fallback below must not swallow it — that
            // regression (H-3 reorder, #258) made CLIENT TRACKING answer
            // "unknown subcommand" on the entire monoio runtime.
            if sub_bytes.eq_ignore_ascii_case(b"TRACKING") {
                return false;
            }
            if sub_bytes.eq_ignore_ascii_case(b"LIST") {
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
                // No conversion call here any more: `responses` is an
                // `InterceptReplies`, which applies the RESP3 policy on push.
                // The hand-written `Resp3Shape::Verbatim` this replaces was the
                // patch that fixed CLIENT INFO and left CLIENT LIST — one line
                // away, same function — wrong (moon#462).
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
                // ON|OFF is mandatory (moon#580) — shared with the other two
                // dispatch paths so the three cannot drift.
                responses.push(crate::command::client::no_evict_or_no_touch(
                    &sub_bytes, cmd_args,
                ));
                return true;
            }
            // Unknown CLIENT subcommand
            if let Some(help) = crate::command::help_text::help_if_requested("CLIENT", &sub_bytes) {
                responses.push(help);
                return true;
            }
            responses.push(crate::command::helpers::err_unknown_subcommand(
                "CLIENT", &sub_bytes,
            ));
            return true;
        }
    }
    responses.push(Frame::Error(Bytes::from_static(
        b"ERR wrong number of arguments for 'client' command",
    )));
    true
}

/// Handle persistence commands (BGSAVE, SAVE, LASTSAVE, BGREWRITEAOF).
/// Returns `true` if consumed.
#[inline]
pub(super) fn try_handle_persistence(
    cmd: &[u8],
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
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

/// Outcome of `try_handle_shutdown`. See the sharded-handler twin
/// (`handler_sharded::dispatch::ShutdownOutcome`) for the full rationale.
pub(super) enum ShutdownOutcome {
    NotShutdown,
    Rejected,
    Exiting,
}

/// Handle SHUTDOWN [NOSAVE|SAVE] on the monoio runtime.
///
/// Mirrors `handler_sharded::dispatch::try_handle_shutdown`: a forced save
/// uses the cooperative per-shard BGSAVE snapshot (there is no synchronous
/// single-threaded SAVE in this mode) and polls for completion with a
/// bounded timeout via `monoio::time::sleep` (this handler's native runtime
/// -- `tokio::time::sleep` would not drive monoio's `!Send` reactor).
pub(super) async fn try_handle_shutdown(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    shutdown: &CancellationToken,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
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
            monoio::time::sleep(std::time::Duration::from_millis(
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

/// Handle SWAPDB — atomically exchange two databases across all shards.
///
/// Validates arguments, enforces the BGREWRITEAOF guard, handles the same-index
/// no-op, then delegates to `coordinate_swapdb` for multi-shard broadcast.
/// Returns `true` if consumed (caller should `continue`).
pub(super) async fn try_handle_swapdb(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut crate::server::conn::core::ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"SWAPDB") {
        return false;
    }

    // Reject inside MULTI/EXEC queue (SWAPDB is not transactional).
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

    // Parse args: SWAPDB <db1> <db2> — exact arity, Redis-compatible error.
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

    // Same-index: no-op, no WAL, return OK immediately.
    if a == b {
        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        return true;
    }

    // Reject if BGREWRITEAOF is in progress.
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

/// Handle ACL permission check (NOPERM gate).
/// Returns `true` if the command was denied (caller should `continue`).
#[inline]
pub(super) fn try_enforce_acl(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if conn.acl_skip_allowed() {
        return false;
    }
    #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
    let acl_guard = ctx.acl_table.read().unwrap();
    if let Some(deny_reason) = acl_guard.check_command_permission(&conn.current_user, cmd, cmd_args)
    {
        drop(acl_guard);
        conn.acl_log.push(crate::acl::AclLogEntry {
            reason: "command".to_string(),
            object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
            username: conn.current_user.clone(),
            client_addr: peer_addr.to_string(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        });
        responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
        return true;
    }

    // === ACL key pattern check (same lock guard) ===
    let is_write_for_acl = metadata::is_write(cmd);
    if let Some(deny_reason) =
        acl_guard.check_key_permission(&conn.current_user, cmd, cmd_args, is_write_for_acl)
    {
        drop(acl_guard);
        conn.acl_log.push(crate::acl::AclLogEntry {
            reason: "command".to_string(),
            object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
            username: conn.current_user.clone(),
            client_addr: peer_addr.to_string(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        });
        responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
        return true;
    }
    false
}

/// Handle FUNCTION/FCALL/FCALL_RO commands. Returns `true` if consumed.
/// Placed AFTER ACL check. Skipped when conn.in_multi (fall through to MULTI queue).
///
/// Async since moon#514: `FUNCTION LOAD`/`DELETE`/`FLUSH` fan out to every
/// other shard, and `FCALL`/`FCALL_RO` route to the shard owning their key.
/// Cold path — the Functions API is never hot.
pub(super) async fn try_handle_functions(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    func_registry: &Rc<RefCell<Option<crate::scripting::FunctionRegistry>>>,
    shutdown: &crate::runtime::cancel::CancellationToken,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
) -> bool {
    if conn.in_multi {
        return false;
    }
    if cmd.eq_ignore_ascii_case(b"FUNCTION") {
        crate::server::conn::core::ensure_function_registry(func_registry, ctx);
        // Borrow scoped to this block, and released before the fan-out await.
        // The registry `RefCell` is shared with this shard thread's SPSC drain
        // loop, which applies INBOUND fan-outs — holding it across a yield
        // would make an arriving library land on a borrowed cell and be
        // dropped. A trailing `drop(guard)` is not enough: it satisfies the
        // borrow checker but still trips `await_holding_refcell_ref`.
        let mut response = {
            let mut guard = func_registry.borrow_mut();
            #[allow(clippy::unwrap_used)]
            // ensure_function_registry guarantees Some
            crate::command::functions::handle_function(guard.as_mut().unwrap(), cmd_args)
        };
        // A replay that did not reach every shard REPLACES the local reply.
        // The client asked for a server-wide mutation; telling it `+OK` over a
        // registry that answers differently per shard is the defect this fix
        // exists to remove, not a shape to preserve on the failure path.
        if let Some(op) = crate::server::conn::shared::function_fanout_op(cmd_args, &response) {
            if let Some(partial) =
                crate::server::conn::shared::function_registry_fanout(ctx, shutdown, op).await
            {
                response = partial;
            }
        }
        responses.push(response);
        return true;
    }
    let is_fcall = cmd.eq_ignore_ascii_case(b"FCALL");
    if is_fcall || cmd.eq_ignore_ascii_case(b"FCALL_RO") {
        // moon#569: FCALL bodies are ACL-gated per `redis.call`, same as
        // EVAL. Resolved BEFORE routing so the same identity is used whether
        // the call runs here or on the shard that owns the key — routing must
        // never change what a caller is allowed to do.
        let script_acl = crate::acl::ScriptAcl::for_user(&ctx.acl_table, &conn.current_user);
        // moon#514 defect 1 — the same root cause as moon#508. FCALL used to
        // require every key to hash to the CONNECTION's shard, so a single
        // key living anywhere else was refused `CROSSSLOT`; one key cannot
        // cross a slot, it just lives elsewhere. Route it to the shard that
        // owns the key. A genuinely cross-shard key set is still refused,
        // before anything is touched.
        if let Some(routed) = crate::server::conn::shared::route_script_elsewhere(
            cmd,
            cmd_args,
            conn.selected_db,
            &script_acl,
            ctx,
        )
        .await
        {
            responses.push(routed);
            return true;
        }
        crate::server::conn::core::ensure_function_registry(func_registry, ctx);
        // moon#685: the registry borrow lives in its own block. `finish_script_flush`
        // below awaits, and a `RefCell` borrow held across an await is how a second
        // command on this connection would find the registry already borrowed.
        let (response, pending_flush) = {
            let guard = func_registry.borrow();
            #[allow(clippy::unwrap_used)]
            // ensure_function_registry guarantees Some
            let reg = guard.as_ref().unwrap();
            crate::shard::slice::with_shard(|s| {
                let db_count = s.databases.len();
                // moon#685: a FUNCTION body reaches `redis.call` through the same
                // bridge an EVAL does, so it needs the same completion.
                crate::scripting::pending_flush::run_and_complete(s, conn.selected_db, |db| {
                    if is_fcall {
                        crate::command::functions::handle_fcall(
                            reg,
                            cmd_args,
                            db,
                            ctx.shard_id,
                            ctx.num_shards,
                            conn.selected_db,
                            db_count,
                            &script_acl,
                        )
                    } else {
                        crate::command::functions::handle_fcall_ro(
                            reg,
                            cmd_args,
                            db,
                            ctx.shard_id,
                            ctx.num_shards,
                            conn.selected_db,
                            db_count,
                            &script_acl,
                        )
                    }
                })
            })
        };
        let response = crate::server::conn::shared::finish_script_flush(
            pending_flush,
            response,
            conn.selected_db,
            // The script ran on THIS connection's shard, so that is the
            // leg already cleared (moon#705).
            ctx.shard_id,
            ctx,
        )
        .await;
        responses.push(response);
        return true;
    }
    false
}

/// Handle cross-shard aggregation commands: KEYS, SCAN, DBSIZE, RANDOMKEY,
/// HOTKEYS, and multi-key commands.
/// Returns `true` if consumed.
pub(super) async fn try_handle_cross_shard_commands(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut crate::server::conn::intercept::InterceptReplies<'_>,
    // v3-5 group commit: response indexes of local-leg writes pending the
    // batch-end fsync_barrier(ctx.shard_id) (appendfsync=always only).
    local_leg_write_idxs: &mut Vec<usize>,
) -> bool {
    if ctx.num_shards <= 1 {
        return false;
    }
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
            &(), // monoio: coordinator uses oneshot, not response_pool
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
            &(), // monoio: coordinator uses oneshot, not response_pool
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
            &(), // monoio: coordinator uses oneshot, not response_pool
        )
        .await;
        responses.push(response);
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"RANDOMKEY") {
        let mut response = crate::shard::coordinator::coordinate_randomkey(
            ctx.shard_id,
            ctx.num_shards,
            conn.selected_db,
            &ctx.shard_databases,
            &ctx.dispatch_tx,
            &ctx.spsc_notifiers,
            &(), // monoio: coordinator uses oneshot, not response_pool
        )
        .await;
        if let Some(ws_id) = conn.workspace_id.as_ref() {
            strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
        }
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
                    &(), // monoio: coordinator uses oneshot, not response_pool
                )
                .await
            }
            Err(e) => e,
        };
        responses.push(response);
        return true;
    }

    // --- Multi-key commands: MGET, MSET, DEL, UNLINK, EXISTS ---
    if is_multi_key_command(cmd, cmd_args) {
        // moon#513 (A1): when ONE shard owns every key, decline the command here
        // and let ordinary routing slot it into that shard's batch instead of
        // executing it inline. `extract_primary_key` hashes its first key, which
        // names that same owner, so the routing decision cannot disagree with this
        // one. A LOCAL owner stays on the coordinator: it is already correct there,
        // `remote_groups` never holds the local shard, so nothing is gained.
        //
        // Paired with the `count_ones() == 1` arm of `must_wait_for_pending_remote`
        // — that arm skips the wait BECAUSE this fall-through slots the command.
        if crate::server::conn::shared::single_owner_shard(cmd, cmd_args, ctx.num_shards)
            .is_some_and(|owner| owner != ctx.shard_id)
        {
            return false;
        }
        let mut local_barrier_pending = false;
        let response = crate::shard::coordinator::coordinate_multi_key(
            cmd,
            cmd_args,
            ctx.shard_id,
            ctx.num_shards,
            conn.selected_db,
            &ctx.shard_databases,
            &ctx.dispatch_tx,
            &ctx.spsc_notifiers,
            &ctx.cached_clock,
            ctx.aof_pool.as_ref(),
            &ctx.repl_state,
            &mut local_barrier_pending,
            &(), // monoio: coordinator uses oneshot, not response_pool
        )
        .await;
        // A response that is already an error must not be overwritten by a
        // barrier failure; only successful writes join the barrier set.
        if local_barrier_pending && !matches!(response, Frame::Error(_)) {
            local_leg_write_idxs.push(responses.len());
        }
        // CLIENT TRACKING: multi-key writes (DEL/MSET/UNLINK/…) invalidate
        // every key; multi-key reads (MGET) by a tracking client register
        // every key.
        if !matches!(response, Frame::Error(_)) {
            crate::tracking::invalidation::invalidate_after_write(
                &ctx.tracking_table,
                cmd,
                cmd_args,
                conn.client_id,
            );
            if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                crate::tracking::invalidation::track_read_keys(
                    &ctx.tracking_table,
                    cmd,
                    cmd_args,
                    conn.client_id,
                    conn.tracking_state.noloop,
                );
            }
        }
        responses.push(response);
        return true;
    }
    false
}

/// Result of blocking command handling.
pub(super) enum BlockingResult {
    /// Not a blocking command.
    NotBlocking,
    /// In MULTI: queued as non-blocking variant. Caller should `continue`.
    Queued,
    /// Blocking command handled. Caller must `break` (ends pipeline).
    Handled,
    /// Write error during flush. Caller should return Done.
    WriteError,
    /// c10k A1: the client vanished while blocked. Its registrations are gone;
    /// the caller must close the connection WITHOUT writing a reply.
    PeerGone,
}

/// Handle blocking commands (BLPOP, BRPOP, BLMOVE, etc.).
///
/// c10k A1: `read_buf` is the handler's own accumulation buffer, passed in so
/// the peer watch can carry any bytes the client pipelines behind its blocking
/// command straight back into the parse stream. It holds only the unparsed
/// tail of the current batch here, so appending preserves wire order.
pub(super) async fn try_handle_blocking<
    S: monoio::io::AsyncWriteRent + super::idle_park::IdleParkRead,
>(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    // NOT an `InterceptReplies`: this intercept flushes the batch, then
    // encodes its own reply straight into `write_buf` and CLEARS the vector,
    // because the reply arrives long after the batch was written. It applies
    // the RESP3 policy itself, below — see `conn::intercept` for the two paths
    // that are allowed to.
    responses: &mut Vec<Frame>,
    local_leg_write_idxs: &mut Vec<usize>,
    codec: &mut crate::server::codec::RespCodec,
    write_buf: &mut bytes::BytesMut,
    read_buf: &mut bytes::BytesMut,
    stream: &mut S,
    shutdown: &CancellationToken,
    client_live: &std::sync::Arc<crate::client_registry::ClientLiveState>,
) -> BlockingResult {
    if !crate::server::conn::blocking::is_blocking_command_args(cmd, cmd_args) {
        return BlockingResult::NotBlocking;
    }

    // Inside MULTI: queue the form EXEC can run without blocking — the
    // non-blocking twin, or the command itself for the four whose twin answers
    // a different shape (moon#524).
    if conn.in_multi {
        let nb_frame = super::queued_blocking_frame(cmd, cmd_args);
        conn.command_queue.push(nb_frame);
        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
        return BlockingResult::Queued;
    }

    // Earlier frames in this batch may hold barrier-pending local-leg
    // writes — confirm (or fail-loud) them before this early flush, and
    // clear the indexes so the batch-end barrier never sees stale ones.
    crate::server::conn::shared::resolve_local_leg_barrier(
        &ctx.aof_pool,
        ctx.shard_id,
        local_leg_write_idxs,
        responses,
    )
    .await;

    // Flush accumulated responses before blocking
    for resp in &*responses {
        codec.encode_frame(resp, write_buf);
    }
    if !write_buf.is_empty() {
        use monoio::io::AsyncWriteRentExt;
        let data = write_buf.split().freeze();
        let (result, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
        if result.is_err() {
            return BlockingResult::WriteError;
        }
    }

    // D1: mark the connection blocked for the duration of the wait. The
    // idle-timeout sweep (`client_registry::kill_idle_clients`) exempts
    // blocked clients, matching Redis — without this a client parked in
    // `BLPOP key 0` looks idle and gets closed at `timeout`.
    // RAII, not a set/clear pair: a leaked `blocked` bit exempts the client
    // from `timeout` permanently.
    let blocked_guard = client_live.blocked_guard();
    let outcome = handle_blocking_command_monoio(
        cmd,
        cmd_args,
        conn.selected_db,
        &ctx.shard_databases,
        &ctx.blocking_registry,
        ctx.shard_id,
        ctx.num_shards,
        &ctx.dispatch_tx,
        shutdown,
        &ctx.spsc_notifiers,
        stream,
        read_buf,
    )
    .await;
    drop(blocked_guard);

    let blocking_response = match outcome {
        crate::server::conn::blocking::BlockingOutcome::Reply(frame) => frame,
        crate::server::conn::blocking::BlockingOutcome::PeerGone => {
            responses.clear();
            return BlockingResult::PeerGone;
        }
    };

    // moon#644: the blocking path modifies the keyspace, so it owes tracking
    // clients an invalidation exactly like every other write — it just never
    // sent one. Placed BEFORE the RESP3 conversion so the reply still has the
    // RESP2 shapes `blocking_served_keys` reads (the conversion rewrites
    // scores and containers, and the served key must not depend on which
    // protocol the client negotiated).
    crate::tracking::invalidation::invalidate_after_blocking_serve(
        &ctx.tracking_table,
        cmd,
        cmd_args,
        &blocking_response,
        conn.client_id,
    );

    // moon#559 / moon#462: this is an INTERCEPT — it short-circuits the
    // dispatch exit where every other reply meets the RESP3 policy — so it
    // must apply that policy itself or the whole blocking family answers
    // RESP2 shapes to a RESP3 client. It did not, which is why BZPOPMIN's
    // score reached RESP3 clients as a BulkString on the shipped (monoio)
    // runtime while the tokio handler, which does convert here, was right.
    // Routed through the SAME choke point rather than a second table.
    let blocking_response = crate::server::conn::util::apply_resp3_conversion(
        cmd,
        cmd_args,
        blocking_response,
        conn.protocol_version,
    );

    // Encode blocking response directly
    codec.encode_frame(&blocking_response, write_buf);
    responses.clear();
    BlockingResult::Handled
}

/// Execute one queued connection-level intercept at `EXEC` time (moon#639).
///
/// The transaction executor cannot run these: they never reach `dispatch()`,
/// they need `ConnectionState`/`ConnectionContext`, and on a routed body the
/// executor is not even on this thread. It leaves a
/// [`crate::server::conn::shared::TXN_INTERCEPT_PLACEHOLDER`] in the result
/// array and the caller replaces that slot with what this returns.
///
/// The chain below is the SAME sequence, in the same order, that the main loop
/// runs — `AUTH`/`HELLO` first (they are intercepted above the queue gate and
/// skip themselves while `conn.in_multi`), then the rest. A family that gains a
/// new intercept must be added in both places or its queued form answers
/// "unknown command" while its live form works, which no single test would
/// catch. `me12` drives every member of
/// [`crate::server::conn::shared::is_txn_connection_intercept`] through `EXEC`
/// and fails on an error reply, which is that test.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_txn_connection_intercept(
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    shutdown: &crate::runtime::cancel::CancellationToken,
    codec: &mut crate::server::codec::RespCodec,
    switch_index: usize,
    func_registry: &Rc<RefCell<Option<crate::scripting::FunctionRegistry>>>,
) -> Frame {
    let proto = conn.protocol_version;
    let mut out: Vec<Frame> = Vec::with_capacity(1);
    // The delay an AUTH failure would impose on the live path. Inside EXEC
    // there is no read loop to slow down, so it is collected and dropped:
    // sleeping here would hold up the rest of the transaction's replies.
    let mut auth_delay_ms: u64 = 0;

    macro_rules! shaped {
        () => {
            &mut crate::server::conn::intercept::InterceptReplies::new(
                &mut out, cmd, cmd_args, proto,
            )
        };
    }

    let handled = try_handle_auth(
        cmd,
        cmd_args,
        conn,
        ctx,
        peer_addr,
        &mut auth_delay_ms,
        shaped!(),
    ) || try_handle_hello(
        cmd,
        cmd_args,
        conn,
        ctx,
        client_id,
        peer_addr,
        &mut auth_delay_ms,
        shaped!(),
        codec,
        Some(switch_index),
    ) || try_handle_cluster(cmd, cmd_args, ctx, shaped!())
        || try_handle_script(cmd, cmd_args, ctx, shutdown, shaped!()).await
        || try_handle_acl(cmd, cmd_args, conn, ctx, peer_addr, shaped!())
        || try_handle_config(cmd, cmd_args, ctx, shaped!())
        || try_handle_wait(cmd, cmd_args, ctx, shaped!()).await
        || try_handle_client_early(cmd, cmd_args, client_id, conn, shaped!())
        || try_handle_client_tracking(cmd, cmd_args, client_id, conn, ctx, shaped!())
        || try_handle_client_admin(cmd, cmd_args, client_id, conn, shaped!())
        || super::pubsub::try_handle_pubsub_introspection(cmd, cmd_args, ctx, &mut out)
        || crate::server::conn::shared::try_handle_function_in_txn(
            cmd,
            cmd_args,
            ctx,
            shutdown,
            func_registry,
            &mut out,
        )
        .await;

    if !handled {
        return Frame::Error(Bytes::from(format!(
            "ERR unknown command '{}' inside MULTI/EXEC",
            String::from_utf8_lossy(cmd)
        )));
    }
    // An intercept pushes exactly one reply. Taking the LAST is defensive
    // against one that pushes a preamble; taking `None` cannot happen when
    // `handled` is true, but erroring beats indexing.
    out.pop().unwrap_or_else(|| {
        Frame::Error(Bytes::from_static(
            b"ERR intercept produced no reply inside MULTI/EXEC",
        ))
    })
}

/// Replace every [`crate::server::conn::shared::TXN_INTERCEPT_PLACEHOLDER`] the
/// executor left in an `EXEC` array with the intercept's real reply (moon#639).
///
/// Walks the QUEUE, not the result array, so the mapping from slot to command
/// is the queue order the client saw — the executor pushes exactly one result
/// per queued command, which is the invariant `me12b` pins.
///
/// A `NullArray` result is an aborted transaction (`WATCH` conflict): nothing
/// in the body ran, so nothing in the intercepts may run either. Returning
/// early here is what makes that true, and it is the reason the intercepts run
/// after the body rather than before it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fill_txn_intercept_slots(
    result: &mut Frame,
    queue: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    codec: &mut crate::server::codec::RespCodec,
    switch_index: usize,
    func_registry: &Rc<RefCell<Option<crate::scripting::FunctionRegistry>>>,
) {
    let Frame::Array(results) = result else {
        return; // NullArray (aborted) or an error frame: no slots to fill.
    };
    if !queue
        .iter()
        .filter_map(crate::server::conn::util::extract_command)
        .any(|(c, _)| crate::server::conn::shared::is_txn_connection_intercept(c))
    {
        return; // the common case: no intercepts queued, nothing to walk.
    }
    let client_id = conn.client_id;
    // Cloned once per EXEC that actually has intercepts: `run_txn_connection_intercept`
    // takes `&mut conn`, so the address cannot be borrowed out of it at the
    // same time. Not a hot path.
    let peer_addr = conn.peer_addr.clone();
    for (i, frame) in queue.iter().enumerate() {
        if i >= results.len() {
            break;
        }
        let Some((c, a)) = crate::server::conn::util::extract_command(frame) else {
            continue;
        };
        if !crate::server::conn::shared::is_txn_connection_intercept(c) {
            continue;
        }
        results[i] = run_txn_connection_intercept(
            c,
            a,
            client_id,
            conn,
            ctx,
            &peer_addr,
            shutdown,
            codec,
            switch_index,
            func_registry,
        )
        .await;
    }
}
