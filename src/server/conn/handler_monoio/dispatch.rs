//! Connection-level command dispatchers for the monoio handler.
//!
//! AUTH gate, HELLO, ACL, CLIENT subcommands, CONFIG, BGSAVE/SAVE/LASTSAVE/BGREWRITEAOF,
//! REPLICAOF/REPLCONF, INFO, READONLY enforcement, CLUSTER, EVAL/EVALSHA/SCRIPT,
//! FUNCTION/FCALL/FCALL_RO, ACL permission gate, cross-shard KEYS/SCAN/DBSIZE,
//! multi-key commands, and blocking commands.
//!
//! Each helper returns `true` if the command was consumed (caller should `continue`).

use bytes::Bytes;
use ringbuf::traits::Producer;
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
use crate::shard::dispatch::ShardMessage;
use crate::shard::mesh::ChannelMesh;
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
    responses: &mut Vec<Frame>,
    auth_delay_ms: &mut u64,
    codec: &mut crate::server::codec::RespCodec,
) -> AuthGateResult {
    if conn.authenticated {
        return AuthGateResult::Authenticated;
    }
    match extract_command(frame) {
        Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"AUTH") => {
            let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &ctx.acl_table);
            if let Some(uname) = opt_user {
                conn.authenticated = true;
                conn.current_user = uname;
                conn.refresh_acl_cache(&ctx.acl_table);
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
            );
            if !matches!(&response, Frame::Error(_)) {
                conn.protocol_version = new_proto;
                // Keep the wire codec in lockstep: the HELLO reply itself must
                // already be serialized in the negotiated protocol (RESP3 map).
                codec.set_protocol_version(new_proto);
            }
            if let Some(name) = new_name {
                conn.client_name = Some(name);
            }
            if let Some(ref uname) = opt_user {
                conn.current_user = uname.clone();
                conn.refresh_acl_cache(&ctx.acl_table);
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
    responses: &mut Vec<Frame>,
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
pub(super) fn try_handle_evalsha(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"EVALSHA") {
        return false;
    }
    let response = if crate::shard::slice::is_initialized() {
        crate::shard::slice::with_shard(|s| {
            let db_count = s.databases.len();
            crate::scripting::handle_evalsha(
                &ctx.lua,
                &ctx.script_cache,
                cmd_args,
                &mut s.databases[conn.selected_db],
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
            )
        })
    } else {
        let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
        let db_count = ctx.shard_databases.db_count();
        let db = &mut guard;
        crate::scripting::handle_evalsha(
            &ctx.lua,
            &ctx.script_cache,
            cmd_args,
            db,
            ctx.shard_id,
            ctx.num_shards,
            conn.selected_db,
            db_count,
        )
    };
    responses.push(response);
    true
}

/// Handle the Redis EVAL command. Returns `true` if consumed.
///
/// `#[inline]`: see `try_handle_cluster` rationale — name check inlines so
/// non-matching commands cost only a length + byte compare.
#[inline]
pub(super) fn try_handle_eval(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"EVAL") {
        return false;
    }
    // Phase 2a: gate on is_initialized(); new path uses ShardSlice directly.
    let response = if crate::shard::slice::is_initialized() {
        crate::shard::slice::with_shard(|s| {
            let db_count = s.databases.len();
            let db = &mut s.databases[conn.selected_db];
            crate::scripting::handle_eval(
                &ctx.lua,
                &ctx.script_cache,
                cmd_args,
                db,
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
            )
        })
    } else {
        let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
        let db_count = ctx.shard_databases.db_count();
        let db = &mut guard;
        crate::scripting::handle_eval(
            &ctx.lua,
            &ctx.script_cache,
            cmd_args,
            db,
            ctx.shard_id,
            ctx.num_shards,
            conn.selected_db,
            db_count,
        )
    };
    responses.push(response);
    true
}

/// Handle SCRIPT subcommands (LOAD, EXISTS, FLUSH). Returns `true` if consumed.
///
/// `#[inline]`: see `try_handle_cluster` rationale.
#[inline]
pub(super) fn try_handle_script(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"SCRIPT") {
        return false;
    }
    let (response, fanout) =
        crate::scripting::handle_script_subcommand(&ctx.script_cache, cmd_args);
    if let Some((sha1, script_bytes)) = fanout {
        let mut producers = ctx.dispatch_tx.borrow_mut();
        for target in 0..ctx.num_shards {
            if target == ctx.shard_id {
                continue;
            }
            let idx = ChannelMesh::target_index(ctx.shard_id, target);
            let msg = ShardMessage::ScriptLoad {
                sha1: sha1.clone(),
                script: script_bytes.clone(),
            };
            if producers[idx].try_push(msg).is_ok() {
                ctx.spsc_notifiers[target].notify_one();
            }
        }
        drop(producers);
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
    responses: &mut Vec<Frame>,
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
        #[allow(clippy::unwrap_used)]
        // std RwLock: poison = prior panic = unrecoverable
        let route = cs.read().unwrap().route_slot(slot, was_asking);
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
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"AUTH") {
        return false;
    }
    let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &ctx.acl_table);
    if let Some(uname) = opt_user {
        conn.current_user = uname;
        conn.refresh_acl_cache(&ctx.acl_table);
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
    responses: &mut Vec<Frame>,
    codec: &mut crate::server::codec::RespCodec,
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
    );
    if !matches!(&response, Frame::Error(_)) {
        conn.protocol_version = new_proto;
        // Keep the wire codec in lockstep: the HELLO reply itself must
        // already be serialized in the negotiated protocol (RESP3 map).
        codec.set_protocol_version(new_proto);
    }
    if let Some(name) = new_name {
        conn.client_name = Some(name);
    }
    if let Some(ref uname) = opt_user {
        conn.current_user = uname.clone();
        conn.refresh_acl_cache(&ctx.acl_table);
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
    responses: &mut Vec<Frame>,
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
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CONFIG") {
        return false;
    }
    responses.push(handle_config(cmd_args, &ctx.runtime_config, &ctx.config));
    true
}

/// Handle REPLICAOF / SLAVEOF. Returns `true` if consumed.
#[inline]
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
                    monoio::spawn(crate::replication::replica::run_replica_task(cfg));
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

/// Handle REPLCONF command. Returns `true` if consumed.
///
/// Side effect: when REPLCONF is observed on a master, eagerly allocate the
/// per-shard replication backlogs so that subsequent writes between now and
/// PSYNC arrival are captured for partial resync. This fixes the
/// chicken-and-egg gap where the original code only allocated on
/// `RegisterReplica` (after PSYNC), causing the master to buffer zero bytes
/// during the handshake window.
#[inline]
pub(super) fn try_handle_replconf(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"REPLCONF") {
        return false;
    }
    if let Some(ref rs) = ctx.repl_state {
        if let Ok(g) = rs.read() {
            if matches!(g.role, crate::replication::state::ReplicationRole::Master) {
                g.ensure_backlogs_allocated(1024 * 1024);
            }
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
    responses: &mut Vec<Frame>,
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
/// Returns `Some((..))` only when num_shards == 1 (the supported topology).
/// For multi-shard topologies, pushes a clear error and returns `None`
/// (consumed via `responses`); the caller treats it like any other command
/// reply and continues — the replica will see the error and give up.
pub(super) fn try_handle_psync(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
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
    if ctx.num_shards != 1 {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR PSYNC across multiple shards is not yet supported (use --shards 1 on the master)",
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
        let g = rs.read().ok();
        let is_master = g
            .as_ref()
            .map(|g| matches!(g.role, crate::replication::state::ReplicationRole::Master))
            .unwrap_or(false);
        if !is_master {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR PSYNC is only valid on a master",
            )));
            return None;
        }
        if let Some(g) = g {
            g.ensure_backlogs_allocated(1024 * 1024);
        }
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
    // Phase 2a: gate on is_initialized(); new path uses ShardSlice directly.
    let response_text = if crate::shard::slice::is_initialized() {
        crate::shard::slice::with_shard_db(conn.selected_db, |db| {
            let resp_frame = conn_cmd::info_readonly(db, cmd_args);
            match resp_frame {
                Frame::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                _ => String::new(),
            }
        })
    } else {
        let guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
        let text = {
            let resp_frame = conn_cmd::info_readonly(&guard, cmd_args);
            match resp_frame {
                Frame::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                _ => String::new(),
            }
        };
        drop(guard);
        text
    };
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

/// MA12 + MA1: Refuse write commands when any write stall is active.
///
/// Returns `true` if the command was blocked (caller should `continue`).
///
/// Stall sources (OR-merged):
/// - MA12 disk-pressure monitor (`is_write_paused`) — set every 5s.
/// - MA1 segment-backlog stall (`is_segment_stall_active`) — set every 1s.
///
/// Hot path: two `Atomic::load(Relaxed)` — no allocation, no lock.
/// Read-only commands pass through unaffected. Background compaction is exempt.
#[inline]
pub(super) fn try_enforce_disk_full(cmd: &[u8], responses: &mut Vec<Frame>) -> bool {
    if metadata::is_write(cmd) && crate::shard::segment_stall::is_any_write_stall_active() {
        let msg: &'static [u8] = if crate::shard::disk_monitor::is_write_paused() {
            b"MOONERR diskfull: writes paused until free space recovers"
        } else {
            b"MOONERR busy: compaction backlog; too many unflushed immutable segments"
        };
        responses.push(Frame::Error(Bytes::from_static(msg)));
        return true;
    }
    false
}

/// Handle CLIENT subcommands that are checked BEFORE the ACL gate:
/// ID, SETNAME, GETNAME, TRACKING.
/// Returns `true` if a subcommand was consumed (caller should `continue`).
/// Returns `false` for admin subcommands (LIST, INFO, KILL, PAUSE, UNPAUSE, NO-EVICT, NO-TOUCH)
/// which must pass through the ACL gate first.
#[inline]
pub(super) fn try_handle_client_early(
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
            // Admin CLIENT subcommands (LIST, INFO, KILL, PAUSE, UNPAUSE,
            // NO-EVICT, NO-TOUCH) fall through to the ACL gate below.
        }
    }
    // Fall through -- admin subcommands handled after ACL check.
    false
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
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CLIENT") {
        return false;
    }
    if let Some(sub) = cmd_args.first() {
        if let Some(sub_bytes) = extract_bytes(sub) {
            if sub_bytes.eq_ignore_ascii_case(b"LIST") {
                crate::client_registry::update(client_id, |e| {
                    e.live.touch(
                        conn.selected_db,
                        crate::client_registry::ClientFlags {
                            subscriber: conn.subscription_count > 0,
                            in_multi: conn.in_multi,
                            blocked: false,
                        },
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
                            blocked: false,
                        },
                    );
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
            // Unknown CLIENT subcommand
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

/// Handle persistence commands (BGSAVE, SAVE, LASTSAVE, BGREWRITEAOF).
/// Returns `true` if consumed.
#[inline]
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

/// Handle SWAPDB — atomically exchange two databases across all shards.
///
/// Validates arguments, enforces the BGREWRITEAOF guard, handles the same-index
/// no-op, then delegates to `coordinate_swapdb` for multi-shard broadcast.
/// Returns `true` if consumed (caller should `continue`).
pub(super) async fn try_handle_swapdb(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &crate::server::conn::core::ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
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
    responses: &mut Vec<Frame>,
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
#[inline]
pub(super) fn try_handle_functions(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    func_registry: &Rc<RefCell<crate::scripting::FunctionRegistry>>,
    responses: &mut Vec<Frame>,
) -> bool {
    if conn.in_multi {
        return false;
    }
    if cmd.eq_ignore_ascii_case(b"FUNCTION") {
        let response =
            crate::command::functions::handle_function(&mut func_registry.borrow_mut(), cmd_args);
        responses.push(response);
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"FCALL") {
        // Phase 2a: gate on is_initialized(); new path uses ShardSlice directly.
        let response = if crate::shard::slice::is_initialized() {
            crate::shard::slice::with_shard(|s| {
                let db_count = s.databases.len();
                crate::command::functions::handle_fcall(
                    &func_registry.borrow(),
                    cmd_args,
                    &mut s.databases[conn.selected_db],
                    ctx.shard_id,
                    ctx.num_shards,
                    conn.selected_db,
                    db_count,
                )
            })
        } else {
            let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
            let db_count = ctx.shard_databases.db_count();
            crate::command::functions::handle_fcall(
                &func_registry.borrow(),
                cmd_args,
                &mut guard,
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
            )
        };
        responses.push(response);
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"FCALL_RO") {
        // Phase 2a: gate on is_initialized(); new path uses ShardSlice directly.
        let response = if crate::shard::slice::is_initialized() {
            crate::shard::slice::with_shard(|s| {
                let db_count = s.databases.len();
                crate::command::functions::handle_fcall_ro(
                    &func_registry.borrow(),
                    cmd_args,
                    &mut s.databases[conn.selected_db],
                    ctx.shard_id,
                    ctx.num_shards,
                    conn.selected_db,
                    db_count,
                )
            })
        } else {
            let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
            let db_count = ctx.shard_databases.db_count();
            crate::command::functions::handle_fcall_ro(
                &func_registry.borrow(),
                cmd_args,
                &mut guard,
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
            )
        };
        responses.push(response);
        return true;
    }
    false
}

/// Handle cross-shard aggregation commands: KEYS, SCAN, DBSIZE, and multi-key commands.
/// Returns `true` if consumed.
pub(super) async fn try_handle_cross_shard_commands(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
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
            &(), // monoio: coordinator uses oneshot, not response_pool
        )
        .await;
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
}

/// Handle blocking commands (BLPOP, BRPOP, BLMOVE, etc.).
pub(super) async fn try_handle_blocking<S: monoio::io::AsyncWriteRent>(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
    codec: &mut crate::server::codec::RespCodec,
    write_buf: &mut bytes::BytesMut,
    stream: &mut S,
    shutdown: &CancellationToken,
) -> BlockingResult {
    if !cmd.eq_ignore_ascii_case(b"BLPOP")
        && !cmd.eq_ignore_ascii_case(b"BRPOP")
        && !cmd.eq_ignore_ascii_case(b"BLMOVE")
        && !cmd.eq_ignore_ascii_case(b"BZPOPMIN")
        && !cmd.eq_ignore_ascii_case(b"BZPOPMAX")
        && !cmd.eq_ignore_ascii_case(b"BLMPOP")
        && !cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
        && !cmd.eq_ignore_ascii_case(b"BZMPOP")
    {
        return BlockingResult::NotBlocking;
    }

    // Inside MULTI: queue as non-blocking variant
    if conn.in_multi {
        let nb_frame = super::convert_blocking_to_nonblocking(cmd, cmd_args);
        conn.command_queue.push(nb_frame);
        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
        return BlockingResult::Queued;
    }

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

    let blocking_response = handle_blocking_command_monoio(
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
    )
    .await;

    // Encode blocking response directly
    codec.encode_frame(&blocking_response, write_buf);
    responses.clear();
    BlockingResult::Handled
}
