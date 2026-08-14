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
                conn.protocol_version = new_proto;
                // Keep the wire codec in lockstep: the HELLO reply itself must
                // already be serialized in the negotiated protocol (RESP3 map).
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
    let response = crate::shard::slice::with_shard(|s| {
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
    });
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
    let response = crate::shard::slice::with_shard(|s| {
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
    });
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
    responses: &mut Vec<Frame>,
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
        let route = cs.read().route_slot(slot, was_asking);
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
        crate::command::identity::hello_role_and_mode(
            ctx.repl_state.as_ref(),
            ctx.cluster_state.is_some(),
        ),
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
    responses: &mut Vec<Frame>,
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
/// Returns `Some((..))` for every accepted PSYNC — the accept loop routes the
/// hijacked stream to the single-shard inline handler or, at num_shards > 1,
/// to the R2 multi-shard handler (`handle_psync_inline_multi_shard`).
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
    };
    let resp_frame = crate::shard::slice::with_shard_db(conn.selected_db, |db| {
        conn_cmd::info_with_facts(db, cmd_args, &keyspace, real_repl.as_deref(), &pubsub_facts)
    });
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
    responses: &mut Vec<Frame>,
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
pub(super) fn try_enforce_disk_full(cmd: &[u8], responses: &mut Vec<Frame>) -> bool {
    if metadata::is_write(cmd) && crate::shard::segment_stall::is_any_write_stall_active() {
        // dir-lost first: it also sets `is_write_paused`, and "diskfull"
        // would send the operator hunting free space instead of the missing
        // data directory (#366).
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
    responses: &mut Vec<Frame>,
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
    responses: &mut Vec<Frame>,
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
    func_registry: &Rc<RefCell<Option<crate::scripting::FunctionRegistry>>>,
    responses: &mut Vec<Frame>,
) -> bool {
    if conn.in_multi {
        return false;
    }
    if cmd.eq_ignore_ascii_case(b"FUNCTION") {
        crate::server::conn::core::ensure_function_registry(func_registry, ctx);
        let mut guard = func_registry.borrow_mut();
        #[allow(clippy::unwrap_used)]
        // ensure_function_registry guarantees Some
        let response =
            crate::command::functions::handle_function(guard.as_mut().unwrap(), cmd_args);
        drop(guard);
        responses.push(response);
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"FCALL") {
        crate::server::conn::core::ensure_function_registry(func_registry, ctx);
        let guard = func_registry.borrow();
        #[allow(clippy::unwrap_used)]
        // ensure_function_registry guarantees Some
        let reg = guard.as_ref().unwrap();
        let response = crate::shard::slice::with_shard(|s| {
            let db_count = s.databases.len();
            crate::command::functions::handle_fcall(
                reg,
                cmd_args,
                &mut s.databases[conn.selected_db],
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
            )
        });
        drop(guard);
        responses.push(response);
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"FCALL_RO") {
        crate::server::conn::core::ensure_function_registry(func_registry, ctx);
        let guard = func_registry.borrow();
        #[allow(clippy::unwrap_used)]
        // ensure_function_registry guarantees Some
        let reg = guard.as_ref().unwrap();
        let response = crate::shard::slice::with_shard(|s| {
            let db_count = s.databases.len();
            crate::command::functions::handle_fcall_ro(
                reg,
                cmd_args,
                &mut s.databases[conn.selected_db],
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                db_count,
            )
        });
        drop(guard);
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
    responses: &mut Vec<Frame>,
    local_leg_write_idxs: &mut Vec<usize>,
    codec: &mut crate::server::codec::RespCodec,
    write_buf: &mut bytes::BytesMut,
    read_buf: &mut bytes::BytesMut,
    stream: &mut S,
    shutdown: &CancellationToken,
    client_live: &std::sync::Arc<crate::client_registry::ClientLiveState>,
) -> BlockingResult {
    if !crate::server::conn::blocking::is_blocking_command(cmd) {
        return BlockingResult::NotBlocking;
    }

    // Inside MULTI: queue as non-blocking variant
    if conn.in_multi {
        let nb_frame = super::convert_blocking_to_nonblocking(cmd, cmd_args);
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

    // Encode blocking response directly
    codec.encode_frame(&blocking_response, write_buf);
    responses.clear();
    BlockingResult::Handled
}

/// `MONITOR` — attach this connection to the command feed.
///
/// Returns the reply to send, or `None` when the connection is ALREADY
/// attached: Redis answers a second `MONITOR` with nothing at all. Measured,
/// not inferred — it is silence, not an error.
///
/// The ACL check is not repeated here: this is reached only below
/// `try_enforce_acl`, and `MONITOR` carries the admin category in
/// `COMMAND_META`, so a non-admin user is refused by the general gate with the
/// same `NOPERM` text every other command uses.
pub(super) fn handle_monitor(
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    _ctx: &ConnectionContext,
    _peer_addr: &str,
) -> Option<Frame> {
    if !cmd_args.is_empty() {
        return Some(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'monitor' command",
        )));
    }
    if conn.monitor_attached {
        return None;
    }
    // Bounded, and deliberately not large. A monitor that cannot keep up has
    // this channel fill; the feed then drops the SINK, which closes the
    // channel and ends the connection. Contracted at freeze: silently skipping
    // lines would leave an operator unable to tell a quiet server from a lossy
    // feed, and blocking would let one slow reader stall every shard.
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(4096);
    if !crate::monitor::attach(conn.client_id, tx) {
        // Registry already knows this connection — treat as already attached
        // rather than double-registering, which would duplicate every line.
        conn.monitor_attached = true;
        return None;
    }
    conn.monitor_attached = true;
    conn.monitor_rx = Some(rx);
    Some(Frame::SimpleString(Bytes::from_static(b"OK")))
}
