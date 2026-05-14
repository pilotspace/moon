// Note: some imports/variables may be conditionally used across feature flags
//! Sharded tokio connection handlers.
//!
//! Extracted from `server/connection.rs` (Plan 48-02).
//! Contains `handle_connection_sharded` (thin wrapper) and
//! `handle_connection_sharded_inner` (generic inner handler).

use crate::runtime::TcpStream;
use crate::runtime::cancel::CancellationToken;
use bumpalo::Bump;
use bytes::{Bytes, BytesMut};
use ringbuf::traits::Producer;
use std::collections::HashMap;

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::storage::eviction::try_evict_if_needed;
use crate::workspace::{strip_workspace_prefix_from_response, workspace_rewrite_args};

use super::affinity::MigratedConnectionState;
use crate::server::response_slot::ResponseSlotPool;

mod dispatch;
mod ft;
mod pubsub;
mod read;
mod txn;
mod write;

/// Result of `handle_connection_sharded_inner` execution.
///
/// The generic inner handler cannot perform FD extraction (requires concrete stream type).
/// When migration is triggered, it returns `MigrateConnection` so the concrete caller
/// can extract the raw FD and send the migration message via SPSC.
pub enum HandlerResult {
    /// Normal connection close (QUIT, EOF, error, shutdown).
    Done,
    /// AffinityTracker detected a dominant remote shard. The caller should:
    /// 1. Extract the raw FD from the concrete stream (into_std + into_raw_fd)
    /// 2. Send ShardMessage::MigrateConnection via SPSC to `target_shard`
    /// 3. Drop the handler (connection ownership transferred)
    MigrateConnection {
        state: MigratedConnectionState,
        target_shard: usize,
    },
}

use super::{
    apply_resp3_conversion, convert_blocking_to_nonblocking, execute_transaction_sharded,
    extract_bytes, extract_command, extract_primary_key, handle_blocking_command, handle_config,
    is_multi_key_command, unpropagate_subscription,
};

/// Handle a single client connection on a sharded (thread-per-core) runtime.
///
/// Runs within a shard's single-threaded Tokio runtime. Has direct mutable access
/// to the shard's databases via `Arc<ShardDatabases>` (thread-safe: parking_lot RwLock
/// single-threaded scheduling means no concurrent borrows).
///
/// Routing logic:
/// - **Keyless commands** (PING, ECHO, SELECT, etc.): execute locally, zero overhead.
/// - **Single-key, local shard**: execute directly on borrowed database -- ZERO cross-shard overhead.
/// - **Single-key, remote shard**: dispatch via SPSC `ShardMessage::Execute`, await oneshot reply.
/// - **Multi-key commands** (MGET, MSET, multi-DEL): delegate to VLL coordinator.
///
/// Connection-level commands (AUTH, SUBSCRIBE, MULTI/EXEC) are handled at the
/// connection level same as the non-sharded handler.
#[tracing::instrument(skip_all, level = "debug")]
pub(crate) async fn handle_connection_sharded(
    mut stream: TcpStream,
    ctx: &super::core::ConnectionContext,
    shutdown: CancellationToken,
    client_id: u64,
) {
    let maxclients = ctx.runtime_config.read().maxclients;
    if !crate::admin::metrics_setup::try_accept_connection(maxclients) {
        use tokio::io::AsyncWriteExt;
        let _ = stream
            .write_all(b"-ERR max number of clients reached\r\n")
            .await;
        return;
    }
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let result = handle_connection_sharded_inner(
        stream,
        peer_addr,
        ctx,
        shutdown,
        client_id,
        true, // can_migrate: plain TCP supports FD extraction
        BytesMut::new(),
        None, // fresh connection, no migrated state
    )
    .await;

    // Handle migration result: extract FD from the returned stream and send via SPSC
    if let (
        HandlerResult::MigrateConnection {
            state,
            target_shard,
        },
        Some(stream),
    ) = (result.0, result.1)
    {
        use std::os::unix::io::IntoRawFd;
        match stream.into_std() {
            Ok(std_stream) => {
                let raw_fd = std_stream.into_raw_fd();
                let msg = ShardMessage::MigrateConnection(Box::new(
                    crate::shard::dispatch::MigrateConnectionPayload { fd: raw_fd, state },
                ));
                let target_idx = ChannelMesh::target_index(ctx.shard_id, target_shard);
                let push_result = {
                    let mut producers = ctx.dispatch_tx.borrow_mut();
                    producers[target_idx].try_push(msg)
                };
                match push_result {
                    Ok(()) => {
                        ctx.spsc_notifiers[target_shard].notify_one();
                        tracing::info!(
                            "Shard {}: migrated connection {} to shard {}",
                            ctx.shard_id,
                            client_id,
                            target_shard
                        );
                    }
                    Err(returned_msg) => {
                        // SPSC full — retry with yield before giving up.
                        let mut pending = Some(returned_msg);
                        for _ in 0..8 {
                            tokio::task::yield_now().await;
                            #[allow(clippy::unwrap_used)]
                            // pending is always re-filled on retry via Err(returned_msg)
                            let msg = pending.take().unwrap();
                            let push_result = {
                                let mut producers = ctx.dispatch_tx.borrow_mut();
                                producers[target_idx].try_push(msg)
                            };
                            match push_result {
                                Ok(()) => {
                                    ctx.spsc_notifiers[target_shard].notify_one();
                                    tracing::info!(
                                        "Shard {}: migrated connection {} to shard {} (after retry)",
                                        ctx.shard_id,
                                        client_id,
                                        target_shard
                                    );
                                    break;
                                }
                                Err(msg) => pending = Some(msg),
                            }
                        }
                        if let Some(ShardMessage::MigrateConnection(payload)) = pending {
                            use std::os::unix::io::FromRawFd;
                            // SAFETY: fd is a valid, uniquely-owned file descriptor obtained
                            // from TcpStream::into_raw_fd() above. OwnedFd closes it on drop.
                            drop(unsafe { std::os::unix::io::OwnedFd::from_raw_fd(payload.fd) });
                        }
                        tracing::warn!(
                            "Shard {}: migration SPSC full, connection {} lost",
                            ctx.shard_id,
                            client_id
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Shard {}: migration into_std failed: {}", ctx.shard_id, e);
                // Stream consumed by into_std attempt, connection lost either way
            }
        }
    } else {
        // Only decrement connected_clients when the connection is actually closing,
        // not when migrating to another shard (the connection stays alive).
        crate::admin::metrics_setup::record_connection_closed();
    }
}

/// Generic inner handler for sharded connections (Tokio runtime).
///
/// Works with any stream implementing `AsyncRead + AsyncWrite + Unpin`,
/// enabling both plain TCP (`TcpStream`) and TLS (`tokio_rustls::server::TlsStream<TcpStream>`).
///
/// Returns `(HandlerResult, Option<S>)`: the stream is returned when migration is triggered
/// so the concrete caller can extract the raw FD. `can_migrate` controls whether the
/// AffinityTracker is active (set to `false` for TLS connections).
pub(crate) async fn handle_connection_sharded_inner<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
>(
    stream: S,
    peer_addr: String,
    ctx: &super::core::ConnectionContext,
    shutdown: CancellationToken,
    client_id: u64,
    can_migrate: bool,
    initial_read_buf: BytesMut,
    migrated_state: Option<&MigratedConnectionState>,
) -> (HandlerResult, Option<S>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Direct buffer I/O: bypass Framed/codec for the hot path.
    let mut stream = stream;
    let mut read_buf = if initial_read_buf.is_empty() {
        BytesMut::with_capacity(8192)
    } else {
        // Migration buffer: leftover bytes from the source connection.
        let mut buf = initial_read_buf;
        buf.reserve(8192);
        buf
    };
    let mut write_buf = BytesMut::with_capacity(8192);
    let parse_config = crate::protocol::ParseConfig::default();
    let mut conn = super::core::ConnectionState::new(
        client_id,
        peer_addr.clone(),
        &ctx.requirepass,
        ctx.shard_id,
        ctx.num_shards,
        can_migrate,
        ctx.runtime_config.read().acllog_max_len,
        migrated_state,
    );
    conn.refresh_acl_cache(&ctx.acl_table);

    // Register in global client registry for CLIENT LIST/INFO/KILL.
    // RegistryGuard ensures deregister on all exit paths (including early returns).
    crate::client_registry::register(
        client_id,
        peer_addr.clone(),
        conn.current_user.clone(),
        ctx.shard_id,
    );
    struct RegistryGuard(u64);
    impl Drop for RegistryGuard {
        fn drop(&mut self) {
            crate::client_registry::deregister(self.0);
        }
    }
    let _registry_guard = RegistryGuard(client_id);

    // Functions API registry (per-shard, lazy init) — kept as local because Rc<RefCell<>> is !Send
    let func_registry = std::rc::Rc::new(std::cell::RefCell::new(
        crate::scripting::FunctionRegistry::new(),
    ));

    // Per-connection arena for batch processing temporaries.
    // 4KB initial capacity, grows on demand (rarely exceeds 16KB per batch).
    let mut arena = Bump::with_capacity(4096);

    // Pre-allocated response slots for zero-allocation cross-shard dispatch.
    let response_pool = ResponseSlotPool::new(ctx.num_shards, ctx.shard_id);

    // Client idle timeout: 0 = disabled (read once, avoid lock on hot path)
    let idle_timeout_secs = ctx.runtime_config.read().timeout;
    let idle_timeout = if idle_timeout_secs > 0 {
        Some(std::time::Duration::from_secs(idle_timeout_secs))
    } else {
        None
    };

    let mut break_outer = false;
    loop {
        // Check if CLIENT KILL targeted this connection
        if crate::client_registry::is_killed(client_id) {
            break;
        }

        // --- Subscriber mode: bidirectional select on client commands + published messages ---
        if conn.subscription_count > 0 {
            match pubsub::run_subscriber_step(
                &mut stream,
                &mut read_buf,
                &mut write_buf,
                &parse_config,
                &mut conn,
                ctx,
                &peer_addr,
                &shutdown,
            )
            .await
            {
                pubsub::SubscriberAction::Continue => {
                    continue;
                }
                pubsub::SubscriberAction::BreakOuter => {
                    break;
                }
                pubsub::SubscriberAction::EarlyReturn => {
                    return (HandlerResult::Done, None);
                }
            }
        }
        tokio::select! {
            result = async {
                if let Some(dur) = idle_timeout {
                    match tokio::time::timeout(dur, stream.read_buf(&mut read_buf)).await {
                        Ok(r) => r,
                        Err(_) => Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "idle timeout")),
                    }
                } else {
                    stream.read_buf(&mut read_buf).await
                }
            } => {
                match result {
                    Ok(0) => break, // connection closed
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                        tracing::debug!("Connection {} idle timeout ({}s)", client_id, idle_timeout_secs);
                        break;
                    }
                    Err(_) => break,
                }

                // Parse all complete frames from buffer
                let mut batch: Vec<Frame> = Vec::with_capacity(64);
                const MAX_BATCH: usize = 1024;
                loop {
                    match crate::protocol::parse(&mut read_buf, &parse_config) {
                        Ok(Some(frame)) => {
                            batch.push(frame);
                            if batch.len() >= MAX_BATCH { break; }
                        }
                        Ok(None) => break,
                        Err(crate::protocol::ParseError::Incomplete) => break,
                        Err(_) => { break_outer = true; break; }
                    }
                }
                if break_outer { break; }
                if batch.is_empty() { continue; }

                // CLIENT PAUSE: delay processing if server is paused
                // Check with is_write=true (conservative — pauses all batches in ALL mode)
                crate::client_pause::expire_if_needed();
                if let Some(remaining) = crate::client_pause::check_pause(true) {
                    tokio::time::sleep(remaining).await;
                }

                let mut responses: Vec<Frame> = Vec::with_capacity(batch.len());
                let mut should_quit = false;
                let mut remote_groups: HashMap<usize, Vec<(usize, std::sync::Arc<Frame>, Option<Bytes>, Bytes, usize)>> = HashMap::with_capacity(ctx.num_shards);
                // Accumulate cross-shard PUBLISH pairs per target shard for batch dispatch
                // Key: target shard ID -> Vec of (response_index, channel, message)
                let mut publish_batches: HashMap<usize, Vec<(usize, Bytes, Bytes)>> = HashMap::new();

                // Track if AUTH rate limiting delay is needed (applied after batch response)
                let mut auth_delay_ms: u64 = 0;

                // Per-batch dispatch-path accumulators — flushed once at end of
                // batch so we pay one global atomic per path instead of N.
                let mut local_dispatches: u32 = 0;
                // Phase 3.1: cross_read_fast_dispatches counter removed
                // alongside the SHARED-READ FAST PATH; all cross-shard reads
                // are now counted under cross_spsc_dispatches.
                let mut cross_spsc_dispatches: u32 = 0;

                for frame in batch {
                    // --- AUTH gate ---
                    if !conn.authenticated {
                        match extract_command(&frame) {
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
                                        auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
                                    }
                                    conn.acl_log.push(crate::acl::AclLogEntry {
                                        reason: "auth".to_string(),
                                        object: "AUTH".to_string(),
                                        username: conn.current_user.clone(),
                                        client_addr: peer_addr.clone(),
                                        timestamp_ms: std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_millis() as u64,
                                    });
                                }
                                responses.push(response);
                                continue;
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
                                }
                                if let Some(name) = new_name {
                                    conn.client_name = Some(name);
                                }
                                if let Some(ref uname) = opt_user {
                                    conn.current_user = uname.clone();
                                    conn.refresh_acl_cache(&ctx.acl_table);
                                }
                                // HELLO AUTH rate limiting (same as AUTH gate)
                                if matches!(&response, Frame::Error(_)) {
                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                        auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
                                    }
                                } else if opt_user.is_some() {
                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                        crate::auth_ratelimit::record_success(addr.ip());
                                    }
                                }
                                responses.push(response);
                                continue;
                            }
                            Some((cmd, _)) if cmd.eq_ignore_ascii_case(b"QUIT") => {
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                should_quit = true;
                                break;
                            }
                            _ => {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"NOAUTH Authentication required.")
                                ));
                                continue;
                            }
                        }
                    }

                    let (cmd, cmd_args) = match extract_command(&frame) {
                        Some(pair) => pair,
                        None => {
                            responses.push(Frame::Error(Bytes::from_static(
                                b"ERR invalid command format",
                            )));
                            continue;
                        }
                    };

                    // --- QUIT ---
                    if cmd.eq_ignore_ascii_case(b"QUIT") {
                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        should_quit = true;
                        break;
                    }

                    // --- ASKING ---
                    if cmd.eq_ignore_ascii_case(b"ASKING") {
                        conn.asking = true;
                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        continue;
                    }

                    // --- CLUSTER subcommands ---
                    if cmd.eq_ignore_ascii_case(b"CLUSTER") {
                        if let Some(ref cs) = ctx.cluster_state {
                            #[allow(clippy::unwrap_used)] // Fallback "127.0.0.1:6379" is a valid literal
                            let self_addr: std::net::SocketAddr =
                                format!("127.0.0.1:{}", ctx.config_port)
                                    .parse()
                                    .unwrap_or_else(|_| "127.0.0.1:6379".parse().unwrap());
                            let resp = crate::cluster::command::handle_cluster_command(
                                cmd_args, cs, self_addr,
                            );
                            responses.push(resp);
                        } else {
                            responses.push(Frame::Error(Bytes::from_static(
                                b"ERR This instance has cluster support disabled",
                            )));
                        }
                        continue;
                    }

                    // --- Lua scripting: EVAL / EVALSHA ---
                    if cmd.eq_ignore_ascii_case(b"EVAL") || cmd.eq_ignore_ascii_case(b"EVALSHA") {
                        let db_count = ctx.shard_databases.db_count();
                        // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                        let response = if crate::shard::slice::is_initialized() {
                            crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                                if cmd.eq_ignore_ascii_case(b"EVAL") {
                                    crate::scripting::handle_eval(
                                        &ctx.lua, &ctx.script_cache, cmd_args, db,
                                        ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                    )
                                } else {
                                    crate::scripting::handle_evalsha(
                                        &ctx.lua, &ctx.script_cache, cmd_args, db,
                                        ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                    )
                                }
                            })
                        } else {
                            let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                            if cmd.eq_ignore_ascii_case(b"EVAL") {
                                crate::scripting::handle_eval(
                                    &ctx.lua, &ctx.script_cache, cmd_args, &mut guard,
                                    ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                )
                            } else {
                                crate::scripting::handle_evalsha(
                                    &ctx.lua, &ctx.script_cache, cmd_args, &mut guard,
                                    ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                )
                            }
                        };
                        responses.push(response);
                        continue;
                    }

                    // --- SCRIPT subcommands ---
                    if cmd.eq_ignore_ascii_case(b"SCRIPT") {
                        let (response, fanout) = crate::scripting::handle_script_subcommand(&ctx.script_cache, cmd_args);
                        if let Some((sha1, script_bytes)) = fanout {
                            let mut producers = ctx.dispatch_tx.borrow_mut();
                            for target in 0..ctx.num_shards {
                                if target == ctx.shard_id { continue; }
                                let idx = ChannelMesh::target_index(ctx.shard_id, target);
                                let msg = ShardMessage::ScriptLoad { sha1: sha1.clone(), script: script_bytes.clone() };
                                if producers[idx].try_push(msg).is_ok() {
                                    ctx.spsc_notifiers[target].notify_one();
                                }
                            }
                            drop(producers);
                        }
                        responses.push(response);
                        continue;
                    }

                    // --- Cluster slot routing (pre-dispatch) ---
                    if crate::cluster::cluster_enabled() {
                        if let Some(ref cs) = ctx.cluster_state {
                            let was_asking = conn.asking;
                            conn.asking = false;
                            let maybe_key = extract_primary_key(cmd, cmd_args);
                            if let Some(key) = maybe_key {
                                let slot = crate::cluster::slots::slot_for_key(key);
                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                let route = cs.read().unwrap().route_slot(slot, was_asking);
                                match route {
                                    crate::cluster::SlotRoute::Local => {}
                                    other => {
                                        responses.push(other.into_error_frame(slot));
                                        continue;
                                    }
                                }
                                if is_multi_key_command(cmd, cmd_args) {
                                    let first_slot = slot;
                                    let mut cross_slot = false;
                                    for arg in cmd_args.iter().skip(1) {
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
                                        continue;
                                    }
                                }
                            }
                        }
                    }

                    // --- AUTH (already conn.authenticated) ---
                    if cmd.eq_ignore_ascii_case(b"AUTH") {
                        let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &ctx.acl_table);
                        if let Some(uname) = opt_user {
                            conn.current_user = uname;
                            conn.refresh_acl_cache(&ctx.acl_table);
                            if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                crate::auth_ratelimit::record_success(addr.ip());
                            }
                        } else if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                            auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
                        }
                        responses.push(response);
                        continue;
                    }

                    // --- HELLO ---
                    if cmd.eq_ignore_ascii_case(b"HELLO") {
                        let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                            cmd_args, conn.protocol_version, client_id, &ctx.acl_table, &mut conn.authenticated,
                        );
                        if !matches!(&response, Frame::Error(_)) { conn.protocol_version = new_proto; }
                        if let Some(name) = new_name { conn.client_name = Some(name); }
                        if let Some(ref uname) = opt_user {
                            conn.current_user = uname.clone();
                            conn.refresh_acl_cache(&ctx.acl_table);
                        }
                        if matches!(&response, Frame::Error(_)) {
                            if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
                            }
                        } else if opt_user.is_some() {
                            if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                crate::auth_ratelimit::record_success(addr.ip());
                            }
                        }
                        responses.push(response);
                        continue;
                    }

                    // --- ACL ---
                    if cmd.eq_ignore_ascii_case(b"ACL") {
                        let response = crate::command::acl::handle_acl(
                            cmd_args, &ctx.acl_table, &mut conn.acl_log, &conn.current_user, &peer_addr, &ctx.runtime_config,
                        );
                        responses.push(response);
                        continue;
                    }

                    // === CLIENT PAUSE check ===
                    // Extract pause info with short lock hold, then sleep outside lock scope
                    let pause_wait_ms = {
                        let rt = ctx.runtime_config.read();
                        let deadline = rt.client_pause_deadline_ms;
                        if deadline > 0 {
                            let now = crate::storage::entry::current_time_ms();
                            if now < deadline {
                                let should_pause = if rt.client_pause_write_only {
                                    crate::command::metadata::is_write(cmd)
                                } else {
                                    true
                                };
                                if should_pause { deadline.saturating_sub(now) } else { 0 }
                            } else { 0 }
                        } else { 0 }
                    };
                    if pause_wait_ms > 0 {
                        // Poll in 50ms intervals so CLIENT UNPAUSE takes effect quickly
                        let mut remaining = pause_wait_ms;
                        while remaining > 0 {
                            let chunk = remaining.min(50);
                            #[cfg(feature = "runtime-tokio")]
                            {
                                tokio::time::sleep(std::time::Duration::from_millis(chunk)).await;
                            }
                            #[cfg(feature = "runtime-monoio")]
                            {
                                monoio::time::sleep(std::time::Duration::from_millis(chunk)).await;
                            }
                            remaining = remaining.saturating_sub(chunk);
                            // Re-check if UNPAUSE was called
                            let still_paused = {
                                let rt = ctx.runtime_config.read();
                                rt.client_pause_deadline_ms > 0
                                    && crate::storage::entry::current_time_ms() < rt.client_pause_deadline_ms
                            };
                            if !still_paused {
                                break;
                            }
                        }
                    }

                    // === ACL permission check ===
                    // Must run before any command-specific handlers (CONFIG, REPLICAOF, etc.)
                    // so that low-privilege users cannot reach admin commands.
                    // Fast path: skip RwLock + HashMap for unrestricted users
                    // with a fresh cache.  Stale caches (after ACL SETUSER /
                    // DELUSER / LOAD) fall through to the full check.
                    if !conn.acl_skip_allowed() {
                        #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                        let acl_guard = ctx.acl_table.read().unwrap();
                        if let Some(deny_reason) = acl_guard.check_command_permission(&conn.current_user, cmd, cmd_args) {
                            drop(acl_guard);
                            conn.acl_log.push(crate::acl::AclLogEntry {
                                reason: "command".to_string(),
                                object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                username: conn.current_user.clone(),
                                client_addr: peer_addr.clone(),
                                timestamp_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                            });
                            responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                            continue;
                        }
                        let is_write_for_acl = metadata::is_write(cmd);
                        if let Some(deny_reason) = acl_guard.check_key_permission(&conn.current_user, cmd, cmd_args, is_write_for_acl) {
                            drop(acl_guard);
                            conn.acl_log.push(crate::acl::AclLogEntry {
                                reason: "command".to_string(),
                                object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                username: conn.current_user.clone(),
                                client_addr: peer_addr.clone(),
                                timestamp_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                            });
                            responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                            continue;
                        }
                    }

                    // --- Functions API: FUNCTION/FCALL/FCALL_RO ---
                    // Placed AFTER ACL check. Respects MULTI queue — if conn.in_multi,
                    // fall through to the MULTI queue gate instead of executing.
                    if !conn.in_multi {
                        if cmd.eq_ignore_ascii_case(b"FUNCTION") {
                            let response = crate::command::functions::handle_function(
                                &mut func_registry.borrow_mut(), cmd_args,
                            );
                            responses.push(response);
                            continue;
                        }
                        if cmd.eq_ignore_ascii_case(b"FCALL") {
                            let db_count = ctx.shard_databases.db_count();
                            // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                            let response = if crate::shard::slice::is_initialized() {
                                crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                                    crate::command::functions::handle_fcall(
                                        &func_registry.borrow(), cmd_args, db,
                                        ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                    )
                                })
                            } else {
                                let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                                crate::command::functions::handle_fcall(
                                    &func_registry.borrow(), cmd_args, &mut guard,
                                    ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                )
                            };
                            responses.push(response);
                            continue;
                        }
                        if cmd.eq_ignore_ascii_case(b"FCALL_RO") {
                            let db_count = ctx.shard_databases.db_count();
                            // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                            let response = if crate::shard::slice::is_initialized() {
                                crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                                    crate::command::functions::handle_fcall_ro(
                                        &func_registry.borrow(), cmd_args, db,
                                        ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                    )
                                })
                            } else {
                                let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                                crate::command::functions::handle_fcall_ro(
                                    &func_registry.borrow(), cmd_args, &mut guard,
                                    ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                )
                            };
                            responses.push(response);
                            continue;
                        }
                    }

                    // --- CONFIG ---
                    if dispatch::try_handle_config(cmd, cmd_args, ctx, &mut responses) {
                        continue;
                    }

                    // --- SLOWLOG ---
                    if dispatch::try_handle_slowlog(cmd, cmd_args, &mut responses) {
                        continue;
                    }

                    // --- REPLICAOF / SLAVEOF ---
                    if dispatch::try_handle_replicaof(cmd, cmd_args, ctx, &mut responses) {
                        continue;
                    }

                    // --- REPLCONF ---
                    if dispatch::try_handle_replconf(cmd, cmd_args, &mut responses) {
                        continue;
                    }

                    // --- CDC.READ ---
                    if dispatch::try_handle_cdc_read(cmd, cmd_args, &mut responses) {
                        continue;
                    }

                    // --- INFO ---
                    if dispatch::try_handle_info(cmd, cmd_args, &conn, ctx, &mut responses) {
                        continue;
                    }

                    // --- READONLY enforcement ---
                    if dispatch::try_enforce_readonly(cmd, ctx, &mut responses) {
                        continue;
                    }

                    // --- MA12: Disk full enforcement ---
                    if dispatch::try_enforce_disk_full(cmd, &mut responses) {
                        continue;
                    }

                    // --- CLIENT subcommands ---
                    if dispatch::try_handle_client_command(cmd, cmd_args, client_id, &mut conn, ctx, &mut responses) {
                        continue;
                    }

                    // --- TXN.BEGIN / TXN.COMMIT / TXN.ABORT ---
                    if txn::try_handle_txn_begin(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                        continue;
                    }
                    if txn::try_handle_txn_commit(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                        continue;
                    }
                    if txn::try_handle_txn_abort(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                        continue;
                    }

                    // --- TEMPORAL.SNAPSHOT_AT / TEMPORAL.INVALIDATE ---
                    if txn::try_handle_temporal_snapshot_at(cmd, cmd_args, ctx, &mut responses) {
                        continue;
                    }
                    if txn::try_handle_temporal_invalidate(cmd, cmd_args, ctx, &mut responses) {
                        continue;
                    }

                    // --- WS.* ---
                    if write::try_handle_ws_command(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                        continue;
                    }

                    // --- MQ.* ---
                    if write::try_handle_mq_command(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                        continue;
                    }

                    // --- MULTI / EXEC_CMD / DISCARD ---
                    if write::try_handle_multi_exec(cmd, &mut conn, ctx, &mut responses) {
                        continue;
                    }

                    // --- Workspace key prefix injection ---
                    // MUST happen before key_to_shard() so the {ws_id} hash tag determines
                    // shard routing. This is the ONLY code path where workspace prefixing
                    // occurs (WS-07, WS-12). All subsequent dispatch uses cmd_args (shadowed).
                    let rewritten = conn
                        .workspace_id
                        .as_ref()
                        .map(|ws_id| workspace_rewrite_args(cmd, cmd_args, ws_id));
                    let cmd_args: &[Frame] = rewritten.as_deref().unwrap_or(cmd_args);

                    // --- BLOCKING COMMANDS ---
                    if cmd.eq_ignore_ascii_case(b"BLPOP") || cmd.eq_ignore_ascii_case(b"BRPOP")
                        || cmd.eq_ignore_ascii_case(b"BLMOVE") || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
                        || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
                        || cmd.eq_ignore_ascii_case(b"BLMPOP") || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
                        || cmd.eq_ignore_ascii_case(b"BZMPOP")
                    {
                        if conn.in_multi {
                            let nb_frame = convert_blocking_to_nonblocking(cmd, cmd_args);
                            conn.command_queue.push(nb_frame);
                            responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                            continue;
                        }
                        write_buf.clear();
                        for response in responses.iter() {
                            if conn.protocol_version >= 3 {
                                crate::protocol::serialize_resp3(response, &mut write_buf);
                            } else {
                                crate::protocol::serialize(response, &mut write_buf);
                            }
                        }
                        if stream.write_all(&write_buf).await.is_err() { arena.reset(); return (HandlerResult::Done, None); }
                        let blocking_response = handle_blocking_command(
                            cmd, cmd_args, conn.selected_db, &ctx.shard_databases, &ctx.blocking_registry,
                            ctx.shard_id, ctx.num_shards, &ctx.dispatch_tx, &shutdown,
                        ).await;
                        let blocking_response = apply_resp3_conversion(cmd, blocking_response, conn.protocol_version);
                        responses = Vec::with_capacity(1);
                        responses.push(blocking_response);
                        break;
                    }

                    // --- PUBLISH ---
                    if pubsub::try_handle_publish(cmd, cmd_args, &conn, ctx, &mut responses, &mut publish_batches) {
                        continue;
                    }

                    // --- SUBSCRIBE / PSUBSCRIBE ---
                    if let Some(action) = pubsub::try_handle_subscribe(
                        cmd, cmd_args, &mut stream, &mut write_buf,
                        &mut conn, ctx, &peer_addr, &mut responses,
                    ).await {
                        match action {
                            pubsub::SubscriberAction::Continue => { continue; }
                            pubsub::SubscriberAction::BreakOuter => { break; }
                            pubsub::SubscriberAction::EarlyReturn => { return (HandlerResult::Done, None); }
                        }
                    }
                    // UNSUBSCRIBE/PUNSUBSCRIBE in normal mode (not subscribed)
                    if pubsub::try_handle_unsubscribe(cmd, &mut responses) {
                        continue;
                    }

                    // --- PUBSUB introspection (zero-SPSC: direct shared-read) ---
                    if pubsub::try_handle_pubsub_introspection(cmd, cmd_args, ctx, &mut responses) {
                        continue;
                    }

                    // --- MULTI queue mode ---
                    if conn.in_multi {
                        conn.command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                        continue;
                    }

                    // --- BGSAVE / SAVE / LASTSAVE / BGREWRITEAOF ---
                    if dispatch::try_handle_persistence(cmd, ctx, &mut responses) {
                        continue;
                    }

                    // --- Cross-shard aggregation: KEYS, SCAN, DBSIZE ---
                    if dispatch::try_handle_cross_shard_scan(cmd, cmd_args, &conn, ctx, &mut responses).await {
                        continue;
                    }

                    // --- FT.* vector search commands ---
                    if ft::try_handle_ft_command(cmd, cmd_args, &frame, &conn, ctx, &mut responses).await {
                        continue;
                    }

                    // --- GRAPH.* graph commands ---
                    #[cfg(feature = "graph")]
                    if write::try_handle_graph_command(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                        continue;
                    }

                    // --- MA2: KILL SNAPSHOT <txn_id> ---
                    // Routes directly to VectorStore's TransactionManager on the
                    // local shard. Bypasses write-stall guards (admin command).
                    if cmd.eq_ignore_ascii_case(b"KILL") {
                        // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                        let response = if crate::shard::slice::is_initialized() {
                            crate::shard::slice::with_shard(|s| {
                                crate::command::server_admin::kill_snapshot(&mut s.vector_store, cmd_args)
                            })
                        } else {
                            let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            let response =
                                crate::command::server_admin::kill_snapshot(&mut vs, cmd_args);
                            drop(vs);
                            response
                        };
                        responses.push(response);
                        continue;
                    }

                    // --- P8: VACUUM — manual reclamation (MVCC passes only;
                    //     manifest/WAL not accessible from connection handler).
                    //
                    // B1 fix: route VECTOR/GRAPH subcommands to the dedicated
                    // entry points; the parent `vacuum()` still stubs them.
                    if cmd.eq_ignore_ascii_case(b"VACUUM") {
                        if let Some(sub_frame) = cmd_args.first() {
                            if let Some(sub) =
                                crate::command::helpers::extract_bytes(sub_frame)
                            {
                                if sub.eq_ignore_ascii_case(b"VECTOR") {
                                    // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                                    let response = if crate::shard::slice::is_initialized() {
                                        crate::shard::slice::with_shard(|s| {
                                            crate::command::server_admin::vacuum_vector(
                                                &mut s.vector_store,
                                                &cmd_args[1..],
                                            )
                                        })
                                    } else {
                                        let mut vs =
                                            ctx.shard_databases.vector_store(ctx.shard_id);
                                        let r =
                                            crate::command::server_admin::vacuum_vector(
                                                &mut vs,
                                                &cmd_args[1..],
                                            );
                                        drop(vs);
                                        r
                                    };
                                    responses.push(response);
                                    continue;
                                }
                                #[cfg(feature = "graph")]
                                if sub.eq_ignore_ascii_case(b"GRAPH") {
                                    // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                                    let response = if crate::shard::slice::is_initialized() {
                                        let graph_merge_max = ctx.config.graph_merge_max_segments;
                                        let graph_dead = ctx.config.graph_dead_edge_trigger;
                                        crate::shard::slice::with_shard(|s| {
                                            crate::command::server_admin::vacuum_graph(
                                                &mut s.graph_store,
                                                &cmd_args[1..],
                                                graph_merge_max,
                                                graph_dead,
                                            )
                                        })
                                    } else {
                                        let mut gs = ctx
                                            .shard_databases
                                            .graph_store_write(ctx.shard_id);
                                        let r =
                                            crate::command::server_admin::vacuum_graph(
                                                &mut gs,
                                                &cmd_args[1..],
                                                ctx.config.graph_merge_max_segments,
                                                ctx.config.graph_dead_edge_trigger,
                                            );
                                        drop(gs);
                                        r
                                    };
                                    responses.push(response);
                                    continue;
                                }
                            }
                        }
                        // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                        let response = if crate::shard::slice::is_initialized() {
                            crate::shard::slice::with_shard(|s| {
                                crate::command::server_admin::vacuum(
                                    &mut s.vector_store,
                                    None,
                                    None,
                                    cmd_args,
                                    crate::command::server_admin::DEFAULT_VACUUM_PRUNE_MARGIN,
                                )
                            })
                        } else {
                            let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            let r = crate::command::server_admin::vacuum(
                                &mut vs,
                                None, // manifest — not available in connection handler
                                None, // wal_v3 — not available in connection handler
                                cmd_args,
                                crate::command::server_admin::DEFAULT_VACUUM_PRUNE_MARGIN, // see server_admin.rs
                            );
                            drop(vs);
                            r
                        };
                        responses.push(response);
                        continue;
                    }

                    // --- P8: DEBUG RECLAMATION ---
                    if cmd.eq_ignore_ascii_case(b"DEBUG") {
                        if let Some(sub) = cmd_args.first() {
                            if let Some(s) = crate::command::helpers::extract_bytes(sub) {
                                if s.eq_ignore_ascii_case(b"RECLAMATION") {
                                    // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                                    let response = if crate::shard::slice::is_initialized() {
                                        crate::shard::slice::with_shard(|s| {
                                            crate::command::server_admin::debug_reclamation(
                                                &s.vector_store,
                                                None,
                                                None,
                                            )
                                        })
                                    } else {
                                        let vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                        let r = crate::command::server_admin::debug_reclamation(
                                            &vs, None, None,
                                        );
                                        drop(vs);
                                        r
                                    };
                                    responses.push(response);
                                    continue;
                                }
                            }
                        }
                        // Other DEBUG subcommands fall through.
                    }

                    // --- Multi-key commands ---
                    if is_multi_key_command(cmd, cmd_args) {
                        let response = crate::shard::coordinator::coordinate_multi_key(cmd, cmd_args, ctx.shard_id, ctx.num_shards, conn.selected_db, &ctx.shard_databases, &ctx.dispatch_tx, &ctx.spsc_notifiers, &ctx.cached_clock, &()).await;
                        responses.push(response);
                        continue;
                    }

                    // --- Routing: keyless, local, or remote ---
                    let target_shard = extract_primary_key(cmd, cmd_args).map(|key| key_to_shard(key, ctx.num_shards));
                    let is_local = match target_shard {
                        None => true,
                        Some(s) if s == ctx.shard_id => true,
                        _ => false,
                    };

                    // Affinity sampling: record shard target for migration decision.
                    // Only sample when we have a concrete target shard (key-bearing command).
                    // Migration is deferred until AFTER the current batch is fully processed
                    // and all responses are written, ensuring no command/response desync.
                    if let (Some(tracker), Some(target)) = (&mut conn.affinity_tracker, target_shard) {
                        if let Some(migrate_to) = tracker.record(target) {
                            // IP-level hint: future connections from the same IP land
                            // directly on the data shard, skipping the 16-sample warm-up.
                            // Recorded whenever the per-connection sampler converges —
                            // the keyspace-locality signal is valid even if THIS
                            // connection is held back by MULTI/tracking from migrating.
                            // (Populated only for can_migrate=true connections; TLS
                            // connections don't own a sampler so they don't contribute.)
                            if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                ctx.pubsub_affinity.write().register_key(addr.ip(), migrate_to);
                            }
                            // Migration preconditions: not in MULTI, no active CLIENT TRACKING
                            // (tracking connections need untrack_all cleanup which doesn't transfer)
                            if !conn.in_multi && !conn.tracking_state.enabled {
                                conn.migration_target = Some(migrate_to);
                            }
                        }
                    }

                    let is_write = if ctx.aof_tx.is_some() || conn.tracking_state.enabled { metadata::is_write(cmd) } else { false };
                    let aof_bytes = if is_write && ctx.aof_tx.is_some() { Some(aof::serialize_command(&frame)) } else { None };

                    if is_local {
                        // LOCAL PATH: split into read/write to avoid exclusive lock on reads.
                        // Using read_db for local reads eliminates RwLock contention with
                        // cross-shard shared reads from other shard threads.
                        local_dispatches = local_dispatches.saturating_add(1);
                        if metadata::is_write(cmd) {
                            // WRITE PATH: single lock acquisition for eviction + dispatch.
                            //
                            // Phase 2f: gate on is_initialized(); new path uses ShardSlice
                            // directly. Eviction → KV undo-log capture → dispatch → wakeup
                            // run inside the with_shard_db closure so the &mut Database
                            // borrow stays disjoint from the post-drop HSET auto-index
                            // and DEL/UNLINK auto-delete paths (which need vector/text
                            // stores, separate ShardSlice fields).
                            let db_count = ctx.shard_databases.db_count();
                            // Returns Ok((response, sample_latency, dispatch_start)) on
                            // success or Err(oom_frame) when eviction fails (caller
                            // pushes + continues).
                            type WriteOutcome = Result<
                                (Frame, bool, Option<std::time::Instant>),
                                Frame,
                            >;
                            let mut do_write = |db: &mut crate::storage::db::Database,
                                                conn: &mut super::core::ConnectionState|
                             -> WriteOutcome {
                                let rt = ctx.runtime_config.read();
                                if let Err(oom_frame) = try_evict_if_needed(db, &rt) {
                                    drop(rt);
                                    return Err(oom_frame);
                                }
                                drop(rt);

                                // KV undo-log capture for active cross-store transactions.
                                // MUST happen BEFORE dispatch() overwrites the database entry.
                                if let Some(ref mut txn) = conn.active_cross_txn {
                                    if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
                                        for arg in cmd_args.iter() {
                                            if let Frame::BulkString(key_bytes) = arg {
                                                if let Some(old_entry) = db.get(key_bytes.as_ref()).cloned() {
                                                    txn.kv_undo.record_delete(key_bytes.clone(), old_entry);
                                                    let lsn = txn.snapshot_lsn;
                                                    let tid = txn.txn_id;
                                                    ctx.shard_databases.kv_intents(ctx.shard_id)
                                                        .record_write(key_bytes.clone(), lsn, tid);
                                                }
                                            }
                                        }
                                    } else if let Some(key) = crate::server::conn::shared::extract_primary_key(cmd, cmd_args) {
                                        let old_entry = db.get(key.as_ref()).cloned();
                                        let lsn = txn.snapshot_lsn;
                                        let tid = txn.txn_id;
                                        match old_entry {
                                            None => txn.kv_undo.record_insert(key.clone()),
                                            Some(entry) => txn.kv_undo.record_update(key.clone(), entry),
                                        }
                                        ctx.shard_databases.kv_intents(ctx.shard_id)
                                            .record_write(key.clone(), lsn, tid);
                                    }
                                }

                                db.refresh_now_from_cache(&ctx.cached_clock);
                                conn.cmd_counter = conn.cmd_counter.wrapping_add(1);
                                let sample_latency = (conn.cmd_counter & 0xF) == 0;
                                let dispatch_start = sample_latency.then(std::time::Instant::now);
                                let result = dispatch(db, cmd, cmd_args, &mut conn.selected_db, db_count);
                                let response = match result {
                                    DispatchResult::Response(f) => f,
                                    DispatchResult::Quit(f) => { should_quit = true; f }
                                };
                                if !matches!(response, Frame::Error(_)) {
                                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH")
                                        || cmd.eq_ignore_ascii_case(b"LMOVE") || cmd.eq_ignore_ascii_case(b"ZADD");
                                    if needs_wake {
                                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                            let mut reg = ctx.blocking_registry.borrow_mut();
                                            if cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH") || cmd.eq_ignore_ascii_case(b"LMOVE") {
                                                crate::blocking::wakeup::try_wake_list_waiter(&mut reg, db, conn.selected_db, &key);
                                            } else {
                                                crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, db, conn.selected_db, &key);
                                            }
                                        }
                                    }
                                }
                                Ok((response, sample_latency, dispatch_start))
                            };

                            let write_outcome: WriteOutcome = if crate::shard::slice::is_initialized() {
                                crate::shard::slice::with_shard_db(conn.selected_db, |db| do_write(db, &mut conn))
                            } else {
                                let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                                let r = do_write(&mut *guard, &mut conn);
                                drop(guard);
                                r
                            };

                            let (response, _sample_latency, dispatch_start): (Frame, bool, Option<std::time::Instant>) =
                                match write_outcome {
                                    Ok(t) => t,
                                    Err(oom_frame) => {
                                        responses.push(oom_frame);
                                        continue;
                                    }
                                };
                            if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                if let Some(start) = dispatch_start {
                                    let elapsed_us = start.elapsed().as_micros() as u64;
                                    crate::admin::metrics_setup::record_command_cached(
                                        cmd_str,
                                        elapsed_us,
                                        &mut conn.cached_metrics,
                                    );
                                    if let Frame::Array(ref args) = frame {
                                        crate::admin::metrics_setup::global_slowlog().maybe_record(
                                            elapsed_us,
                                            args.as_slice(),
                                            peer_addr.as_bytes(),
                                            conn.client_name.as_ref().map_or(b"" as &[u8], |n| n.as_ref()),
                                        );
                                    }
                                } else {
                                    crate::admin::metrics_setup::record_command_no_latency_cached(
                                        cmd_str,
                                        &mut conn.cached_metrics,
                                    );
                                }
                            }
                            if matches!(response, Frame::Error(_)) {
                                if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                    crate::admin::metrics_setup::record_command_error_cached(
                                        cmd_str,
                                        &mut conn.cached_metrics,
                                    );
                                }
                            }
                            // Auto-index vectors on successful HSET (local write path)
                            // Placed AFTER drop(guard) to avoid DB→vector_store lock order
                            // inversion with the shard event loop (vector_store→DB).
                            if !matches!(response, Frame::Error(_))
                                && (cmd.eq_ignore_ascii_case(b"HSET") || cmd.eq_ignore_ascii_case(b"HMSET"))
                            {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    // Phase 166 (Plan 02): if inside an active TXN, tag each
                                    // inserted mutable-HNSW entry with the txn_id so that
                                    // non-TXN readers (snapshot_lsn==0) see it as uncommitted
                                    // and exclude it until TXN.COMMIT calls
                                    // txn_manager.commit(txn_id) (ACID-09 isolation fix).
                                    let active_txn_id = conn
                                        .active_cross_txn
                                        .as_ref()
                                        .map(|t| t.txn_id)
                                        .unwrap_or(0);
                                    // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                                    // vector_store + text_store accessed in ONE with_shard closure
                                    // (multi-resource arm) to avoid re-entrant RefCell borrow.
                                    let inserted = if crate::shard::slice::is_initialized() {
                                        crate::shard::slice::with_shard(|s| {
                                            if active_txn_id != 0 {
                                                crate::shard::spsc_handler::auto_index_hset_public_txn(&mut s.vector_store, &mut s.text_store, &key, cmd_args, active_txn_id)
                                            } else {
                                                crate::shard::spsc_handler::auto_index_hset_public(&mut s.vector_store, &mut s.text_store, &key, cmd_args)
                                            }
                                        })
                                    } else {
                                        let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                        let mut ts = ctx.shard_databases.text_store(ctx.shard_id);
                                        if active_txn_id != 0 {
                                            crate::shard::spsc_handler::auto_index_hset_public_txn(&mut vs, &mut *ts, &key, cmd_args, active_txn_id)
                                        } else {
                                            crate::shard::spsc_handler::auto_index_hset_public(&mut vs, &mut *ts, &key, cmd_args)
                                        }
                                    };
                                    // Push one VectorIntent per (index_name, key_hash) so
                                    // TXN.ABORT (Plan 166-03) can tombstone via
                                    // MutableSegment::mark_deleted_by_key_hash.
                                    if let Some(txn) = conn.active_cross_txn.as_mut() {
                                        for (index_name, key_hash) in inserted {
                                            txn.record_vector(key_hash, index_name);
                                        }
                                    }
                                }
                            }
                            // Auto-delete vectors on DEL/UNLINK (local write path)
                            // Note: HDEL removes fields, not keys — it should NOT trigger
                            // vector deletion unless the entire key is removed.
                            if !matches!(response, Frame::Error(_))
                                && (cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK"))
                            {
                                // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                                if crate::shard::slice::is_initialized() {
                                    crate::shard::slice::with_shard(|s| {
                                        for arg in cmd_args.iter() {
                                            if let Some(key) = extract_bytes(arg) {
                                                s.vector_store.mark_deleted_for_key(key.as_ref());
                                            }
                                        }
                                    });
                                } else {
                                    let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                    for arg in cmd_args.iter() {
                                        if let Some(key) = extract_bytes(arg) {
                                            vs.mark_deleted_for_key(key.as_ref());
                                        }
                                    }
                                }
                            }
                            if let Some(bytes) = aof_bytes {
                                if !matches!(response, Frame::Error(_)) {
                                    if let Some(ref tx) = ctx.aof_tx { let _ = tx.try_send(AofMessage::Append(bytes)); }
                                }
                            }
                            if conn.tracking_state.enabled && !matches!(response, Frame::Error(_)) {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    let senders = ctx.tracking_table.borrow_mut().invalidate_key(&key, client_id);
                                    if !senders.is_empty() {
                                        let push = crate::tracking::invalidation::invalidation_push(&[key]);
                                        for tx in senders { let _ = tx.try_send(push.clone()); }
                                    }
                                }
                            }
                            let mut response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                            if let Some(ws_id) = conn.workspace_id.as_ref() {
                                strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                            }
                            responses.push(response);
                        } else {
                            // Snapshot visibility filter for active cross-store transactions.
                            // MVCC: hide keys written by uncommitted foreign transactions.
                            if conn.in_cross_txn() {
                                if let Some(ref txn) = conn.active_cross_txn {
                                    if let Some(key) = crate::server::conn::shared::extract_primary_key(cmd, cmd_args) {
                                        let snapshot_lsn = txn.snapshot_lsn;
                                        let my_txn_id = txn.txn_id;
                                        // Clone committed treemap to release vector_store lock
                                        // before acquiring kv_intents lock (lock ordering).
                                        // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                                        let committed = if crate::shard::slice::is_initialized() {
                                            crate::shard::slice::with_shard(|s| {
                                                s.vector_store.txn_manager().committed_treemap().clone()
                                            })
                                        } else {
                                            let vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                            vs.txn_manager().committed_treemap().clone()
                                        };
                                        let visible = {
                                            let intents = ctx.shard_databases.kv_intents(ctx.shard_id);
                                            intents.is_key_visible(key.as_ref(), snapshot_lsn, my_txn_id, &committed)
                                        };
                                        if !visible {
                                            responses.push(Frame::Null);
                                            continue;
                                        }
                                    }
                                }
                            }

                            // READ PATH: shared lock — no contention with other shards' reads.
                            // Phase 2f: gate on is_initialized(); new path uses ShardSlice directly.
                            let now_ms = ctx.cached_clock.ms();
                            let db_count = ctx.shard_databases.db_count();
                            conn.cmd_counter = conn.cmd_counter.wrapping_add(1);
                            let sample_latency = (conn.cmd_counter & 0xF) == 0;
                            let dispatch_start = sample_latency.then(std::time::Instant::now);
                            let result = if crate::shard::slice::is_initialized() {
                                crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                                    dispatch_read(db, cmd, cmd_args, now_ms, &mut conn.selected_db, db_count)
                                })
                            } else {
                                let guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
                                let r = dispatch_read(&guard, cmd, cmd_args, now_ms, &mut conn.selected_db, db_count);
                                drop(guard);
                                r
                            };
                            if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                if let Some(start) = dispatch_start {
                                    let elapsed_us = start.elapsed().as_micros() as u64;
                                    crate::admin::metrics_setup::record_command_cached(
                                        cmd_str,
                                        elapsed_us,
                                        &mut conn.cached_metrics,
                                    );
                                    if let Frame::Array(ref args) = frame {
                                        crate::admin::metrics_setup::global_slowlog().maybe_record(
                                            elapsed_us,
                                            args.as_slice(),
                                            peer_addr.as_bytes(),
                                            conn.client_name.as_ref().map_or(b"" as &[u8], |n| n.as_ref()),
                                        );
                                    }
                                } else {
                                    crate::admin::metrics_setup::record_command_no_latency_cached(
                                        cmd_str,
                                        &mut conn.cached_metrics,
                                    );
                                }
                            }
                            let response = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => { should_quit = true; f }
                            };
                            if matches!(response, Frame::Error(_)) {
                                if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                    crate::admin::metrics_setup::record_command_error_cached(
                                        cmd_str,
                                        &mut conn.cached_metrics,
                                    );
                                }
                            }
                            if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    ctx.tracking_table.borrow_mut().track_key(client_id, &key, conn.tracking_state.noloop);
                                }
                            }
                            let mut response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                            if let Some(ws_id) = conn.workspace_id.as_ref() {
                                strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                            }
                            responses.push(response);
                        }
                    } else if let Some(target) = target_shard {
                        // TXN cross-shard guard: cross-shard writes bypass the undo log and
                        // cannot be rolled back on TXN.ABORT. Return an explicit error instead
                        // of silently permitting writes that resist rollback.
                        if conn.in_cross_txn() && metadata::is_write(cmd) {
                            responses.push(Frame::Error(bytes::Bytes::from_static(
                                crate::command::transaction::ERR_TXN_CROSS_SHARD,
                            )));
                            continue;
                        }
                        // Phase 3.1: SHARED-READ FAST PATH deleted. Cross-shard
                        // reads and writes both dispatch via SPSC below. This is
                        // a prerequisite for Phase 4 (stripping RwLock<Database>).
                        //
                        // Cross-shard SPSC dispatch (deferred):
                        // When workspace rewriting occurred, rebuild the frame with
                        // prefixed args so the target shard stores the correct key.
                        let dispatch_frame = if rewritten.is_some() {
                            let mut parts = Vec::with_capacity(1 + cmd_args.len());
                            parts.push(Frame::BulkString(Bytes::copy_from_slice(cmd)));
                            parts.extend_from_slice(cmd_args);
                            Frame::Array(parts.into())
                        } else {
                            frame
                        };
                        let resp_idx = responses.len();
                        responses.push(Frame::Null);
                        let cmd_bytes = if let Frame::Array(ref args) = dispatch_frame {
                            extract_bytes(&args[0]).unwrap_or_default()
                        } else {
                            Bytes::new()
                        };
                        remote_groups.entry(target).or_default().push((resp_idx, std::sync::Arc::new(dispatch_frame), aof_bytes, cmd_bytes, conn.selected_db));
                        cross_spsc_dispatches = cross_spsc_dispatches.saturating_add(1);
                    }
                }

                // Flush per-batch dispatch-path counters — one global atomic
                // per path instead of N per batch. Short-circuits on 0.
                crate::admin::metrics_setup::record_dispatch_local_batch(local_dispatches as u64);
                // Phase 3.1: cross_read_fastpath metric removed (fast path deleted).
                crate::admin::metrics_setup::record_dispatch_cross_spsc_batch(cross_spsc_dispatches as u64);

                // Phase 2: Dispatch deferred remote commands (zero-allocation via ResponseSlotPool)
                if !remote_groups.is_empty() {
                    let mut reply_futures: Vec<(Vec<(usize, Option<Bytes>, Bytes)>, usize)> = Vec::with_capacity(remote_groups.len());
                    for (target, entries) in remote_groups {
                        let slot_ptr = response_pool.slot_ptr(target);
                        // Use the db_index captured with the first command (all commands in a
                        // pipeline batch targeting the same shard share the same db_index).
                        let batch_db = entries.first().map(|(_, _, _, _, db)| *db).unwrap_or(conn.selected_db);
                        let (meta, commands): (Vec<(usize, Option<Bytes>, Bytes)>, Vec<std::sync::Arc<Frame>>) =
                            entries.into_iter().map(|(idx, arc_frame, aof, cmd, _db)| ((idx, aof, cmd), arc_frame)).unzip();
                        let msg = ShardMessage::PipelineBatchSlotted { db_index: batch_db, commands, response_slot: crate::shard::dispatch::ResponseSlotPtr(slot_ptr) };
                        let target_idx = ChannelMesh::target_index(ctx.shard_id, target);
                        {
                            let mut pending = msg;
                            loop {
                                let push_result = { let mut producers = ctx.dispatch_tx.borrow_mut(); producers[target_idx].try_push(pending) };
                                match push_result {
                                    Ok(()) => { ctx.spsc_notifiers[target].notify_one(); break; }
                                    Err(val) => { pending = val; tokio::task::yield_now().await; }
                                }
                            }
                        }
                        reply_futures.push((meta, target));
                    }
                    let proto_ver = conn.protocol_version;
                    for (meta, target) in reply_futures {
                        let shard_responses = response_pool.future_for(target).await;
                        for ((resp_idx, aof_bytes, cmd_name), resp) in meta.into_iter().zip(shard_responses) {
                            if let Some(bytes) = aof_bytes {
                                if !matches!(resp, Frame::Error(_)) {
                                    if let Some(ref tx) = ctx.aof_tx { let _ = tx.try_send(AofMessage::Append(bytes)); }
                                }
                            }
                            responses[resp_idx] = apply_resp3_conversion(&cmd_name, resp, proto_ver);
                        }
                    }
                }

                // Phase 3: Flush accumulated PUBLISH batches as PubSubPublishBatch messages
                if !publish_batches.is_empty() {
                    let mut batch_slots: Vec<(std::sync::Arc<crate::shard::dispatch::PubSubResponseSlot>, Vec<usize>)> = Vec::new();
                    {
                        let mut producers = ctx.dispatch_tx.borrow_mut();
                        for (target, entries) in publish_batches.drain() {
                            let n = entries.len();
                            let slot = std::sync::Arc::new(crate::shard::dispatch::PubSubResponseSlot::with_counts(1, n));
                            let resp_indices: Vec<usize> = entries.iter().map(|(idx, _, _)| *idx).collect();
                            let pairs: Vec<(Bytes, Bytes)> = entries.into_iter().map(|(_, ch, msg)| (ch, msg)).collect();

                            let idx = ChannelMesh::target_index(ctx.shard_id, target);
                            let batch_msg = ShardMessage::PubSubPublishBatch {
                                pairs,
                                slot: slot.clone(),
                            };
                            if producers[idx].try_push(batch_msg).is_ok() {
                                ctx.spsc_notifiers[target].notify_one();
                            } else {
                                slot.add(0); // push failed, mark as done
                            }
                            batch_slots.push((slot, resp_indices));
                        }
                    }
                    // Resolve all batch slots
                    for (slot, resp_indices) in &batch_slots {
                        crate::shard::dispatch::PubSubResponseFuture::new(slot.clone()).await;
                        for (i, resp_idx) in resp_indices.iter().enumerate() {
                            let remote_count = slot.counts[i].load(std::sync::atomic::Ordering::Relaxed);
                            if remote_count > 0 {
                                if let Frame::Integer(ref mut total) = responses[*resp_idx] { *total += remote_count; }
                            }
                        }
                    }
                }

                arena.reset();

                // AUTH rate limiting: delay response to slow down brute-force attacks
                if auth_delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(auth_delay_ms)).await;
                }

                write_buf.clear();
                for response in &responses {
                    if conn.protocol_version >= 3 {
                        crate::protocol::serialize_resp3(response, &mut write_buf);
                    } else {
                        crate::protocol::serialize(response, &mut write_buf);
                    }
                }
                if stream.write_all(&write_buf).await.is_err() {
                    return (HandlerResult::Done, None);
                }

                // Update registry with current state after each batch
                crate::client_registry::update(client_id, |e| {
                    e.db = conn.selected_db;
                    e.last_cmd_at = std::time::Instant::now();
                    e.flags = crate::client_registry::ClientFlags {
                        subscriber: conn.subscription_count > 0,
                        in_multi: conn.in_multi,
                        blocked: false,
                    };
                });

                // Check if migration was triggered during frame processing.
                // All responses for the current batch have been written, so the
                // client sees no interruption -- TCP socket stays open.
                if let Some(target_shard) = conn.migration_target {
                    let migrated_state = MigratedConnectionState {
                        selected_db: conn.selected_db,
                        authenticated: conn.authenticated,
                        client_name: conn.client_name.clone(),
                        protocol_version: conn.protocol_version,
                        current_user: conn.current_user.clone(),
                        flags: 0,
                        read_buf_remainder: read_buf.split(),
                        client_id,
                        peer_addr: peer_addr.clone(),
                        workspace_id: conn.workspace_id,
                    };
                    return (
                        HandlerResult::MigrateConnection { state: migrated_state, target_shard },
                        Some(stream),
                    );
                }

                if write_buf.capacity() > 65536 { write_buf = BytesMut::with_capacity(8192); }
                if read_buf.capacity() > 65536 {
                    let remaining = read_buf.split();
                    read_buf = BytesMut::with_capacity(8192);
                    if !remaining.is_empty() { read_buf.extend_from_slice(&remaining); }
                }

                if should_quit { break; }
            }
            _ = shutdown.cancelled() => {
                write_buf.clear();
                let shutdown_err = Frame::Error(Bytes::from_static(b"ERR server shutting down"));
                if conn.protocol_version >= 3 {
                    crate::protocol::serialize_resp3(&shutdown_err, &mut write_buf);
                } else {
                    crate::protocol::serialize(&shutdown_err, &mut write_buf);
                }
                let _ = stream.write_all(&write_buf).await;
                break;
            }
        }
    }

    // Phase 166: release any leaked cross-store TXN (client disconnected mid-txn).
    // Idempotent: TXN.ABORT already takes() active_cross_txn so this is a no-op if abort ran.
    // Closes T-161-05 — without this, a disconnect after TXN.BEGIN + SET would leak
    // kv_intents and pin the key invisible for all subsequent readers.
    if let Some(txn) = conn.active_cross_txn.take() {
        crate::transaction::abort::abort_cross_store_txn(
            &ctx.shard_databases,
            ctx.shard_id,
            conn.selected_db,
            txn,
        );
    }

    // Clean up pub/sub subscriptions on disconnect
    if conn.subscriber_id > 0 {
        let removed_channels = {
            ctx.pubsub_registry
                .write()
                .unsubscribe_all(conn.subscriber_id)
        };
        let removed_patterns = {
            ctx.pubsub_registry
                .write()
                .punsubscribe_all(conn.subscriber_id)
        };
        for ch in removed_channels {
            unpropagate_subscription(
                &ctx.all_remote_sub_maps,
                &ch,
                ctx.shard_id,
                ctx.num_shards,
                false,
            );
        }
        for pat in removed_patterns {
            unpropagate_subscription(
                &ctx.all_remote_sub_maps,
                &pat,
                ctx.shard_id,
                ctx.num_shards,
                true,
            );
        }
        // Clear pub/sub affinity on disconnect (no subscriptions remain).
        // Preserves any key-access hint — storage locality outlives the subscription.
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            ctx.pubsub_affinity.write().remove_pubsub(&addr.ip());
        }
    }

    if conn.tracking_state.enabled {
        ctx.tracking_table.borrow_mut().untrack_all(client_id);
    }

    (HandlerResult::Done, None)
}
