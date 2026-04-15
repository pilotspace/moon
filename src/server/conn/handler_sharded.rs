// Note: some imports/variables may be conditionally used across feature flags
//! Sharded tokio connection handlers.
//!
//! Extracted from `server/connection.rs` (Plan 48-02).
//! Contains `handle_connection_sharded` (thin wrapper) and
//! `handle_connection_sharded_inner` (generic inner handler).

use crate::runtime::TcpStream;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use bumpalo::Bump;
use bytes::{Bytes, BytesMut};
use ringbuf::traits::Producer;
use std::collections::HashMap;
use std::sync::Arc;

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::storage::eviction::try_evict_if_needed;
use crate::tracking::TrackingState;

use super::affinity::MigratedConnectionState;
use crate::server::response_slot::ResponseSlotPool;

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
    is_multi_key_command, propagate_subscription, unpropagate_subscription,
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
                let msg = ShardMessage::MigrateConnection { fd: raw_fd, state };
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
                        if let Some(ShardMessage::MigrateConnection { fd, .. }) = pending {
                            use std::os::unix::io::FromRawFd;
                            // SAFETY: fd is a valid, uniquely-owned file descriptor obtained
                            // from TcpStream::into_raw_fd() above. OwnedFd closes it on drop.
                            drop(unsafe { std::os::unix::io::OwnedFd::from_raw_fd(fd) });
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

    use crate::pubsub::{self, subscriber::Subscriber};

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
            #[allow(clippy::unwrap_used)]
            // conn.pubsub_rx is always Some when conn.subscription_count > 0
            let rx = conn.pubsub_rx.as_mut().unwrap();
            tokio::select! {
                n = stream.read_buf(&mut read_buf) => {
                    match n {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    let mut sub_break = false;
                    loop {
                        match crate::protocol::parse(&mut read_buf, &parse_config) {
                            Ok(Some(frame)) => {
                                if let Some((cmd, cmd_args)) = extract_command(&frame) {
                                    if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") {
                                        if cmd_args.is_empty() {
                                            let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'subscribe' command"));
                                            write_buf.clear();
                                            crate::protocol::serialize(&err, &mut write_buf);
                                            if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            continue;
                                        }
                                        for arg in cmd_args {
                                            if let Some(ch) = extract_bytes(arg) {
                                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                let acl_deny = { ctx.acl_table.read().unwrap().check_channel_permission(&conn.current_user, ch.as_ref()) };
                                                if let Some(reason) = acl_deny {
                                                    let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&err, &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                    continue;
                                                }
                                                #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some in subscriber mode
                                                let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
                                                { ctx.pubsub_registry.write().subscribe(ch.clone(), sub); }
                                                conn.subscription_count += 1;
                                                // Register pub/sub affinity for this client IP
                                                if conn.subscription_count == 1 {
                                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                        ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                    }
                                                }
                                                propagate_subscription(&ctx.all_remote_sub_maps, &ch, ctx.shard_id, ctx.num_shards, false);
                                                write_buf.clear();
                                                let resp = pubsub::subscribe_response(&ch, conn.subscription_count);
                                                crate::protocol::serialize(&resp, &mut write_buf);
                                                if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            }
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                                        if cmd_args.is_empty() {
                                            let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'psubscribe' command"));
                                            write_buf.clear();
                                            crate::protocol::serialize(&err, &mut write_buf);
                                            if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            continue;
                                        }
                                        for arg in cmd_args {
                                            if let Some(pat) = extract_bytes(arg) {
                                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                let acl_deny = { ctx.acl_table.read().unwrap().check_channel_permission(&conn.current_user, pat.as_ref()) };
                                                if let Some(reason) = acl_deny {
                                                    let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&err, &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                    continue;
                                                }
                                                #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some in subscriber mode
                                                let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
                                                { ctx.pubsub_registry.write().psubscribe(pat.clone(), sub); }
                                                conn.subscription_count += 1;
                                                // Register pub/sub affinity for this client IP
                                                if conn.subscription_count == 1 {
                                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                        ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                    }
                                                }
                                                propagate_subscription(&ctx.all_remote_sub_maps, &pat, ctx.shard_id, ctx.num_shards, true);
                                                write_buf.clear();
                                                let resp = pubsub::psubscribe_response(&pat, conn.subscription_count);
                                                crate::protocol::serialize(&resp, &mut write_buf);
                                                if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            }
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
                                        if cmd_args.is_empty() {
                                            let removed = { ctx.pubsub_registry.write().unsubscribe_all(conn.subscriber_id) };
                                            if removed.is_empty() {
                                                conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                                write_buf.clear();
                                                crate::protocol::serialize(&pubsub::unsubscribe_response(&Bytes::from_static(b""), conn.subscription_count), &mut write_buf);
                                                if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            } else {
                                                for ch in &removed {
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    unpropagate_subscription(&ctx.all_remote_sub_maps, ch, ctx.shard_id, ctx.num_shards, false);
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::unsubscribe_response(ch, conn.subscription_count), &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(ch) = extract_bytes(arg) {
                                                    { ctx.pubsub_registry.write().unsubscribe(ch.as_ref(), conn.subscriber_id); }
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    unpropagate_subscription(&ctx.all_remote_sub_maps, &ch, ctx.shard_id, ctx.num_shards, false);
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::unsubscribe_response(&ch, conn.subscription_count), &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                }
                                            }
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                                        if cmd_args.is_empty() {
                                            let removed = { ctx.pubsub_registry.write().punsubscribe_all(conn.subscriber_id) };
                                            if removed.is_empty() {
                                                conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                                write_buf.clear();
                                                crate::protocol::serialize(&pubsub::punsubscribe_response(&Bytes::from_static(b""), conn.subscription_count), &mut write_buf);
                                                if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            } else {
                                                for pat in &removed {
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    unpropagate_subscription(&ctx.all_remote_sub_maps, pat, ctx.shard_id, ctx.num_shards, true);
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::punsubscribe_response(pat, conn.subscription_count), &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(pat) = extract_bytes(arg) {
                                                    { ctx.pubsub_registry.write().punsubscribe(pat.as_ref(), conn.subscriber_id); }
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    unpropagate_subscription(&ctx.all_remote_sub_maps, &pat, ctx.shard_id, ctx.num_shards, true);
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::punsubscribe_response(&pat, conn.subscription_count), &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                }
                                            }
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"PING") {
                                        write_buf.clear();
                                        let resp = Frame::Array(crate::framevec![
                                            Frame::BulkString(Bytes::from_static(b"pong")),
                                            Frame::BulkString(Bytes::from_static(b"")),
                                        ]);
                                        crate::protocol::serialize(&resp, &mut write_buf);
                                        if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                    } else if cmd.eq_ignore_ascii_case(b"QUIT") {
                                        write_buf.clear();
                                        crate::protocol::serialize(&Frame::SimpleString(Bytes::from_static(b"OK")), &mut write_buf);
                                        let _ = stream.write_all(&write_buf).await;
                                        sub_break = true;
                                        break;
                                    } else if cmd.eq_ignore_ascii_case(b"RESET") {
                                        { ctx.pubsub_registry.write().unsubscribe_all(conn.subscriber_id); }
                                        { ctx.pubsub_registry.write().punsubscribe_all(conn.subscriber_id); }
                                        conn.subscription_count = 0;
                                        write_buf.clear();
                                        crate::protocol::serialize(&Frame::SimpleString(Bytes::from_static(b"RESET")), &mut write_buf);
                                        if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                    } else {
                                        let cmd_str = String::from_utf8_lossy(cmd);
                                        let err = Frame::Error(Bytes::from(format!(
                                            "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context",
                                            cmd_str.to_lowercase()
                                        )));
                                        write_buf.clear();
                                        crate::protocol::serialize(&err, &mut write_buf);
                                        if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                    }
                                }
                                if conn.subscription_count == 0 { break; }
                            }
                            Ok(None) => break,
                            Err(_) => { return (HandlerResult::Done, None); }
                        }
                    }
                    if sub_break { break; }
                    if conn.subscription_count == 0 { continue; }
                }
                msg = rx.recv_async() => {
                    match msg {
                        Ok(data) => {
                            // Data is pre-serialized RESP bytes — write directly
                            if stream.write_all(&data).await.is_err() { break; }
                        }
                        Err(_) => break,
                    }
                }
                _ = shutdown.cancelled() => {
                    write_buf.clear();
                    crate::protocol::serialize(&Frame::Error(Bytes::from_static(b"ERR server shutting down")), &mut write_buf);
                    let _ = stream.write_all(&write_buf).await;
                    break;
                }
            }
            continue;
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
                        let response = {
                            let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                            let db_count = ctx.shard_databases.db_count();
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
                            let response = {
                                let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                                let db_count = ctx.shard_databases.db_count();
                                crate::command::functions::handle_fcall(
                                    &func_registry.borrow(), cmd_args, &mut guard,
                                    ctx.shard_id, ctx.num_shards, conn.selected_db, db_count,
                                )
                            };
                            responses.push(response);
                            continue;
                        }
                        if cmd.eq_ignore_ascii_case(b"FCALL_RO") {
                            let response = {
                                let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                                let db_count = ctx.shard_databases.db_count();
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
                    if cmd.eq_ignore_ascii_case(b"CONFIG") {
                        responses.push(handle_config(cmd_args, &ctx.runtime_config, &ctx.config));
                        continue;
                    }

                    // --- SLOWLOG ---
                    if cmd.eq_ignore_ascii_case(b"SLOWLOG") {
                        let sl = crate::admin::metrics_setup::global_slowlog();
                        responses.push(crate::admin::slowlog::handle_slowlog(sl, cmd_args));
                        continue;
                    }

                    // --- REPLICAOF / SLAVEOF ---
                    if cmd.eq_ignore_ascii_case(b"REPLICAOF") || cmd.eq_ignore_ascii_case(b"SLAVEOF") {
                        use crate::command::connection::{replicaof, ReplicaofAction};
                        let (resp, action) = replicaof(cmd_args);
                        if let Some(action) = action {
                            if let Some(ref rs) = ctx.repl_state {
                                match action {
                                    ReplicaofAction::StartReplication { host, port } => {
                                        if let Ok(mut rs_guard) = rs.write() {
                                            rs_guard.role = crate::replication::state::ReplicationRole::Replica {
                                                host: host.clone(), port,
                                                state: crate::replication::handshake::ReplicaHandshakeState::PingPending,
                                            };
                                        }
                                        let rs_clone = Arc::clone(rs);
                                        let cfg = crate::replication::replica::ReplicaTaskConfig {
                                            master_host: host, master_port: port,
                                            repl_state: rs_clone, num_shards: ctx.num_shards,
                                            persistence_dir: None, listening_port: 0,
                                        };
                                        tokio::task::spawn_local(crate::replication::replica::run_replica_task(cfg));
                                    }
                                    ReplicaofAction::PromoteToMaster => {
                                        use crate::replication::state::generate_repl_id;
                                        if let Ok(mut rs_guard) = rs.write() {
                                            rs_guard.repl_id2 = rs_guard.repl_id.clone();
                                            rs_guard.repl_id = generate_repl_id();
                                            rs_guard.role = crate::replication::state::ReplicationRole::Master;
                                        }
                                    }
                                    ReplicaofAction::NoOp => {}
                                }
                            }
                        }
                        responses.push(resp);
                        continue;
                    }

                    // --- REPLCONF ---
                    if cmd.eq_ignore_ascii_case(b"REPLCONF") {
                        responses.push(crate::command::connection::replconf(cmd_args));
                        continue;
                    }

                    // --- INFO ---
                    if cmd.eq_ignore_ascii_case(b"INFO") {
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
                                response_text.push_str(
                                    &crate::replication::handshake::build_info_replication(&rs_guard),
                                );
                            }
                        }
                        responses.push(Frame::BulkString(Bytes::from(response_text)));
                        continue;
                    }

                    // --- READONLY enforcement ---
                    if let Some(ref rs) = ctx.repl_state {
                        if let Ok(rs_guard) = rs.try_read() {
                            if matches!(rs_guard.role, crate::replication::state::ReplicationRole::Replica { .. }) {
                                if metadata::is_write(cmd) {
                                    responses.push(Frame::Error(Bytes::from_static(
                                        b"READONLY You can't write against a read only replica.",
                                    )));
                                    continue;
                                }
                            }
                        }
                    }

                    // --- CLIENT subcommands ---
                    if cmd.eq_ignore_ascii_case(b"CLIENT") {
                        if let Some(sub) = cmd_args.first() {
                            if let Some(sub_bytes) = extract_bytes(sub) {
                                if sub_bytes.eq_ignore_ascii_case(b"ID") {
                                    responses.push(conn_cmd::client_id(client_id));
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"SETNAME") {
                                    if cmd_args.len() != 2 {
                                        responses.push(Frame::Error(Bytes::from_static(
                                            b"ERR wrong number of arguments for 'CLIENT SETNAME' command",
                                        )));
                                    } else {
                                        conn.client_name = extract_bytes(&cmd_args[1]);
                                        let name_str = conn.client_name.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                                        crate::client_registry::update(client_id, |e| { e.name = name_str; });
                                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                    }
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"GETNAME") {
                                    responses.push(match &conn.client_name {
                                        Some(name) => Frame::BulkString(name.clone()),
                                        None => Frame::Null,
                                    });
                                    continue;
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
                                                        table.register_prefix(client_id, prefix.clone(), config_parsed.noloop);
                                                    }
                                                }
                                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                            } else {
                                                conn.tracking_state = TrackingState::default();
                                                ctx.tracking_table.borrow_mut().untrack_all(client_id);
                                                conn.tracking_rx = None;
                                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                            }
                                            continue;
                                        }
                                        Err(err_frame) => { responses.push(err_frame); continue; }
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
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"INFO") {
                                    crate::client_registry::update(client_id, |e| {
                                        e.db = conn.selected_db;
                                        e.last_cmd_at = std::time::Instant::now();
                                    });
                                    let info = crate::client_registry::client_info(client_id)
                                        .unwrap_or_default();
                                    responses.push(Frame::BulkString(Bytes::from(info)));
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"KILL") {
                                    let raw_args: Vec<&[u8]> = cmd_args[1..].iter()
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
                                    continue;
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
                                        match timeout_bytes.and_then(|b| std::str::from_utf8(b).ok()).and_then(|s| s.parse::<u64>().ok()) {
                                            Some(ms) => {
                                                let mode = if cmd_args.len() > 2 {
                                                    match &cmd_args[2] {
                                                        Frame::BulkString(b) | Frame::SimpleString(b) if b.eq_ignore_ascii_case(b"WRITE") => crate::client_pause::PauseMode::Write,
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
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"UNPAUSE") {
                                    crate::client_pause::unpause();
                                    responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"NO-EVICT") || sub_bytes.eq_ignore_ascii_case(b"NO-TOUCH") {
                                    responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                    continue;
                                }
                                responses.push(Frame::Error(Bytes::from(format!(
                                    "ERR unknown subcommand '{}'", String::from_utf8_lossy(&sub_bytes)
                                ))));
                                continue;
                            }
                        }
                        responses.push(Frame::Error(Bytes::from_static(
                            b"ERR wrong number of arguments for 'client' command",
                        )));
                        continue;
                    }

                    // --- MULTI ---
                    if cmd.eq_ignore_ascii_case(b"MULTI") {
                        if conn.in_multi {
                            responses.push(Frame::Error(Bytes::from_static(b"ERR MULTI calls can not be nested")));
                        } else {
                            conn.in_multi = true;
                            conn.command_queue.clear();
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        }
                        continue;
                    }

                    // --- EXEC ---
                    if cmd.eq_ignore_ascii_case(b"EXEC") {
                        if !conn.in_multi {
                            responses.push(Frame::Error(Bytes::from_static(b"ERR EXEC without MULTI")));
                        } else {
                            conn.in_multi = false;
                            let result = execute_transaction_sharded(&ctx.shard_databases, ctx.shard_id, &conn.command_queue, conn.selected_db, &ctx.cached_clock);
                            conn.command_queue.clear();
                            responses.push(result);
                        }
                        continue;
                    }

                    // --- DISCARD ---
                    if cmd.eq_ignore_ascii_case(b"DISCARD") {
                        if !conn.in_multi {
                            responses.push(Frame::Error(Bytes::from_static(b"ERR DISCARD without MULTI")));
                        } else {
                            conn.in_multi = false;
                            conn.command_queue.clear();
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        }
                        continue;
                    }

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
                    if cmd.eq_ignore_ascii_case(b"PUBLISH") {
                        if cmd_args.len() != 2 {
                            responses.push(Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'publish' command")));
                        } else {
                            let channel_arg = extract_bytes(&cmd_args[0]);
                            let message_arg = extract_bytes(&cmd_args[1]);
                            // ACL channel permission check for PUBLISH
                            if let Some(ref ch) = channel_arg {
                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                let acl_guard = ctx.acl_table.read().unwrap();
                                if let Some(deny_reason) = acl_guard.check_channel_permission(&conn.current_user, ch.as_ref()) {
                                    drop(acl_guard);
                                    responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                                    continue;
                                }
                            }
                            match (channel_arg, message_arg) {
                                (Some(ch), Some(msg)) => {
                                    let local_count = { ctx.pubsub_registry.write().publish(&ch, &msg) };
                                    // Targeted fanout: only send to shards that have subscribers
                                    let targets = ctx.remote_subscriber_map.read().target_shards(&ch);
                                    if targets.is_empty() {
                                        // Fast path: no remote subscribers, return local count immediately
                                        responses.push(Frame::Integer(local_count));
                                    } else {
                                        // Filter to remote targets only (skip self)
                                        let remote_targets: Vec<usize> = targets.into_iter().filter(|&t| t != ctx.shard_id).collect();
                                        if remote_targets.is_empty() {
                                            responses.push(Frame::Integer(local_count));
                                        } else {
                                            // Accumulate into per-shard batches for coalesced dispatch
                                            let resp_idx = responses.len();
                                            responses.push(Frame::Integer(local_count)); // placeholder, updated after batch flush
                                            for target in &remote_targets {
                                                publish_batches.entry(*target)
                                                    .or_default()
                                                    .push((resp_idx, ch.clone(), msg.clone()));
                                            }
                                        }
                                    }
                                }
                                _ => responses.push(Frame::Error(Bytes::from_static(b"ERR invalid channel or message"))),
                            }
                        }
                        continue;
                    }

                    // --- SUBSCRIBE / PSUBSCRIBE ---
                    if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                        let is_pattern = cmd.eq_ignore_ascii_case(b"PSUBSCRIBE");
                        let cmd_name = if is_pattern { "psubscribe" } else { "subscribe" };
                        if cmd_args.is_empty() {
                            responses.push(Frame::Error(Bytes::from(format!(
                                "ERR wrong number of arguments for '{}' command", cmd_name
                            ))));
                            continue;
                        }
                        // Allocate pubsub channel if not yet created
                        if conn.pubsub_tx.is_none() {
                            let (tx, rx) = channel::mpsc_bounded::<Bytes>(256);
                            conn.pubsub_tx = Some(tx);
                            conn.pubsub_rx = Some(rx);
                        }
                        if conn.subscriber_id == 0 {
                            conn.subscriber_id = crate::pubsub::next_subscriber_id();
                        }
                        // Flush accumulated responses before entering subscriber mode
                        if !responses.is_empty() {
                            write_buf.clear();
                            for resp in &responses {
                                if conn.protocol_version >= 3 {
                                    crate::protocol::serialize_resp3(resp, &mut write_buf);
                                } else {
                                    crate::protocol::serialize(resp, &mut write_buf);
                                }
                            }
                            if stream.write_all(&write_buf).await.is_err() { return (HandlerResult::Done, None); }
                            responses.clear();
                        }
                        // Process subscribe arguments
                        for arg in cmd_args {
                            if let Some(ch) = extract_bytes(arg) {
                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                let acl_deny = { ctx.acl_table.read().unwrap().check_channel_permission(&conn.current_user, ch.as_ref()) };
                                if let Some(reason) = acl_deny {
                                    write_buf.clear();
                                    let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                    crate::protocol::serialize(&err, &mut write_buf);
                                    if stream.write_all(&write_buf).await.is_err() { return (HandlerResult::Done, None); }
                                    continue;
                                }
                                #[allow(clippy::unwrap_used)] // conn.pubsub_tx is set to Some just above before this loop
                                let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
                                if is_pattern {
                                    { ctx.pubsub_registry.write().psubscribe(ch.clone(), sub); }
                                } else {
                                    { ctx.pubsub_registry.write().subscribe(ch.clone(), sub); }
                                }
                                conn.subscription_count += 1;
                                // Register pub/sub affinity for this client IP
                                if conn.subscription_count == 1 {
                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                        ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                    }
                                }
                                propagate_subscription(&ctx.all_remote_sub_maps, &ch, ctx.shard_id, ctx.num_shards, is_pattern);
                                write_buf.clear();
                                let resp = if is_pattern {
                                    pubsub::psubscribe_response(&ch, conn.subscription_count)
                                } else {
                                    pubsub::subscribe_response(&ch, conn.subscription_count)
                                };
                                crate::protocol::serialize(&resp, &mut write_buf);
                                if stream.write_all(&write_buf).await.is_err() { return (HandlerResult::Done, None); }
                            }
                        }
                        break; // break out of frame batch loop to re-enter main loop in subscriber mode
                    }
                    // UNSUBSCRIBE/PUNSUBSCRIBE in normal mode (not subscribed)
                    if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                        let is_pattern = cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE");
                        let resp = if is_pattern {
                            pubsub::punsubscribe_response(&Bytes::from_static(b""), 0)
                        } else {
                            pubsub::unsubscribe_response(&Bytes::from_static(b""), 0)
                        };
                        responses.push(resp);
                        continue;
                    }

                    // --- PUBSUB introspection (zero-SPSC: direct shared-read) ---
                    if cmd.eq_ignore_ascii_case(b"PUBSUB") {
                        if cmd_args.is_empty() {
                            responses.push(Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'pubsub' command")));
                            continue;
                        }
                        let subcmd = extract_bytes(&cmd_args[0]);
                        match subcmd {
                            Some(ref sc) if sc.eq_ignore_ascii_case(b"CHANNELS") => {
                                let pattern = if cmd_args.len() > 1 { extract_bytes(&cmd_args[1]) } else { None };
                                let mut all_channels: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
                                for reg in &ctx.all_pubsub_registries {
                                    let guard = reg.read();
                                    all_channels.extend(guard.active_channels(pattern.as_deref()));
                                }
                                let arr: Vec<Frame> = all_channels.into_iter().map(Frame::BulkString).collect();
                                responses.push(Frame::Array(arr.into()));
                            }
                            Some(ref sc) if sc.eq_ignore_ascii_case(b"NUMSUB") => {
                                let channels: Vec<Bytes> = cmd_args[1..].iter().filter_map(|a| extract_bytes(a)).collect();
                                let mut counts: HashMap<Bytes, i64> = HashMap::new();
                                for reg in &ctx.all_pubsub_registries {
                                    let guard = reg.read();
                                    for (ch, c) in guard.numsub(&channels) {
                                        *counts.entry(ch).or_insert(0) += c;
                                    }
                                }
                                let mut arr = Vec::with_capacity(channels.len() * 2);
                                for ch in &channels {
                                    arr.push(Frame::BulkString(ch.clone()));
                                    arr.push(Frame::Integer(*counts.get(ch).unwrap_or(&0)));
                                }
                                responses.push(Frame::Array(arr.into()));
                            }
                            Some(ref sc) if sc.eq_ignore_ascii_case(b"NUMPAT") => {
                                let mut total: usize = 0;
                                for reg in &ctx.all_pubsub_registries {
                                    total += reg.read().numpat();
                                }
                                responses.push(Frame::Integer(total as i64));
                            }
                            _ => {
                                responses.push(Frame::Error(Bytes::from_static(b"ERR unknown subcommand or wrong number of arguments for 'pubsub' command")));
                            }
                        }
                        continue;
                    }

                    // --- MULTI queue mode ---
                    if conn.in_multi {
                        conn.command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                        continue;
                    }

                    // --- BGSAVE ---
                    if cmd.eq_ignore_ascii_case(b"BGSAVE") {
                        responses.push(crate::command::persistence::bgsave_start_sharded(&ctx.snapshot_trigger_tx, ctx.num_shards));
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"SAVE") {
                        responses.push(Frame::Error(Bytes::from_static(b"ERR SAVE not supported in sharded mode, use BGSAVE")));
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"LASTSAVE") {
                        responses.push(crate::command::persistence::handle_lastsave());
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"BGREWRITEAOF") {
                        if let Some(ref tx) = ctx.aof_tx {
                            responses.push(crate::command::persistence::bgrewriteaof_start_sharded(
                                tx,
                                ctx.shard_databases.clone(),
                            ));
                        } else {
                            responses.push(Frame::Error(Bytes::from_static(
                                b"ERR AOF is not enabled",
                            )));
                        }
                        continue;
                    }

                    // --- Cross-shard aggregation: KEYS, SCAN, DBSIZE ---
                    if cmd.eq_ignore_ascii_case(b"KEYS") {
                        let response = crate::shard::coordinator::coordinate_keys(cmd_args, ctx.shard_id, ctx.num_shards, conn.selected_db, &ctx.shard_databases, &ctx.dispatch_tx, &ctx.spsc_notifiers, &ctx.cached_clock, &()).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"SCAN") {
                        let response = crate::shard::coordinator::coordinate_scan(cmd_args, ctx.shard_id, ctx.num_shards, conn.selected_db, &ctx.shard_databases, &ctx.dispatch_tx, &ctx.spsc_notifiers, &ctx.cached_clock, &()).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                        let response = crate::shard::coordinator::coordinate_dbsize(ctx.shard_id, ctx.num_shards, conn.selected_db, &ctx.shard_databases, &ctx.dispatch_tx, &ctx.spsc_notifiers, &()).await;
                        responses.push(response);
                        continue;
                    }

                    // --- FT.* vector search commands ---
                    if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
                        if ctx.num_shards > 1 {
                            // Multi-shard: dispatch via SPSC
                            if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                let response = match crate::command::vector_search::parse_ft_search_args(cmd_args) {
                                    Ok((index_name, query_blob, k, filter, _offset, _count)) => {
                                        if filter.is_some() {
                                            Frame::Error(Bytes::from_static(
                                                b"ERR FILTER not supported in multi-shard mode yet",
                                            ))
                                        } else {
                                            crate::shard::coordinator::scatter_vector_search_remote(
                                                index_name, query_blob, k,
                                                ctx.shard_id, ctx.num_shards,
                                                &ctx.shard_databases,
                                                &ctx.dispatch_tx, &ctx.spsc_notifiers,
                                            ).await
                                        }
                                    }
                                    Err(err_frame) => err_frame,
                                };
                                responses.push(response);
                                continue;
                            }
                            let response = crate::shard::coordinator::broadcast_vector_command(
                                std::sync::Arc::new(frame),
                                ctx.shard_id, ctx.num_shards,
                                &ctx.shard_databases,
                                &ctx.dispatch_tx, &ctx.spsc_notifiers,
                            ).await;
                            responses.push(response);
                            continue;
                        } else {
                            // Single-shard: no SPSC channels available.
                            // Dispatch directly to shard's VectorStore via shared access.
                            let response = {
                                let shard_databases_ref = &ctx.shard_databases;
                                let mut vs = shard_databases_ref.vector_store(ctx.shard_id);
                                if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                                    crate::command::vector_search::ft_create(&mut vs, cmd_args)
                                } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                    let has_session = cmd_args.iter().any(|a| {
                                        if let Frame::BulkString(b) = a { b.eq_ignore_ascii_case(b"SESSION") } else { false }
                                    });
                                    if has_session {
                                        let mut db_guard = shard_databases_ref.write_db(ctx.shard_id, 0);
                                        crate::command::vector_search::ft_search(&mut vs, cmd_args, Some(&mut *db_guard))
                                    } else {
                                        crate::command::vector_search::ft_search(&mut vs, cmd_args, None)
                                    }
                                } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                                    crate::command::vector_search::ft_dropindex(&mut vs, cmd_args)
                                } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                                    crate::command::vector_search::ft_info(&vs, cmd_args)
                                } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                                    crate::command::vector_search::ft_list(&vs)
                                } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                                    crate::command::vector_search::ft_compact(&mut vs, cmd_args)
                                } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                                    crate::command::vector_search::cache_search::ft_cachesearch(&mut vs, cmd_args)
                                } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                                    crate::command::vector_search::ft_config(&mut vs, cmd_args)
                                } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                    let mut db_guard = shard_databases_ref.write_db(ctx.shard_id, 0);
                                    crate::command::vector_search::recommend::ft_recommend(&mut vs, cmd_args, Some(&mut *db_guard))
                                } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                    #[cfg(feature = "graph")]
                                    {
                                        let graph_guard = shard_databases_ref.graph_store_read(ctx.shard_id);
                                        crate::command::vector_search::navigate::ft_navigate(&mut vs, Some(&graph_guard), cmd_args, None)
                                    }
                                    #[cfg(not(feature = "graph"))]
                                    {
                                        Frame::Error(Bytes::from_static(b"ERR FT.NAVIGATE requires graph feature"))
                                    }
                                } else if cmd.eq_ignore_ascii_case(b"FT.EXPAND") {
                                    #[cfg(feature = "graph")]
                                    {
                                        let graph_guard = shard_databases_ref.graph_store_read(ctx.shard_id);
                                        crate::command::vector_search::ft_expand(&graph_guard, cmd_args)
                                    }
                                    #[cfg(not(feature = "graph"))]
                                    {
                                        Frame::Error(Bytes::from_static(b"ERR FT.EXPAND requires graph feature"))
                                    }
                                } else {
                                    Frame::Error(Bytes::from_static(b"ERR unknown FT.* command"))
                                }
                            };
                            responses.push(response);
                            continue;
                        }
                    }

                    // --- GRAPH.* graph commands ---
                    #[cfg(feature = "graph")]
                    if cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
                        let (response, wal_records) = if crate::command::graph::is_graph_write_cmd(cmd)
                            || (cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
                                && crate::command::graph::is_cypher_write_query(cmd_args))
                        {
                            let mut gs = ctx.shard_databases.graph_store_write(ctx.shard_id);
                            let resp = if cmd.eq_ignore_ascii_case(b"GRAPH.QUERY") {
                                crate::command::graph::graph_query_or_write(&mut gs, cmd_args)
                            } else {
                                crate::command::graph::dispatch_graph_write(&mut gs, cmd, cmd_args)
                            };
                            let records = gs.drain_wal();
                            (resp, records)
                        } else {
                            let gs = ctx.shard_databases.graph_store_read(ctx.shard_id);
                            let resp = crate::command::graph::dispatch_graph_read(&gs, cmd, cmd_args);
                            (resp, Vec::new())
                        };
                        for record in wal_records {
                            ctx.shard_databases.wal_append(ctx.shard_id, bytes::Bytes::from(record));
                        }
                        responses.push(response);
                        continue;
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
                        if metadata::is_write(cmd) {
                            // WRITE PATH: single lock acquisition for eviction + dispatch
                            let rt = ctx.runtime_config.read();
                            let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                            if let Err(oom_frame) = try_evict_if_needed(&mut guard, &rt) {
                                drop(guard);
                                drop(rt);
                                responses.push(oom_frame);
                                continue;
                            }
                            drop(rt);

                            let db_count = ctx.shard_databases.db_count();
                            guard.refresh_now_from_cache(&ctx.cached_clock);
                            let dispatch_start = std::time::Instant::now();
                            let result = dispatch(&mut guard, cmd, cmd_args, &mut conn.selected_db, db_count);
                            let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                            if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                crate::admin::metrics_setup::record_command(cmd_str, elapsed_us);
                            }
                            if let Frame::Array(ref args) = frame {
                                crate::admin::metrics_setup::global_slowlog().maybe_record(
                                    elapsed_us,
                                    args.as_slice(),
                                    peer_addr.as_bytes(),
                                    conn.client_name.as_ref().map_or(b"" as &[u8], |n| n.as_ref()),
                                );
                            }
                            let response = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => { should_quit = true; f }
                            };
                            if matches!(response, Frame::Error(_)) {
                                if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                    crate::admin::metrics_setup::record_command_error(cmd_str);
                                }
                            } else {
                                let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH")
                                    || cmd.eq_ignore_ascii_case(b"LMOVE") || cmd.eq_ignore_ascii_case(b"ZADD");
                                if needs_wake {
                                    if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                        let mut reg = ctx.blocking_registry.borrow_mut();
                                        if cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH") || cmd.eq_ignore_ascii_case(b"LMOVE") {
                                            crate::blocking::wakeup::try_wake_list_waiter(&mut reg, &mut guard, conn.selected_db, &key);
                                        } else {
                                            crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, &mut guard, conn.selected_db, &key);
                                        }
                                    }
                                }
                            }
                            drop(guard);
                            // Auto-index vectors on successful HSET (local write path)
                            // Placed AFTER drop(guard) to avoid DB→vector_store lock order
                            // inversion with the shard event loop (vector_store→DB).
                            if !matches!(response, Frame::Error(_))
                                && (cmd.eq_ignore_ascii_case(b"HSET") || cmd.eq_ignore_ascii_case(b"HMSET"))
                            {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                    crate::shard::spsc_handler::auto_index_hset_public(&mut vs, &key, cmd_args);
                                }
                            }
                            // Auto-delete vectors on DEL/UNLINK (local write path)
                            // Note: HDEL removes fields, not keys — it should NOT trigger
                            // vector deletion unless the entire key is removed.
                            if !matches!(response, Frame::Error(_))
                                && (cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK"))
                            {
                                let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                for arg in cmd_args.iter() {
                                    if let Some(key) = extract_bytes(arg) {
                                        vs.mark_deleted_for_key(key.as_ref());
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
                            let response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                            responses.push(response);
                        } else {
                            // READ PATH: shared lock — no contention with other shards' reads
                            let guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
                            let now_ms = ctx.cached_clock.ms();
                            let db_count = ctx.shard_databases.db_count();
                            let dispatch_start = std::time::Instant::now();
                            let result = dispatch_read(&guard, cmd, cmd_args, now_ms, &mut conn.selected_db, db_count);
                            let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                            if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                crate::admin::metrics_setup::record_command(cmd_str, elapsed_us);
                            }
                            if let Frame::Array(ref args) = frame {
                                crate::admin::metrics_setup::global_slowlog().maybe_record(
                                    elapsed_us,
                                    args.as_slice(),
                                    peer_addr.as_bytes(),
                                    conn.client_name.as_ref().map_or(b"" as &[u8], |n| n.as_ref()),
                                );
                            }
                            drop(guard);
                            let response = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => { should_quit = true; f }
                            };
                            if matches!(response, Frame::Error(_)) {
                                if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                    crate::admin::metrics_setup::record_command_error(cmd_str);
                                }
                            }
                            if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    ctx.tracking_table.borrow_mut().track_key(client_id, &key, conn.tracking_state.noloop);
                                }
                            }
                            let response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                            responses.push(response);
                        }
                    } else if let Some(target) = target_shard {
                        // SHARED-READ FAST PATH: cross-shard reads bypass SPSC dispatch entirely.
                        // By this point conn.in_multi is false (MULTI queuing happens earlier with `continue`).
                        // Read commands execute directly on the target shard's database via RwLock read guard,
                        // avoiding ~88us of two async scheduling hops through the SPSC channel.
                        //
                        // Guard: if there are already pending writes for this target shard in the
                        // current pipeline batch, we must NOT take the fast path -- the read would
                        // execute before the deferred writes, violating command ordering. Fall through
                        // to SPSC dispatch to preserve pipeline semantics.
                        if !metadata::is_write(cmd) && !remote_groups.contains_key(&target) {
                            let guard = ctx.shard_databases.read_db(target, conn.selected_db);
                            let now_ms = ctx.cached_clock.ms();
                            let db_count = ctx.shard_databases.db_count();
                            let result = dispatch_read(&guard, cmd, cmd_args, now_ms, &mut conn.selected_db, db_count);
                            drop(guard);
                            let response = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => { should_quit = true; f }
                            };
                            if matches!(response, Frame::Error(_)) {
                                if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                                    crate::admin::metrics_setup::record_command_error(cmd_str);
                                }
                            }
                            // Client tracking for cross-shard reads
                            if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    ctx.tracking_table.borrow_mut().track_key(client_id, &key, conn.tracking_state.noloop);
                                }
                            }
                            let response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                            responses.push(response);
                            continue;
                        }
                        // Cross-shard write: deferred SPSC dispatch (unchanged)
                        let resp_idx = responses.len();
                        responses.push(Frame::Null);
                        // Zero-copy: extract Bytes from frame's first element (refcount bump, no alloc).
                        let cmd_bytes = if let Frame::Array(ref args) = frame {
                            extract_bytes(&args[0]).unwrap_or_default()
                        } else {
                            Bytes::new()
                        };
                        remote_groups.entry(target).or_default().push((resp_idx, std::sync::Arc::new(frame), aof_bytes, cmd_bytes, conn.selected_db));
                    }
                }

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
        // Remove affinity on disconnect (no subscriptions remain)
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            ctx.pubsub_affinity.write().remove(&addr.ip());
        }
    }

    if conn.tracking_state.enabled {
        ctx.tracking_table.borrow_mut().untrack_all(client_id);
    }

    (HandlerResult::Done, None)
}
