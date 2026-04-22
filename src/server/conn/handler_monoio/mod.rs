// Note: some imports/variables may be conditionally used across feature flags
//! Monoio connection handler using ownership-based I/O (AsyncReadRent/AsyncWriteRent).
//!
//! Extracted from `server/connection.rs` (Plan 48-02).

mod dispatch;
mod ft;
mod pubsub;
mod read;
mod txn;
mod write;

use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use bytes::{Bytes, BytesMut};
use ringbuf::traits::Producer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read, is_dispatch_read_supported};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::shard::dispatch::key_to_shard;
use crate::shard::mesh::ChannelMesh;
use crate::storage::eviction::{try_evict_if_needed, try_evict_if_needed_async_spill};
use crate::workspace::{strip_workspace_prefix_from_response, workspace_rewrite_args};

use super::affinity::MigratedConnectionState;
use super::{
    apply_resp3_conversion, convert_blocking_to_nonblocking, execute_transaction_sharded,
    extract_bytes, extract_command, extract_primary_key, handle_blocking_command_monoio,
    handle_config, is_multi_key_command, propagate_subscription, try_inline_dispatch_loop,
    unpropagate_subscription,
};
use crate::framevec;
use crate::pubsub::subscriber::Subscriber;
use crate::server::codec::RespCodec;
use crate::shard::dispatch::ShardMessage;
// ResponseSlotPool NOT used on monoio — its AtomicWaker doesn't cross
// monoio's single-threaded (!Send) executor boundary. Use oneshot channels.

/// Result of `handle_connection_sharded_monoio` execution.
///
/// Same purpose as the Tokio handler's `HandlerResult`: the generic handler cannot
/// perform FD extraction, so it returns the stream when migration is triggered.
#[cfg(feature = "runtime-monoio")]
pub enum MonoioHandlerResult {
    /// Normal connection close.
    Done,
    /// Migration triggered: caller should extract raw FD and send via SPSC.
    MigrateConnection {
        state: MigratedConnectionState,
        target_shard: usize,
    },
}

/// Monoio connection handler using ownership-based I/O (AsyncReadRent/AsyncWriteRent).
/// Dispatches commands through `crate::command::dispatch()` with monoio's ownership I/O model.
#[cfg(feature = "runtime-monoio")]
#[tracing::instrument(skip_all, level = "debug")]
pub(crate) async fn handle_connection_sharded_monoio<
    S: monoio::io::AsyncReadRent + monoio::io::AsyncWriteRent,
>(
    mut stream: S,
    peer_addr: String,
    ctx: &super::core::ConnectionContext,
    shutdown: CancellationToken,
    client_id: u64,
    can_migrate: bool,
    initial_read_buf: BytesMut,
    pending_wakers: Rc<RefCell<Vec<std::task::Waker>>>,
    migrated_state: Option<&MigratedConnectionState>,
) -> (MonoioHandlerResult, Option<S>) {
    use monoio::io::AsyncWriteRentExt;

    // NOTE: do NOT call record_connection_opened() here — the caller
    // (conn_accept.rs) already increments via try_accept_connection().

    let mut read_buf = if initial_read_buf.is_empty() {
        BytesMut::with_capacity(8192)
    } else {
        let mut buf = initial_read_buf;
        buf.reserve(8192);
        buf
    };
    let mut write_buf = BytesMut::with_capacity(8192);
    let mut codec = RespCodec::default();
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
    let db_count = ctx.shard_databases.db_count();

    // Register in global client registry for CLIENT LIST/INFO/KILL.
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

    // Functions API registry (per-connection, lazy init) — kept as local because Rc<RefCell<>> is !Send
    let func_registry = Rc::new(RefCell::new(crate::scripting::FunctionRegistry::new()));

    // Pre-allocate read buffer outside the loop to avoid per-read heap allocation.
    // Monoio's ownership I/O takes ownership and returns the buffer, so we reassign.
    let mut tmp_buf = vec![0u8; 8192];

    // Client idle timeout: 0 = disabled (read once, avoid lock on hot path)
    let idle_timeout_secs = ctx.runtime_config.read().timeout;
    let idle_timeout = if idle_timeout_secs > 0 {
        Some(std::time::Duration::from_secs(idle_timeout_secs))
    } else {
        None
    };

    // Pre-allocate batch containers outside the loop to avoid per-batch heap allocation.
    // These are cleared and reused each iteration instead of being recreated.
    let mut responses: Vec<Frame> = Vec::with_capacity(64);
    let mut remote_groups: HashMap<
        usize,
        Vec<(usize, std::sync::Arc<Frame>, Option<Bytes>, Bytes)>,
    > = HashMap::with_capacity(ctx.num_shards);
    let mut reply_futures: Vec<(Vec<(usize, Option<Bytes>, Bytes)>, usize)> =
        Vec::with_capacity(ctx.num_shards);

    // Pre-allocate frames Vec outside the loop; reused via .clear() each iteration.
    let mut frames: Vec<Frame> = Vec::with_capacity(64);

    loop {
        // Check if CLIENT KILL targeted this connection
        if crate::client_registry::is_killed(client_id) {
            break;
        }

        // Subscriber mode: bidirectional select on client commands + published messages
        if conn.subscription_count > 0 {
            #[allow(clippy::unwrap_used)]
            // conn.pubsub_rx is always Some when conn.subscription_count > 0
            let rx = conn.pubsub_rx.as_ref().unwrap();
            let sub_tmp_buf = vec![0u8; 8192];
            monoio::select! {
                read_result = stream.read(sub_tmp_buf) => {
                    let (result, buf) = read_result;
                    match result {
                        Ok(0) => {
                            // Client half-closed — break out of loop.
                            // Stream drop (end of function) triggers monoio's cleanup.
                            break;
                        }
                        Ok(n) => {
                            read_buf.extend_from_slice(&buf[..n]);
                            // Parse frames from buffer
                            loop {
                                match codec.decode_frame(&mut read_buf) {
                                    Ok(Some(frame)) => {
                                        if let Some((cmd, cmd_args)) = extract_command(&frame) {
                                            match cmd {
                                                _ if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'subscribe' command"));
                                                        let mut resp_buf = BytesMut::new();
                                                        codec.encode_frame(&err, &mut resp_buf);
                                                        let data = resp_buf.freeze();
                                                        let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                        if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        continue;
                                                    }
                                                    for arg in cmd_args {
                                                        if let Some(channel) = extract_bytes(arg) {
                                                            // ACL channel permission check
                                                            let denied = {
                                                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                                let acl_guard = ctx.acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&conn.current_user, channel.as_ref())
                                                            };
                                                            if let Some(deny_reason) = denied {
                                                                let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&err, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                                continue;
                                                            }
                                                            #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some when in subscriber mode
                                                            let sub = Subscriber::with_protocol(
                                                                conn.pubsub_tx.clone().unwrap(),
                                                                conn.subscriber_id,
                                                                conn.protocol_version >= 3,
                                                            );
                                                            ctx.pubsub_registry.write().subscribe(channel.clone(), sub);
                                                            propagate_subscription(&ctx.all_remote_sub_maps, &channel, ctx.shard_id, ctx.num_shards, false);
                                                            conn.subscription_count += 1;
                                                            // Register pub/sub affinity for this client IP
                                                            if conn.subscription_count == 1 {
                                                                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                                    ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                                }
                                                            }
                                                            let resp = crate::pubsub::subscribe_response(&channel, conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let removed = ctx.pubsub_registry.write().unsubscribe_all(conn.subscriber_id);
                                                        for ch in &removed {
                                                            unpropagate_subscription(&ctx.all_remote_sub_maps, ch, ctx.shard_id, ctx.num_shards, false);
                                                        }
                                                        if removed.is_empty() {
                                                            conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                                            let resp = crate::pubsub::unsubscribe_response(&Bytes::from_static(b""), conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for ch in &removed {
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::unsubscribe_response(ch, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    } else {
                                                        for arg in cmd_args {
                                                            if let Some(channel) = extract_bytes(arg) {
                                                                ctx.pubsub_registry.write().unsubscribe(channel.as_ref(), conn.subscriber_id);
                                                                unpropagate_subscription(&ctx.all_remote_sub_maps, &channel, ctx.shard_id, ctx.num_shards, false);
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::unsubscribe_response(&channel, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'psubscribe' command"));
                                                        let mut resp_buf = BytesMut::new();
                                                        codec.encode_frame(&err, &mut resp_buf);
                                                        let data = resp_buf.freeze();
                                                        let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                        if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        continue;
                                                    }
                                                    for arg in cmd_args {
                                                        if let Some(pattern) = extract_bytes(arg) {
                                                            // ACL channel permission check
                                                            let denied = {
                                                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                                let acl_guard = ctx.acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&conn.current_user, pattern.as_ref())
                                                            };
                                                            if let Some(deny_reason) = denied {
                                                                let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&err, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                                continue;
                                                            }
                                                            #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some when in subscriber mode
                                                            let sub = Subscriber::with_protocol(
                                                                conn.pubsub_tx.clone().unwrap(),
                                                                conn.subscriber_id,
                                                                conn.protocol_version >= 3,
                                                            );
                                                            ctx.pubsub_registry.write().psubscribe(pattern.clone(), sub);
                                                            propagate_subscription(&ctx.all_remote_sub_maps, &pattern, ctx.shard_id, ctx.num_shards, true);
                                                            conn.subscription_count += 1;
                                                            // Register pub/sub affinity for this client IP
                                                            if conn.subscription_count == 1 {
                                                                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                                    ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                                }
                                                            }
                                                            let resp = crate::pubsub::psubscribe_response(&pattern, conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let removed = ctx.pubsub_registry.write().punsubscribe_all(conn.subscriber_id);
                                                        for pat in &removed {
                                                            unpropagate_subscription(&ctx.all_remote_sub_maps, pat, ctx.shard_id, ctx.num_shards, true);
                                                        }
                                                        if removed.is_empty() {
                                                            conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                                            let resp = crate::pubsub::punsubscribe_response(&Bytes::from_static(b""), conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for pat in &removed {
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::punsubscribe_response(pat, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    } else {
                                                        for arg in cmd_args {
                                                            if let Some(pattern) = extract_bytes(arg) {
                                                                ctx.pubsub_registry.write().punsubscribe(pattern.as_ref(), conn.subscriber_id);
                                                                unpropagate_subscription(&ctx.all_remote_sub_maps, &pattern, ctx.shard_id, ctx.num_shards, true);
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::punsubscribe_response(&pattern, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"PING") => {
                                                    let resp = Frame::Array(framevec![
                                                        Frame::BulkString(Bytes::from_static(b"pong")),
                                                        Frame::BulkString(Bytes::from_static(b"")),
                                                    ]);
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&resp, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"QUIT") => {
                                                    let resp = Frame::SimpleString(Bytes::from_static(b"OK"));
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&resp, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    let _ = wr; // ignore write error on quit
                                                    return (MonoioHandlerResult::Done, None); // exit connection
                                                }
                                                _ => {
                                                    let cmd_str = String::from_utf8_lossy(cmd);
                                                    let err = Frame::Error(Bytes::from(format!(
                                                        "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context",
                                                        cmd_str.to_lowercase()
                                                    )));
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&err, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => break, // need more data
                                    Err(_) => return (MonoioHandlerResult::Done, None),  // parse error
                                }
                            }
                        }
                        Err(_) => break, // connection error
                    }
                }
                msg = rx.recv_async() => {
                    match msg {
                        Ok(data) => {
                            // Data is pre-serialized RESP bytes — write directly
                            let (result, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                            if result.is_err() { break; }
                        }
                        Err(_) => break, // all senders dropped
                    }
                }
                _ = shutdown.cancelled() => { break; }
            }
            continue;
        }

        // Read data from stream using monoio ownership I/O.
        // Reuse pre-allocated buffer; restore length to 8192 for the read.
        if tmp_buf.len() < 8192 {
            tmp_buf.resize(8192, 0);
        }
        if let Some(dur) = idle_timeout {
            // Timeout-aware read: select between read and sleep.
            // monoio::select! drops the losing future, so tmp_buf ownership transfers.
            // We allocate a fresh buffer when timeout is enabled (safety feature, not hot path).
            let timeout_buf = std::mem::take(&mut tmp_buf);
            monoio::select! {
                read_result = stream.read(timeout_buf) => {
                    let (result, returned_buf) = read_result;
                    tmp_buf = returned_buf;
                    match result {
                        Ok(0) => break,
                        Ok(n) => { read_buf.extend_from_slice(&tmp_buf[..n]); }
                        Err(_) => break,
                    }
                }
                _ = monoio::time::sleep(dur) => {
                    tracing::debug!("Connection {} idle timeout ({}s)", client_id, idle_timeout_secs);
                    break;
                }
            }
        } else {
            let (result, returned_buf) = stream.read(tmp_buf).await;
            tmp_buf = returned_buf;
            match result {
                Ok(0) => break,
                Ok(n) => {
                    read_buf.extend_from_slice(&tmp_buf[..n]);
                }
                Err(_) => break,
            }
        }

        // Inline dispatch: GET/SET directly from raw bytes, skipping Frame construction.
        // Skip when unauthenticated or workspace-bound (prefix injection in normal path only).
        if conn.authenticated && conn.workspace_id.is_none() {
            // Inline writes safe only when: ACL unrestricted, !in_multi, !tracking,
            // !is_replica, no spill_sender. Replica check is non-blocking try_read.
            let is_replica = ctx.repl_state.as_ref().is_some_and(|rs| {
                rs.try_read().is_ok_and(|g| {
                    matches!(
                        g.role,
                        crate::replication::state::ReplicationRole::Replica { .. }
                    )
                })
            });
            let can_inline_writes = conn.acl_skip_allowed()
                && !conn.in_multi
                && !conn.tracking_state.enabled
                && !is_replica
                && ctx.spill_sender.is_none();
            let inlined = try_inline_dispatch_loop(
                &mut read_buf,
                &mut write_buf,
                &ctx.shard_databases,
                ctx.shard_id,
                conn.selected_db,
                &ctx.aof_tx,
                ctx.cached_clock.ms(),
                ctx.num_shards,
                can_inline_writes,
                &ctx.runtime_config,
            );
            crate::admin::metrics_setup::record_dispatch_local_inline(inlined as u64);
            if inlined > 0 && read_buf.is_empty() {
                // All commands were inlined -- flush write_buf and continue
                if !write_buf.is_empty() {
                    let data = write_buf.split().freeze();
                    let (result, _): (std::io::Result<usize>, bytes::Bytes) =
                        stream.write_all(data).await;
                    if result.is_err() {
                        break;
                    }
                }
                continue;
            }
            // If read_buf still has data, fall through to normal Frame parsing
            // for remaining commands. Inlined responses are already in write_buf.
        }

        // Parse all complete frames from the read buffer (reuse pre-allocated Vec, cap at 1024)
        frames.clear();
        loop {
            match codec.decode_frame(&mut read_buf) {
                Ok(Some(frame)) => {
                    frames.push(frame);
                    if frames.len() >= 1024 {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => return (MonoioHandlerResult::Done, None), // parse error, close connection
            }
        }

        if frames.is_empty() {
            continue;
        }

        // CLIENT PAUSE: delay processing if server is paused
        crate::client_pause::expire_if_needed();
        if let Some(remaining) = crate::client_pause::check_pause(true) {
            monoio::time::sleep(remaining).await;
        }

        // Process frames (do NOT clear write_buf -- may have inline dispatch responses).
        let mut should_quit = false;
        responses.clear();
        remote_groups.clear();
        let mut publish_batches: std::collections::HashMap<usize, Vec<(usize, Bytes, Bytes)>> =
            std::collections::HashMap::new();

        // Refresh time once per batch — sub-millisecond accuracy not needed per-command.
        {
            let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
            guard.refresh_now_from_cache(&ctx.cached_clock);
        }

        let mut auth_delay_ms: u64 = 0;

        for frame in frames.drain(..) {
            // --- AUTH gate ---
            match dispatch::check_auth_gate(
                &frame,
                &mut conn,
                ctx,
                &peer_addr,
                client_id,
                &mut responses,
                &mut auth_delay_ms,
            ) {
                dispatch::AuthGateResult::Consumed => continue,
                dispatch::AuthGateResult::Quit => {
                    should_quit = true;
                    break;
                }
                dispatch::AuthGateResult::NotAuth => {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"NOAUTH Authentication required.",
                    )));
                    continue;
                }
                dispatch::AuthGateResult::Authenticated => {}
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
            // --- Connection-level commands (dispatched to dispatch.rs) ---
            if dispatch::try_handle_cluster(cmd, cmd_args, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_handle_evalsha(cmd, cmd_args, &conn, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_handle_eval(cmd, cmd_args, &conn, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_handle_script(cmd, cmd_args, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_handle_cluster_routing(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_handle_auth(
                cmd,
                cmd_args,
                &mut conn,
                ctx,
                &peer_addr,
                &mut auth_delay_ms,
                &mut responses,
            ) {
                continue;
            }
            if dispatch::try_handle_hello(
                cmd,
                cmd_args,
                &mut conn,
                ctx,
                client_id,
                &peer_addr,
                &mut auth_delay_ms,
                &mut responses,
            ) {
                continue;
            }
            if dispatch::try_handle_acl(cmd, cmd_args, &mut conn, ctx, &peer_addr, &mut responses) {
                continue;
            }
            if dispatch::try_handle_config(cmd, cmd_args, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_handle_replicaof(cmd, cmd_args, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_handle_replconf(cmd, cmd_args, &mut responses) {
                continue;
            }
            if dispatch::try_handle_info(cmd, cmd_args, &conn, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_enforce_readonly(cmd, ctx, &mut responses) {
                continue;
            }
            // CLIENT early (ID, SETNAME, GETNAME, TRACKING) -- admin subcmds fall through to ACL gate
            if dispatch::try_handle_client_early(
                cmd,
                cmd_args,
                client_id,
                &mut conn,
                ctx,
                &mut responses,
            ) {
                continue;
            }
            // --- Pub/sub commands ---
            if pubsub::try_handle_publish(
                cmd,
                cmd_args,
                &conn,
                ctx,
                &mut responses,
                &mut publish_batches,
            ) {
                continue;
            }
            match pubsub::try_handle_subscribe_entry(
                cmd,
                cmd_args,
                &mut conn,
                ctx,
                &peer_addr,
                &mut responses,
                &mut codec,
                &mut write_buf,
                &mut stream,
            )
            .await
            {
                pubsub::SubscribeResult::NotSubscribe => {}
                pubsub::SubscribeResult::ArgError => continue,
                pubsub::SubscribeResult::Subscribed => break,
                pubsub::SubscribeResult::WriteError => return (MonoioHandlerResult::Done, None),
            }
            if pubsub::try_handle_unsubscribe(cmd, &mut responses) {
                continue;
            }
            if pubsub::try_handle_pubsub_introspection(cmd, cmd_args, ctx, &mut responses) {
                continue;
            }
            // --- Persistence + ACL gate + CLIENT admin + Functions ---
            if dispatch::try_handle_persistence(cmd, ctx, &mut responses) {
                continue;
            }
            if dispatch::try_enforce_acl(cmd, cmd_args, &mut conn, ctx, &peer_addr, &mut responses)
            {
                continue;
            }
            if dispatch::try_handle_client_admin(cmd, cmd_args, client_id, &conn, &mut responses) {
                continue;
            }
            if dispatch::try_handle_functions(
                cmd,
                cmd_args,
                &conn,
                ctx,
                &func_registry,
                &mut responses,
            ) {
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

            // --- MULTI / EXEC / DISCARD ---
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
            match dispatch::try_handle_blocking(
                cmd,
                cmd_args,
                &mut conn,
                ctx,
                &mut responses,
                &mut codec,
                &mut write_buf,
                &mut stream,
                &shutdown,
            )
            .await
            {
                dispatch::BlockingResult::NotBlocking => {}
                dispatch::BlockingResult::Queued => continue,
                dispatch::BlockingResult::Handled => break,
                dispatch::BlockingResult::WriteError => return (MonoioHandlerResult::Done, None),
            }

            // --- MULTI queue mode: queue commands when in transaction ---
            if conn.in_multi {
                conn.command_queue.push(frame);
                responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                continue;
            }

            // --- Cross-shard aggregation commands: KEYS, SCAN, DBSIZE + multi-key ---
            if dispatch::try_handle_cross_shard_commands(cmd, cmd_args, &conn, ctx, &mut responses)
                .await
            {
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

            // --- Routing: keyless, local, or remote ---
            let target_shard =
                extract_primary_key(cmd, cmd_args).map(|key| key_to_shard(key, ctx.num_shards));

            let is_local = match target_shard {
                None => true,
                Some(s) if s == ctx.shard_id => true,
                _ => false,
            };

            // Affinity sampling: record shard target for migration decision.
            // Migration is deferred until AFTER the current batch is fully processed.
            if let (Some(tracker), Some(target)) = (&mut conn.affinity_tracker, target_shard) {
                if let Some(migrate_to) = tracker.record(target) {
                    if !conn.in_multi
                        && conn.subscription_count == 0
                        && !conn.tracking_state.enabled
                    {
                        conn.migration_target = Some(migrate_to);
                    }
                }
            }

            // Pre-classify write commands for AOF + tracking
            let is_write = if ctx.aof_tx.is_some() || conn.tracking_state.enabled {
                metadata::is_write(cmd)
            } else {
                false
            };

            if is_local {
                crate::admin::metrics_setup::record_dispatch_local();
                if metadata::is_write(cmd) {
                    // WRITE PATH: eviction + dispatch under write lock.
                    let rt = ctx.runtime_config.read();
                    let mut guard = ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
                    let evict_result = if let Some(ref sender) = ctx.spill_sender {
                        let mut fid = ctx.spill_file_id.get();
                        let dir = ctx
                            .disk_offload_dir
                            .as_deref()
                            .unwrap_or(std::path::Path::new("."));
                        let res = try_evict_if_needed_async_spill(
                            &mut guard,
                            &rt,
                            sender,
                            dir,
                            &mut fid,
                            conn.selected_db,
                        );
                        ctx.spill_file_id.set(fid);
                        res
                    } else {
                        try_evict_if_needed(&mut guard, &rt)
                    };
                    if let Err(oom_frame) = evict_result {
                        drop(guard);
                        drop(rt);
                        responses.push(oom_frame);
                        continue;
                    }
                    drop(rt);

                    // KV undo-log capture for active cross-store transactions.
                    // MUST happen BEFORE dispatch() overwrites the database entry.
                    if let Some(ref mut txn) = conn.active_cross_txn {
                        if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
                            // Multi-key DEL: iterate all args for undo capture
                            for arg in cmd_args.iter() {
                                if let Frame::BulkString(key_bytes) = arg {
                                    if let Some(old_entry) = guard.get(key_bytes.as_ref()).cloned()
                                    {
                                        txn.kv_undo.record_delete(key_bytes.clone(), old_entry);
                                        let lsn = txn.snapshot_lsn;
                                        let tid = txn.txn_id;
                                        ctx.shard_databases.kv_intents(ctx.shard_id).record_write(
                                            key_bytes.clone(),
                                            lsn,
                                            tid,
                                        );
                                    }
                                    // Key not found: nothing to undo, no intent registered
                                }
                            }
                        } else if let Some(key) =
                            crate::server::conn::shared::extract_primary_key(cmd, cmd_args)
                        {
                            // SET / HSET / INCR / etc. — single primary key
                            let old_entry = guard.get(key.as_ref()).cloned();
                            let lsn = txn.snapshot_lsn;
                            let tid = txn.txn_id;
                            match old_entry {
                                None => txn.kv_undo.record_insert(key.clone()),
                                Some(entry) => txn.kv_undo.record_update(key.clone(), entry),
                            }
                            ctx.shard_databases.kv_intents(ctx.shard_id).record_write(
                                key.clone(),
                                lsn,
                                tid,
                            );
                        }
                    }

                    // 1-in-16 latency sampling: skips ~30-40ns `Instant::now()`
                    // on 15/16 of writes while keeping histogram + counter
                    // statistically accurate. Slowlog at default 10 ms threshold
                    // never fires on pipelined GET/SET regardless.
                    conn.cmd_counter = conn.cmd_counter.wrapping_add(1);
                    let sample_latency = (conn.cmd_counter & 0xF) == 0;
                    let dispatch_start = sample_latency.then(std::time::Instant::now);
                    let result =
                        dispatch(&mut guard, cmd, cmd_args, &mut conn.selected_db, db_count);
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
                                    conn.client_name
                                        .as_ref()
                                        .map_or(b"" as &[u8], |n| n.as_ref()),
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
                        DispatchResult::Quit(f) => {
                            should_quit = true;
                            f
                        }
                    };

                    // AOF logging for successful local writes
                    if !matches!(response, Frame::Error(_)) && is_write {
                        if let Some(ref tx) = ctx.aof_tx {
                            let serialized = aof::serialize_command(&frame);
                            let _ = tx.try_send(AofMessage::Append(serialized));
                        }
                    }

                    // Auto-index HSET into vector/text stores (if key matches index prefix)
                    if !matches!(response, Frame::Error(_)) && cmd.eq_ignore_ascii_case(b"HSET") {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            let inserted = {
                                let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                let mut ts = ctx.shard_databases.text_store(ctx.shard_id);
                                crate::shard::spsc_handler::auto_index_hset_public(
                                    &mut vs,
                                    &mut *ts,
                                    key.as_ref(),
                                    cmd_args,
                                )
                            };
                            // Phase 166 (Plan 02): if inside an active cross-store
                            // TXN, push one VectorIntent per (index_name, key_hash)
                            // so TXN.ABORT (Plan 166-03) can tombstone via
                            // MutableSegment::mark_deleted_by_key_hash. Non-TXN
                            // paths discard the tuples — auto_index_hset already
                            // performed the append.
                            if let Some(txn) = conn.active_cross_txn.as_mut() {
                                for (index_name, key_hash) in inserted {
                                    txn.record_vector(key_hash, index_name);
                                }
                            }
                        }
                    }

                    // Post-dispatch wakeup hooks for producer commands
                    if !matches!(response, Frame::Error(_)) {
                        let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                            || cmd.eq_ignore_ascii_case(b"RPUSH")
                            || cmd.eq_ignore_ascii_case(b"LMOVE")
                            || cmd.eq_ignore_ascii_case(b"ZADD");
                        if needs_wake {
                            if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                let mut reg = ctx.blocking_registry.borrow_mut();
                                if cmd.eq_ignore_ascii_case(b"LPUSH")
                                    || cmd.eq_ignore_ascii_case(b"RPUSH")
                                    || cmd.eq_ignore_ascii_case(b"LMOVE")
                                {
                                    crate::blocking::wakeup::try_wake_list_waiter(
                                        &mut reg,
                                        &mut guard,
                                        conn.selected_db,
                                        &key,
                                    );
                                } else {
                                    crate::blocking::wakeup::try_wake_zset_waiter(
                                        &mut reg,
                                        &mut guard,
                                        conn.selected_db,
                                        &key,
                                    );
                                }
                            }
                        }
                    }

                    drop(guard);

                    // Track key on write / invalidate tracked keys
                    if conn.tracking_state.enabled && !matches!(response, Frame::Error(_)) {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            let senders = ctx
                                .tracking_table
                                .borrow_mut()
                                .invalidate_key(&key, client_id);
                            if !senders.is_empty() {
                                let push = crate::tracking::invalidation::invalidation_push(&[key]);
                                for tx in senders {
                                    let _ = tx.try_send(push.clone());
                                }
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
                            if let Some(key) =
                                crate::server::conn::shared::extract_primary_key(cmd, cmd_args)
                            {
                                let snapshot_lsn = txn.snapshot_lsn;
                                let my_txn_id = txn.txn_id;
                                // Clone committed treemap to release vector_store lock
                                // before acquiring kv_intents lock (lock ordering).
                                let committed = {
                                    let vs = ctx.shard_databases.vector_store(ctx.shard_id);
                                    vs.txn_manager().committed_treemap().clone()
                                };
                                let visible = {
                                    let intents = ctx.shard_databases.kv_intents(ctx.shard_id);
                                    intents.is_key_visible(
                                        key.as_ref(),
                                        snapshot_lsn,
                                        my_txn_id,
                                        &committed,
                                    )
                                };
                                if !visible {
                                    responses.push(Frame::Null);
                                    continue;
                                }
                            }
                        }
                    }

                    // READ PATH: shared lock — no contention with other shards' reads
                    let guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
                    let now_ms = ctx.cached_clock.ms();
                    conn.cmd_counter = conn.cmd_counter.wrapping_add(1);
                    let sample_latency = (conn.cmd_counter & 0xF) == 0;
                    let dispatch_start = sample_latency.then(std::time::Instant::now);
                    let result = dispatch_read(
                        &guard,
                        cmd,
                        cmd_args,
                        now_ms,
                        &mut conn.selected_db,
                        db_count,
                    );
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
                                    conn.client_name
                                        .as_ref()
                                        .map_or(b"" as &[u8], |n| n.as_ref()),
                                );
                            }
                        } else {
                            crate::admin::metrics_setup::record_command_no_latency_cached(
                                cmd_str,
                                &mut conn.cached_metrics,
                            );
                        }
                    }
                    drop(guard);

                    let response = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => {
                            should_quit = true;
                            f
                        }
                    };

                    // Track key on local read
                    if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            ctx.tracking_table.borrow_mut().track_key(
                                client_id,
                                &key,
                                conn.tracking_state.noloop,
                            );
                        }
                    }
                    let mut response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                    if let Some(ws_id) = conn.workspace_id.as_ref() {
                        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                    }
                    responses.push(response);
                } // end read/write split

            // (tracking and response push handled inside read/write branches above)
            } else if let Some(target) = target_shard {
                // TXN cross-shard guard: reject cross-shard writes in active TXN (no undo log).
                if conn.in_cross_txn() && metadata::is_write(cmd) {
                    responses.push(Frame::Error(bytes::Bytes::from_static(
                        crate::command::transaction::ERR_TXN_CROSS_SHARD,
                    )));
                    continue;
                }
                // SHARED-READ FAST PATH: bypass SPSC for cross-shard reads.
                // Guard: skip if pending writes exist for this target (pipeline ordering).
                if !metadata::is_write(cmd)
                    && !remote_groups.contains_key(&target)
                    && is_dispatch_read_supported(cmd)
                {
                    crate::admin::metrics_setup::record_dispatch_cross_read_fastpath();
                    let guard = ctx.shard_databases.read_db(target, conn.selected_db);
                    let now_ms = ctx.cached_clock.ms();
                    let result = dispatch_read(
                        &guard,
                        cmd,
                        cmd_args,
                        now_ms,
                        &mut conn.selected_db,
                        db_count,
                    );
                    drop(guard);
                    let response = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => {
                            should_quit = true;
                            f
                        }
                    };
                    // Client tracking for cross-shard reads
                    if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            ctx.tracking_table.borrow_mut().track_key(
                                client_id,
                                &key,
                                conn.tracking_state.noloop,
                            );
                        }
                    }
                    let mut response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                    if let Some(ws_id) = conn.workspace_id.as_ref() {
                        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                    }
                    responses.push(response);
                    continue;
                }
                // Cross-shard write: deferred SPSC dispatch.
                // When workspace rewriting occurred, rebuild the frame with
                // prefixed args so the target shard stores the correct key.
                let dispatch_frame = if rewritten.is_some() {
                    let mut parts = Vec::with_capacity(1 + cmd_args.len());
                    parts.push(Frame::BulkString(Bytes::copy_from_slice(cmd)));
                    parts.extend_from_slice(cmd_args);
                    Frame::Array(parts.into())
                } else {
                    frame.clone()
                };
                let resp_idx = responses.len();
                responses.push(Frame::Null); // placeholder, filled after batch dispatch
                // Pre-compute AOF bytes before moving frame into Arc
                let aof_bytes = if ctx.aof_tx.is_some() && metadata::is_write(cmd) {
                    Some(aof::serialize_command(&dispatch_frame))
                } else {
                    None
                };
                let cmd_bytes = if let Frame::Array(ref args) = dispatch_frame {
                    extract_bytes(&args[0]).unwrap_or_default()
                } else {
                    Bytes::new()
                };
                remote_groups.entry(target).or_default().push((
                    resp_idx,
                    std::sync::Arc::new(dispatch_frame),
                    aof_bytes,
                    cmd_bytes,
                ));
                crate::admin::metrics_setup::record_dispatch_cross_spsc();
            }
        }

        // Phase 2a: Flush accumulated PUBLISH batches as PubSubPublishBatch messages
        if !publish_batches.is_empty() {
            let mut batch_slots: Vec<(
                std::sync::Arc<crate::shard::dispatch::PubSubResponseSlot>,
                Vec<usize>,
            )> = Vec::new();
            {
                let mut producers = ctx.dispatch_tx.borrow_mut();
                for (target, entries) in publish_batches.drain() {
                    let n = entries.len();
                    let slot = std::sync::Arc::new(
                        crate::shard::dispatch::PubSubResponseSlot::with_counts(1, n),
                    );
                    let resp_indices: Vec<usize> = entries.iter().map(|(idx, _, _)| *idx).collect();
                    let pairs: Vec<(Bytes, Bytes)> =
                        entries.into_iter().map(|(_, ch, msg)| (ch, msg)).collect();

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
                        if let Frame::Integer(ref mut total) = responses[*resp_idx] {
                            *total += remote_count;
                        }
                    }
                }
            }
        }

        // Phase 2b: Dispatch all deferred remote commands as batched PipelineBatch
        // messages (one per target shard), await all in parallel.
        if !remote_groups.is_empty() {
            reply_futures.clear();

            let mut oneshot_futures: Vec<(
                Vec<(usize, Option<Bytes>, Bytes)>,
                channel::OneshotReceiver<Vec<Frame>>,
            )> = Vec::new();
            for (target, entries) in remote_groups.drain() {
                let (reply_tx, reply_rx) = channel::oneshot();
                let (meta, commands): (
                    Vec<(usize, Option<Bytes>, Bytes)>,
                    Vec<std::sync::Arc<Frame>>,
                ) = entries
                    .into_iter()
                    .map(|(idx, arc_frame, aof, cmd)| ((idx, aof, cmd), arc_frame))
                    .unzip();

                let msg = ShardMessage::PipelineBatch {
                    db_index: conn.selected_db,
                    commands,
                    reply_tx,
                };
                let target_idx = ChannelMesh::target_index(ctx.shard_id, target);
                {
                    let mut pending = msg;
                    loop {
                        let push_result = {
                            let mut producers = ctx.dispatch_tx.borrow_mut();
                            producers[target_idx].try_push(pending)
                        };
                        match push_result {
                            Ok(()) => {
                                tracing::trace!(
                                    "Shard {}: pushed PipelineBatch to shard {}, notifying",
                                    ctx.shard_id,
                                    target
                                );
                                ctx.spsc_notifiers[target].notify_one();
                                break;
                            }
                            Err(val) => {
                                pending = val;
                                monoio::time::sleep(std::time::Duration::from_micros(10)).await;
                            }
                        }
                    }
                }
                oneshot_futures.push((meta, reply_rx));
            }

            // Poll all shard responses via pending_wakers relay (monoio cross-thread waker fix).
            // monoio's !Send executor doesn't see cross-thread Waker::wake() from flume.
            // Instead, the connection task registers its waker in pending_wakers; the event
            // loop drains and wakes them after every SPSC cycle (~1ms). On wake, try_recv()
            // checks if the response arrived; if not, re-register and yield again.
            for (meta, reply_rx) in oneshot_futures.drain(..) {
                tracing::trace!(
                    "Shard {}: awaiting cross-shard response via pending_wakers",
                    ctx.shard_id
                );
                let shard_responses = {
                    let pw = pending_wakers.clone();
                    loop {
                        match reply_rx.try_recv() {
                            Ok(value) => break Ok(value),
                            Err(flume::TryRecvError::Disconnected) => break Err(()),
                            Err(flume::TryRecvError::Empty) => {
                                // Yield once: register waker, return Pending, then Ready on wake.
                                let mut yielded = false;
                                std::future::poll_fn(|cx| {
                                    if yielded {
                                        std::task::Poll::Ready(())
                                    } else {
                                        yielded = true;
                                        pw.borrow_mut().push(cx.waker().clone());
                                        std::task::Poll::Pending
                                    }
                                })
                                .await;
                                // After wake, loop back to try_recv
                            }
                        }
                    }
                };
                let shard_responses = match shard_responses {
                    Ok(r) => r,
                    Err(()) => {
                        for (resp_idx, _, _) in &meta {
                            responses[*resp_idx] = Frame::Error(Bytes::from_static(
                                b"ERR cross-shard dispatch failed",
                            ));
                        }
                        continue;
                    }
                };
                for ((resp_idx, aof_bytes, cmd_name), resp) in meta.into_iter().zip(shard_responses)
                {
                    // AOF logging for successful remote writes
                    if let Some(bytes) = aof_bytes {
                        if !matches!(resp, Frame::Error(_)) {
                            if let Some(ref tx) = ctx.aof_tx {
                                let _ = tx.try_send(AofMessage::Append(bytes));
                            }
                        }
                    }
                    let resp = apply_resp3_conversion(&cmd_name, resp, conn.protocol_version);
                    responses[resp_idx] = resp;
                }
            }
        }

        // AUTH rate limiting: delay response to slow down brute-force attacks
        if auth_delay_ms > 0 {
            monoio::time::sleep(std::time::Duration::from_millis(auth_delay_ms)).await;
        }

        // Serialize all responses into write_buf, then do ONE write_all syscall.
        for response in &responses {
            codec.encode_frame(response, &mut write_buf);
        }

        // Write all responses in one batch using ownership I/O
        if !write_buf.is_empty() {
            let data = write_buf.split().freeze();
            let (result, _data): (std::io::Result<usize>, bytes::Bytes) =
                stream.write_all(data).await;
            if result.is_err() {
                break;
            }
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
                MonoioHandlerResult::MigrateConnection {
                    state: migrated_state,
                    target_shard,
                },
                Some(stream),
            );
        }

        if should_quit {
            break;
        }

        // Check shutdown (polled after each batch -- acceptable for MVP)
        if shutdown.is_cancelled() {
            break;
        }

        // Shrink buffers if they grew too large
        if read_buf.capacity() > 65536 {
            let remaining = read_buf.split();
            read_buf = BytesMut::with_capacity(8192);
            if !remaining.is_empty() {
                read_buf.extend_from_slice(&remaining);
            }
        }
        if write_buf.capacity() > 65536 {
            write_buf = BytesMut::with_capacity(8192);
        }
        if tmp_buf.capacity() > 65536 {
            tmp_buf = vec![0u8; 8192];
        }
    }

    // --- Graceful TCP shutdown: send FIN to client to avoid CLOSE_WAIT ---
    // Uses monoio's own shutdown() which properly manages the fd through
    // the runtime (unlike raw libc::shutdown which corrupts monoio state).
    let _ = stream.shutdown().await;

    // Phase 166: release any leaked cross-store TXN (client disconnected mid-txn).
    // Idempotent: TXN.ABORT already takes() active_cross_txn so this is a no-op if abort ran.
    // Closes T-161-05 — without this, a disconnect after TXN.BEGIN + SET would leak
    // kv_intents and pin the key invisible for all subsequent readers. Mirrors the
    // sharded runtime block in handler_sharded.rs so both paths delegate to the same
    // shared helper. FIN has already been sent; shard state is still intact.
    if let Some(txn) = conn.active_cross_txn.take() {
        crate::transaction::abort::abort_cross_store_txn(
            &ctx.shard_databases,
            ctx.shard_id,
            conn.selected_db,
            txn,
        );
    }

    // --- Disconnect cleanup: propagate unsubscribe to all shards' remote subscriber maps ---
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

    // NOTE: connection close is recorded by the caller (conn_accept.rs) to
    // preserve symmetry with `try_accept_connection`, which owns the
    // increment.  Decrementing here too produces a double-decrement on the
    // AtomicU64 counter — it wraps to u64::MAX on the second subtraction
    // and all subsequent `try_accept_connection` comparisons against
    // `maxclients` reject new connections.
    (MonoioHandlerResult::Done, None)
}
