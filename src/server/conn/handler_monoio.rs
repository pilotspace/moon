// Note: some imports/variables may be conditionally used across feature flags
//! Monoio connection handler using ownership-based I/O (AsyncReadRent/AsyncWriteRent).
//!
//! Extracted from `server/connection.rs` (Plan 48-02).

use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use bytes::{Bytes, BytesMut};
use ringbuf::traits::Producer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read, is_dispatch_read_supported};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::pubsub::subscriber::Subscriber;
use crate::pubsub::{self};
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::storage::eviction::{try_evict_if_needed, try_evict_if_needed_async_spill};
use crate::tracking::TrackingState;

use super::affinity::MigratedConnectionState;
use super::{
    apply_resp3_conversion, convert_blocking_to_nonblocking, execute_transaction_sharded,
    extract_bytes, extract_command, extract_primary_key, handle_blocking_command_monoio,
    handle_config, is_multi_key_command, propagate_subscription, try_inline_dispatch_loop,
    unpropagate_subscription,
};
use crate::framevec;
use crate::server::codec::RespCodec;
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
///
/// Reads RESP frames from the TCP stream, dispatches commands through the same
/// `crate::command::dispatch()` path as the tokio handler, and writes responses back.
///
/// MVP scope: SET/GET/PING/QUIT/SELECT/DEL/COMMAND and all other commands supported
/// by `dispatch()`. Skips pub/sub, blocking, tracking, cluster, replication, and ACL
/// enforcement -- those parameters are accepted but unused for future wiring.
///
/// Key difference from tokio path: monoio's `stream.read(buf)` takes ownership of the
/// buffer and returns `(Result<usize>, buf)`. We use a `Vec<u8>` intermediate for the
/// read since monoio's IoBufMut is implemented for Vec<u8>, then copy into BytesMut
/// for codec parsing.
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
                                                            let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
                                                            ctx.pubsub_registry.write().subscribe(channel.clone(), sub);
                                                            propagate_subscription(&ctx.all_remote_sub_maps, &channel, ctx.shard_id, ctx.num_shards, false);
                                                            conn.subscription_count += 1;
                                                            // Register pub/sub affinity for this client IP
                                                            if conn.subscription_count == 1 {
                                                                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                                    ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                                }
                                                            }
                                                            let resp = pubsub::subscribe_response(&channel, conn.subscription_count);
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
                                                            let resp = pubsub::unsubscribe_response(&Bytes::from_static(b""), conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for ch in &removed {
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = pubsub::unsubscribe_response(ch, conn.subscription_count);
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
                                                                let resp = pubsub::unsubscribe_response(&channel, conn.subscription_count);
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
                                                            let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
                                                            ctx.pubsub_registry.write().psubscribe(pattern.clone(), sub);
                                                            propagate_subscription(&ctx.all_remote_sub_maps, &pattern, ctx.shard_id, ctx.num_shards, true);
                                                            conn.subscription_count += 1;
                                                            // Register pub/sub affinity for this client IP
                                                            if conn.subscription_count == 1 {
                                                                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                                    ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                                }
                                                            }
                                                            let resp = pubsub::psubscribe_response(&pattern, conn.subscription_count);
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
                                                            let resp = pubsub::punsubscribe_response(&Bytes::from_static(b""), conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for pat in &removed {
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = pubsub::punsubscribe_response(pat, conn.subscription_count);
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
                                                                let resp = pubsub::punsubscribe_response(&pattern, conn.subscription_count);
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

        // Inline dispatch: handle GET/SET directly from raw bytes without Frame
        // construction or dispatch table lookup. For multi-shard, only local keys
        // are inlined; remote keys fall through to normal cross-shard dispatch.
        // Skip inline dispatch when not conn.authenticated — AUTH must go through normal path.
        if conn.authenticated {
            // Inline writes are only safe when every side-effect handled by
            // the normal dispatch path is either covered by the inline path
            // or provably unnecessary:
            //   - `cached_acl_unrestricted`: ACL check can be skipped
            //   - `!in_multi`: writes must be queued into the transaction
            //   - `!tracking_enabled`: CLIENT TRACKING invalidation required
            //   - `!is_replica`: replica rejects writes with READONLY
            //   - `spill_sender.is_none()`: tiered storage needs async spill
            //     eviction, not the synchronous delete path.
            //
            // The replica check does a non-blocking `try_read` on the shared
            // `RwLock<ReplicationState>`. If the lock is momentarily held for
            // write (role change in progress), fail safe by disabling inline
            // writes for this batch — the normal dispatch path will do the
            // full check next iteration.
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

        // Process frames with shard routing, cross-shard dispatch, and AOF logging
        // Note: do NOT clear write_buf -- it may contain responses from inline dispatch.
        // The inline path appends directly; the normal path appends via encode_frame below.
        // write_buf is cleared via .split().freeze() at the flush point each iteration.
        let mut should_quit = false;

        // Pipeline batch optimization: reuse pre-allocated containers (clear, not re-create).
        responses.clear();
        remote_groups.clear();
        // Accumulate cross-shard PUBLISH pairs per target shard for batch dispatch
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
                                    .as_millis()
                                    as u64,
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
                        // HELLO AUTH rate limiting
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
                        responses.push(Frame::Error(Bytes::from_static(
                            b"NOAUTH Authentication required.",
                        )));
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

            // --- ASKING: set per-connection flag for next command ---
            if cmd.eq_ignore_ascii_case(b"ASKING") {
                conn.asking = true;
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                continue;
            }

            // --- CLUSTER subcommands ---
            if cmd.eq_ignore_ascii_case(b"CLUSTER") {
                if let Some(ref cs) = ctx.cluster_state {
                    #[allow(clippy::unwrap_used)] // Fallback "127.0.0.1:6379" is a valid literal
                    let self_addr: std::net::SocketAddr = format!("127.0.0.1:{}", ctx.config_port)
                        .parse()
                        .unwrap_or_else(|_| "127.0.0.1:6379".parse().unwrap());
                    let resp =
                        crate::cluster::command::handle_cluster_command(cmd_args, cs, self_addr);
                    responses.push(resp);
                } else {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR This instance has cluster support disabled",
                    )));
                }
                continue;
            }

            // --- Lua scripting: EVALSHA ---
            if cmd.eq_ignore_ascii_case(b"EVALSHA") {
                let response = {
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
                continue;
            }

            // --- Lua scripting: EVAL ---
            if cmd.eq_ignore_ascii_case(b"EVAL") {
                let response = {
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
                continue;
            }

            // --- SCRIPT subcommands: LOAD, EXISTS, FLUSH ---
            if cmd.eq_ignore_ascii_case(b"SCRIPT") {
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
                        #[allow(clippy::unwrap_used)]
                        // std RwLock: poison = prior panic = unrecoverable
                        let route = cs.read().unwrap().route_slot(slot, was_asking);
                        match route {
                            crate::cluster::SlotRoute::Local => {} // proceed
                            other => {
                                let err_frame = other.into_error_frame(slot);
                                responses.push(err_frame);
                                continue;
                            }
                        }

                        // CROSSSLOT check for multi-key commands
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

            // --- HELLO (protocol negotiation, ACL-aware) ---
            if cmd.eq_ignore_ascii_case(b"HELLO") {
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

            // --- ACL command (intercepted at connection level) ---
            if cmd.eq_ignore_ascii_case(b"ACL") {
                let response = crate::command::acl::handle_acl(
                    cmd_args,
                    &ctx.acl_table,
                    &mut conn.acl_log,
                    &conn.current_user,
                    &peer_addr,
                    &ctx.runtime_config,
                );
                responses.push(response);
                continue;
            }

            // --- CONFIG GET/SET ---
            if cmd.eq_ignore_ascii_case(b"CONFIG") {
                responses.push(handle_config(cmd_args, &ctx.runtime_config, &ctx.config));
                continue;
            }

            // --- REPLICAOF / SLAVEOF ---
            if cmd.eq_ignore_ascii_case(b"REPLICAOF") || cmd.eq_ignore_ascii_case(b"SLAVEOF") {
                use crate::command::connection::{ReplicaofAction, replicaof};
                let (resp, action) = replicaof(cmd_args);
                if let Some(action) = action {
                    if let Some(ref rs) = ctx.repl_state {
                        match action {
                            ReplicaofAction::StartReplication { host, port } => {
                                if let Ok(mut rs_guard) = rs.write() {
                                    rs_guard.role = crate::replication::state::ReplicationRole::Replica {
                                        host: host.clone(),
                                        port,
                                        state: crate::replication::handshake::ReplicaHandshakeState::PingPending,
                                    };
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
                                    rs_guard.role =
                                        crate::replication::state::ReplicationRole::Master;
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
                let resp = crate::command::connection::replconf(cmd_args);
                responses.push(resp);
                continue;
            }

            // --- INFO (with replication section) ---
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

            // --- READONLY enforcement: reject writes on replicas ---
            if let Some(ref rs) = ctx.repl_state {
                if let Ok(rs_guard) = rs.try_read() {
                    if matches!(
                        rs_guard.role,
                        crate::replication::state::ReplicationRole::Replica { .. }
                    ) {
                        if metadata::is_write(cmd) {
                            responses.push(Frame::Error(Bytes::from_static(
                                b"READONLY You can't write against a read only replica.",
                            )));
                            continue;
                        }
                    }
                }
            }

            // --- CLIENT subcommands (ID, SETNAME, GETNAME, TRACKING) ---
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
                                let name_str = conn
                                    .client_name
                                    .as_ref()
                                    .map(|b| String::from_utf8_lossy(b).to_string());
                                crate::client_registry::update(client_id, |e| {
                                    e.name = name_str;
                                });
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
                                                table.register_prefix(
                                                    client_id,
                                                    prefix.clone(),
                                                    config_parsed.noloop,
                                                );
                                            }
                                        }
                                        responses
                                            .push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                    } else {
                                        conn.tracking_state = TrackingState::default();
                                        ctx.tracking_table.borrow_mut().untrack_all(client_id);
                                        conn.tracking_rx = None;
                                        responses
                                            .push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                    }
                                    continue;
                                }
                                Err(err_frame) => {
                                    responses.push(err_frame);
                                    continue;
                                }
                            }
                        }
                        // Admin CLIENT subcommands (LIST, INFO, KILL, PAUSE, UNPAUSE,
                        // NO-EVICT, NO-TOUCH) fall through to the ACL gate below.
                    }
                }
                // Fall through — admin subcommands handled after ACL check.
            }

            // --- PUBLISH: local delivery + cross-shard fan-out ---
            if cmd.eq_ignore_ascii_case(b"PUBLISH") {
                if cmd_args.len() != 2 {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR wrong number of arguments for 'publish' command",
                    )));
                } else {
                    let channel = extract_bytes(&cmd_args[0]);
                    let message = extract_bytes(&cmd_args[1]);
                    // ACL channel permission check for PUBLISH
                    if let Some(ref ch) = channel {
                        let denied = {
                            #[allow(clippy::unwrap_used)]
                            // std RwLock: poison = prior panic = unrecoverable
                            let acl_guard = ctx.acl_table.read().unwrap();
                            acl_guard.check_channel_permission(&conn.current_user, ch.as_ref())
                        };
                        if let Some(deny_reason) = denied {
                            responses
                                .push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                            continue;
                        }
                    }
                    match (channel, message) {
                        (Some(ch), Some(msg)) => {
                            let local_count = { ctx.pubsub_registry.write().publish(&ch, &msg) };
                            // Targeted fanout: only send to shards that have subscribers
                            let targets = ctx.remote_subscriber_map.read().target_shards(&ch);
                            if targets.is_empty() {
                                // Fast path: no remote subscribers
                                responses.push(Frame::Integer(local_count));
                            } else {
                                let remote_targets: Vec<usize> =
                                    targets.into_iter().filter(|&t| t != ctx.shard_id).collect();
                                if remote_targets.is_empty() {
                                    responses.push(Frame::Integer(local_count));
                                } else {
                                    // Accumulate into per-shard batches for coalesced dispatch
                                    let resp_idx = responses.len();
                                    responses.push(Frame::Integer(local_count)); // placeholder, updated after batch flush
                                    for target in &remote_targets {
                                        publish_batches.entry(*target).or_default().push((
                                            resp_idx,
                                            ch.clone(),
                                            msg.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                        _ => responses.push(Frame::Error(Bytes::from_static(
                            b"ERR invalid channel or message",
                        ))),
                    }
                }
                continue;
            }

            // --- SUBSCRIBE / PSUBSCRIBE ---
            if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                let is_pattern = cmd.eq_ignore_ascii_case(b"PSUBSCRIBE");
                if cmd_args.is_empty() {
                    let cmd_name = if is_pattern {
                        "psubscribe"
                    } else {
                        "subscribe"
                    };
                    let err = Frame::Error(Bytes::from(format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                    responses.push(err);
                    continue;
                }
                // Allocate pubsub channel if not yet created
                if conn.pubsub_tx.is_none() {
                    let (tx, rx) = channel::mpsc_bounded::<bytes::Bytes>(256);
                    conn.pubsub_tx = Some(tx);
                    conn.pubsub_rx = Some(rx);
                }
                if conn.subscriber_id == 0 {
                    conn.subscriber_id = crate::pubsub::next_subscriber_id();
                }
                // Flush accumulated responses before entering subscriber mode
                for resp in &responses {
                    codec.encode_frame(resp, &mut write_buf);
                }
                for arg in cmd_args {
                    if let Some(ch) = extract_bytes(arg) {
                        // ACL channel permission check
                        {
                            #[allow(clippy::unwrap_used)]
                            // std RwLock: poison = prior panic = unrecoverable
                            let acl_guard = ctx.acl_table.read().unwrap();
                            if let Some(deny_reason) =
                                acl_guard.check_channel_permission(&conn.current_user, ch.as_ref())
                            {
                                drop(acl_guard);
                                let err =
                                    Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                codec.encode_frame(&err, &mut write_buf);
                                continue;
                            }
                        }
                        #[allow(clippy::unwrap_used)]
                        // conn.pubsub_tx is set to Some just above before this loop
                        let sub =
                            Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
                        if is_pattern {
                            ctx.pubsub_registry.write().psubscribe(ch.clone(), sub);
                        } else {
                            ctx.pubsub_registry.write().subscribe(ch.clone(), sub);
                        }
                        propagate_subscription(
                            &ctx.all_remote_sub_maps,
                            &ch,
                            ctx.shard_id,
                            ctx.num_shards,
                            is_pattern,
                        );
                        conn.subscription_count += 1;
                        // Register pub/sub affinity for this client IP
                        if conn.subscription_count == 1 {
                            if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                ctx.pubsub_affinity
                                    .write()
                                    .register(addr.ip(), ctx.shard_id);
                            }
                        }
                        let resp = if is_pattern {
                            pubsub::psubscribe_response(&ch, conn.subscription_count)
                        } else {
                            pubsub::subscribe_response(&ch, conn.subscription_count)
                        };
                        codec.encode_frame(&resp, &mut write_buf);
                    }
                }
                // Flush responses and re-enter loop (next iteration enters subscriber mode)
                if !write_buf.is_empty() {
                    let data = write_buf.split().freeze();
                    let (result, _): (std::io::Result<usize>, bytes::Bytes) =
                        stream.write_all(data).await;
                    if result.is_err() {
                        return (MonoioHandlerResult::Done, None);
                    }
                }
                responses.clear();
                break; // break out of frame loop to re-enter main loop in subscriber mode
            }

            // --- UNSUBSCRIBE / PUNSUBSCRIBE (in normal mode, no-op if not subscribed) ---
            if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE")
            {
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
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR wrong number of arguments for 'pubsub' command",
                    )));
                    continue;
                }
                let subcmd = extract_bytes(&cmd_args[0]);
                match subcmd {
                    Some(ref sc) if sc.eq_ignore_ascii_case(b"CHANNELS") => {
                        let pattern = if cmd_args.len() > 1 {
                            extract_bytes(&cmd_args[1])
                        } else {
                            None
                        };
                        let mut all_channels: std::collections::HashSet<Bytes> =
                            std::collections::HashSet::new();
                        for reg in &ctx.all_pubsub_registries {
                            let guard = reg.read();
                            all_channels.extend(guard.active_channels(pattern.as_deref()));
                        }
                        let arr: Vec<Frame> =
                            all_channels.into_iter().map(Frame::BulkString).collect();
                        responses.push(Frame::Array(arr.into()));
                    }
                    Some(ref sc) if sc.eq_ignore_ascii_case(b"NUMSUB") => {
                        let channels: Vec<Bytes> = cmd_args[1..]
                            .iter()
                            .filter_map(|a| extract_bytes(a))
                            .collect();
                        let mut counts: std::collections::HashMap<Bytes, i64> =
                            std::collections::HashMap::new();
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
                        responses.push(Frame::Error(Bytes::from_static(
                            b"ERR unknown subcommand or wrong number of arguments for 'pubsub' command",
                        )));
                    }
                }
                continue;
            }

            // --- BGSAVE: trigger per-shard cooperative snapshot ---
            if cmd.eq_ignore_ascii_case(b"BGSAVE") {
                let response = crate::command::persistence::bgsave_start_sharded(
                    &ctx.snapshot_trigger_tx,
                    ctx.num_shards,
                );
                responses.push(response);
                continue;
            }
            // SAVE -- not supported in sharded mode
            if cmd.eq_ignore_ascii_case(b"SAVE") {
                responses.push(Frame::Error(Bytes::from_static(
                    b"ERR SAVE not supported in sharded mode, use BGSAVE",
                )));
                continue;
            }
            // LASTSAVE -- return timestamp of last successful save
            if cmd.eq_ignore_ascii_case(b"LASTSAVE") {
                responses.push(crate::command::persistence::handle_lastsave());
                continue;
            }
            // BGREWRITEAOF -- multi-part AOF rewrite
            if cmd.eq_ignore_ascii_case(b"BGREWRITEAOF") {
                if let Some(ref tx) = ctx.aof_tx {
                    responses.push(crate::command::persistence::bgrewriteaof_start_sharded(
                        tx,
                        ctx.shard_databases.clone(),
                    ));
                } else {
                    responses.push(Frame::Error(Bytes::from_static(b"ERR AOF is not enabled")));
                }
                continue;
            }

            // === ACL permission check (NOPERM gate) ===
            // Exempt commands (AUTH, HELLO, QUIT, ACL) already handled above.
            // Fast path: skip RwLock + HashMap probe for unrestricted users
            // whose per-connection cache is still fresh (no ACL mutation has
            // occurred since the cache was populated).  A stale cache MUST
            // NOT bypass this check — see `ConnectionState::acl_skip_allowed`.
            if !conn.acl_skip_allowed() {
                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                let acl_guard = ctx.acl_table.read().unwrap();
                if let Some(deny_reason) =
                    acl_guard.check_command_permission(&conn.current_user, cmd, cmd_args)
                {
                    drop(acl_guard);
                    conn.acl_log.push(crate::acl::AclLogEntry {
                        reason: "command".to_string(),
                        object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                        username: conn.current_user.clone(),
                        client_addr: peer_addr.clone(),
                        timestamp_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    });
                    responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                    continue;
                }

                // === ACL key pattern check (same lock guard) ===
                let is_write_for_acl = metadata::is_write(cmd);
                if let Some(deny_reason) = acl_guard.check_key_permission(
                    &conn.current_user,
                    cmd,
                    cmd_args,
                    is_write_for_acl,
                ) {
                    drop(acl_guard);
                    conn.acl_log.push(crate::acl::AclLogEntry {
                        reason: "command".to_string(),
                        object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                        username: conn.current_user.clone(),
                        client_addr: peer_addr.clone(),
                        timestamp_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    });
                    responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                    continue;
                }
            } // !cached_acl_unrestricted

            // --- CLIENT admin subcommands (LIST, INFO, KILL, PAUSE, UNPAUSE) ---
            // Placed AFTER ACL check so restricted users cannot access admin ops.
            if cmd.eq_ignore_ascii_case(b"CLIENT") {
                if let Some(sub) = cmd_args.first() {
                    if let Some(sub_bytes) = extract_bytes(sub) {
                        if sub_bytes.eq_ignore_ascii_case(b"LIST") {
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
                            let info =
                                crate::client_registry::client_info(client_id).unwrap_or_default();
                            responses.push(Frame::BulkString(Bytes::from(info)));
                            continue;
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
                                        responses
                                            .push(Frame::SimpleString(Bytes::from_static(b"OK")));
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
                        if sub_bytes.eq_ignore_ascii_case(b"NO-EVICT")
                            || sub_bytes.eq_ignore_ascii_case(b"NO-TOUCH")
                        {
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            continue;
                        }
                        // Unknown CLIENT subcommand
                        responses.push(Frame::Error(Bytes::from(format!(
                            "ERR unknown subcommand '{}'",
                            String::from_utf8_lossy(&sub_bytes)
                        ))));
                        continue;
                    }
                }
                responses.push(Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'client' command",
                )));
                continue;
            }

            // --- Functions API: FUNCTION/FCALL/FCALL_RO ---
            // Placed AFTER ACL check. Respects MULTI queue — if conn.in_multi,
            // fall through to the MULTI queue gate instead of executing.
            if !conn.in_multi {
                if cmd.eq_ignore_ascii_case(b"FUNCTION") {
                    let response = crate::command::functions::handle_function(
                        &mut func_registry.borrow_mut(),
                        cmd_args,
                    );
                    responses.push(response);
                    continue;
                }
                if cmd.eq_ignore_ascii_case(b"FCALL") {
                    let response = {
                        let mut guard =
                            ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
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
                    continue;
                }
                if cmd.eq_ignore_ascii_case(b"FCALL_RO") {
                    let response = {
                        let mut guard =
                            ctx.shard_databases.write_db(ctx.shard_id, conn.selected_db);
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
                    continue;
                }
            }

            // --- MULTI ---
            if cmd.eq_ignore_ascii_case(b"MULTI") {
                if conn.in_multi {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR MULTI calls can not be nested",
                    )));
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
                continue;
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
                continue;
            }

            // --- BLOCKING COMMANDS ---
            if cmd.eq_ignore_ascii_case(b"BLPOP")
                || cmd.eq_ignore_ascii_case(b"BRPOP")
                || cmd.eq_ignore_ascii_case(b"BLMOVE")
                || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
                || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
                || cmd.eq_ignore_ascii_case(b"BLMPOP")
                || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
                || cmd.eq_ignore_ascii_case(b"BZMPOP")
            {
                // Inside MULTI: queue as non-blocking variant
                if conn.in_multi {
                    let nb_frame = convert_blocking_to_nonblocking(cmd, cmd_args);
                    conn.command_queue.push(nb_frame);
                    responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                    continue;
                }

                // Flush accumulated responses before blocking
                for resp in &responses {
                    codec.encode_frame(resp, &mut write_buf);
                }
                if !write_buf.is_empty() {
                    let data = write_buf.split().freeze();
                    let (result, _): (std::io::Result<usize>, bytes::Bytes) =
                        stream.write_all(data).await;
                    if result.is_err() {
                        return (MonoioHandlerResult::Done, None);
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
                    &shutdown,
                    &ctx.spsc_notifiers,
                )
                .await;

                // Encode blocking response directly
                codec.encode_frame(&blocking_response, &mut write_buf);
                responses.clear();
                break; // Blocking command ends the pipeline batch
            }

            // --- MULTI queue mode: queue commands when in transaction ---
            if conn.in_multi {
                conn.command_queue.push(frame);
                responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                continue;
            }

            // --- Cross-shard aggregation commands: KEYS, SCAN, DBSIZE ---
            if ctx.num_shards > 1 {
                if cmd.eq_ignore_ascii_case(b"KEYS") {
                    let response = crate::shard::coordinator::coordinate_keys(
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
                    continue;
                }
                if cmd.eq_ignore_ascii_case(b"SCAN") {
                    let response = crate::shard::coordinator::coordinate_scan(
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
                    continue;
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
                    continue;
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
                    continue;
                }
            }

            // --- FT.* vector search commands ---
            // Local shard: direct VectorStore access via ctx.shard_databases.
            // Remote shards: SPSC dispatch. Works with any shard count (including 1).
            if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
                if ctx.num_shards > 1 {
                    // Multi-shard: dispatch via SPSC
                    #[cfg(feature = "text-index")]
                    if cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
                        // ── FT.AGGREGATE: multi-shard scatter-gather (Phase 152 Plan 03). ──
                        // Mirrors handler_sharded.rs:1458. scatter_text_aggregate acquires
                        // its own per-shard guards inside the single-shard block internally,
                        // so we never hold a MutexGuard across the .await below.
                        let parsed =
                            match crate::command::vector_search::ft_aggregate::parse_aggregate_args(
                                cmd_args,
                            ) {
                                Ok(p) => p,
                                Err(err_frame) => {
                                    responses.push(err_frame);
                                    continue;
                                }
                            };
                        let response = crate::shard::scatter_aggregate::scatter_text_aggregate(
                            parsed.index_name,
                            parsed.query,
                            parsed.pipeline,
                            ctx.shard_id,
                            ctx.num_shards,
                            &ctx.shard_databases,
                            &ctx.dispatch_tx,
                            &ctx.spsc_notifiers,
                        )
                        .await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                        // Check if this is a text query BEFORE trying parse_ft_search_args
                        // (which would return an error for non-KNN queries).
                        let query_bytes = cmd_args
                            .get(1)
                            .and_then(|f| crate::command::vector_search::extract_bulk(f));
                        let is_text = query_bytes
                            .as_ref()
                            .map_or(false, |q| crate::command::vector_search::is_text_query(q));

                        // ── HYBRID multi-shard path (Phase 152 Plan 05, D-13) ──────────
                        #[cfg(feature = "text-index")]
                        {
                            match crate::command::vector_search::hybrid::parse_hybrid_modifier(
                                cmd_args,
                            ) {
                                Ok(Some(partial)) => {
                                    let index_name = match cmd_args.first().and_then(|f| {
                                        crate::command::vector_search::extract_bulk(f)
                                    }) {
                                        Some(b) => b,
                                        None => {
                                            responses.push(Frame::Error(Bytes::from_static(
                                                b"ERR invalid index name",
                                            )));
                                            continue;
                                        }
                                    };
                                    let text_query = match query_bytes.clone() {
                                        Some(q) => q,
                                        None => {
                                            responses.push(Frame::Error(Bytes::from_static(
                                                b"ERR invalid query",
                                            )));
                                            continue;
                                        }
                                    };
                                    let (limit_offset, limit_count) =
                                        crate::command::vector_search::parse_limit_clause(cmd_args);
                                    let top_k = if limit_count == usize::MAX {
                                        limit_offset.saturating_add(10).max(1)
                                    } else {
                                        limit_offset.saturating_add(limit_count).max(1)
                                    };
                                    let hq = crate::command::vector_search::hybrid::HybridQuery {
                                        index_name,
                                        text_query,
                                        dense_field: partial.dense_field,
                                        dense_blob: partial.dense_blob,
                                        sparse: partial.sparse,
                                        weights: partial.weights,
                                        k_per_stream: partial.k_per_stream,
                                        top_k,
                                        offset: limit_offset,
                                        count: limit_count,
                                    };
                                    let response =
                                        crate::shard::scatter_hybrid::scatter_hybrid_search(
                                            hq,
                                            ctx.shard_id,
                                            ctx.num_shards,
                                            &ctx.shard_databases,
                                            &ctx.dispatch_tx,
                                            &ctx.spsc_notifiers,
                                        )
                                        .await;
                                    responses.push(response);
                                    continue;
                                }
                                Ok(None) => { /* fall through */ }
                                Err(err_frame) => {
                                    responses.push(err_frame);
                                    continue;
                                }
                            }
                        }

                        if is_text {
                            // ── Text FT.SEARCH: two-phase DFS scatter-gather ──────────────────
                            let index_name = match cmd_args
                                .first()
                                .and_then(|f| crate::command::vector_search::extract_bulk(f))
                            {
                                Some(b) => b,
                                None => {
                                    responses.push(Frame::Error(Bytes::from_static(
                                        b"ERR invalid index name",
                                    )));
                                    continue;
                                }
                            };
                            let query_str = query_bytes.unwrap();

                            // B-01 SITE 2 FIX (Plan 152-06): FieldFilter short-circuit BEFORE
                            // the analyzer-first parse_result block. TAG queries (and Plan 07
                            // NumericRange) route through the InvertedSearch fan-out — no
                            // analyzer touched, no field_idx resolution (the filter carries
                            // its own field name; search_tag resolves against tag_fields).
                            #[cfg(feature = "text-index")]
                            {
                                match crate::command::vector_search::pre_parse_field_filter(
                                    query_str.as_ref(),
                                ) {
                                    Ok(Some(clause)) => {
                                        if let Some(filter) = clause.filter {
                                            let (offset, count) =
                                                crate::command::vector_search::parse_limit_clause(
                                                    cmd_args,
                                                );
                                            let top_k = if count == usize::MAX {
                                                10000
                                            } else {
                                                offset.saturating_add(count)
                                            }
                                            .max(1);
                                            let response =
                                                crate::shard::coordinator::scatter_text_search_filter(
                                                    index_name,
                                                    filter,
                                                    top_k,
                                                    offset,
                                                    count,
                                                    ctx.shard_id,
                                                    ctx.num_shards,
                                                    &ctx.shard_databases,
                                                    &ctx.dispatch_tx,
                                                    &ctx.spsc_notifiers,
                                                )
                                                .await;
                                            responses.push(response);
                                            continue;
                                        }
                                    }
                                    Ok(None) => { /* fall through to BM25 path */ }
                                    Err(e) => {
                                        responses.push(Frame::Error(Bytes::from(e.to_owned())));
                                        continue;
                                    }
                                }
                            }

                            // Parse query and resolve field_idx inside a block scope so the
                            // MutexGuard from text_store() is dropped BEFORE .await.
                            // We use the TextIndex's own field_analyzers (same pipeline used at index time).
                            type ParseResult = Result<
                                (Vec<crate::command::vector_search::QueryTerm>, Option<usize>),
                                String,
                            >;
                            let parse_result: ParseResult = {
                                let ts = ctx.shard_databases.text_store(ctx.shard_id);
                                match ts.get_index(&index_name) {
                                    None => Err("ERR no such index".to_owned()),
                                    Some(text_index) => {
                                        match text_index.field_analyzers.first() {
                                            None => Err("ERR index has no TEXT fields".to_owned()),
                                            Some(analyzer) => {
                                                // analyzer borrows text_index which borrows ts — all in this block.
                                                let parsed =
                                                    crate::command::vector_search::parse_text_query(
                                                        &query_str, analyzer,
                                                    );
                                                match parsed {
                                                    Err(e) => Err(e.to_owned()),
                                                    Ok(clause) => {
                                                        let field_idx = match &clause.field_name {
                                                            None => Ok(None),
                                                            Some(field_name) => match text_index
                                                                .text_fields
                                                                .iter()
                                                                .position(|f| {
                                                                    f.field_name
                                                                        .as_ref()
                                                                        .eq_ignore_ascii_case(
                                                                            field_name.as_ref(),
                                                                        )
                                                                }) {
                                                                Some(idx) => Ok(Some(idx)),
                                                                None => Err(format!(
                                                                    "ERR unknown field '{}'",
                                                                    String::from_utf8_lossy(
                                                                        field_name
                                                                    )
                                                                )),
                                                            },
                                                        };
                                                        field_idx.map(|idx| (clause.terms, idx))
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }; // MutexGuard dropped here

                            let (query_terms, field_idx) = match parse_result {
                                Ok(t) => t,
                                Err(e) => {
                                    responses.push(Frame::Error(Bytes::from(e)));
                                    continue;
                                }
                            };

                            let (offset, count) =
                                crate::command::vector_search::parse_limit_clause(cmd_args);
                            let top_k = if count == usize::MAX {
                                10000
                            } else {
                                offset.saturating_add(count)
                            }
                            .max(1);

                            // Parse optional HIGHLIGHT/SUMMARIZE clauses from args.
                            let highlight_opts =
                                crate::command::vector_search::parse_highlight_clause(cmd_args);
                            let summarize_opts =
                                crate::command::vector_search::parse_summarize_clause(cmd_args);

                            let response = crate::shard::coordinator::scatter_text_search(
                                index_name,
                                query_terms,
                                field_idx,
                                top_k,
                                offset,
                                count,
                                ctx.shard_id,
                                ctx.num_shards,
                                &ctx.shard_databases,
                                &ctx.dispatch_tx,
                                &ctx.spsc_notifiers,
                                highlight_opts,
                                summarize_opts,
                            )
                            .await;
                            responses.push(response);
                            continue;
                        }

                        // ── Vector FT.SEARCH (KNN / SPARSE): existing path ────────────────
                        let response =
                            match crate::command::vector_search::parse_ft_search_args(cmd_args) {
                                Ok((index_name, query_blob, k, filter, _offset, _count)) => {
                                    if filter.is_some() {
                                        Frame::Error(Bytes::from_static(
                                            b"ERR FILTER not supported in multi-shard mode yet",
                                        ))
                                    } else {
                                        crate::shard::coordinator::scatter_vector_search_remote(
                                            index_name,
                                            query_blob,
                                            k,
                                            ctx.shard_id,
                                            ctx.num_shards,
                                            &ctx.shard_databases,
                                            &ctx.dispatch_tx,
                                            &ctx.spsc_notifiers,
                                        )
                                        .await
                                    }
                                }
                                Err(err_frame) => err_frame,
                            };
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"FT.CREATE")
                        || cmd.eq_ignore_ascii_case(b"FT.DROPINDEX")
                    {
                        // Broadcast to ALL shards so every shard has the index
                        let response = crate::shard::coordinator::broadcast_vector_command(
                            std::sync::Arc::new(frame),
                            ctx.shard_id,
                            ctx.num_shards,
                            &ctx.shard_databases,
                            &ctx.dispatch_tx,
                            &ctx.spsc_notifiers,
                        )
                        .await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                        let response = {
                            let vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            let ts = ctx.shard_databases.text_store(ctx.shard_id);
                            crate::command::vector_search::ft_info(&vs, &ts, cmd_args)
                        };
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                        let response = {
                            let vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            crate::command::vector_search::ft_list(&vs)
                        };
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                        let response = {
                            let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            let mut ts = ctx.shard_databases.text_store(ctx.shard_id);
                            crate::command::vector_search::ft_compact(&mut vs, &mut ts, cmd_args)
                        };
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                        let response = {
                            let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            crate::command::vector_search::cache_search::ft_cachesearch(
                                &mut vs, cmd_args,
                            )
                        };
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                        let response = {
                            let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            let mut ts = ctx.shard_databases.text_store(ctx.shard_id);
                            crate::command::vector_search::ft_config(&mut vs, &mut ts, cmd_args)
                        };
                        responses.push(response);
                        continue;
                    }
                    // FT.RECOMMEND, FT.NAVIGATE, FT.EXPAND need db/graph — dispatch locally
                    if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND")
                        || cmd.eq_ignore_ascii_case(b"FT.NAVIGATE")
                        || cmd.eq_ignore_ascii_case(b"FT.EXPAND")
                    {
                        let response = {
                            let sdb = &ctx.shard_databases;
                            let mut vs = sdb.vector_store(ctx.shard_id);
                            if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                let mut db_guard = sdb.write_db(ctx.shard_id, 0);
                                crate::command::vector_search::recommend::ft_recommend(
                                    &mut vs,
                                    cmd_args,
                                    Some(&mut *db_guard),
                                )
                            } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                #[cfg(feature = "graph")]
                                {
                                    let graph_guard = sdb.graph_store_read(ctx.shard_id);
                                    crate::command::vector_search::navigate::ft_navigate(
                                        &mut vs,
                                        Some(&graph_guard),
                                        cmd_args,
                                        None,
                                    )
                                }
                                #[cfg(not(feature = "graph"))]
                                {
                                    Frame::Error(Bytes::from_static(
                                        b"ERR FT.NAVIGATE requires graph feature",
                                    ))
                                }
                            } else {
                                // FT.EXPAND
                                #[cfg(feature = "graph")]
                                {
                                    let graph_guard = sdb.graph_store_read(ctx.shard_id);
                                    crate::command::vector_search::ft_expand(&graph_guard, cmd_args)
                                }
                                #[cfg(not(feature = "graph"))]
                                {
                                    Frame::Error(Bytes::from_static(
                                        b"ERR FT.EXPAND requires graph feature",
                                    ))
                                }
                            }
                        };
                        responses.push(response);
                        continue;
                    }
                    responses.push(Frame::Error(Bytes::from_static(b"ERR unknown FT command")));
                    continue;
                } else {
                    // Single-shard: no SPSC channels needed.
                    // Dispatch directly to shard's VectorStore via shared access.
                    //
                    // ── 154-01 single-shard FT.AGGREGATE fast path ────────────────
                    // scatter_text_aggregate internally fast-paths num_shards == 1
                    // to execute_local_full with locally-acquired guards dropped
                    // before any .await. Calling the scatter entry here (instead
                    // of execute_local_full directly) keeps the dispatch body
                    // byte-symmetric with the multi-shard site above.
                    #[cfg(feature = "text-index")]
                    if cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
                        let parsed =
                            match crate::command::vector_search::ft_aggregate::parse_aggregate_args(
                                cmd_args,
                            ) {
                                Ok(p) => p,
                                Err(err_frame) => {
                                    responses.push(err_frame);
                                    continue;
                                }
                            };
                        let response = crate::shard::scatter_aggregate::scatter_text_aggregate(
                            parsed.index_name,
                            parsed.query,
                            parsed.pipeline,
                            ctx.shard_id,
                            ctx.num_shards,
                            &ctx.shard_databases,
                            &ctx.dispatch_tx,
                            &ctx.spsc_notifiers,
                        )
                        .await;
                        responses.push(response);
                        continue;
                    }
                    //
                    // ── 151-03 single-shard text FT.SEARCH fast path ──────────────
                    // Bare text queries (exact / fuzzy / prefix / field-targeted)
                    // are not understood by `ft_search()` (which only parses
                    // KNN / SPARSE / HYBRID) — they would otherwise return
                    // `ERR invalid KNN query syntax`. Route them directly to
                    // `execute_text_search_local` here, the same function the
                    // multi-shard path uses once its per-shard scatter has
                    // aggregated IDFs. We skip HYBRID (existing ft_search
                    // handles it) and KNN/SPARSE (is_text_query returns false).
                    #[cfg(feature = "text-index")]
                    if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                        if let Some(Frame::BulkString(query_bytes)) = cmd_args.get(1) {
                            match crate::command::vector_search::parse_hybrid_modifier(cmd_args) {
                                Ok(Some(_)) => {
                                    // HYBRID present — defer to existing ft_search() below.
                                }
                                Err(frame_err) => {
                                    responses.push(frame_err);
                                    continue;
                                }
                                Ok(None) => {
                                    if crate::command::vector_search::is_text_query(
                                        query_bytes.as_ref(),
                                    ) {
                                        // Step 1: index_name from cmd_args[0].
                                        let index_name = match cmd_args.first() {
                                            Some(Frame::BulkString(b)) => b.clone(),
                                            _ => {
                                                responses.push(Frame::Error(Bytes::from_static(
                                                    b"ERR wrong number of arguments for FT.SEARCH",
                                                )));
                                                continue;
                                            }
                                        };
                                        // B-01 SITE 2 FIX (single-shard 151-03 fast path, Plan 152-06):
                                        // FieldFilter short-circuit BEFORE the analyzer lookup and
                                        // BEFORE the text_fields.is_empty() bail.
                                        #[cfg(feature = "text-index")]
                                        match crate::command::vector_search::pre_parse_field_filter(
                                            query_bytes.as_ref(),
                                        ) {
                                            Ok(Some(clause)) => {
                                                if clause.filter.is_some() {
                                                    let (offset, count) = crate::command::vector_search::parse_limit_clause(cmd_args);
                                                    let top_k = if count == usize::MAX {
                                                        10000
                                                    } else {
                                                        offset.saturating_add(count)
                                                    }
                                                    .max(1);
                                                    let ts_guard = ctx
                                                        .shard_databases
                                                        .text_store(ctx.shard_id);
                                                    let response = match ts_guard
                                                        .get_index(&index_name)
                                                    {
                                                        None => Frame::Error(Bytes::from_static(
                                                            b"ERR no such index",
                                                        )),
                                                        Some(text_index) => {
                                                            let results = crate::command::vector_search::ft_text_search::execute_query_on_index(
                                                                text_index, &clause, None, None, top_k,
                                                            );
                                                            crate::command::vector_search::ft_text_search::build_text_response(
                                                                &results, offset, count,
                                                            )
                                                        }
                                                    };
                                                    drop(ts_guard);
                                                    responses.push(response);
                                                    continue;
                                                }
                                            }
                                            Ok(None) => { /* fall through */ }
                                            Err(e) => {
                                                responses.push(Frame::Error(
                                                    Bytes::copy_from_slice(e.as_bytes()),
                                                ));
                                                continue;
                                            }
                                        }
                                        // Step 2: acquire ts guard (single-shard monoio).
                                        let ts_guard = ctx.shard_databases.text_store(ctx.shard_id);
                                        // Step 3: resolve index.
                                        let text_index = match ts_guard.get_index(&index_name) {
                                            Some(idx) => idx,
                                            None => {
                                                drop(ts_guard);
                                                responses.push(Frame::Error(Bytes::from_static(
                                                    b"ERR no such index",
                                                )));
                                                continue;
                                            }
                                        };
                                        // Step 4: ensure at least one TEXT field.
                                        if text_index.text_fields.is_empty() {
                                            drop(ts_guard);
                                            responses.push(Frame::Error(Bytes::from_static(
                                                b"ERR index has no TEXT fields",
                                            )));
                                            continue;
                                        }
                                        // Step 5: parse the query via the index's first analyzer.
                                        let analyzer = match text_index.field_analyzers.first() {
                                            Some(a) => a,
                                            None => {
                                                drop(ts_guard);
                                                responses.push(Frame::Error(Bytes::from_static(
                                                    b"ERR index has no TEXT fields",
                                                )));
                                                continue;
                                            }
                                        };
                                        let clause =
                                            match crate::command::vector_search::parse_text_query(
                                                query_bytes.as_ref(),
                                                analyzer,
                                            ) {
                                                Ok(c) => c,
                                                Err(msg) => {
                                                    drop(ts_guard);
                                                    responses.push(Frame::Error(
                                                        Bytes::copy_from_slice(msg.as_bytes()),
                                                    ));
                                                    continue;
                                                }
                                            };
                                        // Step 5b: resolve field_idx.
                                        let field_idx = match &clause.field_name {
                                            None => None,
                                            Some(field_name) => {
                                                match text_index.text_fields.iter().position(|f| {
                                                    f.field_name
                                                        .as_ref()
                                                        .eq_ignore_ascii_case(field_name.as_ref())
                                                }) {
                                                    Some(idx) => Some(idx),
                                                    None => {
                                                        let bad_name = field_name.clone();
                                                        drop(ts_guard);
                                                        responses.push(Frame::Error(Bytes::from(
                                                            format!(
                                                                "ERR unknown field '{}'",
                                                                String::from_utf8_lossy(&bad_name)
                                                            ),
                                                        )));
                                                        continue;
                                                    }
                                                }
                                            }
                                        };
                                        // Step 6: extract query_terms.
                                        let query_terms = clause.terms;
                                        // Step 7: LIMIT parsing + top_k cap (T-151-03-02).
                                        let (offset, count) =
                                            crate::command::vector_search::parse_limit_clause(
                                                cmd_args,
                                            );
                                        let top_k = if count == usize::MAX {
                                            10000
                                        } else {
                                            offset.saturating_add(count)
                                        }
                                        .max(1);
                                        // Step 8: HIGHLIGHT / SUMMARIZE options.
                                        let highlight_opts =
                                            crate::command::vector_search::parse_highlight_clause(
                                                cmd_args,
                                            );
                                        let summarize_opts =
                                            crate::command::vector_search::parse_summarize_clause(
                                                cmd_args,
                                            );
                                        // Step 9: acquire DB read guard iff post-processing is needed.
                                        let db_guard_opt = if highlight_opts.is_some()
                                            || summarize_opts.is_some()
                                        {
                                            Some(ctx.shard_databases.read_db(ctx.shard_id, 0))
                                        } else {
                                            None
                                        };
                                        // Step 10: execute + optional post-processing.
                                        let mut response =
                                            crate::command::vector_search::execute_text_search_local(
                                                &ts_guard,
                                                &index_name,
                                                field_idx,
                                                &query_terms,
                                                top_k,
                                                offset,
                                                count,
                                            );
                                        if let Some(ref db_guard) = db_guard_opt {
                                            let term_strings: Vec<String> = query_terms
                                                .iter()
                                                .map(|qt| qt.text.clone())
                                                .collect();
                                            crate::command::vector_search::apply_post_processing(
                                                &mut response,
                                                &term_strings,
                                                text_index,
                                                db_guard,
                                                highlight_opts.as_ref(),
                                                summarize_opts.as_ref(),
                                            );
                                        }
                                        // Explicit drop order: inner db_guard first, outer ts_guard last.
                                        drop(db_guard_opt);
                                        drop(ts_guard);
                                        responses.push(response);
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    let response = {
                        let shard_databases_ref = &ctx.shard_databases;
                        let mut vs = shard_databases_ref.vector_store(ctx.shard_id);
                        let mut ts = shard_databases_ref.text_store(ctx.shard_id);
                        if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                            crate::command::vector_search::ft_create(&mut vs, &mut ts, cmd_args)
                        } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                            // Detect SESSION keyword to provide database access for sorted set tracking
                            let has_session = cmd_args.iter().any(|a| {
                                if let Frame::BulkString(b) = a {
                                    b.eq_ignore_ascii_case(b"SESSION")
                                } else {
                                    false
                                }
                            });
                            if has_session {
                                let mut db_guard = shard_databases_ref.write_db(ctx.shard_id, 0);
                                crate::command::vector_search::ft_search(
                                    &mut vs,
                                    cmd_args,
                                    Some(&mut *db_guard),
                                    Some(&*ts),
                                )
                            } else {
                                crate::command::vector_search::ft_search(
                                    &mut vs,
                                    cmd_args,
                                    None,
                                    Some(&*ts),
                                )
                            }
                        } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                            crate::command::vector_search::ft_dropindex(&mut vs, &mut ts, cmd_args)
                        } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                            crate::command::vector_search::ft_info(&vs, &ts, cmd_args)
                        } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                            crate::command::vector_search::ft_list(&vs)
                        } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                            crate::command::vector_search::ft_compact(&mut vs, &mut ts, cmd_args)
                        } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                            crate::command::vector_search::cache_search::ft_cachesearch(
                                &mut vs, cmd_args,
                            )
                        } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                            crate::command::vector_search::ft_config(&mut vs, &mut ts, cmd_args)
                        } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                            let mut db_guard = shard_databases_ref.write_db(ctx.shard_id, 0);
                            crate::command::vector_search::recommend::ft_recommend(
                                &mut vs,
                                cmd_args,
                                Some(&mut *db_guard),
                            )
                        } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                            #[cfg(feature = "graph")]
                            {
                                let graph_guard =
                                    shard_databases_ref.graph_store_read(ctx.shard_id);
                                crate::command::vector_search::navigate::ft_navigate(
                                    &mut vs,
                                    Some(&graph_guard),
                                    cmd_args,
                                    None,
                                )
                            }
                            #[cfg(not(feature = "graph"))]
                            {
                                Frame::Error(Bytes::from_static(
                                    b"ERR FT.NAVIGATE requires graph feature",
                                ))
                            }
                        } else if cmd.eq_ignore_ascii_case(b"FT.EXPAND") {
                            #[cfg(feature = "graph")]
                            {
                                let graph_guard =
                                    shard_databases_ref.graph_store_read(ctx.shard_id);
                                crate::command::vector_search::ft_expand(&graph_guard, cmd_args)
                            }
                            #[cfg(not(feature = "graph"))]
                            {
                                Frame::Error(Bytes::from_static(
                                    b"ERR FT.EXPAND requires graph feature",
                                ))
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
                    ctx.shard_databases
                        .wal_append(ctx.shard_id, bytes::Bytes::from(record));
                }
                responses.push(response);
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
                // LOCAL PATH: split into read/write to avoid exclusive lock on reads.
                // Using read_db for local reads eliminates RwLock contention with
                // cross-shard shared reads from other shard threads.
                if metadata::is_write(cmd) {
                    // WRITE PATH: eviction + dispatch under write lock.
                    // When disk offload is enabled, use async spill: evicted keys
                    // are sent to SpillThread for background pwrite to NVMe.
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

                    let dispatch_start = std::time::Instant::now();
                    let result =
                        dispatch(&mut guard, cmd, cmd_args, &mut conn.selected_db, db_count);
                    let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                    if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                        crate::admin::metrics_setup::record_command(cmd_str, elapsed_us);
                    }
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
                            let mut vs = ctx.shard_databases.vector_store(ctx.shard_id);
                            let mut ts = ctx.shard_databases.text_store(ctx.shard_id);
                            crate::shard::spsc_handler::auto_index_hset_public(
                                &mut vs,
                                &mut *ts,
                                key.as_ref(),
                                cmd_args,
                            );
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
                    let response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                    responses.push(response);
                } else {
                    // READ PATH: shared lock — no contention with other shards' reads
                    let guard = ctx.shard_databases.read_db(ctx.shard_id, conn.selected_db);
                    let now_ms = ctx.cached_clock.ms();
                    let dispatch_start = std::time::Instant::now();
                    let result = dispatch_read(
                        &guard,
                        cmd,
                        cmd_args,
                        now_ms,
                        &mut conn.selected_db,
                        db_count,
                    );
                    let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                    if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                        crate::admin::metrics_setup::record_command(cmd_str, elapsed_us);
                    }
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
                    let response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                    responses.push(response);
                } // end read/write split

            // (tracking and response push handled inside read/write branches above)
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
                if !metadata::is_write(cmd)
                    && !remote_groups.contains_key(&target)
                    && is_dispatch_read_supported(cmd)
                {
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
                    let response = apply_resp3_conversion(cmd, response, conn.protocol_version);
                    responses.push(response);
                    continue;
                }
                // Cross-shard write: deferred SPSC dispatch (unchanged)
                let resp_idx = responses.len();
                responses.push(Frame::Null); // placeholder, filled after batch dispatch
                // Pre-compute AOF bytes before moving frame into Arc
                let aof_bytes = if ctx.aof_tx.is_some() && metadata::is_write(cmd) {
                    Some(aof::serialize_command(&frame))
                } else {
                    None
                };
                // Zero-copy: extract Bytes from frame's first element (refcount bump, no alloc).
                let cmd_bytes = if let Frame::Array(ref args) = frame {
                    extract_bytes(&args[0]).unwrap_or_default()
                } else {
                    Bytes::new()
                };
                remote_groups.entry(target).or_default().push((
                    resp_idx,
                    std::sync::Arc::new(frame),
                    aof_bytes,
                    cmd_bytes,
                ));
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
