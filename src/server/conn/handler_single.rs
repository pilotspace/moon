// Note: some imports/variables may be conditionally used across feature flags
//! Single-thread tokio connection handler.
//!
//! Extracted from `server/connection.rs` (Plan 48-02).

use crate::runtime::TcpStream;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;
use bytes::{Bytes, BytesMut};

/// c10k C1 — bound a reply send so a peer that stops reading cannot park the
/// handler forever.
///
/// `Framed::send` is feed + flush; the flush blocks once the socket's receive
/// window closes and never returns. The handler then holds its `maxclients`
/// slot, and its share of the batch's serialized replies, for as long as the
/// client cares to wait. N such clients is an OOM that costs the attacker
/// nothing but an unread socket.
///
/// Cancelling the flush may leave a partial frame on the wire; that is
/// correct here, because the only thing we do afterwards is close the
/// connection. Evaluates to `true` when the send completed, `false` when it
/// failed or stalled. `$wt` is an `Option<Duration>`; `None` keeps the pre-C1
/// wait-forever behaviour.
macro_rules! send_bounded {
    ($framed:expr, $frame:expr, $wt:expr, $client_id:expr) => {{
        match $wt {
            None => $framed.send($frame).await.is_ok(),
            Some(dur) => match tokio::time::timeout(dur, $framed.send($frame)).await {
                Ok(r) => r.is_ok(),
                Err(_) => {
                    tracing::warn!(
                        "Connection {} reply write made no progress for {}ms — \
                         closing (client is not reading)",
                        $client_id,
                        dur.as_millis(),
                    );
                    false
                }
            },
        }
    }};
}

use futures::{FutureExt, SinkExt, StreamExt};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio_util::codec::Framed;

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::protocol::Frame;
use crate::pubsub::subscriber::Subscriber;
use crate::pubsub::{self, PubSubRegistry};
use crate::storage::eviction::{EvictionRun, evict_to_budget};
use crate::tracking::{TrackingState, TrackingTable};

use super::shared::resolve_ft_search_as_of_lsn;
use super::{
    SharedDatabases, apply_resp3_conversion, execute_transaction, extract_bytes, extract_command,
    handle_config,
};
use crate::framevec;
use crate::server::codec::RespCodec;

/// Flush AOF entries and responses under the `appendfsync=always` ordering contract (H1).
///
/// **Invariant (H1):** awaits ALL fsync acks BEFORE sending ANY response to the
/// client.  The client must never receive `+OK` before the entry is durable on
/// disk when `appendfsync=always` is configured.
///
/// Returns `true` when the connection loop should break (sink send failed).
///
/// Making the sink generic (`S: futures::Sink<Frame> + Unpin`) lets unit tests
/// supply a lightweight recording sink instead of a real TcpStream, while the
/// production call site passes `&mut framed` (`Framed<TcpStream, RespCodec>`)
/// unchanged — both satisfy the bound.
///
/// # Arguments
///
/// - `sink` — frame sink; production passes `Framed<TcpStream, RespCodec>`,
///   tests pass any `Sink<Frame>` mock.
/// - `responses` — per-command response slots; fsync failures patch the
///   corresponding slot to `Frame::Error`.
/// - `aof_entries` — `(resp_idx, db, bytes)`: bytes to fsync (executed in `db`),
///   slot to patch on failure.
/// - `pool` — AOF writer pool (caller must ensure Always policy).
/// - `repl_state` — replication state for LSN issuance (`&None` in tests).
/// - `change_counter` — auto-save dirty counter (`&None` if not configured).
pub(crate) async fn flush_with_aof_ack<S>(
    sink: &mut S,
    mut responses: Vec<Frame>,
    aof_entries: Vec<(usize, usize, Bytes)>,
    pool: &crate::persistence::aof::AofWriterPool,
    repl_state: &Option<Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>>,
    change_counter: &Option<Arc<AtomicU64>>,
) -> bool
where
    S: futures::Sink<Frame> + Unpin,
{
    // Phase 1 — group commit: enqueue every append fire-and-forget, then
    // confirm the whole batch with ONE fsync barrier (Always) instead of an
    // awaited fsync per entry — same contract as the sharded handlers'
    // resolve_local_leg_barrier. On barrier failure every enqueued write in
    // the batch is unconfirmed, so every joined slot is patched.
    let mut barrier_idxs: Vec<usize> = Vec::new();
    for (resp_idx, db, bytes) in aof_entries {
        let lsn =
            crate::persistence::aof::AofWriterPool::issue_append_lsn(repl_state, 0, bytes.len());
        match pool.send_append_group(0, lsn, db, bytes).await {
            Ok(true) => barrier_idxs.push(resp_idx),
            Ok(false) => {}
            Err(_) => {
                if resp_idx < responses.len() {
                    responses[resp_idx] =
                        Frame::Error(Bytes::from_static(b"WRITEFAIL aof fsync failed"));
                }
            }
        }
        if let Some(counter) = change_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
    if !barrier_idxs.is_empty() && pool.fsync_barrier(0).await.is_err() {
        for resp_idx in barrier_idxs {
            if resp_idx < responses.len() {
                responses[resp_idx] =
                    Frame::Error(Bytes::from_static(b"WRITEFAIL aof fsync failed"));
            }
        }
    }
    // Phase 2 — all acks received; flush responses to client.
    let mut break_outer = false;
    for response in responses {
        if SinkExt::send(sink, response).await.is_err() {
            break_outer = true;
            break;
        }
    }
    break_outer
}

/// Handle a single client connection.
///
/// Reads frames from the TCP stream, dispatches commands, and writes responses.
/// Terminates on client disconnect, protocol error, QUIT command, or server shutdown.
///
/// When `requirepass` is set, clients must authenticate via AUTH before any other
/// commands are accepted (except QUIT).
///
/// When `aof_pool` is provided, write commands are logged via the per-shard
/// AOF writer pool. handler_single is single-shard mode by definition
/// (num_shards = 1, shard_id = 0), so the pool is always a TopLevel layout
/// wrapping a single writer sender — see `AofWriterPool::top_level`.
/// When `change_counter` is provided, write commands increment the counter for auto-save.
///
/// Supports Pub/Sub subscriber mode: when a client subscribes to channels/patterns,
/// the connection enters subscriber mode where only SUBSCRIBE, UNSUBSCRIBE,
/// PSUBSCRIBE, PUNSUBSCRIBE, PING, and QUIT commands are accepted. Published
/// messages are forwarded via tokio::select! on the subscriber's mpsc receiver.
///
/// Pipeline batching: In normal mode, collects all immediately available frames
/// into a batch, executes them under a single lock acquisition, then writes all
/// responses outside the lock. This reduces lock acquisitions from N per pipeline
/// to 1 per batch cycle.
#[tracing::instrument(skip_all, level = "debug")]
pub async fn handle_connection(
    stream: TcpStream,
    db: SharedDatabases,
    shutdown: CancellationToken,
    requirepass: Option<String>,
    config: Arc<ServerConfig>,
    aof_pool: Option<Arc<crate::persistence::aof::AofWriterPool>>,
    change_counter: Option<Arc<AtomicU64>>,
    pubsub_registry: Arc<Mutex<PubSubRegistry>>,
    runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
    tracking_table: Arc<Mutex<TrackingTable>>,
    client_id: u64,
    repl_state: Option<Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>>,
    acl_table: Arc<RwLock<crate::acl::AclTable>>,
    vector_store: Option<Arc<Mutex<crate::vector::store::VectorStore>>>,
    text_store: Option<Arc<Mutex<crate::text::store::TextStore>>>,
    #[cfg(feature = "graph")] graph_store: Option<Arc<Mutex<crate::graph::store::GraphStore>>>,
) {
    crate::admin::metrics_setup::record_connection_opened();
    // Capture peer address before Framed wraps the stream (stream is moved)
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let mut framed = Framed::new(stream, RespCodec::default());
    let mut conn = super::core::ConnectionState::new(
        client_id,
        peer_addr.clone(),
        &requirepass,
        0,     // shard_id: single-shard mode
        1,     // num_shards: single-shard mode
        false, // can_migrate: handler_single doesn't support migration
        runtime_config.read().acllog_max_len,
        None, // no migrated state
    );
    conn.refresh_acl_cache(&acl_table);

    // c10k C2: arm the query-buffer ceiling. This handler reads through
    // `Framed`, which loops read -> decode -> read internally and does not
    // yield while a frame is incomplete — so the ceiling lives in the codec
    // (see `RespCodec::max_query_buf`) rather than in this loop. A connection
    // that starts out already authenticated (no requirepass) gets the full
    // limit immediately; the rest are raised by `arm_query_buf_limit` on
    // their first successful AUTH/HELLO.
    let (qbuf_limit, qbuf_preauth, write_timeout_ms) = {
        let rt = runtime_config.read();
        (
            rt.client_query_buffer_limit,
            rt.client_query_buffer_limit_preauth,
            rt.client_write_timeout_ms,
        )
    };
    // c10k C1: reply-write ceiling. 0 = wait forever (pre-C1 behaviour).
    let write_timeout = match write_timeout_ms {
        0 => None,
        ms => Some(std::time::Duration::from_millis(ms)),
    };

    // Per-connection arena for batch processing temporaries.
    // Primary use in Phase 8: scratch buffer during inline token assembly.
    // Phase 9+ will leverage this for per-request temporaries.
    let mut arena = Bump::with_capacity(4096); // 4KB initial capacity

    loop {
        // c10k C2: re-arm the ceiling every pass rather than at each of the
        // five AUTH/HELLO success sites — two local loads and a field store,
        // and it cannot be forgotten when a sixth site appears. It must run
        // before `framed.next()`, which is exactly here.
        framed
            .codec_mut()
            .set_max_query_buf(super::util::query_buf_limit(
                conn.authenticated,
                qbuf_limit,
                qbuf_preauth,
            ));
        // Subscriber mode: bidirectional select on client commands + published messages
        if conn.subscription_count > 0 {
            #[allow(clippy::unwrap_used)]
            // conn.pubsub_rx is always Some when conn.subscription_count > 0
            let rx = conn.pubsub_rx.as_mut().unwrap();
            tokio::select! {
                result = framed.next() => {
                    match result {
                        Some(Ok(frame)) => {
                            if let Some((cmd, cmd_args)) = extract_command(&frame) {
                                match cmd {
                                    _ if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") => {
                                        if cmd_args.is_empty() {
                                            let _ = framed.send(Frame::Error(
                                                Bytes::from_static(b"ERR wrong number of arguments for 'subscribe' command"),
                                            )).await;
                                            continue;
                                        }
                                        for arg in cmd_args {
                                            if let Some(channel) = extract_bytes(arg) {
                                                // ACL channel permission check
                                                let deny = {
                                                    #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                    let acl_guard = acl_table.read().unwrap();
                                                    acl_guard.check_channel_permission(&conn.current_user, channel.as_ref())
                                                };
                                                if let Some(deny_reason) = deny {
                                                    let _ = framed.send(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)))).await;
                                                    continue;
                                                }
                                                #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some in subscriber mode
                                                let sub = Subscriber::with_protocol(
                                                    conn.pubsub_tx.clone().unwrap(),
                                                    conn.subscriber_id,
                                                    framed.codec().protocol_version() == 3,
                                                );
                                                pubsub_registry.lock().subscribe(channel.clone(), sub);
                                                conn.subscription_count += 1;
                                                if framed.send(pubsub::subscribe_response(&channel, conn.subscription_count)).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") => {
                                        if cmd_args.is_empty() {
                                            // Unsubscribe from all channels
                                            let removed = pubsub_registry.lock().unsubscribe_all(conn.subscriber_id);
                                            if removed.is_empty() {
                                                // No channels, send response with count 0
                                                conn.subscription_count = pubsub_registry.lock().total_subscription_count(conn.subscriber_id);
                                                let _ = framed.send(pubsub::unsubscribe_response(
                                                    &Bytes::from_static(b""),
                                                    conn.subscription_count,
                                                )).await;
                                            } else {
                                                for ch in &removed {
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::unsubscribe_response(ch, conn.subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(channel) = extract_bytes(arg) {
                                                    pubsub_registry.lock().unsubscribe(channel.as_ref(), conn.subscriber_id);
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::unsubscribe_response(&channel, conn.subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") => {
                                        if cmd_args.is_empty() {
                                            let _ = framed.send(Frame::Error(
                                                Bytes::from_static(b"ERR wrong number of arguments for 'psubscribe' command"),
                                            )).await;
                                            continue;
                                        }
                                        for arg in cmd_args {
                                            if let Some(pattern) = extract_bytes(arg) {
                                                // ACL channel permission check
                                                let deny = {
                                                    #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                    let acl_guard = acl_table.read().unwrap();
                                                    acl_guard.check_channel_permission(&conn.current_user, pattern.as_ref())
                                                };
                                                if let Some(deny_reason) = deny {
                                                    let _ = framed.send(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)))).await;
                                                    continue;
                                                }
                                                #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some in subscriber mode
                                                let sub = Subscriber::with_protocol(
                                                    conn.pubsub_tx.clone().unwrap(),
                                                    conn.subscriber_id,
                                                    framed.codec().protocol_version() == 3,
                                                );
                                                pubsub_registry.lock().psubscribe(pattern.clone(), sub);
                                                conn.subscription_count += 1;
                                                if framed.send(pubsub::psubscribe_response(&pattern, conn.subscription_count)).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") => {
                                        if cmd_args.is_empty() {
                                            let removed = pubsub_registry.lock().punsubscribe_all(conn.subscriber_id);
                                            if removed.is_empty() {
                                                conn.subscription_count = pubsub_registry.lock().total_subscription_count(conn.subscriber_id);
                                                let _ = framed.send(pubsub::punsubscribe_response(
                                                    &Bytes::from_static(b""),
                                                    conn.subscription_count,
                                                )).await;
                                            } else {
                                                for pat in &removed {
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::punsubscribe_response(pat, conn.subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(pattern) = extract_bytes(arg) {
                                                    pubsub_registry.lock().punsubscribe(pattern.as_ref(), conn.subscriber_id);
                                                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::punsubscribe_response(&pattern, conn.subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"HELLO") => {
                                        // HELLO allowed in subscriber mode (Redis 7+)
                                        let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                                            cmd_args,
                                            framed.codec().protocol_version(),
                                            client_id,
                                            &acl_table,
                                            &mut conn.authenticated,
                                            // Derived, never assumed. This
                                            // handler is reached only via
                                            // `listener::run_with_shutdown`
                                            // (tokio-only; `main.rs` and
                                            // `embedded.rs` both route through
                                            // `run_sharded`), and THAT listener
                                            // passes a real ReplicationState —
                                            // `Some(rs)` at listener.rs:315.
                                            // So a replica here would announce
                                            // itself a master if this were the
                                            // constant it used to be, which is
                                            // the exact defect class this task
                                            // exists to close. No cluster state
                                            // reaches this handler, so the mode
                                            // is standalone.
                                            crate::command::identity::hello_role_and_mode(
                                                repl_state.as_ref(),
                                                false,
                                            ),
                                        );
                                        if !matches!(&response, Frame::Error(_)) {
                                            framed.codec_mut().set_protocol_version(new_proto);
                                        }
                                        if let Some(name) = new_name {
                                            conn.client_name = Some(name);
                                        }
                                        if let Some(uname) = opt_user {
                                            conn.adopt_user(uname, &acl_table);
                                        }
                                        let _ = framed.send(response).await;
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"PING") => {
                                        // In subscriber mode, PING returns Array per Redis spec
                                        let _ = framed.send(Frame::Array(framevec![
                                            Frame::BulkString(Bytes::from_static(b"pong")),
                                            Frame::BulkString(Bytes::from_static(b"")),
                                        ])).await;
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"QUIT") => {
                                        let _ = framed.send(Frame::SimpleString(Bytes::from_static(b"OK"))).await;
                                        break;
                                    }
                                    _ => {
                                        let cmd_str = String::from_utf8_lossy(cmd);
                                        let _ = framed.send(Frame::Error(Bytes::from(format!(
                                            "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / HELLO are allowed in this context",
                                            cmd_str.to_lowercase()
                                        )))).await;
                                    }
                                }
                            }
                            // If conn.subscription_count dropped to 0, exit subscriber mode
                            if conn.subscription_count == 0 {
                                continue;
                            }
                        }
                        Some(Err(_)) => break,
                        None => break,
                    }
                }
                msg = rx.recv_async() => {
                    match msg {
                        Ok(data) => {
                            // Data is pre-serialized RESP. Coalesce any burst already
                            // queued into ONE PreSerialized send — one flush/syscall per
                            // burst instead of per message. Single-message case stays
                            // zero-copy via the is_empty fast path.
                            const MAX_COALESCE_BYTES: usize = 64 * 1024;
                            let payload = if rx.is_empty() {
                                data
                            } else {
                                let mut agg = BytesMut::with_capacity((data.len() * 4).min(MAX_COALESCE_BYTES));
                                agg.extend_from_slice(&data);
                                while agg.len() < MAX_COALESCE_BYTES {
                                    match rx.try_recv() {
                                        Ok(next) => agg.extend_from_slice(&next),
                                        Err(_) => break,
                                    }
                                }
                                agg.freeze()
                            };
                            if !send_bounded!(framed, Frame::PreSerialized(payload), write_timeout, client_id) {
                                break;
                            }
                        }
                        Err(_) => {
                            // All senders dropped (shouldn't happen normally)
                            break;
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    let _ = framed.send(Frame::Error(
                        Bytes::from_static(b"ERR server shutting down")
                    )).await;
                    break;
                }
            }
            continue;
        }

        // Normal mode with pipeline batching
        tokio::select! {
            first_result = framed.next() => {
                let first_frame = match first_result {
                    Some(Ok(frame)) => frame,
                    Some(Err(_)) => {
                        // Name the fault before closing. Nothing valid
                        // preceded it — this IS the first frame of the read.
                        if let Some(kind) = framed.codec_mut().take_last_fault() {
                            use tokio::io::AsyncWriteExt;
                            let msg = crate::server::conn::util::proto_error_frame(kind);
                            let _ = framed.get_mut().write_all(msg.as_bytes()).await;
                        }
                        break;
                    }
                    None => break,
                };

                // Collect batch: first frame + all immediately available frames
                let mut batch = vec![first_frame];
                const MAX_BATCH: usize = 1024;
                // Set when a later frame in this same read is malformed: the
                // batch collected so far still runs and is answered, and the
                // fault is reported after it — never instead of it.
                let mut proto_fault: Option<crate::protocol::ProtoFault> = None;
                while batch.len() < MAX_BATCH {
                    match framed.next().now_or_never() {
                        Some(Some(Ok(frame))) => batch.push(frame),
                        Some(Some(Err(_))) => {
                            proto_fault = framed.codec_mut().take_last_fault();
                            break;
                        }
                        _ => break,
                    }
                }

                // Process batch using two-phase execution:
                // Phase 1: Handle connection-level intercepts, collect dispatchable frames
                // Phase 2: Acquire ONE write lock, execute ALL dispatchable frames
                let mut responses: Vec<Frame> = Vec::with_capacity(batch.len());
                // Each entry carries (resp_idx, db, bytes) so the Always-policy flush
                // path can patch responses[resp_idx] with WRITEFAIL when fsync fails,
                // before any response is sent to the client (H1 fix — FIX-W1-1).
                // task #35: `db` is the connection's selected db at the moment this
                // entry was recorded (only SELECT changes it, and SELECT itself is
                // never persisted, so it is stable for every persisted entry).
                let mut aof_entries: Vec<(usize, usize, Bytes)> = Vec::new();
                let mut should_quit = false;
                let mut break_outer = false;

                // Dispatchable frame: (response_index, frame, is_write, aof_bytes)
                let mut dispatchable: Vec<(usize, Frame, bool, Option<Bytes>)> = Vec::new();

                // === Phase 1: Connection-level intercepts ===
                for frame in batch {
                    // --- AUTH gate (unauthenticated) ---
                    if !conn.authenticated {
                        match extract_command(&frame) {
                            Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"AUTH") => {
                                let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                                if let Some(uname) = opt_user {
                                    conn.authenticated = true;
                                    conn.adopt_user(uname, &acl_table);
                                } else {
                                    // Log failed auth attempt
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
                                // HELLO allowed when unauthenticated (can carry AUTH)
                                let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                                    cmd_args,
                                    framed.codec().protocol_version(),
                                    client_id,
                                    &acl_table,
                                    &mut conn.authenticated,
                                    // Derived from repl_state, never assumed
                                    // — see the HELLO site above.
                                    crate::command::identity::hello_role_and_mode(
                                        repl_state.as_ref(),
                                        false,
                                    ),
                                );
                                // CRITICAL: Set protocol version BEFORE sending response (Pitfall 6)
                                if !matches!(&response, Frame::Error(_)) {
                                    framed.codec_mut().set_protocol_version(new_proto);
                                }
                                if let Some(name) = new_name {
                                    conn.client_name = Some(name);
                                }
                                if let Some(uname) = opt_user {
                                    conn.adopt_user(uname, &acl_table);
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

                    // --- Connection-level command intercepts (no db lock needed) ---
                    if let Some((cmd, cmd_args)) = extract_command(&frame) {
                        // AUTH when already conn.authenticated
                        if cmd.eq_ignore_ascii_case(b"AUTH") {
                            let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                            if let Some(uname) = opt_user {
                                conn.adopt_user(uname, &acl_table);
                            }
                            responses.push(response);
                            continue;
                        }
                        // HELLO -- protocol negotiation (ACL-aware)
                        if cmd.eq_ignore_ascii_case(b"HELLO") {
                            let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                                cmd_args,
                                framed.codec().protocol_version(),
                                client_id,
                                &acl_table,
                                &mut conn.authenticated,
                                // Derived from repl_state, never assumed —
                                // see the HELLO site above.
                                crate::command::identity::hello_role_and_mode(
                                    repl_state.as_ref(),
                                    false,
                                ),
                            );
                            // CRITICAL: Set protocol version BEFORE sending response (Pitfall 6)
                            if !matches!(&response, Frame::Error(_)) {
                                framed.codec_mut().set_protocol_version(new_proto);
                            }
                            if let Some(name) = new_name {
                                conn.client_name = Some(name);
                            }
                            if let Some(uname) = opt_user {
                                conn.adopt_user(uname, &acl_table);
                            }
                            responses.push(response);
                            continue;
                        }
                        // ACL command -- intercepted at connection level
                        if cmd.eq_ignore_ascii_case(b"ACL") {
                            let response = crate::command::acl::handle_acl(
                                cmd_args,
                                &acl_table,
                                &mut conn.acl_log,
                                &conn.current_user,
                                &peer_addr,
                                &runtime_config,
                                client_id,
                            );
                            responses.push(response);
                            continue;
                        }
                        // CLIENT subcommands (ID, SETNAME, GETNAME, TRACKING)
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

                                                        let mut table = tracking_table.lock();
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
                                                    tracking_table.lock().untrack_all(client_id);
                                                    conn.tracking_rx = None;
                                                    responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                                }
                                                continue;
                                            }
                                            Err(err_frame) => {
                                                responses.push(err_frame);
                                                continue;
                                            }
                                        }
                                    }
                                    if sub_bytes.eq_ignore_ascii_case(b"PAUSE") {
                                        // CLIENT PAUSE timeout [WRITE|ALL]
                                        if cmd_args.len() < 2 {
                                            responses.push(Frame::Error(Bytes::from_static(
                                                b"ERR wrong number of arguments for 'CLIENT PAUSE' command",
                                            )));
                                        } else {
                                            let timeout_ms = match extract_bytes(&cmd_args[1]) {
                                                Some(b) => std::str::from_utf8(&b).ok().and_then(|s| s.parse::<u64>().ok()),
                                                None => None,
                                            };
                                            let mode_valid = if cmd_args.len() >= 3 {
                                                match extract_bytes(&cmd_args[2]) {
                                                    Some(b) => b.eq_ignore_ascii_case(b"WRITE") || b.eq_ignore_ascii_case(b"ALL"),
                                                    None => false,
                                                }
                                            } else {
                                                true
                                            };
                                            if cmd_args.len() > 3 || !mode_valid {
                                                responses.push(Frame::Error(Bytes::from_static(
                                                    b"ERR syntax error",
                                                )));
                                            } else {
                                                match timeout_ms {
                                                    Some(ms) => {
                                                        let write_only = cmd_args.get(2)
                                                            .and_then(|f| extract_bytes(f))
                                                            .is_some_and(|b| b.eq_ignore_ascii_case(b"WRITE"));
                                                        let deadline = crate::storage::entry::current_time_ms().saturating_add(ms);
                                                        let mut rt = runtime_config.write();
                                                        rt.client_pause_deadline_ms = deadline;
                                                        rt.client_pause_write_only = write_only;
                                                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                                    }
                                                    None => {
                                                        responses.push(Frame::Error(Bytes::from_static(
                                                            b"ERR timeout is not a valid integer or out of range",
                                                        )));
                                                    }
                                                }
                                            }
                                        }
                                        continue;
                                    }
                                    if sub_bytes.eq_ignore_ascii_case(b"UNPAUSE") {
                                        let mut rt = runtime_config.write();
                                        rt.client_pause_deadline_ms = 0;
                                        rt.client_pause_write_only = false;
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
                            responses.push(Frame::Error(
                                Bytes::from_static(b"ERR wrong number of arguments for 'client' command"),
                            ));
                            continue;
                        }
                        // BGSAVE -- handle outside lock
                        if cmd.eq_ignore_ascii_case(b"BGSAVE") {
                            let response = crate::command::persistence::bgsave_start(
                                db.clone(),
                                config.dir.clone(),
                                config.dbfilename.clone(),
                            );
                            responses.push(response);
                            continue;
                        }
                        // SAVE -- synchronous save (single-threaded mode only)
                        if cmd.eq_ignore_ascii_case(b"SAVE") {
                            let response = crate::command::persistence::handle_save(
                                &db,
                                &config.dir,
                                &config.dbfilename,
                            );
                            responses.push(response);
                            continue;
                        }
                        // LASTSAVE -- return timestamp of last successful save
                        if cmd.eq_ignore_ascii_case(b"LASTSAVE") {
                            responses.push(crate::command::persistence::handle_lastsave());
                            continue;
                        }
                        // SHUTDOWN [NOSAVE|SAVE] -- Redis parity: on success no reply
                        // is sent (the client observes the connection close as the
                        // server exits); on failure (bad syntax, or a forced SAVE
                        // that failed) an error is returned and the server stays up.
                        if cmd.eq_ignore_ascii_case(b"SHUTDOWN") {
                            match crate::command::persistence::parse_shutdown_args(cmd_args) {
                                Ok(mode) => {
                                    use crate::command::persistence::ShutdownSaveMode;
                                    let should_save = match mode {
                                        ShutdownSaveMode::Save => true,
                                        ShutdownSaveMode::NoSave => false,
                                        ShutdownSaveMode::Default => {
                                            crate::command::persistence::shutdown_default_should_save(
                                                config.save.as_deref(),
                                            )
                                        }
                                    };
                                    if should_save
                                        && let err @ Frame::Error(_) = crate::command::persistence::handle_save(
                                            &db,
                                            &config.dir,
                                            &config.dbfilename,
                                        )
                                    {
                                        responses.push(err);
                                        continue;
                                    }
                                    tracing::info!(
                                        "SHUTDOWN command received -- initiating graceful shutdown"
                                    );
                                    shutdown.cancel();
                                    should_quit = true;
                                    break;
                                }
                                Err(e) => {
                                    responses.push(e);
                                    continue;
                                }
                            }
                        }
                        // BGREWRITEAOF
                        if cmd.eq_ignore_ascii_case(b"BGREWRITEAOF") {
                            let response = if let Some(ref pool) = aof_pool {
                                crate::command::persistence::bgrewriteaof_start(pool, db.clone())
                            } else {
                                Frame::Error(Bytes::from_static(b"ERR AOF is not enabled"))
                            };
                            responses.push(response);
                            continue;
                        }
                        // SWAPDB — atomically exchange two databases (single-shard path).
                        if cmd.eq_ignore_ascii_case(b"SWAPDB") {
                            if cmd_args.len() != 2 {
                                responses.push(Frame::Error(Bytes::from_static(
                                    b"ERR wrong number of arguments for 'swapdb' command",
                                )));
                                continue;
                            }
                            let parse_idx = |f: &Frame| -> Option<usize> {
                                match f {
                                    Frame::BulkString(b) => {
                                        std::str::from_utf8(b).ok()?.parse::<usize>().ok()
                                    }
                                    Frame::Integer(n) => usize::try_from(*n).ok(),
                                    _ => None,
                                }
                            };
                            let idx_a = cmd_args.first().and_then(parse_idx);
                            let idx_b = cmd_args.get(1).and_then(parse_idx);
                            let resp = match (idx_a, idx_b) {
                                (Some(a), Some(b)) => {
                                    let db_count = db.len();
                                    if a >= db_count || b >= db_count {
                                        Frame::Error(Bytes::from_static(
                                            b"ERR DB index is out of range",
                                        ))
                                    } else if a == b {
                                        // Same-index: no-op, return OK.
                                        Frame::SimpleString(Bytes::from_static(b"OK"))
                                    } else if crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                                        .load(std::sync::atomic::Ordering::SeqCst)
                                    {
                                        Frame::Error(Bytes::from_static(
                                            b"ERR cannot SWAPDB during BGREWRITEAOF",
                                        ))
                                    } else {
                                        // WAL must be durable BEFORE the swap (no rollback
                                        // path for SWAPDB). Use try_send_append_durable so
                                        // that the fsync policy is honoured:
                                        //   - appendfsync=always  → await AppendSync ack
                                        //     (rendezvous guarantees data is on disk before +OK)
                                        //   - appendfsync=everysec/no → fire-and-forget (fast)
                                        // On any Err the caller aborts and leaves both DBs
                                        // untouched, preserving atomicity from the WAL's perspective.
                                        let mut a_buf = itoa::Buffer::new();
                                        let mut b_buf = itoa::Buffer::new();
                                        let wal_frame = Frame::Array(crate::framevec![
                                            Frame::BulkString(Bytes::from_static(b"SWAPDB")),
                                            Frame::BulkString(Bytes::copy_from_slice(
                                                a_buf.format(a).as_bytes()
                                            )),
                                            Frame::BulkString(Bytes::copy_from_slice(
                                                b_buf.format(b).as_bytes()
                                            )),
                                        ]);
                                        let serialized =
                                            crate::persistence::aof::serialize_command(&wal_frame);
                                        let wal_ok = if let Some(ref pool) = aof_pool {
                                            // Single-shard mode — shard_id = 0.
                                            let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(&repl_state, 0, serialized.len());
                                            // task #35: SWAPDB affects both `a` and
                                            // `b` — no single db context applies;
                                            // pass 0 (writer may emit a harmless
                                            // redundant SELECT 0).
                                            pool.try_send_append_durable(0, lsn, 0, serialized.clone())
                                                .await
                                                .is_ok()
                                        } else {
                                            true // persistence disabled — no durability requirement
                                        };
                                        if !wal_ok {
                                            Frame::Error(Bytes::from_static(
                                                b"ERR SWAPDB aborted: WAL enqueue failed (persistence backpressure)",
                                            ))
                                        } else {
                                            let (lo, hi) =
                                                if a < b { (a, b) } else { (b, a) };
                                            // Acquire in ascending index order (deadlock prevention).
                                            let mut guard_lo = db[lo].write();
                                            let mut guard_hi = db[hi].write();
                                            std::mem::swap(&mut *guard_lo, &mut *guard_hi);
                                            drop(guard_hi);
                                            drop(guard_lo);
                                            // #386 — replication plane, exactly once per
                                            // client SWAPDB, AFTER the durability gate and
                                            // the swap itself (mirrors the coordinator leg).
                                            crate::replication::state::record_local_write_global(
                                                0, serialized,
                                            );
                                            Frame::SimpleString(Bytes::from_static(b"OK"))
                                        }
                                    }
                                }
                                _ => Frame::Error(Bytes::from_static(
                                    b"ERR value is not an integer or out of range",
                                )),
                            };
                            responses.push(resp);
                            continue;
                        }
                        // CONFIG
                        if cmd.eq_ignore_ascii_case(b"CONFIG") {
                            responses.push(handle_config(cmd_args, &runtime_config, &config));
                            continue;
                        }

                        // --- REPLICAOF / SLAVEOF ---
                        if cmd.eq_ignore_ascii_case(b"REPLICAOF")
                            || cmd.eq_ignore_ascii_case(b"SLAVEOF")
                        {
                            use crate::command::connection::{replicaof, ReplicaofAction};
                            let (resp, action) = replicaof(cmd_args);
                            if let Some(action) = action {
                                if let Some(ref rs) = repl_state {
                                    match action {
                                        ReplicaofAction::StartReplication { host, port } => {
                                            rs.write().set_role(crate::replication::state::ReplicationRole::Replica {
                                                host: host.clone(),
                                                port,
                                                state: crate::replication::handshake::ReplicaHandshakeState::PingPending,
                                            });
                                        }
                                        ReplicaofAction::PromoteToMaster => {
                                            use crate::replication::state::generate_repl_id;
                                            // handler_single spawns no replica task itself, but
                                            // bump the generation anyway so any task spawned by
                                            // another handler path stops applying.
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
                            continue;
                        }

                        // --- REPLCONF ---
                        if cmd.eq_ignore_ascii_case(b"REPLCONF") {
                            let resp = crate::command::connection::replconf(cmd_args);
                            responses.push(resp);
                            continue;
                        }

                        // --- PSYNC (unsupported on tokio; clear error, R3/2A) ---
                        if cmd.eq_ignore_ascii_case(b"PSYNC") {
                            responses.push(Frame::Error(Bytes::from_static(
                                b"ERR PSYNC requires runtime-monoio on the master (this build runs runtime-tokio)",
                            )));
                            continue;
                        }

                        // --- WAIT ---
                        if cmd.eq_ignore_ascii_case(b"WAIT") {
                            // WAIT numreplicas timeout
                            let num_required: usize = cmd_args.first()
                                .and_then(|f| extract_bytes(f))
                                .and_then(|b| std::str::from_utf8(&b).ok().and_then(|s| s.parse().ok()))
                                .unwrap_or(0);
                            let timeout_ms: u64 = cmd_args.get(1)
                                .and_then(|f| extract_bytes(f))
                                .and_then(|b| std::str::from_utf8(&b).ok().and_then(|s| s.parse().ok()))
                                .unwrap_or(0);
                            if let Some(ref rs) = repl_state {
                                let count = crate::replication::master::wait_for_replicas(num_required, timeout_ms, rs).await;
                                responses.push(Frame::Integer(count as i64));
                            } else {
                                responses.push(Frame::Integer(0));
                            }
                            continue;
                        }

                        // --- INFO (append replication section) ---
                        if cmd.eq_ignore_ascii_case(b"INFO") {
                            if let Some(ref rs) = repl_state {
                                // # Keyspace parity: every non-empty db, not
                                // just the selected one mislabeled as db0.
                                // Single shard — the local dbs are the truth.
                                let keyspace: Vec<(u64, u64)> = db
                                    .iter()
                                    .map(|d| {
                                        let g = d.read();
                                        // logical_len, not len(): a spilled
                                        // (cold-only) key is still a logical
                                        // key — INFO must agree with DBSIZE
                                        // (#355). Only the embedded /
                                        // non-sharded server drives this
                                        // handler; the sharded binary goes
                                        // through coordinate_keyspace_info.
                                        (g.logical_len() as u64, g.expires_count() as u64)
                                    })
                                    .collect();
                                // Passed in rather than appended — appending
                                // emitted `# Replication` twice.
                                let real_repl = rs.try_read().map(|rs_guard| {
                                    crate::replication::handshake::build_info_replication(&rs_guard)
                                });
                                // One registry on this handler (embedded /
                                // non-sharded), so the union the sharded
                                // handlers perform collapses to a direct read.
                                let pubsub_facts = {
                                    let reg = pubsub_registry.lock();
                                    conn_cmd::InstanceFacts {
                                        pubsub_channels: reg.active_channels(None).len(),
                                        pubsub_patterns: reg.pattern_names().len(),
                                    }
                                };
                                let guard = db[conn.selected_db].read();
                                let resp_frame = conn_cmd::info_with_facts(
                                    &guard,
                                    cmd_args,
                                    &keyspace,
                                    real_repl.as_deref(),
                                    &pubsub_facts,
                                );
                                drop(guard);
                                responses.push(resp_frame);
                                continue;
                            }
                            // Fall through to normal dispatch if no repl_state
                        }

                        // --- READONLY enforcement: reject writes on replicas ---
                        if let Some(ref rs) = repl_state {
                            if let Some(rs_guard) = rs.try_read() {
                                if matches!(
                                    rs_guard.role,
                                    crate::replication::state::ReplicationRole::Replica { .. }
                                ) {
                                    // GRAPH.QUERY is blanket-W (Cypher CAN
                                    // write); serve read-only MATCH/RETURN on
                                    // replicas — see handler_monoio::dispatch.
                                    #[cfg(feature = "graph")]
                                    let graph_ro = cmd.eq_ignore_ascii_case(b"GRAPH.QUERY")
                                        && !crate::command::graph::is_cypher_write_query(cmd_args);
                                    #[cfg(not(feature = "graph"))]
                                    let graph_ro = false;
                                    // SELECT is flagged W but only mutates
                                    // CONNECTION state — serve it on replicas
                                    // (task #23).
                                    let conn_only = cmd.eq_ignore_ascii_case(b"SELECT");
                                    if metadata::is_write(cmd) && !graph_ro && !conn_only {
                                        responses.push(Frame::Error(Bytes::from_static(
                                            b"READONLY You can't write against a read only replica.",
                                        )));
                                        continue;
                                    }
                                }
                            }
                        }

                        // SUBSCRIBE / PSUBSCRIBE: enter subscriber mode
                        // Flush accumulated responses first, then handle subscribe and break batch
                        if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                            // Execute any pending dispatchable frames before switching modes
                            if !dispatchable.is_empty() {
                                let mut guard = db[conn.selected_db].write();
                                guard.refresh_now();
                                let db_count = db.len();
                                for (resp_idx, disp_frame, is_write, aof_bytes) in dispatchable.drain(..) {
                                    #[allow(clippy::unwrap_used)] // Frame was parsed earlier; extract_command succeeds on valid frames
                                    let (d_cmd, d_args) = extract_command(&disp_frame).unwrap();
                                    if is_write {
                                        let rt = runtime_config.read();
                                        // WS6 fix (HIGH, adversarial review
                                        // 2026-07-08): a command that can only
                                        // shrink memory (HDEL, SREM, LPOP, ...)
                                        // must never be REJECTED by either gate
                                        // below, or a key/db that crosses its
                                        // noeviction boundary has no
                                        // self-recovery path. Eviction is still
                                        // attempted; only the reject is
                                        // bypassed. See
                                        // `db_quota::is_shrink_only_command`.
                                        let shrink_only =
                                            crate::storage::db_quota::is_shrink_only_command(
                                                d_cmd,
                                            );
                                        let evict_result =
                                            evict_to_budget(&mut *guard, &rt, EvictionRun::plain());
                                        if !shrink_only {
                                            if let Err(oom_frame) = evict_result {
                                                responses[resp_idx] = oom_frame;
                                                continue;
                                            }
                                        }
                                        // WS5b: per-db quota, additive to the
                                        // whole-instance gate above. `_for_command`
                                        // exempts SELECT/SWAPDB — see
                                        // `db_quota::command_exempt_from_db_quota`.
                                        let db_quota_result =
                                            crate::storage::db_quota::check_db_maxmemory_for_command(&mut *guard, conn.selected_db, &rt, d_cmd);
                                        if !shrink_only {
                                            if let Err(oom_frame) = db_quota_result {
                                                responses[resp_idx] = oom_frame;
                                                continue;
                                            }
                                        }
                                    }
                                    let dispatch_start = std::time::Instant::now();
                                    let result = dispatch(&mut *guard, d_cmd, d_args, &mut conn.selected_db, db_count);
                                    let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                                    if let Ok(cmd_str) = std::str::from_utf8(d_cmd) {
                                        crate::admin::metrics_setup::record_command_cached(
                                            cmd_str,
                                            elapsed_us,
                                            &mut conn.cached_metrics,
                                        );
                                    }
                                    if let Frame::Array(ref args) = disp_frame {
                                        crate::admin::metrics_setup::global_slowlog().maybe_record(
                                            elapsed_us,
                                            args.as_slice(),
                                            peer_addr.as_bytes(),
                                            conn.client_name.as_ref().map_or(b"" as &[u8], |n| n.as_ref()),
                                        );
                                    }
                                    let (response, quit) = match result {
                                        DispatchResult::Response(f) => (f, false),
                                        DispatchResult::Quit(f) => (f, true),
                                    };
                                    if let Some(bytes) = aof_bytes {
                                        if !matches!(&response, Frame::Error(_)) {
                                            // Carry resp_idx so the Always-policy flush can
                                            // patch responses[resp_idx] on fsync failure.
                                            aof_entries.push((resp_idx, conn.selected_db, bytes));
                                        }
                                    }
                                    // Apply RESP3 response conversion if needed
                                    let response = apply_resp3_conversion(
                                    d_cmd,
                                    d_args,
                                    response,
                                    framed.codec().protocol_version(),
                                );
                                    responses[resp_idx] = response;
                                    if quit {
                                        should_quit = true;
                                        break;
                                    }
                                }
                            } // lock dropped here

                            if should_quit {
                                break;
                            }

                            // FIX-W1-1 + FIX-W2-4: Await AOF fsync ack for prior write
                            // commands BEFORE flushing their +OK responses. Ordering:
                            // (a) Under appendfsync=always: WRITEFAIL replaces +OK if
                            //     fsync fails — no +OK is ever sent for a non-durable
                            //     write.
                            // (b) The WRITEFAIL frame lands before the SUBSCRIBE
                            //     response slot, not inside it (prior code flushed
                            //     +OK first, then checked AOF, causing WRITEFAIL to
                            //     be mistaken for the SUBSCRIBE ack by the client).
                            //
                            // For everysec/no policies, try_send_append_durable is
                            // fire-and-forget (returns Ok immediately) so no latency
                            // penalty.
                            //
                            // Note: aof_entries carries (resp_idx, db, bytes) from
                            // FIX-W1-1 — resp_idx is unused here because the
                            // all-or-nothing failure mode discards the entire response
                            // buffer; future per-slot patching could use it.
                            let mut aof_write_failed = false;
                            let mut aof_barrier_needed = false;
                            for (_resp_idx, entry_db, bytes) in aof_entries.drain(..) {
                                if let Some(ref pool) = aof_pool {
                                    let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(&repl_state, 0, bytes.len());
                                    match pool.send_append_group(0, lsn, entry_db, bytes).await {
                                        Ok(true) => aof_barrier_needed = true,
                                        Ok(false) => {}
                                        Err(_) => aof_write_failed = true,
                                    }
                                }
                                if let Some(ref counter) = change_counter {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            // ONE fsync confirms the whole batch (group commit).
                            if aof_barrier_needed {
                                if let Some(ref pool) = aof_pool {
                                    if pool.fsync_barrier(0).await.is_err() {
                                        aof_write_failed = true;
                                    }
                                }
                            }
                            if aof_write_failed {
                                // Discard buffered +OK responses — the writes are not
                                // durable. Log at warn level so operators can correlate
                                // with disk I/O errors.
                                responses.clear();
                                tracing::warn!(
                                    "AOF fsync failed for prior write batch; returning error \
                                     to client and closing connection"
                                );
                                let _ = framed.send(Frame::Error(Bytes::from_static(
                                    crate::persistence::aof::AOF_FSYNC_ERR,
                                ))).await;
                                break;
                            }
                            // Flush accumulated +OK responses now that AOF durability
                            // has been confirmed (or is fire-and-forget).
                            for resp in responses.drain(..) {
                                if !send_bounded!(framed, resp, write_timeout, client_id) {
                                    break_outer = true;
                                    break;
                                }
                            }
                            if break_outer {
                                break;
                            }
                            // Command-level ACL (H-3): -@pubsub must block
                            // SUBSCRIBE/PSUBSCRIBE at the command level, not
                            // just per-channel, before entering subscriber mode.
                            if let Some(err) = crate::server::conn::shared::pubsub_command_acl_deny(
                                &acl_table,
                                &conn.current_user,
                                cmd,
                                cmd_args,
                            ) {
                                let _ = framed.send(err).await;
                                continue;
                            }
                            // Handle subscribe
                            if cmd_args.is_empty() {
                                let cmd_lower = if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") { "subscribe" } else { "psubscribe" };
                                let _ = framed.send(Frame::Error(Bytes::from(format!(
                                    "ERR wrong number of arguments for '{}' command", cmd_lower
                                )))).await;
                            } else {
                                // Allocate subscriber resources if not yet done
                                if conn.pubsub_tx.is_none() {
                                    let (tx, rx) = channel::mpsc_bounded::<Bytes>(256);
                                    conn.subscriber_id = pubsub::next_subscriber_id();
                                    conn.pubsub_tx = Some(tx);
                                    conn.pubsub_rx = Some(rx);
                                }
                                let is_pattern = cmd.eq_ignore_ascii_case(b"PSUBSCRIBE");
                                for arg in cmd_args {
                                    if let Some(channel_or_pattern) = extract_bytes(arg) {
                                        // ACL channel permission check
                                        let deny = {
                                            #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                            let acl_guard = acl_table.read().unwrap();
                                            acl_guard.check_channel_permission(&conn.current_user, channel_or_pattern.as_ref()).map(|r| r.to_string())
                                        };
                                        if let Some(deny_reason) = deny {
                                            let _ = framed.send(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)))).await;
                                            continue;
                                        }
                                        #[allow(clippy::unwrap_used)] // conn.pubsub_tx is set to Some just above before this loop
                                        let sub = Subscriber::with_protocol(
                                            conn.pubsub_tx.clone().unwrap(),
                                            conn.subscriber_id,
                                            framed.codec().protocol_version() == 3,
                                        );
                                        {
                                            let mut registry = pubsub_registry.lock();
                                            if is_pattern {
                                                registry.psubscribe(channel_or_pattern.clone(), sub);
                                            } else {
                                                registry.subscribe(channel_or_pattern.clone(), sub);
                                            }
                                        }
                                        conn.subscription_count += 1;
                                        let response = if is_pattern {
                                            pubsub::psubscribe_response(&channel_or_pattern, conn.subscription_count)
                                        } else {
                                            pubsub::subscribe_response(&channel_or_pattern, conn.subscription_count)
                                        };
                                        if !send_bounded!(framed, response, write_timeout, client_id) {
                                            break_outer = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            // Remaining batch frames after SUBSCRIBE are dropped (subscriber mode takes over)
                            break;
                        }
                        // PUBLISH
                        // C2 fix: inside MULTI, PUBLISH must fall through to
                        // the queue instead of executing immediately.
                        if !conn.in_multi && cmd.eq_ignore_ascii_case(b"PUBLISH") {
                            // Command-level ACL (H-3): -@pubsub must block
                            // PUBLISH itself, not just specific channels.
                            if let Some(err) = crate::server::conn::shared::pubsub_command_acl_deny(
                                &acl_table,
                                &conn.current_user,
                                cmd,
                                cmd_args,
                            ) {
                                responses.push(err);
                                continue;
                            }
                            if cmd_args.len() != 2 {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR wrong number of arguments for 'publish' command"),
                                ));
                            } else {
                                let channel = extract_bytes(&cmd_args[0]);
                                let message = extract_bytes(&cmd_args[1]);
                                match (channel, message) {
                                    (Some(ch), Some(msg)) => {
                                        // Channel ACL (parity with sharded/monoio
                                        // handlers, which already gate PUBLISH).
                                        if let Some(err) = crate::server::conn::shared::publish_channel_acl_deny(
                                            &acl_table,
                                            &conn.current_user,
                                            &ch,
                                        ) {
                                            responses.push(err);
                                        } else {
                                            let count = pubsub_registry.lock().publish(&ch, &msg);
                                            responses.push(Frame::Integer(count));
                                        }
                                    }
                                    _ => responses.push(Frame::Error(
                                        Bytes::from_static(b"ERR invalid channel or message"),
                                    )),
                                }
                            }
                            continue;
                        }
                        // UNSUBSCRIBE / PUNSUBSCRIBE when not subscribed
                        if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
                            let ch = if !cmd_args.is_empty() {
                                extract_bytes(&cmd_args[0]).unwrap_or(Bytes::from_static(b""))
                            } else {
                                Bytes::from_static(b"")
                            };
                            responses.push(pubsub::unsubscribe_response(&ch, 0));
                            continue;
                        }
                        if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                            let pat = if !cmd_args.is_empty() {
                                extract_bytes(&cmd_args[0]).unwrap_or(Bytes::from_static(b""))
                            } else {
                                Bytes::from_static(b"")
                            };
                            responses.push(pubsub::punsubscribe_response(&pat, 0));
                            continue;
                        }
                        // MULTI
                        if cmd.eq_ignore_ascii_case(b"MULTI") {
                            if conn.in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR MULTI calls can not be nested"),
                                ));
                            } else {
                                conn.in_multi = true;
                                conn.command_queue.clear();
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            }
                            continue;
                        }
                        // EXEC
                        if cmd.eq_ignore_ascii_case(b"EXEC") {
                            if !conn.in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR EXEC without MULTI"),
                                ));
                            } else {
                                conn.in_multi = false;
                                // A transaction poisoned at queue time
                                // executes NOTHING (Redis CLIENT_DIRTY_EXEC).
                                if std::mem::take(&mut conn.multi_dirty) {
                                    conn.command_queue.clear();
                                    conn.watched_keys.clear();
                                    responses.push(
                                        crate::server::conn::shared::execabort_frame(),
                                    );
                                    continue;
                                }
                                let mut exec_publishes: Vec<(usize, Bytes, Bytes)> = Vec::new();
                                // PR #282 review: `execute_transaction` holds
                                // ONE guard on the db selected at EXEC time —
                                // every body write physically lands there,
                                // even when a queued SELECT mutates
                                // `conn.selected_db` mid-body. Capture the
                                // guard's db NOW; attributing entries to the
                                // post-EXEC `conn.selected_db` would mis-place
                                // them on recovery.
                                let txn_db = conn.selected_db;
                                let (mut result, txn_aof_entries) = execute_transaction(
                                    &db,
                                    &conn.command_queue,
                                    &conn.watched_keys,
                                    &mut conn.selected_db,
                                    &mut exec_publishes,
                                );
                                // C2: fan out PUBLISHes queued in the txn only
                                // now — after the transaction body — and patch
                                // their placeholders with the receiver count.
                                // Channel ACL gates this path too (C2 security):
                                // a denied channel is patched with NOPERM and
                                // never delivered, matching the immediate path.
                                for (inner, ch, msg) in exec_publishes.drain(..) {
                                    let patched = match crate::server::conn::shared::publish_channel_acl_deny(
                                        &acl_table,
                                        &conn.current_user,
                                        &ch,
                                    ) {
                                        Some(err) => err,
                                        None => Frame::Integer(pubsub_registry.lock().publish(&ch, &msg)),
                                    };
                                    if let Frame::Array(items) = &mut result {
                                        if inner < items.len() {
                                            items[inner] = patched;
                                        }
                                    }
                                }
                                // CLIENT TRACKING: writes applied inside the txn
                                // must invalidate tracked keys, same as the normal
                                // write path (EXEC previously bypassed this, so a
                                // SET/DEL/MSET inside MULTI left cached readers
                                // stale). Self-gated on tracking_active().
                                if crate::tracking::tracking_active() {
                                    if let Frame::Array(ref txn_results) = result {
                                        for (i, cmd_frame) in conn.command_queue.iter().enumerate() {
                                            if i >= txn_results.len()
                                                || matches!(txn_results[i], Frame::Error(_))
                                            {
                                                continue;
                                            }
                                            if let Some((c, a)) = extract_command(cmd_frame) {
                                                crate::tracking::invalidation::invalidate_after_write(
                                                    &tracking_table, c, a, client_id,
                                                );
                                            }
                                        }
                                    }
                                }
                                // Auto-index HSETs from the transaction
                                if let Some(ref vs) = vector_store {
                                    if let Frame::Array(ref txn_results) = result {
                                        let mut fallback_ts = crate::text::store::TextStore::new();
                                        for (i, cmd_frame) in conn.command_queue.iter().enumerate() {
                                            if let Some((c, a)) = extract_command(cmd_frame) {
                                                if c.eq_ignore_ascii_case(b"HSET")
                                                    && i < txn_results.len()
                                                    && !matches!(txn_results[i], Frame::Error(_))
                                                {
                                                    if let Some(Frame::BulkString(key_bytes)) = a.first() {
                                                        let mut store = vs.lock();
                                                        if let Some(ref ts) = text_store {
                                                            let mut ts_guard = ts.lock();
                                                            // Plan 166-01: handler_single has no
                                                            // TXN support; return value discarded.
                                                            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                                                                &mut store, &mut *ts_guard, key_bytes, a,
                                                                conn.selected_db as u8,
                                                            );
                                                        } else {
                                                            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                                                                &mut store, &mut fallback_ts, key_bytes, a,
                                                                conn.selected_db as u8,
                                                            );
                                                        }
                                                    }
                                                } else if (c.eq_ignore_ascii_case(b"DEL")
                                                    || c.eq_ignore_ascii_case(b"UNLINK"))
                                                    && i < txn_results.len()
                                                    && !matches!(txn_results[i], Frame::Error(_))
                                                {
                                                    // Auto-delete vectors (parity with
                                                    // the HSET auto-index arm above).
                                                    crate::shard::spsc_handler::auto_delete_vectors(
                                                        &mut vs.lock(),
                                                        a,
                                                        conn.selected_db as u8,
                                                    );
                                                } else if c.eq_ignore_ascii_case(b"HDEL")
                                                    && i < txn_results.len()
                                                    && !matches!(txn_results[i], Frame::Error(_))
                                                {
                                                    // R4 parity inside MULTI/EXEC.
                                                    crate::shard::spsc_handler::auto_hdel_vectors(
                                                        &mut vs.lock(),
                                                        a,
                                                        conn.selected_db as u8,
                                                    );
                                                } else if (c.eq_ignore_ascii_case(b"FLUSHDB")
                                                    || c.eq_ignore_ascii_case(b"FLUSHALL"))
                                                    && i < txn_results.len()
                                                    && !matches!(txn_results[i], Frame::Error(_))
                                                {
                                                    // R3 parity inside MULTI/EXEC (text
                                                    // store cleared via its own guard or
                                                    // the throwaway fallback store).
                                                    // WS5a: FLUSHDB scopes to
                                                    // `conn.selected_db`; FLUSHALL clears
                                                    // every db.
                                                    if c.eq_ignore_ascii_case(b"FLUSHDB") {
                                                        vs.lock().clear_all_contents_for_db(
                                                            conn.selected_db as u8,
                                                        );
                                                        if let Some(ref ts) = text_store {
                                                            ts.lock().clear_all_contents_for_db(
                                                                conn.selected_db as u8,
                                                            );
                                                        }
                                                    } else {
                                                        vs.lock().clear_all_contents();
                                                        if let Some(ref ts) = text_store {
                                                            ts.lock().clear_all_contents();
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                conn.command_queue.clear();
                                conn.watched_keys.clear();
                                // The EXEC response occupies responses[exec_resp_idx].
                                // All txn AOF entries map to this same slot so that
                                // the Always-policy flush can patch the EXEC frame if
                                // any command's fsync fails.
                                let exec_resp_idx = responses.len();
                                responses.push(result);
                                // task #35 + PR #282 review: the whole body
                                // ran against the guard taken on `txn_db`
                                // (captured before EXEC) — NOT the possibly
                                // SELECT-mutated post-EXEC `conn.selected_db`.
                                // A queued SELECT itself is never persisted.
                                aof_entries.extend(
                                    txn_aof_entries
                                        .into_iter()
                                        .map(|b| (exec_resp_idx, txn_db, b)),
                                );
                            }
                            continue;
                        }
                        // DISCARD
                        if cmd.eq_ignore_ascii_case(b"DISCARD") {
                            if !conn.in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR DISCARD without MULTI"),
                                ));
                            } else {
                                conn.in_multi = false;
                                conn.command_queue.clear();
                                // DISCARD clears the poison too, or the NEXT
                                // transaction aborts for a fault not its own.
                                conn.multi_dirty = false;
                                conn.watched_keys.clear();
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            }
                            continue;
                        }
                        // WATCH
                        if cmd.eq_ignore_ascii_case(b"WATCH") {
                            if conn.in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR WATCH inside MULTI is not allowed"),
                                ));
                            } else if cmd_args.is_empty() {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR wrong number of arguments for 'watch' command"),
                                ));
                            } else {
                                let guard = db[conn.selected_db].read();
                                for arg in cmd_args {
                                    if let Frame::BulkString(key) = arg {
                                        let version = guard.get_version(key);
                                        conn.watched_keys.insert(
                                            key.clone(),
                                            crate::server::conn::shared::WatchToken {
                                                version,
                                            },
                                        );
                                    }
                                }
                                // guard dropped here
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            }
                            continue;
                        }
                        // UNWATCH
                        if cmd.eq_ignore_ascii_case(b"UNWATCH") {
                            conn.watched_keys.clear();
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            continue;
                        }

                        // === ACL permission check (NOPERM gate) ===
                        // Exempt commands (AUTH, HELLO, QUIT, ACL) already handled via continue above.
                        // Fast path: skip RwLock + HashMap for unrestricted users
                        // whose cache is still fresh.  Stale caches (after ACL
                        // SETUSER / DELUSER / LOAD) fall through to the full check.
                        if !conn.acl_skip_allowed() {
                            #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                            if let Some(deny_reason) = acl_table.read().unwrap().check_command_permission(
                                &conn.current_user, cmd, cmd_args,
                            ) {
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
                                responses.push(Frame::Error(Bytes::from(format!(
                                    "NOPERM {}", deny_reason
                                ))));
                                continue;
                            }

                            // === ACL key pattern check ===
                            let is_write = metadata::is_write(cmd);
                            #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                            if let Some(deny_reason) = acl_table.read().unwrap().check_key_permission(
                                &conn.current_user, cmd, cmd_args, is_write,
                            ) {
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
                                responses.push(Frame::Error(Bytes::from(format!(
                                    "NOPERM {}", deny_reason
                                ))));
                                continue;
                            }
                        }

                        // === RESET ===
                        // Shares `try_handle_reset` with the other two handlers
                        // rather than re-deriving "default state" here, so the
                        // three paths cannot drift on what RESET restores.
                        if crate::server::conn::shared::try_handle_reset(
                            cmd,
                            cmd_args,
                            client_id,
                            &mut conn,
                            &requirepass,
                            &tracking_table,
                            &*pubsub_registry,
                            &mut responses,
                            Some(framed.codec_mut()),
                        ) {
                            continue;
                        }

                        // === CLIENT PAUSE check ===
                        let pause_wait_ms = {
                            let rt = runtime_config.read();
                            let deadline = rt.client_pause_deadline_ms;
                            if deadline > 0 {
                                let now = crate::storage::entry::current_time_ms();
                                if now < deadline {
                                    let should_pause = if rt.client_pause_write_only {
                                        metadata::is_write(cmd)
                                    } else {
                                        true
                                    };
                                    if should_pause { deadline.saturating_sub(now) } else { 0 }
                                } else { 0 }
                            } else { 0 }
                        };
                        if pause_wait_ms > 0 {
                            let mut remaining = pause_wait_ms;
                            while remaining > 0 {
                                let chunk = remaining.min(50);
                                tokio::time::sleep(std::time::Duration::from_millis(chunk)).await;
                                remaining = remaining.saturating_sub(chunk);
                                let still_paused = {
                                    let rt = runtime_config.read();
                                    rt.client_pause_deadline_ms > 0
                                        && crate::storage::entry::current_time_ms() < rt.client_pause_deadline_ms
                                };
                                if !still_paused {
                                    break;
                                }
                            }
                        }
                    }

                    // --- MULTI queue mode ---
                    if conn.in_multi {
                        // Reject FT.* commands inside MULTI — vector hooks are not
                        // wired through the transaction execution path yet.
                        if let Some((cmd, _)) = extract_command(&frame) {
                            if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
                                responses.push(Frame::Error(Bytes::from_static(
                                    b"ERR FT.* commands are not supported inside MULTI/EXEC",
                                )));
                                continue;
                            }
                        }
                        // Queue-time validation (Redis CLIENT_DIRTY_EXEC): a
                        // command that could never run poisons the whole
                        // transaction HERE, so EXEC refuses everything rather
                        // than applying the half that happened to be valid.
                        if let Some((cmd, cmd_args)) = extract_command(&frame)
                            && let Some(err) =
                                crate::server::conn::shared::queue_time_rejection(cmd, cmd_args)
                        {
                            conn.multi_dirty = true;
                            responses.push(err);
                            continue;
                        }
                        conn.command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                        continue;
                    }

                    // --- Collect for phase 2 dispatch (needs db lock) ---
                    match extract_command(&frame) {
                        Some((cmd, cmd_args)) => {
                            // FT.* vector commands: dispatch immediately (no db lock needed)
                            if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
                                if let Some(ref vs) = vector_store {
                                    let mut store = vs.lock();
                                    let mut fallback_ts = crate::text::store::TextStore::new();
                                    let mut ts_guard = text_store.as_ref().map(|ts| ts.lock());
                                    let ts_mut = match ts_guard {
                                        Some(ref mut guard) => &mut **guard,
                                        None => &mut fallback_ts,
                                    };
                                    // ── 151-03 non-sharded text FT.SEARCH fast path ──
                                    // Bare text queries bypass ft_search() (KNN/SPARSE/HYBRID
                                    // only) and route to execute_text_search_local. Mirrors the
                                    // monoio/sharded single-shard fast paths. HYBRID falls
                                    // through to the existing ft_search() chain below.
                                    // Note: we reuse the already-acquired ts_mut borrow instead
                                    // of re-locking (parking_lot Mutex is not re-entrant).
                                    #[cfg(feature = "text-index")]
                                    if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                        if let Some(crate::protocol::Frame::BulkString(query_bytes)) = cmd_args.get(1) {
                                            match crate::command::vector_search::parse_hybrid_modifier(cmd_args) {
                                                Ok(Some(_)) => {
                                                    // HYBRID present — defer to existing ft_search() chain below.
                                                }
                                                Err(frame_err) => {
                                                    responses.push(frame_err);
                                                    continue;
                                                }
                                                Ok(None) => {
                                                    if crate::command::vector_search::is_text_query(
                                                        query_bytes.as_ref(),
                                                    ) && !crate::command::vector_search::has_sparse_clause(
                                                        cmd_args,
                                                    ) {
                                                        // Step 1: index_name.
                                                        let index_name = match cmd_args.first() {
                                                            Some(crate::protocol::Frame::BulkString(b)) => b.clone(),
                                                            _ => {
                                                                responses.push(crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                                                    b"ERR wrong number of arguments for FT.SEARCH",
                                                                )));
                                                                continue;
                                                            }
                                                        };
                                                        // fts-query-eval-dispatch 2b: single-shard (non-sharded) fast path.
                                                        // run_text_query handles index lookup, parse, eval, tag/numeric
                                                        // filters, OR, multi-@clause combinators in one call.
                                                        // ts_mut is already borrowed (parking_lot Mutex, non-reentrant);
                                                        // ts_mut points to the empty fallback when text_store is None —
                                                        // get_index() returns None → "ERR no such index" (correct).
                                                        let (offset, count) =
                                                            crate::command::vector_search::parse_limit_clause(cmd_args);
                                                        let top_k = if count == usize::MAX {
                                                            10000
                                                        } else {
                                                            offset.saturating_add(count)
                                                        }
                                                        .max(1);
                                                        let highlight_opts =
                                                            crate::command::vector_search::parse_highlight_clause(cmd_args);
                                                        let summarize_opts =
                                                            crate::command::vector_search::parse_summarize_clause(cmd_args);
                                                        #[cfg(feature = "text-index")]
                                                        {
                                                            let db_index = conn.selected_db as u8;
                                                            let mut response = crate::command::vector_search::run_text_query(
                                                                &*ts_mut,
                                                                &index_name,
                                                                query_bytes.as_ref(),
                                                                top_k,
                                                                offset,
                                                                count,
                                                                db_index,
                                                            );
                                                            if highlight_opts.is_some() || summarize_opts.is_some() {
                                                                if let Some(text_index) = ts_mut.get_index_for_db(&index_name, db_index) {
                                                                    if let Ok(node) = crate::text::query::parse_query(
                                                                        query_bytes.as_ref(),
                                                                        &crate::text::query::QuerySchema::from_index(text_index),
                                                                    ) {
                                                                        let terms = crate::text::query::collect_highlight_terms(&node, text_index);
                                                                        let db_guard = db[conn.selected_db].read();
                                                                        crate::command::vector_search::apply_post_processing(
                                                                            &mut response,
                                                                            &terms,
                                                                            text_index,
                                                                            &db_guard,
                                                                            highlight_opts.as_ref(),
                                                                            summarize_opts.as_ref(),
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                            responses.push(response);
                                                            continue;
                                                        }
                                                        #[cfg(not(feature = "text-index"))]
                                                        {
                                                            responses.push(crate::protocol::Frame::Error(
                                                                bytes::Bytes::from_static(b"ERR text-index feature not enabled"),
                                                            ));
                                                            continue;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    let response = if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                                        crate::command::vector_search::ft_create(&mut *store, ts_mut, cmd_args, conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                        // TEMP-04: single-shard handler has no TemporalRegistry and no cross-store TXN.
                                        // Unified helper with shard_databases=None returns ERR on AS_OF (correct per
                                        // Plan 165-01 contract); non-AS_OF continues to return latest (as_of_lsn=0).
                                        match resolve_ft_search_as_of_lsn(cmd_args, None, None) {
                                            Err(err_frame) => err_frame,
                                            Ok(as_of_lsn) => {
                                                let has_session = cmd_args.iter().any(|a| {
                                                    if let crate::protocol::Frame::BulkString(b) = a { b.eq_ignore_ascii_case(b"SESSION") } else { false }
                                                });
                                                if has_session {
                                                    let mut db_guard = db[conn.selected_db].write();
                                                    crate::command::vector_search::ft_search(&mut *store, cmd_args, Some(&mut *db_guard), Some(&*ts_mut), as_of_lsn, conn.selected_db as u8)
                                                } else {
                                                    crate::command::vector_search::ft_search(&mut *store, cmd_args, None, Some(&*ts_mut), as_of_lsn, conn.selected_db as u8)
                                                }
                                            }
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                                        let mut db_guard = db[conn.selected_db].write();
                                        crate::command::vector_search::ft_dropindex(&mut *store, ts_mut, Some(&mut *db_guard), cmd_args, conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                                        crate::command::vector_search::ft_info(&*store, ts_mut, cmd_args, conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                                        crate::command::vector_search::ft_list(&*store, conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                                        crate::command::vector_search::ft_compact(&mut *store, ts_mut, cmd_args, conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                                        crate::command::vector_search::cache_search::ft_cachesearch(&mut *store, cmd_args, conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                                        crate::command::vector_search::ft_config(&mut *store, ts_mut, cmd_args, conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                        let mut db_guard = db[conn.selected_db].write();
                                        crate::command::vector_search::recommend::ft_recommend(&mut *store, cmd_args, Some(&mut *db_guard), conn.selected_db as u8)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                        #[cfg(feature = "graph")]
                                        {
                                            let gs_frame = if let Some(ref gs) = graph_store {
                                                let graph_guard = gs.lock();
                                                crate::command::vector_search::navigate::ft_navigate(&mut *store, Some(&graph_guard), cmd_args, None, conn.selected_db as u8)
                                            } else {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.NAVIGATE requires graph store"))
                                            };
                                            gs_frame
                                        }
                                        #[cfg(not(feature = "graph"))]
                                        {
                                            Frame::Error(bytes::Bytes::from_static(b"ERR FT.NAVIGATE requires graph feature"))
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"FT.EXPAND") {
                                        #[cfg(feature = "graph")]
                                        {
                                            let gs_frame = if let Some(ref gs) = graph_store {
                                                let graph_guard = gs.lock();
                                                crate::command::vector_search::ft_expand(&graph_guard, cmd_args)
                                            } else {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.EXPAND requires graph store"))
                                            };
                                            gs_frame
                                        }
                                        #[cfg(not(feature = "graph"))]
                                        {
                                            Frame::Error(bytes::Bytes::from_static(b"ERR FT.EXPAND requires graph feature"))
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
                                        // ── 154-01: FT.AGGREGATE non-sharded single-shard local exec ──
                                        // handler_single has no SPSC dispatch — call execute_local_full
                                        // directly, reusing the already-held `store` (vs) and `ts_mut`
                                        // borrows. Acquire a fresh `db` read-guard in the match arm;
                                        // it drops at arm-scope end, before the response is returned
                                        // from the surrounding else-if chain. No `.await` in scope.
                                        #[cfg(feature = "text-index")]
                                        {
                                            match crate::command::vector_search::ft_aggregate::parse_aggregate_args(cmd_args) {
                                                Ok(parsed) => {
                                                    let db_guard = db[conn.selected_db].read();
                                                    crate::command::vector_search::ft_aggregate::execute_local_full(
                                                        &mut *store,
                                                        &*ts_mut,
                                                        &parsed.index_name,
                                                        &parsed.query,
                                                        &parsed.pipeline,
                                                        &*db_guard,
                                                        conn.selected_db as u8,
                                                    )
                                                }
                                                Err(err_frame) => err_frame,
                                            }
                                        }
                                        #[cfg(not(feature = "text-index"))]
                                        {
                                            Frame::Error(bytes::Bytes::from_static(b"ERR FT.AGGREGATE requires text-index feature"))
                                        }
                                    } else {
                                        Frame::Error(bytes::Bytes::from_static(b"ERR unknown FT.* command"))
                                    };
                                    responses.push(response);
                                    continue; // skip dispatchable
                                } else {
                                    responses.push(Frame::Error(bytes::Bytes::from_static(b"ERR vector search not initialized")));
                                    continue;
                                }
                            }

                            // GRAPH.* graph commands: dispatch to GraphStore directly
                            #[cfg(feature = "graph")]
                            if cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
                                if let Some(ref gs) = graph_store {
                                    let (response, wal_records) = {
                                        let mut store = gs.lock();
                                        let resp = crate::command::graph::dispatch_graph_cmd_args(&mut store, cmd, cmd_args);
                                        let records = store.drain_wal();
                                        (resp, records)
                                    };
                                    let mut graph_aof_failed = false;
                                    let mut graph_barrier_needed = false;
                                    for record in wal_records {
                                        if let Some(ref pool) = aof_pool {
                                            // Single-shard mode (shard_id = 0).
                                            // Group commit: enqueue all records,
                                            // ONE fsync barrier below confirms them
                                            // (Always); fire-and-forget for
                                            // everysec/no.
                                            let bytes = bytes::Bytes::from(record);
                                            let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(&repl_state, 0, bytes.len());
                                            match pool
                                                .send_append_group(0, lsn, conn.selected_db, bytes)
                                                .await
                                            {
                                                Ok(true) => graph_barrier_needed = true,
                                                Ok(false) => {}
                                                Err(_) => graph_aof_failed = true,
                                            }
                                        }
                                        if let Some(ref counter) = change_counter {
                                            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                    }
                                    if graph_barrier_needed {
                                        if let Some(ref pool) = aof_pool {
                                            if pool.fsync_barrier(0).await.is_err() {
                                                graph_aof_failed = true;
                                            }
                                        }
                                    }
                                    if graph_aof_failed {
                                        responses.push(Frame::Error(bytes::Bytes::from_static(
                                            crate::persistence::aof::AOF_FSYNC_ERR,
                                        )));
                                    } else {
                                        responses.push(response);
                                    }
                                    continue;
                                } else {
                                    responses.push(Frame::Error(bytes::Bytes::from_static(b"ERR graph engine not initialized")));
                                    continue;
                                }
                            }

                            let is_write = metadata::is_write(cmd);

                            // Serialize for AOF before dispatch.
                            // `is_persisted_write`: never AOF a literal client
                            // SELECT (task #35 — poisons the stream db context).
                            let aof_bytes = if metadata::is_persisted_write(cmd) && aof_pool.is_some() {
                                let mut buf = BytesMut::new();
                                crate::protocol::serialize::serialize(&frame, &mut buf);
                                Some(buf.freeze())
                            } else {
                                None
                            };

                            // Reserve a slot in responses for phase 2 to fill
                            let resp_idx = responses.len();
                            responses.push(Frame::Null); // placeholder
                            dispatchable.push((resp_idx, frame, is_write, aof_bytes));
                        }
                        None => {
                            responses.push(Frame::Error(Bytes::from_static(
                                b"ERR invalid command format",
                            )));
                        }
                    }
                }

                // === Phase 2: Execute dispatchable frames with read/write lock batching ===
                // Group consecutive reads under ONE shared read lock, consecutive writes
                // under ONE exclusive write lock. Minimizes lock transitions while
                // enabling read parallelism across connections.
                if !dispatchable.is_empty() && !should_quit {
                    // Arena-backed scratch: collect write-command response indices for
                    // post-dispatch AOF batching. Rebuilt each batch cycle and
                    // bulk-deallocated by arena.reset() after the batch completes.
                    let mut write_indices: BumpVec<usize> = BumpVec::new_in(&arena);
                    for item in &dispatchable {
                        if item.2 { // is_write
                            write_indices.push(item.0); // resp_idx
                        }
                    }
                    // write_indices consumed here; drop before any await
                    drop(write_indices);

                    let db_count = db.len();
                    let mut i = 0;
                    while i < dispatchable.len() {
                        // Determine if this run starts with a read or write
                        let run_is_read = !dispatchable[i].2; // .2 is is_write

                        // Find end of consecutive same-type commands
                        let run_start = i;
                        while i < dispatchable.len() && (!dispatchable[i].2) == run_is_read {
                            i += 1;
                        }

                        if run_is_read {
                            // === Read run: shared read lock ===
                            // Re-acquire guard if conn.selected_db changes mid-run.
                            let mut current_db = conn.selected_db;
                            let mut guard = db[current_db].read();
                            let now_ms = crate::storage::entry::current_time_ms();
                            let proto = framed.codec().protocol_version();
                            for j in run_start..i {
                                if conn.selected_db != current_db {
                                    drop(guard);
                                    current_db = conn.selected_db;
                                    guard = db[current_db].read();
                                }
                                let (resp_idx, ref disp_frame, _, _) = dispatchable[j];
                                #[allow(clippy::unwrap_used)] // Frame was parsed earlier; extract_command succeeds on valid frames
                                let (d_cmd, d_args) = extract_command(disp_frame).unwrap();

                                // FT.* read commands (FT.SEARCH, FT.INFO)
                                if d_cmd.len() > 3 && d_cmd[..3].eq_ignore_ascii_case(b"FT.") {
                                    if let Some(ref vs) = vector_store {
                                        let mut store = vs.lock();
                                        let mut fb_ts = crate::text::store::TextStore::new();
                                        let mut ts_g2 = text_store.as_ref().map(|ts| ts.lock());
                                        let ts_m2 = match ts_g2 { Some(ref mut g) => &mut **g, None => &mut fb_ts };
                                        let response = if d_cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                            // TEMP-04: single-shard handler has no TemporalRegistry and no cross-store TXN.
                                            // Unified helper with shard_databases=None returns ERR on AS_OF (correct per
                                            // Plan 165-01 contract); non-AS_OF continues to return latest (as_of_lsn=0).
                                            match resolve_ft_search_as_of_lsn(d_args, None, None) {
                                                Err(err_frame) => err_frame,
                                                Ok(as_of_lsn) => {
                                                    let has_session = d_args.iter().any(|a| {
                                                        if let crate::protocol::Frame::BulkString(b) = a { b.eq_ignore_ascii_case(b"SESSION") } else { false }
                                                    });
                                                    if has_session {
                                                        drop(guard);
                                                        let mut db_guard = db[conn.selected_db].write();
                                                        let r = crate::command::vector_search::ft_search(&mut *store, d_args, Some(&mut *db_guard), Some(&*ts_m2), as_of_lsn, conn.selected_db as u8);
                                                        drop(db_guard);
                                                        guard = db[conn.selected_db].read();
                                                        r
                                                    } else {
                                                        crate::command::vector_search::ft_search(&mut *store, d_args, None, Some(&*ts_m2), as_of_lsn, conn.selected_db as u8)
                                                    }
                                                }
                                            }
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.INFO") {
                                            crate::command::vector_search::ft_info(&*store, ts_m2, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT._LIST") {
                                            crate::command::vector_search::ft_list(&*store, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                                            crate::command::vector_search::ft_compact(&mut *store, ts_m2, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                                            crate::command::vector_search::cache_search::ft_cachesearch(&mut *store, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                                            crate::command::vector_search::ft_config(&mut *store, ts_m2, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                            drop(guard);
                                            let mut db_guard = db[conn.selected_db].write();
                                            let r = crate::command::vector_search::recommend::ft_recommend(&mut *store, d_args, Some(&mut *db_guard), conn.selected_db as u8);
                                            drop(db_guard);
                                            guard = db[conn.selected_db].read();
                                            r
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                            #[cfg(feature = "graph")]
                                            {
                                                if let Some(ref gs) = graph_store {
                                                    let graph_guard = gs.lock();
                                                    crate::command::vector_search::navigate::ft_navigate(&mut *store, Some(&graph_guard), d_args, None, conn.selected_db as u8)
                                                } else {
                                                    Frame::Error(bytes::Bytes::from_static(b"ERR FT.NAVIGATE requires graph store"))
                                                }
                                            }
                                            #[cfg(not(feature = "graph"))]
                                            {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.NAVIGATE requires graph feature"))
                                            }
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.EXPAND") {
                                            #[cfg(feature = "graph")]
                                            {
                                                if let Some(ref gs) = graph_store {
                                                    let graph_guard = gs.lock();
                                                    crate::command::vector_search::ft_expand(&graph_guard, d_args)
                                                } else {
                                                    Frame::Error(bytes::Bytes::from_static(b"ERR FT.EXPAND requires graph store"))
                                                }
                                            }
                                            #[cfg(not(feature = "graph"))]
                                            {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.EXPAND requires graph feature"))
                                            }
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
                                            // ── 154-01: FT.AGGREGATE read-run local exec ──
                                            // Outer `guard = db[conn.selected_db].read()` is already
                                            // held (acquired at the read-run entry); reuse it via
                                            // `&*guard`. `ts_m2` is the read-run TextStore borrow.
                                            // Synchronous — no `.await` inside the arm.
                                            #[cfg(feature = "text-index")]
                                            {
                                                match crate::command::vector_search::ft_aggregate::parse_aggregate_args(d_args) {
                                                    Ok(parsed) => crate::command::vector_search::ft_aggregate::execute_local_full(
                                                        &mut *store,
                                                        &*ts_m2,
                                                        &parsed.index_name,
                                                        &parsed.query,
                                                        &parsed.pipeline,
                                                        &*guard,
                                                        conn.selected_db as u8,
                                                    ),
                                                    Err(err_frame) => err_frame,
                                                }
                                            }
                                            #[cfg(not(feature = "text-index"))]
                                            {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.AGGREGATE requires text-index feature"))
                                            }
                                        } else {
                                            Frame::Error(bytes::Bytes::from_static(b"ERR unknown FT.* command"))
                                        };
                                        responses[resp_idx] = response;
                                        continue;
                                    }
                                }

                                // MA2: KILL SNAPSHOT is admin (not is_write) so it lands in
                                // the read run. Intercept before dispatch_read which doesn't
                                // know about VectorStore.
                                if d_cmd.eq_ignore_ascii_case(b"KILL") {
                                    if let Some(ref vs) = vector_store {
                                        let mut vs_guard = vs.lock();
                                        let response = crate::command::server_admin::kill_snapshot(
                                            &mut vs_guard,
                                            d_args,
                                        );
                                        drop(vs_guard);
                                        responses[resp_idx] = response;
                                    } else {
                                        responses[resp_idx] = Frame::Error(
                                            bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                        );
                                    }
                                    continue;
                                }

                                // P8: VACUUM — admin, lands in read run.
                                // manifest/WAL not available here; MVCC passes still run.
                                //
                                // B1 fix: route VECTOR/GRAPH to dedicated entry points;
                                // parent `vacuum()` still stubs them.
                                if d_cmd.eq_ignore_ascii_case(b"VACUUM") {
                                    let mut routed_subcommand = false;
                                    if let Some(sub_frame) = d_args.first() {
                                        if let Some(sub) =
                                            crate::command::helpers::extract_bytes(sub_frame)
                                        {
                                            if sub.eq_ignore_ascii_case(b"VECTOR") {
                                                if let Some(ref vs) = vector_store {
                                                    let mut vs_guard = vs.lock();
                                                    let response =
                                                        crate::command::server_admin::vacuum_vector(
                                                            &mut vs_guard,
                                                            &d_args[1..],
                                                        conn.selected_db as u8,
                                        );
                                                    drop(vs_guard);
                                                    responses[resp_idx] = response;
                                                } else {
                                                    responses[resp_idx] = Frame::Error(
                                                        bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                                    );
                                                }
                                                routed_subcommand = true;
                                            }
                                            #[cfg(feature = "graph")]
                                            if sub.eq_ignore_ascii_case(b"GRAPH") {
                                                if let Some(ref gs) = graph_store {
                                                    let mut gs_guard = gs.lock();
                                                    let response =
                                                        crate::command::server_admin::vacuum_graph(
                                                            &mut gs_guard,
                                                            &d_args[1..],
                                                            config.graph_merge_max_segments,
                                                            config.graph_dead_edge_trigger,
                                                        );
                                                    drop(gs_guard);
                                                    responses[resp_idx] = response;
                                                } else {
                                                    responses[resp_idx] = Frame::Error(
                                                        bytes::Bytes::from_static(b"ERR graph store not initialized"),
                                                    );
                                                }
                                                routed_subcommand = true;
                                            }
                                        }
                                    }
                                    if !routed_subcommand {
                                        if let Some(ref vs) = vector_store {
                                            let mut vs_guard = vs.lock();
                                            let response = crate::command::server_admin::vacuum(
                                                &mut vs_guard,
                                                None, // manifest — not available here
                                                None, // wal_v3 — not available here
                                                d_args,
                                                crate::command::server_admin::DEFAULT_VACUUM_PRUNE_MARGIN, // see server_admin.rs
                                                None, // disk_offload_dir — dead: wal is None on this path too
                                                0,    // shard_id — dead, see above
                                            );
                                            drop(vs_guard);
                                            responses[resp_idx] = response;
                                        } else {
                                            responses[resp_idx] = Frame::Error(
                                                bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                            );
                                        }
                                    }
                                    continue;
                                }

                                // P8: DEBUG RECLAMATION — admin, lands in read run.
                                if d_cmd.eq_ignore_ascii_case(b"DEBUG") {
                                    if let Some(sub) = d_args.first() {
                                        if let Some(s) = crate::command::helpers::extract_bytes(sub) {
                                            if s.eq_ignore_ascii_case(b"RECLAMATION") {
                                                if let Some(ref vs) = vector_store {
                                                    let vs_guard = vs.lock();
                                                    let response = crate::command::server_admin::debug_reclamation(
                                                        &vs_guard, None, None,
                                                    );
                                                    drop(vs_guard);
                                                    responses[resp_idx] = response;
                                                } else {
                                                    responses[resp_idx] = Frame::Error(
                                                        bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                                    );
                                                }
                                                continue;
                                            }
                                        }
                                    }
                                    // Other DEBUG subcommands fall through to dispatch_read.
                                }

                                let dispatch_start = std::time::Instant::now();
                                let result = dispatch_read(&*guard, d_cmd, d_args, now_ms, &mut conn.selected_db, db_count);
                                let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                                if let Ok(cmd_str) = std::str::from_utf8(d_cmd) {
                                    crate::admin::metrics_setup::record_command_cached(
                                        cmd_str,
                                        elapsed_us,
                                        &mut conn.cached_metrics,
                                    );
                                }
                                if let Frame::Array(ref args) = *disp_frame {
                                    crate::admin::metrics_setup::global_slowlog().maybe_record(
                                        elapsed_us,
                                        args.as_slice(),
                                        peer_addr.as_bytes(),
                                        conn.client_name.as_ref().map_or(b"" as &[u8], |n| n.as_ref()),
                                    );
                                }
                                let (response, quit) = match result {
                                    DispatchResult::Response(f) => (f, false),
                                    DispatchResult::Quit(f) => (f, true),
                                };
                                // Track EVERY key of a successful read for
                                // client-side caching invalidation.
                                if conn.tracking_state.enabled
                                    && !conn.tracking_state.bcast
                                    && !matches!(response, Frame::Error(_))
                                {
                                    crate::tracking::invalidation::track_read_keys(
                                        &tracking_table,
                                        d_cmd,
                                        d_args,
                                        client_id,
                                        conn.tracking_state.noloop,
                                    );
                                }
                                // Apply RESP3 response conversion if needed
                                let response = apply_resp3_conversion(d_cmd, d_args, response, proto);
                                responses[resp_idx] = response;
                                if quit {
                                    should_quit = true;
                                    break;
                                }
                            }
                            // read guard dropped here
                        } else {
                            // === Write run: exclusive write lock ===
                            // Re-acquire guard if conn.selected_db changes mid-run (e.g. SELECT).
                            let mut current_db = conn.selected_db;
                            let mut guard = db[current_db].write();
                            guard.refresh_now();
                            for j in run_start..i {
                                // Re-acquire guard if a previous SELECT changed the db
                                if conn.selected_db != current_db {
                                    drop(guard);
                                    current_db = conn.selected_db;
                                    guard = db[current_db].write();
                                    guard.refresh_now();
                                }
                                let (resp_idx, ref disp_frame, _, ref aof_bytes) = dispatchable[j];
                                #[allow(clippy::unwrap_used)] // Frame was parsed earlier; extract_command succeeds on valid frames
                                let (d_cmd, d_args) = extract_command(disp_frame).unwrap();
                                let rt = runtime_config.read();
                                // WS6 fix (HIGH, adversarial review 2026-07-08): a
                                // command that can only shrink memory (HDEL, SREM,
                                // LPOP, ...) must never be REJECTED by either gate
                                // below, or a key/db that crosses its noeviction
                                // boundary has no self-recovery path. Eviction is
                                // still attempted; only the reject is bypassed.
                                // See `db_quota::is_shrink_only_command`.
                                let shrink_only =
                                    crate::storage::db_quota::is_shrink_only_command(d_cmd);
                                let evict_result = evict_to_budget(&mut *guard, &rt, EvictionRun::plain());
                                if !shrink_only {
                                    if let Err(oom_frame) = evict_result {
                                        responses[resp_idx] = oom_frame;
                                        continue;
                                    }
                                }
                                // WS5b: per-db quota, additive to the whole-instance
                                // gate above. `_for_command` exempts SELECT/SWAPDB so
                                // a quota'd db doesn't trap the connection.
                                let db_quota_result = crate::storage::db_quota::check_db_maxmemory_for_command(&mut *guard, current_db, &rt, d_cmd);
                                if !shrink_only {
                                    if let Err(oom_frame) = db_quota_result {
                                        responses[resp_idx] = oom_frame;
                                        continue;
                                    }
                                }
                                drop(rt);

                                // FT.* vector commands: dispatch to VectorStore directly
                                if d_cmd.len() > 3 && d_cmd[..3].eq_ignore_ascii_case(b"FT.") {
                                    if let Some(ref vs) = vector_store {
                                        let mut store = vs.lock();
                                        let mut fb_ts3 = crate::text::store::TextStore::new();
                                        let mut ts_g3 = text_store.as_ref().map(|ts| ts.lock());
                                        let ts_m3 = match ts_g3 { Some(ref mut g) => &mut **g, None => &mut fb_ts3 };
                                        let response = if d_cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                                            crate::command::vector_search::ft_create(&mut *store, ts_m3, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                            // Write run: guard is already write-locked.
                                            // TEMP-04: single-shard handler has no registry and no TXN; helper returns
                                            // ERR on AS_OF and Ok(0) otherwise (Plan 165-01 contract).
                                            match resolve_ft_search_as_of_lsn(d_args, None, None) {
                                                Err(err_frame) => err_frame,
                                                Ok(as_of_lsn) => crate::command::vector_search::ft_search(&mut *store, d_args, Some(&mut *guard), Some(&*ts_m3), as_of_lsn, conn.selected_db as u8),
                                            }
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                                            crate::command::vector_search::ft_dropindex(&mut *store, ts_m3, Some(&mut *guard), d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.INFO") {
                                            crate::command::vector_search::ft_info(&*store, ts_m3, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT._LIST") {
                                            crate::command::vector_search::ft_list(&*store, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                                            crate::command::vector_search::ft_compact(&mut *store, ts_m3, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                                            crate::command::vector_search::cache_search::ft_cachesearch(&mut *store, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                                            crate::command::vector_search::ft_config(&mut *store, ts_m3, d_args, conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                            crate::command::vector_search::recommend::ft_recommend(&mut *store, d_args, Some(&mut *guard), conn.selected_db as u8)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                            #[cfg(feature = "graph")]
                                            {
                                                if let Some(ref gs) = graph_store {
                                                    let graph_guard = gs.lock();
                                                    crate::command::vector_search::navigate::ft_navigate(&mut *store, Some(&graph_guard), d_args, None, conn.selected_db as u8)
                                                } else {
                                                    Frame::Error(bytes::Bytes::from_static(b"ERR FT.NAVIGATE requires graph store"))
                                                }
                                            }
                                            #[cfg(not(feature = "graph"))]
                                            {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.NAVIGATE requires graph feature"))
                                            }
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.EXPAND") {
                                            #[cfg(feature = "graph")]
                                            {
                                                if let Some(ref gs) = graph_store {
                                                    let graph_guard = gs.lock();
                                                    crate::command::vector_search::ft_expand(&graph_guard, d_args)
                                                } else {
                                                    Frame::Error(bytes::Bytes::from_static(b"ERR FT.EXPAND requires graph store"))
                                                }
                                            }
                                            #[cfg(not(feature = "graph"))]
                                            {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.EXPAND requires graph feature"))
                                            }
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
                                            // ── 154-01: FT.AGGREGATE write-run local exec ──
                                            // Outer `guard = db[conn.selected_db].write()` is already
                                            // held; pass as `&*guard` (mut → immut reborrow is safe —
                                            // execute_local_full takes &Database). `ts_m3` is the
                                            // write-run TextStore borrow. Synchronous.
                                            #[cfg(feature = "text-index")]
                                            {
                                                match crate::command::vector_search::ft_aggregate::parse_aggregate_args(d_args) {
                                                    Ok(parsed) => crate::command::vector_search::ft_aggregate::execute_local_full(
                                                        &mut *store,
                                                        &*ts_m3,
                                                        &parsed.index_name,
                                                        &parsed.query,
                                                        &parsed.pipeline,
                                                        &*guard,
                                                        conn.selected_db as u8,
                                                    ),
                                                    Err(err_frame) => err_frame,
                                                }
                                            }
                                            #[cfg(not(feature = "text-index"))]
                                            {
                                                Frame::Error(bytes::Bytes::from_static(b"ERR FT.AGGREGATE requires text-index feature"))
                                            }
                                        } else {
                                            Frame::Error(bytes::Bytes::from_static(b"ERR unknown FT.* command"))
                                        };
                                        responses[resp_idx] = response;
                                        continue;
                                    } else {
                                        responses[resp_idx] = Frame::Error(bytes::Bytes::from_static(b"ERR vector search not initialized"));
                                        continue;
                                    }
                                }

                                // MA2: KILL SNAPSHOT <txn_id> — admin command routed to
                                // VectorStore's TransactionManager. Must intercept before
                                // the main dispatch() path which has no VectorStore access.
                                if d_cmd.eq_ignore_ascii_case(b"KILL") {
                                    if let Some(vs) = vector_store.as_ref() {
                                        let mut vs_guard = vs.lock();
                                        let response = crate::command::server_admin::kill_snapshot(
                                            &mut vs_guard,
                                            d_args,
                                        );
                                        drop(vs_guard);
                                        responses[resp_idx] = response;
                                    } else {
                                        responses[resp_idx] = Frame::Error(
                                            bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                        );
                                    }
                                    continue;
                                }

                                // P8: VACUUM — write-run intercept (same as read-run;
                                // VACUUM is admin so may land in either branch depending
                                // on future flag changes).
                                //
                                // B1 fix: route VECTOR/GRAPH to dedicated entry points.
                                if d_cmd.eq_ignore_ascii_case(b"VACUUM") {
                                    let mut routed_subcommand = false;
                                    if let Some(sub_frame) = d_args.first() {
                                        if let Some(sub) =
                                            crate::command::helpers::extract_bytes(sub_frame)
                                        {
                                            if sub.eq_ignore_ascii_case(b"VECTOR") {
                                                if let Some(vs) = vector_store.as_ref() {
                                                    let mut vs_guard = vs.lock();
                                                    let response =
                                                        crate::command::server_admin::vacuum_vector(
                                                            &mut vs_guard,
                                                            &d_args[1..],
                                                        conn.selected_db as u8,
                                        );
                                                    drop(vs_guard);
                                                    responses[resp_idx] = response;
                                                } else {
                                                    responses[resp_idx] = Frame::Error(
                                                        bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                                    );
                                                }
                                                routed_subcommand = true;
                                            }
                                            #[cfg(feature = "graph")]
                                            if sub.eq_ignore_ascii_case(b"GRAPH") {
                                                if let Some(ref gs) = graph_store {
                                                    let mut gs_guard = gs.lock();
                                                    let response =
                                                        crate::command::server_admin::vacuum_graph(
                                                            &mut gs_guard,
                                                            &d_args[1..],
                                                            config.graph_merge_max_segments,
                                                            config.graph_dead_edge_trigger,
                                                        );
                                                    drop(gs_guard);
                                                    responses[resp_idx] = response;
                                                } else {
                                                    responses[resp_idx] = Frame::Error(
                                                        bytes::Bytes::from_static(b"ERR graph store not initialized"),
                                                    );
                                                }
                                                routed_subcommand = true;
                                            }
                                        }
                                    }
                                    if !routed_subcommand {
                                        if let Some(vs) = vector_store.as_ref() {
                                            let mut vs_guard = vs.lock();
                                            let response = crate::command::server_admin::vacuum(
                                                &mut vs_guard,
                                                None, // manifest — not available here
                                                None, // wal_v3 — not available here
                                                d_args,
                                                crate::command::server_admin::DEFAULT_VACUUM_PRUNE_MARGIN, // see server_admin.rs
                                                None, // disk_offload_dir — dead: wal is None on this path too
                                                0,    // shard_id — dead, see above
                                            );
                                            drop(vs_guard);
                                            responses[resp_idx] = response;
                                        } else {
                                            responses[resp_idx] = Frame::Error(
                                                bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                            );
                                        }
                                    }
                                    continue;
                                }

                                // P8: DEBUG RECLAMATION — write-run intercept.
                                if d_cmd.eq_ignore_ascii_case(b"DEBUG") {
                                    if let Some(sub) = d_args.first() {
                                        if let Some(s) = crate::command::helpers::extract_bytes(sub) {
                                            if s.eq_ignore_ascii_case(b"RECLAMATION") {
                                                if let Some(vs) = vector_store.as_ref() {
                                                    let vs_guard = vs.lock();
                                                    let response = crate::command::server_admin::debug_reclamation(
                                                        &vs_guard, None, None,
                                                    );
                                                    drop(vs_guard);
                                                    responses[resp_idx] = response;
                                                } else {
                                                    responses[resp_idx] = Frame::Error(
                                                        bytes::Bytes::from_static(b"ERR vector store not initialized"),
                                                    );
                                                }
                                                continue;
                                            }
                                        }
                                    }
                                    // Other DEBUG subcommands fall through to dispatch().
                                }

                                // T2.2 MOVE — needs two databases simultaneously.
                                // dispatch() only receives one &mut Database; intercept here.
                                if d_cmd.eq_ignore_ascii_case(b"MOVE") {
                                    let src_db = conn.selected_db;
                                    let response = match crate::command::keyspace::move_cmd::parse_move_args(d_args, db_count) {
                                        Err(e) => e,
                                        Ok((_key, dst_db)) if dst_db == src_db => Frame::Integer(0),
                                        Ok((key, dst_db)) => {
                                            // Release single-db guard before acquiring two-db locks
                                            drop(guard);
                                            let r = crate::command::keyspace::move_cmd::with_two_dbs_locked(
                                                db.as_slice(), src_db, dst_db,
                                                |src, dst| crate::command::keyspace::move_cmd::move_core(src, dst, &key),
                                            );
                                            // Restore loop invariant: re-acquire guard
                                            current_db = conn.selected_db;
                                            guard = db[current_db].write();
                                            guard.refresh_now();
                                            r
                                        }
                                    };
                                    if matches!(response, Frame::Integer(1)) {
                                        if let Some(bytes) = &aof_bytes {
                                            // task #35: MOVE persists against its
                                            // SOURCE db (src_db == conn.selected_db;
                                            // MOVE never changes the connection's
                                            // selected db).
                                            aof_entries.push((resp_idx, src_db, bytes.clone()));
                                        }
                                    }
                                    responses[resp_idx] = response;
                                    continue;
                                }

                                // T2.3 COPY DB n — cross-db copy needs two databases.
                                // parse_copy_db_args returns None when no DB clause or same db
                                // (falls through to key_extra::copy for the single-db case).
                                if d_cmd.eq_ignore_ascii_case(b"COPY") {
                                    let src_db = conn.selected_db;
                                    if let Some(copy_result) = crate::command::keyspace::move_cmd::parse_copy_db_args(d_args, src_db, db_count) {
                                        let response = match copy_result {
                                            Err(e) => e,
                                            Ok(ca) => {
                                                drop(guard);
                                                let r = crate::command::keyspace::move_cmd::with_two_dbs_locked(
                                                    db.as_slice(), src_db, ca.dst_db,
                                                    |src, dst| crate::command::keyspace::move_cmd::copy_core(src, dst, &ca.src_key, &ca.dst_key, ca.replace),
                                                );
                                                current_db = conn.selected_db;
                                                guard = db[current_db].write();
                                                guard.refresh_now();
                                                r
                                            }
                                        };
                                        if matches!(response, Frame::Integer(1)) {
                                            if let Some(bytes) = &aof_bytes {
                                                // task #35: COPY ... DB n persists
                                                // against its SOURCE db, same as MOVE.
                                                aof_entries.push((resp_idx, src_db, bytes.clone()));
                                            }
                                        }
                                        responses[resp_idx] = response;
                                        continue;
                                    }
                                    // No DB clause or same-db: fall through to dispatch() → key_extra::copy
                                }

                                // HSET auto-indexing: after dispatch, check for vector index match
                                let is_hset = d_cmd.eq_ignore_ascii_case(b"HSET");

                                let dispatch_start = std::time::Instant::now();
                                let result = dispatch(&mut *guard, d_cmd, d_args, &mut conn.selected_db, db_count);
                                let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                                if let Ok(cmd_str) = std::str::from_utf8(d_cmd) {
                                    crate::admin::metrics_setup::record_command_cached(
                                        cmd_str,
                                        elapsed_us,
                                        &mut conn.cached_metrics,
                                    );
                                }
                                if let Frame::Array(ref args) = *disp_frame {
                                    crate::admin::metrics_setup::global_slowlog().maybe_record(
                                        elapsed_us,
                                        args.as_slice(),
                                        peer_addr.as_bytes(),
                                        conn.client_name.as_ref().map_or(b"" as &[u8], |n| n.as_ref()),
                                    );
                                }
                                let (response, quit) = match result {
                                    DispatchResult::Response(f) => (f, false),
                                    DispatchResult::Quit(f) => (f, true),
                                };

                                // Auto-index vector/text on successful HSET
                                if is_hset && !matches!(&response, Frame::Error(_)) {
                                    if let Some(ref vs) = vector_store {
                                        if let Some(key) = d_args.first().and_then(|f| extract_bytes(f)) {
                                            let mut store = vs.lock();
                                            if let Some(ref ts) = text_store {
                                                let mut ts_guard = ts.lock();
                                                // Plan 166-01: discard return (no TXN here).
                                                let _ = crate::shard::spsc_handler::auto_index_hset_public(&mut store, &mut *ts_guard, &key, d_args, conn.selected_db as u8);
                                            } else {
                                                let mut fallback_ts = crate::text::store::TextStore::new();
                                                let _ = crate::shard::spsc_handler::auto_index_hset_public(&mut store, &mut fallback_ts, &key, d_args, conn.selected_db as u8);
                                            }
                                        }
                                    }
                                }

                                // Auto-delete vectors on DEL/UNLINK (parity with
                                // the HSET auto-index hook above).
                                if !matches!(&response, Frame::Error(_))
                                    && (d_cmd.eq_ignore_ascii_case(b"DEL")
                                        || d_cmd.eq_ignore_ascii_case(b"UNLINK"))
                                {
                                    if let Some(ref vs) = vector_store {
                                        crate::shard::spsc_handler::auto_delete_vectors(
                                            &mut vs.lock(),
                                            d_args,
                                            conn.selected_db as u8,
                                        );
                                    }
                                }

                                // R4: HDEL of an indexed vector field tombstones it.
                                if !matches!(&response, Frame::Error(_))
                                    && d_cmd.eq_ignore_ascii_case(b"HDEL")
                                {
                                    if let Some(ref vs) = vector_store {
                                        crate::shard::spsc_handler::auto_hdel_vectors(
                                            &mut vs.lock(),
                                            d_args,
                                            conn.selected_db as u8,
                                        );
                                    }
                                }

                                // R3: FLUSHALL/FLUSHDB clears vector + text index
                                // contents (FT.CREATE definitions survive).
                                // WS5a: FLUSHDB scopes to `conn.selected_db`;
                                // FLUSHALL clears every db.
                                if !matches!(&response, Frame::Error(_))
                                    && (d_cmd.eq_ignore_ascii_case(b"FLUSHDB")
                                        || d_cmd.eq_ignore_ascii_case(b"FLUSHALL"))
                                {
                                    if d_cmd.eq_ignore_ascii_case(b"FLUSHDB") {
                                        if let Some(ref vs) = vector_store {
                                            vs.lock()
                                                .clear_all_contents_for_db(conn.selected_db as u8);
                                        }
                                        if let Some(ref ts) = text_store {
                                            ts.lock()
                                                .clear_all_contents_for_db(conn.selected_db as u8);
                                        }
                                    } else {
                                        if let Some(ref vs) = vector_store {
                                            vs.lock().clear_all_contents();
                                        }
                                        if let Some(ref ts) = text_store {
                                            ts.lock().clear_all_contents();
                                        }
                                    }
                                }

                                // Invalidate EVERY tracked key of a successful
                                // write (multi-key writes included), from any
                                // writer — gated by tracking_active().
                                if !matches!(&response, Frame::Error(_)) {
                                    crate::tracking::invalidation::invalidate_after_write(
                                        &tracking_table,
                                        d_cmd,
                                        d_args,
                                        client_id,
                                    );
                                    if d_cmd.eq_ignore_ascii_case(b"FLUSHALL")
                                        || d_cmd.eq_ignore_ascii_case(b"FLUSHDB")
                                    {
                                        crate::tracking::invalidation::invalidate_flush(
                                            &tracking_table,
                                        );
                                    }
                                    if let Some(bytes) = aof_bytes {
                                        aof_entries.push((resp_idx, conn.selected_db, bytes.clone()));
                                    }
                                }
                                // Apply RESP3 response conversion if needed
                                let response = apply_resp3_conversion(
                                    d_cmd,
                                    d_args,
                                    response,
                                    framed.codec().protocol_version(),
                                );
                                responses[resp_idx] = response;
                                if quit {
                                    should_quit = true;
                                    break;
                                }
                            }
                            // write guard dropped here
                        }

                        if should_quit {
                            break;
                        }
                    }
                } // all locks dropped here -- BEFORE any await

                // FIX-W1-1: appendfsync=always ordering — H1 close for the single-shard
                // tokio path. Under Always policy: await all AOF fsync acks FIRST, patch
                // any failed response slots with WRITEFAIL, THEN flush responses to the
                // client (delegated to `flush_with_aof_ack` so tests can call the real
                // production path rather than reproducing it inline).
                // Under EverySec/No: keep existing fire-and-forget ordering (flush
                // responses first, then enqueue AOF in the background — no latency impact).
                let use_always_ordering = aof_pool
                    .as_ref()
                    .map(|p| p.fsync_policy() == crate::persistence::aof::FsyncPolicy::Always)
                    .unwrap_or(false);

                if use_always_ordering {
                    // `use_always_ordering` is only true when aof_pool is Some + Always.
                    if let Some(ref pool) = aof_pool {
                        break_outer = flush_with_aof_ack(
                            &mut framed,
                            responses,
                            aof_entries,
                            pool,
                            &repl_state,
                            &change_counter,
                        )
                        .await;
                    }
                } else {
                    // EverySec / No policy: flush responses first (zero added latency),
                    // then fire-and-forget AOF enqueue.
                    for response in responses {
                        if !send_bounded!(framed, response, write_timeout, client_id) {
                            break_outer = true;
                            break;
                        }
                    }
                    for (_, entry_db, bytes) in aof_entries {
                        if let Some(ref pool) = aof_pool {
                            let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(&repl_state, 0, bytes.len());
                            let _ = pool.try_send_append_durable(0, lsn, entry_db, bytes).await;
                        }
                        if let Some(ref counter) = change_counter {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                arena.reset(); // O(1) bulk deallocation of batch temporaries

                if break_outer || should_quit {
                    break;
                }

                // The valid prefix has been executed and flushed. Name the
                // fault, then close — in that order.
                if let Some(kind) = proto_fault.take() {
                    use tokio::io::AsyncWriteExt;
                    let msg = crate::server::conn::util::proto_error_frame(kind);
                    let _ = framed.get_mut().write_all(msg.as_bytes()).await;
                    break;
                }
            }
            // Deliver tracking invalidation Push frames to client
            msg = async {
                if let Some(ref mut rx) = conn.tracking_rx {
                    rx.recv_async().await.ok()
                } else {
                    std::future::pending().await
                }
            } => {
                if let Some(push_frame) = msg {
                    if !send_bounded!(framed, push_frame, write_timeout, client_id) {
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                let _ = framed.send(Frame::Error(
                    Bytes::from_static(b"ERR server shutting down")
                )).await;
                break;
            }
        }
    }

    // Cleanup: remove subscriber from all channels/patterns on disconnect
    if conn.subscriber_id != 0 {
        let mut registry = pubsub_registry.lock();
        registry.unsubscribe_all(conn.subscriber_id);
        registry.punsubscribe_all(conn.subscriber_id);
    }

    // Cleanup: remove tracking state on disconnect
    if conn.tracking_state.enabled {
        tracking_table.lock().untrack_all(client_id);
    }
    crate::admin::metrics_setup::record_connection_closed();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aof::{AofAck, AofMessage, AofWriterPool, FsyncPolicy};
    use crate::runtime::channel;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    // ── Minimal recording sink ──────────────────────────────────────────────
    //
    // Implements `futures::Sink<Frame>` so `flush_with_aof_ack` can be called
    // directly in unit tests without a real TcpStream.  Each successful
    // `start_send` appends `(frame, Instant::now())` to the internal log.
    struct RecordingSink {
        log: Vec<(Frame, Instant)>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self { log: Vec::new() }
        }
        fn first_send_instant(&self) -> Option<Instant> {
            self.log.first().map(|(_, t)| *t)
        }
    }

    impl futures::Sink<Frame> for RecordingSink {
        type Error = ();

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), ()> {
            self.log.push((item, Instant::now()));
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// FIX-W1-1 r3 (updated for group commit): discriminating ordering test
    /// for `flush_with_aof_ack`.
    ///
    /// This test calls the **real** `flush_with_aof_ack` function that the
    /// production handler uses — not an inline copy.  The H1 contract is:
    ///
    ///   AOF durability MUST be confirmed BEFORE any response is sent.
    ///
    /// Under the group-commit protocol the writer channel carries the
    /// entry's fire-and-forget `Append` followed by ONE zero-length
    /// `AppendSync` barrier for the whole batch (previously: one awaited
    /// `AppendSync` per entry). The 60ms mock fsync delay gates the barrier
    /// ack; a broken ordering (flush responses before the barrier ack)
    /// would send the first response at ~0ms → `elapsed_ms < 55` → FAIL.
    #[tokio::test]
    async fn flush_with_aof_ack_ack_precedes_response() {
        // Build an Always-policy pool backed by a real bounded channel.
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::top_level_with_policy(
            tx,
            FsyncPolicy::Always,
            std::time::Duration::ZERO,
        );

        // Mock writer: receives the entry's fire-and-forget Append, then the
        // batch's ONE AppendSync barrier; sleeps 60ms to simulate the fsync,
        // then acks Synced. Runs on a blocking thread because flume's
        // `Receiver::recv()` is synchronous.
        let mock_writer = tokio::task::spawn_blocking(move || {
            let first = rx.recv().expect("mock writer: append received");
            assert!(
                matches!(first, AofMessage::Append { .. }),
                "group commit enqueues the entry fire-and-forget (Append first)"
            );
            let msg = rx.recv().expect("mock writer: barrier received");
            if let AofMessage::AppendSync { ack, .. } = msg {
                std::thread::sleep(Duration::from_millis(60));
                let _ = ack.send(AofAck::Synced);
            } else {
                panic!("Always policy MUST send an AppendSync barrier");
            }
        });

        let start = Instant::now();

        let responses = vec![Frame::SimpleString(bytes::Bytes::from_static(b"OK"))];
        let aof_entries = vec![(0usize, 0usize, bytes::Bytes::from_static(b"SET k v\r\n"))];
        let mut sink = RecordingSink::new();

        let broke = flush_with_aof_ack(
            &mut sink,
            responses,
            aof_entries,
            &pool,
            &None, // no replication state
            &None, // no change counter
        )
        .await;

        mock_writer.await.expect("mock writer completed cleanly");

        assert!(!broke, "sink send must not have failed");
        assert_eq!(sink.log.len(), 1, "exactly one response must be sent");

        let first_send = sink
            .first_send_instant()
            .expect("RecordingSink recorded at least one send");
        let elapsed_ms = first_send.duration_since(start).as_millis();

        assert!(
            elapsed_ms >= 55,
            "H1 violation: first response sent {elapsed_ms}ms after start — \
             expected >= 55ms (mock fsync delay is 60ms). \
             This means +OK was flushed before the AOF ack."
        );
    }
}
