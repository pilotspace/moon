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
use futures::{FutureExt, SinkExt, StreamExt};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio_util::codec::Framed;

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::AofMessage;
use crate::protocol::Frame;
use crate::pubsub::subscriber::Subscriber;
use crate::pubsub::{self, PubSubRegistry};
use crate::storage::eviction::try_evict_if_needed;
use crate::tracking::{TrackingState, TrackingTable};

use super::{
    SharedDatabases, apply_resp3_conversion, execute_transaction, extract_bytes, extract_command,
    handle_config,
};
use crate::framevec;
use crate::server::codec::RespCodec;

/// Handle a single client connection.
///
/// Reads frames from the TCP stream, dispatches commands, and writes responses.
/// Terminates on client disconnect, protocol error, QUIT command, or server shutdown.
///
/// When `requirepass` is set, clients must authenticate via AUTH before any other
/// commands are accepted (except QUIT).
///
/// When `aof_tx` is provided, write commands are logged to the AOF file.
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
    aof_tx: Option<channel::MpscSender<AofMessage>>,
    change_counter: Option<Arc<AtomicU64>>,
    pubsub_registry: Arc<Mutex<PubSubRegistry>>,
    runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
    tracking_table: Arc<Mutex<TrackingTable>>,
    client_id: u64,
    repl_state: Option<Arc<RwLock<crate::replication::state::ReplicationState>>>,
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

    // Per-connection arena for batch processing temporaries.
    // Primary use in Phase 8: scratch buffer during inline token assembly.
    // Phase 9+ will leverage this for per-request temporaries.
    let mut arena = Bump::with_capacity(4096); // 4KB initial capacity

    loop {
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
                                                let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
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
                                                let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
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
                                        );
                                        if !matches!(&response, Frame::Error(_)) {
                                            framed.codec_mut().set_protocol_version(new_proto);
                                        }
                                        if let Some(name) = new_name {
                                            conn.client_name = Some(name);
                                        }
                                        if let Some(uname) = opt_user {
                                            conn.current_user = uname;
                                            conn.refresh_acl_cache(&acl_table);
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
                            // Data is pre-serialized RESP — wrap in PreSerialized for framed send
                            if framed.send(Frame::PreSerialized(data)).await.is_err() {
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
                    Some(Err(_)) => break,
                    None => break,
                };

                // Collect batch: first frame + all immediately available frames
                let mut batch = vec![first_frame];
                const MAX_BATCH: usize = 1024;
                while batch.len() < MAX_BATCH {
                    match framed.next().now_or_never() {
                        Some(Some(Ok(frame))) => batch.push(frame),
                        _ => break,
                    }
                }

                // Process batch using two-phase execution:
                // Phase 1: Handle connection-level intercepts, collect dispatchable frames
                // Phase 2: Acquire ONE write lock, execute ALL dispatchable frames
                let mut responses: Vec<Frame> = Vec::with_capacity(batch.len());
                let mut aof_entries: Vec<Bytes> = Vec::new();
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
                                    conn.current_user = uname;
                                    conn.refresh_acl_cache(&acl_table);
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
                                );
                                // CRITICAL: Set protocol version BEFORE sending response (Pitfall 6)
                                if !matches!(&response, Frame::Error(_)) {
                                    framed.codec_mut().set_protocol_version(new_proto);
                                }
                                if let Some(name) = new_name {
                                    conn.client_name = Some(name);
                                }
                                if let Some(uname) = opt_user {
                                    conn.current_user = uname;
                                    conn.refresh_acl_cache(&acl_table);
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
                                conn.current_user = uname;
                                conn.refresh_acl_cache(&acl_table);
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
                            );
                            // CRITICAL: Set protocol version BEFORE sending response (Pitfall 6)
                            if !matches!(&response, Frame::Error(_)) {
                                framed.codec_mut().set_protocol_version(new_proto);
                            }
                            if let Some(name) = new_name {
                                conn.client_name = Some(name);
                            }
                            if let Some(uname) = opt_user {
                                conn.current_user = uname;
                                conn.refresh_acl_cache(&acl_table);
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
                        // BGREWRITEAOF
                        if cmd.eq_ignore_ascii_case(b"BGREWRITEAOF") {
                            let response = if let Some(ref tx) = aof_tx {
                                crate::command::persistence::bgrewriteaof_start(tx, db.clone())
                            } else {
                                Frame::Error(Bytes::from_static(b"ERR AOF is not enabled"))
                            };
                            responses.push(response);
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
                                            if let Ok(mut rs_guard) = rs.write() {
                                                rs_guard.role = crate::replication::state::ReplicationRole::Replica {
                                                    host: host.clone(),
                                                    port,
                                                    state: crate::replication::handshake::ReplicaHandshakeState::PingPending,
                                                };
                                            }
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
                            let resp = crate::command::connection::replconf(cmd_args);
                            responses.push(resp);
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
                                let guard = db[conn.selected_db].read();
                                let resp_frame = conn_cmd::info_readonly(&guard, cmd_args);
                                drop(guard);
                                let mut response_text = match resp_frame {
                                    Frame::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                                    _ => String::new(),
                                };
                                if let Ok(rs_guard) = rs.try_read() {
                                    response_text.push_str(
                                        &crate::replication::handshake::build_info_replication(&rs_guard),
                                    );
                                }
                                responses.push(Frame::BulkString(Bytes::from(response_text)));
                                continue;
                            }
                            // Fall through to normal dispatch if no repl_state
                        }

                        // --- READONLY enforcement: reject writes on replicas ---
                        if let Some(ref rs) = repl_state {
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

                        // SUBSCRIBE / PSUBSCRIBE: enter subscriber mode
                        // Flush accumulated responses first, then handle subscribe and break batch
                        if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                            // Execute any pending dispatchable frames before switching modes
                            if !dispatchable.is_empty() {
                                let mut guard = db[conn.selected_db].write();
                                guard.refresh_now();
                                let db_count = db.len();
                                for (resp_idx, disp_frame, is_write, aof_bytes) in dispatchable.drain(..) {
                                    if is_write {
                                        let rt = runtime_config.read();
                                        if let Err(oom_frame) = try_evict_if_needed(&mut *guard, &rt) {
                                            responses[resp_idx] = oom_frame;
                                            continue;
                                        }
                                    }
                                    #[allow(clippy::unwrap_used)] // Frame was parsed earlier; extract_command succeeds on valid frames
                                    let (d_cmd, d_args) = extract_command(&disp_frame).unwrap();
                                    let dispatch_start = std::time::Instant::now();
                                    let result = dispatch(&mut *guard, d_cmd, d_args, &mut conn.selected_db, db_count);
                                    let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                                    if let Ok(cmd_str) = std::str::from_utf8(d_cmd) {
                                        crate::admin::metrics_setup::record_command(cmd_str, elapsed_us);
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
                                            aof_entries.push(bytes);
                                        }
                                    }
                                    // Apply RESP3 response conversion if needed
                                    let response = apply_resp3_conversion(d_cmd, response, framed.codec().protocol_version());
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

                            // Flush accumulated responses first
                            for resp in responses.drain(..) {
                                if framed.send(resp).await.is_err() {
                                    break_outer = true;
                                    break;
                                }
                            }
                            if break_outer {
                                break;
                            }
                            // Send AOF entries accumulated so far
                            for bytes in aof_entries.drain(..) {
                                if let Some(ref tx) = aof_tx {
                                    let _ = tx.send_async(AofMessage::Append(bytes)).await;
                                }
                                if let Some(ref counter) = change_counter {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                }
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
                                        let sub = Subscriber::new(conn.pubsub_tx.clone().unwrap(), conn.subscriber_id);
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
                                        if framed.send(response).await.is_err() {
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
                        if cmd.eq_ignore_ascii_case(b"PUBLISH") {
                            if cmd_args.len() != 2 {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR wrong number of arguments for 'publish' command"),
                                ));
                            } else {
                                let channel = extract_bytes(&cmd_args[0]);
                                let message = extract_bytes(&cmd_args[1]);
                                match (channel, message) {
                                    (Some(ch), Some(msg)) => {
                                        let count = pubsub_registry.lock().publish(&ch, &msg);
                                        responses.push(Frame::Integer(count));
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
                                let (result, txn_aof_entries) = execute_transaction(
                                    &db,
                                    &conn.command_queue,
                                    &conn.watched_keys,
                                    &mut conn.selected_db,
                                );
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
                                                            crate::shard::spsc_handler::auto_index_hset_public(
                                                                &mut store, &mut *ts_guard, key_bytes, a,
                                                            );
                                                        } else {
                                                            crate::shard::spsc_handler::auto_index_hset_public(
                                                                &mut store, &mut fallback_ts, key_bytes, a,
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                conn.command_queue.clear();
                                conn.watched_keys.clear();
                                responses.push(result);
                                aof_entries.extend(txn_aof_entries);
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
                                        conn.watched_keys.insert(key.clone(), version);
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
                                                        // B-01 FIX (single-shard handler_single, Plan 152-06):
                                                        // FieldFilter short-circuit BEFORE text_fields.is_empty() bail.
                                                        #[cfg(feature = "text-index")]
                                                        match crate::command::vector_search::pre_parse_field_filter(query_bytes.as_ref()) {
                                                            Ok(Some(clause)) => {
                                                                if clause.filter.is_some() {
                                                                    let (offset, count) = crate::command::vector_search::parse_limit_clause(cmd_args);
                                                                    let top_k = if count == usize::MAX { 10000 } else { offset.saturating_add(count) }.max(1);
                                                                    let response = match ts_mut.get_index(&index_name) {
                                                                        None => crate::protocol::Frame::Error(bytes::Bytes::from_static(b"ERR no such index")),
                                                                        Some(text_index) => {
                                                                            let results = crate::command::vector_search::ft_text_search::execute_query_on_index(
                                                                                text_index, &clause, None, None, top_k,
                                                                            );
                                                                            crate::command::vector_search::ft_text_search::build_text_response(
                                                                                &results, offset, count,
                                                                            )
                                                                        }
                                                                    };
                                                                    responses.push(response);
                                                                    continue;
                                                                }
                                                            }
                                                            Ok(None) => { /* fall through */ }
                                                            Err(e) => {
                                                                responses.push(crate::protocol::Frame::Error(bytes::Bytes::copy_from_slice(e.as_bytes())));
                                                                continue;
                                                            }
                                                        }
                                                        // Step 2: TextStore is already borrowed via ts_mut (no extra guard to drop).
                                                        // When text_store is None, ts_mut points at the empty fallback —
                                                        // get_index() returns None → Step 3 emits "ERR no such index" (correct).
                                                        // Step 3: index lookup.
                                                        let text_index = match ts_mut.get_index(&index_name) {
                                                            Some(idx) => idx,
                                                            None => {
                                                                responses.push(crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                                                    b"ERR no such index",
                                                                )));
                                                                continue;
                                                            }
                                                        };
                                                        // Step 4: ensure at least one TEXT field.
                                                        if text_index.text_fields.is_empty() {
                                                            responses.push(crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                                                b"ERR index has no TEXT fields",
                                                            )));
                                                            continue;
                                                        }
                                                        // Step 5: parse via first analyzer.
                                                        let analyzer = match text_index.field_analyzers.first() {
                                                            Some(a) => a,
                                                            None => {
                                                                responses.push(crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                                                    b"ERR index has no TEXT fields",
                                                                )));
                                                                continue;
                                                            }
                                                        };
                                                        let clause = match crate::command::vector_search::parse_text_query(
                                                            query_bytes.as_ref(),
                                                            analyzer,
                                                        ) {
                                                            Ok(c) => c,
                                                            Err(msg) => {
                                                                responses.push(crate::protocol::Frame::Error(
                                                                    bytes::Bytes::copy_from_slice(msg.as_bytes()),
                                                                ));
                                                                continue;
                                                            }
                                                        };
                                                        // Step 5b: resolve field_idx.
                                                        let field_idx = match &clause.field_name {
                                                            None => None,
                                                            Some(field_name) => match text_index
                                                                .text_fields
                                                                .iter()
                                                                .position(|f| {
                                                                    f.field_name.as_ref().eq_ignore_ascii_case(
                                                                        field_name.as_ref(),
                                                                    )
                                                                }) {
                                                                Some(idx) => Some(idx),
                                                                None => {
                                                                    let bad_name = field_name.clone();
                                                                    responses.push(crate::protocol::Frame::Error(bytes::Bytes::from(
                                                                        format!(
                                                                            "ERR unknown field '{}'",
                                                                            String::from_utf8_lossy(&bad_name)
                                                                        ),
                                                                    )));
                                                                    continue;
                                                                }
                                                            },
                                                        };
                                                        // Step 6: query_terms.
                                                        let query_terms = clause.terms;
                                                        // Step 7: LIMIT + top_k cap (T-151-03-02).
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
                                                        // Step 8: HIGHLIGHT / SUMMARIZE.
                                                        let highlight_opts =
                                                            crate::command::vector_search::parse_highlight_clause(
                                                                cmd_args,
                                                            );
                                                        let summarize_opts =
                                                            crate::command::vector_search::parse_summarize_clause(
                                                                cmd_args,
                                                            );
                                                        // Step 9: DB read guard iff post-processing needed.
                                                        let db_guard_opt = if highlight_opts.is_some()
                                                            || summarize_opts.is_some()
                                                        {
                                                            Some(db[conn.selected_db].read())
                                                        } else {
                                                            None
                                                        };
                                                        // Step 10: execute + optional post-processing.
                                                        let mut response =
                                                            crate::command::vector_search::execute_text_search_local(
                                                                &*ts_mut,
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
                                                        // Explicit drop of db_guard (inner); ts_mut / ts_guard drop at scope end.
                                                        drop(db_guard_opt);
                                                        responses.push(response);
                                                        continue;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    let response = if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                                        crate::command::vector_search::ft_create(&mut *store, ts_mut, cmd_args)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                        let has_session = cmd_args.iter().any(|a| {
                                            if let crate::protocol::Frame::BulkString(b) = a { b.eq_ignore_ascii_case(b"SESSION") } else { false }
                                        });
                                        if has_session {
                                            let mut db_guard = db[conn.selected_db].write();
                                            crate::command::vector_search::ft_search(&mut *store, cmd_args, Some(&mut *db_guard), Some(&*ts_mut), 0)
                                        } else {
                                            crate::command::vector_search::ft_search(&mut *store, cmd_args, None, Some(&*ts_mut), 0)
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                                        let mut db_guard = db[conn.selected_db].write();
                                        crate::command::vector_search::ft_dropindex(&mut *store, ts_mut, Some(&mut *db_guard), cmd_args)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
                                        crate::command::vector_search::ft_info(&*store, ts_mut, cmd_args)
                                    } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
                                        crate::command::vector_search::ft_list(&*store)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                                        crate::command::vector_search::ft_compact(&mut *store, ts_mut, cmd_args)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                                        crate::command::vector_search::cache_search::ft_cachesearch(&mut *store, cmd_args)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                                        crate::command::vector_search::ft_config(&mut *store, ts_mut, cmd_args)
                                    } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                        let mut db_guard = db[conn.selected_db].write();
                                        crate::command::vector_search::recommend::ft_recommend(&mut *store, cmd_args, Some(&mut *db_guard))
                                    } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                        #[cfg(feature = "graph")]
                                        {
                                            let gs_frame = if let Some(ref gs) = graph_store {
                                                let graph_guard = gs.lock();
                                                crate::command::vector_search::navigate::ft_navigate(&mut *store, Some(&graph_guard), cmd_args, None)
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
                                    for record in wal_records {
                                        if let Some(ref tx) = aof_tx {
                                            let _ = tx.send_async(AofMessage::Append(bytes::Bytes::from(record))).await;
                                        }
                                        if let Some(ref counter) = change_counter {
                                            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                    }
                                    responses.push(response);
                                    continue;
                                } else {
                                    responses.push(Frame::Error(bytes::Bytes::from_static(b"ERR graph engine not initialized")));
                                    continue;
                                }
                            }

                            let is_write = metadata::is_write(cmd);

                            // Serialize for AOF before dispatch
                            let aof_bytes = if is_write && aof_tx.is_some() {
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
                                            let has_session = d_args.iter().any(|a| {
                                                if let crate::protocol::Frame::BulkString(b) = a { b.eq_ignore_ascii_case(b"SESSION") } else { false }
                                            });
                                            if has_session {
                                                drop(guard);
                                                let mut db_guard = db[conn.selected_db].write();
                                                let r = crate::command::vector_search::ft_search(&mut *store, d_args, Some(&mut *db_guard), Some(&*ts_m2), 0);
                                                drop(db_guard);
                                                guard = db[conn.selected_db].read();
                                                r
                                            } else {
                                                crate::command::vector_search::ft_search(&mut *store, d_args, None, Some(&*ts_m2), 0)
                                            }
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.INFO") {
                                            crate::command::vector_search::ft_info(&*store, ts_m2, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT._LIST") {
                                            crate::command::vector_search::ft_list(&*store)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                                            crate::command::vector_search::ft_compact(&mut *store, ts_m2, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                                            crate::command::vector_search::cache_search::ft_cachesearch(&mut *store, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                                            crate::command::vector_search::ft_config(&mut *store, ts_m2, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                            drop(guard);
                                            let mut db_guard = db[conn.selected_db].write();
                                            let r = crate::command::vector_search::recommend::ft_recommend(&mut *store, d_args, Some(&mut *db_guard));
                                            drop(db_guard);
                                            guard = db[conn.selected_db].read();
                                            r
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                            #[cfg(feature = "graph")]
                                            {
                                                if let Some(ref gs) = graph_store {
                                                    let graph_guard = gs.lock();
                                                    crate::command::vector_search::navigate::ft_navigate(&mut *store, Some(&graph_guard), d_args, None)
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

                                let dispatch_start = std::time::Instant::now();
                                let result = dispatch_read(&*guard, d_cmd, d_args, now_ms, &mut conn.selected_db, db_count);
                                let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                                if let Ok(cmd_str) = std::str::from_utf8(d_cmd) {
                                    crate::admin::metrics_setup::record_command(cmd_str, elapsed_us);
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
                                // Track key on read for client-side caching invalidation
                                if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                                    if let Some(key) = d_args.first().and_then(|f| extract_bytes(f)) {
                                        tracking_table.lock().track_key(client_id, &key, conn.tracking_state.noloop);
                                    }
                                }
                                // Apply RESP3 response conversion if needed
                                let response = apply_resp3_conversion(d_cmd, response, proto);
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
                                let rt = runtime_config.read();
                                if let Err(oom_frame) = try_evict_if_needed(&mut *guard, &rt) {
                                    responses[resp_idx] = oom_frame;
                                    continue;
                                }
                                drop(rt);
                                #[allow(clippy::unwrap_used)] // Frame was parsed earlier; extract_command succeeds on valid frames
                                let (d_cmd, d_args) = extract_command(disp_frame).unwrap();

                                // FT.* vector commands: dispatch to VectorStore directly
                                if d_cmd.len() > 3 && d_cmd[..3].eq_ignore_ascii_case(b"FT.") {
                                    if let Some(ref vs) = vector_store {
                                        let mut store = vs.lock();
                                        let mut fb_ts3 = crate::text::store::TextStore::new();
                                        let mut ts_g3 = text_store.as_ref().map(|ts| ts.lock());
                                        let ts_m3 = match ts_g3 { Some(ref mut g) => &mut **g, None => &mut fb_ts3 };
                                        let response = if d_cmd.eq_ignore_ascii_case(b"FT.CREATE") {
                                            crate::command::vector_search::ft_create(&mut *store, ts_m3, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
                                            // Write run: guard is already write-locked
                                            crate::command::vector_search::ft_search(&mut *store, d_args, Some(&mut *guard), Some(&*ts_m3), 0)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
                                            crate::command::vector_search::ft_dropindex(&mut *store, ts_m3, Some(&mut *guard), d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.INFO") {
                                            crate::command::vector_search::ft_info(&*store, ts_m3, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT._LIST") {
                                            crate::command::vector_search::ft_list(&*store)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
                                            crate::command::vector_search::ft_compact(&mut *store, ts_m3, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
                                            crate::command::vector_search::cache_search::ft_cachesearch(&mut *store, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
                                            crate::command::vector_search::ft_config(&mut *store, ts_m3, d_args)
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
                                            crate::command::vector_search::recommend::ft_recommend(&mut *store, d_args, Some(&mut *guard))
                                        } else if d_cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
                                            #[cfg(feature = "graph")]
                                            {
                                                if let Some(ref gs) = graph_store {
                                                    let graph_guard = gs.lock();
                                                    crate::command::vector_search::navigate::ft_navigate(&mut *store, Some(&graph_guard), d_args, None)
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

                                // HSET auto-indexing: after dispatch, check for vector index match
                                let is_hset = d_cmd.eq_ignore_ascii_case(b"HSET");

                                let dispatch_start = std::time::Instant::now();
                                let result = dispatch(&mut *guard, d_cmd, d_args, &mut conn.selected_db, db_count);
                                let elapsed_us = dispatch_start.elapsed().as_micros() as u64;
                                if let Ok(cmd_str) = std::str::from_utf8(d_cmd) {
                                    crate::admin::metrics_setup::record_command(cmd_str, elapsed_us);
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
                                                crate::shard::spsc_handler::auto_index_hset_public(&mut store, &mut *ts_guard, &key, d_args);
                                            } else {
                                                let mut fallback_ts = crate::text::store::TextStore::new();
                                                crate::shard::spsc_handler::auto_index_hset_public(&mut store, &mut fallback_ts, &key, d_args);
                                            }
                                        }
                                    }
                                }

                                // Invalidate tracked key on successful write
                                if !matches!(&response, Frame::Error(_)) {
                                    if let Some(key) = d_args.first().and_then(|f| extract_bytes(f)) {
                                        let senders = tracking_table.lock().invalidate_key(&key, client_id);
                                        if !senders.is_empty() {
                                            let push = crate::tracking::invalidation::invalidation_push(&[key]);
                                            for tx in senders {
                                                let _ = tx.try_send(push.clone());
                                            }
                                        }
                                    }
                                    if let Some(bytes) = aof_bytes {
                                        aof_entries.push(bytes.clone());
                                    }
                                }
                                // Apply RESP3 response conversion if needed
                                let response = apply_resp3_conversion(d_cmd, response, framed.codec().protocol_version());
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

                // --- Write all responses OUTSIDE the lock ---
                for response in responses {
                    if framed.send(response).await.is_err() {
                        break_outer = true;
                        break;
                    }
                }

                // --- Send AOF entries OUTSIDE the lock ---
                for bytes in aof_entries {
                    if let Some(ref tx) = aof_tx {
                        let _ = tx.send_async(AofMessage::Append(bytes)).await;
                    }
                    if let Some(ref counter) = change_counter {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }

                arena.reset(); // O(1) bulk deallocation of batch temporaries

                if break_outer || should_quit {
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
                    if framed.send(push_frame).await.is_err() {
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
