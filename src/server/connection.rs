#![allow(unused_imports, dead_code, unused_variables)]
use crate::runtime::TcpStream;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, SinkExt, StreamExt};
use parking_lot::Mutex;
use ringbuf::HeapProd;
use ringbuf::traits::Producer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
#[cfg(feature = "runtime-tokio")]
use tokio_util::codec::Framed;

use crate::command::config as config_cmd;
use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::pubsub::subscriber::Subscriber;
use crate::pubsub::{self, PubSubRegistry};
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::storage::Database;
use crate::storage::entry::CachedClock;
use crate::storage::eviction::try_evict_if_needed;
use crate::tracking::{TrackingState, TrackingTable};

use super::codec::RespCodec;
use super::conn::{
    SharedDatabases, apply_resp3_conversion, convert_blocking_to_nonblocking, extract_command,
    execute_transaction, execute_transaction_sharded, extract_primary_key, handle_config,
    is_multi_key_command,
};
#[cfg(feature = "runtime-tokio")]
use super::conn::handle_blocking_command;
#[cfg(feature = "runtime-monoio")]
use super::conn::{handle_blocking_command_monoio, try_inline_dispatch, try_inline_dispatch_loop};
use crate::framevec;

// Re-export extract_bytes for external callers (shard/mod.rs uses crate::server::connection::extract_bytes)
pub(crate) use super::conn::extract_bytes;

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
#[cfg(feature = "runtime-tokio")]
pub async fn handle_connection(
    stream: TcpStream,
    db: SharedDatabases,
    shutdown: CancellationToken,
    requirepass: Option<String>,
    config: Arc<ServerConfig>,
    aof_tx: Option<channel::MpscSender<AofMessage>>,
    change_counter: Option<Arc<AtomicU64>>,
    pubsub_registry: Arc<Mutex<PubSubRegistry>>,
    runtime_config: Arc<RwLock<RuntimeConfig>>,
    tracking_table: Arc<Mutex<TrackingTable>>,
    client_id: u64,
    repl_state: Option<Arc<RwLock<crate::replication::state::ReplicationState>>>,
    acl_table: Arc<RwLock<crate::acl::AclTable>>,
) {
    // Capture peer address before Framed wraps the stream (stream is moved)
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let mut framed = Framed::new(stream, RespCodec::default());
    let mut selected_db: usize = 0;
    let mut authenticated = requirepass.is_none();
    let mut current_user: String = "default".to_string();
    let acl_max_len = runtime_config
        .read()
        .map(|cfg| cfg.acllog_max_len)
        .unwrap_or(128);
    let mut acl_log = crate::acl::AclLog::new(acl_max_len);

    // Pub/Sub connection-local state
    let mut subscription_count: usize = 0;
    let mut pubsub_rx: Option<channel::MpscReceiver<Frame>> = None;
    let mut pubsub_tx: Option<channel::MpscSender<Frame>> = None;
    let mut subscriber_id: u64 = 0;

    // Client tracking state
    let mut tracking_state = TrackingState::default();
    let mut tracking_rx: Option<channel::MpscReceiver<Frame>> = None;

    // RESP3/HELLO connection-local state
    let mut client_name: Option<Bytes> = None;

    // Transaction (MULTI/EXEC) connection-local state
    let mut in_multi: bool = false;
    let mut command_queue: Vec<Frame> = Vec::new();
    let mut watched_keys: HashMap<Bytes, u32> = HashMap::new();

    // Per-connection arena for batch processing temporaries.
    // Primary use in Phase 8: scratch buffer during inline token assembly.
    // Phase 9+ will leverage this for per-request temporaries.
    let mut arena = Bump::with_capacity(4096); // 4KB initial capacity

    loop {
        // Subscriber mode: bidirectional select on client commands + published messages
        if subscription_count > 0 {
            let rx = pubsub_rx.as_mut().unwrap();
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
                                                    let acl_guard = acl_table.read().unwrap();
                                                    acl_guard.check_channel_permission(&current_user, channel.as_ref())
                                                };
                                                if let Some(deny_reason) = deny {
                                                    let _ = framed.send(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)))).await;
                                                    continue;
                                                }
                                                let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                pubsub_registry.lock().subscribe(channel.clone(), sub);
                                                subscription_count += 1;
                                                if framed.send(pubsub::subscribe_response(&channel, subscription_count)).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") => {
                                        if cmd_args.is_empty() {
                                            // Unsubscribe from all channels
                                            let removed = pubsub_registry.lock().unsubscribe_all(subscriber_id);
                                            if removed.is_empty() {
                                                // No channels, send response with count 0
                                                subscription_count = pubsub_registry.lock().total_subscription_count(subscriber_id);
                                                let _ = framed.send(pubsub::unsubscribe_response(
                                                    &Bytes::from_static(b""),
                                                    subscription_count,
                                                )).await;
                                            } else {
                                                for ch in &removed {
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::unsubscribe_response(ch, subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(channel) = extract_bytes(arg) {
                                                    pubsub_registry.lock().unsubscribe(channel.as_ref(), subscriber_id);
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::unsubscribe_response(&channel, subscription_count)).await.is_err() {
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
                                                    let acl_guard = acl_table.read().unwrap();
                                                    acl_guard.check_channel_permission(&current_user, pattern.as_ref())
                                                };
                                                if let Some(deny_reason) = deny {
                                                    let _ = framed.send(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)))).await;
                                                    continue;
                                                }
                                                let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                pubsub_registry.lock().psubscribe(pattern.clone(), sub);
                                                subscription_count += 1;
                                                if framed.send(pubsub::psubscribe_response(&pattern, subscription_count)).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    _ if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") => {
                                        if cmd_args.is_empty() {
                                            let removed = pubsub_registry.lock().punsubscribe_all(subscriber_id);
                                            if removed.is_empty() {
                                                subscription_count = pubsub_registry.lock().total_subscription_count(subscriber_id);
                                                let _ = framed.send(pubsub::punsubscribe_response(
                                                    &Bytes::from_static(b""),
                                                    subscription_count,
                                                )).await;
                                            } else {
                                                for pat in &removed {
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::punsubscribe_response(pat, subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(pattern) = extract_bytes(arg) {
                                                    pubsub_registry.lock().punsubscribe(pattern.as_ref(), subscriber_id);
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::punsubscribe_response(&pattern, subscription_count)).await.is_err() {
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
                                            &mut authenticated,
                                        );
                                        if !matches!(&response, Frame::Error(_)) {
                                            framed.codec_mut().set_protocol_version(new_proto);
                                        }
                                        if let Some(name) = new_name {
                                            client_name = Some(name);
                                        }
                                        if let Some(uname) = opt_user {
                                            current_user = uname;
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
                            // If subscription_count dropped to 0, exit subscriber mode
                            if subscription_count == 0 {
                                continue;
                            }
                        }
                        Some(Err(_)) => break,
                        None => break,
                    }
                }
                msg = rx.recv_async() => {
                    match msg {
                        Ok(frame) => {
                            if framed.send(frame).await.is_err() {
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
                    if !authenticated {
                        match extract_command(&frame) {
                            Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"AUTH") => {
                                let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                                if let Some(uname) = opt_user {
                                    authenticated = true;
                                    current_user = uname;
                                } else {
                                    // Log failed auth attempt
                                    acl_log.push(crate::acl::AclLogEntry {
                                        reason: "auth".to_string(),
                                        object: "AUTH".to_string(),
                                        username: current_user.clone(),
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
                                    &mut authenticated,
                                );
                                // CRITICAL: Set protocol version BEFORE sending response (Pitfall 6)
                                if !matches!(&response, Frame::Error(_)) {
                                    framed.codec_mut().set_protocol_version(new_proto);
                                }
                                if let Some(name) = new_name {
                                    client_name = Some(name);
                                }
                                if let Some(uname) = opt_user {
                                    current_user = uname;
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
                        // AUTH when already authenticated
                        if cmd.eq_ignore_ascii_case(b"AUTH") {
                            let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                            if let Some(uname) = opt_user {
                                current_user = uname;
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
                                &mut authenticated,
                            );
                            // CRITICAL: Set protocol version BEFORE sending response (Pitfall 6)
                            if !matches!(&response, Frame::Error(_)) {
                                framed.codec_mut().set_protocol_version(new_proto);
                            }
                            if let Some(name) = new_name {
                                client_name = Some(name);
                            }
                            if let Some(uname) = opt_user {
                                current_user = uname;
                            }
                            responses.push(response);
                            continue;
                        }
                        // ACL command -- intercepted at connection level
                        if cmd.eq_ignore_ascii_case(b"ACL") {
                            let response = crate::command::acl::handle_acl(
                                cmd_args,
                                &acl_table,
                                &mut acl_log,
                                &current_user,
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
                                            client_name = extract_bytes(&cmd_args[1]);
                                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                        }
                                        continue;
                                    }
                                    if sub_bytes.eq_ignore_ascii_case(b"GETNAME") {
                                        responses.push(match &client_name {
                                            Some(name) => Frame::BulkString(name.clone()),
                                            None => Frame::Null,
                                        });
                                        continue;
                                    }
                                    if sub_bytes.eq_ignore_ascii_case(b"TRACKING") {
                                        match crate::command::client::parse_tracking_args(cmd_args) {
                                            Ok(config_parsed) => {
                                                if config_parsed.enable {
                                                    tracking_state.enabled = true;
                                                    tracking_state.bcast = config_parsed.bcast;
                                                    tracking_state.noloop = config_parsed.noloop;
                                                    tracking_state.optin = config_parsed.optin;
                                                    tracking_state.optout = config_parsed.optout;

                                                    if tracking_rx.is_none() {
                                                        let (tx, rx) = channel::mpsc_bounded::<Frame>(256);
                                                        tracking_state.invalidation_tx = Some(tx.clone());
                                                        tracking_rx = Some(rx);

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
                                                    tracking_state = TrackingState::default();
                                                    tracking_table.lock().untrack_all(client_id);
                                                    tracking_rx = None;
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
                                let guard = db[selected_db].read();
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
                                let mut guard = db[selected_db].write();
                                guard.refresh_now();
                                let db_count = db.len();
                                for (resp_idx, disp_frame, is_write, aof_bytes) in dispatchable.drain(..) {
                                    if is_write {
                                        let rt = runtime_config.read().unwrap();
                                        if let Err(oom_frame) = try_evict_if_needed(&mut *guard, &rt) {
                                            responses[resp_idx] = oom_frame;
                                            continue;
                                        }
                                    }
                                    let (d_cmd, d_args) = extract_command(&disp_frame).unwrap();
                                    let result = dispatch(&mut *guard, d_cmd, d_args, &mut selected_db, db_count);
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
                                if pubsub_tx.is_none() {
                                    let (tx, rx) = channel::mpsc_bounded::<Frame>(256);
                                    subscriber_id = pubsub::next_subscriber_id();
                                    pubsub_tx = Some(tx);
                                    pubsub_rx = Some(rx);
                                }
                                let is_pattern = cmd.eq_ignore_ascii_case(b"PSUBSCRIBE");
                                for arg in cmd_args {
                                    if let Some(channel_or_pattern) = extract_bytes(arg) {
                                        // ACL channel permission check
                                        let deny = {
                                            let acl_guard = acl_table.read().unwrap();
                                            acl_guard.check_channel_permission(&current_user, channel_or_pattern.as_ref()).map(|r| r.to_string())
                                        };
                                        if let Some(deny_reason) = deny {
                                            let _ = framed.send(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)))).await;
                                            continue;
                                        }
                                        let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                        {
                                            let mut registry = pubsub_registry.lock();
                                            if is_pattern {
                                                registry.psubscribe(channel_or_pattern.clone(), sub);
                                            } else {
                                                registry.subscribe(channel_or_pattern.clone(), sub);
                                            }
                                        }
                                        subscription_count += 1;
                                        let response = if is_pattern {
                                            pubsub::psubscribe_response(&channel_or_pattern, subscription_count)
                                        } else {
                                            pubsub::subscribe_response(&channel_or_pattern, subscription_count)
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
                            if in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR MULTI calls can not be nested"),
                                ));
                            } else {
                                in_multi = true;
                                command_queue.clear();
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            }
                            continue;
                        }
                        // EXEC
                        if cmd.eq_ignore_ascii_case(b"EXEC") {
                            if !in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR EXEC without MULTI"),
                                ));
                            } else {
                                in_multi = false;
                                let (result, txn_aof_entries) = execute_transaction(
                                    &db,
                                    &command_queue,
                                    &watched_keys,
                                    &mut selected_db,
                                );
                                command_queue.clear();
                                watched_keys.clear();
                                responses.push(result);
                                aof_entries.extend(txn_aof_entries);
                            }
                            continue;
                        }
                        // DISCARD
                        if cmd.eq_ignore_ascii_case(b"DISCARD") {
                            if !in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR DISCARD without MULTI"),
                                ));
                            } else {
                                in_multi = false;
                                command_queue.clear();
                                watched_keys.clear();
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            }
                            continue;
                        }
                        // WATCH
                        if cmd.eq_ignore_ascii_case(b"WATCH") {
                            if in_multi {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR WATCH inside MULTI is not allowed"),
                                ));
                            } else if cmd_args.is_empty() {
                                responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR wrong number of arguments for 'watch' command"),
                                ));
                            } else {
                                let guard = db[selected_db].read();
                                for arg in cmd_args {
                                    if let Frame::BulkString(key) = arg {
                                        let version = guard.get_version(key);
                                        watched_keys.insert(key.clone(), version);
                                    }
                                }
                                // guard dropped here
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            }
                            continue;
                        }
                        // UNWATCH
                        if cmd.eq_ignore_ascii_case(b"UNWATCH") {
                            watched_keys.clear();
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            continue;
                        }

                        // === ACL permission check (NOPERM gate) ===
                        // Exempt commands (AUTH, HELLO, QUIT, ACL) already handled via continue above.
                        // All remaining commands must pass through the permission gate.
                        if let Some(deny_reason) = acl_table.read().unwrap().check_command_permission(
                            &current_user, cmd, cmd_args,
                        ) {
                            acl_log.push(crate::acl::AclLogEntry {
                                reason: "command".to_string(),
                                object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                username: current_user.clone(),
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
                        {
                            let is_write = metadata::is_write(cmd);
                            if let Some(deny_reason) = acl_table.read().unwrap().check_key_permission(
                                &current_user, cmd, cmd_args, is_write,
                            ) {
                                acl_log.push(crate::acl::AclLogEntry {
                                    reason: "command".to_string(),
                                    object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                    username: current_user.clone(),
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
                    }

                    // --- MULTI queue mode ---
                    if in_multi {
                        command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                        continue;
                    }

                    // --- Collect for phase 2 dispatch (needs db lock) ---
                    match extract_command(&frame) {
                        Some((cmd, _cmd_args)) => {
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
                            let guard = db[selected_db].read();
                            let now_ms = crate::storage::entry::current_time_ms();
                            let proto = framed.codec().protocol_version();
                            for j in run_start..i {
                                let (resp_idx, ref disp_frame, _, _) = dispatchable[j];
                                let (d_cmd, d_args) = extract_command(disp_frame).unwrap();
                                let result = dispatch_read(&*guard, d_cmd, d_args, now_ms, &mut selected_db, db_count);
                                let (response, quit) = match result {
                                    DispatchResult::Response(f) => (f, false),
                                    DispatchResult::Quit(f) => (f, true),
                                };
                                // Track key on read for client-side caching invalidation
                                if tracking_state.enabled && !tracking_state.bcast {
                                    if let Some(key) = d_args.first().and_then(|f| extract_bytes(f)) {
                                        tracking_table.lock().track_key(client_id, &key, tracking_state.noloop);
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
                            let mut guard = db[selected_db].write();
                            guard.refresh_now();
                            for j in run_start..i {
                                let (resp_idx, ref disp_frame, _, ref aof_bytes) = dispatchable[j];
                                let rt = runtime_config.read().unwrap();
                                if let Err(oom_frame) = try_evict_if_needed(&mut *guard, &rt) {
                                    responses[resp_idx] = oom_frame;
                                    continue;
                                }
                                drop(rt);
                                let (d_cmd, d_args) = extract_command(disp_frame).unwrap();
                                let result = dispatch(&mut *guard, d_cmd, d_args, &mut selected_db, db_count);
                                let (response, quit) = match result {
                                    DispatchResult::Response(f) => (f, false),
                                    DispatchResult::Quit(f) => (f, true),
                                };
                                if let Some(bytes) = aof_bytes {
                                    if !matches!(&response, Frame::Error(_)) {
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
                                }
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
                if let Some(ref mut rx) = tracking_rx {
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
    if subscriber_id != 0 {
        let mut registry = pubsub_registry.lock();
        registry.unsubscribe_all(subscriber_id);
        registry.punsubscribe_all(subscriber_id);
    }

    // Cleanup: remove tracking state on disconnect
    if tracking_state.enabled {
        tracking_table.lock().untrack_all(client_id);
    }
}


// ============================================================================
// Sharded connection handler (thread-per-core shared-nothing architecture)
// ============================================================================


/// Handle a single client connection on a sharded (thread-per-core) runtime.
///
/// Runs within a shard's single-threaded Tokio runtime. Has direct mutable access
/// to the shard's databases via `Rc<RefCell<Vec<Database>>>` (safe: cooperative
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
#[cfg(feature = "runtime-tokio")]
pub async fn handle_connection_sharded(
    stream: TcpStream,
    databases: Rc<RefCell<Vec<Database>>>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_registry: Rc<RefCell<PubSubRegistry>>,
    blocking_registry: Rc<RefCell<crate::blocking::BlockingRegistry>>,
    shutdown: CancellationToken,
    requirepass: Option<String>,
    aof_tx: Option<channel::MpscSender<AofMessage>>,
    tracking_table: Rc<RefCell<TrackingTable>>,
    client_id: u64,
    repl_state: Option<Arc<RwLock<crate::replication::state::ReplicationState>>>,
    cluster_state: Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    lua: std::rc::Rc<mlua::Lua>,
    script_cache: std::rc::Rc<std::cell::RefCell<crate::scripting::ScriptCache>>,
    config_port: u16,
    acl_table: Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: Arc<RwLock<RuntimeConfig>>,
    config: Arc<ServerConfig>,
    spsc_notifiers: Vec<std::sync::Arc<channel::Notify>>,
    snapshot_trigger_tx: channel::WatchSender<u64>,
    cached_clock: CachedClock,
) {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    handle_connection_sharded_inner(
        stream,
        peer_addr,
        databases,
        shard_id,
        num_shards,
        dispatch_tx,
        pubsub_registry,
        blocking_registry,
        shutdown,
        requirepass,
        aof_tx,
        tracking_table,
        client_id,
        repl_state,
        cluster_state,
        lua,
        script_cache,
        config_port,
        acl_table,
        runtime_config,
        config,
        spsc_notifiers,
        snapshot_trigger_tx,
        cached_clock,
    )
    .await;
}

/// Generic inner handler for sharded connections (Tokio runtime).
///
/// Works with any stream implementing `AsyncRead + AsyncWrite + Unpin`,
/// enabling both plain TCP (`TcpStream`) and TLS (`tokio_rustls::server::TlsStream<TcpStream>`).
#[cfg(feature = "runtime-tokio")]
pub async fn handle_connection_sharded_inner<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
>(
    stream: S,
    peer_addr: String,
    databases: Rc<RefCell<Vec<Database>>>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_registry: Rc<RefCell<PubSubRegistry>>,
    blocking_registry: Rc<RefCell<crate::blocking::BlockingRegistry>>,
    shutdown: CancellationToken,
    requirepass: Option<String>,
    aof_tx: Option<channel::MpscSender<AofMessage>>,
    tracking_table: Rc<RefCell<TrackingTable>>,
    client_id: u64,
    repl_state: Option<Arc<RwLock<crate::replication::state::ReplicationState>>>,
    cluster_state: Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    lua: std::rc::Rc<mlua::Lua>,
    script_cache: std::rc::Rc<std::cell::RefCell<crate::scripting::ScriptCache>>,
    config_port: u16,
    acl_table: Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: Arc<RwLock<RuntimeConfig>>,
    config: Arc<ServerConfig>,
    spsc_notifiers: Vec<std::sync::Arc<channel::Notify>>,
    snapshot_trigger_tx: channel::WatchSender<u64>,
    cached_clock: CachedClock,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Direct buffer I/O: bypass Framed/codec for the hot path.
    let mut stream = stream;
    let mut read_buf = BytesMut::with_capacity(8192);
    let mut write_buf = BytesMut::with_capacity(8192);
    let parse_config = crate::protocol::ParseConfig::default();
    let mut protocol_version: u8 = 2;
    let mut selected_db: usize = 0;
    let mut authenticated = requirepass.is_none();
    let mut current_user: String = "default".to_string();
    let acl_max_len = runtime_config
        .read()
        .map(|cfg| cfg.acllog_max_len)
        .unwrap_or(128);
    let mut acl_log = crate::acl::AclLog::new(acl_max_len);

    // Transaction (MULTI/EXEC) connection-local state
    let mut in_multi: bool = false;
    let mut command_queue: Vec<Frame> = Vec::new();

    // Client tracking state
    let mut tracking_state = TrackingState::default();
    let mut tracking_rx: Option<channel::MpscReceiver<Frame>> = None;

    // RESP3/HELLO connection-local state
    let mut client_name: Option<Bytes> = None;

    // Cluster ASKING flag: set by ASKING command, cleared unconditionally before routing check.
    let mut asking: bool = false;

    // Per-connection arena for batch processing temporaries.
    // 4KB initial capacity, grows on demand (rarely exceeds 16KB per batch).
    let mut arena = Bump::with_capacity(4096);

    let mut break_outer = false;
    loop {
        tokio::select! {
            result = stream.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) => break, // connection closed
                    Ok(_) => {}
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

                let mut responses: Vec<Frame> = Vec::with_capacity(batch.len());
                let mut should_quit = false;
                // Pipeline batch optimization: collect remote commands by target shard
                // to dispatch as a single PipelineBatch per shard instead of individual Execute messages.
                // Maps target_shard -> Vec<(response_index, Arc<Frame>, aof_bytes, cmd_name_bytes)>
                let mut remote_groups: HashMap<usize, Vec<(usize, std::sync::Arc<Frame>, Option<Bytes>, Vec<u8>)>> = HashMap::with_capacity(num_shards);

                for frame in batch {
                    // --- AUTH gate ---
                    if !authenticated {
                        match extract_command(&frame) {
                            Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"AUTH") => {
                                let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                                if let Some(uname) = opt_user {
                                    authenticated = true;
                                    current_user = uname;
                                } else {
                                    acl_log.push(crate::acl::AclLogEntry {
                                        reason: "auth".to_string(),
                                        object: "AUTH".to_string(),
                                        username: current_user.clone(),
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
                                    protocol_version,
                                    client_id,
                                    &acl_table,
                                    &mut authenticated,
                                );
                                if !matches!(&response, Frame::Error(_)) {
                                    protocol_version = new_proto;
                                }
                                if let Some(name) = new_name {
                                    client_name = Some(name);
                                }
                                if let Some(uname) = opt_user {
                                    current_user = uname;
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

                    // --- ASKING: set per-connection flag for next command ---
                    if cmd.eq_ignore_ascii_case(b"ASKING") {
                        asking = true;
                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        continue;
                    }

                    // --- CLUSTER subcommands ---
                    if cmd.eq_ignore_ascii_case(b"CLUSTER") {
                        if let Some(ref cs) = cluster_state {
                            let self_addr: std::net::SocketAddr =
                                format!("127.0.0.1:{}", config_port)
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
                            let mut dbs = databases.borrow_mut();
                            let db_count = dbs.len();
                            let db = &mut dbs[selected_db];
                            if cmd.eq_ignore_ascii_case(b"EVAL") {
                                crate::scripting::handle_eval(
                                    &lua,
                                    &script_cache,
                                    cmd_args,
                                    db,
                                    shard_id,
                                    num_shards,
                                    selected_db,
                                    db_count,
                                )
                            } else {
                                crate::scripting::handle_evalsha(
                                    &lua,
                                    &script_cache,
                                    cmd_args,
                                    db,
                                    shard_id,
                                    num_shards,
                                    selected_db,
                                    db_count,
                                )
                            }
                        };
                        responses.push(response);
                        continue;
                    }

                    // --- SCRIPT subcommands: LOAD, EXISTS, FLUSH ---
                    if cmd.eq_ignore_ascii_case(b"SCRIPT") {
                        let (response, fanout) = crate::scripting::handle_script_subcommand(&script_cache, cmd_args);
                        // SCRIPT LOAD: fan-out the script to all OTHER shards via SPSC mesh
                        if let Some((sha1, script_bytes)) = fanout {
                            let mut producers = dispatch_tx.borrow_mut();
                            for target in 0..num_shards {
                                if target == shard_id {
                                    continue;
                                }
                                let idx = ChannelMesh::target_index(shard_id, target);
                                let msg = ShardMessage::ScriptLoad {
                                    sha1: sha1.clone(),
                                    script: script_bytes.clone(),
                                };
                                if producers[idx].try_push(msg).is_ok() {
                                    spsc_notifiers[target].notify_one();
                                }
                            }
                            drop(producers);
                        }
                        responses.push(response);
                        continue;
                    }

                    // --- Cluster slot routing (pre-dispatch) ---
                    // Only active when cluster_enabled() is true AND cluster_state is Some.
                    // AtomicBool outer check avoids RwLock acquisition on non-cluster path.
                    if crate::cluster::cluster_enabled() {
                        if let Some(ref cs) = cluster_state {
                            // Capture and clear ASKING flag unconditionally before routing check
                            let was_asking = asking;
                            asking = false;

                            let maybe_key = extract_primary_key(cmd, cmd_args);
                            if let Some(key) = maybe_key {
                                let slot = crate::cluster::slots::slot_for_key(key);
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

                    // --- AUTH (already authenticated) ---
                    if cmd.eq_ignore_ascii_case(b"AUTH") {
                        let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                        if let Some(uname) = opt_user {
                            current_user = uname;
                        }
                        responses.push(response);
                        continue;
                    }

                    // --- HELLO (protocol negotiation, ACL-aware) ---
                    if cmd.eq_ignore_ascii_case(b"HELLO") {
                        let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                            cmd_args,
                            protocol_version,
                            client_id,
                            &acl_table,
                            &mut authenticated,
                        );
                        if !matches!(&response, Frame::Error(_)) {
                            protocol_version = new_proto;
                        }
                        if let Some(name) = new_name {
                            client_name = Some(name);
                        }
                        if let Some(uname) = opt_user {
                            current_user = uname;
                        }
                        responses.push(response);
                        continue;
                    }

                    // --- ACL command (intercepted at connection level) ---
                    if cmd.eq_ignore_ascii_case(b"ACL") {
                        let response = crate::command::acl::handle_acl(
                            cmd_args,
                            &acl_table,
                            &mut acl_log,
                            &current_user,
                            &peer_addr,
                            &runtime_config,
                        );
                        responses.push(response);
                        continue;
                    }

                    // --- CONFIG GET/SET ---
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
                                        let rs_clone = Arc::clone(rs);
                                        let cfg = crate::replication::replica::ReplicaTaskConfig {
                                            master_host: host,
                                            master_port: port,
                                            repl_state: rs_clone,
                                            num_shards,
                                            persistence_dir: None, // wired in Plan 04
                                            listening_port: 0,     // wired in Plan 04
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
                        let resp = crate::command::connection::replconf(cmd_args);
                        responses.push(resp);
                        continue;
                    }

                    // --- INFO (with replication section) ---
                    if cmd.eq_ignore_ascii_case(b"INFO") {
                        let dbs = databases.borrow();
                        let response_text = {
                            let resp_frame = conn_cmd::info_readonly(&dbs[selected_db], cmd_args);
                            match resp_frame {
                                Frame::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                                _ => String::new(),
                            }
                        };
                        drop(dbs);
                        let mut response_text = response_text;
                        // Append replication section if repl_state available
                        if let Some(ref rs) = repl_state {
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
                                        client_name = extract_bytes(&cmd_args[1]);
                                        responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                                    }
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"GETNAME") {
                                    responses.push(match &client_name {
                                        Some(name) => Frame::BulkString(name.clone()),
                                        None => Frame::Null,
                                    });
                                    continue;
                                }
                                if sub_bytes.eq_ignore_ascii_case(b"TRACKING") {
                                    match crate::command::client::parse_tracking_args(cmd_args) {
                                        Ok(config_parsed) => {
                                            if config_parsed.enable {
                                                tracking_state.enabled = true;
                                                tracking_state.bcast = config_parsed.bcast;
                                                tracking_state.noloop = config_parsed.noloop;
                                                tracking_state.optin = config_parsed.optin;
                                                tracking_state.optout = config_parsed.optout;

                                                if tracking_rx.is_none() {
                                                    let (tx, rx) = channel::mpsc_bounded::<Frame>(256);
                                                    tracking_state.invalidation_tx = Some(tx.clone());
                                                    tracking_rx = Some(rx);

                                                    let mut table = tracking_table.borrow_mut();
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
                                                tracking_state = TrackingState::default();
                                                tracking_table.borrow_mut().untrack_all(client_id);
                                                tracking_rx = None;
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

                    // --- MULTI ---
                    if cmd.eq_ignore_ascii_case(b"MULTI") {
                        if in_multi {
                            responses.push(Frame::Error(
                                Bytes::from_static(b"ERR MULTI calls can not be nested"),
                            ));
                        } else {
                            in_multi = true;
                            command_queue.clear();
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        }
                        continue;
                    }

                    // --- EXEC ---
                    if cmd.eq_ignore_ascii_case(b"EXEC") {
                        if !in_multi {
                            responses.push(Frame::Error(
                                Bytes::from_static(b"ERR EXEC without MULTI"),
                            ));
                        } else {
                            in_multi = false;
                            // Execute transaction locally (same-shard restriction)
                            let result = execute_transaction_sharded(
                                &databases,
                                &command_queue,
                                selected_db,
                                &cached_clock,
                            );
                            command_queue.clear();
                            responses.push(result);
                        }
                        continue;
                    }

                    // --- DISCARD ---
                    if cmd.eq_ignore_ascii_case(b"DISCARD") {
                        if !in_multi {
                            responses.push(Frame::Error(
                                Bytes::from_static(b"ERR DISCARD without MULTI"),
                            ));
                        } else {
                            in_multi = false;
                            command_queue.clear();
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        }
                        continue;
                    }

                    // --- BLOCKING COMMANDS (BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX) ---
                    if cmd.eq_ignore_ascii_case(b"BLPOP")
                        || cmd.eq_ignore_ascii_case(b"BRPOP")
                        || cmd.eq_ignore_ascii_case(b"BLMOVE")
                        || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
                        || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
                    {
                        // Inside MULTI: queue as non-blocking variant
                        if in_multi {
                            let nb_frame = convert_blocking_to_nonblocking(cmd, cmd_args);
                            command_queue.push(nb_frame);
                            responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                            continue;
                        }

                        // Flush accumulated responses before entering blocking mode.
                        write_buf.clear();
                        for response in responses.iter() {
                            if protocol_version >= 3 {
                                crate::protocol::serialize_resp3(response, &mut write_buf);
                            } else {
                                crate::protocol::serialize(response, &mut write_buf);
                            }
                        }
                        if stream.write_all(&write_buf).await.is_err() {
                            arena.reset();
                            return;
                        }

                        let blocking_response = handle_blocking_command(
                            cmd,
                            cmd_args,
                            selected_db,
                            &databases,
                            &blocking_registry,
                            shard_id,
                            num_shards,
                            &dispatch_tx,
                            &shutdown,
                        ).await;
                        let blocking_response = apply_resp3_conversion(cmd, blocking_response, protocol_version);

                        // Re-initialize responses for the blocking command's result
                        responses = Vec::with_capacity(1);
                        responses.push(blocking_response);
                        // Break out of batch -- blocking command ends the pipeline
                        break;
                    }

                    // --- PUBLISH: local delivery + cross-shard fan-out ---
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
                                    // Publish to local shard's subscribers
                                    let local_count = pubsub_registry.borrow_mut().publish(&ch, &msg);
                                    // Fan out to all other shards via SPSC
                                    let mut producers = dispatch_tx.borrow_mut();
                                    for target in 0..num_shards {
                                        if target == shard_id {
                                            continue;
                                        }
                                        let idx = ChannelMesh::target_index(shard_id, target);
                                        let fanout_msg = ShardMessage::PubSubFanOut {
                                            channel: ch.clone(),
                                            message: msg.clone(),
                                        };
                                        if producers[idx].try_push(fanout_msg).is_ok() {
                                            spsc_notifiers[target].notify_one();
                                        }
                                    }
                                    drop(producers);
                                    responses.push(Frame::Integer(local_count));
                                }
                                _ => responses.push(Frame::Error(
                                    Bytes::from_static(b"ERR invalid channel or message"),
                                )),
                            }
                        }
                        continue;
                    }

                    // --- SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE ---
                    // In the sharded path, subscriptions operate on the local shard's
                    // PubSubRegistry. No cross-shard needed for subscribe/unsubscribe
                    // because the subscriber's connection lives on this shard.
                    // TODO: Full subscriber mode support (Plan 06+ or future enhancement).
                    // For now, handle basic subscribe/unsubscribe responses.
                    if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                        // ACL channel permission check for Tokio sharded handler
                        for arg in cmd_args {
                            if let Some(channel) = extract_bytes(arg) {
                                let acl_guard = acl_table.read().unwrap();
                                if let Some(deny_reason) = acl_guard.check_channel_permission(&current_user, channel.as_ref()) {
                                    drop(acl_guard);
                                    responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                                    continue;
                                }
                            }
                        }
                        // Stub: subscriber mode in sharded handler is deferred
                        responses.push(Frame::Error(
                            Bytes::from_static(b"ERR SUBSCRIBE not yet supported in sharded mode"),
                        ));
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                        responses.push(Frame::Error(
                            Bytes::from_static(b"ERR UNSUBSCRIBE not yet supported in sharded mode"),
                        ));
                        continue;
                    }

                    // --- BGSAVE: trigger per-shard cooperative snapshot ---
                    if cmd.eq_ignore_ascii_case(b"BGSAVE") {
                        let response = crate::command::persistence::bgsave_start_sharded(
                            &snapshot_trigger_tx,
                            num_shards,
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

                    // === ACL permission check (NOPERM gate) ===
                    // Exempt commands (AUTH, HELLO, QUIT, ACL) already handled via continue above.
                    // Acquire the ACL read lock ONCE for both command and key permission checks.
                    {
                        let acl_guard = acl_table.read().unwrap();
                        if let Some(deny_reason) = acl_guard.check_command_permission(
                            &current_user, cmd, cmd_args,
                        ) {
                            drop(acl_guard);
                            acl_log.push(crate::acl::AclLogEntry {
                                reason: "command".to_string(),
                                object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                username: current_user.clone(),
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

                        // === ACL key pattern check (same lock guard) ===
                        let is_write_for_acl = metadata::is_write(cmd);
                        if let Some(deny_reason) = acl_guard.check_key_permission(
                            &current_user, cmd, cmd_args, is_write_for_acl,
                        ) {
                            drop(acl_guard);
                            acl_log.push(crate::acl::AclLogEntry {
                                reason: "command".to_string(),
                                object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                username: current_user.clone(),
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

                    // --- MULTI queue mode ---
                    if in_multi {
                        command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                        continue;
                    }

                    // --- Cross-shard aggregation commands: KEYS, SCAN, DBSIZE ---
                    if cmd.eq_ignore_ascii_case(b"KEYS") {
                        let response = crate::shard::coordinator::coordinate_keys(
                            cmd_args,
                            shard_id,
                            num_shards,
                            selected_db,
                            &databases,
                            &dispatch_tx,
                            &spsc_notifiers,
                            &cached_clock,
                        ).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"SCAN") {
                        let response = crate::shard::coordinator::coordinate_scan(
                            cmd_args,
                            shard_id,
                            num_shards,
                            selected_db,
                            &databases,
                            &dispatch_tx,
                            &spsc_notifiers,
                            &cached_clock,
                        ).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                        let response = crate::shard::coordinator::coordinate_dbsize(
                            shard_id,
                            num_shards,
                            selected_db,
                            &databases,
                            &dispatch_tx,
                            &spsc_notifiers,
                        ).await;
                        responses.push(response);
                        continue;
                    }

                    // --- Multi-key commands: VLL coordinator ---
                    if is_multi_key_command(cmd, cmd_args) {
                        let response = crate::shard::coordinator::coordinate_multi_key(
                            cmd,
                            cmd_args,
                            shard_id,
                            num_shards,
                            selected_db,
                            &databases,
                            &dispatch_tx,
                            &spsc_notifiers,
                            &cached_clock,
                        ).await;
                        responses.push(response);
                        continue;
                    }

                    // --- Routing: keyless, local, or remote ---
                    let target_shard = extract_primary_key(cmd, cmd_args)
                        .map(|key| key_to_shard(key, num_shards));

                    let is_local = match target_shard {
                        None => true,
                        Some(s) if s == shard_id => true,
                        _ => false,
                    };

                    // Pre-serialize write commands for AOF logging (before dispatch).
                    // Skip the metadata::is_write lookup entirely when neither
                    // AOF persistence nor client tracking needs the write classification.
                    let is_write = if aof_tx.is_some() || tracking_state.enabled {
                        metadata::is_write(cmd)
                    } else {
                        false
                    };
                    let aof_bytes = if is_write && aof_tx.is_some() {
                        Some(aof::serialize_command(&frame))
                    } else {
                        None
                    };

                    if is_local {
                        // LOCAL FAST PATH: zero cross-shard overhead
                        // Eviction check before write dispatch
                        if metadata::is_write(cmd) {
                            let rt = runtime_config.read().unwrap();
                            let mut dbs_ev = databases.borrow_mut();
                            if let Err(oom_frame) = try_evict_if_needed(&mut dbs_ev[selected_db], &rt) {
                                drop(dbs_ev);
                                responses.push(oom_frame);
                                continue;
                            }
                            drop(dbs_ev);
                        }
                        let mut dbs = databases.borrow_mut();
                        let db_count = dbs.len();
                        dbs[selected_db].refresh_now_from_cache(&cached_clock);
                        let result = dispatch(
                            &mut dbs[selected_db],
                            cmd,
                            cmd_args,
                            &mut selected_db,
                            db_count,
                        );
                        let response = match result {
                            DispatchResult::Response(f) => f,
                            DispatchResult::Quit(f) => {
                                should_quit = true;
                                f
                            }
                        };
                        // Post-dispatch wakeup hooks for producer commands
                        if !matches!(response, Frame::Error(_)) {
                            let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                                || cmd.eq_ignore_ascii_case(b"ZADD");
                            if needs_wake {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    let mut reg = blocking_registry.borrow_mut();
                                    if cmd.eq_ignore_ascii_case(b"LPUSH")
                                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                                    {
                                        crate::blocking::wakeup::try_wake_list_waiter(
                                            &mut reg, &mut dbs[selected_db], selected_db, &key,
                                        );
                                    } else {
                                        crate::blocking::wakeup::try_wake_zset_waiter(
                                            &mut reg, &mut dbs[selected_db], selected_db, &key,
                                        );
                                    }
                                }
                            }
                        }
                        drop(dbs); // drop before any subsequent await
                        // Log successful write commands to AOF
                        if let Some(bytes) = aof_bytes {
                            if !matches!(response, Frame::Error(_)) {
                                if let Some(ref tx) = aof_tx {
                                    let _ = tx.try_send(AofMessage::Append(bytes));
                                }
                            }
                        }
                        // Track key on read / invalidate tracked key on write
                        if tracking_state.enabled {
                            if is_write {
                                if !matches!(response, Frame::Error(_)) {
                                    if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                        let senders = tracking_table.borrow_mut().invalidate_key(&key, client_id);
                                        if !senders.is_empty() {
                                            let push = crate::tracking::invalidation::invalidation_push(&[key]);
                                            for tx in senders {
                                                let _ = tx.try_send(push.clone());
                                            }
                                        }
                                    }
                                }
                            } else if !tracking_state.bcast {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    tracking_table.borrow_mut().track_key(client_id, &key, tracking_state.noloop);
                                }
                            }
                        }
                        // Apply RESP3 response conversion if needed
                        let response = apply_resp3_conversion(cmd, response, protocol_version);
                        responses.push(response);
                    } else if let Some(target) = target_shard {
                        // DEFERRED REMOTE: collect for batch dispatch after loop
                        let resp_idx = responses.len();
                        responses.push(Frame::Null); // placeholder, filled after batch dispatch
                        let cmd_name = cmd.to_vec();
                        remote_groups
                            .entry(target)
                            .or_default()
                            .push((resp_idx, std::sync::Arc::new(frame), aof_bytes, cmd_name));
                    }
                }

                // Phase 2: Dispatch all deferred remote commands as batched PipelineBatch
                // messages (one per target shard), await all in parallel.
                if !remote_groups.is_empty() {
                    let mut reply_futures: Vec<(
                        Vec<(usize, Option<Bytes>, Vec<u8>)>, // (resp_idx, aof_bytes, cmd_name)
                        channel::OneshotReceiver<Vec<Frame>>,
                    )> = Vec::with_capacity(remote_groups.len());

                    for (target, entries) in remote_groups {
                        let (reply_tx, reply_rx) = channel::oneshot();
                        let (meta, commands): (Vec<(usize, Option<Bytes>, Vec<u8>)>, Vec<std::sync::Arc<Frame>>) =
                            entries.into_iter().map(|(idx, arc_frame, aof, cmd)| ((idx, aof, cmd), arc_frame)).unzip();

                        let msg = ShardMessage::PipelineBatch {
                            db_index: selected_db,
                            commands,
                            reply_tx,
                        };
                        let target_idx = ChannelMesh::target_index(shard_id, target);
                        {
                            let mut pending = msg;
                            loop {
                                let push_result = {
                                    let mut producers = dispatch_tx.borrow_mut();
                                    producers[target_idx].try_push(pending)
                                };
                                match push_result {
                                    Ok(()) => {
                                        spsc_notifiers[target].notify_one();
                                        break;
                                    }
                                    Err(val) => {
                                        pending = val;
                                        tokio::task::yield_now().await;
                                    }
                                }
                            }
                        }
                        reply_futures.push((meta, reply_rx));
                    }

                    // Await all shard responses (they execute in parallel on different shards)
                    let proto_ver = protocol_version;
                    for (meta, reply_rx) in reply_futures {
                        match reply_rx.await {
                            Ok(shard_responses) => {
                                for ((resp_idx, aof_bytes, cmd_name), resp) in meta.into_iter().zip(shard_responses) {
                                    // AOF logging for successful remote writes
                                    if let Some(bytes) = aof_bytes {
                                        if !matches!(resp, Frame::Error(_)) {
                                            if let Some(ref tx) = aof_tx {
                                                let _ = tx.try_send(AofMessage::Append(bytes));
                                            }
                                        }
                                    }
                                    let resp = apply_resp3_conversion(&cmd_name, resp, proto_ver);
                                    responses[resp_idx] = resp;
                                }
                            }
                            Err(_) => {
                                for (resp_idx, _, _) in meta {
                                    responses[resp_idx] = Frame::Error(
                                        Bytes::from_static(b"ERR cross-shard dispatch failed"),
                                    );
                                }
                            }
                        }
                    }
                }

                arena.reset(); // O(1) bulk deallocation of batch temporaries

                // Serialize all responses into write_buf, then do ONE write_all syscall.
                write_buf.clear();
                for response in &responses {
                    if protocol_version >= 3 {
                        crate::protocol::serialize_resp3(response, &mut write_buf);
                    } else {
                        crate::protocol::serialize(response, &mut write_buf);
                    }
                }
                if stream.write_all(&write_buf).await.is_err() {
                    return;
                }

                // Shrink write_buf if it grew beyond 64KB (e.g., large pipeline response)
                // to prevent permanent per-connection memory bloat.
                if write_buf.capacity() > 65536 {
                    write_buf = BytesMut::with_capacity(8192);
                }

                // Shrink read_buf if it grew beyond 64KB (e.g., huge pipeline request)
                // to reclaim memory after occasional large bursts.
                if read_buf.capacity() > 65536 {
                    // Preserve any unparsed trailing data
                    let remaining = read_buf.split();
                    read_buf = BytesMut::with_capacity(8192);
                    if !remaining.is_empty() {
                        read_buf.extend_from_slice(&remaining);
                    }
                }

                if should_quit {
                    break;
                }
            }
            _ = shutdown.cancelled() => {
                write_buf.clear();
                let shutdown_err = Frame::Error(
                    Bytes::from_static(b"ERR server shutting down")
                );
                if protocol_version >= 3 {
                    crate::protocol::serialize_resp3(&shutdown_err, &mut write_buf);
                } else {
                    crate::protocol::serialize(&shutdown_err, &mut write_buf);
                }
                let _ = stream.write_all(&write_buf).await;
                break;
            }
        }
    }

    // Cleanup: remove tracking state on disconnect
    if tracking_state.enabled {
        tracking_table.borrow_mut().untrack_all(client_id);
    }
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
pub async fn handle_connection_sharded_monoio<
    S: monoio::io::AsyncReadRent + monoio::io::AsyncWriteRent,
>(
    mut stream: S,
    peer_addr: String,
    databases: Rc<RefCell<Vec<Database>>>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_registry: Rc<RefCell<PubSubRegistry>>,
    blocking_registry: Rc<RefCell<crate::blocking::BlockingRegistry>>,
    shutdown: CancellationToken,
    requirepass: Option<String>,
    aof_tx: Option<channel::MpscSender<AofMessage>>,
    tracking_table: Rc<RefCell<TrackingTable>>,
    client_id: u64,
    repl_state: Option<Arc<RwLock<crate::replication::state::ReplicationState>>>,
    cluster_state: Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    lua: Rc<mlua::Lua>,
    script_cache: Rc<RefCell<crate::scripting::ScriptCache>>,
    config_port: u16,
    acl_table: Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: Arc<RwLock<RuntimeConfig>>,
    config: Arc<ServerConfig>,
    spsc_notifiers: Vec<Arc<channel::Notify>>,
    snapshot_trigger_tx: channel::WatchSender<u64>,
    cached_clock: CachedClock,
) {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};

    let mut read_buf = BytesMut::with_capacity(8192);
    let mut write_buf = BytesMut::with_capacity(8192);
    let mut codec = RespCodec::default();
    let mut selected_db: usize = 0;
    let db_count = databases.borrow().len();

    // Connection-level state (mirroring Tokio handler)
    let mut protocol_version: u8 = 2;
    let mut authenticated = requirepass.is_none();
    let mut current_user: String = "default".to_string();
    let acl_max_len = runtime_config
        .read()
        .map(|cfg| cfg.acllog_max_len)
        .unwrap_or(128);
    let mut acl_log = crate::acl::AclLog::new(acl_max_len);
    let mut tracking_state = TrackingState::default();
    let mut tracking_rx: Option<channel::MpscReceiver<Frame>> = None;
    let mut client_name: Option<Bytes> = None;
    let mut asking: bool = false;

    // Pub/Sub connection-local state
    let mut subscription_count: usize = 0;
    let mut subscriber_id: u64 = 0;
    let mut pubsub_tx: Option<channel::MpscSender<Frame>> = None;
    let mut pubsub_rx: Option<channel::MpscReceiver<Frame>> = None;

    // Transaction (MULTI/EXEC) connection-local state
    let mut in_multi: bool = false;
    let mut command_queue: Vec<Frame> = Vec::new();

    // Pre-allocate read buffer outside the loop to avoid per-read heap allocation.
    // Monoio's ownership I/O takes ownership and returns the buffer, so we reassign.
    let mut tmp_buf = vec![0u8; 8192];

    // Pre-allocate batch containers outside the loop to avoid per-batch heap allocation.
    // These are cleared and reused each iteration instead of being recreated.
    let mut responses: Vec<Frame> = Vec::with_capacity(64);
    let mut remote_groups: HashMap<
        usize,
        Vec<(usize, std::sync::Arc<Frame>, Option<Bytes>, Vec<u8>)>,
    > = HashMap::with_capacity(num_shards);
    let mut reply_futures: Vec<(
        Vec<(usize, Option<Bytes>, Vec<u8>)>,
        channel::OneshotReceiver<Vec<Frame>>,
    )> = Vec::with_capacity(num_shards);

    // Pre-allocate frames Vec outside the loop; reused via .clear() each iteration.
    let mut frames: Vec<Frame> = Vec::with_capacity(64);

    loop {
        // Subscriber mode: bidirectional select on client commands + published messages
        if subscription_count > 0 {
            let rx = pubsub_rx.as_ref().unwrap();
            let sub_tmp_buf = vec![0u8; 8192];
            monoio::select! {
                read_result = stream.read(sub_tmp_buf) => {
                    let (result, buf) = read_result;
                    match result {
                        Ok(0) => break, // connection closed
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
                                                        if wr.is_err() { break; }
                                                        continue;
                                                    }
                                                    for arg in cmd_args {
                                                        if let Some(channel) = extract_bytes(arg) {
                                                            // ACL channel permission check
                                                            let denied = {
                                                                let acl_guard = acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&current_user, channel.as_ref())
                                                            };
                                                            if let Some(deny_reason) = denied {
                                                                let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&err, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { break; }
                                                                continue;
                                                            }
                                                            let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                            pubsub_registry.borrow_mut().subscribe(channel.clone(), sub);
                                                            subscription_count += 1;
                                                            let resp = pubsub::subscribe_response(&channel, subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { break; }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let removed = pubsub_registry.borrow_mut().unsubscribe_all(subscriber_id);
                                                        if removed.is_empty() {
                                                            subscription_count = pubsub_registry.borrow().total_subscription_count(subscriber_id);
                                                            let resp = pubsub::unsubscribe_response(&Bytes::from_static(b""), subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { break; }
                                                        } else {
                                                            for ch in &removed {
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::unsubscribe_response(ch, subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { break; }
                                                            }
                                                        }
                                                    } else {
                                                        for arg in cmd_args {
                                                            if let Some(channel) = extract_bytes(arg) {
                                                                pubsub_registry.borrow_mut().unsubscribe(channel.as_ref(), subscriber_id);
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::unsubscribe_response(&channel, subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { break; }
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
                                                        if wr.is_err() { break; }
                                                        continue;
                                                    }
                                                    for arg in cmd_args {
                                                        if let Some(pattern) = extract_bytes(arg) {
                                                            // ACL channel permission check
                                                            let denied = {
                                                                let acl_guard = acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&current_user, pattern.as_ref())
                                                            };
                                                            if let Some(deny_reason) = denied {
                                                                let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&err, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { break; }
                                                                continue;
                                                            }
                                                            let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                            pubsub_registry.borrow_mut().psubscribe(pattern.clone(), sub);
                                                            subscription_count += 1;
                                                            let resp = pubsub::psubscribe_response(&pattern, subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { break; }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let removed = pubsub_registry.borrow_mut().punsubscribe_all(subscriber_id);
                                                        if removed.is_empty() {
                                                            subscription_count = pubsub_registry.borrow().total_subscription_count(subscriber_id);
                                                            let resp = pubsub::punsubscribe_response(&Bytes::from_static(b""), subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { break; }
                                                        } else {
                                                            for pat in &removed {
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::punsubscribe_response(pat, subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { break; }
                                                            }
                                                        }
                                                    } else {
                                                        for arg in cmd_args {
                                                            if let Some(pattern) = extract_bytes(arg) {
                                                                pubsub_registry.borrow_mut().punsubscribe(pattern.as_ref(), subscriber_id);
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::punsubscribe_response(&pattern, subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { break; }
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
                                                    if wr.is_err() { break; }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"QUIT") => {
                                                    let resp = Frame::SimpleString(Bytes::from_static(b"OK"));
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&resp, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    let _ = wr; // ignore write error on quit
                                                    return; // exit connection
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
                                                    if wr.is_err() { break; }
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => break, // need more data
                                    Err(_) => return,  // parse error
                                }
                            }
                        }
                        Err(_) => break, // connection error
                    }
                }
                msg = rx.recv_async() => {
                    match msg {
                        Ok(frame) => {
                            let mut resp_buf = BytesMut::new();
                            codec.encode_frame(&frame, &mut resp_buf);
                            let data = resp_buf.freeze();
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
        let (result, returned_buf) = stream.read(tmp_buf).await;
        tmp_buf = returned_buf;
        match result {
            Ok(0) => break, // connection closed
            Ok(n) => {
                read_buf.extend_from_slice(&tmp_buf[..n]);
            }
            Err(_) => break,
        }

        // Inline dispatch: for single-shard mode, handle GET/SET directly from raw
        // bytes without Frame construction or dispatch table lookup.
        // Skip inline dispatch when not authenticated — AUTH must go through normal path.
        if num_shards == 1 && authenticated {
            // Refresh time once before inline dispatch (same as batch refresh below)
            {
                let mut dbs = databases.borrow_mut();
                dbs[selected_db].refresh_now_from_cache(&cached_clock);
            }
            let inlined = try_inline_dispatch_loop(
                &mut read_buf,
                &mut write_buf,
                &databases,
                selected_db,
                &aof_tx,
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
                Err(_) => return, // parse error, close connection
            }
        }

        if frames.is_empty() {
            continue;
        }

        // Process frames with shard routing, cross-shard dispatch, and AOF logging
        // Note: do NOT clear write_buf -- it may contain responses from inline dispatch.
        // The inline path appends directly; the normal path appends via encode_frame below.
        // write_buf is cleared via .split().freeze() at the flush point each iteration.
        let mut should_quit = false;

        // Pipeline batch optimization: reuse pre-allocated containers (clear, not re-create).
        responses.clear();
        remote_groups.clear();

        // Refresh time once per batch — sub-millisecond accuracy not needed per-command.
        {
            let mut dbs = databases.borrow_mut();
            dbs[selected_db].refresh_now_from_cache(&cached_clock);
        }

        for frame in frames.drain(..) {
            // --- AUTH gate ---
            if !authenticated {
                match extract_command(&frame) {
                    Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"AUTH") => {
                        let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                        if let Some(uname) = opt_user {
                            authenticated = true;
                            current_user = uname;
                        } else {
                            acl_log.push(crate::acl::AclLogEntry {
                                reason: "auth".to_string(),
                                object: "AUTH".to_string(),
                                username: current_user.clone(),
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
                            protocol_version,
                            client_id,
                            &acl_table,
                            &mut authenticated,
                        );
                        if !matches!(&response, Frame::Error(_)) {
                            protocol_version = new_proto;
                        }
                        if let Some(name) = new_name {
                            client_name = Some(name);
                        }
                        if let Some(uname) = opt_user {
                            current_user = uname;
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
                asking = true;
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                continue;
            }

            // --- CLUSTER subcommands ---
            if cmd.eq_ignore_ascii_case(b"CLUSTER") {
                if let Some(ref cs) = cluster_state {
                    let self_addr: std::net::SocketAddr = format!("127.0.0.1:{}", config_port)
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
                    let mut dbs = databases.borrow_mut();
                    let db_count = dbs.len();
                    let db = &mut dbs[selected_db];
                    crate::scripting::handle_evalsha(
                        &lua,
                        &script_cache,
                        cmd_args,
                        db,
                        shard_id,
                        num_shards,
                        selected_db,
                        db_count,
                    )
                };
                responses.push(response);
                continue;
            }

            // --- Lua scripting: EVAL ---
            if cmd.eq_ignore_ascii_case(b"EVAL") {
                let response = {
                    let mut dbs = databases.borrow_mut();
                    let db_count = dbs.len();
                    let db = &mut dbs[selected_db];
                    crate::scripting::handle_eval(
                        &lua,
                        &script_cache,
                        cmd_args,
                        db,
                        shard_id,
                        num_shards,
                        selected_db,
                        db_count,
                    )
                };
                responses.push(response);
                continue;
            }

            // --- SCRIPT subcommands: LOAD, EXISTS, FLUSH ---
            if cmd.eq_ignore_ascii_case(b"SCRIPT") {
                let (response, fanout) =
                    crate::scripting::handle_script_subcommand(&script_cache, cmd_args);
                if let Some((sha1, script_bytes)) = fanout {
                    let mut producers = dispatch_tx.borrow_mut();
                    for target in 0..num_shards {
                        if target == shard_id {
                            continue;
                        }
                        let idx = ChannelMesh::target_index(shard_id, target);
                        let msg = ShardMessage::ScriptLoad {
                            sha1: sha1.clone(),
                            script: script_bytes.clone(),
                        };
                        if producers[idx].try_push(msg).is_ok() {
                            spsc_notifiers[target].notify_one();
                        }
                    }
                    drop(producers);
                }
                responses.push(response);
                continue;
            }

            // --- Cluster slot routing (pre-dispatch) ---
            if crate::cluster::cluster_enabled() {
                if let Some(ref cs) = cluster_state {
                    let was_asking = asking;
                    asking = false;

                    let maybe_key = extract_primary_key(cmd, cmd_args);
                    if let Some(key) = maybe_key {
                        let slot = crate::cluster::slots::slot_for_key(key);
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

            // --- AUTH (already authenticated) ---
            if cmd.eq_ignore_ascii_case(b"AUTH") {
                let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                if let Some(uname) = opt_user {
                    current_user = uname;
                }
                responses.push(response);
                continue;
            }

            // --- HELLO (protocol negotiation, ACL-aware) ---
            if cmd.eq_ignore_ascii_case(b"HELLO") {
                let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                    cmd_args,
                    protocol_version,
                    client_id,
                    &acl_table,
                    &mut authenticated,
                );
                if !matches!(&response, Frame::Error(_)) {
                    protocol_version = new_proto;
                }
                if let Some(name) = new_name {
                    client_name = Some(name);
                }
                if let Some(uname) = opt_user {
                    current_user = uname;
                }
                responses.push(response);
                continue;
            }

            // --- ACL command (intercepted at connection level) ---
            if cmd.eq_ignore_ascii_case(b"ACL") {
                let response = crate::command::acl::handle_acl(
                    cmd_args,
                    &acl_table,
                    &mut acl_log,
                    &current_user,
                    &peer_addr,
                    &runtime_config,
                );
                responses.push(response);
                continue;
            }

            // --- CONFIG GET/SET ---
            if cmd.eq_ignore_ascii_case(b"CONFIG") {
                responses.push(handle_config(cmd_args, &runtime_config, &config));
                continue;
            }

            // --- REPLICAOF / SLAVEOF ---
            if cmd.eq_ignore_ascii_case(b"REPLICAOF") || cmd.eq_ignore_ascii_case(b"SLAVEOF") {
                use crate::command::connection::{ReplicaofAction, replicaof};
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
                                let rs_clone = Arc::clone(rs);
                                let cfg = crate::replication::replica::ReplicaTaskConfig {
                                    master_host: host,
                                    master_port: port,
                                    repl_state: rs_clone,
                                    num_shards,
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
                let dbs = databases.borrow();
                let response_text = {
                    let resp_frame = conn_cmd::info_readonly(&dbs[selected_db], cmd_args);
                    match resp_frame {
                        Frame::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                        _ => String::new(),
                    }
                };
                drop(dbs);
                let mut response_text = response_text;
                if let Some(ref rs) = repl_state {
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
                                client_name = extract_bytes(&cmd_args[1]);
                                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                            }
                            continue;
                        }
                        if sub_bytes.eq_ignore_ascii_case(b"GETNAME") {
                            responses.push(match &client_name {
                                Some(name) => Frame::BulkString(name.clone()),
                                None => Frame::Null,
                            });
                            continue;
                        }
                        if sub_bytes.eq_ignore_ascii_case(b"TRACKING") {
                            match crate::command::client::parse_tracking_args(cmd_args) {
                                Ok(config_parsed) => {
                                    if config_parsed.enable {
                                        tracking_state.enabled = true;
                                        tracking_state.bcast = config_parsed.bcast;
                                        tracking_state.noloop = config_parsed.noloop;
                                        tracking_state.optin = config_parsed.optin;
                                        tracking_state.optout = config_parsed.optout;

                                        if tracking_rx.is_none() {
                                            let (tx, rx) = channel::mpsc_bounded::<Frame>(256);
                                            tracking_state.invalidation_tx = Some(tx.clone());
                                            tracking_rx = Some(rx);

                                            let mut table = tracking_table.borrow_mut();
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
                                        tracking_state = TrackingState::default();
                                        tracking_table.borrow_mut().untrack_all(client_id);
                                        tracking_rx = None;
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

            // --- PUBLISH: local delivery + cross-shard fan-out ---
            if cmd.eq_ignore_ascii_case(b"PUBLISH") {
                if cmd_args.len() != 2 {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR wrong number of arguments for 'publish' command",
                    )));
                } else {
                    let channel = extract_bytes(&cmd_args[0]);
                    let message = extract_bytes(&cmd_args[1]);
                    match (channel, message) {
                        (Some(ch), Some(msg)) => {
                            let local_count = pubsub_registry.borrow_mut().publish(&ch, &msg);
                            let mut producers = dispatch_tx.borrow_mut();
                            for target in 0..num_shards {
                                if target == shard_id {
                                    continue;
                                }
                                let idx = ChannelMesh::target_index(shard_id, target);
                                let fanout_msg = ShardMessage::PubSubFanOut {
                                    channel: ch.clone(),
                                    message: msg.clone(),
                                };
                                if producers[idx].try_push(fanout_msg).is_ok() {
                                    spsc_notifiers[target].notify_one();
                                }
                            }
                            drop(producers);
                            responses.push(Frame::Integer(local_count));
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
                if pubsub_tx.is_none() {
                    let (tx, rx) = channel::mpsc_bounded(256);
                    pubsub_tx = Some(tx);
                    pubsub_rx = Some(rx);
                }
                if subscriber_id == 0 {
                    subscriber_id = crate::pubsub::next_subscriber_id();
                }
                // Flush accumulated responses before entering subscriber mode
                for resp in &responses {
                    codec.encode_frame(resp, &mut write_buf);
                }
                for arg in cmd_args {
                    if let Some(ch) = extract_bytes(arg) {
                        // ACL channel permission check
                        {
                            let acl_guard = acl_table.read().unwrap();
                            if let Some(deny_reason) =
                                acl_guard.check_channel_permission(&current_user, ch.as_ref())
                            {
                                drop(acl_guard);
                                let err =
                                    Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                codec.encode_frame(&err, &mut write_buf);
                                continue;
                            }
                        }
                        let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                        if is_pattern {
                            pubsub_registry.borrow_mut().psubscribe(ch.clone(), sub);
                        } else {
                            pubsub_registry.borrow_mut().subscribe(ch.clone(), sub);
                        }
                        subscription_count += 1;
                        let resp = if is_pattern {
                            pubsub::psubscribe_response(&ch, subscription_count)
                        } else {
                            pubsub::subscribe_response(&ch, subscription_count)
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
                        return;
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

            // --- BGSAVE: trigger per-shard cooperative snapshot ---
            if cmd.eq_ignore_ascii_case(b"BGSAVE") {
                let response = crate::command::persistence::bgsave_start_sharded(
                    &snapshot_trigger_tx,
                    num_shards,
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

            // === ACL permission check (NOPERM gate) ===
            // Exempt commands (AUTH, HELLO, QUIT, ACL) already handled above.
            {
                let acl_guard = acl_table.read().unwrap();
                if let Some(deny_reason) =
                    acl_guard.check_command_permission(&current_user, cmd, cmd_args)
                {
                    drop(acl_guard);
                    acl_log.push(crate::acl::AclLogEntry {
                        reason: "command".to_string(),
                        object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                        username: current_user.clone(),
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
                if let Some(deny_reason) =
                    acl_guard.check_key_permission(&current_user, cmd, cmd_args, is_write_for_acl)
                {
                    drop(acl_guard);
                    acl_log.push(crate::acl::AclLogEntry {
                        reason: "command".to_string(),
                        object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                        username: current_user.clone(),
                        client_addr: peer_addr.clone(),
                        timestamp_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    });
                    responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                    continue;
                }
            }

            // --- MULTI ---
            if cmd.eq_ignore_ascii_case(b"MULTI") {
                if in_multi {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR MULTI calls can not be nested",
                    )));
                } else {
                    in_multi = true;
                    command_queue.clear();
                    responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                }
                continue;
            }

            // --- EXEC ---
            if cmd.eq_ignore_ascii_case(b"EXEC") {
                if !in_multi {
                    responses.push(Frame::Error(Bytes::from_static(b"ERR EXEC without MULTI")));
                } else {
                    in_multi = false;
                    let result = execute_transaction_sharded(
                        &databases,
                        &command_queue,
                        selected_db,
                        &cached_clock,
                    );
                    command_queue.clear();
                    responses.push(result);
                }
                continue;
            }

            // --- DISCARD ---
            if cmd.eq_ignore_ascii_case(b"DISCARD") {
                if !in_multi {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR DISCARD without MULTI",
                    )));
                } else {
                    in_multi = false;
                    command_queue.clear();
                    responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                }
                continue;
            }

            // --- BLOCKING COMMANDS (BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX) ---
            if cmd.eq_ignore_ascii_case(b"BLPOP")
                || cmd.eq_ignore_ascii_case(b"BRPOP")
                || cmd.eq_ignore_ascii_case(b"BLMOVE")
                || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
                || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
            {
                // Inside MULTI: queue as non-blocking variant
                if in_multi {
                    let nb_frame = convert_blocking_to_nonblocking(cmd, cmd_args);
                    command_queue.push(nb_frame);
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
                        return;
                    }
                }

                let blocking_response = handle_blocking_command_monoio(
                    cmd,
                    cmd_args,
                    selected_db,
                    &databases,
                    &blocking_registry,
                    shard_id,
                    num_shards,
                    &dispatch_tx,
                    &shutdown,
                    &spsc_notifiers,
                )
                .await;

                // Encode blocking response directly
                codec.encode_frame(&blocking_response, &mut write_buf);
                responses.clear();
                break; // Blocking command ends the pipeline batch
            }

            // --- MULTI queue mode: queue commands when in transaction ---
            if in_multi {
                command_queue.push(frame);
                responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                continue;
            }

            // --- Cross-shard aggregation commands: KEYS, SCAN, DBSIZE ---
            if num_shards > 1 {
                if cmd.eq_ignore_ascii_case(b"KEYS") {
                    let response = crate::shard::coordinator::coordinate_keys(
                        cmd_args,
                        shard_id,
                        num_shards,
                        selected_db,
                        &databases,
                        &dispatch_tx,
                        &spsc_notifiers,
                        &cached_clock,
                    )
                    .await;
                    responses.push(response);
                    continue;
                }
                if cmd.eq_ignore_ascii_case(b"SCAN") {
                    let response = crate::shard::coordinator::coordinate_scan(
                        cmd_args,
                        shard_id,
                        num_shards,
                        selected_db,
                        &databases,
                        &dispatch_tx,
                        &spsc_notifiers,
                        &cached_clock,
                    )
                    .await;
                    responses.push(response);
                    continue;
                }
                if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                    let response = crate::shard::coordinator::coordinate_dbsize(
                        shard_id,
                        num_shards,
                        selected_db,
                        &databases,
                        &dispatch_tx,
                        &spsc_notifiers,
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
                        shard_id,
                        num_shards,
                        selected_db,
                        &databases,
                        &dispatch_tx,
                        &spsc_notifiers,
                        &cached_clock,
                    )
                    .await;
                    responses.push(response);
                    continue;
                }
            }

            // --- Routing: keyless, local, or remote ---
            let target_shard =
                extract_primary_key(cmd, cmd_args).map(|key| key_to_shard(key, num_shards));

            let is_local = match target_shard {
                None => true,
                Some(s) if s == shard_id => true,
                _ => false,
            };

            // Pre-classify write commands for AOF + tracking
            let is_write = if aof_tx.is_some() || tracking_state.enabled {
                metadata::is_write(cmd)
            } else {
                false
            };

            if is_local {
                // LOCAL FAST PATH: single borrow_mut covers dispatch + wakeup (no per-command acquire/release)
                // Eviction check before write dispatch
                if metadata::is_write(cmd) {
                    let rt = runtime_config.read().unwrap();
                    let mut dbs_ev = databases.borrow_mut();
                    if let Err(oom_frame) = try_evict_if_needed(&mut dbs_ev[selected_db], &rt) {
                        drop(dbs_ev);
                        responses.push(oom_frame);
                        continue;
                    }
                    drop(dbs_ev);
                }
                let mut dbs = databases.borrow_mut();
                // No refresh_now here — called once per batch before the loop
                let result = dispatch(
                    &mut dbs[selected_db],
                    cmd,
                    cmd_args,
                    &mut selected_db,
                    db_count,
                );

                let response = match result {
                    DispatchResult::Response(f) => f,
                    DispatchResult::Quit(f) => {
                        should_quit = true;
                        f
                    }
                };

                // AOF logging for successful local writes
                if !matches!(response, Frame::Error(_)) && is_write {
                    if let Some(ref tx) = aof_tx {
                        let serialized = aof::serialize_command(&frame);
                        let _ = tx.try_send(AofMessage::Append(serialized));
                    }
                }

                // Post-dispatch wakeup hooks for producer commands (reuse outer dbs — no re-borrow)
                if !matches!(response, Frame::Error(_)) {
                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD");
                    if needs_wake {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg,
                                    &mut dbs[selected_db],
                                    selected_db,
                                    &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg,
                                    &mut dbs[selected_db],
                                    selected_db,
                                    &key,
                                );
                            }
                        }
                    }
                }

                drop(dbs); // Single drop at end of local block

                // Track key on read / invalidate tracked key on write
                if tracking_state.enabled {
                    if is_write {
                        if !matches!(response, Frame::Error(_)) {
                            if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                let senders =
                                    tracking_table.borrow_mut().invalidate_key(&key, client_id);
                                if !senders.is_empty() {
                                    let push =
                                        crate::tracking::invalidation::invalidation_push(&[key]);
                                    for tx in senders {
                                        let _ = tx.try_send(push.clone());
                                    }
                                }
                            }
                        }
                    } else if !tracking_state.bcast {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            tracking_table.borrow_mut().track_key(
                                client_id,
                                &key,
                                tracking_state.noloop,
                            );
                        }
                    }
                }

                // Apply RESP3 response conversion if needed
                let response = apply_resp3_conversion(cmd, response, protocol_version);
                responses.push(response);
            } else if let Some(target) = target_shard {
                // DEFERRED REMOTE: collect for batch dispatch after loop
                let resp_idx = responses.len();
                responses.push(Frame::Null); // placeholder, filled after batch dispatch
                // Pre-compute AOF bytes before moving frame into Arc
                let aof_bytes = if aof_tx.is_some() && metadata::is_write(cmd) {
                    Some(aof::serialize_command(&frame))
                } else {
                    None
                };
                let cmd_name = cmd.to_vec();
                remote_groups.entry(target).or_default().push((
                    resp_idx,
                    std::sync::Arc::new(frame),
                    aof_bytes,
                    cmd_name,
                ));
            }
        }

        // Phase 2: Dispatch all deferred remote commands as batched PipelineBatch
        // messages (one per target shard), await all in parallel.
        if !remote_groups.is_empty() {
            reply_futures.clear();

            for (target, entries) in remote_groups.drain() {
                let (reply_tx, reply_rx) = channel::oneshot();
                let (meta, commands): (
                    Vec<(usize, Option<Bytes>, Vec<u8>)>,
                    Vec<std::sync::Arc<Frame>>,
                ) = entries
                    .into_iter()
                    .map(|(idx, arc_frame, aof, cmd)| ((idx, aof, cmd), arc_frame))
                    .unzip();

                let msg = ShardMessage::PipelineBatch {
                    db_index: selected_db,
                    commands,
                    reply_tx,
                };
                let target_idx = ChannelMesh::target_index(shard_id, target);
                {
                    let mut pending = msg;
                    loop {
                        let push_result = {
                            let mut producers = dispatch_tx.borrow_mut();
                            producers[target_idx].try_push(pending)
                        };
                        match push_result {
                            Ok(()) => {
                                spsc_notifiers[target].notify_one();
                                break;
                            }
                            Err(val) => {
                                pending = val;
                                monoio::time::sleep(std::time::Duration::from_micros(10)).await;
                            }
                        }
                    }
                }
                reply_futures.push((meta, reply_rx));
            }

            // Await all shard responses (they execute in parallel on different shards)
            for (meta, reply_rx) in reply_futures.drain(..) {
                match reply_rx.recv().await {
                    Ok(shard_responses) => {
                        for ((resp_idx, aof_bytes, cmd_name), resp) in
                            meta.into_iter().zip(shard_responses)
                        {
                            // AOF logging for successful remote writes
                            if let Some(bytes) = aof_bytes {
                                if !matches!(resp, Frame::Error(_)) {
                                    if let Some(ref tx) = aof_tx {
                                        let _ = tx.try_send(AofMessage::Append(bytes));
                                    }
                                }
                            }
                            let resp = apply_resp3_conversion(&cmd_name, resp, protocol_version);
                            responses[resp_idx] = resp;
                        }
                    }
                    Err(_) => {
                        for (resp_idx, _, _) in meta {
                            responses[resp_idx] = Frame::Error(Bytes::from_static(
                                b"ERR cross-shard dispatch failed",
                            ));
                        }
                    }
                }
            }
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
}

#[cfg(all(test, feature = "runtime-monoio"))]
mod inline_dispatch_tests {
    use super::*;
    use crate::storage::Database;
    use crate::storage::entry::Entry;
    use bytes::{Bytes, BytesMut};
    use std::cell::RefCell;
    use std::rc::Rc;

    /// Helper: create a single-database Rc<RefCell<Vec<Database>>> for testing.
    fn make_dbs() -> Rc<RefCell<Vec<Database>>> {
        Rc::new(RefCell::new(vec![Database::new()]))
    }

    #[test]
    fn test_inline_get_hit() {
        let dbs = make_dbs();
        {
            let mut d = dbs.borrow_mut();
            d[0].set(
                Bytes::from_static(b"foo"),
                Entry::new_string(Bytes::from_static(b"bar")),
            );
        }
        let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(result, 1);
        assert!(read_buf.is_empty());
        assert_eq!(&write_buf[..], b"$3\r\nbar\r\n");
    }

    #[test]
    fn test_inline_get_miss() {
        let dbs = make_dbs();
        let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(result, 1);
        assert!(read_buf.is_empty());
        assert_eq!(&write_buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_inline_set() {
        let dbs = make_dbs();
        let mut read_buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(result, 1);
        assert!(read_buf.is_empty());
        assert_eq!(&write_buf[..], b"+OK\r\n");

        // Verify key was stored
        let mut d = dbs.borrow_mut();
        let entry = d[0].get(b"foo").expect("key should exist");
        assert_eq!(entry.value.as_bytes().unwrap(), b"bar");
    }

    #[test]
    fn test_inline_fallthrough() {
        let dbs = make_dbs();
        let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
        let mut read_buf = BytesMut::from(&ping_cmd[..]);
        let original_len = read_buf.len();
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(result, 0);
        assert_eq!(read_buf.len(), original_len);
        assert!(write_buf.is_empty());
    }

    #[test]
    fn test_inline_mixed_batch() {
        let dbs = make_dbs();
        {
            let mut d = dbs.borrow_mut();
            d[0].set(
                Bytes::from_static(b"foo"),
                Entry::new_string(Bytes::from_static(b"bar")),
            );
        }
        // GET foo followed by PING
        let mut read_buf = BytesMut::new();
        read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        read_buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        // Inline loop should process GET but leave PING
        let total = try_inline_dispatch_loop(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(total, 1);
        assert_eq!(&write_buf[..], b"$3\r\nbar\r\n");
        assert_eq!(&read_buf[..], b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn test_inline_case_insensitive() {
        let dbs = make_dbs();
        {
            let mut d = dbs.borrow_mut();
            d[0].set(
                Bytes::from_static(b"foo"),
                Entry::new_string(Bytes::from_static(b"baz")),
            );
        }
        let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n"[..]);
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(result, 1);
        assert!(read_buf.is_empty());
        assert_eq!(&write_buf[..], b"$3\r\nbaz\r\n");
    }

    #[test]
    fn test_inline_partial() {
        let dbs = make_dbs();
        // Partial command: missing key data
        let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\n"[..]);
        let original_len = read_buf.len();
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(result, 0);
        assert_eq!(read_buf.len(), original_len);
        assert!(write_buf.is_empty());
    }

    #[test]
    fn test_inline_set_with_aof() {
        let dbs = make_dbs();
        let (aof_sender, aof_receiver) = channel::mpsc_bounded::<AofMessage>(16);
        let aof_tx: Option<channel::MpscSender<AofMessage>> = Some(aof_sender);
        let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut read_buf = BytesMut::from(&cmd[..]);
        let mut write_buf = BytesMut::new();

        let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(result, 1);
        assert_eq!(&write_buf[..], b"+OK\r\n");

        // Verify AOF message was sent
        let msg = aof_receiver.try_recv().expect("should have AOF message");
        match msg {
            AofMessage::Append(bytes) => {
                assert_eq!(&bytes[..], &cmd[..]);
            }
            _ => panic!("expected Append message"),
        }
    }

    #[test]
    fn test_inline_multiple_gets() {
        let dbs = make_dbs();
        {
            let mut d = dbs.borrow_mut();
            d[0].set(
                Bytes::from_static(b"a"),
                Entry::new_string(Bytes::from_static(b"1")),
            );
            d[0].set(
                Bytes::from_static(b"b"),
                Entry::new_string(Bytes::from_static(b"2")),
            );
        }
        let mut read_buf = BytesMut::new();
        read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
        read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n");
        let mut write_buf = BytesMut::new();
        let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

        let total = try_inline_dispatch_loop(&mut read_buf, &mut write_buf, &dbs, 0, &aof_tx);
        assert_eq!(total, 2);
        assert!(read_buf.is_empty());
        assert_eq!(&write_buf[..], b"$1\r\n1\r\n$1\r\n2\r\n");
    }
}
