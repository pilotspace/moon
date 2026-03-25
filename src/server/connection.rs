use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, SinkExt, StreamExt};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use parking_lot::Mutex;
use ringbuf::traits::Producer;
use ringbuf::HeapProd;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::command::config as config_cmd;
use crate::command::connection as conn_cmd;
use crate::command::{dispatch, dispatch_read, DispatchResult};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::pubsub::{self, PubSubRegistry};
use crate::pubsub::subscriber::Subscriber;
use crate::shard::dispatch::{key_to_shard, ShardMessage};
use crate::shard::mesh::ChannelMesh;
use crate::storage::eviction::try_evict_if_needed;
use crate::storage::Database;
use crate::tracking::{TrackingState, TrackingTable};

/// Type alias for the per-database RwLock container.
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

use super::codec::RespCodec;

/// Extract command name (as raw byte slice reference) and args from a Frame::Array.
/// Returns the name without allocation -- callers use `eq_ignore_ascii_case` for matching.
fn extract_command(frame: &Frame) -> Option<(&[u8], &[Frame])> {
    match frame {
        Frame::Array(args) if !args.is_empty() => {
            let name = match &args[0] {
                Frame::BulkString(s) => s.as_ref(),
                Frame::SimpleString(s) => s.as_ref(),
                _ => return None,
            };
            Some((name, &args[1..]))
        }
        _ => None,
    }
}

/// Extract a Bytes value from a Frame argument.
pub(crate) fn extract_bytes(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// Apply RESP3 response type conversion based on command name and protocol version.
/// Uppercases the command name into a stack buffer for O(1) lookup.
#[inline]
fn apply_resp3_conversion(cmd: &[u8], response: Frame, proto: u8) -> Frame {
    if proto < 3 {
        return response;
    }
    let mut cmd_upper_buf = [0u8; 32];
    let cmd_upper_len = cmd.len().min(32);
    cmd_upper_buf[..cmd_upper_len].copy_from_slice(&cmd[..cmd_upper_len]);
    cmd_upper_buf[..cmd_upper_len].make_ascii_uppercase();
    crate::protocol::resp3::maybe_convert_resp3(&cmd_upper_buf[..cmd_upper_len], response, proto)
}

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
pub async fn handle_connection(
    stream: TcpStream,
    db: SharedDatabases,
    shutdown: CancellationToken,
    requirepass: Option<String>,
    config: Arc<ServerConfig>,
    aof_tx: Option<mpsc::Sender<AofMessage>>,
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
    let mut acl_log = crate::acl::AclLog::new(128);

    // Pub/Sub connection-local state
    let mut subscription_count: usize = 0;
    let mut pubsub_rx: Option<mpsc::Receiver<Frame>> = None;
    let mut pubsub_tx: Option<mpsc::Sender<Frame>> = None;
    let mut subscriber_id: u64 = 0;

    // Client tracking state
    let mut tracking_state = TrackingState::default();
    let mut tracking_rx: Option<mpsc::Receiver<Frame>> = None;

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
                                        let _ = framed.send(Frame::Array(vec![
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
                msg = rx.recv() => {
                    match msg {
                        Some(frame) => {
                            if framed.send(frame).await.is_err() {
                                break;
                            }
                        }
                        None => {
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
                                                        let (tx, rx) = mpsc::channel::<Frame>(256);
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
                                    if crate::persistence::aof::is_write_command(cmd) {
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
                                    let _ = tx.send(AofMessage::Append(bytes)).await;
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
                                    let (tx, rx) = mpsc::channel::<Frame>(256);
                                    subscriber_id = pubsub::next_subscriber_id();
                                    pubsub_tx = Some(tx);
                                    pubsub_rx = Some(rx);
                                }
                                let is_pattern = cmd.eq_ignore_ascii_case(b"PSUBSCRIBE");
                                for arg in cmd_args {
                                    if let Some(channel_or_pattern) = extract_bytes(arg) {
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
                            let is_write = crate::persistence::aof::is_write_command(cmd);
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
                            let is_write = aof::is_write_command(cmd);

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
                        let _ = tx.send(AofMessage::Append(bytes)).await;
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
                    rx.recv().await
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

/// Handle CONFIG GET/SET subcommands.
fn handle_config(
    args: &[Frame],
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
    server_config: &Arc<ServerConfig>,
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config' command",
        ));
    }

    let subcmd = match &args[0] {
        Frame::BulkString(s) => s.as_ref(),
        Frame::SimpleString(s) => s.as_ref(),
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR unknown subcommand for CONFIG",
            ));
        }
    };

    let sub_args = &args[1..];

    if subcmd.eq_ignore_ascii_case(b"GET") {
        let rt = runtime_config.read().unwrap();
        config_cmd::config_get(&rt, server_config, sub_args)
    } else if subcmd.eq_ignore_ascii_case(b"SET") {
        let mut rt = runtime_config.write().unwrap();
        config_cmd::config_set(&mut rt, sub_args)
    } else {
        Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}'. Try CONFIG GET, CONFIG SET.",
            String::from_utf8_lossy(subcmd)
        )))
    }
}

/// Execute a queued transaction atomically under a single database lock.
///
/// Checks WATCH versions first -- if any watched key's version has changed since
/// the snapshot was taken, the transaction is aborted and Frame::Null is returned.
///
/// Returns the result Frame (Array of responses, or Null on abort) and a Vec of
/// AOF byte entries for write commands that succeeded (caller sends them async).
fn execute_transaction(
    db: &SharedDatabases,
    command_queue: &[Frame],
    watched_keys: &HashMap<Bytes, u32>,
    selected_db: &mut usize,
) -> (Frame, Vec<Bytes>) {
    let mut guard = db[*selected_db].write();
    let db_count = db.len();
    guard.refresh_now();

    // Check WATCH versions -- if any key's version changed, abort
    for (key, watched_version) in watched_keys {
        let current_version = guard.get_version(key);
        if current_version != *watched_version {
            return (Frame::Null, Vec::new()); // Transaction aborted
        }
    }

    // Execute all queued commands atomically (under the same lock)
    let mut results = Vec::with_capacity(command_queue.len());
    let mut aof_entries: Vec<Bytes> = Vec::new();

    for cmd_frame in command_queue {
        // Extract command name and args (zero-alloc)
        let (cmd, cmd_args) = match extract_command(cmd_frame) {
            Some(pair) => pair,
            None => {
                results.push(Frame::Error(Bytes::from_static(
                    b"ERR invalid command format",
                )));
                continue;
            }
        };

        // Check if this is a write command for AOF logging
        let is_write = aof::is_write_command(cmd);

        // Serialize for AOF before dispatch
        let aof_bytes = if is_write {
            let mut buf = BytesMut::new();
            crate::protocol::serialize::serialize(cmd_frame, &mut buf);
            Some(buf.freeze())
        } else {
            None
        };

        let result = dispatch(&mut *guard, cmd, cmd_args, selected_db, db_count);
        let response = match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f, // QUIT inside MULTI just returns OK
        };

        // Collect AOF entry for successful writes (not error responses)
        if let Some(bytes) = aof_bytes {
            if !matches!(&response, Frame::Error(_)) {
                aof_entries.push(bytes);
            }
        }

        results.push(response);
    }

    (Frame::Array(results), aof_entries)
}

// ============================================================================
// Sharded connection handler (thread-per-core shared-nothing architecture)
// ============================================================================

/// Extract the primary key from a parsed command for shard routing.
///
/// Returns `None` for keyless commands (PING, DBSIZE, SELECT, etc.)
/// which should execute locally on the connection's shard.
fn extract_primary_key<'a>(cmd: &[u8], args: &'a [Frame]) -> Option<&'a Bytes> {
    // Fast check: is this command keyless? Uses (length, first_byte) dispatch.
    let len = cmd.len();
    if len == 0 { return None; }
    let b0 = cmd[0] | 0x20;

    let is_keyless = match (len, b0) {
        (4, b'a') => cmd.eq_ignore_ascii_case(b"AUTH"),
        (4, b'e') => cmd.eq_ignore_ascii_case(b"ECHO") || cmd.eq_ignore_ascii_case(b"EXEC"),
        (4, b'i') => cmd.eq_ignore_ascii_case(b"INFO"),
        (4, b'k') => cmd.eq_ignore_ascii_case(b"KEYS"),
        (4, b'p') => cmd.eq_ignore_ascii_case(b"PING"),
        (4, b'q') => cmd.eq_ignore_ascii_case(b"QUIT"),
        (4, b's') => cmd.eq_ignore_ascii_case(b"SCAN"),
        (4, b'w') => cmd.eq_ignore_ascii_case(b"WAIT"),
        (5, b'd') => cmd.eq_ignore_ascii_case(b"DEBUG"),
        (5, b'h') => cmd.eq_ignore_ascii_case(b"HELLO"),
        (5, b'm') => cmd.eq_ignore_ascii_case(b"MULTI"),
        (5, b'p') => cmd.eq_ignore_ascii_case(b"PSYNC"),
        (6, b'a') => cmd.eq_ignore_ascii_case(b"ASKING"),
        (6, b'b') => cmd.eq_ignore_ascii_case(b"BGSAVE"),
        (6, b'c') => cmd.eq_ignore_ascii_case(b"CLIENT") || cmd.eq_ignore_ascii_case(b"CONFIG"),
        (6, b'd') => cmd.eq_ignore_ascii_case(b"DBSIZE"),
        (6, b's') => cmd.eq_ignore_ascii_case(b"SELECT"),
        (7, b'c') => cmd.eq_ignore_ascii_case(b"COMMAND") || cmd.eq_ignore_ascii_case(b"CLUSTER"),
        (7, b'd') => cmd.eq_ignore_ascii_case(b"DISCARD"),
        (7, b'p') => cmd.eq_ignore_ascii_case(b"PUBLISH"),
        (7, b's') => cmd.eq_ignore_ascii_case(b"SLAVEOF"),
        (8, b'r') => cmd.eq_ignore_ascii_case(b"REPLCONF"),
        (9, b'r') => cmd.eq_ignore_ascii_case(b"REPLICAOF"),
        (9, b's') => cmd.eq_ignore_ascii_case(b"SUBSCRIBE"),
        (10, b'p') => cmd.eq_ignore_ascii_case(b"PSUBSCRIBE"),
        (11, b'u') => cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE"),
        (12, b'p') => cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE"),
        (13, b'b') => cmd.eq_ignore_ascii_case(b"BGREWRITEAOF"),
        _ => false,
    };

    if is_keyless || args.is_empty() {
        return None;
    }
    match &args[0] {
        Frame::BulkString(key) => Some(key),
        _ => None,
    }
}

/// Check if a command is a multi-key command requiring VLL coordination.
///
/// These commands operate on multiple keys that may live on different shards.
/// Single-arg DEL/UNLINK/EXISTS are NOT multi-key (handled as single-key fast path).
fn is_multi_key_command(cmd: &[u8], args: &[Frame]) -> bool {
    let len = cmd.len();
    if len == 0 { return false; }
    let b0 = cmd[0] | 0x20;
    match (len, b0) {
        (4, b'm') => cmd.eq_ignore_ascii_case(b"MGET") || cmd.eq_ignore_ascii_case(b"MSET"),
        // DEL, UNLINK, EXISTS with multiple keys
        (3, b'd') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"DEL"),
        (6, b'u') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"UNLINK"),
        (6, b'e') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"EXISTS"),
        _ => false,
    }
}

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
    aof_tx: Option<mpsc::Sender<AofMessage>>,
    tracking_table: Rc<RefCell<TrackingTable>>,
    client_id: u64,
    repl_state: Option<Arc<RwLock<crate::replication::state::ReplicationState>>>,
    cluster_state: Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    lua: std::rc::Rc<mlua::Lua>,
    script_cache: std::rc::Rc<std::cell::RefCell<crate::scripting::ScriptCache>>,
    config_port: u16,
    acl_table: Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: Arc<RwLock<RuntimeConfig>>,
    spsc_notifiers: Vec<std::sync::Arc<tokio::sync::Notify>>,
) {
    use tokio::io::AsyncWriteExt;

    // Capture peer address before we start using the stream
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    // Direct buffer I/O: bypass Framed/codec for the hot path.
    let mut stream = stream;
    let mut read_buf = BytesMut::with_capacity(8192);
    let mut write_buf = BytesMut::with_capacity(8192);
    let parse_config = crate::protocol::ParseConfig::default();
    let mut protocol_version: u8 = 2;
    let mut selected_db: usize = 0;
    let mut authenticated = requirepass.is_none();
    let mut current_user: String = "default".to_string();
    let mut acl_log = crate::acl::AclLog::new(128);

    // Transaction (MULTI/EXEC) connection-local state
    let mut in_multi: bool = false;
    let mut command_queue: Vec<Frame> = Vec::new();

    // Client tracking state
    let mut tracking_state = TrackingState::default();
    let mut tracking_rx: Option<mpsc::Receiver<Frame>> = None;

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
            result = stream.readable() => {
                if result.is_err() { break; }

                // Read all available data from socket into read_buf
                match stream.try_read_buf(&mut read_buf) {
                    Ok(0) => break, // connection closed
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(_) => break,
                }
                // Drain socket buffer: read more if available
                loop {
                    match stream.try_read_buf(&mut read_buf) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
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
                                if crate::persistence::aof::is_write_command(cmd) {
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
                                                    let (tx, rx) = mpsc::channel::<Frame>(256);
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
                            &dispatch_tx,
                            ".",  // Use current dir; proper path from config
                            num_shards,
                        );
                        responses.push(response);
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
                        let is_write_for_acl = crate::persistence::aof::is_write_command(cmd);
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
                    // Skip the is_write_command string comparison entirely when neither
                    // AOF persistence nor client tracking needs the write classification.
                    let is_write = if aof_tx.is_some() || tracking_state.enabled {
                        aof::is_write_command(cmd)
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
                        let mut dbs = databases.borrow_mut();
                        let db_count = dbs.len();
                        dbs[selected_db].refresh_now();
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
                        tokio::sync::oneshot::Receiver<Vec<Frame>>,
                    )> = Vec::with_capacity(remote_groups.len());

                    for (target, entries) in remote_groups {
                        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
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

/// Execute a queued transaction on the local shard (sharded path).
///
/// Transactions in the shared-nothing architecture are restricted to local-shard
/// keys only. Cross-shard transactions require distributed coordination (future work).
fn execute_transaction_sharded(
    databases: &Rc<RefCell<Vec<Database>>>,
    command_queue: &[Frame],
    selected_db: usize,
) -> Frame {
    let mut dbs = databases.borrow_mut();
    let db_count = dbs.len();
    dbs[selected_db].refresh_now();

    let mut results = Vec::with_capacity(command_queue.len());
    let mut selected = selected_db;

    for cmd_frame in command_queue {
        let (cmd, cmd_args) = match extract_command(cmd_frame) {
            Some(pair) => pair,
            None => {
                results.push(Frame::Error(Bytes::from_static(
                    b"ERR invalid command format",
                )));
                continue;
            }
        };

        let result = dispatch(&mut dbs[selected], cmd, cmd_args, &mut selected, db_count);
        let response = match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        };
        results.push(response);
    }

    Frame::Array(results)
}

/// Convert a blocking command to its non-blocking equivalent for MULTI/EXEC.
/// BLPOP key [key ...] timeout -> LPOP key (first key only)
/// BRPOP key [key ...] timeout -> RPOP key
/// BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout -> LMOVE src dst LEFT|RIGHT LEFT|RIGHT
/// BZPOPMIN key [key ...] timeout -> ZPOPMIN key
/// BZPOPMAX key [key ...] timeout -> ZPOPMAX key
fn convert_blocking_to_nonblocking(cmd: &[u8], args: &[Frame]) -> Frame {
    let mut new_args = Vec::new();
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"LPOP")));
        if let Some(first_key) = args.first() {
            new_args.push(first_key.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"RPOP")));
        if let Some(first_key) = args.first() {
            new_args.push(first_key.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"LMOVE")));
        // src dst LEFT|RIGHT LEFT|RIGHT (skip timeout which is last arg)
        for arg in args.iter().take(4) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"ZPOPMIN")));
        if let Some(first_key) = args.first() {
            new_args.push(first_key.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"ZPOPMAX")));
        if let Some(first_key) = args.first() {
            new_args.push(first_key.clone());
        }
    }
    Frame::Array(new_args)
}

/// Handle a blocking command (BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX).
///
/// 1. Non-blocking fast path: check if data is immediately available.
/// 2. Single-key fast path: one oneshot, one local registration (zero overhead).
/// 3. Multi-key coordinator: FuturesUnordered across local + remote shards,
///    first-wakeup-wins, BlockCancel cleanup on completion/timeout/shutdown.
///
/// CRITICAL: RefCell borrows MUST be dropped before any .await point.
async fn handle_blocking_command(
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
    databases: &Rc<RefCell<Vec<Database>>>,
    blocking_registry: &Rc<RefCell<crate::blocking::BlockingRegistry>>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    shutdown: &CancellationToken,
) -> Frame {
    use futures::stream::FuturesUnordered;

    // Parse timeout (last argument for all blocking commands)
    let timeout_secs = match parse_blocking_timeout(cmd, args) {
        Ok(t) => t,
        Err(e) => return e,
    };

    // Parse keys and command-specific args
    let (keys, blocked_cmd_factory) = match parse_blocking_args(cmd, args) {
        Ok(v) => v,
        Err(e) => return e,
    };

    // --- Non-blocking fast path: try to get data immediately ---
    {
        let mut dbs = databases.borrow_mut();
        let db = &mut dbs[selected_db];
        for key in &keys {
            let result = try_immediate_pop(cmd, db, key, args);
            if let Some(frame) = result {
                return frame;
            }
        }
    } // dbs borrow dropped here -- CRITICAL before await

    let deadline = if timeout_secs > 0.0 {
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 = block forever
    };

    // --- Single-key fast path: one registration, direct await (zero overhead) ---
    if keys.len() == 1 {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Option<Frame>>();
        let wait_id = {
            let mut reg = blocking_registry.borrow_mut();
            let wait_id = reg.next_wait_id();
            let entry = crate::blocking::WaitEntry {
                wait_id,
                cmd: blocked_cmd_factory(),
                reply_tx,
                deadline,
            };
            reg.register(selected_db, keys[0].clone(), entry);
            wait_id
        }; // reg borrow dropped

        return if let Some(dl) = deadline {
            tokio::select! {
                res = reply_rx => {
                    match res {
                        Ok(Some(frame)) => frame,
                        Ok(None) | Err(_) => Frame::Null,
                    }
                }
                _ = tokio::time::sleep_until(dl) => {
                    blocking_registry.borrow_mut().remove_wait(wait_id);
                    Frame::Null
                }
                _ = shutdown.cancelled() => {
                    blocking_registry.borrow_mut().remove_wait(wait_id);
                    Frame::Error(Bytes::from_static(b"ERR server shutting down"))
                }
            }
        } else {
            tokio::select! {
                res = reply_rx => {
                    match res {
                        Ok(Some(frame)) => frame,
                        Ok(None) | Err(_) => Frame::Null,
                    }
                }
                _ = shutdown.cancelled() => {
                    blocking_registry.borrow_mut().remove_wait(wait_id);
                    Frame::Error(Bytes::from_static(b"ERR server shutting down"))
                }
            }
        };
    }

    // --- Multi-key coordinator: register on ALL keys across local + remote shards ---
    // Uses FuturesUnordered for first-wakeup-wins semantics.
    let wait_id;
    let mut receivers: FuturesUnordered<tokio::sync::oneshot::Receiver<Option<Frame>>> =
        FuturesUnordered::new();
    let mut registered_remote_shards: Vec<usize> = Vec::new();

    {
        let mut reg = blocking_registry.borrow_mut();
        let mut producers = dispatch_tx.borrow_mut();
        wait_id = reg.next_wait_id();

        for key in &keys {
            let target = key_to_shard(key, num_shards);
            let (tx, rx) = tokio::sync::oneshot::channel::<Option<Frame>>();
            receivers.push(rx);

            if target == shard_id {
                // Local registration
                let entry = crate::blocking::WaitEntry {
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx: tx,
                    deadline,
                };
                reg.register(selected_db, key.clone(), entry);
            } else {
                // Remote registration via SPSC
                let target_idx = ChannelMesh::target_index(shard_id, target);
                let msg = ShardMessage::BlockRegister {
                    db_index: selected_db,
                    key: key.clone(),
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx: tx,
                };
                let _ = producers[target_idx].try_push(msg);
                if !registered_remote_shards.contains(&target) {
                    registered_remote_shards.push(target);
                }
            }
        }
    } // borrows dropped -- CRITICAL before await

    // Await first successful result from any key/shard.
    // FuturesUnordered may return Err (sender dropped by remove_wait cleanup) before
    // returning the successful Ok. We must skip Err/None results and keep polling.
    let frame = if let Some(dl) = deadline {
        let sleep = tokio::time::sleep_until(dl);
        tokio::pin!(sleep);
        let mut result_frame = Frame::Null;
        loop {
            tokio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) => { result_frame = Frame::Null; break; }
                        Some(Err(_)) => continue, // sender dropped (cleanup race), try next
                        None => break, // all receivers exhausted
                    }
                }
                _ = &mut sleep => break,
                _ = shutdown.cancelled() => {
                    result_frame = Frame::Error(Bytes::from_static(b"ERR server shutting down"));
                    break;
                }
            }
        }
        result_frame
    } else {
        let mut result_frame = Frame::Null;
        loop {
            tokio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) => { result_frame = Frame::Null; break; }
                        Some(Err(_)) => continue,
                        None => break,
                    }
                }
                _ = shutdown.cancelled() => {
                    result_frame = Frame::Error(Bytes::from_static(b"ERR server shutting down"));
                    break;
                }
            }
        }
        result_frame
    };

    // Cleanup: cancel all remaining registrations (local + remote)
    blocking_registry.borrow_mut().remove_wait(wait_id);
    if !registered_remote_shards.is_empty() {
        let mut producers = dispatch_tx.borrow_mut();
        for &remote_shard in &registered_remote_shards {
            let target_idx = ChannelMesh::target_index(shard_id, remote_shard);
            let _ = producers[target_idx].try_push(ShardMessage::BlockCancel { wait_id });
        }
    }
    // Drop remaining receivers; remote senders get Err on send -- harmless
    drop(receivers);

    frame
}

/// Parse timeout from the last argument of a blocking command.
/// Returns seconds as f64. 0 = block forever.
fn parse_blocking_timeout(cmd: &[u8], args: &[Frame]) -> Result<f64, Frame> {
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from(format!(
            "ERR wrong number of arguments for '{}' command",
            String::from_utf8_lossy(cmd).to_lowercase()
        ))));
    }
    let timeout_frame = args.last().unwrap();
    let timeout_bytes = match timeout_frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => b,
        _ => return Err(Frame::Error(Bytes::from_static(b"ERR timeout is not a float or out of range"))),
    };
    let timeout_str = std::str::from_utf8(timeout_bytes)
        .map_err(|_| Frame::Error(Bytes::from_static(b"ERR timeout is not a float or out of range")))?;
    let timeout: f64 = timeout_str.parse()
        .map_err(|_| Frame::Error(Bytes::from_static(b"ERR timeout is not a float or out of range")))?;
    if timeout < 0.0 {
        return Err(Frame::Error(Bytes::from_static(b"ERR timeout is negative")));
    }
    Ok(timeout)
}

/// Parse keys and build a BlockedCommand factory from blocking command args.
fn parse_blocking_args(
    cmd: &[u8],
    args: &[Frame],
) -> Result<(Vec<Bytes>, Box<dyn Fn() -> crate::blocking::BlockedCommand>), Frame> {
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        // BLPOP key [key ...] timeout
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blpop' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blpop' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BLPop)))
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'brpop' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'brpop' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BRPop)))
    } else if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        // BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
        if args.len() != 5 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blmove' command",
            )));
        }
        let source = extract_bytes(&args[0]).ok_or_else(|| {
            Frame::Error(Bytes::from_static(b"ERR invalid source key"))
        })?;
        let destination = extract_bytes(&args[1]).ok_or_else(|| {
            Frame::Error(Bytes::from_static(b"ERR invalid destination key"))
        })?;
        let wherefrom_bytes = extract_bytes(&args[2]).ok_or_else(|| {
            Frame::Error(Bytes::from_static(b"ERR syntax error"))
        })?;
        let whereto_bytes = extract_bytes(&args[3]).ok_or_else(|| {
            Frame::Error(Bytes::from_static(b"ERR syntax error"))
        })?;
        let wherefrom = if wherefrom_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else if wherefrom_bytes.eq_ignore_ascii_case(b"RIGHT") {
            crate::blocking::Direction::Right
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        let whereto = if whereto_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else if whereto_bytes.eq_ignore_ascii_case(b"RIGHT") {
            crate::blocking::Direction::Right
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        Ok((
            vec![source],
            Box::new(move || crate::blocking::BlockedCommand::BLMove {
                destination: destination.clone(),
                wherefrom,
                whereto,
            }),
        ))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmin' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmin' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BZPopMin)))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmax' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmax' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BZPopMax)))
    } else {
        Err(Frame::Error(Bytes::from_static(b"ERR unknown blocking command")))
    }
}

/// Try to pop data immediately (non-blocking fast path).
fn try_immediate_pop(cmd: &[u8], db: &mut Database, key: &Bytes, args: &[Frame]) -> Option<Frame> {
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        db.list_pop_front(key).map(|v| Frame::Array(vec![
            Frame::BulkString(key.clone()),
            Frame::BulkString(v),
        ]))
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        db.list_pop_back(key).map(|v| Frame::Array(vec![
            Frame::BulkString(key.clone()),
            Frame::BulkString(v),
        ]))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        db.zset_pop_min(key).map(|(member, score)| Frame::Array(vec![
            Frame::BulkString(key.clone()),
            Frame::BulkString(member),
            Frame::BulkString(Bytes::from(format_blocking_score(score))),
        ]))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        db.zset_pop_max(key).map(|(member, score)| Frame::Array(vec![
            Frame::BulkString(key.clone()),
            Frame::BulkString(member),
            Frame::BulkString(Bytes::from(format_blocking_score(score))),
        ]))
    } else if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        // BLMOVE: try immediate LMOVE
        // args: [source, destination, wherefrom, whereto, timeout]
        use crate::blocking::Direction;
        let dest = extract_bytes(&args[1])?;
        let wherefrom = if extract_bytes(&args[2])?.eq_ignore_ascii_case(b"LEFT") {
            Direction::Left
        } else {
            Direction::Right
        };
        let whereto = if extract_bytes(&args[3])?.eq_ignore_ascii_case(b"LEFT") {
            Direction::Left
        } else {
            Direction::Right
        };
        let val = match wherefrom {
            Direction::Left => db.list_pop_front(key),
            Direction::Right => db.list_pop_back(key),
        }?;
        match whereto {
            Direction::Left => db.list_push_front(&dest, val.clone()),
            Direction::Right => db.list_push_back(&dest, val.clone()),
        }
        Some(Frame::BulkString(val))
    } else {
        None
    }
}

/// Format a float score the same way Redis does (integer if whole, otherwise full precision).
fn format_blocking_score(score: f64) -> String {
    if score == score.floor() && score.abs() < i64::MAX as f64 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}
