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
use ringbuf::HeapProd;
use ringbuf::traits::Producer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::pubsub::PubSubRegistry;
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::entry::CachedClock;
use crate::storage::eviction::try_evict_if_needed;
use crate::tracking::{TrackingState, TrackingTable};

use super::affinity::{AffinityTracker, MigratedConnectionState};
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
    is_multi_key_command, restore_migrated_state,
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
pub async fn handle_connection_sharded(
    stream: TcpStream,
    shard_databases: Arc<ShardDatabases>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_registry: Arc<RwLock<PubSubRegistry>>,
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
    remote_subscriber_map: Arc<RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>,
    all_pubsub_registries: Vec<Arc<RwLock<PubSubRegistry>>>,
    all_remote_sub_maps: Vec<Arc<RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>>,
    affinity_tracker: Arc<RwLock<crate::shard::affinity::AffinityTracker>>,
) {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let result = handle_connection_sharded_inner(
        stream,
        peer_addr,
        shard_databases,
        shard_id,
        num_shards,
        dispatch_tx.clone(),
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
        spsc_notifiers.clone(),
        snapshot_trigger_tx,
        cached_clock,
        remote_subscriber_map,
        all_pubsub_registries,
        all_remote_sub_maps,
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
                let target_idx = ChannelMesh::target_index(shard_id, target_shard);
                let push_result = {
                    let mut producers = dispatch_tx.borrow_mut();
                    producers[target_idx].try_push(msg)
                };
                match push_result {
                    Ok(()) => {
                        spsc_notifiers[target_shard].notify_one();
                        tracing::info!(
                            "Shard {}: migrated connection {} to shard {}",
                            shard_id,
                            client_id,
                            target_shard
                        );
                    }
                    Err(returned_msg) => {
                        // SPSC full — retry with yield before giving up.
                        let mut pending = Some(returned_msg);
                        for _ in 0..8 {
                            tokio::task::yield_now().await;
                            let msg = pending.take().unwrap();
                            let push_result = {
                                let mut producers = dispatch_tx.borrow_mut();
                                producers[target_idx].try_push(msg)
                            };
                            match push_result {
                                Ok(()) => {
                                    spsc_notifiers[target_shard].notify_one();
                                    tracing::info!(
                                        "Shard {}: migrated connection {} to shard {} (after retry)",
                                        shard_id,
                                        client_id,
                                        target_shard
                                    );
                                    break;
                                }
                                Err(msg) => pending = Some(msg),
                            }
                        }
                        if let Some(ShardMessage::MigrateConnection { fd, .. }) = pending {
                            // SAFETY: fd is a valid, uniquely-owned file descriptor obtained
                            // from TcpStream::into_raw_fd() above. No other code holds a
                            // reference to this fd. OwnedFd closes it on drop.
                            use std::os::unix::io::FromRawFd;
                            drop(unsafe { std::os::unix::io::OwnedFd::from_raw_fd(fd) });
                        }
                        tracing::warn!(
                            "Shard {}: migration SPSC full, connection {} lost",
                            shard_id,
                            client_id
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Shard {}: migration into_std failed: {}", shard_id, e);
                // Stream consumed by into_std attempt, connection lost either way
            }
        }
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
pub async fn handle_connection_sharded_inner<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
>(
    stream: S,
    peer_addr: String,
    shard_databases: Arc<ShardDatabases>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_registry: Arc<RwLock<PubSubRegistry>>,
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
    remote_subscriber_map: Arc<RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>,
    all_pubsub_registries: Vec<Arc<RwLock<PubSubRegistry>>>,
    all_remote_sub_maps: Vec<Arc<RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>>,
    affinity_tracker: Arc<RwLock<AffinityTracker>>,
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
    let (
        mut protocol_version,
        mut selected_db,
        mut authenticated,
        mut current_user,
        client_name_restored,
    ) = restore_migrated_state(migrated_state, &requirepass);
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

    // Pub/Sub subscriber mode state
    use crate::pubsub::{self, subscriber::Subscriber};
    let mut pubsub_tx: Option<channel::MpscSender<Frame>> = None;
    let mut pubsub_rx: Option<channel::MpscReceiver<Frame>> = None;
    let mut subscriber_id: u64 = 0;
    let mut subscription_count: usize = 0;

    // RESP3/HELLO connection-local state
    let mut client_name: Option<Bytes> = client_name_restored;

    // Cluster ASKING flag: set by ASKING command, cleared unconditionally before routing check.
    let mut asking: bool = false;

    // Per-connection arena for batch processing temporaries.
    // 4KB initial capacity, grows on demand (rarely exceeds 16KB per batch).
    let mut arena = Bump::with_capacity(4096);

    // Connection affinity: only track when multi-shard AND migration is possible (plain TCP).
    // Single-shard has no cross-shard traffic; TLS connections cannot transfer session state.
    let mut affinity_tracker = if num_shards > 1 && can_migrate {
        Some(AffinityTracker::new(shard_id, num_shards))
    } else {
        None
    };

    // Pre-allocated response slots for zero-allocation cross-shard dispatch.
    let response_pool = ResponseSlotPool::new(num_shards, shard_id);

    let mut break_outer = false;
    // Migration target: set when AffinityTracker triggers, acted on after batch response flush.
    let mut migration_target: Option<usize> = None;
    loop {
        // --- Subscriber mode: bidirectional select on client commands + published messages ---
        if subscription_count > 0 {
            let rx = pubsub_rx.as_mut().unwrap();
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
                                                let acl_deny = { acl_table.read().unwrap().check_channel_permission(&current_user, ch.as_ref()) };
                                                if let Some(reason) = acl_deny {
                                                    let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&err, &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                    continue;
                                                }
                                                let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                { pubsub_registry.write().unwrap().subscribe(ch.clone(), sub); }
                                                subscription_count += 1;
                                                // Register pub/sub affinity for this client IP
                                                if subscription_count == 1 {
                                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                        affinity_tracker.write().unwrap().register(addr.ip(), shard_id);
                                                    }
                                                }
                                                // Direct shared-write: propagate subscription to all shards' remote subscriber maps
                                                for target in 0..num_shards {
                                                    if target == shard_id { continue; }
                                                    all_remote_sub_maps[target].write().unwrap().add(ch.clone(), shard_id, false);
                                                }
                                                write_buf.clear();
                                                let resp = pubsub::subscribe_response(&ch, subscription_count);
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
                                                let acl_deny = { acl_table.read().unwrap().check_channel_permission(&current_user, pat.as_ref()) };
                                                if let Some(reason) = acl_deny {
                                                    let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&err, &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                    continue;
                                                }
                                                let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                { pubsub_registry.write().unwrap().psubscribe(pat.clone(), sub); }
                                                subscription_count += 1;
                                                // Register pub/sub affinity for this client IP
                                                if subscription_count == 1 {
                                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                        affinity_tracker.write().unwrap().register(addr.ip(), shard_id);
                                                    }
                                                }
                                                // Direct shared-write: propagate pattern subscription to all shards' remote subscriber maps
                                                for target in 0..num_shards {
                                                    if target == shard_id { continue; }
                                                    all_remote_sub_maps[target].write().unwrap().add(pat.clone(), shard_id, true);
                                                }
                                                write_buf.clear();
                                                let resp = pubsub::psubscribe_response(&pat, subscription_count);
                                                crate::protocol::serialize(&resp, &mut write_buf);
                                                if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            }
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
                                        if cmd_args.is_empty() {
                                            let removed = { pubsub_registry.write().unwrap().unsubscribe_all(subscriber_id) };
                                            if removed.is_empty() {
                                                subscription_count = pubsub_registry.read().unwrap().total_subscription_count(subscriber_id);
                                                write_buf.clear();
                                                crate::protocol::serialize(&pubsub::unsubscribe_response(&Bytes::from_static(b""), subscription_count), &mut write_buf);
                                                if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            } else {
                                                for ch in &removed {
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    for target in 0..num_shards {
                                                        if target == shard_id { continue; }
                                                        all_remote_sub_maps[target].write().unwrap().remove(ch, shard_id, false);
                                                    }
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::unsubscribe_response(ch, subscription_count), &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(ch) = extract_bytes(arg) {
                                                    { pubsub_registry.write().unwrap().unsubscribe(ch.as_ref(), subscriber_id); }
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    for target in 0..num_shards {
                                                        if target == shard_id { continue; }
                                                        all_remote_sub_maps[target].write().unwrap().remove(&ch, shard_id, false);
                                                    }
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::unsubscribe_response(&ch, subscription_count), &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                }
                                            }
                                        }
                                    } else if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                                        if cmd_args.is_empty() {
                                            let removed = { pubsub_registry.write().unwrap().punsubscribe_all(subscriber_id) };
                                            if removed.is_empty() {
                                                subscription_count = pubsub_registry.read().unwrap().total_subscription_count(subscriber_id);
                                                write_buf.clear();
                                                crate::protocol::serialize(&pubsub::punsubscribe_response(&Bytes::from_static(b""), subscription_count), &mut write_buf);
                                                if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                            } else {
                                                for pat in &removed {
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    for target in 0..num_shards {
                                                        if target == shard_id { continue; }
                                                        all_remote_sub_maps[target].write().unwrap().remove(pat, shard_id, true);
                                                    }
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::punsubscribe_response(pat, subscription_count), &mut write_buf);
                                                    if stream.write_all(&write_buf).await.is_err() { sub_break = true; break; }
                                                }
                                            }
                                        } else {
                                            for arg in cmd_args {
                                                if let Some(pat) = extract_bytes(arg) {
                                                    { pubsub_registry.write().unwrap().punsubscribe(pat.as_ref(), subscriber_id); }
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    for target in 0..num_shards {
                                                        if target == shard_id { continue; }
                                                        all_remote_sub_maps[target].write().unwrap().remove(&pat, shard_id, true);
                                                    }
                                                    write_buf.clear();
                                                    crate::protocol::serialize(&pubsub::punsubscribe_response(&pat, subscription_count), &mut write_buf);
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
                                        { pubsub_registry.write().unwrap().unsubscribe_all(subscriber_id); }
                                        { pubsub_registry.write().unwrap().punsubscribe_all(subscriber_id); }
                                        subscription_count = 0;
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
                                if subscription_count == 0 { break; }
                            }
                            Ok(None) => break,
                            Err(_) => { return; }
                        }
                    }
                    if sub_break { break; }
                    if subscription_count == 0 { continue; }
                }
                msg = rx.recv_async() => {
                    match msg {
                        Ok(frame) => {
                            write_buf.clear();
                            if protocol_version >= 3 {
                                crate::protocol::serialize_resp3(&frame, &mut write_buf);
                            } else {
                                crate::protocol::serialize(&frame, &mut write_buf);
                            }
                            if stream.write_all(&write_buf).await.is_err() { break; }
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
                let mut remote_groups: HashMap<usize, Vec<(usize, std::sync::Arc<Frame>, Option<Bytes>, Bytes)>> = HashMap::with_capacity(num_shards);
                // Accumulate cross-shard PUBLISH pairs per target shard for batch dispatch
                // Key: target shard ID -> Vec of (response_index, channel, message)
                let mut publish_batches: HashMap<usize, Vec<(usize, Bytes, Bytes)>> = HashMap::new();

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

                    // --- ASKING ---
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
                            let mut guard = shard_databases.write_db(shard_id, selected_db);
                            let db_count = shard_databases.db_count();
                            if cmd.eq_ignore_ascii_case(b"EVAL") {
                                crate::scripting::handle_eval(
                                    &lua, &script_cache, cmd_args, &mut guard,
                                    shard_id, num_shards, selected_db, db_count,
                                )
                            } else {
                                crate::scripting::handle_evalsha(
                                    &lua, &script_cache, cmd_args, &mut guard,
                                    shard_id, num_shards, selected_db, db_count,
                                )
                            }
                        };
                        responses.push(response);
                        continue;
                    }

                    // --- SCRIPT subcommands ---
                    if cmd.eq_ignore_ascii_case(b"SCRIPT") {
                        let (response, fanout) = crate::scripting::handle_script_subcommand(&script_cache, cmd_args);
                        if let Some((sha1, script_bytes)) = fanout {
                            let mut producers = dispatch_tx.borrow_mut();
                            for target in 0..num_shards {
                                if target == shard_id { continue; }
                                let idx = ChannelMesh::target_index(shard_id, target);
                                let msg = ShardMessage::ScriptLoad { sha1: sha1.clone(), script: script_bytes.clone() };
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

                    // --- AUTH (already authenticated) ---
                    if cmd.eq_ignore_ascii_case(b"AUTH") {
                        let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &acl_table);
                        if let Some(uname) = opt_user { current_user = uname; }
                        responses.push(response);
                        continue;
                    }

                    // --- HELLO ---
                    if cmd.eq_ignore_ascii_case(b"HELLO") {
                        let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
                            cmd_args, protocol_version, client_id, &acl_table, &mut authenticated,
                        );
                        if !matches!(&response, Frame::Error(_)) { protocol_version = new_proto; }
                        if let Some(name) = new_name { client_name = Some(name); }
                        if let Some(uname) = opt_user { current_user = uname; }
                        responses.push(response);
                        continue;
                    }

                    // --- ACL ---
                    if cmd.eq_ignore_ascii_case(b"ACL") {
                        let response = crate::command::acl::handle_acl(
                            cmd_args, &acl_table, &mut acl_log, &current_user, &peer_addr, &runtime_config,
                        );
                        responses.push(response);
                        continue;
                    }

                    // === ACL permission check ===
                    // Must run before any command-specific handlers (CONFIG, REPLICAOF, etc.)
                    // so that low-privilege users cannot reach admin commands.
                    {
                        let acl_guard = acl_table.read().unwrap();
                        if let Some(deny_reason) = acl_guard.check_command_permission(&current_user, cmd, cmd_args) {
                            drop(acl_guard);
                            acl_log.push(crate::acl::AclLogEntry {
                                reason: "command".to_string(),
                                object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                username: current_user.clone(),
                                client_addr: peer_addr.clone(),
                                timestamp_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                            });
                            responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                            continue;
                        }
                        let is_write_for_acl = metadata::is_write(cmd);
                        if let Some(deny_reason) = acl_guard.check_key_permission(&current_user, cmd, cmd_args, is_write_for_acl) {
                            drop(acl_guard);
                            acl_log.push(crate::acl::AclLogEntry {
                                reason: "command".to_string(),
                                object: String::from_utf8_lossy(cmd).to_ascii_lowercase(),
                                username: current_user.clone(),
                                client_addr: peer_addr.clone(),
                                timestamp_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                            });
                            responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                            continue;
                        }
                    }

                    // --- CONFIG ---
                    if cmd.eq_ignore_ascii_case(b"CONFIG") {
                        responses.push(handle_config(cmd_args, &runtime_config, &config));
                        continue;
                    }

                    // --- REPLICAOF / SLAVEOF ---
                    if cmd.eq_ignore_ascii_case(b"REPLICAOF") || cmd.eq_ignore_ascii_case(b"SLAVEOF") {
                        use crate::command::connection::{replicaof, ReplicaofAction};
                        let (resp, action) = replicaof(cmd_args);
                        if let Some(action) = action {
                            if let Some(ref rs) = repl_state {
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
                                            repl_state: rs_clone, num_shards,
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
                        let guard = shard_databases.read_db(shard_id, selected_db);
                        let response_text = {
                            let resp_frame = conn_cmd::info_readonly(&guard, cmd_args);
                            match resp_frame {
                                Frame::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                                _ => String::new(),
                            }
                        };
                        drop(guard);
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

                    // --- READONLY enforcement ---
                    if let Some(ref rs) = repl_state {
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
                                        Err(err_frame) => { responses.push(err_frame); continue; }
                                    }
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
                        if in_multi {
                            responses.push(Frame::Error(Bytes::from_static(b"ERR MULTI calls can not be nested")));
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
                            let result = execute_transaction_sharded(&shard_databases, shard_id, &command_queue, selected_db, &cached_clock);
                            command_queue.clear();
                            responses.push(result);
                        }
                        continue;
                    }

                    // --- DISCARD ---
                    if cmd.eq_ignore_ascii_case(b"DISCARD") {
                        if !in_multi {
                            responses.push(Frame::Error(Bytes::from_static(b"ERR DISCARD without MULTI")));
                        } else {
                            in_multi = false;
                            command_queue.clear();
                            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                        }
                        continue;
                    }

                    // --- BLOCKING COMMANDS ---
                    if cmd.eq_ignore_ascii_case(b"BLPOP") || cmd.eq_ignore_ascii_case(b"BRPOP")
                        || cmd.eq_ignore_ascii_case(b"BLMOVE") || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
                        || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
                    {
                        if in_multi {
                            let nb_frame = convert_blocking_to_nonblocking(cmd, cmd_args);
                            command_queue.push(nb_frame);
                            responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                            continue;
                        }
                        write_buf.clear();
                        for response in responses.iter() {
                            if protocol_version >= 3 {
                                crate::protocol::serialize_resp3(response, &mut write_buf);
                            } else {
                                crate::protocol::serialize(response, &mut write_buf);
                            }
                        }
                        if stream.write_all(&write_buf).await.is_err() { arena.reset(); return (HandlerResult::Done, None); }
                        let blocking_response = handle_blocking_command(
                            cmd, cmd_args, selected_db, &shard_databases, &blocking_registry,
                            shard_id, num_shards, &dispatch_tx, &shutdown,
                        ).await;
                        let blocking_response = apply_resp3_conversion(cmd, blocking_response, protocol_version);
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
                                let acl_guard = acl_table.read().unwrap();
                                if let Some(deny_reason) = acl_guard.check_channel_permission(&current_user, ch.as_ref()) {
                                    drop(acl_guard);
                                    responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                                    continue;
                                }
                            }
                            match (channel_arg, message_arg) {
                                (Some(ch), Some(msg)) => {
                                    let local_count = { pubsub_registry.write().unwrap().publish(&ch, &msg) };
                                    // Targeted fanout: only send to shards that have subscribers
                                    let targets = remote_subscriber_map.read().unwrap().target_shards(&ch);
                                    if targets.is_empty() {
                                        // Fast path: no remote subscribers, return local count immediately
                                        responses.push(Frame::Integer(local_count));
                                    } else {
                                        // Filter to remote targets only (skip self)
                                        let remote_targets: Vec<usize> = targets.into_iter().filter(|&t| t != shard_id).collect();
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
                        if pubsub_tx.is_none() {
                            let (tx, rx) = channel::mpsc_bounded(256);
                            pubsub_tx = Some(tx);
                            pubsub_rx = Some(rx);
                        }
                        if subscriber_id == 0 {
                            subscriber_id = crate::pubsub::next_subscriber_id();
                        }
                        // Flush accumulated responses before entering subscriber mode
                        if !responses.is_empty() {
                            write_buf.clear();
                            for resp in &responses {
                                if protocol_version >= 3 {
                                    crate::protocol::serialize_resp3(resp, &mut write_buf);
                                } else {
                                    crate::protocol::serialize(resp, &mut write_buf);
                                }
                            }
                            if stream.write_all(&write_buf).await.is_err() { return; }
                            responses.clear();
                        }
                        // Process subscribe arguments
                        for arg in cmd_args {
                            if let Some(ch) = extract_bytes(arg) {
                                let acl_deny = { acl_table.read().unwrap().check_channel_permission(&current_user, ch.as_ref()) };
                                if let Some(reason) = acl_deny {
                                    write_buf.clear();
                                    let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                    crate::protocol::serialize(&err, &mut write_buf);
                                    if stream.write_all(&write_buf).await.is_err() { return; }
                                    continue;
                                }
                                let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                if is_pattern {
                                    { pubsub_registry.write().unwrap().psubscribe(ch.clone(), sub); }
                                } else {
                                    { pubsub_registry.write().unwrap().subscribe(ch.clone(), sub); }
                                }
                                subscription_count += 1;
                                // Register pub/sub affinity for this client IP
                                if subscription_count == 1 {
                                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                        affinity_tracker.write().unwrap().register(addr.ip(), shard_id);
                                    }
                                }
                                // Direct shared-write: propagate subscription to all shards' remote subscriber maps
                                for target in 0..num_shards {
                                    if target == shard_id { continue; }
                                    all_remote_sub_maps[target].write().unwrap().add(ch.clone(), shard_id, is_pattern);
                                }
                                write_buf.clear();
                                let resp = if is_pattern {
                                    pubsub::psubscribe_response(&ch, subscription_count)
                                } else {
                                    pubsub::subscribe_response(&ch, subscription_count)
                                };
                                crate::protocol::serialize(&resp, &mut write_buf);
                                if stream.write_all(&write_buf).await.is_err() { return; }
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
                                for reg in &all_pubsub_registries {
                                    let guard = reg.read().unwrap();
                                    all_channels.extend(guard.active_channels(pattern.as_deref()));
                                }
                                let arr: Vec<Frame> = all_channels.into_iter().map(Frame::BulkString).collect();
                                responses.push(Frame::Array(arr.into()));
                            }
                            Some(ref sc) if sc.eq_ignore_ascii_case(b"NUMSUB") => {
                                let channels: Vec<Bytes> = cmd_args[1..].iter().filter_map(|a| extract_bytes(a)).collect();
                                let mut counts: HashMap<Bytes, i64> = HashMap::new();
                                for reg in &all_pubsub_registries {
                                    let guard = reg.read().unwrap();
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
                                for reg in &all_pubsub_registries {
                                    total += reg.read().unwrap().numpat();
                                }
                                responses.push(Frame::Integer(total as i64));
                            }
                            _ => {
                                responses.push(Frame::Error(Bytes::from_static(b"ERR unknown subcommand or wrong number of arguments for 'pubsub' command")));
                            }
                        }
                        continue;
                    }

                    // --- BGSAVE ---
                    if cmd.eq_ignore_ascii_case(b"BGSAVE") {
                        responses.push(crate::command::persistence::bgsave_start_sharded(&snapshot_trigger_tx, num_shards));
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

                    // --- MULTI queue mode ---
                    if in_multi {
                        command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                        continue;
                    }

                    // --- Cross-shard aggregation: KEYS, SCAN, DBSIZE ---
                    if cmd.eq_ignore_ascii_case(b"KEYS") {
                        let response = crate::shard::coordinator::coordinate_keys(cmd_args, shard_id, num_shards, selected_db, &shard_databases, &dispatch_tx, &spsc_notifiers, &cached_clock, &()).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"SCAN") {
                        let response = crate::shard::coordinator::coordinate_scan(cmd_args, shard_id, num_shards, selected_db, &shard_databases, &dispatch_tx, &spsc_notifiers, &cached_clock, &()).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                        let response = crate::shard::coordinator::coordinate_dbsize(shard_id, num_shards, selected_db, &shard_databases, &dispatch_tx, &spsc_notifiers, &()).await;
                        responses.push(response);
                        continue;
                    }

                    // --- Multi-key commands ---
                    if is_multi_key_command(cmd, cmd_args) {
                        let response = crate::shard::coordinator::coordinate_multi_key(cmd, cmd_args, shard_id, num_shards, selected_db, &shard_databases, &dispatch_tx, &spsc_notifiers, &cached_clock, &()).await;
                        responses.push(response);
                        continue;
                    }

                    // --- Routing: keyless, local, or remote ---
                    let target_shard = extract_primary_key(cmd, cmd_args).map(|key| key_to_shard(key, num_shards));
                    let is_local = match target_shard {
                        None => true,
                        Some(s) if s == shard_id => true,
                        _ => false,
                    };

                    // Affinity sampling: record shard target for migration decision.
                    // Only sample when we have a concrete target shard (key-bearing command).
                    // Migration is deferred until AFTER the current batch is fully processed
                    // and all responses are written, ensuring no command/response desync.
                    if let (Some(tracker), Some(target)) = (&mut affinity_tracker, target_shard) {
                        if let Some(migrate_to) = tracker.record(target) {
                            // Migration preconditions: not in MULTI, no active CLIENT TRACKING
                            // (tracking connections need untrack_all cleanup which doesn't transfer)
                            if !in_multi && !tracking_state.enabled {
                                migration_target = Some(migrate_to);
                            }
                        }
                    }

                    let is_write = if aof_tx.is_some() || tracking_state.enabled { metadata::is_write(cmd) } else { false };
                    let aof_bytes = if is_write && aof_tx.is_some() { Some(aof::serialize_command(&frame)) } else { None };

                    if is_local {
                        // LOCAL PATH: split into read/write to avoid exclusive lock on reads.
                        // Using read_db for local reads eliminates RwLock contention with
                        // cross-shard shared reads from other shard threads.
                        if metadata::is_write(cmd) {
                            // WRITE PATH: single lock acquisition for eviction + dispatch
                            let rt = runtime_config.read().unwrap();
                            let mut guard = shard_databases.write_db(shard_id, selected_db);
                            if let Err(oom_frame) = try_evict_if_needed(&mut guard, &rt) {
                                drop(guard);
                                drop(rt);
                                responses.push(oom_frame);
                                continue;
                            }
                            drop(rt);

                            let db_count = shard_databases.db_count();
                            guard.refresh_now_from_cache(&cached_clock);
                            let result = dispatch(&mut guard, cmd, cmd_args, &mut selected_db, db_count);
                            let response = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => { should_quit = true; f }
                            };
                            if !matches!(response, Frame::Error(_)) {
                                let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH")
                                    || cmd.eq_ignore_ascii_case(b"LMOVE") || cmd.eq_ignore_ascii_case(b"ZADD");
                                if needs_wake {
                                    if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                        let mut reg = blocking_registry.borrow_mut();
                                        if cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH") || cmd.eq_ignore_ascii_case(b"LMOVE") {
                                            crate::blocking::wakeup::try_wake_list_waiter(&mut reg, &mut guard, selected_db, &key);
                                        } else {
                                            crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, &mut guard, selected_db, &key);
                                        }
                                    }
                                }
                            }
                            drop(guard);
                            if let Some(bytes) = aof_bytes {
                                if !matches!(response, Frame::Error(_)) {
                                    if let Some(ref tx) = aof_tx { let _ = tx.try_send(AofMessage::Append(bytes)); }
                                }
                            }
                            if tracking_state.enabled && !matches!(response, Frame::Error(_)) {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    let senders = tracking_table.borrow_mut().invalidate_key(&key, client_id);
                                    if !senders.is_empty() {
                                        let push = crate::tracking::invalidation::invalidation_push(&[key]);
                                        for tx in senders { let _ = tx.try_send(push.clone()); }
                                    }
                                }
                            }
                            let response = apply_resp3_conversion(cmd, response, protocol_version);
                            responses.push(response);
                        } else {
                            // READ PATH: shared lock — no contention with other shards' reads
                            let guard = shard_databases.read_db(shard_id, selected_db);
                            let now_ms = cached_clock.ms();
                            let db_count = shard_databases.db_count();
                            let result = dispatch_read(&guard, cmd, cmd_args, now_ms, &mut selected_db, db_count);
                            drop(guard);
                            let response = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => { should_quit = true; f }
                            };
                            if tracking_state.enabled && !tracking_state.bcast {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    tracking_table.borrow_mut().track_key(client_id, &key, tracking_state.noloop);
                                }
                            }
                            let response = apply_resp3_conversion(cmd, response, protocol_version);
                            responses.push(response);
                        }
                    } else if let Some(target) = target_shard {
                        // SHARED-READ FAST PATH: cross-shard reads bypass SPSC dispatch entirely.
                        // By this point in_multi is false (MULTI queuing happens earlier with `continue`).
                        // Read commands execute directly on the target shard's database via RwLock read guard,
                        // avoiding ~88us of two async scheduling hops through the SPSC channel.
                        //
                        // Guard: if there are already pending writes for this target shard in the
                        // current pipeline batch, we must NOT take the fast path -- the read would
                        // execute before the deferred writes, violating command ordering. Fall through
                        // to SPSC dispatch to preserve pipeline semantics.
                        if !metadata::is_write(cmd) && !remote_groups.contains_key(&target) {
                            let guard = shard_databases.read_db(target, selected_db);
                            let now_ms = cached_clock.ms();
                            let db_count = shard_databases.db_count();
                            let result = dispatch_read(&guard, cmd, cmd_args, now_ms, &mut selected_db, db_count);
                            drop(guard);
                            let response = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => { should_quit = true; f }
                            };
                            // Client tracking for cross-shard reads
                            if tracking_state.enabled && !tracking_state.bcast {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    tracking_table.borrow_mut().track_key(client_id, &key, tracking_state.noloop);
                                }
                            }
                            let response = apply_resp3_conversion(cmd, response, protocol_version);
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
                        remote_groups.entry(target).or_default().push((resp_idx, std::sync::Arc::new(frame), aof_bytes, cmd_bytes));
                    }
                }

                // Phase 2: Dispatch deferred remote commands (zero-allocation via ResponseSlotPool)
                if !remote_groups.is_empty() {
                    let mut reply_futures: Vec<(Vec<(usize, Option<Bytes>, Bytes)>, usize)> = Vec::with_capacity(remote_groups.len());
                    for (target, entries) in remote_groups {
                        let slot_ptr = response_pool.slot_ptr(target);
                        let (meta, commands): (Vec<(usize, Option<Bytes>, Bytes)>, Vec<std::sync::Arc<Frame>>) =
                            entries.into_iter().map(|(idx, arc_frame, aof, cmd)| ((idx, aof, cmd), arc_frame)).unzip();
                        let msg = ShardMessage::PipelineBatchSlotted { db_index: selected_db, commands, response_slot: crate::shard::dispatch::ResponseSlotPtr(slot_ptr) };
                        let target_idx = ChannelMesh::target_index(shard_id, target);
                        {
                            let mut pending = msg;
                            loop {
                                let push_result = { let mut producers = dispatch_tx.borrow_mut(); producers[target_idx].try_push(pending) };
                                match push_result {
                                    Ok(()) => { spsc_notifiers[target].notify_one(); break; }
                                    Err(val) => { pending = val; tokio::task::yield_now().await; }
                                }
                            }
                        }
                        reply_futures.push((meta, target));
                    }
                    let proto_ver = protocol_version;
                    for (meta, target) in reply_futures {
                        let shard_responses = response_pool.future_for(target).await;
                        for ((resp_idx, aof_bytes, cmd_name), resp) in meta.into_iter().zip(shard_responses) {
                            if let Some(bytes) = aof_bytes {
                                if !matches!(resp, Frame::Error(_)) {
                                    if let Some(ref tx) = aof_tx { let _ = tx.try_send(AofMessage::Append(bytes)); }
                                }
                            }
                            responses[resp_idx] = apply_resp3_conversion(&cmd_name, resp, proto_ver);
                        }
                    }
                }

                // Phase 3: Flush accumulated PUBLISH batches as PubSubPublishBatch messages
                if !publish_batches.is_empty() {
                    let mut batch_slots: Vec<(std::sync::Arc<crate::shard::dispatch::PubSubResponseSlot>, std::sync::Arc<Vec<std::sync::atomic::AtomicI64>>, Vec<usize>)> = Vec::new();
                    {
                        let mut producers = dispatch_tx.borrow_mut();
                        for (target, entries) in publish_batches.drain() {
                            let n = entries.len();
                            let slot = std::sync::Arc::new(crate::shard::dispatch::PubSubResponseSlot::new(1));
                            let counts: std::sync::Arc<Vec<std::sync::atomic::AtomicI64>> = std::sync::Arc::new(
                                (0..n).map(|_| std::sync::atomic::AtomicI64::new(0)).collect()
                            );
                            let resp_indices: Vec<usize> = entries.iter().map(|(idx, _, _)| *idx).collect();
                            let pairs: Vec<(Bytes, Bytes)> = entries.into_iter().map(|(_, ch, msg)| (ch, msg)).collect();

                            let idx = ChannelMesh::target_index(shard_id, target);
                            let batch_msg = ShardMessage::PubSubPublishBatch {
                                pairs,
                                slot: slot.clone(),
                                counts: counts.clone(),
                            };
                            if producers[idx].try_push(batch_msg).is_ok() {
                                spsc_notifiers[target].notify_one();
                            } else {
                                slot.add(0); // push failed, mark as done
                            }
                            batch_slots.push((slot, counts, resp_indices));
                        }
                    }
                    // Resolve all batch slots
                    for (slot, counts, resp_indices) in &batch_slots {
                        while !slot.is_ready() {
                            tokio::task::yield_now().await;
                        }
                        for (i, resp_idx) in resp_indices.iter().enumerate() {
                            let remote_count = counts[i].load(std::sync::atomic::Ordering::Relaxed);
                            if remote_count > 0 {
                                if let Frame::Integer(ref mut total) = responses[*resp_idx] { *total += remote_count; }
                            }
                        }
                    }
                }

                arena.reset();

                write_buf.clear();
                for response in &responses {
                    if protocol_version >= 3 {
                        crate::protocol::serialize_resp3(response, &mut write_buf);
                    } else {
                        crate::protocol::serialize(response, &mut write_buf);
                    }
                }
                if stream.write_all(&write_buf).await.is_err() {
                    return (HandlerResult::Done, None);
                }

                // Check if migration was triggered during frame processing.
                // All responses for the current batch have been written, so the
                // client sees no interruption -- TCP socket stays open.
                if let Some(target_shard) = migration_target {
                    let migrated_state = MigratedConnectionState {
                        selected_db,
                        authenticated,
                        client_name: client_name.clone(),
                        protocol_version,
                        current_user: current_user.clone(),
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

    // Clean up pub/sub subscriptions on disconnect
    if subscriber_id > 0 {
        let removed_channels = {
            pubsub_registry
                .write()
                .unwrap()
                .unsubscribe_all(subscriber_id)
        };
        let removed_patterns = {
            pubsub_registry
                .write()
                .unwrap()
                .punsubscribe_all(subscriber_id)
        };
        // Direct shared-write: propagate unsubscribe to all shards' remote subscriber maps
        for ch in removed_channels {
            for target in 0..num_shards {
                if target == shard_id {
                    continue;
                }
                all_remote_sub_maps[target]
                    .write()
                    .unwrap()
                    .remove(&ch, shard_id, false);
            }
        }
        for pat in removed_patterns {
            for target in 0..num_shards {
                if target == shard_id {
                    continue;
                }
                all_remote_sub_maps[target]
                    .write()
                    .unwrap()
                    .remove(&pat, shard_id, true);
            }
        }
        // Remove affinity on disconnect (no subscriptions remain)
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            affinity_tracker.write().unwrap().remove(&addr.ip());
        }
    }

    if tracking_state.enabled {
        tracking_table.borrow_mut().untrack_all(client_id);
    }

    (HandlerResult::Done, None)
}
