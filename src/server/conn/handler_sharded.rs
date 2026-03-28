#![allow(unused_imports, dead_code, unused_variables)]
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
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use crate::command::config as config_cmd;
use crate::command::connection as conn_cmd;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::pubsub::{self, PubSubRegistry};
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::storage::Database;
use crate::storage::entry::CachedClock;
use crate::storage::eviction::try_evict_if_needed;
use crate::tracking::{TrackingState, TrackingTable};

use crate::server::codec::RespCodec;
use super::{
    SharedDatabases, apply_resp3_conversion, convert_blocking_to_nonblocking, extract_command,
    execute_transaction_sharded, extract_bytes, extract_primary_key, handle_config,
    is_multi_key_command, handle_blocking_command,
};

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
                            let mut dbs = databases.borrow_mut();
                            let db_count = dbs.len();
                            let db = &mut dbs[selected_db];
                            if cmd.eq_ignore_ascii_case(b"EVAL") {
                                crate::scripting::handle_eval(
                                    &lua, &script_cache, cmd_args, db,
                                    shard_id, num_shards, selected_db, db_count,
                                )
                            } else {
                                crate::scripting::handle_evalsha(
                                    &lua, &script_cache, cmd_args, db,
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
                            let result = execute_transaction_sharded(&databases, &command_queue, selected_db, &cached_clock);
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
                        if stream.write_all(&write_buf).await.is_err() { arena.reset(); return; }
                        let blocking_response = handle_blocking_command(
                            cmd, cmd_args, selected_db, &databases, &blocking_registry,
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
                            let channel = extract_bytes(&cmd_args[0]);
                            let message = extract_bytes(&cmd_args[1]);
                            match (channel, message) {
                                (Some(ch), Some(msg)) => {
                                    let local_count = pubsub_registry.borrow_mut().publish(&ch, &msg);
                                    let mut producers = dispatch_tx.borrow_mut();
                                    for target in 0..num_shards {
                                        if target == shard_id { continue; }
                                        let idx = ChannelMesh::target_index(shard_id, target);
                                        let fanout_msg = ShardMessage::PubSubFanOut { channel: ch.clone(), message: msg.clone() };
                                        if producers[idx].try_push(fanout_msg).is_ok() { spsc_notifiers[target].notify_one(); }
                                    }
                                    drop(producers);
                                    responses.push(Frame::Integer(local_count));
                                }
                                _ => responses.push(Frame::Error(Bytes::from_static(b"ERR invalid channel or message"))),
                            }
                        }
                        continue;
                    }

                    // --- SUBSCRIBE / PSUBSCRIBE ---
                    if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
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
                        responses.push(Frame::Error(Bytes::from_static(b"ERR SUBSCRIBE not yet supported in sharded mode")));
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                        responses.push(Frame::Error(Bytes::from_static(b"ERR UNSUBSCRIBE not yet supported in sharded mode")));
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

                    // === ACL permission check ===
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

                    // --- MULTI queue mode ---
                    if in_multi {
                        command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                        continue;
                    }

                    // --- Cross-shard aggregation: KEYS, SCAN, DBSIZE ---
                    if cmd.eq_ignore_ascii_case(b"KEYS") {
                        let response = crate::shard::coordinator::coordinate_keys(cmd_args, shard_id, num_shards, selected_db, &databases, &dispatch_tx, &spsc_notifiers, &cached_clock).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"SCAN") {
                        let response = crate::shard::coordinator::coordinate_scan(cmd_args, shard_id, num_shards, selected_db, &databases, &dispatch_tx, &spsc_notifiers, &cached_clock).await;
                        responses.push(response);
                        continue;
                    }
                    if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                        let response = crate::shard::coordinator::coordinate_dbsize(shard_id, num_shards, selected_db, &databases, &dispatch_tx, &spsc_notifiers).await;
                        responses.push(response);
                        continue;
                    }

                    // --- Multi-key commands ---
                    if is_multi_key_command(cmd, cmd_args) {
                        let response = crate::shard::coordinator::coordinate_multi_key(cmd, cmd_args, shard_id, num_shards, selected_db, &databases, &dispatch_tx, &spsc_notifiers, &cached_clock).await;
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

                    let is_write = if aof_tx.is_some() || tracking_state.enabled { metadata::is_write(cmd) } else { false };
                    let aof_bytes = if is_write && aof_tx.is_some() { Some(aof::serialize_command(&frame)) } else { None };

                    if is_local {
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
                        let result = dispatch(&mut dbs[selected_db], cmd, cmd_args, &mut selected_db, db_count);
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
                                        crate::blocking::wakeup::try_wake_list_waiter(&mut reg, &mut dbs[selected_db], selected_db, &key);
                                    } else {
                                        crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, &mut dbs[selected_db], selected_db, &key);
                                    }
                                }
                            }
                        }
                        drop(dbs);
                        if let Some(bytes) = aof_bytes {
                            if !matches!(response, Frame::Error(_)) {
                                if let Some(ref tx) = aof_tx { let _ = tx.try_send(AofMessage::Append(bytes)); }
                            }
                        }
                        if tracking_state.enabled {
                            if is_write {
                                if !matches!(response, Frame::Error(_)) {
                                    if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                        let senders = tracking_table.borrow_mut().invalidate_key(&key, client_id);
                                        if !senders.is_empty() {
                                            let push = crate::tracking::invalidation::invalidation_push(&[key]);
                                            for tx in senders { let _ = tx.try_send(push.clone()); }
                                        }
                                    }
                                }
                            } else if !tracking_state.bcast {
                                if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                    tracking_table.borrow_mut().track_key(client_id, &key, tracking_state.noloop);
                                }
                            }
                        }
                        let response = apply_resp3_conversion(cmd, response, protocol_version);
                        responses.push(response);
                    } else if let Some(target) = target_shard {
                        let resp_idx = responses.len();
                        responses.push(Frame::Null);
                        let cmd_name = cmd.to_vec();
                        remote_groups.entry(target).or_default().push((resp_idx, std::sync::Arc::new(frame), aof_bytes, cmd_name));
                    }
                }

                // Phase 2: Dispatch deferred remote commands
                if !remote_groups.is_empty() {
                    let mut reply_futures: Vec<(Vec<(usize, Option<Bytes>, Vec<u8>)>, channel::OneshotReceiver<Vec<Frame>>)> = Vec::with_capacity(remote_groups.len());
                    for (target, entries) in remote_groups {
                        let (reply_tx, reply_rx) = channel::oneshot();
                        let (meta, commands): (Vec<(usize, Option<Bytes>, Vec<u8>)>, Vec<std::sync::Arc<Frame>>) =
                            entries.into_iter().map(|(idx, arc_frame, aof, cmd)| ((idx, aof, cmd), arc_frame)).unzip();
                        let msg = ShardMessage::PipelineBatch { db_index: selected_db, commands, reply_tx };
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
                        reply_futures.push((meta, reply_rx));
                    }
                    let proto_ver = protocol_version;
                    for (meta, reply_rx) in reply_futures {
                        match reply_rx.await {
                            Ok(shard_responses) => {
                                for ((resp_idx, aof_bytes, cmd_name), resp) in meta.into_iter().zip(shard_responses) {
                                    if let Some(bytes) = aof_bytes {
                                        if !matches!(resp, Frame::Error(_)) {
                                            if let Some(ref tx) = aof_tx { let _ = tx.try_send(AofMessage::Append(bytes)); }
                                        }
                                    }
                                    responses[resp_idx] = apply_resp3_conversion(&cmd_name, resp, proto_ver);
                                }
                            }
                            Err(_) => {
                                for (resp_idx, _, _) in meta {
                                    responses[resp_idx] = Frame::Error(Bytes::from_static(b"ERR cross-shard dispatch failed"));
                                }
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
                if stream.write_all(&write_buf).await.is_err() { return; }

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

    if tracking_state.enabled {
        tracking_table.borrow_mut().untrack_all(client_id);
    }
}
