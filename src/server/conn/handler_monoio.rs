// Note: some imports/variables may be conditionally used across feature flags
//! Monoio connection handler using ownership-based I/O (AsyncReadRent/AsyncWriteRent).
//!
//! Extracted from `server/connection.rs` (Plan 48-02).

use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
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
use crate::pubsub::subscriber::Subscriber;
use crate::pubsub::{self, PubSubRegistry};
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::entry::CachedClock;
use crate::storage::eviction::try_evict_if_needed;
use crate::tracking::{TrackingState, TrackingTable};

use super::affinity::{AffinityTracker, MigratedConnectionState};
use super::{
    apply_resp3_conversion, convert_blocking_to_nonblocking, execute_transaction_sharded,
    extract_bytes, extract_command, extract_primary_key, handle_blocking_command_monoio,
    handle_config, is_multi_key_command, try_inline_dispatch_loop,
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
pub async fn handle_connection_sharded_monoio<
    S: monoio::io::AsyncReadRent + monoio::io::AsyncWriteRent,
>(
    mut stream: S,
    peer_addr: String,
    shard_databases: Arc<ShardDatabases>,
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
    can_migrate: bool,
    initial_read_buf: BytesMut,
    pending_wakers: Rc<RefCell<Vec<std::task::Waker>>>,
    migrated_state: Option<&MigratedConnectionState>,
) -> (MonoioHandlerResult, Option<S>) {
    use monoio::io::AsyncWriteRentExt;

    let mut read_buf = if initial_read_buf.is_empty() {
        BytesMut::with_capacity(8192)
    } else {
        let mut buf = initial_read_buf;
        buf.reserve(8192);
        buf
    };
    let mut write_buf = BytesMut::with_capacity(8192);
    let mut codec = RespCodec::default();
    let mut selected_db: usize = migrated_state.map_or(0, |s| s.selected_db);
    let db_count = shard_databases.db_count();

    // Connection-level state — restored from migration or defaults for fresh connections.
    let mut protocol_version: u8 = migrated_state.map_or(2, |s| s.protocol_version);
    let mut authenticated = migrated_state.map_or(requirepass.is_none(), |s| s.authenticated);
    let mut current_user: String = migrated_state.map_or_else(|| "default".to_string(), |s| s.current_user.clone());
    let acl_max_len = runtime_config
        .read()
        .map(|cfg| cfg.acllog_max_len)
        .unwrap_or(128);
    let mut acl_log = crate::acl::AclLog::new(acl_max_len);
    let mut tracking_state = TrackingState::default();
    let mut tracking_rx: Option<channel::MpscReceiver<Frame>> = None;
    let mut client_name: Option<Bytes> = migrated_state.and_then(|s| s.client_name.clone());
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
        Vec<(usize, std::sync::Arc<Frame>, Option<Bytes>, Bytes)>,
    > = HashMap::with_capacity(num_shards);
    let mut reply_futures: Vec<(Vec<(usize, Option<Bytes>, Bytes)>, usize)> =
        Vec::with_capacity(num_shards);

    // Pre-allocate frames Vec outside the loop; reused via .clear() each iteration.
    let mut frames: Vec<Frame> = Vec::with_capacity(64);

    // Pre-allocated response slots for zero-allocation cross-shard dispatch.
    // Monoio uses oneshot channels (not ResponseSlotPool) for cross-shard writes.
    // ResponseSlotPool's AtomicWaker doesn't work with monoio's !Send executor.

    // Connection affinity: only track when multi-shard AND migration is possible (plain TCP).
    // Single-shard has no cross-shard traffic; TLS connections cannot transfer session state.
    let mut affinity_tracker = if num_shards > 1 && can_migrate {
        Some(AffinityTracker::new(shard_id, num_shards))
    } else {
        None
    };

    // Migration target: set when AffinityTracker triggers, acted on after batch response flush.
    let mut migration_target: Option<usize> = None;

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
                                                        if wr.is_err() { return (MonoioHandlerResult::Done, None); }
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
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
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
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
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
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for ch in &removed {
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::unsubscribe_response(ch, subscription_count);
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
                                                                pubsub_registry.borrow_mut().unsubscribe(channel.as_ref(), subscriber_id);
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::unsubscribe_response(&channel, subscription_count);
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
                                                                let acl_guard = acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&current_user, pattern.as_ref())
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
                                                            let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                            pubsub_registry.borrow_mut().psubscribe(pattern.clone(), sub);
                                                            subscription_count += 1;
                                                            let resp = pubsub::psubscribe_response(&pattern, subscription_count);
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
                                                        let removed = pubsub_registry.borrow_mut().punsubscribe_all(subscriber_id);
                                                        if removed.is_empty() {
                                                            subscription_count = pubsub_registry.borrow().total_subscription_count(subscriber_id);
                                                            let resp = pubsub::punsubscribe_response(&Bytes::from_static(b""), subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for pat in &removed {
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::punsubscribe_response(pat, subscription_count);
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
                                                                pubsub_registry.borrow_mut().punsubscribe(pattern.as_ref(), subscriber_id);
                                                                subscription_count = subscription_count.saturating_sub(1);
                                                                let resp = pubsub::punsubscribe_response(&pattern, subscription_count);
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
                let mut guard = shard_databases.write_db(shard_id, selected_db);
                guard.refresh_now_from_cache(&cached_clock);
            }
            let inlined = try_inline_dispatch_loop(
                &mut read_buf,
                &mut write_buf,
                &shard_databases,
                shard_id,
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
                Err(_) => return (MonoioHandlerResult::Done, None), // parse error, close connection
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
            let mut guard = shard_databases.write_db(shard_id, selected_db);
            guard.refresh_now_from_cache(&cached_clock);
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
                    let mut guard = shard_databases.write_db(shard_id, selected_db);
                    let db_count = shard_databases.db_count();
                    let db = &mut guard;
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
                    let mut guard = shard_databases.write_db(shard_id, selected_db);
                    let db_count = shard_databases.db_count();
                    let db = &mut guard;
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
                    // ACL channel permission check for PUBLISH
                    if let Some(ref ch) = channel {
                        let denied = {
                            let acl_guard = acl_table.read().unwrap();
                            acl_guard.check_channel_permission(&current_user, ch.as_ref())
                        };
                        if let Some(deny_reason) = denied {
                            responses
                                .push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                            continue;
                        }
                    }
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
                        &shard_databases,
                        shard_id,
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
                        return (MonoioHandlerResult::Done, None);
                    }
                }

                let blocking_response = handle_blocking_command_monoio(
                    cmd,
                    cmd_args,
                    selected_db,
                    &shard_databases,
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
                        &shard_databases,
                        &dispatch_tx,
                        &spsc_notifiers,
                        &cached_clock,
                        &(), // monoio: coordinator uses oneshot, not response_pool
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
                        &shard_databases,
                        &dispatch_tx,
                        &spsc_notifiers,
                        &cached_clock,
                        &(), // monoio: coordinator uses oneshot, not response_pool
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
                        &shard_databases,
                        &dispatch_tx,
                        &spsc_notifiers,
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
                        shard_id,
                        num_shards,
                        selected_db,
                        &shard_databases,
                        &dispatch_tx,
                        &spsc_notifiers,
                        &cached_clock,
                        &(), // monoio: coordinator uses oneshot, not response_pool
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

            // Affinity sampling: record shard target for migration decision.
            // Migration is deferred until AFTER the current batch is fully processed.
            if let (Some(tracker), Some(target)) = (&mut affinity_tracker, target_shard) {
                if let Some(migrate_to) = tracker.record(target) {
                    if !in_multi && subscription_count == 0 && !tracking_state.enabled {
                        migration_target = Some(migrate_to);
                    }
                }
            }

            // Pre-classify write commands for AOF + tracking
            let is_write = if aof_tx.is_some() || tracking_state.enabled {
                metadata::is_write(cmd)
            } else {
                false
            };

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

                    let result = dispatch(&mut guard, cmd, cmd_args, &mut selected_db, db_count);

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
                                        &mut reg,
                                        &mut guard,
                                        selected_db,
                                        &key,
                                    );
                                } else {
                                    crate::blocking::wakeup::try_wake_zset_waiter(
                                        &mut reg,
                                        &mut guard,
                                        selected_db,
                                        &key,
                                    );
                                }
                            }
                        }
                    }

                    drop(guard);

                    // Track key on write / invalidate tracked keys
                    if tracking_state.enabled && !matches!(response, Frame::Error(_)) {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            let senders =
                                tracking_table.borrow_mut().invalidate_key(&key, client_id);
                            if !senders.is_empty() {
                                let push = crate::tracking::invalidation::invalidation_push(&[key]);
                                for tx in senders {
                                    let _ = tx.try_send(push.clone());
                                }
                            }
                        }
                    }
                    let response = apply_resp3_conversion(cmd, response, protocol_version);
                    responses.push(response);
                } else {
                    // READ PATH: shared lock — no contention with other shards' reads
                    let guard = shard_databases.read_db(shard_id, selected_db);
                    let now_ms = cached_clock.ms();
                    let result =
                        dispatch_read(&guard, cmd, cmd_args, now_ms, &mut selected_db, db_count);
                    drop(guard);

                    let response = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => {
                            should_quit = true;
                            f
                        }
                    };

                    // Track key on local read
                    if tracking_state.enabled && !tracking_state.bcast {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            tracking_table.borrow_mut().track_key(
                                client_id,
                                &key,
                                tracking_state.noloop,
                            );
                        }
                    }
                    let response = apply_resp3_conversion(cmd, response, protocol_version);
                    responses.push(response);
                } // end read/write split

            // (tracking and response push handled inside read/write branches above)
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
                    let result =
                        dispatch_read(&guard, cmd, cmd_args, now_ms, &mut selected_db, db_count);
                    drop(guard);
                    let response = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => {
                            should_quit = true;
                            f
                        }
                    };
                    // Client tracking for cross-shard reads
                    if tracking_state.enabled && !tracking_state.bcast {
                        if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                            tracking_table.borrow_mut().track_key(
                                client_id,
                                &key,
                                tracking_state.noloop,
                            );
                        }
                    }
                    let response = apply_resp3_conversion(cmd, response, protocol_version);
                    responses.push(response);
                    continue;
                }
                // Cross-shard write: deferred SPSC dispatch (unchanged)
                let resp_idx = responses.len();
                responses.push(Frame::Null); // placeholder, filled after batch dispatch
                // Pre-compute AOF bytes before moving frame into Arc
                let aof_bytes = if aof_tx.is_some() && metadata::is_write(cmd) {
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

        // Phase 2: Dispatch all deferred remote commands as batched PipelineBatch
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
                                tracing::trace!(
                                    "Shard {}: pushed PipelineBatch to shard {}, notifying",
                                    shard_id,
                                    target
                                );
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
                    shard_id
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
                            if let Some(ref tx) = aof_tx {
                                let _ = tx.try_send(AofMessage::Append(bytes));
                            }
                        }
                    }
                    let resp = apply_resp3_conversion(&cmd_name, resp, protocol_version);
                    responses[resp_idx] = resp;
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

    (MonoioHandlerResult::Done, None)
}
