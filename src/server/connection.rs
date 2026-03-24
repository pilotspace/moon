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
fn extract_bytes(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
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
) {
    let mut framed = Framed::new(stream, RespCodec::default());
    let mut selected_db: usize = 0;
    let mut authenticated = requirepass.is_none();

    // Pub/Sub connection-local state
    let mut subscription_count: usize = 0;
    let mut pubsub_rx: Option<mpsc::Receiver<Frame>> = None;
    let mut pubsub_tx: Option<mpsc::Sender<Frame>> = None;
    let mut subscriber_id: u64 = 0;

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
                                            "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context",
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
                                let response = conn_cmd::auth(cmd_args, &requirepass);
                                if response == Frame::SimpleString(Bytes::from_static(b"OK")) {
                                    authenticated = true;
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
                            responses.push(conn_cmd::auth(cmd_args, &requirepass));
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
                            for j in run_start..i {
                                let (resp_idx, ref disp_frame, _, _) = dispatchable[j];
                                let (d_cmd, d_args) = extract_command(disp_frame).unwrap();
                                let result = dispatch_read(&*guard, d_cmd, d_args, now_ms, &mut selected_db, db_count);
                                let (response, quit) = match result {
                                    DispatchResult::Response(f) => (f, false),
                                    DispatchResult::Quit(f) => (f, true),
                                };
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
                                        aof_entries.push(bytes.clone());
                                    }
                                }
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
    // Keyless commands: execute locally
    if cmd.eq_ignore_ascii_case(b"PING")
        || cmd.eq_ignore_ascii_case(b"ECHO")
        || cmd.eq_ignore_ascii_case(b"SELECT")
        || cmd.eq_ignore_ascii_case(b"QUIT")
        || cmd.eq_ignore_ascii_case(b"INFO")
        || cmd.eq_ignore_ascii_case(b"COMMAND")
        || cmd.eq_ignore_ascii_case(b"DBSIZE")
        || cmd.eq_ignore_ascii_case(b"KEYS")
        || cmd.eq_ignore_ascii_case(b"SCAN")
    {
        return None;
    }
    if args.is_empty() {
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
    if cmd.eq_ignore_ascii_case(b"MGET") || cmd.eq_ignore_ascii_case(b"MSET") {
        return true;
    }
    // DEL, UNLINK, EXISTS with multiple keys
    if args.len() > 1
        && (cmd.eq_ignore_ascii_case(b"DEL")
            || cmd.eq_ignore_ascii_case(b"UNLINK")
            || cmd.eq_ignore_ascii_case(b"EXISTS"))
    {
        return true;
    }
    false
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
    shutdown: CancellationToken,
    requirepass: Option<String>,
    aof_tx: Option<mpsc::Sender<AofMessage>>,
) {
    let mut framed = Framed::new(stream, RespCodec::default());
    let mut selected_db: usize = 0;
    let mut authenticated = requirepass.is_none();

    // Transaction (MULTI/EXEC) connection-local state
    let mut in_multi: bool = false;
    let mut command_queue: Vec<Frame> = Vec::new();

    loop {
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

                let mut responses: Vec<Frame> = Vec::with_capacity(batch.len());
                let mut should_quit = false;

                for frame in batch {
                    // --- AUTH gate ---
                    if !authenticated {
                        match extract_command(&frame) {
                            Some((cmd, cmd_args)) if cmd.eq_ignore_ascii_case(b"AUTH") => {
                                let response = conn_cmd::auth(cmd_args, &requirepass);
                                if response == Frame::SimpleString(Bytes::from_static(b"OK")) {
                                    authenticated = true;
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
                                        let _ = producers[idx].try_push(fanout_msg); // best-effort
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

                    // --- MULTI queue mode ---
                    if in_multi {
                        command_queue.push(frame);
                        responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
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

                    // Pre-serialize write commands for AOF logging (before dispatch)
                    let is_write = aof::is_write_command(cmd);
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
                        // Log successful write commands to AOF
                        if let Some(bytes) = aof_bytes {
                            if !matches!(response, Frame::Error(_)) {
                                if let Some(ref tx) = aof_tx {
                                    let _ = tx.try_send(AofMessage::Append(bytes));
                                }
                            }
                        }
                        responses.push(response);
                    } else if let Some(target) = target_shard {
                        // REMOTE DISPATCH: send via SPSC, await oneshot reply
                        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                        let msg = ShardMessage::Execute {
                            db_index: selected_db,
                            command: frame.clone(),
                            reply_tx,
                        };
                        let target_idx = ChannelMesh::target_index(shard_id, target);
                        {
                            // Spin-retry on full ring buffer (rare in practice)
                            let mut pending = msg;
                            loop {
                                let push_result = {
                                    let mut producers = dispatch_tx.borrow_mut();
                                    producers[target_idx].try_push(pending)
                                }; // borrow dropped here before yield
                                match push_result {
                                    Ok(()) => break,
                                    Err(val) => {
                                        pending = val;
                                        tokio::task::yield_now().await;
                                    }
                                }
                            }
                        }
                        // Await the reply from the target shard
                        match reply_rx.await {
                            Ok(response) => {
                                // Log successful write commands to AOF
                                if let Some(bytes) = aof_bytes {
                                    if !matches!(response, Frame::Error(_)) {
                                        if let Some(ref tx) = aof_tx {
                                            let _ = tx.try_send(AofMessage::Append(bytes));
                                        }
                                    }
                                }
                                responses.push(response);
                            }
                            Err(_) => responses.push(Frame::Error(
                                Bytes::from_static(b"ERR cross-shard dispatch failed"),
                            )),
                        }
                    }
                }

                // Write all responses
                for response in responses {
                    if framed.send(response).await.is_err() {
                        return;
                    }
                }

                if should_quit {
                    break;
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
