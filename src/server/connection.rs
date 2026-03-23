use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::command::config as config_cmd;
use crate::command::connection as conn_cmd;
use crate::command::{dispatch, DispatchResult};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::{self, AofMessage};
use crate::protocol::Frame;
use crate::pubsub::{self, PubSubRegistry};
use crate::pubsub::subscriber::Subscriber;
use crate::storage::eviction::try_evict_if_needed;
use crate::storage::Database;

use super::codec::RespCodec;

/// Extract command name (uppercased) and args from a Frame::Array.
fn extract_command(frame: &Frame) -> Option<(Vec<u8>, &[Frame])> {
    match frame {
        Frame::Array(args) if !args.is_empty() => {
            let name = match &args[0] {
                Frame::BulkString(s) => s.to_ascii_uppercase(),
                Frame::SimpleString(s) => s.to_ascii_uppercase(),
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
pub async fn handle_connection(
    stream: TcpStream,
    db: Arc<Mutex<Vec<Database>>>,
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

    loop {
        // Subscriber mode: bidirectional select on client commands + published messages
        if subscription_count > 0 {
            let rx = pubsub_rx.as_mut().unwrap();
            tokio::select! {
                result = framed.next() => {
                    match result {
                        Some(Ok(frame)) => {
                            if let Some((ref cmd, cmd_args)) = extract_command(&frame) {
                                match cmd.as_slice() {
                                    b"SUBSCRIBE" => {
                                        if cmd_args.is_empty() {
                                            let _ = framed.send(Frame::Error(
                                                Bytes::from_static(b"ERR wrong number of arguments for 'subscribe' command"),
                                            )).await;
                                            continue;
                                        }
                                        for arg in cmd_args {
                                            if let Some(channel) = extract_bytes(arg) {
                                                let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                pubsub_registry.lock().unwrap().subscribe(channel.clone(), sub);
                                                subscription_count += 1;
                                                if framed.send(pubsub::subscribe_response(&channel, subscription_count)).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    b"UNSUBSCRIBE" => {
                                        if cmd_args.is_empty() {
                                            // Unsubscribe from all channels
                                            let removed = pubsub_registry.lock().unwrap().unsubscribe_all(subscriber_id);
                                            if removed.is_empty() {
                                                // No channels, send response with count 0
                                                subscription_count = pubsub_registry.lock().unwrap().total_subscription_count(subscriber_id);
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
                                                    pubsub_registry.lock().unwrap().unsubscribe(channel.as_ref(), subscriber_id);
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::unsubscribe_response(&channel, subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    b"PSUBSCRIBE" => {
                                        if cmd_args.is_empty() {
                                            let _ = framed.send(Frame::Error(
                                                Bytes::from_static(b"ERR wrong number of arguments for 'psubscribe' command"),
                                            )).await;
                                            continue;
                                        }
                                        for arg in cmd_args {
                                            if let Some(pattern) = extract_bytes(arg) {
                                                let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                                pubsub_registry.lock().unwrap().psubscribe(pattern.clone(), sub);
                                                subscription_count += 1;
                                                if framed.send(pubsub::psubscribe_response(&pattern, subscription_count)).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    b"PUNSUBSCRIBE" => {
                                        if cmd_args.is_empty() {
                                            let removed = pubsub_registry.lock().unwrap().punsubscribe_all(subscriber_id);
                                            if removed.is_empty() {
                                                subscription_count = pubsub_registry.lock().unwrap().total_subscription_count(subscriber_id);
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
                                                    pubsub_registry.lock().unwrap().punsubscribe(pattern.as_ref(), subscriber_id);
                                                    subscription_count = subscription_count.saturating_sub(1);
                                                    if framed.send(pubsub::punsubscribe_response(&pattern, subscription_count)).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    b"PING" => {
                                        // In subscriber mode, PING returns Array per Redis spec
                                        let _ = framed.send(Frame::Array(vec![
                                            Frame::BulkString(Bytes::from_static(b"pong")),
                                            Frame::BulkString(Bytes::from_static(b"")),
                                        ])).await;
                                    }
                                    b"QUIT" => {
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

        // Normal mode
        tokio::select! {
            result = framed.next() => {
                match result {
                    Some(Ok(frame)) => {
                        // Check auth gate before dispatching
                        if !authenticated {
                            match extract_command(&frame) {
                                Some((ref cmd, cmd_args)) if cmd.as_slice() == b"AUTH" => {
                                    let response = conn_cmd::auth(cmd_args, &requirepass);
                                    if response == Frame::SimpleString(Bytes::from_static(b"OK")) {
                                        authenticated = true;
                                    }
                                    if framed.send(response).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }
                                Some((ref cmd, _)) if cmd.as_slice() == b"QUIT" => {
                                    let _ = framed.send(Frame::SimpleString(Bytes::from_static(b"OK"))).await;
                                    break;
                                }
                                _ => {
                                    if framed.send(Frame::Error(
                                        Bytes::from_static(b"NOAUTH Authentication required.")
                                    )).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }
                            }
                        }

                        // Handle AUTH when already authenticated
                        if let Some((ref cmd, cmd_args)) = extract_command(&frame) {
                            if cmd.as_slice() == b"AUTH" {
                                let response = conn_cmd::auth(cmd_args, &requirepass);
                                if framed.send(response).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            if cmd.as_slice() == b"BGSAVE" {
                                let response = crate::command::persistence::bgsave_start(
                                    db.clone(),
                                    config.dir.clone(),
                                    config.dbfilename.clone(),
                                );
                                if framed.send(response).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            if cmd.as_slice() == b"BGREWRITEAOF" {
                                if let Some(ref tx) = aof_tx {
                                    let response = crate::command::persistence::bgrewriteaof_start(
                                        tx,
                                        db.clone(),
                                    );
                                    if framed.send(response).await.is_err() {
                                        break;
                                    }
                                } else {
                                    if framed.send(Frame::Error(
                                        Bytes::from_static(b"ERR AOF is not enabled"),
                                    )).await.is_err() {
                                        break;
                                    }
                                }
                                continue;
                            }

                            // Intercept CONFIG command
                            if cmd.as_slice() == b"CONFIG" {
                                let response = handle_config(cmd_args, &runtime_config, &config);
                                if framed.send(response).await.is_err() {
                                    break;
                                }
                                continue;
                            }

                            // SUBSCRIBE / PSUBSCRIBE: enter subscriber mode
                            if cmd.as_slice() == b"SUBSCRIBE" || cmd.as_slice() == b"PSUBSCRIBE" {
                                if cmd_args.is_empty() {
                                    let cmd_lower = if cmd.as_slice() == b"SUBSCRIBE" { "subscribe" } else { "psubscribe" };
                                    let _ = framed.send(Frame::Error(Bytes::from(format!(
                                        "ERR wrong number of arguments for '{}' command", cmd_lower
                                    )))).await;
                                    continue;
                                }
                                // Allocate subscriber resources if not yet done
                                if pubsub_tx.is_none() {
                                    let (tx, rx) = mpsc::channel::<Frame>(256);
                                    subscriber_id = pubsub::next_subscriber_id();
                                    pubsub_tx = Some(tx);
                                    pubsub_rx = Some(rx);
                                }
                                let is_pattern = cmd.as_slice() == b"PSUBSCRIBE";
                                for arg in cmd_args {
                                    if let Some(channel_or_pattern) = extract_bytes(arg) {
                                        let sub = Subscriber::new(pubsub_tx.clone().unwrap(), subscriber_id);
                                        {
                                            let mut registry = pubsub_registry.lock().unwrap();
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
                                            break;
                                        }
                                    }
                                }
                                continue; // next iteration enters subscriber mode
                            }

                            // PUBLISH: fan out through registry (not a write command for AOF)
                            if cmd.as_slice() == b"PUBLISH" {
                                if cmd_args.len() != 2 {
                                    let _ = framed.send(Frame::Error(
                                        Bytes::from_static(b"ERR wrong number of arguments for 'publish' command"),
                                    )).await;
                                    continue;
                                }
                                let channel = match extract_bytes(&cmd_args[0]) {
                                    Some(b) => b,
                                    None => {
                                        let _ = framed.send(Frame::Error(
                                            Bytes::from_static(b"ERR invalid channel"),
                                        )).await;
                                        continue;
                                    }
                                };
                                let message = match extract_bytes(&cmd_args[1]) {
                                    Some(b) => b,
                                    None => {
                                        let _ = framed.send(Frame::Error(
                                            Bytes::from_static(b"ERR invalid message"),
                                        )).await;
                                        continue;
                                    }
                                };
                                let count = pubsub_registry.lock().unwrap().publish(&channel, &message);
                                if framed.send(Frame::Integer(count)).await.is_err() {
                                    break;
                                }
                                continue;
                            }

                            // UNSUBSCRIBE / PUNSUBSCRIBE when not subscribed
                            if cmd.as_slice() == b"UNSUBSCRIBE" {
                                let channel_name = if !cmd_args.is_empty() {
                                    extract_bytes(&cmd_args[0]).unwrap_or(Bytes::from_static(b""))
                                } else {
                                    Bytes::from_static(b"")
                                };
                                let _ = framed.send(pubsub::unsubscribe_response(&channel_name, 0)).await;
                                continue;
                            }
                            if cmd.as_slice() == b"PUNSUBSCRIBE" {
                                let pattern_name = if !cmd_args.is_empty() {
                                    extract_bytes(&cmd_args[0]).unwrap_or(Bytes::from_static(b""))
                                } else {
                                    Bytes::from_static(b"")
                                };
                                let _ = framed.send(pubsub::punsubscribe_response(&pattern_name, 0)).await;
                                continue;
                            }
                        }

                        // Check if this is a write command BEFORE dispatch (which consumes the frame)
                        let is_write = extract_command(&frame)
                            .map(|(ref cmd, _)| aof::is_write_command(cmd))
                            .unwrap_or(false);

                        // If this is a write command, check eviction first
                        if is_write {
                            let eviction_result = {
                                let rt = runtime_config.read().unwrap();
                                let mut dbs = db.lock().unwrap();
                                try_evict_if_needed(&mut dbs[selected_db], &rt)
                            };
                            if let Err(oom_frame) = eviction_result {
                                if framed.send(oom_frame).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        }

                        // Serialize for AOF before dispatch consumes the frame
                        let aof_bytes = if is_write && aof_tx.is_some() {
                            let mut buf = BytesMut::new();
                            crate::protocol::serialize::serialize(&frame, &mut buf);
                            Some(buf.freeze())
                        } else {
                            None
                        };

                        // Normal dispatch
                        let result = {
                            let mut dbs = db.lock().unwrap();
                            let db_count = dbs.len();
                            dispatch(&mut dbs[selected_db], frame, &mut selected_db, db_count)
                        };
                        let (response, should_quit) = match result {
                            DispatchResult::Response(f) => (f, false),
                            DispatchResult::Quit(f) => (f, true),
                        };

                        // Log to AOF after successful dispatch (not error responses)
                        if let Some(bytes) = aof_bytes {
                            if !matches!(&response, Frame::Error(_)) {
                                if let Some(ref tx) = aof_tx {
                                    let _ = tx.send(AofMessage::Append(bytes)).await;
                                }
                                // Increment change counter for auto-save
                                if let Some(ref counter) = change_counter {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }

                        if framed.send(response).await.is_err() {
                            break;
                        }
                        if should_quit {
                            break;
                        }
                    }
                    Some(Err(_)) => break,
                    None => break,
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
        let mut registry = pubsub_registry.lock().unwrap();
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
        Frame::BulkString(s) => s.to_ascii_uppercase(),
        Frame::SimpleString(s) => s.to_ascii_uppercase(),
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR unknown subcommand for CONFIG",
            ));
        }
    };

    let sub_args = &args[1..];

    match subcmd.as_slice() {
        b"GET" => {
            let rt = runtime_config.read().unwrap();
            config_cmd::config_get(&rt, server_config, sub_args)
        }
        b"SET" => {
            let mut rt = runtime_config.write().unwrap();
            config_cmd::config_set(&mut rt, sub_args)
        }
        _ => Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}'. Try CONFIG GET, CONFIG SET.",
            String::from_utf8_lossy(&subcmd)
        ))),
    }
}
