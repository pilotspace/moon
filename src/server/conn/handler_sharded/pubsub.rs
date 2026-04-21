//! Pub/sub command handlers: subscriber-mode loop, SUBSCRIBE/PSUBSCRIBE,
//! PUBLISH, UNSUBSCRIBE/PUNSUBSCRIBE, and PUBSUB introspection.

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;

use crate::protocol::Frame;
use crate::pubsub::{self, subscriber::Subscriber};
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::util::{
    extract_bytes, extract_command, propagate_subscription, unpropagate_subscription,
};

/// Signal from subscriber-mode or subscribe-entry helpers to the outer loop.
pub(super) enum SubscriberAction {
    /// Normal: re-enter the main loop (connection still in subscriber mode or exited it).
    Continue,
    /// Break the outer connection loop (QUIT, stream error, shutdown).
    BreakOuter,
    /// Return `(HandlerResult::Done, None)` immediately (unrecoverable parse error).
    EarlyReturn,
}

/// Run one iteration of the subscriber-mode select loop.
///
/// Called when `conn.subscription_count > 0`. Processes incoming client commands
/// (limited to (P)(UN)SUBSCRIBE, PING, QUIT, RESET) and published messages.
///
/// Returns `SubscriberAction` to tell the caller what to do next.
pub(super) async fn run_subscriber_step<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    parse_config: &crate::protocol::ParseConfig,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    shutdown: &CancellationToken,
) -> SubscriberAction {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[allow(clippy::unwrap_used)]
    // conn.pubsub_rx is always Some when conn.subscription_count > 0
    let rx = conn.pubsub_rx.as_mut().unwrap();
    tokio::select! {
        n = stream.read_buf(read_buf) => {
            match n {
                Ok(0) | Err(_) => return SubscriberAction::BreakOuter,
                Ok(_) => {}
            }
            let mut sub_break = false;
            loop {
                match crate::protocol::parse(read_buf, parse_config) {
                    Ok(Some(frame)) => {
                        if let Some((cmd, cmd_args)) = extract_command(&frame) {
                            if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") {
                                if cmd_args.is_empty() {
                                    let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'subscribe' command"));
                                    write_buf.clear();
                                    crate::protocol::serialize(&err, write_buf);
                                    if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                    continue;
                                }
                                for arg in cmd_args {
                                    if let Some(ch) = extract_bytes(arg) {
                                        #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                        let acl_deny = { ctx.acl_table.read().unwrap().check_channel_permission(&conn.current_user, ch.as_ref()) };
                                        if let Some(reason) = acl_deny {
                                            let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                            write_buf.clear();
                                            crate::protocol::serialize(&err, write_buf);
                                            if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                            continue;
                                        }
                                        #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some in subscriber mode
                                        let sub = Subscriber::with_protocol(
                                            conn.pubsub_tx.clone().unwrap(),
                                            conn.subscriber_id,
                                            conn.protocol_version >= 3,
                                        );
                                        { ctx.pubsub_registry.write().subscribe(ch.clone(), sub); }
                                        conn.subscription_count += 1;
                                        // Register pub/sub affinity for this client IP
                                        if conn.subscription_count == 1 {
                                            if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                            }
                                        }
                                        propagate_subscription(&ctx.all_remote_sub_maps, &ch, ctx.shard_id, ctx.num_shards, false);
                                        write_buf.clear();
                                        let resp = pubsub::subscribe_response(&ch, conn.subscription_count);
                                        crate::protocol::serialize(&resp, write_buf);
                                        if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                    }
                                }
                            } else if cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                                if cmd_args.is_empty() {
                                    let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'psubscribe' command"));
                                    write_buf.clear();
                                    crate::protocol::serialize(&err, write_buf);
                                    if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                    continue;
                                }
                                for arg in cmd_args {
                                    if let Some(pat) = extract_bytes(arg) {
                                        #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                        let acl_deny = { ctx.acl_table.read().unwrap().check_channel_permission(&conn.current_user, pat.as_ref()) };
                                        if let Some(reason) = acl_deny {
                                            let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                                            write_buf.clear();
                                            crate::protocol::serialize(&err, write_buf);
                                            if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                            continue;
                                        }
                                        #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some in subscriber mode
                                        let sub = Subscriber::with_protocol(
                                            conn.pubsub_tx.clone().unwrap(),
                                            conn.subscriber_id,
                                            conn.protocol_version >= 3,
                                        );
                                        { ctx.pubsub_registry.write().psubscribe(pat.clone(), sub); }
                                        conn.subscription_count += 1;
                                        // Register pub/sub affinity for this client IP
                                        if conn.subscription_count == 1 {
                                            if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                            }
                                        }
                                        propagate_subscription(&ctx.all_remote_sub_maps, &pat, ctx.shard_id, ctx.num_shards, true);
                                        write_buf.clear();
                                        let resp = pubsub::psubscribe_response(&pat, conn.subscription_count);
                                        crate::protocol::serialize(&resp, write_buf);
                                        if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                    }
                                }
                            } else if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
                                if cmd_args.is_empty() {
                                    let removed = { ctx.pubsub_registry.write().unsubscribe_all(conn.subscriber_id) };
                                    if removed.is_empty() {
                                        conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                        write_buf.clear();
                                        crate::protocol::serialize(&pubsub::unsubscribe_response(&Bytes::from_static(b""), conn.subscription_count), write_buf);
                                        if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                    } else {
                                        for ch in &removed {
                                            conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                            unpropagate_subscription(&ctx.all_remote_sub_maps, ch, ctx.shard_id, ctx.num_shards, false);
                                            write_buf.clear();
                                            crate::protocol::serialize(&pubsub::unsubscribe_response(ch, conn.subscription_count), write_buf);
                                            if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                        }
                                    }
                                } else {
                                    for arg in cmd_args {
                                        if let Some(ch) = extract_bytes(arg) {
                                            { ctx.pubsub_registry.write().unsubscribe(ch.as_ref(), conn.subscriber_id); }
                                            conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                            unpropagate_subscription(&ctx.all_remote_sub_maps, &ch, ctx.shard_id, ctx.num_shards, false);
                                            write_buf.clear();
                                            crate::protocol::serialize(&pubsub::unsubscribe_response(&ch, conn.subscription_count), write_buf);
                                            if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                        }
                                    }
                                }
                            } else if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                                if cmd_args.is_empty() {
                                    let removed = { ctx.pubsub_registry.write().punsubscribe_all(conn.subscriber_id) };
                                    if removed.is_empty() {
                                        conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                        write_buf.clear();
                                        crate::protocol::serialize(&pubsub::punsubscribe_response(&Bytes::from_static(b""), conn.subscription_count), write_buf);
                                        if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                    } else {
                                        for pat in &removed {
                                            conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                            unpropagate_subscription(&ctx.all_remote_sub_maps, pat, ctx.shard_id, ctx.num_shards, true);
                                            write_buf.clear();
                                            crate::protocol::serialize(&pubsub::punsubscribe_response(pat, conn.subscription_count), write_buf);
                                            if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                        }
                                    }
                                } else {
                                    for arg in cmd_args {
                                        if let Some(pat) = extract_bytes(arg) {
                                            { ctx.pubsub_registry.write().punsubscribe(pat.as_ref(), conn.subscriber_id); }
                                            conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                            unpropagate_subscription(&ctx.all_remote_sub_maps, &pat, ctx.shard_id, ctx.num_shards, true);
                                            write_buf.clear();
                                            crate::protocol::serialize(&pubsub::punsubscribe_response(&pat, conn.subscription_count), write_buf);
                                            if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                                        }
                                    }
                                }
                            } else if cmd.eq_ignore_ascii_case(b"PING") {
                                write_buf.clear();
                                let resp = Frame::Array(crate::framevec![
                                    Frame::BulkString(Bytes::from_static(b"pong")),
                                    Frame::BulkString(Bytes::from_static(b"")),
                                ]);
                                crate::protocol::serialize(&resp, write_buf);
                                if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                            } else if cmd.eq_ignore_ascii_case(b"QUIT") {
                                write_buf.clear();
                                crate::protocol::serialize(&Frame::SimpleString(Bytes::from_static(b"OK")), write_buf);
                                let _ = stream.write_all(write_buf).await;
                                sub_break = true;
                                break;
                            } else if cmd.eq_ignore_ascii_case(b"RESET") {
                                { ctx.pubsub_registry.write().unsubscribe_all(conn.subscriber_id); }
                                { ctx.pubsub_registry.write().punsubscribe_all(conn.subscriber_id); }
                                conn.subscription_count = 0;
                                write_buf.clear();
                                crate::protocol::serialize(&Frame::SimpleString(Bytes::from_static(b"RESET")), write_buf);
                                if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                            } else {
                                let cmd_str = String::from_utf8_lossy(cmd);
                                let err = Frame::Error(Bytes::from(format!(
                                    "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context",
                                    cmd_str.to_lowercase()
                                )));
                                write_buf.clear();
                                crate::protocol::serialize(&err, write_buf);
                                if stream.write_all(write_buf).await.is_err() { sub_break = true; break; }
                            }
                        }
                        if conn.subscription_count == 0 { break; }
                    }
                    Ok(None) => break,
                    Err(_) => { return SubscriberAction::EarlyReturn; }
                }
            }
            if sub_break { return SubscriberAction::BreakOuter; }
            if conn.subscription_count == 0 { return SubscriberAction::Continue; }
        }
        msg = rx.recv_async() => {
            match msg {
                Ok(data) => {
                    // Data is pre-serialized RESP bytes — write directly
                    if stream.write_all(&data).await.is_err() { return SubscriberAction::BreakOuter; }
                }
                Err(_) => return SubscriberAction::BreakOuter,
            }
        }
        _ = shutdown.cancelled() => {
            write_buf.clear();
            crate::protocol::serialize(&Frame::Error(Bytes::from_static(b"ERR server shutting down")), write_buf);
            let _ = stream.write_all(write_buf).await;
            return SubscriberAction::BreakOuter;
        }
    }
    SubscriberAction::Continue
}

/// Handle SUBSCRIBE / PSUBSCRIBE in normal (non-subscriber) mode.
///
/// Returns `Some(SubscriberAction)` if the command was consumed, `None` otherwise.
/// The caller should `break` from the frame batch loop on `BreakOuter` (to re-enter
/// subscriber mode) or `return (HandlerResult::Done, None)` on `EarlyReturn`.
pub(super) async fn try_handle_subscribe<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
>(
    cmd: &[u8],
    cmd_args: &[Frame],
    stream: &mut S,
    write_buf: &mut BytesMut,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    responses: &mut Vec<Frame>,
) -> Option<SubscriberAction> {
    use tokio::io::AsyncWriteExt;

    if !cmd.eq_ignore_ascii_case(b"SUBSCRIBE") && !cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
        return None;
    }
    let is_pattern = cmd.eq_ignore_ascii_case(b"PSUBSCRIBE");
    let cmd_name = if is_pattern {
        "psubscribe"
    } else {
        "subscribe"
    };
    if cmd_args.is_empty() {
        responses.push(Frame::Error(Bytes::from(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd_name
        ))));
        return Some(SubscriberAction::Continue);
    }
    // Allocate pubsub channel if not yet created
    if conn.pubsub_tx.is_none() {
        let (tx, rx) = channel::mpsc_bounded::<Bytes>(256);
        conn.pubsub_tx = Some(tx);
        conn.pubsub_rx = Some(rx);
    }
    if conn.subscriber_id == 0 {
        conn.subscriber_id = crate::pubsub::next_subscriber_id();
    }
    // Flush accumulated responses before entering subscriber mode
    if !responses.is_empty() {
        write_buf.clear();
        for resp in responses.iter() {
            if conn.protocol_version >= 3 {
                crate::protocol::serialize_resp3(resp, write_buf);
            } else {
                crate::protocol::serialize(resp, write_buf);
            }
        }
        if stream.write_all(write_buf).await.is_err() {
            return Some(SubscriberAction::EarlyReturn);
        }
        responses.clear();
    }
    // Process subscribe arguments
    for arg in cmd_args {
        if let Some(ch) = extract_bytes(arg) {
            #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
            let acl_deny = {
                ctx.acl_table
                    .read()
                    .unwrap()
                    .check_channel_permission(&conn.current_user, ch.as_ref())
            };
            if let Some(reason) = acl_deny {
                write_buf.clear();
                let err = Frame::Error(Bytes::from(format!("NOPERM {}", reason)));
                crate::protocol::serialize(&err, write_buf);
                if stream.write_all(write_buf).await.is_err() {
                    return Some(SubscriberAction::EarlyReturn);
                }
                continue;
            }
            #[allow(clippy::unwrap_used)]
            // conn.pubsub_tx is set to Some just above before this loop
            let sub = Subscriber::with_protocol(
                conn.pubsub_tx.clone().unwrap(),
                conn.subscriber_id,
                conn.protocol_version >= 3,
            );
            if is_pattern {
                ctx.pubsub_registry.write().psubscribe(ch.clone(), sub);
            } else {
                ctx.pubsub_registry.write().subscribe(ch.clone(), sub);
            }
            conn.subscription_count += 1;
            // Register pub/sub affinity for this client IP
            if conn.subscription_count == 1 {
                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                    ctx.pubsub_affinity
                        .write()
                        .register(addr.ip(), ctx.shard_id);
                }
            }
            propagate_subscription(
                &ctx.all_remote_sub_maps,
                &ch,
                ctx.shard_id,
                ctx.num_shards,
                is_pattern,
            );
            write_buf.clear();
            let resp = if is_pattern {
                pubsub::psubscribe_response(&ch, conn.subscription_count)
            } else {
                pubsub::subscribe_response(&ch, conn.subscription_count)
            };
            crate::protocol::serialize(&resp, write_buf);
            if stream.write_all(write_buf).await.is_err() {
                return Some(SubscriberAction::EarlyReturn);
            }
        }
    }
    // break out of frame batch loop to re-enter main loop in subscriber mode
    Some(SubscriberAction::BreakOuter)
}

/// Handle UNSUBSCRIBE / PUNSUBSCRIBE in normal (non-subscriber) mode.
/// Returns `true` if the command was consumed (caller should `continue`).
pub(super) fn try_handle_unsubscribe(cmd: &[u8], responses: &mut Vec<Frame>) -> bool {
    if !cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") && !cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
        return false;
    }
    let is_pattern = cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE");
    let resp = if is_pattern {
        pubsub::punsubscribe_response(&Bytes::from_static(b""), 0)
    } else {
        pubsub::unsubscribe_response(&Bytes::from_static(b""), 0)
    };
    responses.push(resp);
    true
}

/// Handle PUBLISH command.
/// Returns `true` if the command was consumed (caller should `continue`).
pub(super) fn try_handle_publish(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
    publish_batches: &mut HashMap<usize, Vec<(usize, Bytes, Bytes)>>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"PUBLISH") {
        return false;
    }
    if cmd_args.len() != 2 {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'publish' command",
        )));
    } else {
        let channel_arg = extract_bytes(&cmd_args[0]);
        let message_arg = extract_bytes(&cmd_args[1]);
        // ACL channel permission check for PUBLISH
        if let Some(ref ch) = channel_arg {
            #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
            let acl_guard = ctx.acl_table.read().unwrap();
            if let Some(deny_reason) =
                acl_guard.check_channel_permission(&conn.current_user, ch.as_ref())
            {
                drop(acl_guard);
                responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                return true;
            }
        }
        match (channel_arg, message_arg) {
            (Some(ch), Some(msg)) => {
                let local_count = { ctx.pubsub_registry.write().publish(&ch, &msg) };
                // Targeted fanout: only send to shards that have subscribers
                let targets = ctx.remote_subscriber_map.read().target_shards(&ch);
                if targets.is_empty() {
                    // Fast path: no remote subscribers, return local count immediately
                    responses.push(Frame::Integer(local_count));
                } else {
                    // Filter to remote targets only (skip self)
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
    true
}

/// Handle PUBSUB introspection commands (CHANNELS, NUMSUB, NUMPAT).
/// Returns `true` if the command was consumed (caller should `continue`).
pub(super) fn try_handle_pubsub_introspection(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"PUBSUB") {
        return false;
    }
    if cmd_args.is_empty() {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'pubsub' command",
        )));
        return true;
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
            let arr: Vec<Frame> = all_channels.into_iter().map(Frame::BulkString).collect();
            responses.push(Frame::Array(arr.into()));
        }
        Some(ref sc) if sc.eq_ignore_ascii_case(b"NUMSUB") => {
            let channels: Vec<Bytes> = cmd_args[1..]
                .iter()
                .filter_map(|a| extract_bytes(a))
                .collect();
            let mut counts: HashMap<Bytes, i64> = HashMap::new();
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
    true
}
