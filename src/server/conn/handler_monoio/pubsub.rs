//! Pub/sub command handlers: PUBLISH, UNSUBSCRIBE/PUNSUBSCRIBE (no-op in normal mode),
//! and PUBSUB introspection (CHANNELS, NUMSUB, NUMPAT).
//!
//! The subscriber-mode select loop and SUBSCRIBE/PSUBSCRIBE entry remain in mod.rs
//! because monoio's ownership I/O model (AsyncReadRent/AsyncWriteRent) requires the
//! stream to be passed by value into `monoio::select!`, which is tightly coupled to
//! the connection loop state machine.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::util::extract_bytes;

/// Handle PUBLISH command. Returns `true` if the command was consumed.
pub(super) fn try_handle_publish(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
    publish_batches: &mut std::collections::HashMap<usize, Vec<(usize, Bytes, Bytes)>>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"PUBLISH") {
        return false;
    }
    if cmd_args.len() != 2 {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'publish' command",
        )));
        return true;
    }
    let channel = extract_bytes(&cmd_args[0]);
    let message = extract_bytes(&cmd_args[1]);
    // ACL channel permission check for PUBLISH
    if let Some(ref ch) = channel {
        let denied = {
            #[allow(clippy::unwrap_used)]
            // std RwLock: poison = prior panic = unrecoverable
            let acl_guard = ctx.acl_table.read().unwrap();
            acl_guard.check_channel_permission(&conn.current_user, ch.as_ref())
        };
        if let Some(deny_reason) = denied {
            responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
            return true;
        }
    }
    match (channel, message) {
        (Some(ch), Some(msg)) => {
            let local_count = { ctx.pubsub_registry.write().publish(&ch, &msg) };
            // Targeted fanout: only send to shards that have subscribers
            let targets = ctx.remote_subscriber_map.read().target_shards(&ch);
            if targets.is_empty() {
                // Fast path: no remote subscribers
                responses.push(Frame::Integer(local_count));
            } else {
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
    true
}

/// Handle UNSUBSCRIBE / PUNSUBSCRIBE in normal mode (no-op, not in subscriber mode).
/// Returns `true` if the command was consumed.
pub(super) fn try_handle_unsubscribe(cmd: &[u8], responses: &mut Vec<Frame>) -> bool {
    if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
        responses.push(crate::pubsub::unsubscribe_response(
            &Bytes::from_static(b""),
            0,
        ));
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
        responses.push(crate::pubsub::punsubscribe_response(
            &Bytes::from_static(b""),
            0,
        ));
        return true;
    }
    false
}

/// Result of SUBSCRIBE/PSUBSCRIBE dispatch in normal mode.
pub(super) enum SubscribeResult {
    /// Not a SUBSCRIBE/PSUBSCRIBE command.
    NotSubscribe,
    /// Argument validation failed; error pushed to responses. Caller should `continue`.
    ArgError,
    /// Subscription registered, responses encoded into write_buf.
    /// Caller must flush write_buf and `break` the frame loop.
    Subscribed,
    /// Write error during flush. Caller should return Done.
    WriteError,
}

/// Handle SUBSCRIBE / PSUBSCRIBE entry in normal (non-subscriber) mode.
///
/// Allocates pubsub channel if needed, registers subscriptions, encodes responses
/// into write_buf, and flushes. Returns `SubscribeResult` to tell the caller
/// whether to break or continue.
pub(super) async fn try_handle_subscribe_entry<S: monoio::io::AsyncWriteRent>(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut super::super::core::ConnectionState,
    ctx: &super::super::core::ConnectionContext,
    peer_addr: &str,
    responses: &mut Vec<Frame>,
    codec: &mut crate::server::codec::RespCodec,
    write_buf: &mut bytes::BytesMut,
    stream: &mut S,
) -> SubscribeResult {
    if !cmd.eq_ignore_ascii_case(b"SUBSCRIBE") && !cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
        return SubscribeResult::NotSubscribe;
    }
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
        return SubscribeResult::ArgError;
    }
    // Allocate pubsub channel if not yet created
    if conn.pubsub_tx.is_none() {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(256);
        conn.pubsub_tx = Some(tx);
        conn.pubsub_rx = Some(rx);
    }
    if conn.subscriber_id == 0 {
        conn.subscriber_id = crate::pubsub::next_subscriber_id();
    }
    // Flush accumulated responses before entering subscriber mode
    for resp in &*responses {
        codec.encode_frame(resp, write_buf);
    }
    for arg in cmd_args {
        if let Some(ch) = extract_bytes(arg) {
            // ACL channel permission check
            {
                #[allow(clippy::unwrap_used)]
                // std RwLock: poison = prior panic = unrecoverable
                let acl_guard = ctx.acl_table.read().unwrap();
                if let Some(deny_reason) =
                    acl_guard.check_channel_permission(&conn.current_user, ch.as_ref())
                {
                    drop(acl_guard);
                    let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                    codec.encode_frame(&err, write_buf);
                    continue;
                }
            }
            #[allow(clippy::unwrap_used)]
            // conn.pubsub_tx is set to Some just above before this loop
            let sub = crate::pubsub::subscriber::Subscriber::with_protocol(
                conn.pubsub_tx.clone().unwrap(),
                conn.subscriber_id,
                conn.protocol_version >= 3,
            );
            if is_pattern {
                ctx.pubsub_registry.write().psubscribe(ch.clone(), sub);
            } else {
                ctx.pubsub_registry.write().subscribe(ch.clone(), sub);
            }
            super::propagate_subscription(
                &ctx.all_remote_sub_maps,
                &ch,
                ctx.shard_id,
                ctx.num_shards,
                is_pattern,
            );
            conn.subscription_count += 1;
            // Register pub/sub affinity for this client IP
            if conn.subscription_count == 1 {
                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                    ctx.pubsub_affinity
                        .write()
                        .register(addr.ip(), ctx.shard_id);
                }
            }
            let resp = if is_pattern {
                crate::pubsub::psubscribe_response(&ch, conn.subscription_count)
            } else {
                crate::pubsub::subscribe_response(&ch, conn.subscription_count)
            };
            codec.encode_frame(&resp, write_buf);
        }
    }
    // Flush responses and re-enter loop (next iteration enters subscriber mode)
    if !write_buf.is_empty() {
        use monoio::io::AsyncWriteRentExt;
        let data = write_buf.split().freeze();
        let (result, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
        if result.is_err() {
            return SubscribeResult::WriteError;
        }
    }
    responses.clear();
    SubscribeResult::Subscribed
}

/// Handle PUBSUB introspection subcommands (CHANNELS, NUMSUB, NUMPAT).
/// Returns `true` if the command was consumed.
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
            let mut counts: std::collections::HashMap<Bytes, i64> =
                std::collections::HashMap::new();
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
