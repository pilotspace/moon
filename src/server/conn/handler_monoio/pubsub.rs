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
    publish_batches: &mut std::collections::HashMap<usize, Vec<(usize, Bytes, Bytes, bool)>>,
) -> bool {
    // SPUBLISH shares this handler: same arity, same ACL checks, same batched
    // fan-out. Only the DESTINATION differs, and that is carried by `sharded`
    // all the way to the target shard's registry.
    let sharded = cmd.eq_ignore_ascii_case(b"SPUBLISH");
    if !sharded && !cmd.eq_ignore_ascii_case(b"PUBLISH") {
        return false;
    }
    if cmd_args.len() != 2 {
        responses.push(Frame::Error(Bytes::from(format!(
            "ERR wrong number of arguments for '{}' command",
            if sharded { "spublish" } else { "publish" }
        ))));
        return true;
    }
    let channel = extract_bytes(&cmd_args[0]);
    let message = extract_bytes(&cmd_args[1]);
    // ACL command- AND channel-permission check for PUBLISH (H-3): the
    // command-level `-@pubsub`/allow-list gate was previously skipped here —
    // only the `&pattern` channel rule was consulted — so a `-@pubsub`
    // carve-out was silently ineffective for a user with `&*`.
    {
        #[allow(clippy::unwrap_used)]
        // std RwLock: poison = prior panic = unrecoverable
        let acl_guard = ctx.acl_table.read().unwrap();
        if let Some(deny_reason) =
            acl_guard.check_command_permission(&conn.current_user, cmd, cmd_args)
        {
            responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
            return true;
        }
        if let Some(ref ch) = channel {
            if let Some(deny_reason) =
                acl_guard.check_channel_permission(&conn.current_user, ch.as_ref())
            {
                responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
                return true;
            }
        }
    }
    match (channel, message) {
        (Some(ch), Some(msg)) => {
            let local_count = if sharded {
                crate::pubsub::spublish_shared(&ctx.pubsub_registry, &ch, &msg)
            } else {
                crate::pubsub::publish_shared(&ctx.pubsub_registry, &ch, &msg)
            };
            // Targeted fanout: only send to shards that have subscribers. The
            // sharded lookup is a DIFFERENT map — a plain channel and a
            // sharded one may share a name without sharing subscribers.
            let targets = if sharded {
                ctx.remote_subscriber_map.read().shard_target_shards(&ch)
            } else {
                ctx.remote_subscriber_map.read().target_shards(&ch)
            };
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
                            sharded,
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
        responses.push(crate::pubsub::unsubscribe_none_response(0));
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
        responses.push(crate::pubsub::punsubscribe_none_response(0));
        return true;
    }
    if cmd.eq_ignore_ascii_case(b"SUNSUBSCRIBE") {
        responses.push(crate::pubsub::sunsubscribe_none_response(0));
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
    local_leg_write_idxs: &mut Vec<usize>,
    codec: &mut crate::server::codec::RespCodec,
    write_buf: &mut bytes::BytesMut,
    stream: &mut S,
) -> SubscribeResult {
    let is_sharded = cmd.eq_ignore_ascii_case(b"SSUBSCRIBE");
    if !is_sharded
        && !cmd.eq_ignore_ascii_case(b"SUBSCRIBE")
        && !cmd.eq_ignore_ascii_case(b"PSUBSCRIBE")
    {
        return SubscribeResult::NotSubscribe;
    }
    let is_pattern = cmd.eq_ignore_ascii_case(b"PSUBSCRIBE");
    if cmd_args.is_empty() {
        let cmd_name = if is_sharded {
            "ssubscribe"
        } else if is_pattern {
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
    // Command-level ACL check (H-3) BEFORE entering subscriber mode: the
    // per-channel `&pattern` check below is not enough — a `-@pubsub`
    // carve-out must block SUBSCRIBE/PSUBSCRIBE at the command level. Push a
    // NOPERM error and bail via ArgError (caller flushes it and continues,
    // never entering the subscriber loop).
    {
        #[allow(clippy::unwrap_used)]
        // std RwLock: poison = prior panic = unrecoverable
        let acl_guard = ctx.acl_table.read().unwrap();
        if let Some(deny_reason) =
            acl_guard.check_command_permission(&conn.current_user, cmd, cmd_args)
        {
            drop(acl_guard);
            responses.push(Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason))));
            return SubscribeResult::ArgError;
        }
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
    // Earlier frames in this batch may hold barrier-pending local-leg
    // writes — confirm (or fail-loud) them before this early flush.
    crate::server::conn::shared::resolve_local_leg_barrier(
        &ctx.aof_pool,
        ctx.shard_id,
        local_leg_write_idxs,
        responses,
    )
    .await;
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
            if is_sharded {
                ctx.pubsub_registry.write().ssubscribe(ch.clone(), sub);
                super::propagate_shard_subscription(
                    &ctx.all_remote_sub_maps,
                    &ch,
                    ctx.shard_id,
                    ctx.num_shards,
                );
            } else {
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
            let resp = if is_sharded {
                crate::pubsub::ssubscribe_response(&ch, conn.subscription_count)
            } else if is_pattern {
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
        Some(ref sc) if sc.eq_ignore_ascii_case(b"SHARDCHANNELS") => {
            // The SHARDED namespace only. `PUBSUB CHANNELS` must not list
            // these and this must not list plain ones — they are separate
            // maps, so the separation is structural rather than a filter.
            let pattern = if cmd_args.len() > 1 {
                extract_bytes(&cmd_args[1])
            } else {
                None
            };
            let mut all: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
            for reg in &ctx.all_pubsub_registries {
                all.extend(reg.read().active_shard_channels(pattern.as_deref()));
            }
            let arr: Vec<Frame> = all.into_iter().map(Frame::BulkString).collect();
            responses.push(Frame::Array(arr.into()));
        }
        Some(ref sc) if sc.eq_ignore_ascii_case(b"SHARDNUMSUB") => {
            let channels: Vec<Bytes> = cmd_args[1..]
                .iter()
                .filter_map(|a| extract_bytes(a))
                .collect();
            let mut counts: std::collections::HashMap<Bytes, i64> =
                std::collections::HashMap::new();
            for reg in &ctx.all_pubsub_registries {
                for (ch, n) in reg.read().shard_numsub(&channels) {
                    *counts.entry(ch).or_insert(0) += n;
                }
            }
            let mut arr: Vec<Frame> = Vec::with_capacity(channels.len() * 2);
            for ch in &channels {
                arr.push(Frame::BulkString(ch.clone()));
                arr.push(Frame::Integer(*counts.get(ch).unwrap_or(&0)));
            }
            responses.push(Frame::Array(arr.into()));
        }
        Some(ref sc) if sc.eq_ignore_ascii_case(b"NUMPAT") => {
            // DISTINCT patterns, not pattern SUBSCRIPTIONS. Summing
            // `numpat()` was wrong twice over: it counted two clients on one
            // pattern as two, and it counted one pattern twice when its
            // subscribers landed on different shard threads. Reusing the
            // INFO gather means `pubsub_patterns` and NUMPAT cannot disagree.
            let (_, patterns) = crate::pubsub::instance_pubsub_counts(&ctx.all_pubsub_registries);
            responses.push(Frame::Integer(patterns as i64));
        }
        _ => {
            let sub = subcmd.as_deref().unwrap_or(b"");
            if let Some(help) = crate::command::help_text::help_if_requested("PUBSUB", sub) {
                responses.push(help);
                return true;
            }
            // moon#670: naming the subcommand matters — the old message blended
            // "unknown" and "wrong arity" into one string, so a client could not
            // tell a typo from a mis-call, and never learned which name failed.
            responses.push(crate::command::helpers::err_unknown_subcommand(
                "PUBSUB", sub,
            ));
        }
    }
    true
}
