use std::cell::RefCell;
use std::rc::Rc;
#[cfg(feature = "runtime-monoio")]
use std::sync::Arc;

use bytes::Bytes;
#[cfg(feature = "runtime-monoio")]
use bytes::BytesMut;
use futures::StreamExt;
use ringbuf::HeapProd;
use ringbuf::traits::Producer;

use crate::framevec;
use crate::protocol::Frame;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;

use super::util::extract_bytes;

/// Convert a blocking command to its non-blocking equivalent for MULTI/EXEC.
/// BLPOP key [key ...] timeout -> LPOP key [key ...]
/// BRPOP key [key ...] timeout -> RPOP key [key ...]
/// BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout -> LMOVE src dst LEFT|RIGHT LEFT|RIGHT
/// BZPOPMIN key [key ...] timeout -> ZPOPMIN key [key ...]
/// BZPOPMAX key [key ...] timeout -> ZPOPMAX key [key ...]
pub(crate) fn convert_blocking_to_nonblocking(cmd: &[u8], args: &[Frame]) -> Frame {
    let mut new_args = Vec::new();
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"LPOP")));
        // All args except the last (timeout)
        for arg in args.iter().take(args.len().saturating_sub(1)) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"RPOP")));
        for arg in args.iter().take(args.len().saturating_sub(1)) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"LMOVE")));
        // src dst LEFT|RIGHT LEFT|RIGHT (skip timeout which is last arg)
        for arg in args.iter().take(4) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"ZPOPMIN")));
        for arg in args.iter().take(args.len().saturating_sub(1)) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"ZPOPMAX")));
        for arg in args.iter().take(args.len().saturating_sub(1)) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BLMPOP") {
        // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]
        // -> LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT n] (skip timeout = args[0])
        new_args.push(Frame::BulkString(Bytes::from_static(b"LMPOP")));
        for arg in args.iter().skip(1) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        // BRPOPLPUSH src dst timeout -> RPOPLPUSH src dst (skip timeout = args[2])
        // Actually, convert to LMOVE src dst RIGHT LEFT
        new_args.push(Frame::BulkString(Bytes::from_static(b"LMOVE")));
        // src, dst
        for arg in args.iter().take(2) {
            new_args.push(arg.clone());
        }
        new_args.push(Frame::BulkString(Bytes::from_static(b"RIGHT")));
        new_args.push(Frame::BulkString(Bytes::from_static(b"LEFT")));
    } else if cmd.eq_ignore_ascii_case(b"BZMPOP") {
        // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]
        // -> ZMPOP numkeys key [key ...] MIN|MAX [COUNT n] (skip timeout = args[0])
        new_args.push(Frame::BulkString(Bytes::from_static(b"ZMPOP")));
        for arg in args.iter().skip(1) {
            new_args.push(arg.clone());
        }
    }
    Frame::Array(new_args.into())
}

/// Handle a blocking command (BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX).
///
/// 1. Non-blocking fast path: check if data is immediately available.
/// 2. Single-key fast path: one oneshot, one local registration (zero overhead).
/// 3. Multi-key coordinator: FuturesUnordered across local + remote shards,
///    first-wakeup-wins, BlockCancel cleanup on completion/timeout/shutdown.
///
/// CRITICAL: RefCell borrows MUST be dropped before any .await point.
#[cfg(feature = "runtime-tokio")]
pub(crate) async fn handle_blocking_command(
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
    shard_databases: &std::sync::Arc<ShardDatabases>,
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
        let mut guard = shard_databases.write_db(shard_id, selected_db);
        for key in &keys {
            let result = try_immediate_pop(cmd, &mut guard, key, args);
            if let Some(frame) = result {
                return frame;
            }
        }
        drop(guard); // CRITICAL before await
    }

    let deadline = if timeout_secs > 0.0 {
        Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 = block forever
    };

    // --- Single-key fast path: one registration, direct await (zero overhead) ---
    if keys.len() == 1 {
        let target = key_to_shard(&keys[0], num_shards);
        let (reply_tx, reply_rx) = channel::oneshot::<Option<Frame>>();
        let wait_id = {
            let mut reg = blocking_registry.borrow_mut();
            let wait_id = reg.next_wait_id();
            if target == shard_id {
                // Local registration
                let entry = crate::blocking::WaitEntry {
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx,
                    deadline,
                };
                reg.register(selected_db, keys[0].clone(), entry);
            } else {
                // Remote registration via SPSC
                let mut producers = dispatch_tx.borrow_mut();
                let target_idx = ChannelMesh::target_index(shard_id, target);
                let msg = ShardMessage::BlockRegister {
                    db_index: selected_db,
                    key: keys[0].clone(),
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx,
                };
                let _ = producers[target_idx].try_push(msg);
            }
            wait_id
        }; // reg borrow dropped

        let is_remote = target != shard_id;
        let result = if let Some(dl) = deadline {
            tokio::select! {
                res = reply_rx => {
                    match res {
                        Ok(Some(frame)) => frame,
                        Ok(None) | Err(_) => Frame::Null,
                    }
                }
                _ = tokio::time::sleep(dl.saturating_duration_since(std::time::Instant::now())) => {
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
        // Cleanup remote registration on timeout/shutdown
        if is_remote && !matches!(result, Frame::Null) || matches!(result, Frame::Error(_)) {
            let mut producers = dispatch_tx.borrow_mut();
            let target_idx = ChannelMesh::target_index(shard_id, target);
            let _ = producers[target_idx].try_push(ShardMessage::BlockCancel { wait_id });
        }
        return result;
    }

    // --- Multi-key coordinator: register on ALL keys across local + remote shards ---
    // Uses FuturesUnordered for first-wakeup-wins semantics.
    let wait_id;
    let mut receivers: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
        FuturesUnordered::new();
    let mut registered_remote_shards: Vec<usize> = Vec::new();

    {
        let mut reg = blocking_registry.borrow_mut();
        let mut producers = dispatch_tx.borrow_mut();
        wait_id = reg.next_wait_id();

        for key in &keys {
            let target = key_to_shard(key, num_shards);
            let (tx, rx) = channel::oneshot::<Option<Frame>>();
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
        let sleep = tokio::time::sleep(dl.saturating_duration_since(std::time::Instant::now()));
        tokio::pin!(sleep);
        let mut result_frame = Frame::Null;
        loop {
            tokio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
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
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
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

/// Monoio version of handle_blocking_command.
///
/// Identical logic to the tokio version but uses:
/// - `monoio::select!` instead of `tokio::select!`
/// - `monoio::time::sleep` instead of `tokio::time::sleep`
/// - `std::pin::pin!` instead of `tokio::pin!`
/// - `monoio::time::sleep(Duration::from_micros(10))` for SPSC backpressure
///
/// CRITICAL: RefCell borrows MUST be dropped before any .await point.
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::await_holding_refcell_ref)]
pub(crate) async fn handle_blocking_command_monoio(
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
    shard_databases: &std::sync::Arc<ShardDatabases>,
    blocking_registry: &Rc<RefCell<crate::blocking::BlockingRegistry>>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    shutdown: &CancellationToken,
    spsc_notifiers: &[Arc<channel::Notify>],
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
        let mut guard = shard_databases.write_db(shard_id, selected_db);
        for key in &keys {
            let result = try_immediate_pop(cmd, &mut guard, key, args);
            if let Some(frame) = result {
                return frame;
            }
        }
        drop(guard); // CRITICAL before await
    }

    let deadline = if timeout_secs > 0.0 {
        Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 = block forever
    };

    // --- Single-key fast path: one registration, direct await (zero overhead) ---
    if keys.len() == 1 {
        let target = key_to_shard(&keys[0], num_shards);
        let (reply_tx, reply_rx) = channel::oneshot::<Option<Frame>>();
        let wait_id = {
            let mut reg = blocking_registry.borrow_mut();
            let wait_id = reg.next_wait_id();
            if target == shard_id {
                let entry = crate::blocking::WaitEntry {
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx,
                    deadline,
                };
                reg.register(selected_db, keys[0].clone(), entry);
            } else {
                drop(reg);
                let mut producers = dispatch_tx.borrow_mut();
                let target_idx = ChannelMesh::target_index(shard_id, target);
                let msg = ShardMessage::BlockRegister {
                    db_index: selected_db,
                    key: keys[0].clone(),
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx,
                };
                let mut msg_pending = msg;
                loop {
                    match producers[target_idx].try_push(msg_pending) {
                        Ok(()) => {
                            spsc_notifiers[target].notify_one();
                            break;
                        }
                        Err(val) => {
                            msg_pending = val;
                            drop(producers);
                            monoio::time::sleep(std::time::Duration::from_micros(10)).await;
                            producers = dispatch_tx.borrow_mut();
                        }
                    }
                }
            }
            wait_id
        }; // reg borrow dropped

        let is_remote = target != shard_id;
        let result = if let Some(dl) = deadline {
            monoio::select! {
                res = reply_rx => {
                    match res {
                        Ok(Some(frame)) => frame,
                        Ok(None) | Err(_) => Frame::Null,
                    }
                }
                _ = monoio::time::sleep(dl.saturating_duration_since(std::time::Instant::now())) => {
                    blocking_registry.borrow_mut().remove_wait(wait_id);
                    Frame::Null
                }
                _ = shutdown.cancelled() => {
                    blocking_registry.borrow_mut().remove_wait(wait_id);
                    Frame::Error(Bytes::from_static(b"ERR server shutting down"))
                }
            }
        } else {
            monoio::select! {
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
        if is_remote {
            let mut producers = dispatch_tx.borrow_mut();
            let target_idx = ChannelMesh::target_index(shard_id, target);
            let _ = producers[target_idx].try_push(ShardMessage::BlockCancel { wait_id });
        }
        return result;
    }

    // --- Multi-key coordinator: register on ALL keys across local + remote shards ---
    // Uses FuturesUnordered for first-wakeup-wins semantics.
    let wait_id;
    let mut receivers: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
        FuturesUnordered::new();
    let mut registered_remote_shards: Vec<usize> = Vec::new();

    {
        let mut reg = blocking_registry.borrow_mut();
        let mut producers = dispatch_tx.borrow_mut();
        wait_id = reg.next_wait_id();

        for key in &keys {
            let target = key_to_shard(key, num_shards);
            let (tx, rx) = channel::oneshot::<Option<Frame>>();
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
                // SPSC push with backpressure retry (monoio pattern)
                let mut msg_pending = msg;
                loop {
                    match producers[target_idx].try_push(msg_pending) {
                        Ok(()) => {
                            spsc_notifiers[target].notify_one();
                            break;
                        }
                        Err(val) => {
                            msg_pending = val;
                            // Drop borrows before await
                            drop(producers);
                            drop(reg);
                            monoio::time::sleep(std::time::Duration::from_micros(10)).await;
                            reg = blocking_registry.borrow_mut();
                            producers = dispatch_tx.borrow_mut();
                        }
                    }
                }
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
        let mut sleep = std::pin::pin!(monoio::time::sleep(
            dl.saturating_duration_since(std::time::Instant::now())
        ));
        let mut result_frame = Frame::Null;
        loop {
            monoio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
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
            monoio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
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
            spsc_notifiers[remote_shard].notify_one();
        }
    }
    // Drop remaining receivers; remote senders get Err on send -- harmless
    drop(receivers);

    frame
}

/// Parse timeout from a blocking command.
/// For most commands, timeout is the last argument.
/// For BLMPOP, timeout is the first argument.
/// Returns seconds as f64. 0 = block forever.
pub(crate) fn parse_blocking_timeout(cmd: &[u8], args: &[Frame]) -> Result<f64, Frame> {
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for blocking command",
        )));
    }
    // BLMPOP/BZMPOP: timeout is the FIRST argument; others: last argument
    let timeout_frame =
        if cmd.eq_ignore_ascii_case(b"BLMPOP") || cmd.eq_ignore_ascii_case(b"BZMPOP") {
            &args[0]
        } else {
            // args confirmed non-empty above
            #[allow(clippy::unwrap_used)] // args.is_empty() checked at entry
            args.last().unwrap()
        };
    let timeout_bytes = match timeout_frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => b,
        _ => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR timeout is not a float or out of range",
            )));
        }
    };
    let timeout_str = std::str::from_utf8(timeout_bytes).map_err(|_| {
        Frame::Error(Bytes::from_static(
            b"ERR timeout is not a float or out of range",
        ))
    })?;
    let timeout: f64 = timeout_str.parse().map_err(|_| {
        Frame::Error(Bytes::from_static(
            b"ERR timeout is not a float or out of range",
        ))
    })?;
    if !timeout.is_finite() || timeout < 0.0 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR timeout is not a float or out of range",
        )));
    }
    Ok(timeout)
}

/// Parse keys and build a BlockedCommand factory from blocking command args.
pub(crate) fn parse_blocking_args(
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
        let source = extract_bytes(&args[0])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid source key")))?;
        let destination = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid destination key")))?;
        let wherefrom_bytes = extract_bytes(&args[2])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let whereto_bytes = extract_bytes(&args[3])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
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
    } else if cmd.eq_ignore_ascii_case(b"BLMPOP") {
        // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]
        // args[0] = timeout (already parsed), args[1] = numkeys, args[2..2+numkeys] = keys,
        // args[2+numkeys] = direction, optionally args[2+numkeys+1..] = COUNT n
        if args.len() < 4 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blmpop' command",
            )));
        }
        let numkeys_bytes = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes)
            .map_err(|_| Frame::Error(Bytes::from_static(b"ERR numkeys is not an integer")))?
            .parse()
            .map_err(|_| {
                Frame::Error(Bytes::from_static(
                    b"ERR numkeys is not an integer or is out of range",
                ))
            })?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR numkeys is not an integer or is out of range",
            )));
        }
        let keys: Vec<Bytes> = args[2..2 + numkeys]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.len() != numkeys {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        }
        let dir_bytes = extract_bytes(&args[2 + numkeys])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let dir = if dir_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else if dir_bytes.eq_ignore_ascii_case(b"RIGHT") {
            crate::blocking::Direction::Right
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        // Parse optional COUNT n
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            let kw = extract_bytes(&remaining[0]);
            if let Some(kw) = kw {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    let count_bytes = extract_bytes(&remaining[1])
                        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
                    count = std::str::from_utf8(&count_bytes)
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(b"ERR count is not an integer"))
                        })?
                        .parse()
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(
                                b"ERR count is not an integer or is out of range",
                            ))
                        })?;
                    if count == 0 {
                        return Err(Frame::Error(Bytes::from_static(
                            b"ERR count is not an integer or is out of range",
                        )));
                    }
                }
            }
        }
        Ok((
            keys,
            Box::new(move || crate::blocking::BlockedCommand::BLMPop { dir, count }),
        ))
    } else if cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        // BRPOPLPUSH source destination timeout
        if args.len() != 3 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'brpoplpush' command",
            )));
        }
        let source = extract_bytes(&args[0])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid source key")))?;
        let destination = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid destination key")))?;
        Ok((
            vec![source],
            Box::new(move || crate::blocking::BlockedCommand::BLMove {
                destination: destination.clone(),
                wherefrom: crate::blocking::Direction::Right,
                whereto: crate::blocking::Direction::Left,
            }),
        ))
    } else if cmd.eq_ignore_ascii_case(b"BZMPOP") {
        // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]
        // args[0] = timeout (already parsed), args[1] = numkeys, args[2..2+numkeys] = keys,
        // args[2+numkeys] = MIN|MAX, optionally args[2+numkeys+1..] = COUNT n
        if args.len() < 4 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzmpop' command",
            )));
        }
        let numkeys_bytes = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes)
            .map_err(|_| Frame::Error(Bytes::from_static(b"ERR numkeys is not an integer")))?
            .parse()
            .map_err(|_| {
                Frame::Error(Bytes::from_static(
                    b"ERR numkeys is not an integer or is out of range",
                ))
            })?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR numkeys is not an integer or is out of range",
            )));
        }
        let keys: Vec<Bytes> = args[2..2 + numkeys]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.len() != numkeys {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        }
        let side_bytes = extract_bytes(&args[2 + numkeys])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let is_min = if side_bytes.eq_ignore_ascii_case(b"MIN") {
            true
        } else if side_bytes.eq_ignore_ascii_case(b"MAX") {
            false
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        // Parse optional COUNT n
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            let kw = extract_bytes(&remaining[0]);
            if let Some(kw) = kw {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    let count_bytes = extract_bytes(&remaining[1])
                        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
                    count = std::str::from_utf8(&count_bytes)
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(b"ERR count is not an integer"))
                        })?
                        .parse()
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(
                                b"ERR count is not an integer or is out of range",
                            ))
                        })?;
                    if count == 0 {
                        return Err(Frame::Error(Bytes::from_static(
                            b"ERR count is not an integer or is out of range",
                        )));
                    }
                }
            }
        }
        Ok((
            keys,
            Box::new(move || crate::blocking::BlockedCommand::BZMPop { min: is_min, count }),
        ))
    } else {
        Err(Frame::Error(Bytes::from_static(
            b"ERR unknown blocking command",
        )))
    }
}

/// Try to pop data immediately (non-blocking fast path).
pub(crate) fn try_immediate_pop(
    cmd: &[u8],
    db: &mut Database,
    key: &Bytes,
    args: &[Frame],
) -> Option<Frame> {
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        db.list_pop_front(key).map(|v| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(v),
            ])
        })
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        db.list_pop_back(key).map(|v| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(v),
            ])
        })
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        db.zset_pop_min(key).map(|(member, score)| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(member),
                Frame::BulkString(Bytes::from(format_blocking_score(score))),
            ])
        })
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        db.zset_pop_max(key).map(|(member, score)| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(member),
                Frame::BulkString(Bytes::from(format_blocking_score(score))),
            ])
        })
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
    } else if cmd.eq_ignore_ascii_case(b"BLMPOP") {
        // BLMPOP: immediate pop up to COUNT elements
        // args: [timeout, numkeys, key [key ...], LEFT|RIGHT, [COUNT n]]
        let numkeys_bytes = extract_bytes(&args[1])?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes).ok()?.parse().ok()?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return None;
        }
        let dir_bytes = extract_bytes(&args[2 + numkeys])?;
        let dir = if dir_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else {
            crate::blocking::Direction::Right
        };
        // Parse COUNT
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            if let Some(kw) = extract_bytes(&remaining[0]) {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    if let Some(cb) = extract_bytes(&remaining[1]) {
                        if let Some(c) = std::str::from_utf8(&cb)
                            .ok()
                            .and_then(|s| s.parse::<u32>().ok())
                        {
                            if c > 0 {
                                count = c;
                            }
                        }
                    }
                }
            }
        }
        // Check if list has elements
        let list_len = match db.get_list(key) {
            Ok(Some(l)) => l.len(),
            _ => 0,
        };
        if list_len == 0 {
            return None;
        }
        let n = std::cmp::min(count as usize, list_len);
        let mut elems = smallvec::SmallVec::<[Frame; 16]>::new();
        for _ in 0..n {
            let val = match dir {
                crate::blocking::Direction::Left => db.list_pop_front(key),
                crate::blocking::Direction::Right => db.list_pop_back(key),
            };
            match val {
                Some(v) => elems.push(Frame::BulkString(v)),
                None => break,
            }
        }
        if elems.is_empty() {
            None
        } else {
            let elem_vec: Vec<Frame> = elems.into_vec();
            Some(Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::Array(elem_vec.into()),
            ]))
        }
    } else if cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        // BRPOPLPUSH: immediate RPOP from source, LPUSH to destination
        // args: [source, destination, timeout]
        let dest = extract_bytes(&args[1])?;
        let val = db.list_pop_back(key)?;
        db.list_push_front(&dest, val.clone());
        Some(Frame::BulkString(val))
    } else if cmd.eq_ignore_ascii_case(b"BZMPOP") {
        // BZMPOP: immediate pop from sorted set
        // args: [timeout, numkeys, key [key ...], MIN|MAX, [COUNT n]]
        let numkeys_bytes = extract_bytes(&args[1])?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes).ok()?.parse().ok()?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return None;
        }
        let side_bytes = extract_bytes(&args[2 + numkeys])?;
        let is_min = side_bytes.eq_ignore_ascii_case(b"MIN");
        // Parse COUNT
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            if let Some(kw) = extract_bytes(&remaining[0]) {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    if let Some(cb) = extract_bytes(&remaining[1]) {
                        if let Some(c) = std::str::from_utf8(&cb)
                            .ok()
                            .and_then(|s| s.parse::<u32>().ok())
                        {
                            if c > 0 {
                                count = c;
                            }
                        }
                    }
                }
            }
        }
        // Check if sorted set has elements (use zset_pop to check)
        let n = count as usize;
        let mut elems = smallvec::SmallVec::<[Frame; 16]>::new();
        for _ in 0..n {
            let popped = if is_min {
                db.zset_pop_min(key)
            } else {
                db.zset_pop_max(key)
            };
            match popped {
                Some((member, score)) => {
                    elems.push(Frame::Array(framevec![
                        Frame::BulkString(member),
                        Frame::BulkString(Bytes::from(format_blocking_score(score))),
                    ]));
                }
                None => break,
            }
        }
        if elems.is_empty() {
            None
        } else {
            let elem_vec: Vec<Frame> = elems.into_vec();
            Some(Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::Array(elem_vec.into()),
            ]))
        }
    } else {
        None
    }
}

/// Format a float score the same way Redis does (integer if whole, otherwise full precision).
pub(crate) fn format_blocking_score(score: f64) -> String {
    if score == score.floor() && score.abs() < i64::MAX as f64 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

/// Inline dispatch: attempt to process a single GET or plain SET command directly
/// from raw RESP bytes in `read_buf`, bypassing Frame construction and the dispatch
/// table entirely.
///
/// **GET:** read-only, no side-effects.  Handles cold storage fallback.
///
/// **SET:** plain `SET key value` only (exactly `*3` args, no NX/XX/EX/PX options).
///   Side-effects handled by this path:
///   - maxmemory eviction (`try_evict_if_needed`)
///   - AOF append (raw RESP bytes, zero re-serialization)
///
///   Side-effects intentionally skipped (caller gates via `can_inline_writes`):
///   - ACL permission check (caller sets `can_inline_writes = false` unless
///     `cached_acl_unrestricted`)
///   - CLIENT TRACKING invalidation (guarded by `!tracking_state.enabled`)
///   - MULTI transaction queue (guarded by `!in_multi`)
///   - Metrics / slowlog recording (matches existing inline GET behaviour)
///
///   Side-effects not applicable to plain SET:
///   - Blocking-waiter wakeup (only for LPUSH/RPUSH/ZADD, not SET)
///   - Vector auto-index (only for HSET, not SET)
///
/// Returns the number of commands inlined (0 if none, 1 on success).
/// On success the serialized response is appended to `write_buf`.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn try_inline_dispatch(
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    shard_databases: &std::sync::Arc<ShardDatabases>,
    shard_id: usize,
    selected_db: usize,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    now_ms: u64,
    num_shards: usize,
    can_inline_writes: bool,
    runtime_config: &parking_lot::RwLock<crate::config::RuntimeConfig>,
) -> usize {
    let buf = &read_buf[..];
    let len = buf.len();

    // Minimum valid command: *2\r\n$3\r\nGET\r\n$1\r\nX\r\n = 20 bytes
    if len < 20 || buf[0] != b'*' {
        return 0;
    }

    // Parse array count: only *2 (GET) and *3 (SET plain) are inlined.
    let argc = buf[1];
    if buf[2] != b'\r' || buf[3] != b'\n' {
        return 0;
    }
    let is_get = argc == b'2';
    let is_set = argc == b'3' && can_inline_writes;
    if !is_get && !is_set {
        return 0;
    }

    // Expect $3\r\n for 3-letter command name (GET or SET)
    if buf[4] != b'$' || buf[5] != b'3' || buf[6] != b'\r' || buf[7] != b'\n' {
        return 0;
    }

    // Command name at positions 8,9,10
    let cmd_upper = [
        buf[8].to_ascii_uppercase(),
        buf[9].to_ascii_uppercase(),
        buf[10].to_ascii_uppercase(),
    ];
    if buf[11] != b'\r' || buf[12] != b'\n' {
        return 0;
    }

    // Validate command matches argc
    match (&cmd_upper, is_get) {
        ([b'G', b'E', b'T'], true) => {}
        ([b'S', b'E', b'T'], false) => {}
        _ => return 0,
    }

    // --- Parse first bulk-string argument (the key) ---
    if len <= 13 || buf[13] != b'$' {
        return 0;
    }
    let mut pos = 14usize;
    let mut key_len: usize = 0;
    while pos < len && buf[pos] != b'\r' {
        let d = buf[pos];
        if d < b'0' || d > b'9' {
            return 0;
        }
        // Saturating arithmetic defends against a malicious client sending
        // a huge digit run. On overflow `key_len` clamps to `usize::MAX`,
        // which trips the subsequent bounds check (`key_end + 2 > len`).
        key_len = key_len
            .saturating_mul(10)
            .saturating_add((d - b'0') as usize);
        pos += 1;
    }
    if pos + 1 >= len || buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
        return 0;
    }
    pos += 2;
    let key_start = pos;
    // `checked_add` catches the `key_len = usize::MAX` saturation case above:
    // plain `key_start + key_len` would wrap to a small value and falsely
    // satisfy the subsequent `key_end + 2 > len` bounds check.
    let Some(key_end) = key_start.checked_add(key_len) else {
        return 0;
    };
    if key_end + 2 > len || buf[key_end] != b'\r' || buf[key_end + 1] != b'\n' {
        return 0;
    }

    // Multi-shard: bail if key routes to a remote shard
    if num_shards > 1 && key_to_shard(&buf[key_start..key_end], num_shards) != shard_id {
        return 0;
    }

    if is_get {
        // ---- GET path (read-only) ----
        let consumed = key_end + 2;
        let key_bytes = &buf[key_start..key_end];
        let guard = shard_databases.read_db(shard_id, selected_db);
        match guard.get_if_alive(key_bytes, now_ms) {
            Some(entry) => match entry.value.as_bytes() {
                Some(val) => {
                    write_buf.extend_from_slice(b"$");
                    let mut itoa_buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(itoa_buf.format(val.len()).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                    write_buf.extend_from_slice(val);
                    write_buf.extend_from_slice(b"\r\n");
                }
                None => {
                    write_buf.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            },
            None => {
                // Cold storage fallback
                let cold_loc = guard.cold_lookup_location(key_bytes);
                drop(guard);
                let cold = cold_loc.and_then(|(loc, shard_dir)| {
                    crate::storage::tiered::cold_read::read_cold_entry_at(&shard_dir, loc, now_ms)
                });
                if let Some((value, _ttl)) = cold {
                    if let crate::storage::entry::RedisValue::String(v) = value {
                        write_buf.extend_from_slice(b"$");
                        let mut itoa_buf2 = itoa::Buffer::new();
                        write_buf.extend_from_slice(itoa_buf2.format(v.len()).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                        write_buf.extend_from_slice(&v);
                        write_buf.extend_from_slice(b"\r\n");
                    } else {
                        write_buf.extend_from_slice(
                            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                        );
                    }
                } else {
                    write_buf.extend_from_slice(b"$-1\r\n");
                }
                let _ = read_buf.split_to(consumed);
                return 1;
            }
        }
        drop(guard);
        let _ = read_buf.split_to(consumed);
        return 1;
    }

    // ---- SET path (write, plain *3 only) ----
    // Parse second bulk-string argument (the value)
    pos = key_end + 2;
    if pos >= len || buf[pos] != b'$' {
        return 0;
    }
    pos += 1;
    let mut val_len: usize = 0;
    while pos < len && buf[pos] != b'\r' {
        let d = buf[pos];
        if d < b'0' || d > b'9' {
            return 0;
        }
        // See saturating_mul rationale on the matching key_len parse above.
        val_len = val_len
            .saturating_mul(10)
            .saturating_add((d - b'0') as usize);
        pos += 1;
    }
    if pos + 1 >= len || buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
        return 0;
    }
    pos += 2;
    let val_start = pos;
    // See key_end checked_add above — defends against saturated val_len.
    let Some(val_end) = val_start.checked_add(val_len) else {
        return 0;
    };
    if val_end + 2 > len || buf[val_end] != b'\r' || buf[val_end + 1] != b'\n' {
        return 0;
    }
    let consumed = val_end + 2;

    // Freeze the consumed prefix of `read_buf` into an Arc-backed `Bytes`.
    // This replaces the BytesMut prefix with a refcounted view over the SAME
    // allocation, so `key`, `value`, and the AOF record can all be extracted
    // with `slice()` (Arc refcount bump, no malloc + memcpy).
    //
    // NOTE: this releases the earlier `&read_buf[..]` borrow (held as `buf`).
    // We must not index into `buf` after this point — use `frozen` instead.
    let frozen = read_buf.split_to(consumed).freeze();

    // Eviction check + write under exclusive lock
    {
        let rt = runtime_config.read();
        let mut guard = shard_databases.write_db(shard_id, selected_db);
        if crate::storage::eviction::try_evict_if_needed(&mut guard, &rt).is_err() {
            write_buf
                .extend_from_slice(b"-OOM command not allowed when used memory > 'maxmemory'\r\n");
            return 1;
        }
        drop(rt);

        let key = frozen.slice(key_start..key_end);
        let value = frozen.slice(val_start..val_end);
        let mut entry = crate::storage::entry::Entry::new_string(value);
        entry.set_last_access(guard.now());
        entry.set_access_counter(5);
        guard.set(key, entry);
    }

    // AOF: reuse the frozen RESP bytes directly (Arc clone, zero-copy).
    if let Some(tx) = aof_tx {
        let _ = tx.try_send(crate::persistence::aof::AofMessage::Append(frozen));
    }

    write_buf.extend_from_slice(b"+OK\r\n");
    1
}

/// Loop wrapper: call try_inline_dispatch repeatedly until it returns 0.
/// Returns total number of commands inlined.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn try_inline_dispatch_loop(
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    shard_databases: &std::sync::Arc<ShardDatabases>,
    shard_id: usize,
    selected_db: usize,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    now_ms: u64,
    num_shards: usize,
    can_inline_writes: bool,
    runtime_config: &parking_lot::RwLock<crate::config::RuntimeConfig>,
) -> usize {
    let mut total = 0;
    loop {
        let n = try_inline_dispatch(
            read_buf,
            write_buf,
            shard_databases,
            shard_id,
            selected_db,
            aof_tx,
            now_ms,
            num_shards,
            can_inline_writes,
            runtime_config,
        );
        if n == 0 {
            break;
        }
        total += n;
    }
    total
}
