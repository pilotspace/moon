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
use crate::storage::Database;

use super::util::extract_bytes;

/// Convert a blocking command to its non-blocking equivalent for MULTI/EXEC.
/// BLPOP key [key ...] timeout -> LPOP key (first key only)
/// BRPOP key [key ...] timeout -> RPOP key
/// BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout -> LMOVE src dst LEFT|RIGHT LEFT|RIGHT
/// BZPOPMIN key [key ...] timeout -> ZPOPMIN key
/// BZPOPMAX key [key ...] timeout -> ZPOPMAX key
pub(crate) fn convert_blocking_to_nonblocking(cmd: &[u8], args: &[Frame]) -> Frame {
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
        Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 = block forever
    };

    // --- Single-key fast path: one registration, direct await (zero overhead) ---
    if keys.len() == 1 {
        let (reply_tx, reply_rx) = channel::oneshot::<Option<Frame>>();
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
    databases: &Rc<RefCell<Vec<Database>>>,
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
        Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 = block forever
    };

    // --- Single-key fast path: one registration, direct await (zero overhead) ---
    if keys.len() == 1 {
        let (reply_tx, reply_rx) = channel::oneshot::<Option<Frame>>();
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
            monoio::select! {
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
            spsc_notifiers[remote_shard].notify_one();
        }
    }
    // Drop remaining receivers; remote senders get Err on send -- harmless
    drop(receivers);

    frame
}

/// Parse timeout from the last argument of a blocking command.
/// Returns seconds as f64. 0 = block forever.
pub(crate) fn parse_blocking_timeout(cmd: &[u8], args: &[Frame]) -> Result<f64, Frame> {
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from(format!(
            "ERR wrong number of arguments for '{}' command",
            String::from_utf8_lossy(cmd).to_lowercase()
        ))));
    }
    let timeout_frame = args.last().unwrap();
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
    if timeout < 0.0 {
        return Err(Frame::Error(Bytes::from_static(b"ERR timeout is negative")));
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

/// Inline dispatch: attempt to process a single GET or SET command directly from
/// the raw RESP bytes in `read_buf`, bypassing Frame construction and the dispatch
/// table entirely.  Only active when `num_shards == 1` (all keys are local).
///
/// Returns the number of bytes consumed from `read_buf` (0 if no command was inlined).
/// On success the serialized response is appended to `write_buf`.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn try_inline_dispatch(
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    databases: &Rc<RefCell<Vec<Database>>>,
    selected_db: usize,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
) -> usize {
    let buf = &read_buf[..];
    let len = buf.len();

    // Minimum valid command: *2\r\n$3\r\nGET\r\n$1\r\nX\r\n = 20 bytes
    // (*2\r\n=4) + ($3\r\n=4) + (GET\r\n=5) + ($1\r\n=4) + (X\r\n=3) = 20
    if len < 20 {
        return 0;
    }

    // Must start with RESP array marker
    if buf[0] != b'*' {
        return 0;
    }

    // --- Detect *2\r\n (GET) or *3\r\n (SET) ---
    let (is_get, is_set) = if buf[1] == b'2' && buf[2] == b'\r' && buf[3] == b'\n' {
        (true, false)
    } else if buf[1] == b'3' && buf[2] == b'\r' && buf[3] == b'\n' {
        (false, true)
    } else {
        return 0;
    };

    // After "*N\r\n" expect "$3\r\n" for 3-letter command name
    // Position 4: must be '$', pos 5: '3', pos 6-7: \r\n
    if buf[4] != b'$' || buf[5] != b'3' || buf[6] != b'\r' || buf[7] != b'\n' {
        return 0;
    }

    // Positions 8,9,10 = command name (case-insensitive)
    let cmd_upper = [
        buf[8].to_ascii_uppercase(),
        buf[9].to_ascii_uppercase(),
        buf[10].to_ascii_uppercase(),
    ];

    if is_get && cmd_upper != [b'G', b'E', b'T'] {
        return 0;
    }
    if is_set && cmd_upper != [b'S', b'E', b'T'] {
        return 0;
    }

    // After command: \r\n at positions 11,12
    if buf[11] != b'\r' || buf[12] != b'\n' {
        return 0;
    }

    // Now parse first argument (the key): "$<keylen>\r\n<key>\r\n"
    // Position 13 must be '$'
    if len <= 13 || buf[13] != b'$' {
        return 0;
    }

    // Parse key length digits starting at position 14
    let mut pos = 14usize;
    let mut key_len: usize = 0;
    while pos < len && buf[pos] != b'\r' {
        let d = buf[pos];
        if d < b'0' || d > b'9' {
            return 0; // non-digit in length field
        }
        key_len = key_len * 10 + (d - b'0') as usize;
        pos += 1;
    }
    // Need \r\n after key length
    if pos + 1 >= len || buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
        return 0;
    }
    pos += 2; // skip \r\n

    // Need key_len bytes + \r\n
    let key_start = pos;
    let key_end = key_start + key_len;
    if key_end + 2 > len {
        return 0; // partial key data
    }
    if buf[key_end] != b'\r' || buf[key_end + 1] != b'\n' {
        return 0;
    }

    if is_get {
        // GET: done parsing -- total consumed = key_end + 2
        let consumed = key_end + 2;
        let key_bytes = &buf[key_start..key_end];

        // Lookup in database
        let mut dbs = databases.borrow_mut();
        let db = &mut dbs[selected_db];
        match db.get(key_bytes) {
            Some(entry) => {
                match entry.value.as_bytes() {
                    Some(val) => {
                        // $<len>\r\n<val>\r\n
                        write_buf.extend_from_slice(b"$");
                        let mut itoa_buf = itoa::Buffer::new();
                        write_buf.extend_from_slice(itoa_buf.format(val.len()).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                        write_buf.extend_from_slice(val);
                        write_buf.extend_from_slice(b"\r\n");
                    }
                    None => {
                        // Wrong type
                        write_buf.extend_from_slice(
                            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                        );
                    }
                }
            }
            None => {
                // Null bulk string
                write_buf.extend_from_slice(b"$-1\r\n");
            }
        }
        drop(dbs);
        let _ = read_buf.split_to(consumed);
        return 1;
    }

    // --- SET: parse value argument ---
    let mut vpos = key_end + 2; // after key's trailing \r\n
    if vpos >= len || buf[vpos] != b'$' {
        return 0;
    }
    vpos += 1; // skip '$'

    let mut val_len: usize = 0;
    while vpos < len && buf[vpos] != b'\r' {
        let d = buf[vpos];
        if d < b'0' || d > b'9' {
            return 0;
        }
        val_len = val_len * 10 + (d - b'0') as usize;
        vpos += 1;
    }
    if vpos + 1 >= len || buf[vpos] != b'\r' || buf[vpos + 1] != b'\n' {
        return 0;
    }
    vpos += 2; // skip \r\n

    let val_start = vpos;
    let val_end = val_start + val_len;
    if val_end + 2 > len {
        return 0; // partial value
    }
    if buf[val_end] != b'\r' || buf[val_end + 1] != b'\n' {
        return 0;
    }

    let consumed = val_end + 2;

    // Create owned copies of key and value before advancing read_buf
    let key_owned = Bytes::copy_from_slice(&buf[key_start..key_end]);
    let val_owned = Bytes::copy_from_slice(&buf[val_start..val_end]);

    // AOF: capture the raw RESP bytes before we advance the buffer
    if let Some(tx) = aof_tx {
        let aof_bytes = Bytes::copy_from_slice(&buf[..consumed]);
        let _ = tx.try_send(crate::persistence::aof::AofMessage::Append(aof_bytes));
    }

    // Insert into database
    {
        let entry = crate::storage::entry::Entry::new_string(val_owned);
        let mut dbs = databases.borrow_mut();
        dbs[selected_db].set(key_owned, entry);
    }

    // +OK\r\n
    write_buf.extend_from_slice(b"+OK\r\n");

    let _ = read_buf.split_to(consumed);
    1
}

/// Loop wrapper: call try_inline_dispatch repeatedly until it returns 0.
/// Returns total number of commands inlined.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn try_inline_dispatch_loop(
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    databases: &Rc<RefCell<Vec<Database>>>,
    selected_db: usize,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
) -> usize {
    let mut total = 0;
    loop {
        let n = try_inline_dispatch(read_buf, write_buf, databases, selected_db, aof_tx);
        if n == 0 {
            break;
        }
        total += n;
    }
    total
}
