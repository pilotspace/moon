//! VLL multi-key coordination for MGET, MSET, DEL, UNLINK, EXISTS.
//!
//! Groups keys by target shard in a BTreeMap (ascending shard-ID order for
//! deadlock prevention -- VLL pattern), dispatches to each shard, collects
//! results, and assembles the final response. Local-shard keys are executed
//! directly without SPSC overhead.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use ringbuf::HeapProd;
use ringbuf::traits::Producer;

use crate::command::{DispatchResult, dispatch as cmd_dispatch};
use crate::framevec;
use crate::protocol::Frame;
use crate::runtime::channel;
// Coordinator uses oneshot channels (not ResponseSlotPool) for cross-thread safety.
// ResponseSlotPool's AtomicWaker doesn't work with monoio's !Send executor.
// The oneshot overhead (~80ns) is negligible on the multi-key coordination path.
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::storage::entry::CachedClock;

use super::shared_databases::ShardDatabases;
/// Coordinate a multi-key command across shards.
///
/// Routes MGET, MSET, DEL (multi), UNLINK (multi), and EXISTS (multi)
/// to the appropriate per-command coordinator.
pub async fn coordinate_multi_key(
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if cmd.eq_ignore_ascii_case(b"MGET") {
        coordinate_mget(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"MSET") {
        coordinate_mset(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    } else {
        // DEL, UNLINK, EXISTS with multiple keys
        coordinate_multi_del_or_exists(
            cmd,
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    }
}

/// Extract Bytes from a Frame argument.
fn extract_key(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// Send a ShardMessage via SPSC with spin-retry on full buffer.
///
/// Calls `notify_one()` on the target shard's notifier after successful push
/// for immediate wake (avoids relying on the 1ms periodic timer safety net).
///
/// Exposed at `pub(crate)` so sibling files (e.g. `scatter_aggregate`)
/// can dispatch via the same contention-safe path.
pub(crate) async fn spsc_send(
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    my_shard: usize,
    target_shard: usize,
    msg: ShardMessage,
    spsc_notifiers: &[Arc<channel::Notify>],
) {
    let target_idx = ChannelMesh::target_index(my_shard, target_shard);
    let mut pending = msg;
    loop {
        let push_result = {
            let mut producers = dispatch_tx.borrow_mut();
            producers[target_idx].try_push(pending)
        }; // borrow dropped before yield
        match push_result {
            Ok(()) => {
                spsc_notifiers[target_shard].notify_one();
                return;
            }
            Err(val) => {
                pending = val;
                // Yield to other tasks to avoid busy-spinning on full SPSC buffer.
                #[cfg(feature = "runtime-tokio")]
                tokio::task::yield_now().await;
                #[cfg(feature = "runtime-monoio")]
                monoio::time::sleep(std::time::Duration::from_micros(10)).await;
            }
        }
    }
}

/// Coordinate MGET across shards using VLL pattern.
///
/// Groups keys by shard in a BTreeMap (ascending shard-ID order), executes
/// local keys directly, dispatches remote keys via MultiExecute batches,
/// reassembles results in original order.
async fn coordinate_mget(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'mget' command",
        ));
    }

    // Group keys by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<(usize, Bytes)>> = BTreeMap::new();
    for (i, arg) in args.iter().enumerate() {
        if let Some(key) = extract_key(arg) {
            let shard = key_to_shard(&key, num_shards);
            groups.entry(shard).or_default().push((i, key));
        }
    }

    // Fast path: all keys on local shard -- use mget directly
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        let mut guard = shard_databases.write_db(my_shard, db_index);
        guard.refresh_now_from_cache(cached_clock);
        return crate::command::string::mget(&mut guard, args);
    }

    let total = args.len();
    let mut results: Vec<Option<Frame>> = vec![None; total];
    let mut pending_shards: Vec<(Vec<usize>, channel::OneshotReceiver<Vec<Frame>>)> = Vec::new();

    // Iterate in ascending shard-ID order (BTreeMap guarantees this)
    for (shard_id, indexed_keys) in &groups {
        let original_indices: Vec<usize> = indexed_keys.iter().map(|(i, _)| *i).collect();

        if *shard_id == my_shard {
            // Local execution: GET each key directly
            let mut guard = shard_databases.write_db(my_shard, db_index);
            guard.refresh_now_from_cache(cached_clock);
            for (orig_idx, key) in indexed_keys {
                let entry = guard.get(key);
                let frame = match entry {
                    Some(e) => match e.value.as_bytes() {
                        Some(v) => Frame::BulkString(Bytes::copy_from_slice(v)),
                        None => Frame::Null,
                    },
                    None => Frame::Null,
                };
                results[*orig_idx] = Some(frame);
            }
        } else {
            // Remote dispatch: batch of GET commands via MultiExecuteSlotted
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = indexed_keys
                .iter()
                .map(|(_, k)| {
                    let cmd = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"GET")),
                        Frame::BulkString(k.clone()),
                    ]);
                    (k.clone(), cmd)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push((original_indices, reply_rx));
        }
    }

    // Await all remote results
    for (indices, reply_rx) in pending_shards {
        match reply_rx.recv().await {
            Ok(frames) => {
                for (idx, frame) in indices.into_iter().zip(frames) {
                    results[idx] = Some(frame);
                }
            }
            Err(_) => {
                // Channel closed — target shard dropped without responding.
                for idx in indices {
                    results[idx] = Some(Frame::Error(Bytes::from_static(
                        b"ERR cross-shard reply channel closed",
                    )));
                }
            }
        }
    }

    // Assemble in original order
    Frame::Array(
        results
            .into_iter()
            .map(|opt| opt.unwrap_or(Frame::Null))
            .collect(),
    )
}

/// Coordinate MSET across shards using VLL pattern.
///
/// Groups key-value pairs by shard in ascending order, dispatches SET
/// sub-commands per shard. Returns OK when all complete.
async fn coordinate_mset(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'mset' command",
        ));
    }

    // Group key-value pairs by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<(Bytes, Bytes)>> = BTreeMap::new();
    for pair in args.chunks(2) {
        let key = match extract_key(&pair[0]) {
            Some(k) => k,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'mset' command",
                ));
            }
        };
        let value = match extract_key(&pair[1]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'mset' command",
                ));
            }
        };
        let shard = key_to_shard(&key, num_shards);
        groups.entry(shard).or_default().push((key, value));
    }

    // Fast path: all keys on local shard
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        let mut guard = shard_databases.write_db(my_shard, db_index);
        guard.refresh_now_from_cache(cached_clock);
        return crate::command::string::mset(&mut guard, args);
    }

    let mut pending_shards: Vec<channel::OneshotReceiver<Vec<Frame>>> = Vec::new();

    for (shard_id, kv_pairs) in &groups {
        if *shard_id == my_shard {
            let mut guard = shard_databases.write_db(my_shard, db_index);
            guard.refresh_now_from_cache(cached_clock);
            for (key, value) in kv_pairs {
                guard.set_string(key.clone(), value.clone());
            }
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = kv_pairs
                .iter()
                .map(|(k, v)| {
                    let cmd = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"SET")),
                        Frame::BulkString(k.clone()),
                        Frame::BulkString(v.clone()),
                    ]);
                    (k.clone(), cmd)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push(reply_rx);
        }
    }

    for reply_rx in pending_shards {
        let _ = reply_rx.recv().await;
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Coordinate DEL/UNLINK/EXISTS with multiple keys across shards using VLL pattern.
///
/// Groups keys by shard in ascending order (BTreeMap), dispatches sub-commands
/// per shard via MultiExecute, sums integer results.
async fn coordinate_multi_del_or_exists(
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let cmd_upper = cmd.to_ascii_uppercase();

    // Group keys by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<Frame>> = BTreeMap::new();
    for arg in args {
        if let Some(key) = extract_key(arg) {
            let shard = key_to_shard(&key, num_shards);
            groups.entry(shard).or_default().push(arg.clone());
        }
    }

    // Fast path: all keys on local shard
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        let mut guard = shard_databases.write_db(my_shard, db_index);
        guard.refresh_now_from_cache(cached_clock);
        let mut selected = db_index;
        let db_count = shard_databases.db_count();
        let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
        return match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        };
    }

    let mut total_count: i64 = 0;
    let mut pending_shards: Vec<channel::OneshotReceiver<Vec<Frame>>> = Vec::new();

    for (shard_id, key_args) in &groups {
        if *shard_id == my_shard {
            let mut guard = shard_databases.write_db(my_shard, db_index);
            guard.refresh_now_from_cache(cached_clock);
            let db_count = shard_databases.db_count();
            let mut selected = db_index;
            let result = cmd_dispatch(&mut guard, cmd, key_args, &mut selected, db_count);
            if let DispatchResult::Response(Frame::Integer(n)) = result {
                total_count += n;
            }
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = key_args
                .iter()
                .map(|arg| {
                    let key = extract_key(arg).unwrap_or_default();
                    let cmd_frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from(cmd_upper.clone())),
                        arg.clone(),
                    ]);
                    (key, cmd_frame)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push(reply_rx);
        }
    }

    for reply_rx in pending_shards {
        match reply_rx.recv().await {
            Ok(frames) => {
                for frame in frames {
                    match frame {
                        Frame::Integer(n) => total_count += n,
                        Frame::Error(_) => return frame,
                        _ => {}
                    }
                }
            }
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during DEL/UNLINK",
                ));
            }
        }
    }

    Frame::Integer(total_count)
}

/// Coordinate KEYS across all shards.
///
/// Dispatches KEYS command to every shard, collects and merges results.
pub async fn coordinate_keys(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'keys' command",
        ));
    }

    let mut all_keys: Vec<Frame> = Vec::new();
    let mut pending_shards: Vec<channel::OneshotReceiver<Frame>> = Vec::new();

    // Execute locally on this shard
    {
        let mut guard = shard_databases.write_db(my_shard, db_index);
        guard.refresh_now_from_cache(cached_clock);
        let db_count = shard_databases.db_count();
        let mut selected = db_index;
        let result = cmd_dispatch(&mut guard, b"KEYS", args, &mut selected, db_count);
        if let DispatchResult::Response(Frame::Array(keys)) = result {
            all_keys.extend(keys);
        }
    }

    // Dispatch to all remote shards
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = {
            let mut parts = vec![Frame::BulkString(Bytes::from_static(b"KEYS"))];
            for a in args {
                parts.push(a.clone());
            }
            Frame::Array(parts.into())
        };
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    // Collect remote results
    for reply_rx in pending_shards {
        match reply_rx.recv().await {
            Ok(Frame::Array(keys)) => all_keys.extend(keys),
            Ok(_) => {} // Non-array response (e.g., error) — skip
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during KEYS",
                ));
            }
        }
    }

    Frame::Array(all_keys.into())
}

/// Coordinate SCAN across all shards.
///
/// Cursor encoding: upper 16 bits = shard index, lower 48 bits = per-shard cursor.
/// This allows SCAN to iterate through all shards sequentially.
pub async fn coordinate_scan(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'scan' command",
        ));
    }

    // Parse the composite cursor from the first arg
    let cursor_val: i64 = match &args[0] {
        Frame::BulkString(b) | Frame::SimpleString(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0),
        Frame::Integer(n) => *n,
        _ => 0,
    };
    let cursor_u64 = cursor_val as u64;

    // Decode: upper 16 bits = shard index, lower 48 bits = per-shard cursor
    let current_shard = ((cursor_u64 >> 48) & 0xFFFF) as usize;
    let shard_cursor = (cursor_u64 & 0x0000_FFFF_FFFF_FFFF) as i64;

    // Determine the target shard (may differ from my_shard)
    let target_shard_id = current_shard.min(num_shards - 1);

    // Build the SCAN command with the per-shard cursor
    let mut scan_args = vec![Frame::BulkString(Bytes::from(shard_cursor.to_string()))];
    // Forward remaining args (COUNT, MATCH, etc.)
    for a in &args[1..] {
        scan_args.push(a.clone());
    }

    // Execute SCAN on the target shard
    let scan_result = if target_shard_id == my_shard {
        let mut guard = shard_databases.write_db(my_shard, db_index);
        guard.refresh_now_from_cache(cached_clock);
        let db_count = shard_databases.db_count();
        let mut selected = db_index;
        let result = cmd_dispatch(&mut guard, b"SCAN", &scan_args, &mut selected, db_count);
        match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        }
    } else {
        // Remote dispatch
        let (reply_tx, reply_rx) = channel::oneshot();
        let mut parts = vec![Frame::BulkString(Bytes::from_static(b"SCAN"))];
        parts.extend(scan_args);
        let cmd_frame = Frame::Array(parts.into());
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target_shard_id, msg, spsc_notifiers).await;
        match reply_rx.recv().await {
            Ok(frame) => frame,
            Err(_) => Frame::Error(Bytes::from_static(
                b"ERR cross-shard reply channel closed during SCAN",
            )),
        }
    };

    // Parse the SCAN response: [cursor, [keys...]]
    match scan_result {
        Frame::Array(parts) if parts.len() == 2 => {
            let next_shard_cursor: i64 = match &parts[0] {
                Frame::BulkString(b) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                Frame::Integer(n) => *n,
                _ => 0,
            };

            let keys = parts[1].clone();

            // Compute next composite cursor
            let next_composite = if next_shard_cursor == 0 {
                // This shard is done, move to the next shard
                let next_shard = target_shard_id + 1;
                if next_shard >= num_shards {
                    // All shards done
                    0u64
                } else {
                    // Start of next shard (cursor 0)
                    (next_shard as u64) << 48
                }
            } else {
                // Continue on current shard
                ((target_shard_id as u64) << 48)
                    | (next_shard_cursor as u64 & 0x0000_FFFF_FFFF_FFFF)
            };

            Frame::Array(framevec![
                Frame::BulkString(Bytes::from(next_composite.to_string())),
                keys,
            ])
        }
        other => other,
    }
}

/// Coordinate DBSIZE across all shards.
///
/// Returns the sum of keys across all shards.
pub async fn coordinate_dbsize(
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let mut total: i64 = 0;
    let mut pending_shards: Vec<channel::OneshotReceiver<Frame>> = Vec::new();

    // Local shard
    {
        let guard = shard_databases.read_db(my_shard, db_index);
        total += guard.len() as i64;
    }

    // Remote shards
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"DBSIZE"))]);
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    for reply_rx in pending_shards {
        match reply_rx.recv().await {
            Ok(Frame::Integer(n)) => total += n,
            Ok(_) => {} // Non-integer response — skip
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during DBSIZE",
                ));
            }
        }
    }

    Frame::Integer(total)
}

/// Scatter a vector search query to all shards, collect per-shard results,
/// and merge into a global top-K response.
///
/// Used when the connection handler receives FT.SEARCH and num_shards > 1.
/// Each shard runs a local search and returns its local top-K. The coordinator
/// merges all per-shard results and returns the globally correct top-K.
///
/// For single-shard deployments, FT.SEARCH executes directly without scatter.
pub async fn scatter_vector_search(
    index_name: Bytes,
    query_blob: Bytes,
    k: usize,
    as_of_lsn: u64,
    my_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    vector_store: &mut crate::vector::store::VectorStore,
) -> Frame {
    let mut receivers = Vec::with_capacity(num_shards);
    let mut local_result: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Execute locally -- avoid SPSC overhead for local shard.
            // Phase 171 SCAT-01: thread as_of_lsn through the local branch so
            // the coordinator honors temporal filtering on its own shard too.
            local_result = Some(crate::command::vector_search::search_local_filtered(
                vector_store,
                &index_name,
                &query_blob,
                k,
                None,
                0,
                usize::MAX,
                None,
                as_of_lsn,
            ));
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::VectorSearch {
                index_name: index_name.clone(),
                query_blob: query_blob.clone(),
                k,
                as_of_lsn,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            receivers.push(reply_rx);
        }
    }

    let mut shard_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_result {
        shard_responses.push(local);
    }
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => shard_responses.push(frame),
            Err(_) => {
                return Frame::Error(bytes::Bytes::from_static(
                    b"ERR shard reply channel closed during vector search scatter-gather",
                ));
            }
        }
    }

    crate::command::vector_search::merge_search_results(&shard_responses, k, 0, usize::MAX)
}

/// Scatter FT.SEARCH to all shards via SPSC (no local vector_store needed).
///
/// Used by connection handlers that don't have direct vector_store access.
/// Sends VectorSearch to every shard (including local) via SPSC, collects
/// results, and merges into a global top-K response.
/// Scatter FT.SEARCH to all shards (local + remote), merge top-K results.
///
/// Local shard: direct VectorStore access via shard_databases (no SPSC self-send).
/// Remote shards: SPSC dispatch with VectorSearch message.
/// Single-shard (num_shards == 1): local-only, no SPSC needed.
pub async fn scatter_vector_search_remote(
    index_name: Bytes,
    query_blob: Bytes,
    k: usize,
    as_of_lsn: u64,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    // LOCAL: direct vector store access (avoids SPSC self-send).
    // Phase 171 SCAT-01: honor AS_OF on the coordinator's own shard by
    // routing through `search_local_filtered` with the resolved LSN rather
    // than the AS_OF-unaware `search_local` helper.
    let local_result = {
        let mut vs = shard_databases.vector_store(my_shard);
        crate::command::vector_search::search_local_filtered(
            &mut vs,
            &index_name,
            &query_blob,
            k,
            None,
            0,
            usize::MAX,
            None,
            as_of_lsn,
        )
    };

    // REMOTE: SPSC to all other shards
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorSearch {
            index_name: index_name.clone(),
            query_blob: query_blob.clone(),
            k,
            as_of_lsn,
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    let mut shard_responses = Vec::with_capacity(num_shards);
    shard_responses.push(local_result);
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => shard_responses.push(frame),
            Err(_) => {
                return Frame::Error(bytes::Bytes::from_static(
                    b"ERR shard reply channel closed during vector search scatter-gather",
                ));
            }
        }
    }

    crate::command::vector_search::merge_search_results(&shard_responses, k, 0, usize::MAX)
}

/// Broadcast an FT.* command (FT.CREATE, FT.DROPINDEX) to ALL shards.
///
/// Each shard creates its own copy of the index so HSET auto-indexing works
/// regardless of which shard the key routes to.
///
/// Local shard: direct VectorStore access via shard_databases.
/// Remote shards: SPSC dispatch with VectorCommand message.
/// Single-shard (num_shards == 1): local-only, no SPSC needed.
pub async fn broadcast_vector_command(
    command: std::sync::Arc<Frame>,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    // REMOTE FIRST: send to all other shards via SPSC before local mutation.
    // This ensures we detect remote failures before committing locally,
    // avoiding partial index metadata across the cluster.
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorCommand {
            command: command.clone(),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    // Collect remote results — fail if any shard errors or disconnects
    for rx in receivers {
        match rx.recv().await {
            Ok(Frame::Error(e)) => return Frame::Error(e),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR vector command failed: cross-shard reply channel closed",
                ));
            }
            _ => {}
        }
    }

    // LOCAL: execute only after all remote shards succeeded
    // FT.DROPINDEX with DD flag needs Database to delete indexed docs.
    let is_dropindex = match command.as_ref() {
        Frame::Array(arr) if !arr.is_empty() => {
            matches!(&arr[0], Frame::BulkString(b) if b.eq_ignore_ascii_case(b"FT.DROPINDEX"))
        }
        _ => false,
    };

    let local_result = {
        let mut vs = shard_databases.vector_store(my_shard);
        let mut ts = shard_databases.text_store(my_shard);
        #[cfg(feature = "graph")]
        let graph_guard = shard_databases.graph_store_read(my_shard);
        let mut db_guard;
        let db_opt = if is_dropindex {
            db_guard = shard_databases.write_db(my_shard, 0);
            Some(&mut *db_guard)
        } else {
            None
        };
        crate::shard::spsc_handler::dispatch_vector_command(
            &mut vs,
            &mut *ts,
            #[cfg(feature = "graph")]
            Some(&graph_guard),
            &command,
            db_opt,
        )
    };
    local_result
}

/// Two-phase DFS scatter-gather for globally accurate BM25 text search (per D-04).
///
/// **Phase 1** — DocFreq scatter: collect (term, df) + total N from every shard,
/// aggregate into global document frequency statistics.
///
/// **Phase 2** — TextSearch scatter: execute BM25 search on every shard using the
/// injected global IDF weights, then merge per-shard top-K results.
///
/// **Single-shard fast path** (per D-06): when `num_shards == 1`, the local shard's
/// FieldStats are globally accurate, so the DFS pre-pass is skipped entirely.
///
/// # Lock safety
/// Every access to `shard_databases.text_store(shard_id)` returns a `MutexGuard`.
/// All local data extraction is wrapped in a block scope so the guard is dropped
/// **before** any `.await` point — required by RESEARCH Pitfall 2.
pub async fn scatter_text_search(
    index_name: Bytes,
    query_terms: Vec<crate::command::vector_search::QueryTerm>,
    field_idx: Option<usize>,
    top_k: usize,
    offset: usize,
    count: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    highlight_opts: Option<crate::command::vector_search::HighlightOpts>,
    summarize_opts: Option<crate::command::vector_search::SummarizeOpts>,
) -> Frame {
    // Extract plain term strings for DocFreq phase (only needs term text, not modifiers).
    let term_strings: Vec<String> = query_terms.iter().map(|qt| qt.text.clone()).collect();

    // ── Single-shard fast path (per D-06) ────────────────────────────────────
    if num_shards == 1 {
        // Local IDF is globally accurate with one shard — skip DFS pre-pass.
        // Apply HIGHLIGHT/SUMMARIZE post-processing after local search.
        let result = {
            let ts = shard_databases.text_store(my_shard);
            let mut r = crate::command::vector_search::ft_text_search::execute_text_search_local(
                &ts,
                &index_name,
                field_idx,
                &query_terms,
                top_k,
                offset,
                count,
            );
            // Apply post-processing if requested — guards held, no .await below.
            if highlight_opts.is_some() || summarize_opts.is_some() {
                if let Some(text_index) = ts.get_index(&index_name) {
                    let db_guard = shard_databases.read_db(my_shard, 0);
                    crate::command::vector_search::ft_text_search::apply_post_processing(
                        &mut r,
                        &term_strings,
                        text_index,
                        &*db_guard,
                        highlight_opts.as_ref(),
                        summarize_opts.as_ref(),
                    );
                    // db_guard drops here.
                }
            }
            r
        }; // MutexGuard dropped here — no .await held
        return result;
    }

    // ── Phase 1: scatter DocFreq to all shards ────────────────────────────────
    // Collect (term, df, N) from each shard to build global IDF weights.
    // DocFreq only needs term strings (not modifiers) for df lookup.
    let field_queries = vec![(field_idx, term_strings.clone())];
    let mut doc_freq_receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_doc_freq: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local: extract df/N directly — no SPSC overhead.
            // CRITICAL: block scope drops MutexGuard before any .await (RESEARCH Pitfall 2).
            let response = {
                let ts = shard_databases.text_store(shard_id);
                match ts.get_index(&index_name) {
                    Some(text_index) => {
                        let mut items: Vec<Frame> = Vec::new();
                        for (field_idx_opt, terms) in &field_queries {
                            let fidx = field_idx_opt.unwrap_or(0);
                            let (term_dfs, n) = text_index.doc_freq_for_terms(fidx, terms);
                            for (term, df) in term_dfs {
                                items.push(Frame::BulkString(Bytes::from(term)));
                                items.push(Frame::Integer(i64::from(df)));
                            }
                            items.push(Frame::BulkString(Bytes::from_static(b"N")));
                            items.push(Frame::Integer(i64::from(n)));
                        }
                        Frame::Array(items.into())
                    }
                    None => Frame::Error(Bytes::from_static(b"ERR unknown index")),
                }
            }; // MutexGuard dropped here, before .await below
            local_doc_freq = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::DocFreq {
                index_name: index_name.clone(),
                field_queries: field_queries.clone(),
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            doc_freq_receivers.push(reply_rx);
        }
    }

    // Collect Phase 1 responses and aggregate.
    let mut doc_freq_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_doc_freq {
        doc_freq_responses.push(local);
    }
    for rx in doc_freq_receivers {
        match rx.recv().await {
            Ok(frame) => doc_freq_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR DFS phase 1 channel closed unexpectedly",
                ));
            }
        }
    }

    let (global_df, global_n) = aggregate_doc_freq(&doc_freq_responses);

    // ── Phase 2: scatter TextSearch with global IDF to all shards ─────────────
    let mut search_receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_search: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local: execute with global IDF directly — block scope drops guard.
            // CRITICAL: guard dropped before any .await (RESEARCH Pitfall 2).
            let response = {
                let ts = shard_databases.text_store(shard_id);
                match ts.get_index(&index_name) {
                    Some(text_index) => {
                        let mut r =
                            crate::command::vector_search::ft_text_search::execute_text_search_with_global_idf(
                                text_index,
                                field_idx,
                                &query_terms,
                                &global_df,
                                global_n,
                                top_k,
                                0,      // each shard returns top_k; coordinator applies final offset
                                top_k,
                            );
                        // Apply HIGHLIGHT/SUMMARIZE while guards are held — sync, no .await.
                        if highlight_opts.is_some() || summarize_opts.is_some() {
                            let db_guard = shard_databases.read_db(shard_id, 0);
                            crate::command::vector_search::ft_text_search::apply_post_processing(
                                &mut r,
                                &term_strings,
                                text_index,
                                &*db_guard,
                                highlight_opts.as_ref(),
                                summarize_opts.as_ref(),
                            );
                            // db_guard drops here.
                        }
                        r
                    }
                    None => Frame::Error(Bytes::from_static(b"ERR unknown index")),
                }
            }; // MutexGuard dropped here, before .await below
            local_search = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::TextSearch {
                index_name: index_name.clone(),
                field_idx,
                // Send full QueryTerm so remote shard applies the same expansion.
                query_terms: query_terms.clone(),
                global_df: global_df.clone(),
                global_n,
                top_k,
                offset: 0, // each shard returns top_k; coordinator applies final offset+count
                count: top_k,
                // Pass opts to each remote shard — each applies post-processing locally.
                highlight_opts: highlight_opts.clone(),
                summarize_opts: summarize_opts.clone(),
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            search_receivers.push(reply_rx);
        }
    }

    // Collect Phase 2 responses.
    let mut search_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_search {
        search_responses.push(local);
    }
    for rx in search_receivers {
        match rx.recv().await {
            Ok(frame) => search_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR DFS phase 2 channel closed unexpectedly",
                ));
            }
        }
    }

    // Merge and apply final pagination.
    crate::command::vector_search::merge_text_results(&search_responses, top_k, offset, count)
}

/// Scatter a FieldFilter (TAG — Plan 07 adds NumericRange) across all shards.
///
/// Plan 152-06 (B-02): mirrors `scatter_text_search` for FieldFilter clauses
/// but:
/// - Skips the DFS pre-pass (FieldFilter has no per-term IDF).
/// - Dispatches `ShardMessage::InvertedSearch` instead of `TextSearch`.
/// - Merges response frames via `merge_text_results` — results arrive with
///   `score=0.0`, `merge_text_results` preserves insertion order within its
///   bucket so the per-shard doc_id ascending order becomes a consistent
///   cross-shard ordering after the sort-by-score tie-break (score is
///   uniform, so the secondary key — key-bytes — resolves deterministically).
///
/// # Lock safety
/// Single-shard fast path scopes the `MutexGuard` inside a block so it
/// drops before any `.await` (RESEARCH Pitfall 2).
#[cfg(feature = "text-index")]
#[allow(clippy::too_many_arguments)]
pub async fn scatter_text_search_filter(
    index_name: Bytes,
    filter: crate::command::vector_search::ft_text_search::FieldFilter,
    top_k: usize,
    offset: usize,
    count: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    // ── Single-shard fast path ────────────────────────────────────────────────
    if num_shards == 1 {
        let response = {
            let ts = shard_databases.text_store(my_shard);
            match ts.get_index(&index_name) {
                None => Frame::Error(Bytes::from_static(b"ERR no such index")),
                Some(text_index) => {
                    let clause = crate::command::vector_search::ft_text_search::TextQueryClause {
                        field_name: None,
                        terms: Vec::new(),
                        filter: Some(filter),
                    };
                    let results =
                        crate::command::vector_search::ft_text_search::execute_query_on_index(
                            text_index, &clause, None, None, top_k,
                        );
                    crate::command::vector_search::ft_text_search::build_text_response(
                        &results, offset, count,
                    )
                }
            }
            // MutexGuard dropped here
        };
        return response;
    }

    // ── Multi-shard fan-out ──────────────────────────────────────────────────
    let mut receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_response: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            let response = {
                let ts = shard_databases.text_store(my_shard);
                match ts.get_index(&index_name) {
                    None => Frame::Error(Bytes::from_static(b"ERR no such index")),
                    Some(text_index) => {
                        let clause =
                            crate::command::vector_search::ft_text_search::TextQueryClause {
                                field_name: None,
                                terms: Vec::new(),
                                filter: Some(filter.clone()),
                            };
                        let results =
                            crate::command::vector_search::ft_text_search::execute_query_on_index(
                                text_index, &clause, None, None, top_k,
                            );
                        // Each shard returns top_k; coordinator applies final offset+count
                        // after merging.
                        crate::command::vector_search::ft_text_search::build_text_response(
                            &results, 0, top_k,
                        )
                    }
                }
                // MutexGuard dropped here, before the next `.await`
            };
            local_response = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::InvertedSearch(Box::new(
                crate::shard::dispatch::InvertedSearchPayload {
                    index_name: index_name.clone(),
                    filter: filter.clone(),
                    top_k,
                    offset: 0,
                    count: top_k,
                    reply_tx,
                },
            ));
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            receivers.push(reply_rx);
        }
    }

    let mut responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_response {
        responses.push(local);
    }
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR InvertedSearch channel closed unexpectedly",
                ));
            }
        }
    }

    // Uniform score=0.0 across shards; merge_text_results stabilizes order.
    crate::command::vector_search::merge_text_results(&responses, top_k, offset, count)
}

/// Aggregate document frequencies from multiple shard `DocFreq` responses.
///
/// Each response is a `Frame::Array` with interleaved `[term, df, ..., "N", n]` entries.
/// This function sums `df` per term across shards and sums `N` (total docs) across shards.
///
/// Returns `(global_df: HashMap<String, u32>, global_n: u32)`.
pub(crate) fn aggregate_doc_freq(
    responses: &[Frame],
) -> (std::collections::HashMap<String, u32>, u32) {
    let mut global_df: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut global_n: u32 = 0;

    for resp in responses {
        let items = match resp {
            Frame::Array(items) => items,
            _ => continue, // Skip error frames from shards that don't have the index
        };

        let mut i = 0;
        while i + 1 < items.len() {
            match &items[i] {
                Frame::BulkString(key) => {
                    if key.as_ref() == b"N" {
                        // "N" sentinel: next item is the total doc count for this shard
                        if let Frame::Integer(n) = &items[i + 1] {
                            global_n = global_n.saturating_add(*n as u32);
                        }
                        i += 2;
                    } else {
                        // term -> df pair
                        let term = match std::str::from_utf8(key) {
                            Ok(s) => s.to_owned(),
                            Err(_) => {
                                i += 2;
                                continue;
                            }
                        };
                        if let Frame::Integer(df) = &items[i + 1] {
                            *global_df.entry(term).or_insert(0) = global_df
                                .get(&term)
                                .copied()
                                .unwrap_or(0)
                                .saturating_add(*df as u32);
                        }
                        i += 2;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }
    }

    (global_df, global_n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btreemap_ascending_order() {
        // BTreeMap guarantees ascending shard order -- VLL deadlock prevention
        let keys = vec![
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
            Bytes::from_static(b"key3"),
            Bytes::from_static(b"key4"),
        ];
        let mut groups: BTreeMap<usize, Vec<Bytes>> = BTreeMap::new();
        for key in &keys {
            let shard = key_to_shard(key, 4);
            groups.entry(shard).or_default().push(key.clone());
        }
        let shard_ids: Vec<usize> = groups.keys().copied().collect();
        for i in 1..shard_ids.len() {
            assert!(
                shard_ids[i] > shard_ids[i - 1],
                "BTreeMap should yield ascending shard IDs"
            );
        }
    }

    #[test]
    fn test_hash_tag_co_location() {
        let keys = vec![
            Bytes::from_static(b"{user}.name"),
            Bytes::from_static(b"{user}.email"),
            Bytes::from_static(b"{user}.age"),
        ];
        let mut shards: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for key in &keys {
            shards.insert(key_to_shard(key, 8));
        }
        assert_eq!(
            shards.len(),
            1,
            "all keys with same hash tag should map to one shard"
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_mget_all_local() {
        use crate::storage::Database;
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));

        let shard_databases = ShardDatabases::new(vec![dbs]);
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ];

        // With num_shards=1, all keys are local
        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let result = coordinate_mget(
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            &response_pool,
        )
        .await;
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Frame::BulkString(Bytes::from_static(b"1")));
                assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"2")));
            }
            _ => panic!("expected Array response"),
        }
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_mset_all_local() {
        use crate::storage::Database;
        let dbs = vec![Database::new()];
        let shard_databases = ShardDatabases::new(vec![dbs]);
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"x")),
            Frame::BulkString(Bytes::from_static(b"10")),
            Frame::BulkString(Bytes::from_static(b"y")),
            Frame::BulkString(Bytes::from_static(b"20")),
        ];

        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let result = coordinate_mset(
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            &response_pool,
        )
        .await;
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify keys were set
        let mut guard = shard_databases.write_db(0, 0);
        guard.refresh_now_from_cache(&cached_clock);
        let entry = guard.get(b"x");
        assert!(entry.is_some());
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_del_all_local() {
        use crate::storage::Database;
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        dbs[0].set_string(Bytes::from_static(b"c"), Bytes::from_static(b"3"));

        let shard_databases = ShardDatabases::new(vec![dbs]);
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
            Frame::BulkString(Bytes::from_static(b"nonexistent")),
        ];

        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let result = coordinate_multi_del_or_exists(
            b"DEL",
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            &response_pool,
        )
        .await;
        assert_eq!(result, Frame::Integer(2)); // a and b deleted, nonexistent = 0
    }

    // ── aggregate_doc_freq tests ───────────────────────────────────────────────

    /// Helper: build a DocFreq response frame from a list of (term, df) pairs + N.
    fn make_doc_freq_frame(term_dfs: &[(&str, u32)], n: u32) -> Frame {
        let mut items: Vec<Frame> = Vec::new();
        for (term, df) in term_dfs {
            items.push(Frame::BulkString(Bytes::copy_from_slice(term.as_bytes())));
            items.push(Frame::Integer(i64::from(*df)));
        }
        items.push(Frame::BulkString(Bytes::from_static(b"N")));
        items.push(Frame::Integer(i64::from(n)));
        Frame::Array(items.into())
    }

    #[test]
    fn test_aggregate_doc_freq_two_shards() {
        // Shard A: "machine" df=3, N=10
        // Shard B: "machine" df=5, N=15
        // Global: "machine" df=8, N=25
        let shard_a = make_doc_freq_frame(&[("machine", 3)], 10);
        let shard_b = make_doc_freq_frame(&[("machine", 5)], 15);

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 25, "global N should be 10+15=25");
        assert_eq!(
            global_df.get("machine").copied(),
            Some(8),
            "global df for 'machine' should be 3+5=8"
        );
    }

    #[test]
    fn test_aggregate_doc_freq_missing_term_on_one_shard() {
        // Shard A: "rare" df=1, N=10
        // Shard B: no "rare" entry, N=8
        // Global: "rare" df=1, N=18
        let shard_a = make_doc_freq_frame(&[("rare", 1)], 10);
        let shard_b = make_doc_freq_frame(&[], 8); // empty term list, just N=8

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 18, "global N should be 10+8=18");
        assert_eq!(
            global_df.get("rare").copied(),
            Some(1),
            "global df for 'rare' should be 1 (only present on shard A)"
        );
    }

    #[test]
    fn test_aggregate_doc_freq_multiple_terms() {
        // Two shards each have two terms
        let shard_a = make_doc_freq_frame(&[("rust", 4), ("async", 2)], 20);
        let shard_b = make_doc_freq_frame(&[("rust", 6), ("async", 3)], 30);

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 50, "global N should be 20+30=50");
        assert_eq!(global_df.get("rust").copied(), Some(10), "rust df=4+6=10");
        assert_eq!(global_df.get("async").copied(), Some(5), "async df=2+3=5");
    }

    #[test]
    fn test_aggregate_doc_freq_error_frame_skipped() {
        // If one shard returns an error (e.g. index not found), it should be skipped.
        let shard_a = make_doc_freq_frame(&[("term", 3)], 10);
        let shard_err = Frame::Error(Bytes::from_static(b"ERR unknown index"));

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_err]);

        // Error frame should be skipped; only shard_a contributes
        assert_eq!(global_n, 10);
        assert_eq!(global_df.get("term").copied(), Some(3));
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_scatter_text_search_single_shard_skips_dfs() {
        // Single-shard (num_shards==1): scatter_text_search must return immediately
        // from execute_text_search_local without sending any DocFreq or TextSearch
        // ShardMessages via SPSC. We verify this by:
        //   1. Passing an empty dispatch_tx (no SPSC channels — would panic if used)
        //   2. Verifying the result is an Array (success format, not a channel error)
        //
        // We use an empty TextStore (no indexes), so the result is "ERR no such index".
        // That's still the correct single-shard path — no channels were touched.
        use crate::storage::Database;

        let dbs = vec![Database::new()];
        let shard_databases = Arc::new(ShardDatabases::new(vec![dbs]));

        // Empty dispatch_tx — any SPSC send would panic (no channels configured).
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));
        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();

        let result = scatter_text_search(
            Bytes::from_static(b"nonexistent_index"),
            vec![crate::command::vector_search::QueryTerm {
                text: "machine".to_owned(),
                #[cfg(feature = "text-index")]
                modifier: crate::text::store::TermModifier::Exact,
            }],
            None, // cross-field
            10,
            0,
            10,
            0, // my_shard
            1, // num_shards = 1 -> single-shard fast path
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            None, // highlight_opts
            None, // summarize_opts
        )
        .await;

        // Should be "ERR no such index" (local execute_text_search_local path),
        // NOT a channel error. This proves the DFS pre-pass was skipped entirely.
        match &result {
            Frame::Error(e) => {
                let msg = std::str::from_utf8(e).unwrap_or("");
                assert!(
                    msg.contains("no such index"),
                    "expected 'no such index' error for missing index, got: {}",
                    msg
                );
            }
            other => panic!("expected Error frame, got: {:?}", other),
        }
    }
}
