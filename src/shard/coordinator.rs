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
async fn spsc_send(
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
    if args.is_empty() || args.len() % 2 != 0 {
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
                    if let Frame::Integer(n) = frame {
                        total_count += n;
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
}
