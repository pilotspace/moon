//! VLL multi-key coordination for MGET, MSET, DEL, UNLINK, EXISTS.
//!
//! Groups keys by target shard in a BTreeMap (ascending shard-ID order for
//! deadlock prevention -- VLL pattern), dispatches to each shard, collects
//! results, and assembles the final response. Local-shard keys are executed
//! directly without SPSC overhead.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use bytes::Bytes;
use ringbuf::traits::Producer;
use ringbuf::HeapProd;

use crate::command::{dispatch as cmd_dispatch, DispatchResult};
use crate::protocol::Frame;
use crate::shard::dispatch::{key_to_shard, ShardMessage};
use crate::shard::mesh::ChannelMesh;
use crate::storage::Database;

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
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
) -> Frame {
    if cmd.eq_ignore_ascii_case(b"MGET") {
        coordinate_mget(args, my_shard, num_shards, db_index, databases, dispatch_tx).await
    } else if cmd.eq_ignore_ascii_case(b"MSET") {
        coordinate_mset(args, my_shard, num_shards, db_index, databases, dispatch_tx).await
    } else {
        // DEL, UNLINK, EXISTS with multiple keys
        coordinate_multi_del_or_exists(
            cmd,
            args,
            my_shard,
            num_shards,
            db_index,
            databases,
            dispatch_tx,
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
async fn spsc_send(
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    my_shard: usize,
    target_shard: usize,
    msg: ShardMessage,
) {
    let target_idx = ChannelMesh::target_index(my_shard, target_shard);
    let mut pending = msg;
    loop {
        let push_result = {
            let mut producers = dispatch_tx.borrow_mut();
            producers[target_idx].try_push(pending)
        }; // borrow dropped before yield
        match push_result {
            Ok(()) => return,
            Err(val) => {
                pending = val;
                tokio::task::yield_now().await;
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
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
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
        let mut dbs = databases.borrow_mut();
        dbs[db_index].refresh_now();
        return crate::command::string::mget(&mut dbs[db_index], args);
    }

    let total = args.len();
    let mut results: Vec<Option<Frame>> = vec![None; total];
    let mut pending_rxs: Vec<(Vec<usize>, tokio::sync::oneshot::Receiver<Vec<Frame>>)> = Vec::new();

    // Iterate in ascending shard-ID order (BTreeMap guarantees this)
    for (shard_id, indexed_keys) in &groups {
        let original_indices: Vec<usize> = indexed_keys.iter().map(|(i, _)| *i).collect();

        if *shard_id == my_shard {
            // Local execution: GET each key directly
            let mut dbs = databases.borrow_mut();
            dbs[db_index].refresh_now();
            for (orig_idx, key) in indexed_keys {
                let entry = dbs[db_index].get(key);
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
            // Remote dispatch: batch of GET commands via MultiExecute
            let (tx, rx) = tokio::sync::oneshot::channel();
            let commands: Vec<(Bytes, Frame)> = indexed_keys
                .iter()
                .map(|(_, k)| {
                    let cmd = Frame::Array(vec![
                        Frame::BulkString(Bytes::from_static(b"GET")),
                        Frame::BulkString(k.clone()),
                    ]);
                    (k.clone(), cmd)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx: tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg).await;
            pending_rxs.push((original_indices, rx));
        }
    }

    // Await all remote results
    for (indices, rx) in pending_rxs {
        match rx.await {
            Ok(frames) => {
                for (idx, frame) in indices.into_iter().zip(frames) {
                    results[idx] = Some(frame);
                }
            }
            Err(_) => {
                for idx in indices {
                    results[idx] = Some(Frame::Error(Bytes::from_static(
                        b"ERR cross-shard dispatch failed",
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
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
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
                ))
            }
        };
        let value = match extract_key(&pair[1]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'mset' command",
                ))
            }
        };
        let shard = key_to_shard(&key, num_shards);
        groups.entry(shard).or_default().push((key, value));
    }

    // Fast path: all keys on local shard
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        let mut dbs = databases.borrow_mut();
        dbs[db_index].refresh_now();
        return crate::command::string::mset(&mut dbs[db_index], args);
    }

    let mut pending_rxs: Vec<tokio::sync::oneshot::Receiver<Vec<Frame>>> = Vec::new();

    for (shard_id, kv_pairs) in &groups {
        if *shard_id == my_shard {
            let mut dbs = databases.borrow_mut();
            dbs[db_index].refresh_now();
            for (key, value) in kv_pairs {
                dbs[db_index].set_string(key.clone(), value.clone());
            }
        } else {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let commands: Vec<(Bytes, Frame)> = kv_pairs
                .iter()
                .map(|(k, v)| {
                    let cmd = Frame::Array(vec![
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
                reply_tx: tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg).await;
            pending_rxs.push(rx);
        }
    }

    for rx in pending_rxs {
        let _ = rx.await;
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
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
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
        let mut dbs = databases.borrow_mut();
        dbs[db_index].refresh_now();
        let mut selected = db_index;
        let db_count = dbs.len();
        let result = cmd_dispatch(&mut dbs[db_index], cmd, args, &mut selected, db_count);
        return match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        };
    }

    let mut total_count: i64 = 0;
    let mut pending_rxs: Vec<tokio::sync::oneshot::Receiver<Vec<Frame>>> = Vec::new();

    for (shard_id, key_args) in &groups {
        if *shard_id == my_shard {
            let mut dbs = databases.borrow_mut();
            dbs[db_index].refresh_now();
            let db_count = dbs.len();
            let mut selected = db_index;
            let result =
                cmd_dispatch(&mut dbs[db_index], cmd, key_args, &mut selected, db_count);
            if let DispatchResult::Response(Frame::Integer(n)) = result {
                total_count += n;
            }
        } else {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let commands: Vec<(Bytes, Frame)> = key_args
                .iter()
                .map(|arg| {
                    let key = extract_key(arg).unwrap_or_default();
                    let cmd_frame = Frame::Array(vec![
                        Frame::BulkString(Bytes::from(cmd_upper.clone())),
                        arg.clone(),
                    ]);
                    (key, cmd_frame)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx: tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg).await;
            pending_rxs.push(rx);
        }
    }

    for rx in pending_rxs {
        if let Ok(frames) = rx.await {
            for frame in frames {
                if let Frame::Integer(n) = frame {
                    total_count += n;
                }
            }
        }
    }

    Frame::Integer(total_count)
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

    #[tokio::test]
    async fn test_coordinate_mget_all_local() {
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));

        let databases = Rc::new(RefCell::new(dbs));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ];

        // With num_shards=1, all keys are local
        let result =
            coordinate_mget(&args, 0, 1, 0, &databases, &dispatch_tx).await;
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Frame::BulkString(Bytes::from_static(b"1")));
                assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"2")));
            }
            _ => panic!("expected Array response"),
        }
    }

    #[tokio::test]
    async fn test_coordinate_mset_all_local() {
        let dbs = vec![Database::new()];
        let databases = Rc::new(RefCell::new(dbs));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"x")),
            Frame::BulkString(Bytes::from_static(b"10")),
            Frame::BulkString(Bytes::from_static(b"y")),
            Frame::BulkString(Bytes::from_static(b"20")),
        ];

        let result = coordinate_mset(&args, 0, 1, 0, &databases, &dispatch_tx).await;
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify keys were set
        let mut dbs = databases.borrow_mut();
        dbs[0].refresh_now();
        let entry = dbs[0].get(b"x");
        assert!(entry.is_some());
    }

    #[tokio::test]
    async fn test_coordinate_del_all_local() {
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        dbs[0].set_string(Bytes::from_static(b"c"), Bytes::from_static(b"3"));

        let databases = Rc::new(RefCell::new(dbs));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
            Frame::BulkString(Bytes::from_static(b"nonexistent")),
        ];

        let result =
            coordinate_multi_del_or_exists(b"DEL", &args, 0, 1, 0, &databases, &dispatch_tx).await;
        assert_eq!(result, Frame::Integer(2)); // a and b deleted, nonexistent = 0
    }
}
