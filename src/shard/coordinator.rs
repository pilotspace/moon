//! VLL (Very Lightweight Locking) coordinator for multi-key cross-shard operations.
//!
//! Coordinates MGET, MSET, multi-DEL and similar commands that span multiple shards.
//! Groups sub-operations by target shard, dispatches via SPSC, collects results.

use std::cell::RefCell;
use std::rc::Rc;

use bytes::Bytes;
use ringbuf::traits::Producer;
use ringbuf::HeapProd;

use crate::command::{dispatch, DispatchResult};
use crate::protocol::Frame;
use crate::storage::Database;

use super::dispatch::{key_to_shard, ShardMessage};
use super::mesh::ChannelMesh;

/// Coordinate a multi-key command across shards.
///
/// Groups keys by target shard, executes local keys directly via dispatch,
/// dispatches remote keys via SPSC channels, and assembles the combined result.
pub async fn coordinate_multi_key(
    cmd: &[u8],
    args: &[Frame],
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
) -> Frame {
    if cmd.eq_ignore_ascii_case(b"MGET") {
        coordinate_mget(args, shard_id, num_shards, selected_db, databases, dispatch_tx).await
    } else if cmd.eq_ignore_ascii_case(b"MSET") {
        coordinate_mset(args, shard_id, num_shards, selected_db, databases, dispatch_tx).await
    } else {
        // DEL, UNLINK, EXISTS with multiple keys
        coordinate_multi_del(cmd, args, shard_id, num_shards, selected_db, databases, dispatch_tx).await
    }
}

/// Helper: dispatch a single command locally via the standard dispatch function.
fn dispatch_local(
    databases: &Rc<RefCell<Vec<Database>>>,
    selected_db: usize,
    cmd: &[u8],
    args: &[Frame],
) -> Frame {
    let mut dbs = databases.borrow_mut();
    let db_count = dbs.len();
    let db_idx = selected_db.min(db_count.saturating_sub(1));
    dbs[db_idx].refresh_now();
    let mut sel = db_idx;
    let result = dispatch(&mut dbs[db_idx], cmd, args, &mut sel, db_count);
    match result {
        DispatchResult::Response(f) => f,
        DispatchResult::Quit(f) => f,
    }
}

/// Helper: dispatch a command to a remote shard via SPSC.
async fn dispatch_remote(
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    shard_id: usize,
    target: usize,
    selected_db: usize,
    command: Frame,
) -> Frame {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let msg = ShardMessage::Execute {
        db_index: selected_db,
        command,
        reply_tx,
    };
    let target_idx = ChannelMesh::target_index(shard_id, target);
    {
        let mut producers = dispatch_tx.borrow_mut();
        let mut pending = msg;
        loop {
            match producers[target_idx].try_push(pending) {
                Ok(()) => break,
                Err(val) => {
                    pending = val;
                    drop(producers);
                    tokio::task::yield_now().await;
                    producers = dispatch_tx.borrow_mut();
                }
            }
        }
    }
    match reply_rx.await {
        Ok(response) => response,
        Err(_) => Frame::Error(Bytes::from_static(b"ERR cross-shard dispatch failed")),
    }
}

/// Coordinate MGET across shards.
async fn coordinate_mget(
    args: &[Frame],
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'mget' command"));
    }

    let mut results = vec![Frame::Null; args.len()];

    for (i, arg) in args.iter().enumerate() {
        if let Frame::BulkString(key) = arg {
            let target = key_to_shard(key, num_shards);
            let get_cmd = Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"GET")),
                Frame::BulkString(key.clone()),
            ]);
            if target == shard_id {
                results[i] = dispatch_local(databases, selected_db, b"GET", &[arg.clone()]);
            } else {
                results[i] = dispatch_remote(dispatch_tx, shard_id, target, selected_db, get_cmd).await;
            }
        }
    }

    Frame::Array(results)
}

/// Coordinate MSET across shards.
async fn coordinate_mset(
    args: &[Frame],
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
) -> Frame {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'mset' command"));
    }

    for pair in args.chunks(2) {
        if let Frame::BulkString(key) = &pair[0] {
            let target = key_to_shard(key, num_shards);
            let set_cmd = Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                pair[0].clone(),
                pair[1].clone(),
            ]);
            if target == shard_id {
                dispatch_local(databases, selected_db, b"SET", &[pair[0].clone(), pair[1].clone()]);
            } else {
                let _ = dispatch_remote(dispatch_tx, shard_id, target, selected_db, set_cmd).await;
            }
        }
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Coordinate multi-key DEL/UNLINK/EXISTS across shards.
async fn coordinate_multi_del(
    cmd: &[u8],
    args: &[Frame],
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    databases: &Rc<RefCell<Vec<Database>>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
) -> Frame {
    let mut total: i64 = 0;

    for arg in args {
        if let Frame::BulkString(key) = arg {
            let target = key_to_shard(key, num_shards);
            let single_cmd = Frame::Array(vec![
                Frame::BulkString(Bytes::from(cmd.to_ascii_uppercase())),
                Frame::BulkString(key.clone()),
            ]);
            if target == shard_id {
                let response = dispatch_local(databases, selected_db, cmd, &[arg.clone()]);
                if let Frame::Integer(n) = response {
                    total += n;
                }
            } else {
                let response = dispatch_remote(dispatch_tx, shard_id, target, selected_db, single_cmd).await;
                if let Frame::Integer(n) = response {
                    total += n;
                }
            }
        }
    }

    Frame::Integer(total)
}
