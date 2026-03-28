//! SPSC message drain and cross-shard command dispatch handlers.
//!
//! Extracted from shard/mod.rs to reduce file size. These are synchronous
//! functions called from the event loop's select! arms.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use ringbuf::HeapCons;
use ringbuf::traits::Consumer;
use tracing::info;

use crate::blocking::BlockingRegistry;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch as cmd_dispatch};
use crate::persistence::aof;
use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal::WalWriter;
use crate::pubsub::PubSubRegistry;
use crate::replication::backlog::ReplicationBacklog;
use crate::replication::state::ReplicationState;
use crate::runtime::channel;
use crate::storage::Database;
use crate::storage::entry::CachedClock;

use super::dispatch::ShardMessage;

/// Drain all SPSC consumer channels, processing cross-shard messages.
///
/// SnapshotBegin messages are collected into `pending_snapshot` for deferred handling
/// (the caller has mutable access to snapshot_state). COW intercepts and WAL appends
/// happen inline for Execute/MultiExecute write commands.
pub(crate) fn drain_spsc_shared(
    databases: &Rc<RefCell<Vec<Database>>>,
    consumers: &mut [HeapCons<ShardMessage>],
    pubsub_registry: &mut PubSubRegistry,
    blocking_registry: &Rc<RefCell<BlockingRegistry>>,
    pending_snapshot: &mut Option<(
        u64,
        std::path::PathBuf,
        channel::OneshotSender<Result<(), String>>,
    )>,
    snapshot_state: &mut Option<SnapshotState>,
    wal_writer: &mut Option<WalWriter>,
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &mut Vec<(u64, channel::MpscSender<bytes::Bytes>)>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
    script_cache: &Rc<RefCell<crate::scripting::ScriptCache>>,
    cached_clock: &CachedClock,
) {
    const MAX_DRAIN_PER_CYCLE: usize = 256;
    let mut drained = 0;

    // Collect all messages first, then batch Execute/PipelineBatch under single borrow.
    let mut execute_batch: Vec<ShardMessage> = Vec::new();
    let mut other_messages: Vec<ShardMessage> = Vec::new();

    for consumer in consumers.iter_mut() {
        while drained < MAX_DRAIN_PER_CYCLE {
            match consumer.try_pop() {
                Some(msg) => {
                    drained += 1;
                    match &msg {
                        ShardMessage::Execute { .. }
                        | ShardMessage::PipelineBatch { .. }
                        | ShardMessage::MultiExecute { .. } => {
                            execute_batch.push(msg);
                        }
                        _ => other_messages.push(msg),
                    }
                }
                None => break,
            }
        }
        if drained >= MAX_DRAIN_PER_CYCLE {
            break;
        }
    }

    // Process Execute/PipelineBatch/MultiExecute batch under single borrow_mut
    if !execute_batch.is_empty() {
        for msg in execute_batch {
            handle_shard_message_shared(
                databases,
                pubsub_registry,
                blocking_registry,
                msg,
                pending_snapshot,
                snapshot_state,
                wal_writer,
                repl_backlog,
                replica_txs,
                repl_state,
                shard_id,
                script_cache,
                cached_clock,
            );
        }
    }

    // Process other messages (PubSubFanOut, SnapshotBegin, etc.)
    for msg in other_messages {
        handle_shard_message_shared(
            databases,
            pubsub_registry,
            blocking_registry,
            msg,
            pending_snapshot,
            snapshot_state,
            wal_writer,
            repl_backlog,
            replica_txs,
            repl_state,
            shard_id,
            script_cache,
            cached_clock,
        );
    }
}

/// Process a single cross-shard message using shared database access.
///
/// Performs COW intercept for write commands when a snapshot is active,
/// and appends write commands to the per-shard WAL writer.
pub(crate) fn handle_shard_message_shared(
    databases: &Rc<RefCell<Vec<Database>>>,
    pubsub_registry: &mut PubSubRegistry,
    blocking_registry: &Rc<RefCell<BlockingRegistry>>,
    msg: ShardMessage,
    pending_snapshot: &mut Option<(
        u64,
        std::path::PathBuf,
        channel::OneshotSender<Result<(), String>>,
    )>,
    snapshot_state: &mut Option<SnapshotState>,
    wal_writer: &mut Option<WalWriter>,
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &mut Vec<(u64, channel::MpscSender<bytes::Bytes>)>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
    script_cache: &Rc<RefCell<crate::scripting::ScriptCache>>,
    cached_clock: &CachedClock,
) {
    match msg {
        ShardMessage::Execute {
            db_index,
            command,
            reply_tx,
        } => {
            let response = {
                let mut dbs = databases.borrow_mut();
                let db_count = dbs.len();
                let db_idx = db_index.min(db_count.saturating_sub(1));
                dbs[db_idx].refresh_now_from_cache(cached_clock);
                let (cmd, args) = match extract_command_static(&command) {
                    Some(pair) => pair,
                    None => {
                        let _ = reply_tx.send(crate::protocol::Frame::Error(
                            bytes::Bytes::from_static(b"ERR invalid command format"),
                        ));
                        return;
                    }
                };

                // COW intercept: capture old value before write if snapshot is active
                let is_write = metadata::is_write(cmd);
                if is_write {
                    cow_intercept(snapshot_state, &dbs, db_idx, &command);
                }

                let mut selected = db_idx;
                let result = cmd_dispatch(&mut dbs[db_idx], cmd, args, &mut selected, db_count);
                let frame = match result {
                    DispatchResult::Response(f) => f,
                    DispatchResult::Quit(f) => f,
                };

                // WAL append + replication fan-out for successful write commands
                if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let serialized = aof::serialize_command(&command);
                    wal_append_and_fanout(
                        &serialized,
                        wal_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );
                }

                // Post-dispatch wakeup hooks for producer commands (cross-shard blocking)
                if !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD")
                        || cmd.eq_ignore_ascii_case(b"XADD");
                    if needs_wake {
                        if let Some(key) = args
                            .first()
                            .and_then(|f| crate::server::connection::extract_bytes(f))
                        {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg,
                                    &mut dbs[db_idx],
                                    db_idx,
                                    &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg,
                                    &mut dbs[db_idx],
                                    db_idx,
                                    &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg,
                                    &mut dbs[db_idx],
                                    db_idx,
                                    &key,
                                );
                            }
                        }
                    }
                }

                frame
            };
            let _ = reply_tx.send(response);
        }
        ShardMessage::MultiExecute {
            db_index,
            commands,
            reply_tx,
        } => {
            let mut results = Vec::with_capacity(commands.len());
            let mut dbs = databases.borrow_mut();
            let db_count = dbs.len();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            dbs[db_idx].refresh_now_from_cache(cached_clock);
            for (_key, cmd_frame) in &commands {
                let (cmd, args) = match extract_command_static(cmd_frame) {
                    Some(pair) => pair,
                    None => {
                        results.push(crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            b"ERR invalid command format",
                        )));
                        continue;
                    }
                };

                // COW intercept for each write command in the batch
                let is_write = metadata::is_write(cmd);
                if is_write {
                    cow_intercept(snapshot_state, &dbs, db_idx, cmd_frame);
                }

                let mut selected = db_idx;
                let result = cmd_dispatch(&mut dbs[db_idx], cmd, args, &mut selected, db_count);
                let frame = match result {
                    DispatchResult::Response(f) => f,
                    DispatchResult::Quit(f) => f,
                };

                // WAL append + replication fan-out for successful write commands
                if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let serialized = aof::serialize_command(cmd_frame);
                    wal_append_and_fanout(
                        &serialized,
                        wal_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );
                }

                results.push(frame);
            }
            let _ = reply_tx.send(results);
        }
        ShardMessage::PipelineBatch {
            db_index,
            commands,
            reply_tx,
        } => {
            let mut results = Vec::with_capacity(commands.len());
            let mut dbs = databases.borrow_mut();
            let db_count = dbs.len();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            dbs[db_idx].refresh_now_from_cache(cached_clock);
            for cmd_frame in &commands {
                let (cmd, args) = match extract_command_static(cmd_frame) {
                    Some(pair) => pair,
                    None => {
                        results.push(crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            b"ERR invalid command format",
                        )));
                        continue;
                    }
                };

                // COW intercept for each write command in the batch
                let is_write = metadata::is_write(cmd);
                if is_write {
                    cow_intercept(snapshot_state, &dbs, db_idx, cmd_frame);
                }

                let mut selected = db_idx;
                let result = cmd_dispatch(&mut dbs[db_idx], cmd, args, &mut selected, db_count);
                let frame = match result {
                    DispatchResult::Response(f) => f,
                    DispatchResult::Quit(f) => f,
                };

                // WAL append + replication fan-out for successful write commands
                if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let serialized = aof::serialize_command(cmd_frame);
                    wal_append_and_fanout(
                        &serialized,
                        wal_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );
                }

                // Post-dispatch wakeup hooks for producer commands (cross-shard blocking)
                if !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD")
                        || cmd.eq_ignore_ascii_case(b"XADD");
                    if needs_wake {
                        if let Some(key) = args
                            .first()
                            .and_then(|f| crate::server::connection::extract_bytes(f))
                        {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg,
                                    &mut dbs[db_idx],
                                    db_idx,
                                    &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg,
                                    &mut dbs[db_idx],
                                    db_idx,
                                    &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg,
                                    &mut dbs[db_idx],
                                    db_idx,
                                    &key,
                                );
                            }
                        }
                    }
                }

                results.push(frame);
            }
            let _ = reply_tx.send(results);
        }
        ShardMessage::PubSubFanOut { channel, message } => {
            pubsub_registry.publish(&channel, &message);
        }
        ShardMessage::ScriptLoad { sha1, script } => {
            // Fan-out: cache this script on this shard so EVALSHA works locally
            let computed = sha1_smol::Sha1::from(&script[..]).hexdigest();
            if computed == sha1 {
                script_cache.borrow_mut().load(script);
            }
        }
        ShardMessage::SnapshotBegin {
            epoch,
            snapshot_dir,
            reply_tx,
        } => {
            // Defer to main event loop where we have mutable access to snapshot_state
            *pending_snapshot = Some((epoch, snapshot_dir, reply_tx));
        }
        ShardMessage::BlockRegister {
            db_index,
            key,
            wait_id,
            cmd,
            reply_tx,
        } => {
            let entry = crate::blocking::WaitEntry {
                wait_id,
                cmd,
                reply_tx,
                deadline: None, // Remote registrations don't manage timeout locally
            };
            let mut reg = blocking_registry.borrow_mut();
            reg.register(db_index, key.clone(), entry);
            // Check if data is already available (race: data arrived before registration).
            let mut dbs = databases.borrow_mut();
            if dbs[db_index].exists(&key) {
                let db = &mut dbs[db_index];
                crate::blocking::wakeup::try_wake_list_waiter(&mut reg, db, db_index, &key);
                crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, db, db_index, &key);
                crate::blocking::wakeup::try_wake_stream_waiter(&mut reg, db, db_index, &key);
            }
        }
        ShardMessage::BlockCancel { wait_id } => {
            blocking_registry.borrow_mut().remove_wait(wait_id);
        }
        ShardMessage::GetKeysInSlot {
            db_index,
            slot,
            count,
            reply_tx,
        } => {
            let keys = crate::cluster::migration::handle_get_keys_in_slot(
                &databases.borrow(),
                db_index,
                slot,
                count,
            );
            let _ = reply_tx.send(keys);
        }
        ShardMessage::SlotOwnershipUpdate {
            add_slots: _,
            remove_slots: _,
        } => {
            // Slot ownership is tracked in ClusterState, not per-shard.
        }
        ShardMessage::Shutdown => {
            info!("Received shutdown via SPSC");
        }
        ShardMessage::RegisterReplica { replica_id, tx } => {
            // Lazy-init replication backlog on first replica registration (saves 1MB/shard)
            if repl_backlog.is_none() {
                *repl_backlog = Some(ReplicationBacklog::new(1024 * 1024));
            }
            replica_txs.push((replica_id, tx));
        }
        ShardMessage::UnregisterReplica { replica_id } => {
            replica_txs.retain(|(id, _)| *id != replica_id);
        }
        ShardMessage::NewConnection(_) => {
            // NewConnection is handled via conn_rx, not SPSC
        }
    }
}

/// COW intercept: capture old value for a key being written if its segment is pending.
///
/// Called before cmd_dispatch to preserve snapshot consistency. Only clones the old entry
/// if the key's segment is actually pending serialization (fast bool check in hot path).
pub(crate) fn cow_intercept(
    snapshot: &mut Option<SnapshotState>,
    dbs: &[Database],
    db_index: usize,
    command: &crate::protocol::Frame,
) {
    let Some(snap) = snapshot else { return };
    // Extract the primary key from the command (args[1] for Array commands)
    let key = match command {
        crate::protocol::Frame::Array(args) if args.len() >= 2 => match &args[1] {
            crate::protocol::Frame::BulkString(k) => k,
            _ => return,
        },
        _ => return,
    };
    let hash = crate::storage::dashtable::hash_key(key);
    let seg_idx = dbs[db_index].data().segment_index_for_hash(hash);
    if snap.is_segment_pending(db_index, seg_idx) {
        if let Some(old_entry) = dbs[db_index].data().get(key) {
            snap.capture_cow(db_index, seg_idx, key.clone(), old_entry.clone());
        }
    }
}

/// Append WAL bytes, update the replication backlog, advance the monotonic shard offset,
/// and fan-out to all connected replica sender channels (non-blocking try_send).
///
/// CRITICAL: shard_offset in ReplicationState is SEPARATE from WalWriter::bytes_written.
/// WalWriter::bytes_written resets on snapshot truncation; shard_offset NEVER resets.
pub(crate) fn wal_append_and_fanout(
    data: &[u8],
    wal_writer: &mut Option<WalWriter>,
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &[(u64, channel::MpscSender<bytes::Bytes>)],
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
) {
    // 1. WAL append (disk durability, unchanged behavior)
    if let Some(w) = wal_writer {
        w.append(data);
    }
    // 2. Replication backlog (in-memory circular buffer for partial resync)
    if let Some(backlog) = repl_backlog {
        backlog.append(data);
    }
    // 3. Advance monotonic replication offset (NEVER resets on WAL truncation)
    if let Some(rs) = repl_state {
        if let Ok(rs) = rs.try_read() {
            rs.increment_shard_offset(shard_id, data.len() as u64);
        }
    }
    // 4. Fan-out to replica sender tasks (non-blocking: lagging replicas are skipped)
    if !replica_txs.is_empty() {
        let bytes = bytes::Bytes::copy_from_slice(data);
        for (_id, tx) in replica_txs {
            let _ = tx.try_send(bytes.clone());
        }
    }
}

/// Extract command name and args from a Frame (static helper for SPSC dispatch).
pub(crate) fn extract_command_static(
    frame: &crate::protocol::Frame,
) -> Option<(&[u8], &[crate::protocol::Frame])> {
    match frame {
        crate::protocol::Frame::Array(args) if !args.is_empty() => {
            let name = match &args[0] {
                crate::protocol::Frame::BulkString(s) => s.as_ref(),
                crate::protocol::Frame::SimpleString(s) => s.as_ref(),
                _ => return None,
            };
            Some((name, &args[1..]))
        }
        _ => None,
    }
}
