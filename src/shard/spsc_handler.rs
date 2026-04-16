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
use crate::persistence::wal_v3::segment::WalWriterV3;
use crate::pubsub::PubSubRegistry;
use crate::replication::backlog::ReplicationBacklog;
use crate::replication::state::ReplicationState;
use crate::runtime::channel;
use crate::storage::Database;
use crate::storage::entry::CachedClock;

use crate::command::vector_search;
use crate::vector::store::VectorStore;

use super::dispatch::ShardMessage;
use super::shared_databases::ShardDatabases;

/// Drain all SPSC consumer channels, processing cross-shard messages.
///
/// SnapshotBegin messages are collected into `pending_snapshot` for deferred handling
/// (the caller has mutable access to snapshot_state). COW intercepts and WAL appends
/// happen inline for Execute/MultiExecute write commands.
#[tracing::instrument(skip_all, level = "debug")]
pub(crate) fn drain_spsc_shared(
    shard_databases: &Arc<ShardDatabases>,
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
    wal_v3_writer: &mut Option<WalWriterV3>,
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &mut Vec<(u64, channel::MpscSender<bytes::Bytes>)>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
    script_cache: &Rc<RefCell<crate::scripting::ScriptCache>>,
    cached_clock: &CachedClock,
    pending_migrations: &mut Vec<(
        std::os::unix::io::RawFd,
        crate::server::conn::affinity::MigratedConnectionState,
    )>,
    vector_store: &mut VectorStore,
) {
    const MAX_DRAIN_PER_CYCLE: usize = 256;
    let mut drained = 0;

    // Collect all messages first, then batch Execute/PipelineBatch under single borrow.
    let mut execute_batch: Vec<ShardMessage> = Vec::new();
    let mut other_messages: Vec<ShardMessage> = Vec::new();

    let mut snapshot_seen = false;
    for consumer in consumers.iter_mut() {
        if snapshot_seen {
            break;
        }
        while drained < MAX_DRAIN_PER_CYCLE {
            match consumer.try_pop() {
                Some(msg) => {
                    drained += 1;
                    // Stop draining once a SnapshotBegin arrives so later writes
                    // aren't processed before the snapshot captures current state.
                    if matches!(&msg, ShardMessage::SnapshotBegin { .. }) {
                        other_messages.push(msg);
                        snapshot_seen = true;
                        break;
                    }
                    match msg {
                        ShardMessage::Execute { .. }
                        | ShardMessage::PipelineBatch { .. }
                        | ShardMessage::MultiExecute { .. }
                        | ShardMessage::ExecuteSlotted { .. }
                        | ShardMessage::PipelineBatchSlotted { .. }
                        | ShardMessage::MultiExecuteSlotted { .. }
                        | ShardMessage::VectorSearch { .. }
                        | ShardMessage::VectorCommand { .. } => {
                            execute_batch.push(msg);
                        }
                        #[cfg(feature = "graph")]
                        ShardMessage::GraphCommand { .. } => {
                            execute_batch.push(msg);
                        }
                        #[cfg(feature = "graph")]
                        ShardMessage::GraphTraverse { .. } => {
                            execute_batch.push(msg);
                        }
                        ShardMessage::MigrateConnection { fd, state } => {
                            pending_migrations.push((fd, state));
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
                shard_databases,
                pubsub_registry,
                blocking_registry,
                msg,
                pending_snapshot,
                snapshot_state,
                wal_writer,
                wal_v3_writer,
                repl_backlog,
                replica_txs,
                repl_state,
                shard_id,
                script_cache,
                cached_clock,
                vector_store,
            );
        }
    }

    if drained > 0 {
        crate::admin::metrics_setup::record_spsc_drain(shard_id, drained as u64);
    }

    // Process other messages (PubSubPublish, SnapshotBegin, etc.)
    for msg in other_messages {
        handle_shard_message_shared(
            shard_databases,
            pubsub_registry,
            blocking_registry,
            msg,
            pending_snapshot,
            snapshot_state,
            wal_writer,
            wal_v3_writer,
            repl_backlog,
            replica_txs,
            repl_state,
            shard_id,
            script_cache,
            cached_clock,
            vector_store,
        );
    }
}

/// Process a single cross-shard message using shared database access.
///
/// Performs COW intercept for write commands when a snapshot is active,
/// and appends write commands to the per-shard WAL writer.
pub(crate) fn handle_shard_message_shared(
    shard_databases: &Arc<ShardDatabases>,
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
    wal_v3_writer: &mut Option<WalWriterV3>,
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &mut Vec<(u64, channel::MpscSender<bytes::Bytes>)>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
    script_cache: &Rc<RefCell<crate::scripting::ScriptCache>>,
    cached_clock: &CachedClock,
    vector_store: &mut VectorStore,
) {
    match msg {
        ShardMessage::Execute {
            db_index,
            command,
            reply_tx,
        } => {
            let response = {
                let db_count = shard_databases.db_count();
                let db_idx = db_index.min(db_count.saturating_sub(1));
                let (cmd, args) = match extract_command_static(&command) {
                    Some(pair) => pair,
                    None => {
                        let _ = reply_tx.send(crate::protocol::Frame::Error(
                            bytes::Bytes::from_static(b"ERR invalid command format"),
                        ));
                        return;
                    }
                };

                // FT.* commands route to VectorStore, not the KV Database.
                // Intercept before cmd_dispatch so the console gateway's
                // ShardMessage::Execute path reaches the vector handlers.
                if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
                    #[cfg(feature = "graph")]
                    let graph_guard = shard_databases.graph_store_read(shard_id);

                    // SESSION clause needs Database access for sorted set storage.
                    // Only acquire write lock when SESSION keyword is present.
                    // FT.NAVIGATE internally calls ft_search which may use SESSION.
                    // FT.RECOMMEND always needs Database access (reads hash keys).
                    let needs_db = cmd.eq_ignore_ascii_case(b"FT.RECOMMEND")
                        || ((cmd.eq_ignore_ascii_case(b"FT.SEARCH")
                            || cmd.eq_ignore_ascii_case(b"FT.NAVIGATE"))
                            && has_session_keyword(&command));
                    let mut db_guard;
                    let db_opt = if needs_db {
                        db_guard = shard_databases.write_db(shard_id, 0);
                        Some(&mut *db_guard)
                    } else {
                        None
                    };

                    let mut text_guard = shard_databases.text_store(shard_id);
                    let frame = dispatch_vector_command(
                        vector_store,
                        &mut *text_guard,
                        #[cfg(feature = "graph")]
                        Some(&graph_guard),
                        &command,
                        db_opt,
                    );
                    let _ = reply_tx.send(frame);
                    return;
                }

                // GRAPH.* commands route to GraphStore.
                #[cfg(feature = "graph")]
                if cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
                    let (frame, wal_records) = {
                        let mut gs = shard_databases.graph_store_write(shard_id);
                        let resp = crate::command::graph::dispatch_graph_command(&mut gs, &command);
                        let records = gs.drain_wal();
                        (resp, records)
                    };
                    for record in wal_records {
                        shard_databases.wal_append(shard_id, bytes::Bytes::from(record));
                    }
                    let _ = reply_tx.send(frame);
                    return;
                }

                // COW intercept: capture old value before write if snapshot is active
                let is_write = metadata::is_write(cmd);
                if is_write {
                    let db_guard = shard_databases.read_db(shard_id, db_idx);
                    cow_intercept(snapshot_state, &db_guard, db_idx, &command);
                    drop(db_guard);
                }

                let mut guard = shard_databases.write_db(shard_id, db_idx);
                guard.refresh_now_from_cache(cached_clock);
                let mut selected = db_idx;
                let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
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
                        wal_v3_writer,
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
                        // For LMOVE, wake waiters on the destination key (args[1]),
                        // not the source key (args[0]).
                        let wake_key = if cmd.eq_ignore_ascii_case(b"LMOVE") {
                            args.get(1)
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        } else {
                            args.first()
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        };
                        if let Some(key) = wake_key {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            }
                        }
                    }
                }

                // Auto-index: if HSET succeeded and key matches a vector index prefix,
                // extract the vector field and append to mutable segment.
                if cmd.eq_ignore_ascii_case(b"HSET")
                    && !matches!(frame, crate::protocol::Frame::Error(_))
                {
                    if let Some(crate::protocol::Frame::BulkString(key_bytes)) = args.first() {
                        auto_index_hset(vector_store, key_bytes, args);
                    }
                }

                // Auto-delete: if DEL/UNLINK succeeded and key matches a vector
                // index prefix, mark stale vectors as deleted in matching indexes.
                // Note: HDEL removes fields, not keys — it should NOT trigger vector
                // deletion unless the entire key is removed.
                if (cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK"))
                    && !matches!(frame, crate::protocol::Frame::Error(_))
                {
                    for arg in args {
                        if let crate::protocol::Frame::BulkString(key_bytes) = arg {
                            vector_store.mark_deleted_for_key(key_bytes);
                        }
                    }
                }

                drop(guard);
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
            let db_count = shard_databases.db_count();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            let mut guard = shard_databases.write_db(shard_id, db_idx);
            guard.refresh_now_from_cache(cached_clock);
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
                    cow_intercept(snapshot_state, &guard, db_idx, cmd_frame);
                }

                let mut selected = db_idx;
                let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
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
                        wal_v3_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );

                    // Wake blocked waiters for producer commands (same as Execute path)
                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD")
                        || cmd.eq_ignore_ascii_case(b"XADD");
                    if needs_wake {
                        let wake_key = if cmd.eq_ignore_ascii_case(b"LMOVE") {
                            args.get(1)
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        } else {
                            args.first()
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        };
                        if let Some(key) = wake_key {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            }
                        }
                    }
                }

                results.push(frame);
            }
            drop(guard);
            let _ = reply_tx.send(results);
        }
        ShardMessage::PipelineBatch {
            db_index,
            commands,
            reply_tx,
        } => {
            let mut results = Vec::with_capacity(commands.len());
            let db_count = shard_databases.db_count();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            let mut guard = shard_databases.write_db(shard_id, db_idx);
            guard.refresh_now_from_cache(cached_clock);
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
                    cow_intercept(snapshot_state, &guard, db_idx, cmd_frame);
                }

                let mut selected = db_idx;
                let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
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
                        wal_v3_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );
                }

                // Auto-index: if HSET succeeded, check for vector index match
                if cmd.eq_ignore_ascii_case(b"HSET")
                    && !matches!(frame, crate::protocol::Frame::Error(_))
                {
                    if let Some(crate::protocol::Frame::BulkString(key_bytes)) = args.first() {
                        // Use the `vector_store` parameter (already locked by caller),
                        // NOT shard_databases.vector_store() which would deadlock
                        // (parking_lot::Mutex is non-reentrant).
                        auto_index_hset(vector_store, key_bytes, args);
                    }
                }

                // Post-dispatch wakeup hooks for producer commands (cross-shard blocking)
                if !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD")
                        || cmd.eq_ignore_ascii_case(b"XADD");
                    if needs_wake {
                        // For LMOVE, wake waiters on the destination key (args[1]),
                        // not the source key (args[0]).
                        let wake_key = if cmd.eq_ignore_ascii_case(b"LMOVE") {
                            args.get(1)
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        } else {
                            args.first()
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        };
                        if let Some(key) = wake_key {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            }
                        }
                    }
                }

                results.push(frame);
            }
            drop(guard);
            let _ = reply_tx.send(results);
        }
        ShardMessage::ExecuteSlotted {
            db_index,
            command,
            response_slot,
        } => {
            let response = {
                let db_count = shard_databases.db_count();
                let db_idx = db_index.min(db_count.saturating_sub(1));
                let (cmd, args) = match extract_command_static(&command) {
                    Some(pair) => pair,
                    None => {
                        // SAFETY: response_slot points to a valid ResponseSlot owned by the
                        // connection's ResponseSlotPool, which outlives all dispatched messages.
                        let slot = unsafe { &*response_slot.0 };
                        slot.fill(vec![crate::protocol::Frame::Error(
                            bytes::Bytes::from_static(b"ERR invalid command format"),
                        )]);
                        return;
                    }
                };

                let is_write = metadata::is_write(cmd);
                if is_write {
                    let db_guard = shard_databases.read_db(shard_id, db_idx);
                    cow_intercept(snapshot_state, &db_guard, db_idx, &command);
                    drop(db_guard);
                }

                let mut guard = shard_databases.write_db(shard_id, db_idx);
                guard.refresh_now_from_cache(cached_clock);
                let mut selected = db_idx;
                let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
                let frame = match result {
                    DispatchResult::Response(f) => f,
                    DispatchResult::Quit(f) => f,
                };

                if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let serialized = aof::serialize_command(&command);
                    wal_append_and_fanout(
                        &serialized,
                        wal_writer,
                        wal_v3_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );
                }

                if !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD")
                        || cmd.eq_ignore_ascii_case(b"XADD");
                    if needs_wake {
                        let wake_key = if cmd.eq_ignore_ascii_case(b"LMOVE") {
                            args.get(1)
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        } else {
                            args.first()
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        };
                        if let Some(key) = wake_key {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            }
                        }
                    }
                }

                drop(guard);
                frame
            };
            // SAFETY: response_slot points to a valid ResponseSlot (see above).
            let slot = unsafe { &*response_slot.0 };
            slot.fill(vec![response]);
        }
        ShardMessage::MultiExecuteSlotted {
            db_index,
            commands,
            response_slot,
        } => {
            let mut results = Vec::with_capacity(commands.len());
            let db_count = shard_databases.db_count();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            let mut guard = shard_databases.write_db(shard_id, db_idx);
            guard.refresh_now_from_cache(cached_clock);
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

                let is_write = metadata::is_write(cmd);
                if is_write {
                    cow_intercept(snapshot_state, &guard, db_idx, cmd_frame);
                }

                let mut selected = db_idx;
                let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
                let frame = match result {
                    DispatchResult::Response(f) => f,
                    DispatchResult::Quit(f) => f,
                };

                if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let serialized = aof::serialize_command(cmd_frame);
                    wal_append_and_fanout(
                        &serialized,
                        wal_writer,
                        wal_v3_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );

                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD")
                        || cmd.eq_ignore_ascii_case(b"XADD");
                    if needs_wake {
                        let wake_key = if cmd.eq_ignore_ascii_case(b"LMOVE") {
                            args.get(1)
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        } else {
                            args.first()
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        };
                        if let Some(key) = wake_key {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            }
                        }
                    }
                }

                results.push(frame);
            }
            drop(guard);
            // SAFETY: response_slot points to a valid ResponseSlot (see ExecuteSlotted).
            let slot = unsafe { &*response_slot.0 };
            slot.fill(results);
        }
        ShardMessage::PipelineBatchSlotted {
            db_index,
            commands,
            response_slot,
        } => {
            let mut results = Vec::with_capacity(commands.len());
            let db_count = shard_databases.db_count();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            let mut guard = shard_databases.write_db(shard_id, db_idx);
            guard.refresh_now_from_cache(cached_clock);
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

                let is_write = metadata::is_write(cmd);
                if is_write {
                    cow_intercept(snapshot_state, &guard, db_idx, cmd_frame);
                }

                let mut selected = db_idx;
                let result = cmd_dispatch(&mut guard, cmd, args, &mut selected, db_count);
                let frame = match result {
                    DispatchResult::Response(f) => f,
                    DispatchResult::Quit(f) => f,
                };

                if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let serialized = aof::serialize_command(cmd_frame);
                    wal_append_and_fanout(
                        &serialized,
                        wal_writer,
                        wal_v3_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                    );
                }

                // Auto-index: if HSET succeeded, check for vector index match
                if cmd.eq_ignore_ascii_case(b"HSET")
                    && !matches!(frame, crate::protocol::Frame::Error(_))
                {
                    if let Some(crate::protocol::Frame::BulkString(key_bytes)) = args.first() {
                        // Use the `vector_store` parameter (already locked by caller),
                        // NOT shard_databases.vector_store() which would deadlock
                        // (parking_lot::Mutex is non-reentrant).
                        auto_index_hset(vector_store, key_bytes, args);
                    }
                }

                if !matches!(frame, crate::protocol::Frame::Error(_)) {
                    let needs_wake = cmd.eq_ignore_ascii_case(b"LPUSH")
                        || cmd.eq_ignore_ascii_case(b"RPUSH")
                        || cmd.eq_ignore_ascii_case(b"LMOVE")
                        || cmd.eq_ignore_ascii_case(b"ZADD")
                        || cmd.eq_ignore_ascii_case(b"XADD");
                    if needs_wake {
                        let wake_key = if cmd.eq_ignore_ascii_case(b"LMOVE") {
                            args.get(1)
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        } else {
                            args.first()
                                .and_then(|f| crate::server::connection::extract_bytes(f))
                        };
                        if let Some(key) = wake_key {
                            let mut reg = blocking_registry.borrow_mut();
                            if cmd.eq_ignore_ascii_case(b"LPUSH")
                                || cmd.eq_ignore_ascii_case(b"RPUSH")
                                || cmd.eq_ignore_ascii_case(b"LMOVE")
                            {
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            } else {
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg, &mut guard, db_idx, &key,
                                );
                            }
                        }
                    }
                }

                results.push(frame);
            }
            drop(guard);
            // SAFETY: response_slot points to a valid ResponseSlot (see ExecuteSlotted).
            let slot = unsafe { &*response_slot.0 };
            slot.fill(results);
        }
        ShardMessage::PubSubPublish {
            channel,
            message,
            slot,
        } => {
            let count = pubsub_registry.publish(&channel, &message);
            slot.add(count);
        }
        ShardMessage::PubSubPublishBatch { pairs, slot } => {
            let mut batch_total: i64 = 0;
            for (i, (channel, message)) in pairs.iter().enumerate() {
                let count = pubsub_registry.publish(channel, message);
                if i < slot.counts.len() {
                    slot.counts[i].store(count, std::sync::atomic::Ordering::Relaxed);
                }
                batch_total += count;
            }
            slot.add(batch_total);
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
            let mut guard = shard_databases.write_db(shard_id, db_index);
            if guard.exists(&key) {
                crate::blocking::wakeup::try_wake_list_waiter(&mut reg, &mut guard, db_index, &key);
                crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, &mut guard, db_index, &key);
                crate::blocking::wakeup::try_wake_stream_waiter(
                    &mut reg, &mut guard, db_index, &key,
                );
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
            let db_guard = shard_databases.read_db(shard_id, db_index);
            let keys = crate::cluster::migration::handle_get_keys_in_slot(
                std::slice::from_ref(&*db_guard),
                0,
                slot,
                count,
            );
            drop(db_guard);
            let _ = reply_tx.send(keys);
        }
        ShardMessage::SlotOwnershipUpdate {
            add_slots: _,
            remove_slots: _,
        } => {
            // Slot ownership is tracked in ClusterState, not per-shard.
        }
        ShardMessage::VectorSearch {
            index_name,
            query_blob,
            k,
            reply_tx,
        } => {
            let response = vector_search::search_local(vector_store, &index_name, &query_blob, k);
            let _ = reply_tx.send(response);
        }
        ShardMessage::VectorCommand { command, reply_tx } => {
            #[cfg(feature = "graph")]
            let graph_guard = shard_databases.graph_store_read(shard_id);

            // SESSION clause needs Database access for sorted set storage.
            let has_session = has_session_keyword(&command);
            let mut db_guard;
            let db_opt = if has_session {
                db_guard = shard_databases.write_db(shard_id, 0);
                Some(&mut *db_guard)
            } else {
                None
            };

            let mut text_guard = shard_databases.text_store(shard_id);
            let response = dispatch_vector_command(
                vector_store,
                &mut *text_guard,
                #[cfg(feature = "graph")]
                Some(&graph_guard),
                &command,
                db_opt,
            );
            let _ = reply_tx.send(response);
        }
        #[cfg(feature = "graph")]
        ShardMessage::GraphCommand { command, reply_tx } => {
            // GraphCommand is dispatched via connection handlers using ShardDatabases,
            // not through SPSC. If we receive one here, dispatch it locally.
            let (response, wal_records) = {
                let mut gs = shard_databases.graph_store_write(shard_id);
                let resp = crate::command::graph::dispatch_graph_command(&mut gs, &command);
                let records = gs.drain_wal();
                (resp, records)
            };
            for record in wal_records {
                shard_databases.wal_append(shard_id, bytes::Bytes::from(record));
            }
            let _ = reply_tx.send(response);
        }
        #[cfg(feature = "graph")]
        ShardMessage::GraphTraverse {
            graph_name,
            node_ids,
            remaining_hops: _,
            edge_type_filter,
            snapshot_lsn,
            reply_tx,
        } => {
            let response = crate::graph::cross_shard::handle_graph_traverse(
                &shard_databases.graph_store_read(shard_id),
                &graph_name,
                &node_ids,
                edge_type_filter,
                snapshot_lsn,
            );
            let _ = reply_tx.send(response);
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
        ShardMessage::MigrateConnection { .. } => {
            // MigrateConnection is collected by drain_spsc_shared into pending_migrations,
            // not dispatched through handle_shard_message_shared.
            // If we reach here, it's a logic error — log and drop.
            tracing::warn!(
                "Shard {}: MigrateConnection reached handle_shard_message_shared unexpectedly",
                shard_id
            );
        }
        ShardMessage::NewConnection(_) => {
            // NewConnection is handled via conn_rx, not SPSC
        }
    }
}

/// Dispatch FT.* commands to the appropriate vector_search handler.
///
/// Public within crate so coordinator can call it directly for local-shard execution
/// (avoiding SPSC self-send).
///
/// When `graph_store` is `Some`, FT.SEARCH will check for `EXPAND GRAPH` clause
/// and perform graph-expanded search if requested (GraphRAG).
pub(crate) fn dispatch_vector_command(
    vector_store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    #[cfg(feature = "graph")] graph_store: Option<&crate::graph::store::GraphStore>,
    command: &crate::protocol::Frame,
    db: Option<&mut crate::storage::db::Database>,
) -> crate::protocol::Frame {
    let (cmd, args) = match extract_command_static(command) {
        Some(pair) => pair,
        None => {
            return crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR invalid command format",
            ));
        }
    };

    if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
        vector_search::ft_create(vector_store, text_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
        #[cfg(feature = "graph")]
        {
            vector_search::ft_search_with_graph(vector_store, graph_store, args, db)
        }
        #[cfg(not(feature = "graph"))]
        {
            vector_search::ft_search(vector_store, args, db)
        }
    } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
        vector_search::ft_dropindex(vector_store, text_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
        vector_search::ft_info(vector_store, text_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
        vector_search::ft_list(vector_store)
    } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
        vector_search::ft_compact(vector_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
        vector_search::ft_config(vector_store, text_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
        vector_search::cache_search::ft_cachesearch(vector_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT.EXPAND") {
        #[cfg(feature = "graph")]
        {
            match graph_store {
                Some(gs) => vector_search::ft_expand(gs, args),
                None => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                    b"ERR graph feature not available",
                )),
            }
        }
        #[cfg(not(feature = "graph"))]
        {
            crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR FT.EXPAND requires graph feature",
            ))
        }
    } else if cmd.eq_ignore_ascii_case(b"FT.NAVIGATE") {
        #[cfg(feature = "graph")]
        {
            vector_search::navigate::ft_navigate(vector_store, graph_store, args, db)
        }
        #[cfg(not(feature = "graph"))]
        {
            crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR FT.NAVIGATE requires graph feature",
            ))
        }
    } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
        vector_search::recommend::ft_recommend(vector_store, args, db)
    } else {
        crate::protocol::Frame::Error(bytes::Bytes::from_static(b"ERR unknown FT command"))
    }
}

/// After a successful HSET, check if the key matches any vector index prefix.
/// If so, extract the vector field value, SQ-quantize, and append to mutable segment.
///
/// Check if a Frame (array command) contains the SESSION keyword.
/// Used to determine whether we need Database access for sorted set storage.
fn has_session_keyword(frame: &crate::protocol::Frame) -> bool {
    if let crate::protocol::Frame::Array(items) = frame {
        for item in items {
            if let crate::protocol::Frame::BulkString(b) = item {
                if b.eq_ignore_ascii_case(b"SESSION") {
                    return true;
                }
            }
        }
    }
    false
}

/// NOTE: Vec allocations here are acceptable because auto-indexing only fires when
/// a key matches an index prefix (rare per-operation), and f32 decode + SQ encode
/// is inherently O(dim) work. This is post-dispatch processing, not hot-path.
/// Public wrapper for auto-indexing on HSET — called from single-shard handler.
pub fn auto_index_hset_public(
    vector_store: &mut VectorStore,
    key: &[u8],
    args: &[crate::protocol::Frame],
) {
    auto_index_hset(vector_store, key, args);
}

fn auto_index_hset(vector_store: &mut VectorStore, key: &[u8], args: &[crate::protocol::Frame]) {
    let matching_names = vector_store.find_matching_index_names(key);
    if matching_names.is_empty() {
        return;
    }

    for idx_name in matching_names {
        let idx = match vector_store.get_index_mut(&idx_name) {
            Some(i) => i,
            None => continue,
        };
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);

        // Iterate ALL vector fields defined in the index.
        // For single-field indexes, this is exactly one iteration (backward compatible).
        let field_count = idx.meta.vector_fields.len();
        let mut any_vector_inserted = false;

        for field_idx in 0..field_count {
            let field_name = idx.meta.vector_fields[field_idx].field_name.clone();
            let dim = idx.meta.vector_fields[field_idx].dimension as usize;

            let has_vector = find_vector_blob(args, &field_name, dim).is_some();
            if !has_vector {
                continue;
            }

            if field_idx == 0 {
                // Default field: use existing top-level segments
                handle_vector_insert(idx, key, args, &field_name, dim, key_hash);
            } else {
                // Additional field: use field_segments
                handle_vector_insert_field(idx, &field_name, key, args, dim, key_hash);
            }
            any_vector_inserted = true;
        }

        // Metadata-only path: if no vector was inserted but key already exists
        if !any_vector_inserted {
            if let Some(&global_id) = idx.key_hash_to_global_id.get(&key_hash) {
                let source_field = idx.meta.source_field.clone();
                update_metadata_only(idx, args, &source_field, global_id);
            }
        }
    }
}

/// Find the vector blob in HSET args for the given source_field.
/// Returns Some(blob) if found with correct dimension, None otherwise.
fn find_vector_blob<'a>(
    args: &'a [crate::protocol::Frame],
    source_field: &[u8],
    dim: usize,
) -> Option<&'a bytes::Bytes> {
    let mut i = 1;
    while i + 1 < args.len() {
        if let crate::protocol::Frame::BulkString(field) = &args[i] {
            if field.eq_ignore_ascii_case(source_field) {
                if let crate::protocol::Frame::BulkString(blob) = &args[i + 1] {
                    if blob.len() == dim * 4 {
                        return Some(blob);
                    }
                }
                return None;
            }
        }
        i += 2;
    }
    None
}

/// Vector-present path: decode vector, SQ-quantize, append to mutable segment,
/// populate payload index for all HASH fields.
fn handle_vector_insert(
    idx: &mut crate::vector::store::VectorIndex,
    key: &[u8],
    args: &[crate::protocol::Frame],
    source_field: &bytes::Bytes,
    dim: usize,
    key_hash: u64,
) {
    let blob = match find_vector_blob(args, source_field, dim) {
        Some(b) => b.clone(),
        None => return,
    };

    // Decode f32 from blob
    let mut f32_vec = Vec::with_capacity(dim);
    for chunk in blob.chunks_exact(4) {
        f32_vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    // SQ quantize
    let mut sq_vec = vec![0i8; dim];
    vector_search::quantize_f32_to_sq(&f32_vec, &mut sq_vec);
    // Compute norm
    let norm: f32 = f32_vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    // Record original Redis key for FT.SEARCH response.
    idx.key_hash_to_key
        .entry(key_hash)
        .or_insert_with(|| bytes::Bytes::copy_from_slice(key));
    // Append to mutable segment
    let snap = idx.segments.load();
    let internal_id = snap.mutable.append(key_hash, &f32_vec, &sq_vec, norm, 0);
    // Use global_id for payload index so filter bitmaps match
    // search results after compaction advances global_id_base.
    let global_id = snap.mutable.global_id_base() + internal_id;
    crate::vector::metrics::add_vectors(1);

    // Record key_hash → global_id mapping for future metadata-only updates
    idx.key_hash_to_global_id.insert(key_hash, global_id);

    // Populate payload index with all HASH fields (for filtered search)
    let mut j = 1;
    while j + 1 < args.len() {
        if let (
            crate::protocol::Frame::BulkString(f_name),
            crate::protocol::Frame::BulkString(f_val),
        ) = (&args[j], &args[j + 1])
        {
            if !f_name.eq_ignore_ascii_case(source_field) {
                index_payload_field(&mut idx.payload_index, f_name, f_val, global_id);
            }
        }
        j += 2;
    }
}

/// Vector-present path for ADDITIONAL (non-default) fields.
/// Mirrors `handle_vector_insert` but targets `idx.field_segments[field_name]`.
/// Does NOT populate payload_index (payload is shared, handled by default field insert
/// or by the metadata-only path).
fn handle_vector_insert_field(
    idx: &mut crate::vector::store::VectorIndex,
    field_name: &bytes::Bytes,
    key: &[u8],
    args: &[crate::protocol::Frame],
    dim: usize,
    key_hash: u64,
) {
    let blob = match find_vector_blob(args, field_name, dim) {
        Some(b) => b.clone(),
        None => return,
    };

    // Decode f32 from blob
    let mut f32_vec = Vec::with_capacity(dim);
    for chunk in blob.chunks_exact(4) {
        f32_vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    // SQ quantize
    let mut sq_vec = vec![0i8; dim];
    vector_search::quantize_f32_to_sq(&f32_vec, &mut sq_vec);
    // Compute norm
    let norm: f32 = f32_vec.iter().map(|x| x * x).sum::<f32>().sqrt();

    // Record original Redis key (shared across all fields)
    idx.key_hash_to_key
        .entry(key_hash)
        .or_insert_with(|| bytes::Bytes::copy_from_slice(key));

    // Look up the additional field's SegmentHolder
    let fs = match idx.field_segments.get(field_name.as_ref()) {
        Some(fs) => fs,
        None => return, // field not found (should not happen with valid schema)
    };
    let snap = fs.segments.load();
    let _internal_id = snap.mutable.append(key_hash, &f32_vec, &sq_vec, norm, 0);
    crate::vector::metrics::add_vectors(1);
    // Note: global_id and payload_index are NOT updated here.
    // Payload is shared and managed by the default field's insert path.
}

/// Metadata-only path: update payload index for an existing vector.
///
/// For each field in the HSET args (skipping the vector source field), removes
/// the old index entries for that specific field and re-inserts the new value.
/// This is per-field remove+reinsert, NOT a blanket remove of all fields.
fn update_metadata_only(
    idx: &mut crate::vector::store::VectorIndex,
    args: &[crate::protocol::Frame],
    source_field: &bytes::Bytes,
    global_id: u32,
) {
    let mut j = 1;
    while j + 1 < args.len() {
        if let (
            crate::protocol::Frame::BulkString(f_name),
            crate::protocol::Frame::BulkString(f_val),
        ) = (&args[j], &args[j + 1])
        {
            if !f_name.eq_ignore_ascii_case(source_field) {
                // Remove old entries for this field only, then re-insert
                idx.payload_index.remove_field(f_name, global_id);
                index_payload_field(&mut idx.payload_index, f_name, f_val, global_id);
            }
        }
        j += 2;
    }
}

/// Classify and insert a payload field into the PayloadIndex.
///
/// Shared by both vector-present and metadata-only paths. Detects geo
/// coordinates ("lon,lat"), numeric values, and tag values (including booleans).
fn index_payload_field(
    payload_index: &mut crate::vector::filter::PayloadIndex,
    field: &bytes::Bytes,
    value: &bytes::Bytes,
    global_id: u32,
) {
    if let Ok(val_str) = std::str::from_utf8(value) {
        // Geo detection: "lon,lat" pattern (two floats separated by comma)
        if let Some((lon, lat)) = parse_geo_value(val_str) {
            payload_index.insert_geo(field, lat, lon, global_id);
            // Also store raw value as tag for display
            payload_index.insert_tag(field, value, global_id);
        } else if let Ok(num) = val_str.parse::<f64>() {
            // Numeric value
            payload_index.insert_numeric(field, num, global_id);
        } else {
            // Tag value (includes "true"/"false" for BoolEq)
            payload_index.insert_tag(field, value, global_id);
        }
    } else {
        // Non-UTF8 binary: store as tag
        payload_index.insert_tag(field, value, global_id);
    }
    // Also index into full-text TextIndex (if text-index feature enabled).
    // All payload string fields are indexed; only fields queried via TextMatch
    // will actually be searched at query time.
    payload_index.insert_text(field, value, global_id);
}

/// Parse a "lon,lat" geo value string. Returns `Some((lon, lat))` if the value
/// contains exactly one comma and both parts parse as valid f64 coordinates.
fn parse_geo_value(s: &str) -> Option<(f64, f64)> {
    let comma_pos = s.find(',')?;
    // Ensure exactly one comma
    if s[comma_pos + 1..].contains(',') {
        return None;
    }
    let lon: f64 = s[..comma_pos].trim().parse().ok()?;
    let lat: f64 = s[comma_pos + 1..].trim().parse().ok()?;
    // Basic coordinate validation
    if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
        return None;
    }
    Some((lon, lat))
}

/// COW intercept: capture old value for a key being written if its segment is pending.
///
/// Called before cmd_dispatch to preserve snapshot consistency. Only clones the old entry
/// if the key's segment is actually pending serialization (fast bool check in hot path).
pub(crate) fn cow_intercept(
    snapshot: &mut Option<SnapshotState>,
    db: &Database,
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
    let seg_idx = db.data().segment_index_for_hash(hash);
    if snap.is_segment_pending(db_index, seg_idx) {
        if let Some(old_entry) = db.data().get(key) {
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
    wal_v3_writer: &mut Option<WalWriterV3>,
    repl_backlog: &mut Option<ReplicationBacklog>,
    replica_txs: &[(u64, channel::MpscSender<bytes::Bytes>)],
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
) {
    // 1a. WAL v2 append (disk durability, legacy path)
    if let Some(w) = wal_writer {
        w.append(data);
    }
    // 1b. WAL v3 append (disk-offload mode: per-record LSN, CRC32C)
    if let Some(w3) = wal_v3_writer {
        w3.append(
            crate::persistence::wal_v3::record::WalRecordType::Command,
            data,
        );
    }
    // 2. Replication backlog (in-memory circular buffer for partial resync)
    if let Some(backlog) = repl_backlog {
        backlog.append(data);
    }
    // 3. Advance monotonic replication offset (NEVER resets on WAL truncation)
    if let Some(rs) = repl_state {
        match rs.read() {
            Ok(rs) => rs.increment_shard_offset(shard_id, data.len() as u64),
            Err(_) => tracing::error!("repl_state lock poisoned, replication offset not updated"),
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
