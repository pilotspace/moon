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
    repl_backlog: &crate::replication::backlog::SharedBacklog,
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
    pending_cdc_subscribes: &mut Vec<crate::shard::dispatch::CdcSubscribePayload>,
    // P8: optional manifest for VACUUM manifest/WAL passes; None when no persistence_dir.
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    // P8: MVCC committed-prune margin from server config (default 1000).
    mvcc_prune_margin: u64,
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
                        | ShardMessage::VectorSearch(_)
                        | ShardMessage::VectorCommand { .. }
                        | ShardMessage::DocFreq { .. }
                        | ShardMessage::TextSearch(_) => {
                            execute_batch.push(msg);
                        }
                        #[cfg(feature = "text-index")]
                        ShardMessage::TextAggregate(_) => {
                            execute_batch.push(msg);
                        }
                        #[cfg(feature = "text-index")]
                        ShardMessage::FtHybrid(_) => {
                            execute_batch.push(msg);
                        }
                        #[cfg(feature = "text-index")]
                        ShardMessage::InvertedSearch(_) => {
                            execute_batch.push(msg);
                        }
                        #[cfg(feature = "graph")]
                        ShardMessage::GraphCommand { .. } => {
                            execute_batch.push(msg);
                        }
                        #[cfg(feature = "graph")]
                        ShardMessage::GraphTraverse(_) => {
                            execute_batch.push(msg);
                        }
                        ShardMessage::MigrateConnection(payload) => {
                            pending_migrations.push((payload.fd, payload.state));
                        }
                        ShardMessage::CdcSubscribe(payload) => {
                            // C3b-2 — captured here and handed to event_loop,
                            // which owns the CdcSubscriberRegistry. We don't
                            // touch the registry through handle_shard_message
                            // because the registry's WalTailReader needs the
                            // shard's wal_dir, which the event loop already
                            // has from wal_v3_writer.wal_dir().
                            pending_cdc_subscribes.push(*payload);
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
                shard_manifest,
                mvcc_prune_margin,
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
            shard_manifest,
            mvcc_prune_margin,
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
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<(u64, channel::MpscSender<bytes::Bytes>)>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
    script_cache: &Rc<RefCell<crate::scripting::ScriptCache>>,
    cached_clock: &CachedClock,
    vector_store: &mut VectorStore,
    // P8: optional manifest for VACUUM manifest/WAL passes; None when no persistence_dir.
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    // P8: MVCC committed-prune margin from server config (default 1000).
    mvcc_prune_margin: u64,
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
                    // FT.AGGREGATE materialises rows from the hash store (Phase 152,
                    // Plan 02 — reads @field values per doc).
                    // FT.DROPINDEX with DD flag needs Database to delete indexed docs.
                    let needs_db = cmd.eq_ignore_ascii_case(b"FT.RECOMMEND")
                        || cmd.eq_ignore_ascii_case(b"FT.AGGREGATE")
                        || cmd.eq_ignore_ascii_case(b"FT.DROPINDEX")
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

                // MA2: KILL SNAPSHOT <txn_id> — forcibly kill an active MVCC snapshot.
                // Routes directly to VectorStore's TransactionManager; bypasses
                // write-stall guards (it is an admin command, never a data write).
                if cmd.eq_ignore_ascii_case(b"KILL") {
                    let frame = crate::command::server_admin::kill_snapshot(vector_store, args);
                    let _ = reply_tx.send(frame);
                    return;
                }

                // P8: VACUUM — manual reclamation across manifest, MVCC, and WAL.
                // Bypasses write-stall guards (reclaims, does not write data).
                if cmd.eq_ignore_ascii_case(b"VACUUM") {
                    let frame = crate::command::server_admin::vacuum(
                        vector_store,
                        shard_manifest.as_mut(),
                        wal_v3_writer.as_mut(),
                        args,
                        mvcc_prune_margin,
                    );
                    let _ = reply_tx.send(frame);
                    return;
                }

                // P8: DEBUG RECLAMATION — verbose per-subsystem diagnostic dump.
                // Intercept here so it has access to manifest and WAL (read-only).
                if cmd.eq_ignore_ascii_case(b"DEBUG") {
                    if let Some(sub) = args.first() {
                        if let Some(s) = crate::command::helpers::extract_bytes(sub) {
                            if s.eq_ignore_ascii_case(b"RECLAMATION") {
                                let frame = crate::command::server_admin::debug_reclamation(
                                    vector_store,
                                    shard_manifest.as_ref(),
                                    wal_v3_writer.as_ref(),
                                );
                                let _ = reply_tx.send(frame);
                                return;
                            }
                        }
                    }
                    // All other DEBUG subcommands fall through to cmd_dispatch.
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
                        let mut ts = shard_databases.text_store(shard_id);
                        // Plan 166-01: return value (index_name, key_hash)
                        // tuples will be consumed by Plan 166-02 to record
                        // VectorIntents on the active CrossStoreTxn. Discarded
                        // here because this path is not txn-aware yet.
                        let _ = auto_index_hset(vector_store, &mut *ts, key_bytes, args, 0);
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
                        let mut ts = shard_databases.text_store(shard_id);
                        // Plan 166-01: Vec<(idx, key_hash)> return discarded
                        // here; Plan 166-02 threads it into CrossStoreTxn.
                        let _ = auto_index_hset(vector_store, &mut *ts, key_bytes, args, 0);
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
                        let mut ts = shard_databases.text_store(shard_id);
                        // Plan 166-01: Vec<(idx, key_hash)> return discarded
                        // here; Plan 166-02 threads it into CrossStoreTxn.
                        let _ = auto_index_hset(vector_store, &mut *ts, key_bytes, args, 0);
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
        ShardMessage::PubSubPublish(payload) => {
            let count = pubsub_registry.publish(&payload.channel, &payload.message);
            payload.slot.add(count);
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
        ShardMessage::BlockRegister(payload) => {
            let crate::shard::dispatch::BlockRegisterPayload {
                db_index,
                key,
                wait_id,
                cmd,
                reply_tx,
            } = *payload;
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
        ShardMessage::VectorSearch(payload) => {
            let crate::shard::dispatch::VectorSearchPayload {
                index_name,
                query_blob,
                k,
                as_of_lsn,
                reply_tx,
            } = *payload;
            // Phase 171 SCAT-01: honor coordinator-resolved AS_OF / TXN LSN
            // for multi-shard FT.SEARCH. When `as_of_lsn == 0` the filter is a
            // no-op and behavior matches the pre-171 path. Route through
            // `search_local_filtered` with AS_OF threaded in to apply MVCC
            // filtering against the committed treemap inside `search_local_raw`.
            let response = vector_search::search_local_filtered(
                vector_store,
                &index_name,
                &query_blob,
                k,
                None,
                0,
                usize::MAX,
                None,
                as_of_lsn,
            );
            let _ = reply_tx.send(response);
        }
        ShardMessage::DocFreq {
            index_name,
            field_queries,
            reply_tx,
        } => {
            // DFS Phase 1: collect per-term df + total N from this shard's TextIndex.
            // Returns crate::protocol::Frame::Array with interleaved [term, df, ..., "N", n] per field_query.
            let text_guard = shard_databases.text_store(shard_id);
            let response = match text_guard.get_index(&index_name) {
                Some(text_index) => {
                    let mut items: Vec<crate::protocol::Frame> = Vec::new();
                    for (field_idx_opt, terms) in &field_queries {
                        let fidx = field_idx_opt.unwrap_or(0);
                        let (term_dfs, n) = text_index.doc_freq_for_terms(fidx, terms);
                        for (term, df) in term_dfs {
                            items
                                .push(crate::protocol::Frame::BulkString(bytes::Bytes::from(term)));
                            items.push(crate::protocol::Frame::Integer(i64::from(df)));
                        }
                        items.push(crate::protocol::Frame::BulkString(
                            bytes::Bytes::from_static(b"N"),
                        ));
                        items.push(crate::protocol::Frame::Integer(i64::from(n)));
                    }
                    crate::protocol::Frame::Array(items.into())
                }
                None => {
                    crate::protocol::Frame::Error(bytes::Bytes::from_static(b"ERR unknown index"))
                }
            };
            drop(text_guard);
            let _ = reply_tx.send(response);
        }
        ShardMessage::TextSearch(payload) => {
            let crate::shard::dispatch::TextSearchPayload {
                index_name,
                field_idx,
                query_terms,
                global_df,
                global_n,
                top_k,
                offset,
                count,
                highlight_opts,
                summarize_opts,
                reply_tx,
            } = *payload;
            // DFS Phase 2: execute BM25 text search with global IDF injected by coordinator.
            // After scoring, apply HIGHLIGHT/SUMMARIZE post-processing if requested.
            // Each shard applies post-processing to its own results using its local hash store
            // (direct access — no cross-shard reads needed, no .await — safe to hold guards).
            //
            // query_terms is Vec<QueryTerm> — fuzzy/prefix terms use the OR-union expansion path.
            // Extract plain strings for HIGHLIGHT/SUMMARIZE (needs analyzed term text only).
            let term_strings: Vec<String> = query_terms.iter().map(|qt| qt.text.clone()).collect();
            let response = {
                let text_guard = shard_databases.text_store(shard_id);
                match text_guard.get_index(&index_name) {
                    Some(text_index) => {
                        let mut result =
                            crate::command::vector_search::ft_text_search::execute_text_search_with_global_idf(
                                text_index,
                                field_idx,
                                &query_terms,
                                &global_df,
                                global_n,
                                top_k,
                                offset,
                                count,
                            );
                        // Apply HIGHLIGHT/SUMMARIZE post-processing in-place.
                        // Safe to hold both text_guard and db_guard here — synchronous context,
                        // no .await points. Guards drop at end of this block.
                        if highlight_opts.is_some() || summarize_opts.is_some() {
                            let db_guard = shard_databases.read_db(shard_id, 0);
                            crate::command::vector_search::ft_text_search::apply_post_processing(
                                &mut result,
                                &term_strings,
                                text_index,
                                &*db_guard,
                                highlight_opts.as_ref(),
                                summarize_opts.as_ref(),
                            );
                            // db_guard drops here.
                        }
                        result
                    }
                    None => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                        b"ERR unknown index",
                    )),
                }
                // text_guard drops here (end of block).
            };
            let _ = reply_tx.send(response);
        }
        ShardMessage::VectorCommand { command, reply_tx } => {
            #[cfg(feature = "graph")]
            let graph_guard = shard_databases.graph_store_read(shard_id);

            // SESSION clause needs Database access for sorted set storage.
            // FT.DROPINDEX with DD flag needs Database to delete indexed docs.
            let cmd_bytes = extract_command_static(&command).map(|(c, _)| c);
            let is_dropindex = cmd_bytes
                .map(|c| c.eq_ignore_ascii_case(b"FT.DROPINDEX"))
                .unwrap_or(false);
            let has_session = has_session_keyword(&command);
            let mut db_guard;
            let db_opt = if has_session || is_dropindex {
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
        ShardMessage::GraphTraverse(payload) => {
            let crate::shard::dispatch::GraphTraversePayload {
                graph_name,
                node_ids,
                remaining_hops: _,
                edge_type_filter,
                snapshot_lsn,
                reply_tx,
            } = *payload;
            let response = crate::graph::cross_shard::handle_graph_traverse(
                &shard_databases.graph_store_read(shard_id),
                &graph_name,
                &node_ids,
                edge_type_filter,
                snapshot_lsn,
            );
            let _ = reply_tx.send(response);
        }
        #[cfg(feature = "text-index")]
        ShardMessage::InvertedSearch(payload) => {
            // Phase 152 Plan 06 (B-02): remote shard executes a FieldFilter
            // (TAG — Plan 07 adds NumericRange) and returns the same response
            // frame shape `build_text_response` emits. No BM25 globals, no
            // analyzer. Guards dropped before reply.
            let crate::shard::dispatch::InvertedSearchPayload {
                index_name,
                filter,
                top_k,
                offset,
                count,
                reply_tx,
            } = *payload;
            let response = {
                let text_guard = shard_databases.text_store(shard_id);
                match text_guard.get_index(&index_name) {
                    Some(text_index) => {
                        let clause =
                            crate::command::vector_search::ft_text_search::TextQueryClause {
                                field_name: None,
                                terms: Vec::new(),
                                filter: Some(filter),
                            };
                        let results =
                            crate::command::vector_search::ft_text_search::execute_query_on_index(
                                text_index, &clause, None, None, top_k,
                            );
                        // Same response shape TextSearch emits — matches what
                        // the coordinator's merge_text_results consumes.
                        crate::command::vector_search::ft_text_search::build_text_response(
                            &results, offset, count,
                        )
                    }
                    None => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                        b"ERR no such index",
                    )),
                }
                // text_guard dropped here
            };
            let _ = reply_tx.send(response);
        }
        #[cfg(feature = "text-index")]
        ShardMessage::TextAggregate(payload) => {
            // FT.AGGREGATE PHASE 1 (Plan 03 D-05/D-07): run pipeline UP
            // TO post-GROUPBY on this shard; ship encoded ShardPartial.
            // The boxed payload is destructured into locals so guards can
            // be dropped before `reply_tx.send()` — mirrors DocFreq /
            // TextSearch arms above.
            let crate::shard::dispatch::TextAggregatePayload {
                index_name,
                query,
                pipeline,
                reply_tx,
            } = *payload;
            let response = {
                let text_guard = shard_databases.text_store(shard_id);
                let db_guard = shard_databases.read_db(shard_id, 0);
                crate::command::vector_search::ft_aggregate::execute_local_partial(
                    &text_guard,
                    &index_name,
                    &query,
                    &pipeline,
                    &db_guard,
                )
                // guards dropped here at end of block
            };
            let _ = reply_tx.send(response);
        }
        #[cfg(feature = "text-index")]
        ShardMessage::FtHybrid(payload) => {
            // Phase 152 Plan 05 (D-13): each shard computes BM25 (with
            // coordinator-provided global IDF), dense KNN, and optional
            // sparse, then returns three raw per-stream lists UNFUSED.
            // The coordinator calls `rrf_fuse_three` exactly once on the
            // unions. Guards are dropped before reply.
            let crate::shard::dispatch::FtHybridPayload {
                index_name,
                query_terms,
                dense_field,
                dense_blob,
                sparse_field,
                sparse_blob,
                weights,
                k_per_stream,
                top_k,
                global_df,
                global_n,
                as_of_lsn,
                reply_tx,
            } = *payload;
            let response = {
                let text_guard = shard_databases.text_store(shard_id);
                let sparse_pair = match (sparse_field.as_ref(), sparse_blob.as_ref()) {
                    (Some(f), Some(b)) => Some((f, b)),
                    _ => None,
                };
                // Phase 171 HYB-02 / SCAT-02: forward the coordinator-resolved
                // AS_OF LSN into the raw-streams executor so the dense branch
                // applies MVCC filtering consistently across shards. BM25
                // AS_OF coherent post-G-1 (v0.1.10); text-index MVCC upsert-chain
                // pending Phase 178 (MVCC-01).
                crate::command::vector_search::hybrid_multi::execute_hybrid_search_local_raw_streams(
                    vector_store,
                    &text_guard,
                    &index_name,
                    &query_terms,
                    &dense_field,
                    &dense_blob,
                    sparse_pair,
                    weights,
                    k_per_stream,
                    top_k,
                    &global_df,
                    global_n,
                    as_of_lsn,
                )
                // text_guard drops here (end of block)
            };
            let _ = reply_tx.send(response);
        }
        ShardMessage::Shutdown => {
            info!("Received shutdown via SPSC");
        }
        ShardMessage::RegisterReplica { replica_id, tx } => {
            // Lazy-init replication backlog on first replica registration (saves 1MB/shard).
            // The backlog is shared with PSYNC handlers via Arc<Mutex<Option<...>>> on
            // ReplicationState — see ReplicationState::ensure_backlogs_allocated for the
            // earlier allocation point triggered by REPLCONF.
            let mut guard = repl_backlog.lock();
            if guard.is_none() {
                *guard = Some(ReplicationBacklog::new(1024 * 1024));
            }
            drop(guard);
            replica_txs.push((replica_id, tx));
        }
        ShardMessage::UnregisterReplica { replica_id } => {
            replica_txs.retain(|(id, _)| *id != replica_id);
        }
        ShardMessage::MigrateConnection(_) => {
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
        ShardMessage::CdcSubscribe(_) => {
            // CdcSubscribe is collected by drain_spsc_shared into
            // pending_cdc_subscribes, not dispatched through
            // handle_shard_message_shared. Reaching here is a logic error.
            tracing::warn!(
                "Shard {}: CdcSubscribe reached handle_shard_message_shared unexpectedly",
                shard_id
            );
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
        // Check if this is a text query (no KNN/SPARSE markers) before
        // dispatching to the vector search path. Text queries are handled
        // by ft_text_search which reads from the TextStore (BM25 posting index).
        let query_bytes = args.get(1).and_then(|f| match f {
            crate::protocol::Frame::BulkString(b) => Some(b.as_ref()),
            _ => None,
        });
        if query_bytes.map_or(false, vector_search::is_text_query) {
            return vector_search::ft_text_search(text_store, args);
        }
        // Existing vector search path (KNN / SPARSE / hybrid).
        #[cfg(feature = "graph")]
        {
            vector_search::ft_search_with_graph(
                vector_store,
                graph_store,
                args,
                db,
                Some(text_store),
                0,
            )
        }
        #[cfg(not(feature = "graph"))]
        {
            vector_search::ft_search(vector_store, args, db, Some(text_store), 0)
        }
    } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
        vector_search::ft_dropindex(vector_store, text_store, db, args)
    } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
        vector_search::ft_info(vector_store, text_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
        vector_search::ft_list(vector_store)
    } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
        vector_search::ft_compact(vector_store, text_store, args)
    } else if cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
        // FT.AGGREGATE (Phase 152, Plan 02) — linear else-if branch per W8.
        // D-19's phf reference is superseded by the established FT.* dispatch
        // pattern in RESEARCH §ARM. FT.AGGREGATE needs Database access to
        // materialise rows from the hash store (see `needs_db` gate above).
        #[cfg(feature = "text-index")]
        {
            match db.as_deref() {
                Some(db_ref) => vector_search::ft_aggregate(vector_store, text_store, args, db_ref),
                None => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                    b"ERR FT.AGGREGATE requires Database access",
                )),
            }
        }
        #[cfg(not(feature = "text-index"))]
        {
            crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR FT.AGGREGATE requires text-index feature",
            ))
        }
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
///
/// Returns the `(index_name, key_hash)` pairs for vector indexes where a
/// vector value was actually appended to the mutable segment on this call.
/// Caller must record these as `VectorIntent`s on the active CrossStoreTxn
/// (if any) so TXN.ABORT can tombstone the entries via
/// `MutableSegment::mark_deleted_by_key_hash`. Metadata-only updates and
/// text-only indexes are NOT included — there is nothing to roll back for
/// those paths. SmallVec inline cap 4 keeps the common case (single index,
/// single vector field) heap-free.
pub fn auto_index_hset_public(
    vector_store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    key: &[u8],
    args: &[crate::protocol::Frame],
) -> smallvec::SmallVec<[(bytes::Bytes, u64); 4]> {
    auto_index_hset(vector_store, text_store, key, args, 0)
}

/// TXN-aware variant: tags each inserted vector entry with `txn_id` so
/// non-transactional readers (snapshot_lsn == 0) see it as uncommitted and
/// exclude it until TXN.COMMIT calls `txn_manager.commit(txn_id)`.
pub fn auto_index_hset_public_txn(
    vector_store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    key: &[u8],
    args: &[crate::protocol::Frame],
    txn_id: u64,
) -> smallvec::SmallVec<[(bytes::Bytes, u64); 4]> {
    auto_index_hset(vector_store, text_store, key, args, txn_id)
}

fn auto_index_hset(
    vector_store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    key: &[u8],
    args: &[crate::protocol::Frame],
    txn_id: u64,
) -> smallvec::SmallVec<[(bytes::Bytes, u64); 4]> {
    let mut inserted: smallvec::SmallVec<[(bytes::Bytes, u64); 4]> = smallvec::SmallVec::new();
    let matching_names = vector_store.find_matching_index_names(key);
    let text_matching = text_store.find_matching_index_names(key);
    if matching_names.is_empty() && text_matching.is_empty() {
        return inserted;
    }

    // Allocate ONE monotonic insert_lsn per HSET so the MVCC visibility rule
    // at src/vector/mvcc/visibility.rs filters these inserts out of snapshots
    // captured before this call (required for FT.SEARCH AS_OF and TXN snapshot
    // isolation — see Plan 165-03 TEMP-04/ACID-09). v0.1.10 G-1: the same LSN
    // is forwarded into the text-index path below so `FT.SEARCH HYBRID AS_OF`
    // honours snapshot isolation across both dense AND BM25 streams.
    //
    // Allocation is skipped only when neither a vector nor a text index
    // matches the HSET key — saves a counter bump on unrelated HSETs.
    // Borrow must complete before `get_index_mut` reborrows vector_store.
    let insert_lsn = if matching_names.is_empty() && text_matching.is_empty() {
        0
    } else {
        vector_store.txn_manager_mut().allocate_lsn()
    };

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
                handle_vector_insert(
                    idx,
                    key,
                    args,
                    &field_name,
                    dim,
                    key_hash,
                    insert_lsn,
                    txn_id,
                );
            } else {
                // Additional field: use field_segments
                handle_vector_insert_field(
                    idx,
                    &field_name,
                    key,
                    args,
                    dim,
                    key_hash,
                    insert_lsn,
                    txn_id,
                );
            }
            any_vector_inserted = true;
        }

        // Record ONE `(index_name, key_hash)` per index per HSET call — not
        // per vector field — so multi-vector-field indexes don't produce
        // duplicate intents. Plan 166-02/03 consumes this: the handler
        // pushes a `VectorIntent` for each entry here onto the active
        // CrossStoreTxn so TXN.ABORT can tombstone via
        // `mark_deleted_by_key_hash(key_hash, rollback_lsn)`.
        if any_vector_inserted {
            inserted.push((idx_name.clone(), key_hash));
        }

        // Metadata-only path: if no vector was inserted but key already exists
        if !any_vector_inserted {
            if let Some(&global_id) = idx.key_hash_to_global_id.get(&key_hash) {
                let source_field = idx.meta.source_field.clone();
                update_metadata_only(idx, args, &source_field, global_id);
            }
        }
    }

    // TEXT field indexing: use pre-computed text_matching from guard.
    // args[0] is the Redis key; field-value pairs start at args[1..].
    //
    // v0.1.10 G-1: thread `insert_lsn` through so every text doc records the
    // same monotonic LSN as its paired vector entry. `FT.SEARCH HYBRID AS_OF`
    // uses this to exclude post-snapshot BM25 hits (closing the HYB-03
    // deferral). Pre-MVCC callers (tests, non-HSET paths) leave `insert_lsn`
    // at 0 and the visibility filter treats such docs as always-visible.
    let text_args = if args.is_empty() { args } else { &args[1..] };
    for idx_name in text_matching {
        if let Some(idx) = text_store.get_index_mut(&idx_name) {
            let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
            let doc_id = idx.index_document_with_lsn(key_hash, key, text_args, insert_lsn);
            let _ = doc_id;
            // TAG auto-indexing (Plan 152-06): safe no-op on indexes with no
            // TAG fields (tag_index_document returns early on empty tag_fields).
            #[cfg(feature = "text-index")]
            idx.tag_index_document(key_hash, key, text_args);
            // NUMERIC auto-indexing (Plan 152-07): safe no-op on indexes with
            // no NUMERIC fields (numeric_index_document returns early on empty
            // numeric_fields).
            #[cfg(feature = "text-index")]
            idx.numeric_index_document(key_hash, key, text_args);
        }
    }

    inserted
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
    insert_lsn: u64,
    txn_id: u64,
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
    // Append to mutable segment. `insert_lsn` is the monotonic LSN allocated
    // by `auto_index_hset`; MVCC visibility (src/vector/mvcc/visibility.rs)
    // compares against query snapshot_lsn to enforce FT.SEARCH AS_OF and
    // TXN snapshot isolation. When inside a TXN (txn_id != 0), use the
    // transactional variant so non-TXN readers see the entry as uncommitted.
    let snap = idx.segments.load();
    let internal_id = if txn_id != 0 {
        snap.mutable
            .append_transactional(key_hash, &f32_vec, &sq_vec, norm, insert_lsn, txn_id)
    } else {
        snap.mutable
            .append(key_hash, &f32_vec, &sq_vec, norm, insert_lsn)
    };
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
    insert_lsn: u64,
    txn_id: u64,
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
    // `insert_lsn` comes from the same allocation as the default-field insert
    // so both fields share one logical write event (Phase 165 MVCC contract).
    // When inside a TXN (txn_id != 0), tag with txn_id for uncommitted visibility.
    let snap = fs.segments.load();
    let _internal_id = if txn_id != 0 {
        snap.mutable
            .append_transactional(key_hash, &f32_vec, &sq_vec, norm, insert_lsn, txn_id)
    } else {
        snap.mutable
            .append(key_hash, &f32_vec, &sq_vec, norm, insert_lsn)
    };
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
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &[(u64, channel::MpscSender<bytes::Bytes>)],
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    shard_id: usize,
) {
    // S3.5b (2026-04-27): hot-path bypass when nothing actually has work.
    // ARM perf annotate showed `repl_backlog.lock()` (caslb/casab) and
    // `repl_state.read()` (RwLock CAS) were ~21% of CPU on 8-shard SET p=64
    // even with `--appendonly no` and zero replicas connected. The criterion
    // is fully derivable from the inputs — no flags or shared state needed.
    // Skipping leaves shard_offset un-advanced; that is fine since with no
    // WAL and no replicas the offsets are dead bytes (no consumer exists).
    if wal_writer.is_none() && wal_v3_writer.is_none() && replica_txs.is_empty() {
        return;
    }
    // WAL v3 supersedes v2 — skip v2 append when v3 is active to avoid
    // double-write overhead (2 write syscalls per SPSC drain batch).
    if let Some(w3) = wal_v3_writer {
        w3.append(
            crate::persistence::wal_v3::record::WalRecordType::Command,
            data,
        );
    } else if let Some(w) = wal_writer {
        w.append(data);
    }
    // 2. Replication backlog (in-memory circular buffer for partial resync).
    //
    // The backlog is shared via Arc<Mutex<Option<...>>> with PSYNC handlers.
    // Cost on the write path:
    //   - When `None` (no replica ever connected): one branch, no lock acquire.
    //   - When `Some` (replication active): one uncontended parking_lot::Mutex
    //     acquire per WAL flush (typically once per 1ms tick batch, NOT per write).
    let mut guard = repl_backlog.lock();
    if let Some(backlog) = guard.as_mut() {
        backlog.append(data);
    }
    drop(guard);
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

#[cfg(test)]
mod wal_append_tests {
    use super::*;
    use crate::replication::backlog::{ReplicationBacklog, SharedBacklog};

    /// S3.5b: when there is no WAL writer and no connected replica, the
    /// function must skip the backlog `Mutex::lock()` and the `repl_state`
    /// `RwLock::read()` entirely. We assert this indirectly by allocating
    /// the backlog and checking that its end_offset stays at 0 — the bypass
    /// returns before the backlog append.
    #[test]
    fn test_wal_append_bypass_when_no_writers_no_replicas() {
        let backlog: SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(Some(ReplicationBacklog::new(1024))));
        let initial_end = backlog.lock().as_ref().unwrap().end_offset();

        wal_append_and_fanout(
            b"hello",
            &mut None, // no v2 writer
            &mut None, // no v3 writer
            &backlog,
            &[],   // no replicas
            &None, // no repl_state
            0,
        );

        let final_end = backlog.lock().as_ref().unwrap().end_offset();
        assert_eq!(
            final_end, initial_end,
            "bypass must skip backlog append when no writers and no replicas"
        );
    }

    /// S3.5b: when a replica is connected (replica_txs non-empty), the
    /// bypass must NOT trigger — the backlog must still receive bytes so
    /// partial resync continues to work after this optimization.
    #[test]
    fn test_wal_append_writes_backlog_when_replicas_present() {
        let backlog: SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(Some(ReplicationBacklog::new(1024))));
        let (tx, _rx) = crate::runtime::channel::mpsc_unbounded::<bytes::Bytes>();
        let replica_txs = vec![(1u64, tx)];

        wal_append_and_fanout(
            b"hello",
            &mut None,
            &mut None,
            &backlog,
            &replica_txs,
            &None,
            0,
        );

        let end = backlog.lock().as_ref().unwrap().end_offset();
        assert_eq!(
            end, 5,
            "backlog must receive 5 bytes when at least one replica is connected"
        );
    }
}
