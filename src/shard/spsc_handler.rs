//! SPSC message drain and cross-shard command dispatch handlers.
//!
//! Extracted from shard/mod.rs to reduce file size. These are synchronous
//! functions called from the event loop's select! arms.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use ringbuf::HeapCons;
use ringbuf::traits::Consumer;
use tracing::info;

use crate::blocking::BlockingRegistry;
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch as cmd_dispatch};
use crate::config::RuntimeConfig;
use crate::persistence::aof;
use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal_v3::segment::WalWriterV3;
use crate::pubsub::PubSubRegistry;
use crate::replication::backlog::ReplicationBacklog;
use crate::runtime::channel;
use crate::storage::Database;
use crate::storage::entry::CachedClock;
use crate::storage::eviction::{
    try_evict_if_needed_async_spill_budget_reporting, try_evict_if_needed_budget_reporting,
};
use crate::storage::tiered::spill_thread::SpillRequest;

use crate::command::vector_search;
use crate::vector::store::VectorStore;

use super::dispatch::ShardMessage;
use super::shared_databases::ShardDatabases;

/// SPSC-side write-path OOM/eviction gate (M2 fix).
///
/// Mirrors `run_write_eviction_gate` in `src/server/conn/handler_monoio/mod.rs`
/// exactly (same elastic-budget lookup, same spill-vs-plain branching, same
/// OOM `Frame::Error` on failure) — this file is runtime-agnostic (shared by
/// both `runtime-monoio` and `runtime-tokio`, so it cannot call that
/// `#[cfg(feature = "runtime-monoio")]`-gated function directly. Cross-shard
/// SPSC legs (`Execute`/`MultiExecute`/`PipelineBatch` + their `*Slotted`
/// variants) execute writes against the TARGET shard's `&mut Database`
/// directly, bypassing the connection handlers' write path entirely — without
/// this gate a scatter-gather write could grow a remote shard's memory past
/// `maxmemory` without limit.
#[allow(clippy::too_many_arguments)]
pub(super) fn spsc_eviction_gate(
    db: &mut Database,
    db_idx: usize,
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    spill_sender: Option<&flume::Sender<SpillRequest>>,
    spill_file_id: &Rc<Cell<u64>>,
    disk_offload_dir: Option<&std::path::Path>,
    // task #34 (Wave A): fires once per plain-dropped (non-spill) victim so
    // the caller can emit a dual-plane DEL record. Callers build this from
    // `record_reason_del` (event-loop-context flavor) — `drain_spsc_shared`
    // already has the shard loop's pre-extracted `SharedBacklog`/
    // `OffsetHandle`/`Vec<ReplicaFanout>` in scope for its own
    // `wal_append_and_fanout` calls right next to each `spsc_eviction_gate`
    // call site.
    on_plain_drop: &mut dyn FnMut(&[u8]),
) -> Result<(), crate::protocol::Frame> {
    let rt = runtime_config.read();
    let budget = shard_databases.elastic_budget(shard_id);
    let global_result = if let Some(sender) = spill_sender {
        let mut fid = spill_file_id.get();
        let dir = disk_offload_dir.unwrap_or(std::path::Path::new("."));
        // Task #34 review (defect 1 follow-through): this gate has no
        // `ShardManifest` handle either (same reasoning as
        // `run_write_eviction_gate`'s doc comment) — a cross-shard write
        // past `maxmemory` under `--disk-offload enable` reliably takes the
        // "no manifest reachable" plain-drop fallback. Previously called the
        // non-reporting wrapper (hardcoded no-op sink), silently dropping
        // `on_plain_drop` on the floor for this branch even though the
        // caller already threads a real one for the sibling branch below.
        let res = try_evict_if_needed_async_spill_budget_reporting(
            db,
            &rt,
            sender,
            dir,
            &mut fid,
            db_idx,
            budget,
            on_plain_drop,
        );
        spill_file_id.set(fid);
        res
    } else {
        try_evict_if_needed_budget_reporting(db, &rt, budget, on_plain_drop)
    };
    global_result?;
    // WS5b: per-db quota, additive and finer-grained than the whole-instance
    // maxmemory gate above. Zero-cost when unconfigured for this db. This is
    // the ONE gate for cross-shard scatter-gather writes (MSET/DEL/etc. and
    // any command whose keys land on a shard other than the client's own),
    // so a quota'd db cannot be grown past its cap via a remote leg either.
    crate::storage::db_quota::check_db_maxmemory(db, db_idx, &rt)
}

/// Drain all SPSC consumer channels, processing cross-shard messages.
///
/// SnapshotBegin messages are collected into `pending_snapshot` for deferred handling
/// (the caller has mutable access to snapshot_state). COW intercepts and WAL appends
/// happen inline for Execute/MultiExecute write commands.
///
/// Returns `true` when the cycle stopped early (MAX_DRAIN_PER_CYCLE cap or a
/// SnapshotBegin barrier) and queued messages may remain — the caller must
/// self-re-notify its own `spsc_notify` so the tail drains on the next loop
/// iteration instead of waiting for the periodic tick (spsc-wake-floor M3).
#[tracing::instrument(skip_all, level = "debug")]
pub(crate) fn drain_spsc_shared(
    shard_databases: &Arc<ShardDatabases>,
    consumers: &mut [HeapCons<ShardMessage>],
    pubsub_registry: &parking_lot::RwLock<PubSubRegistry>,
    blocking_registry: &Rc<RefCell<BlockingRegistry>>,
    pending_snapshot: &mut Option<(
        u64,
        std::path::PathBuf,
        channel::OneshotSender<Result<(), String>>,
    )>,
    snapshot_state: &mut Option<SnapshotState>,
    wal_writer: &mut Option<WalWriterV3>,
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    shard_id: usize,
    script_cache: &Rc<RefCell<crate::scripting::ScriptCache>>,
    cached_clock: &CachedClock,
    pending_migrations: &mut Vec<(
        crate::shard::dispatch::RawSocketFd,
        crate::server::conn::affinity::MigratedConnectionState,
    )>,
    pending_cdc_subscribes: &mut Vec<crate::shard::dispatch::CdcSubscribePayload>,
    // P8: optional manifest for VACUUM manifest/WAL passes; None when no persistence_dir.
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    // P8: MVCC committed-prune margin from server config (default 1000).
    mvcc_prune_margin: u64,
    // P7: graph segment merge thresholds for VACUUM GRAPH.
    #[cfg_attr(not(feature = "graph"), allow(unused_variables))] graph_merge_max_segments: usize,
    #[cfg_attr(not(feature = "graph"), allow(unused_variables))] graph_dead_edge_trigger: f64,
    // MA5: autovacuum daemon reference for RECLAMATION SCHEDULE commands.
    autovacuum_daemon: &mut crate::shard::autovacuum::AutovacuumDaemon,
    // FIX-W1-2: per-shard AOF writer pool. Passed through to handle_shard_message_shared
    // so cross-shard writes (MSET/MultiExecute) also land in the per-shard AOF files.
    aof_pool: Option<&std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    // Whether KV command records should be logged to the per-shard WAL this
    // drain cycle (`--wal-kv-log`; auto = false when the AOF is the recovery
    // authority and no CDC subscriber is attached — see wal_append_and_fanout).
    wal_kv_log: bool,
    // M2 fix: OOM/eviction context for cross-shard write legs — mirrors what
    // `ConnectionContext` carries for the local write path.
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    spill_sender: Option<&flume::Sender<SpillRequest>>,
    spill_file_id: &Rc<Cell<u64>>,
    disk_offload_dir: Option<&std::path::Path>,
) -> bool {
    const MAX_DRAIN_PER_CYCLE: usize = 256;
    let mut drained = 0;

    // Batch-level eviction gate (perf parity with handler_monoio's
    // `batch_eviction_active`): snapshot "is maxmemory set?" once per drain
    // cycle from the process-global atomic (Gap C) instead of taking
    // `runtime_config.read()` per drain cycle. When neither maxmemory nor
    // disk-offload is configured — the common non-memory-bound path — every
    // write arm below skips the eviction call (and any lock acquire)
    // entirely. `runtime_config` is still threaded through for the actual
    // eviction pass (`spsc_eviction_gate`) when this is true.
    //
    // WS5b fix-first review: MUST also consult the per-db quota atomic —
    // this gate skips the call to `spsc_eviction_gate` entirely (which is
    // where the db-quota check for cross-shard write legs lives), so without
    // this term a server with `--maxmemory 0` and no spill sender never
    // enforces `--db-maxmemory` on any remote-shard write leg either. Same
    // bug class as the `batch_eviction_active` fix in handler_monoio/mod.rs.
    let evict_active = spill_sender.is_some()
        || crate::storage::eviction::maxmemory_is_set()
        || crate::storage::db_quota::db_maxmemory_any_set();

    // Collect all messages first, then batch Execute/PipelineBatch under single borrow.
    //
    // Scratch buffers are thread-local (one shard per OS thread) so this
    // function — called from the 1ms tick and every I/O select arm — does
    // not heap-allocate two Vecs per invocation. `mem::take` (instead of
    // holding the RefCell borrow) keeps re-entrancy safe: a nested call
    // would simply fall back to fresh empty Vecs.
    thread_local! {
        static DRAIN_SCRATCH: RefCell<(Vec<ShardMessage>, Vec<ShardMessage>)> =
            const { RefCell::new((Vec::new(), Vec::new())) };
    }
    let (mut execute_batch, mut other_messages) =
        DRAIN_SCRATCH.with(|s| std::mem::take(&mut *s.borrow_mut()));
    execute_batch.clear();
    other_messages.clear();

    // Self-queue FIRST: same-shard tasks (inline PSYNC RegisterReplica,
    // local-write ReplicaLiveFanout) cannot SPSC to their own shard — the
    // mesh is N·(N−1) skip-self — so they enqueue on the thread-local self
    // queue (`shard::self_msg`). All self messages are control-plane arms
    // (never Execute-batch / SnapshotBegin types), so they route through
    // `other_messages`. FIFO here is a correctness invariant: a write's
    // ReplicaLiveFanout drained before RegisterReplica never live-sends to
    // the not-yet-registered replica (it gets those bytes via backlog
    // catch-up instead), one drained after fans to the freshly-registered
    // replica — no gap, no double-delivery (see RegisterReplica.push_offset).
    //
    // Drained UNBOUNDED, on purpose: every arm is cheap (try_send loop /
    // vec push), the queue refills only while this thread's own tasks run
    // (bounded by one loop iteration's frame budget), and an entry left
    // behind by a MAX_DRAIN_PER_CYCLE cut-off would delay a replica's live
    // bytes by up to a full tick. The SPSC consumers below keep their
    // per-cycle bound.
    loop {
        let Some(msg) = crate::shard::self_msg::pop() else {
            break;
        };
        drained += 1;
        other_messages.push(msg);
    }

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
                        | ShardMessage::TxnExecute(_)
                        | ShardMessage::ExecuteSlotted { .. }
                        | ShardMessage::PipelineBatchSlotted { .. }
                        | ShardMessage::MultiExecuteSlotted { .. }
                        | ShardMessage::VectorSearch(_)
                        | ShardMessage::VectorCommand { .. }
                        | ShardMessage::DocFreq(_)
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
                        ShardMessage::GraphRollback(_) => {
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
                            // has from wal_writer.wal_dir().
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
        for msg in execute_batch.drain(..) {
            handle_shard_message_shared(
                shard_databases,
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
                shard_manifest,
                mvcc_prune_margin,
                graph_merge_max_segments,
                graph_dead_edge_trigger,
                autovacuum_daemon,
                aof_pool, // FIX-W1-2: thread AOF pool through SPSC drain
                wal_kv_log,
                evict_active,
                runtime_config,
                spill_sender,
                spill_file_id,
                disk_offload_dir,
            );
        }
    }

    if drained > 0 {
        crate::admin::metrics_setup::record_spsc_drain(shard_id, drained as u64);
    }

    // Process other messages (PubSubPublish, SnapshotBegin, etc.)
    for msg in other_messages.drain(..) {
        handle_shard_message_shared(
            shard_databases,
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
            shard_manifest,
            mvcc_prune_margin,
            graph_merge_max_segments,
            graph_dead_edge_trigger,
            autovacuum_daemon,
            aof_pool, // FIX-W1-2: thread AOF pool through SPSC drain
            wal_kv_log,
            evict_active,
            runtime_config,
            spill_sender,
            spill_file_id,
            disk_offload_dir,
        );
    }

    // Return the (now drained) scratch buffers so their capacity is reused
    // by the next drain cycle.
    DRAIN_SCRATCH.with(|s| {
        *s.borrow_mut() = (execute_batch, other_messages);
    });

    // spsc-wake-floor M3: `true` means this cycle stopped early (drain cap or
    // SnapshotBegin barrier) and messages may remain queued — the caller must
    // self-re-notify so the tail is drained on the next loop iteration instead
    // of stranding until the next periodic tick.
    drained >= MAX_DRAIN_PER_CYCLE || snapshot_seen
}

/// Process a single cross-shard message using shared database access.
///
/// Performs COW intercept for write commands when a snapshot is active,
/// and appends write commands to the per-shard WAL writer.
pub(crate) fn handle_shard_message_shared(
    shard_databases: &Arc<ShardDatabases>,
    pubsub_registry: &parking_lot::RwLock<PubSubRegistry>,
    blocking_registry: &Rc<RefCell<BlockingRegistry>>,
    msg: ShardMessage,
    pending_snapshot: &mut Option<(
        u64,
        std::path::PathBuf,
        channel::OneshotSender<Result<(), String>>,
    )>,
    snapshot_state: &mut Option<SnapshotState>,
    wal_writer: &mut Option<WalWriterV3>,
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    shard_id: usize,
    script_cache: &Rc<RefCell<crate::scripting::ScriptCache>>,
    cached_clock: &CachedClock,
    // P8: optional manifest for VACUUM manifest/WAL passes; None when no persistence_dir.
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    // P8: MVCC committed-prune margin from server config (default 1000).
    mvcc_prune_margin: u64,
    // P7: graph segment merge thresholds for VACUUM GRAPH.
    #[cfg_attr(not(feature = "graph"), allow(unused_variables))] graph_merge_max_segments: usize,
    #[cfg_attr(not(feature = "graph"), allow(unused_variables))] graph_dead_edge_trigger: f64,
    // MA5: autovacuum daemon reference for RECLAMATION SCHEDULE commands.
    autovacuum_daemon: &mut crate::shard::autovacuum::AutovacuumDaemon,
    // FIX-W1-2: per-shard AOF writer pool. When Some, each successful write command
    // is also routed to the owning shard's AOF file via fire-and-forget try_send_append.
    aof_pool: Option<&std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    // Whether KV command records should be logged to the per-shard WAL
    // (`--wal-kv-log`; see wal_append_and_fanout).
    wal_kv_log: bool,
    // M2 fix: precomputed `maxmemory != 0 || disk-offload configured` (see
    // `drain_spsc_shared`) — skips the eviction gate call (and its lock
    // acquire) entirely on the common non-memory-bound path.
    evict_active: bool,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    spill_sender: Option<&flume::Sender<SpillRequest>>,
    spill_file_id: &Rc<Cell<u64>>,
    disk_offload_dir: Option<&std::path::Path>,
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
                    // All slice fields (vector_store, text_store, graph_store, databases)
                    // acquired in one flat with_shard closure — no outer borrow active,
                    // so this cannot re-enter with_shard.
                    let frame = {
                        crate::shard::slice::with_shard(|s| {
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
                            // WS5a: use the message's own selected db, not a
                            // hardcoded db 0 — this is the ShardMessage::Execute
                            // console-gateway path, which carries a real db_index.
                            let db_opt: Option<&mut crate::storage::db::Database> = if needs_db {
                                s.databases.get_mut(db_idx)
                            } else {
                                None
                            };
                            dispatch_vector_command(
                                &mut s.vector_store,
                                &mut s.text_store,
                                #[cfg(feature = "graph")]
                                Some(&s.graph_store),
                                &command,
                                db_opt,
                                db_idx as u8,
                            )
                        })
                    };
                    let _ = reply_tx.send(frame);
                    return;
                }

                // VACUUM VECTOR <idx> — P2 segment merge. Intercept before main dispatch
                // because it needs mutable VectorStore access (not available in cmd_dispatch).
                // VACUUM GRAPH <name> — P7 graph segment merge, same reason.
                if cmd.eq_ignore_ascii_case(b"VACUUM") {
                    if let Some(crate::protocol::Frame::BulkString(sub)) = args.first() {
                        if sub.eq_ignore_ascii_case(b"VECTOR") {
                            let idx_args = &args[1..];
                            let frame = crate::shard::slice::with_shard(|s| {
                                crate::command::server_admin::vacuum_vector(
                                    &mut s.vector_store,
                                    idx_args,
                                    db_idx as u8,
                                )
                            });
                            let _ = reply_tx.send(frame);
                            return;
                        }
                        #[cfg(feature = "graph")]
                        if sub.eq_ignore_ascii_case(b"GRAPH") {
                            let graph_args = &args[1..];
                            let frame = {
                                crate::shard::slice::with_shard(|s| {
                                    crate::command::server_admin::vacuum_graph(
                                        &mut s.graph_store,
                                        graph_args,
                                        graph_merge_max_segments,
                                        graph_dead_edge_trigger,
                                    )
                                })
                            };
                            let _ = reply_tx.send(frame);
                            return;
                        }
                    }
                    // Fall through for VACUUM without VECTOR/GRAPH subcommand.
                }

                // GRAPH.* commands route to GraphStore.
                #[cfg(feature = "graph")]
                if cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.") {
                    let (frame, wal_records) = {
                        crate::shard::slice::with_shard(|s| {
                            let resp = crate::command::graph::dispatch_graph_command(
                                &mut s.graph_store,
                                &command,
                            );
                            let records = s.graph_store.drain_wal();
                            (resp, records)
                        })
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
                    let frame = crate::shard::slice::with_shard(|s| {
                        crate::command::server_admin::kill_snapshot(&mut s.vector_store, args)
                    });
                    let _ = reply_tx.send(frame);
                    return;
                }

                // P8: VACUUM — manual reclamation across manifest, MVCC, and WAL.
                // Bypasses write-stall guards (reclaims, does not write data).
                if cmd.eq_ignore_ascii_case(b"VACUUM") {
                    let frame = crate::shard::slice::with_shard(|s| {
                        crate::command::server_admin::vacuum(
                            &mut s.vector_store,
                            shard_manifest.as_mut(),
                            wal_writer.as_mut(),
                            args,
                            mvcc_prune_margin,
                        )
                    });
                    let _ = reply_tx.send(frame);
                    return;
                }

                // P8: DEBUG RECLAMATION — verbose per-subsystem diagnostic dump.
                // Intercept here so it has access to manifest and WAL (read-only).
                if cmd.eq_ignore_ascii_case(b"DEBUG") {
                    if let Some(sub) = args.first() {
                        if let Some(sub_bytes) = crate::command::helpers::extract_bytes(sub) {
                            if sub_bytes.eq_ignore_ascii_case(b"RECLAMATION") {
                                let frame = crate::shard::slice::with_shard(|s| {
                                    crate::command::server_admin::debug_reclamation(
                                        &s.vector_store,
                                        shard_manifest.as_ref(),
                                        wal_writer.as_ref(),
                                    )
                                });
                                let _ = reply_tx.send(frame);
                                return;
                            }
                        }
                    }
                    // All other DEBUG subcommands fall through to cmd_dispatch.
                }

                // MA5: RECLAMATION SCHEDULE — maintenance-window scheduler.
                // Needs &mut AutovacuumDaemon (schedule lives there); intercept here.
                if cmd.eq_ignore_ascii_case(b"RECLAMATION") {
                    if let Some(sub) = args.first() {
                        if let Some(s) = crate::command::helpers::extract_bytes(sub) {
                            if s.eq_ignore_ascii_case(b"SCHEDULE") {
                                let frame = crate::command::server_admin::reclamation_schedule(
                                    &mut autovacuum_daemon.maintenance_schedule,
                                    &args[1..],
                                );
                                let _ = reply_tx.send(frame);
                                return;
                            }
                        }
                    }
                    // Other RECLAMATION subcommands fall through.
                }

                // T2.2 MOVE — atomically moves a key between two dbs on the same shard.
                // T2.3 COPY DB n — copies a key to a different db on the same shard.
                // Both require two databases simultaneously; intercept before cmd_dispatch.
                // Gap A: shared with every other ShardMessage arm via
                // spsc_two_db::try_two_db_intercept — SPSC runs single-threaded
                // per shard, so the slice path (split_at_mut, no locking) is safe.
                if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
                    let intercepted = crate::shard::slice::with_shard(|s| {
                        crate::shard::spsc_two_db::try_two_db_intercept(
                            cmd,
                            args,
                            &mut s.databases,
                            db_idx,
                            db_count,
                            cached_clock,
                            evict_active,
                            shard_databases,
                            shard_id,
                            runtime_config,
                            spill_sender,
                            spill_file_id,
                            disk_offload_dir,
                        )
                    });
                    if let Some(mut response) = intercepted {
                        if matches!(response, crate::protocol::Frame::Integer(1)) {
                            let serialized = aof::serialize_command(&command);
                            let mut aof_budget =
                                crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
                            if !wal_append_and_fanout(
                                &serialized,
                                db_idx,
                                wal_writer,
                                repl_backlog,
                                replica_txs,
                                repl_state,
                                shard_id,
                                aof_pool, // FIX-W1-2
                                wal_kv_log,
                                &mut aof_budget,
                            ) {
                                response = crate::protocol::Frame::Error(
                                    bytes::Bytes::from_static(AOF_APPEND_LOST_ERR),
                                );
                            }
                        }
                        let _ = reply_tx.send(response);
                        return;
                    }
                    // COPY with no DB clause or same-db: fall through to
                    // cmd_dispatch → key_extra::copy. (MOVE always returns
                    // Some(..) from the helper — parse_move_args either
                    // succeeds or produces an error frame — so this fall-
                    // through is COPY-only in practice.)
                }

                // COW intercept: capture old value before write if snapshot is active
                let is_write = metadata::is_write(cmd);
                // M2 fix: OOM/eviction gate for the target shard, run BEFORE
                // COW intercept/dispatch (same order as handler_monoio's
                // write path) — a remote-shard write leg must not be able to
                // grow memory past `maxmemory` just because it arrived via
                // SPSC instead of a local connection.
                let mut oom_frame: Option<crate::protocol::Frame> = None;
                // write_db and text_store (HSET auto-index) accessed in one with_shard
                // closure to avoid re-entrant borrow (multi-resource arm).
                let frame = {
                    if is_write {
                        crate::shard::slice::with_shard_db(db_idx, |db| {
                            if evict_active {
                                if let Err(oom) = spsc_eviction_gate(
                                    db,
                                    db_idx,
                                    shard_databases,
                                    shard_id,
                                    runtime_config,
                                    spill_sender,
                                    spill_file_id,
                                    disk_offload_dir,
                                    // task #34 (Wave A): cross-shard write leg.
                                    &mut |key| {
                                        crate::replication::reason_del::record_reason_del(
                                            key,
                                            db_idx,
                                            wal_writer,
                                            repl_backlog,
                                            replica_txs,
                                            repl_state,
                                            shard_id,
                                            aof_pool,
                                            wal_kv_log,
                                        );
                                    },
                                ) {
                                    oom_frame = Some(oom);
                                    return;
                                }
                            }
                            cow_intercept(snapshot_state, db, db_idx, &command);
                        });
                    }
                    if let Some(oom) = oom_frame.take() {
                        oom
                    } else {
                        crate::shard::slice::with_shard(|s| {
                            let db = &mut s.databases[db_idx];
                            db.refresh_now_from_cache(cached_clock);
                            let mut selected = db_idx;
                            let result = cmd_dispatch(db, cmd, args, &mut selected, db_count);
                            let frame = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => f,
                            };

                            // WAL append + replication fan-out for successful write commands
                            let mut aof_ok = true;
                            if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                                let serialized = aof::serialize_command(&command);
                                let mut aof_budget =
                                    crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
                                aof_ok = wal_append_and_fanout(
                                    &serialized,
                                    db_idx,
                                    wal_writer,
                                    repl_backlog,
                                    replica_txs,
                                    repl_state,
                                    shard_id,
                                    aof_pool, // FIX-W1-2
                                    wal_kv_log,
                                    &mut aof_budget,
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
                                    let wake_key = if cmd.eq_ignore_ascii_case(b"LMOVE") {
                                        args.get(1).and_then(|f| {
                                            crate::server::connection::extract_bytes(f)
                                        })
                                    } else {
                                        args.first().and_then(|f| {
                                            crate::server::connection::extract_bytes(f)
                                        })
                                    };
                                    if let Some(key) = wake_key {
                                        let mut reg = blocking_registry.borrow_mut();
                                        if cmd.eq_ignore_ascii_case(b"LPUSH")
                                            || cmd.eq_ignore_ascii_case(b"RPUSH")
                                            || cmd.eq_ignore_ascii_case(b"LMOVE")
                                        {
                                            crate::blocking::wakeup::try_wake_list_waiter(
                                                &mut reg, db, db_idx, &key,
                                            );
                                        } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                            crate::blocking::wakeup::try_wake_zset_waiter(
                                                &mut reg, db, db_idx, &key,
                                            );
                                        } else {
                                            crate::blocking::wakeup::try_wake_stream_waiter(
                                                &mut reg, db, db_idx, &key,
                                            );
                                        }
                                    }
                                }
                            }

                            // Auto-index: if HSET succeeded and key matches a vector index prefix,
                            // extract the vector field and append to mutable segment.
                            // vector_store and text_store accessed here (same with_shard closure).
                            if cmd.eq_ignore_ascii_case(b"HSET")
                                && !matches!(frame, crate::protocol::Frame::Error(_))
                            {
                                if let Some(crate::protocol::Frame::BulkString(key_bytes)) =
                                    args.first()
                                {
                                    // Plan 166-01: return value (index_name, key_hash)
                                    // tuples will be consumed by Plan 166-02 to record
                                    // VectorIntents on the active CrossStoreTxn. Discarded
                                    // here because this path is not txn-aware yet.
                                    let _ = auto_index_hset(
                                        &mut s.vector_store,
                                        &mut s.text_store,
                                        key_bytes,
                                        args,
                                        0,
                                        db_idx as u8,
                                    );
                                }
                            }

                            // Fail-loud: the mutation is applied (wake/auto-index above
                            // ran on real state), but the client must not see success
                            // for a write whose AOF record was dropped.
                            if aof_ok {
                                frame
                            } else {
                                crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                    AOF_APPEND_LOST_ERR,
                                ))
                            }
                        })
                    }
                };

                // Auto-delete is a vector_store-only operation; runs outside the gate.
                // Each arm uses its own flat with_shard borrow — no outer borrow is active.
                if (cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK"))
                    && !matches!(frame, crate::protocol::Frame::Error(_))
                {
                    crate::shard::slice::with_shard(|s| {
                        for arg in args.iter() {
                            if let crate::protocol::Frame::BulkString(key_bytes) = arg {
                                s.vector_store
                                    .mark_deleted_for_key_for_db(key_bytes.as_ref(), db_idx as u8);
                            }
                        }
                    });
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
            let db_count = shard_databases.db_count();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            crate::shard::slice::with_shard(|s| {
                s.databases[db_idx].refresh_now_from_cache(cached_clock);
                // ONE backpressure budget for the whole batch: under sustained
                // AOF backpressure the shard thread stalls at most BOUND total,
                // not BOUND × batch-len (review finding, PR #211).
                let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
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

                    // Gap A: MOVE / COPY-DB two-db intercept, mirroring the
                    // plain Execute arm — needs two &mut Database borrows at
                    // once, so it must run before `guard` narrows to a single
                    // db below.
                    if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
                        if let Some(response) = crate::shard::spsc_two_db::try_two_db_intercept(
                            cmd,
                            args,
                            &mut s.databases,
                            db_idx,
                            db_count,
                            cached_clock,
                            evict_active,
                            shard_databases,
                            shard_id,
                            runtime_config,
                            spill_sender,
                            spill_file_id,
                            disk_offload_dir,
                        ) {
                            let mut aof_ok = true;
                            if matches!(response, crate::protocol::Frame::Integer(1))
                                && wal_fanout_has_work(
                                    wal_writer,
                                    replica_txs,
                                    aof_pool,
                                    wal_kv_log,
                                )
                            {
                                let serialized = aof::serialize_command(cmd_frame);
                                aof_ok = wal_append_and_fanout(
                                    &serialized,
                                    db_idx,
                                    wal_writer,
                                    repl_backlog,
                                    replica_txs,
                                    repl_state,
                                    shard_id,
                                    aof_pool, // FIX-W1-2
                                    wal_kv_log,
                                    &mut aof_budget,
                                );
                            }
                            results.push(if aof_ok {
                                response
                            } else {
                                crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                    AOF_APPEND_LOST_ERR,
                                ))
                            });
                            continue;
                        }
                        // COPY with no DB clause or same-db: fall through to
                        // the generic single-db write path below.
                    }

                    let guard = &mut s.databases[db_idx];
                    let is_write = metadata::is_write(cmd);
                    if is_write {
                        // M2 fix: same gate as the Execute arm, applied per
                        // command in this batch's shared `guard` borrow.
                        if evict_active {
                            if let Err(oom) = spsc_eviction_gate(
                                guard,
                                db_idx,
                                shard_databases,
                                shard_id,
                                runtime_config,
                                spill_sender,
                                spill_file_id,
                                disk_offload_dir,
                                // task #34 (Wave A): cross-shard write leg.
                                &mut |key| {
                                    crate::replication::reason_del::record_reason_del(
                                        key,
                                        db_idx,
                                        wal_writer,
                                        repl_backlog,
                                        replica_txs,
                                        repl_state,
                                        shard_id,
                                        aof_pool,
                                        wal_kv_log,
                                    );
                                },
                            ) {
                                results.push(oom);
                                continue;
                            }
                        }
                        cow_intercept(snapshot_state, guard, db_idx, cmd_frame);
                    }

                    let mut selected = db_idx;
                    let result = cmd_dispatch(guard, cmd, args, &mut selected, db_count);
                    let frame = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    };

                    let mut aof_ok = true;
                    if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                        // Skip the serialization alloc when the fanout would
                        // no-op (persistence + replication all off) — it was
                        // pure waste on every cross-shard write.
                        if wal_fanout_has_work(wal_writer, replica_txs, aof_pool, wal_kv_log) {
                            let serialized = aof::serialize_command(cmd_frame);
                            aof_ok = wal_append_and_fanout(
                                &serialized,
                                db_idx,
                                wal_writer,
                                repl_backlog,
                                replica_txs,
                                repl_state,
                                shard_id,
                                aof_pool, // FIX-W1-2
                                wal_kv_log,
                                &mut aof_budget,
                            );
                        }

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
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                    crate::blocking::wakeup::try_wake_zset_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else {
                                    crate::blocking::wakeup::try_wake_stream_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                }
                            }
                        }
                    }

                    results.push(if aof_ok {
                        frame
                    } else {
                        crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            AOF_APPEND_LOST_ERR,
                        ))
                    });
                }
            });
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
            // write_db and text_store (HSET auto-index) accessed in one with_shard
            // closure to avoid re-entrant borrow (multi-resource arm).
            crate::shard::slice::with_shard(|s| {
                // One-time refresh via a scoped temporary borrow — `guard`
                // itself moves INSIDE the loop below (Gap A) so the MOVE/
                // COPY-DB branch can borrow `&mut s.databases` (both src and
                // dst) for the same command.
                s.databases[db_idx].refresh_now_from_cache(cached_clock);
                // ONE backpressure budget for the whole batch: under sustained
                // AOF backpressure the shard thread stalls at most BOUND total,
                // not BOUND × batch-len (review finding, PR #211).
                let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
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

                    // Gap A: MOVE / COPY-DB two-db intercept, mirroring the
                    // plain Execute arm — needs two &mut Database borrows at
                    // once. Returns BEFORE the auto-index/wake hooks below
                    // too, mirroring the Execute arm's pre-existing behavior
                    // (not new for this fix — see the Gap A commit body).
                    if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
                        if let Some(response) = crate::shard::spsc_two_db::try_two_db_intercept(
                            cmd,
                            args,
                            &mut s.databases,
                            db_idx,
                            db_count,
                            cached_clock,
                            evict_active,
                            shard_databases,
                            shard_id,
                            runtime_config,
                            spill_sender,
                            spill_file_id,
                            disk_offload_dir,
                        ) {
                            let mut aof_ok = true;
                            if matches!(response, crate::protocol::Frame::Integer(1))
                                && wal_fanout_has_work(
                                    wal_writer,
                                    replica_txs,
                                    aof_pool,
                                    wal_kv_log,
                                )
                            {
                                let serialized = aof::serialize_command(cmd_frame);
                                aof_ok = wal_append_and_fanout(
                                    &serialized,
                                    db_idx,
                                    wal_writer,
                                    repl_backlog,
                                    replica_txs,
                                    repl_state,
                                    shard_id,
                                    aof_pool, // FIX-C4-FOLD
                                    wal_kv_log,
                                    &mut aof_budget,
                                );
                            }
                            results.push(if aof_ok {
                                response
                            } else {
                                crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                    AOF_APPEND_LOST_ERR,
                                ))
                            });
                            continue;
                        }
                        // COPY with no DB clause or same-db: fall through to
                        // the generic single-db write path below.
                    }

                    let guard = &mut s.databases[db_idx];
                    let is_write = metadata::is_write(cmd);
                    if is_write {
                        // M2 fix: same gate as the Execute arm, applied per
                        // command in this batch's shared `guard` borrow.
                        if evict_active {
                            if let Err(oom) = spsc_eviction_gate(
                                guard,
                                db_idx,
                                shard_databases,
                                shard_id,
                                runtime_config,
                                spill_sender,
                                spill_file_id,
                                disk_offload_dir,
                                // task #34 (Wave A): cross-shard write leg.
                                &mut |key| {
                                    crate::replication::reason_del::record_reason_del(
                                        key,
                                        db_idx,
                                        wal_writer,
                                        repl_backlog,
                                        replica_txs,
                                        repl_state,
                                        shard_id,
                                        aof_pool,
                                        wal_kv_log,
                                    );
                                },
                            ) {
                                results.push(oom);
                                continue;
                            }
                        }
                        cow_intercept(snapshot_state, guard, db_idx, cmd_frame);
                    }

                    let mut selected = db_idx;
                    let result = cmd_dispatch(guard, cmd, args, &mut selected, db_count);
                    let frame = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    };

                    let mut aof_ok = true;
                    if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                        // See `wal_fanout_has_work` — skip the serialization alloc
                        // entirely when the fanout would no-op (persistence off).
                        if wal_fanout_has_work(wal_writer, replica_txs, aof_pool, wal_kv_log) {
                            let serialized = aof::serialize_command(cmd_frame);
                            aof_ok = wal_append_and_fanout(
                                &serialized,
                                db_idx,
                                wal_writer,
                                repl_backlog,
                                replica_txs,
                                repl_state,
                                shard_id,
                                // C4-FOLD-FIX: AOF append MUST happen here (in the SPSC arm,
                                // before the response is sent) so the append is already in the
                                // AOF channel when AofFold reads sender.len(). Moving the append
                                // to the connection handler (after awaiting the response) defers
                                // it until AFTER drain_spsc_shared returns, so AofFold's
                                // pending_aof_count undercount by ≥1 and that append escapes
                                // into the NEW incr → double-apply on restart (+1 after
                                // restart observed in test_ssm4a_fold_4shard_experimental).
                                // The handler_monoio cross-shard AOF write is removed to avoid
                                // the double-write that was the original reason for None.
                                aof_pool, // FIX-C4-FOLD
                                wal_kv_log,
                                &mut aof_budget,
                            );
                        }
                    }

                    // Auto-index: if HSET succeeded, check for vector index match.
                    // text_store and vector_store accessed here (same with_shard closure).
                    if cmd.eq_ignore_ascii_case(b"HSET")
                        && !matches!(frame, crate::protocol::Frame::Error(_))
                    {
                        if let Some(crate::protocol::Frame::BulkString(key_bytes)) = args.first() {
                            // Plan 166-01: Vec<(idx, key_hash)> return discarded
                            // here; Plan 166-02 threads it into CrossStoreTxn.
                            let _ = auto_index_hset(
                                &mut s.vector_store,
                                &mut s.text_store,
                                key_bytes,
                                args,
                                0,
                                db_idx as u8,
                            );
                        }
                    }

                    // Auto-delete vectors on DEL/UNLINK (parity with the HSET
                    // hook above and the Execute arm's auto-delete).
                    if !matches!(frame, crate::protocol::Frame::Error(_))
                        && (cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK"))
                    {
                        auto_delete_vectors(&mut s.vector_store, args, db_idx as u8);
                    }

                    // R4: HDEL of an indexed vector field tombstones the vector.
                    if !matches!(frame, crate::protocol::Frame::Error(_))
                        && cmd.eq_ignore_ascii_case(b"HDEL")
                    {
                        auto_hdel_vectors(&mut s.vector_store, args, db_idx as u8);
                    }

                    // R3: FLUSHALL/FLUSHDB clears index contents (definitions kept).
                    // WS5a: FLUSHDB scopes to `db_idx`; FLUSHALL clears every db.
                    if !matches!(frame, crate::protocol::Frame::Error(_))
                        && (cmd.eq_ignore_ascii_case(b"FLUSHDB")
                            || cmd.eq_ignore_ascii_case(b"FLUSHALL"))
                    {
                        auto_flush_indexes(
                            &mut s.vector_store,
                            &mut s.text_store,
                            cmd.eq_ignore_ascii_case(b"FLUSHDB"),
                            db_idx as u8,
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
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                    crate::blocking::wakeup::try_wake_zset_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else {
                                    crate::blocking::wakeup::try_wake_stream_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                }
                            }
                        }
                    }

                    results.push(if aof_ok {
                        frame
                    } else {
                        crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            AOF_APPEND_LOST_ERR,
                        ))
                    });
                }
            });
            let _ = reply_tx.send(results);
        }
        ShardMessage::ExecuteSlotted {
            db_index,
            command,
            response_slot,
        } => {
            let db_count = shard_databases.db_count();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            let (cmd, args) = match extract_command_static(&command) {
                Some(pair) => pair,
                None => {
                    // Arc-owned slot: deref is safe, refcount keeps it alive.
                    let slot = &*response_slot.0;
                    slot.fill(vec![crate::protocol::Frame::Error(
                        bytes::Bytes::from_static(b"ERR invalid command format"),
                    )]);
                    return;
                }
            };

            // Gap A: MOVE / COPY-DB two-db intercept, mirroring the plain
            // Execute arm — needs two &mut Database borrows at once.
            if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
                let intercepted = crate::shard::slice::with_shard(|s| {
                    crate::shard::spsc_two_db::try_two_db_intercept(
                        cmd,
                        args,
                        &mut s.databases,
                        db_idx,
                        db_count,
                        cached_clock,
                        evict_active,
                        shard_databases,
                        shard_id,
                        runtime_config,
                        spill_sender,
                        spill_file_id,
                        disk_offload_dir,
                    )
                });
                if let Some(mut response) = intercepted {
                    if matches!(response, crate::protocol::Frame::Integer(1)) {
                        let serialized = aof::serialize_command(&command);
                        let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
                        if !wal_append_and_fanout(
                            &serialized,
                            db_idx,
                            wal_writer,
                            repl_backlog,
                            replica_txs,
                            repl_state,
                            shard_id,
                            aof_pool, // FIX-W1-2
                            wal_kv_log,
                            &mut aof_budget,
                        ) {
                            response = crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                AOF_APPEND_LOST_ERR,
                            ));
                        }
                    }
                    // Arc-owned slot: deref is safe, refcount keeps it alive.
                    let slot = &*response_slot.0;
                    slot.fill(vec![response]);
                    return;
                }
                // COPY with no DB clause or same-db: fall through to the
                // generic single-db write path below.
            }

            {
                let is_write = metadata::is_write(cmd);
                // M2 fix: see the Execute arm for rationale/ordering.
                let mut oom_frame: Option<crate::protocol::Frame> = None;
                let frame = {
                    if is_write {
                        crate::shard::slice::with_shard_db(db_idx, |db| {
                            if evict_active {
                                if let Err(oom) = spsc_eviction_gate(
                                    db,
                                    db_idx,
                                    shard_databases,
                                    shard_id,
                                    runtime_config,
                                    spill_sender,
                                    spill_file_id,
                                    disk_offload_dir,
                                    // task #34 (Wave A): cross-shard write leg.
                                    &mut |key| {
                                        crate::replication::reason_del::record_reason_del(
                                            key,
                                            db_idx,
                                            wal_writer,
                                            repl_backlog,
                                            replica_txs,
                                            repl_state,
                                            shard_id,
                                            aof_pool,
                                            wal_kv_log,
                                        );
                                    },
                                ) {
                                    oom_frame = Some(oom);
                                    return;
                                }
                            }
                            cow_intercept(snapshot_state, db, db_idx, &command);
                        });
                    }
                    if let Some(oom) = oom_frame.take() {
                        oom
                    } else {
                        crate::shard::slice::with_shard(|s| {
                            let db = &mut s.databases[db_idx];
                            db.refresh_now_from_cache(cached_clock);
                            let mut selected = db_idx;
                            let result = cmd_dispatch(db, cmd, args, &mut selected, db_count);
                            let frame = match result {
                                DispatchResult::Response(f) => f,
                                DispatchResult::Quit(f) => f,
                            };

                            let mut aof_ok = true;
                            if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                                let serialized = aof::serialize_command(&command);
                                let mut aof_budget =
                                    crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
                                aof_ok = wal_append_and_fanout(
                                    &serialized,
                                    db_idx,
                                    wal_writer,
                                    repl_backlog,
                                    replica_txs,
                                    repl_state,
                                    shard_id,
                                    aof_pool, // FIX-W1-2
                                    wal_kv_log,
                                    &mut aof_budget,
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
                                        args.get(1).and_then(|f| {
                                            crate::server::connection::extract_bytes(f)
                                        })
                                    } else {
                                        args.first().and_then(|f| {
                                            crate::server::connection::extract_bytes(f)
                                        })
                                    };
                                    if let Some(key) = wake_key {
                                        let mut reg = blocking_registry.borrow_mut();
                                        if cmd.eq_ignore_ascii_case(b"LPUSH")
                                            || cmd.eq_ignore_ascii_case(b"RPUSH")
                                            || cmd.eq_ignore_ascii_case(b"LMOVE")
                                        {
                                            crate::blocking::wakeup::try_wake_list_waiter(
                                                &mut reg, db, db_idx, &key,
                                            );
                                        } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                            crate::blocking::wakeup::try_wake_zset_waiter(
                                                &mut reg, db, db_idx, &key,
                                            );
                                        } else {
                                            crate::blocking::wakeup::try_wake_stream_waiter(
                                                &mut reg, db, db_idx, &key,
                                            );
                                        }
                                    }
                                }
                            }

                            // Fail-loud: mutation applied, but the client must not
                            // see success for a write whose AOF record was dropped.
                            if aof_ok {
                                frame
                            } else {
                                crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                    AOF_APPEND_LOST_ERR,
                                ))
                            }
                        })
                    }
                };
                // Arc-owned slot: deref is safe, refcount keeps it alive.
                let slot = &*response_slot.0;
                slot.fill(vec![frame]);
            }
        }
        ShardMessage::MultiExecuteSlotted {
            db_index,
            commands,
            response_slot,
        } => {
            let mut results = Vec::with_capacity(commands.len());
            let db_count = shard_databases.db_count();
            let db_idx = db_index.min(db_count.saturating_sub(1));
            crate::shard::slice::with_shard(|s| {
                s.databases[db_idx].refresh_now_from_cache(cached_clock);
                // ONE backpressure budget for the whole batch: under sustained
                // AOF backpressure the shard thread stalls at most BOUND total,
                // not BOUND × batch-len (review finding, PR #211).
                let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
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

                    // Gap A: MOVE / COPY-DB two-db intercept, mirroring the
                    // plain Execute arm — needs two &mut Database borrows at
                    // once, so it must run before `guard` narrows to a single
                    // db below.
                    if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
                        if let Some(response) = crate::shard::spsc_two_db::try_two_db_intercept(
                            cmd,
                            args,
                            &mut s.databases,
                            db_idx,
                            db_count,
                            cached_clock,
                            evict_active,
                            shard_databases,
                            shard_id,
                            runtime_config,
                            spill_sender,
                            spill_file_id,
                            disk_offload_dir,
                        ) {
                            let mut aof_ok = true;
                            if matches!(response, crate::protocol::Frame::Integer(1))
                                && wal_fanout_has_work(
                                    wal_writer,
                                    replica_txs,
                                    aof_pool,
                                    wal_kv_log,
                                )
                            {
                                let serialized = aof::serialize_command(cmd_frame);
                                aof_ok = wal_append_and_fanout(
                                    &serialized,
                                    db_idx,
                                    wal_writer,
                                    repl_backlog,
                                    replica_txs,
                                    repl_state,
                                    shard_id,
                                    aof_pool, // FIX-W1-2
                                    wal_kv_log,
                                    &mut aof_budget,
                                );
                            }
                            results.push(if aof_ok {
                                response
                            } else {
                                crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                    AOF_APPEND_LOST_ERR,
                                ))
                            });
                            continue;
                        }
                        // COPY with no DB clause or same-db: fall through to
                        // the generic single-db write path below.
                    }

                    let guard = &mut s.databases[db_idx];
                    let is_write = metadata::is_write(cmd);
                    if is_write {
                        // M2 fix: same gate as the Execute arm, applied per
                        // command in this batch's shared `guard` borrow.
                        if evict_active {
                            if let Err(oom) = spsc_eviction_gate(
                                guard,
                                db_idx,
                                shard_databases,
                                shard_id,
                                runtime_config,
                                spill_sender,
                                spill_file_id,
                                disk_offload_dir,
                                // task #34 (Wave A): cross-shard write leg.
                                &mut |key| {
                                    crate::replication::reason_del::record_reason_del(
                                        key,
                                        db_idx,
                                        wal_writer,
                                        repl_backlog,
                                        replica_txs,
                                        repl_state,
                                        shard_id,
                                        aof_pool,
                                        wal_kv_log,
                                    );
                                },
                            ) {
                                results.push(oom);
                                continue;
                            }
                        }
                        cow_intercept(snapshot_state, guard, db_idx, cmd_frame);
                    }

                    let mut selected = db_idx;
                    let result = cmd_dispatch(guard, cmd, args, &mut selected, db_count);
                    let frame = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    };

                    let mut aof_ok = true;
                    if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                        // Skip the serialization alloc when the fanout would
                        // no-op (persistence + replication all off) — it was
                        // pure waste on every cross-shard write.
                        if wal_fanout_has_work(wal_writer, replica_txs, aof_pool, wal_kv_log) {
                            let serialized = aof::serialize_command(cmd_frame);
                            aof_ok = wal_append_and_fanout(
                                &serialized,
                                db_idx,
                                wal_writer,
                                repl_backlog,
                                replica_txs,
                                repl_state,
                                shard_id,
                                aof_pool, // FIX-W1-2
                                wal_kv_log,
                                &mut aof_budget,
                            );
                        }

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
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                    crate::blocking::wakeup::try_wake_zset_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else {
                                    crate::blocking::wakeup::try_wake_stream_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                }
                            }
                        }
                    }

                    results.push(if aof_ok {
                        frame
                    } else {
                        crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            AOF_APPEND_LOST_ERR,
                        ))
                    });
                }
            });
            // Arc-owned slot: deref is safe, refcount keeps it alive.
            let slot = &*response_slot.0;
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
            // write_db and text_store (HSET auto-index) in one with_shard closure.
            crate::shard::slice::with_shard(|s| {
                // One-time refresh via a scoped temporary borrow — `guard`
                // itself moves INSIDE the loop below (Gap A) so the MOVE/
                // COPY-DB branch can borrow `&mut s.databases` (both src and
                // dst) for the same command.
                s.databases[db_idx].refresh_now_from_cache(cached_clock);
                // ONE backpressure budget for the whole batch: under sustained
                // AOF backpressure the shard thread stalls at most BOUND total,
                // not BOUND × batch-len (review finding, PR #211).
                let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
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

                    // Gap A: MOVE / COPY-DB two-db intercept, mirroring the
                    // plain Execute arm — needs two &mut Database borrows at
                    // once. Returns BEFORE the auto-index/wake hooks below
                    // too, mirroring the Execute arm's pre-existing behavior
                    // (not new for this fix — see the Gap A commit body).
                    if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
                        if let Some(response) = crate::shard::spsc_two_db::try_two_db_intercept(
                            cmd,
                            args,
                            &mut s.databases,
                            db_idx,
                            db_count,
                            cached_clock,
                            evict_active,
                            shard_databases,
                            shard_id,
                            runtime_config,
                            spill_sender,
                            spill_file_id,
                            disk_offload_dir,
                        ) {
                            let mut aof_ok = true;
                            if matches!(response, crate::protocol::Frame::Integer(1))
                                && wal_fanout_has_work(
                                    wal_writer,
                                    replica_txs,
                                    aof_pool,
                                    wal_kv_log,
                                )
                            {
                                let serialized = aof::serialize_command(cmd_frame);
                                aof_ok = wal_append_and_fanout(
                                    &serialized,
                                    db_idx,
                                    wal_writer,
                                    repl_backlog,
                                    replica_txs,
                                    repl_state,
                                    shard_id,
                                    aof_pool, // FIX-C4-FOLD
                                    wal_kv_log,
                                    &mut aof_budget,
                                );
                            }
                            results.push(if aof_ok {
                                response
                            } else {
                                crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                    AOF_APPEND_LOST_ERR,
                                ))
                            });
                            continue;
                        }
                        // COPY with no DB clause or same-db: fall through to
                        // the generic single-db write path below.
                    }

                    let guard = &mut s.databases[db_idx];
                    let is_write = metadata::is_write(cmd);
                    if is_write {
                        // M2 fix: same gate as the Execute arm, applied per
                        // command in this batch's shared `guard` borrow.
                        if evict_active {
                            if let Err(oom) = spsc_eviction_gate(
                                guard,
                                db_idx,
                                shard_databases,
                                shard_id,
                                runtime_config,
                                spill_sender,
                                spill_file_id,
                                disk_offload_dir,
                                // task #34 (Wave A): cross-shard write leg.
                                &mut |key| {
                                    crate::replication::reason_del::record_reason_del(
                                        key,
                                        db_idx,
                                        wal_writer,
                                        repl_backlog,
                                        replica_txs,
                                        repl_state,
                                        shard_id,
                                        aof_pool,
                                        wal_kv_log,
                                    );
                                },
                            ) {
                                results.push(oom);
                                continue;
                            }
                        }
                        cow_intercept(snapshot_state, guard, db_idx, cmd_frame);
                    }

                    let mut selected = db_idx;
                    let result = cmd_dispatch(guard, cmd, args, &mut selected, db_count);
                    let frame = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    };

                    let mut aof_ok = true;
                    if is_write && !matches!(frame, crate::protocol::Frame::Error(_)) {
                        // See `wal_fanout_has_work` — skip the serialization alloc
                        // entirely when the fanout would no-op (persistence off).
                        if wal_fanout_has_work(wal_writer, replica_txs, aof_pool, wal_kv_log) {
                            let serialized = aof::serialize_command(cmd_frame);
                            aof_ok = wal_append_and_fanout(
                                &serialized,
                                db_idx,
                                wal_writer,
                                repl_backlog,
                                replica_txs,
                                repl_state,
                                shard_id,
                                // C4-FOLD-FIX: AOF append MUST happen here, before the
                                // response_slot is filled, so the append is already in the
                                // AOF channel when AofFold reads sender.len(). Deferring to
                                // the connection handler (after slot.fill wakes the handler
                                // task) means the append arrives AFTER drain_spsc_shared
                                // returns and AFTER AofFold's sender.len() snapshot, so
                                // pending_aof_count undercounts by ≥1 → that append escapes
                                // into the NEW incr → double-apply on restart (+1 observed
                                // in test_ssm4a_fold_4shard_experimental). The handler's
                                // cross-shard AOF write (handler_sharded/mod.rs) is removed
                                // to avoid the double-write this None guard was preventing.
                                aof_pool, // FIX-C4-FOLD
                                wal_kv_log,
                                &mut aof_budget,
                            );
                        }
                    }

                    // Auto-index: if HSET succeeded, check for vector index match.
                    // vector_store and text_store in same with_shard closure.
                    if cmd.eq_ignore_ascii_case(b"HSET")
                        && !matches!(frame, crate::protocol::Frame::Error(_))
                    {
                        if let Some(crate::protocol::Frame::BulkString(key_bytes)) = args.first() {
                            // Plan 166-01: Vec<(idx, key_hash)> return discarded
                            // here; Plan 166-02 threads it into CrossStoreTxn.
                            let _ = auto_index_hset(
                                &mut s.vector_store,
                                &mut s.text_store,
                                key_bytes,
                                args,
                                0,
                                db_idx as u8,
                            );
                        }
                    }

                    // Auto-delete vectors on DEL/UNLINK (parity with the HSET
                    // hook above and the Execute arm's auto-delete).
                    if !matches!(frame, crate::protocol::Frame::Error(_))
                        && (cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK"))
                    {
                        auto_delete_vectors(&mut s.vector_store, args, db_idx as u8);
                    }

                    // R4: HDEL of an indexed vector field tombstones the vector.
                    if !matches!(frame, crate::protocol::Frame::Error(_))
                        && cmd.eq_ignore_ascii_case(b"HDEL")
                    {
                        auto_hdel_vectors(&mut s.vector_store, args, db_idx as u8);
                    }

                    // R3: FLUSHALL/FLUSHDB clears index contents (definitions kept).
                    // WS5a: FLUSHDB scopes to `db_idx`; FLUSHALL clears every db.
                    if !matches!(frame, crate::protocol::Frame::Error(_))
                        && (cmd.eq_ignore_ascii_case(b"FLUSHDB")
                            || cmd.eq_ignore_ascii_case(b"FLUSHALL"))
                    {
                        auto_flush_indexes(
                            &mut s.vector_store,
                            &mut s.text_store,
                            cmd.eq_ignore_ascii_case(b"FLUSHDB"),
                            db_idx as u8,
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
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else if cmd.eq_ignore_ascii_case(b"ZADD") {
                                    crate::blocking::wakeup::try_wake_zset_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                } else {
                                    crate::blocking::wakeup::try_wake_stream_waiter(
                                        &mut reg, guard, db_idx, &key,
                                    );
                                }
                            }
                        }
                    }

                    results.push(if aof_ok {
                        frame
                    } else {
                        crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            AOF_APPEND_LOST_ERR,
                        ))
                    });
                }
            });
            // Arc-owned slot: deref is safe, refcount keeps it alive.
            let slot = &*response_slot.0;
            slot.fill(results);
        }
        ShardMessage::PubSubPublish(payload) => {
            let count =
                crate::pubsub::publish_shared(pubsub_registry, &payload.channel, &payload.message);
            payload.slot.add(count);
        }
        ShardMessage::PubSubPublishBatch { pairs, slot } => {
            let mut batch_total: i64 = 0;
            for (i, (channel, message)) in pairs.iter().enumerate() {
                let count = crate::pubsub::publish_shared(pubsub_registry, channel, message);
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
            crate::shard::slice::with_shard_db(db_index, |guard| {
                if guard.exists(&key) {
                    crate::blocking::wakeup::try_wake_list_waiter(&mut reg, guard, db_index, &key);
                    crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, guard, db_index, &key);
                    crate::blocking::wakeup::try_wake_stream_waiter(
                        &mut reg, guard, db_index, &key,
                    );
                }
            });
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
            let keys = {
                crate::shard::slice::with_shard_db(db_index, |db| {
                    crate::cluster::migration::handle_get_keys_in_slot(
                        std::slice::from_ref(db),
                        0,
                        slot,
                        count,
                    )
                })
            };
            let _ = reply_tx.send(keys);
        }
        ShardMessage::KeyspaceStats { reply_tx } => {
            // Per-db (keys, expires) for INFO # Keyspace. O(#dbs) counter
            // reads — no key iteration.
            let stats: Vec<(u64, u64)> = crate::shard::slice::with_shard(|s| {
                s.databases
                    .iter()
                    .map(|db| (db.len() as u64, db.expires_count() as u64))
                    .collect()
            });
            let _ = reply_tx.send(stats);
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
                db_index,
            } = *payload;
            // Phase 171 SCAT-01: honor coordinator-resolved AS_OF / TXN LSN
            // for multi-shard FT.SEARCH. When `as_of_lsn == 0` the filter is a
            // no-op and behavior matches the pre-171 path. Route through
            // `search_local_filtered` with AS_OF threaded in to apply MVCC
            // filtering against the committed treemap inside `search_local_raw`.
            // Flat with_shard borrow — no outer borrow active, no re-entrancy.
            // WS5a: db_index forwarded from the originating connection.
            let response = crate::shard::slice::with_shard(|s| {
                vector_search::search_local_filtered(
                    &mut s.vector_store,
                    &index_name,
                    &query_blob,
                    k,
                    None,
                    0,
                    usize::MAX,
                    None,
                    as_of_lsn,
                    db_index,
                )
            });
            let _ = reply_tx.send(response);
        }
        ShardMessage::DocFreq(payload) => {
            let crate::shard::dispatch::DocFreqPayload {
                index_name,
                field_queries,
                reply_tx,
                db_index,
            } = *payload;
            // DFS Phase 1: collect per-term df + total N from this shard's TextIndex.
            // Returns crate::protocol::Frame::Array with interleaved [term, df, ..., "N", n] per field_query.
            // WS5a: db-scoped — an index owned by a different db is invisible.
            let response = {
                crate::shard::slice::with_shard(|s| {
                    match s.text_store.get_index_for_db(&index_name, db_index) {
                        Some(text_index) => {
                            let mut items: Vec<crate::protocol::Frame> = Vec::new();
                            for (field_idx_opt, terms) in &field_queries {
                                let fidx = field_idx_opt.unwrap_or(0);
                                let (term_dfs, n) = text_index.doc_freq_for_terms(fidx, terms);
                                for (term, df) in term_dfs {
                                    items.push(crate::protocol::Frame::BulkString(
                                        bytes::Bytes::from(term),
                                    ));
                                    items.push(crate::protocol::Frame::Integer(i64::from(df)));
                                }
                                items.push(crate::protocol::Frame::BulkString(
                                    bytes::Bytes::from_static(b"N"),
                                ));
                                items.push(crate::protocol::Frame::Integer(i64::from(n)));
                            }
                            crate::protocol::Frame::Array(items.into())
                        }
                        None => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            b"ERR unknown index",
                        )),
                    }
                })
            };
            let _ = reply_tx.send(response);
        }
        ShardMessage::TextSearch(payload) => {
            // Non-text-index build: only reply_tx is needed (the BM25 path is feature-gated, so the
            // other payload fields would be unused). `..` ignores them.
            #[cfg(not(feature = "text-index"))]
            {
                let crate::shard::dispatch::TextSearchPayload { reply_tx, .. } = *payload;
                let _ = reply_tx.send(crate::protocol::Frame::Error(bytes::Bytes::from_static(
                    b"ERR text-index feature not enabled",
                )));
            }
            #[cfg(feature = "text-index")]
            {
                let crate::shard::dispatch::TextSearchPayload {
                    index_name,
                    query,
                    global_df,
                    global_n,
                    top_k,
                    offset,
                    count,
                    highlight_opts,
                    summarize_opts,
                    reply_tx,
                    db_index,
                } = *payload;
                // DFS Phase 2: execute BM25 text search with global IDF injected by coordinator.
                // The raw query bytes are re-parsed here with the recursive-descent parser so the
                // full AST (OR, multi-@clause, grouping) is evaluated correctly on this shard.
                // After scoring, apply HIGHLIGHT/SUMMARIZE post-processing if requested. Each shard
                // post-processes against its own local hash store (no cross-shard read, no .await —
                // safe to hold guards). text_store + databases[db_index] in one with_shard
                // (multi-resource). WS5a: db-scoped — an index owned by a different db is invisible.
                let response = {
                    crate::shard::slice::with_shard(|s| {
                        match s.text_store.get_index_for_db(&index_name, db_index) {
                            Some(text_index) => {
                                let mut result =
                                crate::command::vector_search::ft_text_search::run_text_query_on_index(
                                    text_index,
                                    &query,
                                    Some(&global_df),
                                    Some(global_n),
                                    top_k,
                                    offset,
                                    count,
                                );
                                if highlight_opts.is_some() || summarize_opts.is_some() {
                                    // Re-parse to extract highlight terms for post-processing.
                                    let term_strings = crate::text::query::parse_query(
                                        &query,
                                        &crate::text::query::QuerySchema::from_index(text_index),
                                    )
                                    .map(|n| {
                                        crate::text::query::collect_highlight_terms(&n, text_index)
                                    })
                                    .unwrap_or_default();
                                    let db = s
                                        .databases
                                        .get(db_index as usize)
                                        .unwrap_or(&s.databases[0]);
                                    crate::command::vector_search::ft_text_search::apply_post_processing(
                                    &mut result,
                                    &term_strings,
                                    text_index,
                                    db,
                                    highlight_opts.as_ref(),
                                    summarize_opts.as_ref(),
                                );
                                }
                                result
                            }
                            None => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                                b"ERR unknown index",
                            )),
                        }
                    })
                };
                let _ = reply_tx.send(response);
            }
        }
        ShardMessage::VectorCommand {
            command,
            reply_tx,
            db_index,
        } => {
            // All slice fields (vector_store, text_store, graph_store, databases)
            // acquired in one flat with_shard closure — no outer borrow active.
            let response = {
                crate::shard::slice::with_shard(|s| {
                    let cmd_bytes = extract_command_static(&command).map(|(c, _)| c);
                    let is_dropindex = cmd_bytes
                        .map(|c| c.eq_ignore_ascii_case(b"FT.DROPINDEX"))
                        .unwrap_or(false);
                    let has_session = has_session_keyword(&command);
                    // WS5a: db_index comes from the originating connection
                    // (threaded through the message), not hardcoded db 0.
                    let db_opt: Option<&mut crate::storage::db::Database> =
                        if has_session || is_dropindex {
                            s.databases.get_mut(db_index as usize)
                        } else {
                            None
                        };
                    dispatch_vector_command(
                        &mut s.vector_store,
                        &mut s.text_store,
                        #[cfg(feature = "graph")]
                        Some(&s.graph_store),
                        &command,
                        db_opt,
                        db_index,
                    )
                })
            };
            let _ = reply_tx.send(response);
        }
        #[cfg(feature = "graph")]
        ShardMessage::GraphCommand { command, reply_tx } => {
            // GraphCommand is dispatched via connection handlers using ShardDatabases,
            // not through SPSC. If we receive one here, dispatch it locally.
            let (response, wal_records) = {
                crate::shard::slice::with_shard(|s| {
                    let resp =
                        crate::command::graph::dispatch_graph_command(&mut s.graph_store, &command);
                    let records = s.graph_store.drain_wal();
                    (resp, records)
                })
            };
            for record in wal_records {
                shard_databases.wal_append(shard_id, bytes::Bytes::from(record));
            }
            let _ = reply_tx.send(response);
        }
        #[cfg(feature = "graph")]
        ShardMessage::GraphRollback(payload) => {
            let crate::shard::dispatch::GraphRollbackPayload {
                txn_id,
                graph_undo,
                graph_intents,
                reply_tx,
            } = *payload;
            // Multi-shard TXN.ABORT leg: this shard owns the graphs named in
            // these ops. Apply the rollback on the local store and append the
            // drained WAL records here so replay sees them on the owning shard.
            let wal_records = {
                crate::shard::slice::with_shard(|s| {
                    crate::transaction::abort::apply_graph_rollback(
                        &mut s.graph_store,
                        txn_id,
                        &graph_undo,
                        &graph_intents,
                    )
                })
            };
            for record in wal_records {
                shard_databases.wal_append(shard_id, bytes::Bytes::from(record));
            }
            let _ = reply_tx.send(crate::protocol::Frame::SimpleString(
                bytes::Bytes::from_static(b"OK"),
            ));
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
                db_index,
            } = *payload;
            // WS5a: db-scoped — an index owned by a different db is invisible.
            let response = {
                crate::shard::slice::with_shard(|s| {
                    match s.text_store.get_index_for_db(&index_name, db_index) {
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
                            crate::command::vector_search::ft_text_search::build_text_response(
                                &results, offset, count,
                            )
                        }
                        None => crate::protocol::Frame::Error(bytes::Bytes::from_static(
                            b"ERR no such index",
                        )),
                    }
                })
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
                db_index,
            } = *payload;
            // text_store and read_db(db_index) accessed in one with_shard closure
            // (multi-resource). WS5a: db-scoped — an index owned by a different
            // db is invisible; falls back to db 0 defensively if `db_index`
            // somehow exceeds the configured count (constructor-guaranteed).
            let response = {
                crate::shard::slice::with_shard(|s| {
                    let db = s
                        .databases
                        .get(db_index as usize)
                        .unwrap_or(&s.databases[0]);
                    crate::command::vector_search::ft_aggregate::execute_local_partial(
                        &s.text_store,
                        &index_name,
                        &query,
                        &pipeline,
                        db,
                        db_index,
                    )
                })
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
                filter,
                reply_tx,
                db_index,
            } = *payload;
            let response = {
                crate::shard::slice::with_shard(|s| {
                    let sparse_pair = match (sparse_field.as_ref(), sparse_blob.as_ref()) {
                        (Some(f), Some(b)) => Some((f, b)),
                        _ => None,
                    };
                    // Phase 171 HYB-02 / SCAT-02: forward the coordinator-resolved
                    // AS_OF LSN into the raw-streams executor so the dense branch
                    // applies MVCC filtering consistently across shards.
                    // CHANGE F: forward the filter for per-shard pre-fusion filtering.
                    // WS5a: db-scoped — an index owned by a different db is invisible.
                    crate::command::vector_search::hybrid_multi::execute_hybrid_search_local_raw_streams(
                        &mut s.vector_store,
                        &s.text_store,
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
                        filter.as_ref(),
                        db_index,
                    )
                })
            };
            let _ = reply_tx.send(response);
        }
        ShardMessage::SwapDb { a, b, reply_tx } => {
            // WAL-before-swap: emit the SWAPDB record so that crash-recovery
            // replay can re-apply the swap in the correct order.  The record
            // is written even when wal_writer/wal_writer are None (the
            // fast-path in wal_append_and_fanout will skip it cheaply).
            //
            // Serialise "SWAPDB <a> <b>" without heap allocation on the number
            // formatting (itoa writes into a stack buffer).
            let mut a_buf = itoa::Buffer::new();
            let mut b_buf = itoa::Buffer::new();
            let a_str = a_buf.format(a);
            let b_str = b_buf.format(b);
            let wal_frame = crate::protocol::Frame::Array(crate::framevec![
                crate::protocol::Frame::BulkString(bytes::Bytes::from_static(b"SWAPDB")),
                crate::protocol::Frame::BulkString(bytes::Bytes::copy_from_slice(a_str.as_bytes())),
                crate::protocol::Frame::BulkString(bytes::Bytes::copy_from_slice(b_str.as_bytes())),
            ]);
            let serialized = aof::serialize_command(&wal_frame);
            // No per-client response frame exists here (coordinator broadcast,
            // reply is `()`): an AOF-append loss is already counted +
            // error!-logged inside the pool, so the result is discarded.
            let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
            let _ = wal_append_and_fanout(
                &serialized,
                // task #35: SWAPDB affects both `a` and `b` — no single db
                // context applies; pass 0 (writer may emit a harmless
                // redundant SELECT 0 if last_db was already non-zero).
                0,
                wal_writer,
                repl_backlog,
                replica_txs,
                repl_state,
                shard_id,
                aof_pool, // FIX-W1-2
                wal_kv_log,
                &mut aof_budget,
            );

            // Perform the in-place swap via ShardSlice (thread-local, no locks needed).
            crate::shard::slice::with_shard(|s| {
                if a != b {
                    s.databases.swap(a, b);
                }
            });

            // Notify the coordinator that this shard completed its swap.
            let _ = reply_tx.send(());
        }
        // ── C2: shardslice-migration Wave A1 ─────────────────────────────────
        //
        // These four arms are the owner-side execution legs for the new
        // ShardMessage variants defined in §C2 of the frozen contract.
        // They run AFTER init_shard is wired (Wave B) — until then the
        // variants are never sent, so these arms are dead code at runtime
        // today. These use ShardSlice directly:
        // the owning shard's slice is the authoritative state once slice mode
        // is live.
        ShardMessage::MqCommand(payload) => {
            // MQ domain hop: execute the full MQ.* subcommand arm against the
            // owner's ShardSlice. All six subcommands (CREATE/PUSH/POP/ACK/
            // DLQLEN/TRIGGER) are dispatched through `mq_exec::execute_mq_on_owner`,
            // which takes the three data fields directly and returns a Frame.
            // Destructure first so reply_tx stays here for the send.
            let crate::shard::dispatch::MqCommandPayload {
                db_index,
                key_prefix,
                command,
                reply_tx,
            } = *payload;
            let response =
                crate::shard::mq_exec::execute_mq_on_owner(db_index, key_prefix, command);
            // Ignore send failure: receiver dropped means the client disconnected.
            let _ = reply_tx.send(response);
        }
        ShardMessage::MqTxnMaterialize {
            db_index,
            intents,
            reply_tx,
        } => {
            // TXN.COMMIT MQ-intent materialize: fold deferred MQ.PUBLISH messages
            // onto the owner shard. Mirrors txn.rs:160–167 exactly:
            //   for intent in intents: get_stream_mut → durable-check → add.
            crate::shard::slice::with_shard_db(db_index, |db| {
                for intent in &intents {
                    if let Ok(Some(stream)) = db.get_stream_mut(&intent.queue_key) {
                        if stream.durable {
                            let msg_id = stream.next_auto_id();
                            stream.add(msg_id, intent.fields.clone());
                        }
                    }
                }
            });
            // Ignore send failure: receiver dropped means the TXN coordinator
            // has already given up (e.g. client disconnect mid-commit).
            let _ = reply_tx.send(());
        }
        ShardMessage::WsDropCleanup { prefix, reply_tx } => {
            // WS.DROP best-effort key cleanup. The connection handler routes
            // this to the shard that owns `prefix` (hash-tag co-location).
            //
            // Sweeps EVERY logical db on this shard, not just db 0: a
            // workspace-bound connection can `SELECT` to any db before
            // writing (WS AUTH and SELECT are orthogonal — the workspace
            // hash-tag prefix composes with whichever db is currently
            // selected), so workspace keys can legitimately live in db != 0.
            // A db-0-only sweep (the original implementation) silently
            // leaked those keys forever after WS DROP — found during the
            // WS5b hardening sweep (docs/guides/isolation.md).
            //
            // Cost note: this is a synchronous, in-place O(total keys ×
            // --databases) full scan on THIS shard's event-loop thread — no
            // yield points, so it blocks every other connection pinned to
            // this shard for the duration. Accepted trade-off for an
            // admin-rare operation (create/drop a tenant); see
            // docs/guides/isolation.md's "WS DROP" cost-note for the
            // large-keyspace / large---databases caveat.
            let deleted_count = crate::shard::slice::with_shard(|s| {
                let mut total = 0u64;
                for db in s.databases.iter_mut() {
                    let keys_to_delete: Vec<Vec<u8>> = db
                        .keys()
                        .filter(|k| k.as_bytes().starts_with(prefix.as_ref()))
                        .map(|k| k.as_bytes().to_vec())
                        .collect();
                    total += keys_to_delete.len() as u64;
                    for key in &keys_to_delete {
                        db.remove(key);
                    }
                }
                total
            });
            // Ignore send failure: caller logs the count but the drop already
            // completed; losing the ack is harmless.
            let _ = reply_tx.send(deleted_count);
        }
        ShardMessage::AofFold { reply_tx } => {
            // AOF cooperative-snapshot (C4): build an expired-filtered snapshot
            // of ALL databases on this shard and reply. The AOF rewrite writer
            // blocks on the oneshot; the shard processes this between commands,
            // providing the equivalent mutual-exclusion that the old RwLock write
            // guards gave (single-threaded event loop = no concurrent mutations).
            //
            // C4-DRAIN-BOUND: Before building the snapshot, record how many
            // messages are currently pending in this shard's AOF channel. Since
            // the shard event loop is single-threaded, no new commands execute
            // between this read and the reply being sent. Therefore ALL of these
            // pending messages are pre-snapshot appends. The AOF writer uses this
            // count as the phase-3 mid-drain bound, preventing an infinite drain
            // loop under sustained high write load where the channel never empties.
            let pending_aof_count = aof_pool.map(|p| p.sender(shard_id).len()).unwrap_or(0);
            let now_ms = crate::storage::entry::current_time_ms();
            let snapshot = crate::shard::slice::with_shard(|s| {
                let mut dbs = Vec::with_capacity(s.databases.len());
                for db in s.databases.iter() {
                    let base_ts = db.base_timestamp();
                    let mut entries = Vec::new();
                    for (key, entry) in db.data().iter() {
                        if !entry.is_expired_at(base_ts, now_ms) {
                            entries.push((key.clone(), entry.clone()));
                        }
                    }
                    dbs.push((entries, base_ts));
                }
                crate::shard::dispatch::AofFoldSnapshot {
                    dbs,
                    pending_aof_count,
                }
            });
            // Ignore send failure: the AOF writer dropped its receiver
            // (e.g. rewrite aborted) — the snapshot is simply discarded.
            let _ = reply_tx.send(snapshot);
        }

        // ─────────────────────────────────────────────────────────────────────
        ShardMessage::TxnExecute(payload) => {
            // Sharded MULTI/EXEC Phase B: the originating connection proved via
            // `analyze_txn_locality` that every key is owned by THIS shard (its
            // own accept shard differs), so run the whole body on our slice and
            // persist each write to OUR AOF/WAL. `execute_transaction_sharded`
            // manages its own `with_shard`/`with_shard_db` visits, so it must be
            // called at the top of this arm (never nested in a slice borrow).
            let crate::shard::dispatch::TxnExecutePayload {
                db_index,
                commands,
                reply_tx,
            } = *payload;
            let mut exec_publishes: Vec<(usize, bytes::Bytes, bytes::Bytes)> = Vec::new();
            let (result, aof_entries) = crate::server::conn::shared::execute_transaction_sharded(
                shard_databases,
                shard_id,
                &commands,
                db_index,
                cached_clock,
                &mut exec_publishes,
            );
            // Persist via the SAME sync path as normal cross-shard writes (the
            // MultiExecute arm): fire-and-forget append + WAL/replica fan-out,
            // ONE shared backpressure budget for the whole body so a stall costs
            // at most AOF_SPSC_BACKPRESSURE_BOUND, not that per command. The
            // always-mode fsync barrier is issued by the ORIGINATOR after the
            // reply (mirrors the H1-BARRIER for normal cross-shard writes).
            let mut wrote = false;
            let mut append_lost = false;
            let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
            // PR #282 review: each entry carries the db it EXECUTED in (a
            // SELECT queued inside the body redirects the commands after it)
            // — attribute per entry, not the body's entry db.
            for (entry_db, entry_bytes) in &aof_entries {
                wrote = true;
                let ok = wal_append_and_fanout(
                    entry_bytes,
                    *entry_db,
                    wal_writer,
                    repl_backlog,
                    replica_txs,
                    repl_state,
                    shard_id,
                    aof_pool,
                    wal_kv_log,
                    &mut aof_budget,
                );
                if !ok {
                    append_lost = true;
                }
            }
            let _ = reply_tx.send(crate::shard::dispatch::TxnExecReply {
                result,
                exec_publishes,
                wrote,
                append_lost,
            });
        }

        // ─────────────────────────────────────────────────────────────────────
        ShardMessage::Shutdown => {
            info!("Received shutdown via SPSC");
        }
        ShardMessage::RegisterReplica(payload) => {
            let crate::shard::dispatch::RegisterReplicaPayload {
                replica_id,
                tx,
                kicked,
                backlog_capacity,
                registered,
                push_offset,
                cut,
            } = *payload;
            // Lazy-init replication backlog on first replica registration (saves 1MB/shard).
            // The backlog is shared with PSYNC handlers via Arc<Mutex<Option<...>>> on
            // ReplicationState — see ReplicationState::ensure_backlogs_allocated for the
            // earlier allocation point triggered by REPLCONF. Capacity is carried
            // in the message (from `--repl-backlog-size`) so this fallback can't
            // silently diverge from the handshake-path allocation.
            crate::replication::state::mark_fanout_active();
            let mut guard = repl_backlog.lock();
            if guard.is_none() {
                // Seed byte positions at the current shard offset so range
                // math stays aligned with pre-attach `issue_lsn` advances —
                // see `ReplicationBacklog::new_at`.
                let offset = repl_state
                    .as_ref()
                    .map(|h| h.shard_offset(shard_id))
                    .unwrap_or(0);
                *guard = Some(ReplicationBacklog::new_at(backlog_capacity, offset));
            }
            drop(guard);
            // Exactly-once cut (per-shard axis): pusher-captured when the
            // registration rode with an inline snapshot capture, else the
            // shard offset now — every record already fanned out (or whose
            // fan-out message precedes this registration in the FIFO) is at
            // or below it, so the filtered delivery can't double-send.
            let entry_cut = cut.unwrap_or_else(|| {
                repl_state
                    .as_ref()
                    .map(|h| h.shard_offset(shard_id))
                    .unwrap_or(0)
            });
            replica_txs.push(crate::shard::dispatch::ReplicaFanout {
                replica_id,
                tx,
                kicked,
                cut: entry_cut,
            });
            // Reply with the offset at which live fan-out begins. For
            // same-thread self-queue registrations this is `push_offset`,
            // captured AT PUSH TIME: local writes advance the offset
            // synchronously at write time (`record_local_write`), so a write
            // W that lands between the registration's push and this drain has
            // already advanced the counter — an offset read HERE would cover
            // W in the catch-up range while W's `ReplicaLiveFanout` message
            // (queued behind this registration, drained after it) ALSO
            // delivers it live: double-applied. With the push-time offset the
            // ledger is exact either way W interleaves: W before the push →
            // below `reg_offset`, delivered via catch-up, its fan-out message
            // drains before `tx` is registered (no live copy); W after the
            // push → at/above `reg_offset`, excluded from catch-up, delivered
            // live. Cross-shard legacy registrations (`None`, R2 redesign)
            // keep the drain-time read.
            if let Some(reg_tx) = registered {
                let offset = push_offset.unwrap_or_else(|| {
                    repl_state
                        .as_ref()
                        .map(|h| h.shard_offset(shard_id))
                        .unwrap_or(0)
                });
                let _ = reg_tx.send(offset);
            }
        }
        ShardMessage::UnregisterReplica { replica_id } => {
            replica_txs.retain(|r| r.replica_id != replica_id);
        }
        ShardMessage::PrepareReplicaSync(payload) => {
            // R2 (task #20): this shard's leg of a multi-shard full resync.
            // The ENTIRE leg — RDB body serialization, offset capture, live
            // fan-out registration — runs in this one synchronous stretch on
            // the shard's own thread, so no mutation can slip between "inside
            // the snapshot" and "delivered live" (the same atomicity argument
            // as `handle_psync_inline_single_shard`, applied per shard).
            //
            // This additionally leans on the self-queue-FIRST drain order
            // (see `drain_spsc_shared`): a local write visible to this body
            // capture pushed its `ReplicaLiveFanout` BEFORE this arm could
            // drain, and the self queue drains first — so that fan-out
            // message no-ops against the not-yet-registered replica instead
            // of double-delivering a record that is already in the body.
            let crate::shard::dispatch::PrepareReplicaSyncPayload {
                replica_id,
                tx,
                kicked,
                backlog_capacity,
                reply_tx,
            } = *payload;
            crate::replication::state::mark_fanout_active();
            // Lazy backlog init (offset accounting parity with RegisterReplica;
            // the backlog itself is not replayed on this path — multi-shard
            // PSYNC always answers with a full resync).
            {
                let mut guard = repl_backlog.lock();
                if guard.is_none() {
                    let offset = repl_state
                        .as_ref()
                        .map(|h| h.shard_offset(shard_id))
                        .unwrap_or(0);
                    *guard = Some(ReplicationBacklog::new_at(backlog_capacity, offset));
                }
            }
            let started = std::time::Instant::now();
            let mut rdb_body: Vec<u8> = Vec::new();
            let mut vector_defs: Option<Vec<u8>> = None;
            let mut text_defs: Option<Vec<u8>> = None;
            #[cfg(feature = "graph")]
            let mut graph_blob: Vec<u8> = Vec::new();
            crate::shard::slice::with_shard(|s| {
                let refs: Vec<&crate::storage::Database> = s.databases.iter().collect();
                crate::persistence::redis_rdb::write_rdb_body_refs(&refs, &mut rdb_body);
                // Index DEFINITIONS ride as moon aux (same as the single-shard
                // path); contents stay in sync via the live stream + backfill.
                let pairs = s.vector_store.collect_index_metas_with_weights();
                if !pairs.is_empty() {
                    vector_defs = Some(crate::vector::index_persist::serialize_index_metas_v5(
                        &pairs,
                    ));
                }
                let metas = s.text_store.collect_index_metas();
                if !metas.is_empty() {
                    text_defs = Some(crate::text::index_persist::serialize_text_index_metas(
                        &metas,
                    ));
                }
                #[cfg(feature = "graph")]
                {
                    graph_blob =
                        crate::replication::graph_sync::export_graph_store(&mut s.graph_store);
                }
            });
            let shard_offset = repl_state
                .as_ref()
                .map(|h| h.shard_offset(shard_id))
                .unwrap_or(0);
            // `cut = shard_offset` is the exactly-once line: every mutation
            // in the body captured above has already advanced the counter to
            // at most this value, so its (possibly still-queued) fan-out
            // message is filtered out at delivery; anything applied later
            // carries a higher end_offset and is delivered live exactly once.
            replica_txs.push(crate::shard::dispatch::ReplicaFanout {
                replica_id,
                tx,
                kicked,
                cut: shard_offset,
            });
            tracing::debug!(
                shard_id,
                replica_id,
                body_bytes = rdb_body.len(),
                shard_offset,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "prepared multi-shard replica sync leg"
            );
            let prepared = crate::shard::dispatch::PreparedShardSync {
                rdb_body,
                shard_offset,
                vector_defs,
                text_defs,
                #[cfg(feature = "graph")]
                graph_blob,
            };
            if reply_tx.try_send(prepared).is_err() {
                // The PSYNC task is gone (replica dropped mid-handshake) —
                // undo the registration so this shard doesn't fan out to a
                // channel nobody drains.
                replica_txs.retain(|r| r.replica_id != replica_id);
                tracing::warn!(
                    shard_id,
                    replica_id,
                    "PrepareReplicaSync reply dropped — replica disconnected before sync"
                );
            }
        }
        ShardMessage::ReplicaLiveFanout { bytes, end_offset } => {
            // Live-delivery leg ONLY: backlog append + offset advance already
            // happened synchronously at the write's own execution point on
            // this same thread (`record_local_write` for handler-local
            // writes, `wal_append_and_fanout` for SPSC-dispatched ones) —
            // doing either again here would double-count. ALL live replica
            // sends flow through this single arm so the wire order per shard
            // equals the self-queue FIFO order equals the offset order — a
            // direct send from the execute path would let a LATER-offset
            // cross-shard write overtake an earlier local write's queued
            // fan-out message, reordering same-key writes on the replica.
            // A replica whose channel is FULL is KICKED (task #35): skipping
            // the record would silently and permanently diverge it.
            fanout_send_or_kick(replica_txs, &bytes, end_offset);
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
    db_index: u8,
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
        vector_search::ft_create(vector_store, text_store, args, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.SEARCH") {
        // Check if this is a text query (no KNN/SPARSE markers) before
        // dispatching to the vector search path. Text queries are handled
        // by ft_text_search which reads from the TextStore (BM25 posting index).
        let query_bytes = args.get(1).and_then(|f| match f {
            crate::protocol::Frame::BulkString(b) => Some(b.as_ref()),
            _ => None,
        });
        if query_bytes.map_or(false, vector_search::is_text_query)
            && !vector_search::has_sparse_clause(args)
        {
            return vector_search::ft_text_search(text_store, args, db_index);
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
                db_index,
            )
        }
        #[cfg(not(feature = "graph"))]
        {
            vector_search::ft_search(vector_store, args, db, Some(text_store), 0, db_index)
        }
    } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
        vector_search::ft_dropindex(vector_store, text_store, db, args, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.INFO") {
        vector_search::ft_info(vector_store, text_store, args, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT._LIST") {
        vector_search::ft_list(vector_store, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.COMPACT") {
        vector_search::ft_compact(vector_store, text_store, args, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.AGGREGATE") {
        // FT.AGGREGATE (Phase 152, Plan 02) — linear else-if branch per W8.
        // D-19's phf reference is superseded by the established FT.* dispatch
        // pattern in RESEARCH §ARM. FT.AGGREGATE needs Database access to
        // materialise rows from the hash store (see `needs_db` gate above).
        #[cfg(feature = "text-index")]
        {
            match db.as_deref() {
                Some(db_ref) => {
                    vector_search::ft_aggregate(vector_store, text_store, args, db_ref, db_index)
                }
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
        vector_search::ft_config(vector_store, text_store, args, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.CACHESEARCH") {
        vector_search::cache_search::ft_cachesearch(vector_store, args, db_index)
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
            vector_search::navigate::ft_navigate(vector_store, graph_store, args, db, db_index)
        }
        #[cfg(not(feature = "graph"))]
        {
            crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR FT.NAVIGATE requires graph feature",
            ))
        }
    } else if cmd.eq_ignore_ascii_case(b"FT.RECOMMEND") {
        vector_search::recommend::ft_recommend(vector_store, args, db, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.INVALIDATE_RANGE") {
        // FT.INVALIDATE_RANGE: bulk-delete by (TAG ∩ NUMERIC range); bumps text_version_token.
        // Requires text-index feature for TAG + NUMERIC bitmap indexes.
        #[cfg(feature = "text-index")]
        {
            vector_search::ft_invalidate_range(text_store, args, db_index)
        }
        #[cfg(not(feature = "text-index"))]
        {
            crate::protocol::Frame::Error(bytes::Bytes::from_static(
                b"ERR FT.INVALIDATE_RANGE requires text-index feature",
            ))
        }
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
    db_index: u8,
) -> smallvec::SmallVec<[(bytes::Bytes, u64); 4]> {
    auto_index_hset(vector_store, text_store, key, args, 0, db_index)
}

/// Tombstone auto-indexed vectors for every key argument of a successful
/// DEL/UNLINK. Wire-parity requirement: every dispatch path that runs
/// `auto_index_hset*` on HSET must run this on DEL/UNLINK, or deleted keys
/// keep matching FT.SEARCH forever (resurrection + live-set recall collapse;
/// found by the Bundle-5 soak diagnostic at shards=1).
///
/// WS5a round 4 (adversarial review): db-scoped via `mark_deleted_for_key_for_db`
/// — a DEL issued in db N must only tombstone indexes owned by db N, or a
/// foreign db's documents get silently unindexed (data-integrity leak, worse
/// than the read-path leak fixed in round 3).
pub fn auto_delete_vectors(
    vector_store: &mut VectorStore,
    args: &[crate::protocol::Frame],
    db_index: u8,
) {
    for arg in args {
        if let Some(key) = crate::server::connection::extract_bytes(arg) {
            vector_store.mark_deleted_for_key_for_db(key.as_ref(), db_index);
        }
    }
}

/// FLUSHALL/FLUSHDB parity (persistence-review R3): clear vector + text
/// index CONTENTS while keeping the FT.CREATE definitions. Wire-parity
/// requirement mirrors `auto_delete_vectors`: every dispatch path that runs
/// the HSET auto-index hook must run this on a successful FLUSH, or flushed
/// hashes stay searchable as ghost documents until restart.
///
/// WS5a (db-scoped indexes): FLUSHDB (`is_flushdb = true`) now clears ONLY
/// the contents of indexes owned by `db_index` — every other db's index
/// contents survive. FLUSHALL (`is_flushdb = false`) still clears every
/// db's contents, matching Redis semantics for a whole-keyspace flush.
///
/// NOTE: the auto-index HSET hook that FEEDS these indexes is now ALSO
/// db-scoped (WS5a round 4, adversarial review) via `auto_index_hset`'s
/// `find_matching_index_names_for_db` — a HSET issued in another db can no
/// longer repopulate an index after a scoped FLUSHDB. Previously this
/// function's own doc comment tracked this as an open gap; it is closed.
pub fn auto_flush_indexes(
    vector_store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    is_flushdb: bool,
    db_index: u8,
) {
    if is_flushdb {
        vector_store.clear_all_contents_for_db(db_index);
        text_store.clear_all_contents_for_db(db_index);
    } else {
        vector_store.clear_all_contents();
        text_store.clear_all_contents();
    }
}

/// HDEL parity (persistence-review R4): a successful `HDEL key field...`
/// that removes an index's VECTOR field must tombstone that key's vector in
/// exactly the affected indexes — previously the vector stayed searchable
/// until whole-key DEL or a re-HSET. After a successful HDEL the named
/// fields are definitively absent from the hash, so tombstoning any index
/// keyed on one of them always agrees with the hash state (fields that were
/// already absent tombstone a mapping that should not exist anyway).
///
/// Known limitations (documented follow-ups): a multi-vector-field index
/// tombstones the WHOLE document when any of its vector fields is removed
/// (a later HSET re-indexes the remainder), and TEXT/TAG/NUMERIC field
/// removal is not yet re-indexed.
///
/// WS5a round 4 (adversarial review, bonus fix — same bug class as
/// `auto_delete_vectors`): db-scoped via `find_matching_index_names_for_db`
/// so an HDEL issued in db N cannot tombstone a foreign db's vector field.
pub fn auto_hdel_vectors(
    vector_store: &mut VectorStore,
    args: &[crate::protocol::Frame],
    db_index: u8,
) {
    let Some(key) = args
        .first()
        .and_then(crate::server::connection::extract_bytes)
    else {
        return;
    };
    let matching = vector_store.find_matching_index_names_for_db(key.as_ref(), db_index);
    for idx_name in matching {
        let Some(idx) = vector_store.get_index(&idx_name) else {
            continue;
        };
        let removed_vector_field = args[1..]
            .iter()
            .filter_map(crate::server::connection::extract_bytes)
            .any(|field| {
                idx.meta.source_field.as_ref() == field.as_ref()
                    || idx
                        .meta
                        .vector_fields
                        .iter()
                        .any(|vf| vf.field_name.as_ref() == field.as_ref())
            });
        if removed_vector_field {
            vector_store.mark_deleted_for_key_in_index(&idx_name, key.as_ref());
        }
    }
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
    db_index: u8,
) -> smallvec::SmallVec<[(bytes::Bytes, u64); 4]> {
    auto_index_hset(vector_store, text_store, key, args, txn_id, db_index)
}

/// WS5a round 4 (adversarial review, CRITICAL): `db_index` scopes both the
/// vector and text `find_matching_index_names` lookups below via their
/// `_for_db` variants — an HSET issued in db N must only auto-index into
/// indexes owned by db N. Previously unscoped: any db's HSET could feed an
/// index created in ANY other db whose PREFIX happened to match the key,
/// silently cross-contaminating index contents (worse than the read-path
/// leak fixed in round 3, since it corrupts data rather than just exposing
/// it). Triggers at `--shards 1` (no multi-shard fan-out required).
fn auto_index_hset(
    vector_store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    key: &[u8],
    args: &[crate::protocol::Frame],
    txn_id: u64,
    db_index: u8,
) -> smallvec::SmallVec<[(bytes::Bytes, u64); 4]> {
    let mut inserted: smallvec::SmallVec<[(bytes::Bytes, u64); 4]> = smallvec::SmallVec::new();
    let matching_names = vector_store.find_matching_index_names_for_db(key, db_index);
    let text_matching = text_store.find_matching_index_names_for_db(key, db_index);
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
            // Insert-path compaction trigger: poll installs + dispatch a
            // background build when the mutable segment crosses its compact
            // threshold. Without this, a pure bulk load (no FT.SEARCH
            // traffic) leaves everything in the brute-force mutable tier
            // until the autovacuum backstop's 30s tick — the whole HNSW
            // build then lands on the first explicit FT.COMPACT. Cheap when
            // below threshold or a build is already in flight (two
            // non-blocking polls + a length compare, same calls the
            // FT.SEARCH path makes per query).
            idx.try_compact();
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
    let mut any_text_indexed = false;
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
            any_text_indexed = true;
        }
    }

    // Bump version tokens AFTER successful writes (monotonicity-on-success
    // contract). Vector and text bumps are independent — each engine's
    // downstream consumer checks its own token.
    if !inserted.is_empty() {
        vector_store.bump_version();
    }
    if any_text_indexed {
        text_store.bump_version();
    }

    inserted
}

/// Find the vector blob in HSET args for the given source_field.
/// Returns Some(blob) if found with correct dimension, None otherwise.
///
/// `pub(crate)` so the B3 recovery dedup rescan
/// (`crate::vector::persistence::recover_v2`) can compute the CURRENT
/// vector-field checksum through the exact same field-scan the write path
/// uses (`handle_vector_insert`) — any divergence would mean the dedup
/// decision silently never fires.
pub(crate) fn find_vector_blob<'a>(
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
    // Record original Redis key for FT.SEARCH response. Bucket-scoped COW:
    // clones only the ONE bucket if a search snapshot holds the map
    // concurrently (QP-1 + RSS/CPU wave 4).
    idx.key_hash_to_key
        .get_or_insert_with(key_hash, || bytes::Bytes::copy_from_slice(key));
    // Append to mutable segment. `insert_lsn` is the monotonic LSN allocated
    // by `auto_index_hset`; MVCC visibility (src/vector/mvcc/visibility.rs)
    // compares against query snapshot_lsn to enforce FT.SEARCH AS_OF and
    // TXN snapshot isolation. When inside a TXN (txn_id != 0), use the
    // transactional variant so non-TXN readers see the entry as uncommitted.
    let snap = idx.segments.load();
    // VEC-1: an HSET on an already-indexed key is an UPDATE — tombstone the
    // prior version BEFORE appending, or the index accumulates stale
    // duplicates (doc returned twice, num_docs inflating under churn).
    // Non-txn path only: a txn's tombstone must not leak to other readers
    // before commit (txn vector updates keep prior append-only behavior).
    if txn_id == 0 {
        if let Some(&old_gid) = idx.key_hash_to_global_id.get(&key_hash) {
            let base = snap.mutable.global_id_base();
            if old_gid >= base
                && snap
                    .mutable
                    .mark_deleted_if_key(old_gid - base, key_hash, insert_lsn)
            {
                // O(1) fast path: old version still in the mutable segment,
                // MVCC-tombstoned at the new version's LSN (older snapshots
                // keep seeing the old vector; new snapshots see only the new).
            } else {
                // Old version was compacted (or the gid mapping was stale):
                // steady-state interior tombstone across immutable segments —
                // the same path DEL/UNLINK takes via `mark_deleted_for_key` —
                // plus a defensive mutable scan for the stale-mapping case.
                snap.mutable.mark_deleted_by_key_hash(key_hash, insert_lsn);
                for imm in snap.immutable.iter() {
                    imm.mark_deleted_by_key_hash(key_hash);
                }
            }
        }
    }
    let internal_id = if txn_id != 0 {
        snap.mutable
            .append_transactional(key_hash, &f32_vec, insert_lsn, txn_id)
    } else {
        snap.mutable.append(key_hash, &f32_vec, insert_lsn)
    };
    // Use global_id for payload index so filter bitmaps match
    // search results after compaction advances global_id_base.
    let global_id = snap.mutable.global_id_base() + internal_id;
    crate::vector::metrics::add_vectors(1);

    // Record key_hash → global_id mapping for future metadata-only updates.
    // Bucket-scoped COW, mirroring key_hash_to_key (QP-1 + RSS/CPU wave 4).
    idx.key_hash_to_global_id.insert(key_hash, global_id);
    // B2 (durability): mirror the same key_hash into the checksum map so the
    // two maps never drift — the B3 dedup rescan compares this checksum
    // against a freshly-hashed current value to decide unchanged-vs-changed.
    idx.key_hash_to_vec_checksum
        .insert(key_hash, xxhash_rust::xxh64::xxh64(&blob, 0));

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
    // Record original Redis key (shared across all fields). Bucket-scoped
    // COW: clones only the ONE bucket if a search snapshot holds the map
    // concurrently (QP-1 + RSS/CPU wave 4).
    idx.key_hash_to_key
        .get_or_insert_with(key_hash, || bytes::Bytes::copy_from_slice(key));

    // Look up the additional field's SegmentHolder
    let fs = match idx.field_segments.get(field_name.as_ref()) {
        Some(fs) => fs,
        None => return, // field not found (should not happen with valid schema)
    };
    // `insert_lsn` comes from the same allocation as the default-field insert
    // so both fields share one logical write event (Phase 165 MVCC contract).
    // When inside a TXN (txn_id != 0), tag with txn_id for uncommitted visibility.
    let snap = fs.segments.load();
    // VEC-1 (additional fields): tombstone the prior version on update. Field
    // segments have no `key_hash → global_id` map, so this is the scan path
    // (mutable is bounded by compact_threshold; immutables are set lookups).
    if txn_id == 0 {
        snap.mutable.mark_deleted_by_key_hash(key_hash, insert_lsn);
        for imm in snap.immutable.iter() {
            imm.mark_deleted_by_key_hash(key_hash);
        }
    }
    let _internal_id = if txn_id != 0 {
        snap.mutable
            .append_transactional(key_hash, &f32_vec, insert_lsn, txn_id)
    } else {
        snap.mutable.append(key_hash, &f32_vec, insert_lsn)
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
/// fan-out to all connected replica sender channels (non-blocking try_send), and route
/// the entry to the per-shard AOF writer pool when AOF is enabled.
///
/// CRITICAL: shard_offset in ReplicationState is SEPARATE from WalWriterV3::bytes_written.
/// WalWriterV3's on-disk bytes reset only on segment recycling; shard_offset NEVER resets.
///
/// FIX-W1-2: `aof_pool` was added to route MSET/coordinator cross-shard writes
/// through the per-shard AOF pool. The SPSC drain is synchronous so we use
/// `try_send_append` (fire-and-forget). The `appendfsync=always` rendezvous is
/// handled by the connection handler (async context), not here.
/// True when `wal_append_and_fanout` has any consumer for the serialized
/// command (S3.5b criterion: WAL, live replicas, or the AOF pool).
/// ARM perf annotate showed the pre-S3.5b locks were ~21% of CPU on 8-shard
/// SET p=64 with everything off; the criterion is fully derivable from the
/// inputs — no flags or shared state. Skipping leaves shard_offset
/// un-advanced, which is fine: with no WAL and no replicas the offsets are
/// dead bytes (no consumer exists). Callers on the cross-shard write arms
/// check THIS before `aof::serialize_command` so the serialization alloc +
/// copy is also skipped when the fanout would no-op (it was pure waste on
/// every cross-shard write with persistence off).
#[inline]
pub(crate) fn wal_fanout_has_work(
    wal_writer: &Option<WalWriterV3>,
    replica_txs: &[crate::shard::dispatch::ReplicaFanout],
    aof_pool: Option<&std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    wal_kv_log: bool,
) -> bool {
    (wal_kv_log && wal_writer.is_some()) || !replica_txs.is_empty() || aof_pool.is_some()
}

/// Deliver one replication record to every registered replica, KICKING any
/// replica whose bounded channel is full (task #35).
///
/// The old policy — `try_send` and skip on `Full` — silently dropped records
/// for a lagging replica, leaving a permanent gap in its stream while
/// `master_link_status` stayed "up" (observed: 2k of 40k keys delivered under
/// pipelined load, offsets diverged forever, WAIT correctly reported 0).
/// A replica that cannot keep up must instead be disconnected so it retries
/// PSYNC and resyncs from the backlog — Redis's output-buffer-limit policy.
/// The kick is two-stage because the drain task and `ReplicaInfo.shard_txs`
/// hold sender clones (dropping our entry alone cannot close the channel):
/// set the shared `kicked` flag (the drain task polls it and closes the
/// socket), then drop our entry so this shard stops queueing immediately.
pub(crate) fn fanout_send_or_kick(
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    bytes: &bytes::Bytes,
    end_offset: u64,
) {
    replica_txs.retain(|r| {
        // Exactly-once cut: a record at or below the replica's snapshot cut
        // is already inside its FULLRESYNC body — delivering it live would
        // double-apply non-idempotent commands (INCR/LPUSH) on the replica.
        if end_offset <= r.cut {
            return true;
        }
        match r.tx.try_send(bytes.clone()) {
            Ok(()) => true,
            Err(flume::TrySendError::Full(_)) => {
                r.kicked.store(true, std::sync::atomic::Ordering::Release);
                tracing::warn!(
                    replica_id = r.replica_id,
                    "replica live fan-out channel FULL — kicking replica to force a \
                     resync (a skipped record would silently diverge it forever)"
                );
                false
            }
            // Drain task already gone; just stop queueing.
            Err(flume::TrySendError::Disconnected(_)) => false,
        }
    });
}

/// Error frame substituted for a write's success frame when the command
/// mutated memory but its AOF record could not be enqueued within the
/// backpressure budget — fail-loud so the client knows durability was not
/// achieved (review finding, PR #211).
pub(crate) const AOF_APPEND_LOST_ERR: &[u8] =
    b"MOONERR AOF backpressure: write applied in memory but not queued for persistence";

/// Returns `false` iff the AOF append was NOT enqueued (bounded backpressure
/// exhausted / writer gone) — callers with a per-command response frame MUST
/// replace it with [`AOF_APPEND_LOST_ERR`]. WAL/replica fan-out remains
/// fire-and-forget by design (replication has its own resync path). Batched
/// callers pass ONE `aof_budget` across the whole batch so sustained
/// backpressure stalls the shard thread at most `AOF_SPSC_BACKPRESSURE_BOUND`
/// per drain arm, not per command.
pub(crate) fn wal_append_and_fanout(
    data: &[u8],
    // task #35: db the command executed in — threaded into the AOF pool so
    // the writer can inject a `SELECT <db>` record on a db-context change.
    db: usize,
    wal_writer: &mut Option<WalWriterV3>,
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    shard_id: usize,
    aof_pool: Option<&std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    wal_kv_log: bool,
    aof_budget: &mut std::time::Duration,
) -> bool {
    // S3.5b (2026-04-27): hot-path bypass when nothing actually has work.
    // See `wal_fanout_has_work` — callers use the same predicate to skip the
    // `aof::serialize_command` alloc entirely when the fanout would no-op.
    if !wal_fanout_has_work(wal_writer, replica_txs, aof_pool, wal_kv_log) {
        return true;
    }
    // `wal_kv_log == false` (--wal-kv-log auto/off): the AOF is the recovery
    // authority and no CDC subscriber is attached, so the WAL copy of this
    // KV command would be written and then discarded by Phase-B recovery —
    // pure write amplification (measured 2.7× file bytes at shards=4).
    // FPI/checkpoint/feature records are unaffected (different entry points).
    if wal_kv_log {
        if let Some(w) = wal_writer {
            w.append(
                crate::persistence::wal_v3::record::WalRecordType::Command,
                data,
            );
        }
    }
    // R2 (task #20): on a multi-shard master every db-scoped record on the
    // replica wire carries its OWN `SELECT <db>` prefix. N shard threads feed
    // one merged wire, so a shared "current db" context cannot exist — and
    // the prefix+payload must travel as ONE record (one channel send, one
    // backlog append pair, one offset advance) so no cross-shard interleave
    // can split a SELECT from the write it frames. Single-shard masters keep
    // the emit-on-change tracking in `record_local_write_db` (this fan-out
    // leg only fires for cross-shard dispatch, which needs num_shards > 1).
    let select_prefix: Option<bytes::Bytes> =
        if !replica_txs.is_empty() && repl_state.as_ref().is_some_and(|h| h.num_shards() > 1) {
            Some(crate::persistence::aof::serialize_select_record(db))
        } else {
            None
        };
    // 2. Replication backlog (in-memory circular buffer for partial resync).
    //
    // The backlog is shared via Arc<Mutex<Option<...>>> with PSYNC handlers.
    // Cost on the write path:
    //   - When `None` (no replica ever connected): one branch, no lock acquire.
    //   - When `Some` (replication active): one uncontended parking_lot::Mutex
    //     acquire per WAL flush (typically once per 1ms tick batch, NOT per write).
    let mut guard = repl_backlog.lock();
    if let Some(backlog) = guard.as_mut() {
        if let Some(prefix) = &select_prefix {
            backlog.append(prefix);
        }
        backlog.append(data);
    }
    drop(guard);
    // 3. Advance monotonic replication offset (NEVER resets on WAL truncation)
    // QW3 (2026-06 review finding 1.4): `repl_state` is a lock-free
    // OffsetHandle cloned out of `RwLock<ReplicationState>` once at shard
    // startup — the per-write advance no longer read-locks the RwLock.
    // The SELECT prefix counts too: offset accounting must equal the bytes
    // the replica receives, or WAIT/ACK math diverges.
    let end_offset = if let Some(offsets) = repl_state {
        let prefix_len = select_prefix.as_ref().map_or(0, |p| p.len());
        offsets.increment_shard_offset(shard_id, (prefix_len + data.len()) as u64)
    } else {
        // No offset handle — no cut accounting possible; deliver
        // unconditionally (replicas can't exist without repl_state in
        // practice, this keeps the degenerate path fail-open).
        u64::MAX
    };
    // 4. Fan-out to replica sender tasks — DEFERRED through the self queue
    //    (`ReplicaLiveFanout`), never sent directly from here. Two reasons
    //    (R2 exactly-once redesign, task #20):
    //    - Ordering: local handler writes already queue their delivery as
    //      self-queue messages; a direct send here would put this (later-
    //      offset) record on the wire BEFORE their (earlier-offset) queued
    //      bytes — the replica would apply same-key writes out of the
    //      master's order.
    //    - Registration cut: a replica registration queued behind this
    //      drain cycle (self-shard PSYNC leg) would miss a direct send
    //      entirely — the record is past its snapshot body but not in
    //      `replica_txs` yet: lost, with the offset advanced (permanent
    //      replica lag). Deferring one cycle guarantees delivery lands
    //      after the registration; the per-replica `cut` filter keeps it
    //      exactly-once.
    if !replica_txs.is_empty() {
        let bytes = match &select_prefix {
            Some(prefix) => {
                let mut combined = Vec::with_capacity(prefix.len() + data.len());
                combined.extend_from_slice(prefix);
                combined.extend_from_slice(data);
                bytes::Bytes::from(combined)
            }
            None => bytes::Bytes::copy_from_slice(data),
        };
        crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::ReplicaLiveFanout {
            bytes,
            end_offset,
        });
    }
    // 5. Per-shard AOF pool (FIX-W1-2): route to the owning shard's writer.
    // Bounded-blocking (`send_append_bounded_blocking`) because this function
    // is sync and cannot await the fsync rendezvous: the fast path is the same
    // try_send as before; only when the writer channel is FULL (writer >10k
    // appends behind) does the shard thread block up to the bound instead of
    // silently losing a record the client already got `+OK` for. The
    // `appendfsync=always` ack is handled by the async connection handler
    // (handler_sharded / handler_single). LSN=0 is safe here: per-shard order
    // is preserved by write order; the LSN is only meaningful for cross-shard
    // TXN merge (RFC step 5, not yet wired).
    if let Some(pool) = aof_pool {
        return pool.send_append_bounded_blocking(
            shard_id,
            0,
            db,
            bytes::Bytes::copy_from_slice(data),
            aof_budget,
        );
    }
    true
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
            0,         // db
            &mut None, // no writer
            &backlog,
            &mut vec![], // no replicas
            &None,       // no repl_state
            0,
            None, // no aof_pool
            true, // wal_kv_log
            &mut std::time::Duration::from_millis(5),
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
        let mut replica_txs: Vec<crate::shard::dispatch::ReplicaFanout> =
            vec![crate::shard::dispatch::ReplicaFanout {
                replica_id: 1,
                tx,
                kicked: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                cut: 0,
            }];

        wal_append_and_fanout(
            b"hello",
            0, // db
            &mut None,
            &backlog,
            &mut replica_txs,
            &None,
            0,
            None, // no aof_pool
            true, // wal_kv_log
            &mut std::time::Duration::from_millis(5),
        );

        let end = backlog.lock().as_ref().unwrap().end_offset();
        assert_eq!(
            end, 5,
            "backlog must receive 5 bytes when at least one replica is connected"
        );
    }

    /// FIX-W1-2: When an AofWriterPool is provided, wal_append_and_fanout must
    /// route bytes to the pool even when there is no WAL writer and no replicas
    /// (S3.5b bypass must NOT trigger when aof_pool is Some).
    #[test]
    fn test_wal_append_routes_to_aof_pool_when_provided() {
        use crate::persistence::aof::{AofMessage, AofWriterPool, FsyncPolicy};
        use crate::runtime::channel::mpsc_bounded;

        let backlog: SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(Some(ReplicationBacklog::new(1024))));

        // Build a pool backed by a real channel so we can observe what arrives.
        let (tx, rx) = mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level_with_policy(
            tx,
            FsyncPolicy::EverySec,
            std::time::Duration::ZERO,
        );

        wal_append_and_fanout(
            b"world",
            0,         // db
            &mut None, // no writer
            &backlog,
            &mut vec![], // no replicas — S3.5b bypass triggered without pool guard
            &None,       // no repl_state
            0,           // shard_id
            Some(&pool), // aof_pool provided — bypass must NOT fire
            true,        // wal_kv_log
            &mut std::time::Duration::from_millis(5),
        );

        // The pool should have received exactly one message.
        let msg = rx
            .try_recv()
            .expect("pool must have received an AOF append");
        match msg {
            AofMessage::Append { bytes, .. } => {
                assert_eq!(
                    bytes.as_ref(),
                    b"world",
                    "pool must receive the correct bytes"
                );
            }
            AofMessage::AppendSync { .. } => panic!("expected Append, got AppendSync"),
            AofMessage::Rewrite(_) => panic!("expected Append, got Rewrite"),
            AofMessage::RewriteSharded(_) => panic!("expected Append, got RewriteSharded"),
            AofMessage::RewritePerShard { .. } => panic!("expected Append, got RewritePerShard"),
            AofMessage::Shutdown => panic!("expected Append, got Shutdown"),
        }
    }

    /// FIX-W1-2 r2: PipelineBatch/PipelineBatchSlotted arms MUST NOT forward
    /// writes to the AofWriterPool. The connection-handler coordinator already
    /// appends AOF for these arms after collecting the shard response
    /// (handler_monoio/mod.rs:2004, handler_sharded/mod.rs:1703).
    ///
    /// Verify the invariant directly: `wal_append_and_fanout` called with
    /// `None` (the PipelineBatch fix) must produce zero messages in the pool
    /// channel, while the same call with `Some(&pool)` (the MultiExecute path)
    /// must produce exactly one message.
    ///
    /// Red state (pre-fix): the PipelineBatch arms passed `aof_pool` instead
    /// of `None`, so calling this test function using the arm's actual argument
    /// would have produced 1 message instead of 0 — the double-write.
    #[test]
    fn pipeline_batch_arm_passes_none_to_prevent_double_write() {
        use crate::persistence::aof::{AofMessage, AofWriterPool, FsyncPolicy};
        use crate::runtime::channel::mpsc_bounded;

        let backlog: SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(Some(ReplicationBacklog::new(1024))));

        // Build a 2-shard pool so per_shard_with_policy's debug_assert passes.
        let (tx0, rx0) = mpsc_bounded::<AofMessage>(16);
        let (tx1, rx1) = mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::EverySec,
            std::time::Duration::ZERO,
        );

        // ── PipelineBatch path: caller passes None ──
        // Pre-fix this was `aof_pool` (Some), which caused the double-write.
        wal_append_and_fanout(
            b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n",
            0,         // db
            &mut None, // no writer
            &backlog,
            &mut vec![], // no replicas
            &None,       // no repl_state
            0,           // shard_id
            None,        // PipelineBatch fix: None prevents double-write
            true,        // wal_kv_log
            &mut std::time::Duration::from_millis(5),
        );
        assert!(
            rx0.try_recv().is_err(),
            "PipelineBatch must NOT forward to aof_pool (coordinator handles it); \
             a message here means the double-write P0 bug is still present"
        );
        assert!(
            rx1.try_recv().is_err(),
            "shard-1 pool must also be empty for PipelineBatch arm"
        );

        // ── MultiExecute path: caller passes Some(&pool) ──
        // This arm has no coordinator-side AOF write, so the pool MUST receive
        // the entry (otherwise the per-shard AOF would be silently empty for
        // cross-shard MSET/DEL/EXISTS commands).
        wal_append_and_fanout(
            b"*3\r\n$4\r\nMSET\r\n$1\r\nb\r\n$1\r\n2\r\n",
            0, // db
            &mut None,
            &backlog,
            &mut vec![],
            &None,
            0,
            Some(&pool), // MultiExecute: pool must receive this entry
            true,        // wal_kv_log
            &mut std::time::Duration::from_millis(5),
        );
        let msg = rx0
            .try_recv()
            .expect("MultiExecute MUST forward to aof_pool; pool is empty — AOF silent drop");
        assert!(
            matches!(msg, AofMessage::Append { .. }),
            "expected AofMessage::Append from MultiExecute arm, got unexpected variant",
        );
    }

    /// `--wal-kv-log` gate (2026-07 write-path durability): with
    /// `wal_kv_log == false` (AOF is the recovery authority, no CDC
    /// subscriber) the KV Command record must NOT reach the WAL — startup
    /// recovery wipes WAL-replayed state and replays the AOF, so the WAL
    /// copy is pure 2× write amplification. The AOF pool must STILL receive
    /// the entry (it is the surviving durability log).
    #[test]
    fn test_kv_append_skipped_when_wal_kv_log_false() {
        use crate::persistence::aof::{AofMessage, AofWriterPool, FsyncPolicy};
        use crate::runtime::channel::mpsc_bounded;

        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let mut w3 = Some(WalWriterV3::new(0, &wal_dir, 16 * 1024 * 1024).unwrap());
        w3.as_mut().unwrap().flush_sync().unwrap();
        let seg = wal_dir.join("000000000001.wal");
        let base_len = std::fs::metadata(&seg).unwrap().len();

        let (tx, rx) = mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level_with_policy(
            tx,
            FsyncPolicy::EverySec,
            std::time::Duration::ZERO,
        );
        let backlog: SharedBacklog = std::sync::Arc::new(parking_lot::Mutex::new(None));
        let cmd = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";

        wal_append_and_fanout(
            cmd,
            0, // db
            &mut w3,
            &backlog,
            &mut vec![],
            &None,
            0,
            Some(&pool),
            false, // wal_kv_log: AOF authoritative, no CDC consumer
            &mut std::time::Duration::from_millis(5),
        );
        w3.as_mut().unwrap().flush_sync().unwrap();
        assert_eq!(
            std::fs::metadata(&seg).unwrap().len(),
            base_len,
            "KV Command record must be skipped when wal_kv_log is false (double-write gate)"
        );
        assert!(
            matches!(rx.try_recv(), Ok(AofMessage::Append { .. })),
            "the AOF pool must still receive the entry — it is the surviving KV log"
        );

        // Control: with wal_kv_log == true (CDC attached / --wal-kv-log on)
        // the record must land in the WAL as before.
        wal_append_and_fanout(
            cmd,
            0, // db
            &mut w3,
            &backlog,
            &mut vec![],
            &None,
            0,
            Some(&pool),
            true, // wal_kv_log
            &mut std::time::Duration::from_millis(5),
        );
        w3.as_mut().unwrap().flush_sync().unwrap();
        assert!(
            std::fs::metadata(&seg).unwrap().len() > base_len,
            "with wal_kv_log true the KV record must be logged to the WAL"
        );
    }
}

#[cfg(test)]
mod drain_cap_tests {
    use super::*;
    use ringbuf::HeapRb;
    use ringbuf::traits::{Producer, Split};

    /// M3 (spsc-wake-floor): a drain cycle that stops at MAX_DRAIN_PER_CYCLE
    /// (256) must return `true` — queued messages may remain, so the caller
    /// self-re-notifies — while a cycle that empties the rings returns
    /// `false`. The integration suite cannot reach the cap from one client
    /// (pipelined commands coalesce into one PipelineBatch per target per
    /// read chunk), so the cap path is pinned here with 300 real ring
    /// messages. `BlockCancel` for an unknown wait_id is a harmless no-op,
    /// which keeps every other dependency inert (no WAL, no snapshot).
    #[test]
    fn drain_cap_reports_possible_tail() {
        let (shard_databases_inner, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let shard_databases = Arc::new(shard_databases_inner);
        let rb = HeapRb::<ShardMessage>::new(512);
        let (mut prod, cons) = rb.split();
        for i in 0..300u64 {
            assert!(
                prod.try_push(ShardMessage::BlockCancel { wait_id: i })
                    .is_ok(),
                "ring accepts 300 messages"
            );
        }
        let mut consumers = vec![cons];

        let pubsub = parking_lot::RwLock::new(PubSubRegistry::new());
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let mut pending_snapshot = None;
        let mut snapshot_state: Option<SnapshotState> = None;
        let mut wal_writer: Option<WalWriterV3> = None;
        let backlog: crate::replication::backlog::SharedBacklog =
            Arc::new(parking_lot::Mutex::new(None));
        let mut replica_txs = Vec::new();
        let offsets: Option<crate::replication::state::OffsetHandle> = None;
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        let mut migrations = Vec::new();
        let mut cdc = Vec::new();
        let mut manifest = None;
        let mut autovacuum = crate::shard::autovacuum::AutovacuumDaemon::new(Default::default());
        // M2 fix: no shard context needed for this drain-cap test — maxmemory
        // unset (evict_active == false) is the fast, no-op path.
        let rtcfg = Arc::new(parking_lot::RwLock::new(RuntimeConfig::default()));
        let spill_fid = Rc::new(Cell::new(1u64));

        // BlockCancel messages don't touch ShardSlice, so no init_shard needed.
        // First cycle: 300 queued > 256 cap -> drains exactly 256, reports tail.
        let hit_cap = drain_spsc_shared(
            &shard_databases,
            &mut consumers,
            &pubsub,
            &blocking,
            &mut pending_snapshot,
            &mut snapshot_state,
            &mut wal_writer,
            &backlog,
            &mut replica_txs,
            &offsets,
            0,
            &script_cache,
            &clock,
            &mut migrations,
            &mut cdc,
            &mut manifest,
            1000,
            8,
            0.2,
            &mut autovacuum,
            None,
            true, // wal_kv_log
            &rtcfg,
            None,
            &spill_fid,
            None,
        );
        assert!(
            hit_cap,
            "300 queued messages exceed the 256 cap: first cycle must report a possible tail"
        );

        // Second cycle: the 44 remaining messages drain fully -> no tail.
        let hit_cap2 = drain_spsc_shared(
            &shard_databases,
            &mut consumers,
            &pubsub,
            &blocking,
            &mut pending_snapshot,
            &mut snapshot_state,
            &mut wal_writer,
            &backlog,
            &mut replica_txs,
            &offsets,
            0,
            &script_cache,
            &clock,
            &mut migrations,
            &mut cdc,
            &mut manifest,
            1000,
            8,
            0.2,
            &mut autovacuum,
            None,
            true, // wal_kv_log
            &rtcfg,
            None,
            &spill_fid,
            None,
        );
        assert!(
            !hit_cap2,
            "44 remaining messages drain fully: second cycle must report no tail"
        );
        use ringbuf::traits::Observer;
        assert!(consumers[0].is_empty(), "all 300 messages must be consumed");
    }
}
