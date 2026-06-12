use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Mutex, MutexGuard};

use crate::storage::Database;
use crate::workspace::wal::{decode_workspace_create, decode_workspace_drop};
use crate::workspace::{WorkspaceId, WorkspaceMetadata, WorkspaceRegistry};

/// Published per-shard store-memory counters (C5 / M4).
///
/// Each shard's 100ms tick writes vector/text/graph resident bytes into these
/// atomics. Cross-thread observers — Prometheus publisher (metrics_setup.rs)
/// and MEMORY DOCTOR (server_admin.rs) — read from these atomics with zero
/// lock acquisitions.
///
/// Figures lag at most one tick (≤100 ms); this is documented and acceptable
/// for observability paths.
pub struct ShardStoreMemory {
    /// Combined resident bytes of all VectorStore segments (mutable + immutable).
    pub vector: AtomicUsize,
    /// Resident bytes of TextStore indexes.
    pub text: AtomicUsize,
    /// Resident bytes of GraphStore CSR segments.
    pub graph: AtomicUsize,
}

/// Shared infrastructure handle — the residual cross-shard state after M5.
///
/// Contains ONLY genuinely-shared handles. Per-shard data (databases, stores,
/// registries) was moved into `ShardSliceInit` packages returned by `new()`.
/// No per-shard `RwLock`/`Mutex` wrappers live here.
///
/// # Fields kept (C6)
/// - `wal_append_txs`: WAL append senders — `Send` by design; cross-shard
///   `wal_append(owner, …)` stays legal under slice with no message variant.
/// - `num_shards` / `db_count`: immutable configuration.
/// - `memory_per_shard` / `elastic_budgets`: published-atomic patterns.
/// - `workspace_registry`: single process-global registry (C3 / M3).
/// - `store_memory_per_shard`: published store-memory atomics (C5 / M4).
pub struct ShardDatabases {
    /// Per-shard WAL append channel sender. Connection handlers send serialized
    /// write commands here; the event loop drains into WAL v2/v3 on the 1ms tick.
    /// OnceLock: set once at event-loop startup (before connections are
    /// accepted), then every hot-path read is lock-free.
    wal_append_txs: Vec<std::sync::OnceLock<crate::runtime::channel::MpscSender<bytes::Bytes>>>,
    /// Process-global WorkspaceRegistry (C3 / M3).
    ///
    /// Workspaces are control-plane objects looked up by every connection
    /// regardless of which shard accepted it — a single Mutex is not a
    /// hot-path concern (WS commands are rare). The per-shard array
    /// (`workspace_registries`) is retired; all paths use this one field.
    /// WAL records keep the shard-0 stream via `wal_append(0, …)` (unchanged).
    /// Caller lazy-inits via `get_or_insert_with(|| Box::new(WorkspaceRegistry::new()))`.
    workspace_registry: Mutex<Option<Box<WorkspaceRegistry>>>,
    num_shards: usize,
    db_count: usize,
    /// Per-shard memory publishers for lock-free cross-shard reads.
    ///
    /// Each shard's event loop holds a clone of its `Arc<AtomicUsize>` in
    /// `ShardSlice.estimated_memory`. Cross-shard readers — maxmemory eviction
    /// and metrics scrape — call `read_memory_sum()` for a lock-free sum.
    memory_per_shard: Vec<Arc<AtomicUsize>>,
    /// Per-shard elastic memory budgets (GAP-1 hot-shard pooling).
    elastic_budgets: Vec<Arc<AtomicUsize>>,
    /// Per-shard published store-memory atomics (C5 / M4).
    ///
    /// The shard's 100ms tick refreshes these via `with_shard`. Prometheus
    /// publisher and MEMORY DOCTOR read them with zero lock acquisitions.
    pub store_memory_per_shard: Box<[Arc<ShardStoreMemory>]>,
}

impl ShardDatabases {
    /// Create from pre-restored database vectors (one `Vec<Database>` per shard).
    ///
    /// Returns the shared infrastructure handle AND one `ShardSliceInit`
    /// package per shard. Each package is handed to its shard thread's closure
    /// by move; the shard calls `slice::init_shard(init)` at startup.
    ///
    /// WAL/AOF recovery MUST happen BEFORE this call (on the raw per-shard
    /// state via `recover_*` free functions in this module) so the databases
    /// passed here are already fully restored.
    pub fn new(
        shard_databases: Vec<Vec<Database>>,
    ) -> (Arc<Self>, Vec<crate::shard::slice::ShardSliceInit>) {
        let num_shards = shard_databases.len();
        let db_count = shard_databases.first().map_or(0, |v| v.len());

        let wal_append_txs = (0..num_shards)
            .map(|_| std::sync::OnceLock::new())
            .collect();
        let workspace_registry = Mutex::new(None);
        let memory_per_shard: Vec<Arc<AtomicUsize>> = (0..num_shards)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        let elastic_budgets: Vec<Arc<AtomicUsize>> = (0..num_shards)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        let store_memory_per_shard: Box<[Arc<ShardStoreMemory>]> = (0..num_shards)
            .map(|_| {
                Arc::new(ShardStoreMemory {
                    vector: AtomicUsize::new(0),
                    text: AtomicUsize::new(0),
                    graph: AtomicUsize::new(0),
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let shared = Arc::new(Self {
            wal_append_txs,
            workspace_registry,
            num_shards,
            db_count,
            memory_per_shard: memory_per_shard.clone(),
            elastic_budgets: elastic_budgets.clone(),
            store_memory_per_shard: store_memory_per_shard.clone(),
        });

        // Build one ShardSliceInit per shard, consuming the databases. The
        // construction lives in slice.rs with the type it builds (the only
        // per-shard state this module touches is the moment of handoff).
        let inits = crate::shard::slice::ShardSliceInit::build_all(
            shard_databases,
            &memory_per_shard,
            &store_memory_per_shard,
        );

        (shared, inits)
    }

    /// Set the WAL append channel sender for a shard.
    ///
    /// Called once during event loop startup before connections are accepted.
    pub fn set_wal_append_tx(
        &self,
        shard_id: usize,
        tx: crate::runtime::channel::MpscSender<bytes::Bytes>,
    ) {
        if self.wal_append_txs[shard_id].set(tx).is_err() {
            tracing::warn!(
                shard_id,
                "wal_append_tx already initialized; re-init ignored (OnceLock)"
            );
        }
    }

    /// Send serialized command bytes to the WAL append channel for a shard.
    ///
    /// Called by connection handlers for local write commands. The event loop
    /// drains this channel on the 1ms tick into WAL v2/v3.
    /// No-op when persistence is disabled.
    #[inline]
    pub fn wal_append(&self, shard_id: usize, data: bytes::Bytes) {
        if let Some(tx) = self.wal_append_txs[shard_id].get() {
            let _ = tx.try_send(data);
        }
    }

    /// Strict variant of [`wal_append`]: returns `true` if the message was
    /// either accepted by the WAL channel **or** persistence is disabled
    /// (no durability requirement). Returns `false` only when persistence is
    /// configured but the channel rejected the send — in that case the caller
    /// must NOT proceed with a state mutation that depends on this WAL
    /// record's durability (e.g. SWAPDB has no command-level rollback).
    #[inline]
    #[must_use = "callers must check the result and skip the mutation on WAL failure"]
    pub fn try_wal_append_required(&self, shard_id: usize, data: bytes::Bytes) -> bool {
        match self.wal_append_txs[shard_id].get() {
            Some(tx) => tx.try_send(data).is_ok(),
            None => true, // persistence disabled — no durability requirement
        }
    }

    /// Acquire the process-global WorkspaceRegistry lock (C3 / M3).
    ///
    /// Workspaces are control-plane objects looked up by every connection
    /// regardless of which shard accepted it, so all paths — handlers,
    /// uring intercept, WAL replay — share one registry. WS commands are
    /// rare; a single mutex is not a hot-path concern.
    /// Caller lazy-inits via `get_or_insert_with(|| Box::new(WorkspaceRegistry::new()))`.
    #[inline]
    pub fn workspace_registry(&self) -> MutexGuard<'_, Option<Box<WorkspaceRegistry>>> {
        self.workspace_registry.lock()
    }

    /// Total number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Number of databases per shard (typically 16).
    #[inline]
    pub fn db_count(&self) -> usize {
        self.db_count
    }

    /// Get the per-shard memory publisher `Arc`.
    ///
    /// Called once per shard at startup when constructing the `ShardSlice`.
    /// The returned `Arc` is cloned into `ShardSlice.estimated_memory`; the
    /// master copy lives here in `memory_per_shard[shard_id]`.
    #[inline]
    pub fn memory_publisher(&self, shard_id: usize) -> Arc<AtomicUsize> {
        self.memory_per_shard[shard_id].clone()
    }

    /// Publish a shard's aggregate memory usage (GAP-1 / Phase 2 wiring).
    ///
    /// Called from the shard's 100ms eviction tick. One Relaxed store.
    #[inline]
    pub fn publish_memory(&self, shard_id: usize, used: usize) {
        self.memory_per_shard[shard_id].store(used, Ordering::Relaxed);
    }

    /// Recompute and publish this shard's elastic memory budget (GAP-1).
    pub fn recompute_elastic_budget(
        &self,
        shard_id: usize,
        config: &crate::config::RuntimeConfig,
    ) -> usize {
        let base = config.maxmemory_per_shard();
        if base == 0 || self.num_shards <= 1 {
            self.elastic_budgets[shard_id].store(0, Ordering::Relaxed);
            return 0;
        }
        let used: Vec<usize> = self
            .memory_per_shard
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect();
        let budget = crate::storage::eviction::compute_elastic_budget(shard_id, base, &used);
        self.elastic_budgets[shard_id].store(budget, Ordering::Relaxed);
        budget
    }

    /// This shard's current elastic budget; `0` = none published yet.
    #[inline]
    pub fn elastic_budget(&self, shard_id: usize) -> usize {
        self.elastic_budgets[shard_id].load(Ordering::Relaxed)
    }

    /// Sum all per-shard memory publishers with `Relaxed` loads. Lock-free.
    #[inline]
    pub fn read_memory_sum(&self) -> usize {
        self.memory_per_shard
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum()
    }

    /// Read the last published KV memory for a single shard with one Relaxed load.
    #[inline]
    pub fn published_shard_memory(&self, shard_id: usize) -> usize {
        self.memory_per_shard[shard_id].load(Ordering::Relaxed)
    }

    /// Return a clone of the `Arc<ShardStoreMemory>` for `shard_id`.
    ///
    /// Called once at shard startup to hand the `Arc` into `ShardSliceInit`.
    #[inline]
    pub fn store_memory_publisher(&self, shard_id: usize) -> Arc<ShardStoreMemory> {
        self.store_memory_per_shard[shard_id].clone()
    }
}

// ── Boot-time recovery free functions ─────────────────────────────────────────
//
// These functions operate on `&mut [ShardSliceInit]` (pre-packaged per-shard
// state) rather than on `ShardDatabases`. They are called single-threaded
// during server startup AFTER `ShardDatabases::new` returns the init packages
// and BEFORE shard threads are spawned — no locks needed.

/// Replay workspace WAL records into the process-global `WorkspaceRegistry`.
///
/// Called during server startup after graph and temporal WAL replay.
/// Scans `shard-{id}/wal-v3/` for v3 segment files and processes
/// WorkspaceCreate/WorkspaceDrop records. All records populate the single
/// global registry (C3/M3).
pub fn replay_workspace_wal(shared: &Arc<ShardDatabases>, persistence_dir: &std::path::Path) {
    use crate::persistence::wal_v3::record::{WalRecord, WalRecordType, read_wal_v3_record};

    let num_shards = shared.num_shards;
    let mut total_create = 0u64;
    let mut total_drop = 0u64;

    for shard_id in 0..num_shards {
        // WAL v3 segments live in shard-{id}/wal-v3/ — matching recovery.rs:361.
        let wal_dir = persistence_dir
            .join(format!("shard-{}", shard_id))
            .join("wal-v3");
        if !wal_dir.exists() {
            continue;
        }

        let mut create_count = 0u64;
        let mut drop_count = 0u64;

        let mut handle_record = |record_type: WalRecordType, payload: &[u8]| match record_type {
            WalRecordType::WorkspaceCreate => {
                if let Some((ws_bytes, name)) = decode_workspace_create(payload) {
                    let ws_id = WorkspaceId::from_bytes(ws_bytes);
                    let meta = WorkspaceMetadata {
                        id: ws_id,
                        name: bytes::Bytes::from(name),
                        created_at: 0,
                    };
                    let mut guard = shared.workspace_registry.lock();
                    let reg = guard.get_or_insert_with(|| Box::new(WorkspaceRegistry::new()));
                    reg.insert(ws_id, meta);
                    create_count += 1;
                }
            }
            WalRecordType::WorkspaceDrop => {
                if let Some(ws_bytes) = decode_workspace_drop(payload) {
                    let ws_id = WorkspaceId::from_bytes(ws_bytes);
                    let mut guard = shared.workspace_registry.lock();
                    if let Some(reg) = guard.as_mut() {
                        reg.remove(&ws_id);
                    }
                    drop_count += 1;
                }
            }
            _ => {}
        };

        let on_command = &mut |record: &WalRecord| match record.record_type {
            WalRecordType::WorkspaceCreate | WalRecordType::WorkspaceDrop => {
                handle_record(record.record_type, &record.payload);
            }
            WalRecordType::Command => {
                if let Some(inner) = read_wal_v3_record(&record.payload) {
                    match inner.record_type {
                        WalRecordType::WorkspaceCreate | WalRecordType::WorkspaceDrop => {
                            handle_record(inner.record_type, &inner.payload);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        };
        let on_fpi = &mut |_: &WalRecord| {};

        if let Ok(entries) = std::fs::read_dir(&wal_dir) {
            let mut wal_files: Vec<_> = entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
                .map(|e| e.path())
                .collect();
            wal_files.sort();

            for wal_file in &wal_files {
                let _ = crate::persistence::wal_v3::replay::replay_wal_v3_file(
                    wal_file, 0, on_command, on_fpi,
                );
            }
        }

        total_create += create_count;
        total_drop += drop_count;

        if create_count > 0 || drop_count > 0 {
            tracing::info!(
                "Shard {}: replayed {} WorkspaceCreate + {} WorkspaceDrop WAL records \
                 into global registry",
                shard_id,
                create_count,
                drop_count,
            );
        }
    }

    if total_create > 0 || total_drop > 0 {
        tracing::info!(
            "Workspace WAL replay complete: {} creates, {} drops across all shards",
            total_create,
            total_drop,
        );
    }
}

/// Replay MQ WAL records to restore DurableQueueRegistry and apply cursor-rollback.
///
/// Operates on `&mut [ShardSliceInit]` — called single-threaded at boot
/// before shard threads are spawned. No locks needed.
pub fn replay_mq_wal(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
) {
    use std::collections::HashMap;

    use crate::persistence::wal_v3::record::{WalRecord, WalRecordType};
    use crate::storage::stream::StreamId;

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_dir = persistence_dir.join(format!("shard-{}", shard_id));
        if !wal_dir.exists() {
            continue;
        }

        let mut durable_configs: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut ack_count = 0u64;

        let on_command = &mut |record: &WalRecord| match record.record_type {
            WalRecordType::MqCreate => {
                if let Some((queue_key, max_delivery_count)) =
                    crate::mq::wal::decode_mq_create(&record.payload)
                {
                    durable_configs.insert(queue_key, max_delivery_count);
                }
            }
            WalRecordType::MqAck => {
                if crate::mq::wal::decode_mq_ack(&record.payload).is_some() {
                    ack_count += 1;
                }
            }
            _ => {}
        };
        let on_fpi = &mut |_: &WalRecord| {};

        if let Ok(entries) = std::fs::read_dir(&wal_dir) {
            let mut wal_files: Vec<_> = entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
                .map(|e| e.path())
                .collect();
            wal_files.sort();

            for wal_file in &wal_files {
                let _ = crate::persistence::wal_v3::replay::replay_wal_v3_file(
                    wal_file, 0, on_command, on_fpi,
                );
            }
        }

        if !durable_configs.is_empty() {
            let reg = init
                .durable_queue_registry
                .get_or_insert_with(|| Box::new(crate::mq::DurableQueueRegistry::new()));
            for (queue_key_bytes, max_delivery_count) in &durable_configs {
                let key = bytes::Bytes::copy_from_slice(queue_key_bytes);
                let config = crate::mq::DurableStreamConfig::new(key.clone(), *max_delivery_count);
                reg.insert(key, config);
            }
        }

        // Cursor-rollback for each durable queue using db 0.
        for (queue_key_bytes, max_dc) in &durable_configs {
            let key_bytes = bytes::Bytes::copy_from_slice(queue_key_bytes);
            // db 0 is the first database in the slice.
            if let Some(db) = init.databases.get_mut(0) {
                if let Ok(Some(stream)) = db.get_stream_mut(&key_bytes) {
                    stream.durable = true;
                    stream.max_delivery_count = *max_dc;

                    let group_name = bytes::Bytes::from_static(b"__mq_consumers");
                    if let Some(group) = stream.groups.get_mut(&group_name) {
                        if let Some((min_pel_id, _)) = group.pel.iter().next() {
                            let rollback_target = if min_pel_id.seq > 0 {
                                StreamId {
                                    ms: min_pel_id.ms,
                                    seq: min_pel_id.seq - 1,
                                }
                            } else if min_pel_id.ms > 0 {
                                StreamId {
                                    ms: min_pel_id.ms - 1,
                                    seq: u64::MAX,
                                }
                            } else {
                                StreamId::ZERO
                            };

                            tracing::info!(
                                "Shard {}: MQ cursor-rollback for queue {:?}: \
                                 last_delivered_id {}-{} -> {}-{} (PEL size: {})",
                                shard_id,
                                String::from_utf8_lossy(queue_key_bytes),
                                group.last_delivered_id.ms,
                                group.last_delivered_id.seq,
                                rollback_target.ms,
                                rollback_target.seq,
                                group.pel.len(),
                            );

                            group.last_delivered_id = rollback_target;
                        }
                    }
                }
            }
        }

        if !durable_configs.is_empty() {
            tracing::info!(
                "Shard {}: replayed {} MQ queue configs, {} ack records",
                shard_id,
                durable_configs.len(),
                ack_count,
            );
        }
    }
}

/// Recover graph stores from persistence for all shards.
///
/// Operates on `&mut [ShardSliceInit]` — called single-threaded at boot.
#[cfg(feature = "graph")]
pub fn recover_graph_stores(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
) {
    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        match crate::graph::recovery::recover_graph_store(persistence_dir, shard_id) {
            Ok(Some(result)) => {
                if result.store.graph_count() > 0 {
                    tracing::info!(
                        "Shard {}: recovered {} graph(s) ({} segments loaded, {} skipped)",
                        shard_id,
                        result.store.graph_count(),
                        result.segments_loaded,
                        result.segments_skipped,
                    );
                }
                init.graph_store = result.store;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("Shard {}: graph recovery failed: {}", shard_id, e);
            }
        }
    }
}

/// Replay graph WAL commands into graph stores for all shards.
#[cfg(feature = "graph")]
pub fn replay_graph_wal(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
    db_count: usize,
) {
    use crate::persistence::replay::DispatchReplayEngine;
    use crate::persistence::wal;

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_file = wal::wal_path(persistence_dir, shard_id);
        if !wal_file.exists() {
            continue;
        }
        let engine = DispatchReplayEngine::new();
        let mut dummy_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();
        match wal::replay_wal(&mut dummy_dbs, &wal_file, &engine) {
            Ok(_) => {
                let graph_count = engine.graph_command_count();
                if graph_count > 0 {
                    let applied = engine.replay_graph_commands(&mut init.graph_store);
                    tracing::info!(
                        "Shard {}: replayed {} graph WAL commands ({} applied)",
                        shard_id,
                        graph_count,
                        applied,
                    );
                }
            }
            Err(e) => {
                tracing::error!("Shard {}: graph WAL replay failed: {}", shard_id, e);
            }
        }
    }
}

/// Replay temporal WAL records into per-shard TemporalKvIndex and GraphStore.
pub fn replay_temporal_wal(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
) {
    #[cfg(feature = "graph")]
    use crate::persistence::wal_v3::record::decode_graph_temporal;
    use crate::persistence::wal_v3::record::{WalRecord, WalRecordType, decode_temporal_upsert};

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_dir = persistence_dir.join(format!("shard-{}", shard_id));
        if !wal_dir.exists() {
            continue;
        }

        let mut temporal_upsert_count = 0usize;
        #[cfg(feature = "graph")]
        let mut graph_temporal_count = 0usize;

        let on_command = &mut |record: &WalRecord| match record.record_type {
            WalRecordType::TemporalUpsert => {
                if let Some((key, valid_from, _system_from, value)) =
                    decode_temporal_upsert(&record.payload)
                {
                    let idx = init
                        .temporal_kv_index
                        .get_or_insert_with(|| Box::new(crate::temporal::TemporalKvIndex::new()));
                    idx.record(
                        bytes::Bytes::copy_from_slice(key),
                        valid_from,
                        bytes::Bytes::copy_from_slice(value),
                    );
                    temporal_upsert_count += 1;
                }
            }
            #[cfg(feature = "graph")]
            WalRecordType::GraphTemporal => {
                if let Some((entity_id, is_node, valid_to, _system_from)) =
                    decode_graph_temporal(&record.payload)
                {
                    for named_graph in init.graph_store.iter_graphs_mut() {
                        let found = if is_node {
                            let nk: crate::graph::types::NodeKey =
                                slotmap::KeyData::from_ffi(entity_id).into();
                            if let Some(node) = named_graph.write_buf.get_node_mut(nk) {
                                node.valid_to = valid_to;
                                true
                            } else {
                                false
                            }
                        } else {
                            let ek: crate::graph::types::EdgeKey =
                                slotmap::KeyData::from_ffi(entity_id).into();
                            if let Some(edge) = named_graph.write_buf.get_edge_mut(ek) {
                                edge.valid_to = valid_to;
                                true
                            } else {
                                false
                            }
                        };
                        if found {
                            graph_temporal_count += 1;
                            break;
                        }
                    }
                }
            }
            _ => {}
        };
        let on_fpi = &mut |_: &WalRecord| {};

        if let Ok(entries) = std::fs::read_dir(&wal_dir) {
            let mut wal_files: Vec<_> = entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
                .map(|e| e.path())
                .collect();
            wal_files.sort();

            for wal_file in &wal_files {
                let _ = crate::persistence::wal_v3::replay::replay_wal_v3_file(
                    wal_file, 0, on_command, on_fpi,
                );
            }
        }

        #[cfg(feature = "graph")]
        if temporal_upsert_count > 0 || graph_temporal_count > 0 {
            tracing::info!(
                "Shard {}: replayed {} TemporalUpsert + {} GraphTemporal WAL records",
                shard_id,
                temporal_upsert_count,
                graph_temporal_count,
            );
        }
        #[cfg(not(feature = "graph"))]
        if temporal_upsert_count > 0 {
            tracing::info!(
                "Shard {}: replayed {} TemporalUpsert WAL records",
                shard_id,
                temporal_upsert_count,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

    fn new_shared(shard_count: usize, db_per_shard: usize) -> Arc<ShardDatabases> {
        let dbs: Vec<Vec<Database>> = (0..shard_count)
            .map(|_| (0..db_per_shard).map(|_| Database::new()).collect())
            .collect();
        let (shared, _inits) = ShardDatabases::new(dbs);
        shared
    }

    #[test]
    fn test_new_creates_correct_dimensions() {
        let shared = new_shared(3, 2);
        assert_eq!(shared.num_shards(), 3);
        assert_eq!(shared.db_count(), 2);
    }

    #[test]
    fn test_empty_shard_databases() {
        let (shared, inits) = ShardDatabases::new(vec![]);
        assert_eq!(shared.num_shards(), 0);
        assert_eq!(shared.db_count(), 0);
        assert!(inits.is_empty());
    }

    #[test]
    fn test_new_returns_correct_init_count() {
        let dbs = vec![vec![Database::new()], vec![Database::new()]];
        let (shared, inits) = ShardDatabases::new(dbs);
        assert_eq!(shared.num_shards(), 2);
        assert_eq!(inits.len(), 2);
        assert_eq!(inits[0].shard_id, 0);
        assert_eq!(inits[1].shard_id, 1);
    }

    // ── GAP-1: elastic budget publish/recompute ──────────────────────────

    fn rt_config(maxmemory: usize, num_shards: usize) -> crate::config::RuntimeConfig {
        let mut rt = crate::config::RuntimeConfig::default();
        rt.maxmemory = maxmemory;
        rt.num_shards = num_shards;
        rt
    }

    #[test]
    fn elastic_budget_defaults_to_zero_until_recomputed() {
        let shared = new_shared(2, 1);
        assert_eq!(shared.elastic_budget(0), 0);
        assert_eq!(shared.elastic_budget(1), 0);
    }

    #[test]
    fn recompute_elastic_budget_hot_shard_borrows_idle_headroom() {
        let shared = new_shared(4, 1);
        let rt = rt_config(400, 4); // base = 100 per shard
        shared.publish_memory(0, 120); // hot
        shared.publish_memory(1, 10);
        shared.publish_memory(2, 10);
        shared.publish_memory(3, 10);
        // Hot shard borrows (100-10)*3 = 270 → budget 370.
        assert_eq!(shared.recompute_elastic_budget(0, &rt), 370);
        assert_eq!(shared.elastic_budget(0), 370);
        // Idle shards keep base.
        assert_eq!(shared.recompute_elastic_budget(1, &rt), 100);
    }

    #[test]
    fn recompute_elastic_budget_disabled_for_single_shard_or_unlimited() {
        let shared = new_shared(1, 1);
        let rt = rt_config(400, 1);
        assert_eq!(shared.recompute_elastic_budget(0, &rt), 0);

        let shared2 = new_shared(2, 1);
        let unlimited = rt_config(0, 2);
        assert_eq!(shared2.recompute_elastic_budget(0, &unlimited), 0);
        assert_eq!(shared2.elastic_budget(0), 0);
    }
}
