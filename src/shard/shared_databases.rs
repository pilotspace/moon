use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "graph")]
use crate::graph::store::GraphStore;
use crate::mq::{DurableQueueRegistry, TriggerRegistry};
use crate::storage::Database;
use crate::temporal::{TemporalKvIndex, TemporalRegistry};
use crate::text::store::TextStore;
use crate::transaction::{DeferredHnswInserts, KvWriteIntents};
use crate::vector::store::VectorStore;
use crate::workspace::wal::{decode_workspace_create, decode_workspace_drop};
use crate::workspace::{WorkspaceId, WorkspaceMetadata, WorkspaceRegistry};

/// Published per-shard store-memory counters (C5 / M4).
///
/// Each shard's 100ms tick writes vector/text/graph resident bytes into these
/// atomics via the existing lock path. Cross-thread observers — Prometheus
/// publisher (metrics_setup.rs) and MEMORY DOCTOR (server_admin.rs) — read
/// from these atomics with zero lock acquisitions.
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

/// Thread-safe wrapper over per-shard databases.
///
/// Each shard owns `db_count` databases (SELECT 0-15). The outer Vec is indexed
/// by shard_id, the inner Vec by db_index. All access goes through `read_db()`
/// (shared) or `write_db()` (exclusive) to enable cross-shard direct reads.
pub struct ShardDatabases {
    shards: Vec<Vec<RwLock<Database>>>,
    /// Per-shard VectorStore for FT.* commands in single-shard mode.
    vector_stores: Vec<Mutex<VectorStore>>,
    /// Per-shard TextStore for full-text search indexes.
    text_stores: Vec<Mutex<TextStore>>,
    /// Per-shard GraphStore for GRAPH.* commands.
    #[cfg(feature = "graph")]
    graph_stores: Vec<RwLock<GraphStore>>,
    /// Per-shard WAL append channel sender. Connection handlers send serialized
    /// write commands here; the event loop drains into WAL v2/v3 on the 1ms tick.
    /// OnceLock: set once at event-loop startup (before connections are
    /// accepted), then every hot-path read is lock-free (QW2, 2026-06 review
    /// finding 1.8 — was Mutex<Option<>>, one lock acquire per write command).
    wal_append_txs: Vec<std::sync::OnceLock<crate::runtime::channel::MpscSender<bytes::Bytes>>>,
    /// Per-shard TemporalRegistry for wall-clock-to-LSN bindings.
    /// Lazy-init: None until first TEMPORAL.SNAPSHOT_AT call.
    temporal_registries: Vec<Mutex<Option<Box<TemporalRegistry>>>>,
    /// Per-shard TemporalKvIndex for versioned KV reads.
    /// Lazy-init: None until first TemporalUpsert WAL write.
    temporal_kv_indexes: Vec<Mutex<Option<Box<TemporalKvIndex>>>>,
    /// Process-global WorkspaceRegistry (C3 / M3).
    ///
    /// Workspaces are control-plane objects looked up by every connection
    /// regardless of which shard accepted it — a single Mutex is not a
    /// hot-path concern (WS commands are rare). The per-shard array
    /// (`workspace_registries`) is retired; all paths use this one field.
    /// WAL records keep the shard-0 stream via `wal_append(0, …)` (unchanged).
    /// Caller lazy-inits via `get_or_insert_with(|| Box::new(WorkspaceRegistry::new()))`.
    workspace_registry: Mutex<Option<Box<WorkspaceRegistry>>>,
    /// Per-shard DurableQueueRegistry for MQ.* commands.
    /// Lazy-init: None until first MQ.CREATE call on this shard.
    durable_queue_registries: Vec<Mutex<Option<Box<DurableQueueRegistry>>>>,
    /// Per-shard TriggerRegistry for MQ.TRIGGER debounced callbacks.
    /// Lazy-init: None until first MQ.TRIGGER call on this shard.
    trigger_registries: Vec<Mutex<Option<Box<TriggerRegistry>>>>,
    /// Per-shard KV write-intent side-table for transactional MVCC.
    /// Shard-global: visible to all connections for cross-txn visibility checks.
    kv_write_intents: Vec<Mutex<KvWriteIntents>>,
    /// Per-shard deferred HNSW insert queue for post-commit processing.
    /// Shard-global: survives connection drops before commit.
    deferred_hnsw_inserts: Vec<Mutex<DeferredHnswInserts>>,
    num_shards: usize,
    db_count: usize,
    /// Per-shard memory publishers for lock-free cross-shard reads.
    ///
    /// Each shard's event loop will call `memory_publisher(shard_id)` once at
    /// startup to clone an `Arc` into `ShardSlice.estimated_memory`. The shard
    /// then atomically publishes its estimated memory after write operations
    /// (Phase 2). Cross-shard readers — maxmemory eviction
    /// (`persistence_tick.rs:436,511`) and metrics scrape
    /// (`metrics_setup.rs:1238`) — call `read_memory_sum()` for a lock-free
    /// sum (Phase 3). Coexists with the existing `aggregate_memory()` path
    /// until Phase 3 switches the eviction tick.
    memory_per_shard: Vec<Arc<AtomicUsize>>,
    /// Per-shard elastic memory budgets (GAP-1 hot-shard pooling).
    ///
    /// Recomputed by each shard's 100ms eviction tick from the
    /// `memory_per_shard` snapshot (`recompute_elastic_budget`). `0` means
    /// "no elastic budget yet" — readers fall back to the static
    /// `maxmemory / num_shards`. Write paths read with one Relaxed load
    /// (`elastic_budget`).
    elastic_budgets: Vec<Arc<AtomicUsize>>,
    /// Per-shard published store-memory atomics (C5 / M4).
    ///
    /// The shard's 100ms tick refreshes these via the existing lock path.
    /// Prometheus publisher and MEMORY DOCTOR read them with zero lock
    /// acquisitions. Figures lag at most one tick (≤100 ms) — acceptable for
    /// observability paths.
    pub store_memory_per_shard: Box<[Arc<ShardStoreMemory>]>,
}

impl ShardDatabases {
    /// Create from pre-restored database vectors (one `Vec<Database>` per shard).
    pub fn new(shard_databases: Vec<Vec<Database>>) -> Arc<Self> {
        let num_shards = shard_databases.len();
        let db_count = shard_databases.first().map_or(0, |v| v.len());
        let shards = shard_databases
            .into_iter()
            .map(|dbs| dbs.into_iter().map(RwLock::new).collect())
            .collect();
        let vector_stores = (0..num_shards)
            .map(|_| Mutex::new(VectorStore::new()))
            .collect();
        let text_stores = (0..num_shards)
            .map(|_| Mutex::new(TextStore::new()))
            .collect();
        #[cfg(feature = "graph")]
        let graph_stores = (0..num_shards)
            .map(|_| RwLock::new(GraphStore::new()))
            .collect();
        let wal_append_txs = (0..num_shards)
            .map(|_| std::sync::OnceLock::new())
            .collect();
        let temporal_registries = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let temporal_kv_indexes = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let workspace_registry = Mutex::new(None);
        let durable_queue_registries = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let trigger_registries = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let kv_write_intents = (0..num_shards)
            .map(|_| Mutex::new(KvWriteIntents::new()))
            .collect();
        let deferred_hnsw_inserts = (0..num_shards)
            .map(|_| Mutex::new(DeferredHnswInserts::new()))
            .collect();
        let memory_per_shard = (0..num_shards)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        let elastic_budgets = (0..num_shards)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        let store_memory_per_shard = (0..num_shards)
            .map(|_| {
                Arc::new(ShardStoreMemory {
                    vector: AtomicUsize::new(0),
                    text: AtomicUsize::new(0),
                    graph: AtomicUsize::new(0),
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(Self {
            shards,
            vector_stores,
            text_stores,
            #[cfg(feature = "graph")]
            graph_stores,
            wal_append_txs,
            temporal_registries,
            temporal_kv_indexes,
            workspace_registry,
            durable_queue_registries,
            trigger_registries,
            kv_write_intents,
            deferred_hnsw_inserts,
            num_shards,
            db_count,
            memory_per_shard,
            elastic_budgets,
            store_memory_per_shard,
        })
    }

    /// Set the WAL append channel sender for a shard.
    ///
    /// Called once during event loop startup. Uses interior mutability via
    /// unsafe transmutation of the Arc — safe because this is called exactly
    /// once per shard before any connections are accepted.
    /// Set the WAL append channel sender for a shard.
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

    /// Acquire exclusive access to a shard's VectorStore.
    #[inline]
    pub fn vector_store(&self, shard_id: usize) -> MutexGuard<'_, VectorStore> {
        self.vector_stores[shard_id].lock()
    }

    /// Acquire exclusive access to a shard's TextStore.
    #[inline]
    pub fn text_store(&self, shard_id: usize) -> MutexGuard<'_, TextStore> {
        self.text_stores[shard_id].lock()
    }

    /// Acquire shared read access to a shard's GraphStore.
    #[cfg(feature = "graph")]
    #[inline]
    pub fn graph_store_read(&self, shard_id: usize) -> RwLockReadGuard<'_, GraphStore> {
        self.graph_stores[shard_id].read()
    }

    /// Acquire exclusive write access to a shard's GraphStore.
    #[cfg(feature = "graph")]
    #[inline]
    pub fn graph_store_write(&self, shard_id: usize) -> RwLockWriteGuard<'_, GraphStore> {
        self.graph_stores[shard_id].write()
    }

    /// Acquire the per-shard TemporalRegistry lock. Caller must lazy-init
    /// via `get_or_insert_with(|| Box::new(TemporalRegistry::new()))`.
    #[inline]
    pub fn temporal_registry(
        &self,
        shard_id: usize,
    ) -> MutexGuard<'_, Option<Box<TemporalRegistry>>> {
        self.temporal_registries[shard_id].lock()
    }

    /// Acquire the per-shard TemporalKvIndex lock. Caller must lazy-init
    /// via `get_or_insert_with(|| Box::new(TemporalKvIndex::new()))`.
    #[inline]
    pub fn temporal_kv_index(
        &self,
        shard_id: usize,
    ) -> MutexGuard<'_, Option<Box<TemporalKvIndex>>> {
        self.temporal_kv_indexes[shard_id].lock()
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

    /// Acquire the per-shard DurableQueueRegistry lock.
    /// Caller lazy-inits via `get_or_insert_with(|| Box::new(DurableQueueRegistry::new()))`.
    #[inline]
    pub fn durable_queue_registry(
        &self,
        shard_id: usize,
    ) -> MutexGuard<'_, Option<Box<DurableQueueRegistry>>> {
        self.durable_queue_registries[shard_id].lock()
    }

    /// Acquire the per-shard TriggerRegistry lock.
    /// Caller lazy-inits via `get_or_insert_with(|| Box::new(TriggerRegistry::new()))`.
    #[inline]
    pub fn trigger_registry(
        &self,
        shard_id: usize,
    ) -> MutexGuard<'_, Option<Box<TriggerRegistry>>> {
        self.trigger_registries[shard_id].lock()
    }

    /// Acquire the per-shard KV write-intent side-table lock.
    /// Used by handler write path (record_write) and read path (is_key_visible).
    #[inline]
    pub fn kv_intents(&self, shard_id: usize) -> MutexGuard<'_, KvWriteIntents> {
        self.kv_write_intents[shard_id].lock()
    }

    /// Acquire the per-shard deferred HNSW insert queue lock.
    /// Used by TXN.COMMIT (drain_for_txn) and TXN.ABORT (discard_for_txn).
    #[inline]
    pub fn hnsw_queue(&self, shard_id: usize) -> MutexGuard<'_, DeferredHnswInserts> {
        self.deferred_hnsw_inserts[shard_id].lock()
    }

    /// Replay WAL WorkspaceCreate and WorkspaceDrop records to restore the
    /// process-global workspace registry (C3 / M3).
    ///
    /// Called during server startup after graph and temporal WAL replay.
    /// Scans `shard-{id}/wal-v3/` (matching recovery.rs:361 convention) for
    /// v3 segment files and processes WorkspaceCreate/WorkspaceDrop records.
    /// All records populate the single global registry regardless of which
    /// shard stream they came from (WS WAL records are always written via
    /// `wal_append(0, …)`, so they live in `shard-0/wal-v3/`).
    pub fn replay_workspace_wal(&self, persistence_dir: &std::path::Path) {
        use crate::persistence::wal_v3::record::{WalRecord, WalRecordType, read_wal_v3_record};

        let mut total_create = 0u64;
        let mut total_drop = 0u64;

        for shard_id in 0..self.num_shards {
            // WAL v3 segments live in shard-{id}/wal-v3/ — matching recovery.rs:361.
            let wal_dir = persistence_dir
                .join(format!("shard-{}", shard_id))
                .join("wal-v3");
            if !wal_dir.exists() {
                continue;
            }

            let mut create_count = 0u64;
            let mut drop_count = 0u64;

            // Process a workspace WAL record payload (either a direct WorkspaceCreate/Drop
            // record OR an inner record embedded in a Command wrapper).
            let mut handle_record = |record_type: WalRecordType, payload: &[u8]| {
                match record_type {
                    WalRecordType::WorkspaceCreate => {
                        if let Some((ws_bytes, name)) = decode_workspace_create(payload) {
                            let ws_id = WorkspaceId::from_bytes(ws_bytes);
                            let meta = WorkspaceMetadata {
                                id: ws_id,
                                name: bytes::Bytes::from(name),
                                created_at: 0, // WAL doesn't store created_at; use 0 as placeholder
                            };
                            // Process-global registry (C3).
                            let mut guard = self.workspace_registry.lock();
                            let reg =
                                guard.get_or_insert_with(|| Box::new(WorkspaceRegistry::new()));
                            reg.insert(ws_id, meta);
                            create_count += 1;
                        }
                    }
                    WalRecordType::WorkspaceDrop => {
                        if let Some(ws_bytes) = decode_workspace_drop(payload) {
                            let ws_id = WorkspaceId::from_bytes(ws_bytes);
                            let mut guard = self.workspace_registry.lock();
                            if let Some(reg) = guard.as_mut() {
                                reg.remove(&ws_id);
                            }
                            drop_count += 1;
                        }
                    }
                    _ => {}
                }
            };

            let on_command = &mut |record: &WalRecord| {
                match record.record_type {
                    WalRecordType::WorkspaceCreate | WalRecordType::WorkspaceDrop => {
                        // Direct workspace record — process it.
                        handle_record(record.record_type, &record.payload);
                    }
                    WalRecordType::Command => {
                        // The connection handler pre-builds a WorkspaceCreate/Drop WAL
                        // record and sends it via wal_append(). The event loop (event_loop.rs,
                        // spsc_handler.rs) then wraps the raw bytes in a Command (0x01) outer
                        // record. So workspace records appear as Command records whose payload
                        // IS a complete inner WAL v3 record.
                        //
                        // Decode the inner record and check if it is a workspace operation.
                        if let Some(inner) = read_wal_v3_record(&record.payload) {
                            match inner.record_type {
                                WalRecordType::WorkspaceCreate | WalRecordType::WorkspaceDrop => {
                                    handle_record(inner.record_type, &inner.payload);
                                }
                                _ => {} // Not a workspace record — skip
                            }
                        }
                    }
                    _ => {} // Skip non-workspace records
                }
            };
            let on_fpi = &mut |_: &WalRecord| {};

            // Scan WAL v3 segment files in the wal-v3 subdirectory.
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
    /// This is the P2 CRITICAL path: after restart, unacknowledged messages must be
    /// redelivered. The cursor-rollback resets `ConsumerGroup.last_delivered_id` to
    /// the last-acked position so that MQ.POP will re-deliver unacked messages.
    ///
    /// **Cursor-rollback algorithm:**
    /// For each durable queue, after WAL scan:
    /// - If PEL is non-empty: rollback target = min(PEL keys) - 1
    ///   (the ID just before the first unacked message)
    /// - If PEL is empty: no rollback needed (all messages were acked)
    ///
    /// This handles out-of-order acks correctly: if messages 1,2,3,4,5 were delivered
    /// but only 1,3,5 were acked, the PEL contains {2,4}. Rollback target = 1
    /// (min(PEL) - 1), so messages 2,3,4,5 are all redelivered.
    pub fn replay_mq_wal(&self, persistence_dir: &std::path::Path) {
        use crate::persistence::wal_v3::record::{WalRecord, WalRecordType};
        use crate::storage::stream::StreamId;

        for shard_id in 0..self.num_shards {
            let wal_dir = persistence_dir.join(format!("shard-{}", shard_id));
            if !wal_dir.exists() {
                continue;
            }

            // Phase 1: Scan WAL to collect MqCreate configs and MqAck positions
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

            // Scan WAL files in the shard directory
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

            // Phase 2: Restore DurableQueueRegistry from MqCreate records
            if !durable_configs.is_empty() {
                let mut guard = self.durable_queue_registries[shard_id].lock();
                let reg =
                    guard.get_or_insert_with(|| Box::new(crate::mq::DurableQueueRegistry::new()));
                for (queue_key_bytes, max_delivery_count) in &durable_configs {
                    let key = bytes::Bytes::copy_from_slice(queue_key_bytes);
                    let config =
                        crate::mq::DurableStreamConfig::new(key.clone(), *max_delivery_count);
                    reg.insert(key, config);
                }
            }

            // Phase 3: Cursor-rollback for each durable queue.
            // The Stream and its __mq_consumers consumer group were already restored
            // from RESP Command WAL records (regular KV replay path).
            // We just need to reset last_delivered_id based on PEL state.
            for (queue_key_bytes, max_dc) in &durable_configs {
                let key_bytes = bytes::Bytes::copy_from_slice(queue_key_bytes);
                let mut db_guard = self.write_db(shard_id, 0);

                // Look up the Stream entry via get_stream_mut
                if let Ok(Some(stream)) = db_guard.get_stream_mut(&key_bytes) {
                    // Mark as durable (may not have been set during Command replay)
                    stream.durable = true;
                    stream.max_delivery_count = *max_dc;

                    // Find the __mq_consumers group
                    let group_name = bytes::Bytes::from_static(b"__mq_consumers");
                    if let Some(group) = stream.groups.get_mut(&group_name) {
                        // Cursor-rollback: if PEL is non-empty, set last_delivered_id
                        // to the ID just before the first unacked message
                        if let Some((min_pel_id, _)) = group.pel.iter().next() {
                            let rollback_target = if min_pel_id.seq > 0 {
                                StreamId {
                                    ms: min_pel_id.ms,
                                    seq: min_pel_id.seq - 1,
                                }
                            } else if min_pel_id.ms > 0 {
                                // Edge case: seq=0, roll back to previous ms with max seq
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
                        // If PEL is empty: all messages were acked, no rollback needed
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
    /// Called once after construction during server startup. Loads graph
    /// metadata and CSR segments from the persistence directory.
    #[cfg(feature = "graph")]
    pub fn recover_graph_stores(&self, persistence_dir: &std::path::Path) {
        for shard_id in 0..self.num_shards {
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
                    *self.graph_stores[shard_id].write() = result.store;
                }
                Ok(None) => {
                    // No graph metadata — clean start, nothing to recover.
                }
                Err(e) => {
                    tracing::error!("Shard {}: graph recovery failed: {}", shard_id, e);
                }
            }
        }
    }

    /// Replay graph WAL commands into graph stores for all shards.
    ///
    /// Called after `recover_graph_stores` during startup. Reads per-shard
    /// WAL files, filters graph commands via `GraphReplayCollector`, and
    /// applies them to each shard's `GraphStore`.
    #[cfg(feature = "graph")]
    pub fn replay_graph_wal(&self, persistence_dir: &std::path::Path) {
        use crate::persistence::replay::DispatchReplayEngine;
        use crate::persistence::wal;

        for shard_id in 0..self.num_shards {
            let wal_file = wal::wal_path(persistence_dir, shard_id);
            if !wal_file.exists() {
                continue;
            }
            // Create a temporary engine to collect graph commands from the WAL.
            let engine = DispatchReplayEngine::new();
            // We need dummy databases for the KV replay path (graph commands
            // are intercepted before KV dispatch, so these are unused).
            let mut dummy_dbs: Vec<Database> =
                (0..self.db_count).map(|_| Database::new()).collect();
            match wal::replay_wal(&mut dummy_dbs, &wal_file, &engine) {
                Ok(_) => {
                    let graph_count = engine.graph_command_count();
                    if graph_count > 0 {
                        let mut gs = self.graph_stores[shard_id].write();
                        let applied = engine.replay_graph_commands(&mut gs);
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
    ///
    /// Called at startup after graph recovery. TemporalUpsert (KV) records are
    /// always replayed regardless of the `graph` feature. GraphTemporal records
    /// are only processed when the `graph` feature is enabled.
    pub fn replay_temporal_wal(&self, persistence_dir: &std::path::Path) {
        #[cfg(feature = "graph")]
        use crate::persistence::wal_v3::record::decode_graph_temporal;
        use crate::persistence::wal_v3::record::{
            WalRecord, WalRecordType, decode_temporal_upsert,
        };

        for shard_id in 0..self.num_shards {
            let wal_dir = persistence_dir.join(format!("shard-{}", shard_id));
            if !wal_dir.exists() {
                continue;
            }

            let mut temporal_upsert_count = 0usize;
            #[cfg(feature = "graph")]
            let mut graph_temporal_count = 0usize;

            let on_command = &mut |record: &WalRecord| {
                match record.record_type {
                    WalRecordType::TemporalUpsert => {
                        if let Some((key, valid_from, _system_from, value)) =
                            decode_temporal_upsert(&record.payload)
                        {
                            let mut guard = self.temporal_kv_indexes[shard_id].lock();
                            let idx = guard.get_or_insert_with(|| {
                                Box::new(crate::temporal::TemporalKvIndex::new())
                            });
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
                            let mut gs = self.graph_stores[shard_id].write();
                            for named_graph in gs.iter_graphs_mut() {
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
                    _ => {} // Other record types handled by their respective replay paths
                }
            };
            let on_fpi = &mut |_: &WalRecord| {};

            // Scan WAL files in the shard directory
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

    /// Acquire a shared read lock on a specific database.
    #[inline]
    pub fn read_db(&self, shard_id: usize, db_index: usize) -> RwLockReadGuard<'_, Database> {
        debug_assert!(
            shard_id < self.shards.len(),
            "shard_id {shard_id} out of bounds ({})",
            self.shards.len()
        );
        debug_assert!(
            db_index < self.shards[shard_id].len(),
            "db_index {db_index} out of bounds ({})",
            self.shards[shard_id].len()
        );
        self.shards[shard_id][db_index].read()
    }

    /// Acquire an exclusive write lock on a specific database.
    ///
    /// Fast path: bounded spin with `try_write()` for the common case where
    /// cross-shard read locks are held briefly (~51ns for DashTable lookup).
    /// Slow path: falls back to blocking `write()` (OS-level parking) to avoid
    /// infinite busy-spin when readers hold locks longer (e.g., KEYS, SCAN).
    #[inline]
    pub fn write_db(&self, shard_id: usize, db_index: usize) -> RwLockWriteGuard<'_, Database> {
        debug_assert!(
            shard_id < self.shards.len(),
            "shard_id {shard_id} out of bounds ({})",
            self.shards.len()
        );
        debug_assert!(
            db_index < self.shards[shard_id].len(),
            "db_index {db_index} out of bounds ({})",
            self.shards[shard_id].len()
        );
        // Spin briefly — resolves in 1-3 iterations for typical cross-shard reads.
        for _ in 0..32 {
            if let Some(guard) = self.shards[shard_id][db_index].try_write() {
                return guard;
            }
            std::hint::spin_loop();
        }
        // Slow path: park the thread via OS futex instead of busy-spinning.
        self.shards[shard_id][db_index].write()
    }

    /// Non-blocking write lock attempt for async callers that can yield.
    #[inline]
    pub fn try_write_db(
        &self,
        shard_id: usize,
        db_index: usize,
    ) -> Option<RwLockWriteGuard<'_, Database>> {
        self.shards[shard_id][db_index].try_write()
    }

    /// Total number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Return a reference to all databases across all shards (for AOF rewrite).
    /// Callers iterate shards × dbs and acquire read locks individually.
    #[inline]
    pub fn all_shard_dbs(&self) -> &[Vec<RwLock<Database>>] {
        &self.shards
    }

    /// Number of databases per shard (typically 16).
    #[inline]
    pub fn db_count(&self) -> usize {
        self.db_count
    }

    /// Aggregate estimated memory across all databases in a shard.
    ///
    /// Acquires read locks briefly on each DB. Used for maxmemory eviction
    /// decisions (Redis maxmemory is a server-wide limit, not per-DB).
    ///
    /// Phase 3 will replace call sites with `read_memory_sum()` to eliminate
    /// these lock acquisitions. Both methods coexist during the transition.
    pub fn aggregate_memory(&self, shard_id: usize) -> usize {
        let mut total = 0usize;
        for db_idx in 0..self.db_count {
            let guard = self.read_db(shard_id, db_idx);
            total += guard.estimated_memory();
        }
        total
    }

    /// Get the per-shard memory publisher `Arc`.
    ///
    /// Called once per shard at startup when constructing the `ShardSlice`.
    /// The returned `Arc` is cloned into `ShardSlice.estimated_memory`; the
    /// master copy lives here in `memory_per_shard[shard_id]`.
    ///
    /// Phase 2 write paths will store into this atomic after mutations.
    /// Phase 3 eviction and metrics paths will read via `read_memory_sum()`.
    #[inline]
    pub fn memory_publisher(&self, shard_id: usize) -> Arc<AtomicUsize> {
        self.memory_per_shard[shard_id].clone()
    }

    /// Publish a shard's aggregate memory usage (GAP-1 / Phase 2 wiring).
    ///
    /// Called from the shard's 100ms eviction tick with the freshly computed
    /// `aggregate_memory(shard_id)` value. One Relaxed store — siblings read
    /// the snapshot in `recompute_elastic_budget`.
    #[inline]
    pub fn publish_memory(&self, shard_id: usize, used: usize) {
        self.memory_per_shard[shard_id].store(used, Ordering::Relaxed);
    }

    /// Recompute and publish this shard's elastic memory budget (GAP-1).
    ///
    /// Reads every shard's published usage (N Relaxed loads, once per 100ms
    /// tick) and derives this shard's budget via
    /// [`crate::storage::eviction::compute_elastic_budget`]: under-budget
    /// siblings donate their headroom, hot shards split it evenly, and the
    /// snapshot aggregate never exceeds `maxmemory`. Returns the budget it
    /// stored. Single-shard or unlimited deployments publish `0`
    /// ("use static budget") so readers take the unchanged fallback path.
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
        // Snapshot of every shard's published usage. SmallVec would save the
        // alloc, but this runs 10×/s on a background tick — a Vec is fine.
        let used: Vec<usize> = self
            .memory_per_shard
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect();
        let budget = crate::storage::eviction::compute_elastic_budget(shard_id, base, &used);
        self.elastic_budgets[shard_id].store(budget, Ordering::Relaxed);
        budget
    }

    /// This shard's current elastic budget; `0` = none published yet (use
    /// the static `maxmemory / num_shards`). One Relaxed load — cheap enough
    /// for the per-write eviction check.
    #[inline]
    pub fn elastic_budget(&self, shard_id: usize) -> usize {
        self.elastic_budgets[shard_id].load(Ordering::Relaxed)
    }

    /// Sum all per-shard memory publishers with `Relaxed` loads. Lock-free.
    ///
    /// Returns a best-effort estimate of total server memory across all shards.
    /// `Relaxed` ordering is intentional: the eviction tick and metrics scrape
    /// do not require cross-thread synchronization with write operations — they
    /// only need a recent-enough value for threshold decisions. The error
    /// introduced by stale atomics is far smaller than the headroom kept below
    /// `maxmemory`.
    ///
    /// Phase 3 will switch `persistence_tick.rs:436,511` and
    /// `metrics_setup.rs:1238` from `aggregate_memory()` to this method.
    #[inline]
    pub fn read_memory_sum(&self) -> usize {
        self.memory_per_shard
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum()
    }

    /// Collect snapshot metadata (segment counts, base timestamps) for a shard.
    ///
    /// Acquires brief read locks on each database to gather metadata needed
    /// by `SnapshotState::new_from_metadata`. Called once at snapshot epoch start.
    pub fn snapshot_metadata(&self, shard_id: usize) -> (Vec<usize>, Vec<u32>) {
        let mut segment_counts = Vec::with_capacity(self.db_count);
        let mut base_timestamps = Vec::with_capacity(self.db_count);
        for i in 0..self.db_count {
            let guard = self.read_db(shard_id, i);
            segment_counts.push(guard.data().segment_count());
            base_timestamps.push(guard.base_timestamp());
        }
        (segment_counts, base_timestamps)
    }

    /// Atomically swap two databases within a single shard.
    ///
    /// Acquires write locks in ascending index order (lower index first) to
    /// prevent deadlock if two concurrent SWAPDB calls swap the same pair from
    /// opposite directions.  `std::mem::swap` exchanges the `Database` values
    /// in-place while both locks are held.
    ///
    /// # Panics
    ///
    /// Panics (debug_assert) if `shard_id`, `a`, or `b` are out of bounds.
    /// Callers must validate indices before calling.
    pub fn swap_dbs(&self, shard_id: usize, a: usize, b: usize) {
        debug_assert!(shard_id < self.shards.len(), "shard_id out of bounds");
        debug_assert!(a < self.db_count, "db index a out of bounds");
        debug_assert!(b < self.db_count, "db index b out of bounds");
        // Same-index swap is a no-op; short-circuit in release builds to
        // avoid self-deadlocking on the second write() acquire (parking_lot
        // RwLock is not reentrant). Callers normally short-circuit earlier,
        // but defending here is cheap and prevents a stall on the SPSC path.
        if a == b {
            return;
        }

        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        // Acquire in ascending index order to prevent deadlock.
        let mut guard_lo = self.shards[shard_id][lo].write();
        let mut guard_hi = self.shards[shard_id][hi].write();
        std::mem::swap(&mut *guard_lo, &mut *guard_hi);
    }

    /// Read the last published KV memory for a single shard with one Relaxed
    /// load. Lock-free. Returns the value written by the most recent
    /// `publish_memory(shard_id, …)` call — zero until the first 100ms tick.
    ///
    /// Used by the eviction tick (persistence_tick.rs) as a lock-free
    /// replacement for `aggregate_memory(shard_id)` in pressure-check paths
    /// (C5 / Phase 3).
    #[inline]
    pub fn published_shard_memory(&self, shard_id: usize) -> usize {
        self.memory_per_shard[shard_id].load(Ordering::Relaxed)
    }

    /// Return a clone of the `Arc<ShardStoreMemory>` for `shard_id`.
    ///
    /// Called once at shard startup to hand the `Arc` into `ShardSliceInit`.
    /// The master copy lives in `store_memory_per_shard`; the slice holds a
    /// second owner and calls `store()` on the atomics on its 100ms tick.
    #[inline]
    pub fn store_memory_publisher(&self, shard_id: usize) -> Arc<ShardStoreMemory> {
        self.store_memory_per_shard[shard_id].clone()
    }

    /// Publish this shard's vector/text/graph memory usage into the per-shard
    /// atomics (C5).
    ///
    /// Called from the shard's 100ms persistence/elastic tick AFTER acquiring
    /// the store locks (existing lock path — Wave E will collapse to slice).
    /// Observers (metrics, MEMORY DOCTOR) read these atomics without locks.
    pub fn publish_store_memory(&self, shard_id: usize) {
        // VectorStore: sum mutable + immutable resident bytes.
        {
            let vs = self.vector_stores[shard_id].lock();
            let (mutable, immutable) = vs.resident_bytes();
            self.store_memory_per_shard[shard_id]
                .vector
                .store(mutable + immutable, Ordering::Relaxed);
        }

        // TextStore: TextStore has no aggregate resident_bytes API yet.
        // Observers (metrics, MEMORY DOCTOR) do not currently report text
        // memory, so publishing 0 is a safe placeholder until a store-level
        // memory accessor is added (Wave E).
        self.store_memory_per_shard[shard_id]
            .text
            .store(0, Ordering::Relaxed);

        // GraphStore: resident bytes (cfg-gated; zero when graph feature off).
        #[cfg(feature = "graph")]
        {
            let gs = self.graph_stores[shard_id].read();
            self.store_memory_per_shard[shard_id]
                .graph
                .store(gs.resident_bytes(), Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_correct_dimensions() {
        let dbs = vec![
            vec![Database::new(), Database::new()],
            vec![Database::new(), Database::new()],
            vec![Database::new(), Database::new()],
        ];
        let shared = ShardDatabases::new(dbs);
        assert_eq!(shared.num_shards(), 3);
        assert_eq!(shared.db_count(), 2);
    }

    #[test]
    fn test_read_db_returns_shared_guard() {
        let dbs = vec![vec![Database::new()]];
        let shared = ShardDatabases::new(dbs);
        let _guard1 = shared.read_db(0, 0);
        let _guard2 = shared.read_db(0, 0); // Multiple readers OK
    }

    #[test]
    fn test_write_db_returns_exclusive_guard() {
        let dbs = vec![vec![Database::new()]];
        let shared = ShardDatabases::new(dbs);
        let _guard = shared.write_db(0, 0);
        // Exclusive access -- would deadlock if we tried another write_db here
    }

    #[test]
    fn test_cross_shard_access() {
        let dbs = vec![vec![Database::new()], vec![Database::new()]];
        let shared = ShardDatabases::new(dbs);
        // Can read from different shards concurrently
        let _guard0 = shared.read_db(0, 0);
        let _guard1 = shared.read_db(1, 0);
    }

    #[test]
    fn test_snapshot_metadata() {
        let dbs = vec![vec![Database::new(), Database::new()]];
        let shared = ShardDatabases::new(dbs);
        let (seg_counts, base_ts) = shared.snapshot_metadata(0);
        assert_eq!(seg_counts.len(), 2);
        assert_eq!(base_ts.len(), 2);
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
        let shared = ShardDatabases::new(vec![vec![Database::new()], vec![Database::new()]]);
        assert_eq!(shared.elastic_budget(0), 0);
        assert_eq!(shared.elastic_budget(1), 0);
    }

    #[test]
    fn recompute_elastic_budget_hot_shard_borrows_idle_headroom() {
        let shared = ShardDatabases::new(vec![
            vec![Database::new()],
            vec![Database::new()],
            vec![Database::new()],
            vec![Database::new()],
        ]);
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
        let shared = ShardDatabases::new(vec![vec![Database::new()]]);
        let rt = rt_config(400, 1);
        assert_eq!(shared.recompute_elastic_budget(0, &rt), 0);

        let shared2 = ShardDatabases::new(vec![vec![Database::new()], vec![Database::new()]]);
        let unlimited = rt_config(0, 2);
        assert_eq!(shared2.recompute_elastic_budget(0, &unlimited), 0);
        assert_eq!(shared2.elastic_budget(0), 0);
    }

    #[test]
    fn test_empty_shard_databases() {
        let dbs: Vec<Vec<Database>> = vec![];
        let shared = ShardDatabases::new(dbs);
        assert_eq!(shared.num_shards(), 0);
        assert_eq!(shared.db_count(), 0);
    }

    // ── T2.1 SWAPDB: swap_dbs unit tests ─────────────────────────────────────

    /// Insert a string key into a Database via cmd_dispatch so we don't need
    /// to know internal storage layout.
    fn db_set_key(db: &mut Database, key: &[u8], value: &[u8]) {
        use crate::protocol::Frame;
        let mut selected = 0usize;
        let args = crate::framevec![
            Frame::BulkString(bytes::Bytes::copy_from_slice(key)),
            Frame::BulkString(bytes::Bytes::copy_from_slice(value)),
        ];
        let _ = crate::command::dispatch(db, b"SET", &args, &mut selected, 16);
    }

    #[test]
    fn test_swap_dbs_exchanges_contents() {
        // Shard 0 has 2 databases.  Put "key_a" in db-0 and "key_b" in db-1.
        let mut db0 = Database::new();
        let db1 = Database::new();
        db_set_key(&mut db0, b"key_a", b"val_a");

        // Put key_b in db-1 using a temporary binding.
        let mut db1_tmp = db1;
        db_set_key(&mut db1_tmp, b"key_b", b"val_b");

        let shared = ShardDatabases::new(vec![vec![db0, db1_tmp]]);
        assert_eq!(
            shared.read_db(0, 0).len(),
            1,
            "db-0 should have 1 key before swap"
        );
        assert_eq!(
            shared.read_db(0, 1).len(),
            1,
            "db-1 should have 1 key before swap"
        );

        shared.swap_dbs(0, 0, 1);

        // After swap: db-0 should have key_b, db-1 should have key_a.
        assert_eq!(
            shared.read_db(0, 0).len(),
            1,
            "db-0 should still have 1 key after swap"
        );
        assert_eq!(
            shared.read_db(0, 1).len(),
            1,
            "db-1 should still have 1 key after swap"
        );

        // Verify key moved: db-0 now has the key from the former db-1.
        let mut dummy_selected = 0usize;
        let get_args = crate::framevec![crate::protocol::Frame::BulkString(
            bytes::Bytes::from_static(b"key_b")
        )];
        {
            let mut guard = shared.write_db(0, 0);
            let result =
                crate::command::dispatch(&mut *guard, b"GET", &get_args, &mut dummy_selected, 16);
            match result {
                crate::command::DispatchResult::Response(crate::protocol::Frame::BulkString(v)) => {
                    assert_eq!(v.as_ref(), b"val_b", "db-0 should have val_b after swap");
                }
                crate::command::DispatchResult::Response(_) => {
                    panic!("expected BulkString(val_b) for key_b in db-0 after swap");
                }
                crate::command::DispatchResult::Quit(_) => {
                    panic!("unexpected Quit result");
                }
            }
        }
    }

    #[test]
    fn test_swap_dbs_reverse_order_same_result() {
        // Swapping (1, 0) should produce the same result as (0, 1).
        let mut db0 = Database::new();
        let db1 = Database::new();
        db_set_key(&mut db0, b"alpha", b"a");

        let shared = ShardDatabases::new(vec![vec![db0, db1]]);
        assert_eq!(shared.read_db(0, 0).len(), 1);
        assert_eq!(shared.read_db(0, 1).len(), 0);

        shared.swap_dbs(0, 1, 0); // reversed argument order

        assert_eq!(
            shared.read_db(0, 0).len(),
            0,
            "db-0 should be empty after swap"
        );
        assert_eq!(
            shared.read_db(0, 1).len(),
            1,
            "db-1 should have 1 key after swap"
        );
    }
}
