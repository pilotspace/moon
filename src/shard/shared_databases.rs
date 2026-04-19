use std::collections::HashMap;
use std::sync::Arc;

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
    /// Mutex<Option<>> for single-writer init, then read-only via wal_append().
    wal_append_txs: Vec<Mutex<Option<crate::runtime::channel::MpscSender<bytes::Bytes>>>>,
    /// Per-shard TemporalRegistry for wall-clock-to-LSN bindings.
    /// Lazy-init: None until first TEMPORAL.SNAPSHOT_AT call.
    temporal_registries: Vec<Mutex<Option<Box<TemporalRegistry>>>>,
    /// Per-shard TemporalKvIndex for versioned KV reads.
    /// Lazy-init: None until first TemporalUpsert WAL write.
    temporal_kv_indexes: Vec<Mutex<Option<Box<TemporalKvIndex>>>>,
    /// Per-shard WorkspaceRegistry for workspace metadata.
    /// Lazy-init: None until first WS.CREATE call on this shard.
    workspace_registries: Vec<Mutex<Option<Box<WorkspaceRegistry>>>>,
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
        let wal_append_txs = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let temporal_registries = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let temporal_kv_indexes = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let workspace_registries = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let durable_queue_registries = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let trigger_registries = (0..num_shards).map(|_| Mutex::new(None)).collect();
        let kv_write_intents = (0..num_shards)
            .map(|_| Mutex::new(KvWriteIntents::new()))
            .collect();
        let deferred_hnsw_inserts = (0..num_shards)
            .map(|_| Mutex::new(DeferredHnswInserts::new()))
            .collect();
        Arc::new(Self {
            shards,
            vector_stores,
            text_stores,
            #[cfg(feature = "graph")]
            graph_stores,
            wal_append_txs,
            temporal_registries,
            temporal_kv_indexes,
            workspace_registries,
            durable_queue_registries,
            trigger_registries,
            kv_write_intents,
            deferred_hnsw_inserts,
            num_shards,
            db_count,
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
        *self.wal_append_txs[shard_id].lock() = Some(tx);
    }

    /// Send serialized command bytes to the WAL append channel for a shard.
    ///
    /// Called by connection handlers for local write commands. The event loop
    /// drains this channel on the 1ms tick into WAL v2/v3.
    /// No-op when persistence is disabled.
    #[inline]
    pub fn wal_append(&self, shard_id: usize, data: bytes::Bytes) {
        if let Some(ref tx) = *self.wal_append_txs[shard_id].lock() {
            let _ = tx.try_send(data);
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

    /// Acquire the per-shard WorkspaceRegistry lock.
    /// Caller lazy-inits via `get_or_insert_with(|| Box::new(WorkspaceRegistry::new()))`.
    #[inline]
    pub fn workspace_registry(
        &self,
        shard_id: usize,
    ) -> MutexGuard<'_, Option<Box<WorkspaceRegistry>>> {
        self.workspace_registries[shard_id].lock()
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

    /// Replay WAL WorkspaceCreate and WorkspaceDrop records to restore workspace registry.
    ///
    /// Called during server startup after graph and temporal WAL replay.
    /// Scans per-shard WAL directories for v3 segment files and processes
    /// WorkspaceCreate/WorkspaceDrop records to restore workspace metadata.
    pub fn replay_workspace_wal(&self, persistence_dir: &std::path::Path) {
        use crate::persistence::wal_v3::record::{WalRecord, WalRecordType};

        for shard_id in 0..self.num_shards {
            let wal_dir = persistence_dir.join(format!("shard-{}", shard_id));
            if !wal_dir.exists() {
                continue;
            }

            let mut create_count = 0u64;
            let mut drop_count = 0u64;

            let on_command = &mut |record: &WalRecord| {
                match record.record_type {
                    WalRecordType::WorkspaceCreate => {
                        if let Some((ws_bytes, name)) = decode_workspace_create(&record.payload) {
                            let ws_id = WorkspaceId::from_bytes(ws_bytes);
                            let meta = WorkspaceMetadata {
                                id: ws_id,
                                name: bytes::Bytes::from(name),
                                created_at: 0, // WAL doesn't store created_at; use 0 as placeholder
                            };
                            let mut guard = self.workspace_registries[shard_id].lock();
                            let reg =
                                guard.get_or_insert_with(|| Box::new(WorkspaceRegistry::new()));
                            reg.insert(ws_id, meta);
                            create_count += 1;
                        }
                    }
                    WalRecordType::WorkspaceDrop => {
                        if let Some(ws_bytes) = decode_workspace_drop(&record.payload) {
                            let ws_id = WorkspaceId::from_bytes(ws_bytes);
                            let mut guard = self.workspace_registries[shard_id].lock();
                            if let Some(reg) = guard.as_mut() {
                                reg.remove(&ws_id);
                            }
                            drop_count += 1;
                        }
                    }
                    _ => {} // Skip non-workspace records
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

            if create_count > 0 || drop_count > 0 {
                tracing::info!(
                    "Shard {}: replayed {} WorkspaceCreate + {} WorkspaceDrop WAL records",
                    shard_id,
                    create_count,
                    drop_count,
                );
            }
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
    pub fn aggregate_memory(&self, shard_id: usize) -> usize {
        let mut total = 0usize;
        for db_idx in 0..self.db_count {
            let guard = self.read_db(shard_id, db_idx);
            total += guard.estimated_memory();
        }
        total
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

    #[test]
    fn test_empty_shard_databases() {
        let dbs: Vec<Vec<Database>> = vec![];
        let shared = ShardDatabases::new(dbs);
        assert_eq!(shared.num_shards(), 0);
        assert_eq!(shared.db_count(), 0);
    }
}
