use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Mutex, MutexGuard};
use smallvec::SmallVec;

use crate::persistence::wal_v3::record::WalRecordType;
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
    /// Approximate resident bytes of the shard's Lua `ScriptCache` (C4 wave-5
    /// hygiene). The cache itself stays unbounded (Redis parity -- `SCRIPT
    /// FLUSH` is the only eviction path); this is observability only.
    pub lua: AtomicUsize,
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
    /// Per-shard WAL append channel sender. Connection handlers send
    /// `(WalRecordType, payload)` pairs here — the caller's REAL record type,
    /// unframed — and the event loop drains into WAL v3 on the 1ms tick via
    /// `wal.append(record_type, &payload)` (K1a: no more nested-Command
    /// re-wrapping). OnceLock: set once at event-loop startup (before
    /// connections are accepted), then every hot-path read is lock-free.
    wal_append_txs: Vec<
        std::sync::OnceLock<crate::runtime::channel::MpscSender<(WalRecordType, bytes::Bytes)>>,
    >,
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
                    lua: AtomicUsize::new(0),
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
        tx: crate::runtime::channel::MpscSender<(WalRecordType, bytes::Bytes)>,
    ) {
        if self.wal_append_txs[shard_id].set(tx).is_err() {
            tracing::warn!(
                shard_id,
                "wal_append_tx already initialized; re-init ignored (OnceLock)"
            );
        }
    }

    /// Send a WAL record to the append channel for a shard, tagged with its
    /// REAL `record_type` (K1a). The event loop's 1ms-tick drain calls
    /// `wal.append(record_type, &data)` directly — no nested-Command
    /// re-framing, so replay sees the record's true outer type.
    /// No-op when persistence is disabled.
    #[inline]
    pub fn wal_append(&self, shard_id: usize, record_type: WalRecordType, data: bytes::Bytes) {
        if let Some(tx) = self.wal_append_txs[shard_id].get() {
            let _ = tx.try_send((record_type, data));
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
    pub fn try_wal_append_required(
        &self,
        shard_id: usize,
        record_type: WalRecordType,
        data: bytes::Bytes,
    ) -> bool {
        match self.wal_append_txs[shard_id].get() {
            Some(tx) => tx.try_send((record_type, data)).is_ok(),
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
        // SmallVec: most deployments run <=16 shards, so this 100ms-tick
        // snapshot stays fully on the stack; only larger shard counts spill
        // to a single heap allocation (still one per call, same as before).
        //
        // A4 (accounting spine, tiering-v2 D3): each shard's used-term is
        // KV + published vector resident bytes. A vector-heavy/KV-light
        // shard was misclassified as an idle donor — it lent headroom to
        // siblings while its true footprint was already over base, and the
        // pressure cascade then compared a vector-INCLUSIVE used against a
        // budget inflated by that donation. Two Relaxed loads per shard.
        let used: SmallVec<[usize; 16]> = self
            .memory_per_shard
            .iter()
            .zip(self.store_memory_per_shard.iter())
            .map(|(kv, store)| {
                kv.load(Ordering::Relaxed)
                    .saturating_add(store.vector.load(Ordering::Relaxed))
            })
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
                if let Some((ws_bytes, name, created_at)) = decode_workspace_create(payload) {
                    let ws_id = WorkspaceId::from_bytes(ws_bytes);
                    let meta = WorkspaceMetadata {
                        id: ws_id,
                        name: bytes::Bytes::from(name),
                        created_at,
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
///
/// WAL v3 segments for a shard live in `shard-{id}/wal-v3/` (matching
/// `replay_workspace_wal` / `replay_graph_wal` / `recovery.rs`) — NOT
/// directly under `shard-{id}/`, which `std::fs::read_dir` (non-recursive)
/// would silently scan as empty. Every cross-thread `wal_append` blob is
/// also re-wrapped as `WalRecordType::Command` by the shard event-loop
/// drain (`event_loop.rs`), so `MqCreate`/`MqAck` records are nested inside
/// a Command payload on disk; `handle_record` below is shared by both the
/// direct-type arm (for any legacy/self-written records) and the
/// Command-unwrap arm, mirroring `replay_workspace_wal`.
pub fn replay_mq_wal(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
) {
    use std::collections::HashMap;

    use crate::persistence::wal_v3::record::{WalRecord, WalRecordType, read_wal_v3_record};
    use crate::storage::stream::StreamId;

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_dir = persistence_dir
            .join(format!("shard-{}", shard_id))
            .join("wal-v3");
        if !wal_dir.exists() {
            continue;
        }

        let mut durable_configs: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut ack_count = 0u64;

        let mut handle_record = |record_type: WalRecordType, payload: &[u8]| match record_type {
            WalRecordType::MqCreate => {
                if let Some((queue_key, max_delivery_count)) =
                    crate::mq::wal::decode_mq_create(payload)
                {
                    durable_configs.insert(queue_key, max_delivery_count);
                }
            }
            WalRecordType::MqAck => {
                if crate::mq::wal::decode_mq_ack(payload).is_some() {
                    ack_count += 1;
                }
            }
            _ => {}
        };

        let on_command = &mut |record: &WalRecord| match record.record_type {
            WalRecordType::MqCreate | WalRecordType::MqAck => {
                handle_record(record.record_type, &record.payload);
            }
            WalRecordType::Command => {
                if let Some(inner) = read_wal_v3_record(&record.payload) {
                    match inner.record_type {
                        WalRecordType::MqCreate | WalRecordType::MqAck => {
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
///
/// Pre-1.0 WAL-v3-only format freeze: ported from the deleted WAL v2 flat
/// file (`shard-{id}.wal`) to the v3 segment directory
/// (`shard-{id}/wal-v3/*.wal`, matching `replay_workspace_wal` /
/// `replay_temporal_wal` above and `recovery.rs`). `Command` records are
/// forwarded to `DispatchReplayEngine::replay_command` exactly as
/// `replay_wal_auto`'s v3 branch does, so this stays bug-for-bug consistent
/// with the rest of the WAL v3 command-replay path.
#[cfg(feature = "graph")]
pub fn replay_graph_wal(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
    db_count: usize,
) {
    use crate::persistence::replay::{CommandReplayEngine, DispatchReplayEngine};
    use crate::persistence::wal_v3::record::{WalRecord, WalRecordType};

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_dir = persistence_dir
            .join(format!("shard-{}", shard_id))
            .join("wal-v3");
        if !wal_dir.exists() {
            continue;
        }

        let engine = DispatchReplayEngine::new();
        let mut dummy_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();
        let mut selected_db = 0usize;
        let on_command = &mut |record: &WalRecord| {
            if record.record_type == WalRecordType::Command {
                engine.replay_command(&mut dummy_dbs, &record.payload, &[], &mut selected_db);
            }
        };
        let on_fpi = &mut |_record: &WalRecord| {};

        let mut wal_files: Vec<_> = match std::fs::read_dir(&wal_dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
                .map(|e| e.path())
                .collect(),
            Err(e) => {
                tracing::error!("Shard {}: graph WAL dir read failed: {}", shard_id, e);
                continue;
            }
        };
        wal_files.sort();

        for wal_file in &wal_files {
            if let Err(e) = crate::persistence::wal_v3::replay::replay_wal_v3_file(
                wal_file, 0, on_command, on_fpi,
            ) {
                tracing::error!("Shard {}: graph WAL replay failed: {}", shard_id, e);
            }
        }

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
}

/// Replay graph commands from the WAL **v3** directory into graph stores
/// (2026-07 graph durability P0, Bug A).
///
/// Under the default disk-offload configuration graph WAL records live in
/// `<offload_base>/shard-<N>/wal-v3/` — NOT the legacy `shard-<N>.wal` that
/// [`replay_graph_wal`] reads. `Shard::restore_from_persistence`'s v3 replay
/// collects graph commands into a throwaway engine (its job is KV), so this
/// dedicated pass re-scans the v3 WAL and applies graph records to
/// `init.graph_store`.
///
/// Records with `lsn <= GraphStore::snapshot_lsn()` are skipped: they are
/// already materialized in the CSR segments persisted by the checkpoint's
/// graph snapshot (`persist_graph_at_checkpoint`, Bug B fix), and the WAL
/// segments holding them may have been recycled anyway. The scan starts at
/// LSN 0 rather than the KV checkpoint floor — graph coverage is governed
/// by the GRAPH snapshot floor, never the KV one.
#[cfg(feature = "graph")]
pub fn replay_graph_wal_v3(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    disk_offload_base: &std::path::Path,
) {
    use crate::persistence::wal_v3::record::{WalRecord, WalRecordType};
    use crate::persistence::wal_v3::replay::replay_wal_v3_dir;

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_dir = disk_offload_base
            .join(format!("shard-{shard_id}"))
            .join("wal-v3");
        if !wal_dir.exists() {
            continue;
        }
        let floor = init.graph_store.snapshot_lsn();
        let mut collector = crate::graph::replay::GraphReplayCollector::new();
        let mut skipped_covered = 0usize;
        let on_command = &mut |record: &WalRecord| {
            if record.record_type != WalRecordType::Command {
                return;
            }
            let mut buf = bytes::BytesMut::from(&record.payload[..]);
            let parse_cfg = crate::protocol::ParseConfig::default();
            while let Ok(Some(frame)) = crate::protocol::parse::parse(&mut buf, &parse_cfg) {
                let crate::protocol::Frame::Array(ref arr) = frame else {
                    continue;
                };
                let Some(first) = arr.first() else { continue };
                let cmd_name: &[u8] = match first {
                    crate::protocol::Frame::BulkString(s) => s.as_ref(),
                    crate::protocol::Frame::SimpleString(s) => s.as_ref(),
                    _ => continue,
                };
                if !crate::graph::replay::GraphReplayCollector::is_graph_command(cmd_name) {
                    continue;
                }
                if record.lsn <= floor {
                    skipped_covered += 1;
                    continue;
                }
                // Args may mix BulkString and Integer frames (ids); the
                // collector expects all-text slices.
                let owned: smallvec::SmallVec<[Vec<u8>; 8]> = arr[1..]
                    .iter()
                    .filter_map(|f| match f {
                        crate::protocol::Frame::BulkString(b) => Some(b.to_vec()),
                        crate::protocol::Frame::Integer(i) => Some(i.to_string().into_bytes()),
                        _ => None,
                    })
                    .collect();
                let refs: smallvec::SmallVec<[&[u8]; 8]> =
                    owned.iter().map(|v| v.as_slice()).collect();
                if !collector.collect_command(cmd_name, &refs) {
                    tracing::warn!(
                        "graph WAL v3 replay: malformed {:?} record at LSN {} — skipping",
                        String::from_utf8_lossy(cmd_name),
                        record.lsn,
                    );
                }
            }
        };
        let on_fpi = &mut |_record: &WalRecord| {};
        match replay_wal_v3_dir(&wal_dir, 0, on_command, on_fpi) {
            Ok(_) => {
                let collected = collector.command_count();
                if collected > 0 || skipped_covered > 0 {
                    let applied = collector.replay_into(&mut init.graph_store);
                    tracing::info!(
                        "Shard {}: graph WAL v3 replay — {} command(s) applied, \
                         {} covered by snapshot_lsn={} (skipped)",
                        shard_id,
                        applied,
                        skipped_covered,
                        floor,
                    );
                }
            }
            Err(e) => {
                tracing::error!("Shard {}: graph WAL v3 replay failed: {}", shard_id, e);
            }
        }
    }
}

/// Replay temporal WAL records into per-shard TemporalKvIndex and GraphStore.
///
/// WAL v3 segments for a shard live in `shard-{id}/wal-v3/` (matching
/// `replay_workspace_wal` / `replay_graph_wal` / `recovery.rs`) — NOT
/// directly under `shard-{id}/`, which `std::fs::read_dir` (non-recursive)
/// would silently scan as empty. Every cross-thread `wal_append` blob is
/// also re-wrapped as `WalRecordType::Command` by the shard event-loop
/// drain (`event_loop.rs`), so `TemporalUpsert`/`GraphTemporal` records are
/// nested inside a Command payload on disk; `handle_record` below is
/// shared by both the direct-type arm (for any legacy/self-written
/// records) and the Command-unwrap arm, mirroring `replay_workspace_wal`.
pub fn replay_temporal_wal(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
) {
    #[cfg(feature = "graph")]
    use crate::persistence::wal_v3::record::decode_graph_temporal;
    use crate::persistence::wal_v3::record::{
        WalRecord, WalRecordType, decode_temporal_upsert, read_wal_v3_record,
    };

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_dir = persistence_dir
            .join(format!("shard-{}", shard_id))
            .join("wal-v3");
        if !wal_dir.exists() {
            continue;
        }

        let mut temporal_upsert_count = 0usize;
        #[cfg(feature = "graph")]
        let mut graph_temporal_count = 0usize;

        let mut handle_record = |record_type: WalRecordType, payload: &[u8]| match record_type {
            WalRecordType::TemporalUpsert => {
                if let Some((key, valid_from, _system_from, value)) =
                    decode_temporal_upsert(payload)
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
                    decode_graph_temporal(payload)
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

        let on_command = &mut |record: &WalRecord| match record.record_type {
            WalRecordType::TemporalUpsert => {
                handle_record(record.record_type, &record.payload);
            }
            #[cfg(feature = "graph")]
            WalRecordType::GraphTemporal => {
                handle_record(record.record_type, &record.payload);
            }
            WalRecordType::Command => {
                // Try the documented nested-Command unwrap FIRST (the
                // convention `replay_workspace_wal`/`replay_mq_wal` use, and
                // the ONLY framing `command::temporal::apply_invalidate`
                // produces since it started pre-framing GraphTemporal records
                // with `write_wal_v3_record` — matching how MQ's
                // `mq_exec.rs::handle_create`/`handle_ack` already pre-frame
                // MqCreate/MqAck). `read_wal_v3_record` validates a CRC32C
                // over the inner record, so a false-positive here requires a
                // CRC collision, not a byte-pattern coincidence. Matching on
                // the nested type (rather than a separate "did we handle it"
                // bool) doubles as the dispatch to the legacy fallback below:
                // the `_` arm covers BOTH "no valid nested record at all" and
                // "a nested record of some other type" — either way, a
                // pre-framed NEW TemporalUpsert/GraphTemporal record is
                // already fully handled by the arms above and never reaches
                // the fallback.
                match read_wal_v3_record(&record.payload) {
                    Some(inner) if inner.record_type == WalRecordType::TemporalUpsert => {
                        handle_record(inner.record_type, &inner.payload);
                    }
                    #[cfg(feature = "graph")]
                    Some(inner) if inner.record_type == WalRecordType::GraphTemporal => {
                        handle_record(inner.record_type, &inner.payload);
                    }
                    _ => {
                        // Legacy fallback for WAL segments written BEFORE
                        // `apply_invalidate` pre-framed its record: those
                        // pushed the RAW `encode_graph_temporal` bytes
                        // directly as the Command payload (verified against
                        // real on-disk bytes: exactly the 25-byte
                        // `[entity_id:8][is_node:1][valid_to:8][system_from:8]`
                        // layout, no nested record header/CRC).
                        //
                        // The old discriminator (`payload[0] != b'*'`, i.e.
                        // "doesn't look like a RESP array") was WRONG: it
                        // misread any legitimate record whose entity_id's low
                        // byte was 0x2A ('*') as a RESP command and silently
                        // dropped the invalidation.
                        // `decode_graph_temporal_legacy_raw` replaces it with
                        // decode-time sanity checks (is_node byte must be
                        // literally 0/1, both timestamps must be plausible)
                        // instead of a single leading-byte guess. This does
                        // not fully eliminate the ambiguity — a CRC-free
                        // 25-byte blob can in principle still coincide with a
                        // real RESP payload that also passes the sanity gate
                        // — but that residual window only affects WAL
                        // segments written before this fix ships, and closes
                        // as those segments recycle via normal WAL/checkpoint
                        // rotation.
                        #[cfg(feature = "graph")]
                        if crate::persistence::wal_v3::record::decode_graph_temporal_legacy_raw(
                            &record.payload,
                        )
                        .is_some()
                        {
                            handle_record(WalRecordType::GraphTemporal, &record.payload);
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

    /// Accounting-spine A4 (tiering-v2 D3): the donor/hot classification must
    /// see vector resident bytes. A vector-heavy/KV-light shard was
    /// misclassified as an idle donor — it lent per-shard headroom to
    /// siblings while its true resident footprint (KV + vector) was already
    /// over base, inflating the budget the pressure cascade later compares a
    /// vector-INCLUSIVE used-term against. RED until `recompute_elastic_budget`
    /// adds the published vector bytes to its `used` snapshot.
    #[test]
    fn recompute_elastic_budget_vector_heavy_shard_not_donor() {
        let shared = new_shared(4, 1);
        let rt = rt_config(400, 4); // base = 100 per shard

        shared.publish_memory(0, 120); // hot
        shared.publish_memory(1, 10); // KV-light...
        shared.store_memory_per_shard[1]
            .vector
            .store(200, Ordering::Relaxed); // ...but 200 of vector RAM
        shared.publish_memory(2, 10);
        shared.publish_memory(3, 10);

        // KV-blind math: shard 1 looks idle (10 < 100) and donates 90 —
        // surplus 270, one hot shard ⇒ shard 0 budget 370.
        // Vector-aware: shard 1's true used is 210 > base — it is HOT, not
        // a donor. Surplus = 90 + 90 (shards 2,3), split across the two hot
        // shards ⇒ 100 + 180/2 = 190 each.
        assert_eq!(
            shared.recompute_elastic_budget(0, &rt),
            190,
            "vector-heavy shard must not be classified as an idle donor"
        );
        assert_eq!(
            shared.recompute_elastic_budget(1, &rt),
            190,
            "the vector-heavy shard itself is hot and shares the pool"
        );
        // True idle shards keep base.
        assert_eq!(shared.recompute_elastic_budget(2, &rt), 100);
    }

    #[test]
    fn recompute_elastic_budget_correct_beyond_smallvec_inline_capacity() {
        // The per-call `used` snapshot is a `SmallVec<[usize; 16]>` — pin
        // correctness both inline (<=16 shards, covered above) and once
        // spilled to the heap (>16 shards) so the container swap never
        // silently truncates or reorders shard readings.
        const N: usize = 20;
        let shared = new_shared(N, 1);
        let rt = rt_config(N * 100, N); // base = 100 per shard
        shared.publish_memory(0, 150); // hot
        for i in 1..N {
            shared.publish_memory(i, 10);
        }
        // Hot shard borrows (100-10)*19 = 1710 -> budget 1810.
        assert_eq!(shared.recompute_elastic_budget(0, &rt), 1810);
        // An idle shard keeps base.
        assert_eq!(shared.recompute_elastic_budget(5, &rt), 100);
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

    // ── task #42 (P0 data loss): replay_mq_wal dir-join + nested-Command
    // unwrap ─────────────────────────────────────────────────────────────
    //
    // Why this is a real-file, function-level test rather than a live
    // spawned-server round trip (the `replay_workspace_wal`/`replay_temporal_wal`
    // model in tests/shardslice_live.rs / tests/crash_recovery_temporal_mq.rs):
    // proving `replay_mq_wal` end to end through a real server requires the
    // underlying `Stream` to survive the restart at all. It does NOT, in ANY
    // config that also produces the WAL v3 file this function reads:
    //   - WAL v3 is only initialized when `appendonly_enabled` is true
    //     (event_loop.rs's `wal_shard_dir` gate) — so proving this fix live
    //     needs `--appendonly yes`.
    //   - With `--appendonly yes`, `main.rs`'s multi-part AOF replay is
    //     unconditionally authoritative: it `db.clear()`s every database and
    //     rebuilds solely from the AOF manifest, for BOTH the single-shard and
    //     PerShard (`--shards >= 2`) layouts (main.rs ~1246-1253, ~1307-1315).
    //     MQ.CREATE/PUSH/POP never log to the AOF (`mq_exec.rs` bypasses
    //     `cmd_dispatch` entirely — confirmed by spawning a real server,
    //     BGSAVE-ing after MQ.CREATE, and finding `TYPE <queue>` => `none`
    //     after a restart despite the `.rrdshard` snapshot having reported
    //     "loaded 1 keys from snapshot"). So the Stream this function is
    //     supposed to re-mark `durable` on is wiped before replay ever runs,
    //     regardless of this fix. That gap is a separate, deeper MQ
    //     persistence hole (MQ mutations have no AOF durability at all) —
    //     out of scope for this task's minimal dir/unwrap fix, and NOT
    //     something `--wal-kv-log` or any existing flag works around.
    // This test instead proves the two named defects directly: write a REAL
    // wal-v3 segment (via the same `WalWriterV3`/`write_wal_v3_record` calls
    // `mq_exec.rs::handle_create`/`handle_ack` make, nested exactly as the
    // shard event-loop drain leaves them on disk — verified byte-for-byte
    // against a live-captured segment), seed a `Stream` shaped like a
    // `.rrdshard`-restored one (`durable: false`, default `max_delivery_count`,
    // a pending PEL entry), call `replay_mq_wal`, and assert the registry +
    // stream + cursor are restored.
    fn write_mq_wal_records(
        dir: &std::path::Path,
        shard_id: usize,
        inner_records: &[(crate::persistence::wal_v3::record::WalRecordType, Vec<u8>)],
    ) {
        use crate::persistence::wal_v3::record::{WalRecordType, write_wal_v3_record};
        use crate::persistence::wal_v3::segment::WalWriterV3;

        let wal_dir = dir.join(format!("shard-{}", shard_id)).join("wal-v3");
        let mut writer =
            WalWriterV3::new(shard_id, &wal_dir, 16 * 1024 * 1024).expect("create WalWriterV3");
        for (rtype, payload) in inner_records {
            let mut inner_buf = Vec::new();
            write_wal_v3_record(&mut inner_buf, 0, *rtype, payload);
            writer.append(WalRecordType::Command, &inner_buf);
        }
        writer.flush_sync().expect("flush wal segment");
    }

    #[test]
    fn test_replay_mq_wal_restores_registry_and_rolls_back_cursor() {
        use crate::mq::wal::{encode_mq_ack, encode_mq_create};
        use crate::persistence::wal_v3::record::WalRecordType;
        use crate::storage::stream::{PendingEntry, StreamId};

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"orders".to_vec();
        let group_name = bytes::Bytes::from_static(b"__mq_consumers");

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);
        {
            let db = inits[0].databases.get_mut(0).expect("db 0");
            let stream = db.get_or_create_stream(&queue_key).expect("create stream");
            stream
                .create_group(group_name.clone(), StreamId::ZERO)
                .expect("create group");
            assert!(
                !stream.durable,
                "sanity: a .rrdshard-restored stream starts non-durable \
                 (rdb.rs's TYPE_STREAM body has no durable/max_delivery_count bytes)"
            );
            let group = stream.groups.get_mut(&group_name).expect("group");
            // Simulate a crash mid-delivery: one message claimed but not yet
            // acked survived (via the KV plane) in the PEL.
            group.last_delivered_id = StreamId { ms: 100, seq: 5 };
            group.pel.insert(
                StreamId { ms: 100, seq: 5 },
                PendingEntry {
                    consumer: bytes::Bytes::from_static(b"__mq_default"),
                    delivery_time: 0,
                    delivery_count: 1,
                },
            );
        }

        // Real WAL v3 bytes, nested exactly as the shard event-loop drain
        // produces them (bug (b): a naive `record.record_type` match never
        // sees these, since the outer type is always Command).
        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, encode_mq_create(&queue_key, 7)),
                (WalRecordType::MqAck, encode_mq_ack(&queue_key, 100, 5)),
            ],
        );

        // Written at `<dir>/shard-0/wal-v3/...` (bug (a): the pre-fix
        // function scanned `<dir>/shard-0/` directly, a non-recursive
        // `read_dir` over an otherwise-empty directory).
        replay_mq_wal(&mut inits, tmp.path());

        let reg = inits[0]
            .durable_queue_registry
            .as_ref()
            .expect("MqCreate record must populate durable_queue_registry");
        let config = reg
            .get(&queue_key)
            .expect("queue must be registered after replay");
        assert_eq!(config.max_delivery_count, 7);

        let db = inits[0].databases.get_mut(0).expect("db 0");
        let stream = db
            .get_stream_mut(&queue_key)
            .expect("get_stream_mut")
            .expect("stream must still exist");
        assert!(
            stream.durable,
            "replay must restore durable=true from the MqCreate WAL record"
        );
        assert_eq!(stream.max_delivery_count, 7);

        let group = stream.groups.get(&group_name).expect("group");
        assert_eq!(
            group.last_delivered_id,
            StreamId { ms: 100, seq: 4 },
            "cursor-rollback must rewind last_delivered_id to just before the \
             sole PEL entry (ms=100,seq=5) so MQ.POP redelivers it"
        );
    }

    #[test]
    fn test_replay_mq_wal_missing_wal_v3_subdir_is_a_noop() {
        // Regression guard for bug (a): files sitting directly under
        // `shard-{id}/` (the pre-fix scan path) must NOT be picked up by the
        // fixed function -- it must look under `shard-{id}/wal-v3/` only.
        // A stray file at the old (wrong) location must not be misread as a
        // WAL v3 segment (`replay_wal_v3_file` would reject it, but the
        // directory must not even be scanned).
        let tmp = tempfile::tempdir().expect("tempdir");
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).expect("mkdir shard-0");
        std::fs::write(shard_dir.join("000000000001.wal"), b"not a real segment")
            .expect("write stray file");

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);
        replay_mq_wal(&mut inits, tmp.path());

        assert!(
            inits[0].durable_queue_registry.is_none(),
            "no shard-0/wal-v3/ directory exists -- replay must be a no-op, \
             not scan shard-0/ directly"
        );
    }

    // ── CodeRabbit review finding 2 (PR #286): replay_temporal_wal's
    // GraphTemporal legacy-raw-fallback discriminator ───────────────────────
    //
    // Pre-fix, the raw (unframed) GraphTemporal Command payload was
    // discriminated from a RESP command payload by `payload[0] != b'*'` --
    // WRONG whenever the entity_id's low byte happened to be 0x2A ('*'),
    // which silently dropped the invalidation on replay. The fix:
    // (a) `apply_invalidate` now pre-frames NEW records with
    //     `write_wal_v3_record` (WalRecordType::GraphTemporal), so the
    //     existing nested-Command unwrap recovers them unambiguously by
    //     type tag + CRC -- no first-byte guessing needed.
    // (b) the raw-legacy fallback (still needed for WAL segments written
    //     before this fix) is gated by `decode_graph_temporal_legacy_raw`'s
    //     decode-time sanity checks instead of the first-byte guess.

    #[cfg(feature = "graph")]
    fn write_temporal_wal_command(dir: &std::path::Path, shard_id: usize, command_payload: &[u8]) {
        use crate::persistence::wal_v3::record::WalRecordType;
        use crate::persistence::wal_v3::segment::WalWriterV3;

        let wal_dir = dir.join(format!("shard-{}", shard_id)).join("wal-v3");
        let mut writer =
            WalWriterV3::new(shard_id, &wal_dir, 16 * 1024 * 1024).expect("create WalWriterV3");
        writer.append(WalRecordType::Command, command_payload);
        writer.flush_sync().expect("flush wal segment");
    }

    #[cfg(feature = "graph")]
    fn seed_node(
        inits: &mut [crate::shard::slice::ShardSliceInit],
        graph_name: &[u8],
        entity_id: u64,
    ) {
        let gs = &mut inits[0].graph_store;
        gs.create_graph(bytes::Bytes::copy_from_slice(graph_name), 1000, 1)
            .expect("create_graph");
        let named = gs.get_graph_mut(graph_name).expect("graph exists");
        named.write_buf.add_node_with_id(
            entity_id,
            smallvec::SmallVec::new(),
            smallvec::SmallVec::new(),
            None,
            1,
        );
    }

    #[cfg(feature = "graph")]
    fn node_valid_to(
        inits: &mut [crate::shard::slice::ShardSliceInit],
        graph_name: &[u8],
        entity_id: u64,
    ) -> i64 {
        let nk: crate::graph::types::NodeKey = slotmap::KeyData::from_ffi(entity_id).into();
        let gs = &mut inits[0].graph_store;
        let named = gs.get_graph_mut(graph_name).expect("graph exists");
        named
            .write_buf
            .get_node_mut(nk)
            .expect("node exists")
            .valid_to
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_replay_temporal_wal_legacy_raw_0x2a_entity_low_byte_is_applied() {
        // RED (pre-fix): entity_id's low byte is 0x2A ('*') -- the OLD
        // `payload[0] != b'*'` discriminator misread this legitimate raw
        // GraphTemporal record as a RESP command and skipped it, leaving
        // valid_to stuck at the default i64::MAX.
        let tmp = tempfile::tempdir().expect("tempdir");
        let graph_name = b"tg".as_slice();
        let entity_id: u64 = (1u64 << 32) | 0x2A; // version=1 (odd/valid), index low byte = 0x2A
        let valid_to: i64 = 1_700_000_000_123;
        let system_from: i64 = 1_700_000_000_000;

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);
        seed_node(&mut inits, graph_name, entity_id);

        let raw_payload = crate::persistence::wal_v3::record::encode_graph_temporal(
            entity_id,
            true,
            valid_to,
            system_from,
        );
        assert_eq!(
            raw_payload.first(),
            Some(&b'*'),
            "sanity: this entity_id must reproduce the reported 0x2A collision \
             (test setup, not the assertion under test)"
        );
        write_temporal_wal_command(tmp.path(), 0, &raw_payload);

        replay_temporal_wal(&mut inits, tmp.path());

        assert_eq!(
            node_valid_to(&mut inits, graph_name, entity_id),
            valid_to,
            "legacy raw GraphTemporal record must be applied even when its \
             entity_id's low byte is 0x2A"
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_replay_temporal_wal_framed_producer_record_roundtrips() {
        // GREEN: a NEW record, framed the way `apply_invalidate` frames it
        // post-fix (write_wal_v3_record(GraphTemporal) nested inside the
        // outer Command the event-loop drain always wraps local writes in).
        let tmp = tempfile::tempdir().expect("tempdir");
        let graph_name = b"tg2".as_slice();
        let entity_id: u64 = (1u64 << 32) | 7;
        let valid_to: i64 = 1_777_777_777_777;
        let system_from: i64 = 1_777_777_777_000;

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);
        seed_node(&mut inits, graph_name, entity_id);

        let inner_payload = crate::persistence::wal_v3::record::encode_graph_temporal(
            entity_id,
            true,
            valid_to,
            system_from,
        );
        let mut framed = Vec::new();
        crate::persistence::wal_v3::record::write_wal_v3_record(
            &mut framed,
            0,
            crate::persistence::wal_v3::record::WalRecordType::GraphTemporal,
            &inner_payload,
        );
        write_temporal_wal_command(tmp.path(), 0, &framed);

        replay_temporal_wal(&mut inits, tmp.path());

        assert_eq!(
            node_valid_to(&mut inits, graph_name, entity_id),
            valid_to,
            "a pre-framed GraphTemporal record must replay via the nested-Command unwrap"
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_replay_temporal_wal_rejects_resp_shaped_25_byte_payload() {
        // Adversarial: a 25-byte payload that opens with a byte matching the
        // old (now-removed) `b'*'` legacy discriminator, mimicking a RESP
        // command payload, must NOT be misdecoded into a spurious
        // invalidation. `is_node` (byte 8) is 0xFF -- neither 0 nor 1 --
        // which `decode_graph_temporal_legacy_raw`'s sanity gate rejects
        // regardless of the rest of the bytes.
        let tmp = tempfile::tempdir().expect("tempdir");
        let graph_name = b"tg3".as_slice();
        let entity_id: u64 = (1u64 << 32) | 99;

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);
        seed_node(&mut inits, graph_name, entity_id);

        let mut payload = vec![0u8; 25];
        payload[0] = b'*';
        payload[8] = 0xFF; // invalid is_node discriminant -- must be rejected
        write_temporal_wal_command(tmp.path(), 0, &payload);

        replay_temporal_wal(&mut inits, tmp.path());

        assert_eq!(
            node_valid_to(&mut inits, graph_name, entity_id),
            i64::MAX,
            "a RESP-shaped 25-byte payload with an invalid is_node byte must \
             never be misdecoded into a spurious invalidation"
        );
    }
}
