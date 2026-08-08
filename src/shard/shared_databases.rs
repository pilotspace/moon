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
    /// Resident bytes of this shard's `PageCache` frame buffers (task #58,
    /// LOW-2): `PageCache::resident_buffer_bytes()` -- num 4KB frames actually
    /// grown * 4096 + num 64KB frames actually grown * 65536. Observability
    /// only: NOT read by any eviction or budget-gating path.
    pub pagecache: AtomicUsize,
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
                    pagecache: AtomicUsize::new(0),
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
        //
        // K4 (kernel-m2-brief-2026-07-12 stage 2): extend the same fix to
        // text (FTS) and graph resident bytes — a text- or graph-heavy/
        // KV-light shard was exactly as misclassifiable as an idle donor as
        // the vector case above (same mechanism, different plane). KV's own
        // ColdIndex bytes are already folded into `memory_per_shard` at the
        // publish site (persistence_tick.rs), so they flow through here for
        // free. Four Relaxed loads per shard.
        let used: SmallVec<[usize; 16]> = self
            .memory_per_shard
            .iter()
            .zip(self.store_memory_per_shard.iter())
            .map(|(kv, store)| {
                kv.load(Ordering::Relaxed)
                    .saturating_add(store.vector.load(Ordering::Relaxed))
                    .saturating_add(store.text.load(Ordering::Relaxed))
                    .saturating_add(store.graph.load(Ordering::Relaxed))
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

/// Fixed consumer-group / consumer names used by every MQ.POP (mirrors
/// `src/shard/mq_exec.rs`'s hardcoded constants — MQ.* never exposes custom
/// group/consumer names, unlike the generic XREADGROUP API).
const MQ_GROUP_NAME: &[u8] = b"__mq_consumers";
const MQ_CONSUMER_NAME: &[u8] = b"__mq_default";

/// Replay-time counters for `replay_mq_wal`, logged once per shard.
#[derive(Default)]
struct MqReplayStats {
    create: u64,
    push: u64,
    pop: u64,
    ack: u64,
    trigger: u64,
    drop: u64,
    skipped: u64,
}

impl MqReplayStats {
    fn total(&self) -> u64 {
        self.create + self.push + self.pop + self.ack + self.trigger + self.drop
    }
}

/// Log a skip-and-warn for a malformed or unsupported-version MQ WAL record.
/// Never aborts the replay scan — one bad record just doesn't get applied.
fn warn_skip_mq_record(shard_id: usize, kind: &str, payload: &[u8], stats: &mut MqReplayStats) {
    stats.skipped += 1;
    match crate::mq::wal::peek_version(payload) {
        Some(v) if v > crate::mq::wal::MQ_WAL_VERSION => {
            tracing::warn!(
                "Shard {}: {} WAL record has unsupported version {} (this build \
                 supports {}) — skipping, replay continues",
                shard_id,
                kind,
                v,
                crate::mq::wal::MQ_WAL_VERSION,
            );
        }
        _ => {
            tracing::warn!(
                "Shard {}: {} WAL record is malformed — skipping, replay continues",
                shard_id,
                kind,
            );
        }
    }
}

/// Shared field accessors for the MQ apply engine, implemented by both the
/// boot-time WAL-replay target (`ShardSliceInit`, single-threaded, pre-shard-
/// spawn) and the live-replica-apply target (`ShardSlice`, the running
/// shard's thread-local aggregate) — Wave B stage 2b. `durable_queue_registry`
/// / `trigger_registry` / `databases` are identically named and typed on
/// both structs (see `shard::slice`), so the trait is a thin adapter that
/// lets `apply_mq_*` stay generic instead of existing twice: `replay_mq_wal`
/// (boot) and `replication::apply::apply_mq` (live stream) share ONE engine.
pub(crate) trait MqApplyTarget {
    fn mq_databases_mut(&mut self) -> &mut [crate::storage::Database];
    fn mq_durable_queue_registry_mut(
        &mut self,
    ) -> &mut Option<Box<crate::mq::DurableQueueRegistry>>;
    fn mq_trigger_registry_mut(&mut self) -> &mut Option<Box<crate::mq::TriggerRegistry>>;
}

impl MqApplyTarget for crate::shard::slice::ShardSliceInit {
    #[inline]
    fn mq_databases_mut(&mut self) -> &mut [crate::storage::Database] {
        &mut self.databases
    }
    #[inline]
    fn mq_durable_queue_registry_mut(
        &mut self,
    ) -> &mut Option<Box<crate::mq::DurableQueueRegistry>> {
        &mut self.durable_queue_registry
    }
    #[inline]
    fn mq_trigger_registry_mut(&mut self) -> &mut Option<Box<crate::mq::TriggerRegistry>> {
        &mut self.trigger_registry
    }
}

impl MqApplyTarget for crate::shard::slice::ShardSlice {
    #[inline]
    fn mq_databases_mut(&mut self) -> &mut [crate::storage::Database] {
        &mut self.databases
    }
    #[inline]
    fn mq_durable_queue_registry_mut(
        &mut self,
    ) -> &mut Option<Box<crate::mq::DurableQueueRegistry>> {
        &mut self.durable_queue_registry
    }
    #[inline]
    fn mq_trigger_registry_mut(&mut self) -> &mut Option<Box<crate::mq::TriggerRegistry>> {
        &mut self.trigger_registry
    }
}

/// Apply a single decoded MqCreate record: registers the shard-level durable
/// queue config AND (re)creates the stream in the record's OWN db (fixes the
/// pre-K2 db-0 hardcode — MqCreate now carries `db_index`).
pub(crate) fn apply_mq_create<T: MqApplyTarget>(
    target: &mut T,
    db_index: usize,
    key: &[u8],
    max_delivery_count: u32,
) {
    let key_bytes = bytes::Bytes::copy_from_slice(key);
    let reg = target
        .mq_durable_queue_registry_mut()
        .get_or_insert_with(|| Box::new(crate::mq::DurableQueueRegistry::new()));
    reg.insert(
        key_bytes.clone(),
        crate::mq::DurableStreamConfig::new(key_bytes, max_delivery_count),
    );

    if let Some(db) = target.mq_databases_mut().get_mut(db_index) {
        if let Ok(stream) = db.get_or_create_stream(key) {
            stream.durable = true;
            stream.max_delivery_count = max_delivery_count;
            let group_name = bytes::Bytes::from_static(MQ_GROUP_NAME);
            // Idempotent: `create_group` errs BUSYGROUP if already created by
            // an earlier replayed record (or a surviving RDB/rrdshard load);
            // that's expected and fine, nothing else to do.
            let _ = stream.create_group(group_name, crate::storage::stream::StreamId::ZERO);
        }
    }
}

/// Apply a single decoded MqPush record: re-inserts the entry at its
/// ORIGINAL assigned id. Idempotent — replaying the same id twice (e.g. a
/// stream that already had this entry via a surviving RDB/rrdshard load) is
/// a harmless no-op rather than double-counting `length`.
pub(crate) fn apply_mq_push<T: MqApplyTarget>(
    target: &mut T,
    db_index: usize,
    key: &[u8],
    id: crate::storage::stream::StreamId,
    fields: Vec<(bytes::Bytes, bytes::Bytes)>,
) {
    if let Some(db) = target.mq_databases_mut().get_mut(db_index) {
        if let Ok(stream) = db.get_or_create_stream(key) {
            if !stream.entries.contains_key(&id) {
                stream.add(id, fields);
            }
        }
    }
}

/// Apply a single decoded MqPop record: rebuilds the consumer group's PEL
/// entries for every claimed id, restores `last_delivered_id`, and replays
/// any DLQ routing (removing the source id from the PEL and re-pushing it
/// into the sibling DLQ stream at its ORIGINAL assigned id). Field content
/// for DLQ pushes is looked up from the main stream's `entries` map, which
/// by this point already reflects every MqPush record replayed so far
/// (records are applied strictly in WAL order).
pub(crate) fn apply_mq_pop<T: MqApplyTarget>(
    target: &mut T,
    db_index: usize,
    key: &[u8],
    last_delivered: crate::storage::stream::StreamId,
    claimed: Vec<(crate::storage::stream::StreamId, u64)>,
    dlq: Vec<(
        crate::storage::stream::StreamId,
        crate::storage::stream::StreamId,
    )>,
    // task #47: WAL v2 `MqPop` carries the ORIGINAL `delivery_time`/
    // `seen_time` captured at claim time. A v1 payload (or a captured `0`,
    // which should never legitimately happen post-epoch) decodes these as
    // sentinel `0` -- resolved HERE, at apply time, to "now" so a replayed
    // PEL entry reports a plausible idle time instead of "maximally idle
    // since epoch" (the pre-task-#47 behavior this replaces).
    delivery_time_ms: u64,
    seen_time_ms: u64,
) {
    use crate::storage::entry::current_time_ms;
    use crate::storage::stream::{Consumer, PendingEntry};

    let delivery_time = if delivery_time_ms != 0 {
        delivery_time_ms
    } else {
        current_time_ms()
    };
    let seen_time = if seen_time_ms != 0 {
        seen_time_ms
    } else {
        current_time_ms()
    };

    let Some(db) = target.mq_databases_mut().get_mut(db_index) else {
        return;
    };

    let dlq_pushes: Vec<(
        crate::storage::stream::StreamId,
        Vec<(bytes::Bytes, bytes::Bytes)>,
    )> = match db.get_stream_mut(key) {
        Ok(Some(stream)) => {
            // Look up DLQ field content FIRST (reads of `stream.entries`)
            // before taking a mutable borrow of `stream.groups` below —
            // keeps the borrow shape unambiguous rather than relying on
            // interleaved disjoint-field access.
            let dlq_fields: Vec<Vec<(bytes::Bytes, bytes::Bytes)>> = dlq
                .iter()
                .map(|(src_id, _)| stream.entries.get(src_id).cloned().unwrap_or_default())
                .collect();

            let group_name = bytes::Bytes::from_static(MQ_GROUP_NAME);
            let consumer_name = bytes::Bytes::from_static(MQ_CONSUMER_NAME);
            if !stream.groups.contains_key(group_name.as_ref()) {
                // Should already exist from MqCreate replay; defensively
                // create it so a truncated/out-of-order WAL can't panic.
                let _ =
                    stream.create_group(group_name.clone(), crate::storage::stream::StreamId::ZERO);
            }

            match stream.groups.get_mut(group_name.as_ref()) {
                Some(group) => {
                    for (id, delivery_count) in &claimed {
                        group.pel.insert(
                            *id,
                            PendingEntry {
                                consumer: consumer_name.clone(),
                                delivery_time,
                                delivery_count: *delivery_count,
                            },
                        );
                        let consumer =
                            group
                                .consumers
                                .entry(consumer_name.clone())
                                .or_insert_with(|| Consumer {
                                    name: consumer_name.clone(),
                                    pending: std::collections::BTreeMap::new(),
                                    seen_time,
                                });
                        consumer.pending.insert(*id, ());
                    }
                    group.last_delivered_id = last_delivered;

                    let mut collected = Vec::with_capacity(dlq.len());
                    for ((src_id, dlq_id), fields) in dlq.iter().zip(dlq_fields) {
                        group.pel.remove(src_id);
                        if let Some(c) = group.consumers.get_mut(consumer_name.as_ref()) {
                            c.pending.remove(src_id);
                        }
                        collected.push((*dlq_id, fields));
                    }
                    collected
                }
                None => Vec::new(),
            }
        }
        _ => Vec::new(),
    };

    if !dlq_pushes.is_empty() {
        let mut dlq_key = Vec::with_capacity(key.len() + 8);
        dlq_key.extend_from_slice(key);
        dlq_key.extend_from_slice(b"::mq:dlq");
        if let Ok(dlq_stream) = db.get_or_create_stream(&dlq_key) {
            for (dlq_id, fields) in dlq_pushes {
                if !dlq_stream.entries.contains_key(&dlq_id) {
                    dlq_stream.add(dlq_id, fields);
                }
            }
        }
    }
}

/// Apply a single decoded MqAck record BY ID (not count) — the task #34 fix
/// for the old cursor-rollback-by-count heuristic. `Stream::xack` is
/// idempotent (no-ops if the id isn't in the PEL), so replaying the same ack
/// twice or acking an id that was never actually pending is harmless.
pub(crate) fn apply_mq_ack<T: MqApplyTarget>(
    target: &mut T,
    db_index: usize,
    key: &[u8],
    id: crate::storage::stream::StreamId,
) {
    if let Some(db) = target.mq_databases_mut().get_mut(db_index) {
        if let Ok(Some(stream)) = db.get_stream_mut(key) {
            let group_name = bytes::Bytes::from_static(MQ_GROUP_NAME);
            let _ = stream.xack(&group_name, &[id]);
        }
    }
}

/// Apply a single decoded MqTrigger record: registers the trigger as opaque
/// data. Registration is durable/replayed but NEVER fired during replay —
/// firing only happens from live `MQ.PUSH` debounce arming
/// (`src/shard/timers.rs::fire_pending_mq_triggers`).
pub(crate) fn apply_mq_trigger<T: MqApplyTarget>(
    target: &mut T,
    trig_key: Vec<u8>,
    queue_key: Vec<u8>,
    callback_cmd: Vec<u8>,
    debounce_ms: u64,
) {
    let entry = crate::mq::TriggerEntry {
        queue_key: bytes::Bytes::from(queue_key),
        callback_cmd: bytes::Bytes::from(callback_cmd),
        debounce_ms,
        last_fire_ms: 0,
        pending_fire_ms: 0,
    };
    let reg = target
        .mq_trigger_registry_mut()
        .get_or_insert_with(|| Box::new(crate::mq::TriggerRegistry::new()));
    reg.register(bytes::Bytes::from(trig_key), entry);
}

/// Apply a single decoded MqDrop record: tombstones a durable MQ stream
/// deleted via generic keyspace `DEL`/`UNLINK`/`FLUSHDB`/`FLUSHALL` (kernel
/// M3 stage 3 / task #46). Removes the registry entry (by key — the
/// registry itself carries no db index, mirroring `apply_mq_create`'s
/// existing db-lax registry design) and deletes the stream from the
/// record's own db. Idempotent: a key with no registry entry / no stream is
/// a harmless no-op (e.g. replaying a Drop for a stream a prior malformed
/// record already failed to (re)create).
///
/// # Ordering
///
/// Applied strictly in WAL order alongside every other MQ record (task #46
/// review): a `Drop` only undoes records that precede it for this key — a
/// LATER `MqCreate` for the same key re-materializes normally, so
/// create -> drop -> create survives a kill-9 with the second incarnation
/// intact.
pub(crate) fn apply_mq_drop<T: MqApplyTarget>(target: &mut T, db_index: usize, key: &[u8]) {
    if let Some(reg) = target.mq_durable_queue_registry_mut().as_mut() {
        reg.remove(key);
    }
    if let Some(db) = target.mq_databases_mut().get_mut(db_index) {
        let _ = db.remove_counting_cold(key);
    }
}

/// Decode + dispatch one MQ WAL record to its `apply_mq_*` handler,
/// skip-and-warn on any decode failure (malformed payload OR an
/// unsupported/future version byte).
fn apply_mq_wal_record(
    init: &mut crate::shard::slice::ShardSliceInit,
    shard_id: usize,
    record_type: crate::persistence::wal_v3::record::WalRecordType,
    payload: &[u8],
    stats: &mut MqReplayStats,
) {
    use crate::persistence::wal_v3::record::WalRecordType;
    use crate::storage::stream::StreamId;

    match record_type {
        WalRecordType::MqCreate => match crate::mq::wal::decode_mq_create(payload) {
            Some((db_index, key, mdc)) => {
                apply_mq_create(init, db_index as usize, &key, mdc);
                stats.create += 1;
            }
            None => warn_skip_mq_record(shard_id, "MqCreate", payload, stats),
        },
        WalRecordType::MqPush => match crate::mq::wal::decode_mq_push(payload) {
            Some((db_index, key, ms, seq, fields)) => {
                apply_mq_push(init, db_index as usize, &key, StreamId { ms, seq }, fields);
                stats.push += 1;
            }
            None => warn_skip_mq_record(shard_id, "MqPush", payload, stats),
        },
        WalRecordType::MqPop => match crate::mq::wal::decode_mq_pop(payload) {
            Some((db_index, key, last_delivered, claimed, dlq, delivery_time_ms, seen_time_ms)) => {
                let claimed: Vec<(StreamId, u64)> = claimed
                    .into_iter()
                    .map(|(ms, seq, dc)| (StreamId { ms, seq }, dc))
                    .collect();
                let dlq: Vec<(StreamId, StreamId)> = dlq
                    .into_iter()
                    .map(|(src_ms, src_seq, dlq_ms, dlq_seq)| {
                        (
                            StreamId {
                                ms: src_ms,
                                seq: src_seq,
                            },
                            StreamId {
                                ms: dlq_ms,
                                seq: dlq_seq,
                            },
                        )
                    })
                    .collect();
                apply_mq_pop(
                    init,
                    db_index as usize,
                    &key,
                    StreamId {
                        ms: last_delivered.0,
                        seq: last_delivered.1,
                    },
                    claimed,
                    dlq,
                    delivery_time_ms,
                    seen_time_ms,
                );
                stats.pop += 1;
            }
            None => warn_skip_mq_record(shard_id, "MqPop", payload, stats),
        },
        WalRecordType::MqAck => match crate::mq::wal::decode_mq_ack(payload) {
            Some((db_index, key, ms, seq)) => {
                apply_mq_ack(init, db_index as usize, &key, StreamId { ms, seq });
                stats.ack += 1;
            }
            None => warn_skip_mq_record(shard_id, "MqAck", payload, stats),
        },
        WalRecordType::MqTrigger => match crate::mq::wal::decode_mq_trigger(payload) {
            Some((trig_key, queue_key, callback_cmd, debounce_ms)) => {
                apply_mq_trigger(init, trig_key, queue_key, callback_cmd, debounce_ms);
                stats.trigger += 1;
            }
            None => warn_skip_mq_record(shard_id, "MqTrigger", payload, stats),
        },
        WalRecordType::MqDrop => match crate::mq::wal::decode_mq_drop(payload) {
            Some((db_index, key)) => {
                apply_mq_drop(init, db_index as usize, &key);
                stats.drop += 1;
            }
            None => warn_skip_mq_record(shard_id, "MqDrop", payload, stats),
        },
        _ => {}
    }
}

/// Replay MQ WAL records to restore full durable-queue state: registry
/// config, stream content, consumer-group PEL/cursor, DLQ routing, and
/// trigger registrations (task #34 / Wave B stage 2a).
///
/// Operates on `&mut [ShardSliceInit]` — called single-threaded at boot
/// before shard threads are spawned. No locks needed.
///
/// # Ordering requirement
///
/// MUST run AFTER any AOF-authority `db.clear()` wipe. `main.rs` calls this
/// near the end of the boot sequence, well after the AOF-authority
/// wipe-then-replay block — see `tests::test_replay_mq_wal_survives_prior_db_clear`
/// for a direct regression proof, and
/// `tests/crash_recovery_mq_effects.rs::mq_effect_records_survive_kill9` for
/// the live kill-9 round trip. MQ.* commands are intercepted before the
/// generic AOF-logging dispatch path, so under `--appendonly yes` the wipe
/// discards every durable Stream unconditionally; only this replay's own
/// effect-record log rebuilds it.
///
/// # Ordering WITHIN the log
///
/// Records are applied ONE AT A TIME, in WAL order (LSN order, preserved by
/// `replay_wal_v3_file`'s sequential per-file scan + the caller's
/// lexicographically sorted file list) — NOT collected into a map and
/// bulk-applied at the end. PEL membership and the consumer group's
/// `last_delivered_id` are only correct if interleaved MqPush/MqPop/MqAck
/// records are replayed in their original relative order (e.g. a POP that
/// claims ids 1-3, an ACK of id 1, then a LATER POP that claims ids 4-5 must
/// leave the PEL at `{2, 3, 4, 5}` with `last_delivered_id = 5` — batching
/// "all pops then all acks" would get this wrong whenever an id is
/// claimed-then-acked-then-a-later-pop-claims-more).
///
/// # WAL v3 framing
///
/// WAL v3 segments for a shard live in `shard-{id}/wal-v3/` (matching
/// `replay_workspace_wal` / `replay_graph_wal` / `recovery.rs`) — NOT
/// directly under `shard-{id}/`, which `std::fs::read_dir` (non-recursive)
/// would silently scan as empty (task #42). Every cross-thread `wal_append`
/// blob SHOULD arrive with its real type directly post-K1a (no `Command`
/// wrapper) — the nested-unwrap arm below is defensive-only, covering any
/// stray pre-K1a segment still on disk.
pub fn replay_mq_wal(
    inits: &mut [crate::shard::slice::ShardSliceInit],
    persistence_dir: &std::path::Path,
) {
    use crate::persistence::wal_v3::record::{WalRecord, WalRecordType, read_wal_v3_record};

    for init in inits.iter_mut() {
        let shard_id = init.shard_id;
        let wal_dir = persistence_dir
            .join(format!("shard-{}", shard_id))
            .join("wal-v3");
        if !wal_dir.exists() {
            continue;
        }

        let mut stats = MqReplayStats::default();

        {
            let mut handle_record = |record_type: WalRecordType, payload: &[u8]| {
                apply_mq_wal_record(init, shard_id, record_type, payload, &mut stats);
            };

            let on_command = &mut |record: &WalRecord| match record.record_type {
                WalRecordType::MqCreate
                | WalRecordType::MqAck
                | WalRecordType::MqPush
                | WalRecordType::MqPop
                | WalRecordType::MqTrigger
                | WalRecordType::MqDrop => {
                    handle_record(record.record_type, &record.payload);
                }
                WalRecordType::Command => {
                    if let Some(inner) = read_wal_v3_record(&record.payload) {
                        match inner.record_type {
                            WalRecordType::MqCreate
                            | WalRecordType::MqAck
                            | WalRecordType::MqPush
                            | WalRecordType::MqPop
                            | WalRecordType::MqTrigger
                            | WalRecordType::MqDrop => {
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
        }

        if stats.total() > 0 || stats.skipped > 0 {
            tracing::info!(
                "Shard {}: replayed MQ WAL — {} create, {} push, {} pop, {} ack, \
                 {} trigger, {} drop record(s), {} skipped (malformed/unsupported version)",
                shard_id,
                stats.create,
                stats.push,
                stats.pop,
                stats.ack,
                stats.trigger,
                stats.drop,
                stats.skipped,
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
            if record.record_type != WalRecordType::Command {
                return;
            }
            // Payload is RESP-encoded (same format as AOF/WAL v2 blocks) — it
            // must be parsed into frames before dispatch. `replay_command`
            // expects a bare command name plus already-parsed `&[Frame]`
            // args, never the raw multi-line RESP blob (mirrors
            // `replay_graph_wal_v3` above and `recovery.rs`'s KV Command
            // replay; passing the raw payload as `cmd` made
            // `GraphReplayCollector::is_graph_command` never match, silently
            // dropping the entire legacy-mode graph plane on restart).
            let mut buf = bytes::BytesMut::from(&record.payload[..]);
            let parse_cfg = crate::protocol::ParseConfig::default();
            while let Ok(Some(frame)) = crate::protocol::parse::parse(&mut buf, &parse_cfg) {
                let crate::protocol::Frame::Array(ref arr) = frame else {
                    continue;
                };
                if arr.is_empty() {
                    continue;
                }
                let cmd_name: &[u8] = match &arr[0] {
                    crate::protocol::Frame::BulkString(s) => s.as_ref(),
                    crate::protocol::Frame::SimpleString(s) => s.as_ref(),
                    _ => continue,
                };
                engine.replay_command(&mut dummy_dbs, cmd_name, &arr[1..], &mut selected_db);
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
                // #452.2: mid-chain tear ⇒ refuse to boot (see recovery.rs).
                if crate::persistence::wal_v3::replay::is_mid_chain_tear(&e) {
                    crate::persistence::wal_v3::replay::abort_boot_on_mid_chain_tear(shard_id, &e);
                }
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
        crate::config::RuntimeConfig {
            maxmemory,
            num_shards,
            ..Default::default()
        }
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

    /// K4 (kernel-m2-brief-2026-07-12 stage 2): the same donor/hot
    /// misclassification the vector fix (A4) addressed above applies
    /// identically to text (FTS) and graph resident bytes -- the elastic
    /// budget's used-term counted kv+vector ONLY until this commit. Mirrors
    /// `recompute_elastic_budget_vector_heavy_shard_not_donor` exactly, but
    /// splits the "hidden" RAM across text on one shard and graph on
    /// another, so both plane wires are exercised in one test.
    #[test]
    fn recompute_elastic_budget_text_and_graph_heavy_shards_not_donors() {
        let shared = new_shared(4, 1);
        let rt = rt_config(400, 4); // base = 100 per shard

        shared.publish_memory(0, 120); // hot
        shared.publish_memory(1, 10); // KV-light...
        shared.store_memory_per_shard[1]
            .text
            .store(200, Ordering::Relaxed); // ...but 200 of FTS RAM
        shared.publish_memory(2, 10); // KV-light...
        shared.store_memory_per_shard[2]
            .graph
            .store(200, Ordering::Relaxed); // ...but 200 of graph RAM
        shared.publish_memory(3, 10);

        // Blind-to-text/graph math: shards 1 and 2 look idle (10 < 100) and
        // each donate 90 — surplus 90+90+90=270 to the one hot shard ⇒ 370.
        // Aware: shards 1 and 2 are each truly at 210 > base — HOT, not
        // donors. Only shard 3 (true 10) donates 90. Three hot shards split
        // the 90 surplus ⇒ 100 + 30 = 130 each.
        assert_eq!(
            shared.recompute_elastic_budget(0, &rt),
            130,
            "text/graph-heavy shards must not be classified as idle donors"
        );
        assert_eq!(
            shared.recompute_elastic_budget(1, &rt),
            130,
            "the text-heavy shard itself is hot and shares the pool"
        );
        assert_eq!(
            shared.recompute_elastic_budget(2, &rt),
            130,
            "the graph-heavy shard itself is hot and shares the pool"
        );
        // The true idle shard keeps base.
        assert_eq!(shared.recompute_elastic_budget(3, &rt), 100);
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

    // ── Wave B stage 2a (task #34): full-lifecycle effect-record replay ────
    //
    // Supersedes the old `test_replay_mq_wal_restores_registry_and_rolls_back_cursor`,
    // which asserted the PRE-task-#34 "blind cursor-rollback by PEL-count"
    // heuristic (roll back to just before the min PEL id, regardless of
    // which ids were actually acked). That heuristic is gone: MqAck now
    // replays BY ID via `Stream::xack`, and MqPush/MqPop give replay the
    // actual content + claim/DLQ outcomes needed to reconstruct state
    // exactly, without depending on whatever partial content a `.rrdshard`
    // snapshot happened to preserve.

    #[test]
    fn test_replay_mq_wal_restores_full_lifecycle_by_id() {
        use crate::mq::wal::{encode_mq_ack, encode_mq_create, encode_mq_pop, encode_mq_push};
        use crate::persistence::wal_v3::record::WalRecordType;
        use crate::storage::stream::StreamId;

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"orders".to_vec();
        let group_name = bytes::Bytes::from_static(b"__mq_consumers");

        // Simulate the AOF-authority wipe: db starts completely empty (no
        // .rrdshard content survived — matches `--appendonly yes` reality).
        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        let f = |v: &[u8]| {
            (
                bytes::Bytes::from_static(b"seq"),
                bytes::Bytes::copy_from_slice(v),
            )
        };
        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, encode_mq_create(0, &queue_key, 5)),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(0, &queue_key, 1, 0, &[f(b"1")]),
                ),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(0, &queue_key, 1, 1, &[f(b"2")]),
                ),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(0, &queue_key, 1, 2, &[f(b"3")]),
                ),
                // POP claims ids (1,0) and (1,1) with delivery_count=1;
                // resulting last_delivered_id = (1,1); no DLQ routing.
                (
                    WalRecordType::MqPop,
                    encode_mq_pop(0, &queue_key, (1, 1), &[(1, 0, 1), (1, 1, 1)], &[], 1, 1),
                ),
                // ACK only (1,0) — (1,1) must remain pending.
                (WalRecordType::MqAck, encode_mq_ack(0, &queue_key, 1, 0)),
            ],
        );

        replay_mq_wal(&mut inits, tmp.path());

        let reg = inits[0]
            .durable_queue_registry
            .as_ref()
            .expect("MqCreate record must populate durable_queue_registry");
        assert_eq!(
            reg.get(&queue_key).expect("registered").max_delivery_count,
            5
        );

        let db = inits[0].databases.get_mut(0).expect("db 0");
        let stream = db
            .get_stream_mut(&queue_key)
            .expect("get_stream_mut")
            .expect("stream must exist — rebuilt entirely from the WAL");
        assert!(stream.durable, "MqCreate replay must restore durable=true");
        assert_eq!(stream.max_delivery_count, 5);
        assert_eq!(
            stream.entries.len(),
            3,
            "all 3 MqPush records must rebuild stream content from nothing"
        );
        assert_eq!(
            stream.entries.get(&StreamId { ms: 1, seq: 2 }),
            Some(&vec![f(b"3")]),
            "field content must match exactly, including the never-popped 3rd entry"
        );

        let group = stream.groups.get(&group_name).expect("group");
        assert_eq!(
            group.last_delivered_id,
            StreamId { ms: 1, seq: 1 },
            "cursor must land exactly where the MqPop record said, not a \
             count-derived heuristic"
        );
        assert_eq!(
            group.pel.keys().collect::<Vec<_>>(),
            vec![&StreamId { ms: 1, seq: 1 }],
            "only the un-acked id (1,1) may remain pending -- ack-by-id must \
             NOT roll back (1,0) too"
        );
    }

    #[test]
    fn test_replay_mq_wal_dlq_routing_restored() {
        use crate::mq::wal::{encode_mq_create, encode_mq_pop, encode_mq_push};
        use crate::persistence::wal_v3::record::WalRecordType;

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"dlqtestq".to_vec();

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        let f = |v: &[u8]| {
            (
                bytes::Bytes::from_static(b"f"),
                bytes::Bytes::copy_from_slice(v),
            )
        };
        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, encode_mq_create(0, &queue_key, 1)),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(0, &queue_key, 1, 0, &[f(b"a")]),
                ),
                // MAXDELIVERY 1: the claim immediately routes to DLQ,
                // assigned id (2,0) in the sibling DLQ stream.
                (
                    WalRecordType::MqPop,
                    encode_mq_pop(0, &queue_key, (1, 0), &[(1, 0, 1)], &[(1, 0, 2, 0)], 1, 1),
                ),
            ],
        );

        replay_mq_wal(&mut inits, tmp.path());

        let mut dlq_key = queue_key.clone();
        dlq_key.extend_from_slice(b"::mq:dlq");
        let db = inits[0].databases.get_mut(0).expect("db 0");
        let dlq_stream = db
            .get_stream_mut(&dlq_key)
            .expect("get_stream_mut")
            .expect("DLQ stream must exist after replay");
        assert_eq!(dlq_stream.entries.len(), 1);
        assert_eq!(
            dlq_stream
                .entries
                .get(&crate::storage::stream::StreamId { ms: 2, seq: 0 }),
            Some(&vec![f(b"a")]),
            "DLQ entry must land at its ORIGINAL assigned id with original fields"
        );

        let main_stream = db
            .get_stream_mut(&queue_key)
            .expect("get_stream_mut")
            .expect("main stream must exist");
        let group = main_stream
            .groups
            .get(bytes::Bytes::from_static(b"__mq_consumers").as_ref())
            .expect("group");
        assert!(
            group.pel.is_empty(),
            "DLQ-routed entry must NOT remain pending in the main PEL"
        );
    }

    #[test]
    fn test_replay_mq_wal_trigger_registered() {
        use crate::mq::wal::encode_mq_trigger;
        use crate::persistence::wal_v3::record::WalRecordType;

        let tmp = tempfile::tempdir().expect("tempdir");
        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        write_mq_wal_records(
            tmp.path(),
            0,
            &[(
                WalRecordType::MqTrigger,
                encode_mq_trigger(b"trigq", b"trigq", b"FIRED", 200),
            )],
        );

        replay_mq_wal(&mut inits, tmp.path());

        let reg = inits[0]
            .trigger_registry
            .as_ref()
            .expect("MqTrigger record must populate trigger_registry");
        let entry = reg.get(b"trigq").expect("trigger must be registered");
        assert_eq!(entry.queue_key.as_ref(), b"trigq");
        assert_eq!(entry.callback_cmd.as_ref(), b"FIRED");
        assert_eq!(entry.debounce_ms, 200);
        assert_eq!(
            entry.pending_fire_ms, 0,
            "replay must never arm a pending fire -- only live MQ.PUSH does"
        );
    }

    #[test]
    fn test_replay_mq_wal_survives_prior_db_clear() {
        // Direct regression proof for the ordering requirement documented on
        // `replay_mq_wal`: it must be safe to call AFTER an AOF-authority
        // `db.clear()` wipe (main.rs's ordering), because that's exactly the
        // scenario `--appendonly yes` produces in production. Simulates the
        // wipe explicitly rather than depending on main.rs's call order.
        use crate::mq::wal::{encode_mq_create, encode_mq_push};
        use crate::persistence::wal_v3::record::WalRecordType;

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"survives".to_vec();

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        // Pre-populate as if content existed before the "crash" (e.g. from
        // an earlier boot in the same process) — then wipe it exactly like
        // main.rs's AOF-authority path does.
        {
            let db = inits[0].databases.get_mut(0).expect("db 0");
            let _ = db.get_or_create_stream(&queue_key);
            db.clear();
            assert!(
                db.get_stream_mut(&queue_key).unwrap().is_none(),
                "sanity: db.clear() must actually wipe the stream"
            );
        }

        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, encode_mq_create(0, &queue_key, 3)),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(
                        0,
                        &queue_key,
                        1,
                        0,
                        &[(
                            bytes::Bytes::from_static(b"f"),
                            bytes::Bytes::from_static(b"v"),
                        )],
                    ),
                ),
            ],
        );

        replay_mq_wal(&mut inits, tmp.path());

        let db = inits[0].databases.get_mut(0).expect("db 0");
        let stream = db
            .get_stream_mut(&queue_key)
            .expect("get_stream_mut")
            .expect(
                "RED: replay_mq_wal must rebuild the stream from its own \
                 effect-record log even after an earlier db.clear() wipe",
            );
        assert!(stream.durable);
        assert_eq!(stream.entries.len(), 1);
    }

    #[test]
    fn test_replay_mq_wal_unknown_version_skipped_not_fatal() {
        // A corrupted/future-version MqCreate record must be skipped (not
        // panic, not abort the rest of the scan) — the well-formed MqPush
        // record right after it must still apply.
        use crate::mq::wal::encode_mq_push;
        use crate::persistence::wal_v3::record::WalRecordType;

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"q".to_vec();
        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        let mut bogus_create = crate::mq::wal::encode_mq_create(0, &queue_key, 1);
        bogus_create[0] = 200; // unsupported future version

        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, bogus_create),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(
                        0,
                        &queue_key,
                        1,
                        0,
                        &[(
                            bytes::Bytes::from_static(b"f"),
                            bytes::Bytes::from_static(b"v"),
                        )],
                    ),
                ),
            ],
        );

        // Must not panic.
        replay_mq_wal(&mut inits, tmp.path());

        assert!(
            inits[0].durable_queue_registry.is_none(),
            "the malformed MqCreate must be skipped, not applied"
        );
        let db = inits[0].databases.get_mut(0).expect("db 0");
        assert_eq!(
            db.get_or_create_stream(&queue_key).unwrap().entries.len(),
            1,
            "a well-formed record after a skipped malformed one must still apply"
        );
    }

    // ── MqDrop tombstone replay (kernel M3 stage 3 / task #46) ──────────────

    #[test]
    fn test_replay_mq_wal_drop_tombstones_stream_and_registry() {
        // Create + push, then Drop: the stream must be gone from the db AND
        // the registry entry removed after replay.
        use crate::mq::wal::{encode_mq_create, encode_mq_drop, encode_mq_push};
        use crate::persistence::wal_v3::record::WalRecordType;

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"dropped-queue".to_vec();

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, encode_mq_create(0, &queue_key, 3)),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(
                        0,
                        &queue_key,
                        1,
                        0,
                        &[(
                            bytes::Bytes::from_static(b"f"),
                            bytes::Bytes::from_static(b"v"),
                        )],
                    ),
                ),
                (WalRecordType::MqDrop, encode_mq_drop(0, &queue_key)),
            ],
        );

        replay_mq_wal(&mut inits, tmp.path());

        assert!(
            inits[0]
                .durable_queue_registry
                .as_ref()
                .is_none_or(|reg| reg.get(&queue_key).is_none()),
            "MqDrop must remove the registry entry"
        );
        let db = inits[0].databases.get_mut(0).expect("db 0");
        assert!(
            db.get_stream_mut(&queue_key).unwrap().is_none(),
            "MqDrop must remove the stream from the db"
        );
    }

    #[test]
    fn test_replay_mq_wal_drop_only_kills_prior_records() {
        // create -> drop -> create -> push must leave the SECOND
        // incarnation intact: a Drop only undoes records that precede it in
        // WAL order, never a later MqCreate/MqPush for the same key.
        use crate::mq::wal::{encode_mq_create, encode_mq_drop, encode_mq_push};
        use crate::persistence::wal_v3::record::WalRecordType;

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"recreated-queue".to_vec();

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, encode_mq_create(0, &queue_key, 3)),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(
                        0,
                        &queue_key,
                        1,
                        0,
                        &[(
                            bytes::Bytes::from_static(b"f"),
                            bytes::Bytes::from_static(b"old"),
                        )],
                    ),
                ),
                (WalRecordType::MqDrop, encode_mq_drop(0, &queue_key)),
                (WalRecordType::MqCreate, encode_mq_create(0, &queue_key, 5)),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(
                        0,
                        &queue_key,
                        2,
                        0,
                        &[(
                            bytes::Bytes::from_static(b"f"),
                            bytes::Bytes::from_static(b"new"),
                        )],
                    ),
                ),
            ],
        );

        replay_mq_wal(&mut inits, tmp.path());

        assert!(
            inits[0]
                .durable_queue_registry
                .as_ref()
                .and_then(|reg| reg.get(&queue_key))
                .is_some(),
            "the SECOND MqCreate must re-register the queue"
        );
        let db = inits[0].databases.get_mut(0).expect("db 0");
        let stream = db
            .get_stream_mut(&queue_key)
            .unwrap()
            .expect("second incarnation stream must exist");
        assert_eq!(
            stream.entries.len(),
            1,
            "only the SECOND incarnation's push must survive — the first \
             incarnation's push preceded the Drop and must not resurrect"
        );
        let entry = stream.entries.values().next().expect("exactly one entry");
        assert_eq!(
            entry[0].1.as_ref(),
            b"new",
            "the surviving entry must be from the SECOND incarnation, not \
             a stale first-incarnation record the Drop should have killed"
        );
    }

    #[test]
    fn test_replay_mq_wal_drop_malformed_skipped_not_fatal() {
        use crate::mq::wal::{encode_mq_create, encode_mq_push};
        use crate::persistence::wal_v3::record::WalRecordType;

        let tmp = tempfile::tempdir().expect("tempdir");
        let queue_key = b"q".to_vec();

        let dbs = vec![vec![Database::new()]];
        let (_shared, mut inits) = ShardDatabases::new(dbs);

        let bogus_drop = vec![99u8, 0, 0, 0, 0]; // bad version byte
        write_mq_wal_records(
            tmp.path(),
            0,
            &[
                (WalRecordType::MqCreate, encode_mq_create(0, &queue_key, 3)),
                (WalRecordType::MqDrop, bogus_drop),
                (
                    WalRecordType::MqPush,
                    encode_mq_push(
                        0,
                        &queue_key,
                        1,
                        0,
                        &[(
                            bytes::Bytes::from_static(b"f"),
                            bytes::Bytes::from_static(b"v"),
                        )],
                    ),
                ),
            ],
        );

        // Must not panic; the malformed Drop is skipped, the stream must
        // survive with the later well-formed Push applied.
        replay_mq_wal(&mut inits, tmp.path());

        let db = inits[0].databases.get_mut(0).expect("db 0");
        assert_eq!(
            db.get_or_create_stream(&queue_key).unwrap().entries.len(),
            1,
            "a well-formed record after a skipped malformed MqDrop must still apply"
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
