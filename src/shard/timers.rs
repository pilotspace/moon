//! Timer arm body handlers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains expiry, eviction, block timeout,
//! and WAL sync tick handlers.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::blocking::BlockingRegistry;
use crate::config::RuntimeConfig;
use crate::persistence::wal::WalWriter;

use super::shared_databases::ShardDatabases;

/// Run cooperative active expiry across all databases.
/// Shard 0 also updates the RSS gauge (once per expiry cycle, ~100ms).
pub(crate) fn run_active_expiry(shard_databases: &Arc<ShardDatabases>, shard_id: usize) {
    let db_count = shard_databases.db_count();
    for i in 0..db_count {
        let mut guard = shard_databases.write_db(shard_id, i);
        crate::server::expiration::expire_cycle_direct(&mut guard);
    }
    // Update RSS gauge on shard 0 only, once per second (not every 100ms tick).
    // Gated by a simple counter to reduce /proc/self/statm open/read/close churn.
    if shard_id == 0 {
        use std::sync::atomic::{AtomicU8, Ordering};
        static RSS_TICK: AtomicU8 = AtomicU8::new(0);
        let tick = RSS_TICK.fetch_add(1, Ordering::Relaxed);
        if tick.is_multiple_of(10) {
            let rss = crate::admin::metrics_setup::get_rss_bytes();
            if rss > 0 {
                crate::admin::metrics_setup::update_rss_bytes(rss);
            }
        }
    }
}

/// Run background eviction if maxmemory is configured.
pub(crate) fn run_eviction(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
) {
    let rt = runtime_config.read();
    if rt.maxmemory > 0 {
        let db_count = shard_databases.db_count();
        for i in 0..db_count {
            let mut guard = shard_databases.write_db(shard_id, i);
            let _ = crate::storage::eviction::try_evict_if_needed(&mut guard, &rt);
        }
    }
}

/// Expire timed-out blocked clients.
pub(crate) fn expire_blocked_clients(blocking_rc: &Rc<RefCell<BlockingRegistry>>) {
    let now = std::time::Instant::now();
    blocking_rc.borrow_mut().expire_timed_out(now);
}

/// Checkpoint tick interval in milliseconds.
/// Same 1ms tick as WAL flush — checkpoint manager advances one tick per call.
#[allow(dead_code)]
pub const CHECKPOINT_TICK_MS: u64 = 1;

/// Warm tier transition check interval in milliseconds (10 seconds).
/// Infrequent enough to avoid overhead, responsive enough to catch aged segments.
pub const WARM_CHECK_INTERVAL_MS: u64 = 10_000;

/// WAL fsync on 1-second interval (everysec durability).
pub(crate) fn sync_wal(wal_writer: &mut Option<WalWriter>) {
    if let Some(wal) = wal_writer {
        if let Err(e) = wal.sync_to_disk() {
            tracing::error!("WAL sync failed: {}", e);
        }
    }
}

/// Fire MQ triggers whose debounce window has elapsed.
///
/// Called from the event loop periodic tick at 100ms cadence (same interval
/// as expiry/eviction). For each trigger entry where `pending_fire_ms > 0`
/// and `pending_fire_ms <= now_ms`, fires the callback by publishing a
/// notification to the `mq:trigger:{queue_key}` pub/sub channel.
///
/// Triggers fire on the workspace home shard only (hash tag ensures
/// all workspace keys route to one shard, so the TriggerRegistry on
/// that shard is authoritative).
pub(crate) fn fire_pending_mq_triggers(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    now_ms: u64,
    pubsub_registry: &Arc<parking_lot::RwLock<crate::pubsub::PubSubRegistry>>,
) {
    let mut guard = shard_databases.trigger_registry(shard_id);
    let Some(reg) = guard.as_mut() else { return };

    // Collect keys of triggers ready to fire
    let ready_keys = reg.fire_ready(now_ms);
    if ready_keys.is_empty() {
        return;
    }

    // Collect (channel, message) pairs while holding trigger lock,
    // then publish after releasing it to avoid holding two locks.
    let mut notifications: Vec<(bytes::Bytes, bytes::Bytes)> = Vec::with_capacity(ready_keys.len());

    for key in &ready_keys {
        if let Some(entry) = reg.get(key) {
            // Build pub/sub channel name: "mq:trigger:{queue_key}"
            let channel = {
                let mut ch = Vec::with_capacity(11 + entry.queue_key.len());
                ch.extend_from_slice(b"mq:trigger:");
                ch.extend_from_slice(&entry.queue_key);
                bytes::Bytes::from(ch)
            };
            notifications.push((channel, entry.callback_cmd.clone()));
        }
        // Mark as fired (updates last_fire_ms, clears pending_fire_ms)
        reg.mark_fired(key, now_ms);
    }

    // Release trigger registry lock before acquiring pubsub lock
    drop(guard);

    // Publish each trigger notification via pub/sub
    if !notifications.is_empty() {
        let mut pubsub = pubsub_registry.write();
        for (channel, message) in &notifications {
            let _subscriber_count = pubsub.publish(channel, message);
            tracing::debug!(
                "Shard {}: MQ trigger fired on channel {:?} -> {} subscriber(s)",
                shard_id,
                String::from_utf8_lossy(channel),
                _subscriber_count,
            );
        }
    }
}

/// Default interval for cold-tier orphan sweep (5 minutes).
pub const COLD_ORPHAN_SWEEP_INTERVAL_SECS: u64 = 300;

/// Run a cold-tier orphan sweep across all databases for a single shard.
///
/// Iterates each database's cold index and removes entries whose key is now
/// present in the hot DashTable (i.e. a hot write shadowed the spilled copy).
/// The corresponding DataFile and manifest entry are tombstoned atomically.
///
/// Called from the shard event loop on a low-priority 5-minute timer (or the
/// interval configured via `--cold-orphan-sweep-interval-secs`).
///
/// # Concurrency
///
/// Each database is locked with `write_db` for the duration of its sweep.
/// This is safe: spill, read-promotion, and eviction all run under the same
/// per-db write lock, preventing TOCTOU races with the sweep.
pub(crate) fn run_cold_orphan_sweep(
    shard_databases: &Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    shard_dir: &std::path::Path,
    mut manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
) {
    use crate::storage::tiered::cold_index::SweepStats;

    let db_count = shard_databases.db_count();
    let mut total = SweepStats::default();

    for db_idx in 0..db_count {
        let mut guard = shard_databases.write_db(shard_id, db_idx);

        // Skip databases without a cold index (disk-offload disabled or no spills).
        if guard.cold_index.is_none() {
            continue;
        }

        // Two-phase sweep to sidestep the &mut cold_index / &db borrow conflict:
        //
        // Phase 1: collect orphan keys (immutable borrow of cold_index + db).
        // A cold entry is an orphan when the key is also present in the hot
        // DashTable — meaning a hot write shadowed the spilled copy.
        let orphan_keys: Vec<bytes::Bytes> = guard
            .cold_index
            .as_ref()
            .map(|ci| {
                ci.iter()
                    .filter(|(key, _loc)| guard.is_hot(key))
                    .map(|(k, _)| k.clone())
                    .collect()
            })
            .unwrap_or_default();

        if orphan_keys.is_empty() {
            continue;
        }

        // Phase 2: delete files + update cold_index (mutable borrow; db not borrowed).
        let stats = guard.cold_index.as_mut().and_then(|ci| {
            ci.sweep_known_orphans(orphan_keys, shard_dir, manifest.as_deref_mut())
                .map_err(|e| {
                    tracing::error!(
                        shard = shard_id,
                        db = db_idx,
                        err = %e,
                        "cold_orphan_sweep: manifest commit error",
                    );
                    e
                })
                .ok()
        });

        if let Some(s) = stats {
            total.entries_reclaimed += s.entries_reclaimed;
            total.bytes_reclaimed = total.bytes_reclaimed.saturating_add(s.bytes_reclaimed);
        }
    }

    if total.entries_reclaimed > 0 {
        tracing::info!(
            shard = shard_id,
            entries = total.entries_reclaimed,
            bytes = total.bytes_reclaimed,
            "cold_orphan_sweep: shard sweep complete",
        );
    }
}

/// WAL v3 fsync on 1-second interval (mirrors v2 everysec pattern).
///
/// Flush any buffered WAL v3 data and fsync to stable storage.
///
/// Called on the 1s timer. Writes remaining buffer contents then fsyncs.
/// Only active when disk-offload is enabled and WalWriterV3 was successfully initialized.
pub(crate) fn sync_wal_v3(wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>) {
    if let Some(wal) = wal_v3 {
        if let Err(e) = wal.flush_sync() {
            tracing::error!("WAL v3 sync failed: {}", e);
        }
    }
}

/// MVCC sweep tick: prune committed treemap + sweep zombie intents + update RECL_* metrics.
///
/// Called on the 1s timer (same cadence as WAL fsync). Takes mutable access to the
/// per-shard VectorStore so it can reach the `TransactionManager` and, when the `graph`
/// feature is enabled, the `GraphStore` for graph intent cleanup.
///
/// ## What this does
///
/// 1. `mark_old_snapshots_killed(now, threshold)` — MA2: kill snapshots older than
///    `old_snapshot_threshold_secs`. Skipped when threshold == 0 (disabled).
/// 2. `prune_committed(margin)` — evicts `committed` entries below
///    `oldest_snapshot - margin`, freeing RoaringTreemap memory.
/// 3. `sweep_zombies_mut()` — removes any write intents whose owner txn_id is
///    neither active nor committed (leaked by dropped-future or panic paths).
/// 4. Updates five RECL_MVCC_* atomics so `INFO` picks them up immediately,
///    including `RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS` (wired here for MA2).
/// 5. MA1: samples total immutable segment count and updates `RECL_SEGMENT_STALL_ACTIVE`
///    via `segment_stall::update_segment_stall`. Read commands continue; only
///    foreground writes are blocked when the threshold is exceeded.
///
/// ## Safety
///
/// Must be called from the shard thread only. `VectorStore` is `!Send`; no lock
/// is held across this call.
pub(crate) fn run_mvcc_sweep(
    vector_store: &mut crate::vector::store::VectorStore,
    #[cfg(feature = "graph")] graph_store: &mut crate::graph::store::GraphStore,
    prune_margin: u64,
    max_unflushed_immutable_segments: u64,
    old_snapshot_threshold_secs: u64,
) {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use crate::command::info_reclamation::{
        RECL_MVCC_ACTIVE, RECL_MVCC_COMMITTED, RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS,
        RECL_MVCC_OLDEST_SNAPSHOT_LAG, RECL_MVCC_ZOMBIES_SWEPT_TOTAL,
    };

    let now = Instant::now();
    let mgr = vector_store.txn_manager_mut();

    // 1. MA2: mark snapshots older than threshold as killed so oldest_snapshot advances.
    let newly_killed = if old_snapshot_threshold_secs > 0 {
        let threshold = Duration::from_secs(old_snapshot_threshold_secs);
        let count = mgr.mark_old_snapshots_killed(now, threshold);
        if count > 0 {
            tracing::warn!(
                count,
                threshold_secs = old_snapshot_threshold_secs,
                "mvcc_sweep: killed {} snapshot(s) older than threshold",
                count
            );
        }
        count
    } else {
        0
    };

    // 2. Prune committed treemap.
    let pruned = mgr.prune_committed(prune_margin);

    // 3. Sweep zombie vector write intents.
    let swept_vec = mgr.sweep_zombies_mut();

    // 4. Sweep zombie graph write intents (feature-gated).
    #[cfg(feature = "graph")]
    let swept_graph = {
        // GraphStore owns its own LSN counter but shares the same transaction
        // lifecycle model. Sweep the graph intents via the MVCC manager.
        let _ = graph_store; // referenced for future graph-txn sweep wiring
        mgr.sweep_graph_zombies_mut()
    };
    #[cfg(not(feature = "graph"))]
    let swept_graph = 0usize;

    let total_swept = swept_vec + swept_graph;
    let _ = pruned; // used for debug; not emitted separately (merged into committed count)

    // 5. Update RECL_MVCC_* atomics.
    let committed_len = mgr.committed_count();
    let active_len = mgr.active_count() as u64;
    let oldest_snap = mgr.oldest_snapshot();
    let current_lsn = mgr.current_lsn();
    let lag = current_lsn.saturating_sub(oldest_snap);

    // MA2: oldest_snapshot_age measures age of oldest NON-killed snapshot.
    // Killed snapshots are excluded so the metric reflects real GC pressure.
    let age_secs = mgr.oldest_snapshot_age(now).map_or(0, |d| d.as_secs());

    RECL_MVCC_COMMITTED.store(committed_len, Ordering::Relaxed);
    RECL_MVCC_ACTIVE.store(active_len, Ordering::Relaxed);
    RECL_MVCC_OLDEST_SNAPSHOT_LAG.store(lag, Ordering::Relaxed);
    RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS.store(age_secs, Ordering::Relaxed);
    if total_swept > 0 {
        RECL_MVCC_ZOMBIES_SWEPT_TOTAL.fetch_add(total_swept as u64, Ordering::Relaxed);
    }

    // 6. MA1: update segment-stall bit based on current immutable segment count.
    let imm_count = vector_store.total_immutable_segment_count();
    crate::shard::segment_stall::update_segment_stall(imm_count, max_unflushed_immutable_segments);

    if total_swept > 0 || pruned > 0 || newly_killed > 0 {
        tracing::debug!(
            pruned,
            swept_vec,
            swept_graph,
            newly_killed,
            committed_remaining = committed_len,
            oldest_snapshot_lag = lag,
            oldest_snapshot_age_secs = age_secs,
            imm_count,
            "mvcc_sweep: pruned committed + swept zombies + killed old snapshots",
        );
    }
}
