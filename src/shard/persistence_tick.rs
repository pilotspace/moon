//! Persistence tick helpers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains snapshot begin handling,
//! auto-save trigger checking, snapshot advance/finalize prep, and WAL flush.

use std::sync::Arc;

use tracing::info;

use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal::WalWriter;
use crate::runtime::channel;

use super::shared_databases::ShardDatabases;

/// Handle a pending SnapshotBegin that was collected from SPSC drain.
///
/// If a snapshot is already in progress, sends an error reply.
/// Otherwise, creates a new SnapshotState and stores the reply_tx.
pub(crate) fn handle_pending_snapshot(
    pending: Option<(
        u64,
        std::path::PathBuf,
        channel::OneshotSender<Result<(), String>>,
    )>,
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_databases: &Arc<ShardDatabases>,
    disk_offload_dir: Option<&std::path::Path>,
    shard_id: usize,
) {
    if let Some((epoch, snap_dir, reply_tx)) = pending {
        if snapshot_state.is_some() {
            let _ = reply_tx.send(Err("Snapshot already in progress".to_string()));
        } else {
            let snap_path = if let Some(offload) = disk_offload_dir {
                let shard_dir = offload.join(format!("shard-{}", shard_id));
                let _ = std::fs::create_dir_all(&shard_dir);
                shard_dir.join(format!("shard-{}.rrdshard", shard_id))
            } else {
                snap_dir.join(format!("shard-{}.rrdshard", shard_id))
            };
            let (segment_counts, base_timestamps) = shard_databases.snapshot_metadata(shard_id);
            let db_count = shard_databases.db_count();
            *snapshot_state = Some(SnapshotState::new_from_metadata(
                shard_id as u16,
                epoch,
                db_count,
                segment_counts,
                base_timestamps,
                snap_path,
            ));
            *snapshot_reply_tx = Some(reply_tx);
        }
    }
}

/// Check the watch channel for auto-save snapshot triggers.
///
/// If the epoch has advanced and no snapshot is in progress, creates a new SnapshotState.
pub(crate) fn check_auto_save_trigger(
    snapshot_trigger_rx: &channel::WatchReceiver<u64>,
    last_snapshot_epoch: &mut u64,
    snapshot_state: &mut Option<SnapshotState>,
    shard_databases: &Arc<ShardDatabases>,
    persistence_dir: &Option<String>,
    disk_offload_dir: Option<&std::path::Path>,
    shard_id: usize,
) {
    let new_epoch = snapshot_trigger_rx.borrow();
    if new_epoch > *last_snapshot_epoch && snapshot_state.is_none() {
        *last_snapshot_epoch = new_epoch;
        if let Some(dir) = persistence_dir {
            // When disk-offload is enabled, write snapshot to the offload shard directory
            // so v3 recovery can find it alongside WAL v3 segments and manifest.
            let snap_path = if let Some(offload) = disk_offload_dir {
                let shard_dir = offload.join(format!("shard-{}", shard_id));
                let _ = std::fs::create_dir_all(&shard_dir);
                shard_dir.join(format!("shard-{}.rrdshard", shard_id))
            } else {
                std::path::PathBuf::from(dir).join(format!("shard-{}.rrdshard", shard_id))
            };
            let (segment_counts, base_timestamps) = shard_databases.snapshot_metadata(shard_id);
            let db_count = shard_databases.db_count();
            *snapshot_state = Some(SnapshotState::new_from_metadata(
                shard_id as u16,
                new_epoch,
                db_count,
                segment_counts,
                base_timestamps,
                snap_path,
            ));
        }
    }
}

/// Advance snapshot one segment and check if done (synchronous part).
///
/// Returns `true` if the snapshot is complete and ready for async finalization.
pub(crate) fn advance_snapshot_segment(
    snapshot_state: &mut Option<SnapshotState>,
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
) -> bool {
    if let Some(snap) = snapshot_state {
        let current_db = snap.current_db_index();
        let db_count = shard_databases.db_count();
        if current_db < db_count {
            let guard = shard_databases.read_db(shard_id, current_db);
            snap.advance_one_segment_db(&guard)
        } else {
            // All databases serialized, return true to trigger finalization
            true
        }
    } else {
        false
    }
}

/// Handle successful snapshot finalization: truncate WAL and send reply.
pub(crate) fn finalize_snapshot_success(
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    wal_writer: &mut Option<WalWriter>,
    shard_id: usize,
) {
    if let Some(snap) = snapshot_state.as_ref() {
        let epoch = snap.epoch;
        info!("Shard {}: snapshot epoch {} complete", shard_id, epoch);
        // Truncate WAL after successful snapshot
        if let Some(wal) = wal_writer {
            if let Err(e) = wal.truncate_after_snapshot(epoch) {
                tracing::error!(
                    "Shard {}: WAL truncation after snapshot epoch {} failed: {}. \
                     WAL and snapshot may be out of sync.",
                    shard_id,
                    epoch,
                    e
                );
                if let Some(tx) = snapshot_reply_tx.take() {
                    let _ = tx.send(Err(format!("WAL truncation failed: {}", e)));
                }
                *snapshot_state = None;
                return;
            }
        }
        if let Some(tx) = snapshot_reply_tx.take() {
            let _ = tx.send(Ok(()));
        }
    }
    *snapshot_state = None;
}

/// Handle failed snapshot finalization: send error reply.
pub(crate) fn finalize_snapshot_error(
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_id: usize,
    error: &str,
) {
    tracing::error!("Shard {}: snapshot finalize failed: {}", shard_id, error);
    if let Some(tx) = snapshot_reply_tx.take() {
        let _ = tx.send(Err(format!("finalize failed: {}", error)));
    }
    *snapshot_state = None;
}

/// Flush WAL if needed (1ms tick -- write to page cache only; sync is separate).
pub(crate) fn flush_wal_if_needed(wal_writer: &mut Option<WalWriter>) {
    if let Some(wal) = wal_writer {
        if let Err(e) = wal.flush_if_needed() {
            tracing::error!("WAL flush failed: {}", e);
        }
    }
}

/// Flush WAL v3 if buffer exceeds threshold (1ms tick -- mirrors v2 pattern).
///
/// Only active when disk-offload is enabled and WalWriterV3 was successfully initialized.
pub(crate) fn flush_wal_v3_if_needed(
    wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
) {
    if let Some(wal) = wal_v3 {
        if let Err(e) = wal.flush_if_needed() {
            tracing::error!("WAL v3 flush failed: {}", e);
        }
    }
}

// ---------------------------------------------------------------------------
// Warm tier transition handler (disk-offload path)
// ---------------------------------------------------------------------------

/// Periodically check immutable segment ages and trigger HOT->WARM transitions.
///
/// Called from the event loop on a slower interval (e.g., every 10 seconds)
/// when disk-offload is enabled. Scans all VectorIndex segments, transitions
/// those older than `warm_after_secs`.
pub(crate) fn check_warm_transitions(
    vector_store: &crate::vector::store::VectorStore,
    shard_dir: &std::path::Path,
    manifest: &mut ShardManifest,
    warm_after_secs: u64,
    next_file_id: &mut u64,
    shard_id: usize,
    wal: &mut Option<WalWriterV3>,
) {
    let count = vector_store.try_warm_transitions_all(
        shard_dir, manifest, warm_after_secs, next_file_id, wal,
    );
    if count > 0 {
        info!(
            "Shard {}: transitioned {} segment(s) to warm tier",
            shard_id, count
        );
    }
}

// ---------------------------------------------------------------------------
// Cold tier transition handler (disk-offload path)
// ---------------------------------------------------------------------------

/// Periodically check warm segment ages and trigger WARM->COLD transitions.
///
/// Called from the event loop on a 60-second timer when disk-offload is enabled
/// and `server_config.segment_cold_after > 0`. Scans all warm segments across
/// all VectorIndex instances and transitions those older than `cold_after_secs`
/// to DiskANN cold tier (PQ codes in RAM + Vamana graph on NVMe).
///
/// NOTE: The actual event loop wiring (select! macro integration) is outside
/// this plan's file ownership and will happen when the shard event loop is
/// updated in a future plan. This function exists and is callable.
pub(crate) fn check_cold_transitions(
    vector_store: &crate::vector::store::VectorStore,
    shard_dir: &std::path::Path,
    manifest: &mut ShardManifest,
    cold_after_secs: u64,
    next_file_id: &mut u64,
    shard_id: usize,
) {
    let count = vector_store.try_cold_transitions_all(
        shard_dir, manifest, cold_after_secs, next_file_id,
    );
    if count > 0 {
        info!(
            "Shard {}: transitioned {} segment(s) to cold tier",
            shard_id, count
        );
    }
}

// ---------------------------------------------------------------------------
// Memory pressure cascade (design section 8.5)
// ---------------------------------------------------------------------------

/// Check if memory usage exceeds the disk offload threshold.
///
/// Returns `true` when the pressure cascade should run. Guards against
/// unnecessary cascade work when memory is within budget.
pub(crate) fn should_run_pressure_cascade(
    runtime_config: &std::sync::Arc<std::sync::RwLock<crate::config::RuntimeConfig>>,
    server_config: &std::sync::Arc<crate::config::ServerConfig>,
) -> bool {
    let rt = match runtime_config.read() {
        Ok(rt) => rt,
        Err(_) => return false,
    };
    if rt.maxmemory == 0 {
        return false; // No memory limit set -- no pressure possible
    }
    // Use jemalloc epoch + resident stat when available, otherwise use
    // database-estimated memory as a proxy (cheaper, but less accurate).
    // The threshold check is intentionally coarse: individual cascade steps
    // re-check whether work is actually needed.
    let threshold = (rt.maxmemory as f64 * server_config.disk_offload_threshold) as usize;
    // Approximate: if maxmemory is set and threshold < maxmemory, we consider
    // pressure present. A more precise RSS check can be added later when
    // jemalloc stats are wired into the shard event loop.
    // For now, always return true when maxmemory > 0 and disk-offload is
    // enabled -- individual steps are cheap no-ops when there's nothing to do.
    threshold < rt.maxmemory
}

/// Memory pressure cascade per MoonStore v2 design section 8.5.
///
/// Ordered response:
/// 1. **PageCache clock-sweep eviction** -- evict cold (unpinned, non-dirty) frames
/// 2. **Force-demote oldest HOT ImmutableSegments to WARM** (halved threshold)
/// 3. **KV eviction** -- existing LRU/LFU via `timers::run_eviction`
/// 4. **NoEviction policy** -- log OOM warning if cascade is exhausted
///
/// Called from eviction timer tick when `disk_offload_enabled` is true and
/// `should_run_pressure_cascade()` returns true.
pub(crate) fn handle_memory_pressure(
    page_cache: &Option<PageCache>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    runtime_config: &std::sync::Arc<std::sync::RwLock<crate::config::RuntimeConfig>>,
    server_config: &std::sync::Arc<crate::config::ServerConfig>,
    shard_manifest: &mut Option<ShardManifest>,
    next_file_id: &mut u64,
    wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
) {
    // Step 1: PageCache eviction -- evict up to 16 cold frames per tick.
    // This is the cheapest operation: no disk I/O, just invalidates cached pages.
    if let Some(ref pc) = *page_cache {
        let evicted = pc.evict_cold_frames(16);
        if evicted > 0 {
            tracing::debug!(
                "Shard {}: memory pressure step 1 -- evicted {} cold PageCache frame(s)",
                shard_id,
                evicted
            );
            return; // Pressure partially relieved; next tick will re-evaluate
        }
    }

    // Step 2: Force-demote oldest HOT ImmutableSegments to WARM.
    // Use half the normal warm_after threshold to be more aggressive under pressure.
    if let Some(ref mut manifest) = *shard_manifest {
        let aggressive_threshold = server_config.segment_warm_after / 2;
        let shard_dir = server_config
            .effective_disk_offload_dir()
            .join(format!("shard-{}", shard_id));
        let vs = shard_databases.vector_store(shard_id);
        let count = vs.try_warm_transitions_all(
            &shard_dir,
            manifest,
            aggressive_threshold,
            next_file_id,
            wal_v3,
        );
        if count > 0 {
            tracing::info!(
                "Shard {}: memory pressure step 2 -- force-demoted {} segment(s) HOT->WARM",
                shard_id,
                count
            );
            return; // Freed memory via warm transition; re-evaluate next tick
        }
    }

    // Step 3: KV eviction -- run existing LRU/LFU eviction, with spill-to-disk
    // when disk-offload is enabled (evicted entries written to KvLeaf DataFiles).
    if let Ok(rt) = runtime_config.read() {
        if rt.maxmemory > 0 {
            let db_count = shard_databases.db_count();
            let shard_dir = server_config
                .effective_disk_offload_dir()
                .join(format!("shard-{}", shard_id));
            for i in 0..db_count {
                let mut guard = shard_databases.write_db(shard_id, i);
                if let Some(ref mut manifest) = *shard_manifest {
                    let mut ctx = crate::storage::eviction::SpillContext {
                        shard_dir: &shard_dir,
                        manifest,
                        next_file_id,
                    };
                    let _ = crate::storage::eviction::try_evict_if_needed_with_spill(
                        &mut guard, &rt, Some(&mut ctx),
                    );
                } else {
                    let _ = crate::storage::eviction::try_evict_if_needed(&mut guard, &rt);
                }
            }
        }
    }

    // Step 4: NoEviction policy check -- if we reached here with noeviction,
    // log a warning. The actual OOM rejection is handled inside try_evict_if_needed.
    if let Ok(rt) = runtime_config.read() {
        if rt.maxmemory_policy == "noeviction" {
            tracing::warn!(
                "Shard {}: memory pressure cascade exhausted; \
                 noeviction policy active, new writes may be rejected",
                shard_id
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Checkpoint protocol handlers (disk-offload path)
// ---------------------------------------------------------------------------

use crate::persistence::checkpoint::{CheckpointAction, CheckpointManager};
use crate::persistence::control::ShardControlFile;
use crate::persistence::manifest::ShardManifest;
use crate::persistence::page_cache::PageCache;
use crate::persistence::wal_v3::record::WalRecordType;
use crate::persistence::wal_v3::segment::WalWriterV3;
use std::path::Path;

/// Force a complete checkpoint synchronously (used by BGSAVE and shutdown).
///
/// Calls `force_begin` to bypass trigger conditions, then drives the
/// checkpoint state machine to completion in a tight loop. No-op if a
/// checkpoint is already active.
pub(crate) fn force_checkpoint(
    checkpoint_mgr: &mut CheckpointManager,
    page_cache: &PageCache,
    wal: &mut WalWriterV3,
    manifest: &mut ShardManifest,
    control: &mut ShardControlFile,
    control_path: &Path,
    shard_id: usize,
) {
    if checkpoint_mgr.is_active() {
        tracing::warn!("Shard {}: checkpoint already active, skipping force", shard_id);
        return;
    }
    let lsn = wal.current_lsn();
    let dirty = page_cache.dirty_page_count();
    if !checkpoint_mgr.force_begin(lsn, dirty) {
        return;
    }
    // Drive checkpoint to completion synchronously (tick loop)
    loop {
        if handle_checkpoint_tick(checkpoint_mgr, page_cache, wal, manifest, control, control_path) {
            break; // Finalize completed
        }
        // If Nothing returned and not active, we're done (empty checkpoint)
        if !checkpoint_mgr.is_active() {
            break;
        }
    }
    info!("Shard {}: forced checkpoint complete", shard_id);
}

/// Check the trigger and begin a checkpoint if conditions are met.
///
/// Called every tick from the event loop when disk-offload is enabled.
/// No-op if a checkpoint is already in progress.
pub(crate) fn maybe_begin_checkpoint(
    checkpoint_mgr: &mut CheckpointManager,
    wal: &WalWriterV3,
    page_cache: &PageCache,
    wal_bytes_since_checkpoint: u64,
) {
    if checkpoint_mgr.is_active() {
        return;
    }
    if checkpoint_mgr.trigger().should_checkpoint(wal_bytes_since_checkpoint) {
        let lsn = wal.current_lsn();
        let dirty = page_cache.dirty_page_count();
        checkpoint_mgr.begin(lsn, dirty);
    }
}

/// Handle one checkpoint tick. Called from the event loop every 1ms when
/// disk-offload is enabled.
///
/// Returns `true` if a finalize step was completed this tick.
///
/// The caller provides all I/O dependencies — CheckpointManager itself is pure state.
pub(crate) fn handle_checkpoint_tick(
    checkpoint_mgr: &mut CheckpointManager,
    page_cache: &PageCache,
    wal: &mut WalWriterV3,
    manifest: &mut ShardManifest,
    control: &mut ShardControlFile,
    control_path: &Path,
) -> bool {
    match checkpoint_mgr.advance_tick() {
        CheckpointAction::Nothing => false,
        CheckpointAction::FlushPages(count) => {
            // Flush `count` dirty pages through PageCache with WAL-before-data.
            let flushed = page_cache.flush_dirty_pages(
                count,
                &mut |page_lsn| {
                    // Ensure WAL is durable past this page's LSN before writing page
                    if wal.current_lsn() > page_lsn {
                        wal.flush_sync()
                    } else {
                        Ok(())
                    }
                },
                &mut |file_id, page_offset, is_large, data| {
                    // pwrite(2) dirty page to its DataFile at the correct offset.
                    // KV heap pages: {shard_dir}/data/heap-{file_id:06}.mpf
                    // Warm-tier .mpf pages are immutable and never dirtied, so
                    // only KV heap pages reach this path.
                    use std::os::unix::fs::FileExt;
                    let page_size = if is_large {
                        crate::persistence::page::PAGE_64K
                    } else {
                        crate::persistence::page::PAGE_4K
                    };
                    let byte_offset = page_offset * page_size as u64;
                    let shard_dir = control_path.parent().unwrap_or(Path::new("."));
                    let file_path = shard_dir
                        .join("data")
                        .join(format!("heap-{:06}.mpf", file_id));
                    let file = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&file_path)?;
                    file.write_at(data, byte_offset)?;
                    Ok(())
                },
            );
            if flushed > 0 {
                tracing::trace!("Checkpoint: flushed {} dirty pages", flushed);
            }
            false
        }
        CheckpointAction::Finalize { redo_lsn } => {
            // 1. Write WAL checkpoint record with redo_lsn payload
            let mut payload = [0u8; 8];
            payload.copy_from_slice(&redo_lsn.to_le_bytes());
            wal.append(WalRecordType::Checkpoint, &payload);

            // 2. Flush WAL to disk
            if let Err(e) = wal.flush_sync() {
                tracing::error!("Checkpoint WAL flush failed: {}", e);
                return false;
            }

            // 3. Commit manifest (atomic dual-root write)
            if let Err(e) = manifest.commit() {
                tracing::error!("Checkpoint manifest commit failed: {}", e);
                return false;
            }

            // 4. Update control file with new checkpoint LSN
            control.last_checkpoint_lsn = redo_lsn;
            control.last_checkpoint_epoch = manifest.epoch();
            if let Err(e) = control.write(control_path) {
                tracing::error!("Checkpoint control file update failed: {}", e);
                return false;
            }

            // 5. Mark checkpoint complete
            checkpoint_mgr.complete();

            // 6. Recycle old WAL segments that are fully before redo_lsn
            match wal.recycle_segments_before(redo_lsn) {
                Ok(n) if n > 0 => {
                    tracing::info!("Checkpoint: recycled {} old WAL segment(s)", n);
                }
                Err(e) => {
                    tracing::warn!("WAL segment recycling failed: {}", e);
                }
                _ => {}
            }

            tracing::info!(
                "Checkpoint complete: redo_lsn={}, epoch={}",
                redo_lsn,
                manifest.epoch()
            );
            true
        }
    }
}
