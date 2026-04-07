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
        shard_dir,
        manifest,
        warm_after_secs,
        next_file_id,
        wal,
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
    let count =
        vector_store.try_cold_transitions_all(shard_dir, manifest, cold_after_secs, next_file_id);
    if count > 0 {
        info!(
            "Shard {}: transitioned {} segment(s) to cold tier",
            shard_id, count
        );
    }
}

// ---------------------------------------------------------------------------
// Async spill completion polling (background pwrite thread)
// ---------------------------------------------------------------------------

/// Poll background spill thread for completed pwrite operations.
/// For each successful completion: update manifest and ColdIndex.
/// Called on each eviction tick from the event loop.
pub(crate) fn apply_spill_completions(
    spill_thread: &crate::storage::tiered::spill_thread::SpillThread,
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
) {
    let completions = spill_thread.drain_completions();
    if completions.is_empty() {
        return;
    }

    for c in completions {
        if !c.success {
            tracing::warn!(
                key = %String::from_utf8_lossy(&c.key),
                file_id = c.file_id,
                "Spill pwrite failed on background thread"
            );
            continue;
        }

        // Update manifest
        if let Some(ref mut manifest) = *shard_manifest {
            manifest.add_file(c.file_entry);
            if let Err(e) = manifest.commit() {
                tracing::warn!(file_id = c.file_id, error = %e, "Manifest commit failed for spill completion");
            }
        }

        // Update ColdIndex in the originating logical DB.
        let mut guard = shard_databases.write_db(shard_id, c.db_index);
        if let Some(ref mut ci) = guard.cold_index {
            ci.insert(
                c.key,
                crate::storage::tiered::cold_index::ColdLocation {
                    file_id: c.file_id,
                    slot_idx: c.slot_idx,
                },
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Memory pressure cascade (design section 8.5)
// ---------------------------------------------------------------------------

/// Check if memory usage exceeds the disk offload threshold.
///
/// Returns `true` when the pressure cascade should run. Uses actual
/// aggregate database memory estimate vs maxmemory * threshold.
pub(crate) fn should_run_pressure_cascade(
    runtime_config: &std::sync::Arc<std::sync::RwLock<crate::config::RuntimeConfig>>,
    server_config: &std::sync::Arc<crate::config::ServerConfig>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
) -> bool {
    let rt = match runtime_config.read() {
        Ok(rt) => rt,
        Err(_) => return false,
    };
    if rt.maxmemory == 0 {
        return false; // No memory limit set -- no pressure possible
    }
    let threshold = (rt.maxmemory as f64 * server_config.disk_offload_threshold) as usize;
    let used = shard_databases.aggregate_memory(shard_id);
    used > threshold
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
    spill_thread: Option<&crate::storage::tiered::spill_thread::SpillThread>,
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
    // Use aggregate memory (server-wide) to match Redis maxmemory semantics.
    //
    // When a SpillThread is available, use the async path: entries are removed
    // from DashTable immediately (freeing RAM) and pwrite is deferred to the
    // background thread. Otherwise, fall back to synchronous spill.
    if let Ok(rt) = runtime_config.read() {
        if rt.maxmemory > 0 {
            // Compute aggregate BEFORE acquiring write locks (same pattern as handler_sharded).
            let total_mem = shard_databases.aggregate_memory(shard_id);
            if total_mem > rt.maxmemory {
                let db_count = shard_databases.db_count();
                let shard_dir = server_config
                    .effective_disk_offload_dir()
                    .join(format!("shard-{}", shard_id));

                if let Some(spill_t) = spill_thread {
                    // Async spill path: background thread does pwrite
                    let sender = spill_t.sender();
                    for i in 0..db_count {
                        let mut guard = shard_databases.write_db(shard_id, i);
                        let _ =
                            crate::storage::eviction::try_evict_if_needed_async_spill_with_total(
                                &mut guard,
                                &rt,
                                &sender,
                                &shard_dir,
                                next_file_id,
                                total_mem,
                                i,
                            );
                    }
                    // Drop sender clone immediately to avoid shutdown deadlock
                    drop(sender);
                } else {
                    // Sync spill fallback
                    for i in 0..db_count {
                        let mut guard = shard_databases.write_db(shard_id, i);
                        if let Some(ref mut manifest) = *shard_manifest {
                            let mut ctx = crate::storage::eviction::SpillContext {
                                shard_dir: &shard_dir,
                                manifest,
                                next_file_id,
                            };
                            let _ =
                                crate::storage::eviction::try_evict_if_needed_with_spill_and_total(
                                    &mut guard,
                                    &rt,
                                    Some(&mut ctx),
                                    total_mem,
                                );
                        } else {
                            let _ =
                                crate::storage::eviction::try_evict_if_needed_with_spill_and_total(
                                    &mut guard, &rt, None, total_mem,
                                );
                        }
                    }
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
        tracing::warn!(
            "Shard {}: checkpoint already active, skipping force",
            shard_id
        );
        return;
    }
    let lsn = wal.current_lsn();
    let dirty = page_cache.dirty_page_count();
    if !checkpoint_mgr.force_begin(lsn, dirty) {
        return;
    }
    page_cache.clear_all_fpi_pending();
    // Drive checkpoint to completion synchronously (tick loop)
    loop {
        if handle_checkpoint_tick(
            checkpoint_mgr,
            page_cache,
            wal,
            manifest,
            control,
            control_path,
        ) {
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
    if checkpoint_mgr
        .trigger()
        .should_checkpoint(wal_bytes_since_checkpoint)
    {
        let lsn = wal.current_lsn();
        let dirty = page_cache.dirty_page_count();
        checkpoint_mgr.begin(lsn, dirty);
        page_cache.clear_all_fpi_pending();
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
            // Collect FPI payloads during sweep, then append to WAL after.
            // This avoids dual-mutable-borrow of `wal` across closures.
            let mut fpi_payloads: Vec<Vec<u8>> = Vec::new();

            let flushed = page_cache.flush_dirty_pages_with_fpi(
                count,
                &mut |page_lsn| {
                    // Ensure WAL is durable past this page's LSN before writing page
                    if wal.current_lsn() > page_lsn {
                        wal.flush_sync()
                    } else {
                        Ok(())
                    }
                },
                &mut |file_id, page_offset, _is_large, data| {
                    // Collect FPI payload for deferred WAL append.
                    // Payload format: file_id(8 LE) + page_offset(8 LE) + flag(1) + page_data
                    // Flag: 0x00 = uncompressed, 0x01 = LZ4-compressed
                    let mut payload = Vec::with_capacity(17 + data.len());
                    payload.extend_from_slice(&file_id.to_le_bytes());
                    payload.extend_from_slice(&page_offset.to_le_bytes());
                    if data.len() > 256 {
                        let compressed = lz4_flex::compress_prepend_size(data);
                        if compressed.len() < data.len() {
                            payload.push(0x01);
                            payload.extend_from_slice(&compressed);
                        } else {
                            payload.push(0x00);
                            payload.extend_from_slice(data);
                        }
                    } else {
                        payload.push(0x00);
                        payload.extend_from_slice(data);
                    }
                    fpi_payloads.push(payload);
                    Ok(())
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
                    let file = std::fs::OpenOptions::new().write(true).open(&file_path)?;
                    file.write_at(data, byte_offset)?;
                    Ok(())
                },
            );

            // Deferred FPI WAL append -- now safe since flush_dirty_pages_with_fpi
            // returned and the closures no longer borrow `wal`.
            for payload in &fpi_payloads {
                wal.append(WalRecordType::FullPageImage, payload);
            }

            if flushed > 0 {
                tracing::trace!(
                    "Checkpoint: flushed {} dirty pages (with FPI, {} FPI records)",
                    flushed,
                    fpi_payloads.len()
                );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::checkpoint::CheckpointTrigger;
    use crate::persistence::wal_v3::record::{WalRecordType, read_wal_v3_record};
    use crate::persistence::wal_v3::segment::{DEFAULT_SEGMENT_SIZE, WAL_V3_HEADER_SIZE};

    /// Count FullPageImage records in a raw WAL segment file.
    fn count_fpi_records(raw_data: &[u8]) -> usize {
        let mut offset = WAL_V3_HEADER_SIZE;
        let mut fpi_count = 0usize;
        while offset + 4 <= raw_data.len() {
            let record_len =
                u32::from_le_bytes(raw_data[offset..offset + 4].try_into().unwrap()) as usize;
            if record_len < 20 || offset + record_len > raw_data.len() {
                break;
            }
            if let Some(record) = read_wal_v3_record(&raw_data[offset..]) {
                if record.record_type == WalRecordType::FullPageImage {
                    fpi_count += 1;
                }
            }
            offset += record_len;
        }
        fpi_count
    }

    #[test]
    fn test_checkpoint_tick_produces_fpi_wal_records() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        let data_dir = shard_dir.join("data");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create PageCache with 4 frames of 4KB, 0 of 64KB
        let page_cache = PageCache::new(4, 0);

        // Set up 2 frames: fetch pages to make them VALID, then mark dirty
        for i in 0..2usize {
            let handle = page_cache
                .fetch_page(1, i as u64, false, |buf| {
                    buf[0] = 0xDE;
                    buf[1] = (i as u8) + 1;
                    Ok(())
                })
                .unwrap();
            page_cache.unpin_page(handle);
            page_cache.mark_dirty(1, i as u64, (i + 1) as u64);
        }

        // Set FPI_PENDING on all valid frames (simulates checkpoint begin)
        page_cache.clear_all_fpi_pending();

        assert_eq!(
            page_cache.dirty_page_count(),
            2,
            "Should have 2 dirty pages"
        );

        // Create a dummy heap file (at least 8KB so pwrite succeeds for 2 pages)
        let heap_path = data_dir.join("heap-000001.mpf");
        std::fs::write(&heap_path, vec![0u8; 8192]).unwrap();

        // Create WAL writer
        let mut wal = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

        // Create checkpoint manager and begin checkpoint with dirty_count=2
        let trigger = CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9);
        let mut checkpoint_mgr = CheckpointManager::new(trigger);
        checkpoint_mgr.begin(wal.current_lsn(), 2);

        // Create manifest and control file
        let manifest_path = shard_dir.join("manifest.dat");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut control = ShardControlFile::new([0u8; 16]);
        let control_path = ShardControlFile::control_path(&shard_dir, 0);
        control.write(&control_path).unwrap();

        // Drive checkpoint ticks until all pages are flushed.
        // pages_per_tick is 1 (2 dirty / 270000 ticks, clamped to 1), so we need
        // 2 ticks of FlushPages before reaching Finalize.
        let mut tick_count = 0;
        loop {
            let finalized = handle_checkpoint_tick(
                &mut checkpoint_mgr,
                &page_cache,
                &mut wal,
                &mut manifest,
                &mut control,
                &control_path,
            );
            tick_count += 1;
            if finalized || !checkpoint_mgr.is_active() {
                break;
            }
            // Safety: don't loop forever
            assert!(
                tick_count < 100,
                "Checkpoint should complete within 100 ticks"
            );
        }

        // Flush WAL to disk
        wal.flush_sync().unwrap();

        // Read back the WAL segment and count FullPageImage records
        let seg_path = wal_dir.join("000000000001.wal");
        let raw_data = std::fs::read(&seg_path).unwrap();
        let fpi_count = count_fpi_records(&raw_data);

        assert_eq!(fpi_count, 2, "Expected exactly 2 FPI WAL records");

        // Verify dirty pages were flushed (DIRTY cleared via public API)
        assert_eq!(
            page_cache.dirty_page_count(),
            0,
            "All dirty pages should be flushed"
        );
    }

    #[test]
    fn test_checkpoint_tick_no_fpi_when_flag_not_set() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        let data_dir = shard_dir.join("data");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create PageCache with 4 frames of 4KB, 0 of 64KB
        let page_cache = PageCache::new(4, 0);

        // Set up 2 frames: VALID + DIRTY only (NO FPI_PENDING)
        for i in 0..2usize {
            let handle = page_cache
                .fetch_page(1, i as u64, false, |buf| {
                    buf[0] = 0xAB;
                    Ok(())
                })
                .unwrap();
            page_cache.unpin_page(handle);
            page_cache.mark_dirty(1, i as u64, (i + 1) as u64);
        }
        // Do NOT call clear_all_fpi_pending -- no FPI_PENDING set

        // Create a dummy heap file
        let heap_path = data_dir.join("heap-000001.mpf");
        std::fs::write(&heap_path, vec![0u8; 8192]).unwrap();

        // Create WAL writer
        let mut wal = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

        // Create checkpoint manager and begin
        let trigger = CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9);
        let mut checkpoint_mgr = CheckpointManager::new(trigger);
        checkpoint_mgr.begin(wal.current_lsn(), 2);

        // Create manifest and control file
        let manifest_path = shard_dir.join("manifest.dat");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut control = ShardControlFile::new([0u8; 16]);
        let control_path = ShardControlFile::control_path(&shard_dir, 0);
        control.write(&control_path).unwrap();

        // Drive checkpoint ticks until all pages are flushed.
        let mut tick_count = 0;
        loop {
            let finalized = handle_checkpoint_tick(
                &mut checkpoint_mgr,
                &page_cache,
                &mut wal,
                &mut manifest,
                &mut control,
                &control_path,
            );
            tick_count += 1;
            if finalized || !checkpoint_mgr.is_active() {
                break;
            }
            assert!(
                tick_count < 100,
                "Checkpoint should complete within 100 ticks"
            );
        }

        // Flush WAL to disk
        wal.flush_sync().unwrap();

        // Read back and count FPI records -- should be 0
        let seg_path = wal_dir.join("000000000001.wal");
        let raw_data = std::fs::read(&seg_path).unwrap();
        let fpi_count = count_fpi_records(&raw_data);

        assert_eq!(
            fpi_count, 0,
            "Expected 0 FPI WAL records when FPI_PENDING not set"
        );

        // DIRTY should still be cleared (pages were flushed to disk)
        assert_eq!(
            page_cache.dirty_page_count(),
            0,
            "All dirty pages should be flushed even without FPI"
        );
    }
}
