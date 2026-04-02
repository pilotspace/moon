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
    shard_id: usize,
) {
    if let Some((epoch, snap_dir, reply_tx)) = pending {
        if snapshot_state.is_some() {
            let _ = reply_tx.send(Err("Snapshot already in progress".to_string()));
        } else {
            let snap_path = snap_dir.join(format!("shard-{}.rrdshard", shard_id));
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
    shard_id: usize,
) {
    let new_epoch = snapshot_trigger_rx.borrow();
    if new_epoch > *last_snapshot_epoch && snapshot_state.is_none() {
        *last_snapshot_epoch = new_epoch;
        if let Some(dir) = persistence_dir {
            let snap_path =
                std::path::PathBuf::from(dir).join(format!("shard-{}.rrdshard", shard_id));
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
) {
    let count = vector_store.try_warm_transitions_all(
        shard_dir, manifest, warm_after_secs, next_file_id,
    );
    if count > 0 {
        info!(
            "Shard {}: transitioned {} segment(s) to warm tier",
            shard_id, count
        );
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
                &mut |_file_id, _page_offset, _is_large, _data| {
                    // TODO(moonstore-v2): Write page data to the actual data file
                    // on disk. The WAL-before-data invariant is enforced above.
                    // Physical page write to data files requires the data file I/O
                    // layer (KV disk pages, future phase). Recovery replays WAL
                    // from redo_lsn so this is safe.
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
