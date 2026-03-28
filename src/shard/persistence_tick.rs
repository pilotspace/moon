//! Persistence tick helpers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains snapshot begin handling,
//! auto-save trigger checking, snapshot advance/finalize prep, and WAL flush.

use std::cell::RefCell;
use std::rc::Rc;

use tracing::info;

use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal::WalWriter;
use crate::runtime::channel;
use crate::storage::Database;

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
    databases: &Rc<RefCell<Vec<Database>>>,
    shard_id: usize,
) {
    if let Some((epoch, snap_dir, reply_tx)) = pending {
        if snapshot_state.is_some() {
            let _ = reply_tx.send(Err("Snapshot already in progress".to_string()));
        } else {
            let dbs = databases.borrow();
            let snap_path = snap_dir.join(format!("shard-{}.rrdshard", shard_id));
            *snapshot_state = Some(SnapshotState::new(shard_id as u16, epoch, &dbs, snap_path));
            *snapshot_reply_tx = Some(reply_tx);
            drop(dbs);
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
    databases: &Rc<RefCell<Vec<Database>>>,
    persistence_dir: &Option<String>,
    shard_id: usize,
) {
    let new_epoch = snapshot_trigger_rx.borrow();
    if new_epoch > *last_snapshot_epoch && snapshot_state.is_none() {
        *last_snapshot_epoch = new_epoch;
        if let Some(dir) = persistence_dir {
            let snap_path =
                std::path::PathBuf::from(dir).join(format!("shard-{}.rrdshard", shard_id));
            let dbs = databases.borrow();
            *snapshot_state = Some(SnapshotState::new(
                shard_id as u16,
                new_epoch,
                &dbs,
                snap_path,
            ));
            drop(dbs);
        }
    }
}

/// Advance snapshot one segment and check if done (synchronous part).
///
/// Returns `true` if the snapshot is complete and ready for async finalization.
pub(crate) fn advance_snapshot_segment(
    snapshot_state: &mut Option<SnapshotState>,
    databases: &Rc<RefCell<Vec<Database>>>,
) -> bool {
    if let Some(snap) = snapshot_state {
        let dbs = databases.borrow();
        snap.advance_one_segment(&dbs)
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
