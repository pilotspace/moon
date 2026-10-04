//! The shard's half of an automatic snapshot round's `TXN` check (moon#1289
//! R2, review N1). The rule and why it is complete:
//! `persistence::snapshot_request::txn_round`.
//!
//! Two points on the shard thread, both in `shard::persistence_tick`:
//! - [`skip_start`], where the shard would start its part of an epoch: an
//!   automatic round is checked against this thread's hold table, and a
//!   round already abandoned is not started;
//! - [`publish_held`], where a walked snapshot would begin its publish: an
//!   automatic round waits for every shard's start check, and an abandoned
//!   one drops its temp file instead.
//!
//! A user's `BGSAVE` / `SAVE` / save rule / `SHUTDOWN` save is not a
//! registered round and passes both untouched (WS42, moon#1300).

use crate::persistence::snapshot::SnapshotState;
use crate::persistence::snapshot_request::txn_round::{self, ShardCommit, ShardStart};
use crate::runtime::channel;

/// Why an abandoned part was dropped (the state's abort reason).
const ABANDONED: &str = "automatic snapshot round abandoned: a TXN held uncommitted writes \
                         at a shard's start (moon#1289)";

/// The shard is about to start its part of `epoch`. `true`: the round is
/// abandoned — nothing was started, the part is reported to the fan-in, and
/// the caller returns.
pub(crate) fn skip_start(epoch: u64, shard_id: usize) -> bool {
    match txn_round::shard_starts(epoch, shard_id) {
        ShardStart::NotAutomatic | ShardStart::Proceed => false,
        ShardStart::Abandon => {
            crate::command::persistence::bgsave_shard_abandoned();
            true
        }
    }
}

/// A walked snapshot is about to begin its publish. `true`: not now — the
/// round waits for another shard's start (poll again next tick), or it was
/// abandoned and this part is already dropped and reported.
///
/// A snapshot that already failed is not held: its finalize reports the
/// failure, which outranks an abandon.
pub(crate) fn publish_held(
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_id: usize,
) -> bool {
    let Some(snap) = snapshot_state.as_ref() else {
        return false;
    };
    if snap.stream_failed() {
        return false;
    }
    match txn_round::shard_commit(snap.epoch) {
        ShardCommit::Go => false,
        ShardCommit::Wait => true,
        ShardCommit::Abandon => {
            abandon(snapshot_state, snapshot_reply_tx, shard_id);
            true
        }
    }
}

/// Drop this shard's part of an abandoned round: nothing published, the
/// hold's epoch not committed (no held file released), the temp file removed
/// when the state drops (the stream writer's cancel path), and the part
/// reported to the fan-in as abandoned.
fn abandon(
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_id: usize,
) {
    if let Some(snap) = snapshot_state.as_mut() {
        tracing::info!(
            shard = shard_id,
            epoch = snap.epoch,
            "automatic snapshot part dropped: another shard abandoned the round (moon#1289)"
        );
        snap.abandon(ABANDONED);
    }
    // An automatic round has no waiter; answer one defensively.
    if let Some(tx) = snapshot_reply_tx.take() {
        let _ = tx.send(Err(ABANDONED.to_string()));
    }
    crate::persistence::snapshot_cow::disarm();
    crate::storage::tiered::snapshot_hold::note_snapshot_finished(false);
    *snapshot_state = None;
    crate::command::persistence::bgsave_shard_abandoned();
}
