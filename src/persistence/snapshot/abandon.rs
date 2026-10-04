//! Abandon a snapshot that did nothing wrong (moon#1289 R2): an automatic
//! round another shard abandoned because a `TXN` held uncommitted writes at
//! its start (`persistence::snapshot_request::txn_round`).

use super::SnapshotState;

impl SnapshotState {
    /// [`SnapshotState::abort`] without its error line: the caller logs the
    /// abandon once. Nothing is published; dropping the state afterwards
    /// removes the temp file (the stream writer's cancel path).
    pub(crate) fn abandon(&mut self, why: &'static str) {
        // `abort` logs only the first reason; one already set keeps it quiet.
        self.aborted.get_or_insert(why);
        self.abort(why);
    }
}
