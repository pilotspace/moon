//! Test-only views of a [`SnapshotState`]'s private parts, split out of
//! `snapshot.rs` to keep it under the 1500-line cap (moon#1295).

use super::{SnapshotState, Source};

impl SnapshotState {
    /// Test-only: how many segments the table a flush froze for epoch
    /// database `db` has, if one is frozen (moon#1228 review 5).
    pub(crate) fn frozen_segments_for_test(&self, db: usize) -> Option<usize> {
        match self.sources.get(db) {
            Some(Source::Frozen(frozen)) => Some(frozen.table.segment_count()),
            _ => None,
        }
    }

    /// Test-only: the frozen table of `db` is trimmed to its epoch-start
    /// rows (review 6); `None` when `db` has no frozen table.
    pub(crate) fn frozen_trimmed_for_test(&self, db: usize) -> Option<bool> {
        match self.sources.get(db) {
            Some(Source::Frozen(frozen)) => Some(frozen.trimmed()),
            _ => None,
        }
    }

    /// Test-only: the frozen table of `db`: its bill, and what its rows
    /// hold at `entry_overhead` (review 6, N1).
    pub(crate) fn frozen_bill_and_rows_for_test(&self, db: usize) -> Option<(u64, u64)> {
        match self.sources.get(db) {
            Some(Source::Frozen(frozen)) => Some((
                frozen.bill,
                frozen
                    .table
                    .iter()
                    .map(|(k, e)| crate::storage::db::entry_overhead(k.as_bytes(), e) as u64)
                    .sum(),
            )),
            _ => None,
        }
    }

    /// Test-only: row operations the last drain's trim did.
    pub(crate) fn trim_ops_last_drain_for_test(&self) -> usize {
        self.trim_ops_last_drain
    }
}
