//! Undo log for KV transactional rollback.
//!
//! Records before-images of KV writes. On abort, the undo log is replayed
//! in reverse to restore the database to pre-transaction state.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::storage::entry::Entry;

/// A single undo record capturing the before-image of a KV write.
#[derive(Debug, Clone)]
pub enum UndoRecord {
    /// Key did not exist before write - remove on rollback.
    Insert { key: Bytes },
    /// Key had this entry before write - restore on rollback.
    Update { key: Bytes, old_entry: Entry },
    /// Key was deleted - no rollback action needed (already gone).
    Delete { key: Bytes },
}

/// Per-transaction undo log.
///
/// Uses SmallVec to inline typical small transactions (up to 16 records)
/// without heap allocation. Larger transactions spill to heap.
#[derive(Debug, Clone, Default)]
pub struct UndoLog {
    records: SmallVec<[UndoRecord; 16]>,
}

impl UndoLog {
    /// Create a new empty undo log.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an insert (key did not exist before).
    #[inline]
    pub fn record_insert(&mut self, key: Bytes) {
        self.records.push(UndoRecord::Insert { key });
    }

    /// Record an update (key had a previous entry).
    #[inline]
    pub fn record_update(&mut self, key: Bytes, old_entry: Entry) {
        self.records.push(UndoRecord::Update { key, old_entry });
    }

    /// Record a delete (key will be removed - no rollback needed).
    #[inline]
    pub fn record_delete(&mut self, key: Bytes) {
        self.records.push(UndoRecord::Delete { key });
    }

    /// Number of records in the undo log.
    #[inline]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the undo log is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Consume the undo log and return records in reverse order for rollback.
    #[inline]
    pub fn into_rollback_order(self) -> impl Iterator<Item = UndoRecord> {
        self.records.into_iter().rev()
    }

    /// Get a reference to all records (for WAL serialization).
    #[inline]
    pub fn records(&self) -> &[UndoRecord] {
        &self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_log_inline_capacity() {
        let mut log = UndoLog::new();
        // Should stay inline for 16 records
        for i in 0..16 {
            log.record_insert(Bytes::from(format!("key{i}")));
        }
        assert_eq!(log.len(), 16);
    }

    #[test]
    fn test_rollback_order_reversed() {
        let mut log = UndoLog::new();
        log.record_insert(Bytes::from_static(b"a"));
        log.record_insert(Bytes::from_static(b"b"));
        log.record_insert(Bytes::from_static(b"c"));

        let keys: Vec<_> = log
            .into_rollback_order()
            .map(|r| match r {
                UndoRecord::Insert { key } => key,
                _ => panic!("expected insert"),
            })
            .collect();

        assert_eq!(
            keys,
            vec![
                Bytes::from_static(b"c"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"a"),
            ]
        );
    }
}
