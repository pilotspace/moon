//! Post-commit hooks for deferred operations.
//!
//! HNSW graph updates are deferred to post-commit to avoid phantom neighbors
//! when a transaction aborts. The deferred queue collects point_ids during
//! the transaction and processes them after commit is durable.

use std::collections::VecDeque;

use bytes::Bytes;

/// A deferred HNSW insertion.
#[derive(Debug, Clone)]
pub struct DeferredHnswInsert {
    /// Point ID in the mutable segment.
    pub point_id: u64,
    /// Index name (collection).
    pub index_name: Bytes,
    /// Transaction ID that committed this insert.
    pub txn_id: u64,
}

/// Queue of deferred HNSW insertions.
///
/// After TXN.COMMIT, the handler drains this queue and schedules HNSW
/// graph updates. This is a cold path (per-commit, not per-insert).
#[derive(Debug, Clone, Default)]
pub struct DeferredHnswInserts {
    queue: VecDeque<DeferredHnswInsert>,
}

impl DeferredHnswInserts {
    /// Create a new empty queue.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueue a deferred HNSW insert.
    #[inline]
    pub fn enqueue(&mut self, point_id: u64, index_name: Bytes, txn_id: u64) {
        self.queue.push_back(DeferredHnswInsert {
            point_id,
            index_name,
            txn_id,
        });
    }

    /// Drain all deferred inserts for a specific transaction.
    ///
    /// Returns an iterator over the inserts that should be processed.
    /// Called after TXN.COMMIT is durable.
    pub fn drain_for_txn(&mut self, txn_id: u64) -> impl Iterator<Item = DeferredHnswInsert> + '_ {
        // Drain matching items while preserving others
        let mut result = Vec::new();
        self.queue.retain(|item| {
            if item.txn_id == txn_id {
                result.push(item.clone());
                false // Remove from queue
            } else {
                true // Keep in queue
            }
        });
        result.into_iter()
    }

    /// Discard all deferred inserts for a specific transaction.
    ///
    /// Called after TXN.ABORT - these inserts should never materialize.
    pub fn discard_for_txn(&mut self, txn_id: u64) {
        self.queue.retain(|item| item.txn_id != txn_id);
    }

    /// Number of pending deferred inserts.
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Check if queue is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enqueue_and_drain() {
        let mut q = DeferredHnswInserts::new();
        q.enqueue(1, Bytes::from_static(b"idx1"), 100);
        q.enqueue(2, Bytes::from_static(b"idx1"), 100);
        q.enqueue(3, Bytes::from_static(b"idx2"), 101);

        let drained: Vec<_> = q.drain_for_txn(100).collect();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].point_id, 1);
        assert_eq!(drained[1].point_id, 2);

        // Queue should only have txn 101's insert left
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn test_discard_for_txn() {
        let mut q = DeferredHnswInserts::new();
        q.enqueue(1, Bytes::from_static(b"idx1"), 100);
        q.enqueue(2, Bytes::from_static(b"idx1"), 101);

        q.discard_for_txn(100);

        assert_eq!(q.len(), 1);
        let remaining: Vec<_> = q.drain_for_txn(101).collect();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].point_id, 2);
    }
}
