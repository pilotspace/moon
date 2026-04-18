//! Cross-store transaction module.
//!
//! Provides ACID transactions spanning KV, vector, and graph stores within
//! a single shard. Uses undo-log for KV rollback and write-intent tracking
//! for vector and graph operations.

pub mod commit_hooks;
pub mod kv_mvcc;
pub mod undo_log;

pub use commit_hooks::{DeferredHnswInsert, DeferredHnswInserts};
pub use kv_mvcc::{KvWriteIntents, WriteIntent};
pub use undo_log::{UndoLog, UndoRecord};

use bytes::Bytes;
use smallvec::SmallVec;

/// Type alias for unified LSN across all stores.
pub type UnifiedLsn = u64;

/// Vector write intent: (point_id, index_name)
#[derive(Debug, Clone)]
pub struct VectorIntent {
    pub point_id: u64,
    pub index_name: Bytes,
}

/// Graph write intent: (entity_id, is_node: bool)
#[derive(Debug, Clone)]
pub struct GraphIntent {
    pub entity_id: u64,
    pub is_node: bool,
}

/// MQ write intent: message to enqueue on TXN.COMMIT.
#[derive(Debug, Clone)]
pub struct MqIntent {
    pub queue_key: Bytes,
    pub fields: Vec<(Bytes, Bytes)>,
}

/// Cross-store transaction state.
///
/// Tracks all modifications across KV, vector, and graph stores.
/// On commit: all changes become visible atomically.
/// On abort: KV undo log is replayed; vector/graph intents are discarded.
#[derive(Debug, Clone)]
pub struct CrossStoreTxn {
    /// Transaction ID (same as LSN at begin time).
    pub txn_id: u64,
    /// Snapshot LSN for reads.
    pub snapshot_lsn: u64,
    /// KV undo log for rollback.
    pub kv_undo: UndoLog,
    /// Vector point_ids modified in this transaction.
    pub vector_intents: SmallVec<[VectorIntent; 8]>,
    /// Graph entity_ids modified in this transaction.
    pub graph_intents: SmallVec<[GraphIntent; 8]>,
    /// MQ messages to enqueue on commit.
    pub mq_intents: SmallVec<[MqIntent; 4]>,
}

impl CrossStoreTxn {
    /// Create a new cross-store transaction.
    #[inline]
    pub fn new(txn_id: u64, snapshot_lsn: u64) -> Self {
        Self {
            txn_id,
            snapshot_lsn,
            kv_undo: UndoLog::new(),
            vector_intents: SmallVec::new(),
            graph_intents: SmallVec::new(),
            mq_intents: SmallVec::new(),
        }
    }

    /// Record a KV insert (key did not exist).
    #[inline]
    pub fn record_kv_insert(&mut self, key: Bytes) {
        self.kv_undo.record_insert(key);
    }

    /// Record a KV update (key had previous entry).
    #[inline]
    pub fn record_kv_update(&mut self, key: Bytes, old_entry: crate::storage::entry::Entry) {
        self.kv_undo.record_update(key, old_entry);
    }

    /// Record a KV delete.
    #[inline]
    pub fn record_kv_delete(&mut self, key: Bytes) {
        self.kv_undo.record_delete(key);
    }

    /// Record a vector modification.
    #[inline]
    pub fn record_vector(&mut self, point_id: u64, index_name: Bytes) {
        self.vector_intents.push(VectorIntent {
            point_id,
            index_name,
        });
    }

    /// Record a graph modification.
    #[inline]
    pub fn record_graph(&mut self, entity_id: u64, is_node: bool) {
        self.graph_intents.push(GraphIntent { entity_id, is_node });
    }

    /// Record an MQ publish intent (MQ.PUBLISH inside TXN).
    #[inline]
    pub fn record_mq(&mut self, queue_key: Bytes, fields: Vec<(Bytes, Bytes)>) {
        self.mq_intents.push(MqIntent { queue_key, fields });
    }

    /// Check if this transaction has any modifications.
    #[inline]
    pub fn has_modifications(&self) -> bool {
        !self.kv_undo.is_empty()
            || !self.vector_intents.is_empty()
            || !self.graph_intents.is_empty()
            || !self.mq_intents.is_empty()
    }

    /// Get KV undo log for rollback (consumes self).
    #[inline]
    pub fn into_kv_undo(self) -> UndoLog {
        self.kv_undo
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cross_store_txn_new() {
        let txn = CrossStoreTxn::new(42, 41);
        assert_eq!(txn.txn_id, 42);
        assert_eq!(txn.snapshot_lsn, 41);
        assert!(!txn.has_modifications());
    }

    #[test]
    fn test_has_modifications_kv() {
        let mut txn = CrossStoreTxn::new(1, 0);
        assert!(!txn.has_modifications());
        txn.record_kv_insert(Bytes::from_static(b"key"));
        assert!(txn.has_modifications());
    }

    #[test]
    fn test_has_modifications_vector() {
        let mut txn = CrossStoreTxn::new(1, 0);
        txn.record_vector(100, Bytes::from_static(b"idx"));
        assert!(txn.has_modifications());
    }

    #[test]
    fn test_has_modifications_graph() {
        let mut txn = CrossStoreTxn::new(1, 0);
        txn.record_graph(200, true);
        assert!(txn.has_modifications());
    }

    #[test]
    fn test_mq_intent_tracking() {
        let mut txn = CrossStoreTxn::new(10, 9);
        assert!(!txn.has_modifications());

        txn.record_mq(
            Bytes::from_static(b"orders"),
            vec![(
                Bytes::from_static(b"item"),
                Bytes::from_static(b"widget"),
            )],
        );
        assert!(txn.has_modifications());
        assert_eq!(txn.mq_intents.len(), 1);
        assert_eq!(txn.mq_intents[0].queue_key.as_ref(), b"orders");
        assert_eq!(txn.mq_intents[0].fields.len(), 1);
    }
}
