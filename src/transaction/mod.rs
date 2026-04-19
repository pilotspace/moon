//! Cross-store transaction module.
//!
//! Provides ACID transactions spanning KV, vector, and graph stores within
//! a single shard. Uses undo-log for KV rollback and write-intent tracking
//! for vector and graph operations.

pub mod abort;
pub mod commit_hooks;
pub mod kv_mvcc;
pub mod undo_log;

pub use abort::abort_cross_store_txn;
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
    /// xxh64 key_hash (not internal_id). Rollback calls
    /// `MutableSegment::mark_deleted_by_key_hash(point_id, rollback_lsn)` to
    /// tombstone the entry under MVCC. Using `key_hash` (rather than
    /// `internal_id`) keeps the intent stable across mutable-segment
    /// compactions, which renumber internal ids but preserve key hashes.
    pub point_id: u64,
    pub index_name: Bytes,
}

/// Graph write intent: `(graph_name, entity_id, is_node)`.
///
/// `graph_name` is captured at intent-creation time so TXN.ABORT can look up
/// the correct `MemGraph` on rollback. `entity_id` is the NodeKey or EdgeKey
/// encoded via `slotmap::KeyData::as_ffi()`. `is_node` distinguishes which
/// `MemGraph::remove_*` primitive the rollback loop must call.
#[derive(Debug, Clone)]
pub struct GraphIntent {
    pub graph_name: Bytes,
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

    /// Record a KV delete (captures before-image for rollback).
    #[inline]
    pub fn record_kv_delete(&mut self, key: Bytes, old_entry: crate::storage::entry::Entry) {
        self.kv_undo.record_delete(key, old_entry);
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
    ///
    /// `graph_name` is captured per-intent (not per-txn) to tolerate
    /// multi-graph transactions — rollback loops over `graph_intents` in
    /// reverse order (LIFO) and calls `MemGraph::remove_node` or
    /// `MemGraph::remove_edge` on the graph identified by `graph_name`.
    #[inline]
    pub fn record_graph(&mut self, entity_id: u64, is_node: bool, graph_name: Bytes) {
        self.graph_intents.push(GraphIntent {
            graph_name,
            entity_id,
            is_node,
        });
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
        txn.record_graph(200, true, Bytes::from_static(b"g"));
        assert!(txn.has_modifications());
    }

    #[test]
    fn test_graph_intent_records_graph_name() {
        let mut txn = CrossStoreTxn::new(1, 0);
        txn.record_graph(10, true, Bytes::from_static(b"g1"));
        txn.record_graph(11, false, Bytes::from_static(b"g2"));
        assert_eq!(txn.graph_intents.len(), 2);
        assert_eq!(txn.graph_intents[0].graph_name.as_ref(), b"g1");
        assert_eq!(txn.graph_intents[0].entity_id, 10);
        assert!(txn.graph_intents[0].is_node);
        assert_eq!(txn.graph_intents[1].graph_name.as_ref(), b"g2");
        assert_eq!(txn.graph_intents[1].entity_id, 11);
        assert!(!txn.graph_intents[1].is_node);
    }

    #[test]
    fn test_graph_intent_reverse_order_is_lifo() {
        let mut txn = CrossStoreTxn::new(1, 0);
        // Push three intents; reverse iteration must yield them LIFO so Plan
        // 166-03 can remove edges before their endpoint nodes on rollback.
        txn.record_graph(1, true, Bytes::from_static(b"g"));
        txn.record_graph(2, true, Bytes::from_static(b"g"));
        txn.record_graph(3, false, Bytes::from_static(b"g"));
        let reversed: Vec<u64> = txn.graph_intents.iter().rev().map(|g| g.entity_id).collect();
        assert_eq!(reversed, vec![3, 2, 1]);
        // Spot-check the first reverse element matches the last push.
        assert_eq!(
            txn.graph_intents.iter().rev().next().unwrap().entity_id,
            3
        );
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
