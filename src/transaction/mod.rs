//! Cross-store transaction module.
//!
//! Provides ACID transactions spanning KV, vector, and graph stores within
//! a single shard. Uses undo-log for KV rollback and write-intent tracking
//! for vector and graph operations.

pub mod abort;
pub mod commit_hooks;
pub mod conn_capture;
pub mod kv_compensation;
pub mod kv_mvcc;
pub mod undo_log;

pub use abort::abort_cross_store_txn;
pub use commit_hooks::{DeferredHnswInsert, DeferredHnswInserts};
pub use kv_mvcc::{KvWriteIntents, WriteIntent};
pub use undo_log::{UndoLog, UndoRecord};

use bytes::Bytes;
use smallvec::SmallVec;

// Phase 174 FIX-01: Graph undo operations for SET/DELETE/MERGE rollback.
#[cfg(feature = "graph")]
use crate::graph::types::PropertyValue;

/// Phase 174 FIX-01: Graph undo operation — reverses a Cypher SET, DELETE, or
/// MERGE ON MATCH SET that was applied to write_buf during execute_mut.
///
/// Processed by `abort_cross_store_txn` in LIFO order (after existing
/// `graph_intents` which handle CreateNode/CreateEdge removal).
#[cfg(feature = "graph")]
#[derive(Debug, Clone)]
pub enum GraphUndoOp {
    /// Restore a property to its pre-SET value. `old_value = None` means the
    /// property did not exist before SET and should be removed on rollback.
    RestoreProperty {
        graph_name: Bytes,
        entity_id: u64,
        is_node: bool,
        prop_key: u16,
        old_value: Option<PropertyValue>,
    },
    /// Un-soft-delete a node (set `deleted_lsn = u64::MAX`) and restore live
    /// count. The `delete_lsn` field records the LSN used for the soft-delete
    /// so we can also un-soft-delete incident edges deleted at the same LSN.
    UndeleteNode {
        graph_name: Bytes,
        node_id: u64,
        delete_lsn: u64,
    },
    /// Un-soft-delete an edge (set `deleted_lsn = u64::MAX`) and restore live
    /// count.
    UndeleteEdge { graph_name: Bytes, edge_id: u64 },
}

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
    /// Logical db index the connection had SELECTed at `TXN.BEGIN`. Written
    /// into the `XactCommitV2` WAL header so crash replay restores the KV ops
    /// into this db (previously replay hardcoded db 0). KV writes inside the
    /// txn use the connection's live `selected_db` per command; a mid-txn
    /// `SELECT` is a pre-existing single-db assumption shared with the
    /// forward-image encoder, which reads back from one db at commit.
    pub db_index: usize,
    /// KV undo log for rollback.
    pub kv_undo: UndoLog,
    /// Vector point_ids modified in this transaction.
    pub vector_intents: SmallVec<[VectorIntent; 8]>,
    /// Graph entity_ids modified in this transaction.
    pub graph_intents: SmallVec<[GraphIntent; 8]>,
    /// Phase 174 FIX-01: Graph undo operations for SET/DELETE/MERGE rollback.
    /// Processed in LIFO order by abort_cross_store_txn AFTER graph_intents.
    #[cfg(feature = "graph")]
    pub graph_undo: Vec<GraphUndoOp>,
    /// MQ messages to enqueue on commit.
    pub mq_intents: SmallVec<[MqIntent; 4]>,
    /// #499: number of operations a TXN guard REJECTED while this transaction
    /// was open (cross-shard write, MOVE, COPY ... DB, SWAPDB, cross-shard
    /// Cypher write). Non-zero poisons the transaction: `TXN.COMMIT` refuses
    /// and rolls back instead of applying the accepted subset, mirroring
    /// Redis's `CLIENT_DIRTY_EXEC` → `EXECABORT` contract for MULTI.
    pub rejected_ops: u32,
    /// Name of the FIRST rejected command, for the commit-time error message.
    /// Captured once (an error path, never a hot path).
    pub first_rejected_cmd: Option<Bytes>,
}

impl CrossStoreTxn {
    /// Create a new cross-store transaction bound to the connection's
    /// currently SELECTed logical db.
    #[inline]
    pub fn new(txn_id: u64, snapshot_lsn: u64, db_index: usize) -> Self {
        Self {
            txn_id,
            snapshot_lsn,
            db_index,
            kv_undo: UndoLog::new(),
            vector_intents: SmallVec::new(),
            graph_intents: SmallVec::new(),
            #[cfg(feature = "graph")]
            graph_undo: Vec::new(),
            mq_intents: SmallVec::new(),
            rejected_ops: 0,
            first_rejected_cmd: None,
        }
    }

    /// #499: record that `cmd` was rejected by a TXN guard inside this
    /// transaction body, poisoning it.
    ///
    /// The client already saw the per-op error; this is what makes
    /// `TXN.COMMIT` refuse afterwards instead of silently committing the
    /// accepted subset.
    #[inline]
    pub fn record_rejected_op(&mut self, cmd: &[u8]) {
        self.rejected_ops = self.rejected_ops.saturating_add(1);
        if self.first_rejected_cmd.is_none() {
            self.first_rejected_cmd = Some(Bytes::copy_from_slice(cmd));
        }
    }

    /// [`record_rejected_op`](Self::record_rejected_op) for `count` rejected
    /// ops of which `first_cmd` came first — a script whose writes the TXN
    /// refused (moon#1285, PR #1301 review), counted once per refusal.
    #[inline]
    pub fn record_rejected_ops(&mut self, first_cmd: &[u8], count: u32) {
        if count == 0 {
            return;
        }
        self.rejected_ops = self.rejected_ops.saturating_add(count);
        if self.first_rejected_cmd.is_none() {
            self.first_rejected_cmd = Some(Bytes::copy_from_slice(first_cmd));
        }
    }

    /// #499: true when at least one op in the body was rejected — the
    /// transaction may not commit.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.rejected_ops > 0
    }

    /// Record a KV insert in database `db` (key did not exist).
    #[inline]
    pub fn record_kv_insert(&mut self, db: usize, key: Bytes) {
        self.kv_undo.record_insert(db, key);
    }

    /// Record a KV update in database `db` (key had previous entry).
    #[inline]
    pub fn record_kv_update(
        &mut self,
        db: usize,
        key: Bytes,
        old_entry: crate::storage::entry::Entry,
    ) {
        self.kv_undo.record_update(db, key, old_entry);
    }

    /// Record a KV delete in database `db` (captures before-image for rollback).
    #[inline]
    pub fn record_kv_delete(
        &mut self,
        db: usize,
        key: Bytes,
        old_entry: crate::storage::entry::Entry,
    ) {
        self.kv_undo.record_delete(db, key, old_entry);
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

    /// Phase 174 FIX-01: Record a graph undo operation for SET/DELETE/MERGE
    /// rollback. Processed in LIFO order by `abort_cross_store_txn`.
    #[cfg(feature = "graph")]
    #[inline]
    pub fn record_graph_undo(&mut self, op: GraphUndoOp) {
        self.graph_undo.push(op);
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
            || {
                #[cfg(feature = "graph")]
                {
                    !self.graph_undo.is_empty()
                }
                #[cfg(not(feature = "graph"))]
                {
                    false
                }
            }
            || !self.mq_intents.is_empty()
    }

    /// Get KV undo log for rollback (consumes self).
    #[inline]
    pub fn into_kv_undo(self) -> UndoLog {
        self.kv_undo
    }
}

/// Keyless writes `command::dispatch` executes whose pre-image is a whole
/// database (`FLUSHDB`, `FLUSHALL`, `SWAPDB`): the undo log cannot capture
/// them, so a TXN refuses them before they run and is poisoned (#499) — on
/// the connection leg of both runtimes and from a script (moon#1285, PR #1301
/// review). The connection leg used to accept `FLUSHDB` / `FLUSHALL`, and
/// `TXN.ABORT` then answered `+OK` restoring nothing.
pub(crate) const TXN_WHOLE_DB_WRITES: [&[u8]; 3] = [b"FLUSHDB", b"FLUSHALL", b"SWAPDB"];

/// Is `cmd` one of [`TXN_WHOLE_DB_WRITES`]?
#[inline]
pub(crate) fn is_txn_whole_db_write(cmd: &[u8]) -> bool {
    TXN_WHOLE_DB_WRITES
        .iter()
        .any(|k| cmd.eq_ignore_ascii_case(k))
}

/// The keys a connection's write (other than `DEL` / `UNLINK`) inside an open
/// TXN undo-captures and holds write intents on, on both runtimes' generic
/// write leg (moon#500).
///
/// The shared key walker's WRITE positions. Only for an argv the walker
/// cannot enumerate does it fall back to the primary key — fewer keys than
/// the historical single-key capture would be a regression. It used to fall
/// back whenever the written set was EMPTY, which also covered an argv the
/// walker read and found write-free: `SORT src` or `GEORADIUS src ...`
/// without `STORE` captured `src`, a key they only READ, so `TXN.ABORT`
/// restored its pre-image over another client's write (logged to the AOF
/// and replicas) and the write intent hid it from other transactions
/// (moon#1285, PR #1301 review).
pub(crate) fn conn_txn_capture_keys(
    cmd: &[u8],
    args: &[crate::protocol::Frame],
) -> SmallVec<[Bytes; 4]> {
    match crate::tracking::invalidation::written_keys_if_known(cmd, args) {
        Some(written) => written,
        None => crate::server::conn::shared::extract_primary_key(cmd, args)
            .cloned()
            .into_iter()
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture_keys(cmd: &str, args: &[&str]) -> Vec<Bytes> {
        let args: Vec<crate::protocol::Frame> = args
            .iter()
            .map(|a| crate::protocol::Frame::BulkString(Bytes::copy_from_slice(a.as_bytes())))
            .collect();
        conn_txn_capture_keys(cmd.as_bytes(), &args).into_vec()
    }

    /// PR #1301 review round 4: the whole-database writes a TXN refuses, in
    /// any case, and nothing else.
    #[test]
    fn whole_db_writes_are_named_in_any_case() {
        for cmd in ["FLUSHDB", "flushdb", "FLUSHALL", "FlushAll", "SWAPDB"] {
            assert!(is_txn_whole_db_write(cmd.as_bytes()), "{cmd}");
        }
        for cmd in ["FLUSH", "FLUSHDBX", "DEL", "SELECT", "MOVE", "COPY", ""] {
            assert!(!is_txn_whole_db_write(cmd.as_bytes()), "{cmd}");
        }
    }

    /// PR #1301 review round 3: a write whose keys are all READ in this argv
    /// captures nothing; one the walker cannot read keeps the primary-key
    /// fallback; written keys are captured as before.
    #[test]
    fn conn_txn_capture_keys_never_captures_a_key_only_read() {
        let none: Vec<Bytes> = Vec::new();
        assert_eq!(capture_keys("SORT", &["src"]), none);
        assert_eq!(
            capture_keys("SORT", &["src", "ALPHA", "LIMIT", "0", "1"]),
            none
        );
        assert_eq!(capture_keys("SORT", &["src", "BY", "w_*"]), none);
        assert_eq!(capture_keys("GEORADIUS", &["g", "0", "0", "1", "km"]), none);
        assert_eq!(
            capture_keys("GEORADIUSBYMEMBER", &["g", "m", "1", "km", "ASC"]),
            none
        );
        assert_eq!(
            capture_keys("SORT", &["src", "STORE", "dst"]),
            vec![Bytes::from("dst")]
        );
        assert_eq!(
            capture_keys("GEORADIUS", &["g", "0", "0", "1", "km", "STORE", "d"]),
            vec![Bytes::from("d")]
        );
        assert_eq!(capture_keys("SET", &["k", "v"]), vec![Bytes::from("k")]);
        assert_eq!(
            capture_keys("MSET", &["a", "1", "b", "2"]),
            vec![Bytes::from("a"), Bytes::from("b")]
        );
        // The walker cannot enumerate a malformed `numkeys`: the primary-key
        // fallback still applies (here `extract_primary_key`'s answer).
        assert_eq!(
            capture_keys("LMPOP", &["5", "a", "b", "LEFT"]),
            vec![Bytes::from("a")]
        );
    }

    #[test]
    fn test_cross_store_txn_new() {
        let txn = CrossStoreTxn::new(42, 41, 0);
        assert_eq!(txn.txn_id, 42);
        assert_eq!(txn.snapshot_lsn, 41);
        assert!(!txn.has_modifications());
    }

    /// #499: a fresh transaction is clean; a guard rejection poisons it and
    /// the FIRST rejected command name is what the commit error reports.
    #[test]
    fn test_rejected_ops_poison_txn() {
        let mut txn = CrossStoreTxn::new(7, 6, 0);
        assert!(!txn.is_dirty());
        assert_eq!(txn.rejected_ops, 0);
        assert!(txn.first_rejected_cmd.is_none());

        txn.record_rejected_op(b"SET");
        txn.record_rejected_op(b"MOVE");

        assert!(txn.is_dirty());
        assert_eq!(txn.rejected_ops, 2);
        assert_eq!(txn.first_rejected_cmd.as_deref(), Some(&b"SET"[..]));
        // A rejected op applied nothing, so it is not a "modification".
        assert!(!txn.has_modifications());

        // A script's refusals count one each; the first command stays.
        txn.record_rejected_ops(b"FLUSHDB", 3);
        txn.record_rejected_ops(b"SWAPDB", 0);
        assert_eq!(txn.rejected_ops, 5);
        assert_eq!(txn.first_rejected_cmd.as_deref(), Some(&b"SET"[..]));
        let mut fresh = CrossStoreTxn::new(8, 7, 0);
        fresh.record_rejected_ops(b"FLUSHDB", 0);
        assert!(!fresh.is_dirty(), "zero refusals poison nothing");
        fresh.record_rejected_ops(b"FLUSHDB", 2);
        assert_eq!(fresh.rejected_ops, 2);
        assert_eq!(fresh.first_rejected_cmd.as_deref(), Some(&b"FLUSHDB"[..]));
    }

    #[test]
    fn test_has_modifications_kv() {
        let mut txn = CrossStoreTxn::new(1, 0, 0);
        assert!(!txn.has_modifications());
        txn.record_kv_insert(0, Bytes::from_static(b"key"));
        assert!(txn.has_modifications());
    }

    #[test]
    fn test_has_modifications_vector() {
        let mut txn = CrossStoreTxn::new(1, 0, 0);
        txn.record_vector(100, Bytes::from_static(b"idx"));
        assert!(txn.has_modifications());
    }

    #[test]
    fn test_has_modifications_graph() {
        let mut txn = CrossStoreTxn::new(1, 0, 0);
        txn.record_graph(200, true, Bytes::from_static(b"g"));
        assert!(txn.has_modifications());
    }

    #[test]
    fn test_graph_intent_records_graph_name() {
        let mut txn = CrossStoreTxn::new(1, 0, 0);
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
        let mut txn = CrossStoreTxn::new(1, 0, 0);
        // Push three intents; reverse iteration must yield them LIFO so Plan
        // 166-03 can remove edges before their endpoint nodes on rollback.
        txn.record_graph(1, true, Bytes::from_static(b"g"));
        txn.record_graph(2, true, Bytes::from_static(b"g"));
        txn.record_graph(3, false, Bytes::from_static(b"g"));
        let reversed: Vec<u64> = txn
            .graph_intents
            .iter()
            .rev()
            .map(|g| g.entity_id)
            .collect();
        assert_eq!(reversed, vec![3, 2, 1]);
        // Spot-check the first reverse element matches the last push.
        assert_eq!(txn.graph_intents.iter().next_back().unwrap().entity_id, 3);
    }

    #[test]
    fn test_mq_intent_tracking() {
        let mut txn = CrossStoreTxn::new(10, 9, 0);
        assert!(!txn.has_modifications());

        txn.record_mq(
            Bytes::from_static(b"orders"),
            vec![(Bytes::from_static(b"item"), Bytes::from_static(b"widget"))],
        );
        assert!(txn.has_modifications());
        assert_eq!(txn.mq_intents.len(), 1);
        assert_eq!(txn.mq_intents[0].queue_key.as_ref(), b"orders");
        assert_eq!(txn.mq_intents[0].fields.len(), 1);
    }
}
