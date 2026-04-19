//! Shared cross-store transaction abort helper.
//!
//! Single source of truth for rolling back a `CrossStoreTxn`. Called from:
//!   1. Explicit `TXN.ABORT` in `handler_sharded.rs` and `handler_monoio.rs`
//!      (Phase 166 Plan 03 — this file).
//!   2. Disconnect cleanup in both handlers (Phase 166 Plan 04 — forthcoming,
//!      reuses this exact helper to eliminate two-handler drift).
//!
//! # Execution sequence (Phase 161 T-161-01 lock ordering)
//!
//! The helper performs five phases under the per-shard lock hierarchy defined
//! in Phase 161 Plan 02:
//!
//! ```text
//!   write_db guard          ─┐
//!     KV undo replay         │ drop before next layer
//!     drop(db)               │
//!   graph_store_write guard ─┤ drop before next layer
//!     graph_intents reverse replay
//!     (edges removed before nodes via LIFO iteration)
//!     drain graph WAL records
//!     drop(gs)               │
//!   vector_store guard      ─┤ drop before next layer
//!     mark_deleted_by_key_hash for every vector_intent
//!     txn_manager_mut().abort(txn.txn_id)
//!     // LOCK-ORDER: drop vector_store before kv_intents
//!     drop(vector_store)     │
//!   kv_intents               │ no guard held across boundary
//!     release_txn
//!   hnsw_queue               │ no guard held across boundary
//!     discard_for_txn
//! ```
//!
//! The `// LOCK-ORDER: drop vector_store before kv_intents` marker below is
//! an explicit audit gate (Phase 166 Plan 03 acceptance criterion) — it
//! preserves the Phase 161 T-161-01 invariant regardless of guard variable
//! naming.
//!
//! # Error discipline
//!
//! Every step is fault-tolerant:
//! - Missing graph (e.g. `GRAPH.DELETE` earlier in the transaction) → log at
//!   `tracing::warn!` and skip. No panic.
//! - Missing vector index (e.g. `FT.DROPINDEX` earlier) → log and skip.
//! - `MemGraph::remove_node` / `remove_edge` return `false` on invalid or
//!   already-deleted keys — treated as idempotent no-op.
//! - No `unwrap()` / `expect()` anywhere in the helper — every failure path
//!   logs and continues.
//!
//! # WAL durability
//!
//! `TransactionManager::abort` emits the `XactAbort`-equivalent internal
//! state transition. The graph rollback drains any WAL records produced by
//! `MemGraph::remove_node` / `remove_edge` into `shard_databases.wal_append`
//! so crash recovery replay sees the soft-delete records in the same order
//! the live write path would have produced them. KV undo replay writes
//! directly to the database without emitting new WAL records — the
//! XactCommit record for this txn was never written (this is ABORT, not
//! COMMIT), so WAL replay correctly reconstructs pre-txn state by skipping
//! the uncommitted forward records.

use crate::shard::shared_databases::ShardDatabases;
use crate::transaction::{CrossStoreTxn, UndoRecord};

#[cfg(feature = "graph")]
use bytes::Bytes;
#[cfg(feature = "graph")]
use crate::graph::types::{EdgeKey, NodeKey};

/// Roll back every store side-effect of `txn`.
///
/// Consumes the transaction by value — the caller must have already
/// `.take()`'d it off `conn.active_cross_txn`. Idempotent on a re-entry
/// because the transaction is consumed and the per-shard side tables
/// (`kv_intents`, `hnsw_queue`) are keyed by `txn_id` and treat missing
/// entries as no-op.
///
/// # Arguments
///
/// * `shard_databases` — the per-server `ShardDatabases` registry (both
///   handlers expose this as `ctx.shard_databases` or an `Arc<_>` field).
/// * `shard_id`        — the shard this transaction lives on.
/// * `selected_db`     — the logical KV database index (from
///   `conn.selected_db`) used to scope the undo replay.
/// * `txn`             — the transaction to abort. Consumed.
///
/// # Safety / concurrency
///
/// Runs on the shard event-loop thread. All locks taken are per-shard
/// `parking_lot` guards; no `.await` points are crossed while any guard is
/// held. Follows Phase 161 lock ordering: each layer's guard is dropped
/// before the next layer is accessed.
#[allow(clippy::needless_pass_by_value)]
pub fn abort_cross_store_txn(
    shard_databases: &ShardDatabases,
    shard_id: usize,
    selected_db: usize,
    txn: CrossStoreTxn,
) {
    let txn_id = txn.txn_id;

    // ------------------------------------------------------------------
    // 1. KV undo replay — walk the undo log in reverse insertion order
    //    and restore before-images. Mirrors the original logic lifted
    //    out of handler_sharded/handler_monoio TXN.ABORT.
    // ------------------------------------------------------------------
    {
        let mut db = shard_databases.write_db(shard_id, selected_db);
        for record in txn.kv_undo.into_rollback_order() {
            match record {
                UndoRecord::Insert { key } => {
                    db.remove(&key);
                }
                UndoRecord::Update { key, old_entry } => {
                    db.set(key, old_entry);
                }
                UndoRecord::Delete { key, old_entry } => {
                    db.set(key, old_entry);
                }
            }
        }
        // db guard drops at scope end — releases write_db before next layer.
    }

    // ------------------------------------------------------------------
    // 2. Graph rollback — iterate intents in REVERSE (LIFO) so edges are
    //    removed before their endpoint nodes. Gated on the `graph`
    //    feature: the `graph_intents` field exists unconditionally on
    //    `CrossStoreTxn`, but the MemGraph types and GraphStore accessor
    //    only compile with `--features graph`.
    // ------------------------------------------------------------------
    #[cfg(feature = "graph")]
    {
        if !txn.graph_intents.is_empty() {
            let mut gs = shard_databases.graph_store_write(shard_id);
            // Rollback LSN: allocate from GraphStore's own LSN stream so
            // replay order stays monotonic with forward writes (Phase 158
            // CSR v2 migration tests verify this ordering).
            let rollback_lsn = gs.allocate_lsn();
            for intent in txn.graph_intents.iter().rev() {
                let Some(graph) = gs.get_graph_mut(&intent.graph_name) else {
                    // Graph was dropped mid-txn (or never created) — silent
                    // skip per research § "Graph name changes mid-txn".
                    tracing::warn!(
                        txn_id,
                        graph_name = ?intent.graph_name,
                        entity_id = intent.entity_id,
                        is_node = intent.is_node,
                        "txn abort: graph missing at rollback time, skipping intent",
                    );
                    continue;
                };
                if intent.is_node {
                    let nk = NodeKey::from(slotmap::KeyData::from_ffi(intent.entity_id));
                    let removed = graph.write_buf.remove_node(nk, rollback_lsn);
                    if !removed {
                        tracing::warn!(
                            txn_id,
                            entity_id = intent.entity_id,
                            "txn abort: remove_node returned false (already deleted or invalid key)",
                        );
                    }
                } else {
                    let ek = EdgeKey::from(slotmap::KeyData::from_ffi(intent.entity_id));
                    let removed = graph.write_buf.remove_edge(ek, rollback_lsn);
                    if !removed {
                        tracing::warn!(
                            txn_id,
                            entity_id = intent.entity_id,
                            "txn abort: remove_edge returned false (already deleted or invalid key)",
                        );
                    }
                }
            }
            // Drain any WAL records produced by remove_node/remove_edge so
            // replay observes the rollback in the same ordering the live
            // write path would have produced.
            for record in gs.drain_wal() {
                shard_databases.wal_append(shard_id, Bytes::from(record));
            }
            // gs guard drops at scope end — releases graph_store_write
            // before the next layer (vector_store).
        }
    }

    // ------------------------------------------------------------------
    // 3. Vector rollback — tombstone every mutable-HNSW entry appended
    //    during the transaction via `mark_deleted_by_key_hash`, then
    //    transition the TransactionManager into the ABORT state.
    //    This is the core of ACID-08 (HNSW rollback).
    // ------------------------------------------------------------------
    {
        let mut vector_store = shard_databases.vector_store(shard_id);

        // Allocate a rollback LSN from the vector store's own manager so
        // MVCC visibility (`is_visible`) filters the tombstoned rows out
        // of every reader whose `snapshot_lsn >= rollback_lsn`. Per
        // research A4, we use `allocate_lsn()` (monotonic side-effect)
        // rather than `current_lsn()` (read-only) so subsequent writes
        // cannot share the rollback LSN and accidentally become visible.
        let rollback_lsn = vector_store.txn_manager_mut().allocate_lsn();

        for intent in &txn.vector_intents {
            let Some(idx) = vector_store.get_index_mut(&intent.index_name) else {
                tracing::warn!(
                    txn_id,
                    index_name = ?intent.index_name,
                    point_id = intent.point_id,
                    "txn abort: vector index missing at rollback time, skipping intent",
                );
                continue;
            };
            let snap = idx.segments.load();
            let count = snap
                .mutable
                .mark_deleted_by_key_hash(intent.point_id, rollback_lsn);
            if count == 0 {
                tracing::warn!(
                    txn_id,
                    index_name = ?intent.index_name,
                    point_id = intent.point_id,
                    "txn abort: mark_deleted_by_key_hash matched zero entries (rollback may leak)",
                );
            }
        }

        // Transition the TransactionManager into abort state — emits
        // whatever XactAbort-equivalent bookkeeping the manager owns.
        vector_store.txn_manager_mut().abort(txn_id);

        // LOCK-ORDER: drop vector_store before kv_intents
        drop(vector_store);
    }

    // ------------------------------------------------------------------
    // 4. Side-table cleanup — release KV write-intents so other readers
    //    see this transaction's keys again, and discard any deferred
    //    HNSW insertions queued for this txn (prevents phantom neighbors
    //    from showing up post-compaction on a txn that never committed).
    // ------------------------------------------------------------------
    shard_databases
        .kv_intents(shard_id)
        .release_txn(txn_id);
    shard_databases
        .hnsw_queue(shard_id)
        .discard_for_txn(txn_id);
}
