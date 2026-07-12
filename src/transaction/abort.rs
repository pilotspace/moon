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
use crate::graph::types::{EdgeKey, NodeKey};
#[cfg(feature = "graph")]
use bytes::Bytes;

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
// `shard_databases` and `shard_id` are consumed only under `#[cfg(feature = "graph")]`
// (WAL record drain). Without the graph feature they are structurally unused;
// suppress the lint rather than removing a semantically load-bearing parameter.
#[allow(clippy::needless_pass_by_value)]
#[cfg_attr(not(feature = "graph"), allow(unused_variables))]
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
        crate::shard::slice::with_shard_db(selected_db, |db| {
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
        });
        // with_shard_db releases the borrow at the closure boundary — before next layer.
    }

    // ------------------------------------------------------------------
    // 2. Graph rollback — undo ops (2a) then create-intent removal (2b),
    //    both in LIFO order, applied via the shared `apply_graph_rollback`
    //    helper (also used by the ShardMessage::GraphRollback handler for
    //    the multi-shard TXN.ABORT legs). Slice-aware: once ShardSlice is
    //    initialized the graph store is thread-local and the lock-based
    //    accessor would hit a different store.
    // ------------------------------------------------------------------
    #[cfg(feature = "graph")]
    {
        if !txn.graph_undo.is_empty() || !txn.graph_intents.is_empty() {
            // Unconditional slice path: ShardSlice is always initialized.
            let wal_records = crate::shard::slice::with_shard(|s| {
                apply_graph_rollback(
                    &mut s.graph_store,
                    txn_id,
                    &txn.graph_undo,
                    &txn.graph_intents,
                )
            });
            // Drain any WAL records produced by remove_node/remove_edge so
            // replay observes the rollback in the same ordering the live
            // write path would have produced.
            for record in wal_records {
                shard_databases.wal_append(
                    shard_id,
                    crate::persistence::wal_v3::record::WalRecordType::Command,
                    Bytes::from(record),
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // 3. Vector rollback — tombstone every mutable-HNSW entry appended
    //    during the transaction. Uses with_shard so the thread-local
    //    VectorStore is accessed without a lock.
    // ------------------------------------------------------------------
    {
        let txn_snapshot_lsn = txn.snapshot_lsn;
        crate::shard::slice::with_shard(|s| {
            for intent in &txn.vector_intents {
                let Some(idx) = s.vector_store.get_index_mut(&intent.index_name) else {
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
                    .mark_deleted_by_key_hash_after_lsn(intent.point_id, txn_snapshot_lsn);
                if count == 0 {
                    tracing::warn!(
                        txn_id,
                        index_name = ?intent.index_name,
                        point_id = intent.point_id,
                        "txn abort: mark_deleted_by_key_hash_after_lsn matched zero entries (rollback may leak)",
                    );
                }
            }
            // Transition the TransactionManager into abort state.
            s.vector_store.txn_manager_mut().abort(txn_id);
            // LOCK-ORDER: with_shard releases before kv_intents step.
        });
    }

    // ------------------------------------------------------------------
    // 4. Side-table cleanup — release KV write-intents so other readers
    //    see this transaction's keys again, and discard any deferred
    //    HNSW insertions queued for this txn (prevents phantom neighbors
    //    from showing up post-compaction on a txn that never committed).
    // ------------------------------------------------------------------
    crate::shard::slice::with_shard(|s| {
        s.kv_write_intents.release_txn(txn_id);
        s.deferred_hnsw_inserts.discard_for_txn(txn_id);
    });
}

/// Apply the graph half of a TXN.ABORT to one `GraphStore`.
///
/// Section 2a (undo ops, LIFO) then section 2b (create-intent removal, LIFO
/// so edges go before their endpoint nodes). Returns the drained graph WAL
/// records — the CALLER appends them on its own shard, which keeps the
/// helper usable from both `abort_cross_store_txn` (connection-local leg)
/// and the `ShardMessage::GraphRollback` handler (multi-shard legs).
#[cfg(feature = "graph")]
pub fn apply_graph_rollback(
    gs: &mut crate::graph::store::GraphStore,
    txn_id: u64,
    graph_undo: &[crate::transaction::GraphUndoOp],
    graph_intents: &[crate::transaction::GraphIntent],
) -> Vec<Vec<u8>> {
    // 2a. Phase 174 FIX-01: reverse SET/DELETE/MERGE mutations in LIFO order
    //     BEFORE removing created entities (2b). This ensures property
    //     restores on existing nodes happen before any newly-created nodes
    //     are removed.
    for undo_op in graph_undo.iter().rev() {
        match undo_op {
            crate::transaction::GraphUndoOp::RestoreProperty {
                graph_name,
                entity_id,
                is_node,
                prop_key,
                old_value,
            } => {
                let Some(graph) = gs.get_graph_mut(graph_name) else {
                    tracing::warn!(
                        txn_id,
                        graph_name = ?graph_name,
                        "txn abort: graph missing for RestoreProperty undo, skipping",
                    );
                    continue;
                };
                if *is_node {
                    let nk = NodeKey::from(slotmap::KeyData::from_ffi(*entity_id));
                    // `set_node_property`/`remove_node_property` are the
                    // single source of truth for node-property mutation —
                    // they keep the mutable-tier property index (Task #31)
                    // in sync and are no-ops (return `None`) if the node is
                    // missing, matching the old `get_node_mut`-guarded
                    // behavior.
                    match old_value {
                        Some(val) => {
                            graph
                                .write_buf
                                .set_node_property(nk, *prop_key, val.clone());
                        }
                        None => {
                            // Property did not exist before SET — remove it.
                            graph.write_buf.remove_node_property(nk, *prop_key);
                        }
                    }
                } else {
                    let ek = EdgeKey::from(slotmap::KeyData::from_ffi(*entity_id));
                    if let Some(edge) = graph.write_buf.get_edge_mut(ek) {
                        if let Some(ref mut props) = edge.properties {
                            match old_value {
                                Some(val) => {
                                    let mut found = false;
                                    for entry in props.iter_mut() {
                                        if entry.0 == *prop_key {
                                            entry.1 = val.clone();
                                            found = true;
                                            break;
                                        }
                                    }
                                    if !found {
                                        props.push((*prop_key, val.clone()));
                                    }
                                }
                                None => {
                                    props.retain(|(k, _)| *k != *prop_key);
                                }
                            }
                        }
                    }
                }
                // Task #32: RestoreProperty always changes query-visible
                // state (either a value flip or a property removal) --
                // invalidate this graph's cached query results.
                graph.touch();
            }
            crate::transaction::GraphUndoOp::UndeleteNode {
                graph_name,
                node_id,
                delete_lsn,
            } => {
                let Some(graph) = gs.get_graph_mut(graph_name) else {
                    tracing::warn!(
                        txn_id,
                        graph_name = ?graph_name,
                        "txn abort: graph missing for UndeleteNode undo, skipping",
                    );
                    continue;
                };
                let nk = NodeKey::from(slotmap::KeyData::from_ffi(*node_id));
                // `undelete_node` centralizes the flip + live-count bump +
                // property re-indexing (Task #31 design doc risk #6): a
                // naive `deleted_lsn = u64::MAX` flip without re-indexing
                // would leave the node live but permanently invisible to
                // `MATCH {prop: val}` index probes.
                graph.write_buf.undelete_node(nk, *delete_lsn);
                // Un-soft-delete incident edges that were cascade-deleted
                // at the same LSN by remove_node.
                graph.write_buf.undelete_edges_at_lsn(nk, *delete_lsn);
                // Task #32: undelete always changes query-visible state --
                // invalidate this graph's cached query results.
                graph.touch();
            }
            crate::transaction::GraphUndoOp::UndeleteEdge {
                graph_name,
                edge_id,
            } => {
                let Some(graph) = gs.get_graph_mut(graph_name) else {
                    tracing::warn!(
                        txn_id,
                        graph_name = ?graph_name,
                        "txn abort: graph missing for UndeleteEdge undo, skipping",
                    );
                    continue;
                };
                let ek = EdgeKey::from(slotmap::KeyData::from_ffi(*edge_id));
                if let Some(edge) = graph.write_buf.get_edge_mut(ek) {
                    if edge.deleted_lsn != u64::MAX {
                        edge.deleted_lsn = u64::MAX;
                        graph.write_buf.inc_live_edge_count();
                        // Task #32: only a REAL flip (edge was actually
                        // deleted) changes query-visible state.
                        graph.touch();
                    }
                }
            }
        }
    }

    // 2b. Create-intent removal — iterate in REVERSE (LIFO) so edges are
    //     removed before their endpoint nodes.
    if !graph_intents.is_empty() {
        // Rollback LSN: allocate from GraphStore's own LSN stream so
        // replay order stays monotonic with forward writes (Phase 158
        // CSR v2 migration tests verify this ordering).
        let rollback_lsn = gs.allocate_lsn();
        for intent in graph_intents.iter().rev() {
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
                } else {
                    // Task #32: a create-intent rollback removes an entity
                    // that a cached query may have already returned rows
                    // for -- invalidate.
                    graph.touch();
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
                } else {
                    graph.touch();
                }
            }
        }
    }

    gs.drain_wal()
}

/// Multi-shard-aware TXN.ABORT.
///
/// Graphs live on the shard that owns their NAME (`graph_to_shard`), so the
/// graph half of the rollback must run where the entities actually are.
/// This wrapper partitions `txn.graph_undo` / `txn.graph_intents` by owning
/// shard, runs the full local abort (KV undo, local graph ops, vector
/// tombstones, side tables) via `abort_cross_store_txn`, then ships each
/// remote group to its owner via `ShardMessage::GraphRollback` and awaits
/// the acknowledgements.
///
/// Failure handling: a closed reply channel (owner shard gone) is logged and
/// skipped — abort is already best-effort per-step (see module docs), and
/// the per-shard side tables treat missing entries as no-ops.
///
/// At `num_shards <= 1` this is exactly `abort_cross_store_txn`.
#[allow(clippy::too_many_arguments)]
pub async fn abort_cross_store_txn_routed(
    shard_databases: &ShardDatabases,
    shard_id: usize,
    selected_db: usize,
    num_shards: usize,
    dispatch_tx: &std::rc::Rc<
        std::cell::RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
    >,
    spsc_notifiers: &[std::sync::Arc<crate::runtime::channel::Notify>],
    #[allow(unused_mut)] mut txn: CrossStoreTxn,
) {
    // Partition the graph ops by owning shard BEFORE the local abort
    // consumes the transaction. Plain data shuffling — no locks held.
    #[cfg(feature = "graph")]
    let remote: Vec<(
        usize,
        Vec<crate::transaction::GraphUndoOp>,
        Vec<crate::transaction::GraphIntent>,
    )> = if num_shards > 1 && (!txn.graph_undo.is_empty() || !txn.graph_intents.is_empty()) {
        use crate::shard::dispatch::graph_to_shard;
        let mut by_owner: std::collections::BTreeMap<
            usize,
            (
                Vec<crate::transaction::GraphUndoOp>,
                Vec<crate::transaction::GraphIntent>,
            ),
        > = std::collections::BTreeMap::new();
        let mut local_undo = Vec::with_capacity(txn.graph_undo.len());
        for op in txn.graph_undo.drain(..) {
            let owner = {
                let name = match &op {
                    crate::transaction::GraphUndoOp::RestoreProperty { graph_name, .. }
                    | crate::transaction::GraphUndoOp::UndeleteNode { graph_name, .. }
                    | crate::transaction::GraphUndoOp::UndeleteEdge { graph_name, .. } => {
                        graph_name
                    }
                };
                graph_to_shard(name, num_shards)
            };
            if owner == shard_id {
                local_undo.push(op);
            } else {
                by_owner.entry(owner).or_default().0.push(op);
            }
        }
        txn.graph_undo = local_undo;
        let mut local_intents: smallvec::SmallVec<[crate::transaction::GraphIntent; 8]> =
            smallvec::SmallVec::new();
        for intent in txn.graph_intents.drain(..) {
            let owner = graph_to_shard(&intent.graph_name, num_shards);
            if owner == shard_id {
                local_intents.push(intent);
            } else {
                by_owner.entry(owner).or_default().1.push(intent);
            }
        }
        txn.graph_intents = local_intents;
        by_owner
            .into_iter()
            .map(|(owner, (undo, intents))| (owner, undo, intents))
            .collect()
    } else {
        Vec::new()
    };
    #[cfg(not(feature = "graph"))]
    let _ = num_shards;

    let txn_id = txn.txn_id;
    abort_cross_store_txn(shard_databases, shard_id, selected_db, txn);

    #[cfg(feature = "graph")]
    for (owner, graph_undo, graph_intents) in remote {
        let (reply_tx, reply_rx) = crate::runtime::channel::oneshot();
        let msg = crate::shard::dispatch::ShardMessage::GraphRollback(Box::new(
            crate::shard::dispatch::GraphRollbackPayload {
                txn_id,
                graph_undo,
                graph_intents,
                reply_tx,
            },
        ));
        let outcome =
            crate::shard::coordinator::spsc_send(dispatch_tx, shard_id, owner, msg, spsc_notifiers)
                .await;
        if outcome != crate::shard::dispatch::PushOutcome::Pushed {
            // The rollback was never delivered: the owner shard keeps the
            // txn's graph intents un-undone until its own reconcile/GC path
            // catches them. Loud, not warn — this is leaked remote state.
            tracing::error!(
                txn_id,
                owner,
                "txn abort: remote graph rollback DROPPED under dispatch \
                 backpressure — remote graph intents not undone"
            );
            continue;
        }
        if crate::shard::coordinator::recv_reply_bounded(reply_rx)
            .await
            .is_err()
        {
            tracing::warn!(
                txn_id,
                owner,
                "txn abort: remote graph rollback reply channel closed"
            );
        }
    }
    #[cfg(not(feature = "graph"))]
    let _ = (txn_id, dispatch_tx, spsc_notifiers);
}

#[cfg(all(test, feature = "graph"))]
mod tests {
    //! TXN.ABORT graph-rollback regression tests, targeting the mutable-tier
    //! property index (Task #31, `tmp/DESIGN-MUTABLE-PROP-INDEX.md` risk
    //! #6). `apply_graph_rollback` is a free function whose only dependency
    //! is a `GraphStore` — no `ShardDatabases`/`CrossStoreTxn` plumbing
    //! needed, so these tests hand-construct `GraphUndoOp`/`GraphIntent`
    //! directly and assert on the mutable-tier property index state via
    //! `MemGraph::prop_index_keys_eq` after rollback.

    use super::*;
    use crate::graph::store::GraphStore;
    use crate::graph::types::PropertyValue;
    use crate::transaction::GraphUndoOp;
    use slotmap::Key;
    use smallvec::smallvec;

    fn id_props(id: i64) -> crate::graph::types::PropertyMap {
        smallvec![(0u16, PropertyValue::Int(id))]
    }

    /// TXN.ABORT undo of `SET n.id = new` must restore BOTH the raw
    /// property value AND the mutable-tier index: the original value must
    /// be index-reachable again, and the SET value must not be.
    #[test]
    fn test_rollback_restore_property_reindexes() {
        let mut gs = GraphStore::new();
        gs.create_graph(Bytes::from("g"), 1_000, 0)
            .expect("create ok");
        let graph = gs.get_graph_mut(b"g").expect("graph");
        let nk = graph.write_buf.add_node(smallvec![0], id_props(1), None, 1);

        // Simulate the live SET n.id = 2 that would have run before ABORT.
        graph
            .write_buf
            .set_node_property(nk, 0, PropertyValue::Int(2));
        assert!(
            graph
                .write_buf
                .prop_index_keys_eq(0, &PropertyValue::Int(1))
                .is_empty()
        );
        assert_eq!(
            graph
                .write_buf
                .prop_index_keys_eq(0, &PropertyValue::Int(2)),
            &[nk]
        );

        let undo = vec![GraphUndoOp::RestoreProperty {
            graph_name: Bytes::from("g"),
            entity_id: nk.data().as_ffi(),
            is_node: true,
            prop_key: 0,
            old_value: Some(PropertyValue::Int(1)),
        }];
        apply_graph_rollback(&mut gs, 1, &undo, &[]);

        let graph = gs.get_graph(b"g").expect("graph");
        assert_eq!(
            graph
                .write_buf
                .prop_index_keys_eq(0, &PropertyValue::Int(1)),
            &[nk],
            "rollback must restore index reachability under the ORIGINAL value"
        );
        assert!(
            graph
                .write_buf
                .prop_index_keys_eq(0, &PropertyValue::Int(2))
                .is_empty(),
            "the SET value must no longer be index-reachable after rollback"
        );
    }

    /// TXN.ABORT undo of `SET n.newprop = v` (property did not exist
    /// before) must remove it from BOTH the raw property vec AND the index.
    #[test]
    fn test_rollback_restore_property_none_removes_from_index() {
        let mut gs = GraphStore::new();
        gs.create_graph(Bytes::from("g"), 1_000, 0)
            .expect("create ok");
        let graph = gs.get_graph_mut(b"g").expect("graph");
        let nk = graph.write_buf.add_node(smallvec![0], smallvec![], None, 1);
        graph
            .write_buf
            .set_node_property(nk, 7, PropertyValue::Int(99));
        assert_eq!(
            graph
                .write_buf
                .prop_index_keys_eq(7, &PropertyValue::Int(99)),
            &[nk]
        );

        let undo = vec![GraphUndoOp::RestoreProperty {
            graph_name: Bytes::from("g"),
            entity_id: nk.data().as_ffi(),
            is_node: true,
            prop_key: 7,
            old_value: None,
        }];
        apply_graph_rollback(&mut gs, 1, &undo, &[]);

        let graph = gs.get_graph(b"g").expect("graph");
        assert!(
            graph
                .write_buf
                .prop_index_keys_eq(7, &PropertyValue::Int(99))
                .is_empty(),
            "a property that didn't exist before SET must be unindexed on rollback"
        );
    }

    /// Regression guard for the design doc's risk #6 (§5): TXN.ABORT's
    /// `UndeleteNode` undo must re-index the node's properties, not just
    /// flip `deleted_lsn`. A naive port that skips re-indexing leaves the
    /// node live but permanently invisible to `MATCH {prop: val}` index
    /// probes — this is exactly the regression `MemGraph::undelete_node`
    /// (memgraph.rs) exists to close.
    #[test]
    fn test_rollback_undelete_node_reindexes_properties() {
        let mut gs = GraphStore::new();
        gs.create_graph(Bytes::from("g"), 1_000, 0)
            .expect("create ok");
        let graph = gs.get_graph_mut(b"g").expect("graph");
        let nk = graph.write_buf.add_node(smallvec![0], id_props(5), None, 1);

        // Simulate the live DELETE that would have run before ABORT.
        let delete_lsn = 2;
        assert!(graph.write_buf.remove_node(nk, delete_lsn));
        assert!(
            graph
                .write_buf
                .prop_index_keys_eq(0, &PropertyValue::Int(5))
                .is_empty(),
            "soft-deleted node must not be index-reachable"
        );

        let undo = vec![GraphUndoOp::UndeleteNode {
            graph_name: Bytes::from("g"),
            node_id: nk.data().as_ffi(),
            delete_lsn,
        }];
        apply_graph_rollback(&mut gs, 1, &undo, &[]);

        let graph = gs.get_graph(b"g").expect("graph");
        assert_eq!(
            graph
                .write_buf
                .get_node(nk)
                .expect("node still resident")
                .deleted_lsn,
            u64::MAX,
            "rollback must un-soft-delete the node"
        );
        assert_eq!(
            graph
                .write_buf
                .prop_index_keys_eq(0, &PropertyValue::Int(5)),
            &[nk],
            "rollback of a DELETE must re-index the node's properties, \
             not just flip deleted_lsn"
        );
    }
}
