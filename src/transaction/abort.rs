//! Shared cross-store transaction abort helper.
//!
//! Single source of truth for rolling back a `CrossStoreTxn`. Called from
//! explicit `TXN.ABORT`, from a `TXN.COMMIT` refused for rejected ops (#499)
//! and from disconnect cleanup, on both runtimes — always through
//! `server::conn::txn_abort::abort_logged`, which logs what this module
//! returns.
//!
//! # Execution sequence (Phase 161 T-161-01 lock ordering)
//!
//! ```text
//!   KV undo            per (db, key): first undo record only, pre-image to
//!                      an armed snapshot by MOVE, compensating record out
//!   graph rollback     undo ops (LIFO) then create-intent removal (LIFO);
//!                      every step that changed the graph yields its WAL
//!                      record
//!   vector rollback    mark_deleted_by_key_hash_after_lsn per intent,
//!                      txn_manager.abort
//!                      // LOCK-ORDER: drop vector_store before kv_intents
//!   index re-derive    every undone key's vector/text documents rebuilt
//!                      from the RESTORED value
//!   side tables        kv_intents.release_txn, hnsw_queue.discard_for_txn
//! ```
//!
//! The `// LOCK-ORDER: drop vector_store before kv_intents` marker below is
//! an explicit audit gate (Phase 166 Plan 03 acceptance criterion) — it
//! preserves the Phase 161 T-161-01 invariant regardless of guard variable
//! naming.
//!
//! # Durability (moon#1285, moon#1185 option b)
//!
//! A transaction's writes reach the AOF, the WAL and the replication stream
//! as they run. An abort that only rewinds memory is therefore undone by the
//! next restart and never reaches a replica. Every plane now returns the
//! records that make its rollback durable, in an [`AbortLog`]:
//!
//! - **KV** — `DEL` / `RESTORE … REPLACE ABSTTL` (+ `HPEXPIREAT`) per restored
//!   key ([`crate::transaction::kv_compensation`]); the caller replicates
//!   them and appends them to the AOF with the fold epoch read in the same
//!   no-await stretch as the undo.
//! - **Graph** — `GRAPH.REMOVENODE` / `REMOVEEDGE` for created entities,
//!   `GRAPH.SETPROP` / `GRAPH.DELPROP` for restored properties,
//!   `GRAPH.UNDELETENODE` / `UNDELETEEDGE` for deleted ones; the caller
//!   WAL-appends (and, single-shard, replicates) them. Remote legs append on
//!   the owning shard (`ShardMessage::GraphRollback`).
//! - **Vector / text** — derived planes: the indexes are rebuilt from the
//!   restored KV value here, and a restart rebuilds them from the recovered
//!   keyspace (`vector::persistence::recover_v2`: dedup rescan + deletion
//!   probe), a replica from the replicated `DEL` / `RESTORE` (index hooks).
//!   No vector WAL record is needed: recovery never replays one.
//! - **MQ** — `MQ PUBLISH` intents are held until commit; an abort drops them
//!   and there is nothing to log.
//!
//! # Error discipline
//!
//! Every step is fault-tolerant: a missing graph or vector index is logged
//! and skipped, `remove_node` / `remove_edge` returning `false` is an
//! idempotent no-op, and there is no `unwrap()` / `expect()` in the helper.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::transaction::CrossStoreTxn;
use crate::transaction::kv_compensation::{self, CompensatingRecord};

#[cfg(feature = "graph")]
use crate::graph::types::{EdgeKey, NodeKey};

/// What a rollback must log so a restart and a replica land the same state
/// the abort left in memory (moon#1285).
#[derive(Debug, Default)]
pub struct AbortLog {
    /// KV compensating records, `(db, RESP command)`, in apply order — for
    /// the AOF and the replication stream.
    pub kv: Vec<CompensatingRecord>,
    /// Graph compensating WAL records of the LOCAL graph leg (RESP
    /// `GRAPH.*`, `WalRecordType::Command`), in apply order. Always empty
    /// without the `graph` feature.
    pub graph: Vec<Bytes>,
}

/// The graph ops of a multi-shard abort that belong to OTHER shards, grouped
/// by owner. Produced by [`abort_local`], shipped by
/// [`send_remote_graph_rollbacks`].
#[cfg(feature = "graph")]
pub type RemoteGraphLegs = Vec<(
    usize,
    Vec<crate::transaction::GraphUndoOp>,
    Vec<crate::transaction::GraphIntent>,
)>;
/// Without the `graph` feature there is never a remote leg.
#[cfg(not(feature = "graph"))]
pub type RemoteGraphLegs = Vec<std::convert::Infallible>;

/// Roll back every store side-effect of `txn` on THIS shard and return the
/// records that make the rollback durable.
///
/// Consumes the transaction by value — the caller must have already
/// `.take()`'d it off `conn.active_cross_txn`. Idempotent on a re-entry
/// because the transaction is consumed and the per-shard side tables
/// (`kv_intents`, `hnsw_queue`) are keyed by `txn_id` and treat missing
/// entries as no-op.
///
/// # Concurrency
///
/// Runs on the shard event-loop thread, synchronously: nothing here awaits,
/// so the keyspace changes and the caller's fold-stamp read and replication
/// records form one no-await stretch. Each layer's thread-local borrow ends
/// before the next layer is accessed (Phase 161 lock ordering).
pub fn abort_cross_store_txn(txn: CrossStoreTxn) -> AbortLog {
    let txn_id = txn.txn_id;
    let mut log = AbortLog::default();

    // ------------------------------------------------------------------
    // 1. KV undo — the first undo record of each (db, key), which holds the
    //    key's pre-transaction state (moon#1285). Each restore hands the
    //    value it replaces to an armed snapshot by move and yields its
    //    compensating record(s).
    // ------------------------------------------------------------------
    let plan = kv_compensation::first_per_key(txn.kv_undo);
    let mut undone_keys: SmallVec<[(usize, Bytes); 8]> = SmallVec::with_capacity(plan.len());
    log.kv.reserve(plan.len());
    for (db, record) in plan {
        undone_keys.push((db, kv_compensation::record_key(&record).clone()));
        crate::shard::slice::with_shard_db(db, |d| {
            kv_compensation::undo_one(d, db, record, &mut log.kv);
        });
    }

    // ------------------------------------------------------------------
    // 2. Graph rollback — undo ops (2a) then create-intent removal (2b),
    //    both in LIFO order, via the shared `apply_graph_rollback` (also
    //    used by the ShardMessage::GraphRollback handler for the
    //    multi-shard legs). The records it returns are the rollback's WAL.
    // ------------------------------------------------------------------
    #[cfg(feature = "graph")]
    if !txn.graph_undo.is_empty() || !txn.graph_intents.is_empty() {
        let records = crate::shard::slice::with_shard(|s| {
            apply_graph_rollback(
                &mut s.graph_store,
                txn_id,
                &txn.graph_undo,
                &txn.graph_intents,
            )
        });
        log.graph.extend(records.into_iter().map(Bytes::from));
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
                    // Not a leak since moon#1285: step 4 re-derives the key,
                    // which tombstones every live copy in every tier (a
                    // compaction may have moved the entry out of the mutable
                    // segment).
                    tracing::debug!(
                        txn_id,
                        index_name = ?intent.index_name,
                        point_id = intent.point_id,
                        "txn abort: no mutable entry after the snapshot lsn for this intent",
                    );
                }
            }
            // Transition the TransactionManager into abort state.
            s.vector_store.txn_manager_mut().abort(txn_id);
            // LOCK-ORDER: with_shard releases before kv_intents step.
        });
    }

    // ------------------------------------------------------------------
    // 4. Index re-derivation (moon#1285) — the vector/text documents of
    //    every key the KV undo restored are rebuilt from the restored value.
    //    Step 3 only tombstones what the transaction APPENDED; a TXN `HSET`
    //    also tombstoned the key's previous vector (non-transactional
    //    append on the monoio path) and a TXN `DEL` tombstoned it outright,
    //    so without this the aborted-to hash was searchable nowhere. The
    //    rebuild goes through the HSET path, whose MVCC tombstone keeps the
    //    pre-transaction version visible to `FT.SEARCH … AS_OF` earlier
    //    snapshots. It is what a replica runs for the `DEL` / `RESTORE` it
    //    receives and what a restart's dedup rescan converges to.
    // ------------------------------------------------------------------
    if !undone_keys.is_empty() {
        crate::shard::slice::with_shard(|s| {
            for (db, key) in &undone_keys {
                let guard = s.databases.read(*db);
                crate::shard::write_hooks::reindex_key_from_keyspace(
                    &mut s.vector_store,
                    &mut s.text_store,
                    &guard,
                    key,
                    *db,
                );
            }
        });
    }

    // ------------------------------------------------------------------
    // 5. Side-table cleanup — release KV write-intents so other readers
    //    see this transaction's keys again, and discard any deferred
    //    HNSW insertions queued for this txn (prevents phantom neighbors
    //    from showing up post-compaction on a txn that never committed).
    // ------------------------------------------------------------------
    crate::shard::slice::with_shard(|s| {
        s.kv_write_intents.release_txn(txn_id);
        s.deferred_hnsw_inserts.discard_for_txn(txn_id);
    });
    log
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
    use crate::graph::wal;
    use slotmap::Key as _;
    // moon#1285: one WAL record per step that changed the graph, in apply
    // order. The forward writes are already in the WAL (and, single-shard, in
    // the replication stream); without these a restart or a replica replays
    // the aborted graph writes. Appended to `wal_pending` at the end so
    // `drain_wal` marks the store dirty like any live mutation.
    let mut records: Vec<Vec<u8>> = Vec::new();
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
                let present = if *is_node {
                    graph
                        .write_buf
                        .get_node(NodeKey::from(slotmap::KeyData::from_ffi(*entity_id)))
                        .is_some()
                } else {
                    graph
                        .write_buf
                        .get_edge(EdgeKey::from(slotmap::KeyData::from_ffi(*entity_id)))
                        .is_some_and(|e| e.properties.is_some())
                };
                if present {
                    records.push(match old_value {
                        Some(val) => wal::serialize_set_prop(
                            graph_name, *entity_id, *is_node, *prop_key, val,
                        ),
                        None => {
                            wal::serialize_del_prop(graph_name, *entity_id, *is_node, *prop_key)
                        }
                    });
                }
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
                let node_restored = graph.write_buf.undelete_node(nk, *delete_lsn);
                // Un-soft-delete incident edges that were cascade-deleted
                // at the same LSN by remove_node.
                let edges = graph.write_buf.undelete_edges_at_lsn(nk, *delete_lsn);
                if node_restored || !edges.is_empty() {
                    let edge_ids: SmallVec<[u64; 8]> =
                        edges.iter().map(|ek| ek.data().as_ffi()).collect();
                    records.push(wal::serialize_undelete_node(
                        graph_name, *node_id, &edge_ids,
                    ));
                }
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
                if graph.write_buf.undelete_edge(ek) {
                    records.push(wal::serialize_undelete_edge(graph_name, *edge_id));
                    // Task #32: only a REAL flip (edge was actually
                    // deleted) changes query-visible state.
                    graph.touch();
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
                    records.push(wal::serialize_remove_node(
                        &intent.graph_name,
                        intent.entity_id,
                    ));
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
                    records.push(wal::serialize_remove_edge(
                        &intent.graph_name,
                        intent.entity_id,
                    ));
                    graph.touch();
                }
            }
        }
    }

    gs.wal_pending.extend(records);
    gs.drain_wal()
}

/// Multi-shard-aware local half of TXN.ABORT.
///
/// Graphs live on the shard that owns their NAME (`graph_to_shard`), so the
/// graph half of the rollback must run where the entities actually are.
/// This partitions `txn.graph_undo` / `txn.graph_intents` by owning shard,
/// runs the full local abort (KV undo, local graph ops, vector tombstones,
/// index re-derivation, side tables) via [`abort_cross_store_txn`], and
/// returns the remote groups for [`send_remote_graph_rollbacks`].
///
/// Synchronous on purpose: the caller reads the AOF fold epoch and records
/// the replication stream right after it, before its first await.
///
/// At `num_shards <= 1` this is exactly `abort_cross_store_txn`.
#[allow(unused_mut)]
pub fn abort_local(
    shard_id: usize,
    num_shards: usize,
    mut txn: CrossStoreTxn,
) -> (AbortLog, RemoteGraphLegs) {
    // Partition the graph ops by owning shard BEFORE the local abort
    // consumes the transaction. Plain data shuffling — no locks held.
    #[cfg(feature = "graph")]
    let remote: RemoteGraphLegs =
        if num_shards > 1 && (!txn.graph_undo.is_empty() || !txn.graph_intents.is_empty()) {
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
    let remote: RemoteGraphLegs = {
        let _ = (shard_id, num_shards);
        Vec::new()
    };

    let log = abort_cross_store_txn(txn);
    (log, remote)
}

/// Ship the remote graph legs [`abort_local`] split off to their owning
/// shards via `ShardMessage::GraphRollback` and await the acknowledgements.
/// The owner applies the rollback and WAL-appends its records itself.
///
/// Failure handling: a closed reply channel (owner shard gone) is logged and
/// skipped — abort is best-effort per step (see module docs), and the
/// per-shard side tables treat missing entries as no-ops.
pub async fn send_remote_graph_rollbacks(
    shard_id: usize,
    txn_id: u64,
    dispatch_tx: &std::rc::Rc<
        std::cell::RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
    >,
    spsc_notifiers: &[std::sync::Arc<crate::runtime::channel::Notify>],
    remote: RemoteGraphLegs,
) {
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
    let _ = (shard_id, txn_id, dispatch_tx, spsc_notifiers, remote);
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

    /// Split a RESP array of bulk strings (a graph WAL record) into
    /// `(command, args)` the way replay and replica apply do.
    fn parts(record: &[u8]) -> (Vec<u8>, Vec<Vec<u8>>) {
        let mut buf = bytes::BytesMut::from(record);
        let frame = crate::protocol::parse(&mut buf, &crate::protocol::ParseConfig::default())
            .expect("well-formed record")
            .expect("complete record");
        let crate::protocol::Frame::Array(items) = frame else {
            panic!("record is not an array")
        };
        let mut all: Vec<Vec<u8>> = items
            .iter()
            .map(|f| match f {
                crate::protocol::Frame::BulkString(b) => b.to_vec(),
                other => panic!("not a bulk string: {other:?}"),
            })
            .collect();
        let cmd = all.remove(0);
        (cmd, all)
    }

    /// Live nodes `(id, sorted props)` and live edges `(id, src, dst)`.
    type GraphView = (Vec<(u64, Vec<(u16, PropertyValue)>)>, Vec<(u64, u64, u64)>);

    fn view(gs: &GraphStore) -> GraphView {
        let g = gs.get_graph(b"g").expect("graph");
        let mut nodes: Vec<_> = g
            .write_buf
            .iter_nodes()
            .map(|(k, n)| {
                let mut props: Vec<(u16, PropertyValue)> = n.properties.iter().cloned().collect();
                props.sort_by_key(|(k, _)| *k);
                (k.data().as_ffi(), props)
            })
            .collect();
        nodes.sort_by_key(|(id, _)| *id);
        let mut edges: Vec<_> = g
            .write_buf
            .iter_edges()
            .map(|(k, e)| {
                (
                    k.data().as_ffi(),
                    e.src.data().as_ffi(),
                    e.dst.data().as_ffi(),
                )
            })
            .collect();
        edges.sort();
        (nodes, edges)
    }

    /// moon#1285: the rollback's WAL records, replayed after the forward
    /// records (restart replay AND a replica's one-record-at-a-time apply),
    /// land exactly the graph the live abort left — created node removed,
    /// SET restored, SET-added property removed, DELETE (and the edge it
    /// cascaded to) undone.
    #[test]
    fn rollback_records_replay_to_the_live_aborted_to_graph() {
        use crate::graph::replay::GraphReplayCollector;
        use crate::graph::wal;

        let mut gs = GraphStore::new();
        gs.create_graph(Bytes::from("g"), 1_000, 0)
            .expect("create ok");
        let mut forward: Vec<Vec<u8>> = vec![wal::serialize_graph_create(b"g")];
        let graph = gs.get_graph_mut(b"g").expect("graph");
        let a = graph.write_buf.add_node(smallvec![0], id_props(1), None, 1);
        let b = graph.write_buf.add_node(smallvec![0], id_props(2), None, 1);
        let e = graph
            .write_buf
            .add_edge(a, b, 3, 1.0, None, 1)
            .expect("edge");
        for (nk, id) in [(a, 1), (b, 2)] {
            forward.push(wal::serialize_add_node(
                b"g",
                nk.data().as_ffi(),
                &[0],
                &id_props(id),
                None,
            ));
        }
        forward.push(wal::serialize_add_edge(
            b"g",
            e.data().as_ffi(),
            a.data().as_ffi(),
            b.data().as_ffi(),
            3,
            1.0,
            None,
        ));
        let before = view(&gs);

        // The transaction, applied live and WAL-logged as the executor does.
        let graph = gs.get_graph_mut(b"g").expect("graph");
        let c = graph.write_buf.add_node(smallvec![0], id_props(3), None, 5);
        forward.push(wal::serialize_add_node(
            b"g",
            c.data().as_ffi(),
            &[0],
            &id_props(3),
            None,
        ));
        graph
            .write_buf
            .set_node_property(a, 0, PropertyValue::Int(9));
        forward.push(wal::serialize_set_prop(
            b"g",
            a.data().as_ffi(),
            true,
            0,
            &PropertyValue::Int(9),
        ));
        graph
            .write_buf
            .set_node_property(a, 5, PropertyValue::Int(7));
        forward.push(wal::serialize_set_prop(
            b"g",
            a.data().as_ffi(),
            true,
            5,
            &PropertyValue::Int(7),
        ));
        assert!(graph.write_buf.remove_node(b, 10));
        forward.push(wal::serialize_remove_node(b"g", b.data().as_ffi()));

        let undo = vec![
            GraphUndoOp::RestoreProperty {
                graph_name: Bytes::from("g"),
                entity_id: a.data().as_ffi(),
                is_node: true,
                prop_key: 0,
                old_value: Some(PropertyValue::Int(1)),
            },
            GraphUndoOp::RestoreProperty {
                graph_name: Bytes::from("g"),
                entity_id: a.data().as_ffi(),
                is_node: true,
                prop_key: 5,
                old_value: None,
            },
            GraphUndoOp::UndeleteNode {
                graph_name: Bytes::from("g"),
                node_id: b.data().as_ffi(),
                delete_lsn: 10,
            },
        ];
        let intents = [crate::transaction::GraphIntent {
            graph_name: Bytes::from("g"),
            entity_id: c.data().as_ffi(),
            is_node: true,
        }];
        let records = apply_graph_rollback(&mut gs, 1, &undo, &intents);
        let live = view(&gs);
        assert_eq!(
            live, before,
            "the live abort restores the pre-transaction graph"
        );
        assert_eq!(records.len(), 4, "one record per effective rollback step");

        // Restart: every record collected, then replayed in phases.
        let mut collector = GraphReplayCollector::new();
        for record in forward.iter().chain(records.iter()) {
            let (cmd, args) = parts(record);
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            assert!(
                collector.collect_command(&cmd, &refs),
                "{:?}",
                String::from_utf8_lossy(&cmd)
            );
        }
        let mut restarted = GraphStore::new();
        collector.replay_into(&mut restarted);
        assert_eq!(view(&restarted), live, "restart replay");

        // Replica: each record applied on its own, in stream order.
        let mut replica = GraphStore::new();
        for record in forward.iter().chain(records.iter()) {
            let (cmd, args) = parts(record);
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            let mut one = GraphReplayCollector::new();
            assert!(one.collect_command(&cmd, &refs));
            one.replay_into(&mut replica);
        }
        assert_eq!(view(&replica), live, "replica apply");
    }

    /// A later DELETE of a node the abort undeleted must stay deleted after
    /// replay: removes and undeletes replay in WAL order, not by kind.
    #[test]
    fn a_delete_after_an_undelete_replays_in_wal_order() {
        use crate::graph::replay::GraphReplayCollector;
        use crate::graph::wal;
        let id = 4_294_967_297u64; // slotmap ffi id of index 1, version 1
        let log = [
            wal::serialize_graph_create(b"g"),
            wal::serialize_add_node(b"g", id, &[0], &id_props(1), None),
            wal::serialize_remove_node(b"g", id),
            wal::serialize_undelete_node(b"g", id, &[]),
            wal::serialize_remove_node(b"g", id),
        ];
        let mut collector = GraphReplayCollector::new();
        for record in &log {
            let (cmd, args) = parts(record);
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            assert!(collector.collect_command(&cmd, &refs));
        }
        let mut gs = GraphStore::new();
        collector.replay_into(&mut gs);
        assert_eq!(view(&gs).0.len(), 0, "the last word was a DELETE");
    }

    /// Malformed rollback records are refused whole, never half-applied.
    #[test]
    fn malformed_rollback_records_are_refused() {
        use crate::graph::replay::GraphReplayCollector;
        let mut c = GraphReplayCollector::new();
        // count disagrees with the ids that follow
        assert!(!c.collect_command(b"GRAPH.UNDELETENODE", &[b"g", b"1", b"2", b"7"]));
        assert!(!c.collect_command(b"GRAPH.UNDELETENODE", &[b"g", b"1", b"1", b"x"]));
        assert!(!c.collect_command(b"GRAPH.UNDELETENODE", &[b"g", b"1"]));
        assert!(!c.collect_command(b"GRAPH.DELPROP", &[b"g", b"Q", b"1", b"2"]));
        assert!(!c.collect_command(b"GRAPH.DELPROP", &[b"g", b"N", b"1", b"70000"]));
        assert!(!c.collect_command(b"GRAPH.UNDELETEEDGE", &[b"g"]));
        assert_eq!(c.command_count(), 0);
        assert!(c.collect_command(b"graph.undeleteedge", &[b"g", b"5"]));
        assert!(GraphReplayCollector::is_graph_command(b"GRAPH.DELPROP"));
    }
}
