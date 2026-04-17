//! Graph MVCC visibility checks.
//!
//! Mirrors `src/vector/mvcc/visibility.rs` for graph nodes and edges.
//! Visibility rule: `created_lsn <= snapshot AND deleted_lsn > snapshot`
//! with transaction ownership checks for uncommitted writes.

use roaring::RoaringBitmap;

use crate::graph::types::{MutableEdge, MutableNode};

/// Check if a node is visible at the given snapshot.
///
/// Visibility rule:
///   - `created_lsn <= snapshot_lsn` (or own transaction's write)
///   - `deleted_lsn > snapshot_lsn` (or not deleted)
///   - Transaction ownership: uncommitted writes by other txns are invisible
///
/// When `snapshot_lsn == 0`, this is a non-transactional read: all committed
/// or txn_id=0 nodes that are not deleted are visible.
///
/// This function is called per-node during traversal. Zero-allocation,
/// branch-predictable.
#[inline(always)]
pub fn is_node_visible(
    node: &MutableNode,
    snapshot_lsn: u64,
    my_txn_id: u64,
    committed: &RoaringBitmap,
    valid_at: Option<i64>,
) -> bool {
    if !is_entity_visible(
        node.created_lsn,
        node.deleted_lsn,
        node.txn_id,
        snapshot_lsn,
        my_txn_id,
        committed,
    ) {
        return false;
    }
    if let Some(t) = valid_at {
        if t < node.valid_from || t > node.valid_to {
            return false;
        }
    }
    true
}

/// Check if an edge is visible at the given snapshot.
///
/// Same semantics as `is_node_visible` but operates on `MutableEdge`.
#[inline(always)]
pub fn is_edge_visible(
    edge: &MutableEdge,
    snapshot_lsn: u64,
    my_txn_id: u64,
    committed: &RoaringBitmap,
    valid_at: Option<i64>,
) -> bool {
    if !is_entity_visible(
        edge.created_lsn,
        edge.deleted_lsn,
        edge.txn_id,
        snapshot_lsn,
        my_txn_id,
        committed,
    ) {
        return false;
    }
    if let Some(t) = valid_at {
        if t < edge.valid_from || t > edge.valid_to {
            return false;
        }
    }
    true
}

/// Check if a transaction is in the committed bitmap.
///
/// RoaringBitmap only supports u32 keys. txn_ids beyond u32::MAX are treated
/// as uncommitted. This is safe because graph txn_ids are allocated from a
/// separate counter that stays within u32 range. If explicit transactions are
/// added in the future, consider migrating to `RoaringTreemap` (u64 support).
#[inline(always)]
fn is_txn_committed(txn_id: u64, committed: &RoaringBitmap) -> bool {
    if txn_id > u32::MAX as u64 {
        return false;
    }
    committed.contains(txn_id as u32)
}

/// Shared visibility logic for both nodes and edges.
///
/// `txn_id` is the transaction that created the entity (0 = no transaction / pre-MVCC).
/// `deleted_lsn` is u64::MAX if alive.
#[inline(always)]
fn is_entity_visible(
    created_lsn: u64,
    deleted_lsn: u64,
    txn_id: u64,
    snapshot_lsn: u64,
    my_txn_id: u64,
    committed: &RoaringBitmap,
) -> bool {
    // Non-transactional read (snapshot_lsn == 0): skip MVCC, just check ownership + delete
    if snapshot_lsn == 0 {
        if txn_id != 0 && !is_txn_committed(txn_id, committed) {
            return false;
        }
        return deleted_lsn == u64::MAX;
    }

    // Insert visibility: must be at or before our snapshot
    if created_lsn > snapshot_lsn {
        // Exception: our own transaction's writes are always visible
        if txn_id != my_txn_id {
            return false;
        }
    }

    // Transaction ownership check
    if txn_id != 0 && txn_id != my_txn_id {
        if !is_txn_committed(txn_id, committed) {
            return false;
        }
    }

    // Delete visibility: if deleted, only visible if deletion is after our snapshot
    // For graph entities, deleted_lsn == u64::MAX means alive.
    if deleted_lsn != u64::MAX && deleted_lsn <= snapshot_lsn {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use smallvec::{SmallVec, smallvec};

    use crate::graph::types::{MutableEdge, MutableNode, NodeKey, PropertyMap};

    fn empty_committed() -> RoaringBitmap {
        RoaringBitmap::new()
    }

    fn committed_with(ids: &[u32]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for &id in ids {
            bm.insert(id);
        }
        bm
    }

    fn make_node(created_lsn: u64, deleted_lsn: u64, txn_id: u64) -> MutableNode {
        MutableNode {
            labels: smallvec![0],
            outgoing: SmallVec::new(),
            incoming: SmallVec::new(),
            properties: SmallVec::new(),
            embedding: None,
            created_lsn,
            deleted_lsn,
            txn_id,
            valid_from: 0,
            valid_to: i64::MAX,
        }
    }

    fn make_edge(created_lsn: u64, deleted_lsn: u64, txn_id: u64) -> MutableEdge {
        // Use default NodeKeys -- we only care about LSN/txn visibility here.
        let default_key = slotmap::KeyData::from_ffi(0).into();
        MutableEdge {
            src: default_key,
            dst: default_key,
            edge_type: 0,
            weight: 1.0,
            properties: None,
            created_lsn,
            deleted_lsn,
            txn_id,
            valid_from: 0,
            valid_to: i64::MAX,
        }
    }

    // --- Node visibility tests ---

    #[test]
    fn test_node_committed_visible() {
        let node = make_node(5, u64::MAX, 0);
        let committed = empty_committed();
        assert!(is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_created_after_snapshot_invisible() {
        let node = make_node(15, u64::MAX, 0);
        let committed = empty_committed();
        assert!(!is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_committed_txn_visible() {
        let node = make_node(5, u64::MAX, 2);
        let committed = committed_with(&[2]);
        assert!(is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_uncommitted_other_txn_invisible() {
        let node = make_node(5, u64::MAX, 3);
        let committed = empty_committed();
        assert!(!is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_own_writes_visible() {
        let node = make_node(5, u64::MAX, 1);
        let committed = empty_committed();
        assert!(is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_own_writes_visible_even_after_snapshot() {
        let node = make_node(15, u64::MAX, 1);
        let committed = empty_committed();
        assert!(is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_deleted_before_snapshot_invisible() {
        let node = make_node(5, 8, 0);
        let committed = empty_committed();
        assert!(!is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_deleted_after_snapshot_visible() {
        let node = make_node(5, 15, 0);
        let committed = empty_committed();
        assert!(is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_non_transactional_read_committed() {
        let node = make_node(5, u64::MAX, 2);
        let committed = committed_with(&[2]);
        assert!(is_node_visible(&node, 0, 0, &committed, None));
    }

    #[test]
    fn test_node_non_transactional_read_uncommitted_invisible() {
        let node = make_node(5, u64::MAX, 3);
        let committed = empty_committed();
        assert!(!is_node_visible(&node, 0, 0, &committed, None));
    }

    #[test]
    fn test_node_non_transactional_deleted_invisible() {
        let node = make_node(5, 10, 0);
        let committed = empty_committed();
        assert!(!is_node_visible(&node, 0, 0, &committed, None));
    }

    // --- Edge visibility tests ---

    #[test]
    fn test_edge_committed_visible() {
        let edge = make_edge(5, u64::MAX, 0);
        let committed = empty_committed();
        assert!(is_edge_visible(&edge, 10, 1, &committed, None));
    }

    #[test]
    fn test_edge_created_after_snapshot_invisible() {
        let edge = make_edge(15, u64::MAX, 0);
        let committed = empty_committed();
        assert!(!is_edge_visible(&edge, 10, 1, &committed, None));
    }

    #[test]
    fn test_edge_own_writes_visible() {
        let edge = make_edge(5, u64::MAX, 1);
        let committed = empty_committed();
        assert!(is_edge_visible(&edge, 10, 1, &committed, None));
    }

    #[test]
    fn test_edge_deleted_before_snapshot_invisible() {
        let edge = make_edge(5, 8, 0);
        let committed = empty_committed();
        assert!(!is_edge_visible(&edge, 10, 1, &committed, None));
    }

    #[test]
    fn test_edge_boundary_created_at_snapshot() {
        let edge = make_edge(10, u64::MAX, 0);
        let committed = empty_committed();
        assert!(is_edge_visible(&edge, 10, 1, &committed, None));
    }

    #[test]
    fn test_edge_boundary_deleted_at_snapshot() {
        let edge = make_edge(5, 10, 0);
        let committed = empty_committed();
        assert!(!is_edge_visible(&edge, 10, 1, &committed, None));
    }

    // --- Bi-temporal visibility tests ---

    #[test]
    fn test_node_valid_at_within_window() {
        let mut node = make_node(5, u64::MAX, 0);
        node.valid_from = 1000;
        node.valid_to = 2000;
        let committed = empty_committed();
        assert!(is_node_visible(&node, 10, 1, &committed, Some(1500)));
    }

    #[test]
    fn test_node_valid_at_before_window() {
        let mut node = make_node(5, u64::MAX, 0);
        node.valid_from = 1000;
        node.valid_to = 2000;
        let committed = empty_committed();
        assert!(!is_node_visible(&node, 10, 1, &committed, Some(500)));
    }

    #[test]
    fn test_node_valid_at_after_window() {
        let mut node = make_node(5, u64::MAX, 0);
        node.valid_from = 1000;
        node.valid_to = 2000;
        let committed = empty_committed();
        assert!(!is_node_visible(&node, 10, 1, &committed, Some(3000)));
    }

    #[test]
    fn test_node_valid_at_none_ignores_window() {
        let mut node = make_node(5, u64::MAX, 0);
        node.valid_from = 1000;
        node.valid_to = 2000;
        let committed = empty_committed();
        assert!(is_node_visible(&node, 10, 1, &committed, None));
    }

    #[test]
    fn test_node_valid_at_boundary_inclusive() {
        let mut node = make_node(5, u64::MAX, 0);
        node.valid_from = 1000;
        node.valid_to = 2000;
        let committed = empty_committed();
        assert!(is_node_visible(&node, 10, 1, &committed, Some(1000)));
        assert!(is_node_visible(&node, 10, 1, &committed, Some(2000)));
    }

    #[test]
    fn test_edge_valid_at_filtering() {
        let mut edge = make_edge(5, u64::MAX, 0);
        edge.valid_from = 500;
        edge.valid_to = 1500;
        let committed = empty_committed();
        assert!(is_edge_visible(&edge, 10, 1, &committed, Some(1000)));
        assert!(!is_edge_visible(&edge, 10, 1, &committed, Some(2000)));
        assert!(is_edge_visible(&edge, 10, 1, &committed, None));
    }
}
