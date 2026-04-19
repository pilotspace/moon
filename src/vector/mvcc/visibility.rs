use roaring::RoaringTreemap;

/// MVCC visibility check for a single entry during search.
///
/// Visibility rule (from architecture spec):
///   visible = insert_lsn <= snapshot
///             AND (txn_id == 0 OR txn_id == my_txn_id OR committed.contains(txn_id))
///             AND (delete_lsn == 0 OR delete_lsn > snapshot)
///
/// When snapshot_lsn == 0, this is a non-transactional read:
/// all entries with txn_id == 0 or committed txn_id are visible (if not deleted).
///
/// This function is called per-candidate during brute-force scan and HNSW result
/// collection. It MUST be zero-allocation and branch-predictable.
///
/// # Arguments
/// - `insert_lsn`: entry's insert LSN
/// - `delete_lsn`: entry's delete LSN (0 = not deleted)
/// - `txn_id`: entry's owning transaction ID (0 = no transaction / pre-MVCC)
/// - `snapshot_lsn`: the querying transaction's snapshot (0 = non-transactional)
/// - `my_txn_id`: the querying transaction's ID (0 = non-transactional)
/// - `committed`: treemap of committed transaction IDs
#[inline(always)]
pub fn is_visible(
    insert_lsn: u64,
    delete_lsn: u64,
    txn_id: u64,
    snapshot_lsn: u64,
    my_txn_id: u64,
    committed: &RoaringTreemap,
) -> bool {
    // Non-transactional read (snapshot_lsn == 0): skip MVCC, just check ownership + delete
    if snapshot_lsn == 0 {
        if txn_id != 0 && !committed.contains(txn_id) {
            return false; // uncommitted by some txn
        }
        return delete_lsn == 0;
    }

    // Insert visibility: must be at or before our snapshot.
    // Exception: our own transaction's writes are always visible. The exception
    // only applies when the reader IS in a transaction (my_txn_id != 0) AND the
    // entry was written by a transaction (txn_id != 0); otherwise both being 0
    // (non-transactional reader looking at non-transactional write) would spuriously
    // match and leak post-snapshot writes through — precisely the bug Phase 165
    // guards against for FT.SEARCH AS_OF.
    if insert_lsn > snapshot_lsn
        && !(my_txn_id != 0 && txn_id == my_txn_id)
    {
        return false;
    }

    // Transaction ownership check
    if txn_id != 0 && txn_id != my_txn_id {
        // Entry belongs to another transaction -- must be committed to be visible
        if !committed.contains(txn_id) {
            return false;
        }
    }

    // Delete visibility: if deleted, only visible if deletion is after our snapshot
    if delete_lsn != 0 && delete_lsn <= snapshot_lsn {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_committed() -> RoaringTreemap {
        RoaringTreemap::new()
    }

    fn committed_with(ids: &[u64]) -> RoaringTreemap {
        let mut tm = RoaringTreemap::new();
        for &id in ids {
            tm.insert(id);
        }
        tm
    }

    #[test]
    fn test_committed_no_txn_not_deleted_visible() {
        // insert_lsn=5, delete_lsn=0, txn_id=0, snapshot=10, my_txn=1
        let committed = empty_committed();
        assert!(is_visible(5, 0, 0, 10, 1, &committed));
    }

    #[test]
    fn test_insert_after_snapshot_not_visible() {
        // insert_lsn=15 > snapshot=10
        let committed = empty_committed();
        assert!(!is_visible(15, 0, 0, 10, 1, &committed));
    }

    #[test]
    fn test_committed_txn_not_deleted_visible() {
        // insert_lsn=5, txn_id=2 which is committed, snapshot=10
        let committed = committed_with(&[2]);
        assert!(is_visible(5, 0, 2, 10, 1, &committed));
    }

    #[test]
    fn test_committed_txn_deleted_before_snapshot_not_visible() {
        // insert_lsn=5, txn_id=2 committed, delete_lsn=8 <= snapshot=10
        let committed = committed_with(&[2]);
        assert!(!is_visible(5, 8, 2, 10, 1, &committed));
    }

    #[test]
    fn test_committed_txn_deleted_after_snapshot_visible() {
        // insert_lsn=5, txn_id=2 committed, delete_lsn=15 > snapshot=10
        let committed = committed_with(&[2]);
        assert!(is_visible(5, 15, 2, 10, 1, &committed));
    }

    #[test]
    fn test_active_other_txn_not_visible() {
        // insert_lsn=5, txn_id=3 not committed (active by other), snapshot=10, my_txn=1
        let committed = empty_committed();
        assert!(!is_visible(5, 0, 3, 10, 1, &committed));
    }

    #[test]
    fn test_read_your_own_writes_visible() {
        // insert_lsn=5, txn_id=1 == my_txn_id=1, snapshot=10
        let committed = empty_committed();
        assert!(is_visible(5, 0, 1, 10, 1, &committed));
    }

    #[test]
    fn test_read_your_own_writes_even_after_snapshot() {
        // insert_lsn=15 > snapshot=10, but txn_id=1 == my_txn_id=1
        let committed = empty_committed();
        assert!(is_visible(15, 0, 1, 10, 1, &committed));
    }

    #[test]
    fn test_aborted_txn_not_visible() {
        // txn_id=5 not active, not committed (aborted)
        let committed = empty_committed();
        assert!(!is_visible(5, 0, 5, 10, 1, &committed));
    }

    #[test]
    fn test_non_transactional_read_sees_committed() {
        // snapshot_lsn=0 means non-transactional
        let committed = committed_with(&[2]);
        // txn_id=0 (no txn), not deleted -> visible
        assert!(is_visible(5, 0, 0, 0, 0, &committed));
        // txn_id=2 committed, not deleted -> visible
        assert!(is_visible(5, 0, 2, 0, 0, &committed));
        // txn_id=3 NOT committed -> not visible
        assert!(!is_visible(5, 0, 3, 0, 0, &committed));
    }

    #[test]
    fn test_non_transactional_read_deleted_not_visible() {
        // snapshot_lsn=0, delete_lsn != 0
        let committed = empty_committed();
        assert!(!is_visible(5, 10, 0, 0, 0, &committed));
    }

    #[test]
    fn test_insert_at_exact_snapshot_visible() {
        // insert_lsn == snapshot_lsn (boundary condition)
        let committed = empty_committed();
        assert!(is_visible(10, 0, 0, 10, 1, &committed));
    }

    #[test]
    fn test_delete_at_exact_snapshot_not_visible() {
        // delete_lsn == snapshot_lsn (boundary: delete_lsn <= snapshot means not visible)
        let committed = empty_committed();
        assert!(!is_visible(5, 10, 0, 10, 1, &committed));
    }

    #[test]
    fn test_non_txn_reader_filters_post_snapshot_non_txn_insert() {
        // Phase 165 regression guard (FT.SEARCH AS_OF on non-transactional writes).
        // A non-TXN reader (my_txn_id=0) with an explicit snapshot (AS_OF bound
        // to an LSN resolved > 0) inspecting a non-TXN insert (txn_id=0) with
        // insert_lsn > snapshot MUST be filtered. Before the Phase 165 fix the
        // `txn_id == my_txn_id` self-visibility exception matched when both were
        // 0 and leaked post-snapshot writes through.
        let committed = empty_committed();
        assert!(
            !is_visible(2, 0, 0, 1, 0, &committed),
            "post-snapshot non-txn insert must be filtered for non-txn AS_OF reader"
        );
        // Boundary: insert_lsn == snapshot_lsn stays visible.
        assert!(is_visible(1, 0, 0, 1, 0, &committed));
        // Pre-snapshot insert stays visible.
        assert!(is_visible(0, 0, 0, 1, 0, &committed));
    }
}
