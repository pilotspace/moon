use std::collections::HashMap;
use std::collections::hash_map;

use roaring::RoaringBitmap;

/// Error returned when a write-write conflict is detected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictError {
    pub point_id: u64,
    pub owner: u64,
}

/// Active transaction metadata.
#[derive(Debug, Clone)]
pub struct ActiveTxn {
    pub txn_id: u64,
    pub snapshot_lsn: u64,
}

/// Per-shard MVCC transaction manager.
///
/// Owns: monotonic LSN counter, active txn map, write-intent map,
/// committed bitmap, oldest_snapshot watermark.
///
/// NOT Send/Sync -- owned exclusively by shard thread (same as VectorStore).
///
/// Note: txn_ids are stored as u32 in RoaringBitmap. This limits the committed
/// set to 4 billion transactions. For Phase 65 this is acceptable.
/// All `as u32` casts are guarded against overflow.
pub struct TransactionManager {
    next_lsn: u64,
    /// Active transactions: txn_id -> snapshot_lsn.
    active: HashMap<u64, u64>,
    /// Write intents: point_id -> owning txn_id. First-writer-wins.
    write_intents: HashMap<u64, u64>,
    /// Committed transaction IDs (stored as u32 -- wraps beyond u32::MAX).
    committed: RoaringBitmap,
    /// Oldest active snapshot LSN (for zombie cleanup watermark).
    oldest_snapshot: u64,
}

impl TransactionManager {
    /// Create a new transaction manager with LSN starting at 1.
    pub fn new() -> Self {
        Self {
            next_lsn: 1,
            active: HashMap::new(),
            write_intents: HashMap::new(),
            committed: RoaringBitmap::new(),
            oldest_snapshot: 0,
        }
    }

    /// Begin a new transaction. Returns monotonically increasing txn_id
    /// with snapshot_lsn = next_lsn - 1 (sees everything committed before this point).
    pub fn begin(&mut self) -> ActiveTxn {
        let snapshot_lsn = self.next_lsn - 1;
        let txn_id = self.next_lsn;
        self.next_lsn += 1;
        self.active.insert(txn_id, snapshot_lsn);

        // If this is the only active txn, update oldest_snapshot
        if self.active.len() == 1 {
            self.oldest_snapshot = snapshot_lsn;
        }

        ActiveTxn {
            txn_id,
            snapshot_lsn,
        }
    }

    /// Get the snapshot LSN for an active transaction. Returns None if not active.
    pub fn get_snapshot(&self, txn_id: u64) -> Option<u64> {
        self.active.get(&txn_id).copied()
    }

    /// Acquire a write intent on a point. First-writer-wins conflict detection.
    ///
    /// - Vacant: insert, return Ok
    /// - Same txn_id: idempotent Ok
    /// - Owner committed or aborted (not active): steal intent, Ok
    /// - Owner active and different: Err(ConflictError)
    pub fn acquire_write(&mut self, point_id: u64, txn_id: u64) -> Result<(), ConflictError> {
        match self.write_intents.entry(point_id) {
            hash_map::Entry::Vacant(e) => {
                e.insert(txn_id);
                Ok(())
            }
            hash_map::Entry::Occupied(mut e) => {
                let owner = *e.get();
                if owner == txn_id {
                    // Idempotent re-acquire
                    Ok(())
                } else if Self::txn_id_to_u32(owner).is_some_and(|id| self.committed.contains(id))
                    || !self.active.contains_key(&owner)
                {
                    // Owner committed or aborted -- steal the intent
                    e.insert(txn_id);
                    Ok(())
                } else {
                    // Active owner conflict
                    Err(ConflictError { point_id, owner })
                }
            }
        }
    }

    /// Commit a transaction. Adds to committed bitmap, removes from active,
    /// releases write intents. Returns false if txn was not active.
    pub fn commit(&mut self, txn_id: u64) -> bool {
        if self.active.remove(&txn_id).is_none() {
            return false;
        }
        if let Some(id) = Self::txn_id_to_u32(txn_id) {
            self.committed.insert(id);
        }
        self.write_intents.retain(|_, owner| *owner != txn_id);
        self.update_oldest_snapshot();
        true
    }

    /// Abort a transaction. Removes from active, releases write intents,
    /// does NOT add to committed. Returns false if txn was not active.
    pub fn abort(&mut self, txn_id: u64) -> bool {
        if self.active.remove(&txn_id).is_none() {
            return false;
        }
        self.write_intents.retain(|_, owner| *owner != txn_id);
        self.update_oldest_snapshot();
        true
    }

    /// Check if a transaction ID has been committed.
    #[inline]
    pub fn is_committed(&self, txn_id: u64) -> bool {
        Self::txn_id_to_u32(txn_id).is_some_and(|id| self.committed.contains(id))
    }

    /// Get the oldest active snapshot LSN.
    #[inline]
    pub fn oldest_snapshot(&self) -> u64 {
        self.oldest_snapshot
    }

    /// Sweep write intents owned by aborted transactions (neither active nor committed).
    /// Returns list of (point_id, txn_id) for stale intents.
    ///
    /// Vec allocation acceptable -- runs on background timer, not hot path.
    pub fn sweep_zombies(&self) -> Vec<(u64, u64)> {
        let mut zombies = Vec::new();
        for (&point_id, &owner) in &self.write_intents {
            let in_committed =
                Self::txn_id_to_u32(owner).is_some_and(|id| self.committed.contains(id));
            if !self.active.contains_key(&owner) && !in_committed {
                zombies.push((point_id, owner));
            }
        }
        zombies
    }

    /// Number of active transactions.
    #[inline]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Number of committed transactions.
    #[inline]
    pub fn committed_count(&self) -> u64 {
        self.committed.len()
    }

    /// Access the committed bitmap (for visibility checks).
    #[inline]
    pub fn committed_bitmap(&self) -> &RoaringBitmap {
        &self.committed
    }

    /// Try to convert a u64 txn_id to u32 for RoaringBitmap operations.
    /// Returns `None` and logs an error if the id exceeds u32::MAX.
    #[inline]
    fn txn_id_to_u32(id: u64) -> Option<u32> {
        if id > u32::MAX as u64 {
            tracing::error!(
                txn_id = id,
                "txn_id exceeds u32::MAX, cannot store in RoaringBitmap"
            );
            None
        } else {
            Some(id as u32)
        }
    }

    /// Recalculate oldest_snapshot from active transactions.
    fn update_oldest_snapshot(&mut self) {
        if self.active.is_empty() {
            self.oldest_snapshot = self.next_lsn;
        } else {
            self.oldest_snapshot = self.active.values().copied().min().unwrap_or(self.next_lsn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_returns_unique_monotonic_txn_ids() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        let t2 = mgr.begin();
        let t3 = mgr.begin();
        assert!(t1.txn_id < t2.txn_id);
        assert!(t2.txn_id < t3.txn_id);
        // All unique
        assert_ne!(t1.txn_id, t2.txn_id);
        assert_ne!(t2.txn_id, t3.txn_id);
    }

    #[test]
    fn test_begin_records_snapshot_lsn() {
        let mut mgr = TransactionManager::new();
        // next_lsn starts at 1, so snapshot_lsn = 0 for first txn
        let t1 = mgr.begin();
        assert_eq!(t1.snapshot_lsn, 0);
        assert_eq!(t1.txn_id, 1);

        // next_lsn is now 2, snapshot_lsn = 1
        let t2 = mgr.begin();
        assert_eq!(t2.snapshot_lsn, 1);
        assert_eq!(t2.txn_id, 2);
    }

    #[test]
    fn test_acquire_write_first_writer_succeeds() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_write(100, t1.txn_id).is_ok());
    }

    #[test]
    fn test_acquire_write_same_txn_idempotent() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_write(100, t1.txn_id).is_ok());
        // Re-acquire same point by same txn -- should succeed
        assert!(mgr.acquire_write(100, t1.txn_id).is_ok());
    }

    #[test]
    fn test_acquire_write_conflict_with_active_txn() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        let t2 = mgr.begin();
        assert!(mgr.acquire_write(100, t1.txn_id).is_ok());
        // t2 tries to acquire same point -- conflict
        let err = mgr.acquire_write(100, t2.txn_id).unwrap_err();
        assert_eq!(err.point_id, 100);
        assert_eq!(err.owner, t1.txn_id);
    }

    #[test]
    fn test_acquire_write_steals_from_committed() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_write(100, t1.txn_id).is_ok());
        mgr.commit(t1.txn_id);

        // t2 can steal the intent since t1 is committed
        let t2 = mgr.begin();
        assert!(mgr.acquire_write(100, t2.txn_id).is_ok());
    }

    #[test]
    fn test_acquire_write_steals_from_aborted() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_write(100, t1.txn_id).is_ok());
        mgr.abort(t1.txn_id);

        // t2 can steal the intent since t1 is aborted (not active, not committed)
        let t2 = mgr.begin();
        assert!(mgr.acquire_write(100, t2.txn_id).is_ok());
    }

    #[test]
    fn test_commit_adds_to_committed_removes_from_active() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.committed_count(), 0);

        mgr.acquire_write(100, t1.txn_id).unwrap();
        assert!(mgr.commit(t1.txn_id));

        assert_eq!(mgr.active_count(), 0);
        assert_eq!(mgr.committed_count(), 1);
        assert!(mgr.is_committed(t1.txn_id));
        // Write intent released
        assert!(mgr.sweep_zombies().is_empty());
    }

    #[test]
    fn test_abort_removes_from_active_not_committed() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        mgr.acquire_write(100, t1.txn_id).unwrap();
        assert!(mgr.abort(t1.txn_id));

        assert_eq!(mgr.active_count(), 0);
        assert_eq!(mgr.committed_count(), 0);
        assert!(!mgr.is_committed(t1.txn_id));
    }

    #[test]
    fn test_oldest_snapshot_updated_on_commit_abort() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin(); // snapshot_lsn = 0
        let t2 = mgr.begin(); // snapshot_lsn = 1
        let _t3 = mgr.begin(); // snapshot_lsn = 2

        assert_eq!(mgr.oldest_snapshot(), 0); // t1's snapshot

        mgr.commit(t1.txn_id);
        assert_eq!(mgr.oldest_snapshot(), 1); // t2's snapshot is now oldest

        mgr.abort(t2.txn_id);
        assert_eq!(mgr.oldest_snapshot(), 2); // t3's snapshot is now oldest
    }

    #[test]
    fn test_sweep_zombies_finds_aborted_intents() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        mgr.acquire_write(100, t1.txn_id).unwrap();
        mgr.acquire_write(200, t1.txn_id).unwrap();

        // Abort releases intents owned by t1
        mgr.abort(t1.txn_id);

        // After abort, write_intents are cleaned up, so sweep_zombies finds nothing
        let zombies = mgr.sweep_zombies();
        assert!(zombies.is_empty());
    }

    #[test]
    fn test_get_snapshot_returns_none_for_nonexistent() {
        let mgr = TransactionManager::new();
        assert!(mgr.get_snapshot(999).is_none());
    }

    #[test]
    fn test_get_snapshot_returns_value_for_active() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert_eq!(mgr.get_snapshot(t1.txn_id), Some(t1.snapshot_lsn));
    }

    #[test]
    fn test_commit_nonexistent_returns_false() {
        let mut mgr = TransactionManager::new();
        assert!(!mgr.commit(999));
    }

    #[test]
    fn test_abort_nonexistent_returns_false() {
        let mut mgr = TransactionManager::new();
        assert!(!mgr.abort(999));
    }

    #[test]
    fn test_oldest_snapshot_advances_when_empty() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        mgr.commit(t1.txn_id);
        // No active txns -- oldest_snapshot should be next_lsn
        assert_eq!(mgr.oldest_snapshot(), mgr.next_lsn);
    }
}
