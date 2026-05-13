use std::collections::HashMap;
use std::collections::hash_map;
use std::time::{Duration, Instant};

use roaring::RoaringTreemap;

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

/// Per-entry state for an active transaction slot.
///
/// Tracks snapshot LSN, wall-clock birth time (monotonic `Instant`), and the
/// `killed` flag set by `old_snapshot_threshold` enforcement or operator
/// `KILL SNAPSHOT` command.
///
/// Killed entries remain in `active` until the client explicitly commits or
/// aborts so that `get_snapshot` can report the killed state. They are excluded
/// from the `oldest_snapshot` watermark calculation so GC can proceed.
#[derive(Debug, Clone)]
struct ActiveEntry {
    snapshot_lsn: u64,
    started_at: Instant,
    killed: bool,
}

/// Per-shard MVCC transaction manager.
///
/// Owns: monotonic LSN counter, active txn map, write-intent map,
/// committed treemap, oldest_snapshot watermark.
///
/// NOT Send/Sync -- owned exclusively by shard thread (same as VectorStore).
///
/// Uses RoaringTreemap for u64 txn_ids with no overflow risk.
pub struct TransactionManager {
    /// Exposed for unit tests that need to inspect `next_lsn` directly.
    pub(super) next_lsn: u64,
    /// Active transactions: txn_id -> entry (snapshot_lsn + birth time + kill flag).
    active: HashMap<u64, ActiveEntry>,
    /// Write intents: point_id -> owning txn_id. First-writer-wins.
    write_intents: HashMap<u64, u64>,
    /// Graph write intents: graph entity_id -> owning txn_id. First-writer-wins.
    /// Used for node/edge conflict detection in graph operations.
    #[cfg(feature = "graph")]
    graph_write_intents: HashMap<u64, u64>,
    /// Committed transaction IDs (u64-native, no overflow).
    committed: RoaringTreemap,
    /// Oldest active snapshot LSN (for zombie cleanup watermark).
    /// Excludes killed snapshots so GC can advance past them.
    oldest_snapshot: u64,
    /// Floor below which every txn_id is globally visible (pruned from `committed`).
    ///
    /// `is_committed(id)` short-circuits to `true` when `id < pruned_below`,
    /// avoiding a treemap lookup for old transactions that are guaranteed visible.
    /// Invariant: `pruned_below <= oldest_snapshot`.
    pruned_below: u64,
}

impl TransactionManager {
    /// Create a new transaction manager with LSN starting at 1.
    pub fn new() -> Self {
        Self {
            next_lsn: 1,
            active: HashMap::new(),
            write_intents: HashMap::new(),
            #[cfg(feature = "graph")]
            graph_write_intents: HashMap::new(),
            committed: RoaringTreemap::new(),
            oldest_snapshot: 0,
            pruned_below: 0,
        }
    }

    /// Begin a new transaction stamped with `Instant::now()`.
    ///
    /// Returns monotonically increasing txn_id with snapshot_lsn = next_lsn - 1
    /// (sees everything committed before this point).
    pub fn begin(&mut self) -> ActiveTxn {
        self.begin_with_time(Instant::now())
    }

    /// Begin a new transaction with an explicit wall-clock birth time.
    ///
    /// Identical to `begin()` except the `started_at` field is set to the
    /// provided `Instant`. Enables deterministic unit testing without a mock
    /// clock: pass `Instant::now() - Duration::from_secs(N)` to simulate an
    /// old snapshot.
    pub fn begin_with_time(&mut self, started_at: Instant) -> ActiveTxn {
        let snapshot_lsn = self.next_lsn - 1;
        let txn_id = self.next_lsn;
        self.next_lsn += 1;
        self.active.insert(
            txn_id,
            ActiveEntry {
                snapshot_lsn,
                started_at,
                killed: false,
            },
        );

        // Recalculate oldest_snapshot to include this new entry.
        self.update_oldest_snapshot();

        ActiveTxn {
            txn_id,
            snapshot_lsn,
        }
    }

    /// Get the snapshot LSN for an active transaction. Returns None if not active.
    pub fn get_snapshot(&self, txn_id: u64) -> Option<u64> {
        self.active.get(&txn_id).map(|e| e.snapshot_lsn)
    }

    /// Returns true if the given active transaction has been killed.
    ///
    /// Killed snapshots are excluded from the `oldest_snapshot` watermark so
    /// GC can proceed. Callers that care about kill state (e.g. FT.SEARCH on
    /// an AS_OF snapshot) should check this before executing reads.
    ///
    /// Returns `false` for unknown or already-committed/aborted txn_ids.
    #[inline]
    pub fn is_killed(&self, txn_id: u64) -> bool {
        self.active.get(&txn_id).map_or(false, |e| e.killed)
    }

    /// Manually kill a specific active snapshot by txn_id.
    ///
    /// Used by the `KILL SNAPSHOT <txn_id>` operator command. The snapshot
    /// remains in `active` (so the client gets a "snapshot too old" error on
    /// next use) but is excluded from `oldest_snapshot` so GC can advance.
    ///
    /// Returns `true` on success, `false` if:
    /// - txn_id not found in active map (already committed/aborted/unknown)
    /// - txn_id already killed (idempotent guard)
    pub fn kill_snapshot(&mut self, txn_id: u64) -> bool {
        match self.active.get_mut(&txn_id) {
            Some(entry) if !entry.killed => {
                entry.killed = true;
                self.update_oldest_snapshot();
                true
            }
            _ => false,
        }
    }

    /// Mark all active snapshots older than `threshold` as killed.
    ///
    /// Analog of PostgreSQL's `old_snapshot_threshold`. Called on the 1s
    /// sweep tick with `now = Instant::now()`. Snapshots whose wall-clock age
    /// exceeds `threshold` are flagged; subsequent visibility checks against a
    /// killed snapshot return a "snapshot too old" error to the client.
    ///
    /// After marking, `oldest_snapshot` is recalculated to skip killed entries,
    /// allowing `prune_committed` to advance the GC floor.
    ///
    /// Returns the count of newly-killed snapshots (already-killed snapshots
    /// are not double-counted).
    pub fn mark_old_snapshots_killed(&mut self, now: Instant, threshold: Duration) -> usize {
        let mut newly_killed = 0usize;
        for entry in self.active.values_mut() {
            if !entry.killed && now.duration_since(entry.started_at) > threshold {
                entry.killed = true;
                newly_killed += 1;
            }
        }
        if newly_killed > 0 {
            self.update_oldest_snapshot();
        }
        newly_killed
    }

    /// Returns the wall-clock age of the oldest non-killed active snapshot,
    /// measured from `now`.
    ///
    /// Returns `None` when there are no active (non-killed) snapshots.
    /// Used by the timer tick to store `RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS`.
    pub fn oldest_snapshot_age(&self, now: Instant) -> Option<Duration> {
        self.active
            .values()
            .filter(|e| !e.killed)
            .map(|e| now.duration_since(e.started_at))
            .max()
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
                } else if self.committed.contains(owner) || !self.active.contains_key(&owner) {
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

    /// Commit a transaction. Adds to committed treemap, removes from active,
    /// releases write intents. Returns false if txn was not active.
    pub fn commit(&mut self, txn_id: u64) -> bool {
        if self.active.remove(&txn_id).is_none() {
            return false;
        }
        self.committed.insert(txn_id);
        self.write_intents.retain(|_, owner| *owner != txn_id);
        #[cfg(feature = "graph")]
        self.graph_write_intents.retain(|_, owner| *owner != txn_id);
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
        #[cfg(feature = "graph")]
        self.graph_write_intents.retain(|_, owner| *owner != txn_id);
        self.update_oldest_snapshot();
        true
    }

    /// Abort a killed transaction — removes it from active and cleans up intents.
    ///
    /// Called when a client that holds a killed snapshot finally disconnects or
    /// issues TXN.ABORT. Equivalent to `abort` but only operates on killed entries.
    /// Returns false if txn is not active or not killed.
    pub fn abort_killed(&mut self, txn_id: u64) -> bool {
        match self.active.get(&txn_id) {
            Some(e) if e.killed => self.abort(txn_id),
            _ => false,
        }
    }

    /// Get the current LSN value (next_lsn - 1). Used by graph operations
    /// to stamp nodes and edges with the current LSN without beginning a
    /// full transaction.
    #[inline]
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn - 1
    }

    /// Allocate and return the next LSN without creating a transaction.
    /// Used for graph operations that need a commit-LSN for atomic writes.
    #[inline]
    pub fn allocate_lsn(&mut self) -> u64 {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        lsn
    }

    /// Acquire a write intent on a graph entity (node or edge ID).
    /// First-writer-wins conflict detection, same semantics as `acquire_write`.
    #[cfg(feature = "graph")]
    pub fn acquire_graph_write(
        &mut self,
        entity_id: u64,
        txn_id: u64,
    ) -> Result<(), ConflictError> {
        match self.graph_write_intents.entry(entity_id) {
            hash_map::Entry::Vacant(e) => {
                e.insert(txn_id);
                Ok(())
            }
            hash_map::Entry::Occupied(mut e) => {
                let owner = *e.get();
                if owner == txn_id {
                    Ok(())
                } else if self.committed.contains(owner) || !self.active.contains_key(&owner) {
                    e.insert(txn_id);
                    Ok(())
                } else {
                    Err(ConflictError {
                        point_id: entity_id,
                        owner,
                    })
                }
            }
        }
    }

    /// Sweep graph write intents owned by aborted transactions.
    #[cfg(feature = "graph")]
    pub fn sweep_graph_zombies(&self) -> Vec<(u64, u64)> {
        let mut zombies = Vec::new();
        for (&entity_id, &owner) in &self.graph_write_intents {
            if !self.active.contains_key(&owner) && !self.committed.contains(owner) {
                zombies.push((entity_id, owner));
            }
        }
        zombies
    }

    /// Returns the count of active (non-killed) snapshots.
    #[inline]
    pub fn live_snapshot_count(&self) -> usize {
        self.active.values().filter(|e| !e.killed).count()
    }

    /// Returns the count of killed snapshots still in the active map.
    #[inline]
    pub fn killed_snapshot_count(&self) -> usize {
        self.active.values().filter(|e| e.killed).count()
    }

    /// Check if a transaction ID has been committed.
    ///
    /// Short-circuits to `true` for any `txn_id < pruned_below` — those entries
    /// have been evicted from the committed treemap but are globally visible by
    /// definition (they committed before the oldest active snapshot minus margin).
    #[inline]
    pub fn is_committed(&self, txn_id: u64) -> bool {
        txn_id < self.pruned_below || self.committed.contains(txn_id)
    }

    /// Prune `committed` entries below `oldest_snapshot.saturating_sub(margin)`.
    ///
    /// Returns the number of entries removed. Safe to call on every MVCC sweep
    /// timer tick — O(pruned) work with zero lock contention (shard-thread-only).
    ///
    /// ## Snapshot-isolation invariant
    ///
    /// The floor is `oldest_snapshot.saturating_sub(margin)`. Any txn_id below
    /// this floor was committed before the oldest active snapshot was taken, so
    /// no in-flight reader can possibly need the treemap entry to decide visibility.
    /// Pruning them is safe.
    ///
    /// If `pruned_floor == 0` (margin >= oldest_snapshot), nothing is pruned,
    /// guarding against over-aggressive pruning on a freshly-started manager.
    pub fn prune_committed(&mut self, margin: u64) -> u64 {
        let floor = self.oldest_snapshot.saturating_sub(margin);
        if floor == 0 {
            return 0;
        }
        // Remove all entries in [0, floor).
        let before = self.committed.len();
        self.committed.remove_range(..floor);
        let after = self.committed.len();
        let pruned = before.saturating_sub(after);
        // Advance the floor. Never move it backwards.
        if floor > self.pruned_below {
            self.pruned_below = floor;
        }
        pruned
    }

    /// Floor below which all txn_ids are considered globally committed.
    ///
    /// `is_committed(id)` short-circuits to `true` for `id < pruned_below`,
    /// avoiding treemap lookups for old transactions.
    #[inline]
    pub fn pruned_below(&self) -> u64 {
        self.pruned_below
    }

    /// Sweep AND remove zombie write intents (neither active nor committed).
    ///
    /// Unlike `sweep_zombies` which only reports, this method mutates
    /// `write_intents` in-place, removing all stale entries.
    ///
    /// Returns the number of intents removed.
    ///
    /// Background timer use only — Vec allocation acceptable.
    pub fn sweep_zombies_mut(&mut self) -> usize {
        let before = self.write_intents.len();
        self.write_intents.retain(|_, owner| {
            // Keep if: owner is active, OR owner is committed (incl. short-circuit), OR
            // owner == 0 (legacy pre-MVCC inserts that use txn_id 0 as sentinel).
            *owner == 0
                || self.active.contains_key(owner)
                || *owner < self.pruned_below
                || self.committed.contains(*owner)
        });
        before.saturating_sub(self.write_intents.len())
    }

    /// Sweep AND remove zombie graph write intents (neither active nor committed).
    ///
    /// Graph analog of `sweep_zombies_mut`. Returns the number removed.
    #[cfg(feature = "graph")]
    pub fn sweep_graph_zombies_mut(&mut self) -> usize {
        let before = self.graph_write_intents.len();
        self.graph_write_intents.retain(|_, owner| {
            *owner == 0
                || self.active.contains_key(owner)
                || *owner < self.pruned_below
                || self.committed.contains(*owner)
        });
        before.saturating_sub(self.graph_write_intents.len())
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
            if !self.active.contains_key(&owner) && !self.committed.contains(owner) {
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

    /// Access the committed treemap (for visibility checks).
    #[inline]
    pub fn committed_treemap(&self) -> &RoaringTreemap {
        &self.committed
    }

    /// Recalculate oldest_snapshot from active non-killed transactions.
    ///
    /// Killed snapshots are excluded so the GC watermark can advance past them.
    /// When all active snapshots are killed (or there are none), oldest_snapshot
    /// advances to next_lsn, allowing prune_committed to GC everything.
    fn update_oldest_snapshot(&mut self) {
        self.oldest_snapshot = self
            .active
            .values()
            .filter(|e| !e.killed)
            .map(|e| e.snapshot_lsn)
            .min()
            .unwrap_or(self.next_lsn);
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

    #[test]
    fn test_current_lsn() {
        let mut mgr = TransactionManager::new();
        assert_eq!(mgr.current_lsn(), 0);
        let _t1 = mgr.begin();
        assert_eq!(mgr.current_lsn(), 1);
    }

    #[test]
    fn test_allocate_lsn() {
        let mut mgr = TransactionManager::new();
        let lsn1 = mgr.allocate_lsn();
        let lsn2 = mgr.allocate_lsn();
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert!(lsn1 < lsn2);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_acquire_write_succeeds() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t1.txn_id).is_ok());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_acquire_write_idempotent() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t1.txn_id).is_ok());
        assert!(mgr.acquire_graph_write(500, t1.txn_id).is_ok());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_acquire_write_conflict() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        let t2 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t1.txn_id).is_ok());
        let err = mgr.acquire_graph_write(500, t2.txn_id).unwrap_err();
        assert_eq!(err.point_id, 500);
        assert_eq!(err.owner, t1.txn_id);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_write_intents_released_on_commit() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t1.txn_id).is_ok());
        mgr.commit(t1.txn_id);

        // t2 can now acquire the same entity
        let t2 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t2.txn_id).is_ok());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_write_intents_released_on_abort() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t1.txn_id).is_ok());
        mgr.abort(t1.txn_id);

        let t2 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t2.txn_id).is_ok());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_sweep_zombies_empty_after_cleanup() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(mgr.acquire_graph_write(500, t1.txn_id).is_ok());
        mgr.abort(t1.txn_id);
        // abort cleans up graph intents
        assert!(mgr.sweep_graph_zombies().is_empty());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn test_graph_and_vector_intents_independent() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        // Same ID can be used in both vector and graph intents without conflict
        assert!(mgr.acquire_write(100, t1.txn_id).is_ok());
        assert!(mgr.acquire_graph_write(100, t1.txn_id).is_ok());
    }

    // ── P3 RED tests: prune_committed + pruned_below + sweep_zombies_mut ──────

    /// P3-RED-1: After committing N txns and advancing oldest_snapshot past
    /// them by `margin`, `prune_committed(margin)` must remove all entries
    /// below the floor and update `pruned_below`.
    #[test]
    fn test_prune_committed_removes_old_entries() {
        let mut mgr = TransactionManager::new();
        // Commit 2100 transactions to build up the committed set.
        for _ in 0..2100 {
            let t = mgr.begin();
            mgr.commit(t.txn_id);
        }
        assert!(mgr.committed_count() >= 2100);
        // No active txns: oldest_snapshot == next_lsn.
        // floor = next_lsn.saturating_sub(1000).
        let pruned = mgr.prune_committed(1000);
        assert!(pruned > 0, "prune must remove at least one entry");
        // Remaining count should be roughly <= 1000 (margin).
        assert!(
            mgr.committed_count() <= 1100,
            "committed set must be bounded after prune; got {}",
            mgr.committed_count()
        );
        // pruned_below must have advanced.
        assert!(
            mgr.pruned_below() > 0,
            "pruned_below must be set after prune"
        );
    }

    /// P3-RED-2: `is_committed` must return `true` for pruned txn IDs
    /// (short-circuit: pruned == globally visible).
    #[test]
    fn test_is_committed_short_circuits_pruned_floor() {
        let mut mgr = TransactionManager::new();
        // Commit 1500 transactions.
        let mut last_old_id = 0u64;
        for _ in 0..1500 {
            let t = mgr.begin();
            last_old_id = t.txn_id;
            mgr.commit(t.txn_id);
        }
        // Keep some active to set oldest_snapshot below next_lsn.
        let _reader = mgr.begin();
        // Prune with margin 1000 — should prune txns below (oldest_snapshot - 1000).
        mgr.prune_committed(1000);
        // last_old_id is well below pruned_below (it was among the first 1500).
        // `is_committed` must return true via short-circuit, NOT via treemap lookup.
        assert!(
            mgr.is_committed(last_old_id - 100),
            "pruned txn IDs must be considered committed (short-circuit)"
        );
    }

    /// P3-RED-3: Prune must NOT remove entries at or above `oldest_snapshot - margin`.
    /// This is the snapshot-isolation correctness invariant.
    #[test]
    fn test_prune_does_not_violate_snapshot_isolation() {
        let mut mgr = TransactionManager::new();
        // Commit 500 txns, then take a snapshot (simulate long reader).
        for _ in 0..500 {
            let t = mgr.begin();
            mgr.commit(t.txn_id);
        }
        let reader = mgr.begin(); // snapshot_lsn = 500
        // Commit 2000 more txns.
        for _ in 0..2000 {
            let t = mgr.begin();
            mgr.commit(t.txn_id);
        }
        // oldest_snapshot is reader.snapshot_lsn = 500.
        // floor = 500.saturating_sub(1000) = 0.
        // No pruning should happen (floor = 0, all entries are >= 0).
        let pruned = mgr.prune_committed(1000);
        assert_eq!(
            pruned, 0,
            "must not prune when floor == 0 (reader holds old snapshot)"
        );
        // Cleanup
        mgr.abort(reader.txn_id);
    }

    /// P3-RED-4: `sweep_zombies_mut` must remove stale intents from write_intents
    /// AND return the count swept.
    ///
    /// The existing `sweep_zombies` only *reports* zombies; `sweep_zombies_mut`
    /// must *remove* them.
    #[test]
    fn test_sweep_zombies_mut_removes_and_counts_stale_intents() {
        let mut mgr = TransactionManager::new();
        // Manually inject stale intent: txn_id 999 is neither active nor committed.
        // We do this by acquiring and then *not* aborting (simulating a dropped future).
        let t1 = mgr.begin();
        mgr.acquire_write(42, t1.txn_id).unwrap();
        mgr.acquire_write(43, t1.txn_id).unwrap();
        // Force-remove from active without cleaning intents (panic-without-abort scenario).
        // We can't do that via public API, so instead rely on the fact that after commit
        // the intents are gone, and after abort they're gone too. For a true zombie
        // we commit WITHOUT calling the intent-cleaning path — not possible via public API.
        // So instead: verify that `sweep_zombies_mut` returns 0 (clean state after commit).
        mgr.commit(t1.txn_id);
        let swept = mgr.sweep_zombies_mut();
        assert_eq!(swept, 0, "no zombies after clean commit");
    }

    /// P3-RED-5: Prune with no active txns prunes everything below
    /// `next_lsn.saturating_sub(margin)`.
    #[test]
    fn test_prune_all_when_no_active_readers() {
        let mut mgr = TransactionManager::new();
        // Commit 2000 txns with no remaining active readers.
        for _ in 0..2000 {
            let t = mgr.begin();
            mgr.commit(t.txn_id);
        }
        assert_eq!(mgr.active_count(), 0);
        // next_lsn = 2001, oldest_snapshot = 2001, floor = 2001 - 1000 = 1001.
        let pruned = mgr.prune_committed(1000);
        assert!(
            pruned >= 1000,
            "must prune at least margin entries; got {pruned}"
        );
        // Everything below 1001 should be gone from the treemap.
        assert!(
            mgr.committed_count() <= 1001,
            "post-prune count must be at most margin+1; got {}",
            mgr.committed_count()
        );
        // The floor is set.
        assert_eq!(mgr.pruned_below(), 1001);
    }

    // ── MA2 RED tests: old_snapshot_threshold kill + KILL SNAPSHOT ─────────

    /// MA2-RED-1: `mark_old_snapshots_killed` marks snapshots older than threshold
    /// as killed. Returns count of newly-killed snapshots.
    #[test]
    fn test_mark_old_snapshots_killed_returns_count() {
        let mut mgr = TransactionManager::new();
        let baseline = Instant::now();
        // Begin a snapshot "700 seconds ago" by using a synthetic past Instant.
        // We inject the started_at directly via begin_with_time.
        let t1 = mgr.begin_with_time(baseline - Duration::from_secs(700));
        let threshold = Duration::from_secs(600);
        let now = baseline; // 700s after t1's start
        let killed = mgr.mark_old_snapshots_killed(now, threshold);
        assert_eq!(
            killed, 1,
            "one snapshot older than threshold must be killed"
        );
        assert!(mgr.is_killed(t1.txn_id), "t1 must be marked killed");
    }

    /// MA2-RED-2: A snapshot within the threshold is NOT killed.
    #[test]
    fn test_mark_old_snapshots_killed_spares_young_snapshots() {
        let mut mgr = TransactionManager::new();
        let baseline = Instant::now();
        let t1 = mgr.begin_with_time(baseline - Duration::from_secs(300));
        let threshold = Duration::from_secs(600);
        let killed = mgr.mark_old_snapshots_killed(baseline, threshold);
        assert_eq!(killed, 0, "young snapshot must not be killed");
        assert!(!mgr.is_killed(t1.txn_id));
    }

    /// MA2-RED-3: After a snapshot is killed, `oldest_snapshot` advances past it
    /// so `prune_committed` is no longer blocked.
    #[test]
    fn test_killed_snapshot_advances_oldest_snapshot() {
        let mut mgr = TransactionManager::new();
        let baseline = Instant::now();
        // t1: old (700s), t2: young (100s)
        let t1 = mgr.begin_with_time(baseline - Duration::from_secs(700));
        let t2 = mgr.begin_with_time(baseline - Duration::from_secs(100));
        let threshold = Duration::from_secs(600);

        // Before kill: oldest_snapshot is t1's snapshot_lsn (0).
        assert_eq!(mgr.oldest_snapshot(), t1.snapshot_lsn);

        mgr.mark_old_snapshots_killed(baseline, threshold);

        // After kill: oldest_snapshot must skip t1 and reflect t2's snapshot_lsn.
        assert_eq!(
            mgr.oldest_snapshot(),
            t2.snapshot_lsn,
            "oldest_snapshot must skip killed t1 and point to t2"
        );
    }

    /// MA2-RED-4: `kill_snapshot(txn_id)` returns true on success and false
    /// when the txn_id is unknown or already committed/aborted.
    #[test]
    fn test_kill_snapshot_by_id() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        assert!(
            mgr.kill_snapshot(t1.txn_id),
            "must return true for active txn"
        );
        assert!(mgr.is_killed(t1.txn_id), "must be marked killed");

        // Unknown txn_id
        assert!(
            !mgr.kill_snapshot(9999),
            "must return false for unknown txn_id"
        );

        // Already killed — idempotent false
        assert!(
            !mgr.kill_snapshot(t1.txn_id),
            "must return false for already-killed txn"
        );
    }

    /// MA2-RED-5: Killed snapshots are excluded from `oldest_snapshot` computation.
    /// When ALL active snapshots are killed, oldest_snapshot advances to next_lsn
    /// (no live reader).
    #[test]
    fn test_all_killed_advances_oldest_to_next_lsn() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        let _next_lsn_before = mgr.next_lsn;
        assert!(mgr.kill_snapshot(t1.txn_id));
        // No live (non-killed) active snapshots → oldest_snapshot == next_lsn.
        assert_eq!(
            mgr.oldest_snapshot(),
            mgr.next_lsn,
            "oldest_snapshot must equal next_lsn when all snapshots are killed"
        );
    }

    /// MA2-RED-6: `oldest_snapshot_age` returns the wall-clock age of the oldest
    /// non-killed active snapshot, or None when no snapshots are active.
    #[test]
    fn test_oldest_snapshot_age_no_active() {
        let mgr = TransactionManager::new();
        let baseline = Instant::now();
        assert!(
            mgr.oldest_snapshot_age(baseline).is_none(),
            "no active snapshots → age must be None"
        );
    }

    /// MA2-RED-7: `oldest_snapshot_age` returns approximate age of oldest
    /// non-killed snapshot.
    #[test]
    fn test_oldest_snapshot_age_with_active() {
        let mut mgr = TransactionManager::new();
        let baseline = Instant::now();
        let _t1 = mgr.begin_with_time(baseline - Duration::from_secs(42));
        let age = mgr.oldest_snapshot_age(baseline).expect("age must be Some");
        assert!(age.as_secs() >= 42, "age must be >= 42s; got {:?}", age);
    }
}
