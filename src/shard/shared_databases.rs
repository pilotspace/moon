use std::sync::Arc;

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, MutexGuard};

use crate::storage::Database;
use crate::vector::store::VectorStore;

/// Thread-safe wrapper over per-shard databases.
///
/// Each shard owns `db_count` databases (SELECT 0-15). The outer Vec is indexed
/// by shard_id, the inner Vec by db_index. All access goes through `read_db()`
/// (shared) or `write_db()` (exclusive) to enable cross-shard direct reads.
pub struct ShardDatabases {
    shards: Vec<Vec<RwLock<Database>>>,
    /// Per-shard VectorStore for FT.* commands in single-shard mode.
    vector_stores: Vec<Mutex<VectorStore>>,
    num_shards: usize,
    db_count: usize,
}

impl ShardDatabases {
    /// Create from pre-restored database vectors (one `Vec<Database>` per shard).
    pub fn new(shard_databases: Vec<Vec<Database>>) -> Arc<Self> {
        let num_shards = shard_databases.len();
        let db_count = shard_databases.first().map_or(0, |v| v.len());
        let shards = shard_databases
            .into_iter()
            .map(|dbs| dbs.into_iter().map(RwLock::new).collect())
            .collect();
        let vector_stores = (0..num_shards)
            .map(|_| Mutex::new(VectorStore::new()))
            .collect();
        Arc::new(Self {
            shards,
            vector_stores,
            num_shards,
            db_count,
        })
    }

    /// Acquire exclusive access to a shard's VectorStore.
    #[inline]
    pub fn vector_store(&self, shard_id: usize) -> MutexGuard<'_, VectorStore> {
        self.vector_stores[shard_id].lock()
    }

    /// Acquire a shared read lock on a specific database.
    #[inline]
    pub fn read_db(&self, shard_id: usize, db_index: usize) -> RwLockReadGuard<'_, Database> {
        debug_assert!(
            shard_id < self.shards.len(),
            "shard_id {shard_id} out of bounds ({})",
            self.shards.len()
        );
        debug_assert!(
            db_index < self.shards[shard_id].len(),
            "db_index {db_index} out of bounds ({})",
            self.shards[shard_id].len()
        );
        self.shards[shard_id][db_index].read()
    }

    /// Acquire an exclusive write lock on a specific database.
    ///
    /// Fast path: bounded spin with `try_write()` for the common case where
    /// cross-shard read locks are held briefly (~51ns for DashTable lookup).
    /// Slow path: falls back to blocking `write()` (OS-level parking) to avoid
    /// infinite busy-spin when readers hold locks longer (e.g., KEYS, SCAN).
    #[inline]
    pub fn write_db(&self, shard_id: usize, db_index: usize) -> RwLockWriteGuard<'_, Database> {
        debug_assert!(
            shard_id < self.shards.len(),
            "shard_id {shard_id} out of bounds ({})",
            self.shards.len()
        );
        debug_assert!(
            db_index < self.shards[shard_id].len(),
            "db_index {db_index} out of bounds ({})",
            self.shards[shard_id].len()
        );
        // Spin briefly — resolves in 1-3 iterations for typical cross-shard reads.
        for _ in 0..32 {
            if let Some(guard) = self.shards[shard_id][db_index].try_write() {
                return guard;
            }
            std::hint::spin_loop();
        }
        // Slow path: park the thread via OS futex instead of busy-spinning.
        self.shards[shard_id][db_index].write()
    }

    /// Non-blocking write lock attempt for async callers that can yield.
    #[inline]
    pub fn try_write_db(
        &self,
        shard_id: usize,
        db_index: usize,
    ) -> Option<RwLockWriteGuard<'_, Database>> {
        self.shards[shard_id][db_index].try_write()
    }

    /// Total number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Number of databases per shard (typically 16).
    #[inline]
    pub fn db_count(&self) -> usize {
        self.db_count
    }

    /// Collect snapshot metadata (segment counts, base timestamps) for a shard.
    ///
    /// Acquires brief read locks on each database to gather metadata needed
    /// by `SnapshotState::new_from_metadata`. Called once at snapshot epoch start.
    pub fn snapshot_metadata(&self, shard_id: usize) -> (Vec<usize>, Vec<u32>) {
        let mut segment_counts = Vec::with_capacity(self.db_count);
        let mut base_timestamps = Vec::with_capacity(self.db_count);
        for i in 0..self.db_count {
            let guard = self.read_db(shard_id, i);
            segment_counts.push(guard.data().segment_count());
            base_timestamps.push(guard.base_timestamp());
        }
        (segment_counts, base_timestamps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_correct_dimensions() {
        let dbs = vec![
            vec![Database::new(), Database::new()],
            vec![Database::new(), Database::new()],
            vec![Database::new(), Database::new()],
        ];
        let shared = ShardDatabases::new(dbs);
        assert_eq!(shared.num_shards(), 3);
        assert_eq!(shared.db_count(), 2);
    }

    #[test]
    fn test_read_db_returns_shared_guard() {
        let dbs = vec![vec![Database::new()]];
        let shared = ShardDatabases::new(dbs);
        let _guard1 = shared.read_db(0, 0);
        let _guard2 = shared.read_db(0, 0); // Multiple readers OK
    }

    #[test]
    fn test_write_db_returns_exclusive_guard() {
        let dbs = vec![vec![Database::new()]];
        let shared = ShardDatabases::new(dbs);
        let _guard = shared.write_db(0, 0);
        // Exclusive access -- would deadlock if we tried another write_db here
    }

    #[test]
    fn test_cross_shard_access() {
        let dbs = vec![vec![Database::new()], vec![Database::new()]];
        let shared = ShardDatabases::new(dbs);
        // Can read from different shards concurrently
        let _guard0 = shared.read_db(0, 0);
        let _guard1 = shared.read_db(1, 0);
    }

    #[test]
    fn test_snapshot_metadata() {
        let dbs = vec![vec![Database::new(), Database::new()]];
        let shared = ShardDatabases::new(dbs);
        let (seg_counts, base_ts) = shared.snapshot_metadata(0);
        assert_eq!(seg_counts.len(), 2);
        assert_eq!(base_ts.len(), 2);
    }

    #[test]
    fn test_empty_shard_databases() {
        let dbs: Vec<Vec<Database>> = vec![];
        let shared = ShardDatabases::new(dbs);
        assert_eq!(shared.num_shards(), 0);
        assert_eq!(shared.db_count(), 0);
    }
}
