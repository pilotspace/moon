use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::storage::Database;

/// Thread-safe wrapper over per-shard databases.
///
/// Each shard owns `db_count` databases (SELECT 0-15). The outer Vec is indexed
/// by shard_id, the inner Vec by db_index. All access goes through `read_db()`
/// (shared) or `write_db()` (exclusive) to enable cross-shard direct reads.
pub struct ShardDatabases {
    shards: Vec<Vec<RwLock<Database>>>,
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
        Arc::new(Self {
            shards,
            num_shards,
            db_count,
        })
    }

    /// Acquire a shared read lock on a specific database.
    #[inline]
    pub fn read_db(&self, shard_id: usize, db_index: usize) -> RwLockReadGuard<'_, Database> {
        self.shards[shard_id][db_index].read()
    }

    /// Acquire an exclusive write lock on a specific database.
    #[inline]
    pub fn write_db(&self, shard_id: usize, db_index: usize) -> RwLockWriteGuard<'_, Database> {
        self.shards[shard_id][db_index].write()
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

    /// Take ownership of a shard's databases, replacing with defaults.
    ///
    /// Only valid during initialization before any concurrent access begins.
    /// Each `RwLock` content is swapped with a fresh `Database::default()`.
    pub fn take_shard(&self, shard_id: usize) -> Vec<Database> {
        self.shards[shard_id]
            .iter()
            .map(|lock| std::mem::take(&mut *lock.write()))
            .collect()
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
    fn test_take_shard_extracts_databases() {
        let dbs = vec![vec![Database::new(), Database::new()]];
        let shared = ShardDatabases::new(dbs);
        let taken = shared.take_shard(0);
        assert_eq!(taken.len(), 2);
        // After take, the shard's databases should be default (empty)
        assert_eq!(shared.db_count(), 2);
    }

    #[test]
    fn test_empty_shard_databases() {
        let dbs: Vec<Vec<Database>> = vec![];
        let shared = ShardDatabases::new(dbs);
        assert_eq!(shared.num_shards(), 0);
        assert_eq!(shared.db_count(), 0);
    }
}
