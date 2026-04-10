use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::storage::Database;
use crate::vector::store::VectorStore;
#[cfg(feature = "graph")]
use crate::graph::store::GraphStore;

/// Thread-safe wrapper over per-shard databases.
///
/// Each shard owns `db_count` databases (SELECT 0-15). The outer Vec is indexed
/// by shard_id, the inner Vec by db_index. All access goes through `read_db()`
/// (shared) or `write_db()` (exclusive) to enable cross-shard direct reads.
pub struct ShardDatabases {
    shards: Vec<Vec<RwLock<Database>>>,
    /// Per-shard VectorStore for FT.* commands in single-shard mode.
    vector_stores: Vec<Mutex<VectorStore>>,
    /// Per-shard GraphStore for GRAPH.* commands.
    #[cfg(feature = "graph")]
    graph_stores: Vec<Mutex<GraphStore>>,
    /// Per-shard WAL append channel sender. Connection handlers send serialized
    /// write commands here; the event loop drains into WAL v2/v3 on the 1ms tick.
    /// Mutex<Option<>> for single-writer init, then read-only via wal_append().
    wal_append_txs: Vec<Mutex<Option<crate::runtime::channel::MpscSender<bytes::Bytes>>>>,
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
        #[cfg(feature = "graph")]
        let graph_stores = (0..num_shards)
            .map(|_| Mutex::new(GraphStore::new()))
            .collect();
        let wal_append_txs = (0..num_shards).map(|_| Mutex::new(None)).collect();
        Arc::new(Self {
            shards,
            vector_stores,
            #[cfg(feature = "graph")]
            graph_stores,
            wal_append_txs,
            num_shards,
            db_count,
        })
    }

    /// Set the WAL append channel sender for a shard.
    ///
    /// Called once during event loop startup. Uses interior mutability via
    /// unsafe transmutation of the Arc — safe because this is called exactly
    /// once per shard before any connections are accepted.
    /// Set the WAL append channel sender for a shard.
    /// Called once during event loop startup before connections are accepted.
    pub fn set_wal_append_tx(
        &self,
        shard_id: usize,
        tx: crate::runtime::channel::MpscSender<bytes::Bytes>,
    ) {
        *self.wal_append_txs[shard_id].lock() = Some(tx);
    }

    /// Send serialized command bytes to the WAL append channel for a shard.
    ///
    /// Called by connection handlers for local write commands. The event loop
    /// drains this channel on the 1ms tick into WAL v2/v3.
    /// No-op when persistence is disabled.
    #[inline]
    pub fn wal_append(&self, shard_id: usize, data: bytes::Bytes) {
        if let Some(ref tx) = *self.wal_append_txs[shard_id].lock() {
            let _ = tx.try_send(data);
        }
    }

    /// Acquire exclusive access to a shard's VectorStore.
    #[inline]
    pub fn vector_store(&self, shard_id: usize) -> MutexGuard<'_, VectorStore> {
        self.vector_stores[shard_id].lock()
    }

    /// Acquire exclusive access to a shard's GraphStore.
    #[cfg(feature = "graph")]
    #[inline]
    pub fn graph_store(&self, shard_id: usize) -> MutexGuard<'_, GraphStore> {
        self.graph_stores[shard_id].lock()
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

    /// Return a reference to all databases across all shards (for AOF rewrite).
    /// Callers iterate shards × dbs and acquire read locks individually.
    #[inline]
    pub fn all_shard_dbs(&self) -> &[Vec<RwLock<Database>>] {
        &self.shards
    }

    /// Number of databases per shard (typically 16).
    #[inline]
    pub fn db_count(&self) -> usize {
        self.db_count
    }

    /// Aggregate estimated memory across all databases in a shard.
    ///
    /// Acquires read locks briefly on each DB. Used for maxmemory eviction
    /// decisions (Redis maxmemory is a server-wide limit, not per-DB).
    pub fn aggregate_memory(&self, shard_id: usize) -> usize {
        let mut total = 0usize;
        for db_idx in 0..self.db_count {
            let guard = self.read_db(shard_id, db_idx);
            total += guard.estimated_memory();
        }
        total
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
