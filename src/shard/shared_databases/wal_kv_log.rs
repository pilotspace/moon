//! The per-shard `--wal-kv-log` flag of [`ShardDatabases`] (moon#1275), for
//! the connection-side legs that write a KV record to the WAL themselves
//! (the SWAPDB local leg). Kept out of `shared_databases.rs`, which is over
//! the file-size cap.

use std::sync::atomic::Ordering;

use super::ShardDatabases;

impl ShardDatabases {
    /// Record this shard's `--wal-kv-log` decision (moon#1275): whether KV
    /// command records go to its WAL v3. Set by every SPSC drain cycle,
    /// which already computes it; a store only when it changed.
    #[inline]
    pub fn publish_wal_kv_log(&self, shard_id: usize, on: bool) {
        if let Some(flag) = self.wal_kv_log.get(shard_id)
            && flag.load(Ordering::Relaxed) != on
        {
            flag.store(on, Ordering::Relaxed);
        }
    }

    /// Whether KV command records go to shard `shard_id`'s WAL v3 (see
    /// [`Self::publish_wal_kv_log`]); `false` until published. A KV record
    /// written there otherwise makes a tokio `--shards 1` recovery take the
    /// WAL for the KV authority and never replay the AOF (moon#1275).
    #[inline]
    pub fn wal_kv_log(&self, shard_id: usize) -> bool {
        self.wal_kv_log
            .get(shard_id)
            .is_some_and(|f| f.load(Ordering::Relaxed))
    }
}
