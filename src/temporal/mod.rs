//! Bi-temporal registry and versioned KV index.
//!
//! `TemporalRegistry`: per-shard wall-clock -> LSN binding for
//! `TEMPORAL.SNAPSHOT_AT` / `FT.SEARCH AS_OF` queries. Uses `BTreeMap`
//! for O(log n) range lookups.
//!
//! `TemporalKvIndex`: sparse versioned KV index for temporal reads.
//! Lazy-init (cold path only). Stored as `Option<Box<TemporalKvIndex>>`
//! on shard state, initialized on first temporal KV write.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

/// Wall-clock to LSN mapping registry.
///
/// Records bindings from wall-clock timestamps (Unix millis, i64) to
/// shard-local LSN values (u64). Used by `TEMPORAL.SNAPSHOT_AT` to register
/// bindings and by `FT.SEARCH AS_OF T` to resolve T to a snapshot LSN.
///
/// CRITICAL: `record()` accepts literal wall_ms and lsn values captured
/// by the CALLER (command handler). This struct NEVER calls SystemTime::now().
/// See STATE.md P6 MEDIUM constraint.
#[derive(Debug, Default)]
pub struct TemporalRegistry {
    /// Sorted wall_ms -> lsn bindings.
    entries: BTreeMap<i64, u64>,
}

impl TemporalRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a wall-clock -> LSN binding.
    ///
    /// Both `wall_ms` and `lsn` MUST be captured by the caller at the
    /// handler level before calling this method.
    pub fn record(&mut self, wall_ms: i64, lsn: u64) {
        self.entries.insert(wall_ms, lsn);
    }

    /// Find the LSN that was active at wall-clock time T.
    ///
    /// Returns the LSN from the most recent binding at or before `wall_ms`.
    /// Returns `None` if `wall_ms` precedes all known bindings.
    pub fn lsn_at(&self, wall_ms: i64) -> Option<u64> {
        self.entries.range(..=wall_ms).next_back().map(|(_, &lsn)| lsn)
    }

    /// Number of registered bindings.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Versioned KV index for temporal reads (cold path only).
///
/// Maps keys to a sorted chain of `(valid_from_ms, value_bytes)` tuples.
/// Lookup uses `partition_point` for O(log n) binary search on the
/// version chain.
///
/// Lazy-init: stored as `Option<Box<TemporalKvIndex>>` on shard state,
/// initialized on first TemporalUpsert WAL write. Non-temporal reads
/// never touch this index.
#[derive(Debug, Default)]
pub struct TemporalKvIndex {
    /// key -> sorted version chain [(valid_from_ms, value_bytes)].
    versions: HashMap<Bytes, Vec<(i64, Bytes)>>,
}

impl TemporalKvIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a versioned KV entry.
    ///
    /// `valid_from` is the wall-clock timestamp (Unix millis) at which
    /// this version becomes effective. The chain is kept sorted by
    /// `valid_from` for binary search.
    pub fn record(&mut self, key: Bytes, valid_from: i64, value: Bytes) {
        let chain = self.versions.entry(key).or_default();
        chain.push((valid_from, value));
        // Maintain sort order for partition_point lookups.
        chain.sort_unstable_by_key(|(t, _)| *t);
    }

    /// Retrieve the value at a given wall-clock time.
    ///
    /// Returns the value from the most recent version with
    /// `valid_from <= valid_at`. Returns `None` if `valid_at` precedes
    /// all versions for this key.
    pub fn get_at(&self, key: &[u8], valid_at: i64) -> Option<&Bytes> {
        let chain = self.versions.get(key)?;
        let idx = chain.partition_point(|(t, _)| *t <= valid_at);
        if idx == 0 {
            None
        } else {
            Some(&chain[idx - 1].1)
        }
    }

    /// Number of keys tracked.
    pub fn key_count(&self) -> usize {
        self.versions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- TemporalRegistry tests ---

    #[test]
    fn test_registry_empty_returns_none() {
        let reg = TemporalRegistry::new();
        assert!(reg.lsn_at(1000).is_none());
    }

    #[test]
    fn test_registry_exact_match() {
        let mut reg = TemporalRegistry::new();
        reg.record(1000, 42);
        assert_eq!(reg.lsn_at(1000), Some(42));
    }

    #[test]
    fn test_registry_returns_most_recent_before() {
        let mut reg = TemporalRegistry::new();
        reg.record(1000, 10);
        reg.record(2000, 20);
        reg.record(3000, 30);
        assert_eq!(reg.lsn_at(2500), Some(20));
    }

    #[test]
    fn test_registry_before_first_entry_returns_none() {
        let mut reg = TemporalRegistry::new();
        reg.record(1000, 10);
        assert!(reg.lsn_at(500).is_none());
    }

    #[test]
    fn test_registry_at_boundary() {
        let mut reg = TemporalRegistry::new();
        reg.record(1000, 10);
        reg.record(2000, 20);
        assert_eq!(reg.lsn_at(2000), Some(20));
        assert_eq!(reg.lsn_at(1000), Some(10));
    }

    #[test]
    fn test_registry_after_last_entry() {
        let mut reg = TemporalRegistry::new();
        reg.record(1000, 10);
        assert_eq!(reg.lsn_at(9999), Some(10));
    }

    #[test]
    fn test_registry_overwrite_same_wall_ms() {
        let mut reg = TemporalRegistry::new();
        reg.record(1000, 10);
        reg.record(1000, 42);
        assert_eq!(reg.lsn_at(1000), Some(42));
    }

    // --- TemporalKvIndex tests ---

    #[test]
    fn test_kv_index_empty_returns_none() {
        let idx = TemporalKvIndex::new();
        assert!(idx.get_at(b"key1", 1000).is_none());
    }

    #[test]
    fn test_kv_index_single_version() {
        let mut idx = TemporalKvIndex::new();
        idx.record(Bytes::from_static(b"key1"), 1000, Bytes::from_static(b"v1"));
        assert_eq!(idx.get_at(b"key1", 1000), Some(&Bytes::from_static(b"v1")));
        assert_eq!(idx.get_at(b"key1", 2000), Some(&Bytes::from_static(b"v1")));
        assert!(idx.get_at(b"key1", 500).is_none());
    }

    #[test]
    fn test_kv_index_multiple_versions() {
        let mut idx = TemporalKvIndex::new();
        idx.record(Bytes::from_static(b"key1"), 1000, Bytes::from_static(b"v1"));
        idx.record(Bytes::from_static(b"key1"), 2000, Bytes::from_static(b"v2"));
        idx.record(Bytes::from_static(b"key1"), 3000, Bytes::from_static(b"v3"));
        assert_eq!(idx.get_at(b"key1", 1500), Some(&Bytes::from_static(b"v1")));
        assert_eq!(idx.get_at(b"key1", 2000), Some(&Bytes::from_static(b"v2")));
        assert_eq!(idx.get_at(b"key1", 2500), Some(&Bytes::from_static(b"v2")));
        assert_eq!(idx.get_at(b"key1", 5000), Some(&Bytes::from_static(b"v3")));
    }

    #[test]
    fn test_kv_index_before_first_version() {
        let mut idx = TemporalKvIndex::new();
        idx.record(Bytes::from_static(b"key1"), 1000, Bytes::from_static(b"v1"));
        assert!(idx.get_at(b"key1", 999).is_none());
    }

    #[test]
    fn test_kv_index_different_keys() {
        let mut idx = TemporalKvIndex::new();
        idx.record(Bytes::from_static(b"a"), 1000, Bytes::from_static(b"va"));
        idx.record(Bytes::from_static(b"b"), 2000, Bytes::from_static(b"vb"));
        assert_eq!(idx.get_at(b"a", 1000), Some(&Bytes::from_static(b"va")));
        assert_eq!(idx.get_at(b"b", 2000), Some(&Bytes::from_static(b"vb")));
        assert!(idx.get_at(b"a", 500).is_none());
        assert!(idx.get_at(b"c", 1000).is_none());
    }

    #[test]
    fn test_kv_index_out_of_order_insert() {
        let mut idx = TemporalKvIndex::new();
        idx.record(Bytes::from_static(b"key1"), 3000, Bytes::from_static(b"v3"));
        idx.record(Bytes::from_static(b"key1"), 1000, Bytes::from_static(b"v1"));
        idx.record(Bytes::from_static(b"key1"), 2000, Bytes::from_static(b"v2"));
        assert_eq!(idx.get_at(b"key1", 1500), Some(&Bytes::from_static(b"v1")));
        assert_eq!(idx.get_at(b"key1", 2500), Some(&Bytes::from_static(b"v2")));
        assert_eq!(idx.get_at(b"key1", 4000), Some(&Bytes::from_static(b"v3")));
    }
}
