//! Sparse KV write-intent side-table for transactional MVCC.
//!
//! Only keys with active transactional writes have entries here.
//! Non-transactional operations bypass this entirely (zero overhead).

use std::collections::HashMap;

use bytes::Bytes;
use roaring::RoaringTreemap;

/// Write intent metadata for a single key.
#[derive(Debug, Clone, Copy)]
pub struct WriteIntent {
    /// LSN at which the write was performed.
    pub insert_lsn: u64,
    /// Transaction ID that owns this intent.
    pub txn_id: u64,
}

/// Sparse side-table tracking KV keys with active write intents.
///
/// Only transactional writes register here. Non-transactional reads and
/// writes bypass this structure entirely. On commit or abort, intents
/// are released in bulk.
#[derive(Debug, Clone, Default)]
pub struct KvWriteIntents {
    /// key -> WriteIntent for active transactions only.
    intents: HashMap<Bytes, WriteIntent>,
}

impl KvWriteIntents {
    /// Create a new empty side-table.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a write intent for a key.
    ///
    /// Returns the previous intent if one existed (for conflict detection).
    #[inline]
    pub fn record_write(&mut self, key: Bytes, lsn: u64, txn_id: u64) -> Option<WriteIntent> {
        self.intents.insert(
            key,
            WriteIntent {
                insert_lsn: lsn,
                txn_id,
            },
        )
    }

    /// Get the write intent for a key, if any.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&WriteIntent> {
        self.intents.get(key)
    }

    /// Release all intents owned by a transaction (on commit or abort).
    pub fn release_txn(&mut self, txn_id: u64) {
        self.intents.retain(|_, intent| intent.txn_id != txn_id);
    }

    /// Check if a key is visible to a snapshot read.
    ///
    /// Visibility rules:
    /// - No intent -> visible (non-transactional write)
    /// - Own transaction's intent -> visible (read-your-writes)
    /// - Intent LSN > snapshot_lsn -> not visible (future write)
    /// - Intent txn committed -> visible
    /// - Intent txn not committed -> not visible (uncommitted write)
    #[inline]
    pub fn is_key_visible(
        &self,
        key: &[u8],
        snapshot_lsn: u64,
        my_txn_id: u64,
        committed: &RoaringTreemap,
    ) -> bool {
        match self.intents.get(key) {
            None => true, // No active write intent
            Some(intent) => {
                // Own transaction's writes are always visible
                if intent.txn_id == my_txn_id {
                    return true;
                }
                // Future writes not visible
                if intent.insert_lsn > snapshot_lsn {
                    return false;
                }
                // Must be committed to be visible
                committed.contains(intent.txn_id)
            }
        }
    }

    /// Number of active write intents.
    #[inline]
    pub fn len(&self) -> usize {
        self.intents.len()
    }

    /// Check if there are no active write intents.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.intents.is_empty()
    }

    /// Get all keys with intents owned by a specific transaction.
    pub fn keys_for_txn(&self, txn_id: u64) -> impl Iterator<Item = &Bytes> {
        self.intents
            .iter()
            .filter(move |(_, intent)| intent.txn_id == txn_id)
            .map(|(key, _)| key)
    }
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
    fn test_no_intent_is_visible() {
        let intents = KvWriteIntents::new();
        assert!(intents.is_key_visible(b"key1", 100, 1, &empty_committed()));
    }

    #[test]
    fn test_own_intent_visible() {
        let mut intents = KvWriteIntents::new();
        intents.record_write(Bytes::from_static(b"key1"), 50, 1);
        // Own txn sees its writes
        assert!(intents.is_key_visible(b"key1", 100, 1, &empty_committed()));
    }

    #[test]
    fn test_future_intent_not_visible() {
        let mut intents = KvWriteIntents::new();
        intents.record_write(Bytes::from_static(b"key1"), 150, 2);
        // snapshot_lsn=100 < insert_lsn=150
        assert!(!intents.is_key_visible(b"key1", 100, 1, &empty_committed()));
    }

    #[test]
    fn test_committed_intent_visible() {
        let mut intents = KvWriteIntents::new();
        intents.record_write(Bytes::from_static(b"key1"), 50, 2);
        let committed = committed_with(&[2]);
        assert!(intents.is_key_visible(b"key1", 100, 1, &committed));
    }

    #[test]
    fn test_uncommitted_intent_not_visible() {
        let mut intents = KvWriteIntents::new();
        intents.record_write(Bytes::from_static(b"key1"), 50, 2);
        // txn 2 not committed
        assert!(!intents.is_key_visible(b"key1", 100, 1, &empty_committed()));
    }

    #[test]
    fn test_release_txn_removes_intents() {
        let mut intents = KvWriteIntents::new();
        intents.record_write(Bytes::from_static(b"key1"), 50, 1);
        intents.record_write(Bytes::from_static(b"key2"), 51, 1);
        intents.record_write(Bytes::from_static(b"key3"), 52, 2);

        intents.release_txn(1);

        assert_eq!(intents.len(), 1);
        assert!(intents.get(b"key1").is_none());
        assert!(intents.get(b"key2").is_none());
        assert!(intents.get(b"key3").is_some());
    }
}
