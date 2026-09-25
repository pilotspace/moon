//! Bulk-load helpers for RDB/AOF restore: unchecked insert, the one-pass
//! memory recalculation and table pre-sizing (split from `db/kv_ops.rs`;
//! pure move).

use bytes::Bytes;

use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::DashTable;
use crate::storage::db::{Database, entry_overhead};
use crate::storage::entry::Entry;

impl Database {
    /// Bulk-load insert: skip duplicate check and per-key memory accounting.
    ///
    /// Used exclusively during RDB/AOF restore where keys are guaranteed unique and
    /// we recalculate `used_memory` once after the entire load completes.
    ///
    /// Still draws a creation ticket, unlike the rest of the fast path it skips.
    /// Versions are not persisted, so every restored entry would otherwise carry
    /// `INITIAL_VERSION`, and the FIRST key created after a restore would draw
    /// ticket 1 and collide with them — reopening the ABA hole for exactly the
    /// window right after a restart. One u32 increment per restored key against
    /// a decode is not measurable.
    #[inline]
    pub fn insert_for_load(&mut self, key: Bytes, mut entry: Entry) {
        // moon#1163: `recalculate_memory` bills a stream at `billed_memory`;
        // a loaded one is measured here, once.
        super::settle_stream_billing(&mut entry);
        if entry.has_expiry() {
            self.maybe_has_expiring_keys = true;
            self.expiry_index_insert(entry.expires_at_ms(), &key);
        }
        self.hash_expiry_index_note_value(&key, &entry);
        entry.set_version(self.next_birth_version());
        self.data.insert(CompactKey::from(key), entry);
    }

    /// Recalculate `used_memory` by scanning all entries. Call once after bulk load.
    pub fn recalculate_memory(&mut self) {
        // moon#1190: rebuilt from the hot table alone — a queued lazy-free
        // value is no longer in it, so it must not be credited later.
        self.lazy_free_forget_charges();
        let mut total = 0usize;
        let mut any_expiring = false;
        // moon#541: this post-bulk-load pass is also the index's healer —
        // rebuild it from scratch so any load path that bypassed
        // `insert_for_load` still ends consistent.
        let mut index = std::collections::BTreeSet::new();
        let mut hash_index = std::collections::BTreeSet::new();
        for (key, entry) in self.data.iter() {
            total += entry_overhead(key.as_bytes(), entry);
            if entry.has_expiry() {
                any_expiring = true;
                index.insert(super::expiry_index::ExpiryPair {
                    ts: entry.expires_at_ms(),
                    key: key.clone(),
                });
            }
            // moon#543: the same healing property for the hash-field index —
            // a load path that bypassed `insert_for_load` still ends indexed.
            if let crate::storage::compact_value::RedisValueRef::HashWithTtl { ttls, .. } =
                entry.value.as_redis_value()
                && let Some(min) = ttls.values().copied().min()
            {
                hash_index.insert((min, key.clone()));
            }
        }
        self.used_memory = total;
        self.maybe_has_expiring_keys = any_expiring;
        self.expiry_index = index;
        // Authoritative, like the whole-key index rebuild above.
        self.hash_expiry_index = hash_index;
    }

    /// Pre-size the internal hash table for an expected key count.
    ///
    /// WARNING: This REPLACES the internal hash table with a fresh one sized for
    /// `additional` entries. It MUST be called on an empty `Database` (typically
    /// immediately after `Database::new()` during RDB/AOF bulk load). Calling it
    /// on a populated database silently discards all entries.
    ///
    /// Named `reserve` rather than `reset_with_capacity` to match the plan
    /// nomenclature, but the debug assertion guards the misuse case.
    pub fn reserve(&mut self, additional: usize) {
        debug_assert!(
            self.data.is_empty(),
            "Database::reserve() must only be called on an empty database (bulk-load pre-sizing); called with {} existing entries",
            self.data.len()
        );
        if additional > self.data.len() {
            let new_table = DashTable::with_capacity(additional);
            self.data = new_table;
            self.maybe_has_expiring_keys = false;
            // moon#541: the replaced table's index/latch state goes with it —
            // matching `clear` (no-ops on the documented empty-db call, but
            // the misuse case must not leave stale pairs behind).
            self.expiry_index.clear();
            self.hash_expiry_index.clear();
        }
    }
}
