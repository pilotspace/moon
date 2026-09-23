//! Keys the LIVE write path indexed while this shard's boot index recovery
//! was still running (moon#1124).
//!
//! Boot recovery snapshots every recovered document's key as a baseline,
//! walks the keys it LISTED, and then deletes (the "deletion probe") every
//! document whose key the walk never observed. Writes routed here from
//! another shard are applied during the walk (the `-LOADING` gate guards the
//! connection path only). A key that was absent when the walk listed keys and
//! is re-created by such a write gets indexed by the live path but is never
//! observed by the walk, so the probe used to delete the document that had
//! just been written.
//!
//! The live auto-index hook records `(db, key_hash)` here while recovery is
//! running; a live whole-key delete removes the record again, so a key that
//! was written and then deleted during the walk still ends deleted. Both
//! planes' `finish` skip a recorded key.
//!
//! Lives on [`crate::vector::store::VectorStore`] as an `Option`: `None`
//! outside recovery, so the steady-state write path pays one branch.

use std::collections::HashSet;

/// See the module docs.
#[derive(Debug, Default)]
pub struct LiveWriteLedger {
    written: HashSet<(u8, u64)>,
}

impl LiveWriteLedger {
    /// The live path indexed `key_hash` in `db_index`.
    pub fn note_write(&mut self, db_index: u8, key_hash: u64) {
        self.written.insert((db_index, key_hash));
    }

    /// The live path deleted the whole key `key_hash` in `db_index`.
    pub fn note_delete(&mut self, db_index: u8, key_hash: u64) {
        self.written.remove(&(db_index, key_hash));
    }

    /// Db-unscoped delete: forget `key_hash` in every db.
    pub fn note_delete_any_db(&mut self, key_hash: u64) {
        if !self.written.is_empty() {
            self.written.retain(|&(_, kh)| kh != key_hash);
        }
    }

    /// Whether the live path's last word on `key_hash` in `db_index` was a
    /// write — the deletion probe must then leave its document alone.
    #[must_use]
    pub fn written(&self, db_index: u8, key_hash: u64) -> bool {
        self.written.contains(&(db_index, key_hash))
    }

    /// Number of recorded keys (log line / tests).
    #[must_use]
    pub fn len(&self) -> usize {
        self.written.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.written.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_delete_forgets_and_is_db_scoped() {
        let mut l = LiveWriteLedger::default();
        l.note_write(0, 7);
        l.note_write(1, 7);
        assert!(l.written(0, 7) && l.written(1, 7));
        l.note_delete(0, 7);
        assert!(!l.written(0, 7), "a delete in db 0 forgets db 0 only");
        assert!(l.written(1, 7));
        l.note_delete_any_db(7);
        assert!(l.is_empty());
    }
}
