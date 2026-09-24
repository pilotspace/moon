//! The dead-slot ledger (moon#1215): spill-file slots that are still on disk
//! in a manifest-listed file but are no longer their key's cold-index entry.
//!
//! A spill file is unlinked only when its LAST live key leaves it, so a file
//! with live neighbours keeps every slot it was written with — including the
//! slots of keys that have since been deleted, flushed, overwritten,
//! read-promoted, re-spilled elsewhere, or expired. After a restart the cold
//! index is rebuilt from EVERY slot of every listed file, so each such slot
//! comes back unless something durable says it is gone. Until an AOF rewrite
//! that something is the DEL/FLUSH record in the log; the rewrite discards
//! it, and its new generation's `MOON.COLDCUT` authorizes the file wholesale.
//!
//! This ledger is what the rewrite uses instead: at the fold instant every
//! key it holds that is not alive gets a plain `DEL` at the head of the new
//! generation (`persistence::aof::fold_stream`). It lives inside
//! [`super::cold_index::ColdIndex`] so that every path that takes a slot out
//! of the index records it, with no call site to forget.
//!
//! Lifetime: a file's entries go when the file is unlinked (its slots are
//! gone from disk with it). Nothing else removes them — a key that is alive
//! again at a later fold is filtered then, not forgotten, because its old
//! slot is still on disk and would come back the moment the key dies again.

use std::collections::HashMap;

use bytes::Bytes;

/// Approximate RAM of one ledger entry beyond its key bytes: the `Bytes`
/// handle plus the per-file `Vec` slot. Monotonic, not exact — the same
/// approximation style as `cold_index::COLD_ENTRY_OVERHEAD`.
const DEAD_SLOT_OVERHEAD: usize = std::mem::size_of::<Bytes>() + 8;

#[inline]
fn dead_slot_cost(key_len: usize) -> usize {
    key_len + DEAD_SLOT_OVERHEAD
}

/// Keys with a dead slot, by the file that holds the slot.
#[derive(Debug, Default)]
pub struct DeadSlots {
    by_file: HashMap<u64, Vec<Bytes>>,
    resident_bytes: usize,
    len: usize,
}

impl DeadSlots {
    /// Record that `file_id` holds a slot of `key` that is no longer the
    /// key's cold-index entry.
    pub fn note(&mut self, file_id: u64, key: Bytes) {
        self.resident_bytes += dead_slot_cost(key.len());
        self.len += 1;
        self.by_file.entry(file_id).or_default().push(key);
    }

    /// `file_id` is gone from disk: its slots cannot come back.
    pub fn forget_file(&mut self, file_id: u64) {
        if let Some(keys) = self.by_file.remove(&file_id) {
            self.len -= keys.len();
            let freed: usize = keys.iter().map(|k| dead_slot_cost(k.len())).sum();
            self.resident_bytes = self.resident_bytes.saturating_sub(freed);
        }
    }

    /// Fold another ledger into this one (recovery merges per-db indexes).
    pub fn merge(&mut self, other: DeadSlots) {
        for (file_id, keys) in other.by_file {
            for key in keys {
                self.note(file_id, key);
            }
        }
    }

    /// Every key with at least one dead slot. A key dead in several files is
    /// yielded once per file; callers dedupe.
    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.by_file.values().flatten()
    }

    /// Number of dead slots recorded.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Approximate RAM this ledger holds (charged to the cold index).
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Whether `file_id` has any dead slot recorded (tests, diagnostics).
    pub fn file_has_dead_slots(&self, file_id: u64) -> bool {
        self.by_file.get(&file_id).is_some_and(|k| !k.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_files_entries_go_with_the_file_and_the_accounting_follows() {
        let mut d = DeadSlots::default();
        assert!(d.is_empty());
        d.note(1, Bytes::from_static(b"a"));
        d.note(1, Bytes::from_static(b"bb"));
        d.note(2, Bytes::from_static(b"a"));
        assert_eq!(d.len(), 3);
        let full = d.resident_bytes();
        assert_eq!(full, 3 * DEAD_SLOT_OVERHEAD + 4);
        let mut keys: Vec<&[u8]> = d.keys().map(|k| k.as_ref()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![&b"a"[..], b"a", b"bb"]);

        d.forget_file(1);
        assert_eq!(d.len(), 1);
        assert!(!d.file_has_dead_slots(1));
        assert!(d.file_has_dead_slots(2));
        assert_eq!(d.resident_bytes(), DEAD_SLOT_OVERHEAD + 1);
        d.forget_file(9); // unknown file: no-op
        d.forget_file(2);
        assert!(d.is_empty());
        assert_eq!(d.resident_bytes(), 0);
    }

    #[test]
    fn merge_keeps_every_entry() {
        let mut a = DeadSlots::default();
        a.note(1, Bytes::from_static(b"x"));
        let mut b = DeadSlots::default();
        b.note(1, Bytes::from_static(b"y"));
        b.note(3, Bytes::from_static(b"z"));
        a.merge(b);
        assert_eq!(a.len(), 3);
        assert!(a.file_has_dead_slots(3));
    }
}
