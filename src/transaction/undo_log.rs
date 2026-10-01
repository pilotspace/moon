//! Undo log for KV transactional rollback.
//!
//! Records before-images of KV writes. On abort, the undo log is replayed
//! in reverse to restore the database to pre-transaction state.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::storage::entry::Entry;

/// A single undo record capturing the before-image of a KV write.
#[derive(Debug, Clone)]
pub enum UndoRecord {
    /// Key did not exist before write - remove on rollback.
    Insert { key: Bytes },
    /// Key had this entry before write - restore on rollback.
    Update { key: Bytes, old_entry: Entry },
    /// Key was deleted — restore old_entry on rollback.
    Delete { key: Bytes, old_entry: Entry },
    /// R2b W2 (moon#1300): a `TXN` connection or script write whose
    /// before-image lives in the transaction's HOLD (`isolation::hold`), not
    /// here — one copy of a key's pre-transaction value, shared by every
    /// snapshot and the abort. Either the key's FIRST write (the key was
    /// present; the hold took the image by move: the abort restores from it)
    /// or a LATER write of a key the transaction already holds (the abort
    /// restores the first record only, so no image is needed at all).
    /// `deleted`: the write was a `DEL` / `UNLINK` (a [`Self::Delete`]'s
    /// kind; otherwise an [`Self::Update`]'s, for the commit's WAL image).
    Held { key: Bytes, deleted: bool },
}

/// Per-transaction undo log.
///
/// Uses SmallVec to inline typical small transactions (up to 16 records)
/// without heap allocation. Larger transactions spill to heap.
///
/// moon#1285: every record carries the logical database it was captured in
/// (`dbs[i]` belongs to `records[i]`). A `SELECT` inside the transaction moves
/// the connection to another database, and `TXN.ABORT` used to replay every
/// record into the database selected AT ABORT TIME — restoring keys into the
/// wrong database. The compensating AOF / replication records the abort now
/// emits must name the same database the undo writes to, so the index is
/// kept per record rather than per transaction.
#[derive(Debug, Clone, Default)]
pub struct UndoLog {
    records: SmallVec<[UndoRecord; 16]>,
    dbs: SmallVec<[usize; 16]>,
}

impl UndoLog {
    /// Create a new empty undo log.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an insert in database `db` (key did not exist before).
    #[inline]
    pub fn record_insert(&mut self, db: usize, key: Bytes) {
        self.push(db, UndoRecord::Insert { key });
    }

    /// Record an update in database `db` (key had a previous entry).
    #[inline]
    pub fn record_update(&mut self, db: usize, key: Bytes, old_entry: Entry) {
        self.push(db, UndoRecord::Update { key, old_entry });
    }

    /// Record a delete in database `db` (captures before-image for rollback).
    #[inline]
    pub fn record_delete(&mut self, db: usize, key: Bytes, old_entry: Entry) {
        self.push(db, UndoRecord::Delete { key, old_entry });
    }

    /// Record a write in database `db` whose before-image the transaction's
    /// hold keeps ([`UndoRecord::Held`]).
    #[inline]
    pub fn record_held(&mut self, db: usize, key: Bytes, deleted: bool) {
        self.push(db, UndoRecord::Held { key, deleted });
    }

    #[inline]
    fn push(&mut self, db: usize, record: UndoRecord) {
        self.records.push(record);
        self.dbs.push(db);
    }

    /// Move every record of `other` to the end of this log, keeping each
    /// record's database and `other`'s order (moon#1285: a script's captured
    /// pre-images join the transaction's log at the script's position).
    #[inline]
    pub fn append(&mut self, other: UndoLog) {
        self.records.extend(other.records);
        self.dbs.extend(other.dbs);
    }

    /// Drop every record past the first `len` (moon#1285: a script write
    /// that answered an error wrote nothing, so the pre-images captured for
    /// it are taken back).
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.records.truncate(len);
        self.dbs.truncate(len);
    }

    /// Number of records in the undo log.
    #[inline]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the undo log is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Consume the undo log and return records in reverse order for rollback.
    #[inline]
    pub fn into_rollback_order(self) -> impl Iterator<Item = UndoRecord> {
        self.records.into_iter().rev()
    }

    /// Consume the undo log and return `(db, record)` in CAPTURE order.
    ///
    /// `TXN.ABORT` (moon#1285) walks this order and applies only the FIRST
    /// record of each `(db, key)`: that record's before-image is the key's
    /// pre-transaction state, which is exactly where the reverse replay of
    /// every record ends. One restore and one compensating log record per
    /// key, however often the transaction wrote it.
    #[inline]
    pub fn into_records_with_db(self) -> impl Iterator<Item = (usize, UndoRecord)> {
        self.dbs.into_iter().zip(self.records)
    }

    /// Every record with its database, in capture order (moon#1300: a
    /// script's first record of a key is the key's pre-transaction image).
    pub fn records_with_db(&self) -> impl Iterator<Item = (usize, &UndoRecord)> {
        self.dbs.iter().copied().zip(self.records.iter())
    }

    /// Get a reference to all records (for WAL serialization).
    #[inline]
    pub fn records(&self) -> &[UndoRecord] {
        &self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_log_inline_capacity() {
        let mut log = UndoLog::new();
        // Should stay inline for 16 records
        for i in 0..16 {
            log.record_insert(0, Bytes::from(format!("key{i}")));
        }
        assert_eq!(log.len(), 16);
    }

    #[test]
    fn test_rollback_order_reversed() {
        let mut log = UndoLog::new();
        log.record_insert(0, Bytes::from_static(b"a"));
        log.record_insert(0, Bytes::from_static(b"b"));
        log.record_insert(0, Bytes::from_static(b"c"));

        let keys: Vec<_> = log
            .into_rollback_order()
            .map(|r| match r {
                UndoRecord::Insert { key } => key,
                _ => panic!("expected insert"),
            })
            .collect();

        assert_eq!(
            keys,
            vec![
                Bytes::from_static(b"c"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"a"),
            ]
        );
    }

    #[test]
    fn test_delete_undo_captures_entry() {
        use crate::storage::entry::Entry;
        let mut log = UndoLog::new();
        let old = Entry::new_string(Bytes::from_static(b"original_value"));
        log.record_delete(0, Bytes::from_static(b"mykey"), old);
        assert_eq!(log.len(), 1);

        let records: Vec<_> = log.into_rollback_order().collect();
        match &records[0] {
            UndoRecord::Delete { key, old_entry } => {
                assert_eq!(key.as_ref(), b"mykey");
                assert_eq!(
                    old_entry.value.as_bytes().expect("string value"),
                    b"original_value"
                );
            }
            _ => panic!("expected Delete"),
        }
    }

    /// moon#1285: each record keeps the database it was captured in, in
    /// capture order.
    #[test]
    fn test_records_keep_their_database_in_capture_order() {
        let mut log = UndoLog::new();
        log.record_insert(3, Bytes::from_static(b"a"));
        log.record_delete(
            5,
            Bytes::from_static(b"b"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        log.record_update(
            3,
            Bytes::from_static(b"c"),
            Entry::new_string(Bytes::from_static(b"w")),
        );
        log.record_held(4, Bytes::from_static(b"d"), true);
        let got: Vec<(usize, Bytes)> = log
            .into_records_with_db()
            .map(|(db, r)| match r {
                UndoRecord::Insert { key }
                | UndoRecord::Update { key, .. }
                | UndoRecord::Delete { key, .. }
                | UndoRecord::Held { key, .. } => (db, key),
            })
            .collect();
        assert_eq!(
            got,
            vec![
                (3, Bytes::from_static(b"a")),
                (5, Bytes::from_static(b"b")),
                (3, Bytes::from_static(b"c")),
                (4, Bytes::from_static(b"d")),
            ]
        );
    }
}
