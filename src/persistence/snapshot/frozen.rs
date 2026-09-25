//! What a table a FLUSHDB detached keeps while the epoch holds it
//! (moon#1228, review 5).
//!
//! `Database::clear` hands the epoch its table AS FLUSHED: the database's
//! epoch-start rows, and every row written since the epoch began — rows the
//! file must not contain (each has a pre-image in the epoch's overflow map:
//! its epoch-start entry, or a tombstone for a key created since). Held
//! whole, that is unbounded: a client that fills a database and flushes it,
//! database after database, made one held save keep 8 x 40 MB of
//! post-epoch rows under `--maxmemory 64mb`, none of it in `used_memory`.
//! The drain therefore trims the table before it freezes it
//! ([`SnapshotState::trim_to_epoch_start`]), so a frozen table holds only
//! the epoch-start rows its database still has to write.

use super::{SnapshotState, Table, pre_image_bytes, segment_block};
use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::hash_key;
use crate::storage::db::entry_overhead;

impl SnapshotState {
    /// Trim `table`, which a FLUSHDB detached from epoch database `db`
    /// before the walk finished it, to the rows the walk still has to write:
    /// `db`'s EPOCH-START rows at or above the cursor. Returns the trimmed
    /// table's bill, from `table_bytes` (its `used_memory` at the flush)
    /// minus what was removed plus what was restored, each row at its
    /// `entry_overhead`.
    ///
    /// 1. Every key written since the epoch began has a pre-image in `db`'s
    ///    overflow map (all of them: the drain folds the queued captures in
    ///    before it applies a freeze, and nothing writes the slot's epoch
    ///    table after the flush). The row the table holds for such a key is
    ///    post-epoch: it is removed, and the epoch-start entry put back in
    ///    its place (a tombstone puts nothing back). The map for `db` is
    ///    then empty — its pre-images live in the table.
    /// 2. For the database in progress, rows below the cursor are written
    ///    already, or were written into a written range after the epoch
    ///    began (no pre-image is ever taken there): the walk never reads
    ///    them, so they go too.
    /// 3. Removing rows does not shrink a DashTable's segments. When the
    ///    post-epoch rows had grown the table to more than twice its
    ///    epoch-start segment count, the kept rows move into a table sized
    ///    for them.
    ///
    /// The kept rows are a subset of `db`'s epoch-start rows, each with its
    /// epoch-start entry, so the bill is at most `db`'s epoch-start bill.
    ///
    /// Cost, on the shard thread at the drain: step 1 is one remove (and
    /// one insert) per key written since the epoch began, work those writes
    /// already paid for when they were captured; step 2 visits only the
    /// segments below the cursor, which the walk already visited; step 3
    /// runs only when the post-epoch rows at least doubled the table, and
    /// moves at most the epoch-start rows.
    pub(super) fn trim_to_epoch_start(
        &mut self,
        db: usize,
        table: &mut Table,
        table_bytes: u64,
    ) -> u64 {
        let mut bytes = table_bytes;
        let charge =
            |key: &[u8], entry: &crate::storage::entry::Entry| entry_overhead(key, entry) as u64;

        // 1. Post-epoch rows out, epoch-start entries back.
        let pre_images = std::mem::take(&mut self.overflow[db]);
        for ((_, key), pre_image) in pre_images {
            self.overflow_bytes = self
                .overflow_bytes
                .saturating_sub(pre_image_bytes(&key, &pre_image));
            if let Some(post) = table.remove(&key) {
                bytes = bytes.saturating_sub(charge(&key, &post));
            }
            if let Some(entry) = pre_image {
                bytes = bytes.saturating_add(charge(&key, &entry));
                table.insert(CompactKey::from(key), entry);
            }
        }

        // 2. The database in progress: nothing below the cursor is read again.
        if db == self.current_db && self.cursor > 0 {
            let cursor = self.cursor;
            let mut written: Vec<CompactKey> = Vec::new();
            let mut at = 0u64;
            while at < cursor {
                let segment = table.segment(table.segment_index_for_hash(at));
                written.extend(
                    segment
                        .iter_occupied()
                        .filter(|(key, _)| hash_key(key.as_bytes()) < cursor)
                        .map(|(key, _)| key.clone()),
                );
                match segment_block(at, segment.depth()).1 {
                    Some(end) => at = end,
                    None => break,
                }
            }
            for key in written {
                if let Some(entry) = table.remove(key.as_bytes()) {
                    bytes = bytes.saturating_sub(charge(key.as_bytes(), &entry));
                }
            }
        }

        // 3. No skeleton of the post-epoch rows.
        let start_segments = self.segment_counts.get(db).copied().unwrap_or(1).max(1);
        if table.segment_count() > 2 * start_segments {
            let kept: Vec<CompactKey> = table.keys().cloned().collect();
            let mut sized = Table::with_capacity(kept.len());
            for key in kept {
                if let Some((key, entry)) = table.remove_entry(key.as_bytes()) {
                    sized.insert(key, entry);
                }
            }
            *table = sized;
        }
        bytes
    }
}
