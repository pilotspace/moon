//! Recording a key's epoch-start state: by copy for a writer that changes a
//! key in place, by MOVE for one that has just removed it (moon#1257 review
//! F1).
//!
//! Split out of the `snapshot_cow` parent to keep it under the 1500-line cap;
//! a child module sees the parent's thread-locals.

use std::collections::HashSet;

use bytes::Bytes;

use super::{DEDUPE_BYTES, PENDING, PENDING_KEYS, PROGRESS, is_armed};
use crate::persistence::snapshot::PreImage;
use crate::storage::db::Database;
use crate::storage::entry::Entry;

#[cfg(test)]
thread_local! {
    /// Deep clones [`capture_key`] made on this thread (tests).
    static CLONES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Deep clones this thread's captures have made (tests).
#[cfg(test)]
pub(crate) fn pre_image_clones_for_test() -> usize {
    CLONES.with(std::cell::Cell::get)
}

/// The epoch database a write in shard slot `slot` to `key` must record a
/// pre-image under, or `None` when it needs none.
///
/// moon#1186: a key whose range is already written needs no pre-image — the
/// file holds its epoch-start bytes and the drain would drop the copy. Skip it
/// BEFORE the deep clone. moon#1228: nor does a write to a slot whose epoch
/// table a flush detached (its contents are post-epoch).
fn target(slot: usize, key: &[u8]) -> Option<usize> {
    PROGRESS.with(|p| match p.borrow().as_ref() {
        None => Some(slot),
        Some(progress) => {
            let logical = progress.logical_of(slot)?;
            progress
                .is_pending(logical, crate::storage::dashtable::hash_key(key))
                .then_some(logical)
        }
    })
}

/// Already captured this epoch — the FIRST pre-image is the epoch-start
/// state; a later one would be a state the snapshot must not contain.
fn captured(db_index: usize, key: &[u8]) -> bool {
    PENDING_KEYS.with(|k| {
        k.borrow()
            .get(db_index)
            .is_some_and(|set| set.contains(key))
    })
}

/// Queue `entry` as `key`'s pre-image and enter it in the epoch dedupe set.
fn record_entry(db_index: usize, owned: Bytes, entry: Entry) {
    PENDING_KEYS.with(|k| {
        let mut sets = k.borrow_mut();
        if sets.len() <= db_index {
            sets.resize_with(db_index + 1, HashSet::new);
        }
        if sets[db_index].insert(owned.clone()) {
            // The key's bytes plus a hash-set slot and its `Bytes` header.
            DEDUPE_BYTES.with(|b| b.set(b.get() + owned.len() as u64 + 48));
        }
    });
    let pre_image: PreImage = Some(entry);
    PENDING.with(|p| p.borrow_mut().push((db_index, owned, pre_image)));
}

/// Out-of-line slow path: record the key's current state, first write wins.
///
/// `slot` is the shard slot the write runs in (`databases[slot]`); the
/// pre-image is filed under the database of the EPOCH whose table is in that
/// slot now (moon#1228 — a SWAPDB moves tables between slots, a flush
/// detaches one).
pub(super) fn capture_key(db: &Database, slot: usize, key: &[u8]) {
    let Some(db_index) = target(slot, key) else {
        return;
    };
    if captured(db_index, key) {
        return;
    }
    // A key that does not exist yet is captured too, as a TOMBSTONE
    // (moon#1216): its epoch-start state is "absent", and without the
    // tombstone the serializer would write the entry this write is about to
    // create. The key is copied so the capture never pins the connection's
    // read buffer (`key` is usually a slice of it) for the rest of the epoch.
    let owned = Bytes::copy_from_slice(key);
    let Some(entry) = db.data().get(key) else {
        // Absent: a tombstone, NOT entered in the epoch dedupe set. Under an
        // insert flood that set was the larger half of a tombstone's cost,
        // and exactness does not need it: the queue is FIFO and
        // `SnapshotState::capture_cow` keeps the FIRST capture of a key, so a
        // later write of the now-present key (which does enter the set, once)
        // can only queue a copy the drain discards.
        PENDING.with(|p| p.borrow_mut().push((db_index, owned, None)));
        return;
    };
    #[cfg(test)]
    CLONES.with(|c| c.set(c.get() + 1));
    record_entry(db_index, owned, entry.clone());
}

/// What became of an entry handed to [`capture_removed`].
#[derive(Debug)]
pub(crate) enum Removed {
    /// The snapshot keeps it as the key's epoch-start state; the caller must
    /// not free it (the walk releases it once written).
    Held,
    /// Not needed by the snapshot: the caller disposes of it as usual.
    Dispose(Entry),
}

/// An entry just REMOVED from shard slot `slot`'s table (an eviction victim,
/// moon#1257): if the armed epoch still needs `key`'s epoch-start state and
/// has none yet, the entry itself becomes the pre-image — moved, not
/// deep-cloned (review F1: a 1M-field victim used to be cloned on the shard
/// thread, 198–255 ms, and held twice while the original was lazily freed).
///
/// The removed entry IS the key's state right before the removal, so as a
/// first capture it is exactly what [`capture_key`] would have copied. No
/// save running, a range already written, or a key already captured:
/// [`Removed::Dispose`], and the caller frees it as it always did. Disarmed,
/// this is one thread-local `bool` load.
#[inline]
pub(crate) fn capture_removed(slot: usize, key: &[u8], entry: Entry) -> Removed {
    if !is_armed() {
        return Removed::Dispose(entry);
    }
    capture_removed_slow(slot, key, entry)
}

fn capture_removed_slow(slot: usize, key: &[u8], entry: Entry) -> Removed {
    let Some(db_index) = target(slot, key) else {
        return Removed::Dispose(entry);
    };
    if captured(db_index, key) {
        return Removed::Dispose(entry);
    }
    record_entry(db_index, Bytes::copy_from_slice(key), entry);
    Removed::Held
}
