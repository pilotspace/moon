//! Taking an eviction victim out of the hot table (moon#1257).
//!
//! moon's BGSAVE is fork-less copy-on-write: every writer that changes a key
//! the armed snapshot epoch has not written yet must capture the key's
//! pre-image first (moon#1228). Eviction is such a writer — a plain drop, a
//! drop of a victim about to expire, and a spill (sync batch or async) all
//! take the key out of the hot table the snapshot walks. With no capture, a
//! key evicted before the walk reached its range was simply missing from the
//! file, which was then not the point-in-time image a forked redis save
//! would hold; and an async spill whose write then failed re-inserted the
//! key after the walk had passed it, so the file never held it at all.
//!
//! [`remove`] is the ONE way the eviction paths take a victim out: it
//! removes, then hands the removed entry to the armed epoch as the key's
//! pre-image — MOVED, never deep-cloned (review F1). Disarmed (no save
//! running) that hand-over is one thread-local `bool` load.

pub(super) use crate::persistence::snapshot_cow::Removed;
use crate::persistence::snapshot_cow::capture_removed;
use crate::storage::Database;

/// Remove eviction victim `key` from `db`. `None`: nothing was hot. Otherwise
/// [`Removed::Held`] when the armed epoch keeps the removed entry as the
/// key's epoch-start state (the caller must not free it; the walk releases
/// it, off the shard thread when large), or [`Removed::Dispose`] with the
/// entry for the caller to free as usual.
///
/// The removed entry IS the key's state right before the eviction, so as the
/// epoch's first capture of the key it is exactly the pre-image a copy would
/// have taken — without the copy: a 1M-field hash victim cost 198–255 ms of
/// shard-thread cloning and was resident twice (the clone, and the original
/// on the lazy-free queue) before review F1.
///
/// The capture is filed under `db.db_index`, which is the shard slot `db`
/// lives in: the shard stamps it when it builds its databases
/// (`shard::Shard`), `swap_contents` keeps each slot's index across a SWAPDB
/// (#1242), and `rdb::load*` carries it into a replacement table — exactly
/// the slot the epoch's capture asks for, so no caller threads an index
/// through `EvictionRun` and none can pass a wrong one.
#[inline]
pub(super) fn remove(db: &mut Database, key: &[u8]) -> Option<Removed> {
    let entry = db.remove(key)?;
    Some(capture_removed(db.db_index, key, entry))
}
