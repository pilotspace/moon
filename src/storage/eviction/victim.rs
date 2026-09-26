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
//! captures, then removes. Disarmed (no save running) the capture is one
//! thread-local `bool` load.

use crate::storage::Database;
use crate::storage::entry::Entry;

/// Remove eviction victim `key` from `db`, capturing its pre-image for an
/// armed snapshot epoch first (moon#1257).
///
/// The capture is filed under `db.db_index`, which is the shard slot `db`
/// lives in: the shard stamps it when it builds its databases
/// (`shard::Shard`), `swap_contents` keeps each slot's index across a SWAPDB
/// (#1242), and `rdb::load*` carries it into a replacement table. That is
/// exactly the slot [`crate::persistence::snapshot_cow::capture_write_pre_image`]
/// asks for, so no caller has to thread an index through `EvictionRun` —
/// and none can pass the wrong one.
#[inline]
pub(super) fn remove(db: &mut Database, key: &[u8]) -> Option<Entry> {
    crate::persistence::snapshot_cow::capture_write_pre_image(db, db.db_index, key);
    db.remove(key)
}
