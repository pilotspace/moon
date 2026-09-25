//! Deferred copy-on-write pre-image capture for writes that happen OFF the
//! shard event loop's own stack (moon#517).
//!
//! # Why this exists
//!
//! The incremental snapshot ([`crate::persistence::snapshot::SnapshotState`])
//! is a fuzzy, segment-at-a-time serializer: the shard event loop writes one
//! segment per tick while the shard keeps serving traffic. Correctness comes
//! from COW — a key overwritten BEFORE its segment has been serialized must
//! have its epoch-start value stashed in the snapshot's overflow buffer, or
//! recovery double-applies the WAL record that replays on top of the
//! already-new value (`INCR` is the classic case: snapshot holds 2, WAL
//! replays the INCR that produced it, key ends at 3).
//!
//! `spsc_handler::cow_intercept` does that capture inline — but it can only
//! be called where the `&mut Option<SnapshotState>` is in scope, i.e. from
//! the shard event loop's own stack. A **Lua script** runs on a connection
//! task (or inside the routed `ShardMessage::Execute` arm) and issues its
//! writes from `scripting::bridge`, which has no access to that state.
//! Worse, `EVAL <script> <n> k` names its keys by position only — which of
//! them the script WRITES is known only per `redis.call` — so wrapping the
//! `handle_eval` call sites in `cow_intercept` could not capture the right
//! keys (it used to key off `command[1]`, the script body).
//!
//! # Design: capture eagerly, filter at the drain
//!
//! The pre-image must be taken at mutation time (only then is the old value
//! still there), but the "is this key still pending?" question can be
//! answered later — because ONLY the event loop's per-tick
//! `advance_snapshot_segment` ever moves the snapshot's hash-space cursor
//! (moon#1216: pending ⇔ `hash(key) >= cursor` in the current database, a
//! question no DashTable split can change the answer to). So:
//!
//! 1. A snapshot starting on this shard [`arm`]s a thread-local queue.
//! 2. Every write path — `command::dispatch`, the routed arms'
//!    `cow_intercept`, the monoio inline SET, a script's `redis.call`, the
//!    blocking waker, a blocking command served on the spot by its
//!    connection (`conn::blocking::immediate_serve`, and in MULTI
//!    `blocking_txn::try_exec_blocking_in_txn`), the `MOVE` / `COPY … DB n`
//!    cores on both databases ([`capture_two_db`], moon#1228) — captures
//!    before it mutates, for EVERY key it may write (moon#1217). Armed: the old entry, or a tombstone when the key
//!    does not exist yet (first capture of a key wins — a later one would
//!    be a post-epoch state). Disarmed (the overwhelmingly common case, no
//!    BGSAVE in flight): one thread-local `bool` load and return.
//! 3. The next tick [`drain_into`]s the queue into the live `SnapshotState`
//!    **before** advancing: first any abort a FLUSHALL or a replica full
//!    resync queued (moon#1224), then the pre-images, dropping those whose
//!    range was already written (the file already holds their epoch-start
//!    bytes), then the table changes queued since (moon#1228: a SWAPDB
//!    re-points the epoch's databases, a FLUSHDB freezes the detached
//!    table into it — trimmed to its epoch-start rows, which needs every
//!    pre-image of its database in the state first).
//!
//! One shard per OS thread, and every producer/consumer here runs on that
//! shard's thread, so a `thread_local!` IS the per-shard queue — same
//! pattern as [`crate::shard::slice`] and [`crate::shard::self_msg`]. A
//! non-shard thread is never armed, so it can never accumulate a queue that
//! nobody drains.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;

use bytes::Bytes;

use crate::persistence::snapshot::{PreImage, SnapshotState, Table};
use crate::protocol::Frame;
use crate::storage::db::Database;
#[cfg(test)]
use crate::storage::entry::Entry;

thread_local! {
    /// Is a snapshot in flight on this shard? The whole capture path is one
    /// `Cell<bool>` load when it is not.
    static ARMED: Cell<bool> = const { Cell::new(false) };
    /// Pre-images captured since the last drain: `(db_index, key, state)`,
    /// where `state` is the key's entry or `None` if it did not exist
    /// (moon#1216: absence is part of the epoch-start keyspace too).
    static PENDING: RefCell<Vec<(usize, Bytes, PreImage)>> = const { RefCell::new(Vec::new()) };
    /// First-wins dedupe set: every key whose pre-image was captured this
    /// EPOCH, one set per database. Held for the whole snapshot (moon#1186)
    /// — it used to be cleared on every drain, so a hot key was deep-cloned
    /// again on every tick only for `SnapshotState::capture_cow` to discard
    /// the copy. Per database so a lookup borrows the key (`&[u8]`) instead
    /// of building an owned `(db, Bytes)` probe.
    static PENDING_KEYS: RefCell<Vec<HashSet<Bytes>>> = const { RefCell::new(Vec::new()) };
    /// Serialization progress of the armed snapshot (moon#1186): lets
    /// `capture_key` skip keys whose range is already written, whose
    /// pre-image the drain would drop anyway. `None` = unknown (capture
    /// everything, the pre-moon#1186 behaviour).
    static PROGRESS: RefCell<Option<Progress>> = const { RefCell::new(None) };
    /// A whole-table change the armed epoch does not follow (moon#1224): a
    /// FLUSHALL or a replica full resync while the epoch has databases left
    /// to write (a FLUSHDB or SWAPDB no longer aborts, moon#1228). The next
    /// drain fails the snapshot with this reason; a table `Database::clear`
    /// hands over meanwhile is dropped at once, never frozen.
    static ABORT: Cell<Option<&'static str>> = const { Cell::new(None) };
    /// Table changes the epoch follows (moon#1228), in the order they
    /// happened, applied to the `SnapshotState` by the next drain.
    static EVENTS: RefCell<Vec<TableEvent>> = const { RefCell::new(Vec::new()) };
    /// Post-epoch bytes of the flushed tables in `EVENTS`, waiting whole for
    /// the drain that trims them (moon#1228 review 5): each table's
    /// `used_memory` at its flush above its database's epoch-start bill.
    static WAITING_EXCESS: Cell<u64> = const { Cell::new(0) };
    /// Post-epoch bytes the epoch's frozen tables still held after the last
    /// drain, their trims unfinished (review 6, S2: the trim is budgeted, so
    /// a grown table holds its post-epoch rows for ceil(work / budget)
    /// drains, not one).
    static UNTRIMMED_EXCESS: Cell<u64> = const { Cell::new(0) };
    /// Bumped by every [`arm`]: a lazily freed value crediting an earlier
    /// epoch's frozen table cannot credit this one's (review 7).
    static ARM_SERIAL: Cell<u64> = const { Cell::new(0) };
    /// `(epoch database, bytes)` the lazy-free drain owes frozen tables'
    /// bills (review 7), applied by the next [`drain_into`].
    static FROZEN_CREDITS: RefCell<Vec<(usize, u64)>> = const { RefCell::new(Vec::new()) };
    /// Approximate bytes of `PENDING_KEYS` (moon#1228, INFO
    /// `current_cow_size`).
    static DEDUPE_BYTES: Cell<u64> = const { Cell::new(0) };
    /// This shard's share of [`CURRENT_COW_SIZE`] as last published.
    static PUBLISHED_COW: Cell<u64> = const { Cell::new(0) };
}

/// Bytes every shard's armed snapshot holds only for the snapshot's sake,
/// summed: queued pre-images (entries and tombstones), tables a flush handed
/// over (moon#1228), and the first-wins key sets. INFO persistence reports it
/// as `current_cow_size` — redis's name for the copy-on-write memory its
/// forked child pins.
///
/// Deliberately NOT part of `used_memory`: that figure drives `maxmemory`
/// eviction and refusal, and a save's transient copies must not evict or
/// refuse user writes because a BGSAVE happens to be running (redis does not
/// count fork COW in `used_memory` either). The copies vanish as the walk
/// passes their range, and the whole figure returns to 0 when the save ends.
static CURRENT_COW_SIZE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// See [`CURRENT_COW_SIZE`]. Published by each shard at every drain (one
/// tick stale at most).
pub(crate) fn current_cow_size() -> u64 {
    CURRENT_COW_SIZE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Publish this shard's COW bytes: the delta against what it last published.
fn publish_cow_size(bytes: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    let before = PUBLISHED_COW.with(|p| p.replace(bytes));
    if bytes > before {
        CURRENT_COW_SIZE.fetch_add(bytes - before, Relaxed);
    } else if before > bytes {
        CURRENT_COW_SIZE.fetch_sub(before - bytes, Relaxed);
    }
}

/// A whole-table change the armed epoch follows instead of aborting
/// (moon#1228).
enum TableEvent {
    /// `SWAPDB a b` exchanged shard slots `a` and `b`.
    Swap(usize, usize),
    /// A flush detached this epoch database's table (holding the given
    /// bytes; the last field is the database's epoch-start bill) before it
    /// was written.
    Freeze(usize, Box<Table>, u64, u64),
}

/// Mirror of `SnapshotState`'s serialization cursor, so a capture can
/// answer `is_hash_pending` without the state in scope.
struct Progress {
    current_db: usize,
    /// Hash-space position within `current_db` (moon#1216).
    cursor: u64,
    num_databases: usize,
    /// Which database of the epoch each shard slot's CURRENT table belongs
    /// to (moon#1228): the identity at arm, exchanged by a SWAPDB, and
    /// `None` once a flush detached the slot's epoch table (the slot's new
    /// contents are post-epoch — nothing written to it needs a pre-image).
    logical_of_slot: Vec<Option<usize>>,
    /// Each shard slot's `Database` address, recorded at arm: how
    /// [`note_cleared_table`] tells which slot `Database::clear` ran on (a
    /// slot's database lives at a fixed address in the shard's `ShardDbSet`;
    /// a SWAPDB exchanges contents, not addresses). Empty when the arming
    /// thread has no shard slice.
    slot_addrs: Vec<usize>,
    /// Each epoch database's `used_memory` at arm, its epoch-start bill
    /// (moon#1228 review 5): what a flushed table holds beyond it is
    /// post-epoch. Indexed like the file's databases; empty with
    /// `slot_addrs`.
    start_bills: Vec<u64>,
}

impl Progress {
    /// The database of the epoch whose epoch-start table is in `slot`.
    fn logical_of(&self, slot: usize) -> Option<usize> {
        self.logical_of_slot.get(slot).copied().flatten()
    }

    /// Exactly `SnapshotState::is_hash_pending`. A mirror that lags the
    /// state (a cursor published late) only answers "pending" for more keys,
    /// which costs a clone the drain then drops — never a missed pre-image.
    fn is_pending(&self, db_index: usize, hash: u64) -> bool {
        db_index < self.num_databases
            && (db_index > self.current_db || (db_index == self.current_db && hash >= self.cursor))
    }

    /// Is any of `db_index`'s epoch-start contents still to be written?
    fn is_unfinished(&self, db_index: usize) -> bool {
        db_index < self.num_databases && db_index >= self.current_db
    }
}

/// Arm capture for a snapshot that just began on this shard.
pub(crate) fn arm() {
    clear();
    ARM_SERIAL.with(|s| s.set(s.get().wrapping_add(1)));
    ARMED.with(|a| a.set(true));
}

/// Where a lazily freed value's credits go once a FLUSHDB froze its table
/// (review 7): that table's bill, which took the ledger as its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FrozenCharge {
    serial: u64,
    db: usize,
}

/// The lazy-free drain freed `bytes` charged to a frozen table's bill:
/// credited at the next drain. Dropped once its epoch is not armed.
pub(crate) fn credit_frozen(frozen: FrozenCharge, bytes: u64) {
    if !is_armed() || ARM_SERIAL.with(Cell::get) != frozen.serial {
        return;
    }
    FROZEN_CREDITS.with(|c| {
        let mut credits = c.borrow_mut();
        match credits.last_mut() {
            Some((db, sum)) if *db == frozen.db => *sum = sum.saturating_add(bytes),
            _ => credits.push((frozen.db, bytes)),
        }
    });
}

/// [`arm`] with the snapshot's epoch-start layout, so captures for keys
/// whose range is already written are skipped at the source (moon#1186).
/// Only the number of databases matters since moon#1216: progress is a
/// position in hash space that starts at database 0, hash 0.
///
/// Also records each shard slot's `Database` address from this thread's
/// shard slice (moon#1228, see [`note_cleared_table`]). Must be called
/// outside any `with_shard` borrow — both arming sites in
/// `shard::persistence_tick` are.
pub(crate) fn arm_with_layout(segment_counts: Vec<usize>) {
    let slots = crate::shard::slice::try_with_shard(|s| {
        s.databases.with_all_read(|dbs| {
            dbs.iter()
                .map(|db| (db_addr(db), db.ledger_bytes() as u64))
                .collect::<Vec<_>>()
        })
    })
    .unwrap_or_default();
    arm_with_slots(segment_counts.len(), slots);
}

/// [`arm_with_layout`] over an explicit database slice (`dbs[i]` is shard
/// slot `i`), for harnesses that drive a `SnapshotState` over a plain
/// `Vec<Database>` instead of a shard slice.
#[cfg(test)]
pub(crate) fn arm_with_databases(dbs: &[Database]) {
    arm_with_slots(
        dbs.len(),
        dbs.iter()
            .map(|db| (db_addr(db), db.ledger_bytes() as u64))
            .collect(),
    );
}

/// `slots[i]`: shard slot `i`'s `Database` address and `used_memory` now,
/// at the epoch's start.
fn arm_with_slots(num_databases: usize, slots: Vec<(usize, u64)>) {
    arm();
    let (slot_addrs, start_bills) = slots.into_iter().unzip();
    PROGRESS.with(|p| {
        *p.borrow_mut() = Some(Progress {
            current_db: 0,
            cursor: 0,
            num_databases,
            logical_of_slot: (0..num_databases).map(Some).collect(),
            slot_addrs,
            start_bills,
        })
    });
}

/// A database's identity as a shard slot: its address.
#[inline]
fn db_addr(db: &Database) -> usize {
    db as *const Database as usize
}

/// Publish the snapshot's cursor after a segment advance (moon#1186). Must
/// be called AFTER the tick's [`drain_into`] and advance, never between
/// them: a pre-image is filtered against the cursor it was captured under.
pub(crate) fn note_progress(current_db: usize, cursor: u64) {
    PROGRESS.with(|p| {
        if let Some(progress) = p.borrow_mut().as_mut() {
            progress.current_db = current_db;
            progress.cursor = cursor;
        }
    });
}

/// Disarm and drop anything still queued (the snapshot finished or failed —
/// the file is closed, so a pre-image has nowhere left to go).
pub(crate) fn disarm() {
    ARMED.with(|a| a.set(false));
    clear();
}

/// True while a snapshot on this shard wants pre-images.
#[inline]
pub(crate) fn is_armed() -> bool {
    ARMED.with(|a| a.get())
}

/// Test-only view of the queue, for suites that exercise a write path end
/// to end (e.g. a Lua script) and need to assert the pre-image was taken
/// without standing up a whole shard event loop to drain it. Lists the
/// captured ENTRIES only; see [`pending_tombstones_for_test`] for keys
/// captured as absent.
#[cfg(test)]
pub(crate) fn pending_for_test() -> Vec<(usize, Bytes, Entry)> {
    PENDING.with(|p| {
        p.borrow()
            .iter()
            .filter_map(|(db, k, e)| e.as_ref().map(|e| (*db, k.clone(), e.clone())))
            .collect()
    })
}

/// Test-only: keys captured as ABSENT at epoch start (tombstones).
#[cfg(test)]
pub(crate) fn pending_tombstones_for_test() -> Vec<(usize, Bytes)> {
    PENDING.with(|p| {
        p.borrow()
            .iter()
            .filter(|(_, _, e)| e.is_none())
            .map(|(db, k, _)| (*db, k.clone()))
            .collect()
    })
}

/// Test-only [`drain_into`] without a shard slice: what the persistence
/// tick does before every advance.
#[cfg(test)]
pub(crate) fn drain_pending_for_test(snap: &mut SnapshotState) {
    drain_into(snap);
}

/// Test-only: this thread's share of `current_cow_size` as last published
/// (the process-wide sum mixes in parallel tests' threads).
#[cfg(test)]
pub(crate) fn published_cow_size_for_test() -> u64 {
    PUBLISHED_COW.with(Cell::get)
}

/// Test-only: the epoch database each shard slot maps to (moon#1228).
#[cfg(test)]
pub(crate) fn logical_of_slot_for_test() -> Vec<Option<usize>> {
    PROGRESS.with(|p| {
        p.borrow()
            .as_ref()
            .map(|p| p.logical_of_slot.clone())
            .unwrap_or_default()
    })
}

/// Test-only: how many flushed tables are queued to be frozen into the
/// epoch at the next drain (moon#1228).
#[cfg(test)]
pub(crate) fn frozen_tables_queued_for_test() -> usize {
    EVENTS.with(|e| {
        e.borrow()
            .iter()
            .filter(|ev| matches!(ev, TableEvent::Freeze(..)))
            .count()
    })
}

/// Test-only: the abort a structural change queued for the next drain.
#[cfg(test)]
pub(crate) fn abort_pending_for_test() -> Option<&'static str> {
    ABORT.with(Cell::get)
}

fn clear() {
    PENDING.with(|p| p.borrow_mut().clear());
    PENDING_KEYS.with(|k| k.borrow_mut().clear());
    DEDUPE_BYTES.with(|b| b.set(0));
    publish_cow_size(0);
    PROGRESS.with(|p| *p.borrow_mut() = None);
    ABORT.with(|a| a.set(None));
    EVENTS.with(|e| e.borrow_mut().clear());
    WAITING_EXCESS.with(|w| w.set(0));
    UNTRIMMED_EXCESS.with(|u| u.set(0));
    FROZEN_CREDITS.with(|c| c.borrow_mut().clear());
}

/// Would the armed epoch still write anything? Without a published layout
/// the answer is a conservative yes.
fn epoch_unfinished() -> bool {
    PROGRESS.with(|p| match p.borrow().as_ref() {
        None => true,
        Some(progress) => progress.current_db < progress.num_databases,
    })
}

/// Queue the failure of the armed epoch (moon#1224): the next drain aborts
/// it, before another segment is written. Flushed tables still waiting to
/// be frozen are released now — nothing will be written from them.
fn abort_epoch(why: &'static str) {
    ABORT.with(|a| {
        if a.get().is_none() {
            a.set(Some(why));
        }
    });
    EVENTS.with(|e| {
        e.borrow_mut()
            .retain(|event| !matches!(event, TableEvent::Freeze(..)))
    });
    WAITING_EXCESS.with(|w| w.set(0));
    UNTRIMMED_EXCESS.with(|u| u.set(0));
}

/// `SWAPDB a b` is about to exchange the tables of shard slots `a` and `b`
/// (moon#1224, moon#1228). Called from `ShardDbSet::swap`, the one place
/// every SWAPDB path — the coordinator's local leg, the SPSC arm, replica
/// apply — exchanges them.
///
/// The epoch follows the tables instead of aborting (it used to fail the
/// BGSAVE whenever `a` or `b` was unfinished): from now on a write to slot
/// `a` is a write to the epoch database whose table moved there, and the
/// serializer reads that database from slot `a` (applied to the state at the
/// next drain, before another segment is written). The logged SWAPDB
/// replays on top of the file. One thread-local `bool` load when nothing is
/// armed.
pub(crate) fn note_swapdb(a: usize, b: usize) {
    if !is_armed() || a == b {
        return;
    }
    let followed = PROGRESS.with(|p| {
        let mut p = p.borrow_mut();
        let Some(progress) = p.as_mut() else {
            return false;
        };
        let n = progress.logical_of_slot.len();
        if a >= n || b >= n {
            return false;
        }
        progress.logical_of_slot.swap(a, b);
        true
    });
    if followed {
        EVENTS.with(|e| e.borrow_mut().push(TableEvent::Swap(a, b)));
    } else if epoch_unfinished() {
        // No slot map (an epoch armed without a layout): the pre-moon#1228
        // answer.
        abort_epoch("SWAPDB exchanged a database the snapshot had not finished writing");
    }
}

/// `Database::clear` just detached `db`'s table (moon#1228): FLUSHDB (from
/// a client, MULTI/EXEC, a script, a routed or replicated command), the
/// clears of a FLUSHALL or of a replica full resync. `table` is the old
/// table, which `clear` used to drop, and `table_bytes` what the database's
/// ledger billed for it at the flush (`used_memory`, without the
/// spill-in-flight bytes the table does not hold; it still counts a value a
/// lazy free has not finished releasing, which is resident until the tick
/// frees it). Reported in INFO `current_cow_size` while the epoch keeps it.
///
/// If the table is the epoch-start table of a database the armed epoch has
/// not finished writing, it is FROZEN into the epoch (moved, never cloned):
/// with the pre-images captured before the flush it holds that database's
/// epoch-start contents, so the BGSAVE completes with the pre-flush image —
/// redis's fork does the same for FLUSHDB — where it used to abort. The
/// slot's new contents are post-epoch, so writes to it capture nothing from
/// here on. Otherwise the table is dropped here, where `clear` dropped it: a
/// database already written, a slot whose epoch table an earlier flush
/// already froze, or an epoch with an abort queued (a FLUSHALL or a resync —
/// the tables its clears hand over are released now, not at the next drain).
///
/// Memory bound (reviews 5, 6): a table arrives AS FLUSHED, with every row
/// written since the epoch began. The next drain freezes it and starts
/// trimming it to its epoch-start rows (`snapshot::frozen`), budgeted at
/// `TRIM_BUDGET` row operations per drain and done after
/// ceil(work / budget) drains — work being the keys written since the epoch
/// began, the rows below the cursor, and the rows kept if the table is
/// rebuilt. At most ONE table per database of the epoch is frozen (a flushed
/// slot is unmapped, so flushing it again drops the new table); each is
/// released as soon as the walk finishes its database. Once its trim is done
/// the epoch therefore holds at most the epoch-start bills of the databases
/// it has not written — however much a client inserts and flushes — the same
/// worst case as redis, whose forked child keeps every pre-flush page on
/// FLUSHDB and is killed only by FLUSHALL (pinned by
/// `table_swap_tests::flushdb_of_every_database_holds_at_most_the_epoch_start_tables`
/// and `prop_tests`). Until then the table holds its post-epoch rows too.
///
/// When a flush fails the save instead (review 6, S2, re-derived for the
/// budgeted trim): a table holds post-epoch rows from its flush until its
/// trim's steps 1-2 are done — the queue until the next drain, then ceil(work
/// / budget) drains. One GROWN table (bill above its database's epoch-start
/// bill) may do so. A further flush of a grown database while another grown
/// table is still waiting or trimming fails the save once their post-epoch
/// bytes together pass [`FREEZE_WAIT_SLACK`] — whether the flushes come in
/// one tick (a pipeline, MULTI, a script, two clients) or spread over the
/// trim. A flush of a database that did not grow never does (it used to,
/// whenever a grown table was already waiting). So the epoch holds at most
/// one grown table's post-epoch rows, or the slack, beyond the epoch-start
/// bills — memory that `used_memory` carried a moment before the flush.
/// The trim clears ~512 rows a tick, faster than a client refills a database
/// (8 x 40 MB of SETs + FLUSHDB completes in a release build).
///
/// Returns the frozen table's charge when the table was queued to freeze:
/// `table_bytes` includes the remaining charges of values the database's
/// lazy-free queue has not finished freeing (moon#1190), so `clear` points
/// those values' credits at the frozen bill (review 7) instead of freeing
/// them inside the FLUSHDB. `None` otherwise.
///
/// A slot that cannot be identified (an epoch armed on a thread with no
/// shard slice) keeps the old answer: an unfinished epoch is aborted. One
/// thread-local `bool` load when nothing is armed.
pub(crate) fn note_cleared_table(
    db: &Database,
    table: Table,
    table_bytes: u64,
) -> Option<FrozenCharge> {
    if !is_armed() {
        return None;
    }
    if ABORT.with(|a| a.get().is_some()) {
        // The epoch is being failed: nothing will be written from this
        // table. `table` drops here.
        return None;
    }
    enum Outcome {
        /// The epoch database, and its epoch-start bill.
        Freeze(usize, u64),
        Drop,
        Unknown,
    }
    let addr = db_addr(db);
    let outcome = PROGRESS.with(|p| {
        let mut p = p.borrow_mut();
        let Some(progress) = p.as_mut() else {
            return Outcome::Unknown;
        };
        let Some(slot) = progress.slot_addrs.iter().position(|a| *a == addr) else {
            return Outcome::Unknown;
        };
        match progress.logical_of(slot) {
            Some(logical) if progress.is_unfinished(logical) => {
                progress.logical_of_slot[slot] = None;
                let start_bill = progress.start_bills.get(logical).copied().unwrap_or(0);
                Outcome::Freeze(logical, start_bill)
            }
            _ => Outcome::Drop,
        }
    });
    match outcome {
        Outcome::Freeze(logical, start_bill) => {
            // Review 6 (S2): only a table that GREW adds to what waits — a
            // flush of an un-grown database (excess 0) never fails the save
            // (it used to, whenever a grown one was already waiting: a
            // plain `SELECT 1; FLUSHDB; SELECT 2; FLUSHDB` pipeline). What
            // waits is what the queue holds since the last drain plus what
            // the frozen tables' unfinished trims still hold.
            let excess = table_bytes.saturating_sub(start_bill);
            let waiting = WAITING_EXCESS
                .with(Cell::get)
                .saturating_add(UNTRIMMED_EXCESS.with(Cell::get));
            if excess > 0 && waiting > 0 && waiting.saturating_add(excess) > FREEZE_WAIT_SLACK {
                // `table` drops here, and the waiting ones with the abort.
                abort_epoch(
                    "FLUSHDB detached databases that grew during the save faster than the \
                     snapshot could trim them",
                );
                return None;
            }
            WAITING_EXCESS.with(|w| w.set(w.get().saturating_add(excess)));
            EVENTS.with(|e| {
                e.borrow_mut().push(TableEvent::Freeze(
                    logical,
                    Box::new(table),
                    table_bytes,
                    start_bill,
                ))
            });
            Some(FrozenCharge {
                serial: ARM_SERIAL.with(Cell::get),
                db: logical,
            })
        }
        Outcome::Drop => None,
        Outcome::Unknown => {
            if epoch_unfinished() && !table.is_empty() {
                abort_epoch("a flush cleared a database the snapshot could not identify");
            }
            None
        }
    }
}

/// Post-epoch bytes (above their databases' epoch-start bills) that flushed
/// tables may hold while more than one grown table waits for, or is in, its
/// trim (see [`note_cleared_table`]). One grown table is always allowed:
/// its rows were in `used_memory` an instant before the flush.
const FREEZE_WAIT_SLACK: u64 = 8 << 20;

/// Every database of this shard is about to be REPLACED wholesale outside
/// `command::dispatch` (moon#1227 review F6): a replica full resync
/// (`replication::apply::load_snapshot`) clears each table and loads the
/// master's RDB in its place. Unlike a FLUSH (moon#1228), the resync is not
/// a record of this node's own log: a pre-resync image with the post-resync
/// tail replayed on top would mix this node's data with the master's. So an
/// unfinished epoch is aborted — the BGSAVE fails loudly and the previous
/// file stays; the tables the resync's `clear` calls hand over are dropped
/// at once ([`note_cleared_table`]). One thread-local `bool` load when
/// nothing is armed.
pub(crate) fn note_table_replace(why: &'static str) {
    if is_armed() && epoch_unfinished() {
        abort_epoch(why);
    }
}

/// A FLUSHALL is about to clear every database of this shard while an epoch
/// is armed. Redis parity: `flushAllDataAndResetRDB` kills an in-flight
/// RDB child (`killRDBChild`), so a FLUSHALL fails the BGSAVE — here the
/// epoch is aborted when it still has databases to write, the previous file
/// stays, and the tables the FLUSHALL's clears hand over are dropped at once
/// instead of being held until the walk passes them. A FLUSHDB (or SWAPDB)
/// does not abort: redis keeps its child for those, and so does the epoch
/// (moon#1228, [`note_cleared_table`]). Nothing happens when every database
/// is already written (the logged FLUSHALL replays on top of the file), or
/// when the command will refuse its arguments and flush nothing.
fn note_flushall(args: &[Frame]) {
    if flush_args_accepted(args) && epoch_unfinished() {
        abort_epoch("FLUSHALL cleared databases the snapshot had not finished writing");
    }
}

/// Exactly `command::server_admin`'s FLUSHDB/FLUSHALL argument check: no
/// argument, or one of `ASYNC` / `SYNC`. Anything else is refused before
/// `Database::clear` runs (pinned by `table_swap_tests::a_refused_flush_does_not_abort`).
fn flush_args_accepted(args: &[Frame]) -> bool {
    match args {
        [] => true,
        [only] => crate::command::helpers::extract_bytes(only)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"ASYNC") || s.eq_ignore_ascii_case(b"SYNC")),
        _ => false,
    }
}

/// Capture the pre-images of the keys a script's `redis.call(cmd, args..)`
/// is about to write.
///
/// `cmd_and_args[0]` is the command name. Every WRITTEN key position is
/// captured (moon#1217), through the same walker as
/// [`capture_dispatch_pre_image`] — a script's writes get exactly the
/// fidelity the generic path has.
///
/// Costs one thread-local `bool` load when no snapshot is in flight.
#[inline]
pub(crate) fn capture_command_pre_image(db: &Database, db_index: usize, cmd_and_args: &[Frame]) {
    if !is_armed() {
        return;
    }
    let Some((Frame::BulkString(cmd), args)) = cmd_and_args.split_first() else {
        return;
    };
    if !crate::command::metadata::is_write(cmd) {
        return;
    }
    capture_written_keys(db, db_index, cmd, args);
}

/// Capture the pre-image for a generic command about to run against `db`
/// (moon#558).
///
/// This is the choke point for every write that executes on the shard's own
/// stack instead of the event loop's: [`crate::command::dispatch`] is what
/// the monoio local arm, the tokio sharded local arm, `handler_single`, both
/// MULTI/EXEC executors, the coordinator's scatter arms and the SPSC drain
/// all funnel through. `spsc_handler::cow_intercept` (the routed arms)
/// captures through this same function, so every capture of an epoch lands
/// in ONE queue with ONE first-wins dedupe set, in the order the writes ran.
///
/// Cost when no snapshot is in flight — the overwhelmingly common case — is
/// one thread-local `bool` load; the `is_write` PHF lookup and the key
/// extraction are behind that gate.
///
/// Invariant: `db` MUST be `databases[db_index]` on the shard that armed the
/// capture — the drain files the pre-image under `db_index`, so a mismatched
/// pair would file it against the wrong database. Every live caller
/// satisfies it (`dispatch` is always handed `databases[*selected_db]`).
/// The one structural exception, `conn::shared::execute_transaction`, holds
/// a lock on the ENTRY db while `*selected_db` can be moved by a `SELECT`
/// queued inside the same MULTI — that executor belongs to `handler_single`,
/// which is not wired into the shipped server and runs no shard event loop,
/// so it can never be armed.
///
/// Every key position the command may WRITE is captured (moon#1217) — the
/// destination of `LMOVE`/`SMOVE`/`RENAME`/`COPY`/`SORT ... STORE`,
/// `k2..kN` of `MSET`/`DEL`, the non-first keys of `LMPOP`/`ZMPOP` —
/// not just `command[1]`: otherwise the file mixes pre- and post-epoch
/// states of the keys one atomic command touched.
#[inline]
pub(crate) fn capture_dispatch_pre_image(
    db: &Database,
    db_index: usize,
    cmd: &[u8],
    args: &[Frame],
) {
    if !is_armed() {
        return;
    }
    // The whole-table writes have no key to capture. FLUSHDB: `clear`
    // hands the epoch the table itself (`note_cleared_table`, moon#1228).
    // FLUSHALL fails the save first, as redis kills its child — every
    // FLUSHALL on a shard (client, MULTI/EXEC, script, routed, replicated)
    // runs through `dispatch`, and its other databases are cleared right
    // after by `flush_every_database`, on the same shard.
    if cmd.eq_ignore_ascii_case(b"FLUSHDB") {
        return;
    }
    if cmd.eq_ignore_ascii_case(b"FLUSHALL") {
        note_flushall(args);
        return;
    }
    if !crate::command::metadata::is_write(cmd) {
        return;
    }
    capture_written_keys(db, db_index, cmd, args);
}

/// Capture every key position `cmd args..` may WRITE (moon#1217).
///
/// The positions come from the one key walker every consumer shares
/// (`acl::keyspec::command_key_positions`, which also drives blocking
/// wake-ups and client-tracking invalidation): its `Write` role is
/// deliberately over-inclusive where the argv cannot say (`LMPOP 2 a b`
/// writes whichever is non-empty, so both are `Write`) — for a snapshot that
/// costs at most a clone, while a missed key would be a post-epoch value in
/// the file. An argv the walker cannot enumerate falls back to the primary
/// key, the pre-moon#1217 contract.
fn capture_written_keys(db: &Database, db_index: usize, cmd: &[u8], args: &[Frame]) {
    use crate::acl::keyspec::{KeyPositions, KeyRole, command_key_positions};
    match command_key_positions(cmd, args) {
        KeyPositions::At(positions) | KeyPositions::AtPlusComputed(positions) => {
            for at in positions.iter().filter(|at| at.role == KeyRole::Write) {
                if let Some(key) = args
                    .get(at.idx)
                    .and_then(crate::command::helpers::extract_bytes)
                {
                    capture_key(db, db_index, key);
                }
            }
        }
        KeyPositions::None => {}
        KeyPositions::Unknown => {
            if let Some(key) = crate::server::conn::shared::extract_primary_key(cmd, args) {
                capture_key(db, db_index, key);
            }
        }
    }
}

/// Capture the pre-image for a write whose key is already parsed — the
/// monoio inline fast path (`server::conn::blocking::try_inline_dispatch`),
/// which frames a plain `SET` straight from the read buffer and never builds
/// a `Frame` or enters [`crate::command::dispatch`] at all (moon#558).
///
/// `cfg`-gated to match its sole caller — the inline fast path only exists
/// under the monoio runtime.
#[cfg(feature = "runtime-monoio")]
#[inline]
pub(crate) fn capture_key_pre_image(db: &Database, db_index: usize, key: &Bytes) {
    if !is_armed() {
        return;
    }
    capture_key(db, db_index, key);
}

/// Capture the pre-image of a key a blocking-command WAKE is about to
/// modify (moon#1217): the waker serves a parked `BLPOP`/`BLMOVE`/`BZPOPMIN`
/// /`XREADGROUP` by popping (and, for a move, pushing the destination)
/// straight through `Database` methods, outside `command::dispatch`, and
/// logs the pop at that moment — so a key it writes that no capture has
/// seen (a `BLMOVE` destination) would otherwise reach the file at its
/// post-wake state while the logged move replays on top.
///
/// One thread-local `bool` load when no snapshot is in flight.
#[inline]
pub(crate) fn capture_wake_pre_image(db: &Database, db_index: usize, key: &Bytes) {
    if !is_armed() {
        return;
    }
    capture_key(db, db_index, key);
}

/// Capture the pre-image of a key that a writer OUTSIDE `command::dispatch`
/// is about to create, change or delete (moon#1228): the `WS DROP` key sweep
/// ([`crate::workspace::sweep_prefix`], on the master and in replica
/// apply), the owner-side `MQ` subcommands and their TXN / replica siblings,
/// and the stream waker's group reads. Each of them reaches the keyspace
/// through `Database` methods directly, so no dispatch hook sees the key.
/// `TXN.ABORT`'s KV undo (`transaction::abort`) writes through `Database`
/// directly too, and is NOT a caller.
///
/// `db` MUST be `databases[db_index]`. One thread-local `bool` load when no
/// snapshot is in flight.
#[inline]
pub(crate) fn capture_write_pre_image(db: &Database, db_index: usize, key: &[u8]) {
    if !is_armed() {
        return;
    }
    capture_key(db, db_index, key);
}

/// Capture the pre-images a `MOVE` or `COPY … DB n` needs before it writes
/// (moon#1228). Both commands bypass `command::dispatch` — they need two
/// databases — so the two-database cores (`move_cmd::move_core` /
/// `copy_core`, which every live path, both MULTI executors, scripts, the
/// replica apply and replay end in) call this first:
///
/// - `MOVE key db`: `src_key = Some(key)` (it leaves `src`) and
///   `dst_key = key` (it lands in `dst`). Without the source capture a MOVE
///   out of a pending range drops the key from `src`'s part of the file;
///   without the destination capture a MOVE into a pending database
///   serializes the key there too — a duplicate, or with a WAL tail a
///   resurrection (the replayed MOVE finds the destination taken).
/// - `COPY src dst DB n`: `src_key = None` (the source is only read) and
///   `dst_key = dst`.
///
/// `src` / `dst` MUST be `databases[src_idx]` / `databases[dst_idx]`. One
/// thread-local `bool` load when no snapshot is in flight.
#[inline]
pub(crate) fn capture_two_db(
    src: &Database,
    src_idx: usize,
    src_key: Option<&[u8]>,
    dst: &Database,
    dst_idx: usize,
    dst_key: &[u8],
) {
    if !is_armed() {
        return;
    }
    if let Some(key) = src_key {
        capture_key(src, src_idx, key);
    }
    capture_key(dst, dst_idx, dst_key);
}

/// Out-of-line slow path: record the key's current state, first write wins.
///
/// `slot` is the shard slot the write runs in (`databases[slot]`); the
/// pre-image is filed under the database of the EPOCH whose table is in that
/// slot now (moon#1228 — a SWAPDB moves tables between slots, a flush
/// detaches one).
fn capture_key(db: &Database, slot: usize, key: &[u8]) {
    // moon#1186: a key whose range is already written needs no pre-image —
    // the file holds its epoch-start bytes and the drain would drop the copy.
    // Skip it BEFORE the deep clone. moon#1228: nor does a write to a slot
    // whose epoch table a flush detached (its contents are post-epoch).
    let target = PROGRESS.with(|p| match p.borrow().as_ref() {
        None => Some(slot),
        Some(progress) => {
            let logical = progress.logical_of(slot)?;
            progress
                .is_pending(logical, crate::storage::dashtable::hash_key(key))
                .then_some(logical)
        }
    });
    let Some(db_index) = target else {
        return;
    };
    if PENDING_KEYS.with(|k| {
        k.borrow()
            .get(db_index)
            .is_some_and(|set| set.contains(key))
    }) {
        // Already captured this epoch — the FIRST pre-image is the
        // epoch-start state; a later one would be a state the snapshot must
        // not contain.
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
    let pre_image: PreImage = Some(entry.clone());
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
    PENDING.with(|p| p.borrow_mut().push((db_index, owned, pre_image)));
}

/// Fold everything captured since the last drain into `snap`, dropping
/// pre-images whose range was already serialized.
///
/// MUST run before the tick advances another segment, otherwise a pre-image
/// captured while its range was still pending would be filtered out by a
/// cursor that moved past it in the meantime.
pub(crate) fn drain_into(snap: &mut SnapshotState) {
    apply_queued_abort(snap);
    if !PENDING.with(|p| p.borrow().is_empty()) {
        let captured: Vec<(usize, Bytes, PreImage)> =
            PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
        // PENDING_KEYS is NOT reset here (moon#1186): first-wins holds for
        // the whole epoch, so a hot key is cloned once, not once per tick.
        drain_captured(snap, captured);
    }
    // After the captures (moon#1228 review 5): a freeze trims its table
    // against every pre-image of its database, and the ones queued before
    // the flush are in `captured`. Nothing captured after a flush belongs
    // to the flushed database (its slot is unmapped), and a capture's
    // database and pending-ness do not depend on the table events, so the
    // order is free otherwise.
    apply_table_events(snap);
    // Review 7: what the lazy-free drain freed of values whose charge moved
    // to a frozen table's bill with the FLUSHDB — after the freezes (a
    // credit may follow its table's flush within the tick), before the trim.
    FROZEN_CREDITS.with(|c| {
        for (db, bytes) in c.borrow_mut().drain(..) {
            snap.credit_frozen(db, bytes);
        }
    });
    // Review 6: the frozen tables' trims advance by a bounded amount per
    // drain (`snapshot::frozen::TRIM_BUDGET`), not whole in one.
    snap.trim_frozen(crate::persistence::snapshot::frozen::trim_budget());
    UNTRIMMED_EXCESS.with(|u| u.set(snap.untrimmed_excess()));
    // moon#1228: INFO `current_cow_size`.
    publish_cow_size(snap.cow_bytes() + DEDUPE_BYTES.with(Cell::get));
}

/// Fail the snapshot if a structural change queued an abort (moon#1224).
/// Runs first in every drain, so no segment is written after the change.
fn apply_queued_abort(snap: &mut SnapshotState) {
    if let Some(why) = ABORT.with(|a| a.take()) {
        snap.abort(why);
    }
}

/// Hand the state the table changes it follows (moon#1228), in the order
/// they happened: SWAPDBs re-point its sources, flushes freeze detached
/// tables into it. Runs before any further segment is written.
fn apply_table_events(snap: &mut SnapshotState) {
    let events = EVENTS.with(|e| std::mem::take(&mut *e.borrow_mut()));
    WAITING_EXCESS.with(|w| w.set(0));
    for event in events {
        match event {
            TableEvent::Swap(a, b) => snap.swap_slots(a, b),
            TableEvent::Freeze(db, table, bytes, start_bill) => {
                snap.freeze(db, table, bytes, start_bill)
            }
        }
    }
}

/// Core of [`drain_into`]. Needs no database: whether a key's range is still
/// pending is a function of its hash and the cursor alone (moon#1216), so a
/// split between the capture and this drain — which moves the key to a new
/// segment — cannot misfile or drop its pre-image.
fn drain_captured(snap: &mut SnapshotState, captured: Vec<(usize, Bytes, PreImage)>) {
    for (db_index, key, pre_image) in captured {
        snap.capture_cow(db_index, key, pre_image);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::snapshot::shard_snapshot_load;
    use crate::storage::compact_value::RedisValueRef;

    fn armed_guard() {
        // Tests run in parallel THREADS; every piece of state here is
        // thread-local, so each test owns its own queue.
        arm();
    }

    /// The disarmed path must never queue anything — this is the invariant
    /// that keeps the capture free on the hot path when no BGSAVE runs.
    #[test]
    fn capture_is_inert_when_disarmed() {
        disarm();
        let mut db = Database::new();
        db.set_string(b"k", Bytes::from_static(b"old"));
        let cmd = [
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"k")),
        ];
        capture_command_pre_image(&db, 0, &cmd);
        assert!(PENDING.with(|p| p.borrow().is_empty()));
    }

    /// First write wins: a script that writes the same key twice inside one
    /// snapshot epoch must stash the EPOCH-START value, not the value the
    /// first write produced.
    #[test]
    fn capture_keeps_the_first_pre_image_only() {
        armed_guard();
        let mut db = Database::new();
        db.set_string(b"k", Bytes::from_static(b"v0"));
        let cmd = [
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"k")),
        ];
        capture_command_pre_image(&db, 0, &cmd);
        db.set_string(b"k", Bytes::from_static(b"v1"));
        capture_command_pre_image(&db, 0, &cmd);

        let captured = pending_for_test();
        assert_eq!(captured.len(), 1, "second write must not re-capture");
        match captured[0].2.value.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(s as &[u8], b"v0"),
            _ => panic!("expected a string entry"),
        }
        disarm();
    }

    /// moon#1216: a write that CREATES a key captures its epoch-start state
    /// too — "absent" — so the serializer does not write the new entry. The
    /// tombstone stays out of the epoch dedupe set (memory under an insert
    /// flood), so a second write queues ONE copy of the now-present value,
    /// behind the tombstone: the drain keeps the tombstone (first capture
    /// wins) and the file does not contain the key. Later writes queue
    /// nothing.
    #[test]
    fn capture_records_absence_for_a_key_created_during_the_epoch() {
        armed_guard();
        let mut dbs = vec![Database::new()];
        for i in 0..200 {
            dbs[0].set_string(format!("k{i}").as_bytes(), Bytes::from_static(b"1"));
        }
        let mut selected = 0usize;
        let args = [Frame::BulkString(Bytes::from_static(b"fresh"))];
        for _ in 0..3 {
            let _ = crate::command::dispatch(&mut dbs[0], b"INCR", &args, &mut selected, 16);
        }
        let queued = PENDING.with(|p| p.borrow().clone());
        let order: Vec<bool> = queued.iter().map(|(_, _, e)| e.is_some()).collect();
        assert_eq!(
            order,
            vec![false, true],
            "tombstone first, one copy, then nothing"
        );
        assert_eq!(
            pending_tombstones_for_test(),
            vec![(0, Bytes::from_static(b"fresh"))]
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.rrdshard");
        let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
        drain_pending_for_test(&mut state);
        disarm();
        while !state.advance_one_segment(&dbs) {}
        state.finalize().unwrap();
        let mut loaded = vec![Database::new()];
        shard_snapshot_load(&mut loaded, &path).unwrap();
        assert!(
            loaded[0].get(b"fresh").is_none(),
            "created during the epoch"
        );
        assert_eq!(loaded[0].len(), 200);
    }

    /// End-to-end drain semantics: a pre-image captured off-loop must land
    /// in the snapshot file, and a pre-image for an ALREADY-serialized
    /// segment must be dropped (the file already holds that segment's
    /// epoch-start bytes; re-adding it would append a duplicate record that
    /// load-order would resolve to the wrong value).
    #[test]
    fn drain_folds_pending_segments_and_drops_serialized_ones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard-0.rrdshard");
        let mut dbs = vec![Database::new()];
        for i in 0..100 {
            dbs[0].set_string(
                &Bytes::from(format!("cow_{:04}", i)),
                Bytes::from(format!("val_{:04}", i)),
            );
        }
        assert!(
            dbs[0].data().segment_count() > 1,
            "fixture needs multiple segments"
        );

        let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
        // Serialize segment 0 — anything in it is already on disk.
        assert!(!state.advance_one_segment(&dbs));

        let seg0_key = dbs[0]
            .data()
            .segment(0)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes();
        let seg1_key = dbs[0]
            .data()
            .segment(1)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes();

        let captured = vec![
            (
                0,
                seg0_key.clone(),
                Some(dbs[0].data().get(&seg0_key).unwrap().clone()),
            ),
            (
                0,
                seg1_key.clone(),
                Some(dbs[0].data().get(&seg1_key).unwrap().clone()),
            ),
        ];
        drain_captured(&mut state, captured);

        // Both keys are overwritten AFTER the capture, exactly as a script
        // write would have done.
        dbs[0].set_string(&seg0_key, Bytes::from_static(b"NEW_VALUE"));
        dbs[0].set_string(&seg1_key, Bytes::from_static(b"NEW_VALUE"));

        while !state.advance_one_segment(&dbs) {}
        state.finalize().unwrap();

        let mut loaded = vec![Database::new()];
        shard_snapshot_load(&mut loaded, &path).unwrap();

        // Segment 1 was still pending at capture time -> old value survives.
        match loaded[0].get(&seg1_key).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => assert_ne!(
                s as &[u8], b"NEW_VALUE",
                "pending-segment pre-image must reach the snapshot"
            ),
            _ => panic!("expected a string entry"),
        }
        // Segment 0 was already serialized before the capture was drained;
        // its epoch-start bytes are in the file from the first advance.
        match loaded[0].get(&seg0_key).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => assert_ne!(
                s as &[u8], b"NEW_VALUE",
                "already-serialized segment keeps its epoch-start bytes"
            ),
            _ => panic!("expected a string entry"),
        }
    }

    /// moon#558: `spsc_handler::cow_intercept` only runs on the ROUTED /
    /// queued arms. An ordinary LOCAL write — every write at `--shards 1`,
    /// and the same-shard fraction at `--shards N` — reaches the database
    /// through `command::dispatch` called straight from the connection task,
    /// with no `&mut Option<SnapshotState>` anywhere in scope.
    ///
    /// RED before the fix: `pending` is empty, so the snapshot serializes the
    /// POST-`INCR` value while the WAL still holds the `INCR` — recovery
    /// double-applies it.
    #[test]
    fn generic_dispatch_captures_local_write_pre_image() {
        disarm();
        let mut db = Database::new();
        db.set_string(b"n", Bytes::from_static(b"1"));
        arm();
        let mut selected = 0usize;
        let args = [Frame::BulkString(Bytes::from_static(b"n"))];
        let _ = crate::command::dispatch(&mut db, b"INCR", &args, &mut selected, 16);
        let pending = pending_for_test();
        disarm();

        assert_eq!(
            pending.len(),
            1,
            "a LOCAL INCR during a snapshot must capture its pre-image"
        );
        assert_eq!(pending[0].0, 0, "captured under the executing db index");
        assert_eq!(pending[0].1.as_ref(), b"n");
        match pending[0].2.value.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(
                s as &[u8], b"1",
                "the pre-image must be the EPOCH-START value, not the INCR result"
            ),
            _ => panic!("expected a string entry"),
        }
    }

    /// The capture is gated on `metadata::is_write`. A NON-idempotent write
    /// that the flag table does not mark `WRITE` would silently fall back
    /// through the gate and double-apply on replay — exactly the failure
    /// moon#558 fixes, just moved one layer down. Pin the whole family of
    /// read-modify-write commands (one per value type) so a flag-table edit
    /// cannot quietly reopen it.
    #[test]
    fn every_read_modify_write_command_passes_the_is_write_gate() {
        for (cmd, args) in [
            (&b"INCR"[..], vec![Bytes::from_static(b"n")]),
            (&b"DECR"[..], vec![Bytes::from_static(b"n")]),
            (
                &b"INCRBY"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"2")],
            ),
            (
                &b"INCRBYFLOAT"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"1.5")],
            ),
            (
                &b"APPEND"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"x")],
            ),
            (
                &b"SETRANGE"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"x"),
                ],
            ),
            (&b"GETDEL"[..], vec![Bytes::from_static(b"n")]),
            (
                &b"HINCRBY"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"f"),
                    Bytes::from_static(b"1"),
                ],
            ),
            (
                &b"LPUSH"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"v")],
            ),
            (
                &b"RPUSH"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"v")],
            ),
            (&b"LPOP"[..], vec![Bytes::from_static(b"n")]),
            (
                &b"ZINCRBY"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"1"),
                    Bytes::from_static(b"m"),
                ],
            ),
            (
                &b"SETBIT"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"1"),
                ],
            ),
            (
                &b"EXPIRE"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"100")],
            ),
            (&b"PERSIST"[..], vec![Bytes::from_static(b"n")]),
            (&b"DEL"[..], vec![Bytes::from_static(b"n")]),
        ] {
            disarm();
            let mut db = Database::new();
            // A STRING pre-image is all this asserts on: the point is that the
            // gate LET THE COMMAND THROUGH, not that the command succeeded.
            db.set_string(b"n", Bytes::from_static(b"1"));
            arm();
            let frames: Vec<Frame> = args.into_iter().map(Frame::BulkString).collect();
            let mut selected = 0usize;
            let _ = crate::command::dispatch(&mut db, cmd, &frames, &mut selected, 16);
            let pending = pending_for_test();
            disarm();
            assert_eq!(
                pending.len(),
                1,
                "{} must capture a pre-image while a snapshot is armed",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    /// The choke point must stay inert for reads and for keyless commands —
    /// otherwise every GET on an armed shard pays a DashTable lookup plus a
    /// queue push for a key nothing is about to overwrite.
    #[test]
    fn generic_dispatch_does_not_capture_reads_or_keyless_commands() {
        disarm();
        let mut db = Database::new();
        db.set_string(b"n", Bytes::from_static(b"1"));
        arm();
        let mut selected = 0usize;
        let key = [Frame::BulkString(Bytes::from_static(b"n"))];
        let _ = crate::command::dispatch(&mut db, b"GET", &key, &mut selected, 16);
        let _ = crate::command::dispatch(&mut db, b"TTL", &key, &mut selected, 16);
        let _ = crate::command::dispatch(&mut db, b"PING", &[], &mut selected, 16);
        let pending = pending_for_test();
        disarm();
        assert!(
            pending.is_empty(),
            "reads and keyless commands must not queue pre-images, got {}",
            pending.len()
        );
    }

    /// End-to-end statement of the corruption moon#558 describes: a local
    /// `INCR` lands on a key whose segment has NOT been serialized yet, the
    /// snapshot then finishes, and the loaded file must hold the EPOCH-START
    /// value. If it holds the post-`INCR` value, WAL replay of that same
    /// `INCR` on top of the snapshot double-counts the key.
    ///
    /// RED before the fix: the loaded value is `2` (post-INCR), so recovery
    /// would land on `3`.
    #[test]
    fn local_incr_during_snapshot_does_not_double_apply_on_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard-0.rrdshard");
        let mut dbs = vec![Database::new()];
        for i in 0..100 {
            dbs[0].set_string(
                &Bytes::from(format!("cow_{:04}", i)),
                Bytes::from(format!("{}", i)),
            );
        }
        assert!(
            dbs[0].data().segment_count() > 1,
            "fixture needs multiple segments"
        );

        let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
        // Serialize segment 0 only; segment 1 is still pending.
        assert!(!state.advance_one_segment(&dbs));
        let victim = dbs[0]
            .data()
            .segment(1)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes();
        let epoch_start = match dbs[0].get(&victim).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => s.to_vec(),
            _ => panic!("expected a string entry"),
        };

        // The BGSAVE is in flight on this shard.
        disarm();
        arm();
        // ... and a connection task on the same thread runs a LOCAL INCR
        // between two `advance_snapshot_segment` ticks. This is exactly the
        // call the monoio/tokio local dispatch arms make.
        let mut selected = 0usize;
        let args = [Frame::BulkString(victim.clone())];
        let _ = crate::command::dispatch(&mut dbs[0], b"INCR", &args, &mut selected, 16);

        // Next tick: drain, then advance (the real ordering in
        // `shard::persistence_tick::advance_snapshot_segment`).
        drain_captured(&mut state, {
            let captured = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
            PENDING_KEYS.with(|k| k.borrow_mut().clear());
            captured
        });
        disarm();
        while !state.advance_one_segment(&dbs) {}
        state.finalize().unwrap();

        let mut loaded = vec![Database::new()];
        shard_snapshot_load(&mut loaded, &path).unwrap();
        match loaded[0].get(&victim).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(
                s as &[u8],
                &epoch_start[..],
                "snapshot must hold the epoch-start value; holding the post-INCR \
                 value double-counts when the WAL replays that INCR"
            ),
            _ => panic!("expected a string entry"),
        }
    }

    /// moon#1186: first-wins holds for the whole EPOCH. A hot key written on
    /// every tick used to be deep-cloned again after each drain (the dedupe
    /// set was cleared per drain) only for `capture_cow` to drop the copy.
    #[test]
    fn hot_key_is_cloned_once_per_epoch_not_once_per_tick() {
        let mut dbs = vec![Database::new()];
        for i in 0..100 {
            dbs[0].set_string(format!("hot_{i:03}").as_bytes(), Bytes::from_static(b"0"));
        }
        let dir = tempfile::tempdir().unwrap();
        let mut state = SnapshotState::new(0, 1, &dbs, dir.path().join("s.rrdshard"));
        disarm();
        arm_with_layout(state.segment_counts().to_vec());
        let hot = Bytes::from_static(b"hot_050");
        let args = [Frame::BulkString(hot.clone())];
        for tick in 0..5 {
            let mut selected = 0usize;
            let _ = crate::command::dispatch(&mut dbs[0], b"INCR", &args, &mut selected, 16);
            let captured = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
            if tick == 0 {
                assert_eq!(
                    captured.len(),
                    1,
                    "first write captures the epoch-start value"
                );
            } else {
                assert!(
                    captured.is_empty(),
                    "tick {tick}: a key already captured this epoch was cloned again"
                );
            }
            drain_captured(&mut state, captured);
        }
        disarm();
    }

    /// moon#1186: a write to a key whose segment is already written needs no
    /// pre-image — it is skipped BEFORE the deep clone, not dropped by the
    /// drain after it.
    #[test]
    fn capture_skips_keys_in_already_written_segments() {
        let mut dbs = vec![Database::new()];
        for i in 0..200 {
            dbs[0].set_string(format!("w_{i:04}").as_bytes(), Bytes::from_static(b"1"));
        }
        let dir = tempfile::tempdir().unwrap();
        let mut state = SnapshotState::new(0, 1, &dbs, dir.path().join("s.rrdshard"));
        assert!(dbs[0].data().segment_count() > 2, "fixture needs segments");
        disarm();
        arm_with_layout(state.segment_counts().to_vec());
        assert!(!state.advance_one_segment(&dbs));
        note_progress(state.current_db_index(), state.cursor());

        let written = key_in(&dbs[0], 0);
        let pending = key_in(&dbs[0], 1);
        let mut selected = 0usize;
        let _ = crate::command::dispatch(
            &mut dbs[0],
            b"INCR",
            &[Frame::BulkString(written)],
            &mut selected,
            16,
        );
        assert!(
            pending_for_test().is_empty(),
            "a written segment's key must not be cloned"
        );
        let _ = crate::command::dispatch(
            &mut dbs[0],
            b"INCR",
            &[Frame::BulkString(pending.clone())],
            &mut selected,
            16,
        );
        let captured = pending_for_test();
        disarm();
        assert_eq!(
            captured.len(),
            1,
            "a pending segment's key is still captured"
        );
        assert_eq!(captured[0].1, pending);
    }

    fn key_in(db: &Database, seg: usize) -> Bytes {
        db.data()
            .segment(seg)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes()
    }
}
