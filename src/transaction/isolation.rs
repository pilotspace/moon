//! moon#1299: write isolation for open cross-store transactions (`TXN`).
//!
//! A key a `TXN` writes is **held** until that transaction commits or aborts.
//! `TXN.ABORT` restores each written key's pre-transaction image (the first
//! undo record, `kv_compensation`), so any other client's write to the key in
//! between would be overwritten by the abort — acknowledged, then lost, live,
//! after a restart and on every replica (the compensation is logged). Holding
//! the key closes that: another client's write to a held key is refused with
//! [`ERR_TXN_CONFLICT_KEY`] instead of being overwritten later.
//!
//! # What is held, and where
//!
//! Every `(db, key)` the TXN's undo capture recorded — the connection leg
//! ([`crate::transaction::conn_capture`]) and the script leg
//! (`server::conn::txn_script_undo::run_local_script`). A TXN writes only on
//! its connection's shard (cross-shard writes are refused, #499), so the hold
//! table is per shard thread: a `thread_local!`, like the snapshot COW queue.
//! Holds are released by `TXN.COMMIT`, and by `TXN.ABORT` (explicit, dirty
//! commit, disconnect) only once the restore and its compensating records
//! are applied and enqueued ([`txn_end`]).
//!
//! # Who checks
//!
//! - [`check_write`] runs at the top of `command::dispatch` — the funnel for
//!   every generic write: both runtimes' local legs, routed SPSC legs, the
//!   coordinator's scatter legs, MULTI/EXEC bodies and every script
//!   `redis.call` — and on the writes that run beside dispatch: a blocking
//!   pop served at registration (`immediate_serve`, and the MULTI
//!   `BLPOP`→`LPOP` rewrite), `MOVE` / `COPY … DB` (`move_cmd`), `MQ.*`.
//! - `FLUSHDB` / `FLUSHALL` ([`check_flush`], in their dispatch arms) and
//!   `SWAPDB` ([`check_swapdb`], in both runtimes' intercepts) are refused
//!   with [`ERR_TXN_CONFLICT_DB`] while ANY shard holds a key in a database
//!   they would clear or move — read from each shard's published view
//!   ([`Published`]), since the flush runs on this shard first and then fans
//!   out.
//! - The monoio inline `SET` stands down to generic dispatch while this
//!   shard holds anything ([`any_held`]).
//! - Eviction ([`is_held`] in the victim sampler) and active expiry skip held
//!   keys; a blocking-pop waker leaves a held key's waiters parked and
//!   re-wakes them when the hold is released ([`defer_wake_if_held`]).
//! - The transaction's OWN writes pass: its connection enters
//!   [`OwnerScope`] around dispatch. The replica's apply of the master stream
//!   passes unconditionally ([`BypassScope`]): the master already decided.
//!
//! # Cost with no TXN holding anything
//!
//! One thread-local `Cell<usize>` load ([`any_held`]) per checked write, the
//! same shape as the snapshot COW gate beside it. No atomics on that path:
//! the per-shard [`Published`] view is written only when a hold appears or
//! goes, and read only by `FLUSHDB` / `FLUSHALL` / `SWAPDB` and `INFO`.
//!
//! Two transactions: a TXN writing a key ANOTHER open TXN holds is refused
//! the same way (and, like every TXN guard refusal, poisons it — #499).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;

/// The reply to a write of a key an open transaction holds (moon#1299).
///
/// Its own error code, so a client can tell "retry after the other
/// transaction ends" from a command error.
pub const ERR_TXN_CONFLICT_KEY: &[u8] = b"TXNCONFLICT key held by an open transaction";

/// The reply to `FLUSHDB` / `FLUSHALL` / `SWAPDB` while an open transaction
/// holds a key in a database it would clear or move (moon#1299).
pub const ERR_TXN_CONFLICT_DB: &[u8] = b"TXNCONFLICT database has keys held by an open transaction";

/// `true` when `reply` is one of this module's refusals.
#[inline]
pub(crate) fn is_conflict_reply(reply: &Frame) -> bool {
    matches!(reply, Frame::Error(e) if e.starts_with(b"TXNCONFLICT"))
}

// ---------------------------------------------------------------------------
// Per-shard state (shard thread only)
// ---------------------------------------------------------------------------

/// This shard's holds and open transactions.
#[derive(Default)]
struct ShardHolds {
    /// key -> the `(db, txn_id)` holding it. One holder per `(db, key)`:
    /// [`hold`] is only reached after [`check_write`] passed for that txn.
    by_key: HashMap<Bytes, SmallVec<[(usize, u64); 1]>>,
    /// Held keys per database, for `FLUSHDB` / `SWAPDB`.
    per_db: Vec<u32>,
    /// Open transactions on this shard: `(txn_id, begin unix ms)`.
    open: SmallVec<[(u64, u64); 4]>,
    /// Blocking-pop wakes skipped because the key was held; retried when a
    /// hold is released.
    rewake: Vec<(usize, Bytes)>,
}

impl ShardHolds {
    fn holder(&self, db: usize, key: &[u8]) -> Option<u64> {
        self.by_key
            .get(key)
            .and_then(|hs| hs.iter().find(|(d, _)| *d == db).map(|(_, t)| *t))
    }

    fn held_total(&self) -> usize {
        self.per_db.iter().map(|n| *n as usize).sum()
    }

    fn db_mask(&self) -> u64 {
        self.per_db
            .iter()
            .enumerate()
            .filter(|(_, n)| **n > 0)
            .fold(0u64, |m, (db, _)| m | db_bit(db))
    }
}

thread_local! {
    /// Number of `(db, key)` held on this shard: the zero-cost gate.
    static HELD: Cell<usize> = const { Cell::new(0) };
    /// The transaction whose own write is executing on this thread (0: none;
    /// txn ids start at 1).
    static OWNER: Cell<u64> = const { Cell::new(0) };
    /// Depth of [`BypassScope`]s (replica apply of the master stream).
    static BYPASS: Cell<u32> = const { Cell::new(0) };
    /// Lazily built on this shard's first `TXN.BEGIN`.
    static HOLDS: RefCell<Option<ShardHolds>> = const { RefCell::new(None) };
    /// This shard's published view, registered on first use.
    static MY_VIEW: RefCell<Option<Arc<Published>>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    /// Unit tests run in parallel threads of one process, and a published
    /// hold would make another test's `FLUSHDB` answer a conflict: only a
    /// test that opts in publishes.
    static PUBLISH_IN_TESTS: Cell<bool> = const { Cell::new(false) };
}

/// Does this shard hold any key? One thread-local load: the gate every
/// checked write takes first.
#[inline]
pub(crate) fn any_held() -> bool {
    HELD.with(Cell::get) != 0
}

#[inline]
fn bypassed() -> bool {
    BYPASS.with(Cell::get) != 0
}

/// The transaction holding `(db, key)` on this shard, if any.
#[inline]
pub(crate) fn holder(db: usize, key: &[u8]) -> Option<u64> {
    if !any_held() {
        return None;
    }
    HOLDS.with(|h| h.borrow().as_ref().and_then(|h| h.holder(db, key)))
}

/// Is `(db, key)` held by an open transaction on this shard? For eviction
/// and expiry, which skip held keys.
#[inline]
pub(crate) fn is_held(db: usize, key: &[u8]) -> bool {
    holder(db, key).is_some()
}

/// Would a write of `(db, key)` by the current writer conflict?
#[inline]
fn conflicts(db: usize, key: &[u8]) -> bool {
    let me = OWNER.with(Cell::get);
    holder(db, key).is_some_and(|t| t != me)
}

static CONFLICTS_REFUSED: AtomicU64 = AtomicU64::new(0);

/// `CONFIG RESETSTAT`: zero `txn_conflicts_refused` (a statistic; the
/// `txn_open` / `txn_held_keys` / `txn_oldest_age_ms` gauges describe live
/// state and are not reset).
pub(crate) fn reset_stats() {
    CONFLICTS_REFUSED.store(0, Ordering::Relaxed);
}

fn refuse(msg: &'static [u8]) -> Frame {
    CONFLICTS_REFUSED.fetch_add(1, Ordering::Relaxed);
    Frame::Error(Bytes::from_static(msg))
}

/// Refuse `cmd args` in database `db` when it may write a key another open
/// transaction holds (moon#1299). `None` = go ahead.
///
/// The keys are the shared key walker's WRITE positions — the set the TXN's
/// own undo capture records, deliberately over-inclusive (`LMPOP 2 a b`
/// names both). An argv the walker cannot enumerate falls back to the
/// primary key, as the capture does.
#[inline]
pub(crate) fn check_write(db: usize, cmd: &[u8], args: &[Frame]) -> Option<Frame> {
    if !any_held() {
        return None;
    }
    check_write_held(db, cmd, args)
}

#[cold]
#[inline(never)]
fn check_write_held(db: usize, cmd: &[u8], args: &[Frame]) -> Option<Frame> {
    use crate::acl::keyspec::{KeyPositions, KeyRole, command_key_positions};
    if bypassed() || !crate::command::metadata::is_write(cmd) {
        return None;
    }
    match command_key_positions(cmd, args) {
        KeyPositions::At(positions) | KeyPositions::AtPlusComputed(positions) => {
            for at in positions.iter().filter(|at| at.role == KeyRole::Write) {
                if let Some(key) = args
                    .get(at.idx)
                    .and_then(crate::command::helpers::extract_bytes)
                    && conflicts(db, key)
                {
                    return Some(refuse(ERR_TXN_CONFLICT_KEY));
                }
            }
            None
        }
        KeyPositions::None => None,
        KeyPositions::Unknown => crate::server::conn::shared::extract_primary_key(cmd, args)
            .filter(|key| conflicts(db, key.as_ref()))
            .map(|_| refuse(ERR_TXN_CONFLICT_KEY)),
    }
}

/// [`check_write`] for explicit `(db, key)` pairs — the writes that name
/// their keys outside an argv the walker reads (`MOVE`, `COPY … DB`, `MQ`).
#[inline]
pub(crate) fn check_keys<'a>(pairs: impl IntoIterator<Item = (usize, &'a [u8])>) -> Option<Frame> {
    if !any_held() || bypassed() {
        return None;
    }
    pairs
        .into_iter()
        .any(|(db, key)| conflicts(db, key))
        .then(|| refuse(ERR_TXN_CONFLICT_KEY))
}

// ---------------------------------------------------------------------------
// Hold lifecycle (the TXN's own connection, on its shard thread)
// ---------------------------------------------------------------------------

/// Record that `txn_id` opened on this shard (for `INFO`).
pub(crate) fn txn_begin(txn_id: u64) {
    let now = crate::storage::entry::current_time_ms();
    with_holds(|h| {
        if !h.open.iter().any(|(t, _)| *t == txn_id) {
            h.open.push((txn_id, now));
        }
    });
    publish();
}

/// Hold `(db, key)` for `txn_id`. Returns `true` when the hold is new —
/// what an erroring write takes back ([`unhold`]). The caller has already
/// checked that no other transaction holds it.
pub(crate) fn hold(db: usize, key: &Bytes, txn_id: u64) -> bool {
    let (newly, first_in_db) = with_holds(|h| {
        let holders = h.by_key.entry(key.clone()).or_default();
        if holders.iter().any(|(d, _)| *d == db) {
            return (false, false);
        }
        holders.push((db, txn_id));
        if h.per_db.len() <= db {
            h.per_db.resize(db + 1, 0);
        }
        h.per_db[db] += 1;
        (true, h.per_db[db] == 1)
    });
    if newly {
        HELD.with(|c| c.set(c.get() + 1));
        if first_in_db {
            publish();
        }
    }
    newly
}

/// Take back a hold [`hold`] just made for a write that answered an error
/// (moon#1303). No-op unless `txn_id` holds `(db, key)`.
pub(crate) fn unhold(db: usize, key: &[u8], txn_id: u64) {
    let (removed, last_in_db) = with_holds(|h| {
        let Some(holders) = h.by_key.get_mut(key) else {
            return (false, false);
        };
        let before = holders.len();
        holders.retain(|(d, t)| !(*d == db && *t == txn_id));
        let removed = holders.len() != before;
        if holders.is_empty() {
            h.by_key.remove(key);
        }
        if !removed {
            return (false, false);
        }
        h.per_db[db] -= 1;
        (true, h.per_db[db] == 0)
    });
    if removed {
        HELD.with(|c| c.set(c.get().saturating_sub(1)));
        if last_in_db {
            publish();
        }
    }
}

/// `txn_id` ended (committed, or aborted with its restore applied and
/// logged): release every key it holds, forget it, and re-wake the blocking
/// waiters a held key kept parked. Idempotent.
pub(crate) fn txn_end(txn_id: u64) {
    let rewake = with_holds(|h| {
        h.open.retain(|(t, _)| *t != txn_id);
        let mut released = 0usize;
        let per_db = &mut h.per_db;
        h.by_key.retain(|_, holders| {
            holders.retain(|(db, t)| {
                if *t == txn_id {
                    per_db[*db] -= 1;
                    released += 1;
                    false
                } else {
                    true
                }
            });
            !holders.is_empty()
        });
        let wakes: Vec<(usize, Bytes)> = if released == 0 || h.rewake.is_empty() {
            Vec::new()
        } else {
            let pending = std::mem::take(&mut h.rewake);
            let (now, later): (Vec<_>, Vec<_>) = pending
                .into_iter()
                .partition(|(db, key)| h.holder(*db, key).is_none());
            h.rewake = later;
            now
        };
        HELD.with(|c| c.set(h.held_total()));
        wakes
    });
    for (db, key) in &rewake {
        crate::blocking::wakeup::defer_wake(*db, key);
    }
    publish();
}

/// Calls [`txn_end`] when dropped: `TXN.ABORT`'s release, which must run
/// after the abort's last await AND if that future is dropped mid-await.
/// Dropped on the shard thread (connection futures never leave it).
#[must_use = "the release happens when the guard drops"]
pub(crate) struct EndOnDrop(u64);

impl EndOnDrop {
    pub(crate) fn new(txn_id: u64) -> Self {
        EndOnDrop(txn_id)
    }
}

impl Drop for EndOnDrop {
    fn drop(&mut self) {
        txn_end(self.0);
    }
}

/// A blocking-pop waker is about to serve `(db, key)`. When the key is held,
/// the waiters stay parked (serving one would write the held key) and the
/// wake is retried once the hold is released. `true` = skip the serve.
#[inline]
pub(crate) fn defer_wake_if_held(db: usize, key: &Bytes) -> bool {
    if !any_held() || bypassed() || !is_held(db, key) {
        return false;
    }
    rewake_on_release(db, key);
    true
}

/// Retry a wake on `(db, key)` once a hold is released — a `BLMOVE` waiter
/// whose DESTINATION is held stays parked on its (unheld) source.
pub(crate) fn rewake_on_release(db: usize, key: &Bytes) {
    with_holds(|h| {
        if !h.rewake.iter().any(|(d, k)| *d == db && k == key) {
            h.rewake.push((db, key.clone()));
        }
    });
}

fn with_holds<R>(f: impl FnOnce(&mut ShardHolds) -> R) -> R {
    HOLDS.with(|h| f(h.borrow_mut().get_or_insert_with(ShardHolds::default)))
}

/// Run `run` as `txn_id`'s own write: keys that transaction holds pass the
/// checks, keys another one holds are still refused. Restored on drop.
#[must_use = "the scope ends when the guard drops"]
pub(crate) struct OwnerScope {
    prev: u64,
}

impl OwnerScope {
    pub(crate) fn enter(txn_id: u64) -> Self {
        OwnerScope {
            prev: OWNER.with(|o| o.replace(txn_id)),
        }
    }
}

impl Drop for OwnerScope {
    fn drop(&mut self) {
        OWNER.with(|o| o.set(self.prev));
    }
}

/// Skip every check while alive: a replica applying its master's stream
/// (the master already decided; its own transactions are not the replica's).
#[must_use = "the scope ends when the guard drops"]
pub(crate) struct BypassScope(());

impl BypassScope {
    pub(crate) fn enter() -> Self {
        BYPASS.with(|b| b.set(b.get() + 1));
        BypassScope(())
    }
}

impl Drop for BypassScope {
    fn drop(&mut self) {
        BYPASS.with(|b| b.set(b.get().saturating_sub(1)));
    }
}

// ---------------------------------------------------------------------------
// Cross-shard published view: FLUSHDB / FLUSHALL / SWAPDB and INFO
// ---------------------------------------------------------------------------

/// Bit for database `db` in [`Published::held_dbs`]. Databases past 63 share
/// bit 63: a refusal there may be conservative, never missed.
#[inline]
fn db_bit(db: usize) -> u64 {
    1u64 << db.min(63)
}

/// Bits of [`Published::open_word`] holding the oldest open TXN's begin time
/// (unix ms, 48 bits: good past the year 10000).
const BEGIN_BITS: u32 = 48;
const BEGIN_MASK: u64 = (1u64 << BEGIN_BITS) - 1;

/// One shard's view of its transactions, for other shards to read.
///
/// Single writer — the owning shard thread stores whole words, never a
/// read-modify-write — and readers that only load, so a reader sees some
/// recent consistent word. `open_word` packs `open count (16 bits) |
/// oldest begin ms (48 bits)` into ONE word, so a reader never pairs a count
/// with another moment's timestamp. Modelled in `tests/loom_response_slot.rs`
/// (`txn_isolation_view`).
#[derive(Default)]
pub(crate) struct Published {
    open_word: AtomicU64,
    held_dbs: AtomicU64,
    held_keys: AtomicU64,
}

/// Pack an open count and the oldest begin time into one word.
#[inline]
pub(crate) fn pack_open(count: usize, oldest_begin_ms: u64) -> u64 {
    let count = count.min(u16::MAX as usize) as u64;
    (count << BEGIN_BITS) | (oldest_begin_ms & BEGIN_MASK)
}

/// Inverse of [`pack_open`].
#[inline]
pub(crate) fn unpack_open(word: u64) -> (usize, u64) {
    ((word >> BEGIN_BITS) as usize, word & BEGIN_MASK)
}

static REGISTRY: parking_lot::Mutex<Vec<Arc<Published>>> = parking_lot::Mutex::new(Vec::new());

fn my_view() -> Arc<Published> {
    MY_VIEW.with(|v| {
        v.borrow_mut()
            .get_or_insert_with(|| {
                let view = Arc::new(Published::default());
                REGISTRY.lock().push(Arc::clone(&view));
                view
            })
            .clone()
    })
}

/// Store this shard's current view (hold appeared / went, TXN began / ended).
fn publish() {
    #[cfg(test)]
    if !PUBLISH_IN_TESTS.with(Cell::get) {
        return;
    }
    let (word, dbs, keys) = with_holds(|h| {
        let oldest = h.open.iter().map(|(_, at)| *at).min().unwrap_or(0);
        (
            pack_open(h.open.len(), oldest),
            h.db_mask(),
            h.held_total() as u64,
        )
    });
    let view = my_view();
    view.held_dbs.store(dbs, Ordering::Release);
    view.held_keys.store(keys, Ordering::Release);
    view.open_word.store(word, Ordering::Release);
}

/// Does any shard hold a key in a database under `mask`?
fn any_shard_holds(mask: u64) -> bool {
    REGISTRY
        .lock()
        .iter()
        .any(|v| v.held_dbs.load(Ordering::Acquire) & mask != 0)
}

/// Refuse `FLUSHDB` (database `db`) / `FLUSHALL` while any shard holds a key
/// in a database it would clear. Called from their dispatch arms, which
/// every flush runs through (client, MULTI/EXEC, script, the fan-out legs):
/// the originating shard refuses before it clears anything or fans out.
/// The per-shard legs check again against their own thread-local table, so
/// a hold taken while a fan-out is in flight is never cleared under its TXN.
pub(crate) fn check_flush(all_dbs: bool, db: usize, db_count: usize) -> Option<Frame> {
    if bypassed() {
        return None;
    }
    let local = any_held()
        && with_holds(|h| {
            if all_dbs {
                h.held_total() > 0
            } else {
                h.per_db.get(db).is_some_and(|n| *n > 0)
            }
        });
    let mask = if all_dbs {
        (0..db_count.min(64)).fold(0u64, |m, d| m | db_bit(d))
    } else {
        db_bit(db)
    };
    (local || any_shard_holds(mask)).then(|| refuse(ERR_TXN_CONFLICT_DB))
}

/// Refuse `SWAPDB a b` while any shard holds a key in `a` or `b`: the
/// transaction's undo would restore into the database now holding the other
/// one's contents.
pub(crate) fn check_swapdb(a: usize, b: usize) -> Option<Frame> {
    if bypassed() {
        return None;
    }
    let local = any_held()
        && with_holds(|h| {
            [a, b]
                .iter()
                .any(|db| h.per_db.get(*db).is_some_and(|n| *n > 0))
        });
    (local || any_shard_holds(db_bit(a) | db_bit(b))).then(|| refuse(ERR_TXN_CONFLICT_DB))
}

/// `INFO` fields: open transactions, the oldest one's age, keys held, and
/// writes refused so far, summed over every shard.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxnInfo {
    pub open: u64,
    pub oldest_age_ms: u64,
    pub held_keys: u64,
    pub conflicts_refused: u64,
}

pub(crate) fn info(now_ms: u64) -> TxnInfo {
    let mut out = TxnInfo {
        conflicts_refused: CONFLICTS_REFUSED.load(Ordering::Relaxed),
        ..TxnInfo::default()
    };
    let mut oldest: Option<u64> = None;
    for view in REGISTRY.lock().iter() {
        let (count, begin) = unpack_open(view.open_word.load(Ordering::Acquire));
        if count > 0 {
            out.open += count as u64;
            oldest = Some(oldest.map_or(begin, |o| o.min(begin)));
        }
        out.held_keys += view.held_keys.load(Ordering::Acquire);
    }
    out.oldest_age_ms = oldest.map_or(0, |b| now_ms.saturating_sub(b));
    out
}

/// Append the `INFO` stats lines.
pub(crate) fn write_info(out: &mut String) {
    use std::fmt::Write as _;
    let i = info(crate::storage::entry::current_time_ms());
    let _ = write!(
        out,
        "txn_open:{}\r\ntxn_oldest_age_ms:{}\r\ntxn_held_keys:{}\r\ntxn_conflicts_refused:{}\r\n",
        i.open, i.oldest_age_ms, i.held_keys, i.conflicts_refused
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::from(s.to_string()))
    }

    fn args(parts: &[&str]) -> Vec<Frame> {
        parts.iter().map(|p| bulk(p)).collect()
    }

    /// Every test runs on its own thread: the thread-local tables start
    /// empty, and each ends its transactions so the registry view is clean.
    fn on_fresh_thread(f: impl FnOnce() + Send + 'static) {
        std::thread::spawn(f).join().expect("test thread");
    }

    #[test]
    fn nothing_held_checks_nothing() {
        on_fresh_thread(|| {
            assert!(!any_held());
            assert_eq!(check_write(0, b"SET", &args(&["k", "v"])), None);
            assert!(!is_held(0, b"k"));
        });
    }

    #[test]
    fn a_held_key_refuses_other_writers_and_admits_its_owner() {
        on_fresh_thread(|| {
            txn_begin(7);
            assert!(hold(0, &Bytes::from_static(b"k"), 7));
            assert!(
                !hold(0, &Bytes::from_static(b"k"), 7),
                "second hold is not new"
            );
            let refused = check_write(0, b"SET", &args(&["k", "v"]));
            assert!(
                refused.as_ref().is_some_and(is_conflict_reply),
                "{refused:?}"
            );
            // Other db, other key, a read: unaffected.
            assert_eq!(check_write(1, b"SET", &args(&["k", "v"])), None);
            assert_eq!(check_write(0, b"SET", &args(&["j", "v"])), None);
            assert_eq!(check_write(0, b"GET", &args(&["k"])), None);
            // A multi-key write naming it anywhere is refused.
            assert!(check_write(0, b"MSET", &args(&["a", "1", "k", "2"])).is_some());
            assert!(check_write(0, b"RENAME", &args(&["src", "k"])).is_some());
            // COPY only reads its source.
            assert_eq!(check_write(0, b"COPY", &args(&["k", "dst"])), None);
            {
                let _me = OwnerScope::enter(7);
                assert_eq!(check_write(0, b"SET", &args(&["k", "v"])), None);
            }
            {
                let _other = OwnerScope::enter(8);
                assert!(check_write(0, b"SET", &args(&["k", "v"])).is_some());
            }
            {
                let _replica = BypassScope::enter();
                assert_eq!(check_write(0, b"SET", &args(&["k", "v"])), None);
            }
            txn_end(7);
            assert!(!any_held());
            assert_eq!(check_write(0, b"SET", &args(&["k", "v"])), None);
        });
    }

    #[test]
    fn end_on_drop_releases() {
        on_fresh_thread(|| {
            txn_begin(51);
            hold(0, &Bytes::from_static(b"k"), 51);
            {
                let _g = EndOnDrop::new(51);
                assert!(any_held(), "held until the guard drops");
            }
            assert!(!any_held());
        });
    }

    #[test]
    fn unhold_takes_back_only_a_new_hold() {
        on_fresh_thread(|| {
            txn_begin(3);
            let k = Bytes::from_static(b"k");
            assert!(hold(2, &k, 3));
            unhold(2, &k, 3);
            assert!(!is_held(2, b"k"));
            assert!(!any_held());
            unhold(2, &k, 3); // idempotent
            txn_end(3);
        });
    }

    #[test]
    fn flush_and_swapdb_see_the_holding_database() {
        on_fresh_thread(|| {
            txn_begin(11);
            hold(5, &Bytes::from_static(b"x"), 11);
            assert!(check_flush(false, 5, 16).is_some());
            assert!(check_flush(true, 0, 16).is_some());
            assert_eq!(check_flush(false, 4, 16), None);
            assert!(check_swapdb(5, 6).is_some());
            assert!(check_swapdb(6, 5).is_some());
            txn_end(11);
            assert_eq!(check_flush(false, 5, 16), None);
            assert_eq!(check_swapdb(5, 6), None);
        });
    }

    #[test]
    fn another_shards_hold_is_visible_to_flush() {
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        // db 62: no other unit test's database (they use 16).
        let holder = std::thread::spawn(move || {
            PUBLISH_IN_TESTS.with(|p| p.set(true));
            txn_begin(21);
            hold(62, &Bytes::from_static(b"y"), 21);
            held_tx.send(()).expect("send");
            done_rx.recv().expect("recv");
            txn_end(21);
        });
        held_rx.recv().expect("held");
        on_fresh_thread(|| {
            // This thread holds nothing; the other "shard" holds db 62.
            assert!(!any_held());
            assert!(check_flush(false, 62, 64).is_some());
            assert!(check_flush(true, 0, 64).is_some());
            assert_eq!(check_flush(true, 0, 16), None, "dbs past db_count");
            assert!(check_swapdb(62, 10).is_some());
        });
        done_tx.send(()).expect("done");
        holder.join().expect("holder");
    }

    #[test]
    fn a_skipped_wake_is_deferred_until_release() {
        on_fresh_thread(|| {
            txn_begin(4);
            let l = Bytes::from_static(b"list");
            hold(0, &l, 4);
            assert!(defer_wake_if_held(0, &l));
            assert!(!defer_wake_if_held(0, &Bytes::from_static(b"other")));
            txn_end(4);
            assert!(!defer_wake_if_held(0, &l));
            let deferred = crate::blocking::wakeup::take_deferred_wakes().unwrap_or_default();
            assert_eq!(deferred, vec![(0, l)]);
        });
    }

    /// Active expiry leaves a held expired key in place (its pair stays in
    /// the index) and reaps it once the transaction ends; the backlog latch
    /// does not report the held key as work left.
    #[test]
    fn active_expiry_skips_a_held_key_until_release() {
        on_fresh_thread(|| {
            use crate::storage::entry::{Entry, current_time_ms};
            let mut db = crate::storage::Database::new();
            let past = current_time_ms() - 1000;
            for k in ["held", "free"] {
                db.set(
                    &Bytes::from(k),
                    Entry::new_string_with_expiry(Bytes::from_static(b"v"), past),
                );
            }
            txn_begin(41);
            hold(db.db_index, &Bytes::from_static(b"held"), 41);
            let mut reaped = Vec::new();
            let left = crate::server::expiration::expire_cycle_direct_scaled(
                &mut db,
                &mut |k| reaped.push(k.to_vec()),
                1,
            );
            assert_eq!(reaped, vec![b"free".to_vec()]);
            assert!(!left, "a held key is no backlog");
            assert_eq!(db.logical_len(), 1);
            txn_end(41);
            crate::server::expiration::expire_cycle_direct(&mut db, &mut |k| {
                reaped.push(k.to_vec())
            });
            assert_eq!(reaped, vec![b"free".to_vec(), b"held".to_vec()]);
            assert_eq!(db.logical_len(), 0);
        });
    }

    #[test]
    fn packed_open_word_round_trips() {
        assert_eq!(unpack_open(pack_open(0, 0)), (0, 0));
        assert_eq!(
            unpack_open(pack_open(3, 1_790_000_000_000)),
            (3, 1_790_000_000_000)
        );
        assert_eq!(unpack_open(pack_open(1 << 20, 5)).0, u16::MAX as usize);
    }

    #[test]
    fn info_counts_open_transactions_and_their_age() {
        on_fresh_thread(|| {
            PUBLISH_IN_TESTS.with(|p| p.set(true));
            txn_begin(31);
            hold(62, &Bytes::from_static(b"a"), 31);
            let now = crate::storage::entry::current_time_ms();
            let i = info(now + 250);
            assert!(i.open >= 1);
            assert!(i.oldest_age_ms >= 250);
            assert!(i.held_keys >= 1);
            txn_end(31);
        });
    }
}
