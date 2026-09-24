//! L4 shared read plane — the ONLY cross-thread window into shard-owned state.
//!
//! # Why this module exists
//!
//! `ShardSlice` is deliberately `!Send + !Sync` (`slice.rs`, `_not_send`
//! marker): it owns `VectorStore`, `TextStore`, `GraphStore` and the lazy
//! registries, none of which are `Sync`. A foreign shard therefore cannot
//! touch a slice at all — a keyed command whose key lives elsewhere takes an
//! SPSC hop to the owning shard and parks awaiting the reply.
//!
//! That park is the cost. Measured on GCE t2a-standard-8 (aarch64, 8 vCPU),
//! fitting per-command CPU against pipeline depth gives
//! `cost = 0.413 − 0.046·msgs/cmd + 2.488·parks/cmd` (CPU%/kops): **2.49 per
//! park, ~zero per message.** At p=1 the park term is 85% of the total.
//!
//! This module shares strictly one thing — `Database` — so that a *read* of a
//! foreign key can be served on the calling thread without a hop. `Database`
//! is `Send + Sync` (pinned by the static assertion at the bottom of this
//! file); the slice's `!Send` marker stays exactly where it is, and no vector,
//! text, graph, or registry state ever crosses a thread.
//!
//! # Granularity
//!
//! One `RwLock` per `(shard, db)`, each on its own cache line:
//!
//! - a write to db 0 does not exclude a foreign read of db 3;
//! - no lock is taken on any per-key path — one per command, not per key;
//! - `s8 × 16 dbs` costs 8 KB of padding, and nothing per key.
//!
//! # The one rule that makes it safe to reason about
//!
//! **Foreign readers use `try_read` and NEVER park.** One CAS; on failure the
//! command falls through to the SPSC path it takes today. A foreign reader can
//! therefore never convoy behind the owner, and the owner's writes can never
//! be starved by reader arrival rate — `parking_lot` sets `WRITER_BIT`
//! immediately (even with readers inside), after which every `try_read`
//! refuses and diverts to SPSC. The owner waits only for readers already
//! inside their critical section, and every such section is RAM-only.
//!
//! Guards are handed out through `FnOnce(&Database)` closures rather than
//! returned, so a guard cannot escape and cannot cross an `.await`. "Never
//! hold a lock across `.await`" is enforced by the type system here, not by
//! review.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_utils::CachePadded;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use smallvec::SmallVec;

use crate::storage::Database;

/// One shard's databases, shared for the L4 read plane.
pub struct ShardDbSet {
    /// Which shard owns these databases. Diagnostics only — the panic message
    /// on an out-of-range index names it, matching the message `with_shard_db`
    /// documented before the databases moved behind this plane.
    shard_id: usize,
    dbs: Box<[CachePadded<RwLock<Database>>]>,
}

impl ShardDbSet {
    /// Number of `SELECT`-able databases in this shard.
    #[inline]
    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

    /// OWNER ONLY — exclusive guard for a mutating command or a bounded
    /// maintenance chunk.
    ///
    /// # Panics
    /// If this thread already holds a guard on `idx` (see [`guard_depth`]), or
    /// if `idx` is out of range.
    #[inline]
    pub fn write(&self, idx: usize) -> DbWriteGuard<'_> {
        let cell = self.slot(idx);
        let _depth = guard_depth::acquire(idx);
        #[cfg(test)]
        exclusive_count::bump();
        DbWriteGuard {
            inner: cell.write(),
            _depth,
        }
    }

    /// OWNER ONLY — shared guard for the owner's own read commands.
    ///
    /// The owner is the sole writer, so this can only ever contend with other
    /// readers' count CAS: a retry, never a park.
    ///
    /// # Panics
    /// Same contract as [`ShardDbSet::write`].
    #[inline]
    pub fn read(&self, idx: usize) -> DbReadGuard<'_> {
        let cell = self.slot(idx);
        let _depth = guard_depth::acquire(idx);
        DbReadGuard {
            inner: cell.read(),
            _depth,
        }
    }

    /// FOREIGN READERS ONLY — one CAS attempt, **never parks**.
    ///
    /// `None` means the owner holds (or is waiting for) the write lock; the
    /// caller must fall through to the SPSC path. No re-entrancy bookkeeping:
    /// a single non-blocking attempt cannot deadlock, and a foreign thread
    /// never holds two guards.
    #[inline]
    pub fn try_read(&self, idx: usize) -> Option<RwLockReadGuard<'_, Database>> {
        self.dbs.get(idx)?.try_read()
    }

    /// FOREIGN WRITERS ONLY — one CAS attempt for EXCLUSIVE access, **never
    /// parks**. The D3 counterpart of [`ShardDbSet::try_read`].
    ///
    /// `None` means someone else holds the database (owner or another foreign
    /// writer); the caller must fall through to the SPSC path. As with
    /// `try_read`, no re-entrancy bookkeeping: a single non-blocking attempt
    /// cannot deadlock, and a foreign thread never holds two guards.
    ///
    /// **Not to be confused with [`ShardDbSet::try_write`]**, whose "try" is
    /// only about the index — it calls `write()` and BLOCKS on a held
    /// database, which is exactly what a foreign thread must never do.
    #[inline]
    pub fn try_write_foreign(&self, idx: usize) -> Option<RwLockWriteGuard<'_, Database>> {
        self.dbs.get(idx)?.try_write()
    }

    /// OWNER ONLY — two databases at once (`MOVE`, `COPY … DB n`, `SWAPDB`).
    ///
    /// Always acquires in **ascending index order** — that ordering is the
    /// single deadlock rule in this module; the only other multi-lock path,
    /// [`ShardDbSet::write_all`], obeys the same order. Guards come back in the
    /// caller's argument order regardless.
    ///
    /// # Panics
    /// If `a == b` (callers must special-case the same-db degenerate form), on
    /// the re-entrancy contract, or if either index is out of range.
    pub fn write_pair(&self, a: usize, b: usize) -> (DbWriteGuard<'_>, DbWriteGuard<'_>) {
        assert_ne!(a, b, "write_pair requires distinct databases");
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let lo_guard = self.write(lo);
        let hi_guard = self.write(hi);
        if a < b {
            (lo_guard, hi_guard)
        } else {
            (hi_guard, lo_guard)
        }
    }

    /// OWNER ONLY — every database, ascending, for multi-db atomic operations
    /// (`FLUSHALL` looping over 0..16 per moon#677, `SWAPDB`, checkpoint
    /// capture, `DEBUG DIGEST`).
    ///
    /// Preserves the cross-db atomicity these paths get for free today from
    /// being single-threaded, which a foreign reader could otherwise observe
    /// mid-flight.
    pub fn write_all(&self) -> SmallVec<[DbWriteGuard<'_>; 16]> {
        (0..self.dbs.len()).map(|i| self.write(i)).collect()
    }

    /// OWNER ONLY — like [`ShardDbSet::write`] but `None` instead of a panic
    /// when `idx` is out of range. The replacement for `get_mut(i)` at sites
    /// that already handled a missing database.
    #[inline]
    pub fn try_write(&self, idx: usize) -> Option<DbWriteGuard<'_>> {
        let cell = self.dbs.get(idx)?;
        let _depth = guard_depth::acquire(idx);
        Some(DbWriteGuard {
            inner: cell.write(),
            _depth,
        })
    }

    /// OWNER ONLY — an exclusive guard on a SECOND database, taken while this
    /// thread already holds a guard on another one: a script's `MOVE` /
    /// `COPY ... DB n` (moon#1068), whose own db stays locked for the whole
    /// script.
    ///
    /// **Blocks** until a foreign reader or writer holding `idx` lets go, like
    /// [`ShardDbSet::write`]. That cannot deadlock even though `idx` may be
    /// below the index already held, against [`ShardDbSet::write_pair`]'s
    /// ascending rule: that rule orders the OWNER's acquisitions against each
    /// other, and the owner is the only thread that ever parks on these
    /// locks. Foreign parties ([`ShardDbSet::try_read`],
    /// [`ShardDbSet::try_write_foreign`]) make one non-blocking attempt and
    /// hold a single guard without waiting on anything, so they cannot be one
    /// side of a cycle.
    ///
    /// `None` only for the two structural cases, never for contention:
    /// `idx` is out of range, or this thread already holds `idx`. The second
    /// would deadlock on a real `RwLock` (and panics in
    /// [`ShardDbSet::write`]); here it is a refusal the caller turns into an
    /// error reply, so a caller bug cannot take the shard thread down.
    #[inline]
    pub fn write_second(&self, idx: usize) -> Option<DbWriteGuard<'_>> {
        let cell = self.dbs.get(idx)?;
        if guard_depth::is_held(idx) {
            return None;
        }
        let _depth = guard_depth::acquire(idx);
        Some(DbWriteGuard {
            inner: cell.write(),
            _depth,
        })
    }

    /// OWNER ONLY — like [`ShardDbSet::read`] but `None` instead of a panic
    /// when `idx` is out of range.
    #[inline]
    pub fn try_read_owned(&self, idx: usize) -> Option<DbReadGuard<'_>> {
        let cell = self.dbs.get(idx)?;
        let _depth = guard_depth::acquire(idx);
        Some(DbReadGuard {
            inner: cell.read(),
            _depth,
        })
    }

    /// OWNER ONLY — every database, ascending, under SHARED guards, for the
    /// read-only multi-db consumers (RDB save, snapshot capture, `DEBUG
    /// DIGEST`).
    ///
    /// Shared rather than exclusive so a concurrent foreign reader is not
    /// excluded by a checkpoint; cross-db consistency still holds, because no
    /// writer can interleave while every db is read-guarded.
    pub fn read_all(&self) -> SmallVec<[DbReadGuard<'_>; 16]> {
        (0..self.dbs.len()).map(|i| self.read(i)).collect()
    }

    /// OWNER ONLY — read-only counterpart of [`ShardDbSet::with_all`], for
    /// helpers that take `&[Database]`.
    pub fn with_all_read<R>(&self, f: impl FnOnce(&[&Database]) -> R) -> R {
        let guards = self.read_all();
        let refs: SmallVec<[&Database; 16]> = guards.iter().map(|g| &**g).collect();
        f(&refs)
    }

    /// OWNER ONLY — exchange the CONTENTS of two databases (`SWAPDB`).
    ///
    /// Replaces the slice's `databases.swap(a, b)`. Swapping the contents
    /// rather than the lock cells matters: a foreign reader holding
    /// `try_read(a)` keeps reading db `a`'s lock, and sees the post-swap
    /// contents once the write guard is released — the same visibility the
    /// SPSC path gives today.
    ///
    /// A same-db swap is a no-op, matching `slice::swap`'s behaviour.
    pub fn swap(&self, a: usize, b: usize) {
        if a == b {
            return;
        }
        // moon#1224: an in-flight snapshot epoch must hear of it first.
        crate::persistence::snapshot_cow::note_swapdb(a, b);
        self.with_pair(a, b, |da, db| std::mem::swap(da, db));
    }

    /// OWNER ONLY — exclusive access to EVERY database at once, as a slice of
    /// mutable references, for the multi-db helpers that used to receive the
    /// slice's `&mut [Database]` directly (`FLUSHALL` per moon#677, `SWAPDB`,
    /// RDB save/load, `DEBUG DIGEST`, checkpoint capture).
    ///
    /// The guards are held for the whole closure, so the cross-db atomicity
    /// these paths got for free from being single-threaded is preserved
    /// exactly — a foreign reader cannot observe a half-flushed keyspace.
    ///
    /// Acquires ascending, per the module's single deadlock rule.
    ///
    /// # Panics
    /// If this thread already holds a guard on any database.
    pub fn with_all<R>(&self, f: impl FnOnce(&mut [&mut Database]) -> R) -> R {
        let mut guards = self.write_all();
        let mut refs: SmallVec<[&mut Database; 16]> = guards.iter_mut().map(|g| &mut **g).collect();
        f(&mut refs)
    }

    /// OWNER ONLY — exclusive access to two databases as mutable references,
    /// for `MOVE` / `COPY … DB n` / `SWAPDB`.
    ///
    /// Acquires ascending; the closure still receives them in `(a, b)` order.
    ///
    /// # Panics
    /// If `a == b`, or on the re-entrancy contract.
    pub fn with_pair<R>(
        &self,
        a: usize,
        b: usize,
        f: impl FnOnce(&mut Database, &mut Database) -> R,
    ) -> R {
        let (mut ga, mut gb) = self.write_pair(a, b);
        f(&mut ga, &mut gb)
    }

    #[inline]
    fn slot(&self, idx: usize) -> &CachePadded<RwLock<Database>> {
        match self.dbs.get(idx) {
            Some(cell) => cell,
            None => panic!(
                "db_index {idx} out of bounds ({} databases on shard {})",
                self.dbs.len(),
                self.shard_id
            ),
        }
    }
}

/// Owner's exclusive guard. Releases the re-entrancy bit on drop, so the mask
/// cannot leak through an early return or a panic unwind.
pub struct DbWriteGuard<'a> {
    inner: RwLockWriteGuard<'a, Database>,
    _depth: guard_depth::DepthToken,
}

impl std::ops::Deref for DbWriteGuard<'_> {
    type Target = Database;
    #[inline]
    fn deref(&self) -> &Database {
        &self.inner
    }
}

impl std::ops::DerefMut for DbWriteGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Database {
        &mut self.inner
    }
}

/// Owner's shared guard. Same drop discipline as [`DbWriteGuard`].
pub struct DbReadGuard<'a> {
    inner: RwLockReadGuard<'a, Database>,
    _depth: guard_depth::DepthToken,
}

impl std::ops::Deref for DbReadGuard<'_> {
    type Target = Database;
    #[inline]
    fn deref(&self) -> &Database {
        &self.inner
    }
}

/// Re-entrancy contract, replacing the `RefCell` double-borrow panic that
/// guarded database access before the locks existed.
///
/// A thread-local bitmask of db indexes held by *this* thread. Re-acquiring
/// the same index from inside a guard's closure is a bug that would DEADLOCK
/// on a real `RwLock` where the `RefCell` merely panicked; the mask restores
/// the loud failure. One thread-local bit-op per acquire, released by
/// [`DepthToken`]'s `Drop`.
///
/// Only owner acquisitions register: foreign `try_read` cannot deadlock, so it
/// pays nothing here.
/// Test-only count of the owner's EXCLUSIVE acquisitions ([`ShardDbSet::write`])
/// on this thread. Each one is a window in which a foreign `try_read` of that
/// database declines into a parked SPSC hop (cost model §8.3), so an arm that
/// takes more of them than it has commands is measurable (moon#1198).
#[cfg(test)]
pub(crate) mod exclusive_count {
    use std::cell::Cell;

    thread_local! {
        static COUNT: Cell<u64> = const { Cell::new(0) };
    }

    #[inline]
    pub(super) fn bump() {
        COUNT.with(|c| c.set(c.get() + 1));
    }

    /// Exclusive acquisitions on this thread so far.
    pub(crate) fn get() -> u64 {
        COUNT.with(Cell::get)
    }
}

mod guard_depth {
    use std::cell::Cell;

    thread_local! {
        static HELD: Cell<u64> = const { Cell::new(0) };
    }

    /// Databases beyond this index are not tracked. Redis allows 16 by
    /// default and moon has never supported more than 64; an untracked index
    /// degrades to today's behaviour (a real deadlock on re-entry), it does
    /// not corrupt the mask.
    const TRACKED: usize = 64;

    /// RAII marker: clears the thread's bit for `idx` when dropped.
    pub(super) struct DepthToken(usize);

    impl Drop for DepthToken {
        #[inline]
        fn drop(&mut self) {
            if self.0 < TRACKED {
                HELD.with(|h| h.set(h.get() & !(1u64 << self.0)));
            }
        }
    }

    /// Marks `idx` held on this thread, panicking if it already is.
    #[inline]
    pub(super) fn acquire(idx: usize) -> DepthToken {
        if idx < TRACKED {
            HELD.with(|h| {
                let bit = 1u64 << idx;
                assert!(
                    h.get() & bit == 0,
                    "db guard held recursively — closure may not re-acquire its own db (index {idx})"
                );
                h.set(h.get() | bit);
            });
        }
        DepthToken(idx)
    }

    /// Whether this thread holds a guard on `idx`. An index beyond
    /// [`TRACKED`] is never reported held (moon has never supported that
    /// many databases).
    #[inline]
    pub(super) fn is_held(idx: usize) -> bool {
        idx < TRACKED && HELD.with(|h| h.get() & (1u64 << idx) != 0)
    }

    /// Test-only view of the mask.
    #[cfg(test)]
    pub(super) fn held_mask() -> u64 {
        HELD.with(|h| h.get())
    }
}

/// The process-wide registry: one [`ShardDbSet`] per shard.
///
/// Populated by `ShardDatabases::new` on the main thread **before** any shard
/// thread is spawned, so there is no initialisation race by construction.
static L4_REGISTRY: OnceLock<Box<[Arc<ShardDbSet>]>> = OnceLock::new();

/// Install the registry. Called exactly once, from `ShardDatabases::new`.
///
/// Returns `false` if a registry was already installed — the caller decides
/// whether that is a fatal double-init or a benign re-entry in tests.
pub fn install_registry(shard_databases: Vec<Vec<Database>>) -> bool {
    install_registry_sets(build_sets(shard_databases))
}

/// Install pre-built sets. Returns `false` if a registry was already
/// installed.
///
/// A second install is NOT fatal and must not panic: the test suite builds
/// many servers in one process, and `ShardDatabases::new` runs once per
/// server. The first registry keeps the plane; later servers simply do not
/// publish theirs, so their foreign reads fall back to SPSC — correct, just
/// not accelerated. Production calls this exactly once.
pub fn install_registry_sets(sets: Box<[Arc<ShardDbSet>]>) -> bool {
    L4_REGISTRY.set(sets).is_ok()
}

/// Build the per-shard sets without touching the global — the construction the
/// registry installs, exposed so it is testable without poisoning a process-
/// wide `OnceLock` for every other test in the binary.
pub(crate) fn build_sets(shard_databases: Vec<Vec<Database>>) -> Box<[Arc<ShardDbSet>]> {
    shard_databases
        .into_iter()
        .enumerate()
        .map(|(shard_id, dbs)| {
            let padded: Box<[CachePadded<RwLock<Database>>]> = dbs
                .into_iter()
                .map(|db| CachePadded::new(RwLock::new(db)))
                .collect::<Vec<_>>()
                .into_boxed_slice();
            Arc::new(ShardDbSet {
                shard_id,
                dbs: padded,
            })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

/// The registry, or `None` if it was never installed (unit tests that build a
/// slice directly, and any binary that never calls `ShardDatabases::new`).
#[inline]
pub fn registry() -> Option<&'static [Arc<ShardDbSet>]> {
    L4_REGISTRY.get().map(|b| &**b)
}

/// This shard's database set, or `None` if the registry is absent or the shard
/// id is out of range.
#[inline]
pub fn shard_dbs(shard_id: usize) -> Option<&'static Arc<ShardDbSet>> {
    registry()?.get(shard_id)
}

/// L4 S4 master switch, resolved once from `--cross-shard-fast-path`.
///
/// A process-global `AtomicBool` rather than a field threaded through the
/// connection context, matching the existing
/// `replication::state::fanout_hint_active()` hint: the read is one Relaxed
/// load on the hot routing path, and the value never changes after startup.
static CROSS_SHARD_FAST_PATH: AtomicBool = AtomicBool::new(false);

/// Set the S4 master switch from config. Call once, before shards spawn.
pub fn set_cross_shard_fast_path(enabled: bool) {
    CROSS_SHARD_FAST_PATH.store(enabled, Ordering::Relaxed);
}

/// Resolve `--cross-shard-fast-path` into the master switch's value.
///
/// A pure function so the policy is unit-testable; `main.rs` calls it once,
/// before any shard spawns, and turns an `Err` into a startup failure. An
/// unknown value is an error rather than a silent default, so a typo cannot
/// quietly cost an SPSC hop per cross-shard read.
///
/// # `auto`
///
/// Enabled when BOTH hold:
///
/// * `num_shards > 1` — at one shard every key is local, so the path is
///   unreachable and the switch is noise;
/// * the monoio handler is compiled in — the dispatch site lives in
///   `handler_monoio`. On the tokio leg `handler_sharded` still routes every
///   cross-shard read through SPSC, so enabling it there does nothing at all
///   (moon#776). `auto` declines instead of lighting a switch that lies.
///
/// `on` forces it regardless, so the tokio leg can still be told to set the
/// flag (and still gets `main.rs`'s no-op warning).
///
/// # `num_shards` must be the RESOLVED count
///
/// Never `config.shards`, which is `0` for `--shards 0` (auto-detect). Passing
/// the unresolved value makes `auto` decline on exactly the multi-core hosts
/// this path exists for, and nothing else would notice. `0` therefore resolves
/// to `false`, which is the safe direction and is pinned by a test.
pub fn resolve_cross_shard_fast_path(
    mode: &str,
    num_shards: usize,
    monoio_handler: bool,
) -> Result<bool, String> {
    match mode {
        "auto" => Ok(num_shards > 1 && monoio_handler),
        "on" | "yes" => Ok(true),
        "off" | "no" => Ok(false),
        other => Err(format!(
            "--cross-shard-fast-path must be one of auto|on|off (got {other:?})"
        )),
    }
}

/// Whether a cross-shard read may be served under a shared guard on the
/// calling thread instead of hopping through SPSC.
#[inline]
pub fn cross_shard_fast_path_enabled() -> bool {
    CROSS_SHARD_FAST_PATH.load(Ordering::Relaxed)
}

/// Number of shards the registry was built for. `0` when absent.
#[inline]
pub fn registry_shard_count() -> usize {
    registry().map_or(0, |r| r.len())
}

// ── compile-time contract, pinned forever ──
//
// The whole design rests on `Database` being `Send + Sync`. If anyone later
// adds a non-`Sync` field to `Database`, this fails to COMPILE rather than
// silently making the shared plane unsound. This is the guard that can report
// its own failure.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Database>();
};

#[cfg(test)]
mod tests {
    use super::*;

    /// moon#1068: a script holding its own db waits for a FOREIGN reader of
    /// the destination db and then gets it — contention is a wait, never a
    /// refusal. Only the structural cases refuse: the db this thread already
    /// holds, and an index out of range.
    #[test]
    fn write_second_waits_for_a_foreign_reader_and_refuses_only_structurally() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        let set = std::sync::Arc::new(set_of(4));
        let own = set.write(0);

        // A foreign shard thread takes a read guard on db 1 and holds it.
        let (held_tx, held_rx) = mpsc::channel();
        let foreign = std::sync::Arc::clone(&set);
        let reader = std::thread::spawn(move || {
            let guard = foreign.try_read(1).expect("db 1 is free");
            held_tx.send(()).expect("signal held");
            std::thread::sleep(Duration::from_millis(50));
            drop(guard);
        });
        held_rx.recv().expect("reader holds db 1");

        let start = Instant::now();
        let second = set.write_second(1);
        let waited = start.elapsed();
        assert!(
            second.is_some(),
            "contention must be waited out, not refused"
        );
        assert!(
            waited >= Duration::from_millis(30),
            "the guard was granted while the foreign reader still held db 1 ({waited:?})"
        );
        drop(second);
        reader.join().expect("reader thread");

        // Structural refusals: the db already held here, and a missing db.
        assert!(
            set.write_second(0).is_none(),
            "re-entry must refuse, not deadlock"
        );
        assert!(set.write_second(9).is_none(), "out of range must refuse");
        drop(own);
        // Once released, the same index is available again.
        assert!(set.write_second(0).is_some());
    }

    fn set_of(db_count: usize) -> ShardDbSet {
        let dbs: Box<[CachePadded<RwLock<Database>>]> = (0..db_count)
            .map(|_| CachePadded::new(RwLock::new(Database::new())))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        ShardDbSet { shard_id: 0, dbs }
    }

    #[test]
    fn auto_enables_the_fast_path_on_monoio_with_more_than_one_shard() {
        // The measured reason this default flipped: at `--shards 8` with a
        // POPULATED keyspace the path serves 100% of foreign reads in place and
        // takes parks/cmd for GET at p=1 from 0.875 to 0.000. Counter ratios
        // (`total_dispatch_cross_read_fast` vs `total_dispatch_cross_spsc`,
        // `total_remote_awaits_parked`), reproduced against DBSIZE:
        //
        //   dbsize  63,114 -> 62.9% in place, parks/cmd 0.325
        //   dbsize  98,169 -> 98.2% in place, parks/cmd 0.016
        //   dbsize 100,000 -> 100.0% in place, parks/cmd 0.000
        //
        // The residual is the benchmark's key MISS rate, not lock contention.
        assert_eq!(resolve_cross_shard_fast_path("auto", 8, true), Ok(true));
    }

    #[test]
    fn auto_declines_where_the_path_cannot_fire() {
        // One shard: every key is local, the branch is unreachable.
        assert_eq!(resolve_cross_shard_fast_path("auto", 1, true), Ok(false));
        // Tokio: `handler_sharded` has no fast-path site (moon#776). A switch
        // that cannot change behaviour must not read as enabled.
        assert_eq!(resolve_cross_shard_fast_path("auto", 8, false), Ok(false));
    }

    #[test]
    fn on_forces_the_switch_and_off_clears_it_regardless_of_shape() {
        for (mode, want) in [("on", true), ("yes", true), ("off", false), ("no", false)] {
            assert_eq!(
                resolve_cross_shard_fast_path(mode, 1, false),
                Ok(want),
                "{mode}"
            );
            assert_eq!(
                resolve_cross_shard_fast_path(mode, 8, true),
                Ok(want),
                "{mode}"
            );
        }
    }

    /// `--shards 0` is auto-detect and leaves `config.shards == 0`. `main.rs`
    /// must pass the RESOLVED count; if it ever regresses to `config.shards`,
    /// `auto` silently disables the fast path on every auto-detect deployment.
    #[test]
    fn an_unresolved_shard_count_declines_rather_than_guessing() {
        assert_eq!(resolve_cross_shard_fast_path("auto", 0, true), Ok(false));
    }

    #[test]
    fn an_unknown_mode_is_a_startup_error_not_a_silent_default() {
        let err = resolve_cross_shard_fast_path("ON ", 8, true).unwrap_err();
        assert!(err.contains("auto|on|off"), "{err}");
    }

    #[test]
    fn out_of_range_panic_keeps_the_message_with_shard_db_documented() {
        let sets = build_sets(vec![vec![], (0..2).map(|_| Database::new()).collect()]);
        let msg = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = sets[1].write(99);
        }))
        .unwrap_err()
        .downcast::<String>()
        .map(|s| *s)
        .unwrap_or_else(|_| String::from("<non-string>"));
        assert!(
            msg.contains("out of bounds"),
            "slice::with_shard_db documents this wording: {msg:?}"
        );
        assert!(
            msg.contains("on shard 1"),
            "the message must name the owning shard: {msg:?}"
        );
    }

    #[test]
    fn db_count_reports_the_slice_length() {
        assert_eq!(set_of(16).db_count(), 16);
        assert_eq!(set_of(1).db_count(), 1);
    }

    #[test]
    fn foreign_try_read_succeeds_when_owner_is_idle() {
        let set = set_of(4);
        assert!(set.try_read(0).is_some());
    }

    #[test]
    fn foreign_try_read_refuses_while_a_writer_holds_the_db() {
        let set = set_of(4);
        let _w = set.dbs[2].write();
        // The whole no-park guarantee: a busy db yields None, not a block.
        assert!(set.try_read(2).is_none());
        // ...and the refusal is scoped to that db, not the shard.
        assert!(set.try_read(0).is_some());
    }

    #[test]
    fn foreign_try_read_out_of_range_is_none_not_panic() {
        let set = set_of(2);
        assert!(set.try_read(99).is_none());
    }

    #[test]
    fn concurrent_shared_reads_coexist() {
        let set = set_of(2);
        let a = set.try_read(0);
        let b = set.try_read(0);
        assert!(a.is_some() && b.is_some(), "shared guards must not exclude");
    }

    #[test]
    #[should_panic(expected = "db guard held recursively")]
    fn reacquiring_the_same_db_on_one_thread_panics() {
        let set = set_of(4);
        let _first = set.read(1);
        let _second = set.read(1); // must panic, not deadlock
    }

    #[test]
    fn distinct_dbs_on_one_thread_are_fine() {
        let set = set_of(4);
        {
            let _a = set.read(0);
            let _b = set.read(1);
            assert_eq!(super::guard_depth::held_mask(), 0b11);
        }
        assert_eq!(
            super::guard_depth::held_mask(),
            0,
            "dropping the guards must clear the mask"
        );
    }

    #[test]
    fn the_mask_is_cleared_by_a_panic_unwind() {
        let set = set_of(4);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = set.read(2);
            panic!("boom");
        }));
        assert!(r.is_err());
        assert_eq!(
            super::guard_depth::held_mask(),
            0,
            "an unwind through a guard must not leak the re-entrancy bit"
        );
    }

    #[test]
    fn write_pair_orders_ascending_but_returns_in_argument_order() {
        let set = set_of(4);
        // Descending arguments must still acquire 1 before 3 internally.
        let (a, b) = set.write_pair(3, 1);
        assert_eq!(super::guard_depth::held_mask(), 0b1010, "dbs 1 and 3 held");
        drop((a, b));
        assert_eq!(super::guard_depth::held_mask(), 0);
    }

    #[test]
    #[should_panic(expected = "distinct databases")]
    fn write_pair_rejects_the_same_db_twice() {
        let set = set_of(4);
        let _ = set.write_pair(2, 2);
    }

    #[test]
    fn write_all_takes_every_db() {
        let set = set_of(8);
        let guards = set.write_all();
        assert_eq!(guards.len(), 8);
        assert_eq!(super::guard_depth::held_mask(), 0xff);
        drop(guards);
        assert_eq!(super::guard_depth::held_mask(), 0);
    }

    #[test]
    fn build_sets_makes_one_set_per_shard_each_with_its_own_dbs() {
        let sets = build_sets(vec![
            (0..16).map(|_| Database::new()).collect(),
            (0..16).map(|_| Database::new()).collect(),
            (0..16).map(|_| Database::new()).collect(),
            (0..16).map(|_| Database::new()).collect(),
        ]);
        assert_eq!(sets.len(), 4, "one ShardDbSet per shard");
        for set in sets.iter() {
            assert_eq!(set.db_count(), 16);
        }
        // The sets must be genuinely independent: a writer on shard 0 db 0
        // must not exclude a reader on shard 1 db 0.
        let _w = sets[0].write(0);
        assert!(
            sets[1].try_read(0).is_some(),
            "shards must not share a lock"
        );
        assert!(
            sets[0].try_read(0).is_none(),
            "same (shard, db) must exclude"
        );
    }

    #[test]
    fn a_write_to_one_db_does_not_exclude_another_db_on_the_same_shard() {
        let sets = build_sets(vec![(0..16).map(|_| Database::new()).collect()]);
        let _w = sets[0].write(0);
        for idx in 1..16 {
            assert!(
                sets[0].try_read(idx).is_some(),
                "db {idx} must stay readable while db 0 is written"
            );
        }
    }
}
