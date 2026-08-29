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

use crossbeam_utils::CachePadded;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use smallvec::SmallVec;

use crate::storage::Database;

/// One shard's databases, shared for the L4 read plane.
pub struct ShardDbSet {
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

    #[inline]
    fn slot(&self, idx: usize) -> &CachePadded<RwLock<Database>> {
        match self.dbs.get(idx) {
            Some(cell) => cell,
            None => panic!(
                "db index {idx} out of range (shard has {} databases)",
                self.dbs.len()
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
    L4_REGISTRY.set(build_sets(shard_databases)).is_ok()
}

/// Build the per-shard sets without touching the global — the construction the
/// registry installs, exposed so it is testable without poisoning a process-
/// wide `OnceLock` for every other test in the binary.
pub(crate) fn build_sets(shard_databases: Vec<Vec<Database>>) -> Box<[Arc<ShardDbSet>]> {
    shard_databases
        .into_iter()
        .map(|dbs| {
            let padded: Box<[CachePadded<RwLock<Database>>]> = dbs
                .into_iter()
                .map(|db| CachePadded::new(RwLock::new(db)))
                .collect::<Vec<_>>()
                .into_boxed_slice();
            Arc::new(ShardDbSet { dbs: padded })
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

    fn set_of(db_count: usize) -> ShardDbSet {
        let dbs: Box<[CachePadded<RwLock<Database>>]> = (0..db_count)
            .map(|_| CachePadded::new(RwLock::new(Database::new())))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        ShardDbSet { dbs }
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
