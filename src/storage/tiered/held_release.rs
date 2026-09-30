//! A trigger for held cold files (moon#1289).
//!
//! A dead spill file that the unlink hold covers ([`super::unlink_hold`],
//! [`super::snapshot_hold`]) is released only by a committed fold (with an AOF)
//! or a snapshot that STARTED after it went zero-ref (without one). Neither was
//! ever asked for: the auto-rewrite monitor folds for growth (min 64 MB), a
//! forced rewrite, or ledger pressure (`ledger > maxmemory/4`), and without an
//! AOF nothing snapshots. A server whose hold covers live spills (any fold cut
//! that moved `hold_below` above them) kept the dead files on disk until an
//! operator ran `BGREWRITEAOF` / `BGSAVE`; `INFO cold_files_pending_unlink`
//! counts them.
//!
//! # The rule
//!
//! After each orphan sweep the shard asks every database whether a held file
//! still waits for a fold (`UnlinkHold::awaits_fold`) and how many consecutive
//! sweeps have said so at the SAME committed floor ([`HeldWait`]). A database
//! that has seen it [`STALE_AFTER_SWEEPS`] times running is *stale*: its held
//! files waited at least two whole sweep intervals, so no fold or snapshot is
//! coming on its own.
//!
//! - **With an AOF** a stale database raises a signal the auto-rewrite monitor
//!   answers with a fold (`aof::auto_rewrite`, the ledger-pressure path
//!   `held_files_pressure` uses), at most once per [`spacing`].
//! - **Without one** the shard requests a snapshot through
//!   [`crate::persistence::snapshot_request`], gated to one per [`spacing`]
//!   across the process.
//!
//! Nothing new runs on a timer: the check rides the sweep's own cadence
//! (`runtime::interval` on tokio, `tick_cadence::Cadence` on monoio), which
//! already differs between the runtimes only in when the first sweep runs.
//! A committed fold or snapshot releases the files at the sweep after it, and
//! the next sweep finds nothing awaiting and resets the count. A server with no
//! held file counts nothing, signals nothing and folds nothing.
//!
//! # The defaults
//!
//! The unit is the sweep interval (`--cold-orphan-sweep-interval-secs`, 60 s
//! by default), the one existing knob for "how often does the cold tier get
//! looked at", so nothing new is configurable:
//! - [`STALE_AFTER_SWEEPS`] = 3: seen held at three sweeps running, i.e. at
//!   least two whole intervals (2 min by default). A fold or snapshot already
//!   running when the file went zero-ref commits well inside that and releases
//!   the file at the next sweep, so the trigger never races it; longer would
//!   leave the disk held for minutes for no gain.
//! - [`SPACING_SWEEPS`] = 10 intervals between requests (10 min by default).
//!   A fold rewrites the whole base and a snapshot writes the whole keyspace;
//!   under continuous churn (files die faster than a fold can cover them) this
//!   bounds the cost to six a hour, the order of an ordinary `--save` rule.
//!   It is measured from the last request of ANY reason.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use super::cold_index::ColdIndex;

/// Consecutive sweeps a database must see a held file awaiting a fold, at one
/// committed floor, before it is stale (see the module docs).
pub const STALE_AFTER_SWEEPS: u32 = 3;

/// Sweep intervals between two requests for a fold or a snapshot.
pub const SPACING_SWEEPS: u64 = 10;

/// `--cold-orphan-sweep-interval-secs`'s default, until a shard reports the
/// configured one ([`configure`]).
const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 60;

static SPACING_MS: AtomicU64 = AtomicU64::new(DEFAULT_SWEEP_INTERVAL_SECS * SPACING_SWEEPS * 1_000);

/// Databases (over every shard) that are stale now.
static STALE_DATABASES: AtomicUsize = AtomicUsize::new(0);

/// Folds the auto-rewrite monitor dispatched because of a stale database.
static FOLDS_REQUESTED: AtomicU64 = AtomicU64::new(0);

/// Adopt the configured sweep interval: the spacing is [`SPACING_SWEEPS`] of
/// it. Idempotent, called by every shard after its sweep. `0` (sweeps off)
/// changes nothing: with no sweep nothing is ever stale.
pub fn configure(sweep_interval_secs: u64) {
    if sweep_interval_secs > 0 {
        SPACING_MS.store(
            sweep_interval_secs.saturating_mul(SPACING_SWEEPS) * 1_000,
            Ordering::Relaxed,
        );
    }
}

/// The minimum time between two requests (see the module docs).
#[must_use]
pub fn spacing() -> Duration {
    Duration::from_millis(SPACING_MS.load(Ordering::Relaxed))
}

/// Databases with held files that no fold or snapshot is coming for,
/// process-wide (INFO `cold_held_files_stale_databases`; the auto-rewrite
/// monitor's input).
#[must_use]
pub fn stale_databases() -> usize {
    STALE_DATABASES.load(Ordering::Relaxed)
}

/// Folds requested for held files, process-wide (INFO
/// `cold_held_release_folds_requested`).
#[must_use]
pub fn folds_requested() -> u64 {
    FOLDS_REQUESTED.load(Ordering::Relaxed)
}

/// The auto-rewrite monitor dispatched a fold because of a stale database.
pub fn note_fold_requested() {
    FOLDS_REQUESTED.fetch_add(1, Ordering::Relaxed);
}

/// Per-database count of consecutive sweeps that saw a held file awaiting a
/// fold. Lives in `ReclaimState`, so it goes with its index (a `SWAPDB` or
/// `FLUSHALL` that replaces the index drops the count and the signal).
#[derive(Debug, Default)]
pub struct HeldWait {
    /// Consecutive sweeps, this one included, that saw a held file awaiting
    /// a fold at `floor`. `0` = none.
    sweeps: u32,
    /// The committed floor those sweeps saw.
    floor: u64,
    /// Whether this database counts in [`STALE_DATABASES`].
    stale: bool,
}

impl HeldWait {
    /// One sweep's observation. `awaiting`: some held file's stamp is not
    /// below the committed `floor`. Returns whether the database is stale.
    ///
    /// A committed fold that moves the floor restarts the count: what still
    /// awaits was stamped after that fold began, so it has not waited yet.
    pub fn observe(&mut self, awaiting: bool, floor: u64) -> bool {
        if !awaiting {
            self.sweeps = 0;
        } else if self.sweeps == 0 || self.floor != floor {
            self.sweeps = 1;
            self.floor = floor;
        } else {
            self.sweeps = self.sweeps.saturating_add(1);
        }
        self.set_stale(self.sweeps >= STALE_AFTER_SWEEPS);
        self.stale
    }

    fn set_stale(&mut self, stale: bool) {
        if stale == self.stale {
            return;
        }
        self.stale = stale;
        if stale {
            STALE_DATABASES.fetch_add(1, Ordering::Relaxed);
        } else {
            STALE_DATABASES.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for HeldWait {
    fn drop(&mut self) {
        self.set_stale(false);
    }
}

impl ColdIndex {
    /// The orphan sweep's check that a held file is not waiting for nothing
    /// (module docs). `committed_floor` is the view's committed floor;
    /// `floor_covers_shard` is `false` where no committed fold can cover this
    /// shard (a TopLevel AOF layout with several shards, which no shipped
    /// configuration builds): held files are never released there, so a
    /// trigger would fold forever for nothing.
    ///
    /// Returns whether this database is stale after this sweep.
    pub fn note_held_sweep(&mut self, committed_floor: u64, floor_covers_shard: bool) -> bool {
        let awaiting = floor_covers_shard && self.hold.awaits_fold(committed_floor);
        self.reclaim.held_wait.observe(awaiting, committed_floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stale count is process-wide and other tests' indexes bump it, so
    /// these tests read the per-object flag, never the global.
    #[test]
    fn a_held_file_is_stale_after_three_sweeps_at_one_floor() {
        let mut w = HeldWait::default();
        assert!(!w.observe(true, 4));
        assert!(!w.observe(true, 4));
        assert!(w.observe(true, 4), "held at three sweeps running");
        assert!(w.observe(true, 4), "and it stays stale while it waits");
    }

    #[test]
    fn nothing_held_is_never_stale() {
        let mut w = HeldWait::default();
        for _ in 0..20 {
            assert!(!w.observe(false, 4));
        }
    }

    /// A fold that commits moves the floor: whatever still awaits was stamped
    /// after it began and has not waited yet.
    #[test]
    fn a_committed_fold_restarts_the_count() {
        let mut w = HeldWait::default();
        w.observe(true, 4);
        w.observe(true, 4);
        assert!(w.observe(true, 4));
        assert!(!w.observe(true, 6), "floor moved: a fresh wait");
        assert!(!w.observe(true, 6));
        assert!(w.observe(true, 6));
    }

    /// The release resets it: the files are gone at the sweep after the fold.
    #[test]
    fn a_release_clears_the_signal() {
        let mut w = HeldWait::default();
        for _ in 0..3 {
            w.observe(true, 4);
        }
        assert!(w.stale);
        assert!(!w.observe(false, 6));
        assert!(!w.stale);
        assert!(!w.observe(true, 6), "a later hold starts from one");
    }

    /// The process-wide count moves only on a transition, once each way, and
    /// a dropped stale wait withdraws its signal (the global is shared with
    /// every other test's indexes, so the pairing is asserted on the flag).
    #[test]
    fn the_stale_signal_is_counted_once_and_withdrawn_on_drop() {
        let mut w = HeldWait::default();
        w.set_stale(true);
        w.set_stale(true);
        assert!(w.stale);
        w.set_stale(false);
        w.set_stale(false);
        assert!(!w.stale);
        w.set_stale(true);
        drop(w); // Drop withdraws: a second decrement would underflow the count.
    }

    #[test]
    fn the_spacing_follows_the_configured_sweep_interval() {
        configure(0);
        let before = spacing();
        configure(1);
        assert_eq!(spacing(), Duration::from_secs(SPACING_SWEEPS));
        configure(60);
        assert_eq!(
            spacing(),
            Duration::from_secs(60 * SPACING_SWEEPS),
            "10 minutes by default"
        );
        configure(0);
        assert_eq!(spacing(), Duration::from_secs(60 * SPACING_SWEEPS));
        let _ = before;
    }
}
