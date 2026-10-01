//! An automatic snapshot is abandoned, whole, when any shard finds an
//! uncommitted `TXN` write in memory as it starts its part (moon#1289 R2,
//! review N1).
//!
//! [`super::request`]'s `txn_open` check is a pre-filter: it runs once, on
//! the requesting thread, and each shard starts its part of the snapshot
//! later, at its own tick. A `TXN` whose `BEGIN` and first write land in
//! between would be serialized, and without an AOF a crash-restart from that
//! image brings the aborted write back. So each shard checks again at the
//! instant it starts its part of an automatic snapshot, on its own thread:
//!
//! - **Its own hold table.** `transaction::isolation` holds every `(db, key)`
//!   a `TXN` wrote, from before the write is dispatched until the abort's
//!   restore is applied (or the commit); a `TXN` writes only on its
//!   connection's shard (#499). A shard with no hold at its start has no
//!   uncommitted write in memory.
//! - **What lands after the start** is covered by the snapshot's
//!   copy-on-write: every `TXN` write reaches the keyspace through
//!   `command::dispatch` (both runtimes' `capture_conn_write` legs), a
//!   script's `redis.call`, or the abort's `kv_compensation::undo_one`, and
//!   each captures the key's epoch-start image first, first capture wins
//!   (`persistence::snapshot_cow`). The file holds the pre-`TXN` value.
//!
//! A shard that finds a hold abandons the ROUND: it does not start, and no
//! shard publishes. A shard's file is renamed into place by its own writer
//! thread, so the shards that did start wait before their publish until
//! every shard has passed its start ([`ShardCommit::Wait`]), and then
//! either all go ahead or all drop their temp file. An abandoned round moves
//! nothing: no file, no `LASTSAVE`, no held file released (the hold's epoch
//! only commits on a published snapshot), and the spacing slot the request
//! took is given back so a later sweep asks again.
//!
//! Only rounds registered by [`register`] are checked: `BGSAVE`, `SAVE`, the
//! save rules and `SHUTDOWN`'s save are WS42's (moon#1300).

use std::sync::atomic::{AtomicU64, Ordering};

use super::SnapshotReason;

/// What a shard does with the epoch it is about to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardStart {
    /// Not an automatic round: start as always.
    NotAutomatic,
    /// Automatic, and this shard holds nothing: start.
    Proceed,
    /// The round is abandoned (by this shard, or an earlier one): do not
    /// start, report the part abandoned.
    Abandon,
}

/// What a shard whose walk is done does before it publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardCommit {
    /// Publish (not automatic, or every shard passed its start).
    Go,
    /// Some shard has not started its part yet: poll again next tick.
    Wait,
    /// The round was abandoned: drop the temp file, publish nothing.
    Abandon,
}

/// One automatic snapshot round.
#[derive(Debug, Clone, Copy)]
struct Round {
    epoch: u64,
    reason: SnapshotReason,
    /// Shards taking part (the fan-in's count).
    shards: usize,
    /// Shards past their start decision.
    started: usize,
    abandoned: bool,
    /// The gate slot [`super::request`] took, given back on abandon.
    slot_ms: u64,
}

/// A shard abandoned the round for the first time: what the caller reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Abandoned {
    pub(crate) reason: SnapshotReason,
    pub(crate) slot_ms: u64,
}

/// The current round. One at a time: rounds are sharded saves, and
/// `SAVE_IN_PROGRESS` admits one save at a time.
pub(crate) struct Rounds {
    round: parking_lot::Mutex<Option<Round>>,
    /// The epoch of the round last abandoned (0: none), so a walk can stop
    /// early with one load instead of a lock per tick.
    abandoned_epoch: AtomicU64,
}

impl Rounds {
    pub(crate) const fn new() -> Self {
        Self {
            round: parking_lot::Mutex::new(None),
            abandoned_epoch: AtomicU64::new(0),
        }
    }

    /// Epoch `epoch` is an automatic snapshot for `reason` across `shards`
    /// shards. Call before the epoch is broadcast.
    pub(crate) fn register(&self, epoch: u64, reason: SnapshotReason, shards: usize, slot_ms: u64) {
        *self.round.lock() = Some(Round {
            epoch,
            reason,
            shards,
            started: 0,
            abandoned: false,
            slot_ms,
        });
    }

    /// A shard is about to start its part of `epoch`; `txn_held` is its own
    /// hold table's answer. `Some(abandoned)` when this call abandoned the
    /// round (the first shard that found a hold).
    pub(crate) fn shard_starts(
        &self,
        epoch: u64,
        txn_held: bool,
    ) -> (ShardStart, Option<Abandoned>) {
        let mut guard = self.round.lock();
        let Some(round) = guard.as_mut().filter(|r| r.epoch == epoch && epoch != 0) else {
            return (ShardStart::NotAutomatic, None);
        };
        round.started += 1;
        if round.abandoned {
            return (ShardStart::Abandon, None);
        }
        if !txn_held {
            return (ShardStart::Proceed, None);
        }
        round.abandoned = true;
        self.abandoned_epoch.store(epoch, Ordering::Release);
        let first = Abandoned {
            reason: round.reason,
            slot_ms: round.slot_ms,
        };
        (ShardStart::Abandon, Some(first))
    }

    /// A shard's walk of `epoch` is done: may it publish?
    pub(crate) fn shard_commit(&self, epoch: u64) -> ShardCommit {
        let guard = self.round.lock();
        match guard.as_ref().filter(|r| r.epoch == epoch && epoch != 0) {
            None => ShardCommit::Go,
            Some(r) if r.abandoned => ShardCommit::Abandon,
            Some(r) if r.started < r.shards => ShardCommit::Wait,
            Some(_) => ShardCommit::Go,
        }
    }

    /// Was `epoch` abandoned? One load: a walk still in progress stops.
    #[inline]
    pub(crate) fn is_abandoned(&self, epoch: u64) -> bool {
        epoch != 0 && self.abandoned_epoch.load(Ordering::Acquire) == epoch
    }
}

static ROUNDS: Rounds = Rounds::new();

/// [`Rounds::register`] on the process's rounds.
pub(crate) fn register(epoch: u64, reason: SnapshotReason, shards: usize, slot_ms: u64) {
    ROUNDS.register(epoch, reason, shards, slot_ms);
}

/// A shard starts its part of `epoch`: checks THIS thread's hold table when
/// the epoch is an automatic round. On the first abandon the spacing slot is
/// given back and the abandon is counted.
pub(crate) fn shard_starts(epoch: u64, shard_id: usize) -> ShardStart {
    let held = crate::transaction::isolation::any_held();
    let (start, first) = ROUNDS.shard_starts(epoch, held);
    if let Some(first) = first {
        super::note_abandoned(first.reason, first.slot_ms);
        tracing::warn!(
            shard = shard_id,
            epoch,
            "automatic snapshot abandoned: a TXN on this shard holds uncommitted writes \
             at the snapshot's start; nothing is published and the request is retried at \
             a later sweep (moon#1289, moon#1300)"
        );
    }
    start
}

/// [`Rounds::shard_commit`] on the process's rounds.
pub(crate) fn shard_commit(epoch: u64) -> ShardCommit {
    ROUNDS.shard_commit(epoch)
}

/// [`Rounds::is_abandoned`] on the process's rounds.
#[inline]
pub(crate) fn is_abandoned(epoch: u64) -> bool {
    ROUNDS.is_abandoned(epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    const R: SnapshotReason = SnapshotReason::HeldColdFiles;

    #[test]
    fn an_unregistered_epoch_is_not_automatic_and_always_publishes() {
        let rounds = Rounds::new();
        assert_eq!(rounds.shard_starts(7, true).0, ShardStart::NotAutomatic);
        assert_eq!(rounds.shard_commit(7), ShardCommit::Go);
        rounds.register(8, R, 2, 100);
        assert_eq!(
            rounds.shard_starts(7, true).0,
            ShardStart::NotAutomatic,
            "a user BGSAVE's epoch is never checked, even while a round is registered"
        );
        assert_eq!(rounds.shard_commit(7), ShardCommit::Go);
        assert!(!rounds.is_abandoned(7));
    }

    #[test]
    fn no_shard_publishes_before_every_shard_passed_its_start() {
        let rounds = Rounds::new();
        rounds.register(3, R, 3, 100);
        assert_eq!(rounds.shard_starts(3, false), (ShardStart::Proceed, None));
        assert_eq!(rounds.shard_commit(3), ShardCommit::Wait);
        assert_eq!(rounds.shard_starts(3, false), (ShardStart::Proceed, None));
        assert_eq!(rounds.shard_commit(3), ShardCommit::Wait);
        assert_eq!(rounds.shard_starts(3, false), (ShardStart::Proceed, None));
        assert_eq!(rounds.shard_commit(3), ShardCommit::Go);
        assert!(!rounds.is_abandoned(3));
    }

    /// The N1 shape: shard 0 started and walked; shard 1's TXN wrote before
    /// shard 1's start. Nobody publishes, and only the first abandon is
    /// reported (one counter bump, one slot give-back).
    #[test]
    fn a_hold_at_any_shards_start_abandons_the_whole_round() {
        let rounds = Rounds::new();
        rounds.register(4, R, 3, 1_234);
        assert_eq!(rounds.shard_starts(4, false).0, ShardStart::Proceed);
        assert_eq!(
            rounds.shard_starts(4, true),
            (
                ShardStart::Abandon,
                Some(Abandoned {
                    reason: R,
                    slot_ms: 1_234
                })
            )
        );
        assert!(rounds.is_abandoned(4));
        assert_eq!(rounds.shard_commit(4), ShardCommit::Abandon, "shard 0");
        assert_eq!(
            rounds.shard_starts(4, false),
            (ShardStart::Abandon, None),
            "a later shard does not start an abandoned round"
        );
        assert_eq!(
            rounds.shard_starts(4, true),
            (ShardStart::Abandon, None),
            "reported once"
        );
    }

    /// A new round starts clean; the abandoned epoch stays abandoned.
    #[test]
    fn a_new_round_is_not_abandoned_by_the_last_one() {
        let rounds = Rounds::new();
        rounds.register(5, R, 1, 10);
        assert_eq!(rounds.shard_starts(5, true).0, ShardStart::Abandon);
        rounds.register(6, R, 1, 20);
        assert!(!rounds.is_abandoned(6));
        assert!(rounds.is_abandoned(5));
        assert_eq!(rounds.shard_starts(6, false).0, ShardStart::Proceed);
        assert_eq!(rounds.shard_commit(6), ShardCommit::Go);
    }

    /// The process-wide entry point reads the CALLING shard thread's hold
    /// table. Epochs no server reaches, on fresh threads (the table is
    /// thread-local); serialized, since the round slot is process-wide.
    #[test]
    fn the_start_check_reads_this_shards_own_holds() {
        static SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
        let _serial = SERIAL.lock();
        let (quiet, busy) = (u64::MAX - 101, u64::MAX - 102);
        std::thread::spawn(move || {
            register(quiet, R, 1, 0);
            assert_eq!(shard_starts(quiet, 0), ShardStart::Proceed);
            assert_eq!(shard_commit(quiet), ShardCommit::Go);
        })
        .join()
        .unwrap();
        std::thread::spawn(move || {
            use crate::transaction::isolation;
            register(busy, R, 2, 0);
            isolation::txn_begin(9);
            assert!(isolation::hold(0, &bytes::Bytes::from_static(b"k"), 9));
            assert_eq!(shard_starts(busy, 0), ShardStart::Abandon);
            assert!(is_abandoned(busy));
            assert_eq!(shard_commit(busy), ShardCommit::Abandon);
            isolation::txn_end(9);
        })
        .join()
        .unwrap();
    }

    /// moon#1297: a cold-reclaim round is checked and abandoned like a
    /// held-file one, and reports its own reason (its INFO counter and its
    /// slot).
    #[test]
    fn a_cold_reclaim_round_is_abandoned_under_its_own_reason() {
        let rounds = Rounds::new();
        let reclaim = SnapshotReason::ColdReclaim;
        rounds.register(9, reclaim, 2, 4_321);
        assert_eq!(rounds.shard_starts(9, false).0, ShardStart::Proceed);
        assert_eq!(rounds.shard_commit(9), ShardCommit::Wait);
        assert_eq!(
            rounds.shard_starts(9, true),
            (
                ShardStart::Abandon,
                Some(Abandoned {
                    reason: reclaim,
                    slot_ms: 4_321
                })
            )
        );
        assert_eq!(rounds.shard_commit(9), ShardCommit::Abandon);
    }

    #[test]
    fn epoch_zero_is_never_a_round() {
        let rounds = Rounds::new();
        rounds.register(0, R, 1, 10);
        assert_eq!(rounds.shard_starts(0, true).0, ShardStart::NotAutomatic);
        assert!(!rounds.is_abandoned(0));
    }
}
