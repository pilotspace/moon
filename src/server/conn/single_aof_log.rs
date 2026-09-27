//! moon#1099: `handler_single` logs each write in the stretch that applies it.
//!
//! `handler_single` shares `Arc<Vec<RwLock<Database>>>` between connections
//! that `tokio::spawn` puts on a multi-thread runtime. It used to collect a
//! pipelined batch's AOF records and send them after the batch, once every db
//! guard was released and after the batch's awaits (WAIT, CLIENT PAUSE, the
//! reply writes under `everysec`). Another connection's write, applied later
//! but in a batch that finished first, was then logged first, and replay put
//! the older value back.
//!
//! The contract is now the one the sharded handlers keep (#1084): a record is
//! enqueued while the guard of the db it mutated is still held, so log order
//! is apply order. Only the fsync barrier of `appendfsync always` stays per
//! batch — it confirms what is already in the queue and orders nothing.
//!
//! Enqueueing under a `parking_lot` guard must not park, so a producer first
//! awaits room ([`AofWriterPool::await_append_room`]) with no guard held, then
//! takes the guard, applies and enqueues synchronously. Room can still be
//! taken by another producer between the two on a multi-thread runtime; that
//! residual race falls to [`AofWriterPool::send_append_bounded_blocking`],
//! whose short block is shared by the whole batch (the writer drains on
//! another worker meanwhile).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;

use crate::persistence::aof::{
    AOF_BACKLOG_ERR, AOF_FSYNC_ERR, AOF_SPSC_BACKPRESSURE_BOUND, AofWriterPool, FsyncPolicy,
};
use crate::protocol::Frame;
use crate::replication::state::ReplicationState;

/// How a batch's AOF bookkeeping settled ([`SingleAofLog::settle`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settled {
    /// Every record reached the writer (and, under `always`, is on disk).
    Clean,
    /// Only writer-backlog refusals (moon#1272): each such reply now reads
    /// [`crate::persistence::aof::AOF_BACKLOG_ERR`]; the rest of the batch
    /// stands and the connection carries on, like the sharded handlers.
    Backlogged,
    /// A record hit a gone writer, or the `always` barrier failed: those
    /// replies read [`crate::persistence::aof::AOF_FSYNC_ERR`].
    NotDurable,
}

/// Per-batch AOF bookkeeping for `handler_single`.
///
/// [`Self::append_locked`] enqueues one record while the caller holds the
/// guard it applied under; [`Self::settle`] runs the batch's one fsync
/// barrier and patches the replies whose record did not make it.
pub(crate) struct SingleAofLog<'a> {
    pool: Option<&'a AofWriterPool>,
    repl_state: &'a Option<Arc<parking_lot::RwLock<ReplicationState>>>,
    change_counter: &'a Option<Arc<AtomicU64>>,
    /// Blocking budget shared by the whole batch, spent only when admission
    /// lost a race for the last free slots.
    budget: Duration,
    /// Reply slots whose record is enqueued and, under `always`, awaits the
    /// barrier.
    barrier_idxs: Vec<usize>,
    /// Reply slots whose record never reached the writer, with their reply
    /// text: [`AOF_BACKLOG_ERR`] when the writer was backlogged (moon#1272),
    /// [`AOF_FSYNC_ERR`] when it is gone.
    failed_idxs: Vec<(usize, &'static [u8])>,
}

impl<'a> SingleAofLog<'a> {
    /// `pool == None` (persistence off) makes every call a no-op.
    pub(crate) fn new(
        pool: Option<&'a AofWriterPool>,
        repl_state: &'a Option<Arc<parking_lot::RwLock<ReplicationState>>>,
        change_counter: &'a Option<Arc<AtomicU64>>,
    ) -> Self {
        Self {
            pool,
            repl_state,
            change_counter,
            budget: AOF_SPSC_BACKPRESSURE_BOUND,
            barrier_idxs: Vec::new(),
            failed_idxs: Vec::new(),
        }
    }

    /// Whether writes are logged at all.
    #[inline]
    pub(crate) fn enabled(&self) -> bool {
        self.pool.is_some()
    }

    /// Wait, holding no guard, until the writer can take `records` more
    /// appends. Bounded by the pool's `fsync_timeout`. On timeout or a gone
    /// writer this returns anyway: the appends that follow fail and their
    /// replies become [`AOF_BACKLOG_ERR`] (timeout) or [`AOF_FSYNC_ERR`]
    /// (writer gone), as they did when the enqueue itself waited.
    pub(crate) async fn admit(&self, records: usize) {
        if records == 0 {
            return;
        }
        if let Some(pool) = self.pool {
            let _ = pool.await_append_room(0, records).await;
        }
    }

    /// Enqueue the record of a write that was just applied, for reply slot
    /// `resp_idx`, attributed to `db`. Call it while the guard the write
    /// was applied under is still held. Never parks. Returns whether the
    /// record reached the writer (`true` when persistence is off), so a
    /// multi-record effect can stop at the first refusal.
    pub(crate) fn append_locked(&mut self, resp_idx: usize, db: usize, bytes: Bytes) -> bool {
        let Some(pool) = self.pool else {
            return true;
        };
        let lsn = AofWriterPool::issue_append_lsn(self.repl_state, 0, bytes.len());
        let sent = pool.send_append_bounded_blocking(0, lsn, db, bytes, &mut self.budget);
        if sent {
            if pool.fsync_policy() == FsyncPolicy::Always {
                self.barrier_idxs.push(resp_idx);
            }
        } else if pool.append_writer_gone(0) {
            self.failed_idxs.push((resp_idx, AOF_FSYNC_ERR));
        } else {
            // moon#1272: the writer is backlogged, not failing — the write
            // stands in memory without its record (counted as dropped by
            // `send_append_bounded_blocking`); say so, and count it.
            crate::persistence::aof::note_append_backpressure_refusal(
                0,
                "append dropped (write applied in memory, not queued)",
                pool.fsync_timeout(),
            );
            self.failed_idxs.push((resp_idx, AOF_BACKLOG_ERR));
        }
        if let Some(counter) = self.change_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        sent
    }

    /// Confirm the batch: under `always`, ONE fsync barrier for every record
    /// enqueued so far (group commit). Patches the reply of every write whose
    /// record was not enqueued ([`AOF_BACKLOG_ERR`] / [`AOF_FSYNC_ERR`]) or
    /// not confirmed ([`AOF_FSYNC_ERR`]). Leaves the log empty for the next
    /// stretch.
    pub(crate) async fn settle(&mut self, responses: &mut [Frame]) -> Settled {
        let mut outcome = Settled::Clean;
        for (idx, err) in self.failed_idxs.drain(..) {
            outcome = if err == AOF_FSYNC_ERR || outcome == Settled::NotDurable {
                Settled::NotDurable
            } else {
                Settled::Backlogged
            };
            if let Some(slot) = responses.get_mut(idx) {
                *slot = Frame::Error(Bytes::from_static(err));
            }
        }
        if !self.barrier_idxs.is_empty() {
            let synced = match self.pool {
                Some(pool) => pool.fsync_barrier(0).await.is_ok(),
                None => true,
            };
            if !synced {
                outcome = Settled::NotDurable;
                for &idx in &self.barrier_idxs {
                    if let Some(slot) = responses.get_mut(idx) {
                        *slot = Frame::Error(Bytes::from_static(AOF_FSYNC_ERR));
                    }
                }
            }
            self.barrier_idxs.clear();
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aof::{AofMessage, FoldEpoch};
    use crate::runtime::channel;

    fn ok() -> Frame {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    }

    /// moon#1272: a record the BACKLOGGED writer could not take answers
    /// `AOF_BACKLOG_ERR` in its own slot and leaves the batch standing
    /// (`Backlogged`); it used to read `WRITEFAIL aof fsync failed` and end
    /// the connection with `AOF_FSYNC_ERR` although nothing failed on disk.
    #[test]
    fn backlogged_writer_patches_its_slot_with_the_backlog_error() {
        let (tx, _rx) = channel::mpsc_bounded::<AofMessage>(1);
        tx.try_send(AofMessage::Append {
            lsn: 0,
            db: 0,
            bytes: Bytes::from_static(b"filler"),
            epoch: FoldEpoch::INITIAL,
        })
        .expect("pre-fill the only slot");
        let pool = AofWriterPool::top_level_with_policy(
            tx,
            FsyncPolicy::EverySec,
            Duration::from_millis(10),
        );
        let (no_repl, no_counter) = (None, None);
        let mut log = SingleAofLog::new(Some(&*pool), &no_repl, &no_counter);
        let before = crate::persistence::aof::AOF_APPEND_BACKPRESSURE_REFUSALS
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(!log.append_locked(1, 0, Bytes::from_static(b"SET k v")));
        let mut responses = vec![ok(), ok()];
        let settled = futures::executor::block_on(log.settle(&mut responses));
        assert_eq!(settled, Settled::Backlogged);
        assert!(matches!(&responses[0], Frame::SimpleString(_)));
        assert!(
            matches!(&responses[1], Frame::Error(e) if e.as_ref() == AOF_BACKLOG_ERR),
            "got {:?}",
            responses[1]
        );
        assert!(
            crate::persistence::aof::AOF_APPEND_BACKPRESSURE_REFUSALS
                .load(std::sync::atomic::Ordering::Relaxed)
                > before
        );
    }

    /// A writer that is GONE is a write failure: the fsync-failure text, and
    /// `NotDurable` (the handler still ends the connection for it).
    #[test]
    fn gone_writer_patches_its_slot_with_the_fsync_error() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(1);
        drop(rx);
        let pool = AofWriterPool::top_level_with_policy(
            tx,
            FsyncPolicy::EverySec,
            Duration::from_millis(10),
        );
        let (no_repl, no_counter) = (None, None);
        let mut log = SingleAofLog::new(Some(&*pool), &no_repl, &no_counter);
        assert!(!log.append_locked(0, 0, Bytes::from_static(b"SET k v")));
        let mut responses = vec![ok()];
        let settled = futures::executor::block_on(log.settle(&mut responses));
        assert_eq!(settled, Settled::NotDurable);
        assert!(matches!(&responses[0], Frame::Error(e) if e.as_ref() == AOF_FSYNC_ERR));
    }
}
