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

use crate::persistence::aof::{AOF_SPSC_BACKPRESSURE_BOUND, AofWriterPool, FsyncPolicy};
use crate::protocol::Frame;
use crate::replication::state::ReplicationState;

/// The reply a write gets when its record did not reach the AOF, or its
/// `always` fsync did not confirm.
const WRITEFAIL: &[u8] = b"WRITEFAIL aof fsync failed";

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
    /// Reply slots whose record never reached the writer.
    failed_idxs: Vec<usize>,
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
    /// replies become `WRITEFAIL`, as they did when the enqueue itself
    /// waited.
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
        } else {
            self.failed_idxs.push(resp_idx);
        }
        if let Some(counter) = self.change_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        sent
    }

    /// Confirm the batch: under `always`, ONE fsync barrier for every record
    /// enqueued so far (group commit). Patches the reply of every write whose
    /// record was not enqueued or not confirmed with `WRITEFAIL`. Returns
    /// whether anything failed. Leaves the log empty for the next stretch.
    pub(crate) async fn settle(&mut self, responses: &mut [Frame]) -> bool {
        let mut failed = !self.failed_idxs.is_empty();
        for idx in self.failed_idxs.drain(..) {
            if let Some(slot) = responses.get_mut(idx) {
                *slot = Frame::Error(Bytes::from_static(WRITEFAIL));
            }
        }
        if !self.barrier_idxs.is_empty() {
            let synced = match self.pool {
                Some(pool) => pool.fsync_barrier(0).await.is_ok(),
                None => true,
            };
            if !synced {
                failed = true;
                for &idx in &self.barrier_idxs {
                    if let Some(slot) = responses.get_mut(idx) {
                        *slot = Frame::Error(Bytes::from_static(WRITEFAIL));
                    }
                }
            }
            self.barrier_idxs.clear();
        }
        failed
    }
}
