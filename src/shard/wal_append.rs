//! Lossless per-shard WAL-v3 append (moon#1302).
//!
//! Graph, MQ, workspace and temporal records reach a shard's WAL-v3 writer
//! through that shard's bounded append channel (4096 slots,
//! `shard::event_loop`), which the shard's own event loop drains on its 1 ms
//! tick. Every producer runs on the owning shard's thread, and the tick cannot
//! run while one connection's read batch (or one SPSC drain) executes, so a
//! single command that emits more records than the channel's free slots — a
//! 6000-node Cypher `CREATE`, the rollback of a large `TXN`, a `TXN.COMMIT`
//! materializing thousands of `MQ PUBLISH`es — used to lose the rest to an
//! unchecked `try_send`, after the mutation was applied and acknowledged.
//!
//! # Mechanism
//!
//! The channel stays the fast path. When it is FULL, the record goes to this
//! thread's overflow queue instead — unbounded, in memory, on the shard
//! thread — and the tick appends it right after the channel's records:
//! [`drain_into`] empties the channel, then the overflow, in one synchronous
//! stretch. So there is no fixed capacity for one command's records and no
//! drop:
//!
//! * **FIFO.** The overflow only receives while the channel is full, and only
//!   [`drain_into`] empties either, both at once; so "overflow non-empty"
//!   implies "channel full", every later record also overflows, and the
//!   order the WAL sees is exactly the order the records were produced.
//! * **Durability parity.** An overflowed record is appended on the same
//!   tick a channel record produced at the same moment would be, and both
//!   then wait for the same flush — neither is ever durable before the tick,
//!   and replies are not gated on WAL-v3 durability either way.
//! * **Same thread only.** The overflow is per THREAD; a record for shard N
//!   overflows only on the thread that registered as N's owner
//!   ([`register_owner`], called by N's event loop when it wires its sender).
//!   Every graph and MQ producer is on its owner's thread. The one foreign
//!   producer is a workspace `WS CREATE` / `WS DROP` record, pinned to shard
//!   0's stream, appended from another shard's thread by the tokio handler
//!   and the io_uring intercept: it still goes through the channel, and if
//!   shard 0's channel is full at that instant it is refused
//!   ([`Refused::NotOwner`]) and counted — never queued where the owner
//!   cannot drain it. A foreign record cannot jump the owner's overflow:
//!   it can only enter the channel once the owner's drain freed a slot, and
//!   that drain appends the whole overflow before the owner produces again.
//!   The overflow is thread-local and the counters are `Relaxed`
//!   statistics, so there is no new cross-thread protocol (the channel is
//!   flume's).
//!
//! What remains a refusal: a closed channel (the event loop is gone — the
//! writer genuinely cannot take the record) and the foreign-thread case.
//! The unchecked appenders count those in
//! `reclamation_wal_append_channel_dropped_total`; the checked ones (a TXN
//! rollback, SWAPDB) report them to their caller.
//!
//! Why not hand the connection the writer itself: the writer is a local of
//! the event loop, lent by `&mut` to the SPSC drain (whose GRAPH.* arm is
//! itself a producer) and to the checkpoint/sync ticks. Sharing it would put
//! a `RefCell` borrow on ~40 event-loop sites and still need a queue for a
//! producer that runs while the loop holds that borrow. The overflow gives
//! the same order and durability with neither.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::persistence::wal_v3::record::WalRecordType;
use crate::persistence::wal_v3::segment::WalWriterV3;
use crate::runtime::channel::{MpscReceiver, MpscSender};

/// One record on a shard's WAL append channel: its REAL outer type (K1a) and
/// its unframed payload.
pub type WalAppendMsg = (WalRecordType, Bytes);

/// Records that took the overflow path because the channel was full
/// (cumulative). Before moon#1302 each of these was dropped. Exposed as
/// `reclamation_wal_append_overflow_total` in `INFO reclamation`; a steady
/// climb means commands routinely emit bursts past the channel's capacity.
pub static WAL_APPEND_OVERFLOW_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Once the overflow drained, a queue that grew past this many slots is
/// released instead of kept for the next burst.
const OVERFLOW_KEEP_CAPACITY: usize = 4096;

/// No shard owns this thread.
const NO_OWNER: usize = usize::MAX;

thread_local! {
    /// The shard whose event loop runs on this thread, once it wired its WAL
    /// append sender.
    static OWNER: Cell<usize> = const { Cell::new(NO_OWNER) };
    /// Records produced while the channel was full, in production order.
    static OVERFLOW: RefCell<VecDeque<WalAppendMsg>> = const { RefCell::new(VecDeque::new()) };
}

/// Why a record did not reach the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The channel's receiver is gone: the shard's event loop has exited.
    Closed,
    /// The channel is full and this thread is not the record's shard's
    /// owner, so it cannot queue the record where the owner drains it.
    NotOwner,
}

/// Mark this thread as `shard_id`'s owner: the event loop that drains its
/// WAL append channel runs here. Called once, when the loop wires the sender.
pub fn register_owner(shard_id: usize) {
    OWNER.with(|o| o.set(shard_id));
}

/// Enqueue one record for `shard_id`'s WAL through `tx`, never dropping it
/// while the shard's event loop is alive and this is its thread.
#[inline]
pub fn enqueue(
    tx: &MpscSender<WalAppendMsg>,
    shard_id: usize,
    record_type: WalRecordType,
    data: Bytes,
) -> Result<(), Refused> {
    match tx.try_send((record_type, data)) {
        Ok(()) => Ok(()),
        Err(flume::TrySendError::Full(msg)) => overflow(shard_id, msg),
        Err(flume::TrySendError::Disconnected(_)) => Err(Refused::Closed),
    }
}

#[cold]
#[inline(never)]
fn overflow(shard_id: usize, msg: WalAppendMsg) -> Result<(), Refused> {
    if OWNER.with(Cell::get) != shard_id {
        return Err(Refused::NotOwner);
    }
    OVERFLOW.with(|q| q.borrow_mut().push_back(msg));
    WAL_APPEND_OVERFLOW_TOTAL.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Count and log a record an UNCHECKED appender could not enqueue — its
/// mutation is applied but missing from crash recovery. Logged at the first
/// drop and then at every power of two, so a dead writer cannot flood the log.
#[cold]
pub fn report_dropped(why: Refused, shard_id: usize, record_type: WalRecordType) {
    let n = crate::command::info_reclamation::RECL_WAL_APPEND_CHANNEL_DROPPED_TOTAL
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    if n.is_power_of_two() {
        tracing::error!(
            shard_id,
            ?record_type,
            ?why,
            dropped_total = n,
            "WAL append refused a record AFTER its in-memory mutation was applied — it \
             will be MISSING from crash recovery (the shard's WAL writer is gone, or the \
             record was produced off its shard's thread)"
        );
    }
}

/// Append every queued record to `wal` in production order: the channel's,
/// then the overflow's. The shard's 1 ms tick (and its shutdown path) calls
/// this; `wal` is `None` only when persistence is off, and then the sender is
/// never wired, so both queues are empty. Returns the records appended.
pub fn drain_into(rx: &MpscReceiver<WalAppendMsg>, wal: &mut Option<WalWriterV3>) -> usize {
    let mut n = 0usize;
    while let Ok((record_type, data)) = rx.try_recv() {
        if let Some(w) = wal.as_mut() {
            w.append(record_type, &data);
        }
        n += 1;
    }
    OVERFLOW.with(|q| {
        let mut q = q.borrow_mut();
        if q.is_empty() {
            return;
        }
        while let Some((record_type, data)) = q.pop_front() {
            if let Some(w) = wal.as_mut() {
                w.append(record_type, &data);
            }
            n += 1;
        }
        if q.capacity() > OVERFLOW_KEEP_CAPACITY {
            *q = VecDeque::new();
        }
    });
    n
}

/// Nothing is waiting to be appended (the idle-park quiet check).
pub fn is_drained(rx: &MpscReceiver<WalAppendMsg>) -> bool {
    rx.is_empty() && OVERFLOW.with(|q| q.borrow().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(i: u32) -> Bytes {
        Bytes::copy_from_slice(&i.to_le_bytes())
    }

    fn drain_order(rx: &MpscReceiver<WalAppendMsg>) -> Vec<u32> {
        let mut out = Vec::new();
        while let Ok((_, d)) = rx.try_recv() {
            out.push(u32::from_le_bytes([d[0], d[1], d[2], d[3]]));
        }
        OVERFLOW.with(|q| {
            for (_, d) in q.borrow_mut().drain(..) {
                out.push(u32::from_le_bytes([d[0], d[1], d[2], d[3]]));
            }
        });
        out
    }

    /// Each test runs on its own thread, so the thread-locals start clean.
    #[test]
    fn owner_overflows_past_capacity_in_fifo_order() {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded(4);
        register_owner(3);
        let before = WAL_APPEND_OVERFLOW_TOTAL.load(Ordering::Relaxed);
        for i in 0..10 {
            assert_eq!(enqueue(&tx, 3, WalRecordType::Command, rec(i)), Ok(()));
        }
        assert!(WAL_APPEND_OVERFLOW_TOTAL.load(Ordering::Relaxed) >= before + 6);
        assert!(!is_drained(&rx));
        assert_eq!(drain_order(&rx), (0..10).collect::<Vec<_>>());
        assert!(is_drained(&rx));
    }

    #[test]
    fn records_after_an_overflow_keep_overflowing_until_the_drain() {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded(2);
        register_owner(0);
        for i in 0..5 {
            assert_eq!(enqueue(&tx, 0, WalRecordType::MqPush, rec(i)), Ok(()));
        }
        // Freeing a channel slot alone (no drain of the overflow) would let a
        // later record jump the queue; only `drain_into`-style draining of
        // BOTH preserves order, which is the only consumer.
        assert_eq!(drain_order(&rx), vec![0, 1, 2, 3, 4]);
        for i in 5..8 {
            assert_eq!(enqueue(&tx, 0, WalRecordType::MqPush, rec(i)), Ok(()));
        }
        assert_eq!(drain_order(&rx), vec![5, 6, 7]);
    }

    #[test]
    fn a_foreign_thread_is_refused_when_full() {
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded(1);
        register_owner(1);
        assert_eq!(enqueue(&tx, 2, WalRecordType::Command, rec(0)), Ok(()));
        assert_eq!(
            enqueue(&tx, 2, WalRecordType::Command, rec(1)),
            Err(Refused::NotOwner)
        );
    }

    #[test]
    fn an_unowned_thread_is_refused_when_full() {
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded(1);
        assert_eq!(enqueue(&tx, 0, WalRecordType::Command, rec(0)), Ok(()));
        assert_eq!(
            enqueue(&tx, 0, WalRecordType::Command, rec(1)),
            Err(Refused::NotOwner)
        );
    }

    #[test]
    fn a_closed_channel_is_refused() {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded(4);
        register_owner(0);
        drop(rx);
        assert_eq!(
            enqueue(&tx, 0, WalRecordType::Command, rec(0)),
            Err(Refused::Closed)
        );
    }

    #[test]
    fn drain_into_without_a_writer_empties_both_queues() {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded(2);
        register_owner(0);
        for i in 0..6 {
            assert_eq!(enqueue(&tx, 0, WalRecordType::Command, rec(i)), Ok(()));
        }
        assert_eq!(drain_into(&rx, &mut None), 6);
        assert!(is_drained(&rx));
    }

    #[test]
    fn drain_into_appends_channel_then_overflow_to_the_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = Some(
            WalWriterV3::new(
                0,
                &dir.path().join("wal-v3"),
                crate::persistence::wal_v3::segment::DEFAULT_SEGMENT_SIZE,
                crate::persistence::wal_v3::segment::WalBounds::DEFAULT,
            )
            .expect("wal writer"),
        );
        let (tx, rx) = crate::runtime::channel::mpsc_bounded(3);
        register_owner(0);
        let lsn0 = wal.as_ref().map(WalWriterV3::current_lsn).unwrap_or(0);
        for i in 0..9 {
            assert_eq!(enqueue(&tx, 0, WalRecordType::Command, rec(i)), Ok(()));
        }
        assert_eq!(drain_into(&rx, &mut wal), 9);
        let lsn1 = wal.as_ref().map(WalWriterV3::current_lsn).unwrap_or(0);
        assert_eq!(
            lsn1 - lsn0,
            9,
            "every record, channel and overflow, got an LSN"
        );
    }
}
