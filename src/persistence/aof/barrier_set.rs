//! Several fsync barriers awaited TOGETHER.
//!
//! A cross-shard write confirms each written shard with an fsync barrier
//! (moon#1322). One after another, `N` barriers cost the SUM of `N` fsyncs —
//! and with a stalled disk `N × fsync_timeout`. Here every barrier is sent
//! first ([`PendingBarriers::begin`]), so the writers fsync in parallel, and
//! then all acks are awaited under ONE deadline ([`PendingBarriers::wait`]):
//! the cost is the slowest fsync, bounded by one `fsync_timeout`.
//!
//! Zero cost when nothing is owed: `begin` is one policy load per shard and
//! sends nothing unless that shard's lane is held or the policy is `always`
//! (the same test as `AofWriterPool::fsync_barrier`). The receivers live
//! inline (no heap) up to 16 shards.
//!
//! [`BarrierDebt`] is what a connection's pipeline batch OWES:
//! the shards its writes touched and the replies waiting on them. The
//! commands of a batch record into it instead of awaiting; ONE
//! [`BarrierDebt::settle`] — one `PendingBarriers` set over every owed shard,
//! the connection's own included — confirms them all before any reply of the
//! batch is flushed.

use std::time::{Duration, Instant};

use bytes::Bytes;
use smallvec::SmallVec;

use super::{AckOutcome, AofAck, AofWriterPool, FoldEpoch, FsyncPolicy};
use crate::protocol::Frame;
use crate::runtime::channel::OneshotReceiver;

/// Barriers sent and not yet awaited, each with its shard.
#[derive(Default)]
pub struct PendingBarriers {
    rxs: SmallVec<[(usize, OneshotReceiver<AofAck>); 16]>,
}

/// Failed barriers: `(shard, ack)`. Inline up to 4 — a failure is rare, and
/// several at once means a sick disk, not a hot path.
pub type BarrierFailures = SmallVec<[(usize, AofAck); 4]>;

impl PendingBarriers {
    /// No barrier pending.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether no barrier was sent.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rxs.is_empty()
    }

    /// Send `shard_id`'s barrier now, if one is owed (`always`, or a held
    /// lane); nothing otherwise. Its ack is awaited by [`Self::wait`].
    #[inline]
    pub fn begin(&mut self, pool: &AofWriterPool, shard_id: usize) {
        if pool.fsync_policy_for(shard_id) == FsyncPolicy::Always {
            // A zero-length AppendSync: no record, fsync + ack only (see
            // `AofWriterPool::fsync_barrier`).
            self.rxs.push((
                shard_id,
                pool.try_send_append_sync(shard_id, 0, 0, Bytes::new(), FoldEpoch::INITIAL),
            ));
        }
    }

    /// Await every barrier sent, under ONE `fsync_timeout` deadline (`ZERO`:
    /// unbounded, as `fsync_barrier`). Every ack is awaited even after a
    /// failure; the first failure is returned.
    pub async fn wait(self, pool: &AofWriterPool) -> Result<(), AofAck> {
        match self.wait_each(pool).await.first() {
            Some(&(_, ack)) => Err(ack),
            None => Ok(()),
        }
    }

    /// [`Self::wait`], reporting EVERY failed shard (in send order) instead of
    /// the first failure only — a batch maps each one to the replies that
    /// waited on that shard ([`BarrierDebt::settle`]).
    pub async fn wait_each(self, pool: &AofWriterPool) -> BarrierFailures {
        let mut failed = BarrierFailures::new();
        if self.rxs.is_empty() {
            return failed;
        }
        let bound = pool.fsync_timeout();
        let deadline = (!bound.is_zero()).then(|| Instant::now() + bound);
        for (shard, rx) in self.rxs {
            let timeout = match deadline {
                // A deadline already passed still polls the ack once.
                Some(d) => d
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_micros(1)),
                None => Duration::ZERO,
            };
            let res = match AofWriterPool::await_ack(rx, timeout).await {
                AckOutcome::Ack(AofAck::Synced) => Ok(()),
                AckOutcome::Ack(other) => Err(other),
                AckOutcome::Disconnected => Err(AofAck::WriteFailed),
                AckOutcome::TimedOut => Err(AofAck::FsyncFailed),
            };
            if let Err(ack) = res {
                failed.push((shard, ack));
            }
        }
        failed
    }
}

/// `shard`'s bit in a reply's shard mask. Above 64 shards two shards share a
/// bit — a failure then also fails the replies of the shard it aliases:
/// conservative (an unconfirmed reply is never sent as confirmed), and the
/// same folding the handlers' `pending_mask` uses.
#[inline]
fn shard_bit(shard: usize) -> u64 {
    1u64 << (shard % u64::BITS as usize)
}

/// The fsync barriers a connection's current batch OWES before any of its
/// replies leaves (coalescing, moon#1322).
///
/// A write records the shards whose AOF records it queued — its own shard's
/// local leg, every remote owner of a coordinated `MSET`/`DEL`/…, every
/// remote shard a pipelined single-key write went to — and, when its reply
/// is a success, the reply's index ([`Self::owe`]). The handler then pays the
/// whole debt with ONE [`Self::settle`] before it flushes the batch, and
/// every path that flushes earlier (blocking commands, SUBSCRIBE, PSYNC)
/// settles first. Under `always` a pipeline of `N` spanning writes thus costs
/// one parallel barrier per owed shard, not `N` serial barrier sets.
///
/// Holding a reply back until the batch's barrier is the same contract the
/// per-command barrier gave — nothing leaves before every shard it wrote has
/// its record written (and fsynced under `always`) — the batch's replies just
/// wait for one shared confirmation. Later commands of the batch reading an
/// earlier one's key is shard-local order, unaffected.
///
/// No heap on the hot path: the shard set is inline up to 16 shards; the
/// reply list is a per-connection `Vec` cleared (capacity kept) each batch.
pub struct BarrierDebt {
    /// The connection's own shard (the target of [`Self::push`]).
    home: usize,
    /// Owed shards, deduplicated; `seen` is their bits (fast dedup).
    shards: SmallVec<[usize; 16]>,
    seen: u64,
    /// `(reply index, shard mask)` for every SUCCESS reply waiting on the
    /// debt. A barrier failure on a shard in the mask fails that reply.
    waiters: Vec<(usize, u64)>,
}

impl BarrierDebt {
    /// Nothing owed, for a connection served by shard `home`.
    pub fn new(home: usize) -> Self {
        Self {
            home,
            shards: SmallVec::new(),
            seen: 0,
            waiters: Vec::new(),
        }
    }

    /// Whether nothing is owed.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    /// The owed shards, in first-owed order.
    #[inline]
    pub fn shards(&self) -> &[usize] {
        &self.shards
    }

    /// Reply `idx` waits for the connection's OWN shard's barrier (a local
    /// leg, a script that wrote, a FLUSH's local leg). The form every
    /// pre-coalescing local-leg site used.
    #[inline]
    pub fn push(&mut self, idx: usize) {
        let home = self.home;
        self.owe(Some(idx), [home]);
    }

    /// `shards` owe a barrier; reply `waiter`, when given, waits for all of
    /// them. Pass `None` for a write whose reply is an ERROR: its shards are
    /// still confirmed before anything leaves (the records it did queue are
    /// covered), but no barrier failure overwrites the reply it has.
    pub fn owe(&mut self, waiter: Option<usize>, shards: impl IntoIterator<Item = usize>) {
        let mut mask = 0u64;
        for s in shards {
            let bit = shard_bit(s);
            mask |= bit;
            if self.seen & bit == 0 || !self.shards.contains(&s) {
                self.seen |= bit;
                self.shards.push(s);
            }
        }
        if let Some(idx) = waiter
            && mask != 0
        {
            self.waiters.push((idx, mask));
        }
    }

    /// Forget everything owed (batch start; after [`Self::settle`]).
    #[inline]
    pub fn clear(&mut self) {
        self.shards.clear();
        self.seen = 0;
        self.waiters.clear();
    }

    /// Pay the debt: one barrier per owed shard, all SENT before any is
    /// awaited, awaited under one deadline ([`PendingBarriers`]). Every
    /// waiting reply whose mask holds a failed shard becomes that shard's
    /// `barrier_refusal_frame` (`AOF_FSYNC_ERR`, or the backlog text) — never
    /// a false `+OK`. Always leaves the debt empty.
    pub async fn settle(&mut self, pool: &AofWriterPool, responses: &mut [Frame]) {
        if self.shards.is_empty() {
            self.waiters.clear();
            return;
        }
        let mut set = PendingBarriers::new();
        for &s in &self.shards {
            set.begin(pool, s);
        }
        let failed = set.wait_each(pool).await;
        if !failed.is_empty() {
            for &(idx, mask) in &self.waiters {
                let hit = failed.iter().find(|(s, _)| mask & shard_bit(*s) != 0);
                if let (Some(&(_, ack)), Some(slot)) = (hit, responses.get_mut(idx)) {
                    *slot = super::barrier_refusal_frame(ack);
                }
            }
        }
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::aof::AofMessage;

    /// Nothing owed: nothing sent, `wait` is immediate.
    #[test]
    fn nothing_is_sent_when_no_barrier_is_owed() {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level(tx);
        let mut set = PendingBarriers::new();
        set.begin(&pool, 0);
        assert!(set.is_empty(), "everysec, unheld");
        assert!(rx.is_empty());
    }

    /// Owed (held lane): sent at `begin`, before any await — every writer
    /// gets its barrier before the first ack is awaited.
    #[test]
    fn an_owed_barrier_is_sent_at_begin() {
        if !crate::persistence::aof::lane::enabled() {
            return; // MOON_AOF_SHARD_WRITE=0 in this test's environment
        }
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level(tx);
        let _lane = pool.lane(0); // attached: held until its first hand-over
        let mut set = PendingBarriers::new();
        set.begin(&pool, 0);
        set.begin(&pool, 0);
        assert!(!set.is_empty());
        assert_eq!(rx.len(), 2, "both barriers queued before any wait");
        assert!(matches!(
            rx.try_recv(),
            Ok(AofMessage::AppendSync { ref bytes, .. }) if bytes.is_empty()
        ));
    }

    /// The debt deduplicates shards (also past 64, where bits alias), keeps a
    /// waiter only for a success reply with at least one shard, and `push`
    /// owes the connection's own shard.
    #[test]
    fn debt_records_shards_once_and_waiters_only_for_successes() {
        let mut d = BarrierDebt::new(2);
        assert!(d.is_empty());
        d.push(0); // home
        d.owe(Some(1), [1, 3, 2]);
        d.owe(None, [5]); // an error reply: shard owed, nobody waits
        d.owe(Some(9), std::iter::empty()); // nothing owed: no waiter
        d.owe(Some(2), [66, 2]); // 66 aliases bit 2, still its own shard
        assert_eq!(d.shards(), &[2, 1, 3, 5, 66]);
        assert_eq!(
            d.waiters,
            vec![
                (0, 1 << 2),
                (1, (1 << 1) | (1 << 3) | (1 << 2)),
                (2, 1 << 2)
            ]
        );
        d.clear();
        assert!(d.is_empty());
        assert!(d.waiters.is_empty());
        assert_eq!(d.seen, 0);
    }

    /// Nothing owed under `everysec`: settling sends nothing and touches no
    /// reply, and the debt is empty afterwards.
    #[test]
    fn settling_under_everysec_sends_nothing() {
        let (tx0, rx0) = crate::runtime::channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, rx1) = crate::runtime::channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::EverySec,
            Duration::ZERO,
        );
        let mut d = BarrierDebt::new(0);
        d.push(0);
        d.owe(Some(1), [1]);
        let ok = Frame::SimpleString(Bytes::from_static(b"OK"));
        let mut responses = vec![ok.clone(), ok.clone()];
        futures::executor::block_on(d.settle(&pool, &mut responses));
        assert!(d.is_empty());
        assert!(rx0.is_empty() && rx1.is_empty());
        assert_eq!(responses, vec![ok.clone(), ok]);
    }

    /// Under `always`: one barrier per owed shard, all sent before any is
    /// awaited; a failed shard fails exactly the replies that waited on it.
    #[test]
    fn a_failed_shard_fails_only_the_replies_that_waited_on_it() {
        use crate::persistence::aof::{AOF_BARRIER_BACKLOG_ERR, AofMessage};
        // Shard 0's writer is backlogged (full channel): its barrier is
        // refused at once. Shards 1 and 2 ack.
        let (tx0, _rx0) = crate::runtime::channel::mpsc_bounded::<AofMessage>(1);
        tx0.try_send(AofMessage::Shutdown).expect("pre-fill");
        let (tx1, rx1) = crate::runtime::channel::mpsc_bounded::<AofMessage>(4);
        let (tx2, rx2) = crate::runtime::channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1, tx2],
            FsyncPolicy::Always,
            Duration::ZERO,
        );
        let ack_one = |rx: crate::runtime::channel::MpscReceiver<AofMessage>| {
            std::thread::spawn(move || match rx.recv() {
                Ok(AofMessage::AppendSync { bytes, ack, .. }) => {
                    assert!(bytes.is_empty(), "a barrier carries no record");
                    let _ = ack.send(AofAck::Synced);
                }
                other => panic!("expected a barrier, got ok={}", other.is_ok()),
            })
        };
        let (h1, h2) = (ack_one(rx1), ack_one(rx2));
        let mut d = BarrierDebt::new(0);
        d.push(0); // waits on 0
        d.owe(Some(1), [1]); // waits on 1
        d.owe(Some(2), [1, 2]); // waits on 1 and 2
        d.owe(Some(3), [2, 0]); // waits on 2 and 0
        let ok = Frame::SimpleString(Bytes::from_static(b"OK"));
        let mut responses = vec![ok.clone(); 4];
        futures::executor::block_on(d.settle(&pool, &mut responses));
        h1.join().expect("shard 1 writer");
        h2.join().expect("shard 2 writer");
        let refused = Frame::Error(Bytes::from_static(AOF_BARRIER_BACKLOG_ERR));
        assert_eq!(responses, vec![refused.clone(), ok.clone(), ok, refused]);
        assert!(d.is_empty(), "settling always empties the debt");
    }
}
