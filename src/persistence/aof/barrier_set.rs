//! Several fsync barriers awaited TOGETHER (R2b round 3 P1).
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

use std::time::{Duration, Instant};

use bytes::Bytes;
use smallvec::SmallVec;

use super::{AckOutcome, AofAck, AofWriterPool, FoldEpoch, FsyncPolicy};
use crate::runtime::channel::OneshotReceiver;

/// Barriers sent and not yet awaited.
#[derive(Default)]
pub struct PendingBarriers {
    rxs: SmallVec<[OneshotReceiver<AofAck>; 16]>,
}

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
            self.rxs.push(pool.try_send_append_sync(
                shard_id,
                0,
                0,
                Bytes::new(),
                FoldEpoch::INITIAL,
            ));
        }
    }

    /// Await every barrier sent, under ONE `fsync_timeout` deadline (`ZERO`:
    /// unbounded, as `fsync_barrier`). Every ack is awaited even after a
    /// failure; the first failure is returned.
    pub async fn wait(self, pool: &AofWriterPool) -> Result<(), AofAck> {
        if self.rxs.is_empty() {
            return Ok(());
        }
        let bound = pool.fsync_timeout();
        let deadline = (!bound.is_zero()).then(|| Instant::now() + bound);
        let mut first: Option<AofAck> = None;
        for rx in self.rxs {
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
                first.get_or_insert(ack);
            }
        }
        first.map_or(Ok(()), Err)
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
}
