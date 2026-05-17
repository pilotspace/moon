//! Per-shard CDC subscriber registry and tick-driven fan-out.
//!
//! C3b-1: pulls newly-flushed WAL records via a per-subscriber
//! [`WalTailReader`], encodes each as a Debezium JSON envelope, and
//! broadcasts the bytes over a bounded `flume::Sender<Bytes>`.
//!
//! Architectural invariant — fan-out runs on the shard's 1ms periodic
//! tick, **never** inside `wal_append_and_fanout`. Sprint 3.5b made the
//! write hot path lock-free for the empty-replica case; adding CDC
//! sinks inline would re-introduce that overhead even when no consumer
//! is subscribed. This module sees zero CPU until the first subscriber
//! is added.
//!
//! Visibility: tail readers re-stat segment file length on each call,
//! so they observe records that have already been `flush_write`'d
//! (i.e. handed to the OS page cache). With the default 1ms
//! `flush_if_needed` cadence and the 1s `flush_sync` timer, CDC
//! visibility latency tracks the AOF durability policy.

use bytes::Bytes;

use crate::cdc::{decode_wal_record, encode_debezium};
use crate::persistence::wal_v3::{TailCursor, WalTailReader};

/// Hard cap on envelopes fanned out per subscriber per tick. Prevents one
/// slow consumer (or a fresh subscriber doing a large `from_lsn` replay)
/// from pinning the shard tick on a CDC scan.
const MAX_EVENTS_PER_SUBSCRIBER_PER_TICK: usize = 1024;

/// One CDC subscriber attached to a shard's fan-out registry.
pub struct CdcSubscriber {
    /// Bounded channel to the per-connection sender task. The shard side
    /// is non-blocking: `try_send` returning `Full` triggers disconnect.
    pub tx: flume::Sender<Bytes>,
    /// Inclusive LSN floor — records with `lsn < from_lsn` are skipped.
    pub from_lsn: u64,
    /// Resumable tail cursor. Each subscriber maintains its own to
    /// support different replay start points without sharing state.
    pub tail: WalTailReader,
}

impl CdcSubscriber {
    /// Create a fresh subscriber tailing `wal_dir` from segment 0,
    /// emitting envelopes for any record with `lsn >= from_lsn`.
    pub fn new(
        wal_dir: impl AsRef<std::path::Path>,
        from_lsn: u64,
    ) -> (Self, flume::Receiver<Bytes>) {
        // Reasonable backpressure: 1024 envelopes ≈ ~1 MB of JSON for
        // typical KV writes. Slow consumers get disconnected promptly
        // rather than ballooning shard memory.
        let (tx, rx) = flume::bounded(1024);
        let tail = WalTailReader::new(wal_dir.as_ref(), TailCursor::start());
        (Self { tx, from_lsn, tail }, rx)
    }

    /// Construct from caller-supplied channel ends — used by the
    /// dispatch path where the receiver is owned by the connection
    /// task before the subscriber reaches the shard registry.
    pub fn from_parts(
        tx: flume::Sender<Bytes>,
        wal_dir: impl AsRef<std::path::Path>,
        from_lsn: u64,
    ) -> Self {
        Self {
            tx,
            from_lsn,
            tail: WalTailReader::new(wal_dir.as_ref(), TailCursor::start()),
        }
    }
}

/// Per-shard registry that owns subscribers and drains them on each tick.
pub struct CdcSubscriberRegistry {
    shard_id: u16,
    subscribers: Vec<CdcSubscriber>,
}

impl CdcSubscriberRegistry {
    pub fn new(shard_id: u16) -> Self {
        Self {
            shard_id,
            subscribers: Vec::new(),
        }
    }

    /// Subscriber count.
    #[inline]
    pub fn len(&self) -> usize {
        self.subscribers.len()
    }

    /// Whether any subscriber is currently attached. The shard tick uses
    /// this to short-circuit the fan-out entirely when no one is reading.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    /// Register a new subscriber.
    pub fn add(&mut self, sub: CdcSubscriber) {
        self.subscribers.push(sub);
    }

    /// Drain `pending` and register each as a subscriber tailing `wal_dir`.
    ///
    /// If `wal_dir` is `None` (shard has no WAL v3 writer — e.g. running
    /// without `--disk-offload`), the sender ends are dropped, which the
    /// caller observes as a closed receiver. CDC is a v3-only feature in
    /// v0.2; the closed channel is the well-defined error signal.
    pub fn register_pending<I>(&mut self, pending: I, wal_dir: Option<&std::path::Path>)
    where
        I: IntoIterator<Item = crate::shard::dispatch::CdcSubscribePayload>,
    {
        if let Some(dir) = wal_dir {
            for p in pending {
                self.subscribers
                    .push(CdcSubscriber::from_parts(p.tx, dir, p.from_lsn));
            }
        } else {
            // Drop pending senders — receivers will observe disconnect.
            for _ in pending {}
        }
    }

    /// Drain freshly-flushed WAL records and fan-out Debezium envelopes
    /// to every subscriber. Subscribers whose bounded channel is full or
    /// closed are removed (slow-consumer disconnect).
    ///
    /// Returns the total number of envelopes successfully delivered
    /// across all subscribers — useful for tests and metrics.
    pub fn fanout_tick(&mut self, now_ms: i64) -> usize {
        if self.subscribers.is_empty() {
            return 0;
        }
        let shard = self.shard_id;
        let mut total = 0usize;
        let mut to_remove: smallvec::SmallVec<[usize; 4]> = smallvec::SmallVec::new();

        for (idx, sub) in self.subscribers.iter_mut().enumerate() {
            let mut drained = 0usize;
            let mut disconnect = false;
            while drained < MAX_EVENTS_PER_SUBSCRIBER_PER_TICK {
                match sub.tail.read_next() {
                    Ok(Some(rec)) => {
                        if rec.lsn < sub.from_lsn {
                            continue;
                        }
                        let event = decode_wal_record(&rec, shard);
                        let envelope = encode_debezium(&event, now_ms);
                        match sub.tx.try_send(envelope) {
                            Ok(()) => {
                                drained += 1;
                                total += 1;
                            }
                            Err(_) => {
                                // Full or Disconnected — both terminal for
                                // this subscriber. The bytes we just tried
                                // to send are dropped; the consumer will
                                // see the channel close and reconnect with
                                // its last cursor.
                                disconnect = true;
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(
                            "Shard {}: CDC tail read error, dropping subscriber: {}",
                            shard,
                            e
                        );
                        disconnect = true;
                        break;
                    }
                }
            }
            if disconnect {
                to_remove.push(idx);
            }
        }

        // Remove in reverse index order so swap_remove doesn't shift the
        // indices we still need to visit.
        for idx in to_remove.into_iter().rev() {
            self.subscribers.swap_remove(idx);
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::wal_v3::record::WalRecordType;
    use crate::persistence::wal_v3::segment::{DEFAULT_SEGMENT_SIZE, WalWriterV3};

    fn write_kv_records(wal_dir: &std::path::Path, n: u64) {
        let mut w = WalWriterV3::new(0, wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();
        for _ in 0..n {
            w.append(
                WalRecordType::Command,
                b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
            );
        }
        w.flush_sync().unwrap();
    }

    /// C3b-1 — happy path: fan-out drains a 10-record WAL to a single
    /// subscriber, returning 10 envelopes via the bounded channel.
    #[test]
    fn test_cdc_fanout_tick_drains_to_subscriber() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        write_kv_records(&wal_dir, 10);

        let mut reg = CdcSubscriberRegistry::new(0);
        let (sub, rx) = CdcSubscriber::new(&wal_dir, 1);
        reg.add(sub);

        let delivered = reg.fanout_tick(1_700_000_000_000);
        assert_eq!(delivered, 10, "all 10 records should reach the subscriber");

        let mut count = 0;
        while let Ok(env) = rx.try_recv() {
            let s = std::str::from_utf8(&env).unwrap();
            assert!(s.starts_with("{\"op\":\"u\""));
            count += 1;
        }
        assert_eq!(count, 10);
    }

    /// C3b-1 — subscriber's `from_lsn` filter respected — write 10,
    /// subscribe from LSN 6, only LSN 6..=10 (five envelopes) arrive.
    #[test]
    fn test_cdc_fanout_replays_from_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        write_kv_records(&wal_dir, 10);

        let mut reg = CdcSubscriberRegistry::new(0);
        let (sub, rx) = CdcSubscriber::new(&wal_dir, 6);
        reg.add(sub);

        let delivered = reg.fanout_tick(0);
        assert_eq!(delivered, 5);
        assert_eq!(rx.len(), 5);
    }

    /// C3b-1 — slow-consumer policy: bounded(1) channel fills, the next
    /// `try_send` returns Full, the subscriber is removed from the
    /// registry. The dropped envelopes are NOT redelivered — the
    /// consumer's responsibility is to reconnect with its last LSN.
    #[test]
    fn test_cdc_fanout_drops_full_subscriber() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        write_kv_records(&wal_dir, 5);

        let (tx, rx) = flume::bounded::<Bytes>(1);
        // Do not drain rx — the second try_send will return Full.
        let sub = CdcSubscriber::from_parts(tx, &wal_dir, 1);

        let mut reg = CdcSubscriberRegistry::new(0);
        reg.add(sub);

        let delivered = reg.fanout_tick(0);
        // Channel capacity is 1, so exactly one envelope is delivered
        // before the second try_send hits Full and we evict the sub.
        assert_eq!(delivered, 1);
        assert_eq!(reg.len(), 0, "slow consumer should be dropped");

        // Receiver still owns the one buffered envelope.
        assert_eq!(rx.len(), 1);
    }

    /// C3b-1 — empty registry is a no-op (zero-CPU short-circuit
    /// invariant the shard tick relies on).
    #[test]
    fn test_cdc_fanout_no_subscribers_is_noop() {
        let mut reg = CdcSubscriberRegistry::new(0);
        assert_eq!(reg.fanout_tick(0), 0);
        assert!(reg.is_empty());
    }

    /// C3b-2 — `register_pending` accepts an iterator of
    /// `CdcSubscribePayload` items (the shape that arrives from the
    /// SPSC drain) and turns them into subscribers tailing `wal_dir`.
    /// Verifies the full plumbing path: a payload struct in →
    /// envelopes out the receiver after one tick.
    #[test]
    fn test_register_pending_creates_working_subscriber() {
        use crate::shard::dispatch::CdcSubscribePayload;

        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        write_kv_records(&wal_dir, 3);

        let (tx, rx) = flume::bounded::<Bytes>(8);
        let payload = CdcSubscribePayload { tx, from_lsn: 1 };

        let mut reg = CdcSubscriberRegistry::new(0);
        reg.register_pending(std::iter::once(payload), Some(wal_dir.as_path()));
        assert_eq!(reg.len(), 1);

        let delivered = reg.fanout_tick(0);
        assert_eq!(delivered, 3);
        assert_eq!(rx.len(), 3);
    }

    /// C3b-2 — `register_pending(_, None)` drops sender ends (CDC is a
    /// WAL v3 feature; without a writer the receiver gets a clean close).
    #[test]
    fn test_register_pending_drops_when_no_wal_dir() {
        use crate::shard::dispatch::CdcSubscribePayload;

        let (tx, rx) = flume::bounded::<Bytes>(1);
        let payload = CdcSubscribePayload { tx, from_lsn: 1 };

        let mut reg = CdcSubscriberRegistry::new(0);
        reg.register_pending(std::iter::once(payload), None);

        assert_eq!(reg.len(), 0);
        // Sender was dropped — receiver sees Disconnected.
        assert!(matches!(
            rx.try_recv(),
            Err(flume::TryRecvError::Disconnected)
        ));
    }
}
