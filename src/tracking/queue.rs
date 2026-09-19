//! A tracking connection's invalidation queue (moon#1088).
//!
//! Redis never drops an invalidation. It appends each one to the client's
//! output buffer, and when that buffer passes `client-output-buffer-limit` it
//! disconnects the client — which a caching client treats as "flush
//! everything" (measured on redis-server 8.6.1 with
//! `client-output-buffer-limit normal 8192 0 0`: a 400-key `MSET` closes the
//! tracker instead of delivering any push).
//!
//! Moon used a 256-slot channel and `try_send`, so the 257th invalidation of a
//! burst vanished and the client kept serving the old value. This queue is
//! unbounded in slots and bounded in BYTES instead: every frame is charged its
//! approximate wire size when queued and credited back when the connection
//! takes it. A send that would carry the queued total past the connection's
//! output-buffer limit disconnects the connection, through the same path as
//! `CLIENT KILL`, and nothing more is queued for it.
//!
//! Nothing here runs unless a connection has enabled tracking: the queue is
//! created by `CLIENT TRACKING on`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::protocol::Frame;
use crate::runtime::channel;

/// Shared between a connection's [`InvalidationTx`] clones and its
/// [`InvalidationRx`].
struct Gauge {
    /// Approximate wire bytes queued and not yet taken by the connection.
    queued: AtomicUsize,
    /// Byte ceiling; `usize::MAX` means unlimited.
    cap: usize,
    /// The connection this queue belongs to.
    client_id: u64,
    /// Set once, when the ceiling is crossed. Every later send is a no-op:
    /// the connection is being closed, and a caching client discards its
    /// whole cache on reconnect.
    overflowed: AtomicBool,
    /// Set while the connection speaks RESP2 (see
    /// [`InvalidationRx::set_deliverable`]): a push has nowhere to go there,
    /// so it is not queued at all.
    discard: AtomicBool,
    /// How the overflowing connection is disconnected. The client registry
    /// in production; injectable so the rule is unit-testable.
    disconnect: fn(u64),
}

/// The sending half, held by the tracking table. Cheap to clone.
#[derive(Clone)]
pub struct InvalidationTx {
    tx: channel::MpscSender<Frame>,
    gauge: Arc<Gauge>,
}

impl std::fmt::Debug for InvalidationTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvalidationTx")
            .field("client_id", &self.gauge.client_id)
            .field("queued", &self.gauge.queued.load(Ordering::Relaxed))
            .finish()
    }
}

/// The receiving half, owned by the connection.
pub struct InvalidationRx {
    rx: channel::MpscReceiver<Frame>,
    gauge: Arc<Gauge>,
    /// A frame taken off the channel that did not fit in the last coalesced
    /// write; the next `recv`/`try_recv` returns it first.
    carry: parking_lot::Mutex<Option<Frame>>,
}

impl std::fmt::Debug for InvalidationRx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvalidationRx")
            .field("client_id", &self.gauge.client_id)
            .finish()
    }
}

/// Create the invalidation queue of connection `client_id`, bounded at
/// `cap_bytes` queued bytes (0 = unlimited, redis's meaning of a zero limit).
pub fn invalidation_queue(client_id: u64, cap_bytes: usize) -> (InvalidationTx, InvalidationRx) {
    with_disconnect(client_id, cap_bytes, disconnect_client)
}

fn with_disconnect(
    client_id: u64,
    cap_bytes: usize,
    disconnect: fn(u64),
) -> (InvalidationTx, InvalidationRx) {
    let (tx, rx) = channel::mpsc_unbounded::<Frame>();
    let gauge = Arc::new(Gauge {
        queued: AtomicUsize::new(0),
        cap: if cap_bytes == 0 {
            usize::MAX
        } else {
            cap_bytes
        },
        client_id,
        overflowed: AtomicBool::new(false),
        discard: AtomicBool::new(false),
        disconnect,
    });
    (
        InvalidationTx {
            tx,
            gauge: Arc::clone(&gauge),
        },
        InvalidationRx {
            rx,
            gauge,
            carry: parking_lot::Mutex::new(None),
        },
    )
}

/// Close a connection whose invalidation queue crossed its limit: the
/// `CLIENT KILL` path, so a connection parked in `read()` is torn down at
/// once. Called under the tracking table lock, which is the permitted order
/// (tracking mutex, then a registry stripe — see `TrackingTable::route`).
fn disconnect_client(client_id: u64) {
    crate::client_registry::kill_clients(&crate::client_registry::KillFilter::Id(client_id), None);
}

impl InvalidationTx {
    /// Queue one frame for the connection. Never blocks and never drops
    /// silently: past the byte ceiling the connection is disconnected.
    pub fn send(&self, frame: Frame) {
        let g = &*self.gauge;
        if g.overflowed.load(Ordering::Relaxed) || g.discard.load(Ordering::Relaxed) {
            return;
        }
        let size = wire_len(&frame);
        let queued = g
            .queued
            .fetch_add(size, Ordering::Relaxed)
            .saturating_add(size);
        if queued > g.cap {
            g.queued.fetch_sub(size, Ordering::Relaxed);
            self.overflow(queued);
            return;
        }
        if self.tx.send(frame).is_err() {
            // The connection is gone; nothing will ever take this frame.
            g.queued.fetch_sub(size, Ordering::Relaxed);
        }
    }

    #[cold]
    #[inline(never)]
    fn overflow(&self, queued: usize) {
        let g = &*self.gauge;
        if g.overflowed.swap(true, Ordering::Relaxed) {
            return;
        }
        tracing::warn!(
            client_id = g.client_id,
            queued_bytes = queued,
            limit_bytes = g.cap,
            "CLIENT TRACKING: invalidations queued past the client output buffer limit; \
             closing the connection (redis closes it the same way)"
        );
        (g.disconnect)(g.client_id);
    }

    /// Bytes queued and not yet taken (tests, diagnostics).
    pub fn queued_bytes(&self) -> usize {
        self.gauge.queued.load(Ordering::Relaxed)
    }

    /// Whether the connection crossed its limit and is being disconnected.
    pub fn overflowed(&self) -> bool {
        self.gauge.overflowed.load(Ordering::Relaxed)
    }
}

/// A bare sender with no limit and no connection behind it (unit tests that
/// read the frames off the raw receiver).
#[cfg(test)]
impl From<channel::MpscSender<Frame>> for InvalidationTx {
    fn from(tx: channel::MpscSender<Frame>) -> Self {
        Self {
            tx,
            gauge: Arc::new(Gauge {
                queued: AtomicUsize::new(0),
                cap: usize::MAX,
                client_id: 0,
                overflowed: AtomicBool::new(false),
                discard: AtomicBool::new(false),
                disconnect: |_| {},
            }),
        }
    }
}

impl InvalidationRx {
    /// Wait for the next frame. `None` once every sender is gone.
    pub async fn recv(&self) -> Option<Frame> {
        if let Some(frame) = self.carry.lock().take() {
            return Some(frame);
        }
        let frame = self.rx.recv_async().await.ok()?;
        self.credit(&frame);
        Some(frame)
    }

    /// Record whether the connection can carry a push (RESP3). Redis writes a
    /// RESP2 connection nothing for its own invalidations, and some RESP2
    /// states (the subscriber loop, MONITOR) never read this queue, so while
    /// the connection speaks RESP2 nothing is queued — otherwise it would
    /// fill to the byte limit and be disconnected for invalidations it could
    /// never have received. One relaxed store; called once per pass of the
    /// connection loop, and only for a tracking connection.
    #[inline]
    pub fn set_deliverable(&self, deliverable: bool) {
        self.gauge.discard.store(!deliverable, Ordering::Relaxed);
    }

    /// Take a frame that is already queued, without waiting.
    pub fn try_recv(&self) -> Option<Frame> {
        if let Some(frame) = self.carry.lock().take() {
            return Some(frame);
        }
        let frame = self.rx.try_recv().ok()?;
        self.credit(&frame);
        Some(frame)
    }

    /// `first` plus whatever else is already queued, serialised as RESP3 into
    /// one buffer (up to ~64 KiB), so a burst of invalidations costs one
    /// socket write instead of one per key.
    ///
    /// `deliverable` is whether the connection can carry a push at all: a
    /// RESP2 connection cannot, and redis writes it nothing, so the queued
    /// frames are consumed and `None` is returned.
    pub fn coalesce(&self, first: Frame, deliverable: bool) -> Option<bytes::Bytes> {
        const MAX_COALESCE_BYTES: usize = 64 * 1024;
        if !deliverable {
            while self.try_recv().is_some() {}
            return None;
        }
        // Never past the byte limit either: senders on other shards keep
        // queueing while this drains, and one write larger than the
        // connection's output-buffer limit is refused by the write path.
        let max = MAX_COALESCE_BYTES.min(self.gauge.cap);
        let mut buf = bytes::BytesMut::new();
        crate::protocol::serialize_resp3(&first, &mut buf);
        while let Some(next) = self.try_recv() {
            // `wire_len` bounds the serialised size from above, so a frame
            // that might not fit waits for the next write.
            if buf.len() + wire_len(&next) > max {
                *self.carry.lock() = Some(next);
                break;
            }
            crate::protocol::serialize_resp3(&next, &mut buf);
        }
        Some(buf.freeze())
    }

    #[inline]
    fn credit(&self, frame: &Frame) {
        self.gauge
            .queued
            .fetch_sub(wire_len(frame), Ordering::Relaxed);
    }
}

/// Approximate RESP3 size of `frame`. Charged on send and credited on receive
/// with the same function, so the gauge returns exactly to zero; the value
/// only has to be close enough to compare against a byte limit.
fn wire_len(frame: &Frame) -> usize {
    match frame {
        // `$<len>\r\n<bytes>\r\n`
        Frame::BulkString(b) | Frame::SimpleString(b) | Frame::Error(b) => b.len() + 8,
        Frame::Array(items) | Frame::Push(items) | Frame::Set(items) => {
            8 + items.iter().map(wire_len).sum::<usize>()
        }
        Frame::Integer(_) => 22,
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::AtomicU64;

    fn push(key: &'static [u8]) -> Frame {
        crate::tracking::invalidation::invalidation_push(&[Bytes::from_static(key)])
    }

    static DISCONNECTED: AtomicU64 = AtomicU64::new(0);

    fn record(id: u64) {
        DISCONNECTED.store(id, Ordering::Relaxed);
    }

    /// The moon#1088 shape: far more than 256 invalidations queued while the
    /// connection is not reading. Every one arrives, in order, and the gauge
    /// returns to zero.
    #[test]
    fn a_burst_is_delivered_whole_and_in_order() {
        let (tx, rx) = with_disconnect(7001, 0, record);
        for i in 0..1000u32 {
            tx.send(Frame::Integer(i64::from(i)));
        }
        assert!(tx.queued_bytes() > 0);
        for i in 0..1000u32 {
            assert_eq!(rx.try_recv(), Some(Frame::Integer(i64::from(i))));
        }
        assert_eq!(rx.try_recv(), None);
        assert_eq!(tx.queued_bytes(), 0, "every charge must be credited back");
        assert!(!tx.overflowed());
    }

    /// Past the byte limit the connection is disconnected once, and nothing
    /// more is queued for it.
    #[test]
    fn crossing_the_limit_disconnects_exactly_once() {
        let per = wire_len(&push(b"k"));
        let (tx, rx) = with_disconnect(7002, per * 3, record);
        for _ in 0..3 {
            tx.send(push(b"k"));
        }
        assert!(!tx.overflowed(), "exactly at the limit is still within it");
        tx.send(push(b"k"));
        assert!(tx.overflowed());
        assert_eq!(DISCONNECTED.load(Ordering::Relaxed), 7002);
        DISCONNECTED.store(0, Ordering::Relaxed);
        // Draining does not re-arm the queue: the connection is closing.
        while rx.try_recv().is_some() {}
        tx.send(push(b"k"));
        assert_eq!(rx.try_recv(), None);
        assert_eq!(
            DISCONNECTED.load(Ordering::Relaxed),
            0,
            "the disconnect must fire once, not per dropped frame"
        );
    }

    /// Taking frames makes room again, so a connection that keeps up is never
    /// disconnected however much passes through it.
    #[test]
    fn a_reader_that_keeps_up_is_never_disconnected() {
        let per = wire_len(&push(b"k"));
        let (tx, rx) = with_disconnect(7003, per * 2, record);
        for _ in 0..10_000 {
            tx.send(push(b"k"));
            assert!(rx.try_recv().is_some());
        }
        assert!(!tx.overflowed());
        assert_eq!(tx.queued_bytes(), 0);
    }

    /// A coalesced write never exceeds the byte limit: a frame that might not
    /// fit is carried to the next write, not dropped.
    #[test]
    fn a_coalesced_write_stays_within_the_limit() {
        let per = wire_len(&push(b"k"));
        let cap = per * 2;
        let (tx, rx) = with_disconnect(7005, cap, record);
        tx.send(push(b"k"));
        tx.send(push(b"k"));
        // The connection takes one frame; a sender on another shard refills
        // the room it freed before the connection coalesces the rest.
        let first = rx.try_recv().expect("queued");
        tx.send(push(b"k"));
        let write = rx.coalesce(first, true).expect("deliverable");
        assert!(
            write.len() <= cap,
            "{} bytes past a {cap}-byte limit",
            write.len()
        );
        let single = {
            let mut b = bytes::BytesMut::new();
            crate::protocol::serialize_resp3(&push(b"k"), &mut b);
            b.len()
        };
        assert_eq!(write.len(), single * 2, "two frames fit");
        assert_eq!(
            rx.try_recv(),
            Some(push(b"k")),
            "the third is carried, not lost"
        );
        assert_eq!(rx.try_recv(), None);
        assert_eq!(tx.queued_bytes(), 0);
    }

    /// While the connection speaks RESP2 nothing is queued at all.
    #[test]
    fn a_resp2_connection_queues_nothing() {
        let (tx, rx) = with_disconnect(7007, 64, record);
        rx.set_deliverable(false);
        for _ in 0..1000 {
            tx.send(push(b"k"));
        }
        assert_eq!(tx.queued_bytes(), 0);
        assert!(!tx.overflowed(), "discarding is not an overflow");
        assert_eq!(rx.try_recv(), None);
        rx.set_deliverable(true);
        tx.send(push(b"k"));
        assert!(rx.try_recv().is_some());
    }

    /// A dropped receiver (TRACKING off, disconnect) leaves nothing charged.
    #[test]
    fn sending_to_a_closed_connection_charges_nothing() {
        let (tx, rx) = with_disconnect(7004, 0, record);
        drop(rx);
        tx.send(push(b"k"));
        assert_eq!(tx.queued_bytes(), 0);
    }
}
