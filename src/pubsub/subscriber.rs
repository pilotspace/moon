use bytes::Bytes;
use crate::runtime::channel;

/// A subscriber wrapping an mpsc sender for delivering pub/sub messages.
///
/// Each subscriber has a unique ID for tracking and removal, and a bounded
/// channel sender for delivering pre-serialized RESP bytes. Pre-serializing
/// once in `publish()` and sending `Bytes` (refcount bump) eliminates
/// per-subscriber Frame allocation and serialization overhead.
#[derive(Clone)]
pub struct Subscriber {
    pub tx: channel::MpscSender<Bytes>,
    pub id: u64,
}

impl Subscriber {
    pub fn new(tx: channel::MpscSender<Bytes>, id: u64) -> Self {
        Self { tx, id }
    }

    /// Attempt to send pre-serialized RESP bytes without blocking.
    /// Returns true if sent, false if the channel is full or closed (slow subscriber).
    #[inline]
    pub fn try_send(&self, data: Bytes) -> bool {
        self.tx.try_send(data).is_ok()
    }
}
