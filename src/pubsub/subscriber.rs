use crate::runtime::channel;
use bytes::Bytes;

/// A subscriber wrapping an mpsc sender for delivering pub/sub messages.
///
/// Each subscriber has a unique ID for tracking and removal, and a bounded
/// channel sender for delivering pre-serialized RESP bytes. Pre-serializing
/// once in `publish()` and sending `Bytes` (refcount bump) eliminates
/// per-subscriber Frame allocation and serialization overhead.
///
/// `is_resp3` flags whether the subscriber's underlying connection negotiated
/// RESP3 via `HELLO 3`. When true, `publish()` frames pub/sub messages as
/// RESP3 Push (`>`) instead of RESP2 Array (`*`). Backwards-compatible:
/// RESP2 subscribers continue to receive Array frames unchanged.
#[derive(Clone)]
pub struct Subscriber {
    pub tx: channel::MpscSender<Bytes>,
    pub id: u64,
    pub is_resp3: bool,
}

impl Subscriber {
    /// Backwards-compatible constructor — defaults to RESP2 (Array-framed) delivery.
    ///
    /// Use [`Subscriber::with_protocol`] when the caller knows the connection's
    /// RESP protocol version (e.g., from `framed.codec().protocol_version()`).
    pub fn new(tx: channel::MpscSender<Bytes>, id: u64) -> Self {
        Self {
            tx,
            id,
            is_resp3: false,
        }
    }

    /// Explicit constructor for callers that know the connection protocol
    /// version. `is_resp3` should be `true` when the connection has issued
    /// `HELLO 3` (i.e., `framed.codec().protocol_version() == 3`).
    pub fn with_protocol(tx: channel::MpscSender<Bytes>, id: u64, is_resp3: bool) -> Self {
        Self { tx, id, is_resp3 }
    }

    /// Attempt to send pre-serialized RESP bytes without blocking.
    /// Returns true if sent, false if the channel is full or closed (slow subscriber).
    #[inline]
    pub fn try_send(&self, data: Bytes) -> bool {
        self.tx.try_send(data).is_ok()
    }
}
