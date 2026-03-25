use crate::runtime::channel;
use crate::protocol::Frame;

/// A subscriber wrapping an mpsc sender for delivering pub/sub messages.
///
/// Each subscriber has a unique ID for tracking and removal, and a bounded
/// channel sender for delivering messages. If the channel is full (slow
/// subscriber), `try_send` returns false and the subscriber should be dropped.
#[derive(Clone)]
pub struct Subscriber {
    pub tx: channel::MpscSender<Frame>,
    pub id: u64,
}

impl Subscriber {
    pub fn new(tx: channel::MpscSender<Frame>, id: u64) -> Self {
        Self { tx, id }
    }

    /// Attempt to send a frame without blocking.
    /// Returns true if sent, false if the channel is full or closed (slow subscriber).
    pub fn try_send(&self, frame: Frame) -> bool {
        self.tx.try_send(frame).is_ok()
    }
}
