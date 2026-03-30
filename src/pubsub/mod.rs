pub mod subscriber;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};

use crate::command::key::glob_match;
use crate::protocol::Frame;

use self::subscriber::Subscriber;
use crate::framevec;
static NEXT_SUBSCRIBER_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a globally unique subscriber ID.
pub fn next_subscriber_id() -> u64 {
    NEXT_SUBSCRIBER_ID.fetch_add(1, Ordering::Relaxed)
}

/// Central registry for Pub/Sub channel and pattern subscriptions.
///
/// Manages exact-channel subscribers and glob-pattern subscribers.
/// Publishing fans out messages to all matching subscribers; slow
/// subscribers whose channels are full are automatically removed.
#[derive(Default)]
pub struct PubSubRegistry {
    channels: HashMap<Bytes, Vec<Subscriber>>,
    patterns: Vec<(Bytes, Vec<Subscriber>)>,
}

impl PubSubRegistry {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            patterns: Vec::new(),
        }
    }

    /// Subscribe to an exact channel.
    pub fn subscribe(&mut self, channel: Bytes, sub: Subscriber) {
        self.channels
            .entry(channel)
            .or_insert_with(Vec::new)
            .push(sub);
    }

    /// Unsubscribe from an exact channel by subscriber ID.
    pub fn unsubscribe(&mut self, channel: &[u8], sub_id: u64) {
        if let Some(subs) = self.channels.get_mut(channel) {
            subs.retain(|s| s.id != sub_id);
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }
    }

    /// Subscribe to a glob pattern.
    pub fn psubscribe(&mut self, pattern: Bytes, sub: Subscriber) {
        for (existing_pattern, subs) in &mut self.patterns {
            if existing_pattern.as_ref() == pattern.as_ref() {
                subs.push(sub);
                return;
            }
        }
        self.patterns.push((pattern, vec![sub]));
    }

    /// Unsubscribe from a glob pattern by subscriber ID.
    pub fn punsubscribe(&mut self, pattern: &[u8], sub_id: u64) {
        self.patterns.retain_mut(|(p, subs)| {
            if p.as_ref() == pattern {
                subs.retain(|s| s.id != sub_id);
                !subs.is_empty()
            } else {
                true
            }
        });
    }

    /// Remove subscriber from all channels. Returns list of channels they were in.
    pub fn unsubscribe_all(&mut self, sub_id: u64) -> Vec<Bytes> {
        let mut removed = Vec::new();
        self.channels.retain(|channel, subs| {
            let before = subs.len();
            subs.retain(|s| s.id != sub_id);
            if subs.len() < before {
                removed.push(channel.clone());
            }
            !subs.is_empty()
        });
        removed
    }

    /// Remove subscriber from all patterns. Returns list of patterns they were in.
    pub fn punsubscribe_all(&mut self, sub_id: u64) -> Vec<Bytes> {
        let mut removed = Vec::new();
        self.patterns.retain_mut(|(pattern, subs)| {
            let before = subs.len();
            subs.retain(|s| s.id != sub_id);
            if subs.len() < before {
                removed.push(pattern.clone());
            }
            !subs.is_empty()
        });
        removed
    }

    /// Publish a message to a channel. Returns the number of subscribers that received it.
    ///
    /// Pre-serializes the RESP message once, then fans out `Bytes` (refcount bump)
    /// to all matching subscribers. This eliminates per-subscriber Frame allocation
    /// and serialization — the dominant cost in high fan-out scenarios.
    ///
    /// Slow subscribers (full channel) are automatically removed.
    pub fn publish(&mut self, channel: &Bytes, message: &Bytes) -> i64 {
        let mut count: i64 = 0;

        // Exact channel subscribers — pre-serialize once, send Bytes to all
        if let Some(subs) = self.channels.get_mut(channel) {
            let serialized = serialize_message_bytes(channel, message);
            subs.retain(|sub| {
                if sub.try_send(serialized.clone()) {
                    count += 1;
                    true
                } else {
                    false // slow subscriber, remove
                }
            });
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }

        // Pattern subscribers — only iterate if patterns exist
        if !self.patterns.is_empty() {
            let mut had_removals = false;
            for (pattern, subs) in &mut self.patterns {
                if glob_match(pattern, channel) {
                    let serialized = serialize_pmessage_bytes(pattern, channel, message);
                    let before = subs.len();
                    subs.retain(|sub| {
                        if sub.try_send(serialized.clone()) {
                            count += 1;
                            true
                        } else {
                            false
                        }
                    });
                    if subs.len() < before {
                        had_removals = true;
                    }
                }
            }
            // Only clean up if we actually removed subscribers
            if had_removals {
                self.patterns.retain(|(_, subs)| !subs.is_empty());
            }
        }

        count
    }

    /// List active channels, optionally filtered by glob pattern.
    pub fn active_channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        self.channels
            .keys()
            .filter(|ch| match pattern {
                Some(pat) => crate::command::key::glob_match(pat, ch),
                None => true,
            })
            .cloned()
            .collect()
    }

    /// Return subscriber counts for specific channels.
    pub fn numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .channels
                    .get(ch)
                    .map(|subs| subs.len() as i64)
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Return total number of pattern subscriptions across all patterns.
    pub fn numpat(&self) -> usize {
        self.patterns.iter().map(|(_, subs)| subs.len()).sum()
    }

    /// Count channels this subscriber is subscribed to.
    pub fn channel_subscription_count(&self, sub_id: u64) -> usize {
        self.channels
            .values()
            .filter(|subs| subs.iter().any(|s| s.id == sub_id))
            .count()
    }

    /// Count patterns this subscriber is subscribed to.
    pub fn pattern_subscription_count(&self, sub_id: u64) -> usize {
        self.patterns
            .iter()
            .filter(|(_, subs)| subs.iter().any(|s| s.id == sub_id))
            .count()
    }

    /// Total subscription count (channels + patterns) for a subscriber.
    pub fn total_subscription_count(&self, sub_id: u64) -> usize {
        self.channel_subscription_count(sub_id) + self.pattern_subscription_count(sub_id)
    }
}

// -- Pre-serialization helpers for zero-copy fan-out --
//
// NOTE: Pub/sub messages use Array + BulkString frames, which serialize identically
// in RESP2 and RESP3. When proper RESP3 Push (`>`) framing is added, these helpers
// must produce both RESP2 and RESP3 variants, with the subscriber storing which
// protocol version its client uses.

/// Pre-serialize a "message" delivery into RESP2 wire bytes.
/// Called once per PUBLISH; the returned Bytes is cloned (refcount bump) per subscriber.
#[inline]
fn serialize_message_bytes(channel: &Bytes, payload: &Bytes) -> Bytes {
    // *3\r\n$7\r\nmessage\r\n$<chlen>\r\n<ch>\r\n$<plen>\r\n<payload>\r\n
    let capacity = 32 + channel.len() + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    crate::protocol::serialize(&message_frame(channel, payload), &mut buf);
    buf.freeze()
}

/// Pre-serialize a "pmessage" delivery into RESP2 wire bytes.
#[inline]
fn serialize_pmessage_bytes(pattern: &Bytes, channel: &Bytes, payload: &Bytes) -> Bytes {
    let capacity = 48 + pattern.len() + channel.len() + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    crate::protocol::serialize(&pmessage_frame(pattern, channel, payload), &mut buf);
    buf.freeze()
}

// -- Message frame helpers --

/// Build a subscribe confirmation response frame.
pub fn subscribe_response(channel: &Bytes, count: usize) -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"subscribe")),
        Frame::BulkString(channel.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Build an unsubscribe confirmation response frame.
pub fn unsubscribe_response(channel: &Bytes, count: usize) -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"unsubscribe")),
        Frame::BulkString(channel.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Build a psubscribe confirmation response frame.
pub fn psubscribe_response(pattern: &Bytes, count: usize) -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"psubscribe")),
        Frame::BulkString(pattern.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Build a punsubscribe confirmation response frame.
pub fn punsubscribe_response(pattern: &Bytes, count: usize) -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"punsubscribe")),
        Frame::BulkString(pattern.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Build a message delivery frame for exact-channel subscription.
fn message_frame(channel: &Bytes, payload: &Bytes) -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"message")),
        Frame::BulkString(channel.clone()),
        Frame::BulkString(payload.clone()),
    ])
}

/// Build a pmessage delivery frame for pattern subscription.
fn pmessage_frame(pattern: &Bytes, channel: &Bytes, payload: &Bytes) -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"pmessage")),
        Frame::BulkString(pattern.clone()),
        Frame::BulkString(channel.clone()),
        Frame::BulkString(payload.clone()),
    ])
}

#[cfg(all(test, feature = "runtime-tokio"))]
mod tests {
    use super::*;
    use crate::protocol::ParseConfig;
    use crate::runtime::channel;

    /// Parse pre-serialized RESP bytes back into a Frame for assertion.
    fn parse_resp(data: &[u8]) -> Frame {
        let mut buf = BytesMut::from(data);
        crate::protocol::parse(&mut buf, &ParseConfig::default())
            .expect("valid RESP")
            .expect("complete frame")
    }

    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let mut registry = PubSubRegistry::new();
        let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
        let sub = Subscriber::new(tx, 1);
        let channel = Bytes::from_static(b"news");

        registry.subscribe(channel.clone(), sub);

        let count = registry.publish(&channel, &Bytes::from_static(b"hello"));
        assert_eq!(count, 1);

        let msg = rx.recv_async().await.unwrap();
        let parsed = parse_resp(&msg);
        assert_eq!(
            parsed,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"message")),
                Frame::BulkString(Bytes::from_static(b"news")),
                Frame::BulkString(Bytes::from_static(b"hello")),
            ])
        );
    }

    #[tokio::test]
    async fn test_psubscribe_glob() {
        let mut registry = PubSubRegistry::new();
        let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
        let sub = Subscriber::new(tx, 1);
        let pattern = Bytes::from_static(b"news.*");

        registry.psubscribe(pattern.clone(), sub);

        let channel = Bytes::from_static(b"news.sports");
        let count = registry.publish(&channel, &Bytes::from_static(b"goal!"));
        assert_eq!(count, 1);

        let msg = rx.recv_async().await.unwrap();
        let parsed = parse_resp(&msg);
        assert_eq!(
            parsed,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"pmessage")),
                Frame::BulkString(Bytes::from_static(b"news.*")),
                Frame::BulkString(Bytes::from_static(b"news.sports")),
                Frame::BulkString(Bytes::from_static(b"goal!")),
            ])
        );
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let mut registry = PubSubRegistry::new();
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        let sub = Subscriber::new(tx, 1);
        let channel = Bytes::from_static(b"news");

        registry.subscribe(channel.clone(), sub);
        registry.unsubscribe(b"news", 1);

        let count = registry.publish(&channel, &Bytes::from_static(b"hello"));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_slow_subscriber_disconnected() {
        let mut registry = PubSubRegistry::new();
        // capacity-1 channel: immediately full after one message
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(1);
        let sub = Subscriber::new(tx, 1);
        let channel = Bytes::from_static(b"news");

        registry.subscribe(channel.clone(), sub);

        // First publish fills the buffer
        let count = registry.publish(&channel, &Bytes::from_static(b"msg1"));
        assert_eq!(count, 1);

        // Second publish: buffer full, subscriber should be removed
        let count = registry.publish(&channel, &Bytes::from_static(b"msg2"));
        assert_eq!(count, 0);

        // Subscriber should now be gone
        assert_eq!(registry.channel_subscription_count(1), 0);
    }

    #[tokio::test]
    async fn test_publish_returns_count() {
        let mut registry = PubSubRegistry::new();
        let (tx1, _rx1) = channel::mpsc_bounded::<Bytes>(16);
        let (tx2, _rx2) = channel::mpsc_bounded::<Bytes>(16);
        let sub1 = Subscriber::new(tx1, 1);
        let sub2 = Subscriber::new(tx2, 2);
        let channel = Bytes::from_static(b"news");

        registry.subscribe(channel.clone(), sub1);
        registry.subscribe(channel.clone(), sub2);

        let count = registry.publish(&channel, &Bytes::from_static(b"hello"));
        assert_eq!(count, 2);
    }

    #[test]
    fn test_unsubscribe_all() {
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        let mut registry = PubSubRegistry::new();
        let sub1 = Subscriber::new(tx.clone(), 1);
        let sub2 = Subscriber::new(tx, 1); // same id, different channels

        registry.subscribe(Bytes::from_static(b"ch1"), sub1);
        registry.subscribe(Bytes::from_static(b"ch2"), sub2);

        let removed = registry.unsubscribe_all(1);
        assert_eq!(removed.len(), 2);
        assert_eq!(registry.channel_subscription_count(1), 0);
    }

    #[test]
    fn test_active_channels_no_filter() {
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        let mut registry = PubSubRegistry::new();
        registry.subscribe(Bytes::from_static(b"news"), Subscriber::new(tx.clone(), 1));
        registry.subscribe(
            Bytes::from_static(b"sports"),
            Subscriber::new(tx.clone(), 2),
        );
        registry.subscribe(Bytes::from_static(b"weather"), Subscriber::new(tx, 3));

        let mut channels = registry.active_channels(None);
        channels.sort();
        assert_eq!(channels.len(), 3);
        assert!(channels.contains(&Bytes::from_static(b"news")));
        assert!(channels.contains(&Bytes::from_static(b"sports")));
        assert!(channels.contains(&Bytes::from_static(b"weather")));
    }

    #[test]
    fn test_active_channels_with_glob() {
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        let mut registry = PubSubRegistry::new();
        registry.subscribe(
            Bytes::from_static(b"news.a"),
            Subscriber::new(tx.clone(), 1),
        );
        registry.subscribe(
            Bytes::from_static(b"news.b"),
            Subscriber::new(tx.clone(), 2),
        );
        registry.subscribe(Bytes::from_static(b"sports"), Subscriber::new(tx, 3));

        let channels = registry.active_channels(Some(b"news.*"));
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&Bytes::from_static(b"news.a")));
        assert!(channels.contains(&Bytes::from_static(b"news.b")));
    }

    #[test]
    fn test_numsub() {
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        let mut registry = PubSubRegistry::new();
        registry.subscribe(Bytes::from_static(b"ch1"), Subscriber::new(tx.clone(), 1));
        registry.subscribe(Bytes::from_static(b"ch1"), Subscriber::new(tx.clone(), 2));
        registry.subscribe(Bytes::from_static(b"ch2"), Subscriber::new(tx, 3));

        let result = registry.numsub(&[
            Bytes::from_static(b"ch1"),
            Bytes::from_static(b"ch2"),
            Bytes::from_static(b"ch3"),
        ]);
        assert_eq!(result[0], (Bytes::from_static(b"ch1"), 2));
        assert_eq!(result[1], (Bytes::from_static(b"ch2"), 1));
        assert_eq!(result[2], (Bytes::from_static(b"ch3"), 0));
    }

    #[test]
    fn test_numpat() {
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        let mut registry = PubSubRegistry::new();
        registry.psubscribe(Bytes::from_static(b"a.*"), Subscriber::new(tx.clone(), 1));
        registry.psubscribe(Bytes::from_static(b"b.*"), Subscriber::new(tx, 2));

        assert_eq!(registry.numpat(), 2);
    }

    #[test]
    fn test_punsubscribe_all() {
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        let mut registry = PubSubRegistry::new();
        let sub1 = Subscriber::new(tx.clone(), 1);
        let sub2 = Subscriber::new(tx, 1);

        registry.psubscribe(Bytes::from_static(b"news.*"), sub1);
        registry.psubscribe(Bytes::from_static(b"sports.*"), sub2);

        let removed = registry.punsubscribe_all(1);
        assert_eq!(removed.len(), 2);
        assert_eq!(registry.pattern_subscription_count(1), 0);
    }
}
