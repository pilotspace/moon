//! Pub/Sub: per-shard channel/pattern registries with cross-shard fan-out.
//!
//! # Ordering and consistency guarantees (C1, 2026-07 pub/sub review)
//!
//! **Same-connection write→publish ordering IS guaranteed.** A connection's
//! commands are processed strictly in order by its handler: a `PUBLISH` is not
//! dispatched until every preceding command on that connection (including
//! cross-shard writes, which the handler awaits) has completed. Therefore a
//! subscriber that receives a message may immediately read any key the
//! publisher wrote *before* the `PUBLISH` on the same connection and observe
//! the new value — the classic cache-invalidation pattern
//! (`SET k v; PUBLISH ch k`) is safe. This matches Redis semantics and is
//! locked in by the `pubsub_kv_ordering` integration test.
//!
//! **Cross-connection / cross-channel ordering is NOT guaranteed.** Publishes
//! from different connections may be delivered in any relative order (each
//! shard fans out independently), and delivery is at-most-once: a slow
//! subscriber whose buffer is full is dropped, and a subscriber that
//! (un)subscribes concurrently with an in-flight publish may miss or still
//! receive that one message. Redis makes the same trades.
//!
//! Delivery within one (publisher connection → subscriber connection) pair is
//! FIFO: messages traverse a single bounded mpsc per subscriber.

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
        let mut slow_drops: i64 = 0;

        // Exact channel subscribers — lazy pre-serialize RESP2/RESP3 variants at most once each
        if let Some(subs) = self.channels.get_mut(channel) {
            let mut resp2_bytes: Option<Bytes> = None;
            let mut resp3_bytes: Option<Bytes> = None;
            let before = subs.len();
            subs.retain(|sub| {
                let data = if sub.is_resp3 {
                    resp3_bytes
                        .get_or_insert_with(|| serialize_message_bytes_push(channel, message))
                        .clone()
                } else {
                    resp2_bytes
                        .get_or_insert_with(|| serialize_message_bytes(channel, message))
                        .clone()
                };
                if sub.try_send(data) {
                    count += 1;
                    true
                } else {
                    false // slow subscriber, remove
                }
            });
            slow_drops += (before - subs.len()) as i64;
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }

        // Pattern subscribers — only iterate if patterns exist
        if !self.patterns.is_empty() {
            let mut had_removals = false;
            for (pattern, subs) in &mut self.patterns {
                if glob_match(pattern, channel) {
                    let mut resp2_bytes: Option<Bytes> = None;
                    let mut resp3_bytes: Option<Bytes> = None;
                    let before = subs.len();
                    subs.retain(|sub| {
                        let data = if sub.is_resp3 {
                            resp3_bytes
                                .get_or_insert_with(|| {
                                    serialize_pmessage_bytes_push(pattern, channel, message)
                                })
                                .clone()
                        } else {
                            resp2_bytes
                                .get_or_insert_with(|| {
                                    serialize_pmessage_bytes(pattern, channel, message)
                                })
                                .clone()
                        };
                        if sub.try_send(data) {
                            count += 1;
                            true
                        } else {
                            false
                        }
                    });
                    if subs.len() < before {
                        slow_drops += (before - subs.len()) as i64;
                        had_removals = true;
                    }
                }
            }
            // Only clean up if we actually removed subscribers
            if had_removals {
                self.patterns.retain(|(_, subs)| !subs.is_empty());
            }
        }

        if count > 0 {
            crate::admin::metrics_setup::record_pubsub_published();
        }
        for _ in 0..slow_drops {
            crate::admin::metrics_setup::record_pubsub_slow_drop();
        }

        count
    }

    /// Remove specific (channel, subscriber-id) pairs — the slow-subscriber
    /// reconciliation pass for [`publish_shared`]. Also prunes emptied
    /// channel/pattern entries.
    fn remove_slow(&mut self, channel: &Bytes, slow_exact: &[u64], slow_patterns: &[(Bytes, u64)]) {
        if !slow_exact.is_empty() {
            if let Some(subs) = self.channels.get_mut(channel) {
                subs.retain(|s| !slow_exact.contains(&s.id));
                if subs.is_empty() {
                    self.channels.remove(channel);
                }
            }
        }
        if !slow_patterns.is_empty() {
            self.patterns.retain_mut(|(p, subs)| {
                subs.retain(|s| {
                    !slow_patterns
                        .iter()
                        .any(|(sp, sid)| *sid == s.id && sp.as_ref() == p.as_ref())
                });
                !subs.is_empty()
            });
        }
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

    /// The DISTINCT patterns this registry holds, for INFO's
    /// `pubsub_patterns`.
    ///
    /// Deliberately not [`Self::numpat`], which sums subscribers per pattern:
    /// INFO reports how many patterns exist, so two clients on one pattern is
    /// one, and the caller unions these across shards to avoid counting a
    /// pattern twice when its subscribers landed on different shard threads.
    pub fn pattern_names(&self) -> Vec<Bytes> {
        self.patterns.iter().map(|(p, _)| p.clone()).collect()
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

/// Publish with the fan-out OUTSIDE the registry lock (P1, 2026-07 pub/sub
/// review): `PubSubRegistry::publish` under a `write()` guard holds the
/// per-shard registry lock for the whole O(N) subscriber loop — at high
/// fan-out (10K cache clients on one invalidation channel) that stalls every
/// concurrent SUBSCRIBE/UNSUBSCRIBE and, on the SPSC path, the whole drain.
///
/// Three phases:
/// 1. snapshot matching subscribers under a brief READ lock (Subscriber is
///    a cheap clone: mpsc sender + id + flag),
/// 2. serialize + `try_send` completely lock-free,
/// 3. only if a slow subscriber was hit, take the WRITE lock briefly to
///    remove exactly those (channel, id) pairs.
///
/// Semantics vs the locked path (documented trade): a subscriber that
/// unsubscribes concurrently with a publish may still receive that one
/// in-flight message, and a subscriber added mid-fan-out may miss it —
/// both allowed by Redis's at-most-once, no-ordering-across-connections
/// pub/sub contract.
pub fn publish_shared(
    lock: &parking_lot::RwLock<PubSubRegistry>,
    channel: &Bytes,
    message: &Bytes,
) -> i64 {
    use smallvec::SmallVec;

    // Phase 1: snapshot under read lock.
    let (exact, pattern_matches): (
        SmallVec<[Subscriber; 8]>,
        SmallVec<[(Bytes, SmallVec<[Subscriber; 8]>); 2]>,
    ) = {
        let reg = lock.read();
        let exact = reg
            .channels
            .get(channel)
            .map(|subs| subs.iter().cloned().collect())
            .unwrap_or_default();
        let pats = reg
            .patterns
            .iter()
            .filter(|(p, _)| glob_match(p, channel))
            .map(|(p, subs)| (p.clone(), subs.iter().cloned().collect()))
            .collect();
        (exact, pats)
    };
    if exact.is_empty() && pattern_matches.is_empty() {
        return 0;
    }

    // Phase 2: serialize once per RESP variant, fan out lock-free.
    let mut count: i64 = 0;
    let mut slow_exact: SmallVec<[u64; 4]> = SmallVec::new();
    let mut slow_patterns: SmallVec<[(Bytes, u64); 4]> = SmallVec::new();
    {
        let mut resp2: Option<Bytes> = None;
        let mut resp3: Option<Bytes> = None;
        for sub in &exact {
            let data = if sub.is_resp3 {
                resp3
                    .get_or_insert_with(|| serialize_message_bytes_push(channel, message))
                    .clone()
            } else {
                resp2
                    .get_or_insert_with(|| serialize_message_bytes(channel, message))
                    .clone()
            };
            if sub.try_send(data) {
                count += 1;
            } else {
                slow_exact.push(sub.id);
            }
        }
    }
    for (pattern, subs) in &pattern_matches {
        let mut resp2: Option<Bytes> = None;
        let mut resp3: Option<Bytes> = None;
        for sub in subs {
            let data = if sub.is_resp3 {
                resp3
                    .get_or_insert_with(|| serialize_pmessage_bytes_push(pattern, channel, message))
                    .clone()
            } else {
                resp2
                    .get_or_insert_with(|| serialize_pmessage_bytes(pattern, channel, message))
                    .clone()
            };
            if sub.try_send(data) {
                count += 1;
            } else {
                slow_patterns.push((pattern.clone(), sub.id));
            }
        }
    }

    // Phase 3: reconcile slow-subscriber removals under a brief write lock.
    let slow_total = (slow_exact.len() + slow_patterns.len()) as i64;
    if slow_total > 0 {
        lock.write()
            .remove_slow(channel, &slow_exact, &slow_patterns);
    }

    if count > 0 {
        crate::admin::metrics_setup::record_pubsub_published();
    }
    for _ in 0..slow_total {
        crate::admin::metrics_setup::record_pubsub_slow_drop();
    }
    count
}

// -- Pre-serialization helpers for zero-copy fan-out --
//
// G-2: Pub/sub envelopes are framed as RESP2 Array (`*`) for legacy clients and
// RESP3 Push (`>`) for clients that negotiated HELLO 3. `publish()` lazily
// pre-serializes each variant at most once per PUBLISH (on demand) and sends
// the matching Bytes to each Subscriber based on its `is_resp3` flag.

/// Pre-serialize a "message" delivery into RESP2 (Array-framed) wire bytes.
/// Called once per PUBLISH; the returned Bytes is cloned (refcount bump) per subscriber.
#[inline]
fn serialize_message_bytes(channel: &Bytes, payload: &Bytes) -> Bytes {
    // *3\r\n$7\r\nmessage\r\n$<chlen>\r\n<ch>\r\n$<plen>\r\n<payload>\r\n
    let capacity = 32 + channel.len() + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    crate::protocol::serialize(&message_frame(channel, payload), &mut buf);
    buf.freeze()
}

/// Pre-serialize a "message" delivery into RESP3 (Push-framed) wire bytes.
#[inline]
fn serialize_message_bytes_push(channel: &Bytes, payload: &Bytes) -> Bytes {
    // >3\r\n$7\r\nmessage\r\n$<chlen>\r\n<ch>\r\n$<plen>\r\n<payload>\r\n
    let capacity = 32 + channel.len() + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    crate::protocol::serialize_resp3(&message_frame_push(channel, payload), &mut buf);
    buf.freeze()
}

/// Pre-serialize a "pmessage" delivery into RESP2 (Array-framed) wire bytes.
#[inline]
fn serialize_pmessage_bytes(pattern: &Bytes, channel: &Bytes, payload: &Bytes) -> Bytes {
    let capacity = 48 + pattern.len() + channel.len() + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    crate::protocol::serialize(&pmessage_frame(pattern, channel, payload), &mut buf);
    buf.freeze()
}

/// Pre-serialize a "pmessage" delivery into RESP3 (Push-framed) wire bytes.
#[inline]
fn serialize_pmessage_bytes_push(pattern: &Bytes, channel: &Bytes, payload: &Bytes) -> Bytes {
    let capacity = 48 + pattern.len() + channel.len() + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    crate::protocol::serialize_resp3(&pmessage_frame_push(pattern, channel, payload), &mut buf);
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

/// Build a RESP3 Push-framed message delivery for exact-channel subscription.
fn message_frame_push(channel: &Bytes, payload: &Bytes) -> Frame {
    Frame::Push(framevec![
        Frame::BulkString(Bytes::from_static(b"message")),
        Frame::BulkString(channel.clone()),
        Frame::BulkString(payload.clone()),
    ])
}

/// Build a RESP3 Push-framed pmessage delivery for pattern subscription.
fn pmessage_frame_push(pattern: &Bytes, channel: &Bytes, payload: &Bytes) -> Frame {
    Frame::Push(framevec![
        Frame::BulkString(Bytes::from_static(b"pmessage")),
        Frame::BulkString(pattern.clone()),
        Frame::BulkString(channel.clone()),
        Frame::BulkString(payload.clone()),
    ])
}

/// Instance-wide `(pubsub_channels, pubsub_patterns)` for INFO.
///
/// Unions across every shard's registry rather than summing: a channel with
/// subscribers on two shard threads exists in two registries, and reporting it
/// twice would make a healthy fan-out look like a leak. Mirrors exactly what
/// `PUBSUB CHANNELS` / `PUBSUB NUMPAT` scatter-gather, so the two surfaces
/// cannot disagree.
pub fn instance_pubsub_counts(
    registries: &[std::sync::Arc<parking_lot::RwLock<PubSubRegistry>>],
) -> (usize, usize) {
    let mut channels: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
    let mut patterns: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
    for reg in registries {
        let guard = reg.read();
        channels.extend(guard.active_channels(None));
        patterns.extend(guard.pattern_names());
    }
    (channels.len(), patterns.len())
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

    #[tokio::test]
    async fn test_publish_shared_delivers_and_counts() {
        let lock = parking_lot::RwLock::new(PubSubRegistry::new());
        let (tx1, rx1) = channel::mpsc_bounded::<Bytes>(16);
        let (tx2, _rx2) = channel::mpsc_bounded::<Bytes>(16);
        let channel = Bytes::from_static(b"news");
        {
            let mut reg = lock.write();
            reg.subscribe(channel.clone(), Subscriber::new(tx1, 1));
            reg.subscribe(channel.clone(), Subscriber::new(tx2, 2));
        }

        let count = publish_shared(&lock, &channel, &Bytes::from_static(b"hello"));
        assert_eq!(count, 2);

        let msg = rx1.recv_async().await.unwrap();
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
    async fn test_publish_shared_pattern_delivery() {
        let lock = parking_lot::RwLock::new(PubSubRegistry::new());
        let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
        lock.write()
            .psubscribe(Bytes::from_static(b"news.*"), Subscriber::new(tx, 1));

        let channel = Bytes::from_static(b"news.sports");
        let count = publish_shared(&lock, &channel, &Bytes::from_static(b"goal!"));
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
    async fn test_publish_shared_removes_slow_subscriber() {
        // Parity with test_slow_subscriber_disconnected on the locked path.
        let lock = parking_lot::RwLock::new(PubSubRegistry::new());
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(1);
        let channel = Bytes::from_static(b"news");
        lock.write()
            .subscribe(channel.clone(), Subscriber::new(tx, 1));

        // First publish fills the capacity-1 buffer.
        assert_eq!(
            publish_shared(&lock, &channel, &Bytes::from_static(b"msg1")),
            1
        );
        // Second publish: buffer full -> phase-3 reconciliation removes the subscriber.
        assert_eq!(
            publish_shared(&lock, &channel, &Bytes::from_static(b"msg2")),
            0
        );
        assert_eq!(lock.read().channel_subscription_count(1), 0);
    }

    #[tokio::test]
    async fn test_publish_shared_removes_slow_pattern_subscriber() {
        let lock = parking_lot::RwLock::new(PubSubRegistry::new());
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(1);
        lock.write()
            .psubscribe(Bytes::from_static(b"news.*"), Subscriber::new(tx, 1));

        let channel = Bytes::from_static(b"news.a");
        assert_eq!(
            publish_shared(&lock, &channel, &Bytes::from_static(b"m1")),
            1
        );
        assert_eq!(
            publish_shared(&lock, &channel, &Bytes::from_static(b"m2")),
            0
        );
        let reg = lock.read();
        assert_eq!(reg.pattern_subscription_count(1), 0);
        assert_eq!(reg.numpat(), 0);
    }

    #[test]
    fn test_publish_shared_no_subscribers_fast_path() {
        let lock = parking_lot::RwLock::new(PubSubRegistry::new());
        assert_eq!(
            publish_shared(
                &lock,
                &Bytes::from_static(b"empty"),
                &Bytes::from_static(b"x")
            ),
            0
        );
    }
}
