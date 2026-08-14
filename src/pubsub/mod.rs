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
    /// Sharded (`SSUBSCRIBE`) channels — a separate namespace from `channels`,
    /// so `SPUBLISH ch` structurally cannot reach a `SUBSCRIBE ch`.
    shard_channels: HashMap<Bytes, Vec<Subscriber>>,
}

impl PubSubRegistry {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            patterns: Vec::new(),
            shard_channels: HashMap::new(),
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

    /// Total subscription count (channels + patterns + sharded) for a
    /// subscriber.
    pub fn total_subscription_count(&self, sub_id: u64) -> usize {
        self.channel_subscription_count(sub_id)
            + self.pattern_subscription_count(sub_id)
            + self.shard_subscription_count(sub_id)
    }

    // ── Sharded pub/sub ─────────────────────────────────────────────────
    //
    // A SEPARATE map, not a flag on the existing one. The namespaces must not
    // leak in either direction — `SPUBLISH ch` never reaches a `SUBSCRIBE ch`
    // and vice versa — and sharing one map keyed by name with a discriminator
    // would make that a filtering rule every call site has to remember rather
    // than a structural guarantee.
    //
    // Standalone semantics only: in a real cluster `SSUBSCRIBE` is served by
    // the slot's owner, which is `cluster-client-bootstrap`'s territory. What
    // is contracted here is what a standalone redis-server does.

    /// Subscribe to a sharded channel.
    pub fn ssubscribe(&mut self, channel: Bytes, sub: Subscriber) {
        self.shard_channels.entry(channel).or_default().push(sub);
    }

    /// Unsubscribe from a sharded channel by subscriber ID.
    pub fn sunsubscribe(&mut self, channel: &[u8], sub_id: u64) {
        if let Some(subs) = self.shard_channels.get_mut(channel) {
            subs.retain(|s| s.id != sub_id);
            if subs.is_empty() {
                self.shard_channels.remove(channel);
            }
        }
    }

    /// Remove a subscriber from every sharded channel. Returns those channels.
    pub fn sunsubscribe_all(&mut self, sub_id: u64) -> Vec<Bytes> {
        let mut removed = Vec::new();
        self.shard_channels.retain(|channel, subs| {
            let before = subs.len();
            subs.retain(|s| s.id != sub_id);
            if subs.len() < before {
                removed.push(channel.clone());
            }
            !subs.is_empty()
        });
        removed
    }

    /// Count sharded channels this subscriber is subscribed to.
    pub fn shard_subscription_count(&self, sub_id: u64) -> usize {
        self.shard_channels
            .values()
            .filter(|subs| subs.iter().any(|s| s.id == sub_id))
            .count()
    }

    /// List active sharded channels, optionally filtered by glob pattern.
    pub fn active_shard_channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        self.shard_channels
            .keys()
            .filter(|ch| match pattern {
                Some(pat) => crate::command::key::glob_match(pat, ch),
                None => true,
            })
            .cloned()
            .collect()
    }

    /// Subscriber counts for specific sharded channels.
    pub fn shard_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .shard_channels
                    .get(ch)
                    .map(|subs| subs.len() as i64)
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Publish to a sharded channel. Returns how many subscribers received it.
    ///
    /// Deliberately has no pattern leg: `PSUBSCRIBE` does not match sharded
    /// channels in Redis either.
    pub fn spublish(&mut self, channel: &Bytes, message: &Bytes) -> i64 {
        let mut count: i64 = 0;
        let mut slow_drops: i64 = 0;
        if let Some(subs) = self.shard_channels.get_mut(channel) {
            let mut resp2_bytes: Option<Bytes> = None;
            let mut resp3_bytes: Option<Bytes> = None;
            let before = subs.len();
            subs.retain(|sub| {
                let data = if sub.is_resp3 {
                    resp3_bytes
                        .get_or_insert_with(|| serialize_smessage_bytes(channel, message, true))
                        .clone()
                } else {
                    resp2_bytes
                        .get_or_insert_with(|| serialize_smessage_bytes(channel, message, false))
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
                self.shard_channels.remove(channel);
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
}

/// `SPUBLISH` with the fan-out OUTSIDE the registry lock.
///
/// Mirrors [`publish_shared`] and exists for the same reason: holding the
/// per-shard registry lock across an O(N) subscriber loop stalls every other
/// connection on that shard. Simpler than its plain counterpart because there
/// is no pattern leg — `PSUBSCRIBE` does not match sharded channels.
pub fn spublish_shared(
    lock: &parking_lot::RwLock<PubSubRegistry>,
    channel: &Bytes,
    message: &Bytes,
) -> i64 {
    use smallvec::SmallVec;

    // Phase 1: snapshot under the read lock.
    let subs: SmallVec<[Subscriber; 8]> = {
        let reg = lock.read();
        reg.shard_channels
            .get(channel)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    };
    if subs.is_empty() {
        return 0;
    }

    // Phase 2: serialize once per protocol variant, fan out lock-free.
    let mut count: i64 = 0;
    let mut slow: SmallVec<[u64; 4]> = SmallVec::new();
    {
        let mut resp2: Option<Bytes> = None;
        let mut resp3: Option<Bytes> = None;
        for sub in &subs {
            let data = if sub.is_resp3 {
                resp3
                    .get_or_insert_with(|| serialize_smessage_bytes(channel, message, true))
                    .clone()
            } else {
                resp2
                    .get_or_insert_with(|| serialize_smessage_bytes(channel, message, false))
                    .clone()
            };
            if sub.try_send(data) {
                count += 1;
            } else {
                slow.push(sub.id);
            }
        }
    }

    // Phase 3: reconcile slow subscribers under the write lock.
    if !slow.is_empty() {
        let mut reg = lock.write();
        if let Some(entry) = reg.shard_channels.get_mut(channel) {
            entry.retain(|s| !slow.contains(&s.id));
            if entry.is_empty() {
                reg.shard_channels.remove(channel);
            }
        }
        for _ in 0..slow.len() {
            crate::admin::metrics_setup::record_pubsub_slow_drop();
        }
    }
    if count > 0 {
        crate::admin::metrics_setup::record_pubsub_published();
    }
    count
}

/// Pre-serialize an `smessage` delivery.
///
/// `smessage`, not `message`: the sharded delivery carries its own event name,
/// so a client subscribed to both namespaces can tell them apart.
#[inline]
fn serialize_smessage_bytes(channel: &Bytes, payload: &Bytes, resp3: bool) -> Bytes {
    let capacity = 32 + channel.len() + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    let frame = Frame::Push(framevec![
        Frame::BulkString(Bytes::from_static(b"smessage")),
        Frame::BulkString(channel.clone()),
        Frame::BulkString(payload.clone()),
    ]);
    if resp3 {
        crate::protocol::serialize_resp3(&frame, &mut buf);
    } else {
        // RESP2 downgrades Push to Array — the same rule the confirmations use.
        crate::protocol::serialize(&frame, &mut buf);
    }
    buf.freeze()
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

/// Build a pub/sub confirmation frame (`subscribe`, `unsubscribe`, …).
///
/// Always a [`Frame::Push`], never an `Array`, and that is the whole fix for
/// the RESP3 confirmation divergence. A confirmation IS out-of-band pub/sub
/// traffic — the same kind of thing a `message` delivery is — so the frame is
/// built as what it means and each protocol's serializer renders it in that
/// protocol's form: `serialize_resp3` writes `>`, while RESP2 `serialize`
/// downgrades Push to `*` (`src/protocol/serialize.rs`, the same mechanism
/// `Frame::Set` relies on). RESP2 clients therefore see byte-for-byte what
/// they saw before.
///
/// The alternative — threading a `resp3: bool` down to every call site — was
/// rejected once it became clear the serializer already encodes exactly this
/// rule: a second copy of the decision is a second thing to get out of sync,
/// and the three handlers drifting apart is precisely what this task is
/// cleaning up.
///
/// `name` is a static verb; `channel` is `None` only for the
/// `UNSUBSCRIBE`-with-no-arguments case on a connection subscribed to nothing,
/// where Redis sends a Null channel name rather than an empty string.
#[inline]
fn confirmation(name: &'static [u8], channel: Option<&Bytes>, count: usize) -> Frame {
    Frame::Push(framevec![
        Frame::BulkString(Bytes::from_static(name)),
        match channel {
            Some(c) => Frame::BulkString(c.clone()),
            None => Frame::Null,
        },
        Frame::Integer(count as i64),
    ])
}

/// Build a subscribe confirmation response frame.
pub fn subscribe_response(channel: &Bytes, count: usize) -> Frame {
    confirmation(b"subscribe", Some(channel), count)
}

/// Build an unsubscribe confirmation response frame.
pub fn unsubscribe_response(channel: &Bytes, count: usize) -> Frame {
    confirmation(b"unsubscribe", Some(channel), count)
}

/// Build the `UNSUBSCRIBE`-with-no-arguments reply when no CHANNEL was removed.
///
/// Redis names a Null channel (`$-1`) here, not an empty bulk string (`$0`) —
/// measured, and a statically-typed client decodes the two differently.
///
/// `count` is the connection's REMAINING total subscription count, not zero: a
/// connection holding only pattern subscriptions removes no channel here but
/// still reports what it is subscribed to.
pub fn unsubscribe_none_response(count: usize) -> Frame {
    confirmation(b"unsubscribe", None, count)
}

/// Build a psubscribe confirmation response frame.
pub fn psubscribe_response(pattern: &Bytes, count: usize) -> Frame {
    confirmation(b"psubscribe", Some(pattern), count)
}

/// Build a punsubscribe confirmation response frame.
pub fn punsubscribe_response(pattern: &Bytes, count: usize) -> Frame {
    confirmation(b"punsubscribe", Some(pattern), count)
}

/// Build the `PUNSUBSCRIBE`-with-no-arguments reply when nothing is subscribed.
pub fn punsubscribe_none_response(count: usize) -> Frame {
    confirmation(b"punsubscribe", None, count)
}

/// Build an ssubscribe confirmation response frame (sharded pub/sub).
pub fn ssubscribe_response(channel: &Bytes, count: usize) -> Frame {
    confirmation(b"ssubscribe", Some(channel), count)
}

/// Build an sunsubscribe confirmation response frame (sharded pub/sub).
pub fn sunsubscribe_response(channel: &Bytes, count: usize) -> Frame {
    confirmation(b"sunsubscribe", Some(channel), count)
}

/// Build the `SUNSUBSCRIBE`-with-no-arguments reply when nothing is subscribed.
pub fn sunsubscribe_none_response(count: usize) -> Frame {
    confirmation(b"sunsubscribe", None, count)
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
    async fn test_sharded_namespace_is_isolated_from_plain() {
        // The invariant the whole sharded design rests on. `SPUBLISH ch` and
        // `SUBSCRIBE ch` name the SAME channel and must still be different
        // destinations — which is why the registry keeps two maps rather than
        // one map with a flag every call site has to remember to check.
        let mut registry = PubSubRegistry::new();
        let (tx_plain, rx_plain) = channel::mpsc_bounded::<Bytes>(16);
        let (tx_shard, rx_shard) = channel::mpsc_bounded::<Bytes>(16);
        let ch = Bytes::from_static(b"news");

        registry.subscribe(ch.clone(), Subscriber::new(tx_plain, 1));
        registry.ssubscribe(ch.clone(), Subscriber::new(tx_shard, 2));

        // Each publish reaches exactly its own namespace — never both, never
        // the other one.
        assert_eq!(registry.spublish(&ch, &Bytes::from_static(b"s")), 1);
        assert_eq!(registry.publish(&ch, &Bytes::from_static(b"p")), 1);

        let got_shard = rx_shard.recv_async().await.unwrap();
        assert_eq!(
            parse_resp(&got_shard),
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"smessage")),
                Frame::BulkString(Bytes::from_static(b"news")),
                Frame::BulkString(Bytes::from_static(b"s")),
            ]),
            "the sharded subscriber gets `smessage`, and gets it exactly once"
        );
        assert!(
            rx_shard.try_recv().is_err(),
            "the plain PUBLISH must not have leaked into the sharded namespace"
        );

        let got_plain = rx_plain.recv_async().await.unwrap();
        assert_eq!(
            parse_resp(&got_plain),
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"message")),
                Frame::BulkString(Bytes::from_static(b"news")),
                Frame::BulkString(Bytes::from_static(b"p")),
            ])
        );
        assert!(
            rx_plain.try_recv().is_err(),
            "the SPUBLISH must not have leaked into the plain namespace"
        );
    }

    #[tokio::test]
    async fn test_spublish_shared_removes_slow_subscriber() {
        // Parity with test_publish_shared_removes_slow_subscriber: the sharded
        // fan-out reconciles a subscriber that cannot keep up, rather than
        // blocking the publisher on it.
        let lock = parking_lot::RwLock::new(PubSubRegistry::new());
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(1);
        let ch = Bytes::from_static(b"news");
        lock.write().ssubscribe(ch.clone(), Subscriber::new(tx, 1));

        assert_eq!(spublish_shared(&lock, &ch, &Bytes::from_static(b"m1")), 1);
        assert_eq!(spublish_shared(&lock, &ch, &Bytes::from_static(b"m2")), 0);
        assert_eq!(lock.read().shard_subscription_count(1), 0);
        assert!(
            lock.read().active_shard_channels(None).is_empty(),
            "reconciling the last subscriber must retire the channel, not leave it empty"
        );
    }

    #[tokio::test]
    async fn test_sunsubscribe_all_returns_channels_for_unpropagation() {
        // Teardown depends on this return value: RESET and disconnect feed it
        // to `unpropagate_shard_subscription`. A version that cleaned the
        // registry but returned nothing would leave every other shard fanning
        // SPUBLISH at a shard with no receiver, forever.
        let mut registry = PubSubRegistry::new();
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
        registry.ssubscribe(Bytes::from_static(b"a"), Subscriber::new(tx.clone(), 7));
        registry.ssubscribe(Bytes::from_static(b"b"), Subscriber::new(tx, 7));

        let mut gone = registry.sunsubscribe_all(7);
        gone.sort();
        assert_eq!(
            gone,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            "every sharded channel the connection held must come back for unpropagation"
        );
        assert_eq!(registry.shard_subscription_count(7), 0);
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
