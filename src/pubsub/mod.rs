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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};

use crate::command::key::glob_match;
use crate::protocol::Frame;

use self::subscriber::Subscriber;
use crate::framevec;
static NEXT_SUBSCRIBER_ID: AtomicU64 = AtomicU64::new(1);

/// One channel's (or pattern's) subscribers, in subscription order — the
/// delivery order.
///
/// Shared, not owned (moon#1180): PUBLISH snapshots the list with ONE
/// `Arc::clone` under the read lock and fans out after releasing it, instead
/// of cloning every subscriber's flume `Sender` (two atomic RMWs, two more on
/// drop) per message. A `Vec` behind the `Arc`, not a slice, so that
/// SUBSCRIBE / UNSUBSCRIBE / slow-drop mutate it IN PLACE through
/// [`Arc::make_mut`] (moon#1227 review M2): the list is copied only when a
/// publish snapshot of it is alive at that moment, and the copy is then the
/// registry's own, so the next change is in place again. Rebuilding an
/// `Arc<[Subscriber]>` on every call cloned all N handles and allocated
/// twice under the registry write lock — N²/2 handle clones to fill one
/// channel.
type SubList = Arc<Vec<Subscriber>>;

/// Append a subscriber: in place, amortized O(1), unless a publish snapshot
/// shares the list (then one copy, see [`SubList`]).
#[inline]
fn list_push(subs: &mut SubList, sub: Subscriber) {
    Arc::make_mut(subs).push(sub);
}

/// Remove `sub_id`, keeping everyone else's order.
///
/// Returns `true` when the id was present and removed, `false` when it was
/// absent — then the list is not touched at all (no copy even under a live
/// snapshot), so callers can report the true "was removed" set the
/// remote-subscriber maps rely on.
#[inline]
fn list_remove_id(subs: &mut SubList, sub_id: u64) -> bool {
    if !subs.iter().any(|s| s.id == sub_id) {
        return false;
    }
    Arc::make_mut(subs).retain(|s| s.id != sub_id);
    true
}

/// Drop every subscriber `slow` names (the slow-subscriber eviction), in
/// place like [`list_remove_id`]. Returns `true` when any was present.
#[inline]
fn list_remove_ids(subs: &mut SubList, slow: &[u64]) -> bool {
    if !subs.iter().any(|s| slow.contains(&s.id)) {
        return false;
    }
    Arc::make_mut(subs).retain(|s| !slow.contains(&s.id));
    true
}

/// A keyspace-relevant exact channel / pattern gained its first subscriber
/// (moon#1214 item 2). Called when a channel/pattern entry is CREATED.
#[inline]
fn note_keyspace_added(name: &[u8]) {
    if crate::notify::subscription_targets_keyspace(name) {
        crate::notify::keyspace_listener_added();
    }
}

/// A keyspace-relevant exact channel / pattern lost its last subscriber
/// (moon#1214 item 2). Called when a channel/pattern entry is REMOVED. Paired
/// 1:1 with [`note_keyspace_added`] via the map's present↔absent transitions,
/// so the global count never leaks and (saturating) never goes negative.
#[inline]
fn note_keyspace_removed(name: &[u8]) {
    if crate::notify::subscription_targets_keyspace(name) {
        crate::notify::keyspace_listener_removed();
    }
}

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
    /// Each channel's subscribers, shared with in-flight publishes and
    /// mutated in place otherwise ([`SubList`], moon#1180).
    channels: HashMap<Bytes, SubList>,
    patterns: Vec<(Bytes, SubList)>,
    /// Sharded (`SSUBSCRIBE`) channels — a separate namespace from `channels`,
    /// so `SPUBLISH ch` structurally cannot reach a `SUBSCRIBE ch`.
    shard_channels: HashMap<Bytes, SubList>,
    /// REVERSE index of `channels`: subscriber id -> the channels it joined.
    ///
    /// Teardown used to `retain` over the whole channel map (moon#651) — under
    /// the registry's exclusive lock, three times per disconnect, so every
    /// shard's `PUBLISH` fan-out blocked behind a walk of channels the
    /// departing connection had never heard of. Unlike the tracking table this
    /// map has no cap, so there was no ceiling on the stall.
    ///
    /// Kept in step by `subscribe` and `unsubscribe`. It MAY name a channel the
    /// subscriber is no longer in: `publish` drops slow subscribers from inside
    /// a `retain` over `channels`, which borrows the map exclusively. Teardown
    /// therefore treats a reverse entry as a *candidate* and only reports a
    /// channel it actually removed the subscriber from. Staleness is bounded by
    /// the distinct channels one connection ever joined without unsubscribing —
    /// re-subscribing re-inserts the same set member — so it can never exceed
    /// what the old code walked, and it is paid once, at that connection's own
    /// disconnect.
    sub_channels: HashMap<u64, std::collections::HashSet<Bytes>>,
    /// REVERSE index of `shard_channels`. Same contract as `sub_channels`.
    sub_shard_channels: HashMap<u64, std::collections::HashSet<Bytes>>,
}

impl PubSubRegistry {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            patterns: Vec::new(),
            shard_channels: HashMap::new(),
            sub_channels: HashMap::new(),
            sub_shard_channels: HashMap::new(),
        }
    }

    /// Subscribe to an exact channel.
    pub fn subscribe(&mut self, channel: Bytes, sub: Subscriber) {
        self.sub_channels
            .entry(sub.id)
            .or_default()
            .insert(channel.clone());
        match self.channels.get_mut(&channel) {
            Some(subs) => list_push(subs, sub),
            None => {
                // First subscriber for this channel: a present transition.
                note_keyspace_added(&channel);
                self.channels.insert(channel, Arc::new(vec![sub]));
            }
        }
    }

    /// Unsubscribe from an exact channel by subscriber ID.
    pub fn unsubscribe(&mut self, channel: &[u8], sub_id: u64) {
        // Explicit UNSUBSCRIBE must clear the reverse entry, or a connection
        // cycling through distinct channels would grow it without bound.
        Self::forget_sub_channel(&mut self.sub_channels, sub_id, channel);
        if let Some(subs) = self.channels.get_mut(channel) {
            list_remove_id(subs, sub_id);
            if subs.is_empty() {
                self.channels.remove(channel);
                note_keyspace_removed(channel);
            }
        }
    }

    /// Drop one (subscriber, channel) pair from a reverse index, retiring the
    /// subscriber's entry once it names nothing.
    fn forget_sub_channel(
        index: &mut HashMap<u64, std::collections::HashSet<Bytes>>,
        sub_id: u64,
        channel: &[u8],
    ) {
        if let Some(joined) = index.get_mut(&sub_id) {
            joined.remove(channel);
            if joined.is_empty() {
                index.remove(&sub_id);
            }
        }
    }

    /// Remove `sub_id` from the channels its reverse index names, pruning any
    /// channel it was the last subscriber of. Returns only the channels the
    /// subscriber was *actually* removed from — callers use that list to
    /// unpropagate remote subscription maps, so a stale candidate must not
    /// appear in it.
    fn retire_subscriber(
        channels: &mut HashMap<Bytes, SubList>,
        joined: Option<std::collections::HashSet<Bytes>>,
        sub_id: u64,
        // moon#1214 item 2: exact channels feed keyspace notifications; sharded
        // channels never do, so `sunsubscribe_all` passes `false` and does not
        // touch the listener count.
        count_keyspace: bool,
    ) -> Vec<Bytes> {
        let Some(joined) = joined else {
            return Vec::new();
        };
        let mut removed = Vec::with_capacity(joined.len());
        for channel in joined {
            let Some(subs) = channels.get_mut(&channel) else {
                continue; // channel already gone (last subscriber slow-dropped)
            };
            if !list_remove_id(subs, sub_id) {
                continue; // stale candidate: already slow-dropped from here
            }
            if subs.is_empty() {
                channels.remove(&channel);
                if count_keyspace {
                    note_keyspace_removed(&channel);
                }
            }
            removed.push(channel);
        }
        removed
    }

    /// Subscribe to a glob pattern.
    pub fn psubscribe(&mut self, pattern: Bytes, sub: Subscriber) {
        for (existing_pattern, subs) in &mut self.patterns {
            if existing_pattern.as_ref() == pattern.as_ref() {
                list_push(subs, sub);
                return;
            }
        }
        // New pattern entry: a present transition.
        note_keyspace_added(&pattern);
        self.patterns.push((pattern, Arc::new(vec![sub])));
    }

    /// Unsubscribe from a glob pattern by subscriber ID.
    pub fn punsubscribe(&mut self, pattern: &[u8], sub_id: u64) {
        self.patterns.retain_mut(|(p, subs)| {
            if p.as_ref() == pattern {
                list_remove_id(subs, sub_id);
                let keep = !subs.is_empty();
                if !keep {
                    note_keyspace_removed(p);
                }
                keep
            } else {
                true
            }
        });
    }

    /// Remove subscriber from all channels. Returns list of channels they were in.
    pub fn unsubscribe_all(&mut self, sub_id: u64) -> Vec<Bytes> {
        Self::retire_subscriber(
            &mut self.channels,
            self.sub_channels.remove(&sub_id),
            sub_id,
            true,
        )
    }

    /// Remove subscriber from all patterns. Returns list of patterns they were in.
    pub fn punsubscribe_all(&mut self, sub_id: u64) -> Vec<Bytes> {
        let mut removed = Vec::new();
        self.patterns.retain_mut(|(pattern, subs)| {
            if list_remove_id(subs, sub_id) {
                removed.push(pattern.clone());
            }
            let keep = !subs.is_empty();
            if !keep {
                note_keyspace_removed(pattern);
            }
            keep
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

        // Exact channel subscribers ([`SubList`]): iterate the list and, only
        // if a slow subscriber was hit, drop them from it — in place, keeping
        // the delivery order and the slow-drop eviction semantics.
        if let Some(subs) = self.channels.get_mut(channel) {
            let mut resp2_bytes: Option<Bytes> = None;
            let mut resp3_bytes: Option<Bytes> = None;
            let mut slow: smallvec::SmallVec<[u64; 4]> = smallvec::SmallVec::new();
            for sub in subs.iter() {
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
                } else {
                    slow.push(sub.id);
                }
            }
            if !slow.is_empty() {
                slow_drops += slow.len() as i64;
                list_remove_ids(subs, &slow);
                if subs.is_empty() {
                    self.channels.remove(channel);
                    note_keyspace_removed(channel);
                }
            }
        }

        // Pattern subscribers — only iterate if patterns exist
        if !self.patterns.is_empty() {
            let mut had_removals = false;
            for (pattern, subs) in &mut self.patterns {
                if glob_match(pattern, channel) {
                    let mut resp2_bytes: Option<Bytes> = None;
                    let mut resp3_bytes: Option<Bytes> = None;
                    let mut slow: smallvec::SmallVec<[u64; 4]> = smallvec::SmallVec::new();
                    for sub in subs.iter() {
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
                        } else {
                            slow.push(sub.id);
                        }
                    }
                    if !slow.is_empty() {
                        slow_drops += slow.len() as i64;
                        list_remove_ids(subs, &slow);
                        had_removals = true;
                    }
                }
            }
            // Only clean up if we actually removed subscribers
            if had_removals {
                self.patterns.retain(|(pattern, subs)| {
                    let keep = !subs.is_empty();
                    if !keep {
                        note_keyspace_removed(pattern);
                    }
                    keep
                });
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
                if list_remove_ids(subs, slow_exact) && subs.is_empty() {
                    self.channels.remove(channel);
                    note_keyspace_removed(channel);
                }
            }
        }
        if !slow_patterns.is_empty() {
            self.patterns.retain_mut(|(p, subs)| {
                let hit = subs.iter().any(|s| {
                    slow_patterns
                        .iter()
                        .any(|(sp, sid)| *sid == s.id && sp == p)
                });
                if hit {
                    Arc::make_mut(subs).retain(|s| {
                        !slow_patterns
                            .iter()
                            .any(|(sp, sid)| *sid == s.id && sp.as_ref() == p.as_ref())
                    });
                }
                let keep = !subs.is_empty();
                if !keep {
                    note_keyspace_removed(p);
                }
                keep
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
        self.sub_shard_channels
            .entry(sub.id)
            .or_default()
            .insert(channel.clone());
        match self.shard_channels.get_mut(&channel) {
            Some(subs) => list_push(subs, sub),
            None => {
                self.shard_channels.insert(channel, Arc::new(vec![sub]));
            }
        }
    }

    /// Unsubscribe from a sharded channel by subscriber ID.
    pub fn sunsubscribe(&mut self, channel: &[u8], sub_id: u64) {
        Self::forget_sub_channel(&mut self.sub_shard_channels, sub_id, channel);
        if let Some(subs) = self.shard_channels.get_mut(channel) {
            list_remove_id(subs, sub_id);
            if subs.is_empty() {
                self.shard_channels.remove(channel);
            }
        }
    }

    /// Remove a subscriber from every sharded channel. Returns those channels.
    pub fn sunsubscribe_all(&mut self, sub_id: u64) -> Vec<Bytes> {
        // Sharded channels never carry keyspace notifications, so the listener
        // count is left untouched (moon#1214 item 2).
        Self::retire_subscriber(
            &mut self.shard_channels,
            self.sub_shard_channels.remove(&sub_id),
            sub_id,
            false,
        )
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
            let mut slow: smallvec::SmallVec<[u64; 4]> = smallvec::SmallVec::new();
            for sub in subs.iter() {
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
                } else {
                    slow.push(sub.id);
                }
            }
            if !slow.is_empty() {
                slow_drops += slow.len() as i64;
                list_remove_ids(subs, &slow);
                if subs.is_empty() {
                    self.shard_channels.remove(channel);
                }
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

    // Phase 1: snapshot under the read lock — ONE `Arc::clone` of the whole
    // subscriber list, not a per-subscriber flume `Sender` clone (moon#1180).
    let subs: SubList = {
        let reg = lock.read();
        match reg.shard_channels.get(channel) {
            Some(s) => Arc::clone(s),
            None => return 0,
        }
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
        for sub in subs.iter() {
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

    // Phase 3: reconcile slow subscribers under the write lock. The snapshot
    // is released first, so unless another publish holds one the removal is
    // in place.
    drop(subs);
    if !slow.is_empty() {
        let mut reg = lock.write();
        if let Some(entry) = reg.shard_channels.get_mut(channel) {
            if list_remove_ids(entry, &slow) && entry.is_empty() {
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
/// 1. snapshot the matching subscriber lists under a brief READ lock — one
///    `Arc::clone` per list, not a clone per subscriber (moon#1180),
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

    // Phase 1: snapshot under read lock — ONE `Arc::clone` for the exact channel
    // and one per MATCHING pattern, regardless of how many subscribers each
    // holds (moon#1180). No per-subscriber flume `Sender` clone/drop.
    let (exact, pattern_matches): (Option<SubList>, SmallVec<[(Bytes, SubList); 2]>) = {
        let reg = lock.read();
        let exact = reg.channels.get(channel).map(Arc::clone);
        let pats = reg
            .patterns
            .iter()
            .filter(|(p, _)| glob_match(p, channel))
            .map(|(p, subs)| (p.clone(), Arc::clone(subs)))
            .collect();
        (exact, pats)
    };
    // No exact subscriber is the common case of a pattern-only channel — every
    // keyspace notification a `PSUBSCRIBE __key*` listener gets arrives here
    // through `notify_fanout` — so keep the `Option` rather than allocating an
    // empty list to iterate on every such publish (moon#1227 review m1).
    if exact.as_ref().is_none_or(|subs| subs.is_empty()) && pattern_matches.is_empty() {
        return 0;
    }

    // Phase 2: serialize once per RESP variant, fan out lock-free.
    let mut count: i64 = 0;
    let mut slow_exact: SmallVec<[u64; 4]> = SmallVec::new();
    let mut slow_patterns: SmallVec<[(Bytes, u64); 4]> = SmallVec::new();
    {
        let mut resp2: Option<Bytes> = None;
        let mut resp3: Option<Bytes> = None;
        for sub in exact.as_deref().into_iter().flatten() {
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
        for sub in subs.iter() {
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
    // The snapshots are released first, so unless another publish holds one
    // the removal is in place.
    drop(exact);
    drop(pattern_matches);
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

    /// The reverse index may name channels a subscriber has since been
    /// slow-dropped from, but it must never MISS one it is still in, and the
    /// forward map must never keep an emptied channel entry.
    fn assert_no_missing_reverse_entries(reg: &PubSubRegistry) {
        for (channel, subs) in &reg.channels {
            assert!(
                !subs.is_empty(),
                "empty subscriber list left on {channel:?}"
            );
            for sub in subs.iter() {
                assert!(
                    reg.sub_channels
                        .get(&sub.id)
                        .is_some_and(|joined| joined.contains(channel)),
                    "subscriber {} is in {channel:?} but the reverse index does not say so",
                    sub.id
                );
            }
        }
        for (channel, subs) in &reg.shard_channels {
            assert!(
                !subs.is_empty(),
                "empty subscriber list left on {channel:?}"
            );
            for sub in subs.iter() {
                assert!(
                    reg.sub_shard_channels
                        .get(&sub.id)
                        .is_some_and(|joined| joined.contains(channel)),
                    "subscriber {} is in sharded {channel:?} but the reverse index does not say so",
                    sub.id
                );
            }
        }
    }

    fn live_sub(id: u64) -> Subscriber {
        let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
        std::mem::forget(rx);
        Subscriber::new(tx, id)
    }

    #[test]
    fn disconnect_removes_only_the_departing_subscriber() {
        let mut reg = PubSubRegistry::new();
        let shared = Bytes::from_static(b"shared");
        let solo = Bytes::from_static(b"solo");
        reg.subscribe(shared.clone(), live_sub(1));
        reg.subscribe(shared.clone(), live_sub(2));
        reg.subscribe(solo.clone(), live_sub(1));

        let mut removed = reg.unsubscribe_all(1);
        removed.sort();
        assert_eq!(removed, vec![shared.clone(), solo.clone()]);
        assert_eq!(reg.channels[&shared].len(), 1);
        assert!(
            !reg.channels.contains_key(&solo),
            "a channel with no subscribers left must be dropped"
        );
        assert!(!reg.sub_channels.contains_key(&1));
        assert_no_missing_reverse_entries(&reg);
    }

    #[test]
    fn a_slow_dropped_channel_is_not_reported_as_removed_on_disconnect() {
        // The returned list drives `unpropagate_subscription` against every
        // other shard's remote map, so reporting a channel this subscriber was
        // already dropped from would tear down a subscription it never had.
        let mut reg = PubSubRegistry::new();
        let ch = Bytes::from_static(b"ch");
        let (tx, rx) = channel::mpsc_bounded::<Bytes>(1);
        reg.subscribe(ch.clone(), Subscriber::new(tx, 1));
        // A healthy second subscriber keeps the channel alive, so teardown
        // reaches the "is this candidate stale?" branch rather than the
        // "channel is gone entirely" one.
        reg.subscribe(ch.clone(), live_sub(2));
        // Fill subscriber 1's one-slot buffer, then publish again: its second
        // send fails and `publish` drops it.
        reg.publish(&ch, &Bytes::from_static(b"a"));
        reg.publish(&ch, &Bytes::from_static(b"b"));
        drop(rx);
        assert_eq!(
            reg.channels[&ch].iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![2],
            "slow subscriber was not dropped"
        );
        // The reverse index still names it -- that is the tolerated staleness.
        assert!(reg.sub_channels[&1].contains(&ch));

        assert!(
            reg.unsubscribe_all(1).is_empty(),
            "a channel the subscriber was already dropped from must not be reported"
        );
        assert_eq!(
            reg.channels[&ch].len(),
            1,
            "teardown disturbed a live subscriber"
        );
        assert_no_missing_reverse_entries(&reg);
    }

    #[test]
    fn explicit_unsubscribe_clears_the_reverse_entry() {
        let mut reg = PubSubRegistry::new();
        // A connection cycling through distinct channels must not accumulate.
        for i in 0..50 {
            let ch = Bytes::from(format!("ch:{i}"));
            reg.subscribe(ch.clone(), live_sub(1));
            reg.unsubscribe(&ch, 1);
        }
        assert!(
            !reg.sub_channels.contains_key(&1),
            "reverse index accumulated across subscribe/unsubscribe cycles"
        );
        assert!(reg.channels.is_empty());
        assert_no_missing_reverse_entries(&reg);
    }

    #[test]
    fn resubscribing_after_a_disconnect_works() {
        let mut reg = PubSubRegistry::new();
        let ch = Bytes::from_static(b"ch");
        reg.subscribe(ch.clone(), live_sub(1));
        reg.unsubscribe_all(1);
        reg.subscribe(ch.clone(), live_sub(1));
        assert_eq!(reg.publish(&ch, &Bytes::from_static(b"m")), 1);
        assert_eq!(reg.unsubscribe_all(1), vec![ch]);
        assert_no_missing_reverse_entries(&reg);
    }

    #[test]
    fn sharded_and_exact_namespaces_retire_independently() {
        let mut reg = PubSubRegistry::new();
        let ch = Bytes::from_static(b"ch");
        reg.subscribe(ch.clone(), live_sub(1));
        reg.ssubscribe(ch.clone(), live_sub(1));

        assert_eq!(reg.sunsubscribe_all(1), vec![ch.clone()]);
        assert!(reg.shard_channels.is_empty());
        assert_eq!(
            reg.channels[&ch].len(),
            1,
            "retiring the sharded namespace touched the exact one"
        );
        assert_eq!(reg.unsubscribe_all(1), vec![ch]);
        assert_no_missing_reverse_entries(&reg);
    }

    #[test]
    fn disconnecting_an_unknown_subscriber_leaves_the_registry_alone() {
        let mut reg = PubSubRegistry::new();
        let ch = Bytes::from_static(b"ch");
        reg.subscribe(ch.clone(), live_sub(1));
        assert!(reg.unsubscribe_all(999).is_empty());
        assert!(reg.sunsubscribe_all(999).is_empty());
        assert_eq!(reg.channels[&ch].len(), 1);
        assert_no_missing_reverse_entries(&reg);
    }

    /// moon#1227 review M2 (moon#1180): SUBSCRIBE / UNSUBSCRIBE must not
    /// rebuild the whole subscriber list. The copy-on-write `Arc<[Subscriber]>`
    /// cloned every existing handle (a flume `Sender` clone: two atomic RMWs,
    /// two more on drop, plus two allocations) on each call, so filling one
    /// channel with N subscribers cost N²/2 handle clones. Counted, not timed:
    /// the handle-clone counter is deterministic where wall time is not.
    #[test]
    fn filling_and_draining_a_channel_clones_no_handles_per_call() {
        let mut per_n: Vec<(u64, u64, u64)> = Vec::new();
        for n in [1_000u64, 8_000] {
            let mut reg = PubSubRegistry::new();
            let ch = Bytes::from_static(b"broadcast");
            let pat = Bytes::from_static(b"broad*");
            let fresh: Vec<Subscriber> = (1..=3 * n).map(live_sub).collect();
            let mut fresh = fresh.into_iter();
            let before = subscriber::clones_on_this_thread();
            for _ in 0..n {
                reg.subscribe(ch.clone(), fresh.next().unwrap());
                reg.psubscribe(pat.clone(), fresh.next().unwrap());
                reg.ssubscribe(ch.clone(), fresh.next().unwrap());
            }
            let filled = subscriber::clones_on_this_thread() - before;
            assert_eq!(reg.channels[&ch].len() as u64, n);
            assert_eq!(reg.numpat() as u64, n);
            // Drain: explicit unsubscribes, then the disconnect paths.
            let mut ids = (1..=3 * n).collect::<Vec<u64>>().into_iter();
            for _ in 0..n / 2 {
                reg.unsubscribe(&ch, ids.next().unwrap());
                reg.punsubscribe(&pat, ids.next().unwrap());
                reg.sunsubscribe(&ch, ids.next().unwrap());
            }
            for id in ids {
                reg.unsubscribe_all(id);
                reg.punsubscribe_all(id);
                reg.sunsubscribe_all(id);
            }
            let total = subscriber::clones_on_this_thread() - before;
            assert!(reg.channels.is_empty() && reg.numpat() == 0 && reg.shard_channels.is_empty());
            per_n.push((n, filled, total));
            // O(1) amortized per call: at most one handle clone per
            // subscriber over the whole fill + drain (in fact none, since no
            // publish snapshot is alive here). Checked per size, so a
            // quadratic registry fails at the small one.
            assert!(
                total <= 3 * n,
                "N={n}: {filled} handle clones to fill, {total} in all — not linear ({per_n:?})"
            );
        }
    }

    /// A publish snapshot taken before a SUBSCRIBE/UNSUBSCRIBE is never
    /// mutated under its reader: the registry copies the list once (the
    /// subscribers alive at that moment) and mutates its own copy in place
    /// from then on.
    #[test]
    fn a_live_publish_snapshot_is_copied_once_and_left_intact() {
        let mut reg = PubSubRegistry::new();
        let ch = Bytes::from_static(b"ch");
        for id in 1..=100 {
            reg.subscribe(ch.clone(), live_sub(id));
        }
        let snapshot = Arc::clone(&reg.channels[&ch]);
        let before = subscriber::clones_on_this_thread();
        for id in 101..=150 {
            reg.subscribe(ch.clone(), live_sub(id));
        }
        reg.unsubscribe(&ch, 7);
        assert_eq!(
            subscriber::clones_on_this_thread() - before,
            100,
            "one copy of the 100 handles the snapshot shares, then in place"
        );
        assert_eq!(snapshot.len(), 100, "the snapshot changed under its reader");
        assert!(snapshot.iter().map(|s| s.id).eq(1..=100));
        assert_eq!(reg.channels[&ch].len(), 149);
        drop(snapshot);
        let before = subscriber::clones_on_this_thread();
        reg.subscribe(ch.clone(), live_sub(151));
        reg.unsubscribe(&ch, 8);
        assert_eq!(subscriber::clones_on_this_thread(), before);
        // Delivery order is subscription order, minus the departed.
        let order: Vec<u64> = reg.channels[&ch].iter().map(|s| s.id).collect();
        let want: Vec<u64> = (1..=151).filter(|id| *id != 7 && *id != 8).collect();
        assert_eq!(order, want);
        assert_no_missing_reverse_entries(&reg);
    }

    /// Cost of ONE subscriber disconnecting, as the registry's channel count
    /// grows. `#[ignore]`d: a measurement, not an assertion.
    ///
    /// `cargo test --release --lib bench_unsubscribe_all_cost_vs_channels -- --ignored --nocapture`
    ///
    /// The disconnecting subscribers are subscribed to NOTHING, so every
    /// microsecond is the sweep over other connections' channels.
    #[test]
    #[ignore = "measurement harness; run explicitly with --nocapture"]
    fn bench_unsubscribe_all_cost_vs_channels() {
        use std::time::Instant;
        println!("{:<10} {:<16} total", "channels", "µs/disconnect");
        for chans in [1000_usize, 2000, 4000, 8000, 16000] {
            let mut reg = PubSubRegistry::new();
            let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
            std::mem::forget(rx);
            for c in 0..chans {
                reg.subscribe(
                    Bytes::from(format!("ch:{c}")),
                    Subscriber::new(tx.clone(), 1),
                );
            }
            const CHURN: usize = 100;
            let t = Instant::now();
            for c in 0..CHURN {
                reg.unsubscribe_all(1000 + c as u64);
            }
            let el = t.elapsed();
            println!(
                "{:<10} {:<16.3} {:?}",
                chans,
                el.as_secs_f64() * 1e6 / CHURN as f64,
                el
            );
            assert_eq!(reg.channels.len(), chans, "sweep dropped live channels");
        }
    }

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
