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

use crate::storage::owned_bytes::detach;
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

/// A keyspace-relevant exact channel / pattern lost its last subscriber
/// (moon#1214 item 2). Called when a channel/pattern entry is REMOVED. Paired
/// 1:1 with the increment `subscribe` / `psubscribe` make when they CREATE an
/// entry (after inserting it, moon#1226) via the map's present↔absent
/// transitions, so the global count never leaks and (saturating) never goes
/// negative.
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
    ///
    /// moon#1160: the name is stored as an exact-size copy. The caller hands
    /// in a slice of the connection's request buffer, and keeping that slice
    /// in a registry that lives as long as the subscription pinned the whole
    /// read buffer (the same leak `storage::owned_bytes` closed for stored
    /// collection elements). One copy per SUBSCRIBE, shared by both maps.
    pub fn subscribe(&mut self, channel: Bytes, sub: Subscriber) {
        let channel = detach(&channel);
        self.sub_channels
            .entry(sub.id)
            .or_default()
            .insert(channel.clone());
        match self.channels.get_mut(&channel) {
            Some(subs) => list_push(subs, sub),
            None => {
                // First subscriber for this channel: a present transition.
                // Counted AFTER the entry exists — the count's Release pairs
                // with the notify gate's Acquire (moon#1226, `crate::notify`).
                let keyspace = crate::notify::subscription_targets_keyspace(&channel);
                self.channels.insert(channel, Arc::new(vec![sub]));
                if keyspace {
                    crate::notify::keyspace_listener_added();
                }
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
        // moon#1160: a NEW pattern entry stores an exact-size copy, never a
        // slice of the request buffer (see `subscribe`).
        let pattern = detach(&pattern);
        // New pattern entry: a present transition, counted after the entry
        // exists (moon#1226, see `subscribe`).
        let keyspace = crate::notify::subscription_targets_keyspace(&pattern);
        self.patterns.push((pattern, Arc::new(vec![sub])));
        if keyspace {
            crate::notify::keyspace_listener_added();
        }
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

    /// Subscribe to a sharded channel. The name is stored as an exact-size
    /// copy (moon#1160, see `subscribe`).
    pub fn ssubscribe(&mut self, channel: Bytes, sub: Subscriber) {
        let channel = detach(&channel);
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

#[cfg(test)]
mod tests;
