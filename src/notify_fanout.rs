//! Draining the keyspace-notification outbox onto the pub/sub mesh.
//!
//! Split from [`crate::notify`] because that module is pure logic — flags,
//! parsing, the thread-local queue — while this one needs the shard's SPSC
//! producers, its notifiers and the remote-subscriber map. Keeping them apart
//! means the flag model stays unit-testable without a running shard.
//!
//! There is exactly one reason this indirection exists at all: a subscriber's
//! connection task lives on the shard thread that accepted it, and under
//! monoio a `Waker` fired from another OS thread does not reach it. Publishing
//! straight into a remote shard's registry would therefore queue the message
//! and never wake the reader. The SPSC ring plus its notifier is the only
//! cross-thread wake that works, so remote deliveries go through it.

use bytes::Bytes;

use crate::notify::{self, NotifyFlags};

/// Drain this thread's outbox and deliver every queued event.
///
/// Local subscribers are served synchronously; every other shard that has a
/// subscriber matching the channel gets one `NotifyPublish` message. Returns
/// the shard ids that were notified, so callers that must kick a notifier can
/// do so without re-deriving the set.
///
/// Fire-and-forget by design: a notification has no return value, so this
/// never awaits and can be called from the shard timer as well as from a
/// connection handler. A full ring drops the batch rather than blocking the
/// write path — the same trade Redis makes for a slow subscriber, and the
/// reason `notify-keyspace-events` is documented as best-effort.
pub fn flush_outbox<P>(
    shard_id: usize,
    local_registry: &parking_lot::RwLock<crate::pubsub::PubSubRegistry>,
    remote_map: &parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    push_to: P,
) where
    P: FnMut(usize, Vec<(Bytes, Bytes)>),
{
    let Some(pending) = notify::take_outbox() else {
        return;
    };
    let flags = notify::published_flags();
    if !flags.is_enabled() {
        // Flags were turned off between queueing and draining. Dropping is
        // right: the operator's most recent instruction is "do not deliver".
        return;
    }

    publish_fanout(
        shard_id,
        local_registry,
        remote_map,
        pending.iter().flat_map(|n| notify::channels_for(n, flags)),
        push_to,
    );
}

/// Publish `pairs` to local subscribers AND to every other shard holding a
/// subscriber for the channel.
///
/// This is the whole cross-shard publish rule in one place: a subscriber's
/// connection lives on the shard that accepted it, which has nothing to do
/// with the shard the publisher runs on, so publishing into the local registry
/// alone reaches only the subscribers that happened to land there. At
/// `--shards N` that silently loses about `(N-1)/N` of deliveries and is
/// perfectly correct at `--shards 1`, which is why it survives review.
///
/// Both callers that produce events off a connection use it: keyspace
/// notifications via [`flush_outbox`], and MQ triggers via
/// [`publish_from_shard`] (moon#474 — the trigger timer published locally
/// only, so a queue consumer was woken only when it happened to connect to the
/// queue's home shard).
///
/// Deliveries are grouped by target shard, so a burst costs one message per
/// shard rather than one per event.
pub fn publish_fanout<P>(
    shard_id: usize,
    local_registry: &parking_lot::RwLock<crate::pubsub::PubSubRegistry>,
    remote_map: &parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    pairs: impl IntoIterator<Item = (Bytes, Bytes)>,
    mut push_to: P,
) where
    P: FnMut(usize, Vec<(Bytes, Bytes)>),
{
    let mut remote: Vec<(usize, Vec<(Bytes, Bytes)>)> = Vec::new();
    for (channel, payload) in pairs {
        crate::pubsub::publish_shared(local_registry, &channel, &payload);
        let targets = remote_map.read().target_shards(&channel);
        for t in targets {
            if t == shard_id {
                continue;
            }
            match remote.iter_mut().find(|(id, _)| *id == t) {
                Some((_, batch)) => batch.push((channel.clone(), payload.clone())),
                None => remote.push((t, vec![(channel.clone(), payload.clone())])),
            }
        }
    }

    for (target, pairs) in remote {
        push_to(target, pairs);
    }
}

/// Drain and deliver from a connection handler.
///
/// Wraps [`flush_outbox`] with the mesh plumbing every sharded handler holds.
/// Cheap to call unconditionally after a command batch: with nothing queued it
/// is one thread-local borrow.
pub(crate) fn flush_from_connection(ctx: &crate::server::conn::core::ConnectionContext) {
    use ringbuf::traits::Producer;
    flush_outbox(
        ctx.shard_id,
        &ctx.pubsub_registry,
        &ctx.remote_subscriber_map,
        |target, pairs| {
            let msg = crate::shard::dispatch::ShardMessage::NotifyPublish(Box::new(pairs));
            let idx = crate::shard::mesh::ChannelMesh::target_index(ctx.shard_id, target);
            let pushed = {
                let mut producers = ctx.dispatch_tx.borrow_mut();
                producers[idx].try_push(msg).is_ok()
            };
            if pushed {
                ctx.spsc_notifiers[target].notify_one();
            }
            // A full ring drops this batch. Deliberate: notifications are
            // best-effort in Redis too, and blocking a write path on a
            // notification would be a worse failure than losing one.
        },
    );
}

/// Drain and deliver from the shard event loop.
///
/// The counterpart to [`flush_from_connection`], and the one that makes
/// cross-shard writes work: a write routed to the shard that owns the key
/// executes on THAT thread, so its events land in THAT thread's outbox. With
/// only the connection-side drain, every event from a cross-shard write would
/// be queued and never delivered — invisible at `--shards 1` and losing
/// roughly (N-1)/N of events at `--shards N`.
///
/// Also the delivery path for events with no connection at all: TTL expiry and
/// eviction both run from the shard timer.
pub fn flush_from_shard(
    shard_id: usize,
    local_registry: &parking_lot::RwLock<crate::pubsub::PubSubRegistry>,
    remote_map: &parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    dispatch_tx: &std::cell::RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
    notifiers: &[std::sync::Arc<crate::runtime::channel::Notify>],
) {
    use ringbuf::traits::Producer;
    flush_outbox(shard_id, local_registry, remote_map, |target, pairs| {
        let msg = crate::shard::dispatch::ShardMessage::NotifyPublish(Box::new(pairs));
        let idx = crate::shard::mesh::ChannelMesh::target_index(shard_id, target);
        let pushed = {
            let mut producers = dispatch_tx.borrow_mut();
            producers[idx].try_push(msg).is_ok()
        };
        if pushed {
            notifiers[target].notify_one();
        }
    });
}

/// Fan out arbitrary `(channel, payload)` pairs from a shard event loop.
///
/// The counterpart to [`flush_from_shard`] for producers that are not keyspace
/// notifications — today the MQ trigger timer. It carries the same mesh
/// plumbing (SPSC ring + notifier), because that is the only cross-thread wake
/// that actually reaches a subscriber's connection task: under monoio a
/// `Waker` fired from another OS thread does not.
///
/// Remote deliveries ride `ShardMessage::NotifyPublish`, whose receiver simply
/// publishes each pair into its own registry. Reusing it is deliberate — a
/// second fan-out mechanism is a second thing to forget to call.
pub fn publish_from_shard(
    shard_id: usize,
    local_registry: &parking_lot::RwLock<crate::pubsub::PubSubRegistry>,
    remote_map: &parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    dispatch_tx: &std::cell::RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
    notifiers: &[std::sync::Arc<crate::runtime::channel::Notify>],
    pairs: impl IntoIterator<Item = (Bytes, Bytes)>,
) {
    use ringbuf::traits::Producer;
    publish_fanout(
        shard_id,
        local_registry,
        remote_map,
        pairs,
        |target, batch| {
            let msg = crate::shard::dispatch::ShardMessage::NotifyPublish(Box::new(batch));
            let idx = crate::shard::mesh::ChannelMesh::target_index(shard_id, target);
            let pushed = {
                let mut producers = dispatch_tx.borrow_mut();
                producers[idx].try_push(msg).is_ok()
            };
            if pushed {
                notifiers[target].notify_one();
            }
        },
    );
}

/// Class of the event a command produced, for call sites that need to name it
/// once and emit several events.
pub const GENERIC: NotifyFlags = NotifyFlags::GENERIC;
/// String-command class (`$`).
pub const STRING: NotifyFlags = NotifyFlags::STRING;
/// Expired-key class (`x`).
pub const EXPIRED: NotifyFlags = NotifyFlags::EXPIRED;
/// Evicted-key class (`e`).
pub const EVICTED: NotifyFlags = NotifyFlags::EVICTED;
/// Key-miss class (`m`).
pub const KEY_MISS: NotifyFlags = NotifyFlags::KEY_MISS;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::{PendingNotification, parse_flags};

    #[test]
    fn keyspace_and_keyevent_channels_are_inverted() {
        // The pair consumers most often get backwards: the keyspace channel is
        // named for the KEY and carries the EVENT; the keyevent channel is
        // named for the EVENT and carries the KEY.
        let n = PendingNotification {
            db: 0,
            event: "set",
            key: Bytes::from_static(b"mykey"),
        };
        let pairs = notify::channels_for(&n, parse_flags("KEA").expect("valid"));
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, Bytes::from_static(b"__keyspace@0__:mykey"));
        assert_eq!(pairs[0].1, Bytes::from_static(b"set"));
        assert_eq!(pairs[1].0, Bytes::from_static(b"__keyevent@0__:set"));
        assert_eq!(pairs[1].1, Bytes::from_static(b"mykey"));
    }

    #[test]
    fn k_alone_emits_only_the_keyspace_channel() {
        let n = PendingNotification {
            db: 3,
            event: "del",
            key: Bytes::from_static(b"k"),
        };
        let pairs = notify::channels_for(&n, parse_flags("Kg").expect("valid"));
        assert_eq!(pairs.len(), 1, "E is unset, so no keyevent channel");
        assert_eq!(pairs[0].0, Bytes::from_static(b"__keyspace@3__:k"));
    }

    #[test]
    fn e_alone_emits_only_the_keyevent_channel() {
        let n = PendingNotification {
            db: 3,
            event: "del",
            key: Bytes::from_static(b"k"),
        };
        let pairs = notify::channels_for(&n, parse_flags("Eg").expect("valid"));
        assert_eq!(pairs.len(), 1, "K is unset, so no keyspace channel");
        assert_eq!(pairs[0].0, Bytes::from_static(b"__keyevent@3__:del"));
    }

    #[test]
    fn db_index_is_part_of_both_channel_names() {
        // A consumer subscribed to __keyspace@0__:* must not see db-9 traffic.
        let n = PendingNotification {
            db: 9,
            event: "set",
            key: Bytes::from_static(b"k"),
        };
        let pairs = notify::channels_for(&n, parse_flags("KEA").expect("valid"));
        assert!(pairs.iter().all(|(ch, _)| ch.starts_with(b"__key")));
        assert!(
            pairs
                .iter()
                .any(|(ch, _)| ch.as_ref() == b"__keyspace@9__:k")
        );
        assert!(
            pairs
                .iter()
                .any(|(ch, _)| ch.as_ref() == b"__keyevent@9__:set")
        );
    }
}
