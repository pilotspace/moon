//! `PubSubRegistry` unit tests (moved out of `mod.rs` for the 1,500-line
//! rule, moon#1226).
//!
//! Runtime-neutral, so they run in the default (monoio) lib run too: they
//! were gated on `runtime-tokio` only because a dozen of them were
//! `#[tokio::test]`s awaiting `recv_async`. `publish` delivers with a
//! non-blocking `try_send`, so the message is already in the channel when it
//! returns and `try_recv` reads it without a runtime (moon#1226).

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

#[test]
fn test_subscribe_and_publish() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
    let sub = Subscriber::new(tx, 1);
    let channel = Bytes::from_static(b"news");

    registry.subscribe(channel.clone(), sub);

    let count = registry.publish(&channel, &Bytes::from_static(b"hello"));
    assert_eq!(count, 1);

    let msg = rx.try_recv().expect("publish delivers synchronously");
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

#[test]
fn test_psubscribe_glob() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
    let sub = Subscriber::new(tx, 1);
    let pattern = Bytes::from_static(b"news.*");

    registry.psubscribe(pattern.clone(), sub);

    let channel = Bytes::from_static(b"news.sports");
    let count = registry.publish(&channel, &Bytes::from_static(b"goal!"));
    assert_eq!(count, 1);

    let msg = rx.try_recv().expect("publish delivers synchronously");
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

#[test]
fn test_unsubscribe() {
    let mut registry = PubSubRegistry::new();
    let (tx, _rx) = channel::mpsc_bounded::<Bytes>(16);
    let sub = Subscriber::new(tx, 1);
    let channel = Bytes::from_static(b"news");

    registry.subscribe(channel.clone(), sub);
    registry.unsubscribe(b"news", 1);

    let count = registry.publish(&channel, &Bytes::from_static(b"hello"));
    assert_eq!(count, 0);
}

#[test]
fn test_slow_subscriber_disconnected() {
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

#[test]
fn test_publish_returns_count() {
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

#[test]
fn test_publish_shared_delivers_and_counts() {
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

    let msg = rx1.try_recv().expect("publish delivers synchronously");
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

#[test]
fn test_publish_shared_pattern_delivery() {
    let lock = parking_lot::RwLock::new(PubSubRegistry::new());
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
    lock.write()
        .psubscribe(Bytes::from_static(b"news.*"), Subscriber::new(tx, 1));

    let channel = Bytes::from_static(b"news.sports");
    let count = publish_shared(&lock, &channel, &Bytes::from_static(b"goal!"));
    assert_eq!(count, 1);

    let msg = rx.try_recv().expect("publish delivers synchronously");
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

#[test]
fn test_publish_shared_removes_slow_subscriber() {
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

#[test]
fn test_sharded_namespace_is_isolated_from_plain() {
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

    let got_shard = rx_shard.try_recv().expect("publish delivers synchronously");
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

    let got_plain = rx_plain.try_recv().expect("publish delivers synchronously");
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

#[test]
fn test_spublish_shared_removes_slow_subscriber() {
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

#[test]
fn test_sunsubscribe_all_returns_channels_for_unpropagation() {
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

#[test]
fn test_publish_shared_removes_slow_pattern_subscriber() {
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
