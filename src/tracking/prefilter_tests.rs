//! moon#1166: the pre-filter's counters must track a global-flagged table
//! EXACTLY through every mutation path — a count that stays high only costs a
//! lock, but one that drops early would let a write to a tracked key skip its
//! invalidation. Each test uses keys no other test names and asserts DELTAS,
//! so tests running in parallel on the shared counters cannot disturb it.

use super::*;
use bytes::Bytes;

fn tx() -> crate::runtime::channel::MpscSender<Frame> {
    let (tx, rx) = crate::runtime::channel::mpsc_unbounded::<Frame>();
    std::mem::forget(rx);
    tx
}

#[test]
fn key_counts_follow_track_invalidate_untrack_and_flush() {
    let mut t = TrackingTable::new_global();
    let k1 = Bytes::from_static(b"ws7:pf:k1");
    let k2 = Bytes::from_static(b"ws7:pf:k2");
    let (b1, b2) = (prefilter::bucket_count(&k1), prefilter::bucket_count(&k2));
    t.register_client(900_001, tx());
    t.register_client(900_002, tx());

    let _ = t.track_key(900_001, &k1, false);
    let _ = t.track_key(900_002, &k1, false); // same key: no new entry
    assert_eq!(prefilter::bucket_count(&k1), b1 + 1);
    assert!(
        global_may_track(&k1),
        "a tracked key must read as maybe-tracked"
    );

    let _ = t.invalidate_key(&k1, 0);
    assert_eq!(
        prefilter::bucket_count(&k1),
        b1,
        "invalidation consumed the entry"
    );

    let _ = t.track_key(900_001, &k2, false);
    assert_eq!(prefilter::bucket_count(&k2), b2 + 1);
    t.untrack_all(900_001);
    assert_eq!(
        prefilter::bucket_count(&k2),
        b2,
        "untrack_all drops the last tracker"
    );

    let _ = t.track_key(900_002, &k1, false);
    let _ = t.track_key(900_002, &k2, false);
    let _ = t.invalidate_all();
    assert_eq!(prefilter::bucket_count(&k1), b1, "flush drops every key");
    assert_eq!(prefilter::bucket_count(&k2), b2);
    t.untrack_all(900_002);
}

#[test]
fn cap_eviction_keeps_the_counts_exact() {
    let mut t = TrackingTable::new_global();
    t.max_keys = 1;
    let a = Bytes::from_static(b"ws7:pf:evict:a");
    let b = Bytes::from_static(b"ws7:pf:evict:b");
    let (ba, bb) = (prefilter::bucket_count(&a), prefilter::bucket_count(&b));
    t.register_client(900_010, tx());
    let _ = t.track_key(900_010, &a, false);
    let evicted = t.track_key(900_010, &b, false);
    assert!(evicted.is_some(), "precondition: the cap evicted `a`");
    assert_eq!(
        prefilter::bucket_count(&a),
        ba,
        "evicted key left the filter"
    );
    assert_eq!(prefilter::bucket_count(&b), bb + 1);
    t.untrack_all(900_010);
    assert_eq!(prefilter::bucket_count(&b), bb);
}

#[test]
fn bcast_prefixes_are_counted_and_released() {
    let mut t = TrackingTable::new_global();
    t.register_client(900_020, tx());
    let before = prefilter::prefix_count();
    t.register_prefix(900_020, Bytes::from_static(b"ws7pf:a:"), false);
    t.register_prefix(900_020, Bytes::from_static(b"ws7pf:a:"), true); // duplicate
    t.register_prefix(900_020, Bytes::from_static(b"ws7pf:b:"), false);
    assert!(prefilter::prefix_count() >= before + 2);
    assert!(
        global_may_track(b"anything"),
        "with a BCAST prefix registered every write must take the lock"
    );
    let registered = prefilter::prefix_count();
    t.untrack_all(900_020);
    assert_eq!(
        registered - prefilter::prefix_count(),
        2,
        "untrack_all must release exactly the client's two prefixes"
    );
}

#[test]
fn private_tables_do_not_touch_the_filter() {
    let mut t = TrackingTable::new();
    let k = Bytes::from_static(b"ws7:pf:private");
    let b = prefilter::bucket_count(&k);
    t.register_client(900_030, tx());
    let _ = t.track_key(900_030, &k, false);
    assert_eq!(
        prefilter::bucket_count(&k),
        b,
        "a test table must not feed the global filter"
    );
    t.untrack_all(900_030);
}
