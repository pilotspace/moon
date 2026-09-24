//! WS1 (2026-09 performance review) regression tests for the storage core.
//!
//! Kept out of `db/mod.rs` (already far past the 1500-line guideline) so each
//! issue's red/green pins live together:
//! - moon#1159 — `Database::set` builds no owned key on an overwrite.

use bytes::Bytes;

use crate::storage::dashtable::take_upsert_key_builds;
use crate::storage::db::Database;
use crate::storage::entry::Entry;

// ── moon#1159 ────────────────────────────────────────────────────────────

/// `Database::set` on an EXISTING key must not construct an owned key: the
/// DashTable upsert is keyed by the borrowed slice and builds a `CompactKey`
/// only on a miss. Before moon#1159's follow-up every SET overwrite of a key
/// longer than 23 bytes allocated a heap key block and dropped it again.
#[test]
fn set_overwrite_builds_no_owned_key() {
    let mut db = Database::new();
    let key: &[u8] = b"a-long-key-that-does-not-fit-inline:0001";
    let _ = take_upsert_key_builds();
    db.set(key, Entry::new_string(Bytes::from_static(b"v1")));
    assert_eq!(
        take_upsert_key_builds(),
        1,
        "a new key stores one owned key"
    );
    for _ in 0..50 {
        db.set(key, Entry::new_string(Bytes::from_static(b"v2")));
    }
    assert_eq!(
        take_upsert_key_builds(),
        0,
        "SET overwrite built an owned key per call"
    );
    assert_eq!(
        db.get(key).and_then(|e| e.value.as_bytes_owned()),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(db.len(), 1);
}

// ── moon#1161 ────────────────────────────────────────────────────────────

mod access_tracking_1161 {
    use bytes::Bytes;

    use crate::command::key;
    use crate::protocol::Frame;
    use crate::storage::db::Database;
    use crate::storage::entry::{AccessTracking, ClockPin, Entry};
    use crate::storage::eviction::force_access_tracking;

    const T0: u32 = 1_800_000_000;

    fn bs(b: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(b))
    }

    /// A db whose shard clock (and the thread-local clock the `Entry`
    /// constructors read) sits at `secs`.
    fn at(db: &mut Database, secs: u32) -> ClockPin {
        let pin = ClockPin::set(secs, u64::from(secs) * 1000);
        db.refresh_now();
        pin
    }

    fn lfu() -> AccessTracking {
        AccessTracking::Lfu {
            log_factor: 10,
            decay_time: 1,
        }
    }

    // ---- Entry: the `&self` recorder ----------------------------------

    #[test]
    fn lru_stamp_moves_last_access_through_a_shared_reference() {
        let _pin = ClockPin::set(T0, u64::from(T0) * 1000);
        let e = Entry::new_string(Bytes::from_static(b"v"));
        assert_eq!(e.last_access(), T0);
        let shared: &Entry = &e;
        shared.note_access(AccessTracking::Lru, T0 + 7);
        assert_eq!(e.last_access(), T0 + 7);
        shared.note_access(AccessTracking::Off, T0 + 99);
        assert_eq!(e.last_access(), T0 + 7, "Off must record nothing");
    }

    #[test]
    fn lfu_access_grows_the_counter_and_never_disturbs_the_watch_version() {
        let mut e = Entry::new_string(Bytes::from_static(b"v"));
        e.set_version(0xAB_CDEF);
        assert_eq!(e.access_counter(), 5);
        for _ in 0..1000 {
            e.note_access(lfu(), T0);
        }
        // redis's documented table: factor 10 reaches ~18 after 1K hits.
        let c = e.access_counter();
        assert!((10..=30).contains(&c), "counter after 1000 LFU hits = {c}");
        assert_eq!(e.version(), 0xAB_CDEF, "LFU recorder clobbered the version");
        assert_eq!(e.last_access(), T0);
        // A clone carries the metadata.
        let c2 = e.clone();
        assert_eq!(c2.version(), 0xAB_CDEF);
        assert_eq!(c2.access_counter(), c);
    }

    #[test]
    fn lfu_counter_decays_by_idle_minutes_before_the_increment() {
        let mut e = Entry::new_string(Bytes::from_static(b"v"));
        e.set_access_counter(100);
        e.set_last_access(T0);
        // 30 idle minutes at decay_time 1 => 70, then one increment attempt.
        e.note_access(lfu(), T0 + 30 * 60);
        let c = e.access_counter();
        assert!(c == 70 || c == 71, "decayed counter = {c}");
        assert_eq!(e.lfu_frequency_at(1, T0 + 30 * 60 + 600), c - 10);
    }

    /// The publish side: tracking follows the POLICY (not `maxmemory`), and
    /// the LFU parameters ride in the same word.
    #[test]
    fn published_tracking_follows_the_policy_and_lfu_params() {
        use crate::storage::eviction::{
            PublishedLimits, publish_lfu_params, publish_maxmemory_policy,
            published_access_tracking,
        };
        let _restore = PublishedLimits::capture();
        publish_lfu_params(7, 3);
        publish_maxmemory_policy("allkeys-lru");
        assert_eq!(published_access_tracking(), AccessTracking::Lru);
        publish_maxmemory_policy("volatile-lfu");
        assert_eq!(
            published_access_tracking(),
            AccessTracking::Lfu {
                log_factor: 7,
                decay_time: 3
            }
        );
        publish_lfu_params(10, 1);
        assert_eq!(published_access_tracking(), lfu());
        for off in [
            "noeviction",
            "allkeys-random",
            "volatile-random",
            "volatile-ttl",
        ] {
            publish_maxmemory_policy(off);
            assert_eq!(published_access_tracking(), AccessTracking::Off, "{off}");
        }
        publish_maxmemory_policy("ALLKEYS-LRU");
        assert_eq!(published_access_tracking(), AccessTracking::Lru);
    }

    // ---- Database accessors ---------------------------------------------

    fn last_access(db: &Database, key: &[u8]) -> u32 {
        db.data().get(key).map(|e| e.last_access()).expect("key")
    }

    #[test]
    fn every_read_accessor_records_under_lru_and_no_peek_does() {
        let _lru = force_access_tracking(AccessTracking::Lru);
        let mut db = Database::new();
        let _p0 = at(&mut db, T0);
        db.set(b"s", Entry::new_string(Bytes::from_static(b"v")));
        let mut t = T0;
        let mut step = |db: &mut Database| {
            t += 10;
            let pin = at(db, t);
            (pin, t)
        };

        let (_p, now) = step(&mut db);
        let _ = db.peek(b"s");
        let _ = db.peek_if_alive(b"s", db.now_ms());
        let _ = db.peek_if_alive_any_plane(b"s", db.now_ms());
        assert_eq!(last_access(&db, b"s"), T0, "a peek recorded an access");

        let _ = db.get(b"s");
        assert_eq!(last_access(&db, b"s"), now, "Database::get");

        let (_p, now) = step(&mut db);
        let ms = db.now_ms();
        let _ = db.get_if_alive(b"s", ms);
        assert_eq!(
            last_access(&db, b"s"),
            now,
            "get_if_alive (inline GET path)"
        );

        let (_p, now) = step(&mut db);
        let ms = db.now_ms();
        let _ = db.get_if_alive_any_plane(b"s", ms);
        assert_eq!(last_access(&db, b"s"), now, "get_if_alive_any_plane");
        let s_stamp = now;

        // Typed read (HGET/LRANGE/... path) and a write to an existing key.
        let (_p, _) = step(&mut db);
        db.get_or_create_hash(b"h")
            .expect("hash")
            .insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let (_p, now) = step(&mut db);
        let ms = db.now_ms();
        assert!(db.get_hash_ref_if_alive(b"h", ms).expect("type").is_some());
        assert_eq!(last_access(&db, b"h"), now, "typed read");
        let (_p, now) = step(&mut db);
        let _ = db.get_or_create_hash(b"h").expect("hash");
        assert_eq!(last_access(&db, b"h"), now, "write to an existing key");

        // EXISTS is NOTOUCH in redis.
        let (_p, _) = step(&mut db);
        assert!(db.exists(b"s"));
        assert!(db.exists_if_alive(b"s", db.now_ms()));
        assert_eq!(last_access(&db, b"s"), s_stamp, "EXISTS recorded an access");
    }

    #[test]
    fn reads_record_nothing_when_the_policy_tracks_nothing() {
        let _off = force_access_tracking(AccessTracking::Off);
        let mut db = Database::new();
        let _p0 = at(&mut db, T0);
        db.set(b"k", Entry::new_string(Bytes::from_static(b"v")));
        let _p1 = at(&mut db, T0 + 50);
        let ms = db.now_ms();
        let _ = db.get(b"k");
        let _ = db.get_if_alive(b"k", ms);
        let _ = db.get_if_alive_any_plane(b"k", ms);
        let e = db.data().get(b"k").expect("key");
        assert_eq!(e.last_access(), T0, "noeviction must keep reads free");
        assert_eq!(e.access_counter(), 5);
    }

    #[test]
    fn lfu_reads_grow_the_frequency_and_an_overwrite_keeps_it() {
        let _lfu = force_access_tracking(lfu());
        let mut db = Database::new();
        let _p = at(&mut db, T0);
        db.set(b"k", Entry::new_string(Bytes::from_static(b"v")));
        for _ in 0..200 {
            let _ = db.get(b"k");
        }
        let after_reads = db.data().get(b"k").expect("k").access_counter();
        // redis 7.0.15, same sequence: 10 (factor 10 is logarithmic).
        assert!(
            (8..=16).contains(&after_reads),
            "OBJECT FREQ after 200 GETs = {after_reads} (redis: ~10; HEAD: 5)"
        );
        db.set(b"k", Entry::new_string(Bytes::from_static(b"v2")));
        let after_set = db.data().get(b"k").expect("k").access_counter();
        assert!(
            after_set >= after_reads,
            "SET reset the LFU counter {after_reads} -> {after_set} (redis keeps it)"
        );
    }

    // ---- Commands: OBJECT / TOUCH / NOTOUCH metadata ---------------------

    fn object(db: &mut Database, sub: &[u8], k: &[u8]) -> Frame {
        key::object(db, &[bs(sub), bs(k)])
    }

    #[test]
    fn object_idletime_resets_on_read_under_lru() {
        let _lru = force_access_tracking(AccessTracking::Lru);
        let mut db = Database::new();
        let _p = at(&mut db, T0);
        db.set(b"k", Entry::new_string(Bytes::from_static(b"v")));
        let _p = at(&mut db, T0 + 3);
        assert_eq!(object(&mut db, b"IDLETIME", b"k"), Frame::Integer(3));
        // OBJECT itself is NOTOUCH: asking again does not reset it.
        assert_eq!(object(&mut db, b"IDLETIME", b"k"), Frame::Integer(3));
        // Neither do TTL / TYPE / KEYS / SCAN.
        let _ = key::ttl(&mut db, &[bs(b"k")]);
        let _ = key::type_cmd(&mut db, &[bs(b"k")]);
        let _ = key::keys(&mut db, &[bs(b"*")]);
        let _ = key::scan(&mut db, &[bs(b"0")]);
        let ms = db.now_ms();
        let _ = key::ttl_readonly(&db, &[bs(b"k")], ms);
        let _ = key::type_cmd_readonly(&db, &[bs(b"k")], ms);
        let _ = key::keys_readonly(&db, &[bs(b"*")], ms);
        let _ = key::object_readonly(&db, &[bs(b"FREQ"), bs(b"k")], ms);
        assert_eq!(object(&mut db, b"IDLETIME", b"k"), Frame::Integer(3));
        // A GET does (redis: `SET k v; sleep 3; GET k` -> IDLETIME 0).
        let _ = db.get(b"k");
        assert_eq!(object(&mut db, b"IDLETIME", b"k"), Frame::Integer(0));
    }

    #[test]
    fn touch_records_even_when_the_policy_tracks_nothing() {
        let _off = force_access_tracking(AccessTracking::Off);
        let mut db = Database::new();
        let _p = at(&mut db, T0);
        db.set(b"a", Entry::new_string(Bytes::from_static(b"v")));
        db.set(b"b", Entry::new_string(Bytes::from_static(b"v")));
        let _p = at(&mut db, T0 + 9);
        assert_eq!(object(&mut db, b"IDLETIME", b"a"), Frame::Integer(9));
        assert_eq!(
            key::touch(&mut db, &[bs(b"a"), bs(b"missing")]),
            Frame::Integer(1)
        );
        assert_eq!(object(&mut db, b"IDLETIME", b"a"), Frame::Integer(0));
        let ms = db.now_ms();
        let _p = at(&mut db, T0 + 20);
        assert_eq!(
            key::touch_readonly(&db, &[bs(b"b"), bs(b"missing")], ms + 11_000),
            Frame::Integer(1)
        );
        assert_eq!(object(&mut db, b"IDLETIME", b"b"), Frame::Integer(0));
    }

    #[test]
    fn object_on_a_missing_key_answers_nil_like_redis() {
        let mut db = Database::new();
        for sub in [&b"ENCODING"[..], b"FREQ", b"IDLETIME", b"REFCOUNT"] {
            assert_eq!(
                object(&mut db, sub, b"nokey"),
                Frame::Null,
                "exclusive {sub:?}"
            );
            assert_eq!(
                key::object_readonly(&db, &[bs(sub), bs(b"nokey")], db.now_ms()),
                Frame::Null,
                "read-only {sub:?}"
            );
        }
    }

    #[test]
    fn object_freq_reports_the_decayed_counter_under_lfu() {
        let _lfu = force_access_tracking(lfu());
        let mut db = Database::new();
        let _p = at(&mut db, T0);
        db.set(b"k", Entry::new_string(Bytes::from_static(b"v")));
        for _ in 0..200 {
            let _ = db.get(b"k");
        }
        let Frame::Integer(f) = object(&mut db, b"FREQ", b"k") else {
            panic!("FREQ not an integer");
        };
        assert!(f >= 8, "OBJECT FREQ after 200 GETs = {f} (HEAD answered 5)");
        // 5 idle minutes decay it by 5 without recording an access.
        let _p = at(&mut db, T0 + 300);
        assert_eq!(object(&mut db, b"FREQ", b"k"), Frame::Integer(f - 5));
        assert_eq!(object(&mut db, b"FREQ", b"k"), Frame::Integer(f - 5));
    }
}

/// moon#1161's hit-ratio scenario, deterministic: the review's "write once,
/// read hot" cache (hot keys read between rounds of cold writes) under a
/// memory cap, with the shard clock stepped explicitly so the verdict does
/// not depend on how many rounds fit in one wall-clock second.
///
/// Each round writes cold keys at `2r` (evicting as it goes, like the
/// per-write gate) and then reads every hot key at `2r + 1`. With reads
/// recording accesses, a hot key is always younger than every earlier cold
/// key, so the sampler only picks one when all its candidates are hot or
/// current-round cold. On HEAD `935c555` reads recorded nothing: the hot keys
/// kept their creation stamp, were the OLDEST keys in the database, and went
/// first (review: 0.6% retained, redis 100%).
mod hit_ratio_1161 {
    use bytes::Bytes;

    use crate::config::RuntimeConfig;
    use crate::storage::db::Database;
    use crate::storage::entry::{AccessTracking, ClockPin, Entry};
    use crate::storage::eviction::{EvictionRun, evict_to_budget, force_access_tracking};

    const HOT: usize = 500;
    const ROUNDS: u32 = 40;
    const COLD_PER_ROUND: usize = 200;
    const T0: u32 = 1_800_000_000;

    fn run(policy: &str, tracking: AccessTracking) -> f64 {
        let _t = force_access_tracking(tracking);
        let value = Bytes::from(vec![b'x'; 1024]);
        let mut db = Database::new();
        let pin_at = |db: &mut Database, secs: u32| {
            let pin = ClockPin::set(secs, u64::from(secs) * 1000);
            db.refresh_now();
            pin
        };
        let _p = pin_at(&mut db, T0);
        for i in 0..HOT {
            db.set(
                format!("hot:{i}").as_bytes(),
                Entry::new_string(value.clone()),
            );
        }
        // Room for the hot set plus ~1500 cold keys.
        let per_key = db.estimated_memory() / HOT;
        let cfg = RuntimeConfig {
            maxmemory: per_key * (HOT + 1500),
            maxmemory_policy: policy.to_string(),
            maxmemory_samples: 5,
            ..RuntimeConfig::default()
        };
        let mut cold = 0usize;
        let mut hits = 0usize;
        for r in 1..=ROUNDS {
            let _p = pin_at(&mut db, T0 + 2 * r);
            for _ in 0..COLD_PER_ROUND {
                db.set(
                    format!("cold:{cold}").as_bytes(),
                    Entry::new_string(value.clone()),
                );
                cold += 1;
                evict_to_budget(&mut db, &cfg, EvictionRun::plain()).expect("evictable");
            }
            let _p = pin_at(&mut db, T0 + 2 * r + 1);
            hits = (0..HOT)
                .filter(|i| db.get(format!("hot:{i}").as_bytes()).is_some())
                .count();
        }
        assert!(
            db.len() < HOT + ROUNDS as usize * COLD_PER_ROUND,
            "fixture must actually evict"
        );
        let ratio = hits as f64 / HOT as f64;
        eprintln!(
            "moon#1161 hit-ratio fixture: {policy} {tracking:?} -> {:.1}%",
            ratio * 100.0
        );
        ratio
    }

    /// Fail-closed control: the same fixture with reads recording NOTHING —
    /// exactly HEAD `935c555`'s behaviour — must lose the hot set, or the
    /// two tests below could pass without the fix doing anything.
    #[test]
    fn control_without_read_tracking_the_hot_set_goes_first() {
        let ratio = run("allkeys-lru", AccessTracking::Off);
        assert!(
            ratio <= 0.10,
            "control kept {:.1}% of the hot keys without read tracking — the \
             fixture no longer exercises moon#1161",
            ratio * 100.0
        );
    }

    #[test]
    fn allkeys_lru_keeps_the_keys_that_are_read() {
        let ratio = run("allkeys-lru", AccessTracking::Lru);
        assert!(
            ratio >= 0.90,
            "allkeys-lru kept {:.1}% of the hot keys (redis: 100%, HEAD: ~0%)",
            ratio * 100.0
        );
    }

    #[test]
    fn allkeys_lfu_keeps_the_keys_that_are_read() {
        let ratio = run(
            "allkeys-lfu",
            AccessTracking::Lfu {
                log_factor: 10,
                decay_time: 1,
            },
        );
        assert!(
            ratio >= 0.90,
            "allkeys-lfu kept {:.1}% of the hot keys (HEAD: ~0%)",
            ratio * 100.0
        );
    }
}
