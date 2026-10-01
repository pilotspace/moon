//! R2 review item 5: a volatile policy with a volatile key present must find
//! it. The sampler draws random segments and stopped after `8 x samples`
//! draws: with one TTL key in ~50 segments it missed about half the time and
//! the write was refused OOM. (Found as the tokio lib-suite flake in
//! `snapshot::eviction_capture_tests`, whose fixture has one TTL key in 8
//! segments: 14 misses in 3,000 single-threaded runs, (7/8)^40 = 0.48 %.)

use bytes::Bytes;

use super::*;
use crate::admin::footprint::pin_footprint_correction_for_test;
use crate::config::RuntimeConfig;

#[test]
fn a_lone_volatile_key_is_always_found() {
    // The budget has no slack: a published footprint correction from a
    // parallel test must not shrink it.
    let _pin = pin_footprint_correction_for_test(1.0);
    for policy in ["volatile-lru", "volatile-lfu", "volatile-random"] {
        for round in 0..50 {
            let mut db = Database::new();
            for i in 0..2_000 {
                db.set_string(
                    format!("plain:{i:05}").as_bytes(),
                    Bytes::from_static(b"plain-value"),
                );
            }
            db.set_string(b"vol", Bytes::from_static(b"volatile-value"));
            let now = db.now_ms();
            db.set_expiry(b"vol", now + 3_600_000);
            assert!(db.data().segment_count() >= 16, "fixture: many segments");
            let config = RuntimeConfig {
                maxmemory: db.estimated_memory() - 1,
                maxmemory_policy: policy.to_string(),
                appendonly: "no".to_string(),
                num_shards: 1,
                ..RuntimeConfig::default()
            };
            let outcome = evict_to_budget(&mut db, &config, EvictionRun::plain());
            assert!(
                outcome.is_ok(),
                "{policy} round {round}: OOM with a volatile key present: {outcome:?}"
            );
            assert!(db.data().get(b"vol").is_none(), "{policy}: the victim");
            assert_eq!(db.data().len(), 2_000, "{policy}: only the TTL key goes");
        }
    }
}
