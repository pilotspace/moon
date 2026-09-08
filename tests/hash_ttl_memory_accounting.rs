//! moon#861 — the `HashWithTtl` promotion/downgrade ledger must balance.
//!
//! `RedisValue::HashWithTtl` boxes BOTH of its maps, so promoting a hash on
//! the first `HEXPIRE` allocates a second payload block (`ttls`) that
//! `estimate_memory` bills. `promote_to_hash_with_ttl` performed that
//! allocation with no `charge_memory`, while `remove_hot` credits
//! `entry_overhead` recomputed from the value as it stands at DEL time —
//! which DOES include the box.
//!
//! Net: every `HSET h f v; HEXPIRE h ...; DEL h` cycle credited more than it
//! ever charged, so `used_memory` walked down without bound. `credit_memory`
//! saturates at 0, and once there `--maxmemory` can never bind again.
//! `recalculate_memory` is a load-time healer only, so nothing repairs it at
//! runtime. Same class as moon#814 / moon#788 / moon#810.
//!
//! The oracle here is `recalculate_memory()`: a full rescan of the keyspace
//! by the same `entry_overhead` the ledger claims to track incrementally. Any
//! uncharged allocation or over-credit shows up as a mismatch, which is
//! strictly stronger than eyeballing individual deltas.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;

use moon::storage::db::{Database, HashTtlCond};
use moon::storage::entry::{Entry, RedisValue};

/// Insert a plain `RedisValue::Hash` key through `Database::set`, which is the
/// charging path every hash-creating command ultimately lands on.
fn make_hash_key(db: &mut Database, key: &[u8], fields: &[(&[u8], &[u8])]) {
    let mut entry = Entry::new_hash();
    if let Some(RedisValue::Hash(map)) = entry.value.as_redis_value_mut() {
        for (f, v) in fields {
            map.insert(Bytes::copy_from_slice(f), Bytes::copy_from_slice(v));
        }
    }
    db.set(key, entry);
}

/// Insert a `RedisValue::HashListpack` key — the encoding a small hash
/// actually gets in production, and the arm `promote_to_hash_with_ttl`
/// converts into a full `HashMap` plus two boxes.
fn make_hash_listpack_key(db: &mut Database, key: &[u8], fields: &[(&[u8], &[u8])]) {
    let mut entry = Entry::new_hash_listpack();
    if let Some(RedisValue::HashListpack(lp)) = entry.value.as_redis_value_mut() {
        for (f, v) in fields {
            lp.push_back(f);
            lp.push_back(v);
        }
    }
    db.set(key, entry);
}

/// Park enough billed bytes in the db that a per-iteration drift shows up as a
/// difference rather than being swallowed by `credit_memory`'s saturation at 0.
///
/// Without this the whole repro is VACUOUS: on an empty `Database` the ledger
/// is already 0 when the cycle starts, over-crediting saturates back to 0, and
/// the assertion passes against the very bug it exists to catch.
fn add_ballast(db: &mut Database) {
    for i in 0..256u32 {
        let key = format!("ballast:{i:04}");
        db.set(
            key.as_bytes(),
            Entry::new_string(Bytes::from(vec![b'x'; 512])),
        );
    }
}

/// The running ledger must agree with a full rescan at every step.
fn assert_ledger_matches_rescan(db: &mut Database, stage: &str) {
    let running = db.resident_bytes();
    db.recalculate_memory();
    let truth = db.resident_bytes();
    assert_eq!(
        running,
        truth,
        "used_memory drifted at `{stage}`: running ledger {running} B vs full rescan {truth} B \
         (delta {})",
        running as i64 - truth as i64
    );
}

/// The headline repro: HSET / HEXPIRE / DEL in a loop must return the ledger
/// to its starting value every single time.
#[test]
fn hset_hexpire_del_cycle_does_not_drift_used_memory() {
    let mut db = Database::new();
    add_ballast(&mut db);
    let baseline = db.resident_bytes();
    // 64 iterations can drift at most a few hundred bytes each; the ballast
    // must clear that by an order of magnitude or `saturating_sub` hides it.
    assert!(
        baseline > 64 * 1024,
        "ballast must dominate the drift, got {baseline} B"
    );

    for i in 0..64 {
        make_hash_key(&mut db, b"h", &[(b"f", b"v")]);
        let exp = db.now_ms() + 60_000;
        db.hash_set_field_ttl(b"h", b"f", exp, HashTtlCond::Always)
            .unwrap();
        db.remove(b"h");
        assert_eq!(
            db.resident_bytes(),
            baseline,
            "iteration {i}: used_memory must return to {baseline} B after the key is deleted, \
             got {} B — an uncharged promotion allocation credited back at DEL",
            db.resident_bytes()
        );
    }
}

/// Same cycle from the encoding a small hash really has: `HashListpack`.
/// Promotion here materialises a whole `HashMap` plus two boxes.
#[test]
fn listpack_hexpire_del_cycle_does_not_drift_used_memory() {
    let mut db = Database::new();
    add_ballast(&mut db);
    let baseline = db.resident_bytes();
    // 64 iterations can drift at most a few hundred bytes each; the ballast
    // must clear that by an order of magnitude or `saturating_sub` hides it.
    assert!(
        baseline > 64 * 1024,
        "ballast must dominate the drift, got {baseline} B"
    );

    for i in 0..64 {
        make_hash_listpack_key(&mut db, b"lp", &[(b"f", b"v")]);
        let exp = db.now_ms() + 60_000;
        db.hash_set_field_ttl(b"lp", b"f", exp, HashTtlCond::Always)
            .unwrap();
        db.remove(b"lp");
        assert_eq!(
            db.resident_bytes(),
            baseline,
            "iteration {i}: used_memory must return to {baseline} B after the key is deleted, \
             got {} B",
            db.resident_bytes()
        );
    }
}

/// Promotion, then the downgrade back to plain `Hash` when the last TTL is
/// dropped by HPERSIST. Both directions are checked against the rescan oracle.
#[test]
fn promote_and_persist_downgrade_keep_the_ledger_honest() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1"), (b"f2", b"v2")]);
    assert_ledger_matches_rescan(&mut db, "after HSET");

    let exp = db.now_ms() + 60_000;
    db.hash_set_field_ttl(b"h", b"f1", exp, HashTtlCond::Always)
        .unwrap();
    assert_ledger_matches_rescan(&mut db, "after HEXPIRE (promotion to HashWithTtl)");

    // HPERSIST drops the last TTL -> downgrade back to plain Hash.
    assert!(db.hash_persist_field(b"h", b"f1"));
    assert_ledger_matches_rescan(&mut db, "after HPERSIST (downgrade to Hash)");
}

/// The listpack promotion arm, checked against the rescan oracle rather than
/// only end-to-end — the conversion changes the cost MODEL (capacity-based
/// listpack -> per-field HashMap sum), so it can move the ledger in either
/// direction.
#[test]
fn listpack_promotion_charges_the_full_conversion() {
    let mut db = Database::new();
    make_hash_listpack_key(&mut db, b"lp", &[(b"alpha", b"one"), (b"beta", b"two")]);
    assert_ledger_matches_rescan(&mut db, "after listpack HSET");

    let exp = db.now_ms() + 60_000;
    db.hash_set_field_ttl(b"lp", b"alpha", exp, HashTtlCond::Always)
        .unwrap();
    assert_ledger_matches_rescan(&mut db, "after HEXPIRE (listpack -> HashWithTtl)");
}

/// HSET over a TTL'd field clears the sidecar; when it was the last TTL the
/// value downgrades to plain `Hash` and the box must be credited.
#[test]
fn clear_field_ttls_downgrade_keeps_the_ledger_honest() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1")]);
    let exp = db.now_ms() + 60_000;
    db.hash_set_field_ttl(b"h", b"f1", exp, HashTtlCond::Always)
        .unwrap();
    assert_ledger_matches_rescan(&mut db, "after HEXPIRE");

    db.hash_clear_field_ttls(b"h", &[&b"f1"[..]]);
    assert_ledger_matches_rescan(&mut db, "after HSET-clears-TTL downgrade");
}

/// The already-past-expiry short-circuit deletes the field and, when it was
/// the last TTL'd one, downgrades. Both the field credit and the box credit
/// must land.
#[test]
fn past_expiry_shortcircuit_keeps_the_ledger_honest() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1"), (b"f2", b"v2")]);
    let future = db.now_ms() + 60_000;
    db.hash_set_field_ttl(b"h", b"f1", future, HashTtlCond::Always)
        .unwrap();
    assert_ledger_matches_rescan(&mut db, "after HEXPIRE f1");

    // f2 with a past timestamp: field is deleted outright, code 2.
    let past = 1;
    assert_eq!(
        db.hash_set_field_ttl(b"h", b"f2", past, HashTtlCond::Always),
        Ok(2)
    );
    assert_ledger_matches_rescan(&mut db, "after past-expiry field delete");
}

/// HDEL of the last TTL'd field downgrades to plain `Hash` while live fields
/// remain — the `hash_delete_field` downgrade site.
#[test]
fn hdel_downgrade_keeps_the_ledger_honest() {
    let mut db = Database::new();
    make_hash_key(&mut db, b"h", &[(b"f1", b"v1"), (b"f2", b"v2")]);
    let exp = db.now_ms() + 60_000;
    db.hash_set_field_ttl(b"h", b"f1", exp, HashTtlCond::Always)
        .unwrap();
    assert_ledger_matches_rescan(&mut db, "after HEXPIRE f1");

    db.hash_delete_field(b"h", b"f1").unwrap();
    assert_ledger_matches_rescan(&mut db, "after HDEL f1 (downgrade to Hash)");
}
