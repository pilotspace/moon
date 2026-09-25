//! moon#1255: an in-flight spill record whose TTL has passed must be retired
//! when the key is answered absent or created afresh.
//!
//! `promote_inflight_if_present` answered `false` for an expired record
//! without retiring it. A write then fabricated the key (`get_or_create*`,
//! `insert_fresh`) while the stale record was still its request's
//! authorization to publish. The completion published the EXPIRED slot as the
//! key's cold entry behind the live value and logged its `MOON.SPILLED` AFTER
//! the write, so replaying that marker dropped the acknowledged write
//! (review of moon#1253: `pre_existing_the_marker_of_an_expired_in_flight_
//! slot_drops_a_later_write`). The completion half is pinned in
//! `shard::persistence_tick::superseded_settle_tests`.

use bytes::Bytes;

use crate::persistence::kv_page::ValueType;
use crate::storage::Database;
use crate::storage::db::PendingSpill;

fn in_flight(db: &mut Database, key: &'static [u8], req_id: u64, ttl_ms: Option<u64>) {
    db.spill_inflight_mark(
        Bytes::from_static(key),
        PendingSpill {
            req_id,
            value_type: ValueType::String,
            value_bytes: Bytes::from_static(b"old"),
            ttl_ms,
        },
    );
}

/// A collection write re-creates an expired in-flight key: the record goes,
/// superseded (moon#1253), so the completion cannot publish it.
#[test]
fn a_write_that_recreates_an_expired_in_flight_key_retires_its_record() {
    let mut db = Database::new();
    in_flight(&mut db, b"k", 7, Some(1));
    db.get_or_create_list(b"k")
        .expect("list")
        .push_back(Bytes::from_static(b"a"));
    assert!(
        !db.spill_inflight_is_newest(b"k", 7),
        "request 7 would publish k's EXPIRED slot behind the live list"
    );
    assert_eq!(db.spill_superseded_len(), 1, "retired as superseded");
    assert!(db.is_hot(b"k"));
}

/// A read of an expired in-flight key answers absent and retires the record.
#[test]
fn a_read_of_an_expired_in_flight_key_retires_its_record() {
    let mut db = Database::new();
    in_flight(&mut db, b"k", 7, Some(1));
    assert!(!db.promote_inflight_if_present(b"k", 10));
    assert!(!db.spill_inflight_is_newest(b"k", 7));
    assert!(db.spill_inflight_is_empty());
}

/// A record that has NOT expired but whose payload does not rehydrate is the
/// key's only copy: it stays.
#[test]
fn an_unreadable_but_live_in_flight_record_stays() {
    let mut db = Database::new();
    db.spill_inflight_mark(
        Bytes::from_static(b"k"),
        PendingSpill {
            req_id: 7,
            value_type: ValueType::Hash,
            value_bytes: Bytes::from_static(b"\xff not a hash encoding"),
            ttl_ms: None,
        },
    );
    assert!(!db.promote_inflight_if_present(b"k", 10));
    assert!(db.spill_inflight_is_newest(b"k", 7), "not expired: kept");
    assert!(db.spill_superseded_is_empty());
}
