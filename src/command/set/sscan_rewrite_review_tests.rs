//! Adversarial review of moon#1287 (wave 1): the SSCAN position cursor
//! against a WRITE that REBUILDS the scanned set.
//!
//! The position cursor is sound only while nothing moves a member UP
//! (`sscan_positions` doc). SREM / SPOP / SMOVE / SADD respect that. The
//! `*STORE` family does not: it replaces the destination with a FRESH set
//! whose order is the algebra's iteration order — `union` collects into a
//! `std::collections::HashSet` (random order), `intersect` walks the
//! SMALLEST input. A `SUNIONSTORE s s extra` (bulk add) or `SINTERSTORE s s
//! filter` (bulk filter) between two SSCAN calls reshuffles every position.
//!
//! redis keeps the guarantee here (its cursor is a hash-bucket index, the
//! same for the same members), and so did moon's pre-#1287 cursor (rank in a
//! sorted snapshot: the same members give the same ranks). Red on f7f1d96,
//! green on ce65400.

use std::collections::HashSet;

use bytes::Bytes;

use super::*;
use crate::protocol::Frame;
use crate::storage::Database;

fn bs(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

fn member(i: usize) -> Bytes {
    Bytes::from(format!("m:{i:06}"))
}

fn scan_page(db: &mut Database, key: &[u8], cursor: &[u8], count: &[u8]) -> (Bytes, Vec<Bytes>) {
    let Frame::Array(outer) = sscan(db, &[bs(key), bs(cursor), bs(b"COUNT"), bs(count)]) else {
        panic!("SSCAN reply is not an array")
    };
    let Frame::BulkString(next) = &outer[0] else {
        panic!("cursor is not a bulk string")
    };
    let Frame::Array(items) = &outer[1] else {
        panic!("items are not an array")
    };
    let items = items
        .iter()
        .map(|f| match f {
            Frame::BulkString(b) => b.clone(),
            other => panic!("unexpected item {other:?}"),
        })
        .collect();
    (next.clone(), items)
}

fn fill(db: &mut Database, key: &[u8], range: std::ops::Range<usize>) {
    let all: Vec<usize> = range.collect();
    for chunk in all.chunks(500) {
        let mut args = vec![bs(key)];
        args.extend(chunk.iter().map(|&i| Frame::BulkString(member(i))));
        sadd(db, &args);
    }
}

/// Scan `s` with one rewrite (`rewrite`) between the first and second page;
/// return the members of `must` the scan never returned.
fn missed_after_rewrite(
    n: usize,
    setup: impl Fn(&mut Database),
    rewrite: impl Fn(&mut Database),
    must: &HashSet<Bytes>,
) -> Vec<Bytes> {
    let mut db = Database::new();
    fill(&mut db, b"s", 0..n);
    setup(&mut db);
    let mut returned: HashSet<Bytes> = HashSet::new();
    let (mut cursor, first) = scan_page(&mut db, b"s", b"0", b"100");
    returned.extend(first);
    assert_ne!(
        cursor.as_ref(),
        b"0",
        "fixture: the scan needs several pages"
    );
    rewrite(&mut db);
    let mut guard = 0;
    while cursor.as_ref() != b"0" {
        let (next, items) = scan_page(&mut db, b"s", &cursor, b"100");
        returned.extend(items);
        cursor = next;
        guard += 1;
        assert!(guard < 10_000, "the scan never terminated");
    }
    let mut missed: Vec<Bytes> = must.difference(&returned).cloned().collect();
    missed.sort();
    missed
}

/// `SUNIONSTORE s s` — the membership does not change at all, every member
/// is present for the whole scan, and the scan must return all of them.
#[test]
fn sscan_returns_every_member_across_a_same_membership_sunionstore() {
    let n = 2_000;
    let must: HashSet<Bytes> = (0..n).map(member).collect();
    let missed = missed_after_rewrite(
        n,
        |_| {},
        |db| {
            let r = sunionstore(db, &[bs(b"s"), bs(b"s")]);
            assert_eq!(r, Frame::Integer(n as i64));
        },
        &must,
    );
    assert!(
        missed.is_empty(),
        "SSCAN skipped {} of {n} members present for the whole scan after SUNIONSTORE s s \
         (first: {:?})",
        missed.len(),
        missed.first()
    );
}

/// `SINTERSTORE s filter s` where `filter` holds every member of `s` in the
/// reverse order: the intersection walks the smaller input first... both are
/// equal-sized, so it walks the FIRST minimum — `filter` — and the rewritten
/// set is `s` reversed. Membership unchanged; the scan must return all.
#[test]
fn sscan_returns_every_member_across_a_same_membership_sinterstore() {
    let n = 2_000;
    let must: HashSet<Bytes> = (0..n).map(member).collect();
    let missed = missed_after_rewrite(
        n,
        |db| {
            // `filter` built in reverse insertion order.
            for i in (0..n).rev() {
                sadd(db, &[bs(b"filter"), Frame::BulkString(member(i))]);
            }
        },
        |db| {
            let r = sinterstore(db, &[bs(b"s"), bs(b"filter"), bs(b"s")]);
            assert_eq!(r, Frame::Integer(n as i64));
        },
        &must,
    );
    assert!(
        missed.is_empty(),
        "SSCAN skipped {} of {n} members present for the whole scan after SINTERSTORE \
         (first: {:?})",
        missed.len(),
        missed.first()
    );
}
