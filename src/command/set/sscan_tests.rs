//! moon#1287: SSCAN pages the full set encoding by position.
//!
//! The property every SCAN-family command owes (redis `SCAN` guarantees):
//! an element present in the collection for the WHOLE scan is returned at
//! least once, whatever is added or removed between the calls. The old
//! SSCAN sorted a fresh snapshot on every call and paged by rank in it, so a
//! removal of a member ranked before the cursor shifted every later rank
//! down by one and the scan skipped a member it had never returned.

use std::collections::HashSet;

use bytes::Bytes;
use rand::{RngExt, SeedableRng};

use super::*;
use crate::protocol::Frame;
use crate::storage::Database;

fn bs(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

fn member(i: usize) -> Bytes {
    Bytes::from(format!("m:{i:06}"))
}

/// One SSCAN call: (next cursor, members returned).
fn scan_page(db: &mut Database, cursor: &[u8], extra: &[&[u8]]) -> (Bytes, Vec<Bytes>) {
    let mut args = vec![bs(b"s"), bs(cursor)];
    args.extend(extra.iter().map(|a| bs(a)));
    let Frame::Array(outer) = sscan(db, &args) else {
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

fn fill(db: &mut Database, n: usize) {
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let mut args = vec![bs(b"s")];
        args.extend(chunk.iter().map(|&i| Frame::BulkString(member(i))));
        sadd(db, &args);
    }
}

fn encoding(db: &mut Database) -> String {
    let now = db.now_ms();
    match crate::command::key::object_readonly(db, &[bs(b"ENCODING"), bs(b"s")], now) {
        Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => format!("{other:?}"),
    }
}

/// The SCAN guarantee under concurrent SREM / SPOP / SADD, over many
/// random schedules. Red on ce65400: removals ranked before the cursor
/// made the rank-in-sorted-snapshot cursor skip members.
#[test]
fn every_member_present_for_the_whole_scan_is_returned() {
    for seed in 0..64u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut db = Database::new();
        let initial = rng.random_range(200..3_000);
        fill(&mut db, initial);
        assert_eq!(encoding(&mut db), "hashtable", "fixture: full encoding");
        let mut live: HashSet<Bytes> = (0..initial).map(member).collect();
        let mut ever_removed: HashSet<Bytes> = HashSet::new();
        let mut next_new = initial;
        let mut returned: HashSet<Bytes> = HashSet::new();
        let count = rng.random_range(1..60usize).to_string();
        let mut cursor = Bytes::from_static(b"0");
        let mut calls = 0usize;
        loop {
            let (next, items) = scan_page(&mut db, &cursor, &[b"COUNT", count.as_bytes()]);
            calls += 1;
            returned.extend(items);
            if next.as_ref() == b"0" {
                break;
            }
            cursor = next;
            // Mutate between calls.
            for _ in 0..rng.random_range(0..8) {
                match rng.random_range(0..4) {
                    0 | 1 => {
                        // SREM a random live member.
                        if let Some(m) = live
                            .iter()
                            .nth(rng.random_range(0..live.len().max(1)))
                            .cloned()
                        {
                            srem(&mut db, &[bs(b"s"), Frame::BulkString(m.clone())]);
                            live.remove(&m);
                            ever_removed.insert(m);
                        }
                    }
                    2 => {
                        // SPOP: the server picks; learn which from the reply.
                        if let Frame::BulkString(m) = spop(&mut db, &[bs(b"s")]) {
                            live.remove(&m);
                            ever_removed.insert(m);
                        }
                    }
                    _ => {
                        let m = member(next_new);
                        next_new += 1;
                        sadd(&mut db, &[bs(b"s"), Frame::BulkString(m.clone())]);
                        live.insert(m);
                    }
                }
            }
            assert!(
                calls < 1_000_000,
                "seed {seed}: the scan does not terminate"
            );
        }
        let missed: Vec<Bytes> = (0..initial)
            .map(member)
            .filter(|m| !ever_removed.contains(m) && !returned.contains(m))
            .collect();
        assert!(
            missed.is_empty(),
            "seed {seed}: {} member(s) present for the whole scan were never returned, e.g. {:?}",
            missed.len(),
            &missed[..missed.len().min(5)]
        );
        // Nothing returned that was never in the set.
        for m in &returned {
            assert!(m.starts_with(b"m:"), "seed {seed}: foreign member {m:?}");
        }
    }
}

/// Without mutation a full scan returns every member EXACTLY once, in
/// `ceil(N / COUNT)` calls.
#[test]
fn a_quiet_scan_returns_each_member_once() {
    let mut db = Database::new();
    fill(&mut db, 1_000);
    let mut seen = Vec::new();
    let mut cursor = Bytes::from_static(b"0");
    let mut calls = 0;
    loop {
        let (next, items) = scan_page(&mut db, &cursor, &[b"COUNT", b"100"]);
        calls += 1;
        assert!(items.len() <= 100);
        seen.extend(items);
        if next.as_ref() == b"0" {
            break;
        }
        cursor = next;
    }
    assert_eq!(calls, 10);
    let unique: HashSet<_> = seen.iter().cloned().collect();
    assert_eq!(seen.len(), 1_000, "no duplicates without mutation");
    assert_eq!(unique.len(), 1_000);
}

/// A cursor beyond the set's length (the set shrank) clamps: nothing below
/// it was visited yet. MATCH filters the page, it does not stop it.
#[test]
fn a_cursor_past_the_end_clamps_and_match_filters() {
    let mut db = Database::new();
    fill(&mut db, 300);
    let (next, items) = scan_page(&mut db, b"100000", &[b"COUNT", b"1000"]);
    assert_eq!(next.as_ref(), b"0");
    assert_eq!(items.len(), 300);
    let (_, items) = scan_page(&mut db, b"0", &[b"MATCH", b"m:00000*", b"COUNT", b"1000"]);
    assert_eq!(items.len(), 10, "m:000000..m:000009");
}

/// Compact encodings answer in one call with cursor 0 whatever COUNT says,
/// as redis does — so a small set never has a cursor in flight when it
/// grows into the full encoding.
#[test]
fn a_compact_set_is_returned_whole_in_one_call() {
    let mut db = Database::new();
    fill(&mut db, 50);
    assert_eq!(encoding(&mut db), "listpack");
    let (next, items) = scan_page(&mut db, b"0", &[b"COUNT", b"2"]);
    assert_eq!(next.as_ref(), b"0");
    assert_eq!(items.len(), 50);
    // A stale non-zero cursor still yields everything.
    let (next, items) = scan_page(&mut db, b"37", &[b"COUNT", b"2"]);
    assert_eq!(next.as_ref(), b"0");
    assert_eq!(items.len(), 50);

    let mut db = Database::new();
    for i in 0..20 {
        sadd(
            &mut db,
            &[bs(b"s"), Frame::BulkString(Bytes::from(i.to_string()))],
        );
    }
    assert_eq!(encoding(&mut db), "intset");
    let (next, items) = scan_page(&mut db, b"0", &[b"COUNT", b"3"]);
    assert_eq!(next.as_ref(), b"0");
    assert_eq!(items.len(), 20);
}
