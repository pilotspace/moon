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

// ---------------------------------------------------------------------------
// moon#1287 wave-1 review F1: whole-value replacement mid-scan
// ---------------------------------------------------------------------------

use super::sscan_cursor::test_hooks;

fn cursor_u64(c: &Bytes) -> u64 {
    std::str::from_utf8(c).unwrap().parse().unwrap()
}

/// Full scan with `mutate(db, page_no)` run between the calls; returns
/// (members returned, calls, whether any cursor was a hash-mode one).
fn scan_all(
    db: &mut Database,
    count: &[u8],
    mut mutate: impl FnMut(&mut Database, usize),
) -> (HashSet<Bytes>, usize, bool) {
    let mut returned = HashSet::new();
    let mut cursor = Bytes::from_static(b"0");
    let mut calls = 0;
    let mut hash_mode = false;
    loop {
        let (next, items) = scan_page(db, &cursor, &[b"COUNT", count]);
        calls += 1;
        returned.extend(items);
        if next.as_ref() == b"0" {
            break;
        }
        hash_mode |= test_hooks::is_hash_mode(cursor_u64(&next));
        cursor = next;
        mutate(db, calls);
        assert!(calls < 100_000, "the scan does not terminate");
    }
    (returned, calls, hash_mode)
}

/// Every cursor stays below 2^63 (clients that parse a signed 64-bit
/// integer), and a quiet scan never leaves position mode.
#[test]
fn a_quiet_scan_stays_in_position_mode_with_small_cursors() {
    let mut db = Database::new();
    fill(&mut db, 5_000);
    let mut cursor = Bytes::from_static(b"0");
    loop {
        let (next, _) = scan_page(&mut db, &cursor, &[b"COUNT", b"37"]);
        if next.as_ref() == b"0" {
            break;
        }
        let c = cursor_u64(&next);
        assert!(c < 1 << 63, "cursor {c} does not fit an i64");
        assert!(!test_hooks::is_hash_mode(c), "a quiet scan fell back");
        cursor = next;
    }
}

/// A set REBUILT between every pair of calls (the review's SUNIONSTORE
/// rewrite, repeated) is still scanned completely, and the scan terminates
/// in about `HASH_MODE_PAGES` calls — a restart-on-rewrite design would
/// never finish.
#[test]
fn a_set_rebuilt_between_every_call_is_scanned_whole_and_terminates() {
    let n = 20_000;
    let mut db = Database::new();
    fill(&mut db, n);
    let (returned, calls, hash_mode) = scan_all(&mut db, b"10", |db, _| {
        assert_eq!(
            sunionstore(db, &[bs(b"s"), bs(b"s")]),
            Frame::Integer(n as i64)
        );
    });
    assert!(hash_mode, "the rewrite must be detected");
    let missed = (0..n).map(member).filter(|m| !returned.contains(m)).count();
    assert_eq!(missed, 0, "members present for the whole scan were skipped");
    assert!(
        calls <= super::sscan_cursor::HASH_MODE_PAGES + 2,
        "{calls} calls: hash mode pages by max(COUNT, N / HASH_MODE_PAGES)"
    );
}

/// Every whole-value replacement that keeps the membership — SINTERSTORE,
/// SDIFFSTORE, RENAME onto the key, COPY ... REPLACE onto the key — once,
/// mid-scan, among random SREM / SPOP / SADD.
#[test]
fn every_member_present_for_the_whole_scan_survives_any_replacement() {
    type Rewrite = fn(&mut Database);
    let rewrites: [(&str, Rewrite); 4] = [
        ("SINTERSTORE s s s", |db| {
            sinterstore(db, &[bs(b"s"), bs(b"s"), bs(b"s")]);
        }),
        ("SDIFFSTORE s s nothing", |db| {
            sdiffstore(db, &[bs(b"s"), bs(b"s"), bs(b"nothing")]);
        }),
        ("SUNIONSTORE t s + RENAME t s", |db| {
            sunionstore(db, &[bs(b"t"), bs(b"s")]);
            crate::command::key::rename(db, &[bs(b"t"), bs(b"s")]);
        }),
        ("SUNIONSTORE t s + COPY t s REPLACE", |db| {
            sunionstore(db, &[bs(b"t"), bs(b"s")]);
            crate::command::key_extra::copy(db, &[bs(b"t"), bs(b"s"), bs(b"REPLACE")]);
        }),
    ];
    for (name, rewrite) in rewrites {
        for seed in 0..8u64 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut db = Database::new();
            let initial = rng.random_range(600..4_000);
            fill(&mut db, initial);
            let mut removed: HashSet<Bytes> = HashSet::new();
            let mut next_new = initial;
            let rewrite_at = rng.random_range(1..6usize);
            let count = rng.random_range(5..80usize).to_string();
            let (returned, _, _) = scan_all(&mut db, count.as_bytes(), |db, page| {
                if page == rewrite_at {
                    rewrite(db);
                }
                for _ in 0..rng.random_range(0..6) {
                    match rng.random_range(0..3) {
                        0 => {
                            let m = member(rng.random_range(0..initial));
                            srem(db, &[bs(b"s"), Frame::BulkString(m.clone())]);
                            removed.insert(m);
                        }
                        1 => {
                            if let Frame::BulkString(m) = spop(db, &[bs(b"s")]) {
                                removed.insert(m);
                            }
                        }
                        _ => {
                            sadd(db, &[bs(b"s"), Frame::BulkString(member(next_new))]);
                            next_new += 1;
                        }
                    }
                }
            });
            let missed: Vec<Bytes> = (0..initial)
                .map(member)
                .filter(|m| !removed.contains(m) && !returned.contains(m))
                .collect();
            assert!(
                missed.is_empty(),
                "{name}, seed {seed}: {} member(s) present for the whole scan never returned, \
                 e.g. {:?}",
                missed.len(),
                &missed[..missed.len().min(3)]
            );
        }
    }
}

/// Hash mode never splits members that share a hash across two pages, and
/// makes progress when a whole page is one hash (forced with a coarse hash:
/// 8 buckets for 1000 members, COUNT 5).
#[test]
fn hash_mode_ties_straddling_a_page_are_all_returned() {
    let set: crate::storage::entry::SetValue = (0..1_000).map(member).collect();
    for buckets in [8u64, 1, 997] {
        let coarse = move |m: &[u8]| xxhash_rust::xxh64::xxh64(m, 7) % buckets;
        let mut below = None;
        let mut returned = HashSet::new();
        let mut calls = 0;
        loop {
            let (next, items) = test_hooks::hash_page_by(&set, below, 5, coarse);
            returned.extend(items);
            calls += 1;
            assert!(calls < 5_000, "{buckets} buckets: no progress");
            match next {
                None => break,
                Some(n) => below = Some(n),
            }
        }
        assert_eq!(returned.len(), 1_000, "{buckets} buckets: members skipped");
    }
}

/// A value decoded fresh from the cold tier on every call has a new layout
/// each time: it scans in hash mode from the first call, whole.
#[test]
fn an_unstable_layout_scans_in_hash_mode_from_the_start() {
    let set: crate::storage::entry::SetValue = (0..3_000).map(member).collect();
    let mut cursor = 0u64;
    let mut returned = HashSet::new();
    loop {
        // A fresh instance per call, as a cold decode produces.
        let fresh: crate::storage::entry::SetValue = set.iter().cloned().collect();
        let Frame::Array(outer) = test_hooks::sscan_page_unstable(&fresh, cursor, 10) else {
            panic!()
        };
        let (Frame::BulkString(c), Frame::Array(items)) = (&outer[0], &outer[1]) else {
            panic!()
        };
        for f in items.iter() {
            if let Frame::BulkString(b) = f {
                returned.insert(b.clone());
            }
        }
        cursor = cursor_u64(c);
        if cursor == 0 {
            break;
        }
        assert!(test_hooks::is_hash_mode(cursor));
    }
    assert_eq!(returned.len(), 3_000);
}

/// redis 7.0.15 answers an intset in ascending numeric order and accepts
/// `-1` (strtoul) as a cursor; moon answered `1 10 2 3` and
/// `ERR invalid cursor`.
#[test]
fn intset_order_is_numeric_and_a_minus_one_cursor_is_accepted() {
    let mut db = Database::new();
    for v in ["10", "2", "3", "1", "100", "-5"] {
        sadd(&mut db, &[bs(b"s"), bs(v.as_bytes())]);
    }
    assert_eq!(encoding(&mut db), "intset");
    let (next, items) = scan_page(&mut db, b"0", &[]);
    assert_eq!(next.as_ref(), b"0");
    let got: Vec<&[u8]> = items.iter().map(|b| b.as_ref()).collect();
    assert_eq!(got, [&b"-5"[..], b"1", b"2", b"3", b"10", b"100"]);
    let (next, items) = scan_page(&mut db, b"-1", &[]);
    assert_eq!(next.as_ref(), b"0");
    assert_eq!(items.len(), 6);
    // On the full encoding `-1` is a foreign cursor: a hash-mode walk from
    // the top, which still terminates and returns every member.
    let mut db = Database::new();
    fill(&mut db, 2_000);
    let mut cursor = Bytes::from_static(b"-1");
    let mut returned = HashSet::new();
    loop {
        let (next, items) = scan_page(&mut db, &cursor, &[b"COUNT", b"50"]);
        returned.extend(items);
        if next.as_ref() == b"0" {
            break;
        }
        cursor = next;
    }
    assert_eq!(returned.len(), 2_000);
    let Frame::Error(e) = sscan(&mut db, &[bs(b"s"), bs(b" 1")]) else {
        panic!("a leading space must be refused")
    };
    assert_eq!(e.as_ref(), b"ERR invalid cursor");
}
