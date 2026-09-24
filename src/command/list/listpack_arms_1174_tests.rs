//! moon#1174 §1: LTRIM / LREM / LINSERT / LMOVE / RPOPLPUSH / LMPOP keep a
//! listpack list a listpack.
//!
//! Every one of them gated with `db.get_list` (= `get_promoted`) and then took
//! `get_or_create_list`, whose `ListKind::upgrade` flattens unconditionally and
//! forever, so the most common list idioms -- a capped recent-items list
//! (`LPUSH` + `LTRIM 0 N`) and a reliable queue (`RPOPLPUSH`) -- turned every
//! small list into a `linkedlist` on first touch. redis 7.2+ keeps them
//! listpacks. (redis 7.0.15, the pinned oracle, has no list listpack encoding at
//! all and answers `quicklist` for every list, so it has no shrink-back either:
//! moon does not add one.)
//!
//! Every guard here also checks that the fix did not disable the policy it
//! preserves (both sides of the 128-element / 64-byte thresholds), that the
//! ledger stays exact against a full recompute, and that numeric-looking bytes
//! survive (moon#795).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

use super::{linsert, lmove, lmpop, lpush, lrange_readonly, lrem, ltrim, rpoplpush, rpush};

fn bs(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

fn encoding_of(db: &mut Database, key: &[u8]) -> String {
    match crate::command::key::object(db, &[bs(b"ENCODING"), bs(key)]) {
        Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        Frame::Null => "<missing>".to_owned(),
        other => panic!("OBJECT ENCODING answered {other:?}"),
    }
}

fn assert_ledger_exact(db: &mut Database, step: &str) {
    let running = db.estimated_memory();
    db.recalculate_memory();
    let recomputed = db.estimated_memory();
    assert_eq!(running, recomputed, "{step}: running ledger != recompute");
}

fn elements(db: &Database, key: &[u8]) -> Vec<Vec<u8>> {
    match lrange_readonly(db, &[bs(key), bs(b"0"), bs(b"-1")], 0) {
        Frame::Array(items) => items
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.to_vec(),
                other => panic!("expected bulk, got {other:?}"),
            })
            .collect(),
        other => panic!("LRANGE answered {other:?}"),
    }
}

fn setup(db: &mut Database, key: &[u8], values: &[&[u8]]) {
    let mut args = vec![bs(key)];
    args.extend(values.iter().map(|v| bs(v)));
    rpush(db, &args);
}

fn owned(values: &[&[u8]]) -> Vec<Vec<u8>> {
    values.iter().map(|v| v.to_vec()).collect()
}

/// The idiom the issue names: `LPUSH k x; LTRIM k 0 99`, over and over.
/// HEAD flattened on the FIRST trim and never came back.
#[test]
fn a_capped_list_stays_a_listpack() {
    let mut db = Database::new();
    for i in 0..300u32 {
        let item = format!("event:{i:08}:xxxxxxxx");
        assert!(matches!(
            lpush(&mut db, &[bs(b"recent"), bs(item.as_bytes())]),
            Frame::Integer(_)
        ));
        assert_eq!(ltrim(&mut db, &[bs(b"recent"), bs(b"0"), bs(b"99")]), ok());
        assert_eq!(
            encoding_of(&mut db, b"recent"),
            "listpack",
            "moon#1174 §1: LTRIM flattened the capped list at push {i}"
        );
    }
    let got = elements(&db, b"recent");
    assert_eq!(got.len(), 100);
    assert_eq!(got[0], b"event:00000299:xxxxxxxx".to_vec());
    assert_eq!(got[99], b"event:00000200:xxxxxxxx".to_vec());
    assert_ledger_exact(&mut db, "capped list");
}

/// Each secondary write, once, on a small listpack: same answer as before,
/// same encoding after.
#[test]
fn each_secondary_write_keeps_a_small_listpack() {
    type Case = (
        &'static str,
        fn(&mut Database) -> Frame,
        Frame,
        Vec<Vec<u8>>,
    );
    let cases: Vec<Case> = vec![
        (
            "LTRIM 1 -2",
            |db| ltrim(db, &[bs(b"l"), bs(b"1"), bs(b"-2")]),
            ok(),
            owned(&[b"b", b"a", b"c"]),
        ),
        (
            "LREM 1 a",
            |db| lrem(db, &[bs(b"l"), bs(b"1"), bs(b"a")]),
            Frame::Integer(1),
            owned(&[b"b", b"a", b"c", b"a"]),
        ),
        (
            "LREM -1 a",
            |db| lrem(db, &[bs(b"l"), bs(b"-1"), bs(b"a")]),
            Frame::Integer(1),
            owned(&[b"a", b"b", b"a", b"c"]),
        ),
        (
            "LINSERT BEFORE c",
            |db| linsert(db, &[bs(b"l"), bs(b"BEFORE"), bs(b"c"), bs(b"x")]),
            Frame::Integer(6),
            owned(&[b"a", b"b", b"a", b"x", b"c", b"a"]),
        ),
        (
            "LINSERT AFTER a",
            |db| linsert(db, &[bs(b"l"), bs(b"after"), bs(b"a"), bs(b"x")]),
            Frame::Integer(6),
            owned(&[b"a", b"x", b"b", b"a", b"c", b"a"]),
        ),
        (
            "LINSERT missing pivot",
            |db| linsert(db, &[bs(b"l"), bs(b"BEFORE"), bs(b"zz"), bs(b"x")]),
            Frame::Integer(-1),
            owned(&[b"a", b"b", b"a", b"c", b"a"]),
        ),
        (
            "LMOVE l d LEFT RIGHT",
            |db| lmove(db, &[bs(b"l"), bs(b"d"), bs(b"LEFT"), bs(b"RIGHT")]),
            Frame::BulkString(Bytes::from_static(b"a")),
            owned(&[b"b", b"a", b"c", b"a"]),
        ),
        (
            "RPOPLPUSH l d",
            |db| rpoplpush(db, &[bs(b"l"), bs(b"d")]),
            Frame::BulkString(Bytes::from_static(b"a")),
            owned(&[b"a", b"b", b"a", b"c"]),
        ),
        (
            "LMOVE l l RIGHT LEFT (rotate)",
            |db| lmove(db, &[bs(b"l"), bs(b"l"), bs(b"RIGHT"), bs(b"LEFT")]),
            Frame::BulkString(Bytes::from_static(b"a")),
            owned(&[b"a", b"a", b"b", b"a", b"c"]),
        ),
        (
            "LMPOP 1 l RIGHT COUNT 2",
            |db| {
                lmpop(
                    db,
                    &[bs(b"1"), bs(b"l"), bs(b"RIGHT"), bs(b"COUNT"), bs(b"2")],
                )
            },
            Frame::Array(crate::framevec![
                bs(b"l"),
                Frame::Array(crate::framevec![bs(b"a"), bs(b"c")])
            ]),
            owned(&[b"a", b"b", b"a"]),
        ),
    ];
    for (name, op, want_reply, want_after) in cases {
        let mut db = Database::new();
        setup(&mut db, b"l", &[b"a", b"b", b"a", b"c", b"a"]);
        assert_eq!(encoding_of(&mut db, b"l"), "listpack", "{name}: fixture");
        assert_eq!(op(&mut db), want_reply, "{name}: reply");
        assert_eq!(elements(&db, b"l"), want_after, "{name}: contents");
        assert_eq!(
            encoding_of(&mut db, b"l"),
            "listpack",
            "moon#1174 §1: {name} flattened a 5-element listpack"
        );
        if name.starts_with("LMOVE l d") || name.starts_with("RPOPLPUSH") {
            assert_eq!(elements(&db, b"d"), owned(&[b"a"]), "{name}: destination");
            assert_eq!(
                encoding_of(&mut db, b"d"),
                "listpack",
                "{name}: a destination born from a small element is a listpack, as LPUSH makes it"
            );
        }
        assert_ledger_exact(&mut db, name);
    }
}

/// The policy still decides: every arm promotes exactly where a push would.
#[test]
fn the_arms_still_promote_past_the_threshold() {
    let limits = crate::storage::db::EncodingLimits::moon_defaults();
    let n = limits.list_entries;
    let fill = |db: &mut Database, key: &[u8], count: usize| {
        let values: Vec<Vec<u8>> = (0..count)
            .map(|i| format!("e{i:05}").into_bytes())
            .collect();
        let mut args = vec![bs(key)];
        args.extend(values.iter().map(|v| bs(v)));
        rpush(db, &args);
    };

    // LINSERT into a full listpack: 128 -> 129 promotes, as RPUSH would.
    let mut db = Database::new();
    fill(&mut db, b"l", n);
    assert_eq!(encoding_of(&mut db, b"l"), "listpack");
    assert_eq!(
        linsert(&mut db, &[bs(b"l"), bs(b"BEFORE"), bs(b"e00000"), bs(b"x")]),
        Frame::Integer(n as i64 + 1)
    );
    assert_eq!(encoding_of(&mut db, b"l"), "linkedlist");
    assert_eq!(elements(&db, b"l")[0], b"x".to_vec());
    assert_ledger_exact(&mut db, "LINSERT past the entry threshold");

    // LINSERT of an over-long element promotes; with a MISSING pivot it
    // changes nothing at all.
    let big = vec![b'v'; limits.max_value(crate::storage::db::Shape::List) + 1];
    let mut db = Database::new();
    setup(&mut db, b"l", &[b"a", b"b"]);
    assert_eq!(
        linsert(&mut db, &[bs(b"l"), bs(b"AFTER"), bs(b"zz"), bs(&big)]),
        Frame::Integer(-1)
    );
    assert_eq!(
        encoding_of(&mut db, b"l"),
        "listpack",
        "a no-op must not promote"
    );
    assert_eq!(
        linsert(&mut db, &[bs(b"l"), bs(b"AFTER"), bs(b"a"), bs(&big)]),
        Frame::Integer(3)
    );
    assert_eq!(encoding_of(&mut db, b"l"), "linkedlist");
    assert_eq!(
        elements(&db, b"l"),
        vec![b"a".to_vec(), big.clone(), b"b".to_vec()]
    );
    assert_ledger_exact(&mut db, "LINSERT of an over-long element");

    // LMOVE into a full listpack destination promotes it; out of a
    // linkedlist source into a missing destination, the destination is born
    // exactly as RPUSH would make it -- listpack for a small element,
    // linkedlist for an over-long one.
    let mut db = Database::new();
    fill(&mut db, b"d", n);
    setup(&mut db, b"s", &[b"x"]);
    assert_eq!(
        lmove(&mut db, &[bs(b"s"), bs(b"d"), bs(b"LEFT"), bs(b"RIGHT")]),
        Frame::BulkString(Bytes::from_static(b"x"))
    );
    assert_eq!(encoding_of(&mut db, b"d"), "linkedlist");
    assert_eq!(
        encoding_of(&mut db, b"s"),
        "<missing>",
        "the drained source is deleted"
    );
    assert_ledger_exact(&mut db, "LMOVE into a full listpack");

    let mut db = Database::new();
    fill(&mut db, b"s", n + 10);
    rpush(&mut db, &[bs(b"s"), bs(&big)]);
    assert_eq!(encoding_of(&mut db, b"s"), "linkedlist");
    rpoplpush(&mut db, &[bs(b"s"), bs(b"big")]);
    assert_eq!(encoding_of(&mut db, b"big"), "linkedlist");
    rpoplpush(&mut db, &[bs(b"s"), bs(b"small")]);
    assert_eq!(encoding_of(&mut db, b"small"), "listpack");
    assert_eq!(encoding_of(&mut db, b"s"), "linkedlist", "nothing demotes");
    assert_ledger_exact(&mut db, "RPOPLPUSH out of a linkedlist");

    // LTRIM / LREM of a listpack that a TIGHTENED policy no longer fits
    // promotes it, like LPOP does (the post-mutation check is not decoration).
    for op in ["LTRIM", "LREM"] {
        let mut db = Database::new();
        fill(&mut db, b"l", 20);
        db.set_encoding_limits(crate::storage::db::EncodingLimits {
            list_entries: 4,
            ..limits
        });
        match op {
            "LTRIM" => assert_eq!(ltrim(&mut db, &[bs(b"l"), bs(b"1"), bs(b"-1")]), ok()),
            _ => assert_eq!(
                lrem(&mut db, &[bs(b"l"), bs(b"0"), bs(b"e00000")]),
                Frame::Integer(1)
            ),
        }
        assert_eq!(
            encoding_of(&mut db, b"l"),
            "linkedlist",
            "{op} under a tightened policy"
        );
        assert_eq!(elements(&db, b"l").len(), 19);
        assert_ledger_exact(&mut db, op);
    }
}

/// LTRIM's window arithmetic on a listpack, against the same windows on a
/// linkedlist (the pre-existing path, unchanged), for every start/stop sign.
#[test]
fn ltrim_windows_match_the_linkedlist_path() {
    let values: Vec<Vec<u8>> = (0..9).map(|i| format!("v{i}").into_bytes()).collect();
    let windows: [i64; 9] = [-100, -9, -5, -1, 0, 1, 4, 8, 100];
    for &start in &windows {
        for &stop in &windows {
            let mut small = Database::new();
            let mut big = Database::new();
            let mut args = vec![bs(b"l")];
            args.extend(values.iter().map(|v| bs(v)));
            rpush(&mut small, &args);
            // Same nine values, but the list went past the threshold first and
            // was trimmed back, so it is a linkedlist holding them.
            let mut bargs = vec![bs(b"l")];
            bargs.extend((0..200).map(|_| bs(b"pad")));
            bargs.extend(values.iter().map(|v| bs(v)));
            rpush(&mut big, &bargs);
            ltrim(&mut big, &[bs(b"l"), bs(b"200"), bs(b"-1")]);
            assert_eq!(encoding_of(&mut big, b"l"), "linkedlist");
            assert_eq!(elements(&big, b"l"), values);

            let a = [
                bs(b"l"),
                bs(start.to_string().as_bytes()),
                bs(stop.to_string().as_bytes()),
            ];
            assert_eq!(ltrim(&mut small, &a), ok());
            assert_eq!(ltrim(&mut big, &a), ok());
            assert_eq!(
                elements(&small, b"l"),
                elements(&big, b"l"),
                "LTRIM {start} {stop}"
            );
            let want = if elements(&small, b"l").is_empty() {
                "<missing>"
            } else {
                "listpack"
            };
            assert_eq!(encoding_of(&mut small, b"l"), want, "LTRIM {start} {stop}");
            assert_ledger_exact(&mut small, "LTRIM window");
            assert_ledger_exact(&mut big, "LTRIM window (linkedlist)");
        }
    }
}

/// `RPUSH k a; EXPIRE k 100; LMOVE k k LEFT RIGHT; TTL k` is 100 on redis
/// 7.0.15. HEAD answered -1: the pop deleted the one-element key, TTL and
/// all, and the push created a new one. The rotation is now in place, on
/// both encodings.
#[test]
fn a_rotation_keeps_the_key_and_its_ttl() {
    for compact in [true, false] {
        let mut db = Database::new();
        if compact {
            setup(&mut db, b"k", &[b"a"]);
        } else {
            let pad: Vec<Frame> = std::iter::once(bs(b"k"))
                .chain((0..200).map(|_| bs(b"pad")))
                .collect();
            rpush(&mut db, &pad);
            ltrim(&mut db, &[bs(b"k"), bs(b"0"), bs(b"0")]);
        }
        let want = if compact { "listpack" } else { "linkedlist" };
        assert_eq!(encoding_of(&mut db, b"k"), want, "fixture");
        let deadline = db.now_ms() + 100_000;
        assert!(db.set_expiry(b"k", deadline), "fixture: EXPIRE");
        for (from, to) in [(&b"LEFT"[..], &b"RIGHT"[..]), (b"RIGHT", b"LEFT")] {
            let got = lmove(&mut db, &[bs(b"k"), bs(b"k"), bs(from), bs(to)]);
            assert!(
                matches!(got, Frame::BulkString(_)),
                "compact={compact}: {got:?}"
            );
            assert_eq!(
                db.get(b"k").map(|e| e.expires_at_ms()),
                Some(deadline),
                "compact={compact}: LMOVE k k dropped the key's TTL"
            );
        }
        assert_eq!(encoding_of(&mut db, b"k"), want);
        assert_eq!(elements(&db, b"k").len(), 1);
        assert_ledger_exact(&mut db, "rotation");
    }
}

/// moon#795/#903: numeric-looking bytes survive every arm.
#[test]
fn the_arms_are_byte_transparent() {
    let tricky: [&[u8]; 5] = [b"+5", b"000000012345", b"-0", b"7", b"-9223372036854775808"];
    let mut db = Database::new();
    setup(&mut db, b"l", &tricky);
    linsert(&mut db, &[bs(b"l"), bs(b"AFTER"), bs(b"7"), bs(b"007")]);
    lrem(&mut db, &[bs(b"l"), bs(b"0"), bs(b"5")]); // must not match "+5"
    ltrim(&mut db, &[bs(b"l"), bs(b"0"), bs(b"-1")]);
    lmove(&mut db, &[bs(b"l"), bs(b"d"), bs(b"RIGHT"), bs(b"LEFT")]);
    assert_eq!(
        elements(&db, b"l"),
        owned(&[b"+5", b"000000012345", b"-0", b"7", b"007"])
    );
    assert_eq!(elements(&db, b"d"), owned(&[b"-9223372036854775808"]));
    assert_eq!(encoding_of(&mut db, b"l"), "listpack");
    // A stored integer 7 answers only to "7".
    assert_eq!(
        lrem(&mut db, &[bs(b"l"), bs(b"0"), bs(b"07")]),
        Frame::Integer(0)
    );
    assert_eq!(
        lrem(&mut db, &[bs(b"l"), bs(b"0"), bs(b"7")]),
        Frame::Integer(1)
    );
    assert_ledger_exact(&mut db, "byte transparency");
}

/// Refusals are unchanged and create nothing: WRONGTYPE on either LMOVE key
/// before anything is popped, missing keys answered without fabricating one.
#[test]
fn refusals_create_nothing_and_pop_nothing() {
    let mut db = Database::new();
    crate::command::string::set(&mut db, &[bs(b"str"), bs(b"v")]);
    setup(&mut db, b"l", &[b"a", b"b"]);
    let floor = db.estimated_memory();
    for got in [
        ltrim(&mut db, &[bs(b"str"), bs(b"0"), bs(b"1")]),
        lrem(&mut db, &[bs(b"str"), bs(b"0"), bs(b"a")]),
        linsert(&mut db, &[bs(b"str"), bs(b"BEFORE"), bs(b"a"), bs(b"x")]),
        lmove(&mut db, &[bs(b"str"), bs(b"l"), bs(b"LEFT"), bs(b"LEFT")]),
        lmove(&mut db, &[bs(b"l"), bs(b"str"), bs(b"LEFT"), bs(b"LEFT")]),
        rpoplpush(&mut db, &[bs(b"l"), bs(b"str")]),
        lmpop(&mut db, &[bs(b"2"), bs(b"str"), bs(b"l"), bs(b"LEFT")]),
    ] {
        assert!(
            matches!(&got, Frame::Error(e) if e.starts_with(b"WRONGTYPE")),
            "expected WRONGTYPE, got {got:?}"
        );
    }
    assert_eq!(
        elements(&db, b"l"),
        owned(&[b"a", b"b"]),
        "nothing was popped"
    );
    assert_eq!(encoding_of(&mut db, b"l"), "listpack");
    assert_eq!(db.estimated_memory(), floor);

    // Missing keys: LTRIM OK, LREM 0, LINSERT 0, LMOVE nil -- and no key.
    assert_eq!(ltrim(&mut db, &[bs(b"nope"), bs(b"0"), bs(b"1")]), ok());
    assert_eq!(
        lrem(&mut db, &[bs(b"nope"), bs(b"0"), bs(b"a")]),
        Frame::Integer(0)
    );
    assert_eq!(
        linsert(&mut db, &[bs(b"nope"), bs(b"BEFORE"), bs(b"a"), bs(b"x")]),
        Frame::Integer(0)
    );
    assert_eq!(
        lmove(
            &mut db,
            &[bs(b"nope"), bs(b"str"), bs(b"LEFT"), bs(b"LEFT")]
        ),
        Frame::Null,
        "a missing source answers nil before the destination is type-checked"
    );
    assert_eq!(encoding_of(&mut db, b"nope"), "<missing>");
    assert_eq!(db.estimated_memory(), floor);
    assert_ledger_exact(&mut db, "refusals");
}

/// moon#1174 §2: LRANGE on a listpack is ONE seek, whatever the window --
/// HEAD walked from the head once PER returned element (`LRANGE 0 -1` on 128
/// entries decoded 8,256). LINDEX is one seek too.
#[test]
fn lrange_and_lindex_on_a_listpack_seek_once() {
    use crate::storage::listpack::head_seeks;
    let values: Vec<Vec<u8>> = (0..128)
        .map(|i| format!("element:{i:05}").into_bytes())
        .collect();
    let mut db = Database::new();
    let mut args = vec![bs(b"l")];
    args.extend(values.iter().map(|v| bs(v)));
    rpush(&mut db, &args);
    assert_eq!(encoding_of(&mut db, b"l"), "listpack", "fixture");
    let len = values.len() as i64;
    for (start, stop) in [
        (0i64, -1i64),
        (10, 20),
        (-5, -1),
        (100, 127),
        (0, 0),
        (-1, -1),
        (64, 64),
        (130, 140),
        (5, 2),
        (-500, 500),
    ] {
        let mark = head_seeks();
        let got = lrange_readonly(
            &db,
            &[
                bs(b"l"),
                bs(start.to_string().as_bytes()),
                bs(stop.to_string().as_bytes()),
            ],
            0,
        );
        let seeks = head_seeks() - mark;
        assert!(seeks <= 1, "LRANGE {start} {stop} took {seeks} seeks");
        let s = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };
        let e = if stop < 0 {
            len + stop
        } else {
            stop.min(len - 1)
        };
        let want: Vec<Frame> = if s > e || s >= len {
            Vec::new()
        } else {
            values[s as usize..=e as usize]
                .iter()
                .map(|v| bs(v))
                .collect()
        };
        assert_eq!(got, Frame::Array(want.into()), "LRANGE {start} {stop}");
    }
    let head = head_seeks();
    assert_eq!(
        super::lindex_readonly(&db, &[bs(b"l"), bs(b"-1")], 0),
        bs(values.last().unwrap())
    );
    assert_eq!(head_seeks() - head, 1, "LINDEX is one seek");
}
