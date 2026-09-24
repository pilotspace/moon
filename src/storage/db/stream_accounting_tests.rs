//! moon#1163: streams are charged to `used_memory`, and a DEL credits back
//! exactly what was charged.
//!
//! Pre-fix no stream write charged anything (200K XADDs moved `used_memory`
//! by 0 B), while a DEL credited the stream's full scanned estimate — which
//! saturated at 0 and under-counted every OTHER key from then on (the
//! moon#861 class).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::{Entry, RedisValue};
use crate::storage::stream::{Stream, StreamId};

fn run(db: &mut Database, parts: &[&str]) -> Frame {
    let args: Vec<Frame> = parts[1..]
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
        .collect();
    let mut selected = 0usize;
    match crate::command::dispatch(db, parts[0].as_bytes(), &args, &mut selected, 16) {
        crate::command::DispatchResult::Response(f) | crate::command::DispatchResult::Quit(f) => f,
    }
}

/// `(billed, measured)` for the stream at `key`.
fn sizes(db: &mut Database, key: &str) -> (usize, usize) {
    let entry = db.peek(key.as_bytes()).expect("stream exists");
    match entry.as_redis_value() {
        RedisValueRef::Stream(s) => (s.billed_memory(), s.estimate_memory()),
        _ => panic!("{key} is not a stream"),
    }
}

/// The two identities the fix keeps: the stream's O(1) billed size equals
/// its measured size (every delta was exact), and the running ledger equals
/// a from-scratch recompute (every delta reached `used_memory`).
fn assert_exact(db: &mut Database, key: &str, step: &str) {
    let (billed, measured) = sizes(db, key);
    assert_eq!(
        billed, measured,
        "{step}: billed {billed} B != measured {measured} B — a stream mutation's delta is wrong"
    );
    let running = db.estimated_memory();
    db.recalculate_memory();
    assert_eq!(
        running,
        db.estimated_memory(),
        "{step}: the running ledger drifted from a recompute"
    );
}

#[test]
fn xadd_charges_its_entries_and_del_returns_to_the_baseline() {
    let mut db = Database::new();
    // Another key, so an over-credit would show as a drop BELOW the baseline
    // rather than hiding in a saturation at zero.
    assert_eq!(
        run(&mut db, &["SET", "other", "some-string-value-past-sso"]),
        Frame::SimpleString(Bytes::from_static(b"OK"))
    );
    let base = db.estimated_memory();
    let value = "0123456789abcdef0123456789abcdef";
    for _ in 0..1000 {
        assert!(matches!(
            run(&mut db, &["XADD", "st", "*", "f", value]),
            Frame::BulkString(_)
        ));
    }
    let grown = db.estimated_memory() - base;
    // Each entry really holds a B-tree slot, a field vector and two copies:
    // well over 100 B (redis 7.0.15 measured ~42 B/entry in listpack nodes).
    assert!(
        grown >= 1000 * 100,
        "1000 XADDs grew used_memory by only {grown} B (moon#1163)"
    );
    assert_eq!(run(&mut db, &["DEL", "st"]), Frame::Integer(1));
    assert_eq!(
        db.estimated_memory(),
        base,
        "DEL must return used_memory to the baseline exactly — no under-count of \
         the other keys (moon#1163)"
    );
}

#[test]
fn every_stream_write_keeps_billed_equal_to_measured() {
    let mut db = Database::new();
    let base = db.estimated_memory();
    let steps: &[&[&str]] = &[
        &["XADD", "st", "1-1", "f1", "v1", "f2", "v2"],
        &[
            "XADD",
            "st",
            "1-2",
            "field",
            "a-longer-value-than-the-others",
        ],
        &["XADD", "st", "1-3", "f", "v"],
        &["XADD", "st", "1-4", "f", "v"],
        &["XADD", "st", "1-5", "f", "v"],
        &["XGROUP", "CREATE", "st", "g1", "0"],
        &["XGROUP", "CREATE", "st", "g2", "$"],
        &["XGROUP", "CREATECONSUMER", "st", "g1", "alice"],
        &[
            "XREADGROUP",
            "GROUP",
            "g1",
            "bob",
            "COUNT",
            "3",
            "STREAMS",
            "st",
            ">",
        ],
        &[
            "XREADGROUP",
            "GROUP",
            "g1",
            "carol",
            "NOACK",
            "STREAMS",
            "st",
            ">",
        ],
        &["XREADGROUP", "GROUP", "g1", "bob", "STREAMS", "st", "0"],
        &["XACK", "st", "g1", "1-1"],
        &["XCLAIM", "st", "g1", "alice", "0", "1-2"],
        &["XCLAIM", "st", "g1", "dave", "0", "1-5", "FORCE", "JUSTID"],
        &["XDEL", "st", "1-3"],
        &["XAUTOCLAIM", "st", "g1", "erin", "0", "0-0"],
        &["XGROUP", "SETID", "st", "g1", "0"],
        &["XREADGROUP", "GROUP", "g1", "bob", "STREAMS", "st", ">"],
        &["XTRIM", "st", "MAXLEN", "3"],
        &["XTRIM", "st", "MINID", "1-5"],
        &["XGROUP", "DELCONSUMER", "st", "g1", "bob"],
        &["XGROUP", "DESTROY", "st", "g2"],
        &["XSETID", "st", "9-9"],
        &["XADD", "st", "MAXLEN", "2", "*", "f", "v"],
        &["XGROUP", "DESTROY", "st", "g1"],
    ];
    for step in steps {
        let reply = run(&mut db, step);
        assert!(
            !matches!(reply, Frame::Error(_)),
            "{step:?} answered {reply:?}"
        );
        assert_exact(&mut db, "st", &format!("{step:?}"));
    }
    // MKSTREAM creates the key AND a group in one command.
    run(&mut db, &["XGROUP", "CREATE", "mk", "g", "$", "MKSTREAM"]);
    assert_exact(&mut db, "mk", "XGROUP CREATE MKSTREAM");
    run(&mut db, &["DEL", "st", "mk"]);
    assert_eq!(db.estimated_memory(), base);
}

/// A mutation made outside the stream commands (the MQ paths, a transaction
/// intent, the blocking-read wake) is not billed where it happens. It must
/// never be credited without having been charged, and the next stream
/// command must bring the ledger level again.
#[test]
fn an_unbilled_mutation_is_caught_up_and_never_over_credited() {
    let mut db = Database::new();
    run(&mut db, &["SET", "other", "some-string-value-past-sso"]);
    let base = db.estimated_memory();
    run(&mut db, &["XADD", "st", "1-1", "f", "v"]);
    {
        let s = db.get_stream_mut(b"st").unwrap().unwrap();
        for i in 2..50u64 {
            s.add(
                StreamId { ms: 1, seq: i },
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            );
        }
    }
    let running = db.estimated_memory();
    db.recalculate_memory();
    assert_eq!(
        running,
        db.estimated_memory(),
        "not billed yet, and not credited either"
    );
    // The next stream command catches up.
    run(&mut db, &["XADD", "st", "2-0", "f", "v"]);
    assert_exact(&mut db, "st", "the XADD after an unbilled mutation");
    run(&mut db, &["DEL", "st"]);
    assert_eq!(db.estimated_memory(), base);

    // Unbilled, then deleted with no stream command in between: the DEL
    // credits what was charged — not the unbilled growth.
    run(&mut db, &["XADD", "st2", "1-1", "f", "v"]);
    {
        let s = db.get_stream_mut(b"st2").unwrap().unwrap();
        s.add(
            StreamId { ms: 1, seq: 2 },
            vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
        );
    }
    run(&mut db, &["DEL", "st2"]);
    assert_eq!(db.estimated_memory(), base, "no over-credit");
}

/// A stream that enters the keyspace WHOLE (an RDB / cold-tier / RESTORE
/// load builds it field by field) is measured on the way in.
#[test]
fn a_stream_arriving_whole_is_billed_at_its_measured_size() {
    let mut s = Stream::new();
    for i in 0..100u64 {
        s.entries.insert(
            StreamId { ms: 5, seq: i },
            vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))],
        );
        s.length += 1;
    }
    let measured = s.estimate_memory();
    let mut db = Database::new();
    let base = db.estimated_memory();
    let mut entry = Entry::new_string(Bytes::new());
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
        RedisValue::Stream(Box::new(s.clone())),
    );
    db.set(b"loaded", entry);
    assert_eq!(sizes(&mut db, "loaded"), (measured, measured));
    assert_exact(&mut db, "loaded", "set of a whole stream");
    run(&mut db, &["DEL", "loaded"]);
    assert_eq!(db.estimated_memory(), base);

    // The bulk-load path bills it once, in `recalculate_memory`.
    let mut entry = Entry::new_string(Bytes::new());
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
        RedisValue::Stream(Box::new(s)),
    );
    db.insert_for_load(Bytes::from_static(b"bulk"), entry);
    db.recalculate_memory();
    assert_eq!(sizes(&mut db, "bulk"), (measured, measured));
    run(&mut db, &["DEL", "bulk"]);
    assert_eq!(db.estimated_memory(), base);
}
