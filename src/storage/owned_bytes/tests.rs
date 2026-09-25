//! moon#1160: no stored collection element may share an allocation with the
//! request that wrote it.
//!
//! Each command is encoded as RESP into its own 4 KiB `BytesMut` — the shape
//! of a connection read buffer — and parsed by the REAL parser, so every
//! argument is a `Bytes` slice of that buffer (asserted, or the test would be
//! vacuous). After the command runs through the real dispatch, every `Bytes`
//! the value stores is checked against the address range of EVERY buffer the
//! test has used so far (all kept alive, so an address cannot be recycled
//! into a false positive). Pre-fix, `.clone()` of the argument stored the
//! slice itself: the pointer lands inside the buffer and the check fails.

use std::ops::Range;

use bytes::{Bytes, BytesMut};

use crate::protocol::{Frame, ParseConfig, parse};
use crate::storage::Database;
use crate::storage::compact_value::RedisValueRef;

use super::detach;

/// Every wire buffer used so far, kept alive with its parsed frame.
#[derive(Default)]
struct Wire {
    held: Vec<(Frame, BytesMut)>,
    ranges: Vec<Range<usize>>,
}

fn encode(parts: &[&[u8]], out: &mut BytesMut) {
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
}

impl Wire {
    fn run(&mut self, db: &mut Database, parts: &[&[u8]]) -> Frame {
        let mut buf = BytesMut::with_capacity(4096);
        encode(parts, &mut buf);
        let base = buf.as_ptr() as usize;
        let range = base..base + buf.capacity();
        let frame = parse::parse(&mut buf, &ParseConfig::default())
            .expect("well-formed RESP")
            .expect("one complete frame");
        let Frame::Array(items) = &frame else {
            panic!("a command parses to an array");
        };
        for item in items.iter() {
            let Frame::BulkString(b) = item else {
                panic!("every argument is a bulk string");
            };
            assert!(
                range.contains(&(b.as_ptr() as usize)),
                "precondition: the parser hands arguments out as slices of the read buffer"
            );
        }
        let name = match &items[0] {
            Frame::BulkString(b) => b.clone(),
            _ => unreachable!(),
        };
        let mut selected = 0usize;
        let reply = match crate::command::dispatch(db, &name, &items[1..], &mut selected, 16) {
            crate::command::DispatchResult::Response(f)
            | crate::command::DispatchResult::Quit(f) => f,
        };
        self.held.push((frame, buf));
        self.ranges.push(range);
        reply
    }

    /// Assert that no `Bytes` stored under `key` points into any wire buffer.
    fn assert_detached(&self, db: &mut Database, key: &str) {
        let mut stored: Vec<(&'static str, usize)> = Vec::new();
        let entry = db.peek(key.as_bytes()).expect("the key exists");
        let mut note = |what: &'static str, b: &Bytes| stored.push((what, b.as_ptr() as usize));
        match entry.as_redis_value() {
            RedisValueRef::Hash(m) => m.iter().for_each(|(k, v)| {
                note("hash field", k);
                note("hash value", v);
            }),
            RedisValueRef::HashWithTtl { fields, ttls, .. } => {
                fields.iter().for_each(|(k, v)| {
                    note("hash field", k);
                    note("hash value", v);
                });
                ttls.keys().for_each(|k| note("hash ttl field", k));
            }
            RedisValueRef::List(l) => l.iter().for_each(|e| note("list element", e)),
            RedisValueRef::Set(s) => s.iter().for_each(|m| note("set member", m)),
            RedisValueRef::SortedSetBPTree { tree, members } => {
                members.keys().for_each(|m| note("zset map member", m));
                tree.iter().for_each(|(_, m)| note("zset tree member", m));
            }
            RedisValueRef::Stream(s) => {
                for fields in s.entries.values() {
                    for (f, v) in fields {
                        note("stream field", f);
                        note("stream value", v);
                    }
                }
                for (gname, g) in &s.groups {
                    note("stream group name", gname);
                    for (cname, c) in &g.consumers {
                        note("stream consumer key", cname);
                        note("stream consumer name", &c.name);
                    }
                    for pe in g.pel.values() {
                        note("stream PEL consumer", &pe.consumer);
                    }
                }
            }
            other => panic!(
                "{key}: the test must reach a FULL encoding (compact ones copy), got {}",
                other.encoding_name()
            ),
        }
        assert!(!stored.is_empty(), "{key}: nothing stored to check");
        for (what, ptr) in stored {
            assert!(
                !self.ranges.iter().any(|r| r.contains(&ptr)),
                "{key}: a stored {what} is a slice of a request buffer — it keeps the whole \
                 read buffer alive (moon#1160)"
            );
        }
    }
}

/// 80 bytes: past every listpack value limit (64 B), so each write lands on
/// the full encoding — the one that used to store the request's slice.
fn wide(tag: &str) -> Vec<u8> {
    let mut v = tag.as_bytes().to_vec();
    v.resize(80, b'.');
    v
}

#[test]
fn detach_never_shares_its_input_allocation() {
    let mut buf = BytesMut::with_capacity(64);
    buf.extend_from_slice(b"0123456789abcdef");
    let frozen = buf.freeze();
    let slice = frozen.slice(4..12);
    let owned = detach(&slice);
    assert_eq!(owned, slice);
    let r = frozen.as_ptr() as usize..frozen.as_ptr() as usize + frozen.len();
    assert!(!r.contains(&(owned.as_ptr() as usize)));
    assert!(detach(b"").is_empty());
}

#[test]
fn hash_writes_store_owned_fields_and_values() {
    let mut db = Database::new();
    let mut w = Wire::default();
    let (f1, v1, f2, v2, f3, f4, f5) = (
        wide("f1"),
        wide("v1"),
        wide("f2"),
        wide("v2"),
        wide("f3"),
        wide("f4"),
        wide("f5"),
    );
    w.run(&mut db, &[b"HSET", b"h", &f1, &v1]);
    w.run(&mut db, &[b"HMSET", b"h", &f2, &v2]);
    w.run(&mut db, &[b"HSET", b"h", &f1, &v2]); // update: value replaced
    w.run(&mut db, &[b"HSETNX", b"h", &f3, &v1]);
    w.run(&mut db, &[b"HINCRBY", b"h", &f4, b"7"]);
    w.run(&mut db, &[b"HINCRBY", b"h", &f4, b"1"]); // update in place
    w.run(&mut db, &[b"HINCRBYFLOAT", b"h", &f5, b"1.5"]);
    assert_eq!(w.run(&mut db, &[b"HLEN", b"h"]), Frame::Integer(5));
    w.assert_detached(&mut db, "h");
    // A per-field TTL promotes to `HashWithTtl`; the sidecar copies too.
    w.run(&mut db, &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"1", &f2]);
    w.assert_detached(&mut db, "h");
}

#[test]
fn set_writes_store_owned_members() {
    let mut db = Database::new();
    let mut w = Wire::default();
    let (m1, m2, m3) = (wide("m1"), wide("m2"), wide("m3"));
    w.run(&mut db, &[b"SADD", b"s", &m1, &m2]);
    w.run(&mut db, &[b"SADD", b"s", &m2, &m3]); // m2 already there
    w.assert_detached(&mut db, "s");
    // Crossing from an intset: the members after the crossing point.
    w.run(&mut db, &[b"SADD", b"is", b"1", b"2", &m1]);
    w.assert_detached(&mut db, "is");
    // SMOVE moves the source's stored member, never the request's.
    assert_eq!(
        w.run(&mut db, &[b"SMOVE", b"s", b"dst", &m3]),
        Frame::Integer(1)
    );
    w.assert_detached(&mut db, "dst");
}

#[test]
fn zset_writes_store_owned_members_in_both_structures() {
    let mut db = Database::new();
    let mut w = Wire::default();
    let (m1, m2) = (wide("m1"), wide("m2"));
    w.run(&mut db, &[b"ZADD", b"z", b"1", &m1]);
    w.run(&mut db, &[b"ZADD", b"z", b"5", &m1]); // rescore: tree re-insert
    w.run(&mut db, &[b"ZINCRBY", b"z", b"2", &m2]);
    w.run(&mut db, &[b"ZINCRBY", b"z", b"2", &m2]); // rescore
    w.assert_detached(&mut db, "z");
}

#[test]
fn list_writes_store_owned_elements() {
    let mut db = Database::new();
    let mut w = Wire::default();
    let (e1, e2, e3, e4, e5, e6) = (
        wide("e1"),
        wide("e2"),
        wide("e3"),
        wide("e4"),
        wide("e5"),
        wide("e6"),
    );
    w.run(&mut db, &[b"RPUSH", b"l", &e1]);
    w.run(&mut db, &[b"LPUSH", b"l", &e2]);
    w.run(&mut db, &[b"RPUSHX", b"l", &e3]);
    w.run(&mut db, &[b"LPUSHX", b"l", &e4]);
    w.run(&mut db, &[b"LINSERT", b"l", b"BEFORE", &e1, &e5]);
    w.run(&mut db, &[b"LSET", b"l", b"0", &e6]);
    assert_eq!(w.run(&mut db, &[b"LLEN", b"l"]), Frame::Integer(5));
    w.assert_detached(&mut db, "l");
}

#[test]
fn stream_writes_store_owned_fields_groups_consumers_and_pel() {
    let mut db = Database::new();
    let mut w = Wire::default();
    let (f, v, g, c1, c2) = (wide("f"), wide("v"), wide("g"), wide("c1"), wide("c2"));
    w.run(&mut db, &[b"XADD", b"st", b"1-1", &f, &v]);
    w.run(&mut db, &[b"XADD", b"st", b"1-2", &f, &v]);
    w.run(&mut db, &[b"XGROUP", b"CREATE", b"st", &g, b"0"]);
    w.run(&mut db, &[b"XGROUP", b"CREATECONSUMER", b"st", &g, &c1]);
    let read = w.run(
        &mut db,
        &[
            b"XREADGROUP",
            b"GROUP",
            &g,
            &c2,
            b"COUNT",
            b"1",
            b"STREAMS",
            b"st",
            b">",
        ],
    );
    assert!(
        matches!(read, Frame::Array(_)),
        "XREADGROUP answered {read:?}"
    );
    w.run(&mut db, &[b"XCLAIM", b"st", &g, &c1, b"0", b"1-1"]);
    w.run(
        &mut db,
        &[b"XCLAIM", b"st", &g, &c2, b"0", b"1-2", b"FORCE", b"JUSTID"],
    );
    w.run(&mut db, &[b"XAUTOCLAIM", b"st", &g, &c2, b"0", b"0-0"]);
    w.assert_detached(&mut db, "st");
}
