//! moon#1217 — a multi-key write during a snapshot epoch must leave EVERY
//! key it writes at its epoch-start state in the file, not only
//! `command[1]`.
//!
//! Each family runs one command against keys whose ranges are all still
//! pending (nothing is written before it), so its non-first keys — the
//! destination, `k2..kN`, the popped non-first key — reach the file only
//! through their own pre-image. The file must then equal the epoch-start
//! keyspace exactly, values of every type included.

use std::collections::BTreeMap;

use bytes::Bytes;

use super::epoch_harness::{Epoch, Record, run};
use super::*;
use crate::protocol::Frame;

/// A type-independent rendering of one key's value, read back through the
/// command layer (`TYPE`, then the type's full read), with unordered
/// containers sorted — so two databases compare equal exactly when every
/// key holds the same value.
fn canonical(dbs: &mut [Database]) -> BTreeMap<(usize, Vec<u8>), String> {
    let mut out = BTreeMap::new();
    for db in 0..dbs.len() {
        let keys: Vec<Vec<u8>> = dbs[db]
            .data()
            .iter()
            .map(|(k, _)| k.as_bytes().to_vec())
            .collect();
        for key in keys {
            let ty = match run(dbs, db, &[b"TYPE", &key]) {
                Frame::SimpleString(t) => String::from_utf8_lossy(&t).into_owned(),
                other => panic!("TYPE answered {other:?}"),
            };
            let read: Vec<&[u8]> = match ty.as_str() {
                "string" => vec![b"GET", &key],
                "list" => vec![b"LRANGE", &key, b"0", b"-1"],
                "set" => vec![b"SMEMBERS", &key],
                "zset" => vec![b"ZRANGE", &key, b"0", b"-1", b"WITHSCORES"],
                "hash" => vec![b"HGETALL", &key],
                other => panic!("fixture uses no {other} values"),
            };
            let mut items = flatten(run(dbs, db, &read));
            if matches!(ty.as_str(), "set") {
                items.sort();
            }
            if ty == "hash" {
                let mut pairs: Vec<String> = items.chunks(2).map(|c| c.join("=")).collect();
                pairs.sort();
                items = pairs;
            }
            out.insert((db, key), format!("{ty}:{}", items.join(",")));
        }
    }
    out
}

fn flatten(f: Frame) -> Vec<String> {
    match f {
        Frame::BulkString(b) | Frame::SimpleString(b) => {
            vec![String::from_utf8_lossy(&b).into_owned()]
        }
        Frame::Integer(n) => vec![n.to_string()],
        Frame::Double(d) => vec![d.to_string()],
        Frame::Array(items) | Frame::Set(items) => items.into_iter().flat_map(flatten).collect(),
        Frame::Map(pairs) => pairs
            .into_iter()
            .flat_map(|(k, v)| flatten(k).into_iter().chain(flatten(v)))
            .collect(),
        other => panic!("unexpected read reply {other:?}"),
    }
}

/// Load raw records into fresh databases, refusing a key written twice.
fn load(n: usize, records: Vec<Record>) -> Vec<Database> {
    let mut seen = std::collections::HashSet::new();
    let mut dbs: Vec<Database> = (0..n).map(|_| Database::new()).collect();
    for (db, key, entry) in records {
        assert!(
            seen.insert((db, key.clone())),
            "key {key:?} written twice in db {db}"
        );
        dbs[db].set(&key, entry);
    }
    dbs
}

/// Run `setup` (the epoch-start keyspace, over ~1,500 filler keys so the
/// table has many segments), begin an epoch, run `cmd` through dispatch,
/// finish, and diff the file against the epoch-start keyspace.
fn family(name: &str, setup: &[&[&[u8]]], cmd: &[&[u8]]) -> Option<String> {
    let mut dbs = vec![Database::new()];
    for i in 0..1500u32 {
        dbs[0].set_string(format!("f:{i:05}").as_bytes(), Bytes::from_static(b"."));
    }
    for argv in setup {
        let reply = run(&mut dbs, 0, argv);
        assert!(
            !matches!(reply, Frame::Error(_)),
            "{name}: setup {argv:?} failed: {reply:?}"
        );
    }
    let expected = canonical(&mut dbs);
    let epoch = Epoch::begin(&dbs);
    let reply = run(&mut dbs, 0, cmd);
    assert!(
        !matches!(reply, Frame::Error(_)),
        "{name}: {reply:?} — the command must run for the test to mean anything"
    );
    let changed = canonical(&mut dbs) != expected;
    assert!(changed, "{name}: the command changed nothing");
    let mut loaded = load(1, epoch.finish(&dbs));
    let got = canonical(&mut loaded);
    if got == expected {
        return None;
    }
    let mut diff = Vec::new();
    for (k, v) in &expected {
        match got.get(k) {
            None => diff.push(format!("missing {}", String::from_utf8_lossy(&k.1))),
            Some(g) if g != v => diff.push(format!(
                "{}: file {g} / epoch-start {v}",
                String::from_utf8_lossy(&k.1)
            )),
            Some(_) => {}
        }
    }
    for k in got.keys().filter(|k| !expected.contains_key(*k)) {
        diff.push(format!("extra {}", String::from_utf8_lossy(&k.1)));
    }
    Some(format!("{name}: {}", diff.join("; ")))
}

#[test]
fn every_written_key_of_a_multi_key_write_keeps_its_epoch_start_state() {
    let cases: Vec<(&str, Vec<&[&[u8]]>, &[&[u8]])> = vec![
        (
            "LMOVE dst",
            vec![
                &[b"RPUSH", b"l:src", b"a", b"b", b"c"],
                &[b"RPUSH", b"l:dst", b"x", b"y"],
            ],
            &[b"LMOVE", b"l:src", b"l:dst", b"LEFT", b"RIGHT"],
        ),
        (
            "RPOPLPUSH dst",
            vec![
                &[b"RPUSH", b"l:src", b"a", b"b"],
                &[b"RPUSH", b"l:dst", b"x"],
            ],
            &[b"RPOPLPUSH", b"l:src", b"l:dst"],
        ),
        (
            "SMOVE dst",
            vec![
                &[b"SADD", b"s:src", b"m1", b"m2"],
                &[b"SADD", b"s:dst", b"z"],
            ],
            &[b"SMOVE", b"s:src", b"s:dst", b"m1"],
        ),
        (
            "MSET k2..kN (existing and new)",
            vec![
                &[b"SET", b"k1", b"v1"],
                &[b"SET", b"k2", b"v2"],
                &[b"SET", b"k3", b"v3"],
            ],
            &[b"MSET", b"k1", b"N", b"k2", b"N", b"k3", b"N", b"k4", b"N"],
        ),
        (
            "MSETNX all new",
            vec![&[b"SET", b"other", b"o"]],
            &[b"MSETNX", b"n1", b"N", b"n2", b"N", b"n3", b"N"],
        ),
        (
            "RENAME onto an existing dst",
            vec![&[b"SET", b"r:src", b"S"], &[b"SET", b"r:dst", b"D"]],
            &[b"RENAME", b"r:src", b"r:dst"],
        ),
        (
            "RENAMENX to a new dst",
            vec![&[b"SET", b"r:src", b"S"]],
            &[b"RENAMENX", b"r:src", b"r:new"],
        ),
        (
            "COPY REPLACE dst",
            vec![&[b"SET", b"c:src", b"S"], &[b"RPUSH", b"c:dst", b"old"]],
            &[b"COPY", b"c:src", b"c:dst", b"REPLACE"],
        ),
        (
            "SUNIONSTORE dst",
            vec![
                &[b"SADD", b"s:a", b"1", b"2"],
                &[b"SADD", b"s:b", b"3"],
                &[b"SET", b"s:dst", b"was-a-string"],
            ],
            &[b"SUNIONSTORE", b"s:dst", b"s:a", b"s:b"],
        ),
        (
            "SINTERSTORE dst",
            vec![
                &[b"SADD", b"s:a", b"1", b"2"],
                &[b"SADD", b"s:b", b"2", b"3"],
                &[b"SADD", b"s:dst", b"old"],
            ],
            &[b"SINTERSTORE", b"s:dst", b"s:a", b"s:b"],
        ),
        (
            "SDIFFSTORE dst",
            vec![
                &[b"SADD", b"s:a", b"1", b"2"],
                &[b"SADD", b"s:b", b"2"],
                &[b"SADD", b"s:dst", b"old"],
            ],
            &[b"SDIFFSTORE", b"s:dst", b"s:a", b"s:b"],
        ),
        (
            "ZUNIONSTORE dst",
            vec![
                &[b"ZADD", b"z:a", b"1", b"a"],
                &[b"ZADD", b"z:b", b"2", b"b"],
                &[b"ZADD", b"z:dst", b"9", b"old"],
            ],
            &[b"ZUNIONSTORE", b"z:dst", b"2", b"z:a", b"z:b"],
        ),
        (
            "ZINTERSTORE dst",
            vec![
                &[b"ZADD", b"z:a", b"1", b"a", b"2", b"b"],
                &[b"ZADD", b"z:b", b"2", b"b"],
                &[b"ZADD", b"z:dst", b"9", b"old"],
            ],
            &[b"ZINTERSTORE", b"z:dst", b"2", b"z:a", b"z:b"],
        ),
        (
            "ZDIFFSTORE dst",
            vec![
                &[b"ZADD", b"z:a", b"1", b"a", b"2", b"b"],
                &[b"ZADD", b"z:b", b"2", b"b"],
            ],
            &[b"ZDIFFSTORE", b"z:dst", b"2", b"z:a", b"z:b"],
        ),
        (
            "ZRANGESTORE dst",
            vec![
                &[b"ZADD", b"z:a", b"1", b"a", b"2", b"b"],
                &[b"ZADD", b"z:dst", b"9", b"old"],
            ],
            &[b"ZRANGESTORE", b"z:dst", b"z:a", b"0", b"-1"],
        ),
        (
            "GEOSEARCHSTORE dst",
            vec![
                &[
                    b"GEOADD",
                    b"g:src",
                    b"13.361389",
                    b"38.115556",
                    b"Palermo",
                    b"15.087269",
                    b"37.502669",
                    b"Catania",
                ],
                &[b"SET", b"g:dst", b"old"],
            ],
            &[
                b"GEOSEARCHSTORE",
                b"g:dst",
                b"g:src",
                b"FROMLONLAT",
                b"15",
                b"37",
                b"BYRADIUS",
                b"200",
                b"km",
            ],
        ),
        (
            "BITOP dst",
            vec![
                &[b"SET", b"b:1", b"ab"],
                &[b"SET", b"b:2", b"cd"],
                &[b"SET", b"b:dst", b"old"],
            ],
            &[b"BITOP", b"AND", b"b:dst", b"b:1", b"b:2"],
        ),
        (
            "SORT ... STORE dst",
            vec![
                &[b"RPUSH", b"so:src", b"3", b"1", b"2"],
                &[b"RPUSH", b"so:dst", b"old"],
            ],
            &[b"SORT", b"so:src", b"STORE", b"so:dst"],
        ),
        (
            "LMPOP pops the non-first key",
            vec![&[b"RPUSH", b"lm:b", b"x", b"y"]],
            &[b"LMPOP", b"2", b"lm:a", b"lm:b", b"LEFT"],
        ),
        (
            "ZMPOP pops the non-first key",
            vec![&[b"ZADD", b"zm:b", b"1", b"x", b"2", b"y"]],
            &[b"ZMPOP", b"2", b"zm:a", b"zm:b", b"MIN"],
        ),
        (
            "DEL k2..kN",
            vec![
                &[b"SET", b"d1", b"1"],
                &[b"SET", b"d2", b"2"],
                &[b"RPUSH", b"d3", b"3"],
            ],
            &[b"DEL", b"d1", b"d2", b"d3"],
        ),
        (
            "UNLINK k2..kN",
            vec![
                &[b"SET", b"u1", b"1"],
                &[b"SADD", b"u2", b"2"],
                &[b"SET", b"u3", b"3"],
            ],
            &[b"UNLINK", b"u1", b"u2", b"u3"],
        ),
        (
            "PFMERGE dst",
            vec![
                &[b"PFADD", b"hll:a", b"x", b"y"],
                &[b"PFADD", b"hll:b", b"z"],
                &[b"PFADD", b"hll:dst", b"old"],
            ],
            &[b"PFMERGE", b"hll:dst", b"hll:a", b"hll:b"],
        ),
    ];
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|(name, setup, cmd)| family(name, setup, cmd))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} families are not point-in-time:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// The routed arm (`spsc_handler::cow_intercept`) and a script's
/// `redis.call` (`capture_command_pre_image`) capture every written key too
/// — same walker, same queue.
#[test]
fn routed_and_script_captures_cover_every_written_key() {
    use crate::persistence::snapshot_cow;
    let mut db = Database::new();
    for k in [&b"a"[..], b"b", b"c"] {
        db.set_string(k, Bytes::from_static(b"old"));
    }
    let frames = |parts: &[&[u8]]| -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
            .collect()
    };
    let keys_of = |pending: Vec<(usize, Bytes, crate::storage::entry::Entry)>,
                   tombs: Vec<(usize, Bytes)>| {
        let mut ks: Vec<Bytes> = pending
            .into_iter()
            .map(|(_, k, _)| k)
            .chain(tombs.into_iter().map(|(_, k)| k))
            .collect();
        ks.sort();
        ks
    };
    let want: Vec<Bytes> = [&b"a"[..], b"b", b"c", b"d"]
        .iter()
        .map(|k| Bytes::copy_from_slice(k))
        .collect();

    // Routed arm.
    let dir = tempfile::tempdir().unwrap();
    let mut snap = Some(SnapshotState::new(
        0,
        1,
        std::slice::from_ref(&db),
        dir.path().join("x.rrdshard"),
    ));
    snapshot_cow::disarm();
    snapshot_cow::arm();
    let cmd = Frame::Array(crate::protocol::FrameVec::from_vec(frames(&[
        b"MSET", b"a", b"1", b"b", b"1", b"c", b"1", b"d", b"1",
    ])));
    crate::shard::spsc_handler::cow_intercept(&mut snap, &db, 0, &cmd);
    let got = keys_of(
        snapshot_cow::pending_for_test(),
        snapshot_cow::pending_tombstones_for_test(),
    );
    snapshot_cow::disarm();
    assert_eq!(got, want, "cow_intercept");

    // Script `redis.call`.
    snapshot_cow::arm();
    let call = frames(&[b"DEL", b"a", b"b", b"c", b"d"]);
    snapshot_cow::capture_command_pre_image(&db, 0, &call);
    let got = keys_of(
        snapshot_cow::pending_for_test(),
        snapshot_cow::pending_tombstones_for_test(),
    );
    snapshot_cow::disarm();
    assert_eq!(got, want, "script redis.call");
}

/// A BLMOVE parked on an empty source is served by the WAKER when a later
/// write fills the source: the waker pops the source and pushes the
/// destination itself (and logs the move at that moment). The destination
/// is a key that move writes, so its epoch-start state must reach the file
/// too — otherwise the logged move replays on top of a destination that
/// already holds its element.
#[test]
fn a_woken_blmove_keeps_its_destination_point_in_time() {
    use crate::blocking::{BlockedCommand, BlockingRegistry, Direction, WaitEntry};
    let mut dbs = vec![Database::new()];
    for i in 0..1500u32 {
        dbs[0].set_string(format!("f:{i:05}").as_bytes(), Bytes::from_static(b"."));
    }
    run(&mut dbs, 0, &[b"RPUSH", b"bm:dst", b"x"]);
    let expected = canonical(&mut dbs);

    let src = Bytes::from_static(b"bm:src");
    let dst = Bytes::from_static(b"bm:dst");
    let mut reg = BlockingRegistry::new(0);
    let wait_id = reg.next_wait_id();
    let (tx, rx) = crate::runtime::channel::oneshot();
    reg.register(
        0,
        src.clone(),
        WaitEntry {
            wait_id,
            cmd: BlockedCommand::BLMove {
                destination: dst.clone(),
                wherefrom: Direction::Left,
                whereto: Direction::Right,
            },
            reply_tx: tx,
            deadline: None,
            claim: None,
        },
    );

    let epoch = Epoch::begin(&dbs);
    run(&mut dbs, 0, &[b"LPUSH", b"bm:src", b"v"]);
    assert!(crate::blocking::wakeup::try_wake_list_waiter(
        &mut reg,
        &mut dbs[0],
        0,
        &src
    ));
    assert!(
        matches!(rx.try_recv(), Ok(Some(Frame::BulkString(_)))),
        "setup: the waiter must be served"
    );
    assert_ne!(
        canonical(&mut dbs),
        expected,
        "setup: the wake moved nothing"
    );
    let mut loaded = load(1, epoch.finish(&dbs));
    let got = canonical(&mut loaded);
    let moved = |m: &BTreeMap<(usize, Vec<u8>), String>| {
        m.iter()
            .filter(|((_, k), _)| k.starts_with(b"bm:"))
            .map(|((_, k), v)| (String::from_utf8_lossy(k).into_owned(), v.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        moved(&got),
        moved(&expected),
        "the woken move's destination must keep its epoch-start state"
    );
    assert_eq!(got, expected);
}
