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
            if ty == "stream" {
                let state = stream_state(&mut dbs[db], &key);
                out.insert((db, key), format!("stream:{state}"));
                continue;
            }
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

/// A stream's entries AND its consumer groups — last-delivered id, pending
/// entries with their owner, consumers: the state a group read writes.
fn stream_state(db: &mut Database, key: &[u8]) -> String {
    let Ok(Some(stream)) = db.get_stream(key) else {
        panic!("TYPE said stream");
    };
    let id = |id: &crate::storage::stream::StreamId| format!("{}-{}", id.ms, id.seq);
    let entries: Vec<String> = stream
        .entries
        .iter()
        .map(|(eid, fields)| format!("{}{fields:?}", id(eid)))
        .collect();
    let mut groups: Vec<String> = stream
        .groups
        .iter()
        .map(|(name, g)| {
            let pel: Vec<String> = g
                .pel
                .iter()
                .map(|(pid, p)| format!("{}@{}", id(pid), String::from_utf8_lossy(&p.consumer)))
                .collect();
            let mut consumers: Vec<String> = g
                .consumers
                .keys()
                .map(|c| String::from_utf8_lossy(c).into_owned())
                .collect();
            consumers.sort();
            format!(
                "{}:last={},pel=[{}],consumers=[{}]",
                String::from_utf8_lossy(name),
                id(&g.last_delivered_id),
                pel.join(","),
                consumers.join(",")
            )
        })
        .collect();
    groups.sort();
    format!("{}|{}", entries.join(","), groups.join(";"))
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
    family_with(name, setup, cmd, |dbs, parts| run(dbs, 0, parts))
}

/// [`family`], with `cmd` run by `exec` — for write paths that do not go
/// through `command::dispatch`.
fn family_with(
    name: &str,
    setup: &[&[&[u8]]],
    cmd: &[&[u8]],
    exec: impl Fn(&mut [Database], &[&[u8]]) -> Frame,
) -> Option<String> {
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
    let reply = exec(&mut dbs, cmd);
    assert!(
        !matches!(reply, Frame::Error(_) | Frame::Null | Frame::NullArray),
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

// ---------------------------------------------------------------------------
// moon#1227 review F2 (moon#1217 completeness): a blocking command whose data
// is ALREADY there never parks. The connection serves it on the spot —
// `handle_blocking_command{,_monoio}` -> `immediate_serve` (both runtimes),
// and `blocking_txn::try_exec_blocking_in_txn` for one queued in MULTI —
// popping, pushing a move's destination and advancing a group's cursor
// straight through `Database` methods, outside `command::dispatch`. Nothing
// captured a pre-image there, so a BGSAVE crossed by the standard BLMOVE
// reliable-queue path wrote post-epoch states (and could drop the element).
// ---------------------------------------------------------------------------

fn bulk_args(parts: &[&[u8]]) -> Vec<Frame> {
    parts
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
        .collect()
}

/// The connection-side immediate serve, exactly as both runtimes' blocking
/// handlers run it before deciding to park (`--shards 1`, db 0).
fn serve_now(dbs: &mut [Database], parts: &[&[u8]]) -> Frame {
    let args = bulk_args(&parts[1..]);
    let Ok((keys, _)) = crate::server::conn::blocking::parse_blocking_args(parts[0], &args) else {
        panic!("{parts:?}: not a valid blocking command");
    };
    let Some((frame, popped)) = crate::server::conn::blocking::immediate_serve(
        parts[0],
        &args,
        &keys,
        &mut dbs[0],
        0,
        0,
        1,
    ) else {
        panic!("{parts:?}: the data is there, so it must be served now");
    };
    assert!(popped, "{parts:?}: served nothing: {frame:?}");
    frame
}

/// Every key an immediately-served blocking command writes keeps its
/// epoch-start state in the file — both keys pending, so each non-captured
/// key shows up as a post-epoch value (for BLMOVE: `src=[b]`, `dst=[x,a]`
/// instead of `src=[a,b]`, `dst=[x]`; a WAL tail then replays the logged
/// LMOVE on top and duplicates `a`).
#[test]
fn an_immediately_served_blocking_command_keeps_every_key_it_writes_point_in_time() {
    let cases: Vec<(&str, Vec<&[&[u8]]>, &[&[u8]])> = vec![
        (
            "BLMOVE",
            vec![
                &[b"RPUSH", b"im:src", b"a", b"b"],
                &[b"RPUSH", b"im:dst", b"x"],
            ],
            &[b"BLMOVE", b"im:src", b"im:dst", b"LEFT", b"RIGHT", b"0"],
        ),
        (
            "BRPOPLPUSH",
            vec![
                &[b"RPUSH", b"im:src", b"a", b"b"],
                &[b"RPUSH", b"im:dst", b"x"],
            ],
            &[b"BRPOPLPUSH", b"im:src", b"im:dst", b"0"],
        ),
        (
            "BLPOP",
            vec![&[b"RPUSH", b"im:l", b"a", b"b"]],
            &[b"BLPOP", b"im:none", b"im:l", b"0"],
        ),
        (
            "BRPOP",
            vec![&[b"RPUSH", b"im:l", b"a", b"b"]],
            &[b"BRPOP", b"im:l", b"0"],
        ),
        (
            "BLMPOP",
            vec![&[b"RPUSH", b"im:l", b"a", b"b", b"c"]],
            &[
                b"BLMPOP", b"0", b"2", b"im:none", b"im:l", b"LEFT", b"COUNT", b"2",
            ],
        ),
        (
            "BZPOPMIN",
            vec![&[b"ZADD", b"im:z", b"1", b"a", b"2", b"b"]],
            &[b"BZPOPMIN", b"im:z", b"0"],
        ),
        (
            "BZPOPMAX",
            vec![&[b"ZADD", b"im:z", b"1", b"a", b"2", b"b"]],
            &[b"BZPOPMAX", b"im:z", b"0"],
        ),
        (
            "BZMPOP",
            vec![&[b"ZADD", b"im:z", b"1", b"a", b"2", b"b"]],
            &[b"BZMPOP", b"0", b"1", b"im:z", b"MIN"],
        ),
        (
            "XREADGROUP",
            vec![
                &[b"XADD", b"im:s", b"1-1", b"f", b"v"],
                &[b"XADD", b"im:s", b"2-1", b"f", b"w"],
                &[b"XGROUP", b"CREATE", b"im:s", b"g", b"0"],
            ],
            &[
                b"XREADGROUP",
                b"GROUP",
                b"g",
                b"c",
                b"COUNT",
                b"1",
                b"BLOCK",
                b"0",
                b"STREAMS",
                b"im:s",
                b">",
            ],
        ),
    ];
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|(name, setup, cmd)| family_with(name, setup, cmd, serve_now))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} immediately-served blocking commands left a post-epoch state in the file:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// The reliable-queue shape across the cursor: the destination's range is
/// already WRITTEN, the source's still pending. Before the fix the file held
/// the destination's epoch-start `[x]` and the source's post-move `[b]`, so
/// the moved element was in NEITHER key — an RDB-only restore lost it.
#[test]
fn an_immediate_move_across_the_cursor_keeps_its_element() {
    let hash = crate::storage::dashtable::hash_key;
    let find = |prefix: &str, want: &dyn Fn(u64) -> bool| -> String {
        (0u32..)
            .map(|i| format!("{prefix}{i}"))
            .find(|k| want(hash(k.as_bytes())))
            .unwrap_or_default()
    };
    let dst = find("q:dst:", &|h| h < u64::MAX / 4);
    let src = find("q:src:", &|h| h > u64::MAX / 4 * 3);
    for mv in [
        vec![
            src.as_bytes(),
            dst.as_bytes(),
            &b"LEFT"[..],
            &b"RIGHT"[..],
            &b"0"[..],
        ],
        vec![src.as_bytes(), dst.as_bytes(), &b"0"[..]],
    ] {
        let cmd: &[u8] = if mv.len() == 5 {
            b"BLMOVE"
        } else {
            b"BRPOPLPUSH"
        };
        let mut dbs = vec![Database::new()];
        for i in 0..1500u32 {
            dbs[0].set_string(format!("f:{i:05}").as_bytes(), Bytes::from_static(b"."));
        }
        run(&mut dbs, 0, &[b"RPUSH", src.as_bytes(), b"a", b"b"]);
        run(&mut dbs, 0, &[b"RPUSH", dst.as_bytes(), b"x"]);
        let expected = canonical(&mut dbs);
        let mut epoch = Epoch::begin(&dbs);
        let pending = |epoch: &Epoch, key: &str| {
            epoch
                .state
                .as_ref()
                .is_some_and(|s| s.is_key_pending(0, key.as_bytes()))
        };
        while pending(&epoch, &dst) {
            assert!(!epoch.tick_one(&dbs));
        }
        assert!(
            pending(&epoch, &src),
            "fixture: the source must still be pending"
        );
        let mut parts: Vec<&[u8]> = vec![cmd];
        parts.extend(mv);
        assert!(matches!(serve_now(&mut dbs, &parts), Frame::BulkString(_)));
        let mut loaded = load(1, epoch.finish(&dbs));
        let got = canonical(&mut loaded);
        let queue = |m: &BTreeMap<(usize, Vec<u8>), String>| {
            m.iter()
                .filter(|((_, k), _)| k.starts_with(b"q:"))
                .map(|((_, k), v)| (String::from_utf8_lossy(k).into_owned(), v.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            queue(&got),
            queue(&expected),
            "{}: the element an immediate move took during BGSAVE is missing from the file",
            String::from_utf8_lossy(cmd)
        );
    }
}

/// A blocking pop queued inside MULTI runs at EXEC in immediate-only mode
/// (`blocking_txn::try_exec_blocking_in_txn`), also outside dispatch.
#[test]
fn a_blocking_pop_queued_in_multi_keeps_its_key_point_in_time() {
    let exec = |dbs: &mut [Database], parts: &[&[u8]]| -> Frame {
        let args = bulk_args(&parts[1..]);
        let Some(outcome) = crate::server::conn::blocking_txn::try_exec_blocking_in_txn(
            parts[0],
            &args,
            &mut dbs[0],
            0,
        ) else {
            panic!("{parts:?} is queued unrewritten");
        };
        outcome.reply
    };
    let cases: Vec<(&str, Vec<&[&[u8]]>, &[&[u8]])> = vec![
        (
            "BLPOP",
            vec![&[b"RPUSH", b"tx:l", b"a", b"b"]],
            &[b"BLPOP", b"tx:none", b"tx:l", b"0"],
        ),
        (
            "BRPOP",
            vec![&[b"RPUSH", b"tx:l", b"a", b"b"]],
            &[b"BRPOP", b"tx:l", b"0"],
        ),
        (
            "BZPOPMIN",
            vec![&[b"ZADD", b"tx:z", b"1", b"a", b"2", b"b"]],
            &[b"BZPOPMIN", b"tx:z", b"0"],
        ),
        (
            "BZPOPMAX",
            vec![&[b"ZADD", b"tx:z", b"1", b"a", b"2", b"b"]],
            &[b"BZPOPMAX", b"tx:z", b"0"],
        ),
    ];
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|(name, setup, cmd)| family_with(name, setup, cmd, exec))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} blocking pops queued in MULTI left a post-epoch state in the file:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}
