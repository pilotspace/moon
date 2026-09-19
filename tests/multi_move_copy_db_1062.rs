//! moon#1062: `MOVE` and `COPY src dst DB n` queued inside `MULTI` must do at
//! `EXEC` exactly what they do outside it, and exactly what redis does.
//!
//! Both commands need two databases at once. The live (non-MULTI) paths run
//! them through a two-db intercept ahead of the single-db dispatch; the
//! transaction executors had no such intercept. So inside MULTI:
//!
//! * `MOVE` answered `-ERR MOVE requires handler-level dispatch` at EXEC;
//! * `COPY a b DB 4` answered `:1` but wrote `b` into the SOURCE db, and the
//!   AOF logged the command as written. A replica (and, with moon#1046, AOF
//!   replay) applies that record into db 4, so master, replica and restart
//!   disagreed about where `b` lives.
//!
//! Every expected EXEC reply below is the one redis-server 8.6.1 returns for
//! the same body (recorded against a live redis before this test was written).
//! The keyspace is then compared EXACTLY — `DBSIZE` of all 16 dbs, every
//! value, every `PTTL` — live, and again after `kill -9` and a restart with
//! `--appendonly yes --appendfsync always`.
//!
//! Each key set carries its own hash tag, so at `--shards 4` the sets spread
//! over the shards: a body owned by the connection's shard runs locally, one
//! owned by another shard is routed to its owner (`TxnExecute`). Both
//! executors are therefore exercised.
//!
//! A second group covers the non-MULTI sibling found while fixing this: at
//! `--shards 4`, `COPY src dst DB n` whose two keys hash to different shards
//! acked `:1` and wrote `dst` into the SOURCE's shard, where no normally
//! routed read can see it (8 of 12 constructed placements were unreadable).

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};

use moon::shard::dispatch::key_to_shard;

/// Independent key sets. At `--shards 4` eight hash tags spread over every
/// shard, so both the local and the owner-routed executor carry the bodies.
const SETS: usize = 8;
/// TTL given to the keys that carry one.
const TTL_MS: i64 = 600_000;

fn log_sink(dir: &Path, name: &str) -> Stdio {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(name))
    {
        Ok(f) => Stdio::from(f),
        Err(_) => Stdio::null(),
    }
}

fn spawn(dir: &Path, shards: usize) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                // Crash harnesses always disable the disk-free guard: near the
                // threshold it refuses writes and the test mis-reads that as loss.
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .stdout(log_sink(dir, "server.out"))
            .stderr(log_sink(dir, "server.err"))
            .spawn()
            .expect("spawn moon")
    })
}

fn server_log(dir: &Path) -> String {
    let mut s = String::new();
    for name in ["server.out", "server.err"] {
        if let Ok(text) = std::fs::read_to_string(dir.join(name)) {
            s.push_str(&text);
        }
    }
    s
}

fn expect(c: &mut common::Conn, parts: &[&str], want: &str) {
    let got = c.send(parts);
    assert_eq!(got, want, "{parts:?}");
}

fn select(c: &mut common::Conn, db: usize) {
    expect(c, &["SELECT", &db.to_string()], "+OK\r\n");
}

/// Run `body` as one MULTI/EXEC and assert every QUEUED reply and the exact
/// EXEC reply (raw RESP2 bytes).
fn txn(c: &mut common::Conn, body: &[&[&str]], exec_reply: &str) {
    expect(c, &["MULTI"], "+OK\r\n");
    for cmd in body {
        expect(c, cmd, "+QUEUED\r\n");
    }
    let got = c.send(&["EXEC"]);
    assert_eq!(got, exec_reply, "EXEC of {body:?}");
}

fn int_reply(reply: &str) -> i64 {
    reply
        .strip_prefix(':')
        .and_then(|r| r.trim_end().parse().ok())
        .unwrap_or_else(|| panic!("expected an integer reply, got {reply:?}"))
}

fn bulk_reply(reply: &str) -> Option<String> {
    if reply == "$-1\r\n" {
        return None;
    }
    let (_, rest) = reply
        .split_once("\r\n")
        .unwrap_or_else(|| panic!("expected a bulk reply, got {reply:?}"));
    Some(rest.trim_end_matches("\r\n").to_owned())
}

#[derive(Clone, Copy, Debug)]
enum Ttl {
    /// No expiry (`PTTL` = -1).
    Persistent,
    /// An expiry in `(0, TTL_MS]`.
    Bounded,
}

/// The whole expected keyspace: `(db, key, value, ttl)`.
type Model = Vec<(usize, String, String, Ttl)>;

const ERR_SAME: &str = "-ERR source and destination objects are the same\r\n";
const ERR_RANGE: &str = "-ERR DB index is out of range\r\n";

/// Drive every MOVE / COPY ... DB n case for key set `i` inside MULTI and
/// return the keyspace redis would leave behind. The connection is left in
/// db 0.
fn run_set(c: &mut common::Conn, i: usize) -> Model {
    let k = |name: &str| format!("{{s{i}}}:{name}");
    let v = |name: &str| format!("v{i}:{name}");
    let ttl = TTL_MS.to_string();
    let mut model: Model = Vec::new();
    select(c, 0);

    // MOVE: success.
    expect(c, &["SET", &k("mk"), &v("mk")], "+OK\r\n");
    txn(c, &[&["MOVE", &k("mk"), "3"]], "*1\r\n:1\r\n");
    model.push((3, k("mk"), v("mk"), Ttl::Persistent));

    // MOVE keeps the TTL.
    expect(c, &["SET", &k("mttl"), &v("mttl"), "PX", &ttl], "+OK\r\n");
    txn(c, &[&["MOVE", &k("mttl"), "3"]], "*1\r\n:1\r\n");
    model.push((3, k("mttl"), v("mttl"), Ttl::Bounded));

    // MOVE onto an existing destination key is a no-op.
    expect(c, &["SET", &k("mcoll"), &v("mcoll-src")], "+OK\r\n");
    select(c, 3);
    expect(c, &["SET", &k("mcoll"), &v("mcoll-dst")], "+OK\r\n");
    select(c, 0);
    txn(c, &[&["MOVE", &k("mcoll"), "3"]], "*1\r\n:0\r\n");
    model.push((0, k("mcoll"), v("mcoll-src"), Ttl::Persistent));
    model.push((3, k("mcoll"), v("mcoll-dst"), Ttl::Persistent));

    // MOVE of a missing key is a no-op.
    txn(c, &[&["MOVE", &k("missing"), "3"]], "*1\r\n:0\r\n");

    // MOVE into the current db, and to a db that does not exist: redis's
    // errors, and the key stays where it was. Redis checks "same object"
    // before it looks the key up, so a missing key gets the error too — for
    // COPY without a DB clause as well.
    expect(c, &["SET", &k("mself"), &v("mself")], "+OK\r\n");
    txn(
        c,
        &[
            &["MOVE", &k("mself"), "0"],
            &["MOVE", &k("mself"), "99"],
            &["MOVE", &k("absent"), "0"],
            &["COPY", &k("absent"), &k("absent")],
        ],
        &format!("*4\r\n{ERR_SAME}{ERR_RANGE}{ERR_SAME}{ERR_SAME}"),
    );
    model.push((0, k("mself"), v("mself"), Ttl::Persistent));

    // The moved key is used in the SAME body: the source name is reused in
    // db 0 and the moved copy is appended to in db 3. The body's own SELECTs
    // decide where each command runs.
    expect(c, &["SET", &k("mu"), "1"], "+OK\r\n");
    txn(
        c,
        &[
            &["MOVE", &k("mu"), "3"],
            &["SET", &k("mu"), "2"],
            &["SELECT", "3"],
            &["APPEND", &k("mu"), "x"],
            &["SELECT", "0"],
        ],
        "*5\r\n:1\r\n+OK\r\n+OK\r\n:2\r\n+OK\r\n",
    );
    model.push((0, k("mu"), "2".to_owned(), Ttl::Persistent));
    model.push((3, k("mu"), "1x".to_owned(), Ttl::Persistent));

    // MOVE from a non-zero db selected before MULTI.
    select(c, 7);
    expect(c, &["SET", &k("m7"), &v("m7")], "+OK\r\n");
    txn(c, &[&["MOVE", &k("m7"), "2"]], "*1\r\n:1\r\n");
    select(c, 0);
    model.push((2, k("m7"), v("m7"), Ttl::Persistent));

    // MOVE after a SELECT queued in the same body: the source is the db the
    // body selected, not the one EXEC was issued from.
    select(c, 5);
    expect(c, &["SET", &k("ms"), &v("ms")], "+OK\r\n");
    select(c, 0);
    txn(
        c,
        &[&["SELECT", "5"], &["MOVE", &k("ms"), "1"], &["SELECT", "0"]],
        "*3\r\n+OK\r\n:1\r\n+OK\r\n",
    );
    model.push((1, k("ms"), v("ms"), Ttl::Persistent));

    // COPY ... DB n: a new name, the same name, and the TTL travels.
    expect(c, &["SET", &k("ck"), &v("ck"), "PX", &ttl], "+OK\r\n");
    txn(
        c,
        &[
            &["COPY", &k("ck"), &k("ck2"), "DB", "4"],
            &["COPY", &k("ck"), &k("ck"), "DB", "4"],
        ],
        "*2\r\n:1\r\n:1\r\n",
    );
    model.push((0, k("ck"), v("ck"), Ttl::Bounded));
    model.push((4, k("ck"), v("ck"), Ttl::Bounded));
    model.push((4, k("ck2"), v("ck"), Ttl::Bounded));

    // COPY ... DB n onto an existing key: a no-op without REPLACE, an
    // overwrite with it.
    expect(c, &["SET", &k("cs"), &v("cs-new")], "+OK\r\n");
    select(c, 4);
    expect(c, &["SET", &k("cn"), &v("cn-keep")], "+OK\r\n");
    expect(c, &["SET", &k("cr"), &v("cr-old")], "+OK\r\n");
    select(c, 0);
    txn(
        c,
        &[
            &["COPY", &k("cs"), &k("cn"), "DB", "4"],
            &["COPY", &k("cs"), &k("cr"), "DB", "4", "REPLACE"],
        ],
        "*2\r\n:0\r\n:1\r\n",
    );
    model.push((0, k("cs"), v("cs-new"), Ttl::Persistent));
    model.push((4, k("cn"), v("cn-keep"), Ttl::Persistent));
    model.push((4, k("cr"), v("cs-new"), Ttl::Persistent));

    // COPY ... DB n of a missing source is a no-op.
    txn(
        c,
        &[&["COPY", &k("nosrc"), &k("nodst"), "DB", "4"]],
        "*1\r\n:0\r\n",
    );

    // COPY with a DB clause naming the CURRENT db is an ordinary same-db
    // copy; the same key in the same db, and a db that does not exist, are
    // redis's errors and write nothing.
    expect(c, &["SET", &k("c0"), &v("c0")], "+OK\r\n");
    txn(
        c,
        &[
            &["COPY", &k("c0"), &k("c0b"), "DB", "0"],
            &["COPY", &k("c0"), &k("c0"), "DB", "0"],
            &["COPY", &k("c0"), &k("c0c"), "DB", "99"],
        ],
        &format!("*3\r\n:1\r\n{ERR_SAME}{ERR_RANGE}"),
    );
    model.push((0, k("c0"), v("c0"), Ttl::Persistent));
    model.push((0, k("c0b"), v("c0"), Ttl::Persistent));

    // COPY ... DB n from a non-zero db, then after a SELECT queued in the body.
    select(c, 7);
    expect(c, &["SET", &k("c7"), &v("c7")], "+OK\r\n");
    txn(
        c,
        &[&["COPY", &k("c7"), &k("c7b"), "DB", "2"]],
        "*1\r\n:1\r\n",
    );
    select(c, 5);
    expect(c, &["SET", &k("c5"), &v("c5")], "+OK\r\n");
    select(c, 0);
    txn(
        c,
        &[
            &["SELECT", "5"],
            &["COPY", &k("c5"), &k("c5b"), "DB", "1"],
            &["SELECT", "0"],
        ],
        "*3\r\n+OK\r\n:1\r\n+OK\r\n",
    );
    model.push((7, k("c7"), v("c7"), Ttl::Persistent));
    model.push((2, k("c7b"), v("c7"), Ttl::Persistent));
    model.push((5, k("c5"), v("c5"), Ttl::Persistent));
    model.push((1, k("c5b"), v("c5"), Ttl::Persistent));

    // A mixed body: the copy is taken before the INCR that follows it.
    expect(c, &["SET", &k("xa"), "1"], "+OK\r\n");
    expect(c, &["SET", &k("xb"), "2"], "+OK\r\n");
    txn(
        c,
        &[
            &["MOVE", &k("xa"), "3"],
            &["COPY", &k("xb"), &k("xb"), "DB", "4"],
            &["INCR", &k("xb")],
            &["GET", &k("xb")],
        ],
        "*4\r\n:1\r\n:1\r\n:3\r\n$1\r\n3\r\n",
    );
    model.push((3, k("xa"), "1".to_owned(), Ttl::Persistent));
    model.push((0, k("xb"), "3".to_owned(), Ttl::Persistent));
    model.push((4, k("xb"), "2".to_owned(), Ttl::Persistent));

    model
}

/// Compare the keyspace against `model` exactly; every mismatch is returned.
fn diff_against(c: &mut common::Conn, model: &Model) -> Vec<String> {
    let mut problems = Vec::new();
    for db in 0..16usize {
        select(c, db);
        let want = model.iter().filter(|(d, ..)| *d == db).count() as i64;
        let got = int_reply(&c.send(&["DBSIZE"]));
        if got != want {
            problems.push(format!("db{db}: DBSIZE {got}, want {want}"));
        }
        for (_, key, value, ttl) in model.iter().filter(|(d, ..)| *d == db) {
            match bulk_reply(&c.send(&["GET", key])) {
                Some(got) if got == *value => {}
                Some(got) => problems.push(format!("db{db} {key}: value {got:?}, want {value:?}")),
                None => problems.push(format!("db{db} {key}: MISSING, want {value:?}")),
            }
            let pttl = int_reply(&c.send(&["PTTL", key]));
            let ok = match ttl {
                Ttl::Persistent => pttl == -1,
                Ttl::Bounded => pttl > 0 && pttl <= TTL_MS,
            };
            if !ok {
                problems.push(format!("db{db} {key}: PTTL {pttl}, want {ttl:?}"));
            }
        }
    }
    select(c, 0);
    problems
}

/// Run every set live, check the keyspace, and return the model plus the
/// server and its dir for the caller's restart leg.
fn run_live(shards: usize, tag: &str) -> (Model, common::ServerGuard, u16, std::path::PathBuf) {
    let dir = common::unique_test_dir(&format!("moon-1062-{tag}-s{shards}"));
    let (server, port) = spawn(&dir, shards);
    let mut model: Model = Vec::new();
    let mut c = common::Conn::open(port);
    for i in 0..SETS {
        model.extend(run_set(&mut c, i));
    }
    let live = diff_against(&mut c, &model);
    assert!(
        live.is_empty(),
        "--shards {shards}: the LIVE keyspace after MOVE / COPY ... DB n inside \
         MULTI differs from redis ({} problems):\n{live:#?}",
        live.len()
    );
    (model, server, port, dir)
}

fn live_matches_redis(shards: usize) {
    let (_model, mut server, _port, dir) = run_live(shards, "live");
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The live keyspace, then `kill -9` and a restart on the same `--dir`: the
/// recovered keyspace must be the one the client was acknowledged.
fn survives_kill9(shards: usize) {
    let (model, mut server, port, dir) = run_live(shards, "kill9");
    // appendfsync always: every EXEC reply above was sent after its fsync.
    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir, shards);
    let mut c = common::Conn::open(port);
    let recovered = diff_against(&mut c, &model);
    assert!(
        recovered.is_empty(),
        "--shards {shards}: recovery did not reproduce the acknowledged \
         MOVE / COPY ... DB n state ({} problems):\n{recovered:#?}\n--- server log ---\n{}",
        recovered.len(),
        server_log(&dir)
    );
    drop(c);
    restarted.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A replica attached BEFORE the transactions must end with the master's
/// keyspace: the records the EXEC path streams are applied by the replica's
/// two-db apply path, so the master has to have written the same dbs.
fn replica_agrees(master_shards: usize) {
    let mdir = common::unique_test_dir(&format!("moon-1062-repl-m{master_shards}"));
    let rdir = common::unique_test_dir("moon-1062-repl-r");
    let (mut master, mport) = spawn(&mdir, master_shards);
    let (mut replica, rport) = spawn(&rdir, 1);
    let mut r = common::Conn::open(rport);
    expect(
        &mut r,
        &["REPLICAOF", "127.0.0.1", &mport.to_string()],
        "+OK\r\n",
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !r
        .send(&["INFO", "replication"])
        .contains("master_link_status:up")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "replica link never came up\n{}",
            server_log(&rdir)
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let mut m = common::Conn::open(mport);
    let mut model: Model = Vec::new();
    for i in 0..SETS {
        model.extend(run_set(&mut m, i));
    }
    let live = diff_against(&mut m, &model);
    assert!(live.is_empty(), "master differs from redis: {live:#?}");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut diverged = diff_against(&mut r, &model);
    while !diverged.is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(200));
        diverged = diff_against(&mut r, &model);
    }
    assert!(
        diverged.is_empty(),
        "master --shards {master_shards}: the replica's keyspace differs from the \
         master's ({} problems):\n{diverged:#?}",
        diverged.len()
    );
    drop((m, r));
    master.kill_now();
    replica.kill_now();
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
#[ignore] // Replication suite: needs a monoio binary and a real link; run explicitly.
fn multi_move_copy_db_replica_agrees_1_shard_master() {
    replica_agrees(1);
}

#[test]
#[ignore] // Replication suite: needs a monoio binary and a real link; run explicitly.
fn multi_move_copy_db_replica_agrees_4_shard_master() {
    replica_agrees(4);
}

#[test]
fn multi_move_copy_db_matches_redis_1_shard() {
    live_matches_redis(1);
}

#[test]
fn multi_move_copy_db_matches_redis_4_shards() {
    live_matches_redis(4);
}

#[test]
fn multi_move_copy_db_survives_kill9_1_shard() {
    survives_kill9(1);
}

#[test]
fn multi_move_copy_db_survives_kill9_4_shards() {
    survives_kill9(4);
}

// ---------------------------------------------------------------------------
// COPY ... DB n OUTSIDE MULTI whose two keys hash to different shards
// ---------------------------------------------------------------------------

const XSHARDS: usize = 4;
const XTRIALS: usize = 12;

/// `(src, dst)` names that provably hash to different shards at
/// `--shards 4`, chosen with the server's own routing function.
fn split_pairs() -> Vec<(String, String)> {
    (0..XTRIALS)
        .map(|i| {
            let src = format!("xsrc{i}");
            let owner = key_to_shard(src.as_bytes(), XSHARDS);
            let dst = (0..)
                .map(|j| format!("xdst{i}-{j}"))
                .find(|d| key_to_shard(d.as_bytes(), XSHARDS) != owner)
                .expect("a destination on another shard");
            (src, dst)
        })
        .collect()
}

/// The acknowledgement contract, not a particular remedy: a `:1` must be
/// readable where the command named it; anything else must leave the
/// destination absent everywhere and the source untouched.
#[test]
fn cross_shard_copy_db_never_misplaces_the_destination() {
    let dir = common::unique_test_dir("moon-1062-xshard");
    let (mut server, port) = spawn(&dir, XSHARDS);
    let mut c = common::Conn::open(port);
    let mut problems = Vec::new();
    for (src, dst) in split_pairs() {
        for dst_db in ["3", "0"] {
            select(&mut c, 0);
            expect(&mut c, &["SET", &src, "payload"], "+OK\r\n");
            let reply = c.send(&["COPY", &src, &dst, "DB", dst_db]);
            select(&mut c, dst_db.parse().expect("db"));
            let got = bulk_reply(&c.send(&["GET", &dst]));
            let dbsize = int_reply(&c.send(&["DBSIZE"]));
            select(&mut c, 0);
            let src_after = bulk_reply(&c.send(&["GET", &src]));
            match reply.as_str() {
                ":1\r\n" if got.as_deref() == Some("payload") => {}
                ":1\r\n" => problems.push(format!(
                    "COPY {src} {dst} DB {dst_db} acked :1 but GET {dst} in db{dst_db} -> {got:?}"
                )),
                _ => {
                    // Refused: nothing may have been written anywhere.
                    let want_dbsize = if dst_db == "0" { 1 } else { 0 };
                    if got.is_some() || dbsize != want_dbsize {
                        problems.push(format!(
                            "COPY {src} {dst} DB {dst_db} answered {reply:?} yet db{dst_db} \
                             has GET {got:?}, DBSIZE {dbsize}"
                        ));
                    }
                }
            }
            if src_after.as_deref() != Some("payload") {
                problems.push(format!("{src} changed to {src_after:?}"));
            }
            expect(&mut c, &["FLUSHALL"], "+OK\r\n");
        }
    }
    assert!(
        problems.is_empty(),
        "cross-shard COPY ... DB n misplaced its destination ({} problems):\n{problems:#?}",
        problems.len()
    );
    drop(c);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Control for the test above: the same command with `{hash}`-tagged, so
/// co-located, keys still works at `--shards 4`. A blanket refusal fails here.
#[test]
fn colocated_copy_db_still_works_at_4_shards() {
    let dir = common::unique_test_dir("moon-1062-xshard-ctl");
    let (mut server, port) = spawn(&dir, XSHARDS);
    let mut c = common::Conn::open(port);
    for i in 0..XTRIALS {
        let src = format!("{{x{i}}}src");
        let dst = format!("{{x{i}}}dst");
        select(&mut c, 0);
        expect(&mut c, &["SET", &src, "payload"], "+OK\r\n");
        expect(&mut c, &["COPY", &src, &dst, "DB", "3"], ":1\r\n");
        expect(&mut c, &["COPY", &src, &dst, "DB", "0"], ":1\r\n");
        expect(&mut c, &["GET", &dst], "$7\r\npayload\r\n");
        select(&mut c, 3);
        expect(&mut c, &["GET", &dst], "$7\r\npayload\r\n");
    }
    drop(c);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}
