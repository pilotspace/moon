//! moon#1068: `redis.call('MOVE', ...)` and `redis.call('COPY', ..., 'DB', n)`
//! inside a script (EVAL, EVALSHA, FCALL, and a script queued in MULTI) must do
//! what they do on the connection, and exactly what redis does.
//!
//! A script reaches the keyspace through the ONE database it is pinned to, so
//! before the fix:
//!
//! * `MOVE` answered `-ERR MOVE requires handler-level dispatch`;
//! * `COPY a b DB 4` answered `:1` but wrote `b` into the SOURCE db, while the
//!   effect record (AOF + replication) named db 4 — so master, replica and a
//!   restart disagreed about where `b` lives.
//!
//! Every expected reply below is what redis-server 8.6.1 returns for the same
//! script. `oracle_redis_agrees` runs this very file's cases against a
//! `redis-server` on PATH, so the expectations are checked, not remembered.
//! The keyspace is compared EXACTLY — `DBSIZE` of all 16 dbs, every value, and
//! every deadline as `PEXPIRETIME` (clock-free) — live, after `kill -9` and a
//! restart with `--appendonly yes --appendfsync always`, and on a replica.
//!
//! A second group covers the wake rule (moon#1056): a client blocked in the
//! destination db is served by the script's write, and that pop is logged
//! AFTER the write that fed it, so a restart neither loses nor resurrects the
//! element.
//!
//! Each key set carries its own hash tag, so at `--shards 4` the scripts run on
//! every shard: locally on the connection's shard, or routed to the owner.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Independent key sets, spread over the shards by their hash tags.
const SETS: usize = 8;
/// A fixed absolute deadline (2100-01-01, unix ms): the TTL-bearing keys are
/// compared by `PEXPIRETIME`, exactly, with no clock involved.
const ABS: &str = "4102444800000";

const ERR_SAME: &str = "-ERR source and destination objects are the same\r\n";
const ERR_RANGE: &str = "-ERR DB index is out of range\r\n";

const LIB: &str = "#!lua name=mc1068\n\
redis.register_function('mv', function(keys, args) return redis.call('MOVE', keys[1], args[1]) end)\n\
redis.register_function('cp', function(keys, args) return redis.call('COPY', keys[1], keys[2], 'DB', args[1]) end)\n\
redis.register_function('cpr', function(keys, args) return redis.call('COPY', keys[1], keys[2], 'DB', args[1], 'REPLACE') end)\n";

const MOVE3: &str = "return redis.call('MOVE', KEYS[1], '3')";

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

#[derive(Clone, Copy, Debug, PartialEq)]
enum Ttl {
    /// No expiry (`PEXPIRETIME` = -1).
    Persistent,
    /// Expires at exactly [`ABS`].
    Abs,
}

/// The whole expected keyspace: `(db, key, value, ttl)`.
type Model = Vec<(usize, String, String, Ttl)>;

/// `SET key value` plus, for [`Ttl::Abs`], the fixed deadline.
fn set(c: &mut common::Conn, key: &str, value: &str, ttl: Ttl) {
    match ttl {
        Ttl::Persistent => expect(c, &["SET", key, value], "+OK\r\n"),
        Ttl::Abs => expect(c, &["SET", key, value, "PXAT", ABS], "+OK\r\n"),
    }
}

/// `EVAL script numkeys keys... argv...`, asserting the exact reply.
fn run_lua(c: &mut common::Conn, script: &str, keys: &[&str], argv: &[&str], want: &str) {
    let numkeys = keys.len().to_string();
    let mut parts: Vec<&str> = vec!["EVAL", script, &numkeys];
    parts.extend_from_slice(keys);
    parts.extend_from_slice(argv);
    expect(c, &parts, want);
}

fn fcall(c: &mut common::Conn, func: &str, keys: &[&str], argv: &[&str], want: &str) {
    let numkeys = keys.len().to_string();
    let mut parts: Vec<&str> = vec!["FCALL", func, &numkeys];
    parts.extend_from_slice(keys);
    parts.extend_from_slice(argv);
    expect(c, &parts, want);
}

/// Load the MOVE/COPY function library (every shard, as FUNCTION LOAD does).
fn load_library(c: &mut common::Conn) {
    expect(c, &["FUNCTION", "LOAD", "REPLACE", LIB], "$6\r\nmc1068\r\n");
}

/// Drive every script MOVE / COPY ... DB n case for key set `i` and return the
/// keyspace redis leaves behind. The connection is left in db 0.
fn run_set(c: &mut common::Conn, i: usize) -> Model {
    let k = |name: &str| format!("{{s{i}}}:{name}");
    let v = |name: &str| format!("v{i}:{name}");
    let mut model: Model = Vec::new();
    select(c, 0);

    // EVAL MOVE: success, and the deadline travels with the key.
    set(c, &k("mk"), &v("mk"), Ttl::Persistent);
    run_lua(c, MOVE3, &[&k("mk")], &[], ":1\r\n");
    model.push((3, k("mk"), v("mk"), Ttl::Persistent));
    set(c, &k("mttl"), &v("mttl"), Ttl::Abs);
    run_lua(c, MOVE3, &[&k("mttl")], &[], ":1\r\n");
    model.push((3, k("mttl"), v("mttl"), Ttl::Abs));

    // MOVE onto an existing destination key, and of a missing key: no-ops.
    set(c, &k("mcoll"), &v("mcoll-src"), Ttl::Persistent);
    select(c, 3);
    set(c, &k("mcoll"), &v("mcoll-dst"), Ttl::Persistent);
    select(c, 0);
    run_lua(c, MOVE3, &[&k("mcoll")], &[], ":0\r\n");
    run_lua(c, MOVE3, &[&k("missing")], &[], ":0\r\n");
    model.push((0, k("mcoll"), v("mcoll-src"), Ttl::Persistent));
    model.push((3, k("mcoll"), v("mcoll-dst"), Ttl::Persistent));

    // MOVE into the script's own db, and to a db that does not exist: redis's
    // errors (through pcall, so the script returns them), and nothing moves.
    set(c, &k("mself"), &v("mself"), Ttl::Persistent);
    run_lua(
        c,
        "return redis.pcall('MOVE', KEYS[1], ARGV[1])",
        &[&k("mself")],
        &["0"],
        ERR_SAME,
    );
    run_lua(
        c,
        "return redis.pcall('MOVE', KEYS[1], ARGV[1])",
        &[&k("mself")],
        &["99"],
        ERR_RANGE,
    );
    model.push((0, k("mself"), v("mself"), Ttl::Persistent));

    // One script: move a key, reuse its name, copy the new value to db 4, and
    // keep writing. The effect records must replay in this exact order.
    run_lua(
        c,
        "redis.call('SET', KEYS[1], '1'); \
         redis.call('MOVE', KEYS[1], '3'); \
         redis.call('SET', KEYS[1], '2'); \
         redis.call('COPY', KEYS[1], KEYS[2], 'DB', '4'); \
         return redis.call('INCR', KEYS[1])",
        &[&k("mix"), &k("mix2")],
        &[],
        ":3\r\n",
    );
    model.push((0, k("mix"), "3".to_owned(), Ttl::Persistent));
    model.push((3, k("mix"), "1".to_owned(), Ttl::Persistent));
    model.push((4, k("mix2"), "2".to_owned(), Ttl::Persistent));

    // MOVE from a non-zero db selected before EVAL.
    select(c, 7);
    set(c, &k("m7"), &v("m7"), Ttl::Persistent);
    run_lua(
        c,
        "return redis.call('MOVE', KEYS[1], '2')",
        &[&k("m7")],
        &[],
        ":1\r\n",
    );
    select(c, 0);
    model.push((2, k("m7"), v("m7"), Ttl::Persistent));

    // EVAL COPY ... DB n: a new name and the same name, deadline included.
    set(c, &k("ck"), &v("ck"), Ttl::Abs);
    run_lua(
        c,
        "redis.call('COPY', KEYS[1], KEYS[2], 'DB', '4'); \
         return redis.call('COPY', KEYS[1], KEYS[1], 'DB', '4')",
        &[&k("ck"), &k("ck2")],
        &[],
        ":1\r\n",
    );
    model.push((0, k("ck"), v("ck"), Ttl::Abs));
    model.push((4, k("ck"), v("ck"), Ttl::Abs));
    model.push((4, k("ck2"), v("ck"), Ttl::Abs));

    // Onto an existing key: a no-op without REPLACE, an overwrite with it —
    // and a persistent source leaves the overwritten key persistent.
    set(c, &k("cs"), &v("cs-new"), Ttl::Persistent);
    select(c, 4);
    set(c, &k("cn"), &v("cn-keep"), Ttl::Persistent);
    set(c, &k("cr"), &v("cr-old"), Ttl::Abs);
    select(c, 0);
    run_lua(
        c,
        "return redis.call('COPY', KEYS[1], KEYS[2], 'DB', '4')",
        &[&k("cs"), &k("cn")],
        &[],
        ":0\r\n",
    );
    run_lua(
        c,
        "return redis.call('COPY', KEYS[1], KEYS[2], 'DB', '4', 'REPLACE')",
        &[&k("cs"), &k("cr")],
        &[],
        ":1\r\n",
    );
    model.push((0, k("cs"), v("cs-new"), Ttl::Persistent));
    model.push((4, k("cn"), v("cn-keep"), Ttl::Persistent));
    model.push((4, k("cr"), v("cs-new"), Ttl::Persistent));

    // A missing source is a no-op. A DB clause naming the script's own db is
    // an ordinary same-db copy; the same key in the same db, and a db that
    // does not exist, are redis's errors.
    set(c, &k("c0"), &v("c0"), Ttl::Persistent);
    run_lua(
        c,
        "return {redis.call('COPY', KEYS[1], KEYS[2], 'DB', '4'), \
                 redis.call('COPY', KEYS[3], KEYS[4], 'DB', '0')}",
        &[&k("nosrc"), &k("nodst"), &k("c0"), &k("c0b")],
        &[],
        "*2\r\n:0\r\n:1\r\n",
    );
    run_lua(
        c,
        "return redis.pcall('COPY', KEYS[1], KEYS[1], 'DB', '0')",
        &[&k("c0")],
        &[],
        ERR_SAME,
    );
    run_lua(
        c,
        "return redis.pcall('COPY', KEYS[1], KEYS[2], 'DB', '99')",
        &[&k("c0"), &k("c0c")],
        &[],
        ERR_RANGE,
    );
    model.push((0, k("c0"), v("c0"), Ttl::Persistent));
    model.push((0, k("c0b"), v("c0"), Ttl::Persistent));

    // EVALSHA takes the same bridge.
    let sha = bulk_reply(&c.send(&["SCRIPT", "LOAD", MOVE3])).expect("SCRIPT LOAD sha");
    set(c, &k("sha"), &v("sha"), Ttl::Abs);
    expect(c, &["EVALSHA", &sha, "1", &k("sha")], ":1\r\n");
    model.push((3, k("sha"), v("sha"), Ttl::Abs));

    // FCALL: COPY ... DB n, again (collision), with REPLACE, then MOVE.
    set(c, &k("fa"), &v("fa"), Ttl::Abs);
    fcall(c, "cp", &[&k("fa"), &k("fb")], &["6"], ":1\r\n");
    fcall(c, "cp", &[&k("fa"), &k("fb")], &["6"], ":0\r\n");
    fcall(c, "cpr", &[&k("fa"), &k("fb")], &["6"], ":1\r\n");
    fcall(c, "mv", &[&k("fa")], &["7"], ":1\r\n");
    model.push((6, k("fb"), v("fa"), Ttl::Abs));
    model.push((7, k("fa"), v("fa"), Ttl::Abs));

    // Scripts queued in MULTI: their effects join the transaction's records.
    set(c, &k("xa"), &v("xa"), Ttl::Persistent);
    set(c, &k("xz"), &v("xz"), Ttl::Abs);
    expect(c, &["MULTI"], "+OK\r\n");
    let (xa, xb, xz, xc) = (k("xa"), k("xb"), k("xz"), k("xc"));
    let body: [&[&str]; 3] = [
        &["EVAL", MOVE3, "1", &xa],
        &[
            "EVAL",
            "return redis.call('COPY', KEYS[1], KEYS[2], 'DB', '8')",
            "2",
            &xz,
            &xb,
        ],
        &["FCALL", "cp", "2", &xz, &xc, "8"],
    ];
    for cmd in body {
        expect(c, cmd, "+QUEUED\r\n");
    }
    expect(c, &["EXEC"], "*3\r\n:1\r\n:1\r\n:1\r\n");
    model.push((3, k("xa"), v("xa"), Ttl::Persistent));
    model.push((0, k("xz"), v("xz"), Ttl::Abs));
    model.push((8, k("xb"), v("xz"), Ttl::Abs));
    model.push((8, k("xc"), v("xz"), Ttl::Abs));

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
            let at = c.send(&["PEXPIRETIME", key]);
            let want_at = match ttl {
                Ttl::Persistent => ":-1\r\n".to_owned(),
                Ttl::Abs => format!(":{ABS}\r\n"),
            };
            if at != want_at {
                problems.push(format!(
                    "db{db} {key}: PEXPIRETIME {at:?}, want {want_at:?}"
                ));
            }
        }
    }
    select(c, 0);
    problems
}

/// Run every set live against a fresh server and check the keyspace.
fn run_live(shards: usize, tag: &str) -> (Model, common::ServerGuard, u16, std::path::PathBuf) {
    let dir = common::unique_test_dir(&format!("moon-1068-{tag}-s{shards}"));
    let (server, port) = spawn(&dir, shards);
    let mut c = common::Conn::open(port);
    load_library(&mut c);
    let mut model: Model = Vec::new();
    for i in 0..SETS {
        model.extend(run_set(&mut c, i));
    }
    let live = diff_against(&mut c, &model);
    assert!(
        live.is_empty(),
        "--shards {shards}: the LIVE keyspace after script MOVE / COPY ... DB n \
         differs from redis ({} problems):\n{live:#?}",
        live.len()
    );
    (model, server, port, dir)
}

fn live_matches_redis(shards: usize) {
    let (_model, mut server, _port, dir) = run_live(shards, "live");
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// `kill -9` and a restart on the same `--dir`: every key lands in the db the
/// client was acknowledged, exactly once.
fn survives_kill9(shards: usize) {
    let (model, mut server, port, dir) = run_live(shards, "kill9");
    // appendfsync always: every reply above was sent after its fsync.
    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir, shards);
    let mut c = common::Conn::open(port);
    let recovered = diff_against(&mut c, &model);
    assert!(
        recovered.is_empty(),
        "--shards {shards}: recovery did not reproduce the acknowledged script \
         MOVE / COPY ... DB n state ({} problems):\n{recovered:#?}\n--- server log ---\n{}",
        recovered.len(),
        server_log(&dir)
    );
    drop(c);
    restarted.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

fn wait_link_up(r: &mut common::Conn, rdir: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !r
        .send(&["INFO", "replication"])
        .contains("master_link_status:up")
    {
        assert!(
            Instant::now() < deadline,
            "replica link never came up\n{}",
            server_log(rdir)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A replica attached BEFORE the scripts must end with the master's keyspace:
/// it applies the script's effect records with its own two-db apply path, so
/// the master must have written the db the record names.
fn replica_agrees(master_shards: usize) {
    let mdir = common::unique_test_dir(&format!("moon-1068-repl-m{master_shards}"));
    let rdir = common::unique_test_dir("moon-1068-repl-r");
    let (mut master, mport) = spawn(&mdir, master_shards);
    let (mut replica, rport) = spawn(&rdir, 1);
    let mut r = common::Conn::open(rport);
    expect(
        &mut r,
        &["REPLICAOF", "127.0.0.1", &mport.to_string()],
        "+OK\r\n",
    );
    wait_link_up(&mut r, &rdir);

    let mut m = common::Conn::open(mport);
    load_library(&mut m);
    let mut model: Model = Vec::new();
    for i in 0..SETS {
        model.extend(run_set(&mut m, i));
    }
    let live = diff_against(&mut m, &model);
    assert!(live.is_empty(), "master differs from redis: {live:#?}");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut diverged = diff_against(&mut r, &model);
    while !diverged.is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
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
fn script_move_copy_db_matches_redis_1_shard() {
    live_matches_redis(1);
}

#[test]
fn script_move_copy_db_matches_redis_4_shards() {
    live_matches_redis(4);
}

#[test]
fn script_move_copy_db_survives_kill9_1_shard() {
    survives_kill9(1);
}

#[test]
fn script_move_copy_db_survives_kill9_4_shards() {
    survives_kill9(4);
}

#[test]
#[ignore] // Replication suite: needs a monoio binary and a real link; run explicitly.
fn script_move_copy_db_replica_agrees_1_shard_master() {
    replica_agrees(1);
}

#[test]
#[ignore] // Replication suite: needs a monoio binary and a real link; run explicitly.
fn script_move_copy_db_replica_agrees_4_shard_master() {
    replica_agrees(4);
}

/// The oracle: this file's cases against a real `redis-server` (on PATH), so
/// every expected reply and the model are redis's, not this file's opinion.
#[test]
#[ignore] // Needs redis-server on PATH; run explicitly.
fn oracle_redis_agrees() {
    let dir = common::unique_test_dir("moon-1068-oracle");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let (mut server, port) = common::spawn_listening_guarded(|port| {
        Command::new("redis-server")
            .args(["--port", &port.to_string(), "--save", ""])
            .arg("--dir")
            .arg(&dir)
            .stdout(log_sink(&dir, "server.out"))
            .stderr(log_sink(&dir, "server.err"))
            .spawn()
            .expect("spawn redis-server (on PATH)")
    });
    let mut c = common::Conn::open(port);
    load_library(&mut c);
    let mut model: Model = Vec::new();
    for i in 0..SETS {
        model.extend(run_set(&mut c, i));
    }
    let diff = diff_against(&mut c, &model);
    assert!(diff.is_empty(), "redis disagrees with the model: {diff:#?}");
    expect(&mut c, &["FLUSHALL"], "+OK\r\n");
    run_wake_cases(&mut c, port);
    drop(c);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// A client blocked in the DESTINATION db is served, and the pop is logged
// after the write that fed it (moon#1056).
// ---------------------------------------------------------------------------

/// Block a client on `key` in `db`, and wait until the server reports it
/// blocked — so the script's write cannot race ahead of the registration.
fn park_blpop(port: u16, db: usize, key: &str) -> common::Conn {
    use std::io::Write;
    let mut w = common::Conn::open(port);
    select(&mut w, db);
    w.sock
        .write_all(&common::encode(&["BLPOP", key, "10"]))
        .expect("send BLPOP");
    let mut probe = common::Conn::open(port);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = probe.send(&["INFO", "clients"]);
        if info
            .lines()
            .any(|l| l.starts_with("blocked_clients:") && l.trim_end() != "blocked_clients:0")
        {
            break;
        }
        assert!(Instant::now() < deadline, "BLPOP never parked: {info}");
        std::thread::sleep(Duration::from_millis(20));
    }
    w
}

/// `MOVE {w}l 9` with a `BLPOP {w}l` parked in db 9, then
/// `COPY {w}c {w}c DB 10` with a `BLPOP {w}c` parked in db 10. Each waiter is
/// served the element; the moved list is gone from db 0, the copied one is
/// intact there.
fn run_wake_cases(c: &mut common::Conn, port: u16) {
    select(c, 0);
    let mut w = park_blpop(port, 9, "{w}l");
    expect(c, &["RPUSH", "{w}l", "e1"], ":1\r\n");
    run_lua(
        c,
        "return redis.call('MOVE', KEYS[1], '9')",
        &["{w}l"],
        &[],
        ":1\r\n",
    );
    assert_eq!(w.read_replies(1), "*2\r\n$4\r\n{w}l\r\n$2\r\ne1\r\n");

    let mut w = park_blpop(port, 10, "{w}c");
    expect(c, &["RPUSH", "{w}c", "c1"], ":1\r\n");
    run_lua(
        c,
        "return redis.call('COPY', KEYS[1], KEYS[1], 'DB', '10')",
        &["{w}c"],
        &[],
        ":1\r\n",
    );
    assert_eq!(w.read_replies(1), "*2\r\n$4\r\n{w}c\r\n$2\r\nc1\r\n");

    let problems = wake_state(c);
    assert!(problems.is_empty(), "live: {problems:#?}");
}

/// The keyspace both wake cases leave behind.
fn wake_state(c: &mut common::Conn) -> Vec<String> {
    let mut problems = Vec::new();
    for (db, key, want) in [
        (0, "{w}l", "*0\r\n"),
        (9, "{w}l", "*0\r\n"),
        (10, "{w}c", "*0\r\n"),
        (0, "{w}c", "*1\r\n$2\r\nc1\r\n"),
    ] {
        select(c, db);
        let got = c.send(&["LRANGE", key, "0", "-1"]);
        if got != want {
            problems.push(format!("db{db} LRANGE {key}: {got:?}, want {want:?}"));
        }
    }
    select(c, 0);
    problems
}

fn wake_survives_kill9(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1068-wake-s{shards}"));
    let (mut server, port) = spawn(&dir, shards);
    let mut c = common::Conn::open(port);
    run_wake_cases(&mut c, port);
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir, shards);
    let mut c = common::Conn::open(port);
    let problems = wake_state(&mut c);
    assert!(
        problems.is_empty(),
        "--shards {shards}: a pop served by a script's MOVE / COPY ... DB n did not \
         replay as served ({} problems):\n{problems:#?}\n--- server log ---\n{}",
        problems.len(),
        server_log(&dir)
    );
    drop(c);
    restarted.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn script_move_copy_db_wakes_after_logging_1_shard() {
    wake_survives_kill9(1);
}

#[test]
fn script_move_copy_db_wakes_after_logging_4_shards() {
    wake_survives_kill9(4);
}
