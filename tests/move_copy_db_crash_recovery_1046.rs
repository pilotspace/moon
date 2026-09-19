//! moon#1046: an acknowledged `MOVE` or `COPY src dst DB n` must survive
//! crash recovery exactly as it was applied.
//!
//! Both commands need two databases at once, so the live paths run them
//! through a two-db intercept ahead of the generic single-db dispatch. The
//! AOF logs the command verbatim under a `SELECT <source db>` context. Before
//! the fix, replay handed it to that same single-db dispatch: `MOVE` hit its
//! "requires handler-level dispatch" error arm and `COPY ... DB n` never
//! reached the destination db. Replay discards errors, so after `kill -9` and
//! a restart a MOVE came back in its source db and a COPY's copy was gone,
//! although both had answered `:1`.
//!
//! Every scenario below runs `--appendonly yes --appendfsync always`, so an
//! acknowledged write is durable before its reply and no settle time is
//! needed before the SIGKILL. The post-restart keyspace is compared EXACTLY
//! against the model: per-db `DBSIZE`, every value, and every TTL (a moved
//! or copied key keeps its expiry; a key without one stays persistent).
//!
//! The scenario is replicated over several key sets so that, at
//! `--shards 4`, both a key owned by the connection's shard (local path) and
//! a key owned by another shard (the SPSC path) are exercised.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Number of independent key sets. At `--shards 4` eight distinct names
/// spread over the shards, so both the local and the cross-shard write paths
/// carry MOVE/COPY records.
const SETS: usize = 8;
/// TTL given to the moved and copied keys that carry one.
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

/// Send one command and return its raw RESP reply.
fn cmd(c: &mut common::Conn, parts: &[&str]) -> String {
    c.send(parts)
}

/// Send one command and assert its exact raw reply.
fn expect(c: &mut common::Conn, parts: &[&str], want: &str) {
    let got = cmd(c, parts);
    assert_eq!(got, want, "{parts:?}");
}

fn select(c: &mut common::Conn, db: usize) {
    expect(c, &["SELECT", &db.to_string()], "+OK\r\n");
}

/// Parse a RESP integer reply.
fn int_reply(reply: &str) -> i64 {
    reply
        .strip_prefix(':')
        .and_then(|r| r.trim_end().parse().ok())
        .unwrap_or_else(|| panic!("expected an integer reply, got {reply:?}"))
}

/// Parse a RESP bulk-string reply; `None` for the null bulk.
fn bulk_reply(reply: &str) -> Option<String> {
    if reply == "$-1\r\n" {
        return None;
    }
    let (_, rest) = reply
        .split_once("\r\n")
        .unwrap_or_else(|| panic!("expected a bulk reply, got {reply:?}"));
    Some(rest.trim_end_matches("\r\n").to_owned())
}

/// What a key must look like in one db.
#[derive(Clone, Copy)]
enum Ttl {
    /// No expiry (`PTTL` = -1).
    Persistent,
    /// An expiry no later than [`TTL_MS`] from now and still in the future.
    Bounded,
}

/// The whole expected keyspace: `(db, key, value, ttl)`.
type Model = Vec<(usize, String, String, Ttl)>;

/// Run the MOVE / COPY ... DB n scenario for key set `i` and return the
/// keyspace it must leave behind. Every reply is asserted, so the model only
/// contains writes the server acknowledged.
fn run_set(c: &mut common::Conn, i: usize) -> Model {
    // One hash tag per set: `COPY src dst` with two names must not straddle
    // shards, and eight sets still spread over all four shards.
    let k = |name: &str| format!("{{s{i}}}:{name}");
    let v = |name: &str| format!("v{i}:{name}");
    let mut model: Model = Vec::new();
    let ttl = TTL_MS.to_string();

    select(c, 0);

    // MOVE of a plain key; the moved key is then written in its new db, so
    // replay must have put it there first for the APPEND to land on it.
    expect(c, &["SET", &k("mk"), &v("mk")], "+OK\r\n");
    expect(c, &["MOVE", &k("mk"), "3"], ":1\r\n");
    select(c, 3);
    expect(
        c,
        &["APPEND", &k("mk"), "+x"],
        &format!(":{}\r\n", v("mk").len() + 2),
    );
    select(c, 0);
    model.push((3, k("mk"), format!("{}+x", v("mk")), Ttl::Persistent));

    // MOVE keeps the TTL.
    expect(c, &["SET", &k("mttl"), &v("mttl"), "PX", &ttl], "+OK\r\n");
    expect(c, &["MOVE", &k("mttl"), "3"], ":1\r\n");
    model.push((3, k("mttl"), v("mttl"), Ttl::Bounded));

    // MOVE onto an existing destination key is a no-op: both copies stay.
    expect(c, &["SET", &k("mcoll"), &v("mcoll-src")], "+OK\r\n");
    select(c, 3);
    expect(c, &["SET", &k("mcoll"), &v("mcoll-dst")], "+OK\r\n");
    select(c, 0);
    expect(c, &["MOVE", &k("mcoll"), "3"], ":0\r\n");
    model.push((0, k("mcoll"), v("mcoll-src"), Ttl::Persistent));
    model.push((3, k("mcoll"), v("mcoll-dst"), Ttl::Persistent));

    // MOVE of a missing key is a no-op.
    expect(c, &["MOVE", &k("missing"), "3"], ":0\r\n");

    // MOVE out, then the source name is reused: both dbs keep their own value.
    expect(c, &["SET", &k("mr"), &v("mr-a")], "+OK\r\n");
    expect(c, &["MOVE", &k("mr"), "4"], ":1\r\n");
    expect(c, &["SET", &k("mr"), &v("mr-b")], "+OK\r\n");
    model.push((0, k("mr"), v("mr-b"), Ttl::Persistent));
    model.push((4, k("mr"), v("mr-a"), Ttl::Persistent));

    // MOVE issued from a non-zero db: replay must honour the SELECT context
    // for the source and the argument for the destination.
    select(c, 7);
    expect(c, &["SET", &k("m7"), &v("m7")], "+OK\r\n");
    expect(c, &["MOVE", &k("m7"), "2"], ":1\r\n");
    select(c, 0);
    model.push((2, k("m7"), v("m7"), Ttl::Persistent));

    // COPY ... DB n, same name and a new name; the TTL is copied too.
    expect(c, &["SET", &k("ck"), &v("ck"), "PX", &ttl], "+OK\r\n");
    expect(c, &["COPY", &k("ck"), &k("ck"), "DB", "5"], ":1\r\n");
    expect(c, &["COPY", &k("ck"), &k("ck2"), "DB", "5"], ":1\r\n");
    model.push((0, k("ck"), v("ck"), Ttl::Bounded));
    model.push((5, k("ck"), v("ck"), Ttl::Bounded));
    model.push((5, k("ck2"), v("ck"), Ttl::Bounded));

    // COPY ... DB n REPLACE overwrites the destination.
    select(c, 5);
    expect(c, &["SET", &k("cr"), &v("cr-old")], "+OK\r\n");
    select(c, 0);
    expect(c, &["SET", &k("cr-src"), &v("cr-new")], "+OK\r\n");
    expect(
        c,
        &["COPY", &k("cr-src"), &k("cr"), "DB", "5", "REPLACE"],
        ":1\r\n",
    );
    model.push((0, k("cr-src"), v("cr-new"), Ttl::Persistent));
    model.push((5, k("cr"), v("cr-new"), Ttl::Persistent));

    // COPY ... DB n without REPLACE onto an existing key is a no-op.
    select(c, 5);
    expect(c, &["SET", &k("cn"), &v("cn-keep")], "+OK\r\n");
    select(c, 0);
    expect(c, &["SET", &k("cn-src"), &v("cn-other")], "+OK\r\n");
    expect(c, &["COPY", &k("cn-src"), &k("cn"), "DB", "5"], ":0\r\n");
    model.push((0, k("cn-src"), v("cn-other"), Ttl::Persistent));
    model.push((5, k("cn"), v("cn-keep"), Ttl::Persistent));

    // A COPY'd key diverges from its source afterwards.
    expect(c, &["SET", &k("cd"), &v("cd-1")], "+OK\r\n");
    expect(c, &["COPY", &k("cd"), &k("cd"), "DB", "7"], ":1\r\n");
    expect(c, &["SET", &k("cd"), &v("cd-2")], "+OK\r\n");
    model.push((0, k("cd"), v("cd-2"), Ttl::Persistent));
    model.push((7, k("cd"), v("cd-1"), Ttl::Persistent));

    model
}

/// Compare the live keyspace against `model` exactly. Returns every mismatch
/// (empty = identical) so a failure lists all of them at once.
fn diff_against(c: &mut common::Conn, model: &Model) -> Vec<String> {
    let mut problems = Vec::new();
    for db in 0..16usize {
        select(c, db);
        let want = model.iter().filter(|(d, ..)| *d == db).count() as i64;
        let got = int_reply(&cmd(c, &["DBSIZE"]));
        if got != want {
            problems.push(format!("db{db}: DBSIZE {got}, want {want}"));
        }
        for (_, key, value, ttl) in model.iter().filter(|(d, ..)| *d == db) {
            match bulk_reply(&cmd(c, &["GET", key])) {
                Some(got) if got == *value => {}
                Some(got) => problems.push(format!("db{db} {key}: value {got:?}, want {value:?}")),
                None => problems.push(format!("db{db} {key}: MISSING, want {value:?}")),
            }
            let pttl = int_reply(&cmd(c, &["PTTL", key]));
            let ok = match ttl {
                Ttl::Persistent => pttl == -1,
                Ttl::Bounded => pttl > 0 && pttl <= TTL_MS,
            };
            if !ok {
                problems.push(format!(
                    "db{db} {key}: PTTL {pttl}, want {:?}",
                    ttl_name(*ttl)
                ));
            }
        }
    }
    select(c, 0);
    problems
}

fn ttl_name(t: Ttl) -> &'static str {
    match t {
        Ttl::Persistent => "-1 (no expiry)",
        Ttl::Bounded => "in (0, 600000]",
    }
}

/// Poll `INFO persistence` until no AOF rewrite is in progress.
fn wait_rewrite_done(c: &mut common::Conn) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let info = cmd(c, &["INFO", "persistence"]);
        if info.contains("aof_rewrite_in_progress:0") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "BGREWRITEAOF still in progress after 20s:\n{info}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Drive the scenario, check it live, SIGKILL, restart on the same `--dir`,
/// and check that recovery reproduced the identical keyspace.
///
/// With `rewrite_midway`, half the key sets are written, then BGREWRITEAOF
/// folds them into a new base, then the rest are written on top of it — so
/// recovery loads a rewritten base AND replays MOVE/COPY records from the
/// tail that follows it.
fn scenario(shards: usize, rewrite_midway: bool) {
    let tag = format!(
        "moon-1046-s{shards}-{}",
        if rewrite_midway { "rw" } else { "plain" }
    );
    let dir = common::unique_test_dir(&tag);
    let (mut server, port) = spawn(&dir, shards);

    let mut model: Model = Vec::new();
    {
        let mut c = common::Conn::open(port);
        for i in 0..SETS {
            if rewrite_midway && i == SETS / 2 {
                let reply = cmd(&mut c, &["BGREWRITEAOF"]);
                assert!(reply.starts_with('+'), "BGREWRITEAOF refused: {reply:?}");
                // Give the rewrite a moment to register before polling it.
                std::thread::sleep(Duration::from_millis(200));
                wait_rewrite_done(&mut c);
            }
            model.extend(run_set(&mut c, i));
        }
        let live = diff_against(&mut c, &model);
        assert!(
            live.is_empty(),
            "the LIVE keyspace already disagrees with the model, so the \
             scenario itself is wrong (not recovery):\n{live:#?}"
        );
    }

    // appendfsync always: every reply above was sent after its fsync.
    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir, shards);

    let mut c = common::Conn::open(port);
    let recovered = diff_against(&mut c, &model);
    assert!(
        recovered.is_empty(),
        "--shards {shards} (rewrite_midway={rewrite_midway}): recovery did not \
         reproduce the acknowledged MOVE / COPY ... DB n state ({} problems):\n{:#?}\n\
         --- server log ---\n{}",
        recovered.len(),
        recovered,
        server_log(&dir)
    );
    drop(c);
    restarted.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn move_and_copy_db_survive_kill9_1_shard() {
    scenario(1, false);
}

#[test]
fn move_and_copy_db_survive_kill9_4_shards() {
    scenario(4, false);
}

#[test]
fn move_and_copy_db_survive_kill9_after_rewrite_1_shard() {
    scenario(1, true);
}

#[test]
fn move_and_copy_db_survive_kill9_after_rewrite_4_shards() {
    scenario(4, true);
}
