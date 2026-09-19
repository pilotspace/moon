//! moon#1084: an `EXEC` whose body holds a connection-level intercept must be
//! logged in the order it was APPLIED, not the order its reply was finished.
//!
//! The executor applies the body synchronously and leaves a placeholder for
//! each queued intercept (`CLIENT`, `CONFIG`, `SCRIPT`, `FUNCTION`, ...), which
//! the connection fills afterwards. Some of those fills await. The body's AOF
//! records (and, on monoio, its replication records) used to be appended only
//! AFTER that await, so a write another client made while `EXEC` was waiting
//! was logged BEFORE the body although it was applied AFTER it:
//!
//! ```text
//! A: SET k 0
//! A: MULTI / INCR k / SCRIPT LOAD s / EXEC   (body applied, EXEC waits on the
//!                                             SCRIPT LOAD fan-out)
//! B: SET k 5                                  -> +OK   (applied after INCR)
//! A: EXEC reply [1, <sha>]
//! B: GET k                                    -> "5"   (acknowledged state)
//! kill -9, restart
//! GET k                                       -> "6"   (INCR replayed after SET)
//! ```
//!
//! The await is a queued `SCRIPT LOAD`: filling it fans the script out to every
//! other shard and waits for each one's ack. One of those shards is kept busy
//! with `DEBUG SLEEP`, run inside a transaction that shard owns, so the fan-out
//! — and with it the `EXEC` reply — waits for the sleep to end. That gives a
//! deterministic window for the second client's write, with no test hook.
//!
//! (This scenario used to park `EXEC` on a queued `WAIT` asking for more
//! replicas than exist. Redis answers a `WAIT` inside `MULTI` at once, and so
//! does moon since moon#1098, so that window is gone. With it went every await
//! a `--shards 1` `EXEC` could hit: the fan-outs have no other shard to wait
//! for. The local `EXEC` path is the same code at any shard count, and the
//! local round below covers it.)
//!
//! Which shard a connection lands on is not observable, so every key is raced
//! once with each other shard stalled. A round opens the window when the stalled
//! shard is neither A's nor B's. Every server runs `--appendonly yes
//! --appendfsync always`: each reply is sent after its fsync, so the SIGKILL
//! needs no settle time. Each shard owns keys, and one connection A runs every
//! round, so the keys of A's own shard take the connection's LOCAL `EXEC` path
//! (the one that was wrong) and the others take the owner-routed path, which
//! logs on the owner in the same stretch as the apply and must stay right.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

const SHARDS: usize = 4;
/// How long the stalled shard sleeps. Under the 2 s fan-out budget, so the
/// `SCRIPT LOAD` fan-out waits for it rather than giving up on it.
const STALL_MS: u64 = 1_000;
/// Pause between starting the stall and sending the transaction.
const STEP_MS: u64 = 150;
/// The script the queued `SCRIPT LOAD` loads.
const SCRIPT: &str = "return 1084";

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
                // No rewrite may fold the tail mid-scenario: the bug is about
                // the order of records in the tail itself.
                "--auto-aof-rewrite-percentage",
                "0",
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

fn bulk(v: &str) -> String {
    format!("${}\r\n{v}\r\n", v.len())
}

/// The first key with `prefix` that shard `shard` owns.
fn key_on(prefix: &str, shard: usize) -> String {
    (0u32..100_000)
        .map(|i| format!("{prefix}:{i}"))
        .find(|k| key_to_shard(k.as_bytes(), SHARDS) == shard)
        .expect("a key for every shard")
}

/// One race: the key it writes, the shard that owns it, the shard kept busy.
struct Round {
    key: String,
    owner: usize,
    stalled: usize,
}

/// Every owner shard raced once with each OTHER shard stalled: the owner must
/// stay free, or the body could not apply while the fan-out waits. Each round
/// has a key of its own, so a later round cannot overwrite an earlier one's
/// evidence.
fn rounds() -> Vec<Round> {
    let mut out = Vec::new();
    for owner in 0..SHARDS {
        for stalled in (0..SHARDS).filter(|&s| s != owner) {
            out.push(Round {
                key: key_on(&format!("k1084:o{owner}:s{stalled}"), owner),
                owner,
                stalled,
            });
        }
    }
    out
}

fn send_raw(c: &mut common::Conn, cmds: &[&[&str]]) {
    let mut batch = Vec::new();
    for parts in cmds {
        batch.extend_from_slice(&common::encode(parts));
    }
    c.sock.write_all(&batch).expect("write batch");
}

/// Occupy the thread of shard `shard` for [`STALL_MS`].
///
/// `DEBUG SLEEP` blocks the thread that executes it, and a transaction body
/// executes on the shard that owns its keys — routed there when the
/// connection lives elsewhere. The replies are left unread; the caller reads
/// them with [`finish_stall`].
fn start_stall(port: u16, shard: usize) -> common::Conn {
    let mut s = common::Conn::open(port);
    let tagged = format!("{{{}}}:stall", key_on("stall1084", shard));
    let secs = format!("{:.3}", STALL_MS as f64 / 1000.0);
    send_raw(
        &mut s,
        &[
            &["MULTI"],
            &["SET", &tagged, "1"],
            &["DEBUG", "SLEEP", &secs],
            &["EXEC"],
        ],
    );
    s
}

fn finish_stall(mut s: common::Conn) {
    assert_eq!(
        s.read_replies(4),
        "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n+OK\r\n",
        "the stalling transaction must run DEBUG SLEEP"
    );
}

/// Run one race. On return the server has acknowledged `key = 5`. Returns
/// whether B's write provably landed while `EXEC` was still waiting.
fn race_one(
    port: u16,
    a: &mut common::Conn,
    b: &mut common::Conn,
    round: &Round,
    sha: &str,
) -> bool {
    let key = round.key.as_str();
    assert_eq!(a.send(&["SET", key, "0"]), "+OK\r\n");

    let stall = start_stall(port, round.stalled);
    let stall_start = Instant::now();
    std::thread::sleep(Duration::from_millis(STEP_MS));

    // MULTI / INCR / SCRIPT LOAD / EXEC in one write. The replies are read
    // after the second client has written.
    send_raw(
        a,
        &[
            &["MULTI"],
            &["INCR", key],
            &["SCRIPT", "LOAD", SCRIPT],
            &["EXEC"],
        ],
    );

    // The body is applied before the intercept is filled: wait until it is
    // visible, so the write below is provably applied AFTER it. When A or B
    // lives on the stalled shard it only becomes visible after the stall;
    // that round simply does not open the window.
    let deadline = stall_start + Duration::from_millis(STALL_MS * 5);
    while b.send(&["GET", key]) != bulk("1") {
        assert!(
            Instant::now() < deadline,
            "{key}: the EXEC body never became visible"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(b.send(&["SET", key, "5"]), "+OK\r\n", "{key}: SET k 5");
    // The EXEC reply needs the stalled shard's ack for the SCRIPT LOAD
    // fan-out, so a write acknowledged well before the stall ends landed
    // while EXEC was still waiting.
    let in_window = stall_start.elapsed() < Duration::from_millis(STALL_MS - STEP_MS);

    assert_eq!(
        a.read_replies(4),
        format!("+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n{}", bulk(sha)),
        "{key}: MULTI/INCR/SCRIPT LOAD/EXEC replies"
    );
    finish_stall(stall);
    assert_eq!(
        b.send(&["GET", key]),
        bulk("5"),
        "{key}: acknowledged value"
    );
    in_window
}

/// Run every round over ONE connection A (its shard is fixed, so the keys of
/// that shard take the local `EXEC` path) and check that the window opened.
fn race_all(port: u16) -> Vec<Round> {
    let rounds = rounds();
    let mut a = common::Conn::open(port);
    let mut b = common::Conn::open(port);
    let reply = a.send(&["SCRIPT", "LOAD", SCRIPT]);
    let sha = reply
        .strip_prefix("$40\r\n")
        .and_then(|r| r.strip_suffix("\r\n"))
        .unwrap_or_else(|| panic!("SCRIPT LOAD answered {reply:?}"))
        .to_string();

    let mut opened = [0usize; SHARDS];
    for round in &rounds {
        if race_one(port, &mut a, &mut b, round, &sha) {
            opened[round.owner] += 1;
        }
    }
    eprintln!("in-window rounds per owner shard: {opened:?}");
    // Every owner has at least one stalled shard that is neither A's nor B's,
    // so every owner — A's own shard included — must have raced at least
    // once. Otherwise the host was too loaded to open the window and a green
    // result would mean nothing.
    assert!(
        opened.iter().all(|&n| n > 0),
        "some owner shard never raced B's write inside the stall \
         (in-window rounds per owner: {opened:?}); the local EXEC path may not \
         have been exercised"
    );
    rounds
}

#[test]
fn exec_with_waiting_intercept_logs_in_applied_order() {
    let dir = common::unique_test_dir("moon-1084-exec");
    let (mut server, port) = spawn(&dir, SHARDS);
    let rounds = race_all(port);

    // appendfsync always: every reply above was sent after its fsync.
    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir, SHARDS);

    let mut c = common::Conn::open(port);
    let wrong: Vec<String> = rounds
        .iter()
        .filter_map(|r| {
            let got = c.send(&["GET", &r.key]);
            (got != bulk("5")).then(|| {
                format!(
                    "{} (owner {}, stalled {}): acknowledged \"5\", recovered {got:?}",
                    r.key, r.owner, r.stalled
                )
            })
        })
        .collect();
    assert!(
        wrong.is_empty(),
        "recovery replayed the EXEC body after a write that was applied after \
         it (moon#1084):\n{wrong:#?}\n--- server log ---\n{}",
        server_log(&dir)
    );
    drop(c);
    restarted.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The replication stream has the same ordering contract as the AOF: a replica
/// applies the master's records in stream order, so a body recorded after the
/// waiting intercept reached the replica after the other client's write and
/// the replica settled on `6` while the master held `5`.
///
/// monoio only: it is the runtime with master-side fan-out. `#[ignore]`d like
/// every replication suite (two live servers):
///
/// ```text
/// MOON_BIN=... cargo test --test exec_intercept_log_order_1084 -- --ignored
/// ```
#[cfg(feature = "runtime-monoio")]
#[test]
#[ignore]
fn exec_with_waiting_intercept_replicates_in_applied_order() {
    let mdir = common::unique_test_dir("moon-1084-master");
    let rdir = common::unique_test_dir("moon-1084-replica");
    let (mut master, mport) = spawn(&mdir, SHARDS);
    let (mut replica, rport) = spawn(&rdir, 1);

    let mut r = common::Conn::open(rport);
    assert!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()])
            .starts_with("+OK"),
        "REPLICAOF refused"
    );
    let up_deadline = Instant::now() + Duration::from_secs(20);
    while !r
        .send(&["INFO", "replication"])
        .contains("master_link_status:up")
    {
        assert!(Instant::now() < up_deadline, "replica link never came up");
        std::thread::sleep(Duration::from_millis(100));
    }

    let rounds = race_all(mport);
    // A fence written after every race. The per-key poll below tolerates a
    // stream that is merely behind; the fence bounds how long that can be.
    let mut a = common::Conn::open(mport);
    assert_eq!(a.send(&["SET", "fence1084", "end"]), "+OK\r\n");
    let fence_deadline = Instant::now() + Duration::from_secs(20);
    while r.send(&["GET", "fence1084"]) != bulk("end") {
        assert!(
            Instant::now() < fence_deadline,
            "fence never reached the replica"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut wrong = Vec::new();
    for round in &rounds {
        let k = round.key.as_str();
        let deadline = Instant::now() + Duration::from_secs(5);
        let got = loop {
            let got = r.send(&["GET", k]);
            if got == bulk("5") || Instant::now() >= deadline {
                break got;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        if got != bulk("5") {
            wrong.push(format!(
                "{k} (owner {}, stalled {}): master acknowledged \"5\", replica holds {got:?}",
                round.owner, round.stalled
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "the replica applied the EXEC body after a write the master applied \
         after it (moon#1084):\n{wrong:#?}"
    );
    replica.kill_now();
    master.kill_now();
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}
