//! moon#1084: an `EXEC` whose body holds a connection-level intercept must be
//! logged in the order it was APPLIED, not the order its reply was finished.
//!
//! The executor applies the body synchronously and leaves a placeholder for
//! each queued intercept (`WAIT`, `CLIENT`, `CONFIG`, `SCRIPT`, ...), which the
//! connection fills afterwards. Several of those fills await. The body's AOF
//! records (and, on monoio, its replication records) used to be appended only
//! AFTER that await, so a write another client made while `EXEC` was parked
//! was logged BEFORE the body although it was applied AFTER it:
//!
//! ```text
//! A: SET k 0
//! A: MULTI / INCR k / WAIT 1 <ms> / EXEC     (body applied, EXEC parks in WAIT)
//! B: SET k 5                                  -> +OK   (applied after INCR)
//! A: EXEC reply [1, 0]
//! B: GET k                                    -> "5"   (acknowledged state)
//! kill -9, restart
//! GET k                                       -> "6"   (INCR replayed after SET)
//! ```
//!
//! `WAIT n timeout` with more replicas asked for than exist parks for the whole
//! timeout, which gives a deterministic window for the second client's write —
//! no test hook is needed.
//!
//! Every server runs `--appendonly yes --appendfsync always`: each reply is
//! sent after its fsync, so the SIGKILL needs no settle time. At `--shards 4`
//! the scenario runs once per shard with a key owned by that shard, over ONE
//! connection, so exactly one iteration takes the connection's LOCAL `EXEC`
//! path (the one that was wrong) and the others take the owner-routed path,
//! which logs on the owner in the same stretch as the apply and must stay
//! right.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

/// How long the queued `WAIT` parks. Long enough that the second client's
/// write lands inside it on a loaded host, short enough to keep four
/// iterations cheap.
const WAIT_MS: u64 = 1_500;

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

/// One key per shard, so that whichever shard the connection landed on, one
/// of them is owned by it.
fn keys_covering(shards: usize) -> Vec<String> {
    let mut keys: Vec<Option<String>> = vec![None; shards];
    let mut i = 0u32;
    while keys.iter().any(Option::is_none) {
        let k = format!("k1084:{i}");
        let s = key_to_shard(k.as_bytes(), shards);
        if keys[s].is_none() {
            keys[s] = Some(k);
        }
        i += 1;
        assert!(i < 100_000, "could not find a key for every shard");
    }
    keys.into_iter().flatten().collect()
}

/// Run the race for one key. On return the server has acknowledged `k = 5`.
///
/// `attached` is the number of replicas the server has. The queued `WAIT` asks
/// for one more, so it parks for its whole timeout and then answers `attached`.
fn race_one(a: &mut common::Conn, b: &mut common::Conn, key: &str, attached: usize) {
    assert_eq!(a.send(&["SET", key, "0"]), "+OK\r\n");

    // MULTI / INCR / WAIT / EXEC in one write. The replies are read after the
    // second client has written, while EXEC is parked in WAIT.
    let wait_ms = WAIT_MS.to_string();
    let wait_replicas = (attached + 1).to_string();
    let mut batch = Vec::new();
    for parts in [
        &["MULTI"][..],
        &["INCR", key][..],
        &["WAIT", &wait_replicas, &wait_ms][..],
        &["EXEC"][..],
    ] {
        batch.extend_from_slice(&common::encode(parts));
    }
    let sent_at = Instant::now();
    a.sock.write_all(&batch).expect("write MULTI batch");

    // The body is applied before the intercept is filled: wait until it is
    // visible, so the write below is provably applied AFTER it.
    let deadline = sent_at + Duration::from_millis(WAIT_MS / 2);
    loop {
        if b.send(&["GET", key]) == bulk("1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{key}: the EXEC body never became visible while EXEC was parked"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(b.send(&["SET", key, "5"]), "+OK\r\n", "{key}: SET k 5");
    // The race only means something if the write really landed while EXEC
    // was still parked; otherwise the ordering was never in question.
    assert!(
        sent_at.elapsed() < Duration::from_millis(WAIT_MS * 9 / 10),
        "{key}: the second client's write was too slow to land inside WAIT \
         ({:?}); the scenario did not exercise the race",
        sent_at.elapsed()
    );

    let replies = a.read_replies(4);
    assert_eq!(
        replies,
        format!("+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n:{attached}\r\n"),
        "{key}: MULTI/INCR/WAIT/EXEC replies"
    );
    // And EXEC really was parked while it happened. Redis answers a WAIT
    // queued in MULTI at once; if moon ever does too, this scenario has no
    // window left and must move to another intercept that awaits, rather than
    // keep passing without testing anything.
    assert!(
        sent_at.elapsed() >= Duration::from_millis(WAIT_MS * 9 / 10),
        "{key}: EXEC answered after {:?}, so the queued WAIT did not park and \
         the second client's write did not race it",
        sent_at.elapsed()
    );
    assert_eq!(
        b.send(&["GET", key]),
        bulk("5"),
        "{key}: acknowledged value"
    );
}

fn scenario(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1084-s{shards}"));
    let (mut server, port) = spawn(&dir, shards);

    let keys = keys_covering(shards);
    {
        // ONE connection A for every iteration: its shard is fixed, so exactly
        // one key is local to it at --shards 4.
        let mut a = common::Conn::open(port);
        let mut b = common::Conn::open(port);
        for key in &keys {
            race_one(&mut a, &mut b, key, 0);
        }
    }

    // appendfsync always: every reply above was sent after its fsync.
    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir, shards);

    let mut c = common::Conn::open(port);
    let wrong: Vec<String> = keys
        .iter()
        .filter_map(|k| {
            let got = c.send(&["GET", k]);
            (got != bulk("5")).then(|| format!("{k}: acknowledged \"5\", recovered {got:?}"))
        })
        .collect();
    assert!(
        wrong.is_empty(),
        "--shards {shards}: recovery replayed the EXEC body after a write that \
         was applied after it (moon#1084):\n{wrong:#?}\n--- server log ---\n{}",
        server_log(&dir)
    );
    drop(c);
    restarted.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn exec_with_parked_intercept_logs_in_applied_order_1_shard() {
    scenario(1);
}

#[test]
fn exec_with_parked_intercept_logs_in_applied_order_4_shards() {
    scenario(4);
}

/// The replication stream has the same ordering contract as the AOF: a replica
/// applies the master's records in stream order, so a body recorded after the
/// parked intercept reached the replica after the other client's write and the
/// replica settled on `6` while the master held `5`.
///
/// monoio only: it is the runtime with master-side fan-out. `#[ignore]`d like
/// every replication suite (two live servers):
///
/// ```text
/// MOON_BIN=... cargo test --test exec_intercept_log_order_1084 -- --ignored
/// ```
#[cfg(feature = "runtime-monoio")]
fn replication_scenario(shards: usize) {
    let mdir = common::unique_test_dir(&format!("moon-1084-master-s{shards}"));
    let rdir = common::unique_test_dir(&format!("moon-1084-replica-s{shards}"));
    let (mut master, mport) = spawn(&mdir, shards);
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

    let keys = keys_covering(shards);
    let mut a = common::Conn::open(mport);
    let mut b = common::Conn::open(mport);
    for key in &keys {
        race_one(&mut a, &mut b, key, 1);
    }
    // A fence written after every race. The per-key poll below tolerates a
    // stream that is merely behind; the fence bounds how long that can be.
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
    for k in &keys {
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
                "{k}: master acknowledged \"5\", replica holds {got:?}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "--shards {shards}: the replica applied the EXEC body after a write the \
         master applied after it (moon#1084):\n{wrong:#?}"
    );
    replica.kill_now();
    master.kill_now();
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

#[cfg(feature = "runtime-monoio")]
#[test]
#[ignore]
fn exec_with_parked_intercept_replicates_in_applied_order_1_shard() {
    replication_scenario(1);
}

#[cfg(feature = "runtime-monoio")]
#[test]
#[ignore]
fn exec_with_parked_intercept_replicates_in_applied_order_4_shards() {
    replication_scenario(4);
}
