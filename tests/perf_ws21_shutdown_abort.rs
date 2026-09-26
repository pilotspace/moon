//! moon#1264 (a) against a real server: `SHUTDOWN ABORT` cancels a
//! shutdown that is still saving, with redis 7.0.15's replies.
//!
//! A `SHUTDOWN` with save points waits for a save already running before it
//! saves and exits (moon#1232). redis 7 calls that window "a shutdown in
//! progress": `SHUTDOWN ABORT` from another client answers `+OK`, the waiting
//! client gets `-ERR Errors trying to SHUTDOWN. Check logs.`, and the server
//! stays up; with nothing in progress the answer is
//! `-ERR No shutdown in progress.`. moon always answered the latter (without
//! the period) and the shutdown went ahead.
//!
//! The window is made deterministic with a BGSAVE held by
//! `MOON_TEST_SNAPSHOT_HOLD_FILE`. A SIGTERM's final save (moon#1263) is
//! cancelled the same way. `shutdown_abort_oracle_redis_agrees` (ignored;
//! needs `redis-server` on PATH) re-measures the replies on redis 7.0.15,
//! where the window is a master waiting for a stopped replica.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws21_shutdown_abort`.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

const ABORTED: &str = "-ERR Errors trying to SHUTDOWN. Check logs.\r\n";
const NONE_IN_PROGRESS: &str = "-ERR No shutdown in progress.\r\n";

fn spawn(dir: &Path, shards: usize) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "3600 1000000",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .env("MOON_TEST_SNAPSHOT_HOLD_FILE", dir.join("snapshot.hold"))
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

fn info_field(c: &mut Conn, field: &str) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

fn alive(server: &mut ServerGuard) -> bool {
    server.as_mut().try_wait().unwrap().is_none()
}

fn wait_exit(server: &mut ServerGuard, budget: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = server.as_mut().try_wait().unwrap() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Start a BGSAVE whose walk is held, and wait until every shard armed it.
fn hold_a_save(dir: &Path, shards: usize, c: &mut Conn) {
    std::fs::write(dir.join("snapshot.hold"), b"held").unwrap();
    assert!(
        c.send(&["BGSAVE"])
            .starts_with("+Background saving started")
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !(0..shards).all(|s| dir.join(format!("shard-{s}.rrdshard.tmp")).exists()) {
        assert!(
            Instant::now() < deadline,
            "the BGSAVE never armed every shard"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Review F5: poll for the shutdown to be pending — observable as ABORT
/// answering `+OK` (until then it answers "No shutdown in progress." and
/// cancels nothing) — instead of assuming a fixed sleep covers it.
fn abort_once_pending(server: &mut ServerGuard, c: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            alive(server),
            "the shutdown did not wait for the running save"
        );
        match c.send(&["SHUTDOWN", "ABORT"]).as_str() {
            "+OK\r\n" => return,
            NONE_IN_PROGRESS => {}
            other => panic!("SHUTDOWN ABORT: {other:?}"),
        }
        assert!(
            Instant::now() < deadline,
            "the shutdown never became pending"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn release_the_save(dir: &Path, c: &mut Conn) {
    std::fs::remove_file(dir.join("snapshot.hold")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while info_field(c, "rdb_bgsave_in_progress") != "0" {
        assert!(Instant::now() < deadline, "the save never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn abort_cancels_a_waiting_shutdown(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws21-1264a-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards);
    let mut control = Conn::open(port);
    assert!(control.send(&["SET", "k", "v"]).starts_with("+OK"));
    hold_a_save(&dir, shards, &mut control);

    // Client A's SHUTDOWN waits for the held save.
    let mut waiting = Conn::open(port);
    waiting.sock.write_all(&encode(&["SHUTDOWN"])).unwrap();
    abort_once_pending(&mut server, &mut control);
    assert_eq!(waiting.read_replies(1), ABORTED);
    assert_eq!(control.send(&["SHUTDOWN", "ABORT"]), NONE_IN_PROGRESS);
    assert_eq!(control.send(&["PING"]), "+PONG\r\n");

    // The cancelled SHUTDOWN started no save: the held one finishes alone,
    // and the server keeps serving after it.
    release_the_save(&dir, &mut control);
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        alive(&mut server),
        "the aborted SHUTDOWN shut the server down"
    );
    assert_eq!(control.send(&["GET", "k"]), "$1\r\nv\r\n");

    // A later SHUTDOWN still saves and exits.
    control.sock.write_all(&encode(&["SHUTDOWN"])).unwrap();
    let status = wait_exit(&mut server, Duration::from_secs(60)).expect("never exited");
    assert!(status.success(), "{status:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn shutdown_abort_cancels_a_waiting_shutdown_single_shard() {
    abort_cancels_a_waiting_shutdown(1);
}

#[test]
fn shutdown_abort_cancels_a_waiting_shutdown_four_shards() {
    abort_cancels_a_waiting_shutdown(4);
}

/// A SIGTERM's final save (moon#1263) is a shutdown in progress too.
#[test]
fn shutdown_abort_cancels_a_sigterm_shutdown() {
    let shards = 4;
    let dir = common::unique_test_dir("ws21-1264a-sigterm");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards);
    let mut control = Conn::open(port);
    hold_a_save(&dir, shards, &mut control);

    let status = std::process::Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    abort_once_pending(&mut server, &mut control);

    release_the_save(&dir, &mut control);
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        alive(&mut server),
        "the aborted SIGTERM shutdown shut the server down"
    );
    assert_eq!(control.send(&["PING"]), "+PONG\r\n");
    control
        .sock
        .write_all(&encode(&["SHUTDOWN", "NOSAVE"]))
        .unwrap();
    wait_exit(&mut server, Duration::from_secs(60)).expect("never exited");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The same replies from redis-server 7.0.15 (the oracle), whose shutdown is
/// "in progress" while a master waits for a replica: the replica is stopped
/// with SIGSTOP so the master's SHUTDOWN waits.
#[test]
#[ignore = "needs redis-server on PATH"]
fn shutdown_abort_oracle_redis_agrees() {
    let dir = common::unique_test_dir("ws21-1264a-oracle");
    let (mdir, rdir) = (dir.join("master"), dir.join("replica"));
    std::fs::create_dir_all(&mdir).unwrap();
    std::fs::create_dir_all(&rdir).unwrap();
    let redis = |d: &Path, extra: &[String]| {
        let d = d.to_path_buf();
        let extra = extra.to_vec();
        move |port: u16| {
            std::process::Command::new("redis-server")
                .args([
                    "--port",
                    &port.to_string(),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                ])
                .args(["--dir", &d.to_string_lossy()])
                .args(&extra)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn redis-server")
        }
    };
    let (mut master, mport) = common::spawn_listening_guarded(redis(&mdir, &[]));
    let replica_of = [
        "--replicaof".to_string(),
        "127.0.0.1".to_string(),
        mport.to_string(),
    ];
    let (replica, rport) = common::spawn_listening_guarded(redis(&rdir, &replica_of));
    let mut m = Conn::open(mport);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !m
        .send(&["INFO", "replication"])
        .contains("connected_slaves:1")
    {
        assert!(Instant::now() < deadline, "the replica never connected");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(m.send(&["SHUTDOWN", "ABORT"]), NONE_IN_PROGRESS);
    let stop = |sig: &str| {
        std::process::Command::new("kill")
            .args([sig, &replica.id().to_string()])
            .status()
            .unwrap()
    };
    assert!(stop("-STOP").success());
    assert!(m.send(&["SET", "k", "v"]).starts_with("+OK"));
    let mut waiting = Conn::open(mport);
    waiting.sock.write_all(&encode(&["SHUTDOWN"])).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(m.send(&["SHUTDOWN", "ABORT"]), "+OK\r\n");
    assert_eq!(waiting.read_replies(1), ABORTED);
    assert_eq!(m.send(&["SHUTDOWN", "ABORT"]), NONE_IN_PROGRESS);
    assert!(alive(&mut master));
    assert!(stop("-CONT").success());
    drop((m, waiting, rport));
    let _ = std::fs::remove_dir_all(&dir);
}
