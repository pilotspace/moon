//! moon#1263 against a real server: SIGTERM / SIGINT with save points and
//! `--appendonly no` save the dataset before the server exits, as redis's
//! `prepareForShutdown` does for a plain `SHUTDOWN`.
//!
//! Before the fix both signals cancelled the server at once: every write
//! acknowledged since the last automatic save was lost on `systemctl stop`.
//!
//! - `SET`, SIGTERM (or SIGINT), restart: the key is there (`--shards 1` and
//!   `4`). Without save points the signal still exits at once, unsaved.
//! - SIGTERM while a BGSAVE runs (held by `MOON_TEST_SNAPSHOT_HOLD_FILE`): the
//!   server waits for it, then saves ONCE more — a key written after the
//!   BGSAVE began is in the final snapshot, and exactly two saves ran.
//! - The final save fails (a directory squats on the snapshot path): the
//!   server logs it and KEEPS RUNNING, as redis 7.0.15 does ("Error trying to
//!   save the DB, can't exit"); a SIGTERM once the path is fixed saves and
//!   exits 0.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws21_signal_save`.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

fn spawn(dir: &Path, shards: usize, save: Option<&str>) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let mut args: Vec<String> = [
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
            "--disk-offload",
            "disable",
            "--maxmemory",
            "0",
            "--disk-free-min-pct",
            "0",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if let Some(save) = save {
            args.extend(["--save".to_string(), save.to_string()]);
        }
        std::process::Command::new(&bin)
            .args(&args)
            .env("MOON_TEST_SNAPSHOT_HOLD_FILE", dir.join("snapshot.hold"))
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

/// A rule that never fires within a test.
const RULES: &str = "3600 1000000";

fn signal(server: &ServerGuard, sig: &str) {
    let status = std::process::Command::new("kill")
        .args([sig, &server.id().to_string()])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill {sig} failed: {status:?}");
}

fn wait_exit(server: &mut ServerGuard, budget: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = server.as_mut().try_wait().expect("try_wait") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn info_field(c: &mut Conn, field: &str) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

fn set_keys(c: &mut Conn, prefix: &str, n: usize) {
    for i in 0..n {
        let reply = c.send(&["SET", &format!("{prefix}:{i}"), &format!("{prefix}-{i}")]);
        assert_eq!(reply, "+OK\r\n");
    }
}

/// Keys `prefix:0..n` that are not back with their value.
fn missing(c: &mut Conn, prefix: &str, n: usize) -> usize {
    (0..n)
        .filter(|i| {
            let want = format!("{prefix}-{i}");
            c.send(&["GET", &format!("{prefix}:{i}")]) != format!("${}\r\n{want}\r\n", want.len())
        })
        .count()
}

fn server_log(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("server.err")).unwrap_or_default()
}

fn a_signal_saves_before_exiting(shards: usize, sig: &str) {
    let dir = common::unique_test_dir(&format!("ws21-1263-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, Some(RULES));
    let mut c = Conn::open(port);
    set_keys(&mut c, "k", 200);
    drop(c);

    signal(&server, sig);
    let status = wait_exit(&mut server, Duration::from_secs(60)).expect("never exited");
    assert!(status.success(), "{sig}: exit status {status:?}");
    common::wait_for_port_down(port);

    let (_server, port) = spawn(&dir, shards, Some(RULES));
    let mut c = Conn::open(port);
    let lost = missing(&mut c, "k", 200);
    assert_eq!(
        lost, 0,
        "--shards {shards}, {sig}: {lost} of 200 keys written before the signal were lost"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sigterm_saves_before_exiting_single_shard() {
    a_signal_saves_before_exiting(1, "-TERM");
}

#[test]
fn sigterm_saves_before_exiting_four_shards() {
    a_signal_saves_before_exiting(4, "-TERM");
}

#[test]
fn sigint_saves_before_exiting_four_shards() {
    a_signal_saves_before_exiting(4, "-INT");
}

/// No save points: a signal exits at once and saves nothing (unchanged).
#[test]
fn sigterm_without_save_points_does_not_save() {
    let dir = common::unique_test_dir("ws21-1263-nosave");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, 1, None);
    let mut c = Conn::open(port);
    set_keys(&mut c, "k", 10);
    drop(c);
    signal(&server, "-TERM");
    let status = wait_exit(&mut server, Duration::from_secs(60)).expect("never exited");
    assert!(status.success(), "exit status {status:?}");
    assert!(
        !dir.join("shard-0.rrdshard").exists(),
        "a SIGTERM without save points wrote a snapshot"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// SIGTERM during a running BGSAVE waits for it, then saves once more.
#[test]
fn sigterm_during_a_bgsave_waits_then_saves_once() {
    let shards = 4;
    let dir = common::unique_test_dir("ws21-1263-running");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, Some(RULES));
    let mut c = Conn::open(port);
    set_keys(&mut c, "early", 100);

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
    // Written after the running save began: only a later save holds them.
    set_keys(&mut c, "late", 100);

    signal(&server, "-TERM");
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        server.as_mut().try_wait().unwrap().is_none(),
        "the server exited while a save was still running"
    );
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
    assert_eq!(info_field(&mut c, "rdb_bgsave_in_progress"), "1");
    drop(c);

    std::fs::remove_file(dir.join("snapshot.hold")).unwrap();
    let status = wait_exit(&mut server, Duration::from_secs(60)).expect("never exited");
    assert!(status.success(), "exit status {status:?}");
    common::wait_for_port_down(port);
    let saves = server_log(&dir).matches("BGSAVE triggered: epoch").count();
    assert_eq!(saves, 2, "the BGSAVE and ONE final save should have run");

    let (_server, port) = spawn(&dir, shards, Some(RULES));
    let mut c = Conn::open(port);
    assert_eq!(missing(&mut c, "early", 100), 0);
    let lost = missing(&mut c, "late", 100);
    assert_eq!(
        lost, 0,
        "{lost} of 100 keys written during the BGSAVE were lost"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The final save fails: the server stays up (redis parity), and a SIGTERM
/// once the disk is fixed saves and exits.
#[test]
fn a_failed_final_save_keeps_the_server_running() {
    let dir = common::unique_test_dir("ws21-1263-fail");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, 1, Some(RULES));
    let mut c = Conn::open(port);
    set_keys(&mut c, "k", 50);
    // A directory where the snapshot's rename must land: the save fails.
    let squatter = dir.join("shard-0.rrdshard");
    std::fs::create_dir_all(squatter.join("x")).unwrap();

    signal(&server, "-TERM");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !server_log(&dir).contains("Errors trying to shut down the server") {
        assert!(
            server.as_mut().try_wait().unwrap().is_none(),
            "the server exited although its final save failed"
        );
        assert!(Instant::now() < deadline, "no shutdown-failure log line");
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        server.as_mut().try_wait().unwrap().is_none(),
        "the server exited although its final save failed"
    );
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "err");
    drop(c);

    std::fs::remove_dir_all(&squatter).unwrap();
    signal(&server, "-TERM");
    let status = wait_exit(&mut server, Duration::from_secs(60)).expect("never exited");
    assert!(status.success(), "exit status {status:?}");
    common::wait_for_port_down(port);

    let (_server, port) = spawn(&dir, 1, Some(RULES));
    let mut c = Conn::open(port);
    assert_eq!(missing(&mut c, "k", 50), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Review F4 (the reviewer's `review_ws21_sigterm_deadline`, kept): a
/// SIGTERM whose wait outlasts the stall limit — here a BGSAVE held for
/// longer than 20 s — logs redis's shutdown errors but is NOT dropped: once
/// the save completes, the final save runs and the server exits 0 with the
/// key on disk. redis has no deadline on a signal's final save.
///
/// Red before the fix: it gave up after 20.01 s, and the server was still
/// running 15 s after the held save completed.
#[test]
fn a_sigterm_outlasting_the_stall_limit_stays_armed() {
    let dir = common::unique_test_dir("ws21-f4-sigterm-stall");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, 1, Some(RULES));
    let mut c = Conn::open(port);
    set_keys(&mut c, "k", 20);
    std::fs::write(dir.join("snapshot.hold"), b"held").unwrap();
    assert!(
        c.send(&["BGSAVE"])
            .starts_with("+Background saving started")
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !dir.join("shard-0.rrdshard.tmp").exists() {
        assert!(Instant::now() < deadline, "the BGSAVE never armed");
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(c);

    let sigterm_at = Instant::now();
    signal(&server, "-TERM");
    while !server_log(&dir).contains("Errors trying to shut down the server") {
        assert!(
            server.as_mut().try_wait().unwrap().is_none(),
            "exited while the save was held"
        );
        assert!(
            sigterm_at.elapsed() < Duration::from_secs(40),
            "no stall report 40 s into a held save"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let reported = sigterm_at.elapsed();
    assert!(
        server_log(&dir).contains("The stop stays armed"),
        "the stall report does not say the stop stays armed"
    );

    // The slow save completes: the stop is still honoured.
    std::fs::remove_file(dir.join("snapshot.hold")).unwrap();
    let status = wait_exit(&mut server, Duration::from_secs(15)).unwrap_or_else(|| {
        panic!("the SIGTERM was dropped: reported a stall after {reported:?}, and 15 s after the save completed the server is still up")
    });
    assert!(status.success(), "exit status {status:?}");
    common::wait_for_port_down(port);

    let (_server, port) = spawn(&dir, 1, Some(RULES));
    let mut c = Conn::open(port);
    assert_eq!(missing(&mut c, "k", 20), 0);
    let _ = std::fs::remove_dir_all(&dir);
}
