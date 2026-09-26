//! moon#1264 (b) against a real server: with save points, `FLUSHALL` saves
//! the empty dataset before it replies, as redis's `flushallCommand` does
//! (`flushAllDataAndResetRDB`: a synchronous `rdbSave` whenever save points
//! are configured).
//!
//! Before the fix moon cleared the keyspace and left the previous snapshot
//! on disk until a save rule fired: a crash in between (here a SIGKILL right
//! after the reply) restored every flushed key. Covered: a plain `FLUSHALL`
//! (and `ASYNC`), one inside `MULTI`/`EXEC`, and one a script issues, at
//! `--shards 1` and `4`. Without save points `FLUSHALL` saves nothing, as in
//! redis: the previous snapshot comes back.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws21_flushall_save`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

const KEYS: usize = 200;

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

fn info_field(c: &mut Conn, field: &str) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

fn dbsize(c: &mut Conn) -> usize {
    let reply = c.send(&["DBSIZE"]);
    reply
        .trim()
        .strip_prefix(':')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("DBSIZE: {reply:?}"))
}

/// Fill `KEYS` keys and snapshot them, so a restart without a later save
/// brings them back.
fn fill_and_save(c: &mut Conn) {
    let mut out = Vec::new();
    for i in 0..KEYS {
        out.extend_from_slice(&encode(&["SET", &format!("f:{i}"), "v"]));
    }
    c.sock.write_all(&out).unwrap();
    let reply = c.read_replies(KEYS);
    assert!(!reply.contains('-'), "SET refused: {reply:.200}");
    assert!(
        c.send(&["BGSAVE"])
            .starts_with("+Background saving started")
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while info_field(c, "rdb_bgsave_in_progress") != "0" {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(info_field(c, "rdb_last_bgsave_status"), "ok");
    assert_eq!(dbsize(c), KEYS);
}

/// How the flush is issued.
#[derive(Clone, Copy, Debug)]
enum Flush {
    Plain,
    Async,
    Exec,
    Script,
}

fn flush(c: &mut Conn, how: Flush) {
    match how {
        Flush::Plain => assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n"),
        Flush::Async => assert_eq!(c.send(&["FLUSHALL", "ASYNC"]), "+OK\r\n"),
        Flush::Exec => {
            let reply = c.pipeline(&[&["MULTI"], &["FLUSHALL"], &["EXEC"]]);
            assert_eq!(
                reply, "+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n",
                "MULTI/FLUSHALL/EXEC"
            );
        }
        Flush::Script => {
            let reply = c.send(&["EVAL", "return redis.call('FLUSHALL')", "0"]);
            assert_eq!(reply, "+OK\r\n", "EVAL FLUSHALL");
        }
    }
}

/// Flush, SIGKILL at once, restart: how many keys came back, and INFO's
/// `rdb_changes_since_last_save` right after the flush.
fn keys_after_flush_and_crash(shards: usize, save: Option<&str>, how: Flush) -> (usize, String) {
    let dir = common::unique_test_dir(&format!("ws21-1264b-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, save);
    let mut c = Conn::open(port);
    fill_and_save(&mut c);
    flush(&mut c, how);
    let changes = info_field(&mut c, "rdb_changes_since_last_save");
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);

    let (_server, port) = spawn(&dir, shards, save);
    let mut c = Conn::open(port);
    let back = dbsize(&mut c);
    drop(c);
    drop(_server);
    let _ = std::fs::remove_dir_all(&dir);
    (back, changes)
}

fn flushall_saves_before_replying(shards: usize, how: Flush) {
    let (back, changes) = keys_after_flush_and_crash(shards, Some(RULES), how);
    assert_eq!(
        back, 0,
        "--shards {shards}, {how:?} with save points: {back} of {KEYS} flushed keys came back \
         after a crash right after the reply"
    );
    // redis 7.0.15 after FLUSHALL with save points: 0 (its save reset it).
    assert_eq!(changes, "0", "rdb_changes_since_last_save after the flush");
}

#[test]
fn flushall_saves_before_replying_single_shard() {
    flushall_saves_before_replying(1, Flush::Plain);
}

#[test]
fn flushall_saves_before_replying_four_shards() {
    flushall_saves_before_replying(4, Flush::Plain);
}

#[test]
fn flushall_async_saves_before_replying_four_shards() {
    flushall_saves_before_replying(4, Flush::Async);
}

#[test]
fn flushall_in_exec_saves_before_replying_single_shard() {
    flushall_saves_before_replying(1, Flush::Exec);
}

#[test]
fn flushall_in_exec_saves_before_replying_four_shards() {
    flushall_saves_before_replying(4, Flush::Exec);
}

#[test]
fn flushall_in_a_script_saves_before_replying_single_shard() {
    flushall_saves_before_replying(1, Flush::Script);
}

#[test]
fn flushall_in_a_script_saves_before_replying_four_shards() {
    flushall_saves_before_replying(4, Flush::Script);
}

/// No save points: FLUSHALL saves nothing (redis parity), so the snapshot
/// taken before it comes back after a crash.
#[test]
fn flushall_without_save_points_does_not_save() {
    let (back, _) = keys_after_flush_and_crash(4, None, Flush::Plain);
    assert_eq!(
        back, KEYS,
        "a FLUSHALL without save points wrote a snapshot"
    );
}
