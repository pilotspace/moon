//! A keyspace event names the database the command RAN in.
//!
//! `Database::db_index` is the number `__keyspace@<db>__` / `__keyevent@<db>__`
//! carry, and command code reads it from the database it was handed. Two
//! places replaced a database's CONTENTS and took the number along:
//!
//! - `SWAPDB` (`DbPlane::swap`, AOF replay, the single-listener handler):
//!   after `SWAPDB 0 3`, a `DEL` in db 0 published `__keyevent@3__:del`. redis
//!   7.0.15 publishes `@0`. Part 3b memory review S2; the root cause predates
//!   part 3b (`set` events at `--shards 4` were wrong the same way on
//!   ae21476).
//! - `rdb::load` (`*live = temp`, a fresh `Database::new()` whose index is 0):
//!   after a restart from an AOF with an RDB base, every event from db 3
//!   named db 0.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test keyspace_event_db_index`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

fn spawn(dir: &Path, shards: usize, extra: &[&str]) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let d = dir.to_path_buf();
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--disk-offload",
                "disable",
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

/// A subscriber on every keyevent channel of every db.
fn keyevent_subscriber(port: u16) -> Conn {
    let mut sub = Conn::open(port);
    let ack = sub.send(&["PSUBSCRIBE", "__keyevent@*__:*"]);
    assert!(ack.contains("psubscribe"), "PSUBSCRIBE ack: {ack:?}");
    sub
}

/// Read `n` pmessages; returns `channel -> [key]`, each list in arrival order.
fn events(sub: &mut Conn, n: usize) -> BTreeMap<String, Vec<String>> {
    let raw = sub.read_replies_within(n, Duration::from_secs(10));
    // `*4 $8 pmessage $<len> <pattern> $<len> <channel> $<len> <key>`
    let lines: Vec<&str> = raw.split("\r\n").collect();
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut i = 0;
    while i + 8 < lines.len() {
        if lines[i] == "*4" && lines[i + 2] == "pmessage" {
            out.entry(lines[i + 6].to_string())
                .or_default()
                .push(lines[i + 8].to_string());
            i += 9;
        } else {
            i += 1;
        }
    }
    out
}

fn swapdb_events_name_the_db_the_command_ran_in(shards: usize) {
    let dir = common::unique_test_dir(&format!("keyevent-db-swapdb-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (guard, port) = spawn(&dir, shards, &["--appendonly", "no", "--save", ""]);
    let mut c = Conn::open(port);
    assert_eq!(
        c.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        "+OK\r\n"
    );
    let mut sub = keyevent_subscriber(port);

    // Two commands that name their event from the DATABASE they were handed
    // (`db.db_index`): INCR (`incrby`) and DEL. A plain SET is no probe: the
    // inline fast path names it from the connection's selected db.
    assert_eq!(c.send(&["SWAPDB", "0", "3"]), "+OK\r\n");
    assert_eq!(c.send(&["INCR", "b"]), ":1\r\n");
    assert_eq!(c.send(&["DEL", "b"]), ":1\r\n");
    assert_eq!(c.send(&["SELECT", "3"]), "+OK\r\n");
    assert_eq!(c.send(&["INCR", "c"]), ":1\r\n");
    assert_eq!(c.send(&["DEL", "c"]), ":1\r\n");
    // And back: both slots keep their numbers through a second swap.
    assert_eq!(c.send(&["SWAPDB", "3", "0"]), "+OK\r\n");
    assert_eq!(c.send(&["INCR", "d"]), ":1\r\n");
    assert_eq!(c.send(&["DEL", "d"]), ":1\r\n");

    let got = events(&mut sub, 6);
    let want: BTreeMap<String, Vec<String>> = [
        ("__keyevent@0__:incrby", vec!["b"]),
        ("__keyevent@0__:del", vec!["b"]),
        ("__keyevent@3__:incrby", vec!["c", "d"]),
        ("__keyevent@3__:del", vec!["c", "d"]),
    ]
    .into_iter()
    .map(|(ch, keys)| (ch.to_string(), keys.into_iter().map(String::from).collect()))
    .collect();
    assert_eq!(
        got, want,
        "--shards {shards}: after SWAPDB the events name the db the command \
         ran in (redis 7.0.15)"
    );
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn swapdb_events_name_the_db_the_command_ran_in_shards_1() {
    swapdb_events_name_the_db_the_command_ran_in(1);
}

#[test]
fn swapdb_events_name_the_db_the_command_ran_in_shards_4() {
    swapdb_events_name_the_db_the_command_ran_in(4);
}

/// How many rewrites have installed a base: the multi-part manifest's `seq`
/// line, or — on tokio `--shards 1`, which keeps the legacy single
/// `appendonly.aof` and no manifest — 1 once that file starts with a
/// snapshot preamble (moon's `MOON` format, or redis's `REDIS`), else 0.
fn rewrite_generation(dir: &Path) -> u64 {
    if let Ok(m) = std::fs::read_to_string(dir.join("appendonlydir").join("moon.aof.manifest")) {
        return m
            .lines()
            .find_map(|l| l.strip_prefix("seq "))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
    }
    let legacy = std::fs::read(dir.join("appendonly.aof")).unwrap_or_default();
    u64::from(legacy.starts_with(b"MOON") || legacy.starts_with(b"REDIS"))
}

/// Poll until the rewrite started after `before` was read has FINISHED: none
/// in progress, and a newer base installed. Waiting only for "none
/// in progress" can pass before a slow rewrite has even started.
fn wait_rewrite_done(c: &mut Conn, dir: &Path, before: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = c.send(&["INFO", "persistence"]);
        let idle = info.contains("aof_rewrite_in_progress:0");
        if idle && rewrite_generation(dir) > before {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "AOF rewrite never finished: {info}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn events_after_a_restart_from_an_aof_base(shards: usize) {
    let dir = common::unique_test_dir(&format!("keyevent-db-restart-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let args = [
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
        "--save",
        "",
    ];
    let (mut guard, port) = spawn(&dir, shards, &args);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SELECT", "3"]), "+OK\r\n");
    assert_eq!(c.send(&["SET", "seed", "v"]), "+OK\r\n");
    let before = rewrite_generation(&dir);
    let reply = c.send(&["BGREWRITEAOF"]);
    assert!(reply.starts_with('+'), "BGREWRITEAOF: {reply:?}");
    wait_rewrite_done(&mut c, &dir, before);
    drop(c);
    guard.kill_now();

    // The seed now lives in the AOF's RDB base, loaded by `rdb::load`.
    let (guard, port) = spawn(&dir, shards, &args);
    let mut c = Conn::open(port);
    assert_eq!(
        c.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        "+OK\r\n"
    );
    let mut sub = keyevent_subscriber(port);
    assert_eq!(c.send(&["SELECT", "3"]), "+OK\r\n");
    assert_eq!(
        c.send(&["GET", "seed"]),
        "$1\r\nv\r\n",
        "the base was loaded"
    );
    assert_eq!(c.send(&["DEL", "seed"]), ":1\r\n");
    let got = events(&mut sub, 1);
    assert_eq!(
        got.get("__keyevent@3__:del"),
        Some(&vec!["seed".to_string()]),
        "--shards {shards}: after a restart from an AOF base, DEL in db 3 \
         published {got:?}"
    );
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn events_name_the_right_db_after_a_restart_from_an_aof_base_shards_1() {
    events_after_a_restart_from_an_aof_base(1);
}

#[test]
fn events_name_the_right_db_after_a_restart_from_an_aof_base_shards_4() {
    events_after_a_restart_from_an_aof_base(4);
}
