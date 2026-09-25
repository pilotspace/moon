//! A keyspace event names the database the command RAN in.
//!
//! `Database::db_index` is the number `__keyspace@<db>__` / `__keyevent@<db>__`
//! carry, and command code reads it from the database it was handed.
//! `SWAPDB` (`DbPlane::swap`, AOF replay, the single-listener handler)
//! replaced a database's CONTENTS and took the number along: after
//! `SWAPDB 0 3`, a `DEL` in db 0 published `__keyevent@3__:del`. redis 7.0.15
//! publishes `@0`. Part 3b memory review S2; the root cause predates part 3b
//! (`set` events at `--shards 4` were wrong the same way on ae21476).
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test keyspace_event_db_index`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

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
