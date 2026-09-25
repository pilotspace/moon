//! moon#1183, end to end at `--shards 4`: `WATCH` of keys owned by other
//! shards reads their versions through the L4 foreign-read fast path instead
//! of one sequential SPSC round trip per owner — and the optimistic
//! transaction it guards still aborts on a conflicting write from another
//! connection, and still commits when nothing changed.
//!
//! The hop count is read from `INFO stats` `spsc_notify_wakes` (a shard loop
//! woken by a cross-shard push). The server runs with
//! `--cross-shard-fast-path on`, the default (`auto`) on a monoio build. Red on `ae21476`
//! (`MOON_BIN=/home/user/wt/bin/baseline-ae21476`): every WATCH of three
//! remote owners wakes three owner loops.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_watch_versions`.

#![allow(clippy::unwrap_used)]

mod common;

use common::{Conn, ServerGuard};

const SHARDS: usize = 4;
const ROUNDS: u64 = 200;

fn spawn(dir: &std::path::Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &SHARDS.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
                // Explicit, not `auto`: `auto` enables the foreign-read fast
                // path only on the monoio build (the tokio handler's reads do
                // not use it). WATCH's version read does on either runtime
                // once the switch is on, and this test is about that read.
                "--cross-shard-fast-path",
                "on",
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn owner(key: &str) -> usize {
    moon::shard::dispatch::key_to_shard(key.as_bytes(), SHARDS)
}

/// A key owned by `shard`, distinct per `tag`.
fn key_on(shard: usize, tag: &str) -> String {
    (0..)
        .map(|i| format!("{tag}:{i}"))
        .find(|k| owner(k) == shard)
        .unwrap()
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    c.send(&["INFO", "stats"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO stats has no {field}"))
}

/// One key per shard: whichever shard this connection landed on (the kernel
/// picks it), three of them are remote owners.
fn keys_on_every_shard(tag: &str) -> Vec<String> {
    (0..SHARDS).map(|s| key_on(s, tag)).collect()
}

#[test]
fn watch_of_remote_keys_takes_no_spsc_hop_and_keeps_cas_semantics() {
    let dir = common::unique_test_dir("ws8-watch");
    let (_guard, port) = spawn(&dir);
    let mut c = Conn::open(port);
    let keys = keys_on_every_shard("w");
    for k in &keys {
        assert_eq!(c.send(&["SET", k, "0"]), "+OK\r\n");
    }
    let mut watch: Vec<&str> = vec!["WATCH"];
    watch.extend(keys.iter().map(String::as_str));

    // Hops: a WATCH of one key per shard names three remote owners whatever
    // shard this connection landed on.
    let before = info_u64(&mut c, "spsc_notify_wakes");
    for _ in 0..ROUNDS {
        assert_eq!(c.send(&watch), "+OK\r\n");
        assert_eq!(c.send(&["UNWATCH"]), "+OK\r\n");
    }
    let wakes = info_u64(&mut c, "spsc_notify_wakes") - before;
    eprintln!("{ROUNDS} WATCH+UNWATCH rounds: {wakes} owner-loop wakes");
    assert!(
        wakes < ROUNDS / 2,
        "{ROUNDS} WATCHes of {} remote owners woke owner shard loops {wakes} times — \
         the versions took SPSC round trips instead of the foreign-read fast path",
        SHARDS - 1
    );

    // CAS semantics through the fast path, one key per shard (a watch set
    // spanning shards is refused at EXEC — the body commits on one owner):
    // a conflicting write from another connection aborts EXEC, and an
    // untouched watch commits.
    let mut other = Conn::open(port);
    for k in &keys {
        assert_eq!(c.send(&["WATCH", k]), "+OK\r\n");
        assert_eq!(other.send(&["INCR", k]), ":1\r\n");
        assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
        assert_eq!(c.send(&["SET", k, "tx"]), "+QUEUED\r\n");
        assert_eq!(
            c.send(&["EXEC"]),
            "*-1\r\n",
            "EXEC must abort after another connection wrote watched key {k} (shard {})",
            owner(k)
        );
        assert_eq!(c.send(&["WATCH", k]), "+OK\r\n");
        assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
        assert_eq!(c.send(&["SET", k, "tx"]), "+QUEUED\r\n");
        assert_eq!(
            c.send(&["EXEC"]),
            "*1\r\n+OK\r\n",
            "an untouched watch on {k} (shard {}) commits",
            owner(k)
        );
        assert_eq!(c.send(&["GET", k]), "$2\r\ntx\r\n");
    }

    drop(c);
    drop(other);
    drop(_guard);
    let _ = std::fs::remove_dir_all(&dir);
}
