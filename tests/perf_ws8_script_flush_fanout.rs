//! moon#1229, end to end: `SCRIPT FLUSH` must empty the script cache of
//! EVERY shard before it replies, so an `EVALSHA` whose keys route to any
//! shard answers `NOSCRIPT` and `SCRIPT EXISTS` answers 0 from every
//! connection — at `--shards 2` and `--shards 4`, for the bare form and for
//! `SYNC`/`ASYNC`. A mode redis refuses is refused with redis's text.
//!
//! Red on `ae21476` (`MOON_BIN=/home/user/wt/bin/baseline-ae21476`): the flush
//! reached only the connection's shard, so EVALSHA kept running for the keys
//! owned by the other shards.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_script_flush_fanout`.

#![allow(clippy::unwrap_used)]

mod common;

use common::{Conn, ServerGuard};

const SCRIPT: &str = "return redis.call('GET', KEYS[1])";
const KEYS: usize = 48;

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
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
                &shards.to_string(),
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
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn bulk_body(reply: &str) -> &str {
    reply.split("\r\n").nth(1).unwrap_or("")
}

/// EVALSHA over keys that cover every shard; returns the keys whose call
/// still RAN (did not answer NOSCRIPT).
fn still_running(c: &mut Conn, sha: &str) -> Vec<String> {
    let mut ran = Vec::new();
    for i in 0..KEYS {
        let key = format!("sf:{i}");
        let r = c.send(&["EVALSHA", sha, "1", &key]);
        if !r.starts_with("-NOSCRIPT") {
            ran.push(format!("{key} -> {}", r.trim_end()));
        }
    }
    ran
}

fn check(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws8-script-flush-s{shards}"));
    let (guard, port) = spawn(&dir, shards);
    let owners: std::collections::BTreeSet<usize> = (0..KEYS)
        .map(|i| moon::shard::dispatch::key_to_shard(format!("sf:{i}").as_bytes(), shards))
        .collect();
    assert_eq!(owners.len(), shards, "the keys must cover every shard");

    let mut c = Conn::open(port);
    for flush in [
        &["SCRIPT", "FLUSH"][..],
        &["SCRIPT", "FLUSH", "SYNC"],
        &["SCRIPT", "FLUSH", "ASYNC"],
    ] {
        let sha = bulk_body(&c.send(&["SCRIPT", "LOAD", SCRIPT])).to_string();
        assert_eq!(sha.len(), 40, "SCRIPT LOAD");
        assert_eq!(
            still_running(&mut c, &sha).len(),
            KEYS,
            "before {flush:?}: the loaded script runs for every key"
        );
        assert_eq!(c.send(flush), "+OK\r\n", "{flush:?}");
        let ran = still_running(&mut c, &sha);
        assert!(
            ran.is_empty(),
            "--shards {shards}: after {flush:?}, EVALSHA still ran for {} of {KEYS} keys: {ran:?}",
            ran.len()
        );
        // Every connection, whichever shard it lands on, agrees.
        for n in 0..(4 * shards) {
            let mut other = Conn::open(port);
            assert_eq!(
                other.send(&["SCRIPT", "EXISTS", &sha]),
                "*1\r\n:0\r\n",
                "--shards {shards}: SCRIPT EXISTS after {flush:?} on connection {n}"
            );
        }
    }

    // A mode redis refuses: refused with redis's text, and nothing flushed.
    let sha = bulk_body(&c.send(&["SCRIPT", "LOAD", SCRIPT])).to_string();
    assert_eq!(
        c.send(&["SCRIPT", "FLUSH", "BOGUS"]),
        "-ERR SCRIPT FLUSH only support SYNC|ASYNC option\r\n"
    );
    assert_eq!(
        still_running(&mut c, &sha).len(),
        KEYS,
        "a refused flush flushes nothing"
    );

    drop(c);
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn script_flush_reaches_every_shard_at_shards_2() {
    check(2);
}

#[test]
fn script_flush_reaches_every_shard_at_shards_4() {
    check(4);
}
