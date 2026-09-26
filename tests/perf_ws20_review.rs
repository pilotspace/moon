//! WS20 review round: real-server regression tests cheap enough for every PR.
//!
//! - moon#1275: a SWAPDB made a tokio `--shards 1` restart skip the AOF.
//!
//! Pin the binary for a specific runtime:
//! `MOON_BIN=<moon> cargo test --test perf_ws20_review`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;

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

fn bulk(reply: &str) -> Option<String> {
    if reply.starts_with("$-1") {
        return None;
    }
    reply.split("\r\n").nth(1).map(str::to_string)
}

/// moon#1275: on the tokio `--shards 1` layout the SWAPDB local leg wrote its
/// record to WAL v3 whatever `--wal-kv-log` said. That record was then the
/// only KV record in the WAL, so recovery took the WAL for the KV authority
/// and never replayed `appendonly.aof`: every key written before AND after
/// the swap was gone after a kill -9. Correct on every layout: each key is
/// back in the database the swap put it in.
fn swapdb_survives_a_crash(shards: usize, offload: &str) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let args = [
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
        "--disk-offload",
        offload,
    ];
    let (mut server, port) = spawn(dir, shards, &args);
    let mut c = Conn::open(port);
    assert!(c.send(&["SET", "a", "1"]).starts_with("+OK"));
    assert_eq!(c.send(&["SWAPDB", "0", "1"]), "+OK\r\n");
    assert!(c.send(&["SET", "b", "2"]).starts_with("+OK"));
    assert!(c.send(&["SELECT", "1"]).starts_with("+OK"));
    assert!(c.send(&["SET", "c", "3"]).starts_with("+OK"));
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);

    let (_server2, port) = spawn(dir, shards, &args);
    let mut c = Conn::open(port);
    let db0 = (bulk(&c.send(&["GET", "a"])), bulk(&c.send(&["GET", "b"])));
    assert!(c.send(&["SELECT", "1"]).starts_with("+OK"));
    let db1 = (bulk(&c.send(&["GET", "a"])), bulk(&c.send(&["GET", "c"])));
    assert_eq!(
        (db0, db1),
        (
            (None, Some("2".to_owned())),
            (Some("1".to_owned()), Some("3".to_owned()))
        ),
        "after SET a; SWAPDB 0 1; SET b (db 0); SET c (db 1); kill -9; restart at \
         --shards {shards}, offload {offload}: ((db0 a, db0 b), (db1 a, db1 c))"
    );
}

#[test]
fn moon_1275_a_swapdb_does_not_make_a_restart_skip_the_aof_s1() {
    swapdb_survives_a_crash(1, "enable");
    swapdb_survives_a_crash(1, "disable");
}

#[test]
fn moon_1275_a_swapdb_does_not_make_a_restart_skip_the_aof_s4() {
    swapdb_survives_a_crash(4, "enable");
}
