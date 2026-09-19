//! moon#1094: `aof_last_append_status` / `aof_last_write_status` return to
//! `ok` once a rewrite that postdates the last dropped append COMMITS.
//!
//! The status turns `err` when an acked append is dropped: the AOF lacks a
//! write the client was told about. Before this fix it never went back, even
//! after `BGREWRITEAOF` folded the live keyspace, the dropped write included,
//! into a fresh base, so it reported a hole that no longer existed for the
//! rest of the process's life.
//!
//! The drop is forced with the writer-side test hook the moon#838 suite uses:
//! `MOON_TEST_AOF_FSYNC_STALL_MS` holds the writer before each everysec
//! proactive fsync, and `--aof-fsync-timeout-ms 100` makes the generic leg
//! give up waiting long before the stall ends, so pipelined SETs are refused
//! and their records dropped.
//!
//! The third case in the issue (a drop that lands during the fold, after its
//! snapshot, keeps `err`) cannot be timed deterministically from outside the
//! process; `persistence::aof::rewrite_overflow` unit-tests it
//! (`a_drop_during_the_fold_survives_its_commit`).
//!
//! Runs at `--shards 1` (one writer) and `--shards 4`, where the status is the
//! AND across the per-shard writers.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

struct Server {
    _guard: common::ServerGuard,
    port: u16,
    _tmp: tempfile::TempDir,
}

fn spawn(shards: u32) -> Server {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_path_buf();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--aof-fsync-timeout-ms",
                "100",
                "--disk-free-min-pct",
                "0",
            ])
            .env("MOON_TEST_AOF_FSYNC_STALL_MS", "1500")
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    Server {
        _guard: guard,
        port,
        _tmp: tmp,
    }
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s
}

/// One command, one bulk/simple/error reply as text.
fn command(port: u16, parts: &[&str]) -> String {
    let mut s = connect(port);
    s.write_all(&common::encode(parts)).unwrap();
    let mut raw = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        let n = s.read(&mut chunk).expect("read reply");
        assert!(n > 0, "connection closed mid-reply");
        raw.extend_from_slice(&chunk[..n]);
        if let Some(len) = common::framed_len(&raw, 1) {
            return String::from_utf8_lossy(&raw[..len]).replace('\r', "");
        }
    }
}

fn info_field(port: u16, field: &str) -> String {
    let info = command(port, &["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(field).and_then(|v| v.strip_prefix(':')))
        .unwrap_or_else(|| panic!("INFO persistence has no {field}:\n{info}"))
        .to_string()
}

/// Pipelined SET bursts until at least one is refused. Returns how many were.
fn drop_some_appends(port: u16, tag: &str) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut refused = 0usize;
    let mut round = 0usize;
    while refused == 0 && Instant::now() < deadline {
        let mut s = connect(port);
        const N: usize = 20_000;
        let mut wire = Vec::with_capacity(N * 48);
        for i in 0..N {
            wire.extend_from_slice(&common::encode(&[
                "SET",
                &format!("{tag}:{round}:{i}"),
                "v",
            ]));
        }
        s.write_all(&wire).unwrap();
        let mut raw = Vec::new();
        let mut chunk = [0u8; 65536];
        while common::framed_len(&raw, N).is_none() {
            let n = s.read(&mut chunk).expect("read burst replies");
            assert!(n > 0, "server closed mid-burst");
            raw.extend_from_slice(&chunk[..n]);
        }
        refused += raw
            .split(|&b| b == b'\n')
            .filter(|l| l.first() == Some(&b'-'))
            .count();
        round += 1;
    }
    refused
}

fn wait_for_rewrite(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if info_field(port, "aof_rewrite_in_progress") == "0" {
            return;
        }
        assert!(Instant::now() < deadline, "BGREWRITEAOF never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn status_heals_after_a_covering_rewrite(shards: u32) {
    let srv = spawn(shards);
    assert_eq!(info_field(srv.port, "aof_last_append_status"), "ok");

    let refused = drop_some_appends(srv.port, "drop");
    assert!(
        refused > 0,
        "shards={shards}: the stall hook must drop at least one append, or this test proves nothing"
    );
    assert_eq!(
        info_field(srv.port, "aof_last_append_status"),
        "err",
        "shards={shards}: a dropped acked append must report err"
    );
    assert_eq!(info_field(srv.port, "aof_last_write_status"), "err");

    // No writes from here on, so nothing can be dropped after the snapshot.
    let reply = command(srv.port, &["BGREWRITEAOF"]);
    assert!(reply.starts_with('+'), "BGREWRITEAOF: {reply:?}");
    wait_for_rewrite(srv.port);
    assert_eq!(
        info_field(srv.port, "aof_last_bgrewrite_status"),
        "ok",
        "shards={shards}: the rewrite must have committed"
    );
    // With the PerShard layout the last writer to finish clears
    // `aof_rewrite_in_progress` when it commits the manifest, and each writer
    // adopts the commit (and heals) when it wakes from that barrier, a moment
    // later. Poll briefly for that, never for a new rewrite.
    let deadline = Instant::now() + Duration::from_secs(5);
    while info_field(srv.port, "aof_last_append_status") != "ok" && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        info_field(srv.port, "aof_last_append_status"),
        "ok",
        "shards={shards}: a committed rewrite after the last drop holds the dropped write; \
         the status must return to ok"
    );
    assert_eq!(info_field(srv.port, "aof_last_write_status"), "ok");
}

#[test]
fn append_status_heals_after_a_covering_rewrite_one_shard() {
    status_heals_after_a_covering_rewrite(1);
}

#[test]
fn append_status_heals_after_a_covering_rewrite_four_shards() {
    status_heals_after_a_covering_rewrite(4);
}
