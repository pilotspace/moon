//! moon#1228, end to end: writers that change a key while a BGSAVE epoch is
//! still writing it must leave the snapshot holding the EPOCH-START keyspace.
//! Each test restores the snapshot ALONE (`--appendonly no`, SIGKILL, restart)
//! and compares it with the keyspace the save started from.
//!
//! The overlap between the save and the writes is observed, never assumed
//! (the `perf_ws8_mset_bgsave_capture` method): an attempt waits until every
//! shard has armed the epoch (its `shard-<id>.rrdshard.tmp` exists), pipelines
//! each round of writes with an `INFO persistence` probe, and counts the rounds
//! that ran while the save was still in progress. An attempt with too few is
//! retried on a doubled keyspace; only an attempt that overlapped is judged.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws16_bgsave_capture`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

fn spawn(dir: &Path, shards: usize) -> (ServerGuard, u16) {
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

fn key(j: u64) -> String {
    format!("pre:{j:08}")
}

/// Empty every database, then set `pre:<j>` = `v<j>` for `j` in `0..n` in
/// db 0: the keyspace the next BGSAVE must capture.
fn reset(c: &mut Conn, n: u64) {
    assert!(c.send(&["FLUSHALL"]).starts_with("+OK"));
    let mut i = 0;
    while i < n {
        let end = (i + 1000).min(n);
        let mut out = Vec::new();
        for j in i..end {
            out.extend_from_slice(&encode(&["SET", &key(j), &format!("v{j}")]));
        }
        c.sock.write_all(&out).unwrap();
        let reply = c.read_replies((end - i) as usize);
        assert!(!reply.contains('-'), "preload refused: {reply:.200}");
        i = end;
    }
}

fn bgsave_in_progress(c: &mut Conn) -> bool {
    c.send(&["INFO", "persistence"])
        .lines()
        .any(|l| l.trim() == "rdb_bgsave_in_progress:1")
}

fn last_bgsave_status(c: &mut Conn) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix("rdb_last_bgsave_status:"))
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

/// Keys of db 0 whose restored value is not their epoch-start `v<j>`.
fn wrong_pre_keys(c: &mut Conn, n: u64) -> Vec<String> {
    assert!(c.send(&["SELECT", "0"]).starts_with("+OK"));
    let mut wrong = Vec::new();
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
        let mut out = Vec::new();
        for j in i..end {
            out.extend_from_slice(&encode(&["GET", &key(j)]));
        }
        c.sock.write_all(&out).unwrap();
        let reply = c.read_replies((end - i) as usize);
        let mut lines = reply.split("\r\n");
        for j in i..end {
            let head = lines.next().unwrap_or("");
            let body = if head == "$-1" {
                "<nil>"
            } else {
                lines.next().unwrap_or("")
            };
            if body != format!("v{j}") && wrong.len() < 10_000 {
                wrong.push(format!("{}={body}", key(j)));
            }
        }
        i = end;
    }
    wrong
}

fn dbsize(c: &mut Conn, db: usize) -> i64 {
    assert!(c.send(&["SELECT", &db.to_string()]).starts_with("+OK"));
    let reply = c.send(&["DBSIZE"]);
    reply
        .trim()
        .strip_prefix(':')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("DBSIZE: {reply:?}"))
}

/// Attempts before the test gives up on overlapping a save with the writes.
const MAX_ATTEMPTS: u32 = 5;
/// Keys in db 0 in the first attempt; each retry doubles it, up to 8x.
const FIRST_KEYS: u64 = 200_000;
/// Rounds of writes that must land inside one epoch for it to count.
const MIN_OVERLAP: u64 = 16;

/// Every shard armed the save's epoch: a shard creates its
/// `shard-<id>.rrdshard.tmp` in the same synchronous stretch of its thread
/// that arms its pre-image capture, and only that thread writes its keys.
fn wait_until_armed(dir: &Path, shards: usize, probe: &mut Conn) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if (0..shards).all(|s| dir.join(format!("shard-{s}.rrdshard.tmp")).exists()) {
            return true;
        }
        if !bgsave_in_progress(probe) {
            return false;
        }
        assert!(Instant::now() < deadline, "BGSAVE never armed every shard");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_bgsave_done(probe: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(300);
    while bgsave_in_progress(probe) {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// One round of writes: `(conn, n, round)` → the commands to pipeline. The
/// harness appends the `INFO persistence` probe and checks every reply is not
/// an error.
type Round<'a> = &'a mut dyn FnMut(u64, u64) -> Vec<Vec<String>>;

/// Run rounds of writes while a BGSAVE is in flight, retrying on a doubled
/// keyspace until [`MIN_OVERLAP`] rounds landed inside the epoch. Returns the
/// keyspace size and the overlapped round count of the judged attempt; the
/// file on disk is that attempt's (each save replaces it).
fn writes_during_bgsave(
    dir: &Path,
    shards: usize,
    c: &mut Conn,
    probe: &mut Conn,
    reset: &mut dyn FnMut(&mut Conn, u64),
    round: Round<'_>,
) -> (u64, u64) {
    let mut attempts: Vec<String> = Vec::new();
    for attempt in 1..=MAX_ATTEMPTS {
        let n = FIRST_KEYS << (attempt - 1).min(3);
        reset(c, n);
        for s in 0..shards {
            let _ = std::fs::remove_file(dir.join(format!("shard-{s}.rrdshard.tmp")));
        }
        let started = Instant::now();
        assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
        if !wait_until_armed(dir, shards, probe) {
            wait_bgsave_done(probe);
            attempts.push(format!("{n} keys: the save ended before every shard armed"));
            continue;
        }
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut during = 0u64;
        for r in 0.. {
            let cmds = round(n, r);
            let mut parts: Vec<Vec<&str>> = cmds
                .iter()
                .map(|c| c.iter().map(String::as_str).collect())
                .collect();
            parts.push(vec!["INFO", "persistence"]);
            let refs: Vec<&[&str]> = parts.iter().map(Vec::as_slice).collect();
            let reply = c.pipeline(&refs);
            assert!(
                !reply.lines().any(|l| l.starts_with('-')),
                "a write was refused: {reply:.300}"
            );
            if !reply
                .lines()
                .any(|l| l.trim() == "rdb_bgsave_in_progress:1")
            {
                break;
            }
            during += 1;
            assert!(Instant::now() < deadline, "BGSAVE outlived the writer");
        }
        assert_eq!(last_bgsave_status(probe), "ok", "BGSAVE failed");
        attempts.push(format!(
            "{n} keys: {during} rounds inside the epoch, save took {} ms",
            started.elapsed().as_millis()
        ));
        if during >= MIN_OVERLAP {
            eprintln!("attempts: {attempts:#?}");
            return (n, during);
        }
    }
    panic!("no BGSAVE in {MAX_ATTEMPTS} attempts overlapped {MIN_OVERLAP} rounds: {attempts:#?}");
}

fn cmd(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| (*p).to_string()).collect()
}

/// `MOVE` out of db 0 and `COPY … DB 2` while the save walks db 0: the
/// restored snapshot must hold every key in db 0 with its epoch-start value,
/// and nothing in db 1 or db 2. Before moon#1228 neither command captured a
/// pre-image on either database, so a moved key came back in db 1 (and, when
/// its db-0 range was already written, in db 0 too) and a copy came back in
/// db 2.
fn move_and_copy_during_bgsave(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws16-move-copy-s{shards}"));
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    let mut probe = Conn::open(port);
    const PER_ROUND: u64 = 16;
    let (n, during) =
        writes_during_bgsave(&dir, shards, &mut c, &mut probe, &mut reset, &mut |n, r| {
            let half = n / 2;
            let mut out = Vec::new();
            for i in 0..PER_ROUND {
                let j = (r * PER_ROUND + i) % half;
                out.push(cmd(&["MOVE", &key(j), "1"]));
                let k = half + j;
                out.push(cmd(&["COPY", &key(k), &key(k), "DB", "2"]));
            }
            out
        });

    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir, shards);
    let mut c2 = Conn::open(port2);
    let in_db1 = dbsize(&mut c2, 1);
    let in_db2 = dbsize(&mut c2, 2);
    let wrong = wrong_pre_keys(&mut c2, n);
    assert!(
        wrong.is_empty() && in_db1 == 0 && in_db2 == 0,
        "restored snapshot (MOVE/COPY in {during} rounds during the save, {n} keys, \
         --shards {shards}) is not the epoch-start keyspace: {} db-0 keys missing or wrong \
         (first: {:?}), db 1 holds {in_db1} keys, db 2 holds {in_db2} keys (both must be 0)",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn move_and_copy_during_bgsave_single_shard() {
    move_and_copy_during_bgsave(1);
}

#[test]
fn move_and_copy_during_bgsave_four_shards() {
    move_and_copy_during_bgsave(4);
}
