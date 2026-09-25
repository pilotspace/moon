//! moon#1228 item 1, end to end (the WS12 pattern of
//! `perf_ws12_bgsave_split.rs`): spanning `MSET`s that overwrite keys while a
//! BGSAVE epoch is still writing them must leave the snapshot holding every
//! key's EPOCH-START value. The coordinator's local leg wrote through a
//! capture-free path, so on `ae21476` the restored snapshot holds post-epoch
//! values for keys owned by the connection's shard.
//!
//! The overlap between the save and the writes is observed, never assumed
//! (PR #1233 review: a fixed 30 ms sleep plus blocks of 16 MSETs failed 3 of
//! 4 debug runs because the save ended first). Each attempt waits until every
//! shard has armed the epoch, probes `rdb_bgsave_in_progress` after every
//! MSET, and is retried on a larger keyspace when too few MSETs landed inside
//! the epoch. Only an attempt that overlapped is restored and judged.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_mset_bgsave_capture`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

const SHARDS: usize = 4;

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

/// Set keys `pre:<j>` = `v<j>` for `j` in `from..to`, pipelined.
fn preload(c: &mut Conn, from: u64, to: u64) {
    let mut i = from;
    while i < to {
        let end = (i + 1000).min(to);
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

/// Keys whose restored value is not their epoch-start `v<j>`.
fn wrong_pre_keys(c: &mut Conn, n: u64) -> Vec<String> {
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

/// Attempts before the test gives up on overlapping a save with the writes.
const MAX_ATTEMPTS: u32 = 5;
/// Keys in the first attempt; each retry doubles it, up to [`MAX_KEYS`]
/// (400K, 800K, 1.6M, 3.2M, 3.2M: small `v<j>` values, ~60 MB of snapshot
/// per 1.6M keys).
const FIRST_KEYS: u64 = 100_000 * SHARDS as u64;
const MAX_KEYS: u64 = 8 * FIRST_KEYS;
/// Spanning MSETs that must land inside one epoch for it to count: 512
/// overwritten pairs, about 128 of them on the connection's own shard (the
/// capture this test exists for), as many as the original threshold.
const MIN_OVERLAP: u64 = 16;
/// Pairs per spanning MSET.
const PAIRS: u64 = 32;

/// Every shard armed the save's epoch. A shard spawns its snapshot writer,
/// which creates `shard-<id>.rrdshard.tmp`, in the same synchronous stretch
/// of the shard thread that arms its pre-image capture, and only that thread
/// writes its keys: once the file exists, every later write on that shard
/// runs after its arm point. `false`: the save ended before every shard was
/// seen armed (a shard published first), so this attempt cannot overlap.
fn wait_until_armed(dir: &std::path::Path, probe: &mut Conn) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if (0..SHARDS).all(|s| dir.join(format!("shard-{s}.rrdshard.tmp")).exists()) {
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

#[test]
fn spanning_mset_during_bgsave_keeps_epoch_start_values() {
    let dir = common::unique_test_dir("ws8-mset-bgsave");
    let (mut server, port) = spawn(&dir);
    let mut c = Conn::open(port);
    let mut probe = Conn::open(port);

    // Keys `0..n` hold `v<j>`, the value every key has when the next BGSAVE
    // starts; `dirty` keys were overwritten by an attempt that did not count
    // and are restored before the next one.
    let mut n = 0u64;
    let mut dirty = 0u64;
    let mut attempts: Vec<String> = Vec::new();
    let mut overlapped: Option<u64> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let target = (FIRST_KEYS << (attempt - 1)).min(MAX_KEYS);
        preload(&mut c, 0, dirty.min(n));
        preload(&mut c, n, target);
        n = target;
        // A `.tmp` left by an earlier save must not read as this one armed.
        for s in 0..SHARDS {
            let _ = std::fs::remove_file(dir.join(format!("shard-{s}.rrdshard.tmp")));
        }

        let started = Instant::now();
        assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
        if !wait_until_armed(&dir, &mut probe) {
            wait_bgsave_done(&mut probe);
            attempts.push(format!("{n} keys: the save ended before every shard armed"));
            dirty = 0;
            continue;
        }
        let armed_ms = started.elapsed().as_millis();

        // Overwrite with spanning MSETs until the epoch ends, probing after
        // every one: each MSET is pipelined with an `INFO persistence` on the
        // same connection, which runs only once the MSET has completed on
        // every owner. An MSET followed by a probe that still sees the save
        // running landed inside the epoch.
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut j = 0u64;
        let mut during = 0u64;
        loop {
            let mut cmd: Vec<String> = vec!["MSET".into()];
            for _ in 0..PAIRS {
                cmd.push(key(j % n));
                cmd.push("new".into());
                j += 1;
            }
            let parts: Vec<&str> = cmd.iter().map(String::as_str).collect();
            let reply = c.pipeline(&[parts.as_slice(), &["INFO", "persistence"]]);
            assert!(reply.starts_with("+OK\r\n"), "spanning MSET: {reply:.200}");
            if !reply
                .lines()
                .any(|l| l.trim() == "rdb_bgsave_in_progress:1")
            {
                break;
            }
            during += 1;
            assert!(Instant::now() < deadline, "BGSAVE outlived the writer");
        }
        assert_eq!(last_bgsave_status(&mut probe), "ok", "BGSAVE failed");
        attempts.push(format!(
            "{n} keys: armed after {armed_ms} ms, {during} spanning MSETs inside the epoch, \
             save took {} ms",
            started.elapsed().as_millis()
        ));
        if during >= MIN_OVERLAP {
            overlapped = Some(during);
            break;
        }
        dirty = j;
    }
    eprintln!("attempts: {attempts:#?}");
    let Some(during) = overlapped else {
        panic!(
            "no BGSAVE in {MAX_ATTEMPTS} attempts overlapped {MIN_OVERLAP} spanning MSETs: \
             {attempts:#?}"
        );
    };

    // Crash, restart from the snapshot alone: the file on disk is the save
    // the writes overlapped (each save replaces it).
    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir);
    let mut c2 = Conn::open(port2);
    let wrong = wrong_pre_keys(&mut c2, n);
    assert!(
        wrong.is_empty(),
        "{} of {n} keys hold a post-epoch value after restoring the BGSAVE taken \
         under {during} spanning MSETs (first: {:?})",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
    let _ = std::fs::remove_dir_all(&dir);
}
