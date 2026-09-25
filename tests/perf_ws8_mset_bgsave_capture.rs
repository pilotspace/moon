//! moon#1228 item 1, end to end (the WS12 pattern of
//! `perf_ws12_bgsave_split.rs`): spanning `MSET`s that overwrite keys while a
//! BGSAVE epoch is still writing them must leave the snapshot holding every
//! key's EPOCH-START value. The coordinator's local leg wrote through a
//! capture-free path, so on `ae21476` the restored snapshot holds post-epoch
//! values for keys owned by the connection's shard.
//!
//! The overlap between the save and the writes is made, not raced. The
//! server runs with `MOON_TEST_SNAPSHOT_HOLD_FILE` (test-only): while that
//! file exists, no shard advances the armed epoch's segments, so every key
//! stays pending and every overwrite must be captured. The test waits until
//! every shard armed the epoch, then pipelines its spanning MSETs, each
//! followed by an `INFO persistence` that must still see the save running,
//! and only then releases the hold. Racing the save instead failed twice:
//! - PR #1233 review: a fixed 30 ms sleep, 3 of 4 debug runs;
//! - PR #1242's hosted Check at `5d1a37c`: 4-12 MSETs inside epochs of
//!   125-1022 ms, over 400K-3.2M keys, in all five attempts.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_mset_bgsave_capture`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

const SHARDS: usize = 4;

/// The hold file: while it exists, a running save's epoch cannot advance.
fn hold_file(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("snapshot.hold")
}

fn spawn(dir: &std::path::Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .env("MOON_TEST_SNAPSHOT_HOLD_FILE", hold_file(dir))
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

/// Keys preloaded: 100K per shard, each `v<j>` at the epoch's start.
const KEYS: u64 = 100_000 * SHARDS as u64;
/// Spanning MSETs inside the held epoch: 512 overwritten pairs, about 128 of
/// them on the connection's own shard (the capture this test exists for).
const MSETS: u64 = 16;
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
    preload(&mut c, 0, KEYS);

    // Hold the epoch, start the save, wait until every shard armed it.
    std::fs::write(hold_file(&dir), b"").expect("create the hold file");
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    assert!(
        wait_until_armed(&dir, &mut probe),
        "the held BGSAVE ended before every shard armed it: the hold did not hold"
    );

    // Spanning MSETs over the epoch-start keys, each followed by a probe on
    // the same connection. A probe runs only once the MSET before it has
    // completed on every owner, so a probe that still sees the save running
    // proves that MSET landed inside the epoch.
    let mut j = 0u64;
    let batch: Vec<Vec<String>> = (0..MSETS)
        .map(|_| {
            let mut cmd: Vec<String> = vec!["MSET".into()];
            for _ in 0..PAIRS {
                cmd.push(key(j % KEYS));
                cmd.push("new".into());
                j += 1;
            }
            cmd
        })
        .collect();
    let mut parts: Vec<Vec<&str>> = Vec::with_capacity(2 * batch.len());
    for cmd in &batch {
        parts.push(cmd.iter().map(String::as_str).collect());
        parts.push(vec!["INFO", "persistence"]);
    }
    let refs: Vec<&[&str]> = parts.iter().map(Vec::as_slice).collect();
    let reply = c.pipeline(&refs);
    let oks = reply.matches("+OK\r\n").count() as u64;
    let inside = reply
        .lines()
        .filter(|l| l.trim() == "rdb_bgsave_in_progress:1")
        .count() as u64;
    assert_eq!(oks, MSETS, "spanning MSETs: {reply:.300}");
    assert_eq!(
        inside, MSETS,
        "every spanning MSET must land inside the held epoch ({inside} of {MSETS} did)"
    );
    assert!(bgsave_in_progress(&mut probe), "the hold must still hold");

    // Release the epoch and let the save finish.
    std::fs::remove_file(hold_file(&dir)).expect("release the hold");
    wait_bgsave_done(&mut probe);
    assert_eq!(last_bgsave_status(&mut probe), "ok", "BGSAVE failed");

    // Crash, restart from the snapshot alone.
    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir);
    let mut c2 = Conn::open(port2);
    let wrong = wrong_pre_keys(&mut c2, KEYS);
    assert!(
        wrong.is_empty(),
        "{} of {KEYS} keys hold a post-epoch value after restoring the BGSAVE taken \
         under {MSETS} spanning MSETs (first: {:?})",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
    let _ = std::fs::remove_dir_all(&dir);
}
