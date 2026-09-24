//! WS12 end to end, against a real server: a BGSAVE taken while clients keep
//! inserting (so the DashTable keeps splitting mid-epoch) must restore every
//! key that existed before the save started, with its value (moon#1216);
//! and a FLUSHDB / FLUSHALL / SWAPDB landing mid-BGSAVE must neither crash
//! the shard (moon#1224) nor publish a mixed file.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws12_bgsave_split`
//! (falls back to the binary Cargo built for this run).
//!
//! Red on the base `f32546c` binary: the split test restores with thousands
//! of epoch-start keys missing; the FLUSH/SWAPDB test finds the server dead.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
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
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

/// Load `n` keys `pre:<i>` = `v<i>`, pipelined.
fn preload(c: &mut Conn, n: u64) {
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
        let keys: Vec<(String, String)> = (i..end)
            .map(|j| (format!("pre:{j:08}"), format!("v{j}")))
            .collect();
        let cmds: Vec<Vec<&str>> = keys
            .iter()
            .map(|(k, v)| vec!["SET", k.as_str(), v.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        assert!(!reply.contains('-'), "preload refused: {reply:.200}");
        i = end;
    }
}

fn info_field(c: &mut Conn, field: &str) -> Option<String> {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
}

/// Wait for the in-flight BGSAVE to finish; returns `rdb_last_bgsave_status`.
fn wait_bgsave(c: &mut Conn, budget: Duration) -> String {
    let deadline = Instant::now() + budget;
    loop {
        if info_field(c, "rdb_bgsave_in_progress").as_deref() == Some("0") {
            return info_field(c, "rdb_last_bgsave_status").unwrap_or_default();
        }
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Count `pre:` keys that are missing or hold another value.
fn wrong_pre_keys(c: &mut Conn, n: u64) -> u64 {
    let mut wrong = 0u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
        let keys: Vec<String> = (i..end).map(|j| format!("pre:{j:08}")).collect();
        let cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        // Each reply is `$<len>\r\nv<j>\r\n` or `$-1\r\n`.
        let mut lines = reply.split("\r\n");
        for j in i..end {
            let head = lines.next().unwrap_or("");
            if head == "$-1" {
                wrong += 1;
                continue;
            }
            let body = lines.next().unwrap_or("");
            if body != format!("v{j}") {
                wrong += 1;
            }
        }
        i = end;
    }
    wrong
}

/// Keep inserting fresh keys (forcing DashTable splits) until `stop`.
fn start_inserter(
    port: u16,
    stop: Arc<AtomicBool>,
) -> (std::thread::JoinHandle<()>, Arc<AtomicU64>) {
    let written = Arc::new(AtomicU64::new(0));
    let counter = written.clone();
    let handle = std::thread::spawn(move || {
        let mut c = Conn::open(port);
        let mut j = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let mut batch = Vec::new();
            for _ in 0..500 {
                batch.extend_from_slice(&encode(&["SET", &format!("new:{j:010}"), "n"]));
                j += 1;
            }
            c.sock.write_all(&batch).unwrap();
            c.read_replies(500);
            counter.store(j, Ordering::Relaxed);
        }
    });
    (handle, written)
}

fn split_during_bgsave_keeps_every_pre_epoch_key(shards: usize) {
    let dir = common::unique_test_dir("ws12-split");
    std::fs::create_dir_all(&dir).unwrap();
    let n = 300_000u64;
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    preload(&mut c, n);

    let stop = Arc::new(AtomicBool::new(false));
    let (inserter, inserted) = start_inserter(port, stop.clone());
    // Let the inserter get going so the epoch starts under load.
    while inserted.load(Ordering::Relaxed) < 20_000 {
        std::thread::sleep(Duration::from_millis(5));
    }
    let before = inserted.load(Ordering::Relaxed);
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    let status = wait_bgsave(&mut c, Duration::from_secs(120));
    let during = inserted.load(Ordering::Relaxed) - before;
    stop.store(true, Ordering::Relaxed);
    inserter.join().unwrap();
    assert_eq!(status, "ok", "BGSAVE failed");
    assert!(
        during >= 1_000,
        "only {during} inserts landed during the BGSAVE — the epoch saw no split load"
    );

    // Crash, restart from the snapshot alone.
    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir, shards);
    let mut c2 = Conn::open(port2);
    let wrong = wrong_pre_keys(&mut c2, n);
    assert_eq!(
        wrong, 0,
        "--shards {shards}: {wrong} of {n} keys that existed before BGSAVE are missing or \
         changed after restoring its snapshot ({during} inserts during the epoch)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn split_during_bgsave_keeps_every_pre_epoch_key_shards_1() {
    split_during_bgsave_keeps_every_pre_epoch_key(1);
}

#[test]
fn split_during_bgsave_keeps_every_pre_epoch_key_shards_4() {
    split_during_bgsave_keeps_every_pre_epoch_key(4);
}

/// moon#1224: each whole-table write, issued right after BGSAVE started (the
/// epoch is still writing thousands of segments), must leave the server
/// serving — the save fails (it cannot be one instant any more) and the
/// server says so.
#[test]
fn table_swaps_during_bgsave_fail_the_save_not_the_server() {
    for cmd in [
        &["FLUSHALL"][..],
        &["FLUSHDB"][..],
        &["FLUSHDB", "ASYNC"][..],
        &["SWAPDB", "0", "1"][..],
    ] {
        let dir = common::unique_test_dir("ws12-swap");
        std::fs::create_dir_all(&dir).unwrap();
        let (_server, port) = spawn(&dir, 1);
        let mut c = Conn::open(port);
        preload(&mut c, 600_000);
        assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
        // Let the shard pick the epoch up (1 ms tick). At ~1,024 entries per
        // tick, 600K keys keep the epoch open for ~0.6 s after this.
        std::thread::sleep(Duration::from_millis(30));
        let reply = c.send(cmd);
        assert!(reply.starts_with('+'), "{cmd:?} refused: {reply}");
        // The shard must still be serving (it panicked here before).
        std::thread::sleep(Duration::from_millis(50));
        let mut probe = Conn::open(port);
        assert!(
            probe.send(&["PING"]).contains("PONG"),
            "{cmd:?} during BGSAVE killed the server"
        );
        let status = wait_bgsave(&mut probe, Duration::from_secs(120));
        assert_eq!(
            status, "err",
            "{cmd:?} crossed the epoch: the save must fail, not publish a mixed file"
        );
        assert!(probe.send(&["PING"]).contains("PONG"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
