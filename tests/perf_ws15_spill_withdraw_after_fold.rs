//! moon#1223 against a real server: a key whose spill is in flight when a
//! BGREWRITEAOF fold cuts its base, and whose `MOON.SPILLED` marker the
//! saturated AOF writer then refuses (the moon#1202 withdraw puts it back in
//! RAM with no log record), must survive a restart — clean `SHUTDOWN` and
//! `kill -9` alike.
//!
//! Forcing the ordering: `MOON_TEST_AOF_FSYNC_STALL_MS` (the moon#838 writer
//! hook, present in every binary since) holds the AOF writer before each
//! everysec fsync, so a pipelined write flood keeps its 10k channel full and
//! spill-completion markers are refused (`spill_completion_marker_withdrawn`
//! counts them; the test asserts it moved, so it cannot pass without having
//! exercised the withdraw). The flood also keeps eviction spilling, so keys
//! are in flight at every instant — including the fold's.
//!
//! What is checked: every key acknowledged BEFORE the BGREWRITEAOF, with its
//! value, after the restart. Those writes are all in the fold's base or in a
//! cold file its `MOON.COLDCUT` authorizes — unless the fold caught them in
//! flight and the base left them out (the bug).
//!
//! Runs at `--shards 1` (TopLevel fold on monoio, the legacy flat-file fold
//! on tokio) and `--shards 4` (per-shard fold). Pin the binary:
//! `MOON_BIN=<moon> cargo test --test perf_ws15_spill_withdraw_after_fold`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

const PRE_KEYS: usize = 20_000;
const VALUE_PAD: usize = 480;
const MAXMEMORY: &str = "8388608";
const FLOOD_CONNS: usize = 8;

fn spawn(dir: &std::path::Path, shards: usize, stall: bool) -> (ServerGuard, u16) {
    let off = dir.join("off");
    std::fs::create_dir_all(&off).unwrap();
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let mut cmd = std::process::Command::new(&bin);
        cmd.args([
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
            "--maxmemory",
            MAXMEMORY,
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "enable",
            "--disk-offload-dir",
            &off.to_string_lossy(),
            // The only rewrite is the test's own.
            "--auto-aof-rewrite-percentage",
            "0",
            // No sweep: nothing here depends on file reclamation.
            "--cold-orphan-sweep-interval-secs",
            "3600",
            "--save",
            "",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(common::server_stderr(dir))
        .stderr(common::server_stderr(dir));
        if stall {
            cmd.env("MOON_TEST_AOF_FSYNC_STALL_MS", "1500");
        }
        cmd.spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

fn value_of(i: usize) -> String {
    format!("pre-{i}-{}", "p".repeat(VALUE_PAD))
}

fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

fn preload(c: &mut Conn) {
    for start in (0..PRE_KEYS).step_by(500) {
        let end = (start + 500).min(PRE_KEYS);
        let kv: Vec<(String, String)> = (start..end)
            .map(|i| (format!("pre:{i}"), value_of(i)))
            .collect();
        let cmds: Vec<Vec<&str>> = kv
            .iter()
            .map(|(k, v)| vec!["SET", k.as_str(), v.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        assert!(
            !reply.lines().any(|l| l.starts_with('-')),
            "a pre-rewrite write was refused, so it is not an acknowledged write: {reply:.300}"
        );
    }
}

/// Pipelined SETs of fresh keys until `stop`: keeps eviction spilling and
/// the (stalled) AOF writer's channel full. Refusals are expected and fine.
fn start_flood(port: u16, conn: usize, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut c = Conn::open(port);
        // The stalled writer back-pressures the flood's own appends.
        c.sock
            .set_write_timeout(Some(Duration::from_secs(120)))
            .unwrap();
        let value = "f".repeat(VALUE_PAD);
        let mut j = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let keys: Vec<String> = (0..500)
                .map(|n| format!("flood:{conn}:{}", j + n))
                .collect();
            j += 500;
            let cmds: Vec<Vec<&str>> = keys
                .iter()
                .map(|k| vec!["SET", k.as_str(), value.as_str()])
                .collect();
            let mut wire = Vec::new();
            for cmd in &cmds {
                wire.extend_from_slice(&common::encode(cmd));
            }
            c.sock.write_all(&wire).expect("flood write");
            let _ = c.read_replies_within(cmds.len(), Duration::from_secs(120));
        }
    })
}

fn heap_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                {
                    *acc += 1;
                }
            }
        }
    }
    let mut acc = 0;
    walk(&dir.join("off"), &mut acc);
    acc
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stop {
    Kill9,
    Shutdown,
}

fn wrong_pre_keys(c: &mut Conn) -> Vec<usize> {
    let mut wrong = Vec::new();
    for start in (0..PRE_KEYS).step_by(500) {
        let end = (start + 500).min(PRE_KEYS);
        let keys: Vec<String> = (start..end).map(|i| format!("pre:{i}")).collect();
        let cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        let mut lines = reply.split("\r\n");
        for i in start..end {
            let head = lines.next().unwrap_or("");
            if head == "$-1" || head.starts_with('-') {
                wrong.push(i);
                continue;
            }
            if lines.next().unwrap_or("") != value_of(i) {
                wrong.push(i);
            }
        }
    }
    wrong
}

fn scenario(shards: usize, stop: Stop) {
    let dir = common::unique_test_dir(&format!("ws15-1223-s{shards}-{stop:?}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    preload(&mut c);

    let halt = Arc::new(AtomicBool::new(false));
    // Several connections: at --shards 4 a connection's writes for keys
    // other shards own are routed legs, which (moon#769) are admitted
    // against the owner's writer and refused unapplied under the stall, so
    // only LOCAL legs fill a writer's channel — and each connection is local
    // to one shard. Eight connections spread over every shard.
    let floods: Vec<_> = (0..FLOOD_CONNS)
        .map(|n| start_flood(port, n, halt.clone()))
        .collect();
    // Let the flood saturate the writer and push spills through.
    let deadline = Instant::now() + Duration::from_secs(60);
    while heap_files(&dir) == 0 || info_field(&mut c, "spill_completion_marker_withdrawn") == "0" {
        assert!(
            Instant::now() < deadline,
            "fixture: the flood never produced a spill withdrawn under AOF backpressure \
             (heap files: {}) — this run would prove nothing",
            heap_files(&dir)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let withdrawn_before = info_field(&mut c, "spill_completion_marker_withdrawn");

    // The rewrite request itself travels through the (saturated) writer
    // channel and is refused while it is full: retry until it gets in.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let reply = c.send(&["BGREWRITEAOF"]);
        if reply.contains("rewriting started") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "BGREWRITEAOF never started: {reply}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    while info_field(&mut c, "aof_rewrite_in_progress") != "0" {
        assert!(Instant::now() < deadline, "the rewrite never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        info_field(&mut c, "aof_last_bgrewrite_status"),
        "ok",
        "the rewrite must commit for this test to mean anything"
    );
    // Keep the flood going a little past the commit, so completions of the
    // spills the fold caught in flight land while the writer is still full.
    std::thread::sleep(Duration::from_secs(2));
    halt.store(true, Ordering::Relaxed);
    for flood in floods {
        flood.join().unwrap();
    }
    let withdrawn_after = info_field(&mut c, "spill_completion_marker_withdrawn");
    std::thread::sleep(Duration::from_millis(500));

    match stop {
        Stop::Shutdown => {
            // A successful SHUTDOWN closes the connection without a reply.
            c.sock
                .write_all(&common::encode(&["SHUTDOWN", "NOSAVE"]))
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
                std::thread::sleep(Duration::from_millis(50));
            }
            server.kill_now();
        }
        Stop::Kill9 => server.kill_now(),
    }
    common::wait_for_port_down(port);

    let (_server2, port2) = spawn(&dir, shards, false);
    let mut c2 = Conn::open(port2);
    let wrong = wrong_pre_keys(&mut c2);
    assert!(
        wrong.is_empty(),
        "--shards {shards} {stop:?}: {} of {PRE_KEYS} keys acknowledged before BGREWRITEAOF are \
         missing or wrong after the restart (first: pre:{:?}; spills withdrawn under AOF \
         backpressure: {withdrawn_before} before the rewrite, {withdrawn_after} by the end). \
         Logs: {}",
        wrong.len(),
        wrong.first(),
        dir.display()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn withdrawn_spills_survive_kill9_after_a_fold_shards_1() {
    scenario(1, Stop::Kill9);
}

#[test]
fn withdrawn_spills_survive_shutdown_after_a_fold_shards_1() {
    scenario(1, Stop::Shutdown);
}

#[test]
fn withdrawn_spills_survive_kill9_after_a_fold_shards_4() {
    scenario(4, Stop::Kill9);
}

#[test]
fn withdrawn_spills_survive_shutdown_after_a_fold_shards_4() {
    scenario(4, Stop::Shutdown);
}
