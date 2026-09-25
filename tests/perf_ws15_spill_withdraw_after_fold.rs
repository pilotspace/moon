//! moon#1223 against a real server: every key acknowledged before a
//! BGREWRITEAOF must survive a restart after it — `kill -9` and a clean
//! `SHUTDOWN` alike — including the keys whose spill was IN FLIGHT at the
//! fold instant.
//!
//! Such a key is in no plane the fold used to stream (its hot copy is gone,
//! its spill not yet published). If its spill then does not publish — the
//! `MOON.SPILLED` marker refused by a saturated writer (the moon#1202
//! withdraw), or the process stopping before the completion lands — the old
//! base had no copy of it and the post-fold log has no record of it.
//!
//! Making that likely rather than lucky:
//! - `MOON_TEST_AOF_FSYNC_STALL_MS` (the moon#838 writer hook, in every binary
//!   since) holds the AOF writer before each everysec fsync, so the writer
//!   channel stays full under the flood and the fold's writer side is slow;
//! - an 8-connection write flood over a small `--maxmemory` keeps eviction
//!   spilling, so keys are in flight at every instant, the fold's included —
//!   and LRU makes those the flood's own recent keys, which is why the flood
//!   keys (all of them acknowledged before the rewrite was requested) are
//!   what the test checks;
//! - the flood stops the moment the rewrite commits and the server is stopped
//!   right after, so a withdrawn key has no time to be re-spilled with a
//!   marker.
//!
//! `spill_completion_marker_withdrawn` is reported, not required: a key the
//! fold caught in flight is lost on the unfixed server whether its spill is
//! withdrawn or simply unpublished at the stop.
//!
//! **This is a smoke test, not a regression guard.** The loss needs the fold
//! to catch a flood key in flight AND that key's spill to end unpublished,
//! and this test only makes that likely: in the PR #1233 review it passed on
//! the unfixed `ae21476` in 4 of 5 runs. A pass here proves nothing about
//! moon#1223. The deterministic regression guard is in-process —
//! `shard::persistence_tick::fold_inflight_tests`, which forces each
//! rehydrate path and was red on `ae21476` in 4 of 4 runs.
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

const VALUE_PAD: usize = 480;
const MAXMEMORY: &str = "8388608";
const FLOOD_CONNS: usize = 8;
const BATCH: u64 = 250;

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

fn key_of(conn: usize, j: u64) -> String {
    format!("flood:{conn}:{j}")
}

fn value_of(conn: usize, j: u64) -> String {
    format!("v-{conn}-{j}-{}", "f".repeat(VALUE_PAD))
}

fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

/// Pipelined SETs of fresh keys until `stop`. Pushes every key the server
/// acknowledged (`+OK`) onto `acked`, in order; a refusal under the stall is
/// expected and simply not acknowledged.
fn start_flood(
    port: u16,
    conn: usize,
    stop: Arc<AtomicBool>,
    acked: Arc<parking_lot::Mutex<Vec<(usize, u64)>>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut c = Conn::open(port);
        // The stalled writer back-pressures the flood's own appends.
        c.sock
            .set_write_timeout(Some(Duration::from_secs(120)))
            .unwrap();
        let mut j = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let mut wire = Vec::new();
            for n in j..j + BATCH {
                wire.extend_from_slice(&common::encode(&[
                    "SET",
                    &key_of(conn, n),
                    &value_of(conn, n),
                ]));
            }
            c.sock.write_all(&wire).expect("flood write");
            let replies = c.read_replies_within(BATCH as usize, Duration::from_secs(120));
            let mut ok = Vec::new();
            for (n, line) in (j..j + BATCH).zip(replies.split("\r\n")) {
                if line == "+OK" {
                    ok.push((conn, n));
                }
            }
            acked.lock().extend(ok);
            j += BATCH;
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

fn wrong_keys(c: &mut Conn, keys: &[(usize, u64)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for chunk in keys.chunks(500) {
        let names: Vec<String> = chunk.iter().map(|(c, j)| key_of(*c, *j)).collect();
        let cmds: Vec<Vec<&str>> = names.iter().map(|k| vec!["GET", k.as_str()]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        let mut lines = reply.split("\r\n");
        for ((conn, j), name) in chunk.iter().zip(&names) {
            let head = lines.next().unwrap_or("");
            if head == "$-1" || head.starts_with('-') {
                wrong.push(name.clone());
                continue;
            }
            if lines.next().unwrap_or("") != value_of(*conn, *j) {
                wrong.push(name.clone());
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

    let halt = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let floods: Vec<_> = (0..FLOOD_CONNS)
        .map(|n| start_flood(port, n, halt.clone(), acked.clone()))
        .collect();
    // Let the flood reach steady spilling.
    let deadline = Instant::now() + Duration::from_secs(60);
    while heap_files(&dir) < 16 {
        assert!(
            Instant::now() < deadline,
            "fixture: the flood never made the server spill (heap files: {})",
            heap_files(&dir)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_secs(1));

    // Every key acknowledged by now was acknowledged before the fold.
    let before_rewrite: Vec<(usize, u64)> = acked.lock().clone();
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
        std::thread::sleep(Duration::from_millis(10));
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    while info_field(&mut c, "aof_rewrite_in_progress") != "0" {
        assert!(Instant::now() < deadline, "the rewrite never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
    halt.store(true, Ordering::Relaxed);
    assert_eq!(
        info_field(&mut c, "aof_last_bgrewrite_status"),
        "ok",
        "the rewrite must commit for this test to mean anything"
    );
    let withdrawn = info_field(&mut c, "spill_completion_marker_withdrawn");
    match stop {
        Stop::Shutdown => {
            // A successful SHUTDOWN closes the connection without a reply.
            c.sock
                .write_all(&common::encode(&["SHUTDOWN", "NOSAVE"]))
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(60);
            while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
                std::thread::sleep(Duration::from_millis(20));
            }
            server.kill_now();
        }
        Stop::Kill9 => server.kill_now(),
    }
    for flood in floods {
        // A flood connection may be cut by the stop; its keys after the
        // rewrite are not checked.
        let _ = flood.join();
    }
    common::wait_for_port_down(port);

    assert!(
        before_rewrite.len() >= 10_000,
        "fixture: only {} keys acknowledged before the rewrite",
        before_rewrite.len()
    );
    let (_server2, port2) = spawn(&dir, shards, false);
    let mut c2 = Conn::open(port2);
    let wrong = wrong_keys(&mut c2, &before_rewrite);
    assert!(
        wrong.is_empty(),
        "--shards {shards} {stop:?}: {} of {} keys acknowledged before BGREWRITEAOF are missing \
         or wrong after the restart (first: {:?}; spills withdrawn under AOF backpressure: \
         {withdrawn}). Logs: {}",
        wrong.len(),
        before_rewrite.len(),
        wrong.first(),
        dir.display()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn keys_in_flight_at_a_fold_survive_kill9_shards_1() {
    scenario(1, Stop::Kill9);
}

#[test]
fn keys_in_flight_at_a_fold_survive_shutdown_shards_1() {
    scenario(1, Stop::Shutdown);
}

#[test]
fn keys_in_flight_at_a_fold_survive_kill9_shards_4() {
    scenario(4, Stop::Kill9);
}

#[test]
fn keys_in_flight_at_a_fold_survive_shutdown_shards_4() {
    scenario(4, Stop::Shutdown);
}
