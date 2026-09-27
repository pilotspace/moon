//! Review round 2b (moon#1265): a DEGRADED shard's plain-drop eviction must
//! reach the AOF, or the dropped keys come back on the next restart.
//!
//! #1265 makes `evict_to_budget`'s async-spill arm take the no-spill path
//! when the spill channel is closed (`sender.is_disconnected()`): evicting
//! policies now PLAIN-DROP. Before #1265 that arm could only plain-drop under
//! `--appendonly no`, so two tokio call sites build `EvictionRun::async_spill`
//! with no `.report(..)` sink (`handler_sharded/mod.rs` per-command write gate,
//! `handler_sharded/write.rs::mq_write_gate`): the default no-op sink runs, no
//! `DEL` is appended to the AOF, and after a restart the AOF replays the
//! evicted keys back. redis propagates every eviction as a `DEL`
//! (`propagateDeletion`), so an evicted key never survives a restart there.
//!
//! The monoio write gates pass a reporting sink, so this passed on monoio
//! and failed on tokio (3/3) until the tokio gates got the same sink.
//!
//! Run: MOON_BIN=<bin> cargo test [--no-default-features --features
//!   runtime-tokio,jemalloc] --test review_r2b_spill_degraded_aof_1265 -- --ignored

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::Conn;

const VALUE_LEN: usize = 600;
const FILL: usize = 16_000;

fn panic_file(dir: &Path) -> PathBuf {
    dir.join("spill-panic")
}

/// `maxmemory`: "8388608" for the degraded run, "0" for the restart (so the
/// AOF replay itself evicts nothing and every replayed key stays visible).
fn start(dir: &Path, maxmemory: &str) -> (common::ServerGuard, u16) {
    start_with(dir, maxmemory, true)
}

/// `offload`: disk-offload enabled (the spill arm) or disabled (the plain
/// arm, which evicts by drop from the start).
fn start_with(dir: &Path, maxmemory: &str, offload: bool) -> (common::ServerGuard, u16) {
    let off = dir.join("off");
    std::fs::create_dir_all(&off).unwrap();
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let maxmemory = maxmemory.to_string();
    let (child, port) = common::spawn_listening(move |port| {
        let offload_args: Vec<&str> = if offload {
            vec![
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off.to_str().unwrap(),
            ]
        } else {
            vec!["--disk-offload", "disable"]
        };
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--maxmemory",
                &maxmemory,
                "--maxmemory-policy",
                "allkeys-lru",
            ])
            .args(&offload_args)
            .args([
                "--appendonly",
                "yes",
                "--cold-orphan-sweep-interval-secs",
                "3600",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .env("MOON_TEST_SPILL_PANIC_FILE", panic_file(&dir))
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    (common::ServerGuard::new(child), port)
}

fn value(i: usize) -> String {
    let head = format!("v{i}-");
    format!("{head}{}", "x".repeat(VALUE_LEN - head.len()))
}

fn set_range(port: u16, range: std::ops::Range<usize>) -> usize {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut buf = Vec::new();
    for i in range.clone() {
        buf.extend_from_slice(&common::encode(&["SET", &format!("k:{i}"), &value(i)]));
    }
    s.write_all(&buf).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    let mut out = Vec::new();
    let mut chunk = [0u8; 65536];
    while out.iter().filter(|&&b| b == b'\n').count() < range.len() {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "conn closed");
        out.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8_lossy(&out)
        .split("\r\n")
        .filter(|l| l.starts_with("-OOM"))
        .count()
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}: {info}"))
}

fn is_nil(c: &mut Conn, i: usize) -> bool {
    c.send(&["GET", &format!("k:{i}")]).starts_with("$-1")
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_degraded_shards_evictions_are_not_resurrected_by_the_aof() {
    let dir = common::unique_test_dir("r2b-spill-degraded-aof");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = start(&dir, "8388608");
    let mut c = Conn::open(port);
    let _ = set_range(port, 0..FILL);
    let deadline = Instant::now() + Duration::from_secs(30);
    while info_u64(&mut c, "spill_batches_flushed") == 0 {
        assert!(Instant::now() < deadline, "never spilled");
        std::thread::sleep(Duration::from_millis(50));
    }

    // Crash-loop the spill thread until the shard degrades.
    std::fs::write(panic_file(&dir), "after-write").unwrap();
    let mut next = FILL;
    let deadline = Instant::now() + Duration::from_secs(90);
    while info_u64(&mut c, "spill_thread_degraded") < 1 {
        assert!(Instant::now() < deadline, "never degraded");
        let _ = set_range(port, next..next + 500);
        next += 500;
        std::thread::sleep(Duration::from_millis(100));
    }
    std::fs::remove_file(panic_file(&dir)).unwrap();

    // Degraded: these writes evict by plain drop.
    let evicted_before = {
        let s = c.send(&["INFO", "stats"]);
        s.lines()
            .find_map(|l| l.strip_prefix("evicted_keys:"))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    assert_eq!(
        set_range(port, next..next + 6_000),
        0,
        "no -OOM when degraded"
    );
    next += 6_000;
    let evicted_after = {
        let s = c.send(&["INFO", "stats"]);
        s.lines()
            .find_map(|l| l.strip_prefix("evicted_keys:"))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    let nil_live: Vec<usize> = (0..next).filter(|&i| is_nil(&mut c, i)).collect();
    eprintln!(
        "evicted_keys {evicted_before} -> {evicted_after}; {} of {next} keys read nil live",
        nil_live.len()
    );
    assert!(
        !nil_live.is_empty(),
        "fixture: the degraded shard dropped nothing"
    );

    // everysec: let the tail reach the disk, then restart without maxmemory.
    std::thread::sleep(Duration::from_millis(2_500));
    server.kill_now();
    common::wait_for_port_down(port);
    drop(c);
    let (_server2, port2) = start(&dir, "0");
    let mut c2 = Conn::open(port2);
    let resurrected: Vec<usize> = nil_live
        .iter()
        .copied()
        .filter(|&i| !is_nil(&mut c2, i))
        .collect();
    assert!(
        resurrected.is_empty(),
        "{} of {} keys the degraded shard EVICTED came back after the restart \
         (no DEL reached the AOF; first: {:?}); dir {}",
        resurrected.len(),
        nil_live.len(),
        &resurrected[..resurrected.len().min(5)],
        dir.display()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The pre-existing root cause, without #1265: the plain (no disk-offload)
/// arm of the same tokio write gates dropped victims without a `DEL`. The
/// reviewer measured it by hand on 273e6bc tokio: 10,700 keys live, 39,185
/// after the restart.
#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn plain_evictions_are_not_resurrected_by_the_aof() {
    const KEYS: usize = 40_000;
    let dir = common::unique_test_dir("r2b-plain-evict-aof");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = start_with(&dir, "8388608", false);
    let mut c = Conn::open(port);
    let mut ooms = 0;
    for chunk in (0..KEYS).step_by(2_000) {
        ooms += set_range(port, chunk..chunk + 2_000);
    }
    assert_eq!(ooms, 0, "allkeys-lru must evict, not refuse");
    let nil_live: Vec<usize> = (0..KEYS).filter(|&i| is_nil(&mut c, i)).collect();
    eprintln!("{} of {KEYS} keys read nil live", nil_live.len());
    assert!(!nil_live.is_empty(), "fixture: nothing was evicted");

    std::thread::sleep(Duration::from_millis(2_500));
    server.kill_now();
    common::wait_for_port_down(port);
    drop(c);
    let (_server2, port2) = start_with(&dir, "0", false);
    let mut c2 = Conn::open(port2);
    let resurrected = nil_live.iter().filter(|&&i| !is_nil(&mut c2, i)).count();
    assert_eq!(
        resurrected,
        0,
        "{resurrected} of {} EVICTED keys came back after the restart (no DEL reached \
         the AOF); dir {}",
        nil_live.len(),
        dir.display()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
