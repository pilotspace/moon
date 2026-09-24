//! moon#1163: streams are charged to `used_memory`, so `maxmemory` binds on a
//! stream workload and a DEL returns the ledger to its baseline.
//!
//! Measured on `935c555` (review): 200K `XADD st * f <32 B>` moved
//! `used_memory` by +354 B and `MEMORY USAGE st` answered 114, where redis
//! 7.0.15 grew 8,429 KB and answered 9,344,884. With `--maxmemory` set, a
//! stream could grow until the OOM killer arrived while INFO read near-empty.
//!
//! Run with a pinned binary:
//!   MOON_BIN=/path/to/moon cargo test --test perf_ws10_stream_memory

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::process::Command;

use common::{Conn, ServerGuard, find_moon_binary};

const ENTRIES: usize = 20_000;
const PER_WRITE: usize = 500;
const VALUE: &str = "0123456789abcdef0123456789abcdef";

fn spawn(dir: &std::path::Path, maxmemory: &str) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    common::spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--save",
                "",
                "--maxmemory",
                maxmemory,
                "--maxmemory-policy",
                "noeviction",
                "--disk-offload",
                "disable",
                "--disk-free-min-pct",
                "0",
                "--protected-mode",
                "no",
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    })
}

fn used_memory(c: &mut Conn) -> u64 {
    let raw = c.send(&["INFO", "memory"]);
    raw.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO memory has no used_memory: {raw:?}"))
}

/// XADD `n` entries to `key` in pipelined batches; returns how many were
/// accepted before the first error reply (all of them if none).
fn xadd_until_refused(c: &mut Conn, key: &str, n: usize) -> (usize, Option<String>) {
    let mut accepted = 0usize;
    let mut sent = 0usize;
    while sent < n {
        let batch = PER_WRITE.min(n - sent);
        let mut buf = Vec::new();
        for _ in 0..batch {
            buf.extend_from_slice(&common::encode(&["XADD", key, "*", "f", VALUE]));
        }
        c.sock.write_all(&buf).unwrap();
        let replies = c.read_replies(batch);
        for line in replies.split("\r\n") {
            if let Some(err) = line.strip_prefix('-') {
                return (accepted, Some(err.to_string()));
            }
            if line.starts_with('$') {
                accepted += 1;
            }
        }
        sent += batch;
    }
    (accepted, None)
}

#[test]
fn stream_growth_is_charged_and_del_returns_to_baseline() {
    let dir = common::unique_test_dir("ws10-stream-mem");
    let (server, port) = spawn(&dir, "0");
    let mut c = Conn::open(port);
    assert_eq!(
        c.send(&["SET", "other", "a-value-past-the-sso-limit"]),
        "+OK\r\n"
    );
    let base = used_memory(&mut c);
    let (accepted, err) = xadd_until_refused(&mut c, "st", ENTRIES);
    assert_eq!((accepted, err), (ENTRIES, None));
    let grown = used_memory(&mut c).saturating_sub(base);
    let usage_raw = c.send(&["MEMORY", "USAGE", "st"]);
    eprintln!(
        "[1163] {ENTRIES} XADDs: used_memory +{grown} B ({:.1} B/entry); MEMORY USAGE {}",
        grown as f64 / ENTRIES as f64,
        usage_raw.trim_end()
    );
    assert!(
        grown >= (ENTRIES * 100) as u64,
        "{ENTRIES} XADDs grew used_memory by {grown} B — streams are not charged (moon#1163)"
    );
    assert_eq!(c.send(&["DEL", "st"]), ":1\r\n");
    assert_eq!(
        used_memory(&mut c),
        base,
        "DEL of the stream must return used_memory to the baseline exactly"
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn maxmemory_binds_on_a_stream_workload() {
    let dir = common::unique_test_dir("ws10-stream-maxmem");
    let (server, port) = spawn(&dir, "4mb");
    let mut c = Conn::open(port);
    // 200K entries of ~32 B would be tens of MB of real memory.
    let (accepted, err) = xadd_until_refused(&mut c, "st", 200_000);
    eprintln!("[1163] maxmemory 4mb: {accepted} XADDs accepted, then {err:?}");
    let err = err.unwrap_or_else(|| {
        panic!(
            "all 200000 XADDs were accepted under --maxmemory 4mb noeviction: the stream \
             is invisible to the memory gate (moon#1163)"
        )
    });
    assert!(err.starts_with("OOM"), "refusal must be -OOM, got -{err}");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}
