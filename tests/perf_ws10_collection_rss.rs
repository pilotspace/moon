//! moon#1160: a stored collection element must not keep the connection's
//! read buffer alive.
//!
//! The review's scenario, per collection type, on a fresh `--shards 1`
//! server: 100K pipelined pairs of `SET scratch <4 KiB>` + one small element
//! write (200 commands per write), then `DEL scratch`. Before the fix every
//! surviving element was a `Bytes` slice of the frozen read buffer its
//! command arrived in, so 100K 15-byte set members held ~550 MB of RSS that
//! `used_memory` (8.2 MB) never saw — measured on `935c555`: RSS 18.8 →
//! 578.2 MB; redis 7.0.15 12.7 → 20.4 MB.
//!
//! Asserted per type: RSS growth ≤ 3 × `used_memory` growth + 24 MiB (the
//! plan's bound: allocator slack plus the connection's own buffers), and for
//! streams — which `used_memory` did not charge at all before moon#1163 — an
//! absolute 64 MiB. A string control (`SET k:<i>`, strings always copied)
//! runs the same traffic so a harness regression shows up as a failing
//! control, not as a false fix.
//!
//! Linux-only: RSS is read from `/proc/<pid>/status`. Run against a release
//! build (the debug one is slow but gives the same answer):
//!   MOON_BIN=/path/to/moon cargo test --release --test perf_ws10_collection_rss

#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::process::Command;

use common::{Conn, ServerGuard, find_moon_binary};

const PAIRS: usize = 100_000;
const PAIRS_PER_WRITE: usize = 100;
const SCRATCH_LEN: usize = 4096;
const MIB: u64 = 1024 * 1024;

fn spawn(dir: &std::path::Path) -> (ServerGuard, u16) {
    spawn_with(dir, "no")
}

fn spawn_with(dir: &std::path::Path, appendonly: &str) -> (ServerGuard, u16) {
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
                appendonly,
                // The replayed log must stay the log the traffic wrote: an
                // automatic rewrite would fold it into an RDB base.
                "--auto-aof-rewrite-percentage",
                "0",
                "--save",
                "",
                "--maxmemory",
                "0",
                "--disk-offload",
                "disable",
                // The shared CI box runs with little free disk; the guard
                // would refuse writes and measure nothing.
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

fn rss_bytes(pid: u32) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .expect("VmRSS in /proc/<pid>/status");
    kb * 1024
}

fn used_memory(c: &mut Conn) -> u64 {
    let raw = c.send(&["INFO", "memory"]);
    raw.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO memory has no used_memory: {raw:?}"))
}

/// The element write for pair `i` of `kind`.
fn element_cmd(kind: &str, i: usize) -> Vec<String> {
    let m = format!("member:{i}");
    let parts: Vec<&str> = match kind {
        "SADD" => vec!["SADD", "coll", &m],
        "HSET" => vec!["HSET", "coll", &m, "v"],
        "ZADD" => vec!["ZADD", "coll", "1", &m],
        "RPUSH" => vec!["RPUSH", "coll", &m],
        "XADD" => vec!["XADD", "coll", "*", "f", &m],
        // The control: strings always copied their bytes.
        "SET" => return vec!["SET".into(), format!("k:{i}"), m],
        other => panic!("{other}"),
    };
    parts.into_iter().map(str::to_string).collect()
}

struct Measured {
    rss_growth: u64,
    used_growth: u64,
}

fn run(kind: &str) -> Measured {
    let dir = common::unique_test_dir(&format!("ws10-rss-{}", kind.to_lowercase()));
    let (server, port) = spawn(&dir);
    let pid = server.id();
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
    let rss_before = rss_bytes(pid);
    let used_before = used_memory(&mut c);

    let scratch = "s".repeat(SCRATCH_LEN);
    let mut batch = Vec::with_capacity(PAIRS_PER_WRITE * (SCRATCH_LEN + 128));
    let mut i = 0usize;
    while i < PAIRS {
        batch.clear();
        for _ in 0..PAIRS_PER_WRITE {
            batch.extend_from_slice(&common::encode(&["SET", "scratch", &scratch]));
            let cmd = element_cmd(kind, i);
            let refs: Vec<&str> = cmd.iter().map(String::as_str).collect();
            batch.extend_from_slice(&common::encode(&refs));
            i += 1;
        }
        c.sock.write_all(&batch).unwrap();
        let replies = c.read_replies(PAIRS_PER_WRITE * 2);
        assert!(
            !replies.contains("\r\n-") && !replies.starts_with('-'),
            "{kind}: an error reply in the batch: {:?}",
            &replies[..replies.len().min(200)]
        );
    }
    assert_eq!(c.send(&["DEL", "scratch"]), ":1\r\n");
    // Let the allocator settle its thread caches' purge tick.
    std::thread::sleep(std::time::Duration::from_millis(500));
    let rss_after = rss_bytes(pid);
    let used_after = used_memory(&mut c);
    let m = Measured {
        rss_growth: rss_after.saturating_sub(rss_before),
        used_growth: used_after.saturating_sub(used_before),
    };
    eprintln!(
        "[1160] {kind}: RSS {:.1} -> {:.1} MiB (+{:.1}); used_memory +{:.1} MiB",
        rss_before as f64 / MIB as f64,
        rss_after as f64 / MIB as f64,
        m.rss_growth as f64 / MIB as f64,
        m.used_growth as f64 / MIB as f64,
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
    m
}

fn assert_bounded_by_used_memory(kind: &str) {
    let m = run(kind);
    let bound = 3 * m.used_growth + 24 * MIB;
    assert!(
        m.rss_growth <= bound,
        "{kind}: RSS grew {} MiB for {PAIRS} small elements while used_memory grew {} MiB \
         (bound {} MiB) — stored elements are pinning request read buffers (moon#1160)",
        m.rss_growth / MIB,
        m.used_growth / MIB,
        bound / MIB
    );
}

#[test]
fn string_control_stays_bounded() {
    assert_bounded_by_used_memory("SET");
}

#[test]
fn sadd_members_do_not_pin_read_buffers() {
    assert_bounded_by_used_memory("SADD");
}

#[test]
fn hset_fields_do_not_pin_read_buffers() {
    assert_bounded_by_used_memory("HSET");
}

#[test]
fn zadd_members_do_not_pin_read_buffers() {
    assert_bounded_by_used_memory("ZADD");
}

#[test]
fn rpush_elements_do_not_pin_read_buffers() {
    assert_bounded_by_used_memory("RPUSH");
}

#[test]
fn xadd_entries_do_not_pin_read_buffers() {
    let m = run("XADD");
    assert!(
        m.rss_growth <= 64 * MIB,
        "XADD: RSS grew {} MiB for {PAIRS} one-field entries — stored fields are pinning \
         request read buffers (moon#1160)",
        m.rss_growth / MIB
    );
}

/// The replay half of moon#1160, on the path a `--shards 1` monoio server
/// boots through (the multi-part manifest's RESP incr file; tokio
/// `--shards 1` takes the legacy single-file reader, which shares the same
/// bounded reader). Pre-fix the incr file was read whole, copied AGAIN into
/// one `BytesMut`, and every replayed member was a slice of that copy — so
/// one surviving member pinned the entire replayed log for the life of the
/// process.
#[test]
fn aof_replay_does_not_pin_the_replayed_log() {
    const REPLAY_PAIRS: usize = 20_000;
    let dir = common::unique_test_dir("ws10-rss-replay");
    let (server, port) = spawn_with(&dir, "yes");
    let fresh_rss = rss_bytes(server.id());
    let mut c = Conn::open(port);
    let scratch = "s".repeat(SCRATCH_LEN);
    let mut batch = Vec::new();
    let mut i = 0usize;
    while i < REPLAY_PAIRS {
        batch.clear();
        for _ in 0..PAIRS_PER_WRITE {
            batch.extend_from_slice(&common::encode(&["SET", "scratch", &scratch]));
            let m = format!("member:{i}");
            batch.extend_from_slice(&common::encode(&["SADD", "coll", &m]));
            i += 1;
        }
        c.sock.write_all(&batch).unwrap();
        c.read_replies(PAIRS_PER_WRITE * 2);
    }
    assert_eq!(c.send(&["DEL", "scratch"]), ":1\r\n");
    // `everysec`: give the last second a chance to reach the file.
    std::thread::sleep(std::time::Duration::from_millis(2500));
    drop(c);
    drop(server);
    common::wait_for_port_down(port);

    let (server, port) = spawn_with(&dir, "yes");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SCARD", "coll"]), format!(":{REPLAY_PAIRS}\r\n"));
    std::thread::sleep(std::time::Duration::from_millis(500));
    let rss = rss_bytes(server.id());
    let used = used_memory(&mut c);
    let aof_bytes: u64 = walk_len(&dir.join("appendonlydir"));
    eprintln!(
        "[1160] replay: fresh RSS {:.1} MiB; after replaying a {:.1} MiB log RSS {:.1} MiB, \
         used_memory {:.1} MiB",
        fresh_rss as f64 / MIB as f64,
        aof_bytes as f64 / MIB as f64,
        rss as f64 / MIB as f64,
        used as f64 / MIB as f64
    );
    let bound = fresh_rss + 3 * used + 24 * MIB;
    assert!(
        rss <= bound,
        "after replaying a {} MiB log the server holds {} MiB RSS for {} MiB of data \
         (bound {} MiB) — replayed elements pin the replay buffer (moon#1160)",
        aof_bytes / MIB,
        rss / MIB,
        used / MIB,
        bound / MIB
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

fn walk_len(p: &std::path::Path) -> u64 {
    let Ok(md) = std::fs::metadata(p) else {
        return 0;
    };
    if md.is_file() {
        return md.len();
    }
    std::fs::read_dir(p)
        .map(|rd| rd.flatten().map(|e| walk_len(&e.path())).sum())
        .unwrap_or(0)
}
