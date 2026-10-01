//! moon#1295 R2b review: a write parked on a snapshot stream, at shutdown and
//! in the statistics.
//!
//! - SIGTERM while a writer waits for a stream: the shard stopped ticking, so
//!   the stream never ended and nothing woke the writer — the connection
//!   drain ran to its 5 s deadline and the write was dropped unanswered.
//!   Shutdown now abandons the unfinishable save first (redis kills its
//!   saving child), which releases the writer: it runs and is answered, and
//!   the process exits at once.
//! - `rdb_cow_stream_waits` counts writes: a writer that re-parks (woken by a
//!   stream end, still blocked) is one write. `CONFIG RESETSTAT` zeroes it and
//!   `rdb_cow_streamed_keys`.
//!
//! The save is HELD mid-walk (`MOON_TEST_SNAPSHOT_HOLD_FILE`); a stream still
//! runs, 16 bytes a tick (`MOON_TEST_COW_STREAM_TICK_BYTES`), so a 20k-field
//! hash keeps its writer parked for seconds. Pin the binary:
//! `MOON_BIN=<moon> cargo test --release --test cow_stream_shutdown_1295 --
//! --include-ignored`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

struct DirGuard(PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn hold_file(dir: &Path) -> PathBuf {
    dir.join("snapshot.hold")
}

fn spawn(dir: &Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .env("MOON_TEST_SNAPSHOT_HOLD_FILE", hold_file(dir))
            .env("MOON_TEST_COW_STREAM_MIN_ELEMENTS", "1000")
            .env("MOON_TEST_COW_STREAM_TICK_BYTES", "16")
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .env("RUST_LOG", "moon=info")
            .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
            .args(["--shards", "1", "--appendonly", "no", "--save", ""])
            .args(["--disk-offload", "disable", "--disk-free-min-pct", "0"])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    });
    let guard = ServerGuard::new(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["PING"]).starts_with("+PONG")
        {
            break;
        }
        assert!(Instant::now() < deadline, "server never answered PING");
        std::thread::sleep(Duration::from_millis(50));
    }
    (guard, port)
}

fn info_field(c: &mut Conn, field: &str) -> Option<String> {
    c.send(&["INFO", "persistence"]).lines().find_map(|l| {
        l.strip_prefix(&format!("{field}:"))
            .map(|v| v.trim().to_string())
    })
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    info_field(c, field)
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("INFO has no {field}"))
}

/// A hash of `n` fields (above the lowered stream threshold of 1000).
fn load_hash(c: &mut Conn, key: &str, n: usize) {
    for from in (0..n).step_by(1000) {
        let mut parts = vec!["HSET".to_string(), key.to_string()];
        for i in from..from + 1000 {
            parts.extend([format!("f{i}"), "vvvvvvvvvv".to_string()]);
        }
        let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
        assert_eq!(c.send(&parts), ":1000\r\n");
    }
}

fn start_held_bgsave(c: &mut Conn, dir: &Path) {
    std::fs::File::create(hold_file(dir)).unwrap();
    assert!(c.send(&["BGSAVE"]).starts_with('+'));
    let deadline = Instant::now() + Duration::from_secs(30);
    while info_field(c, "rdb_bgsave_in_progress").as_deref() != Some("1") {
        assert!(Instant::now() < deadline, "BGSAVE never started");
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(200));
}

/// Send `HSET <key> new 1` on its own connection and wait until it parks.
fn park_a_writer(c: &mut Conn, port: u16, key: &str) -> Conn {
    let before = info_u64(c, "rdb_cow_stream_waits");
    let mut w = Conn::open(port);
    w.sock
        .write_all(&encode(&["HSET", key, "new", "1"]))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while info_u64(c, "rdb_cow_stream_waits") == before {
        assert!(
            Instant::now() < deadline,
            "the HSET never parked on the stream"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    w
}

#[test]
#[ignore = "real server: run with --include-ignored"]
fn sigterm_releases_a_writer_parked_on_a_stream() {
    let dir = common::unique_test_dir("cow-stream-shutdown");
    let _dir = DirGuard(dir.clone());
    let (mut server, port) = spawn(&dir);
    let mut c = Conn::open(port);
    load_hash(&mut c, "h", 20_000);
    start_held_bgsave(&mut c, &dir);
    let mut w = park_a_writer(&mut c, port, "h");
    drop(c);

    let pid = server.id();
    let t0 = Instant::now();
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    // The parked write runs and is answered before the connection closes.
    let reply = w.read_replies_within(1, Duration::from_secs(10));
    assert_eq!(
        reply, ":1\r\n",
        "the parked HSET must be answered at shutdown"
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = server.as_mut().try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "moon did not exit after SIGTERM");
        std::thread::sleep(Duration::from_millis(5));
    };
    let took = t0.elapsed();
    assert!(status.success(), "exit status {status:?}");
    // Base: the 5 s drain deadline (`drain timed out with 1 connection task`).
    assert!(
        took < Duration::from_millis(2000),
        "SIGTERM took {took:?} with a writer parked on a stream"
    );
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    assert!(
        !log.contains("shutdown drain timed out"),
        "the connection drain timed out"
    );
}

/// Two writers, two large hashes: the second writer's key is queued behind
/// the first's stream, so that stream's end wakes it and it parks again.
/// Base: `rdb_cow_stream_waits:3` for two writes.
#[test]
#[ignore = "real server: run with --include-ignored"]
fn a_writer_that_re_parks_is_one_wait_and_resetstat_zeroes_the_stream_counts() {
    let dir = common::unique_test_dir("cow-stream-stats");
    let _dir = DirGuard(dir.clone());
    let (_server, port) = spawn(&dir);
    let mut c = Conn::open(port);
    load_hash(&mut c, "h1", 3000);
    load_hash(&mut c, "h2", 3000);
    assert_eq!(info_u64(&mut c, "rdb_cow_stream_waits"), 0);
    // The walk is held; the streams still run (16 bytes a tick).
    start_held_bgsave(&mut c, &dir);
    let mut w1 = park_a_writer(&mut c, port, "h1");
    let mut w2 = park_a_writer(&mut c, port, "h2");
    assert_eq!(
        w1.read_replies_within(1, Duration::from_secs(120)),
        ":1\r\n"
    );
    assert_eq!(
        w2.read_replies_within(1, Duration::from_secs(120)),
        ":1\r\n"
    );
    assert!(
        info_u64(&mut c, "rdb_cow_streamed_keys") >= 2,
        "both hashes streamed"
    );
    assert_eq!(
        info_u64(&mut c, "rdb_cow_stream_waits"),
        2,
        "two writes waited, whatever their re-parks"
    );
    std::fs::remove_file(hold_file(&dir)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while info_field(&mut c, "rdb_bgsave_in_progress").as_deref() != Some("0") {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(c.send(&["CONFIG", "RESETSTAT"]), "+OK\r\n");
    assert_eq!(info_u64(&mut c, "rdb_cow_stream_waits"), 0);
    assert_eq!(info_u64(&mut c, "rdb_cow_streamed_keys"), 0);
}
