//! R1 review of moon#1266 — what an operator sees while the disk holds an AOF
//! fsync (`MOON_TEST_AOF_SYNC_GATE` holds every AOF data fsync while its file
//! exists).
//!
//! Findings 3 and 4: since the everysec fsync moved to an agent thread, a
//! stalled fsync no longer blocks the writer — and was therefore silent: no
//! log line, `aof_last_fsync_status:ok`, and `aof_delayed_fsync` counted once
//! per stall where redis counts every 2 s. Now, while an 8 s stall lasts, the
//! server logs redis's "Asynchronous AOF fsync is taking too long (disk is
//! busy?)", INFO shows `aof_pending_bio_fsync` and `aof_fsync_in_flight_ms`,
//! and `aof_delayed_fsync` grows by one per 2 s; after the release everything
//! returns to 0 / ok.
//!
//! `MOON_BIN=<moon> cargo test --test aof_fsync_stall_r1 -- --include-ignored`
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

fn spawn(dir: &Path, shards: usize, policy: &str, gate: &Path) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let d = dir.to_path_buf();
    let gate = gate.to_path_buf();
    let policy = policy.to_string();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--appendfsync",
                &policy,
                "--auto-aof-rewrite-percentage",
                "0",
                "--disk-free-min-pct",
                "0",
                // A held fsync must not turn into a timeout reply here.
                "--aof-fsync-timeout-ms",
                "0",
            ])
            .env("MOON_TEST_AOF_SYNC_GATE", &gate)
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}: {info}"))
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    info_field(c, field).parse().expect("numeric INFO field")
}

/// A writer thread: one SET every 10 ms until `stop` is set.
fn trickle(
    port: u16,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<usize> {
    std::thread::spawn(move || {
        let mut c = Conn::open(port);
        let mut n = 0usize;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            assert_eq!(c.send(&["SET", &format!("t:{n}"), "v"]), "+OK\r\n");
            n += 1;
            std::thread::sleep(Duration::from_millis(10));
        }
        n
    })
}

fn stall_is_visible(shards: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let gate = dir.join("sync.gate");
    let (_server, port) = spawn(dir, shards, "everysec", &gate);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", "warm", "1"]), "+OK\r\n");
    std::thread::sleep(Duration::from_millis(1_500));
    let delayed_before = info_u64(&mut c, "aof_delayed_fsync");
    assert_eq!(
        info_u64(&mut c, "aof_pending_bio_fsync"),
        0,
        "idle: nothing in flight"
    );

    std::fs::write(&gate, b"").expect("hold the fsync");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = trickle(port, std::sync::Arc::clone(&stop));
    let held = Instant::now();
    let mut max_pending = 0u64;
    let mut max_age = 0u64;
    while held.elapsed() < Duration::from_secs(8) {
        std::thread::sleep(Duration::from_millis(500));
        max_pending = max_pending.max(info_u64(&mut c, "aof_pending_bio_fsync"));
        max_age = max_age.max(info_u64(&mut c, "aof_fsync_in_flight_ms"));
    }
    let delayed_held = info_u64(&mut c, "aof_delayed_fsync") - delayed_before;
    let status_held = info_field(&mut c, "aof_last_fsync_status");
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    let warned = log.contains("Asynchronous AOF fsync is taking too long");

    std::fs::remove_file(&gate).expect("release the fsync");
    std::thread::sleep(Duration::from_millis(1_500));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let writes = writer.join().expect("writer thread");
    // Every deadline after the release finds its fsync done.
    let deadline = Instant::now() + Duration::from_secs(10);
    while info_u64(&mut c, "aof_pending_bio_fsync") != 0 {
        assert!(
            Instant::now() < deadline,
            "the released fsync never settled"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let delayed_after = info_u64(&mut c, "aof_delayed_fsync");
    std::thread::sleep(Duration::from_millis(2_500));
    let delayed_later = info_u64(&mut c, "aof_delayed_fsync");
    let (pending_after, age_after, status_after) = (
        info_u64(&mut c, "aof_pending_bio_fsync"),
        info_u64(&mut c, "aof_fsync_in_flight_ms"),
        info_field(&mut c, "aof_last_fsync_status"),
    );
    let report = format!(
        "--shards {shards}, {}: {writes} SETs; during the 8 s hold: max aof_pending_bio_fsync \
         {max_pending}, max aof_fsync_in_flight_ms {max_age}, aof_delayed_fsync +{delayed_held}, \
         status {status_held}, WARN logged {warned}; after: pending {pending_after}, age \
         {age_after}, delayed {delayed_after} -> {delayed_later}, status {status_after}",
        common::find_moon_binary().display()
    );
    eprintln!("{report}");
    assert!(max_pending >= 1, "no fsync shown in flight: {report}");
    assert!(
        max_age >= 5_000,
        "the in-flight age never passed 5 s: {report}"
    );
    assert!(
        delayed_held >= 3,
        "aof_delayed_fsync must count every 2 s: {report}"
    );
    assert!(
        warned,
        "no 'taking too long' WARN in the server log: {report}"
    );
    assert_eq!(
        (pending_after, age_after, status_after.as_str()),
        (0, 0, "ok"),
        "{report}"
    );
    assert_eq!(
        delayed_after, delayed_later,
        "the count stops with the stall: {report}"
    );
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn a_stalled_everysec_fsync_is_visible_s1() {
    stall_is_visible(1);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn a_stalled_everysec_fsync_is_visible_s4() {
    stall_is_visible(4);
}

// ── R1 review, finding 8: CONFIG SET appendfsync at runtime ─────────────────

/// `CONFIG SET appendfsync always` used to answer OK — and CONFIG GET showed
/// `always` — while every writer kept its startup policy: with the fsync
/// held, a `SET` still returned at once, acknowledged without its fsync.
/// Now the switch reaches the producers and every writer: with the fsync
/// held, the `SET` answers only after the release; switching back to
/// `everysec` acknowledges at once again while the fsync is held, and
/// `appendfsync no` does too.
fn runtime_appendfsync_switch(shards: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let gate = dir.join("sync.gate");
    let (_server, port) = spawn(dir, shards, "everysec", &gate);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", "warm", "1"]), "+OK\r\n");

    assert!(
        c.send(&["CONFIG", "SET", "appendfsync", "bogus"])
            .starts_with("-ERR")
    );
    assert_eq!(
        c.send(&["CONFIG", "GET", "appendfsync"]),
        "*2\r\n$11\r\nappendfsync\r\n$8\r\neverysec\r\n"
    );

    std::fs::write(&gate, b"").expect("hold the fsync");
    assert_eq!(
        c.send(&["CONFIG", "SET", "appendfsync", "always"]),
        "+OK\r\n"
    );
    assert_eq!(
        c.send(&["CONFIG", "GET", "appendfsync"]),
        "*2\r\n$11\r\nappendfsync\r\n$6\r\nalways\r\n"
    );
    // A SET (one per shard at --shards 4: tagged keys all over) must wait.
    let (tx, rx) = std::sync::mpsc::channel();
    let setter = std::thread::spawn(move || {
        let mut c = Conn::open(port);
        for i in 0..8 {
            let t = Instant::now();
            let r = c.send(&["SET", &format!("x{i}"), "1"]);
            tx.send((r, t.elapsed())).expect("report");
        }
    });
    let early = rx.recv_timeout(Duration::from_millis(1_500));
    std::fs::remove_file(&gate).expect("release the fsync");
    let Ok((first, waited)) = early.or_else(|_| rx.recv_timeout(Duration::from_secs(20))) else {
        panic!("--shards {shards}: the SET never answered after the release");
    };
    assert_eq!(first, "+OK\r\n");
    assert!(
        waited >= Duration::from_millis(1_400),
        "--shards {shards}: after CONFIG SET appendfsync always, a SET answered in {waited:?} \
         while its fsync was held — acknowledged without the fsync"
    );
    for _ in 1..8 {
        let (r, _) = rx
            .recv_timeout(Duration::from_secs(20))
            .expect("later SETs");
        assert_eq!(r, "+OK\r\n");
    }
    setter.join().expect("setter");

    // Back to everysec: acknowledged at once while the fsync is held.
    for policy in ["everysec", "no"] {
        assert_eq!(c.send(&["CONFIG", "SET", "appendfsync", policy]), "+OK\r\n");
        std::fs::write(&gate, b"").expect("hold the fsync");
        // Let a deadline find the held fsync (everysec hands it off).
        for i in 0..20 {
            let t = Instant::now();
            assert_eq!(c.send(&["SET", &format!("{policy}{i}"), "1"]), "+OK\r\n");
            assert!(
                t.elapsed() < Duration::from_millis(900),
                "--shards {shards}: appendfsync {policy}: a SET took {:?} with the fsync held",
                t.elapsed()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        std::fs::remove_file(&gate).expect("release the fsync");
    }
    assert_eq!(
        c.send(&["CONFIG", "SET", "appendfsync", "everysec"]),
        "+OK\r\n"
    );
    assert_eq!(c.send(&["SET", "last", "1"]), "+OK\r\n");
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn config_set_appendfsync_reaches_the_writers_s1() {
    runtime_appendfsync_switch(1);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn config_set_appendfsync_reaches_the_writers_s4() {
    runtime_appendfsync_switch(4);
}
