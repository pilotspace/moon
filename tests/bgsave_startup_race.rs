//! BGSAVE-vs-shard-startup race (found via crash_matrix_cross_plane flake,
//! 2026-07-17): the listener answers clients as soon as the FASTEST shard is
//! up, so a BGSAVE can broadcast its snapshot epoch while a slower shard is
//! still initializing its event loop. The per-shard epoch cursor used to be
//! seeded from the watch channel's CURRENT value at loop start, silently
//! swallowing such a pending trigger — that shard never snapshotted,
//! `BGSAVE_SHARDS_REMAINING` never reached zero, and `rdb_bgsave_in_progress`
//! stuck at 1 forever (every later BGSAVE refused as already-in-progress).
//!
//! `MOON_TEST_SLOW_SHARD_START_MS` (test-only fault injection in
//! `event_loop.rs`) makes the race deterministic: shards 1..N sleep before
//! entering their loops while shard 0 answers the client. Pre-fix this test
//! times out at the 10s deadline on every run; post-fix (cursor starts at 0,
//! pending triggers are honored on the first tick) it completes in <2s.
//!
//! Run with:
//!   cargo test --release --test bgsave_startup_race

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::time::{Duration, Instant};

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

struct Conn(std::net::TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(s) = std::net::TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                return Self(s);
            }
            assert!(Instant::now() < deadline, "server never accepted");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Send an inline command, return the raw reply up to one read() chunk.
    fn cmd(&mut self, line: &str) -> String {
        self.0
            .write_all(format!("{line}\r\n").as_bytes())
            .expect("write");
        let mut buf = [0u8; 64 * 1024];
        let n = self.0.read(&mut buf).expect("read");
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }
}

#[test]
fn bgsave_issued_during_shard_startup_still_completes() {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bgsave-race-tmp");
    std::fs::create_dir_all(&base).expect("create tmp base");
    let dir = tempfile::Builder::new()
        .prefix("bgrace-")
        .tempdir_in(&base)
        .expect("tempdir");

    let port = common::reserve_port();
    let mut child = std::process::Command::new(find_moon_binary());
    child
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.path().to_string_lossy(),
            "--shards",
            "4",
            "--appendonly",
            "yes",
            "--disk-free-min-pct",
            "0",
        ])
        // Shards 1-3 enter their event loops ~500ms late; shard 0 (and the
        // listener) come up immediately, so the BGSAVE below lands inside
        // the startup window.
        .env("MOON_TEST_SLOW_SHARD_START_MS", "500")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let child = child.spawn().expect("spawn moon");
    // Kill-on-drop guard: every assert! below this line panics through the
    // guard instead of orphaning the server. An orphan from THIS suite is
    // what produced the 667%-CPU incident behind issue #366 (leaked child,
    // tmpdir later cleaned, pre-fix binary error-looping its tick).
    let child = MoonGuard(Some(child));

    let mut c = Conn::open(port);
    // PING to confirm command plane is up, then fire BGSAVE immediately —
    // while shards 1-3 are still asleep in setup.
    assert!(c.cmd("PING").contains("PONG"), "server must answer PING");
    let reply = c.cmd("BGSAVE");
    assert!(
        reply.contains("Background saving started"),
        "BGSAVE must be accepted, got: {reply}"
    );

    // The snapshot must COMPLETE even though 3 of 4 shards received the
    // trigger before their loops started. Pre-fix: in_progress sticks at 1.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = c.cmd("INFO persistence");
        if info.contains("rdb_bgsave_in_progress:0") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "BGSAVE never completed — a shard swallowed the snapshot trigger \
             broadcast during its startup (INFO: {info})"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    drop(child); // MoonGuard SIGKILLs + reaps
}

/// Kill-on-drop guard so a mid-test panic can't orphan the server
/// (see tests/dir_deleted_degraded.rs for the same pattern).
struct MoonGuard(Option<std::process::Child>);

impl Drop for MoonGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            common::sigkill(&mut child);
        }
    }
}
