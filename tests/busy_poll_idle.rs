//! `--io-busy-poll-us` must not burn CPU on an IDLE server.
//!
//! RSS/CPU/OOM review item 2 (CPU lens): the poll-mode park spins its full
//! budget on EVERY park even with zero connections and zero work — an idle
//! server burns ~budget/park-interval CPU per shard thread, forever (the
//! "zombie CPU eater" when such a server is orphaned). The spin exists to
//! delete wake latency UNDER TRAFFIC; after an idle window it must disengage
//! and fall back to plain blocking parks, re-arming on the next real event.
//!
//! RED before the fix: an idle 4-shard server with `--io-busy-poll-us 200`
//! accumulates CPU time at roughly 0.8 cores (200µs spin per ~1ms timer
//! park × 4 shard threads); the 3s idle window below measures well over
//! 500ms of CPU. GREEN after: background ticks only, well under the bound.
//!
//! Linux-only (reads /proc/<pid>/stat). The busy-poll CLI flag forces the
//! epoll/kqueue LegacyDriver, so this exercises the patched spin path on
//! both io_uring-capable and MOON_NO_URING environments.
//!
//! monoio-only: under the tokio runtime `--io-busy-poll-us` is an explicit
//! no-op (main.rs warns and skips the spin wiring), so there is no spin to
//! measure — and a debug-build tokio server's unrelated idle-tick cost
//! (~0.9s/3s at 4 shards on CI) would fail the bound spuriously.

#![cfg(all(unix, target_os = "linux", feature = "runtime-monoio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn moon with `--io-busy-poll-us <poll_us>` on `shards` shards and wait
/// until it ACCEPTS a connection (see `common::spawn_listening`).
fn spawn_moon(dir: &std::path::Path, shards: u32, poll_us: &str) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--io-busy-poll-us",
                poll_us,
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

fn wait_for_ready(port: u16, deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        let addr = format!("127.0.0.1:{port}");
        if let Ok(mut s) =
            TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(200))
        {
            s.set_read_timeout(Some(Duration::from_millis(500))).ok();
            s.set_write_timeout(Some(Duration::from_millis(500))).ok();
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 16];
                if let Ok(n) = s.read(&mut buf) {
                    if buf[..n].windows(4).any(|w| w == b"PONG") {
                        return true;
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Process CPU time (user + system) in seconds from /proc/<pid>/stat.
fn cpu_seconds(pid: u32) -> f64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).expect("read /proc stat");
    // comm can contain spaces — split after the closing paren.
    let after = stat.rsplit_once(')').expect("stat paren").1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    // After ')': field[0]=state ... utime is the 12th, stime the 13th.
    let utime: u64 = fields[11].parse().expect("utime");
    let stime: u64 = fields[12].parse().expect("stime");
    // SAFETY: sysconf(_SC_CLK_TCK) reads a static kernel constant; it takes
    // no pointers and touches no shared state.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    (utime + stime) as f64 / hz
}

#[test]
fn busy_poll_idle_server_does_not_burn_cpu() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (guard, port) = spawn_moon(dir.path(), 4, "200");
    let pid = guard.0.id();

    assert!(
        wait_for_ready(port, Duration::from_secs(15)),
        "server never ready"
    );

    // Settle past startup (recovery scan, first ticks) AND past the idle
    // disengage window, then measure a fully idle interval.
    std::thread::sleep(Duration::from_secs(1));
    let before = cpu_seconds(pid);
    std::thread::sleep(Duration::from_secs(3));
    let burned = cpu_seconds(pid) - before;

    // RED (spin always engaged): ~200µs per ~1ms park × 4 shards ≈ 2.4s here.
    // GREEN (idle disengage): background ticks only — comfortably < 0.5s
    // even on a loaded CI runner.
    assert!(
        burned < 0.5,
        "idle 4-shard server with --io-busy-poll-us 200 burned {burned:.2}s \
         CPU in 3s — spin never disengages when idle"
    );
}

/// Traffic after an idle period must still be served (the disengaged spin
/// re-arms via the normal wake path — correctness, not latency, asserted).
#[test]
fn busy_poll_recovers_after_idle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (guard, port) = spawn_moon(dir.path(), 2, "40");

    assert!(
        wait_for_ready(port, Duration::from_secs(15)),
        "server never ready"
    );
    // Idle well past any disengage window, then talk to every shard's path.
    std::thread::sleep(Duration::from_secs(2));
    let addr = format!("127.0.0.1:{port}");
    let mut s = TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_secs(2))
        .expect("post-idle connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    for i in 0..100 {
        s.write_all(format!("SET pk:{i} v{i}\r\nGET pk:{i}\r\n").as_bytes())
            .expect("post-idle write");
        let mut buf = [0u8; 128];
        let n = s.read(&mut buf).expect("post-idle read");
        assert!(n > 0, "no reply after idle re-engage (op {i})");
    }
    drop(guard);
}
