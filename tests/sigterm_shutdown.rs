//! SIGTERM-SHUTDOWN: the server must exit cleanly (exit code 0) when sent SIGTERM.
//!
//! Regression guard for the missing SIGTERM handler: before this fix, systemd/launchd
//! `stop` sent SIGTERM which was not handled, so the kernel delivered the default
//! action (signal death, graceful-shutdown path skipped).
//!
//! What IS asserted: `status.code() == Some(0)` — i.e. the process took the graceful
//! shutdown path (signal death reports `code() == None`). Exit code 0 is only
//! reachable through main()'s normal return after `broadcast_shutdown` + shard joins.
//! What is NOT asserted: log output or AOF contents (the test runs with
//! `--appendonly no`; an AOF flush-on-SIGTERM integration test is follow-up work).
//!
//! TDD note: this test was written RED first (before the SIGTERM handler was installed
//! in main.rs). The red phase confirmed `status.code()` returns `None` (signal death).

#![cfg(unix)]
#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// Readiness poll deadline (RSS/CPU wave 5, item C8).
///
/// Was a fixed 15s, hardcoded independently at each `wait_for_ready` call
/// site — PR #218 documented this as a flake under host load (shared/
/// contended CI runners, or the OrbStack virtiofs starvation gotcha in
/// CLAUDE.md) rather than a real readiness regression, since the poll
/// loop itself is already load-tolerant (100ms re-poll, no fixed sleep
/// before the first attempt). Widened to 60s: still fails fast on an
/// actually-broken server (the 100ms poll notices readiness within one
/// tick of it happening), but tolerates a slow CI host without flaking.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Pick an ephemeral port by binding :0 and releasing it.
///
/// Note: classic TOCTOU race — another process can grab the port between the
/// `drop(l)` and the server's bind. Acceptable in test code: ephemeral-range
/// collisions are rare, and a collision fails loudly in `wait_for_ready`.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// Poll TCP connect until the server is ready or deadline expires.
/// Returns true if we received a PONG within the deadline.
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

/// Send SIGTERM to the given PID using `kill -TERM`.
fn send_sigterm(pid: u32) {
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("failed to run kill");
    assert!(status.success(), "kill -TERM failed: {:?}", status);
}

/// Core test body: spawn moon, wait for readiness, send SIGTERM, assert clean exit.
///
/// `extra_args` is appended to the base arg list so individual cases can enable
/// optional subsystems (e.g. `--admin-port`) that spawn threads earlier in main().
fn assert_sigterm_clean_exit(label: &str, extra_args: &[&str]) {
    assert_sigterm_clean_exit_shards(label, 1, extra_args, ConnAtSigterm::None)
}

/// Connection state to hold when SIGTERM lands — the zombie review
/// (tmp/RSS-CPU-REVIEW.md item 1) implicates the per-shard accept path, and
/// the bench-harness hangs happened with recent/live connections, so the
/// matrix covers both.
enum ConnAtSigterm {
    /// No client connected (beyond the readiness probe, already closed).
    None,
    /// One client connection held OPEN across the SIGTERM.
    HeldOpen,
}

fn assert_sigterm_clean_exit_shards(
    label: &str,
    shards: u32,
    extra_args: &[&str],
    conn: ConnAtSigterm,
) {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!(
        "moon-sigterm-{}-{}-{}",
        std::process::id(),
        port,
        label,
    ));
    std::fs::create_dir_all(&dir).expect("mk tmpdir");

    // Bind to locals so the &str slices in base_args are not temporaries.
    let port_str = port.to_string();
    let dir_str = dir.to_string_lossy().into_owned();
    let shards_str = shards.to_string();

    let mut base_args: Vec<&str> = vec![
        "--port",
        &port_str,
        "--dir",
        &dir_str,
        "--appendonly",
        "no",
        "--shards",
        &shards_str,
    ];
    base_args.extend_from_slice(extra_args);

    let mut child = Command::new(moon_binary())
        .args(&base_args)
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
        .spawn()
        .expect("spawn moon");

    let pid = child.id();

    // Wait for the server to be ready (load-tolerant deadline; see
    // READY_TIMEOUT doc comment).
    let ready = wait_for_ready(port, READY_TIMEOUT);
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!(
            "[{}] server did not become ready within {:?} on port {}",
            label, READY_TIMEOUT, port
        );
    }

    // Optionally hold a live client connection across the SIGTERM (verified
    // alive with a PING round-trip so the conn is fully registered on a shard).
    let held = match conn {
        ConnAtSigterm::None => None,
        ConnAtSigterm::HeldOpen => {
            let addr = format!("127.0.0.1:{port}");
            let mut s =
                TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_secs(2))
                    .expect("held conn connect");
            s.set_read_timeout(Some(Duration::from_secs(2))).ok();
            s.write_all(b"PING\r\n").expect("held conn ping");
            let mut buf = [0u8; 16];
            let n = s.read(&mut buf).expect("held conn pong");
            assert!(buf[..n].windows(4).any(|w| w == b"PONG"), "held conn PONG");
            Some(s)
        }
    };

    // Send SIGTERM and wait for the process to exit.
    send_sigterm(pid);

    let deadline = Instant::now();
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(s) => break s,
            None => {
                if deadline.elapsed() >= Duration::from_secs(10) {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("[{}] server did not exit within 10s after SIGTERM", label);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };

    // Clean exit: code must be Some(0), not None (signal death) or Some(non-zero).
    assert_eq!(
        status.code(),
        Some(0),
        "[{}] expected exit code 0 after SIGTERM, got {:?}",
        label,
        status
    );
    drop(held);
}

#[test]
fn sigterm_clean_exit_default() {
    assert_sigterm_clean_exit("default", &[]);
}

/// Regression guard: the Prometheus/admin thread is spawned at line ~267 in
/// main(), which is BEFORE the old (wrong) location of the SIGTERM block (~370).
/// With `--admin-port` set, that thread exists with SIGTERM unblocked unless the
/// block runs at the very top of main (current correct position: ~69).
///
/// If someone moves the sigmask call back below metrics init, a process-directed
/// SIGTERM will be delivered to the admin thread (SIG_DFL → terminate with
/// signal 15), this test turns RED, and the regression is caught.
#[test]
fn sigterm_clean_exit_with_admin_port() {
    let admin_port = free_port();
    let admin_port_str = admin_port.to_string();
    assert_sigterm_clean_exit("admin", &["--admin-port", &admin_port_str]);
}

// ── Zombie-review matrix (tmp/RSS-CPU-REVIEW.md item 1) ─────────────────────
//
// The detached monoio per-shard accept task never observes the shutdown
// token; the documented bench-harness hangs involved multi-shard SO_REUSEPORT
// servers and live/recent connections. Each ingredient gets a case.

#[test]
fn sigterm_clean_exit_shards_4_idle() {
    assert_sigterm_clean_exit_shards("s4-idle", 4, &[], ConnAtSigterm::None);
}

#[test]
fn sigterm_clean_exit_shards_4_held_conn() {
    assert_sigterm_clean_exit_shards("s4-held", 4, &[], ConnAtSigterm::HeldOpen);
}

#[test]
fn sigterm_clean_exit_shards_1_held_conn() {
    assert_sigterm_clean_exit_shards("s1-held", 1, &[], ConnAtSigterm::HeldOpen);
}

/// The zombie-CPU-eater composition: busy-poll spin configured AND SIGTERM'd.
#[test]
fn sigterm_clean_exit_busy_poll() {
    assert_sigterm_clean_exit_shards(
        "s2-busypoll",
        2,
        &["--io-busy-poll-us", "40"],
        ConnAtSigterm::HeldOpen,
    );
}

/// Bench-harness shape: SIGTERM lands while many clients hammer pipelined
/// writes (the documented GCE hang always involved live benchmark traffic).
/// Writer threads tolerate every I/O error — after SIGTERM their connections
/// are expected to die; the only assertion is that the SERVER exits cleanly.
#[test]
fn sigterm_clean_exit_under_write_storm() {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!(
        "moon-sigterm-{}-{}-storm",
        std::process::id(),
        port
    ));
    std::fs::create_dir_all(&dir).expect("mk tmpdir");
    let port_str = port.to_string();
    let dir_str = dir.to_string_lossy().into_owned();

    let mut child = Command::new(moon_binary())
        .args([
            "--port",
            &port_str,
            "--dir",
            &dir_str,
            "--appendonly",
            "yes",
            "--shards",
            "4",
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon");
    let pid = child.id();

    if !wait_for_ready(port, READY_TIMEOUT) {
        let _ = child.kill();
        let _ = child.wait();
        panic!("[storm] server did not become ready within {READY_TIMEOUT:?} on port {port}");
    }

    // 16 writer threads, each pipelining SETs in a tight loop until the
    // connection dies or the stop flag flips.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..16)
        .map(|t| {
            let stop = stop.clone();
            let addr = format!("127.0.0.1:{port}");
            std::thread::spawn(move || {
                let Ok(mut s) = TcpStream::connect_timeout(
                    &addr.parse().expect("addr"),
                    Duration::from_secs(2),
                ) else {
                    return;
                };
                s.set_write_timeout(Some(Duration::from_millis(500))).ok();
                s.set_read_timeout(Some(Duration::from_millis(500))).ok();
                let mut n = 0u64;
                let mut buf = [0u8; 4096];
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // 8-deep pipeline of SETs; ignore all errors (the server
                    // is being killed under us — that's the point).
                    let mut pipe = Vec::new();
                    for i in 0..8 {
                        pipe.extend_from_slice(
                            format!("SET k:{t}:{}:{i} v{n}\r\n", n % 512).as_bytes(),
                        );
                    }
                    if s.write_all(&pipe).is_err() {
                        break;
                    }
                    let _ = s.read(&mut buf);
                    n += 1;
                }
            })
        })
        .collect();

    // Let the storm establish, then SIGTERM mid-flight.
    std::thread::sleep(Duration::from_millis(500));
    send_sigterm(pid);

    let deadline = Instant::now();
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(s) => break s,
            None => {
                if deadline.elapsed() >= Duration::from_secs(10) {
                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("[storm] server did not exit within 10s after SIGTERM");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in writers {
        let _ = w.join();
    }
    assert_eq!(
        status.code(),
        Some(0),
        "[storm] expected exit code 0 after SIGTERM under load, got {status:?}"
    );
}
