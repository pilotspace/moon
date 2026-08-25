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

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// Readiness poll deadline (RSS/CPU wave 5, item C8; widened again below for
/// the sigterm-shutdown flake hardening pass).
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

/// CI-aware readiness deadline: doubles `READY_TIMEOUT` when the `CI` env var
/// is set (GitHub Actions and effectively every other CI provider sets it).
///
/// A loaded macOS CI runner just needs longer to fork+exec+bind+listen the
/// spawned `moon` binary — this is startup latency, not a hang, and the poll
/// loop already retries every 100ms rather than sleeping once and giving up.
/// Local `cargo test` runs stay at the already-generous 60s base so a
/// genuinely broken server still fails a local run promptly.
fn ready_timeout() -> Duration {
    if std::env::var_os("CI").is_some() {
        READY_TIMEOUT * 2
    } else {
        READY_TIMEOUT
    }
}

/// Read a captured child-process log file for diagnostics. Tolerates a
/// missing/unreadable file — this runs on a failure path and must never
/// itself panic (that would mask the real failure with a confusing one).
fn read_log(path: &std::path::Path) -> String {
    match std::fs::read_to_string(path) {
        Ok(s) if s.trim().is_empty() => "<empty>".to_string(),
        Ok(s) => s,
        Err(e) => format!("<unreadable: {e}>"),
    }
}

/// Outcome of polling for server readiness.
enum Readiness {
    /// TCP connect + PING/PONG round-tripped within the deadline.
    Ready,
    /// The child process exited (crashed, or exited some other way) before
    /// ever becoming ready — a REAL failure, distinct from "just slow".
    Exited(std::process::ExitStatus),
    /// Neither happened before the deadline. On a loaded CI host this is
    /// almost always still-starting-up, not a hang.
    TimedOut,
}

/// Poll TCP connect until the server is ready, the child process exits, or
/// the deadline expires.
///
/// Checking `try_wait()` every iteration means a genuinely crashed server
/// fails fast (within one 100ms tick) instead of burning the entire
/// `deadline` budget polling a socket nothing will ever bind — the old
/// bool-returning version couldn't distinguish "slow to start" from "already
/// dead", so a real crash looked identical to a loaded CI host and both
/// produced the same uninformative "did not become ready" panic.
fn wait_for_ready(child: &mut std::process::Child, port: u16, deadline: Duration) -> Readiness {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            return Readiness::Exited(status);
        }
        let addr = format!("127.0.0.1:{port}");
        if let Ok(mut s) =
            TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(200))
        {
            s.set_read_timeout(Some(Duration::from_millis(500))).ok();
            s.set_write_timeout(Some(Duration::from_millis(500))).ok();
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 16];
                if let Ok(n) = s.read(&mut buf)
                    && buf[..n].windows(4).any(|w| w == b"PONG")
                {
                    return Readiness::Ready;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Readiness::TimedOut
}

/// Build a diagnostic panic message for a non-`Ready` outcome, surfacing the
/// child's captured stdout/stderr so a REAL failure (crash, bind error, panic
/// in `main`) is still diagnosable instead of just "did not become ready".
fn readiness_failure_message(
    label: &str,
    port: u16,
    dir: &std::path::Path,
    deadline: Duration,
    outcome: &Readiness,
) -> String {
    let stdout = read_log(&dir.join("moon.stdout.log"));
    let stderr = read_log(&dir.join("moon.stderr.log"));
    let what = match outcome {
        Readiness::Exited(status) => {
            format!("server process exited with {status:?} before becoming ready")
        }
        Readiness::TimedOut => {
            format!("server did not become ready within {deadline:?} (process still running)")
        }
        Readiness::Ready => unreachable!("readiness_failure_message called with Ready"),
    };
    format!(
        "[{label}] {what} on port {port}\n--- moon.stdout.log ---\n{stdout}\n--- moon.stderr.log ---\n{stderr}"
    )
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
    let dir = std::env::temp_dir().join(format!("moon-sigterm-{}-{}", std::process::id(), label,));
    std::fs::create_dir_all(&dir).expect("mk tmpdir");

    let dir_str = dir.to_string_lossy().into_owned();
    let shards_str = shards.to_string();

    let mut base_args: Vec<String> = vec![
        "--dir".into(),
        dir_str,
        "--appendonly".into(),
        "no".into(),
        "--shards".into(),
        shards_str,
    ];
    base_args.extend(extra_args.iter().map(|s| s.to_string()));

    let (mut child, port) = common::spawn_listening_guarded(|port| {
        let mut args: Vec<String> = vec!["--port".into(), port.to_string()];
        args.extend(base_args.iter().cloned());
        Command::new(moon_binary())
            .args(&args)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
            .spawn()
            .expect("spawn moon")
    });

    let pid = child.id();

    // Wait for the server to be ready (CI-aware, load-tolerant deadline; see
    // ready_timeout() doc comment). wait_for_ready also fails fast on an
    // early process exit rather than burning the whole budget.
    let deadline = ready_timeout();
    let outcome = wait_for_ready(child.as_mut(), port, deadline);
    if !matches!(outcome, Readiness::Ready) {
        let msg = readiness_failure_message(label, port, &dir, deadline, &outcome);
        child.kill_now();
        panic!("{msg}");
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
        match child.as_mut().try_wait().expect("try_wait") {
            Some(s) => break s,
            None => {
                if deadline.elapsed() >= Duration::from_secs(10) {
                    child.kill_now();
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
    // Not the main server port `spawn_listening` binds/retries — an
    // independent secondary listener the CLI needs to know about up front.
    // `common::reserve_port` still dedups it against every other port this
    // process has handed out (the intra-process half of the port-flake fix);
    // it just can't defend against an external steal the way
    // `spawn_listening`'s respawn-on-dead-child does for the primary port.
    let admin_port = common::reserve_port();
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
    let dir = std::env::temp_dir().join(format!("moon-sigterm-{}-storm", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mk tmpdir");
    let dir_str = dir.to_string_lossy().into_owned();

    let (mut child, port) = common::spawn_listening_guarded(|port| {
        Command::new(moon_binary())
            .args([
                "--port",
                &port.to_string(),
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
            .expect("spawn moon")
    });
    let pid = child.id();

    let deadline = ready_timeout();
    let outcome = wait_for_ready(child.as_mut(), port, deadline);
    if !matches!(outcome, Readiness::Ready) {
        let msg = readiness_failure_message("storm", port, &dir, deadline, &outcome);
        child.kill_now();
        panic!("{msg}");
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
        match child.as_mut().try_wait().expect("try_wait") {
            Some(s) => break s,
            None => {
                if deadline.elapsed() >= Duration::from_secs(10) {
                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                    child.kill_now();
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
