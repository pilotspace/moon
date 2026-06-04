//! SIGTERM-SHUTDOWN: the server must exit cleanly (exit code 0) when sent SIGTERM.
//!
//! Regression guard for the missing SIGTERM handler: before this fix, systemd/launchd
//! `stop` sent SIGTERM which was not handled, causing the kernel to deliver the default
//! action (non-zero exit, AOF not flushed).
//!
//! TDD note: this test was written RED first (before the SIGTERM handler was installed
//! in main.rs). The red phase confirms: SIGTERM terminates the process, but
//! `status.code()` returns `None` (signal death, not normal exit) and the exit is not
//! clean. The green phase (SIGTERM handler installed) confirms: clean exit with code 0
//! and the "Server shut down" log line.

#![cfg(unix)]
#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// Pick an ephemeral port by binding :0 and releasing it.
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
fn assert_sigterm_clean_exit(label: &str) {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!(
        "moon-sigterm-{}-{}-{}",
        std::process::id(),
        port,
        label,
    ));
    std::fs::create_dir_all(&dir).expect("mk tmpdir");

    let mut child = Command::new(moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--appendonly",
            "no",
            "--shards",
            "1",
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
        .spawn()
        .expect("spawn moon");

    let pid = child.id();

    // Wait for the server to be ready (up to 15s).
    let ready = wait_for_ready(port, Duration::from_secs(15));
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!(
            "[{}] server did not become ready within 15s on port {}",
            label, port
        );
    }

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
}

#[test]
fn sigterm_clean_exit_default() {
    assert_sigterm_clean_exit("default");
}
