//! Data-dir instance lock: two moon processes on the SAME `--dir` means two
//! writers on the same per-shard WAL / AOF / spill manifests — silent
//! corruption. Nothing prevented a double-start (observed in dev: dozens of
//! leaked instances accumulating against shared folders, 2026-07-16).
//!
//! Contract under test:
//!   1. A second instance pointed at a locked --dir must REFUSE to start
//!      (non-zero exit, diagnostic mentioning the lock/holder) and must not
//!      disturb the first instance.
//!   2. The lock dies with the process — after `kill -9` (no clean shutdown,
//!      no unlock code runs) an immediate same-dir restart must succeed.
//!      This is why it's an OS advisory lock (flock), not a pidfile.
//!
//! Run with:
//!   cargo test --release --test instance_lock

#![allow(clippy::unwrap_used)]

mod common;

use std::process::Command;
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

/// Repo-volume scratch (see gotcha_vm_diskfull_shared_volume); the guard is
/// also disabled explicitly below.
fn test_tmpdir() -> tempfile::TempDir {
    let base =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/instance-lock-tmp");
    std::fs::create_dir_all(&base).expect("create instance-lock-tmp base dir");
    tempfile::Builder::new()
        .prefix("ilock-")
        .tempdir_in(&base)
        .expect("tempdir_in target/instance-lock-tmp")
}

fn moon_cmd(dir: &std::path::Path, port: u16, log_prefix: &str) -> Command {
    let mut cmd = Command::new(find_moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--dir",
        &dir.to_string_lossy(),
        "--shards",
        "1",
        "--appendonly",
        "yes",
        "--disk-free-min-pct",
        "0",
    ])
    .stdout(
        std::fs::File::create(dir.join(format!("{log_prefix}.stdout.log"))).expect("stdout log"),
    )
    .stderr(
        std::fs::File::create(dir.join(format!("{log_prefix}.stderr.log"))).expect("stderr log"),
    );
    cmd
}

fn ping_ok(port: u16) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = std::net::TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().unwrap(),
        Duration::from_millis(200),
    ) else {
        return false;
    };
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    if s.write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
        return false;
    }
    let mut buf = [0u8; 7];
    s.read_exact(&mut buf).is_ok() && &buf == b"+PONG\r\n"
}

fn wait_ping(port: u16, deadline: Duration) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if ping_ok(port) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

#[test]
fn second_instance_on_same_dir_is_refused_and_kill9_releases_the_lock() {
    let dir = test_tmpdir();

    // First instance: must come up normally.
    let port_a = common::reserve_port();
    let mut a = moon_cmd(dir.path(), port_a, "moon-a")
        .spawn()
        .expect("spawn A");
    assert!(
        wait_ping(port_a, Duration::from_secs(30)),
        "first instance never became ready"
    );

    // Second instance, SAME --dir, different port: must exit non-zero,
    // quickly, with a diagnostic — not run alongside A.
    let port_b = common::reserve_port();
    let mut b = moon_cmd(dir.path(), port_b, "moon-b")
        .spawn()
        .expect("spawn B");
    let b_deadline = Instant::now() + Duration::from_secs(15);
    let b_status = loop {
        if let Some(status) = b.try_wait().expect("try_wait B") {
            break status;
        }
        assert!(
            Instant::now() < b_deadline,
            "second instance kept running against a locked dir — double-start \
             was not refused (this is the pre-fix behavior)"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(
        !b_status.success(),
        "second instance exited zero — expected a refused start"
    );
    let b_stderr =
        std::fs::read_to_string(dir.path().join("moon-b.stderr.log")).unwrap_or_default();
    assert!(
        b_stderr.to_lowercase().contains("lock"),
        "refusal diagnostic must mention the lock; got stderr:\n{b_stderr}"
    );

    // A must be undisturbed by B's failed start.
    assert!(
        ping_ok(port_a),
        "first instance stopped answering after the refused double-start"
    );

    // kill -9 A: no unlock code runs. An immediate same-dir restart must
    // succeed (the kernel released the flock with the process).
    common::sigkill(&mut a);
    common::wait_for_port_down(port_a);

    let port_c = common::reserve_port();
    let mut c = moon_cmd(dir.path(), port_c, "moon-c")
        .spawn()
        .expect("spawn C");
    assert!(
        wait_ping(port_c, Duration::from_secs(30)),
        "restart after kill -9 never became ready — stale lock? stderr:\n{}",
        std::fs::read_to_string(dir.path().join("moon-c.stderr.log")).unwrap_or_default()
    );
    common::sigkill(&mut c);
    let _ = c.wait();
    let _ = a.wait();
    let _ = b.wait();
}

/// A `--disk-offload-dir` that spells the SAME directory as `--dir`
/// differently (here via a `..` round-trip) must not self-conflict on the
/// shared moon.lock: flock conflicts are per open-file-description, so a
/// naive textual comparison would take a second lock on the same file and
/// abort startup with a false "another instance" refusal.
#[test]
fn same_dir_offload_spelled_differently_does_not_self_conflict() {
    let dir = test_tmpdir();
    let base = dir.path();
    let respelled = base
        .join("..")
        .join(base.file_name().expect("tempdir has a basename"));

    let port = common::reserve_port();
    let mut a = moon_cmd(base, port, "moon-respell");
    a.args([
        "--disk-offload",
        "enable",
        "--disk-offload-dir",
        &respelled.to_string_lossy(),
    ]);
    let mut a = a.spawn().expect("spawn with respelled offload dir");
    assert!(
        wait_ping(port, Duration::from_secs(30)),
        "startup self-conflicted on its own lock — same-dir --disk-offload-dir \
         via a different spelling must not be treated as a second instance; \
         stderr:\n{}",
        std::fs::read_to_string(base.join("moon-respell.stderr.log")).unwrap_or_default()
    );
    common::sigkill(&mut a);
    let _ = a.wait();
}
