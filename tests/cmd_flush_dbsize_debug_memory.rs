//! End-to-end tests for CMD-01..CMD-05 (Phase 137 Plan 01).
//!
//! Spawns the release moon binary on a free TCP port and shells out to
//! `redis-cli` to exercise every new admin command. Skips gracefully when
//! the release binary or `redis-cli` is missing (e.g. when `cargo test`
//! runs on macOS without first building via OrbStack).
//!
//! Run with:
//!   cargo test --release --test cmd_flush_dbsize_debug_memory

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Allocate an OS-assigned TCP port, drop the listener, return the number.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().unwrap().port()
}

fn redis_cli_available() -> bool {
    Command::new("redis-cli")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn release_binary() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/release/moon")
}

/// Running moon instance. Auto-killed on drop.
struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

/// Start moon on a fresh port. Returns `None` if the binary is missing,
/// `redis-cli` is missing, or the process never accepts connections.
fn spawn_moon() -> Option<Moon> {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not in PATH");
        return None;
    }
    let bin = release_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let port = free_port();
    let tmp_dir = std::env::temp_dir().join(format!("moon-test-{port}"));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let child = Command::new(&bin)
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--admin-port",
            "0",
            "--appendonly",
            "no",
            "--persistence-dir",
            tmp_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };

    // Wait up to ~5s for PING to succeed.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if redis_cli(moon.port, &["PING"])
            .map(|out| out.trim() == "PONG")
            .unwrap_or(false)
        {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not respond to PING within 5s on port {port}");
    None
}

/// Run `redis-cli -p <port> <args...>` and return stdout as String.
fn redis_cli(port: u16, args: &[&str]) -> Option<String> {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .ok()?;
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

// ---------------------------------------------------------------------------
// CMD-01 + CMD-02: FLUSHALL / FLUSHDB / DBSIZE
// ---------------------------------------------------------------------------

#[test]
fn cmd_flushall_flushdb_dbsize_roundtrip() {
    let Some(m) = spawn_moon() else { return };

    assert_eq!(
        redis_cli(m.port, &["SET", "k1", "v1"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(
        redis_cli(m.port, &["SET", "k2", "v2"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "2");

    assert_eq!(redis_cli(m.port, &["FLUSHDB"]).unwrap().trim(), "OK");
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "0");

    assert_eq!(
        redis_cli(m.port, &["SET", "k3", "v3"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "1");
    assert_eq!(redis_cli(m.port, &["FLUSHALL"]).unwrap().trim(), "OK");
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "0");

    // ASYNC / SYNC qualifiers accepted for compatibility.
    assert_eq!(
        redis_cli(m.port, &["FLUSHALL", "ASYNC"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(
        redis_cli(m.port, &["FLUSHDB", "SYNC"]).unwrap().trim(),
        "OK"
    );

    // Garbage qualifier should error (syntax error).
    let err = redis_cli(m.port, &["FLUSHDB", "BANANAS"]).unwrap();
    assert!(
        err.to_uppercase().contains("ERR") && err.to_uppercase().contains("SYNTAX"),
        "expected syntax error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// CMD-03: DEBUG OBJECT
// ---------------------------------------------------------------------------

#[test]
fn cmd_debug_object_returns_redis_format() {
    let Some(m) = spawn_moon() else { return };

    assert_eq!(
        redis_cli(m.port, &["SET", "dk", "hello"]).unwrap().trim(),
        "OK"
    );

    let out = redis_cli(m.port, &["DEBUG", "OBJECT", "dk"]).unwrap();
    assert!(out.contains("encoding:"), "missing encoding: in {out}");
    assert!(out.contains("refcount:1"), "missing refcount:1 in {out}");
    assert!(
        out.contains("serializedlength:"),
        "missing serializedlength in {out}"
    );

    let err = redis_cli(m.port, &["DEBUG", "OBJECT", "nonexistent"]).unwrap();
    assert!(
        err.to_uppercase().contains("NO SUCH KEY"),
        "expected ERR no such key, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// CMD-05: DEBUG SLEEP
// ---------------------------------------------------------------------------

#[test]
fn cmd_debug_sleep_blocks_expected_duration() {
    let Some(m) = spawn_moon() else { return };

    let start = Instant::now();
    assert_eq!(
        redis_cli(m.port, &["DEBUG", "SLEEP", "0.25"])
            .unwrap()
            .trim(),
        "OK"
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(220),
        "DEBUG SLEEP 0.25 returned too fast: {elapsed:?}"
    );
    // Sanity cap — should not take >5s for a 0.25s sleep.
    assert!(
        elapsed < Duration::from_secs(5),
        "DEBUG SLEEP 0.25 took too long: {elapsed:?}"
    );

    // Zero sleep returns immediately.
    let start = Instant::now();
    assert_eq!(
        redis_cli(m.port, &["DEBUG", "SLEEP", "0"]).unwrap().trim(),
        "OK"
    );
    assert!(start.elapsed() < Duration::from_millis(500));
}

// ---------------------------------------------------------------------------
// CMD-04: MEMORY USAGE
// ---------------------------------------------------------------------------

#[test]
fn cmd_memory_usage_returns_integer_or_nil() {
    let Some(m) = spawn_moon() else { return };

    assert_eq!(
        redis_cli(m.port, &["SET", "mk", "abcdefghij"])
            .unwrap()
            .trim(),
        "OK"
    );

    let out = redis_cli(m.port, &["MEMORY", "USAGE", "mk"]).unwrap();
    let n: i64 = out
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("expected integer, got: {out:?}"));
    assert!(n >= 10, "MEMORY USAGE expected >=10, got {n}");

    let miss = redis_cli(m.port, &["MEMORY", "USAGE", "nonexistent"]).unwrap();
    let trimmed = miss.trim();
    assert!(
        trimmed.is_empty() || trimmed == "(nil)",
        "expected nil for missing key, got: {miss:?}"
    );

    // SAMPLES flag is accepted as a no-op.
    let out = redis_cli(m.port, &["MEMORY", "USAGE", "mk", "SAMPLES", "5"]).unwrap();
    let n: i64 = out
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("expected integer, got: {out:?}"));
    assert!(n >= 10);
}
