//! Phase 191 PERF-11 -- mimalloc-alt smoke test.
//!
//! This test compiles only when the `mimalloc-alt` feature is enabled.
//! Run with:
//!   cargo test --release --no-default-features \
//!     --features runtime-monoio,mimalloc-alt,graph,text-index \
//!     --test allocator_mimalloc_smoke

#![cfg(feature = "mimalloc-alt")]

use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

fn redis_cli_available() -> bool {
    Command::new("redis-cli")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn spawn_moon(port: u16) -> std::process::Child {
    // Explicit --dir: the default resolves to the platform user-data
    // directory, which parallel test servers must not share.
    let dir = std::env::temp_dir().join(format!("moon-mimalloc-smoke-{port}"));
    std::fs::create_dir_all(&dir).expect("create test dir");
    Command::new(moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "no",
            "--dir",
            &dir.to_string_lossy(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn moon")
}

fn wait_ready(port: u16) {
    for _ in 0..50 {
        if Command::new("redis-cli")
            .args(["-p", &port.to_string(), "PING"])
            .output()
            .map(|o| o.stdout.starts_with(b"PONG"))
            .unwrap_or(false)
        {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("moon did not become ready on port {port}");
}

#[test]
fn mimalloc_set_get_roundtrip() {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not on PATH");
        return;
    }
    let port = 16401;
    let mut child = spawn_moon(port);
    wait_ready(port);

    let set_out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", "alloc-smoke", "mimalloc-ok"])
        .output()
        .expect("SET failed to spawn");
    let set_stdout = String::from_utf8_lossy(&set_out.stdout);

    let get_out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "GET", "alloc-smoke"])
        .output()
        .expect("GET failed to spawn");
    let get_stdout = String::from_utf8_lossy(&get_out.stdout);

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        set_stdout.contains("OK"),
        "SET expected OK, got: {set_stdout}"
    );
    assert!(
        get_stdout.contains("mimalloc-ok"),
        "GET expected `mimalloc-ok`, got: {get_stdout}"
    );
}
