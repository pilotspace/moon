//! Phase 191 PERF-10 -- verifies jemalloc arena cap is honored.
//!
//! Cross-phase dependency: requires Phase 190 Plan 02 MEMORY DOCTOR to be landed.
//! If MEMORY DOCTOR is missing, the test fails with a diagnostic pointing at
//! Phase 190 -- that is the intended gate, not a flake.

#![cfg(feature = "jemalloc")]

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
    // CARGO_BIN_EXE_<name> is set for `cargo test` over the [[bin]] target.
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn spawn_moon(extra_args: &[&str], port: u16) -> std::process::Child {
    let mut cmd = Command::new(moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--shards",
        "1",
        "--appendonly",
        "no",
    ])
    .args(extra_args)
    .stdout(Stdio::null())
    .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn moon")
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

fn parse_arenas(output: &str) -> Option<u32> {
    for line in output.lines() {
        if let Some(rest) = line.trim_start().strip_prefix("Arenas:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

fn run_memory_doctor(port: u16) -> String {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "MEMORY", "DOCTOR"])
        .output()
        .expect("redis-cli MEMORY DOCTOR failed to spawn");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn default_arenas_is_eight() {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not on PATH");
        return;
    }
    let port = 16399;
    let mut child = spawn_moon(&[], port);
    wait_ready(port);
    let output = run_memory_doctor(port);
    let _ = child.kill();
    let _ = child.wait();

    let arenas = parse_arenas(&output).unwrap_or_else(|| {
        panic!(
            "MEMORY DOCTOR did not contain `Arenas:` line. \
             Phase 190 (MEMORY DOCTOR) must land first.\nOutput:\n{output}"
        )
    });
    assert_eq!(
        arenas, 8,
        "default arenas should be 8 (PERF-10); got {arenas}"
    );
}

#[test]
fn override_via_cli_flag() {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not on PATH");
        return;
    }
    let port = 16400;
    let mut child = spawn_moon(&["--memory-arenas-cap", "4"], port);
    wait_ready(port);
    let output = run_memory_doctor(port);
    let _ = child.kill();
    let _ = child.wait();

    let arenas = parse_arenas(&output).expect("MEMORY DOCTOR missing Arenas line");
    assert_eq!(
        arenas, 4,
        "--memory-arenas-cap 4 should result in Arenas: 4; got {arenas}"
    );
}
