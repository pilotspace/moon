//! Integration test for task #58 LOW-1/LOW-2: `allocator_overhead_bytes` and
//! `pagecache_bytes` in `INFO memory`.
//!
//! Spawns the release moon binary with disk-offload (and its PageCache)
//! enabled, loads a dataset large enough to force page-cache traffic and
//! genuine allocator overhead, and asserts both new INFO fields are present
//! with sane values. Neither field feeds eviction -- this test only checks
//! observability output.
//!
//! Run with:
//!   cargo test --release --test info_memory_allocator_pagecache -- --nocapture

mod common;

use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn redis_cli_available() -> bool {
    Command::new("redis-cli")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Resolve the release binary to spawn. Honors `MOON_BIN` when set (VM /
/// worktree parity -- see `gotcha_orbstack_macho_binary_trap`).
fn release_binary() -> std::path::PathBuf {
    common::find_moon_binary()
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

const MAXMEMORY_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

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
    let tmp_dir = std::env::temp_dir().join(format!("moon-test-info-mem-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--dir",
                tmp_dir.to_str().unwrap(),
                // PageCache only exists when disk-offload is enabled --
                // required so `pagecache_bytes` has a chance to be non-zero.
                "--disk-offload",
                "enable",
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };

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

fn redis_cli(port: u16, args: &[&str]) -> Option<String> {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .ok()?;
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Extract `field:<value>` from an `INFO` bulk-string body.
fn parse_info_field(body: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}:");
    body.lines()
        .find(|l| l.starts_with(&prefix))
        .and_then(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse::<u64>().ok())
}

#[test]
fn info_memory_exposes_allocator_overhead_and_pagecache_fields() {
    let Some(m) = spawn_moon() else { return };

    // Load enough data to (a) grow RSS meaningfully above a fresh-boot
    // baseline (allocator_overhead > 0) and (b) push some pages through the
    // PageCache under disk-offload.
    for i in 0..20_000 {
        let key = format!("key:{i}");
        let val = "x".repeat(256);
        redis_cli(m.port, &["SET", &key, &val]).unwrap();
    }

    // The accounting fields are sampled on the 100ms tick -- poll INFO until
    // allocator_overhead_bytes stops reading as the pre-tick zero default.
    // Timeout after 10s (100ms tick should fire many times over).
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut allocator_overhead_bytes = 0u64;
    let mut pagecache_bytes = 0u64;
    let mut last_body = String::new();

    while Instant::now() < deadline {
        if let Some(body) = redis_cli(m.port, &["INFO", "memory"]) {
            last_body = body.clone();
            if let Some(v) = parse_info_field(&body, "allocator_overhead_bytes") {
                allocator_overhead_bytes = v;
            }
            pagecache_bytes = parse_info_field(&body, "pagecache_bytes").unwrap_or(0);
            if allocator_overhead_bytes > 0 {
                break;
            }
        }
        thread::sleep(Duration::from_millis(200));
    }

    assert!(
        last_body.contains("allocator_overhead_bytes:"),
        "INFO memory missing 'allocator_overhead_bytes' field:\n{last_body}"
    );
    assert!(
        last_body.contains("pagecache_bytes:"),
        "INFO memory missing 'pagecache_bytes' field:\n{last_body}"
    );

    // ── allocator_overhead_bytes > 0 under load (task success criteria) ──
    assert!(
        allocator_overhead_bytes > 0,
        "allocator_overhead_bytes should be > 0 after loading 20k keys, got {allocator_overhead_bytes}\n{last_body}"
    );

    // ── pagecache_bytes <= 25% of maxmemory (task success criteria) ──
    let quarter_maxmemory = MAXMEMORY_BYTES / 4;
    assert!(
        pagecache_bytes <= quarter_maxmemory,
        "pagecache_bytes ({pagecache_bytes}) exceeds 25% of maxmemory ({quarter_maxmemory})\n{last_body}"
    );
}
