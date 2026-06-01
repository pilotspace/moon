//! CRASH-01-LITE: per-shard AOF crash-recovery matrix (RFC step 8).
//!
//! Boots a multi-shard moon with `--appendonly yes`, drives a write load,
//! kills the process with SIGKILL, restarts, and asserts the recovered
//! state matches what was on disk pre-crash. Validates the full
//! step 2-5 pipeline end-to-end:
//!
//!   handler write → AOF channel → per-shard writer → fsync
//!     → SIGKILL → restart → PerShard manifest load → replay_per_shard
//!     → ordered merge (empty today) → server accepts client traffic
//!
//! Test matrix (subset of RFC § 7 — "LITE" defers cross-shard TXN and
//! BGREWRITEAOF interleaving to step 9 + future steps):
//!   - `--shards 2 --appendonly yes --appendfsync everysec` + SIGKILL
//!     → ≥99% recover (everysec fsync window allows ≤1s loss).
//!
//! Run with:
//!   cargo build --release --features runtime-monoio,jemalloc
//!   cargo test --release --features runtime-monoio,jemalloc \
//!     --test crash_matrix_per_shard_aof -- --ignored
//!
//! Requires: built release binary, `redis-cli` on PATH, monoio runtime
//! (PerShard AOF currently only ships on monoio).

#![cfg(feature = "runtime-monoio")]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

const KEY_COUNT: usize = 200;

fn unique_port() -> u16 {
    // Pick a high port and offset by current PID to avoid clashes across
    // parallel test runs in CI. 16700-17200 range is unused on dev hosts.
    16700 + (std::process::id() as u16 % 500)
}

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-crash-matrix-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path) -> Child {
    Command::new("./target/release/moon")
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "2",
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--unsafe-multishard-aof",
            "--dir",
        ])
        .arg(dir)
        // Captured to a log file so a CI flake produces a real diagnostic
        // rather than the silent "connection refused" symptom the project
        // already paid for once (see feedback_silenced_child_stdio_flake).
        .stdout(
            std::fs::File::create(dir.join("moon.stdout.log"))
                .expect("create moon stdout log"),
        )
        .stderr(
            std::fs::File::create(dir.join("moon.stderr.log"))
                .expect("create moon stderr log"),
        )
        .spawn()
        .expect("spawn moon (build --release --features runtime-monoio,jemalloc first)")
}

fn wait_for_port(port: u16) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            std::thread::sleep(Duration::from_millis(200));
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("moon did not start within 8s on port {}", port);
}

fn redis_set(port: u16, key: &str, value: &str) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", key, value])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli SET");
    assert!(
        out.status.success(),
        "redis-cli SET {} {} failed: {}",
        key,
        value,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn redis_get(port: u16, key: &str) -> Option<String> {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "GET", key])
        .output()
        .expect("redis-cli GET");
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() || s == "(nil)" {
        None
    } else {
        Some(s)
    }
}

/// SIGKILL via `kill -9` (Child::kill on Unix already sends SIGKILL but
/// being explicit here documents intent and survives stdlib changes).
#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    // Wait for the kernel to reap the process so its file handles are
    // released and the next spawn can lock the AOF files.
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn crash_01_lite_per_shard_aof_recovers_after_sigkill() {
    let port = unique_port();
    let dir = unique_dir("crash01");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1 --------------------------------------------------------
    let mut child = start_moon(port, &dir);
    wait_for_port(port);

    // Write KEY_COUNT keys. Use hash tags to deterministically spread
    // across both shards (half on each) — confirms per-shard files are
    // populated.
    let mut expected: std::collections::HashMap<String, String> =
        std::collections::HashMap::with_capacity(KEY_COUNT);
    for i in 0..KEY_COUNT {
        // Alternate hash tag → shard partition.
        let tag = if i % 2 == 0 { "a" } else { "b" };
        let key = format!("crash:{{{}}}:{}", tag, i);
        let value = format!("v-{}", i);
        redis_set(port, &key, &value);
        expected.insert(key, value);
    }

    // Wait > 1s so the everysec fsync window definitely flushed every
    // entry to durable storage.
    std::thread::sleep(Duration::from_millis(1500));

    // SIGKILL — no graceful shutdown, no chance for in-flight buffers
    // to drain.
    sigkill(&mut child);

    // -- Round 2 (recovery) ---------------------------------------------
    let mut child2 = start_moon(port, &dir);
    wait_for_port(port);

    // Verify every key recovered. The everysec contract permits up to 1s
    // of loss, but we slept 1.5s before the kill so we should see 100%.
    let mut missing: Vec<String> = Vec::new();
    let mut mismatched: Vec<String> = Vec::new();
    for (key, want) in &expected {
        match redis_get(port, key) {
            None => missing.push(key.clone()),
            Some(got) if got != *want => {
                mismatched.push(format!("{}: want={} got={}", key, want, got))
            }
            Some(_) => {}
        }
    }

    // Cleanup before any failure assertion so the temp dir isn't leaked
    // when the assertion fires.
    sigkill(&mut child2);

    assert!(
        missing.is_empty() && mismatched.is_empty(),
        "CRASH-01-LITE: {} missing, {} mismatched. Sample missing: {:?}, sample mismatched: {:?}",
        missing.len(),
        mismatched.len(),
        missing.iter().take(5).collect::<Vec<_>>(),
        mismatched.iter().take(5).collect::<Vec<_>>(),
    );

    // Successful run — clean up the temp dir.
    let _ = std::fs::remove_dir_all(&dir);
}
