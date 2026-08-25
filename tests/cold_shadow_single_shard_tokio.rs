//! CRASH-COLD-OVERWRITE (single-shard / tokio variant): task #56 finding 2
//! (adversarial review).
//!
//! `Database::demote_replayed_cold_shadows` (added to fix "used_memory got
//! worse after restart" — AOF replay re-hydrating already-cold keys into
//! hot RAM) was only wired in at two call sites in `src/main.rs`, both
//! inside the manifest-based (multi-part / per-shard) AOF replay branches.
//! Under `runtime-tokio` + `--shards 1`, NO such manifest is ever created
//! (`AofManifest::initialize` is `#[cfg(feature = "runtime-monoio")]`-only
//! for that config), so those branches never run at all — the ONLY KV
//! replay for that runtime/shard combination happens inside
//! `recover_shard_v3_pitr`'s own "Phase 4b" fallback
//! (`src/persistence/recovery.rs`), which replays `appendonly.aof` directly
//! whenever the disk-offload WAL v3 carried zero KV commands (the default,
//! `--wal-kv-log` off). That fallback had no demotion call at all, leaving
//! this specific runtime/shard combination just as exposed to the
//! AOF-replay-rehydrates-cold-shadows bug as the manifest-based paths were
//! before task #56 fixed those.
//!
//! This test exercises exactly that gap: single shard, and (per the
//! documented monoio write-gate/inline-SET gotcha) uses SETEX rather than a
//! bare top-level SET so it is provably going through the normal dispatch
//! path rather than any inline fast path.
//!
//! To actually exercise the tokio-runtime code path this test targets, MUST
//! be run against a binary built WITHOUT default features:
//!   cargo build --release --no-default-features --features runtime-tokio,jemalloc
//!   MOON_BIN=./target/release/moon cargo test --release \
//!       --test cold_shadow_single_shard_tokio -- --ignored --nocapture
//!
//! `common::find_moon_binary()` falls back to a default-features (monoio)
//! build if `MOON_BIN` is unset — the scenario below is a valid regression
//! guard either way (the `Database::set` fix and the Phase 4b demotion call
//! are both runtime-agnostic), but only a tokio-feature binary proves this
//! SPECIFIC gap (main.rs's manifest branches never running) is closed.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

const PROBE_COUNT: usize = 60;
const PROBE_VALUE_LEN: usize = 500;
const FILLER_COUNT: usize = 12_000;
const FILLER_VALUE_LEN: usize = 600;
const MAXMEMORY_BYTES: usize = 4 * 1024 * 1024;
const SETTLE_AFTER_FILLER: u64 = 8;
const SETTLE_AFTER_OVERWRITE: u64 = 3;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-cold-overwrite-1shard-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn spawn_moon(port: u16, dir: &std::path::Path) -> Child {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--maxmemory",
            &MAXMEMORY_BYTES.to_string(),
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "enable",
            "--disk-offload-dir",
            off_dir.to_str().expect("off dir utf8"),
            "--appendonly",
            "yes",
            "--cold-orphan-sweep-interval-secs",
            "3600",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create moon stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create moon stderr log"))
        .spawn()
        .expect("spawn moon")
}

fn probe_key(i: usize) -> String {
    format!("probe:{}", i)
}

fn assert_no_error_reply(op: &str, out: &std::process::Output) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && !stdout.contains("MOONERR") && !stdout.starts_with("ERR"),
        "redis-cli {op} rejected by server: stdout={} stderr={}",
        stdout.trim(),
        stderr.trim()
    );
}

/// SETEX rather than plain SET — the monoio write-gate/inline-SET gotcha:
/// a bare top-level SET can hit an inline fast path that bypasses the
/// normal dispatch this test wants to exercise. SETEX always carries an
/// expiry argument, forcing the full command path.
fn redis_setex(port: u16, key: &str, ttl_secs: u64, value: &str) {
    let out = Command::new("redis-cli")
        .args([
            "-p",
            &port.to_string(),
            "SETEX",
            key,
            &ttl_secs.to_string(),
            value,
        ])
        .output()
        .expect("redis-cli SETEX");
    assert_no_error_reply(&format!("SETEX {key}"), &out);
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

/// Long TTL so expiry never interferes with the crash-recovery window; the
/// point is exercising SETEX's dispatch path, not testing expiry itself.
const LONG_TTL_SECS: u64 = 3600;

fn write_filler(port: u16) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect for filler");
    stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
    let val = "F".repeat(FILLER_VALUE_LEN);
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    for i in 0..FILLER_COUNT {
        let key = format!("filler:{}", i);
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            val.len(),
            val
        );
        buf.extend_from_slice(cmd.as_bytes());
        if buf.len() >= 64 * 1024 {
            stream.write_all(&buf).expect("filler write");
            buf.clear();
        }
    }
    if !buf.is_empty() {
        stream.write_all(&buf).expect("filler tail write");
    }
    stream.flush().ok();
}

fn count_heap_files(dir: &std::path::Path) -> usize {
    let off = dir.join("off");
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for entry in rd.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                {
                    *acc += 1;
                }
            }
        }
    }
    let mut acc = 0;
    walk(&off, &mut acc);
    acc
}

const RESTART_ATTEMPTS: usize = 6;

/// Restart on the SAME port + data dir, retrying the transient rebind
/// EADDRINUSE self-shutdown race (see crash_recovery_disk_offload_no_aof.rs
/// for the full rationale). Deliberately NOT `common::spawn_listening` here:
/// that helper reserves a fresh port on each retry, but a restart must reuse
/// the exact port/dir this scenario already spawned on.
fn restart_moon_alive(port: u16, dir: &std::path::Path) -> common::ServerGuard {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = common::ServerGuard::new(spawn_moon(port, dir));
        let mut up = false;
        for _ in 0..80 {
            if let Ok(Some(_status)) = child.as_mut().try_wait() {
                break;
            }
            if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                std::thread::sleep(Duration::from_millis(200));
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if up {
            return child;
        }
        child.kill_now();
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!("moon failed to restart+serve on port {port} after {RESTART_ATTEMPTS} attempts");
}

#[test]
#[ignore] // requires a built moon binary (MOON_BIN or target/release/moon) + redis-cli
fn single_shard_overwritten_cold_key_returns_new_value_after_crash() {
    let dir = unique_dir("overwrite");
    std::fs::create_dir_all(&dir).expect("create test dir");

    let (mut server, port) = common::spawn_listening_guarded(|p| spawn_moon(p, &dir));

    let v1 = "1".repeat(PROBE_VALUE_LEN);
    for i in 0..PROBE_COUNT {
        redis_setex(port, &probe_key(i), LONG_TTL_SECS, &v1);
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));

    let heap_files = count_heap_files(&dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill \
         (single-shard budget may need retuning for this binary)"
    );

    let v2 = "2".repeat(PROBE_VALUE_LEN);
    for i in 0..PROBE_COUNT {
        redis_setex(port, &probe_key(i), LONG_TTL_SECS, &v2);
    }
    assert_eq!(
        redis_get(port, &probe_key(0)).as_deref(),
        Some(v2.as_str()),
        "precondition failed: probe:0 did not read back v2 before the crash"
    );
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_OVERWRITE));

    server.kill_now();
    common::wait_for_port_down(port);

    let mut server2 = restart_moon_alive(port, &dir);

    let mut wrong = 0usize;
    let mut missing = 0usize;
    let mut example = String::new();
    for i in 0..PROBE_COUNT {
        match redis_get(port, &probe_key(i)) {
            Some(v) if v == v2 => {}
            Some(v) => {
                wrong += 1;
                if example.is_empty() {
                    example = format!("{} => {}", probe_key(i), &v[..v.len().min(16)]);
                }
            }
            None => missing += 1,
        }
    }

    server2.kill_now();
    if wrong == 0 && missing == 0 && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }

    assert_eq!(
        missing, 0,
        "{missing} probe(s) missing entirely after crash recovery (heap files at kill time: {heap_files})"
    );
    assert_eq!(
        wrong, 0,
        "{wrong} probe(s) returned a STALE value after crash recovery (first: {example}); \
         single-shard Phase-4b AOF replay resurrected the spilled v1 instead of \
         keeping the replayed v2 (heap files at kill time: {heap_files})"
    );
}
