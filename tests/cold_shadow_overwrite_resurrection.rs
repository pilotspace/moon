//! CRASH-COLD-OVERWRITE: a key overwritten AFTER being spilled to the cold
//! tier must never resurrect the stale spilled value after a crash.
//!
//! Adversarial-review finding #1 on task #56 (`fix/t56-used-memory-offload`):
//! `Database::demote_replayed_cold_shadows` (added to fix the "used_memory
//! got worse after restart" bug — AOF replay re-hydrating already-cold keys
//! into hot RAM) originally assumed that ANY key still present in the
//! recovered `ColdIndex` after AOF replay finishes must be crash-time-cold
//! and untouched, so replay's hot copy for it is provably redundant with
//! the cold copy and safe to drop.
//!
//! That assumption is false whenever a key is spilled with value v1 and
//! then LIVE-overwritten with v2 (no further eviction/orphan-sweep touches
//! it before a crash): the manifest still shows the key spilled with v1
//! (the overwrite never re-evicted it), so a restart rebuilds
//! `ColdIndex[key] = v1`, AOF replay reconstructs `hot[key] = v2`, and the
//! (pre-fix) demote pass would see `ColdIndex` still holding the key and
//! wrongly drop the hot v2 copy — the next `GET` promotes the stale v1 from
//! disk. `Database::set`'s `InsertOrUpdate::Updated` arm now invalidates the
//! cold shadow the moment a SECOND write to the same key is observed
//! (during replay this can only happen if the AOF recorded a write AFTER
//! the one that got spilled, proving the cold copy stale) — see its doc
//! comment and `storage::db::tests::test_second_write_invalidates_cold_shadow`
//! for the unit-level proof. This test is the end-to-end regression guard.
//!
//! Failure scenario (RED without the fix):
//!   1. SET probes to v1 → filler load evicts them to the cold tier.
//!   2. SET every probe to v2 (live overwrite of an already-cold-shadowed
//!      key — no further eviction touches them again before the crash).
//!   3. SIGKILL.
//!   4. Restart: ColdIndex rebuilds v1 from the still-Active manifest
//!      entry; AOF replay reconstructs hot=v2; the demote pass wrongly
//!      treats the untouched-looking ColdIndex entry as redundant and
//!      drops v2.
//!   5. GET probe → returns v1 (WRONG — should be v2, or the test fails).
//!
//! GREEN with the fix: the second SET (the overwrite, replayed) clears the
//! ColdIndex shadow at write time, so after replay there is nothing left
//! for the demote pass to (wrongly) act on; GET returns v2.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test cold_shadow_overwrite_resurrection -- --ignored
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

const PROBE_COUNT: usize = 100;
const PROBE_VALUE_LEN: usize = 500;
/// Filler keys written after the probes to push memory past the disk-offload
/// threshold, forcing the (older) probe keys to be evicted to the cold tier.
const FILLER_COUNT: usize = 16_000;
const FILLER_VALUE_LEN: usize = 600;
/// 8 MiB total across 4 shards (2 MiB/shard); disk-offload spills at
/// 0.85 x maxmemory. Matches crash_recovery_cold_del_resurrection.rs, whose
/// recovery floor proves most probes land (and stay) cold under this load.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const SHARDS: usize = 4;
/// Seconds to let the async spill/manifest ticks drain so the probes' cold
/// entries are manifest-committed before the overwrite.
const SETTLE_AFTER_FILLER: u64 = 8;
/// Seconds after the overwrite for the per-shard AOF writer (appendfsync
/// everysec) to make it durable before the SIGKILL. Kept well clear of the
/// eviction tick's ~100ms cadence so no re-eviction of the (now MRU, freshly
/// overwritten) probes can happen before the crash — that would re-spill
/// v2, updating the manifest and defeating the whole point of this test.
const SETTLE_AFTER_OVERWRITE: u64 = 3;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-cold-overwrite-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path) -> common::ServerGuard {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    common::ServerGuard::new(
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &SHARDS.to_string(),
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off_dir.to_str().expect("off dir utf8"),
                // `yes` is the bug surface: KV writes are durably logged in the
                // per-shard AOF and replayed at boot. Under `--appendonly no`
                // the WAL writer is skipped entirely and this reconstruction
                // path never runs.
                "--appendonly",
                "yes",
                // Hold the pre-sweep window open for the whole test: the probes
                // must stay exactly as spilled (v1) in the manifest through the
                // crash, with only the overwrite (v2) recorded in the AOF.
                "--cold-orphan-sweep-interval-secs",
                "3600",
                // The diskfull guard write-flags SET too, and dev/CI machines
                // routinely sit under 5% free — with the guard active the
                // overwrite under test would be REJECTED (MOONERR diskfull) and
                // never reach the AOF, silently gutting the test (redis-cli
                // exits 0 on error replies).
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            // Captured to a log file so a CI flake produces a real diagnostic
            // (never Stdio::null()).
            .stdout(
                std::fs::File::create(dir.join("moon.stdout.log")).expect("create moon stdout log"),
            )
            .stderr(
                std::fs::File::create(dir.join("moon.stderr.log")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon (run `cargo build --release` with default features first)"),
    )
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

/// SO_REUSEPORT makes a bind-probe useless; poll until connect is REFUSED
/// twice in a row (see crash_recovery_disk_offload_no_aof.rs).
fn wait_for_port_down(port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let mut consecutive_refused = 0;
    for _ in 0..120 {
        match std::net::TcpStream::connect_timeout(
            &addr.parse().expect("addr"),
            Duration::from_millis(100),
        ) {
            Ok(_) => {
                consecutive_refused = 0;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                consecutive_refused += 1;
                if consecutive_refused >= 2 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

const RESTART_ATTEMPTS: usize = 6;

/// Start moon, retrying the transient rebind EADDRINUSE self-shutdown race
/// (see crash_recovery_disk_offload_no_aof.rs for the full rationale).
fn start_moon_alive(port: u16, dir: &std::path::Path) -> common::ServerGuard {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon(port, dir);
        let mut up = false;
        for _ in 0..80 {
            if let Ok(Some(_status)) = child.as_mut().try_wait() {
                break;
            }
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
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
    panic!(
        "moon failed to start+serve on port {} after {} attempts",
        port, RESTART_ATTEMPTS
    );
}

fn probe_key(i: usize) -> String {
    format!("probe:{}", i)
}

/// redis-cli exits 0 even when the server replies an error (MOONERR/ERR land
/// on stdout/stderr, not the exit status) — every mutation helper must check
/// the reply text too, or a server-side rejection (e.g. the diskfull guard)
/// silently guts the test.
fn assert_no_error_reply(op: &str, out: &std::process::Output) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && !stdout.contains("MOONERR") && !stdout.starts_with("ERR"),
        "redis-cli {} rejected by server: stdout={} stderr={}",
        op,
        stdout.trim(),
        stderr.trim()
    );
}

fn redis_set(port: u16, key: &str, value: &str) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", key, value])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli SET");
    assert_no_error_reply(&format!("SET {}", key), &out);
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

/// Pipelined filler SETs to push memory past the offload threshold.
fn write_filler(port: u16) {
    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect for filler");
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

/// A key spilled to the cold tier, then LIVE-overwritten with a new value
/// and never touched again before a crash, must come back as the NEW value
/// after recovery -- not resurrect the stale spilled one.
#[test]
#[ignore] // requires ./target/release/moon + redis-cli; run with -- --ignored
fn overwritten_cold_key_returns_new_value_after_crash() {
    let port = common::reserve_port();
    let dir = unique_dir("overwrite");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // Round 1: load v1, spill, overwrite with v2, crash.
    let mut server = start_moon(port, &dir);
    wait_for_port(port);

    let v1 = "1".repeat(PROBE_VALUE_LEN);
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &v1);
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));

    // Precondition: the probes really were spilled to the cold tier. Without
    // heap files on disk this test has no power (nothing to shadow).
    let heap_files = count_heap_files(&dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill"
    );

    // Live overwrite of the (now cold-shadowed) probes. This is the exact
    // scenario finding #1 flagged: the manifest still records v1 for these
    // keys (no re-eviction has happened), while the AOF now also holds a
    // SET for v2 after the original SET for v1.
    let v2 = "2".repeat(PROBE_VALUE_LEN);
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &v2);
    }
    // Pre-crash sanity: the overwrite must be visible on the LIVE server.
    assert_eq!(
        redis_get(port, &probe_key(0)).as_deref(),
        Some(v2.as_str()),
        "precondition failed: probe:0 did not read back v2 before the crash — \
         the overwrite was rejected server-side (write guard?)"
    );
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_OVERWRITE));

    // Hard crash inside the pre-sweep window (sweep interval is 1h) and
    // before any eviction tick could re-evict the (now MRU) probes.
    server.kill_now();
    wait_for_port_down(port);

    // Round 2: recover and check every probe reads back v2, never v1.
    let mut server2 = start_moon_alive(port, &dir);

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
    // Keep the data dir + server logs for post-mortem when the assertion is
    // about to fail (or when explicitly requested via MOON_TEST_KEEP=1).
    if wrong == 0 && missing == 0 && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }

    assert_eq!(
        missing, 0,
        "{} probe(s) missing entirely after crash recovery (heap files at kill time: {})",
        missing, heap_files
    );
    assert_eq!(
        wrong, 0,
        "{} probe(s) returned a STALE value after crash recovery (first: {}); \
         a cold-shadowed key's overwrite was lost — the demote pass \
         resurrected the spilled v1 instead of keeping the replayed v2 \
         (heap files at kill time: {})",
        wrong, example, heap_files
    );
}
