//! Issue #364: SCAN/KEYS/RANDOMKEY must enumerate the LOGICAL keyspace
//! under disk-offload, not just the hot plane.
//!
//! Found while fixing #355 (DBSIZE): with disk-offload enabled, spilled
//! keys were readable (GET/EXISTS) but invisible to enumeration — a
//! 4-shard instance holding 400 logical keys returned only 116 from
//! `redis-cli --scan` (hot residents only). Any SCAN consumer doing
//! migration/backup (`--scan | xargs MIGRATE`) silently lost spilled keys.
//!
//! This test drives the fix end-to-end: writes ~1.5x `--maxmemory` of
//! string keys plus a batch of hashes under disk-offload, confirms real
//! spill happened (ground truth: `heap-*.mpf` files), then asserts
//!  1. a full SCAN loop returns EVERY written key exactly (dedup'd),
//!  2. KEYS * agrees,
//!  3. RANDOMKEY answers non-nil,
//!  4. `SCAN ... TYPE hash` returns exactly the hash keys (cold keys are
//!     judged from the in-RAM `ColdLocation::value_type` cache — no disk
//!     reads, no promotion),
//!
//! then SIGKILLs the server, restarts on the same `--dir` (all spilled
//! keys recover as cold-only stubs, `ColdIndex::rebuild_from_manifest`
//! re-derives `value_type` from the on-disk pages), and asserts 1 and 4
//! again post-restart.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test scan_offload_visibility -- --ignored --nocapture
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// 8 MiB cap — small enough for a fast local test, large enough that the
/// filler below forces real eviction + disk spill.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const SHARDS: usize = 2;

/// ~1.5x MAXMEMORY_BYTES of raw string payload — enough to force spill
/// without the long fill time of the 10x used_memory test.
const FILLER_COUNT: usize = 6_000;
const FILLER_VALUE_LEN: usize = 2_000;
/// Hash keys interleaved with the strings so the TYPE filter has both
/// planes and both types to discriminate.
const HASH_COUNT: usize = 50;

/// Kill-on-drop child guard: a mid-test panic (failed assert) must never
/// orphan the spawned server. A leaked moon whose tmpdir is later cleaned
/// spins its persistence tick at ~100% CPU per shard thread with no
/// backoff (issue #366) — observed live at 667% CPU on 2026-07-17.
struct MoonGuard(Option<Child>);

impl MoonGuard {
    /// Kill + reap now (the deliberate mid-test SIGKILL leg).
    fn kill_now(&mut self) {
        if let Some(mut c) = self.0.take() {
            common::sigkill(&mut c);
        }
    }
}

impl Drop for MoonGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
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

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-scan-offload-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path) -> Child {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &SHARDS.to_string(),
            "--admin-port",
            "0",
            "--maxmemory",
            &MAXMEMORY_BYTES.to_string(),
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "enable",
            "--disk-offload-dir",
            off_dir.to_str().expect("off dir utf8"),
            // Durability backstop required or disk-offload spill is inert
            // (config::disk_offload_spill_inert) — eviction would plain-drop
            // victims instead of spilling them.
            "--appendonly",
            "yes",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        // Captured to a log file, never Stdio::null(): a CI flake needs a
        // real diagnostic, not silence.
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` first)")
}

const RESTART_ATTEMPTS: usize = 6;

/// Start moon and return a child that is alive AND accepting PING, retrying
/// on a transient rebind EADDRINUSE self-termination (same pattern as
/// tests/used_memory_offload_truthful.rs).
fn start_moon_alive(port: u16, dir: &std::path::Path) -> Child {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon(port, dir);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut up = false;
        while Instant::now() < deadline {
            if let Ok(Some(_status)) = child.try_wait() {
                break; // self-terminated — fall through to retry
            }
            if redis_cli(port, &["PING"]).as_deref() == Some("PONG") {
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if up {
            return child;
        }
        let _ = child.kill();
        let _ = child.wait();
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!(
        "moon failed to start+serve on port {} after {} attempts",
        port, RESTART_ATTEMPTS
    );
}

fn wait_for_ping(port: u16, deadline: Duration) {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if redis_cli(port, &["PING"]).as_deref() == Some("PONG") {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("moon did not respond to PING within {deadline:?} on port {port}");
}

fn redis_cli(port: u16, args: &[&str]) -> Option<String> {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

/// Push `FILLER_COUNT` string keys via many small, paced `MSET` batches
/// (paced so the background SpillThread drains its bounded channel between
/// bursts — see tests/used_memory_offload_truthful.rs `write_filler` for
/// the full rationale), plus `HASH_COUNT` hashes via HSET.
fn write_dataset(port: u16) {
    const FILLER_BATCH_SIZE: usize = 400;
    let val = "F".repeat(FILLER_VALUE_LEN);
    let mut written = 0usize;
    while written < FILLER_COUNT {
        let batch = FILLER_BATCH_SIZE.min(FILLER_COUNT - written);
        let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .expect("connect for filler batch");
        stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

        let total_args = 1 + 2 * batch;
        let mut buf: Vec<u8> = Vec::with_capacity(batch * (FILLER_VALUE_LEN + 32));
        buf.extend_from_slice(format!("*{total_args}\r\n$4\r\nMSET\r\n").as_bytes());
        for i in written..written + batch {
            let key = format!("filler:{i}");
            buf.extend_from_slice(
                format!("${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, val.len(), val).as_bytes(),
            );
        }
        stream.write_all(&buf).expect("filler MSET batch write");

        let mut reply = String::new();
        let mut reader = BufReader::new(&stream);
        reader
            .read_line(&mut reply)
            .expect("filler MSET batch reply");
        assert!(
            reply.starts_with('+'),
            "filler MSET batch (keys {written}..{}) must succeed, got: {reply}",
            written + batch
        );

        written += batch;
        std::thread::sleep(Duration::from_millis(30));
    }

    for i in 0..HASH_COUNT {
        let key = format!("hobj:{i}");
        let reply = redis_cli(port, &["HSET", &key, "f", "v"]);
        assert!(
            reply.as_deref().is_some_and(|r| r.parse::<u64>().is_ok()),
            "HSET {key} must succeed, got: {reply:?}"
        );
    }
}

fn count_heap_files(dir: &std::path::Path) -> usize {
    let off = dir.join("off");
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                    .unwrap_or(false)
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

/// Drive a full SCAN loop (with optional extra args like `TYPE hash`) to
/// cursor 0, returning the dedup'd key set. Bounded to catch a cursor that
/// never converges.
fn full_scan(port: u16, extra: &[&str]) -> HashSet<String> {
    let mut keys = HashSet::new();
    let mut cursor = String::from("0");
    for _round in 0..10_000 {
        let mut args: Vec<&str> = vec!["SCAN", &cursor, "COUNT", "1000"];
        args.extend_from_slice(extra);
        let out = redis_cli(port, &args).unwrap_or_default();
        let mut lines = out.lines();
        let next = lines
            .next()
            .unwrap_or_else(|| panic!("SCAN (extra={extra:?}) returned empty reply"))
            .trim()
            .to_string();
        for l in lines {
            let l = l.trim();
            if !l.is_empty() {
                keys.insert(l.to_string());
            }
        }
        if next == "0" {
            return keys;
        }
        cursor = next;
    }
    panic!("SCAN cursor did not converge to 0 within 10000 rounds (extra={extra:?})");
}

fn expected_keys() -> HashSet<String> {
    let mut set: HashSet<String> = (0..FILLER_COUNT).map(|i| format!("filler:{i}")).collect();
    set.extend((0..HASH_COUNT).map(|i| format!("hobj:{i}")));
    set
}

fn expected_hash_keys() -> HashSet<String> {
    (0..HASH_COUNT).map(|i| format!("hobj:{i}")).collect()
}

/// Assert `got` covers exactly `want`, printing a small sample of the
/// difference on failure (6000 raw keys in a panic message helps nobody).
fn assert_keyset(context: &str, got: &HashSet<String>, want: &HashSet<String>) {
    let missing: Vec<_> = want.difference(got).take(10).collect();
    let extra: Vec<_> = got.difference(want).take(10).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "{context}: keyset mismatch — got {} keys, want {} keys; \
         first missing: {missing:?}, first unexpected: {extra:?}",
        got.len(),
        want.len()
    );
}

#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn scan_keys_randomkey_enumerate_spilled_keys_and_survive_restart() {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not in PATH");
        return;
    }

    let dir = unique_dir("t364");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1: populate past the cap, force real disk spill -----------
    let (child, port) = common::spawn_listening(|p| start_moon(p, &dir));
    let mut child = MoonGuard(Some(child));
    wait_for_ping(port, Duration::from_secs(10));

    write_dataset(port);
    // Let the async spill thread + periodic eviction tick drain and commit
    // manifests before enumerating.
    std::thread::sleep(Duration::from_secs(6));

    let heap_files = count_heap_files(&dir);
    eprintln!("scan_offload_visibility: heap_files={heap_files}");
    assert!(
        heap_files > 0,
        "test setup: expected real disk spill (heap-*.mpf files), found none — \
         eviction never triggered, this test exercised nothing"
    );

    let want = expected_keys();
    let want_hashes = expected_hash_keys();

    // 1. Full SCAN loop sees the whole logical keyspace.
    let scanned = full_scan(port, &[]);
    assert_keyset("SCAN (pre-restart)", &scanned, &want);

    // 2. KEYS * agrees.
    let keys_out = redis_cli(port, &["KEYS", "*"]).unwrap_or_default();
    let keys_set: HashSet<String> = keys_out
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    assert_keyset("KEYS * (pre-restart)", &keys_set, &want);

    // 3. RANDOMKEY answers non-nil on a database that is mostly spilled.
    let rk = redis_cli(port, &["RANDOMKEY"]);
    assert!(
        rk.as_deref().is_some_and(|k| want.contains(k)),
        "RANDOMKEY must return a logical key, got: {rk:?}"
    );

    // 4. TYPE filter discriminates cold keys from the in-RAM index.
    let scanned_hashes = full_scan(port, &["TYPE", "hash"]);
    assert_keyset(
        "SCAN TYPE hash (pre-restart)",
        &scanned_hashes,
        &want_hashes,
    );

    // -- SIGKILL + restart on the SAME dir/port ---------------------------
    // All spilled keys recover as cold-only stubs; ColdIndex::rebuild_from_manifest
    // re-derives ttl_ms AND value_type from the on-disk pages.
    child.kill_now();
    common::wait_for_port_down(port);

    let mut child2 = MoonGuard(Some(start_moon_alive(port, &dir)));

    let scanned_after = full_scan(port, &[]);
    let hashes_after = full_scan(port, &["TYPE", "hash"]);
    child2.kill_now();

    assert_keyset("SCAN (post-restart)", &scanned_after, &want);
    assert_keyset("SCAN TYPE hash (post-restart)", &hashes_after, &want_hashes);

    let _ = std::fs::remove_dir_all(&dir);
}
