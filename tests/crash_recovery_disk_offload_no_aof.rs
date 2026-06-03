//! CRASH-COLD-NOAOF: disk-offload cold recovery WITHOUT AOF (#22 regression).
//!
//! Reproduces and guards the #22 durability bug: with `--appendonly no` and
//! `--disk-offload enable`, cold (spilled-to-disk) keys must survive a hard
//! crash and be served via cold read-through after restart — driven PURELY by
//! the per-shard manifest, with NO AOF replay (there is no AOF under
//! `appendonly no`).
//!
//! Root cause (main.rs): `persistence_dir` was derived as `Some(..)` only when
//! `appendonly == "yes" || save.is_some()`. Under `appendonly no` it was `None`,
//! and the shard-construction closure gated `restore_from_persistence` behind
//! `if let Some(ref dir) = persistence_dir { .. }` — so the v3 cold rebuild was
//! skipped entirely even though `disk_offload_base` was `Some`. Cold keys
//! recovered 0/200. The fix fires recovery when
//! `persistence_dir.is_some() || disk_offload_base.is_some()`.
//!
//! Discriminating signal (RED vs GREEN):
//!   * `heap-*.mpf` files on disk prove the probes were durably spilled to cold.
//!   * POST-crash read-through proves recovery re-attached the cold index.
//!   RED (pre-#22-fix binary): heap files present, POST == ~0 (recovery skipped).
//!   GREEN (post-#22-fix):     heap files present, POST == ~PROBE_COUNT.
//! Because there is NO AOF under `appendonly no`, a non-zero POST count can ONLY
//! come from the cold-manifest recovery path the #22 fix enables.
//!
//! NOTE: we deliberately do NOT read the probes back PRE-crash. A cold GET
//! PROMOTES the key back to the hot tier (db.rs cold_read_through), and hot keys
//! are NOT durable under `appendonly no` — a pre-crash read-through would pull
//! probes out of cold and lose them on crash, corrupting the measurement.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_disk_offload_no_aof -- --ignored
//!
//! tokio runtime:
//!   cargo build --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index
//!   cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index \
//!     --test crash_recovery_disk_offload_no_aof -- --ignored
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const PROBE_COUNT: usize = 200;
const PROBE_VALUE_LEN: usize = 500;
/// Filler keys written after the probes to drive memory past the disk-offload
/// threshold, forcing the (older) probe keys to be evicted to the cold tier.
const FILLER_COUNT: usize = 16_000;
const FILLER_VALUE_LEN: usize = 600;
/// 8 MiB total across 4 shards (2 MiB/shard). disk-offload spills at
/// 0.85 × maxmemory; probes (~100 KiB) + filler (~9.6 MiB) >> threshold.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const SHARDS: usize = 4;
/// Post-crash recovery floor. This test catches the *categorical* #22
/// regression: when recovery is skipped the cold tier recovers a hard 0; when
/// it fires it recovers most of the cold probes. Observed GREEN range under
/// SIGKILL is ~172–200/200 — the spread is intrinsic to crashing under
/// `appendonly no`, where a cold key only survives once its eviction-tick
/// manifest commit has landed (no per-write durability without AOF). A 75%
/// floor sits robustly inside that band while remaining 150× above the RED 0,
/// so it flags both a full path-skip (→0) and a partial-recovery regression.
const RECOVERY_FLOOR: usize = (PROBE_COUNT * 75) / 100;

fn unique_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to port 0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-cold-noaof-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path) -> Child {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    Command::new("./target/release/moon")
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
            // The bug under test: NO AOF. Cold recovery must work anyway.
            "--appendonly",
            "no",
            "--cold-orphan-sweep-interval-secs",
            "60",
            "--dir",
        ])
        .arg(dir)
        // Captured to a log file so a CI flake produces a real diagnostic
        // (see feedback_silenced_child_stdio_flake — never Stdio::null()).
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create moon stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create moon stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` with default features first)")
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

fn probe_key(i: usize) -> String {
    format!("probe:{}", i)
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
        "redis-cli SET {} failed: {}",
        key,
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

/// Write `FILLER_COUNT` distinct keys via pipelined TCP SETs to push memory
/// past the disk-offload threshold and evict the (older) probe keys to cold.
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
        // Flush in ~64 KiB chunks so the server applies eviction incrementally
        // rather than receiving one giant burst that overruns the spill queue.
        if buf.len() >= 64 * 1024 {
            stream.write_all(&buf).expect("filler write");
            buf.clear();
        }
    }
    if !buf.is_empty() {
        stream.write_all(&buf).expect("filler tail write");
    }
    stream.flush().ok();
    // Drain is unnecessary for correctness here; closing the stream is fine.
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

fn count_probes_readable(port: u16) -> usize {
    (0..PROBE_COUNT)
        .filter(|&i| redis_get(port, &probe_key(i)).is_some())
        .count()
}

#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// #22: cold keys spilled under `--appendonly no --disk-offload enable` must
/// recover after a SIGKILL crash via the per-shard manifest (no AOF).
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn cold_keys_recover_after_crash_without_aof() {
    let port = unique_port();
    let dir = unique_dir("c22");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1: populate + force cold spill -------------------------------
    let mut child = start_moon(port, &dir);
    wait_for_port(port);

    let probe_val = "P".repeat(PROBE_VALUE_LEN);
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &probe_val);
    }
    // Filler evicts the older probes to the cold tier.
    write_filler(port);
    // Let the async spill thread drain its queue and commit manifests. Under
    // `appendonly no` a cold key is only crash-durable once its eviction-tick
    // manifest commit lands, so give the ticks ample time before the kill.
    std::thread::sleep(Duration::from_secs(5));

    // NOTE: do NOT read the probes here — a cold GET promotes the key back to
    // hot, and hot is not durable under `appendonly no`, which would corrupt
    // the POST measurement. `heap_files > 0` is the spill-happened proof.
    let heap_files = count_heap_files(&dir);

    // SIGKILL — hard crash, no graceful drain.
    sigkill(&mut child);

    // -- Round 2: recover ---------------------------------------------------
    let mut child2 = start_moon(port, &dir);
    wait_for_port(port);
    // Give recovery a beat to re-attach the cold index before probing.
    std::thread::sleep(Duration::from_secs(1));

    let post = count_probes_readable(port);

    sigkill(&mut child2);

    // Setup sanity: if nothing spilled, the test exercised nothing — fail loud
    // (a maxmemory/eviction misconfig, not a recovery pass).
    assert!(
        heap_files > 0,
        "test setup: expected cold spill (heap-*.mpf) but found 0 — eviction never fired \
         (maxmemory/threshold too high). post={}",
        post
    );

    // The actual #22 assertion: cold keys recovered WITHOUT AOF.
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        post >= RECOVERY_FLOOR,
        "#22 REGRESSION: post-crash cold read-through {}/{} (floor {}). heap_files={}. \
         A POST near 0 with cold files on disk means restore_from_persistence was skipped \
         under `appendonly no` — the persistence_dir gate regressed (recovery must fire on \
         disk_offload_base.is_some()).",
        post,
        PROBE_COUNT,
        RECOVERY_FLOOR,
        heap_files
    );
}
