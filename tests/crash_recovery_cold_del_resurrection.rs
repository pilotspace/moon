//! CRASH-COLD-DEL: deleted/flushed spilled keys must NOT resurrect after a crash.
//!
//! Guards the replay-time cold-plane tombstone gap. Under `--appendonly yes`
//! (the only config where KV writes are durably logged — `--appendonly no`
//! skips the WAL writer entirely, so post-crash resurrection there is the
//! documented no-AOF RPO, not a bug), DEL/FLUSHALL are replayed at boot from
//! the per-shard AOF (`replay_per_shard`, main.rs). Pre-fix, the B-2
//! cold-wiring preservation in main.rs `take()`s `cold_index` off every
//! database BEFORE the replay and re-attaches it only AFTER, so the replayed
//! DEL/FLUSHALL hit `remove_counting_cold`/`clear()` as silent no-ops against
//! the cold plane (src/storage/db.rs — `as_mut()` on None). The same gap
//! existed in `recover_shard_v3_pitr` Phase 4 WAL replay (ColdIndex built in
//! Phase 3 but attached only after recovery returned).
//!
//! Failure scenario (RED without the fix):
//!   1. SET probes → filler load evicts them to the cold tier (.mpf + manifest
//!      commit, durable).
//!   2. DEL every probe. Live path tombstones the in-memory ColdIndex and queues
//!      the backing file for the orphan sweep — but the manifest entry stays
//!      Active until the sweep runs (held off here via a 1h sweep interval).
//!      The DEL is durably logged in the per-shard AOF incr file.
//!   3. SIGKILL inside the pre-sweep window.
//!   4. Restart: v3 recovery Phase 3 rebuilds the ColdIndex from the
//!      still-Active manifest entries; the per-shard AOF replay then replays
//!      SET (hot) and DEL (hot removed; cold no-op — index detached).
//!   5. GET probe → hot miss → cold read-through returns the DELETED value.
//!
//! GREEN with the fix (cold wiring live during incr replay, preserved across
//! the base-RDB swap inside `replay_per_shard`): the replayed DEL tombstones
//! the cold entry; every post-crash GET is nil.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_cold_del_resurrection -- --ignored
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const PROBE_COUNT: usize = 200;
const PROBE_VALUE_LEN: usize = 500;
/// Filler keys written after the probes to push memory past the disk-offload
/// threshold, forcing the (older) probe keys to be evicted to the cold tier.
const FILLER_COUNT: usize = 16_000;
const FILLER_VALUE_LEN: usize = 600;
/// 8 MiB total across 4 shards (2 MiB/shard); disk-offload spills at
/// 0.85 × maxmemory. Matches crash_recovery_disk_offload_no_aof.rs, whose
/// recovery floor proves most probes land (and stay) cold under this load.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const SHARDS: usize = 4;
/// Seconds to let the async spill/manifest ticks drain so the probes' cold
/// entries are manifest-committed before we DEL them.
const SETTLE_AFTER_FILLER: u64 = 8;
/// Seconds after the DELs for the per-shard AOF writer (appendfsync everysec)
/// to make them durable before the SIGKILL.
const SETTLE_AFTER_DEL: u64 = 3;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-cold-del-{}-{}-{}",
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
            "--maxmemory",
            &MAXMEMORY_BYTES.to_string(),
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "enable",
            "--disk-offload-dir",
            off_dir.to_str().expect("off dir utf8"),
            // `yes` is the bug surface: KV writes (incl. the DELs) are durably
            // logged in the per-shard AOF and replayed at boot. Under
            // `--appendonly no` the WAL writer is skipped entirely
            // ("WAL skipped (appendonly=no)") — nothing replays, and cold
            // resurrection there is the documented no-AOF RPO, not this bug.
            "--appendonly",
            "yes",
            // Hold the pre-sweep window open for the whole test: the DEL's
            // manifest tombstone is deferred to this sweep, and the bug under
            // test lives precisely in the crash-before-sweep window.
            "--cold-orphan-sweep-interval-secs",
            "3600",
            // The diskfull guard write-flags DEL/FLUSHALL too (documented
            // trade-off) and dev/CI machines routinely sit under 5% free —
            // with the guard active the DELs under test would be REJECTED
            // (MOONERR diskfull) and never reach the AOF, silently gutting
            // the test (redis-cli exits 0 on error replies).
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        // Captured to a log file so a CI flake produces a real diagnostic
        // (never Stdio::null()).
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

/// See crash_recovery_disk_offload_no_aof.rs: SO_REUSEPORT makes a bind-probe
/// useless; poll until connect is REFUSED twice in a row.
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
fn start_moon_alive(port: u16, dir: &std::path::Path) -> Child {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon(port, dir);
        let mut up = false;
        for _ in 0..80 {
            if let Ok(Some(_status)) = child.try_wait() {
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

/// DEL the probes in batches of 50 keys per redis-cli invocation.
fn redis_del_probes(port: u16) {
    for chunk in (0..PROBE_COUNT).collect::<Vec<_>>().chunks(50) {
        let mut args = vec!["-p".to_string(), port.to_string(), "DEL".to_string()];
        args.extend(chunk.iter().map(|i| probe_key(*i)));
        let out = Command::new("redis-cli")
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("redis-cli DEL");
        assert_no_error_reply("DEL batch", &out);
    }
}

fn redis_flushall(port: u16) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "FLUSHALL"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli FLUSHALL");
    assert_no_error_reply("FLUSHALL", &out);
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

/// Shared body: load → spill → delete via `delete_fn` → crash → restart →
/// assert zero resurrections.
fn run_scenario(suffix: &str, delete_fn: impl Fn(u16)) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");

    // Round 1: load, spill, delete, crash.
    let mut server = start_moon(port, &dir);
    wait_for_port(port);

    let probe_val = "P".repeat(PROBE_VALUE_LEN);
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &probe_val);
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));

    // Precondition: the probes really were spilled to the cold tier. Without
    // heap files on disk this test has no power (nothing to resurrect).
    let heap_files = count_heap_files(&dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill"
    );

    delete_fn(port);
    // Pre-crash sanity: the deletion must be visible on the LIVE server.
    // Guards against server-side rejection (e.g. a still-active write guard)
    // silently gutting the test — the crash assertions below are meaningless
    // if the keys were never deleted in the first place.
    assert!(
        redis_get(port, &probe_key(0)).is_none(),
        "precondition failed: probe:0 still readable after delete — the \
         deletion was rejected server-side (write guard?)"
    );
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_DEL));

    // Hard crash inside the pre-sweep window (sweep interval is 1h).
    server.kill().expect("SIGKILL moon");
    let _ = server.wait();
    wait_for_port_down(port);

    // Round 2: recover and check for resurrections.
    let mut server2 = start_moon_alive(port, &dir);

    let mut resurrected = 0usize;
    let mut example = String::new();
    for i in 0..PROBE_COUNT {
        if redis_get(port, &probe_key(i)).is_some() {
            resurrected += 1;
            if example.is_empty() {
                example = probe_key(i);
            }
        }
    }

    server2.kill().expect("kill moon round 2");
    let _ = server2.wait();
    // Keep the data dir + server logs for post-mortem when the assertion is
    // about to fail (or when explicitly requested via MOON_TEST_KEEP=1).
    if resurrected == 0 && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }

    assert_eq!(
        resurrected, 0,
        "{} deleted key(s) resurrected from the cold tier after crash recovery \
         (first: {}); Phase-4 replay did not tombstone the cold plane \
         (heap files at kill time: {})",
        resurrected, example, heap_files
    );
}

/// DEL'd spilled keys must stay deleted across a kill-9 (pre-sweep window).
#[test]
#[ignore] // requires ./target/release/moon + redis-cli; run with -- --ignored
fn deleted_cold_keys_stay_deleted_after_crash() {
    run_scenario("del", redis_del_probes);
}

/// FLUSHALL must clear the cold plane across a kill-9 too (Database::clear()'s
/// cold_all path has the same None-during-replay gap as DEL).
#[test]
#[ignore] // requires ./target/release/moon + redis-cli; run with -- --ignored
fn flushed_cold_keys_stay_flushed_after_crash() {
    run_scenario("flushall", redis_flushall);
}
