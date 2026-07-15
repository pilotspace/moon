//! Task #56 (v0.8 item 2): `used_memory` truthfulness under disk-offload.
//!
//! Regression guard for two distinct mechanisms observed in the G2
//! acceptance run (4 shards, `--maxmemory 256MB`, 2.6GB dataset,
//! disk-offload enabled): `INFO` `used_memory` reported 406-762MB against
//! the 256MB cap, and got WORSE after a kill-9 restart.
//!
//! 1. **`used_memory` was literally process RSS** (`src/command/connection.rs`),
//!    not the logical ledger `--maxmemory` eviction actually gates on (KV +
//!    ColdIndex + vector/text/graph resident bytes -- see
//!    `ShardDatabases::recompute_elastic_budget`). RSS always carries
//!    allocator arena overhead, page-cache frames, thread stacks, the
//!    binary image, and more -- none of which the eviction system charges
//!    against the cap, so `used_memory` was guaranteed to read high under
//!    ANY disk-offload workload regardless of whether eviction was working
//!    correctly.
//! 2. **Recovery double-loaded spilled KV entries** (`src/persistence/recovery.rs`):
//!    Phase 3 used to re-insert every previously-spilled `ValueType::String`
//!    entry directly into the hot DashTable (added by #79-04, predating cold
//!    read-through), and independently rebuild a `ColdIndex` stub for the
//!    SAME key (added two days later by #80-02) -- nobody removed the first
//!    loop when the second, correct mechanism landed. A restart therefore
//!    un-evicted the entire disk-offloaded dataset back into the hot ledger
//!    in one shot, with no eviction pass in between -- exactly the "got
//!    WORSE after restart" symptom.
//!
//! This test drives BOTH mechanisms end-to-end: writes far more data than
//! `--maxmemory` under disk-offload, confirms real spill happened (ground
//! truth: `heap-*.mpf` files on disk), asserts `used_memory` (the logical
//! ledger) stays bounded near the cap at steady state, SIGKILLs the server,
//! restarts it on the same `--dir`, and asserts `used_memory` stays bounded
//! immediately after recovery too -- BEFORE anything reads the recovered
//! keys back into hot RAM (which would legitimately, lazily, grow it again).
//!
//! Run with (monoio default -- matches CI):
//!   cargo build --release
//!   cargo test --release --test used_memory_offload_truthful -- --ignored --nocapture
//!
//! tokio runtime:
//!   cargo build --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index
//!   cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index \
//!     --test used_memory_offload_truthful -- --ignored --nocapture
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// 8 MiB cap -- small enough for a fast local test, large enough that the
/// filler write (below) forces real eviction + disk spill under
/// disk-offload.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const SHARDS: usize = 2;

/// ~10x MAXMEMORY_BYTES of raw value payload (ignoring key/protocol
/// overhead) -- matches the task's reproduction ratio (2.6GB dataset over a
/// 256MB cap ~= 10x).
const FILLER_COUNT: usize = 40_000;
const FILLER_VALUE_LEN: usize = 2_000;

/// Bound used to judge "is `used_memory` truthful" at both steady state and
/// post-restart: the logical ledger must stay within this multiple of
/// `--maxmemory`. Generous enough to absorb per-shard elastic-budget slop
/// and 100ms-tick publish lag, but far tighter than the ~3x overage
/// (762MB / 256MB) the G2 acceptance run observed -- and MUCH tighter than
/// the ~10x a full un-evicted reload (the restart bug) would produce.
const TRUTHFUL_MULTIPLE: f64 = 1.75;

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
        "moon-used-mem-offload-{}-{}-{}",
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
            // (config::disk_offload_spill_inert) -- eviction would plain-drop
            // victims instead of spilling them, and this test would exercise
            // nothing.
            "--appendonly",
            "yes",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        // Captured to a log file, never Stdio::null() (see
        // feedback_silenced_child_stdio_flake): a CI flake needs a real
        // diagnostic, not silence.
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` first)")
}

const RESTART_ATTEMPTS: usize = 6;

/// Start moon and return a child that is alive AND accepting PING, retrying
/// on a transient rebind EADDRINUSE self-termination (see
/// tests/crash_recovery_disk_offload_no_aof.rs for the same pattern).
fn start_moon_alive(port: u16, dir: &std::path::Path) -> Child {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon(port, dir);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut up = false;
        while Instant::now() < deadline {
            if let Ok(Some(_status)) = child.try_wait() {
                break; // self-terminated -- fall through to retry
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

/// Poll `PING` until it replies `PONG` or `deadline` elapses (panics on
/// timeout -- a dead-on-arrival server is a real setup failure here).
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

/// Extract `field:<value>` (u64) from an `INFO` bulk-string body.
fn parse_info_u64(body: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}:");
    body.lines()
        .find(|l| l.starts_with(&prefix))
        .and_then(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse::<u64>().ok())
}

/// Keys per batched `MSET` (see `write_filler` below for why this must be
/// batched rather than one giant MSET under `--appendonly yes`).
const FILLER_BATCH_SIZE: usize = 400;

/// Push `FILLER_COUNT` keys via many small, paced `MSET` batches.
///
/// Task #56 test-design note: `tests/crash_recovery_disk_offload_no_aof.rs`
/// pushes its filler via ONE giant MSET, which works there because that test
/// runs `--appendonly no` -- the write-path eviction gate takes the
/// synchronous, in-process "no manifest reachable, no AOF backstop"
/// plain-drop branch (`try_evict_if_needed_with_spill`, no channel
/// involved). This test needs REAL durable disk spill (`--appendonly yes`
/// is required or `disk_offload_spill_inert()` makes eviction plain-drop
/// instead of spilling -- see `start_moon`'s comment), which routes each
/// evicted key through `evict_one_async_spill`'s bounded
/// `flume::bounded::<SpillRequest>(4096)` channel
/// (`storage::tiered::spill_thread`). A single MSET whose cross-shard
/// remote leg lands as one giant `ShardMessage::MultiExecute` batch runs
/// `spsc_eviction_gate` per embedded command, ALL within one synchronous
/// batch/tick -- with no I/O in between to let the background spill thread
/// drain pwrite+fsync work, thousands of evictions can queue faster than
/// the bounded channel drains, and the gate legitimately (not a bug)
/// returns `-OOM` once the channel is full and `try_send` fails. Batching
/// the filler into small MSETs with a short sleep between them gives the
/// background `SpillThread` room to drain between bursts, exactly like a
/// real client's write rate would.
fn write_filler(port: u16) {
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
        // Let the background spill thread drain the bounded SpillRequest
        // channel between bursts (see the doc comment above).
        std::thread::sleep(Duration::from_millis(30));
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

/// Sample `INFO memory` every `interval` for `duration`, returning every
/// `(used_memory, used_memory_rss)` pair observed. Used to catch a
/// TRANSIENT spike, not just a final steady-state value -- a restart that
/// balloons memory for even one tick before self-healing is still the bug
/// this test guards against.
fn sample_used_memory(port: u16, duration: Duration, interval: Duration) -> Vec<(u64, u64)> {
    let deadline = Instant::now() + duration;
    let mut samples = Vec::new();
    while Instant::now() < deadline {
        if let Some(body) = redis_cli(port, &["INFO", "memory"]) {
            let used = parse_info_u64(&body, "used_memory");
            let rss = parse_info_u64(&body, "used_memory_rss");
            if let (Some(u), Some(r)) = (used, rss) {
                samples.push((u, r));
            }
        }
        std::thread::sleep(interval);
    }
    samples
}

#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn used_memory_stays_bounded_at_steady_state_and_after_restart() {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not in PATH");
        return;
    }

    let dir = unique_dir("t56");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let bound = (MAXMEMORY_BYTES as f64 * TRUTHFUL_MULTIPLE) as u64;

    // -- Round 1: populate past the cap, force real disk spill -----------
    let (mut child, port) = common::spawn_listening(|p| start_moon(p, &dir));
    wait_for_ping(port, Duration::from_secs(10));

    write_filler(port);
    // Let the async spill thread + periodic eviction tick drain and commit
    // manifests before sampling steady state.
    std::thread::sleep(Duration::from_secs(6));

    let heap_files_before_kill = count_heap_files(&dir);
    let steady = sample_used_memory(port, Duration::from_secs(3), Duration::from_millis(300));
    assert!(
        !steady.is_empty(),
        "collected no INFO memory samples before the kill"
    );
    let steady_max_used = steady.iter().map(|(u, _)| *u).max().unwrap_or(0);
    let steady_last = *steady.last().unwrap();

    eprintln!(
        "used_memory_offload_truthful: heap_files_before_kill={heap_files_before_kill} \
         maxmemory={MAXMEMORY_BYTES} bound={bound} steady_max_used={steady_max_used} \
         steady_last=(used={}, rss={})",
        steady_last.0, steady_last.1
    );

    // Setup sanity: if eviction/spill never fired, this test exercised
    // nothing -- fail loud rather than silently pass.
    assert!(
        heap_files_before_kill > 0,
        "test setup: expected real disk spill (heap-*.mpf files) before the kill, found none -- \
         eviction never triggered (maxmemory/threshold too high, or the periodic \
         memory-pressure tick never ran)"
    );
    assert!(
        steady_max_used <= bound,
        "STEADY-STATE used_memory ({steady_max_used}) exceeds {TRUTHFUL_MULTIPLE}x maxmemory \
         ({bound}) -- used_memory is not truthful relative to the eviction gate"
    );

    // -- SIGKILL + restart on the SAME dir/port ---------------------------
    common::sigkill(&mut child);
    common::wait_for_port_down(port);

    let mut child2 = start_moon_alive(port, &dir);

    // Sample immediately (and for a few seconds after) -- BEFORE issuing any
    // GET against the recovered keys, so nothing gets lazily promoted into
    // hot RAM by this test itself. Under the pre-fix recovery bug this
    // would already read near the FULL filler dataset size (~10x
    // maxmemory) on the very first sample, because the hot reload happened
    // synchronously during recovery, before the server even started
    // listening.
    let post_restart = sample_used_memory(port, Duration::from_secs(3), Duration::from_millis(300));
    let post_restart_max_used = post_restart.iter().map(|(u, _)| *u).max().unwrap_or(0);
    let post_restart_last = *post_restart.last().unwrap_or(&(0, 0));

    eprintln!(
        "used_memory_offload_truthful: post_restart_max_used={post_restart_max_used} \
         post_restart_last=(used={}, rss={}) bound={bound}",
        post_restart_last.0, post_restart_last.1
    );

    common::sigkill(&mut child2);

    assert!(
        !post_restart.is_empty(),
        "collected no INFO memory samples after restart"
    );

    let recovered_truthfully = post_restart_max_used <= bound;
    if recovered_truthfully {
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        recovered_truthfully,
        "#56 REGRESSION: POST-RESTART used_memory ({post_restart_max_used}) exceeds \
         {TRUTHFUL_MULTIPLE}x maxmemory ({bound}) -- restart re-charged spilled entries into the \
         hot ledger instead of recovering them as cold-only stubs (steady-state was \
         {steady_max_used}, so this is strictly worse). Logs kept at {} for diagnosis.",
        dir.display()
    );
}
