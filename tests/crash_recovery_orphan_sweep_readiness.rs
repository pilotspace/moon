//! Task #55: startup readiness must not block on the crash-orphan heap-file
//! sweep, regardless of how many spilled files sit in the cold-tier data dir.
//!
//! Root cause (pre-fix): `recover_shard_v3_pitr` called
//! `kv_spill::sweep_orphan_heap_files` synchronously — classify AND delete —
//! for every shard, on the main thread, BEFORE the listener/event loops ever
//! start. At production scale (G2 bench: ~236K spilled files) this stalled
//! "restart to first served command" by ~153s (~40s/shard × 4 shards), even
//! though the manifests themselves recovered in ~4s.
//!
//! Fix: recovery now only CLASSIFIES orphans synchronously (cheap:
//! `read_dir` + `HashSet` membership, no `remove_file` syscalls) and defers
//! the actual deletes to a `std::thread` spawned once the shard's own event
//! loop starts — fully decoupled from the accept path, so it can never delay
//! the first served command. See `src/persistence/recovery.rs`,
//! `src/storage/tiered/kv_spill.rs` (`classify_orphan_heap_files` /
//! `remove_orphan_heap_file`), and `src/shard/event_loop.rs`.
//!
//! Discriminating signal (RED vs GREEN):
//!   * RED (pre-fix): time-to-first-PING scales with orphan count — with
//!     enough injected orphans, PING is not observed within `PING_BOUND`.
//!   * GREEN (post-fix): PING succeeds within `PING_BOUND` regardless of
//!     orphan count, AND all injected orphans are eventually deleted (background
//!     sweep completes within `RECLAIM_BOUND`).
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_orphan_sweep_readiness -- --ignored
//!
//! tokio runtime:
//!   cargo build --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index
//!   cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index \
//!     --test crash_recovery_orphan_sweep_readiness -- --ignored
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const SHARDS: usize = 1;
/// Synthetic crash-orphan heap files injected directly into the shard's cold
/// data dir before round 2 starts (never registered in the manifest, so 100%
/// of them classify as orphans). Large enough that a synchronous
/// classify+delete sweep is trivially observable if reintroduced, small
/// enough to keep the test's own file-creation cost negligible.
const ORPHAN_COUNT: usize = 150_000;
/// Restart-to-first-PING budget. Generous relative to the fixed
/// (deferred-delete) path, which should answer within the first connect
/// retry regardless of `ORPHAN_COUNT` — but far tighter than the
/// synchronous-sweep-at-startup regression this test guards against.
const PING_BOUND: Duration = Duration::from_secs(5);
/// Budget for the background sweep thread to reclaim every injected orphan.
const RECLAIM_BOUND: Duration = Duration::from_secs(30);

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-orphan-readiness-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path, off_dir: &std::path::Path) -> common::ServerGuard {
    common::ServerGuard::new(
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &SHARDS.to_string(),
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off_dir.to_str().expect("off dir utf8"),
                "--appendonly",
                "no",
                "--cold-orphan-sweep-interval-secs",
                "60",
                "--dir",
            ])
            .arg(dir)
            // Captured to a log file so a CI flake produces a real diagnostic
            // (see feedback_silenced_child_stdio_flake — never Stdio::null()).
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

/// Poll until a PING round-trips, or panic after `PING_BOUND`.
fn wait_for_ping_within(port: u16, bound: Duration) -> Duration {
    let start = Instant::now();
    loop {
        let out = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "PING"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output();
        if let Ok(out) = out
            && out.status.success()
            && String::from_utf8_lossy(&out.stdout)
                .trim()
                .eq_ignore_ascii_case("PONG")
        {
            return start.elapsed();
        }
        if start.elapsed() > bound {
            panic!(
                "moon did not answer PING within {:?} on port {} \
                 (readiness gated on the orphan sweep? see task #55)",
                bound, port
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn shard_data_dir(off_dir: &std::path::Path, shard_id: usize) -> std::path::PathBuf {
    off_dir.join(format!("shard-{}", shard_id)).join("data")
}

/// Inject `count` synthetic crash-orphan `heap-*.mpf` files directly into the
/// shard's cold data dir. File ids start well above anything a fresh manifest
/// could have registered, so every one of them classifies as an orphan.
/// Content is irrelevant — the sweep only inspects the filename and manifest
/// membership, never parses the file.
fn inject_orphans(
    off_dir: &std::path::Path,
    shard_id: usize,
    count: usize,
) -> Vec<std::path::PathBuf> {
    let data_dir = shard_data_dir(off_dir, shard_id);
    std::fs::create_dir_all(&data_dir).expect("create shard data dir");
    let mut paths = Vec::with_capacity(count);
    for i in 0..count {
        let file_id = 9_000_000u64 + i as u64;
        let path = data_dir.join(format!("heap-{:06}.mpf", file_id));
        std::fs::write(&path, b"orphan").expect("write synthetic orphan heap file");
        paths.push(path);
    }
    paths
}

fn orphans_remaining(paths: &[std::path::PathBuf]) -> usize {
    paths.iter().filter(|p| p.exists()).count()
}

/// Task #55: restart-to-ready must be seconds-scale regardless of how many
/// crash-orphaned heap files sit in the cold-tier data dir, and the orphans
/// must still be reclaimed (just not on the readiness critical path).
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn readiness_not_gated_on_orphan_sweep_and_orphans_still_reclaimed() {
    let port = common::reserve_port();
    let dir = unique_dir("t55");
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&dir).expect("create test dir");
    std::fs::create_dir_all(&off_dir).expect("create off dir");

    // -- Round 1: boot once so moon creates a real (empty) manifest for
    //    shard 0, then shut down cleanly. A fresh, zero-entry manifest means
    //    every injected file below is unambiguously an orphan. -------------
    let mut child = start_moon(port, &dir, &off_dir);
    wait_for_ping_within(port, Duration::from_secs(10));
    child.kill_now();

    // Manifest must exist now — recovery only classifies orphans when it does
    // (see recovery.rs: `if manifest_path.exists()`).
    let manifest_path = off_dir.join("shard-0").join("shard-0.manifest");
    assert!(
        manifest_path.exists(),
        "test setup: round 1 did not create a shard manifest at {:?}",
        manifest_path
    );

    // -- Inject synthetic crash orphans directly on disk -------------------
    let orphan_paths = inject_orphans(&off_dir, 0, ORPHAN_COUNT);
    assert_eq!(orphans_remaining(&orphan_paths), ORPHAN_COUNT);

    // -- Round 2: restart and measure time-to-first-PING --------------------
    let mut child2 = start_moon(port, &dir, &off_dir);
    let ping_elapsed = wait_for_ping_within(port, PING_BOUND);

    // -- Orphans must still be reclaimed, just off the readiness path -------
    let reclaim_start = Instant::now();
    loop {
        let remaining = orphans_remaining(&orphan_paths);
        if remaining == 0 {
            break;
        }
        if reclaim_start.elapsed() > RECLAIM_BOUND {
            child2.kill_now();
            panic!(
                "background orphan sweep did not reclaim all {} injected orphans within {:?} \
                 ({} still present)",
                ORPHAN_COUNT, RECLAIM_BOUND, remaining
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    child2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        ping_elapsed < PING_BOUND,
        "readiness took {:?}, expected < {:?}",
        ping_elapsed,
        PING_BOUND
    );
}
