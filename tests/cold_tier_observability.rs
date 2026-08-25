//! moon#656: `INFO MoonStore` must report the size and shape of the KV cold
//! tier.
//!
//! Before this, the whole `# MoonStore` section was one boolean
//! (`disk_offload_enabled`) while the cold tier was the single largest
//! consumer in the data directory — on the instance that motivated the issue,
//! 7.6 GB of `heap-*.mpf` inside a 15 GB data dir, with `used_memory` at
//! 1.49 GB. There was no way, from outside, to answer "how big is my cold
//! tier" or "how much of it is dead".
//!
//! Two fields already LOOK like they answer this and do not:
//!
//!   * `reclamation_cold_segments` and friends count VECTOR segments — they
//!     sit under `-- Vector segment tiers --`. On a KV-only instance they read
//!     0 while the KV cold tier holds gigabytes, which is worse than absent.
//!   * `spilled_keys` is a monotonic counter of keys ever spilled: a rate,
//!     never a level. It never goes down, so it cannot answer "how much is
//!     there now".
//!
//! This test drives a real spill and asserts the new fields against GROUND
//! TRUTH read off the filesystem, not against each other — a set of INFO
//! fields that merely agree with one another would pass while all reporting
//! the same wrong number.
//!
//! Run with (monoio default -- matches CI):
//!   cargo build --release
//!   cargo test --release --test cold_tier_observability -- --ignored --nocapture
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const SHARDS: usize = 2;
const FILLER_COUNT: usize = 20_000;
const FILLER_VALUE_LEN: usize = 2_000;
const FILLER_BATCH_SIZE: usize = 400;

/// The sweep publishes the stats, so the test cannot wait 60s for the default.
const SWEEP_SECS: u64 = 1;

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
        "moon-cold-observability-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon_no_offload(port: u16, dir: &std::path::Path) -> common::ServerGuard {
    common::ServerGuard::new(
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--admin-port",
                "0",
                // Explicit: `--disk-offload` DEFAULTS to "enable", so omitting it
                // would leave the tier on and this test would assert nothing.
                "--disk-offload",
                "disable",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon (run `cargo build --release` first)"),
    )
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
                // Without a durability backstop `disk_offload_spill_inert` makes
                // eviction plain-drop victims instead of spilling them, and this
                // test would measure an empty cold tier and pass vacuously.
                "--appendonly",
                "yes",
                "--disk-free-min-pct",
                "0",
                "--cold-orphan-sweep-interval-secs",
                &SWEEP_SECS.to_string(),
                "--dir",
            ])
            .arg(dir)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon (run `cargo build --release` first)"),
    )
}

const RESTART_ATTEMPTS: usize = 6;

fn start_moon_alive(port: u16, dir: &std::path::Path) -> common::ServerGuard {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon(port, dir);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut up = false;
        while Instant::now() < deadline {
            if let Ok(Some(_status)) = child.as_mut().try_wait() {
                break;
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
        child.kill_now();
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!("moon failed to start+serve on port {port} after {RESTART_ATTEMPTS} attempts");
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

fn parse_info_u64(body: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}:");
    body.lines()
        .find(|l| l.starts_with(&prefix))
        .and_then(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse::<u64>().ok())
}

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
        // Give the background spill thread room to drain its bounded channel.
        std::thread::sleep(Duration::from_millis(30));
    }
}

/// Ground truth from the filesystem: every `heap-*.mpf` under the offload
/// dir, and the total bytes they occupy. Deliberately independent of every
/// number the server reports — this is what the INFO fields are graded
/// against.
fn heap_files_on_disk(dir: &std::path::Path) -> (usize, u64) {
    fn walk(p: &std::path::Path, n: &mut usize, bytes: &mut u64) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, n, bytes);
                } else if path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.starts_with("heap-") && s.ends_with(".mpf"))
                    .unwrap_or(false)
                {
                    *n += 1;
                    *bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }
    let mut n = 0;
    let mut bytes = 0;
    walk(&dir.join("off"), &mut n, &mut bytes);
    (n, bytes)
}

/// Poll `INFO moonstore` until the fields APPEAR at all (the block is
/// omitted until a sweep has published) or `deadline` elapses.
fn wait_for_cold_block(port: u16, deadline: Duration) -> String {
    let end = Instant::now() + deadline;
    let mut last = String::new();
    while Instant::now() < end {
        if let Some(body) = redis_cli(port, &["INFO", "moonstore"]) {
            if parse_info_u64(&body, "cold_disk_bytes").is_some() {
                return body;
            }
            last = body;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    last
}

/// Poll until `cold_disk_bytes` stops moving across consecutive sweeps.
///
/// These values are published by the cold orphan sweep, so a read taken
/// while the filler is still spilling is legitimately behind the filesystem
/// — the FIRST version of this test read mid-spill and saw 36 of 63 files.
/// That is staleness, not under-reporting, and the difference matters: one
/// is a documented freshness bound, the other is a bug. Settling first is
/// what lets the assertions below be tight enough to catch the bug.
fn wait_until_cold_stats_settle(port: u16, deadline: Duration) -> String {
    let end = Instant::now() + deadline;
    let mut prev = u64::MAX;
    let mut stable = 0;
    let mut last = String::new();
    while Instant::now() < end {
        if let Some(body) = redis_cli(port, &["INFO", "moonstore"]) {
            let now = parse_info_u64(&body, "cold_disk_bytes").unwrap_or(0);
            if now == prev && now > 0 {
                stable += 1;
                // Three identical readings a sweep apart: spilling is done and
                // the publisher has caught up with it.
                if stable >= 3 {
                    return body;
                }
            } else {
                stable = 0;
            }
            prev = now;
            last = body;
        }
        std::thread::sleep(Duration::from_millis(SWEEP_SECS * 1000 + 200));
    }
    last
}

#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn info_moonstore_reports_the_cold_tier_against_on_disk_ground_truth() {
    if !redis_cli_available() {
        eprintln!("redis-cli not on PATH -- skipping");
        return;
    }
    let dir = unique_dir("main");
    std::fs::create_dir_all(&dir).expect("create dir");
    let port = common::reserve_port();
    let mut child = start_moon_alive(port, &dir);

    // ── The section must exist and be honest BEFORE any spill ──────────
    // A field that is only correct once it is non-zero is not much of a
    // field: an operator reads it on a healthy instance too.
    // The sweep runs on a fixed timer whenever disk-offload is enabled — it is
    // NOT gated on there being work to do — so the fields must appear on an
    // instance whose cold tier is completely empty, and read a truthful zero
    // rather than being absent forever.
    let empty = wait_for_cold_block(port, Duration::from_secs(15));
    assert!(
        empty.contains("disk_offload_enabled:1"),
        "disk-offload must be on for this test to mean anything:\n{empty}"
    );
    for f in [
        "cold_keys",
        "cold_disk_bytes",
        "cold_files",
        "cold_files_referenced",
        "cold_files_dead",
        "cold_files_pending_unlink",
        "cold_index_bytes",
    ] {
        assert!(
            parse_info_u64(&empty, f).is_some(),
            "field `{f}` must be present even before a spill; got:\n{empty}"
        );
    }
    assert_eq!(
        parse_info_u64(&empty, "cold_keys"),
        Some(0),
        "nothing spilled yet, so cold_keys must be a truthful 0:\n{empty}"
    );

    write_filler(port);
    let body = wait_until_cold_stats_settle(port, Duration::from_secs(60));

    let (disk_files, disk_bytes) = heap_files_on_disk(&dir);
    assert!(
        disk_files > 0,
        "no heap-*.mpf on disk -- the filler did not spill, so this test would \
         have proven nothing. Check --appendonly/--maxmemory in start_moon."
    );

    let cold_keys = parse_info_u64(&body, "cold_keys").expect("cold_keys");
    let cold_disk_bytes = parse_info_u64(&body, "cold_disk_bytes").expect("cold_disk_bytes");
    let cold_files = parse_info_u64(&body, "cold_files").expect("cold_files");
    let cold_referenced =
        parse_info_u64(&body, "cold_files_referenced").expect("cold_files_referenced");
    let cold_dead = parse_info_u64(&body, "cold_files_dead").expect("cold_files_dead");
    let cold_index_bytes = parse_info_u64(&body, "cold_index_bytes").expect("cold_index_bytes");

    eprintln!(
        "on disk: {disk_files} files / {disk_bytes} bytes\n\
         INFO   : keys={cold_keys} disk_bytes={cold_disk_bytes} files={cold_files} \
         referenced={cold_referenced} dead={cold_dead} index_bytes={cold_index_bytes}"
    );

    // ── Graded against the filesystem, not against each other ──────────
    assert!(
        cold_keys > 0,
        "keys spilled to disk but cold_keys reads 0 -- this is the whole gap \
         the issue reports. INFO:\n{body}"
    );
    assert!(
        cold_disk_bytes > 0,
        "heap files exist on disk ({disk_files} files, {disk_bytes} bytes) but \
         cold_disk_bytes reads 0. INFO:\n{body}"
    );

    // The manifest counts LIVE (Active, non-tombstoned) files; the filesystem
    // also still holds files the sweep has tombstoned but not yet unlinked. So
    // the reported count must never EXCEED what is on disk, and must be within
    // an order of magnitude of it -- an exact match would be a race against the
    // sweep, but "reports 3 files while 900 sit on disk" is a real bug.
    assert!(
        cold_files <= disk_files as u64,
        "cold_files ({cold_files}) claims more live files than exist on disk \
         ({disk_files})"
    );
    // Settled, the manifest's live-file count must be most of what is on
    // disk. The residue is files the sweep has tombstoned but not yet
    // unlinked — real, bounded, and exactly what `cold_files_dead` and
    // `cold_files_pending_unlink` exist to expose. A loose bound here would
    // let a broken manifest predicate (say, one that matched a single shard)
    // pass, which is the failure this assertion is really aimed at.
    assert!(
        cold_files * 2 >= disk_files as u64,
        "cold_files ({cold_files}) is less than half the {disk_files} heap \
         files on disk AFTER settling -- the manifest predicate is probably \
         wrong (a per-shard predicate would report ~1/N of the truth). INFO:\n{body}"
    );
    assert!(
        cold_disk_bytes <= disk_bytes,
        "cold_disk_bytes ({cold_disk_bytes}) exceeds the {disk_bytes} bytes \
         actually on disk"
    );

    // Internal consistency, checked only AFTER the ground-truth assertions
    // above have established the numbers are real.
    assert!(
        cold_referenced <= cold_files,
        "more files referenced ({cold_referenced}) than live ({cold_files})"
    );
    assert_eq!(
        cold_dead,
        cold_files.saturating_sub(cold_referenced),
        "cold_files_dead must be exactly cold_files - cold_files_referenced"
    );

    // The index costs RAM; it is charged against used_memory, and must not be
    // confused with the disk figure. A non-trivial cold tier cannot have a
    // zero-byte index.
    assert!(
        cold_index_bytes > 0,
        "cold_keys={cold_keys} but the index reports 0 bytes of RAM"
    );

    // ── The misleading neighbours, asserted explicitly ─────────────────
    // These are the two fields an operator would reach for today. Recording
    // their behaviour here means a future change that makes either of them
    // start answering this question cannot do so silently.
    if let Some(v) = redis_cli(port, &["INFO", "reclamation"])
        .as_deref()
        .and_then(|recl| parse_info_u64(recl, "reclamation_cold_segments"))
    {
        assert_eq!(
            v, 0,
            "reclamation_cold_segments counts VECTOR segments; on this KV-only \
             instance it must read 0 -- if it ever reports the KV cold tier, \
             this test's premise (and the issue's) changed"
        );
    }

    child.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The other half of the contract: with disk-offload OFF there is no cold
/// tier, no sweep ever runs, and the fields must be ABSENT rather than a row
/// of zeros.
///
/// This is the assertion that makes the feature honest. A zero here is
/// indistinguishable from a healthy instance whose cold tier happens to be
/// empty — which is exactly the failure mode `reclamation_cold_segments`
/// already has and the reason this issue was filed. Without this test the
/// omission logic could rot into "publish zeros" and every other assertion in
/// this file would still pass.
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn cold_fields_are_absent_not_zero_when_disk_offload_is_off() {
    if !redis_cli_available() {
        eprintln!("redis-cli not on PATH -- skipping");
        return;
    }
    let dir = unique_dir("nooffload");
    std::fs::create_dir_all(&dir).expect("create dir");
    let port = common::reserve_port();

    let mut child = start_moon_no_offload(port, &dir);
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if redis_cli(port, &["PING"]).as_deref() == Some("PONG") {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Wait past the point where a sweep WOULD have published, so this is a
    // real absence and not merely an early read.
    std::thread::sleep(Duration::from_secs(3));

    let body = redis_cli(port, &["INFO", "moonstore"]).expect("INFO moonstore");
    assert!(
        body.contains("disk_offload_enabled:0"),
        "this test needs disk-offload OFF; got:\n{body}"
    );
    for f in [
        "cold_keys",
        "cold_disk_bytes",
        "cold_files",
        "cold_files_referenced",
        "cold_files_dead",
        "cold_files_pending_unlink",
        "cold_index_bytes",
    ] {
        assert!(
            parse_info_u64(&body, f).is_none(),
            "`{f}` must be ABSENT with disk-offload off, not reported as 0 -- a \
             zero is indistinguishable from a healthy empty cold tier. INFO:\n{body}"
        );
    }

    child.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}
