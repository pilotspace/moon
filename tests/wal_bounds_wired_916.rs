//! moon#916: `--max-wal-size` must reach `WalWriterV3` — the object the P6
//! overflow check consults — not only `CheckpointTrigger`.
//!
//! The defect was not a parse failure. `ServerConfig` always returned the
//! right number, and `src/config.rs` already asserted that — an assertion
//! that passed for the entire life of the bug. What was missing was the
//! *wiring*: the writer's bounds setter had no production caller, so every
//! instance ever run kept `DEFAULT_MAX_WAL_BYTES` (256 MiB). A live instance
//! launched with `--max-wal-size 1gb` logged:
//!
//! ```text
//! P6 WAL ceiling trigger — 326177598 bytes > max 268435456 bytes
//! ```
//!
//! ## Why these tests spawn a server
//!
//! The first version of this guard grepped `event_loop.rs` for the setter's
//! name. It stayed green with the call commented out, with the arguments
//! swapped, and with the bounds applied to a throwaway writer — and it would
//! have gone red on a *correct* tree the day the construction moved to a
//! helper module. A guard that cannot distinguish a fixed tree from a broken
//! one is worse than none.
//!
//! Every test here observes the WRITER, from outside the process:
//!
//! - the startup line `WAL writer initialized (… min_wal=…, max_wal=…)` is
//!   printed from `w.min_wal_bytes()` / `w.max_wal_bytes()`, not from the
//!   config locals, so a throwaway-writer mutation shows the default here;
//! - the P6 trigger line prints `wal.max_wal_bytes()` at the moment it
//!   fires, which is the consumer the bug hid from. That test pins the
//!   ceiling on the exact code path the live instance logged.
//!
//! Each was proven red by mutation before landing (see PR #917): passing
//! `WalBounds::DEFAULT` at the production site, swapping the arguments, and
//! binding the bounds to a second, dropped writer.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Read as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// `tracing_subscriber::fmt()` in `main.rs` writes to STDOUT, not stderr —
/// the startup and P6 lines this suite reads live there. stderr carries only
/// the pre-tracing prints (jemalloc, the `--check-config` refusal), so both
/// streams are kept, in append mode (one file per respawn attempt would
/// hide why the first attempt died).
fn server_log_sink(dir: &Path, name: &str) -> Stdio {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(name))
    {
        Ok(f) => Stdio::from(f),
        Err(_) => Stdio::null(),
    }
}

/// Spawn a persistent single-shard server with `extra` appended to the
/// baseline flags. stdout lands in `<dir>/server.out`, stderr in
/// `<dir>/server.err`.
fn spawn(dir: &Path, extra: &[&str]) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--appendonly",
                "yes",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .args(extra)
            .env("RUST_LOG", "moon=info")
            .stdout(server_log_sink(dir, "server.out"))
            .stderr(server_log_sink(dir, "server.err"))
            .spawn()
            .expect("spawn moon")
    })
}

/// Both streams, stdout first.
fn read_log(dir: &Path) -> String {
    let mut s = String::new();
    for name in ["server.out", "server.err"] {
        if let Ok(mut f) = std::fs::File::open(dir.join(name)) {
            let _ = f.read_to_string(&mut s);
        }
    }
    s
}

/// Poll the server's log until a line containing `needle` appears, or
/// `deadline` passes. Returns the FIRST matching line.
fn wait_for_log_line(dir: &Path, needle: &str, deadline: Duration) -> Option<String> {
    let until = Instant::now() + deadline;
    loop {
        if let Some(line) = read_log(dir).lines().find(|l| l.contains(needle)) {
            return Some(line.to_string());
        }
        if Instant::now() >= until {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Assert the process on `port` is OUR moon: it answers `INFO server` with a
/// `moon_version` line. A foreign listener on the port (a stray
/// redis-server, a leaked instance) would otherwise turn a wiring test into
/// a test of somebody else's log file.
fn assert_is_our_moon(port: u16) {
    let mut c = common::Conn::open(port);
    let info = c.send(&["INFO", "server"]);
    assert!(
        info.contains("moon_version"),
        "port {port} is not answering as moon: {info:?}"
    );
}

/// The ceiling the writer reports at startup is the flag, not the default.
///
/// Red by mutation: `WalBounds::DEFAULT` at the production site prints
/// `max_wal=268435456`; `WalBounds::new(max, min)` prints `max_wal=50331648`;
/// a second writer built with the bounds and dropped leaves the kept one at
/// the default.
#[test]
fn max_wal_size_reaches_the_writer_at_startup() {
    let dir = common::unique_test_dir("moon-916-startup");
    let (guard, port) = spawn(&dir, &["--max-wal-size", "1gb"]);
    assert_is_our_moon(port);

    let line = wait_for_log_line(&dir, "WAL writer initialized", Duration::from_secs(20))
        .unwrap_or_else(|| {
            panic!(
                "no 'WAL writer initialized' line within 20 s; log:\n{}",
                read_log(&dir)
            )
        });
    assert!(
        line.contains("max_wal=1073741824"),
        "moon#916 regression: the writer's ceiling is not --max-wal-size 1gb: {line}"
    );
    assert!(
        line.contains("min_wal=50331648"),
        "a 1 GiB ceiling keeps the 48 MiB default floor: {line}"
    );

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A ceiling small enough to sit under the default floor lowers the floor
/// with it (`min = max / 2`), and says so — the configuration the moon#916
/// wiring would otherwise have turned into a permanent P6 loop.
#[test]
fn small_max_wal_size_lowers_the_floor_and_warns() {
    let dir = common::unique_test_dir("moon-916-floor");
    let (guard, port) = spawn(
        &dir,
        &["--wal-segment-size", "32kb", "--max-wal-size", "96kb"],
    );
    assert_is_our_moon(port);

    let line = wait_for_log_line(&dir, "WAL writer initialized", Duration::from_secs(20))
        .unwrap_or_else(|| panic!("no WAL writer line; log:\n{}", read_log(&dir)));
    assert!(
        line.contains("min_wal=49152, max_wal=98304"),
        "floor must be max/2 under a 96kb ceiling: {line}"
    );
    assert!(
        wait_for_log_line(&dir, "lowers the WAL recycle floor", Duration::from_secs(5)).is_some(),
        "a lowered floor must be announced at startup; log:\n{}",
        read_log(&dir)
    );

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A ceiling under two segments can never be enforced — the active segment
/// is never recycled — so the server refuses to start and `--check-config`
/// reports it. Red by mutation: delete the `validate_wal_bounds` call in
/// `main.rs` and `--check-config` exits 0.
#[test]
fn max_wal_size_under_two_segments_refuses_to_start() {
    let dir = common::unique_test_dir("moon-916-refuse");
    std::fs::create_dir_all(&dir).unwrap();
    let port = common::reserve_port().to_string();
    let base = |max: &str| {
        Command::new(common::find_moon_binary())
            .args([
                "--check-config",
                "--port",
                &port,
                "--max-wal-size",
                max,
                "--dir",
            ])
            .arg(&dir)
            .output()
            .expect("run moon --check-config")
    };

    let refused = base("8mb");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success(),
        "--max-wal-size 8mb (under two 16 MiB segments) must be refused; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--max-wal-size 8mb") && stderr.contains("--wal-segment-size"),
        "refusal must name both flags:\n{stderr}"
    );

    // Positive control on the boundary: exactly two segments is accepted, so
    // the refusal above is the validation and not some other startup error.
    let accepted = base("32mb");
    assert!(
        accepted.status.success(),
        "--max-wal-size 32mb must pass --check-config; stderr:\n{}",
        String::from_utf8_lossy(&accepted.stderr)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The P6 trigger itself compares against the configured ceiling.
///
/// This is the consumer the bug hid from, on the code path the live
/// instance logged. The WAL is padded with `MQ PUSH` records: MQ history
/// has no snapshot in any mode, so neither recycler may delete a segment
/// holding one (`plane_type_blocks_recycle`), the WAL grows monotonically
/// past the ceiling, and the trigger fires deterministically once
/// `--wal-max-checkpoint-lag-ms` has elapsed. That is also the live
/// instance's shape: a ceiling breached, a recycle that frees nothing.
/// (KV writes are not used because on a single shard they take the inline
/// path, which does not log to the v3 WAL even with `--wal-kv-log on`.)
///
/// Red by mutation: `WalBounds::DEFAULT` at the production site never fires
/// (256 MiB is not reachable here), so the wait times out; swapped
/// arguments fire at `max 65536`.
#[test]
fn p6_trigger_fires_at_the_configured_ceiling() {
    let dir = common::unique_test_dir("moon-916-p6");
    let (guard, port) = spawn(
        &dir,
        &[
            "--disk-offload",
            "enable",
            "--wal-segment-size",
            "32kb",
            "--max-wal-size",
            "128kb",
            "--wal-max-checkpoint-lag-ms",
            "100",
        ],
    );
    assert_is_our_moon(port);

    let mut c = common::Conn::open(port);
    let created = c.send(&["MQ", "CREATE", "q916"]);
    assert!(created.contains("OK"), "MQ CREATE: {created}");
    let pad = "m".repeat(2048);
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut batch = 0u32;
    let line = loop {
        if let Some(l) = wait_for_log_line(&dir, "P6 WAL ceiling trigger", Duration::ZERO) {
            break l;
        }
        assert!(
            Instant::now() < deadline,
            "P6 never fired after {batch} batches; log:\n{}",
            read_log(&dir)
        );
        // 16 x 2 KiB = one 32 KiB segment per batch, every one of them
        // sole-copy plane history.
        let push: [&str; 5] = ["MQ", "PUSH", "q916", "f", &pad];
        let cmds: Vec<&[&str]> = (0..16).map(|_| &push[..]).collect();
        let replies = c.pipeline(&cmds);
        assert!(
            !replies.contains("-ERR"),
            "push batch {batch} was rejected: {replies}"
        );
        batch += 1;
        std::thread::sleep(Duration::from_millis(100));
    };

    assert!(
        line.contains("> max 131072 bytes"),
        "moon#916 regression: the P6 ceiling is not --max-wal-size 128kb: {line}"
    );

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
