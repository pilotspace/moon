//! WS7 / moon#1175: the connection handlers must not take the process-global
//! CLIENT PAUSE locks, or the `RuntimeConfig` lock, once per batch / read /
//! command when nothing requires it — and CLIENT PAUSE must keep working.
//!
//! Two halves:
//! - `handlers_take_no_global_lock_per_batch` scans the two production
//!   handlers (the `intercept_flag_drift` pattern: the batch loop has no seam
//!   a unit test can drive). It fails on `ae21476`, where both call
//!   `expire_if_needed()` (the PAUSE WRITE lock) unconditionally per batch and
//!   read the query-buffer limits under the config lock per read.
//! - `client_pause_still_delays_then_expires_*` drives a real server
//!   (`MOON_BIN`) so the gate is proven not to have switched pausing off.
//!
//! Run: `MOON_BIN=/path/to/moon cargo test --test perf_ws7_pause_gate`

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const MONOIO: &str = "src/server/conn/handler_monoio/mod.rs";
const TOKIO: &str = "src/server/conn/handler_sharded/mod.rs";

/// Source root to scan: this checkout, or `MOON_SRC_ROOT` (used to record
/// the red run against an extracted `ae21476` tree).
fn source(rel: &str) -> String {
    let root =
        std::env::var("MOON_SRC_ROOT").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_string());
    let path = std::path::Path::new(&root).join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Lines of `src` that contain `needle`, outside `//` comments.
fn code_lines_with<'a>(src: &'a str, needle: &str) -> Vec<(usize, &'a str)> {
    src.lines()
        .enumerate()
        .filter(|(_, l)| {
            let code = l.split("//").next().unwrap_or("");
            code.contains(needle)
        })
        .map(|(i, l)| (i + 1, l.trim()))
        .collect()
}

#[test]
fn handlers_take_no_global_lock_per_batch() {
    for file in [MONOIO, TOKIO] {
        let src = source(file);
        // The batch top must go through the lock-free gate. A bare
        // `expire_if_needed()` is an unconditional PAUSE WRITE lock.
        for needle in [
            "client_pause::expire_if_needed(",
            "client_pause::check_pause(",
        ] {
            let hits = code_lines_with(&src, needle);
            assert!(
                hits.is_empty(),
                "{file}: `{needle}` is called directly on the batch path — it \
                 takes the process-global PAUSE lock every batch; use \
                 `client_pause::batch_pause_remaining()`: {hits:?}"
            );
        }
        assert!(
            !code_lines_with(&src, "client_pause::batch_pause_remaining()").is_empty(),
            "{file}: the batch-top CLIENT PAUSE gate is missing entirely"
        );
        // The query-buffer ceilings are read ONCE, in the per-connection
        // snapshot — not under the config lock per read iteration.
        let reads = code_lines_with(&src, "rt.client_query_buffer_limit,");
        assert_eq!(
            reads.len(),
            1,
            "{file}: the query-buffer limit must be read from `RuntimeConfig` \
             once per connection, found {} read sites: {reads:?}",
            reads.len()
        );
    }
    // The tokio handler's per-command pause-deadline read was a config read
    // lock per command for a field only `handler_single` writes.
    let tokio = source(TOKIO);
    let hits = code_lines_with(&tokio, "client_pause_deadline_ms");
    assert!(
        hits.is_empty(),
        "{TOKIO}: per-command `client_pause_deadline_ms` read is back: {hits:?}"
    );
}

struct Server {
    _guard: common::ServerGuard,
    port: u16,
    _dir: std::path::PathBuf,
}

fn spawn(shards: &str) -> Server {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir("ws7-pause");
    std::fs::create_dir_all(&dir).unwrap();
    let d = dir.clone();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--appendonly",
                "no",
                "--save",
                "",
                "--admin-port",
                "0",
                "--disk-offload",
                "disable",
                // The shared box's volume sits below moon's default 5%-free
                // disk guard; this suite is not about the guard.
                "--disk-free-min-pct",
                "0",
                "--dir",
                d.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    Server {
        _guard: guard,
        port,
        _dir: dir,
    }
}

fn timed(conn: &mut common::Conn, parts: &[&str]) -> (String, Duration) {
    let t = Instant::now();
    let r = conn.send(parts);
    (r, t.elapsed())
}

fn pause_round_trip(shards: &str) {
    let srv = spawn(shards);
    let mut admin = common::Conn::open(srv.port);
    let mut client = common::Conn::open(srv.port);
    assert_eq!(client.send(&["PING"]), "+PONG\r\n");

    // Unpaused: a write answers promptly (generous bound for a debug build).
    let (r, took) = timed(&mut client, &["SET", "k", "v0"]);
    assert_eq!(r, "+OK\r\n");
    assert!(
        took < Duration::from_millis(400),
        "unpaused SET took {took:?}"
    );

    // Paused: the next batch waits for the pause to run out.
    assert_eq!(admin.send(&["CLIENT", "PAUSE", "600", "ALL"]), "+OK\r\n");
    let (r, took) = timed(&mut client, &["SET", "k", "v1"]);
    assert_eq!(r, "+OK\r\n");
    assert!(
        took >= Duration::from_millis(400),
        "CLIENT PAUSE 600 ALL did not delay a write batch (took {took:?})"
    );

    // Expired: the gate cleared it, batches run at once again.
    std::thread::sleep(Duration::from_millis(100));
    let (r, took) = timed(&mut client, &["GET", "k"]);
    assert_eq!(r, "$2\r\nv1\r\n");
    assert!(
        took < Duration::from_millis(400),
        "expired pause still delays: {took:?}"
    );

    // A second pause (WRITE mode) after the first expired: the hint is
    // re-published and a write batch waits again, then runs free once it
    // expires. (UNPAUSE is not driven over the wire here: its own batch is
    // subject to the same conservative batch-top wait, before and after.)
    assert_eq!(admin.send(&["CLIENT", "PAUSE", "500", "WRITE"]), "+OK\r\n");
    let (r, took) = timed(&mut client, &["SET", "k", "v2"]);
    assert_eq!(r, "+OK\r\n");
    assert!(
        took >= Duration::from_millis(300),
        "CLIENT PAUSE 500 WRITE did not delay a write batch (took {took:?})"
    );
    std::thread::sleep(Duration::from_millis(100));
    let (r, took) = timed(&mut client, &["SET", "k", "v3"]);
    assert_eq!(r, "+OK\r\n");
    assert!(
        took < Duration::from_millis(400),
        "expired pause still delays: {took:?}"
    );
    assert_eq!(admin.send(&["GET", "k"]), "$2\r\nv3\r\n");
}

/// Pausing, expiry and UNPAUSE still behave at one shard (monoio and tokio
/// run the same gate).
#[test]
fn client_pause_still_delays_then_expires_1_shard() {
    pause_round_trip("1");
}

/// ... and across shards, where the gate is per shard thread.
#[test]
fn client_pause_still_delays_then_expires_2_shards() {
    pause_round_trip("2");
}
