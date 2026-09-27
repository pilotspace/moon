//! moon#1280 against a real server: after a stall, the shard must not replay
//! every missed periodic tick back to back.
//!
//! The shard loop's periodic intervals defaulted to `MissedTickBehavior::Burst`
//! (tokio) and the same Burst in the vendored monoio, so a stalled shard —
//! SIGSTOP here; a descheduled CI runner, a VM steal or a long command in the
//! wild — fired every missed 1 ms tick on resume, and every tick ran its
//! bounded slice of housekeeping (lazy-free drain, expiry, snapshot walk…):
//! N bounded slices in one unbounded piece. Measured on 273e6bc (release-fast,
//! `--shards 1`, 3 s SIGSTOP, 4M-field UNLINK backlog): the first PING after
//! SIGCONT waited 178–211 ms on monoio and 36–50 ms on tokio.
//!
//! The signal asserted here is structural, not a latency: INFO
//! `shard_tick_burst_max` is the most periodic ticks one stall fired back to
//! back — a tick more than 5 ms late, then every tick that was already due
//! when its predecessor fired. A 1.5 s stall replayed ~1,500 of them under
//! Burst (measured: 1,502 monoio, 1,507 tokio); with the Skip policy it is one
//! tick per stall. The idle park
//! is pinned off (`MOON_IDLE_PARK=0`) so the monoio loop is on its 1 ms
//! interval — not the idle one-shot sleep — when the stall hits.
//!
//! Also pinned: skipping ticks loses granularity, never correctness — a
//! blocking command's timeout and active expiry still fire right after the
//! stall — and `instantaneous_ops_per_sec` is a rate (it read 0 forever).
//!
//! Run on both runtimes:
//! `cargo test --test perf_ws24_tick_catchup` and
//! `cargo test --no-default-features --features runtime-tokio,jemalloc --test perf_ws24_tick_catchup`.
//! Pin the binary with `MOON_BIN=<moon>`.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

/// How long the server is stopped. A Burst interval owes ~1,500 ticks.
const STALL: Duration = Duration::from_millis(1_500);

/// Upper bound on `shard_tick_burst_max` after the stall. The fix gives 1
/// per stall, and a merely late (busy or descheduled) loop cannot raise it:
/// each late tick is re-scheduled into the future and starts a new burst of
/// one. The slack covers the runtimes' own 5 ms grace — a tick late by just
/// under 5 ms is still replayed tick by tick, and the loop observes it a
/// little later than the timer fired, so one such replay can count as a burst
/// of 2 (seen once in 12 shard-runs). Measured with a 3 s stall: 1–2 fixed;
/// 3,006–3,012 (monoio) and 3,014–3,037 (tokio) with Burst restored.
const MAX_BURST: u64 = 3;

fn spawn(dir: &Path, shards: usize) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
            ])
            // Keep the monoio loop on its 1 ms Interval (tokio always is).
            .env("MOON_IDLE_PARK", "0")
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().parse::<u64>().unwrap())
        .unwrap_or_else(|| panic!("INFO stats has no {field} (moon#1280 instrumentation)"))
}

fn signal(server: &ServerGuard, sig: &str) {
    let ok = std::process::Command::new("kill")
        .args([sig, &server.id().to_string()])
        .status()
        .unwrap()
        .success();
    assert!(ok, "kill {sig} failed");
}

/// Stop the whole server process for [`STALL`], then resume it. Returns the
/// instant it was resumed.
fn stall(server: &ServerGuard) -> Instant {
    signal(server, "-STOP");
    std::thread::sleep(STALL);
    signal(server, "-CONT");
    Instant::now()
}

/// THE regression: RED on 273e6bc (~1,500 back-to-back late ticks on either
/// runtime, and the INFO field did not exist), GREEN after.
#[test]
fn a_stall_costs_one_catch_up_tick_not_a_burst() {
    for shards in [1usize, 2] {
        let dir = common::unique_test_dir(&format!("ws24-1280-burst-s{shards}"));
        let (server, port) = spawn(&dir, shards);
        let mut c = Conn::open(port);
        assert_eq!(c.send(&["PING"]), "+PONG\r\n");
        // Let every shard settle on its interval grid.
        std::thread::sleep(Duration::from_millis(200));
        let late_before = info_u64(&mut c, "shard_tick_late_total");

        stall(&server);
        assert_eq!(c.send(&["PING"]), "+PONG\r\n");
        // Whatever catch-up there is has run by now (a 1,500-tick burst takes
        // well under this even in a debug build).
        std::thread::sleep(Duration::from_millis(500));

        let late_after = info_u64(&mut c, "shard_tick_late_total");
        let run_max = info_u64(&mut c, "shard_tick_burst_max");
        assert!(
            late_after > late_before,
            "[shards={shards}] the {STALL:?} stall was not observed as a late tick \
             (late_total {late_before} -> {late_after}): the instrument is blind"
        );
        assert!(
            run_max <= MAX_BURST,
            "[shards={shards}] a {STALL:?} stall replayed {run_max} periodic ticks back to \
             back (max {MAX_BURST}); missed ticks must be skipped, not burst \
             (late_total {late_before} -> {late_after})"
        );
        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// GUARD (green before and after): skipping the missed ticks must not lose
/// the chores they drive. A BLPOP whose timeout passed during the stall is
/// answered right after it, and keys that expired during the stall are
/// actively reclaimed without being touched.
#[test]
fn chores_still_fire_after_a_stall() {
    let dir = common::unique_test_dir("ws24-1280-chores");
    let (server, port) = spawn(&dir, 1);
    let mut c = Conn::open(port);
    for i in 0..100 {
        let k = format!("ttl:{i}");
        assert_eq!(c.send(&["SET", &k, "v", "PX", "300"]), "+OK\r\n");
    }
    assert_eq!(c.send(&["DBSIZE"]), ":100\r\n");

    let mut blocked = Conn::open(port);
    blocked
        .sock
        .write_all(&encode(&["BLPOP", "ws24:none", "0.5"]))
        .unwrap();
    // Let the server register the waiter before stopping it.
    std::thread::sleep(Duration::from_millis(100));

    let resumed = stall(&server);
    let reply = blocked.read_replies_within(1, Duration::from_secs(5));
    let waited = resumed.elapsed();
    assert_eq!(reply, "*-1\r\n", "BLPOP must time out with a null reply");
    assert!(
        waited < Duration::from_secs(1),
        "a BLPOP timeout that passed during the stall must fire right after it, took {waited:?}"
    );

    // Active expiry (100 ms cadence) reclaims all 100 without any read.
    // DBSIZE counts resident keys and never expires lazily, so it only
    // drops when the sweep removes them. (INFO `expired_keys` would be the
    // natural probe, but nothing in production increments it — a separate,
    // pre-existing gap.)
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let size = c.send(&["DBSIZE"]);
        if size == ":0\r\n" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "active expiry left {size:?} of 100 keys resident 3 s after the stall"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// moon#1280 rate-estimator audit: `instantaneous_ops_per_sec` is a rate over
/// the time that actually elapsed. RED on 273e6bc: its sampler had no caller
/// and the field read 0 under any load.
#[test]
fn instantaneous_ops_per_sec_is_a_live_rate() {
    let dir = common::unique_test_dir("ws24-1280-ops");
    let (server, port) = spawn(&dir, 1);
    let mut c = Conn::open(port);
    let until = Instant::now() + Duration::from_millis(2_500);
    let mut sent = 0u64;
    while Instant::now() < until {
        assert_eq!(c.send(&["PING"]), "+PONG\r\n");
        sent += 1;
    }
    let ops = info_u64(&mut c, "instantaneous_ops_per_sec");
    // Sent at ~sent/2.5 per second; the 1 s window may straddle the start or
    // end of the loop, so only demand a clearly non-zero fraction of it.
    assert!(
        ops > 0 && ops <= sent,
        "instantaneous_ops_per_sec={ops} after {sent} PINGs in 2.5 s"
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}
