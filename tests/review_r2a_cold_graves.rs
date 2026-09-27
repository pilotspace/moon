//! Adversarial review round 2 (moon#1281 round-1 fixes). Black-box, real server.
//!
//! The round-1 F1 fix (03bf3c1) applies a no-AOF snapshot's cold graves when
//! that snapshot is loaded by a `--appendonly yes` boot, and seeds the AOF
//! fold's key ledger with the buried slots "so the first AOF-led boot cannot
//! re-index it". The ledger is only ever written out by an AOF rewrite FOLD.
//! These tests boot the directory a SECOND time under `--appendonly yes`:
//!
//! 1. `r2a_second_aof_boot_keeps_cold_dels` — no fold in between (kill -9
//!    after a few writes). The second boot's KV authority is the AOF
//!    manifest the first boot created (`initialize_with_base` at s1 monoio,
//!    `initialize_multi` at s4) or, at tokio s1, the snapshot + legacy AOF.
//! 2. `r2a_second_aof_boot_after_bgsave_keeps_cold_dels` — a BGSAVE in the
//!    `--appendonly yes` process first: its snapshot carries no trailer.
//! 3. `r2a_second_aof_boot_after_rewrite_keeps_cold_dels` — a BGREWRITEAOF
//!    in the `--appendonly yes` process first: the fold writes the ledger's
//!    DELs (the control that proves the ledger seeding works).
//!
//!   MOON_BIN=... [MOON_TEST_COLD_DEL_SHARDS=1] cargo test --test review_r2a_cold_graves -- --ignored --test-threads 1 --nocapture

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

const SAVE: [&str; 2] = ["--save", "3600 100000000"];

fn bgsave_and_wait(port: u16) {
    let lastsave = || integer_reply(&redis_cmd(port, &["LASTSAVE"])).unwrap_or(0);
    let before = lastsave();
    std::thread::sleep(Duration::from_millis(1_100));
    redis_cmd(port, &["BGSAVE"]);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = redis_cmd(port, &["INFO", "persistence"]);
        if info.contains("rdb_bgsave_in_progress:0") && lastsave() > before {
            assert!(
                info.contains("rdb_last_bgsave_status:ok"),
                "BGSAVE failed: {info}"
            );
            return;
        }
        assert!(Instant::now() < deadline, "BGSAVE never completed: {info}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Phase 1 (`--appendonly yes`) spills the probes, stops cleanly, and its
/// AOF is removed: the next phase inherits the cold tier only.
fn inherit_cold_probes(port: u16, dir: &Path) {
    let mut s1 = start_moon(port, dir, 3600);
    wait_for_port(port);
    spill_probes(port, dir);
    let _ = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SHUTDOWN", "NOSAVE"])
        .output();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && s1.as_mut().try_wait().ok().flatten().is_none() {
        std::thread::sleep(Duration::from_millis(100));
    }
    s1.kill_now();
    wait_for_port_down(port);
    let _ = std::fs::remove_file(dir.join("appendonly.aof"));
    let _ = std::fs::remove_dir_all(dir.join("appendonlydir"));
}

fn probes_back(port: u16) -> usize {
    (0..PROBE_COUNT)
        .filter(|i| redis_get(port, &probe_key(*i)).is_some())
        .count()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Between {
    Nothing,
    Bgsave,
    Rewrite,
}

/// No-AOF DEL of every cold probe + BGSAVE (trailer) + kill -9; boot with
/// `--appendonly yes` (boot A); optionally BGSAVE / BGREWRITEAOF; kill -9;
/// boot with `--appendonly yes` again (boot B). Every deleted probe must stay
/// deleted and every acknowledged key must stay, at both boots.
fn run(label: &str, between: Between) {
    let port = common::reserve_port();
    let dir = unique_dir(label);
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_probes(port, &dir);

    let mut server = start_moon_with(port, &dir, 1, "no", &SAVE);
    wait_for_port(port);
    assert!(
        wait_until(20, || info_u64(port, "cold_keys").unwrap_or(0) > 0),
        "precondition: nothing cold inherited"
    );
    redis_set(port, "hot:control", "H");
    let mut deleted = 0;
    for i in 0..PROBE_COUNT {
        deleted += integer_reply(&redis_cmd(port, &["DEL", &probe_key(i)])).unwrap_or(0);
    }
    assert!(deleted > 0, "precondition: DEL removed no probe");
    bgsave_and_wait(port);
    std::thread::sleep(Duration::from_secs(3));
    server.kill_now();
    wait_for_port_down(port);

    // Boot A: the operator turns the AOF on.
    let mut a = start_moon_alive_with(port, &dir, 3600, "yes", &SAVE);
    let back_a = probes_back(port);
    let hot_a = redis_get(port, "hot:control");
    // An acknowledged write under the AOF, so boot B has a log to replay.
    redis_set(port, "after:a", "A");
    match between {
        Between::Nothing => {}
        Between::Bgsave => bgsave_and_wait(port),
        Between::Rewrite => {
            let _ = redis_cmd(port, &["BGREWRITEAOF"]);
            let ok = wait_until(60, || {
                redis_cmd(port, &["INFO", "persistence"]).contains("aof_rewrite_in_progress:0")
            });
            assert!(ok, "BGREWRITEAOF never finished");
            std::thread::sleep(Duration::from_secs(2));
        }
    }
    std::thread::sleep(Duration::from_secs(2));
    a.kill_now();
    wait_for_port_down(port);

    // Boot B: same flags.
    let mut b = start_moon_alive_with(port, &dir, 3600, "yes", &SAVE);
    let back_b = probes_back(port);
    let hot_b = redis_get(port, "hot:control");
    let after_b = redis_get(port, "after:a");
    b.kill_now();
    wait_for_port_down(port);

    eprintln!(
        "{label} ({between:?}, shards {}): deleted {deleted}; boot A: {back_a} probes back, \
         hot:control={hot_a:?}; boot B: {back_b} probes back, hot:control={hot_b:?}, \
         after:a={after_b:?}",
        shards()
    );
    let mut wrong = Vec::new();
    if back_a != 0 {
        wrong.push(format!(
            "boot A: {back_a}/{deleted} deleted cold probes back"
        ));
    }
    if hot_a.as_deref() != Some("H") {
        wrong.push(format!("boot A: hot:control = {hot_a:?}"));
    }
    if back_b != 0 {
        wrong.push(format!(
            "boot B: {back_b}/{deleted} deleted cold probes back"
        ));
    }
    if hot_b.as_deref() != Some("H") {
        wrong.push(format!("boot B: hot:control = {hot_b:?} (acked at boot A)"));
    }
    if after_b.as_deref() != Some("A") {
        wrong.push(format!("boot B: after:a = {after_b:?}"));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{label} ({between:?}): {wrong:?}");
}

#[test]
#[ignore]
fn r2a_second_aof_boot_keeps_cold_dels() {
    run("r2a-2nd-aof", Between::Nothing);
}

#[test]
#[ignore]
fn r2a_second_aof_boot_after_bgsave_keeps_cold_dels() {
    run("r2a-2nd-aof-bgsave", Between::Bgsave);
}

#[test]
#[ignore]
fn r2a_second_aof_boot_after_rewrite_keeps_cold_dels() {
    run("r2a-2nd-aof-rewrite", Between::Rewrite);
}
