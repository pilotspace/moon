//! moon#1291 F8: restarting a no-AOF server with a smaller `--databases`
//! must not lose the cold graves of the databases it cannot attach.
//!
//! 1. An `--appendonly yes` phase spills probes and fillers of db 1, stops
//!    cleanly, and its AOF is removed: the next phase inherits db 1's cold
//!    tier only (no hot db 1 key survives it).
//! 2. `--appendonly no`: DEL every cold db-1 probe, BGSAVE (the trailer names
//!    their slots), kill -9.
//! 3. `--appendonly no --databases 1`: db 1 is not attached. BGSAVE, kill -9.
//! 4. `--appendonly no` with the default count again: db 1 is re-attached, and
//!    every probe deleted at step 2 must stay deleted.
//!
//! Before the fix step 3 aborted the snapshot load at db 1's selector
//! ("snapshot references database 1 but only 1 configured"), so the trailer
//! after EOF was never read, and the rebuild dropped db 1's index with its
//! graves; step 3's snapshot carried none, and step 4 brought the deleted
//! probes back.
//!
//!   MOON_BIN=... [MOON_TEST_COLD_DEL_SHARDS=1] cargo test --test cold_graves_reduced_databases_1291 -- --include-ignored --test-threads 1 --nocapture

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

const SAVE: [&str; 2] = ["--save", "3600 100000000"];
const DB: usize = 1;

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

fn probes_in_db(port: u16) -> usize {
    (0..PROBE_COUNT)
        .filter(|i| redis_get_db(port, DB, &probe_key(*i)).is_some())
        .count()
}

fn dbsize(port: u16, db: usize) -> i64 {
    integer_reply(&redis_cmd_db(port, db, &["DBSIZE"])).unwrap_or(-1)
}

/// Step 1: spill db 1's probes and fillers under `--appendonly yes`, stop
/// cleanly, remove the AOF.
fn inherit_cold_db1(port: u16, dir: &Path) {
    let mut s1 = start_moon(port, dir, 3600);
    wait_for_port(port);
    let val = probe_value();
    for i in 0..PROBE_COUNT {
        redis_cmd_db(port, DB, &["SET", &probe_key(i), &val]);
    }
    write_filler_in_db(port, DB);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
    assert!(
        count_heap_files(dir) > 0,
        "precondition: the db-1 filler did not force a spill"
    );
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

#[test]
#[ignore]
fn reduced_databases_restart_keeps_the_unattached_dbs_graves() {
    let port = common::reserve_port();
    let dir = unique_dir("r1291-f8-dbs");
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_db1(port, &dir);

    // Step 2: the no-AOF era deletes db 1's cold probes and snapshots.
    let mut two = start_moon_alive_with(port, &dir, 1, "no", &SAVE);
    let cold_before = probes_in_db(port);
    assert!(
        cold_before > 0,
        "precondition: no cold db-1 probe inherited"
    );
    redis_set(port, "hot:control", "H");
    let mut deleted = 0;
    for i in 0..PROBE_COUNT {
        deleted += integer_reply(&redis_cmd_db(port, DB, &["DEL", &probe_key(i)])).unwrap_or(0);
    }
    assert!(deleted > 0, "precondition: DEL removed no probe");
    let db1_live = dbsize(port, DB);
    bgsave_and_wait(port);
    std::thread::sleep(Duration::from_secs(2));
    two.kill_now();
    wait_for_port_down(port);

    // Step 3: a boot that cannot attach db 1, and its own snapshot.
    let mut three = start_moon_alive_with(
        port,
        &dir,
        3600,
        "no",
        &["--databases", "1", SAVE[0], SAVE[1]],
    );
    let hot_three = redis_get(port, "hot:control");
    bgsave_and_wait(port);
    std::thread::sleep(Duration::from_secs(2));
    three.kill_now();
    wait_for_port_down(port);

    // Step 4: the original count again.
    let mut four = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let back = probes_in_db(port);
    let hot_four = redis_get(port, "hot:control");
    let db1_after = dbsize(port, DB);
    four.kill_now();
    wait_for_port_down(port);

    eprintln!(
        "r1291-f8 (shards {}): {cold_before} cold db-1 probes, deleted {deleted}; db 1 DBSIZE \
         {db1_live} before, {db1_after} after; boot 3 hot:control={hot_three:?}; boot 4: \
         {back} probes back, hot:control={hot_four:?}",
        shards()
    );
    let mut wrong = Vec::new();
    if back != 0 {
        wrong.push(format!(
            "boot 4: {back}/{deleted} deleted cold db-1 probes back"
        ));
    }
    if db1_after != db1_live || db1_live <= 0 {
        wrong.push(format!(
            "boot 4: db 1 DBSIZE {db1_after}, {db1_live} live at the step-2 snapshot (a \
             grave must never hide a live key; every cold filler must be re-attached)"
        ));
    }
    if hot_three.as_deref() != Some("H") || hot_four.as_deref() != Some("H") {
        wrong.push(format!(
            "hot:control boot 3 {hot_three:?}, boot 4 {hot_four:?}"
        ));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{wrong:?}");
}
