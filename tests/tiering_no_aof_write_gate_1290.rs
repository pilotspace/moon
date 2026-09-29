//! moon#1290: under `--appendonly no --disk-offload enable` (a shard
//! manifest exists), `allkeys-lru` must TIER victims, never plain-drop them,
//! while the spill thread is healthy.
//!
//! N6: the connection write gate (`handler_monoio::run_write_eviction_gate`,
//! the tokio `handler_sharded` gates) and the cross-shard SPSC gate built
//! `EvictionRun::async_spill(.., None)`: with no manifest and no AOF the sink
//! fell back to a plain drop. Instrumented on ce65400 (monoio, 16.2K SETs,
//! 8 MB): s4 dropped 5.4K keys — 1.3K at `handler_monoio/mod.rs:315`, 3.9K at
//! `spsc_handler.rs:91` — and s1 5.4K at the write gate; 1–7 keys spilled.
//!
//! N7: `CONFIG SET appendonly yes` changes only the config string, which the
//! eviction sink used to read to pick the async (AOF-backstopped) path in a
//! process with no AOF writer. The second case runs the same flow after it.
//!
//! Each case: probes + 16K fillers (pipelined), settle; `evicted_keys` must be
//! 0, `DBSIZE` 16,200, every probe must read its value; then BGSAVE, kill -9
//! and restart: `DBSIZE` must still be 16,200 (snapshot + cold files).
//!
//!   MOON_BIN=... [MOON_TEST_COLD_DEL_SHARDS=1] cargo test --test tiering_no_aof_write_gate_1290 -- --include-ignored --test-threads 1 --nocapture

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

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

fn dbsize(port: u16) -> i64 {
    integer_reply(&redis_cmd(port, &["DBSIZE"])).unwrap_or(-1)
}

fn probes_ok(port: u16) -> usize {
    let val = probe_value();
    (0..PROBE_COUNT)
        .filter(|i| redis_get(port, &probe_key(*i)).as_deref() == Some(val.as_str()))
        .count()
}

fn run(label: &str, config_set_appendonly: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(label);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let expected = (PROBE_COUNT + FILLER_COUNT) as i64;

    let mut server = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    if config_set_appendonly {
        redis_cmd(port, &["CONFIG", "SET", "appendonly", "yes"]);
    }
    spill_probes(port, &dir);
    let evicted = info_u64(port, "evicted_keys").unwrap_or(u64::MAX);
    let spilled = info_u64(port, "spilled_keys").unwrap_or(0);
    let degraded = info_u64(port, "spill_thread_degraded").unwrap_or(0);
    let live = dbsize(port);
    let probes_live = probes_ok(port);
    bgsave_and_wait(port);
    server.kill_now();
    wait_for_port_down(port);

    let mut after = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let live_after = dbsize(port);
    let probes_after = probes_ok(port);
    after.kill_now();
    wait_for_port_down(port);

    eprintln!(
        "{label} (shards {}, CONFIG SET appendonly yes: {config_set_appendonly}): evicted_keys \
         {evicted}, spilled_keys {spilled}, spill_thread_degraded {degraded}; DBSIZE {live}, \
         probes {probes_live}/{PROBE_COUNT}; after kill -9 + restart DBSIZE {live_after}, \
         probes {probes_after}/{PROBE_COUNT}",
        shards()
    );
    let mut wrong = Vec::new();
    if degraded != 0 {
        wrong.push(format!("precondition: spill thread degraded ({degraded})"));
    }
    if evicted != 0 {
        wrong.push(format!(
            "{evicted} keys plain-dropped (evicted_keys) instead of tiered"
        ));
    }
    if live != expected || probes_live != PROBE_COUNT {
        wrong.push(format!(
            "live: DBSIZE {live} (want {expected}), probes {probes_live}/{PROBE_COUNT}"
        ));
    }
    if live_after != expected || probes_after != PROBE_COUNT {
        wrong.push(format!(
            "after restart: DBSIZE {live_after} (want {expected}), probes \
             {probes_after}/{PROBE_COUNT}"
        ));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{label}: {wrong:?}");
}

#[test]
#[ignore]
fn no_aof_allkeys_lru_tiers_every_victim() {
    run("r1290-n6", false);
}

#[test]
#[ignore]
fn config_set_appendonly_yes_without_a_writer_still_tiers_durably() {
    run("r1290-n7", true);
}
