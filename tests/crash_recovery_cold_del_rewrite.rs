//! A cold key removed BEFORE an AOF rewrite must stay removed across a kill -9.
//!
//! `crash_recovery_cold_del_resurrection` proves the replayed DEL/FLUSH reaches
//! the cold plane. This suite asks what happens once a rewrite has DISCARDED
//! that record: the new generation's base is hot-only, its incr opens with a
//! `MOON.COLDCUT` that authorizes every pre-rewrite cold file, and recovery
//! rebuilds the cold index from every manifest-Active file. A deleted key
//! whose spill file still backs live neighbours (so the orphan sweep never
//! retires it) has nothing durable left that remembers the delete.
//!
//! Scenario per test: SET probes -> filler evicts them cold -> mutate the EVEN
//! probes (DEL / overwrite / FLUSHDB / TTL expiry) -> optionally BGREWRITEAOF
//! and wait until every shard has a compacted base -> SIGKILL -> restart ->
//! every even probe must read its post-mutation state and every odd probe
//! (the live neighbours) its original value.
//!
//! Status on main 843611f5 and v0.8.9 (macOS, monoio/kqueue, release-fast):
//! RED  deleted_*_and_crash, deleted_*_and_clean_restart,
//!      deleted_*_after_the_orphan_sweep, flushed_*_and_crash (sweep held off)
//! ok   control_*_without_rewrite, overwritten_*, expired_*,
//!      flushed_*_after_the_orphan_sweep
//!
//! moon#1231 (promote-then-sweep) lives at the end of the file: a probe cold
//! at the rewrite, then read back into RAM (GET) or read-modify-written
//! (APPEND) after its spill file's other keys were deleted, must survive the
//! orphan sweep and a kill -9. RED on `ae21476` and on int/part3b (`5b14b82`).
//!
//! WS20 (after it): keyspace-level operations on cold keys — MOVE / COPY … DB
//! (moon#1254), a TTL'd overwrite of a cold key (moon#1236) and SWAPDB around
//! cold keys (moon#1237). RED on `ae21476` and on `4a96cd5f`. The
//! `--appendonly no` cases are in `crash_recovery_cold_no_aof.rs`; the shared
//! harness is `crash_recovery_cold_support/`.
//!
//! Run with (monoio default):
//!   cargo build --release
//!   MOON_BIN=target/release/moon cargo test --release \
//!     --test crash_recovery_cold_del_rewrite -- --ignored --test-threads 1
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::process::Command;
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

/// What an even probe must read after recovery.
#[derive(Clone, Copy)]
enum Expect {
    Absent,
    Overwritten,
    /// FLUSHDB removed every probe, odd ones included.
    AllAbsent,
}

/// How round 1 ends.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stop {
    Kill9,
    /// `SHUTDOWN` — a clean stop; the resurrection must not depend on a crash.
    Shutdown,
    /// kill -9 with a 1s orphan sweep that has run several times before the
    /// rewrite: the pre-sweep window is closed, so what survives is only what
    /// the sweep cannot retire.
    Kill9AfterSweep,
}

impl Stop {
    fn sweep_secs(self) -> u64 {
        match self {
            Stop::Kill9AfterSweep => 1,
            Stop::Kill9 | Stop::Shutdown => 3600,
        }
    }
}

fn run_scenario(
    suffix: &str,
    stop: Stop,
    ttl_ms: Option<u64>,
    mutate: impl Fn(u16),
    rewrite: bool,
    expect: Expect,
) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");

    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);

    let val = probe_value();
    for i in 0..PROBE_COUNT {
        match ttl_ms {
            // Only the even probes carry a TTL; odd ones are the live neighbours.
            Some(ms) if i % 2 == 0 => {
                redis_cmd(port, &["SET", &probe_key(i), &val, "PX", &ms.to_string()]);
            }
            _ => redis_set(port, &probe_key(i), &val),
        }
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
    let heap_files = count_heap_files(&dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill"
    );

    mutate(port);
    match expect {
        Expect::Absent | Expect::AllAbsent => assert!(
            redis_get(port, &probe_key(0)).is_none(),
            "precondition failed: probe:0 still readable on the live server"
        ),
        Expect::Overwritten => assert_eq!(
            redis_get(port, &probe_key(0)),
            Some(overwrite_value()),
            "precondition failed: overwrite not visible on the live server"
        ),
    }
    if stop == Stop::Kill9AfterSweep {
        std::thread::sleep(Duration::from_secs(5));
    }
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));

    if stop == Stop::Shutdown {
        // The connection drops on a successful SHUTDOWN, so the reply is not checked.
        let _ = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "SHUTDOWN"])
            .output();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    server.kill_now();
    wait_for_port_down(port);
    let mut server2 = start_moon_alive(port, &dir, stop.sweep_secs());

    let mut wrong = Vec::new();
    let mut neighbours_lost = 0usize;
    for i in 0..PROBE_COUNT {
        let got = redis_get(port, &probe_key(i));
        let even = i % 2 == 0;
        let ok = match (expect, even) {
            (Expect::AllAbsent, _) | (Expect::Absent, true) => got.is_none(),
            (Expect::Overwritten, true) => got.as_deref() == Some(overwrite_value().as_str()),
            (_, false) => {
                let ok = got.as_deref() == Some(val.as_str());
                if !ok {
                    neighbours_lost += 1;
                }
                ok
            }
        };
        if !ok {
            wrong.push(probe_key(i));
        }
    }
    server2.kill_now();
    if wrong.is_empty() && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }
    assert!(
        wrong.is_empty(),
        "{} probe(s) recovered the wrong state after {}restart (neighbours lost: {}, \
         first: {:?}, heap files at mutation: {})",
        wrong.len(),
        if rewrite { "BGREWRITEAOF + " } else { "" },
        neighbours_lost,
        wrong.first(),
        heap_files
    );
}

fn del_even(port: u16) {
    for chunk in (0..PROBE_COUNT)
        .filter(|i| i % 2 == 0)
        .collect::<Vec<_>>()
        .chunks(50)
    {
        let keys: Vec<String> = chunk.iter().map(|i| probe_key(*i)).collect();
        let mut args = vec!["DEL"];
        args.extend(keys.iter().map(String::as_str));
        redis_cmd(port, &args);
    }
}

fn overwrite_even(port: u16) {
    let v = overwrite_value();
    for i in (0..PROBE_COUNT).filter(|i| i % 2 == 0) {
        redis_set(port, &probe_key(i), &v);
    }
}

fn flushdb(port: u16) {
    redis_cmd(port, &["FLUSHDB"]);
}

/// Control: no rewrite — the DEL records replay onto the cold plane.
#[test]
#[ignore] // requires a release moon + redis-cli; run with -- --ignored
fn control_deleted_cold_keys_stay_deleted_without_rewrite() {
    run_scenario(
        "del-norw",
        Stop::Kill9,
        None,
        del_even,
        false,
        Expect::Absent,
    );
}

/// The suspected resurrection: DEL, then a rewrite discards the DEL records.
#[test]
#[ignore]
fn deleted_cold_keys_stay_deleted_across_rewrite_and_crash() {
    run_scenario("del-rw", Stop::Kill9, None, del_even, true, Expect::Absent);
}

/// Overwrite instead of delete: the new value is hot and lands in the base.
#[test]
#[ignore]
fn overwritten_cold_keys_keep_the_new_value_across_rewrite_and_crash() {
    run_scenario(
        "ovw-rw",
        Stop::Kill9,
        None,
        overwrite_even,
        true,
        Expect::Overwritten,
    );
}

/// FLUSHDB while every probe is cold, then a rewrite.
#[test]
#[ignore]
fn flushed_cold_keys_stay_flushed_across_rewrite_and_crash() {
    run_scenario(
        "flush-rw",
        Stop::Kill9,
        None,
        flushdb,
        true,
        Expect::AllAbsent,
    );
}

/// The even probes expire while cold, then a rewrite. Their spill slots carry
/// the deadline, so they must read as absent after recovery.
#[test]
#[ignore]
fn expired_cold_keys_stay_expired_across_rewrite_and_crash() {
    // Long enough to be spilled live (filler + settle ~9s), short enough to
    // have expired before the rewrite.
    const TTL_MS: u64 = 15_000;
    run_scenario(
        "ttl-rw",
        Stop::Kill9,
        Some(TTL_MS),
        |_port| std::thread::sleep(Duration::from_millis(TTL_MS)),
        true,
        Expect::Absent,
    );
}

/// The same DEL + rewrite, ended by a clean SHUTDOWN instead of a crash.
#[test]
#[ignore]
fn deleted_cold_keys_stay_deleted_across_rewrite_and_clean_restart() {
    run_scenario(
        "del-rw-shutdown",
        Stop::Shutdown,
        None,
        del_even,
        true,
        Expect::Absent,
    );
}

/// DEL + rewrite with the orphan sweep given time to run first: a spill file
/// that still backs live neighbours is never retired, so the window is not a
/// crash window at all — it lasts as long as the neighbours live.
#[test]
#[ignore]
fn deleted_cold_keys_stay_deleted_across_rewrite_after_the_orphan_sweep() {
    run_scenario(
        "del-rw-swept",
        Stop::Kill9AfterSweep,
        None,
        del_even,
        true,
        Expect::Absent,
    );
}

/// FLUSHDB + rewrite once the orphan sweep has run: every flushed file lost
/// its last referrer and was retired, so nothing is left to resurrect. The
/// FLUSHDB exposure is bounded by `--cold-orphan-sweep-interval-secs`.
#[test]
#[ignore]
fn flushed_cold_keys_stay_flushed_across_rewrite_after_the_orphan_sweep() {
    run_scenario(
        "flush-rw-swept",
        Stop::Kill9AfterSweep,
        None,
        flushdb,
        true,
        Expect::AllAbsent,
    );
}

// ── moon#1231: promote-then-sweep ────────────────────────────────────────────
//
// A probe that is cold when BGREWRITEAOF cuts its base is not in that base:
// its spill slot is its only durable copy for the whole new generation. The
// fillers that share its spill files are then DELeted and the probe itself is
// read back into RAM (GET) or read-modify-written (APPEND) — neither leaves a
// record that recreates its value. Once every key of a file has left the cold
// index, the orphan sweep (1 s here) used to unlink it, and a kill -9 then
// lost the probe (GET) or replayed the APPEND onto nothing (APPEND). Reproduced
// 88/200 on `ae21476` by the WS15 probe script this scenario encodes.

/// How each probe is touched after the rewrite.
#[derive(Clone, Copy)]
enum Touch {
    /// `GET`: a read promotion, logged nowhere.
    Get,
    /// `APPEND probe +`: a read-modify-write promotion; only the APPEND is
    /// logged.
    Append,
}

fn appended_value() -> String {
    format!("{}+", probe_value())
}

/// moon#1231 scenario (see the section comment). With `second_rewrite`, a
/// second BGREWRITEAOF follows the touches: it captures the promoted probes in
/// its base, after which the sweep must release the spill files it held — the
/// hold is a wait for a fold, not a leak — and a kill -9 still loses nothing.
fn run_promote_scenario(suffix: &str, touch: Touch, second_rewrite: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let sweep_secs = 1;

    let mut server = start_moon(port, &dir, sweep_secs);
    wait_for_port(port);

    let val = probe_value();
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &val);
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
    let heap_files = count_heap_files(&dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill"
    );
    let cold_keys = info_u64(port, "cold_keys").unwrap_or(0);
    assert!(
        cold_keys > 0,
        "precondition failed: INFO reports no cold key after the filler"
    );

    // The fold: the probes that are cold now are in no base.
    rewrite_and_wait(port, &dir);
    del_fillers(port);
    let expected = match touch {
        Touch::Get => val.clone(),
        Touch::Append => appended_value(),
    };
    for i in 0..PROBE_COUNT {
        match touch {
            Touch::Get => assert_eq!(
                redis_get(port, &probe_key(i)).as_deref(),
                Some(val.as_str()),
                "precondition failed: {} unreadable on the live server",
                probe_key(i)
            ),
            Touch::Append => {
                let len = redis_cmd(port, &["APPEND", &probe_key(i), "+"]);
                assert_eq!(
                    len.trim_start_matches("(integer) "),
                    (PROBE_VALUE_LEN + 1).to_string(),
                    "precondition failed: APPEND {} did not see the cold value",
                    probe_key(i)
                );
            }
        }
    }
    // Several 1 s orphan sweeps run.
    std::thread::sleep(Duration::from_secs(5));
    let files_before_second = count_heap_files(&dir);

    let mut released = None;
    if second_rewrite {
        assert!(
            files_before_second > 0,
            "the spill files that backed the probes at the fold were unlinked before any \
             later fold captured them ({heap_files} at the fold, 0 now)"
        );
        rewrite_and_wait(port, &dir);
        std::thread::sleep(Duration::from_secs(4));
        released = Some(count_heap_files(&dir));
    }

    server.kill_now();
    wait_for_port_down(port);
    let mut server2 = start_moon_alive(port, &dir, sweep_secs);
    let mut lost = 0usize;
    let mut wrong = Vec::new();
    for i in 0..PROBE_COUNT {
        let got = redis_get(port, &probe_key(i));
        if got.as_deref() != Some(expected.as_str()) {
            if got.is_none() {
                lost += 1;
            }
            wrong.push((probe_key(i), got.map(|v| v.len())));
        }
    }
    server2.kill_now();
    if wrong.is_empty() && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }
    assert!(
        wrong.is_empty(),
        "{} of {} acknowledged, never-deleted probes recovered wrong after BGREWRITEAOF, \
         DEL of the fillers, {} of every probe, orphan sweeps{} and kill -9 ({} absent; \
         first: {:?}; heap files: {} at the fold, {} before the kill; cold keys at the \
         fold: {})",
        wrong.len(),
        PROBE_COUNT,
        match touch {
            Touch::Get => "GET",
            Touch::Append => "APPEND",
        },
        if second_rewrite {
            " + a second BGREWRITEAOF"
        } else {
            ""
        },
        lost,
        wrong.first(),
        heap_files,
        released.unwrap_or(files_before_second),
        cold_keys
    );
    if let Some(after) = released {
        assert!(
            after < files_before_second,
            "the second rewrite committed but the sweep released no held spill file \
             ({files_before_second} before, {after} after)"
        );
    }
}

/// moon#1231: read-promoted probes survive the orphan sweep and a kill -9.
#[test]
#[ignore]
fn promoted_cold_keys_survive_the_orphan_sweep_and_crash() {
    run_promote_scenario("promote-get", Touch::Get, false);
}

/// moon#1231: an APPEND logged after the promotion replays onto the value
/// the spill file still holds, not onto nothing.
#[test]
#[ignore]
fn appended_cold_keys_keep_their_value_across_the_orphan_sweep_and_crash() {
    run_promote_scenario("promote-append", Touch::Append, false);
}

/// moon#1231: the files the sweep held are released once a later rewrite has
/// captured the promoted probes, and a kill -9 after that still loses nothing.
#[test]
#[ignore]
fn held_spill_files_are_released_after_the_next_rewrite() {
    run_promote_scenario("promote-release", Touch::Get, true);
}

// ── WS20: keyspace-level operations on cold keys ─────────────────────────────
//
// moon#1254 (MOVE / COPY … DB n of a cold key), moon#1236 (a cold key
// overwritten hot WITH a TTL) and moon#1237 (SWAPDB with cold keys). Same
// probes / filler / stop / restart shape as the cases above. The
// `--appendonly no` cases (moon#1260, moon#1236's no-AOF quadrant) are in
// `crash_recovery_cold_no_aof.rs`.

/// Stop round 1 as `stop` says and bring the server back on the same data.
fn stop_and_restart(
    port: u16,
    dir: &std::path::Path,
    server: &mut common::ServerGuard,
    stop: Stop,
) -> common::ServerGuard {
    if stop == Stop::Shutdown {
        let _ = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "SHUTDOWN"])
            .output();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    server.kill_now();
    wait_for_port_down(port);
    start_moon_alive(port, dir, stop.sweep_secs())
}

// ── moon#1254 ────────────────────────────────────────────────────────────────

/// Even probes are MOVEd to db 1, odd probes COPYed to db 2 (`COPY k k DB 2`),
/// while most of them are cold. Every even probe must then live in db 1 only,
/// every odd one in db 0 and db 2 — on the live server, and after an optional
/// BGREWRITEAOF and a restart.
fn run_move_copy_scenario(suffix: &str, stop: Stop, rewrite: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);
    let heap_files = spill_probes(port, &dir);
    let val = probe_value();

    let mut refused = 0usize;
    for i in 0..PROBE_COUNT {
        let key = probe_key(i);
        let reply = if i % 2 == 0 {
            redis_cmd(port, &["MOVE", &key, "1"])
        } else {
            redis_cmd(port, &["COPY", &key, &key, "DB", "2"])
        };
        if integer_reply(&reply) != Some(1) {
            refused += 1;
        }
    }
    let check = |phase: &str| -> Vec<String> {
        let mut wrong = Vec::new();
        for i in 0..PROBE_COUNT {
            let key = probe_key(i);
            let (want0, want1, want2) = if i % 2 == 0 {
                (None, Some(val.as_str()), None)
            } else {
                (Some(val.as_str()), None, Some(val.as_str()))
            };
            let got = (
                redis_get_db(port, 0, &key),
                redis_get_db(port, 1, &key),
                redis_get_db(port, 2, &key),
            );
            if (got.0.as_deref(), got.1.as_deref(), got.2.as_deref()) != (want0, want1, want2) {
                wrong.push(format!(
                    "{phase} {key}: db0 {} db1 {} db2 {}",
                    got.0.is_some(),
                    got.1.is_some(),
                    got.2.is_some()
                ));
            }
        }
        wrong
    };
    let live_wrong = check("live");
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));
    let mut server2 = stop_and_restart(port, &dir, &mut server, stop);
    let mut wrong = live_wrong;
    wrong.extend(check("recovered"));
    server2.kill_now();
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty() && refused == 0,
        "MOVE/COPY … DB of cold probes: {refused} of {PROBE_COUNT} not answered :1, {} probe \
         states wrong (live + after {}restart; heap files at the move: {heap_files}); first: {:?}",
        wrong.len(),
        if rewrite { "BGREWRITEAOF + " } else { "" },
        wrong.first()
    );
}

/// moon#1254: MOVE / COPY … DB of spilled keys, then BGREWRITEAOF + kill -9.
#[test]
#[ignore]
fn moved_and_copied_cold_keys_keep_their_values_across_rewrite_and_crash() {
    run_move_copy_scenario("move-rw", Stop::Kill9, true);
}

/// moon#1254: the same without a rewrite (the MOVE records replay).
#[test]
#[ignore]
fn moved_and_copied_cold_keys_keep_their_values_across_crash() {
    run_move_copy_scenario("move-norw", Stop::Kill9, false);
}

// ── moon#1236 ────────────────────────────────────────────────────────────────

/// The even probes are overwritten hot WITH a TTL while cold (their old
/// value stays on disk as a shadow). Round 1 ends before the TTL passes; the
/// restart comes after it. Each even probe must read absent — never its OLD,
/// pre-overwrite value — and each odd probe its original value.
fn run_ttl_overwrite_scenario(suffix: &str, stop: Stop, rewrite: bool) {
    const TTL_MS: u64 = 20_000;
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);
    let heap_files = spill_probes(port, &dir);
    let val = probe_value();
    let new = overwrite_value();
    let overwritten_at = Instant::now();
    for i in (0..PROBE_COUNT).filter(|i| i % 2 == 0) {
        redis_cmd(
            port,
            &["SET", &probe_key(i), &new, "PX", &TTL_MS.to_string()],
        );
    }
    assert_eq!(
        redis_get(port, &probe_key(0)).as_deref(),
        Some(new.as_str()),
        "precondition failed: the overwrite is not visible on the live server"
    );
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));
    assert!(
        overwritten_at.elapsed() < Duration::from_millis(TTL_MS),
        "the TTL passed before round 1 ended; the case would not test the restart"
    );
    // Stop now; restart only once the TTL has passed (plus a margin).
    if stop == Stop::Shutdown {
        let _ = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "SHUTDOWN"])
            .output();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    server.kill_now();
    wait_for_port_down(port);
    let wait_until = Duration::from_millis(TTL_MS + 1_500);
    if let Some(rest) = wait_until.checked_sub(overwritten_at.elapsed()) {
        std::thread::sleep(rest);
    }
    let mut server2 = start_moon_alive(port, &dir, stop.sweep_secs());
    let mut wrong = Vec::new();
    let mut old_back = 0usize;
    for i in 0..PROBE_COUNT {
        let key = probe_key(i);
        let got = redis_get(port, &key);
        let ok = if i % 2 == 0 {
            if got.as_deref() == Some(val.as_str()) {
                old_back += 1;
            }
            got.is_none()
        } else {
            got.as_deref() == Some(val.as_str())
        };
        if !ok {
            wrong.push(key);
        }
    }
    server2.kill_now();
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty(),
        "{} probe(s) wrong after a TTL'd overwrite of cold keys and a {}restart past the TTL \
         ({old_back} even probes came back with their OLD value; heap files: {heap_files}); \
         first: {:?}",
        wrong.len(),
        if rewrite { "BGREWRITEAOF + " } else { "" },
        wrong.first()
    );
}

/// moon#1236: the reported shape — the rewrite puts the TTL'd value in the
/// base, the restart comes after the TTL.
#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_rewrite_and_crash() {
    run_ttl_overwrite_scenario("ttlow-rw", Stop::Kill9, true);
}

#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_rewrite_and_clean_restart() {
    run_ttl_overwrite_scenario("ttlow-rw-shutdown", Stop::Shutdown, true);
}

#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_crash() {
    run_ttl_overwrite_scenario("ttlow-norw", Stop::Kill9, false);
}

#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_clean_restart() {
    run_ttl_overwrite_scenario("ttlow-norw-shutdown", Stop::Shutdown, false);
}

// ── moon#1237 ────────────────────────────────────────────────────────────────

/// `redis-cli <args>` whose reply may legitimately be an error (returned as
/// text, not asserted).
fn redis_cmd_unchecked(port: u16, args: &[&str]) -> String {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Which SWAPDB shape a case runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SwapShape {
    /// The report: the probes are cold in db 0, then `SWAPDB 0 1`.
    ColdThenSwap,
    /// Found by WS20: `SWAPDB 0 1` while both dbs are empty, then the probes
    /// are written and spilled in db 1 (the post-swap db).
    SwapThenCold,
}

/// SWAPDB 0 1 around a cold tier, DEL of the even probes in the probes' db,
/// optional BGREWRITEAOF, kill -9, restart. A SWAPDB refused because a db
/// holds cold keys is an accepted answer (the probes then stay in db 0);
/// either way every odd probe must be in exactly its db and no deleted
/// probe may come back anywhere.
fn run_swapdb_scenario(suffix: &str, shape: SwapShape, rewrite: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let stop = Stop::Kill9;
    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);
    let val = probe_value();
    let (home, swap_reply, heap_files) = match shape {
        SwapShape::ColdThenSwap => {
            let heap_files = spill_probes(port, &dir);
            let reply = redis_cmd_unchecked(port, &["SWAPDB", "0", "1"]);
            let home = if reply == "OK" { 1 } else { 0 };
            (home, reply, heap_files)
        }
        SwapShape::SwapThenCold => {
            let reply = redis_cmd_unchecked(port, &["SWAPDB", "0", "1"]);
            assert_eq!(reply, "OK", "SWAPDB of two EMPTY dbs must succeed");
            for i in 0..PROBE_COUNT {
                redis_cmd_db(port, 1, &["SET", &probe_key(i), &val]);
            }
            write_filler_in_db(port, 1);
            std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
            let heap_files = count_heap_files(&dir);
            assert!(heap_files > 0, "precondition failed: nothing spilled");
            (1, reply, heap_files)
        }
    };
    for i in (0..PROBE_COUNT).filter(|i| i % 2 == 0) {
        redis_cmd_db(port, home, &["DEL", &probe_key(i)]);
    }
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));
    let mut server2 = stop_and_restart(port, &dir, &mut server, stop);
    let other = 1 - home;
    let mut wrong = Vec::new();
    for i in 0..PROBE_COUNT {
        let key = probe_key(i);
        let in_home = redis_get_db(port, home, &key);
        let in_other = redis_get_db(port, other, &key);
        let ok = if i % 2 == 0 {
            in_home.is_none() && in_other.is_none()
        } else {
            in_home.as_deref() == Some(val.as_str()) && in_other.is_none()
        };
        if !ok {
            wrong.push(format!(
                "{key}: db{home} {} db{other} {}",
                in_home.is_some(),
                in_other.is_some()
            ));
        }
    }
    server2.kill_now();
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty(),
        "{} probe(s) in the wrong db or back from the dead after SWAPDB ({:?}), DEL, {}kill -9 \
         and restart (home db {home}; heap files {heap_files}); first: {:?}",
        wrong.len(),
        swap_reply,
        if rewrite { "BGREWRITEAOF, " } else { "" },
        wrong.first()
    );
}

/// moon#1237 as reported: cold keys in db 0, SWAPDB 0 1, DEL, rewrite, crash.
#[test]
#[ignore]
fn swapdb_of_cold_keys_keeps_every_key_in_its_db_across_rewrite_and_crash() {
    run_swapdb_scenario("swap-cold-rw", SwapShape::ColdThenSwap, true);
}

/// The same without the rewrite (the SWAPDB record replays).
#[test]
#[ignore]
fn swapdb_of_cold_keys_keeps_every_key_in_its_db_across_crash() {
    run_swapdb_scenario("swap-cold-norw", SwapShape::ColdThenSwap, false);
}

/// SWAPDB of empty dbs, then spills in the swapped db, then a crash with no
/// rewrite: the replayed SWAPDB must not carry the later spills' cold entries
/// into the other db.
#[test]
#[ignore]
fn keys_spilled_after_a_swapdb_stay_in_their_db_across_crash() {
    run_swapdb_scenario("swap-then-cold-norw", SwapShape::SwapThenCold, false);
}

#[test]
#[ignore]
fn keys_spilled_after_a_swapdb_stay_in_their_db_across_rewrite_and_crash() {
    run_swapdb_scenario("swap-then-cold-rw", SwapShape::SwapThenCold, true);
}
