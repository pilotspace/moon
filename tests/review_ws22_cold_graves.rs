//! Adversarial review of moon#1281 (WS22 review 1). Black-box, real server.
//!
//! 1. `review_no_aof_snapshot_then_appendonly_yes_restart_keeps_cold_dels`
//!    (RED on 5b5593d): the cold-graves trailer is applied at boot only when
//!    the BOOTING process has no AOF writer (`snapshot_hold::applies()`), not
//!    when the LOADED snapshot is the KV authority. A dir written by a no-AOF
//!    process (snapshot with a trailer + spill files) booted with
//!    `--appendonly yes` and no AOF yet takes `KvSources::SnapshotAndLogs`:
//!    the snapshot's hot keys load, its graves are thrown away, and every
//!    cold key deleted before that snapshot is back. The copy booted with
//!    `--appendonly no` is the control (same bytes, one flag).
//! 2. `review_crash_matrix_during_sweep_unlinks_nothing` (GREEN, evidence):
//!    the matrix's `DuringSweep` point claims to cover "the post-save sweep,
//!    its unlinks and manifest commit", but DELeting only the EVEN probes
//!    leaves every spill file with live neighbours: nothing goes zero-ref,
//!    nothing is unlinked, no tombstone is committed.
//! 3. `review_no_aof_graves_never_drop_a_live_respilled_key` (GREEN, guard):
//!    the live-key-loss direction. Probes whose original slot became a grave
//!    (read-promoted, or overwritten) and that were then re-spilled by the
//!    no-AOF durable eviction must all come back with their CURRENT value.
//!
//!   MOON_BIN=... cargo test --test review_ws22_cold_graves -- --ignored --test-threads 1

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

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create copy dir");
    for e in std::fs::read_dir(from).expect("read dir") {
        let e = e.expect("dir entry");
        let dst = to.join(e.file_name());
        if e.file_type().expect("file type").is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), &dst).expect("copy file");
        }
    }
}

/// Pipelined `SET {prefix}:{i}` of filler-sized values, every reply read.
fn write_new_keys(port: u16, prefix: &str, count: usize) {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
    let val = "G".repeat(FILLER_VALUE_LEN);
    let mut buf = Vec::with_capacity(64 * 1024);
    for i in 0..count {
        let key = format!("{prefix}:{i}");
        buf.extend_from_slice(
            format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, val.len(), val)
                .as_bytes(),
        );
        if buf.len() >= 64 * 1024 {
            stream.write_all(&buf).expect("write");
            buf.clear();
        }
    }
    stream.write_all(&buf).expect("write tail");
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut replies = 0usize;
    let mut chunk = [0u8; 64 * 1024];
    while replies < count {
        let n = stream.read(&mut chunk).expect("replies");
        assert!(n > 0, "server closed after {replies} replies");
        replies += chunk[..n].iter().filter(|&&b| b == b'\n').count();
    }
}

fn probes_back(port: u16) -> usize {
    (0..PROBE_COUNT)
        .filter(|i| redis_get(port, &probe_key(*i)).is_some())
        .count()
}

#[test]
#[ignore]
fn review_no_aof_snapshot_then_appendonly_yes_restart_keeps_cold_dels() {
    let port = common::reserve_port();
    let dir = unique_dir("rv22-no2yes");
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_probes(port, &dir);

    // Phase 2, no AOF: DEL every (cold) probe, BGSAVE — the snapshot's
    // trailer names every deleted slot — kill -9.
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

    // Control: the same bytes booted with `--appendonly no`.
    let control_dir = unique_dir("rv22-no2yes-control");
    copy_dir(&dir, &control_dir);
    let mut control = start_moon_alive_with(port, &control_dir, 3600, "no", &SAVE);
    let control_back = probes_back(port);
    let control_hot = redis_get(port, "hot:control");
    control.kill_now();
    wait_for_port_down(port);

    // Phase 3: the operator turns the AOF on for the next boot.
    let mut server3 = start_moon_alive_with(port, &dir, 3600, "yes", &SAVE);
    let back = probes_back(port);
    let hot = redis_get(port, "hot:control");
    server3.kill_now();
    wait_for_port_down(port);

    let ok = back == 0 && control_back == 0;
    let keep: Vec<String> = if ok { Vec::new() } else { vec!["keep".into()] };
    finish(&dir, &keep);
    finish(&control_dir, &keep);
    assert_eq!(
        control_hot.as_deref(),
        Some("H"),
        "control: the snapshot loads under --appendonly no"
    );
    assert_eq!(
        control_back, 0,
        "control: --appendonly no must keep all {deleted} deleted cold probes deleted"
    );
    assert_eq!(
        hot.as_deref(),
        Some("H"),
        "precondition: --appendonly yes with no AOF loads the snapshot (SnapshotAndLogs)"
    );
    assert_eq!(
        back, 0,
        "moon#1281 review: {back} of {deleted} cold probes DELeted before a successful \
         no-AOF snapshot came back when the same dir was booted with --appendonly yes: the \
         snapshot's hot keys load, its cold-graves trailer is ignored (applied only when \
         the BOOTING process has no AOF writer)"
    );
}

#[test]
#[ignore]
fn review_crash_matrix_during_sweep_unlinks_nothing() {
    let port = common::reserve_port();
    let dir = unique_dir("rv22-sweep");
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_probes(port, &dir);
    let mut server = start_moon_alive_with(port, &dir, 1, "no", &SAVE);
    let files_before = count_heap_files(&dir);
    let evens: Vec<String> = (0..PROBE_COUNT).step_by(2).map(probe_key).collect();
    let mut del: Vec<&str> = vec!["DEL"];
    del.extend(evens.iter().map(String::as_str));
    redis_cmd(port, &del);
    bgsave_and_wait(port);
    // The matrix's `DuringSweep` window is 0-2.5 s; give the sweep 5 s.
    std::thread::sleep(Duration::from_secs(5));
    let files_after = count_heap_files(&dir);
    let pending = info_u64(port, "cold_files_pending_unlink").unwrap_or(u64::MAX);
    server.kill_now();
    wait_for_port_down(port);
    finish(&dir, &[]);
    eprintln!(
        "heap files before {files_before}, after DEL evens + BGSAVE + 5 s {files_after}, \
         cold_files_pending_unlink {pending}"
    );
    assert!(files_before > 0);
    assert_eq!(
        (files_after, pending),
        (files_before, 0),
        "the DuringSweep point unlinked or queued something after all"
    );
}

#[test]
#[ignore]
fn review_no_aof_graves_never_drop_a_live_respilled_key() {
    let port = common::reserve_port();
    let dir = unique_dir("rv22-respill");
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_probes(port, &dir);
    let mut server = start_moon_with(port, &dir, 1, "no", &SAVE);
    wait_for_port(port);
    assert!(
        wait_until(20, || info_u64(port, "cold_keys").unwrap_or(0) > 0),
        "precondition: nothing cold inherited"
    );
    let val = probe_value();
    let new = overwrite_value();
    // Odd: read-promote (the old slot becomes a grave). Even: overwrite (the
    // old slot becomes a grave). Then re-spill them all through the no-AOF
    // durable eviction by writing the fillers again.
    for i in 0..PROBE_COUNT {
        if i % 2 == 1 {
            let _ = redis_get(port, &probe_key(i));
        } else {
            redis_set(port, &probe_key(i), &new);
        }
    }
    let files_mid = count_heap_files(&dir);
    // NEW keys (not the inherited fillers): the inherited files keep their
    // live fillers, so the probes' old slots stay on disk as graves the
    // snapshot must carry — and the boot must apply — while the probes live
    // in the new files.
    write_new_keys(port, "fill2", FILLER_COUNT);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
    let files_respilled = count_heap_files(&dir);
    bgsave_and_wait(port);
    std::thread::sleep(Duration::from_secs(2));
    // What exists at the kill (EXISTS does not promote a cold key), and how
    // many keys allkeys-lru plain-dropped (those are legitimately gone).
    let present: Vec<bool> = (0..PROBE_COUNT)
        .map(|i| integer_reply(&redis_cmd(port, &["EXISTS", &probe_key(i)])) == Some(1))
        .collect();
    let evicted = info_u64(port, "evicted_keys").unwrap_or(0);
    let dbsize_at_kill = integer_reply(&redis_cmd(port, &["DBSIZE"])).unwrap_or(-1);
    server.kill_now();
    wait_for_port_down(port);

    let mut server2 = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let dbsize_after = integer_reply(&redis_cmd(port, &["DBSIZE"])).unwrap_or(-1);
    eprintln!(
        "evicted_keys {evicted}; probes present at kill {}; DBSIZE {dbsize_at_kill} at kill, \
         {dbsize_after} after",
        present.iter().filter(|p| **p).count()
    );
    let mut wrong = Vec::new();
    if dbsize_after != dbsize_at_kill {
        wrong.push(format!("DBSIZE {dbsize_at_kill} -> {dbsize_after}"));
    }
    for i in 0..PROBE_COUNT {
        if !present[i] {
            continue;
        }
        let want = if i % 2 == 1 { &val } else { &new };
        let got = redis_get(port, &probe_key(i));
        if got.as_deref() != Some(want.as_str()) {
            wrong.push(format!(
                "{} = {:?}",
                probe_key(i),
                got.map(|g| g.chars().take(8).collect::<String>())
            ));
        }
    }
    let tombstoned_log = std::fs::read_to_string(dir.join("moon.stdout.log"))
        .unwrap_or_default()
        .lines()
        .filter(|l| l.contains("moon#1281"))
        .count();
    server2.kill_now();
    wait_for_port_down(port);
    finish(&dir, &wrong);
    eprintln!(
        "heap files: {files_mid} before the re-spill, {files_respilled} after; \
         boot lines naming moon#1281: {tombstoned_log}"
    );
    assert!(
        files_respilled > files_mid,
        "precondition: the probes were not re-spilled"
    );
    assert!(
        wrong.is_empty(),
        "live keys lost or wrong after grave + re-spill + BGSAVE + kill -9: {wrong:?}"
    );
}
