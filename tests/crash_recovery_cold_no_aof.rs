//! Cold keys under `--appendonly no`: the durable state after a crash is the
//! shard's last snapshot (the hot keyspace) plus every spill file the
//! manifest lists. Without an AOF a server spills rarely (the connection
//! gate drops instead), so every case first INHERITS cold probes from an
//! `--appendonly yes` run (stopped cleanly, its AOF removed), then runs the
//! same directory without an AOF.
//!
//! - moon#1260: a cold key read back into RAM (no log records it) must
//!   survive the orphan sweep and a kill -9 — its spill file is its only
//!   durable copy until a snapshot captures it (review F2: with or without
//!   save points).
//! - moon#1260 review F1: FLUSHALL + a successful snapshot + kill -9 brings
//!   nothing back.
//! - moon#1236 review F6: a cold key overwritten with a TTL, snapshotted,
//!   restarted after the TTL, must not come back OLD from its spill slot.
//!
//! Split out of `crash_recovery_cold_del_rewrite.rs`; same harness
//! (`crash_recovery_cold_support/`), same knobs:
//!   MOON_BIN=target/release/moon cargo test --release \
//!     --test crash_recovery_cold_no_aof -- --ignored --test-threads 1
//! (`MOON_TEST_COLD_DEL_SHARDS=1` for the single-shard layouts.)
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::io::Write;
use std::process::Command;
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

// ── moon#1260 ────────────────────────────────────────────────────────────────

/// BGSAVE and wait until it completed successfully (LASTSAVE has 1 s
/// resolution, so the wait starts one second after the previous value).
fn bgsave_and_wait(port: u16) {
    let lastsave = || integer_reply(&redis_cmd(port, &["LASTSAVE"])).unwrap_or(0);
    let before = lastsave();
    std::thread::sleep(Duration::from_millis(1_100));
    let reply = redis_cmd(port, &["BGSAVE"]);
    assert!(!reply.is_empty(), "BGSAVE returned an empty reply");
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
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// `GET probe:i` for every probe; returns how many read `val`.
fn probes_readable(port: u16, val: &str) -> usize {
    (0..PROBE_COUNT)
        .filter(|i| redis_get(port, &probe_key(*i)).as_deref() == Some(val))
        .count()
}

/// The REVIEW4-WS19 `r4_no_aof_promoted_…` proof, at `MOON_TEST_COLD_DEL_SHARDS`.
/// Phase 1 spills the probes under `--appendonly yes` and stops cleanly (under
/// `--appendonly no` the connection gate drops instead of spilling, so a
/// no-AOF server holds cold keys it inherited). Phase 2 runs the same dir
/// with `--appendonly no --save`: BGSAVE (the hot table), DEL the fillers,
/// GET every probe (read back into RAM, no log exists), let the orphan
/// sweep run (`sweep_secs`), kill -9. Snapshot semantics: every probe was
/// unchanged since the save, so every probe must come back.
fn run_no_aof_promote_scenario(suffix: &str, sweep_secs: u64, save_points: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    // Phase 1 (`--appendonly yes`), clean stop, its AOF removed.
    inherit_cold_probes(port, &dir);
    // Phase 2 (`--appendonly no`, with save points and a BGSAVE, or with no
    // `--save` at all and no save: REVIEW-WS20 F2).
    let save: &[&str] = if save_points {
        &["--save", "3600 100000000"]
    } else {
        &[]
    };
    let mut server = start_moon_with(port, &dir, sweep_secs, "no", save);
    wait_for_port(port);
    if save_points {
        redis_set(port, "hot:control", "H");
        bgsave_and_wait(port);
    }
    let at_save = count_heap_files(&dir);
    assert!(
        del_fillers_counting(port) > 0,
        "precondition failed: no inherited filler to delete"
    );
    let val = probe_value();
    let readable = probes_readable(port, &val);
    assert!(
        readable > 0,
        "precondition failed: no probe readable in phase 2"
    );
    std::thread::sleep(Duration::from_secs(6)); // several orphan sweeps
    let before_kill = count_heap_files(&dir);
    server.kill_now();
    wait_for_port_down(port);
    let mut server2 = start_moon_alive_with(port, &dir, sweep_secs, "no", save);
    let hot_ok = !save_points || redis_get(port, "hot:control").as_deref() == Some("H");
    let back = probes_readable(port, &val);
    server2.kill_now();
    let wrong: Vec<String> = if back < readable || !hot_ok {
        vec![format!("{back} of {readable} back, hot control {hot_ok}")]
    } else {
        Vec::new()
    };
    finish(&dir, &wrong);
    assert!(hot_ok, "the snapshot was not loaded (hot control missing)");
    assert_eq!(
        readable - back.min(readable),
        0,
        "probes unchanged since the last save (or boot) lost after GET, orphan sweeps and kill -9 \
         under --appendonly no ({readable} readable before, {back} after; heap files {at_save} \
         at the save, {before_kill} before the kill)"
    );
}

/// moon#1260: GET-promoted inherited cold keys survive the sweep and a crash.
#[test]
#[ignore]
fn no_aof_promoted_cold_keys_survive_the_orphan_sweep_and_crash() {
    run_no_aof_promote_scenario("noaof-sweep", 1, true);
}

/// REVIEW-WS20 F2: the same with `--appendonly no` and NO save points (the
/// hold applies there too since moon#1267; main unlinked the files).
#[test]
#[ignore]
fn no_aof_no_save_promoted_cold_keys_survive_the_orphan_sweep_and_crash() {
    run_no_aof_promote_scenario("noaof-nosave-sweep", 1, false);
}

/// moon#1260 control: no sweep in the window, nothing is lost.
#[test]
#[ignore]
fn no_aof_promoted_cold_keys_survive_a_crash_without_a_sweep() {
    run_no_aof_promote_scenario("noaof-nosweep", 3600, true);
}

/// Phase 1 of the no-AOF cases: spill the probes under `--appendonly yes`
/// and stop cleanly (without an AOF the connection gate drops instead of
/// spilling, so a no-AOF server's cold keys are inherited ones).
fn inherit_cold_probes(port: u16, dir: &std::path::Path) {
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
    drop_phase1_aof(dir);
}

/// Remove phase 1's AOF (the tokio `--shards 1` flat `appendonly.aof`, the
/// manifest layout's `appendonlydir`): the no-AOF phase must inherit the cold
/// tier ONLY. A tokio `--shards 1` boot with `--appendonly no` still replays
/// a leftover `appendonly.aof` (a separate defect, WS20 review NOTES), which
/// rebuilt every probe hot and so masked the cold-tier behaviour under test
/// on that layout.
fn drop_phase1_aof(dir: &std::path::Path) {
    let _ = std::fs::remove_file(dir.join("appendonly.aof"));
    let _ = std::fs::remove_dir_all(dir.join("appendonlydir"));
}

/// Poll `cond` every 200 ms until it holds or `secs` pass; whether it held.
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

/// How the flushed keys' snapshot is taken in [`run_no_aof_flush_scenario`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum FlushSave {
    /// `--save "3600 1"`: FLUSHALL saves before it replies (moon#1264).
    FlushallSave,
    /// `--save ""`: FLUSHALL, then an explicit BGSAVE.
    FlushThenBgsave,
}

/// moon#1260 review F1 (`rv20_noaof_flush_resurrect.py`): without an AOF,
/// FLUSHALL of inherited cold keys followed by a successful snapshot, then
/// a kill -9 once the sweep had time to reclaim the files: nothing may come
/// back — the last snapshot is empty and was taken after the flush. The
/// hold stamped the flushed files at the sweep AFTER the save and kept them
/// until a second snapshot, so the restart rebuilt them.
fn run_no_aof_flush_scenario(suffix: &str, how: FlushSave) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_probes(port, &dir);
    let save: [&str; 2] = match how {
        FlushSave::FlushallSave => ["--save", "3600 1"],
        FlushSave::FlushThenBgsave => ["--save", ""],
    };
    let sweep_secs = 1;
    let mut server = start_moon_with(port, &dir, sweep_secs, "no", &save);
    wait_for_port(port);
    let before = integer_reply(&redis_cmd(port, &["DBSIZE"])).unwrap_or(0);
    assert!(before > 0, "precondition failed: nothing inherited");
    let at_flush = count_heap_files(&dir);
    match how {
        FlushSave::FlushallSave => {
            redis_cmd(port, &["FLUSHALL"]);
        }
        // The BGSAVE must start before the next orphan sweep sees the
        // flushed files: both commands on one connection, back to back.
        FlushSave::FlushThenBgsave => flushall_then_bgsave(port),
    }
    // Every file backed only flushed keys: the sweeps reclaim all of them.
    let reclaimed = wait_until(20, || count_heap_files(&dir) == 0);
    let before_kill = count_heap_files(&dir);
    server.kill_now();
    wait_for_port_down(port);
    let mut server2 = start_moon_alive_with(port, &dir, 3600, "no", &save);
    let dbsize = integer_reply(&redis_cmd(port, &["DBSIZE"])).unwrap_or(-1);
    let back = probes_readable(port, &probe_value());
    server2.kill_now();
    let wrong: Vec<String> = if back > 0 || dbsize != 0 {
        vec![format!("{back} probes back, DBSIZE {dbsize}")]
    } else {
        Vec::new()
    };
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty(),
        "FLUSHALL + a successful snapshot + kill -9 under --appendonly no brought back \
         {back} of {PROBE_COUNT} probes (DBSIZE {dbsize}; {before} keys before the flush; \
         heap files {at_flush} at the flush, {before_kill} before the kill, all reclaimed \
         within 20 s: {reclaimed})"
    );
}

/// `FLUSHALL` and `BGSAVE` pipelined on one connection, then wait until that
/// save completed successfully.
fn flushall_then_bgsave(port: u16) {
    use std::io::Read;
    let lastsave = || integer_reply(&redis_cmd(port, &["LASTSAVE"])).unwrap_or(0);
    let before = lastsave();
    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect for FLUSHALL");
    stream
        .write_all(b"*1\r\n$8\r\nFLUSHALL\r\n*1\r\n$6\r\nBGSAVE\r\n")
        .expect("FLUSHALL + BGSAVE");
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    let mut got = Vec::new();
    let mut chunk = [0u8; 512];
    while got.iter().filter(|&&b| b == b'\n').count() < 2 {
        let n = stream.read(&mut chunk).expect("FLUSHALL + BGSAVE replies");
        assert!(
            n > 0,
            "server closed after {:?}",
            String::from_utf8_lossy(&got)
        );
        got.extend_from_slice(&chunk[..n]);
    }
    let replies = String::from_utf8_lossy(&got);
    assert!(
        replies.starts_with("+OK") && replies.contains("\r\n+"),
        "FLUSHALL + BGSAVE answered {replies:?}"
    );
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
        // LASTSAVE has 1 s resolution: a save finished in the same second as
        // the previous one is only visible once the status says so.
        if info.contains("rdb_bgsave_in_progress:0")
            && info.contains("rdb_last_bgsave_status:ok")
            && Instant::now() + Duration::from_secs(58) > deadline
        {
            return;
        }
        assert!(Instant::now() < deadline, "BGSAVE never completed: {info}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// moon#1260 review F1: FLUSHALL with save points saves before it replies.
#[test]
#[ignore]
fn no_aof_flushed_cold_keys_stay_flushed_after_the_flushall_save_and_crash() {
    run_no_aof_flush_scenario("noaof-flush-save", FlushSave::FlushallSave);
}

/// moon#1260 review F1: FLUSHALL without save points, then a BGSAVE.
#[test]
#[ignore]
fn no_aof_flushed_cold_keys_stay_flushed_after_a_bgsave_and_crash() {
    run_no_aof_flush_scenario("noaof-flush-bgsave", FlushSave::FlushThenBgsave);
}

// ── moon#1236, the no-AOF quadrant (review F6) ────────────────────────────────

/// moon#1236 without an AOF (`probe1236_noaof.py`): the even probes, cold
/// (inherited), are overwritten hot WITH a TTL, a BGSAVE captures the new
/// values, kill -9, and the restart comes after the TTL. The snapshot is the
/// KV authority: its loader skips the expired entries, and the cold index
/// attached after it surfaced every even probe's OLD spill slot. Each even
/// probe must read absent; the odd probes (the cold tier's control) keep
/// their value.
fn run_no_aof_ttl_overwrite_scenario(suffix: &str) {
    const TTL_MS: u64 = 15_000;
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_probes(port, &dir);
    let save = ["--save", "3600 100000000"];
    let mut server = start_moon_with(port, &dir, 3600, "no", &save);
    wait_for_port(port);
    let val = probe_value();
    let odd_before = (1..PROBE_COUNT)
        .step_by(2)
        .filter(|i| redis_get(port, &probe_key(*i)).as_deref() == Some(val.as_str()))
        .count();
    assert!(odd_before > 0, "precondition failed: nothing inherited");
    let new = overwrite_value();
    let overwritten_at = Instant::now();
    for i in (0..PROBE_COUNT).step_by(2) {
        redis_cmd(
            port,
            &["SET", &probe_key(i), &new, "PX", &TTL_MS.to_string()],
        );
    }
    bgsave_and_wait(port);
    let saved_after = overwritten_at.elapsed();
    server.kill_now();
    wait_for_port_down(port);
    if let Some(rest) = Duration::from_millis(TTL_MS + 1_500).checked_sub(overwritten_at.elapsed())
    {
        std::thread::sleep(rest);
    }
    let mut server2 = start_moon_alive_with(port, &dir, 3600, "no", &save);
    let even_back: Vec<(usize, String)> = (0..PROBE_COUNT)
        .step_by(2)
        .filter_map(|i| redis_get(port, &probe_key(i)).map(|v| (i, v)))
        .collect();
    let old_back = even_back.iter().filter(|(_, v)| *v == val).count();
    let odd_after = (1..PROBE_COUNT)
        .step_by(2)
        .filter(|i| redis_get(port, &probe_key(*i)).as_deref() == Some(val.as_str()))
        .count();
    server2.kill_now();
    let wrong: Vec<String> = even_back
        .iter()
        .map(|(i, _)| probe_key(*i))
        .chain((odd_after < odd_before).then(|| format!("{odd_after}/{odd_before} odd")))
        .collect();
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty(),
        "--appendonly no: a TTL'd overwrite of inherited cold keys, a BGSAVE {saved_after:?} \
         after it, kill -9 and a restart past the {TTL_MS} ms TTL brought {} even probe(s) \
         back ({old_back} with the OLD value); odd probes {odd_after} of {odd_before}",
        even_back.len()
    );
}

/// moon#1236 review F6: the snapshot boot must not surface a cold shadow of
/// a key it holds as expired.
#[test]
#[ignore]
fn no_aof_cold_keys_overwritten_with_a_ttl_do_not_come_back_old_after_a_bgsave_and_crash() {
    run_no_aof_ttl_overwrite_scenario("noaof-ttlow");
}
