//! moon#1293: a crash while a boot creates a fresh AOF generation must not
//! leave a generation without its `MOON.COLDCUT` + cold-`DEL` head.
//!
//! The `--appendonly no` -> `yes` switch (`tests/review_r2a_cold_graves.rs`):
//! the no-AOF process deleted cold keys and saved a snapshot whose trailer
//! carries their graves. The first `--appendonly yes` boot loads that
//! snapshot, applies the graves, and creates generation 1, whose head must
//! carry a `DEL` for every buried key: from the next boot on the manifest is
//! the KV authority and the snapshot (with its graves) is skipped.
//!
//! Before the fix the manifest was committed FIRST and the heads appended
//! after it, one shard at a time. A crash in between left a committed
//! generation with an empty (or, at `--shards 4`, some empty) incr: the next
//! boot replayed it ungated and re-indexed every dead slot, so the deleted
//! cold keys came back.
//!
//! Boot A runs with `MOON_TEST_AOF_INIT_CRASH=<point>` and stops itself
//! there (exit 86). A binary without the hook (older than this test) serves
//! instead; the test then SIGKILLs it and rebuilds the directory a crash at
//! that point leaves in the pre-fix order (manifest committed, the incrs of
//! the shards whose head was not yet written truncated to their pre-head
//! length, 0 bytes) — the single-variable image of the same crash. A layout
//! without a manifest (tokio `--shards 1`, legacy `appendonly.aof`) has no
//! such window: the test still checks boots B and C.
//!
//! Boot B (no hook) and boot C (after a SIGKILL of B) must both read every
//! deleted probe as absent and keep `hot:control` and B's acknowledged write.
//!
//!   MOON_BIN=... [MOON_TEST_COLD_DEL_SHARDS=1] cargo test --test crash_aof_init_generation_1293 -- --include-ignored --test-threads 1 --nocapture

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

const SAVE: [&str; 2] = ["--save", "3600 100000000"];

/// `src/persistence/aof_manifest/test_hooks.rs::INIT_CRASH_EXIT_CODE`.
const INIT_CRASH_EXIT_CODE: i32 = 86;

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

/// Every generation-1 incr file, with the shard it belongs to (0 for the
/// TopLevel layout).
fn generation_one_incrs(dir: &Path) -> Vec<(u16, PathBuf)> {
    let aof = dir.join("appendonlydir");
    let mut out = Vec::new();
    let top = aof.join("moon.aof.1.incr.aof");
    if top.exists() {
        out.push((0, top));
    }
    if let Ok(entries) = std::fs::read_dir(&aof) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(sid) = name
                .strip_prefix("shard-")
                .and_then(|n| n.parse::<u16>().ok())
            {
                let incr = e.path().join("moon.aof.1.incr.aof");
                if incr.exists() {
                    out.push((sid, incr));
                }
            }
        }
    }
    out
}

#[derive(Debug)]
enum BootA {
    /// The hook stopped the process at the crash point.
    Hooked,
    /// No hook (older binary): the pre-fix crash image was rebuilt by hand.
    Emulated { truncated: usize },
    /// No manifest layout (tokio `--shards 1`): no such window.
    NoManifest,
}

/// Boot A with the crash hook armed at `point`.
fn boot_a(port: u16, dir: &Path, point: &str) -> BootA {
    let mut a = start_moon_with_env(
        port,
        dir,
        3600,
        "yes",
        &SAVE,
        &[("MOON_TEST_AOF_INIT_CRASH", point)],
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(Some(status)) = a.as_mut().try_wait() {
            assert_eq!(
                status.code(),
                Some(INIT_CRASH_EXIT_CODE),
                "boot A exited, but not through MOON_TEST_AOF_INIT_CRASH (status {status:?}); \
                 see {}/moon.stderr.log",
                dir.display()
            );
            return BootA::Hooked;
        }
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "boot A neither stopped at {point} nor served within 60 s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Serving: this binary has no hook. Nothing was written to it, so its
    // incrs hold exactly the heads its boot appended.
    a.kill_now();
    wait_for_port_down(port);
    if !dir.join("appendonlydir").join("moon.aof.manifest").exists() {
        return BootA::NoManifest;
    }
    // The pre-fix order: manifest committed, then shard 0's head, then shard
    // 1's, ... `after_commit` = no head yet; `after_head:<k>` = the heads of
    // shards 0..=k only.
    let written_through: Option<u16> = point
        .strip_prefix("after_head:")
        .and_then(|k| k.parse().ok());
    let mut truncated = 0;
    for (sid, incr) in generation_one_incrs(dir) {
        if written_through.is_some_and(|k| sid <= k) {
            continue;
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(&incr)
            .and_then(|f| f.set_len(0))
            .expect("truncate incr to its pre-head length");
        truncated += 1;
    }
    BootA::Emulated { truncated }
}

fn run(label: &str, point: &str) {
    let port = common::reserve_port();
    let dir = unique_dir(label);
    std::fs::create_dir_all(&dir).expect("create test dir");
    inherit_cold_probes(port, &dir);

    // The no-AOF era: delete every cold probe, snapshot (graves trailer), kill.
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

    // Boot A: the operator turns the AOF on, and the boot dies at `point`.
    let a = boot_a(port, &dir, point);
    if let BootA::Emulated { truncated } = a {
        assert!(
            truncated > 0 || point.starts_with("after_head:"),
            "precondition: the {point} image must lack at least one head"
        );
    }

    // Boot B: same flags, no hook.
    let mut b = start_moon_alive_with(port, &dir, 3600, "yes", &SAVE);
    let back_b = probes_back(port);
    let hot_b = redis_get(port, "hot:control");
    redis_set(port, "after:b", "B");
    std::thread::sleep(Duration::from_secs(2));
    b.kill_now();
    wait_for_port_down(port);

    // Boot C: the generation boot B used (or created) is now the authority.
    let mut c = start_moon_alive_with(port, &dir, 3600, "yes", &SAVE);
    let back_c = probes_back(port);
    let hot_c = redis_get(port, "hot:control");
    let after_c = redis_get(port, "after:b");
    c.kill_now();
    wait_for_port_down(port);

    eprintln!(
        "{label} ({point}, shards {}): deleted {deleted}; boot A: {a:?}; boot B: {back_b} \
         probes back, hot:control={hot_b:?}; boot C: {back_c} probes back, \
         hot:control={hot_c:?}, after:b={after_c:?}",
        shards()
    );
    let mut wrong = Vec::new();
    if back_b != 0 {
        wrong.push(format!(
            "boot B: {back_b}/{deleted} deleted cold probes back"
        ));
    }
    if hot_b.as_deref() != Some("H") {
        wrong.push(format!("boot B: hot:control = {hot_b:?}"));
    }
    if back_c != 0 {
        wrong.push(format!(
            "boot C: {back_c}/{deleted} deleted cold probes back"
        ));
    }
    if hot_c.as_deref() != Some("H") {
        wrong.push(format!("boot C: hot:control = {hot_c:?}"));
    }
    if after_c.as_deref() != Some("B") {
        wrong.push(format!("boot C: after:b = {after_c:?} (acked at boot B)"));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{label} ({point}, {a:?}): {wrong:?}");
}

/// The crash right after the manifest commit: before the fix, no incr had
/// its head yet.
#[test]
#[ignore]
fn crash_after_generation_commit_keeps_cold_dels() {
    run("r1293-after-commit", "after_commit");
}

/// The crash right after shard 0's head: before the fix the manifest was
/// already committed and, at `--shards >= 2`, shards 1.. had no head.
#[test]
#[ignore]
fn crash_between_generation_heads_keeps_cold_dels() {
    run("r1293-after-head0", "after_head:0");
}
