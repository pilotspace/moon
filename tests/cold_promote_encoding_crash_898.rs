//! moon#898 durability gate: `kill -9` mid-spill and mid-promote.
//!
//! The fix (`kv_serde::compact_for_promotion`) sits on the promote-back
//! boundary, which is also the path a crash lands in the middle of. This suite
//! proves the two things a re-derivation on that boundary could plausibly
//! break:
//!
//!   1. **Mid-spill SIGKILL.** Kill the server while the filler wave is still
//!      driving `allkeys-lru` spills. Restart on the same `--dir`. Every probe
//!      that survives must hold its COMPLETE original content — the fix must
//!      not turn a crash into data loss (the moon#866 class), and EXACTLY its
//!      original content — this suite is where moon#902 (cold copy restored
//!      from the manifest AND the AOF's record of the same `RPUSH` replayed
//!      on top) was found; it tolerated that shape for lists until the fix
//!      landed, and tolerates nothing now. `tests/
//!      cold_tier_aof_double_apply_902.rs` owns that defect's own gate.
//!   2. **Mid-promote SIGKILL.** Drive a second spill wave post-restart, then
//!      kill while a promote-back wave is in flight — the exact window where
//!      `promote_cold_outcome` is rebuilding compact encodings. Restart again
//!      and re-assert content, then promote everything back and assert the
//!      COMPACT encoding, which is the moon#898 claim itself.
//!
//! `--disk-free-min-pct 0` is REQUIRED: on a nearly-full shared volume the
//! disk-free guard pauses writes and silently guts the crash test — a green
//! run that spilled nothing (see the diskfull-guard gotcha ledger).
//!
//! Non-vacuity: the suite asserts cold data files (`shard-*/data/heap-*.mpf`) exist
//! at each kill point. Without that, a run in which nothing ever spilled would
//! pass every assertion below while testing nothing at all.
//!
//! `#[ignore]`d per the crash-suite convention (these spawn servers, SIGKILL
//! them, and take tens of seconds). Run:
//!   cargo test --profile release-fast --test cold_promote_encoding_crash_898 \
//!     -- --ignored --nocapture

#![allow(clippy::unwrap_used)]

mod common;

use std::process::{Child, Command};
use std::time::Duration;

const PROBES: usize = 24;
const FILLER_COUNT: usize = 400;
const FILLER_LEN: usize = 4096;
const MAXMEMORY: &str = "524288"; // 512 KiB — tiny, so eviction starts at once.

fn start_moon(dir: &std::path::Path) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                dir.to_str().expect("utf8 dir"),
                "--appendonly",
                "yes",
                // Spill is INERT without a durability backstop; AOF supplies
                // the ShardManifest the spilling eviction paths need.
                "--disk-offload",
                "enable",
                "--maxmemory",
                MAXMEMORY,
                "--maxmemory-policy",
                "allkeys-lru",
                // Approximate LRU samples 5 keys by default, which makes it a
                // coin flip whether any given probe is ever sampled out of
                // hundreds of filler keys.
                "--maxmemory-samples",
                "200",
                // REQUIRED — see the module doc.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// Cold data files written by the spilling eviction paths (`shard-N/data/
/// heap-NNNNNN.mpf`). `> 0` is this suite's proof that it is testing a cold
/// tier with something in it, and not an empty one.
fn count_cold_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                {
                    *acc += 1;
                }
            }
        }
    }
    let mut acc = 0;
    walk(dir, &mut acc);
    acc
}

fn probe_keys() -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    for kind in ["h", "l", "ss", "si", "z"] {
        for i in 0..PROBES {
            out.push((kind, format!("{kind}:{i}")));
        }
    }
    out
}

fn write_probe(c: &mut common::Conn, kind: &str, key: &str) {
    match kind {
        "h" => {
            c.send(&["HSET", key, "f1", "v1", "f2", "v2", "f3", "v3"]);
        }
        "l" => {
            c.send(&["RPUSH", key, "a", "b", "c", "d", "e"]);
        }
        "ss" => {
            c.send(&["SADD", key, "alpha", "beta", "gamma"]);
        }
        "si" => {
            c.send(&["SADD", key, "1", "2", "3"]);
        }
        "z" => {
            c.send(&["ZADD", key, "1", "a", "2.5", "b", "3", "c"]);
        }
        other => panic!("unknown probe kind {other}"),
    }
}

/// The probe's content, as a stable string. `None` == the key is gone, which
/// is a LEGITIMATE outcome: the periodic tick path plain-DROPS its victims
/// instead of spilling them, and a hard crash can lose an un-fsynced write.
fn read_probe(c: &mut common::Conn, kind: &str, key: &str) -> Option<String> {
    let raw = match kind {
        "h" => c.send(&["HGETALL", key]),
        "l" => c.send(&["LRANGE", key, "0", "-1"]),
        "ss" | "si" => c.send(&["SMEMBERS", key]),
        "z" => c.send(&["ZRANGE", key, "0", "-1", "WITHSCORES"]),
        other => panic!("unknown probe kind {other}"),
    };
    let mut items: Vec<String> = raw
        .lines()
        .skip(1)
        .filter(|l| !l.starts_with('$'))
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    if items.is_empty() {
        return None;
    }
    items.sort();
    Some(items.join(","))
}

fn expected(kind: &str) -> String {
    let mut items: Vec<&str> = match kind {
        "h" => vec!["f1", "v1", "f2", "v2", "f3", "v3"],
        "l" => vec!["a", "b", "c", "d", "e"],
        "ss" => vec!["alpha", "beta", "gamma"],
        "si" => vec!["1", "2", "3"],
        "z" => vec!["a", "1", "b", "2.5", "c", "3"],
        other => panic!("unknown probe kind {other}"),
    };
    items.sort();
    items.join(",")
}

fn want_encoding(kind: &str) -> &'static str {
    if kind == "si" { "intset" } else { "listpack" }
}

/// `OBJECT ENCODING` through MULTI/EXEC — the MUTABLE dispatch path, whose
/// `Database::get` calls `promote_cold_if_present`. A plain `OBJECT ENCODING`
/// takes the read-only path, which answers from the NON-promoting cold
/// read-through and would report the full form no matter what this fix does.
fn promote_and_encoding(c: &mut common::Conn, key: &str) -> Option<String> {
    c.send(&["MULTI"]);
    c.send(&["OBJECT", "ENCODING", key]);
    let exec = c.send(&["EXEC"]);
    let enc = exec
        .lines()
        .find(|l| {
            !l.starts_with('*') && !l.starts_with('$') && !l.is_empty() && !l.starts_with("-")
        })
        .map(|l| l.trim().to_string())?;
    if enc.is_empty() { None } else { Some(enc) }
}

fn drive_filler(c: &mut common::Conn, from: usize, to: usize) {
    let value = "f".repeat(FILLER_LEN);
    for i in from..to {
        c.send(&["SET", &format!("filler:{i}"), &value]);
    }
}

fn write_all_probes(c: &mut common::Conn) {
    for (kind, key) in probe_keys() {
        write_probe(c, kind, &key);
    }
}

/// Content check over every probe: none may come back PARTIAL. Returns how
/// many were still present, so the caller can refuse a vacuous run.
fn assert_content_intact(c: &mut common::Conn, phase: &str) -> usize {
    let mut present = 0;
    let mut broken = Vec::new();
    for (kind, key) in probe_keys() {
        match read_probe(c, kind, &key) {
            None => {}
            Some(got) => {
                present += 1;
                if got != expected(kind) {
                    broken.push(format!("{key}: {got:?} != {:?}", expected(kind)));
                }
            }
        }
    }
    assert!(
        broken.is_empty(),
        "[{phase}] probes came back WRONG after a SIGKILL — a surviving key must \
         hold exactly its original content (partial = moon#866 class, a whole \
         multiple = moon#902 class): {}",
        broken.join("; ")
    );
    present
}

#[test]
#[ignore] // Crash suite: spawns servers and SIGKILLs them. Run with --ignored.
fn compact_encodings_survive_a_kill9_mid_spill_and_mid_promote() {
    let dir = common::unique_test_dir("cold-promote-crash-898");
    std::fs::create_dir_all(&dir).expect("create dir");

    // ── round 1: kill -9 MID-SPILL ───────────────────────────────────────
    let (child, port) = start_moon(&dir);
    let mut guard = common::ServerGuard::new(child);
    {
        let mut c = common::Conn::open(port);
        write_all_probes(&mut c);
        // Half the filler wave, then kill with the rest still unwritten: the
        // spill thread is draining its queue at this moment.
        drive_filler(&mut c, 0, FILLER_COUNT / 2);
        std::thread::sleep(Duration::from_millis(300));
    }
    let cold_files_at_kill = count_cold_files(&dir);
    guard.kill_now();
    common::wait_for_port_down(port);
    assert!(
        cold_files_at_kill > 0,
        "no cold data files existed at the mid-spill SIGKILL — nothing had spilled, \
         so this run would test nothing. Check --disk-free-min-pct and the \
         maxmemory/filler sizing."
    );

    // ── round 2: restart, verify, then kill -9 MID-PROMOTE ───────────────
    let (child2, port2) = start_moon(&dir);
    let mut guard2 = common::ServerGuard::new(child2);
    let survivors_after_crash;
    {
        let mut c = common::Conn::open(port2);
        survivors_after_crash = assert_content_intact(&mut c, "after mid-spill SIGKILL");

        // A second spill wave, so the promote-back path has work to do.
        drive_filler(&mut c, FILLER_COUNT / 2, FILLER_COUNT);
        std::thread::sleep(Duration::from_secs(2));

        // Promote a PREFIX of the probes, then die with the rest still cold —
        // the SIGKILL lands squarely inside the promote-back wave.
        let keys = probe_keys();
        for (_, key) in keys.iter().take(keys.len() / 3) {
            let _ = promote_and_encoding(&mut c, key);
        }
    }
    let cold_files_at_second_kill = count_cold_files(&dir);
    guard2.kill_now();
    common::wait_for_port_down(port2);
    assert!(
        survivors_after_crash > 0,
        "every probe was gone after the mid-spill SIGKILL — nothing survived to \
         check, so the content assertions above were vacuous"
    );
    assert!(
        cold_files_at_second_kill > 0,
        "no cold data files existed at the mid-promote SIGKILL"
    );

    // ── round 3: restart, content intact, encodings compact ──────────────
    let (child3, port3) = start_moon(&dir);
    let mut guard3 = common::ServerGuard::new(child3);
    let (survivors, failures) = {
        let mut c = common::Conn::open(port3);
        let survivors = assert_content_intact(&mut c, "after mid-promote SIGKILL");

        // The moon#898 claim, after two hard crashes: promoting a key back
        // must land it in the encoding a live write would have produced.
        let mut failures = Vec::new();
        for (kind, key) in probe_keys() {
            if let Some(enc) = promote_and_encoding(&mut c, &key) {
                let want = want_encoding(kind);
                if enc != want {
                    failures.push(format!("{key}: promoted back as {enc}, want {want}"));
                }
            }
        }
        (survivors, failures)
    };
    guard3.kill_now();
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        survivors > 0,
        "every probe was gone after the mid-promote SIGKILL — the run is vacuous"
    );
    assert!(
        failures.is_empty(),
        "cold promote-back flattened these encodings after a kill -9 (moon#898): {}",
        failures.join(", ")
    );
}
