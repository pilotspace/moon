//! moon#902 crash suite: a key durable in BOTH planes (cold tier + AOF) must
//! come back from a `kill -9` with every non-idempotent write applied
//! EXACTLY once, across repeated unclean restarts, and a write issued to a
//! cold key after a `BGREWRITEAOF` must not vanish either.
//!
//! Mechanism (confirmed in code, see `storage::db::cold_replay_gate`):
//! recovery rebuilds the cold index from the shard manifest, then replays
//! the AOF, and every `get_or_create_*` a replayed write goes through
//! promotes the cold copy before mutating it — `RPUSH l a b c d e` lands on
//! the `[a,b,c,d,e]` it already produced. It compounds by one per crash
//! because the replay-driven eviction re-spills the doubled value.
//!
//! Three legs:
//!   1. **Three `kill -9` cycles**, seven non-idempotent families (`RPUSH`,
//!      `RPUSHX`/`LPUSHX`, `APPEND`, `INCRBY`, `HINCRBY`, `ZINCRBY`,
//!      `BITFIELD INCRBY`) plus two idempotent controls (`HSET`, `SET`).
//!      Every probe that survives must equal its single-application value
//!      after EVERY cycle — a single cycle cannot see the compounding.
//!   2. **Post-rewrite write to a cold key** (the other direction): after a
//!      `BGREWRITEAOF` taken while the probes are cold, the base RDB is
//!      hot-only and the cold copy is the ONLY copy; a write issued then must
//!      land on it and survive. Red on `7572f1ec` for `SET`: the old
//!      end-of-replay demote dropped the replayed v2 in favour of the stale
//!      cold v1 (12/24 probes on the reference run).
//!   3. **`--disk-offload disable` control**: no cold plane, no defect.
//!
//! `--disk-free-min-pct 0` is REQUIRED (the disk-free guard silently guts a
//! crash test on a nearly-full volume). Each leg asserts cold data files
//! exist at the kill point so a run that spilled nothing cannot pass.
//!
//! `#[ignore]`d per the crash-suite convention. Run:
//!   MOON_BIN=target/release-fast/moon cargo test --profile release-fast \
//!     --test cold_tier_aof_double_apply_902 -- --ignored --nocapture

#![allow(clippy::unwrap_used)]

mod common;

use std::process::{Child, Command};
use std::time::{Duration, Instant};

const PROBES: usize = 24;
const FILLER_COUNT: usize = 200;
const FILLER_LEN: usize = 4096;
const MAXMEMORY: &str = "524288"; // 512 KiB — far below the filler wave.

fn start_moon(dir: &std::path::Path, offload: &str) -> (Child, u16) {
    let offload = offload.to_string();
    common::spawn_listening(move |port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                dir.to_str().unwrap(),
                "--appendonly",
                "yes",
                "--disk-offload",
                &offload,
                "--maxmemory",
                MAXMEMORY,
                "--maxmemory-policy",
                "allkeys-lru",
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

/// One RESP reply -> a stable string. `None` == nil (key gone), which is a
/// legitimate outcome for a plain-dropped victim.
fn scalar(reply: &str) -> Option<String> {
    let mut lines = reply.lines();
    let head = lines.next()?;
    if head.starts_with("$-1") || head.starts_with('_') {
        return None;
    }
    if let Some(n) = head.strip_prefix(':') {
        return Some(n.to_string());
    }
    if head.starts_with('$') {
        return lines.next().map(|s| s.to_string());
    }
    if let Some(simple) = head.strip_prefix('+') {
        return Some(simple.to_string());
    }
    Some(head.to_string())
}

/// An array reply -> its items joined by ',' (ints lose their ':').
fn items(reply: &str) -> Option<String> {
    let v: Vec<String> = reply
        .lines()
        .skip(1)
        .filter(|l| !l.starts_with('$'))
        .map(|l| l.strip_prefix(':').unwrap_or(l).to_string())
        .filter(|l| !l.is_empty())
        .collect();
    if v.is_empty() {
        None
    } else {
        Some(v.join(","))
    }
}

const NON_IDEMPOTENT: &[&str] = &["l", "lx", "ap", "n", "hi", "zi", "bf"];
const CONTROLS: &[&str] = &["h", "s"];

fn write_probe(c: &mut common::Conn, fam: &str, key: &str) {
    match fam {
        "l" => {
            c.send(&["RPUSH", key, "a", "b", "c", "d", "e"]);
        }
        "lx" => {
            c.send(&["RPUSH", key, "a"]);
            c.send(&["RPUSHX", key, "b"]);
            c.send(&["LPUSHX", key, "z"]);
        }
        "ap" => {
            c.send(&["APPEND", key, "hello"]);
        }
        "n" => {
            c.send(&["INCRBY", key, "7"]);
        }
        "hi" => {
            c.send(&["HINCRBY", key, "f", "3"]);
        }
        "zi" => {
            c.send(&["ZINCRBY", key, "2.5", "m"]);
        }
        "bf" => {
            c.send(&["BITFIELD", key, "INCRBY", "u8", "0", "5"]);
        }
        "h" => {
            c.send(&["HSET", key, "f1", "v1", "f2", "v2", "f3", "v3"]);
        }
        "s" => {
            c.send(&["SET", key, "v1"]);
        }
        other => panic!("unknown family {other}"),
    }
}

fn read_probe(c: &mut common::Conn, fam: &str, key: &str) -> Option<String> {
    match fam {
        "l" | "lx" => items(&c.send(&["LRANGE", key, "0", "-1"])),
        "ap" | "n" | "s" => scalar(&c.send(&["GET", key])),
        "hi" => scalar(&c.send(&["HGET", key, "f"])),
        "zi" => scalar(&c.send(&["ZSCORE", key, "m"])),
        "bf" => {
            if scalar(&c.send(&["EXISTS", key])).as_deref() != Some("1") {
                return None;
            }
            items(&c.send(&["BITFIELD", key, "GET", "u8", "0"]))
        }
        "h" => items(&c.send(&["HGETALL", key])).map(|s| {
            let mut fields: Vec<&str> = s.split(',').step_by(2).collect();
            fields.sort_unstable();
            fields.join(",")
        }),
        other => panic!("unknown family {other}"),
    }
}

fn expected_once(fam: &str) -> &'static str {
    match fam {
        "l" => "a,b,c,d,e",
        "lx" => "z,a,b",
        "ap" => "hello",
        "n" => "7",
        "hi" => "3",
        "zi" => "2.5",
        "bf" => "5",
        "h" => "f1,f2,f3",
        "s" => "v1",
        other => panic!("unknown family {other}"),
    }
}

fn write_all(c: &mut common::Conn, fams: &[&str]) {
    for fam in fams {
        for i in 0..PROBES {
            write_probe(c, fam, &format!("{fam}:{i}"));
        }
    }
}

fn drive_filler(c: &mut common::Conn) {
    let value = "f".repeat(FILLER_LEN);
    for i in 0..FILLER_COUNT {
        c.send(&["SET", &format!("filler:{i}"), &value]);
    }
}

/// Every present probe must hold its expected value. Returns (present,
/// mismatches) so the caller decides vacuity and reports the CORRUPTED LIST.
fn audit(
    c: &mut common::Conn,
    fams: &[&str],
    expect: impl Fn(&str) -> String,
) -> (usize, Vec<String>) {
    let mut present = 0;
    let mut bad = Vec::new();
    for fam in fams {
        for i in 0..PROBES {
            let key = format!("{fam}:{i}");
            if let Some(got) = read_probe(c, fam, &key) {
                present += 1;
                let want = expect(fam);
                if got != want {
                    bad.push(format!("{key}={got:?} (want {want:?})"));
                }
            }
        }
    }
    (present, bad)
}

fn wait_manifest_change(path: &std::path::Path, before: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if std::fs::read_to_string(path).ok().as_deref() != Some(before) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("BGREWRITEAOF did not advance {}", path.display());
}

#[test]
#[ignore] // Crash suite: spawns servers and SIGKILLs them. Run with --ignored.
fn non_idempotent_writes_survive_three_kill9_cycles_exactly_once() {
    let dir = common::unique_test_dir("cold-aof-double-apply-902");
    std::fs::create_dir_all(&dir).expect("create dir");
    let fams: Vec<&str> = NON_IDEMPOTENT.iter().chain(CONTROLS).copied().collect();

    let (child, mut port) = start_moon(&dir, "enable");
    let mut guard = common::ServerGuard::new(child);
    {
        let mut c = common::Conn::open(port);
        write_all(&mut c, &fams);
        let (present, bad) = audit(&mut c, &fams, |f| expected_once(f).to_string());
        assert_eq!(present, fams.len() * PROBES);
        assert!(bad.is_empty(), "pre-crash sanity: {}", bad.join("; "));
        drive_filler(&mut c);
        std::thread::sleep(Duration::from_secs(1));
    }

    for cycle in 1..=3 {
        let cold_files = count_cold_files(&dir);
        guard.kill_now();
        common::wait_for_port_down(port);
        assert!(
            cold_files > 0,
            "cycle {cycle}: no cold data files at the SIGKILL — nothing spilled, the run \
             would be vacuous (check --disk-free-min-pct and maxmemory/filler sizing)"
        );

        // `spawn_listening` picks a fresh port per restart.
        let (child, new_port) = start_moon(&dir, "enable");
        guard = common::ServerGuard::new(child);
        port = new_port;
        {
            let mut c = common::Conn::open(port);
            let (present, bad) = audit(&mut c, &fams, |f| expected_once(f).to_string());
            assert!(
                present >= fams.len() * PROBES / 2,
                "cycle {cycle}: only {present} probes survived — too few to judge"
            );
            assert!(
                bad.is_empty(),
                "cycle {cycle}: {} probe(s) came back with a write applied more than once \
                 (moon#902 — cold copy restored from the manifest AND the AOF's record of \
                 the same command replayed on top): {}",
                bad.len(),
                bad.join("; ")
            );
        }
        // Let the replay-driven eviction re-spill before the next kill: this
        // is what made the defect COMPOUND (k grows by one per cycle).
        std::thread::sleep(Duration::from_millis(1500));
    }
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore] // Crash suite: spawns servers and SIGKILLs them. Run with --ignored.
fn writes_to_a_cold_key_after_a_rewrite_survive_kill9() {
    let dir = common::unique_test_dir("cold-aof-post-rewrite-902");
    std::fs::create_dir_all(&dir).expect("create dir");
    let fams = ["l", "n", "ap", "s"];

    let (child, port) = start_moon(&dir, "enable");
    let mut guard = common::ServerGuard::new(child);
    {
        let mut c = common::Conn::open(port);
        write_all(&mut c, &fams);
        drive_filler(&mut c);
        std::thread::sleep(Duration::from_secs(1));
        assert!(
            count_cold_files(&dir) > 0,
            "nothing spilled before the rewrite"
        );

        // Rewrite while the probes are cold: the new base is hot-only, so
        // from here the cold copy is the ONLY copy of each probe.
        let manifest = dir.join("appendonlydir").join("moon.aof.manifest");
        let before = std::fs::read_to_string(&manifest).unwrap();
        c.send(&["BGREWRITEAOF"]);
        wait_manifest_change(&manifest, &before);

        for i in 0..PROBES {
            c.send(&["RPUSH", &format!("l:{i}"), "f"]);
            c.send(&["INCRBY", &format!("n:{i}"), "1"]);
            c.send(&["APPEND", &format!("ap:{i}"), "!"]);
            c.send(&["SET", &format!("s:{i}"), "v2"]);
        }
        let (_, bad) = audit(&mut c, &fams, post_rewrite_expected);
        assert!(
            bad.is_empty(),
            "live post-rewrite sanity: {}",
            bad.join("; ")
        );
    }
    let cold_files = count_cold_files(&dir);
    guard.kill_now();
    common::wait_for_port_down(port);
    assert!(
        cold_files > 0,
        "no cold data files at the SIGKILL — vacuous run"
    );

    let (child, port2) = start_moon(&dir, "enable");
    let mut guard2 = common::ServerGuard::new(child);
    let (present, bad) = {
        let mut c = common::Conn::open(port2);
        audit(&mut c, &fams, post_rewrite_expected)
    };
    guard2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        present >= fams.len() * PROBES / 2,
        "only {present} probes survived"
    );
    assert!(
        bad.is_empty(),
        "{} acknowledged post-rewrite write(s) did not survive a kill -9 on a cold key \
         (moon#592 class — the cold copy was the only copy of the pre-rewrite state): {}",
        bad.len(),
        bad.join("; ")
    );
}

fn post_rewrite_expected(fam: &str) -> String {
    match fam {
        "l" => "a,b,c,d,e,f",
        "n" => "8",
        "ap" => "hello!",
        "s" => "v2",
        other => expected_once(other),
    }
    .to_string()
}

#[test]
#[ignore] // Crash suite: spawns servers and SIGKILLs them. Run with --ignored.
fn disk_offload_disabled_control_replays_exactly_once() {
    let dir = common::unique_test_dir("cold-aof-disabled-control-902");
    std::fs::create_dir_all(&dir).expect("create dir");
    let fams: Vec<&str> = NON_IDEMPOTENT.iter().chain(CONTROLS).copied().collect();

    let (child, port) = start_moon(&dir, "disable");
    let mut guard = common::ServerGuard::new(child);
    {
        let mut c = common::Conn::open(port);
        write_all(&mut c, &fams);
        drive_filler(&mut c);
        std::thread::sleep(Duration::from_millis(500));
    }
    guard.kill_now();
    common::wait_for_port_down(port);
    assert_eq!(
        count_cold_files(&dir),
        0,
        "the disabled control must not spill"
    );

    let (child, port2) = start_moon(&dir, "disable");
    let mut guard2 = common::ServerGuard::new(child);
    let (present, bad) = {
        let mut c = common::Conn::open(port2);
        audit(&mut c, &fams, |f| expected_once(f).to_string())
    };
    guard2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    // Victims are plain-dropped here, so fewer survive — but every survivor
    // must be exact. `present > 0` keeps the leg non-vacuous.
    assert!(present > 0, "every probe was evicted — vacuous run");
    assert!(
        bad.is_empty(),
        "control leg (no cold plane) must replay exactly once: {}",
        bad.join("; ")
    );
}
