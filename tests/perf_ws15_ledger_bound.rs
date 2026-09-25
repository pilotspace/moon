//! PR #1233 review (moon#1215 dead-slot ledger): `used_memory` stays bounded
//! under cold delete churn — adopted from the integration reviewer's proof
//! (`review3_integ_ledger_growth`).
//!
//! The ledger keeps one key copy per deleted cold slot until the slot's
//! spill file is unlinked, and a file is unlinked only when its LAST live key
//! leaves. A SET/DEL churn that keeps 1 key in 50 alive therefore kept every
//! other deleted key in RAM. Red, measured by the reviewer on the PR head
//! `d4a2fd3` (`--shards 1 --maxmemory 16mb` allkeys-lru, 4 rounds of 20,000
//! SETs of ~1 KB keys, DEL of all but 1 in 50): used_memory 20.7 -> 82.4 MB
//! (5.0x maxmemory; 4.9x on tokio), RSS 143 MB, zero writes refused. On
//! `ae21476` (no ledger) it stayed at 1.75 MB.
//!
//! Now bounded two ways, both checked here:
//! - write admission charges the ledger (`eviction::evict_to_budget`), so a
//!   ledger no eviction can shrink answers OOM instead of growing RSS;
//! - the reclaim (`storage::tiered::cold_reclaim`) compacts the mostly-dead
//!   files, and once the fold the auto-rewrite monitor dispatches commits,
//!   adopts them and unlinks the old files — the ledger itself comes down.
//!
//! Then the data: every key whose SET was acknowledged and never deleted
//! reads back, before and after a `kill -9` restart, and a sample of the
//! deleted keys stays deleted — the reclaim moved live cold keys between
//! files and must not lose or resurrect one.
//!
//! `MOON_BIN=<moon> cargo test --test perf_ws15_ledger_bound -- --nocapture`
//! (both runtimes).

#![allow(clippy::unwrap_used)]

mod common;

use std::process::Command;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

const MAXMEMORY: u64 = 16 * 1024 * 1024;
const PER_ROUND: usize = 20_000;
const ROUNDS: usize = 4;
const KEEP_ONE_IN: usize = 50;

fn spawn(dir: &std::path::Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir.join("off")).unwrap();
    let bin = common::find_moon_binary();
    let d = dir.to_path_buf();
    let (child, port) = common::spawn_listening(move |port| {
        Command::new(&bin)
            .args(["--port", &port.to_string(), "--shards", "1"])
            .args(["--maxmemory", &MAXMEMORY.to_string()])
            .args([
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
            ])
            .args(["--disk-offload-dir", d.join("off").to_str().unwrap()])
            .args(["--appendonly", "yes", "--disk-free-min-pct", "0"])
            // The periodic orphan sweep unlinks a file whose keys were all
            // read back into RAM (promote-then-sweep, moon#1231, a separate
            // pre-existing issue). The survivor check below promotes every
            // survivor, so keep that sweep out of this test's window.
            .args(["--cold-orphan-sweep-interval-secs", "3600"])
            .args(["--dir", d.to_str().unwrap()])
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .unwrap()
    });
    (ServerGuard::new(child), port)
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    c.send(&["INFO", "all"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, Default)]
struct Sample {
    used: u64,
    rss: u64,
    dead_slots: u64,
    dead_bytes: u64,
}

fn sample(c: &mut Conn) -> Sample {
    Sample {
        used: info_u64(c, "used_memory"),
        rss: info_u64(c, "used_memory_rss"),
        dead_slots: info_u64(c, "cold_dead_slots"),
        dead_bytes: info_u64(c, "cold_dead_slot_bytes"),
    }
}

fn ratio(used: u64) -> f64 {
    used as f64 / MAXMEMORY as f64
}

/// One reply line per command of a pipeline (SET / GET-nil / errors).
fn reply_lines(replies: &str) -> Vec<&str> {
    replies.split("\r\n").filter(|l| !l.is_empty()).collect()
}

fn key(round: usize, i: usize, pad: &str) -> String {
    format!("r{round}:{i}:{pad}")
}

/// GET every key in `keys` (pipelined) and return how many answer `want`
/// (`None` = nil).
fn count_matching(c: &mut Conn, keys: &[String], want: Option<&str>) -> usize {
    let mut ok = 0usize;
    for chunk in keys.chunks(200) {
        let cmds: Vec<Vec<&str>> = chunk.iter().map(|k| vec!["GET", k.as_str()]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        let replies = c.pipeline(&refs);
        let mut lines = replies.split("\r\n");
        for _ in chunk {
            let head = lines.next().unwrap_or_default();
            let got = if head == "$-1" { None } else { lines.next() };
            if got == want {
                ok += 1;
            }
        }
    }
    ok
}

#[test]
fn used_memory_stays_bounded_under_cold_delete_churn() {
    let dir = common::unique_test_dir("ws15-ledger-bound");
    let (mut server, port) = spawn(&dir);
    let mut c = Conn::open(port);
    let pad = "x".repeat(990);
    let val = "v".repeat(100);
    let mut refused = 0usize;
    let mut survivors: Vec<String> = Vec::new();
    let mut deleted: Vec<String> = Vec::new();
    let mut peak = Sample::default();
    for r in 0..ROUNDS {
        let keys: Vec<String> = (0..PER_ROUND).map(|i| key(r, i, &pad)).collect();
        let mut accepted = vec![false; PER_ROUND];
        for (n, chunk) in keys.chunks(500).enumerate() {
            let cmds: Vec<Vec<&str>> = chunk
                .iter()
                .map(|k| vec!["SET", k.as_str(), &val])
                .collect();
            let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
            let replies = c.pipeline(&refs);
            let lines = reply_lines(&replies);
            assert_eq!(lines.len(), chunk.len(), "one reply per SET");
            for (i, line) in lines.iter().enumerate() {
                if *line == "+OK" {
                    accepted[n * 500 + i] = true;
                } else {
                    assert!(line.starts_with("-OOM"), "unexpected SET reply {line:?}");
                    refused += 1;
                }
            }
        }
        std::thread::sleep(Duration::from_secs(3));
        // Delete every key of the round but 1 in 50 (the files' anchors).
        let mut dead: Vec<String> = Vec::with_capacity(PER_ROUND);
        for (i, k) in keys.into_iter().enumerate() {
            if i % KEEP_ONE_IN != 0 {
                dead.push(k);
            } else if accepted[i] {
                survivors.push(k);
            }
        }
        for chunk in dead.chunks(500) {
            let mut args = vec!["DEL"];
            args.extend(chunk.iter().map(String::as_str));
            let r = c.send(&args);
            assert!(r.starts_with(':'), "DEL is never refused: {r:?}");
        }
        deleted.extend(dead);
        std::thread::sleep(Duration::from_secs(3));
        let s = sample(&mut c);
        if s.dead_bytes > peak.dead_bytes {
            peak.dead_bytes = s.dead_bytes;
            peak.dead_slots = s.dead_slots;
        }
        peak.used = peak.used.max(s.used);
        peak.rss = peak.rss.max(s.rss);
        eprintln!(
            "round {r}: used_memory {} ({:.2}x maxmemory) rss {} cold_dead_slots {} \
             cold_dead_slot_bytes {} writes refused so far {refused}",
            s.used,
            ratio(s.used),
            s.rss,
            s.dead_slots,
            s.dead_bytes
        );
    }
    assert!(
        peak.dead_slots > 1_000,
        "precondition: the rounds spilled keys cold and deleted them, building a dead-slot \
         ledger (INFO cold_dead_slots peaked at {})",
        peak.dead_slots
    );

    // The reclaim: the ledger comes down and used_memory settles within
    // 1.5x maxmemory (a fold per reclaim cycle, dispatched by the monitor).
    let deadline = Instant::now() + Duration::from_secs(120);
    let settled = loop {
        let s = sample(&mut c);
        if ratio(s.used) <= 1.5 && s.dead_bytes <= peak.dead_bytes / 2 {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "used_memory {} is {:.2}x maxmemory {MAXMEMORY} (peak {:.2}x), ledger {} of a \
             {}-byte peak, after {} rounds of delete churn; {refused} writes refused; dir {}",
            s.used,
            ratio(s.used),
            ratio(peak.used),
            s.dead_bytes,
            peak.dead_bytes,
            ROUNDS,
            dir.display()
        );
        std::thread::sleep(Duration::from_secs(1));
    };
    eprintln!(
        "settled: used_memory {} ({:.2}x), rss {} (peak rss {}), ledger {} bytes / {} slots \
         (peak {} / {}), writes refused {refused}",
        settled.used,
        ratio(settled.used),
        settled.rss,
        peak.rss,
        settled.dead_bytes,
        settled.dead_slots,
        peak.dead_bytes,
        peak.dead_slots
    );
    assert!(
        ratio(peak.used) <= 3.0,
        "used_memory peaked at {:.2}x maxmemory; the reviewer's red run reached 5.0x",
        ratio(peak.used)
    );

    // No acknowledged, never-deleted key was lost moving between files, and
    // no deleted key came back — live, then after kill -9 + restart.
    let sampled_dead: Vec<String> = deleted.iter().step_by(13).cloned().collect();
    let check = |c: &mut Conn, when: &str| {
        let live = count_matching(c, &survivors, Some(val.as_str()));
        let back = sampled_dead.len() - count_matching(c, &sampled_dead, None);
        assert!(
            live == survivors.len() && back == 0,
            "{when}: {live} of {} acknowledged survivors read back, {back} of {} sampled \
             deleted keys came back; dir {}",
            survivors.len(),
            sampled_dead.len(),
            dir.display()
        );
    };
    check(&mut c, "live");
    std::thread::sleep(Duration::from_secs(2)); // everysec fsync
    server.kill_now();
    common::wait_for_port_down(port);
    drop(c);
    let (_server2, port2) = spawn(&dir);
    let mut c2 = Conn::open(port2);
    check(&mut c2, "after kill -9 + restart");
    let _ = std::fs::remove_dir_all(&dir);
}
