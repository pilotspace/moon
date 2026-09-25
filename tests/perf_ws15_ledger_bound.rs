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
//!   adopts them and unlinks the old files — the ledger itself comes down,
//!   to under a tenth of the keys deleted.
//!
//! Then the data: every key whose SET was acknowledged and never deleted
//! reads back, before and after a `kill -9` restart, and a sample of the
//! deleted keys stays deleted — the reclaim moved live cold keys between
//! files and must not lose or resurrect one.
//!
//! Admission may now refuse writes on a shard whose ledger holds it over
//! budget, under any policy. A delete must still get through, including one
//! routed to another shard (`a_delete_routed_to_another_shard_is_never_refused_for_memory`).
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
        // Right after the DELs (before a reclaim fold can have committed) and
        // again once things settle a little.
        let fresh = sample(&mut c);
        std::thread::sleep(Duration::from_secs(3));
        let s = sample(&mut c);
        for x in [fresh, s] {
            if x.dead_bytes > peak.dead_bytes {
                peak.dead_bytes = x.dead_bytes;
                peak.dead_slots = x.dead_slots;
            }
            peak.used = peak.used.max(x.used);
            peak.rss = peak.rss.max(x.rss);
        }
        eprintln!(
            "round {r}: after the DELs cold_dead_slots {} used_memory {} ({:.2}x); 3 s later \
             used_memory {} ({:.2}x maxmemory) rss {} cold_dead_slots {} cold_dead_slot_bytes \
             {} writes refused so far {refused}",
            fresh.dead_slots,
            fresh.used,
            ratio(fresh.used),
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

    // The reclaim: used_memory settles within 1.5x maxmemory and the ledger
    // holds under a tenth of the keys deleted. Without reclaim it holds every
    // deleted cold key (~95% of them on the PR head), and even with writes
    // refused at admission a whole round's worth (~20%): reclaim is what
    // gets it lower. (It stops once the ledger is under its share of the
    // budget, so "back to zero" is not the criterion.)
    let deadline = Instant::now() + Duration::from_secs(120);
    let ledger_cap = (deleted.len() / 10) as u64;
    let settled = loop {
        let s = sample(&mut c);
        if ratio(s.used) <= 1.5 && s.dead_slots <= ledger_cap {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "used_memory {} is {:.2}x maxmemory {MAXMEMORY} (peak {:.2}x), ledger {} slots \
             ({} bytes) after deleting {} keys (cap {ledger_cap}), after {} rounds of delete \
             churn; {refused} writes refused; dir {}",
            s.used,
            ratio(s.used),
            ratio(peak.used),
            s.dead_slots,
            s.dead_bytes,
            deleted.len(),
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

/// Write admission refuses a write on a shard over budget; a command that can
/// only shrink memory must never be refused, or the keys that hold the shard
/// over budget could not be deleted. The connection's own shard already
/// exempted them (WS6); the leg of a command routed to ANOTHER shard ran the
/// same gate without the exemption, so `DEL` of a key owned elsewhere answered
/// `-OOM`. That was reachable under `noeviction` before (measured on
/// `ae21476` and `d4a2fd3`: 144 of 200 single-key DELs refused at
/// `--shards 4`), and the ledger charge made it reachable under every policy
/// while a shard's dead-slot ledger holds it over budget. `noeviction` makes
/// the over-budget state deterministic; the gate code is the same.
///
/// Covers the routed single-command leg, the pipelined leg and a multi-key
/// DEL spanning shards.
#[test]
fn a_delete_routed_to_another_shard_is_never_refused_for_memory() {
    let dir = common::unique_test_dir("ws15-routed-del-oom");
    let bin = common::find_moon_binary();
    let d = dir.clone();
    let (child, port) = common::spawn_listening(move |port| {
        Command::new(&bin)
            .args(["--port", &port.to_string(), "--shards", "4"])
            .args(["--maxmemory", &(4u64 << 20).to_string()])
            .args(["--maxmemory-policy", "noeviction", "--appendonly", "no"])
            .args(["--disk-free-min-pct", "0"])
            .args(["--dir", d.to_str().unwrap()])
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .unwrap()
    });
    let _server = ServerGuard::new(child);
    let mut c = Conn::open(port);
    let val = "v".repeat(1000);
    let keys: Vec<String> = (0..12_000).map(|i| format!("routed-del:{i}")).collect();
    let mut accepted = 0usize;
    let mut refused = 0usize;
    for chunk in keys.chunks(500) {
        let cmds: Vec<Vec<&str>> = chunk
            .iter()
            .map(|k| vec!["SET", k.as_str(), &val])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        for line in reply_lines(&c.pipeline(&refs)) {
            if line == "+OK" {
                accepted += 1;
            } else {
                assert!(line.starts_with("-OOM"), "unexpected SET reply {line:?}");
                refused += 1;
            }
        }
    }
    assert!(
        refused > 0 && accepted > 0,
        "precondition: the shards filled past maxmemory ({accepted} SETs accepted, {refused} \
         refused)"
    );

    let mut deleted = 0usize;
    let mut tally = |reply: &str, what: &str| {
        for line in reply_lines(reply) {
            let n: usize = line
                .strip_prefix(':')
                .and_then(|n| n.parse().ok())
                .unwrap_or_else(|| panic!("{what} answered {line:?}; a delete is never refused"));
            deleted += n;
        }
    };
    // One command at a time (the routed single-command leg).
    for k in &keys[..200] {
        tally(&c.send(&["DEL", k.as_str()]), "DEL");
    }
    // Pipelined (the routed pipelined leg), UNLINK too.
    for (n, chunk) in keys[200..6_000].chunks(200).enumerate() {
        let verb = if n % 2 == 0 { "DEL" } else { "UNLINK" };
        let cmds: Vec<Vec<&str>> = chunk.iter().map(|k| vec![verb, k.as_str()]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        tally(&c.pipeline(&refs), verb);
    }
    // Multi-key DELs spanning every shard.
    for chunk in keys[6_000..].chunks(100) {
        let mut args = vec!["DEL"];
        args.extend(chunk.iter().map(String::as_str));
        tally(&c.send(&args), "multi-key DEL");
    }
    assert_eq!(
        deleted, accepted,
        "every key whose SET was accepted is deleted exactly once"
    );
    assert_eq!(
        c.send(&["SET", "after", "v"]),
        "+OK\r\n",
        "memory was freed"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
