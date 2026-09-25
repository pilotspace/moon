//! moon#1253: a key whose spill was still IN FLIGHT when a write retired it
//! must keep that write across BGREWRITEAOF + kill -9.
//!
//! `crash_recovery_cold_del_rewrite` (moon#1215) lets the evictions settle,
//! so every deleted probe is COLD at its DEL. Here the mutation lands right
//! after the filler, while eviction is still spilling probes. Some probes are
//! then in flight: their spill request still writes their slot, and its
//! completion can be applied only after the rewrite has folded.
//!
//! That fold carried no head `DEL` for such a key, and the rewrite discarded
//! the old generation's. The restart re-indexed the slot from a file the new
//! generation authorizes wholesale (`MOON.COLDCUT` below the watermark, or its
//! `MOON.SPILLED` marker), and the key came back. Measured before the fix on
//! Linux, tokio, `--shards 4`, debug build: 6-9 of 100 deleted probes back,
//! in 4 runs out of 4.
//!
//! The window is proved, not assumed: INFO `spill_completion_superseded`
//! counts completions of requests a write retired in flight. Each round
//! reads it just before BGREWRITEAOF and after the rewrite, and a test whose
//! rounds never saw it rise fails. Without that, a runner whose spills all
//! land before the mutation would pass while exercising nothing.
//!
//! Per test: SET probes (db 0) -> filler evicts them -> mutate the EVEN probes
//! at once -> BGREWRITEAOF, wait until every generation is cut -> SIGKILL ->
//! restart -> every even probe reads its post-mutation state, every odd probe
//! its original value. An overwriting SET the server still refuses with
//! `-OOM` after `OOM_RETRIES` never happened: that probe keeps its original.
//!
//! Run (the nightly crash matrix runs both layouts):
//!   cargo build --release --bin moon --test crash_recovery_cold_del_inflight_1253
//!   MOON_BIN=target/release/moon MOON_TEST_COLD_DEL_SHARDS=4 cargo test --release \
//!     --test crash_recovery_cold_del_inflight_1253 -- --ignored --test-threads=1

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::process::Command;
use std::time::{Duration, Instant};

use common::Conn;

const PROBES: usize = 200;
const PROBE_LEN: usize = 500;
const FILLER: usize = 16_000;
const FILLER_LEN: usize = 600;
/// Attempts per overwriting `SET` that the server refuses with `-OOM`
/// (eviction can lag a burst of writes at this `maxmemory`), 5 ms apart. A
/// SET still refused after them is judged against the probe's original value.
const OOM_RETRIES: usize = 200;
/// Rounds per test: each is an independent server and restart. The window
/// the bug needs is hit in most rounds on a Linux host (33 of 36 on debug
/// builds, both runtimes, both layouts), and the test requires it in at
/// least one: more rounds make a runner that happens to settle between
/// filler and rewrite unlikely to fail the window check, or to hide the bug.
const ROUNDS: usize = 3;

/// `--shards` for every server: 4 (the per-shard fold) by default,
/// `MOON_TEST_COLD_DEL_SHARDS=1` for the single-shard layouts, as in
/// `crash_recovery_cold_del_rewrite`.
fn shards() -> usize {
    std::env::var("MOON_TEST_COLD_DEL_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(4)
}

fn start(dir: &std::path::Path) -> (common::ServerGuard, u16) {
    let off = dir.join("off");
    std::fs::create_dir_all(&off).unwrap();
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let (child, port) = common::spawn_listening(move |port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards().to_string(),
                "--maxmemory",
                "8388608",
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off.to_str().unwrap(),
                "--appendonly",
                "yes",
                "--cold-orphan-sweep-interval-secs",
                "3600",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    (common::ServerGuard::new(child), port)
}

fn filler(port: u16) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let val = "F".repeat(FILLER_LEN);
    let mut buf = Vec::new();
    for i in 0..FILLER {
        buf.extend_from_slice(&common::encode(&["SET", &format!("filler:{i}"), &val]));
    }
    s.write_all(&buf).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    let mut got = 0usize;
    let mut chunk = [0u8; 65536];
    while got < FILLER {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "filler conn closed after {got}");
        got += chunk[..n].iter().filter(|&&b| b == b'\n').count();
    }
}

fn get(c: &mut Conn, key: &str) -> Option<String> {
    let r = c.send(&["GET", key]);
    if r.starts_with("$-1") {
        return None;
    }
    Some(r.split("\r\n").nth(1).unwrap_or_default().to_string())
}

/// INFO `spill_completion_superseded`: completions applied for requests a
/// write had retired while they were in flight (process-wide, all shards).
fn superseded_completions(c: &mut Conn) -> u64 {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix("spill_completion_superseded:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO persistence has no spill_completion_superseded: {info}"))
}

/// Newest incr sequence per AOF directory (per shard, or the TopLevel one),
/// and `1` for a tokio `--shards 1` flat file once a rewrite gave it its RDB
/// preamble (`MOON` magic): what a finished rewrite advances in every layout.
fn generations(dir: &std::path::Path) -> Vec<u64> {
    let aof = dir.join("appendonlydir");
    let mut dirs = vec![aof.clone()];
    for e in std::fs::read_dir(&aof).into_iter().flatten().flatten() {
        if e.path().is_dir() {
            dirs.push(e.path());
        }
    }
    let mut out = Vec::new();
    for d in dirs {
        let newest = std::fs::read_dir(&d)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .strip_prefix("moon.aof.")
                    .and_then(|r| r.strip_suffix(".incr.aof"))
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max();
        out.extend(newest);
    }
    if let Ok(bytes) = std::fs::read(dir.join("appendonly.aof")) {
        out.push(u64::from(bytes.starts_with(b"MOON")));
    }
    out
}

/// BGREWRITEAOF, then wait until every generation is cut and the rewrite
/// reported ok.
fn rewrite(c: &mut Conn, dir: &std::path::Path) {
    let before = generations(dir);
    let r = c.send(&["BGREWRITEAOF"]);
    assert!(r.starts_with('+'), "BGREWRITEAOF: {r:?}");
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let info = c.send(&["INFO", "persistence"]);
        let now = generations(dir);
        let advanced = now.len() == before.len()
            && now.iter().zip(&before).filter(|(a, b)| a > b).count() == shards();
        if info.contains("aof_rewrite_in_progress:0") && advanced {
            assert!(
                info.contains("aof_last_bgrewrite_status:ok"),
                "the rewrite failed: {info}"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "rewrite never finished: {now:?} vs {before:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// What a mutation leaves an even probe as, after the restart.
enum Want {
    Absent,
    Value(&'static str),
}

/// Mutates the even probes; returns the ones the server refused (`-OOM`),
/// which keep their original value.
type Mutate = fn(&mut Conn, &[String]) -> Vec<String>;

/// One round: probes, filler, `mutate` the even probes WITHOUT settling,
/// rewrite, SIGKILL, restart, judge. Returns the probes that read wrong, and
/// how many superseded completions were applied from just before the
/// BGREWRITEAOF until the rewrite finished: requests still in flight when
/// the rewrite began, the window the bug needs.
fn round(tag: &str, mutate: Mutate, want: Want) -> (Vec<String>, u64) {
    let dir = common::unique_test_dir(&format!("cold-del-inflight-1253-{tag}-{}", shards()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = start(&dir);
    let mut c = Conn::open(port);
    let original = "P".repeat(PROBE_LEN);
    for i in 0..PROBES {
        assert_eq!(
            c.send(&["SET", &format!("probe:{i}"), &original]),
            "+OK\r\n"
        );
    }
    filler(port);
    let even: Vec<String> = (0..PROBES)
        .filter(|i| i % 2 == 0)
        .map(|i| format!("probe:{i}"))
        .collect();
    let refused = mutate(&mut c, &even);
    if !refused.is_empty() {
        eprintln!("{tag}: {} mutation(s) refused -OOM", refused.len());
    }
    assert!(
        refused.len() < even.len(),
        "{tag}: the server refused every mutation (-OOM)"
    );
    let superseded_before = superseded_completions(&mut c);
    rewrite(&mut c, &dir);
    let hits = superseded_completions(&mut c).saturating_sub(superseded_before);
    server.kill_now();
    common::wait_for_port_down(port);
    drop(c);

    let (_server2, port2) = start(&dir);
    let mut c2 = Conn::open(port2);
    let mut wrong = Vec::new();
    for i in 0..PROBES {
        let key = format!("probe:{i}");
        let got = get(&mut c2, &key);
        let ok = if i % 2 == 1 || refused.contains(&key) {
            // Never mutated, or refused -OOM: the original value. FLUSHALL
            // takes the odd probes too.
            match want {
                Want::Absent if tag == "flushall" => got.is_none(),
                _ => got.as_deref() == Some(original.as_str()),
            }
        } else {
            match want {
                Want::Absent => got.is_none(),
                Want::Value(v) => got.as_deref() == Some(v),
            }
        };
        if !ok {
            wrong.push(format!(
                "{key}={}",
                got.map_or("<nil>".into(), |g| g[..1].to_string())
            ));
        }
    }
    if wrong.is_empty() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("{tag}: dir kept at {}", dir.display());
    }
    (wrong, hits)
}

fn run(tag: &str, mutate: Mutate, want: fn() -> Want) {
    let mut hits = Vec::with_capacity(ROUNDS);
    for r in 0..ROUNDS {
        let (wrong, round_hits) = round(tag, mutate, want());
        hits.push(round_hits);
        assert!(
            wrong.is_empty(),
            "--shards {}, round {r}: {} probe(s) read wrong after the restart (first: {:?})",
            shards(),
            wrong.len(),
            &wrong[..wrong.len().min(5)]
        );
    }
    eprintln!(
        "{tag}, --shards {}: superseded completions applied during the rewrite, per round: {hits:?}",
        shards()
    );
    assert!(
        hits.iter().any(|&h| h > 0),
        "--shards {}: no round had a superseded spill completion applied during its rewrite \
         (INFO spill_completion_superseded never rose): every in-flight spill landed before \
         the BGREWRITEAOF, so this run never reached the moon#1253 window and proves nothing",
        shards()
    );
}

#[test]
#[ignore = "real-server crash suite: nightly crash matrix, MOON_BIN pinned"]
fn keys_deleted_while_their_spill_is_in_flight_stay_deleted() {
    run(
        "del",
        |c, even| {
            for chunk in even.chunks(25) {
                let mut args = vec!["DEL"];
                args.extend(chunk.iter().map(String::as_str));
                let r = c.send(&args);
                assert!(r.starts_with(':'), "DEL: {r:?}");
            }
            Vec::new()
        },
        || Want::Absent,
    );
}

#[test]
#[ignore = "real-server crash suite: nightly crash matrix, MOON_BIN pinned"]
fn keys_flushed_while_their_spill_is_in_flight_stay_flushed() {
    run(
        "flushall",
        |c, _| {
            assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n");
            Vec::new()
        },
        || Want::Absent,
    );
}

#[test]
#[ignore = "real-server crash suite: nightly crash matrix, MOON_BIN pinned"]
fn keys_overwritten_while_their_spill_is_in_flight_keep_the_new_value() {
    run(
        "overwrite",
        |c, even| {
            let mut refused = Vec::new();
            let mut retried = 0usize;
            for key in even {
                let mut attempts = 0;
                loop {
                    let r = c.send(&["SET", key, "new"]);
                    if r == "+OK\r\n" {
                        break;
                    }
                    assert!(r.starts_with("-OOM"), "SET: {r:?}");
                    retried += usize::from(attempts == 0);
                    attempts += 1;
                    if attempts == OOM_RETRIES {
                        refused.push(key.clone());
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            if retried > 0 {
                eprintln!("overwrite: {retried} SET(s) answered -OOM at least once");
            }
            refused
        },
        || Want::Value("new"),
    );
}
