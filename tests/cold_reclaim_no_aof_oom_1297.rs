//! moon#1297: the no-AOF cold reclaim must not make a steady write flood
//! answer `-OOM`, and must still free disk once the writes stop.
//!
//! Without an AOF a compaction waits for a snapshot that starts after it
//! (minutes, with no save rule firing), and its record is charged to write
//! admission until then. Bounded only by a count, a SET flood at `maxmemory`
//! (`redis-benchmark -t set -r 50000 -d 600 -c 16 -P 16`, `--shards 4`, 8 MB,
//! disk offload, no AOF) ran every shard's records past half of its budget,
//! where a write that needs room is refused with
//! `-OOM command not allowed when used memory > 'maxmemory'` — for as long as
//! the records wait, so a second burst after the flood is refused too.
//!
//! - `a_set_flood_without_an_aof_is_never_refused_by_the_reclaim`: a flood,
//!   a pause in which the reclaim compacts what it can, then a burst of new
//!   keys: every SET must answer `+OK`. (Red on the unfixed build: the
//!   burst is refused from its first write.)
//! - `the_reclaim_frees_disk_after_a_flood_ends`: with the sweeps a second
//!   apart, the snapshot the reclaim requests adopts the compactions and old
//!   spill files are unlinked.
//!
//! ```text
//! MOON_BIN=... cargo test --release --test cold_reclaim_no_aof_oom_1297 -- \
//!   --include-ignored --test-threads 1
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

/// Save points that never fire, as in the bench.
const SAVE: [&str; 2] = ["--save", "3600 100000000"];
/// Connections, and SETs each pipelines at a time (`-c 16 -P 16`).
const CLIENTS: usize = 16;
const PIPELINE: usize = 16;
/// Keys (`-r 50000`) and value bytes (`-d 600`) of the flood.
const KEYSPACE: u64 = 50_000;
const VALUE_LEN: usize = 600;
/// SETs of the flood, twice the bench's, so the compactions pile up.
const FLOOD_SETS: usize = 800_000;
/// SETs of the burst of new keys after the pause.
const BURST_SETS: usize = 64_000;

/// What a connection saw: error replies (first few kept) and their count.
#[derive(Default)]
struct Refused {
    count: usize,
    first: Vec<String>,
}

/// One client: `sets` SETs of random keys in `keys` starting at key `base`,
/// `PIPELINE` at a time.
fn flood_client(port: u16, seed: u64, sets: usize, base: u64, keys: u64) -> Refused {
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("timeout");
    let mut w = stream.try_clone().expect("clone");
    let mut r = BufReader::new(stream);
    let value = "x".repeat(VALUE_LEN);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut refused = Refused::default();
    let mut buf = Vec::with_capacity(PIPELINE * (VALUE_LEN + 64));
    let mut line = String::new();
    for _ in 0..(sets / CLIENTS / PIPELINE) {
        buf.clear();
        for _ in 0..PIPELINE {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let key = format!("key:{:012}", base + x % keys);
            buf.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${VALUE_LEN}\r\n{value}\r\n",
                    key.len()
                )
                .as_bytes(),
            );
        }
        w.write_all(&buf).expect("SET write");
        for _ in 0..PIPELINE {
            line.clear();
            r.read_line(&mut line).expect("SET reply");
            if !line.starts_with("+OK") {
                refused.count += 1;
                if refused.first.len() < 3 {
                    refused.first.push(line.trim_end().to_string());
                }
            }
        }
    }
    refused
}

/// `sets` SETs from [`CLIENTS`] connections at once; the replies refused.
fn run_sets(port: u16, sets: usize, base: u64, keys: u64) -> Refused {
    let clients: Vec<_> = (0..CLIENTS as u64)
        .map(|c| std::thread::spawn(move || flood_client(port, c + 1 + base, sets, base, keys)))
        .collect();
    let mut all = Refused::default();
    for c in clients {
        let r = c.join().expect("client thread");
        all.count += r.count;
        for f in r.first {
            if all.first.len() < 3 {
                all.first.push(f);
            }
        }
    }
    all
}

/// Wait until INFO's `cold_reclaim_compactions` has not moved for `quiet`.
fn wait_reclaim_quiet(port: u16, quiet: Duration, limit: Duration) -> u64 {
    let deadline = Instant::now() + limit;
    let mut last = info_u64(port, "cold_reclaim_compactions").unwrap_or(0);
    let mut since = Instant::now();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(250));
        let now = info_u64(port, "cold_reclaim_compactions").unwrap_or(0);
        if now != last {
            last = now;
            since = Instant::now();
        } else if since.elapsed() >= quiet {
            break;
        }
    }
    last
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_set_flood_without_an_aof_is_never_refused_by_the_reclaim() {
    let port = common::reserve_port();
    let dir = unique_dir("oom1297");
    std::fs::create_dir_all(&dir).expect("create test dir");
    // Sweeps an hour apart: no snapshot is requested, so the compactions
    // wait for one, as they do in the bench.
    let mut server = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let started = Instant::now();
    let flood = run_sets(port, FLOOD_SETS, 0, KEYSPACE);
    let flooded = started.elapsed();
    // The writes have stopped: the reclaim compacts what it is allowed to.
    let compactions = wait_reclaim_quiet(port, Duration::from_secs(2), Duration::from_secs(30));
    let pending_bytes = info_u64(port, "cold_reclaim_pending_bytes");
    // A burst of new keys needs room for every write.
    let burst = run_sets(port, BURST_SETS, 1_000_000, 1_000_000);
    server.kill_now();
    eprintln!(
        "{FLOOD_SETS} SETs in {flooded:?} at --shards {}: {} refused {:?}; {compactions} \
         compactions, records {pending_bytes:?} bytes; then {BURST_SETS} new keys: {} refused {:?}",
        shards(),
        flood.count,
        flood.first,
        burst.count,
        burst.first,
    );
    let mut wrong = Vec::new();
    if flood.count > 0 {
        wrong.push(format!(
            "{} of {FLOOD_SETS} flood SETs were refused (first: {:?})",
            flood.count, flood.first
        ));
    }
    if burst.count > 0 {
        wrong.push(format!(
            "{} of {BURST_SETS} SETs of new keys were refused after the flood (first: {:?})",
            burst.count, burst.first
        ));
    }
    if compactions == 0 {
        wrong.push("precondition: the reclaim compacted nothing".to_string());
    }
    // Each shard's records stay far from the half of its budget that refuses
    // a write.
    if let Some(b) = pending_bytes
        && b as usize > MAXMEMORY_BYTES / 8
    {
        wrong.push(format!(
            "the reclaim's records hold {b} bytes, over an eighth of maxmemory"
        ));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{wrong:?}");
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn the_reclaim_frees_disk_after_a_flood_ends() {
    let port = common::reserve_port();
    let dir = unique_dir("oom1297-frees");
    std::fs::create_dir_all(&dir).expect("create test dir");
    // Sweeps a second apart: a compaction that waits three of them for a
    // snapshot makes the shard request one.
    let mut server = start_moon_alive_with(port, &dir, 1, "no", &SAVE);
    let flood = run_sets(port, FLOOD_SETS / 2, 0, KEYSPACE);
    let heap_before = count_heap_files(&dir);
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut unlinked = 0;
    while unlinked == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(500));
        unlinked = info_u64(port, "cold_reclaim_files_unlinked").unwrap_or(0);
    }
    let bytes = info_u64(port, "cold_reclaim_bytes_unlinked").unwrap_or(0);
    let requested = info_u64(port, "cold_reclaim_snapshots_requested").unwrap_or(0);
    let compactions = info_u64(port, "cold_reclaim_compactions").unwrap_or(0);
    let heap_after = count_heap_files(&dir);
    server.kill_now();
    eprintln!(
        "{} refused; {compactions} compactions, {requested} snapshots requested; {unlinked} files \
         ({bytes} bytes) unlinked; heap files {heap_before} -> {heap_after}",
        flood.count
    );
    let mut wrong = Vec::new();
    if flood.count > 0 {
        wrong.push(format!(
            "{} flood SETs were refused (first: {:?})",
            flood.count, flood.first
        ));
    }
    if unlinked == 0 || bytes == 0 {
        wrong.push(format!(
            "no spill file was unlinked within 90 s of the flood ({compactions} compactions, \
             {requested} snapshots requested)"
        ));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{wrong:?}");
}
