//! R2b5 (moon#1297 regression): the no-AOF cold reclaim must never make a
//! write fail.
//!
//! Without an AOF a compaction waits for a snapshot (minutes with no save
//! rule firing) and its record is charged to write admission until then.
//! Bounded only by a count, a SET flood over `maxmemory` with disk offload
//! (the WS43 bench: `redis-benchmark -t set -r 50000 -d 600 -c 16 -P 16`,
//! `--shards 4`, 8 MB, no AOF) ran the records past half of every shard's
//! budget, and `allkeys-lru` writes were answered
//! `-OOM command not allowed when used memory > 'maxmemory'`.
//!
//! The flood here is that workload, twice as long (800K SETs), so the
//! records reach their count cap: every SET must answer `+OK`, the records
//! must stay small, and the reclaim must still compact.
//!
//!   MOON_BIN=... cargo test --test cold_reclaim_no_aof_oom_r2b5 -- --include-ignored

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
/// SETs in all, over this many keys (`-r 50000`), of this many bytes (`-d`).
const SETS: usize = 800_000;
const KEYSPACE: u64 = 50_000;
const VALUE_LEN: usize = 600;

/// One client: `SETS / CLIENTS` SETs of random keys, `PIPELINE` at a time.
/// Returns the error replies it got (first few kept) and their count.
fn flood_client(port: u16, seed: u64) -> (usize, Vec<String>) {
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("timeout");
    let mut w = stream.try_clone().expect("clone");
    let mut r = BufReader::new(stream);
    let value = "x".repeat(VALUE_LEN);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut errors = 0usize;
    let mut first = Vec::new();
    let mut buf = Vec::with_capacity(PIPELINE * (VALUE_LEN + 64));
    let mut line = String::new();
    for _ in 0..(SETS / CLIENTS / PIPELINE) {
        buf.clear();
        for _ in 0..PIPELINE {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let key = format!("key:{:012}", x % KEYSPACE);
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
                errors += 1;
                if first.len() < 3 {
                    first.push(line.trim_end().to_string());
                }
            }
        }
    }
    (errors, first)
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_set_flood_over_maxmemory_without_an_aof_is_never_refused_by_the_reclaim() {
    let port = common::reserve_port();
    let dir = unique_dir("r2b5-oom");
    std::fs::create_dir_all(&dir).expect("create test dir");
    // No requested snapshot during the run (sweeps an hour apart): the
    // compactions wait, as they do in the bench.
    let mut server = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let started = Instant::now();
    let clients: Vec<_> = (0..CLIENTS as u64)
        .map(|c| std::thread::spawn(move || flood_client(port, c + 1)))
        .collect();
    let mut errors = 0usize;
    let mut first = Vec::new();
    for c in clients {
        let (n, f) = c.join().expect("client thread");
        errors += n;
        first.extend(f);
    }
    let elapsed = started.elapsed();
    let pending_bytes = info_u64(port, "cold_reclaim_pending_bytes");
    // After the flood the reclaim is not paced: it must have compacted.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut compactions = info_u64(port, "cold_reclaim_compactions").unwrap_or(0);
    while compactions == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        compactions = info_u64(port, "cold_reclaim_compactions").unwrap_or(0);
    }
    let pending = info_u64(port, "cold_reclaim_compactions_pending").unwrap_or(0);
    server.kill_now();
    eprintln!(
        "{SETS} SETs in {elapsed:?} at --shards {}: {errors} error replies {first:?}; \
         reclaim records {pending_bytes:?} bytes after the flood, {compactions} compactions \
         ({pending} pending)",
        shards()
    );
    let mut wrong = Vec::new();
    if errors > 0 {
        wrong.push(format!(
            "{errors} of {SETS} SETs were refused (first: {first:?})"
        ));
    }
    // Each shard's records stay under a sixteenth of its budget (plus the
    // jobs in flight): far from the half that refuses a write.
    if let Some(b) = pending_bytes
        && b as usize > MAXMEMORY_BYTES / 8
    {
        wrong.push(format!(
            "the reclaim's records hold {b} bytes, over an eighth of maxmemory"
        ));
    }
    if compactions == 0 {
        wrong.push("the reclaim compacted nothing".to_string());
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{wrong:?}");
}
