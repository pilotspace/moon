//! moon#1294: a connection-path eviction run shares ONE AOF backpressure
//! budget across all of its victims.
//!
//! Every write-eviction gate that runs on a connection (monoio's
//! `run_write_eviction_gate`, tokio's `handler_sharded` per-command gate and
//! `mq_write_gate`, the script bridge, the inline SET path) reports each
//! plain-dropped victim as a reason-`DEL` through
//! `replication::reason_del::record_reason_del_conn`. That helper used to
//! mint a fresh 500 ms `AOF_REASON_DEL_BACKPRESSURE_BOUND` per victim, while
//! the gate holds the db write lock and the `RuntimeConfig` read lock — so
//! ONE write that evicted k keys against a full AOF channel blocked the
//! whole shard for k × 500 ms. The shard-loop sweeps already shared one
//! bound per sweep (#454 P2.8); the connection gates now do the same, and
//! past the bound the rest of the run fails fast into
//! `aof_reason_del_dropped` (fail-loud).
//!
//! Setup: `--shards 1`, AOF on, and the writer held by the
//! `MOON_TEST_AOF_FSYNC_STALL_MS` hook (it sleeps before its first everysec
//! fsync). A pipelined burst fills the 10k writer channel, then `maxmemory`
//! is lowered just above usage and one pipeline writes a 256 KB value plus a
//! small key: the small key's gate must evict dozens of 16 KB / tiny keys.
//! The same shape runs once more through `EVAL`, where the script bridge's
//! gate reports the victims.
//!
//! Red on ce65400 (release-fast): the eviction write took 28.0 s on tokio
//! (56 victims) and 85.9 s on monoio with a 1 MB value (171 victims), with a
//! PING on another connection stalled for the whole time.
//!
//! Run on both runtimes, binary pinned:
//! `MOON_BIN=<moon> cargo test --test eviction_reason_del_run_budget_1294 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::Conn;

/// One bound (500 ms) plus the two refused appends' own 10 ms waits, with
/// generous slack for a loaded CI host. Pre-fix the same write costs
/// victims × 500 ms: ≥ 5 s at the vacuity floor of 10 victims below.
const MAX_STALL: Duration = Duration::from_secs(3);
/// Vacuity floor: fewer victims than this could not tell k × bound from one.
const MIN_VICTIMS: u64 = 10;

fn spawn(dir: &Path) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    common::spawn_listening_guarded(move |port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir.to_str().unwrap(),
                "--shards",
                "1",
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                // A refused ordinary append waits this long, not 2 s.
                "--aof-fsync-timeout-ms",
                "10",
                "--auto-aof-rewrite-percentage",
                "0",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            // The writer sleeps this long before its first everysec fsync:
            // nothing drains the channel for the rest of the test.
            .env("MOON_TEST_AOF_FSYNC_STALL_MS", "120000")
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    })
}

fn info_u64(c: &mut Conn, section: &str, field: &str) -> u64 {
    let info = c.send(&["INFO", section]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO {section} has no numeric {field}"))
}

/// Pipeline `n` SETs of `value` under `prefix`, and return how many were
/// answered with an error (a backlog refusal once the channel is full).
fn pipelined_sets(c: &mut Conn, prefix: &str, n: usize, value: &str) -> usize {
    let mut refused = 0;
    for chunk in (0..n).collect::<Vec<_>>().chunks(1000) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("{prefix}:{i}")).collect();
        let cmds: Vec<[&str; 3]> = keys.iter().map(|k| ["SET", k.as_str(), value]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let replies = c.pipeline(&refs);
        refused += replies.matches("\r\n-").count() + usize::from(replies.starts_with('-'));
    }
    refused
}

/// Lower `maxmemory` to just above current usage (allkeys-lru).
fn arm_maxmemory(c: &mut Conn) {
    let used = info_u64(c, "memory", "used_memory");
    assert_eq!(
        c.send(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]),
        "+OK\r\n"
    );
    let cap = (used + 64 * 1024).to_string();
    assert_eq!(c.send(&["CONFIG", "SET", "maxmemory", &cap]), "+OK\r\n");
}

/// PINGs `port` on its own connection until stopped; returns the longest
/// gap between two consecutive replies, in microseconds.
fn start_prober(port: u16) -> (Arc<AtomicBool>, std::thread::JoinHandle<u64>) {
    let stop = Arc::new(AtomicBool::new(false));
    let max_gap = Arc::new(AtomicU64::new(0));
    let (s, g) = (stop.clone(), max_gap.clone());
    let h = std::thread::spawn(move || {
        let mut c = Conn::open(port);
        c.sock
            .set_read_timeout(Some(Duration::from_secs(200)))
            .unwrap();
        let mut last = Instant::now();
        while !s.load(Ordering::Relaxed) {
            assert_eq!(
                c.send_within(&["PING"], Duration::from_secs(200)),
                "+PONG\r\n"
            );
            let now = Instant::now();
            g.fetch_max((now - last).as_micros() as u64, Ordering::Relaxed);
            last = now;
            std::thread::sleep(Duration::from_millis(5));
        }
        g.load(Ordering::Relaxed)
    });
    (stop, h)
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn one_eviction_run_pays_at_most_one_aof_backpressure_bound() {
    let dir = common::unique_test_dir("i1294");
    let (_guard, port) = spawn(&dir);
    let mut c = Conn::open(port);
    c.sock
        .set_read_timeout(Some(Duration::from_secs(200)))
        .unwrap();

    // 1. Evictable population, then let the writer enter its held fsync.
    let big_val = "v".repeat(16 * 1024);
    pipelined_sets(&mut c, "v", 400, &big_val);
    std::thread::sleep(Duration::from_millis(1_500));
    // 2. Fill the 10k writer channel; the overflow is refused (vacuity guard:
    //    a channel with room would never reach the bound at all).
    let refused = pipelined_sets(&mut c, "f", 10_300, "1");
    assert!(
        refused > 0,
        "vacuity guard: 10,300 SETs against a held writer refused none — the \
         AOF channel never filled and this test would prove nothing"
    );

    let dropped0 = info_u64(&mut c, "persistence", "aof_reason_del_dropped");
    let evicted0 = info_u64(&mut c, "stats", "evicted_keys");
    let payload = "y".repeat(256 * 1024);

    // 3a. Connection write gate: the second write's gate evicts the victims.
    arm_maxmemory(&mut c);
    let (stop, prober) = start_prober(port);
    std::thread::sleep(Duration::from_millis(200));
    let t0 = Instant::now();
    {
        use std::io::Write;
        let mut burst = common::encode(&["SET", "big", &payload]);
        burst.extend_from_slice(&common::encode(&["SET", "x", "1"]));
        c.sock.write_all(&burst).unwrap();
    }
    // Pre-fix this takes victims × 500 ms: wait it out so the red reports
    // the measured stall instead of a read timeout.
    let _ = c.read_replies_within(2, Duration::from_secs(180));
    let conn_write = t0.elapsed();
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    let conn_gap = Duration::from_micros(prober.join().unwrap());
    let evicted1 = info_u64(&mut c, "stats", "evicted_keys");
    let dropped1 = info_u64(&mut c, "persistence", "aof_reason_del_dropped");
    eprintln!(
        "conn gate: write {conn_write:?}, max PING gap {conn_gap:?}, evicted {}, \
         reason-DELs dropped {}",
        evicted1 - evicted0,
        dropped1 - dropped0
    );
    assert!(
        evicted1 - evicted0 >= MIN_VICTIMS,
        "vacuity guard: the write evicted only {} keys",
        evicted1 - evicted0
    );
    assert!(
        dropped1 > dropped0,
        "vacuity guard: no reason-DEL was dropped, so the budget was never exhausted"
    );
    assert!(
        conn_write < MAX_STALL && conn_gap < MAX_STALL,
        "one eviction run blocked the shard {conn_write:?} (PING gap {conn_gap:?}) for {} \
         victims: the AOF backpressure bound is being paid per victim, not per run",
        evicted1 - evicted0
    );

    // 3b. Script bridge gate: the same shape inside one EVAL.
    arm_maxmemory(&mut c);
    let (stop, prober) = start_prober(port);
    std::thread::sleep(Duration::from_millis(200));
    let t0 = Instant::now();
    let _ = c.send_within(
        &[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]) redis.call('SET', KEYS[2], '1') return 1",
            "2",
            "big2",
            "x2",
            &payload,
        ],
        Duration::from_secs(180),
    );
    let lua_write = t0.elapsed();
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    let lua_gap = Duration::from_micros(prober.join().unwrap());
    let evicted2 = info_u64(&mut c, "stats", "evicted_keys");
    eprintln!(
        "script gate: EVAL {lua_write:?}, max PING gap {lua_gap:?}, evicted {}",
        evicted2 - evicted1
    );
    assert!(
        evicted2 - evicted1 >= MIN_VICTIMS,
        "vacuity guard: the script evicted only {} keys",
        evicted2 - evicted1
    );
    // The script's own two write effects may each pay a bound of their own
    // (500 ms apiece: one per client-visible write); the victims must not.
    let lua_max = MAX_STALL + Duration::from_secs(1);
    assert!(
        lua_write < lua_max && lua_gap < lua_max,
        "one script eviction run blocked the shard {lua_write:?} (PING gap {lua_gap:?}) \
         for {} victims",
        evicted2 - evicted1
    );
    let _ = std::fs::remove_dir_all(&dir);
}
