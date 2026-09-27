//! Review round 2b (moon#1280): skipping missed ticks must not slow the
//! per-tick budgeted duties down under a SATURATED loop.
//!
//! The lazy-free drain frees at most `LAZY_FREE_TICK_BUDGET` (250 µs) per
//! 1 ms tick, and the snapshot walk advances one budget per tick. Under a
//! write flood the shard loop's rounds exceed the runtimes' 5 ms grace, so
//! the periodic tick is late on nearly every round (INFO
//! `shard_tick_late_total` climbs ~100/s at `-P 128 -c 50`). Under Burst the
//! late interval replayed the missed ticks, so the drain kept ~25% duty in
//! wall time; under Skip it gets one 250 µs slice per ROUND — the moon#1221
//! review F2 condition ("at one 250 µs slice per 10 ms the drain ran at 2.5%
//! duty and held the memory ~10x longer"), now reached through saturation
//! instead of the idle park.
//!
//! This test UNLINKs 8 hashes x 400K fields (~380 MB) and times how long
//! the drain takes to free 70% of it, idle and while one client runs ~25 ms
//! Lua loops back to back, on the SAME binary (the host's speed cancels
//! out). Linux container, 4 vCPU, not merge bar:
//!
//! | binary | idle | under long commands | ratio |
//! |---|---|---|---|
//! | base-monoio (273e6bc) | 0.37-0.39 s | 0.40-0.43 s | 1.07-1.10 |
//! | r2b-monoio (ad55d3d) | 0.33-0.38 s | 3.61-8.06 s | 10.8-21.4 |
//! | base-tokio | 0.38 s | 0.30 s | 0.78 |
//! | r2b-tokio | 0.36 s | 7.68 s | 21.5 |
//!
//! The snapshot walk has the same shape (a hand run, 632K keys, BGSAVE
//! idle vs under the same EVAL stream): 273e6bc 0.73-0.76 s both ways,
//! ad55d3d 0.70-0.78 s idle vs 4.02-4.21 s.
//!
//! Bound: 3x.
//!
//! Run: MOON_BIN=<release binary> cargo test --test
//!   review_r2b_lazy_free_under_flood_1280 -- --ignored --nocapture

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::Conn;

const HASHES: usize = 8;
const FIELDS: usize = 400_000;

fn start() -> (common::ServerGuard, u16) {
    let dir = common::unique_test_dir("r2b-lazy-flood");
    std::fs::create_dir_all(&dir).unwrap();
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(move |port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    (common::ServerGuard::new(child), port)
}

fn used_memory(c: &mut Conn) -> u64 {
    let info = c.send(&["INFO", "memory"]);
    info.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap()
}

fn load_hashes(port: u16) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut cmds = 0usize;
    let mut buf = Vec::new();
    for h in 0..HASHES {
        let key = format!("big:{h}");
        for chunk in (0..FIELDS).collect::<Vec<_>>().chunks(1000) {
            let mut args: Vec<String> = vec!["HSET".into(), key.clone()];
            for f in chunk {
                args.push(format!("f{f}"));
                args.push("valuevvv".into());
            }
            let refs: Vec<&str> = args.iter().map(String::as_str).collect();
            buf.extend_from_slice(&common::encode(&refs));
            cmds += 1;
        }
    }
    s.write_all(&buf).unwrap();
    let mut seen = 0usize;
    let mut chunk = [0u8; 65536];
    while seen < cmds {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0);
        seen += chunk[..n].iter().filter(|&&b| b == b'\n').count();
    }
}

/// Seconds from UNLINK of every hash until 70% of their memory is freed.
fn drain_secs(c: &mut Conn, loaded: u64) -> f64 {
    let m0 = used_memory(c);
    let t0 = Instant::now();
    let keys: Vec<String> = (0..HASHES).map(|h| format!("big:{h}")).collect();
    let mut args = vec!["UNLINK"];
    args.extend(keys.iter().map(String::as_str));
    assert!(c.send(&args).starts_with(':'));
    loop {
        let m = used_memory(c);
        if m0.saturating_sub(m) > loaded / 10 * 7 {
            return t0.elapsed().as_secs_f64();
        }
        assert!(t0.elapsed() < Duration::from_secs(60), "never freed");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A deterministic saturation: one client running ~20 ms Lua loops back to
/// back (a "long command" workload). Each EVAL holds the shard thread for
/// longer than the 5 ms grace, so every periodic tick after it is late.
fn long_commands(port: u16, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || {
        let mut c = Conn::open(port);
        let mut n = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let r = c.send(&[
                "EVAL",
                "local i = 0 while i < 1500000 do i = i + 1 end return 1",
                "0",
            ]);
            assert!(r.starts_with(":1"), "EVAL: {r}");
            n += 1;
        }
        n
    })
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned (release build)"]
fn a_write_flood_does_not_slow_the_lazy_free_drain() {
    let (_server, port) = start();
    let mut c = Conn::open(port);
    let empty = used_memory(&mut c);

    load_hashes(port);
    let loaded = used_memory(&mut c) - empty;
    let idle = drain_secs(&mut c, loaded);

    let mut flooded = f64::MAX;
    for _ in 0..2 {
        load_hashes(port);
        let stop = Arc::new(AtomicBool::new(false));
        let busy = long_commands(port, stop.clone());
        std::thread::sleep(Duration::from_millis(500));
        let t0 = Instant::now();
        flooded = flooded.min(drain_secs(&mut c, loaded));
        stop.store(true, Ordering::Relaxed);
        let evals = busy.join().unwrap();
        eprintln!(
            "  {evals} EVALs, ~{:.1} ms each",
            (t0.elapsed().as_secs_f64() + 0.5) * 1000.0 / evals.max(1) as f64
        );
    }
    let info = c.send(&["INFO"]);
    let late = info
        .lines()
        .find(|l| l.starts_with("shard_tick_late_total:"))
        .unwrap_or("shard_tick_late_total:<absent>")
        .to_string();
    let ratio = flooded / idle;
    eprintln!("lazy-free drain: idle {idle:.2}s, flooded {flooded:.2}s, ratio {ratio:.2}; {late}");
    assert!(
        ratio <= 3.0,
        "under back-to-back long commands the lazy-free drain took {ratio:.2}x as long as idle \
         ({flooded:.2}s vs {idle:.2}s): the per-tick budget no longer keeps its duty \
         when the tick is late ({late})"
    );
}
