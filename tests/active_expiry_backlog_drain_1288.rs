//! moon#1288: active expiry must drain an expired backlog at a useful rate.
//!
//! The 100 ms slow cycle spends 1 ms per cycle (1% duty), so a backlog — a
//! burst of keys sharing a deadline, or a process resuming from a stall past
//! thousands of deadlines — drained at ~7K keys/s: 1.84M keys took ~4 min
//! with their memory charged throughout. redis adapts (up to 25% CPU while
//! the sampled expired ratio stays above 10%) and clears the same backlog in
//! ~4 s. The fix latches a backlog whenever a cycle ends on its budget with
//! due keys left and drains it from the 1 ms tick in duty-capped slices
//! (`server::expire_adaptive`).
//!
//! Here: 200K keys `PX 300`, all expired before the first sweep can reach
//! them, on an otherwise idle server (so the monoio idle park is live and
//! must be held off while the backlog drains). DBSIZE counts expired-but-
//! present keys, as redis's does.
//!
//! Red on ce65400 (release-fast): ~7.7K keys/s → ~26 s for 200K keys.
//!
//! `MOON_BIN=<moon> cargo test --test active_expiry_backlog_drain_1288 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::time::{Duration, Instant};

use common::Conn;

const KEYS: usize = 200_000;
/// ≥ 25K keys/s. The fix measures ~190K keys/s release-fast on a 4-vCPU
/// container; the base ~7.7K keys/s would need ~26 s.
const MAX_DRAIN: Duration = Duration::from_secs(8);

fn spawn(dir: &std::path::Path) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    common::spawn_listening_guarded(move |port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir.to_str().unwrap(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
            ])
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    })
}

fn dbsize(c: &mut Conn) -> usize {
    let r = c.send(&["DBSIZE"]);
    r.trim_start_matches(':').trim().parse().unwrap()
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_expired_backlog_drains_at_an_adaptive_rate() {
    let dir = common::unique_test_dir("i1288");
    let (_guard, port) = spawn(&dir);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", "live", "1"]), "+OK\r\n");
    for chunk in (0..KEYS).collect::<Vec<_>>().chunks(1_000) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("k:{i}")).collect();
        let cmds: Vec<[&str; 5]> = keys
            .iter()
            .map(|k| ["SET", k.as_str(), "v", "PX", "300"])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let replies = c.pipeline(&refs);
        assert_eq!(replies.matches("+OK").count(), chunk.len());
    }
    // Every key is past its deadline from here on.
    std::thread::sleep(Duration::from_millis(400));
    let start_size = dbsize(&mut c);
    assert!(
        start_size > KEYS / 2,
        "vacuity guard: only {start_size} keys were left to drain when the test began"
    );
    let t0 = Instant::now();
    let mut size = start_size;
    while size > 1 && t0.elapsed() < Duration::from_secs(60) {
        std::thread::sleep(Duration::from_millis(50));
        size = dbsize(&mut c);
    }
    let took = t0.elapsed();
    eprintln!(
        "drained {} expired keys in {took:?} ({:.0} keys/s)",
        start_size - size,
        (start_size - size) as f64 / took.as_secs_f64()
    );
    assert_eq!(
        size, 1,
        "the backlog never cleared: {size} keys left after {took:?}"
    );
    assert!(
        took < MAX_DRAIN,
        "{start_size} expired keys took {took:?} to drain (> {MAX_DRAIN:?}): active \
         expiry is not adapting its duty to the backlog"
    );
    assert_eq!(c.send(&["GET", "live"]), "$1\r\n1\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}
