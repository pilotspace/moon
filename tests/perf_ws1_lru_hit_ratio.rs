//! moon#1161 end to end: `allkeys-lru` / `allkeys-lfu` must keep the keys a
//! client READS.
//!
//! The 2026-09 review's hit-ratio scenario, against a real server:
//! `maxmemory 64mb`, 1 KiB values, 5K hot keys written once, then rounds of
//! (write 2K new cold keys, GET all 5K hot keys). Hot-key hit ratio in the
//! final round: moon `935c555` **0.6%**, redis 7.0.15 **100%** — because no
//! read path recorded an access, the hot keys kept their creation stamp and
//! were the OLDEST keys in the database, i.e. the first ones evicted.
//!
//! Pacing: the LRU clock (moon's and redis's) has one-second resolution, so
//! keys written in the same second as the hot reads tie with them. The rounds
//! are paced so the run spans several seconds, as a real cache's traffic
//! does, instead of letting the verdict depend on how many rounds this host
//! fits into one second. The deterministic, clock-stepped version of the
//! same scenario lives in `storage::db::ws1_tests::hit_ratio_1161`.
//!
//! Binary: `MOON_BIN` if set (pin it — see CLAUDE.md), else the binary cargo
//! built for this invocation.

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::Conn;

const HOT: usize = 5_000;
const COLD_PER_ROUND: usize = 2_000;
const ROUNDS: usize = 60;
const ROUND_PAUSE: Duration = Duration::from_millis(100);
const MAXMEMORY: usize = 64 * 1024 * 1024;

struct Moon {
    _guard: common::ServerGuard,
    port: u16,
}

fn spawn_moon(policy: &str) -> Moon {
    let bin = common::find_moon_binary();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        let dir = common::unique_test_dir(&format!("ws1-lru-hit-{port}"));
        let _ = std::fs::create_dir_all(&dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-offload",
                "disable",
                "--maxmemory",
                &MAXMEMORY.to_string(),
                "--maxmemory-policy",
                policy,
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf8 dir"),
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    let mut probe = Conn::open(port);
    let info = probe.send(&["INFO", "server"]);
    assert!(
        info.contains("moon_version"),
        "port {port} is held by something that is not moon: {info:?}"
    );
    Moon {
        _guard: guard,
        port,
    }
}

/// Run the scenario; returns the hot-key hit ratio of the final round.
fn hot_hit_ratio(policy: &str) -> f64 {
    let moon = spawn_moon(policy);
    let mut c = Conn::open(moon.port);
    let value = "x".repeat(1024);

    let hot_keys: Vec<String> = (0..HOT).map(|i| format!("hot:{i}")).collect();
    let sets: Vec<Vec<&str>> = hot_keys
        .iter()
        .map(|k| vec!["SET", k.as_str(), value.as_str()])
        .collect();
    let sets: Vec<&[&str]> = sets.iter().map(Vec::as_slice).collect();
    let _ = c.pipeline(&sets);

    let gets: Vec<Vec<&str>> = hot_keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
    let gets: Vec<&[&str]> = gets.iter().map(Vec::as_slice).collect();

    let mut cold = 0usize;
    let mut misses = HOT;
    for _ in 0..ROUNDS {
        let cold_keys: Vec<String> = (cold..cold + COLD_PER_ROUND)
            .map(|i| format!("cold:{i}"))
            .collect();
        cold += COLD_PER_ROUND;
        let writes: Vec<Vec<&str>> = cold_keys
            .iter()
            .map(|k| vec!["SET", k.as_str(), value.as_str()])
            .collect();
        let writes: Vec<&[&str]> = writes.iter().map(Vec::as_slice).collect();
        let reply = c.pipeline(&writes);
        assert!(
            !reply.contains("-OOM"),
            "allkeys eviction answered OOM instead of evicting"
        );
        let reply = c.pipeline(&gets);
        // A value reply is `$1024\r\nxxx…\r\n`; only a miss spells `$-1`.
        misses = reply.matches("$-1\r\n").count();
        std::thread::sleep(ROUND_PAUSE);
    }
    let dbsize = c.send(&["DBSIZE"]);
    let ratio = (HOT - misses) as f64 / HOT as f64;
    eprintln!(
        "moon#1161 {policy}: hot-key hit ratio in the final round = {:.1}% (DBSIZE {})",
        ratio * 100.0,
        dbsize.trim()
    );
    assert!(
        !dbsize.contains(&format!(":{}", HOT + ROUNDS * COLD_PER_ROUND)),
        "fixture must actually evict under maxmemory {MAXMEMORY}"
    );
    ratio
}

#[test]
fn allkeys_lru_keeps_the_hot_read_set() {
    let ratio = hot_hit_ratio("allkeys-lru");
    assert!(
        ratio >= 0.90,
        "allkeys-lru kept {:.1}% of the hot keys (redis 7.0.15: 100%, moon 935c555: 0.6%)",
        ratio * 100.0
    );
}

#[test]
fn allkeys_lfu_keeps_the_hot_read_set() {
    let ratio = hot_hit_ratio("allkeys-lfu");
    assert!(
        ratio >= 0.90,
        "allkeys-lfu kept {:.1}% of the hot keys (moon 935c555: ~0%)",
        ratio * 100.0
    );
}
