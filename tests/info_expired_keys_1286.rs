//! moon#1286: `INFO stats` `expired_keys` must count every expiry-driven key
//! removal. It read 0 forever: `record_expired_key` had no production caller.
//!
//! redis 7.0.15 / 7.2.7: 50 × `SET kN v PX 20`, wait, `expired_keys:50`.
//!
//! Three arms per shard count, so each removal path is proven on its own:
//! - active: nothing reads the keys, the shard tick reaps them;
//! - lazy: every key is read after its deadline (moon#542 hides it and
//!   queues the reap for the tick);
//! - hash-field expiry is NOT a key expiry and must leave the counter alone.
//!
//! The count is asserted twice, a second apart: a path that counted a key both
//! at discovery and at the reap would read 100 on the second look.
//!
//! `MOON_BIN=<moon> cargo test --test info_expired_keys_1286 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::time::{Duration, Instant};

use common::Conn;

const KEYS: usize = 50;

fn spawn(dir: &std::path::Path, shards: usize) -> (common::ServerGuard, u16) {
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
                &shards.to_string(),
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

fn expired_keys(c: &mut Conn) -> u64 {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix("expired_keys:"))
        .unwrap_or_else(|| panic!("no expired_keys in INFO stats: {info}"))
        .trim()
        .parse()
        .unwrap()
}

fn dbsize(c: &mut Conn) -> i64 {
    c.send(&["DBSIZE"])
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap()
}

/// Wait until `expired_keys` reaches `want` (or the deadline), returning the
/// last value read.
fn wait_for(c: &mut Conn, want: u64, within: Duration) -> u64 {
    let start = Instant::now();
    loop {
        let n = expired_keys(c);
        if n >= want || start.elapsed() >= within {
            return n;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn set_short_keys(c: &mut Conn, prefix: &str) {
    let keys: Vec<String> = (0..KEYS).map(|i| format!("{prefix}{i}")).collect();
    let cmds: Vec<[&str; 5]> = keys
        .iter()
        .map(|k| ["SET", k.as_str(), "v", "PX", "20"])
        .collect();
    let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
    let r = c.pipeline(&refs);
    assert_eq!(r.matches("+OK\r\n").count(), KEYS, "{r}");
}

fn active_arm(shards: usize) {
    let dir = common::unique_test_dir("i1286a");
    let (_guard, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    assert_eq!(
        expired_keys(&mut c),
        0,
        "a fresh server has expired nothing"
    );
    set_short_keys(&mut c, "act:");
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(dbsize(&mut c), 0, "the keys are gone");
    let n = wait_for(&mut c, KEYS as u64, Duration::from_secs(5));
    assert_eq!(n, KEYS as u64, "active expiry must count every reaped key");
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(expired_keys(&mut c), KEYS as u64, "counted exactly once");
}

fn lazy_arm(shards: usize) {
    let dir = common::unique_test_dir("i1286l");
    let (_guard, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    set_short_keys(&mut c, "lazy:");
    std::thread::sleep(Duration::from_millis(60));
    // Read every key past its deadline: each read hides it and queues the
    // reap (moon#542), and the tick removes it.
    for i in 0..KEYS {
        assert_eq!(c.send(&["GET", &format!("lazy:{i}")]), "$-1\r\n");
    }
    std::thread::sleep(Duration::from_millis(1500));
    let n = wait_for(&mut c, KEYS as u64, Duration::from_secs(5));
    assert_eq!(n, KEYS as u64, "lazy expiry must count every reaped key");
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(expired_keys(&mut c), KEYS as u64, "counted exactly once");
}

/// A key reaped by DEL after its deadline is one `expireIfNeeded` counts in
/// redis. The 100 ms slow cycle can win the race for a key, so use many and
/// assert the total, whoever reaped which.
fn del_arm(shards: usize) {
    let dir = common::unique_test_dir("i1286d");
    let (_guard, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    set_short_keys(&mut c, "del:");
    std::thread::sleep(Duration::from_millis(60));
    for i in 0..KEYS {
        // Reaped by DEL (`:0`) or already reaped by the tick (`:0`).
        assert_eq!(c.send(&["DEL", &format!("del:{i}")]), ":0\r\n");
    }
    let n = wait_for(&mut c, KEYS as u64, Duration::from_secs(5));
    assert_eq!(n, KEYS as u64, "DEL of an expired key counts once");
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(expired_keys(&mut c), KEYS as u64, "counted exactly once");
}

fn hash_field_arm(shards: usize) {
    let dir = common::unique_test_dir("i1286h");
    let (_guard, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    for i in 0..KEYS {
        let k = format!("h:{i}");
        c.send(&["HSET", &k, "f", "v", "g", "w"]);
        let r = c.send(&["HPEXPIRE", &k, "20", "FIELDS", "1", "f"]);
        assert!(r.starts_with('*'), "HPEXPIRE must be supported: {r}");
    }
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        c.send(&["HLEN", "h:0"]),
        ":1\r\n",
        "precondition: the field expired and was reaped"
    );
    assert_eq!(
        expired_keys(&mut c),
        0,
        "hash-field expiry is not a key expiry"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn active_expiry_counts_expired_keys_1_shard() {
    active_arm(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn active_expiry_counts_expired_keys_4_shards() {
    active_arm(4);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn lazy_expiry_counts_expired_keys_1_shard() {
    lazy_arm(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn lazy_expiry_counts_expired_keys_4_shards() {
    lazy_arm(4);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn del_of_an_expired_key_counts_1_shard() {
    del_arm(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn del_of_an_expired_key_counts_4_shards() {
    del_arm(4);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn hash_field_expiry_is_not_counted_1_shard() {
    hash_field_arm(1);
}
