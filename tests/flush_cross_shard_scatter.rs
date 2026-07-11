//! D-2 red/green: FLUSHDB/FLUSHALL must clear the WHOLE keyspace on a
//! multi-shard server, not just the shard the issuing connection landed on.
//!
//! Before the fix, FLUSHDB/FLUSHALL were keyless → routed local-only, so on
//! `--shards 4` a client's FLUSHALL left ~3/4 of the keys intact (DBSIZE
//! scatter-gathers across shards, so it exposes the survivors directly).
//!
//! Spawns the release moon binary and shells out to `redis-cli`. Skips
//! gracefully when the binary or `redis-cli` is missing.
//!
//! Run with:
//!   cargo test --release --test flush_cross_shard_scatter

mod common;

use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn redis_cli_available() -> bool {
    Command::new("redis-cli")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn release_binary() -> std::path::PathBuf {
    // MOON_BIN pin wins (VM-local target dirs); fall back to target/release.
    if let Ok(p) = std::env::var("MOON_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/release/moon")
}

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

/// Start moon with `--shards 4` on a fresh port + fresh dir.
fn spawn_moon_4shard() -> Option<Moon> {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not in PATH");
        return None;
    }
    let bin = release_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-flush-scatter-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "4",
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    // Same directory formula the closure used for the winning attempt.
    let tmp_dir = std::env::temp_dir().join(format!("moon-flush-scatter-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if redis_cli(moon.port, &["PING"])
            .map(|out| out.trim() == "PONG")
            .unwrap_or(false)
        {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not respond to PING within 10s on port {port}");
    None
}

fn redis_cli(port: u16, args: &[&str]) -> Option<String> {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .ok()?;
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Populate `n` string keys with distinct names — enough spread that all 4
/// shards own several (key→shard is a hash of the key bytes).
fn seed_keys(port: u16, prefix: &str, n: usize) {
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        assert_eq!(
            redis_cli(port, &["SET", &key, "v"]).unwrap().trim(),
            "OK",
            "seed SET {key} failed"
        );
    }
}

#[test]
fn flushall_clears_all_shards() {
    let Some(m) = spawn_moon_4shard() else { return };

    seed_keys(m.port, "fa", 64);
    let before: i64 = redis_cli(m.port, &["DBSIZE"])
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(before, 64, "seed must land 64 keys across the shards");

    assert_eq!(redis_cli(m.port, &["FLUSHALL"]).unwrap().trim(), "OK");

    // DBSIZE scatter-gathers across all shards — any surviving remote-shard
    // key shows up here. Before the D-2 fix this reported ~48 (3/4 of 64).
    let after: i64 = redis_cli(m.port, &["DBSIZE"])
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        after, 0,
        "FLUSHALL must clear every shard's keyspace, {after} keys survived"
    );

    // Individual GETs on a sample confirm no ghost keys serve stale reads.
    for i in [0usize, 17, 33, 63] {
        let key = format!("fa:{i}");
        let got = redis_cli(m.port, &["GET", &key]).unwrap();
        assert_eq!(got.trim(), "", "ghost key {key} survived FLUSHALL");
    }
}

#[test]
fn flushdb_clears_selected_db_on_all_shards() {
    let Some(m) = spawn_moon_4shard() else { return };

    seed_keys(m.port, "fd", 32);
    assert_eq!(redis_cli(m.port, &["FLUSHDB"]).unwrap().trim(), "OK");
    let after: i64 = redis_cli(m.port, &["DBSIZE"])
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        after, 0,
        "FLUSHDB must clear the selected db on every shard, {after} keys survived"
    );
}

#[test]
fn flushdb_other_db_survives_scatter() {
    let Some(m) = spawn_moon_4shard() else { return };

    // Keys in db 0 must survive a FLUSHDB issued against db 1.
    seed_keys(m.port, "keep", 16);
    // Seed db 1 through the same connection sequence (-n selects the db).
    for i in 0..16 {
        let key = format!("gone:{i}");
        let out = Command::new("redis-cli")
            .args(["-p", &m.port.to_string(), "-n", "1", "SET", &key, "v"])
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "OK");
    }

    // FLUSHDB on db 1: clears db 1 across shards, leaves db 0 intact.
    let out = Command::new("redis-cli")
        .args(["-p", &m.port.to_string(), "-n", "1", "FLUSHDB"])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "OK");

    let db1: String = {
        let out = Command::new("redis-cli")
            .args(["-p", &m.port.to_string(), "-n", "1", "DBSIZE"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    assert_eq!(db1, "0", "db 1 must be empty on every shard");

    let db0: i64 = redis_cli(m.port, &["DBSIZE"])
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(db0, 16, "db 0 keys must survive a db-1 FLUSHDB");
}
