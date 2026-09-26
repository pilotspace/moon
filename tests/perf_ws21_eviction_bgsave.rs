//! moon#1257 against a real server: keys evicted while a BGSAVE runs are in
//! the snapshot, which holds the keyspace as it was when the save started.
//!
//! moon's BGSAVE is fork-less copy-on-write: a writer must capture a key's
//! pre-image before changing a key the save has not written yet. Eviction
//! did not, so a key evicted before the walk reached it was missing from the
//! file. Here the walk is HELD (`MOON_TEST_SNAPSHOT_HOLD_FILE`) once every
//! shard has armed the save, so no range is written when `maxmemory` drops
//! to half the dataset and the eviction tick removes keys under
//! `allkeys-random`. The save is then released, the server SIGKILLed and
//! restarted on the snapshot alone (`--appendonly no`): every key the save
//! started from must come back with its value.
//!
//! Runs at `--shards 1` and `--shards 4`. Pin the binary:
//! `MOON_BIN=<moon> cargo test --test perf_ws21_eviction_bgsave`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

const KEYS: u64 = 20_000;

fn spawn(dir: &Path, shards: usize) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .env("MOON_TEST_SNAPSHOT_HOLD_FILE", dir.join("snapshot.hold"))
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // A rule that never fires in the test, so the restart loads
                // the snapshot on every build under test.
                "--save",
                "3600 1000000",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

fn key(j: u64) -> String {
    format!("ev:{j:08}")
}

fn value(j: u64) -> String {
    format!("{j:08}-{}", "x".repeat(100))
}

fn info_field(c: &mut Conn, section: &str, field: &str) -> String {
    c.send(&["INFO", section])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO {section} has no {field}"))
}

fn dbsize(c: &mut Conn) -> u64 {
    let reply = c.send(&["DBSIZE"]);
    reply
        .trim()
        .strip_prefix(':')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("DBSIZE: {reply:?}"))
}

fn wait_for(what: &str, budget: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + budget;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Keys whose restored value is not the one the save started from.
fn wrong_keys(c: &mut Conn) -> Vec<String> {
    let mut wrong = Vec::new();
    let mut i = 0;
    while i < KEYS {
        let end = (i + 1000).min(KEYS);
        let mut out = Vec::new();
        for j in i..end {
            out.extend_from_slice(&encode(&["GET", &key(j)]));
        }
        c.sock.write_all(&out).unwrap();
        let reply = c.read_replies((end - i) as usize);
        let mut lines = reply.split("\r\n");
        for j in i..end {
            let head = lines.next().unwrap_or("");
            let body = if head == "$-1" {
                "<nil>"
            } else {
                lines.next().unwrap_or("")
            };
            if body != value(j) {
                wrong.push(key(j));
            }
        }
        i = end;
    }
    wrong
}

fn keys_evicted_during_a_save_are_in_the_snapshot(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws21-1257-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);

    let mut i = 0;
    while i < KEYS {
        let end = (i + 1000).min(KEYS);
        let mut out = Vec::new();
        for j in i..end {
            out.extend_from_slice(&encode(&["SET", &key(j), &value(j)]));
        }
        c.sock.write_all(&out).unwrap();
        let reply = c.read_replies((end - i) as usize);
        assert!(!reply.contains('-'), "preload refused: {reply:.200}");
        i = end;
    }
    assert_eq!(dbsize(&mut c), KEYS);

    // Hold the walk, start the save, wait until every shard armed it.
    std::fs::write(dir.join("snapshot.hold"), b"held").unwrap();
    assert!(
        c.send(&["BGSAVE"])
            .starts_with("+Background saving started")
    );
    wait_for(
        "every shard to arm the save",
        Duration::from_secs(30),
        || (0..shards).all(|s| dir.join(format!("shard-{s}.rrdshard.tmp")).exists()),
    );

    // Half the dataset's memory: the eviction tick removes keys the held
    // save has not written.
    let used: u64 = info_field(&mut c, "memory", "used_memory").parse().unwrap();
    assert!(
        c.send(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"])
            .starts_with("+OK")
    );
    assert!(
        c.send(&["CONFIG", "SET", "maxmemory", &(used / 2).to_string()])
            .starts_with("+OK")
    );
    wait_for(
        "eviction to remove a third of the keys",
        Duration::from_secs(60),
        || dbsize(&mut c) < KEYS * 2 / 3,
    );
    assert!(
        c.send(&["CONFIG", "SET", "maxmemory", "0"])
            .starts_with("+OK")
    );
    let evicted = KEYS - dbsize(&mut c);
    assert!(
        info_field(&mut c, "persistence", "rdb_bgsave_in_progress") == "1",
        "the held save ended before the evictions"
    );

    std::fs::remove_file(dir.join("snapshot.hold")).unwrap();
    wait_for("the save to finish", Duration::from_secs(120), || {
        info_field(&mut c, "persistence", "rdb_bgsave_in_progress") == "0"
    });
    assert_eq!(
        info_field(&mut c, "persistence", "rdb_last_bgsave_status"),
        "ok"
    );
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);

    let (_server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    let wrong = wrong_keys(&mut c);
    assert!(
        wrong.is_empty(),
        "--shards {shards}: {} of the {evicted} keys evicted during the save are missing or \
         wrong in the snapshot, e.g. {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(3)]
    );
    assert_eq!(dbsize(&mut c), KEYS);
    drop(c);
    drop(_server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn keys_evicted_during_a_save_are_in_the_snapshot_single_shard() {
    keys_evicted_during_a_save_are_in_the_snapshot(1);
}

#[test]
fn keys_evicted_during_a_save_are_in_the_snapshot_four_shards() {
    keys_evicted_during_a_save_are_in_the_snapshot(4);
}
