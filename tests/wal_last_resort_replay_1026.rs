//! moon#1026: the last-resort WAL v3 replay must restore KV writes.
//!
//! When `appendonly.aof` is missing (lost, rebuilt or never written) and the
//! WAL carries KV records (`--wal-kv-log on`), boot falls back to
//! `replay_wal_v3_dir_commands`. That helper used to pass each `Command`
//! record's RAW RESP payload to the replay engine as the command NAME with no
//! arguments. Dispatch answered "unknown command" for every record, so nothing
//! reached the keyspace, while the boot log still said
//! "replayed N WAL v3 records". An operator read a partial recovery that never
//! happened.
//!
//! This test drives the real server end to end:
//!
//! 1. `--shards 2 --appendonly yes --wal-kv-log on --disk-offload disable`.
//!    Only SPSC-executed writes reach the WAL (connection-local writes bypass
//!    it by design, which is why WAL v3 is NOT the recovery authority), so two
//!    shards are needed for any KV record to land there at all. Writes to the
//!    connection's own shard stay out of the WAL; writes to the other shard
//!    go in.
//! 2. SIGKILL, then delete every AOF artifact (`appendonly.aof`,
//!    `appendonlydir/`), which leaves the WAL as the only KV source.
//! 3. Restart on the same dir. Every key that comes back must hold the value
//!    written, at least one must come back, and the boot log must report the
//!    number of KV commands it APPLIED, which must equal `DBSIZE`.
//!
//! Pre-fix, step 3 recovers zero keys.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const KEYS: usize = 64;

fn start_moon(dir: &Path) -> (Child, u16) {
    let dir = dir.to_path_buf();
    common::spawn_listening(move |port| {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("server.out"))
            .expect("server.out");
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "2",
                "--dir",
                dir.to_str().unwrap(),
                "--appendonly",
                "yes",
                "--wal-kv-log",
                "on",
                "--disk-offload",
                "disable",
                // The diskfull guard refuses writes on a nearly full volume and
                // would turn this into a false red (see the crash suites).
                "--disk-free-min-pct",
                "0",
            ])
            .env("RUST_LOG", "moon=info")
            .stdout(std::process::Stdio::from(log))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// Total bytes of every WAL v3 segment under `dir`.
fn wal_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    for shard in 0..2 {
        let wal = dir.join(format!("shard-{shard}")).join("wal-v3");
        if let Ok(rd) = std::fs::read_dir(&wal) {
            for e in rd.flatten() {
                if e.path().extension().is_some_and(|x| x == "wal") {
                    total += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }
    total
}

/// Parse a single bulk-string reply; `None` for nil.
fn bulk(reply: &str) -> Option<String> {
    if reply.starts_with("$-1") {
        return None;
    }
    let mut lines = reply.split("\r\n");
    let head = lines.next()?;
    assert!(head.starts_with('$'), "not a bulk reply: {reply:?}");
    lines.next().map(str::to_owned)
}

fn integer(reply: &str) -> i64 {
    reply
        .strip_prefix(':')
        .and_then(|r| r.trim_end().parse().ok())
        .unwrap_or_else(|| panic!("not an integer reply: {reply:?}"))
}

/// Sum every "applied <N> KV command" figure the boot log reported.
fn logged_applied(log: &str) -> Option<u64> {
    let mut found = false;
    let mut sum = 0u64;
    for line in log.lines().filter(|l| l.contains("LAST-RESORT")) {
        if let Some(rest) = line.split("applied ").nth(1)
            && let Some(n) = rest.split_whitespace().next()
            && let Ok(n) = n.parse::<u64>()
        {
            found = true;
            sum += n;
        }
    }
    found.then_some(sum)
}

#[test]
fn last_resort_wal_replay_restores_kv_writes_when_the_aof_is_gone() {
    let dir = common::unique_test_dir("moon-1026-lastresort");
    std::fs::create_dir_all(&dir).unwrap();

    // ── 1. write through the WAL ────────────────────────────────────────
    let (child, port) = start_moon(&dir);
    let mut guard = common::ServerGuard::new(child);
    let before = wal_bytes(&dir);
    {
        let mut c = common::Conn::open(port);
        for i in 0..KEYS {
            let key = format!("k1026:{i}");
            let val = format!("value-{i}");
            assert_eq!(c.send(&["SET", &key, &val]), "+OK\r\n");
        }
    }
    // The WAL buffer is flushed on the shard's 1 ms tick. Wait until the
    // segment bytes grow and then hold still, so the SIGKILL below does not
    // race the flush. A deadline bounds it; a WAL that never grows fails the
    // precondition below with a clear message instead of hanging.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = wal_bytes(&dir);
    let mut stable = 0;
    while Instant::now() < deadline && stable < 5 {
        std::thread::sleep(Duration::from_millis(100));
        let now = wal_bytes(&dir);
        if now > before && now == last {
            stable += 1;
        } else {
            stable = 0;
        }
        last = now;
    }
    assert!(
        last > before,
        "precondition: --wal-kv-log on must put cross-shard KV writes in the WAL \
         (WAL bytes before={before} after={last})"
    );
    guard.kill_now();
    common::wait_for_port_down(port);

    // ── 2. lose every AOF artifact ──────────────────────────────────────
    let _ = std::fs::remove_file(dir.join("appendonly.aof"));
    let _ = std::fs::remove_dir_all(dir.join("appendonlydir"));
    assert!(!dir.join("appendonly.aof").exists());
    assert!(!dir.join("appendonlydir").exists());
    let log_path = dir.join("server.out");
    let boot1_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

    // ── 3. restart: the WAL is the only KV source ───────────────────────
    let (child, port) = start_moon(&dir);
    let mut guard = common::ServerGuard::new(child);
    let mut c = common::Conn::open(port);
    let mut recovered = 0usize;
    for i in 0..KEYS {
        let key = format!("k1026:{i}");
        if let Some(v) = bulk(&c.send(&["GET", &key])) {
            assert_eq!(
                v,
                format!("value-{i}"),
                "{key} came back with the wrong value"
            );
            recovered += 1;
        }
    }
    let dbsize = integer(&c.send(&["DBSIZE"]));
    drop(c);
    guard.kill_now();

    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    let boot2 = log.get(boot1_len as usize..).unwrap_or("");
    let last_resort: Vec<&str> = boot2
        .lines()
        .filter(|l| l.contains("LAST-RESORT"))
        .collect();
    eprintln!("recovered {recovered}/{KEYS} keys, DBSIZE {dbsize}; boot log: {last_resort:#?}");

    assert!(
        recovered > 0,
        "moon#1026: no key came back from the WAL v3 last-resort fallback \
         (DBSIZE {dbsize}); boot log said: {last_resort:#?}"
    );
    assert_eq!(
        dbsize, recovered as i64,
        "DBSIZE must match the keys read back"
    );
    assert_eq!(
        logged_applied(boot2),
        Some(recovered as u64),
        "the boot log must report the KV commands it APPLIED, which is what \
         DBSIZE holds; boot log said: {last_resort:#?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
