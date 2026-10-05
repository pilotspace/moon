//! R2b W2 (moon#1300): a `TXN` keeps ONE copy of a written key's
//! pre-transaction value.
//!
//! The key's hold (moon#1299) keeps its pre-transaction image for every
//! snapshot (moon#1300 F3), and the undo log used to keep its own deep copy
//! beside it — and another one for every later write of the key. One `TXN`
//! `HSET` of a single field of a large hash cost two full copies of the hash
//! for the transaction's whole life (+157 MB for 500k fields). Now the hold
//! owns the only copy and the abort restores from it.
//!
//! Measured on a real server (`MOON_BIN` pinned) through `/proc/<pid>/status`
//! (Linux only):
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test txn_hold_one_copy_1300 -- --include-ignored
//! ```

#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::Conn;

const FIELDS: usize = 300_000;
const BATCH: usize = 1_000;

fn rss_mb(pid: u32) -> f64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).expect("proc status");
    let kb: f64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .expect("VmRSS");
    kb / 1024.0
}

/// RSS once the server has settled (a write's transient buffers returned).
fn settled_rss_mb(c: &mut Conn, pid: u32) -> f64 {
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
    std::thread::sleep(Duration::from_millis(500));
    rss_mb(pid)
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_write_to_a_large_hash_keeps_one_copy_of_it() {
    let dir = common::unique_test_dir("txn-1300-one-copy");
    let dir_s = dir.to_str().expect("utf8 dir").to_string();
    let bin = common::find_moon_binary();
    let (mut guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                &dir_s,
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
            ])
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    });
    let pid = guard.id();
    let mut c = Conn::open(port);
    let r0 = settled_rss_mb(&mut c, pid);
    let value = "v".repeat(20);
    for start in (0..FIELDS).step_by(BATCH) {
        let mut parts: Vec<String> = vec!["HSET".into(), "h".into()];
        for j in start..start + BATCH {
            parts.push(format!("f{j}"));
            parts.push(value.clone());
        }
        let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
        assert_eq!(c.send(&parts), format!(":{BATCH}\r\n"));
    }
    let r1 = settled_rss_mb(&mut c, pid);
    let hash = r1 - r0;
    assert!(
        hash > 20.0,
        "precondition: the hash costs {hash:.1} MB of RSS"
    );

    let mut t = Conn::open(port);
    assert_eq!(t.send(&["TXN", "BEGIN"]), "+OK\r\n");
    assert_eq!(t.send(&["HSET", "h", "f1", "x"]), ":0\r\n");
    let r2 = settled_rss_mb(&mut c, pid);
    assert_eq!(t.send(&["HSET", "h", "f2", "y"]), ":0\r\n");
    assert_eq!(t.send(&["HSET", "h", "f3", "z"]), ":0\r\n");
    let r3 = settled_rss_mb(&mut c, pid);
    let (first, later) = (r2 - r1, r3 - r2);
    eprintln!(
        "RSS MB: base {r0:.1}, hash {hash:.1}, first TXN write +{first:.1}, \
         two more writes +{later:.1}"
    );
    // Measured: one copy 0.84x the hash's RSS (a clone is tighter than the
    // grown original), two copies 1.52x.
    assert!(
        first < 1.2 * hash,
        "the first TXN write of the hash kept {first:.1} MB for a {hash:.1} MB hash: \
         more than one copy of its pre-transaction value"
    );
    assert!(
        later < 0.3 * hash,
        "later TXN writes of a held key kept {later:.1} MB for a {hash:.1} MB hash: \
         they copied its value again"
    );

    // The abort still restores the pre-transaction value.
    assert_eq!(t.send(&["TXN", "ABORT"]), "+OK\r\n");
    assert_eq!(
        c.send(&["HGET", "h", "f1"]),
        format!("${}\r\n{value}\r\n", value.len())
    );
    assert_eq!(
        c.send(&["HGET", "h", "f3"]),
        format!("${}\r\n{value}\r\n", value.len())
    );
    assert_eq!(c.send(&["HLEN", "h"]), format!(":{FIELDS}\r\n"));
    drop((c, t));
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}
