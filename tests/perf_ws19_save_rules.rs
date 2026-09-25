//! moon#1232 against a real server: a `--save "<secs> <changes>"` rule fires
//! in the sharded server once both are reached, and not before.
//!
//! Before the fix the sharded auto-save read a counter only the legacy
//! single-listener handler ever incremented, so a rule with a change
//! threshold never fired: `LASTSAVE` stayed where it was and no snapshot was
//! written, however many writes arrived. The trigger now reads
//! `rdb_changes_since_last_save` — the per-shard dirty counts summed on read,
//! the number INFO shows.
//!
//! `--save "1 10"`: nine writes produce no snapshot within a few seconds; ten
//! writes produce one (`LASTSAVE` and `rdb_last_save_time` advance, the status
//! is `ok`, the count returns to 0, and every shard's `shard-N.rrdshard`
//! exists). Runs at `--shards 1` and `--shards 4`. Pin the binary:
//! `MOON_BIN=<moon> cargo test --test perf_ws19_save_rules`.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "1 10",
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

fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

fn lastsave(c: &mut Conn) -> u64 {
    let reply = c.send(&["LASTSAVE"]);
    reply
        .trim()
        .trim_start_matches(':')
        .parse()
        .unwrap_or_else(|_| panic!("LASTSAVE reply {reply:?}"))
}

/// Every `shard-N.rrdshard` under `dir`, recursively.
fn snapshot_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(p: &std::path::Path, acc: &mut Vec<std::path::PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for entry in rd.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("shard-") && n.ends_with(".rrdshard"))
                {
                    acc.push(path);
                }
            }
        }
    }
    let mut acc = Vec::new();
    walk(dir, &mut acc);
    acc
}

fn write(c: &mut Conn, from: usize, n: usize) {
    for i in from..from + n {
        // Distinct keys, so every shard can own some at --shards 4.
        let reply = c.send(&["SET", &format!("save-rule:{i}"), "v"]);
        assert!(reply.starts_with("+OK"), "SET refused: {reply}");
    }
}

fn save_rule_fires_at_its_change_count(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws19-1232-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    let started_at = lastsave(&mut c);

    // Nine changes: under the rule's threshold. Several ticks of the
    // one-second auto-save timer pass.
    write(&mut c, 0, 9);
    assert_eq!(
        info_field(&mut c, "rdb_changes_since_last_save"),
        "9",
        "fixture: one SET is one change"
    );
    std::thread::sleep(Duration::from_millis(3500));
    assert_eq!(
        lastsave(&mut c),
        started_at,
        "--shards {shards}: nine changes must not trigger a \"1 10\" rule"
    );
    assert!(
        snapshot_files(&dir).is_empty(),
        "--shards {shards}: a snapshot was written for nine changes: {:?}",
        snapshot_files(&dir)
    );

    // The tenth change: a snapshot within a few seconds.
    write(&mut c, 9, 1);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let saved =
            lastsave(&mut c) != started_at && info_field(&mut c, "rdb_bgsave_in_progress") == "0";
        if saved {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "--shards {shards}: ten changes with --save \"1 10\" produced no snapshot in 15 s \
             (LASTSAVE still {started_at}, rdb_changes_since_last_save:{}, \
             rdb_bgsave_in_progress:{})",
            info_field(&mut c, "rdb_changes_since_last_save"),
            info_field(&mut c, "rdb_bgsave_in_progress"),
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "ok");
    assert_eq!(
        info_field(&mut c, "rdb_last_save_time"),
        lastsave(&mut c).to_string()
    );
    assert_eq!(
        info_field(&mut c, "rdb_changes_since_last_save"),
        "0",
        "a successful save resets the count"
    );
    let files = snapshot_files(&dir);
    assert_eq!(
        files.len(),
        shards,
        "--shards {shards}: one snapshot file per shard: {files:?}"
    );

    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn save_rule_fires_at_its_change_count_one_shard() {
    save_rule_fires_at_its_change_count(1);
}

#[test]
fn save_rule_fires_at_its_change_count_four_shards() {
    save_rule_fires_at_its_change_count(4);
}
