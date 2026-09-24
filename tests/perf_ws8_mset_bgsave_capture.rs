//! moon#1228 item 1, end to end (the WS12 pattern of
//! `perf_ws12_bgsave_split.rs`): spanning `MSET`s that overwrite keys while a
//! BGSAVE epoch is still writing them must leave the snapshot holding every
//! key's EPOCH-START value. The coordinator's local leg wrote through a
//! capture-free path, so on `ae21476` the restored snapshot holds post-epoch
//! values for keys owned by the connection's shard.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_mset_bgsave_capture`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

const SHARDS: usize = 4;

fn spawn(dir: &std::path::Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &SHARDS.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
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
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn key(j: u64) -> String {
    format!("pre:{j:08}")
}

/// Load `n` keys `pre:<j>` = `v<j>`, pipelined.
fn preload(c: &mut Conn, n: u64) {
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
        let mut out = Vec::new();
        for j in i..end {
            out.extend_from_slice(&encode(&["SET", &key(j), &format!("v{j}")]));
        }
        c.sock.write_all(&out).unwrap();
        let reply = c.read_replies((end - i) as usize);
        assert!(!reply.contains('-'), "preload refused: {reply:.200}");
        i = end;
    }
}

fn bgsave_in_progress(c: &mut Conn) -> bool {
    c.send(&["INFO", "persistence"])
        .lines()
        .any(|l| l.trim() == "rdb_bgsave_in_progress:1")
}

fn last_bgsave_status(c: &mut Conn) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix("rdb_last_bgsave_status:"))
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

/// Keys whose restored value is not their epoch-start `v<j>`.
fn wrong_pre_keys(c: &mut Conn, n: u64) -> Vec<String> {
    let mut wrong = Vec::new();
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
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
            if body != format!("v{j}") && wrong.len() < 10_000 {
                wrong.push(format!("{}={body}", key(j)));
            }
        }
        i = end;
    }
    wrong
}

#[test]
fn spanning_mset_during_bgsave_keeps_epoch_start_values() {
    let dir = common::unique_test_dir("ws8-mset-bgsave");
    // Enough per shard that the epoch outlasts the writer's start-up by a
    // wide margin (a budgeted tick writes ~1,024 entries per shard).
    let n = 100_000u64 * SHARDS as u64;
    let (mut server, port) = spawn(&dir);
    let mut c = Conn::open(port);
    preload(&mut c, n);

    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    // Let every shard pick the epoch up (1 ms tick) before the first
    // overwrite: a write that lands BEFORE a shard arms is legitimately in
    // that shard's image, and would read as a false failure below.
    std::thread::sleep(Duration::from_millis(30));

    // Overwrite the keyspace with spanning MSETs until the epoch ends.
    let mut probe = Conn::open(port);
    assert!(
        bgsave_in_progress(&mut probe),
        "the epoch ended before the writer started — raise the key count"
    );
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut sent = 0u64;
    let mut during = 0u64;
    let mut j = 0u64;
    'write: while Instant::now() < deadline {
        for _ in 0..16 {
            let mut cmd: Vec<String> = vec!["MSET".into()];
            for _ in 0..32 {
                cmd.push(key(j % n));
                cmd.push("new".into());
                j += 1;
            }
            let parts: Vec<&str> = cmd.iter().map(String::as_str).collect();
            assert_eq!(c.send(&parts), "+OK\r\n");
            sent += 1;
        }
        if !bgsave_in_progress(&mut probe) {
            break 'write;
        }
        during = sent;
    }
    assert_eq!(last_bgsave_status(&mut probe), "ok", "BGSAVE failed");
    assert!(
        during >= 16,
        "only {during} spanning MSETs landed during the BGSAVE — the epoch saw no overwrite"
    );

    // Crash, restart from the snapshot alone.
    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir);
    let mut c2 = Conn::open(port2);
    let wrong = wrong_pre_keys(&mut c2, n);
    assert!(
        wrong.is_empty(),
        "{} of {n} keys hold a post-epoch value after restoring the BGSAVE taken \
         under {during} spanning MSETs (first: {:?})",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
    let _ = std::fs::remove_dir_all(&dir);
}
