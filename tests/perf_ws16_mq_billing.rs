//! moon#1250, against a real server: `MQ PUSH` is charged to `used_memory`,
//! so `maxmemory` binds a message queue the way it binds `XADD`.
//!
//! moon#1163 made every stream mutation keep an unbilled byte delta that the
//! X* commands drain into `used_memory` (`Stream::take_unbilled`). The owner-
//! side MQ subcommands never drained it, and MQ ran no eviction gate at all:
//! under `--maxmemory 4mb noeviction` all 20,000 pushes of 100 B were
//! accepted while `MEMORY USAGE` of the queue passed 5 MB and `used_memory`
//! grew by a few hundred bytes (the reviewer's proof,
//! `mq_push_is_charged_so_maxmemory_binds`).
//!
//! Ports come from `common::spawn_listening` (the repo's collision-safe
//! reservation, a cross-process lock per port), not a fixed range.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws16_mq_billing`.

#![allow(clippy::unwrap_used)]

mod common;

use common::{Conn, ServerGuard};

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
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
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--maxmemory",
                "4mb",
                "--maxmemory-policy",
                "noeviction",
                "--disk-offload",
                "disable",
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

fn used_memory(c: &mut Conn) -> i64 {
    let info = c.send(&["INFO", "memory"]);
    info.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no used_memory in {info:.300}"))
}

fn memory_usage(c: &mut Conn, key: &str) -> i64 {
    let reply = c.send(&["MEMORY", "USAGE", key]);
    reply
        .trim()
        .strip_prefix(':')
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("MEMORY USAGE {key}: {reply:?}"))
}

const PUSHES: usize = 20_000;

fn mq_push_is_charged_so_maxmemory_binds(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws16-mq-billing-s{shards}"));
    let (_server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["MQ", "CREATE", "q"]), "+OK\r\n");
    let before = used_memory(&mut c);
    let value = "v".repeat(100);
    let mut accepted = 0usize;
    let mut refusal = None;
    for _ in 0..PUSHES {
        let r = c.send(&["MQ", "PUSH", "q", "f", &value]);
        if r.starts_with('-') {
            refusal = Some(r);
            break;
        }
        accepted += 1;
    }
    let usage = memory_usage(&mut c, "q");
    let grown = used_memory(&mut c) - before;
    eprintln!(
        "--shards {shards}: accepted {accepted} MQ PUSH, refusal {refusal:?}, \
         MEMORY USAGE q {usage}, used_memory grew {grown}"
    );
    assert!(
        refusal.as_deref().is_some_and(|r| r.starts_with("-OOM")),
        "--shards {shards}: --maxmemory 4mb noeviction accepted {accepted} of {PUSHES} \
         MQ PUSHes of 100 B (MEMORY USAGE q {usage}, used_memory +{grown}) — the queue \
         is never charged, or MQ runs no eviction gate; last reply {refusal:?}"
    );
    assert!(
        accepted < PUSHES * 3 / 4,
        "--shards {shards}: the refusal must arrive well before the end: {accepted}"
    );
    assert!(
        grown * 2 >= usage && grown <= usage * 2,
        "--shards {shards}: used_memory must track the queue: +{grown} for MEMORY USAGE {usage}"
    );
    // ACK/POP only shrink or keep the queue: never refused.
    let popped = c.send(&["MQ", "POP", "q", "COUNT", "1"]);
    assert!(
        !popped.starts_with('-'),
        "MQ POP refused over the limit: {popped}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn mq_push_is_charged_so_maxmemory_binds_single_shard() {
    mq_push_is_charged_so_maxmemory_binds(1);
}

#[test]
fn mq_push_is_charged_so_maxmemory_binds_four_shards() {
    mq_push_is_charged_so_maxmemory_binds(4);
}
