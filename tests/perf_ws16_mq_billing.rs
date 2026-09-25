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
    spawn_with(dir, shards, "yes")
}

fn spawn_with(dir: &std::path::Path, shards: usize, appendonly: &str) -> (ServerGuard, u16) {
    // The replication backlog is part of `used_memory` and grows with every
    // logged MQ record until it reaches its size: a small one is full after
    // the preload, so it cannot move the figure a test compares.
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
                appendonly,
                "--maxmemory",
                "4mb",
                "--maxmemory-policy",
                "noeviction",
                "--disk-offload",
                "disable",
                "--repl-backlog-size",
                "16384",
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
    let (server, port) = spawn(&dir, shards);
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
    drop(server);
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

/// `used_memory` once it stops moving. Read right after a burst of writes it
/// can still lag them (measured: the first window after 1,200 pushes carried
/// 3–84 KB that belonged to the pushes; a 200 ms pause removed it).
fn settled_used_memory(c: &mut Conn) -> i64 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    std::thread::sleep(std::time::Duration::from_millis(250));
    let mut last = used_memory(c);
    loop {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let now = used_memory(c);
        if now == last || std::time::Instant::now() > deadline {
            return now;
        }
        last = now;
    }
}

/// The id of the one entry an `MQ POP ... COUNT 1` reply carries.
fn popped_id(reply: &str) -> String {
    // *1 / *2 / $<n> / <id> / ...
    reply
        .split("\r\n")
        .nth(3)
        .unwrap_or_else(|| panic!("MQ POP reply {reply:?}"))
        .to_string()
}

/// moon#1250: POP/ACK churn keeps the charge equal to the queue's true size.
/// `MQ POP` over-claims COUNT + MAXDELIVERY entries and releases the surplus;
/// the release used to bypass the stream's byte tracking, so every POP left
/// the released entries' PEL bytes charged to `used_memory` although they
/// were gone (+286,500 B over 500 POP+ACK pairs in-process), until a default
/// queue hit `-OOM` while MEMORY USAGE stayed small.
///
/// Judged against a `MAXDELIVERY 0` control queue (no over-claim) under the
/// same churn, because `used_memory` also moves with things that are not the
/// queue (the AOF / replication buffers of 1,000 commands).
fn pop_ack_churn_keeps_the_charge_exact(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws16-mq-churn-s{shards}"));
    let (server, port) = spawn_with(&dir, shards, "no");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["MQ", "CREATE", "q"]), "+OK\r\n");
    assert_eq!(
        c.send(&["MQ", "CREATE", "ctl", "MAXDELIVERY", "0"]),
        "+OK\r\n"
    );
    for q in ["q", "ctl"] {
        for _ in 0..600 {
            let r = c.send(&["MQ", "PUSH", q, "f", "v"]);
            assert!(r.starts_with('$'), "MQ PUSH {q}: {r:?}");
        }
    }
    let mut churn = |c: &mut Conn, q: &str| -> (i64, i64) {
        let used_before = settled_used_memory(c);
        let usage_before = memory_usage(c, q);
        for _ in 0..500 {
            let id = popped_id(&c.send(&["MQ", "POP", q, "COUNT", "1"]));
            assert_eq!(c.send(&["MQ", "ACK", q, &id]), ":1\r\n");
        }
        (
            used_memory(c) - used_before,
            memory_usage(c, q) - usage_before,
        )
    };
    let (charged, real) = churn(&mut c, "q");
    let (charged_ctl, real_ctl) = churn(&mut c, "ctl");
    eprintln!(
        "--shards {shards}: 500 POP+ACK: default queue used_memory {charged:+} (MEMORY USAGE \
         {real:+}); MAXDELIVERY 0 control used_memory {charged_ctl:+} (MEMORY USAGE {real_ctl:+})"
    );
    assert!(
        (charged - charged_ctl).abs() <= 4096,
        "--shards {shards}: 500 POP+ACK charged {charged} B on a default queue but \
         {charged_ctl} B on a MAXDELIVERY 0 control (true sizes moved {real} / {real_ctl} B) — \
         the released surplus of every POP stays charged"
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn pop_ack_churn_keeps_the_charge_exact_single_shard() {
    pop_ack_churn_keeps_the_charge_exact(1);
}

#[test]
fn pop_ack_churn_keeps_the_charge_exact_four_shards() {
    pop_ack_churn_keeps_the_charge_exact(4);
}
