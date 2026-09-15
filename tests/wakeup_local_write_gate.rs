//! The local (connection-owned) write path must wake a producer's blocked
//! consumer — and must keep doing so after the database guard in front of
//! `wake_producer` is gated (moon#942, the W7 hoist).
//!
//! # Why this suite exists
//!
//! `handler_monoio`'s write tail acquires a SECOND exclusive database guard
//! after `dispatch`, purely so it can hand a `&mut Database` to
//! `wake_producer`. `wake_producer`'s own first line is
//! `producer_family(cmd)?`, which is `None` for every write that is not
//! `LPUSH`/`RPUSH`/`LMOVE`/`RPOPLPUSH`/`ZADD`/`XADD` — so for `INCR`, `SADD`
//! and `HSET` the guard is taken, nothing happens, and it is dropped. Moving
//! that predicate OUTSIDE the guard is safe **only while the predicate stays
//! exactly the one `wake_producer` itself uses**.
//!
//! Getting that wrong has a name here. moon#595: the local gate omitted
//! `XADD`, so a stream reader blocked on a key THIS shard owned was never
//! woken by a local write — while the same `XADD` arriving over the SPSC mesh
//! woke it. Routing-dependent, so it read as a flake rather than as a bug.
//! moon#623 then collapsed ten open-coded copies of the test into
//! `is_producer` / `producer_family` so a future producer is taught the
//! mapping once, in one place.
//!
//! # What is asserted, and why each half is load-bearing
//!
//! Both the VALUE and the LATENCY. Asserting only the value passes against a
//! server that merely timed out slowly enough to see the write afterwards;
//! asserting only the latency passes against one that answered a premature
//! null. That is the moon#606 lesson and it applies here unchanged.
//!
//! # Why both shard counts
//!
//! At `--shards 1` every key is owned by the writer's own shard, so the local
//! write tail — the code under change — is taken on **every** trial. At
//! `--shards 4` roughly three quarters of the trials route over the SPSC mesh
//! instead and exercise `spsc_handler`'s wake sites. moon#595 was visible at
//! one of the two and invisible at the other, which is precisely why a
//! `--shards 1`-only suite would not have caught it.
//!
//! # Why plain commands, and not `MULTI`/`EXEC` or `SET`
//!
//! `tests/blocking_exec_wakeup.rs` already covers the `EXEC` executor's own
//! wake hook (moon#606). This suite must reach the *connection* write tail in
//! `handler_monoio`, which only a plain, non-transactional write enters. A
//! bare `SET` would not do either: it is served by `try_inline_dispatch` and
//! never reaches this arm at all.

mod common;

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

/// Distinct keys per family. At `--shards 4` a key is remote with p≈0.75, so
/// the chance that all 8 of a family's keys land on the connection's own
/// shard — and vacuously test only one route — is under 2e-5.
const TRIALS: usize = 8;
/// The blocked client's own timeout, as a RESP argument. Long enough that a
/// woken reply and a timed-out one are far apart in the measurement.
const BLOCK_SECS: &str = "5";
/// A reply later than this did not come from the write.
const WOKEN_WITHIN: Duration = Duration::from_secs(2);
/// How long a parked client is given to reach its blocked state before the
/// waking write is issued.
const SETTLE: Duration = Duration::from_millis(400);
/// The control waiter's timeout. Short, because the control's whole point is
/// that it is NOT woken and must run to its own deadline.
const CONTROL_BLOCK_SECS: &str = "2";
/// Floor the control's elapsed time must clear. Comfortably under
/// `CONTROL_BLOCK_SECS` so ordinary scheduling jitter cannot fail it, and far
/// enough above `SETTLE` that a spurious wake cannot pass it.
const CONTROL_FLOOR: Duration = Duration::from_millis(1_500);

fn spawn(shards: &str) -> (ServerGuard, u16) {
    let dir = common::unique_test_dir(&format!("wakeup-gate-s{shards}"));
    std::fs::create_dir_all(&dir).expect("create test dir");
    let bin = find_moon_binary();
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf8 dir"),
            ])
            .stderr(server_stderr(&dir))
            .spawn()
            .expect("moon spawns")
    });
    // `spawn_listening_guarded` proves the port ACCEPTS; it does not prove the
    // server ANSWERS. A suite that parks a blocking read against a
    // not-yet-ready server measures start-up, not wakeups. Raw socket rather
    // than `Conn::open`, which panics on a refused connect.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return (guard, port);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("moon never answered PING on port {port} (--shards {shards})");
}

/// Park `argv` on its own connection and hand back a join handle.
///
/// A fresh connection per waiter is mandatory: a blocking reply desynchronises
/// a shared probe connection, after which every later assertion is fiction.
fn park(port: u16, argv: &[&str]) -> std::thread::JoinHandle<(Duration, String)> {
    let owned: Vec<String> = argv.iter().map(|s| (*s).to_string()).collect();
    std::thread::spawn(move || {
        let mut c = Conn::open(port);
        let start = Instant::now();
        let reply = c.send(&owned.iter().map(String::as_str).collect::<Vec<_>>());
        (start.elapsed(), reply)
    })
}

/// Park a waiter on each of `TRIALS` distinct keys, wake each with ONE plain
/// producer command on a fresh connection, and append every late-or-wrong
/// trial to `failures`.
fn each_trial(
    port: u16,
    tag: &str,
    expect: &str,
    waiter: impl Fn(&str) -> Vec<String>,
    producer: impl Fn(&str) -> Vec<String>,
    failures: &mut Vec<String>,
) {
    for i in 0..TRIALS {
        let key = format!("{tag}{i}");
        let argv = waiter(&key);
        let handle = park(port, &argv.iter().map(String::as_str).collect::<Vec<_>>());
        std::thread::sleep(SETTLE);

        let pargv = producer(&key);
        let mut pc = Conn::open(port);
        let wrote = pc.send(&pargv.iter().map(String::as_str).collect::<Vec<_>>());
        assert!(
            !wrote.starts_with('-'),
            "the waking write {pargv:?} itself failed: {wrote:?}"
        );

        let (elapsed, reply) = handle.join().expect("waiter thread");
        if !reply.contains(expect) {
            failures.push(format!(
                "  {tag}: {key} answered {reply:?} after {elapsed:?} — expected {expect:?}"
            ));
        } else if elapsed > WOKEN_WITHIN {
            failures.push(format!(
                "  {tag}: {key} answered correctly but {elapsed:?} late — that is its \
                 own {BLOCK_SECS}s timeout expiring, not the write waking it"
            ));
        }
    }
}

/// The control. A non-producer write must wake nothing AND must not error —
/// the two things a mis-gated hoist breaks in opposite directions.
///
/// Without it, gating the guard on `false` (never wake) and leaving it
/// unconditional (today) are distinguishable, but gating it correctly and
/// gating it too WIDELY are not: a gate that let `INCR` through would still
/// pass all three producer cases. This is the half that prices the other
/// direction.
fn non_producers_wake_nothing(port: u16, tag: &str, failures: &mut Vec<String>) {
    let idle = format!("{tag}idle");
    let handle = park(port, &["BLPOP", idle.as_str(), CONTROL_BLOCK_SECS]);
    std::thread::sleep(SETTLE);

    // Every non-producer family from moon#942, on keys the waiter is not
    // watching. Each must succeed on its own terms.
    let mut c = Conn::open(port);
    for i in 0..TRIALS {
        for argv in [
            vec!["INCR".to_string(), format!("{tag}n{i}")],
            vec!["SADD".to_string(), format!("{tag}s{i}"), "m".into()],
            vec![
                "HSET".to_string(),
                format!("{tag}h{i}"),
                "f".into(),
                "v".into(),
            ],
        ] {
            let reply = c.send(&argv.iter().map(String::as_str).collect::<Vec<_>>());
            if reply.starts_with('-') {
                failures.push(format!("  {tag}: non-producer {argv:?} errored: {reply:?}"));
            }
        }
    }

    let (elapsed, reply) = handle.join().expect("control waiter thread");
    // RESP2 timeout is `*-1`; RESP3 would be `_`. Accept either rather than
    // pinning a protocol this suite does not negotiate.
    if !reply.starts_with("*-1") && !reply.starts_with('_') {
        failures.push(format!(
            "  {tag}: a non-producer write woke a BLPOP on an unrelated key — \
             reply {reply:?} after {elapsed:?}"
        ));
    }
    if elapsed < CONTROL_FLOOR {
        failures.push(format!(
            "  {tag}: control BLPOP returned after only {elapsed:?}, well inside its \
             {CONTROL_BLOCK_SECS}s timeout — something answered it"
        ));
    }
}

/// Every family, against one server, reporting all failures at once.
fn all_families(shards: &str) {
    let (_guard, port) = spawn(shards);
    let mut failures: Vec<String> = Vec::new();

    // ── list producer → BLPOP ────────────────────────────────────────────
    each_trial(
        port,
        "wg:lpush:",
        "payload",
        |k| vec!["BLPOP".into(), k.into(), BLOCK_SECS.into()],
        |k| vec!["LPUSH".into(), k.into(), "payload".into()],
        &mut failures,
    );

    // ── zset producer → BZPOPMIN ─────────────────────────────────────────
    each_trial(
        port,
        "wg:zadd:",
        "member",
        |k| vec!["BZPOPMIN".into(), k.into(), BLOCK_SECS.into()],
        |k| vec!["ZADD".into(), k.into(), "1".into(), "member".into()],
        &mut failures,
    );

    // ── stream producer → XREAD BLOCK (the exact moon#595 case) ──────────
    // `$` binds at block time, so the stream must already exist — otherwise
    // the assertion would be about binding, not about waking.
    {
        let mut seed = Conn::open(port);
        for i in 0..TRIALS {
            let reply = seed.send(&["XADD", &format!("wg:xadd:{i}"), "1-1", "seed", "v"]);
            assert!(reply.starts_with('$'), "seed XADD failed: {reply:?}");
        }
    }
    each_trial(
        port,
        "wg:xadd:",
        "woken",
        |k| {
            vec![
                "XREAD".into(),
                "BLOCK".into(),
                "5000".into(),
                "STREAMS".into(),
                k.into(),
                "$".into(),
            ]
        },
        |k| {
            vec![
                "XADD".into(),
                k.into(),
                "7-1".into(),
                "woken".into(),
                "v".into(),
            ]
        },
        &mut failures,
    );

    // ── the control ──────────────────────────────────────────────────────
    non_producers_wake_nothing(port, "wg:ctl:", &mut failures);

    assert!(
        failures.is_empty(),
        "{} wakeup assertion(s) failed at --shards {shards}:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// One shard: the key is ALWAYS owned by the writer's own shard, so every
/// trial takes the local write tail this change edits.
#[test]
fn local_write_tail_wakes_every_producer_family_at_one_shard() {
    all_families("1");
}

/// Four shards: most trials route over the SPSC mesh instead. moon#595 was
/// visible at one shard count and invisible at the other; this is the half
/// that catches a local gate which disagrees with the remote one.
#[test]
fn local_write_tail_wakes_every_producer_family_at_four_shards() {
    all_families("4");
}
