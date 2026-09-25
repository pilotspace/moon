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
    let churn = |c: &mut Conn, q: &str| -> (i64, i64) {
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

// ── moon#1261: the MqPop apply (WAL replay, replica) bills the PEL ──────────

/// Every stream id (`<ms>-<seq>`) in a reply, in order.
fn ids_in(reply: &str) -> Vec<String> {
    reply
        .split("\r\n")
        .filter(|l| {
            let mut it = l.splitn(2, '-');
            matches!((it.next(), it.next()), (Some(a), Some(b))
                if !a.is_empty() && !b.is_empty()
                    && a.bytes().all(|c| c.is_ascii_digit())
                    && b.bytes().all(|c| c.is_ascii_digit()))
        })
        .map(str::to_string)
        .collect()
}

fn last_delivered(c: &mut Conn, q: &str) -> String {
    let r = c.send(&["XINFO", "GROUPS", q]);
    let mut lines = r.split("\r\n");
    while let Some(l) = lines.next() {
        if l == "last-delivered-id" {
            let _len = lines.next();
            return lines.next().unwrap_or_default().to_string();
        }
    }
    panic!("no last-delivered-id in {r:?}");
}

fn pending_ids(c: &mut Conn, q: &str) -> Vec<String> {
    ids_in(&c.send(&["XPENDING", q, "__mq_consumers", "-", "+", "1000"]))
}

/// Make `replica` a replica of `master_port` and wait for the link.
fn replicate(replica: &mut Conn, master_port: u16) {
    assert!(
        replica
            .send(&["REPLICAOF", "127.0.0.1", &master_port.to_string()])
            .starts_with("+OK")
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !replica
        .send(&["INFO", "replication"])
        .contains("master_link_status:up")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the replica link never came up"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// 600 pushes of 32 B, then 500 x (POP COUNT 1, ACK): each POP claims
/// 1 + MAXDELIVERY and releases the surplus.
fn churn(c: &mut Conn) {
    assert_eq!(c.send(&["MQ", "CREATE", "q"]), "+OK\r\n");
    let v = "v".repeat(32);
    for _ in 0..600 {
        assert!(c.send(&["MQ", "PUSH", "q", "f", &v]).starts_with('$'));
    }
    for _ in 0..500 {
        let id = ids_in(&c.send(&["MQ", "POP", "q", "COUNT", "1"]))
            .pop()
            .expect("popped id");
        assert_eq!(c.send(&["MQ", "ACK", "q", &id]), ":1\r\n");
    }
}

/// moon#1261 (review 5 proof, adopted): `apply_mq_pop` rebuilt the PEL by
/// hand, untracked, while `apply_mq_ack` credits through `Stream::xack`. So
/// every replayed POP+ACK credited ~191 B that was never charged, and the
/// queue's bill drained toward 0 on the master's own restart: MEMORY USAGE
/// 124,097 live, 28,421 after WAL replay.
#[test]
fn a_restart_bills_a_churned_queue_as_the_live_server_did() {
    let dir = common::unique_test_dir("ws16-mq-replay-bill");
    let (mut server, port) = spawn_with(&dir, 1, "yes");
    let mut c = Conn::open(port);
    churn(&mut c);
    let before = memory_usage(&mut c, "q");
    // The WAL tick has written everything.
    std::thread::sleep(std::time::Duration::from_millis(1500));
    server.kill_now();
    common::wait_for_port_down(port);
    let (server2, port2) = spawn_with(&dir, 1, "yes");
    let mut c2 = Conn::open(port2);
    assert_eq!(c2.send(&["XLEN", "q"]), ":600\r\n", "replayed");
    let after = memory_usage(&mut c2, "q");
    eprintln!("MEMORY USAGE q: live {before}, after WAL replay {after}");
    assert_eq!(
        after, before,
        "WAL replay of 500 POP+ACK bills the queue at {after} B, the live server at {before} B"
    );
    drop(server2);
    let _ = std::fs::remove_dir_all(&dir);
}

/// moon#1261 on a replica (review 5 proof, adopted): master 124,097 B,
/// replica 28,421 B for the same queue.
#[test]
#[cfg_attr(
    not(feature = "runtime-monoio"),
    ignore = "needs a replica, which needs a runtime-monoio master (PSYNC)"
)]
fn a_replica_bills_a_churned_queue_as_its_master_does() {
    let dir = common::unique_test_dir("ws16-mq-replica-bill");
    let (master, mport) = spawn_with(&dir.join("m"), 1, "no");
    let (replica, rport) = spawn_with(&dir.join("r"), 1, "no");
    let mut m = Conn::open(mport);
    let mut r = Conn::open(rport);
    replicate(&mut r, mport);
    churn(&mut m);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !pending_ids(&mut r, "q").is_empty()
        || last_delivered(&mut r, "q") != last_delivered(&mut m, "q")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the replica never caught up"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let on_master = memory_usage(&mut m, "q");
    let on_replica = memory_usage(&mut r, "q");
    eprintln!("MEMORY USAGE q: master {on_master}, replica {on_replica}");
    assert_eq!(
        on_replica, on_master,
        "the replica bills the queue at {on_replica} B, its master at {on_master} B"
    );
    drop(replica);
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// moon#1250 / moon#1261 (review 5 proof Q1, adopted without its MULTI leg,
/// moon#1262): a POP claims COUNT + MAXDELIVERY and releases the surplus.
/// Pipelined POPs deliver every entry once, in order, and the MqPop record
/// carries the post-release set: a replica and a kill -9 restart (WAL
/// replay) hold exactly the master's PEL and `last_delivered_id`, and each
/// serves the rest once, in order.
#[test]
#[cfg_attr(
    not(feature = "runtime-monoio"),
    ignore = "needs a replica, which needs a runtime-monoio master (PSYNC)"
)]
fn a_pop_surplus_release_agrees_on_master_replica_and_restart() {
    let dir = common::unique_test_dir("ws16-mq-pop-prop");
    let (mut master, mport) = spawn_with(&dir.join("m"), 1, "yes");
    let (replica, rport) = spawn_with(&dir.join("r"), 1, "no");
    let mut m = Conn::open(mport);
    let mut r = Conn::open(rport);
    replicate(&mut r, mport);
    assert_eq!(m.send(&["MQ", "CREATE", "q"]), "+OK\r\n"); // MAXDELIVERY 3
    let mut pushed = Vec::new();
    for i in 0..12 {
        let reply = m.send(&["MQ", "PUSH", "q", "f", &format!("m{i}")]);
        pushed.push(ids_in(&reply).pop().expect("push id"));
    }
    // Two POPs in one pipeline (each claims 2 + 3, releases 3).
    let delivered = ids_in(&m.pipeline(&[
        &["MQ", "POP", "q", "COUNT", "2"],
        &["MQ", "POP", "q", "COUNT", "2"],
    ]));
    let k = delivered.len();
    assert_eq!(delivered, pushed[..k].to_vec(), "delivered once, in order");
    let m_pending = pending_ids(&mut m, "q");
    let m_last = last_delivered(&mut m, "q");
    assert_eq!(m_pending, delivered, "master PEL == what was delivered");
    assert_eq!(
        m_last,
        pushed[k - 1],
        "master cursor rewound to the last kept"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let rp = pending_ids(&mut r, "q");
        let rl = last_delivered(&mut r, "q");
        if rp == m_pending && rl == m_last {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica diverged: PEL {rp:?} last {rl} vs master PEL {m_pending:?} last {m_last}"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    std::thread::sleep(std::time::Duration::from_millis(1500));
    master.kill_now();
    common::wait_for_port_down(mport);
    let (master2, mport2) = spawn_with(&dir.join("m"), 1, "yes");
    let mut m2 = Conn::open(mport2);
    assert_eq!(pending_ids(&mut m2, "q"), m_pending, "restart PEL");
    assert_eq!(last_delivered(&mut m2, "q"), m_last, "restart cursor");
    let rest = ids_in(&m2.send(&["MQ", "POP", "q", "COUNT", "100"]));
    assert_eq!(
        rest,
        pushed[k..].to_vec(),
        "restart: the rest, once, in order"
    );

    assert!(r.send(&["REPLICAOF", "NO", "ONE"]).starts_with("+OK"));
    let rest_r = ids_in(&r.send(&["MQ", "POP", "q", "COUNT", "100"]));
    assert_eq!(rest_r, pushed[k..].to_vec(), "promoted replica: the rest");
    drop(master2);
    drop(replica);
    let _ = std::fs::remove_dir_all(&dir);
}

const MAX_U64: &str = "18446744073709551615";

/// Review 6 (F end to end, and N2; the reviewer's proof, adopted): at the
/// last possible stream ID, `XADD *`, `XADD <ms>-*` and `MQ PUSH` answer an
/// error and the server — a DEBUG build here, which panics on integer
/// overflow — survives each. `XADD <ms>-*` computed `last_id.seq + 1` and
/// panicked shard 0 (the server then stopped accepting connections).
#[test]
fn every_auto_id_form_is_refused_at_the_last_possible_id() {
    let dir = common::unique_test_dir("ws16-max-stream-id");
    let (server, port) = spawn_with(&dir, 1, "no");
    let mut c = Conn::open(port);
    let top = format!("{MAX_U64}-{MAX_U64}");
    assert!(c.send(&["XADD", "s", &top, "f", "v"]).starts_with('$'));
    assert_eq!(c.send(&["MQ", "CREATE", "q"]), "+OK\r\n");
    assert!(c.send(&["XADD", "q", &top, "f", "v"]).starts_with('$'));
    let alive = |port: u16| {
        std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
            && Conn::open(port).send(&["PING"]).starts_with("+PONG")
    };
    let star = c.send(&["XADD", "s", "*", "f", "v"]);
    assert!(alive(port), "XADD * killed the server");
    let push = c.send(&["MQ", "PUSH", "q", "f", "v"]);
    assert!(alive(port), "MQ PUSH killed the server");
    assert!(star.starts_with("-ERR"), "XADD *: {star:?}");
    assert!(push.starts_with("-ERR"), "MQ PUSH: {push:?}");
    assert_eq!(c.send(&["XLEN", "q"]), ":1\r\n");
    let mut c2 = Conn::open(port);
    c2.sock
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .unwrap();
    let partial = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        c2.send(&["XADD", "s", &format!("{MAX_U64}-*"), "f", "v"])
    }));
    let survived = alive(port);
    assert!(
        matches!(&partial, Ok(r) if r.starts_with("-ERR The ID specified in XADD is equal or smaller"))
            && survived,
        "XADD <ms>-* at the last possible ID must be refused, not panic: reply {partial:?}, \
         server alive {survived}"
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}
