//! moon#1299 R1 F1: a connection that leaves with an open cross-store `TXN`
//! must roll it back and release its key holds — whichever way it leaves.
//!
//! Before: the only rollback sat at the tail of the connection body, and the
//! exits below `return`ed early and skipped it. The TXN's holds then lasted
//! for the life of the process: every other writer of its keys was answered
//! `-TXNCONFLICT`, `FLUSHALL` was refused, `INFO` reported `txn_open:1` for
//! ever, and the TXN's uncommitted write was never rolled back. After: the
//! rollback runs in one exit epilogue every exit passes through
//! (`handler_{monoio,sharded}/exit.rs`).
//!
//! One test per exit class. Each runs on whichever runtime `MOON_BIN` was
//! built with; run the suite once per runtime:
//!
//!   MOON_DISK_FREE_MIN_PCT=0 MOON_BIN=... cargo test --test txn_exit_epilogue_1299 -- --include-ignored
//!
//! Exit classes and where they leaked on `f766fc2`:
//!
//! | class                                   | monoio | tokio |
//! |-----------------------------------------|--------|-------|
//! | protocol fault                          | leaked | clean |
//! | `BLPOP` blocked in the TXN, peer reset  | leaked | leaked|
//! | reply over the output-buffer limit      | clean  | leaked|
//! | `SUBSCRIBE` then `QUIT`                 | leaked | clean |
//! | `SUBSCRIBE` then protocol fault         | leaked | leaked|
//! | `PSYNC` (replica hijack) inside the TXN | leaked | n/a   |
//!
//! Not driven here (no deterministic black-box trigger): tokio's fatal
//! cross-shard reply (a 30 s slot timeout) and a subscriber's push-write
//! error. Both are `return`s of the same body and reach the same epilogue.

mod common;

use std::io::{Read as _, Write as _};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

fn spawn(shards: usize, extra: &[&str]) -> (common::ServerGuard, u16, std::path::PathBuf) {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir(&format!("txn-exit-{shards}"));
    let dir_s = dir.to_str().expect("utf8 dir").to_string();
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--dir",
                &dir_s,
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .env("RUST_LOG", "moon=warn")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["PING"]) == "+PONG\r\n"
        {
            break;
        }
        assert!(Instant::now() < deadline, "server on {port} never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port, dir)
}

fn cleanup(mut guard: common::ServerGuard, dir: std::path::PathBuf) {
    guard.kill_now();
    let _ = std::fs::remove_dir_all(dir);
}

/// A key on the transaction connection's own shard (a TXN refuses writes
/// that route elsewhere, #499). At `--shards 1` every key qualifies.
fn local_key(txn: &mut Conn) -> String {
    for i in 0..256 {
        let key = format!("{{t{i}}}k");
        assert_eq!(txn.send(&["TXN", "BEGIN"]), OK);
        let wrote = txn.send(&["SET", &key, "probe"]);
        assert_eq!(txn.send(&["TXN", "ABORT"]), OK);
        if wrote == OK {
            return key;
        }
    }
    panic!("no key routes to the transaction's shard");
}

fn info_field(c: &mut Conn, field: &str) -> Option<u64> {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
}

/// Open a connection, pick a key on its shard, seed `key = orig`, open a
/// TXN on the connection and write the key inside it. Returns the TXN
/// connection and the key.
fn open_txn_holding(port: u16) -> (Conn, String) {
    let mut a = Conn::open(port);
    let key = local_key(&mut a);
    let key = key.as_str();
    let mut w = Conn::open(port);
    assert_eq!(w.send(&["SET", key, "orig"]), OK);
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", key, "txnval"]), OK);
    let mut other = Conn::open(port);
    assert!(
        other.send(&["SET", key, "x"]).starts_with("-TXNCONFLICT"),
        "precondition: the open TXN holds {key}"
    );
    (a, key.to_string())
}

/// Drain whatever the server still sends, until it closes the socket (or
/// `budget` passes).
fn drain_until_closed(c: &mut Conn, budget: Duration) {
    c.sock
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("timeout");
    let deadline = Instant::now() + budget;
    let mut buf = [0u8; 65536];
    while Instant::now() < deadline {
        match c.sock.read(&mut buf) {
            Ok(0) => return,
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
    }
}

/// Close `c` with a TCP reset instead of a FIN (the peer vanished).
fn reset(c: Conn) {
    socket2::SockRef::from(&c.sock)
        .set_linger(Some(Duration::ZERO))
        .expect("SO_LINGER 0");
    drop(c);
}

/// The TXN connection is gone: its write must be rolled back, its hold
/// released, and nothing may stay open — the key is writable, `FLUSHALL`
/// works and `INFO` reads `txn_open:0` / `txn_held_keys:0`.
fn assert_txn_ended(port: u16, key: &str, class: &str) {
    let mut w = Conn::open(port);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = w.send(&["GET", key]);
        let open = info_field(&mut w, "txn_open");
        let held = info_field(&mut w, "txn_held_keys");
        if value == bulk("orig") && open == Some(0) && held == Some(0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{class}: the TXN outlived its connection — GET {key} = {value:?} \
             (want the pre-TXN \"orig\": the uncommitted write must be rolled back), \
             txn_open = {open:?}, txn_held_keys = {held:?} (want 0 and 0)"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        w.send(&["SET", key, "other"]),
        OK,
        "{class}: the key must be writable once the TXN's connection is gone"
    );
    assert_eq!(
        w.send(&["FLUSHALL"]),
        OK,
        "{class}: FLUSHALL must not be refused by a dead connection's TXN"
    );
}

// ---------------------------------------------------------------------------
// One test per exit class
// ---------------------------------------------------------------------------

fn protocol_fault(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let (mut a, key) = open_txn_holding(port);
    a.sock.write_all(b"*abc\r\n").expect("write");
    drain_until_closed(&mut a, Duration::from_secs(5));
    drop(a);
    assert_txn_ended(port, &key, "protocol fault");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn protocol_fault_ends_the_txn_1_shard() {
    protocol_fault(1);
}

#[test]
#[ignore]
fn protocol_fault_ends_the_txn_4_shards() {
    protocol_fault(4);
}

fn blocked_pop_then_peer_reset(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let (mut a, key) = open_txn_holding(port);
    // An empty list on the TXN's own shard (same hash tag): the pop blocks.
    let tag_end = key.find('}').map_or(0, |i| i + 1);
    let queue = format!("{}emptyq", &key[..tag_end]);
    a.sock
        .write_all(&common::encode(&["BLPOP", &queue, "0"]))
        .expect("write");
    // Parked, not answered.
    a.sock
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    let mut b = [0u8; 64];
    let got = a.sock.read(&mut b);
    assert!(
        matches!(&got, Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
        "precondition: BLPOP on an empty list blocks, got {got:?} {:?}",
        String::from_utf8_lossy(&b)
    );
    reset(a);
    assert_txn_ended(port, &key, "BLPOP blocked in the TXN, peer reset");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn blocked_pop_then_peer_reset_ends_the_txn_1_shard() {
    blocked_pop_then_peer_reset(1);
}

#[test]
#[ignore]
fn blocked_pop_then_peer_reset_ends_the_txn_4_shards() {
    blocked_pop_then_peer_reset(4);
}

#[test]
#[ignore]
fn reply_over_the_output_buffer_limit_ends_the_txn() {
    let (guard, port, dir) = spawn(1, &["--client-output-buffer-limit-normal", "2000"]);
    let big = "x".repeat(5000);
    assert_eq!(Conn::open(port).send(&["SET", "big", &big]), OK);
    let (mut a, key) = open_txn_holding(port);
    // The reply (5 KB) is over the 2000-byte limit: the server drops the
    // connection instead of answering.
    a.sock
        .write_all(&common::encode(&["GET", "big"]))
        .expect("write");
    drain_until_closed(&mut a, Duration::from_secs(5));
    reset(a);
    assert_txn_ended(port, &key, "reply over the output-buffer limit");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn subscribe_then_quit_ends_the_txn() {
    let (guard, port, dir) = spawn(1, &[]);
    let (mut a, key) = open_txn_holding(port);
    let sub = a.send(&["SUBSCRIBE", "ch"]);
    assert!(sub.contains("subscribe"), "SUBSCRIBE: {sub:?}");
    assert_eq!(a.send(&["QUIT"]), OK);
    drain_until_closed(&mut a, Duration::from_secs(5));
    drop(a);
    assert_txn_ended(port, &key, "SUBSCRIBE then QUIT");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn subscribe_then_protocol_fault_ends_the_txn() {
    let (guard, port, dir) = spawn(1, &[]);
    let (mut a, key) = open_txn_holding(port);
    let sub = a.send(&["SUBSCRIBE", "ch"]);
    assert!(sub.contains("subscribe"), "SUBSCRIBE: {sub:?}");
    a.sock.write_all(b"*abc\r\n").expect("write");
    drain_until_closed(&mut a, Duration::from_secs(5));
    drop(a);
    assert_txn_ended(port, &key, "SUBSCRIBE then protocol fault");
    cleanup(guard, dir);
}

/// monoio hands a `PSYNC` connection's socket to the replication master
/// (the "hijack"). The TXN is rolled back before the hand-off. On tokio
/// (no master-side PSYNC) the connection stays a client and this is a plain
/// disconnect — asserted all the same.
#[test]
#[ignore]
fn psync_inside_the_txn_ends_it() {
    let (guard, port, dir) = spawn(1, &[]);
    let (mut a, key) = open_txn_holding(port);
    a.sock
        .write_all(&common::encode(&["PSYNC", "?", "-1"]))
        .expect("write");
    drain_until_closed(&mut a, Duration::from_secs(1));
    drop(a);
    assert_txn_ended(port, &key, "PSYNC inside the TXN");
    cleanup(guard, dir);
}
