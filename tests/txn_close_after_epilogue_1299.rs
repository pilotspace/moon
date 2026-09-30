//! moon#1299 R2 N1: once a client has read the server's close (its `QUIT`
//! reply, its protocol-error reply, or plain EOF after an output-limit
//! refusal), the `TXN` it left open must already be over — its keys free.
//!
//! The exit epilogue (`handler_{monoio,sharded}/exit.rs`) rolls the open
//! transaction back after the connection body returns. Before this fix the
//! body closed the socket FIRST — monoio's tail `shutdown()` (FIN), every
//! early `return` by dropping the stream — and the epilogue ran after. Under
//! `appendfsync always` the rollback awaits the AOF's fsync barrier before it
//! releases the holds, so a client that saw its `+OK` and EOF and then wrote
//! the key from another connection was answered `-TXNCONFLICT` (199/200 in
//! the review's repro, both runtimes). Now the body hands the stream back
//! and the wrapper closes it only after the epilogue: 0/N.
//!
//! A plain client-initiated close is not in scope — the client cannot know
//! when the server saw its FIN.
//!
//!   MOON_DISK_FREE_MIN_PCT=0 MOON_BIN=... cargo test --test txn_close_after_epilogue_1299 -- --include-ignored

mod common;

use std::io::{Read as _, Write as _};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";
/// Iterations per close class. The pre-fix race lost ~99% of them, so any
/// regression is caught many times over.
const ROUNDS: usize = 40;

fn spawn(shards: usize, extra: &[&str]) -> (common::ServerGuard, u16, std::path::PathBuf) {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir(&format!("txn-close-{shards}"));
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
                // The rollback's fsync barrier is what widens the window.
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
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

/// A key on `txn`'s own shard (a TXN refuses writes that route elsewhere).
fn local_key(txn: &mut Conn) -> String {
    for i in 0..256 {
        let key = format!("{{c{i}}}k");
        assert_eq!(txn.send(&["TXN", "BEGIN"]), OK);
        let wrote = txn.send(&["SET", &key, "probe"]);
        assert_eq!(txn.send(&["TXN", "ABORT"]), OK);
        if wrote == OK {
            return key;
        }
    }
    panic!("no key routes to the transaction's shard");
}

/// Read until the server closes the socket; everything it sent first is
/// returned. Panics if it never closes.
fn read_to_eof(c: &mut Conn) -> String {
    c.sock
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    loop {
        match c.sock.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
            // A reset is a close too (the server may drop a socket that
            // still holds unread bytes).
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) => panic!("the server never closed the connection: {e}"),
        }
    }
    String::from_utf8_lossy(&got).into_owned()
}

/// How the TXN connection makes the server close it, and what it must have
/// read before EOF.
#[derive(Clone, Copy)]
enum CloseBy {
    Quit,
    ProtocolFault,
    SubscribeThenQuit,
    SubscribeThenProtocolFault,
    /// A reply over `--client-output-buffer-limit-normal`: no reply, EOF.
    OutputLimit,
}

/// `ROUNDS` times: open a TXN that writes `key`, make the server close the
/// connection, read to EOF, then write `key` from another connection. Every
/// write must succeed first time and the TXN's value must never be visible.
fn run(port: u16, by: CloseBy, class: &str) {
    let mut w = Conn::open(port);
    let mut refused = 0usize;
    let mut dirty = 0usize;
    let mut first_refusal = String::new();
    for _ in 0..ROUNDS {
        let mut a = Conn::open(port);
        let key = local_key(&mut a);
        assert_eq!(w.send(&["SET", &key, "orig"]), OK);
        assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
        assert_eq!(a.send(&["SET", &key, "txnval"]), OK);
        if matches!(
            by,
            CloseBy::SubscribeThenQuit | CloseBy::SubscribeThenProtocolFault
        ) {
            let sub = a.send(&["SUBSCRIBE", "ch"]);
            assert!(sub.contains("subscribe"), "{class}: SUBSCRIBE: {sub:?}");
        }
        match by {
            CloseBy::Quit | CloseBy::SubscribeThenQuit => {
                a.sock.write_all(&common::encode(&["QUIT"])).expect("write");
            }
            CloseBy::ProtocolFault | CloseBy::SubscribeThenProtocolFault => {
                a.sock.write_all(b"*abc\r\n").expect("write");
            }
            CloseBy::OutputLimit => {
                a.sock
                    .write_all(&common::encode(&["GET", "big"]))
                    .expect("write");
            }
        }
        let seen = read_to_eof(&mut a);
        match by {
            CloseBy::Quit | CloseBy::SubscribeThenQuit => {
                assert_eq!(seen, OK, "{class}: QUIT must answer +OK before EOF")
            }
            CloseBy::ProtocolFault | CloseBy::SubscribeThenProtocolFault => assert!(
                seen.starts_with("-ERR Protocol error"),
                "{class}: the fault must be named before EOF, got {seen:?}"
            ),
            CloseBy::OutputLimit => assert!(
                seen.is_empty(),
                "{class}: an over-limit reply is refused, got {seen:?}"
            ),
        }
        // The client has seen the close: its transaction must be over.
        if w.send(&["GET", &key]) != "$4\r\norig\r\n" {
            dirty += 1;
        }
        let reply = w.send(&["SET", &key, "after"]);
        if reply != OK {
            refused += 1;
            if first_refusal.is_empty() {
                first_refusal = reply;
            }
            // Let the late rollback finish so the next round starts clean.
            let deadline = Instant::now() + Duration::from_secs(10);
            while w.send(&["SET", &key, "after"]) != OK {
                assert!(Instant::now() < deadline, "{class}: {key} stayed held");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        drop(a);
    }
    assert_eq!(
        (refused, dirty),
        (0, 0),
        "{class}: after the TXN client read its close, a write of its key was refused \
         {refused}/{ROUNDS} times (first: {first_refusal:?}) and the uncommitted value \
         was read {dirty}/{ROUNDS} times — the socket was closed before the TXN was \
         rolled back (want 0 and 0)"
    );
}

#[test]
#[ignore]
fn quit_closes_after_the_txn_is_rolled_back_1_shard() {
    let (guard, port, dir) = spawn(1, &[]);
    run(port, CloseBy::Quit, "QUIT, 1 shard");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn quit_closes_after_the_txn_is_rolled_back_4_shards() {
    let (guard, port, dir) = spawn(4, &[]);
    run(port, CloseBy::Quit, "QUIT, 4 shards");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn protocol_fault_closes_after_the_txn_is_rolled_back() {
    let (guard, port, dir) = spawn(1, &[]);
    run(port, CloseBy::ProtocolFault, "protocol fault");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn subscriber_quit_closes_after_the_txn_is_rolled_back() {
    let (guard, port, dir) = spawn(1, &[]);
    run(port, CloseBy::SubscribeThenQuit, "SUBSCRIBE then QUIT");
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn subscriber_protocol_fault_closes_after_the_txn_is_rolled_back() {
    let (guard, port, dir) = spawn(1, &[]);
    run(
        port,
        CloseBy::SubscribeThenProtocolFault,
        "SUBSCRIBE then protocol fault",
    );
    cleanup(guard, dir);
}

#[test]
#[ignore]
fn output_limit_close_comes_after_the_txn_is_rolled_back() {
    let (guard, port, dir) = spawn(1, &["--client-output-buffer-limit-normal", "2000"]);
    let big = "x".repeat(5000);
    assert_eq!(Conn::open(port).send(&["SET", "big", &big]), OK);
    run(
        port,
        CloseBy::OutputLimit,
        "reply over the output-buffer limit",
    );
    cleanup(guard, dir);
}
