//! c10k W3: maxclients rejection must be LOUD (Redis parity).
//!
//! Redis writes `-ERR max number of clients reached\r\n` before closing a
//! connection that exceeds `maxclients`. Moon used to log a warn! and close
//! silently — a client at the cap saw an unexplained EOF (found by the
//! 2026-07-29 c10k review, tmp/C10K-REVIEW.md defect #1).

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::Duration;

fn spawn_moon(dir: &std::path::Path, port: u16, maxclients: u32) -> std::process::Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--maxclients",
            &maxclients.to_string(),
            "--dir",
            dir.to_str().unwrap(),
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

fn ping_ok(stream: &mut TcpStream) -> bool {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    if stream.write_all(b"PING\r\n").is_err() {
        return false;
    }
    let mut buf = [0u8; 16];
    matches!(stream.read(&mut buf), Ok(n) if buf[..n].starts_with(b"+PONG"))
}

/// Acquire the single maxclients slot, retrying while transient holders
/// (spawn_listening's liveness probe, a prior conn's async deregistration)
/// release it.
fn acquire_slot(port: u16) -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        if ping_ok(&mut conn) {
            return conn;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "could not acquire the maxclients slot within 10s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn rejected_connection_receives_err_max_clients() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p, 1));

    // Conn A takes the single slot and must be fully served.
    let mut conn_a = acquire_slot(port);

    // Conn B exceeds maxclients: it must receive the Redis-parity error
    // before the server closes it — not a silent EOF.
    let mut conn_b =
        TcpStream::connect(("127.0.0.1", port)).expect("conn B TCP-connects (accept then reject)");
    conn_b
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set timeout");
    let mut buf = Vec::new();
    let mut chunk = [0u8; 256];
    loop {
        match conn_b.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => panic!(
                "conn B read failed before any reply (silent rejection): {e}; got {:?}",
                String::from_utf8_lossy(&buf)
            ),
        }
    }
    let reply = String::from_utf8_lossy(&buf);
    assert!(
        reply.starts_with("-ERR max number of clients reached"),
        "over-cap connection must get the Redis parity error, got: {reply:?}"
    );

    // The slot-holder must be unaffected by the rejection.
    assert!(ping_ok(&mut conn_a), "conn A must survive conn B's rejection");

    drop(conn_a);
    common::sigkill(&mut child);
}

#[test]
fn slot_frees_on_disconnect() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p, 1));

    let conn_a = acquire_slot(port);
    drop(conn_a);

    // After A disconnects, the slot must free (poll: deregistration is async).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut conn_c = TcpStream::connect(("127.0.0.1", port)).expect("conn C");
        if ping_ok(&mut conn_c) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "slot never freed after disconnect"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    common::sigkill(&mut child);
}
