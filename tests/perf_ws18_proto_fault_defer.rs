//! moon#1226 (the moon#1227-review protocol item): a protocol fault that
//! follows a DEFERRED part of a pipeline must not close the connection before
//! that part runs.
//!
//! redis runs every command it parsed before a malformed frame, answers each,
//! then answers `-ERR Protocol error: …` and closes. moon executes a batch up
//! to a point and carries the rest to the next loop iteration when a command
//! must wait (a blocking pop served on the spot, `SUBSCRIBE`, the #507
//! ordering guard). The fault was reported at the END of the first batch, so
//! the carried commands never ran — `RPUSH l a; BLPOP l 0; SET x 1; <bad>`
//! applied RPUSH and BLPOP and silently dropped the SET. In RESP2 subscriber
//! mode the fault closed the connection without any error at all, and on the
//! inline GET/SET fast path the replies of commands already applied were
//! dropped (`SET k v; <bad>` answered only the error).
//!
//! Every expected transcript below is redis-server 7.0.15's, captured with the
//! same bytes in one write (see the WS18 NOTES.md).
//!
//! Red on `d155cd6` and `ae21476`. Pin the binary:
//! `MOON_BIN=<moon> cargo test --test perf_ws18_proto_fault_defer`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use common::ServerGuard;

fn spawn(shards: usize) -> (ServerGuard, u16, std::path::PathBuf) {
    let dir = common::unique_test_dir(&format!("ws18-proto-fault-s{shards}"));
    std::fs::create_dir_all(&dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let d = dir.clone();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
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
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port, dir)
}

fn enc(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
    }
    out
}

/// A malformed frame: `$abc` is not a bulk length.
const BAD: &[u8] = b"*1\r\n$abc\r\n";
const PROTO_ERR: &str = "-ERR Protocol error: invalid bulk length\r\n";

/// Send `payload` in ONE write, then read until the server closes (or 10 s).
fn transcript(port: u16, payload: &[u8]) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    s.write_all(payload).expect("write");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                assert!(
                    Instant::now() < deadline,
                    "the server neither answered nor closed: {:?}",
                    String::from_utf8_lossy(&out)
                );
            }
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn get(port: u16, key: &str) -> String {
    let mut c = common::Conn::open(port);
    c.send(&["GET", key])
}

fn check(shards: usize) {
    let (guard, port, dir) = spawn(shards);

    // A: a blocking pop served on the spot defers the rest of the batch.
    let mut payload = enc(&["RPUSH", "pf:l", "a"]);
    payload.extend(enc(&["BLPOP", "pf:l", "0"]));
    payload.extend(enc(&["SET", "pf:x", "1"]));
    payload.extend_from_slice(BAD);
    assert_eq!(
        transcript(port, &payload),
        format!(":1\r\n*2\r\n$4\r\npf:l\r\n$1\r\na\r\n+OK\r\n{PROTO_ERR}"),
        "--shards {shards}: the SET deferred behind BLPOP must run and answer before the fault"
    );
    assert_eq!(
        get(port, "pf:x"),
        "$1\r\n1\r\n",
        "--shards {shards}: SET applied"
    );

    // B: the inline fast path's reply is not dropped by the fault behind it.
    let mut payload = enc(&["SET", "pf:y", "1"]);
    payload.extend_from_slice(BAD);
    assert_eq!(
        transcript(port, &payload),
        format!("+OK\r\n{PROTO_ERR}"),
        "--shards {shards}: a command applied before the fault is answered"
    );

    // C (control, already right on the base): a pipeline the ordering guard
    // may defer, over keys on every shard.
    let keys: Vec<String> = (0..8).map(|i| format!("pf:k{i}")).collect();
    let mut payload = Vec::new();
    for k in &keys {
        payload.extend(enc(&["SET", k, "v"]));
    }
    payload.extend(enc(&["MGET", &keys[0], &keys[1]]));
    payload.extend(enc(&["SET", "pf:z", "1"]));
    payload.extend_from_slice(BAD);
    assert_eq!(
        transcript(port, &payload),
        format!(
            "{}*2\r\n$1\r\nv\r\n$1\r\nv\r\n+OK\r\n{PROTO_ERR}",
            "+OK\r\n".repeat(8)
        ),
        "--shards {shards}: ordering-guard pipeline"
    );
    assert_eq!(get(port, "pf:z"), "$1\r\n1\r\n");

    // D: RESP2 SUBSCRIBE diverts the rest into subscriber mode; the PING there
    // is answered, and the fault is REPORTED there (it used to close mute, or
    // be reported before the PING ran).
    let mut payload = enc(&["SET", &keys[2], "w"]);
    payload.extend(enc(&["SUBSCRIBE", "pfch"]));
    payload.extend(enc(&["PING"]));
    payload.extend_from_slice(BAD);
    assert_eq!(
        transcript(port, &payload),
        format!(
            "+OK\r\n*3\r\n$9\r\nsubscribe\r\n$4\r\npfch\r\n:1\r\n*2\r\n$4\r\npong\r\n$0\r\n\r\n{PROTO_ERR}"
        ),
        "--shards {shards}: SUBSCRIBE; PING; <bad>"
    );

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn deferred_commands_run_before_a_protocol_fault_at_shards_1() {
    check(1);
}

#[test]
fn deferred_commands_run_before_a_protocol_fault_at_shards_4() {
    check(4);
}
