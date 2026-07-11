//! Pub/sub burst-delivery integrity: the subscriber delivery arm coalesces
//! queued messages into a single `write_all` (one syscall per burst instead of
//! one per message). Coalescing concatenates pre-serialized RESP frames — this
//! test proves on the wire that a large pipelined publish burst arrives at the
//! subscriber (a) complete, (b) with intact frame boundaries, and (c) in
//! publish order, on the runtime under test.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/release/moon cargo test --test pubsub_burst_delivery

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// Must stay under the subscriber queue capacity (256, see
/// `handler_monoio/pubsub.rs` / `handler_sharded/pubsub.rs`): messages beyond
/// a full queue are DROPPED by design (slow-subscriber policy), and this test
/// asserts delivery integrity of the coalesced drain, not the drop policy.
const BURST: usize = 200;

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/spsc_two_db.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// Rooted under the repo's own volume, not system `$TMPDIR` (diskfull-guard
/// trap — see gotcha_vm_diskfull_shared_volume in project memory).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/pubsub-burst-tmp");
    std::fs::create_dir_all(&base).expect("create pubsub-burst-tmp base dir");
    tempfile::Builder::new()
        .prefix("pubsub-burst-")
        .tempdir_in(&base)
        .expect("tempdir_in target/pubsub-burst-tmp")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_moon(dir: &std::path::Path, shards: u32) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

fn connect(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                s.set_nodelay(true).unwrap();
                return s;
            }
            Err(e) => {
                if Instant::now() > deadline {
                    panic!("server never came up on port {port}: {e}");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn resp_cmd(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Read from `stream` until `buf` contains `needle` (or the deadline passes).
fn read_until_contains(stream: &mut TcpStream, buf: &mut Vec<u8>, needle: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut tmp = [0u8; 65536];
    while !buf.windows(needle.len()).any(|w| w == needle) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {:?}; got {} bytes so far",
            String::from_utf8_lossy(needle),
            buf.len()
        );
        match stream.read(&mut tmp) {
            Ok(0) => panic!(
                "subscriber connection closed early ({} bytes read)",
                buf.len()
            ),
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => panic!("subscriber read error: {e}"),
        }
    }
}

fn run_burst(shards: u32) {
    let dir = test_tmpdir();
    let (_guard, port) = spawn_moon(dir.path(), shards);

    // Subscriber
    let mut sub = connect(port);
    sub.write_all(&resp_cmd(&[b"SUBSCRIBE", b"burst-ch"]))
        .unwrap();
    let mut sub_buf = Vec::with_capacity(1 << 20);
    read_until_contains(&mut sub, &mut sub_buf, b"subscribe");
    sub_buf.clear();

    // Publisher: one pipelined write of BURST publishes → the subscriber's
    // flume queue backs up → the delivery arm's coalesced drain kicks in.
    let mut publisher = connect(port);
    let mut pipeline = Vec::with_capacity(BURST * 64);
    for i in 0..BURST {
        let payload = format!("msg:{i:05}");
        pipeline.extend_from_slice(&resp_cmd(&[b"PUBLISH", b"burst-ch", payload.as_bytes()]));
    }
    publisher.write_all(&pipeline).unwrap();
    publisher.flush().unwrap();

    // The last payload arriving implies the whole burst has been delivered
    // (single channel + single subscriber ⇒ FIFO).
    let last = format!("msg:{:05}", BURST - 1);
    read_until_contains(&mut sub, &mut sub_buf, last.as_bytes());

    // (a) complete + (b) frame boundaries intact: each message must appear as
    // a full RESP entry `$9\r\nmsg:NNNNN\r\n`; (c) in order: strictly
    // increasing positions.
    let mut pos = 0usize;
    for i in 0..BURST {
        let framed = format!("$9\r\nmsg:{i:05}\r\n");
        let found = sub_buf[pos..]
            .windows(framed.len())
            .position(|w| w == framed.as_bytes())
            .unwrap_or_else(|| {
                panic!(
                    "message {i} missing or out of order after byte {pos} \
                     ({} total bytes received)",
                    sub_buf.len()
                )
            });
        pos += found + framed.len();
    }
}

#[test]
fn burst_delivery_intact_and_ordered_single_shard() {
    run_burst(1);
}

#[test]
fn burst_delivery_intact_and_ordered_multi_shard() {
    run_burst(4);
}
