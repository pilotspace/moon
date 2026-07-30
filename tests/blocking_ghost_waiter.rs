//! c10k hardening cluster A — the blocking registry must not leak waiters
//! across shards, and a wake must never destroy the element it popped.
//!
//! Two defects made a timed-out cross-shard `BLPOP` eat the NEXT push to that
//! key, permanently and silently:
//!
//! * **A3** — the tokio single-key cleanup read
//!   `is_remote && !matches!(result, Frame::Null) || matches!(result, Frame::Error(_))`,
//!   which Rust parses as `(is_remote && !Null) || Error`. On a timeout the
//!   result IS `Frame::Null`, so the `BlockCancel` was never sent and the
//!   owning shard kept the `WaitEntry` forever — remote entries carry
//!   `deadline: None`, so the W6 deadline sweep never reaps them either.
//! * **A2** — when a later push found that ghost waiter, the wake path popped
//!   the element and then did `let _ = reply_tx.send(...)`. The receiver was
//!   long gone, so the value was neither delivered, nor requeued, nor offered
//!   to the next waiter. It simply vanished.
//!
//! This suite drives the pair end-to-end through a real 4-shard server.
//! Sixteen distinct keys are used because a connection is pinned to one shard
//! by SO_REUSEPORT: with 4 shards most keys hash to a *different* shard than
//! the client's, which is the only configuration that reproduces A3.
//!
//! Note the runtime asymmetry: the monoio twin of the cleanup condition was
//! always correct, so this test only turns red under `runtime-tokio`. CI runs
//! both feature sets, and the invariant is worth pinning on both.
//!
//! Run with:
//!   cargo test --release --test blocking_ghost_waiter

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::Duration;

const SHARDS: &str = "4";
/// Enough keys that several provably hash to a shard other than the one the
/// client connection landed on.
const KEYS: usize = 16;

/// Serialize `parts` as a RESP array and write it.
fn send(stream: &mut TcpStream, parts: &[&str]) {
    let mut out = format!("*{}\r\n", parts.len());
    for p in parts {
        out.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    stream.write_all(out.as_bytes()).expect("write command");
}

/// Read one reply. Every reply this suite issues fits in a single small
/// response, so one read is sufficient; the socket carries a read timeout so
/// a wedged server fails the test instead of hanging it.
fn read_reply(stream: &mut TcpStream) -> String {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).expect("read reply");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to moon");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    stream.set_nodelay(true).expect("set nodelay");
    stream
}

#[test]
fn timed_out_cross_shard_blpop_does_not_swallow_the_next_push() {
    let bin = common::find_moon_binary();
    if !bin.exists() {
        eprintln!("skipping: no moon binary; build with `cargo build --release`");
        return;
    }

    let (mut child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-ghost-waiter-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                SHARDS,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                // The repo's data volume hovers near the diskfull guard's 5%
                // threshold; without this the server refuses writes and the
                // assertions below fail for an unrelated reason.
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-ghost-waiter-{port}"));

    let result = std::panic::catch_unwind(|| {
        let mut blocker = connect(port);
        let mut pusher = connect(port);

        for i in 0..KEYS {
            let key = format!("ghost:{i}");

            // 1. Block on an empty key and let the timeout expire. This is
            //    what leaves the ghost `WaitEntry` on the owning shard.
            send(&mut blocker, &["BLPOP", &key, "0.2"]);
            let reply = read_reply(&mut blocker);
            assert!(
                reply.starts_with("*-1") || reply.starts_with("$-1"),
                "{key}: BLPOP should time out with nil, got {reply:?}"
            );

            // 2. Push AFTER the timeout. No waiter exists any more, so this
            //    element belongs to the keyspace, not to a wakeup.
            send(&mut pusher, &["RPUSH", &key, "payload"]);
            let reply = read_reply(&mut pusher);
            assert!(
                reply.starts_with(":1"),
                "{key}: RPUSH should report length 1, got {reply:?}"
            );

            // 3. The element must still be there. Before the fix a ghost
            //    waiter on a remote shard consumed it into a dead oneshot.
            send(&mut pusher, &["LLEN", &key]);
            let reply = read_reply(&mut pusher);
            assert!(
                reply.starts_with(":1"),
                "{key}: element was swallowed by a ghost waiter — LLEN {reply:?}"
            );
        }
    });

    common::sigkill(&mut child);
    let _ = std::fs::remove_dir_all(&tmp_dir);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// The other half of the same invariant: a waiter that is served normally
/// must still consume its element exactly once, so the fix above cannot have
/// been implemented by simply never delivering.
#[test]
fn woken_cross_shard_blpop_consumes_exactly_one_element() {
    let bin = common::find_moon_binary();
    if !bin.exists() {
        eprintln!("skipping: no moon binary; build with `cargo build --release`");
        return;
    }

    let (mut child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-wake-once-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                SHARDS,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-wake-once-{port}"));

    let result = std::panic::catch_unwind(|| {
        for i in 0..KEYS {
            let key = format!("wake:{i}");
            let mut blocker = connect(port);
            let mut pusher = connect(port);

            // Park a real waiter, then push two elements from another
            // connection: the waiter takes one, one stays.
            send(&mut blocker, &["BLPOP", &key, "5"]);
            std::thread::sleep(Duration::from_millis(150));

            send(&mut pusher, &["RPUSH", &key, "first", "second"]);
            let reply = read_reply(&mut pusher);
            assert!(
                reply.starts_with(":"),
                "{key}: RPUSH should report a length, got {reply:?}"
            );

            let woken = read_reply(&mut blocker);
            assert!(
                woken.contains("first"),
                "{key}: waiter should receive the head element, got {woken:?}"
            );

            send(&mut pusher, &["LLEN", &key]);
            let reply = read_reply(&mut pusher);
            assert!(
                reply.starts_with(":1"),
                "{key}: exactly one element should remain, got {reply:?}"
            );
        }
    });

    common::sigkill(&mut child);
    let _ = std::fs::remove_dir_all(&tmp_dir);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
