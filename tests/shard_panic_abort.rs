//! A panicked shard thread must take the whole process down (fail-fast),
//! never leave an N-1-shard zombie silently serving partial data.
//!
//! RSS/CPU/OOM review item 7 (zombie lens): shard threads are joined only at
//! shutdown and the join result was discarded (`let _ = handle.join()`), so a
//! shard panic during normal operation left the server listening and
//! answering for the surviving shards while every key owned by the dead
//! shard was silently gone — worse than a crash for a database.
//!
//! RED before the fix: `DEBUG PANIC` kills one shard thread, the process
//! stays alive past the timeout. GREEN: a shard-thread panic hook aborts.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for_ready(port: u16, deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        let addr = format!("127.0.0.1:{port}");
        if let Ok(mut s) =
            TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(200))
        {
            s.set_read_timeout(Some(Duration::from_millis(500))).ok();
            s.set_write_timeout(Some(Duration::from_millis(500))).ok();
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 16];
                if let Ok(n) = s.read(&mut buf) {
                    if buf[..n].windows(4).any(|w| w == b"PONG") {
                        return true;
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

fn assert_panic_aborts_process(shards: u32, label: &str) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = common::spawn_listening(|port| {
        Command::new(moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.path().to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
            ])
            .stdout(std::fs::File::create(dir.path().join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.path().join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    let mut guard = ServerGuard(child);

    assert!(
        wait_for_ready(port, Duration::from_secs(15)),
        "[{label}] server never ready"
    );

    let addr = format!("127.0.0.1:{port}");
    let mut s = TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_secs(2))
        .expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(2))).ok();
    s.write_all(b"DEBUG PANIC\r\n").expect("send DEBUG PANIC");
    // The connection is expected to die; the reply (if any) is irrelevant.
    let mut buf = [0u8; 64];
    let _ = s.read(&mut buf);

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if guard.0.try_wait().expect("try_wait").is_some() {
            return; // process died fail-fast — GREEN
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "[{label}] process still alive 5s after a shard panic — N-1-shard \
         zombie serving partial data (see moon.stderr.log in the temp dir)"
    );
}

#[test]
fn shard_panic_aborts_process_shards_1() {
    assert_panic_aborts_process(1, "s1");
}

#[test]
fn shard_panic_aborts_process_shards_4() {
    assert_panic_aborts_process(4, "s4");
}
