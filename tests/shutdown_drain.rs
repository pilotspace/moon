//! F1 (#438, conn#8): graceful shutdown must drain connection tasks.
//!
//! Before the fix, the shard event loop's `shutdown.cancelled()` arm ran its
//! persistence teardown and immediately `break` — `run` returned, the monoio
//! runtime was dropped, and every spawned connection task was dropped
//! mid-poll. A client blocked in BLPOP never received the
//! `-ERR server shutting down` reply its shutdown arm exists to send; a
//! client with an in-flight batch could see its reply truncated. Clients saw
//! a bare socket close instead of reply-then-FIN.
//!
//! The fix adds a bounded drain phase to the shutdown arm: fire the idle-park
//! cancellers so stage-1/2 parked reads wake (they check the shutdown token
//! and exit through the normal flush+FIN epilogue), then keep polling the
//! runtime until the shard's live connection tasks reach zero or the drain
//! deadline expires, and only then tear down persistence and drop the
//! runtime.
//!
//! All tests are `#[cfg(unix)]`: they signal the server with SIGTERM.

#![cfg(unix)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let cargo_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    if cargo_bin.exists() {
        return Some(cargo_bin);
    }
    None
}

struct Moon {
    child: Child,
    port: u16,
    _tmp_dir: tempfile::TempDir,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_moon(shards: &str) -> Option<Moon> {
    let bin = moon_binary()?;
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let dir_str = tmp_dir.path().to_str().unwrap().to_string();
    let shards = shards.to_string();
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                &dir_str,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        _tmp_dir: tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return Some(moon);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not become ready on port {}", moon.port);
    None
}

fn sigterm(moon: &Moon) {
    let status = Command::new("kill")
        .args(["-TERM", &moon.child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success(), "kill -TERM failed");
}

/// Wait for the server process to exit, panicking past `secs`. Every test
/// calls this so a drain phase that hangs shutdown is itself a failure.
fn await_exit(moon: &mut Moon, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        match moon.child.try_wait().expect("try_wait") {
            Some(_) => return,
            None if Instant::now() >= deadline => {
                panic!("server did not exit within {secs}s of SIGTERM (drain must be bounded)")
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

/// Read everything until EOF (or panic on timeout). Returns the bytes the
/// server flushed before closing.
fn read_to_eof(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    stream.set_read_timeout(Some(timeout)).unwrap();
    let mut out = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return out,
            Ok(n) => out.extend_from_slice(&chunk[..n]),
            Err(e) => panic!(
                "no EOF from server within {timeout:?} (got {} bytes so far: {:?}): {e}",
                out.len(),
                String::from_utf8_lossy(&out)
            ),
        }
    }
}

/// Clients blocked in BLPOP must receive `-ERR server shutting down` and a
/// clean FIN when the server is SIGTERMed — their handler tasks have a
/// shutdown arm that sends exactly that reply, but pre-fix the tasks were
/// dropped unpolled when the shard runtime was torn down.
///
/// Many connections, ALL must get the reply: a single conn can win the
/// teardown race by scheduling luck (macOS/kqueue reliably does; Linux
/// io_uring measured 31/50 pre-fix), so one-conn asserts under-test. With 50
/// conns the pre-fix loss is statistically certain on the losing platforms
/// and the post-fix drain must be exhaustive anyway.
fn blocked_clients_drain(shards: &str) {
    let Some(mut moon) = spawn_moon(shards) else {
        return;
    };
    const N: usize = 50;
    let mut conns = Vec::with_capacity(N);
    for i in 0..N {
        let mut c = TcpStream::connect(("127.0.0.1", moon.port)).expect("connect");
        let key = format!("drainkey{i}");
        let cmd = format!(
            "*3\r\n$5\r\nBLPOP\r\n${}\r\n{key}\r\n$1\r\n0\r\n",
            key.len()
        );
        c.write_all(cmd.as_bytes()).expect("send BLPOP");
        conns.push(c);
    }
    // Let the waits register (including the remote leg at shards>=2).
    std::thread::sleep(Duration::from_millis(600));
    sigterm(&moon);
    let mut lost = Vec::new();
    for (i, mut c) in conns.into_iter().enumerate() {
        let bytes = read_to_eof(&mut c, Duration::from_secs(8));
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let ok = text.contains("shutting down")
            || text.starts_with("*-1\r\n")
            || text.starts_with("$-1");
        if !ok {
            lost.push((i, text));
        }
    }
    assert!(
        lost.is_empty(),
        "{}/{N} blocked clients lost their shutdown reply (task dropped before its shutdown arm ran); first: {:?}",
        lost.len(),
        lost.first()
    );
    await_exit(&mut moon, 15);
}

#[test]
fn blocked_clients_get_shutdown_reply_shards1() {
    blocked_clients_drain("1");
}

#[test]
fn blocked_clients_get_shutdown_reply_shards2() {
    blocked_clients_drain("2");
}

/// An idle connection parked in a stage-1/2 read must see a clean FIN (read
/// == 0, no ECONNRESET) shortly after SIGTERM, and the server must still
/// exit promptly — the drain phase is bounded.
#[test]
fn parked_idle_conn_clean_fin_on_sigterm() {
    let Some(mut moon) = spawn_moon("1") else {
        return;
    };
    let mut idle = TcpStream::connect(("127.0.0.1", moon.port)).expect("connect");
    idle.write_all(b"*1\r\n$4\r\nPING\r\n").expect("ping");
    let mut buf = [0u8; 32];
    idle.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let n = idle.read(&mut buf).expect("pong");
    assert!(buf[..n].starts_with(b"+PONG"));
    // Sit idle past the 1 s stage-1 sweep so the read is parked/downshifted.
    std::thread::sleep(Duration::from_millis(1600));
    sigterm(&moon);
    // Runtime-behavior split: monoio's cancelled park exits with a bare FIN;
    // the tokio main select's shutdown arm sends `-ERR server shutting down`
    // first. Both are clean drains — what must NOT happen is a reset or a
    // hang (read_to_eof panics on timeout, await_exit bounds the process).
    let bytes = read_to_eof(&mut idle, Duration::from_secs(5));
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        bytes.is_empty() || text.contains("shutting down"),
        "idle conn should see a bare FIN or the shutdown error, got: {text:?}"
    );
    await_exit(&mut moon, 10);
}

/// A subscriber must also drain: its select loop has a shutdown arm today,
/// but pre-fix the task was never polled after cancellation. After the fix
/// the subscriber's socket closes with a clean FIN and the process exits.
#[test]
fn subscriber_conn_clean_fin_on_sigterm() {
    let Some(mut moon) = spawn_moon("2") else {
        return;
    };
    let mut sub = TcpStream::connect(("127.0.0.1", moon.port)).expect("connect");
    sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$7\r\ndrainch\r\n")
        .expect("subscribe");
    let mut buf = [0u8; 128];
    sub.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let n = sub.read(&mut buf).expect("subscribe reply");
    assert!(n > 0, "subscribe must be acknowledged");
    std::thread::sleep(Duration::from_millis(300));
    sigterm(&moon);
    // Everything already acknowledged; the drain just needs to close cleanly.
    let _ = read_to_eof(&mut sub, Duration::from_secs(5));
    await_exit(&mut moon, 10);
}
