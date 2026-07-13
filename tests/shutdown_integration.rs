//! Task #27: SHUTDOWN [NOSAVE|SAVE] real-server integration tests.
//!
//! Covers the Redis-parity contract this command must honour end to end:
//!   - `SHUTDOWN NOSAVE` exits the process promptly (no reply is sent; the
//!     client observes the connection close).
//!   - Under `--appendonly yes`, SHUTDOWN flushes durably -- a write made
//!     just before SHUTDOWN survives a restart with no kill-9 tail loss.
//!   - A forced `SHUTDOWN SAVE` that cannot complete (disk write failure)
//!     replies with an error and the server stays up and reachable.
//!   - Malformed arguments (`SHUTDOWN BOGUS`, conflicting modifiers) reply
//!     `ERR syntax error` and the server stays up.
//!
//! Run with (release binary required):
//!   cargo build --release
//!   cargo test --release --test shutdown_integration -- --ignored --test-threads=1

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use common::{find_moon_binary, sigkill, spawn_listening};

fn spawn_moon(dir: &std::path::Path, extra: &[&str]) -> (Child, u16) {
    static GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let log_gen = GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    spawn_listening(|port| {
        let mut args: Vec<String> = vec![
            "--port".into(),
            port.to_string(),
            "--dir".into(),
            dir.to_string_lossy().into_owned(),
            "--shards".into(),
            "1".into(),
            "--disk-free-min-pct".into(),
            "0".into(),
        ];
        for &e in extra {
            args.push(e.into());
        }
        Command::new(find_moon_binary())
            .args(&args)
            .stdout(
                std::fs::File::create(dir.join(format!("moon.stdout.{log_gen}.log")))
                    .expect("create stdout log"),
            )
            .stderr(
                std::fs::File::create(dir.join(format!("moon.stderr.{log_gen}.log")))
                    .expect("create stderr log"),
            )
            .env("RUST_LOG", "moon=info")
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to spawn moon binary at '{}': {e}. Build with \
                     `cargo build --release` or set MOON_BIN.",
                    find_moon_binary().display()
                )
            })
    })
}

/// RAII guard: SIGKILLs the server process when dropped, so a panicking
/// assertion never leaks a live server (which then poisons the next test's
/// port scan / holds the AOF dir open).
struct ServerGuard(Option<Child>);

impl ServerGuard {
    fn take(&mut self) -> Child {
        self.0.take().expect("server already taken")
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            sigkill(&mut child);
        }
    }
}

fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("parse addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                // 20s: comfortably above SHUTDOWN_SAVE_TIMEOUT_MS (10s, see
                // src/command/persistence.rs) so the forced-SAVE-failure test
                // observes the server's own timeout error instead of racing
                // its own socket read timeout.
                s.set_read_timeout(Some(Duration::from_secs(20))).ok();
                s.set_write_timeout(Some(Duration::from_secs(20))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on port {port}: {e}"),
        }
    }
}

/// Minimal inline-protocol client: enough for SET/GET/PING/SHUTDOWN.
struct Conn {
    s: TcpStream,
}

impl Conn {
    fn open(port: u16) -> Self {
        Self {
            s: connect(port, Duration::from_secs(10)),
        }
    }

    /// Send a RESP array command and return the raw reply bytes read in one
    /// `read()` call. Good enough for the simple status/error/bulk replies
    /// these tests assert on (no pipelining, no partial-frame reassembly).
    fn cmd(&mut self, parts: &[&str]) -> std::io::Result<String> {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req)?;
        let mut buf = [0u8; 4096];
        let n = self.s.read(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf[..n]).into_owned())
    }
}

fn wait_ready(port: u16) {
    let start = Instant::now();
    loop {
        if let Ok(stream) = TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        ) {
            stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
            let mut c = Conn { s: stream };
            if let Ok(reply) = c.cmd(&["PING"]) {
                if reply.contains("PONG") {
                    return;
                }
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "server never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Poll until the child is no longer alive, or panic past `deadline`.
fn wait_exited(child: &mut Child, deadline: Duration) -> std::process::ExitStatus {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return status;
        }
        assert!(
            start.elapsed() < deadline,
            "server did not exit within {deadline:?} after SHUTDOWN"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn shutdown_nosave_exits_promptly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), &["--appendonly", "no"]);
    let mut guard = ServerGuard(Some(child));
    wait_ready(port);

    let mut c = Conn::open(port);
    // Redis parity: on success SHUTDOWN sends no reply; the client just sees
    // the connection close (a zero-length read, or a write/read error --
    // either is acceptable here, we only care that the process itself
    // actually terminates below).
    let _ = c.cmd(&["SHUTDOWN", "NOSAVE"]);

    let mut child = guard.take();
    let status = wait_exited(&mut child, Duration::from_secs(10));
    assert!(
        status.success(),
        "SHUTDOWN NOSAVE should exit cleanly (status 0), got {status:?}"
    );
}

#[test]
#[ignore]
fn shutdown_appendonly_flushes_durably() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), &["--appendonly", "yes"]);
    let mut guard = ServerGuard(Some(child));
    wait_ready(port);

    let mut c = Conn::open(port);
    let set_reply = c.cmd(&["SET", "shutdown:durable", "hello"]).unwrap();
    assert!(
        set_reply.contains("OK"),
        "SET should succeed before SHUTDOWN: {set_reply}"
    );
    let _ = c.cmd(&["SHUTDOWN", "NOSAVE"]);

    let mut child = guard.take();
    let status = wait_exited(&mut child, Duration::from_secs(10));
    assert!(status.success(), "SHUTDOWN should exit cleanly: {status:?}");

    // Restart on the same --dir and confirm the AOF replayed the write --
    // a clean SHUTDOWN must not lose the tail the way a bare kill-9 can.
    let (child2, port2) = spawn_moon(dir.path(), &["--appendonly", "yes"]);
    let guard2 = ServerGuard(Some(child2));
    wait_ready(port2);
    let mut c2 = Conn::open(port2);
    let get_reply = c2.cmd(&["GET", "shutdown:durable"]).unwrap();
    assert!(
        get_reply.contains("hello"),
        "SHUTDOWN must flush AOF durably before exiting; got: {get_reply}"
    );
    drop(guard2);
}

#[test]
#[ignore]
fn shutdown_save_failure_keeps_server_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), &["--appendonly", "no"]);
    let mut guard = ServerGuard(Some(child));
    wait_ready(port);

    // Make the data dir unwritable so the forced RDB save inside
    // `SHUTDOWN SAVE` fails -- Redis parity: a save that fails must reply
    // an error and NOT exit the process.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500))
            .expect("chmod read-only");
    }

    let mut c = Conn::open(port);
    let reply = c.cmd(&["SHUTDOWN", "SAVE"]).unwrap();
    assert!(
        reply.starts_with('-') || reply.to_ascii_uppercase().contains("ERR"),
        "SHUTDOWN SAVE against an unwritable dir must reply an error, got: {reply}"
    );

    // Restore permissions before the guard's Drop tries to SIGKILL + the
    // tempdir cleanup tries to remove files under it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod restore");
    }

    // Server must still be up and answering.
    let pong = c.cmd(&["PING"]).unwrap();
    assert!(
        pong.contains("PONG"),
        "server must stay up after a failed SHUTDOWN SAVE, got: {pong}"
    );

    let mut child = guard.take();
    assert!(
        child.try_wait().expect("try_wait").is_none(),
        "server process must still be running after a failed SHUTDOWN SAVE"
    );
    sigkill(&mut child);
}

#[test]
#[ignore]
fn shutdown_syntax_error_keeps_server_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), &["--appendonly", "no"]);
    let mut guard = ServerGuard(Some(child));
    wait_ready(port);

    let mut c = Conn::open(port);
    let reply = c.cmd(&["SHUTDOWN", "BOGUS"]).unwrap();
    assert!(
        reply.to_ascii_uppercase().contains("SYNTAX"),
        "SHUTDOWN with an unknown modifier should be a syntax error, got: {reply}"
    );

    let reply2 = c.cmd(&["SHUTDOWN", "SAVE", "NOSAVE"]).unwrap();
    assert!(
        reply2.to_ascii_uppercase().contains("SYNTAX"),
        "SHUTDOWN with conflicting SAVE/NOSAVE modifiers should be a syntax \
         error, got: {reply2}"
    );

    let pong = c.cmd(&["PING"]).unwrap();
    assert!(
        pong.contains("PONG"),
        "server must stay up after SHUTDOWN syntax errors, got: {pong}"
    );

    let mut child = guard.take();
    assert!(
        child.try_wait().expect("try_wait").is_none(),
        "server process must still be running after SHUTDOWN syntax errors"
    );
    sigkill(&mut child);
}
