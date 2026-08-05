//! c10k hardening B3 (+ the B2 revocation story) — the client registry must
//! track the POST-AUTH identity, and revoking an account must close the
//! sessions it already granted.
//!
//! `client_registry::register` captures `user` once, at accept time, when it
//! is always `default`. Every AUTH/HELLO success updated only the
//! connection-local copy, so the registry — the thing `CLIENT LIST` reports
//! and `CLIENT KILL USER <name>` matches on — never learned who anybody was:
//! `CLIENT LIST` showed `user=default` for every session and `CLIENT KILL
//! USER alice` returned 0. That is the primary incident-response lever for a
//! compromised credential, and it was inert.
//!
//! It is also what makes revocation reachable at all: dropping a user from
//! the ACL table cannot by itself close the sessions that user already holds
//! (Redis disconnects them; we did not).
//!
//! Runs at `--shards 1` and `--shards 4` — the two handlers carry
//! independent AUTH/HELLO paths. Skips gracefully when the binary is missing.

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
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon(tag: &str, shards: &str) -> Option<Moon> {
    let bin = moon_binary()?;
    let tmp_dir = std::env::temp_dir().join(format!(
        "moon-acl-revoke-{}-{tag}-{shards}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--maxmemory",
                "268435456",
                "--dir",
                tmp_dir.to_str().expect("utf8 tmp dir"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
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
    eprintln!("skipping: moon did not become ready on port {port}");
    None
}

struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.buf.clear();
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
        let deadline = Instant::now() + Duration::from_millis(300);
        let mut chunk = [0u8; 8192];
        while Instant::now() < deadline {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => {}
            }
        }
        String::from_utf8_lossy(&self.buf).into_owned()
    }

    /// True once this session is dead — either the peer already tore the
    /// socket down, or it refuses to serve another command.
    ///
    /// The poke matters. `CLIENT KILL` sets a cooperative `kill_flag` AND
    /// `shutdown(2)`s the fd, but `force_close_fd` is `#[cfg(unix)]` — on
    /// Windows only the flag is set, so the connection is torn down when the
    /// handler next looks at it, i.e. on the client's next command. Asserting
    /// on a bare `read` therefore passed on unix and failed on Windows, which
    /// is exactly what CI caught. Sending a PING first exercises the same
    /// teardown Redis clients see on every platform; on unix the socket is
    /// already shut down, so the poke fails immediately and costs nothing.
    fn is_closed(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut chunk = [0u8; 4096];
        while Instant::now() < deadline {
            if self.stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
                return true; // EPIPE / ECONNRESET — already gone
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => return true,
                // A killed connection may still drain a reply that was
                // already queued; keep poking until it stops answering.
                Ok(_) => std::thread::sleep(Duration::from_millis(100)),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => return true, // ECONNRESET and friends
            }
        }
        false
    }
}

/// `CLIENT LIST` must report who a session actually authenticated as, and
/// `CLIENT KILL USER` must find it.
fn run_kill_by_user(shards: &str) {
    let Some(moon) = spawn_moon("kill", shards) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    let mut admin = Resp::connect(moon.port);
    let r = admin.cmd(&["ACL", "SETUSER", "alice", "on", ">pw", "~*", "+@all"]);
    assert!(r.contains("+OK"), "{tag} ACL SETUSER alice failed: {r:?}");

    let mut alice = Resp::connect(moon.port);
    let r = alice.cmd(&["AUTH", "alice", "pw"]);
    assert!(r.contains("+OK"), "{tag} AUTH alice failed: {r:?}");
    // A command after AUTH, so the session is unambiguously established.
    let r = alice.cmd(&["PING"]);
    assert!(r.contains("+PONG"), "{tag} alice PING failed: {r:?}");

    let r = admin.cmd(&["CLIENT", "LIST"]);
    assert!(
        r.contains("user=alice"),
        "{tag} CLIENT LIST must report the post-AUTH user, got: {r:?}"
    );

    let r = admin.cmd(&["CLIENT", "KILL", "USER", "alice"]);
    assert!(
        r.starts_with(":1") || r.starts_with(":2"),
        "{tag} CLIENT KILL USER alice must match alice's session, got: {r:?}"
    );
    assert!(
        alice.is_closed(),
        "{tag} alice's session must actually be torn down"
    );

    // The admin (still `default`) must NOT have been caught by that kill.
    let r = admin.cmd(&["PING"]);
    assert!(
        r.contains("+PONG"),
        "{tag} CLIENT KILL USER alice must not touch other users: {r:?}"
    );
}

/// Deleting a user closes the sessions that user already holds — otherwise a
/// revoked credential keeps its connection (and its maxclients slot) alive.
fn run_deluser_disconnects(shards: &str) {
    let Some(moon) = spawn_moon("del", shards) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    let mut admin = Resp::connect(moon.port);
    let r = admin.cmd(&["ACL", "SETUSER", "bob", "on", ">pw", "~*", "+@all"]);
    assert!(r.contains("+OK"), "{tag} ACL SETUSER bob failed: {r:?}");

    let mut bob = Resp::connect(moon.port);
    let r = bob.cmd(&["AUTH", "bob", "pw"]);
    assert!(r.contains("+OK"), "{tag} AUTH bob failed: {r:?}");
    let r = bob.cmd(&["SET", "k", "v"]);
    assert!(r.contains("+OK"), "{tag} bob SET failed: {r:?}");

    let r = admin.cmd(&["ACL", "DELUSER", "bob"]);
    assert!(r.starts_with(":1"), "{tag} ACL DELUSER bob failed: {r:?}");

    assert!(
        bob.is_closed(),
        "{tag} deleting bob must disconnect bob's live session"
    );
    let r = admin.cmd(&["PING"]);
    assert!(
        r.contains("+PONG"),
        "{tag} ACL DELUSER must not disturb other sessions: {r:?}"
    );
}

/// Deleting your OWN account must still answer before it disconnects you.
///
/// `kill_clients` takes a `self_id` precisely so a self-kill stays
/// cooperative — the flag is set, but the caller's fd is NOT `shutdown(2)`
/// out from under the in-flight reply. `ACL DELUSER` passed `None`, so a user
/// deleting itself had its socket torn down mid-command and saw a connection
/// error instead of the `:1`. Reply-then-disconnect is the contract.
fn run_self_deluser_replies_first(shards: &str) {
    let Some(moon) = spawn_moon("self", shards) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    let mut admin = Resp::connect(moon.port);
    // `carol` needs +acl to delete herself, and ~* so the ACL command's key
    // check does not deny her first.
    let r = admin.cmd(&["ACL", "SETUSER", "carol", "on", ">pw", "~*", "+@all"]);
    assert!(r.contains("+OK"), "{tag} ACL SETUSER carol failed: {r:?}");

    let mut carol = Resp::connect(moon.port);
    let r = carol.cmd(&["AUTH", "carol", "pw"]);
    assert!(r.contains("+OK"), "{tag} AUTH carol failed: {r:?}");
    let r = carol.cmd(&["PING"]);
    assert!(r.contains("+PONG"), "{tag} carol PING failed: {r:?}");

    // The reply must arrive. Before the fix this came back empty (socket shut
    // down mid-command) rather than `:1`.
    let r = carol.cmd(&["ACL", "DELUSER", "carol"]);
    assert!(
        r.starts_with(":1"),
        "{tag} a self-DELUSER must answer before disconnecting, got: {r:?}"
    );

    // ...and only then is the session gone.
    assert!(
        carol.is_closed(),
        "{tag} carol's session must still be torn down after the reply"
    );
    let r = admin.cmd(&["PING"]);
    assert!(
        r.contains("+PONG"),
        "{tag} other sessions must be undisturbed: {r:?}"
    );
}

#[test]
fn self_deluser_replies_before_disconnect_single_shard() {
    run_self_deluser_replies_first("1");
}

#[test]
fn self_deluser_replies_before_disconnect_multi_shard() {
    run_self_deluser_replies_first("4");
}

#[test]
fn client_kill_by_user_finds_authenticated_sessions_single_shard() {
    run_kill_by_user("1");
}

#[test]
fn client_kill_by_user_finds_authenticated_sessions_multi_shard() {
    run_kill_by_user("4");
}

#[test]
fn acl_deluser_disconnects_live_sessions_single_shard() {
    run_deluser_disconnects("1");
}

#[test]
fn acl_deluser_disconnects_live_sessions_multi_shard() {
    run_deluser_disconnects("4");
}
