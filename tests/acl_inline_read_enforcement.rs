//! ACL enforcement on the inline read fast path (client-compat P0).
//!
//! `try_inline_dispatch` (`src/server/conn/blocking.rs`) answers plain
//! `GET key` straight from the shard's map without entering the generic
//! dispatch path. Writes are gated on `can_inline_writes`, which folds in
//! `conn.acl_skip_allowed()` — reads were gated on **nothing**, so an
//! authenticated-but-restricted user read any key by name:
//!
//! ```text
//! ACL SETUSER locked on >pw -@all
//! AUTH locked pw            -> +OK
//! GET secret                -> "value"     (Redis: -NOPERM)
//! SET/DEL/MGET/HGET/TTL/... -> -NOPERM     (correctly gated)
//! ```
//!
//! Not a single-shard quirk: at `--shards 4` the leak covers every key that
//! hashes to the connection's own shard (measured ~27% of a 480-GET sweep).
//! `--shards 1` simply makes it 100%.
//!
//! The inline path exists only in the **monoio** handler
//! (`handler_monoio/mod.rs`); the tokio handlers gate correctly at
//! `handler_single.rs:1587` / `handler_sharded/mod.rs:761`. Every CI test job
//! builds tokio, which is why this was invisible. Under a tokio build these
//! tests still pass — they just stop being a regression guard, so keep a
//! monoio run in the release gate.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Number of distinct keys probed per connection. Must comfortably exceed
/// the shard count so that at `--shards 4` some keys land on the
/// connection's own shard (only those are inline-eligible).
const KEYS: usize = 40;
/// Connections used per probe: each lands on a different shard thread, so
/// collectively they cover every shard's inline path.
const CONNS: usize = 4;

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

fn spawn_moon(shards: &str) -> Option<Moon> {
    // CARGO_BIN_EXE_moon is the binary cargo built for THIS invocation —
    // fresh and feature-matched. Never probe target/release directly.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-aclinline-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
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
                // This host hovers near the 5% diskfull line — the guard
                // would turn every SET into a MOONERR and flake the suite.
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-aclinline-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
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
    eprintln!("skipping: moon did not become ready on port {port}");
    None
}

/// Minimal synchronous RESP client: one command, one reply.
struct Resp {
    stream: TcpStream,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(1500)))
            .unwrap();
        Self { stream }
    }

    /// Send `args` and return the raw first chunk of the reply. Every command
    /// used here yields a single small frame, so one read is sufficient.
    fn cmd(&mut self, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
        let mut buf = [0u8; 4096];
        match self.stream.read(&mut buf) {
            Ok(n) => String::from_utf8_lossy(&buf[..n]).into_owned(),
            Err(e) => format!("<read error: {e}>"),
        }
    }
}

/// Seed `KEYS` values and define `user` with the given ACL rules.
fn seed(port: u16, user: &str, rules: &[&str]) {
    let mut admin = Resp::connect(port);
    for i in 0..KEYS {
        let key = format!("secret{i}");
        let val = format!("leaked{i}");
        assert!(
            admin.cmd(&["SET", &key, &val]).starts_with("+OK"),
            "seed SET must succeed"
        );
    }
    assert!(admin.cmd(&["SET", "app:ok", "public"]).starts_with("+OK"));
    let mut args = vec!["ACL", "SETUSER", user, "on", ">pw"];
    args.extend_from_slice(rules);
    let reply = admin.cmd(&args);
    assert!(reply.starts_with("+OK"), "ACL SETUSER failed: {reply:?}");
}

/// Every `GET secret*` issued by `user` must be refused. Returns the leaks.
fn collect_get_leaks(port: u16, user: &str) -> Vec<(String, String)> {
    let mut leaks = Vec::new();
    for _ in 0..CONNS {
        let mut c = Resp::connect(port);
        let auth = c.cmd(&["AUTH", user, "pw"]);
        assert!(auth.starts_with("+OK"), "AUTH failed: {auth:?}");
        for i in 0..KEYS {
            let key = format!("secret{i}");
            let reply = c.cmd(&["GET", &key]);
            if !reply.starts_with("-NOPERM") {
                leaks.push((key, reply.replace("\r\n", "\\r\\n")));
            }
        }
    }
    leaks
}

fn assert_no_leaks(shards: &str, user: &str, rules: &[&str]) {
    let Some(m) = spawn_moon(shards) else { return };
    seed(m.port, user, rules);
    let leaks = collect_get_leaks(m.port, user);
    assert!(
        leaks.is_empty(),
        "--shards {shards}: user '{user}' ({rules:?}) read {} of {} keys via the inline GET \
         fast path; ACL must deny every one. First leaks: {:?}",
        leaks.len(),
        KEYS * CONNS,
        &leaks[..leaks.len().min(5)]
    );
}

// ── deny-all user: the command check must reject GET ────────────────────

#[test]
fn deny_all_user_cannot_inline_get_single_shard() {
    assert_no_leaks("1", "lockedone", &["-@all"]);
}

#[test]
fn deny_all_user_cannot_inline_get_multi_shard() {
    assert_no_leaks("4", "lockedfour", &["-@all"]);
}

// ── key-pattern user: the key check must reject out-of-pattern GET ──────

fn assert_pattern_enforced(shards: &str, user: &str) {
    let Some(m) = spawn_moon(shards) else { return };
    seed(m.port, user, &["+@read", "~app:*"]);

    let mut c = Resp::connect(m.port);
    assert!(c.cmd(&["AUTH", user, "pw"]).starts_with("+OK"));

    // In-pattern read is allowed and returns the real value.
    let ok = c.cmd(&["GET", "app:ok"]);
    assert!(
        ok.contains("public"),
        "--shards {shards}: in-pattern GET must succeed, got {ok:?}"
    );

    // Out-of-pattern reads must be refused on every key, inline-eligible or not.
    let leaks = collect_get_leaks(m.port, user);
    assert!(
        leaks.is_empty(),
        "--shards {shards}: user '{user}' (+@read ~app:*) read {} keys outside its pattern \
         via the inline GET fast path. First leaks: {:?}",
        leaks.len(),
        &leaks[..leaks.len().min(5)]
    );
}

#[test]
fn key_pattern_enforced_on_inline_get_single_shard() {
    assert_pattern_enforced("1", "scopedone");
}

#[test]
fn key_pattern_enforced_on_inline_get_multi_shard() {
    assert_pattern_enforced("4", "scopedfour");
}
