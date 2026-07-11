//! Sharded MULTI/EXEC key-locality safety (2026-07 follow-up).
//!
//! `execute_transaction_sharded` runs the whole queued body on the connection's
//! OWN shard with no per-key routing. A key owned by a different shard was
//! therefore silently written to / read from the wrong shard's table — EXEC
//! reported success while the data diverged (and WATCH-less "lost updates").
//!
//! Phase A closed the silent-corruption hole by rejecting such a transaction
//! with `CROSSSLOT`; Phase B goes further and ROUTES a single-owner-shard body
//! to its owner (so single-key and hash-tagged bodies now succeed from any
//! connection — see `sharded_multi_exec_routing.rs`). Bodies that genuinely
//! span shards stay rejected. Either way the golden invariant these tests pin
//! is unchanged:
//!
//!   **If EXEC returns a successful array, every write in it is visible to
//!   other connections; if EXEC returns CROSSSLOT, nothing was written.**
//!
//! Never "EXEC says OK but the data isn't there." Skips gracefully when the
//! moon binary is missing (MOON_BIN pin wins, then target/release, then debug).

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for rel in ["target/release/moon", "target/debug/moon"] {
        let p = root.join(rel);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

struct Moon {
    child: Child,
    port: u16,
    // Kept alive for the process lifetime; removed on Drop.
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
                if let Ok(n) = c.read(&mut buf) {
                    if n > 0 && buf.starts_with(b"+PONG") {
                        return Some(moon);
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not become ready on port {}", moon.port);
    None
}

/// One parsed RESP2 reply — only the shapes these tests need.
#[derive(Debug, Clone, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<String>),
    Array(Vec<Reply>),
}

/// Blocking RESP client that parses exactly one reply per command.
struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Client {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(2000)))
            .unwrap();
        Self {
            stream,
            buf: Vec::new(),
            pos: 0,
        }
    }

    fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 4096];
        match self.stream.read(&mut chunk) {
            Ok(0) => panic!("connection closed by server"),
            Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            Err(e) => panic!("read error: {e}"),
        }
    }

    fn read_line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let end = self.pos + rel;
                let line = String::from_utf8_lossy(&self.buf[self.pos..end]).to_string();
                self.pos = end + 2;
                return line;
            }
            self.fill();
        }
    }

    fn read_exact_bytes(&mut self, n: usize) -> String {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + n]).to_string();
        self.pos += n + 2; // skip trailing CRLF
        s
    }

    fn read_reply(&mut self) -> Reply {
        let line = self.read_line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Reply::Simple(rest.to_string()),
            "-" => Reply::Error(rest.to_string()),
            ":" => Reply::Int(rest.parse().unwrap_or(0)),
            "$" => {
                let len: i64 = rest.parse().unwrap_or(-1);
                if len < 0 {
                    Reply::Bulk(None)
                } else {
                    Reply::Bulk(Some(self.read_exact_bytes(len as usize)))
                }
            }
            "*" => {
                let len: i64 = rest.parse().unwrap_or(-1);
                if len < 0 {
                    Reply::Array(Vec::new())
                } else {
                    let mut items = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        items.push(self.read_reply());
                    }
                    Reply::Array(items)
                }
            }
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }

    fn cmd(&mut self, args: &[&str]) -> Reply {
        self.send(args);
        self.read_reply()
    }
}

/// GOLDEN INVARIANT: a transaction that touches a key owned by another shard
/// must never "succeed silently and lose the write." Over many distinct keys
/// (so both the local-shard and remote-shard branches are exercised at random
/// accept placement), assert: EXEC-array success ⇒ the value is readable from a
/// fresh connection; EXEC CROSSSLOT ⇒ the key does not exist.
#[test]
fn no_silent_divergence_across_shards() {
    let Some(m) = spawn_moon("4") else { return };

    let mut saw_ok = 0usize;
    let mut saw_crossslot = 0usize;

    for i in 0..40 {
        let key = format!("txnloc:{i}");
        let val = format!("v{i}");

        let mut writer = Client::connect(m.port);
        assert_eq!(writer.cmd(&["MULTI"]), Reply::Simple("OK".into()));
        assert_eq!(
            writer.cmd(&["SET", &key, &val]),
            Reply::Simple("QUEUED".into())
        );
        let exec = writer.cmd(&["EXEC"]);

        // A fresh connection reads the key (routes to the true owner shard).
        let mut reader = Client::connect(m.port);
        let got = reader.cmd(&["GET", &key]);

        match &exec {
            Reply::Array(items) => {
                saw_ok += 1;
                // The queued SET succeeded, so the value MUST be visible.
                assert_eq!(
                    got,
                    Reply::Bulk(Some(val.clone())),
                    "EXEC reported success {items:?} but GET {key} = {got:?} (silent misplacement)"
                );
            }
            Reply::Error(e) if e.starts_with("CROSSSLOT") => {
                saw_crossslot += 1;
                // Rejected — nothing should have been written.
                assert_eq!(
                    got,
                    Reply::Bulk(None),
                    "EXEC was rejected ({e}) but GET {key} = {got:?} (partial write leaked)"
                );
            }
            other => panic!("unexpected EXEC reply for {key}: {other:?}"),
        }
    }

    // Diagnostic only — the invariant above is what matters. Under Phase B a
    // single-key body is routed to its owner, so `saw_ok` should be ~all 40 and
    // `saw_crossslot` ~0 (pre-fix these "ok" cases silently returned Bulk(None);
    // under Phase A the remote ones were CROSSSLOT).
    eprintln!("no_silent_divergence: {saw_ok} succeeded, {saw_crossslot} rejected");
}

/// A transaction whose keys demonstrably span multiple shards is always
/// rejected with CROSSSLOT (20 distinct keys → span is near-certain at 4
/// shards), and nothing it queued is written.
#[test]
fn multi_shard_span_rejected() {
    let Some(m) = spawn_moon("4") else { return };
    let mut c = Client::connect(m.port);

    assert_eq!(c.cmd(&["MULTI"]), Reply::Simple("OK".into()));
    for i in 0..20 {
        let key = format!("span:{i}");
        assert_eq!(c.cmd(&["SET", &key, "x"]), Reply::Simple("QUEUED".into()));
    }
    match c.cmd(&["EXEC"]) {
        Reply::Error(e) => assert!(
            e.starts_with("CROSSSLOT"),
            "20-key span must be CROSSSLOT, got: {e}"
        ),
        other => panic!("20-key span must be rejected, got: {other:?}"),
    }

    // None of the queued keys should exist.
    let mut r = Client::connect(m.port);
    for i in 0..20 {
        let key = format!("span:{i}");
        assert_eq!(
            r.cmd(&["GET", &key]),
            Reply::Bulk(None),
            "rejected txn must not have written {key}"
        );
    }
}

/// Single-shard servers are unaffected: the `num_shards > 1` guard means the
/// locality check never runs, MULTI/EXEC returns the results array (never
/// CROSSSLOT), and the write is applied. (Uses writes only — a solo GET sent
/// mid-MULTI hits an unrelated monoio inline-read quirk, out of scope here.)
#[test]
fn single_shard_unaffected() {
    let Some(m) = spawn_moon("1") else { return };
    let mut c = Client::connect(m.port);

    assert_eq!(c.cmd(&["MULTI"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["SET", "s:a", "1"]), Reply::Simple("QUEUED".into()));
    assert_eq!(c.cmd(&["SET", "s:b", "2"]), Reply::Simple("QUEUED".into()));
    match c.cmd(&["EXEC"]) {
        Reply::Array(items) => {
            assert_eq!(items.len(), 2, "EXEC array has one reply per queued write");
            assert_eq!(items[0], Reply::Simple("OK".into()));
            assert_eq!(items[1], Reply::Simple("OK".into()));
        }
        other => panic!("single-shard EXEC must return the results array, got: {other:?}"),
    }
    // Writes are visible afterward (no CROSSSLOT swallowed them).
    let mut r = Client::connect(m.port);
    assert_eq!(r.cmd(&["GET", "s:a"]), Reply::Bulk(Some("1".into())));
    assert_eq!(r.cmd(&["GET", "s:b"]), Reply::Bulk(Some("2".into())));
}
