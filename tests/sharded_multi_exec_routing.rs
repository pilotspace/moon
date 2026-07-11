//! Sharded MULTI/EXEC Phase B: route a single-owner-shard body to its owner.
//!
//! Phase A rejected any transaction whose keys weren't owned by the connection's
//! accept shard (random via SO_REUSEPORT) with `CROSSSLOT` — so a hash-tagged
//! `{tag}` multi-key transaction worked only from the ~1/N connections that
//! happened to land on the owning shard. Phase B routes the whole body to the
//! owning shard (via `ShardMessage::TxnExecute`), executes it atomically there,
//! and persists writes to THAT shard's AOF. Genuinely multi-shard bodies stay
//! rejected — a shared-nothing engine can't commit across shards atomically.
//!
//! Invariants pinned here:
//!   1. A hash-tagged MULTI/EXEC succeeds (returns the results array, never
//!      CROSSSLOT) from EVERY connection, and its writes are visible to others.
//!   2. Those writes survive a kill-9 + restart (AOF persisted on the OWNER).
//!   3. A body whose keys span shards is still rejected with CROSSSLOT.
//!
//! Skips gracefully when the moon binary is missing (MOON_BIN pin wins, then
//! target/release, then target/debug).

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
}

impl Moon {
    fn kill9(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `durable` selects `appendonly yes` + `appendfsync always` (for the restart
/// test) vs `appendonly no`. Always `--shards 4`.
fn moon_args(dir: &std::path::Path, durable: bool) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "--shards".into(),
        "4".into(),
        "--admin-port".into(),
        "0".into(),
        "--disk-free-min-pct".into(),
        "0".into(),
        "--dir".into(),
        dir.to_str().unwrap().into(),
    ];
    if durable {
        args.extend(["--appendonly".into(), "yes".into()]);
        args.extend(["--appendfsync".into(), "always".into()]);
    } else {
        args.extend(["--appendonly".into(), "no".into()]);
    }
    args
}

fn wait_moon_ready(moon: Moon) -> Option<Moon> {
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

/// First spawn of a server lifecycle — port chosen via `common::spawn_listening`.
fn spawn_moon_first(dir: &std::path::Path, durable: bool) -> Option<Moon> {
    let bin = moon_binary()?;
    let args = moon_args(dir, durable);
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args(["--port".to_string(), port.to_string()])
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    wait_moon_ready(Moon { child, port })
}

/// Restart on the SAME port + dir on purpose (durability semantics). Per the
/// port-flake-sweep hard rules, restart spawns keep the direct spawn path.
fn spawn_moon_restart(port: u16, dir: &std::path::Path, durable: bool) -> Option<Moon> {
    let bin = moon_binary()?;
    let child = Command::new(&bin)
        .args(["--port".to_string(), port.to_string()])
        .args(moon_args(dir, durable))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    wait_moon_ready(Moon { child, port })
}

#[derive(Debug, Clone, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<String>),
    Array(Vec<Reply>),
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Client {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(3000)))
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
        self.pos += n + 2;
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

/// INVARIANT 1: a hash-tagged multi-key MULTI/EXEC succeeds from EVERY
/// connection (never CROSSSLOT — Phase B routes it to the owning shard) and its
/// writes are visible from a fresh connection. 40 distinct tags exercise all 4
/// shards as owners across random accept placement.
#[test]
fn routed_hashtag_txn_succeeds_from_any_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let Some(m) = spawn_moon_first(dir.path(), false) else {
        return;
    };
    let port = m.port;

    for i in 0..40 {
        let ka = format!("user:{{{i}}}:a");
        let kb = format!("user:{{{i}}}:b");
        let kc = format!("user:{{{i}}}:c");
        let va = format!("va{i}");
        let vb = format!("vb{i}");

        // Fresh connection each iteration → random accept shard.
        let mut w = Client::connect(port);
        assert_eq!(w.cmd(&["MULTI"]), Reply::Simple("OK".into()));
        assert_eq!(w.cmd(&["SET", &ka, &va]), Reply::Simple("QUEUED".into()));
        assert_eq!(w.cmd(&["SET", &kb, &vb]), Reply::Simple("QUEUED".into()));
        assert_eq!(w.cmd(&["INCRBY", &kc, "7"]), Reply::Simple("QUEUED".into()));
        match w.cmd(&["EXEC"]) {
            Reply::Array(items) => {
                assert_eq!(items.len(), 3, "one reply per queued command");
                assert_eq!(items[0], Reply::Simple("OK".into()));
                assert_eq!(items[1], Reply::Simple("OK".into()));
                assert_eq!(items[2], Reply::Int(7));
            }
            other => {
                panic!("hash-tagged txn (tag {i}) must succeed via Phase B routing, got: {other:?}")
            }
        }

        // Visible from a fresh reader (routes each key to its true owner).
        let mut r = Client::connect(port);
        assert_eq!(
            r.cmd(&["GET", &ka]),
            Reply::Bulk(Some(va.clone())),
            "routed txn wrote {ka} but it isn't visible"
        );
        assert_eq!(r.cmd(&["GET", &kb]), Reply::Bulk(Some(vb.clone())));
        assert_eq!(r.cmd(&["GET", &kc]), Reply::Bulk(Some("7".into())));
    }

    m.kill9();
}

/// INVARIANT 2: routed (Phase B) transactional writes survive kill-9 + restart —
/// they must be persisted to the OWNER shard's AOF, not silently dropped.
#[test]
fn routed_hashtag_txn_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    let Some(m1) = spawn_moon_first(dir.path(), true) else {
        return;
    };
    let port = m1.port;

    // 12 distinct tags → spread across all 4 owner shards; fresh conn each time
    // so most bodies route cross-shard.
    for i in 0..12 {
        let ka = format!("acct:{{{i}}}:name");
        let kb = format!("acct:{{{i}}}:bal");
        let mut w = Client::connect(port);
        assert_eq!(w.cmd(&["MULTI"]), Reply::Simple("OK".into()));
        assert_eq!(
            w.cmd(&["SET", &ka, "alice"]),
            Reply::Simple("QUEUED".into())
        );
        assert_eq!(
            w.cmd(&["INCRBY", &kb, "100"]),
            Reply::Simple("QUEUED".into())
        );
        match w.cmd(&["EXEC"]) {
            Reply::Array(items) => assert_eq!(items.len(), 2),
            other => panic!("routed txn (tag {i}) must succeed, got: {other:?}"),
        }
    }

    m1.kill9();

    let Some(m2) = spawn_moon_restart(port, dir.path(), true) else {
        panic!("moon failed to restart from {:?}", dir.path());
    };
    let mut r = Client::connect(port);
    let mut missing = Vec::new();
    for i in 0..12 {
        let ka = format!("acct:{{{i}}}:name");
        let kb = format!("acct:{{{i}}}:bal");
        if r.cmd(&["GET", &ka]) != Reply::Bulk(Some("alice".into())) {
            missing.push(ka);
        }
        if r.cmd(&["GET", &kb]) != Reply::Bulk(Some("100".into())) {
            missing.push(kb);
        }
    }
    m2.kill9();
    assert!(
        missing.is_empty(),
        "routed MULTI/EXEC writes lost on restart (not persisted on owner shard): {missing:?}"
    );
}

/// INVARIANT 3: a body whose keys demonstrably span shards is still rejected
/// with CROSSSLOT — Phase B only routes single-owner-shard bodies.
#[test]
fn multi_shard_span_still_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let Some(m) = spawn_moon_first(dir.path(), false) else {
        return;
    };
    let mut c = Client::connect(m.port);
    assert_eq!(c.cmd(&["MULTI"]), Reply::Simple("OK".into()));
    for i in 0..20 {
        // Distinct untagged keys → near-certain to span >1 of 4 shards.
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
    m.kill9();
}
