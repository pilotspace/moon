//! Sharded MULTI/EXEC AOF durability (2026-07 follow-up).
//!
//! `execute_transaction_sharded` — the MULTI/EXEC executor used by BOTH the
//! monoio handler (all shard counts, incl. `--shards 1`) and the tokio sharded
//! handler (`--shards >= 2`) — previously wrote nothing to the AOF. Every
//! transactional write was therefore silently lost on restart, while an
//! identical write issued outside MULTI survived. This is the worst durability
//! failure class: EXEC acked success, the data was gone after a restart.
//!
//! The invariant these tests pin:
//!
//!   **A write committed by EXEC under `appendonly=yes` is present after the
//!   server is killed and restarted from the same data dir — exactly like a
//!   write issued outside MULTI.**
//!
//! `--shards 1` makes placement deterministic (the Phase-A locality guard is
//! `num_shards > 1`, so it never rejects), and the default (monoio) runtime
//! routes `--shards 1` MULTI/EXEC through `execute_transaction_sharded` — the
//! buggy path. `appendfsync=always` means EXEC only returns after the fsync
//! barrier, so a `kill -9` immediately afterward is a true durability probe.
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

/// A running moon process bound to `port`, persisting into `dir`. Does NOT
/// remove `dir` on drop — the durability test restarts against the same dir.
struct Moon {
    child: Child,
    port: u16,
}

impl Moon {
    fn kill9(mut self) {
        // Hard kill: exercise crash recovery, not graceful shutdown flush.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Common `--dir`-relative arg tail shared by the first spawn and the restart.
fn persistent_args(dir: &std::path::Path) -> Vec<String> {
    vec![
        "--shards".into(),
        "1".into(),
        "--admin-port".into(),
        "0".into(),
        "--appendonly".into(),
        "yes".into(),
        "--appendfsync".into(),
        "always".into(),
        "--disk-free-min-pct".into(),
        "0".into(),
        "--dir".into(),
        dir.to_str().unwrap().into(),
    ]
}

/// Poll for PING readiness (protocol-level; `spawn_listening`/the direct
/// restart spawn only guarantee TCP accept). Unchanged from the pre-sweep
/// version beyond taking an already-constructed `Moon`.
fn wait_moon_ready(moon: Moon) -> Option<Moon> {
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

/// First spawn of a server lifecycle: goes through `common::spawn_listening`
/// so a lost bind-port race (or a dead child) is retried on a fresh port
/// instead of blind-polling a corpse.
fn spawn_moon_persistent_first(dir: &std::path::Path) -> Option<Moon> {
    let bin = moon_binary()?;
    let args = persistent_args(dir);
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

/// Restart on the SAME port + SAME dir on purpose (durability semantics: the
/// whole point is proving data survives a kill -9 + restart against the
/// identical persistence location). Per the port-flake-sweep hard rules,
/// restart spawns keep the direct (non-`spawn_listening`) path.
fn spawn_moon_persistent_restart(port: u16, dir: &std::path::Path) -> Option<Moon> {
    let bin = moon_binary()?;
    let child = Command::new(&bin)
        .args(["--port".to_string(), port.to_string()])
        .args(persistent_args(dir))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    wait_moon_ready(Moon { child, port })
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

/// GOLDEN INVARIANT: a write committed by EXEC must survive a kill-9 + restart,
/// exactly like a write issued outside MULTI. Before the fix the transactional
/// key vanished on restart while the plain control key survived.
#[test]
fn multi_exec_write_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    let Some(m1) = spawn_moon_persistent_first(dir.path()) else {
        return;
    };
    let port = m1.port;

    {
        let mut c = Client::connect(port);
        // Control: a plain (non-MULTI) write — the baseline that always survived.
        assert_eq!(
            c.cmd(&["SET", "plain:key", "plainval"]),
            Reply::Simple("OK".into())
        );
        // The write under test: committed via MULTI/EXEC.
        assert_eq!(c.cmd(&["MULTI"]), Reply::Simple("OK".into()));
        assert_eq!(
            c.cmd(&["SET", "txn:key", "txnval"]),
            Reply::Simple("QUEUED".into())
        );
        assert_eq!(
            c.cmd(&["INCRBY", "txn:counter", "5"]),
            Reply::Simple("QUEUED".into())
        );
        match c.cmd(&["EXEC"]) {
            Reply::Array(items) => {
                assert_eq!(items.len(), 2, "EXEC returns one reply per queued write");
                assert_eq!(items[0], Reply::Simple("OK".into()));
                assert_eq!(items[1], Reply::Int(5));
            }
            other => panic!("EXEC must return the results array, got: {other:?}"),
        }
        // Visible immediately (in-memory) before the restart.
        assert_eq!(
            c.cmd(&["GET", "txn:key"]),
            Reply::Bulk(Some("txnval".into()))
        );
    }

    // Hard kill (no graceful flush) — appendfsync=always already put the EXEC
    // body on disk, so recovery must replay it.
    m1.kill9();

    let Some(m2) = spawn_moon_persistent_restart(port, dir.path()) else {
        panic!("moon failed to restart from {:?}", dir.path());
    };

    let mut r = Client::connect(port);
    let plain = r.cmd(&["GET", "plain:key"]);
    let txn = r.cmd(&["GET", "txn:key"]);
    let counter = r.cmd(&["GET", "txn:counter"]);
    m2.kill9();

    // Control must survive (it always did).
    assert_eq!(
        plain,
        Reply::Bulk(Some("plainval".into())),
        "plain (non-MULTI) write lost on restart — harness/persistence broken"
    );
    // The regression: the EXEC-committed writes must survive too.
    assert_eq!(
        txn,
        Reply::Bulk(Some("txnval".into())),
        "MULTI/EXEC SET lost on restart — transactional writes were not persisted"
    );
    assert_eq!(
        counter,
        Reply::Bulk(Some("5".into())),
        "MULTI/EXEC INCRBY lost on restart — transactional writes were not persisted"
    );
}
