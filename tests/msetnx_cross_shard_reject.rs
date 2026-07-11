//! ADD milestone `v3-4-kv-correctness` — MSETNX cross-shard contract.
//!
//! MSETNX is atomic by contract (set every pair iff none of the keys exist).
//! Moon cannot honor that atomically across shards, so — by deliberate design
//! decision — a MSETNX whose keys span more than one shard is REJECTED with a
//! CROSSSLOT error and writes nothing. When the keys are co-located on a single
//! shard (naturally or via a `{hash-tag}`) the whole command runs atomically on
//! that shard's owner.
//!
//! These are green-after-fix regression tests. They assert BOTH sides so the
//! suite cannot pass vacuously: an "always reject" bug fails the co-located
//! case; a "never reject / route-by-connection" bug fails the cross-shard case.
//!
//! Run alone with: cargo test --test msetnx_cross_shard_reject

mod common;

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

// ---------------------------------------------------------------------------
// Harness (CARGO_BIN_EXE pattern, mirrors cross_shard_consistency_red.rs)
// ---------------------------------------------------------------------------

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn spawn_moon(dir: &std::path::Path, shards: u32) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon (CARGO_BIN_EXE_moon)")
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(5))).ok();
                s.set_write_timeout(Some(Duration::from_secs(5))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on {port}: {e}"),
        }
    }
}

fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf)
            && n > 0
            && buf[..n].windows(4).any(|w| w == b"PONG")
        {
            return s;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "server accepted TCP but never answered PING"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP2 reader
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn open(port: u16) -> Self {
        Conn {
            s: connect(port, Duration::from_secs(10)),
            buf: Vec::with_capacity(16 * 1024),
            pos: 0,
        }
    }

    fn cmd_s(&mut self, parts: &[&str]) -> Resp {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.s.read(&mut chunk).expect("read");
        assert!(n > 0, "connection closed mid-frame");
        self.buf.extend_from_slice(&chunk[..n]);
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let line =
                    String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
                self.pos += rel + 2;
                return line;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        out
    }

    fn frame(&mut self) -> Resp {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap_or(0)),
            "$" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Bulk(None)
                } else {
                    Resp::Bulk(Some(self.exact(n as usize)))
                }
            }
            "*" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Array(None)
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(self.frame());
                    }
                    Resp::Array(Some(items))
                }
            }
            other => panic!("unexpected RESP tag {other:?} (line {line:?})"),
        }
    }
}

/// Return one key per shard (index = shard id), generated deterministically via
/// moon's own hash so cross-shard-ness is provable, not assumed.
fn keys_per_shard(prefix: &str, num_shards: usize) -> Vec<String> {
    let mut out: Vec<Option<String>> = vec![None; num_shards];
    let mut found = 0;
    for i in 0..10_000 {
        let k = format!("{prefix}{i}");
        let s = key_to_shard(k.as_bytes(), num_shards);
        if out[s].is_none() {
            out[s] = Some(k);
            found += 1;
            if found == num_shards {
                break;
            }
        }
    }
    out.into_iter()
        .map(|o| o.expect("found a key for every shard"))
        .collect()
}

const SHARDS: u32 = 4;

// ---------------------------------------------------------------------------
// Cross-shard MSETNX is rejected (CROSSSLOT) and writes NOTHING.
// ---------------------------------------------------------------------------

#[test]
fn msetnx_cross_shard_rejected_no_partial_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), SHARDS);
    let _guard = ServerGuard(child);
    drop(wait_ready(port));

    // Two keys that PROVABLY land on different shards.
    let ks = keys_per_shard("msnx:", SHARDS as usize);
    let (k0, k1) = (&ks[0], &ks[1]);
    assert_ne!(
        key_to_shard(k0.as_bytes(), SHARDS as usize),
        key_to_shard(k1.as_bytes(), SHARDS as usize),
        "test precondition: keys must span shards"
    );

    let mut c = Conn::open(port);
    let r = c.cmd_s(&["MSETNX", k0, "v0", k1, "v1"]);
    match &r {
        Resp::Error(m) => assert!(
            m.starts_with("CROSSSLOT"),
            "cross-shard MSETNX must be a CROSSSLOT error (got {r:?})"
        ),
        other => panic!("cross-shard MSETNX must be rejected, got {other:?}"),
    }

    // Reject is total: NEITHER key was written (no partial side effects).
    assert_eq!(
        c.cmd_s(&["GET", k0]),
        Resp::Bulk(None),
        "rejected MSETNX must not write k0"
    );
    assert_eq!(
        c.cmd_s(&["GET", k1]),
        Resp::Bulk(None),
        "rejected MSETNX must not write k1"
    );
}

// ---------------------------------------------------------------------------
// Co-located MSETNX runs atomically on the owner shard (all-or-nothing).
// ---------------------------------------------------------------------------

#[test]
fn msetnx_colocated_is_atomic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), SHARDS);
    let _guard = ServerGuard(child);
    drop(wait_ready(port));

    let mut c = Conn::open(port);

    // All keys share the {t} hash-tag -> one shard, even at --shards 4.
    assert_eq!(
        c.cmd_s(&["MSETNX", "{t}a", "v1", "{t}b", "v2"]),
        Resp::Int(1),
        "all-new co-located MSETNX returns 1"
    );
    assert_eq!(c.cmd_s(&["GET", "{t}a"]), Resp::Bulk(Some(b"v1".to_vec())));
    assert_eq!(c.cmd_s(&["GET", "{t}b"]), Resp::Bulk(Some(b"v2".to_vec())));

    // One key already exists -> whole command is a no-op, returns 0.
    assert_eq!(
        c.cmd_s(&["MSETNX", "{t}b", "vX", "{t}c", "v3"]),
        Resp::Int(0),
        "MSETNX with any existing key returns 0"
    );
    // Atomic: the new key {t}c must NOT have been written.
    assert_eq!(
        c.cmd_s(&["GET", "{t}c"]),
        Resp::Bulk(None),
        "MSETNX no-op must not write {{t}}c"
    );
    // And the pre-existing value is unchanged.
    assert_eq!(c.cmd_s(&["GET", "{t}b"]), Resp::Bulk(Some(b"v2".to_vec())));
}
