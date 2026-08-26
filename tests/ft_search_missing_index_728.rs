//! `FT.SEARCH` on an index that does not exist must refuse, at every shard
//! count and in every feature set (moon#728).
//!
//! The shipped default build already answers `ERR no such index` at both shard
//! counts. A build compiled **without** `text-index` — which is exactly what
//! CI's tokio leg compiles — did not:
//!
//! ```text
//! --shards 1   FT.SEARCH nosuch "*"  ->  ERR invalid KNN query syntax
//! --shards 4   FT.SEARCH nosuch "*"  ->  0        # a SUCCESSFUL empty listing
//! ```
//!
//! The second is the damaging one: a caller cannot tell "this index does not
//! exist" from "this index is empty", and the answer changes with a deployment
//! knob the caller cannot see. So the assertion below is not "some error" —
//! it is that BOTH shard counts give the SAME answer, and that the answer is
//! an error. An implementation that returns two different errors is still
//! wrong, because shard count is not part of the query.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn spawn_on(port: u16, shards: usize, dir: &std::path::Path) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn moon (run `cargo build --release` first)")
}

/// A raw-socket probe: `redis-cli` is not on every runner, and the reply we
/// care about is the difference between an error and an empty array, which a
/// shell probe flattens.
struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        let _ = s.set_read_timeout(Some(Duration::from_secs(10)));
        Self(s)
    }

    /// Send one command and return the first reply line, verbatim.
    ///
    /// One line is enough to classify: `-ERR ...` and `*0` differ in their
    /// first byte, and for a non-empty array the count line is what would
    /// prove the reply was NOT a refusal.
    fn line(&mut self, args: &[&[u8]]) -> String {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        self.0.write_all(&out).expect("write");
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            let n = self.0.read(&mut b).expect("read");
            assert!(n == 1, "server closed the connection mid-reply");
            if b[0] == b'\r' {
                let _ = self.0.read(&mut b);
                break;
            }
            line.push(b[0]);
        }
        String::from_utf8_lossy(&line).into_owned()
    }
}

fn await_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 64];
            if s.write_all(b"PING\r\n").is_ok()
                && let Ok(n) = s.read(&mut buf)
                && buf[..n].starts_with(b"+PONG")
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "server on port {port} never became ready"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Every query shape that resolves against the text engine, including the
/// match-all `*` that moon#693 introduced.
const QUERIES: &[&[u8]] = &[b"*", b"hello", b"@title:hello", b"hello|world"];

fn answers(shards: usize) -> Vec<(String, String)> {
    let dir = tempfile::Builder::new()
        .prefix("moon-728-")
        .tempdir()
        .expect("tempdir");
    let port = common::reserve_port();
    let mut guard = common::ServerGuard::new(spawn_on(port, shards, dir.path()));
    await_ready(port);
    let mut c = Conn::open(port);

    // A real index exists, on a DIFFERENT name. Without it the server has an
    // empty index store, and "refuses everything" would pass this test for the
    // wrong reason.
    assert!(
        c.line(&[
            b"FT.CREATE",
            b"real728",
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"r:",
            b"SCHEMA",
            b"e",
            b"VECTOR",
            b"HNSW",
            b"6",
            b"TYPE",
            b"FLOAT32",
            b"DIM",
            b"4",
            b"DISTANCE_METRIC",
            b"L2",
        ])
        .starts_with('+'),
        "shards={shards}: the control index must be creatable"
    );

    let out = QUERIES
        .iter()
        .map(|q| {
            (
                String::from_utf8_lossy(q).into_owned(),
                c.line(&[b"FT.SEARCH", b"nosuch728", q]),
            )
        })
        .collect();
    drop(c);
    guard.kill_now();
    out
}

#[test]
fn moon728_a_missing_index_is_refused_identically_at_every_shard_count() {
    let one = answers(1);
    let many = answers(4);

    for ((q, a1), (_, a4)) in one.iter().zip(many.iter()) {
        assert!(
            a1.starts_with('-'),
            "shards=1: FT.SEARCH on a missing index must be an error for query {q:?}, got {a1:?}"
        );
        assert!(
            a4.starts_with('-'),
            "shards=4: FT.SEARCH on a missing index must be an error for query {q:?}, got {a4:?}. \
             A successful empty listing is indistinguishable from an index that exists and holds \
             nothing."
        );
        assert_eq!(
            a1, a4,
            "query {q:?}: shard count is not part of the query, so the two answers must be the \
             same string"
        );
    }
}
