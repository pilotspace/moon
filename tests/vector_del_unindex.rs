//! DEL/UNLINK must remove auto-indexed vectors from FT.* indexes on EVERY
//! dispatch path — not just the cross-shard SPSC `Execute` arm.
//!
//! Found by the Bundle-5 soak diagnostic (scripts/vector-validate.py): after
//! one minute of mixed churn at --shards 1, 20% of FT.SEARCH results were
//! DELETED keys (resurrection) and live-set recall collapsed to 0.735,
//! because the conn-local write path never called
//! `VectorStore::mark_deleted_for_key`. Classic three-dispatch-paths gap:
//! the hook existed on the SPSC `Execute` arm and the tokio sharded handler,
//! but not on the monoio conn-local path (the default runtime's ONLY path at
//! shards=1), handler_single, the MULTI batch path, or the SPSC pipeline arms.
//!
//! Wire-level on purpose: store-level tests cannot catch dispatch wiring.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/release/moon cargo test --test vector_del_unindex

#![allow(clippy::unwrap_used)]

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const DIM: usize = 8;

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/shardslice_live.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let release = std::path::PathBuf::from(format!("{manifest}/target/release/moon"));
    if release.exists() {
        return release;
    }
    let debug = std::path::PathBuf::from(format!("{manifest}/target/debug/moon"));
    if debug.exists() {
        return debug;
    }
    panic!("No moon binary found. Build first or set MOON_BIN=/path/to/moon.");
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_moon(port: u16, dir: &std::path::Path, shards: u32) -> ServerGuard {
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon");
    ServerGuard(child)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe args, full-frame parser)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<V>),
    Null,
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("server never accepted on port {port}: {e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.reader.read_exact(&mut b).expect("read byte");
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0]);
            }
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn parse(&mut self) -> V {
        let line = self.read_line();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().expect("int")),
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    return V::Null;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).expect("bulk body");
                buf.truncate(n as usize);
                V::Bulk(buf)
            }
            "*" => {
                let n: i64 = rest.parse().expect("arr len");
                if n < 0 {
                    return V::Null;
                }
                V::Arr((0..n).map(|_| self.parse()).collect())
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    /// Send all commands in ONE write (a wire pipeline), then read all replies.
    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut buf = Vec::new();
        for c in cmds {
            let refs: Vec<&[u8]> = c.iter().map(|a| a.as_slice()).collect();
            buf.extend_from_slice(&Self::encode(&refs));
        }
        self.writer.write_all(&buf).expect("send pipeline");
        cmds.iter().map(|_| self.parse()).collect()
    }

    /// Fallible PING for the readiness probe: a connection accepted while the
    /// server is still bringing up its per-shard SO_REUSEPORT listeners can be
    /// RESET mid-read — that must retry with a fresh connection, not panic.
    fn try_ping(&mut self) -> std::io::Result<bool> {
        self.writer.write_all(b"*1\r\n$4\r\nPING\r\n")?;
        let mut buf = [0u8; 7];
        self.reader.read_exact(&mut buf)?;
        Ok(&buf == b"+PONG\r\n")
    }
}

fn wait_ready(port: u16) -> Client {
    let start = Instant::now();
    loop {
        let mut c = Client::connect(port);
        // Any I/O error (or a non-PONG answer, which would desync the framing)
        // drops this connection and probes again on a new one.
        if let Ok(true) = c.try_ping() {
            return c;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "server never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

fn vec_blob(seed: u32) -> Vec<u8> {
    // Distinct, deterministic unit-ish vectors; exact values don't matter.
    let mut out = Vec::with_capacity(DIM * 4);
    for i in 0..DIM {
        let v = ((seed * 31 + i as u32 * 7) % 97) as f32 / 97.0 + 0.01;
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

fn ft_create(c: &mut Client) {
    let r = c.cmd(&[
        b"FT.CREATE",
        b"idx",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"d:",
        b"SCHEMA",
        b"vec",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        b"8",
        b"DISTANCE_METRIC",
        b"L2",
    ]);
    assert_eq!(r, V::Simple("OK".into()), "FT.CREATE failed");
}

fn hset_vectors(c: &mut Client, ids: std::ops::Range<u32>) {
    for i in ids {
        let key = format!("d:{i}");
        let blob = vec_blob(i);
        let r = c.cmd(&[b"HSET", key.as_bytes(), b"vec", &blob]);
        assert!(matches!(r, V::Int(_)), "HSET d:{i} failed: {r:?}");
    }
}

/// Returns the set of keys FT.SEARCH finds for a KNN-k probe.
fn search_keys(c: &mut Client, k: u32, probe_seed: u32) -> Vec<String> {
    let query = format!("*=>[KNN {k} @vec $B]");
    let blob = vec_blob(probe_seed);
    let r = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        query.as_bytes(),
        b"PARAMS",
        b"2",
        b"B",
        &blob,
        b"DIALECT",
        b"2",
    ]);
    let V::Arr(items) = r else {
        panic!("FT.SEARCH reply not array: {r:?}");
    };
    // Reply shape: [total, key1, fields1, key2, fields2, ...]
    items[1..]
        .iter()
        .step_by(2)
        .filter_map(|v| match v {
            V::Bulk(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect()
}

fn assert_absent(keys: &[String], dead: &str, ctx: &str) {
    assert!(
        !keys.iter().any(|k| k == dead),
        "{ctx}: deleted key {dead} resurfaced in FT.SEARCH results {keys:?}"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_del_unindexes_vector_conn_local() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let _guard = spawn_moon(port, dir.path(), 1);
    let mut c = wait_ready(port);

    ft_create(&mut c);
    hset_vectors(&mut c, 0..6);
    let before = search_keys(&mut c, 6, 0);
    assert!(
        before.iter().any(|k| k == "d:1"),
        "d:1 must be indexed before DEL (got {before:?})"
    );

    assert_eq!(c.cmd(&[b"DEL", b"d:1"]), V::Int(1), "DEL d:1");
    let after = search_keys(&mut c, 6, 0);
    assert_absent(&after, "d:1", "conn-local DEL (shards=1)");
}

#[test]
fn test_unlink_unindexes_vector_conn_local() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let _guard = spawn_moon(port, dir.path(), 1);
    let mut c = wait_ready(port);

    ft_create(&mut c);
    hset_vectors(&mut c, 0..6);
    assert_eq!(c.cmd(&[b"UNLINK", b"d:2"]), V::Int(1), "UNLINK d:2");
    let after = search_keys(&mut c, 6, 0);
    assert_absent(&after, "d:2", "conn-local UNLINK (shards=1)");
}

#[test]
fn test_multi_exec_del_unindexes_vector() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let _guard = spawn_moon(port, dir.path(), 1);
    let mut c = wait_ready(port);

    ft_create(&mut c);
    hset_vectors(&mut c, 0..6);

    assert_eq!(c.cmd(&[b"MULTI"]), V::Simple("OK".into()));
    assert_eq!(c.cmd(&[b"DEL", b"d:3"]), V::Simple("QUEUED".into()));
    let exec = c.cmd(&[b"EXEC"]);
    assert!(
        matches!(&exec, V::Arr(rs) if rs.first() == Some(&V::Int(1))),
        "EXEC should report DEL=1: {exec:?}"
    );

    let after = search_keys(&mut c, 6, 0);
    assert_absent(&after, "d:3", "MULTI/EXEC DEL (shards=1)");
}

#[test]
fn test_pipelined_del_unindexes_vector_multishard() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let _guard = spawn_moon(port, dir.path(), 4);
    let mut c = wait_ready(port);

    ft_create(&mut c);
    hset_vectors(&mut c, 0..8);

    // One wire write carrying several DELs: exercises the batched/pipelined
    // dispatch arms at shards=4 (whichever arm handles it, the vector must go).
    let dels: Vec<Vec<Vec<u8>>> = (4..7)
        .map(|i| vec![b"DEL".to_vec(), format!("d:{i}").into_bytes()])
        .collect();
    for (i, r) in c.pipeline(&dels).iter().enumerate() {
        assert_eq!(*r, V::Int(1), "pipelined DEL #{i}");
    }

    let after = search_keys(&mut c, 8, 0);
    for dead in ["d:4", "d:5", "d:6"] {
        assert_absent(&after, dead, "pipelined DEL (shards=4)");
    }
}
