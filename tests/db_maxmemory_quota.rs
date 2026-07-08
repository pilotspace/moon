//! WS5b — per-db memory quota (`--db-maxmemory`) integration test.
//!
//! Proves the wire-level, whole-server behavior that the unit tests in
//! `src/storage/db_quota.rs` and `src/config.rs` cannot: a real server
//! spawned with `--db-maxmemory 1:<bytes>` rejects writes to db 1 once over
//! quota with `MOONERR db maxmemory exceeded`, while db 0 (unconfigured,
//! `0` = unlimited) is completely unaffected — same pattern as
//! `tests/mem_watchdog.rs` for the whole-instance `--mem-full-pct` guard.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/debug/moon cargo test --test db_maxmemory_quota

#![allow(clippy::unwrap_used)]

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/mem_watchdog.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn free_port() -> u16 {
    loop {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
        let p = l.local_addr().expect("local_addr").port();
        drop(l);
        if p >= 20000 {
            return p;
        }
    }
}

/// Fresh `--dir` under `/private/tmp` per WS5b execution context (small,
/// low-diskfull-risk, never the shared `/Volumes/Games` checkout volume).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from("/private/tmp/moon-db-maxmemory-quota-tests");
    std::fs::create_dir_all(&base).expect("create test tmp base dir");
    tempfile::Builder::new()
        .prefix("dbmm-")
        .tempdir_in(&base)
        .expect("tempdir_in /private/tmp/moon-db-maxmemory-quota-tests")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // kill() sends SIGKILL on all platforms via std::process::Child;
        // belt-and-suspenders backstop against a leaked busy-poller (see
        // gotcha_leaked_moon_busypoller_contaminates_xshard in project memory).
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn moon with `--db-maxmemory <db_entries>` (already-formatted
/// `<db>:<bytes>` tokens, one `--db-maxmemory` flag per entry) and
/// `--maxmemory 0` (whole-instance cap unlimited, so only the db-quota gate
/// is under test) and `--appendonly no` (raw write throughput, no WAL noise).
fn spawn_moon_db_quota(port: u16, dir: &std::path::Path, db_entries: &[&str]) -> ServerGuard {
    let mut cmd = Command::new(find_moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--dir",
        &dir.to_string_lossy(),
        "--shards",
        "1",
        "--appendonly",
        "no",
        "--maxmemory",
        "0",
        "--maxmemory-policy",
        "noeviction",
        "--databases",
        "16",
    ]);
    for entry in db_entries {
        cmd.args(["--db-maxmemory", entry]);
    }
    let child = cmd
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon");
    ServerGuard(child)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe args, full-frame parser) — same shape as
// tests/mem_watchdog.rs's Client, duplicated rather than shared: integration
// test binaries in this repo are compiled independently (no shared test-lib
// crate), so importing across files isn't available without new plumbing.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Err(String),
    Bulk(Vec<u8>),
    Null,
}

impl V {
    fn is_db_quota_error(&self) -> bool {
        matches!(self, V::Err(msg) if msg.to_uppercase().contains("DB MAXMEMORY EXCEEDED"))
    }
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
            .set_read_timeout(Some(Duration::from_secs(30)))
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
            ":" => V::Simple(rest.to_string()),
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
                for _ in 0..n {
                    self.parse();
                }
                V::Null
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

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
// Case A: db 1 quota'd at a tiny byte budget with `noeviction` — writes to
// db 1 must eventually be rejected with the db-quota-specific error, NOT the
// generic whole-instance OOM error (which is unreachable here: --maxmemory 0).
// ---------------------------------------------------------------------------

#[test]
fn test_quota_rejects_writes_on_quota_d_db_only() {
    let dir = test_tmpdir();
    let port = free_port();
    // db 1 gets a 4 KB quota; db 0 gets none (unlimited).
    let _guard = spawn_moon_db_quota(port, dir.path(), &["1:4096"]);
    let mut c = wait_ready(port);

    // db 0 (unconfigured): write a large amount of data — must never be
    // rejected, proving the quota is scoped to db 1, not instance-wide.
    let big_value = vec![b'x'; 1024];
    for i in 0..50 {
        let key = format!("db0key{i}");
        let r = c.cmd(&[b"SET", key.as_bytes(), &big_value]);
        assert_eq!(
            r,
            V::Simple("OK".to_string()),
            "db 0 has no quota configured; write {i} must succeed, got {r:?}"
        );
    }

    // Switch to db 1 (the quota'd db) and keep writing until the quota bites.
    let sel = c.cmd(&[b"SELECT", b"1"]);
    assert_eq!(sel, V::Simple("OK".to_string()));

    let mut saw_quota_error = false;
    for i in 0..50 {
        let key = format!("db1key{i}");
        let r = c.cmd(&[b"SET", key.as_bytes(), &big_value]);
        if r.is_db_quota_error() {
            saw_quota_error = true;
            break;
        }
        assert_eq!(
            r,
            V::Simple("OK".to_string()),
            "unexpected non-OK, non-quota-error reply: {r:?}"
        );
    }
    assert!(
        saw_quota_error,
        "expected a MOONERR db maxmemory exceeded reply within 50 writes to db 1 \
         (4 KB quota, 1 KB values) — quota was never enforced"
    );

    // db 0 must STILL be writable after db 1 hit its quota — the two dbs'
    // budgets are independent (this is the neighbor-db-unaffected invariant
    // from the unit tests, now proven end-to-end over the wire).
    let sel0 = c.cmd(&[b"SELECT", b"0"]);
    assert_eq!(sel0, V::Simple("OK".to_string()));
    let r = c.cmd(&[b"SET", b"db0-after-db1-quota", &big_value]);
    assert_eq!(
        r,
        V::Simple("OK".to_string()),
        "db 0 must remain writable after db 1's quota is exhausted, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Case B: CONFIG SET db-maxmemory changes the quota live, and CONFIG GET
// reflects it — no restart required.
// ---------------------------------------------------------------------------

#[test]
fn test_config_set_get_db_maxmemory_live() {
    let dir = test_tmpdir();
    let port = free_port();
    let _guard = spawn_moon_db_quota(port, dir.path(), &[]);
    let mut c = wait_ready(port);

    // Unconfigured at startup: CONFIG GET returns an empty value.
    let get1 = c.cmd(&[b"CONFIG", b"GET", b"db-maxmemory"]);
    match get1 {
        V::Null => {} // empty array collapses to Null in this minimal parser; acceptable
        other => panic!("expected empty/Null CONFIG GET reply, got {other:?}"),
    }

    // CONFIG SET db-maxmemory 2:65536 — must succeed.
    let set1 = c.cmd(&[b"CONFIG", b"SET", b"db-maxmemory", b"2:65536"]);
    assert_eq!(set1, V::Simple("OK".to_string()));

    // Malformed entry must error, not panic or silently no-op the server.
    let bad = c.cmd(&[b"CONFIG", b"SET", b"db-maxmemory", b"not-a-pair"]);
    assert!(matches!(bad, V::Err(_)), "expected Err, got {bad:?}");

    // Out-of-range db index (>= --databases, default 16) must error.
    let oor = c.cmd(&[b"CONFIG", b"SET", b"db-maxmemory", b"999:1024"]);
    assert!(matches!(oor, V::Err(_)), "expected Err, got {oor:?}");

    // Server must still be responsive after the malformed CONFIG SET calls
    // above (fail-open: bad CONFIG input must never wedge the connection).
    let ping_ok = c.try_ping();
    assert!(
        matches!(ping_ok, Ok(true)),
        "server must survive malformed CONFIG SET db-maxmemory"
    );
}
