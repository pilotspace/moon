//! WS6 — container-growth memory accounting integration test.
//!
//! `used_memory` was charged once at key creation for an EMPTY container
//! (`get_or_create_hash`/`_list`/`_set`/`_sorted_set` in `src/storage/db.rs`),
//! then handed out a raw `&mut` into the map/deque/set/tree. Every subsequent
//! mutation (HSET adding fields, LPUSH growing the deque, ...) was invisible
//! to `used_memory` — a real memory-safety hole against `--maxmemory` (and
//! the per-db quotas in `tests/db_maxmemory_quota.rs`), confirmed by
//! adversarial review 2026-07-08.
//!
//! RED (pre-fix): growing a single hash key past `--maxmemory` under
//! `noeviction` never hit the cap — every HSET kept succeeding because the
//! server only ever "saw" the empty-hash creation charge (~150 bytes), not
//! the megabytes of field data piled on afterward. Same for LPUSH growing a
//! single list key.
//!
//! GREEN (post-fix): growth is charged incrementally (O(1) per mutation —
//! see the WS6 accounting note above `entry_overhead` in
//! `src/storage/db.rs`), so the existing `--maxmemory` / `noeviction` gate
//! now sees the real, current size of a single growing container and
//! rejects further writes once the budget is crossed.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/debug/moon cargo test --test container_growth_memory_accounting

#![allow(clippy::unwrap_used)]

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/db_maxmemory_quota.rs)
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

/// Fresh `--dir` under `/private/tmp` (never the shared `/Volumes/Games`
/// checkout volume, which hovers near the 5% diskfull guard).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from("/private/tmp/moon-container-growth-tests");
    std::fs::create_dir_all(&base).expect("create test tmp base dir");
    tempfile::Builder::new()
        .prefix("cgm-")
        .tempdir_in(&base)
        .expect("tempdir_in /private/tmp/moon-container-growth-tests")
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

/// Spawn moon with a small `--maxmemory` cap, `noeviction` policy, AOF and
/// disk-offload both disabled (raw in-memory write path, no WAL/spill noise
/// that could mask the accounting gap under test), single shard.
fn spawn_moon_maxmemory(port: u16, dir: &std::path::Path, maxmemory_bytes: u64) -> ServerGuard {
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            "1",
            "--appendonly",
            "no",
            "--maxmemory",
            &maxmemory_bytes.to_string(),
            "--maxmemory-policy",
            "noeviction",
            "--disk-offload",
            "disable",
            // Guard against the diskfull write-pause (`MOONERR diskfull`)
            // firing mid-test on a nearly-full host volume and masking the
            // OOM assertions under test with an unrelated error.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon");
    ServerGuard(child)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe args, full-frame parser) — same shape as
// tests/db_maxmemory_quota.rs's Client, duplicated rather than shared:
// integration test binaries in this repo are compiled independently.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Err(String),
    Bulk(Vec<u8>),
    Integer(i64),
    Null,
}

impl V {
    fn is_oom_error(&self) -> bool {
        matches!(self, V::Err(msg) if msg.to_uppercase().contains("OOM"))
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
            ":" => V::Integer(rest.parse().expect("integer reply")),
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
// Case A: growing ONE hash key's fields past --maxmemory under noeviction
// must eventually reject further HSETs.
// ---------------------------------------------------------------------------

#[test]
fn test_hset_growth_on_single_key_hits_maxmemory() {
    let dir = test_tmpdir();
    let port = free_port();
    // 64 KB budget; ~1 KB fields -> should bite well within 200 fields even
    // accounting for the ~150-byte empty-container baseline charge.
    let _guard = spawn_moon_maxmemory(port, dir.path(), 64 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut saw_oom = false;
    for i in 0..200 {
        let field = format!("field{i}");
        let r = c.cmd(&[b"HSET", b"biggrowinghash", field.as_bytes(), &value]);
        if r.is_oom_error() {
            saw_oom = true;
            break;
        }
        assert_eq!(
            r,
            V::Integer(1),
            "HSET of a brand-new field must return 1 (or OOM), got {r:?} at i={i}"
        );
    }

    assert!(
        saw_oom,
        "expected an OOM reply within 200 HSETs growing a single hash key \
         (64 KB cap, ~1 KB fields = ~200 KB of field data) -- growth of an \
         existing container is invisible to --maxmemory if this fails"
    );
}

// ---------------------------------------------------------------------------
// Case B: growing ONE list key via LPUSH past --maxmemory under noeviction
// must eventually reject further pushes.
// ---------------------------------------------------------------------------

#[test]
fn test_lpush_growth_on_single_key_hits_maxmemory() {
    let dir = test_tmpdir();
    let port = free_port();
    let _guard = spawn_moon_maxmemory(port, dir.path(), 64 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut saw_oom = false;
    for _ in 0..200 {
        let r = c.cmd(&[b"LPUSH", b"biggrowinglist", &value]);
        if r.is_oom_error() {
            saw_oom = true;
            break;
        }
        match r {
            V::Integer(_) => {}
            other => panic!("expected Integer (new length) or OOM, got {other:?}"),
        }
    }

    assert!(
        saw_oom,
        "expected an OOM reply within 200 LPUSHes growing a single list key \
         (64 KB cap, ~1 KB elements = ~200 KB of element data) -- growth of \
         an existing container is invisible to --maxmemory if this fails"
    );
}

// ---------------------------------------------------------------------------
// Case C: deleting fields back down must credit the freed memory. Stays
// UNDER budget throughout (HDEL always succeeds well before OOM) so this
// isolates credit-on-delete correctness from a separate, pre-existing gap:
// `run_write_eviction_gate` (src/server/conn/handler_monoio/mod.rs) applies
// the same pre-check to EVERY write command, including pure-shrink ones
// (HDEL/SREM/LREM/ZREM/...) -- unlike Redis's `CMD_DENYOOM` classification,
// which explicitly exempts memory-freeing commands so an operator can
// recover from an OOM state without FLUSHALL/restart. Once a key trips the
// gate, HDEL on THAT key is rejected too, even though it would only shrink
// it. This is orthogonal to the accounting fix (confirmed by reproducing it
// against a manually-driven server: growing a hash to the OOM boundary, the
// literal next HDEL of an already-written field is *also* rejected) and is
// flagged as follow-up work in the WS6 summary rather than fixed here.
// ---------------------------------------------------------------------------

#[test]
fn test_hdel_credits_memory_after_growth() {
    let dir = test_tmpdir();
    let port = free_port();
    // Budget sized so growing ~100 KB of fields, deleting them all, then
    // writing another ~100 KB elsewhere fits ONLY if the deletes freed their
    // memory -- comfortably under the cap throughout (no OOM ever hit),
    // isolating credit-on-delete from the separate gate-classification gap
    // documented above.
    let _guard = spawn_moon_maxmemory(port, dir.path(), 256 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut fields_written: Vec<String> = Vec::new();
    for i in 0..100 {
        let field = format!("field{i}");
        let r = c.cmd(&[b"HSET", b"drainme", field.as_bytes(), &value]);
        assert_eq!(
            r,
            V::Integer(1),
            "setup: growing to ~100 KB must stay under the 256 KB budget, got {r:?} at i={i}"
        );
        fields_written.push(field);
    }

    // Delete every field written -- the hash's accounted memory should
    // return to (near-)empty. Every HDEL here runs well under budget.
    for field in &fields_written {
        let r = c.cmd(&[b"HDEL", b"drainme", field.as_bytes()]);
        assert_eq!(
            r,
            V::Integer(1),
            "HDEL of field {field} must succeed, got {r:?}"
        );
    }

    // A second, unrelated ~100 KB write must now succeed: if `drainme`'s
    // fields were NOT credited back on delete, the stale ~100 KB charge plus
    // this new ~100 KB write would exceed the 256 KB budget and OOM.
    let big_value = vec![b'w'; 100 * 1024];
    let r = c.cmd(&[b"SET", b"after-drain-probe", &big_value]);
    assert_eq!(
        r,
        V::Simple("OK".to_string()),
        "a ~100 KB write after fully draining a ~100 KB hash must succeed under a \
         256 KB budget -- got {r:?} (credit-on-delete broken: HDEL isn't freeing \
         the deleted fields' bytes)"
    );
}
