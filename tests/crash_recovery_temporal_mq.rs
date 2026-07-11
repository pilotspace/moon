//! Task #42 (P0 data loss): `replay_mq_wal` / `replay_temporal_wal` crash
//! recovery. Companion of `tests/shardslice_live.rs`'s
//! `test_ssm3_ws_registry_survives_restart` (same bug class, same fix
//! shape) — see `.planning/reviews/storage-audit-2026-07-12-wal.md` §5.2.
//!
//! Both replay functions (`src/shard/shared_databases.rs`) have two bugs:
//!   (a) they scan `persistence_dir.join("shard-{id}")` instead of
//!       `.join("shard-{id}").join("wal-v3")` — `std::fs::read_dir` is
//!       non-recursive, so they scan an EMPTY directory every boot and
//!       replay ZERO records.
//!   (b) even with the dir fixed, they match `record.record_type` directly.
//!       Every cross-thread `wal_append` blob is re-wrapped as
//!       `WalRecordType::Command` by the shard event loop drain
//!       (`event_loop.rs` ~1484-1491, ~2101-2108) — the typed inner record
//!       (`MqCreate`/`MqAck`/`GraphTemporal`) is nested in the Command's
//!       payload. `replay_workspace_wal` (same file, ~298-402) is the ONLY
//!       replay function that unwraps this nesting; `replay_mq_wal` and
//!       `replay_temporal_wal` are blind to their own records even once (a)
//!       is fixed.
//!
//! Observable proofs (both exercise real durable state, not internal
//! counters):
//!
//! MQ: `Stream::durable` / `Stream::max_delivery_count` are NOT part of the
//! RDB/`.rrdshard` wire format (`src/persistence/rdb.rs` TYPE_STREAM body:
//! entry count, last_id, entries, groups/PEL/consumers — no durable flag).
//! So a durable queue that survives a crash via a `.rrdshard` snapshot comes
//! back with `durable=false` (`Stream::new()`'s default) UNLESS
//! `replay_mq_wal` re-applies the `MqCreate` WAL record.
//!
//! The MQ replay fix is proven by two white-box unit tests instead of a
//! live-server round trip here — `src/shard/shared_databases.rs`
//! `tests::test_replay_mq_wal_restores_registry_and_rolls_back_cursor` and
//! `tests::test_replay_mq_wal_missing_wal_v3_subdir_is_a_noop`. Reason: a
//! live MQ.CREATE -> kill -9 -> restart -> MQ.PUSH round trip requires
//! `--appendonly yes` (WAL v3 only exists when `appendonly_enabled`, see
//! `event_loop.rs`'s `wal_shard_dir` gate), but `--appendonly yes` triggers
//! an UNCONDITIONAL AOF-authority recovery path (`main.rs`: `db.clear()` on
//! every shard, then rebuild solely from the AOF manifest) whenever a
//! manifest exists. MQ.CREATE/PUSH/POP/ACK are intercepted before the
//! generic AOF-logging dispatch path (`handler_sharded/mod.rs`, `continue`
//! after each `try_handle_*`), so the Stream itself is NEVER AOF-logged —
//! the AOF-authority wipe discards it on every restart regardless of
//! whether `replay_mq_wal` is fixed. That is a distinct, deeper,
//! out-of-scope durability gap (MQ has zero AOF durability today), not the
//! two task #42 defects (wrong dir + blind to nested `Command` framing) this
//! change fixes. Making it observable end-to-end needs either MQ AOF
//! logging or a `.rrdshard`-only (non-AOF) recovery path — both are
//! restructuring work forbidden by task #42's minimal-fix scope. The unit
//! tests instead drive `replay_mq_wal` directly against real WAL v3 bytes
//! written with the production encoder (`write_wal_v3_record` +
//! `WalWriterV3`), which isolates exactly the two fixed defects.
//!
//! TEMPORAL: `TEMPORAL.INVALIDATE` sets `valid_to` on the node/edge living
//! in the graph's mutable `write_buf` and drains a `GraphTemporal` WAL
//! record (`src/command/temporal.rs::apply_invalidate`). The node itself is
//! durable via the (already-correct) graph WAL v3 command replay
//! (`GRAPH.ADDNODE` re-executes with a stable id), but that replay recreates
//! the node with the DEFAULT `valid_to = i64::MAX` — only `replay_temporal_wal`
//! restores the invalidation. `GRAPH.QUERY ... VALID_AT <far-future-ms>`
//! (same oracle as `tests/replication_graph.rs::replica_applies_temporal_invalidate`)
//! is the black-box proof: the node must NOT be visible past its `valid_to`.
//! Unlike MQ, `GRAPH.ADDNODE` DOES go through the normal WAL v3 command path,
//! so a live kill -9 round trip is a faithful, reachable proof here.
//!
//! Run with (release binary required):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_temporal_mq -- --ignored --test-threads=1

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution — honours MOON_BIN, falls back to release then debug.
// Pattern: tests/shardslice_live.rs::find_moon_binary
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

// ---------------------------------------------------------------------------
// Server spawn helpers (pattern: tests/shardslice_live.rs)
// ---------------------------------------------------------------------------

fn spawn_moon(dir: &std::path::Path, shards: u32, extra: &[&str]) -> (Child, u16) {
    common::spawn_listening(|port| {
        let mut args: Vec<String> = vec![
            "--port".into(),
            port.to_string(),
            "--dir".into(),
            dir.to_string_lossy().into_owned(),
            "--shards".into(),
            shards.to_string(),
        ];
        for &e in extra {
            args.push(e.into());
        }
        Command::new(find_moon_binary())
            .args(&args)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
            .env("RUST_LOG", "moon=info")
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to spawn moon binary at '{}': {e}. Build with \
                     `cargo build [--release]` or set MOON_BIN.",
                    find_moon_binary().display()
                )
            })
    })
}

/// RAII guard: SIGKILLs the server process when dropped (`Child::kill` is
/// SIGKILL on Unix — no graceful shutdown, matching the "kill -9" scenario).
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
        .expect("parse addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(10))).ok();
                s.set_write_timeout(Some(Duration::from_secs(10))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on port {port}: {e}"),
        }
    }
}

fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf) {
            if n > 0 && buf[..n].windows(4).any(|w| w == b"PONG") {
                return s;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "server accepted TCP but never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP2 client (self-contained; copied from tests/shardslice_live.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

impl Resp {
    fn flat(&self) -> String {
        match self {
            Resp::Simple(s) | Resp::Error(s) => s.clone(),
            Resp::Int(i) => i.to_string(),
            Resp::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Resp::Bulk(None) => "<nil>".into(),
            Resp::Array(Some(items)) => items.iter().map(Resp::flat).collect::<Vec<_>>().join(" "),
            Resp::Array(None) => "<nil-array>".into(),
        }
    }
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(s: TcpStream) -> Self {
        Conn {
            s,
            buf: Vec::with_capacity(16 * 1024),
            pos: 0,
        }
    }

    fn open(port: u16) -> Self {
        Conn::new(connect(port, Duration::from_secs(10)))
    }

    fn cmd(&mut self, parts: &[&[u8]]) -> Resp {
        let mut req = Vec::with_capacity(128);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p);
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn cmd_s(&mut self, parts: &[&str]) -> Resp {
        let v: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.cmd(&v)
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.s.read(&mut chunk).expect("read from server");
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
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// MQ.CREATE / MQ.ACK durability: see the module doc comment above for why
// this is proven by white-box unit tests in
// `src/shard/shared_databases.rs` (`test_replay_mq_wal_restores_registry_and_rolls_back_cursor`,
// `test_replay_mq_wal_missing_wal_v3_subdir_is_a_noop`) rather than a live
// server round trip in this file — a live MQ.CREATE round trip requires
// `--appendonly yes`, which triggers an unrelated, out-of-scope
// AOF-authority wipe that discards the Stream (MQ has zero AOF durability)
// regardless of whether `replay_mq_wal` itself is correct.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TEMPORAL.INVALIDATE durability
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn temporal_invalidate_survives_kill9() {
    // ~ year 2255 in Unix ms: beyond "now", far short of i64::MAX (the
    // default `valid_to` a freshly-replayed GRAPH.ADDNODE gets — same
    // constant as tests/replication_graph.rs::replica_applies_temporal_invalidate).
    const FAR_FUTURE_MS: &str = "9000000000000";
    let dir = tempfile::tempdir().expect("tempdir");
    let extra: &[&str] = &["--appendonly", "yes", "--disk-free-min-pct", "0"];

    // --- Round 1: create a graph node (durable via the ALREADY-correct
    // graph WAL v3 command replay) and TEMPORAL.INVALIDATE it (durable via
    // the GraphTemporal WAL record — the thing under test), then kill -9. ---
    let node_id: String;
    {
        let (child, port) = spawn_moon(dir.path(), 1, extra);
        let _guard = ServerGuard(child);
        drop(wait_ready(port));

        let mut c = Conn::open(port);
        assert_eq!(
            c.cmd_s(&["GRAPH.CREATE", "tg"]),
            Resp::Simple("OK".into()),
            "GRAPH.CREATE must succeed"
        );
        let reply = c.cmd_s(&["GRAPH.ADDNODE", "tg", "Person", "name", "alice"]);
        node_id = match reply {
            Resp::Int(id) => id.to_string(),
            other => panic!("GRAPH.ADDNODE must return an id: {other:?}"),
        };

        // Sanity: the node is visible far in the future BEFORE invalidation.
        let visible = c
            .cmd_s(&[
                "GRAPH.QUERY",
                "tg",
                "MATCH (n:Person) RETURN n.name",
                "VALID_AT",
                FAR_FUTURE_MS,
            ])
            .flat();
        assert!(
            visible.contains("alice"),
            "node must be visible pre-invalidation at a far-future VALID_AT (got: {visible})"
        );

        assert_eq!(
            c.cmd_s(&["TEMPORAL.INVALIDATE", &node_id, "NODE", "tg"]),
            Resp::Simple("OK".into()),
            "TEMPORAL.INVALIDATE must succeed"
        );

        // Master-side sanity: invalidation is observable immediately.
        let invalidated = c
            .cmd_s(&[
                "GRAPH.QUERY",
                "tg",
                "MATCH (n:Person) RETURN n.name",
                "VALID_AT",
                FAR_FUTURE_MS,
            ])
            .flat();
        assert!(
            !invalidated.contains("alice"),
            "node must NOT be visible past its own valid_to pre-restart (got: {invalidated})"
        );

        // Let the 1ms WAL flush tick drain the GraphTemporal WAL v3 record
        // before the kill.
        std::thread::sleep(Duration::from_millis(1500));
        // _guard dropped here -> SIGKILL
    }

    // --- Round 2: restart on the SAME dir. Graph WAL v3 command replay
    // recreates the node with the DEFAULT valid_to = i64::MAX; only a
    // correct replay_temporal_wal restores the invalidation. ---
    {
        let (child2, port2) = spawn_moon(dir.path(), 1, extra);
        let _guard2 = ServerGuard(child2);
        drop(wait_ready(port2));

        let mut c = Conn::open(port2);
        let after_restart = c
            .cmd_s(&[
                "GRAPH.QUERY",
                "tg",
                "MATCH (n:Person) RETURN n.name",
                "VALID_AT",
                FAR_FUTURE_MS,
            ])
            .flat();
        assert!(
            !after_restart.contains("alice"),
            "after restart, the node (id {node_id}) must still NOT be visible past its \
             valid_to (got: {after_restart}).\n\
             RED: replay_temporal_wal scans shard-{{id}}/ but WAL v3 segments live in \
             shard-{{id}}/wal-v3/, AND doesn't unwrap the nested Command framing -- the \
             GraphTemporal record is never replayed, so the node comes back from graph \
             WAL replay with its DEFAULT valid_to = i64::MAX and the invalidation is \
             silently lost (pre-existing bug; fixed by task #42)."
        );
    }
}
