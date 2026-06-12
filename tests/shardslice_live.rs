//! ADD task `shardslice-migration` — live-server integration tests (phase: tests).
//!
//! Tests in this file cover scenarios ssm1, ssm3, ssm4a, and the
//! `reject_uninitialized_shard` wire guard.
//!
//! RED today:
//!   - test_ssm1_slice_live_at_startup — FAILS because `init_shard` has zero
//!     production call sites; no "ShardSlice initialized" line is emitted in
//!     shard startup log output (frozen contract C1, M1).
//!
//! GREEN today (behavioral oracles, must keep passing through the cutover):
//!   - test_ssm3_ws_global_registry — WS CREATE/AUTH/LIST survive restart;
//!     works today via the per-shard lock path; must keep passing after cutover
//!     via the process-global registry (contract C3).
//!   - test_ssm4a_aof_fold_cooperative — exactly-once INCR across a
//!     BGREWRITEAOF boundary; currently correct via RwLock fold; must keep
//!     passing after the cooperative-snapshot redesign (contract C4).
//!   - test_reject_uninitialized_shard_no_wire_panic — wire-level guard: no
//!     junk command + normal SET/GET causes a with_shard panic reachable from
//!     the network; the server stays alive.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/release/moon \
//!     cargo test --test shardslice_live 2>&1 | tail -40

#![allow(clippy::unwrap_used)]

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution — honours MOON_BIN, falls back to release then debug.
// Pattern: tests/txn_kv_wiring.rs::find_moon_binary
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
    panic!(
        "No moon binary found. Build with `cargo build [--release]` or set \
         MOON_BIN=/path/to/moon."
    );
}

// ---------------------------------------------------------------------------
// Port allocation — bind-to-0 trick, freed before server binds.
// ---------------------------------------------------------------------------

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

// ---------------------------------------------------------------------------
// Server spawn helpers
// ---------------------------------------------------------------------------

/// Spawn a moon server.
///
/// stdout + stderr are written to log files under `dir` so callers can
/// inspect the captured output for log-line assertions.
///
/// RUST_LOG=moon=info is set so startup info lines (including any future
/// "ShardSlice initialized" markers) are emitted.
///
/// ALWAYS wrap the returned `Child` in a `ServerGuard` immediately so that
/// test failures do not leak the process.
fn spawn_moon(port: u16, dir: &std::path::Path, shards: u32, extra: &[&str]) -> Child {
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
}

/// RAII guard: kills the server process when dropped.
///
/// Wrap the `Child` in this immediately after `spawn_moon` so that test
/// panics and early returns cannot leak the process.
struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ---------------------------------------------------------------------------
// Connection helpers (deadline-based, no fixed sleep)
// ---------------------------------------------------------------------------

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

/// Poll until the server answers PING→PONG. Returns the live connection.
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
// Minimal RESP2 client (self-contained; copied helpers from
// cross_shard_consistency_red.rs; not imported across test files)
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

    /// Send a pipelined batch of raw RESP bytes and read back `count` frames.
    fn pipeline(&mut self, raw: &[u8], count: usize) -> Vec<Resp> {
        self.s.write_all(raw).expect("pipeline write");
        (0..count).map(|_| self.frame()).collect()
    }
}

// ---------------------------------------------------------------------------
// Log helpers: read the captured log files for a running server.
// ---------------------------------------------------------------------------

fn read_log(dir: &std::path::Path) -> String {
    let mut out = String::new();
    for name in &["moon.stdout.log", "moon.stderr.log"] {
        if let Ok(content) = std::fs::read_to_string(dir.join(name)) {
            out.push_str(&content);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Helper: detect a seq>1 base RDB file (proves BGREWRITEAOF physically ran).
// Mirrors crash_matrix_per_shard_bgrewriteaof.rs::compacted_base_exists.
//
// Recognises three layouts:
//   PerShard:   appendonlydir/shard-*/moon.aof.<seq>.base.rdb   (seq > 1)
//   TopLevel:   appendonlydir/moon.aof.<seq>.base.rdb            (seq > 1)
//               (also searched directly under `dir` as a fallback)
//   tokio-TopLevel (in-place rewrite): <dir>/appendonly.aof starts with
//               the 4-byte RDB magic b"MOON" — proves a full rewrite
//               completed; the pre-rewrite file is raw RESP (starts with '*'
//               or '+'), so the magic check is sufficient.
// ---------------------------------------------------------------------------

fn compacted_base_exists(dir: &std::path::Path) -> bool {
    let is_seq_gt1 = |name: &str| -> bool {
        if let Some(rest) = name.strip_prefix("moon.aof.") {
            if let Some(seq_str) = rest.strip_suffix(".base.rdb") {
                return seq_str.parse::<u64>().map(|s| s > 1).unwrap_or(false);
            }
        }
        false
    };

    let aof_dir = dir.join("appendonlydir");

    // TopLevel layout: base files live directly in appendonlydir/ (or `dir`).
    for search_dir in &[&aof_dir, dir] {
        if let Ok(files) = std::fs::read_dir(search_dir) {
            for f in files.flatten() {
                let name = f.file_name().to_string_lossy().to_string();
                if is_seq_gt1(&name) {
                    return true;
                }
            }
        }
    }

    // PerShard layout: appendonlydir/shard-*/moon.aof.<seq>.base.rdb
    if let Ok(shards) = std::fs::read_dir(&aof_dir) {
        for entry in shards.flatten() {
            let p = entry.path();
            if !p.is_dir() {
                continue;
            }
            if let Ok(files) = std::fs::read_dir(&p) {
                for f in files.flatten() {
                    let name = f.file_name().to_string_lossy().to_string();
                    if is_seq_gt1(&name) {
                        return true;
                    }
                }
            }
        }
    }

    // tokio TopLevel (--shards 1) rewrites aof_path in-place: the legacy
    // single AOF file gets atomically replaced with an RDB-preamble snapshot.
    // No manifest-stamped base.rdb is produced.  Detect by checking whether
    // appendonly.aof (the default appendfilename) starts with b"MOON".
    // Also check directly under appendonlydir/ as a belt-and-suspenders search.
    for candidate in &[dir.join("appendonly.aof"), aof_dir.join("appendonly.aof")] {
        if let Ok(mut f) = std::fs::File::open(candidate) {
            use std::io::Read as _;
            let mut magic = [0u8; 4];
            if f.read_exact(&mut magic).is_ok() && &magic == b"MOON" {
                return true;
            }
        }
    }

    false
}

/// Return the `seq` field from the PerShard AOF manifest, or 0 if absent/unparseable.
fn aof_manifest_seq(dir: &std::path::Path) -> u64 {
    let p = dir.join("appendonlydir").join("moon.aof.manifest");
    let text = match std::fs::read_to_string(&p) {
        Ok(t) => t,
        Err(_) => return 0,
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("seq ") {
            if let Ok(n) = rest.trim().parse::<u64>() {
                return n;
            }
        }
    }
    0
}

// ===========================================================================
// TESTS
// ===========================================================================

// ---------------------------------------------------------------------------
// test_ssm1_slice_live_at_startup
//
// RED today: `init_shard` has ZERO production call sites. No
// "ShardSlice initialized" log line is emitted for any shard. This test
// FAILS on the log-marker assertion → the correct RED signal.
//
// Contract C1 (frozen): init_shard logs "ShardSlice initialized" (with the
// shard id) exactly once per shard thread, before the first connection is
// accepted. After the Build phase lands this will turn green.
// ---------------------------------------------------------------------------

#[test]
fn test_ssm1_slice_live_at_startup() {
    const SHARDS: u32 = 4;
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");

    // Use --appendonly no so the test is not slowed by WAL replay.
    let _guard = ServerGuard(spawn_moon(
        port,
        dir.path(),
        SHARDS,
        &["--appendonly", "no"],
    ));

    // Wait for the server to accept connections (all 4 shards have started
    // and the first accept loop iteration has run).
    drop(wait_ready(port));

    // Give the shards a moment to flush their startup log lines to disk.
    std::thread::sleep(Duration::from_millis(400));
    let log = read_log(dir.path());

    // --- Contract C1 assertion (RED today) ---
    //
    // The frozen contract mandates ONE "ShardSlice initialized" log line per
    // shard before its first accept. Today `init_shard` is never called in
    // production so this marker is absent → the assertion fails → correct
    // RED signal.
    //
    // The shard id must also appear on or near the same line. We check both
    // substrings anywhere in the combined log; ordering is not asserted
    // (they are on the same tracing event by the contract).
    let has_marker = log.contains("ShardSlice initialized");
    assert!(
        has_marker,
        "expected log to contain \"ShardSlice initialized\" for at least one shard, \
         but the marker is absent.\n\
         RED: init_shard is not wired at shard startup (frozen contract C1, M1).\n\
         Log excerpt (last 3000 chars):\n{}",
        &log[log.len().saturating_sub(3000)..]
    );

    // If the marker is present, verify each shard id 0..4 is covered.
    // This part will also fail today (marker is absent above), but makes the
    // per-shard coverage check explicit for when the Build phase lands.
    for shard_id in 0..SHARDS {
        // The log line shape required by C1:
        //   INFO moon::shard::event_loop: ShardSlice initialized shard=N
        // We search for the marker AND the shard id independently; a
        // properly-formatted tracing field "shard=N" or "shard N" both match.
        let id_present = log.contains(&format!("shard={shard_id}"))
            || log.contains(&format!("shard {shard_id}"));
        assert!(
            id_present,
            "expected log to contain shard id {shard_id} alongside \
             \"ShardSlice initialized\". Log excerpt:\n{}",
            &log[log.len().saturating_sub(3000)..]
        );
    }

    // --- Basic wire sanity: server handles commands after startup ---
    let mut c1 = Conn::open(port);
    assert_eq!(
        c1.cmd_s(&["PING"]),
        Resp::Simple("PONG".into()),
        "PING must return PONG"
    );

    let mut c2 = Conn::open(port);
    assert_eq!(
        c2.cmd_s(&["SET", "ssm1:key", "value"]),
        Resp::Simple("OK".into()),
        "SET must return OK"
    );

    let mut c3 = Conn::open(port);
    assert_eq!(
        c3.cmd_s(&["GET", "ssm1:key"]),
        Resp::Bulk(Some(b"value".to_vec())),
        "GET must return the value written by c2"
    );
}

// ---------------------------------------------------------------------------
// test_ssm3_ws_global_registry
//
// GREEN today (behavioral oracle — must KEEP passing through the cutover).
//
// Workspace CREATE/AUTH/LIST are connection-independent. Today this works via
// the per-shard lock path (slot-0 Mutex trick). After the Build phase (M3) it
// works via the process-global WorkspaceRegistry. The assertions are identical
// — the cutover must not regress this.
//
// The restart half of ssm3 lives in test_ssm3_ws_registry_survives_restart
// below — RED today because of a pre-existing bug in `replay_workspace_wal`
// (shared_databases.rs:289): it scans `shard-{id}/` directly while WAL v3
// segments live in `shard-{id}/wal-v3/`, so zero workspace records replay.
// Contract C3 freezes "WAL replay populates the global registry at boot", so
// the Build phase fixes that bug as part of the registry migration.
//
// The compile-shape half of ssm3 (ShardSliceInit loses its workspace_registry
// field) lives in tests/shardslice_shape.rs, not here.
// ---------------------------------------------------------------------------

#[test]
fn test_ssm3_ws_global_registry() {
    const SHARDS: u32 = 4;
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");

    // --- Create workspace, verify from 12 connections ---
    let _guard = ServerGuard(spawn_moon(
        port,
        dir.path(),
        SHARDS,
        &["--appendonly", "yes"],
    ));
    drop(wait_ready(port));

    // Create workspace from connection A.
    let mut ca = Conn::open(port);
    let ws_id = match ca.cmd_s(&["WS", "CREATE", "ssm3ws"]) {
        Resp::Bulk(Some(id)) => String::from_utf8(id).expect("WS CREATE id must be utf8"),
        other => panic!("WS CREATE reply {other:?}"),
    };

    // WS AUTH must succeed from 12 fresh connections (kernel spreads them
    // across shard listeners on Linux; macOS may pin — both must pass).
    //
    // Today: works via the per-shard lock path.
    // Post-cutover (M3): must work via the global registry.
    // THIS IS THE PRIMARY BEHAVIORAL ORACLE — never weaken this assertion.
    for i in 0..12 {
        let mut c = Conn::open(port);
        let r = c.cmd_s(&["WS", "AUTH", &ws_id]);
        assert_eq!(
            r,
            Resp::Simple("OK".into()),
            "conn {i}: WS AUTH must succeed from ANY shard connection (got {r:?})"
        );
        let list_flat = c.cmd_s(&["WS", "LIST"]).flat();
        assert!(
            list_flat.contains("ssm3ws"),
            "conn {i}: WS LIST must include 'ssm3ws' (got: {list_flat})"
        );
    }

    // Workspace-scoped key: bound write visible to bound GET, invisible to
    // an unbound connection — same oracle as cdg6f.
    let mut bound = Conn::open(port);
    assert_eq!(
        bound.cmd_s(&["WS", "AUTH", &ws_id]),
        Resp::Simple("OK".into()),
        "bound conn: WS AUTH must succeed"
    );
    assert_eq!(
        bound.cmd_s(&["SET", "ssm3:wkey", "wsval"]),
        Resp::Simple("OK".into()),
        "bound conn: SET inside workspace must succeed"
    );
    assert_eq!(
        bound.cmd_s(&["GET", "ssm3:wkey"]),
        Resp::Bulk(Some(b"wsval".to_vec())),
        "bound conn: GET must return workspace-scoped value"
    );
    let mut unbound = Conn::open(port);
    assert_eq!(
        unbound.cmd_s(&["GET", "ssm3:wkey"]),
        Resp::Bulk(None),
        "unbound conn: workspace key must be invisible"
    );
    // _guard dropped here → server killed
}

// ---------------------------------------------------------------------------
// test_ssm3_ws_registry_survives_restart
//
// RED today: `replay_workspace_wal` (shared_databases.rs:289) joins
// `shard-{id}` directly, but WAL v3 segments are written under
// `shard-{id}/wal-v3/` (compare recovery.rs:361) — so workspace
// Create/Drop records are never replayed and the registry comes up empty
// after a restart. Frozen contract C3 mandates "WAL replay populates the
// global registry at boot"; the Build phase fixes the directory path as part
// of the global-registry migration, turning this green.
// ---------------------------------------------------------------------------

#[test]
fn test_ssm3_ws_registry_survives_restart() {
    const SHARDS: u32 = 4;
    let dir = tempfile::tempdir().expect("tempdir");

    // --- Round 1: create the workspace with WAL enabled ---
    {
        let port = free_port();
        let _guard = ServerGuard(spawn_moon(
            port,
            dir.path(),
            SHARDS,
            &["--appendonly", "yes"],
        ));
        drop(wait_ready(port));

        let mut ca = Conn::open(port);
        let ws_id = match ca.cmd_s(&["WS", "CREATE", "ssm3rs"]) {
            Resp::Bulk(Some(id)) => String::from_utf8(id).expect("WS CREATE id must be utf8"),
            other => panic!("WS CREATE reply {other:?}"),
        };
        assert_eq!(
            ca.cmd_s(&["WS", "AUTH", &ws_id]),
            Resp::Simple("OK".into()),
            "WS AUTH must succeed pre-restart"
        );
        // Let the 1ms WAL flush tick + everysec window drain before the kill.
        std::thread::sleep(Duration::from_millis(1500));
        // _guard dropped → server killed
    }

    // --- Round 2: restart on the SAME dir; the workspace must be replayed ---
    {
        let port2 = free_port();
        let _guard2 = ServerGuard(spawn_moon(
            port2,
            dir.path(),
            SHARDS,
            &["--appendonly", "yes"],
        ));
        drop(wait_ready(port2));

        let mut c = Conn::open(port2);
        let list_flat = c.cmd_s(&["WS", "LIST"]).flat();
        assert!(
            list_flat.contains("ssm3rs"),
            "after restart, WS LIST must include 'ssm3rs' (got: {list_flat}).\n\
             RED: replay_workspace_wal scans shard-{{id}}/ but WAL v3 segments \
             live in shard-{{id}}/wal-v3/ — zero workspace records replayed \
             (pre-existing bug; fixed by Build under frozen contract C3)."
        );
    }
}

// ---------------------------------------------------------------------------
// test_ssm4a_aof_fold_cooperative
//
// GREEN today (behavioral oracle — must KEEP passing through the cutover).
//
// Exactly-once INCR across a BGREWRITEAOF boundary:
//   1. Write 2000 keys with pipelined SETs.
//   2. Start a writer thread doing INCR on "ssm4a:ctr".
//   3. Issue BGREWRITEAOF.
//   4. Wait ≤30s for the rewrite to settle (poll for seq>1 base file).
//   5. Stop the writer; record exact successful-INCR count.
//   6. Assert GET ssm4a:ctr == recorded count (exactly-once).
//   7. Restart on the same dir; assert counter and key sample survive.
//
// Two variants share one helper:
//   - shards=1 (no flag): BGREWRITEAOF on shards>=2 + appendonly yes is gated
//     (MULTI_SHARD_AOF_REWRITE_UNSAFE) without the experimental flag.
//   - shards=4 + --experimental-per-shard-rewrite: drives
//     do_rewrite_per_shard — the EXACT foreign-lock fold C4 redesigns into
//     the cooperative snapshot. Post-cutover both use the new protocol; the
//     assertions are unchanged.
//
// Total runtime target: < 60s per variant.
// ---------------------------------------------------------------------------

fn aof_fold_exactly_once(shards: u32, extra: &[&str]) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let mut flags: Vec<&str> = vec!["--appendonly", "yes", "--appendfsync", "everysec"];
    flags.extend_from_slice(extra);

    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let recorded_count: i64;

    // --- Round 1: write data, trigger rewrite under concurrent writes ---
    {
        let _guard = ServerGuard(spawn_moon(port, dir.path(), shards, &flags));
        drop(wait_ready(port));

        // Pipeline 2000 SET operations.
        {
            let mut bulk = Conn::open(port);
            let mut req = Vec::with_capacity(80 * 2000);
            for i in 0..2000_u32 {
                let key = format!("ssm4a:k{i}");
                let val = format!("v{i}");
                req.extend_from_slice(
                    format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        val.len(),
                        val
                    )
                    .as_bytes(),
                );
            }
            let responses = bulk.pipeline(&req, 2000);
            for (i, r) in responses.iter().enumerate() {
                assert_eq!(
                    *r,
                    Resp::Simple("OK".into()),
                    "pipelined SET {i} failed: {r:?}"
                );
            }
        }

        // Spin up a writer thread doing INCR on the counter key.
        let stop = Arc::new(AtomicBool::new(false));
        let success_count = Arc::new(AtomicU64::new(0));
        let writer_port = port;
        let stop_clone = stop.clone();
        let count_clone = success_count.clone();
        let writer = std::thread::spawn(move || {
            let mut c = Conn::open(writer_port);
            while !stop_clone.load(Ordering::Relaxed) {
                if matches!(c.cmd_s(&["INCR", "ssm4a:ctr"]), Resp::Int(_)) {
                    count_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        // Let the writer accumulate some increments before the rewrite.
        std::thread::sleep(Duration::from_millis(200));

        // Issue BGREWRITEAOF.
        let mut admin = Conn::open(port);
        let bgrw = admin.cmd_s(&["BGREWRITEAOF"]);
        // Accept "Background append only file rewriting started" (Simple) or
        // "ERR Background AOF rewrite already in progress" (idempotent).
        assert!(
            matches!(&bgrw, Resp::Simple(_) | Resp::Error(_)),
            "BGREWRITEAOF unexpected reply: {bgrw:?}"
        );

        // Poll for rewrite completion up to 30s.
        //
        // For the per-shard experimental path we wait for the manifest seq to
        // advance: the coordinator atomically bumps seq only after ALL shards
        // complete their folds.  Waiting for any single shard's base file
        // (compacted_base_exists) is too early — other shards may still be
        // folding, so EverySec fsyncs on the new incr have not yet run, and
        // killing the server in that window loses data.
        //
        // For the legacy single-writer path compacted_base_exists remains
        // correct (there is no coordinator; one seq flip = one base file).
        let uses_per_shard_rewrite = flags.contains(&"--experimental-per-shard-rewrite");
        let rewrite_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            std::thread::sleep(Duration::from_millis(500));
            let done = if uses_per_shard_rewrite {
                aof_manifest_seq(dir.path()) > 1
            } else {
                compacted_base_exists(dir.path())
            };
            if done {
                break;
            }
            if Instant::now() > rewrite_deadline {
                panic!(
                    "BGREWRITEAOF did not complete within 30s — \
                     no layout-appropriate compaction artifact appeared in {:?}.",
                    dir.path()
                );
            }
        }

        // Stop the writer and record the exact successful-INCR count.
        stop.store(true, Ordering::Relaxed);
        writer.join().expect("writer thread join");
        let n = success_count.load(Ordering::Relaxed) as i64;
        assert!(n > 0, "writer must have succeeded at least once");

        // Let the everysec flush window close so every acked INCR is durable.
        std::thread::sleep(Duration::from_millis(1500));

        // Read the counter — MUST equal the exact successful-INCR count.
        // This is the exactly-once assertion: neither dropped nor double-applied.
        let mut check = Conn::open(port);
        let got = match check.cmd_s(&["GET", "ssm4a:ctr"]) {
            Resp::Bulk(Some(b)) => String::from_utf8(b)
                .expect("utf8")
                .parse::<i64>()
                .expect("parse i64"),
            other => panic!("GET ssm4a:ctr reply {other:?}"),
        };
        assert_eq!(
            got,
            n,
            "exactly-once invariant: ctr ({got}) must equal recorded INCR count ({n}). \
             diff={}: positive=double-apply, negative=lost writes.",
            got - n
        );

        recorded_count = n;

        // _guard dropped here → server killed (SIGKILL equivalent)
    }

    // --- Round 2: restart on the SAME dir, assert durability ---
    {
        let port2 = free_port();
        let _guard2 = ServerGuard(spawn_moon(port2, dir.path(), shards, &flags));
        drop(wait_ready(port2));

        let mut c = Conn::open(port2);

        // Counter must survive the restart with the same value.
        let ctr_after = match c.cmd_s(&["GET", "ssm4a:ctr"]) {
            Resp::Bulk(Some(b)) => String::from_utf8(b)
                .expect("utf8")
                .parse::<i64>()
                .unwrap_or(-1),
            other => panic!("after restart GET ssm4a:ctr reply {other:?}"),
        };
        assert_eq!(
            ctr_after, recorded_count,
            "after restart, ssm4a:ctr must equal recorded count ({recorded_count}); \
             got {ctr_after}"
        );

        // A sample of the 2000 keys must exist.
        for i in [0_u32, 499, 999, 1499, 1999] {
            let key = format!("ssm4a:k{i}");
            let expected = format!("v{i}");
            let r = c.cmd_s(&["GET", &key]);
            assert_eq!(
                r,
                Resp::Bulk(Some(expected.into_bytes())),
                "after restart, key {key} must exist"
            );
        }
    }
}

#[test]
fn test_ssm4a_aof_fold_cooperative() {
    // shards=1: BGREWRITEAOF runs without the experimental flag.
    aof_fold_exactly_once(1, &[]);
}

#[test]
fn test_ssm4a_fold_4shard_experimental() {
    // do_rewrite_per_shard (F6) — the exact foreign-lock fold contract C4
    // redesigns into the cooperative snapshot. GREEN today; must stay green
    // through the cutover.
    aof_fold_exactly_once(4, &["--experimental-per-shard-rewrite"]);
}

// ---------------------------------------------------------------------------
// test_reject_uninitialized_shard_no_wire_panic
//
// GREEN today and must stay green forever (permanent wire guard).
//
// Sending junk-but-parseable commands + normal SET/GET must not cause a
// with_shard panic that is reachable over the network. The server must stay
// alive and answer PINGs on subsequent connections.
//
// The real uninitialized-abort assertion (C1: startup abort naming the
// shard id, before first accept) is unit-level in slice.rs's tests and in
// shardslice_shape.rs. This wire guard pins "no with_shard panic reachable
// over the wire" and is intended to be a permanent regression guard.
// ---------------------------------------------------------------------------

#[test]
fn test_reject_uninitialized_shard_no_wire_panic() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), 4, &["--appendonly", "no"]));
    drop(wait_ready(port));

    // --- Send a burst of junk-but-parseable commands ---
    let mut c = Conn::open(port);

    // *0\r\n is a valid RESP2 frame (zero-element array) that the server
    // parses and rejects as "empty command" — exercises parse→dispatch without
    // routing to any specific shard key.
    c.s.write_all(b"*0\r\n").expect("write *0");
    let _ = c.frame(); // consume the error reply

    // A negative-arity array (*-1) is also valid RESP2 (null array). The
    // server parses it and replies "ERR invalid command format" — consume
    // that reply to keep the connection in sync.
    c.s.write_all(b"*-1\r\n").expect("write *-1 (null array)");
    let _ = c.frame(); // consume the error reply

    // Normal SET/GET from the SAME connection must still work after the
    // junk burst.
    assert_eq!(
        c.cmd_s(&["SET", "ssm_guard:k", "alive"]),
        Resp::Simple("OK".into()),
        "SET after junk burst must succeed — server must not have crashed"
    );
    assert_eq!(
        c.cmd_s(&["GET", "ssm_guard:k"]),
        Resp::Bulk(Some(b"alive".to_vec())),
        "GET after junk burst must return the set value"
    );

    // Open a SECOND fresh connection and confirm the server is still alive.
    let mut c2 = Conn::open(port);
    assert_eq!(
        c2.cmd_s(&["PING"]),
        Resp::Simple("PONG".into()),
        "server must still answer PING from a fresh connection after the junk burst"
    );
}
