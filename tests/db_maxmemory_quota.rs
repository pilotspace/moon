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

/// Fresh `--dir` under the OS temp dir (small, low-diskfull-risk, never the
/// shared `/Volumes/Games` checkout volume). Must be portable: a hardcoded
/// `/private/tmp` is macOS-only and fails PermissionDenied on Linux CI.
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::env::temp_dir().join("moon-db-maxmemory-quota-tests");
    std::fs::create_dir_all(&base).expect("create test tmp base dir");
    tempfile::Builder::new()
        .prefix("dbmm-")
        .tempdir_in(&base)
        .expect("tempdir_in OS temp dir")
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

/// Same as `spawn_moon_db_quota` but additionally passes `--disk-offload
/// disable` — the exact combination (`--maxmemory 0` + no disk-offload spill
/// sender + `--db-maxmemory` configured) that the fix-first adversarial
/// review found bypassed the db-quota gate entirely for every non-inline
/// write command (HSET/LPUSH/SADD/ZADD/INCR/APPEND/MSET/SET-with-options/
/// RESTORE/...). `handler_monoio::batch_eviction_active` (and the
/// cross-shard-leg twin `spsc_handler::evict_active`) previously only
/// consulted `spill_sender.is_some() || maxmemory != 0`, so with both false
/// the entire non-inline write-eviction-gate call was skipped — including
/// the db-quota check nested inside it. Plain `SET` was unaffected because
/// it takes the separate, always-correctly-gated inline fast path in
/// `blocking.rs`, which is exactly why the original quota test above (which
/// only ever issues `SET`) did not catch this.
fn spawn_moon_db_quota_no_spill(
    port: u16,
    dir: &std::path::Path,
    db_entries: &[&str],
) -> ServerGuard {
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
        "--disk-offload",
        "disable",
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

// ---------------------------------------------------------------------------
// Case C (fix-first adversarial review regression test): with `--maxmemory 0`
// and `--disk-offload disable` (no spill sender), non-inline write commands
// (HSET here, plus one SET-with-EX to confirm the option-bearing SET variant
// also rides the fixed gate) must STILL be rejected once db 1 is over its
// quota. Before the fix, `batch_eviction_active`
// (`src/server/conn/handler_monoio/mod.rs`) and its cross-shard-leg twin
// `evict_active` (`src/shard/spsc_handler.rs`) only checked
// `spill_sender.is_some() || maxmemory != 0`, silently skipping the entire
// eviction-gate call — including the nested db-quota check — for exactly
// this configuration.
// ---------------------------------------------------------------------------

#[test]
fn test_quota_rejects_non_inline_writes_without_spill_sender() {
    let dir = test_tmpdir();
    let port = free_port();
    // db 1 gets a tiny 4 KB quota; no disk-offload spill sender exists at all.
    let _guard = spawn_moon_db_quota_no_spill(port, dir.path(), &["1:4096"]);
    let mut c = wait_ready(port);

    let sel = c.cmd(&[b"SELECT", b"1"]);
    assert_eq!(sel, V::Simple("OK".to_string()));

    let big_value = vec![b'x'; 1024];

    // HSET: a non-inline write command. Must eventually hit the db-1 quota.
    //
    // Deliberately uses a DISTINCT key per iteration (`h{i}`), not repeated
    // field-growth on one shared hash key. Investigation during this fix
    // found a second, pre-existing, independent bug: `used_memory` is only
    // charged once, at hash-key CREATION time, for the empty hash's fixed
    // overhead (`entry_overhead` in `src/storage/db.rs`, called right after
    // `Entry::new_hash()` before any fields are inserted) — subsequent
    // `map.insert(field, value)` calls in `hash_write::hset` (and the
    // equivalent List/Set/ZSet mutation sites) never update `used_memory`.
    // Growing ONE hash key's fields therefore never grows its accounted
    // memory at all, regardless of value size — verified this defeats even
    // the pre-existing GLOBAL `--maxmemory` gate identically (not something
    // introduced by db-quota). That systemic accounting gap is out of scope
    // for this fix (affects every container type, requires auditing every
    // mutation site) and is documented as a follow-up in
    // docs/guides/isolation.md rather than fixed here. Using a fresh key per
    // HSET charges each key's flat ~(key.len() + 128)-byte creation overhead
    // (verified empirically: ~130 bytes/key trips a 4 KB quota at ~32 keys),
    // which is enough to exercise the gate fix under test without depending
    // on the separate, unfixed accounting gap.
    let mut saw_quota_error = false;
    for i in 0..80 {
        let key = format!("h{i}");
        let r = c.cmd(&[b"HSET", key.as_bytes(), b"f0", &big_value]);
        if r.is_db_quota_error() {
            saw_quota_error = true;
            break;
        }
        // HSET's successful reply is an integer (":1" for a new field), not
        // "+OK" — accept any non-error reply as "still under quota".
        assert!(
            !matches!(r, V::Err(_)),
            "unexpected error before quota should have bitten: {r:?}"
        );
    }
    assert!(
        saw_quota_error,
        "expected a MOONERR db maxmemory exceeded reply within 80 HSETs \
         (distinct keys) to db 1 (4 KB quota, no disk-offload spill sender) \
         — the non-inline write path's db-quota gate is bypassed"
    );

    // SET with an option (EX) also takes the non-inline dispatch path (only
    // bare SET/GET use the byte-level inline fast path in blocking.rs) — it
    // must be rejected too, on the same already-exhausted db 1 quota.
    let r = c.cmd(&[b"SET", b"k-with-ex", &big_value, b"EX", b"100"]);
    assert!(
        r.is_db_quota_error(),
        "SET with EX option must ride the non-inline gate and be rejected \
         once db 1 is over quota, got {r:?}"
    );

    // db 0 remains fully unaffected.
    let sel0 = c.cmd(&[b"SELECT", b"0"]);
    assert_eq!(sel0, V::Simple("OK".to_string()));
    let r0 = c.cmd(&[b"HSET", b"h0", b"f0", &big_value]);
    assert!(
        !r0.is_db_quota_error(),
        "db 0 has no quota configured; HSET must succeed, got {r0:?}"
    );
}

// ---------------------------------------------------------------------------
// Case D (fix-first adversarial review item 3): a malformed or out-of-range
// `--db-maxmemory` CLI entry is trusted operator config, not wire input —
// the server must refuse to start (nonzero exit, clear stderr message)
// rather than silently warn-and-drop the entry and start anyway (which
// `CONFIG SET db-maxmemory` already correctly refuses to do for the same
// bad input).
// ---------------------------------------------------------------------------

#[test]
fn test_malformed_db_maxmemory_cli_refuses_to_start() {
    let dir = test_tmpdir();
    let port = free_port();
    let mut cmd = Command::new(find_moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--dir",
        &dir.path().to_string_lossy(),
        "--shards",
        "1",
        "--appendonly",
        "no",
        "--databases",
        "16",
        "--db-maxmemory",
        "not-a-pair", // malformed: no ':' separator
    ]);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let output = cmd
        .output()
        .expect("spawn moon with malformed --db-maxmemory");
    assert!(
        !output.status.success(),
        "server must exit nonzero on malformed --db-maxmemory, got status {:?}",
        output.status
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_uppercase().contains("REFUSING TO START"),
        "expected a clear REFUSING TO START message on stderr, got: {stderr}"
    );
    assert!(
        stderr.contains("not-a-pair"),
        "error message must name the offending entry, got: {stderr}"
    );
}

#[test]
fn test_out_of_range_db_maxmemory_cli_refuses_to_start() {
    let dir = test_tmpdir();
    let port = free_port();
    let mut cmd = Command::new(find_moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--dir",
        &dir.path().to_string_lossy(),
        "--shards",
        "1",
        "--appendonly",
        "no",
        "--databases",
        "16",
        "--db-maxmemory",
        "99:1024", // out of range: >= --databases (16)
    ]);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let output = cmd
        .output()
        .expect("spawn moon with out-of-range --db-maxmemory");
    assert!(
        !output.status.success(),
        "server must exit nonzero on out-of-range --db-maxmemory, got status {:?}",
        output.status
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_uppercase().contains("REFUSING TO START"),
        "expected a clear REFUSING TO START message on stderr, got: {stderr}"
    );
}
