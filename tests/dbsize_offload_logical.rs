//! Issue #355: DBSIZE (and INFO `# Keyspace`) must count LOGICAL keys under
//! disk-offload — a spilled-but-readable key is still a key. Pre-fix, both
//! reported the resident set only: the 2026-07-16 G2 re-run wrote ~164K
//! distinct keys and DBSIZE answered 24,275 (~86% under-report), breaking
//! operator capacity math and any tooling that trusts DBSIZE.
//!
//! Wire-level on purpose (pattern: tests/cold_collection_visibility.rs): the
//! fix spans four dispatch sites (`key::dbsize`, `key::dbsize_readonly`, the
//! INFO fallback keyspace section, and the `KeyspaceStats` /
//! `coordinate_keyspace_info` scatter-gather) — unit tests can't prove the
//! wiring.
//!
//! Assertion strategy: eviction may legitimately DROP victims (plain
//! allkeys-lru semantics) instead of spilling them depending on which
//! eviction path claims a key, so asserting `DBSIZE == N_written` would be
//! flaky by design. Ground truth is an `EXISTS` sweep instead — EXISTS is
//! cold-aware (task #41) and does not promote, so at write-quiesced steady
//! state `DBSIZE == Σ EXISTS` exactly, while a floor assertion
//! (`> N/2 » resident capacity`) proves the run actually spilled enough for
//! the pre-fix resident-only count to fail loudly.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   MOON_BIN=$PWD/target/release/moon cargo test --release --test dbsize_offload_logical
//!
//! tokio runtime:
//!   cargo build --release --no-default-features --features runtime-tokio,jemalloc
//!   MOON_BIN=$PWD/target/release/moon cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc --test dbsize_offload_logical

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/cold_collection_visibility.rs)
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

/// Root scratch dirs under the repo volume, not `$TMPDIR` (macOS root volume
/// can trip the 5%-free diskfull guard); `--disk-free-min-pct 0` is also
/// passed explicitly for the near-full-dev-volume case (see
/// gotcha_vm_diskfull_shared_volume in project memory).
fn test_tmpdir() -> tempfile::TempDir {
    let base =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/dbsize-355-test-tmp");
    std::fs::create_dir_all(&base).expect("create dbsize-355-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("dbsize-355-")
        .tempdir_in(&base)
        .expect("tempdir_in target/dbsize-355-test-tmp")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const MAXMEMORY_BYTES: u64 = 512 * 1024; // 512 KiB — forces spill fast.

fn spawn_moon_offload(dir: &std::path::Path, shards: u32) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                // Spill is INERT without a durability backstop (see
                // tests/cold_collection_visibility.rs module doc) — the
                // async-spill eviction path bails unless a ShardManifest
                // exists, which needs --appendonly yes (or --save).
                "--appendonly",
                "yes",
                "--disk-offload",
                "enable",
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (pattern: tests/cold_collection_visibility.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Clone)]
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
    fn try_connect(port: u16, window: Duration) -> Option<Self> {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < window => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => return None,
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Some(Client {
            reader: BufReader::new(stream),
            writer,
        })
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

    fn try_ping(&mut self) -> std::io::Result<bool> {
        self.writer.write_all(b"*1\r\n$4\r\nPING\r\n")?;
        let mut buf = [0u8; 7];
        self.reader.read_exact(&mut buf)?;
        Ok(&buf == b"+PONG\r\n")
    }
}

fn readiness_deadline() -> Duration {
    if std::env::var_os("CI").is_some() {
        Duration::from_secs(120)
    } else {
        Duration::from_secs(30)
    }
}

fn wait_ready(guard: &mut ServerGuard, dir: &std::path::Path, port: u16) -> Client {
    let deadline = Instant::now() + readiness_deadline();
    loop {
        if let Ok(Some(status)) = guard.0.try_wait() {
            let tail = std::fs::read_to_string(dir.join("moon.stderr.log"))
                .unwrap_or_else(|e| format!("<unreadable: {e}>"));
            panic!("moon exited {status} before ready; stderr tail:\n{tail}");
        }
        if let Some(mut c) = Client::try_connect(port, Duration::from_secs(2))
            && c.try_ping().unwrap_or(false)
        {
            return c;
        }
        assert!(
            Instant::now() < deadline,
            "moon never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ---------------------------------------------------------------------------
// Shared drive: load → quiesce → ground truth
// ---------------------------------------------------------------------------

const N_KEYS: usize = 400;
const VAL_SIZE: usize = 4096; // 400 × 4KiB ≈ 1.6 MiB » 512 KiB cap → heavy spill.

fn key_name(i: usize) -> Vec<u8> {
    format!("k355:{i:06}").into_bytes()
}

fn load_keys(c: &mut Client) {
    let val = vec![b'v'; VAL_SIZE];
    for i in 0..N_KEYS {
        let k = key_name(i);
        match c.cmd(&[b"SET", &k, &val]) {
            V::Simple(ref s) if s == "OK" => {}
            other => panic!("SET {i} failed: {other:?}"),
        }
    }
}

/// Non-promoting logical ground truth: Σ EXISTS over every written key.
/// EXISTS is cold-aware (task #41) and leaves plane membership untouched.
fn exists_sum(c: &mut Client) -> i64 {
    let mut sum = 0;
    for i in 0..N_KEYS {
        let k = key_name(i);
        match c.cmd(&[b"EXISTS", &k]) {
            V::Int(n) => sum += n,
            other => panic!("EXISTS {i} unexpected reply: {other:?}"),
        }
    }
    sum
}

fn dbsize(c: &mut Client) -> i64 {
    match c.cmd(&[b"DBSIZE"]) {
        V::Int(n) => n,
        other => panic!("DBSIZE unexpected reply: {other:?}"),
    }
}

/// Poll until the EXISTS ground truth is STABLE (two consecutive sweeps
/// agree) and DBSIZE equals it. Keys in the spill in-flight window live in
/// NEITHER plane (evict removes hot at queue time; the cold entry only lands
/// when the spill thread's batch flushes — on cap or timeout — and the event
/// loop applies the completion), so right after a write wave BOTH counters
/// legitimately under-read: DBSIZE by the still-buffered tail batches
/// (observed: 68/400 across 4 quiesced shards) and EXISTS itself for a key
/// probed mid-window (observed: 123/400 when sweeping immediately after the
/// load; the same transient is documented in
/// tests/cold_collection_visibility.rs). Joint convergence is the contract:
/// a write-quiesced instance must reach a stable logical count within the
/// flush timeout + a tick. Pre-fix (resident-only) DBSIZE never meets the
/// stabilized truth and this times out loudly.
fn converged_logical_count(c: &mut Client, phase: &str) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut prev_truth = -1;
    loop {
        let truth = exists_sum(c);
        let size = dbsize(c);
        if size == truth && truth == prev_truth {
            return size;
        }
        assert!(
            Instant::now() < deadline,
            "[{phase}] DBSIZE never converged to a stable EXISTS ground \
             truth (DBSIZE {size}, EXISTS {truth}, previous EXISTS {prev_truth})"
        );
        prev_truth = truth;
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// `db0:keys=` from INFO — must agree with DBSIZE (same logical count).
fn info_keyspace_db0_keys(c: &mut Client) -> i64 {
    let text = match c.cmd(&[b"INFO"]) {
        V::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("INFO unexpected reply: {other:?}"),
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("db0:keys=") {
            let keys = rest.split(',').next().unwrap_or("");
            return keys.parse().expect("db0:keys= integer");
        }
    }
    0
}

fn assert_logical_counts(c: &mut Client, phase: &str) -> i64 {
    let size = converged_logical_count(c, phase);
    // Floor: with a 512 KiB cap and 4 KiB values the resident set is ~128
    // keys at most — the pre-fix resident-only DBSIZE cannot reach N/2.
    // (Eviction may legally plain-drop SOME victims instead of spilling —
    // see the module doc — but a majority-drop run would make the fix
    // assertion vacuous, so fail it as an environment problem.)
    assert!(
        size > (N_KEYS as i64) / 2,
        "[{phase}] fewer than half the keys survive ({size}/{N_KEYS}) — the \
         run did not exercise spill-heavy state, assertions are vacuous"
    );
    let info_keys = info_keyspace_db0_keys(c);
    assert_eq!(
        info_keys, size,
        "[{phase}] INFO # Keyspace db0:keys= must agree with DBSIZE"
    );
    size
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Single shard: live spill, then a kill-9 restart (cold_index rebuilt from
/// the manifest; hot plane from AOF replay + shadow demotion) — DBSIZE must
/// report the logical count in both lives.
#[test]
fn dbsize_counts_spilled_keys_and_survives_restart() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_offload(dir.path(), 1);
    let mut c = wait_ready(&mut guard, dir.path(), port);

    load_keys(&mut c);
    let live = assert_logical_counts(&mut c, "live");

    // kill -9 + same-dir respawn (see reserve_unique_port/await_server_ready
    // pattern rationale in project memory: restart legs reuse the same port
    // only via a fresh spawn_listening — here the port is already reserved
    // by the first spawn, so respawn directly on it).
    common::sigkill(&mut guard.0);
    drop(guard);
    common::wait_for_port_down(port);

    let mut child = Command::new(find_moon_binary());
    child
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.path().to_string_lossy(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--disk-offload",
            "enable",
            "--maxmemory",
            &MAXMEMORY_BYTES.to_string(),
            "--maxmemory-policy",
            "allkeys-lru",
            "--maxmemory-samples",
            "200",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::fs::File::create(dir.path().join("moon2.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.path().join("moon2.stderr.log")).expect("stderr log"));
    let mut guard = ServerGuard(child.spawn().expect("respawn moon"));
    let mut c = wait_ready(&mut guard, dir.path(), port);

    let recovered = assert_logical_counts(&mut c, "post-restart");
    assert_eq!(
        recovered, live,
        "restart must not change the logical key count"
    );
}

/// Four shards: DBSIZE goes through `coordinate_dbsize` (per-shard Execute
/// scatter + sum) and INFO through `KeyspaceStats` — both must aggregate
/// logical counts.
#[test]
fn dbsize_counts_spilled_keys_multishard() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_offload(dir.path(), 4);
    let mut c = wait_ready(&mut guard, dir.path(), port);

    load_keys(&mut c);
    assert_logical_counts(&mut c, "multishard");
}
