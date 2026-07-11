//! P0 (task #41): with disk-offload enabled (the DEFAULT), the production
//! eviction paths (`evict_one_async_spill` / `evict_batch_durable_no_aof`,
//! `src/storage/eviction.rs`) spill Hash/List/Set/ZSet values to the cold
//! tier just like strings — but the type-specific command accessors never
//! consulted the cold index:
//!
//!   * HGETALL/LRANGE/SMEMBERS/ZRANGE/EXISTS reported the key ABSENT.
//!   * HSET/LPUSH/SADD/ZADD silently fabricated a brand-new EMPTY container,
//!     which then got written back over the real cold copy on the next
//!     flush — permanently destroying it.
//!
//! This is a wire-level black-box test on purpose: the bug lives in the
//! command-dispatch accessors (`Database::get_or_create_*`,
//! `get_*_ref_if_alive`), which unit tests could route around by calling the
//! storage layer directly. Driving it through the real RESP protocol proves
//! the fix is wired into the actual HSET/HGETALL/EXISTS command handlers,
//! not just the storage primitives (see `tests/oom_bypass_closure.rs` for the
//! same "wire-level on purpose" rationale against a sibling dispatch-wiring
//! bug).
//!
//! Design notes:
//!   * `--disk-offload enable` alone does NOT spill collections — spill is
//!     INERT without a durability backstop (`ServerConfig::
//!     disk_offload_spill_inert`): the async/durable-batch eviction paths
//!     need a `ShardManifest`, which is only wired up when `--appendonly yes`
//!     or `--save` is set. Without one, an evicting policy just DROPS
//!     victims (no tiering) — which would not exercise this bug at all. So
//!     this suite runs with `--appendonly yes`.
//!   * The probe hash is written FIRST, then a wave of filler keys pushes
//!     memory well past `--maxmemory` under `allkeys-lru`, so the probe
//!     (oldest access time) is evicted with very high confidence.
//!   * Eviction/spill is tick-driven (`shard::persistence_tick`), not
//!     synchronous with the write that crosses the threshold — a short
//!     settle sleep follows the filler wave before probing.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   MOON_BIN=$PWD/target/release/moon cargo test --release --test cold_collection_visibility
//!
//! tokio runtime:
//!   cargo build --release --no-default-features --features runtime-tokio,jemalloc
//!   MOON_BIN=$PWD/target/release/moon cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc --test cold_collection_visibility

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/oom_bypass_closure.rs)
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

/// Root the test's scratch dir under the repo's own volume, not `$TMPDIR`
/// (which can trip Moon's 5%-free diskfull write-pause guard — see
/// gotcha_vm_diskfull_shared_volume in project memory). Also pass
/// `--disk-free-min-pct 0` explicitly for the same reason.
fn test_tmpdir() -> tempfile::TempDir {
    let base =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/cold-vis-test-tmp");
    std::fs::create_dir_all(&base).expect("create cold-vis-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("cold-vis-")
        .tempdir_in(&base)
        .expect("tempdir_in target/cold-vis-test-tmp")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const MAXMEMORY_BYTES: u64 = 512 * 1024; // 512 KiB — tiny, forces eviction fast.

fn spawn_moon_cold_offload(dir: &std::path::Path) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                "1",
                // Spill is inert without a durability backstop — see module
                // doc. AOF also gives us a realistic production config.
                "--appendonly",
                "yes",
                "--disk-offload",
                "enable",
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                // Approximate (sampled) LRU, matching Redis semantics — the
                // default sample size (5) makes it a coin flip per eviction
                // round whether the single oldest probe key ever gets
                // sampled out of hundreds of filler keys. A large sample
                // size makes eviction of the globally-oldest key
                // deterministic-in-practice within the filler wave below.
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
// Minimal RESP client (binary-safe args, full-frame parser)
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

fn log_tail(dir: &std::path::Path, name: &str) -> String {
    match std::fs::read_to_string(dir.join(name)) {
        Ok(s) => {
            let tail: Vec<&str> = s.lines().rev().take(20).collect();
            tail.into_iter().rev().collect::<Vec<_>>().join("\n")
        }
        Err(e) => format!("<unreadable: {e}>"),
    }
}

fn wait_ready(guard: &mut ServerGuard, dir: &std::path::Path, port: u16) -> Client {
    let deadline = readiness_deadline();
    let start = Instant::now();
    loop {
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!(
                "moon exited ({status}) before accepting on port {port}\n\
                 --- moon.stderr.log (tail) ---\n{}\n\
                 --- moon.stdout.log (tail) ---\n{}",
                log_tail(dir, "moon.stderr.log"),
                log_tail(dir, "moon.stdout.log"),
            );
        }
        if let Some(mut c) = Client::try_connect(port, Duration::from_secs(1))
            && let Ok(true) = c.try_ping()
        {
            return c;
        }
        assert!(
            start.elapsed() < deadline,
            "server never answered PING on port {port} within {deadline:?}\n\
             --- moon.stderr.log (tail) ---\n{}",
            log_tail(dir, "moon.stderr.log"),
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn blob(size: usize, fill: u8) -> Vec<u8> {
    vec![fill; size]
}

/// Drive memory well past `MAXMEMORY_BYTES` with unique filler keys so
/// `allkeys-lru` evicts (and, with the durability backstop configured,
/// spills-to-cold) everything written before this point — namely the probe
/// keys the caller set up first. Settles briefly afterward: eviction/spill
/// is tick-driven (`shard::persistence_tick`), not synchronous with the
/// write that crosses the threshold.
fn drive_eviction(c: &mut Client) {
    const FILLER_COUNT: usize = 400;
    const FILLER_VALUE_LEN: usize = 4 * 1024; // 4 KiB — 400 * 4KiB ~= 1.6MB >> 512KB cap.
    let value = blob(FILLER_VALUE_LEN, b'f');
    for i in 0..FILLER_COUNT {
        let key = format!("filler:{i}");
        let _ = c.cmd(&[b"SET", key.as_bytes(), &value]);
    }
    std::thread::sleep(Duration::from_secs(3));
}

fn arr_as_bulk_set(v: &V) -> std::collections::HashSet<Vec<u8>> {
    match v {
        V::Arr(items) => items
            .iter()
            .map(|i| match i {
                V::Bulk(b) => b.clone(),
                other => panic!("expected Bulk in array, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Arr, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Hash: HSET (listpack fast path) -> evict/spill -> HGETALL/EXISTS/HSET-merge
// ---------------------------------------------------------------------------

#[test]
fn test_cold_hash_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    // Small field/value sizes deliberately keep this on the listpack fast
    // path (`get_or_create_hash_listpack`), the most common real-world HSET
    // shape — proving the fix covers it, not just the full-HashMap path.
    assert_eq!(
        c.cmd(&[b"HSET", b"probehash", b"f1", b"v1", b"f2", b"v2"]),
        V::Int(2)
    );
    assert_eq!(c.cmd(&[b"EXISTS", b"probehash"]), V::Int(1));

    drive_eviction(&mut c);

    // RED (pre-fix): EXISTS reports 0 (cold-only key invisible to EXISTS).
    assert_eq!(
        c.cmd(&[b"EXISTS", b"probehash"]),
        V::Int(1),
        "P0: a cold-spilled hash must still count as existing"
    );

    // RED (pre-fix): HGETALL returns an empty array (cold-only key reads as
    // absent) — indistinguishable from "never existed" without this check.
    let all = c.cmd(&[b"HGETALL", b"probehash"]);
    match &all {
        V::Arr(items) => assert_eq!(
            items.len(),
            4,
            "P0: HGETALL must return the spilled fields, not report the key absent: {all:?}"
        ),
        other => panic!("expected Arr from HGETALL, got {other:?}"),
    }
    assert_eq!(
        c.cmd(&[b"HGET", b"probehash", b"f1"]),
        V::Bulk(b"v1".to_vec())
    );
    assert_eq!(
        c.cmd(&[b"HGET", b"probehash", b"f2"]),
        V::Bulk(b"v2".to_vec())
    );

    // RED (pre-fix): HSET on a cold-only key silently fabricates a new EMPTY
    // hash and adds only f3 to it — permanently destroying f1/f2 on the next
    // flush. GREEN: f3 merges alongside the promoted f1/f2.
    assert_eq!(
        c.cmd(&[b"HSET", b"probehash", b"f3", b"v3"]),
        V::Int(1),
        "f3 must be counted as a newly-added field"
    );
    let all_after = c.cmd(&[b"HGETALL", b"probehash"]);
    match &all_after {
        V::Arr(items) => assert_eq!(
            items.len(),
            6,
            "P0: HSET must MERGE with the promoted cold fields, not fabricate an \
             empty hash and lose f1/f2: {all_after:?}"
        ),
        other => panic!("expected Arr from HGETALL, got {other:?}"),
    }
    assert_eq!(
        c.cmd(&[b"HGET", b"probehash", b"f1"]),
        V::Bulk(b"v1".to_vec())
    );
    assert_eq!(
        c.cmd(&[b"HGET", b"probehash", b"f2"]),
        V::Bulk(b"v2".to_vec())
    );
    assert_eq!(
        c.cmd(&[b"HGET", b"probehash", b"f3"]),
        V::Bulk(b"v3".to_vec())
    );
}

// ---------------------------------------------------------------------------
// List: LPUSH (listpack fast path) -> evict/spill -> LRANGE/EXISTS/LPUSH-merge
// ---------------------------------------------------------------------------

#[test]
fn test_cold_list_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    assert_eq!(c.cmd(&[b"RPUSH", b"probelist", b"a", b"b"]), V::Int(2));
    drive_eviction(&mut c);

    assert_eq!(
        c.cmd(&[b"EXISTS", b"probelist"]),
        V::Int(1),
        "P0: a cold-spilled list must still count as existing"
    );

    let range = c.cmd(&[b"LRANGE", b"probelist", b"0", b"-1"]);
    match &range {
        V::Arr(items) => assert_eq!(
            items,
            &[V::Bulk(b"a".to_vec()), V::Bulk(b"b".to_vec())],
            "P0: LRANGE must return the spilled elements, not report the key absent"
        ),
        other => panic!("expected Arr from LRANGE, got {other:?}"),
    }

    // Merge-not-fabricate on write.
    assert_eq!(c.cmd(&[b"RPUSH", b"probelist", b"c"]), V::Int(3));
    let range_after = c.cmd(&[b"LRANGE", b"probelist", b"0", b"-1"]);
    match &range_after {
        V::Arr(items) => assert_eq!(
            items,
            &[
                V::Bulk(b"a".to_vec()),
                V::Bulk(b"b".to_vec()),
                V::Bulk(b"c".to_vec())
            ],
            "P0: RPUSH must MERGE with the promoted cold elements, not fabricate an \
             empty list and lose a/b: {range_after:?}"
        ),
        other => panic!("expected Arr from LRANGE, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Set: SADD -> evict/spill -> SMEMBERS/EXISTS/SADD-merge
// ---------------------------------------------------------------------------

#[test]
fn test_cold_set_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    // Non-integer members keep this off the intset fast path, exercising
    // `get_or_create_set` (the general HashSet path).
    assert_eq!(c.cmd(&[b"SADD", b"probeset", b"alpha", b"beta"]), V::Int(2));
    drive_eviction(&mut c);

    assert_eq!(
        c.cmd(&[b"EXISTS", b"probeset"]),
        V::Int(1),
        "P0: a cold-spilled set must still count as existing"
    );

    let members = c.cmd(&[b"SMEMBERS", b"probeset"]);
    assert_eq!(
        arr_as_bulk_set(&members),
        std::collections::HashSet::from([b"alpha".to_vec(), b"beta".to_vec()]),
        "P0: SMEMBERS must return the spilled members, not report the key absent: {members:?}"
    );

    assert_eq!(c.cmd(&[b"SADD", b"probeset", b"gamma"]), V::Int(1));
    let members_after = c.cmd(&[b"SMEMBERS", b"probeset"]);
    assert_eq!(
        arr_as_bulk_set(&members_after),
        std::collections::HashSet::from([b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]),
        "P0: SADD must MERGE with the promoted cold members, not fabricate an empty \
         set and lose alpha/beta: {members_after:?}"
    );
}

// ---------------------------------------------------------------------------
// ZSet: ZADD -> evict/spill -> ZRANGE/EXISTS/ZADD-merge
// ---------------------------------------------------------------------------

#[test]
fn test_cold_zset_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    assert_eq!(
        c.cmd(&[b"ZADD", b"probezset", b"1", b"one", b"2", b"two"]),
        V::Int(2)
    );
    drive_eviction(&mut c);

    assert_eq!(
        c.cmd(&[b"EXISTS", b"probezset"]),
        V::Int(1),
        "P0: a cold-spilled zset must still count as existing"
    );

    let range = c.cmd(&[b"ZRANGE", b"probezset", b"0", b"-1"]);
    match &range {
        V::Arr(items) => assert_eq!(
            items,
            &[V::Bulk(b"one".to_vec()), V::Bulk(b"two".to_vec())],
            "P0: ZRANGE must return the spilled members, not report the key absent"
        ),
        other => panic!("expected Arr from ZRANGE, got {other:?}"),
    }

    assert_eq!(c.cmd(&[b"ZADD", b"probezset", b"3", b"three"]), V::Int(1));
    let range_after = c.cmd(&[b"ZRANGE", b"probezset", b"0", b"-1"]);
    match &range_after {
        V::Arr(items) => assert_eq!(
            items,
            &[
                V::Bulk(b"one".to_vec()),
                V::Bulk(b"two".to_vec()),
                V::Bulk(b"three".to_vec())
            ],
            "P0: ZADD must MERGE with the promoted cold members, not fabricate an \
             empty zset and lose one/two: {range_after:?}"
        ),
        other => panic!("expected Arr from ZRANGE, got {other:?}"),
    }
}
