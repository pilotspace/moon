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
//!   * **Two independent eviction paths race for the same victim, and only
//!     one of them spills.** The interactive write-path gate (synchronous,
//!     runs inline on a `SET`/etc that crosses `maxmemory`) spills every
//!     type via `evict_one_async_spill`. The periodic background tick
//!     (`shard::persistence_tick` / `src/shard/timers.rs`, no
//!     `SpillContext`) plain-DROPS its victim instead — which is legal
//!     `allkeys-lru` semantics (Wave A records a real reason-DEL for it), not
//!     a bug. Which path claims a given sampled-oldest key is a genuine race
//!     that differs by OS scheduler (observed: async-spill path usually wins
//!     on macOS, the 100ms tick usually wins on Linux). A single probe key
//!     therefore makes this suite nondeterministic about WHICH bug-relevant
//!     path it exercises — a plain-dropped probe reads back as
//!     `EXISTS == 0`, which is *correct* behavior, indistinguishable from the
//!     pre-fix bug's *incorrect* `EXISTS == 0` on a still-cold key, purely
//!     from the client's point of view.
//!   * Fix: use `PROBE_COUNT` (32) probe collections per type instead of one,
//!     half written before the filler wave and half interleaved DURING it
//!     (`drive_eviction_interleaved`), so across the whole batch some probes
//!     are very likely to be claimed by each path. After the wave, classify
//!     every probe as `Complete` (present, full original content — the
//!     promote-from-cold path ran), `Absent` (gone, empty content — a
//!     legitimate plain-drop), or `Ghost` (anything else: `EXISTS`
//!     disagreeing with content, or partial/corrupted content — the actual
//!     P0 shape). The suite fails on any `Ghost`, and separately fails (with
//!     an actionable diagnostic) if EVERY probe came back `Absent` — that
//!     would mean this run never exercised the promote path at all, which
//!     would make the rest of the assertions vacuous.
//!   * The no-fabrication-on-write check then runs twice: once against a
//!     `Complete` probe (the write MUST merge with the promoted content) and
//!     once against an `Absent` probe if one exists (the write legitimately
//!     creates a fresh container — there is no cold copy left to merge with,
//!     the plain-drop path already deleted it).
//!   * Eviction/spill is tick-driven or write-path-driven, not synchronous
//!     with the specific filler write that crosses the threshold — a short
//!     settle sleep follows the filler wave before probing.
//!   * A classification's `EXISTS` and read-all round trips are not atomic:
//!     `evict_one_async_spill` removes the hot entry immediately but hands
//!     off to a background spill-thread channel, so a probe caught squarely
//!     in that in-flight window can transiently answer `EXISTS == 0` and
//!     then (a moment later, on the next round trip) read non-empty. This
//!     is NOT the P0 shape — it resolves to a stable state within
//!     milliseconds. `partition_probes` re-checks a ghost verdict a handful
//!     of times (`GHOST_RECHECK_ATTEMPTS`/`GHOST_RECHECK_DELAY`) before
//!     failing, so only a verdict that never stabilizes counts as a real
//!     ghost (observed in practice: caught exactly this transient shape once
//!     during hardening on the Linux VM, confirmed non-reproducing after the
//!     retry was added).
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

/// Number of probe collections per type. Deliberately > 1: a single probe
/// makes the suite nondeterministic about which of the two eviction paths
/// (interactive write-gate spill vs. periodic-tick plain-drop) claims it —
/// see the module doc. With many probes, both paths are very likely to claim
/// at least one each within a single run.
const PROBE_COUNT: usize = 32;

/// Drive memory well past `MAXMEMORY_BYTES` with unique filler keys so
/// `allkeys-lru` evicts everything written before this point — namely the
/// probe keys the caller wrote up front. `extra_probes` additional probes are
/// written via `write_extra_probe(client, index)` INTERLEAVED across the
/// filler wave (evenly spaced), rather than all up front, so the
/// synchronous/interactive write-path eviction gate (which spills
/// collections) has as much chance of claiming a probe as the periodic
/// background tick (which does not — see module doc). Settles briefly
/// afterward: eviction/spill is not synchronous with the specific write that
/// crosses the threshold.
fn drive_eviction_interleaved(
    c: &mut Client,
    extra_probes: usize,
    mut write_extra_probe: impl FnMut(&mut Client, usize),
) {
    const FILLER_COUNT: usize = 400;
    const FILLER_VALUE_LEN: usize = 4 * 1024; // 4 KiB — 400 * 4KiB ~= 1.6MB >> 512KB cap.
    let value = blob(FILLER_VALUE_LEN, b'f');
    let interval = if extra_probes == 0 {
        0
    } else {
        (FILLER_COUNT / extra_probes).max(1)
    };
    let mut next_probe = 0usize;
    for i in 0..FILLER_COUNT {
        let key = format!("filler:{i}");
        let _ = c.cmd(&[b"SET", key.as_bytes(), &value]);
        if next_probe < extra_probes && i % interval == 0 {
            write_extra_probe(c, next_probe);
            next_probe += 1;
        }
    }
    // Rounding leftovers (shouldn't happen for PROBE_COUNT/FILLER_COUNT
    // combinations in this file, but stay correct regardless).
    while next_probe < extra_probes {
        write_extra_probe(c, next_probe);
        next_probe += 1;
    }
    std::thread::sleep(Duration::from_secs(3));
}

/// Classification of one probe's post-eviction state.
#[derive(Debug)]
enum ProbeState {
    /// Present with the COMPLETE original content — the promote-from-cold
    /// path ran (or the probe was never actually evicted).
    Complete,
    /// Gone entirely (`EXISTS == 0`, empty read) — a legitimate plain-drop
    /// by the tick path (no `SpillContext`, no tiering, correct semantics).
    Absent,
    /// Anything else: `EXISTS` disagreeing with content, or partial/
    /// corrupted content. This is the actual P0 shape (a cold-spilled value
    /// visible to neither read nor EXISTS, or a write that clobbered rather
    /// than merged) and always fails the test.
    Ghost(String),
}

fn exists_bit(c: &mut Client, key: &str) -> Result<bool, String> {
    match c.cmd(&[b"EXISTS", key.as_bytes()]) {
        V::Int(1) => Ok(true),
        V::Int(0) => Ok(false),
        other => Err(format!(
            "EXISTS returned unexpected value for {key}: {other:?}"
        )),
    }
}

fn classify_hash(c: &mut Client, key: &str) -> ProbeState {
    let exists = match exists_bit(c, key) {
        Ok(b) => b,
        Err(msg) => return ProbeState::Ghost(msg),
    };
    let all = c.cmd(&[b"HGETALL", key.as_bytes()]);
    let items = match &all {
        V::Arr(items) => items,
        other => {
            return ProbeState::Ghost(format!("HGETALL returned non-array for {key}: {other:?}"));
        }
    };
    if items.is_empty() {
        return if exists {
            ProbeState::Ghost(format!("EXISTS=1 but HGETALL empty for {key}"))
        } else {
            ProbeState::Absent
        };
    }
    if !exists {
        return ProbeState::Ghost(format!("EXISTS=0 but HGETALL non-empty for {key}: {all:?}"));
    }
    let mut map = std::collections::HashMap::new();
    for chunk in items.chunks(2) {
        match (&chunk[0], chunk.get(1)) {
            (V::Bulk(f), Some(V::Bulk(v))) => {
                map.insert(f.clone(), v.clone());
            }
            _ => return ProbeState::Ghost(format!("HGETALL malformed pair for {key}: {all:?}")),
        }
    }
    let expected: std::collections::HashMap<Vec<u8>, Vec<u8>> = [
        (b"f1".to_vec(), b"v1".to_vec()),
        (b"f2".to_vec(), b"v2".to_vec()),
    ]
    .into_iter()
    .collect();
    if map == expected {
        ProbeState::Complete
    } else {
        ProbeState::Ghost(format!("HGETALL partial/wrong content for {key}: {all:?}"))
    }
}

fn classify_list(c: &mut Client, key: &str) -> ProbeState {
    let exists = match exists_bit(c, key) {
        Ok(b) => b,
        Err(msg) => return ProbeState::Ghost(msg),
    };
    let range = c.cmd(&[b"LRANGE", key.as_bytes(), b"0", b"-1"]);
    let items = match &range {
        V::Arr(items) => items,
        other => {
            return ProbeState::Ghost(format!("LRANGE returned non-array for {key}: {other:?}"));
        }
    };
    if items.is_empty() {
        return if exists {
            ProbeState::Ghost(format!("EXISTS=1 but LRANGE empty for {key}"))
        } else {
            ProbeState::Absent
        };
    }
    if !exists {
        return ProbeState::Ghost(format!(
            "EXISTS=0 but LRANGE non-empty for {key}: {range:?}"
        ));
    }
    let expected = vec![V::Bulk(b"a".to_vec()), V::Bulk(b"b".to_vec())];
    if *items == expected {
        ProbeState::Complete
    } else {
        ProbeState::Ghost(format!("LRANGE partial/wrong content for {key}: {range:?}"))
    }
}

fn classify_set(c: &mut Client, key: &str) -> ProbeState {
    let exists = match exists_bit(c, key) {
        Ok(b) => b,
        Err(msg) => return ProbeState::Ghost(msg),
    };
    let members = c.cmd(&[b"SMEMBERS", key.as_bytes()]);
    let items = match &members {
        V::Arr(items) => items,
        other => {
            return ProbeState::Ghost(format!("SMEMBERS returned non-array for {key}: {other:?}"));
        }
    };
    if items.is_empty() {
        return if exists {
            ProbeState::Ghost(format!("EXISTS=1 but SMEMBERS empty for {key}"))
        } else {
            ProbeState::Absent
        };
    }
    if !exists {
        return ProbeState::Ghost(format!(
            "EXISTS=0 but SMEMBERS non-empty for {key}: {members:?}"
        ));
    }
    let mut set = std::collections::HashSet::new();
    for it in items {
        match it {
            V::Bulk(b) => {
                set.insert(b.clone());
            }
            other => {
                return ProbeState::Ghost(format!("SMEMBERS non-bulk item for {key}: {other:?}"));
            }
        }
    }
    let expected: std::collections::HashSet<Vec<u8>> =
        [b"alpha".to_vec(), b"beta".to_vec()].into_iter().collect();
    if set == expected {
        ProbeState::Complete
    } else {
        ProbeState::Ghost(format!(
            "SMEMBERS partial/wrong content for {key}: {members:?}"
        ))
    }
}

fn classify_zset(c: &mut Client, key: &str) -> ProbeState {
    let exists = match exists_bit(c, key) {
        Ok(b) => b,
        Err(msg) => return ProbeState::Ghost(msg),
    };
    let range = c.cmd(&[b"ZRANGE", key.as_bytes(), b"0", b"-1"]);
    let items = match &range {
        V::Arr(items) => items,
        other => {
            return ProbeState::Ghost(format!("ZRANGE returned non-array for {key}: {other:?}"));
        }
    };
    if items.is_empty() {
        return if exists {
            ProbeState::Ghost(format!("EXISTS=1 but ZRANGE empty for {key}"))
        } else {
            ProbeState::Absent
        };
    }
    if !exists {
        return ProbeState::Ghost(format!(
            "EXISTS=0 but ZRANGE non-empty for {key}: {range:?}"
        ));
    }
    let expected = vec![V::Bulk(b"one".to_vec()), V::Bulk(b"two".to_vec())];
    if *items == expected {
        ProbeState::Complete
    } else {
        ProbeState::Ghost(format!("ZRANGE partial/wrong content for {key}: {range:?}"))
    }
}

/// `classify_*` issues two separate round trips (`EXISTS`, then a
/// type-specific read-all) that are not atomic with respect to the server:
/// `evict_one_async_spill` removes the hot entry and hands the value to a
/// background spill-thread channel, which durably records it in the
/// `ColdIndex` slightly LATER. A probe caught squarely in that in-flight
/// window can legitimately answer `EXISTS == 0` on one round trip and then
/// `ZRANGE`-non-empty (post-completion) a moment later on the next —
/// transient, not the permanent P0 shape (a value durably cold-spilled but
/// invisible to every reader forever). Re-checking a handful of times lets a
/// transient window resolve to a stable `Complete`/`Absent` without masking a
/// real, persistent ghost, which never resolves.
const GHOST_RECHECK_ATTEMPTS: usize = 10;
const GHOST_RECHECK_DELAY: Duration = Duration::from_millis(150);

/// Splits classified probe indices into (complete, absent), failing the test
/// immediately on any ghost and again if the promote-from-cold path was
/// never exercised at all (every probe plain-dropped).
fn partition_probes(
    c: &mut Client,
    kind: &str,
    key_of: impl Fn(usize) -> String,
    classify: impl Fn(&mut Client, &str) -> ProbeState,
) -> (Vec<usize>, Vec<usize>) {
    let mut complete = Vec::new();
    let mut absent = Vec::new();
    let mut ghosts = Vec::new();
    for i in 0..PROBE_COUNT {
        let key = key_of(i);
        let mut state = classify(c, &key);
        let mut attempts = 1;
        while matches!(state, ProbeState::Ghost(_)) && attempts < GHOST_RECHECK_ATTEMPTS {
            std::thread::sleep(GHOST_RECHECK_DELAY);
            state = classify(c, &key);
            attempts += 1;
        }
        match state {
            ProbeState::Complete => complete.push(i),
            ProbeState::Absent => absent.push(i),
            ProbeState::Ghost(msg) => {
                ghosts.push(format!("{key} (stable after {attempts} attempts): {msg}"));
            }
        }
    }
    assert!(
        ghosts.is_empty(),
        "P0 GHOST on {} of {PROBE_COUNT} {kind} probes — EXISTS/content disagreement or \
         partial content (promoted-but-invisible, or fabricated-over-cold-copy):\n{}",
        ghosts.len(),
        ghosts.join("\n")
    );
    assert!(
        !complete.is_empty(),
        "all {PROBE_COUNT} {kind} probes were plain-dropped (Absent) by the background tick; \
         none were claimed by the interactive spill-and-promote write-path gate, so this run \
         never exercised the cold-collection promote path at all. Lower the filler wave's \
         FILLER_COUNT/FILLER_VALUE_LEN (drive_eviction_interleaved) or raise MAXMEMORY_BYTES so \
         eviction pressure ramps more gradually, giving the interactive gate more opportunities \
         to claim a probe before the tick does; or increase PROBE_COUNT for more samples."
    );
    (complete, absent)
}

fn arr_len(v: &V) -> usize {
    match v {
        V::Arr(items) => items.len(),
        other => panic!("expected Arr, got {other:?}"),
    }
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
// Hash: HSET (listpack fast path) -> evict/spill (racy) -> ghost-free +
// no-fabrication-on-write checks
// ---------------------------------------------------------------------------

#[test]
fn test_cold_hash_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    let key_of = |i: usize| format!("probehash:{i}");
    let write_probe = |c: &mut Client, i: usize| {
        // Small field/value sizes deliberately keep this on the listpack fast
        // path (`get_or_create_hash_listpack`), the most common real-world
        // HSET shape — proving the fix covers it, not just the full-HashMap
        // path.
        let _ = c.cmd(&[b"HSET", key_of(i).as_bytes(), b"f1", b"v1", b"f2", b"v2"]);
    };

    let upfront = PROBE_COUNT / 2;
    for i in 0..upfront {
        write_probe(&mut c, i);
    }
    drive_eviction_interleaved(&mut c, PROBE_COUNT - upfront, |c, j| {
        write_probe(c, upfront + j);
    });

    let (complete, absent) = partition_probes(&mut c, "hash", key_of, classify_hash);

    // No-fabrication-on-write, PRESENT case: HSET on a promoted hash must
    // MERGE with the promoted fields, never replace them.
    let present_key = key_of(complete[0]);
    assert_eq!(
        c.cmd(&[b"HSET", present_key.as_bytes(), b"f3", b"v3"]),
        V::Int(1),
        "f3 must be counted as a newly-added field on {present_key}"
    );
    let after = c.cmd(&[b"HGETALL", present_key.as_bytes()]);
    assert_eq!(
        arr_len(&after),
        6,
        "P0: HSET must MERGE with the promoted cold fields on {present_key}, not fabricate an \
         empty hash and lose f1/f2: {after:?}"
    );

    // Fresh-container case: a genuinely absent (plain-dropped) probe's next
    // HSET legitimately creates a brand-new hash — there is no cold copy to
    // merge with, the tick path already deleted it.
    if let Some(&idx) = absent.first() {
        let absent_key = key_of(idx);
        assert_eq!(
            c.cmd(&[b"HSET", absent_key.as_bytes(), b"f3", b"v3"]),
            V::Int(1)
        );
        let fresh = c.cmd(&[b"HGETALL", absent_key.as_bytes()]);
        assert_eq!(
            arr_len(&fresh),
            2,
            "fresh hash on genuinely-absent {absent_key} must contain only the new field: {fresh:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// List: RPUSH (listpack fast path) -> evict/spill (racy) -> ghost-free +
// no-fabrication-on-write checks
// ---------------------------------------------------------------------------

#[test]
fn test_cold_list_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    let key_of = |i: usize| format!("probelist:{i}");
    let write_probe = |c: &mut Client, i: usize| {
        let _ = c.cmd(&[b"RPUSH", key_of(i).as_bytes(), b"a", b"b"]);
    };

    let upfront = PROBE_COUNT / 2;
    for i in 0..upfront {
        write_probe(&mut c, i);
    }
    drive_eviction_interleaved(&mut c, PROBE_COUNT - upfront, |c, j| {
        write_probe(c, upfront + j);
    });

    let (complete, absent) = partition_probes(&mut c, "list", key_of, classify_list);

    let present_key = key_of(complete[0]);
    assert_eq!(c.cmd(&[b"RPUSH", present_key.as_bytes(), b"c"]), V::Int(3));
    let after = c.cmd(&[b"LRANGE", present_key.as_bytes(), b"0", b"-1"]);
    match &after {
        V::Arr(items) => assert_eq!(
            items,
            &[
                V::Bulk(b"a".to_vec()),
                V::Bulk(b"b".to_vec()),
                V::Bulk(b"c".to_vec())
            ],
            "P0: RPUSH must MERGE with the promoted cold elements on {present_key}, not \
             fabricate an empty list and lose a/b: {after:?}"
        ),
        other => panic!("expected Arr from LRANGE, got {other:?}"),
    }

    if let Some(&idx) = absent.first() {
        let absent_key = key_of(idx);
        assert_eq!(c.cmd(&[b"RPUSH", absent_key.as_bytes(), b"c"]), V::Int(1));
        let fresh = c.cmd(&[b"LRANGE", absent_key.as_bytes(), b"0", b"-1"]);
        match &fresh {
            V::Arr(items) => assert_eq!(
                items,
                &[V::Bulk(b"c".to_vec())],
                "fresh list on genuinely-absent {absent_key} must contain only the new \
                 element: {fresh:?}"
            ),
            other => panic!("expected Arr from LRANGE, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Set: SADD -> evict/spill (racy) -> ghost-free + no-fabrication-on-write
// checks
// ---------------------------------------------------------------------------

#[test]
fn test_cold_set_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    let key_of = |i: usize| format!("probeset:{i}");
    let write_probe = |c: &mut Client, i: usize| {
        // Non-integer members keep this off the intset fast path, exercising
        // `get_or_create_set` (the general HashSet path).
        let _ = c.cmd(&[b"SADD", key_of(i).as_bytes(), b"alpha", b"beta"]);
    };

    let upfront = PROBE_COUNT / 2;
    for i in 0..upfront {
        write_probe(&mut c, i);
    }
    drive_eviction_interleaved(&mut c, PROBE_COUNT - upfront, |c, j| {
        write_probe(c, upfront + j);
    });

    let (complete, absent) = partition_probes(&mut c, "set", key_of, classify_set);

    let present_key = key_of(complete[0]);
    assert_eq!(
        c.cmd(&[b"SADD", present_key.as_bytes(), b"gamma"]),
        V::Int(1)
    );
    let after = c.cmd(&[b"SMEMBERS", present_key.as_bytes()]);
    assert_eq!(
        arr_as_bulk_set(&after),
        std::collections::HashSet::from([b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]),
        "P0: SADD must MERGE with the promoted cold members on {present_key}, not fabricate an \
         empty set and lose alpha/beta: {after:?}"
    );

    if let Some(&idx) = absent.first() {
        let absent_key = key_of(idx);
        assert_eq!(
            c.cmd(&[b"SADD", absent_key.as_bytes(), b"gamma"]),
            V::Int(1)
        );
        let fresh = c.cmd(&[b"SMEMBERS", absent_key.as_bytes()]);
        assert_eq!(
            arr_as_bulk_set(&fresh),
            std::collections::HashSet::from([b"gamma".to_vec()]),
            "fresh set on genuinely-absent {absent_key} must contain only the new member: {fresh:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// ZSet: ZADD -> evict/spill (racy) -> ghost-free + no-fabrication-on-write
// checks
// ---------------------------------------------------------------------------

#[test]
fn test_cold_zset_visible_after_eviction_no_fabrication_on_write() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_cold_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    let key_of = |i: usize| format!("probezset:{i}");
    let write_probe = |c: &mut Client, i: usize| {
        let _ = c.cmd(&[b"ZADD", key_of(i).as_bytes(), b"1", b"one", b"2", b"two"]);
    };

    let upfront = PROBE_COUNT / 2;
    for i in 0..upfront {
        write_probe(&mut c, i);
    }
    drive_eviction_interleaved(&mut c, PROBE_COUNT - upfront, |c, j| {
        write_probe(c, upfront + j);
    });

    let (complete, absent) = partition_probes(&mut c, "zset", key_of, classify_zset);

    let present_key = key_of(complete[0]);
    assert_eq!(
        c.cmd(&[b"ZADD", present_key.as_bytes(), b"3", b"three"]),
        V::Int(1)
    );
    let after = c.cmd(&[b"ZRANGE", present_key.as_bytes(), b"0", b"-1"]);
    match &after {
        V::Arr(items) => assert_eq!(
            items,
            &[
                V::Bulk(b"one".to_vec()),
                V::Bulk(b"two".to_vec()),
                V::Bulk(b"three".to_vec())
            ],
            "P0: ZADD must MERGE with the promoted cold members on {present_key}, not \
             fabricate an empty zset and lose one/two: {after:?}"
        ),
        other => panic!("expected Arr from ZRANGE, got {other:?}"),
    }

    if let Some(&idx) = absent.first() {
        let absent_key = key_of(idx);
        assert_eq!(
            c.cmd(&[b"ZADD", absent_key.as_bytes(), b"3", b"three"]),
            V::Int(1)
        );
        let fresh = c.cmd(&[b"ZRANGE", absent_key.as_bytes(), b"0", b"-1"]);
        match &fresh {
            V::Arr(items) => assert_eq!(
                items,
                &[V::Bulk(b"three".to_vec())],
                "fresh zset on genuinely-absent {absent_key} must contain only the new \
                 member: {fresh:?}"
            ),
            other => panic!("expected Arr from ZRANGE, got {other:?}"),
        }
    }
}
