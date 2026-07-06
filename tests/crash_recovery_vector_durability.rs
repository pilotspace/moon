//! B4: vector-index durability kill-9 crash-recovery integration tests.
//!
//! Proves the B1-B3 durability line (see `tmp/VECTOR-DURABILITY-DESIGN.md`)
//! end to end against REAL server processes: manifest+segment+keymap
//! persist-on-compact (B1/B2), and startup recovery + dedup rescan +
//! deletion probe + orphan sweep (B3, `src/vector/persistence/recover_v2.rs`).
//!
//! Five scenarios, one `#[test]` each, sharing the harness below:
//!   S1 unchanged-keys fast path (dedup rescan skips re-encoding)
//!   S2 updates+deletes across a crash (reconcile + deletion probe)
//!   S3 orphan sweep (stray staging/segment/keymap files removed on boot)
//!   S4 collection_id pin survives a post-recovery compact+GraphUnion merge
//!   S5 no-persist-dir regression guard (`--appendonly no`: no idx-* dirs)
//!
//! Every wait is a bounded, condition-based poll (port accept / manifest
//! file / log line / FT.INFO field) — never a fixed sleep as
//! synchronization (this repo has a documented 100ms-sleep flake history,
//! see `tests/crash_matrix_per_shard_aof.rs` and the parity-fix that
//! replaced it with a bind-wait).
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_vector_durability -- --ignored
//!
//! tokio runtime:
//!   cargo build --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index
//!   cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index \
//!     --test crash_recovery_vector_durability -- --ignored
//!
//! Requires: built release binary (`MOON_BIN` env var honored, falls back to
//! `target/release/moon` then `target/debug/moon` — see `find_moon_binary`).

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use moon::vector::persistence::manifest::{self, IndexManifest};

// 32 (not 8): S4's clustered recall check needs enough axes that SQ8
// quantization doesn't produce frequent EXACT decoded-distance ties within
// a tight cluster — ties destabilize the internal merge recall gate's
// brute-force "top-k ground truth" SET independent of actual HNSW/graph
// quality (confirmed empirically: the gate's reported recall was IDENTICAL
// to 16 significant digits across very different noise/M/EF_CONSTRUCTION
// settings at DIM=8, the signature of tie-bound ground truth, not a
// genuine approximation gap).
const DIM: usize = 32;

// ---------------------------------------------------------------------------
// Binary resolution (pattern: tests/vector_del_unindex.rs / shardslice_live.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let release = PathBuf::from(format!("{manifest}/target/release/moon"));
    if release.exists() {
        return release;
    }
    let debug = PathBuf::from(format!("{manifest}/target/debug/moon"));
    if debug.exists() {
        return debug;
    }
    panic!(
        "No moon binary found. Build with `cargo build --release` or set \
         MOON_BIN=/path/to/moon."
    );
}

fn unique_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn unique_dir(suffix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-vecdur-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

// ---------------------------------------------------------------------------
// Server spawn / crash / restart harness
// ---------------------------------------------------------------------------

/// Kills the wrapped child on drop (SIGKILL + waitpid), plus a best-effort
/// `pkill -9 -f <dir>` backstop keyed on this test's unique temp dir path —
/// a moon process launched with `--dir <this path>` can only ever be one
/// this test itself spawned, so the backstop cannot collide with another
/// test's server (documented gotcha: leaked busy-poller / SIGTERM+SO_REUSEPORT
/// hangs — always kill -9, never rely on graceful shutdown).
struct ServerGuard {
    child: Child,
    dir_marker: String,
}

impl ServerGuard {
    fn new(child: Child, dir: &Path) -> Self {
        Self {
            child,
            dir_marker: dir.to_string_lossy().into_owned(),
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // Idempotent: a test that already SIGKILLed+waited this child
        // earlier (the crash-then-restart flow) must not re-signal a
        // possibly-recycled pid. Only signal if the process is still
        // observed running; otherwise just reap (harmless if already reaped).
        if matches!(self.child.try_wait(), Ok(None)) {
            sigkill(&mut self.child);
        } else {
            let _ = self.child.wait();
        }
        // Best-effort backstop in case anything survived under a different
        // pid (e.g. a forked helper) — matches this test's own unique --dir
        // path, so it can never collide with another test's server.
        let _ = Command::new("pkill")
            .args(["-9", "-f"])
            .arg(&self.dir_marker)
            .output();
    }
}

#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Wait until nothing is accepting on `port` — required before a same-port
/// restart or the new listener can race the dying process's socket teardown
/// (moon binds SO_REUSEPORT per shard, so a plain bind-based check is
/// useless; see `crash_recovery_disk_offload_no_aof.rs`'s extensive
/// rationale for this exact pattern).
fn wait_for_port_down(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let mut consecutive_refused = 0;
    for _ in 0..120 {
        match TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(100)) {
            Ok(_) => {
                consecutive_refused = 0;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                consecutive_refused += 1;
                if consecutive_refused >= 2 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Spawn moon with `--appendonly yes --shards 1`, stdout/stderr captured to
/// per-test log files (never `Stdio::null()` — a CI flake needs a real
/// diagnostic, not silence). `RUST_LOG=moon=info` is set explicitly so the
/// B3 recovery acceptance-signal `info!` line is emitted regardless of the
/// ambient environment (the binary's own default is already `moon=info`,
/// but the test must not depend on that default silently).
///
/// `--disk-offload disable`: disk-offload defaults to `enable` (see
/// `src/config.rs`'s `#[arg(long = "disk-offload", default_value = "enable")]`),
/// and when enabled the vector persist dir resolves to `<dir>/shard-<N>`
/// instead of `<dir>/shard-<N>-vectors` (`src/shard/event_loop.rs` ~857) —
/// this test targets the plain AOF-driven persistence path the design doc
/// describes, so disk-offload must be off to get the path this harness polls.
fn spawn_moon_aof(port: u16, dir: &Path) -> ServerGuard {
    std::fs::create_dir_all(dir).expect("create test dir");
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--disk-offload",
            "disable",
            // The dev volume routinely sits near Moon's 5% diskfull guard
            // (writes pause with MOONERR diskfull); this test's durability is
            // proven by kill -9 + recovery, not by the free-space monitor.
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .env("RUST_LOG", "moon=info")
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` first, or set MOON_BIN)");
    ServerGuard::new(child, dir)
}

/// Same as `spawn_moon_aof` but `--appendonly no` with disk-offload also
/// explicitly disabled — S5's regression guard (no persistence_dir at all).
fn spawn_moon_no_persist(port: u16, dir: &Path) -> ServerGuard {
    std::fs::create_dir_all(dir).expect("create test dir");
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "no",
            "--disk-offload",
            "disable",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .env("RUST_LOG", "moon=info")
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` first, or set MOON_BIN)");
    ServerGuard::new(child, dir)
}

/// Restart attempts — a rapid SIGKILL->rebind on the same port can lose a
/// transient EADDRINUSE race against the dying process's socket teardown.
/// That is an OS timing artifact, not a recovery defect; retry bounded.
const RESTART_ATTEMPTS: usize = 6;

fn start_moon_alive(
    spawn: impl Fn(u16, &Path) -> ServerGuard,
    port: u16,
    dir: &Path,
) -> ServerGuard {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut guard = spawn(port, dir);
        let mut up = false;
        for _ in 0..100 {
            if let Ok(Some(_status)) = guard.child.try_wait() {
                break; // self-terminated (rebind race) — retry
            }
            if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if up {
            return guard;
        }
        drop(guard); // Drop kills + reaps; back off before retrying.
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!("moon failed to start+serve on port {port} after {RESTART_ATTEMPTS} attempts");
}

// ---------------------------------------------------------------------------
// Minimal RESP client (pattern: tests/vector_del_unindex.rs)
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
            .set_read_timeout(Some(Duration::from_secs(20)))
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
}

fn wait_ready(port: u16) -> Client {
    let mut c = Client::connect(port);
    let start = Instant::now();
    loop {
        match c.cmd(&[b"PING"]) {
            V::Simple(s) if s == "PONG" => return c,
            _ if start.elapsed() < Duration::from_secs(30) => {
                std::thread::sleep(Duration::from_millis(100));
            }
            other => panic!("server never answered PING: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Vector fixtures
// ---------------------------------------------------------------------------

/// Deterministic, well-separated f32 vector (LE bytes) for identity-style
/// KNN probes (exact match = distance 0). Uses a splitmix64-style hash per
/// (seed, dim) pair rather than a small modulus — a modulus (e.g. `% 9973`)
/// wraps distinct seeds back into the SAME narrow value range, so two
/// unrelated seeds (in particular a base id and `id + large_offset`, as S2
/// uses to build a "far away" update vector) can collide onto
/// near-identical vectors and break identity-style KNN assertions (caught
/// by this test suite's own flake-hunt). A hash mix has no such periodicity:
/// distinct u32 seeds land in well-separated regions of R^DIM with
/// overwhelming probability.
fn simple_vec_bytes(seed: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(DIM * 4);
    for i in 0..DIM {
        let mut h = (seed as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add((i as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9))
            .wrapping_add(0x94D0_49BB_1331_11EB);
        h ^= h >> 33;
        h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 33;
        h = h.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
        h ^= h >> 33;
        // Map the top 24 bits to a value in [0, 10) — a continuous range
        // wide enough that hash collisions between distinct seeds are
        // astronomically unlikely at the N used by these tests.
        let v = ((h >> 40) as f32 / (1u64 << 24) as f32) * 10.0;
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Deterministic LCG, seeded per test — used for S4's clustered dataset.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 40) as f32) / (1u64 << 24) as f32
    }
    fn randn(&mut self) -> f32 {
        let u1 = self.next_f32().max(1e-7);
        let u2 = self.next_f32();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos()
    }
}

fn random_vec(rng: &mut Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.randn()).collect()
}

fn f32s_to_le_bytes(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// FT.* / VACUUM helpers
// ---------------------------------------------------------------------------

fn ft_create(
    c: &mut Client,
    idx: &str,
    dim: usize,
    compact_threshold: u32,
    ef_runtime: Option<u32>,
) {
    ft_create_ex(c, idx, dim, compact_threshold, ef_runtime, None, None);
}

/// Extended FT.CREATE with optional HNSW `M` / `EF_CONSTRUCTION` overrides —
/// S4 needs a higher-fidelity graph than the defaults (M=16, EF_CONSTRUCTION=200)
/// to keep the merge's internal recall gate (fixed 0.90 tolerance,
/// `src/vector/segment/compaction.rs::verify_merge_recall`) comfortably clear
/// of its threshold on a small (~1000-vector) synthetic dataset.
#[allow(clippy::too_many_arguments)]
fn ft_create_ex(
    c: &mut Client,
    idx: &str,
    dim: usize,
    compact_threshold: u32,
    ef_runtime: Option<u32>,
    m: Option<u32>,
    ef_construction: Option<u32>,
) {
    let prefix = format!("{idx}:");
    let mut params: Vec<Vec<u8>> = vec![
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        dim.to_string().into_bytes(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
        b"QUANTIZATION".to_vec(),
        b"SQ8".to_vec(),
        b"COMPACT_THRESHOLD".to_vec(),
        compact_threshold.to_string().into_bytes(),
    ];
    if let Some(ef) = ef_runtime {
        params.push(b"EF_RUNTIME".to_vec());
        params.push(ef.to_string().into_bytes());
    }
    if let Some(m) = m {
        params.push(b"M".to_vec());
        params.push(m.to_string().into_bytes());
    }
    if let Some(efc) = ef_construction {
        params.push(b"EF_CONSTRUCTION".to_vec());
        params.push(efc.to_string().into_bytes());
    }
    let mut args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        idx.as_bytes().to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        prefix.into_bytes(),
        b"SCHEMA".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        params.len().to_string().into_bytes(),
    ];
    args.extend(params);
    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
    let r = c.cmd(&refs);
    assert_eq!(r, V::Simple("OK".into()), "FT.CREATE {idx} failed: {r:?}");
}

fn hset_batch(c: &mut Client, prefix: &str, ids: &[u32], blob_of: impl Fn(u32) -> Vec<u8>) {
    let cmds: Vec<Vec<Vec<u8>>> = ids
        .iter()
        .map(|&i| {
            let key = format!("{prefix}{i}");
            vec![
                b"HSET".to_vec(),
                key.into_bytes(),
                b"vec".to_vec(),
                blob_of(i),
            ]
        })
        .collect();
    let replies = c.pipeline(&cmds);
    for (i, r) in replies.iter().enumerate() {
        assert!(matches!(r, V::Int(_)), "HSET batch #{i} failed: {r:?}");
    }
}

fn ft_compact(c: &mut Client, idx: &str) {
    let r = c.cmd(&[b"FT.COMPACT", idx.as_bytes()]);
    assert_eq!(r, V::Simple("OK".into()), "FT.COMPACT {idx} failed: {r:?}");
}

fn vacuum_vector(c: &mut Client, idx: &str) -> String {
    let r = c.cmd(&[b"VACUUM", b"VECTOR", idx.as_bytes()]);
    match r {
        V::Simple(s) => s,
        other => panic!("VACUUM VECTOR {idx} unexpected reply: {other:?}"),
    }
}

fn search_keys(c: &mut Client, idx: &str, k: u32, blob: &[u8]) -> Vec<String> {
    let query = format!("*=>[KNN {k} @vec $B]");
    let r = c.cmd(&[
        b"FT.SEARCH",
        idx.as_bytes(),
        query.as_bytes(),
        b"PARAMS",
        b"2",
        b"B",
        blob,
        b"DIALECT",
        b"2",
    ]);
    let V::Arr(items) = r else {
        panic!("FT.SEARCH {idx} reply not array: {r:?}");
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

fn ft_info_num_docs(c: &mut Client, idx: &str) -> i64 {
    let r = c.cmd(&[b"FT.INFO", idx.as_bytes()]);
    let V::Arr(items) = r else {
        panic!("FT.INFO {idx} reply not array: {r:?}");
    };
    for pair in items.chunks(2) {
        if let [V::Bulk(k), V::Int(n)] = pair
            && k.as_slice() == b"num_docs"
        {
            return *n;
        }
    }
    panic!("num_docs not found in FT.INFO {idx} reply: {items:?}");
}

fn ft_list(c: &mut Client) -> Vec<String> {
    let r = c.cmd(&[b"FT._LIST"]);
    match r {
        V::Arr(items) => items
            .into_iter()
            .filter_map(|v| match v {
                V::Bulk(b) => Some(String::from_utf8_lossy(&b).into_owned()),
                _ => None,
            })
            .collect(),
        V::Null => Vec::new(),
        other => panic!("FT._LIST unexpected reply: {other:?}"),
    }
}

fn assert_absent(keys: &[String], dead: &str, ctx: &str) {
    assert!(
        !keys.iter().any(|k| k == dead),
        "{ctx}: deleted/absent key {dead} resurfaced in FT.SEARCH results {keys:?}"
    );
}

// ---------------------------------------------------------------------------
// Persistence-layer polling helpers (manifest + log line)
// ---------------------------------------------------------------------------

/// Vector persist dir for shard 0 under `--appendonly yes --shards 1`
/// (`<dir>/shard-0-vectors`, per `src/shard/event_loop.rs` ~865).
fn vector_persist_dir(dir: &Path) -> PathBuf {
    dir.join("shard-0-vectors")
}

/// Bounded poll for `manifest.json` to reach at least `min_segments` live
/// segment ids. This is the harness's substitute for a fixed sleep before
/// SIGKILL — the background snapshot job (`global_snapshot_pool()`) commits
/// asynchronously after a compact/merge install, so the test must observe
/// the durable artifact directly rather than guess a timing window.
fn wait_for_manifest_min_segments(
    idx_dir: &Path,
    min_segments: usize,
    timeout: Duration,
) -> IndexManifest {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(m) = manifest::read_manifest_tolerant(idx_dir)
            && m.segment_ids.len() >= min_segments
        {
            return m;
        }
        if Instant::now() >= deadline {
            let found = manifest::read_manifest_tolerant(idx_dir)
                .map(|m| m.segment_ids.len())
                .unwrap_or(0);
            panic!(
                "manifest at {:?} did not reach {} segment(s) within {:?} (found {})",
                idx_dir, min_segments, timeout, found
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Poll until the manifest holds EXACTLY `n_segments` segments (used after a
/// merge, whose manifest rewrite is an async SnapshotPool job — a min-N wait
/// would return instantly on the stale pre-merge manifest).
fn wait_for_manifest_exact_segments(
    idx_dir: &Path,
    n_segments: usize,
    timeout: Duration,
) -> IndexManifest {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(m) = manifest::read_manifest_tolerant(idx_dir)
            && m.segment_ids.len() == n_segments
        {
            return m;
        }
        if Instant::now() >= deadline {
            let found = manifest::read_manifest_tolerant(idx_dir)
                .map(|m| m.segment_ids)
                .unwrap_or_default();
            panic!(
                "manifest at {:?} did not converge to exactly {} segment(s) within {:?} (found {:?})",
                idx_dir, n_segments, timeout, found
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Extract the 4 integer counters from a B3 recovery acceptance log line:
/// "vector index {name}: B3 recovery — loaded {N} segment(s), {N} key(s)
/// verified unchanged, {N} re-indexed, {N} tombstoned" (exact format string
/// lives in `src/vector/persistence/recover_v2.rs::RecoveryState::finish`).
/// Slicing from AFTER the "B3 recovery" marker before scanning digits keeps
/// this robust against a timestamp/level prefix (which also contains
/// digits) added by the ambient `tracing_subscriber::fmt()` layer — slicing
/// AT the marker instead of after it is a trap: "B3" itself contains a
/// digit ('3'), which would silently shift every subsequent count by one
/// (caught by this test suite's own flake-hunt: `loaded segments` bled into
/// `verified_unchanged`'s assertion).
fn parse_recovery_counters(line: &str) -> Option<(usize, usize, usize, usize)> {
    const MARKER: &str = "B3 recovery";
    let pos = line.find(MARKER)?;
    let tail = &line[pos + MARKER.len()..];
    let mut nums: Vec<usize> = Vec::new();
    let mut cur = String::new();
    for ch in tail.chars() {
        if ch.is_ascii_digit() {
            cur.push(ch);
        } else if !cur.is_empty() {
            nums.push(cur.parse().ok()?);
            cur.clear();
        }
    }
    if !cur.is_empty() {
        nums.push(cur.parse().ok()?);
    }
    if nums.len() >= 4 {
        Some((nums[0], nums[1], nums[2], nums[3]))
    } else {
        None
    }
}

/// Bounded poll of the server's captured stdout log for the B3 recovery
/// line belonging to `idx_name`. Recovery runs synchronously during
/// per-shard startup (before the accept loop begins), so by the time
/// `wait_ready` observes PONG the line should already be flushed — Rust's
/// `std::io::Stdout` is unconditionally line-buffered (`LineWriter`), so a
/// `\n`-terminated tracing event is written through immediately regardless
/// of whether stdout is a TTY or a redirected file. The poll here is a
/// defensive bound, not a workaround for buffering.
fn wait_for_recovery_counters(
    dir: &Path,
    idx_name: &str,
    timeout: Duration,
) -> (usize, usize, usize, usize) {
    let marker = format!("vector index {idx_name}:");
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(log) = std::fs::read_to_string(dir.join("moon.stdout.log")) {
            for line in log.lines() {
                if line.contains(&marker)
                    && line.contains("B3 recovery")
                    && let Some(counters) = parse_recovery_counters(line)
                {
                    return counters;
                }
            }
        }
        if Instant::now() >= deadline {
            let log = std::fs::read_to_string(dir.join("moon.stdout.log")).unwrap_or_default();
            panic!(
                "B3 recovery log line for index {idx_name} not found within {timeout:?}.\n\
                 --- moon.stdout.log ---\n{log}"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Recursively collect every path under `root` whose file/dir name matches
/// `pred` — used by S3 (orphan detection) and S5 (no idx-* dirs at all).
fn walk_matching(root: &Path, pred: &dyn Fn(&str) -> bool) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(p: &Path, pred: &dyn Fn(&str) -> bool, acc: &mut Vec<PathBuf>) {
        let Ok(rd) = std::fs::read_dir(p) else {
            return;
        };
        for e in rd.flatten() {
            let path = e.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && pred(name)
            {
                acc.push(path.clone());
            }
            if path.is_dir() {
                walk(&path, pred, acc);
            }
        }
    }
    walk(root, pred, &mut out);
    out
}

// ---------------------------------------------------------------------------
// S1: unchanged-keys fast path
// ---------------------------------------------------------------------------

/// Insert N vectors in 2 batches, each followed by an explicit FT.COMPACT —
/// `force_compact` drains the ENTIRE mutable segment before returning (loops
/// until `frozen_len == mutable_len`; see `src/vector/store.rs::compact_segments`),
/// so two batch+compact rounds deterministically produce >= 2 immutable
/// segments with ZERO residual vectors left in the (unpersisted) mutable
/// segment — the precondition for an exact (not approximate) dedup-rescan
/// assertion after the crash.
fn build_two_segment_snapshot(c: &mut Client, idx: &str, idx_dir: &Path, n: usize) {
    let batch = n / 2;
    let batch1: Vec<u32> = (0..batch as u32).collect();
    let batch2: Vec<u32> = (batch as u32..n as u32).collect();

    hset_batch(c, &format!("{idx}:"), &batch1, simple_vec_bytes);
    ft_compact(c, idx);
    wait_for_manifest_min_segments(idx_dir, 1, Duration::from_secs(10));

    hset_batch(c, &format!("{idx}:"), &batch2, simple_vec_bytes);
    ft_compact(c, idx);
    wait_for_manifest_min_segments(idx_dir, 2, Duration::from_secs(10));
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn s1_unchanged_keys_fast_path_survives_crash() {
    const N: usize = 2000;
    const IDX: &str = "s1";

    let port = unique_port();
    let dir = unique_dir("s1");
    let vdir = vector_persist_dir(&dir);
    let idx_dir = manifest::index_persist_dir(&vdir, IDX.as_bytes());

    let guard = spawn_moon_aof(port, &dir);
    let mut c = wait_ready(port);

    ft_create(&mut c, IDX, DIM, 100, None);
    build_two_segment_snapshot(&mut c, IDX, &idx_dir, N);

    // Pre-crash KNN baseline for a fixed query set (every 200th key).
    let probe_ids: Vec<u32> = (0..N as u32).step_by(200).collect();
    let mut pre_results: Vec<Vec<String>> = Vec::new();
    for &i in &probe_ids {
        let blob = simple_vec_bytes(i);
        pre_results.push(search_keys(&mut c, IDX, 1, &blob));
    }
    let pre_num_docs = ft_info_num_docs(&mut c, IDX);
    assert_eq!(pre_num_docs, N as i64, "pre-crash num_docs must equal N");

    drop(c);
    let mut guard = guard;
    sigkill(&mut guard.child);
    wait_for_port_down(port);

    // -- Restart --------------------------------------------------------
    let guard2 = start_moon_alive(spawn_moon_aof, port, &dir);
    let mut c2 = wait_ready(port);

    // (b) B3 acceptance signal: exact dedup — the final FT.COMPACT before
    // the kill drained the mutable segment to empty, so every one of the N
    // keys must be recognized as unchanged and NONE re-indexed.
    let (loaded_segments, verified_unchanged, re_indexed, tombstoned) =
        wait_for_recovery_counters(&dir, IDX, Duration::from_secs(10));
    assert!(
        loaded_segments >= 2,
        "expected >=2 loaded segments, got {loaded_segments}"
    );
    assert_eq!(
        verified_unchanged, N,
        "every unchanged key must dedup via the metadata-only rebuild path"
    );
    assert_eq!(
        re_indexed, 0,
        "no key should need full re-encode (mutable segment was empty at crash time)"
    );
    assert_eq!(
        tombstoned, 0,
        "no key was deleted between compact and crash"
    );

    // (a) KNN results identical to pre-crash for the fixed query set.
    for (idx, &i) in probe_ids.iter().enumerate() {
        let blob = simple_vec_bytes(i);
        let post = search_keys(&mut c2, IDX, 1, &blob);
        assert_eq!(
            post, pre_results[idx],
            "post-crash top-1 for probe {i} must match pre-crash"
        );
        let want_key = format!("{IDX}:{i}");
        assert_eq!(
            post.first().map(String::as_str),
            Some(want_key.as_str()),
            "exact-match query for {want_key} must return itself as top-1"
        );
    }

    // (c) FT.INFO num_docs correct.
    let post_num_docs = ft_info_num_docs(&mut c2, IDX);
    assert_eq!(post_num_docs, N as i64, "post-crash num_docs must equal N");

    drop(c2);
    drop(guard2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// S2: updates + deletes across a crash
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn s2_updates_and_deletes_reconcile_across_crash() {
    const N: usize = 2000;
    const K: usize = 50;
    const IDX: &str = "s2";
    /// Offset applied to a key's seed on update so its new vector lands far
    /// from its original position (distinguishable in KNN).
    const UPDATE_SEED_OFFSET: u32 = 500_000;

    let port = unique_port();
    let dir = unique_dir("s2");
    let vdir = vector_persist_dir(&dir);
    let idx_dir = manifest::index_persist_dir(&vdir, IDX.as_bytes());

    let guard = spawn_moon_aof(port, &dir);
    let mut c = wait_ready(port);

    ft_create(&mut c, IDX, DIM, 100, None);
    build_two_segment_snapshot(&mut c, IDX, &idx_dir, N);

    // Mutations AFTER the last snapshot: these land in AOF only.
    // Update the first K keys (already durable in segment 1) with a new,
    // far-away vector.
    let update_ids: Vec<u32> = (0..K as u32).collect();
    hset_batch(&mut c, &format!("{IDX}:"), &update_ids, |i| {
        simple_vec_bytes(i + UPDATE_SEED_OFFSET)
    });

    // Delete K other keys from segment 2's range.
    let delete_ids: Vec<u32> = (1000..1000 + K as u32).collect();
    for &i in &delete_ids {
        let key = format!("{IDX}:{i}");
        let r = c.cmd(&[b"DEL", key.as_bytes()]);
        assert_eq!(r, V::Int(1), "DEL {key} failed: {r:?}");
    }

    drop(c);
    let mut guard = guard;
    sigkill(&mut guard.child);
    wait_for_port_down(port);

    // -- Restart --------------------------------------------------------
    let guard2 = start_moon_alive(spawn_moon_aof, port, &dir);
    let mut c2 = wait_ready(port);

    let (_loaded, _verified, re_indexed, tombstoned) =
        wait_for_recovery_counters(&dir, IDX, Duration::from_secs(10));
    assert!(
        re_indexed >= K,
        "expected >= {K} re-indexed (the updated keys), got {re_indexed}"
    );
    assert!(
        tombstoned >= K,
        "expected >= {K} tombstoned (the deleted keys), got {tombstoned}"
    );

    // Updated keys: querying with the NEW vector must return the updated
    // key as top-1 (proves the new content survived, not the stale one).
    for &i in &update_ids {
        let new_blob = simple_vec_bytes(i + UPDATE_SEED_OFFSET);
        let results = search_keys(&mut c2, IDX, 1, &new_blob);
        let want_key = format!("{IDX}:{i}");
        assert_eq!(
            results.first().map(String::as_str),
            Some(want_key.as_str()),
            "updated key {want_key}: KNN on its NEW vector must return itself top-1, got {results:?}"
        );
    }

    // Deleted keys must never resurface, even querying at their own old
    // (tombstoned) position.
    for &i in &delete_ids {
        let old_blob = simple_vec_bytes(i);
        let results = search_keys(&mut c2, IDX, 10, &old_blob);
        let dead_key = format!("{IDX}:{i}");
        assert_absent(&results, &dead_key, "S2 deleted key across crash");
    }

    // NOTE: FT.INFO `num_docs` is NOT asserted to equal N-K here. Once a key
    // is tombstoned against an already-compacted (Arc'd) immutable segment,
    // `ImmutableSegment::mark_deleted_by_key_hash` (the "steady-state"
    // tombstone path — see its doc comment) deliberately does NOT decrement
    // that segment's cached `live_count` (a documented prototype
    // limitation): it only inserts into a `tombstoned_keys` set consulted at
    // SEARCH time (`is_live_bfs`). `num_docs` sums `live_count()`, so it can
    // over-report after a delete against a pre-existing segment — this is
    // true on a live (non-crash) server too, not something B3/B4 introduced
    // or is expected to fix. The correctness contract that actually matters
    // (deleted keys never resurface in KNN) is proven by the search loop
    // above; `num_docs` only regains accuracy after the next compact/merge
    // rebuilds `live_count` from scratch.
    let num_docs = ft_info_num_docs(&mut c2, IDX);
    assert!(
        num_docs >= (N - K) as i64,
        "num_docs ({num_docs}) must never UNDER-report below the true live count \
         (N-K={})",
        N - K
    );

    drop(c2);
    drop(guard2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// S3: orphan sweep
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn s3_orphan_files_swept_on_boot() {
    const N: usize = 400;
    const IDX: &str = "s3";

    let port = unique_port();
    let dir = unique_dir("s3");
    let vdir = vector_persist_dir(&dir);
    let idx_dir = manifest::index_persist_dir(&vdir, IDX.as_bytes());

    let guard = spawn_moon_aof(port, &dir);
    let mut c = wait_ready(port);

    ft_create(&mut c, IDX, DIM, 100, None);
    let ids: Vec<u32> = (0..N as u32).collect();
    hset_batch(&mut c, &format!("{IDX}:"), &ids, simple_vec_bytes);
    ft_compact(&mut c, IDX);
    wait_for_manifest_min_segments(&idx_dir, 1, Duration::from_secs(10));

    drop(c);
    let mut guard = guard;
    sigkill(&mut guard.child);
    wait_for_port_down(port);

    // Inject orphans while the server is DOWN: an unreferenced staging dir,
    // an unreferenced segment dir, and an unreferenced keymap file.
    let staging_dir = idx_dir.join("staging-999");
    let segment_dir = idx_dir.join("segment-998");
    let keymap_file = idx_dir.join("keymap-999.bin");
    std::fs::create_dir_all(&staging_dir).expect("create fake staging dir");
    std::fs::write(staging_dir.join("meta.json"), b"{}").expect("write fake staging file");
    std::fs::create_dir_all(&segment_dir).expect("create fake segment dir");
    std::fs::write(segment_dir.join("meta.json"), b"{}").expect("write fake segment file");
    std::fs::write(&keymap_file, b"not a real keymap").expect("write fake keymap");

    assert!(staging_dir.exists() && segment_dir.exists() && keymap_file.exists());

    // -- Restart: orphan sweep runs in RecoveryState::finish() ------------
    let guard2 = start_moon_alive(spawn_moon_aof, port, &dir);
    let mut c2 = wait_ready(port);

    // Bounded poll: sweep runs synchronously during startup (before accept),
    // but poll defensively rather than assume zero latency.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if !staging_dir.exists() && !segment_dir.exists() && !keymap_file.exists() {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "orphan sweep did not remove all injected orphans within 10s: \
                 staging_exists={} segment_exists={} keymap_exists={}",
                staging_dir.exists(),
                segment_dir.exists(),
                keymap_file.exists()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Index still answers correctly post-sweep.
    let probe = simple_vec_bytes(7);
    let results = search_keys(&mut c2, IDX, 1, &probe);
    assert_eq!(
        results.first().map(String::as_str),
        Some(format!("{IDX}:7").as_str()),
        "index must still search correctly after orphan sweep"
    );
    let num_docs = ft_info_num_docs(&mut c2, IDX);
    assert_eq!(num_docs, N as i64, "num_docs unaffected by orphan sweep");

    drop(c2);
    drop(guard2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// S4: collection_id pin survives a post-recovery compact+merge cycle
// ---------------------------------------------------------------------------

fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn brute_force_topk(dataset: &[(String, Vec<f32>)], query: &[f32], k: usize) -> Vec<String> {
    let mut scored: Vec<(f32, &str)> = dataset
        .iter()
        .map(|(id, v)| (l2_sq(v, query), id.as_str()))
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    scored
        .into_iter()
        .take(k)
        .map(|(_, id)| id.to_string())
        .collect()
}

fn recall_at_k(ground_truth: &[String], approx: &[String], k: usize) -> f32 {
    let gt: HashSet<&str> = ground_truth.iter().take(k).map(String::as_str).collect();
    let ap: HashSet<&str> = approx.iter().take(k).map(String::as_str).collect();
    if gt.is_empty() {
        return 1.0;
    }
    gt.intersection(&ap).count() as f32 / gt.len() as f32
}

/// Generate `n_clusters * per_cluster` clustered vectors: cluster centers
/// are random directions in R^DIM, members are the center plus small
/// Gaussian noise. Low dimensionality (DIM=8) keeps this a meaningful
/// recall test (concentration-of-distance is a high-dimension problem —
/// see the repo gotcha on misleading random-Gaussian recall at high dims).
fn clustered_dataset(
    seed: u64,
    n_clusters: usize,
    per_cluster: usize,
    id_start: u32,
    prefix: &str,
) -> Vec<(String, Vec<f32>)> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::with_capacity(n_clusters * per_cluster);
    let mut next_id = id_start;
    for _ in 0..n_clusters {
        // Scale cluster centers up (x4) so clusters are well-separated in
        // R^DIM, and give members noise (0.3) that comfortably EXCEEDS the
        // per-vector affine SQ8 quantization step (~range/255 ≈ 0.03 for
        // values spanning roughly ±4). Noise at quant-step magnitude would
        // collapse same-cluster members onto near-identical codes, making
        // the rank-10 boundary an arbitrary tie for both this test's
        // client-side recall assert and the merge's internal recall gate
        // (VACUUM VECTOR / force_merge_index, fixed tolerance 0.90 — see
        // src/vector/store.rs::force_merge_index). Noise ≫ quant step keeps
        // the top-10 unambiguous (intra-cluster spread ~0.3·√(2·DIM) ≈ 2.4
        // vs inter-cluster separation ~4·√(2·DIM) ≈ 32 — still clearly
        // clustered). NOTE: an earlier constant 0.899996-vs-0.90 gate
        // failure here was NOT data ties — it was a self-exclusion
        // asymmetry in verify_merge_recall (GT excluded the query point,
        // HNSW included it, capping recall at (k-1)/k = 0.90 exactly);
        // fixed in src/vector/segment/compaction.rs alongside this test.
        let center: Vec<f32> = random_vec(&mut rng, DIM)
            .into_iter()
            .map(|x| x * 4.0)
            .collect();
        for _ in 0..per_cluster {
            let mut v = center.clone();
            for x in v.iter_mut() {
                *x += 0.3 * rng.randn();
            }
            out.push((format!("{prefix}{next_id}"), v));
            next_id += 1;
        }
    }
    out
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn s4_collection_id_pin_survives_post_recovery_merge() {
    const IDX: &str = "s4";
    const N_CLUSTERS: usize = 20;
    const PER_CLUSTER: usize = 25; // 500 pre-crash + 500 post-recovery = 1000 total

    let port = unique_port();
    let dir = unique_dir("s4");
    let vdir = vector_persist_dir(&dir);
    let idx_dir = manifest::index_persist_dir(&vdir, IDX.as_bytes());

    let guard = spawn_moon_aof(port, &dir);
    let mut c = wait_ready(port);

    ft_create_ex(&mut c, IDX, DIM, 100, Some(128), Some(32), Some(400));

    let prefix = format!("{IDX}:");
    let batch1 = clustered_dataset(0xC0FFEE_u64, N_CLUSTERS, PER_CLUSTER, 0, &prefix);
    let cmds: Vec<Vec<Vec<u8>>> = batch1
        .iter()
        .map(|(key, v)| {
            vec![
                b"HSET".to_vec(),
                key.clone().into_bytes(),
                b"vec".to_vec(),
                f32s_to_le_bytes(v),
            ]
        })
        .collect();
    for r in c.pipeline(&cmds) {
        assert!(matches!(r, V::Int(_)), "S4 batch1 HSET failed: {r:?}");
    }
    ft_compact(&mut c, IDX);
    wait_for_manifest_min_segments(&idx_dir, 1, Duration::from_secs(10));

    drop(c);
    let mut guard = guard;
    sigkill(&mut guard.child);
    wait_for_port_down(port);

    // -- Restart: segment A loads with its pinned collection_id -----------
    let guard2 = start_moon_alive(spawn_moon_aof, port, &dir);
    let mut c2 = wait_ready(port);
    wait_for_recovery_counters(&dir, IDX, Duration::from_secs(10));

    // Insert a SECOND batch of NEW clustered vectors post-recovery, compact
    // it into a new segment B — segment B is built under the RECOVERED
    // index's pinned collection_id (the QJL rotation seed). If B3 mis-pinned
    // it, segment A and segment B use incompatible rotations/codebooks and a
    // merge (which stitches graphs over the raw codes, never re-quantizing)
    // produces garbage distances for one half of the data — visible as a
    // recall collapse below.
    let batch2 = clustered_dataset(
        0xC0FFEE_u64 ^ 0xA5A5_A5A5,
        N_CLUSTERS,
        PER_CLUSTER,
        (N_CLUSTERS * PER_CLUSTER) as u32,
        &prefix,
    );
    let cmds2: Vec<Vec<Vec<u8>>> = batch2
        .iter()
        .map(|(key, v)| {
            vec![
                b"HSET".to_vec(),
                key.clone().into_bytes(),
                b"vec".to_vec(),
                f32s_to_le_bytes(v),
            ]
        })
        .collect();
    for r in c2.pipeline(&cmds2) {
        assert!(matches!(r, V::Int(_)), "S4 batch2 HSET failed: {r:?}");
    }
    ft_compact(&mut c2, IDX);
    wait_for_manifest_min_segments(&idx_dir, 2, Duration::from_secs(10));

    // Force the GraphUnion merge (VACUUM VECTOR merges whenever segment
    // count >= 2, even below the auto-merge trigger threshold — see
    // src/command/server_admin.rs's `seg_count < 2` gate).
    let vacuum_reply = vacuum_vector(&mut c2, IDX);
    assert!(
        vacuum_reply.starts_with("OK") || vacuum_reply.starts_with("Merged"),
        "VACUUM VECTOR {IDX} unexpected reply: {vacuum_reply}"
    );
    // After a successful merge, exactly 1 immutable segment should remain.
    // The manifest rewrite is asynchronous (persist_hook_after_install
    // schedules a SnapshotPool job after the segment-list swap), so poll
    // until the manifest CONVERGES to 1 segment — a min>=1 wait would return
    // instantly on the stale pre-merge manifest ([seg_a, seg_b]).
    let post_merge_manifest =
        wait_for_manifest_exact_segments(&idx_dir, 1, Duration::from_secs(10));
    assert_eq!(
        post_merge_manifest.segment_ids.len(),
        1,
        "expected exactly 1 segment after GraphUnion merge, got {:?}",
        post_merge_manifest.segment_ids
    );

    // Recall check: brute-force ground truth over ALL vectors (both
    // batches) computed client-side, compared against FT.SEARCH KNN=10 for
    // one query per cluster (batch1's cluster centers — held-in points).
    let mut dataset = batch1.clone();
    dataset.extend(batch2.clone());

    let mut recalls = Vec::with_capacity(N_CLUSTERS * 2);
    for (id, vec) in batch1.iter().step_by(PER_CLUSTER) {
        let _ = id;
        let approx = search_keys(&mut c2, IDX, 10, &f32s_to_le_bytes(vec));
        let gt = brute_force_topk(&dataset, vec, 10);
        recalls.push(recall_at_k(&gt, &approx, 10));
    }
    for (id, vec) in batch2.iter().step_by(PER_CLUSTER) {
        let _ = id;
        let approx = search_keys(&mut c2, IDX, 10, &f32s_to_le_bytes(vec));
        let gt = brute_force_topk(&dataset, vec, 10);
        recalls.push(recall_at_k(&gt, &approx, 10));
    }
    let mean_recall = recalls.iter().sum::<f32>() / recalls.len() as f32;
    assert!(
        mean_recall >= 0.9,
        "S4 collection_id-pin regression: mean recall@10 = {mean_recall:.4} across {} queries \
         (floor 0.90) — a mis-pinned collection_id/QJL seed on the post-recovery segment would \
         corrupt distances for the merged graph and collapse this number. Per-query: {recalls:?}",
        recalls.len()
    );

    let num_docs = ft_info_num_docs(&mut c2, IDX);
    assert_eq!(
        num_docs,
        dataset.len() as i64,
        "num_docs must equal total inserted vectors after merge"
    );

    drop(c2);
    drop(guard2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// S5: no-persist-dir regression guard
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn s5_no_persist_dir_regression_guard() {
    const N: usize = 50;
    const IDX: &str = "s5";

    let port = unique_port();
    let dir = unique_dir("s5");

    let guard = spawn_moon_no_persist(port, &dir);
    let mut c = wait_ready(port);

    ft_create(&mut c, IDX, DIM, 100, None);
    let ids: Vec<u32> = (0..N as u32).collect();
    hset_batch(&mut c, &format!("{IDX}:"), &ids, simple_vec_bytes);
    ft_compact(&mut c, IDX);

    // No persistence configured -> no idx-* dirs, no sidecar meta file,
    // anywhere under --dir.
    let idx_dirs = walk_matching(&dir, &|n| n.starts_with("idx-"));
    assert!(
        idx_dirs.is_empty(),
        "no vector_persist_dir configured, but found idx-* dirs: {idx_dirs:?}"
    );
    let sidecars = walk_matching(&dir, &|n| n == "vector-indexes.meta");
    assert!(
        sidecars.is_empty(),
        "no vector_persist_dir configured, but found a sidecar file: {sidecars:?}"
    );

    drop(c);
    let mut guard = guard;
    sigkill(&mut guard.child);
    wait_for_port_down(port);

    // -- Restart ----------------------------------------------------------
    let guard2 = start_moon_alive(spawn_moon_no_persist, port, &dir);
    let mut c2 = wait_ready(port);

    // Server is alive and responsive (no crash on empty/absent recovery
    // state).
    assert_eq!(c2.cmd(&[b"PING"]), V::Simple("PONG".into()));

    // Index definitions are gone entirely (never persisted) — FT._LIST must
    // not report the old index name.
    let indexes = ft_list(&mut c2);
    assert!(
        !indexes.iter().any(|n| n == IDX),
        "index {IDX} must NOT survive a restart with no persistence configured, \
         got FT._LIST = {indexes:?}"
    );

    // FT.SEARCH against the now-nonexistent index must fail cleanly, not
    // silently return stale (impossible, since recreation would require the
    // sidecar) or crash.
    let r = c2.cmd(&[
        b"FT.SEARCH",
        IDX.as_bytes(),
        b"*=>[KNN 1 @vec $B]",
        b"PARAMS",
        b"2",
        b"B",
        &simple_vec_bytes(0),
        b"DIALECT",
        b"2",
    ]);
    assert!(
        matches!(r, V::Err(_)),
        "FT.SEARCH on a never-persisted index after restart must error, got {r:?}"
    );

    // Still no idx-*/sidecar files anywhere (recovery must not have created
    // any either).
    let idx_dirs_post = walk_matching(&dir, &|n| n.starts_with("idx-"));
    assert!(
        idx_dirs_post.is_empty(),
        "post-restart, found unexpected idx-* dirs: {idx_dirs_post:?}"
    );
    let sidecars_post = walk_matching(&dir, &|n| n == "vector-indexes.meta");
    assert!(
        sidecars_post.is_empty(),
        "post-restart, found unexpected sidecar file: {sidecars_post:?}"
    );

    drop(c2);
    drop(guard2);
    let _ = std::fs::remove_dir_all(&dir);
}
