//! Every live indexed HASH must still be searchable after a restart, whatever
//! shape it is stored in (moon#1074).
//!
//! At boot, vector recovery walks the keyspace to decide which recovered
//! documents are still current. A key the walk does not observe is taken to
//! have been deleted while the server was down, and `RecoveryState::finish`
//! tombstones its document. The walk read only the HOT table, and within it
//! only two of the three hash encodings. Two kinds of live key were therefore
//! tombstoned on every restart:
//!
//! * a hash spilled to the KV cold tier by `allkeys-lru` eviction (with
//!   disk-offload on). `EXISTS` and `HGETALL` still answer it through the cold
//!   index; `FT.SEARCH` stopped returning it.
//! * a hash with a per-field TTL (after `HEXPIRE`), which is stored as a
//!   distinct variant the walk skipped.
//!
//! A cold document that was still in the mutable segment at shutdown has no
//! durable vector at all: the walk is the only thing that can re-index it.
//!
//! Every assertion is on what a client sees: `FT.INFO num_docs`, `FT.SEARCH`
//! top-1 for each document's own vector, and `EXISTS`.
//!
//! Run with (pin the binary you just built):
//!   MOON_BIN=target/release-fast/moon cargo test --profile release-fast \
//!     --test vector_cold_hash_rescan

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const DIM: usize = 16;
/// Documents written before the filler wave, so LRU picks them first.
const DOCS: usize = 300;
const MAXMEMORY_BYTES: u64 = 512 * 1024;
const FILLER: usize = 1500;
const FILLER_LEN: usize = 2000;

// ---------------------------------------------------------------------------
// Vectors: printable-ASCII bytes, so they travel through the shared `Conn`
// (&str) unchanged. Each f32 is [ascii, ascii, ascii, 0x3F..=0x41] -- a finite
// value between ~0.5 and ~12, so L2 distances never overflow.
// ---------------------------------------------------------------------------

fn vector(seed: u64) -> String {
    let mut out = String::with_capacity(DIM * 4);
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    for _ in 0..DIM {
        for _ in 0..3 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            out.push(char::from(0x21 + (s % 94) as u8));
        }
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        out.push(char::from(0x3F + (s % 3) as u8));
    }
    out
}

fn doc_vector(i: usize) -> String {
    vector(i as u64 + 1)
}

// ---------------------------------------------------------------------------
// Minimal RESP reader over the framed raw reply `Conn` returns.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

fn parse(raw: &str) -> V {
    fn one(s: &str, i: &mut usize) -> V {
        let end = *i + s[*i..].find("\r\n").unwrap();
        let (tag, line) = (&s[*i..*i + 1], &s[*i + 1..end]);
        *i = end + 2;
        match tag {
            "+" => V::Str(line.to_owned()),
            "-" => panic!("server error reply: {line}"),
            ":" => V::Int(line.parse().unwrap()),
            "$" => {
                let n: i64 = line.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                let v = s[*i..*i + n as usize].to_owned();
                *i += n as usize + 2;
                V::Str(v)
            }
            "*" => {
                let n: i64 = line.parse().unwrap();
                V::Arr((0..n.max(0)).map(|_| one(s, i)).collect())
            }
            other => panic!("unexpected RESP tag {other:?} in {s:?}"),
        }
    }
    let mut i = 0;
    one(raw, &mut i)
}

// ---------------------------------------------------------------------------
// Server lifecycle.
// ---------------------------------------------------------------------------

struct Server {
    guard: ServerGuard,
    port: u16,
}

/// `evict` caps memory so `allkeys-lru` spills documents to the cold tier.
fn start(dir: &Path, evict: bool) -> Server {
    let maxmemory = if evict {
        MAXMEMORY_BYTES.to_string()
    } else {
        "0".to_owned()
    };
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                // Spill needs a durability backstop; without one an evicting
                // policy plain-drops its victims instead of tiering them.
                "--appendonly",
                "yes",
                "--disk-offload",
                "enable",
                "--maxmemory",
                &maxmemory,
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .env("RUST_LOG", "moon=info")
            .stdout(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join("moon.log"))
                    .map(Stdio::from)
                    .unwrap_or_else(|_| Stdio::null()),
            )
            .stderr(server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    await_loaded(port);
    Server { guard, port }
}

/// Block until the server has finished loading (index recovery runs after
/// the listener accepts; until then keyspace and FT.* commands get -LOADING).
fn await_loaded(port: u16) {
    const DEADLINE: Duration = Duration::from_secs(180);
    let start = Instant::now();
    loop {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "persistence"]);
        let read = c.send(&["EXISTS", "moon1074:probe"]);
        if info.contains("loading:0\r\n") && read.starts_with(':') {
            return;
        }
        assert!(
            start.elapsed() < DEADLINE,
            "server still loading after {DEADLINE:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Server {
    fn conn(&self) -> Conn {
        Conn::open(self.port)
    }

    fn stop(mut self, crash: bool) {
        if crash {
            // Let the everysec AOF fsync cover the last write before the kill.
            std::thread::sleep(Duration::from_millis(1500));
            self.guard.kill_now();
            return;
        }
        use std::io::Write as _;
        let mut c = self.conn();
        let _ = c.sock.write_all(&common::encode(&["SHUTDOWN"]));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.guard.as_mut().try_wait().unwrap().is_some() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server did not exit within 30s of SHUTDOWN"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn info_int(c: &mut Conn, field: &str) -> i64 {
    let V::Arr(items) = parse(&c.send(&["FT.INFO", "idx"])) else {
        panic!("FT.INFO is not an array");
    };
    for kv in items.chunks(2) {
        if let [V::Str(k), V::Int(n)] = kv
            && k == field
        {
            return *n;
        }
    }
    panic!("FT.INFO has no integer field {field}");
}

fn top1(c: &mut Conn, blob: &str) -> Option<String> {
    let V::Arr(items) = parse(&c.send(&[
        "FT.SEARCH",
        "idx",
        "*=>[KNN 1 @vec $q]",
        "PARAMS",
        "2",
        "q",
        blob,
        "DIALECT",
        "2",
    ])) else {
        panic!("FT.SEARCH is not an array");
    };
    items.iter().skip(1).find_map(|v| match v {
        V::Str(k) => Some(k.clone()),
        _ => None,
    })
}

/// The documents among `docs` whose own vector does NOT return them top-1.
fn unmatched(c: &mut Conn, docs: &[usize]) -> Vec<usize> {
    docs.iter()
        .copied()
        .filter(|&i| top1(c, &doc_vector(i)).as_deref() != Some(&format!("doc:{i}")))
        .collect()
}

fn existing(c: &mut Conn) -> Vec<usize> {
    (0..DOCS)
        .filter(|i| c.send(&["EXISTS", &format!("doc:{i}")]) == ":1\r\n")
        .collect()
}

fn wait_for(what: &str, secs: u64, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn index_dir(dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(dir.join("shard-0"))
        .ok()?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("idx-"))
        })
}

fn hot_segment_ids(dir: &Path) -> Vec<u64> {
    index_dir(dir)
        .and_then(|d| moon::vector::persistence::manifest::read_manifest_tolerant(&d))
        .map(|m| m.segment_ids)
        .unwrap_or_default()
}

fn fresh_dir(tag: &str) -> PathBuf {
    let d = common::unique_test_dir(&format!("moon-1074-{tag}"));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn log_len(dir: &Path) -> usize {
    std::fs::metadata(dir.join("moon.log")).map_or(0, |m| m.len() as usize)
}

/// How many HOT keys the boot whose log starts at byte `from` found under an
/// index prefix (0 when the line is absent). Cold-tier keys are not in this
/// count on any version, so it tells how many documents were hot at boot.
fn boot_hot_scanned(dir: &Path, from: usize) -> usize {
    let text = std::fs::read(dir.join("moon.log")).unwrap_or_default();
    let text = String::from_utf8_lossy(text.get(from..).unwrap_or_default());
    const LINE: &str = "recovery scanned db 0: ";
    text.lines()
        .rev()
        .find_map(|l| {
            let at = l.find(LINE)?;
            l[at + LINE.len()..].split_whitespace().next()?.parse().ok()
        })
        .unwrap_or(0)
}

fn create_index(c: &mut Conn) {
    let r = c.send(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        &DIM.to_string(),
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert_eq!(r, "+OK\r\n", "FT.CREATE: {r}");
}

/// Index `doc:0..DOCS`; with `compact`, into one HOT segment that Stack B has
/// persisted (so the keymap names every document), otherwise left in the
/// mutable segment (no durable vector at all).
fn seed(srv: &Server, dir: &Path, compact: bool) {
    let mut c = srv.conn();
    create_index(&mut c);
    for chunk in (0..DOCS).collect::<Vec<_>>().chunks(100) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("doc:{i}")).collect();
        let vecs: Vec<String> = chunk.iter().map(|&i| doc_vector(i)).collect();
        let cmds: Vec<[&str; 6]> = keys
            .iter()
            .zip(&vecs)
            .map(|(k, v)| ["HSET", k.as_str(), "vec", v.as_str(), "tag", "t"])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
        let replies = c.pipeline(&refs);
        assert!(!replies.contains('-'), "HSET failed: {replies}");
    }
    if compact {
        assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
        wait_for("Stack B to persist the HOT segment", 60, || {
            !hot_segment_ids(dir).is_empty()
        });
        // Let the keymap snapshot that follows the install land.
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Push the documents out of RAM with a filler wave.
fn evict_docs(srv: &Server) {
    let mut c = srv.conn();
    let value = "f".repeat(FILLER_LEN);
    for chunk in (0..FILLER).collect::<Vec<_>>().chunks(50) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("fill:{i}")).collect();
        let cmds: Vec<[&str; 3]> = keys
            .iter()
            .map(|k| ["SET", k.as_str(), value.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
        let _ = c.pipeline(&refs);
    }
    // Eviction and spill hand off to a background thread.
    std::thread::sleep(Duration::from_millis(1500));
}

/// Seed, evict, restart; then every document that still exists must be
/// searchable, and `num_docs` must count exactly those.
fn cold_documents_survive_a_restart(tag: &str, compact: bool, crash: bool) {
    let dir = fresh_dir(tag);
    let srv = start(&dir, true);
    seed(&srv, &dir, compact);
    evict_docs(&srv);
    let mut c = srv.conn();
    let before = existing(&mut c);
    assert!(
        !before.is_empty(),
        "every document was dropped by eviction; nothing to check"
    );
    let stale_before = unmatched(&mut c, &before);
    assert!(
        stale_before.is_empty(),
        "before the restart, {} surviving document(s) already failed to match \
         their own vector: {:?}",
        stale_before.len(),
        &stale_before[..stale_before.len().min(10)]
    );
    drop(c);
    srv.stop(crash);

    let boot_at = log_len(&dir);
    let srv = start(&dir, true);
    let mut c = srv.conn();
    // FT.* first: a keyspace read of a cold key may promote it into RAM.
    let num_docs = info_int(&mut c, "num_docs");
    let missing = unmatched(&mut c, &before);
    let after = existing(&mut c);
    let hot_at_boot = boot_hot_scanned(&dir, boot_at);

    // Instrument: some surviving documents must have been cold at boot, or
    // this run exercised nothing.
    assert!(
        hot_at_boot < after.len(),
        "instrument: every surviving document was hot at boot \
         ({hot_at_boot} hot, {} exist); no cold key was exercised",
        after.len()
    );
    assert_eq!(
        after, before,
        "a document that existed before the restart is gone after it"
    );
    assert!(
        missing.is_empty(),
        "{} of {} surviving document(s) no longer match their own vector \
         after the restart ({hot_at_boot} were hot at boot): first few {:?}",
        missing.len(),
        before.len(),
        &missing[..missing.len().min(10)]
    );
    assert_eq!(
        num_docs,
        before.len() as i64,
        "num_docs after the restart must count every surviving document"
    );
    srv.stop(false);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cold_spilled_document_survives_a_clean_restart() {
    cold_documents_survive_a_restart("clean", true, false);
}

#[test]
fn cold_spilled_document_survives_a_kill9() {
    cold_documents_survive_a_restart("kill9", true, true);
}

/// Not compacted: the documents have no durable vector, so recovery has
/// nothing to tombstone. The walk is the only thing that can re-index them.
#[test]
fn cold_spilled_document_from_the_mutable_segment_is_reindexed() {
    cold_documents_survive_a_restart("mutable", false, false);
}

/// A hash with a per-field TTL is a distinct stored variant; the walk must
/// still read it as a hash.
#[test]
fn document_with_a_field_ttl_survives_a_restart() {
    const TARGET: usize = 7;
    let dir = fresh_dir("httl");
    let srv = start(&dir, false);
    seed(&srv, &dir, true);
    let mut c = srv.conn();
    let key = format!("doc:{TARGET}");
    let r = c.send(&["HEXPIRE", &key, "100000", "FIELDS", "1", "tag"]);
    assert_eq!(r, "*1\r\n:1\r\n", "HEXPIRE: {r}");
    assert_eq!(top1(&mut c, &doc_vector(TARGET)), Some(key.clone()));
    drop(c);
    srv.stop(false);

    let srv = start(&dir, false);
    let mut c = srv.conn();
    let ttl = c.send(&["HTTL", &key, "FIELDS", "1", "tag"]);
    assert!(
        ttl.starts_with("*1\r\n:") && !ttl.starts_with("*1\r\n:-"),
        "instrument: the field TTL did not survive the restart: {ttl}"
    );
    assert_eq!(
        top1(&mut c, &doc_vector(TARGET)),
        Some(key),
        "a document with a field TTL no longer matches its own vector after the restart"
    );
    assert_eq!(info_int(&mut c, "num_docs"), DOCS as i64);
    srv.stop(false);
    let _ = std::fs::remove_dir_all(&dir);
}
