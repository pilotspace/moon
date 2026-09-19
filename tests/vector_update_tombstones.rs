//! A re-written or deleted vector document must stop matching in EVERY tier,
//! at runtime and across a restart (moon#1066, moon#1073).
//!
//! An `HSET` that re-writes an indexed key appends the new copy to the mutable
//! segment and must tombstone the old copy wherever it lives. A `DEL` must
//! tombstone the only copy. Two gaps let a dead copy keep matching:
//!
//! * **runtime, WARM/COLD (moon#1066).** The update path tombstoned the old
//!   copy in the mutable and HOT (immutable) segments only. A copy held by a
//!   WARM segment, or by a COLD stub, stayed live: `num_docs` counted the key
//!   twice and a KNN for the OVERWRITTEN vector still returned the key.
//! * **restart, HOT (moon#1073).** A HOT segment's steady-state tombstones are
//!   in memory only; Stack B writes a segment once and never rewrites it. The
//!   durable record of which copy is current is the keymap (key_hash ->
//!   global_id). Recovery admitted a keymap entry when ANY loaded segment had a
//!   live row for the key_hash, whatever its global_id, so the dead copy came
//!   back live beside (or instead of) the current one.
//!
//! Every test asserts on what a client sees: `FT.INFO num_docs`, and which key
//! `FT.SEARCH` returns top-1 for the current and the overwritten vector.
//!
//! Run with (pin the binary you just built):
//!   MOON_BIN=target/release-fast/moon cargo test --profile release-fast \
//!     --test vector_update_tombstones

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const DIM: usize = 16;
const N: usize = 1000;
/// The key that is re-written (or deleted) after its first copy left the
/// mutable segment.
const TARGET: usize = 7;

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

fn original(i: usize) -> String {
    vector(i as u64 + 1)
}

fn rewritten() -> String {
    vector(1_000_003)
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

/// `warm_after_secs` ages a HOT segment into WARM; `idle_secs` (0 = off) idles
/// it straight into COLD.
fn start(dir: &Path, warm_after_secs: u64, idle_secs: u64) -> Server {
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--appendonly",
                "yes",
                "--segment-warm-after",
                &warm_after_secs.to_string(),
                "--engine-offload-idle-secs",
                &idle_secs.to_string(),
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

/// Block until the server has finished loading (vector recovery runs after
/// the listener accepts; until then FT.* is refused with -LOADING).
fn await_loaded(port: u16) {
    const DEADLINE: Duration = Duration::from_secs(180);
    let start = Instant::now();
    loop {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "persistence"]);
        let read = c.send(&["EXISTS", "moon1066:probe"]);
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

    /// Crash: SIGKILL, no shutdown path at all.
    fn kill9(mut self) {
        self.guard.kill_now();
    }

    /// Clean shutdown: SHUTDOWN, then wait (bounded) for the process to exit.
    fn shutdown(mut self) {
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

    fn stop(self, crash: bool) {
        if crash {
            // Let the everysec AOF fsync cover the last write before the kill.
            std::thread::sleep(Duration::from_millis(1500));
            self.kill9();
        } else {
            self.shutdown();
        }
    }
}

// ---------------------------------------------------------------------------
// FT.* helpers.
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

/// Top-3 keys for `blob`, nearest first -- every returned key, including a
/// synthetic `vec:<id>` for a row whose key the index no longer knows.
fn knn(c: &mut Conn, blob: &str) -> Vec<String> {
    let V::Arr(items) = parse(&c.send(&[
        "FT.SEARCH",
        "idx",
        "*=>[KNN 3 @vec $q]",
        "PARAMS",
        "2",
        "q",
        blob,
        "DIALECT",
        "2",
    ])) else {
        panic!("FT.SEARCH is not an array");
    };
    items
        .iter()
        .skip(1)
        .filter_map(|v| match v {
            V::Str(k) => Some(k.clone()),
            _ => None,
        })
        .collect()
}

fn wait_for(what: &str, secs: u64, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The index's Stack-B directory (`shard-0/idx-<hex>`).
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

/// Stack B's persisted HOT segment ids.
fn hot_segment_ids(dir: &Path) -> Vec<u64> {
    index_dir(dir)
        .and_then(|d| moon::vector::persistence::manifest::read_manifest_tolerant(&d))
        .map(|m| m.segment_ids)
        .unwrap_or_default()
}

fn fresh_dir(tag: &str) -> PathBuf {
    let d = common::unique_test_dir(&format!("moon-1066-{tag}"));
    std::fs::create_dir_all(&d).unwrap();
    d
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

fn hset_range(c: &mut Conn, range: std::ops::Range<usize>) {
    for chunk in range.collect::<Vec<_>>().chunks(100) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("doc:{i}")).collect();
        let vecs: Vec<String> = chunk.iter().map(|&i| original(i)).collect();
        let cmds: Vec<[&str; 4]> = keys
            .iter()
            .zip(&vecs)
            .map(|(k, v)| ["HSET", k.as_str(), "vec", v.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
        let replies = c.pipeline(&refs);
        assert!(!replies.contains('-'), "HSET failed: {replies}");
    }
}

/// Create the index, insert `doc:0..N`, and compact them into ONE HOT segment
/// that Stack B has persisted.
fn seed_hot(srv: &Server, dir: &Path) {
    let mut c = srv.conn();
    create_index(&mut c);
    hset_range(&mut c, 0..N);
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    wait_for("the HOT segment", 120, || {
        info_int(&mut c, "graph_segments") == 1
    });
    wait_for("Stack B to persist the HOT segment", 60, || {
        hot_segment_ids(dir).len() == 1
    });
}

fn rewrite_target(c: &mut Conn) {
    let r = c.send(&["HSET", &format!("doc:{TARGET}"), "vec", &rewritten()]);
    assert_eq!(r, ":0\r\n", "HSET of an existing field: {r}");
}

/// The re-written key answers to its CURRENT vector and never to its
/// overwritten one, every other sampled key still answers to its own, and
/// `num_docs` counts each live key once. Every check runs and every failure
/// is reported together, so a RED run names each symptom, not just the first.
fn check_serves_current(c: &mut Conn, when: &str, expect_docs: usize) -> Vec<String> {
    let target = format!("doc:{TARGET}");
    let mut failures: Vec<String> = Vec::new();
    let stale = knn(c, &original(TARGET));
    if stale.first() == Some(&target) {
        failures.push(format!(
            "the re-written key still matches its OVERWRITTEN vector: {stale:?}"
        ));
    }
    let current = knn(c, &rewritten());
    if current.first() != Some(&target) {
        failures.push(format!(
            "the re-written key does not match its CURRENT vector: {current:?}"
        ));
    }
    for i in (0..N).step_by(97).filter(|&i| i != TARGET) {
        let got = knn(c, &original(i));
        if got.first().map(String::as_str) != Some(format!("doc:{i}").as_str()) {
            failures.push(format!("doc:{i} is not served by its own vector: {got:?}"));
        }
    }
    let docs = info_int(c, "num_docs");
    if docs != expect_docs as i64 {
        failures.push(format!("num_docs = {docs}, expected {expect_docs}"));
    }
    failures
        .into_iter()
        .map(|f| format!("{when}: {f}"))
        .collect()
}

fn assert_serves_current(c: &mut Conn, when: &str, expect_docs: usize) {
    assert_no_failures(check_serves_current(c, when, expect_docs));
}

fn assert_no_failures(failures: Vec<String>) {
    assert!(failures.is_empty(), "\n  {}", failures.join("\n  "));
}

// ---------------------------------------------------------------------------
// moon#1066 -- runtime, no restart.
// ---------------------------------------------------------------------------

/// The old copy lives in a WARM segment when the key is re-written.
#[test]
fn rewrite_of_a_warm_key_tombstones_the_warm_copy() {
    let dir = fresh_dir("warm");
    let srv = start(&dir, 1, 0);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    wait_for("the segment to go WARM", 120, || {
        info_int(&mut c, "warm_segments") == 1
    });
    rewrite_target(&mut c);
    assert_serves_current(&mut c, "after re-writing a WARM key", N);
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The old copy lives in a COLD (unloaded) stub when the key is re-written:
/// the stub must queue the tombstone and the reload must apply it.
#[test]
fn rewrite_of_a_cold_key_tombstones_the_cold_copy() {
    let dir = fresh_dir("cold");
    let srv = start(&dir, 3600, 2);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    wait_for("the segment to go COLD", 120, || {
        info_int(&mut c, "unloaded_segments") == 1
    });
    rewrite_target(&mut c);
    // Counted from the stub, before any search reloads it.
    assert_eq!(
        info_int(&mut c, "num_docs"),
        N as i64,
        "while COLD: num_docs counts a dead copy of the re-written key"
    );
    assert_serves_current(&mut c, "after re-writing a COLD key", N);
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Baseline sibling: the old copy lives in a HOT segment.
#[test]
fn rewrite_of_a_hot_key_tombstones_the_hot_copy() {
    let dir = fresh_dir("hot");
    let srv = start(&dir, 3600, 0);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    rewrite_target(&mut c);
    assert_serves_current(&mut c, "after re-writing a HOT key", N);
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// moon#1073 -- restart, HOT.
// ---------------------------------------------------------------------------

/// The re-written key's new copy is compacted into a second HOT segment; both
/// segments and the keymap are persisted; then the server restarts.
fn rewrite_compacted_into_hot_then_restart(tag: &str, crash: bool) {
    let dir = fresh_dir(tag);
    let srv = start(&dir, 3600, 0);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    rewrite_target(&mut c);
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    wait_for("the second HOT segment", 120, || {
        info_int(&mut c, "graph_segments") == 2
    });
    wait_for("Stack B to persist the second HOT segment", 60, || {
        hot_segment_ids(&dir).len() == 2
    });
    let mut failures = check_serves_current(&mut c, "before the restart", N);
    srv.stop(crash);

    for boot in ["boot 1", "boot 2"] {
        let srv = start(&dir, 3600, 0);
        let mut c = srv.conn();
        failures.extend(check_serves_current(&mut c, boot, N));
        srv.stop(crash);
    }
    assert_no_failures(failures);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rewrite_compacted_into_a_second_hot_segment_survives_restart() {
    rewrite_compacted_into_hot_then_restart("hot2", false);
}

#[test]
fn rewrite_compacted_into_a_second_hot_segment_survives_kill9() {
    rewrite_compacted_into_hot_then_restart("hot2-kill9", true);
}

/// The worst shape: the keymap names the re-written copy while that copy is
/// still in the MUTABLE segment (a compaction of OTHER keys installed and
/// snapshotted while the re-write sat in the tail). The old HOT row then
/// vouched for the key under the NEW checksum, the rescan judged it unchanged,
/// and the key answered only to its overwritten vector.
#[test]
fn rewrite_left_in_mutable_when_the_keymap_was_snapshotted_survives_kill9() {
    let dir = fresh_dir("tail");
    let srv = start(&dir, 3600, 0);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    // N more keys: the N-th insert crosses the compact threshold (default
    // 1000) and freezes them for a background build; the re-write lands in
    // the tail AFTER the frozen prefix.
    hset_range(&mut c, N..2 * N);
    rewrite_target(&mut c);
    // A search polls the install; the install submits the snapshot.
    wait_for("the second HOT segment", 120, || {
        let _ = knn(&mut c, &original(0));
        info_int(&mut c, "graph_segments") == 2
    });
    wait_for("Stack B to persist the second HOT segment", 60, || {
        hot_segment_ids(&dir).len() == 2
    });
    let mut failures = check_serves_current(&mut c, "before the kill", 2 * N);
    srv.stop(true);

    for boot in ["boot 1", "boot 2"] {
        let srv = start(&dir, 3600, 0);
        let mut c = srv.conn();
        failures.extend(check_serves_current(&mut c, boot, 2 * N));
        srv.stop(true);
    }
    assert_no_failures(failures);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A DEL'd key's row stays in its HOT segment on disk; a later snapshot's
/// keymap no longer names the key. The row must not come back after a
/// restart -- not under its key, and not as a synthetic `vec:<id>`.
#[test]
fn deleted_hot_key_does_not_resurrect_after_restart() {
    let dir = fresh_dir("del");
    let srv = start(&dir, 3600, 0);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    assert_eq!(c.send(&["DEL", &format!("doc:{TARGET}")]), ":1\r\n");
    // A second install makes Stack B snapshot a keymap without the key.
    hset_range(&mut c, N..N + 1);
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    wait_for("Stack B to persist the second HOT segment", 120, || {
        hot_segment_ids(&dir).len() == 2
    });
    srv.shutdown();

    let srv = start(&dir, 3600, 0);
    let mut c = srv.conn();
    let got = knn(&mut c, &original(TARGET));
    let docs = info_int(&mut c, "num_docs");
    assert!(
        !got.first()
            .is_some_and(|k| k == &format!("doc:{TARGET}") || k.starts_with("vec:"))
            && docs == N as i64,
        "after restart: the deleted key's row matches again ({got:?}) or is counted \
         (num_docs = {docs}, expected {N})"
    );
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// num_docs: a tombstone is counted once, by the segment that holds the copy.
// ---------------------------------------------------------------------------

/// DEL of a key whose only copy is in a HOT segment.
#[test]
fn deleting_a_hot_key_is_counted_at_runtime() {
    let dir = fresh_dir("hotdel");
    let srv = start(&dir, 3600, 0);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    assert_eq!(c.send(&["DEL", &format!("doc:{TARGET}")]), ":1\r\n");
    assert_eq!(info_int(&mut c, "num_docs"), (N - 1) as i64, "num_docs");
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two segments in one tier; the key lives in the FIRST. A tombstone must be
/// counted only by the segment that holds the key, never by its sibling.
fn two_segments_then(tag: &str, warm_after: u64, idle: u64, tier_field: &str, rewrite: bool) {
    let dir = fresh_dir(tag);
    let srv = start(&dir, warm_after, idle);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    wait_for("the first segment to leave HOT", 120, || {
        info_int(&mut c, tier_field) == 1
    });
    hset_range(&mut c, N..2 * N);
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    wait_for("the second segment to leave HOT", 120, || {
        info_int(&mut c, tier_field) == 2
    });
    assert_eq!(info_int(&mut c, "num_docs"), (2 * N) as i64, "baseline");
    let (expect, what) = if rewrite {
        rewrite_target(&mut c);
        (2 * N, "re-write")
    } else {
        assert_eq!(c.send(&["DEL", &format!("doc:{TARGET}")]), ":1\r\n");
        (2 * N - 1, "DEL")
    };
    assert_eq!(
        info_int(&mut c, "num_docs"),
        expect as i64,
        "num_docs after a {what} of a key held by one of two {tier_field}"
    );
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_in_one_of_two_warm_segments_is_counted_once() {
    two_segments_then("warm2del", 1, 0, "warm_segments", false);
}

#[test]
fn delete_in_one_of_two_cold_segments_is_counted_once() {
    two_segments_then("cold2del", 3600, 2, "unloaded_segments", false);
}

#[test]
fn rewrite_in_one_of_two_warm_segments_is_counted_once() {
    two_segments_then("warm2rw", 1, 0, "warm_segments", true);
}

#[test]
fn rewrite_in_one_of_two_cold_segments_is_counted_once() {
    two_segments_then("cold2rw", 3600, 2, "unloaded_segments", true);
}

// ---------------------------------------------------------------------------
// A row that was already dead when its HOT segment was installed (the key was
// deleted or re-written while the background build ran) stays dead in WARM.
// ---------------------------------------------------------------------------

fn written_during_the_build_then_warm(tag: &str, rewrite: bool) {
    let dir = fresh_dir(tag);
    // WARM 3s after the build: long enough to observe the HOT segment first.
    let srv = start(&dir, 3, 0);
    let mut c = srv.conn();
    create_index(&mut c);
    hset_range(&mut c, 0..N - 1);
    // The N-th insert crosses the compact threshold (default 1000) and SUBMITS
    // a background build of doc:0..N; the write pipelined right behind it
    // lands while the worker builds, so the install reconciles it into the new
    // HOT segment as an install-time (mvcc delete_lsn) tombstone. (FT.COMPACT
    // cannot produce this: it drains synchronously.)
    let target = format!("doc:{TARGET}");
    let last_key = format!("doc:{}", N - 1);
    let last_vec = original(N - 1);
    let new_vec = rewritten();
    let hset_last: &[&str] = &["HSET", &last_key, "vec", &last_vec];
    let write: Vec<&str> = if rewrite {
        vec!["HSET", &target, "vec", &new_vec]
    } else {
        vec!["DEL", &target]
    };
    let replies = c.pipeline(&[hset_last, &write]);
    assert!(!replies.contains('-'), "pipeline: {replies}");
    wait_for("the background build to install", 60, || {
        let _ = knn(&mut c, &original(0));
        info_int(&mut c, "graph_segments") + info_int(&mut c, "warm_segments") >= 1
    });
    // Instrument: the write must have landed INSIDE the build window, or this
    // test exercises nothing. An install-time tombstone is the only kind a HOT
    // segment counted before this fix, so the HOT count proves the window.
    if info_int(&mut c, "graph_segments") == 1 {
        let hot_docs = info_int(&mut c, "num_docs");
        let expect = if rewrite { N } else { N - 1 };
        assert_eq!(
            hot_docs, expect as i64,
            "fixture: the write did not land inside the background build window"
        );
    }
    wait_for("the segment to go WARM", 120, || {
        info_int(&mut c, "warm_segments") >= 1
    });
    if rewrite {
        // The re-write sits in the mutable segment; keep it there.
        assert_serves_current(&mut c, "WARM after a re-write during the build", N);
    } else {
        let got = knn(&mut c, &original(TARGET));
        let docs = info_int(&mut c, "num_docs");
        assert!(
            !got.first()
                .is_some_and(|k| k == &target || k.starts_with("vec:"))
                && docs == (N - 1) as i64,
            "WARM after a DEL during the build: the deleted row matches again ({got:?}) or \
             is counted (num_docs = {docs}, expected {})",
            N - 1
        );
    }
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_during_the_build_stays_deleted_in_warm() {
    written_during_the_build_then_warm("buildwarm-del", false);
}

#[test]
fn rewrite_during_the_build_stays_superseded_in_warm() {
    written_during_the_build_then_warm("buildwarm-rw", true);
}

// ---------------------------------------------------------------------------
// A synchronous merge (`VACUUM VECTOR`, the autovacuum pass) folds HOT
// segments into one. It must carry the sources' steady-state tombstones.
// ---------------------------------------------------------------------------

/// Two HOT segments, the key's only copy in the first; DEL or re-write it,
/// then `VACUUM VECTOR` merges both segments synchronously.
fn two_hot_segments_then_vacuum(tag: &str, rewrite: bool) {
    let dir = fresh_dir(tag);
    let srv = start(&dir, 3600, 0);
    seed_hot(&srv, &dir);
    let mut c = srv.conn();
    hset_range(&mut c, N..2 * N);
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    wait_for("a second HOT segment", 120, || {
        info_int(&mut c, "graph_segments") == 2
    });
    let expect = if rewrite {
        rewrite_target(&mut c);
        2 * N
    } else {
        assert_eq!(c.send(&["DEL", &format!("doc:{TARGET}")]), ":1\r\n");
        2 * N - 1
    };
    // The merge runs synchronously and replies only when it is done: about
    // 0.3s in a release build and about 6s unoptimized on a fast host, which
    // is too close to the 20s default budget on a slower CI runner.
    let merged = c.send_within(
        &["VACUUM", "VECTOR", "idx"],
        std::time::Duration::from_secs(120),
    );
    assert!(
        merged.starts_with("+Merged 2 segments into 1"),
        "instrument: VACUUM VECTOR did not merge: {merged:?}"
    );
    let check = |c: &mut Conn, when: &str| {
        if rewrite {
            assert_serves_current(c, when, expect);
        } else {
            let got = knn(c, &original(TARGET));
            let docs = info_int(c, "num_docs");
            assert!(
                !got.first()
                    .is_some_and(|k| k == &format!("doc:{TARGET}") || k.starts_with("vec:"))
                    && docs == expect as i64,
                "{when}: the deleted key matches again ({got:?}) or is counted \
                 (num_docs = {docs}, expected {expect}); merge reply {merged:?}"
            );
        }
    };
    check(&mut c, "after VACUUM VECTOR");
    // The merged segment was written to disk before the tombstones were
    // replayed onto it; the keymap is what keeps the dead copy dead at boot.
    srv.shutdown();
    let srv = start(&dir, 3600, 0);
    let mut c = srv.conn();
    check(&mut c, "after VACUUM VECTOR and a restart");
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn vacuum_merge_keeps_a_deleted_key_deleted() {
    two_hot_segments_then_vacuum("vacuum-del", false);
}

#[test]
fn vacuum_merge_keeps_only_the_current_copy_of_a_rewritten_key() {
    two_hot_segments_then_vacuum("vacuum-rw", true);
}
