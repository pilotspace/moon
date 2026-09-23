//! A document written to a shard while that shard runs its boot index rescan
//! must stay searchable (moon#1124).
//!
//! At boot each shard snapshots its recovered documents, walks the keys it
//! listed, and then tombstones every recovered document whose key the walk
//! never observed. Writes routed from another shard are applied during the
//! walk: the `-LOADING` gate guards only the connection path. A key that was
//! deleted while the server was down is not listed, so when a routed `HSET`
//! re-created it mid-walk the probe tombstoned the copy just written. The key
//! existed, and `FT.SEARCH` never returned it.
//!
//! Two shards; every document carries the `{b}` hash tag, so one shard holds
//! them all and walks ~100k keys while the other has nothing to load and
//! serves at once. The target is written through whichever connection is not
//! refused with `-LOADING`, as soon as the loaded shard logs that it listed
//! its keys. An attempt whose write only landed after the walk ended proves
//! nothing, so the test retries until it has an in-window attempt.
//!
//! Heavy (~100k documents per attempt), hence `#[ignore]`. Run with (pin the
//! binary you just built):
//!   MOON_BIN=target/release-fast/moon cargo test --profile release-fast \
//!     --test vector_rescan_live_write -- --ignored

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const DIM: usize = 16;
const DOCS: usize = 100_000;
const TARGET: &str = "doc:{b}target";
const ATTEMPTS: usize = 8;

/// Printable-ASCII f32s (each in ~0.5..12), so they travel through `Conn`.
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

fn start(dir: &Path) -> (ServerGuard, u16) {
    spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "2",
                "--appendonly",
                "yes",
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
    })
}

fn await_loaded(port: u16) {
    let start = Instant::now();
    loop {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "persistence"]);
        let read = c.send(&["EXISTS", "moon1124:probe"]);
        if info.contains("loading:0\r\n") && read.starts_with(':') {
            return;
        }
        assert!(start.elapsed() < Duration::from_secs(300), "still loading");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn log_since(dir: &Path, from: usize) -> String {
    let raw = std::fs::read(dir.join("moon.log")).unwrap_or_default();
    String::from_utf8_lossy(raw.get(from..).unwrap_or_default()).into_owned()
}

/// KNN-1 key for `blob`, from the raw RESP reply (`*3\r\n:1\r\n$N\r\n<key>...`).
fn top1(c: &mut Conn, blob: &str) -> Option<String> {
    let r = c.send(&[
        "FT.SEARCH",
        "idx",
        "*=>[KNN 1 @vec $q]",
        "PARAMS",
        "2",
        "q",
        blob,
        "DIALECT",
        "2",
    ]);
    let mut lines = r.split("\r\n");
    let head = lines.next()?;
    assert!(head.starts_with('*'), "FT.SEARCH: {r}");
    let _count = lines.next()?;
    let _len = lines.next()?;
    lines.next().map(str::to_owned)
}

enum Outcome {
    /// The write landed after the walk ended: says nothing either way.
    Inconclusive,
    /// The write landed mid-walk; `Some(top1)` of the written vector.
    InWindow(Option<String>),
}

fn attempt(n: usize) -> Outcome {
    let dir = common::unique_test_dir(&format!("moon-1124-{n}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut guard, port) = start(&dir);
    await_loaded(port);
    let mut c = Conn::open(port);
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
    for chunk in (0..DOCS).collect::<Vec<_>>().chunks(2000) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("doc:{{b}}{i}")).collect();
        let vecs: Vec<String> = chunk.iter().map(|&i| vector(i as u64 + 10)).collect();
        let cmds: Vec<[&str; 4]> = keys
            .iter()
            .zip(&vecs)
            .map(|(k, v)| ["HSET", k.as_str(), "vec", v.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
        assert!(!c.pipeline(&refs).contains('-'), "seed HSET failed");
    }
    let old = vector(1);
    assert_eq!(c.send(&["HSET", TARGET, "vec", &old]), ":1\r\n");
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    // Stack B persists the segment and the keymap that names the target.
    std::thread::sleep(Duration::from_secs(5));
    // Deleted "while the server is down": after the keymap, before the crash.
    assert_eq!(c.send(&["DEL", TARGET]), ":1\r\n");
    std::thread::sleep(Duration::from_millis(1500)); // everysec covers the DEL
    drop(c);
    guard.kill_now();
    drop(guard);

    let boot_at = std::fs::metadata(dir.join("moon.log")).map_or(0, |m| m.len() as usize);
    let (guard, port) = start(&dir);
    let deadline = Instant::now() + Duration::from_secs(120);
    while !log_since(&dir, boot_at)
        .lines()
        .any(|l| l.contains("recovery scanned db 0:") && !l.contains(": 0 key"))
    {
        assert!(Instant::now() < deadline, "the walk never started");
        std::thread::sleep(Duration::from_millis(2));
    }
    let new = vector(2);
    loop {
        let mut c = Conn::open(port);
        let r = c.send(&["HSET", TARGET, "vec", &new]);
        if r == ":1\r\n" {
            break;
        }
        assert!(r.starts_with("-LOADING"), "HSET: {r}");
        assert!(Instant::now() < deadline, "no connection accepted the HSET");
    }
    let walk_over = log_since(&dir, boot_at).contains("auto-reindexed");
    await_loaded(port);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["EXISTS", TARGET]), ":1\r\n");
    let outcome = if walk_over {
        Outcome::Inconclusive
    } else {
        Outcome::InWindow(top1(&mut c, &new))
    };
    drop(c);
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
    outcome
}

#[test]
#[ignore = "heavy: seeds 100k documents per attempt; run with --ignored"]
fn a_document_written_during_the_boot_rescan_stays_searchable() {
    for n in 0..ATTEMPTS {
        match attempt(n) {
            Outcome::Inconclusive => continue,
            Outcome::InWindow(top) => {
                assert_eq!(
                    top.as_deref(),
                    Some(TARGET),
                    "the document a routed HSET wrote during the boot rescan \
                     was tombstoned by the rescan's deletion probe (attempt {n})"
                );
                return;
            }
        }
    }
    panic!("instrument: no attempt landed its write inside the walk in {ATTEMPTS} tries");
}
