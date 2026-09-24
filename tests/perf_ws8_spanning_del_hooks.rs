//! moon#1162, end to end against a real `--shards 4` server: a multi-key
//! `DEL`/`UNLINK` whose keys span shards, and a `FLUSHALL`/`FLUSHDB` (whose
//! other-shard legs travel the same `MultiExecute` arm), must run the same
//! index and queue hooks as every other write path.
//!
//! Red on `ae21476` (`MOON_BIN=/home/user/wt/bin/baseline-ae21476`): the
//! spanning `DEL` leaves 20 of 20 deleted documents in `FT.SEARCH`, a
//! `FLUSHALL` leaves the other shards' documents searchable, and a durable
//! queue removed by a spanning `DEL` comes back after a restart.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_spanning_del_hooks`
//! (falls back to the binary Cargo built for this run).

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

const SHARDS: usize = 4;
const DOCS: usize = 40;

fn spawn(dir: &std::path::Path, extra: &[&str]) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let extra: Vec<String> = extra.iter().map(|s| (*s).to_string()).collect();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &SHARDS.to_string(),
                "--save",
                "",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

/// RESP-encode binary-safe parts (vector blobs are not UTF-8).
fn encode_bytes(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn send_bytes(c: &mut Conn, parts: &[&[u8]]) -> String {
    c.sock.write_all(&encode_bytes(parts)).unwrap();
    c.read_replies(1)
}

fn blob(i: usize) -> Vec<u8> {
    // Deterministic, distinct 4-d vectors.
    let f = i as f32;
    [f * 0.01, 1.0 - f * 0.01, (f * 0.37).sin(), (f * 0.11).cos()]
        .iter()
        .flat_map(|x| x.to_le_bytes())
        .collect()
}

fn create_index(c: &mut Conn) {
    let r = c.send(&[
        "FT.CREATE",
        "vidx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "emb",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert_eq!(r, "+OK\r\n", "FT.CREATE");
}

fn load_docs(c: &mut Conn) {
    for i in 0..DOCS {
        let key = format!("doc:{i}");
        let title = format!("t{i}");
        let v = blob(i);
        let r = send_bytes(
            c,
            &[
                b"HSET",
                key.as_bytes(),
                b"emb",
                &v,
                b"title",
                title.as_bytes(),
            ],
        );
        assert_eq!(r, ":2\r\n", "HSET {key}");
    }
}

/// The document keys `FT.SEARCH` returns for a KNN over every document.
fn search_keys(c: &mut Conn) -> Vec<usize> {
    let q = blob(7);
    let knn = format!("*=>[KNN {DOCS} @emb $q]");
    let r = send_bytes(
        c,
        &[
            b"FT.SEARCH",
            b"vidx",
            knn.as_bytes(),
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"0",
            b"DIALECT",
            b"2",
        ],
    );
    assert!(r.starts_with('*'), "FT.SEARCH failed: {r:.300}");
    let mut keys: Vec<usize> = r
        .split("\r\n")
        .filter_map(|l| l.strip_prefix("doc:"))
        .filter_map(|n| n.parse().ok())
        .collect();
    keys.sort_unstable();
    keys
}

fn names(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|i| format!("doc:{i}")).collect()
}

fn owners(keys: &[String]) -> std::collections::BTreeSet<usize> {
    keys.iter()
        .map(|k| moon::shard::dispatch::key_to_shard(k.as_bytes(), SHARDS))
        .collect()
}

#[test]
fn spanning_del_and_unlink_tombstone_every_shards_vectors() {
    let dir = common::unique_test_dir("ws8-del-hooks");
    let (_guard, port) = spawn(&dir, &["--appendonly", "no", "--disk-offload", "disable"]);
    let mut c = Conn::open(port);
    create_index(&mut c);
    load_docs(&mut c);
    assert_eq!(search_keys(&mut c).len(), DOCS, "every document indexed");

    let del = names(0..10);
    let unlink = names(10..20);
    // The bug needs the coordinator: the key set must span shards.
    assert!(owners(&del).len() > 1, "DEL set must span shards");
    assert!(owners(&unlink).len() > 1, "UNLINK set must span shards");

    let mut cmd: Vec<&str> = vec!["DEL"];
    cmd.extend(del.iter().map(String::as_str));
    assert_eq!(c.send(&cmd), ":10\r\n");
    let mut cmd: Vec<&str> = vec!["UNLINK"];
    cmd.extend(unlink.iter().map(String::as_str));
    assert_eq!(c.send(&cmd), ":10\r\n");

    let left = search_keys(&mut c);
    let resurrected: Vec<usize> = left.iter().copied().filter(|&i| i < 20).collect();
    assert!(
        resurrected.is_empty(),
        "deleted documents still returned by FT.SEARCH after a spanning DEL/UNLINK: \
         {resurrected:?} ({} hits)",
        left.len()
    );
    assert_eq!(left, (20..DOCS).collect::<Vec<_>>(), "surviving documents");
    drop(c);
    drop(_guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flushall_and_flushdb_clear_every_shards_index_contents() {
    let dir = common::unique_test_dir("ws8-flush-hooks");
    let (_guard, port) = spawn(&dir, &["--appendonly", "no", "--disk-offload", "disable"]);
    let mut c = Conn::open(port);
    create_index(&mut c);

    for flush in ["FLUSHALL", "FLUSHDB"] {
        load_docs(&mut c);
        assert_eq!(search_keys(&mut c).len(), DOCS, "{flush}: reloaded");
        assert_eq!(c.send(&[flush]), "+OK\r\n");
        assert_eq!(c.send(&["DBSIZE"]), ":0\r\n", "{flush}: keyspace");
        let left = search_keys(&mut c);
        assert!(
            left.is_empty(),
            "{flush} left {} documents searchable on the other shards: {left:?}",
            left.len()
        );
    }
    drop(c);
    drop(_guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Count occurrences of `needle` across every shard's `wal-v3/` files.
fn wal_v3_count(dir: &std::path::Path, needle: &[u8]) -> usize {
    let mut n = 0;
    for shard in 0..SHARDS {
        let wal = dir.join(format!("shard-{shard}")).join("wal-v3");
        if let Ok(entries) = std::fs::read_dir(&wal) {
            for e in entries.flatten() {
                if let Ok(bytes) = std::fs::read(e.path()) {
                    n += bytes.windows(needle.len()).filter(|w| *w == needle).count();
                }
            }
        }
    }
    n
}

#[test]
fn spanning_del_tombstones_durable_queues_across_restart() {
    let dir = common::unique_test_dir("ws8-del-mq");
    let persist = ["--appendonly", "yes", "--disk-offload", "enable"];
    let queues: Vec<String> = (0..8).map(|i| format!("ws8mq:{i}")).collect();
    assert!(owners(&queues).len() > 1, "queue set must span shards");
    {
        let (mut guard, port) = spawn(&dir, &persist);
        let mut c = Conn::open(port);
        for q in &queues {
            assert_eq!(c.send(&["MQ", "CREATE", q, "MAXDELIVERY", "0"]), "+OK\r\n");
            let id = c.send(&["MQ", "PUSH", q, "f", "v"]);
            assert!(id.starts_with('$'), "MQ PUSH {q}: {id}");
        }
        let mut cmd: Vec<&str> = vec!["DEL"];
        cmd.extend(queues.iter().map(String::as_str));
        assert_eq!(c.send(&cmd), ":8\r\n");
        for q in &queues {
            assert_eq!(c.send(&["XLEN", q]), ":0\r\n", "{q} deleted");
        }
        // Each queue's name appears in its MqCreate and MqPush records; a
        // tombstoned queue adds its MqDrop record. Wait (bounded) until every
        // tombstone is on disk before the crash, so a failure below is the
        // missing tombstone and never a lost race. A binary that writes no
        // tombstone times out here and fails at the restart check instead.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline
            && queues.iter().any(|q| wal_v3_count(&dir, q.as_bytes()) < 3)
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        guard.kill_now();
    }
    let (_guard, port) = spawn(&dir, &persist);
    let mut c = Conn::open(port);
    let resurrected: Vec<&String> = queues
        .iter()
        .filter(|q| c.send(&["XLEN", q]) != ":0\r\n")
        .collect();
    assert!(
        resurrected.is_empty(),
        "durable queues removed by a spanning DEL came back after a restart: {resurrected:?}"
    );
    drop(c);
    drop(_guard);
    let _ = std::fs::remove_dir_all(&dir);
}
