//! moon#1182, end to end at `--shards 4`: a multi-shard KNN FT.SEARCH runs
//! every shard's leg on the cooperative (yielding) search path — the C5 path
//! that was `--shards 1` only — and still answers the same results.
//!
//! - `ft_search_cooperative_yields_total` moves at `--shards 4` (it stayed 0
//!   on every multi-shard server: each leg searched synchronously, the remote
//!   ones inside the owner's SPSC drain). Red on `ae21476`
//!   (`MOON_BIN=/home/user/wt/bin/baseline-ae21476`).
//! - The document equal to the query ranks first whichever shard owns it, and
//!   `k` hits come back: the remote legs' results are merged, not lost to the
//!   new send-before-search ordering or the spawned remote task.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_ft_scatter`.
#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;

use common::{Conn, ServerGuard};

const SHARDS: usize = 4;
const DOCS: usize = 2400;
const K: usize = 10;

fn spawn(dir: &std::path::Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &SHARDS.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            // A small brute-force chunk so a ~600-vector leg crosses it and
            // yields deterministically (the production chunk is coarser; the
            // yield MECHANISM is what is under test — `ft_search_yield_red`
            // pins the same knob for the same reason).
            .env("MOON_FT_YIELD_CHUNK", "64")
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn encode_bytes(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn vector(i: usize) -> [f32; 4] {
    let f = i as f32;
    [
        (f * 0.013).sin(),
        (f * 0.029).cos(),
        (f * 0.007).sin(),
        (f * 0.017).cos(),
    ]
}

fn blob(v: [f32; 4]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    c.send(&["INFO", "stats"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO stats has no {field}"))
}

/// `(total, keys in rank order)` of a KNN FT.SEARCH for `query`.
fn knn(c: &mut Conn, query: &[u8]) -> (usize, Vec<String>) {
    let knn = format!("*=>[KNN {K} @emb $q]");
    c.sock
        .write_all(&encode_bytes(&[
            b"FT.SEARCH",
            b"vidx",
            knn.as_bytes(),
            b"PARAMS",
            b"2",
            b"q",
            query,
            b"RETURN",
            b"0",
            b"DIALECT",
            b"2",
        ]))
        .unwrap();
    let r = c.read_replies(1);
    assert!(r.starts_with('*'), "FT.SEARCH failed: {r:.300}");
    let mut lines = r.split("\r\n");
    lines.next();
    let total: usize = lines
        .next()
        .and_then(|l| l.strip_prefix(':'))
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no total in {r:.200}"));
    let keys = r
        .split("\r\n")
        .filter(|l| l.starts_with("doc:"))
        .map(str::to_string)
        .collect();
    (total, keys)
}

#[test]
fn multi_shard_knn_legs_yield_and_merge_every_shard() {
    let dir = common::unique_test_dir("ws8-ft-scatter");
    let (_guard, port) = spawn(&dir);
    let mut c = Conn::open(port);
    assert_eq!(
        c.send(&[
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
        ]),
        "+OK\r\n"
    );
    let mut i = 0;
    while i < DOCS {
        let end = (i + 200).min(DOCS);
        let mut out = Vec::new();
        for j in i..end {
            let key = format!("doc:{j}");
            out.extend_from_slice(&encode_bytes(&[
                b"HSET",
                key.as_bytes(),
                b"emb",
                &blob(vector(j)),
            ]));
        }
        c.sock.write_all(&out).unwrap();
        let replies = c.read_replies(end - i);
        assert!(!replies.contains('-'), "HSET refused: {replies:.200}");
        i = end;
    }

    let yields_before = info_u64(&mut c, "ft_search_cooperative_yields_total");
    // One query per shard: its exact-match document lives on that shard.
    let mut probed = std::collections::BTreeSet::new();
    for j in (0..DOCS).step_by(37) {
        let key = format!("doc:{j}");
        let owner = moon::shard::dispatch::key_to_shard(key.as_bytes(), SHARDS);
        if !probed.insert(owner) {
            continue;
        }
        let (total, keys) = knn(&mut c, &blob(vector(j)));
        assert_eq!(total, K, "k hits merged from every shard for {key}");
        assert_eq!(keys.len(), K);
        assert_eq!(
            keys[0], key,
            "the document equal to the query (owned by shard {owner}) must rank first: {keys:?}"
        );
        if probed.len() == SHARDS {
            break;
        }
    }
    assert_eq!(probed.len(), SHARDS, "a query landed on every shard");
    let yields = info_u64(&mut c, "ft_search_cooperative_yields_total") - yields_before;
    assert!(
        yields > 0,
        "--shards {SHARDS}: {SHARDS} KNN searches over {DOCS} vectors yielded {yields} times — \
         every leg searched synchronously instead of on the cooperative path"
    );

    drop(c);
    drop(_guard);
    let _ = std::fs::remove_dir_all(&dir);
}
