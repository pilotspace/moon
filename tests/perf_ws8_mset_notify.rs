//! PR #1233 review (moon#1184): every `MSET`/`MSETNX` pair fires exactly one
//! keyspace `set` event, whichever path ran it.
//!
//! redis 7.0.15's `msetGenericCommand` notifies inside its pair loop:
//! - one `__keyevent@<db>__:set` per pair, so a key named twice fires twice;
//! - an `MSETNX` that sets nothing fires none;
//! - the same holds inside `MULTI` and for `redis.call("MSET", …)` in Lua.
//!
//! Moon's `string::mset`/`msetnx` notified nothing. On `ae21476` a spanning
//! `MSET` sent one `SET k v` leg per remote pair, and SET's fast path notified,
//! so only the connection shard's keys went missing. moon#1184 merged those
//! legs into one `MSET` per owner, and every key went silent (0 of 32 on the PR
//! head `d4a2fd3`). The events are emitted in the MSET body now, which every
//! path runs exactly once per pair.
//!
//! The scenario covers each path that runs the body:
//! - a spanning `MSET` (the coordinator's local slice plus one leg per remote
//!   owner), with a duplicated key;
//! - one single-owner `MSET` per shard, so one of them is the coordinator's
//!   all-local fast path and the others a single remote leg;
//! - one `MSETNX` per shard that succeeds, one that fails because a key
//!   exists, and at `--shards > 1` one refused with CROSSSLOT;
//! - `MSET` and `MSETNX` inside `MULTI`, and `MSET` from Lua.
//!
//! It runs at `--shards 4` and at `--shards 1`, where no coordinator is
//! involved and `command::dispatch` runs the whole command.
//!
//! `MOON_BIN=<moon> cargo test --test perf_ws8_mset_notify`

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

/// Keys in the spanning MSET.
const SPANNING_KEYS: usize = 32;

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
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
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn owner(key: &str, shards: usize) -> usize {
    moon::shard::dispatch::key_to_shard(key.as_bytes(), shards)
}

/// `count` fresh keys `<prefix>:<j>` that `shard` owns.
fn keys_on(prefix: &str, shard: usize, shards: usize, count: usize) -> Vec<String> {
    (0..)
        .map(|j| format!("{prefix}:{j}"))
        .filter(|k| owner(k, shards) == shard)
        .take(count)
        .collect()
}

/// A raw `PSUBSCRIBE __keyevent@0__:*` connection. Pushes arrive
/// unsolicited, so it reads frames as they come rather than per request.
struct Subscriber {
    sock: TcpStream,
    buf: Vec<u8>,
}

impl Subscriber {
    fn open(port: u16) -> Self {
        let mut sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        sock.write_all(&common::encode(&["PSUBSCRIBE", "__keyevent@0__:*"]))
            .unwrap();
        let mut sub = Subscriber {
            sock,
            buf: Vec::new(),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(frame) = sub.next_frame() {
                assert!(
                    frame.contains("psubscribe"),
                    "not a PSUBSCRIBE ack: {frame:?}"
                );
                return sub;
            }
            assert!(Instant::now() < deadline, "no PSUBSCRIBE ack");
            sub.fill();
        }
    }

    /// Read whatever the socket has within its short timeout.
    fn fill(&mut self) {
        let mut chunk = [0u8; 65536];
        if let Ok(n) = self.sock.read(&mut chunk) {
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Pop one complete top-level frame, if the buffer holds one.
    fn next_frame(&mut self) -> Option<String> {
        let n = common::framed_len(&self.buf, 1)?;
        let frame = String::from_utf8_lossy(&self.buf[..n]).into_owned();
        self.buf.drain(..n);
        Some(frame)
    }

    /// Collect `(event, key)` counts until `want` events arrived, then keep
    /// listening for `grace`: a duplicate event would arrive in that window.
    fn collect(&mut self, want: usize, grace: Duration) -> BTreeMap<(String, String), usize> {
        let mut seen: BTreeMap<(String, String), usize> = BTreeMap::new();
        let mut total = 0usize;
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut grace_end: Option<Instant> = None;
        loop {
            while let Some(frame) = self.next_frame() {
                // *4 $8 pmessage $n <pattern> $n <channel> $n <key>
                let parts: Vec<&str> = frame.split("\r\n").collect();
                assert_eq!(parts.get(2), Some(&"pmessage"), "unexpected push {frame:?}");
                let event = parts[6].trim_start_matches("__keyevent@0__:").to_string();
                *seen.entry((event, parts[8].to_string())).or_default() += 1;
                total += 1;
            }
            let now = Instant::now();
            if total >= want && grace_end.is_none() {
                grace_end = Some(now + grace);
            }
            if grace_end.is_some_and(|end| now >= end) || now >= deadline {
                return seen;
            }
            self.fill();
        }
    }
}

fn run_scenario(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws8-mset-notify-s{shards}"));
    let (_server, port) = spawn(&dir, shards);

    let mut admin = Conn::open(port);
    assert_eq!(
        admin.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        "+OK\r\n"
    );
    let mut sub = Subscriber::open(port);
    let mut writer = Conn::open(port);
    // key -> how many `set` events redis fires for it (one per pair).
    let mut expected: BTreeMap<String, usize> = BTreeMap::new();

    // 1. Spanning MSET: the local slice and one leg per remote owner. The
    //    last pair repeats the first key, which redis notifies twice.
    let spanning: Vec<String> = (0..SPANNING_KEYS).map(|i| format!("nk:{i}")).collect();
    let mut per_shard = vec![0usize; shards];
    for k in &spanning {
        per_shard[owner(k, shards)] += 1;
    }
    assert!(
        per_shard.iter().all(|&n| n > 0),
        "keys must span every shard"
    );
    let mut mset: Vec<String> = vec!["MSET".into()];
    for (i, k) in spanning.iter().enumerate() {
        mset.push(k.clone());
        mset.push(format!("v{i}"));
        *expected.entry(k.clone()).or_default() += 1;
    }
    mset.push(spanning[0].clone());
    mset.push("again".into());
    *expected.entry(spanning[0].clone()).or_default() += 1;
    let parts: Vec<&str> = mset.iter().map(String::as_str).collect();
    assert_eq!(writer.send(&parts), "+OK\r\n", "spanning MSET");

    // 2. One single-owner MSET per shard: the connection's own shard takes the
    //    coordinator's all-local fast path, every other one a single remote
    //    leg (at --shards 1: plain local dispatch).
    for s in 0..shards {
        let keys = keys_on("own", s, shards, 2);
        let reply = writer.send(&["MSET", &keys[0], "a", &keys[1], "b"]);
        assert_eq!(reply, "+OK\r\n", "single-owner MSET on shard {s}");
        for k in keys {
            *expected.entry(k).or_default() += 1;
        }
    }

    // 3. MSETNX: a success per shard notifies each key; a refusal (a key
    //    exists, or the keys span shards) notifies nothing.
    for s in 0..shards {
        let keys = keys_on("nx", s, shards, 2);
        let reply = writer.send(&["MSETNX", &keys[0], "a", &keys[1], "b"]);
        assert_eq!(reply, ":1\r\n", "MSETNX on shard {s}");
        for k in keys {
            *expected.entry(k).or_default() += 1;
        }
    }
    let taken = &spanning[1];
    let fresh = keys_on("nxfail", owner(taken, shards), shards, 1);
    assert_eq!(
        writer.send(&["MSETNX", &fresh[0], "a", taken, "b"]),
        ":0\r\n",
        "MSETNX over an existing key"
    );
    if shards > 1 {
        let a = keys_on("nxspan", 0, shards, 1);
        let b = keys_on("nxspan", 1, shards, 1);
        let reply = writer.send(&["MSETNX", &a[0], "a", &b[0], "b"]);
        assert!(
            reply.starts_with("-CROSSSLOT"),
            "spanning MSETNX: {reply:?}"
        );
    }

    // 4. Inside MULTI, and from Lua. Hash tags keep each body on one shard.
    let txn = writer.pipeline(&[
        &["MULTI"],
        &["MSET", "{tx}a", "1", "{tx}b", "2"],
        &["MSETNX", "{tx}c", "3"],
        &["EXEC"],
    ]);
    assert!(txn.ends_with("*2\r\n+OK\r\n:1\r\n"), "MULTI/EXEC: {txn:?}");
    for k in ["{tx}a", "{tx}b", "{tx}c"] {
        *expected.entry(k.to_string()).or_default() += 1;
    }
    let lua = writer.send(&[
        "EVAL",
        "return redis.call('MSET', KEYS[1], 'x', KEYS[2], 'y')",
        "2",
        "{lua}a",
        "{lua}b",
    ]);
    assert_eq!(lua, "+OK\r\n", "EVAL MSET");
    for k in ["{lua}a", "{lua}b"] {
        *expected.entry(k.to_string()).or_default() += 1;
    }

    let want: usize = expected.values().sum();
    let seen = sub.collect(want, Duration::from_millis(500));
    let got: usize = seen.values().sum();
    eprintln!("--shards {shards}: {got} keyspace events for {want} MSET/MSETNX pairs");

    let mut wrong: Vec<String> = Vec::new();
    for (key, &n) in &expected {
        let have = seen
            .get(&("set".to_string(), key.clone()))
            .copied()
            .unwrap_or(0);
        if have != n {
            wrong.push(format!("{key}: {have} set events, redis fires {n}"));
        }
    }
    for ((event, key), n) in &seen {
        if event != "set" || !expected.contains_key(key) {
            wrong.push(format!("{key}: {n} unexpected `{event}` events"));
        }
    }
    assert!(
        wrong.is_empty(),
        "--shards {shards}: {got} events for {want} MSET/MSETNX pairs; {} keys differ \
         from redis (first: {:?})",
        wrong.len(),
        &wrong[..wrong.len().min(8)]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn every_mset_pair_notifies_set_exactly_once_at_4_shards() {
    run_scenario(4);
}

#[test]
fn every_mset_pair_notifies_set_exactly_once_at_1_shard() {
    run_scenario(1);
}
