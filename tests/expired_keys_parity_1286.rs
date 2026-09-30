//! moon#1286 residuals (wave-2a R1 review): `INFO stats` `expired_keys` must
//! match redis 7.2.7 where the first fix left gaps.
//!
//! - **Replay (finding 2):** redis counts nothing while it loads. The AOF
//!   holds every reap the live server made (the active cycle's reason `DEL`s,
//!   a client's `DEL` of an expired key, a write over one) and each was
//!   counted once, live; replaying them must not count them again. Oracle,
//!   `--appendonly yes`: 50 active-expired + 500 `HSET` over expired + 500
//!   `DEL` of expired = 1050 before a restart, **0** after a graceful and a
//!   kill -9 restart. f766fc2: 1050 after both, s1 and s4.
//!
//! - **Writes over an expired key (finding 3):** redis's `lookupKeyWrite`
//!   reaps (and counts) the expired key before the write. Oracle: every case
//!   below counts one per key. f766fc2 counted 0 for SET, SETNX, SET NX,
//!   GETSET, APPEND, INCRBYFLOAT, SETBIT, PFADD, the COPY / RENAME
//!   destination, GET→SET and EXISTS→SET (the read hides the key and the SET
//!   lands before the drain), a timing-dependent part of MSET, and INCR at s4.
//!   Also SETRANGE, SET … GET, SETEX, MSETNX, INCRBY, DECR and the
//!   SUNIONSTORE / BITOP destination (0 at s1). HSET, LPUSH, GETDEL, GETEX
//!   and DEL were right and are the controls.
//!
//! Expected values were captured from redis 7.2.7 with the same commands.
//!
//! `MOON_BIN=<moon> cargo test --test expired_keys_parity_1286 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::Conn;

fn spawn(dir: &Path, shards: usize, appendonly: bool) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let (guard, port) = common::spawn_listening_guarded(move |port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir.to_str().unwrap(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                if appendonly { "yes" } else { "no" },
                "--appendfsync",
                "always",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["INFO", "persistence"]).contains("loading:0\r\n")
        {
            break;
        }
        assert!(Instant::now() < deadline, "server on {port} never loaded");
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port)
}

fn expired_keys(c: &mut Conn) -> u64 {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix("expired_keys:"))
        .unwrap_or_else(|| panic!("no expired_keys in INFO stats: {info}"))
        .trim()
        .parse()
        .unwrap()
}

fn dbsize(c: &mut Conn) -> i64 {
    c.send(&["DBSIZE"])
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap()
}

/// Run `verb key args…` for `key` in `prefix0..prefixN`, one pipeline.
fn each(c: &mut Conn, n: usize, prefix: &str, verb: &[&str], tail: &[&str]) -> String {
    let keys: Vec<String> = (0..n).map(|i| format!("{prefix}{i}")).collect();
    let cmds: Vec<Vec<&str>> = keys
        .iter()
        .map(|k| {
            let mut v = verb.to_vec();
            v.push(k);
            v.extend_from_slice(tail);
            v
        })
        .collect();
    let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
    c.pipeline(&refs)
}

/// SIGTERM (a graceful shutdown) and reap.
fn graceful_stop(guard: &mut common::ServerGuard) {
    let pid = guard.id().to_string();
    let _ = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status();
    if let Some(mut child) = guard.take() {
        let deadline = Instant::now() + Duration::from_secs(30);
        while child.try_wait().ok().flatten().is_none() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                panic!("the server did not stop on SIGTERM");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Wait until `expired_keys` reaches `want` (or 10 s), returning the last read.
fn wait_for(c: &mut Conn, want: u64) -> u64 {
    let start = Instant::now();
    loop {
        let n = expired_keys(c);
        if n >= want || start.elapsed() >= Duration::from_secs(10) {
            return n;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// Finding 2: AOF replay
// ---------------------------------------------------------------------------

fn replay_does_not_recount(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1286-replay-s{shards}"));
    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    // Reaped by the active cycle: reason DELs in the AOF.
    each(&mut c, 50, "a", &["SET"], &["v", "PX", "50"]);
    // Reaped by a write over the expired key.
    each(&mut c, 500, "h", &["SET"], &["v", "PX", "30"]);
    std::thread::sleep(Duration::from_millis(40));
    each(&mut c, 500, "h", &["HSET"], &["f", "v"]);
    // Reaped by a client's DEL.
    each(&mut c, 500, "d", &["SET"], &["v", "PX", "30"]);
    std::thread::sleep(Duration::from_millis(40));
    each(&mut c, 500, "d", &["DEL"], &[]);
    let before = wait_for(&mut c, 1050);
    assert_eq!(before, 1050, "s{shards}: live count (redis 7.2.7: 1050)");
    assert_eq!(dbsize(&mut c), 500);
    drop(c);
    graceful_stop(&mut guard);

    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    std::thread::sleep(Duration::from_millis(500));
    let after_graceful = expired_keys(&mut c);
    let size_graceful = dbsize(&mut c);
    drop(c);
    guard.kill_now();

    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    std::thread::sleep(Duration::from_millis(500));
    let after_kill = expired_keys(&mut c);
    let size_kill = dbsize(&mut c);
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        (size_graceful, size_kill),
        (500, 500),
        "s{shards}: the replay restores the hashes"
    );
    assert_eq!(
        (after_graceful, after_kill),
        (0, 0),
        "s{shards}: expired_keys after a graceful and a kill -9 restart (redis 7.2.7: 0, 0): \
         the AOF replay counted the logged reaps again"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn aof_replay_does_not_recount_expired_keys_1_shard() {
    replay_does_not_recount(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn aof_replay_does_not_recount_expired_keys_4_shards() {
    replay_does_not_recount(4);
}

// ---------------------------------------------------------------------------
// Finding 3: a write over an expired key
// ---------------------------------------------------------------------------

/// Keys per case; the oracle counts exactly one per key.
const OVERWRITE_N: usize = 1000;

/// `(name, commands for key index i)`. Keys share a hash tag per index, so a
/// two-key command stays on one shard at s4.
type Case = (&'static str, fn(usize) -> Vec<Vec<String>>);

fn v(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

fn o(i: usize) -> String {
    format!("{{t{i}}}o")
}

fn src(i: usize) -> String {
    format!("{{t{i}}}s")
}

const OVERWRITE_CASES: &[Case] = &[
    ("SET", |i| vec![v(&["SET", &o(i), "x"])]),
    ("SETNX", |i| vec![v(&["SETNX", &o(i), "x"])]),
    ("SET NX", |i| vec![v(&["SET", &o(i), "x", "NX"])]),
    ("GETSET", |i| vec![v(&["GETSET", &o(i), "x"])]),
    ("APPEND", |i| vec![v(&["APPEND", &o(i), "x"])]),
    ("INCR", |i| vec![v(&["INCR", &o(i)])]),
    ("INCRBYFLOAT", |i| vec![v(&["INCRBYFLOAT", &o(i), "1.5"])]),
    ("SETBIT", |i| vec![v(&["SETBIT", &o(i), "1", "1"])]),
    ("PFADD", |i| vec![v(&["PFADD", &o(i), "x"])]),
    ("MSET", |i| vec![v(&["MSET", &o(i), "x"])]),
    ("COPY destination", |i| {
        vec![v(&["SET", &src(i), "x"]), v(&["COPY", &src(i), &o(i)])]
    }),
    ("RENAME destination", |i| {
        vec![v(&["SET", &src(i), "x"]), v(&["RENAME", &src(i), &o(i)])]
    }),
    ("GET then SET", |i| {
        vec![v(&["GET", &o(i)]), v(&["SET", &o(i), "x"])]
    }),
    ("EXISTS then SET", |i| {
        vec![v(&["EXISTS", &o(i)]), v(&["SET", &o(i), "x"])]
    }),
    ("SETRANGE", |i| vec![v(&["SETRANGE", &o(i), "0", "x"])]),
    ("SET GET", |i| vec![v(&["SET", &o(i), "x", "GET"])]),
    ("SETEX", |i| vec![v(&["SETEX", &o(i), "100", "x"])]),
    ("MSETNX", |i| vec![v(&["MSETNX", &o(i), "x"])]),
    ("INCRBY", |i| vec![v(&["INCRBY", &o(i), "3"])]),
    ("DECR", |i| vec![v(&["DECR", &o(i)])]),
    ("SUNIONSTORE destination", |i| {
        vec![
            v(&["SADD", &src(i), "x"]),
            v(&["SUNIONSTORE", &o(i), &src(i)]),
        ]
    }),
    ("BITOP destination", |i| {
        vec![
            v(&["SET", &src(i), "x"]),
            v(&["BITOP", "NOT", &o(i), &src(i)]),
        ]
    }),
    // Controls: counted before this fix.
    ("HSET", |i| vec![v(&["HSET", &o(i), "f", "v"])]),
    ("LPUSH", |i| vec![v(&["LPUSH", &o(i), "x"])]),
    ("GETDEL", |i| vec![v(&["GETDEL", &o(i)])]),
    ("GETEX", |i| vec![v(&["GETEX", &o(i), "EX", "100"])]),
    ("DEL", |i| vec![v(&["DEL", &o(i)])]),
];

fn overwrite_matrix(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1286-overwrite-s{shards}"));
    let (mut guard, port) = spawn(&dir, shards, false);
    let mut c = Conn::open(port);
    let mut wrong = Vec::new();
    for (name, mk) in OVERWRITE_CASES {
        assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n");
        std::thread::sleep(Duration::from_millis(300));
        let before = expired_keys(&mut c);
        let sets: Vec<Vec<String>> = (0..OVERWRITE_N)
            .map(|i| v(&["SET", &o(i), "5", "PX", "30"]))
            .collect();
        pipeline(&mut c, &sets);
        std::thread::sleep(Duration::from_millis(32));
        let writes: Vec<Vec<String>> = (0..OVERWRITE_N).flat_map(mk).collect();
        pipeline(&mut c, &writes);
        // Whatever the write left expired (nothing, here) is reaped by now.
        std::thread::sleep(Duration::from_millis(1200));
        let delta = expired_keys(&mut c) - before;
        if delta != OVERWRITE_N as u64 {
            wrong.push(format!("{name}: {delta}"));
        }
    }
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        wrong.is_empty(),
        "s{shards}: expired_keys per {OVERWRITE_N} writes over an expired key (redis 7.2.7: \
         {OVERWRITE_N} each): {wrong:?}"
    );
}

fn pipeline(c: &mut Conn, cmds: &[Vec<String>]) {
    for chunk in cmds.chunks(500) {
        let owned: Vec<Vec<&str>> = chunk
            .iter()
            .map(|cmd| cmd.iter().map(String::as_str).collect())
            .collect();
        let refs: Vec<&[&str]> = owned.iter().map(Vec::as_slice).collect();
        c.pipeline(&refs);
    }
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_write_over_an_expired_key_counts_it_1_shard() {
    overwrite_matrix(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_write_over_an_expired_key_counts_it_4_shards() {
    overwrite_matrix(4);
}
