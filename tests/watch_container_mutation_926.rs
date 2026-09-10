//! moon#926 — `WATCH` must see an **in-place container mutation**.
//!
//! `WATCH` compares a per-entry version at `EXEC`. Before this suite that
//! version moved only when a whole `Entry` was replaced (`Database::set`), so
//! `SET` and `DEL` aborted a watching transaction and every container write —
//! `HSET`, `LPUSH`, `LPOP`, `SADD`, `ZADD`, `XADD`, and `EXPIRE` — did not. A
//! CAS loop over a hash field therefore lost the update while `EXEC` returned
//! the result array, telling the client the compare-and-swap had held.
//!
//! Measured on the unfixed tree (`ab91a23e`, release-fast, shards=1) against
//! redis 8.6.1 as the oracle, two connections and a real
//! `WATCH`/`MULTI`/`EXEC` cycle per row — 37 divergences, every one of them
//! "moon COMMITs where redis ABORTs":
//!
//! ```text
//!   HSET HDEL HINCRBY HSETNX  LPUSH LPOP LSET LINSERT LREM LTRIM
//!   SADD SREM SPOP  ZADD ZINCRBY ZREM ZPOPMIN  XADD XDEL
//!   EXPIRE PEXPIRE PERSIST
//! ```
//!
//! The two tests that existed for the counter (`storage::entry` and
//! `storage::db`) both call `increment_version` **directly**, so they proved
//! the counter increments and wraps and could not observe that no command ever
//! called it. Every test here drives real commands through a real transaction
//! and asserts on the raw `*-1`; none of them can pass by touching the counter.
//!
//! Both directions are asserted. `wcm8` is the pin for the other failure mode:
//! a bump placed on a *read* accessor (several take `&mut self` — see
//! `Database::get_promoted`'s "This REWRITES the value" note) would abort a
//! transaction that nothing wrote, which is as broken as never aborting.
//!
//! Run alone with: cargo test --test watch_container_mutation_926

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Harness (shape borrowed from tests/watch_cas_transactions.rs)
// ---------------------------------------------------------------------------

fn spawn_moon(dir: &std::path::Path, shards: u32) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // The shared /Volumes checkout hovers near the 5% diskfull
                // guard; a tripped guard turns every write into MOONERR and
                // would fail this suite for an unrelated reason.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        common::sigkill(&mut self.0);
    }
}

/// Connect and return only once the server answered a PING on THIS socket.
fn connect_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut s) = TcpStream::connect(format!("127.0.0.1:{port}")) {
            s.set_read_timeout(Some(Duration::from_secs(10))).ok();
            s.set_write_timeout(Some(Duration::from_secs(10))).ok();
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = s.read(&mut buf)
                    && n > 0
                    && buf[..n].windows(4).any(|w| w == b"PONG")
                {
                    return s;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "server on {port} never answered PING"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// One command, one raw reply. Raw bytes on purpose: the abort signal IS the
/// type byte (`*-1` / `_`), so a reader that rendered replies to text would
/// hide the very thing under test.
fn cmd(s: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    s.write_all(&out).expect("write command");
    read_reply(s)
}

fn read_reply(s: &mut TcpStream) -> Vec<u8> {
    std::thread::sleep(Duration::from_millis(60));
    let mut buf = vec![0u8; 65536];
    match s.read(&mut buf) {
        Ok(n) => buf[..n].to_vec(),
        Err(e) => panic!("read reply: {e}"),
    }
}

fn is_null(reply: &[u8]) -> bool {
    reply.starts_with(b"*-1\r\n") || reply.starts_with(b"$-1\r\n") || reply.starts_with(b"_\r\n")
}

fn text(reply: &[u8]) -> String {
    String::from_utf8_lossy(reply).into_owned()
}

fn server(shards: u32) -> (ServerGuard, u16, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), shards);
    (ServerGuard(child), port, dir)
}

/// The whole cycle, from a clean key: seed on A, `WATCH k`, run `mutation` on a
/// genuinely separate connection B, then `MULTI` / `body` / `EXEC` on A.
///
/// Returns `true` when `EXEC` aborted (`*-1`).
fn watch_cycle(port: u16, seed: &[&[&str]], mutation: Option<&[&str]>, body: &[&str]) -> bool {
    let (mut a, mut b) = (connect_ready(port), connect_ready(port));
    cmd(&mut a, &["DEL", "k"]);
    for s in seed {
        let r = cmd(&mut a, s);
        assert!(
            !text(&r).starts_with("-ERR") && !text(&r).starts_with("-WRONGTYPE"),
            "seed {s:?} failed: {:?}",
            text(&r)
        );
    }
    let w = cmd(&mut a, &["WATCH", "k"]);
    assert!(w.starts_with(b"+OK"), "WATCH refused: {:?}", text(&w));
    if let Some(m) = mutation {
        let r = cmd(&mut b, m);
        assert!(
            !text(&r).starts_with("-ERR") && !text(&r).starts_with("-WRONGTYPE"),
            "mutation {m:?} failed: {:?}",
            text(&r)
        );
    }
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, body);
    is_null(&cmd(&mut a, &["EXEC"]))
}

// ---------------------------------------------------------------------------
// One named test per container type — each is the mutation anchor for the
// accessor that serves that type.
// ---------------------------------------------------------------------------

/// HASH — `Database::get_or_create_hash_listpack` / `get_or_create_hash`.
#[test]
fn wcm1_hash_mutation_aborts_a_live_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation) in [
        (
            "HSET on an existing field",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HSET", "k", "f", "2"],
        ),
        ("HSET creating the key", vec![], vec!["HSET", "k", "f", "2"]),
        (
            "HDEL of a present field",
            vec![vec!["HSET", "k", "f", "1"], vec!["HSET", "k", "g", "1"]],
            vec!["HDEL", "k", "f"],
        ),
        (
            "HINCRBY",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HINCRBY", "k", "f", "1"],
        ),
        (
            // Past `hash-max-listpack-entries`: the full-HashMap encoding,
            // i.e. `get_or_create_hash`, not the listpack accessor.
            "HSET on a hashtable-encoded hash",
            vec![
                {
                    let mut v = vec!["HSET", "k"];
                    for f in HASH_FIELDS.iter() {
                        v.push(f);
                        v.push("x");
                    }
                    v
                },
                vec!["HSET", "k", "f", "1"],
            ],
            vec!["HSET", "k", "f", "2"],
        ),
    ] {
        let seed: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            watch_cycle(port, &seed, Some(&mutation), &["HSET", "k", "f", "9"]),
            "{label}: EXEC must abort — a watched hash was modified by another client"
        );
    }
}

/// 200 field names, enough to push the hash past `hash-max-listpack-entries`
/// and force the full-HashMap encoding.
static HASH_FIELDS: [&str; 200] = [
    "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "b0", "b1", "b2", "b3", "b4", "b5",
    "b6", "b7", "b8", "b9", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "d0", "d1",
    "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7",
    "e8", "e9", "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "g0", "g1", "g2", "g3",
    "g4", "g5", "g6", "g7", "g8", "g9", "h0", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9",
    "i0", "i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8", "i9", "j0", "j1", "j2", "j3", "j4", "j5",
    "j6", "j7", "j8", "j9", "k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "l0", "l1",
    "l2", "l3", "l4", "l5", "l6", "l7", "l8", "l9", "m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7",
    "m8", "m9", "n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "o0", "o1", "o2", "o3",
    "o4", "o5", "o6", "o7", "o8", "o9", "p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9",
    "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "r0", "r1", "r2", "r3", "r4", "r5",
    "r6", "r7", "r8", "r9", "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "t0", "t1",
    "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9",
];

/// LIST — `Database::get_or_create_list_listpack` / `get_or_create_list`, and
/// `get_mut_if_present` for the pops.
#[test]
fn wcm2_list_mutation_aborts_a_live_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation) in [
        (
            "LPUSH",
            vec![vec!["RPUSH", "k", "a"]],
            vec!["LPUSH", "k", "b"],
        ),
        (
            "LPOP",
            vec![vec!["RPUSH", "k", "a", "b"]],
            vec!["LPOP", "k"],
        ),
        ("RPUSH creating the key", vec![], vec!["RPUSH", "k", "b"]),
        (
            "LSET (same length, changed content)",
            vec![vec!["RPUSH", "k", "a", "b"]],
            vec!["LSET", "k", "0", "z"],
        ),
        (
            "LREM",
            vec![vec!["RPUSH", "k", "a", "b", "a"]],
            vec!["LREM", "k", "1", "a"],
        ),
        (
            "LTRIM",
            vec![vec!["RPUSH", "k", "a", "b", "c"]],
            vec!["LTRIM", "k", "0", "1"],
        ),
    ] {
        let seed: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            watch_cycle(port, &seed, Some(&mutation), &["RPUSH", "k", "z"]),
            "{label}: EXEC must abort — a watched list was modified by another client"
        );
    }
}

/// SET — `get_or_create_set_listpack`, `get_or_create_intset`,
/// `get_or_create_set`. All three encodings, because SADD picks between them.
#[test]
fn wcm3_set_mutation_aborts_a_live_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation) in [
        (
            "SADD to a listpack set",
            vec![vec!["SADD", "k", "a"]],
            vec!["SADD", "k", "b"],
        ),
        ("SADD creating the key", vec![], vec!["SADD", "k", "b"]),
        (
            "SADD to an intset",
            vec![vec!["SADD", "k", "1"]],
            vec!["SADD", "k", "2"],
        ),
        (
            "SREM",
            vec![vec!["SADD", "k", "a", "b"]],
            vec!["SREM", "k", "a"],
        ),
        ("SPOP", vec![vec!["SADD", "k", "a", "b"]], vec!["SPOP", "k"]),
    ] {
        let seed: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            watch_cycle(port, &seed, Some(&mutation), &["SADD", "k", "z"]),
            "{label}: EXEC must abort — a watched set was modified by another client"
        );
    }
}

/// SORTED SET — `get_or_create_zset_listpack` / `get_or_create_sorted_set`.
#[test]
fn wcm4_zset_mutation_aborts_a_live_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation) in [
        (
            "ZADD a new member",
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZADD", "k", "2", "b"],
        ),
        ("ZADD creating the key", vec![], vec!["ZADD", "k", "2", "b"]),
        (
            "ZADD rescoring an existing member (cardinality unchanged)",
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZADD", "k", "9", "a"],
        ),
        (
            "ZINCRBY",
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZINCRBY", "k", "1", "a"],
        ),
        (
            "ZREM",
            vec![vec!["ZADD", "k", "1", "a", "2", "b"]],
            vec!["ZREM", "k", "a"],
        ),
        (
            "ZPOPMIN",
            vec![vec!["ZADD", "k", "1", "a", "2", "b"]],
            vec!["ZPOPMIN", "k"],
        ),
    ] {
        let seed: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            watch_cycle(port, &seed, Some(&mutation), &["ZADD", "k", "9", "z"]),
            "{label}: EXEC must abort — a watched zset was modified by another client"
        );
    }
}

/// STREAM — `get_or_create_stream` and `get_stream_mut`.
#[test]
fn wcm5_stream_mutation_aborts_a_live_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation) in [
        (
            "XADD",
            vec![vec!["XADD", "k", "1-1", "f", "v"]],
            vec!["XADD", "k", "2-1", "f", "v"],
        ),
        (
            "XADD creating the key",
            vec![],
            vec!["XADD", "k", "2-1", "f", "v"],
        ),
        (
            "XDEL",
            vec![
                vec!["XADD", "k", "1-1", "f", "v"],
                vec!["XADD", "k", "2-1", "f", "v"],
            ],
            vec!["XDEL", "k", "1-1"],
        ),
    ] {
        let seed: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            watch_cycle(
                port,
                &seed,
                Some(&mutation),
                &["XADD", "k", "9-1", "f", "v"]
            ),
            "{label}: EXEC must abort — a watched stream was modified by another client"
        );
    }
}

/// TTL — `Database::set_expiry`. A watched key given a TTL by another client
/// is a modification: the key now has an end, which is exactly the fact a CAS
/// loop reading it needs to re-check.
#[test]
fn wcm6_expire_aborts_a_live_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation) in [
        (
            "EXPIRE on a string",
            vec![vec!["SET", "k", "v"]],
            vec!["EXPIRE", "k", "1000"],
        ),
        (
            "EXPIRE on a hash",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["EXPIRE", "k", "1000"],
        ),
        (
            "PEXPIRE",
            vec![vec!["SET", "k", "v"]],
            vec!["PEXPIRE", "k", "100000"],
        ),
        (
            "PERSIST removing a live TTL",
            vec![vec!["SET", "k", "v"], vec!["EXPIRE", "k", "1000"]],
            vec!["PERSIST", "k"],
        ),
    ] {
        let seed: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            watch_cycle(port, &seed, Some(&mutation), &["SET", "k", "z"]),
            "{label}: EXEC must abort — a watched key's TTL was changed by another client"
        );
    }
}

/// The abort has to be REAL: the queued write must not have landed.
#[test]
fn wcm7_the_aborted_container_write_did_not_land() {
    let (_g, port, _d) = server(1);
    let (mut a, mut b) = (connect_ready(port), connect_ready(port));

    cmd(&mut a, &["DEL", "k"]);
    cmd(&mut a, &["HSET", "k", "f", "seed"]);
    cmd(&mut a, &["WATCH", "k"]);
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["HSET", "k", "f", "from-A"]);
    cmd(&mut b, &["HSET", "k", "f", "from-B"]);

    let exec = cmd(&mut a, &["EXEC"]);
    assert!(
        is_null(&exec),
        "EXEC must abort after HSET on the watched key, got {:?}",
        text(&exec)
    );
    let got = cmd(&mut b, &["HGET", "k", "f"]);
    assert!(
        text(&got).contains("from-B"),
        "the aborted transaction still wrote: k.f should hold from-B, got {:?}",
        text(&got)
    );
}

/// THE OTHER DIRECTION. Several "read-only" accessors take `&mut self` and
/// rewrite the value's encoding in place (`Database::get_promoted`). A version
/// bump placed there would abort a transaction that nothing wrote — an
/// over-eager CAS check is as broken as a missing one, and it is silent.
///
/// Every command below is a pure read on the *other* connection.
#[test]
fn wcm8_reads_by_another_client_do_not_abort_a_live_watch() {
    let (_g, port, _d) = server(1);
    for (seed, read, body) in [
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HGET", "k", "f"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HGETALL", "k"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HLEN", "k"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["RPUSH", "k", "a"]],
            vec!["LRANGE", "k", "0", "-1"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            vec![vec!["RPUSH", "k", "a"]],
            vec!["LLEN", "k"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            vec![vec!["RPUSH", "k", "a"]],
            vec!["LINDEX", "k", "0"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            vec![vec!["SADD", "k", "a"]],
            vec!["SMEMBERS", "k"],
            vec!["SADD", "k", "z"],
        ),
        (
            vec![vec!["SADD", "k", "a"]],
            vec!["SCARD", "k"],
            vec!["SADD", "k", "z"],
        ),
        (
            vec![vec!["SADD", "k", "a"]],
            vec!["SISMEMBER", "k", "a"],
            vec!["SADD", "k", "z"],
        ),
        (
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZRANGE", "k", "0", "-1"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZCARD", "k"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZSCORE", "k", "a"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            vec![vec!["XADD", "k", "1-1", "f", "v"]],
            vec!["XLEN", "k"],
            vec!["XADD", "k", "9-1", "f", "v"],
        ),
        (
            vec![vec!["XADD", "k", "1-1", "f", "v"]],
            vec!["XRANGE", "k", "-", "+"],
            vec!["XADD", "k", "9-1", "f", "v"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["TYPE", "k"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["TTL", "k"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["EXISTS", "k"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["OBJECT", "ENCODING", "k"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["MEMORY", "USAGE", "k"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HSCAN", "k", "0"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            vec![vec!["SADD", "k", "a"]],
            vec!["SSCAN", "k", "0"],
            vec!["SADD", "k", "z"],
        ),
        (
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZSCAN", "k", "0"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            vec![vec!["SET", "k", "vvv"]],
            vec!["GET", "k"],
            vec!["SET", "k", "z"],
        ),
        (
            vec![vec!["SET", "k", "vvv"]],
            vec!["STRLEN", "k"],
            vec!["SET", "k", "z"],
        ),
    ] {
        let seed_refs: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            !watch_cycle(port, &seed_refs, Some(&read), &body),
            "{read:?} is a pure READ — it must NOT abort a watching transaction. \
             A spurious abort here is a version bump wired into a read accessor."
        );
    }
}

/// A watch with NOTHING intervening must still commit — the floor under
/// `wcm8`, so a total-abort regression cannot pass that test by accident.
#[test]
fn wcm9_a_watch_with_no_intervening_command_still_commits() {
    let (_g, port, _d) = server(1);
    for (seed, body) in [
        (
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HSET", "k", "f", "9"],
        ),
        (vec![vec!["RPUSH", "k", "a"]], vec!["RPUSH", "k", "z"]),
        (vec![vec!["SADD", "k", "a"]], vec!["SADD", "k", "z"]),
        (
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            vec![vec!["XADD", "k", "1-1", "f", "v"]],
            vec!["XADD", "k", "9-1", "f", "v"],
        ),
    ] {
        let seed_refs: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            !watch_cycle(port, &seed_refs, None, &body),
            "{body:?}: EXEC must COMMIT when nothing touched the watched key"
        );
    }
}

/// A write the *watching* client issues itself, between `WATCH` and `MULTI`,
/// also invalidates the watch — Redis makes no exception for the owner, and
/// moon must not either.
#[test]
fn wcm10_a_self_issued_container_write_aborts_the_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation, body) in [
        (
            "HSET",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HSET", "k", "f", "2"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            "SADD",
            vec![vec!["SADD", "k", "a"]],
            vec!["SADD", "k", "b"],
            vec!["SADD", "k", "z"],
        ),
        (
            "LPUSH",
            vec![vec!["RPUSH", "k", "a"]],
            vec!["LPUSH", "k", "b"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            "ZADD",
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZADD", "k", "2", "b"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            "XADD",
            vec![vec!["XADD", "k", "1-1", "f", "v"]],
            vec!["XADD", "k", "2-1", "f", "v"],
            vec!["XADD", "k", "9-1", "f", "v"],
        ),
        (
            "EXPIRE",
            vec![vec!["SET", "k", "v"]],
            vec!["EXPIRE", "k", "1000"],
            vec!["SET", "k", "z"],
        ),
    ] {
        let mut a = connect_ready(port);
        cmd(&mut a, &["DEL", "k"]);
        for s in &seed {
            cmd(&mut a, s);
        }
        cmd(&mut a, &["WATCH", "k"]);
        cmd(&mut a, &mutation);
        cmd(&mut a, &["MULTI"]);
        cmd(&mut a, &body);
        let exec = cmd(&mut a, &["EXEC"]);
        assert!(
            is_null(&exec),
            "{label}: a write issued by the WATCHING client itself must still abort EXEC, \
             got {:?}",
            text(&exec)
        );
    }
}

/// PRECISION, where the storage layer can afford it. A write command that
/// changed nothing must leave the watch intact — redis does not dirty a key
/// for `HDEL` of an absent field, `HPERSIST` on a field with no TTL, `PERSIST`
/// on a key with no TTL, or a pop from a key that is not there.
///
/// These are the seams where the writer already knows the answer for free
/// (`hash_delete_field` returns `removed`, `set_expiry` sees `old_ttl`,
/// `get_mut_if_present` returns `None`), so it is stamped on the branch that
/// actually changed something rather than at the `&mut` handout.
#[test]
fn wcm12_writes_that_changed_nothing_keep_the_watch() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation, body) in [
        (
            "HDEL of an absent field",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HDEL", "k", "no-such-field"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            "HPERSIST on a field with no TTL",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HPERSIST", "k", "FIELDS", "1", "f"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            "HGETDEL of an absent field",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HGETDEL", "k", "FIELDS", "1", "no-such-field"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            "HEXPIRE on an absent field",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HEXPIRE", "k", "1000", "FIELDS", "1", "no-such-field"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            "PERSIST on a key with no TTL",
            vec![vec!["SET", "k", "v"]],
            vec!["PERSIST", "k"],
            vec!["SET", "k", "z"],
        ),
        (
            "EXPIRE on a key that is not there",
            vec![],
            vec!["EXPIRE", "k", "1000"],
            vec!["SET", "k", "z"],
        ),
        (
            "LPOP from a key that is not there",
            vec![],
            vec!["LPOP", "k"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            "DEL of a key that is not there",
            vec![],
            vec!["DEL", "k"],
            vec!["SET", "k", "z"],
        ),
        (
            "SETNX on a key that exists",
            vec![vec!["SET", "k", "v"]],
            vec!["SETNX", "k", "w"],
            vec!["SET", "k", "z"],
        ),
    ] {
        let seed_refs: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            !watch_cycle(port, &seed_refs, Some(&mutation), &body),
            "{label}: the write changed nothing, so EXEC must COMMIT — \
             moon matches redis 8.6.1 here and must keep doing so"
        );
    }
}

/// THE RECORDED RESIDUE. Six no-op writes where moon aborts and redis does
/// not, measured against redis 8.6.1 on the fixed tree.
///
/// The stamp lives at the accessor, so it fires when a *write command from
/// another client* takes a mutable handle on the watched key and then decides
/// to change nothing. Closing these would mean the ~60 write handlers each
/// declaring "I changed something" — which is exactly the enumeration that let
/// moon#926 live, and a 61st handler forgetting it is a silent lost update.
/// A handler forgetting it in THIS direction costs one retry of a CAS loop
/// that already has a concurrent writer on the key.
///
/// So this test asserts the over-abort deliberately: narrowing any row is a
/// real improvement, and it should be a decision that turns this test red,
/// not a change that slips past unnoticed.
#[test]
fn wcm13_the_recorded_no_op_over_abort_residue() {
    let (_g, port, _d) = server(1);
    for (label, seed, mutation, body) in [
        (
            "SADD of a member already present",
            vec![vec!["SADD", "k", "a"]],
            vec!["SADD", "k", "a"],
            vec!["SADD", "k", "z"],
        ),
        (
            "SREM of an absent member",
            vec![vec!["SADD", "k", "a"]],
            vec!["SREM", "k", "no-such-member"],
            vec!["SADD", "k", "z"],
        ),
        (
            "LREM of an absent element",
            vec![vec!["RPUSH", "k", "a"]],
            vec!["LREM", "k", "1", "no-such-element"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            "ZREM of an absent member",
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZREM", "k", "no-such-member"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            "ZADD with the score the member already has",
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZADD", "k", "1", "a"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            "HSETNX on a field that exists",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HSETNX", "k", "f", "9"],
            vec!["HSET", "k", "f", "9"],
        ),
    ] {
        assert!(
            watch_cycle(
                port,
                &seed.iter().map(|s| s.as_slice()).collect::<Vec<_>>(),
                Some(&mutation),
                &body
            ),
            "{label}: this row is the RECORDED over-abort residue — it aborts today and \
             redis 8.6.1 commits. If you narrowed it deliberately, update this test and \
             the moon#926 notes; if it changed by accident, the stamp moved."
        );
    }
}

/// The dispatch paths must agree. A fix that lands on the shards=1 path only
/// is the failure mode that made the #457 inline-GET ACL bypass invisible.
#[test]
fn wcm11_multi_shard_agrees_on_every_container_type() {
    let (_g, port, _d) = server(4);
    for (label, seed, mutation, body) in [
        (
            "HSET",
            vec![vec!["HSET", "k", "f", "1"]],
            vec!["HSET", "k", "f", "2"],
            vec!["HSET", "k", "f", "9"],
        ),
        (
            "LPUSH",
            vec![vec!["RPUSH", "k", "a"]],
            vec!["LPUSH", "k", "b"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            "LPOP",
            vec![vec!["RPUSH", "k", "a", "b"]],
            vec!["LPOP", "k"],
            vec!["RPUSH", "k", "z"],
        ),
        (
            "SADD",
            vec![vec!["SADD", "k", "a"]],
            vec!["SADD", "k", "b"],
            vec!["SADD", "k", "z"],
        ),
        (
            "ZADD",
            vec![vec!["ZADD", "k", "1", "a"]],
            vec!["ZADD", "k", "2", "b"],
            vec!["ZADD", "k", "9", "z"],
        ),
        (
            "XADD",
            vec![vec!["XADD", "k", "1-1", "f", "v"]],
            vec!["XADD", "k", "2-1", "f", "v"],
            vec!["XADD", "k", "9-1", "f", "v"],
        ),
        (
            "EXPIRE",
            vec![vec!["SET", "k", "v"]],
            vec!["EXPIRE", "k", "1000"],
            vec!["SET", "k", "z"],
        ),
    ] {
        let seed_refs: Vec<&[&str]> = seed.iter().map(|s| s.as_slice()).collect();
        assert!(
            watch_cycle(port, &seed_refs, Some(&mutation), &body),
            "shards=4, {label}: EXEC must abort — the multi-shard path must agree with shards=1"
        );
    }
}
