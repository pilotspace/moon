//! moon#1266 Option 1A: the shard thread writes its own AOF records (one
//! `write(2)` per event-loop iteration) before that iteration's replies.
//!
//! `tests/aof_everysec_kill9_1266.rs` (`MOON_1266_STRICT=1`) is the
//! durability leg. This suite pins what 1A adds around it:
//! - the switch: `MOON_AOF_SHARD_WRITE=1` makes the shard threads write
//!   (`INFO persistence` `aof_shard_writes` grows), `=0` keeps Option 3
//!   (the counter stays 0) — same binary;
//! - the hand-over across rewrites: while non-idempotent `INCR`s stream in
//!   and `BGREWRITEAOF` folds run (the writer takes the append position back
//!   for each fold and hands it over again on the new generation), then once
//!   the last fold has ended a final batch is acked, a kill -9 1 ms after
//!   that ack recovers EXACTLY the acked counts — no acked write lost, none
//!   applied twice. (A kill INSIDE a fold can still lose the records the fold
//!   overlaps: they wait for the post-fold drain, as in Option 3 — see the
//!   production guide.)
//! - `MOON.TXN` / `MOON.TS` / `SELECT` framing moved with the position: a
//!   multi-db stream with a TXN that commits and one left open across the
//!   kill recovers the committed one and rolls the open one back.
//! - no reply-before-write window while the writer owns the append position
//!   outside a fold (W2B-1 of the R2b review): a pipeline sent right after
//!   the FIRST `PING` of a fresh server, and one sent right after `CONFIG SET
//!   appendfsync always` → `everysec`, survive a kill -9 on their last ack
//!   (10 reps with one connection and 10 with 16, each); and leaving `always` under a steady write load hands the
//!   position back to the shard threads (the hold does not livelock).
//!
//! The driver is the binary's default; run the suite again with
//! `MOON_NO_URING=1` in the environment for monoio's epoll driver (the
//! servers inherit it).
//!
//! ```text
//! MOON_BIN=/path/to/moon MOON_DISK_FREE_MIN_PCT=0 \
//!   cargo test --test aof_shard_write_1266 -- --include-ignored --nocapture
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn start_moon(port: u16, dir: &std::path::Path, shards: usize, shard_write: &str) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--auto-aof-rewrite-percentage",
            "0",
            "--disk-free-min-pct",
            "0",
        ])
        .arg("--dir")
        .arg(dir)
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .env("MOON_AOF_SHARD_WRITE", shard_write)
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (build it first, or set MOON_BIN)")
}

fn conn(port: u16) -> common::Conn {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(s) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            drop(s);
            let mut c = common::Conn::open(port);
            if c.send(&["PING"]).contains("PONG") {
                return c;
            }
        }
        assert!(Instant::now() < deadline, "server on {port} never answered");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn info_u64(c: &mut common::Conn, field: &str) -> u64 {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO has no {field}: {info}"))
}

fn wait_loaded(port: u16) -> common::Conn {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut c = conn(port);
        if c.send(&["INFO", "persistence"]).contains("loading:0") {
            return c;
        }
        assert!(Instant::now() < deadline, "server never finished loading");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn kill_after_1ms(server: &mut common::ServerGuard, port: u16) {
    let t = Instant::now();
    while t.elapsed() < Duration::from_millis(1) {
        std::hint::spin_loop();
    }
    server.kill_now();
    common::wait_for_port_down(port);
}

fn switch_decides_who_writes(shards: usize) {
    for (switch, expect_shard_writes) in [("1", true), ("0", false)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_server, port) =
            common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, switch));
        let mut c = conn(port);
        // The writer hands the position over at the top of its first wake;
        // the sleep only lets a slow box get there.
        std::thread::sleep(Duration::from_millis(300));
        let cmds: Vec<Vec<String>> = (0..200)
            .map(|i| vec!["SET".into(), format!("k{i}"), "v".into()])
            .collect();
        let refs: Vec<Vec<&str>> = cmds
            .iter()
            .map(|c| c.iter().map(String::as_str).collect())
            .collect();
        let slices: Vec<&[&str]> = refs.iter().map(Vec::as_slice).collect();
        for chunk in slices.chunks(10) {
            let replies = c.pipeline(chunk);
            assert_eq!(replies.matches("+OK").count(), chunk.len(), "{replies}");
        }
        let writes = info_u64(&mut c, "aof_shard_writes");
        assert_eq!(
            writes > 0,
            expect_shard_writes,
            "MOON_AOF_SHARD_WRITE={switch} shards={shards}: aof_shard_writes={writes}"
        );
    }
}

#[test]
#[ignore = "spawns real servers"]
fn the_switch_decides_who_writes_s1() {
    switch_decides_who_writes(1);
}

#[test]
#[ignore = "spawns real servers"]
fn the_switch_decides_who_writes_s4() {
    switch_decides_who_writes(4);
}

/// INCR counters (spread over the shards) stream in pipelined batches while
/// a second client asks for a BGREWRITEAOF every 150 ms; kill -9 1 ms after
/// the last ack; every counter must come back at exactly its acked value.
fn rewrites_under_load_then_kill9(shards: usize) {
    const KEYS: usize = 8;
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut server, port) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
    let mut c = conn(port);
    std::thread::sleep(Duration::from_millis(300));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rewriter = {
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut r = conn(port);
            let mut asked = 0usize;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // "already in progress" errors are fine: the point is folds
                // overlapping the stream.
                let _ = r.send(&["BGREWRITEAOF"]);
                asked += 1;
                std::thread::sleep(Duration::from_millis(150));
            }
            asked
        })
    };
    let keys: Vec<String> = (0..KEYS).map(|i| format!("ctr:{i}")).collect();
    let mut acked = vec![0i64; KEYS];
    let started = Instant::now();
    let mut round = 0usize;
    while started.elapsed() < Duration::from_millis(2_500) {
        let batch: Vec<[&str; 2]> = (0..40)
            .map(|j| ["INCR", keys[(round + j) % KEYS].as_str()])
            .collect();
        let slices: Vec<&[&str]> = batch.iter().map(|a| a.as_slice()).collect();
        let replies = c.pipeline(&slices);
        let ints = replies.lines().filter(|l| l.starts_with(':')).count();
        assert_eq!(ints, batch.len(), "every INCR acked: {replies}");
        for j in 0..batch.len() {
            acked[(round + j) % KEYS] += 1;
        }
        round += batch.len();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let asked = rewriter.join().expect("rewriter");
    // A fold in progress holds the records it overlaps in the writer's
    // channel until its post-fold drain (the writer has the append position
    // for the whole fold, as in Option 3 — the documented residual). Let the
    // last fold end, then ack one more batch: the shard threads have the
    // position back, so a kill 1 ms after that ack must lose nothing.
    let deadline = Instant::now() + Duration::from_secs(60);
    while info_u64(&mut c, "aof_rewrite_in_progress") != 0 {
        assert!(
            Instant::now() < deadline,
            "the last BGREWRITEAOF never ended"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(200));
    let lane_writes_before = info_u64(&mut c, "aof_shard_writes");
    let batch: Vec<[&str; 2]> = (0..40).map(|j| ["INCR", keys[j % KEYS].as_str()]).collect();
    let slices: Vec<&[&str]> = batch.iter().map(|a| a.as_slice()).collect();
    let replies = c.pipeline(&slices);
    assert_eq!(
        replies.lines().filter(|l| l.starts_with(':')).count(),
        batch.len()
    );
    for j in 0..batch.len() {
        acked[j % KEYS] += 1;
    }
    kill_after_1ms(&mut server, port);
    let lane_writes = lane_writes_before;
    assert!(lane_writes > 0, "1A was not active (aof_shard_writes=0)");
    assert!(asked >= 5, "only {asked} BGREWRITEAOF requests were made");

    let (_server2, port2) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
    let mut c2 = wait_loaded(port2);
    let mut got = Vec::with_capacity(KEYS);
    for k in &keys {
        let r = c2.send(&["GET", k.as_str()]);
        let v: i64 = r
            .lines()
            .nth(1)
            .and_then(|l| l.trim().parse().ok())
            .unwrap_or(0);
        got.push(v);
    }
    eprintln!(
        "moon#1266 1A rewrites under load shards={shards}: {asked} BGREWRITEAOF asked, \
         {lane_writes} shard writes, acked {acked:?}, recovered {got:?}"
    );
    assert_eq!(
        got, acked,
        "shards={shards}: recovered counters differ from the acked ones \
         (lower = acked INCRs lost, higher = INCRs applied twice)"
    );
}

#[test]
#[ignore = "spawns real servers"]
fn rewrites_under_load_then_kill9_recover_exactly_s1() {
    rewrites_under_load_then_kill9(1);
}

#[test]
#[ignore = "spawns real servers"]
fn rewrites_under_load_then_kill9_recover_exactly_s4() {
    rewrites_under_load_then_kill9(4);
}

/// A hash tag whose keys this connection's shard owns (a `TXN` refuses a
/// write to another shard's key), probed with a throwaway transaction.
fn local_tag(c: &mut common::Conn) -> String {
    for i in 0..512 {
        let tag = format!("t{i}");
        assert!(c.send(&["TXN", "BEGIN"]).contains("OK"));
        let r = c.send(&["SET", &format!("{{{tag}}}:probe"), "1"]);
        assert!(c.send(&["TXN", "ABORT"]).contains("OK"));
        if r.contains("OK") {
            return tag;
        }
    }
    panic!("no hash tag is local to this connection's shard");
}

/// SELECTed dbs, a committed TXN and a TXN left open across the kill, all
/// written by the shard threads: the replay must see the same SELECT /
/// MOON.TXN framing the writer would have emitted.
fn framing_moves_with_the_position(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut server, port) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
    let mut c = conn(port);
    std::thread::sleep(Duration::from_millis(300));
    assert!(c.send(&["SELECT", "3"]).contains("OK"));
    assert!(c.send(&["SET", "db3key", "a"]).contains("OK"));
    assert!(c.send(&["SELECT", "0"]).contains("OK"));
    assert!(c.send(&["SET", "db0key", "b"]).contains("OK"));
    // Committed TXN.
    let t1 = format!("{{{}}}:t1", local_tag(&mut c));
    let begin = c.send(&["TXN", "BEGIN"]);
    assert!(begin.contains("OK"), "TXN BEGIN: {begin}");
    assert!(c.send(&["SET", &t1, "committed"]).contains("OK"));
    let commit = c.send(&["TXN", "COMMIT"]);
    assert!(!commit.starts_with('-'), "TXN COMMIT: {commit}");
    // Open TXN, left open across the kill (from a second connection, so
    // the first stays usable for the last plain write).
    let mut t = conn(port);
    let t2 = format!("{{{}}}:t2", local_tag(&mut t));
    assert!(t.send(&["TXN", "BEGIN"]).contains("OK"));
    assert!(t.send(&["SET", &t2, "uncommitted"]).contains("OK"));
    assert!(c.send(&["SET", "after", "x"]).contains("OK"));
    kill_after_1ms(&mut server, port);

    let (_server2, port2) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
    let mut c2 = wait_loaded(port2);
    assert!(c2.send(&["GET", "db0key"]).contains("b"));
    assert!(c2.send(&["GET", "after"]).contains("x"));
    assert!(c2.send(&["GET", &t1]).contains("committed"));
    let open = c2.send(&["GET", &t2]);
    assert!(
        open.starts_with("$-1") || open.starts_with('_'),
        "an open TXN's write must roll back: {open}"
    );
    assert!(
        c2.send(&["GET", "db3key"]).starts_with("$-1"),
        "db3key is not in db 0"
    );
    assert!(c2.send(&["SELECT", "3"]).contains("OK"));
    assert!(c2.send(&["GET", "db3key"]).contains("a"));
}

#[test]
#[ignore = "spawns real servers"]
fn framing_moves_with_the_append_position_s1() {
    framing_moves_with_the_position(1);
}

#[test]
#[ignore = "spawns real servers"]
fn framing_moves_with_the_append_position_s4() {
    framing_moves_with_the_position(4);
}

/// SETs of `n` keys named `{prefix}{i}` (spread over the shards), pipelined
/// in ONE write; every reply must be `+OK`.
fn pipelined_sets(c: &mut common::Conn, prefix: &str, n: usize) {
    let cmds: Vec<[String; 3]> = (0..n)
        .map(|i| ["SET".into(), format!("{prefix}{i}"), i.to_string()])
        .collect();
    let refs: Vec<[&str; 3]> = cmds
        .iter()
        .map(|[a, b, v]| [a.as_str(), b.as_str(), v.as_str()])
        .collect();
    let slices: Vec<&[&str]> = refs.iter().map(|a| a.as_slice()).collect();
    let replies = c.pipeline(&slices);
    assert_eq!(replies.matches("+OK").count(), n, "{replies}");
}

const KILL_REPS: usize = 10;
/// Connections that pipeline at once, each under its own hash tag (so each
/// pipeline lives on ONE shard, and under `--shards 4` some connection's
/// pipeline is local to its own shard: its acks leave with no cross-shard
/// hop to give a writer time).
const CONNS: usize = 16;
/// Each kill test runs `KILL_REPS` reps with ONE connection (its pipeline
/// leaves the soonest after the first `PONG` / the `CONFIG SET`) and
/// `KILL_REPS` with [`CONNS`] (some pipeline is local to its own shard).
const SHAPES: [usize; 2] = [1, CONNS];
const PIPELINE: usize = 200;

/// Key `i` of connection `j`'s pipeline.
fn tagged_key(prefix: &str, j: usize, i: usize) -> String {
    format!("{{{prefix}{j}}}:{i}")
}

/// Kill on ack: every connection (already open) sends a `PIPELINE`-SET
/// pipeline at once and counts its `+OK`s as they arrive; the instant the
/// first connection has all of its acks, the server is SIGKILLed. Returns
/// each connection's acks — every one of them was sent before the kill.
fn pipelines_then_kill_on_first_complete(
    server: &mut common::ServerGuard,
    port: u16,
    streams: Vec<std::net::TcpStream>,
    prefix: &str,
) -> Vec<usize> {
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let acks: std::sync::Arc<Vec<AtomicUsize>> =
        std::sync::Arc::new((0..streams.len()).map(|_| AtomicUsize::new(0)).collect());
    let readers: Vec<_> = streams
        .into_iter()
        .enumerate()
        .map(|(j, mut s)| {
            let mut out = Vec::new();
            for i in 0..PIPELINE {
                let key = tagged_key(prefix, j, i);
                out.extend_from_slice(&common::encode(&["SET", &key, &i.to_string()]));
            }
            let acks = std::sync::Arc::clone(&acks);
            std::thread::spawn(move || {
                s.write_all(&out).expect("send pipeline");
                let mut seen = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            seen.extend_from_slice(&buf[..n]);
                            let oks = seen.windows(5).filter(|w| w == b"+OK\r\n").count();
                            acks[j].store(oks, Ordering::Release);
                            if oks >= PIPELINE {
                                break;
                            }
                        }
                    }
                }
            })
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !acks.iter().any(|a| a.load(Ordering::Acquire) >= PIPELINE) {
        assert!(Instant::now() < deadline, "no pipeline was acked in 30 s");
        std::hint::spin_loop();
    }
    server.kill_now();
    common::wait_for_port_down(port);
    for r in readers {
        r.join().expect("reader");
    }
    acks.iter().map(|a| a.load(Ordering::Acquire)).collect()
}

/// Open `n` raw connections at once, then PING them all at once (in the
/// boot test these PONGs are the server's first).
fn raw_conns(port: u16, n: usize) -> Vec<std::net::TcpStream> {
    use std::io::{Read, Write};
    let mut streams: Vec<std::net::TcpStream> = (0..n)
        .map(|_| {
            let s = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
            s.set_nodelay(true).expect("nodelay");
            s
        })
        .collect();
    for s in &mut streams {
        s.write_all(&common::encode(&["PING"])).expect("ping");
    }
    for s in &mut streams {
        let mut b = [0u8; 7];
        s.read_exact(&mut b).expect("pong");
        assert_eq!(&b, b"+PONG\r\n");
    }
    streams
}

/// Restart on `dir`; count the acked keys (connection `j`'s first
/// `acked[j]`) that did not come back with their value.
fn lost_after_restart(
    dir: &std::path::Path,
    shards: usize,
    prefix: &str,
    acked: &[usize],
) -> usize {
    let (_server, port) =
        common::spawn_listening_guarded(|port| start_moon(port, dir, shards, "1"));
    let mut c = wait_loaded(port);
    let mut lost = 0;
    for (j, &n) in acked.iter().enumerate() {
        if n == 0 {
            continue;
        }
        let keys: Vec<String> = (0..n).map(|i| tagged_key(prefix, j, i)).collect();
        let gets: Vec<[&str; 2]> = keys.iter().map(|k| ["GET", k.as_str()]).collect();
        let slices: Vec<&[&str]> = gets.iter().map(|a| a.as_slice()).collect();
        let replies = c.pipeline(&slices);
        // Bulk replies: `$<len>` then the value; nil is `$-1` (or `_`).
        let mut values = Vec::with_capacity(n);
        let mut lines = replies.lines();
        while let Some(l) = lines.next() {
            if l.starts_with("$-1") || l.starts_with('_') {
                values.push(None);
            } else if l.starts_with('$') {
                values.push(lines.next().map(str::to_owned));
            }
        }
        assert_eq!(values.len(), n, "{replies}");
        lost += values
            .iter()
            .enumerate()
            .filter(|(i, v)| v.as_deref() != Some(i.to_string().as_str()))
            .count();
    }
    lost
}

/// The boot window: right after the first `PONG`s, 1 or `CONNS` connections each
/// send a SET pipeline; kill -9 the instant one has all its acks. The writer
/// may not have handed the append position over yet — the replies must
/// then wait for its acks (the lane is held from attach to the first
/// hand-over).
fn boot_window_kill_on_ack(shards: usize) {
    let mut lossy = Vec::new();
    for conns in SHAPES {
        for rep in 0..KILL_REPS {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut server, port) =
                common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
            let streams = raw_conns(port, conns); // the server's first PONGs
            let acked = pipelines_then_kill_on_first_complete(&mut server, port, streams, "boot");
            let lost = lost_after_restart(dir.path(), shards, "boot", &acked);
            if lost > 0 {
                lossy.push((conns, rep, lost, acked.iter().sum::<usize>()));
            }
        }
    }
    assert!(
        lossy.is_empty(),
        "shards={shards}: acked writes lost after a kill on ack in the boot window \
         ((connections, rep, keys lost, keys acked): {lossy:?})"
    );
}

#[test]
#[ignore = "spawns real servers"]
fn boot_window_kill_on_ack_loses_nothing_s1() {
    boot_window_kill_on_ack(1);
}

#[test]
#[ignore = "spawns real servers"]
fn boot_window_kill_on_ack_loses_nothing_s4() {
    boot_window_kill_on_ack(4);
}

/// The policy-switch window: `CONFIG SET appendfsync always` (writes acked
/// under it, so every writer has taken the position back), then `everysec`,
/// then at once 1 or `CONNS` SET pipelines; kill -9 the instant one has all its
/// acks.
fn after_always_kill_on_ack(shards: usize) {
    let mut lossy = Vec::new();
    for conns in SHAPES {
        for rep in 0..KILL_REPS {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut server, port) =
                common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
            let mut c = conn(port);
            let streams = raw_conns(port, conns);
            std::thread::sleep(Duration::from_millis(100));
            assert!(
                c.send(&["CONFIG", "SET", "appendfsync", "always"])
                    .contains("OK")
            );
            pipelined_sets(&mut c, "warm", 64);
            assert!(
                c.send(&["CONFIG", "SET", "appendfsync", "everysec"])
                    .contains("OK")
            );
            let acked = pipelines_then_kill_on_first_complete(&mut server, port, streams, "cfg");
            let lost = lost_after_restart(dir.path(), shards, "cfg", &acked);
            if lost > 0 {
                lossy.push((conns, rep, lost, acked.iter().sum::<usize>()));
            }
        }
    }
    assert!(
        lossy.is_empty(),
        "shards={shards}: acked writes lost after a kill on ack right after leaving \
         `always` ((connections, rep, keys lost, keys acked): {lossy:?})"
    );
}

#[test]
#[ignore = "spawns real servers"]
fn after_always_kill_on_ack_loses_nothing_s1() {
    after_always_kill_on_ack(1);
}

#[test]
#[ignore = "spawns real servers"]
fn after_always_kill_on_ack_loses_nothing_s4() {
    after_always_kill_on_ack(4);
}

/// Leaving `always` while 8 connections keep writing: the writers must hand
/// the position back to the shard threads (`aof_shard_writes` grows again)
/// even though the held producers keep their channel busy.
fn leaving_always_under_load_hands_over(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_server, port) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
    let mut c = conn(port);
    assert!(
        c.send(&["CONFIG", "SET", "appendfsync", "always"])
            .contains("OK")
    );
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..8)
        .map(|w| {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut c = conn(port);
                let mut i = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    pipelined_sets(&mut c, &format!("load{w}:{i}:"), 16);
                    i += 1;
                }
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(300));
    let under_always = info_u64(&mut c, "aof_shard_writes");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        info_u64(&mut c, "aof_shard_writes"),
        under_always,
        "under `always` the writer owns the position"
    );
    assert!(
        c.send(&["CONFIG", "SET", "appendfsync", "everysec"])
            .contains("OK")
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let handed_over = loop {
        std::thread::sleep(Duration::from_millis(50));
        if info_u64(&mut c, "aof_shard_writes") > under_always {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
    };
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in writers {
        w.join().expect("writer");
    }
    assert!(
        handed_over,
        "shards={shards}: 5 s after leaving `always` under load the shard threads still \
         do not write their own records"
    );
}

#[test]
#[ignore = "spawns real servers"]
fn leaving_always_under_load_hands_over_s1() {
    leaving_always_under_load_hands_over(1);
}

#[test]
#[ignore = "spawns real servers"]
fn leaving_always_under_load_hands_over_s4() {
    leaving_always_under_load_hands_over(4);
}
