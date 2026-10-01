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
//!   for each fold and hands it over again on the new generation), a kill -9
//!   1 ms after the last ack recovers EXACTLY the acked counts — no acked
//!   write lost, none applied twice;
//! - `MOON.TXN` / `MOON.TS` / `SELECT` framing moved with the position: a
//!   multi-db stream with a TXN that commits and one left open across the
//!   kill recovers the committed one and rolls the open one back.
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
        // The writer hands the position over at the end of its first wake.
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
    let lane_writes = info_u64(&mut c, "aof_shard_writes");
    // The last pipelined batch's acks are in hand: kill now.
    kill_after_1ms(&mut server, port);
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
    let begin = c.send(&["TXN", "BEGIN"]);
    assert!(begin.contains("OK"), "TXN BEGIN: {begin}");
    assert!(c.send(&["SET", "t1", "committed"]).contains("OK"));
    let commit = c.send(&["TXN", "COMMIT"]);
    assert!(!commit.starts_with('-'), "TXN COMMIT: {commit}");
    // Open TXN, left open across the kill (from a second connection, so
    // the first stays usable for the last plain write).
    let mut t = conn(port);
    assert!(t.send(&["TXN", "BEGIN"]).contains("OK"));
    assert!(t.send(&["SET", "t2", "uncommitted"]).contains("OK"));
    assert!(c.send(&["SET", "after", "x"]).contains("OK"));
    kill_after_1ms(&mut server, port);

    let (_server2, port2) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards, "1"));
    let mut c2 = wait_loaded(port2);
    assert!(c2.send(&["GET", "db0key"]).contains("b"));
    assert!(c2.send(&["GET", "after"]).contains("x"));
    assert!(c2.send(&["GET", "t1"]).contains("committed"));
    let open = c2.send(&["GET", "t2"]);
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
