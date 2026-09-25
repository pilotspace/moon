//! moon#1228, end to end: writers that change a key while a BGSAVE epoch is
//! still writing it must leave the snapshot holding the EPOCH-START keyspace.
//! Each test restores the snapshot ALONE (`--appendonly no`, SIGKILL, restart)
//! and compares it with the keyspace the save started from.
//!
//! The overlap between the save and the writes is observed, never assumed
//! (the `perf_ws8_mset_bgsave_capture` method): an attempt waits until every
//! shard has armed the epoch (its `shard-<id>.rrdshard.tmp` exists), pipelines
//! each round of writes with an `INFO persistence` probe, and counts the rounds
//! that ran while the save was still in progress. The first rounds run while
//! the walk is HELD (`MOON_TEST_SNAPSHOT_HOLD_FILE`, part 3b's test hook), so
//! they land inside the epoch by construction; the rest race the released
//! walk, so captures also interleave with written ranges. An attempt with too
//! few is retried on a doubled keyspace; only an attempt that overlapped is
//! judged.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws16_bgsave_capture`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

/// While this file exists a running save's epoch stays armed but its walk
/// does not advance (`MOON_TEST_SNAPSHOT_HOLD_FILE`, test-only).
fn hold_file(dir: &Path) -> std::path::PathBuf {
    dir.join("snapshot.hold")
}

fn spawn(dir: &Path, shards: usize) -> (ServerGuard, u16) {
    spawn_with_maxmemory(dir, shards, "0")
}

/// [`spawn`] under `--maxmemory <maxmemory>` with `noeviction`.
fn spawn_with_maxmemory(dir: &Path, shards: usize, maxmemory: &str) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .env("MOON_TEST_SNAPSHOT_HOLD_FILE", hold_file(dir))
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
                "--maxmemory",
                maxmemory,
                "--maxmemory-policy",
                "noeviction",
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

/// Removes the test's `--dir` when the test ends, pass or fail: a failed run
/// otherwise leaves a multi-megabyte snapshot behind on a shared box. Declare
/// it BEFORE the server guard so the server is reaped first.
struct DirGuard(std::path::PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn key(j: u64) -> String {
    format!("pre:{j:08}")
}

/// Empty every database, then set `pre:<j>` = `v<j>` for `j` in `0..n` in
/// db 0: the keyspace the next BGSAVE must capture.
fn reset(c: &mut Conn, n: u64) {
    assert!(c.send(&["FLUSHALL"]).starts_with("+OK"));
    let mut i = 0;
    while i < n {
        let end = (i + 1000).min(n);
        let mut out = Vec::new();
        for j in i..end {
            out.extend_from_slice(&encode(&["SET", &key(j), &format!("v{j}")]));
        }
        c.sock.write_all(&out).unwrap();
        let reply = c.read_replies((end - i) as usize);
        assert!(!reply.contains('-'), "preload refused: {reply:.200}");
        i = end;
    }
}

fn bgsave_in_progress(c: &mut Conn) -> bool {
    c.send(&["INFO", "persistence"])
        .lines()
        .any(|l| l.trim() == "rdb_bgsave_in_progress:1")
}

fn last_bgsave_status(c: &mut Conn) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix("rdb_last_bgsave_status:"))
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

/// Keys of db 0 whose restored value is not their epoch-start `v<j>`.
fn wrong_pre_keys(c: &mut Conn, n: u64) -> Vec<String> {
    assert!(c.send(&["SELECT", "0"]).starts_with("+OK"));
    let mut wrong = Vec::new();
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
        let mut out = Vec::new();
        for j in i..end {
            out.extend_from_slice(&encode(&["GET", &key(j)]));
        }
        c.sock.write_all(&out).unwrap();
        let reply = c.read_replies((end - i) as usize);
        let mut lines = reply.split("\r\n");
        for j in i..end {
            let head = lines.next().unwrap_or("");
            let body = if head == "$-1" {
                "<nil>"
            } else {
                lines.next().unwrap_or("")
            };
            if body != format!("v{j}") && wrong.len() < 10_000 {
                wrong.push(format!("{}={body}", key(j)));
            }
        }
        i = end;
    }
    wrong
}

fn dbsize(c: &mut Conn, db: usize) -> i64 {
    assert!(c.send(&["SELECT", &db.to_string()]).starts_with("+OK"));
    let reply = c.send(&["DBSIZE"]);
    reply
        .trim()
        .strip_prefix(':')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("DBSIZE: {reply:?}"))
}

/// Attempts before the test gives up on overlapping a save with the writes.
const MAX_ATTEMPTS: u32 = 5;
/// Keys in db 0 in the first attempt; each retry doubles it, up to 8x.
const FIRST_KEYS: u64 = 200_000;
/// Rounds of writes that must land inside one epoch for a stream of writes
/// to count (a single bulk write such as `WS DROP` needs just its own round).
const MIN_OVERLAP: u64 = 16;

/// Every shard armed the save's epoch: a shard creates its
/// `shard-<id>.rrdshard.tmp` in the same synchronous stretch of its thread
/// that arms its pre-image capture, and only that thread writes its keys.
fn wait_until_armed(dir: &Path, shards: usize, probe: &mut Conn) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if (0..shards).all(|s| dir.join(format!("shard-{s}.rrdshard.tmp")).exists()) {
            return true;
        }
        if !bgsave_in_progress(probe) {
            return false;
        }
        assert!(Instant::now() < deadline, "BGSAVE never armed every shard");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_bgsave_done(probe: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(300);
    while bgsave_in_progress(probe) {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// One round of writes: `(conn, n, round)` → the commands to pipeline. The
/// harness appends the `INFO persistence` probe and checks every reply is not
/// an error.
type Round<'a> = &'a mut dyn FnMut(u64, u64) -> Vec<Vec<String>>;

/// Run rounds of writes while a BGSAVE is in flight, retrying on a doubled
/// keyspace until `min_overlap` rounds landed inside the epoch. Returns the
/// keyspace size and the overlapped round count of the judged attempt; the
/// file on disk is that attempt's (each save replaces it).
fn writes_during_bgsave(
    dir: &Path,
    shards: usize,
    c: &mut Conn,
    probe: &mut Conn,
    min_overlap: u64,
    reset: &mut dyn FnMut(&mut Conn, u64),
    round: Round<'_>,
) -> (u64, u64) {
    let mut attempts: Vec<String> = Vec::new();
    for attempt in 1..=MAX_ATTEMPTS {
        let n = FIRST_KEYS << (attempt - 1).min(3);
        reset(c, n);
        for s in 0..shards {
            let _ = std::fs::remove_file(dir.join(format!("shard-{s}.rrdshard.tmp")));
        }
        let started = Instant::now();
        // Beside the other tests of this file, a 4-shard MQ attempt saw no
        // round inside a save of 220-670 ms in 5 attempts of 5; the hold
        // puts the first `min_overlap` rounds inside it by construction.
        std::fs::write(hold_file(dir), b"").expect("create the hold file");
        assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
        if !wait_until_armed(dir, shards, probe) {
            let _ = std::fs::remove_file(hold_file(dir));
            wait_bgsave_done(probe);
            attempts.push(format!("{n} keys: the save ended before every shard armed"));
            continue;
        }
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut during = 0u64;
        for r in 0.. {
            if r == min_overlap {
                let _ = std::fs::remove_file(hold_file(dir));
            }
            let cmds = round(n, r);
            let mut parts: Vec<Vec<&str>> = cmds
                .iter()
                .map(|c| c.iter().map(String::as_str).collect())
                .collect();
            parts.push(vec!["INFO", "persistence"]);
            let refs: Vec<&[&str]> = parts.iter().map(Vec::as_slice).collect();
            let reply = c.pipeline(&refs);
            assert!(
                !reply.lines().any(|l| l.starts_with('-')),
                "a write was refused: {reply:.300}"
            );
            if !reply
                .lines()
                .any(|l| l.trim() == "rdb_bgsave_in_progress:1")
            {
                break;
            }
            during += 1;
            assert!(Instant::now() < deadline, "BGSAVE outlived the writer");
        }
        let _ = std::fs::remove_file(hold_file(dir));
        assert_eq!(last_bgsave_status(probe), "ok", "BGSAVE failed");
        // moon#1228: INFO `current_cow_size` (redis's field) reports what a
        // save holds for its own sake, and nothing once it is over.
        let info = probe.send(&["INFO", "persistence"]);
        assert!(
            info.lines().any(|l| l.trim() == "current_cow_size:0"),
            "no save in flight: current_cow_size must be present and 0: {info:.400}"
        );
        attempts.push(format!(
            "{n} keys: {during} rounds inside the epoch, save took {} ms",
            started.elapsed().as_millis()
        ));
        if during >= min_overlap {
            eprintln!("attempts: {attempts:#?}");
            return (n, during);
        }
    }
    panic!("no BGSAVE in {MAX_ATTEMPTS} attempts overlapped {min_overlap} rounds: {attempts:#?}");
}

fn cmd(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| (*p).to_string()).collect()
}

/// `MOVE` out of db 0 and `COPY … DB 2` while the save walks db 0: the
/// restored snapshot must hold every key in db 0 with its epoch-start value,
/// and nothing in db 1 or db 2. Before moon#1228 neither command captured a
/// pre-image on either database, so a moved key came back in db 1 (and, when
/// its db-0 range was already written, in db 0 too) and a copy came back in
/// db 2.
fn move_and_copy_during_bgsave(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws16-move-copy-s{shards}"));
    let _cleanup = DirGuard(dir.clone());
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    let mut probe = Conn::open(port);
    const PER_ROUND: u64 = 16;
    let (n, during) = writes_during_bgsave(
        &dir,
        shards,
        &mut c,
        &mut probe,
        MIN_OVERLAP,
        &mut reset,
        &mut |n, r| {
            let half = n / 2;
            let mut out = Vec::new();
            for i in 0..PER_ROUND {
                let j = (r * PER_ROUND + i) % half;
                out.push(cmd(&["MOVE", &key(j), "1"]));
                let k = half + j;
                out.push(cmd(&["COPY", &key(k), &key(k), "DB", "2"]));
            }
            out
        },
    );

    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir, shards);
    let mut c2 = Conn::open(port2);
    let in_db1 = dbsize(&mut c2, 1);
    let in_db2 = dbsize(&mut c2, 2);
    let wrong = wrong_pre_keys(&mut c2, n);
    assert!(
        wrong.is_empty() && in_db1 == 0 && in_db2 == 0,
        "restored snapshot (MOVE/COPY in {during} rounds during the save, {n} keys, \
         --shards {shards}) is not the epoch-start keyspace: {} db-0 keys missing or wrong \
         (first: {:?}), db 1 holds {in_db1} keys, db 2 holds {in_db2} keys (both must be 0)",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
}

#[test]
fn move_and_copy_during_bgsave_single_shard() {
    move_and_copy_during_bgsave(1);
}

#[test]
fn move_and_copy_during_bgsave_four_shards() {
    move_and_copy_during_bgsave(4);
}

/// `WS DROP` while the save walks db 0: the restored snapshot must still hold
/// every workspace key (it existed when the save started). Before moon#1228
/// the drop sweep deleted them with no pre-image, so every workspace key
/// whose range the save had not reached yet was missing from the file.
fn workspace_drop_during_bgsave(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws16-ws-drop-s{shards}"));
    let _cleanup = DirGuard(dir.clone());
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    let mut probe = Conn::open(port);
    const WS_KEYS: u64 = 4000;
    let ws_id = std::cell::RefCell::new(String::new());
    let (n, _) = writes_during_bgsave(
        &dir,
        shards,
        &mut c,
        &mut probe,
        1,
        &mut |c, n| {
            reset(c, n);
            let created = c.send(&["WS", "CREATE", &format!("ws16-{n}")]);
            let id = created
                .lines()
                .nth(1)
                .unwrap_or_else(|| panic!("WS CREATE: {created:?}"))
                .to_string();
            let mut bound = Conn::open(port);
            assert!(bound.send(&["WS", "AUTH", &id]).starts_with("+OK"));
            let mut out = Vec::new();
            for i in 0..WS_KEYS {
                out.extend_from_slice(&encode(&["SET", &format!("w:{i}"), "ws"]));
            }
            bound.sock.write_all(&out).unwrap();
            let reply = bound.read_replies(WS_KEYS as usize);
            assert!(!reply.contains('-'), "workspace preload: {reply:.200}");
            *ws_id.borrow_mut() = id;
        },
        &mut |_, r| {
            if r == 0 {
                vec![cmd(&["WS", "DROP", &ws_id.borrow()])]
            } else {
                vec![cmd(&["PING"])]
            }
        },
    );

    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir, shards);
    let mut c2 = Conn::open(port2);
    let restored = dbsize(&mut c2, 0);
    let expected = (n + WS_KEYS) as i64;
    assert_eq!(
        restored, expected,
        "restored snapshot of a save crossed by WS DROP (--shards {shards}) must hold the \
         {n} plain keys and the {WS_KEYS} workspace keys it started with"
    );
}

#[test]
fn workspace_drop_during_bgsave_single_shard() {
    workspace_drop_during_bgsave(1);
}

#[test]
fn workspace_drop_during_bgsave_four_shards() {
    workspace_drop_during_bgsave(4);
}

/// `MQ PUSH` / `MQ POP` / `MQ ACK` while the save walks db 0: every queue
/// must come back with the three messages it held when the save started.
/// Before moon#1228 the owner-side MQ subcommands wrote the stream with no
/// pre-image, so a queue whose range the save had not reached yet came back
/// with the mid-save push in it.
fn mq_during_bgsave(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws16-mq-s{shards}"));
    let _cleanup = DirGuard(dir.clone());
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    let mut probe = Conn::open(port);
    const QUEUES: u64 = 64;
    let q = |i: u64| format!("mq:{i:03}");
    writes_during_bgsave(
        &dir,
        shards,
        &mut c,
        &mut probe,
        1,
        &mut |c, n| {
            reset(c, n);
            for i in 0..QUEUES {
                assert!(c.send(&["MQ", "CREATE", &q(i)]).starts_with("+OK"));
                for _ in 0..3 {
                    let r = c.send(&["MQ", "PUSH", &q(i), "f", "v"]);
                    assert!(r.starts_with('$'), "MQ PUSH: {r:?}");
                }
            }
        },
        &mut |_, r| {
            if r > 0 {
                return vec![cmd(&["PING"])];
            }
            let mut out = Vec::new();
            for i in 0..QUEUES {
                out.push(cmd(&["MQ", "PUSH", &q(i), "f", "mid-save"]));
                out.push(cmd(&["MQ", "POP", &q(i), "COUNT", "1"]));
            }
            out
        },
    );

    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir, shards);
    let mut c2 = Conn::open(port2);
    let mut wrong = Vec::new();
    for i in 0..QUEUES {
        let len = c2.send(&["XLEN", &q(i)]);
        let pending = c2.send(&["XPENDING", &q(i), "__mq_consumers"]);
        if len.trim() != ":3" || !pending.starts_with("*4\r\n:0\r\n") {
            wrong.push(format!(
                "{}: XLEN {} XPENDING {:.24}",
                q(i),
                len.trim(),
                pending.replace("\r\n", " ")
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "restored snapshot of a save crossed by MQ PUSH/POP (--shards {shards}): {} of \
         {QUEUES} queues are not at their epoch-start state (first: {:?})",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
}

#[test]
fn mq_during_bgsave_single_shard() {
    mq_during_bgsave(1);
}

#[test]
fn mq_during_bgsave_four_shards() {
    mq_during_bgsave(4);
}

fn info_int(c: &mut Conn, section: &str, field: &str) -> i64 {
    let info = c.send(&["INFO", section]);
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no {field} in INFO {section}: {info:.300}"))
}

/// moon#1228 review 5 (the reviewer's reproduction, adopted): fill a
/// database, FLUSHDB it, database after database, while one BGSAVE is held
/// open. The FLUSHDB freeze kept the table AS FLUSHED — every post-epoch
/// row, all of them tombstoned and useless to the file — so under
/// `--maxmemory 64mb` 8 x (40 MB of SETs + FLUSHDB) held 344,782,240 B in
/// `current_cow_size` and grew RSS by 341 MB while `used_memory` stayed at
/// the epoch-start 302,165 B: `maxmemory` never saw it. The drain now trims
/// a frozen table to its epoch-start rows: here dbs 1-8 were empty at epoch
/// start, so the epoch holds nothing for them. The save completes, and the
/// restored file is the epoch-start keyspace (db 0's 2,000 keys, dbs 1-8
/// empty).
#[test]
fn flushdb_after_post_epoch_inserts_holds_only_the_epoch_start_rows() {
    let dir = common::unique_test_dir("ws16-flushdb-bound");
    let _cleanup = DirGuard(dir.clone());
    let (mut server, port) = spawn_with_maxmemory(&dir, 1, "64mb");
    let mut c = Conn::open(port);
    let mut out = Vec::new();
    for j in 0..2000 {
        out.extend_from_slice(&encode(&["SET", &format!("pre:{j:06}"), "v"]));
    }
    c.sock.write_all(&out).unwrap();
    let _ = c.read_replies(2000);
    std::thread::sleep(Duration::from_millis(300)); // used_memory settles
    let start_used = info_int(&mut c, "memory", "used_memory");
    let rss_before = info_int(&mut c, "memory", "used_memory_rss");

    std::fs::write(hold_file(&dir), b"").expect("create the hold file");
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    assert!(wait_until_armed(&dir, 1, &mut c), "the held save ended");
    let value = "x".repeat(4096);
    for db in 1..=8 {
        assert!(c.send(&["SELECT", &db.to_string()]).starts_with("+OK"));
        let mut i = 0;
        while i < 10_000 {
            let mut out = Vec::new();
            for j in i..i + 500 {
                out.extend_from_slice(&encode(&["SET", &format!("post:{db}:{j}"), &value]));
            }
            c.sock.write_all(&out).unwrap();
            let replies = c.read_replies(500);
            assert!(
                !replies.contains("-OOM"),
                "db {db}: refused: {replies:.200}"
            );
            i += 500;
        }
        assert!(c.send(&["FLUSHDB"]).starts_with("+OK"));
    }
    std::thread::sleep(Duration::from_millis(200)); // the drains run
    let used = info_int(&mut c, "memory", "used_memory");
    let rss = info_int(&mut c, "memory", "used_memory_rss");
    let cow = info_int(&mut c, "persistence", "current_cow_size");
    assert!(bgsave_in_progress(&mut c), "the hold must still hold");
    eprintln!(
        "epoch-start used_memory {start_used}; after 8 x (40 MB SET + FLUSHDB): used_memory \
         {used}, current_cow_size {cow}, RSS {rss_before} -> {rss} (+{} MB)",
        (rss - rss_before) >> 20
    );
    std::fs::remove_file(hold_file(&dir)).expect("release the hold");
    assert!(
        cow <= start_used,
        "the held save holds {cow} B (current_cow_size) after 8 x (40 MB SET + FLUSHDB); the \
         epoch-start dataset is {start_used} B"
    );
    assert!(
        rss - rss_before < 150 << 20,
        "RSS grew {} MB under --maxmemory 64mb while used_memory is {used}",
        (rss - rss_before) >> 20
    );
    wait_bgsave_done(&mut c);
    assert_eq!(last_bgsave_status(&mut c), "ok", "the save must complete");

    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn_with_maxmemory(&dir, 1, "64mb");
    let mut c2 = Conn::open(port2);
    assert_eq!(dbsize(&mut c2, 0), 2000, "db 0: the epoch-start keys");
    for db in 1..=8 {
        assert_eq!(dbsize(&mut c2, db), 0, "db {db} was empty at epoch start");
    }
}
