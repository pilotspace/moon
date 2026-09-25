//! WS12 end to end, against a real server: a BGSAVE taken while clients keep
//! inserting (so the DashTable keeps splitting mid-epoch) must restore every
//! key that existed before the save started, with its value (moon#1216);
//! a FLUSHDB / FLUSHALL / SWAPDB landing mid-BGSAVE must neither crash the
//! shard (moon#1224) nor publish a mixed file — since moon#1228 a FLUSHDB or
//! SWAPDB save completes with the pre-change image, and a FLUSHALL fails the
//! save, as redis's do; and a save that IS aborted (a replica full resync)
//! must not let its writer corrupt the NEXT one (moon#1227 review F1).
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws12_bgsave_split`
//! (falls back to the binary Cargo built for this run).
//!
//! Red on the base `f32546c` binary: the split test restores with thousands
//! of epoch-start keys missing; the FLUSH/SWAPDB test finds the server dead.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
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
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            // The server logs to stdout; keep it with stderr in the test's
            // own directory, so a test can see what the server said.
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

/// Load `n` keys `pre:<i>` = `v<i>`, pipelined.
fn preload(c: &mut Conn, n: u64) {
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
        let keys: Vec<(String, String)> = (i..end)
            .map(|j| (format!("pre:{j:08}"), format!("v{j}")))
            .collect();
        let cmds: Vec<Vec<&str>> = keys
            .iter()
            .map(|(k, v)| vec!["SET", k.as_str(), v.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        assert!(!reply.contains('-'), "preload refused: {reply:.200}");
        i = end;
    }
}

fn info_field(c: &mut Conn, field: &str) -> Option<String> {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
}

/// Wait until shard 0 has ARMED the BGSAVE epoch just requested, so a
/// command sent next lands inside it. The shard spawns the writer that
/// creates `shard-0.rrdshard.tmp` in the same synchronous stretch that arms
/// the epoch (part 3b's wait; `perf_ws8_mset_bgsave_capture` relies on the
/// same). A fixed sleep instead let a loaded runner's tick arm the epoch
/// only after the command. `dir` must hold no temp file from an earlier save.
fn wait_epoch_armed(dir: &std::path::Path) {
    let tmp = dir.join("shard-0.rrdshard.tmp");
    let armed_by = Instant::now() + Duration::from_secs(30);
    while !tmp.exists() {
        assert!(
            Instant::now() < armed_by,
            "the BGSAVE never armed its epoch"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Wait for the in-flight BGSAVE to finish; returns `rdb_last_bgsave_status`.
fn wait_bgsave(c: &mut Conn, budget: Duration) -> String {
    let deadline = Instant::now() + budget;
    loop {
        if info_field(c, "rdb_bgsave_in_progress").as_deref() == Some("0") {
            return info_field(c, "rdb_last_bgsave_status").unwrap_or_default();
        }
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Count `pre:` keys that are missing or hold another value.
fn wrong_pre_keys(c: &mut Conn, n: u64) -> u64 {
    let mut wrong = 0u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + 1000).min(n);
        let keys: Vec<String> = (i..end).map(|j| format!("pre:{j:08}")).collect();
        let cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        // Each reply is `$<len>\r\nv<j>\r\n` or `$-1\r\n`.
        let mut lines = reply.split("\r\n");
        for j in i..end {
            let head = lines.next().unwrap_or("");
            if head == "$-1" {
                wrong += 1;
                continue;
            }
            let body = lines.next().unwrap_or("");
            if body != format!("v{j}") {
                wrong += 1;
            }
        }
        i = end;
    }
    wrong
}

/// Keep inserting fresh keys (forcing DashTable splits) until `stop`.
fn start_inserter(
    port: u16,
    stop: Arc<AtomicBool>,
) -> (std::thread::JoinHandle<()>, Arc<AtomicU64>) {
    let written = Arc::new(AtomicU64::new(0));
    let counter = written.clone();
    let handle = std::thread::spawn(move || {
        let mut c = Conn::open(port);
        let mut j = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let mut batch = Vec::new();
            for _ in 0..100 {
                batch.extend_from_slice(&encode(&["SET", &format!("new:{j:010}"), "n"]));
                j += 1;
            }
            c.sock.write_all(&batch).unwrap();
            c.read_replies(100);
            counter.store(j, Ordering::Relaxed);
        }
    });
    (handle, written)
}

fn split_during_bgsave_keeps_every_pre_epoch_key(shards: usize) {
    let dir = common::unique_test_dir("ws12-split");
    std::fs::create_dir_all(&dir).unwrap();
    // Per-shard size fixed, so every shard's epoch lasts ~the same number of
    // ticks (a budgeted tick writes ~1,024 entries) whatever the shard count.
    let n = 200_000u64 * shards as u64;
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    preload(&mut c, n);

    let stop = Arc::new(AtomicBool::new(false));
    let (inserter, inserted) = start_inserter(port, stop.clone());
    // Let the inserter get going so the epoch starts under load.
    while inserted.load(Ordering::Relaxed) < 20_000 {
        std::thread::sleep(Duration::from_millis(5));
    }
    let before = inserted.load(Ordering::Relaxed);
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    let status = wait_bgsave(&mut c, Duration::from_secs(120));
    let during = inserted.load(Ordering::Relaxed) - before;
    stop.store(true, Ordering::Relaxed);
    inserter.join().unwrap();
    assert_eq!(status, "ok", "BGSAVE failed");
    // Overlap guard, not a volume target: a release-fast server takes 10^5+
    // inserts per epoch here; an unoptimized one sharing 4 vCPUs with two
    // sibling servers can take under 1,000. The split cases themselves are
    // pinned deterministically by `persistence::snapshot::split_epoch_tests`.
    assert!(
        during >= 200,
        "only {during} inserts landed during the BGSAVE — the epoch saw no insert load"
    );

    // Crash, restart from the snapshot alone.
    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir, shards);
    let mut c2 = Conn::open(port2);
    let wrong = wrong_pre_keys(&mut c2, n);
    assert_eq!(
        wrong, 0,
        "--shards {shards}: {wrong} of {n} keys that existed before BGSAVE are missing or \
         changed after restoring its snapshot ({during} inserts during the epoch)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn split_during_bgsave_keeps_every_pre_epoch_key_shards_1() {
    split_during_bgsave_keeps_every_pre_epoch_key(1);
}

#[test]
fn split_during_bgsave_keeps_every_pre_epoch_key_shards_4() {
    split_during_bgsave_keeps_every_pre_epoch_key(4);
}

/// moon#1224, moon#1228: each whole-table write, issued while the BGSAVE
/// epoch is still writing (thousands of segments to go), must leave the
/// server serving (it panicked the shard before moon#1224).
///
/// A FLUSHDB or SWAPDB must let the save complete with the keyspace it
/// started from — the save used to be aborted (`err`) instead, so a workload
/// flushing more often than one save takes never saved at all. Restored
/// alone after SIGKILL: every `pre:` key in db 0 with its value, and nothing
/// in db 1.
///
/// A FLUSHALL fails the save, as redis's does (`flushAllDataAndResetRDB`
/// kills the RDB child): `err`, no file published, the abort logged.
#[test]
fn table_swaps_during_bgsave_keep_the_save_point_in_time() {
    const N: u64 = 600_000;
    for (cmd, aborts) in [
        (&["FLUSHALL"][..], true),
        (&["FLUSHDB"][..], false),
        (&["FLUSHDB", "ASYNC"][..], false),
        (&["SWAPDB", "0", "1"][..], false),
    ] {
        let dir = common::unique_test_dir("ws12-swap");
        std::fs::create_dir_all(&dir).unwrap();
        let (mut server, port) = spawn(&dir, 1);
        let mut c = Conn::open(port);
        preload(&mut c, N);
        assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
        // At ~1,024 entries per 1 ms tick, 600K keys keep the epoch open for
        // ~0.6 s after it is armed.
        wait_epoch_armed(&dir);
        let reply = c.pipeline(&[cmd, &["INFO", "persistence"]]);
        assert!(reply.starts_with('+'), "{cmd:?} refused: {reply:.200}");
        assert!(
            reply.contains("rdb_bgsave_in_progress:1"),
            "fixture: the save ended before {cmd:?} landed"
        );
        // Writes after the change land in the post-epoch tables.
        assert!(c.send(&["SET", "pre:00000007", "post"]).starts_with('+'));
        // The shard must still be serving (it panicked here before).
        std::thread::sleep(Duration::from_millis(50));
        let mut probe = Conn::open(port);
        assert!(
            probe.send(&["PING"]).contains("PONG"),
            "{cmd:?} during BGSAVE killed the server"
        );
        let status = wait_bgsave(&mut probe, Duration::from_secs(120));
        if aborts {
            assert_eq!(
                status, "err",
                "{cmd:?} crossed the epoch: the save must fail, as redis's does"
            );
            let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
            assert!(
                log.contains("aborted: FLUSHALL cleared databases"),
                "{cmd:?}: the abort was not logged"
            );
            assert!(
                !dir.join("shard-0.rrdshard").exists(),
                "{cmd:?}: the failed save published a file"
            );
            drop(probe);
            drop(c);
            drop(server);
            let _ = std::fs::remove_dir_all(&dir);
            continue;
        }
        assert_eq!(
            status, "ok",
            "{cmd:?} crossed the epoch: the save must complete with the pre-change image"
        );
        server.kill_now();
        common::wait_for_port_down(port);
        let (server2, port2) = spawn(&dir, 1);
        let mut c2 = Conn::open(port2);
        let wrong = wrong_pre_keys(&mut c2, N);
        assert!(c2.send(&["SELECT", "1"]).starts_with('+'));
        let in_db1 = c2.send(&["DBSIZE"]);
        assert!(
            wrong == 0 && in_db1 == ":0\r\n",
            "{cmd:?}: the snapshot is not the keyspace the save started from: {wrong} of {N} \
             db-0 keys missing or changed, db 1 DBSIZE {in_db1:?}"
        );
        drop(server2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// How the first BGSAVE of [`aborted_bgsave_then_resave`] is aborted.
#[derive(Clone, Copy, Debug)]
enum Abort {
    /// A FLUSHALL (redis parity: it fails an in-flight save).
    FlushAll,
    /// A replica FULL RESYNC from an empty master (moon#1227 review F6): the
    /// node's data becomes the master's, which is not a record of its own
    /// log.
    Resync { master_port: u16 },
}

/// Load `n` values of 64 KiB (`big:<i>`), pipelined 100 at a time.
fn preload_big(c: &mut Conn, n: u32) {
    let value = "v".repeat(64 * 1024);
    let mut i = 0u32;
    while i < n {
        let end = (i + 100).min(n);
        let keys: Vec<String> = (i..end).map(|j| format!("big:{j:06}")).collect();
        let cmds: Vec<Vec<&str>> = keys
            .iter()
            .map(|k| vec!["SET", k.as_str(), value.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let reply = c.pipeline(&refs);
        assert!(!reply.contains('-'), "preload refused: {reply:.200}");
        i = end;
    }
}

/// moon#1227 review F1, end to end. A BGSAVE aborted while its writer thread
/// still holds a backlog, then the next BGSAVE as soon as the shard takes it.
/// The aborted save's writer used to keep appending that backlog into
/// `shard-0.rrdshard.tmp` — the same inode the next save had just truncated
/// and was renaming into place: the second save completed ("BGSAVE OK"), and
/// the file it published held tens to hundreds of MiB of the aborted save's
/// blocks behind its own ~300 bytes, failing its checksum at restart (the
/// canary written between the saves gone). Or the straggler unlinked the new temp file and
/// the second save failed.
///
/// Since moon#1230 `rdb_last_bgsave_status` describes the LAST save, so it
/// judges the second save directly; the log shows the first was aborted, and
/// the restart proves the published file is sound.
fn aborted_bgsave_then_resave(dir: &std::path::Path, abort: Abort) {
    let (mut server, port) = spawn(dir, 1);
    let mut c = Conn::open(port);
    let logged = match abort {
        Abort::FlushAll => "aborted: FLUSHALL",
        Abort::Resync { .. } => "aborted: a replica full resync",
    };
    let aborted_saves = || {
        std::fs::read_to_string(dir.join("server.err"))
            .unwrap_or_default()
            .matches(logged)
            .count()
    };
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match abort {
            // ~375 MiB of 64 KiB values only: the writer thread, not the
            // walk, is the bottleneck, so it holds a deep backlog when the
            // FLUSHALL lands.
            Abort::FlushAll => preload_big(&mut c, 6_000),
            // The resync (connect, PSYNC, load) must land while the epoch is
            // still WRITING. Behind a values-heavy save it came up only after
            // the walk ended — measured: link up 0.7-1.7 s into a 375 MiB
            // save vs ~40 ms idle, most likely its durable
            // `replication.state` write queueing behind the save's dirty
            // pages. Small keys keep the walk going (~1,024 entries per 1 ms
            // tick) with few bytes to flush.
            Abort::Resync { .. } => preload(&mut c, 800_000 << (attempt - 1).min(2)),
        }
        let aborted_before = aborted_saves();
        assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
        // A missed attempt's save completed, so its temp file is renamed away.
        wait_epoch_armed(dir);
        match abort {
            Abort::FlushAll => {
                // Mid-walk, not at its first tick: once the writer has
                // written 128 MiB the shard's hand-over (up to 1 MiB per
                // tick) has run ahead of it and the backlog is deep; at the
                // first tick it can be almost empty, and a straggler with
                // little left drains it while the FLUSHALL runs (tens of ms
                // in a debug build). ~250 MiB of walk remain.
                let tmp = dir.join("shard-0.rrdshard.tmp");
                let grown_by = Instant::now() + Duration::from_secs(60);
                while std::fs::metadata(&tmp).map_or(0, |m| m.len()) < 128 << 20 {
                    assert!(
                        Instant::now() < grown_by,
                        "the first save never wrote 128 MiB"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                assert!(c.send(&["FLUSHALL"]).starts_with('+'));
            }
            Abort::Resync { master_port } => {
                // The master is empty: after the resync this node holds
                // nothing.
                assert!(
                    c.send(&["REPLICAOF", "127.0.0.1", &master_port.to_string()])
                        .starts_with('+')
                );
                let deadline = Instant::now() + Duration::from_secs(60);
                while !c
                    .send(&["INFO", "replication"])
                    .contains("master_link_status:up")
                {
                    assert!(Instant::now() < deadline, "the replica link never came up");
                    std::thread::sleep(Duration::from_millis(1));
                }
                assert!(c.send(&["REPLICAOF", "NO", "ONE"]).starts_with('+'));
            }
        }
        assert!(c.send(&["SET", "canary", "after-abort"]).starts_with('+'));
        // The next BGSAVE, the moment the shard has dropped the aborted one.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let reply = c.send(&["BGSAVE"]);
            if reply.contains("Background saving started") {
                break;
            }
            assert!(Instant::now() < deadline, "BGSAVE never accepted: {reply}");
            std::thread::sleep(Duration::from_micros(200));
        }
        let status = wait_bgsave(&mut c, Duration::from_secs(120));
        if aborted_saves() > aborted_before {
            assert_eq!(
                status, "ok",
                "{abort:?}: the BGSAVE after the aborted one must publish"
            );
            break;
        }
        // Beside this file's other tests (debug servers on a 4-vCPU box),
        // the abort was seen dispatched only after the walk ended in up to
        // two attempts in a row.
        assert!(
            attempt < 4,
            "fixture: {abort:?} never landed inside the save in {attempt} attempts"
        );
        eprintln!("attempt {attempt}: the save finished before {abort:?}; setting it up again");
        assert!(c.send(&["DEL", "canary"]).starts_with(':'));
    }
    // A straggler writer of the aborted save has at most tens of MiB left.
    std::thread::sleep(Duration::from_millis(500));
    let published = std::fs::metadata(dir.join("shard-0.rrdshard"))
        .map(|m| m.len())
        .unwrap_or(0);

    server.kill_now();
    common::wait_for_port_down(port);
    let (server2, port2) = spawn(dir, 1);
    let mut c2 = Conn::open(port2);
    assert!(
        c2.send(&["GET", "canary"]).contains("after-abort"),
        "{abort:?}: the second BGSAVE completed, but its {published}-byte file did not restore"
    );
    assert_eq!(c2.send(&["DBSIZE"]), ":1\r\n");
    drop(server2);
}

/// [`aborted_bgsave_then_resave`], aborted by FLUSHALL: the epoch is 64 KiB
/// values only, so its writer holds a deep backlog (up to
/// `SNAPSHOT_STREAM_MAX_IN_FLIGHT`) when the FLUSHALL lands, and the next
/// BGSAVE follows within a millisecond of its reply. RED on a debug binary
/// without the writer-cancel fix (`snapshot_stream`): the second save fails
/// (the straggler unlinked its temp file) or publishes a file of hundreds of
/// MiB that does not restore — 3 of 3 runs alone, 4 of 5 beside the resync
/// variant. Green runs there were ones where the FLUSHALL itself took long
/// enough for the straggler to drain: a race, as F1 is.
#[test]
fn an_aborted_bgsave_cannot_corrupt_the_next_one() {
    let dir = common::unique_test_dir("ws12-abort-resave");
    std::fs::create_dir_all(&dir).unwrap();
    aborted_bgsave_then_resave(&dir, Abort::FlushAll);
    let _ = std::fs::remove_dir_all(&dir);
}

/// [`aborted_bgsave_then_resave`], aborted by a replica full resync — the
/// abort path (`snapshot_cow::note_table_replace`) no other real-server
/// test reaches: the resync lands mid-save, fails it, and the next save
/// publishes a sound file.
///
/// NOT a regression test for the writer-cancel fix: the small keys that let
/// the resync land inside the walk leave the writer no backlog, and it
/// passes on a binary without that fix. A writer-bound (values-only) epoch
/// does reproduce F1 with a resync, but its resync then lands after the
/// walk in most attempts. The FLUSHALL variant is the F1 regression.
///
/// monoio only: a master answers PSYNC only under runtime-monoio ("-ERR PSYNC
/// requires runtime-monoio on the master"), so a tokio build has no resync
/// to abort a save with. The FLUSHALL variant above runs on both runtimes.
#[test]
#[cfg_attr(
    not(feature = "runtime-monoio"),
    ignore = "needs a replica full resync, which needs a runtime-monoio master"
)]
fn a_resync_mid_bgsave_fails_it_and_the_next_save_publishes() {
    let dir = common::unique_test_dir("ws12-resync-resave");
    std::fs::create_dir_all(&dir).unwrap();
    let mdir = common::unique_test_dir("ws12-resync-resave-master");
    std::fs::create_dir_all(&mdir).unwrap();
    let (master, master_port) = spawn(&mdir, 1);
    aborted_bgsave_then_resave(&dir, Abort::Resync { master_port });
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&mdir);
}
