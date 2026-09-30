//! moon#1289 R2 (review N1): the AUTOMATIC held-file snapshot must not
//! capture a `TXN` that begins after the request's `txn_open` check but
//! before a shard starts its part.
//!
//! `held_release_txn_open_1289` covers a TXN open across the request: the
//! request defers. The check is one instant on the requesting shard, though,
//! and every shard starts its part later, at its own tick: a TXN whose
//! `BEGIN` + `SET` land in between was serialized — the reviewer's
//! `txnrace.sh` (1 ms open, 0.2 ms gap) captured `k=aborted-N` in 12 of 12
//! restarts at 4 shards on 057598f. The fix: each shard re-checks its own
//! hold table as it starts, and one hold abandons the whole round — no file
//! renamed, no held file released, the spacing slot given back.
//!
//! The deterministic tests hold every shard at its start
//! (`MOON_TEST_SNAPSHOT_START_HOLD_FILE`) so the TXN lands in the window by
//! construction:
//!
//! ```text
//! --appendonly no --save "" --disk-offload enable (1 s orphan sweep)
//! SET {t}k original ; fill (spills) ; BGSAVE ; hold starts ; DEL fillers
//! wait: cold_held_release_snapshots_requested >= 1  (pre-check passed)
//! TXN BEGIN ; SET {t}k aborted ; SET {t}new inserted ; release starts
//! wait: rdb_bgsave_in_progress:0
//! kill -9 (TXN still open) ; restart -> GET {t}k original, GET {t}new nil
//! ```
//!
//! Red on 057598f with only the hook added: the restart answers
//! `k=aborted new=inserted`. (On plain 057598f the hook does not exist and
//! the suite fails its "the start is held" precondition instead.)
//!
//! `MOON_BIN=<moon> cargo test --test held_release_txn_race_1289 -- --include-ignored`

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use common::slow_host::{self, WriteStats};
use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const OK: &str = "+OK\r\n";
const NIL: &str = "$-1\r\n";
const FILLER: usize = 1500;
const FILLER_LEN: usize = 2000;

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

struct Server {
    guard: ServerGuard,
    port: u16,
    dir: PathBuf,
}

fn start(dir: &Path, shards: usize, start_hold: &Path) -> Server {
    std::fs::create_dir_all(dir).unwrap();
    let dir_arg = dir.to_path_buf();
    let hold = start_hold.to_path_buf();
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // No save point: only the held-file trigger can snapshot.
                "--save",
                "",
                "--disk-offload",
                "enable",
                "--maxmemory",
                "524288",
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                "--disk-free-min-pct",
                "0",
                "--cold-orphan-sweep-interval-secs",
                "1",
                "--checkpoint-timeout",
                "1",
                "--dir",
            ])
            .arg(&dir_arg)
            .env("MOON_TEST_SNAPSHOT_START_HOLD_FILE", &hold)
            .stdout(Stdio::null())
            .stderr(server_stderr(&dir_arg))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let srv = Server {
        guard,
        port,
        dir: dir.to_path_buf(),
    };
    slow_host::wait_until(
        "the server to finish loading",
        Duration::from_secs(120),
        srv.port,
        &srv.dir,
        || {
            Conn::open(srv.port)
                .send(&["INFO", "persistence"])
                .contains("loading:0\r\n")
        },
    );
    srv
}

impl Server {
    fn info(&self, name: &str) -> Option<u64> {
        slow_host::info_field(self.port, name)
    }

    fn requested(&self) -> u64 {
        self.info("cold_held_release_snapshots_requested")
            .unwrap_or(0)
    }

    fn saving(&self) -> bool {
        self.info("rdb_bgsave_in_progress") != Some(0)
    }
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = e.path();
        if path.is_dir() {
            walk(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn files_named(dir: &Path, suffix: &str) -> Vec<PathBuf> {
    let mut all = Vec::new();
    walk(dir, &mut all);
    all.retain(|p| p.to_string_lossy().ends_with(suffix));
    all
}

/// Every published shard snapshot: path -> (length, mtime).
fn snapshot_files(dir: &Path) -> BTreeMap<PathBuf, (u64, SystemTime)> {
    files_named(dir, ".rrdshard")
        .into_iter()
        .filter_map(|p| {
            let m = std::fs::metadata(&p).ok()?;
            Some((p, (m.len(), m.modified().ok()?)))
        })
        .collect()
}

fn heap_files(dir: &Path) -> usize {
    files_named(dir, ".mpf").len()
}

fn write_all(srv: &Server, c: &mut Conn, verb: &str, value: Option<&str>) {
    let cmds: Vec<Vec<String>> = (0..FILLER)
        .map(|i| {
            let mut cmd = vec![verb.to_string(), format!("held:{i}")];
            cmd.extend(value.map(str::to_string));
            cmd
        })
        .collect();
    let mut stats = WriteStats::default();
    let deadline = Instant::now() + slow_host::CONDITION_DEADLINE;
    slow_host::write_all(c, &cmds, 50, &mut stats, deadline, &srv.dir);
}

/// Spill the fillers, take the snapshot that raises the hold above their
/// files, run `before_del`, then delete them: the files are held and the
/// automatic trigger fires three sweeps later.
fn arm_the_trigger(srv: &Server, c: &mut Conn, before_del: impl FnOnce()) {
    write_all(srv, c, "SET", Some(&"f".repeat(FILLER_LEN)));
    slow_host::wait_until(
        "the fillers to spill",
        slow_host::CONDITION_DEADLINE,
        srv.port,
        &srv.dir,
        || heap_files(&srv.dir) > 0,
    );
    slow_host::wait_spill_idle("the fillers' spills to land", srv.port, &srv.dir);
    let reply = c.send(&["BGSAVE"]);
    assert!(reply.starts_with('+'), "BGSAVE answered {reply:?}");
    std::thread::sleep(Duration::from_millis(300));
    slow_host::wait_until(
        "BGSAVE to finish",
        slow_host::CONDITION_DEADLINE,
        srv.port,
        &srv.dir,
        || !srv.saving(),
    );
    before_del();
    // The sweep raises `hold_below` above every file minted so far.
    std::thread::sleep(Duration::from_millis(2500));
    assert_eq!(srv.requested(), 0, "setup: nothing held yet");
    write_all(srv, c, "DEL", None);
}

/// A connection with a `TXN` open that has written `{t}k = value`. A TXN
/// writes only on its connection's shard (#499) and a connection's shard is
/// not chosen by key, so at several shards reconnect until one lands there.
fn txn_on_the_keys_shard(port: u16, value: &str) -> Conn {
    for _ in 0..64 {
        let mut t = Conn::open(port);
        assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
        let reply = t.send(&["SET", "{t}k", value]);
        if reply == OK {
            return t;
        }
        assert!(reply.contains("cross-shard"), "TXN SET answered {reply:?}");
        assert_eq!(t.send(&["TXN", "ABORT"]), OK);
    }
    panic!("no connection landed on the shard of {{t}}k in 64 tries");
}

/// What the abandoned round left behind.
struct Window {
    srv: Server,
    c: Conn,
    t: Conn,
    hold: PathBuf,
    dir: PathBuf,
    files_before: BTreeMap<PathBuf, (u64, SystemTime)>,
    lastsave_before: Option<u64>,
}

/// Up to the round's end: the request passed its `txn_open` pre-check with
/// no TXN open, every shard is held at its start, the TXN writes, the
/// shards are released, the round ends. The TXN is still open.
fn txn_in_the_window(tag: &str, shards: usize) -> Window {
    let dir = common::unique_test_dir(&format!("moon-1289-race-{tag}"));
    let hold = dir.with_extension("start-hold");
    let _ = std::fs::remove_file(&hold);
    let srv = start(&dir, shards, &hold);
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["SET", "{t}k", "original"]), OK);
    arm_the_trigger(&srv, &mut c, || std::fs::write(&hold, b"hold").unwrap());

    slow_host::wait_until(
        "the automatic snapshot request (its txn_open pre-check passed)",
        Duration::from_secs(60),
        srv.port,
        &srv.dir,
        || srv.requested() >= 1,
    );
    std::thread::sleep(Duration::from_millis(200));
    let files_before = snapshot_files(&dir);
    let lastsave_before = srv.info("rdb_last_save_time");
    assert!(
        srv.saving(),
        "{tag}: precondition — the round is requested and every shard is held at its start \
         (MOON_TEST_SNAPSHOT_START_HOLD_FILE); a binary without the hook has already saved"
    );

    // The TXN begins and writes AFTER the pre-check, BEFORE any shard starts.
    let mut t = txn_on_the_keys_shard(srv.port, "aborted");
    assert_eq!(t.send(&["SET", "{t}new", "inserted"]), OK);
    std::fs::remove_file(&hold).unwrap();
    slow_host::wait_until(
        "the automatic round to end",
        Duration::from_secs(60),
        srv.port,
        &srv.dir,
        || !srv.saving(),
    );
    Window {
        srv,
        c,
        t,
        hold,
        dir,
        files_before,
        lastsave_before,
    }
}

/// kill -9 and restart: `(GET {t}k, GET {t}new)`.
fn crash_and_read(w: &mut Window, shards: usize) -> (String, String) {
    w.srv.guard.kill_now();
    let mut srv = start(&w.dir, shards, &w.hold);
    let mut c = Conn::open(srv.port);
    let k = c.send(&["GET", "{t}k"]);
    let new = c.send(&["GET", "{t}new"]);
    srv.guard.kill_now();
    (k, new)
}

fn a_txn_in_the_window_is_not_saved(tag: &str, shards: usize) {
    let mut w = txn_in_the_window(tag, shards);
    let abandoned = w.srv.info("cold_held_release_snapshots_abandoned_txn");
    let files_after = snapshot_files(&w.dir);
    let temps = files_named(&w.dir, ".tmp");
    let lastsave_after = w.srv.info("rdb_last_save_time");
    let heap_after = heap_files(&w.dir);
    let status_ok = Conn::open(w.srv.port)
        .send(&["INFO", "persistence"])
        .contains("rdb_last_bgsave_status:ok");
    let (k, new) = crash_and_read(&mut w, shards);
    let _ = std::fs::remove_dir_all(&w.dir);

    assert!(
        k == bulk("original") && new == NIL,
        "{tag}: the automatic snapshot captured a TXN that began after its txn_open \
         pre-check and before the shards started: after kill -9 + restart GET k -> {k:?} \
         (want \"original\"), GET new -> {new:?} (want nil)"
    );
    assert_eq!(
        abandoned,
        Some(1),
        "{tag}: the round is counted abandoned once"
    );
    assert_eq!(
        files_after, w.files_before,
        "{tag}: an abandoned round renames no shard file into place"
    );
    assert!(temps.is_empty(), "{tag}: temp files left behind: {temps:?}");
    assert_eq!(
        lastsave_after, w.lastsave_before,
        "{tag}: LASTSAVE did not move"
    );
    assert!(status_ok, "{tag}: an abandon is not a failed save");
    assert!(
        heap_after > 0,
        "{tag}: no held file is released by an abandoned round"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_between_the_request_and_the_start_is_not_saved_1_shard() {
    a_txn_in_the_window_is_not_saved("s1", 1);
}

/// At 4 shards the TXN's shard is one of four: the three others started
/// their part and must drop it, not publish.
#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_txn_between_the_request_and_the_start_is_not_saved_4_shards() {
    a_txn_in_the_window_is_not_saved("s4", 4);
}

/// The abandon is not a cancellation: the slot was given back, the next
/// sweep after the TXN ends takes the snapshot, the held files go, and the
/// crash restarts into the aborted-to state.
#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_abandoned_round_is_retried_once_the_txn_ends() {
    let mut w = txn_in_the_window("retry", 4);
    assert_eq!(w.t.send(&["TXN", "ABORT"]), OK);
    let ended = Instant::now();
    slow_host::wait_until(
        "the retried snapshot to run and release the held files",
        Duration::from_secs(60),
        w.srv.port,
        &w.dir,
        || {
            w.srv.requested() >= 2
                && !w.srv.saving()
                && w.srv.info("cold_files_pending_unlink") == Some(0)
                && heap_files(&w.dir) == 0
        },
    );
    eprintln!(
        "retried snapshot released the files {:?} after the TXN ended",
        ended.elapsed()
    );
    assert_eq!(w.c.send(&["GET", "{t}k"]), bulk("original"));
    let (k, new) = crash_and_read(&mut w, 4);
    let _ = std::fs::remove_dir_all(&w.dir);
    assert_eq!((k, new), (bulk("original"), NIL.to_string()));
}

/// The reviewer's `txnrace` in-suite: one client loops `TXN BEGIN` +
/// `SET {t}k aborted-N`, ~1 ms open, `TXN ABORT`, ~0.2 ms gap, while the held
/// files wait for their snapshot. Every TXN aborts, so once a snapshot has
/// published, a crash must restart into `original`. (If the flood starves
/// the snapshot for 60 s — the documented cost of the guarantee — the gap
/// widens so the test still ends on a published snapshot.)
fn a_busy_txn_workload_never_reaches_the_snapshot(tag: &str, shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1289-busy-{tag}"));
    let hold = dir.with_extension("start-hold");
    let _ = std::fs::remove_file(&hold);
    let mut srv = start(&dir, shards, &hold);
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["SET", "{t}k", "original"]), OK);
    arm_the_trigger(&srv, &mut c, || {});
    let lastsave = srv.info("rdb_last_save_time");
    let mut t = txn_on_the_keys_shard(srv.port, "probe");
    assert_eq!(t.send(&["TXN", "ABORT"]), OK);

    let stop = Arc::new(AtomicBool::new(false));
    let gap_us = Arc::new(AtomicU64::new(200));
    let flood = {
        let (stop, gap_us) = (stop.clone(), gap_us.clone());
        std::thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let value = format!("aborted-{n}");
                let replies = t.pipeline(&[&["TXN", "BEGIN"], &["SET", "{t}k", &value]]);
                assert_eq!(replies, format!("{OK}{OK}"), "TXN {n}");
                std::thread::sleep(Duration::from_millis(1));
                assert_eq!(t.send(&["TXN", "ABORT"]), OK);
                std::thread::sleep(Duration::from_micros(gap_us.load(Ordering::Relaxed)));
            }
            n
        })
    };
    let begun = Instant::now();
    let published =
        || srv.requested() >= 1 && !srv.saving() && srv.info("rdb_last_save_time") > lastsave;
    while !published() && begun.elapsed() < Duration::from_secs(120) {
        if begun.elapsed() > Duration::from_secs(60) {
            gap_us.store(5_000, Ordering::Relaxed);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let done = published();
    stop.store(true, Ordering::Relaxed);
    let cycles = flood.join().unwrap();
    let abandoned = srv.info("cold_held_release_snapshots_abandoned_txn");
    let deferred = srv.info("cold_held_release_snapshots_deferred_txn");
    eprintln!(
        "{tag}: published={done} after {:?}, {cycles} TXNs, deferred={deferred:?} \
         abandoned={abandoned:?}",
        begun.elapsed()
    );
    assert!(done, "{tag}: no automatic snapshot published within 120 s");
    drop(c);
    srv.guard.kill_now();
    let mut srv = start(&dir, shards, &hold);
    let k = Conn::open(srv.port).send(&["GET", "{t}k"]);
    srv.guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        k,
        bulk("original"),
        "{tag}: an aborted TXN came back from the automatic snapshot after kill -9"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_busy_txn_workload_never_reaches_the_automatic_snapshot_1_shard() {
    a_busy_txn_workload_never_reaches_the_snapshot("s1", 1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_busy_txn_workload_never_reaches_the_automatic_snapshot_4_shards() {
    a_busy_txn_workload_never_reaches_the_snapshot("s4", 4);
}
