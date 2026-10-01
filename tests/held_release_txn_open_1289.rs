//! moon#1289 × moon#1300: the AUTOMATIC held-file snapshot must not capture
//! an open `TXN`'s uncommitted writes.
//!
//! The auto-trigger variant of `review_w1_txn_abort_no_aof_snapshot_1285`.
//! Without an AOF a snapshot is the durability authority, and a snapshot
//! taken while a transaction is open keeps its uncommitted writes (moon#1300
//! F3, WS42). `BGSAVE` and the save rules share that flaw and are WS42's;
//! WS39 added a snapshot nobody asks for — held cold files trigger one after
//! three orphan sweeps — so any cold churn during a TXN made the abort come
//! back after a crash. moon#1300 (F3): every snapshot stores an open TXN's
//! keys at their pre-transaction value, so the trigger runs while the TXN is
//! open (no waiting, no starvation) and the abort still does not come back.
//!
//! ```text
//! --appendonly no --save "" --disk-offload enable (1 s orphan sweep)
//! SET k original ; fill (spills) ; BGSAVE ; DEL every filler (files held)
//! TXN BEGIN ; SET k aborted ; SET new inserted
//! wait past the trigger (3 sweeps) ; TXN ABORT -> +OK
//! kill -9 ; restart -> GET k original, GET new nil
//! ```
//!
//! Red on f766fc2 (both runtimes): the snapshot fires ~3 s into the TXN and
//! the restart answers `k=aborted new=inserted`.
//!
//! `MOON_BIN=<moon> cargo test --test held_release_txn_open_1289 -- --include-ignored`

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::slow_host::{self, WriteStats};
use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const OK: &str = "+OK\r\n";
const NIL: &str = "$-1\r\n";
const FILLER: usize = 1500;
const FILLER_LEN: usize = 2000;
/// Several times the trigger's wait: three 1 s sweeps (the base fires at ~3 s).
const PAST_THE_TRIGGER: Duration = Duration::from_secs(8);

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

struct Server {
    guard: ServerGuard,
    port: u16,
    dir: PathBuf,
}

fn start(dir: &Path, shards: usize) -> Server {
    std::fs::create_dir_all(dir).unwrap();
    let dir_arg = dir.to_path_buf();
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

    fn snapshots_requested(&self) -> u64 {
        self.info("cold_held_release_snapshots_requested")
            .unwrap_or(0)
    }
}

fn heap_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("shard-"))
        .map(|shard| {
            std::fs::read_dir(shard.path().join("data"))
                .into_iter()
                .flatten()
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with(".mpf"))
                .count()
        })
        .sum()
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
/// files (the only manual command), then delete them: the files are held
/// and the automatic trigger is armed.
fn arm_the_trigger(srv: &Server, c: &mut Conn) {
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
        || srv.info("rdb_bgsave_in_progress") == Some(0),
    );
    // The sweep raises `hold_below` above every file minted so far.
    std::thread::sleep(Duration::from_millis(2500));
    assert_eq!(srv.snapshots_requested(), 0, "setup: nothing held yet");
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

/// The TXN is open for well past the trigger, then aborted; a crash right
/// after the abort must restart into the pre-transaction state.
fn an_aborted_txn_does_not_come_back(tag: &str, shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1289-txn-{tag}"));
    let mut srv = start(&dir, shards);
    let mut c = Conn::open(srv.port);
    // One hash tag: a TXN writes only on its own shard (#499).
    assert_eq!(c.send(&["SET", "{t}k", "original"]), OK);
    arm_the_trigger(&srv, &mut c);

    let mut t = txn_on_the_keys_shard(srv.port, "aborted");
    assert_eq!(t.send(&["SET", "{t}new", "inserted"]), OK);
    slow_host::wait_until(
        "the automatic snapshot to run and release the held files, the TXN open",
        PAST_THE_TRIGGER * 4,
        srv.port,
        &srv.dir,
        || {
            srv.snapshots_requested() >= 1
                && srv.info("rdb_bgsave_in_progress") == Some(0)
                && srv.info("cold_files_pending_unlink") == Some(0)
                && heap_files(&srv.dir) == 0
        },
    );
    let snaps_during_txn = srv.snapshots_requested();
    assert_eq!(t.send(&["TXN", "ABORT"]), OK);
    assert_eq!(
        c.send(&["GET", "{t}k"]),
        bulk("original"),
        "live, after the abort"
    );
    assert_eq!(c.send(&["GET", "{t}new"]), NIL, "live, after the abort");
    drop(t);
    drop(c);
    srv.guard.kill_now();

    let mut srv = start(&dir, shards);
    let mut c = Conn::open(srv.port);
    let k = c.send(&["GET", "{t}k"]);
    let new = c.send(&["GET", "{t}new"]);
    srv.guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        k == bulk("original") && new == NIL,
        "{tag}: an aborted TXN came back after kill -9 + restart from the automatic held-file \
         snapshot taken while it was open ({snaps_during_txn} requested during the TXN): \
         GET k -> {k:?} (want \"original\"), GET new -> {new:?} (want nil)"
    );
    assert!(
        snaps_during_txn >= 1,
        "{tag}: moon#1300 — the automatic snapshot runs while a TXN is open"
    );
}

/// moon#1300: the snapshot runs while the TXN is open and saves the
/// PRE-transaction value; once the TXN commits, the next save holds the
/// commit, and a crash after it restarts into the committed state.
fn the_snapshot_runs_while_the_txn_is_open(tag: &str, shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1289-txn-after-{tag}"));
    let mut srv = start(&dir, shards);
    let mut c = Conn::open(srv.port);
    assert_eq!(c.send(&["SET", "{t}k", "original"]), OK);
    arm_the_trigger(&srv, &mut c);

    let mut t = txn_on_the_keys_shard(srv.port, "committed");
    let began = Instant::now();
    slow_host::wait_until(
        "the automatic snapshot to run and release the held files, the TXN open",
        PAST_THE_TRIGGER * 4,
        srv.port,
        &srv.dir,
        || {
            srv.snapshots_requested() >= 1
                && srv.info("rdb_bgsave_in_progress") == Some(0)
                && srv.info("cold_files_pending_unlink") == Some(0)
                && heap_files(&srv.dir) == 0
        },
    );
    eprintln!(
        "{tag}: the snapshot ran and released the files {:?} into the open TXN",
        began.elapsed()
    );
    assert_eq!(t.send(&["TXN", "COMMIT"]), OK);
    let lastsave = srv.info("rdb_last_save_time");
    std::thread::sleep(Duration::from_millis(1_100));
    assert!(c.send(&["BGSAVE"]).starts_with('+'));
    slow_host::wait_until(
        "the save after the commit",
        Duration::from_secs(60),
        srv.port,
        &srv.dir,
        || {
            srv.info("rdb_bgsave_in_progress") == Some(0)
                && srv.info("rdb_last_save_time") > lastsave
        },
    );
    drop(t);
    drop(c);
    srv.guard.kill_now();

    let mut srv = start(&dir, shards);
    let k = Conn::open(srv.port).send(&["GET", "{t}k"]);
    srv.guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        k,
        bulk("committed"),
        "{tag}: the committed write is durable"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_aborted_txn_does_not_come_back_from_the_held_file_snapshot_1_shard() {
    an_aborted_txn_does_not_come_back("s1", 1);
}

/// The TXN's shard and the shards whose files are held differ: the signal
/// must be process-wide, not the requesting shard's own table.
#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_aborted_txn_does_not_come_back_from_the_held_file_snapshot_4_shards() {
    an_aborted_txn_does_not_come_back("s4", 4);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn the_held_file_snapshot_runs_while_the_txn_is_open() {
    the_snapshot_runs_while_the_txn_is_open("s1", 1);
}
