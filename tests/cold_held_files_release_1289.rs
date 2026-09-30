//! moon#1289: a held cold file must be released without a manual
//! BGREWRITEAOF / BGSAVE.
//!
//! A dead spill file that the fold (AOF) or snapshot (no AOF) hold covers
//! (`hold_below`) is released only by a committed fold or a snapshot that
//! STARTED after the file went zero-ref. Nothing asked for either: the AOF
//! monitor folds for growth (64 MB), a forced rewrite, or ledger pressure
//! (`ledger > maxmemory/4`), and without an AOF nothing triggers a snapshot.
//! In the `cold_file_id_reuse_1067` shape the ledger is ~63 KiB against a
//! 128 KiB threshold, so the files stayed on disk (`cold_files_pending_unlink`
//! non-zero) until an operator ran the command by hand.
//!
//! Maintainer decision: with an AOF, raise `held_files_pressure` once a held
//! file has waited two orphan sweeps with no committed fold (a fold releases
//! it); without one, request a rate-limited snapshot on the same condition.
//!
//! The shape: fill (keys spill to heap files), take the fold/snapshot that
//! raises the hold above those files (the setup, the only manual command),
//! then DELETE every key. The files are now zero-ref and held. The test asserts
//! `cold_files_pending_unlink` reaches 0, the heap files are gone, and that
//! the trigger fired exactly for the held files: an idle server with live cold
//! keys and no held file requests no fold and no snapshot.
//!
//! Red on the base binaries of both runtimes (the files stay held).
//!
//! `MOON_BIN=<moon> cargo test --test cold_held_files_release_1289 -- --include-ignored`

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::slow_host::{self, WriteStats};
use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const MAXMEMORY_BYTES: u64 = 512 * 1024;
const FILLER: usize = 1500;
const FILLER_LEN: usize = 2000;
/// A held file's wait is two sweeps (1 s each here) plus the trigger's own
/// cadence (monitor tick, rewrite, the next sweep). The base never releases.
const RELEASE_WITHIN: Duration = Duration::from_secs(60);

struct Server {
    _guard: ServerGuard,
    port: u16,
    dir: PathBuf,
}

fn start(tag: &str, shards: usize, aof: bool) -> Server {
    let dir = common::unique_test_dir(&format!("moon-1289-{tag}"));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_arg = dir.clone();
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                if aof { "yes" } else { "no" },
                // No save point: only the trigger under test can snapshot.
                "--save",
                "",
                "--disk-offload",
                "enable",
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
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
        _guard: guard,
        port,
        dir,
    };
    slow_host::wait_until(
        "the server to finish loading",
        Duration::from_secs(120),
        srv.port,
        &srv.dir,
        || {
            let mut c = Conn::open(srv.port);
            c.send(&["INFO", "persistence"]).contains("loading:0\r\n")
        },
    );
    srv
}

impl Server {
    fn conn(&self) -> Conn {
        Conn::open(self.port)
    }

    fn info(&self, name: &str) -> Option<u64> {
        slow_host::info_field(self.port, name)
    }

    /// Heap files on disk, over every shard.
    fn heap_files(&self) -> usize {
        heap_files(&self.dir)
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

fn fill(c: &mut Conn, dir: &Path, stats: &mut WriteStats) {
    let value = "f".repeat(FILLER_LEN);
    let cmds: Vec<Vec<String>> = (0..FILLER)
        .map(|i| vec!["SET".to_string(), format!("held:{i}"), value.clone()])
        .collect();
    let deadline = Instant::now() + slow_host::CONDITION_DEADLINE;
    slow_host::write_all(c, &cmds, 50, stats, deadline, dir);
}

fn delete_all(c: &mut Conn, dir: &Path, stats: &mut WriteStats) {
    let cmds: Vec<Vec<String>> = (0..FILLER)
        .map(|i| vec!["DEL".to_string(), format!("held:{i}")])
        .collect();
    let deadline = Instant::now() + slow_host::CONDITION_DEADLINE;
    slow_host::write_all(c, &cmds, 50, stats, deadline, dir);
}

/// The manual command that raises the hold above the spilled files: a fold
/// with an AOF, a snapshot without. Waits for it to finish.
fn take_the_hold(srv: &Server, aof: bool) {
    let mut c = srv.conn();
    let (cmd, in_progress) = if aof {
        ("BGREWRITEAOF", "aof_rewrite_in_progress")
    } else {
        ("BGSAVE", "rdb_bgsave_in_progress")
    };
    let reply = c.send(&[cmd]);
    assert!(reply.starts_with('+'), "{cmd} answered {reply:?}");
    // The command is asynchronous: wait for its INFO flag to fall back.
    std::thread::sleep(Duration::from_millis(300));
    slow_host::wait_until(
        &format!("{cmd} to finish"),
        slow_host::CONDITION_DEADLINE,
        srv.port,
        &srv.dir,
        || srv.info(in_progress) == Some(0),
    );
    // The orphan sweep observes the new fold epoch and raises `hold_below`
    // above every file minted so far (once per second here).
    std::thread::sleep(Duration::from_millis(2500));
}

/// (folds requested, snapshots requested) for held-file release, from INFO.
fn triggers(srv: &Server) -> (u64, u64) {
    (
        srv.info("cold_held_release_folds_requested").unwrap_or(0),
        srv.info("cold_held_release_snapshots_requested")
            .unwrap_or(0),
    )
}

fn held_files_are_released_without_a_manual_command(tag: &str, shards: usize, aof: bool) {
    let srv = start(tag, shards, aof);
    let mut c = srv.conn();
    let mut stats = WriteStats::default();
    fill(&mut c, &srv.dir, &mut stats);
    slow_host::wait_until(
        "the fillers to spill",
        slow_host::CONDITION_DEADLINE,
        srv.port,
        &srv.dir,
        || srv.heap_files() > 0,
    );
    slow_host::wait_spill_idle("the fillers' spills to land", srv.port, &srv.dir);

    take_the_hold(&srv, aof);
    let (folds0, snaps0) = triggers(&srv);
    assert_eq!(
        (folds0, snaps0),
        (0, 0),
        "no held file yet: the setup's own fold/snapshot is not a held-file trigger"
    );

    delete_all(&mut c, &srv.dir, &mut stats);
    assert_eq!(c.send(&["DBSIZE"]), ":0\r\n");
    slow_host::wait_until(
        "the deleted keys' files to be held (cold_files_pending_unlink > 0)",
        slow_host::CONDITION_DEADLINE,
        srv.port,
        &srv.dir,
        || srv.info("cold_files_pending_unlink").unwrap_or(0) > 0 || srv.heap_files() == 0,
    );
    let held_at = Instant::now();

    // No BGREWRITEAOF, no BGSAVE from here on.
    slow_host::wait_until(
        "the held files to be released with no manual fold or snapshot \
         (cold_files_pending_unlink = 0, no heap file left)",
        RELEASE_WITHIN,
        srv.port,
        &srv.dir,
        || srv.info("cold_files_pending_unlink") == Some(0) && srv.heap_files() == 0,
    );
    eprintln!(
        "{tag}: held files released {:?} after they were held",
        held_at.elapsed()
    );

    let (folds, snaps) = triggers(&srv);
    if aof {
        assert!(folds >= 1, "released, but no fold was requested for it");
        assert_eq!(snaps, 0, "an AOF server folds; it does not snapshot");
    } else {
        assert!(snaps >= 1, "released, but no snapshot was requested for it");
        assert_eq!(folds, 0, "no AOF: nothing to fold");
    }

    // Steady state: nothing held, so nothing more is requested.
    let settled = triggers(&srv);
    std::thread::sleep(Duration::from_secs(6));
    assert_eq!(
        triggers(&srv),
        settled,
        "with no held file the trigger must stay quiet"
    );
}

/// Live cold keys and NO held file: several sweeps pass and nothing is folded
/// or snapshotted. This is the control that the trigger fires for held files
/// and not for cold files in general.
fn steady_state_requests_nothing(tag: &str, shards: usize, aof: bool) {
    let srv = start(tag, shards, aof);
    let mut c = srv.conn();
    let mut stats = WriteStats::default();
    fill(&mut c, &srv.dir, &mut stats);
    slow_host::wait_until(
        "the fillers to spill",
        slow_host::CONDITION_DEADLINE,
        srv.port,
        &srv.dir,
        || srv.heap_files() > 0,
    );
    slow_host::wait_spill_idle("the fillers' spills to land", srv.port, &srv.dir);
    let last_save = srv.info("rdb_last_save_time");
    std::thread::sleep(Duration::from_secs(8));
    assert_eq!(triggers(&srv), (0, 0), "live cold keys are not held files");
    assert_eq!(srv.info("cold_files_pending_unlink"), Some(0));
    assert_eq!(
        srv.info("rdb_last_save_time"),
        last_save,
        "no snapshot ran in the steady state"
    );
    assert!(srv.heap_files() > 0, "the live keys' files are untouched");
    assert_ne!(
        c.send(&["GET", "held:0"]),
        "$-1\r\n",
        "live keys still read"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn held_files_release_with_an_aof_1_shard() {
    held_files_are_released_without_a_manual_command("aof-s1", 1, true);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn held_files_release_with_an_aof_4_shards() {
    held_files_are_released_without_a_manual_command("aof-s4", 4, true);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn held_files_release_without_an_aof_1_shard() {
    held_files_are_released_without_a_manual_command("noaof-s1", 1, false);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn held_files_release_without_an_aof_4_shards() {
    held_files_are_released_without_a_manual_command("noaof-s4", 4, false);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn steady_state_requests_nothing_with_an_aof() {
    steady_state_requests_nothing("steady-aof", 1, true);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn steady_state_requests_nothing_without_an_aof() {
    steady_state_requests_nothing("steady-noaof", 1, false);
}
