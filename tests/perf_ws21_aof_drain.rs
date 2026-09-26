//! moon#1274 against a real server: a graceful stop with `--appendonly yes`
//! (moon's default) keeps every acknowledged write.
//!
//! N SETs are acknowledged under `appendfsync everysec`, the server is
//! stopped at once — SIGTERM (`systemctl stop`), SIGINT (Ctrl-C) or a plain
//! `SHUTDOWN` — and restarted: all N keys must be back, as redis 7.0.15's
//! `prepareForShutdown` (AOF flush + fsync before exit) guarantees. Before
//! the fix shutdown never waited for the AOF writer threads, so the process
//! exited with the records still in their channels: 0 of 100 back at
//! `--shards 1` (monoio) in every run, all lost in about half the runs at
//! `--shards 4`.
//!
//! `a_sigterm_waits_for_a_late_aof_writer` makes the loss deterministic at
//! `--shards 4`: shard 1's writer is held before it reads its channel
//! (`MOON_TEST_AOF_WRITER_HOLD`), so its records are certainly queued when
//! the SIGTERM lands; the exit must wait for the writer, released 300 ms
//! later, to drain them.
//!
//! `--save ""`: no final RDB save, only the AOF. Both runtimes; pin the
//! binary: `MOON_BIN=<moon> cargo test --test perf_ws21_aof_drain`.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

const KEYS: usize = 300;

fn spawn(dir: &Path, shards: usize, hold: Option<&Path>) -> (ServerGuard, u16) {
    spawn_saving(dir, shards, hold, "", None)
}

/// [`spawn`] with save points `save`, and the final save held while
/// `snapshot_hold` exists.
fn spawn_saving(
    dir: &Path,
    shards: usize,
    hold: Option<&Path>,
    save: &str,
    snapshot_hold: Option<&Path>,
) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let mut cmd = std::process::Command::new(&bin);
        cmd.args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--auto-aof-rewrite-percentage",
            "0",
            "--save",
            save,
            "--disk-offload",
            "disable",
            "--maxmemory",
            "0",
            "--disk-free-min-pct",
            "0",
        ])
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .stdout(common::server_stderr(dir))
        .stderr(common::server_stderr(dir));
        if let Some(hold) = hold {
            cmd.env("MOON_TEST_AOF_WRITER_HOLD", format!("1:{}", hold.display()));
        }
        if let Some(hold) = snapshot_hold {
            cmd.env("MOON_TEST_SNAPSHOT_HOLD_FILE", hold);
        }
        cmd.spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

#[derive(Clone, Copy, Debug)]
enum Stop {
    Term,
    Int,
    Shutdown,
}

fn stop(server: &ServerGuard, c: &mut Conn, how: Stop) {
    let sig = match how {
        Stop::Term => "-TERM",
        Stop::Int => "-INT",
        Stop::Shutdown => {
            c.sock.write_all(&common::encode(&["SHUTDOWN"])).unwrap();
            return;
        }
    };
    let status = std::process::Command::new("kill")
        .args([sig, &server.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill {sig}: {status:?}");
}

fn wait_exit(server: &mut ServerGuard, budget: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = server.as_mut().try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "never exited");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Acknowledge `KEYS` SETs of `prefix:*` (pipelined).
fn set_all(c: &mut Conn, prefix: &str) {
    let keys: Vec<String> = (0..KEYS).map(|i| format!("{prefix}:{i}")).collect();
    let cmds: Vec<[&str; 3]> = keys.iter().map(|k| ["SET", k.as_str(), "v"]).collect();
    let cmds: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
    let replies = c.pipeline(&cmds);
    assert_eq!(replies.matches("+OK\r\n").count(), KEYS, "{replies}");
}

/// How long the server took to exit (status 0 required), bounded at 90 s.
fn exit_took(server: &mut ServerGuard) -> Duration {
    let t = Instant::now();
    let status = wait_exit(server, Duration::from_secs(90));
    assert!(status.success(), "exit status {status:?}");
    t.elapsed()
}

/// Acknowledge `KEYS` SETs (pipelined), then stop the server at once.
fn write_then_stop(dir: &Path, shards: usize, how: Stop, hold: Option<&Path>) {
    let (mut server, port) = spawn(dir, shards, hold);
    let mut c = Conn::open(port);
    set_all(&mut c, "k");
    stop(&server, &mut c, how);
    if let Some(hold) = hold {
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            server.as_mut().try_wait().unwrap().is_none(),
            "exited while an AOF writer still held acknowledged records"
        );
        std::fs::remove_file(hold).unwrap();
    }
    let status = wait_exit(&mut server, Duration::from_secs(90));
    assert!(status.success(), "{how:?}: exit status {status:?}");
    drop(c);
}

/// Keys `prefix:*` missing after a restart, per prefix.
fn missing_after_restart_of(dir: &Path, shards: usize, prefixes: &[&str]) -> Vec<usize> {
    let (_server, port) = spawn(dir, shards, None);
    let mut c = Conn::open(port);
    let mut missing = |prefix: &str| {
        (0..KEYS)
            .filter(|i| c.send(&["GET", &format!("{prefix}:{i}")]) != "$1\r\nv\r\n")
            .count()
    };
    prefixes.iter().map(|p| missing(p)).collect()
}

/// Keys `prefix:*` missing after a restart.
fn missing_after_restart(dir: &Path, shards: usize, prefix: &str) -> usize {
    missing_after_restart_of(dir, shards, &[prefix])[0]
}

fn every_write_survives(shards: usize, how: Stop) {
    let dir = common::unique_test_dir(&format!("ws21-1274-s{shards}-{how:?}"));
    std::fs::create_dir_all(&dir).unwrap();
    write_then_stop(&dir, shards, how, None);
    let lost = missing_after_restart(&dir, shards, "k");
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        lost, 0,
        "--shards {shards}, {how:?}: {lost} of {KEYS} acknowledged writes lost"
    );
}

#[test]
fn sigterm_keeps_every_write_single_shard() {
    every_write_survives(1, Stop::Term);
}

#[test]
fn sigterm_keeps_every_write_four_shards() {
    every_write_survives(4, Stop::Term);
}

#[test]
fn sigint_keeps_every_write_single_shard() {
    every_write_survives(1, Stop::Int);
}

#[test]
fn sigint_keeps_every_write_four_shards() {
    every_write_survives(4, Stop::Int);
}

#[test]
fn shutdown_keeps_every_write_single_shard() {
    every_write_survives(1, Stop::Shutdown);
}

#[test]
fn shutdown_keeps_every_write_four_shards() {
    every_write_survives(4, Stop::Shutdown);
}

#[test]
fn a_sigterm_waits_for_a_late_aof_writer() {
    let dir = common::unique_test_dir("ws21-1274-held");
    std::fs::create_dir_all(&dir).unwrap();
    let hold = dir.join("writer.hold");
    std::fs::write(&hold, b"held").unwrap();
    write_then_stop(&dir, 4, Stop::Term, Some(&hold));
    let lost = missing_after_restart(&dir, 4, "k");
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        lost, 0,
        "{lost} of {KEYS} acknowledged writes lost (shard 1's writer was late)"
    );
}

/// Round 3 A1: a `BGREWRITEAOF` that is dispatched when the stop lands. The
/// fold asks each shard for a cooperative snapshot; a shard that stopped
/// first never answered, the writers waited out the whole 60 s drain bound,
/// and the exit then came with status 0. A fold whose shard is gone now
/// fails at once: the rewrite aborts, the old generation stays
/// authoritative, and the writers drain into it.
fn a_sigterm_right_after_bgrewriteaof(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws21-r3-a1-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, None);
    let mut c = Conn::open(port);
    set_all(&mut c, "k");
    let r = c.send(&["BGREWRITEAOF"]);
    assert!(r.starts_with('+'), "BGREWRITEAOF: {r}");
    stop(&server, &mut c, Stop::Term);
    let took = exit_took(&mut server);
    drop(c);
    let lost = missing_after_restart(&dir, shards, "k");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        took < Duration::from_secs(10) && lost == 0,
        "--shards {shards}: exit took {took:?}, {lost} of {KEYS} acknowledged writes lost"
    );
}

#[test]
fn a_sigterm_right_after_bgrewriteaof_exits_promptly_single_shard() {
    a_sigterm_right_after_bgrewriteaof(1);
}

#[test]
fn a_sigterm_right_after_bgrewriteaof_exits_promptly_four_shards() {
    a_sigterm_right_after_bgrewriteaof(4);
}

/// Round 3 A1, the reviewer's case: shard 1's writer is late, so the other
/// three fold and park at the rewrite barrier; more writes are acknowledged;
/// SIGTERM; shard 1's writer is released only after the shards stopped and
/// pushes its fold into a ring nobody reads. Before: exit after 60 s, status
/// 0, 227 of 300 post-rewrite writes lost.
#[test]
fn a_sigterm_with_a_fold_its_shard_never_served() {
    let dir = common::unique_test_dir("ws21-r3-a1-late");
    std::fs::create_dir_all(&dir).unwrap();
    let hold = dir.join("writer.hold");
    std::fs::write(&hold, b"held").unwrap();
    let (mut server, port) = spawn(&dir, 4, Some(&hold));
    let mut c = Conn::open(port);
    set_all(&mut c, "k");
    let r = c.send(&["BGREWRITEAOF"]);
    assert!(r.starts_with('+'), "BGREWRITEAOF: {r}");
    // Writers 0, 2 and 3 fold and park at the barrier, waiting for writer 1.
    std::thread::sleep(Duration::from_millis(800));
    set_all(&mut c, "p");
    stop(&server, &mut c, Stop::Term);
    // The shards stop; only then does shard 1's late writer read its channel.
    std::thread::sleep(Duration::from_millis(500));
    std::fs::remove_file(&hold).unwrap();
    let took = exit_took(&mut server);
    drop(c);
    let lost = missing_after_restart_of(&dir, 4, &["k", "p"]);
    let (lost_k, lost_p) = (lost[0], lost[1]);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        took < Duration::from_secs(10) && lost_k == 0 && lost_p == 0,
        "exit took {took:?} after the late writer's release; lost {lost_k} + {lost_p} of \
         {KEYS} + {KEYS} acknowledged writes"
    );
}

/// Round 3 A2 (the reviewer's proof 2c): with `--appendonly yes` and save
/// points, a SIGINT starts the final save (held here: it never finishes), and
/// a second SIGINT insists — redis's "You insist... exiting now". It skips
/// the RDB save, not the AOF: before, its `exit(1)` bypassed the moon#1274
/// writer drain and lost the records still queued for a late writer (84 of
/// 300). Exit status 1 as in redis, within a few seconds.
#[test]
fn a_second_sigint_skips_the_save_but_keeps_the_aof_records() {
    let dir = common::unique_test_dir("ws21-r3-a2");
    std::fs::create_dir_all(&dir).unwrap();
    let (snap_hold, writer_hold) = (dir.join("snapshot.hold"), dir.join("writer.hold"));
    std::fs::write(&snap_hold, b"held").unwrap();
    std::fs::write(&writer_hold, b"held").unwrap();
    let rules = "3600 1000000";
    let (mut server, port) = spawn_saving(&dir, 4, Some(&writer_hold), rules, Some(&snap_hold));
    let mut c = Conn::open(port);
    set_all(&mut c, "k");
    stop(&server, &mut c, Stop::Int);
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        server.as_mut().try_wait().unwrap().is_none(),
        "the first SIGINT did not wait for its final save"
    );
    stop(&server, &mut c, Stop::Int);
    let t = Instant::now();
    let status = wait_exit(&mut server, Duration::from_secs(30));
    let took = t.elapsed();
    drop(c);
    std::fs::remove_file(&snap_hold).unwrap();
    std::fs::remove_file(&writer_hold).unwrap();
    let lost = missing_after_restart(&dir, 4, "k");
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(status.code(), Some(1), "redis's insist exit status");
    assert!(
        took < Duration::from_secs(10),
        "the second SIGINT took {took:?}"
    );
    assert_eq!(
        lost, 0,
        "{lost} of {KEYS} acknowledged writes lost by the second SIGINT"
    );
}
