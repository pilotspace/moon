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
            "",
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

/// Acknowledge `KEYS` SETs (pipelined), then stop the server at once.
fn write_then_stop(dir: &Path, shards: usize, how: Stop, hold: Option<&Path>) {
    let (mut server, port) = spawn(dir, shards, hold);
    let mut c = Conn::open(port);
    let keys: Vec<String> = (0..KEYS).map(|i| format!("k:{i}")).collect();
    let cmds: Vec<[&str; 3]> = keys.iter().map(|k| ["SET", k.as_str(), "v"]).collect();
    let cmds: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
    let replies = c.pipeline(&cmds);
    assert_eq!(replies.matches("+OK\r\n").count(), KEYS, "{replies}");
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

/// Keys missing after a restart.
fn missing_after_restart(dir: &Path, shards: usize) -> usize {
    let (_server, port) = spawn(dir, shards, None);
    let mut c = Conn::open(port);
    (0..KEYS)
        .filter(|i| c.send(&["GET", &format!("k:{i}")]) != "$1\r\nv\r\n")
        .count()
}

fn every_write_survives(shards: usize, how: Stop) {
    let dir = common::unique_test_dir(&format!("ws21-1274-s{shards}-{how:?}"));
    std::fs::create_dir_all(&dir).unwrap();
    write_then_stop(&dir, shards, how, None);
    let lost = missing_after_restart(&dir, shards);
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
    let lost = missing_after_restart(&dir, 4);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        lost, 0,
        "{lost} of {KEYS} acknowledged writes lost (shard 1's writer was late)"
    );
}
