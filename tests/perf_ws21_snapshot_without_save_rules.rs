//! moon#1267 against a real server: with `--appendonly no` and no save
//! rules, a snapshot in `--dir` loads at boot and manual saves work, as in
//! redis 7 (`save ""` only disables AUTOMATIC saves; `dump.rdb` still loads
//! and `BGSAVE` / `SHUTDOWN SAVE` still write it).
//!
//! Before the fix `main.rs` gave the shards a persistence directory only
//! with `--appendonly yes` or a `--save` argument — so with `--save`
//! OMITTED (the default) boot skipped recovery and started empty beside the
//! snapshot, and `BGSAVE` / `SHUTDOWN SAVE` answered "background save
//! unavailable". (An explicit `--save ""` was a `Some` and worked; both
//! spellings are exercised here.)
//!
//! Each case runs at `--shards 1` and `--shards 4`. Pin the binary:
//! `MOON_BIN=<moon> cargo test --test perf_ws21_snapshot_without_save_rules`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

const KEYS: u64 = 2_000;

/// How the server is told about save rules.
#[derive(Clone, Copy, Debug)]
enum Save {
    /// `--save "3600 1"`.
    Rules,
    /// `--save ""`.
    Empty,
    /// No `--save` argument at all (the default).
    Omitted,
    /// No `--save`, and the default `--disk-offload enable` (the snapshot
    /// then lives in the offload tree, `<dir>/shard-N/`).
    OmittedOffload,
}

fn spawn(dir: &Path, shards: usize, save: Save) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let mut args: Vec<String> = [
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
            "--disk-offload",
            if matches!(save, Save::OmittedOffload) {
                "enable"
            } else {
                "disable"
            },
            "--maxmemory",
            "0",
            "--disk-free-min-pct",
            "0",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        match save {
            Save::Rules => args.extend(["--save".to_string(), "3600 1".to_string()]),
            Save::Empty => args.extend(["--save".to_string(), String::new()]),
            Save::Omitted | Save::OmittedOffload => {}
        }
        std::process::Command::new(&bin)
            .args(&args)
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

fn key(j: u64) -> String {
    format!("snap:{j:06}")
}

fn value(j: u64) -> String {
    format!("value-{j:06}")
}

fn info_field(c: &mut Conn, field: &str) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

fn fill(c: &mut Conn) {
    let mut out = Vec::new();
    for j in 0..KEYS {
        out.extend_from_slice(&encode(&["SET", &key(j), &value(j)]));
    }
    c.sock.write_all(&out).unwrap();
    let reply = c.read_replies(KEYS as usize);
    assert!(!reply.contains('-'), "SET refused: {reply:.200}");
}

/// Keys that are not back with their value.
fn missing(c: &mut Conn) -> usize {
    let mut out = Vec::new();
    for j in 0..KEYS {
        out.extend_from_slice(&encode(&["GET", &key(j)]));
    }
    c.sock.write_all(&out).unwrap();
    let reply = c.read_replies(KEYS as usize);
    let mut lines = reply.split("\r\n");
    let mut missing = 0;
    for j in 0..KEYS {
        let head = lines.next().unwrap_or("");
        let body = if head == "$-1" {
            "<nil>"
        } else {
            lines.next().unwrap_or("")
        };
        if body != value(j) {
            missing += 1;
        }
    }
    missing
}

/// `BGSAVE`, then wait for it: it must start and succeed.
fn bgsave(c: &mut Conn) {
    let reply = c.send(&["BGSAVE"]);
    assert!(
        reply.starts_with("+Background saving started"),
        "BGSAVE refused: {reply:?}"
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while info_field(c, "rdb_bgsave_in_progress") != "0" {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(info_field(c, "rdb_last_bgsave_status"), "ok");
}

/// The server's exit status, or `None` if it is still running after
/// `budget` (the guard then SIGKILLs it).
fn wait_exit(server: &mut ServerGuard, budget: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = server.as_mut().try_wait().expect("try_wait") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn restart_and_count_missing(dir: &Path, shards: usize, save: Save) -> usize {
    let (_server, port) = spawn(dir, shards, save);
    let mut c = Conn::open(port);
    missing(&mut c)
}

/// Write + BGSAVE under save rules, SIGKILL, restart WITHOUT save rules: the
/// snapshot loads.
fn a_snapshot_loads_after_a_restart_without_save_rules(shards: usize, restart: Save) {
    let dir = common::unique_test_dir(&format!("ws21-1267-load-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, Save::Rules);
    let mut c = Conn::open(port);
    fill(&mut c);
    bgsave(&mut c);
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);

    let missing = restart_and_count_missing(&dir, shards, restart);
    assert_eq!(
        missing, 0,
        "--shards {shards}, restart with {restart:?}: {missing} of {KEYS} keys missing although \
         every shard's snapshot is in --dir"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// No save rules at all: BGSAVE writes a snapshot and a restart loads it.
fn bgsave_works_without_save_rules(shards: usize, save: Save) {
    let dir = common::unique_test_dir(&format!("ws21-1267-bgsave-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, save);
    let mut c = Conn::open(port);
    fill(&mut c);
    bgsave(&mut c);
    for s in 0..shards {
        let name = format!("shard-{s}.rrdshard");
        assert!(
            dir.join(&name).exists() || dir.join(format!("shard-{s}")).join(&name).exists(),
            "shard {s} wrote no snapshot"
        );
    }
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);

    let missing = restart_and_count_missing(&dir, shards, save);
    assert_eq!(
        missing, 0,
        "--shards {shards} {save:?}: {missing} keys missing"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// No save rules: `SHUTDOWN SAVE` saves and exits; a plain `SHUTDOWN` does
/// not save (redis: no save points, no save).
fn shutdown_save_works_without_save_rules(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws21-1267-shutdown-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, Save::Omitted);
    let mut c = Conn::open(port);
    fill(&mut c);
    c.sock.write_all(&encode(&["SHUTDOWN", "SAVE"])).unwrap();
    let mut rest = Vec::new();
    let _ = std::io::Read::read_to_end(&mut c.sock, &mut rest);
    assert!(
        rest.is_empty(),
        "SHUTDOWN SAVE answered instead of exiting: {:?}",
        String::from_utf8_lossy(&rest)
    );
    let status = wait_exit(&mut server, Duration::from_secs(60))
        .expect("SHUTDOWN SAVE did not exit within 60 s");
    assert!(status.success(), "SHUTDOWN SAVE exit status {status:?}");
    common::wait_for_port_down(port);

    // A plain SHUTDOWN of the restarted server must not save: the key set
    // after the restart is gone after the next one.
    let (mut server, port) = spawn(&dir, shards, Save::Omitted);
    let mut c = Conn::open(port);
    assert_eq!(
        missing(&mut c),
        0,
        "--shards {shards}: SHUTDOWN SAVE's snapshot"
    );
    assert!(c.send(&["SET", "after-save", "x"]).starts_with("+OK"));
    c.sock.write_all(&encode(&["SHUTDOWN"])).unwrap();
    let _ = std::io::Read::read_to_end(&mut c.sock, &mut Vec::new());
    wait_exit(&mut server, Duration::from_secs(60)).expect("SHUTDOWN did not exit");
    common::wait_for_port_down(port);

    let (_server, port) = spawn(&dir, shards, Save::Omitted);
    let mut c = Conn::open(port);
    assert_eq!(missing(&mut c), 0);
    assert_eq!(c.send(&["EXISTS", "after-save"]), ":0\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_snapshot_loads_after_a_restart_with_save_omitted_single_shard() {
    a_snapshot_loads_after_a_restart_without_save_rules(1, Save::Omitted);
}

#[test]
fn a_snapshot_loads_after_a_restart_with_save_omitted_four_shards() {
    a_snapshot_loads_after_a_restart_without_save_rules(4, Save::Omitted);
}

#[test]
fn a_snapshot_loads_after_a_restart_with_save_empty_four_shards() {
    a_snapshot_loads_after_a_restart_without_save_rules(4, Save::Empty);
}

#[test]
fn bgsave_works_with_save_omitted_single_shard() {
    bgsave_works_without_save_rules(1, Save::Omitted);
}

#[test]
fn bgsave_works_with_save_omitted_four_shards() {
    bgsave_works_without_save_rules(4, Save::Omitted);
}

/// The default configuration: no `--save`, disk offload on. Boot already
/// loaded a snapshot there (the offload recovery), but BGSAVE was refused.
#[test]
fn bgsave_works_with_save_omitted_and_disk_offload_four_shards() {
    bgsave_works_without_save_rules(4, Save::OmittedOffload);
}

#[test]
fn bgsave_works_with_save_empty_single_shard() {
    bgsave_works_without_save_rules(1, Save::Empty);
}

#[test]
fn shutdown_save_works_with_save_omitted_single_shard() {
    shutdown_save_works_without_save_rules(1);
}

#[test]
fn shutdown_save_works_with_save_omitted_four_shards() {
    shutdown_save_works_without_save_rules(4);
}
