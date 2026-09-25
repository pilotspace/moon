//! moon#1230 against a real server: `rdb_last_bgsave_status` describes the
//! LAST save — `err` after one a shard failed, `ok` again after one that
//! succeeded — and `rdb_last_save_time` / `LASTSAVE` move on success only.
//!
//! The failure is forced the way moon#1224 made it deterministic: a FLUSHALL
//! landing while a large BGSAVE epoch is still writing aborts that save.
//! Before the fix the status then stayed `err` forever (and every later
//! `SHUTDOWN SAVE` was refused with "background save error", because it
//! reads the same flag after its own, successful, save); and a BGSAVE on a
//! server with no persistence directory never finished
//! (`rdb_bgsave_in_progress:1` forever). PR #1233 review: such a server now
//! refuses BGSAVE and `SHUTDOWN SAVE` up front with an error, instead of
//! answering "Background saving started" for a save that must fail. (That a
//! sharded auto-save is now a counted save is unit-tested in
//! `command::persistence`: in the shipped sharded server `--save` rules never
//! fire at all — a separate finding.)
//!
//! Runs at `--shards 1` and `--shards 4`. Pin the binary:
//! `MOON_BIN=<moon> cargo test --test perf_ws15_bgsave_status`.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

fn spawn(dir: &std::path::Path, shards: usize, extra: &[&str]) -> (ServerGuard, u16) {
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
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .args(extra)
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

/// Wait for the running BGSAVE to finish; returns `rdb_last_bgsave_status`.
fn wait_bgsave(c: &mut Conn, budget: Duration) -> String {
    let deadline = Instant::now() + budget;
    loop {
        if info_field(c, "rdb_bgsave_in_progress") == "0" {
            return info_field(c, "rdb_last_bgsave_status");
        }
        assert!(
            Instant::now() < deadline,
            "the BGSAVE never finished (rdb_bgsave_in_progress stuck at 1)"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

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

fn lastsave(c: &mut Conn) -> u64 {
    let reply = c.send(&["LASTSAVE"]);
    reply
        .trim()
        .trim_start_matches(':')
        .parse()
        .unwrap_or_else(|_| panic!("LASTSAVE reply {reply:?}"))
}

/// A failed BGSAVE (aborted by FLUSHALL) reports `err` and leaves
/// `LASTSAVE` alone; the next, clean BGSAVE reports `ok` and advances it;
/// and `SHUTDOWN SAVE` then succeeds.
fn failed_then_clean_save(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws15-1230-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards, &["--save", ""]);
    let mut c = Conn::open(port);

    // A first successful save, so "LASTSAVE did not move" is a real value.
    c.send(&["SET", "seed", "1"]);
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    assert_eq!(wait_bgsave(&mut c, Duration::from_secs(60)), "ok");
    let saved_at = lastsave(&mut c);
    assert!(saved_at > 0, "a successful save sets LASTSAVE");
    assert_eq!(
        info_field(&mut c, "rdb_last_save_time"),
        saved_at.to_string()
    );

    // Past the one-second resolution of LASTSAVE, so a later save is visibly
    // later.
    std::thread::sleep(Duration::from_millis(1100));

    // The failing save: a large epoch, then FLUSHALL while it writes.
    preload(&mut c, 600_000);
    let dirty_before = info_field(&mut c, "rdb_changes_since_last_save");
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    std::thread::sleep(Duration::from_millis(30));
    assert!(c.send(&["FLUSHALL"]).starts_with('+'));
    assert_eq!(
        wait_bgsave(&mut c, Duration::from_secs(120)),
        "err",
        "fixture: FLUSHALL mid-BGSAVE must fail that save (moon#1224)"
    );
    // Every broken promise is collected, so one run shows them all.
    let mut violations: Vec<String> = Vec::new();
    let failed_at = lastsave(&mut c);
    if failed_at != saved_at {
        violations.push(format!(
            "a failed save advanced LASTSAVE ({saved_at} -> {failed_at})"
        ));
    }
    let dirty_after: u64 = info_field(&mut c, "rdb_changes_since_last_save")
        .parse()
        .unwrap();
    if dirty_after < dirty_before.parse::<u64>().unwrap() {
        violations.push(format!(
            "a failed save reset rdb_changes_since_last_save ({dirty_before} -> {dirty_after})"
        ));
    }

    // The clean save afterwards, past LASTSAVE's one-second resolution.
    std::thread::sleep(Duration::from_millis(1100));
    c.send(&["SET", "canary", "after-abort"]);
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    let status = wait_bgsave(&mut c, Duration::from_secs(60));
    if status != "ok" {
        violations.push(format!(
            "a clean BGSAVE after a failed one reports rdb_last_bgsave_status:{status}"
        ));
    }
    let resaved_at = lastsave(&mut c);
    if resaved_at <= failed_at.max(saved_at) {
        violations.push(format!(
            "a successful save did not advance LASTSAVE ({saved_at} -> {resaved_at})"
        ));
    }
    let dirty = info_field(&mut c, "rdb_changes_since_last_save");
    if dirty != "0" {
        violations.push(format!(
            "a successful save left rdb_changes_since_last_save at {dirty}"
        ));
    }

    // SHUTDOWN SAVE reads the same status after its own save: it must now
    // succeed, and the server exit.
    c.sock
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    std::io::Write::write_all(&mut c.sock, &common::encode(&["SHUTDOWN", "SAVE"])).unwrap();
    let mut buf = [0u8; 256];
    let reply = match std::io::Read::read(&mut c.sock, &mut buf) {
        Ok(n) => String::from_utf8_lossy(&buf[..n]).into_owned(),
        Err(_) => String::new(),
    };
    if reply.starts_with('-') {
        violations.push(format!(
            "SHUTDOWN SAVE after a successful save was refused: {}",
            reply.trim()
        ));
        server.kill_now();
    }
    assert!(violations.is_empty(), "--shards {shards}: {violations:#?}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while server.as_mut().try_wait().ok().flatten().is_none() {
        assert!(
            Instant::now() < deadline,
            "SHUTDOWN SAVE did not stop the server"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    common::wait_for_port_down(port);

    // And what it saved restores.
    let (_server2, port2) = spawn(&dir, shards, &["--save", ""]);
    let mut c2 = Conn::open(port2);
    assert!(c2.send(&["GET", "canary"]).contains("after-abort"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bgsave_status_recovers_after_a_failed_save_shards_1() {
    failed_then_clean_save(1);
}

#[test]
fn bgsave_status_recovers_after_a_failed_save_shards_4() {
    failed_then_clean_save(4);
}

/// With no persistence directory (`--appendonly no`, no `--save`) no shard
/// can write a snapshot. BGSAVE and `SHUTDOWN SAVE` are refused up front with
/// an error (PR #1233 review) — not "Background saving started" for a save
/// that then fails (`rdb_last_bgsave_status:err`, and `SHUTDOWN SAVE`
/// refused with "background save error"), and not a save stuck in progress
/// forever (moon#1230). Nothing is marked in progress, the last status stays
/// `ok`, the server stays up, and a later BGSAVE is refused the same way.
#[test]
fn bgsave_without_a_persistence_dir_is_refused_up_front() {
    for shards in [1usize, 4] {
        let dir = common::unique_test_dir(&format!("ws15-1230-nodir-s{shards}"));
        std::fs::create_dir_all(&dir).unwrap();
        let (_server, port) = spawn(&dir, shards, &[]);
        let mut c = Conn::open(port);
        c.send(&["SET", "k", "v"]);
        for attempt in 0..2 {
            let reply = c.send(&["BGSAVE"]);
            assert!(
                reply.starts_with("-ERR background save unavailable"),
                "--shards {shards} attempt {attempt}: BGSAVE must be refused up front: {reply:?}"
            );
            assert_eq!(info_field(&mut c, "rdb_bgsave_in_progress"), "0");
            assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "ok");
        }
        let reply = c.send(&["SHUTDOWN", "SAVE"]);
        assert!(
            reply.starts_with("-ERR background save unavailable"),
            "--shards {shards}: SHUTDOWN SAVE must be refused with the same error: {reply:?}"
        );
        assert_eq!(c.send(&["PING"]), "+PONG\r\n", "the server stays up");
        assert!(c.send(&["GET", "k"]).contains('v'));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
