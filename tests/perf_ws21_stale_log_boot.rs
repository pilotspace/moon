//! moon#1267 review F3 against a real server: with `--appendonly no`, boot
//! loads the snapshot and replays NO KV log over it, as redis loads
//! `dump.rdb` and ignores the AOF when `appendonly` is `no`.
//!
//! Nothing in that mode writes a KV log — no AOF writer, no WAL (`WAL
//! skipped (appendonly=no)`) — so a log on disk was left by an earlier
//! `--appendonly yes` run (or a redis dir) and is older than any snapshot
//! this mode saved. Before the fix boot replayed it over the snapshot: a
//! legacy `appendonly.aof` in `--dir`, or the WAL v3 (the v2 path's
//! last-resort fallback, the v3 path's Phase 4 and its Phase 4b legacy
//! fallback), and the restart read keys back at their OLD values. That held
//! with `--save` rules too, and with the default `--disk-offload enable`.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws21_stale_log_boot`.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

const KEYS: usize = 50;

struct Boot<'a> {
    shards: usize,
    appendonly: bool,
    offload: bool,
    save: Option<&'a str>,
    extra: &'a [&'a str],
}

fn spawn(dir: &Path, boot: &Boot<'_>) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    common::spawn_listening_guarded(|port| {
        let mut args: Vec<String> = [
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &boot.shards.to_string(),
            "--appendonly",
            if boot.appendonly { "yes" } else { "no" },
            "--appendfsync",
            "always",
            "--disk-offload",
            if boot.offload { "enable" } else { "disable" },
            "--maxmemory",
            "0",
            "--disk-free-min-pct",
            "0",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if let Some(save) = boot.save {
            args.extend(["--save".to_string(), save.to_string()]);
        }
        args.extend(boot.extra.iter().map(|s| s.to_string()));
        std::process::Command::new(&bin)
            .args(&args)
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    })
}

fn info_field(c: &mut Conn, field: &str) -> String {
    c.send(&["INFO", "persistence"])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}"))
}

fn bgsave(c: &mut Conn) {
    assert!(
        c.send(&["BGSAVE"])
            .starts_with("+Background saving started")
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while info_field(c, "rdb_bgsave_in_progress") != "0" {
        assert!(Instant::now() < deadline, "the BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(info_field(c, "rdb_last_bgsave_status"), "ok");
}

fn set_all(c: &mut Conn, value: &str) {
    for i in 0..KEYS {
        let key = format!("k:{i}");
        assert_eq!(c.send(&["SET", &key, value]), "+OK\r\n");
    }
}

/// Keys that came back with a value other than `want`.
fn wrong(c: &mut Conn, want: &str) -> Vec<String> {
    let expected = format!("${}\r\n{want}\r\n", want.len());
    (0..KEYS)
        .map(|i| format!("k:{i}"))
        .filter(|k| c.send(&["GET", k]) != expected)
        .collect()
}

fn kill(mut server: ServerGuard) {
    server.kill_now();
}

/// The review's case and its variants: a legacy `appendonly.aof` holding
/// `SET k:* old` in `--dir`; the `--appendonly no` server sets `new`, saves,
/// and is killed; the restart must read `new`.
fn a_legacy_aof_is_not_replayed_over_the_snapshot(offload: bool, save: Option<&str>) {
    let dir = common::unique_test_dir(&format!("ws21-f3-aof-{offload}-{}", save.is_some()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut aof = Vec::new();
    for i in 0..KEYS {
        let key = format!("k:{i}");
        aof.extend(common::encode(&["SET", &key, "old"]));
    }
    std::fs::write(dir.join("appendonly.aof"), aof).unwrap();
    let boot = Boot {
        shards: 1,
        appendonly: false,
        offload,
        save,
        extra: &[],
    };

    let (server, port) = spawn(&dir, &boot);
    let mut c = Conn::open(port);
    set_all(&mut c, "new");
    bgsave(&mut c);
    drop(c);
    kill(server);

    let (server, port) = spawn(&dir, &boot);
    let mut c = Conn::open(port);
    let reverted = wrong(&mut c, "new");
    drop(c);
    kill(server);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        reverted.is_empty(),
        "offload={offload} save={save:?}: {} of {KEYS} keys read back from the stale \
         appendonly.aof instead of the snapshot: {reverted:?}",
        reverted.len()
    );
}

#[test]
fn legacy_aof_offload_disabled_no_save_rules() {
    a_legacy_aof_is_not_replayed_over_the_snapshot(false, None);
}

#[test]
fn legacy_aof_offload_disabled_with_save_rules() {
    a_legacy_aof_is_not_replayed_over_the_snapshot(false, Some("3600 1000000"));
}

#[test]
fn legacy_aof_offload_enabled_no_save_rules() {
    a_legacy_aof_is_not_replayed_over_the_snapshot(true, None);
}

/// The WAL v3 variant: an `--appendonly yes --wal-kv-log on` run logs the
/// routed writes of `old` to the per-shard WAL; the operator then switches
/// to `--appendonly no`, sets `new`, saves; the restart must read `new`.
fn a_stale_wal_is_not_replayed_over_the_snapshot(offload: bool) {
    let dir = common::unique_test_dir(&format!("ws21-f3-wal-{offload}"));
    std::fs::create_dir_all(&dir).unwrap();
    let yes = Boot {
        shards: 4,
        appendonly: true,
        offload,
        save: None,
        extra: &["--wal-kv-log", "on"],
    };
    let (server, port) = spawn(&dir, &yes);
    let mut c = Conn::open(port);
    set_all(&mut c, "old");
    // The WAL flushes on a 1 ms tick; a clean stop flushes the rest.
    c.sock
        .write_all(&common::encode(&["SHUTDOWN", "NOSAVE"]))
        .unwrap();
    drop(c);
    let mut server = server;
    let deadline = Instant::now() + Duration::from_secs(30);
    while server.as_mut().try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the first run never exited");
        std::thread::sleep(Duration::from_millis(10));
    }
    let no = Boot {
        shards: 4,
        appendonly: false,
        offload,
        save: None,
        extra: &[],
    };
    let (server, port) = spawn(&dir, &no);
    let mut c = Conn::open(port);
    set_all(&mut c, "new");
    bgsave(&mut c);
    drop(c);
    kill(server);

    let (server, port) = spawn(&dir, &no);
    let mut c = Conn::open(port);
    let reverted = wrong(&mut c, "new");
    drop(c);
    kill(server);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        reverted.is_empty(),
        "offload={offload}: {} of {KEYS} keys read back from the stale WAL v3 instead of \
         the snapshot: {reverted:?}",
        reverted.len()
    );
}

#[test]
fn stale_wal_offload_disabled() {
    a_stale_wal_is_not_replayed_over_the_snapshot(false);
}

#[test]
fn stale_wal_offload_enabled() {
    a_stale_wal_is_not_replayed_over_the_snapshot(true);
}

/// Round 3 A5: switching `--appendonly yes` → `no` WITHOUT a snapshot boots
/// empty — by design (F3: the log is not loaded under `no`) — and now says
/// so: a WARN names the multi-part AOF left unloaded and the remedy (BGSAVE
/// under `yes` first). Before, only a legacy `appendonly.aof` was logged,
/// and only at INFO.
#[test]
fn switching_to_appendonly_no_names_the_unloaded_multi_part_aof() {
    let dir = common::unique_test_dir("ws21-r3-a5");
    std::fs::create_dir_all(&dir).unwrap();
    let yes = Boot {
        shards: 4,
        appendonly: true,
        offload: false,
        save: None,
        extra: &[],
    };
    let (mut server, port) = spawn(&dir, &yes);
    let mut c = Conn::open(port);
    set_all(&mut c, "v");
    c.sock
        .write_all(&common::encode(&["SHUTDOWN", "NOSAVE"]))
        .unwrap();
    drop(c);
    let deadline = Instant::now() + Duration::from_secs(30);
    while server.as_mut().try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the first run never exited");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        dir.join("appendonlydir/moon.aof.manifest").exists(),
        "fixture: no manifest"
    );

    let no = Boot {
        appendonly: false,
        ..yes
    };
    let (server, port) = spawn(&dir, &no);
    let mut c = Conn::open(port);
    let dbsize = c.send(&["DBSIZE"]);
    drop(c);
    kill(server);
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(dbsize, ":0\r\n", "fixture: a snapshot existed");
    let warned = log
        .lines()
        .any(|l| l.contains("WARN") && l.contains("appendonlydir") && l.contains("NOT loaded"));
    assert!(
        warned,
        "no WARN names the multi-part AOF left unloaded under --appendonly no"
    );
}
