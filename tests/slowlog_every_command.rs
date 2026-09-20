//! SLOWLOG logs EVERY command over the threshold (moon#994), and its two
//! parameters are live CONFIG parameters (moon#995).
//!
//! moon#994: the slowlog used to time one command in sixteen PER CONNECTION,
//! so a long-lived connection logged at most 1/16 of its slow commands and a
//! connection that sent fewer than 16 commands was never logged at all — the
//! connect-run-disconnect shape of CLI tools, health checks and serverless
//! clients. Redis logs every command at or over the threshold.
//!
//! moon#995: `slowlog-log-slower-than` and `slowlog-max-len` existed only as
//! startup flags, so the standard runbook — lower the threshold live, watch,
//! restore — was impossible. Every reply and error text asserted below is
//! redis-server 8.6.1's (measured 2026-09-19).
//!
//! Run against a pre-fix binary with `MOON_BIN=<path>`.

mod common;

use std::time::Duration;

use common::Conn;

struct TmpDir(std::path::PathBuf);

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Moon {
    _guard: common::ServerGuard,
    _dir: TmpDir,
    port: u16,
}

fn spawn_moon(shards: &str, extra: &[&str]) -> Moon {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir("moon-slowlog");
    std::fs::create_dir_all(&dir).expect("create tmp dir");
    let (guard, port) = common::spawn_listening_guarded(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf8 dir"),
            ])
            .args(extra)
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    Moon {
        _guard: guard,
        _dir: TmpDir(dir),
        port,
    }
}

/// `SLOWLOG LEN` as an integer.
fn slowlog_len(c: &mut Conn) -> i64 {
    let r = c.send(&["SLOWLOG", "LEN"]);
    r.trim()
        .strip_prefix(':')
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("SLOWLOG LEN reply: {r:?}"))
}

/// A connection that sends `cmds` and nothing else, then closes.
fn short_lived(port: u16, cmds: &[&[&str]]) {
    let mut c = Conn::open(port);
    for argv in cmds {
        c.send_within(argv, Duration::from_secs(10));
    }
}

/// The #994 shape: a fresh connection sends 1-3 commands, one of them slow,
/// and disconnects. With the default threshold (10 ms) the slow one must be
/// logged — with 1-in-16 sampling it never was.
fn slow_command_on_a_fresh_connection_is_logged(shards: &str) {
    let moon = spawn_moon(shards, &[]);
    let mut admin = Conn::open(moon.port);
    admin.send(&["SLOWLOG", "RESET"]);
    short_lived(moon.port, &[&["DEBUG", "SLEEP", "0.02"]]);
    short_lived(
        moon.port,
        &[&["PING"], &["DEBUG", "SLEEP", "0.02"], &["PING"]],
    );
    let log = admin.send(&["SLOWLOG", "GET", "128"]);
    let logged = log.matches("SLEEP").count();
    assert_eq!(
        logged, 2,
        "--shards {shards}: both 20 ms DEBUG SLEEPs must be in the slowlog, got:\n{log}"
    );
}

#[test]
fn slow_command_on_a_fresh_connection_is_logged_at_one_shard() {
    slow_command_on_a_fresh_connection_is_logged("1");
}

#[test]
fn slow_command_on_a_fresh_connection_is_logged_at_four_shards() {
    slow_command_on_a_fresh_connection_is_logged("4");
}

/// Threshold 0 logs every command. 24 connections of one keyed `SET` each
/// (the keys spread over every shard at `--shards 4`) must leave 24 SET
/// entries; the pre-fix sampler logged none of them.
fn every_command_is_logged_at_threshold_zero(shards: &str) {
    let moon = spawn_moon(
        shards,
        &[
            "--slowlog-log-slower-than",
            "0",
            "--slowlog-max-len",
            "5000",
        ],
    );
    let mut admin = Conn::open(moon.port);
    admin.send(&["SLOWLOG", "RESET"]);
    for i in 0..24 {
        short_lived(moon.port, &[&["SET", &format!("fresh:{i}"), "v"]]);
    }
    let log = admin.send(&["SLOWLOG", "GET", "5000"]);
    let sets = (0..24)
        .filter(|i| log.contains(&format!("fresh:{i}\r\n")))
        .count();
    assert_eq!(
        sets, 24,
        "--shards {shards}: threshold 0 must log all 24 single-command connections"
    );
}

#[test]
fn every_command_is_logged_at_threshold_zero_at_one_shard() {
    every_command_is_logged_at_threshold_zero("1");
}

#[test]
fn every_command_is_logged_at_threshold_zero_at_four_shards() {
    every_command_is_logged_at_threshold_zero("4");
}

/// `CONFIG GET` answers both parameters, in redis's table order, with
/// redis's defaults.
#[test]
fn config_get_reports_both_slowlog_parameters() {
    let moon = spawn_moon("1", &[]);
    let mut c = Conn::open(moon.port);
    assert_eq!(
        c.send(&["CONFIG", "GET", "slowlog*"]),
        "*4\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n10000\r\n\
         $15\r\nslowlog-max-len\r\n$3\r\n128\r\n"
    );
    assert_eq!(
        c.send(&["CONFIG", "GET", "slowlog-log-slower-than"]),
        "*2\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n10000\r\n"
    );
}

/// The live runbook: lower the threshold with CONFIG SET and every shard's
/// connections start logging at once; `-1` disables the slowlog.
fn config_set_threshold_is_live_on_every_shard(shards: &str) {
    let moon = spawn_moon(shards, &[]);
    let mut admin = Conn::open(moon.port);
    assert_eq!(
        admin.send(&["CONFIG", "SET", "slowlog-log-slower-than", "0"]),
        "+OK\r\n"
    );
    assert_eq!(
        admin.send(&["CONFIG", "GET", "slowlog-log-slower-than"]),
        "*2\r\n$23\r\nslowlog-log-slower-than\r\n$1\r\n0\r\n"
    );
    admin.send(&["SLOWLOG", "RESET"]);
    // Fresh connections land on every shard's accept loop.
    for i in 0..16 {
        short_lived(moon.port, &[&["SET", &format!("live:{i}"), "v"]]);
    }
    let log = admin.send(&["SLOWLOG", "GET", "128"]);
    let logged = (0..16)
        .filter(|i| log.contains(&format!("live:{i}\r\n")))
        .count();
    assert_eq!(
        logged, 16,
        "--shards {shards}: CONFIG SET must reach every shard:\n{log}"
    );

    assert_eq!(
        admin.send(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"]),
        "+OK\r\n"
    );
    admin.send(&["SLOWLOG", "RESET"]);
    short_lived(moon.port, &[&["DEBUG", "SLEEP", "0.02"]]);
    for i in 0..8 {
        short_lived(moon.port, &[&["SET", &format!("off:{i}"), "v"]]);
    }
    assert_eq!(
        slowlog_len(&mut admin),
        0,
        "--shards {shards}: a negative threshold disables the slowlog"
    );
}

#[test]
fn config_set_threshold_is_live_at_one_shard() {
    config_set_threshold_is_live_on_every_shard("1");
}

#[test]
fn config_set_threshold_is_live_at_four_shards() {
    config_set_threshold_is_live_on_every_shard("4");
}

/// Shrinking `slowlog-max-len` trims the log at once, keeping the newest
/// entries; changing the threshold keeps what is already logged.
#[test]
fn config_set_max_len_trims_and_threshold_change_keeps_entries() {
    let moon = spawn_moon(
        "1",
        &["--slowlog-log-slower-than", "0", "--slowlog-max-len", "128"],
    );
    let mut c = Conn::open(moon.port);
    c.send(&["SLOWLOG", "RESET"]);
    for i in 0..6 {
        c.send(&["SET", &format!("t{i}"), "v"]);
    }
    assert!(slowlog_len(&mut c) >= 6);
    assert_eq!(
        c.send(&["CONFIG", "SET", "slowlog-max-len", "3"]),
        "+OK\r\n"
    );
    assert_eq!(slowlog_len(&mut c), 3, "a shrink trims immediately");
    let log = c.send(&["SLOWLOG", "GET", "3"]);
    assert!(
        ["t0", "t1", "t2"]
            .iter()
            .all(|k| !log.contains(&format!("\r\n{k}\r\n"))),
        "the trim drops the OLDEST entries:\n{log}"
    );
    assert_eq!(
        c.send(&["CONFIG", "SET", "slowlog-log-slower-than", "5000"]),
        "+OK\r\n"
    );
    assert_eq!(
        slowlog_len(&mut c),
        3,
        "a threshold change does not clear the log"
    );
    assert_eq!(
        c.send(&["CONFIG", "GET", "slowlog-max-len"]),
        "*2\r\n$15\r\nslowlog-max-len\r\n$1\r\n3\r\n"
    );
}

/// Invalid values are refused with redis's exact text and change nothing —
/// including when a valid slowlog pair precedes the invalid one in the same
/// CONFIG SET.
#[test]
fn config_set_rejects_invalid_values_like_redis() {
    let moon = spawn_moon("1", &[]);
    let mut c = Conn::open(moon.port);
    let parse = |p: &str| {
        format!(
            "-ERR CONFIG SET failed (possibly related to argument '{p}') - \
             argument couldn't be parsed into an integer\r\n"
        )
    };
    for bad in ["abc", "1.5", "", " 5", "+5", "05", "-0", "0x10", "10mb", "9223372036854775808"] {
        assert_eq!(
            c.send(&["CONFIG", "SET", "slowlog-log-slower-than", bad]),
            parse("slowlog-log-slower-than"),
            "slowlog-log-slower-than {bad:?}"
        );
    }
    assert_eq!(
        c.send(&["CONFIG", "SET", "slowlog-log-slower-than", "-2"]),
        "-ERR CONFIG SET failed (possibly related to argument 'slowlog-log-slower-than') - \
         argument must be between -1 and 9223372036854775807 inclusive\r\n"
    );
    for bad in ["abc", "1.5", "1k", "9223372036854775808"] {
        assert_eq!(
            c.send(&["CONFIG", "SET", "slowlog-max-len", bad]),
            parse("slowlog-max-len"),
            "slowlog-max-len {bad:?}"
        );
    }
    assert_eq!(
        c.send(&["CONFIG", "SET", "slowlog-max-len", "-1"]),
        "-ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - \
         argument must be between 0 and 9223372036854775807 inclusive\r\n"
    );
    // A valid pair followed by an invalid one: nothing is applied.
    assert_eq!(
        c.send(&[
            "CONFIG",
            "SET",
            "slowlog-max-len",
            "7",
            "slowlog-log-slower-than",
            "abc"
        ]),
        parse("slowlog-log-slower-than")
    );
    assert_eq!(
        c.send(&["CONFIG", "GET", "slowlog*"]),
        "*4\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n10000\r\n\
         $15\r\nslowlog-max-len\r\n$3\r\n128\r\n",
        "a refused CONFIG SET must leave both values unchanged"
    );
    // The extremes redis accepts.
    assert_eq!(
        c.send(&[
            "CONFIG",
            "SET",
            "slowlog-log-slower-than",
            "9223372036854775807"
        ]),
        "+OK\r\n"
    );
    assert_eq!(
        c.send(&["CONFIG", "SET", "slowlog-max-len", "0"]),
        "+OK\r\n"
    );
}

/// `--slowlog-log-slower-than -1` is accepted at startup, as redis accepts
/// `slowlog-log-slower-than -1` in its config file.
#[test]
fn negative_threshold_is_accepted_at_startup() {
    let moon = spawn_moon("1", &["--slowlog-log-slower-than", "-1"]);
    let mut c = Conn::open(moon.port);
    assert_eq!(
        c.send(&["CONFIG", "GET", "slowlog-log-slower-than"]),
        "*2\r\n$23\r\nslowlog-log-slower-than\r\n$2\r\n-1\r\n"
    );
    c.send(&["DEBUG", "SLEEP", "0.02"]);
    assert_eq!(slowlog_len(&mut c), 0);
}
