//! `INFO total_commands_processed` counts what the CLIENT sent — once per
//! executed command, on every dispatch path and at every shard count
//! (moon#775, moon#1002).
//!
//! Every expected number below is what redis-server 8.6.1 reports for the
//! same traffic (measured 2026-09-19, one fresh connection per family, INFO
//! read over a separate connection):
//!
//! - a command counts once when it executes, wherever it executes — a 4-key
//!   `MSET` that the coordinator splits into per-shard legs is still ONE
//!   command (moon#1002: shards>1 used to book one per leg);
//! - `MULTI`, `EXEC` and `DISCARD` count, and a queued command counts when
//!   `EXEC` runs it, not when it is queued;
//! - `EVAL` counts, and so does every `redis.call` it issues;
//! - a blocking command counts once, whether it is served at once, woken or
//!   timed out;
//! - `INFO` counts itself;
//! - a command refused before execution (unknown name, wrong arity) does NOT
//!   count, while one that executes and fails (`WRONGTYPE`) does.
//!
//! One server per test. Each family runs on its own fresh connection, and the
//! delta is read over a separate connection, so the measured window holds
//! exactly the family's traffic plus the bracketing `INFO` (subtracted).
//!
//! Run against a pre-fix binary with `MOON_BIN=<path>`.

mod common;

use std::io::Write;
use std::time::Duration;

use common::{Conn, encode};

struct TmpDir(std::path::PathBuf);

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Field order is the drop order: the guard reaps the server first, then the
/// directory goes.
struct Moon {
    _guard: common::ServerGuard,
    _dir: TmpDir,
    port: u16,
}

fn spawn_moon(shards: &str) -> Moon {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir("moon-cmd-accounting");
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

/// `total_commands_processed` from `INFO stats`, over `info`.
fn total(info: &mut Conn) -> i64 {
    let body = info.send(&["INFO", "stats"]);
    body.lines()
        .find_map(|l| l.strip_prefix("total_commands_processed:"))
        .map(|v| v.trim().parse::<i64>().expect("counter value"))
        .unwrap_or_else(|| panic!("no total_commands_processed in INFO stats:\n{body}"))
}

/// Run one family's traffic and return how many commands INFO booked for it.
///
/// The `before` INFO executes inside the window (it is counted after it
/// answers), so one is subtracted; whether a server counts INFO at all is
/// asserted separately by the `INFO counts itself` family, which sends
/// nothing.
fn delta(moon: &Moon, info: &mut Conn, traffic: fn(u16)) -> i64 {
    info.send(&["FLUSHALL"]);
    let before = total(info);
    traffic(moon.port);
    let after = total(info);
    after - before - 1
}

fn f_single(port: u16) {
    let mut c = Conn::open(port);
    for i in 0..10 {
        c.send(&["SET", &format!("k{i}"), "v"]);
    }
    for i in 0..10 {
        c.send(&["GET", &format!("k{i}")]);
    }
}

/// Plain `GET`/`SET` are the inline fast path's commands (`try_inline_dispatch`).
fn f_inline_get_set_pipelined(port: u16) {
    let mut c = Conn::open(port);
    let mut out = Vec::new();
    for i in 0..50 {
        out.extend_from_slice(&encode(&["SET", &format!("p{i}"), "v"]));
    }
    for i in 0..50 {
        out.extend_from_slice(&encode(&["GET", &format!("p{i}")]));
    }
    c.sock.write_all(&out).expect("write");
    c.read_replies(100);
}

/// Telnet-style inline commands (no multibulk header).
fn f_inline_protocol(port: u16) {
    let mut c = Conn::open(port);
    for i in 0..10 {
        c.sock
            .write_all(format!("SET il{i} v\r\n").as_bytes())
            .expect("write");
        c.read_replies(1);
    }
    for i in 0..10 {
        c.sock
            .write_all(format!("GET il{i}\r\n").as_bytes())
            .expect("write");
        c.read_replies(1);
    }
}

/// Untagged keys: at shards>1 the coordinator splits each command into legs.
fn f_mset(port: u16) {
    let mut c = Conn::open(port);
    for i in 0..10 {
        let (a, b, cc, d) = (
            format!("ka{i}"),
            format!("kb{i}"),
            format!("kc{i}"),
            format!("kd{i}"),
        );
        c.send(&["MSET", &a, "1", &b, "1", &cc, "1", &d, "1"]);
    }
}

fn f_mget(port: u16) {
    let mut c = Conn::open(port);
    c.send(&["MSET", "a", "1", "b", "1", "c", "1", "d", "1"]);
    for _ in 0..10 {
        c.send(&["MGET", "a", "b", "c", "d"]);
    }
}

fn f_del_exists(port: u16) {
    let mut c = Conn::open(port);
    for _ in 0..10 {
        c.send(&["EXISTS", "a", "b", "c", "d"]);
    }
    for _ in 0..10 {
        c.send(&["DEL", "a", "b", "c", "d"]);
    }
}

/// Keyspace-wide commands fan out to every shard.
fn f_fanout(port: u16) {
    let mut c = Conn::open(port);
    for _ in 0..3 {
        c.send(&["DBSIZE"]);
        c.send(&["KEYS", "*"]);
        c.send(&["SCAN", "0"]);
    }
}

fn f_multi_exec(port: u16) {
    let mut c = Conn::open(port);
    for _ in 0..5 {
        c.send(&["MULTI"]);
        c.send(&["SET", "x", "1"]);
        c.send(&["GET", "x"]);
        c.send(&["EXEC"]);
    }
}

fn f_multi_discard(port: u16) {
    let mut c = Conn::open(port);
    for _ in 0..5 {
        c.send(&["MULTI"]);
        c.send(&["SET", "x", "1"]);
        c.send(&["DISCARD"]);
    }
}

fn f_script(port: u16) {
    let mut c = Conn::open(port);
    for _ in 0..5 {
        c.send(&[
            "EVAL",
            "redis.call('SET',KEYS[1],'1'); return redis.call('GET',KEYS[1])",
            "1",
            "sk",
        ]);
    }
}

fn f_blpop_ready(port: u16) {
    let mut c = Conn::open(port);
    c.send(&["RPUSH", "L", "0", "1", "2", "3", "4"]);
    for _ in 0..5 {
        c.send(&["BLPOP", "L", "1"]);
    }
}

fn f_blpop_woken(port: u16) {
    let mut c = Conn::open(port);
    let mut d = Conn::open(port);
    for _ in 0..3 {
        c.sock
            .write_all(&encode(&["BLPOP", "BL", "5"]))
            .expect("write");
        std::thread::sleep(Duration::from_millis(100));
        d.send(&["LPUSH", "BL", "x"]);
        c.read_replies(1);
    }
}

fn f_blpop_timeout(port: u16) {
    let mut c = Conn::open(port);
    for _ in 0..2 {
        c.send(&["BLPOP", "EMPTY", "0.1"]);
    }
}

/// Unknown name and wrong arity are refused (not counted); WRONGTYPE and a
/// non-integer INCR execute and fail (counted).
fn f_errors(port: u16) {
    let mut c = Conn::open(port);
    c.send(&["NOSUCHCMD"]);
    c.send(&["GET"]);
    c.send(&["SET", "s", "v"]);
    c.send(&["LPUSH", "s", "x"]);
    c.send(&["INCR", "s"]);
}

fn f_ping_select(port: u16) {
    let mut c = Conn::open(port);
    for _ in 0..5 {
        c.send(&["PING"]);
    }
    for _ in 0..5 {
        c.send(&["SELECT", "0"]);
    }
}

fn f_fresh_connections(port: u16) {
    for i in 0..20 {
        let mut c = Conn::open(port);
        c.send(&["SET", &format!("f{i}"), "v"]);
    }
}

/// Nothing at all: the window holds only the bracketing INFO, which redis
/// counts. `delta` subtracts one, so a server that counts INFO reports 0 and
/// one that does not reports -1.
fn f_info_counts_itself(_port: u16) {}

type Family = (&'static str, fn(u16), i64);

/// `(family, traffic, redis-server 8.6.1 count)`.
const FAMILIES: &[Family] = &[
    ("single-key SET/GET x10", f_single, 20),
    ("inline GET/SET pipelined x100", f_inline_get_set_pipelined, 100),
    ("inline protocol SET/GET x10", f_inline_protocol, 20),
    ("MSET 4 keys x10", f_mset, 10),
    ("MGET 4 keys x10 (+1 MSET)", f_mget, 11),
    ("EXISTS/DEL 4 keys x10", f_del_exists, 20),
    ("DBSIZE/KEYS/SCAN x3", f_fanout, 9),
    ("MULTI/SET/GET/EXEC x5", f_multi_exec, 20),
    ("MULTI/SET/DISCARD x5", f_multi_discard, 10),
    ("EVAL with 2 redis.call x5", f_script, 15),
    ("BLPOP served at once x5 (+1 RPUSH)", f_blpop_ready, 6),
    ("BLPOP woken by LPUSH x3", f_blpop_woken, 6),
    ("BLPOP timed out x2", f_blpop_timeout, 2),
    ("refused vs failed commands", f_errors, 3),
    ("PING/SELECT x5", f_ping_select, 10),
    ("20 fresh connections, 1 SET each", f_fresh_connections, 20),
    ("INFO counts itself", f_info_counts_itself, 0),
];

fn assert_families_match_redis(shards: &str) {
    let moon = spawn_moon(shards);
    let mut info = Conn::open(moon.port);
    let mut table = String::new();
    let mut wrong = 0;
    for &(name, traffic, want) in FAMILIES {
        let got = delta(&moon, &mut info, traffic);
        let mark = if got == want {
            "ok"
        } else {
            wrong += 1;
            "WRONG"
        };
        table.push_str(&format!("  {mark:5} {name:40} moon {got:4}  redis {want:4}\n"));
    }
    assert_eq!(
        wrong, 0,
        "--shards {shards}: total_commands_processed must count client commands \
         exactly as redis does:\n{table}"
    );
}

#[test]
fn client_commands_are_counted_once_at_one_shard() {
    assert_families_match_redis("1");
}

/// moon#1002: at four shards a coordinator command must still count once,
/// however many legs it is split into.
#[test]
fn client_commands_are_counted_once_at_four_shards() {
    assert_families_match_redis("4");
}

/// `instantaneous_ops_per_sec` — the rate every dashboard plots — is the
/// per-second delta of the counter above. Nothing sampled it, so it read 0
/// under any load (reported on moon#775 from a live instance serving 10-90
/// ops/s). Continuous traffic for ~2.5 s must show a nonzero rate.
#[test]
fn instantaneous_ops_per_sec_moves_under_load() {
    let moon = spawn_moon("1");
    let port = moon.port;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let load = {
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut c = Conn::open(port);
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                c.send(&["PING"]);
            }
        })
    };
    let mut info = Conn::open(port);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut seen = 0i64;
    while std::time::Instant::now() < deadline && seen == 0 {
        std::thread::sleep(Duration::from_millis(300));
        let body = info.send(&["INFO", "stats"]);
        seen = body
            .lines()
            .find_map(|l| l.strip_prefix("instantaneous_ops_per_sec:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    load.join().expect("load thread");
    assert!(
        seen > 0,
        "instantaneous_ops_per_sec stayed 0 through 5 s of continuous PING traffic"
    );
}
