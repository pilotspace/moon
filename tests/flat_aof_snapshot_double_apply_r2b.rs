//! R2b review P1: a snapshot and the legacy single-file `appendonly.aof`
//! must never both be applied at boot.
//!
//! tokio `--shards 1` (the default `--appendonly yes`) appends every write
//! to `<dir>/appendonly.aof`; a BGSAVE is a point-in-time image that already
//! holds a prefix of those records. Boot loaded the snapshot and then
//! replayed the WHOLE file over it, so every non-idempotent write before the
//! save ran twice: `RPUSH l a b; INCR c; BGSAVE; kill -9` came back as
//! `l = a b a b`, `c = 2` (pre-existing on main `cf6fa65`). The same file
//! booted by monoio `--shards 1` (no manifest yet: the legacy upgrade) took
//! the same path once, and folded the doubled state into its first base.
//!
//! The fix is redis's rule — under `appendonly yes` the AOF holding a record
//! is the only KV source (`KvSources::AofOnly`) — plus the condition it
//! needs: a fresh generation opened over a non-empty keyspace (the
//! `--appendonly no` -> `yes` switch) carries that keyspace as its RDB
//! preamble (`aof::fresh_generation`), so the AOF is always complete.
//!
//! Every case writes non-idempotent commands (RPUSH, INCR, APPEND, HINCRBY,
//! and an INCR in db 1), saves, `kill -9`s and checks each was applied once.
//! Run against BOTH runtimes:
//!
//! ```text
//! MOON_BIN=<moon> cargo test --release --test flat_aof_snapshot_double_apply_r2b \
//!   -- --include-ignored
//! ```
//!
//! `tokio_dir_booted_by_monoio` needs `MOON_BIN_TOKIO` and `MOON_BIN_MONOIO`
//! (it is skipped, saying so, without them).

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::Conn;

fn boot(bin: &Path, dir: &Path, shards: usize, extra: &[&str]) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("mkdir");
    common::spawn_listening_guarded(|port| {
        Command::new(bin)
            .args(["--port", &port.to_string(), "--shards", &shards.to_string()])
            .arg("--dir")
            .arg(dir)
            .args(["--disk-free-min-pct", "0"])
            .args(extra)
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    })
}

fn ready(port: u16) -> Conn {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            drop(stream);
            let mut c = Conn::open(port);
            if c.send(&["PING"]).starts_with("+PONG") {
                return c;
            }
        }
        assert!(
            Instant::now() < deadline,
            "moon on {port} never answered PING"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn bgsave(c: &mut Conn) {
    let r = c.send(&["BGSAVE"]);
    assert!(r.contains("Background saving started"), "BGSAVE: {r}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !c
        .send(&["INFO", "persistence"])
        .contains("rdb_bgsave_in_progress:0")
    {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The non-idempotent writes, once.
fn write_once(c: &mut Conn) {
    assert_eq!(c.send(&["RPUSH", "l", "a", "b"]), ":2\r\n");
    assert_eq!(c.send(&["INCR", "c"]), ":1\r\n");
    assert_eq!(c.send(&["APPEND", "s", "xy"]), ":2\r\n");
    assert_eq!(c.send(&["HINCRBY", "h", "f", "5"]), ":5\r\n");
    assert_eq!(c.send(&["SELECT", "1"]), "+OK\r\n");
    assert_eq!(c.send(&["INCR", "c1"]), ":1\r\n");
    assert_eq!(c.send(&["SELECT", "0"]), "+OK\r\n");
}

fn assert_applied_once(c: &mut Conn, what: &str) {
    assert_eq!(
        c.send(&["LRANGE", "l", "0", "-1"]),
        "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        "{what}: RPUSH applied once"
    );
    assert_eq!(c.send(&["GET", "c"]), "$1\r\n1\r\n", "{what}: INCR once");
    assert_eq!(c.send(&["GET", "s"]), "$2\r\nxy\r\n", "{what}: APPEND once");
    assert_eq!(
        c.send(&["HGET", "h", "f"]),
        "$1\r\n5\r\n",
        "{what}: HINCRBY once"
    );
    assert_eq!(c.send(&["SELECT", "1"]), "+OK\r\n");
    assert_eq!(
        c.send(&["GET", "c1"]),
        "$1\r\n1\r\n",
        "{what}: db 1 INCR once"
    );
    assert_eq!(c.send(&["SELECT", "0"]), "+OK\r\n");
}

/// `kill -9` once the acknowledged writes are in the AOF file: a build
/// without moon#1266 1A writes them from the writer thread, and a kill that
/// beats it loses the AOF tail — the snapshot alone then "passes" the check.
fn crash(srv: &mut common::ServerGuard) {
    std::thread::sleep(Duration::from_millis(1500));
    srv.kill_now();
}

fn dir(name: &str) -> PathBuf {
    common::unique_test_dir(&format!("flat-aof-r2b-{name}"))
}

/// Write, BGSAVE, kill -9, restart: every write applied once.
fn save_kill_restart(name: &str, shards: usize, extra: &[&str]) {
    let bin = common::find_moon_binary();
    let d = dir(name);
    let (mut srv, port) = boot(&bin, &d, shards, extra);
    let mut c = ready(port);
    write_once(&mut c);
    bgsave(&mut c);
    drop(c);
    crash(&mut srv);

    let (mut srv, port) = boot(&bin, &d, shards, extra);
    let mut c = ready(port);
    assert_applied_once(&mut c, name);
    // A second crash cycle: the boot that just ran must not have doubled
    // anything into a new generation either.
    bgsave(&mut c);
    drop(c);
    crash(&mut srv);
    let (mut srv, port) = boot(&bin, &d, shards, extra);
    let mut c = ready(port);
    assert_applied_once(&mut c, &format!("{name} (second restart)"));
    crash(&mut srv);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn default_layout_one_shard() {
    save_kill_restart("s1", 1, &[]);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn default_layout_four_shards() {
    save_kill_restart("s4", 4, &[]);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn one_shard_without_disk_offload() {
    save_kill_restart("s1-nooffload", 1, &["--disk-offload", "disable"]);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn one_shard_with_wal_kv_log() {
    save_kill_restart("s1-walkv", 1, &["--wal-kv-log", "on"]);
}

/// BGREWRITEAOF folds the dataset into the AOF (tokio `--shards 1`: an RDB
/// preamble in the flat file), then more writes, then a snapshot newer than
/// the rewrite: still applied once.
#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn rewrite_then_writes_then_snapshot() {
    let bin = common::find_moon_binary();
    let d = dir("rewrite");
    let (mut srv, port) = boot(&bin, &d, 1, &[]);
    let mut c = ready(port);
    assert_eq!(c.send(&["RPUSH", "l", "a"]), ":1\r\n");
    let r = c.send(&["BGREWRITEAOF"]);
    assert!(r.starts_with('+'), "BGREWRITEAOF: {r}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !c
        .send(&["INFO", "persistence"])
        .contains("aof_rewrite_in_progress:0")
    {
        assert!(Instant::now() < deadline, "rewrite never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(c.send(&["RPUSH", "l", "b"]), ":2\r\n");
    assert_eq!(c.send(&["INCR", "c"]), ":1\r\n");
    assert_eq!(c.send(&["APPEND", "s", "xy"]), ":2\r\n");
    assert_eq!(c.send(&["HINCRBY", "h", "f", "5"]), ":5\r\n");
    assert_eq!(c.send(&["SELECT", "1"]), "+OK\r\n");
    assert_eq!(c.send(&["INCR", "c1"]), ":1\r\n");
    assert_eq!(c.send(&["SELECT", "0"]), "+OK\r\n");
    bgsave(&mut c);
    drop(c);
    crash(&mut srv);
    let (mut srv, port) = boot(&bin, &d, 1, &[]);
    let mut c = ready(port);
    assert_applied_once(&mut c, "rewrite");
    crash(&mut srv);
    let _ = std::fs::remove_dir_all(&d);
}

/// The `--appendonly no` -> `yes` switch: the first `yes` boot loads the
/// snapshot (no AOF yet) and opens the AOF over it. That AOF must carry the
/// snapshot's keys (it is the only KV source of every later boot), and a
/// snapshot saved after the switch must not double the switch boot's writes.
fn appendonly_switch(shards: usize) {
    let bin = common::find_moon_binary();
    let d = dir(&format!("switch-s{shards}"));
    let no = ["--appendonly", "no"];
    let (mut srv, port) = boot(&bin, &d, shards, &no);
    let mut c = ready(port);
    assert_eq!(c.send(&["SET", "base", "1"]), "+OK\r\n");
    assert_eq!(c.send(&["RPUSH", "l", "a"]), ":1\r\n");
    bgsave(&mut c);
    drop(c);
    crash(&mut srv);

    let (mut srv, port) = boot(&bin, &d, shards, &[]);
    let mut c = ready(port);
    assert_eq!(
        c.send(&["GET", "base"]),
        "$1\r\n1\r\n",
        "switch boot loads the snapshot"
    );
    assert_eq!(c.send(&["RPUSH", "l", "b"]), ":2\r\n");
    assert_eq!(c.send(&["INCR", "c"]), ":1\r\n");
    bgsave(&mut c);
    drop(c);
    crash(&mut srv);

    for round in ["after the switch", "second restart"] {
        let (mut srv, port) = boot(&bin, &d, shards, &[]);
        let mut c = ready(port);
        assert_eq!(
            c.send(&["GET", "base"]),
            "$1\r\n1\r\n",
            "{round}: base kept"
        );
        assert_eq!(
            c.send(&["LRANGE", "l", "0", "-1"]),
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
            "{round}: each RPUSH once"
        );
        assert_eq!(c.send(&["GET", "c"]), "$1\r\n1\r\n", "{round}: INCR once");
        bgsave(&mut c);
        drop(c);
        crash(&mut srv);
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn appendonly_no_to_yes_one_shard() {
    appendonly_switch(1);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn appendonly_no_to_yes_four_shards() {
    appendonly_switch(4);
}

/// A tokio `--shards 1` dir (flat AOF + snapshot) booted by monoio
/// `--shards 1`: the legacy upgrade captures what it loaded as the first
/// manifest base, so a double apply there is permanent.
#[test]
#[ignore = "spawns moon; set MOON_BIN_TOKIO and MOON_BIN_MONOIO"]
fn tokio_dir_booted_by_monoio() {
    let (Some(tokio), Some(monoio)) = (
        std::env::var_os("MOON_BIN_TOKIO"),
        std::env::var_os("MOON_BIN_MONOIO"),
    ) else {
        eprintln!("SKIPPED: set MOON_BIN_TOKIO and MOON_BIN_MONOIO to run the upgrade case");
        return;
    };
    let (tokio, monoio) = (PathBuf::from(tokio), PathBuf::from(monoio));
    let d = dir("upgrade");
    let (mut srv, port) = boot(&tokio, &d, 1, &[]);
    let mut c = ready(port);
    write_once(&mut c);
    bgsave(&mut c);
    drop(c);
    crash(&mut srv);
    for round in ["first monoio boot", "second monoio boot"] {
        let (mut srv, port) = boot(&monoio, &d, 1, &[]);
        let mut c = ready(port);
        assert_applied_once(&mut c, round);
        drop(c);
        crash(&mut srv);
    }
    let _ = std::fs::remove_dir_all(&d);
}
