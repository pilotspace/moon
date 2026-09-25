//! moon#1232 against a real server: a `--save "<secs> <changes>"` rule fires
//! in the sharded server once both are reached, and not before.
//!
//! Before the fix the sharded auto-save read a counter only the legacy
//! single-listener handler ever incremented, so a rule with a change
//! threshold never fired: `LASTSAVE` stayed where it was and no snapshot was
//! written, however many writes arrived. The trigger now reads
//! `rdb_changes_since_last_save` — the per-shard dirty counts summed on read,
//! the number INFO shows.
//!
//! `--save "1 10"`: nine writes produce no snapshot within a few seconds; ten
//! writes produce one (`LASTSAVE` and `rdb_last_save_time` advance, the status
//! is `ok`, the count returns to 0, and every shard's `shard-N.rrdshard`
//! exists). Runs at `--shards 1` and `--shards 4`, with string writes and with
//! collection writes (which counted 0 until the review of moon#1232).
//!
//! What counts is redis 7.0.15's `server.dirty`, command by command
//! (`dirty_count_matches_redis_7_0_15`: collection writes by their redis rule,
//! a `DEL` / `EXPIRE` of a missing key 0, active expiry 0, `RENAME` 1), and a
//! read never counts — not even the GET of a cold key, which promotes it and
//! evicts others to make room (`cold_reads_do_not_count_as_changes`).
//! `dirty_count_oracle_redis_agrees` (ignored; needs `redis-server` on PATH)
//! re-measures the table's expectations against redis itself.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws19_save_rules`.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
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
                "--save",
                "1 10",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
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

fn lastsave(c: &mut Conn) -> u64 {
    let reply = c.send(&["LASTSAVE"]);
    reply
        .trim()
        .trim_start_matches(':')
        .parse()
        .unwrap_or_else(|_| panic!("LASTSAVE reply {reply:?}"))
}

/// Every `shard-N.rrdshard` under `dir`, recursively.
fn snapshot_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(p: &std::path::Path, acc: &mut Vec<std::path::PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for entry in rd.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("shard-") && n.ends_with(".rrdshard"))
                {
                    acc.push(path);
                }
            }
        }
    }
    let mut acc = Vec::new();
    walk(dir, &mut acc);
    acc
}

/// How a test writes its changes: one keyspace change per command.
#[derive(Clone, Copy)]
enum Writes {
    Strings,
    Collections,
}

fn write(c: &mut Conn, writes: Writes, from: usize, n: usize) {
    for i in from..from + n {
        // Distinct keys, so every shard can own some at --shards 4.
        let key = format!("save-rule:{i}");
        let parts: Vec<&str> = match (writes, i % 4) {
            (Writes::Strings, _) => vec!["SET", &key, "v"],
            (Writes::Collections, 0) => vec!["HSET", &key, "f", "v"],
            (Writes::Collections, 1) => vec!["RPUSH", &key, "a"],
            (Writes::Collections, 2) => vec!["SADD", &key, "a"],
            (Writes::Collections, _) => vec!["ZADD", &key, "1", "m"],
        };
        let reply = c.send(&parts);
        assert!(!reply.starts_with('-'), "{} refused: {reply}", parts[0]);
    }
}

fn save_rule_fires_at_its_change_count(shards: usize) {
    save_rule_fires_for(shards, Writes::Strings);
}

fn save_rule_fires_for(shards: usize, writes: Writes) {
    let dir = common::unique_test_dir(&format!("ws19-1232-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);
    let started_at = lastsave(&mut c);

    // Nine changes: under the rule's threshold. Several ticks of the
    // one-second auto-save timer pass.
    write(&mut c, writes, 0, 9);
    assert_eq!(
        info_field(&mut c, "rdb_changes_since_last_save"),
        "9",
        "fixture: each write is one change"
    );
    std::thread::sleep(Duration::from_millis(3500));
    assert_eq!(
        lastsave(&mut c),
        started_at,
        "--shards {shards}: nine changes must not trigger a \"1 10\" rule"
    );
    assert!(
        snapshot_files(&dir).is_empty(),
        "--shards {shards}: a snapshot was written for nine changes: {:?}",
        snapshot_files(&dir)
    );

    // The tenth change: a snapshot within a few seconds.
    write(&mut c, writes, 9, 1);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let saved =
            lastsave(&mut c) != started_at && info_field(&mut c, "rdb_bgsave_in_progress") == "0";
        if saved {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "--shards {shards}: ten changes with --save \"1 10\" produced no snapshot in 15 s \
             (LASTSAVE still {started_at}, rdb_changes_since_last_save:{}, \
             rdb_bgsave_in_progress:{})",
            info_field(&mut c, "rdb_changes_since_last_save"),
            info_field(&mut c, "rdb_bgsave_in_progress"),
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "ok");
    assert_eq!(
        info_field(&mut c, "rdb_last_save_time"),
        lastsave(&mut c).to_string()
    );
    assert_eq!(
        info_field(&mut c, "rdb_changes_since_last_save"),
        "0",
        "a successful save resets the count"
    );
    let files = snapshot_files(&dir);
    assert_eq!(
        files.len(),
        shards,
        "--shards {shards}: one snapshot file per shard: {files:?}"
    );

    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn save_rule_fires_at_its_change_count_one_shard() {
    save_rule_fires_at_its_change_count(1);
}

#[test]
fn save_rule_fires_at_its_change_count_four_shards() {
    save_rule_fires_at_its_change_count(4);
}

/// Collection writes arm a rule too: `HSET`, `RPUSH`, `SADD` and `ZADD`
/// counted 0 before the moon#1232 review, so a hash-, list- or set-only
/// workload never saved.
#[test]
fn save_rule_fires_on_collection_writes_four_shards() {
    save_rule_fires_for(4, Writes::Collections);
}

// ── What counts, against redis 7.0.15 ──────────────────────────────────────

/// One measured row: `setup` runs unmeasured, then the delta of
/// `rdb_changes_since_last_save` across `measured` must be `redis`, the delta
/// redis-server 7.0.15 shows for the same commands. Pseudo commands:
/// `SLEEP <ms>`, and `PARK <cmd...>`, which sends a command on a second
/// connection without waiting for its reply (a client left blocked), read
/// after the row.
struct Row {
    name: &'static str,
    setup: &'static [&'static [&'static str]],
    measured: &'static [&'static [&'static str]],
    redis: u64,
}

macro_rules! row {
    ($name:expr, [$($setup:expr),*], [$($measured:expr),*], $redis:expr) => {
        Row { name: $name, setup: &[$(&$setup),*], measured: &[$(&$measured),*], redis: $redis }
    };
}

const ROWS: &[Row] = &[
    // The reviewer's table (moon#1232 review).
    row!("SET new key", [], [["SET", "a", "1"]], 1),
    row!(
        "MSET 3 keys",
        [],
        [["MSET", "b", "1", "c", "2", "d", "3"]],
        3
    ),
    row!("DEL missing key", [], [["DEL", "nope"]], 0),
    row!(
        "DEL 2 existing",
        [["SET", "d1", "1"], ["SET", "d2", "1"]],
        [["DEL", "d1", "d2"]],
        2
    ),
    row!("GET hit", [["SET", "g", "1"]], [["GET", "g"]], 0),
    row!("GET miss", [], [["GET", "nope2"]], 0),
    row!(
        "HSET 3 new fields",
        [],
        [["HSET", "h", "f1", "1", "f2", "2", "f3", "3"]],
        3
    ),
    row!(
        "RPUSH 5 elements",
        [],
        [["RPUSH", "l", "a", "b", "c", "d", "e"]],
        5
    ),
    row!("SADD 3 members", [], [["SADD", "s", "a", "b", "c"]], 3),
    row!("ZADD 2 members", [], [["ZADD", "z", "1", "a", "2", "b"]], 2),
    row!("INCR", [], [["INCR", "n"]], 1),
    row!(
        "SET NX on existing (no-op)",
        [["SET", "x", "1"]],
        [["SET", "x", "2", "NX"]],
        0
    ),
    row!("EXPIRE missing key", [], [["EXPIRE", "nope3", "10"]], 0),
    row!("HGET", [["HSET", "h2", "f", "v"]], [["HGET", "h2", "f"]], 0),
    row!(
        "HDEL missing field",
        [["HSET", "h3", "f", "v"]],
        [["HDEL", "h3", "nof"]],
        0
    ),
    row!("LPOP missing list", [], [["LPOP", "nolist"]], 0),
    row!(
        "LRANGE",
        [["RPUSH", "l2", "a"]],
        [["LRANGE", "l2", "0", "-1"]],
        0
    ),
    row!("APPEND", [["SET", "ap", "v"]], [["APPEND", "ap", "x"]], 1),
    row!(
        "MULTI 2 SETs EXEC",
        [],
        [["MULTI"], ["SET", "m1", "1"], ["SET", "m2", "2"], ["EXEC"]],
        2
    ),
    row!(
        "EVAL 2 SETs",
        [],
        [[
            "EVAL",
            "redis.call('SET',KEYS[1],'1') redis.call('SET',KEYS[2],'2') return 1",
            "2",
            "e1",
            "e2"
        ]],
        2
    ),
    row!(
        "active expiry of 1 key",
        [["SET", "ex", "v", "PX", "50"]],
        [["SLEEP", "1500"]],
        0
    ),
    row!(
        "HSET existing, 1 new field",
        [["HSET", "h4", "a", "1"]],
        [["HSET", "h4", "b", "2"]],
        1
    ),
    row!(
        "HINCRBY existing",
        [["HSET", "h5", "a", "1"]],
        [["HINCRBY", "h5", "a", "1"]],
        1
    ),
    row!(
        "HDEL existing field",
        [["HSET", "h6", "a", "1", "b", "2"]],
        [["HDEL", "h6", "a"]],
        1
    ),
    row!(
        "RPUSH existing, 2 elems",
        [["RPUSH", "l3", "a"]],
        [["RPUSH", "l3", "b", "c"]],
        2
    ),
    row!(
        "LPOP existing",
        [["RPUSH", "l4", "a", "b"]],
        [["LPOP", "l4"]],
        1
    ),
    row!(
        "LSET",
        [["RPUSH", "l5", "a", "b"]],
        [["LSET", "l5", "0", "z"]],
        1
    ),
    row!(
        "SADD existing, 1 new",
        [["SADD", "s2", "a"]],
        [["SADD", "s2", "b"]],
        1
    ),
    row!(
        "SREM existing",
        [["SADD", "s3", "a", "b"]],
        [["SREM", "s3", "a"]],
        1
    ),
    row!(
        "ZADD existing, 1 new",
        [["ZADD", "z2", "1", "a"]],
        [["ZADD", "z2", "2", "b"]],
        1
    ),
    row!(
        "ZINCRBY existing",
        [["ZADD", "z3", "1", "a"]],
        [["ZINCRBY", "z3", "1", "a"]],
        1
    ),
    row!(
        "ZREM existing",
        [["ZADD", "z4", "1", "a", "2", "b"]],
        [["ZREM", "z4", "a"]],
        1
    ),
    row!("XADD", [], [["XADD", "st", "*", "f", "v"]], 1),
    row!("PFADD 2", [], [["PFADD", "hll", "a", "b"]], 3),
    row!("SETBIT", [], [["SETBIT", "bits", "7", "1"]], 1),
    row!(
        "SETRANGE existing",
        [["SET", "sr", "hello"]],
        [["SETRANGE", "sr", "1", "a"]],
        1
    ),
    row!(
        "EXPIRE existing",
        [["SET", "e1", "v"]],
        [["EXPIRE", "e1", "100"]],
        1
    ),
    row!(
        "PERSIST existing TTL",
        [["SET", "p1", "v", "EX", "100"]],
        [["PERSIST", "p1"]],
        1
    ),
    row!("RENAME", [["SET", "rn", "v"]], [["RENAME", "rn", "rn2"]], 1),
    row!("GETDEL", [["SET", "gd", "v"]], [["GETDEL", "gd"]], 1),
    row!("COPY", [["SET", "cp", "v"]], [["COPY", "cp", "cp2"]], 1),
    row!(
        "SUNIONSTORE",
        [["SADD", "su1", "a"], ["SADD", "su2", "b"]],
        [["SUNIONSTORE", "sud", "su1", "su2"]],
        1
    ),
    row!(
        "LMOVE",
        [["RPUSH", "lm1", "a", "b"]],
        [["LMOVE", "lm1", "lm2", "LEFT", "RIGHT"]],
        1
    ),
    // Beyond the reviewer's table.
    row!(
        "HDEL every field",
        [["HSET", "h7", "a", "1", "b", "2"]],
        [["HDEL", "h7", "a", "b"]],
        2
    ),
    row!(
        "LTRIM 3 of 5",
        [["RPUSH", "l6", "a", "b", "c", "d", "e"]],
        [["LTRIM", "l6", "1", "2"]],
        3
    ),
    row!(
        "LMPOP 2",
        [["RPUSH", "l7", "a", "b", "c"]],
        [["LMPOP", "1", "l7", "LEFT", "COUNT", "2"]],
        2
    ),
    row!(
        "SPOP count 5 of 3",
        [["SADD", "s4", "a", "b", "c"]],
        [["SPOP", "s4", "5"]],
        3
    ),
    row!(
        "SMOVE to a new set",
        [["SADD", "s5", "a"]],
        [["SMOVE", "s5", "s6", "a"]],
        2
    ),
    row!(
        "ZADD same score",
        [["ZADD", "z5", "1", "a"]],
        [["ZADD", "z5", "1", "a"]],
        0
    ),
    row!(
        "ZPOPMIN 2",
        [["ZADD", "z6", "1", "a", "2", "b", "3", "c"]],
        [["ZPOPMIN", "z6", "2"]],
        2
    ),
    row!(
        "ZUNIONSTORE",
        [["ZADD", "za", "1", "x"], ["ZADD", "zb", "1", "y"]],
        [["ZUNIONSTORE", "zd", "2", "za", "zb"]],
        1
    ),
    row!(
        "ZINTERSTORE empty, no dst",
        [["ZADD", "zc", "1", "x"], ["ZADD", "ze", "1", "y"]],
        [["ZINTERSTORE", "zf", "2", "zc", "ze"]],
        0
    ),
    row!(
        "XTRIM 2",
        [
            ["XADD", "st2", "1-1", "f", "v"],
            ["XADD", "st2", "1-2", "f", "v"],
            ["XADD", "st2", "1-3", "f", "v"]
        ],
        [["XTRIM", "st2", "MAXLEN", "1"]],
        2
    ),
    row!(
        "GEOADD 2",
        [],
        [[
            "GEOADD", "geo", "13.36", "38.11", "a", "15.08", "37.50", "b"
        ]],
        2
    ),
    row!(
        "BLPOP ready",
        [["RPUSH", "bl", "a"]],
        [["BLPOP", "bl", "1"]],
        1
    ),
    row!(
        "BLMPOP ready",
        [["RPUSH", "bm", "a", "b"]],
        [["BLMPOP", "1", "1", "bm", "LEFT", "COUNT", "5"]],
        2
    ),
    row!(
        "BLPOP parked, served by RPUSH",
        [],
        [
            ["PARK", "BLPOP", "wl", "5"],
            ["SLEEP", "300"],
            ["RPUSH", "wl", "x"],
            ["SLEEP", "300"]
        ],
        1
    ),
    row!("MOVE", [["SET", "mv", "1"]], [["MOVE", "mv", "1"]], 1),
    row!(
        "FLUSHDB 3 keys",
        [
            ["FLUSHDB"],
            ["SET", "f1", "1"],
            ["SET", "f2", "1"],
            ["SET", "f3", "1"]
        ],
        [["FLUSHDB"]],
        3
    ),
    // Review 5 (moon#1232): a blocking read served at once counts as its
    // non-blocking twin.
    row!(
        "XREADGROUP BLOCK served at once",
        [
            ["XADD", "xb", "1-1", "f", "v"],
            ["XADD", "xb", "1-2", "f", "v"],
            ["XGROUP", "CREATE", "xb", "g", "0"],
            ["XGROUP", "CREATECONSUMER", "xb", "g", "c"]
        ],
        [[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "100",
            "STREAMS",
            "xb",
            ">"
        ]],
        1
    ),
    // SWAPDB is one change, even onto itself. These rows come last: they
    // leave db 0 holding the other database.
    row!(
        "SWAPDB 0 1",
        [["SET", "sw", "1"]],
        [["SWAPDB", "0", "1"]],
        1
    ),
    row!("SWAPDB 0 0", [], [["SWAPDB", "0", "0"]], 1),
];

fn changes(c: &mut Conn) -> u64 {
    info_field(c, "rdb_changes_since_last_save")
        .parse()
        .expect("numeric rdb_changes_since_last_save")
}

/// Run `cmds`; `PARK`ed commands go out on fresh connections, returned so the
/// caller reads their replies once the row is done.
fn run_cmds(c: &mut Conn, port: u16, cmds: &[&[&str]]) -> Vec<Conn> {
    use std::io::Write;
    let mut parked = Vec::new();
    for cmd in cmds {
        match cmd[0] {
            "SLEEP" => std::thread::sleep(Duration::from_millis(cmd[1].parse().unwrap())),
            "PARK" => {
                let mut p = Conn::open(port);
                p.sock.write_all(&common::encode(&cmd[1..])).unwrap();
                parked.push(p);
            }
            _ => {
                let _ = c.send(cmd);
            }
        }
    }
    parked
}

/// Each row's delta on the server at `port`, as `(row, delta)`.
fn measure(port: u16) -> Vec<(&'static Row, u64)> {
    let mut c = Conn::open(port);
    ROWS.iter()
        .map(|row| {
            for mut p in run_cmds(&mut c, port, row.setup) {
                p.read_replies(1);
            }
            let before = changes(&mut c);
            let parked = run_cmds(&mut c, port, row.measured);
            let delta = changes(&mut c) - before;
            for mut p in parked {
                p.read_replies(1);
            }
            (row, delta)
        })
        .collect()
}

fn table_mismatches(measured: &[(&Row, u64)], who: &str) -> Vec<String> {
    measured
        .iter()
        .filter(|(row, got)| *got != row.redis)
        .map(|(row, got)| {
            format!(
                "{:<32} redis 7.0.15 {:>2}   {who} {got:>2}",
                row.name, row.redis
            )
        })
        .collect()
}

#[test]
fn dirty_count_matches_redis_7_0_15() {
    let dir = common::unique_test_dir("ws19-1232-parity");
    std::fs::create_dir_all(&dir).unwrap();
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
            .args([
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--disk-offload",
                "disable",
            ])
            .args(["--maxmemory", "0", "--disk-free-min-pct", "0"])
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let mut server = ServerGuard::new(child);
    let wrong = table_mismatches(&measure(port), "moon");
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        wrong.is_empty(),
        "rdb_changes_since_last_save deltas differ from redis:\n{}",
        wrong.join("\n")
    );
}

/// The oracle: the table's expectations measured on a real `redis-server`.
#[test]
#[ignore] // Needs redis-server (7.0.15) on PATH; run explicitly.
fn dirty_count_oracle_redis_agrees() {
    let dir = common::unique_test_dir("ws19-1232-oracle");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = common::spawn_listening_guarded(|port| {
        std::process::Command::new("redis-server")
            .args([
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .arg("--dir")
            .arg(&dir)
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn redis-server (on PATH)")
    });
    let wrong = table_mismatches(&measure(port), "redis-server");
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        wrong.is_empty(),
        "the table is not redis's:\n{}",
        wrong.join("\n")
    );
}

// ── A read never counts ─────────────────────────────────────────────────────

/// GETs of cold keys promote them into RAM, and the promotions evict other
/// keys to make room; neither is a change. Before the review, 200 such GETs
/// moved the count the `--save` trigger reads by ~165.
#[test]
fn cold_reads_do_not_count_as_changes() {
    const PROBES: usize = 200;
    const FILLERS: usize = 16_000;
    let dir = common::unique_test_dir("ws19-1232-coldreads");
    std::fs::create_dir_all(dir.join("off")).unwrap();
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
            .args(["--shards", "1", "--disk-free-min-pct", "0"])
            .args(["--maxmemory", &(8 * 1024 * 1024).to_string()])
            .args([
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
            ])
            .arg("--disk-offload-dir")
            .arg(dir.join("off"))
            // Only an AOF server spills on eviction (others drop).
            .args(["--appendonly", "yes"])
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let mut server = ServerGuard::new(child);
    let mut c = Conn::open(port);
    let probe = "P".repeat(500);
    let filler = "F".repeat(600);
    for chunk in 0..(PROBES + FILLERS) / 1000 + 1 {
        let cmds: Vec<Vec<String>> = (chunk * 1000..((chunk + 1) * 1000).min(PROBES + FILLERS))
            .map(|i| {
                if i < PROBES {
                    vec!["SET".into(), format!("probe:{i}"), probe.clone()]
                } else {
                    vec!["SET".into(), format!("filler:{i}"), filler.clone()]
                }
            })
            .collect();
        let refs: Vec<Vec<&str>> = cmds
            .iter()
            .map(|v| v.iter().map(String::as_str).collect())
            .collect();
        let parts: Vec<&[&str]> = refs.iter().map(Vec::as_slice).collect();
        if !parts.is_empty() {
            c.pipeline(&parts);
        }
    }
    let spilled = |c: &mut Conn| -> u64 {
        let info = c.send(&["INFO", "stats"]);
        info.lines()
            .find_map(|l| l.strip_prefix("spilled_keys:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while spilled(&mut c) < 1000 {
        assert!(
            Instant::now() < deadline,
            "precondition: nothing spilled in 30 s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    // Let the spill and eviction settle, then prove the count is idle.
    std::thread::sleep(Duration::from_secs(2));
    let before = changes(&mut c);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(changes(&mut c), before, "precondition: the count is idle");
    let mut hits = 0;
    for i in 0..PROBES {
        let reply = c.send(&["GET", &format!("probe:{i}")]);
        if reply.starts_with('$') && !reply.starts_with("$-1") {
            hits += 1;
        }
    }
    std::thread::sleep(Duration::from_secs(1));
    let after = changes(&mut c);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        hits > PROBES / 2,
        "precondition: only {hits} probe GETs answered"
    );
    assert_eq!(
        after - before,
        0,
        "{PROBES} GETs of cold keys (no write at all) moved rdb_changes_since_last_save by {}",
        after - before
    );
}

// ── SHUTDOWN while an auto-save runs ────────────────────────────────────────

fn spawn_saving(dir: &std::path::Path, save: &str) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
            .args([
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--disk-offload",
                "disable",
            ])
            .args([
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
                "--save",
                save,
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

/// moon#1232 makes sharded auto-saves run, which makes this reachable:
/// `SHUTDOWN` (default mode with save points) while an auto-save is in
/// progress used to answer "Background save already in progress" and leave
/// the server running. redis 7.0.15's `prepareForShutdown` kills the saving
/// child and saves synchronously; SHUTDOWN never fails because a save is
/// running. moon waits for the running save and saves again: a key written
/// after the auto-save started — which only the SHUTDOWN save can hold — is
/// there after a restart.
#[test]
fn shutdown_during_an_auto_save_is_not_refused() {
    use std::io::{Read, Write};
    const KEYS: usize = 400_000;
    let dir = common::unique_test_dir("ws19-1232-shutdown");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn_saving(&dir, "1 1");
    let mut c = Conn::open(port);
    // A dataset whose snapshot takes a while.
    for chunk in 0..KEYS / 10_000 {
        let cmds: Vec<[String; 3]> = (0..10_000)
            .map(|i| {
                [
                    "SET".into(),
                    format!("k:{chunk}:{i}"),
                    "vvvvvvvvvvvvvvvv".into(),
                ]
            })
            .collect();
        let parts: Vec<Vec<&str>> = cmds
            .iter()
            .map(|c| c.iter().map(String::as_str).collect())
            .collect();
        let refs: Vec<&[&str]> = parts.iter().map(Vec::as_slice).collect();
        c.pipeline(&refs);
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while info_field(&mut c, "rdb_bgsave_in_progress") != "1" {
        assert!(Instant::now() < deadline, "no auto-save started");
        std::thread::sleep(Duration::from_millis(2));
    }
    // Only a save started after this write can hold it.
    assert!(
        c.send(&["SET", "marker", "after-the-auto-save-started"])
            .starts_with("+OK")
    );
    c.sock.write_all(&common::encode(&["SHUTDOWN"])).unwrap();
    c.sock
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    let mut buf = [0u8; 256];
    let reply = match c.sock.read(&mut buf) {
        Ok(n) => String::from_utf8_lossy(&buf[..n]).into_owned(),
        Err(e) => format!("<read error {e}>"),
    };
    assert!(
        !reply.starts_with('-'),
        "SHUTDOWN during an auto-save was refused: {reply}"
    );
    let exited = (0..600).any(|_| {
        std::thread::sleep(Duration::from_millis(100));
        matches!(server.as_mut().try_wait(), Ok(Some(_)))
    });
    assert!(
        exited,
        "SHUTDOWN during an auto-save: the server did not exit in 60 s"
    );
    drop(server);

    let (mut again, port) = spawn_saving(&dir, "");
    let mut c = Conn::open(port);
    let marker = c.send(&["GET", "marker"]);
    let dbsize = c.send(&["DBSIZE"]);
    again.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        marker.contains("after-the-auto-save-started"),
        "the SHUTDOWN save does not hold a write made while the auto-save ran: {marker:?}"
    );
    assert_eq!(dbsize.trim(), format!(":{}", KEYS + 1));
}

// ── Review 5: paths outside the dispatch arms ──────────────────────────────

fn spawn_with(dir: &std::path::Path, shards: usize, extra: &[&str]) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
            .args(["--shards", &shards.to_string(), "--maxmemory", "0"])
            .args(["--disk-offload", "disable", "--disk-free-min-pct", "0"])
            .args(extra)
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

/// A blocking pop that finds data is its non-blocking twin (redis 7.0.15:
/// BLPOP 1, BZPOPMIN 1). At `--shards 4` a key owned by another shard was
/// served by that shard's `BlockRegister` handler through the parked-waiter
/// serve (`serve_list_key` / `serve_zset_key`), which is muted: 0.
#[test]
fn blocking_pops_served_at_once_count_at_four_shards() {
    let dir = common::unique_test_dir("ws19-r5-bpop");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn_with(&dir, 4, &["--appendonly", "no"]);
    let mut c = Conn::open(port);
    let mut got = Vec::new();
    for i in 0..16 {
        let (l, z) = (format!("q{i}"), format!("zq{i}"));
        c.send(&["RPUSH", &l, "a", "b"]);
        c.send(&["ZADD", &z, "1", "a"]);
        let d0 = changes(&mut c);
        let blpop = c.send(&["BLPOP", &l, "1"]);
        let d1 = changes(&mut c);
        let bzpop = c.send(&["BZPOPMIN", &z, "1"]);
        let d2 = changes(&mut c);
        assert!(
            blpop.starts_with("*2") && bzpop.starts_with("*3"),
            "{blpop:?} {bzpop:?}"
        );
        got.push(format!("{}{}", d1 - d0, d2 - d1));
    }
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        got.join(" "),
        ["11"; 16].join(" "),
        "(BLPOP, BZPOPMIN) change counts per key, --shards 4"
    );
}

/// `XREADGROUP ... BLOCK` that finds entries is served at once outside the
/// dispatch arm's `counted` wrapper: it counted 0, where the same read
/// without BLOCK counts 1 (redis 7.0.15: 2, the stream plus the consumer it
/// creates — a known difference, see `command::keyspace_changes`). The table
/// row "XREADGROUP BLOCK served at once" pins the exact count with an
/// existing consumer.
#[test]
fn xreadgroup_block_served_at_once_counts_like_xreadgroup() {
    let dir = common::unique_test_dir("ws19-r5-xrg");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn_with(&dir, 1, &["--appendonly", "no"]);
    let mut c = Conn::open(port);
    c.send(&["XGROUP", "CREATE", "xs", "g", "$", "MKSTREAM"]);
    c.send(&["XADD", "xs", "1-1", "f", "v"]);
    c.send(&["XADD", "xs", "1-2", "f", "v"]);
    let d0 = changes(&mut c);
    let r = c.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "BLOCK",
        "100",
        "STREAMS",
        "xs",
        ">",
    ]);
    let delta = changes(&mut c) - d0;
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(r.starts_with("*1"), "served at once: {r:?}");
    assert!(
        delta >= 1,
        "XREADGROUP BLOCK served 2 entries at once: {delta}"
    );
}

fn wait_until(what: &str, mut ok: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !ok() {
        assert!(t0.elapsed() < Duration::from_secs(20), "timed out: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `SWAPDB` is one change in redis 7.0.15. moon counted 0, so with RDB-only
/// persistence a `--save "N 1"` rule never persisted a lone SWAPDB and a
/// crash brought the old layout back.
#[test]
fn a_lone_swapdb_is_saved_by_a_save_rule() {
    let dir = common::unique_test_dir("ws19-r5-swapdb");
    std::fs::create_dir_all(&dir).unwrap();
    let args = ["--appendonly", "no", "--save", "1 1"];
    let (mut first, port) = spawn_with(&dir, 1, &args);
    let mut c = Conn::open(port);
    assert!(c.send(&["SET", "a", "1"]).starts_with("+OK"));
    wait_until("the SET is saved", || changes(&mut c) == 0);
    let before = changes(&mut c);
    assert_eq!(c.send(&["SWAPDB", "0", "1"]), "+OK\r\n");
    let delta = changes(&mut c) - before;
    // The rule saves the swap if it counted (the count drops back to 0 when
    // that save completes); then crash.
    let deadline = Instant::now() + Duration::from_secs(10);
    while changes(&mut c) != 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    first.kill_now();
    drop(first);
    // Booted with save points, as the snapshot is loaded only then.
    let (mut again, port) = spawn_with(&dir, 1, &["--appendonly", "no", "--save", "3600 1"]);
    let mut c = Conn::open(port);
    let in_db0 = c.send(&["EXISTS", "a"]);
    c.send(&["SELECT", "1"]);
    let in_db1 = c.send(&["EXISTS", "a"]);
    again.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(delta, 1, "SWAPDB 0 1");
    assert_eq!(
        (in_db0.trim(), in_db1.trim()),
        (":0", ":1"),
        "after kill -9 the saved layout must have the key in db 1"
    );
}
