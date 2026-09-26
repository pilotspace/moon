//! WS20 review round: real-server regression tests cheap enough for every PR.
//!
//! - moon#1275: a SWAPDB made a tokio `--shards 1` restart skip the AOF.
//! - moon#1277: a TTL-preserving read-modify-write replayed after its key's
//!   deadline built a new persistent key.
//! - moon#1278: a replica applied its master's SWAPDB over its own cold tier.
//!
//! Pin the binary for a specific runtime:
//! `MOON_BIN=<moon> cargo test --test perf_ws20_review`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

fn spawn(dir: &Path, shards: usize, extra: &[&str]) -> (ServerGuard, u16) {
    spawn_bin(common::find_moon_binary(), dir, shards, extra)
}

fn spawn_bin(
    bin: std::path::PathBuf,
    dir: &Path,
    shards: usize,
    extra: &[&str],
) -> (ServerGuard, u16) {
    let d = dir.to_path_buf();
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn bulk(reply: &str) -> Option<String> {
    if reply.starts_with("$-1") {
        return None;
    }
    reply.split("\r\n").nth(1).map(str::to_string)
}

/// moon#1275: on the tokio `--shards 1` layout the SWAPDB local leg wrote its
/// record to WAL v3 whatever `--wal-kv-log` said. That record was then the
/// only KV record in the WAL, so recovery took the WAL for the KV authority
/// and never replayed `appendonly.aof`: every key written before AND after
/// the swap was gone after a kill -9. Correct on every layout: each key is
/// back in the database the swap put it in.
fn swapdb_survives_a_crash(shards: usize, offload: &str) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let args = [
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
        "--disk-offload",
        offload,
    ];
    let (mut server, port) = spawn(dir, shards, &args);
    let mut c = Conn::open(port);
    assert!(c.send(&["SET", "a", "1"]).starts_with("+OK"));
    assert_eq!(c.send(&["SWAPDB", "0", "1"]), "+OK\r\n");
    assert!(c.send(&["SET", "b", "2"]).starts_with("+OK"));
    assert!(c.send(&["SELECT", "1"]).starts_with("+OK"));
    assert!(c.send(&["SET", "c", "3"]).starts_with("+OK"));
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);

    let (_server2, port) = spawn(dir, shards, &args);
    let mut c = Conn::open(port);
    let db0 = (bulk(&c.send(&["GET", "a"])), bulk(&c.send(&["GET", "b"])));
    assert!(c.send(&["SELECT", "1"]).starts_with("+OK"));
    let db1 = (bulk(&c.send(&["GET", "a"])), bulk(&c.send(&["GET", "c"])));
    assert_eq!(
        (db0, db1),
        (
            (None, Some("2".to_owned())),
            (Some("1".to_owned()), Some("3".to_owned()))
        ),
        "after SET a; SWAPDB 0 1; SET b (db 0); SET c (db 1); kill -9; restart at \
         --shards {shards}, offload {offload}: ((db0 a, db0 b), (db1 a, db1 c))"
    );
}

#[test]
fn moon_1275_a_swapdb_does_not_make_a_restart_skip_the_aof_s1() {
    swapdb_survives_a_crash(1, "enable");
    swapdb_survives_a_crash(1, "disable");
}

#[test]
fn moon_1275_a_swapdb_does_not_make_a_restart_skip_the_aof_s4() {
    swapdb_survives_a_crash(4, "enable");
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Whether `dir` holds a base with seq > 1 (a completed rewrite).
fn has_compacted_base(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|files| {
        files.flatten().any(|f| {
            let name = f.file_name().to_string_lossy().to_string();
            name.strip_prefix("moon.aof.")
                .and_then(|r| r.strip_suffix(".base.rdb"))
                .and_then(|seq| seq.parse::<u64>().ok())
                .is_some_and(|seq| seq > 1)
        })
    })
}

/// AOF generations cut by a completed rewrite: per-shard dirs, the top-level
/// multi-part dir, or the tokio `--shards 1` flat file with its preamble.
fn compacted_bases(dir: &Path) -> usize {
    let aof_dir = dir.join("appendonlydir");
    let per_shard = std::fs::read_dir(&aof_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|s| s.path().is_dir() && has_compacted_base(&s.path()))
                .count()
        })
        .unwrap_or(0);
    let flat = std::fs::read(dir.join("appendonly.aof")).is_ok_and(|b| b.starts_with(b"MOON"));
    per_shard + usize::from(has_compacted_base(&aof_dir)) + usize::from(flat)
}

fn rewrite_and_wait(c: &mut Conn, dir: &Path, shards: usize) {
    assert!(!c.send(&["BGREWRITEAOF"]).starts_with('-'));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let idle = c
            .send(&["INFO", "persistence"])
            .contains("aof_rewrite_in_progress:0");
        let done = compacted_bases(dir);
        // The per-shard layout cuts one base per shard, the others one.
        if idle && (done == shards || done == 1) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "BGREWRITEAOF did not complete within 60s ({done} bases)"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// moon#1277: keys with a TTL, then a TTL-preserving read-modify-write on
/// each (APPEND, INCR, HSET, SETRANGE) logged while they were alive, then a
/// kill -9 and a restart AFTER the TTL. The replay judged expiry by the wall
/// clock: every RMW replayed onto an absent key and built a NEW key with no
/// TTL, which came back with a wrong value and never expired. Every key must
/// be gone, as with redis. With `rewrite`, the SETs are in the AOF base and
/// the RMWs in the incremental file.
fn rmw_logged_before_the_ttl_does_not_outlive_it(shards: usize, rewrite: bool) {
    const TTL_MS: u64 = 3_000;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let args = [
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
        "--disk-offload",
        "enable",
    ];
    let (mut server, port) = spawn(dir, shards, &args);
    let mut c = Conn::open(port);
    // With a `rewrite` the TTL has to outlive BGREWRITEAOF, so the deadline
    // is set after it: `PEXPIREAT` on keys the base already holds.
    let keys = ["s", "n", "h", "r"];
    assert!(
        c.send(&["SET", "s", "v", "PX", "600000"])
            .starts_with("+OK")
    );
    assert!(
        c.send(&["SET", "n", "5", "PX", "600000"])
            .starts_with("+OK")
    );
    assert_eq!(c.send(&["HSET", "h", "f", "1"]), ":1\r\n");
    assert_eq!(c.send(&["PEXPIRE", "h", "600000"]), ":1\r\n");
    assert!(
        c.send(&["SET", "r", "abc", "PX", "600000"])
            .starts_with("+OK")
    );
    // The control: a persistent key, so a pass means the log WAS replayed.
    assert!(c.send(&["SET", "p", "v"]).starts_with("+OK"));
    if rewrite {
        rewrite_and_wait(&mut c, dir, shards);
    }
    let deadline_ms = now_ms() + TTL_MS;
    let at = deadline_ms.to_string();
    for k in keys {
        assert_eq!(c.send(&["PEXPIREAT", k, &at]), ":1\r\n", "PEXPIREAT {k}");
    }
    let rmw = [
        c.send(&["APPEND", "s", "x"]),
        c.send(&["INCR", "n"]),
        c.send(&["HSET", "h", "g", "2"]),
        c.send(&["SETRANGE", "r", "1", "Z"]),
    ];
    assert_eq!(c.send(&["APPEND", "p", "x"]), ":2\r\n");
    let live: Vec<String> = keys.iter().map(|k| c.send(&["PTTL", k])).collect();
    assert_eq!(
        rmw,
        [":2\r\n", ":6\r\n", ":1\r\n", ":3\r\n"].map(str::to_owned),
        "the RMWs must land on the live keys (a slow setup outran the {TTL_MS} ms TTL: \
         PTTLs {live:?})"
    );
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);
    while now_ms() <= deadline_ms + 200 {
        std::thread::sleep(Duration::from_millis(50));
    }

    let (_server2, port) = spawn(dir, shards, &args);
    let mut c = Conn::open(port);
    let back: Vec<(&str, String, String)> = keys
        .iter()
        .map(|k| (*k, c.send(&["TYPE", k]), c.send(&["PTTL", k])))
        .filter(|(_, t, _)| t != "+none\r\n")
        .collect();
    let control = bulk(&c.send(&["GET", "p"]));
    assert!(
        back.is_empty() && control.as_deref() == Some("vx"),
        "--shards {shards}, rewrite {rewrite}: every key's TTL passed before the restart, \
         yet these came back (key, TYPE, PTTL): {back:?}; the persistent control \
         reads {control:?} (\"vx\" once the log is replayed)"
    );
}

#[test]
fn moon_1277_an_rmw_replayed_after_the_ttl_does_not_resurrect_the_key_s1() {
    rmw_logged_before_the_ttl_does_not_outlive_it(1, false);
}

#[test]
fn moon_1277_an_rmw_replayed_after_the_ttl_does_not_resurrect_the_key_s1_rewrite() {
    rmw_logged_before_the_ttl_does_not_outlive_it(1, true);
}

#[test]
fn moon_1277_an_rmw_replayed_after_the_ttl_does_not_resurrect_the_key_s4() {
    rmw_logged_before_the_ttl_does_not_outlive_it(4, false);
}

#[test]
fn moon_1277_an_rmw_replayed_after_the_ttl_does_not_resurrect_the_key_s4_rewrite() {
    rmw_logged_before_the_ttl_does_not_outlive_it(4, true);
}

/// Poll `cond` every 100 ms for up to `secs`; whether it held.
fn wait_for(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    cond()
}

fn int_reply(reply: &str) -> i64 {
    reply.trim().trim_start_matches(':').parse().unwrap_or(-1)
}

/// `DBSIZE` of `db`, or -1 while the server refuses (a replica loading).
fn dbsize(c: &mut Conn, db: usize) -> i64 {
    let replies = c.pipeline(&[&["SELECT", &db.to_string()], &["DBSIZE"]]);
    match replies.split_once("\r\n") {
        Some(("+OK", size)) => int_reply(size),
        _ => -1,
    }
}

/// Spill files (`heap-*.mpf`) anywhere under `dir`.
fn heap_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .map(|p| {
                    if p.is_dir() {
                        heap_files(&p)
                    } else {
                        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                        usize::from(name.starts_with("heap-") && name.ends_with(".mpf"))
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

fn info_field(c: &mut Conn, section: &str, field: &str) -> Option<String> {
    let info = c.send(&["INFO", section]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_owned())
}

/// moon#1278 (REVIEW-WS20 F3, `rv20_replica_swapdb.py`): a replica spills
/// under its own memory limit, and its spill files keep their db tags. It
/// applied its master's `SWAPDB 0 1` as a plain swap (the master checked
/// only ITS cold tier — it has none here), so after a failover, a rewrite
/// and a kill -9 of the promoted replica its cold keys came back in db 0 as
/// well. The replica must resync from scratch instead: every key in db 1,
/// none in db 0, live and after the restart.
///
/// A replication master needs runtime-monoio (a tokio master refuses
/// `PSYNC`), hence the gate; `MOON_REPL_MASTER_BIN=<monoio moon>` drives a
/// replica built for the other runtime (`MOON_BIN`).
#[cfg(feature = "runtime-monoio")]
#[test]
fn moon_1278_a_replica_resyncs_instead_of_swapping_over_its_cold_tier() {
    const PROBES: usize = 200;
    const FILLERS: usize = 16_000;
    let total = (PROBES + FILLERS) as i64;
    let (mtmp, rtmp) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let master_bin = std::env::var_os("MOON_REPL_MASTER_BIN")
        .map_or_else(common::find_moon_binary, std::path::PathBuf::from);
    let (_master, mport) = spawn_bin(
        master_bin,
        mtmp.path(),
        1,
        &["--appendonly", "yes", "--disk-offload", "disable"],
    );
    let replica_args = [
        "--appendonly",
        "yes",
        "--maxmemory",
        "8388608",
        "--maxmemory-policy",
        "allkeys-lru",
        "--disk-offload",
        "enable",
        "--cold-orphan-sweep-interval-secs",
        "3600",
    ];
    let (mut replica, rport) = spawn(rtmp.path(), 1, &replica_args);
    let mut r = Conn::open(rport);
    let mport_s = mport.to_string();
    assert!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport_s])
            .starts_with("+OK")
    );
    assert!(
        wait_for(60, || info_field(
            &mut r,
            "replication",
            "master_link_status"
        )
        .as_deref()
            == Some("up")),
        "the replica never linked"
    );

    let mut m = Conn::open(mport);
    let probe = "P".repeat(500);
    let filler = "F".repeat(600);
    let keys: Vec<String> = (0..PROBES)
        .map(|i| format!("probe:{i}"))
        .chain((0..FILLERS).map(|i| format!("filler:{i}")))
        .collect();
    for chunk in keys.chunks(1_000) {
        let cmds: Vec<[&str; 3]> = chunk
            .iter()
            .map(|k| {
                let v = if k.starts_with('p') { &probe } else { &filler };
                ["SET", k.as_str(), v.as_str()]
            })
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        m.pipeline(&refs);
    }
    // Applied, and spilled on the replica (its own tier; the master has none).
    assert!(
        wait_for(120, || dbsize(&mut r, 0) == total
            && heap_files(rtmp.path()) > 0),
        "the replica never applied and spilled the writes (db 0 {}, {} spill files)",
        dbsize(&mut r, 0),
        heap_files(rtmp.path())
    );

    let full_syncs =
        |m: &mut Conn| info_field(m, "stats", "sync_full").and_then(|v| v.parse::<u64>().ok());
    let syncs_before = full_syncs(&mut m);
    assert_eq!(m.send(&["SWAPDB", "0", "1"]), "+OK\r\n");
    assert!(
        wait_for(120, || dbsize(&mut r, 1) == total && dbsize(&mut r, 0) == 0),
        "after the master's SWAPDB 0 1 the replica holds db 0 {} / db 1 {}",
        dbsize(&mut r, 0),
        dbsize(&mut r, 1)
    );

    let resynced = full_syncs(&mut m) > syncs_before;
    // Failover: promote the replica, fold its state, crash it, restart it on
    // its own data.
    assert!(r.send(&["REPLICAOF", "NO", "ONE"]).starts_with("+OK"));
    rewrite_and_wait(&mut r, rtmp.path(), 1);
    drop(r);
    replica.kill_now();
    common::wait_for_port_down(rport);
    let (_replica2, rport) = spawn(rtmp.path(), 1, &replica_args);
    let mut r = Conn::open(rport);
    let (db0, db1) = (dbsize(&mut r, 0), dbsize(&mut r, 1));
    let probes_in = |c: &mut Conn, db: usize| {
        assert!(c.send(&["SELECT", &db.to_string()]).starts_with("+OK"));
        (0..PROBES)
            .filter(|i| int_reply(&c.send(&["EXISTS", &format!("probe:{i}")])) == 1)
            .count()
    };
    let (p0, p1) = (probes_in(&mut r, 0), probes_in(&mut r, 1));
    assert_eq!(
        (db0, p0, db1, p1, resynced),
        (0, 0, total, PROBES, true),
        "after the master's SWAPDB 0 1, a failover, BGREWRITEAOF and kill -9 of the \
         replica: (db 0 keys, db 0 probes, db 1 keys, db 1 probes, the SWAPDB made the \
         replica resync in full)"
    );
}
