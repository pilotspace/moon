//! moon#1184, end to end at `--shards 4 --appendonly yes`: a spanning
//! `MSET` / `DEL` / `UNLINK` logs at most ONE record per owner shard (it
//! logged one per key: an `MSET` of 40 pairs wrote ~30 `SET` records on the
//! remote owners), a spanning `DEL` of absent keys logs nothing, and the
//! merged records replay to the same keyspace after a crash.
//!
//! Red on `ae21476` (`MOON_BIN=/home/user/wt/bin/baseline-ae21476`): the
//! record counts exceed the owner count.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws8_spanning_write_merge`.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

const SHARDS: usize = 4;
const KEYS: usize = 40;

fn spawn(dir: &std::path::Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &SHARDS.to_string(),
                "--appendonly",
                "yes",
                "--save",
                "",
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
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn owner(key: &str) -> usize {
    moon::shard::dispatch::key_to_shard(key.as_bytes(), SHARDS)
}

/// Every AOF byte on disk (all shards, all generations).
fn aof_bytes(dir: &std::path::Path) -> Vec<u8> {
    fn walk(p: &std::path::Path, out: &mut Vec<u8>) {
        let Ok(entries) = std::fs::read_dir(p) else {
            return;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.to_string_lossy().contains(".aof") {
                out.extend_from_slice(&std::fs::read(&path).unwrap_or_default());
            }
        }
    }
    let mut out = Vec::new();
    walk(&dir.join("appendonlydir"), &mut out);
    out
}

fn count(hay: &[u8], needle: &[u8]) -> usize {
    hay.windows(needle.len()).filter(|w| *w == needle).count()
}

/// A key owned by `shard`, distinct per `tag`.
fn key_on(shard: usize, tag: &str) -> String {
    (0..)
        .map(|i| format!("{tag}:{i}"))
        .find(|k| owner(k) == shard)
        .unwrap()
}

/// Write a marker on every shard and wait until each is in the AOF: each
/// shard's AOF is FIFO, so every record appended before it is on disk too.
fn sync_every_shard(c: &mut Conn, dir: &std::path::Path, round: &str) {
    let markers: Vec<String> = (0..SHARDS)
        .map(|s| key_on(s, &format!("marker-{round}")))
        .collect();
    for m in &markers {
        assert_eq!(c.send(&["SET", m, "sync"]), "+OK\r\n");
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let aof = aof_bytes(dir);
        if markers.iter().all(|m| count(&aof, m.as_bytes()) > 0) {
            return;
        }
        assert!(Instant::now() < deadline, "markers never reached the AOF");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn spanning_writes_log_one_record_per_owner_and_replay() {
    let dir = common::unique_test_dir("ws8-merge");
    let keys: Vec<String> = (0..KEYS).map(|i| format!("mk:{i}")).collect();
    let owners: std::collections::BTreeSet<usize> = keys.iter().map(|k| owner(k)).collect();
    assert_eq!(owners.len(), SHARDS, "the key set must span every shard");
    let (mut server, port) = spawn(&dir);
    let mut c = Conn::open(port);

    // MSET over every key.
    let mut mset: Vec<String> = vec!["MSET".into()];
    for (i, k) in keys.iter().enumerate() {
        mset.push(k.clone());
        mset.push(format!("val{i}"));
    }
    let parts: Vec<&str> = mset.iter().map(String::as_str).collect();
    assert_eq!(c.send(&parts), "+OK\r\n");

    // DEL half of them, UNLINK a quarter, DEL keys that do not exist.
    let mut del: Vec<&str> = vec!["DEL"];
    del.extend(keys[..20].iter().map(String::as_str));
    assert_eq!(c.send(&del), ":20\r\n");
    let mut unlink: Vec<&str> = vec!["UNLINK"];
    unlink.extend(keys[20..30].iter().map(String::as_str));
    assert_eq!(c.send(&unlink), ":10\r\n");
    let absent: Vec<String> = (0..KEYS).map(|i| format!("absent:{i}")).collect();
    let mut del_absent: Vec<&str> = vec!["DEL"];
    del_absent.extend(absent.iter().map(String::as_str));
    assert_eq!(c.send(&del_absent), ":0\r\n");

    sync_every_shard(&mut c, &dir, "a");
    let aof = aof_bytes(&dir);
    let mset_records = count(&aof, b"$4\r\nMSET\r\n");
    // Every plain SET record but the SHARDS sync markers is a per-key MSET leg.
    let set_records_for_keys = count(&aof, b"$3\r\nSET\r\n").saturating_sub(SHARDS);
    let del_records = count(&aof, b"$3\r\nDEL\r\n");
    let unlink_records = count(&aof, b"$6\r\nUNLINK\r\n");
    let absent_logged = count(&aof, b"absent:");
    eprintln!(
        "AOF records: MSET={mset_records} per-key SET={set_records_for_keys} \
         DEL={del_records} UNLINK={unlink_records} absent-key mentions={absent_logged}"
    );
    assert!(
        mset_records <= SHARDS && set_records_for_keys == 0,
        "one spanning MSET must log at most one MSET per owner shard: {mset_records} MSET \
         records, {set_records_for_keys} per-key SET records"
    );
    let del_owners = keys[..20]
        .iter()
        .map(|k| owner(k))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let unlink_owners = keys[20..30]
        .iter()
        .map(|k| owner(k))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    assert!(
        del_records <= del_owners && unlink_records <= unlink_owners,
        "a spanning DEL/UNLINK must log at most one record per owner shard: \
         {del_records} DEL records ({del_owners} owners), {unlink_records} UNLINK records \
         ({unlink_owners} owners)"
    );
    assert_eq!(
        absent_logged, 0,
        "a spanning DEL that deleted nothing must log nothing"
    );

    // The merged records replay to the same keyspace.
    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir);
    let mut c2 = Conn::open(port2);
    for (i, k) in keys.iter().enumerate() {
        let want = if i < 30 {
            "$-1\r\n".to_string()
        } else {
            let v = format!("val{i}");
            format!("${}\r\n{v}\r\n", v.len())
        };
        assert_eq!(c2.send(&["GET", k]), want, "{k} after replay");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
