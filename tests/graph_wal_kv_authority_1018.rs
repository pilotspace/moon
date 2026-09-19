//! moon#1018: one `GRAPH.*` write must not make recovery discard the AOF.
//!
//! tokio `--shards 1` is the one configuration with no `AofManifest`, so
//! recovery decides between two KV authorities itself: the shard's WAL v3 if
//! it replayed KV history, otherwise the legacy `appendonly.aof` (Phase 4b in
//! `persistence/recovery.rs`). Every `GRAPH.*` write lands in that WAL as a
//! `Command` record (`GraphStore::wal_pending`). Replay diverts it to the
//! graph collector, so it never touches the keyspace, but Phase 4 counted it
//! as KV history. So a single graph write made the WAL the "authority", and
//! every acknowledged KV write, which lived only in the AOF, was gone after a
//! `kill -9`. Measured before the fix: DBSIZE 3 -> 0, and every restart after
//! that lost the same keys again.
//!
//! Legs:
//!   1. `--shards 1`, `--wal-kv-log` at its default (off): the reported shape.
//!   2. `--shards 1`, `--wal-kv-log on`: connection-local writes still bypass
//!      the WAL, so it holds only graph records and must not be the authority.
//!   3. `--shards 4`: a PerShard manifest is the authority. Not reachable by
//!      the bug; this leg guards that the fix leaves it alone.
//!
//! Each leg runs two `kill -9` cycles, because the loss repeated on every
//! boot. It also waits until the graph record is durable in a WAL segment
//! before each kill, and legs 1-2 read the graph back after the restart, so
//! a run whose graph write never reached the WAL cannot pass.
//!
//! This lives in the configuration CI does not otherwise build:
//! `runtime-tokio` + `graph` (the tokio legs drop default features, `graph`
//! among them). `.github/workflows/ci.yml` runs this file under that exact
//! feature set. Under default features it drives monoio, where a manifest
//! makes every leg a still-valid guard. `--disk-free-min-pct 0` is REQUIRED:
//! the disk-free guard silently guts a crash test on a nearly-full volume.

#![cfg(all(
    feature = "graph",
    any(feature = "runtime-monoio", feature = "runtime-tokio")
))]
#![allow(clippy::unwrap_used)]

mod common;

use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// Size of a WAL v3 segment header. A segment longer than this holds records.
const WAL_SEGMENT_HEADER: u64 = 64;

fn start_moon(dir: &std::path::Path, shards: &str, extra: &[&str]) -> (Child, u16) {
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    let shards = shards.to_string();
    common::spawn_listening(move |port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards,
                "--dir",
                dir.to_str().unwrap(),
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                // REQUIRED — see the module doc.
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// Bytes of WAL records (segment headers excluded) under every `wal-v3/`.
fn wal_record_bytes(dir: &std::path::Path) -> u64 {
    fn walk(p: &std::path::Path, in_wal: bool, acc: &mut u64) {
        let Ok(rd) = std::fs::read_dir(p) else {
            return;
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                let is_wal = path.file_name().is_some_and(|n| n == "wal-v3");
                walk(&path, in_wal || is_wal, acc);
            } else if in_wal && path.extension().is_some_and(|x| x == "wal") {
                let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                *acc += len.saturating_sub(WAL_SEGMENT_HEADER);
            }
        }
    }
    let mut acc = 0;
    walk(dir, false, &mut acc);
    acc
}

/// Block until the WAL has grown past `before`, so the graph record written
/// since then is on disk at the kill. The WAL is flushed on a 1 ms tick. A
/// `write(2)` that has landed survives SIGKILL (the page cache outlives the
/// process), so the observed length is the durability point that matters here.
fn await_wal_growth(dir: &std::path::Path, before: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while wal_record_bytes(dir) <= before {
        assert!(
            Instant::now() < deadline,
            "the GRAPH.* write never reached a WAL v3 segment under {dir:?}; \
             without it this test proves nothing"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn bulk(reply: &str) -> Option<String> {
    let mut lines = reply.lines();
    let head = lines.next()?;
    if head.starts_with("$-1") || head.starts_with('_') {
        return None;
    }
    if head.starts_with('$') {
        return lines.next().map(|s| s.to_string());
    }
    Some(head.to_string())
}

fn run_leg(label: &str, shards: &str, extra: &[&str], check_graph: bool) {
    let dir = common::unique_test_dir(&format!("graph-wal-1018-{label}"));
    std::fs::create_dir_all(&dir).unwrap();
    let mut acked: Vec<(String, String)> = Vec::new();

    for cycle in 0..2 {
        let (child, port) = start_moon(&dir, shards, extra);
        // Reaps the server even when an assertion below unwinds.
        let mut server = common::ServerGuard::new(child);
        let mut c = common::Conn::open(port);

        // Everything acknowledged before this boot must have survived it.
        let dbsize = c.send(&["DBSIZE"]);
        assert_eq!(
            dbsize.trim(),
            format!(":{}", acked.len()),
            "[{label}] cycle {cycle}: DBSIZE after restart. Acknowledged KV writes were \
             lost. Server log: {:?}",
            dir.join("server.err")
        );
        for (k, v) in &acked {
            assert_eq!(
                bulk(&c.send(&["GET", k])).as_deref(),
                Some(v.as_str()),
                "[{label}] cycle {cycle}: acknowledged {k} did not survive the restart"
            );
        }
        if check_graph && cycle > 0 {
            let list = c.send(&["GRAPH.LIST"]);
            assert!(
                list.contains(&format!("g{}", cycle - 1)),
                "[{label}] cycle {cycle}: the graph written before the kill was not \
                 recovered, so the WAL never held its record and this cycle is vacuous: {list:?}"
            );
        }

        for i in 0..8 {
            let (k, v) = (format!("k:{cycle}:{i}"), format!("v:{cycle}:{i}"));
            assert_eq!(c.send(&["SET", &k, &v]).trim(), "+OK", "[{label}] SET {k}");
            acked.push((k, v));
        }
        let before = wal_record_bytes(&dir);
        let g = format!("g{cycle}");
        assert_eq!(
            c.send(&["GRAPH.CREATE", &g]).trim(),
            "+OK",
            "[{label}] GRAPH.CREATE"
        );
        let node = c.send(&["GRAPH.ADDNODE", &g, "Person", "name", "alice"]);
        assert!(node.starts_with(':'), "[{label}] GRAPH.ADDNODE -> {node:?}");
        await_wal_growth(&dir, before);

        drop(c);
        server.kill_now();
        common::wait_for_port_down(port);
    }

    // Final boot: both cycles' acknowledged writes are still there.
    let (child, port) = start_moon(&dir, shards, extra);
    let mut server = common::ServerGuard::new(child);
    let mut c = common::Conn::open(port);
    let dbsize = c.send(&["DBSIZE"]);
    let ok = dbsize.trim() == format!(":{}", acked.len());
    drop(c);
    server.kill_now();
    assert!(
        ok,
        "[{label}] final boot: DBSIZE {dbsize:?}, want {}. Log: {:?}",
        acked.len(),
        dir.join("server.err")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn graph_write_does_not_discard_the_aof_single_shard() {
    run_leg("s1", "1", &[], true);
}

#[test]
fn graph_write_does_not_discard_the_aof_single_shard_wal_kv_log_on() {
    run_leg("s1-walkv", "1", &["--wal-kv-log", "on"], true);
}

#[test]
fn graph_write_does_not_discard_the_aof_four_shards() {
    run_leg("s4", "4", &[], false);
}
