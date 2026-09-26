//! moon#1271 against a real server: a per-shard AOF writer that starts LATE
//! must not delete the files of a rewrite the other shards are running.
//!
//! Every per-shard writer loads the AOF manifest when it starts. That load
//! used to end with the orphan sweep, which deletes every `moon.aof.*` file
//! whose sequence is not the committed one in EVERY shard's directory. Under
//! load, one writer finished starting after a `BGREWRITEAOF` had been
//! dispatched, and its load deleted the other shards' `seq + 1` files. The
//! observed failure (`/home/user/wt/handoff/issue1271-server.err`) was the
//! milder interleaving: the temp bases vanished between write and rename,
//! and the rewrite aborted. When the late load lands after the others have
//! published their base and incr, it deletes those instead, and the rewrite
//! then COMMITS a manifest that names files that are gone.
//!
//! `MOON_TEST_AOF_WRITER_HOLD=1:<file>` holds shard 1's writer before its
//! startup load for as long as `<file>` exists, so the test forces that
//! second interleaving instead of racing boot: shards 0, 2 and 3 publish
//! their `seq 2` generation, then shard 1's writer is released, loads, and
//! folds. The rewrite must commit with every shard's `seq 2` base and incr on
//! disk, and a SIGKILL + restart must restore every key, including keys
//! written after the rewrite.
//!
//! `--shards 4` (the per-shard writers exist at `--shards` >= 2 on both
//! runtimes). Pin the binary: `MOON_BIN=<moon> cargo test --test
//! perf_ws21_aof_writer_start`.

#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

const SHARDS: usize = 4;
const HELD_SHARD: usize = 1;

fn spawn(dir: &Path, hold: Option<&Path>) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let mut cmd = std::process::Command::new(&bin);
        cmd.args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &SHARDS.to_string(),
            "--appendonly",
            "yes",
            // A write for the held shard's writer queues in its channel
            // instead of waiting for an fsync that cannot happen yet.
            "--appendfsync",
            "everysec",
            // The only rewrite is the test's own.
            "--auto-aof-rewrite-percentage",
            "0",
            "--save",
            "",
            "--disk-offload",
            "disable",
            "--maxmemory",
            "0",
            "--disk-free-min-pct",
            "0",
        ])
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .stdout(common::server_stderr(dir))
        .stderr(common::server_stderr(dir));
        if let Some(hold) = hold {
            cmd.env(
                "MOON_TEST_AOF_WRITER_HOLD",
                format!("{HELD_SHARD}:{}", hold.display()),
            );
        }
        cmd.spawn()
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

fn wait_for(what: &str, budget: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + budget;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn shard_file(dir: &Path, shard: usize, name: &str) -> PathBuf {
    dir.join("appendonlydir")
        .join(format!("shard-{shard}"))
        .join(name)
}

fn manifest_seq(dir: &Path) -> u64 {
    let text = std::fs::read_to_string(dir.join("appendonlydir").join("moon.aof.manifest"))
        .expect("read the AOF manifest");
    text.lines()
        .find_map(|l| l.strip_prefix("seq "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("manifest has no seq line: {text:?}"))
}

fn set_keys(c: &mut Conn, prefix: &str, n: usize) {
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        let val = format!("{prefix}-value-{i}");
        let reply = c.send(&["SET", &key, &val]);
        assert_eq!(reply, "+OK\r\n", "SET {key}");
    }
}

fn assert_keys(c: &mut Conn, prefix: &str, n: usize) {
    let mut missing = Vec::new();
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        let val = format!("{prefix}-value-{i}");
        let reply = c.send(&["GET", &key]);
        if reply != format!("${}\r\n{val}\r\n", val.len()) {
            missing.push((key, reply));
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {n} `{prefix}` keys wrong or missing after the restart, e.g. {:?}",
        missing.len(),
        &missing[..missing.len().min(3)]
    );
}

#[test]
fn a_late_writer_start_leaves_a_per_shard_rewrite_committed_and_complete() {
    let dir = common::unique_test_dir("ws21-1271");
    std::fs::create_dir_all(&dir).unwrap();
    let hold = dir.join("hold-writer-1");
    std::fs::write(&hold, b"held").unwrap();

    let (mut server, port) = spawn(&dir, Some(&hold));
    let mut c = Conn::open(port);
    // Main's recovery creates the seq 1 manifest; every writer but shard 1's
    // then starts.
    wait_for("the seq 1 manifest", Duration::from_secs(30), || {
        dir.join("appendonlydir/moon.aof.manifest").exists()
    });
    assert_eq!(manifest_seq(&dir), 1);

    // Keys on every shard; shard 1's records wait in its writer's channel.
    set_keys(&mut c, "before", 200);

    let reply = c.send(&["BGREWRITEAOF"]);
    assert!(
        reply.starts_with("+Background append only file rewriting started"),
        "BGREWRITEAOF: {reply}"
    );

    // Shards 0, 2 and 3 publish their seq 2 base and incr (renamed into
    // place, ahead of the manifest commit, which waits for shard 1).
    let others: Vec<usize> = (0..SHARDS).filter(|&s| s != HELD_SHARD).collect();
    wait_for(
        "shards 0, 2 and 3 to publish their seq 2 generation",
        Duration::from_secs(60),
        || {
            others.iter().all(|&s| {
                shard_file(&dir, s, "moon.aof.2.base.rdb").exists()
                    && shard_file(&dir, s, "moon.aof.2.incr.aof").exists()
            })
        },
    );
    assert_eq!(
        manifest_seq(&dir),
        1,
        "the rewrite committed without shard 1"
    );

    // Release shard 1's writer: it loads the manifest (still seq 1), drains
    // its queued records and folds.
    std::fs::remove_file(&hold).unwrap();
    wait_for("the rewrite to finish", Duration::from_secs(60), || {
        info_field(&mut c, "aof_rewrite_in_progress") == "0"
    });
    assert_eq!(
        info_field(&mut c, "aof_last_bgrewrite_status"),
        "ok",
        "the rewrite failed (server log: {})",
        dir.join("server.err").display()
    );
    assert_eq!(manifest_seq(&dir), 2, "the rewrite did not commit seq 2");
    for s in 0..SHARDS {
        for name in ["moon.aof.2.base.rdb", "moon.aof.2.incr.aof"] {
            assert!(
                shard_file(&dir, s, name).exists(),
                "the committed seq 2 manifest names shard-{s}/{name}, which is gone \
                 (server log: {})",
                dir.join("server.err").display()
            );
        }
    }

    // Keys written after the commit land in the seq 2 incr files.
    set_keys(&mut c, "after", 200);
    // everysec: the writers flush their tail within a second.
    std::thread::sleep(Duration::from_millis(1500));
    drop(c);
    server.kill_now();
    common::wait_for_port_down(port);

    let (_server, port) = spawn(&dir, None);
    let mut c = Conn::open(port);
    assert_keys(&mut c, "before", 200);
    assert_keys(&mut c, "after", 200);
    assert_eq!(c.send(&["DBSIZE"]), ":400\r\n");
    drop(c);
    drop(_server);
    let _ = std::fs::remove_dir_all(&dir);
}
