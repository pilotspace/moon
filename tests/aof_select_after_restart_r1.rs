//! R1 review, finding 2: after a restart, a write to db 0 replayed into the db
//! the previous run's AOF ended in.
//!
//! A restarted AOF writer reopens the incr (or the flat `appendonly.aof`) the
//! previous run appended to. That file ends wherever the previous run's last
//! `SELECT` left it, but the writer assumed the stream was at db 0, so its
//! first db-0 record went out with no `SELECT 0` and a replay applied it to
//! the old db. redis starts every AOF writer with `aof_selected_db = -1`; the
//! writer now starts its record context with an unknown db
//! (`RecordCtx::appending`), so its first record always carries a `SELECT`.
//!
//! The probe: `SELECT 3` + writes, restart, writes to db 0, restart, and every
//! key must be in the db it was written to — `--shards` 1 and 4, after a
//! graceful `SHUTDOWN` and after a kill -9. Run it against a binary of each
//! runtime: monoio `--shards 1` is the multi-part top-level incr, tokio
//! `--shards 1` the flat `appendonly.aof`, `--shards 4` the per-shard framed
//! incr files.
//!
//! `MOON_BIN=<moon> cargo test --test aof_select_after_restart_r1 -- --include-ignored`
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard};

/// Keys per db: enough that every shard of `--shards 4` holds some.
const KEYS: usize = 32;

fn spawn(dir: &Path, shards: usize) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let d = dir.to_path_buf();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

/// Stop the server: a graceful `SHUTDOWN` (the writer's final sync), or a
/// kill -9 once the records have had time to reach the kernel.
fn stop(server: &mut ServerGuard, port: u16, graceful: bool) {
    if graceful {
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.write_all(&common::encode(&["SHUTDOWN"]))
            .expect("SHUTDOWN");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if server.as_mut().try_wait().expect("try_wait").is_some() {
                break;
            }
            assert!(Instant::now() < deadline, "SHUTDOWN did not exit in 30 s");
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = server.take();
    } else {
        // everysec: a record reaches the kernel within the writer's pickup
        // latency (well under a millisecond); a kill -9 keeps the page cache.
        std::thread::sleep(Duration::from_millis(300));
        server.kill_now();
    }
    common::wait_for_port_down(port);
}

fn write_keys(c: &mut Conn, db: usize, prefix: &str) {
    assert_eq!(c.send(&["SELECT", &db.to_string()]), "+OK\r\n");
    for i in 0..KEYS {
        assert_eq!(
            c.send(&["SET", &format!("{prefix}{i}"), &format!("{db}")]),
            "+OK\r\n"
        );
    }
}

/// Every key of `prefix` whose value in `db` is not `want` (None = absent).
fn misplaced(c: &mut Conn, db: usize, prefix: &str, want: Option<&str>) -> Vec<String> {
    assert_eq!(c.send(&["SELECT", &db.to_string()]), "+OK\r\n");
    (0..KEYS)
        .filter_map(|i| {
            let k = format!("{prefix}{i}");
            let got = c.send(&["GET", &k]);
            let ok = match want {
                Some(v) => got == format!("${}\r\n{v}\r\n", v.len()),
                None => got == "$-1\r\n",
            };
            (!ok).then(|| format!("db{db} {k} = {got:?}"))
        })
        .collect()
}

fn probe(shards: usize, graceful: bool) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    // Run 1: the log ends in db 3.
    let (mut server, port) = spawn(dir, shards);
    let mut c = Conn::open(port);
    write_keys(&mut c, 3, "a");
    drop(c);
    stop(&mut server, port, graceful);

    // Run 2: a fresh connection writes to db 0 (no SELECT: the default db).
    let (mut server, port) = spawn(dir, shards);
    let mut c = Conn::open(port);
    for i in 0..KEYS {
        assert_eq!(c.send(&["SET", &format!("b{i}"), "0"]), "+OK\r\n");
    }
    drop(c);
    stop(&mut server, port, graceful);

    // Run 3: every key must be where it was written.
    let (_server, port) = spawn(dir, shards);
    let mut c = Conn::open(port);
    let mut wrong = misplaced(&mut c, 0, "b", Some("0"));
    wrong.extend(misplaced(&mut c, 3, "b", None));
    wrong.extend(misplaced(&mut c, 3, "a", Some("3")));
    wrong.extend(misplaced(&mut c, 0, "a", None));
    assert!(
        wrong.is_empty(),
        "--shards {shards}, {} restart, {}: {} keys replayed into the wrong db \
         (the restarted writer's first db-0 record carried no SELECT 0): {wrong:#?}",
        if graceful { "graceful" } else { "kill -9" },
        common::find_moon_binary().display(),
        wrong.len(),
    );
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn db0_writes_after_a_graceful_restart_stay_in_db0_s1() {
    probe(1, true);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn db0_writes_after_a_graceful_restart_stay_in_db0_s4() {
    probe(4, true);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn db0_writes_after_a_kill9_restart_stay_in_db0_s1() {
    probe(1, false);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn db0_writes_after_a_kill9_restart_stay_in_db0_s4() {
    probe(4, false);
}
