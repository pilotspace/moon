//! moon#1096 — on a replica, a write that arrives through replication wakes
//! the clients blocked on the keys it wrote, as a write on the master does.
//!
//! Every write a replica holds arrives through `replication::apply`, and
//! nothing there reached the ready-key hook, so an `XREAD BLOCK` parked on a
//! replica answered nil at its own timeout while its stream filled up under
//! it. redis-server 8.6.1 (master + replica, the reader parks, the master
//! writes 0.4 s later):
//!
//! ```text
//! XADD                      -> 0.41 s  {t}s 1-1 f v
//! MULTI; XADD; EXEC         -> 0.43 s  {t}e 1-1 f v
//! XADD in db 1; MOVE k 0    -> 0.44 s  {t}m 1-1 f v
//! XADD in db 1; SWAPDB 0 1  -> 0.44 s  {t}w 1-1 f v
//! BLPOP / XREADGROUP        -> -READONLY You can't write against a read only replica.
//! ```
//!
//! Blocking pops (and `XREADGROUP`) are writes, so a read-only replica
//! refuses them in both servers: `XREAD` is what can be parked there. The
//! hook the replica now runs is the master's own, so nothing is
//! stream-specific.
//!
//! `#[ignore]`d like every replication suite (master-side replication exists
//! on the monoio runtime only):
//!
//! ```text
//! MOON_BIN=... cargo test --test replica_blocking_wake_1096 -- --include-ignored
//! ```

mod common;

use std::process::Child;
use std::time::{Duration, Instant};

use common::{Conn, spawn_listening_guarded, unique_test_dir};

fn spawn(port: u16, dir: &std::path::Path, shards: usize) -> Child {
    std::process::Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

fn blocked_clients(port: u16) -> u32 {
    let mut c = Conn::open(port);
    c.send(&["INFO", "clients"])
        .lines()
        .find_map(|l| l.trim().strip_prefix("blocked_clients:")?.parse().ok())
        .unwrap_or(0)
}

fn cmd(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| (*s).to_string()).collect()
}

/// Park `read` on the replica, run `writes` on the master (one pipelined
/// batch) once it has parked, and return the reply and how long after the
/// writes it came.
fn park_then(
    rport: u16,
    mport: u16,
    read: Vec<String>,
    writes: &[Vec<String>],
) -> (String, Duration) {
    let reader = std::thread::spawn(move || {
        let mut w = Conn::open(rport);
        let refs: Vec<&str> = read.iter().map(String::as_str).collect();
        let reply = w.send(&refs);
        (reply, Instant::now())
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while blocked_clients(rport) < 1 {
        assert!(Instant::now() < deadline, "the replica reader never parked");
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut m = Conn::open(mport);
    let batch: Vec<Vec<&str>> = writes
        .iter()
        .map(|w| w.iter().map(String::as_str).collect())
        .collect();
    let batch_refs: Vec<&[&str]> = batch.iter().map(Vec::as_slice).collect();
    let sent = Instant::now();
    let _ = m.pipeline(&batch_refs);
    let (reply, at) = reader.join().expect("reader thread");
    (reply, at.saturating_duration_since(sent))
}

#[test]
#[ignore = "replication: monoio master, run with --include-ignored"]
fn a_replicated_write_wakes_a_reader_parked_on_the_replica() {
    for shards in [1, 4] {
        let mdir = unique_test_dir("replica_wake_1096_master");
        let rdir = unique_test_dir("replica_wake_1096_replica");
        let (_m, mport) = spawn_listening_guarded(|p| spawn(p, &mdir, shards));
        let (_r, rport) = spawn_listening_guarded(|p| spawn(p, &rdir, 1));
        let mut r = Conn::open(rport);
        assert!(
            r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()])
                .starts_with("+OK")
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while !r
            .send(&["INFO", "replication"])
            .contains("master_link_status:up")
        {
            assert!(Instant::now() < deadline, "replica link never came up");
            std::thread::sleep(Duration::from_millis(100));
        }

        type Writes = fn(&str) -> Vec<Vec<String>>;
        let rows: [(&str, &str, Writes); 4] = [
            ("XADD", "{t1096}s", |k| {
                vec![cmd(&["XADD", k, "1-1", "f", "v"])]
            }),
            ("MULTI; XADD; EXEC", "{t1096}e", |k| {
                vec![
                    cmd(&["MULTI"]),
                    cmd(&["XADD", k, "1-1", "f", "v"]),
                    cmd(&["EXEC"]),
                ]
            }),
            ("XADD in db 1; MOVE k 0", "{t1096}m", |k| {
                vec![
                    cmd(&["SELECT", "1"]),
                    cmd(&["XADD", k, "1-1", "f", "v"]),
                    cmd(&["MOVE", k, "0"]),
                    cmd(&["SELECT", "0"]),
                ]
            }),
            ("XADD in db 1; SWAPDB 0 1", "{t1096}w", |k| {
                vec![
                    cmd(&["SELECT", "1"]),
                    cmd(&["XADD", k, "1-1", "f", "v"]),
                    cmd(&["SELECT", "0"]),
                    cmd(&["SWAPDB", "0", "1"]),
                ]
            }),
        ];
        let mut failures = Vec::new();
        for (label, key, writes) in rows {
            let read = cmd(&["XREAD", "BLOCK", "4000", "STREAMS", key, "$"]);
            let (reply, after) = park_then(rport, mport, read, &writes(key));
            if !reply.contains("1-1") || after > Duration::from_millis(500) {
                failures.push(format!(
                    "shards={shards} {label}: replica reader got {reply:?} {after:.0?} after the \
                     master's write; redis wakes it at once"
                ));
            }
        }
        // Parity, not a change: a blocking pop is a write on a read-only
        // replica, answered at once in both servers.
        for pop in [
            cmd(&["BLPOP", "{t1096}l", "2"]),
            cmd(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "2000",
                "STREAMS",
                "{t1096}s",
                ">",
            ]),
        ] {
            let refs: Vec<&str> = pop.iter().map(String::as_str).collect();
            let reply = r.send(&refs);
            if !reply.starts_with("-READONLY") {
                failures.push(format!("shards={shards} {}: got {reply:?}", pop[0]));
            }
        }
        assert!(
            failures.is_empty(),
            "a replica must serve its blocked readers from replicated writes:\n  {}",
            failures.join("\n  ")
        );
    }
}
