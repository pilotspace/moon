//! moon#769: under `appendfsync everysec`, a stalled AOF writer must not turn
//! routed (cross-shard) writes into "applied in memory but not queued for
//! persistence".
//!
//! At `--shards > 1` a pipelined `SET` for a key another shard owns is applied
//! by that shard's SPSC drain. The drain used to apply the write first and only
//! then block for its AOF record, for 5 ms (`AOF_SPSC_BACKPRESSURE_BOUND`).
//! Any writer stall longer than it takes to fill the 10k channel, such as a
//! slow proactive fsync, then answered `-MOONERR AOF backpressure: write applied
//! in memory but not queued for persistence` for a write that was in memory
//! and would never reach the AOF. The generic local leg waits up to
//! `--aof-fsync-timeout-ms` (2 s) for the same channel.
//!
//! The drain now admits each routed leg against its writer BEFORE applying it.
//! It waits up to the same `--aof-fsync-timeout-ms` bound, and when the writer
//! still has no room it refuses the leg unapplied
//! (`MOONERR AOF backpressure: command not executed, ...`).
//!
//! `MOON_TEST_AOF_FSYNC_STALL_MS` holds the writer's everysec fsync (the
//! moon#769 mechanism, deterministic on any host; see
//! `persistence/aof/writer_task.rs`).
//!
//! ```text
//! MOON_BIN=./target/release-fast/moon \
//!   cargo test --test aof_everysec_backpressure_769 -- --ignored --nocapture
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const LOST_ERR: &str =
    "MOONERR AOF backpressure: write applied in memory but not queued for persistence";
const REFUSED_ERR_PREFIX: &str = "MOONERR AOF backpressure: command not executed";
/// The generic local leg's reply once `--aof-fsync-timeout-ms` elapses.
const GENERIC_TIMEOUT_ERR: &str = "ERR AOF fsync failed; write not durable";

const SHARDS: &str = "4";
const BURST: usize = 10_000;

fn start_moon(port: u16, dir: &std::path::Path, extra: &[&str], stall_ms: &str) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            SHARDS,
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--auto-aof-rewrite-percentage",
            "0",
            "--disk-free-min-pct",
            "0",
        ])
        .args(extra)
        .arg("--dir")
        .arg(dir)
        .env("MOON_TEST_AOF_FSYNC_STALL_MS", stall_ms)
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (build first; MOON_BIN to override)")
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_nodelay(true).expect("nodelay");
    s.set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    s
}

/// One pipelined write of `n` `SET <prefix>:<i> v`, then exactly `n` status
/// replies. Returns `(ok, errors as (index, text))`.
fn pipelined_sets(port: u16, prefix: &str, n: usize) -> (usize, Vec<(usize, String)>) {
    let mut s = connect(port);
    let mut wire = Vec::with_capacity(n * 40);
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        wire.extend_from_slice(
            format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len()).as_bytes(),
        );
    }
    s.write_all(&wire).expect("write burst");
    let mut raw = Vec::new();
    let mut chunk = [0u8; 65536];
    let (mut ok, mut errors, mut seen, mut cursor) = (0usize, Vec::new(), 0usize, 0usize);
    while seen < n {
        let got = s.read(&mut chunk).expect("read replies");
        assert!(got > 0, "server closed after {seen}/{n} replies");
        raw.extend_from_slice(&chunk[..got]);
        while seen < n {
            let Some(rel) = raw[cursor..].windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            let line = &raw[cursor..cursor + rel];
            match line.first() {
                Some(b'+') => ok += 1,
                Some(b'-') => errors.push((seen, String::from_utf8_lossy(&line[1..]).into_owned())),
                other => panic!("reply {seen} is not a status line: {other:?}"),
            }
            seen += 1;
            cursor += rel + 2;
        }
    }
    (ok, errors)
}

fn info_field_opt(port: u16, field: &str) -> Option<u64> {
    let mut c = common::Conn::open(port);
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
}

fn info_field(port: u16, field: &str) -> u64 {
    info_field_opt(port, field)
        .unwrap_or_else(|| panic!("INFO persistence has no numeric `{field}`"))
}

/// Parallel client connections: three per shard. A connection whose OWN
/// shard's writer is stalled waits on its local legs and stops feeding the
/// other shards. With only a few connections they can all land on that one
/// shard, and then no routed leg ever meets a stalled writer.
const CONNS: usize = 12;

/// Run pipelined SET bursts from [`CONNS`] connections for `for_how_long`,
/// returning `(sent, ok, every error reply as (key, text))`. `tag` keeps
/// keys unique across calls.
fn bursts(port: u16, tag: usize, for_how_long: Duration) -> (usize, usize, Vec<(String, String)>) {
    let workers: Vec<_> = (0..CONNS)
        .map(|c| {
            std::thread::spawn(move || {
                let started = Instant::now();
                let (mut sent, mut ok, mut errors) = (0usize, 0usize, Vec::new());
                let mut round = 0usize;
                while started.elapsed() < for_how_long {
                    let prefix = format!("t{tag}c{c}b{round}");
                    let (o, errs) = pipelined_sets(port, &prefix, BURST);
                    sent += BURST;
                    ok += o;
                    errors.extend(errs.into_iter().map(|(i, t)| (format!("{prefix}:{i}"), t)));
                    round += 1;
                }
                (sent, ok, errors)
            })
        })
        .collect();
    let (mut sent, mut ok, mut errors) = (0usize, 0usize, Vec::new());
    for w in workers {
        let (s, o, e) = w.join().expect("burst worker");
        sent += s;
        ok += o;
        errors.extend(e);
    }
    (sent, ok, errors)
}

/// A writer stall shorter than the wait bound (600 ms stall, 2 s default
/// bound) is absorbed: every routed SET completes.
#[test]
#[ignore]
fn cross_shard_set_bursts_complete_across_an_everysec_writer_stall() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_server, port) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), &[], "600"));
    // Keep bursting until the stall has provably reached a routed leg (at
    // least 3.5 s, at most 12 s): whether a given burst meets a stalled
    // writer with a full channel is timing, the property is not.
    let started = Instant::now();
    let (mut sent, mut ok, mut errors) = (0usize, 0usize, Vec::new());
    let mut stalls = 0;
    let mut tag = 0usize;
    while started.elapsed() < Duration::from_secs(12) {
        let (s, o, e) = bursts(port, tag, Duration::from_millis(500));
        tag += 1;
        sent += s;
        ok += o;
        errors.extend(e);
        // Absent on a server without routed-leg admission: counts as 0.
        stalls = info_field_opt(port, "aof_backpressure_stalls").unwrap_or(0);
        if (stalls >= 1 || !errors.is_empty()) && started.elapsed() >= Duration::from_millis(3500) {
            break;
        }
    }
    assert!(
        errors.is_empty() && ok == sent,
        "a writer stall shorter than --aof-fsync-timeout-ms refused {} of {sent} SETs; \
         first 3: {:?}",
        errors.len(),
        errors.iter().take(3).collect::<Vec<_>>()
    );
    // Vacuity guard: the stall must actually have reached the routed legs.
    assert!(
        stalls >= 1,
        "no routed leg ever waited for the writer in 12 s: the stall did not bite and this \
         proved nothing"
    );
    assert_eq!(info_field(port, "aof_backpressure_dropped"), 0);
    // No rewrite ran, so admission can only have been granted by real room,
    // never by an armed rewrite overflow.
    assert_eq!(info_field(port, "aof_rewrite_overflow_spilled"), 0);
}

/// A writer stall longer than the wait bound (1500 ms stall, 10 ms bound):
/// routed SETs are refused, and a refused SET did not run — the key is
/// absent. Never "applied in memory but not queued for persistence".
#[test]
#[ignore]
fn a_routed_write_the_writer_cannot_take_is_refused_and_not_applied() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_server, port) = common::spawn_listening_guarded(|port| {
        start_moon(port, dir.path(), &["--aof-fsync-timeout-ms", "10"], "1500")
    });
    // Burst until a routed SET has been refused (at least 3.5 s, at most
    // 12 s): which writer stalls while which connection is loading it is
    // timing, the property is not.
    let started = Instant::now();
    let (mut sent, mut ok, mut errors) = (0usize, 0usize, Vec::new());
    let mut tag = 0usize;
    while started.elapsed() < Duration::from_secs(12) {
        let (s, o, e) = bursts(port, tag, Duration::from_millis(500));
        tag += 1;
        sent += s;
        ok += o;
        errors.extend(e);
        let routed_refusal_or_loss = errors
            .iter()
            .any(|(_, t)| t.starts_with(REFUSED_ERR_PREFIX) || t == LOST_ERR);
        if routed_refusal_or_loss && started.elapsed() >= Duration::from_millis(3500) {
            break;
        }
    }
    let lost: Vec<_> = errors.iter().filter(|(_, t)| t == LOST_ERR).collect();
    assert!(
        lost.is_empty(),
        "{} of {sent} SETs were applied and then reported lost; first: {:?}",
        lost.len(),
        lost.first()
    );
    let refused: Vec<&String> = errors
        .iter()
        .filter(|(_, t)| t.starts_with(REFUSED_ERR_PREFIX))
        .map(|(k, _)| k)
        .collect();
    let unexpected: Vec<_> = errors
        .iter()
        .filter(|(_, t)| !t.starts_with(REFUSED_ERR_PREFIX) && t != GENERIC_TIMEOUT_ERR)
        .collect();
    assert!(
        unexpected.is_empty(),
        "unexpected error replies: {:?}",
        unexpected.iter().take(3).collect::<Vec<_>>()
    );
    assert!(
        !refused.is_empty(),
        "vacuity guard: a 1500 ms stall against a 10 ms bound refused no routed SET \
         ({ok} of {sent} +OK, {} local-leg timeouts, stalls={:?}) — the stall did not bite",
        errors.len(),
        info_field_opt(port, "aof_backpressure_stalls")
    );
    assert_eq!(
        info_field(port, "aof_backpressure_refused"),
        refused.len() as u64,
        "INFO counts exactly the refusals clients saw"
    );
    // A refused SET never ran. Checked on a sample: the stall does not
    // delay reads.
    let mut c = common::Conn::open(port);
    for key in refused.iter().step_by((refused.len() / 200).max(1)) {
        assert_eq!(
            c.send(&["GET", key]),
            "$-1\r\n",
            "{key} was refused as not executed, but it was written"
        );
    }
}
