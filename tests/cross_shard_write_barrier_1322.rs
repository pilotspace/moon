//! moon#1322: a cross-shard write's REMOTE legs are durable before its reply.
//!
//! A spanning `MSET`/`DEL`/`UNLINK`/`BITOP`/`COPY` (the coordinator's
//! `MultiExecute` legs) and the `FLUSHALL` broadcast used to barrier only the
//! connection's own shard: under `appendfsync always` the reply left with no
//! fsync of the remote shard's AOF (pre-existing, both runtimes), and in
//! moon#1266 1A's held windows before the remote record's `write(2)`.
//!
//! The probe: `--shards 4`, `appendfsync always`, and the fsync of ONE remote
//! writer (`aof-writer-<G>`) held by the test gate
//! (`MOON_TEST_AOF_SYNC_GATE` + `MOON_TEST_AOF_SYNC_GATE_WRITERS=<G>`). The
//! command writes a key owned by `G` and one owned by a third shard, from a
//! connection on a fourth: its reply must NOT arrive while `G`'s fsync is held
//! (300 ms), and must arrive once it is released.
//!
//! ```text
//! MOON_BIN=/path/to/moon MOON_DISK_FREE_MIN_PCT=0 \
//!   cargo test --test cross_shard_write_barrier_1322 -- --include-ignored
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

const SHARDS: usize = 4;
/// The writer whose fsync the gate holds.
const GATED: usize = 3;
const HOLD: Duration = Duration::from_millis(300);

fn start(port: u16, dir: &std::path::Path, gate: &std::path::Path) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &SHARDS.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--auto-aof-rewrite-percentage",
            "0",
            "--disk-free-min-pct",
            "0",
            // The held fsync must not run into the 2 s barrier timeout (the
            // pipeline test holds it for seconds on a loaded box).
            "--aof-fsync-timeout-ms",
            "30000",
        ])
        .arg("--dir")
        .arg(dir)
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .env("MOON_TEST_AOF_SYNC_GATE", gate)
        .env("MOON_TEST_AOF_SYNC_GATE_WRITERS", GATED.to_string())
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (build it first, or set MOON_BIN)")
}

/// A key owned by `shard`.
fn key_on(shard: usize, prefix: &str) -> String {
    (0..10_000)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| key_to_shard(k.as_bytes(), SHARDS) == shard)
        .expect("a key for every shard")
}

/// The shard this connection runs on: a `TXN` refuses a write to another
/// shard's key, so the first tag it accepts is local.
fn own_shard(c: &mut common::Conn) -> usize {
    for i in 0..256 {
        let k = format!("{{p{i}}}:probe");
        assert!(c.send(&["TXN", "BEGIN"]).contains("OK"));
        let r = c.send(&["SET", &k, "1"]);
        assert!(c.send(&["TXN", "ABORT"]).contains("OK"));
        if r.contains("OK") {
            return key_to_shard(k.as_bytes(), SHARDS);
        }
    }
    panic!("no local hash tag found");
}

/// Send `cmd` with `G`'s fsync held; the reply must not come within
/// [`HOLD`], and must come once the gate opens. Returns a violation, if any.
fn reply_waits_for_remote_fsync(
    s: &mut std::net::TcpStream,
    gate: &std::path::Path,
    cmd: &[&str],
) -> Option<String> {
    std::fs::write(gate, b"held").expect("hold the fsync");
    let t = Instant::now();
    s.write_all(&common::encode(cmd)).expect("send");
    s.set_read_timeout(Some(HOLD)).expect("timeout");
    let mut buf = [0u8; 512];
    let early = match s.read(&mut buf) {
        Ok(n) if n > 0 => Some(String::from_utf8_lossy(&buf[..n]).into_owned()),
        _ => None,
    };
    std::fs::remove_file(gate).expect("release the fsync");
    let violation = early.map(|r| {
        format!(
            "{cmd:?}: replied {r:?} after {:?} while the remote writer's fsync was held",
            t.elapsed()
        )
    });
    if violation.is_none() {
        s.set_read_timeout(Some(Duration::from_secs(20)))
            .expect("timeout");
        let n = s.read(&mut buf).expect("the reply after the release");
        let r = String::from_utf8_lossy(&buf[..n]);
        assert!(!r.starts_with('-'), "{cmd:?}: {r}");
    }
    violation
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn cross_shard_writes_wait_for_the_remote_fsync_under_always() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let gate = dir.join("sync.gate");
    let (_server, port) = common::spawn_listening_guarded(|port| start(port, dir, &gate));
    // A connection whose own shard is neither the gated one nor the third.
    let (mut c, local) = (0..16)
        .map(|_| {
            let mut c = common::Conn::open(port);
            let l = own_shard(&mut c);
            (c, l)
        })
        .find(|(_, l)| *l != GATED)
        .expect("a connection off the gated shard");
    let third = (0..SHARDS)
        .find(|s| *s != local && *s != GATED)
        .expect("a third shard");
    let kg = key_on(GATED, "g");
    let kt = key_on(third, "t");
    // The raw stream of the SAME connection (`Conn` exposes its socket).
    let mut violations = Vec::new();
    let cases: Vec<(Vec<&str>, Vec<Vec<&str>>)> = vec![
        (vec!["MSET", &kg, "1", &kt, "2"], vec![]),
        (
            vec!["DEL", &kg, &kt],
            vec![vec!["SET", &kg, "1"], vec!["SET", &kt, "1"]],
        ),
        (
            vec!["UNLINK", &kg, &kt],
            vec![vec!["SET", &kg, "1"], vec!["SET", &kt, "1"]],
        ),
        (
            vec!["BITOP", "OR", &kg, &kt, &kt],
            vec![vec!["SET", &kt, "ab"]],
        ),
        (
            vec!["COPY", &kt, &kg, "REPLACE"],
            vec![vec!["SET", &kt, "cd"]],
        ),
        (
            vec!["FLUSHALL"],
            vec![vec!["SET", &kg, "1"], vec!["SET", &kt, "1"]],
        ),
    ];
    for (cmd, setup) in &cases {
        for s in setup {
            let r = c.send(s);
            assert!(!r.starts_with('-'), "setup {s:?}: {r}");
        }
        if let Some(v) = reply_waits_for_remote_fsync(&mut c.sock, &gate, cmd) {
            violations.push(v);
        }
    }
    assert!(
        violations.is_empty(),
        "local shard {local}, gated {GATED}, third {third}: cross-shard writes acknowledged \
         before the remote shard's fsync:\n{}",
        violations.join("\n")
    );
}

/// The barriers of a PIPELINE are coalesced — every command of
/// the batch runs, then ONE barrier set covers them all — and no reply of the
/// batch, not even one whose own write was local, leaves before the gated
/// remote fsync.
///
/// Pipeline: a local `SET`, then 16 spanning `MSET`s, each writing its own key
/// on the gated shard and one on a third. With `G`'s fsync held:
///  * no byte of the batch's replies may arrive (the local `SET`'s `+OK` is
///    behind the batch barrier too);
///  * the second `MSET` has already RUN — its key is readable from another
///    connection. Before coalescing, the first `MSET` awaited its own barrier
///    set before the next command ran, so a pipeline of `N` spanning writes
///    paid `N` serial barrier sets (P16 −70% under `always`). This read is an
///    observation of batch execution, not a durability claim: the write is
///    applied in memory, unacknowledged, exactly like a local leg awaiting
///    the batch's group-commit fsync.
///
/// After the release all 17 replies arrive, all `+OK`.
#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn a_pipeline_of_spanning_writes_pays_one_barrier_set_before_any_reply() {
    const N: usize = 16;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let gate = dir.join("sync.gate");
    let (_server, port) = common::spawn_listening_guarded(|port| start(port, dir, &gate));
    let (mut c, local) = (0..16)
        .map(|_| {
            let mut c = common::Conn::open(port);
            let l = own_shard(&mut c);
            (c, l)
        })
        .find(|(_, l)| *l != GATED)
        .expect("a connection off the gated shard");
    let third = (0..SHARDS)
        .find(|s| *s != local && *s != GATED)
        .expect("a third shard");
    let mut reader = common::Conn::open(port);
    let kl = key_on(local, "l");
    let kg: Vec<String> = (0..N).map(|i| key_on(GATED, &format!("g{i}_"))).collect();
    let kt: Vec<String> = (0..N).map(|i| key_on(third, &format!("t{i}_"))).collect();

    let mut pipeline = common::encode(&["SET", &kl, "local"]);
    for i in 0..N {
        let v = i.to_string();
        pipeline.extend(common::encode(&["MSET", &kg[i], &v, &kt[i], &v]));
    }
    std::fs::write(&gate, b"held").expect("hold the fsync");
    let t = Instant::now();
    c.sock.write_all(&pipeline).expect("send the pipeline");
    c.sock.set_read_timeout(Some(HOLD)).expect("timeout");
    let mut buf = [0u8; 512];
    let early = match c.sock.read(&mut buf) {
        Ok(n) if n > 0 => Some(String::from_utf8_lossy(&buf[..n]).into_owned()),
        _ => None,
    };
    // Still held: the batch's second MSET has run (coalesced, not serialized).
    // Polled, not read once: on a loaded box the shard may need a moment to get
    // through the batch. Serialized barriers never get there, so the poll
    // simply times out — with the fsync still held — and the assertion fires.
    let mut second = reader.send(&["GET", &kg[1]]);
    let poll_until = Instant::now() + Duration::from_secs(5);
    while !second.contains("\r\n1\r\n") && Instant::now() < poll_until {
        std::thread::sleep(Duration::from_millis(20));
        second = reader.send(&["GET", &kg[1]]);
    }
    std::fs::remove_file(&gate).expect("release the fsync");
    assert!(
        early.is_none(),
        "local shard {local}, gated {GATED}: the pipeline replied {early:?} after {:?} while \
         the remote writer's fsync was held",
        t.elapsed()
    );
    assert!(
        second.contains("\r\n1\r\n"),
        "the batch's second MSET had not run while the first one's barrier was pending \
         (GET {} -> {second:?}): the pipeline's barriers are serialized per command",
        kg[1]
    );
    c.sock
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("timeout");
    let replies = c.read_replies(N + 1);
    assert_eq!(
        replies.matches("+OK\r\n").count(),
        N + 1,
        "every reply of the batch is +OK after the release: {replies:?}"
    );
}

/// A command that flushes the batch's replies EARLY — SUBSCRIBE entry, a
/// blocking command — settles the batch's barrier debt first: with the gated
/// remote fsync held, neither the spanning `MSET`'s `+OK` nor the early
/// command's own reply may arrive; both arrive, in order, once it is released.
#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn an_early_flush_waits_for_the_batch_debt_before_replying() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let gate = dir.join("sync.gate");
    let (_server, port) = common::spawn_listening_guarded(|port| start(port, dir, &gate));
    let mut violations = Vec::new();
    for early in [
        vec!["SUBSCRIBE", "ch1322"],
        vec!["BLPOP", "no_such_list_1322", "1"],
    ] {
        let (mut c, local) = (0..16)
            .map(|_| {
                let mut c = common::Conn::open(port);
                let l = own_shard(&mut c);
                (c, l)
            })
            .find(|(_, l)| *l != GATED)
            .expect("a connection off the gated shard");
        let third = (0..SHARDS)
            .find(|s| *s != local && *s != GATED)
            .expect("a third shard");
        let kg = key_on(GATED, "eg");
        let kt = key_on(third, "et");
        let mut pipeline = common::encode(&["MSET", &kg, "1", &kt, "2"]);
        pipeline.extend(common::encode(&early));
        std::fs::write(&gate, b"held").expect("hold the fsync");
        let t = Instant::now();
        c.sock.write_all(&pipeline).expect("send");
        c.sock.set_read_timeout(Some(HOLD)).expect("timeout");
        let mut buf = [0u8; 512];
        let reply = match c.sock.read(&mut buf) {
            Ok(n) if n > 0 => Some(String::from_utf8_lossy(&buf[..n]).into_owned()),
            _ => None,
        };
        std::fs::remove_file(&gate).expect("release the fsync");
        if let Some(r) = reply {
            violations.push(format!(
                "MSET + {early:?}: replied {r:?} after {:?} while the remote writer's fsync was held",
                t.elapsed()
            ));
            continue;
        }
        c.sock
            .set_read_timeout(Some(Duration::from_millis(200)))
            .expect("timeout");
        let replies = c.read_replies(2);
        if !replies.starts_with("+OK\r\n") {
            violations.push(format!("MSET + {early:?}: replies {replies:?}"));
        }
    }
    assert!(
        violations.is_empty(),
        "an early-flush command sent a reply before the batch's barrier:\n{}",
        violations.join("\n")
    );
}
