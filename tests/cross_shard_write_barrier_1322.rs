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
