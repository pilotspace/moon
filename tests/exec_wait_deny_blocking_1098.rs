//! moon#1098: a `WAIT` queued inside `MULTI` must not block `EXEC`.
//!
//! Redis runs a transaction body with `CLIENT_DENY_BLOCKING` set, and
//! `waitCommand` answers `replicationCountAcksByOffset(...)` at once for such a
//! client instead of blocking. Measured against redis-server 8.6.1 over a raw
//! socket, no replica attached:
//!
//! ```text
//! MULTI / INCR k / WAIT 1 1500 / EXEC   redis: *2 :1 :0   after 0 ms
//!                                        moon : *2 :1 :0   after >= 1500 ms
//! MULTI / INCR k / WAIT 1 0 / EXEC      redis: *2 :n :0   after 0 ms
//!                                        moon : never answered (WAIT n 0 = forever)
//! ```
//!
//! Moon filled the queued `WAIT` slot with the live intercept, which polls the
//! replicas' acks until its deadline. The reply bytes were already right; the
//! time was not, so every case below asserts both.
//!
//! Each shape runs at `--shards 1` and at `--shards 4`. At `--shards 4` it runs
//! once per shard with a key owned by that shard over ONE connection, so one
//! iteration takes the connection's local `EXEC` path and the others take the
//! owner-routed path; both fill the intercept slots on the connection.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

/// A `WAIT` timeout long enough that a parked `EXEC` cannot be mistaken for a
/// prompt one on a loaded host.
const WAIT_MS: u64 = 1_500;

/// How long a transaction holding a `WAIT` may take. Redis answers in well
/// under a millisecond; this only has to separate "answered" from "parked for
/// [`WAIT_MS`]" with room for a loaded CI host.
const PROMPT: Duration = Duration::from_millis(WAIT_MS / 2);

fn spawn(dir: &Path, shards: usize) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// One key per shard, index = owning shard.
fn keys_covering(shards: usize) -> Vec<String> {
    let mut keys: Vec<Option<String>> = vec![None; shards];
    let mut i = 0u32;
    while keys.iter().any(Option::is_none) {
        let k = format!("k1098:{i}");
        let s = key_to_shard(k.as_bytes(), shards);
        if keys[s].is_none() {
            keys[s] = Some(k);
        }
        i += 1;
        assert!(i < 100_000, "could not find a key for every shard");
    }
    keys.into_iter().flatten().collect()
}

/// Send `cmds` as one write and read `cmds.len()` replies; return them with
/// the wall time to the last one.
fn timed(c: &mut common::Conn, cmds: &[&[&str]]) -> (String, Duration) {
    let mut batch = Vec::new();
    for parts in cmds {
        batch.extend_from_slice(&common::encode(parts));
    }
    let t0 = Instant::now();
    c.sock.write_all(&batch).expect("write batch");
    let replies = c.read_replies(cmds.len());
    (replies, t0.elapsed())
}

fn scenario(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1098-s{shards}"));
    let (mut server, port) = spawn(&dir, shards);
    let mut a = common::Conn::open(port);

    let mut slow = Vec::new();
    let mut check = |label: String, cmds: &[&[&str]], want: &str, a: &mut common::Conn| {
        let (got, took) = timed(a, cmds);
        assert_eq!(got, want, "--shards {shards}: {label}: reply bytes");
        if took >= PROMPT {
            slow.push(format!("{label}: EXEC answered after {took:?}"));
        }
    };

    let keys = keys_covering(shards);
    for key in &keys {
        assert_eq!(a.send(&["SET", key, "0"]), "+OK\r\n");
        for (n, (replicas, timeout)) in [("1", "1500"), ("0", "0")].iter().enumerate() {
            check(
                format!("{key}: MULTI / INCR / WAIT {replicas} {timeout} / EXEC"),
                &[
                    &["MULTI"],
                    &["INCR", key],
                    &["WAIT", replicas, timeout],
                    &["EXEC"],
                ],
                &format!("+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:{}\r\n:0\r\n", n + 1),
                &mut a,
            );
        }
    }
    // No write in the body, and two WAITs in one body: neither may park.
    check(
        "MULTI / WAIT 1 1500 / EXEC".into(),
        &[&["MULTI"], &["WAIT", "1", "1500"], &["EXEC"]],
        "+OK\r\n+QUEUED\r\n*1\r\n:0\r\n",
        &mut a,
    );
    check(
        "MULTI / WAIT 1 1500 / WAIT 1 1500 / EXEC".into(),
        &[
            &["MULTI"],
            &["WAIT", "1", "1500"],
            &["WAIT", "1", "1500"],
            &["EXEC"],
        ],
        "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:0\r\n:0\r\n",
        &mut a,
    );
    assert!(
        slow.is_empty(),
        "--shards {shards}: a WAIT queued inside MULTI parked EXEC; redis answers \
         it at once with the current ack count (moon#1098):\n{slow:#?}"
    );

    // `WAIT n 0` means "forever" to a live client. Inside EXEC it must still
    // answer at once. Last, because the unfixed server never answers it: the
    // read gives up after its own deadline and panics.
    for (n, key) in keys.iter().enumerate() {
        let (got, took) = timed(
            &mut a,
            &[&["MULTI"], &["INCR", key], &["WAIT", "1", "0"], &["EXEC"]],
        );
        assert_eq!(
            got, "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:3\r\n:0\r\n",
            "--shards {shards}: {key} (#{n}): MULTI / INCR / WAIT 1 0 / EXEC"
        );
        assert!(
            took < PROMPT,
            "--shards {shards}: {key}: MULTI / INCR / WAIT 1 0 / EXEC took {took:?}"
        );
    }

    // Negative control: the LIVE WAIT still blocks for its timeout. The fix
    // is scoped to the transaction body, like redis's CLIENT_DENY_BLOCKING.
    let (got, took) = timed(&mut a, &[&["WAIT", "1", "300"]]);
    assert_eq!(got, ":0\r\n", "--shards {shards}: live WAIT 1 300");
    assert!(
        took >= Duration::from_millis(250),
        "--shards {shards}: a live WAIT 1 300 with no replica answered after \
         {took:?}; it must still wait out its timeout"
    );

    drop(a);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wait_queued_in_multi_answers_at_once_1_shard() {
    scenario(1);
}

#[test]
fn wait_queued_in_multi_answers_at_once_4_shards() {
    scenario(4);
}

/// With a replica attached, the queued `WAIT` answers the current ack count
/// at once — it neither parks nor invents acks.
///
/// monoio only: it is the runtime with master-side fan-out. `#[ignore]`d like
/// every replication suite (two live servers):
///
/// ```text
/// MOON_BIN=... cargo test --test exec_wait_deny_blocking_1098 -- --ignored
/// ```
#[cfg(feature = "runtime-monoio")]
#[test]
#[ignore]
fn wait_queued_in_multi_reports_replica_acks_at_once() {
    let mdir = common::unique_test_dir("moon-1098-master");
    let rdir = common::unique_test_dir("moon-1098-replica");
    let (mut master, mport) = spawn(&mdir, 1);
    let (mut replica, rport) = spawn(&rdir, 1);

    let mut r = common::Conn::open(rport);
    assert!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()])
            .starts_with("+OK"),
        "REPLICAOF refused"
    );
    let up_deadline = Instant::now() + Duration::from_secs(20);
    while !r
        .send(&["INFO", "replication"])
        .contains("master_link_status:up")
    {
        assert!(Instant::now() < up_deadline, "replica link never came up");
        std::thread::sleep(Duration::from_millis(100));
    }

    let mut a = common::Conn::open(mport);
    assert_eq!(a.send(&["SET", "k1098r", "0"]), "+OK\r\n");
    // The LIVE WAIT blocks until the replica has acked everything so far,
    // so the transaction below starts with the replica caught up.
    assert_eq!(a.send(&["WAIT", "1", "10000"]), ":1\r\n", "live WAIT 1");

    // Nothing written since the ack: the one replica counts, at once.
    let (got, took) = timed(&mut a, &[&["MULTI"], &["WAIT", "1", "1500"], &["EXEC"]]);
    assert_eq!(
        got, "+OK\r\n+QUEUED\r\n*1\r\n:1\r\n",
        "MULTI / WAIT 1 / EXEC"
    );
    assert!(took < PROMPT, "MULTI / WAIT 1 / EXEC parked for {took:?}");

    // Asking for more replicas than exist must not park either, and the count
    // can never exceed the one replica attached.
    let (got, took) = timed(
        &mut a,
        &[
            &["MULTI"],
            &["INCR", "k1098r"],
            &["WAIT", "2", "1500"],
            &["EXEC"],
        ],
    );
    assert!(
        got == "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n:0\r\n"
            || got == "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n:1\r\n",
        "MULTI / INCR / WAIT 2 / EXEC: {got:?}"
    );
    assert!(
        took < PROMPT,
        "MULTI / INCR / WAIT 2 / EXEC parked for {took:?}"
    );

    drop(a);
    drop(r);
    replica.kill_now();
    master.kill_now();
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}
