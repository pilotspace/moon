//! moon#1084, the multi-shard half: a write whose LOCAL slice is applied and
//! then waits for other shards must log that slice before it waits.
//!
//! Two paths had the same shape as the `EXEC` intercept bug:
//!
//! * a typed `FLUSHDB`/`FLUSHALL` at `--shards > 1` cleared this shard, awaited
//!   the broadcast to every other shard, and only then appended its own record;
//! * a scattered `MSET` set its local keys, awaited the remote legs, and only
//!   then appended the synthesized local-slice `MSET`.
//!
//! A write another client made to a key of the SAME shard during that wait was
//! logged first although it was applied after, so replay put the flush (or the
//! MSET slice) on top of it: the acknowledged value came back missing (flush)
//! or overwritten (MSET) after `kill -9`.
//!
//! The wait is made deterministic by occupying one other shard's thread with
//! `DEBUG SLEEP`, run inside a transaction whose body that shard owns. Which
//! shard the test connections land on is not observable, so the race runs
//! once with each shard stalled: whenever the stalled shard is neither the
//! writer's nor the second client's, the window is open. Each round writes a
//! database of its own, so a later round cannot mask an earlier one. The
//! oracle is the live keyspace immediately before the SIGKILL — with
//! `--appendfsync always` every reply was sent after its fsync, so recovery
//! must reproduce it exactly.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

const SHARDS: usize = 4;
/// How long the stalled shard sleeps.
const STALL_MS: u64 = 1_200;
/// Pause between starting the stall and issuing the write under test, and
/// between that write and the second client's writes.
const STEP_MS: u64 = 150;

fn log_sink(dir: &Path, name: &str) -> Stdio {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(name))
    {
        Ok(f) => Stdio::from(f),
        Err(_) => Stdio::null(),
    }
}

fn spawn(dir: &Path) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    common::spawn_listening_guarded(|port| {
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
                // Crash harnesses always disable the disk-free guard: near the
                // threshold it refuses writes and the test mis-reads that as loss.
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .stdout(log_sink(dir, "server.out"))
            .stderr(log_sink(dir, "server.err"))
            .spawn()
            .expect("spawn moon")
    })
}

fn server_log(dir: &Path) -> String {
    let mut s = String::new();
    for name in ["server.out", "server.err"] {
        if let Ok(text) = std::fs::read_to_string(dir.join(name)) {
            s.push_str(&text);
        }
    }
    s
}

/// One key per shard, index = owning shard.
fn keys_by_shard(prefix: &str) -> Vec<String> {
    let mut keys: Vec<Option<String>> = vec![None; SHARDS];
    let mut i = 0u32;
    while keys.iter().any(Option::is_none) {
        let k = format!("{prefix}:{i}");
        let s = key_to_shard(k.as_bytes(), SHARDS);
        if keys[s].is_none() {
            keys[s] = Some(k);
        }
        i += 1;
        assert!(i < 100_000, "could not find a key for every shard");
    }
    keys.into_iter().flatten().collect()
}

fn send_raw(c: &mut common::Conn, cmds: &[&[&str]]) {
    let mut batch = Vec::new();
    for parts in cmds {
        batch.extend_from_slice(&common::encode(parts));
    }
    c.sock.write_all(&batch).expect("write batch");
}

/// Occupy the thread of the shard that owns `key_on_shard` for [`STALL_MS`].
///
/// `DEBUG SLEEP` blocks the thread that executes it, and a transaction body
/// executes on the shard that owns its keys — routed there when the
/// connection lives elsewhere. The replies are left unread; the caller reads
/// them with [`finish_stall`].
fn start_stall(port: u16, key_on_shard: &str) -> common::Conn {
    let mut s = common::Conn::open(port);
    let tagged = format!("{{{key_on_shard}}}:stall");
    let secs = format!("{:.3}", STALL_MS as f64 / 1000.0);
    send_raw(
        &mut s,
        &[
            &["MULTI"],
            &["SET", &tagged, "1"],
            &["DEBUG", "SLEEP", &secs],
            &["EXEC"],
        ],
    );
    s
}

fn finish_stall(mut s: common::Conn) {
    assert_eq!(
        s.read_replies(4),
        "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n+OK\r\n",
        "the stalling transaction must run DEBUG SLEEP"
    );
}

/// The write under test, issued by connection A while one shard is stalled.
#[derive(Clone, Copy, Debug)]
enum Op {
    FlushDb,
    ScatteredMset,
}

/// Run one round with shard `stalled` occupied. Returns whether the second
/// client's writes provably landed while the write under test was waiting.
fn round(
    port: u16,
    a: &mut common::Conn,
    b: &mut common::Conn,
    op: Op,
    keys: &[String],
    stalled: usize,
) -> bool {
    let db = (stalled + 1).to_string();
    assert_eq!(a.send(&["SELECT", &db]), "+OK\r\n");
    assert_eq!(b.send(&["SELECT", &db]), "+OK\r\n");
    for k in keys {
        assert_eq!(b.send(&["SET", k, "seed"]), "+OK\r\n");
    }

    let stall = start_stall(port, &keys[stalled]);
    let stall_start = Instant::now();
    std::thread::sleep(Duration::from_millis(STEP_MS));

    let a_value = format!("a{stalled}");
    match op {
        Op::FlushDb => send_raw(a, &[&["FLUSHDB"]]),
        Op::ScatteredMset => {
            let mut parts: Vec<&str> = vec!["MSET"];
            for k in keys {
                parts.push(k);
                parts.push(&a_value);
            }
            send_raw(a, &[&parts]);
        }
    }
    std::thread::sleep(Duration::from_millis(STEP_MS));

    // Every shard but the stalled one: a write there cannot be held up by the
    // stall unless B itself lives on the stalled shard.
    let b_value = format!("b{stalled}");
    for (i, k) in keys.iter().enumerate() {
        if i != stalled {
            assert_eq!(
                b.send(&["SET", k, &b_value]),
                "+OK\r\n",
                "{op:?}: B SET {k}"
            );
        }
    }
    let in_window = stall_start.elapsed() < Duration::from_millis(STALL_MS - STEP_MS);

    assert_eq!(
        a.read_replies(1),
        "+OK\r\n",
        "{op:?} with shard {stalled} stalled"
    );
    finish_stall(stall);
    in_window
}

fn scenario(op: Op) {
    let dir = common::unique_test_dir(&format!("moon-1084-leg-{op:?}"));
    let (mut server, port) = spawn(&dir);
    let keys = keys_by_shard(&format!("leg1084{op:?}"));

    let mut live: Vec<(String, String, String)> = Vec::new();
    {
        // ONE connection each for A and B: their shards stay fixed.
        let mut a = common::Conn::open(port);
        let mut b = common::Conn::open(port);
        let mut in_window = 0;
        for stalled in 0..SHARDS {
            if round(port, &mut a, &mut b, op, &keys, stalled) {
                in_window += 1;
            }
        }
        // At most one round has B on the stalled shard. If fewer than the
        // rest landed in time the host was too loaded to open the window,
        // and a green result would mean nothing.
        assert!(
            in_window >= SHARDS - 1,
            "{op:?}: only {in_window} of {SHARDS} rounds landed B's writes inside the stall"
        );
        for stalled in 0..SHARDS {
            let db = (stalled + 1).to_string();
            assert_eq!(b.send(&["SELECT", &db]), "+OK\r\n");
            for k in &keys {
                live.push((db.clone(), k.clone(), b.send(&["GET", k])));
            }
        }
    }

    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir);

    let mut c = common::Conn::open(port);
    let mut wrong = Vec::new();
    for (db, k, want) in &live {
        assert_eq!(c.send(&["SELECT", db]), "+OK\r\n");
        let got = c.send(&["GET", k]);
        if &got != want {
            wrong.push(format!("db {db} {k}: live {want:?}, recovered {got:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{op:?}: recovery did not reproduce the acknowledged keyspace — a write \
         applied after the local slice was logged before it (moon#1084):\n{wrong:#?}\n\
         --- server log ---\n{}",
        server_log(&dir)
    );
    drop(c);
    restarted.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flushdb_local_half_logs_before_the_broadcast_wait() {
    scenario(Op::FlushDb);
}

#[test]
fn scattered_mset_local_slice_logs_before_the_remote_wait() {
    scenario(Op::ScatteredMset);
}
