//! moon#831: a Lua / Functions script write under `appendfsync always` is
//! acknowledged only AFTER the fsync covering it — never before.
//!
//! ## The defect
//!
//! `EVAL`/`EVALSHA` carry no `WRITE` flag by design (`scripting/bridge.rs`):
//! the bridge itself emits one effect record per successful inner
//! `redis.call` (`emit_effect` → `record_effect_write` →
//! `send_append_bounded_blocking`, fire-and-forget, no waiter). Ordinary
//! writes then join the batch-end barrier set (`local_leg_write_idxs`) and
//! `resolve_local_leg_barrier` awaits ONE `fsync_barrier` before the batch is
//! serialized. The script arms — `try_handle_eval` / `try_handle_evalsha` /
//! FCALL in `try_handle_functions` (monoio), the inline arms in
//! `handler_sharded` (tokio), and `route_script_elsewhere` for a script that
//! ran on the shard owning its keys — pushed their reply and never joined that
//! set, so a batch holding only script writes issued ZERO fsyncs before the
//! reply. The records were durable only by the writer's policy-driven fsync
//! landing first, which is what #763's waiter-gating removes.
//!
//! ## What this file pins
//!
//! 1. **Deterministic** — `MOON_TEST_AOF_FSYNC_FAIL=1` makes every barrier
//!    ack `FsyncFailed`. A plain `SET` already answers `AOF_FSYNC_ERR` (the
//!    positive control: the injection works and the ordinary path is gated).
//!    A script that writes must answer the SAME error, on every arm: `EVAL`,
//!    `EVALSHA`, `FCALL`, a pipelined batch of scripts, and at `--shards 4`
//!    the routed arm (keys owned by another shard). A read-only script must
//!    NOT be gated (negative control: no over-gating).
//! 2. **Empirical, kill -9** — a second connection stalls the writer with a
//!    burst of multi-megabyte `SET`s so a script's record sits in the writer
//!    channel (user-space) for milliseconds. If the script's reply arrives
//!    while the record is still there and the server is SIGKILLed, the key is
//!    gone after restart although the client holds a success reply. With the
//!    barrier the reply cannot arrive before the record is on disk, so an
//!    acknowledged key ALWAYS survives. Same shape with a plain `SET` is the
//!    control — it must survive on every build.
//!
//! ## Reddening proof (`origin/main` @ `b04e8990`, macOS aarch64, monoio,
//! `MOON_BIN=moon-main-b04e8990`) — see `tmp/perf-campaign/FIX-831.md`.
//!
//! Harness flags: `--port` (free), `--dir` (fresh tempdir), `--shards`,
//! `--appendonly yes --appendfsync always` (the contract under test),
//! `--disk-free-min-pct 0` (the dev host's root volume trips the default
//! guard and every write would answer the diskfull error instead).

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::process::Command;
use std::time::{Duration, Instant};

const AOF_FSYNC_ERR: &str = "ERR AOF fsync failed; write not durable";

const WRITE_SCRIPT: &str = "redis.call('SET', KEYS[1], ARGV[1]); return 1";
const READ_SCRIPT: &str = "return redis.call('GET', KEYS[1])";
const FUNC_LIB: &str = "#!lua name=lib831\n\
    redis.register_function('setk', function(keys, args) \
        redis.call('SET', keys[1], args[1]); return 1 end)";

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Server {
    guard: common::ServerGuard,
    port: u16,
}

/// `$TMPDIR` on macOS lives on the root volume group, observed at ~95% full
/// on dev hosts. Scratch under the repo's own volume instead (same choice as
/// the moon#660 / moon#833 / moon#838 files).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/i831-test-tmp");
    std::fs::create_dir_all(&base).expect("create i831-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("i831-")
        .tempdir_in(&base)
        .expect("tempdir_in target/i831-test-tmp")
}

fn spawn_on(dir: &std::path::Path, shards: u16, env: &[(&str, &str)]) -> Server {
    let bin = common::find_moon_binary();
    let dir_arg = dir.to_string_lossy().into_owned();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        let mut c = Command::new(&bin);
        c.args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--disk-free-min-pct",
            "0",
            "--dir",
            &dir_arg,
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"));
        for (k, v) in env {
            c.env(k, v);
        }
        c.spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    assert!(
        wait_for_pong(port, Duration::from_secs(60)),
        "moon on {port} never answered PING; stderr:\n{}",
        std::fs::read_to_string(dir.join("moon.stderr.log")).unwrap_or_default()
    );
    Server { guard, port }
}

fn wait_for_pong(port: u16, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if let Ok(mut s) = std::net::TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        ) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 7];
            if s.write_all(b"PING\r\n").is_ok()
                && std::io::Read::read_exact(&mut s, &mut buf).is_ok()
                && buf.starts_with(b"+PONG")
            {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn is_fsync_err(reply: &str) -> bool {
    reply.starts_with('-') && reply.contains(AOF_FSYNC_ERR)
}

// ---------------------------------------------------------------------------
// 1. Deterministic: the reply must carry the fsync outcome
// ---------------------------------------------------------------------------

/// Every script arm on a `--shards 1` server: a script that writes answers
/// the barrier's failure exactly as a plain `SET` does; a script that only
/// reads is untouched by the barrier.
#[test]
fn script_write_reply_carries_fsync_outcome_single_shard() {
    let tmp = test_tmpdir();
    let srv = spawn_on(tmp.path(), 1, &[("MOON_TEST_AOF_FSYNC_FAIL", "1")]);
    let mut c = common::Conn::open(srv.port);

    // Positive control: the injection is live and the ordinary write path
    // is gated by it. Without this line a green run could mean "no fsync
    // ever happened" rather than "the script path is gated".
    let control = c.send(&["SET", "ctl", "v"]);
    assert!(
        is_fsync_err(&control),
        "control SET must surface the injected fsync failure; got {control:?}"
    );

    // EVAL that writes.
    let eval = c.send(&["EVAL", WRITE_SCRIPT, "1", "k:eval", "v"]);
    assert!(
        is_fsync_err(&eval),
        "EVAL with a write was acked before its fsync outcome: reply {eval:?}, \
         expected -{AOF_FSYNC_ERR}"
    );

    // EVALSHA that writes.
    let sha = c.send(&["SCRIPT", "LOAD", WRITE_SCRIPT]);
    let sha = sha.trim_start_matches('$');
    let sha = sha.split("\r\n").nth(1).unwrap_or("").to_owned();
    assert_eq!(sha.len(), 40, "SCRIPT LOAD reply was not a sha1: {sha:?}");
    let evalsha = c.send(&["EVALSHA", &sha, "1", "k:evalsha", "v"]);
    assert!(
        is_fsync_err(&evalsha),
        "EVALSHA with a write was acked before its fsync outcome: reply {evalsha:?}"
    );

    // FCALL that writes.
    let loaded = c.send(&["FUNCTION", "LOAD", "REPLACE", FUNC_LIB]);
    assert!(
        loaded.starts_with('$') || loaded.starts_with('+'),
        "FUNCTION LOAD failed: {loaded:?}"
    );
    let fcall = c.send(&["FCALL", "setk", "1", "k:fcall", "v"]);
    assert!(
        is_fsync_err(&fcall),
        "FCALL with a write was acked before its fsync outcome: reply {fcall:?}"
    );

    // A pipelined batch of script writes: every slot joins the ONE batch
    // barrier and every slot is patched on its failure.
    let batch = c.pipeline(&[
        &["EVAL", WRITE_SCRIPT, "1", "k:p1", "v"],
        &["EVAL", WRITE_SCRIPT, "1", "k:p2", "v"],
        &["EVALSHA", &sha, "1", "k:p3", "v"],
    ]);
    let errs = batch.matches(AOF_FSYNC_ERR).count();
    assert_eq!(
        errs, 3,
        "pipelined script writes: expected 3 fsync errors, got {errs} in {batch:?}"
    );

    // Negative control: a script that only READS must not be gated. The
    // earlier writes were applied in memory (fsync failed AFTER the apply),
    // so the read has a value to return, and that value must come back.
    let read = c.send(&["EVAL", READ_SCRIPT, "1", "ctl"]);
    assert!(
        !read.starts_with('-'),
        "read-only script must not be gated by the fsync barrier; got {read:?}"
    );
    assert!(
        read.contains("\r\nv\r\n"),
        "read-only script lost its value: {read:?}"
    );
    let ro = c.send(&["EVAL_RO", READ_SCRIPT, "1", "ctl"]);
    assert!(
        !ro.starts_with('-'),
        "EVAL_RO must not be gated; got {ro:?}"
    );

    drop(c);
    drop(srv);
}

/// `--shards 4`: the script's keys are spread over every shard, so some run
/// on the connection's own shard and the rest are routed to the owner
/// (`route_script_elsewhere` / `ShardMessage::Execute`). Every reply must
/// carry the fsync outcome regardless of which shard ran the script — the
/// client cannot see routing and routing must not change the contract.
#[test]
fn script_write_reply_carries_fsync_outcome_across_shards() {
    let tmp = test_tmpdir();
    let srv = spawn_on(tmp.path(), 4, &[("MOON_TEST_AOF_FSYNC_FAIL", "1")]);
    let mut c = common::Conn::open(srv.port);

    let control = c.send(&["SET", "ctl", "v"]);
    assert!(
        is_fsync_err(&control),
        "control SET must surface the injected fsync failure; got {control:?}"
    );
    let loaded = c.send(&["FUNCTION", "LOAD", "REPLACE", FUNC_LIB]);
    assert!(
        loaded.starts_with('$') || loaded.starts_with('+'),
        "FUNCTION LOAD failed: {loaded:?}"
    );

    let mut acked = Vec::new();
    for i in 0..16 {
        let key = format!("k:{i}");
        let eval = c.send(&["EVAL", WRITE_SCRIPT, "1", &key, "v"]);
        if !is_fsync_err(&eval) {
            acked.push(format!("EVAL {key} -> {eval:?}"));
        }
        let fcall = c.send(&["FCALL", "setk", "1", &key, "v"]);
        if !is_fsync_err(&fcall) {
            acked.push(format!("FCALL {key} -> {fcall:?}"));
        }
    }
    assert!(
        acked.is_empty(),
        "{} of 32 script writes were acked before their fsync outcome at --shards 4:\n{}",
        acked.len(),
        acked.join("\n")
    );

    // Negative control on the routed arm too: reads are never gated.
    let mut gated = Vec::new();
    for i in 0..16 {
        let key = format!("k:{i}");
        let read = c.send(&["EVAL", READ_SCRIPT, "1", &key]);
        if read.starts_with('-') {
            gated.push(format!("{key} -> {read:?}"));
        }
    }
    assert!(
        gated.is_empty(),
        "read-only scripts were gated:\n{}",
        gated.join("\n")
    );

    drop(c);
    drop(srv);
}

// ---------------------------------------------------------------------------
// 2. Empirical: kill -9 after the reply — an acked script write survives
// ---------------------------------------------------------------------------

/// One attempt of the kill -9 shape. Returns `(reply, survived)`; `survived`
/// is `None` when the reply was not a success (nothing was promised, so
/// nothing can have been broken).
fn kill9_attempt(shards: u16, use_script: bool, attempt: usize) -> (String, Option<bool>) {
    let tmp = test_tmpdir();
    let dir = tmp.path().to_path_buf();
    let mut srv = spawn_on(&dir, shards, &[]);
    let key = format!("survivor:{attempt}");

    // Connection A: a burst of multi-megabyte SETs, replies never read. The
    // writer spends milliseconds per batch in write+fsync, so anything that
    // lands in its channel meanwhile waits there in user space.
    let big = "x".repeat(6 * 1024 * 1024);
    let mut a = common::Conn::open(srv.port);
    let mut burst = Vec::new();
    for i in 0..20 {
        burst.extend_from_slice(&common::encode(&["SET", &format!("big:{i}"), &big]));
    }
    a.sock.write_all(&burst).expect("write burst");
    std::thread::sleep(Duration::from_millis(15));

    // Connection B: the write under test, then SIGKILL the instant a reply
    // is in hand.
    let mut b = common::Conn::open(srv.port);
    let reply = if use_script {
        b.send(&["EVAL", WRITE_SCRIPT, "1", &key, "v"])
    } else {
        b.send(&["SET", &key, "v"])
    };
    srv.guard.kill_now();
    drop(a);
    drop(b);
    let acked = reply.starts_with(':') || reply.starts_with('+');
    if !acked {
        return (reply, None);
    }

    // Restart on the same directory and ask for the promised key.
    common::wait_for_port_down(srv.port);
    let srv2 = spawn_on(&dir, shards, &[]);
    let mut c = common::Conn::open(srv2.port);
    let got = c.send(&["GET", &key]);
    drop(c);
    drop(srv2);
    (reply, Some(got.contains("\r\nv\r\n")))
}

/// The contract: a script write the client was told succeeded is on disk.
/// The plain-SET control runs the identical shape and must survive on every
/// build — if it does not, the harness (restart, replay) is broken and the
/// script result says nothing.
#[test]
fn kill9_acked_script_write_survives_restart() {
    const ATTEMPTS: usize = 4;
    let mut control_lost = Vec::new();
    let mut script_lost = Vec::new();
    let mut script_acked = 0usize;
    for attempt in 0..ATTEMPTS {
        let (reply, survived) = kill9_attempt(1, false, attempt);
        if survived == Some(false) {
            control_lost.push(format!("attempt {attempt}: SET acked {reply:?} then LOST"));
        }
        let (reply, survived) = kill9_attempt(1, true, attempt);
        if survived.is_some() {
            script_acked += 1;
        }
        if survived == Some(false) {
            script_lost.push(format!("attempt {attempt}: EVAL acked {reply:?} then LOST"));
        }
    }
    assert!(
        control_lost.is_empty(),
        "HARNESS: a plain SET was acked and then lost — the restart/replay leg is broken:\n{}",
        control_lost.join("\n")
    );
    assert!(
        script_lost.is_empty(),
        "{} of {script_acked} acked script writes were LOST across kill -9 (moon#831):\n{}",
        script_lost.len(),
        script_lost.join("\n")
    );
}
