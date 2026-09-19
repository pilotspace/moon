//! #455: a rewrite fold must not double-apply a write whose AOF record is
//! enqueued after an await that follows the mutation.
//!
//! `EXEC` used to run its body synchronously, then fill the slots of
//! connection intercepts queued inside it (`WAIT`, `CONFIG`, ...), which can
//! await, and only then enqueue the body's AOF records. A `BGREWRITEAOF` whose
//! snapshot landed in that window captured the body's effect in the new base,
//! and the fold, which split records by position, put the late records into
//! the new incr: an `INCR` inside such an `EXEC` came back from a restart
//! applied twice.
//!
//! Two changes now close it from both sides. Records carry the fold epoch
//! read when the mutation ran, and the writer drops every record stamped
//! below the committed snapshot's epoch. The writer side is pinned by the
//! unit tests in `persistence::aof`, including a record that reaches the
//! channel only after the snapshot, which is what a producer parked on a
//! full channel produces. And since moon#1084 the `EXEC` body is logged
//! before the intercepts await, so in this scenario its records reach the
//! writer before the fold's cut and the late-record window no longer opens.
//! This test keeps the end-to-end guarantee for the scenario that exposed
//! the bug: a rewrite during a parked `EXEC`, then kill -9, applies the
//! `INCR` exactly once.
//!
//! Black-box over a real `moon` process (needs a prebuilt binary):
//!
//! ```text
//! MOON_BIN=./target/release-fast/moon \
//!   cargo test --test aof_fold_exactly_once_455 -- --ignored --nocapture
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long the `WAIT` inside the transaction holds `EXEC` between the body's
/// mutation and its AOF enqueue. No replica ever acks, so it always runs out.
const WAIT_MS: u64 = 3000;

fn start_moon(port: u16, dir: &std::path::Path, shards: usize) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            // Only the rewrite this test triggers may run.
            "--auto-aof-rewrite-percentage",
            "0",
            "--disk-free-min-pct",
            "0",
        ])
        .arg("--dir")
        .arg(dir)
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (build first; MOON_BIN to override)")
}

fn spawn(dir: &std::path::Path, shards: usize) -> (common::ServerGuard, u16) {
    common::spawn_listening_guarded(|port| start_moon(port, dir, shards))
}

/// Value of `field` in an `INFO persistence` reply, if present.
fn info_field(info: &str, field: &str) -> Option<String> {
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
}

fn wait_rewrite_done(port: u16, deadline: Instant) {
    let mut c = common::Conn::open(port);
    loop {
        let info = c.send(&["INFO", "persistence"]);
        if info_field(&info, "aof_rewrite_in_progress").as_deref() == Some("0") {
            assert_eq!(
                info_field(&info, "aof_last_bgrewrite_status").as_deref(),
                Some("ok"),
                "the rewrite under test failed: {info}"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "rewrite did not finish inside the WAIT window: {info}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn run_case(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut server, port) = spawn(dir.path(), shards);

    let mut a = common::Conn::open(port);
    assert_eq!(a.send(&["SET", "k", "0"]), "+OK\r\n");

    // MULTI / INCR / WAIT / EXEC as one batch; do not read the replies yet —
    // EXEC is parked in WAIT with the INCR already applied.
    let wait_ms = WAIT_MS.to_string();
    let mut batch = Vec::new();
    for cmd in [
        &["MULTI"][..],
        &["INCR", "k"][..],
        &["WAIT", "1", &wait_ms][..],
        &["EXEC"][..],
    ] {
        batch.extend_from_slice(&common::encode(cmd));
    }
    a.sock.write_all(&batch).expect("send transaction");
    let exec_sent = Instant::now();

    // The body ran: the INCR is in memory while EXEC waits.
    let mut b = common::Conn::open(port);
    let deadline = exec_sent + Duration::from_millis(WAIT_MS / 2);
    loop {
        if b.send(&["GET", "k"]) == "$1\r\n1\r\n" {
            break;
        }
        assert!(Instant::now() < deadline, "EXEC body never ran");
        std::thread::sleep(Duration::from_millis(10));
    }

    // Fold while EXEC is still parked in WAIT, after its body ran. (The
    // wait below asserts the rewrite completed ok inside that window.)
    let reply = b.send(&["BGREWRITEAOF"]);
    assert!(reply.starts_with('+'), "BGREWRITEAOF refused: {reply:?}");
    wait_rewrite_done(
        port,
        exec_sent + Duration::from_millis(WAIT_MS.saturating_sub(500)),
    );

    let replies = a.read_replies(4);
    assert!(
        replies.ends_with("*2\r\n:1\r\n:0\r\n"),
        "EXEC must commit the INCR (and WAIT must time out with 0 acks): {replies:?}"
    );
    assert!(
        exec_sent.elapsed() >= Duration::from_millis(WAIT_MS),
        "EXEC returned before WAIT ran out — the window under test never opened"
    );

    server.kill_now();
    let (mut server, port) = spawn(dir.path(), shards);
    let mut c = common::Conn::open(port);
    assert_eq!(
        c.send(&["GET", "k"]),
        "$1\r\n1\r\n",
        "shards={shards}: the INCR committed by EXEC must be applied exactly once after \
         restart (2 = the record was replayed on top of a base that already held it)"
    );
    server.kill_now();
}

#[test]
#[ignore]
fn exec_parked_in_wait_across_a_rewrite_replays_once_toplevel() {
    run_case(1);
}
