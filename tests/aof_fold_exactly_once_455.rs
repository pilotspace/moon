//! #455: a rewrite fold must not double-apply a write whose AOF record is
//! enqueued after an await that follows the mutation.
//!
//! A producer applies its mutation, then enqueues the record. When the writer
//! channel is full it parks in between — and while it is parked a fold can
//! snapshot the effect into the new base. A fold that split records by
//! position then wrote that record into the new incr, and the restart applied
//! it a second time: `INCR` counters came back above the value the server had
//! acknowledged. Records now carry the fold epoch read when the mutation ran,
//! and the writer drops every record stamped below the committed snapshot's
//! epoch, wherever it surfaces.
//!
//! The window is opened without touching the server's code: the writer's
//! `EverySec` fsync is stalled (`MOON_TEST_AOF_FSYNC_STALL_MS`, the same knob
//! the moon#769 pinning tests use), a wave of pipelined `INCR`s from many
//! connections fills the 10k writer channel so their producers park after
//! applying, and `BGREWRITEAOF` folds while they are parked.
//!
//! (The scenario used to park `EXEC` on a queued `WAIT` asking for more
//! replicas than exist. Redis answers a `WAIT` inside `MULTI` at once, and so
//! does moon since moon#1098, so that window closed — moon#1134. Parking
//! `EXEC` on a queued `SCRIPT LOAD` fan-out instead, the way
//! `tests/exec_intercept_log_order_1084.rs` does, does not reopen it either:
//! measured on `main` and on the pre-#1085 commit `3b596be0`,
//! `aof_rewrite_late_records_folded` never moves, because since moon#1084 the
//! `EXEC` body is logged in the same synchronous stretch as the executor,
//! before any intercept awaits. The parked producer below is the window that
//! is left, and it is the one the writer-side unit tests pin.)
//!
//! Vacuity guards, because a green run must mean the window opened: the
//! rewrite must commit while producers are parked, and
//! `INFO persistence`'s `aof_rewrite_late_records_folded` must move — it
//! counts exactly the records this fix drops.
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
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Connections writing concurrently. Each parks its own producer once the
/// writer channel is full, so the fold below meets many parked records, not
/// one.
const WRITERS: usize = 24;
/// `INCR`s per connection. `WRITERS * INCRS` must exceed the writer channel
/// (10k) by enough that producers are still parked when the fold snapshots.
const INCRS: usize = 2_000;
/// How long each `EverySec` fsync is held. Long enough that the channel fills
/// and stays full while the rewrite is requested.
const FSYNC_STALL_MS: &str = "3000";
/// The writer's first `EverySec` deadline is one second after it opens, so
/// nothing stalls before then. Keep writing until it has passed, or the wave
/// below drains straight through and no producer ever parks.
const WARMUP_MS: u64 = 1_400;

fn spawn(dir: &Path, stall: bool) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    common::spawn_listening_guarded(|port| {
        let mut cmd = Command::new(common::find_moon_binary());
        cmd.args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            // EverySec: the stalled fsync is what fills the channel. The
            // fence before the SIGKILL makes the tail durable anyway.
            "--appendfsync",
            "everysec",
            // Only the rewrite this test triggers may run.
            "--auto-aof-rewrite-percentage",
            "0",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir);
        if stall {
            cmd.env("MOON_TEST_AOF_FSYNC_STALL_MS", FSYNC_STALL_MS);
        }
        cmd.stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build first; MOON_BIN to override)")
    })
}

/// Value of `field` in an `INFO persistence` reply, if present.
fn info_field(info: &str, field: &str) -> Option<String> {
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
}

/// `aof_rewrite_late_records_folded`, or `None` on a build that predates the
/// counter (every build before #455's fix).
fn folded_records(c: &mut common::Conn) -> Option<u64> {
    info_field(
        &c.send(&["INFO", "persistence"]),
        "aof_rewrite_late_records_folded",
    )
    .and_then(|v| v.parse().ok())
}

fn wait_rewrite_done(c: &mut common::Conn, deadline: Instant) {
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
        assert!(Instant::now() < deadline, "rewrite never finished: {info}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn counter(i: usize) -> String {
    format!("ctr455:{i}")
}

/// One connection's wave: `INCRS` pipelined `INCR`s, then every reply read.
/// Returns the value the server acknowledged last.
fn incr_wave(port: u16, i: usize) -> i64 {
    let mut c = common::Conn::open(port);
    let key = counter(i);
    let mut batch = Vec::new();
    for _ in 0..INCRS {
        batch.extend_from_slice(&common::encode(&["INCR", &key]));
    }
    c.sock.write_all(&batch).expect("write wave");
    let replies = c.read_replies_within(INCRS, Duration::from_secs(120));
    let last = replies
        .trim_end()
        .rsplit("\r\n")
        .next()
        .and_then(|l| l.strip_prefix(':'))
        .and_then(|n| n.parse::<i64>().ok())
        .unwrap_or_else(|| panic!("{key}: last reply of the wave: {:?}", replies.get(..80)));
    assert!(
        !replies.contains("-ERR") && !replies.contains("-AOF"),
        "{key}: a write in the wave was refused: {:?}",
        replies.get(..200)
    );
    last
}

/// A rewrite folded while producers are parked between their mutation and
/// their AOF record applies each of those writes exactly once after a
/// `kill -9`.
#[test]
#[ignore]
fn a_fold_over_parked_producers_replays_each_write_once() {
    let dir = common::unique_test_dir("moon-455-fold");
    let (mut server, port) = spawn(&dir, true);
    let mut admin = common::Conn::open(port);
    let folded_before = folded_records(&mut admin);

    // Write past the writer's first fsync deadline so the wave below meets a
    // stalled writer rather than an empty channel.
    let warm_until = Instant::now() + Duration::from_millis(WARMUP_MS);
    let mut i = 0u64;
    while Instant::now() < warm_until {
        assert_eq!(admin.send(&["SET", "warm455", &i.to_string()]), "+OK\r\n");
        i += 1;
    }

    // The waves run while the writer is inside a stalled fsync: the channel
    // fills, and every producer parks AFTER applying its INCR.
    let waves: Vec<std::thread::JoinHandle<i64>> = (0..WRITERS)
        .map(|i| std::thread::spawn(move || incr_wave(port, i)))
        .collect();

    // Fold in the middle of the wave, retrying while the channel is too full
    // for the rewrite request itself.
    let deadline = Instant::now() + Duration::from_secs(120);
    std::thread::sleep(Duration::from_millis(300));
    loop {
        let reply = admin.send_within(&["BGREWRITEAOF"], Duration::from_secs(30));
        if reply.starts_with('+') {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "BGREWRITEAOF never accepted: {reply:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    wait_rewrite_done(&mut admin, deadline);

    let acked: Vec<i64> = waves
        .into_iter()
        .map(|h| h.join().expect("wave thread"))
        .collect();
    assert!(
        acked.iter().all(|&v| v == INCRS as i64),
        "a wave did not finish: {acked:?}"
    );

    // Make the tail durable without the stall: under `always` each batch is
    // fsynced, so a reply to the fence proves everything before it is on
    // disk. Only then is a SIGKILL a clean cut.
    assert!(
        admin
            .send(&["CONFIG", "SET", "appendfsync", "always"])
            .starts_with('+'),
        "CONFIG SET appendfsync always refused"
    );
    assert_eq!(admin.send(&["SET", "fence455", "1"]), "+OK\r\n");
    let folded_after = folded_records(&mut admin);

    server.kill_now();
    common::wait_for_port_down(port);
    let (mut restarted, port) = spawn(&dir, false);
    let mut c = common::Conn::open(port);
    let wrong: Vec<String> = (0..WRITERS)
        .filter_map(|i| {
            let key = counter(i);
            let got = c.send(&["GET", &key]);
            let want = format!("${}\r\n{}\r\n", INCRS.to_string().len(), INCRS);
            (got != want).then(|| format!("{key}: acknowledged {INCRS}, recovered {got:?}"))
        })
        .collect();
    restarted.kill_now();
    assert!(
        wrong.is_empty(),
        "a write acknowledged before the fold was applied a second time on replay \
         (its record reached the writer after the snapshot that already held it):\n{wrong:#?}"
    );

    // Vacuity: the fold must have met at least one record whose mutation its
    // snapshot already held, or this run proves nothing. Checked after the
    // data assertion so a build without the counter still fails for the
    // reason that matters.
    match (folded_before, folded_after) {
        (Some(before), Some(after)) => assert!(
            after > before,
            "aof_rewrite_late_records_folded did not move ({before} -> {after}): no producer \
             was parked across the fold's snapshot, so the #455 window never opened"
        ),
        _ => panic!(
            "INFO persistence has no aof_rewrite_late_records_folded counter — this build \
             predates #455's fix, so the window cannot be verified from here"
        ),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
