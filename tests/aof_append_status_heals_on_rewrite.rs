//! moon#1094: `aof_last_append_status` / `aof_last_write_status` return to
//! `ok` once a rewrite that postdates the last dropped append COMMITS.
//!
//! The status turns `err` when an acked append is dropped: the AOF lacks a
//! write the client was told about. Before this fix it never went back, even
//! after `BGREWRITEAOF` folded the live keyspace, the dropped write included,
//! into a fresh base, so it reported a hole that no longer existed for the
//! rest of the process's life.
//!
//! The drop is forced with the writer-side test hook the moon#838 suite uses:
//! `MOON_TEST_AOF_FSYNC_STALL_MS` holds the writer before each everysec
//! proactive fsync, and `--aof-fsync-timeout-ms 100` makes the generic LOCAL
//! leg give up waiting long before the stall ends: the write is applied, its
//! record is dropped, and the client gets `ERR AOF fsync failed`. That is the
//! drop this status reports.
//!
//! At `--shards 4` a write for a key another shard owns is a ROUTED leg, and
//! since moon#769 the owning shard admits it against its writer BEFORE
//! applying it: under the stall it is refused unapplied
//! (`aof_backpressure_refused`), nothing is dropped, and the status correctly
//! stays `ok`. An untagged burst is therefore almost all routed, drops
//! nothing, and proves nothing about this status.
//!
//! So each burst is hash-tagged to a single shard, and a round sends one
//! burst per shard down ONE connection. A connection is served by exactly one
//! shard, so whichever shard it landed on, exactly one of a round's bursts is
//! all-local for it — the local leg is the only one that is applied first and
//! can then have its record dropped. Every run asserts that precondition,
//! `aof_backpressure_dropped > 0`, before it asserts `err`, so the test can
//! never again pass or fail on a drop that never happened.
//!
//! The third case in the issue (a drop that lands during the fold, after its
//! snapshot, keeps `err`) cannot be timed deterministically from outside the
//! process; `persistence::aof::rewrite_overflow` unit-tests it
//! (`a_drop_during_the_fold_survives_its_commit`).
//!
//! Runs at `--shards 1` (one writer) and `--shards 4`, where the status is the
//! AND across the per-shard writers.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

struct Server {
    _guard: common::ServerGuard,
    port: u16,
    _tmp: tempfile::TempDir,
}

fn spawn(shards: u32) -> Server {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_path_buf();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--aof-fsync-timeout-ms",
                "100",
                // The only rewrite is the test's own BGREWRITEAOF. The bursts
                // can cross the 64 MB auto-rewrite minimum on a fast host, and
                // an auto-rewrite committing after the drops would correctly
                // clear the status before the test reads `err`.
                "--auto-aof-rewrite-percentage",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .env("MOON_TEST_AOF_FSYNC_STALL_MS", "0")
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    Server {
        _guard: guard,
        port,
        _tmp: tmp,
    }
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s
}

/// One command, one bulk/simple/error reply as text.
fn command(port: u16, parts: &[&str]) -> String {
    let mut s = connect(port);
    s.write_all(&common::encode(parts)).unwrap();
    let mut raw = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        let n = s.read(&mut chunk).expect("read reply");
        assert!(n > 0, "connection closed mid-reply");
        raw.extend_from_slice(&chunk[..n]);
        if let Some(len) = common::framed_len(&raw, 1) {
            return String::from_utf8_lossy(&raw[..len]).replace('\r', "");
        }
    }
}

fn info_field(port: u16, field: &str) -> String {
    let info = command(port, &["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(field).and_then(|v| v.strip_prefix(':')))
        .unwrap_or_else(|| panic!("INFO persistence has no {field}:\n{info}"))
        .to_string()
}

fn info_count(port: u16, field: &str) -> u64 {
    info_field(port, field)
        .parse()
        .unwrap_or_else(|e| panic!("INFO {field} is not a number: {e}"))
}

/// One hash tag per shard, so a burst's keys all belong to one shard. Found
/// with moon's own routing function rather than hard-coded.
fn tags_per_shard(shards: u32) -> Vec<String> {
    let n = shards as usize;
    let mut tags: Vec<Option<String>> = vec![None; n];
    for i in 0.. {
        let tag = format!("s{i}");
        let shard = moon::shard::dispatch::key_to_shard(tag.as_bytes(), n);
        if tags[shard].is_none() {
            tags[shard] = Some(tag);
        }
        if tags.iter().all(Option::is_some) {
            break;
        }
    }
    tags.into_iter().flatten().collect()
}

/// Commands per burst. More than the 10k writer channel holds, so a burst the
/// writer cannot drain (it is inside the fsync stall) fills it.
const BURST: usize = 20_000;

/// One pipelined SET burst, every key tagged to `tag`'s shard, on `s`.
/// Returns how many replies were errors.
///
/// A burst the owning shard refuses is answered with an ~85-byte error per
/// command — about 1.7 MB, far more than the socket buffers hold. Writing the
/// whole burst before reading any of it would wedge both sides: the server
/// blocks writing replies, so it stops reading, so the burst never finishes
/// being sent. The write runs on its own thread and the replies are drained
/// here as they arrive.
fn burst(s: &TcpStream, tag: &str, round: usize) -> usize {
    let mut wire = Vec::with_capacity(BURST * 48);
    for i in 0..BURST {
        wire.extend_from_slice(&common::encode(&[
            "SET",
            &format!("{{{tag}}}:{round}:{i}"),
            "v",
        ]));
    }
    let mut w = s.try_clone().expect("try_clone");
    let writer = std::thread::spawn(move || w.write_all(&wire).expect("write burst"));

    let mut r: &TcpStream = s;
    let mut raw = Vec::new();
    let mut chunk = [0u8; 65536];
    while common::framed_len(&raw, BURST).is_none() {
        let n = r.read(&mut chunk).expect("read burst replies");
        assert!(n > 0, "server closed mid-burst");
        raw.extend_from_slice(&chunk[..n]);
    }
    writer.join().expect("burst writer thread");
    raw.split(|&b| b == b'\n')
        .filter(|l| l.first() == Some(&b'-'))
        .count()
}

/// Pipelined SET bursts until the server reports a dropped acked append
/// (`aof_backpressure_dropped > 0`). Each round opens one connection and
/// sends one burst per shard down it, so one of them is guaranteed to be all
/// LOCAL legs for that connection's shard — the only legs that are applied
/// and then dropped rather than refused unapplied. Returns the error replies
/// seen, for the failure message.
fn drop_some_appends(port: u16, shards: u32) -> usize {
    let tags = tags_per_shard(shards);
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut errors = 0usize;
    let mut round = 0usize;
    while info_count(port, "aof_backpressure_dropped") == 0 && Instant::now() < deadline {
        let s = connect(port);
        for tag in &tags {
            errors += burst(&s, tag, round);
            round += 1;
            if info_count(port, "aof_backpressure_dropped") > 0 {
                return errors;
            }
        }
    }
    errors
}

fn wait_for_rewrite(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if info_field(port, "aof_rewrite_in_progress") == "0" {
            return;
        }
        assert!(Instant::now() < deadline, "BGREWRITEAOF never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn status_heals_after_a_covering_rewrite(shards: u32) {
    let srv = spawn(shards);
    assert_eq!(info_field(srv.port, "aof_last_append_status"), "ok");

    let errors = drop_some_appends(srv.port, shards);
    // The premise, asserted on the server's own counter: an acked append was
    // applied and its record dropped. A refusal (routed, unapplied) is not
    // one, loses nothing, and must not be mistaken for it.
    assert!(
        info_count(srv.port, "aof_backpressure_dropped") > 0,
        "shards={shards}: no acked append was dropped ({errors} error replies; {}), so \
         there is nothing for the status to report",
        command(srv.port, &["INFO", "persistence"]).replace('\n', " ")
    );
    assert_eq!(
        info_field(srv.port, "aof_last_append_status"),
        "err",
        "shards={shards}: a dropped acked append must report err ({errors} error replies; {})",
        command(srv.port, &["INFO", "persistence"]).replace('\n', " ")
    );
    assert_eq!(info_field(srv.port, "aof_last_write_status"), "err");

    // No writes from here on, so nothing can be dropped after the snapshot.
    let reply = command(srv.port, &["BGREWRITEAOF"]);
    assert!(reply.starts_with('+'), "BGREWRITEAOF: {reply:?}");
    wait_for_rewrite(srv.port);
    assert_eq!(
        info_field(srv.port, "aof_last_bgrewrite_status"),
        "ok",
        "shards={shards}: the rewrite must have committed"
    );
    // With the PerShard layout the last writer to finish clears
    // `aof_rewrite_in_progress` when it commits the manifest, and each writer
    // adopts the commit (and heals) when it wakes from that barrier, a moment
    // later. Poll briefly for that, never for a new rewrite.
    let deadline = Instant::now() + Duration::from_secs(5);
    while info_field(srv.port, "aof_last_append_status") != "ok" && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        info_field(srv.port, "aof_last_append_status"),
        "ok",
        "shards={shards}: a committed rewrite after the last drop holds the dropped write; \
         the status must return to ok"
    );
    assert_eq!(info_field(srv.port, "aof_last_write_status"), "ok");
}

#[test]
fn append_status_heals_after_a_covering_rewrite_one_shard() {
    status_heals_after_a_covering_rewrite(1);
}

#[test]
fn append_status_heals_after_a_covering_rewrite_four_shards() {
    status_heals_after_a_covering_rewrite(4);
}
