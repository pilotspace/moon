//! moon#1265: a shard's spill thread that panics is respawned, and a crash
//! loop degrades the shard instead of leaving it answering -OOM for good.
//!
//! Real server, real spill thread, panic injected with the test-only
//! `MOON_TEST_SPILL_PANIC_FILE` hook (`storage::tiered::spill_thread::fault`)
//! at `after-write`: a flush wrote its spill file and dies before announcing
//! it — every request in that batch is in flight, its file is on disk and
//! unlisted.
//!
//! - `a_spill_thread_panic_mid_flight_is_survived`: one panic while keys are
//!   being spilled and DELeted in flight. The thread comes back
//!   (`spill_thread_restarts`), eviction spills again
//!   (`spill_batches_flushed` moves after the restart), and every key reads
//!   right — live keys their value, deleted keys nil — both live and after
//!   SIGKILL + restart (no in-flight key lost, none resurrected).
//! - `a_crash_loop_degrades_the_shard_and_keeps_serving`: the thread panics
//!   at every flush. After five respawns the shard is degraded
//!   (`spill_thread_degraded:1`); writes then succeed (the evicting policy
//!   drops, as with no spill thread) instead of -OOM, and no key ever reads
//!   a wrong value; keys already cold stay readable.
//!
//! Run:
//!   cargo build --release --bin moon
//!   MOON_BIN=target/release/moon cargo test --release \
//!     --test spill_thread_supervision_1265 -- --ignored --test-threads=1
//! `MOON_TEST_SPILL_SUP_SHARDS=<n>` (default 1) sets `--shards`.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::Conn;

const VALUE_LEN: usize = 600;
/// Keys that overflow the 8 MiB `maxmemory` about 1.2 times: spilling runs.
const FILL: usize = 16_000;

fn shards() -> usize {
    std::env::var("MOON_TEST_SPILL_SUP_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(1)
}

fn panic_file(dir: &Path) -> PathBuf {
    dir.join("spill-panic")
}

fn start(dir: &Path) -> (common::ServerGuard, u16) {
    let off = dir.join("off");
    std::fs::create_dir_all(&off).unwrap();
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let (child, port) = common::spawn_listening(move |port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards().to_string(),
                "--maxmemory",
                "8388608",
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off.to_str().unwrap(),
                "--appendonly",
                "yes",
                "--cold-orphan-sweep-interval-secs",
                "3600",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .env("MOON_TEST_SPILL_PANIC_FILE", panic_file(&dir))
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    (common::ServerGuard::new(child), port)
}

fn value(i: usize) -> String {
    let head = format!("v{i}-");
    format!("{head}{}", "x".repeat(VALUE_LEN - head.len()))
}

/// Pipeline `SET k:i value(i)` for `range`; returns how many were refused
/// `-OOM` (the rest must be `+OK`).
fn set_range(port: u16, range: std::ops::Range<usize>) -> usize {
    set_range_refused(port, range).len()
}

/// [`set_range`], returning the indexes whose `SET` was refused `-OOM`
/// (replies arrive in order, one per `SET`).
fn set_range_refused(port: u16, range: std::ops::Range<usize>) -> Vec<usize> {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut buf = Vec::new();
    for i in range.clone() {
        buf.extend_from_slice(&common::encode(&["SET", &format!("k:{i}"), &value(i)]));
    }
    s.write_all(&buf).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    let mut out = Vec::new();
    let mut chunk = [0u8; 65536];
    while out.iter().filter(|&&b| b == b'\n').count() < range.len() {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "conn closed");
        out.extend_from_slice(&chunk[..n]);
    }
    let text = String::from_utf8_lossy(&out);
    let replies: Vec<&str> = text.split("\r\n").filter(|l| !l.is_empty()).collect();
    assert_eq!(replies.len(), range.len(), "one reply per SET");
    let mut refused = Vec::new();
    for (i, line) in range.zip(replies) {
        if line.starts_with("-OOM") {
            refused.push(i);
        } else {
            assert_eq!(line, "+OK", "SET reply");
        }
    }
    refused
}

/// [`set_range`], then retry ONLY the refused keys (100 ms apart, up to
/// 10 s): (refused at first, still refused at the end). A burst right after
/// a respawn may outrun the new thread for a moment on a loaded host (WS25
/// risk 1: death and recovery are observed on the shard's tick); what
/// moon#1265 fixed is the PERMANENT `-OOM` of a shard whose spill thread was
/// gone. An acknowledged key is never rewritten here, so one lost later is
/// still caught by the reads (round-3 review MINOR-3).
fn set_range_settling(port: u16, range: std::ops::Range<usize>) -> (usize, usize) {
    let mut left = set_range_refused(port, range);
    let first = left.len();
    let mut c = Conn::open(port);
    for _ in 0..100 {
        if left.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
        left.retain(|i| {
            !c.send(&["SET", &format!("k:{i}"), &value(*i)])
                .starts_with('+')
        });
    }
    (first, left.len())
}

fn info_u64(c: &mut Conn, field: &str) -> u64 {
    let info = c.send(&["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO persistence has no {field}: {info}"))
}

fn wait_for(c: &mut Conn, what: &str, secs: u64, mut ok: impl FnMut(&mut Conn) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok(c) {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// GET every `k:i` in `range`: `Some(true)` right value, `Some(false)` a
/// WRONG value, `None` nil.
fn read_all(port: u16, range: std::ops::Range<usize>) -> Vec<Option<bool>> {
    let mut c = Conn::open(port);
    range
        .map(|i| {
            let r = c.send(&["GET", &format!("k:{i}")]);
            if r.starts_with("$-1") {
                None
            } else {
                Some(r.split("\r\n").nth(1) == Some(value(i).as_str()))
            }
        })
        .collect()
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_spill_thread_panic_mid_flight_is_survived() {
    let dir = common::unique_test_dir("spill-sup-1265-once");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = start(&dir);
    let mut c = Conn::open(port);
    assert_eq!(set_range(port, 0..FILL), 0, "no -OOM while spilling");
    wait_for(&mut c, "spilling", 30, |c| {
        info_u64(c, "spill_batches_flushed") > 0
    });
    let restarts0 = info_u64(&mut c, "spill_thread_restarts");

    // The next flush dies after writing its file: keys in flight, and the
    // DELs right behind the writes retire some of them in flight.
    std::fs::write(panic_file(&dir), "after-write once").unwrap();
    let second = FILL..FILL + 4_000;
    let deleted: Vec<usize> = second.clone().filter(|i| i % 3 == 0).collect();
    // Writes that land while the thread is down may be refused with -OOM
    // (the dead thread's payloads are pinned in RAM until the reconcile):
    // an acknowledged write is what must survive, so retry the refused ones
    // before judging. Discarding the refusals here read as ~300-1,700 "lost"
    // keys whenever the burst overlapped the death (seen with two suites
    // sharing core 0; none of them had reached the AOF — never acked).
    let (refused2, still2) = set_range_settling(port, second.clone());
    assert_eq!(
        still2, 0,
        "second range still refused after the respawn settled ({refused2} refused at first)"
    );
    for chunk in deleted.chunks(50) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("k:{i}")).collect();
        let mut args = vec!["DEL"];
        args.extend(keys.iter().map(String::as_str));
        assert!(c.send(&args).starts_with(':'));
    }
    wait_for(&mut c, "the respawn", 30, |c| {
        info_u64(c, "spill_thread_restarts") > restarts0 && info_u64(c, "spill_thread_alive") == 1
    });
    assert!(!panic_file(&dir).exists(), "the injected panic fired");
    assert_eq!(info_u64(&mut c, "spill_thread_degraded"), 0);

    // Eviction spills again on the respawned thread.
    let batches = info_u64(&mut c, "spill_batches_flushed");
    let third = FILL + 4_000..FILL + 8_000;
    let (refused, still) = set_range_settling(port, third.clone());
    assert_eq!(
        still, 0,
        "writes still refused after the respawn settled ({refused} refused at first)"
    );
    wait_for(&mut c, "a post-respawn spill", 30, |c| {
        info_u64(c, "spill_batches_flushed") > batches
    });
    assert_eq!(info_u64(&mut c, "spill_thread_restarts"), restarts0 + 1);

    let all = 0..FILL + 8_000;
    let judge = |got: Vec<Option<bool>>, when: &str| {
        let mut wrong = Vec::new();
        for (i, g) in all.clone().zip(got) {
            let want_nil = deleted.binary_search(&i).is_ok();
            if (want_nil && g.is_some()) || (!want_nil && g != Some(true)) {
                wrong.push(format!("k:{i}={g:?}"));
            }
        }
        assert!(
            wrong.is_empty(),
            "{when}: {} key(s) wrong (first: {:?}); dir {}",
            wrong.len(),
            &wrong[..wrong.len().min(5)],
            dir.display()
        );
    };
    judge(read_all(port, all.clone()), "live");

    server.kill_now();
    common::wait_for_port_down(port);
    drop(c);
    let (_server2, port2) = start(&dir);
    judge(read_all(port2, all.clone()), "after SIGKILL + restart");
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    assert!(
        log.contains("injected spill-thread panic"),
        "the panic is logged"
    );
    for line in log.lines().filter(|l| l.contains("reconciled")) {
        eprintln!("{line}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_crash_loop_degrades_the_shard_and_keeps_serving() {
    let dir = common::unique_test_dir("spill-sup-1265-loop");
    std::fs::create_dir_all(&dir).unwrap();
    let (_server, port) = start(&dir);
    let mut c = Conn::open(port);
    assert_eq!(set_range(port, 0..FILL), 0);
    wait_for(&mut c, "spilling", 30, |c| {
        info_u64(c, "spill_batches_flushed") > 0
    });
    let restarts0 = info_u64(&mut c, "spill_thread_restarts");

    // Every flush panics from now on.
    std::fs::write(panic_file(&dir), "after-write").unwrap();
    let mut next = FILL;
    let deadline = Instant::now() + Duration::from_secs(90);
    while info_u64(&mut c, "spill_thread_degraded") < shards() as u64 {
        assert!(Instant::now() < deadline, "never degraded");
        let _ = set_range(port, next..next + 500);
        next += 500;
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        info_u64(&mut c, "spill_thread_restarts") - restarts0,
        5 * shards() as u64,
        "five respawns per shard, then degraded"
    );
    assert_eq!(info_u64(&mut c, "spill_thread_alive"), 0);
    std::fs::remove_file(panic_file(&dir)).unwrap();

    // Degraded: the evicting policy drops, writes are not refused.
    let batches = info_u64(&mut c, "spill_batches_flushed");
    assert_eq!(
        set_range(port, next..next + 4_000),
        0,
        "no -OOM when degraded"
    );
    next += 4_000;
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        info_u64(&mut c, "spill_batches_flushed"),
        batches,
        "a degraded shard spills nothing"
    );

    let got = read_all(port, 0..next);
    let wrong: Vec<usize> = (0..next).filter(|&i| got[i] == Some(false)).collect();
    assert!(
        wrong.is_empty(),
        "wrong values: {:?}",
        &wrong[..wrong.len().min(5)]
    );
    let early_readable = got[..FILL].iter().filter(|g| **g == Some(true)).count();
    assert!(
        early_readable > 0,
        "keys spilled before the crash loop stay readable from their cold files"
    );
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    assert!(log.contains("DEGRADED"), "the degraded mode is announced");
    for line in log
        .lines()
        .filter(|l| l.contains("reconciled") || l.contains("DEGRADED"))
    {
        eprintln!("{line}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
