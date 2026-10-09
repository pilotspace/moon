//! moon#1295, end to end: an in-place write (HSET/LPUSH/SADD/ZADD) to a large
//! collection a running BGSAVE has not written yet must not deep-copy the
//! collection on the shard thread. Its epoch-start image is streamed into the
//! snapshot instead, the write waiting meanwhile, and the snapshot restores
//! every key at its epoch-start value.
//!
//! The save is HELD mid-walk (`MOON_TEST_SNAPSHOT_HOLD_FILE`) so the write
//! lands in the epoch by construction. `MOON_TEST_COW_STREAM_MIN_ELEMENTS` /
//! `MOON_TEST_COW_STREAM_TICK_BYTES` (test-only) lower the stream threshold
//! and stretch a stream over many ticks for the correctness cases.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --release --test
//! cow_stream_1295 -- --include-ignored --test-threads 1`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

struct DirGuard(PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn hold_file(dir: &Path) -> PathBuf {
    dir.join("snapshot.hold")
}

fn spawn(dir: &Path, shards: usize, env: &[(&str, &str)]) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let mut cmd = std::process::Command::new(&bin);
        cmd.env("MOON_TEST_SNAPSHOT_HOLD_FILE", hold_file(dir))
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .env("RUST_LOG", "moon=warn");
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
            "--save",
            "",
            "--disk-offload",
            "disable",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(common::server_stderr(dir))
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN)")
    });
    let guard = ServerGuard::new(child);
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["PING"]).starts_with("+PONG")
        {
            break;
        }
        assert!(Instant::now() < deadline, "server never answered PING");
        std::thread::sleep(Duration::from_millis(50));
    }
    (guard, port)
}

fn info_field(c: &mut Conn, field: &str) -> Option<String> {
    c.send(&["INFO", "persistence"]).lines().find_map(|l| {
        l.strip_prefix(&format!("{field}:"))
            .map(|v| v.trim().to_string())
    })
}

fn int(reply: &str) -> i64 {
    reply
        .trim()
        .strip_prefix(':')
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("not an integer reply: {reply:?}"))
}

/// Pipeline `cmds` in batches of 50, asserting no error reply.
fn pipeline_all(c: &mut Conn, cmds: impl Iterator<Item = Vec<String>>) {
    let mut out = Vec::new();
    let mut n = 0;
    let flush = |c: &mut Conn, out: &mut Vec<u8>, n: &mut usize| {
        if *n == 0 {
            return;
        }
        c.sock.write_all(out).unwrap();
        let reply = c.read_replies_within(*n, Duration::from_secs(120));
        assert!(
            !reply.contains("\r\n-") && !reply.starts_with('-'),
            "refused: {reply:.200}"
        );
        out.clear();
        *n = 0;
    };
    for cmd in cmds {
        let parts: Vec<&str> = cmd.iter().map(String::as_str).collect();
        out.extend_from_slice(&encode(&parts));
        n += 1;
        if n == 50 {
            flush(c, &mut out, &mut n);
        }
    }
    flush(c, &mut out, &mut n);
}

/// `cmd key` followed by `n` elements, `per` to a command.
fn bulk(cmd: &str, key: &str, n: usize, per: usize) -> impl Iterator<Item = Vec<String>> {
    let (cmd, key) = (cmd.to_string(), key.to_string());
    (0..n).step_by(per).map(move |from| {
        let mut parts = vec![cmd.clone(), key.clone()];
        for i in from..(from + per).min(n) {
            match cmd.as_str() {
                "HSET" => parts.extend([format!("f{i}"), format!("v{i}")]),
                "ZADD" => parts.extend([format!("{i}"), format!("m{i}")]),
                _ => parts.push(format!("e{i}")),
            }
        }
        parts
    })
}

fn start_held_bgsave(c: &mut Conn, dir: &Path) {
    std::fs::File::create(hold_file(dir)).unwrap();
    assert!(c.send(&["BGSAVE"]).starts_with('+'));
    let deadline = Instant::now() + Duration::from_secs(30);
    while info_field(c, "rdb_bgsave_in_progress").as_deref() != Some("1") {
        assert!(Instant::now() < deadline, "BGSAVE never started");
        std::thread::sleep(Duration::from_millis(10));
    }
    // Every shard armed its epoch.
    std::thread::sleep(Duration::from_millis(200));
}

fn release_and_wait(c: &mut Conn, dir: &Path) {
    std::fs::remove_file(hold_file(dir)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(300);
    while info_field(c, "rdb_bgsave_in_progress").as_deref() != Some("0") {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        info_field(c, "rdb_last_bgsave_status").as_deref(),
        Some("ok")
    );
}

fn rss_kb(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmRSS:"))
                .and_then(|v| v.split_whitespace().next().and_then(|n| n.parse().ok()))
        })
        .unwrap_or(0)
}

/// The issue's harness: a big hash, the save held mid-walk, `HSET` of one
/// new field. Base (WS42 head, this box): HSET 1.17 s, max PING gap 1.26 s,
/// RSS +836 MB at 5M fields. `MOON_1295_FIELDS` overrides the size.
#[test]
#[ignore = "real server, ~5M-field load: run with --include-ignored"]
fn an_in_place_write_on_a_large_hash_during_a_save_does_not_stall_the_shard() {
    let fields: usize = std::env::var("MOON_1295_FIELDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);
    let dir = common::unique_test_dir("cow-stream-1295-gap");
    let _dir = DirGuard(dir.clone());
    let (mut server, port) = spawn(&dir, 1, &[]);
    let mut c = Conn::open(port);
    pipeline_all(&mut c, bulk("HSET", "big", fields, 1000));
    pipeline_all(
        &mut c,
        (0..20_000).map(|i| vec!["SET".into(), format!("fill:{i}"), "x".into()]),
    );
    assert_eq!(int(&c.send(&["HLEN", "big"])) as usize, fields);
    start_held_bgsave(&mut c, &dir);

    let stop = Arc::new(AtomicBool::new(false));
    let pinger = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut p = Conn::open(port);
            let mut gaps = Vec::new();
            let mut last = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                assert!(p.send(&["PING"]).starts_with("+PONG"));
                let now = Instant::now();
                gaps.push((now, now - last));
                last = now;
            }
            gaps
        })
    };
    let pid = server.id();
    let sampler = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut peak = 0;
            while !stop.load(Ordering::Relaxed) {
                peak = peak.max(rss_kb(pid));
                std::thread::sleep(Duration::from_millis(2));
            }
            peak
        })
    };
    std::thread::sleep(Duration::from_millis(300));
    let rss_before = rss_kb(pid);
    let w0 = Instant::now();
    let reply = c.send_within(&["HSET", "big", "newfield", "x"], Duration::from_secs(120));
    let w1 = Instant::now();
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    let gaps = pinger.join().unwrap();
    let rss_peak = sampler.join().unwrap().max(rss_before);
    assert_eq!(int(&reply), 1, "HSET of a new field");
    let window: Vec<Duration> = gaps
        .iter()
        .filter(|(t, _)| {
            *t + Duration::from_millis(50) >= w0 && *t <= w1 + Duration::from_millis(300)
        })
        .map(|(_, g)| *g)
        .collect();
    let max_gap = window.iter().max().copied().unwrap_or_default();
    let spike_mb = rss_peak.saturating_sub(rss_before) / 1024;
    eprintln!(
        "moon#1295 gap: fields={fields} hset_ms={} max_ping_gap_ms={} pings={} rss_before_mb={} \
         rss_spike_mb={spike_mb}",
        (w1 - w0).as_millis(),
        max_gap.as_millis(),
        window.len(),
        rss_before / 1024
    );
    let streamed: u64 = info_field(&mut c, "rdb_cow_streamed_keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(streamed >= 1, "the hash was not streamed (copied instead?)");
    // Base: 1.26 s / +836 MB at 5M. A tick streams 2-5 ms plus the hash's
    // re-skip (~2 ns a field: ~10 ms at 5M quiet, 20-35 ms on a loaded
    // 4-vCPU box); 150 ms at 5M leaves room for a loaded host.
    let gap_bound = Duration::from_millis(50 + fields as u64 / 50_000);
    assert!(
        max_gap < gap_bound,
        "the shard stalled {max_gap:?} during the HSET (bound {gap_bound:?})"
    );
    let spike_bound_mb = (fields as u64 * 40 / (1 << 20)).max(64);
    assert!(
        spike_mb < spike_bound_mb,
        "RSS rose {spike_mb} MB during the HSET: the hash was copied (bound {spike_bound_mb} MB)"
    );
    assert_eq!(c.send(&["HGET", "big", "newfield"]), "$1\r\nx\r\n");

    release_and_wait(&mut c, &dir);
    server.kill_now();
    drop(server);
    let (_server, port) = spawn(&dir, 1, &[]);
    let mut c = Conn::open(port);
    let deadline = Instant::now() + Duration::from_secs(300);
    while int(&c.send(&["HLEN", "big"])) == 0 {
        assert!(Instant::now() < deadline, "snapshot never loaded");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        int(&c.send(&["HLEN", "big"])) as usize,
        fields,
        "HLEN after restart"
    );
    assert_eq!(
        int(&c.send(&["HEXISTS", "big", "newfield"])),
        0,
        "the HSET came after the save began: not in its image"
    );
    for i in (0..fields).step_by((fields / 997).max(1)) {
        assert_eq!(
            c.send(&["HGET", "big", &format!("f{i}")]),
            format!("${}\r\nv{i}\r\n", format!("v{i}").len())
        );
    }
    assert_eq!(int(&c.send(&["DBSIZE"])), 20_001);
}

/// Every streamed kind, in two databases, written from several connections
/// while their streams run (stretched over many ticks), plus a removal and a
/// RENAME of keys that are streaming: the restored snapshot holds each key at
/// its epoch-start value. `--shards 1` (every write local, so it waits) and
/// `--shards 4` (routed writes fall back to the copy).
#[test]
#[ignore = "real server: run with --include-ignored"]
fn streamed_collections_restore_at_their_epoch_start_value() {
    for shards in [1usize, 4] {
        let dir = common::unique_test_dir(&format!("cow-stream-1295-kinds-s{shards}"));
        let _dir = DirGuard(dir.clone());
        let env = [
            ("MOON_TEST_COW_STREAM_MIN_ELEMENTS", "1000"),
            ("MOON_TEST_COW_STREAM_TICK_BYTES", "8192"),
        ];
        let (mut server, port) = spawn(&dir, shards, &env);
        let mut c = Conn::open(port);
        const N: usize = 20_000;
        pipeline_all(&mut c, bulk("HSET", "{t}h", N, 500));
        pipeline_all(&mut c, bulk("RPUSH", "{t}l", N, 500));
        pipeline_all(&mut c, bulk("SADD", "{t}s", N, 500));
        pipeline_all(&mut c, bulk("ZADD", "{t}z", N, 500));
        pipeline_all(&mut c, bulk("HSET", "{t}gone", N, 500));
        pipeline_all(&mut c, bulk("SADD", "{t}moved", N, 500));
        assert!(c.send(&["SELECT", "3"]).starts_with("+OK"));
        pipeline_all(&mut c, bulk("HSET", "h3", N, 500));
        assert!(c.send(&["SELECT", "0"]).starts_with("+OK"));
        pipeline_all(
            &mut c,
            (0..2_000).map(|i| vec!["SET".into(), format!("k{i}"), format!("{i}")]),
        );
        let digest_before = c.send(&["DEBUG", "DIGEST"]);
        start_held_bgsave(&mut c, &dir);

        let writes: Vec<(usize, Vec<&str>)> = vec![
            (0, vec!["HSET", "{t}h", "new", "x"]),
            (0, vec!["LPUSH", "{t}l", "head"]),
            (0, vec!["SADD", "{t}s", "new"]),
            (0, vec!["ZADD", "{t}z", "-5", "new"]),
            (3, vec!["HSET", "h3", "new", "x"]),
            (0, vec!["DEL", "{t}gone"]),
            (0, vec!["RENAME", "{t}moved", "{t}renamed"]),
            (0, vec!["HDEL", "{t}h", "f7"]),
        ];
        let handles: Vec<_> = writes
            .into_iter()
            .map(|(db, w)| {
                let w: Vec<String> = w.into_iter().map(String::from).collect();
                std::thread::spawn(move || {
                    let mut c = Conn::open(port);
                    assert!(c.send(&["SELECT", &db.to_string()]).starts_with("+OK"));
                    let parts: Vec<&str> = w.iter().map(String::as_str).collect();
                    let reply = c.send_within(&parts, Duration::from_secs(120));
                    assert!(!reply.starts_with('-'), "{parts:?} -> {reply}");
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Writes of other keys were never held up.
        assert_eq!(c.send(&["SET", "after", "1"]), "+OK\r\n");
        if shards == 1 {
            let streamed: u64 = info_field(&mut c, "rdb_cow_streamed_keys")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            assert!(
                streamed >= 5,
                "streamed {streamed} keys, expected every big one"
            );
        }
        release_and_wait(&mut c, &dir);
        server.kill_now();
        drop(server);
        let (_server, port) = spawn(&dir, shards, &[]);
        let mut c = Conn::open(port);
        let deadline = Instant::now() + Duration::from_secs(120);
        while int(&c.send(&["DBSIZE"])) == 0 {
            assert!(Instant::now() < deadline, "snapshot never loaded");
            std::thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(
            c.send(&["DEBUG", "DIGEST"]),
            digest_before,
            "--shards {shards}: the snapshot is not the epoch-start keyspace"
        );
        assert_eq!(int(&c.send(&["HLEN", "{t}h"])) as usize, N);
        assert_eq!(c.send(&["LINDEX", "{t}l", "0"]), "$2\r\ne0\r\n");
        assert_eq!(int(&c.send(&["EXISTS", "{t}gone", "{t}moved"])), 2);
        assert_eq!(int(&c.send(&["EXISTS", "after", "{t}renamed"])), 0);
    }
}

/// kill -9 while a key is streaming (its write waiting): the restart loads
/// the PREVIOUS snapshot, intact.
#[test]
#[ignore = "real server: run with --include-ignored"]
fn kill_during_a_streamed_key_keeps_the_previous_snapshot() {
    let dir = common::unique_test_dir("cow-stream-1295-kill");
    let _dir = DirGuard(dir.clone());
    // ~200K fields at 2 KiB a tick: the stream runs for seconds.
    let env = [
        ("MOON_TEST_COW_STREAM_MIN_ELEMENTS", "1000"),
        ("MOON_TEST_COW_STREAM_TICK_BYTES", "2048"),
    ];
    let (mut server, port) = spawn(&dir, 1, &env);
    let mut c = Conn::open(port);
    const N: usize = 200_000;
    pipeline_all(&mut c, bulk("HSET", "big", N, 1000));
    assert_eq!(c.send(&["SET", "marker", "1"]), "+OK\r\n");
    assert!(c.send(&["BGSAVE"]).starts_with('+'));
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        std::thread::sleep(Duration::from_millis(20));
        if info_field(&mut c, "rdb_bgsave_in_progress").as_deref() == Some("0") {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(
        info_field(&mut c, "rdb_last_bgsave_status").as_deref(),
        Some("ok")
    );
    assert_eq!(c.send(&["SET", "marker", "2"]), "+OK\r\n");

    start_held_bgsave(&mut c, &dir);
    let writer = std::thread::spawn(move || {
        let mut w = Conn::open(port);
        // The kill ends it; the reply never comes.
        let _ = w.sock.write_all(&encode(&["HSET", "big", "new", "x"]));
        std::thread::sleep(Duration::from_secs(30));
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    while info_field(&mut c, "rdb_cow_stream_waits").as_deref() != Some("1") {
        assert!(
            Instant::now() < deadline,
            "the HSET never waited (exactly once) for a stream: {:?}",
            c.send(&["INFO", "persistence"])
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        info_field(&mut c, "rdb_cow_streamed_keys").as_deref(),
        Some("0"),
        "still streaming"
    );
    assert_eq!(
        c.send(&["HEXISTS", "big", "new"]),
        ":0\r\n",
        "the write is waiting"
    );
    server.kill_now();
    drop(server);
    drop(writer);
    let _ = std::fs::remove_file(hold_file(&dir));
    let (_server, port) = spawn(&dir, 1, &[]);
    let mut c = Conn::open(port);
    let deadline = Instant::now() + Duration::from_secs(60);
    while int(&c.send(&["DBSIZE"])) == 0 {
        assert!(Instant::now() < deadline, "snapshot never loaded");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        c.send(&["GET", "marker"]),
        "$1\r\n1\r\n",
        "the previous snapshot"
    );
    assert_eq!(int(&c.send(&["HLEN", "big"])) as usize, N);
    assert_eq!(int(&c.send(&["HEXISTS", "big", "new"])), 0);
}

/// moon#1300 with a large key: an open TXN holds it when the save starts;
/// the save streams its PRE-transaction image from the hold, the commit
/// mid-stream hands the image over by move, and the snapshot restores it.
#[test]
#[ignore = "real server: run with --include-ignored"]
fn a_large_key_held_by_an_open_txn_streams_its_pre_transaction_image() {
    for end in ["COMMIT", "ABORT"] {
        let dir = common::unique_test_dir(&format!("cow-stream-1295-txn-{end}"));
        let _dir = DirGuard(dir.clone());
        let env = [
            ("MOON_TEST_COW_STREAM_MIN_ELEMENTS", "1000"),
            ("MOON_TEST_COW_STREAM_TICK_BYTES", "4096"),
        ];
        let (mut server, port) = spawn(&dir, 1, &env);
        let mut c = Conn::open(port);
        const N: usize = 50_000;
        pipeline_all(&mut c, bulk("HSET", "big", N, 1000));
        let mut t = Conn::open(port);
        assert_eq!(t.send(&["TXN", "BEGIN"]), "+OK\r\n");
        assert_eq!(t.send(&["HSET", "big", "uncommitted", "1"]), ":1\r\n");
        start_held_bgsave(&mut c, &dir);
        assert_eq!(t.send(&["HSET", "big", "uncommitted2", "1"]), ":1\r\n");
        assert_eq!(t.send(&["TXN", end]), "+OK\r\n");
        release_and_wait(&mut c, &dir);
        assert!(
            info_field(&mut c, "rdb_cow_streamed_keys")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|n| n >= 1),
            "the held image was copied, not streamed"
        );
        server.kill_now();
        drop(server);
        let (_server, port) = spawn(&dir, 1, &[]);
        let mut c = Conn::open(port);
        let deadline = Instant::now() + Duration::from_secs(60);
        while int(&c.send(&["DBSIZE"])) == 0 {
            assert!(Instant::now() < deadline, "snapshot never loaded");
            std::thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(int(&c.send(&["HLEN", "big"])) as usize, N, "TXN {end}");
        assert_eq!(
            int(&c.send(&["HEXISTS", "big", "uncommitted"])),
            0,
            "TXN {end}"
        );
    }
}

/// A MULTI body cannot wait once it runs: EXEC waits for the streams of the
/// large collections its writes touch first (no copy), then runs.
#[test]
#[ignore = "real server: run with --include-ignored"]
fn exec_waits_for_the_streams_of_its_body_before_it_runs() {
    let dir = common::unique_test_dir("cow-stream-1295-exec");
    let _dir = DirGuard(dir.clone());
    let env = [
        ("MOON_TEST_COW_STREAM_MIN_ELEMENTS", "1000"),
        ("MOON_TEST_COW_STREAM_TICK_BYTES", "8192"),
    ];
    let (mut server, port) = spawn(&dir, 1, &env);
    let mut c = Conn::open(port);
    const N: usize = 30_000;
    pipeline_all(&mut c, bulk("HSET", "a", N, 1000));
    assert!(c.send(&["SELECT", "2"]).starts_with("+OK"));
    pipeline_all(&mut c, bulk("RPUSH", "b", N, 1000));
    assert!(c.send(&["SELECT", "0"]).starts_with("+OK"));
    let digest_before = c.send(&["DEBUG", "DIGEST"]);
    start_held_bgsave(&mut c, &dir);
    let reply = c.pipeline(&[
        &["MULTI"],
        &["HSET", "a", "new", "x"],
        &["SELECT", "2"],
        &["LPUSH", "b", "head"],
        &["EXEC"],
    ]);
    assert!(reply.ends_with("*3\r\n:1\r\n+OK\r\n:30001\r\n"), "{reply}");
    let streamed: u64 = info_field(&mut c, "rdb_cow_streamed_keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(streamed, 2, "both large values streamed, neither copied");
    assert!(c.send(&["SELECT", "0"]).starts_with("+OK"));
    release_and_wait(&mut c, &dir);
    server.kill_now();
    drop(server);
    let (_server, port) = spawn(&dir, 1, &[]);
    let mut c = Conn::open(port);
    let deadline = Instant::now() + Duration::from_secs(60);
    while int(&c.send(&["DBSIZE"])) == 0 {
        assert!(Instant::now() < deadline, "snapshot never loaded");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(c.send(&["DEBUG", "DIGEST"]), digest_before);
}
