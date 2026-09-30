//! moon#1283: an AOF replay judges key expiry against the shard clock the
//! record was WRITTEN under (the `MOON.TS <ms>` records the writer interleaves
//! with the log), not against the log file's modification time.
//!
//! Since moon#1277 the replay pinned its expiry judgment to the newest log
//! file's mtime. An mtime EARLIER than the last write (a clock stepped back, a
//! lagging network filesystem, a `touch -d` restore) made the replay behave
//! like expiry suppression: a key that expired while the server ran and was
//! then rewritten before its `DEL` was logged (moon#542 defers that `DEL`)
//! replayed onto its OLD value. An mtime LATER than the last write drifted the
//! judgment back to the wall clock: an RMW logged while its key was alive
//! replayed onto an absent key after the TTL passed during the downtime and
//! resurrected it as a persistent key (the moon#1277 shape).
//!
//! The `touchback` probe (REVIEW-FINAL-P5B, re-created here as a Rust test):
//! 40 keys in five classes whose correct verdict after a restart depends on
//! the write-time clock; the AOF files' mtimes are moved an hour back (and,
//! in the sibling test, forward) before the restart.
//!
//! | class | live history | correct after restart |
//! |---|---|---|
//! | `L` × 10 | `SET 5 PX 30`, expires, `INCR` → 1 | `1`, persistent |
//! | `E` × 10 | `SET old PX 30`, expires, `SET new NX` → OK | `new`, persistent |
//! | `B` × 10 | `SET v PX 1500`, `APPEND x` while alive, TTL passes while down | absent |
//! | `C` × 5 | `SET v PX 2h`, `APPEND x` | `vx` with a TTL |
//! | `P` × 5 | `SET v`, `APPEND x` | `vx`, persistent |
//!
//! Also here: a mixed old/new log (records without any `MOON.TS`, as an older
//! binary wrote them, followed by records with it), and a downgrade read (an
//! older binary replaying a `MOON.TS`-bearing log) gated on
//! `MOON_DOWNGRADE_BIN`.
//!
//! Pin the binary for a specific runtime:
//! `MOON_BIN=<moon> cargo test --test aof_replay_clock_1283 -- --include-ignored`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{Conn, ServerGuard};

const ARGS: [&str; 6] = [
    "--appendonly",
    "yes",
    "--appendfsync",
    "always",
    "--disk-offload",
    "enable",
];

/// TTL of the keys that expire while the server RUNS (classes `L`, `E`).
const SHORT_TTL_MS: u64 = 30;
/// TTL of the keys that expire while the server is DOWN (class `B`).
const DOWN_TTL_MS: u64 = 1_500;
/// One hour: how far the mtimes are moved.
const HOUR: Duration = Duration::from_secs(3_600);

fn spawn_bin(bin: &Path, dir: &Path, shards: usize) -> (ServerGuard, u16) {
    let d = dir.to_path_buf();
    let bin = bin.to_path_buf();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--disk-free-min-pct",
                "0",
            ])
            .args(ARGS)
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn sleep_until_ms(t: u64) {
    while now_ms() < t {
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn bulk(reply: &str) -> Option<String> {
    if reply.starts_with("$-1") {
        return None;
    }
    reply.split("\r\n").nth(1).map(str::to_string)
}

/// What a key must read as after the restart.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Want {
    /// This value, with no TTL.
    Persistent(&'static str),
    /// This value, with a TTL still running.
    Expiring(&'static str),
    Absent,
}

struct Workload {
    keys: Vec<(String, Want)>,
    /// The latest deadline of a key that must be gone after the restart.
    down_deadline_ms: u64,
}

fn assert_reply(c: &mut Conn, cmd: &[&str], want: &str) {
    let got = c.send(cmd);
    assert_eq!(got, want, "live reply to {cmd:?}");
}

/// Write the five key classes (see the module doc) under the prefix `tag`.
/// Every live reply is asserted, so the expected verdicts are what the live
/// server actually did.
fn write_workload(c: &mut Conn, tag: &str) -> Workload {
    let mut keys = Vec::new();
    for i in 0..5 {
        let p = format!("{tag}P:{i}");
        assert_reply(c, &["SET", &p, "v"], "+OK\r\n");
        assert_reply(c, &["APPEND", &p, "x"], ":2\r\n");
        keys.push((p, Want::Persistent("vx")));
        let k = format!("{tag}C:{i}");
        assert_reply(c, &["SET", &k, "v", "PX", "7200000"], "+OK\r\n");
        assert_reply(c, &["APPEND", &k, "x"], ":2\r\n");
        keys.push((k, Want::Expiring("vx")));
    }
    let down_deadline_ms = now_ms() + DOWN_TTL_MS;
    for i in 0..10 {
        let k = format!("{tag}B:{i}");
        assert_reply(
            c,
            &["SET", &k, "v", "PX", &DOWN_TTL_MS.to_string()],
            "+OK\r\n",
        );
        assert_reply(c, &["APPEND", &k, "x"], ":2\r\n");
        keys.push((k, Want::Absent));
    }
    // Classes L and E in five rounds of 2 + 2 keys. Each round sets its keys
    // in ONE pipeline, waits just past their deadline and rewrites them in
    // one pipeline, so a key is expired-but-unreaped for only a few ms. The
    // active expiry (every 100 ms) reaps a key in that window now and then
    // and logs its `DEL`, which makes that key's replay right under any
    // clock; spreading the rounds over different phases of the expiry cycle
    // keeps most keys unreaped, so a regressed replay cannot pass by luck.
    let ttl = SHORT_TTL_MS.to_string();
    for round in 0..5 {
        let (l0, l1) = (
            format!("{tag}L:{}", 2 * round),
            format!("{tag}L:{}", 2 * round + 1),
        );
        let (e0, e1) = (
            format!("{tag}E:{}", 2 * round),
            format!("{tag}E:{}", 2 * round + 1),
        );
        let sets: [&[&str]; 4] = [
            &["SET", &l0, "5", "PX", &ttl],
            &["SET", &l1, "5", "PX", &ttl],
            &["SET", &e0, "old", "PX", &ttl],
            &["SET", &e1, "old", "PX", &ttl],
        ];
        assert_eq!(c.pipeline(&sets), "+OK\r\n".repeat(4), "live SETs");
        // Past the deadline even when the server's cached clock lags.
        sleep_until_ms(now_ms() + SHORT_TTL_MS + 15);
        let rewrites: [&[&str]; 4] = [
            &["INCR", &l0],
            &["INCR", &l1],
            &["SET", &e0, "new", "NX"],
            &["SET", &e1, "new", "NX"],
        ];
        assert_eq!(
            c.pipeline(&rewrites),
            ":1\r\n:1\r\n+OK\r\n+OK\r\n",
            "live rewrites of expired keys"
        );
        for k in [l0, l1] {
            keys.push((k, Want::Persistent("1")));
        }
        for k in [e0, e1] {
            keys.push((k, Want::Persistent("new")));
        }
    }
    assert!(
        now_ms() < down_deadline_ms,
        "the setup outran the {DOWN_TTL_MS} ms TTL of the B keys: rerun on a quieter host"
    );
    Workload {
        keys,
        down_deadline_ms,
    }
}

/// Every key whose verdict after the restart is wrong: (key, want, TYPE, value, PTTL).
fn wrong_keys(c: &mut Conn, keys: &[(String, Want)]) -> Vec<(String, Want, String, String)> {
    let mut wrong = Vec::new();
    for (k, want) in keys {
        let value = bulk(&c.send(&["GET", k]));
        let pttl = c.send(&["PTTL", k]);
        let pttl: i64 = pttl
            .trim_start_matches(':')
            .trim_end()
            .parse()
            .unwrap_or(i64::MIN);
        let ok = match want {
            Want::Persistent(v) => value.as_deref() == Some(*v) && pttl == -1,
            Want::Expiring(v) => value.as_deref() == Some(*v) && pttl > 0,
            Want::Absent => value.is_none(),
        };
        if !ok {
            wrong.push((
                k.clone(),
                want.clone(),
                format!("{value:?}"),
                pttl.to_string(),
            ));
        }
    }
    wrong
}

/// Every regular file under `dir`, recursively.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(p),
                Ok(t) if t.is_file() => out.push(p),
                _ => {}
            }
        }
    }
    out
}

/// The files that hold the command log: the flat `appendonly.aof`, and every
/// file of the multi-part / per-shard `appendonlydir`.
fn aof_files(dir: &Path) -> Vec<PathBuf> {
    files_under(dir)
        .into_iter()
        .filter(|p| {
            let s = p.to_string_lossy();
            s.contains("appendonlydir") || s.ends_with("appendonly.aof")
        })
        .collect()
}

/// Move the mtime of every AOF file by `delta` (back when `back`).
fn shift_aof_mtimes(dir: &Path, delta: Duration, back: bool) -> usize {
    let files = aof_files(dir);
    for p in &files {
        let meta = std::fs::metadata(p).expect("stat an AOF file");
        let mtime = meta.modified().expect("mtime");
        let moved = if back { mtime - delta } else { mtime + delta };
        std::fs::File::options()
            .write(true)
            .open(p)
            .expect("open an AOF file")
            .set_modified(moved)
            .expect("set mtime");
    }
    assert!(!files.is_empty(), "no AOF file under {}", dir.display());
    files.len()
}

fn stop(server: &mut ServerGuard, port: u16) {
    server.kill_now();
    common::wait_for_port_down(port);
}

/// Whether `dir` holds an AOF base with seq > 1 (a completed rewrite).
fn has_compacted_base(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|files| {
        files.flatten().any(|f| {
            let name = f.file_name().to_string_lossy().to_string();
            name.strip_prefix("moon.aof.")
                .and_then(|r| r.strip_suffix(".base.rdb"))
                .and_then(|seq| seq.parse::<u64>().ok())
                .is_some_and(|seq| seq > 1)
        })
    })
}

/// Generations cut by a completed rewrite: per-shard dirs, the top-level
/// multi-part dir, or the tokio `--shards 1` flat file with its preamble.
fn compacted_bases(dir: &Path) -> usize {
    let aof_dir = dir.join("appendonlydir");
    let per_shard = std::fs::read_dir(&aof_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|s| s.path().is_dir() && has_compacted_base(&s.path()))
                .count()
        })
        .unwrap_or(0);
    let flat = std::fs::read(dir.join("appendonly.aof")).is_ok_and(|b| b.starts_with(b"MOON"));
    per_shard + usize::from(has_compacted_base(&aof_dir)) + usize::from(flat)
}

/// BGREWRITEAOF and wait until every generation is cut.
fn rewrite_and_wait(c: &mut Conn, dir: &Path, shards: usize) {
    assert!(!c.send(&["BGREWRITEAOF"]).starts_with('-'));
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let idle = c
            .send(&["INFO", "persistence"])
            .contains("aof_rewrite_in_progress:0");
        let done = compacted_bases(dir);
        if idle && (done == shards || done == 1) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "BGREWRITEAOF did not complete within 60s ({done} bases)"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The probe: write, kill -9, wait out the `B` TTLs, move every AOF mtime an
/// hour back (or forward), restart, and check all 40 verdicts. With
/// `rewrite`, a BGREWRITEAOF runs first, so the workload lands in a NEW
/// generation: its head's `MOON.TS` and the writer's reset record context
/// are what stamp it.
fn touch_probe(shards: usize, back: bool) {
    touch_probe_with(shards, back, false);
}

fn touch_probe_with(shards: usize, back: bool, rewrite: bool) {
    let bin = common::find_moon_binary();
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let (mut server, port) = spawn_bin(&bin, dir, shards);
    let mut c = Conn::open(port);
    if rewrite {
        for i in 0..20 {
            assert_reply(&mut c, &["SET", &format!("pre:{i}"), "v"], "+OK\r\n");
        }
        rewrite_and_wait(&mut c, dir, shards);
    }
    let mut work = write_workload(&mut c, "");
    if rewrite {
        work.keys
            .extend((0..20).map(|i| (format!("pre:{i}"), Want::Persistent("v"))));
    }
    drop(c);
    stop(&mut server, port);
    sleep_until_ms(work.down_deadline_ms + 300);
    shift_aof_mtimes(dir, HOUR, back);

    let (_server, port) = spawn_bin(&bin, dir, shards);
    let mut c = Conn::open(port);
    let wrong = wrong_keys(&mut c, &work.keys);
    assert!(
        wrong.is_empty(),
        "moon#1283 touch{} probe, --shards {shards}, rewrite {rewrite}, {}: {}/{} keys wrong \
         after moving the AOF mtimes one hour {} (key, want, GET, PTTL): {wrong:#?}",
        if back { "back" } else { "forward" },
        bin.display(),
        wrong.len(),
        work.keys.len(),
        if back { "back" } else { "forward" },
    );
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_touchback_probe_s1() {
    touch_probe(1, true);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_touchback_probe_s4() {
    touch_probe(4, true);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_touchback_probe_after_a_rewrite_s1() {
    touch_probe_with(1, true, true);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_touchback_probe_after_a_rewrite_s4() {
    touch_probe_with(4, true, true);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_touchforward_probe_s1() {
    touch_probe(1, false);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_touchforward_probe_s4() {
    touch_probe(4, false);
}

// ── mixed old/new log ────────────────────────────────────────────────────────

/// One RESP array of bulk strings at the start of `buf`: its first element
/// and its total encoded length. `None` when `buf` does not start with one.
fn resp_array(buf: &[u8]) -> Option<(&[u8], usize)> {
    fn line(buf: &[u8], at: usize) -> Option<(&[u8], usize)> {
        let rest = buf.get(at..)?;
        let end = rest.windows(2).position(|w| w == b"\r\n")?;
        Some((&rest[..end], at + end + 2))
    }
    let (head, mut at) = line(buf, 0)?;
    let n: usize = std::str::from_utf8(head.strip_prefix(b"*")?)
        .ok()?
        .parse()
        .ok()?;
    let mut first: &[u8] = &[];
    for i in 0..n {
        let (len, next) = line(buf, at)?;
        let len: usize = std::str::from_utf8(len.strip_prefix(b"$")?)
            .ok()?
            .parse()
            .ok()?;
        let body = buf.get(next..next + len)?;
        if i == 0 {
            first = body;
        }
        at = next + len + 2;
        if buf.len() < at {
            return None;
        }
    }
    Some((first, at))
}

/// Remove every `MOON.TS` record from an AOF file, leaving it exactly as a
/// binary that predates moon#1283 would have written it. Handles the framed
/// per-shard incr (`[u64 lsn][u32 len][RESP]`) and plain RESP; returns how
/// many records it dropped. A file in neither shape (a base RDB, the
/// manifest) is left alone.
fn strip_ts_records(path: &Path) -> usize {
    let data = std::fs::read(path).expect("read AOF file");
    let is_ts = |first: &[u8]| first.eq_ignore_ascii_case(b"MOON.TS");
    // Framed?
    let mut framed = Vec::with_capacity(data.len());
    let mut dropped = 0;
    let mut at = 0;
    let mut is_framed = !data.is_empty();
    while at < data.len() {
        let Some(hdr) = data.get(at..at + 12) else {
            is_framed = false;
            break;
        };
        let len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        let Some(payload) = data.get(at + 12..at + 12 + len) else {
            is_framed = false;
            break;
        };
        match resp_array(payload) {
            Some((first, used)) if used == len => {
                if is_ts(first) {
                    dropped += 1;
                } else {
                    framed.extend_from_slice(&data[at..at + 12 + len]);
                }
            }
            _ => {
                is_framed = false;
                break;
            }
        }
        at += 12 + len;
    }
    if is_framed {
        std::fs::write(path, framed).expect("rewrite framed AOF");
        return dropped;
    }
    // Plain RESP?
    let mut plain = Vec::with_capacity(data.len());
    let mut at = 0;
    dropped = 0;
    while at < data.len() {
        let Some((first, used)) = resp_array(&data[at..]) else {
            return 0; // not a RESP log: leave it alone
        };
        if is_ts(first) {
            dropped += 1;
        } else {
            plain.extend_from_slice(&data[at..at + used]);
        }
        at += used;
    }
    std::fs::write(path, plain).expect("rewrite RESP AOF");
    dropped
}

/// The first generation of the log is written with no `MOON.TS` at all — by
/// `MOON_OLD_BIN` when set (a binary that predates moon#1283), otherwise by
/// this binary with every `MOON.TS` stripped afterwards. A second boot of this
/// binary then appends records WITH `MOON.TS` (to the same file, or to a new
/// generation, whichever the layout does). After a touchback restart:
/// - the old records replay: their data is all there (their verdicts do not
///   depend on the judgment clock, which for them is the mtime fallback);
/// - the new records are judged by their own `MOON.TS`, so every verdict of
///   the second workload is right even though an old, stamp-less prefix
///   precedes it.
fn mixed_old_new(shards: usize) {
    let bin = common::find_moon_binary();
    let old_bin = std::env::var("MOON_OLD_BIN").ok().map(PathBuf::from);
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    let (mut server, port) = spawn_bin(old_bin.as_deref().unwrap_or(&bin), dir, shards);
    let mut c = Conn::open(port);
    let mut old_keys = Vec::new();
    for i in 0..10 {
        let p = format!("oldP:{i}");
        assert_reply(&mut c, &["SET", &p, "v"], "+OK\r\n");
        old_keys.push((p, Want::Persistent("v")));
        let k = format!("oldC:{i}");
        assert_reply(&mut c, &["SET", &k, "v", "PX", "7200000"], "+OK\r\n");
        old_keys.push((k, Want::Expiring("v")));
        // Continued by the new binary: order across the generations.
        assert_reply(&mut c, &["SET", &format!("M:{i}"), "10"], "+OK\r\n");
    }
    drop(c);
    stop(&mut server, port);
    if old_bin.is_none() {
        for p in aof_files(dir) {
            strip_ts_records(&p);
        }
    }
    let old_ts = aof_files(dir)
        .iter()
        .map(|p| count_ts(&std::fs::read(p).unwrap()))
        .sum::<usize>();
    assert_eq!(old_ts, 0, "the first generation must carry no MOON.TS");

    let (mut server, port) = spawn_bin(&bin, dir, shards);
    let mut c = Conn::open(port);
    for i in 0..10 {
        assert_reply(&mut c, &["INCR", &format!("M:{i}")], ":11\r\n");
    }
    let work = write_workload(&mut c, "new");
    drop(c);
    stop(&mut server, port);
    sleep_until_ms(work.down_deadline_ms + 300);
    shift_aof_mtimes(dir, HOUR, true);

    let (_server, port) = spawn_bin(&bin, dir, shards);
    let mut c = Conn::open(port);
    let mut keys = old_keys;
    keys.extend(work.keys);
    keys.extend((0..10).map(|i| (format!("M:{i}"), Want::Persistent("11"))));
    let wrong = wrong_keys(&mut c, &keys);
    assert!(
        wrong.is_empty(),
        "moon#1283 mixed old/new log, --shards {shards}, {} (old: {}): {}/{} keys wrong \
         (key, want, GET, PTTL): {wrong:#?}",
        bin.display(),
        old_bin
            .as_deref()
            .map_or("stripped".into(), |p| p.display().to_string()),
        wrong.len(),
        keys.len(),
    );
}

fn count_ts(data: &[u8]) -> usize {
    data.windows(b"MOON.TS".len())
        .filter(|w| w.eq_ignore_ascii_case(b"MOON.TS"))
        .count()
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_mixed_old_new_log_s1() {
    mixed_old_new(1);
}

#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_mixed_old_new_log_s4() {
    mixed_old_new(4);
}

/// The log this binary writes carries `MOON.TS` records: a fresh boot's head
/// and one before the first write of every clock tick.
#[test]
#[ignore = "real-server: run with --include-ignored and MOON_BIN pinned"]
fn moon_1283_the_log_carries_ts_records() {
    for shards in [1, 4] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        let (mut server, port) = spawn_bin(&common::find_moon_binary(), dir, shards);
        let mut c = Conn::open(port);
        for i in 0..20 {
            assert_reply(&mut c, &["SET", &format!("k{i}"), "v"], "+OK\r\n");
            std::thread::sleep(Duration::from_millis(3));
        }
        drop(c);
        stop(&mut server, port);
        let ts: usize = aof_files(dir)
            .iter()
            .map(|p| count_ts(&std::fs::read(p).unwrap()))
            .sum();
        // 20 writes 3 ms apart: at least one stamp per write tick.
        assert!(
            ts >= 20,
            "--shards {shards}: expected >= 20 MOON.TS records in the log, found {ts}"
        );
    }
}

// ── downgrade read ───────────────────────────────────────────────────────────

/// A `MOON.TS`-bearing log replayed by a binary that predates moon#1283
/// (`MOON_DOWNGRADE_BIN`): the old binary sends `MOON.TS` to dispatch, which
/// answers "unknown command"; the record is skipped, the boot succeeds and
/// every data record replays with the old (mtime) judgment. The workload is
/// left untouched here, so the mtime is the last write and every verdict is
/// the right one even under that judgment.
///
/// Skips (passes with a note) when `MOON_DOWNGRADE_BIN` is unset: CI has no
/// pre-#1283 binary. Run it by hand against one.
fn downgrade_read(shards: usize) {
    let Some(old) = std::env::var_os("MOON_DOWNGRADE_BIN").map(PathBuf::from) else {
        eprintln!("MOON_DOWNGRADE_BIN unset: downgrade-read test skipped");
        return;
    };
    let bin = common::find_moon_binary();
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let (mut server, port) = spawn_bin(&bin, dir, shards);
    let mut c = Conn::open(port);
    let work = write_workload(&mut c, "");
    drop(c);
    stop(&mut server, port);
    let ts: usize = aof_files(dir)
        .iter()
        .map(|p| count_ts(&std::fs::read(p).unwrap()))
        .sum();
    assert!(
        ts > 0,
        "the new binary wrote no MOON.TS: nothing to downgrade-read"
    );
    sleep_until_ms(work.down_deadline_ms + 300);

    let (_server, port) = spawn_bin(&old, dir, shards);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
    let wrong = wrong_keys(&mut c, &work.keys);
    assert!(
        wrong.is_empty(),
        "downgrade read by {}, --shards {shards}: {}/{} keys wrong (key, want, GET, PTTL): \
         {wrong:#?}",
        old.display(),
        wrong.len(),
        work.keys.len(),
    );
}

#[test]
#[ignore = "real-server: run with --include-ignored, MOON_BIN and MOON_DOWNGRADE_BIN pinned"]
fn moon_1283_downgrade_read_s1() {
    downgrade_read(1);
}

#[test]
#[ignore = "real-server: run with --include-ignored, MOON_BIN and MOON_DOWNGRADE_BIN pinned"]
fn moon_1283_downgrade_read_s4() {
    downgrade_read(4);
}
