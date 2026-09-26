//! A cold key removed BEFORE an AOF rewrite must stay removed across a kill -9.
//!
//! `crash_recovery_cold_del_resurrection` proves the replayed DEL/FLUSH reaches
//! the cold plane. This suite asks what happens once a rewrite has DISCARDED
//! that record: the new generation's base is hot-only, its incr opens with a
//! `MOON.COLDCUT` that authorizes every pre-rewrite cold file, and recovery
//! rebuilds the cold index from every manifest-Active file. A deleted key
//! whose spill file still backs live neighbours (so the orphan sweep never
//! retires it) has nothing durable left that remembers the delete.
//!
//! Scenario per test: SET probes -> filler evicts them cold -> mutate the EVEN
//! probes (DEL / overwrite / FLUSHDB / TTL expiry) -> optionally BGREWRITEAOF
//! and wait until every shard has a compacted base -> SIGKILL -> restart ->
//! every even probe must read its post-mutation state and every odd probe
//! (the live neighbours) its original value.
//!
//! Status on main 843611f5 and v0.8.9 (macOS, monoio/kqueue, release-fast):
//! RED  deleted_*_and_crash, deleted_*_and_clean_restart,
//!      deleted_*_after_the_orphan_sweep, flushed_*_and_crash (sweep held off)
//! ok   control_*_without_rewrite, overwritten_*, expired_*,
//!      flushed_*_after_the_orphan_sweep
//!
//! moon#1231 (promote-then-sweep) lives at the end of the file: a probe cold
//! at the rewrite, then read back into RAM (GET) or read-modify-written
//! (APPEND) after its spill file's other keys were deleted, must survive the
//! orphan sweep and a kill -9. RED on `ae21476` and on int/part3b (`5b14b82`).
//!
//! WS20 (after it): keyspace-level operations on cold keys — MOVE / COPY … DB
//! (moon#1254), a TTL'd overwrite of a cold key (moon#1236), SWAPDB around
//! cold keys (moon#1237) and, under `--appendonly no`, a read-promoted
//! inherited cold key and the orphan sweep (moon#1260). RED on `ae21476` and
//! on `4a96cd5f`.
//!
//! Run with (monoio default):
//!   cargo build --release
//!   MOON_BIN=target/release/moon cargo test --release \
//!     --test crash_recovery_cold_del_rewrite -- --ignored --test-threads 1
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const PROBE_COUNT: usize = 200;
const PROBE_VALUE_LEN: usize = 500;
const FILLER_COUNT: usize = 16_000;
const FILLER_VALUE_LEN: usize = 600;
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
/// `--shards` for every server this suite starts: 4 (the per-shard fold) by
/// default; `MOON_TEST_COLD_DEL_SHARDS=1` runs the single-shard layouts — the
/// TopLevel manifest fold on monoio, the legacy flat-file fold on tokio.
fn shards() -> usize {
    std::env::var("MOON_TEST_COLD_DEL_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(4)
}
const SETTLE_AFTER_FILLER: u64 = 8;
const SETTLE_AFTER_MUTATION: u64 = 3;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-cold-del-rw-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path, sweep_secs: u64) -> common::ServerGuard {
    start_moon_with(port, dir, sweep_secs, "yes", &[])
}

/// [`start_moon`] with the `--appendonly` setting and extra arguments given
/// (WS20: the `--appendonly no` + `--save` quadrant of moon#1260).
fn start_moon_with(
    port: u16,
    dir: &std::path::Path,
    sweep_secs: u64,
    appendonly: &str,
    extra: &[&str],
) -> common::ServerGuard {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    common::ServerGuard::new(
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards().to_string(),
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off_dir.to_str().expect("off dir utf8"),
                // `yes` is the bug surface: KV writes (incl. the DELs) are durably
                // logged in the per-shard AOF and replayed at boot. Under
                // `--appendonly no` the WAL writer is skipped entirely
                // ("WAL skipped (appendonly=no)") — nothing replays, and cold
                // resurrection there is the documented no-AOF RPO, not this bug.
                "--appendonly",
                appendonly,
                // 3600 holds the pre-sweep window open (a fully-deleted file
                // stays manifest-Active); a short interval proves the sweep does
                // not help a file that still backs live neighbours.
                "--cold-orphan-sweep-interval-secs",
                &sweep_secs.to_string(),
                // The diskfull guard write-flags DEL/FLUSHALL too (documented
                // trade-off) and dev/CI machines routinely sit under 5% free —
                // with the guard active the DELs under test would be REJECTED
                // (MOONERR diskfull) and never reach the AOF, silently gutting
                // the test (redis-cli exits 0 on error replies).
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .args(extra)
            // Captured to a log file so a CI flake produces a real diagnostic
            // (never Stdio::null()).
            .stdout(
                std::fs::File::create(dir.join("moon.stdout.log")).expect("create moon stdout log"),
            )
            .stderr(
                std::fs::File::create(dir.join("moon.stderr.log")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon (run `cargo build --release` with default features first)"),
    )
}

fn wait_for_port(port: u16) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            std::thread::sleep(Duration::from_millis(200));
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("moon did not start within 8s on port {}", port);
}

/// See crash_recovery_disk_offload_no_aof.rs: SO_REUSEPORT makes a bind-probe
/// useless; poll until connect is REFUSED twice in a row.
fn wait_for_port_down(port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let mut consecutive_refused = 0;
    for _ in 0..120 {
        match std::net::TcpStream::connect_timeout(
            &addr.parse().expect("addr"),
            Duration::from_millis(100),
        ) {
            Ok(_) => {
                consecutive_refused = 0;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                consecutive_refused += 1;
                if consecutive_refused >= 2 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

const RESTART_ATTEMPTS: usize = 6;

/// Start moon, retrying the transient rebind EADDRINUSE self-shutdown race
/// (see crash_recovery_disk_offload_no_aof.rs for the full rationale).
fn start_moon_alive(port: u16, dir: &std::path::Path, sweep_secs: u64) -> common::ServerGuard {
    start_moon_alive_with(port, dir, sweep_secs, "yes", &[])
}

/// [`start_moon_alive`] with [`start_moon_with`]'s knobs.
fn start_moon_alive_with(
    port: u16,
    dir: &std::path::Path,
    sweep_secs: u64,
    appendonly: &str,
    extra: &[&str],
) -> common::ServerGuard {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon_with(port, dir, sweep_secs, appendonly, extra);
        let mut up = false;
        for _ in 0..80 {
            if let Ok(Some(_status)) = child.as_mut().try_wait() {
                break;
            }
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
                std::thread::sleep(Duration::from_millis(200));
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if up {
            return child;
        }
        child.kill_now();
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!(
        "moon failed to start+serve on port {} after {} attempts",
        port, RESTART_ATTEMPTS
    );
}

fn probe_key(i: usize) -> String {
    format!("probe:{}", i)
}

/// redis-cli exits 0 even when the server replies an error (MOONERR/ERR land
/// on stdout/stderr, not the exit status) — every mutation helper must check
/// the reply text too, or a server-side rejection (e.g. the diskfull guard)
/// silently guts the test.
fn assert_no_error_reply(op: &str, out: &std::process::Output) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && !stdout.contains("MOONERR") && !stdout.starts_with("ERR"),
        "redis-cli {} rejected by server: stdout={} stderr={}",
        op,
        stdout.trim(),
        stderr.trim()
    );
}

fn redis_set(port: u16, key: &str, value: &str) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", key, value])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli SET");
    assert_no_error_reply(&format!("SET {}", key), &out);
}

fn redis_get(port: u16, key: &str) -> Option<String> {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "GET", key])
        .output()
        .expect("redis-cli GET");
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() || s == "(nil)" {
        None
    } else {
        Some(s)
    }
}

/// Pipelined filler SETs to push memory past the offload threshold.
fn write_filler(port: u16) {
    write_filler_in_db(port, 0);
}

/// [`write_filler`] into database `db` (a `SELECT` first; its `+OK` is one
/// more reply line to drain).
fn write_filler_in_db(port: u16, db: usize) {
    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect for filler");
    stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
    let val = "F".repeat(FILLER_VALUE_LEN);
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let select = db.to_string();
    let mut expected_replies = FILLER_COUNT;
    if db != 0 {
        buf.extend_from_slice(
            format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", select.len(), select).as_bytes(),
        );
        expected_replies += 1;
    }
    for i in 0..FILLER_COUNT {
        let key = format!("filler:{}", i);
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            val.len(),
            val
        );
        buf.extend_from_slice(cmd.as_bytes());
        if buf.len() >= 64 * 1024 {
            stream.write_all(&buf).expect("filler write");
            buf.clear();
        }
    }
    if !buf.is_empty() {
        stream.write_all(&buf).expect("filler tail write");
    }
    stream.flush().ok();
    // Read every reply before the socket is dropped. Closing a socket with
    // unread replies in its receive queue sends an RST, and on Linux the
    // server then discards the pipelined commands it had not read yet:
    // measured on the ae21476 binary, 8,289 of 16,000 SETs landed and
    // nothing spilled, so every case failed its "no heap-*.mpf" precondition
    // before it tested anything (the suite was written on macOS, where the
    // close did not cost the tail). Each SET reply is one line.
    use std::io::Read;
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut replies = 0usize;
    let mut chunk = [0u8; 64 * 1024];
    while replies < expected_replies {
        let n = stream.read(&mut chunk).expect("filler replies");
        assert!(
            n > 0,
            "server closed the filler connection after {replies} replies"
        );
        replies += chunk[..n].iter().filter(|&&b| b == b'\n').count();
    }
}

fn count_heap_files(dir: &std::path::Path) -> usize {
    let off = dir.join("off");
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for entry in rd.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                {
                    *acc += 1;
                }
            }
        }
    }
    let mut acc = 0;
    walk(&off, &mut acc);
    acc
}

fn redis_cmd(port: u16, args: &[&str]) -> String {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli");
    assert_no_error_reply(&args.join(" "), &out);
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn probe_value() -> String {
    "P".repeat(PROBE_VALUE_LEN)
}

fn overwrite_value() -> String {
    "N".repeat(PROBE_VALUE_LEN)
}

/// Whether `dir` holds a base with seq > 1 (a completed rewrite).
fn has_compacted_base(dir: &std::path::Path) -> bool {
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

/// Number of AOF generations cut by a completed rewrite: one per shard dir
/// with a compacted base (PerShard), or one for a compacted TopLevel base,
/// or one for a legacy `appendonly.aof` rewritten with its RDB preamble
/// (tokio `--shards 1`).
fn shards_with_compacted_base(dir: &std::path::Path) -> usize {
    let aof_dir = dir.join("appendonlydir");
    let per_shard = std::fs::read_dir(&aof_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|s| s.path().is_dir() && has_compacted_base(&s.path()))
                .count()
        })
        .unwrap_or(0);
    let top_level = usize::from(has_compacted_base(&aof_dir));
    let flat = usize::from(
        std::fs::read(dir.join("appendonly.aof")).is_ok_and(|b| b.starts_with(b"MOON")),
    );
    per_shard + top_level + flat
}

/// BGREWRITEAOF and wait until every shard has cut a new generation and INFO
/// reports no rewrite in progress.
fn rewrite_and_wait(port: u16, dir: &std::path::Path) {
    let reply = redis_cmd(port, &["BGREWRITEAOF"]);
    assert!(!reply.is_empty(), "BGREWRITEAOF returned an empty reply");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = redis_cmd(port, &["INFO", "persistence"]);
        let idle = info.contains("aof_rewrite_in_progress:0");
        if idle && shards_with_compacted_base(dir) == shards() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "rewrite did not complete on all {} shards within 60s ({} done); INFO:\n{}",
            shards(),
            shards_with_compacted_base(dir),
            info
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// What an even probe must read after recovery.
#[derive(Clone, Copy)]
enum Expect {
    Absent,
    Overwritten,
    /// FLUSHDB removed every probe, odd ones included.
    AllAbsent,
}

/// How round 1 ends.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stop {
    Kill9,
    /// `SHUTDOWN` — a clean stop; the resurrection must not depend on a crash.
    Shutdown,
    /// kill -9 with a 1s orphan sweep that has run several times before the
    /// rewrite: the pre-sweep window is closed, so what survives is only what
    /// the sweep cannot retire.
    Kill9AfterSweep,
}

impl Stop {
    fn sweep_secs(self) -> u64 {
        match self {
            Stop::Kill9AfterSweep => 1,
            Stop::Kill9 | Stop::Shutdown => 3600,
        }
    }
}

fn run_scenario(
    suffix: &str,
    stop: Stop,
    ttl_ms: Option<u64>,
    mutate: impl Fn(u16),
    rewrite: bool,
    expect: Expect,
) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");

    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);

    let val = probe_value();
    for i in 0..PROBE_COUNT {
        match ttl_ms {
            // Only the even probes carry a TTL; odd ones are the live neighbours.
            Some(ms) if i % 2 == 0 => {
                redis_cmd(port, &["SET", &probe_key(i), &val, "PX", &ms.to_string()]);
            }
            _ => redis_set(port, &probe_key(i), &val),
        }
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
    let heap_files = count_heap_files(&dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill"
    );

    mutate(port);
    match expect {
        Expect::Absent | Expect::AllAbsent => assert!(
            redis_get(port, &probe_key(0)).is_none(),
            "precondition failed: probe:0 still readable on the live server"
        ),
        Expect::Overwritten => assert_eq!(
            redis_get(port, &probe_key(0)),
            Some(overwrite_value()),
            "precondition failed: overwrite not visible on the live server"
        ),
    }
    if stop == Stop::Kill9AfterSweep {
        std::thread::sleep(Duration::from_secs(5));
    }
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));

    if stop == Stop::Shutdown {
        // The connection drops on a successful SHUTDOWN, so the reply is not checked.
        let _ = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "SHUTDOWN"])
            .output();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    server.kill_now();
    wait_for_port_down(port);
    let mut server2 = start_moon_alive(port, &dir, stop.sweep_secs());

    let mut wrong = Vec::new();
    let mut neighbours_lost = 0usize;
    for i in 0..PROBE_COUNT {
        let got = redis_get(port, &probe_key(i));
        let even = i % 2 == 0;
        let ok = match (expect, even) {
            (Expect::AllAbsent, _) | (Expect::Absent, true) => got.is_none(),
            (Expect::Overwritten, true) => got.as_deref() == Some(overwrite_value().as_str()),
            (_, false) => {
                let ok = got.as_deref() == Some(val.as_str());
                if !ok {
                    neighbours_lost += 1;
                }
                ok
            }
        };
        if !ok {
            wrong.push(probe_key(i));
        }
    }
    server2.kill_now();
    if wrong.is_empty() && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }
    assert!(
        wrong.is_empty(),
        "{} probe(s) recovered the wrong state after {}restart (neighbours lost: {}, \
         first: {:?}, heap files at mutation: {})",
        wrong.len(),
        if rewrite { "BGREWRITEAOF + " } else { "" },
        neighbours_lost,
        wrong.first(),
        heap_files
    );
}

fn del_even(port: u16) {
    for chunk in (0..PROBE_COUNT)
        .filter(|i| i % 2 == 0)
        .collect::<Vec<_>>()
        .chunks(50)
    {
        let keys: Vec<String> = chunk.iter().map(|i| probe_key(*i)).collect();
        let mut args = vec!["DEL"];
        args.extend(keys.iter().map(String::as_str));
        redis_cmd(port, &args);
    }
}

fn overwrite_even(port: u16) {
    let v = overwrite_value();
    for i in (0..PROBE_COUNT).filter(|i| i % 2 == 0) {
        redis_set(port, &probe_key(i), &v);
    }
}

fn flushdb(port: u16) {
    redis_cmd(port, &["FLUSHDB"]);
}

/// Control: no rewrite — the DEL records replay onto the cold plane.
#[test]
#[ignore] // requires a release moon + redis-cli; run with -- --ignored
fn control_deleted_cold_keys_stay_deleted_without_rewrite() {
    run_scenario(
        "del-norw",
        Stop::Kill9,
        None,
        del_even,
        false,
        Expect::Absent,
    );
}

/// The suspected resurrection: DEL, then a rewrite discards the DEL records.
#[test]
#[ignore]
fn deleted_cold_keys_stay_deleted_across_rewrite_and_crash() {
    run_scenario("del-rw", Stop::Kill9, None, del_even, true, Expect::Absent);
}

/// Overwrite instead of delete: the new value is hot and lands in the base.
#[test]
#[ignore]
fn overwritten_cold_keys_keep_the_new_value_across_rewrite_and_crash() {
    run_scenario(
        "ovw-rw",
        Stop::Kill9,
        None,
        overwrite_even,
        true,
        Expect::Overwritten,
    );
}

/// FLUSHDB while every probe is cold, then a rewrite.
#[test]
#[ignore]
fn flushed_cold_keys_stay_flushed_across_rewrite_and_crash() {
    run_scenario(
        "flush-rw",
        Stop::Kill9,
        None,
        flushdb,
        true,
        Expect::AllAbsent,
    );
}

/// The even probes expire while cold, then a rewrite. Their spill slots carry
/// the deadline, so they must read as absent after recovery.
#[test]
#[ignore]
fn expired_cold_keys_stay_expired_across_rewrite_and_crash() {
    // Long enough to be spilled live (filler + settle ~9s), short enough to
    // have expired before the rewrite.
    const TTL_MS: u64 = 15_000;
    run_scenario(
        "ttl-rw",
        Stop::Kill9,
        Some(TTL_MS),
        |_port| std::thread::sleep(Duration::from_millis(TTL_MS)),
        true,
        Expect::Absent,
    );
}

/// The same DEL + rewrite, ended by a clean SHUTDOWN instead of a crash.
#[test]
#[ignore]
fn deleted_cold_keys_stay_deleted_across_rewrite_and_clean_restart() {
    run_scenario(
        "del-rw-shutdown",
        Stop::Shutdown,
        None,
        del_even,
        true,
        Expect::Absent,
    );
}

/// DEL + rewrite with the orphan sweep given time to run first: a spill file
/// that still backs live neighbours is never retired, so the window is not a
/// crash window at all — it lasts as long as the neighbours live.
#[test]
#[ignore]
fn deleted_cold_keys_stay_deleted_across_rewrite_after_the_orphan_sweep() {
    run_scenario(
        "del-rw-swept",
        Stop::Kill9AfterSweep,
        None,
        del_even,
        true,
        Expect::Absent,
    );
}

/// FLUSHDB + rewrite once the orphan sweep has run: every flushed file lost
/// its last referrer and was retired, so nothing is left to resurrect. The
/// FLUSHDB exposure is bounded by `--cold-orphan-sweep-interval-secs`.
#[test]
#[ignore]
fn flushed_cold_keys_stay_flushed_across_rewrite_after_the_orphan_sweep() {
    run_scenario(
        "flush-rw-swept",
        Stop::Kill9AfterSweep,
        None,
        flushdb,
        true,
        Expect::AllAbsent,
    );
}

// ── moon#1231: promote-then-sweep ────────────────────────────────────────────
//
// A probe that is cold when BGREWRITEAOF cuts its base is not in that base:
// its spill slot is its only durable copy for the whole new generation. The
// fillers that share its spill files are then DELeted and the probe itself is
// read back into RAM (GET) or read-modify-written (APPEND) — neither leaves a
// record that recreates its value. Once every key of a file has left the cold
// index, the orphan sweep (1 s here) used to unlink it, and a kill -9 then
// lost the probe (GET) or replayed the APPEND onto nothing (APPEND). Reproduced
// 88/200 on `ae21476` by the WS15 probe script this scenario encodes.

/// How each probe is touched after the rewrite.
#[derive(Clone, Copy)]
enum Touch {
    /// `GET`: a read promotion, logged nowhere.
    Get,
    /// `APPEND probe +`: a read-modify-write promotion; only the APPEND is
    /// logged.
    Append,
}

fn appended_value() -> String {
    format!("{}+", probe_value())
}

/// Pipelined `DEL filler:i` for every filler, reading every reply (see
/// `write_filler` for why the replies must be drained).
fn del_fillers(port: u16) {
    let deleted = del_fillers_counting(port);
    assert!(
        deleted * 10 >= FILLER_COUNT * 9,
        "precondition failed: only {deleted} of {FILLER_COUNT} fillers existed to delete"
    );
}

/// [`del_fillers`] without its precondition: how many existed. Under
/// `--appendonly no` a restart keeps only the fillers that were cold.
fn del_fillers_counting(port: u16) -> usize {
    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect for DEL");
    stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    for i in 0..FILLER_COUNT {
        let key = format!("filler:{}", i);
        buf.extend_from_slice(
            format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key).as_bytes(),
        );
        if buf.len() >= 64 * 1024 {
            stream.write_all(&buf).expect("DEL write");
            buf.clear();
        }
    }
    if !buf.is_empty() {
        stream.write_all(&buf).expect("DEL tail write");
    }
    stream.flush().ok();
    use std::io::Read;
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut replies = 0usize;
    let mut deleted = 0usize;
    let mut chunk = [0u8; 64 * 1024];
    let mut pending: Vec<u8> = Vec::new();
    while replies < FILLER_COUNT {
        let n = stream.read(&mut chunk).expect("DEL replies");
        assert!(
            n > 0,
            "server closed the DEL connection after {replies} replies"
        );
        pending.extend_from_slice(&chunk[..n]);
        while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = pending.drain(..=pos).collect();
            assert!(
                line.starts_with(b":"),
                "DEL answered {:?}",
                String::from_utf8_lossy(&line)
            );
            if line.starts_with(b":1") {
                deleted += 1;
            }
            replies += 1;
        }
    }
    deleted
}

/// A numeric field of the default `INFO` (None until published).
fn info_u64(port: u16, field: &str) -> Option<u64> {
    let info = redis_cmd(port, &["INFO"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
}

/// moon#1231 scenario (see the section comment). With `second_rewrite`, a
/// second BGREWRITEAOF follows the touches: it captures the promoted probes in
/// its base, after which the sweep must release the spill files it held — the
/// hold is a wait for a fold, not a leak — and a kill -9 still loses nothing.
fn run_promote_scenario(suffix: &str, touch: Touch, second_rewrite: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let sweep_secs = 1;

    let mut server = start_moon(port, &dir, sweep_secs);
    wait_for_port(port);

    let val = probe_value();
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &val);
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
    let heap_files = count_heap_files(&dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill"
    );
    let cold_keys = info_u64(port, "cold_keys").unwrap_or(0);
    assert!(
        cold_keys > 0,
        "precondition failed: INFO reports no cold key after the filler"
    );

    // The fold: the probes that are cold now are in no base.
    rewrite_and_wait(port, &dir);
    del_fillers(port);
    let expected = match touch {
        Touch::Get => val.clone(),
        Touch::Append => appended_value(),
    };
    for i in 0..PROBE_COUNT {
        match touch {
            Touch::Get => assert_eq!(
                redis_get(port, &probe_key(i)).as_deref(),
                Some(val.as_str()),
                "precondition failed: {} unreadable on the live server",
                probe_key(i)
            ),
            Touch::Append => {
                let len = redis_cmd(port, &["APPEND", &probe_key(i), "+"]);
                assert_eq!(
                    len.trim_start_matches("(integer) "),
                    (PROBE_VALUE_LEN + 1).to_string(),
                    "precondition failed: APPEND {} did not see the cold value",
                    probe_key(i)
                );
            }
        }
    }
    // Several 1 s orphan sweeps run.
    std::thread::sleep(Duration::from_secs(5));
    let files_before_second = count_heap_files(&dir);

    let mut released = None;
    if second_rewrite {
        assert!(
            files_before_second > 0,
            "the spill files that backed the probes at the fold were unlinked before any \
             later fold captured them ({heap_files} at the fold, 0 now)"
        );
        rewrite_and_wait(port, &dir);
        std::thread::sleep(Duration::from_secs(4));
        released = Some(count_heap_files(&dir));
    }

    server.kill_now();
    wait_for_port_down(port);
    let mut server2 = start_moon_alive(port, &dir, sweep_secs);
    let mut lost = 0usize;
    let mut wrong = Vec::new();
    for i in 0..PROBE_COUNT {
        let got = redis_get(port, &probe_key(i));
        if got.as_deref() != Some(expected.as_str()) {
            if got.is_none() {
                lost += 1;
            }
            wrong.push((probe_key(i), got.map(|v| v.len())));
        }
    }
    server2.kill_now();
    if wrong.is_empty() && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }
    assert!(
        wrong.is_empty(),
        "{} of {} acknowledged, never-deleted probes recovered wrong after BGREWRITEAOF, \
         DEL of the fillers, {} of every probe, orphan sweeps{} and kill -9 ({} absent; \
         first: {:?}; heap files: {} at the fold, {} before the kill; cold keys at the \
         fold: {})",
        wrong.len(),
        PROBE_COUNT,
        match touch {
            Touch::Get => "GET",
            Touch::Append => "APPEND",
        },
        if second_rewrite {
            " + a second BGREWRITEAOF"
        } else {
            ""
        },
        lost,
        wrong.first(),
        heap_files,
        released.unwrap_or(files_before_second),
        cold_keys
    );
    if let Some(after) = released {
        assert!(
            after < files_before_second,
            "the second rewrite committed but the sweep released no held spill file \
             ({files_before_second} before, {after} after)"
        );
    }
}

/// moon#1231: read-promoted probes survive the orphan sweep and a kill -9.
#[test]
#[ignore]
fn promoted_cold_keys_survive_the_orphan_sweep_and_crash() {
    run_promote_scenario("promote-get", Touch::Get, false);
}

/// moon#1231: an APPEND logged after the promotion replays onto the value
/// the spill file still holds, not onto nothing.
#[test]
#[ignore]
fn appended_cold_keys_keep_their_value_across_the_orphan_sweep_and_crash() {
    run_promote_scenario("promote-append", Touch::Append, false);
}

/// moon#1231: the files the sweep held are released once a later rewrite has
/// captured the promoted probes, and a kill -9 after that still loses nothing.
#[test]
#[ignore]
fn held_spill_files_are_released_after_the_next_rewrite() {
    run_promote_scenario("promote-release", Touch::Get, true);
}

// ── WS20: keyspace-level operations on cold keys ─────────────────────────────
//
// moon#1254 (MOVE / COPY … DB n of a cold key), moon#1236 (a cold key
// overwritten hot WITH a TTL), moon#1237 (SWAPDB with cold keys) and moon#1260
// (`--appendonly no`: a cold key inherited from an AOF run, read back into
// RAM, then the orphan sweep). Same probes / filler / stop / restart shape
// as the cases above.

/// `redis-cli -n <db> <args>`, reply text checked for server errors.
fn redis_cmd_db(port: u16, db: usize, args: &[&str]) -> String {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "-n", &db.to_string()])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli");
    assert_no_error_reply(&args.join(" "), &out);
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn redis_get_db(port: u16, db: usize, key: &str) -> Option<String> {
    let s = redis_cmd_db(port, db, &["GET", key]);
    if s.is_empty() || s == "(nil)" {
        None
    } else {
        Some(s)
    }
}

fn integer_reply(s: &str) -> Option<i64> {
    s.trim_start_matches("(integer) ").parse().ok()
}

/// Spill the probes (and fillers) the way every case above does.
fn spill_probes(port: u16, dir: &std::path::Path) -> usize {
    let val = probe_value();
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &val);
    }
    write_filler(port);
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
    let heap_files = count_heap_files(dir);
    assert!(
        heap_files > 0,
        "precondition failed: no heap-*.mpf files — filler did not force a spill"
    );
    heap_files
}

/// Stop round 1 as `stop` says and bring the server back on the same data.
fn stop_and_restart(
    port: u16,
    dir: &std::path::Path,
    server: &mut common::ServerGuard,
    stop: Stop,
) -> common::ServerGuard {
    if stop == Stop::Shutdown {
        let _ = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "SHUTDOWN"])
            .output();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    server.kill_now();
    wait_for_port_down(port);
    start_moon_alive(port, dir, stop.sweep_secs())
}

fn finish(dir: &std::path::Path, wrong: &[String]) {
    if wrong.is_empty() && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }
}

// ── moon#1254 ────────────────────────────────────────────────────────────────

/// Even probes are MOVEd to db 1, odd probes COPYed to db 2 (`COPY k k DB 2`),
/// while most of them are cold. Every even probe must then live in db 1 only,
/// every odd one in db 0 and db 2 — on the live server, and after an optional
/// BGREWRITEAOF and a restart.
fn run_move_copy_scenario(suffix: &str, stop: Stop, rewrite: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);
    let heap_files = spill_probes(port, &dir);
    let val = probe_value();

    let mut refused = 0usize;
    for i in 0..PROBE_COUNT {
        let key = probe_key(i);
        let reply = if i % 2 == 0 {
            redis_cmd(port, &["MOVE", &key, "1"])
        } else {
            redis_cmd(port, &["COPY", &key, &key, "DB", "2"])
        };
        if integer_reply(&reply) != Some(1) {
            refused += 1;
        }
    }
    let check = |phase: &str| -> Vec<String> {
        let mut wrong = Vec::new();
        for i in 0..PROBE_COUNT {
            let key = probe_key(i);
            let (want0, want1, want2) = if i % 2 == 0 {
                (None, Some(val.as_str()), None)
            } else {
                (Some(val.as_str()), None, Some(val.as_str()))
            };
            let got = (
                redis_get_db(port, 0, &key),
                redis_get_db(port, 1, &key),
                redis_get_db(port, 2, &key),
            );
            if (got.0.as_deref(), got.1.as_deref(), got.2.as_deref()) != (want0, want1, want2) {
                wrong.push(format!(
                    "{phase} {key}: db0 {} db1 {} db2 {}",
                    got.0.is_some(),
                    got.1.is_some(),
                    got.2.is_some()
                ));
            }
        }
        wrong
    };
    let live_wrong = check("live");
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));
    let mut server2 = stop_and_restart(port, &dir, &mut server, stop);
    let mut wrong = live_wrong;
    wrong.extend(check("recovered"));
    server2.kill_now();
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty() && refused == 0,
        "MOVE/COPY … DB of cold probes: {refused} of {PROBE_COUNT} not answered :1, {} probe \
         states wrong (live + after {}restart; heap files at the move: {heap_files}); first: {:?}",
        wrong.len(),
        if rewrite { "BGREWRITEAOF + " } else { "" },
        wrong.first()
    );
}

/// moon#1254: MOVE / COPY … DB of spilled keys, then BGREWRITEAOF + kill -9.
#[test]
#[ignore]
fn moved_and_copied_cold_keys_keep_their_values_across_rewrite_and_crash() {
    run_move_copy_scenario("move-rw", Stop::Kill9, true);
}

/// moon#1254: the same without a rewrite (the MOVE records replay).
#[test]
#[ignore]
fn moved_and_copied_cold_keys_keep_their_values_across_crash() {
    run_move_copy_scenario("move-norw", Stop::Kill9, false);
}

// ── moon#1236 ────────────────────────────────────────────────────────────────

/// The even probes are overwritten hot WITH a TTL while cold (their old
/// value stays on disk as a shadow). Round 1 ends before the TTL passes; the
/// restart comes after it. Each even probe must read absent — never its OLD,
/// pre-overwrite value — and each odd probe its original value.
fn run_ttl_overwrite_scenario(suffix: &str, stop: Stop, rewrite: bool) {
    const TTL_MS: u64 = 20_000;
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);
    let heap_files = spill_probes(port, &dir);
    let val = probe_value();
    let new = overwrite_value();
    let overwritten_at = Instant::now();
    for i in (0..PROBE_COUNT).filter(|i| i % 2 == 0) {
        redis_cmd(
            port,
            &["SET", &probe_key(i), &new, "PX", &TTL_MS.to_string()],
        );
    }
    assert_eq!(
        redis_get(port, &probe_key(0)).as_deref(),
        Some(new.as_str()),
        "precondition failed: the overwrite is not visible on the live server"
    );
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));
    assert!(
        overwritten_at.elapsed() < Duration::from_millis(TTL_MS),
        "the TTL passed before round 1 ended; the case would not test the restart"
    );
    // Stop now; restart only once the TTL has passed (plus a margin).
    if stop == Stop::Shutdown {
        let _ = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "SHUTDOWN"])
            .output();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && server.as_mut().try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    server.kill_now();
    wait_for_port_down(port);
    let wait_until = Duration::from_millis(TTL_MS + 1_500);
    if let Some(rest) = wait_until.checked_sub(overwritten_at.elapsed()) {
        std::thread::sleep(rest);
    }
    let mut server2 = start_moon_alive(port, &dir, stop.sweep_secs());
    let mut wrong = Vec::new();
    let mut old_back = 0usize;
    for i in 0..PROBE_COUNT {
        let key = probe_key(i);
        let got = redis_get(port, &key);
        let ok = if i % 2 == 0 {
            if got.as_deref() == Some(val.as_str()) {
                old_back += 1;
            }
            got.is_none()
        } else {
            got.as_deref() == Some(val.as_str())
        };
        if !ok {
            wrong.push(key);
        }
    }
    server2.kill_now();
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty(),
        "{} probe(s) wrong after a TTL'd overwrite of cold keys and a {}restart past the TTL \
         ({old_back} even probes came back with their OLD value; heap files: {heap_files}); \
         first: {:?}",
        wrong.len(),
        if rewrite { "BGREWRITEAOF + " } else { "" },
        wrong.first()
    );
}

/// moon#1236: the reported shape — the rewrite puts the TTL'd value in the
/// base, the restart comes after the TTL.
#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_rewrite_and_crash() {
    run_ttl_overwrite_scenario("ttlow-rw", Stop::Kill9, true);
}

#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_rewrite_and_clean_restart() {
    run_ttl_overwrite_scenario("ttlow-rw-shutdown", Stop::Shutdown, true);
}

#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_crash() {
    run_ttl_overwrite_scenario("ttlow-norw", Stop::Kill9, false);
}

#[test]
#[ignore]
fn cold_keys_overwritten_with_a_ttl_do_not_come_back_old_across_clean_restart() {
    run_ttl_overwrite_scenario("ttlow-norw-shutdown", Stop::Shutdown, false);
}

// ── moon#1237 ────────────────────────────────────────────────────────────────

/// `redis-cli <args>` whose reply may legitimately be an error (returned as
/// text, not asserted).
fn redis_cmd_unchecked(port: u16, args: &[&str]) -> String {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Which SWAPDB shape a case runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SwapShape {
    /// The report: the probes are cold in db 0, then `SWAPDB 0 1`.
    ColdThenSwap,
    /// Found by WS20: `SWAPDB 0 1` while both dbs are empty, then the probes
    /// are written and spilled in db 1 (the post-swap db).
    SwapThenCold,
}

/// SWAPDB 0 1 around a cold tier, DEL of the even probes in the probes' db,
/// optional BGREWRITEAOF, kill -9, restart. A SWAPDB refused because a db
/// holds cold keys is an accepted answer (the probes then stay in db 0);
/// either way every odd probe must be in exactly its db and no deleted
/// probe may come back anywhere.
fn run_swapdb_scenario(suffix: &str, shape: SwapShape, rewrite: bool) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let stop = Stop::Kill9;
    let mut server = start_moon(port, &dir, stop.sweep_secs());
    wait_for_port(port);
    let val = probe_value();
    let (home, swap_reply, heap_files) = match shape {
        SwapShape::ColdThenSwap => {
            let heap_files = spill_probes(port, &dir);
            let reply = redis_cmd_unchecked(port, &["SWAPDB", "0", "1"]);
            let home = if reply == "OK" { 1 } else { 0 };
            (home, reply, heap_files)
        }
        SwapShape::SwapThenCold => {
            let reply = redis_cmd_unchecked(port, &["SWAPDB", "0", "1"]);
            assert_eq!(reply, "OK", "SWAPDB of two EMPTY dbs must succeed");
            for i in 0..PROBE_COUNT {
                redis_cmd_db(port, 1, &["SET", &probe_key(i), &val]);
            }
            write_filler_in_db(port, 1);
            std::thread::sleep(Duration::from_secs(SETTLE_AFTER_FILLER));
            let heap_files = count_heap_files(&dir);
            assert!(heap_files > 0, "precondition failed: nothing spilled");
            (1, reply, heap_files)
        }
    };
    for i in (0..PROBE_COUNT).filter(|i| i % 2 == 0) {
        redis_cmd_db(port, home, &["DEL", &probe_key(i)]);
    }
    if rewrite {
        rewrite_and_wait(port, &dir);
    }
    std::thread::sleep(Duration::from_secs(SETTLE_AFTER_MUTATION));
    let mut server2 = stop_and_restart(port, &dir, &mut server, stop);
    let other = 1 - home;
    let mut wrong = Vec::new();
    for i in 0..PROBE_COUNT {
        let key = probe_key(i);
        let in_home = redis_get_db(port, home, &key);
        let in_other = redis_get_db(port, other, &key);
        let ok = if i % 2 == 0 {
            in_home.is_none() && in_other.is_none()
        } else {
            in_home.as_deref() == Some(val.as_str()) && in_other.is_none()
        };
        if !ok {
            wrong.push(format!(
                "{key}: db{home} {} db{other} {}",
                in_home.is_some(),
                in_other.is_some()
            ));
        }
    }
    server2.kill_now();
    finish(&dir, &wrong);
    assert!(
        wrong.is_empty(),
        "{} probe(s) in the wrong db or back from the dead after SWAPDB ({:?}), DEL, {}kill -9 \
         and restart (home db {home}; heap files {heap_files}); first: {:?}",
        wrong.len(),
        swap_reply,
        if rewrite { "BGREWRITEAOF, " } else { "" },
        wrong.first()
    );
}

/// moon#1237 as reported: cold keys in db 0, SWAPDB 0 1, DEL, rewrite, crash.
#[test]
#[ignore]
fn swapdb_of_cold_keys_keeps_every_key_in_its_db_across_rewrite_and_crash() {
    run_swapdb_scenario("swap-cold-rw", SwapShape::ColdThenSwap, true);
}

/// The same without the rewrite (the SWAPDB record replays).
#[test]
#[ignore]
fn swapdb_of_cold_keys_keeps_every_key_in_its_db_across_crash() {
    run_swapdb_scenario("swap-cold-norw", SwapShape::ColdThenSwap, false);
}

/// SWAPDB of empty dbs, then spills in the swapped db, then a crash with no
/// rewrite: the replayed SWAPDB must not carry the later spills' cold entries
/// into the other db.
#[test]
#[ignore]
fn keys_spilled_after_a_swapdb_stay_in_their_db_across_crash() {
    run_swapdb_scenario("swap-then-cold-norw", SwapShape::SwapThenCold, false);
}

#[test]
#[ignore]
fn keys_spilled_after_a_swapdb_stay_in_their_db_across_rewrite_and_crash() {
    run_swapdb_scenario("swap-then-cold-rw", SwapShape::SwapThenCold, true);
}

// ── moon#1260 ────────────────────────────────────────────────────────────────

/// BGSAVE and wait until it completed successfully (LASTSAVE has 1 s
/// resolution, so the wait starts one second after the previous value).
fn bgsave_and_wait(port: u16) {
    let lastsave = || integer_reply(&redis_cmd(port, &["LASTSAVE"])).unwrap_or(0);
    let before = lastsave();
    std::thread::sleep(Duration::from_millis(1_100));
    let reply = redis_cmd(port, &["BGSAVE"]);
    assert!(!reply.is_empty(), "BGSAVE returned an empty reply");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = redis_cmd(port, &["INFO", "persistence"]);
        if info.contains("rdb_bgsave_in_progress:0") && lastsave() > before {
            assert!(
                info.contains("rdb_last_bgsave_status:ok"),
                "BGSAVE failed: {info}"
            );
            return;
        }
        assert!(Instant::now() < deadline, "BGSAVE never completed: {info}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// `GET probe:i` for every probe; returns how many read `val`.
fn probes_readable(port: u16, val: &str) -> usize {
    (0..PROBE_COUNT)
        .filter(|i| redis_get(port, &probe_key(*i)).as_deref() == Some(val))
        .count()
}

/// The REVIEW4-WS19 `r4_no_aof_promoted_…` proof, at `MOON_TEST_COLD_DEL_SHARDS`.
/// Phase 1 spills the probes under `--appendonly yes` and stops cleanly (under
/// `--appendonly no` the connection gate drops instead of spilling, so a
/// no-AOF server holds cold keys it inherited). Phase 2 runs the same dir
/// with `--appendonly no --save`: BGSAVE (the hot table), DEL the fillers,
/// GET every probe (read back into RAM, no log exists), let the orphan
/// sweep run (`sweep_secs`), kill -9. Snapshot semantics: every probe was
/// unchanged since the save, so every probe must come back.
fn run_no_aof_promote_scenario(suffix: &str, sweep_secs: u64) {
    let port = common::reserve_port();
    let dir = unique_dir(suffix);
    std::fs::create_dir_all(&dir).expect("create test dir");
    // Phase 1 (`--appendonly yes`), clean stop.
    {
        let mut s1 = start_moon(port, &dir, 3600);
        wait_for_port(port);
        spill_probes(port, &dir);
        let _ = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "SHUTDOWN", "NOSAVE"])
            .output();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && s1.as_mut().try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
        s1.kill_now();
        wait_for_port_down(port);
    }
    // Phase 2 (`--appendonly no` + save points, so BGSAVE has a directory).
    let save = ["--save", "3600 100000000"];
    let mut server = start_moon_with(port, &dir, sweep_secs, "no", &save);
    wait_for_port(port);
    redis_set(port, "hot:control", "H");
    bgsave_and_wait(port);
    let at_save = count_heap_files(&dir);
    assert!(
        del_fillers_counting(port) > 0,
        "precondition failed: no inherited filler to delete"
    );
    let val = probe_value();
    let readable = probes_readable(port, &val);
    assert!(
        readable > 0,
        "precondition failed: no probe readable in phase 2"
    );
    std::thread::sleep(Duration::from_secs(6)); // several orphan sweeps
    let before_kill = count_heap_files(&dir);
    server.kill_now();
    wait_for_port_down(port);
    let mut server2 = start_moon_alive_with(port, &dir, sweep_secs, "no", &save);
    let hot_ok = redis_get(port, "hot:control").as_deref() == Some("H");
    let back = probes_readable(port, &val);
    server2.kill_now();
    let wrong: Vec<String> = if back < readable || !hot_ok {
        vec![format!("{back} of {readable} back, hot control {hot_ok}")]
    } else {
        Vec::new()
    };
    finish(&dir, &wrong);
    assert!(hot_ok, "the snapshot was not loaded (hot control missing)");
    assert_eq!(
        readable - back.min(readable),
        0,
        "probes unchanged since the last BGSAVE lost after GET, orphan sweeps and kill -9 \
         under --appendonly no ({readable} readable before, {back} after; heap files {at_save} \
         at the save, {before_kill} before the kill)"
    );
}

/// moon#1260: GET-promoted inherited cold keys survive the sweep and a crash.
#[test]
#[ignore]
fn no_aof_promoted_cold_keys_survive_the_orphan_sweep_and_crash() {
    run_no_aof_promote_scenario("noaof-sweep", 1);
}

/// moon#1260 control: no sweep in the window, nothing is lost.
#[test]
#[ignore]
fn no_aof_promoted_cold_keys_survive_a_crash_without_a_sweep() {
    run_no_aof_promote_scenario("noaof-nosweep", 3600);
}
