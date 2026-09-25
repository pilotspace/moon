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
                "yes",
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
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon(port, dir, sweep_secs);
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
    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect for filler");
    stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
    let val = "F".repeat(FILLER_VALUE_LEN);
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
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
    while replies < FILLER_COUNT {
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
