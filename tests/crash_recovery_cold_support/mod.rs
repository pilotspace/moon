//! Shared harness of the cold-tier crash suites
//! (`crash_recovery_cold_del_rewrite`, `crash_recovery_cold_no_aof`): the
//! probe / filler / spill shape, server start and stop, and `redis-cli`
//! helpers. Split out of `crash_recovery_cold_del_rewrite.rs` to keep both
//! suites under the file-size cap.
//!
//! Not every helper is used by every suite that includes this module.
#![allow(dead_code)]

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::common;

pub const PROBE_COUNT: usize = 200;

pub const PROBE_VALUE_LEN: usize = 500;

pub const FILLER_COUNT: usize = 16_000;

pub const FILLER_VALUE_LEN: usize = 600;

pub const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;

/// `--shards` for every server this suite starts: 4 (the per-shard fold) by
/// default; `MOON_TEST_COLD_DEL_SHARDS=1` runs the single-shard layouts — the
/// TopLevel manifest fold on monoio, the legacy flat-file fold on tokio.
pub fn shards() -> usize {
    std::env::var("MOON_TEST_COLD_DEL_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(4)
}

pub const SETTLE_AFTER_FILLER: u64 = 8;

pub const SETTLE_AFTER_MUTATION: u64 = 3;

pub fn unique_dir(suffix: &str) -> std::path::PathBuf {
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

pub fn start_moon(port: u16, dir: &std::path::Path, sweep_secs: u64) -> common::ServerGuard {
    start_moon_with(port, dir, sweep_secs, "yes", &[])
}

/// [`start_moon`] with the `--appendonly` setting and extra arguments given
/// (WS20: the `--appendonly no` + `--save` quadrant of moon#1260).
pub fn start_moon_with(
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

pub fn wait_for_port(port: u16) {
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
pub fn wait_for_port_down(port: u16) {
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

pub const RESTART_ATTEMPTS: usize = 6;

/// Start moon, retrying the transient rebind EADDRINUSE self-shutdown race
/// (see crash_recovery_disk_offload_no_aof.rs for the full rationale).
pub fn start_moon_alive(port: u16, dir: &std::path::Path, sweep_secs: u64) -> common::ServerGuard {
    start_moon_alive_with(port, dir, sweep_secs, "yes", &[])
}

/// [`start_moon_alive`] with [`start_moon_with`]'s knobs.
pub fn start_moon_alive_with(
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

pub fn probe_key(i: usize) -> String {
    format!("probe:{}", i)
}

/// redis-cli exits 0 even when the server replies an error (MOONERR/ERR land
/// on stdout/stderr, not the exit status) — every mutation helper must check
/// the reply text too, or a server-side rejection (e.g. the diskfull guard)
/// silently guts the test.
pub fn assert_no_error_reply(op: &str, out: &std::process::Output) {
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

pub fn redis_set(port: u16, key: &str, value: &str) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", key, value])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli SET");
    assert_no_error_reply(&format!("SET {}", key), &out);
}

pub fn redis_get(port: u16, key: &str) -> Option<String> {
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
pub fn write_filler(port: u16) {
    write_filler_in_db(port, 0);
}

/// [`write_filler`] into database `db` (a `SELECT` first; its `+OK` is one
/// more reply line to drain).
pub fn write_filler_in_db(port: u16, db: usize) {
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

pub fn count_heap_files(dir: &std::path::Path) -> usize {
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

pub fn redis_cmd(port: u16, args: &[&str]) -> String {
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

pub fn probe_value() -> String {
    "P".repeat(PROBE_VALUE_LEN)
}

pub fn overwrite_value() -> String {
    "N".repeat(PROBE_VALUE_LEN)
}

/// Whether `dir` holds a base with seq > 1 (a completed rewrite).
pub fn has_compacted_base(dir: &std::path::Path) -> bool {
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
pub fn shards_with_compacted_base(dir: &std::path::Path) -> usize {
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
pub fn rewrite_and_wait(port: u16, dir: &std::path::Path) {
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

/// Pipelined `DEL filler:i` for every filler, reading every reply (see
/// `write_filler` for why the replies must be drained).
pub fn del_fillers(port: u16) {
    let deleted = del_fillers_counting(port);
    assert!(
        deleted * 10 >= FILLER_COUNT * 9,
        "precondition failed: only {deleted} of {FILLER_COUNT} fillers existed to delete"
    );
}

/// [`del_fillers`] without its precondition: how many existed. Under
/// `--appendonly no` a restart keeps only the fillers that were cold.
pub fn del_fillers_counting(port: u16) -> usize {
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
pub fn info_u64(port: u16, field: &str) -> Option<u64> {
    let info = redis_cmd(port, &["INFO"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
}

/// `redis-cli -n <db> <args>`, reply text checked for server errors.
pub fn redis_cmd_db(port: u16, db: usize, args: &[&str]) -> String {
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

pub fn redis_get_db(port: u16, db: usize, key: &str) -> Option<String> {
    let s = redis_cmd_db(port, db, &["GET", key]);
    if s.is_empty() || s == "(nil)" {
        None
    } else {
        Some(s)
    }
}

pub fn integer_reply(s: &str) -> Option<i64> {
    s.trim_start_matches("(integer) ").parse().ok()
}

/// Spill the probes (and fillers) the way every case above does.
pub fn spill_probes(port: u16, dir: &std::path::Path) -> usize {
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

pub fn finish(dir: &std::path::Path, wrong: &[String]) {
    if wrong.is_empty() && std::env::var("MOON_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(dir);
    } else {
        eprintln!("preserved test dir for diagnosis: {}", dir.display());
    }
}
