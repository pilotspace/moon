//! moon#1272: under `appendfsync everysec`, a write refused because the AOF
//! writer is BACKLOGGED must not be reported as an fsync failure.
//!
//! A durable-path producer waits up to `--aof-fsync-timeout-ms` for room in
//! its shard's writer channel. When the writer stays behind for that long the
//! record is refused (`AofAck::ChannelFull`). No fsync ran, and none failed,
//! yet every connection path answered `-ERR AOF fsync failed; write not
//! durable` — the text of a real disk fault — and nothing in INFO explained
//! the refusal.
//!
//! Now the refusal answers `AOF_BACKLOG_ERR`
//! (`-MOONERR AOF backpressure: write applied in memory but not queued for
//! persistence; the AOF writer is backlogged`), is counted in INFO
//! `aof_append_backpressure_refusals` and in `/metrics`
//! `moon_aof_append_backpressure_refusals_total`, is logged once per stall,
//! and leaves the fsync status fields alone.
//!
//! The stall is the writer-side test hook the moon#838/#769 suites use:
//! `MOON_TEST_AOF_FSYNC_STALL_MS` holds the writer before each everysec
//! proactive fsync. `--shards 1` keeps every write on the generic local leg
//! (no routed moon#769 admission), `--aof-fsync-timeout-ms 100` makes that
//! leg give up long before the 1.5 s stall ends, and
//! `--auto-aof-rewrite-percentage 0` keeps a rewrite out of the picture.
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test aof_backpressure_reply_1272
//! ```
//! Run it against both runtimes' binaries (monoio default, and
//! `--no-default-features --features runtime-tokio,jemalloc`).

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

/// `persistence::aof::AOF_BACKLOG_ERR`, without the leading `-`.
const BACKLOG_ERR: &str = "MOONERR AOF backpressure: write applied in memory but not queued for \
                           persistence; the AOF writer is backlogged";
/// The fsync-failure text a backlog refusal used to carry.
const FSYNC_ERR_FRAGMENT: &str = "fsync failed";
/// Pipelined SETs per burst: twice the 10k writer channel, so a burst that
/// lands in a stall fills it.
const BURST: usize = 20_000;

struct Server {
    _guard: common::ServerGuard,
    port: u16,
    admin_port: u16,
    dir: std::path::PathBuf,
    _tmp: tempfile::TempDir,
}

fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/i1272-test-tmp");
    std::fs::create_dir_all(&base).expect("create i1272-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("i1272-")
        .tempdir_in(&base)
        .expect("tempdir_in target/i1272-test-tmp")
}

fn spawn_stalled_server() -> Server {
    let tmp = test_tmpdir();
    let dir = tmp.path().to_path_buf();
    let dir_str = dir.to_string_lossy().into_owned();
    let admin_port = common::reserve_port();
    let bin = common::find_moon_binary();
    let log_dir = dir.clone();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--admin-port",
                &admin_port.to_string(),
                "--dir",
                &dir_str,
                "--shards",
                "1",
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--aof-fsync-timeout-ms",
                "100",
                "--auto-aof-rewrite-percentage",
                "0",
            ])
            .env("MOON_TEST_AOF_FSYNC_STALL_MS", "1500")
            .stdout(std::fs::File::create(log_dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(log_dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    Server {
        _guard: guard,
        port,
        admin_port,
        dir,
        _tmp: tmp,
    }
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_secs(5),
    )
    .expect("connect");
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    s
}

/// One pipelined burst of `SET <prefix>:<i> v`. Returns (+OK count,
/// (key, error text without '-')).
fn pipelined_sets(port: u16, prefix: &str, n: usize) -> (usize, Vec<(String, String)>) {
    let mut s = connect(port);
    let mut keys = Vec::with_capacity(n);
    let mut wire = Vec::with_capacity(n * 40);
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        wire.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\nv\r\n",
                key.len(),
                key
            )
            .as_bytes(),
        );
        keys.push(key);
    }
    s.write_all(&wire).expect("write burst");
    let mut raw = Vec::new();
    let mut chunk = [0u8; 65536];
    let (mut ok, mut errors, mut seen, mut cursor) = (0usize, Vec::new(), 0usize, 0usize);
    while seen < n {
        let got = s.read(&mut chunk).expect("read replies");
        assert!(
            got > 0,
            "server closed the connection after {seen}/{n} replies"
        );
        raw.extend_from_slice(&chunk[..got]);
        while seen < n {
            let Some(rel) = raw[cursor..].windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            let line = &raw[cursor..cursor + rel];
            match line.first() {
                Some(b'+') => ok += 1,
                Some(b'-') => errors.push((
                    keys[seen].clone(),
                    String::from_utf8_lossy(&line[1..]).into_owned(),
                )),
                other => panic!(
                    "reply {seen} is not a status line (first byte {other:?}): {:?}",
                    String::from_utf8_lossy(line)
                ),
            }
            seen += 1;
            cursor += rel + 2;
        }
    }
    (ok, errors)
}

/// One command over a fresh connection; the whole reply as text.
fn command(port: u16, parts: &[&str]) -> String {
    let mut s = connect(port);
    s.write_all(&common::encode(parts)).unwrap();
    let mut raw = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        let n = s.read(&mut chunk).expect("read reply");
        assert!(n > 0, "connection closed mid-reply");
        raw.extend_from_slice(&chunk[..n]);
        if let Some(len) = common_framed(&raw) {
            return String::from_utf8_lossy(&raw[..len]).into_owned();
        }
    }
}

fn common_framed(buf: &[u8]) -> Option<usize> {
    // A bulk string (`$<len>\r\n<payload>\r\n`) or a single status line.
    let hdr = buf.windows(2).position(|w| w == b"\r\n")?;
    if buf.first() == Some(&b'$') {
        let len: i64 = std::str::from_utf8(&buf[1..hdr])
            .ok()?
            .trim()
            .parse()
            .ok()?;
        if len < 0 {
            return Some(hdr + 2);
        }
        let end = hdr + 2 + usize::try_from(len).ok()? + 2;
        (buf.len() >= end).then_some(end)
    } else {
        Some(hdr + 2)
    }
}

fn info_field(port: u16, field: &str) -> String {
    let info = command(port, &["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_owned())
        .unwrap_or_else(|| panic!("INFO persistence has no `{field}`:\n{info}"))
}

fn metrics(admin_port: u16) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", admin_port)).expect("admin connect");
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    write!(s, "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n").unwrap();
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    String::from_utf8_lossy(&raw).into_owned()
}

fn backlog_log_lines(dir: &std::path::Path) -> usize {
    ["moon.stdout.log", "moon.stderr.log"]
        .iter()
        .map(|f| {
            std::fs::read_to_string(dir.join(f))
                .unwrap_or_default()
                .lines()
                .filter(|l| l.contains("AOF writer backlogged") || l.contains("still backlogged"))
                .count()
        })
        .sum()
}

#[test]
fn writer_backlog_refusal_is_not_reported_as_an_fsync_failure() {
    let server = spawn_stalled_server();
    let port = server.port;

    // Bursts until the stall has refused a few writes (each refusal on this
    // connection costs the 100 ms bound, ~15 per 1.5 s stall).
    let started = Instant::now();
    let (mut ok, mut refused) = (0usize, Vec::new());
    let mut round = 0usize;
    while refused.len() < 5 && started.elapsed() < Duration::from_secs(60) {
        let (o, e) = pipelined_sets(port, &format!("r{round}"), BURST);
        ok += o;
        refused.extend(e);
        round += 1;
    }
    eprintln!(
        "{round} bursts x {BURST}: {ok} +OK, {} refused, {:?}",
        refused.len(),
        started.elapsed()
    );
    assert!(
        !refused.is_empty(),
        "vacuity guard: a 1.5 s writer stall against a 100 ms bound refused no SET in 60 s — \
         the stall hook did not bite and this test proved nothing"
    );

    // 1. The reply names the condition: writer backlog, never an fsync failure.
    for (key, text) in &refused {
        assert!(
            !text.contains(FSYNC_ERR_FRAGMENT),
            "{key}: a writer-backlog refusal answered the fsync-failure text {text:?} \
             (moon#1272) — no fsync ran or failed"
        );
        assert_eq!(text, BACKLOG_ERR, "{key}: unexpected refusal text");
    }

    // 2. INFO explains it, and the fsync status fields are untouched.
    let counted: u64 = info_field(port, "aof_append_backpressure_refusals")
        .parse()
        .expect("aof_append_backpressure_refusals is a number");
    assert_eq!(
        counted,
        refused.len() as u64,
        "INFO aof_append_backpressure_refusals must count exactly the refusals clients saw"
    );
    assert_eq!(
        info_field(port, "aof_fsync_failures"),
        "0",
        "no fsync failed"
    );
    assert_eq!(info_field(port, "aof_last_fsync_status"), "ok");

    // 3. The Prometheus mirror.
    let body = metrics(server.admin_port);
    let prom: u64 = body
        .lines()
        .find(|l| l.starts_with("moon_aof_append_backpressure_refusals_total"))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.trim().parse::<f64>().ok())
        .map(|v| v as u64)
        .unwrap_or_else(|| panic!("no moon_aof_append_backpressure_refusals_total in /metrics"));
    assert_eq!(prom, counted, "/metrics must mirror the INFO counter");

    // 4. The text's claim holds: the refused write stands in memory.
    for (key, _) in refused.iter().take(16) {
        assert_eq!(
            command(port, &["GET", key]),
            "$1\r\nv\r\n",
            "{key}: the reply says 'applied in memory'; it must be readable"
        );
    }

    // 5. Logged once per stall, not once per refusal.
    let lines = backlog_log_lines(&server.dir);
    assert!(lines >= 1, "a backlog stall must be logged at WARN");
    assert!(
        lines <= 2 + (started.elapsed().as_secs() as usize) / 10,
        "{lines} backlog log lines for {} refusals in {:?}: the WARN must be rate-limited \
         (one per stall plus a summary at most every 10 s)",
        refused.len(),
        started.elapsed()
    );
}
