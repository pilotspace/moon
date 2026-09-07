//! moon#838: a DEFAULT-config server must complete a pipelined SET burst.
//!
//! ## What broke
//!
//! At v0.8.9 (`6251429f`) a moon server started with NO flags refused a routine
//! `redis-benchmark -t set -c 50 -P 16` with
//!
//! ```text
//! -MOONERR AOF backpressure: write applied in memory but not queued for persistence
//! ```
//!
//! 5/5 on GCE, 2/2 when re-run for the fix. The shipped default is
//! `--appendonly yes --appendfsync everysec --disk-offload enable`; every
//! benchmark script overrides the first and the third, which is why no
//! benchmark saw it (moon#833). moon#812 put plain `SET` on the inline fast
//! path under that default, and the inline path enqueued its AOF record with
//! a synchronous 5 ms bounded block (`AOF_SPSC_BACKPRESSURE_BOUND`) that fails
//! loud AFTER the write is applied, while the generic leg awaits the same
//! channel for 2 s (`--aof-fsync-timeout-ms`) and blocks nothing. Any writer
//! hiccup of ~10 ms at ~1M rps fills the 10k channel and the burst is refused.
//!
//! ## The fix under guard
//!
//! `try_inline_dispatch` now asks the writer (`AofWriterPool::append_would_block`)
//! BEFORE it consumes the command bytes, and stands down to generic dispatch
//! when the channel is full — the same shape as the moon#660 eviction
//! pre-gate. The inline path never applies a write it cannot queue; the leg
//! that can wait takes it.
//!
//! ## The two writer stalls this file drives
//!
//! * **Idle escalation, no fault injection** — the writer polls its channel
//!   park-free and sleeps `wait/16` between empty polls; once idle for ~1.3 s
//!   `IdleWait` escalates the wait to 1 s and the sleep to 50 ms. A burst that
//!   lands inside that sleep queues 50 ms of production against a 10k channel.
//!   This is the first-burst-after-idle shape of the GCE reproduction (rep 1
//!   aborted at `rps=0.0`). Test A uses exactly the shipped flags.
//! * **A slow proactive fsync** — `MOON_TEST_AOF_FSYNC_STALL_MS` (writer-side
//!   test hook, `persistence/aof/writer_task.rs`) holds the everysec proactive
//!   fsync for N ms, the moon#769 mechanism, made deterministic. Tests B and C.
//!
//! ## Harness flags (none touches a dispatch or backpressure predicate)
//!
//! `--port` (free port), `--dir` (fresh tempdir — an omitted `--dir` is the
//! user-data dir with its `moon.lock` and whatever the last run left there),
//! `--admin-port` (the instrument: `moon_dispatch_path_total`, recorded on the
//! dispatch path whether or not a listener exists, moon#774). Test C adds the
//! one non-default flag its contract is about, `--aof-fsync-timeout-ms`.
//!
//! ## Reddening proof (`origin/main` @ `6251429f`, GCE c3-standard-8 Linux,
//! `MOON_BIN=moon-833-6251429f`, `--test-threads=1`)
//!
//! ```text
//! test default_server_completes_pipelined_set_burst_after_idle ... FAILED
//!   bursts after idle: a DEFAULT server refused 9 of 400000 pipelined SETs (399991 +OK).
//!   First 3 refusals (index, text): [(943, "MOONERR AOF backpressure: write applied in
//!   memory but not queued for persistence"), ...]. Server-side: aof_last_append_status:err
//! test default_server_completes_pipelined_set_bursts_across_a_writer_stall ... FAILED
//!   bursts across a 600 ms writer stall: a DEFAULT server refused 17 of 2060000 ...
//! test refused_write_past_the_fsync_timeout_is_applied_and_reported ... FAILED
//!   ... never from the inline 5 ms block; got 21 inline refusals
//! test result: FAILED. 0 passed; 3 failed
//! ```
//!
//! With the fix binary on the same host: `3 passed` (B: `local` advanced, so the
//! stall bit; C: 14–15 refusals, every one the generic leg's text, every refused
//! key readable). Same on macOS (Apple Silicon, kqueue): 3 passed.
//!
//! Not covered: `--shards > 1` (the inline path serves only the accepting
//! shard's keys; cross-shard SETs take the SPSC arm, whose 5 ms bound is
//! moon#769's subject), and the tokio runtime (no inline path; crate-gated
//! like `tests/default_config_dispatch_path_833.rs`).

#![allow(clippy::unwrap_used)]
#![cfg(feature = "runtime-monoio")]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

/// The inline path's refusal (`shard/spsc_handler.rs::AOF_APPEND_LOST_ERR`).
const INLINE_BACKPRESSURE_ERR: &str =
    "MOONERR AOF backpressure: write applied in memory but not queued for persistence";
/// The generic leg's refusal once `--aof-fsync-timeout-ms` elapses
/// (`persistence/aof::AOF_FSYNC_ERR`).
const GENERIC_TIMEOUT_ERR: &str = "ERR AOF fsync failed; write not durable";

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Server {
    _guard: common::ServerGuard,
    port: u16,
    admin_port: u16,
    dir: std::path::PathBuf,
    _tmp: tempfile::TempDir,
}

/// Start moon with the shipped defaults plus the harness flags named in the
/// file header, `extra` flags, and `env` for the child. Retries the port pair
/// if the child comes up dead (moon#811).
fn spawn_server(extra: &[&str], env: &[(&str, &str)]) -> Server {
    const ATTEMPTS: usize = 3;
    let tmp = test_tmpdir();
    let dir = tmp.path().to_path_buf();
    let dir_str = dir.to_string_lossy().into_owned();
    for attempt in 1..=ATTEMPTS {
        let admin_port = common::reserve_port();
        let bin = common::find_moon_binary();
        let dir_arg = dir_str.clone();
        let (guard, port) = common::spawn_listening_guarded(|port| {
            let mut c = Command::new(&bin);
            c.args([
                "--port",
                &port.to_string(),
                "--admin-port",
                &admin_port.to_string(),
                "--dir",
                &dir_arg,
            ])
            .args(extra)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"));
            for (k, v) in env {
                c.env(k, v);
            }
            c.spawn()
                .expect("spawn moon (build it first, or set MOON_BIN)")
        });
        if wait_for_pong(port) && !http_get(admin_port, "/metrics").is_empty() {
            return Server {
                _guard: guard,
                port,
                admin_port,
                dir,
                _tmp: tmp,
            };
        }
        eprintln!(
            "spawn_server: attempt {attempt}/{ATTEMPTS} came up dead (client {port}, admin \
             {admin_port}); retrying on fresh ports"
        );
        drop(guard);
    }
    panic!(
        "moon never came up serving after {ATTEMPTS} attempts; stderr tail:\n{}",
        std::fs::read_to_string(dir.join("moon.stderr.log")).unwrap_or_default()
    );
}

impl Server {
    /// Dropped-append log lines so far: the inline/SPSC path's `AOF append
    /// LOST` (error) and the generic leg's `everysec append dropped` (warn).
    /// `tracing_subscriber::fmt()` writes to STDOUT by default, so both
    /// streams are scanned.
    fn lost_log_lines(&self) -> usize {
        ["moon.stdout.log", "moon.stderr.log"]
            .iter()
            .map(|f| {
                std::fs::read_to_string(self.dir.join(f))
                    .unwrap_or_default()
                    .lines()
                    .filter(|l| l.contains("AOF append LOST") || l.contains("append dropped"))
                    .count()
            })
            .sum()
    }
}

/// `$TMPDIR` on macOS lives on the root volume group, observed at ~95% full
/// on dev hosts — past the default 5%-free disk guard. Root scratch under the
/// repo's own volume instead (same choice as the moon#660 and moon#833 files).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/i838-test-tmp");
    std::fs::create_dir_all(&base).expect("create i838-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("i838-")
        .tempdir_in(&base)
        .expect("tempdir_in target/i838-test-tmp")
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_secs(5),
    )
    .expect("connect");
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s
}

fn wait_for_pong(port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        ) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 7];
            if s.write_all(b"PING\r\n").is_ok()
                && s.read_exact(&mut buf).is_ok()
                && buf.starts_with(b"+PONG")
            {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn http_get(port: u16, path: &str) -> String {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let start = Instant::now();
    let mut stream = loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => break s,
            Err(e) => {
                assert!(
                    start.elapsed() < Duration::from_secs(20),
                    "admin port {port} never accepted a connection: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(stream, "GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n").unwrap();
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw).into_owned();
    if text.is_empty() {
        return text;
    }
    assert!(
        text.starts_with("HTTP/1.0 200") || text.starts_with("HTTP/1.1 200"),
        "GET {path} on admin {port} answered:\n{text}"
    );
    text.split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Instruments
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Paths {
    local_inline: u64,
    local: u64,
}

impl Paths {
    fn scrape(admin_port: u16) -> Self {
        let body = http_get(admin_port, "/metrics");
        let read = |label: &str| -> u64 {
            let needle = format!("path=\"{label}\"");
            body.lines()
                .find(|l| l.starts_with("moon_dispatch_path_total") && l.contains(&needle))
                .map(|l| l.rsplit(' ').next().unwrap().trim().parse::<f64>().unwrap() as u64)
                .unwrap_or(0)
        };
        Self {
            local_inline: read("local_inline"),
            local: read("local"),
        }
    }
}

/// `INFO persistence` over a fresh connection, `\r` stripped.
fn info_persistence(port: u16) -> String {
    let mut s = connect(port);
    s.write_all(b"INFO persistence\r\n").unwrap();
    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = s.read(&mut chunk).expect("read INFO");
        assert!(n > 0, "connection closed mid-INFO");
        raw.extend_from_slice(&chunk[..n]);
        // `$<len>\r\n<payload>\r\n`
        if let Some(hdr_end) = raw.iter().position(|&b| b == b'\n') {
            let len: usize = std::str::from_utf8(&raw[1..hdr_end])
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            if raw.len() >= hdr_end + 1 + len + 2 {
                return String::from_utf8_lossy(&raw[hdr_end + 1..hdr_end + 1 + len])
                    .replace('\r', "");
            }
        }
    }
}

fn get(port: u16, key: &str) -> Option<String> {
    let mut s = connect(port);
    write!(s, "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).unwrap();
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = s.read(&mut chunk).expect("read GET");
        assert!(n > 0, "connection closed mid-GET");
        raw.extend_from_slice(&chunk[..n]);
        if raw.starts_with(b"$-1\r\n") || raw.starts_with(b"_\r\n") {
            return None;
        }
        if let Some(hdr_end) = raw.iter().position(|&b| b == b'\n') {
            let len: usize = std::str::from_utf8(&raw[1..hdr_end])
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            if raw.len() >= hdr_end + 1 + len + 2 {
                return Some(
                    String::from_utf8_lossy(&raw[hdr_end + 1..hdr_end + 1 + len]).into_owned(),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The burst
// ---------------------------------------------------------------------------

/// Outcome of one pipelined burst: reply text per command index, in order.
struct Burst {
    keys: Vec<String>,
    ok: usize,
    /// `(index, error text without the leading '-')`
    errors: Vec<(usize, String)>,
}

/// One pipelined write of `n` plain `SET <prefix>:<i> v` frames on ONE
/// connection, then exactly `n` replies read back. Every reply must be a
/// simple string or an error line — anything else is a framing bug and panics.
fn pipelined_sets(port: u16, prefix: &str, n: usize) -> Burst {
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
    let mut ok = 0usize;
    let mut errors = Vec::new();
    let mut seen = 0usize;
    let mut cursor = 0usize;
    while seen < n {
        let got = s
            .read(&mut chunk)
            .expect("read replies (30 s read timeout elapsed?)");
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
                Some(b'-') => errors.push((seen, String::from_utf8_lossy(&line[1..]).into_owned())),
                other => panic!(
                    "reply {seen} is not a status line (first byte {other:?}): {:?}",
                    String::from_utf8_lossy(line)
                ),
            }
            seen += 1;
            cursor += rel + 2;
        }
    }
    Burst { keys, ok, errors }
}

fn assert_no_refusals(bursts: &[Burst], server: &Server, what: &str) {
    let sent: usize = bursts.iter().map(|b| b.keys.len()).sum();
    let ok: usize = bursts.iter().map(|b| b.ok).sum();
    let errors: Vec<&(usize, String)> = bursts.iter().flat_map(|b| b.errors.iter()).collect();
    let info = info_persistence(server.port);
    let status = info
        .lines()
        .find(|l| l.starts_with("aof_last_append_status:"))
        .unwrap_or("aof_last_append_status:<absent>")
        .to_owned();
    assert!(
        errors.is_empty() && ok == sent,
        "{what}: a DEFAULT server refused {} of {sent} pipelined SETs ({ok} +OK). First 3 \
         refusals (index, text): {:?}. Server-side: {status}, dropped-append log lines: {}",
        errors.len(),
        errors.iter().take(3).collect::<Vec<_>>(),
        server.lost_log_lines()
    );
    assert_eq!(
        status, "aof_last_append_status:ok",
        "{what}: no append may have been dropped"
    );
    assert_eq!(
        server.lost_log_lines(),
        0,
        "{what}: no `AOF append LOST` may be logged"
    );
}

// ---------------------------------------------------------------------------
// A. Shipped flags, no fault injection: bursts after the writer idled.
// ---------------------------------------------------------------------------

/// Each burst: 4 × 25k = 100k SETs against a 10k channel, from four
/// connections at once so the shard produces at full speed.
const IDLE_BURST_CONNS: usize = 4;
const IDLE_BURST_PER_CONN: usize = 25_000;
/// Whether one burst lands inside the writer's 50 ms sleep is a coin toss
/// (measured on GCE with the pre-fix binary and `redis-cli --pipe`: 2 of 3
/// single bursts refused). Several idle→burst cycles make a broken binary
/// red with high probability; a fixed one is green regardless.
const IDLE_CYCLES: usize = 4;
/// `IdleWait` climbs 50 ms → 250 ms → 1 s after ≈1.3 s without a message;
/// only then is the poll step at its 50 ms ceiling.
const IDLE_BEFORE_BURST: Duration = Duration::from_millis(1600);

#[test]
fn default_server_completes_pipelined_set_burst_after_idle() {
    let server = spawn_server(&[], &[]);
    let before = Paths::scrape(server.admin_port);
    let port = server.port;
    let mut bursts = Vec::new();
    for cycle in 0..IDLE_CYCLES {
        std::thread::sleep(IDLE_BEFORE_BURST);
        let handles: Vec<_> = (0..IDLE_BURST_CONNS)
            .map(|c| {
                std::thread::spawn(move || {
                    pipelined_sets(port, &format!("idle:{cycle}:{c}"), IDLE_BURST_PER_CONN)
                })
            })
            .collect();
        bursts.extend(handles.into_iter().map(|h| h.join().unwrap()));
    }
    let after = Paths::scrape(server.admin_port);
    eprintln!(
        "A: {IDLE_CYCLES} idle→burst cycles; local_inline +{} local +{} (a `local` delta is \
         the fast path standing down; not asserted — whether a burst meets the sleep is not \
         deterministic, see IDLE_CYCLES)",
        after.local_inline - before.local_inline,
        after.local - before.local
    );
    assert_no_refusals(&bursts, &server, "bursts after idle");
    for b in &bursts {
        let last = b.keys.last().unwrap();
        assert_eq!(
            get(port, last).as_deref(),
            Some("v"),
            "an acked SET must be readable"
        );
    }
}

// ---------------------------------------------------------------------------
// B. Shipped flags + a 600 ms proactive-fsync stall (< the 2 s generic bound):
//    bursts for 3.5 s must all complete, and the fast path must have stood
//    down at least once (proof the stall bit — the guard cannot pass vacuously).
// ---------------------------------------------------------------------------

const STALL_BURST: usize = 20_000;

#[test]
fn default_server_completes_pipelined_set_bursts_across_a_writer_stall() {
    let server = spawn_server(&[], &[("MOON_TEST_AOF_FSYNC_STALL_MS", "600")]);
    let before = Paths::scrape(server.admin_port);
    let started = Instant::now();
    let mut bursts = Vec::new();
    let mut round = 0usize;
    while started.elapsed() < Duration::from_millis(3500) {
        bursts.push(pipelined_sets(
            server.port,
            &format!("stall:{round}"),
            STALL_BURST,
        ));
        round += 1;
    }
    let after = Paths::scrape(server.admin_port);
    let stood_down = after.local - before.local;
    eprintln!(
        "B: {round} bursts × {STALL_BURST}; local_inline +{} local +{stood_down}",
        after.local_inline - before.local_inline
    );
    assert_no_refusals(&bursts, &server, "bursts across a 600 ms writer stall");
    assert!(
        stood_down >= 1,
        "vacuity guard: with the writer stalled 600 ms per second, at least one SET must have \
         reached generic dispatch (`local` +{stood_down}); if none did, the stall hook did not \
         bite and this test proved nothing"
    );
    let last = bursts.last().unwrap().keys.last().unwrap();
    assert_eq!(get(server.port, last).as_deref(), Some("v"));
}

// ---------------------------------------------------------------------------
// C. The semantics pin: a stall LONGER than the fsync timeout. The refusal
//    must come from the leg that waited `--aof-fsync-timeout-ms` (fail-closed,
//    applied-first — the pre-#812 contract, and moon#769's), never from the
//    inline 5 ms bound; and a refused SET is readable, because it was applied.
// ---------------------------------------------------------------------------

#[test]
fn refused_write_past_the_fsync_timeout_is_applied_and_reported() {
    let server = spawn_server(
        &["--aof-fsync-timeout-ms", "100"],
        &[("MOON_TEST_AOF_FSYNC_STALL_MS", "1500")],
    );
    let started = Instant::now();
    let mut bursts = Vec::new();
    let mut round = 0usize;
    while started.elapsed() < Duration::from_millis(3500) {
        bursts.push(pipelined_sets(
            server.port,
            &format!("timeout:{round}"),
            STALL_BURST,
        ));
        round += 1;
    }
    let refused: Vec<(&str, &str)> = bursts
        .iter()
        .flat_map(|b| {
            b.errors
                .iter()
                .map(|(i, e)| (b.keys[*i].as_str(), e.as_str()))
        })
        .collect();
    let ok: usize = bursts.iter().map(|b| b.ok).sum();
    eprintln!("C: {round} bursts, {ok} +OK, {} refused", refused.len());
    assert!(
        !refused.is_empty(),
        "a 1.5 s writer stall against a 100 ms fsync timeout must refuse at least one SET — \
         if none was refused the stall hook did not bite and this pin proved nothing"
    );
    let inline_refusals = refused
        .iter()
        .filter(|(_, e)| *e == INLINE_BACKPRESSURE_ERR)
        .count();
    assert_eq!(
        inline_refusals,
        0,
        "on a default server the refusal must come from the generic leg's \
         `--aof-fsync-timeout-ms` bound, never from the inline 5 ms block; got {inline_refusals} \
         inline refusals, e.g. {:?}",
        refused.iter().find(|(_, e)| *e == INLINE_BACKPRESSURE_ERR)
    );
    for (key, text) in &refused {
        assert_eq!(
            *text, GENERIC_TIMEOUT_ERR,
            "unexpected refusal text for {key}"
        );
    }
    // Applied-first, fail-closed: every refused key stands in memory — the
    // error is about durability, not about the write.
    for (key, _) in refused.iter().take(64) {
        assert_eq!(
            get(server.port, key).as_deref(),
            Some("v"),
            "{key}: a SET refused for persistence must still be readable (applied-first)"
        );
    }
    let info = info_persistence(server.port);
    assert!(
        info.contains("aof_last_append_status:err"),
        "INFO must latch the dropped append:\n{info}"
    );
    assert!(
        server.lost_log_lines() >= 1,
        "the drop must be logged loud (`everysec append dropped` / `AOF append LOST`)"
    );
}
