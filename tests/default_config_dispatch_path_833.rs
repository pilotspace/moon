//! moon#833: the SHIPPED DEFAULT must serve plain `SET` and `GET` on the
//! inline fast path.
//!
//! ## Why this file exists
//!
//! Every general-purpose benchmark script in this repo starts moon with
//! `--disk-offload disable` (`scripts/bench-compare.sh`,
//! `scripts/bench-production.sh`, `scripts/bench-resources.sh`). The shipped
//! default is `enable`. So for months no benchmark measured the configuration
//! users actually run, and no test asserted the default's dispatch path.
//!
//! That is how moon#812 survived: `can_inline_writes` carried the term
//! `ctx.spill_sender.is_none()`, `--disk-offload enable` hands every connection
//! a live sender, so plain `SET` NEVER took the inline path on a default
//! server. Measured cost on the same binary, one flag apart: ~53% of SET
//! throughput (673,250 vs 1,030,574 rps growth, GCE c3-standard-8).
//!
//! The term was a CONFIG predicate standing in for a STATE one. This file is
//! the guard against the CLASS, not the instance: it starts moon with NO
//! tuning flags at all and reads `moon_dispatch_path_total` off the admin
//! port. If any future default — or any future config-shaped gate term —
//! pushes plain SET/GET off the inline path, this reddens in CI.
//!
//! ## What "no tuning flags" means here, exactly
//!
//! The server is started with precisely three flags, none of which touches a
//! dispatch predicate:
//!
//! * `--port` — a free port; the harness cannot share 6379.
//! * `--admin-port` — the INSTRUMENT. `moon_dispatch_path_total` is only
//!   scrapeable with an admin listener; the counters are recorded on the
//!   dispatch path regardless of whether a listener exists (moon#774), so the
//!   flag observes the path without changing it.
//! * `--dir` — a fresh tempdir. An omitted `--dir` resolves to the platform
//!   user-data directory (`~/.local/share/moon`, `~/Library/Application
//!   Support/moon`), where the server would (a) take the `moon.lock` dir lock
//!   and collide with any default-configured instance already running on the
//!   host, (b) reload whatever AOF/offload state a previous run left there, and
//!   (c) write this test's AOF into the developer's real data directory. The
//!   auto-resolution is exercised by `config.rs` unit tests; it is not what this
//!   file is about.
//!
//! Everything else is what `moon --port N` gives a user: `--shards 1`,
//! `--appendonly yes`, `--appendfsync everysec`, `--disk-offload enable`,
//! `--maxmemory` auto-capped by the guardrail, `--protected-mode yes`
//! (loopback, so it admits us).
//!
//! `MOON_DISK_FREE_MIN_PCT` is deliberately NOT set by this file. CI exports it
//! as `0` (`.github/workflows/ci.yml`), which relaxes the MA12 disk guard for
//! every server a test spawns; on a developer volume under 5% free the guard
//! refuses writes with `-MOONERR diskfull`, and the inline path then bails to
//! generic dispatch (moon#812's write-stall fix). That would show up here as a
//! wrong-path failure with a right-path cause, so the SET reply is checked
//! first and a `diskfull` refusal panics with its own message.
//!
//! ## Reddening proof
//!
//! Run against a pre-moon#812 binary (`d5f3501b~1` = `29fc5fce`), SET goes
//! generic on every default server:
//!
//! ```text
//! MOON_BIN=/path/to/moon-29fc5fce cargo test --release --test default_config_dispatch_path_833
//!   default_server_serves_plain_set_inline_at_shards_1 ... FAILED
//!     local_inline advanced by 0 for 200 plain SETs (local +200, cross_spsc +0)
//!   default_server_pipelined_set_and_get_batch_is_inline ... FAILED
//!     local_inline advanced by 0 for 200 commands (local +200, cross_spsc +0, cross_read_fast +0)
//!   default_server_serves_plain_get_inline_at_shards_1 ... ok   (GET never carried the term)
//! ```
//!
//! and against `origin/main` at `6251429f` all three pass. See
//! `tmp/perf-campaign/DEFAULTS-AUDIT.md` for the raw runs.
//!
//! ## What this file deliberately does NOT cover
//!
//! * `--shards > 1`. The inline path serves only keys the accepting shard owns;
//!   at `--shards 4` most keys are cross-shard by construction and `cross_spsc`
//!   is the correct path for them. The DEFAULT is `--shards 1`, and that is the
//!   configuration under guard.
//! * Commands other than plain `SET k v` / `GET k`. Those are the only two the
//!   inline path handles (`server::conn::try_inline_dispatch`); everything else
//!   takes `command::dispatch` / `dispatch_read` and is counted as `local`.
//!   A default that broke THOSE paths would be caught by every other suite.
//! * The tokio runtime. `record_dispatch_local_inline` has exactly one
//!   production call site, in the monoio connection handler; under
//!   `runtime-tokio` the counter is permanently 0 and every assertion here
//!   would fail for a reason unrelated to the defaults. Gated at crate level,
//!   same as `tests/inline_write_spill_gate_660.rs`.

#![allow(clippy::unwrap_used)]
#![cfg(feature = "runtime-monoio")]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

/// Enough that a partial-inline defect cannot hide inside readiness noise,
/// small enough that the whole file runs in seconds.
const N: u64 = 200;

// ---------------------------------------------------------------------------
// Fixture: a DEFAULT server
// ---------------------------------------------------------------------------

struct DefaultServer {
    _guard: common::ServerGuard,
    port: u16,
    admin_port: u16,
    _dir: tempfile::TempDir,
}

/// Start moon exactly as a user would, plus the three harness flags named in
/// the file header. Retries the whole pair of ports if the child comes up
/// dead (an already-held admin port makes moon exit(1) at start-up, and
/// `spawn_listening` only retries the client port — moon#811).
fn spawn_default_server() -> DefaultServer {
    const ATTEMPTS: usize = 3;
    let tmp = test_tmpdir();
    let dir = tmp.path().to_path_buf();
    let dir_str = dir.to_string_lossy().into_owned();
    for attempt in 1..=ATTEMPTS {
        let admin_port = common::reserve_port();
        let bin = common::find_moon_binary();
        let dir_arg = dir_str.clone();
        let (guard, port) = common::spawn_listening_guarded(|port| {
            Command::new(&bin)
                .args([
                    "--port",
                    &port.to_string(),
                    "--admin-port",
                    &admin_port.to_string(),
                    "--dir",
                    &dir_arg,
                ])
                .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
                .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
                .spawn()
                .expect("spawn moon (build it first, or set MOON_BIN)")
        });
        if wait_for_pong(port) && !http_get(admin_port, "/metrics").is_empty() {
            return DefaultServer {
                _guard: guard,
                port,
                admin_port,
                _dir: tmp,
            };
        }
        eprintln!(
            "spawn_default_server: attempt {attempt}/{ATTEMPTS} came up dead \
             (client {port}, admin {admin_port}); retrying on fresh ports"
        );
        drop(guard);
    }
    panic!(
        "default moon never came up serving after {ATTEMPTS} attempts; stderr tail:\n{}",
        std::fs::read_to_string(dir.join("moon.stderr.log")).unwrap_or_default()
    );
}

/// `$TMPDIR` on macOS lives on the root volume group, observed at ~95% full on
/// dev hosts — past the default 5%-free disk guard this file refuses to relax.
/// Root scratch under the repo's own volume instead (same choice as
/// `tests/inline_write_spill_gate_660.rs`).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/i833-test-tmp");
    std::fs::create_dir_all(&base).expect("create i833-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("i833-")
        .tempdir_in(&base)
        .expect("tempdir_in target/i833-test-tmp")
}

/// Poll until a real `+PONG` comes back, not merely until `connect` succeeds.
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

// ---------------------------------------------------------------------------
// Instrument: moon_dispatch_path_total
// ---------------------------------------------------------------------------

/// One scrape of every `moon_dispatch_path_total` label, so a failure can
/// say where the commands WENT, not only where they did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Paths {
    local_inline: u64,
    local: u64,
    cross_spsc: u64,
    cross_read_fast: u64,
}

impl Paths {
    fn scrape(admin_port: u16) -> Self {
        let body = http_get(admin_port, "/metrics");
        let read = |label: &str| -> u64 {
            let needle = format!("path=\"{label}\"");
            for line in body.lines() {
                if line.starts_with("moon_dispatch_path_total") && line.contains(&needle) {
                    let n = line.rsplit(' ').next().expect("metric line has a value");
                    // Prometheus renders counters as "123" or "123.0".
                    return n
                        .trim()
                        .parse::<f64>()
                        .unwrap_or_else(|e| panic!("could not parse {label} value {n:?}: {e}"))
                        as u64;
                }
            }
            // metrics-rs does not emit an untouched counter: absent == 0.
            // `http_get` already asserted the endpoint answered 200.
            0
        };
        Self {
            local_inline: read("local_inline"),
            local: read("local"),
            cross_spsc: read("cross_spsc"),
            cross_read_fast: read("cross_read_fast"),
        }
    }

    fn delta(after: Self, before: Self) -> Self {
        Self {
            local_inline: after.local_inline - before.local_inline,
            local: after.local - before.local,
            cross_spsc: after.cross_spsc - before.cross_spsc,
            cross_read_fast: after.cross_read_fast - before.cross_read_fast,
        }
    }
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
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").as_bytes())
        .expect("send admin request");
    let mut body = Vec::new();
    stream.read_to_end(&mut body).expect("read admin response");
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(
        text.contains("200"),
        "admin {path} did not answer 200; response was:\n{text}"
    );
    text
}

/// What the default actually resolved to on THIS host — attached to every
/// failure so a red run names the configuration it ran under rather than the
/// one the flag docs describe (the two differ: see the audit).
fn effective_config(port: u16) -> String {
    let mut c = common::Conn::open(port);
    let mut out = String::new();
    for key in [
        "maxmemory",
        "maxmemory-policy",
        "appendonly",
        "appendfsync",
        "disk-offload",
    ] {
        let reply = c.send(&["CONFIG", "GET", key]);
        out.push_str(&format!(
            "  CONFIG GET {key}: {}\n",
            reply.replace("\r\n", " ")
        ));
    }
    let info = c.send(&["INFO", "server"]);
    for line in info.lines() {
        if line.starts_with("num_shards") || line.starts_with("moon_version") {
            out.push_str(&format!("  INFO {line}\n"));
        }
    }
    out
}

/// A `SET` on a default server can be refused by the MA12 disk guard when the
/// test volume is nearly full. That is an ENVIRONMENT failure, and it must not
/// be reported as a dispatch-path one.
fn assert_ok(reply: &str, what: &str) {
    if reply.contains("diskfull") {
        panic!(
            "{what} refused with {reply:?}: the volume under this test's data dir is below \
             moon's default --disk-free-min-pct (5%). Free space, or export \
             MOON_DISK_FREE_MIN_PCT=0 as CI does. This is not a dispatch-path failure."
        );
    }
    assert_eq!(reply, "+OK\r\n", "{what} did not answer +OK");
}

// ---------------------------------------------------------------------------
// The guards
// ---------------------------------------------------------------------------

/// The moon#812 shape: N plain `SET k v`, one connection, request/response,
/// on a DEFAULT server, must ALL be counted as `local_inline`.
///
/// Reddens on a pre-moon#812 binary with `local_inline +0, local +200`.
#[test]
fn default_server_serves_plain_set_inline_at_shards_1() {
    let s = spawn_default_server();
    let mut c = common::Conn::open(s.port);
    let before = Paths::scrape(s.admin_port);

    for i in 0..N {
        let key = format!("i833:set:{i}");
        let reply = c.send(&["SET", &key, "v"]);
        assert_ok(&reply, &format!("SET #{i}"));
    }

    let d = Paths::delta(Paths::scrape(s.admin_port), before);
    assert_eq!(
        d.local_inline,
        N,
        "a DEFAULT server (no tuning flags) must serve every plain SET on the inline fast \
         path at --shards 1: local_inline advanced by {} for {N} plain SETs (local +{}, \
         cross_spsc +{}, cross_read_fast +{}). If a default value or a config-shaped gate \
         term pushed SET off the inline path, this is moon#812 again.\n\
         Effective configuration on this host:\n{}",
        d.local_inline,
        d.local,
        d.cross_spsc,
        d.cross_read_fast,
        effective_config(s.port)
    );
    assert_eq!(
        d.cross_spsc, 0,
        "at --shards 1 nothing is cross-shard; {} SETs took the SPSC hop",
        d.cross_spsc
    );
}

/// The read half: `can_inline_reads` never carried the spill term, so this
/// stays green on the pre-moon#812 binary. It is here so the NEXT
/// config-shaped term — whichever gate it lands in — is caught on both
/// inline-eligible commands, not only the one that already bit.
#[test]
fn default_server_serves_plain_get_inline_at_shards_1() {
    let s = spawn_default_server();
    let mut c = common::Conn::open(s.port);
    // Half hits, half misses: the inline path frames its own null for a miss
    // (moon#522), and both are inline-eligible.
    for i in 0..N / 2 {
        let key = format!("i833:get:{i}");
        assert_ok(&c.send(&["SET", &key, "v"]), &format!("seed SET #{i}"));
    }
    let before = Paths::scrape(s.admin_port);

    for i in 0..N {
        let key = format!("i833:get:{i}");
        let reply = c.send(&["GET", &key]);
        let expected = if i < N / 2 { "$1\r\nv\r\n" } else { "$-1\r\n" };
        assert_eq!(reply, expected, "GET #{i}");
    }

    let d = Paths::delta(Paths::scrape(s.admin_port), before);
    assert_eq!(
        d.local_inline,
        N,
        "a DEFAULT server must serve every plain GET on the inline fast path at --shards 1: \
         local_inline advanced by {} for {N} plain GETs (local +{}, cross_spsc +{}, \
         cross_read_fast +{}).\nEffective configuration on this host:\n{}",
        d.local_inline,
        d.local,
        d.cross_spsc,
        d.cross_read_fast,
        effective_config(s.port)
    );
    assert_eq!(
        d.cross_spsc, 0,
        "{} GETs took the SPSC hop at --shards 1",
        d.cross_spsc
    );
}

/// One pipelined write of N/2 SETs then N/2 GETs — `redis-benchmark -P`'s
/// shape, and `try_inline_dispatch_loop`'s batch arm rather than the
/// one-frame arm. Reddens on the pre-moon#812 binary with `local_inline +0`,
/// not `+100`: the inline loop hands the WHOLE remaining batch to generic
/// dispatch at the first frame it cannot take, so the GET half goes generic
/// with the SETs in front of it. That is the batch arm's own behaviour and
/// the reason a pipelined benchmark saw the full deficit.
#[test]
fn default_server_pipelined_set_and_get_batch_is_inline() {
    let s = spawn_default_server();
    let mut c = common::Conn::open(s.port);
    let before = Paths::scrape(s.admin_port);

    let keys: Vec<String> = (0..N / 2).map(|i| format!("i833:pipe:{i}")).collect();
    let mut cmds: Vec<Vec<&str>> = Vec::with_capacity(N as usize);
    for k in &keys {
        cmds.push(vec!["SET", k, "v"]);
    }
    for k in &keys {
        cmds.push(vec!["GET", k]);
    }
    let cmd_refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
    let replies = c.pipeline(&cmd_refs);
    if replies.contains("diskfull") {
        assert_ok(&replies, "pipelined SET");
    }
    assert_eq!(
        replies.matches("+OK\r\n").count(),
        (N / 2) as usize,
        "every pipelined SET must answer +OK; replies were:\n{replies}"
    );
    assert_eq!(
        replies.matches("$1\r\nv\r\n").count(),
        (N / 2) as usize,
        "every pipelined GET must answer the value; replies were:\n{replies}"
    );

    let d = Paths::delta(Paths::scrape(s.admin_port), before);
    assert_eq!(
        d.local_inline,
        N,
        "a DEFAULT server must serve a pipelined SET+GET batch entirely on the inline fast \
         path at --shards 1: local_inline advanced by {} for {N} commands (local +{}, \
         cross_spsc +{}, cross_read_fast +{}).\nEffective configuration on this host:\n{}",
        d.local_inline,
        d.local,
        d.cross_spsc,
        d.cross_read_fast,
        effective_config(s.port)
    );
}
