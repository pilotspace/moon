//! Wave 3 — proactive RSS memory watchdog ("mem-full guard") integration test.
//!
//! `mem_monitor` (`src/shard/mem_monitor.rs`) is the memory analogue of the
//! existing `disk_monitor` (MA12) diskfull guard: it pauses writes once
//! process RSS crosses `--mem-full-pct` of the detected system/cgroup memory
//! limit, instead of waiting for the kernel OOM-killer to pick this process.
//!
//! Real process RSS cannot be steered from an external test process, and the
//! detected limit (`detect_memory_limit_bytes`) isn't controllable either —
//! so this suite drives the guard via the `MOON_MEM_LIMIT_BYTES` test/
//! diagnostic env override documented in `mem_monitor`'s module header: set
//! it far below the server's real (unavoidable, always > 0) startup RSS, and
//! the very first `init_global` poll engages the pause deterministically.
//!
//! Wire-level on purpose: unit tests in `mem_monitor.rs` already cover the
//! hysteresis state machine in isolation. This suite instead proves the
//! dispatch-level wiring — CLI flag parsing, `init_global`'s immediate poll,
//! and the `MOONERR memfull` error path — the way `tests/oom_bypass_closure.rs`
//! proves the `--maxmemory` eviction gate's wiring.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/debug/moon cargo test --test mem_watchdog

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/oom_bypass_closure.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    // Fall back to the binary cargo built for THIS test run: compile-time
    // path with the right profile, CARGO_TARGET_DIR, and .exe suffix on
    // Windows (the old target/{release,debug}/moon probing found nothing on
    // Windows and could pick a stale release binary).
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// `tempfile::tempdir()` defaults to `$TMPDIR`, which on macOS lives on the
/// root volume group — observed near Moon's 5%-free diskfull write-pause
/// guard (`MOONERR diskfull`) in dev environments. Root the test's scratch
/// dirs under the repo's own volume instead (see
/// gotcha_vm_diskfull_shared_volume in project memory).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/mem-watchdog-tmp");
    std::fs::create_dir_all(&base).expect("create mem-watchdog-tmp base dir");
    tempfile::Builder::new()
        .prefix("mem-watchdog-")
        .tempdir_in(&base)
        .expect("tempdir_in target/mem-watchdog-tmp")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // kill() sends SIGKILL on all platforms via std::process::Child;
        // belt-and-suspenders backstop against a leaked busy-poller (see
        // gotcha_leaked_moon_busypoller_contaminates_xshard in project memory).
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn moon with `--mem-full-pct <pct>` and, optionally,
/// `MOON_MEM_LIMIT_BYTES=<limit_bytes>` to deterministically engage (or, when
/// `None`, leave untouched — falling back to real limit detection) the RSS
/// watchdog.
fn spawn_moon_mem(
    dir: &std::path::Path,
    mem_full_pct: u8,
    limit_bytes_override: Option<u64>,
) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        let mut cmd = Command::new(find_moon_binary());
        cmd.args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            "1",
            "--appendonly",
            "no",
            "--mem-full-pct",
            &mem_full_pct.to_string(),
            // This suite tests the RSS watchdog, not the disk guard; on
            // near-full dev volumes the 5%-free write pause would shadow
            // every assertion with MOONERR diskfull (see
            // gotcha_diskfull_guard_gutted_crash_tests).
            "--disk-free-min-pct",
            "0",
        ]);
        if let Some(limit) = limit_bytes_override {
            cmd.env("MOON_MEM_LIMIT_BYTES", limit.to_string());
        } else {
            // Ensure no ambient override leaks in from the test runner's shell.
            cmd.env_remove("MOON_MEM_LIMIT_BYTES");
        }
        cmd.stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe args, full-frame parser)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Err(String),
    Bulk(Vec<u8>),
    Null,
}

impl V {
    fn is_memfull_error(&self) -> bool {
        matches!(self, V::Err(msg) if msg.to_uppercase().contains("MEMFULL"))
    }
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("server never accepted on port {port}: {e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.reader.read_exact(&mut b).expect("read byte");
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0]);
            }
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn parse(&mut self) -> V {
        let line = self.read_line();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => {
                let _n: i64 = rest.parse().expect("int");
                V::Simple(rest.to_string())
            }
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    return V::Null;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).expect("bulk body");
                buf.truncate(n as usize);
                V::Bulk(buf)
            }
            "*" => {
                let n: i64 = rest.parse().expect("arr len");
                if n < 0 {
                    return V::Null;
                }
                for _ in 0..n {
                    self.parse();
                }
                V::Null
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    /// Fallible PING for the readiness probe: a connection accepted while the
    /// server is still bringing up its per-shard SO_REUSEPORT listeners can be
    /// RESET mid-read — that must retry with a fresh connection, not panic.
    fn try_ping(&mut self) -> std::io::Result<bool> {
        self.writer.write_all(b"*1\r\n$4\r\nPING\r\n")?;
        let mut buf = [0u8; 7];
        self.reader.read_exact(&mut buf)?;
        Ok(&buf == b"+PONG\r\n")
    }
}

fn wait_ready(port: u16) -> Client {
    let start = Instant::now();
    loop {
        let mut c = Client::connect(port);
        // Any I/O error (or a non-PONG answer, which would desync the framing)
        // drops this connection and probes again on a new one.
        if let Ok(true) = c.try_ping() {
            return c;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "server never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ---------------------------------------------------------------------------
// Case A: MOON_MEM_LIMIT_BYTES set far below real startup RSS + a reachable
// --mem-full-pct → the FIRST write must already be refused with
// `MOONERR memfull`, proving `init_global`'s immediate poll (no need to wait
// for the 5s timer tick) and the dispatch-level message-selection wiring.
// ---------------------------------------------------------------------------

/// Linux/macOS only: the watchdog reads real process RSS via
/// `get_rss_bytes()`, which returns the documented `0` fallback on every
/// other platform (Windows included) — RSS 0 means 0% used, so the guard
/// can never engage and this assertion can never pass there.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn test_case_a_tiny_limit_engages_memfull_on_first_write() {
    let dir = test_tmpdir();
    // 1 MB "limit" is far below any real moon process's startup RSS — the
    // watchdog's very first poll (inside init_global) must already compute
    // rss% far past 50%.
    const TINY_LIMIT_BYTES: u64 = 1_000_000;
    let (_guard, port) = spawn_moon_mem(dir.path(), 50, Some(TINY_LIMIT_BYTES));
    let mut c = wait_ready(port);

    let r = c.cmd(&[b"SET", b"k1", b"v1"]);
    assert!(
        r.is_memfull_error(),
        "expected MOONERR memfull on the first write with a 1MB fake limit \
         at 50% threshold, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Case B: read-only commands must NOT be blocked while the watchdog is
// engaged — the write gate (`metadata::is_write`) already exempts reads;
// this locks that invariant against a future over-broad gate.
// ---------------------------------------------------------------------------

/// Linux/macOS only — same `get_rss_bytes()` 0-fallback reasoning as
/// case A: the setup assertion needs the guard to actually engage.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn test_case_b_get_not_blocked_while_memfull_engaged() {
    let dir = test_tmpdir();
    const TINY_LIMIT_BYTES: u64 = 1_000_000;
    let (_guard, port) = spawn_moon_mem(dir.path(), 50, Some(TINY_LIMIT_BYTES));
    let mut c = wait_ready(port);

    // Confirm the guard is actually engaged (setup assertion).
    let set_reply = c.cmd(&[b"SET", b"k2", b"v2"]);
    assert!(
        set_reply.is_memfull_error(),
        "setup: SET should be blocked by the watchdog, got {set_reply:?}"
    );

    // GET must still work — read-only commands are exempt.
    let get_reply = c.cmd(&[b"GET", b"k2"]);
    assert_eq!(
        get_reply,
        V::Null,
        "GET must not be blocked by the memfull guard (key was never written \
         due to the SET block above, so Null is the correct, non-error reply)"
    );
}

// ---------------------------------------------------------------------------
// Case C: control — WITHOUT the env override (real limit detection, which
// on the current platform may return `None`/0 = undetectable, or a real
// large host limit either way) and a low `--mem-full-pct` floor is not
// artificially forced, SET must succeed normally. This is the negative
// control proving the guard doesn't fire spuriously on an ordinary server.
// ---------------------------------------------------------------------------

#[test]
fn test_case_c_control_no_override_set_succeeds() {
    let dir = test_tmpdir();
    let (_guard, port) = spawn_moon_mem(dir.path(), 95, None);
    let mut c = wait_ready(port);

    let r = c.cmd(&[b"SET", b"k3", b"v3"]);
    assert_eq!(
        r,
        V::Simple("OK".into()),
        "control: SET should succeed without a forced tiny memory limit, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Case D: `--mem-full-pct 0` disables the guard even with a tiny forced
// limit that would otherwise engage it at any nonzero threshold.
// ---------------------------------------------------------------------------

#[test]
fn test_case_d_mem_full_pct_zero_disables_guard() {
    let dir = test_tmpdir();
    const TINY_LIMIT_BYTES: u64 = 1_000_000;
    let (_guard, port) = spawn_moon_mem(dir.path(), 0, Some(TINY_LIMIT_BYTES));
    let mut c = wait_ready(port);

    let r = c.cmd(&[b"SET", b"k4", b"v4"]);
    assert_eq!(
        r,
        V::Simple("OK".into()),
        "--mem-full-pct 0 must disable the guard even with a tiny forced limit, got {r:?}"
    );
}
