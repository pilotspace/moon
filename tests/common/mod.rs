//! Shared test-server harness helpers (task #18 flake sweep).
//!
//! Every integration suite that spawns a real `moon` process used to carry
//! its own copy of a `free_port()` that binds `:0`, reads the port, and
//! DROPS the listener before the server spawns. Two failure modes shipped
//! with that pattern:
//!
//! 1. **Port TOCTOU** — between the probe drop and moon's bind, the kernel
//!    can hand the same port to anyone else: another test's probe in the
//!    same process, or (the CI-observed case) a concurrent test's outbound
//!    TCP connection getting it as an *ephemeral source port*. moon then
//!    exits on `EADDRINUSE`.
//! 2. **Dead-server blind poll** — harnesses polled `connect()` for up to
//!    30s without ever checking whether the child was still alive, so a
//!    bind failure surfaced as `server never accepted on port N:
//!    Connection refused` half a minute later, with the real error sitting
//!    unread in the server's stderr log.
//!
//! `reserve_port` kills the intra-process collision (dedup set); external
//! ephemeral-port steals cannot be prevented, so `spawn_listening` detects
//! the resulting child death *immediately* and respawns on a fresh port.
//!
//! Not every helper is used by every suite that includes this module.
#![allow(dead_code)]

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Child;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// Reserve a port that no other call in THIS process has handed out.
///
/// Binds `:0`, records the kernel-chosen port in a process-wide dedup set,
/// and retries until it gets a never-before-returned one. The listener is
/// still dropped before returning (the server must be able to bind), so
/// this alone does not stop *external* steals — pair it with
/// [`spawn_listening`].
pub fn reserve_port() -> u16 {
    static HANDED_OUT: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
    loop {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0 probe");
        let port = probe.local_addr().expect("probe local_addr").port();
        drop(probe);
        if port >= 20000 && HANDED_OUT.lock().unwrap().insert(port) {
            return port;
        }
    }
}

/// How long `spawn_listening` waits for one spawn attempt to accept.
///
/// Covers moon's bootstrap→per-shard SO_REUSEPORT listener handover plus a
/// debug-build startup on a loaded CI box. Attempts where the child DIES
/// are abandoned immediately, so the worst case is `attempts ×` this only
/// when a wedged-but-alive server never listens (a real bug worth the wait).
const ACCEPT_DEADLINE: Duration = Duration::from_secs(30);

/// Spawn a server via `spawn(port)` and wait until it ACCEPTS a TCP
/// connection on that port, respawning on a fresh [`reserve_port`] if the
/// child exits first (the lost-the-bind-race case).
///
/// Returns the live child and the port it is actually serving on. The
/// caller still owns protocol-level readiness (PING/AUTH/etc.) — a
/// successful `connect` here means "listening", nothing more. Panics after
/// three consecutive dead children (something is wrong beyond a port race:
/// read the server's stderr log) or if a live child never accepts within
/// [`ACCEPT_DEADLINE`].
pub fn spawn_listening(mut spawn: impl FnMut(u16) -> Child) -> (Child, u16) {
    const ATTEMPTS: usize = 3;
    for attempt in 1..=ATTEMPTS {
        let port = reserve_port();
        let mut child = spawn(port);
        let start = Instant::now();
        loop {
            if TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                Duration::from_millis(200),
            )
            .is_ok()
            {
                return (child, port);
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    // Lost the bind race (or crashed at startup): give the
                    // next attempt a fresh port instead of polling a corpse.
                    eprintln!(
                        "spawn_listening: child exited {status} before accepting on \
                         port {port} (attempt {attempt}/{ATTEMPTS}) — respawning"
                    );
                    break;
                }
                Ok(None) => {}
                Err(e) => panic!("spawn_listening: try_wait failed: {e}"),
            }
            assert!(
                start.elapsed() < ACCEPT_DEADLINE,
                "spawn_listening: live child never accepted on port {port} within \
                 {ACCEPT_DEADLINE:?} — check the server log in the test's --dir"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = child.wait();
    }
    panic!(
        "spawn_listening: {ATTEMPTS} consecutive children exited before accepting — \
         not a port race; read the server stderr log in the test's --dir"
    );
}

// ---------------------------------------------------------------------------
// Kernel M3 / G1 (task #18 follow-up): consolidated binary-resolution + kill
// helpers. Every crash suite used to carry its own copy of these three
// functions (grepped: `crash_recovery_graph_durability.rs`,
// `crash_recovery_wal_recycle_legacy.rs`, `crash_recovery_vector_durability.rs`
// duplicated all three; `crash_recovery_mq_effects.rs` /
// `crash_recovery_temporal_mq.rs` already used `spawn_listening` above but
// still kept a local `find_moon_binary`). Consolidated here as the mechanical,
// behavior-preserving union of every variant found (MOON_BIN override with a
// non-empty guard, then the compile-time `CARGO_BIN_EXE_moon` path Cargo sets
// for this test binary, then `target/{release,debug}/moon` as a last resort
// for ad-hoc `cargo test --test <name>` invocations outside the normal
// harness). `tests/aof_multidb_kill9.rs`'s `moon_bin()` returns a bare
// `./target/release/moon` default with NO binary-existence check and no
// `CARGO_BIN_EXE_moon` fallback — a real behavioral difference, so it is
// deliberately left alone here rather than force-migrated (see the kernel M3
// brief, Stage 1, decision #5: "if any suite needs behavioral adaptation,
// skip it there and note it for a follow-up task instead").
// ---------------------------------------------------------------------------

/// Resolve the `moon` server binary for crash/integration suites.
///
/// Precedence: `MOON_BIN` env var (only if non-empty and the path exists) →
/// `CARGO_BIN_EXE_moon` (the exact binary Cargo built for THIS test run —
/// right profile, right `CARGO_TARGET_DIR`, right `.exe` suffix on Windows)
/// → `target/release/moon` → `target/debug/moon`. Panics with a actionable
/// message if none resolve.
pub fn find_moon_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN")
        && !bin.trim().is_empty()
    {
        let p = PathBuf::from(&bin);
        if p.exists() {
            return p;
        }
    }
    let cargo_bin = PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    if cargo_bin.exists() {
        return cargo_bin;
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let release = PathBuf::from(format!("{manifest}/target/release/moon"));
    if release.exists() {
        return release;
    }
    let debug = PathBuf::from(format!("{manifest}/target/debug/moon"));
    if debug.exists() {
        return debug;
    }
    panic!(
        "No moon binary found. Build with `cargo build --release` or set \
         MOON_BIN=/path/to/moon."
    );
}

/// SIGKILL a spawned child and reap it (never SIGTERM — SIGTERM +
/// SO_REUSEPORT is a documented hang, see CLAUDE.md / the harness-speed
/// gotcha ledger). `Child::kill()` is documented to send SIGKILL on Unix,
/// so no raw `libc::kill` (and no `unsafe`, no cfg split) is needed.
pub fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Wait until nothing is accepting on `port` — required before a same-port
/// restart, or the new listener can race the dying process's socket
/// teardown. moon binds `SO_REUSEPORT` per shard, so a plain bind-based
/// check is useless here; two consecutive refused connects is the signal.
pub fn wait_for_port_down(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let mut consecutive_refused = 0;
    for _ in 0..120 {
        match TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(100)) {
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
    // No silent-pass verification helpers in a tripwire codebase (review
    // round 3, P2): a caller that proceeds to bind the SAME port after this
    // returns without the port actually being down races the dying
    // process's socket teardown, which can manifest as a flaky bind failure
    // or — worse — a same-port respawn silently talking to the wrong
    // (still-dying) process. Loop exhaustion after 120 iterations (~18s
    // worst case) is not a benign timeout here; it means the port never
    // went down and every caller's assumption is false.
    panic!(
        "wait_for_port_down: port {port} never stopped accepting connections \
         after 120 poll iterations (~18s) — the old process may still be \
         alive, or SO_REUSEPORT is masking a listener that never exited"
    );
}

// ---------------------------------------------------------------------------
// Raw-RESP client
// ---------------------------------------------------------------------------
//
// Suites that must control exactly what goes into one TCP write (pipeline
// ordering, script routing) cannot use a client library: a library is free to
// split or reorder the batch, which is the very thing under test.
//
// The framer here replaced a "read until the socket is quiet for 250ms"
// heuristic. That silently TRUNCATED a reply whenever the server paused longer
// than the window mid-stream, and a short read then surfaced as a wrong VALUE
// — so the suite reported the wrong defect. Do not reintroduce a timing-based
// reader.

pub struct Conn {
    pub sock: TcpStream,
    /// Bytes read from the socket but not yet consumed by a reply. A pipelined
    /// reply stream arrives in arbitrary chunks, so a read can overshoot the
    /// replies asked for; keeping the remainder here stops the next call from
    /// mistaking it for its own reply.
    spill: Vec<u8>,
}

pub fn encode(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
    }
    out
}

/// Bytes consumed by exactly `want` complete top-level RESP replies at the
/// start of `buf`, or `None` when `buf` does not hold that many yet.
///
/// This exists because the obvious harness — "read until the socket goes quiet
/// for 250ms" — silently TRUNCATES a reply whenever the server pauses longer
/// than that mid-stream, and then the test reports a wrong VALUE rather than a
/// short READ. The fix under test makes such pauses more likely, not less:
/// every deferral adds a shard dispatch/await boundary inside a single batch's
/// reply stream. Counting frames removes the timing assumption entirely.
///
/// `pending` counts array elements still outstanding: an item read while
/// `pending > 0` is an ELEMENT of an array already counted, not a reply of its
/// own. Nested arrays work because their children add to the same counter.
pub fn framed_len(buf: &[u8], want: usize) -> Option<usize> {
    let mut i = 0usize;
    let mut done = 0usize;
    let mut pending = 0usize;
    while done < want || pending > 0 {
        let tag = *buf.get(i)?;
        let end = (i..buf.len().checked_sub(1)?).find(|&j| &buf[j..j + 2] == b"\r\n")?;
        let line = std::str::from_utf8(&buf[i + 1..end]).ok()?;
        i = end + 2;

        // RESP3 attribute (`|N`): N key/value pairs of metadata attached to the
        // reply that FOLLOWS. It is not a reply of its own, and not an element
        // of an enclosing aggregate — so it must consume neither a `done` nor a
        // `pending` slot, or the attributed reply is mistaken for the reply
        // itself and every later frame is read one position out of step.
        if tag == b'|' {
            let n: i64 = line.parse().ok()?;
            if n > 0 {
                pending += (n as usize) * 2;
            }
            continue;
        }

        if pending > 0 {
            pending -= 1;
        } else {
            done += 1;
        }

        match tag {
            // Bulk-ish: a length header followed by that many bytes + CRLF.
            // A negative length is a null and carries no payload.
            b'$' | b'=' | b'!' => {
                let n: i64 = line.parse().ok()?;
                if n >= 0 {
                    i = i.checked_add(n as usize + 2)?;
                    if buf.len() < i {
                        return None;
                    }
                }
            }
            // Aggregates. A map's declared length counts PAIRS.
            b'*' | b'~' | b'>' => {
                let n: i64 = line.parse().ok()?;
                if n > 0 {
                    pending += n as usize;
                }
            }
            b'%' => {
                let n: i64 = line.parse().ok()?;
                if n > 0 {
                    pending += (n as usize) * 2;
                }
            }
            // Single-line: +simple, -error, :int, ,double, #bool, (bignum.
            _ => {}
        }
    }
    Some(i)
}

impl Conn {
    pub fn open(port: u16) -> Self {
        let sock = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        sock.set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        Conn {
            sock,
            spill: Vec::new(),
        }
    }

    /// Send several commands as ONE write — the whole point of the test. The
    /// server must not be able to tell this from any other batch, and must
    /// execute it in order.
    pub fn pipeline(&mut self, cmds: &[&[&str]]) -> String {
        let mut out = Vec::new();
        for c in cmds {
            out.extend_from_slice(&encode(c));
        }
        self.sock.write_all(&out).expect("write");
        self.read_replies(cmds.len())
    }

    pub fn send(&mut self, parts: &[&str]) -> String {
        self.sock.write_all(&encode(parts)).expect("write");
        self.read_replies(1)
    }

    /// Read until exactly `want` complete top-level replies have arrived.
    ///
    /// Panics rather than returning short: a truncated read surfacing as a
    /// wrong value is the failure mode that would make this suite lie about
    /// which defect it caught.
    pub fn read_replies(&mut self, want: usize) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut chunk = [0u8; 65536];
        loop {
            if let Some(n) = framed_len(&self.spill, want) {
                let reply = String::from_utf8_lossy(&self.spill[..n]).into_owned();
                self.spill.drain(..n);
                return reply;
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for {want} replies; got {} bytes: {:?}",
                    self.spill.len(),
                    String::from_utf8_lossy(&self.spill)
                );
            }
            match self.sock.read(&mut chunk) {
                Ok(0) => panic!(
                    "server closed after {} bytes while {want} replies were expected: {:?}",
                    self.spill.len(),
                    String::from_utf8_lossy(&self.spill)
                ),
                Ok(n) => self.spill.extend_from_slice(&chunk[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(e) => panic!("read failed after {} bytes: {e}", self.spill.len()),
            }
        }
    }
}
