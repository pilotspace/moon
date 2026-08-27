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

/// Reserve a port no other call in this process — **or any other test process
/// on this machine** — has handed out.
///
/// Binds `:0`, records the kernel-chosen port in a process-wide dedup set,
/// then claims it across processes with [`claim_across_processes`]. The
/// listener is dropped before returning, because the server must be able to
/// bind it; pair this with [`spawn_listening`], which verifies the child
/// actually accepts.
pub fn reserve_port() -> u16 {
    loop {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0 probe");
        let port = probe.local_addr().expect("probe local_addr").port();
        drop(probe);
        if port >= 20000 && HANDED_OUT.lock().unwrap().insert(port) && claim_across_processes(port)
        {
            return port;
        }
    }
}

/// Locks held for every port this process has claimed, kept alive for the
/// whole run.
///
/// Dropping one would release the port back to a sibling binary while our
/// server is still on it, which is the collision this exists to prevent — so
/// they are deliberately never removed.
static PORT_LOCKS: LazyLock<Mutex<Vec<moon::persistence::dir_lock::DirLock>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Claim `port` against every other moon test process on this machine.
///
/// [`HANDED_OUT`] closes collisions inside ONE test binary. Cargo runs test
/// binaries **concurrently**, so that is only half the problem, and the other
/// half cannot be closed by any reserve-then-bind scheme: the probe listener
/// must be dropped before the server can bind, because a held plain
/// `TcpListener` is exactly what stops a `SO_REUSEPORT` bind.
///
/// The consequence is not a loud failure. moon's client listeners use
/// `SO_REUSEPORT`, so a second server on a taken port binds **successfully** —
/// both processes stay alive and the kernel splits or hijacks the connections,
/// with no bind error, no log line and no panic. That is moon#489, and
/// moon#365's `ConnectionReset` in `crash_matrix_cross_plane` has the same
/// signature in a suite that already uses this helper.
///
/// A filesystem lock closes it, because it does not need to hold the port
/// itself. `dir_lock::acquire` takes an exclusive, non-blocking `flock` and the
/// kernel releases it when the holder dies — **including `SIGKILL`**, which the
/// crash-matrix suites do on purpose. A test binary that is killed mid-run
/// therefore frees its ports without a cleanup step to forget.
///
/// The lock DIRECTORIES persist after the lock is released — they are empty
/// and bounded by the number of distinct ports ever used, and `$TMPDIR` is the
/// OS's to reap, so nothing here removes them. Removing a directory another
/// process is mid-`acquire` on is how you turn a flake guard into a flake.
///
/// Returns false when another process holds the port, so the caller tries the
/// next one. Any other failure (an unwritable `TMPDIR`, or Windows, where
/// `dir_lock` is a documented no-op) returns TRUE: this is a flake guard, and
/// one that refuses to hand out ports is worse than the flake.
fn claim_across_processes(port: u16) -> bool {
    let dir = std::env::temp_dir()
        .join("moon-test-ports")
        .join(port.to_string());
    if std::fs::create_dir_all(&dir).is_err() {
        return true;
    }
    match moon::persistence::dir_lock::acquire(&dir) {
        Ok(lock) => {
            PORT_LOCKS.lock().unwrap().push(lock);
            true
        }
        Err(moon::persistence::dir_lock::DirLockError::Held { .. }) => false,
        Err(_) => true,
    }
}

/// Every port number this process has handed out, of EITHER kind.
///
/// Shared between [`reserve_port`] and [`reserve_cluster_port`] on purpose: a
/// cluster node occupies two numbers (`p` and its bus sibling `p + 10000`), and
/// a plain server that later drew `p + 10000` from the ephemeral range would
/// collide with a bus nobody had recorded. One set, both kinds.
static HANDED_OUT: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Client ports for cluster nodes are drawn from `[20000, 22700)`, so their bus
/// siblings land in `[30000, 32700)`. Three properties, all deliberate: the two
/// spaces never overlap each other; both sit BELOW Linux's default ephemeral
/// floor of 32768 and macOS's 49152, so neither can be stolen by an OS-assigned
/// port from [`reserve_port`] or from any other process; and `+ 10000` cannot
/// overflow `u16`.
const CLUSTER_PORT_LOW: u16 = 20000;
const CLUSTER_PORT_HIGH: u16 = 22700;
/// How far apart consecutive reservations start scanning. Purely a spreading
/// device: the dedup set is what makes overlap impossible, so a scan that runs
/// past its own stride into the next one is safe — every port another
/// reservation took is already claimed and gets skipped.
const CLUSTER_STRIDE: u16 = 50;

/// Reserve a cluster-node port whose bus sibling (`port + 10000`) is also free.
///
/// Cluster mode binds `port + 10000` for the bus unconditionally, so an
/// OS-assigned ephemeral port is unusable: those start at 49152 on macOS and
/// 32768 on Linux, where `+ 10000` overflows past 65535. This scans an explicit
/// low window instead.
///
/// Both numbers are recorded in [`HANDED_OUT`] before the bind probe, so no two
/// reservations in this process — of either kind — can ever pick the same port.
/// That closes the dominant half of moon#505: the suites that flaked run their
/// tests in parallel THREADS of one binary, and the per-suite helpers this
/// replaces scanned `start..40000` unbounded, so a test whose own window was
/// busy walked straight into its neighbour's window by design.
///
/// The scan is bounded to that range and panics when it is exhausted, rather
/// than the old helpers' `start..40000` wander. Cross-PROCESS collisions cannot
/// be closed by any reservation scheme — the probe listener must be dropped
/// before the server can bind — so pair this with [`spawn_listening_cluster`],
/// which detects the loser.
pub fn reserve_cluster_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    let span = CLUSTER_PORT_HIGH - CLUSTER_PORT_LOW;
    let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Spread the starting point by pid so concurrent test BINARIES do not all
    // begin at the bottom of the range and race each other down it.
    let offset = (std::process::id() as u16)
        .wrapping_mul(CLUSTER_STRIDE)
        .wrapping_add(seq.wrapping_mul(CLUSTER_STRIDE))
        % span;

    for i in 0..span {
        let candidate = CLUSTER_PORT_LOW + (offset + i) % span;
        let bus = candidate + 10000;
        {
            // Claim both numbers, or skip. A candidate that fails the bind
            // probe below stays claimed deliberately: it is occupied by
            // something outside this process, and no later reservation here
            // should waste a probe on it.
            let mut handed = HANDED_OUT.lock().unwrap();
            if handed.contains(&candidate) || handed.contains(&bus) {
                continue;
            }
            handed.insert(candidate);
            handed.insert(bus);
        }
        let Ok(client_probe) = std::net::TcpListener::bind(("127.0.0.1", candidate)) else {
            continue;
        };
        let Ok(bus_probe) = std::net::TcpListener::bind(("127.0.0.1", bus)) else {
            continue;
        };
        drop(bus_probe);
        drop(client_probe);
        // Both numbers, or neither: a pair whose bus is claimed by another
        // process is unusable even when its client port is free, and the node
        // would die on the bus bind. The client claim is deliberately not
        // released on that path — this process keeps it and moves on, which
        // costs one port out of 2700 and avoids a release/re-claim race.
        if claim_across_processes(candidate) && claim_across_processes(bus) {
            return candidate;
        }
        continue;
    }
    panic!(
        "reserve_cluster_port: no free port pair in [{CLUSTER_PORT_LOW}, \
         {CLUSTER_PORT_HIGH}) — either this process leaked servers or the \
         machine is saturated below {}",
        CLUSTER_PORT_HIGH + 10000
    );
}

/// How long `spawn_listening` waits for one spawn attempt to accept.
///
/// Covers moon's bootstrap→per-shard SO_REUSEPORT listener handover plus a
/// debug-build startup on a loaded CI box. Attempts where the child DIES
/// are abandoned immediately, so the worst case is `attempts ×` this only
/// when a wedged-but-alive server never listens (a real bug worth the wait).
const ACCEPT_DEADLINE: Duration = Duration::from_secs(30);

/// A `$TMPDIR` path that is unique across processes AND threads.
///
/// Suites used to build their own from `pid` + `SystemTime` nanos. That is
/// not unique within one test binary: on macOS `SystemTime::now()` carries
/// only MICROSECOND resolution (every nanos value it returns ends in `000`),
/// so two `#[test]` threads that call it inside the same microsecond get the
/// SAME path. Both servers then receive the same `--dir`, moon's instance
/// flock correctly refuses the second, and it exits 1 before ever accepting.
///
/// The symptom is maximally misleading: `spawn_listening` reports
/// `3 consecutive children exited before accepting — not a port race`
/// (correct — it is a *directory* race), the closure it retries captured the
/// colliding dir so all three attempts fail identically, and the winning
/// test's `Drop` deletes the shared dir, so no evidence survives. Measured
/// at 2 failures / 60 runs of `lua_vm_memory_published` under 6-way load on
/// a macOS host; the two tests printed one identical dir on the failing run.
///
/// The per-process counter is what makes this sound — it cannot collide no
/// matter what the clock's resolution turns out to be. pid and the timestamp
/// stay in the name only so a leftover dir is still attributable to a run.
pub fn unique_test_dir(prefix: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}-{seq}", std::process::id()))
}

/// stderr sink for a spawned `moon`, landing in the test's own `--dir`.
///
/// `spawn_listening`'s give-up panic instructs the reader to open "the server
/// stderr log in the test's --dir". A suite that spawns with
/// `Stdio::null()` makes that instruction unfollowable: the one artifact that
/// distinguishes a bind race from a config rejection is thrown away, and the
/// failure is only reproducible under full-suite load. Use this instead.
///
/// Opens in **append** mode on purpose — the closure passed to
/// `spawn_listening` runs once per respawn attempt, and truncating would
/// leave only the last attempt's reason.
///
/// Falls back to `Stdio::null()` if the log cannot be opened, so a
/// read-only or full `--dir` degrades to today's behaviour rather than
/// panicking inside the spawn closure.
pub fn server_stderr(dir: &std::path::Path) -> std::process::Stdio {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.err"))
    {
        Ok(f) => std::process::Stdio::from(f),
        Err(_) => std::process::Stdio::null(),
    }
}

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
pub fn spawn_listening(spawn: impl FnMut(u16) -> Child) -> (Child, u16) {
    spawn_listening_inner(reserve_port, spawn, Duration::ZERO)
}

/// How long a cluster node must stay alive AFTER its client port accepts
/// before it is trusted.
///
/// moon's client listeners use `SO_REUSEPORT`, so a second node on an
/// already-taken client port binds SUCCESSFULLY and the kernel splits incoming
/// connections between the two — the collision leaves no bind error, no log and
/// no panic, only a client that intermittently reaches the wrong node and gets
/// reset (moon#505). The one observable it does leave is the cluster BUS: that
/// binds plainly, and `main.rs` exits(1) on `EADDRINUSE` by design ("a cluster
/// node without its bus is invisible to every peer"). The loser therefore dies
/// on its own, shortly after start-up — this window is how long we watch for
/// that before handing the node to the test.
///
/// The window is load-bearing, not decorative: because the WINNER is already
/// accepting on the shared port, `TcpStream::connect` succeeds instantly and
/// the plain accept-wait above would hand back a child that is in the middle of
/// dying. Measured on macOS against a release build, the loser lives **79 ms**
/// from spawn to exit, so 300 ms carries a wide margin for a debug build on a
/// loaded box.
///
/// It is still a bounded heuristic, not a proof: the bus binds on the
/// `cluster-ctl` thread concurrently with the listener coming up, so nothing
/// orders "client port accepts" against "bus bind has been attempted". The
/// in-process guarantee comes from [`reserve_cluster_port`]'s dedup set; this
/// only narrows the cross-process residue.
const CLUSTER_BUS_SETTLE: Duration = Duration::from_millis(300);

/// [`spawn_listening`] for CLUSTER nodes: ports come from
/// [`reserve_cluster_port`] so the bus sibling is free too, and readiness
/// additionally requires the child to survive [`CLUSTER_BUS_SETTLE`].
pub fn spawn_listening_cluster(spawn: impl FnMut(u16) -> Child) -> (Child, u16) {
    spawn_listening_inner(reserve_cluster_port, spawn, CLUSTER_BUS_SETTLE)
}

fn spawn_listening_inner(
    mut reserve: impl FnMut() -> u16,
    mut spawn: impl FnMut(u16) -> Child,
    settle: Duration,
) -> (Child, u16) {
    const ATTEMPTS: usize = 3;
    for attempt in 1..=ATTEMPTS {
        let port = reserve();
        let mut child = spawn(port);
        let start = Instant::now();
        loop {
            if TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                Duration::from_millis(200),
            )
            .is_ok()
            {
                match settled(&mut child, settle) {
                    Ok(()) => return (child, port),
                    Err(status) => {
                        eprintln!(
                            "spawn_listening: child exited {status} within {settle:?} of \
                             accepting on port {port} (attempt {attempt}/{ATTEMPTS}) — its \
                             cluster bus almost certainly lost port {} to another node; \
                             respawning",
                            port.wrapping_add(10000)
                        );
                        break;
                    }
                }
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
        "spawn_listening: {ATTEMPTS} consecutive children exited before accepting. \
         Each attempt used a fresh port, so this is not a port race. Two things \
         look like this: (a) the server rejected its arguments or environment — \
         spawn with `common::server_stderr(dir)` and read `server.err` in the \
         test's --dir, which carries moon's own `Error: ...` line; (b) two tests \
         in this binary were handed the SAME --dir and moon's instance flock \
         refused the second — build the path with `common::unique_test_dir` \
         rather than pid+timestamp (moon#741)"
    );
}

/// Watch `child` for `settle`; `Err(status)` if it exits inside the window.
fn settled(child: &mut Child, settle: Duration) -> Result<(), std::process::ExitStatus> {
    let until = Instant::now() + settle;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Err(status),
            Ok(None) => {}
            // A child we cannot poll is not a port race — let the caller have
            // it and fail on a real assertion instead of guessing here.
            Err(_) => return Ok(()),
        }
        if Instant::now() >= until {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
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

/// Owns a spawned server and reaps it on drop — including when the scope is
/// left by an unwinding panic (moon#713).
///
/// The pattern this replaces puts the kill on the last line of the test body:
///
/// ```ignore
/// let mut child = spawn(port);
/// assert_eq!(get(port, "k"), "v");   // <- fires
/// common::sigkill(&mut child);       // <- never runs; the unwind goes past it
/// ```
///
/// The orphan is not merely untidy. Measured 2026-08-25 after one afternoon of
/// deliberately-failing runs of a single suite: five orphaned servers, ~170%
/// CPU each (834% combined) for 7.5 hours, answering nothing on their ports and
/// spending the time in syscalls (13:42 system vs 0:09 user on one thread).
/// Every wall-clock-sensitive test that ran afterwards ran on a machine under
/// invisible load.
///
/// SIGKILL, not a graceful shutdown: these are throwaway data dirs, and a hung
/// server — often the very thing under test — would ignore a polite request.
pub struct ServerGuard {
    child: Option<Child>,
    pid: u32,
}

impl ServerGuard {
    pub fn new(child: Child) -> Self {
        let pid = child.id();
        Self {
            child: Some(child),
            pid,
        }
    }

    /// The server's pid, still readable after the child has been reaped or
    /// taken — assertions about orphans must name the PROCESS, since a dead
    /// server's port frees up either way.
    pub fn id(&self) -> u32 {
        self.pid
    }

    /// Borrow the child for the things only a `Child` can answer:
    /// `try_wait`, custom readiness loops, stdio handles.
    pub fn as_mut(&mut self) -> &mut Child {
        self.child
            .as_mut()
            .expect("server child was already taken or reaped")
    }

    /// Reap now, for a test that needs the server GONE rather than merely
    /// doomed — a same-dir restart, or a lock the next server must acquire.
    ///
    /// Idempotent by construction: crash-recovery suites SIGKILL on purpose and
    /// then restart, so "already reaped" is a normal state. A second call must
    /// not re-`kill` a pid the OS is free to have recycled.
    pub fn kill_now(&mut self) {
        if let Some(mut child) = self.child.take() {
            sigkill(&mut child);
        }
    }

    /// Hand the raw child back, transferring the duty to reap it. The guard
    /// keeps the pid for assertions but will not touch the process again.
    pub fn take(&mut self) -> Option<Child> {
        self.child.take()
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.kill_now();
    }
}

/// [`spawn_listening`] that hands back a [`ServerGuard`] instead of a bare
/// `Child`, so a panic between here and the explicit kill cannot orphan the
/// server.
/// Wait until `port` accepts a TCP connection, for suites that run the server
/// **in-process** (`listener::run_sharded` / `run_with_shutdown` on a spawned
/// thread) rather than as a child process.
///
/// `spawn_listening_guarded` cannot serve these: it watches a `Child` exit
/// status, and an in-process listener has no child to watch. Before moon#752
/// they instead slept a fixed 250ms and hoped — which fails as
/// `Connection refused` whenever startup is slower than the guess (a loaded
/// machine, a cold cache, high suite parallelism). A deadline is an upper
/// bound that is rarely reached, not the wait itself: this returns as soon as
/// the port answers, so the common case is *faster* than the old sleep.
///
/// Uses `std::net::TcpStream` deliberately — a refused connect on loopback
/// returns immediately, and it keeps the helper free of any `tokio/net`
/// feature requirement in dev-dependencies.
pub async fn await_listening(port: u16, deadline: std::time::Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    let mut attempts = 0u32;
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        attempts += 1;
        if start.elapsed() >= deadline {
            return Err(format!(
                "port {port} never accepted a connection within {deadline:?} \
                 ({attempts} attempts); the in-process server failed to start"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

pub fn spawn_listening_guarded(spawn: impl FnMut(u16) -> Child) -> (ServerGuard, u16) {
    let (child, port) = spawn_listening(spawn);
    (ServerGuard::new(child), port)
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
