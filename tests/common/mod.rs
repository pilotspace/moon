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
use std::net::TcpStream;
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
