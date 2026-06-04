//! MULTISHARD-SERVE-SMOKE: the server must actually SERVE commands in multishard
//! mode on every supported runtime/platform.
//!
//! Regression guard for the tokio-on-Linux multishard accept hang (2026-06-03).
//! Root cause: the central tokio listener plain-bound the port (NO SO_REUSEPORT)
//! while each shard binds its own SO_REUSEPORT listener — incompatible sockets on
//! the same port, decided by a bind-order RACE. Whichever way it resolved, the
//! server stopped serving: if the shards won the race the central plain-bind hit
//! EADDRINUSE and `run_sharded` returned Err, tearing down the WHOLE server
//! ("Server shut down"); if the central won, the shards' REUSEPORT binds failed
//! and the accept path was left half-wired. Either way `PING` never got a reply —
//! the "zombie eating RAM" signature (port up, shards parked, nothing dispatched).
//! Fix: the central listener also binds SO_REUSEPORT (server/listener.rs), so it
//! COEXISTS with the shard sockets regardless of order; the kernel load-balances
//! accepts across all of them and every one of them actively accepts.
//!
//! This test boots the real binary in multishard configs and asserts a raw-TCP
//! inline `PING` is answered with `+PONG` inside a hard deadline. It uses
//! `CARGO_BIN_EXE_moon` (cargo auto-builds the bin) and raw TCP (no redis-cli
//! dependency) so it runs unmodified in CI and fails RED — not hangs — on the bug.
//!
//! NOT `#[ignore]`: this must run in the default `cargo test` CI matrix on BOTH
//! `runtime-tokio` and `runtime-monoio` so the accept path can never silently
//! regress again.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// Pick an ephemeral port by binding :0 and releasing it. Good enough for a
/// short-lived test; the server rebinds immediately.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

fn spawn_moon(port: u16, dir: &std::path::Path, extra: &[&str]) -> Child {
    let mut args: Vec<String> = vec![
        "--port".into(),
        port.to_string(),
        "--dir".into(),
        dir.to_string_lossy().into_owned(),
    ];
    for e in extra {
        args.push((*e).into());
    }
    Command::new(moon_binary())
        .args(&args)
        // Pipe to a log file so a CI failure has a diagnostic (never Stdio::null()).
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
        .spawn()
        .expect("spawn moon (CARGO_BIN_EXE_moon)")
}

/// Connect + inline PING, return true iff we read `+PONG` within `deadline`.
/// Distinguishes "not started yet" (connect refused → retry) from "accepted TCP
/// but no reply" (the hang → keep trying until the deadline, then false).
fn ping_ok(port: u16, deadline: Duration) -> bool {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    while start.elapsed() < deadline {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(mut s) => {
                s.set_read_timeout(Some(Duration::from_millis(500))).ok();
                s.set_write_timeout(Some(Duration::from_millis(500))).ok();
                if s.write_all(b"PING\r\n").is_ok() {
                    let mut buf = [0u8; 16];
                    if let Ok(n) = s.read(&mut buf)
                        && n > 0
                        && buf[..n].windows(4).any(|w| w == b"PONG")
                    {
                        return true;
                    }
                }
                // Connected but no PONG (the hang) — drop, brief wait, retry.
                std::thread::sleep(Duration::from_millis(150));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    false
}

/// Shard count for the multishard cases. Capped at the number of available
/// cores so the test cannot oversubscribe the runtime on a small CI runner:
/// BOTH runtimes (thread-per-core) collapse serving once shards >= cores (the
/// documented oversubscription wall — see CLAUDE.md gotchas), which is a
/// SEPARATE phenomenon from the bind race this test guards. Floor of 2 keeps it
/// genuinely multishard (central + >=2 shard SO_REUSEPORT sockets is the full
/// coexistence surface; higher counts add no race surface, only oversubscription
/// confound). 4 on the dev box, scales down on 2-core runners.
fn safe_shards() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .clamp(2, 4)
}

fn run_serves(label: &str, extra: &[&str]) {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!(
        "moon-serve-smoke-{}-{}-{}",
        std::process::id(),
        port,
        label,
    ));
    std::fs::create_dir_all(dir.join("off")).expect("mk dir");

    let mut child = spawn_moon(port, &dir, extra);
    // Hard deadline: a healthy server answers within ~1s; the hang never answers.
    let ok = ping_ok(port, Duration::from_secs(15));

    let _ = child.kill();
    let _ = child.wait();

    if ok {
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        ok,
        "[{label}] multishard server did not answer PING with +PONG within 15s — \
         the central listener / per-shard SO_REUSEPORT accept path is not serving. \
         Logs kept at {}",
        dir.display()
    );
}

// A small explicit --maxmemory is REQUIRED on every smoke case. With it unset,
// the memory guardrail auto-caps maxmemory at ~80% of host RAM, and each shard
// EAGERLY pre-allocates a PageCache frame pool sized to maxmemory/shards AT
// STARTUP (≈3.8 GB for a 4-shard server on a 6 GB box, before serving anything).
// That makes the "smoke" test a multi-GB monster that OOMs small CI runners. The
// bind race this test guards is bind-time / REUSEPORT-coexistence — entirely
// independent of maxmemory — so a 128 MB cap keeps the guard valid (~112 MB RSS)
// while keeping the test light. (PageCache eager pre-alloc is tracked separately.)
const SMOKE_MAXMEMORY: &str = "134217728"; // 128 MiB

#[test]
fn serves_multishard_plain() {
    let shards = safe_shards().to_string();
    run_serves(
        "multishard-plain",
        &[
            "--shards",
            &shards,
            "--appendonly",
            "no",
            "--maxmemory",
            SMOKE_MAXMEMORY,
        ],
    );
}

#[test]
fn serves_1shard_plain() {
    run_serves(
        "1shard-plain",
        &[
            "--shards",
            "1",
            "--appendonly",
            "no",
            "--maxmemory",
            SMOKE_MAXMEMORY,
        ],
    );
}

#[test]
fn serves_multishard_disk_offload() {
    // The exact surface the durability work targets: multishard + disk-offload +
    // appendonly=no. Must serve commands, not just bind the port.
    let shards = safe_shards().to_string();
    run_serves(
        "multishard-diskoffload",
        &[
            "--shards",
            &shards,
            "--maxmemory",
            "8388608",
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "enable",
            "--appendonly",
            "no",
        ],
    );
}
