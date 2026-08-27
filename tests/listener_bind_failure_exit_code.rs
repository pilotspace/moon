//! A server that never bound its port must NOT exit 0 (moon#751).
//!
//! `main()` ran the listener like this:
//!
//! ```ignore
//! if let Err(e) = server::listener::run_sharded(...).await {
//!     tracing::error!("Listener error: {}", e);   // logged, not propagated
//! }
//! ...
//! info!("Server shut down");
//! Ok(())                                          // <-- exit 0, always
//! ```
//!
//! So a fatal bind failure produced exactly the same observable as a clean
//! SIGTERM shutdown: shard-shutdown lines, "Server shut down", and **exit 0**.
//! A supervisor cannot tell the difference. Under systemd `Restart=on-failure`
//! a server that never accepted a single connection is never restarted, and an
//! orchestrator records a successful run.
//!
//! ## Why this test exists
//!
//! moon#751 reported a flaky test whose server "shut down gracefully during
//! startup, before readiness", and reasoned: exit status is 0, therefore the
//! graceful path ran, therefore the process *was asked* to shut down. That
//! inference does not hold — the error path also exits 0 — and it sent the
//! investigation looking for a phantom SIGTERM sender. Pinning the exit code
//! is what makes the two cases distinguishable at all.
//!
//! The bind failure here is an unroutable TEST-NET-3 address (RFC 5737), which
//! fails with `EADDRNOTAVAIL` for root and non-root alike. A privileged-port
//! probe would silently *succeed* in a container running as root and quietly
//! stop testing anything.

use std::process::Command;
use std::time::{Duration, Instant};

mod common;

/// RFC 5737 TEST-NET-3. Guaranteed not to be a local interface address, so
/// `bind()` fails with `EADDRNOTAVAIL` on every platform this test runs on.
const UNROUTABLE: &str = "203.0.113.7";

#[test]
fn fatal_bind_failure_does_not_exit_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = common::reserve_port();

    let mut child = Command::new(common::find_moon_binary())
        .args([
            "--bind",
            UNROUTABLE,
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir.path())
        .stdout(std::fs::File::create(dir.path().join("out.log")).expect("stdout"))
        .stderr(std::fs::File::create(dir.path().join("err.log")).expect("stderr"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` first)");

    // It must exit on its own: there is nothing to serve. Bound the wait so a
    // regression that leaves it running fails here instead of hanging CI.
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(s) => break s,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "moon kept running for 30s despite being unable to bind {UNROUTABLE}:{port}; \
                     it should have failed fast"
                );
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };

    let out = std::fs::read_to_string(dir.path().join("out.log")).unwrap_or_default();
    let err = std::fs::read_to_string(dir.path().join("err.log")).unwrap_or_default();
    let logs = format!("{out}{err}");

    // Precondition: this must fail for the reason the test intends. Without
    // this, a build that silently bound somewhere else would "pass".
    assert!(
        logs.contains("Listener error"),
        "expected a listener bind failure, but the server never logged one — \
         this test is not exercising what it claims.\n--- logs ---\n{logs}"
    );

    // A signal death reports `code() == None`; the graceful path reports
    // `Some(0)`. Neither is acceptable here: the server failed.
    assert!(
        !status.success(),
        "moon exited {status:?} after failing to bind {UNROUTABLE}:{port}. \
         A server that never bound must report failure, or a supervisor \
         (systemd Restart=on-failure, an orchestrator, a test's readiness \
         probe) reads a dead server as a clean stop.\n--- logs ---\n{logs}"
    );
}

/// The other half: a server that binds fine and is then asked to stop must
/// still exit 0. Without this, "make bind failures non-zero" could be
/// satisfied by making *everything* non-zero, which would break every
/// supervisor and the SIGTERM contract in `tests/sigterm_shutdown.rs`.
#[test]
fn clean_shutdown_still_exits_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn moon")
    });

    let pid = guard.id();
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("kill");
    assert!(status.success(), "kill -TERM failed on port {port}");

    let deadline = Instant::now() + Duration::from_secs(30);
    let exit = loop {
        match guard.as_mut().try_wait().expect("try_wait") {
            Some(s) => break s,
            None if Instant::now() >= deadline => {
                panic!("moon did not exit within 30s of SIGTERM on port {port}")
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };

    assert_eq!(
        exit.code(),
        Some(0),
        "a SIGTERM'd server that bound successfully must still exit 0, got {exit:?}"
    );
}
