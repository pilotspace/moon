//! moon#713: a test that fails must not leave its server running.
//!
//! `tests/loading_state_476.rs` produced five orphaned servers in one
//! afternoon of deliberately-failing runs — ~170% CPU each, 834% combined,
//! for 7.5 hours, answering nothing on their ports. The cause was structural,
//! not local to that file: the kill sits on the last line of the test body and
//! a failing `assert!` unwinds straight past it.
//!
//! 26 integration files spawn a real `moon` and none of them owned their child
//! through a `Drop`. This file pins the contract for the shared guard that
//! fixes them, so the guarantee is tested once rather than re-argued per file.

mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::{Command, Stdio};

/// Spawn a real moon on a reserved port. Deliberately not a helper shared with
/// the guard under test — a contract test that builds its subject with its
/// subject proves nothing.
fn spawn_moon(port: u16) -> std::process::Child {
    let dir = std::env::temp_dir().join(format!(
        "moon-guard-contract-{}-{}",
        std::process::id(),
        port
    ));
    std::fs::create_dir_all(&dir).expect("mkdir");
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--dir",
            dir.to_str().expect("utf8 dir"),
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

/// Is `pid` still a live process?
///
/// `ps`, not a connect() probe: a dead server's port frees up either way, so
/// connection-refused says nothing about whether anything is still running —
/// which is the entire failure mode under test. Unix-only for the same reason
/// `loading_state_476` is: `Command::new("ps")` cannot spawn on Windows, and a
/// probe that fails to RUN would report "not alive" and pass vacuously.
#[cfg(unix)]
fn is_alive(pid: u32) -> bool {
    let out = Command::new("ps")
        .args(["-p", &pid.to_string()])
        .output()
        .expect("`ps` must be available to probe process liveness");
    String::from_utf8_lossy(&out.stdout).lines().count() > 1
}

/// The contract: an unwinding panic reaps the server.
#[cfg(unix)]
#[test]
fn sg1_a_panic_reaps_the_server() {
    let port = common::reserve_port();
    let guard = common::ServerGuard::new(spawn_moon(port));
    let pid = guard.id();

    let outcome = catch_unwind(AssertUnwindSafe(move || {
        let _guard = guard;
        panic!("a test failing before its explicit kill");
    }));
    assert!(outcome.is_err(), "the panic must actually have happened");

    assert!(
        !is_alive(pid),
        "server pid {pid} survived a panicking test — this is how orphans that \
         spin at ~170% CPU for hours get created"
    );
}

/// An explicit kill mid-test must not fight the drop.
///
/// Crash-recovery suites SIGKILL their server on purpose and then restart one,
/// so "already killed" is a normal state, not an error. Reaping twice must be a
/// no-op rather than a second `kill` aimed at a pid the OS may have recycled.
#[cfg(unix)]
#[test]
fn sg2_explicit_kill_then_drop_is_not_a_double_kill() {
    let port = common::reserve_port();
    let mut guard = common::ServerGuard::new(spawn_moon(port));
    let pid = guard.id();

    guard.kill_now();
    assert!(!is_alive(pid), "kill_now must reap immediately");

    // Second reap through Drop. If the guard re-killed a taken child this
    // would be a kill aimed at a reaped pid.
    drop(guard);
    assert!(!is_alive(pid), "still dead after the drop");
}

/// A test that legitimately needs the raw `Child` back can take it, and then
/// owns the reaping — the guard must not also try.
#[cfg(unix)]
#[test]
fn sg3_taking_the_child_transfers_ownership() {
    let port = common::reserve_port();
    let mut guard = common::ServerGuard::new(spawn_moon(port));
    let pid = guard.id();

    let mut child = guard.take().expect("the child is present");
    drop(guard); // must NOT kill — ownership moved

    assert!(
        is_alive(pid),
        "taking the child must transfer ownership, not reap"
    );
    common::sigkill(&mut child);
    assert!(!is_alive(pid), "and the new owner can still reap it");
}
