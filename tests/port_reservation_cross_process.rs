//! `reserve_port` must hand out a port no OTHER test process can also take
//! (moon#489).
//!
//! The in-process dedup set was never the hard half. Cargo runs test binaries
//! concurrently, and the probe listener has to be dropped before the server can
//! bind — a held plain `TcpListener` is exactly what stops a `SO_REUSEPORT`
//! bind — so two binaries could pick the same number. moon's client listeners
//! use `SO_REUSEPORT`, so the second server then binds **successfully**: both
//! processes stay alive and the kernel splits or hijacks the connections, with
//! no bind error, no log line and no panic.
//!
//! The three exclusion cases are `#[cfg(unix)]` because the claim is built on
//! `moon::persistence::dir_lock`, whose `acquire` is a deliberate warn-once
//! NO-OP off unix (`src/persistence/dir_lock.rs`) — it returns `Ok` without
//! taking anything. On Windows there is therefore no cross-process claim to
//! assert, and asserting one fails deterministically rather than reporting a
//! real regression. `xp4` stays on every platform: distinctness and the port
//! range come from the in-process half, which does work there.

mod common;

// Used only by the exclusion cases, which are unix-only (see the module note).
#[cfg(unix)]
use moon::persistence::dir_lock::{self, DirLockError};

/// A port handed out by `reserve_port` is held under an exclusive `flock` for
/// the rest of the run, so no other process can claim it.
///
/// `flock(2)` associates the lock with the OPEN FILE DESCRIPTION, not the
/// process, so a second independent `acquire` conflicts with the first even
/// from here — which is what lets this assert the cross-process guarantee
/// without spawning a second test binary.
#[test]
#[cfg(unix)] // dir_lock::acquire is a no-op off unix — see the module note
fn xp1_a_reserved_port_is_locked_against_another_claimant() {
    let port = common::reserve_port();
    let dir = std::env::temp_dir()
        .join("moon-test-ports")
        .join(port.to_string());

    match dir_lock::acquire(&dir) {
        Err(DirLockError::Held { .. }) => {}
        Err(e) => panic!("port {port} lock failed for the wrong reason: {e}"),
        Ok(_) => panic!(
            "port {port} was handed out but NOT locked — a concurrent test \
             binary could take the same port, and SO_REUSEPORT would let its \
             server bind ours without an error"
        ),
    }
}

/// The lock must not be released while the reservation is still in use.
/// Reserving more ports must not drop the earlier locks.
#[test]
#[cfg(unix)] // dir_lock::acquire is a no-op off unix — see the module note
fn xp2_earlier_reservations_stay_locked_as_more_are_taken() {
    let first = common::reserve_port();
    for _ in 0..8 {
        let _ = common::reserve_port();
    }
    let dir = std::env::temp_dir()
        .join("moon-test-ports")
        .join(first.to_string());
    assert!(
        matches!(dir_lock::acquire(&dir), Err(DirLockError::Held { .. })),
        "port {first} lost its lock while later ports were reserved — it is \
         still in use by whatever the first reservation started"
    );
}

/// Ports are distinct, and a cluster reservation locks its bus sibling too —
/// a pair whose bus is taken is unusable even when the client port is free,
/// because the node dies on the bus bind.
#[test]
#[cfg(unix)] // dir_lock::acquire is a no-op off unix — see the module note
fn xp3_cluster_reservations_lock_both_the_client_port_and_its_bus() {
    let port = common::reserve_cluster_port();
    for n in [port, port + 10000] {
        let dir = std::env::temp_dir()
            .join("moon-test-ports")
            .join(n.to_string());
        assert!(
            matches!(dir_lock::acquire(&dir), Err(DirLockError::Held { .. })),
            "cluster reservation {port} left {n} unlocked"
        );
    }
}

/// Distinctness still holds, and every port is in a usable range.
#[test]
fn xp4_reservations_are_distinct() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..24 {
        let p = common::reserve_port();
        assert!(
            p >= 20000,
            "port {p} is below the floor the helper promises"
        );
        assert!(seen.insert(p), "port {p} was handed out twice");
    }
}
