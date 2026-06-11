//! ADD task `hotpath-lock-quickwins` — new-API red tests.
//!
//! This file is COMPILE-RED until the build lands: it references the small
//! public surfaces the contract introduces. Each test is behavior-level once
//! the symbol exists. Kept separate from `quickwins_red.rs` so the runtime
//! suite stays runnable during the red phase:
//!   cargo test --test quickwins_red       # runs (1 red, 4 pins)
//!   cargo test --test quickwins_red_api   # compile error = red, by design

use std::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------------------
// QW1 — every accepted client socket gets TCP_NODELAY (+ shared accept opts)
// ---------------------------------------------------------------------------

/// The accept paths (tokio + uring register) funnel socket options through one
/// helper; applying it to an accepted fd must enable TCP_NODELAY.
#[test]
fn qw1_accepted_socket_has_nodelay() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let _client = TcpStream::connect(addr).unwrap();
    let (accepted, _) = listener.accept().unwrap();

    // Fresh accepted socket: Nagle is on by default (sanity).
    assert!(
        !accepted.nodelay().unwrap(),
        "sanity: OS default leaves Nagle enabled"
    );

    moon::server::socket_opts::apply_client_socket_opts(&accepted)
        .expect("applying client socket opts must succeed");

    assert!(
        accepted.nodelay().unwrap(),
        "QW1: accepted client sockets must have TCP_NODELAY set"
    );
}

// ---------------------------------------------------------------------------
// QW3 — replication offsets reachable WITHOUT locking ReplicationState
// ---------------------------------------------------------------------------

/// The write path receives an `OffsetHandle` at startup and advances offsets
/// through it; the `RwLock<ReplicationState>` is no longer touched per write.
/// Handle and state must observe the same totals.
#[test]
fn qw3_offset_handle_advances_without_state_lock() {
    use moon::replication::state::ReplicationState;

    let state = ReplicationState::new(2, "a".repeat(40), "b".repeat(40));
    let handle = state.offset_handle();

    handle.issue_lsn(0, 100);
    handle.issue_lsn(1, 50);

    assert_eq!(state.shard_offset(0), 100);
    assert_eq!(state.shard_offset(1), 50);
    assert_eq!(state.total_offset(), 150);
}
