//! Red/green cover for `common::await_listening` (moon#752).
//!
//! The bug this replaces: 23 suites spawned a server on a thread and then
//! slept a fixed 250ms before connecting. When startup took longer than the
//! guess, every test in the file died on `Connection refused`. These tests
//! pin the two properties a readiness probe must have and a fixed sleep
//! cannot: it waits as long as the server actually needs, and it gives up
//! with a useful error instead of hanging.

mod common;

use std::time::{Duration, Instant};

/// A listener that binds LATER than any fixed sleep would tolerate must still
/// be detected. With the old `sleep(250ms)` this is exactly the losing race.
#[tokio::test]
async fn await_listening_waits_for_a_slow_bind() {
    let port = common::reserve_port();
    let bind_after = Duration::from_millis(600);

    std::thread::spawn(move || {
        std::thread::sleep(bind_after);
        let _l = std::net::TcpListener::bind(("127.0.0.1", port)).expect("bind");
        std::thread::sleep(Duration::from_secs(5));
    });

    let start = Instant::now();
    common::await_listening(port, Duration::from_secs(10))
        .await
        .expect("must wait for a slow bind rather than giving up early");
    let waited = start.elapsed();

    assert!(
        waited >= bind_after,
        "returned before the listener existed ({waited:?} < {bind_after:?})"
    );
    assert!(
        waited < Duration::from_secs(5),
        "took far longer than the bind delay ({waited:?})"
    );
}

/// Nothing ever binds: the probe must return an error naming the port, and it
/// must do so near the deadline rather than hanging forever.
#[tokio::test]
async fn await_listening_times_out_with_a_useful_error() {
    let port = common::reserve_port();
    let deadline = Duration::from_millis(500);

    let start = Instant::now();
    let err = common::await_listening(port, deadline)
        .await
        .expect_err("must not report readiness for a port nothing ever bound");
    let waited = start.elapsed();

    assert!(
        err.contains(&port.to_string()),
        "error must name the port so a failure is diagnosable, got: {err}"
    );
    assert!(
        waited >= deadline,
        "gave up before the deadline ({waited:?} < {deadline:?})"
    );
    assert!(
        waited < deadline * 8,
        "overshot the deadline badly ({waited:?})"
    );
}

/// The negative control, and the reason this change exists: under the SAME
/// scenario `await_listening_waits_for_a_slow_bind` survives, the pattern this
/// PR removed — sleep a fixed 250ms, then connect — provably loses the race.
///
/// Without this test the file only proves the new helper works; it would not
/// show the old one was broken. The 250ms literal is the exact value the 25
/// replaced call sites used.
#[tokio::test]
async fn fixed_250ms_sleep_loses_the_race_the_probe_wins() {
    let port = common::reserve_port();
    let bind_after = Duration::from_millis(600);

    std::thread::spawn(move || {
        std::thread::sleep(bind_after);
        let _l = std::net::TcpListener::bind(("127.0.0.1", port)).expect("bind");
        std::thread::sleep(Duration::from_secs(5));
    });

    // Exactly what every replaced call site did.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let old_pattern = std::net::TcpStream::connect(("127.0.0.1", port));
    assert!(
        old_pattern.is_err(),
        "the fixed 250ms sleep was supposed to lose this race; if it connected, \
         this control is not discriminating and the premise of moon#752 is unproven"
    );

    // The replacement, same scenario, same thread: succeeds.
    common::await_listening(port, Duration::from_secs(10))
        .await
        .expect("the readiness probe must win where the fixed sleep lost");
}
