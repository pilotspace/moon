//! Red/green cover for `common::await_listening` (moon#752).
//!
//! The bug this replaces: 25 call sites across 24 suites spawned a server on a
//! thread and then slept a fixed 250ms before connecting. When startup took
//! longer than the guess, every test in the file died on `Connection refused`.
//! These tests pin the two properties a readiness probe must have and a fixed
//! sleep cannot: it waits as long as the server actually needs, and it gives
//! up with a useful error instead of hanging.
//!
//! Every upper time bound below is deliberately loose. They exist to catch a
//! HANG, not to assert a latency, and a tight ceiling on a heavily parallel CI
//! runner is exactly the wall-clock fragility this PR removes.

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
        // Outlive the ceiling asserted below, so a slow runner cannot drop the
        // listener out from under the probe and fail for the wrong reason.
        std::thread::sleep(Duration::from_secs(60));
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
        waited < Duration::from_secs(30),
        "probe appears to hang rather than return after the bind ({waited:?})"
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
        waited < deadline * 20,
        "probe appears to hang rather than honour its deadline ({waited:?})"
    );
}

/// The negative control, and the reason this change exists: a single connect
/// attempt made at a fixed moment fails when the server has not bound yet,
/// while the probe succeeds as soon as it does.
///
/// Without this test the file would only prove the new helper works, not that
/// the pattern it replaces was broken.
///
/// The "not yet listening" window is closed by an explicit gate rather than by
/// hoping a sleep outlasts a bind delay. An earlier wall-clock version of this
/// test FAILED on Windows CI (moon#753): under nextest's parallelism the 250ms
/// sleep overshot the 600ms bind, the connect succeeded, and the control
/// reported the opposite of the truth. That is precisely the fragility this PR
/// removes from 25 call sites, so the control must not rely on it either.
#[tokio::test]
async fn fixed_sleep_loses_the_race_the_probe_wins() {
    let port = common::reserve_port();
    let (open_gate, gate) = std::sync::mpsc::channel::<()>();

    std::thread::spawn(move || {
        // Bind ONLY when the test says so, so "nothing is listening yet" is a
        // guarantee at the moment of the connect below, not a race.
        gate.recv().expect("gate closed before bind");
        let listener = std::net::TcpListener::bind(("127.0.0.1", port)).expect("bind");
        std::thread::sleep(Duration::from_secs(5));
        drop(listener);
    });

    // Exactly what every replaced call site did: sleep, then connect once.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", port)).is_err(),
        "a one-shot connect must fail while the server has not bound; if it \
         succeeded, this control is not discriminating and the premise of \
         moon#752 is unproven"
    );

    // Same scenario, same thread: the probe waits for the bind and finds it.
    open_gate.send(()).expect("binder thread died");
    common::await_listening(port, Duration::from_secs(10))
        .await
        .expect("the readiness probe must win where the fixed sleep lost");
}
