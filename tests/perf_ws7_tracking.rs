//! WS7 / moon#1166: one idle `CLIENT TRACKING` client must not cost every
//! writer the inline SET path (the review measured SET -41% with a single
//! idle tracker; redis about -9%), and a write to a key NOBODY tracks must not
//! take the process-wide tracking lock.
//!
//! - `idle_tracker_keeps_inline_set` (monoio: the only handler with an inline
//!   path) is red on `ae21476`: `can_inline_writes` carried
//!   `!tracking_active()`, so the counter stays flat.
//! - The invalidation tests prove the inline SET still invalidates — default
//!   mode, BCAST, RESP2 REDIRECT, and NOLOOP — on both the old and new binary.
//!
//! Run: `MOON_BIN=/path/to/moon cargo test --test perf_ws7_tracking`

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;
mod perf_ws7_support;

use std::io::{Read, Write};
use std::time::{Duration, Instant};

use perf_ws7_support as ws7;

/// Read whatever arrives within `window` (pushes are asynchronous).
fn drain(c: &mut common::Conn, window: Duration) -> String {
    c.sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let deadline = Instant::now() + window;
    let mut acc = Vec::new();
    let mut buf = [0u8; 8192];
    while Instant::now() < deadline {
        match c.sock.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => acc.extend_from_slice(&buf[..n]),
            Err(_) => {}
        }
    }
    c.sock
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    String::from_utf8_lossy(&acc).into_owned()
}

/// Wait until `needle` shows up on `c` (or the window closes).
fn wait_for(c: &mut common::Conn, needle: &str, window: Duration) -> String {
    let deadline = Instant::now() + window;
    let mut seen = String::new();
    while Instant::now() < deadline {
        seen.push_str(&drain(c, Duration::from_millis(100)));
        if seen.contains(needle) {
            break;
        }
    }
    seen
}

fn client_id(c: &mut common::Conn) -> String {
    let r = c.send(&["CLIENT", "ID"]);
    r.trim_start_matches(':').trim().to_string()
}

#[cfg(feature = "runtime-monoio")]
#[test]
fn idle_tracker_keeps_inline_set() {
    let srv = ws7::spawn("ws7-trk-inline", "1", &[]);
    let mut tracker = ws7::conn(srv.port);
    tracker
        .sock
        .write_all(&common::encode(&["HELLO", "3"]))
        .unwrap();
    let _ = drain(&mut tracker, Duration::from_millis(200));
    tracker
        .sock
        .write_all(&common::encode(&["CLIENT", "TRACKING", "ON"]))
        .unwrap();
    assert!(drain(&mut tracker, Duration::from_millis(200)).contains("+OK"));

    let mut w = ws7::conn(srv.port);
    let before = ws7::local_inline(srv.admin_port);
    for i in 0..50 {
        assert_eq!(w.send(&["SET", &format!("k:{i}"), "v"]), "+OK\r\n");
    }
    let inlined = ws7::local_inline(srv.admin_port) - before;
    assert!(
        inlined >= 50,
        "with one idle CLIENT TRACKING client connected, 50 plain SETs were \
         inlined {inlined} times — tracking disabled the inline write path \
         server-wide"
    );
    drop(tracker);
}

fn invalidations_reach_every_mode(shards: &str) {
    let srv = ws7::spawn("ws7-trk-inv", shards, &[]);
    let mut w = ws7::conn(srv.port);

    // Default mode, RESP3 push.
    let mut t = ws7::conn(srv.port);
    t.sock.write_all(&common::encode(&["HELLO", "3"])).unwrap();
    let _ = drain(&mut t, Duration::from_millis(200));
    assert_eq!(w.send(&["SET", "trk:a", "1"]), "+OK\r\n");
    t.sock
        .write_all(&common::encode(&["CLIENT", "TRACKING", "ON"]))
        .unwrap();
    assert!(drain(&mut t, Duration::from_millis(200)).contains("+OK"));
    t.sock
        .write_all(&common::encode(&["GET", "trk:a"]))
        .unwrap();
    assert!(drain(&mut t, Duration::from_millis(200)).contains("$1\r\n1\r\n"));
    // An untracked key written first: no push for it.
    assert_eq!(w.send(&["SET", "trk:untracked", "x"]), "+OK\r\n");
    assert_eq!(w.send(&["SET", "trk:a", "2"]), "+OK\r\n");
    let got = wait_for(&mut t, "trk:a", Duration::from_secs(3));
    assert!(
        got.contains("invalidate") && got.contains("trk:a"),
        "tracked key written by a plain SET was not invalidated: {got:?}"
    );
    assert!(
        !got.contains("trk:untracked"),
        "untracked key invalidated: {got:?}"
    );

    // BCAST prefix.
    let mut b = ws7::conn(srv.port);
    b.sock.write_all(&common::encode(&["HELLO", "3"])).unwrap();
    let _ = drain(&mut b, Duration::from_millis(200));
    b.sock
        .write_all(&common::encode(&[
            "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:",
        ]))
        .unwrap();
    assert!(drain(&mut b, Duration::from_millis(200)).contains("+OK"));
    assert_eq!(w.send(&["SET", "user:7", "x"]), "+OK\r\n");
    let got = wait_for(&mut b, "user:7", Duration::from_secs(3));
    assert!(
        got.contains("user:7"),
        "BCAST prefix write not invalidated: {got:?}"
    );

    // RESP2 REDIRECT to a subscribed connection.
    let mut sink = ws7::conn(srv.port);
    let sink_id = client_id(&mut sink);
    sink.sock
        .write_all(&common::encode(&["SUBSCRIBE", "__redis__:invalidate"]))
        .unwrap();
    assert!(drain(&mut sink, Duration::from_millis(200)).contains("subscribe"));
    let mut r = ws7::conn(srv.port);
    assert_eq!(
        r.send(&["CLIENT", "TRACKING", "ON", "REDIRECT", &sink_id]),
        "+OK\r\n"
    );
    assert_eq!(w.send(&["SET", "trk:r", "1"]), "+OK\r\n");
    assert_eq!(r.send(&["GET", "trk:r"]), "$1\r\n1\r\n");
    assert_eq!(w.send(&["SET", "trk:r", "2"]), "+OK\r\n");
    let got = wait_for(&mut sink, "trk:r", Duration::from_secs(3));
    assert!(
        got.contains("message") && got.contains("trk:r"),
        "RESP2 redirect target got no invalidation: {got:?}"
    );

    // NOLOOP: the tracker's own write does not come back to it, another
    // client's write does.
    let mut n = ws7::conn(srv.port);
    n.sock.write_all(&common::encode(&["HELLO", "3"])).unwrap();
    let _ = drain(&mut n, Duration::from_millis(200));
    n.sock
        .write_all(&common::encode(&["CLIENT", "TRACKING", "ON", "NOLOOP"]))
        .unwrap();
    assert!(drain(&mut n, Duration::from_millis(200)).contains("+OK"));
    n.sock
        .write_all(&common::encode(&["SET", "trk:n", "1"]))
        .unwrap();
    let _ = drain(&mut n, Duration::from_millis(200));
    n.sock
        .write_all(&common::encode(&["GET", "trk:n"]))
        .unwrap();
    let _ = drain(&mut n, Duration::from_millis(200));
    n.sock
        .write_all(&common::encode(&["SET", "trk:n", "2"]))
        .unwrap();
    let own = drain(&mut n, Duration::from_millis(400));
    assert!(
        !own.contains("invalidate"),
        "NOLOOP self-write came back: {own:?}"
    );
    n.sock
        .write_all(&common::encode(&["GET", "trk:n"]))
        .unwrap();
    let _ = drain(&mut n, Duration::from_millis(200));
    assert_eq!(w.send(&["SET", "trk:n", "3"]), "+OK\r\n");
    let got = wait_for(&mut n, "trk:n", Duration::from_secs(3));
    assert!(
        got.contains("trk:n"),
        "NOLOOP tracker missed a foreign write: {got:?}"
    );
}

#[test]
fn inline_set_still_invalidates_every_tracking_mode_1_shard() {
    invalidations_reach_every_mode("1");
}

#[test]
fn inline_set_still_invalidates_every_tracking_mode_2_shards() {
    invalidations_reach_every_mode("2");
}
