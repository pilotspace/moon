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

/// A key `<prefix>:<i>` owned by shard `shard` of `shards`.
fn key_on_shard(prefix: &str, shard: usize, shards: usize) -> String {
    (0..)
        .map(|i| format!("{prefix}:{i}"))
        .find(|k| moon::shard::dispatch::key_to_shard(k.as_bytes(), shards) == shard)
        .unwrap()
}

/// The shard `w`'s connection lives on: the one whose key a plain SET from
/// `w` is served INLINE for (the inline path only runs a key its own shard
/// owns). monoio only — tokio has no inline path.
#[cfg(feature = "runtime-monoio")]
fn home_shard(w: &mut common::Conn, admin_port: u16, shards: usize) -> usize {
    for s in 0..shards {
        let k = key_on_shard("trk:home", s, shards);
        let before = ws7::local_inline(admin_port);
        assert_eq!(w.send(&["SET", &k, "x"]), "+OK\r\n");
        if ws7::local_inline(admin_port) - before == 1 {
            return s;
        }
    }
    panic!("no plain SET from the writer was inlined on any of {shards} shards");
}

fn invalidations_reach_every_mode(shards: &str) {
    let srv = ws7::spawn("ws7-trk-inv", shards, &[]);
    let mut w = ws7::conn(srv.port);
    let n_shards: usize = shards.parse().unwrap();

    // Default mode, RESP3 push. The tracked key lives on the WRITER's own
    // shard, so the SET below is eligible for the inline path — and on
    // monoio the `local_inline` delta proves the inline path served it, so
    // this is the inline SET's invalidation under test, not the generic
    // path's (tokio has no inline path; there it is the generic one).
    #[cfg(feature = "runtime-monoio")]
    let home = home_shard(&mut w, srv.admin_port, n_shards);
    #[cfg(not(feature = "runtime-monoio"))]
    let home = 0;
    let tracked = key_on_shard("trk:a", home, n_shards);
    let mut t = ws7::conn(srv.port);
    t.sock.write_all(&common::encode(&["HELLO", "3"])).unwrap();
    let _ = drain(&mut t, Duration::from_millis(200));
    assert_eq!(w.send(&["SET", &tracked, "1"]), "+OK\r\n");
    t.sock
        .write_all(&common::encode(&["CLIENT", "TRACKING", "ON"]))
        .unwrap();
    assert!(drain(&mut t, Duration::from_millis(200)).contains("+OK"));
    t.sock
        .write_all(&common::encode(&["GET", &tracked]))
        .unwrap();
    assert!(drain(&mut t, Duration::from_millis(200)).contains("$1\r\n1\r\n"));
    // An untracked key written first: no push for it.
    assert_eq!(w.send(&["SET", "trk:untracked", "x"]), "+OK\r\n");
    let before = ws7::local_inline(srv.admin_port);
    assert_eq!(w.send(&["SET", &tracked, "2"]), "+OK\r\n");
    let inlined = ws7::local_inline(srv.admin_port) - before;
    #[cfg(feature = "runtime-monoio")]
    assert_eq!(
        inlined, 1,
        "the default-mode SET of {tracked} (writer's shard {home}) was not \
         served inline, so this block would be testing the generic path's \
         invalidation instead of the inline SET's"
    );
    #[cfg(not(feature = "runtime-monoio"))]
    assert_eq!(inlined, 0, "tokio has no inline path");
    let got = wait_for(&mut t, &tracked, Duration::from_secs(3));
    assert!(
        got.contains("invalidate") && got.contains(&tracked),
        "tracked key {tracked} written by a plain SET was not invalidated: {got:?}"
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
    // client's write does. The tracker's own SET is never inlined — a
    // tracking connection stands down from the inline path
    // (`!tracking_state.enabled` in `can_inline_writes`) — so the writer id
    // `try_inline_dispatch` passes to `invalidate_inline_write` is
    // defensive: on that path it never names a NOLOOP tracker. What is
    // proven here is the generic path's NOLOOP, and that a FOREIGN writer's
    // SET (`w`, below) still reaches a NOLOOP tracker.
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
