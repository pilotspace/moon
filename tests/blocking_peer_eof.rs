//! c10k hardening A1 — a client that vanishes while blocked must be reaped.
//!
//! The defect: the infinite-wait `select!` behind BLPOP/BRPOP/BLMOVE/BZPOP*
//! had exactly two arms, `reply_rx` and `shutdown`. Nothing watched the
//! socket, so `BLPOP key 0` followed by a disconnect left the handler task,
//! the `WaitEntry`, the client-registry entry and the maxclients slot alive
//! forever — infinite waiters carry `deadline: None`, so the deadline sweep
//! never reaps them either, and `timeout` exempts blocked clients by design
//! (Redis parity). A few thousand throwaway connections wedged the server
//! until restart, using one unauthenticated command.
//!
//! The fix adds a peer-watch arm. These tests pin both halves: the slot is
//! released when the peer goes away, and a client that legitimately pipelines
//! behind its blocking command still gets every reply (the watch consumes
//! from the socket, so those bytes have to be carried into the parse stream —
//! pre-A1 they simply waited in the kernel).
//!
//! Run with:
//!   cargo test --release --test blocking_peer_eof

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn spawn(dir: &std::path::Path, port: u16, shards: &str) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            shards,
            "--dir",
            dir.to_str().unwrap(),
            "--disk-free-min-pct",
            "0",
            // Without an explicit cap moon auto-sizes maxmemory to ~80% of
            // host RAM and provisions a multi-GB per-shard page cache. Four
            // of these starting at once makes startup itself flaky; the test
            // stores a handful of tiny keys, so cap it small.
            "--maxmemory",
            "268435456",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    s.set_nodelay(true).expect("nodelay");
    s
}

fn send(stream: &mut TcpStream, parts: &[&str]) {
    let mut out = format!("*{}\r\n", parts.len());
    for p in parts {
        out.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    stream.write_all(out.as_bytes()).expect("write");
}

fn read_some(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf[..n]).into_owned())
}

fn connected_clients(port: u16) -> u64 {
    let mut c = connect(port);
    send(&mut c, &["INFO", "clients"]);
    let body = read_some(&mut c).expect("INFO reply");
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("connected_clients:") {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    panic!("INFO clients has no connected_clients; got:\n{body}");
}

/// Poll until `connected_clients` drops to `at_most`, or give up. Returns the
/// last value seen so failures report the real number.
fn wait_for_clients(port: u16, at_most: u64, budget: Duration) -> u64 {
    let start = Instant::now();
    let mut last = u64::MAX;
    while start.elapsed() < budget {
        last = connected_clients(port);
        // The probe connection above is itself counted while it is open.
        if last <= at_most {
            return last;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    last
}

struct Server {
    child: Child,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        common::sigkill(&mut self.child);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn server(tag: &str, shards: &str) -> Option<Server> {
    let bin = common::find_moon_binary();
    if !bin.exists() {
        eprintln!("skipping: no moon binary; build with `cargo build --release`");
        return None;
    }
    let (child, port) = common::spawn_listening(|port| {
        let dir = std::env::temp_dir().join(format!("moon-{tag}-{port}"));
        let _ = std::fs::create_dir_all(&dir);
        spawn(&dir, port, shards)
    });
    let dir = std::env::temp_dir().join(format!("moon-{tag}-{port}"));
    Some(Server { child, port, dir })
}

/// THE A1 REGRESSION TEST (single-key). Pre-fix every one of these
/// connections stayed connected forever.
#[test]
fn disconnected_single_key_blocked_clients_are_reaped() {
    let Some(srv) = server("a1-single", "1") else {
        return;
    };
    const N: usize = 24;

    for i in 0..N {
        let mut c = connect(srv.port);
        // Block forever on a key nobody will ever push to.
        send(&mut c, &["BLPOP", &format!("a1:single:{i}"), "0"]);
        // Give the server a moment to actually register the wait before the
        // disconnect — otherwise the test could pass by never blocking.
        std::thread::sleep(Duration::from_millis(20));
        drop(c);
    }

    // Only the INFO probe connection itself should remain.
    let remaining = wait_for_clients(srv.port, 2, Duration::from_secs(15));
    assert!(
        remaining <= 2,
        "{N} disconnected `BLPOP key 0` clients must release their slots; \
         connected_clients is still {remaining} — this is the A1 leak"
    );

    // And the server must still be fully serviceable.
    let mut probe = connect(srv.port);
    send(&mut probe, &["PING"]);
    assert!(
        read_some(&mut probe).expect("pong").starts_with("+PONG"),
        "server must still serve after the disconnect storm"
    );
}

/// Same leak via the multi-key coordinator, across shards so the waiter holds
/// REMOTE registrations too — those are torn down by `BlockCancel`, a
/// different cleanup path from the single-key one.
#[test]
fn disconnected_multi_key_blocked_clients_are_reaped() {
    let Some(srv) = server("a1-multi", "4") else {
        return;
    };
    const N: usize = 16;

    for i in 0..N {
        let mut c = connect(srv.port);
        // Several keys with different hash slots — at 4 shards this fans out
        // to remote owners, exercising the BlockCancel unwind.
        send(
            &mut c,
            &[
                "BLPOP",
                &format!("a1:m:{i}:alpha"),
                &format!("a1:m:{i}:beta"),
                &format!("a1:m:{i}:gamma"),
                &format!("a1:m:{i}:delta"),
                "0",
            ],
        );
        std::thread::sleep(Duration::from_millis(20));
        drop(c);
    }

    let remaining = wait_for_clients(srv.port, 2, Duration::from_secs(15));
    assert!(
        remaining <= 2,
        "{N} disconnected multi-key blocked clients must release their slots; \
         connected_clients is still {remaining}"
    );

    // The registrations must be gone too, not just the sockets: a leaked
    // ghost waiter would swallow this push instead of leaving it on the list.
    let mut pusher = connect(srv.port);
    send(&mut pusher, &["RPUSH", "a1:m:0:alpha", "payload"]);
    let _ = read_some(&mut pusher);
    send(&mut pusher, &["LLEN", "a1:m:0:alpha"]);
    let llen = read_some(&mut pusher).unwrap_or_default();
    assert!(
        llen.starts_with(":1"),
        "the pushed element must stay on the list — a ghost waiter swallowed \
         it instead (LLEN said {llen:?})"
    );
}

/// A live blocked client must NOT be mistaken for a dead one. This is the
/// inverse failure mode of the fix: the peer watch reads from the socket, so
/// a client that pipelines behind its blocking command must still be woken,
/// and must still get the replies to what it pipelined.
#[test]
fn pipelining_behind_a_blocking_command_still_works() {
    let Some(srv) = server("a1-carry", "1") else {
        return;
    };
    let mut blocker = connect(srv.port);

    send(&mut blocker, &["BLPOP", "a1:carry", "0"]);
    std::thread::sleep(Duration::from_millis(200));

    // Legal RESP: more commands arrive while the client is blocked. Redis
    // runs them after the block resolves; the peer watch must carry them, not
    // eat them and not treat them as a disconnect.
    send(&mut blocker, &["PING"]);
    std::thread::sleep(Duration::from_millis(200));

    // The blocked client must still be considered alive and wakeable.
    let mut pusher = connect(srv.port);
    send(&mut pusher, &["RPUSH", "a1:carry", "value"]);
    let _ = read_some(&mut pusher);

    // Expect the BLPOP reply, then the pipelined PONG. They may or may not
    // land in the same read, so accumulate.
    blocker
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let mut got = String::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !(got.contains("value") && got.contains("PONG")) {
        match read_some(&mut blocker) {
            Ok(s) if s.is_empty() => break,
            Ok(s) => got.push_str(&s),
            Err(_) => break,
        }
    }
    assert!(
        got.contains("value"),
        "blocked client must still be woken by the push; got {got:?}"
    );
    assert!(
        got.contains("PONG"),
        "the command pipelined while blocked must not be swallowed by the \
         peer watch — it has to be carried into the parse stream; got {got:?}"
    );
}

/// The carry may be a PARTIAL frame. The read-skip guard must then fall
/// through to a real read instead of spinning on unparseable bytes or
/// stalling until the client happens to send something else.
#[test]
fn partial_frame_carried_while_blocked_completes_later() {
    let Some(srv) = server("a1-partial", "1") else {
        return;
    };
    let mut blocker = connect(srv.port);
    send(&mut blocker, &["BLPOP", "a1:partial", "0"]);
    std::thread::sleep(Duration::from_millis(200));

    // Half of `PING`, sent while blocked — the peer watch will carry these
    // bytes, and they cannot be parsed on their own.
    blocker.write_all(b"*1\r\n$4\r\nPI").expect("write partial");
    std::thread::sleep(Duration::from_millis(200));

    let mut pusher = connect(srv.port);
    send(&mut pusher, &["RPUSH", "a1:partial", "value"]);
    let _ = read_some(&mut pusher);

    blocker
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let mut got = read_some(&mut blocker).unwrap_or_default();
    assert!(
        got.contains("value"),
        "blocked client must be woken even with a partial frame carried; got {got:?}"
    );

    // Now complete the frame; the server must have kept the partial bytes.
    blocker.write_all(b"NG\r\n").expect("write rest");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !got.contains("PONG") {
        match read_some(&mut blocker) {
            Ok(s) if s.is_empty() => break,
            Ok(s) => got.push_str(&s),
            Err(_) => break,
        }
    }
    assert!(
        got.contains("PONG"),
        "the carried partial frame must complete once the rest arrives — the \
         carried bytes were dropped instead; got {got:?}"
    );
}

/// A blocked client that stays connected and silent must stay blocked. Guards
/// against the peer watch spinning or self-triggering on an idle socket.
#[test]
fn silent_blocked_client_stays_blocked() {
    let Some(srv) = server("a1-idle", "1") else {
        return;
    };
    let mut blocker = connect(srv.port);
    send(&mut blocker, &["BLPOP", "a1:idle", "0"]);

    // Sit still well past any internal sweep tick.
    std::thread::sleep(Duration::from_secs(4));

    // Still registered? A push must reach it.
    let mut pusher = connect(srv.port);
    send(&mut pusher, &["RPUSH", "a1:idle", "late"]);
    let _ = read_some(&mut pusher);

    blocker
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let woken = read_some(&mut blocker).unwrap_or_default();
    assert!(
        woken.contains("late"),
        "an idle-but-connected blocked client must stay blocked and wakeable; \
         got {woken:?}"
    );
}
