//! A waker must never CONSUME a waiter whose command it cannot serve — moon#535.
//!
//! `try_wake_list_waiter`, `try_wake_zset_waiter` and `try_wake_stream_waiter`
//! each pop whatever waiter is at the front of a key's queue, execute it, and
//! then — unconditionally, for every waiter they popped — do
//!
//! ```ignore
//! registry.remove_wait(wait_id);   // drops the registration across ALL its keys
//! ...
//! let _ = reply_tx.send(None);     // the client decodes this as "timed out"
//! ```
//!
//! A waiter the waker does not handle falls to `_ => (None, None)` and hits
//! that same cleanup, so the waker DESTROYS it: the registration is gone and
//! the client is answered a null it never earned.
//!
//! Two independent paths reach it, which is why the fix belongs in the wakers
//! and not at one call site:
//!
//!   1. **Remote registration.** `ShardMessage::BlockRegister`
//!      (`src/shard/spsc_handler.rs`) registers the waiter and then — gated on
//!      `guard.exists(&key)` — calls all three wakers in sequence, LIST FIRST.
//!      A `BZPOPMIN` on a populated zset owned by another shard is therefore
//!      eaten by the list waker during its own registration, and answered
//!      `*-1` in well under a millisecond instead of returning the member that
//!      is sitting right there. The `exists` gate means this fires ONLY when
//!      the command should have succeeded.
//!   2. **A natural push.** An `RPUSH` on a key name that also carries a zset
//!      waiter runs the list waker for real, and destroys that waiter the same
//!      way — no remote registration involved.
//!
//! `--shards 4` is the point of path 1: a connection is pinned to one shard by
//! SO_REUSEPORT, so most keys hash elsewhere and register remotely. At
//! `--shards 1` every registration is local and path 1 cannot fire at all.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const SHARDS: &str = "4";

/// Enough keys that several provably hash to a shard other than the client's.
/// With 4 shards a client-local key is ~1 in 4, so 12 keys make an all-local
/// draw (which would hide path 1 entirely) about 1 in 16 million.
const KEYS: usize = 12;

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon() -> Moon {
    // `CARGO_BIN_EXE_moon` is the binary cargo just built for THIS test run.
    // `common::find_moon_binary()` falls back to `target/release/moon`, whose
    // provenance is unknown — a stale one turns a real failure into a green.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-cannibal-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                SHARDS,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                // The repo's data volume hovers near the diskfull guard's 5%
                // threshold; without this the server refuses writes and every
                // assertion below fails for an unrelated reason.
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap_or("/tmp"),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-cannibal-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port}\n--- stderr ---\n{log}");
}

fn connect(port: u16, read_timeout: Duration) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).expect("connect to moon");
    s.set_read_timeout(Some(read_timeout))
        .expect("read timeout");
    s.set_nodelay(true).expect("nodelay");
    s
}

fn encode(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn send(s: &mut TcpStream, parts: &[&str]) {
    s.write_all(&encode(parts)).expect("write command");
}

/// Read one complete RESP reply, tolerating TCP segmentation.
fn read_reply(s: &mut TcpStream) -> std::io::Result<String> {
    let mut acc = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = s.read(&mut buf)?;
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        if complete_len(&acc).is_some() {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&acc).into_owned())
}

fn complete_len(buf: &[u8]) -> Option<usize> {
    let line_end = buf.windows(2).position(|w| w == b"\r\n")?;
    let header = &buf[..line_end];
    let after = line_end + 2;
    match header.first()? {
        b'+' | b'-' | b':' => Some(after),
        b'$' => {
            let len: i64 = std::str::from_utf8(&header[1..]).ok()?.parse().ok()?;
            if len < 0 {
                return Some(after);
            }
            let end = after + len as usize + 2;
            (buf.len() >= end).then_some(end)
        }
        b'*' => {
            let n: i64 = std::str::from_utf8(&header[1..]).ok()?.parse().ok()?;
            if n < 0 {
                return Some(after);
            }
            let mut cursor = after;
            for _ in 0..n {
                cursor += complete_len(buf.get(cursor..)?)?;
            }
            Some(cursor)
        }
        _ => None,
    }
}

fn cmd(s: &mut TcpStream, parts: &[&str]) -> String {
    send(s, parts);
    read_reply(s).expect("read reply")
}

/// How many clients the server currently has PARKED on a blocking command.
///
/// Read from `INFO clients` — the server's own account of its registry, rather
/// than the test's guess about how long a registration takes to get there.
fn blocked_clients(port: u16) -> u32 {
    let mut c = connect(port, Duration::from_secs(5));
    cmd(&mut c, &["INFO", "clients"])
        .lines()
        .find_map(|l| l.trim().strip_prefix("blocked_clients:")?.parse().ok())
        .unwrap_or(0)
}

/// Wait until the server reports one MORE parked client than `baseline`.
///
/// A `sleep` cannot do this job, and its two failure directions are not
/// symmetric. Sleeping too long only makes the test slow. Sleeping too short
/// lets the `RPUSH` below reach the key BEFORE the `BZPOPMIN` does — and then
/// the pop runs against a key that is already a list and answers
/// `-WRONGTYPE` immediately, which is moon#556's correct behavior. This test
/// reads that reply as "the waiter was cannibalised" and fails, having never
/// set up the scenario it believes it is testing.
///
/// That is not hypothetical: with the old 150ms sleep wc4 failed 3-of-3 inside
/// a loaded full-suite run (5309 tests, 6-CPU VM) while passing 11-of-11 in
/// isolation, reporting exactly that WRONGTYPE string.
///
/// `blocked_clients` is one process-wide gauge, not a per-key one, so this can
/// only be read as "one MORE than before" — which requires that nothing else
/// DECREMENTS it while we wait. That is why the caller keeps every blocker
/// connection alive for the whole test instead of dropping it per iteration:
/// a dropped blocker unblocks asynchronously, and under load those decrements
/// land after the baseline is sampled. Measured: with per-iteration drops this
/// helper failed 2-of-4 under CPU oversubscription on the sequence
/// `before=3 -> 3 stale reaps -> 0 -> new waiter -> 1`, where `1 > 3` is never
/// true and the poll simply burns its deadline.
fn await_one_more_blocked(port: u16, baseline: u32) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if blocked_clients(port) > baseline {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Issue `probe` against `KEYS` populated zsets and report EVERY key that
/// answered a null instead of the member sitting in it.
///
/// Reporting all of them rather than the first matters: the COUNT is the
/// diagnosis. ~3-of-4 wrong at `--shards 4` is the remote-registration path;
/// 12-of-12 would mean something else entirely, and an assert on the first
/// failure would make those look identical.
#[track_caller]
fn assert_populated_zset_is_never_a_null(
    port: u16,
    label: &str,
    probe: impl Fn(&str) -> Vec<String>,
) {
    let mut c = connect(port, Duration::from_secs(15));
    let mut wrong = Vec::new();
    for i in 0..KEYS {
        let key = format!("cannibal:{label}:{i}");
        assert_eq!(
            cmd(&mut c, &["ZADD", &key, "1", "m1"]),
            ":1\r\n",
            "{label}: ZADD did not create {key} — the probe below would be vacuous"
        );

        let parts = probe(&key);
        let argv: Vec<&str> = parts.iter().map(String::as_str).collect();
        let started = Instant::now();
        let got = cmd(&mut c, &argv);
        let elapsed = started.elapsed();

        // The member is right there, so this must answer with it — and it must
        // do so without ever blocking. A `*-1` here is the bug; note it comes
        // back in microseconds, NOT after the timeout the client asked for,
        // because the null is sent during registration.
        if !got.contains("m1") {
            wrong.push(format!("  {key} -> {got:?} after {elapsed:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "moon#535 — {label} answered a null for {} of {} zsets that DO contain m1. \
         A waker consumed the waiter it could not serve, removed its registration \
         and sent it a null it never earned:\n{}",
        wrong.len(),
        KEYS,
        wrong.join("\n")
    );
}

#[test]
fn wc1_bzpopmin_on_a_populated_zset_returns_its_member() {
    let m = spawn_moon();
    assert_populated_zset_is_never_a_null(m.port, "bzpopmin", |k| {
        vec!["BZPOPMIN".into(), k.into(), "3".into()]
    });
}

#[test]
fn wc2_bzpopmax_on_a_populated_zset_returns_its_member() {
    let m = spawn_moon();
    assert_populated_zset_is_never_a_null(m.port, "bzpopmax", |k| {
        vec!["BZPOPMAX".into(), k.into(), "3".into()]
    });
}

#[test]
fn wc3_bzmpop_on_a_populated_zset_returns_its_member() {
    let m = spawn_moon();
    assert_populated_zset_is_never_a_null(m.port, "bzmpop", |k| {
        vec![
            "BZMPOP".into(),
            "3".into(),
            "1".into(),
            k.into(),
            "MIN".into(),
        ]
    });
}

#[test]
fn wc4_a_list_push_must_not_destroy_a_zset_waiter_on_the_same_key() {
    // The second path, reachable with no remote registration in sight: the
    // list waker runs FOR REAL on an RPUSH and pops whatever waiter is in
    // front, zset waiters included.
    //
    // A zset waiter is not entitled to a list push, so the correct behavior is
    // that the blocked client STAYS blocked. The bug answers it a null.
    let m = spawn_moon();
    let mut wrong = Vec::new();
    let mut usable = 0;
    // Held for the whole test, never dropped per iteration: see
    // `await_one_more_blocked` for why the gauge must not go down while a
    // handshake is in flight.
    let mut blockers: Vec<TcpStream> = Vec::new();

    for i in 0..KEYS {
        let key = format!("cannibal:mixed:{i}");
        let before = blocked_clients(m.port);
        let mut blocker = connect(m.port, Duration::from_millis(1200));
        // The key does not exist yet, so this genuinely blocks.
        send(&mut blocker, &["BZPOPMIN", &key, "0"]);
        // The registration must be ON the owning shard before the push races
        // it. Ask the server; do not guess — see `await_one_more_blocked` for
        // why a sleep gets this wrong in the direction that FAILS the test.
        assert!(
            await_one_more_blocked(m.port, before),
            "{key}: BZPOPMIN never reached the blocking registry, so the push \
             below would prove nothing about waiter cannibalisation"
        );

        let mut pusher = connect(m.port, Duration::from_secs(10));
        let pushed = cmd(&mut pusher, &["RPUSH", &key, "v1"]);

        // moon#539: on a key the CLIENT'S OWN shard owns, the blocking pop
        // above materialises an empty zset, so this RPUSH cannot succeed and
        // the scenario is unreachable for that key. That is a separate,
        // pre-existing defect (redis creates nothing here); skip those keys
        // rather than assert on them, and count what remains so this cannot
        // quietly become a test of zero keys. At --shards 4 roughly 3 in 4
        // keys are remote and therefore usable.
        if pushed.starts_with("-WRONGTYPE") {
            blockers.push(blocker);
            continue;
        }
        assert_eq!(
            pushed, ":1\r\n",
            "{key}: RPUSH answered neither :1 nor WRONGTYPE — the scenario is \
             not set up the way this test believes"
        );
        usable += 1;

        // The blocked client must still be blocked: a read now must TIME OUT.
        match read_reply(&mut blocker) {
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Ok(reply) => wrong.push(format!("  {key} -> woken with {reply:?}")),
            Err(e) => wrong.push(format!("  {key} -> read failed: {e}")),
        }
        blockers.push(blocker);
    }

    assert!(
        usable >= 4,
        "only {usable} of {KEYS} keys were usable — too few to prove anything. \
         Every key hit moon#539's phantom-zset path, which means this test ran \
         vacuously rather than passing"
    );
    assert!(
        wrong.is_empty(),
        "moon#535 — an RPUSH destroyed the BZPOPMIN waiter on the same key for {} of {} \
         usable keys. The list waker popped a waiter it cannot serve, then removed its \
         registration and sent it a null:\n{}",
        wrong.len(),
        usable,
        wrong.join("\n")
    );
}

#[test]
fn wc5_list_blocking_still_works() {
    // The fence. The fix makes wakers refuse waiters they cannot serve; if it
    // over-refuses, blocking lists stop waking at all and this catches it.
    let m = spawn_moon();

    // A populated list must answer immediately.
    let mut c = connect(m.port, Duration::from_secs(15));
    for i in 0..KEYS {
        let key = format!("cannibal:fence:ready:{i}");
        assert_eq!(cmd(&mut c, &["RPUSH", &key, "v1"]), ":1\r\n");
        let got = cmd(&mut c, &["BLPOP", &key, "3"]);
        assert!(
            got.contains("v1"),
            "BLPOP on a populated list must return its element, got {got:?} for {key}"
        );
    }

    // And a genuinely blocked BLPOP must still be woken by a later push.
    for i in 0..KEYS {
        let key = format!("cannibal:fence:wake:{i}");
        let mut blocker = connect(m.port, Duration::from_secs(10));
        send(&mut blocker, &["BLPOP", &key, "5"]);
        std::thread::sleep(Duration::from_millis(150));

        let mut pusher = connect(m.port, Duration::from_secs(10));
        assert_eq!(cmd(&mut pusher, &["RPUSH", &key, "woke"]), ":1\r\n");

        let got = read_reply(&mut blocker).expect("blocked BLPOP must be woken by the push");
        assert!(
            got.contains("woke"),
            "a blocked BLPOP must still be woken by a push to its key, got {got:?} for {key}"
        );
    }
}
