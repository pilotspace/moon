//! Keyspace notifications: `__keyspace@<db>__` / `__keyevent@<db>__`.
//!
//! Moon has none. Measured: zero occurrences of `notify-keyspace-events`,
//! `__keyspace@` or `__keyevent@` anywhere in `src/` or `tests/`. Cache
//! invalidation frameworks and change-data-capture consumers subscribe to
//! these channels and currently get silence from Moon.
//!
//! Every expectation below was captured from redis-server 8.6.1 rather than
//! recalled, which mattered — three of them are counter-intuitive:
//!
//!   * `INCR` publishes `incrby`, NOT `incr`.
//!   * `RENAME` publishes TWO events: `rename_from` on the source key and
//!     `rename_to` on the destination.
//!   * a key MISS publishes nothing under `A`, because `m` (keymiss) is
//!     deliberately not a member of the `A` class.
//!
//! The cross-shard case (`kn9`) is the one most likely to be quietly wrong:
//! Moon keeps one pub/sub registry PER SHARD, a write runs on the shard that
//! owns the key, and the subscriber sits on whichever shard accepted its
//! connection. A local-only publish passes at `--shards 1` and fails at 4.
//! That exact mistake is live elsewhere in the tree — see issue #474.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Redis 8.6.1, verbatim.
const INVALID_FLAG_ERR: &str = "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.";

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

fn spawn_moon(shards: &str) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-keyspacenotif-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-keyspacenotif-{port}"));
    let mut moon = Moon {
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
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port} ({status})\n--- stderr ---\n{log}");
}

struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        Conn(s)
    }

    fn send(&mut self, parts: &[&str]) -> String {
        self.write(parts);
        self.read_reply()
    }

    fn write(&mut self, parts: &[&str]) {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.0.write_all(out.as_bytes()).expect("write");
    }

    fn read_reply(&mut self) -> String {
        let mut buf = [0u8; 16384];
        let mut acc = Vec::new();
        loop {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    acc.extend_from_slice(&buf[..n]);
                    self.0
                        .set_read_timeout(Some(Duration::from_millis(200)))
                        .unwrap();
                }
                Err(_) => break,
            }
        }
        self.0
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        String::from_utf8_lossy(&acc).into_owned()
    }

    /// Drain pmessages for `window`, returning (channel, payload) pairs.
    ///
    /// Waits the full window rather than returning on first message: a test
    /// asserting that NOTHING arrives must not pass merely by reading early.
    fn collect_pmessages(&mut self, window: Duration) -> Vec<(String, String)> {
        self.0
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let deadline = Instant::now() + window;
        let mut acc = Vec::new();
        let mut buf = [0u8; 16384];
        while Instant::now() < deadline {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => acc.extend_from_slice(&buf[..n]),
                Err(_) => {}
            }
        }
        let text = String::from_utf8_lossy(&acc).into_owned();
        // RESP arrays: pmessage / <pattern> / <channel> / <payload>
        let lines: Vec<&str> = text
            .split("\r\n")
            .filter(|l| !l.is_empty() && !l.starts_with('*') && !l.starts_with('$'))
            .collect();
        let mut out = Vec::new();
        for (i, l) in lines.iter().enumerate() {
            if *l == "pmessage" && i + 3 < lines.len() {
                out.push((lines[i + 2].to_string(), lines[i + 3].to_string()));
            }
        }
        out
    }
}

/// Subscribe to a pattern and return the connection, ready to collect.
fn psubscriber(port: u16, pattern: &str) -> Conn {
    let mut c = Conn::open(port);
    let ack = c.send(&["PSUBSCRIBE", pattern]);
    assert!(
        ack.contains("psubscribe"),
        "PSUBSCRIBE not acknowledged: {ack:?}"
    );
    c
}

fn enable(port: u16, flags: &str) {
    let mut c = Conn::open(port);
    let r = c.send(&["CONFIG", "SET", "notify-keyspace-events", flags]);
    assert!(
        r.starts_with("+OK"),
        "CONFIG SET notify-keyspace-events {flags:?} failed: {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Config surface
// ---------------------------------------------------------------------------

#[test]
fn kn1_invalid_flag_char_verbatim() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]);

    let err = c.send(&["CONFIG", "SET", "notify-keyspace-events", "KEQ"]);
    assert!(
        err.starts_with('-'),
        "an out-of-class flag char must be rejected; got {err:?}"
    );
    assert!(
        err.contains(INVALID_FLAG_ERR),
        "the error must name the valid class set the way Redis does, so a \
         config-management tool can surface it unchanged. want {INVALID_FLAG_ERR:?}, got {err:?}"
    );

    // A rejected SET must not have partially applied.
    let readback = c.send(&["CONFIG", "GET", "notify-keyspace-events"]);
    assert!(
        readback.contains("AKE"),
        "a rejected CONFIG SET must leave the PREVIOUS value intact — a \
         half-applied flag set silently changes which events fire. got {readback:?}"
    );
}

#[test]
fn kn2_flags_canonicalized() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    for (set, want) in [("KEA", "AKE"), ("Kg$", "g$K"), ("xe", "xe"), ("Km", "Km")] {
        let r = c.send(&["CONFIG", "SET", "notify-keyspace-events", set]);
        assert!(r.starts_with("+OK"), "CONFIG SET {set:?}: {r:?}");
        let got = c.send(&["CONFIG", "GET", "notify-keyspace-events"]);
        assert!(
            got.contains(want),
            "readback is canonicalized, not echoed: classes in 'g$lshzxetdmn' \
             order, then K, then E. set {set:?} want {want:?}, got {got:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Event wire form
// ---------------------------------------------------------------------------

#[test]
fn kn3_keyspace_and_keyevent_inverted() {
    let m = spawn_moon("1");
    enable(m.port, "KEA");
    let mut sub = psubscriber(m.port, "__key*@0__:*");

    let mut w = Conn::open(m.port);
    w.send(&["SET", "kn3key", "v"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.contains(&("__keyspace@0__:kn3key".into(), "set".into())),
        "keyspace channel is named for the KEY and carries the EVENT; got {msgs:?}"
    );
    assert!(
        msgs.contains(&("__keyevent@0__:set".into(), "kn3key".into())),
        "keyevent channel is named for the EVENT and carries the KEY — the \
         pair is inverted, and getting it backwards breaks every consumer; got {msgs:?}"
    );
}

#[test]
fn kn4_incr_reports_incrby() {
    let m = spawn_moon("1");
    enable(m.port, "KEA");
    let mut sub = psubscriber(m.port, "__keyevent@0__:*");

    let mut w = Conn::open(m.port);
    w.send(&["INCR", "kn4ctr"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.iter().any(|(ch, _)| ch == "__keyevent@0__:incrby"),
        "INCR publishes 'incrby', not 'incr' — the event name is the internal \
         operation, not the command the client typed. got {msgs:?}"
    );
}

#[test]
fn kn5_rename_emits_both_halves() {
    let m = spawn_moon("1");
    enable(m.port, "KEA");
    let mut w = Conn::open(m.port);
    w.send(&["SET", "kn5src", "v"]);

    let mut sub = psubscriber(m.port, "__keyevent@0__:*");
    w.send(&["RENAME", "kn5src", "kn5dst"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.contains(&("__keyevent@0__:rename_from".into(), "kn5src".into())),
        "RENAME emits rename_from carrying the SOURCE key; got {msgs:?}"
    );
    assert!(
        msgs.contains(&("__keyevent@0__:rename_to".into(), "kn5dst".into())),
        "RENAME emits a SECOND event, rename_to, carrying the DESTINATION — a \
         consumer tracking key lifetimes needs both halves; got {msgs:?}"
    );
}

#[test]
fn kn6_expired_event() {
    let m = spawn_moon("1");
    enable(m.port, "KEA");
    let mut sub = psubscriber(m.port, "__keyevent@0__:expired");

    let mut w = Conn::open(m.port);
    w.send(&["SET", "kn6vol", "v", "PX", "60"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(3));
    assert!(
        msgs.iter().any(|(_, key)| key == "kn6vol"),
        "an elapsed TTL must publish 'expired' — cache consumers rely on it to \
         invalidate; got {msgs:?}"
    );
}

/// moon#542 fence: a key READ after its TTL elapsed (the lazy-expiry path,
/// which HIDES the key and defers deletion to the sweep drain) must still
/// publish `expired`. Before #542 the lazy path deleted the key silently —
/// whichever of read/sweep won the race decided whether subscribers heard
/// about it at all. The read must answer nil AND the event must arrive.
#[test]
fn kn6b_expired_event_after_lazy_read() {
    let m = spawn_moon("1");
    enable(m.port, "KEA");
    let mut sub = psubscriber(m.port, "__keyevent@0__:expired");

    let mut w = Conn::open(m.port);
    w.send(&["SET", "kn6lazy", "v", "PX", "40"]);
    std::thread::sleep(Duration::from_millis(60));
    let got = w.send(&["GET", "kn6lazy"]);
    assert!(
        got.contains("$-1") || got.contains("nil") || got.starts_with("_"),
        "elapsed key must read as absent; got {got:?}"
    );

    let msgs = sub.collect_pmessages(Duration::from_secs(3));
    assert!(
        msgs.iter().any(|(_, key)| key == "kn6lazy"),
        "a lazily-read expired key must still publish 'expired'; got {msgs:?}"
    );
}

// ---------------------------------------------------------------------------
// Gating — the cases where NOTHING must arrive
// ---------------------------------------------------------------------------

#[test]
fn kn7_keymiss_silent_under_a() {
    let m = spawn_moon("1");
    enable(m.port, "KEA");
    let mut sub = psubscriber(m.port, "__key*@0__:*");

    let mut w = Conn::open(m.port);
    w.send(&["GET", "kn7definitelymissing"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.is_empty(),
        "'m' (keymiss) is deliberately NOT part of the 'A' class — publishing \
         on every miss would put a pub/sub fan-out on the read path. got {msgs:?}"
    );

    // ...but it DOES fire when asked for explicitly.
    enable(m.port, "Km");
    let mut sub2 = psubscriber(m.port, "__key*@0__:*");
    w.send(&["GET", "kn7definitelymissing"]);
    let msgs2 = sub2.collect_pmessages(Duration::from_secs(2));
    assert!(
        !msgs2.is_empty(),
        "with 'm' set explicitly a key miss MUST publish; got nothing"
    );
}

#[test]
fn kn8_k_or_e_required() {
    let m = spawn_moon("1");
    // Class flags select WHICH events; K/E select WHETHER they are delivered.
    enable(m.port, "g$");
    let mut sub = psubscriber(m.port, "__key*@0__:*");

    let mut w = Conn::open(m.port);
    w.send(&["SET", "kn8key", "v"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.is_empty(),
        "with neither K nor E set nothing is delivered, however many class \
         flags are on; got {msgs:?}"
    );
}

/// NOTE: this is the one test in this file that passes BEFORE the feature
/// exists, because a server that can publish nothing trivially publishes
/// nothing. It is a guard against the default flipping on, not evidence that
/// gating works — `kn8` is what proves gating. Do not read its green as
/// progress.
#[test]
fn kn10_disabled_emits_nothing() {
    let m = spawn_moon("1");
    // No enable() — the default is off, and must stay off.
    let mut sub = psubscriber(m.port, "__key*@0__:*");

    let mut w = Conn::open(m.port);
    w.send(&["SET", "kn10key", "v"]);
    w.send(&["DEL", "kn10key"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.is_empty(),
        "notifications are off by default and must cost nothing; got {msgs:?}"
    );
}

// ---------------------------------------------------------------------------
// kn9 — the one a single-shard test cannot catch. See issue #474.
// ---------------------------------------------------------------------------

#[test]
fn kn9_cross_shard_delivery() {
    let m = spawn_moon("4");
    enable(m.port, "KEA");
    let mut sub = psubscriber(m.port, "__keyevent@0__:set");

    // Distinct key names hash to different shards; the subscriber sits on
    // whichever shard accepted ITS connection, which is at most one of them.
    let keys = [
        "kn9:alpha",
        "kn9:beta",
        "kn9:gamma",
        "kn9:delta",
        "kn9:epsilon",
        "kn9:zeta",
        "kn9:eta",
        "kn9:theta",
    ];
    let mut w = Conn::open(m.port);
    for k in keys {
        w.send(&["SET", k, "v"]);
    }

    let msgs = sub.collect_pmessages(Duration::from_secs(3));
    let got: std::collections::HashSet<&str> = msgs.iter().map(|(_, key)| key.as_str()).collect();
    let missing: Vec<&str> = keys.iter().copied().filter(|k| !got.contains(k)).collect();
    assert!(
        missing.is_empty(),
        "a notification must reach a subscriber on ANY shard, not just the \
         shard that owns the mutated key. Moon keeps one pub/sub registry per \
         shard, so a local-only publish passes at --shards 1 and drops roughly \
         (N-1)/N of events at --shards N. missing: {missing:?} (got {got:?})"
    );
}
