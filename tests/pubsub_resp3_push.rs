//! Pub/sub must speak RESP3 the whole way, obey the protocol's own subscriber
//! rules, and have a sharded channel namespace.
//!
//! Measured against redis-server 8.6.1 (raw sockets, identical bytes). Moon's
//! deliveries are already right — `message` and `pmessage` arrive as RESP3
//! Push frames. Its CONFIRMATIONS are not: `subscribe`, `unsubscribe`,
//! `psubscribe` and `punsubscribe` are built by four functions in
//! `src/pubsub/mod.rs` that hardcode `Frame::Array` and take no protocol
//! argument, so they cannot answer differently under RESP3.
//!
//! That half-correctness is worse than absence. A RESP3 client tells an
//! out-of-band push from a command reply by the leading byte. Moon's `message`
//! leads with `>` (out-of-band, correct) but its `subscribe` confirmation
//! leads with `*` — so a client dispatching on frame type reads the
//! confirmation as the reply to whatever it sends NEXT, and every later reply
//! on that connection is off by one.
//!
//! These tests speak the raw socket rather than going through redis-rs,
//! because the entire contract is WHICH BYTE LEADS THE FRAME and every Redis
//! client library normalises `>` and `*` to the same value before a test could
//! see the difference. A library-based suite would pass against the bug it
//! exists to catch.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Redis 8.6.1's verbatim subscriber-mode refusal. Byte-compared: Moon's
/// current text is a near-miss (`(P)SUBSCRIBE` for `(P|S)SUBSCRIBE`, and no
/// `RESET`), and a substring match on "allowed in this context" would accept
/// it.
const ERR_JAIL_GET: &str = "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context";
const ERR_JAIL_HELLO: &str = "ERR Can't execute 'hello': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context";

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
    // CARGO_BIN_EXE_moon is the binary cargo built for THIS invocation —
    // fresh and feature-matched. Never probe target/release directly: that
    // path's provenance is unknown and has produced false PASSes before.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-pubsub3-{port}"));
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
                // This host hovers near the 5% diskfull line; the guard would
                // turn writes into MOONERR and flake the suite.
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-pubsub3-{port}"));
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
    // Never skip. A silent early return would let this suite report green
    // while exercising no server at all.
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port} ({status})\n--- stderr ---\n{log}");
}

/// A raw client connection.
struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_millis(700)))
            .expect("read timeout");
        s.set_write_timeout(Some(Duration::from_secs(5)))
            .expect("write timeout");
        Conn(s)
    }

    fn hello3(port: u16) -> Self {
        let mut c = Self::open(port);
        let r = c.send(&["HELLO", "3"]);
        assert!(
            !r.is_empty() && r[0] != b'-',
            "HELLO 3 must be accepted; got {}",
            String::from_utf8_lossy(&r)
        );
        c
    }

    /// Send a command and read whatever comes back within the read timeout.
    fn send(&mut self, parts: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
        }
        self.0.write_all(&out).expect("write command");
        self.drain()
    }

    /// Read until the socket goes quiet for one timeout window.
    fn drain(&mut self) -> Vec<u8> {
        let mut got = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    got.extend_from_slice(&buf[..n]);
                    if n < buf.len() {
                        // One more short read to catch a second frame that the
                        // server wrote separately (a push followed by a reply).
                        match self.0.read(&mut buf) {
                            Ok(0) => break,
                            Ok(m) => got.extend_from_slice(&buf[..m]),
                            Err(_) => break,
                        }
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        got
    }
}

/// Split a byte buffer into top-level RESP frames, returning `None` if any
/// frame is truncated or malformed.
///
/// This exists for the tear test: asserting "both frames are present" with a
/// substring search would pass on a buffer where one frame is spliced into the
/// middle of the other, which is precisely the corruption being ruled out.
fn split_frames(buf: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < buf.len() {
        let start = i;
        i = frame_end(buf, i)?;
        out.push(buf[start..i].to_vec());
    }
    Some(out)
}

/// Index just past the frame starting at `i`, or `None` if incomplete.
fn frame_end(buf: &[u8], i: usize) -> Option<usize> {
    let tag = *buf.get(i)?;
    let line_end = find_crlf(buf, i)?;
    match tag {
        b'+' | b'-' | b':' | b',' | b'#' | b'(' | b'_' => Some(line_end),
        b'$' | b'=' => {
            let n: i64 = std::str::from_utf8(&buf[i + 1..line_end - 2])
                .ok()?
                .parse()
                .ok()?;
            if n < 0 {
                return Some(line_end);
            }
            let end = line_end + n as usize + 2;
            if end > buf.len() { None } else { Some(end) }
        }
        b'*' | b'~' | b'>' | b'%' => {
            let n: i64 = std::str::from_utf8(&buf[i + 1..line_end - 2])
                .ok()?
                .parse()
                .ok()?;
            if n < 0 {
                return Some(line_end);
            }
            let count = if tag == b'%' {
                n as usize * 2
            } else {
                n as usize
            };
            let mut j = line_end;
            for _ in 0..count {
                j = frame_end(buf, j)?;
            }
            Some(j)
        }
        _ => None,
    }
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i + 2);
        }
        i += 1;
    }
    None
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Assert every frame in `buf` whose payload names `verb` leads with `lead`.
fn assert_confirmation_lead(buf: &[u8], verb: &str, lead: u8, ctx: &str) {
    let frames = split_frames(buf).unwrap_or_else(|| {
        panic!(
            "{ctx}: reply is not a sequence of whole frames: {:?}",
            s(buf)
        )
    });
    let hit = frames
        .iter()
        .find(|f| f.windows(verb.len()).any(|w| w == verb.as_bytes()));
    let f = hit.unwrap_or_else(|| {
        panic!("{ctx}: no `{verb}` confirmation in {:?}", s(buf));
    });
    assert_eq!(
        f[0] as char,
        lead as char,
        "{ctx}: `{verb}` confirmation must lead with `{}`; got {:?}. A RESP3 client tells an \
         out-of-band push from a command reply by this byte alone.",
        lead as char,
        s(f)
    );
}

// ── Must 1 · 2 — confirmation framing ────────────────────────────────────────

#[test]
fn ps1_resp3_subscribe_confirmation_is_push() {
    let m = spawn_moon("1");
    let mut c = Conn::hello3(m.port);
    let r = c.send(&["SUBSCRIBE", "ch"]);
    assert_confirmation_lead(&r, "subscribe", b'>', "RESP3 SUBSCRIBE");
    // Contents must be untouched by the retype.
    assert!(
        r.ends_with(b":1\r\n"),
        "the confirmation still carries the subscription count; got {:?}",
        s(&r)
    );
}

#[test]
fn ps2_resp3_all_four_confirmations_are_push() {
    let m = spawn_moon("1");
    let mut c = Conn::hello3(m.port);
    for (parts, verb) in [
        (vec!["SUBSCRIBE", "ch"], "subscribe"),
        (vec!["PSUBSCRIBE", "p.*"], "psubscribe"),
        (vec!["UNSUBSCRIBE", "ch"], "unsubscribe"),
        (vec!["PUNSUBSCRIBE", "p.*"], "punsubscribe"),
    ] {
        let r = c.send(&parts);
        assert_confirmation_lead(&r, verb, b'>', "RESP3");
    }
}

#[test]
fn ps3_resp2_confirmations_stay_array() {
    // The regression guard: every existing RESP2 client must see byte-for-byte
    // what it sees today.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    for (parts, verb) in [
        (vec!["SUBSCRIBE", "ch"], "subscribe"),
        (vec!["PSUBSCRIBE", "p.*"], "psubscribe"),
        (vec!["UNSUBSCRIBE", "ch"], "unsubscribe"),
        (vec!["PUNSUBSCRIBE", "p.*"], "punsubscribe"),
    ] {
        let r = c.send(&parts);
        assert_confirmation_lead(&r, verb, b'*', "RESP2");
    }
}

// ── Must 3 — RESP3 lifts the subscriber jail ────────────────────────────────

#[test]
fn ps4_resp3_subscribed_connection_runs_commands() {
    let m = spawn_moon("1");
    let mut c = Conn::hello3(m.port);
    c.send(&["SUBSCRIBE", "ch"]);

    let set = c.send(&["SET", "k", "v"]);
    assert!(
        set.starts_with(b"+OK"),
        "RESP3 lifts the subscriber restriction — SET must be answered, got {:?}",
        s(&set)
    );
    let get = c.send(&["GET", "k"]);
    assert!(
        get.windows(1).any(|w| w == b"v"),
        "GET must return the value, got {:?}",
        s(&get)
    );

    // ...and the connection is STILL subscribed. Running commands must not
    // silently drop the subscription — that would trade one bug for a worse one.
    let mut pubc = Conn::open(m.port);
    pubc.send(&["PUBLISH", "ch", "hi"]);
    let push = c.drain();
    assert!(
        push.starts_with(b">"),
        "the subscription must survive running commands; expected a push, got {:?}",
        s(&push)
    );
}

#[test]
fn ps5_resp3_reply_and_delivery_do_not_tear() {
    // The ⚠ flag's test. Moon's handlers were written assuming a subscribed
    // connection only ever WRITES pushes; lifting the jail means a command
    // reply and a delivery can be in flight together. Substring-searching for
    // both would pass on a buffer where one is spliced into the middle of the
    // other, so this parses the buffer frame by frame instead.
    let m = spawn_moon("1");
    let mut c = Conn::hello3(m.port);
    c.send(&["SUBSCRIBE", "ch"]);

    let mut pubc = Conn::open(m.port);
    pubc.send(&["PUBLISH", "ch", "hi"]);
    let buf = c.send(&["PING"]);

    let frames = split_frames(&buf).unwrap_or_else(|| {
        panic!(
            "a push and a reply on one connection produced a TORN buffer — not a \
             sequence of whole frames: {:?}",
            s(&buf)
        )
    });
    assert!(
        frames.iter().any(|f| f.starts_with(b">")),
        "the delivery must arrive as a whole Push frame; got {:?}",
        s(&buf)
    );
    assert!(
        frames.iter().any(|f| f.starts_with(b"+PONG")),
        "the PING reply must arrive whole and as a SimpleString under RESP3; got {:?}",
        s(&buf)
    );
}

// ── Must 4 · 5 · 6 · Reject 1 · 2 — subscriber-mode rules ───────────────────

#[test]
fn ps6_resp2_jail_error_is_verbatim() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SUBSCRIBE", "ch"]);
    let r = c.send(&["GET", "k"]);
    assert_eq!(
        s(&r),
        format!("-{ERR_JAIL_GET}\r\n"),
        "the refusal is byte-compared: Moon's near-miss text omits the sharded verbs and RESET, \
         and a driver that string-matches sees a different error"
    );
    // Refused, not killed, and still subscribed.
    let mut pubc = Conn::open(m.port);
    pubc.send(&["PUBLISH", "ch", "hi"]);
    let push = c.drain();
    assert!(
        push.windows(7).any(|w| w == b"message"),
        "a refused command must leave the subscription intact; got {:?}",
        s(&push)
    );
}

#[test]
fn ps7_resp2_allow_list_admits_sharded_verbs() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SUBSCRIBE", "ch"]);
    let r = c.send(&["SSUBSCRIBE", "sch"]);
    assert!(
        !r.starts_with(b"-"),
        "SSUBSCRIBE is on Redis's subscriber-mode allow-list; got {:?}",
        s(&r)
    );
    assert!(
        r.windows(10).any(|w| w == b"ssubscribe"),
        "expected an ssubscribe confirmation; got {:?}",
        s(&r)
    );
}

#[test]
fn ps8_reset_escapes_subscriber_mode() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SUBSCRIBE", "ch"]);
    let r = c.send(&["RESET"]);
    assert_eq!(s(&r), "+RESET\r\n", "RESET is allowed while subscribed");
    let g = c.send(&["GET", "k"]);
    assert!(
        !g.starts_with(b"-"),
        "after RESET the connection is out of subscriber mode; got {:?}",
        s(&g)
    );
}

#[test]
fn ps9_ping_shape_follows_protocol() {
    let m = spawn_moon("1");

    let mut two = Conn::open(m.port);
    two.send(&["SUBSCRIBE", "ch"]);
    let r2 = two.send(&["PING"]);
    assert_eq!(
        s(&r2),
        "*2\r\n$4\r\npong\r\n$0\r\n\r\n",
        "RESP2 subscriber-mode PING keeps its array shape"
    );

    let mut three = Conn::hello3(m.port);
    three.send(&["SUBSCRIBE", "ch"]);
    let r3 = three.send(&["PING"]);
    assert_eq!(
        s(&r3),
        "+PONG\r\n",
        "under RESP3 the subscriber-mode array shape does not apply — the shape follows the \
         PROTOCOL, not the mode"
    );
}

#[test]
fn ps10_hello_cannot_escape_the_jail() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SUBSCRIBE", "ch"]);
    let r = c.send(&["HELLO", "3"]);
    assert_eq!(
        s(&r),
        format!("-{ERR_JAIL_HELLO}\r\n"),
        "HELLO is NOT on the allow-list (measured) — a RESP2 subscriber cannot upgrade \
         mid-subscription. handler_single.rs currently advertises HELLO as allowed in its error \
         text while refusing it."
    );
    // Still RESP2: the confirmation must stay an Array.
    let sub = c.send(&["SUBSCRIBE", "ch2"]);
    assert_confirmation_lead(&sub, "subscribe", b'*', "after a refused HELLO");
}

// ── Must 8 · 9 · 10 — sharded pub/sub ───────────────────────────────────────

#[test]
fn ps11_sharded_publish_delivers_smessage() {
    let m = spawn_moon("1");
    let mut sub = Conn::open(m.port);
    let conf = sub.send(&["SSUBSCRIBE", "sch"]);
    assert!(
        conf.windows(10).any(|w| w == b"ssubscribe"),
        "SSUBSCRIBE must confirm; got {:?}",
        s(&conf)
    );

    let mut pubc = Conn::open(m.port);
    let n = pubc.send(&["SPUBLISH", "sch", "hi"]);
    assert_eq!(
        s(&n),
        ":1\r\n",
        "SPUBLISH reports the sharded receiver count"
    );

    let got = sub.drain();
    assert!(
        got.windows(8).any(|w| w == b"smessage"),
        "the sharded delivery event is `smessage`, not `message`; got {:?}",
        s(&got)
    );
}

#[test]
fn ps12_namespaces_do_not_leak() {
    let m = spawn_moon("1");
    let mut plain = Conn::open(m.port);
    plain.send(&["SUBSCRIBE", "dual"]);
    let mut sharded = Conn::open(m.port);
    sharded.send(&["SSUBSCRIBE", "dual"]);

    let mut pubc = Conn::open(m.port);
    pubc.send(&["SPUBLISH", "dual", "hi"]);
    assert!(
        plain.drain().is_empty(),
        "SPUBLISH must NOT reach a plain SUBSCRIBE on the same name"
    );
    assert!(
        sharded.drain().windows(8).any(|w| w == b"smessage"),
        "SPUBLISH must reach the SSUBSCRIBE side"
    );

    pubc.send(&["PUBLISH", "dual", "hi"]);
    assert!(
        sharded.drain().is_empty(),
        "PUBLISH must NOT reach an SSUBSCRIBE on the same name"
    );
    assert!(
        plain.drain().windows(7).any(|w| w == b"message"),
        "PUBLISH must reach the plain side"
    );
}

#[test]
fn ps13_pubsub_shard_introspection() {
    let m = spawn_moon("1");
    let mut sub = Conn::open(m.port);
    sub.send(&["SSUBSCRIBE", "sch"]);
    let mut c = Conn::open(m.port);

    let chans = c.send(&["PUBSUB", "SHARDCHANNELS"]);
    assert!(
        chans.windows(3).any(|w| w == b"sch"),
        "SHARDCHANNELS must list the sharded channel; got {:?}",
        s(&chans)
    );
    let nums = c.send(&["PUBSUB", "SHARDNUMSUB", "sch"]);
    assert!(
        nums.ends_with(b":1\r\n"),
        "SHARDNUMSUB must report one subscriber; got {:?}",
        s(&nums)
    );
    // The namespaces stay separate in introspection too.
    let plain = c.send(&["PUBSUB", "CHANNELS"]);
    assert!(
        !plain.windows(3).any(|w| w == b"sch"),
        "PUBSUB CHANNELS must not list a SHARDED channel; got {:?}",
        s(&plain)
    );
}

// ── Must 11 · 12 · Reject 3 · 4 ─────────────────────────────────────────────

#[test]
fn ps14_numpat_counts_distinct_patterns() {
    // BOTH subscribers must be live at once. After one leaves, the buggy
    // (per-subscriber) and correct (per-pattern) answers coincide at 1 and this
    // test would pass against the bug — which is how it survived to #480.
    let m = spawn_moon("1");
    let mut a = Conn::open(m.port);
    let mut b = Conn::open(m.port);
    a.send(&["PSUBSCRIBE", "p.*"]);
    b.send(&["PSUBSCRIBE", "p.*"]);

    let mut c = Conn::open(m.port);
    let r = c.send(&["PUBSUB", "NUMPAT"]);
    assert_eq!(
        s(&r),
        ":1\r\n",
        "NUMPAT counts DISTINCT patterns, not subscribers: two connections on `p.*` is one \
         pattern. Moon answers :2."
    );
    drop((a, b));
}

#[test]
fn ps15_unsubscribe_from_nothing_names_null_channel() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let r = c.send(&["UNSUBSCRIBE"]);
    assert_eq!(
        s(&r),
        "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n",
        "with nothing subscribed the channel name is Null ($-1), not an empty string ($0)"
    );
}

#[test]
fn ps16_sharded_verbs_check_arity() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    let a = c.send(&["SSUBSCRIBE"]);
    assert_eq!(
        s(&a),
        "-ERR wrong number of arguments for 'ssubscribe' command\r\n",
        "the error must name the command the client sent"
    );
    let b = c.send(&["SPUBLISH", "only-a-channel"]);
    assert_eq!(
        s(&b),
        "-ERR wrong number of arguments for 'spublish' command\r\n"
    );
    // Arity errors do not kill the connection.
    assert!(
        c.send(&["PING"]).starts_with(b"+PONG"),
        "an arity error must leave the connection serving"
    );
}

#[test]
fn ps18_sharded_delivery_crosses_shards() {
    // The half a --shards 1 suite cannot see. Plain PUBLISH fans out to remote
    // shards via `remote_subscriber_map` + an SPSC message; a sharded registry
    // that only ever serves its OWN shard would pass every other sharded test
    // here and silently drop (N-1)/N of deliveries in production.
    //
    // This is the same shape that bit keyspace notifications: a delivery path
    // proven at one shard and broken at four.
    let m = spawn_moon("4");

    // Enough distinct channels that at least one subscriber must land on a
    // shard other than the publisher's, whatever the hash does.
    let channels = ["sc0", "sc1", "sc2", "sc3", "sc4", "sc5", "sc6", "sc7"];
    let mut subs: Vec<Conn> = Vec::new();
    for ch in channels {
        let mut c = Conn::open(m.port);
        c.send(&["SSUBSCRIBE", ch]);
        subs.push(c);
    }

    let mut pubc = Conn::open(m.port);
    for ch in channels {
        let n = pubc.send(&["SPUBLISH", ch, "hi"]);
        assert_eq!(
            s(&n),
            ":1\r\n",
            "SPUBLISH must report the subscriber on whichever shard owns `{ch}`"
        );
    }

    let mut delivered = 0usize;
    for c in subs.iter_mut() {
        if c.drain().windows(8).any(|w| w == b"smessage") {
            delivered += 1;
        }
    }
    assert_eq!(
        delivered,
        channels.len(),
        "every sharded subscriber must be reached at --shards 4, not only those \
         that happened to hash to the publisher's own shard"
    );
}

// ── Must 7 — every handler agrees ───────────────────────────────────────────

#[test]
fn ps17_all_handlers_agree() {
    // Moon states the subscriber-mode allow-list in THREE handlers with TWO
    // different texts today, and the confirmation builders are shared but
    // reached from each. Re-running the two headline rules at a different shard
    // count routes through a different handler, so a fix that lands in one and
    // not the others fails here rather than in production.
    let m = spawn_moon("4");

    let mut three = Conn::hello3(m.port);
    let conf = three.send(&["SUBSCRIBE", "ch"]);
    assert_confirmation_lead(&conf, "subscribe", b'>', "4-shard RESP3");

    let mut two = Conn::open(m.port);
    two.send(&["SUBSCRIBE", "ch"]);
    let jailed = two.send(&["GET", "k"]);
    assert_eq!(
        s(&jailed),
        format!("-{ERR_JAIL_GET}\r\n"),
        "the allow-list must read identically on every handler"
    );

    let lifted = three.send(&["GET", "k"]);
    assert!(
        !lifted.starts_with(b"-"),
        "RESP3 lifts the jail on every handler too; got {:?}",
        s(&lifted)
    );
}
