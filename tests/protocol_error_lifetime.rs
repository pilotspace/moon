//! A malformed frame must end the connection the way Redis ends it.
//!
//! Measured against redis-server 8.6.1 (raw sockets, identical bytes). Moon's
//! divergences all trace to ONE line — `handler_sharded/mod.rs`:
//!
//! ```text
//! Err(crate::protocol::ParseError::Incomplete) => break,
//! Err(_) => { break_outer = true; break; }
//! ```
//!
//! That arm throws away two things: the parse error's *reason* (so the client
//! gets a bare FIN and cannot tell a bad encoder from a dropped network), and
//! `batch` — which already holds every VALID frame parsed before the bad one,
//! so `PING\r\n*-9\r\n` in one write answers nothing at all.
//!
//! These tests speak the raw socket rather than going through redis-rs,
//! because the entire subject is bytes-and-close behavior that a client
//! library normalises away. Each case asserts THREE things, since any one
//! alone hides the interesting half:
//!   * the reply bytes,
//!   * whether the server hung up,
//!   * whether a following command still gets served.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Redis 8.6.1's verbatim wire text. Byte-compared, not substring-matched
/// where the whole string is known — a near-miss ("invalid bulk string
/// length") is exactly the bug this task exists to fix.
const ERR_BULK_LEN: &str = "ERR Protocol error: invalid bulk length";
const ERR_INLINE_TOO_BIG: &str = "ERR Protocol error: too big inline request";
const ERR_UNBALANCED: &str = "ERR Protocol error: unbalanced quotes in request";

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-protoerr-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-protoerr-{port}"));
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

/// What a server did with one payload: what it said, and what it did to the
/// socket afterwards.
#[derive(Debug)]
struct Outcome {
    reply: Vec<u8>,
    closed: bool,
    /// Reply to a follow-up PING, or `None` if the link was already gone.
    /// `Some(b"")` means the connection is open but MUTE — a stall, which is
    /// its own distinct failure and must never be mistaken for a clean close.
    after: Option<Vec<u8>>,
}

impl Outcome {
    fn reply_str(&self) -> String {
        String::from_utf8_lossy(&self.reply).into_owned()
    }
    fn alive_and_serving(&self) -> bool {
        matches!(&self.after, Some(a) if a.starts_with(b"+PONG"))
    }
}

/// Send `payload`, drain the answer, then test whether the link survives.
fn probe(port: u16, payload: &[u8]) -> Outcome {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    // Generous: a loaded CI host has truncated real replies into the
    // follow-up read at 2s, which manufactures phantom "empty reply" results.
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(payload).expect("write payload");

    let mut reply = Vec::new();
    let mut closed = false;
    let mut buf = [0u8; 8192];
    loop {
        match s.read(&mut buf) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(n) => {
                reply.extend_from_slice(&buf[..n]);
                if reply.len() > 4096 {
                    break;
                }
                // Keep draining briefly: an error frame and the close often
                // arrive in separate segments, and stopping at the first read
                // would report "did not close" for a server that did.
                // NOT unwrapped: once the peer closes (which is exactly what
                // an oversized request provokes), macOS fails this setsockopt
                // with EINVAL. Panicking here would report a test bug as a
                // server bug — pe4 did precisely that.
                let _ = s.set_read_timeout(Some(Duration::from_millis(300)));
            }
            // A timeout means "nothing more is coming", NOT that the peer
            // hung up. Conflating the two is how a stall gets misreported.
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(_) => {
                closed = true;
                break;
            }
        }
    }

    if closed {
        return Outcome {
            reply,
            closed,
            after: None,
        };
    }

    let _ = s.set_read_timeout(Some(Duration::from_secs(3)));
    let after = match s.write_all(b"*1\r\n$4\r\nPING\r\n") {
        Ok(()) => {
            let mut a = Vec::new();
            match s.read(&mut buf) {
                Ok(0) => None,
                Ok(n) => {
                    a.extend_from_slice(&buf[..n]);
                    Some(a)
                }
                // Open but mute: the stall case. Distinct from a close.
                Err(_) => Some(Vec::new()),
            }
        }
        Err(_) => None,
    };
    Outcome {
        reply,
        closed,
        after,
    }
}

// ---------------------------------------------------------------------------
// pe1 — the headline: a fault must not eat the valid frames before it.
// ---------------------------------------------------------------------------

#[test]
fn pe1_valid_prefix_is_answered_before_the_fault() {
    let m = spawn_moon("1");
    // ONE write, so both frames land in the same read buffer and the parse
    // loop sees the valid PING and the bad frame together. That is precisely
    // the case `Err(_) => break` drops on the floor: `batch` already holds
    // the parsed PING and is discarded unexecuted.
    let out = probe(m.port, b"*1\r\n$4\r\nPING\r\n*-9\r\n");
    assert!(
        out.reply.starts_with(b"+PONG"),
        "the PING that arrived BEFORE the malformed frame must still be \
         answered; redis-server 8.6.1 answers it. got reply={:?} closed={}",
        out.reply_str(),
        out.closed
    );
}

// ---------------------------------------------------------------------------
// pe2 — name the fault instead of a bare close.
// ---------------------------------------------------------------------------

#[test]
fn pe2_bad_bulk_length_names_itself_then_closes() {
    let m = spawn_moon("1");
    // Three shapes of the same fault: non-numeric, negative-but-not-(-1),
    // and past the configured maximum. Redis answers all three identically.
    for bad in [
        &b"*2\r\n$3\r\nGET\r\n$abc\r\nk\r\n"[..],
        &b"*2\r\n$3\r\nGET\r\n$-5\r\nk\r\n"[..],
        &b"*2\r\n$3\r\nGET\r\n$999999999\r\nk\r\n"[..],
    ] {
        let out = probe(m.port, bad);
        let reply = out.reply_str();
        assert!(
            reply.contains(ERR_BULK_LEN),
            "malformed bulk length must be NAMED on the wire before the close.\n\
             sent    : {:?}\n\
             expected: contains {ERR_BULK_LEN:?}\n\
             got     : {reply:?} (closed={})",
            String::from_utf8_lossy(bad),
            out.closed
        );
        assert!(
            out.closed || out.after.is_none(),
            "after a protocol fault the connection must close; sent {:?}",
            String::from_utf8_lossy(bad)
        );
    }
}

// ---------------------------------------------------------------------------
// pe3 — a negative multibulk count is IGNORED, not fatal. Measured Redis.
// ---------------------------------------------------------------------------

#[test]
fn pe3_negative_multibulk_count_is_ignored_and_connection_survives() {
    let m = spawn_moon("1");
    let out = probe(m.port, b"*-9\r\n");
    assert!(
        !out.closed,
        "redis-server treats a negative multibulk count as a null array: it \
         consumes the bytes, replies nothing, and keeps serving. Moon closes."
    );
    assert!(
        out.alive_and_serving(),
        "connection must still serve after `*-9`; follow-up PING got {:?}",
        out.after
            .as_ref()
            .map(|a| String::from_utf8_lossy(a).into_owned())
    );
}

// ---------------------------------------------------------------------------
// pe4 — Moon already BUILDS this string; it just never reaches the client.
// ---------------------------------------------------------------------------

#[test]
fn pe4_oversized_inline_request_names_itself() {
    let m = spawn_moon("1");
    // Moon's cap bites between 65_530 and 70_000 bytes (measured). 200 KB is
    // comfortably past it on both servers.
    let mut payload = Vec::from(&b"GET "[..]);
    payload.extend(std::iter::repeat_n(b'x', 200_000));
    payload.extend_from_slice(b"\r\n");
    let out = probe(m.port, &payload);
    assert!(
        out.reply_str().contains(ERR_INLINE_TOO_BIG),
        "src/protocol/inline.rs already constructs exactly this message — the \
         handler discards it. expected contains {ERR_INLINE_TOO_BIG:?}, got {:?}",
        out.reply_str()
    );
}

// ---------------------------------------------------------------------------
// pe5 — an unbalanced quote is a protocol fault, not a key name.
// ---------------------------------------------------------------------------

#[test]
fn pe5_unbalanced_quote_is_rejected_not_silently_accepted() {
    let m = spawn_moon("1");
    let out = probe(m.port, b"GET \"unclosed\r\n");
    let reply = out.reply_str();
    assert!(
        !reply.starts_with("$-1"),
        "Moon currently ACCEPTS the unterminated quote, treating it as part of \
         the key, and answers a nil lookup. redis-server rejects the request. \
         got {reply:?}"
    );
    assert!(
        reply.contains(ERR_UNBALANCED),
        "expected contains {ERR_UNBALANCED:?}, got {reply:?}"
    );
}

// ---------------------------------------------------------------------------
// pe6 — an error must not leave the connection open-but-mute.
// ---------------------------------------------------------------------------

#[test]
fn pe6_error_reply_does_not_stall_the_connection() {
    let m = spawn_moon("1");
    // `@bogus` is not a RESP type byte, so both servers route it to the inline
    // parser and answer "unknown command". Both then answer the PING. Redis
    // keeps serving after that; Moon stops answering — the stall this asserts.
    let out = probe(m.port, b"@bogus\r\n*1\r\n$4\r\nPING\r\n");
    assert!(
        !out.closed,
        "an unknown command must not close the connection"
    );
    assert!(
        out.reply_str().contains("PONG"),
        "the PING after the bad command must be answered; got {:?}",
        out.reply_str()
    );
    assert!(
        out.alive_and_serving(),
        "the connection must keep serving AFTER an error reply. \
         `Some(\"\")` here means open-but-mute — a stall, not a close. got {:?}",
        out.after
            .as_ref()
            .map(|a| String::from_utf8_lossy(a).into_owned())
    );
}

// ---------------------------------------------------------------------------
// pe7 — the handlers must agree. A fix in one is not a fix.
// ---------------------------------------------------------------------------

#[test]
fn pe7_all_shard_counts_agree_on_protocol_faults() {
    // shards=1 and shards=4 exercise different handler paths (the inline fast
    // path is only reachable when the key is shard-local). A protocol fault
    // must look identical regardless.
    let one = spawn_moon("1");
    let four = spawn_moon("4");
    let payloads: [&[u8]; 4] = [
        b"*2\r\n$3\r\nGET\r\n$abc\r\nk\r\n",
        b"*-9\r\n",
        b"GET \"unclosed\r\n",
        b"*1\r\n$4\r\nPING\r\n*-9\r\n",
    ];
    for p in payloads {
        let a = probe(one.port, p);
        let b = probe(four.port, p);
        assert_eq!(
            a.reply_str(),
            b.reply_str(),
            "shards=1 and shards=4 disagree on {:?}",
            String::from_utf8_lossy(p)
        );
        assert_eq!(
            a.closed,
            b.closed,
            "shards=1 and shards=4 disagree on whether to close for {:?}",
            String::from_utf8_lossy(p)
        );
    }
}

// ---------------------------------------------------------------------------
// pe8 — a pure unit test over the wire text, so a typo fails without a server.
// ---------------------------------------------------------------------------

#[test]
fn pe8_wire_text_is_redis_verbatim() {
    use moon::protocol::ProtoFault;
    // Copied from redis 8.6.1's networking.c. If one of these ever needs to
    // change, it is because Redis changed — not because Moon found it awkward.
    assert_eq!(
        ProtoFault::BulkLen.wire_text(),
        "Protocol error: invalid bulk length"
    );
    assert_eq!(
        ProtoFault::MultibulkLen.wire_text(),
        "Protocol error: invalid multibulk length"
    );
    assert_eq!(
        ProtoFault::InlineTooBig.wire_text(),
        "Protocol error: too big inline request"
    );
    assert_eq!(
        ProtoFault::UnbalancedQuotes.wire_text(),
        "Protocol error: unbalanced quotes in request"
    );
    assert_eq!(
        ProtoFault::MbulkCountTooBig.wire_text(),
        "Protocol error: too big mbulk count string"
    );
    // The two byte-carrying variants format their offender in.
    assert_eq!(
        ProtoFault::ExpectedDollar(b',').wire_text_owned(),
        "Protocol error: expected '$', got ','"
    );
    assert_eq!(
        ProtoFault::UnknownType(b'@').wire_text_owned(),
        "Protocol error: expected '$', got '@'"
    );
}
