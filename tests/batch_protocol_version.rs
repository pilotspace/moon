//! ADD task `batch-protocol-version-fidelity` — failing-first suite.
//!
//! One rule, stated once: **every reply is encoded in the protocol that was in
//! effect when that reply was produced.** A `HELLO` in the middle of a pipeline
//! changes the protocol for the replies that come AFTER it (and for its own
//! reply), never for the ones already produced.
//!
//! Moon serializes a whole batch at flush time under a single
//! `codec.protocol_version`, while `codec.set_protocol_version` fires
//! synchronously at the HELLO site mid-batch. The post-HELLO version therefore
//! retro-encodes replies produced before it.
//!
//! The UPGRADE direction (`HELLO 3` mid-batch) happens to look right today —
//! not because the batch is encoded correctly, but because
//! `apply_resp3_conversion` already flattened the earlier reply to an `Array`
//! at dispatch time under RESP2, so re-encoding it as RESP3 still emits `*`.
//! The DOWNGRADE direction has no such accident and is visibly wrong. Both
//! directions are asserted, so a fix cannot trade one for the other.
//!
//! Oracle: redis-server 8.6.1, measured 2026-08-14 over a raw socket.
//!
//!   pipelined  CONFIG GET maxmemory / HELLO 2 / CONFIG GET maxmemory
//!   on a RESP3 connection:
//!     redis -> %1 (produced under RESP3)  *14 (HELLO's own reply)  *2
//!     moon  -> *2                         *14                     *2
//!                ^ wrong: retro-downgraded
//!
//! Reproduced on monoio and tokio, shards 1 and 4 — all three dispatch paths.
//!
//! Run alone with: cargo test --test batch_protocol_version

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn spawn_moon(dir: &std::path::Path, shards: u32) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // The shared /Volumes checkout hovers near the 5% diskfull
                // guard; a tripped guard would fail this suite for an unrelated
                // reason.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        common::sigkill(&mut self.0);
    }
}

fn connect_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(s) = TcpStream::connect(format!("127.0.0.1:{port}")) {
            s.set_read_timeout(Some(Duration::from_secs(10))).ok();
            s.set_write_timeout(Some(Duration::from_secs(10))).ok();
            let mut s = s;
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = s.read(&mut buf)
                    && n > 0
                    && buf[..n].windows(4).any(|w| w == b"PONG")
                {
                    return s;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "server on {port} never answered PING"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A reply reduced to what this suite is about: its RESP type byte.
///
/// Values are deliberately discarded. Two protocols may legitimately render
/// the same value differently; what must be right is which protocol was used.
struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(port: u16, proto: u8) -> Self {
        let mut c = Conn {
            s: connect_ready(port),
            buf: Vec::with_capacity(64 * 1024),
            pos: 0,
        };
        if proto == 3 {
            c.write(&[&["HELLO", "3"]]);
            let tag = c.skip_frame();
            assert_eq!(
                tag, '%',
                "HELLO 3 must be answered with a RESP3 map or this suite proves nothing"
            );
        }
        c
    }

    /// Write every command in ONE `write_all`, so the server sees a single
    /// pipelined batch rather than a sequence of round trips. That is the whole
    /// point: a batch is what gets encoded under one protocol version.
    fn write(&mut self, batch: &[&[&str]]) {
        let mut req = Vec::with_capacity(256);
        for parts in batch {
            req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
            for p in *parts {
                req.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
            }
        }
        self.s.write_all(&req).expect("write batch");
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.s.read(&mut chunk).expect("read");
        assert!(n > 0, "connection closed mid-frame");
        self.buf.extend_from_slice(&chunk[..n]);
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let start = self.pos;
                let end = start + rel;
                let out = String::from_utf8_lossy(&self.buf[start..end]).into_owned();
                self.pos = end + 2;
                return out;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        self.pos += n + 2;
    }

    /// Consume exactly one reply and return its top-level type byte.
    fn skip_frame(&mut self) -> char {
        let line = self.line();
        let tag = line.chars().next().expect("empty frame");
        let rest = &line[1..];
        match tag {
            '+' | '-' | ':' | ',' | '#' | '_' | '(' => {}
            '$' | '=' => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n >= 0 {
                    self.exact(n as usize);
                }
            }
            '*' | '~' | '>' => {
                let n: i64 = rest.parse().unwrap_or(-1);
                for _ in 0..n.max(0) {
                    self.skip_frame();
                }
            }
            '%' => {
                let n: i64 = rest.parse().unwrap_or(-1);
                for _ in 0..n.max(0) * 2 {
                    self.skip_frame();
                }
            }
            other => panic!("unknown RESP type byte {other:?} in {line:?}"),
        }
        tag
    }

    /// Type byte of each of the next `n` replies, in order.
    fn tags(&mut self, n: usize) -> String {
        (0..n).map(|_| self.skip_frame()).collect()
    }
}

/// Run `body` against a server on 1 shard and on 4 shards.
///
/// Not decoration: `--shards 1` and `--shards 4` reach different dispatch
/// handlers, and this repo's recurring defect class is a behaviour that exists
/// on some dispatch paths and not others.
fn on_each_shard_count(body: impl Fn(u16)) {
    for shards in [1u32, 4] {
        let dir = tempfile::tempdir().expect("tempdir");
        let (child, port) = spawn_moon(dir.path(), shards);
        let _guard = ServerGuard(child);
        body(port);
    }
}

// ---------------------------------------------------------------------------
// bpv1-bpv4 — protocol in effect at production time
// ---------------------------------------------------------------------------

/// RED on main. The load-bearing test: a reply produced under RESP3 must stay
/// RESP3 even though a later `HELLO 2` in the same batch downgraded the
/// connection.
#[test]
fn bpv1_a_reply_produced_before_hello_2_keeps_its_resp3_encoding() {
    on_each_shard_count(|port| {
        let mut c = Conn::new(port, 3);
        c.write(&[
            &["CONFIG", "GET", "maxmemory"],
            &["HELLO", "2"],
            &["CONFIG", "GET", "maxmemory"],
        ]);
        assert_eq!(
            c.tags(3),
            "%**",
            "reply 1 was produced while RESP3 was in effect and must be a Map; \
             HELLO 2's own reply and everything after it are RESP2 arrays"
        );
    });
}

/// The mirror direction. Expected GREEN today — a pin, so the fix for `bpv1`
/// cannot be a blanket "use the batch-start version" that breaks this.
#[test]
fn bpv2_a_reply_produced_before_hello_3_keeps_its_resp2_encoding() {
    on_each_shard_count(|port| {
        let mut c = Conn::new(port, 2);
        c.write(&[
            &["CONFIG", "GET", "maxmemory"],
            &["HELLO", "3"],
            &["CONFIG", "GET", "maxmemory"],
        ]);
        assert_eq!(
            c.tags(3),
            "*%%",
            "reply 1 was produced under RESP2 and must stay an Array; HELLO 3's \
             own reply and everything after it are RESP3 maps"
        );
    });
}

/// RED on main. Two switches in one batch — proves the fix tracks a SEQUENCE of
/// switch points, not a single "did a HELLO happen" flag.
#[test]
fn bpv3_two_hellos_in_one_batch_each_take_effect_from_their_own_index() {
    on_each_shard_count(|port| {
        let mut c = Conn::new(port, 3);
        c.write(&[
            &["CONFIG", "GET", "maxmemory"], // %  produced under RESP3
            &["HELLO", "2"],                 // *  switch -> RESP2, own reply RESP2
            &["CONFIG", "GET", "maxmemory"], // *  RESP2
            &["HELLO", "3"],                 // %  switch -> RESP3, own reply RESP3
            &["CONFIG", "GET", "maxmemory"], // %  RESP3
        ]);
        assert_eq!(c.tags(5), "%**%%", "each HELLO applies from its own index");
    });
}

/// Expected GREEN today. Pins the hot path: a batch with no HELLO in it must be
/// encoded exactly as before, since that is every real pipeline.
#[test]
fn bpv4_a_batch_without_hello_is_encoded_entirely_in_one_protocol() {
    on_each_shard_count(|port| {
        let mut c = Conn::new(port, 3);
        c.write(&[
            &["CONFIG", "GET", "maxmemory"],
            &["SET", "bpv4", "v"],
            &["CONFIG", "GET", "maxmemory"],
        ]);
        assert_eq!(c.tags(3), "%+%", "no switch point, no change in encoding");

        let mut c2 = Conn::new(port, 2);
        c2.write(&[
            &["CONFIG", "GET", "maxmemory"],
            &["SET", "bpv4b", "v"],
            &["CONFIG", "GET", "maxmemory"],
        ]);
        assert_eq!(c2.tags(3), "*+*", "same, under RESP2");
    });
}

/// RED on main. `HELLO` is not the only command that moves the protocol —
/// `RESET` is contracted to return the connection to its default state, which
/// includes RESP2. §0 measured `HELLO 3` + `RESET` in one write producing `*14`
/// for the HELLO reply, exactly like the `HELLO 2` case.
///
/// Kept as its own test rather than folded into bpv1 because it fails through a
/// different code path: `shared::try_handle_reset`, not the two HELLO sites. A
/// fix that covers only HELLO leaves this red — which is what it did.
#[test]
fn bpv7_reset_is_a_protocol_switch_and_does_not_reach_backwards() {
    on_each_shard_count(|port| {
        let mut c = Conn::new(port, 3);
        c.write(&[
            &["CONFIG", "GET", "maxmemory"], // %  produced under RESP3
            &["RESET"],                      // +  switch -> RESP2 (reply is +RESET either way)
            &["CONFIG", "GET", "maxmemory"], // *  RESP2
        ]);
        assert_eq!(
            c.tags(3),
            "%+*",
            "RESET reverts to RESP2 from its own index onward; the reply produced \
             before it stays a RESP3 map"
        );
    });
}

// ---------------------------------------------------------------------------
// bpv5-bpv6 — CONFIG GET accepts more than one parameter
// ---------------------------------------------------------------------------
//
// Found while measuring the oracle for the batch tests. Moon reads only
// `args[0]` and silently drops the rest, so `CONFIG GET maxmemory appendonly`
// answers with maxmemory alone. Glob patterns work; multiple parameters do not.
// `redis-py`'s `config_get(*params)` and every monitoring agent that reads two
// settings in one call hit this.
//
// Measured on redis-server 8.6.1:
//   CONFIG GET maxmemory appendonly  -> both, in the server's own table order
//   CONFIG GET maxmemory 'maxmemory*'-> deduplicated; maxmemory appears ONCE
//   CONFIG GET nosuchparam maxmemory -> unknown patterns silently skipped
//   CONFIG GET nosuchparam           -> empty array

/// Read a CONFIG GET reply as the set of parameter names it returned.
fn config_get_names(port: u16, args: &[&str]) -> Vec<String> {
    let mut parts: Vec<&str> = vec!["CONFIG", "GET"];
    parts.extend_from_slice(args);
    let mut c = Conn::new(port, 2); // RESP2: a flat array, easiest to read
    c.write(&[&parts]);

    let header = c.line();
    assert_eq!(
        &header[..1],
        "*",
        "RESP2 CONFIG GET is an Array: {header:?}"
    );
    let n: usize = header[1..].parse().expect("array length");
    let mut names = Vec::new();
    for i in 0..n {
        let lead = c.line();
        let len: i64 = lead[1..].parse().unwrap_or(-1);
        let start = c.pos;
        if len >= 0 {
            c.exact(len as usize);
        }
        if i % 2 == 0 {
            names.push(
                String::from_utf8_lossy(&c.buf[start..start + len.max(0) as usize]).into_owned(),
            );
        }
    }
    names
}

/// RED on main — Moon returns only `maxmemory`.
#[test]
fn bpv5_config_get_honours_every_parameter_not_just_the_first() {
    on_each_shard_count(|port| {
        let mut names = config_get_names(port, &["maxmemory", "appendonly"]);
        names.sort();
        assert_eq!(
            names,
            vec!["appendonly".to_string(), "maxmemory".to_string()],
            "every supplied parameter must be answered, not just args[0]"
        );

        assert!(
            config_get_names(port, &["nosuchparam", "maxmemory"]) == vec!["maxmemory".to_string()],
            "an unknown parameter is skipped, not an error, and must not \
             suppress the known ones beside it"
        );
        assert!(
            config_get_names(port, &["nosuchparam"]).is_empty(),
            "all-unknown answers an empty array"
        );
    });
}

/// RED on main. Overlapping patterns must not double-report a parameter.
#[test]
fn bpv6_config_get_deduplicates_overlapping_patterns() {
    on_each_shard_count(|port| {
        let names = config_get_names(port, &["maxmemory", "maxmemory*"]);
        let mut seen = names.clone();
        seen.sort();
        seen.dedup();
        assert_eq!(
            seen.len(),
            names.len(),
            "a parameter matched by two patterns must appear once: {names:?}"
        );
        assert!(
            names.iter().any(|n| n == "maxmemory"),
            "the exact-match parameter is still present: {names:?}"
        );
    });
}
