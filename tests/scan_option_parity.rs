//! SCAN-family option parsing, against redis-server 8.6.1.
//!
//! Two defects, one suite (moon#630):
//!
//! 1. `HSCAN ... NOVALUES` was accepted and IGNORED. A client that passes it
//!    parses the reply as a flat list of field NAMES — that is the entire point
//!    of the option, and it is what `redis-py`'s `hscan(no_values=True)` does.
//!    Against Moon those clients read the VALUES as field names. Nothing
//!    errors; the caller silently gets garbage.
//! 2. Every SCAN-family option parser accepted unknown tokens silently. That is
//!    what kept (1) invisible: `NOVALUES` was not "unimplemented and rejected",
//!    it was accepted and dropped, and any option added later inherits the same
//!    silence.
//!
//! The expected strings below are transcribed from a live sweep on 2026-08-22,
//! not from the Redis docs and not from Moon:
//!
//! ```text
//!   HSCAN h 0 NOVALUES        -> fields only, no values
//!   HSCAN h 0 MATCH           -> ERR syntax error
//!   HSCAN h 0 COUNT           -> ERR syntax error
//!   HSCAN h 0 COUNT abc       -> ERR value is not an integer or out of range
//!   HSCAN h 0 COUNT 0         -> ERR syntax error
//!   HSCAN h 0 COUNT -1        -> ERR syntax error
//!   HSCAN h 0 TYPE hash       -> ERR syntax error
//!   HSCAN h 0 NOVALUES EXTRA  -> ERR syntax error
//!   SSCAN s 0 NOVALUES        -> ERR NOVALUES option can only be used in HSCAN
//!   ZSCAN z 0 NOVALUES        -> ERR NOVALUES option can only be used in HSCAN
//!   SCAN 0 NOVALUES           -> ERR NOVALUES option can only be used in HSCAN
//!   SCAN 0 TYPE               -> ERR syntax error
//!   SCAN 0 TYPE nosuchtype    -> empty result, NOT an error
//!   SSCAN s 0 TYPE set        -> ERR syntax error
//! ```
//!
//! Run alone with: cargo test --test scan_option_parity

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

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

/// Minimal RESP reader. Returns `(type byte, flattened payload strings)`.
struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(30);
        let s = loop {
            if let Ok(s) = TcpStream::connect(format!("127.0.0.1:{port}")) {
                s.set_read_timeout(Some(Duration::from_secs(10))).ok();
                s.set_write_timeout(Some(Duration::from_secs(10))).ok();
                break s;
            }
            assert!(Instant::now() < deadline, "server on {port} never accepted");
            std::thread::sleep(Duration::from_millis(50));
        };
        let mut c = Conn {
            s,
            buf: Vec::with_capacity(16 * 1024),
            pos: 0,
        };
        let ping_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if c.cmd(&["PING"]).0 == '+' {
                break;
            }
            assert!(
                Instant::now() < ping_deadline,
                "server on {port} accepted TCP but never answered PING"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        c
    }

    fn byte(&mut self) -> u8 {
        while self.pos >= self.buf.len() {
            let mut chunk = [0u8; 8192];
            let n = self.s.read(&mut chunk).expect("read reply");
            assert!(n > 0, "peer closed mid-reply");
            self.buf.extend_from_slice(&chunk[..n]);
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        b
    }

    fn line(&mut self) -> String {
        let mut out = Vec::new();
        loop {
            let b = self.byte();
            if b == b'\r' {
                assert_eq!(self.byte(), b'\n', "CR not followed by LF");
                return String::from_utf8_lossy(&out).into_owned();
            }
            out.push(b);
        }
    }

    fn cmd(&mut self, parts: &[&str]) -> (char, Vec<String>) {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
        }
        self.s.write_all(&req).expect("write cmd");
        let mut flat = Vec::new();
        let tag = self.frame(&mut flat);
        (tag, flat)
    }

    fn frame(&mut self, flat: &mut Vec<String>) -> char {
        let tag = self.byte() as char;
        match tag {
            '+' | '-' | ':' | ',' => {
                flat.push(self.line());
            }
            '_' => {
                self.line();
            }
            '$' | '=' => {
                let n: i64 = self.line().parse().expect("len");
                if n >= 0 {
                    let mut body = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        body.push(self.byte());
                    }
                    self.byte();
                    self.byte();
                    flat.push(String::from_utf8_lossy(&body).into_owned());
                }
            }
            '*' | '~' | '>' => {
                let n: i64 = self.line().parse().expect("len");
                for _ in 0..n.max(0) {
                    self.frame(flat);
                }
            }
            '%' => {
                let n: i64 = self.line().parse().expect("len");
                for _ in 0..n.max(0) * 2 {
                    self.frame(flat);
                }
            }
            other => panic!("unknown RESP type byte {other:?}"),
        }
        tag
    }
}

fn with_moon(shards: u32, body: impl Fn(&mut Conn)) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), shards);
    let _guard = ServerGuard(child);
    let mut c = Conn::new(port);
    c.cmd(&["HSET", "so:h", "f1", "v1", "f2", "v2"]);
    c.cmd(&["SADD", "so:s", "a", "b"]);
    c.cmd(&["ZADD", "so:z", "1", "a"]);
    c.cmd(&["SET", "so:k", "v"]);
    body(&mut c);
}

#[test]
fn so1_hscan_novalues_returns_field_names_only() {
    for shards in [1u32, 4] {
        with_moon(shards, |c| {
            let (tag, flat) = c.cmd(&["HSCAN", "so:h", "0", "NOVALUES"]);
            assert_eq!(tag, '*', "shards={shards}: HSCAN answers an array");
            // flat = [cursor, ...elements]
            let elems = &flat[1..];
            assert_eq!(
                elems.len(),
                2,
                "shards={shards}: NOVALUES must yield one element per FIELD — got {elems:?}, \
                 which is the field/value interleave a client would read as four field names"
            );
            let mut got: Vec<&str> = elems.iter().map(String::as_str).collect();
            got.sort_unstable();
            assert_eq!(got, vec!["f1", "f2"], "shards={shards}");
        });
    }
}

#[test]
fn so2_novalues_is_case_insensitive_like_every_other_option() {
    with_moon(1, |c| {
        let (_, flat) = c.cmd(&["HSCAN", "so:h", "0", "novalues"]);
        assert_eq!(
            flat.len() - 1,
            2,
            "Redis matches option tokens case-insensitively; got {flat:?}"
        );
    });
}

#[test]
fn so3_hscan_novalues_composes_with_match_and_count() {
    with_moon(1, |c| {
        let (_, flat) = c.cmd(&["HSCAN", "so:h", "0", "MATCH", "f1*", "NOVALUES"]);
        assert_eq!(&flat[1..], &["f1"], "MATCH filters, NOVALUES strips");
        let (_, flat) = c.cmd(&["HSCAN", "so:h", "0", "COUNT", "10", "NOVALUES"]);
        let mut got: Vec<&str> = flat[1..].iter().map(String::as_str).collect();
        got.sort_unstable();
        assert_eq!(got, vec!["f1", "f2"], "COUNT and NOVALUES compose");
    });
}

/// `(command, expected error text)` — every one transcribed from redis 8.6.1.
const REJECTED: &[(&[&str], &str)] = &[
    (&["HSCAN", "so:h", "0", "MATCH"], "ERR syntax error"),
    (&["HSCAN", "so:h", "0", "COUNT"], "ERR syntax error"),
    (
        &["HSCAN", "so:h", "0", "COUNT", "abc"],
        "ERR value is not an integer or out of range",
    ),
    (&["HSCAN", "so:h", "0", "COUNT", "0"], "ERR syntax error"),
    (&["HSCAN", "so:h", "0", "COUNT", "-1"], "ERR syntax error"),
    (&["HSCAN", "so:h", "0", "TYPE", "hash"], "ERR syntax error"),
    (
        &["HSCAN", "so:h", "0", "NOVALUES", "EXTRA"],
        "ERR syntax error",
    ),
    (&["HSCAN", "so:h", "0", "BOGUSTOKEN"], "ERR syntax error"),
    (
        &["SSCAN", "so:s", "0", "NOVALUES"],
        "ERR NOVALUES option can only be used in HSCAN",
    ),
    (&["SSCAN", "so:s", "0", "TYPE", "set"], "ERR syntax error"),
    (&["SSCAN", "so:s", "0", "COUNT", "0"], "ERR syntax error"),
    (&["SSCAN", "so:s", "0", "BOGUSTOKEN"], "ERR syntax error"),
    (
        &["ZSCAN", "so:z", "0", "NOVALUES"],
        "ERR NOVALUES option can only be used in HSCAN",
    ),
    (
        &["ZSCAN", "so:z", "0", "COUNT", "abc"],
        "ERR value is not an integer or out of range",
    ),
    (&["ZSCAN", "so:z", "0", "BOGUSTOKEN"], "ERR syntax error"),
    (
        &["SCAN", "0", "NOVALUES"],
        "ERR NOVALUES option can only be used in HSCAN",
    ),
    (&["SCAN", "0", "TYPE"], "ERR syntax error"),
    (&["SCAN", "0", "COUNT", "0"], "ERR syntax error"),
    (
        &["SCAN", "0", "MATCH", "*", "COUNT", "abc"],
        "ERR value is not an integer or out of range",
    ),
    (&["SCAN", "0", "BOGUSTOKEN"], "ERR syntax error"),
];

#[test]
fn so4_unknown_and_malformed_options_are_refused() {
    for shards in [1u32, 4] {
        with_moon(shards, |c| {
            for (cmd, want) in REJECTED {
                let (tag, flat) = c.cmd(cmd);
                assert_eq!(
                    tag, '-',
                    "shards={shards} {cmd:?}: redis refuses this; Moon answered a reply \
                     ({flat:?}), which tells the client it got behaviour it did not"
                );
                assert_eq!(&flat[0], want, "shards={shards} {cmd:?}: wrong error text");
            }
        });
    }
}

#[test]
fn so5_valid_forms_still_work() {
    // The blast radius of a strict parser: everything Redis ACCEPTS must keep
    // working, including the one that looks like a typo and is not.
    for shards in [1u32, 4] {
        with_moon(shards, |c| {
            for cmd in [
                &["HSCAN", "so:h", "0"][..],
                &["HSCAN", "so:h", "0", "MATCH", "*"][..],
                &["HSCAN", "so:h", "0", "COUNT", "100"][..],
                &[
                    "HSCAN", "so:h", "0", "MATCH", "*", "COUNT", "100", "NOVALUES",
                ][..],
                &["SSCAN", "so:s", "0", "MATCH", "*", "COUNT", "5"][..],
                &["ZSCAN", "so:z", "0", "COUNT", "5"][..],
                &["SCAN", "0", "MATCH", "so:*", "COUNT", "100"][..],
                &["SCAN", "0", "TYPE", "hash"][..],
                // Not a typo: an unknown TYPE name is a legal filter that
                // matches nothing. Rejecting it would be over-strictness.
                &["SCAN", "0", "TYPE", "nosuchtype"][..],
            ] {
                let (tag, flat) = c.cmd(cmd);
                assert_eq!(
                    tag, '*',
                    "shards={shards} {cmd:?} must be accepted: {flat:?}"
                );
            }
        });
    }
}
