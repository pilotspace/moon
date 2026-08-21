//! RESP3 verbatim-string parity for the four commands Redis answers with `=`.
//!
//! Transcribed from a live sweep against redis-server 8.6.1 (macOS host,
//! 2026-08-21), never from Moon's own behavior:
//!
//! ```text
//!                        RESP3   RESP2
//!   INFO [section...]      =       $
//!   CLIENT LIST [TYPE t]   =       $
//!   CLIENT INFO            =       $
//!   LOLWUT [VERSION n]     =       $
//!   MEMORY DOCTOR          =       $
//!   MEMORY MALLOC-STATS    $       $      <- deliberately NOT verbatim
//! ```
//!
//! RED before the fix: every case except `CLIENT INFO` answered `$` on RESP3.
//! `CLIENT INFO` is here as a PIN — it was already right, and the fix routes it
//! through a different path, so a regression there must fail loudly.
//!
//! Why these four sat wrong while `CLIENT INFO` was right is the whole of
//! moon#462: `CLIENT INFO` had a hand-written conversion call at its intercept,
//! and `INFO` / `CLIENT LIST` are answered by intercepts that had none. An
//! intercept short-circuits the dispatch exit where the RESP3 policy is
//! applied, so each one has to remember — and nothing made forgetting visible.
//! `LOLWUT` and `MEMORY DOCTOR` do reach dispatch; they were simply missing
//! from the policy table.
//!
//! Run alone with: cargo test --test resp3_verbatim_parity

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
                // The shared checkout hovers near the 5% diskfull guard; a
                // tripped guard would fail this suite for an unrelated reason.
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

/// A connection that reads only as far as the reply's TYPE BYTE and payload.
///
/// Deliberately not a general RESP parser: every reply this suite asks for is
/// a single scalar, and a narrow reader cannot silently "fix up" a shape the
/// way a forgiving one can.
struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(port: u16, proto: u8) -> Self {
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
            buf: Vec::with_capacity(64 * 1024),
            pos: 0,
        };
        // PING on THIS socket: the listener can accept before the shard behind
        // it serves, and under a parallel run that first reply comes back RST.
        let ping_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if c.raw(&["PING"]).0 == '+' {
                break;
            }
            assert!(
                Instant::now() < ping_deadline,
                "server on {port} accepted TCP but never answered PING"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        if proto == 3 {
            let (tag, _) = c.raw(&["HELLO", "3"]);
            assert_ne!(
                tag, '-',
                "HELLO 3 rejected — the server must speak RESP3 for this suite to mean anything"
            );
        }
        c
    }

    fn byte(&mut self) -> u8 {
        while self.pos >= self.buf.len() {
            let mut chunk = [0u8; 16 * 1024];
            let n = self.s.read(&mut chunk).expect("read reply");
            assert!(n > 0, "peer closed mid-reply");
            self.buf.extend_from_slice(&chunk[..n]);
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        b
    }

    fn line(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let b = self.byte();
            if b == b'\r' {
                let lf = self.byte();
                assert_eq!(lf, b'\n', "CR not followed by LF");
                return out;
            }
            out.push(b);
        }
    }

    /// Send `parts`, return `(type byte, payload text)`.
    ///
    /// Arrays and maps are read far enough to keep the stream in sync; their
    /// payload comes back empty because no case here asserts on one.
    fn raw(&mut self, parts: &[&str]) -> (char, String) {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn frame(&mut self) -> (char, String) {
        let tag = self.byte() as char;
        match tag {
            '+' | '-' | ':' | ',' | '#' | '(' => {
                let l = self.line();
                (tag, String::from_utf8_lossy(&l).into_owned())
            }
            '_' => {
                self.line();
                (tag, String::new())
            }
            '$' | '=' => {
                let n: i64 = String::from_utf8_lossy(&self.line()).parse().expect("len");
                if n < 0 {
                    return (tag, String::new());
                }
                let mut body = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    body.push(self.byte());
                }
                assert_eq!(self.byte(), b'\r');
                assert_eq!(self.byte(), b'\n');
                (tag, String::from_utf8_lossy(&body).into_owned())
            }
            '*' | '~' | '>' => {
                let n: i64 = String::from_utf8_lossy(&self.line()).parse().expect("len");
                for _ in 0..n.max(0) {
                    self.frame();
                }
                (tag, String::new())
            }
            '%' => {
                let n: i64 = String::from_utf8_lossy(&self.line()).parse().expect("len");
                for _ in 0..n.max(0) * 2 {
                    self.frame();
                }
                (tag, String::new())
            }
            other => panic!("unknown RESP type byte {other:?}"),
        }
    }
}

/// The oracle. `&[&str]` command, as sent.
const VERBATIM_ON_RESP3: &[&[&str]] = &[
    &["INFO"],
    &["INFO", "server"],
    &["CLIENT", "LIST"],
    &["CLIENT", "INFO"],
    &["LOLWUT"],
    &["MEMORY", "DOCTOR"],
];

fn check_shards(shards: u32, body: impl Fn(u16)) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), shards);
    let _guard = ServerGuard(child);
    body(port);
}

#[test]
fn rv1_resp3_answers_verbatim_for_every_command_redis_does() {
    for shards in [1u32, 4] {
        check_shards(shards, |port| {
            let mut c = Conn::new(port, 3);
            for cmd in VERBATIM_ON_RESP3 {
                let (tag, payload) = c.raw(cmd);
                assert_eq!(
                    tag, '=',
                    "shards={shards} {cmd:?}: redis-8.6.1 answers a VerbatimString on RESP3, \
                     Moon answered {tag:?}"
                );
                assert!(
                    payload.starts_with("txt:") || payload.is_empty(),
                    "shards={shards} {cmd:?}: verbatim payload must carry the `txt:` encoding \
                     hint Redis sends, got {:?}",
                    &payload[..payload.len().min(16)]
                );
            }
        });
    }
}

#[test]
fn rv2_resp2_keeps_every_one_of_them_a_bulk_string() {
    // The other half of the contract: a RESP2 client must see no change at
    // all. A conversion that fires on RESP2 would break every existing driver.
    for shards in [1u32, 4] {
        check_shards(shards, |port| {
            let mut c = Conn::new(port, 2);
            for cmd in VERBATIM_ON_RESP3 {
                let (tag, _) = c.raw(cmd);
                assert_eq!(
                    tag, '$',
                    "shards={shards} {cmd:?}: RESP2 must stay a BulkString, got {tag:?}"
                );
            }
        });
    }
}

#[test]
fn rv3_argument_forms_do_not_change_the_type() {
    // Redis answers verbatim for every form of these commands, not just the
    // bare one — the section list, the TYPE filter and the VERSION selector
    // are all still `=`. A classifier keyed on argument count would pass the
    // bare cases above and fail here.
    check_shards(1, |port| {
        let mut c = Conn::new(port, 3);
        for cmd in [
            &["INFO", "server", "clients"][..],
            &["INFO", "all"][..],
            &["CLIENT", "LIST", "TYPE", "normal"][..],
            &["LOLWUT", "VERSION", "5"][..],
        ] {
            let (tag, _) = c.raw(cmd);
            assert_eq!(
                tag, '=',
                "{cmd:?}: every argument form is verbatim on RESP3, got {tag:?}"
            );
        }
    });
}

#[test]
fn rv4_neighbouring_replies_keep_their_own_types() {
    // The blast radius. `MEMORY DOCTOR` becoming verbatim must not drag its
    // siblings with it: redis-8.6.1 answers MEMORY STATS as a Map and MEMORY
    // USAGE as an Integer (Null for a missing key) on RESP3, and CLIENT ID as
    // an Integer. Over-conversion is the failure mode a name-keyed table
    // invites, so it is asserted rather than assumed.
    check_shards(1, |port| {
        let mut c = Conn::new(port, 3);
        assert_eq!(c.raw(&["MEMORY", "STATS"]).0, '%', "MEMORY STATS is a Map");
        assert_eq!(
            c.raw(&["MEMORY", "USAGE", "nosuchkey"]).0,
            '_',
            "MEMORY USAGE on a missing key is Null"
        );
        c.raw(&["SET", "rv4key", "v"]);
        assert_eq!(
            c.raw(&["MEMORY", "USAGE", "rv4key"]).0,
            ':',
            "MEMORY USAGE is an Integer"
        );
        assert_eq!(c.raw(&["CLIENT", "ID"]).0, ':', "CLIENT ID is an Integer");
        assert_eq!(
            c.raw(&["CLIENT", "GETNAME"]).0,
            '_',
            "CLIENT GETNAME with no name set is Null"
        );
    });
}
