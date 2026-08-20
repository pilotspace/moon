//! Inline command framing must match Redis: terminate on `\n`, strip at most
//! one preceding `\r`, and never stall behind a blank line.
//!
//! Covers #381 (bare-LF lines never terminated, so the command was never
//! dispatched and the client simply hung) and #578 (a blank line made the read
//! loop park on a buffer that already held a complete command).
//!
//! Everything here is asserted through a raw socket in ONE `write_all`. Both
//! details matter:
//!   * a client library normalises the framing away, and framing IS the
//!     subject; and
//!   * splitting the payload across two writes MASKS #578 outright — the
//!     second `read()` is precisely what rescues the stalled buffer today, so
//!     a two-write test would pass against the unfixed server.
//!
//! Expectations were measured against redis-server 8.0.5 on the moon-dev VM,
//! one fresh connection per case.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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
    // CARGO_BIN_EXE_moon is the binary cargo built for THIS invocation — fresh
    // and feature-matched. Never probe target/release directly: that path's
    // provenance is unknown and has produced false PASSes before.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-inlineterm-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-inlineterm-{port}"));
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

/// Send `payload` in exactly one write, then read until the server goes quiet.
///
/// `expected_replies` is how many `\r\n`-terminated top-level replies the
/// caller wants; reading stops early once they arrive so a passing case does
/// not pay the full timeout. A stall — the bug under test — still costs the
/// timeout and comes back short, which is what the assertions catch.
fn send_once(port: u16, payload: &[u8], expected_replies: usize) -> Vec<u8> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    s.write_all(payload).expect("write payload");

    let mut reply = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        // Count complete replies cheaply: every RESP reply this suite provokes
        // ends in CRLF, so CRLF occurrences are an upper bound on how many
        // whole replies have landed. Multi-line replies would over-count, so
        // no case here uses one.
        let crlfs = reply.windows(2).filter(|w| w == b"\r\n").count();
        if crlfs >= expected_replies {
            break;
        }
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => reply.extend_from_slice(&buf[..n]),
            // A timeout means nothing more is coming — for a stalled server
            // that IS the observation, so it must not be an error.
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(_) => break,
        }
    }
    reply
}

fn assert_reply(port: u16, payload: &[u8], expect: &[u8], what: &str) {
    let expected_replies = expect.windows(2).filter(|w| w == b"\r\n").count();
    let got = send_once(port, payload, expected_replies);
    assert_eq!(
        String::from_utf8_lossy(&got),
        String::from_utf8_lossy(expect),
        "{what}\n  sent: {:?}",
        String::from_utf8_lossy(payload)
    );
}

// === #381: bare LF terminates a line ===

#[test]
fn inline_bare_lf_command_is_served() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"PING\n",
            b"+PONG\r\n",
            &format!("bare-LF PING must be answered (shards={shards})"),
        );
    }
}

#[test]
fn inline_bare_lf_batch_is_served() {
    // The exact shape #381 reported: a shell/awk-generated command stream with
    // LF endings. Before the fix this wrote nothing at all and the DB stayed
    // empty while the caller saw no error.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"SET a 1\nSET b 2\nGET a\nGET b\n",
            b"+OK\r\n+OK\r\n$1\r\n1\r\n$1\r\n2\r\n",
            &format!("LF-separated batch must all apply (shards={shards})"),
        );
    }
}

#[test]
fn inline_lf_must_not_merge_two_commands() {
    // The correctness half of #381: scanning past the interior `\n` to reach
    // the trailing `\r\n` made ONE command out of two, appending the second
    // command's arguments to the first — a silent write loss.
    //
    // If the merge came back, the first reply would be an error and `GET k`
    // would not return v2.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"SET k v1\nSET k v2\r\nGET k\r\n",
            b"+OK\r\n+OK\r\n$2\r\nv2\r\n",
            &format!("interior LF must end the first command (shards={shards})"),
        );
    }
}

#[test]
fn inline_only_one_cr_is_stripped() {
    // redis answers +PONG for `PING\r\r\n`: one `\r` belongs to the terminator
    // and the other is a token separator. Moon used to answer
    // `unknown command 'PING\r'`.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"PING\r\r\n",
            b"+PONG\r\n",
            &format!("only the CR adjacent to LF is terminator (shards={shards})"),
        );
    }
}

// === #578: a blank line must not stall the buffered command ===

#[test]
fn inline_blank_crlf_lines_do_not_stall_the_next_command() {
    // Pure CRLF — no bare LF anywhere — so this is #578 and not #381.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"\r\n\r\nPING\r\n",
            b"+PONG\r\n",
            &format!("blank CRLF lines must not park the read loop (shards={shards})"),
        );
    }
}

#[test]
fn inline_blank_lf_lines_do_not_stall_the_next_command() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"\n\nPING\n",
            b"+PONG\r\n",
            &format!("blank LF lines must not park the read loop (shards={shards})"),
        );
    }
}

#[test]
fn inline_blank_line_then_resp_frame_is_served() {
    // What follows a blank line is very often a real RESP array. The
    // re-dispatch must go back through the RESP/inline decision rather than
    // retrying the inline splitter, which would parse `*1` as a literal token.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"\r\n*1\r\n$4\r\nPING\r\n",
            b"+PONG\r\n",
            &format!("a RESP frame behind a blank line must parse (shards={shards})"),
        );
    }
}

#[test]
fn inline_whitespace_only_line_does_not_stall() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"   \nPING\n",
            b"+PONG\r\n",
            &format!("whitespace-only line must not park the loop (shards={shards})"),
        );
    }
}

// === guards: the common path must be untouched ===

#[test]
fn inline_crlf_still_works() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"SET g 1\r\nGET g\r\n",
            b"+OK\r\n$1\r\n1\r\n",
            &format!("CRLF inline must be unaffected (shards={shards})"),
        );
    }
}

#[test]
fn inline_quoted_argument_survives_lf_termination() {
    // The quoted splitter and the fast path must agree on where a line ends.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_reply(
            moon.port,
            b"ECHO \"a b\"\n",
            b"$3\r\na b\r\n",
            &format!("quoted arg with LF terminator (shards={shards})"),
        );
    }
}
