//! `MONITOR` streams every executed command to attached admin clients.
//!
//! Measured against redis-server 8.6.1 over raw sockets (three probes,
//! 2026-08-14). Raw sockets because the feed IS a stream of `+SimpleString`
//! lines whose exact bytes are the contract — every client library reformats
//! them into a struct before a test could see the difference, and the
//! divergences that matter here are byte-level: which byte leads the frame,
//! how a `0x00` is escaped, whether a secret survives redaction.
//!
//! Two measured facts invert the obvious implementation, and each has a test
//! that fails loudly if someone "fixes" it back:
//!
//!   * The feed is a **SimpleString even under RESP3**, not a Push frame. The
//!     instinct straight after `pubsub-resp3-push` is to make it a Push; Redis
//!     does not, and a client reading the feed expects `+`. See `mon2`.
//!   * Administrative commands are hidden at **subcommand** granularity, and
//!     the rule is NOT Moon's `CommandFlags::ADMIN | SKIP_MONITOR`. Moon's
//!     ADMIN is container-granular (it would hide `INFO`, `CLIENT GETNAME`,
//!     `ACL WHOAMI`, which Redis shows) and Redis feeds the whole EVAL family
//!     despite flagging it `skip_monitor`. See `mon20`, which drives the
//!     measured table row by row.

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-monitor-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-monitor-{port}"));
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

struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_millis(600)))
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
            s(&r)
        );
        c
    }

    fn write_cmd(&mut self, parts: &[&[u8]]) {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        self.0.write_all(&out).expect("write command");
    }

    fn send(&mut self, parts: &[&str]) -> Vec<u8> {
        let owned: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.write_cmd(&owned);
        self.drain()
    }

    /// Send a command whose arguments may contain arbitrary bytes.
    fn send_bytes(&mut self, parts: &[&[u8]]) -> Vec<u8> {
        self.write_cmd(parts);
        self.drain()
    }

    /// Read until the socket goes quiet for one timeout window.
    fn drain(&mut self) -> Vec<u8> {
        let mut got = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
        got
    }

    /// Drain with a longer settle, for a feed that may lag the command.
    fn feed(&mut self) -> Vec<u8> {
        std::thread::sleep(Duration::from_millis(120));
        self.drain()
    }
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_string()
}

/// Attach a MONITOR connection and assert it was accepted.
fn attach(port: u16) -> Conn {
    let mut m = Conn::open(port);
    let r = m.send(&["MONITOR"]);
    assert_eq!(
        s(&r),
        "+OK\r\n",
        "MONITOR must be accepted for an admin connection; got {:?}",
        s(&r)
    );
    m
}

/// Every feed line, split on CRLF, with the leading '+' retained.
fn lines(buf: &[u8]) -> Vec<String> {
    s(buf)
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
        .collect()
}

/// Does any feed line name this command as its first quoted token?
fn names(buf: &[u8], cmd: &str) -> bool {
    lines(buf).iter().any(|l| l.contains(&format!("\"{cmd}\"")))
}

// ── M1 M2 M4 ────────────────────────────────────────────────────────────────

#[test]
fn mon1_monitor_replies_ok_and_attaches() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);

    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);

    let got = mon.feed();
    let ls = lines(&got);
    assert_eq!(
        ls.len(),
        1,
        "exactly one feed line for one command; got {ls:?}"
    );
    let l = &ls[0];
    assert!(
        l.starts_with('+'),
        "a feed line is a SimpleString; got {l:?}"
    );
    // +<unix>.<micros:06> [<db> <ip>:<port>] "SET" "k" "v"
    let body = &l[1..];
    let (ts, rest) = body.split_once(' ').expect("timestamp then space");
    let (secs, micros) = ts.split_once('.').expect("secs.micros");
    assert!(
        secs.len() >= 10 && secs.chars().all(|c| c.is_ascii_digit()),
        "unix seconds; got {secs:?}"
    );
    assert_eq!(
        micros.len(),
        6,
        "micros zero-padded to exactly 6 digits; got {micros:?} in {l:?}"
    );
    assert!(
        micros.chars().all(|c| c.is_ascii_digit()),
        "micros all digits; got {micros:?}"
    );
    assert!(
        rest.starts_with("[0 127.0.0.1:"),
        "db 0 and the peer address in brackets; got {rest:?}"
    );
    assert!(
        rest.ends_with(r#""SET" "k" "v""#),
        "every token quoted, command name included; got {rest:?}"
    );
}

#[test]
fn mon2_feed_is_simplestring_under_resp3() {
    // The trap this test exists for: pubsub-resp3-push just made confirmations
    // Push frames, so the reflex is to do the same here. Redis does NOT — the
    // feed stays a SimpleString in RESP3, and a client reading it expects '+'.
    let m = spawn_moon("1");
    let mut mon = Conn::hello3(m.port);
    let r = mon.send(&["MONITOR"]);
    assert_eq!(s(&r), "+OK\r\n", "MONITOR answers +OK in RESP3 too");

    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);

    let got = mon.feed();
    assert!(!got.is_empty(), "the RESP3 monitor must receive the feed");
    assert_eq!(
        got[0],
        b'+',
        "the feed line is a SimpleString under RESP3, not a Push ('>'); got {:?}",
        s(&got)
    );
}

#[test]
fn mon3_reads_are_fed() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);
    let mut mon = attach(m.port);
    c.send(&["GET", "k"]);
    let got = mon.feed();
    assert!(
        names(&got, "GET"),
        "reads are fed, not only writes; got {:?}",
        s(&got)
    );
}

#[test]
fn mon4_db_follows_select() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["SELECT", "3"]);
    c.send(&["GET", "k"]);
    let got = mon.feed();
    let ls = lines(&got);
    assert!(
        ls.iter().all(|l| l.contains("[3 ")),
        "both lines report the db AFTER SELECT; got {ls:?}"
    );
}

// ── M6: administrative commands ─────────────────────────────────────────────

#[test]
fn mon5_admin_commands_are_not_fed() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["CONFIG", "SET", "maxmemory", "0"]);
    c.send(&["DBSIZE"]);

    let got = mon.feed();
    assert!(
        !names(&got, "CONFIG"),
        "CONFIG is administrative and must never reach a monitor; got {:?}",
        s(&got)
    );
    // The half that proves the feed was live rather than simply broken.
    assert!(
        names(&got, "DBSIZE"),
        "DBSIZE is not administrative and must be fed — without this the test \
         would pass against a feed that emits nothing at all; got {:?}",
        s(&got)
    );
}

#[test]
fn mon6_monitor_is_absent_from_its_own_feed() {
    let m = spawn_moon("1");
    let mut mon_a = attach(m.port);
    let mut mon_b = attach(m.port);

    let a = mon_a.feed();
    assert!(
        !names(&a, "MONITOR"),
        "MONITOR is administrative, so attaching a second monitor emits nothing; got {:?}",
        s(&a)
    );

    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);
    assert!(
        names(&mon_a.feed(), "SET"),
        "and both monitors are still live"
    );
    assert!(names(&mon_b.feed(), "SET"), "including the second one");
}

#[test]
fn mon7_rejected_commands_are_not_fed() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["NOSUCHCMD", "x"]);
    c.send(&["GET"]); // arity violation
    c.send(&["PING"]);

    let got = mon.feed();
    assert!(
        !names(&got, "NOSUCHCMD"),
        "an unknown command never executes, so it is never fed; got {:?}",
        s(&got)
    );
    assert!(
        !names(&got, "GET"),
        "an arity-rejected command never executes either; got {:?}",
        s(&got)
    );
    assert!(
        names(&got, "PING"),
        "the following valid command IS fed, proving the feed is live; got {:?}",
        s(&got)
    );
}

// ── M8: redaction ───────────────────────────────────────────────────────────

#[test]
fn mon8_auth_arguments_are_redacted() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["AUTH", "hunter2"]);
    c.send(&["AUTH", "someuser", "s3cret-pw"]);

    let got = mon.feed();
    let text = s(&got);
    // The assertion that matters is the ABSENCE of the secret, not the
    // presence of the placeholder: a formatter that appended "(redacted)"
    // after emitting the password would satisfy the weaker check.
    assert!(
        !text.contains("hunter2"),
        "the single-argument AUTH password must not appear anywhere in the feed; got {text:?}"
    );
    assert!(
        !text.contains("s3cret-pw"),
        "the two-argument AUTH password must not appear anywhere in the feed; got {text:?}"
    );
    assert!(
        text.contains(r#""AUTH" "(redacted)""#),
        "AUTH pw renders as \"AUTH\" \"(redacted)\"; got {text:?}"
    );
    assert!(
        text.contains(r#""AUTH" "(redacted)" "(redacted)""#),
        "AUTH user pw redacts BOTH arguments — the username is a credential too; got {text:?}"
    );
}

#[test]
fn mon9_hello_auth_redacts_only_credentials() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["HELLO", "3", "AUTH", "default", "sekrit"]);

    let text = s(&mon.feed());
    assert!(
        !text.contains("sekrit"),
        "the HELLO AUTH password must not appear in the feed; got {text:?}"
    );
    assert!(
        text.contains(r#""HELLO" "3" "AUTH" "(redacted)" "(redacted)""#),
        "the protocol version survives; only the two arguments after AUTH are \
         redacted; got {text:?}"
    );
}

// ── M9: transactions ────────────────────────────────────────────────────────

#[test]
fn mon10_transaction_timing() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);

    c.send(&["MULTI"]);
    let at_multi = mon.feed();
    assert!(
        names(&at_multi, "MULTI"),
        "MULTI is fed when it is issued; got {:?}",
        s(&at_multi)
    );

    let q = c.send(&["SET", "q", "1"]);
    assert_eq!(s(&q), "+QUEUED\r\n");
    let at_queue = mon.feed();
    assert!(
        !names(&at_queue, "SET"),
        "a QUEUED command has not executed, so it must not be fed yet — this \
         is the half a naive implementation gets wrong; got {:?}",
        s(&at_queue)
    );

    c.send(&["EXEC"]);
    let at_exec = mon.feed();
    let ls = lines(&at_exec);
    let set_at = ls.iter().position(|l| l.contains("\"SET\""));
    let exec_at = ls.iter().position(|l| l.contains("\"EXEC\""));
    assert!(
        set_at.is_some() && exec_at.is_some(),
        "at EXEC both the queued command and EXEC are fed; got {ls:?}"
    );
    assert!(
        set_at < exec_at,
        "the queued command is fed BEFORE the EXEC line; got {ls:?}"
    );
}

#[test]
fn mon11_two_monitors_both_receive() {
    let m = spawn_moon("1");
    let mut a = attach(m.port);
    let mut b = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["SET", "dual", "1"]);

    let la = lines(&a.feed());
    let lb = lines(&b.feed());
    assert!(
        la.iter().any(|l| l.contains(r#""SET" "dual" "1""#)),
        "monitor A receives; got {la:?}"
    );
    assert!(
        lb.iter().any(|l| l.contains(r#""SET" "dual" "1""#)),
        "monitor B receives the same line; got {lb:?}"
    );
}

// ── M2: escaping ────────────────────────────────────────────────────────────

#[test]
fn mon12_argument_escaping_is_byte_exact() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);

    c.send_bytes(&[b"SET", b"esc", b"a\"b\\c\nd\re\tf\x00g\xffh"]);
    c.send_bytes(&[b"SET", b"utf", "h\u{e9}llo".as_bytes()]);
    c.send_bytes(&[b"SET", b"empty", b""]);

    let text = s(&mon.feed());
    assert!(
        text.contains(r#""a\"b\\c\nd\re\tf\x00g\xffh""#),
        "quote, backslash, newline, CR, tab, NUL and a high byte each escape \
         per Redis sdscatrepr semantics; got {text:?}"
    );
    assert!(
        text.contains(r#""h\xc3\xa9llo""#),
        "UTF-8 escapes PER BYTE, not per character; got {text:?}"
    );
    assert!(
        text.contains(r#""SET" "empty" """#),
        "an empty argument renders as a pair of quotes; got {text:?}"
    );
}

// ── M13: every dispatch path ────────────────────────────────────────────────

#[test]
fn mon13_inline_fast_path_is_fed() {
    // A plain GET/SET takes try_inline_dispatch under monoio, which is a
    // different code path from everything else here. A feed hook missing there
    // is invisible to any test that uses another command — and GET/SET are
    // what most tests use. This is the shape of the v0.8.6 inline-GET P0.
    let m = spawn_moon("4");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["SET", "inline", "1"]);
    c.send(&["GET", "inline"]);

    let got = mon.feed();
    assert!(
        names(&got, "SET"),
        "the inline SET must be fed at --shards 4; got {:?}",
        s(&got)
    );
    assert!(
        names(&got, "GET"),
        "and the inline GET, which is the exact path the v0.8.6 P0 slipped \
         through; got {:?}",
        s(&got)
    );
}

// ── Rejections ──────────────────────────────────────────────────────────────

#[test]
fn mon14_non_admin_cannot_attach() {
    let m = spawn_moon("1");
    let mut admin = Conn::open(m.port);
    let r = admin.send(&[
        "ACL", "SETUSER", "lowly", "on", ">pw", "~*", "+@all", "-@admin",
    ]);
    assert_eq!(s(&r), "+OK\r\n", "test fixture: create a non-admin user");

    let mut lo = Conn::open(m.port);
    assert_eq!(s(&lo.send(&["AUTH", "lowly", "pw"])), "+OK\r\n");
    let denied = lo.send(&["MONITOR"]);
    assert!(
        denied.starts_with(b"-NOPERM"),
        "MONITOR requires the admin category; got {:?}",
        s(&denied)
    );

    // The security-relevant half: refusing the reply is worthless if the
    // connection was attached anyway.
    let mut c = Conn::open(m.port);
    c.send(&["SET", "secret", "value"]);
    let leaked = lo.feed();
    assert!(
        leaked.is_empty(),
        "a refused MONITOR must not be attached — this connection received \
         another client's traffic: {:?}",
        s(&leaked)
    );
}

#[test]
fn mon15_monitor_rejects_arguments() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let r = c.send(&["MONITOR", "extra"]);
    assert_eq!(
        s(&r),
        "-ERR wrong number of arguments for 'monitor' command\r\n",
        "MONITOR has arity 1"
    );
    let mut other = Conn::open(m.port);
    other.send(&["SET", "k", "v"]);
    assert!(
        c.feed().is_empty(),
        "and the connection was not attached by the failed call"
    );
}

#[test]
fn mon16_monitor_conn_cannot_touch_keyspace() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let r = mon.send(&["SET", "x", "1"]);
    assert_eq!(
        s(&r),
        "-ERR Replica can't interact with the keyspace\r\n",
        "verbatim Redis text for a keyspace command on a monitor connection"
    );

    let mut c = Conn::open(m.port);
    c.send(&["SET", "still", "flowing"]);
    assert!(
        names(&mon.feed(), "SET"),
        "and the refusal does not break the feed"
    );
}

#[test]
fn mon17_second_monitor_is_silent() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let again = mon.send(&["MONITOR"]);
    assert!(
        again.is_empty(),
        "MONITOR on an already-attached connection is answered with NOTHING — \
         measured; Redis does not error here. Got {:?}",
        s(&again)
    );

    let mut c = Conn::open(m.port);
    c.send(&["SET", "once", "1"]);
    let ls = lines(&mon.feed());
    assert_eq!(
        ls.len(),
        1,
        "and the connection is attached exactly ONCE — a second registration \
         would duplicate every line; got {ls:?}"
    );
}

#[test]
fn mon18_reset_detaches() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    assert_eq!(s(&mon.send(&["RESET"])), "+RESET\r\n");

    let mut c = Conn::open(m.port);
    c.send(&["SET", "postreset", "1"]);
    assert!(mon.feed().is_empty(), "RESET detaches: the feed stops");
    assert_eq!(
        s(&mon.send(&["GET", "postreset"])),
        "$1\r\n1\r\n",
        "and keyspace access is restored"
    );
}

#[test]
fn mon19_command_info_monitor() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let r = c.send(&["COMMAND", "INFO", "monitor"]);
    let text = s(&r);
    assert!(
        text.contains("monitor"),
        "MONITOR must be registered so COMMAND and ACL can see it; got {text:?}"
    );
    assert!(
        !text.contains("*-1") && !text.starts_with("*1\r\n*-1"),
        "and answer a real spec rather than a Null element; got {text:?}"
    );
    assert!(
        text.contains("admin"),
        "carrying the admin category, or any user could read every other \
         user's traffic; got {text:?}"
    );
}

// ── M6b: the audit table, driven row by row ─────────────────────────────────

#[test]
fn mon20_admin_audit_table() {
    // The regression guard for the §0 audit. Moon's own CommandFlags::ADMIN is
    // CONTAINER-granular and cannot express this table: flagging on it would
    // hide INFO, CLIENT GETNAME, ACL WHOAMI and CLUSTER INFO, all of which
    // Redis feeds. Every row here was measured against redis-server 8.6.1.
    let m = spawn_moon("1");

    // (command, must_be_fed)
    let cases: &[(&[&str], bool)] = &[
        // Fed — not administrative in Redis.
        (&["INFO", "server"], true),
        (&["DBSIZE"], true),
        (&["LASTSAVE"], true),
        (&["COMMAND", "COUNT"], true),
        (&["CLIENT", "GETNAME"], true),
        (&["CLIENT", "ID"], true),
        (&["ACL", "WHOAMI"], true),
        (&["ACL", "CAT"], true),
        (&["CLUSTER", "INFO"], true),
        (&["MEMORY", "USAGE", "nokey"], true),
        // Fed despite Redis's own `skip_monitor` flag — measured directly.
        // Skipping the EVAL family would hide exactly what an operator most
        // wants to see.
        (&["EVAL", "return 1", "0"], true),
        // Hidden — administrative.
        (&["CONFIG", "GET", "maxmemory"], false),
        (&["CONFIG", "SET", "maxmemory", "0"], false),
        (&["CLIENT", "LIST"], false),
        (&["ACL", "LIST"], false),
        (&["SLOWLOG", "LEN"], false),
        (&["SLOWLOG", "RESET"], false),
        (&["LATENCY", "RESET"], false),
    ];

    let mut failures: Vec<String> = Vec::new();
    for (cmd, want_fed) in cases {
        let mut mon = attach(m.port);
        let mut c = Conn::open(m.port);
        c.send(cmd);
        let got = mon.feed();
        let fed = names(&got, cmd[0]);
        if fed != *want_fed {
            failures.push(format!(
                "  {:30} expected {:6} got {:6}",
                cmd.join(" "),
                if *want_fed { "fed" } else { "hidden" },
                if fed { "fed" } else { "hidden" }
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "monitor visibility diverges from redis-server 8.6.1 on {} row(s).\n\
         A row that flipped to `fed` is a LEAK of administrative arguments; a \
         row that flipped to `hidden` silently under-reports the feed.\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ── The contracted backpressure policy ──────────────────────────────────────

#[test]
fn mon21_slow_monitor_is_dropped_not_stalled() {
    // Contracted at freeze: a monitor that cannot keep up has its CONNECTION
    // DROPPED, loudly. Not silent line-dropping (an operator cannot tell a
    // quiet server from a lossy feed) and never blocking (one slow TCP reader
    // must not stall every shard).
    let m = spawn_moon("1");
    let mon = attach(m.port);
    // Deliberately never read from `mon` again.

    let mut c = Conn::open(m.port);
    let start = Instant::now();
    for i in 0..20_000 {
        c.write_cmd(&[b"SET", b"burst", i.to_string().as_bytes()]);
    }
    let _ = c.drain();
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(20),
        "a monitor that stopped reading must never stall the publishing \
         connection; the burst took {elapsed:?}"
    );

    // And the publisher is still healthy.
    let mut probe = Conn::open(m.port);
    assert_eq!(
        s(&probe.send(&["PING"])),
        "+PONG\r\n",
        "the server is still serving after a slow monitor was shed"
    );

    // The monitor connection itself was closed rather than left half-alive.
    let mut mon = mon;
    let after = mon.drain();
    let closed = after.is_empty() || mon.send(&["PING"]).is_empty();
    assert!(
        closed,
        "the slow monitor's connection must be CLOSED, not silently starved \
         of lines — a lossy feed an operator cannot detect is the failure mode \
         this policy exists to avoid"
    );
}

// ── contract: <addr> is the literal `lua` for script-issued commands ────────

#[test]
fn mon22_script_issued_commands_are_fed_with_the_lua_address() {
    // Measured against redis-server 8.6.1 (2026-08-14):
    //   …[0 127.0.0.1:51772] "eval" "redis.call('SET', …)" "1" "lk"
    //   …[0 lua] "SET" "lk" "v"
    //   …[0 lua] "GET" "lk"
    // The EVAL line carries the client's address; each command the SCRIPT
    // issues carries the literal `lua` instead, in execution order, after it.
    //
    // This is the one contract clause with no connection behind it — a script
    // command never passes a connection handler, so the handler-level hooks
    // cannot see it. An implementation that feeds only what clients send looks
    // completely correct until an operator watches a script-driven workload and
    // sees the EVAL but none of its effects.
    let m = spawn_moon("1");
    let mut mon = attach(m.port);

    let mut c = Conn::open(m.port);
    let r = c.send(&[
        "EVAL",
        "redis.call('SET', KEYS[1], 'v') return redis.call('GET', KEYS[1])",
        "1",
        "lk",
    ]);
    assert!(
        s(&r).contains('v'),
        "the script itself must run; got {:?}",
        s(&r)
    );

    let feed = mon.feed();
    let ls = lines(&feed);

    let eval_at = ls
        .iter()
        .position(|l| l.contains("\"EVAL\"") || l.contains("\"eval\""))
        .unwrap_or_else(|| panic!("the EVAL itself must be fed; got {ls:#?}"));
    let set_at = ls
        .iter()
        .position(|l| l.contains("\"SET\"") && l.contains("\"lk\""))
        .unwrap_or_else(|| panic!("the script's SET must be fed; got {ls:#?}"));
    let get_at = ls
        .iter()
        .position(|l| l.contains("\"GET\"") && l.contains("\"lk\""))
        .unwrap_or_else(|| panic!("the script's GET must be fed; got {ls:#?}"));

    assert!(
        eval_at < set_at && set_at < get_at,
        "script effects follow the EVAL, in execution order; got {ls:#?}"
    );
    assert!(
        ls[set_at].contains("[0 lua] "),
        "a script-issued command carries the literal `lua` address, not a \
         peer address: {:?}",
        ls[set_at]
    );
    assert!(
        ls[get_at].contains("[0 lua] "),
        "reads issued by a script are fed the same way: {:?}",
        ls[get_at]
    );
}
