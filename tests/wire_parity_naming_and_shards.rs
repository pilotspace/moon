//! Command names in arity errors, and the shard count in `INFO Server`.
//!
//! Covers #491 (Moon upper-cased the command name where Redis always uses the
//! registered lower-case name, and spelled container subcommands with a space
//! where Redis uses `parent|sub`) and #497 (no way for a client to learn how
//! many shards it is talking to).
//!
//! Every expected string here was measured against `redis-server 8.0.5` on the
//! moon-dev VM over a raw socket, one fresh connection per probe. Two rules
//! came out of that measurement and they are NOT the same rule:
//!
//!   * an **arity** error names the command as REGISTERED — lower case, and
//!     `parent|sub` for a container command;
//!   * an **unknown command** error echoes back what the client actually sent,
//!     preserving its case.
//!
//! Guard tests below pin both, because "lower-case everything" would break the
//! second one.

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
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-wirename-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-wirename-{port}"));
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

fn encode(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// Send one command, return the raw reply as a string.
fn cmd(port: u16, args: &[&str]) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(&encode(args)).expect("write");
    let mut reply = Vec::new();
    let mut buf = [0u8; 16384];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                reply.extend_from_slice(&buf[..n]);
                // One reply is enough for every probe here; INFO is the only
                // large one and it ends with a lone CRLF after the payload.
                if reply.ends_with(b"\r\n") {
                    let _ = s.set_read_timeout(Some(Duration::from_millis(150)));
                }
            }
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&reply).into_owned()
}

// === #491: arity errors name the command as Redis registers it ===

/// (argv, expected name inside the arity error) — measured against redis 8.0.5.
const ARITY_CASES: &[(&[&str], &str)] = &[
    (&["ECHO", "a", "b"], "echo"),
    (&["GET"], "get"),
    (&["MSETNX", "k"], "msetnx"),
    (&["BITOP"], "bitop"),
    (&["COPY"], "copy"),
    (&["MOVE", "k"], "move"),
    (&["SELECT"], "select"),
    (&["HEXPIRE", "k"], "hexpire"),
    // Container commands use `parent|sub`, NOT a space. This is a form change
    // as well as a case change, which is why guessing from the issue title
    // alone would have produced the wrong string.
    (&["CLIENT", "SETNAME", "a", "b"], "client|setname"),
    (&["CLIENT", "PAUSE"], "client|pause"),
    (&["ACL", "GETUSER"], "acl|getuser"),
    (&["ACL", "SETUSER"], "acl|setuser"),
    (&["ACL", "DELUSER"], "acl|deluser"),
    // Already correct before #491; kept so the sweep cannot regress them.
    (&["CONFIG", "GET"], "config|get"),
    (&["COMMAND", "COUNT", "x"], "command|count"),
];

#[test]
fn arity_errors_use_the_registered_command_name() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        for (argv, want_name) in ARITY_CASES {
            let reply = cmd(moon.port, argv);
            let expected = format!("-ERR wrong number of arguments for '{want_name}' command\r\n");
            assert_eq!(
                reply, expected,
                "argv {argv:?} at shards={shards}\n  got: {reply:?}"
            );
        }
    }
}

// === #580: CLIENT NO-EVICT / NO-TOUCH must not accept a missing argument ===

/// (argv, expected raw reply) — measured against redis-server 8.6.1 over a raw
/// socket on 2026-08-20. `client|no-evict` and `client|no-touch` are registered
/// with arity **3**, so BOTH a missing and an extra argument are arity errors;
/// only a present-but-unrecognised value is a syntax error.
const NO_EVICT_CASES: &[(&[&str], &str)] = &[
    (
        &["CLIENT", "NO-EVICT"],
        "-ERR wrong number of arguments for 'client|no-evict' command\r\n",
    ),
    (
        &["CLIENT", "NO-EVICT", "ON", "EXTRA"],
        "-ERR wrong number of arguments for 'client|no-evict' command\r\n",
    ),
    (&["CLIENT", "NO-EVICT", "MAYBE"], "-ERR syntax error\r\n"),
    (&["CLIENT", "NO-EVICT", "ON"], "+OK\r\n"),
    (&["CLIENT", "NO-EVICT", "OFF"], "+OK\r\n"),
    // Case-insensitive, as the wire allows.
    (&["client", "no-evict", "on"], "+OK\r\n"),
    (
        &["CLIENT", "NO-TOUCH"],
        "-ERR wrong number of arguments for 'client|no-touch' command\r\n",
    ),
    (
        &["CLIENT", "NO-TOUCH", "ON", "EXTRA"],
        "-ERR wrong number of arguments for 'client|no-touch' command\r\n",
    ),
    (&["CLIENT", "NO-TOUCH", "BOGUS"], "-ERR syntax error\r\n"),
    (&["CLIENT", "NO-TOUCH", "ON"], "+OK\r\n"),
    (&["CLIENT", "NO-TOUCH", "OFF"], "+OK\r\n"),
];

#[test]
fn client_no_evict_and_no_touch_require_their_on_off_argument() {
    // Moon used to answer `+OK` to EVERY one of these, including the malformed
    // forms — telling a client the setting had been applied when nothing was
    // ever parsed (#580). Driven at shards=1 and shards=4 because the CLIENT
    // subcommand table is duplicated across dispatch paths.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        for (argv, want) in NO_EVICT_CASES {
            let reply = cmd(moon.port, argv);
            assert_eq!(
                reply, *want,
                "argv {argv:?} at shards={shards}\n  got: {reply:?}"
            );
        }
    }
}

// === guards: the OTHER naming rule must not be swept up ===

#[test]
fn unknown_command_echoes_the_clients_casing() {
    // redis 8.0.5 replies `unknown command 'NoSuchCmd'` — it does NOT
    // normalise here, because there is no registered name to normalise to.
    // A blanket lower-casing of every interpolated command name would break
    // this, so it is pinned.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        let reply = cmd(moon.port, &["NoSuchCmd", "x"]);
        assert!(
            reply.starts_with("-ERR unknown command 'NoSuchCmd'"),
            "unknown-command errors must echo the client's casing (shards={shards}); got: {reply:?}"
        );
    }
}

#[test]
fn valid_commands_still_succeed_after_the_sweep() {
    // Cheap blast-radius check: the sweep edits error text across many files,
    // and a botched edit is far more likely to break a happy path than an
    // error path.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert_eq!(cmd(moon.port, &["ECHO", "hi"]), "$2\r\nhi\r\n");
        assert_eq!(cmd(moon.port, &["SET", "k", "v"]), "+OK\r\n");
        assert_eq!(cmd(moon.port, &["GET", "k"]), "$1\r\nv\r\n");
        assert_eq!(cmd(moon.port, &["CLIENT", "SETNAME", "n"]), "+OK\r\n");
    }
}

// === #497: INFO Server reports the shard count ===

fn info_field(reply: &str, key: &str) -> Option<String> {
    reply.lines().find_map(|l| {
        let l = l.trim_end_matches('\r');
        l.strip_prefix(&format!("{key}:")).map(|v| v.to_string())
    })
}

#[test]
fn info_server_reports_num_shards() {
    // A client that cannot operate multi-shard (one cross-key TXN per ingest,
    // in the reported case) has to refuse at connect time. Without this field
    // the cheapest reliable probe is a two-key co-location canary, which is
    // both slow and a guess.
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        let reply = cmd(moon.port, &["INFO", "server"]);
        let got = info_field(&reply, "num_shards").unwrap_or_else(|| {
            panic!("INFO server has no num_shards field (shards={shards}); got:\n{reply}")
        });
        assert_eq!(
            got, shards,
            "num_shards must be the configured shard count (shards={shards})"
        );
    }
}

#[test]
fn info_server_num_shards_is_never_zero() {
    // The field exists so a client can divide by it / branch on `> 1`.
    // Reporting 0 would be worse than omitting it: it reads as a valid answer
    // and is not one.
    let moon = spawn_moon("1");
    let reply = cmd(moon.port, &["INFO"]);
    let got = info_field(&reply, "num_shards").expect("num_shards present in full INFO too");
    assert_ne!(got, "0", "num_shards must never be reported as zero");
}
