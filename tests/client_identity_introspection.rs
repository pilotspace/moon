//! ADD task `client-identity-introspection` — failing-first suite.
//!
//! The identity/introspection surface answers from constants instead of from
//! server state. Measured on `main` @ec0c4650 against `redis-server` 8.6.1,
//! raw RESP on both sides:
//!
//! | input | moon | redis 8.6.1 |
//! |---|---|---|
//! | `COMMAND COUNT` | `*0` | `:274` |
//! | `COMMAND` (bare) | `:0` | `*274` + 10-field specs |
//! | `COMMAND INFO GET` | `*0` | `*1` + spec |
//! | `COMMAND GETKEYS SET k v` | `*0` | `*1 $1 k` |
//! | `ROLE` | `-ERR unknown command` | `*3 master :0 *0` |
//! | `RESET` | `-ERR unknown command` | `+RESET` |
//! | `CLIENT INFO` | `laddr=127.0.0.1:0` | `laddr=127.0.0.1:<port>` |
//!
//! The headline is that bare `COMMAND` and `COMMAND COUNT` return EACH OTHER'S
//! TYPE — an Integer where an Array belongs and an Array where an Integer
//! belongs. A RESP3-typed driver does not read that as "unsupported"; it reads
//! it as a protocol violation. `src/command/connection.rs:113` says so in its
//! own doc comment.
//!
//! Assertions are on RAW BYTES throughout, because `redis-cli` renders `:0` and
//! `*0` identically as "0" — the rendering is exactly how this survived.
//!
//! Expected RED on main:
//!   ci1  COMMAND COUNT is an Array, not an Integer
//!   ci2  bare COMMAND is an Integer, so there are no specs to count
//!   ci3  COMMAND INFO returns an empty array, not one element per name
//!   ci4  COMMAND GETKEYS extracts nothing
//!   ci5  COMMAND GETKEYS on a keyless command does not reject
//!   ci6  COMMAND COUNT does not enforce arity
//!   ci7  COMMAND LIST names nothing
//!   ci8  ROLE is an unknown command
//!   ci10 RESET is an unknown command
//!   ci11 RESET does not enforce arity
//!   ci13 CLIENT INFO reports laddr port 0
//!
//! ci9 and ci12 (the replica legs of ROLE and HELLO) are `#[ignore]`d: acting
//! as a PSYNC master is monoio-only, so they cannot run on the tokio CI leg.
//! Run them with `--ignored` on a default-features build.
//!
//! Run alone with: cargo test --test client_identity_introspection

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
                // guard; a tripped guard would fail this suite for an
                // unrelated reason.
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
    connect_ready_in(port, None)
}

/// `dir` lets a startup failure print the server's own logs instead of only
/// "never answered PING".
fn connect_ready_in(port: u16, dir: Option<&std::path::Path>) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_err = String::new();
    loop {
        match TcpStream::connect(format!("127.0.0.1:{port}")) {
            Ok(mut s) => {
                s.set_read_timeout(Some(Duration::from_secs(10))).ok();
                s.set_write_timeout(Some(Duration::from_secs(10))).ok();
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
            Err(e) => last_err = e.to_string(),
        }
        assert!(
            Instant::now() < deadline,
            "server on {port} never answered PING in 30s (last connect error: {}){}",
            if last_err.is_empty() {
                "none — connected but no PONG"
            } else {
                &last_err
            },
            dir.map(server_logs).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// RESP reading
//
// A single bounded read is NOT enough here: the bare `COMMAND` reply is one
// element per registered command and arrives across many reads. A test that
// read one chunk would assert against a truncated reply and could pass or fail
// for reasons having nothing to do with the server. So: read until exactly one
// complete top-level value is present.
// ---------------------------------------------------------------------------

/// Byte length of the ONE complete RESP value at the head of `b`, or None if
/// more bytes are needed. Handles the types this suite can encounter.
fn resp_len(b: &[u8]) -> Option<usize> {
    fn line_end(b: &[u8], from: usize) -> Option<usize> {
        // index just past the CRLF
        let mut i = from;
        while i + 1 < b.len() {
            if b[i] == b'\r' && b[i + 1] == b'\n' {
                return Some(i + 2);
            }
            i += 1;
        }
        None
    }
    fn parse_int(b: &[u8], start: usize, end: usize) -> Option<i64> {
        std::str::from_utf8(&b[start..end - 2]).ok()?.parse().ok()
    }
    fn one(b: &[u8], at: usize) -> Option<usize> {
        let kind = *b.get(at)?;
        let hdr = line_end(b, at)?;
        match kind {
            // simple string, error, integer, boolean, double, big number, null
            b'+' | b'-' | b':' | b'#' | b',' | b'(' | b'_' => Some(hdr),
            // bulk string / verbatim: header then N bytes then CRLF
            b'$' | b'=' => {
                let n = parse_int(b, at + 1, hdr)?;
                if n < 0 {
                    return Some(hdr); // $-1 null bulk
                }
                let end = hdr + n as usize + 2;
                if b.len() >= end { Some(end) } else { None }
            }
            // aggregates: N elements (maps have 2N)
            b'*' | b'~' | b'>' | b'%' => {
                let n = parse_int(b, at + 1, hdr)?;
                if n < 0 {
                    return Some(hdr); // *-1 null array
                }
                let count = if kind == b'%' {
                    n as usize * 2
                } else {
                    n as usize
                };
                let mut cur = hdr;
                for _ in 0..count {
                    cur = one(b, cur)?;
                }
                Some(cur)
            }
            _ => None,
        }
    }
    one(b, 0)
}

/// Send one command, return exactly one complete raw reply.
fn cmd(s: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    s.write_all(&out).expect("write command");
    read_one_reply(s)
}

fn read_one_reply(s: &mut TcpStream) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(n) = resp_len(&buf) {
            buf.truncate(n);
            return buf;
        }
        assert!(
            Instant::now() < deadline,
            "incomplete reply after 15s: {:?}",
            String::from_utf8_lossy(&buf[..buf.len().min(200)])
        );
        let mut chunk = vec![0u8; 65536];
        match s.read(&mut chunk) {
            Ok(0) => panic!(
                "server closed mid-reply; got {:?}",
                String::from_utf8_lossy(&buf[..buf.len().min(200)])
            ),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => panic!("read reply: {e}"),
        }
    }
}

fn text(reply: &[u8]) -> String {
    String::from_utf8_lossy(reply).into_owned()
}

/// Element count of an aggregate header, e.g. `*271\r\n` -> 271.
fn agg_count(reply: &[u8]) -> Option<i64> {
    if !matches!(reply.first(), Some(b'*' | b'%' | b'~' | b'>')) {
        return None;
    }
    let end = reply.windows(2).position(|w| w == b"\r\n")? + 1;
    std::str::from_utf8(&reply[1..end - 1]).ok()?.parse().ok()
}

/// Value of an integer reply, e.g. `:271\r\n` -> 271.
fn int_val(reply: &[u8]) -> Option<i64> {
    if reply.first() != Some(&b':') {
        return None;
    }
    let end = reply.windows(2).position(|w| w == b"\r\n")? + 1;
    std::str::from_utf8(&reply[1..end - 1]).ok()?.parse().ok()
}

/// Split the top-level elements of an aggregate into their raw byte slices.
fn agg_elements(reply: &[u8]) -> Vec<Vec<u8>> {
    let Some(n) = agg_count(reply) else {
        return vec![];
    };
    if n <= 0 {
        return vec![];
    }
    let hdr = reply.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    let mut out = Vec::with_capacity(n as usize);
    let mut cur = hdr;
    for _ in 0..n {
        match resp_len(&reply[cur..]) {
            Some(len) => {
                out.push(reply[cur..cur + len].to_vec());
                cur += len;
            }
            None => break,
        }
    }
    out
}

/// Read back what the server said, for a startup failure that would otherwise
/// report only "never answered PING" — a message that names a symptom and no
/// cause, and sends the next person hunting the wrong layer.
fn server_logs(dir: &std::path::Path) -> String {
    let mut out = String::new();
    for f in ["moon.stderr.log", "moon.stdout.log"] {
        match std::fs::read_to_string(dir.join(f)) {
            Ok(s) if !s.trim().is_empty() => {
                let tail: Vec<&str> = s.lines().rev().take(15).collect();
                out.push_str(&format!(
                    "\n--- {f} (last {} lines) ---\n{}\n",
                    tail.len(),
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
                ));
            }
            Ok(_) => out.push_str(&format!("\n--- {f}: empty ---\n")),
            Err(e) => out.push_str(&format!("\n--- {f}: unreadable ({e}) ---\n")),
        }
    }
    out
}

struct Server {
    _guard: ServerGuard,
    _dir: tempfile::TempDir,
    port: u16,
}

/// Serialises server STARTUP across the suite's threads.
///
/// Thirteen servers starting at once — each initialising a data directory —
/// intermittently left one unable to answer PING inside the 30s readiness
/// window: reproduced at ~1 run in 8 with `--test-threads=13`, every failure
/// burning the full 30s rather than erroring fast. `spawn_listening` already
/// waits for the listener to ACCEPT, so the socket was up and the server behind
/// it simply had not finished coming up under the load.
///
/// Two rejected alternatives: a longer timeout (slower AND still flaky under
/// heavier load — it hides the contention rather than removing it), and one
/// `OnceLock` server shared by all tests (statics are never dropped, so the
/// `ServerGuard` would not run and the suite would leak a live moon process
/// past exit — a failure mode this repo has paid for before).
///
/// Holding the lock only across spawn+readiness keeps the test BODIES parallel.
fn startup_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn server(shards: u32) -> Server {
    let _startup = startup_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), shards);
    // Prove the server is serving BEFORE the test body runs, and surface its
    // logs here if it is not, so a startup failure is diagnosed at the point it
    // happens rather than as an opaque timeout inside an assertion.
    drop(connect_ready_in(port, Some(dir.path())));
    Server {
        _guard: ServerGuard(child),
        _dir: dir,
        port,
    }
}

// ---------------------------------------------------------------------------
// COMMAND — the registry-derived replies
// ---------------------------------------------------------------------------

/// ci1: COMMAND COUNT must be an Integer whose value is the registry size.
/// RED on main: replies `*0` — an Array where an Integer belongs.
#[test]
fn ci1_command_count_is_an_integer() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let r = cmd(&mut c, &["COMMAND", "COUNT"]);

    assert_eq!(
        r.first(),
        Some(&b':'),
        "COMMAND COUNT must reply a RESP Integer; got {:?}",
        text(&r)
    );
    let n = int_val(&r).expect("integer reply parses");
    assert!(
        n > 0,
        "COMMAND COUNT must report the real registry size, got {n}"
    );
}

/// ci2: the bare COMMAND array length must equal COMMAND COUNT, and every
/// element must be the 10-field spec the contract froze.
/// RED on main: bare COMMAND replies `:0` — an Integer where an Array belongs.
#[test]
fn ci2_bare_command_array_len_matches_count() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);

    let bare = cmd(&mut c, &["COMMAND"]);
    assert_eq!(
        bare.first(),
        Some(&b'*'),
        "bare COMMAND must reply an Array of specs; got {:?}",
        text(&bare[..bare.len().min(40)])
    );

    let count = int_val(&cmd(&mut c, &["COMMAND", "COUNT"]))
        .expect("COMMAND COUNT must be an integer (see ci1)");
    assert_eq!(
        agg_count(&bare),
        Some(count),
        "bare COMMAND element count must equal COMMAND COUNT"
    );

    let elements = agg_elements(&bare);
    assert_eq!(elements.len() as i64, count, "all specs must be readable");
    for (i, e) in elements.iter().enumerate().take(20) {
        assert_eq!(
            agg_count(e),
            Some(10),
            "spec {i} must have 10 fields (name, arity, flags, first, last, step, \
             acl_cats, tips, key_specs, subcommands); got {:?}",
            text(&e[..e.len().min(60)])
        );
        let fields = agg_elements(e);
        let name = text(&fields[0]);
        assert!(
            name.contains(|ch: char| ch.is_ascii_lowercase()),
            "spec {i} name must be lower-cased as Redis emits it; got {name:?}"
        );
    }
}

/// ci3: COMMAND INFO answers one element per requested name, in request order,
/// with a Null ELEMENT (not an empty array) for an unknown name.
/// RED on main: replies `*0` regardless of what was asked.
#[test]
fn ci3_command_info_order_and_null_element() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let r = cmd(&mut c, &["COMMAND", "INFO", "GET", "nosuchcmd", "SET"]);

    assert_eq!(
        agg_count(&r),
        Some(3),
        "COMMAND INFO must reply one element per requested name; got {:?}",
        text(&r[..r.len().min(60)])
    );
    let e = agg_elements(&r);
    assert!(
        text(&e[0]).contains("get"),
        "element 0 must describe GET; got {:?}",
        text(&e[0])
    );
    assert!(
        e[1].starts_with(b"$-1") || e[1].starts_with(b"_\r\n") || e[1].starts_with(b"*-1"),
        "an unknown name must yield a Null ELEMENT inside the array; got {:?}",
        text(&e[1])
    );
    assert!(
        text(&e[2]).contains("set"),
        "element 2 must describe SET; got {:?}",
        text(&e[2])
    );
}

/// ci4: COMMAND GETKEYS extracts keys using the registry's first/last/step.
/// RED on main: replies `*0`, extracting nothing.
#[test]
fn ci4_command_getkeys_extracts() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let r = cmd(
        &mut c,
        &["COMMAND", "GETKEYS", "MSET", "k1", "v1", "k2", "v2"],
    );

    assert_eq!(
        agg_count(&r),
        Some(2),
        "MSET has two keys at step 2; got {:?}",
        text(&r)
    );
    let e = agg_elements(&r);
    assert!(
        text(&e[0]).contains("k1"),
        "first key; got {:?}",
        text(&e[0])
    );
    assert!(
        text(&e[1]).contains("k2"),
        "second key; got {:?}",
        text(&e[1])
    );
}

/// ci5: COMMAND GETKEYS on a keyless command must reject, not return empty.
/// RED on main: replies `*0`, indistinguishable from "no keys found".
#[test]
fn ci5_command_getkeys_keyless_rejects() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let r = cmd(&mut c, &["COMMAND", "GETKEYS", "PING"]);

    assert!(
        text(&r).contains("The command has no key arguments"),
        "keyless GETKEYS must be an error, not an empty array; got {:?}",
        text(&r)
    );
    // the connection stays usable after the rejection
    assert!(text(&cmd(&mut c, &["PING"])).contains("PONG"));
}

/// ci6: COMMAND COUNT takes no arguments.
/// RED on main: the stub ignores extra args and replies `*0`.
#[test]
fn ci6_command_count_arity_rejects() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let r = cmd(&mut c, &["COMMAND", "COUNT", "extra"]);

    let t = text(&r);
    assert!(
        t.starts_with('-') && t.contains("wrong number of arguments"),
        "COMMAND COUNT must enforce arity; got {t:?}"
    );
    assert!(
        t.contains("command|count"),
        "the error must name the subcommand as 'command|count', matching Redis; got {t:?}"
    );
}

/// ci7: COMMAND LIST names every registered command.
/// RED on main: replies `*0`.
#[test]
fn ci7_command_list_names() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let r = cmd(&mut c, &["COMMAND", "LIST"]);

    let n = agg_count(&r).unwrap_or(0);
    assert!(
        n > 0,
        "COMMAND LIST must name the registered commands; got {:?}",
        text(&r[..r.len().min(60)])
    );
    let joined = text(&r);
    for expect in ["get", "set", "reset"] {
        assert!(
            joined.contains(expect),
            "COMMAND LIST must include {expect:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// ROLE
// ---------------------------------------------------------------------------

/// ci8: ROLE on a master replies [master, offset, replicas[]].
/// RED on main: `-ERR unknown command 'ROLE'`.
#[test]
fn ci8_role_master_shape() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let r = cmd(&mut c, &["ROLE"]);

    assert_eq!(
        agg_count(&r),
        Some(3),
        "ROLE on a master is a 3-element array; got {:?}",
        text(&r)
    );
    let e = agg_elements(&r);
    assert!(
        text(&e[0]).contains("master"),
        "element 0 must be the role; got {:?}",
        text(&e[0])
    );
    assert_eq!(
        e[1].first(),
        Some(&b':'),
        "element 1 must be the replication offset as an Integer; got {:?}",
        text(&e[1])
    );
    assert!(
        e[2].first() == Some(&b'*'),
        "element 2 must be the replica array; got {:?}",
        text(&e[2])
    );
}

/// ci9: ROLE on a replica reports slave, and agrees with INFO replication.
/// Two sources of truth for one fact is the defect class this task closes, so
/// the assertion is that they AGREE, not merely that each is well-formed.
///
/// `#[ignore]`: acting as a PSYNC master is monoio-only, so this cannot run on
/// the tokio CI leg. Run with `--ignored` on a default-features build.
#[test]
#[ignore = "PSYNC-as-master is monoio-only; run with --ignored on default features"]
fn ci9_role_replica_agrees_with_info() {
    let master = server(1);
    let replica = server(1);
    let mut m = connect_ready(master.port);
    assert!(text(&cmd(&mut m, &["PING"])).contains("PONG"));

    let mut r = connect_ready(replica.port);
    cmd(
        &mut r,
        &["REPLICAOF", "127.0.0.1", &master.port.to_string()],
    );
    std::thread::sleep(Duration::from_secs(2));

    let role = cmd(&mut r, &["ROLE"]);
    let e = agg_elements(&role);
    assert!(
        !e.is_empty() && text(&e[0]).contains("slave"),
        "ROLE on a replica must report slave; got {:?}",
        text(&role)
    );

    let info = text(&cmd(&mut r, &["INFO", "replication"]));
    assert!(
        info.contains("role:slave"),
        "INFO must agree with ROLE; got {info:?}"
    );
    assert!(
        info.contains(&format!("master_port:{}", master.port)),
        "ROLE and INFO must name the same master"
    );
}

// ---------------------------------------------------------------------------
// RESET
// ---------------------------------------------------------------------------

/// ci10: RESET returns the connection to default state.
/// Every effect asserted here was measured against redis-server 8.6.1 — in
/// particular the protocol reverting RESP3 -> RESP2, which is easy to assume
/// away.
/// RED on main: `-ERR unknown command 'RESET'`.
#[test]
fn ci10_reset_returns_default_state() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);

    cmd(&mut c, &["HELLO", "3"]);
    cmd(&mut c, &["SELECT", "5"]);
    cmd(&mut c, &["CLIENT", "SETNAME", "bob"]);
    cmd(&mut c, &["WATCH", "k"]);
    cmd(&mut c, &["MULTI"]);

    let r = cmd(&mut c, &["RESET"]);
    assert!(
        r.starts_with(b"+RESET\r\n"),
        "RESET must reply the simple string RESET; got {:?}",
        text(&r)
    );

    assert!(
        text(&cmd(&mut c, &["EXEC"])).contains("without MULTI"),
        "RESET must discard MULTI"
    );

    let name = cmd(&mut c, &["CLIENT", "GETNAME"]);
    assert!(
        name.starts_with(b"$-1") || name.starts_with(b"_\r\n"),
        "RESET must clear the client name; got {:?}",
        text(&name)
    );

    let info = text(&cmd(&mut c, &["CLIENT", "INFO"]));
    assert!(
        info.contains("db=0"),
        "RESET must return to db 0; got {info:?}"
    );

    let hello = cmd(&mut c, &["HELLO"]);
    assert_eq!(
        hello.first(),
        Some(&b'*'),
        "RESET must revert the protocol to RESP2, so bare HELLO replies a flat \
         Array rather than a RESP3 Map; got {:?}",
        text(&hello[..hello.len().min(40)])
    );
}

/// ci11: RESET has arity 1 — the registry already says so.
/// RED on main: unknown command, so arity is never reached.
#[test]
fn ci11_reset_arity_rejects() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);

    cmd(&mut c, &["MULTI"]);
    let r = cmd(&mut c, &["RESET", "now"]);
    let t = text(&r);
    assert!(
        t.starts_with('-') && t.contains("wrong number of arguments"),
        "RESET takes no arguments; got {t:?}"
    );
    // the rejected RESET must not have half-applied: MULTI is still open
    assert!(
        text(&cmd(&mut c, &["EXEC"])).starts_with('*'),
        "a rejected RESET must not discard MULTI"
    );
}

// ---------------------------------------------------------------------------
// HELLO / CLIENT INFO — stop contradicting the rest of the server
// ---------------------------------------------------------------------------

/// ci12: HELLO's role field agrees with INFO replication on the same
/// connection. `hello_acl` hardcodes `role: master`, so on a replica the two
/// contradict each other today.
///
/// `#[ignore]`: same monoio-only constraint as ci9.
#[test]
#[ignore = "PSYNC-as-master is monoio-only; run with --ignored on default features"]
fn ci12_hello_role_matches_info() {
    let master = server(1);
    let replica = server(1);
    let mut r = connect_ready(replica.port);
    cmd(
        &mut r,
        &["REPLICAOF", "127.0.0.1", &master.port.to_string()],
    );
    std::thread::sleep(Duration::from_secs(2));

    let hello = text(&cmd(&mut r, &["HELLO", "3"]));
    let info = text(&cmd(&mut r, &["INFO", "replication"]));
    let role = text(&cmd(&mut r, &["ROLE"]));

    assert!(
        info.contains("role:slave"),
        "precondition: the server must actually be a replica; got {info:?}"
    );
    // Redis uses THREE vocabularies for this ONE fact, measured on 8.6.1
    // against a real replica pair: HELLO says "replica", INFO says "slave",
    // ROLE says "slave". This test originally asserted "slave" in HELLO and
    // failed against a CORRECT implementation — the assertion was wrong, not
    // the code. Corrected against measurement (see §4 of the task record); the
    // point of the test is unchanged and now stronger: all three must agree
    // that this node is a replica, each in its own vocabulary.
    assert!(
        hello.contains("replica"),
        "HELLO must report the real role — Redis spells it 'replica' here, not \
         a hardcoded 'master'; got {hello:?}"
    );
    assert!(
        !hello.contains("$6\r\nmaster"),
        "HELLO must not still claim master on a replica; got {hello:?}"
    );
    assert!(
        role.contains("slave"),
        "ROLE spells the same fact 'slave'; got {role:?}"
    );
}

/// ci13: CLIENT INFO reports the real local address.
/// RED on main: `laddr=127.0.0.1:0` is a literal in the format string at
/// `src/client_registry.rs:709`.
#[test]
fn ci13_client_info_laddr_real_port() {
    let srv = server(1);
    let mut c = connect_ready(srv.port);
    let info = text(&cmd(&mut c, &["CLIENT", "INFO"]));

    let laddr = info
        .split_whitespace()
        .find(|f| f.starts_with("laddr="))
        .unwrap_or_else(|| panic!("CLIENT INFO must carry an laddr field; got {info:?}"))
        .to_string();

    assert!(
        !laddr.ends_with(":0"),
        "laddr must be the real local address, not port 0; got {laddr:?}"
    );
    assert!(
        laddr.ends_with(&format!(":{}", srv.port)),
        "laddr must carry the port the client connected to ({}); got {laddr:?}",
        srv.port
    );
}

// ---------------------------------------------------------------------------
// ci14 — the THIRD handler.
//
// `handler_single` is not reachable from the shipped binary: `main.rs` and
// `embedded.rs` both route through `run_sharded` -> `handler_sharded`. It is
// driven only by `listener::run_with_shutdown`, a tokio-only in-process API
// that several suites (kill_snapshot, graph_bench_*) use. Every other test in
// this file spawns a real `moon` process and therefore CANNOT reach it — an
// A/B proved exactly that: reverting the fix below left ci12 green.
//
// That blind spot is how this surface drifted in the first place, so the third
// copy gets its own test rather than an assurance.
#[cfg(feature = "runtime-tokio")]
// MUST be multi_thread: the body drives BLOCKING sockets and `std::thread::sleep`
// (the same raw-RESP helpers the rest of this file uses, so the assertions stay
// byte-level). On the default current-thread runtime those block the one worker
// and the spawned listener is never polled — the first cut of this test failed
// with "never answered PING" for exactly that reason, not for a server bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ci14_handler_single_identity_surface() {
    use moon::config::ServerConfig;
    use moon::runtime::cancel::CancellationToken;
    use moon::server::listener;

    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);

    let dir = tempfile::tempdir().expect("tempdir");
    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        shards: 1,
        dir: dir.path().to_string_lossy().to_string(),
        appendonly: "no".to_string(),
        // MUST be set explicitly. `ServerConfig` derives `Default`, but the
        // sane values live in clap `default_value_t` attributes, which apply
        // ONLY to CLI parsing — `Default::default()` leaves this 0, and the
        // handler then indexes `db[0]` of an empty slice and panics on the
        // first command. That is a harness trap, not a server defect.
        databases: 16,
        // The data dir sits on a volume that hovers near the 5% diskfull guard;
        // 0 disables it so a full disk cannot masquerade as an identity bug.
        disk_free_min_pct: 0,
        ..Default::default()
    };

    let token = CancellationToken::new();
    let server_token = token.clone();
    tokio::spawn(async move {
        if let Err(e) = listener::run_with_shutdown(config, server_token).await {
            eprintln!("ci14: run_with_shutdown failed: {e:#}");
        }
    });

    // Same readiness discipline as `connect_ready`: poll for a real PONG rather
    // than sleeping a hopeful constant.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut c = loop {
        assert!(
            std::time::Instant::now() < deadline,
            "handler_single server on port {port} never answered PING"
        );
        if let Ok(mut s) = TcpStream::connect(format!("127.0.0.1:{port}")) {
            s.set_read_timeout(Some(Duration::from_secs(10))).ok();
            s.set_write_timeout(Some(Duration::from_secs(10))).ok();
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = s.read(&mut buf)
                    && n > 0
                    && buf[..n].windows(4).any(|w| w == b"PONG")
                {
                    break s;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    // ROLE — was an unknown command on this handler before this task.
    let role = cmd(&mut c, &["ROLE"]);
    assert!(
        role.starts_with(b"*3\r\n"),
        "handler_single ROLE must reply the 3-element master form; got {:?}",
        String::from_utf8_lossy(&role)
    );
    assert!(
        role.windows(6).any(|w| w == b"master"),
        "handler_single ROLE must name the role; got {:?}",
        String::from_utf8_lossy(&role)
    );

    // HELLO's role field must be DERIVED. This server is a master, so the
    // assertion a hard-coded "master" would also satisfy is worthless on its
    // own — what it pins is that the field is present and well-formed, while
    // the derivation itself is pinned by ci12 on the handlers that CI can spawn
    // a replica for.
    let hello = cmd(&mut c, &["HELLO"]);
    assert!(
        hello.windows(4).any(|w| w == b"role"),
        "handler_single HELLO must carry a role field; got {:?}",
        String::from_utf8_lossy(&hello)
    );

    // RESET — was likewise unknown here; it must reply +RESET and, per §1, be
    // executed immediately inside MULTI rather than queued.
    let reset = cmd(&mut c, &["RESET"]);
    assert_eq!(
        &reset[..],
        b"+RESET\r\n",
        "handler_single RESET must reply +RESET; got {:?}",
        String::from_utf8_lossy(&reset)
    );

    assert_eq!(&cmd(&mut c, &["MULTI"])[..], b"+OK\r\n");
    let in_multi = cmd(&mut c, &["RESET"]);
    assert_eq!(
        &in_multi[..],
        b"+RESET\r\n",
        "RESET must execute immediately inside MULTI, never queue; got {:?}",
        String::from_utf8_lossy(&in_multi)
    );
    let exec = cmd(&mut c, &["EXEC"]);
    assert!(
        exec.starts_with(b"-ERR EXEC without MULTI"),
        "RESET must have discarded the transaction; got {:?}",
        String::from_utf8_lossy(&exec)
    );

    token.cancel();
}
