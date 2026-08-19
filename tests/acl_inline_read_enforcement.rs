//! ACL enforcement on the inline read fast path (client-compat P0).
//!
//! `try_inline_dispatch` (`src/server/conn/blocking.rs`) answers plain
//! `GET key` straight from the shard's map without entering the generic
//! dispatch path. Writes are gated on `can_inline_writes`, which folds in
//! `conn.acl_skip_allowed()` — reads were gated on **nothing**, so an
//! authenticated-but-restricted user read any key by name:
//!
//! ```text
//! ACL SETUSER locked on >pw -@all
//! AUTH locked pw            -> +OK
//! GET secret                -> "value"     (Redis: -NOPERM)
//! SET/DEL/MGET/HGET/TTL/... -> -NOPERM     (correctly gated)
//! ```
//!
//! Not a single-shard quirk: at `--shards 4` the leak covers every key that
//! hashes to the connection's own shard (measured ~27% of a 480-GET sweep).
//! `--shards 1` simply makes it 100%.
//!
//! The inline path exists only in the **monoio** handler
//! (`handler_monoio/mod.rs`); the tokio handlers gate correctly at
//! `handler_single.rs:1587` / `handler_sharded/mod.rs:761`. Every CI test job
//! builds tokio, which is why this was invisible. Under a tokio build these
//! tests still pass — they just stop being a regression guard, so keep a
//! monoio run in the release gate.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Number of distinct keys probed per connection. Must comfortably exceed
/// the shard count so that at `--shards 4` some keys land on the
/// connection's own shard (only those are inline-eligible).
const KEYS: usize = 40;
/// Connections used per probe: each lands on a different shard thread, so
/// collectively they cover every shard's inline path.
const CONNS: usize = 4;

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
    // fresh and feature-matched. Never probe target/release directly.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-aclinline-{port}"));
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
                // This host hovers near the 5% diskfull line — the guard
                // would turn every SET into a MOONERR and flake the suite.
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            // Captured, not discarded: when readiness fails the panic below
            // prints this, so the failure names its cause instead of just its
            // symptom.
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-aclinline-{port}"));
    let mut moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(10);
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
    // Never skip. These tests are the regression guard for a security bypass
    // and a transaction-correctness bug; a silent early return would let them
    // report green while exercising no server at all.
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr"))
        .unwrap_or_else(|e| format!("<stderr log unreadable: {e}>"));
    panic!(
        "moon did not become ready on port {port} within 10s (--shards {shards}); \
         child {status}\n--- moon stderr ---\n{log}"
    );
}

/// Minimal synchronous RESP client: one command, one reply.
struct Resp {
    stream: TcpStream,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(1500)))
            .unwrap();
        Self { stream }
    }

    /// Send `args` and return the raw first chunk of the reply. Every command
    /// used here yields a single small frame, so one read is sufficient.
    fn cmd(&mut self, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
        let mut buf = [0u8; 4096];
        match self.stream.read(&mut buf) {
            Ok(n) => String::from_utf8_lossy(&buf[..n]).into_owned(),
            Err(e) => format!("<read error: {e}>"),
        }
    }
}

/// Seed `KEYS` values and define `user` with the given ACL rules.
fn seed(port: u16, user: &str, rules: &[&str]) {
    let mut admin = Resp::connect(port);
    for i in 0..KEYS {
        let key = format!("secret{i}");
        let val = format!("leaked{i}");
        assert!(
            admin.cmd(&["SET", &key, &val]).starts_with("+OK"),
            "seed SET must succeed"
        );
    }
    assert!(admin.cmd(&["SET", "app:ok", "public"]).starts_with("+OK"));
    let mut args = vec!["ACL", "SETUSER", user, "on", ">pw"];
    args.extend_from_slice(rules);
    let reply = admin.cmd(&args);
    assert!(reply.starts_with("+OK"), "ACL SETUSER failed: {reply:?}");
}

/// Every `GET secret*` issued by `user` must be refused. Returns the leaks.
fn collect_get_leaks(port: u16, user: &str) -> Vec<(String, String)> {
    let mut leaks = Vec::new();
    for _ in 0..CONNS {
        let mut c = Resp::connect(port);
        let auth = c.cmd(&["AUTH", user, "pw"]);
        assert!(auth.starts_with("+OK"), "AUTH failed: {auth:?}");
        for i in 0..KEYS {
            let key = format!("secret{i}");
            let reply = c.cmd(&["GET", &key]);
            if !reply.starts_with("-NOPERM") {
                leaks.push((key, reply.replace("\r\n", "\\r\\n")));
            }
        }
    }
    leaks
}

fn assert_no_leaks(shards: &str, user: &str, rules: &[&str]) {
    let m = spawn_moon(shards);
    seed(m.port, user, rules);
    let leaks = collect_get_leaks(m.port, user);
    assert!(
        leaks.is_empty(),
        "--shards {shards}: user '{user}' ({rules:?}) read {} of {} keys via the inline GET \
         fast path; ACL must deny every one. First leaks: {:?}",
        leaks.len(),
        KEYS * CONNS,
        &leaks[..leaks.len().min(5)]
    );
}

// ── deny-all user: the command check must reject GET ────────────────────

#[test]
fn deny_all_user_cannot_inline_get_single_shard() {
    assert_no_leaks("1", "lockedone", &["-@all"]);
}

#[test]
fn deny_all_user_cannot_inline_get_multi_shard() {
    assert_no_leaks("4", "lockedfour", &["-@all"]);
}

// ── key-pattern user: the key check must reject out-of-pattern GET ──────

fn assert_pattern_enforced(shards: &str, user: &str) {
    let m = spawn_moon(shards);
    seed(m.port, user, &["+@read", "~app:*"]);

    let mut c = Resp::connect(m.port);
    assert!(c.cmd(&["AUTH", user, "pw"]).starts_with("+OK"));

    // In-pattern read is allowed and returns the real value.
    let ok = c.cmd(&["GET", "app:ok"]);
    assert!(
        ok.contains("public"),
        "--shards {shards}: in-pattern GET must succeed, got {ok:?}"
    );

    // Out-of-pattern reads must be refused on every key, inline-eligible or not.
    let leaks = collect_get_leaks(m.port, user);
    assert!(
        leaks.is_empty(),
        "--shards {shards}: user '{user}' (+@read ~app:*) read {} keys outside its pattern \
         via the inline GET fast path. First leaks: {:?}",
        leaks.len(),
        &leaks[..leaks.len().min(5)]
    );
}

#[test]
fn key_pattern_enforced_on_inline_get_single_shard() {
    assert_pattern_enforced("1", "scopedone");
}

#[test]
fn key_pattern_enforced_on_inline_get_multi_shard() {
    assert_pattern_enforced("4", "scopedfour");
}

// ── multi-key commands: every key position must be checked (moon#566) ────
//
// The ACL key check used to read a command's keys from a hand-maintained
// name-keyed match whose fallthrough was an empty list — and an empty key
// list makes the permission loop a no-op, so `~pattern` was ignored outright
// for every command it forgot. The commands below are the ones that had that
// shape; they are exercised END TO END here (through the real dispatch path
// of the handler the build actually ships) because the hole was invisible to
// every unit test that only asked about GET/SET/MSET.
//
// `GET` in the same run is the inline-fast-path control: the fast path is
// gated on `acl_skip_allowed()`, so a restricted user must fall back to the
// generic path and be checked there — in-pattern GET still has to work.

/// Out-of-pattern argv per command. Every entry must be answered `-NOPERM`.
fn out_of_pattern_probes() -> Vec<Vec<String>> {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    vec![
        // two-key forms: SOURCE out of pattern, then DESTINATION out of pattern
        s(&["SMOVE", "evil:src", "app:{t}:dst", "m"]),
        s(&["SMOVE", "app:{t}:src", "evil:dst", "m"]),
        s(&["COPY", "evil:src", "app:{t}:dst"]),
        s(&["COPY", "app:{t}:src", "evil:dst"]),
        s(&["ZRANGESTORE", "evil:dst", "app:{t}:src", "0", "-1"]),
        s(&["ZRANGESTORE", "app:{t}:dst", "evil:src", "0", "-1"]),
        s(&["LMOVE", "evil:src", "app:{t}:dst", "LEFT", "RIGHT"]),
        s(&["LMOVE", "app:{t}:src", "evil:dst", "LEFT", "RIGHT"]),
        // numkeys families: first slot, then a later slot
        s(&["LMPOP", "2", "evil:a", "app:{t}:b", "LEFT"]),
        s(&["LMPOP", "2", "app:{t}:a", "evil:b", "LEFT"]),
        s(&["ZMPOP", "2", "app:{t}:a", "evil:b", "MIN"]),
        s(&["BLMPOP", "0.01", "2", "app:{t}:a", "evil:b", "LEFT"]),
        s(&["BZMPOP", "0.01", "2", "app:{t}:a", "evil:b", "MIN"]),
        s(&["SINTERCARD", "2", "app:{t}:a", "evil:b"]),
        s(&["ZDIFF", "2", "app:{t}:a", "evil:b"]),
        s(&["ZINTER", "2", "app:{t}:a", "evil:b"]),
        s(&["ZUNION", "2", "app:{t}:a", "evil:b"]),
        s(&["ZINTERCARD", "2", "app:{t}:a", "evil:b"]),
        s(&["ZUNIONSTORE", "evil:dst", "2", "app:{t}:a", "app:{t}:b"]),
        s(&["ZUNIONSTORE", "app:{t}:dst", "2", "app:{t}:a", "evil:b"]),
        // positional STORE clauses
        s(&["SORT", "evil:src", "STORE", "app:{t}:dst"]),
        s(&["SORT", "app:{t}:src", "STORE", "evil:dst"]),
        s(&["SORT", "app:{t}:src", "BY", "evil:*"]),
        s(&[
            "GEORADIUS",
            "app:{t}:geo",
            "0",
            "0",
            "1",
            "km",
            "STORE",
            "evil:dst",
        ]),
        // streams, scripting, subcommand-shaped key positions
        s(&["XREAD", "COUNT", "1", "STREAMS", "evil:s", "0"]),
        s(&["EVAL", "return 1", "1", "evil:a"]),
        s(&["OBJECT", "ENCODING", "evil:a"]),
        s(&["MEMORY", "USAGE", "evil:a"]),
        // plain single-key control (was already enforced)
        s(&["GET", "evil:a"]),
        s(&["SET", "evil:a", "v"]),
    ]
}

/// In-pattern argv that must NOT be refused: proves the fix is enforcement,
/// not a blanket denial. Replies may be errors of other kinds (empty key,
/// wrong type) — only `-NOPERM` is a failure.
fn in_pattern_probes() -> Vec<Vec<String>> {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    vec![
        s(&["GET", "app:{t}:str"]),
        s(&["SET", "app:{t}:str", "v"]),
        s(&["SMOVE", "app:{t}:s1", "app:{t}:s2", "m"]),
        s(&["COPY", "app:{t}:str", "app:{t}:copy"]),
        s(&["ZRANGESTORE", "app:{t}:zd", "app:{t}:z1", "0", "-1"]),
        s(&["LMPOP", "2", "app:{t}:l1", "app:{t}:l2", "LEFT"]),
        s(&["SINTERCARD", "2", "app:{t}:s1", "app:{t}:s2"]),
        s(&["ZDIFF", "2", "app:{t}:z1", "app:{t}:z2"]),
        s(&["ZUNIONSTORE", "app:{t}:zd", "2", "app:{t}:z1", "app:{t}:z2"]),
        s(&["SORT", "app:{t}:l1", "ALPHA", "STORE", "app:{t}:sorted"]),
        s(&["EVAL", "return 1", "1", "app:{t}:str"]),
        s(&["OBJECT", "ENCODING", "app:{t}:str"]),
        // keyless commands must be completely unaffected by the key check
        s(&["PING"]),
        s(&["ECHO", "hi"]),
        s(&["DBSIZE"]),
        s(&["COMMAND", "COUNT"]),
        s(&["SCAN", "0"]),
    ]
}

fn assert_multi_key_patterns_enforced(shards: &str, user: &str) {
    let m = spawn_moon(shards);
    seed(m.port, user, &["+@all", "~app:*"]);

    // Seed the in-pattern fixtures with an admin connection.
    let mut admin = Resp::connect(m.port);
    assert!(admin.cmd(&["SET", "app:{t}:str", "v"]).starts_with("+OK"));
    admin.cmd(&["SADD", "app:{t}:s1", "m"]);
    admin.cmd(&["SADD", "app:{t}:s2", "m2"]);
    admin.cmd(&["ZADD", "app:{t}:z1", "1", "a"]);
    admin.cmd(&["ZADD", "app:{t}:z2", "1", "b"]);
    admin.cmd(&["RPUSH", "app:{t}:l1", "a"]);
    admin.cmd(&["RPUSH", "app:{t}:l2", "b"]);

    let mut c = Resp::connect(m.port);
    assert!(c.cmd(&["AUTH", user, "pw"]).starts_with("+OK"));

    let mut allowed_through = Vec::new();
    for probe in out_of_pattern_probes() {
        let argv: Vec<&str> = probe.iter().map(String::as_str).collect();
        let reply = c.cmd(&argv);
        if !reply.starts_with("-NOPERM") {
            allowed_through.push((probe.join(" "), reply.replace("\r\n", "\\r\\n")));
        }
    }
    assert!(
        allowed_through.is_empty(),
        "--shards {shards}: user '{user}' (~app:*) reached keys outside its pattern through \
         {} command(s); every one must answer -NOPERM. Got: {:?}",
        allowed_through.len(),
        allowed_through
    );

    let mut over_denied = Vec::new();
    for probe in in_pattern_probes() {
        let argv: Vec<&str> = probe.iter().map(String::as_str).collect();
        let reply = c.cmd(&argv);
        if reply.starts_with("-NOPERM") {
            over_denied.push((probe.join(" "), reply.replace("\r\n", "\\r\\n")));
        }
    }
    assert!(
        over_denied.is_empty(),
        "--shards {shards}: user '{user}' (~app:*) was denied {} in-pattern or keyless \
         command(s) — the fail-closed default must not over-deny. Got: {:?}",
        over_denied.len(),
        over_denied
    );
}

#[test]
fn multi_key_commands_enforce_key_patterns_single_shard() {
    assert_multi_key_patterns_enforced("1", "multione");
}

#[test]
fn multi_key_commands_enforce_key_patterns_multi_shard() {
    assert_multi_key_patterns_enforced("4", "multifour");
}
