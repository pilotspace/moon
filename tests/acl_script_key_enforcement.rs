//! ACL enforcement for commands issued from inside Lua scripts (moon#569).
//!
//! Follow-up to #566/#582. The dispatcher gates `EVAL script numkeys k1 ...`
//! on the keys the script DECLARES, but `redis.call()` used to reach
//! `Database::execute_command` directly, with no ACL check at all:
//!
//! ```text
//! ACL SETUSER app on >pw ~app:* +@all
//! AUTH app pw                                     -> +OK
//! GET secret:x                                    -> -NOPERM   (gated)
//! EVAL "return redis.call('GET','secret:x')" 0    -> "leaked"  (BYPASS)
//! ```
//!
//! Declaring zero keys made the outer key check a no-op, and everything the
//! script then touched was unchecked. The same hole covered FCALL, and every
//! shape of indirection Lua offers (concatenated key names, closures, nested
//! `pcall`, movable-key commands like `LMPOP` / `SORT ... STORE`).
//!
//! These tests drive a REAL server over the wire with a REAL restricted user.
//! Each one asserts on the reply of a single `EVAL`/`FCALL`, so a regression
//! names the exact escape it re-opened.

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
    // CARGO_BIN_EXE_moon is the binary cargo built for THIS invocation —
    // fresh and feature-matched. Never probe target/release directly.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-aclscript-{port}"));
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
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-aclscript-{port}"));
    let mut moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(15);
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
    // Never skip: these are the regression guard for a privilege-escalation
    // bypass. A silent early return would report green against no server.
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr"))
        .unwrap_or_else(|e| format!("<stderr log unreadable: {e}>"));
    panic!(
        "moon did not become ready on port {port} within 15s (--shards {shards}); \
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
            .set_read_timeout(Some(Duration::from_millis(4000)))
            .unwrap();
        Self { stream }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).expect("write");
        let mut buf = [0u8; 8192];
        match self.stream.read(&mut buf) {
            Ok(n) => String::from_utf8_lossy(&buf[..n]).into_owned(),
            Err(e) => format!("<read error: {e}>"),
        }
    }
}

/// Seed out-of-pattern data as the unrestricted `default` user, then define a
/// user confined to `~app:*` with every command allowed. Commands are NOT the
/// restriction under test — keys are.
fn seed(port: u16) {
    let mut admin = Resp::connect(port);
    for (k, v) in [
        ("secret:x", "leaked"),
        ("secret:y", "leaked2"),
        ("app:ok", "public"),
    ] {
        assert!(
            admin.cmd(&["SET", k, v]).starts_with("+OK"),
            "seed SET {k} must succeed"
        );
    }
    assert!(admin.cmd(&["RPUSH", "secret:list", "a"]).starts_with(":"));
    assert!(admin.cmd(&["RPUSH", "app:list", "b"]).starts_with(":"));
    let reply = admin.cmd(&[
        "ACL", "SETUSER", "app", "on", ">pw", "~app:*", "+@all", "-acl",
    ]);
    assert!(reply.starts_with("+OK"), "ACL SETUSER failed: {reply:?}");
}

fn restricted(port: u16) -> Resp {
    let mut c = Resp::connect(port);
    let auth = c.cmd(&["AUTH", "app", "pw"]);
    assert!(auth.starts_with("+OK"), "AUTH failed: {auth:?}");
    c
}

/// A reply is a refusal if it is a RESP error mentioning the ACL denial. The
/// bridge answers with a `NOPERM`-prefixed error; `pcall`-wrapped scripts may
/// surface it re-wrapped, so match on the marker, not the whole string.
fn is_denied(reply: &str) -> bool {
    reply.starts_with('-') && reply.contains("NOPERM")
}

fn assert_denied(what: &str, reply: &str) {
    assert!(
        is_denied(reply),
        "{what}: expected a NOPERM refusal, got {reply:?}"
    );
    assert!(
        !reply.contains("leaked"),
        "{what}: reply leaked out-of-pattern data: {reply:?}"
    );
}

// ── attack 1: undeclared key, read ────────────────────────────────────────

#[test]
fn script_cannot_read_undeclared_out_of_pattern_key() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut c = restricted(m.port);
    let reply = c.cmd(&["EVAL", "return redis.call('GET', 'secret:x')", "0"]);
    assert_denied("EVAL undeclared GET", &reply);
}

// ── attack 2: undeclared key, write ───────────────────────────────────────

#[test]
fn script_cannot_write_undeclared_out_of_pattern_key() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut c = restricted(m.port);
    let reply = c.cmd(&["EVAL", "return redis.call('SET', 'secret:w', 'v')", "0"]);
    assert_denied("EVAL undeclared SET", &reply);
    // And the write must not have landed.
    let mut admin = Resp::connect(m.port);
    let probe = admin.cmd(&["EXISTS", "secret:w"]);
    assert!(
        probe.starts_with(":0"),
        "denied script still wrote secret:w: {probe:?}"
    );
}

// ── attack 3: key name built at runtime by concatenation ──────────────────

#[test]
fn script_cannot_reach_key_built_by_concatenation() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut c = restricted(m.port);
    let reply = c.cmd(&[
        "EVAL",
        "local p = 'sec' .. 'ret:' .. ARGV[1] return redis.call('GET', p)",
        "0",
        "x",
    ]);
    assert_denied("EVAL concatenated key", &reply);
}

// ── attack 4: redis.call inside a Lua closure, behind a nested pcall ──────

#[test]
fn script_cannot_launder_through_closure_and_pcall() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut c = restricted(m.port);
    // A denied call raised inside pcall is caught by the script, so the reply
    // is a value, not an error — assert on the VALUE not containing the data.
    let reply = c.cmd(&[
        "EVAL",
        "local f = function() return redis.call('GET', 'secret:x') end \
         local ok, err = pcall(f) \
         if ok then return err end \
         return 'denied'",
        "0",
    ]);
    assert!(
        !reply.contains("leaked"),
        "closure+pcall laundering leaked data: {reply:?}"
    );
    // redis.pcall must likewise refuse to execute the command.
    let reply = c.cmd(&[
        "EVAL",
        "local r = redis.pcall('GET', 'secret:x') \
         if type(r) == 'table' and r.err then return 'denied' end \
         return r",
        "0",
    ]);
    assert!(
        !reply.contains("leaked"),
        "redis.pcall laundering leaked data: {reply:?}"
    );
    assert!(
        reply.contains("denied"),
        "redis.pcall should have returned an error table: {reply:?}"
    );
}

// ── attack 5: movable-key commands (keys not at a fixed position) ─────────

#[test]
fn script_cannot_reach_movable_key_commands() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut c = restricted(m.port);

    // LMPOP numkeys k [k ...] LEFT — key vector counted by numkeys.
    let reply = c.cmd(&[
        "EVAL",
        "return redis.call('LMPOP', '1', 'secret:list', 'LEFT')",
        "0",
    ]);
    assert_denied("EVAL LMPOP", &reply);

    // SORT ... STORE dst — the destination is a positional clause.
    let reply = c.cmd(&[
        "EVAL",
        "return redis.call('SORT', 'app:list', 'ALPHA', 'STORE', 'secret:sorted')",
        "0",
    ]);
    assert_denied("EVAL SORT STORE", &reply);

    // SORT ... BY pattern — the weight keys are computed at runtime and
    // therefore unnameable: the walker reports them as computed and the
    // policy must FAIL CLOSED.
    let reply = c.cmd(&[
        "EVAL",
        "return redis.call('SORT', 'app:list', 'BY', 'secret:w_*')",
        "0",
    ]);
    assert_denied("EVAL SORT BY", &reply);

    // COPY src dst — the destination used to be unchecked entirely.
    let reply = c.cmd(&[
        "EVAL",
        "return redis.call('COPY', 'app:ok', 'secret:copy')",
        "0",
    ]);
    assert_denied("EVAL COPY dst", &reply);
}

// ── attack 6: declare an allowed key, then touch a denied one ─────────────

#[test]
fn declaring_an_allowed_key_does_not_launder_the_rest() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut c = restricted(m.port);
    let reply = c.cmd(&[
        "EVAL",
        "redis.call('GET', KEYS[1]) return redis.call('GET', 'secret:x')",
        "1",
        "app:ok",
    ]);
    assert_denied("EVAL allowed-then-denied", &reply);
}

// ── attack 7: FCALL, not just EVAL ────────────────────────────────────────

#[test]
fn function_calls_cannot_reach_out_of_pattern_keys() {
    let m = spawn_moon("1");
    seed(m.port);
    // The function registry is per-CONNECTION today, so load and call on the
    // same connection — as the restricted user, who is allowed `+function`.
    let mut c = restricted(m.port);
    let lib = "#!lua name=leaklib\n\
               redis.register_function('leak', function(keys, args) \
                 return redis.call('GET', 'secret:x') end)";
    let load = c.cmd(&["FUNCTION", "LOAD", lib]);
    assert!(!load.starts_with('-'), "FUNCTION LOAD failed: {load:?}");
    let reply = c.cmd(&["FCALL", "leak", "0"]);
    assert_denied("FCALL undeclared GET", &reply);

    // FCALL_RO too: read-only does not mean permission-free.
    let load = c.cmd(&[
        "FUNCTION",
        "LOAD",
        "REPLACE",
        "#!lua name=leaklib\nredis.register_function('leak_ro',          function(keys, args) return redis.call('GET', 'secret:y') end)",
    ]);
    assert!(!load.starts_with('-'), "FUNCTION LOAD REPLACE: {load:?}");
    let reply = c.cmd(&["FCALL_RO", "leak_ro", "0"]);
    assert_denied("FCALL_RO undeclared GET", &reply);
}

// ── attack 8: command-level ACL is enforced too, not just keys ────────────

#[test]
fn script_cannot_run_a_command_the_user_is_denied() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut admin = Resp::connect(m.port);
    // `nodel` may touch every key but must never DEL.
    let r = admin.cmd(&[
        "ACL", "SETUSER", "nodel", "on", ">pw", "~*", "+@all", "-del",
    ]);
    assert!(r.starts_with("+OK"), "ACL SETUSER nodel: {r:?}");
    let mut c = Resp::connect(m.port);
    assert!(c.cmd(&["AUTH", "nodel", "pw"]).starts_with("+OK"));
    assert!(
        c.cmd(&["DEL", "app:ok"]).starts_with("-NOPERM"),
        "direct DEL should already be denied"
    );
    let reply = c.cmd(&["EVAL", "return redis.call('DEL', 'app:ok')", "0"]);
    assert_denied("EVAL DEL via script", &reply);
    let probe = admin.cmd(&["EXISTS", "app:ok"]);
    assert!(
        probe.starts_with(":1"),
        "denied script still deleted app:ok: {probe:?}"
    );
}

// ── attack 9: multi-shard — the script routes to the key owner's shard ────

#[test]
fn cross_shard_routed_script_still_enforces_acl() {
    // With >1 shard a script whose declared keys live on another shard is
    // shipped over SPSC and executed there. The caller's identity must travel
    // with it; without that the routed copy runs unauthenticated.
    let m = spawn_moon("4");
    seed(m.port);
    let mut admin = Resp::connect(m.port);
    // Hash tags force co-location: `app:{t<i>}` and `secret:{t<i>}` land on
    // the SAME shard, so a script that declares the allowed key is routed to
    // the shard that also holds the denied one — the exact case where the
    // caller's identity has to survive the SPSC hop. Sweeping 16 tags from 8
    // connections covers every (origin shard, target shard) pair, including
    // the ones where origin != target.
    for i in 0..16 {
        assert!(
            admin
                .cmd(&["SET", &format!("secret:{{t{i}}}"), "leaked"])
                .starts_with("+OK")
        );
        assert!(
            admin
                .cmd(&["SET", &format!("app:{{t{i}}}"), "public"])
                .starts_with("+OK")
        );
    }
    let mut leaks = Vec::new();
    for _ in 0..8 {
        let mut conn = restricted(m.port);
        for i in 0..16 {
            let script =
                format!("redis.call('GET', KEYS[1]) return redis.call('GET', 'secret:{{t{i}}}')");
            let key = format!("app:{{t{i}}}");
            let reply = conn.cmd(&["EVAL", &script, "1", &key]);
            if reply.contains("leaked") {
                leaks.push((key, reply));
            }
        }
    }
    assert!(
        leaks.is_empty(),
        "routed script leaked on {} probes: {:?}",
        leaks.len(),
        &leaks[..leaks.len().min(3)]
    );
    // ...and a legitimate routed script still works.
    let mut c = restricted(m.port);
    let mut ok_count = 0;
    for i in 0..16 {
        let key = format!("app:{{t{i}}}");
        let ok = c.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", &key]);
        assert!(
            ok.contains("public"),
            "legitimate routed script broke on {key}: {ok:?}"
        );
        ok_count += 1;
    }
    assert_eq!(ok_count, 16);
}

// ── the fix must not break legitimate scripts ─────────────────────────────

#[test]
fn in_pattern_scripts_still_work_for_restricted_users() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut c = restricted(m.port);

    let r = c.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "app:ok"]);
    assert!(r.contains("public"), "declared in-pattern GET broke: {r:?}");

    // Undeclared but in-pattern is fine: the pattern is what gates, not the
    // declaration.
    let r = c.cmd(&["EVAL", "return redis.call('GET', 'app:ok')", "0"]);
    assert!(
        r.contains("public"),
        "undeclared in-pattern GET broke: {r:?}"
    );

    let r = c.cmd(&[
        "EVAL",
        "redis.call('SET', 'app:new', 'v') return redis.call('GET', 'app:new')",
        "0",
    ]);
    assert!(r.contains("v"), "in-pattern SET/GET broke: {r:?}");

    // Keyless commands inside a script are unaffected.
    let r = c.cmd(&["EVAL", "return redis.call('PING')", "0"]);
    assert!(r.contains("PONG"), "in-script PING broke: {r:?}");
}

#[test]
fn unrestricted_users_are_unaffected() {
    let m = spawn_moon("1");
    seed(m.port);
    let mut admin = Resp::connect(m.port);
    let r = admin.cmd(&["EVAL", "return redis.call('GET', 'secret:x')", "0"]);
    assert!(
        r.contains("leaked"),
        "unrestricted default user must still reach any key: {r:?}"
    );
    let r = admin.cmd(&[
        "EVAL",
        "return redis.call('SORT', 'app:list', 'BY', 'secret:w_*')",
        "0",
    ]);
    assert!(
        !r.starts_with('-') || !r.contains("NOPERM"),
        "unrestricted user hit a NOPERM: {r:?}"
    );
}
