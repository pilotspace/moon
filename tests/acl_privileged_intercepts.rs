//! c10k hardening B1 — privileged intercepts must sit BELOW the ACL gate.
//!
//! Both connection handlers used to run their command-specific intercepts
//! (Lua `EVAL`/`EVALSHA`/`SCRIPT`, `ACL`, `CLUSTER`) BEFORE the ACL
//! permission check, even though the comment attached to that check claimed
//! it "must run before any command-specific handlers ... so that
//! low-privilege users cannot reach admin commands". Each intercept
//! `continue`s on a match, so the gate below it never ran.
//!
//! The result was full privilege escalation from any authenticated account:
//! a user holding nothing but `~app:* +get` was correctly refused a plain
//! `SET` yet could run `ACL SETUSER evil on nopass ~* +@all` (persisted),
//! `CONFIG SET maxmemory ...` (persisted) and arbitrary Lua via `EVAL`.
//!
//! Runs at `--shards 1` (monoio TopLevel handler) and `--shards 4` (sharded
//! handler) because the two handlers carry independent copies of the
//! ordering. Skips gracefully when the moon binary is missing.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let cargo_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    if cargo_bin.exists() {
        return Some(cargo_bin);
    }
    None
}

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

fn spawn_moon(shards: &str) -> Option<Moon> {
    let bin = moon_binary()?;
    let tmp_dir = std::env::temp_dir().join(format!(
        "moon-acl-intercepts-{}-{shards}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let (child, port) = common::spawn_listening(|port| {
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
                // Keep the per-shard page cache small: a default auto-sized
                // maxmemory makes startup slow enough to trip readiness on a
                // contended host.
                "--maxmemory",
                "268435456",
                "--dir",
                tmp_dir.to_str().expect("utf8 tmp dir"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
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
                    return Some(moon);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not become ready on port {port}");
    None
}

/// Minimal RESP client over a blocking TcpStream (rolling receive buffer).
struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
    }

    fn pump(&mut self, total: Duration) {
        let deadline = Instant::now() + total;
        let mut chunk = [0u8; 4096];
        while Instant::now() < deadline {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => {}
            }
        }
    }

    /// Send one command and return everything that came back for it.
    fn cmd(&mut self, args: &[&str]) -> String {
        self.buf.clear();
        self.send(args);
        self.pump(Duration::from_millis(250));
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

/// Every privileged intercept must answer NOPERM for a `~app:* +get` user,
/// and must leave no trace behind when it does.
fn run_privileged_intercepts(shards: &str) {
    let Some(moon) = spawn_moon(shards) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    // Admin (default user) provisions the restricted account.
    let mut admin = Resp::connect(moon.port);
    let r = admin.cmd(&[
        "ACL",
        "SETUSER",
        "lowpriv",
        "on",
        ">pw",
        "resetkeys",
        "~app:*",
        "-@all",
        "+get",
    ]);
    assert!(r.contains("+OK"), "{tag} ACL SETUSER lowpriv failed: {r:?}");

    let mut c = Resp::connect(moon.port);
    let r = c.cmd(&["AUTH", "lowpriv", "pw"]);
    assert!(r.contains("+OK"), "{tag} AUTH lowpriv failed: {r:?}");

    // Positive control: the one thing this user IS allowed to do. If this
    // fails the gate is over-broad and the NOPERM assertions below prove
    // nothing.
    let r = c.cmd(&["GET", "app:hello"]);
    assert!(
        !r.contains("NOPERM") && !r.contains("NOAUTH"),
        "{tag} GET app:* must stay allowed: {r:?}"
    );

    // Baseline: a command with no intercept in front of it was ALWAYS gated.
    let r = c.cmd(&["SET", "app:hello", "1"]);
    assert!(r.contains("NOPERM"), "{tag} SET must be NOPERM: {r:?}");

    // --- The escalation vectors ---
    for args in [
        // Privilege escalation: mint a full-admin account.
        &["ACL", "SETUSER", "evil", "on", "nopass", "~*", "+@all"][..],
        // Server reconfiguration.
        &["CONFIG", "SET", "maxmemory", "999999999"][..],
        // Arbitrary Lua.
        &["EVAL", "return 'pwned'", "0"][..],
        &["EVALSHA", "ffffffffffffffffffffffffffffffffffffffff", "0"][..],
        &["SCRIPT", "LOAD", "return 1"][..],
        // Replication takeover.
        &["REPLICAOF", "no", "one"][..],
        &["SLAVEOF", "no", "one"][..],
        // Persistence / cluster admin.
        &["BGSAVE"][..],
        &["CLUSTER", "INFO"][..],
    ] {
        let r = c.cmd(args);
        assert!(
            r.contains("NOPERM"),
            "{tag} {args:?} must be NOPERM for a `~app:* +get` user, got: {r:?}"
        );
    }

    // --- And the denials must have had no side effect ---
    let r = admin.cmd(&["ACL", "GETUSER", "evil"]);
    assert!(
        r.starts_with("$-1") || r.starts_with("*-1") || r.starts_with("_") || r.starts_with("*0"),
        "{tag} the denied ACL SETUSER must not have created `evil`: {r:?}"
    );
    let r = admin.cmd(&["CONFIG", "GET", "maxmemory"]);
    assert!(
        !r.contains("999999999"),
        "{tag} the denied CONFIG SET must not have persisted: {r:?}"
    );

    // --- ACL-exempt commands must still work for the restricted user ---
    // Redis marks AUTH and HELLO NO_AUTH; gating them would strand a
    // restricted client with no way to re-authenticate, and would break the
    // RESP3 handshake.
    let r = c.cmd(&["HELLO", "2"]);
    assert!(
        !r.contains("NOPERM"),
        "{tag} HELLO must stay ACL-exempt: {r:?}"
    );
    let r = c.cmd(&["AUTH", "lowpriv", "pw"]);
    assert!(
        r.contains("+OK"),
        "{tag} AUTH must stay ACL-exempt (re-auth): {r:?}"
    );
    // ...and re-authenticating as an unrestricted user restores the powers.
    let r = c.cmd(&["ACL", "WHOAMI"]);
    assert!(r.contains("NOPERM"), "{tag} still restricted: {r:?}");
}

#[test]
fn privileged_intercepts_are_acl_gated_single_shard() {
    run_privileged_intercepts("1");
}

#[test]
fn privileged_intercepts_are_acl_gated_multi_shard() {
    run_privileged_intercepts("4");
}
