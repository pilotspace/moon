//! Channel-ACL enforcement on the PUBLISH path, including inside MULTI/EXEC
//! (2026-07 pub/sub review, C2 security follow-up).
//!
//! Regression guard: a client restricted to a channel pattern must not be able
//! to wrap `PUBLISH` in `MULTI/EXEC` to reach a denied channel. The immediate
//! PUBLISH path already returned `NOPERM`; the transactional path used to skip
//! the channel check entirely and fan the message out, so `MULTI; PUBLISH
//! denied m; EXEC` bypassed the ACL. Both paths must now reject.
//!
//! Runs at `--shards 1` (monoio TopLevel handler) and `--shards 4` (sharded
//! handler) so both EXEC fan-out sites are covered. Skips gracefully when the
//! moon binary is missing (MOON_BIN pin wins, then target/release, then
//! target/debug).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().unwrap().port()
}

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for rel in ["target/release/moon", "target/debug/moon"] {
        let p = root.join(rel);
        if p.exists() {
            return Some(p);
        }
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
    let port = free_port();
    let tmp_dir = std::env::temp_dir().join(format!("moon-pubsub-acl-{port}"));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let child = Command::new(&bin)
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
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let moon = Moon {
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
                if let Ok(n) = c.read(&mut buf) {
                    if n > 0 && buf.starts_with(b"+PONG") {
                        return Some(moon);
                    }
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
            .unwrap();
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

    fn cmd(&mut self, args: &[&str]) {
        self.send(args);
        self.pump(Duration::from_millis(200));
    }

    fn saw(&self, needle: &[u8]) -> bool {
        self.buf.windows(needle.len()).any(|w| w == needle)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}

/// A user restricted to `allowed:*` channels must be denied `PUBLISH` on any
/// other channel, whether issued immediately or wrapped in MULTI/EXEC.
fn run_channel_acl(shards: &str) {
    let Some(moon) = spawn_moon(shards) else {
        return; // binary missing — skip
    };

    // Admin (default user) provisions the restricted user.
    let mut admin = Resp::connect(moon.port);
    admin.cmd(&[
        "ACL",
        "SETUSER",
        "alice",
        "on",
        ">secret",
        "resetchannels",
        "&allowed:*",
        "+@all",
        "~*",
    ]);
    assert!(
        admin.saw(b"+OK"),
        "[shards={shards}] ACL SETUSER should succeed, wire: {:?}",
        String::from_utf8_lossy(&admin.buf)
    );

    let mut c = Resp::connect(moon.port);
    c.cmd(&["AUTH", "alice", "secret"]);
    assert!(
        c.saw(b"+OK"),
        "[shards={shards}] AUTH alice should succeed, wire: {:?}",
        String::from_utf8_lossy(&c.buf)
    );

    // 1) Immediate PUBLISH to a denied channel -> NOPERM.
    c.clear();
    c.cmd(&["PUBLISH", "denied:x", "hi"]);
    assert!(
        c.saw(b"NOPERM"),
        "[shards={shards}] immediate PUBLISH to denied channel must be NOPERM, wire: {:?}",
        String::from_utf8_lossy(&c.buf)
    );

    // 2) Immediate PUBLISH to an allowed channel -> integer reply, no NOPERM.
    c.clear();
    c.cmd(&["PUBLISH", "allowed:x", "hi"]);
    assert!(
        !c.saw(b"NOPERM"),
        "[shards={shards}] immediate PUBLISH to allowed channel must be permitted, wire: {:?}",
        String::from_utf8_lossy(&c.buf)
    );

    // 3) SECURITY REGRESSION: PUBLISH to a denied channel INSIDE MULTI/EXEC must
    //    also be NOPERM — the txn path must not be a bypass.
    c.clear();
    c.cmd(&["MULTI"]);
    c.cmd(&["PUBLISH", "denied:y", "hi"]); // +QUEUED
    c.cmd(&["EXEC"]);
    assert!(
        c.saw(b"NOPERM"),
        "[shards={shards}] PUBLISH to denied channel inside MULTI/EXEC must be NOPERM \
         (channel ACL bypass), wire: {:?}",
        String::from_utf8_lossy(&c.buf)
    );

    // 4) PUBLISH to an allowed channel inside MULTI/EXEC still works.
    c.clear();
    c.cmd(&["MULTI"]);
    c.cmd(&["PUBLISH", "allowed:y", "hi"]);
    c.cmd(&["EXEC"]);
    assert!(
        !c.saw(b"NOPERM"),
        "[shards={shards}] PUBLISH to allowed channel inside MULTI/EXEC must be permitted, \
         wire: {:?}",
        String::from_utf8_lossy(&c.buf)
    );
}

#[test]
fn channel_acl_enforced_single_shard() {
    run_channel_acl("1");
}

#[test]
fn channel_acl_enforced_multi_shard() {
    run_channel_acl("4");
}
