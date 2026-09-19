//! moon#1043 — `SPUBLISH` queued inside `MULTI` must be delivered at `EXEC`.
//!
//! Red on origin/main `52c3936c`, at `--shards 1` and `--shards 4`:
//!
//! ```text
//! MULTI / SET {x}k 1 / SPUBLISH sch hello / EXEC
//! redis 8.6.1: *2 +OK :3        (3 SSUBSCRIBE subscribers receive it)
//! moon:        *2 +OK -ERR unknown command 'SPUBLISH' ...   (0/3 receive it)
//! ```
//!
//! The EXEC executors intercepted `PUBLISH` for a deferred post-body fan-out
//! (C2) but not `SPUBLISH`, which fell through to `dispatch()` — no pub/sub
//! arm — while the rest of the body committed. `PUBLISH` inside `MULTI` was
//! already correct and is kept here as the namespace-isolation control.
//!
//! Subscribers are separate connections, so at `--shards 4` they spread over
//! shards and the fan-out must cross the mesh. Eight distinct `{tag}` bodies
//! make both EXEC shapes run: a body whose key is local to the connection's
//! shard, and one owner-routed to another shard (moon#247), whose deferred
//! publish list travels back in `TxnExecReply`.

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

fn moon_binary() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn spawn_moon(shards: &str) -> Moon {
    let bin = moon_binary();
    let dir_for = |port: u16| std::env::temp_dir().join(format!("moon-spub1043-{port}"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = dir_for(port);
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
            .stderr(std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr"))
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir: dir_for(port),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let mut c = Resp::connect(moon.port);
        c.cmd(&["PING"]);
        if c.saw(b"+PONG") {
            return moon;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon (--shards {shards}) never answered PING\n--- stderr ---\n{log}");
}

/// Minimal RESP client over a blocking TcpStream.
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
        self.pump(Duration::from_millis(150));
    }

    /// Send, then read until `needle` arrives (or `timeout`) — for replies
    /// whose arrival is the thing under test, instead of a fixed pump.
    fn cmd_until(&mut self, args: &[&str], needle: &[u8], timeout: Duration) {
        self.send(args);
        self.wait_for(needle, timeout);
    }

    fn saw(&self, needle: &[u8]) -> bool {
        self.buf.windows(needle.len()).any(|w| w == needle)
    }

    fn wait_for(&mut self, needle: &[u8], timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while !self.saw(needle) && Instant::now() < deadline {
            self.pump(Duration::from_millis(100));
        }
        self.saw(needle)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }

    fn wire(&self) -> String {
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

/// A fresh connection subscribed with `verb` (`SSUBSCRIBE`/`SUBSCRIBE`).
fn subscriber(port: u16, verb: &str, channel: &str) -> Resp {
    let mut s = Resp::connect(port);
    s.send(&[verb, channel]);
    assert!(
        s.wait_for(channel.as_bytes(), Duration::from_secs(3)),
        "{verb} {channel} must be acknowledged, wire: {:?}",
        s.wire()
    );
    s.clear();
    s
}

const SUBS: usize = 6;

fn spublish_in_multi_is_delivered(shards: &str) {
    let m = spawn_moon(shards);
    let mut subs: Vec<Resp> = (0..SUBS)
        .map(|_| subscriber(m.port, "SSUBSCRIBE", "sch1043"))
        .collect();
    let mut p = Resp::connect(m.port);
    let expected_reply = format!("*2\r\n+OK\r\n:{SUBS}\r\n");
    for tag in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        let key = format!("{{{tag}}}k");
        let msg = format!("m1043-{tag}");
        p.clear();
        p.cmd(&["MULTI"]);
        p.cmd(&["SET", &key, "v"]);
        p.cmd(&["SPUBLISH", "sch1043", &msg]);
        p.cmd_until(&["EXEC"], expected_reply.as_bytes(), Duration::from_secs(3));
        assert!(
            p.saw(expected_reply.as_bytes()),
            "--shards {shards}, body {{{tag}}}: EXEC must answer the SET and the \
             SPUBLISH receiver count like redis ({expected_reply:?}), wire: {:?}",
            p.wire()
        );
        for (i, s) in subs.iter_mut().enumerate() {
            assert!(
                s.wait_for(msg.as_bytes(), Duration::from_secs(3)),
                "--shards {shards}, body {{{tag}}}: subscriber {i} never received the \
                 SPUBLISH queued in MULTI, wire: {:?}",
                s.wire()
            );
        }
        // The rest of the body committed too.
        let mut g = Resp::connect(m.port);
        g.cmd(&["GET", &key]);
        assert!(g.saw(b"$1\r\nv\r\n"), "SET in the same body must apply");
    }
}

#[test]
fn spublish_in_multi_is_delivered_1_shard() {
    spublish_in_multi_is_delivered("1");
}

#[test]
fn spublish_in_multi_is_delivered_4_shards() {
    spublish_in_multi_is_delivered("4");
}

/// The two namespaces share a channel NAME but never subscribers: a queued
/// SPUBLISH reaches only SSUBSCRIBE, a queued PUBLISH only SUBSCRIBE.
fn namespaces_stay_separate(shards: &str) {
    let m = spawn_moon(shards);
    let mut shard_sub = subscriber(m.port, "SSUBSCRIBE", "iso1043");
    let mut plain_sub = subscriber(m.port, "SUBSCRIBE", "iso1043");
    let mut p = Resp::connect(m.port);
    p.cmd(&["MULTI"]);
    p.cmd(&["SPUBLISH", "iso1043", "via-spublish"]);
    p.cmd(&["PUBLISH", "iso1043", "via-publish"]);
    p.cmd_until(&["EXEC"], b"*2\r\n:1\r\n:1\r\n", Duration::from_secs(3));
    assert!(
        p.saw(b"*2\r\n:1\r\n:1\r\n"),
        "--shards {shards}: each publish counts only its own namespace's one \
         subscriber, wire: {:?}",
        p.wire()
    );
    assert!(shard_sub.wait_for(b"via-spublish", Duration::from_secs(3)));
    assert!(plain_sub.wait_for(b"via-publish", Duration::from_secs(3)));
    // Both deliveries have landed; anything cross-delivered would be here too.
    shard_sub.pump(Duration::from_millis(300));
    plain_sub.pump(Duration::from_millis(300));
    assert!(
        !shard_sub.saw(b"via-publish"),
        "--shards {shards}: a PUBLISH leaked into the shard-channel namespace"
    );
    assert!(
        !plain_sub.saw(b"via-spublish"),
        "--shards {shards}: an SPUBLISH leaked into the global namespace"
    );
}

#[test]
fn namespaces_stay_separate_1_shard() {
    namespaces_stay_separate("1");
}

#[test]
fn namespaces_stay_separate_4_shards() {
    namespaces_stay_separate("4");
}

/// Running SPUBLISH at EXEC must not open a channel-ACL bypass. A denied
/// channel is refused — `NOPERM` in its EXEC slot (the fan-out-time check) or
/// `EXECABORT` (the queue-time check moon#1035 adds) — and never delivered; an
/// allowed one is delivered.
fn channel_acl_holds(shards: &str) {
    let m = spawn_moon(shards);
    let mut admin = Resp::connect(m.port);
    admin.cmd(&[
        "ACL",
        "SETUSER",
        "alice1043",
        "on",
        ">pw",
        "resetchannels",
        "&allowed:*",
        "+@all",
        "~*",
    ]);
    assert!(admin.saw(b"+OK"), "ACL SETUSER: {:?}", admin.wire());
    let mut denied_sub = subscriber(m.port, "SSUBSCRIBE", "denied:1043");
    let mut allowed_sub = subscriber(m.port, "SSUBSCRIBE", "allowed:1043");

    let mut c = Resp::connect(m.port);
    c.cmd(&["AUTH", "alice1043", "pw"]);
    assert!(c.saw(b"+OK"), "AUTH: {:?}", c.wire());

    c.clear();
    c.cmd(&["MULTI"]);
    c.cmd(&["SPUBLISH", "denied:1043", "leak"]);
    c.send(&["EXEC"]);
    let refused = c.wait_for(b"NOPERM", Duration::from_secs(3)) || c.saw(b"EXECABORT");
    assert!(
        refused,
        "--shards {shards}: SPUBLISH to a denied channel inside MULTI must be refused, \
         wire: {:?}",
        c.wire()
    );
    denied_sub.pump(Duration::from_millis(500));
    assert!(
        !denied_sub.saw(b"leak"),
        "--shards {shards}: a denied SPUBLISH was delivered (channel-ACL bypass)"
    );

    c.clear();
    c.cmd(&["MULTI"]);
    c.cmd(&["SPUBLISH", "allowed:1043", "ok-msg"]);
    c.cmd_until(&["EXEC"], b"*1\r\n:1\r\n", Duration::from_secs(3));
    assert!(
        c.saw(b"*1\r\n:1\r\n"),
        "--shards {shards}: allowed SPUBLISH inside MULTI must count its subscriber, \
         wire: {:?}",
        c.wire()
    );
    assert!(allowed_sub.wait_for(b"ok-msg", Duration::from_secs(3)));
}

#[test]
fn channel_acl_holds_1_shard() {
    channel_acl_holds("1");
}

#[test]
fn channel_acl_holds_4_shards() {
    channel_acl_holds("4");
}
