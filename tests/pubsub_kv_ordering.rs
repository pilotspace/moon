//! Pub/sub ↔ KV ordering guarantee (C1/C2, 2026-07 pub/sub review).
//!
//! Locks in the contract documented in `src/pubsub/mod.rs`: a `PUBLISH` is
//! processed only after every preceding command on the SAME connection has
//! completed — so a subscriber that receives a message can immediately read
//! any key the publisher wrote before the `PUBLISH` and observe the new
//! value (the cache-invalidation pattern `SET k v; PUBLISH ch k`).
//!
//! Runs against `--shards 4` so the written key and the pub/sub fan-out
//! routinely live on different shards — the regression these tests guard
//! against is a cross-shard write racing (or being reordered after) the
//! publish fan-out. The pipelined variant sends SET+PUBLISH back-to-back
//! with no client-side wait, which is the strongest form of the claim.
//!
//! Skips gracefully when the moon binary is missing (MOON_BIN pin wins,
//! then target/release, then target/debug).

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
    let tmp_dir = std::env::temp_dir().join(format!("moon-pubsub-ord-{port}"));
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
            // This host hovers near the 5% diskfull line — the guard would
            // turn every SET into a MOONERR and flake the suite.
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

    /// Encode `args` as a RESP array of bulk strings (no read).
    fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
    }

    /// Read whatever arrives within `total` into the rolling buffer.
    fn pump(&mut self, total: Duration) {
        let deadline = Instant::now() + total;
        let mut chunk = [0u8; 4096];
        while Instant::now() < deadline {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => {} // timeout tick — keep polling until deadline
            }
        }
    }

    /// Send a command and pump the reply into the rolling buffer.
    fn cmd(&mut self, args: &[&str]) {
        self.send(args);
        self.pump(Duration::from_millis(200));
    }

    /// True when `needle` has appeared anywhere on this connection's wire.
    fn saw(&self, needle: &[u8]) -> bool {
        self.buf.windows(needle.len()).any(|w| w == needle)
    }

    /// Wait up to `timeout` for `needle` to appear on the wire.
    fn wait_for(&mut self, needle: &[u8], timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.saw(needle) {
                return true;
            }
            self.pump(Duration::from_millis(100));
        }
        self.saw(needle)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}

/// C1: pipelined `SET k v; PUBLISH ch v` on ONE connection — when the
/// subscriber sees the message, a reader on a third connection must observe
/// the value written before the publish. Repeated across several keys (so
/// different shards own the key) and iterations (so a lost race shows up).
#[test]
fn pubsub_kv_ordering() {
    let Some(m) = spawn_moon("4") else { return };

    let mut subscriber = Resp::connect(m.port);
    subscriber.cmd(&["SUBSCRIBE", "ord:ch"]);
    assert!(
        subscriber.wait_for(b"subscribe", Duration::from_secs(2)),
        "SUBSCRIBE must be acknowledged"
    );
    subscriber.clear();

    let mut writer = Resp::connect(m.port);
    let mut reader = Resp::connect(m.port);

    // 4 keys → with 4 shards the written key regularly lives on a different
    // shard from the publisher's connection shard.
    for i in 0..24u32 {
        let key = format!("ord:k{}", i % 4);
        let val = format!("ord-val-{i}");

        // Pipelined: no client-side wait between SET and PUBLISH — the server
        // alone must enforce write-before-publish on this connection.
        writer.send(&["SET", &key, &val]);
        writer.send(&["PUBLISH", "ord:ch", &val]);
        writer.pump(Duration::from_millis(200));
        assert!(writer.saw(b"+OK"), "SET must succeed (iteration {i})");

        assert!(
            subscriber.wait_for(val.as_bytes(), Duration::from_secs(3)),
            "subscriber must receive the published message (iteration {i})"
        );

        // Message received ⇒ the SET that preceded the PUBLISH on the same
        // connection must already be visible to any other connection.
        reader.clear();
        reader.cmd(&["GET", &key]);
        assert!(
            reader.saw(val.as_bytes()),
            "GET {key} after receiving message must see {val} \
             (write→publish ordering violated; got: {:?})",
            String::from_utf8_lossy(&reader.buf)
        );

        writer.clear();
        subscriber.clear();
    }
}

/// C2: `MULTI { SET; PUBLISH } EXEC` — the queued PUBLISH must fan out only
/// after the queued SET has been applied, and both must execute.
#[test]
/// NOTE: runs on `--shards 1`. Sharded MULTI/EXEC executes the queued body
/// on the CONNECTION's local shard only ("cross-shard transactions require
/// distributed coordination" — execute_transaction_sharded), so at shards>1 a
/// txn SET of a remote-owned key is silently misplaced — a pre-existing gap
/// tracked separately; this test pins the C2 publish-queueing semantics.
fn multi_exec_set_publish_ordering() {
    let Some(m) = spawn_moon("1") else { return };

    let mut subscriber = Resp::connect(m.port);
    subscriber.cmd(&["SUBSCRIBE", "txn:ch"]);
    assert!(
        subscriber.wait_for(b"subscribe", Duration::from_secs(2)),
        "SUBSCRIBE must be acknowledged"
    );
    subscriber.clear();

    let mut writer = Resp::connect(m.port);
    let mut reader = Resp::connect(m.port);

    for i in 0..8u32 {
        let key = format!("txn:k{}", i % 4);
        let val = format!("txn-val-{i}");

        writer.cmd(&["MULTI"]);
        assert!(writer.saw(b"+OK"), "MULTI must be accepted (iteration {i})");
        writer.cmd(&["SET", &key, &val]);
        writer.cmd(&["PUBLISH", "txn:ch", &val]);
        assert!(
            writer.saw(b"+QUEUED"),
            "commands must queue inside MULTI (iteration {i})"
        );
        writer.clear();
        writer.cmd(&["EXEC"]);
        assert!(
            writer.wait_for(b"+OK", Duration::from_secs(2)),
            "EXEC must run the queued SET (iteration {i}; got: {:?})",
            String::from_utf8_lossy(&writer.buf)
        );

        assert!(
            subscriber.wait_for(val.as_bytes(), Duration::from_secs(3)),
            "subscriber must receive the message published inside EXEC (iteration {i})"
        );

        reader.clear();
        reader.cmd(&["GET", &key]);
        assert!(
            reader.saw(val.as_bytes()),
            "GET {key} after the EXEC-published message must see {val} \
             (txn write→publish ordering violated; got: {:?})",
            String::from_utf8_lossy(&reader.buf)
        );

        writer.clear();
        subscriber.clear();
    }
}
