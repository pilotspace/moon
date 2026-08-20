//! `CLIENT TRACKING` invalidation must cover movablekeys commands (#582).
//!
//! A movablekeys command is one whose keys are not at a fixed argument
//! position; those carry `first_key: 0` in `COMMAND_META`, mirroring redis's
//! own table. `tracking::invalidation::command_keys` read that as "this
//! command has no keys" and returned an empty list, so:
//!
//!   * a movablekeys **read** (`SINTERCARD`, `ZDIFF`, `XREAD`, ...) never
//!     registered the client, which then cached a value it would never be told
//!     about; and
//!   * a movablekeys **write** (`LMPOP`, `ZMPOP`, ...) never pushed an
//!     `invalidate`, so every tracker's copy went stale **permanently**.
//!
//! Client-side caching is a correctness contract — the client may serve its
//! cached value until told otherwise — so a missed invalidation is unbounded
//! wrong data, and it is invisible to the client. That is why this is pinned
//! on the wire and not only as a unit test of the extractor.
//!
//! Every case below was measured against `redis-server 8.0.5`, which fires an
//! invalidation for all of them.
//!
//! Each case runs beside a **control** that differs only in using a
//! fixed-position command. The controls are not decoration: without them, a
//! harness that never delivers pushes at all would make every assertion pass
//! after an inverted fix, or fail for reasons unrelated to key extraction.

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-track-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-track-{port}"));
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

struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut c = Conn(s);
        c.call(&["HELLO", "3"]);
        c
    }

    /// Send a command and drain until the peer is quiet for `wait`.
    fn drain(&mut self, wait: Duration) -> Vec<u8> {
        self.0.set_read_timeout(Some(wait)).unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 16384];
        loop {
            match self.0.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        buf
    }

    fn call(&mut self, args: &[&str]) -> Vec<u8> {
        self.0.write_all(&encode(args)).expect("write");
        self.drain(Duration::from_millis(350))
    }

    /// Wait for an `invalidate` push naming `key`, up to `budget`.
    fn awaits_invalidate(&mut self, key: &str, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            seen.extend_from_slice(&self.drain(Duration::from_millis(250)));
            let text = String::from_utf8_lossy(&seen);
            if text.contains("invalidate") && text.contains(key) {
                return true;
            }
        }
        false
    }
}

fn tracking_client(port: u16) -> Conn {
    let mut c = Conn::open(port);
    let reply = c.call(&["CLIENT", "TRACKING", "ON"]);
    assert!(
        String::from_utf8_lossy(&reply).contains("OK"),
        "CLIENT TRACKING ON failed: {:?}",
        String::from_utf8_lossy(&reply)
    );
    c
}

/// One tracked-read / foreign-write pair: does the tracking client get told?
fn invalidation_reaches_tracker(
    port: u16,
    setup: &[&[&str]],
    tracked_read: &[&str],
    foreign_write: &[&str],
    key: &str,
) -> bool {
    let mut w = Conn::open(port);
    for c in setup {
        w.call(c);
    }

    let mut tracker = tracking_client(port);
    tracker.call(tracked_read);

    let mut other = Conn::open(port);
    other.call(foreign_write);

    tracker.awaits_invalidate(key, Duration::from_secs(3))
}

/// A movablekeys READ must register the client, and a movablekeys WRITE must
/// push. Controls in the same shape prove the harness delivers pushes at all.
#[test]
fn movablekeys_reads_and_writes_reach_tracking_clients() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        let p = moon.port;

        // ── controls: fixed-position commands, known-good before #582 ─────
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "cs"], &["SADD", "cs", "x"]],
                &["SMEMBERS", "cs"],
                &["SADD", "cs", "y"],
                "cs",
            ),
            "CONTROL SMEMBERS/SADD did not deliver at shards={shards} — the harness is \
             broken, so the assertions below would be meaningless"
        );
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "cl"], &["RPUSH", "cl", "a", "b"]],
                &["LRANGE", "cl", "0", "-1"],
                &["LPOP", "cl"],
                "cl",
            ),
            "CONTROL LRANGE/LPOP did not deliver at shards={shards}"
        );

        // ── READ side: a movablekeys read must register the client ────────
        assert!(
            invalidation_reaches_tracker(
                p,
                &[
                    &["DEL", "s1", "s2"],
                    &["SADD", "s1", "x"],
                    &["SADD", "s2", "x"]
                ],
                &["SINTERCARD", "2", "s1", "s2"],
                &["SADD", "s1", "y"],
                "s1",
            ),
            "SINTERCARD read did not register the tracking client (shards={shards})"
        );
        assert!(
            invalidation_reaches_tracker(
                p,
                &[
                    &["DEL", "z1", "z2"],
                    &["ZADD", "z1", "1", "a"],
                    &["ZADD", "z2", "1", "b"]
                ],
                &["ZDIFF", "2", "z1", "z2"],
                &["ZADD", "z1", "2", "c"],
                "z1",
            ),
            "ZDIFF read did not register the tracking client (shards={shards})"
        );

        // ── WRITE side: a movablekeys write must push an invalidation ─────
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "ml"], &["RPUSH", "ml", "a", "b", "c"]],
                &["LRANGE", "ml", "0", "-1"],
                &["LMPOP", "1", "ml", "LEFT"],
                "ml",
            ),
            "LMPOP write did not invalidate the key it popped (shards={shards})"
        );
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "mz"], &["ZADD", "mz", "1", "a", "2", "b"]],
                &["ZRANGE", "mz", "0", "-1"],
                &["ZMPOP", "1", "mz", "MIN"],
                "mz",
            ),
            "ZMPOP write did not invalidate the key it popped (shards={shards})"
        );
    }
}

/// `SORT src STORE dst` WRITES `dst`, but its registry spec names `src`
/// (`first_key = 1`). Moon invalidated the key it had not written and missed
/// the one it had — so a tracker of `dst` was never told, which is the shape
/// this test pins.
#[test]
fn sort_store_invalidates_the_destination_it_writes() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert!(
            invalidation_reaches_tracker(
                moon.port,
                &[
                    &["DEL", "sortsrc", "sortdst"],
                    &["RPUSH", "sortsrc", "b", "a"],
                    &["RPUSH", "sortdst", "stale"],
                ],
                &["LRANGE", "sortdst", "0", "-1"],
                &["SORT", "sortsrc", "ALPHA", "STORE", "sortdst"],
                "sortdst",
            ),
            "SORT ... STORE did not invalidate its DESTINATION (shards={shards})"
        );
    }
}
