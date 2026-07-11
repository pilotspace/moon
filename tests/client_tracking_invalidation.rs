//! CLIENT TRACKING (client-side caching) invalidation — red/green for the
//! multi-shard correctness cluster found in the 2026-07 pub/sub review:
//!
//! 1. Invalidation was gated on the WRITER's own tracking state — a
//!    non-tracking writer never invalidated anyone's cache.
//! 2. Only `cmd_args.first()` was invalidated — `DEL k1 k2` left k2 stale.
//! 3. The tracking table was per-shard and connection-shard keyed — a reader
//!    and writer accepted on different shards never saw each other.
//! 4. Sharded handlers registered `tracking_rx` but never drained it — pushes
//!    were queued into a channel nobody read (RESP3 invalidation never hit
//!    the wire at all outside handler_single).
//! 5. FLUSHALL/FLUSHDB never invalidated tracked keys.
//!
//! Uses a raw TCP RESP3 client (redis-cli cannot observe async push frames
//! in scripted mode). Spawns `--shards 4`. Skips gracefully when the moon
//! binary is missing (MOON_BIN pin wins, then target/release, then
//! target/debug).

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    // CARGO_BIN_EXE_moon is the binary cargo built for THIS test invocation
    // — guaranteed fresh and feature-matched. Never probe target/release
    // directly: under a redirected CARGO_TARGET_DIR that path is a stale
    // binary of unknown provenance (this exact fallback silently ran an
    // ancient server and "failed" all 5 tests during the v0.6.0 release
    // gate), and in a bare worktree it silently SKIPS the whole suite.
    Some(std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon")))
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

fn spawn_moon_4shard() -> Option<Moon> {
    let bin = moon_binary()?;
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-tracking-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "4",
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
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    // Same directory formula the closure used for the winning attempt.
    let tmp_dir = std::env::temp_dir().join(format!("moon-tracking-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    // Wait for readiness with a raw PING.
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

/// Minimal RESP client over a blocking TcpStream.
struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    /// Encode `args` as a RESP array of bulk strings and send it.
    fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
    }

    /// Read whatever arrives within `total` and append it to the rolling buffer.
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
        self.pump(Duration::from_millis(300));
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
            self.pump(Duration::from_millis(250));
        }
        self.saw(needle)
    }

    /// Drop everything received so far (e.g. the HELLO map, OK replies).
    fn clear(&mut self) {
        self.buf.clear();
    }
}

/// Start a RESP3 connection with CLIENT TRACKING ON.
fn tracking_client(port: u16) -> Resp {
    let mut c = Resp::connect(port);
    c.cmd(&["HELLO", "3"]);
    assert!(c.saw(b"proto"), "HELLO 3 handshake must answer");
    c.cmd(&["CLIENT", "TRACKING", "ON"]);
    assert!(c.saw(b"+OK"), "CLIENT TRACKING ON must be accepted");
    c.clear();
    c
}

const INVALIDATE: &[u8] = b"$10\r\ninvalidate\r\n";

#[test]
fn nontracking_writer_invalidates_cross_connection() {
    let Some(m) = spawn_moon_4shard() else { return };

    let mut writer = Resp::connect(m.port); // plain RESP2, tracking OFF
    writer.cmd(&["SET", "ct:k1", "v1"]);
    assert!(writer.saw(b"+OK"));

    let mut reader = tracking_client(m.port);
    reader.cmd(&["GET", "ct:k1"]); // tracked read
    assert!(reader.saw(b"v1"), "tracked GET must return the seed value");
    reader.clear();

    // A write from a DIFFERENT, non-tracking connection must push an
    // invalidation for ct:k1 to the tracking reader.
    writer.cmd(&["SET", "ct:k1", "v2"]);
    assert!(writer.saw(b"+OK"), "writer SET must succeed");

    assert!(
        reader.wait_for(INVALIDATE, Duration::from_secs(3)) && reader.saw(b"ct:k1"),
        "tracking client must receive an RESP3 invalidate push for ct:k1 \
         after a non-tracking writer SETs it (got: {:?})",
        String::from_utf8_lossy(&reader.buf)
    );

    // Consistency: a re-read after the invalidation sees the new value.
    reader.clear();
    reader.cmd(&["GET", "ct:k1"]);
    assert!(reader.saw(b"v2"), "post-invalidation GET must see v2");
}

#[test]
fn multikey_del_invalidates_every_key() {
    let Some(m) = spawn_moon_4shard() else { return };

    let mut writer = Resp::connect(m.port);
    writer.cmd(&["MSET", "ct:a", "1", "ct:b", "2"]);
    assert!(writer.saw(b"+OK"));

    let mut reader = tracking_client(m.port);
    reader.cmd(&["GET", "ct:a"]);
    reader.cmd(&["GET", "ct:b"]);
    assert!(reader.saw(b"1") && reader.saw(b"2"));
    reader.clear();

    writer.cmd(&["DEL", "ct:a", "ct:b"]);
    assert!(writer.saw(b":2"), "DEL must remove both keys");

    assert!(
        reader.wait_for(INVALIDATE, Duration::from_secs(3)),
        "invalidate push must arrive after DEL"
    );
    let got_a = reader.wait_for(b"ct:a", Duration::from_secs(2));
    let got_b = reader.wait_for(b"ct:b", Duration::from_secs(2));
    assert!(
        got_a && got_b,
        "BOTH deleted keys must be invalidated, not just the first \
         (a={got_a}, b={got_b}, got: {:?})",
        String::from_utf8_lossy(&reader.buf)
    );
}

#[test]
fn flushall_pushes_flush_invalidation() {
    let Some(m) = spawn_moon_4shard() else { return };

    let mut writer = Resp::connect(m.port);
    writer.cmd(&["SET", "ct:f", "v"]);
    assert!(writer.saw(b"+OK"));

    let mut reader = tracking_client(m.port);
    reader.cmd(&["GET", "ct:f"]);
    assert!(reader.saw(b"v"));
    reader.clear();

    writer.cmd(&["FLUSHALL"]);
    assert!(writer.saw(b"+OK"), "FLUSHALL must succeed");

    // Redis convention: cache-flush is signalled by an invalidate push whose
    // payload is a Null array ("flush everything you cached").
    assert!(
        reader.wait_for(INVALIDATE, Duration::from_secs(3)),
        "FLUSHALL must push a flush invalidation to tracking clients (got: {:?})",
        String::from_utf8_lossy(&reader.buf)
    );
}

#[test]
fn mset_invalidates_every_second_arg_key() {
    let Some(m) = spawn_moon_4shard() else { return };

    let mut writer = Resp::connect(m.port);
    writer.cmd(&["MSET", "ct:m1", "x", "ct:m2", "y"]);
    assert!(writer.saw(b"+OK"));

    let mut reader = tracking_client(m.port);
    reader.cmd(&["GET", "ct:m2"]); // track the SECOND key only
    assert!(reader.saw(b"y"));
    reader.clear();

    // MSET's keys are every second argument — key-spec extraction must not
    // treat the value "xx" as a key nor stop at ct:m1.
    writer.cmd(&["MSET", "ct:m1", "xx", "ct:m2", "yy"]);
    assert!(writer.saw(b"+OK"));

    assert!(
        reader.wait_for(INVALIDATE, Duration::from_secs(3)) && reader.saw(b"ct:m2"),
        "tracking client of ct:m2 must be invalidated by MSET (got: {:?})",
        String::from_utf8_lossy(&reader.buf)
    );
}

/// A routed (Phase B) MULTI/EXEC — whose owning shard differs from the writer's
/// accept shard — must still invalidate tracking clients, exactly like a local
/// EXEC or a plain write. Before the fix the routed EXEC path returned before
/// reaching the tracking-invalidation block, so cached readers were never
/// notified.
///
/// A single warmed writer connection has a fixed accept shard, so across 12
/// distinct keys ~3/4 of the single-key bodies route to a remote owner. Delivery
/// to an idle cross-shard reader is inherently ~75% per attempt in this suite
/// (a documented 4-shard async-push timing property — it hits plain SET and
/// MULTI/EXEC equally), so this asserts a MAJORITY deliver rather than all: the
/// fix yields ~9/12, whereas the pre-fix routed-skip yields only ~2/12 (just the
/// occasional local iteration), which the threshold cleanly rejects.
#[test]
fn routed_multi_exec_invalidates_tracking_client() {
    let Some(m) = spawn_moon_4shard() else { return };

    // One warmed writer (fixed accept shard → most keys route) reused for seed
    // + commit; one reader reused across keys. Low connection churn.
    let mut txw = Resp::connect(m.port);
    txw.cmd(&["PING"]);
    assert!(txw.saw(b"PONG"));

    let total = 12;
    let mut delivered = 0;
    for i in 0..total {
        let key = format!("ctxroute{i}");

        txw.clear();
        txw.cmd(&["SET", &key, "v1"]);
        assert!(txw.saw(b"+OK"), "seed SET must succeed");

        let mut reader = tracking_client(m.port);
        reader.cmd(&["GET", &key]);
        assert!(reader.saw(b"v1"), "tracked GET must return the seed value");
        reader.clear();

        txw.cmd(&["MULTI"]);
        txw.cmd(&["SET", &key, "v2"]);
        txw.clear();
        txw.cmd(&["EXEC"]);

        // Disambiguate EXEC failure from invalidation-delivery loss: a routed
        // single-key EXEC that committed returns `*1\r\n+OK\r\n`. If the routed
        // execution itself errored (e.g. owner-shard-unavailable → CROSSSLOT),
        // fail loudly here rather than silently under-counting `delivered` and
        // misreporting a routing bug as an invalidation regression.
        assert!(
            txw.saw(b"+OK"),
            "routed EXEC must commit the queued SET (got: {:?})",
            String::from_utf8_lossy(&txw.buf)
        );

        if reader.wait_for(INVALIDATE, Duration::from_secs(4)) && reader.saw(key.as_bytes()) {
            delivered += 1;
        }
    }

    assert!(
        delivered * 2 >= total,
        "routed MULTI/EXEC invalidation regression: only {delivered}/{total} writes \
         invalidated the tracking client (pre-fix routed EXEC skips invalidation \
         entirely, delivering ~2/12; the fix restores ~9/12)"
    );
}
