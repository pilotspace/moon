//! `GET` inside `MULTI` must be QUEUED, not executed (client-compat P0).
//!
//! Found by `scripts/test-client-compat.sh` on its first full run against a
//! real redis-server (2026-08-09). The monoio inline fast path
//! (`try_inline_dispatch`, `src/server/conn/blocking.rs`) answers a plain
//! `GET key` straight from the shard map. `can_inline_writes` folds in
//! `!conn.in_multi`, so `SET` correctly queues — `can_inline_reads` did not,
//! so `GET` executed immediately inside an open transaction:
//!
//! ```text
//! MULTI     -> +OK
//! GET k     -> $1 v      (Redis: +QUEUED)
//! EXEC      -> *0        (Redis: *1[$1 v])
//! MGET k    -> +QUEUED   control: not inline-eligible, always queued correctly
//! ```
//!
//! The client is not merely reading a stale value: it receives a reply that is
//! the wrong *kind* for its position in the exchange, and then an EXEC that
//! silently omits the read. A redis-py/go-redis pipeline built on
//! `MULTI ... EXEC` returns an empty result set for a transaction it believes
//! succeeded.
//!
//! Same gate, same fix shape as the ACL bypass in
//! `tests/acl_inline_read_enforcement.rs` — and the same visibility problem:
//! the inline path exists only in the monoio handler, and every CI test job
//! builds tokio. Under a tokio build these tests pass without exercising the
//! defect, so keep a monoio run in the release gate.

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-multiinline-{port}"));
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
                // This host hovers near the 5% diskfull line — the guard would
                // turn every SET into a MOONERR and flake the suite.
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-multiinline-{port}"));
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

    /// Send several commands in ONE write and read whatever comes back.
    ///
    /// Needed because `can_inline_reads` is computed once per read-buffer
    /// iteration, *before* `try_inline_dispatch_loop` walks the buffer — so a
    /// `MULTI` arriving in the same write as the `GET` after it is the case
    /// where the gate is evaluated before the state it depends on exists.
    fn pipeline(&mut self, cmds: &[&[&str]]) -> String {
        let mut out = Vec::new();
        for args in cmds {
            out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in *args {
                out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
            }
        }
        self.stream.write_all(&out).expect("write");
        // Replies may arrive across several segments; keep reading until the
        // socket goes quiet rather than assuming one packet.
        let mut acc = String::new();
        let mut buf = [0u8; 8192];
        for _ in 0..8 {
            match self.stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    acc.push_str(&String::from_utf8_lossy(&buf[..n]));
                    if acc.matches("\r\n").count() >= cmds.len() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        acc
    }
}

/// Keys enough that at `--shards 4` some land on the connection's own shard —
/// only those are inline-eligible, so a shard-local key is required to
/// reproduce the defect at all.
const KEYS: usize = 24;

fn assert_queues_and_execs(shards: &str) {
    let moon = spawn_moon(shards);
    let mut c = Resp::connect(moon.port);

    for i in 0..KEYS {
        let key = format!("k{i}");
        let val = format!("v{i}");
        assert!(
            c.cmd(&["SET", &key, &val]).starts_with("+OK"),
            "setup SET failed for {key}"
        );
    }

    for i in 0..KEYS {
        let key = format!("k{i}");
        let val = format!("v{i}");

        assert!(c.cmd(&["MULTI"]).starts_with("+OK"), "MULTI refused");

        // The defect: an inline-eligible GET is answered here instead of queued.
        let queued = c.cmd(&["GET", &key]);
        assert!(
            queued.starts_with("+QUEUED"),
            "GET {key} inside MULTI must be QUEUED, got {queued:?} \
             (the inline fast path executed it instead of queueing)"
        );

        // ...and the value must then arrive from EXEC, not before it.
        let exec = c.cmd(&["EXEC"]);
        let want = format!("*1\r\n${}\r\n{val}\r\n", val.len());
        assert_eq!(
            exec, want,
            "EXEC must return the queued GET's value for {key}"
        );
    }
}

#[test]
fn inline_get_is_queued_inside_multi_single_shard() {
    assert_queues_and_execs("1");
}

#[test]
fn inline_get_is_queued_inside_multi_multi_shard() {
    assert_queues_and_execs("4");
}

/// The control: `MGET` is not inline-eligible, so it queued correctly even
/// before the fix. Pinning it keeps a future change from "fixing" the read
/// gate by disabling transaction queueing wholesale.
#[test]
fn non_inlinable_read_still_queues() {
    let moon = spawn_moon("1");
    let mut c = Resp::connect(moon.port);
    assert!(c.cmd(&["SET", "k", "v"]).starts_with("+OK"));
    assert!(c.cmd(&["MULTI"]).starts_with("+OK"));
    assert!(
        c.cmd(&["MGET", "k"]).starts_with("+QUEUED"),
        "MGET must queue inside MULTI"
    );
    assert_eq!(c.cmd(&["EXEC"]), "*1\r\n*1\r\n$1\r\nv\r\n");
}

/// A write inside MULTI already queued (`can_inline_writes` carries
/// `!conn.in_multi`). Pinned so the read fix cannot regress the write gate.
#[test]
fn inline_write_still_queues_inside_multi() {
    let moon = spawn_moon("1");
    let mut c = Resp::connect(moon.port);
    assert!(c.cmd(&["MULTI"]).starts_with("+OK"));
    assert!(
        c.cmd(&["SET", "k", "v"]).starts_with("+QUEUED"),
        "SET must queue inside MULTI"
    );
    assert_eq!(c.cmd(&["EXEC"]), "*1\r\n+OK\r\n");
    assert_eq!(c.cmd(&["GET", "k"]), "$1\r\nv\r\n");
}

/// The gate's sharpest edge: `can_inline_reads` is computed ONCE per
/// read-buffer iteration, before `try_inline_dispatch_loop` walks the buffer.
/// When `MULTI` and the `GET` after it arrive in the SAME write, the gate was
/// evaluated while `conn.in_multi` was still false.
///
/// Measured: this case passes with the `!conn.in_multi` gate REMOVED, because
/// the inline loop stops at the first command that is not a plain GET/SET, so a
/// buffer containing `MULTI` hands its whole remainder to generic dispatch and
/// never reaches the inline path again. The gate is belt-and-braces here — the
/// defect only reproduces when `MULTI` and the `GET` arrive in separate reads
/// (`inline_get_is_queued_inside_multi_*` cover that).
///
/// So this test does not guard the gate; it guards the *structural property
/// that makes the gate sufficient*. Anything that teaches the inline loop to
/// skip past commands it does not recognise, instead of bailing out, would
/// silently reopen the bug for pipelined clients — and only this test would
/// notice. Stated plainly because a test whose value is misdescribed is worse
/// than no test.
#[test]
fn pipelined_multi_still_queues_the_following_get() {
    let moon = spawn_moon("1");
    let mut c = Resp::connect(moon.port);
    assert!(c.cmd(&["SET", "k", "v"]).starts_with("+OK"));

    // MULTI arrives in the same write as the GET it must protect.
    let got = c.pipeline(&[&["MULTI"], &["GET", "k"], &["EXEC"]]);
    assert_eq!(
        got, "+OK\r\n+QUEUED\r\n*1\r\n$1\r\nv\r\n",
        "a pipelined MULTI must still queue the GET that follows it in the \
         same write (gate is computed before MULTI is parsed)"
    );

    // ...and a GET *before* the MULTI in one write must still be inlined and
    // answered directly, so the fix did not simply disable the fast path
    // whenever a buffer happens to contain MULTI.
    let got = c.pipeline(&[&["GET", "k"], &["MULTI"], &["GET", "k"], &["EXEC"]]);
    assert_eq!(
        got, "$1\r\nv\r\n+OK\r\n+QUEUED\r\n*1\r\n$1\r\nv\r\n",
        "a GET before MULTI in the same write must still be served inline"
    );
}

/// Outside a transaction the inline path must still serve the read — the fix
/// is a gate, not a removal. Guards against "fixing" this by disabling inline
/// GET entirely, which would silently cost the hot path.
#[test]
fn inline_get_still_serves_outside_multi() {
    let moon = spawn_moon("1");
    let mut c = Resp::connect(moon.port);
    assert!(c.cmd(&["SET", "k", "v"]).starts_with("+OK"));
    assert_eq!(c.cmd(&["GET", "k"]), "$1\r\nv\r\n");
    // and after a completed transaction the connection returns to normal
    assert!(c.cmd(&["MULTI"]).starts_with("+OK"));
    assert!(c.cmd(&["GET", "k"]).starts_with("+QUEUED"));
    assert_eq!(c.cmd(&["EXEC"]), "*1\r\n$1\r\nv\r\n");
    assert_eq!(c.cmd(&["GET", "k"]), "$1\r\nv\r\n");
}
